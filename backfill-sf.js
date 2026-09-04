// ============================================================
// backfill-sf.js — SF backfill as an importable module.
// Replays completed leads from Postgres through the existing
// pushToSalesforce() (find-by-email → update or create).
// Side effects: Salesforce writes ONLY. No Slack/Meta/email/AWS.
//
// Exposed via a temporary admin route in index.js:
//   GET /admin/backfill-sf?key=KEY&dry=1     ← preview
//   GET /admin/backfill-sf?key=KEY           ← real run
//   optional: &from=ISO&to=ISO      to change the window
//   optional: &emails=a@b.com,c@d.com  to target NAMED rows only
//
// PREFER &emails. Without it this replays a whole WINDOW: every submitted
// lead in it, re-pushed through pushToSalesforce, which UPDATES the existing
// Salesforce Lead with current DB values. Measured 5 Sept 2026: a mid-June
// start meant ~2,180 people and ~19 minutes of Salesforce writes to fix six
// rows. An allow-list makes the blast radius the thing you actually asked for.
// ============================================================

const { pushToSalesforce, getSalesforceToken } = require('./salesforce');

const SKIP_EMAILS = ['b@g.ai'];
const SKIP_DOMAINS = ['gushwork.ai'];

// Pre-flight: is there a Lead for this email, and is it converted?
// Returns 'converted' | 'exists' | 'none'
async function sfLeadStatus(email) {
  const { accessToken, instanceUrl } = await getSalesforceToken();
  const q = encodeURIComponent(
    `SELECT Id, IsConverted FROM Lead WHERE Email = '${email.replace(/'/g, "\\'")}' ORDER BY CreatedDate DESC LIMIT 1`
  );
  const res = await fetch(`${instanceUrl}/services/data/v60.0/query/?q=${q}`, {
    headers: { Authorization: `Bearer ${accessToken}` },
  });
  if (!res.ok) return 'none'; // fail open to normal push path
  const data = await res.json();
  if (!data.records || data.records.length === 0) return 'none';
  return data.records[0].IsConverted ? 'converted' : 'exists';
}

function shouldSkip(email) {
  const e = (email || '').toLowerCase();
  if (!e) return true;
  if (SKIP_EMAILS.includes(e)) return true;
  return SKIP_DOMAINS.includes(e.split('@')[1] || '');
}

// pushToSalesforce reads `booked` (→ completed__c) and `start_time`;
// DB rows carry `completed`/`booking_uid` and a varying time column.
function rowToPayload(row, cols) {
  const payload = { ...row };
  payload.booked = !!row.booking_uid;

  const startCol = ['start_time', 'booking_start_time', 'booking_time', 'booked_at'].find(
    (c) => cols.has(c) && row[c]
  );
  if (startCol && !payload.start_time) payload.start_time = row[startCol];
  if (row.booking_uid && !payload.event_type) payload.event_type = 'demo';

  return payload;
}

/* Normalises an allow-list from either an array or a comma/space separated
   query-string value. Lowercased to match the LOWER(email) comparison. */
function parseEmails(v) {
  if (!v) return null;
  const arr = Array.isArray(v) ? v : String(v).split(/[,\s]+/);
  const out = Array.from(new Set(arr.map((e) => String(e || '').trim().toLowerCase()).filter(Boolean)));
  return out.length ? out : null;
}

/**
 * Run the backfill.
 * @param {object} pool   — the pg Pool index.js already has
 * @param {object} opts   — { from, to, dry, emails }
 *   emails — array or comma-separated string. When given, ONLY these addresses
 *   are considered, and the window is widened to cover them so a caller cannot
 *   name a row and silently miss it because the default window starts later.
 * @returns {object}      — { window, dry, results[], summary }
 */
async function runBackfill(pool, opts = {}) {
  const emails = parseEmails(opts.emails);
  /* The default window is the July 2026 incident start. With an explicit
     allow-list that default is a trap rather than a safeguard: naming a row
     from June returns "found: 0" and reads as "nothing to do". Measured — two
     of the six leads this option was written for predate it. So an allow-list
     opens the window unless the caller set one deliberately. */
  const from = opts.from || (emails ? '2026-01-01T00:00:00Z' : '2026-07-08T12:20:00Z');
  const to = opts.to || new Date().toISOString();
  const dry = !!opts.dry;

  const log = [];

  // Discover the actual leads columns (schema-adaptive)
  const colRes = await pool.query(
    `SELECT column_name FROM information_schema.columns WHERE table_name = 'leads'`
  );
  const cols = new Set(colRes.rows.map((r) => r.column_name));
  if (cols.size === 0) throw new Error("No 'leads' table found");

  const tsCol = ['updated_at', 'last_updated', 'created_at'].find((c) => cols.has(c));
  if (!tsCol) throw new Error('No timestamp column (updated_at/created_at) on leads');

  /* submitted_at IS NOT NULL, NOT completed = true. Two reasons, and the
     first one has teeth:

     1. completed does not mean "submitted the form". The Cal and RevenueHero
        safety-net branches set it for someone who booked without ever touching
        the form — aasnj@meta.com on 18 Aug 2026 has completed = true and
        submitted_at = NULL on both Railway and the mirror. This tool exists to
        replay FORM SUBMISSIONS into Salesforce, and pushToSalesforce runs from
        /submit, so selecting on completed would create Leads for people who
        never filled anything in. That may sometimes be wanted; it should be a
        decision, not a side effect of reading the nearest available column.
     2. IS NOT NULL rather than = true also sidesteps the equals-true trap that
        has its own open ticket: a NULL flag satisfies neither = true nor
        = false and vanishes from the query entirely.

     See docs/tickets/completed-is-not-submitted-a-form.md. */
  const params = [from, to];
  let emailClause = '';
  if (emails && emails.length) {
    /* Parameterised, never interpolated — this takes a query string. */
    params.push(emails);
    emailClause = `AND LOWER(email) = ANY($${params.length}::text[])`;
  }
  const { rows } = await pool.query(
    `SELECT * FROM leads
      WHERE submitted_at IS NOT NULL
        AND ${tsCol} >= $1 AND ${tsCol} <= $2
        ${emailClause}
      ORDER BY ${tsCol} ASC`,
    params
  );

  let pushed = 0, skipped = 0, failed = 0;

  for (const row of rows) {
    if (shouldSkip(row.email)) {
      log.push({ email: row.email, action: 'skipped (test/internal)' });
      skipped++;
      continue;
    }

    const payload = rowToPayload(row, cols);
    const entry = {
      email: row.email,
      booked: payload.booked,
      booking_uid: row.booking_uid || null,
      start_time: payload.start_time || null,
    };

    // HARD GUARD: never touch converted leads — no create, no update
    const status = await sfLeadStatus(row.email);
    if (status === 'converted') {
      entry.action = 'skipped — already CONVERTED in SF';
      log.push(entry);
      skipped++;
      continue;
    }
    entry.sf_status = status; // 'exists' → will update | 'none' → will create

    if (dry) {
      entry.action = status === 'exists' ? 'WOULD UPDATE (dry run)' : 'WOULD CREATE (dry run)';
      log.push(entry);
      pushed++;
      continue;
    }

    const result = await pushToSalesforce(payload);
    if (result && result.success) {
      entry.action = 'pushed';
      entry.leadId = result.leadId;
      pushed++;
    } else {
      entry.action = 'FAILED';
      entry.error = result && result.error;
      failed++;
    }
    log.push(entry);

    // pace SF API
    await new Promise((r) => setTimeout(r, 500));
  }

  /* An address that was asked for and did not match is REPORTED, never left
     as the difference between two counts. "I named six and it pushed four" has
     to say which two and why — otherwise a typo, a row outside the window and
     a row that never submitted all look identical from the summary. */
  const requestedNotFound = emails
    ? emails.filter((e) => !rows.some((r) => (r.email || '').toLowerCase() === e))
    : [];

  return {
    window: { from, to, timeColumn: tsCol },
    dry,
    emails: emails || null,
    requestedNotFound,
    results: log,
    summary: {
      requested: emails ? emails.length : null,
      found: rows.length, pushed, skipped, failed,
      notFound: requestedNotFound.length,
    },
  };
}

module.exports = { runBackfill, parseEmails };
