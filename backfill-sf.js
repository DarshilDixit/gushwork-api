// ============================================================
// backfill-sf.js — SF backfill as an importable module.
// Replays completed leads from Postgres through the existing
// pushToSalesforce() (find-by-email → update or create).
// Side effects: Salesforce writes ONLY. No Slack/Meta/email/AWS.
//
// Exposed via a temporary admin route in index.js:
//   GET /admin/backfill-sf?key=KEY&dry=1     ← preview
//   GET /admin/backfill-sf?key=KEY           ← real run
//   optional: &from=ISO&to=ISO to change the window
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

/**
 * Run the backfill.
 * @param {object} pool  — the pg Pool index.js already has
 * @param {object} opts  — { from, to, dry }
 * @returns {object}     — { window, dry, results[], summary }
 */
async function runBackfill(pool, opts = {}) {
  const from = opts.from || '2026-07-08T12:20:00Z'; // incident start (UTC)
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

  const { rows } = await pool.query(
    `SELECT * FROM leads
      WHERE completed = true
        AND ${tsCol} >= $1 AND ${tsCol} <= $2
      ORDER BY ${tsCol} ASC`,
    [from, to]
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

  return {
    window: { from, to, timeColumn: tsCol },
    dry,
    results: log,
    summary: { found: rows.length, pushed, skipped, failed },
  };
}

module.exports = { runBackfill };
