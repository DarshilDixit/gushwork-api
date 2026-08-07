// ============================================================
// lead-magnet.js — Lead-magnet LP capture (150 buyer questions)
// ------------------------------------------------------------
// FULLY ISOLATED from the /demo form system:
//   • own table   — lead_magnet_leads   (never touches `leads`)
//   • own routes  — /lm/*               (never touches /partial, /submit)
//   • own session — gw_lm_session_id    (never touches gw_session_id)
//   • no Salesforce, no AWS sync, no Slack, no Apollo enrichment
//
// SHARED with the existing system, deliberately:
//   • the ELV cache, via the /verify-email endpoint the browser calls
//   • meta-capi.js, for the Contact event
//
// Mounted as a factory so nothing has to move out of index.js:
//   const createLeadMagnetRouter = require('./lead-magnet');
//   app.use(createLeadMagnetRouter({ pool, elvIsInternal, FREE_EMAIL_DOMAINS }));
// ============================================================

const express = require('express');
const dnsPromises = require('dns').promises;
const { pushContactToMeta } = require('./meta-capi');

/* Row is created at page load, so most rows have no email. That is the
   point — they are the funnel denominator. Everything that reads this
   table as "leads" filters on email IS NOT NULL or completed = true.
   Set to false to only create a row once an email is entered. */
const TRACK_FROM_PAGE_LOAD = true;

/* A second submit of the same email + product inside this window is
   treated as a double-tap: stored, but no Contact event, no queue entry. */
const DUPLICATE_WINDOW_MIN = 10;

const STEP = { VIEW: 1, MODAL: 2, EMAIL: 3, SUBMITTED: 4 };

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const EMAIL_RE = /^[^\s@]+@[^\s@]+\.[a-zA-Z]{2,}$/;

const s = (v, max) => (v || '').toString().trim().slice(0, max);
const bool = (v) => v === true || v === 'true';

/* ------------------------------------------------------------
   Server-side email gate.

   The browser has already called /verify-email (full ELV: DNS raced
   against the provider, typo suggestions, the shared cache). This is
   the backstop for anything that skips the browser — a forged POST,
   a bot, a replayed request.

   It deliberately does NOT call ELV again. Doing so from here would
   either burn a second credit or need the /verify-email handler
   refactored out of index.js, and index.js is running production
   traffic. Syntax + MX catches essentially everything a forged submit
   throws at it, costs nothing, and adds no dependency.

   Fail-open on DNS trouble, same philosophy as the main system: a
   resolver hiccup must never cost a real lead.
------------------------------------------------------------ */
async function gateEmail(email) {
  if (!email || !EMAIL_RE.test(email)) {
    return { ok: false, status: 'invalid_syntax' };
  }
  const domain = email.split('@')[1];
  try {
    const mx = await dnsPromises.resolveMx(domain).catch(() => null);
    if (mx && mx.length) return { ok: true, status: 'mx_ok' };
    // No MX is not fatal — RFC 5321 allows A-record fallback for mail.
    const a = await dnsPromises.resolve4(domain).catch(() => null);
    if (a && a.length) return { ok: true, status: 'a_record_only' };
    return { ok: false, status: 'domain_error' };
  } catch (err) {
    console.warn('[LM] DNS check failed, failing open:', err.message);
    return { ok: true, status: 'dns_error_fallback' };
  }
}

/* ------------------------------------------------------------
   The website field is only SHOWN for free mailboxes — a business
   email already tells us the domain, so asking would be a pointless
   extra field on the highest-intent leads.

   But that would leave `website` NULL for exactly those leads in the
   handoff queue, and whoever builds the question list wants a site to
   look at for every row. So derive it from the email domain instead:
   jacob@acme.com -> https://acme.com. website_source records which
   path it came from, so nobody downstream mistakes a guess for
   something the person actually typed.
------------------------------------------------------------ */
function resolveWebsite(email, entered, free) {
  if (entered) {
    var url = /^https?:\/\//i.test(entered) ? entered : 'https://' + entered;
    return { website: url, website_source: 'entered' };
  }
  if (email && !free) {
    var domain = (email.split('@')[1] || '').toLowerCase();
    if (domain) return { website: 'https://' + domain, website_source: 'derived_from_email' };
  }
  return { website: null, website_source: null };
}

function readPayload(body) {
  return {
    session_id: s(body.session_id, 100),
    linked_session_id: s(body.linked_session_id, 100),
    email: s(body.email, 254).toLowerCase(),
    website: s(body.website, 500),
    industry_category: s(body.industry_category, 200),
    industry_is_custom: bool(body.industry_is_custom),
    product_or_service: s(body.product_or_service, 300),
    sell_to: s(body.sell_to, 50),
    elv_status: s(body.elv_status, 50),
    page_url: s(body.page_url, 500),
    landing_page: s(body.landing_page, 500),
    previous_page: s(body.previous_page, 500),
    referrer: s(body.referrer, 500),
    utm_source: s(body.utm_source, 100),
    utm_medium: s(body.utm_medium, 100),
    utm_campaign: s(body.utm_campaign, 100),
    utm_content: s(body.utm_content, 100),
    utm_term: s(body.utm_term, 100),
    fbc: s(body.fbc, 500),
    fbp: s(body.fbp, 200),
    step_reached: parseInt(body.step_reached, 10) || STEP.VIEW,
  };
}

const UPSERT_SQL = `
  INSERT INTO lead_magnet_leads
    (session_id, linked_session_id, email, is_free_email, is_internal, website, website_source,
     industry_category, industry_is_custom, product_or_service, sell_to,
     elv_status, elv_checked_at, page_url, landing_page, previous_page, referrer,
     utm_source, utm_medium, utm_campaign, utm_content, utm_term,
     fbc, fbp, user_agent, step_reached, completed, submitted_at, updated_at)
  VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,NOW())
  ON CONFLICT (session_id) DO UPDATE SET
    linked_session_id  = COALESCE(EXCLUDED.linked_session_id,  lead_magnet_leads.linked_session_id),
    email              = COALESCE(EXCLUDED.email,              lead_magnet_leads.email),
    is_free_email      = COALESCE(EXCLUDED.is_free_email,      lead_magnet_leads.is_free_email),
    is_internal        = COALESCE(EXCLUDED.is_internal,        lead_magnet_leads.is_internal),
    website            = COALESCE(EXCLUDED.website,            lead_magnet_leads.website),
    website_source     = COALESCE(EXCLUDED.website_source,     lead_magnet_leads.website_source),
    industry_category  = COALESCE(EXCLUDED.industry_category,  lead_magnet_leads.industry_category),
    industry_is_custom = COALESCE(EXCLUDED.industry_is_custom, lead_magnet_leads.industry_is_custom),
    product_or_service = COALESCE(EXCLUDED.product_or_service, lead_magnet_leads.product_or_service),
    sell_to            = COALESCE(EXCLUDED.sell_to,            lead_magnet_leads.sell_to),
    elv_status         = COALESCE(EXCLUDED.elv_status,         lead_magnet_leads.elv_status),
    elv_checked_at     = COALESCE(EXCLUDED.elv_checked_at,     lead_magnet_leads.elv_checked_at),
    page_url           = COALESCE(EXCLUDED.page_url,           lead_magnet_leads.page_url),
    landing_page       = COALESCE(EXCLUDED.landing_page,       lead_magnet_leads.landing_page),
    previous_page      = COALESCE(EXCLUDED.previous_page,      lead_magnet_leads.previous_page),
    referrer           = COALESCE(EXCLUDED.referrer,           lead_magnet_leads.referrer),
    utm_source         = COALESCE(EXCLUDED.utm_source,         lead_magnet_leads.utm_source),
    utm_medium         = COALESCE(EXCLUDED.utm_medium,         lead_magnet_leads.utm_medium),
    utm_campaign       = COALESCE(EXCLUDED.utm_campaign,       lead_magnet_leads.utm_campaign),
    utm_content        = COALESCE(EXCLUDED.utm_content,        lead_magnet_leads.utm_content),
    utm_term           = COALESCE(EXCLUDED.utm_term,           lead_magnet_leads.utm_term),
    fbc                = COALESCE(EXCLUDED.fbc,                lead_magnet_leads.fbc),
    fbp                = COALESCE(EXCLUDED.fbp,                lead_magnet_leads.fbp),
    user_agent         = COALESCE(EXCLUDED.user_agent,         lead_magnet_leads.user_agent),
    -- step only ever moves forward, so an out-of-order request can't rewind it
    step_reached       = GREATEST(EXCLUDED.step_reached,       lead_magnet_leads.step_reached),
    completed          = lead_magnet_leads.completed OR EXCLUDED.completed,
    submitted_at       = COALESCE(lead_magnet_leads.submitted_at, EXCLUDED.submitted_at),
    updated_at         = NOW()
  RETURNING id, completed, capi_contact_sent
`;

module.exports = function createLeadMagnetRouter(deps) {
  const { pool, elvIsInternal, FREE_EMAIL_DOMAINS } = deps;
  const router = express.Router();

  const isFree = (email) =>
    !!email && FREE_EMAIL_DOMAINS.includes((email.split('@')[1] || '').toLowerCase());

  function upsertArgs(p, req, { completed = false } = {}) {
    const internal = p.email ? elvIsInternal(p.email) : false;
    const free = p.email ? isFree(p.email) : false;
    const site = resolveWebsite(p.email, p.website, free);
    return [
      p.session_id,
      p.linked_session_id || null,
      p.email || null,
      p.email ? free : null,
      p.email ? internal : null,
      site.website,
      site.website_source,
      p.industry_category || null,
      p.industry_category ? p.industry_is_custom : null,
      p.product_or_service || null,
      p.sell_to || null,
      p.elv_status || null,
      p.elv_status ? new Date() : null,
      p.page_url || null,
      p.landing_page || null,
      p.previous_page || null,
      p.referrer || null,
      p.utm_source || null,
      p.utm_medium || null,
      p.utm_campaign || null,
      p.utm_content || null,
      p.utm_term || null,
      p.fbc || null,
      p.fbp || null,
      s(req.headers['user-agent'], 400) || null,
      p.step_reached,
      completed,
      completed ? new Date() : null,
    ];
  }

  /* ── POST /lm/track ────────────────────────────────────────
     One endpoint for every pre-submit stage. The client sends
     step_reached 1 (page load), 2 (modal open) or 3 (email entered
     and ELV-passed). Never fires a Meta event — the gap between
     step 3 and submit is seconds here, so an event would be a near
     duplicate of Contact and would pollute optimisation.
  ──────────────────────────────────────────────────────────── */
  router.post('/lm/track', async (req, res) => {
    const p = readPayload(req.body);
    if (!UUID_RE.test(p.session_id)) return res.status(400).json({ error: 'valid session_id required' });
    if (!TRACK_FROM_PAGE_LOAD && !p.email) return res.json({ ok: true, skipped: true });
    if (p.step_reached >= STEP.SUBMITTED) p.step_reached = STEP.EMAIL; // only /lm/submit completes

    try {
      await pool.query(UPSERT_SQL, upsertArgs(p, req));
      res.json({ ok: true });
    } catch (err) {
      console.error('[LM /track]', err.message);
      res.status(500).json({ error: 'track failed' });
    }
  });

  /* ── POST /lm/submit ───────────────────────────────────────
     Completes the row, fires Contact, pushes the webhook.
  ──────────────────────────────────────────────────────────── */
  router.post('/lm/submit', async (req, res) => {
    const p = readPayload(req.body);
    if (!UUID_RE.test(p.session_id)) return res.status(400).json({ error: 'valid session_id required' });

    const gate = await gateEmail(p.email);
    if (!gate.ok) {
      console.log(`[LM /submit] rejected ${p.email} — ${gate.status}`);
      return res.status(422).json({
        ok: false,
        status: gate.status,
        message: "That email address doesn't appear to be valid. Mind double-checking it?",
      });
    }

    if (!p.industry_category || !p.product_or_service || !p.sell_to) {
      return res.status(400).json({ ok: false, message: 'Missing required fields.' });
    }

    p.step_reached = STEP.SUBMITTED;
    const internal = elvIsInternal(p.email);

    try {
      /* Duplicate check runs BEFORE the upsert — after it, the row we
         just wrote would match itself. Scoped to other sessions only. */
      const dupe = await pool.query(
        `SELECT id FROM lead_magnet_leads
          WHERE email = $1 AND product_or_service = $2
            AND session_id <> $3 AND completed = true
            AND submitted_at > NOW() - INTERVAL '${DUPLICATE_WINDOW_MIN} minutes'
          LIMIT 1`,
        [p.email, p.product_or_service, p.session_id]
      );
      const isDuplicate = dupe.rowCount > 0;

      const result = await pool.query(UPSERT_SQL, upsertArgs(p, req, { completed: true }));
      const row = result.rows[0];
      const alreadySent = row.capi_contact_sent === true;

      // Respond immediately — side effects must never hold up the thanks screen.
      res.json({ ok: true, duplicate: isDuplicate });

      if (internal) {
        console.log(`[LM /submit] ✅ ${p.email} (internal — no Contact, not queued)`);
        return;
      }
      if (isDuplicate || alreadySent) {
        console.log(`[LM /submit] ✅ ${p.email} (duplicate — stored, no Contact)`);
        return;
      }

      pushContactToMeta(
        {
          session_id: p.session_id,
          email: p.email,
          website: p.website,
          sell_to: p.sell_to,
          page_url: p.page_url,
          landing_page: p.landing_page,
          fbc: p.fbc,
          fbp: p.fbp,
          industry_category: p.industry_category,
          product_or_service: p.product_or_service,
          is_free_email: isFree(p.email),
        },
        {
          clientIpAddress: req.headers['x-forwarded-for'] || req.ip || '',
          clientUserAgent: req.headers['user-agent'] || '',
        }
      )
        .then(() =>
          pool.query('UPDATE lead_magnet_leads SET capi_contact_sent = true WHERE id = $1', [row.id])
        )
        .catch((err) => console.warn('[LM] Meta Contact failed (non-blocking):', err.message));

      sendWebhook({
        id: row.id,
        session_id: p.session_id,
        email: p.email,
        website: resolveWebsite(p.email, p.website, isFree(p.email)).website,
        industry_category: p.industry_category,
        industry_is_custom: p.industry_is_custom,
        product_or_service: p.product_or_service,
        sell_to: p.sell_to,
        is_free_email: isFree(p.email),
        submitted_at: new Date().toISOString(),
      });

      console.log(`[LM /submit] ✅ ${p.email} | ${p.industry_category} | ${p.sell_to}`);
    } catch (err) {
      console.error('[LM /submit]', err.message);
      if (!res.headersSent) res.status(500).json({ ok: false, message: 'Submit failed' });
    }
  });

  /* ── Real-time handoff ─────────────────────────────────────
     Unset LM_WEBHOOK_URL = this never runs. Fire-and-forget, same
     pattern as syncToAWS: if it fails the row is already committed
     and lead_magnet_queue still surfaces it.
  ──────────────────────────────────────────────────────────── */
  function sendWebhook(lead) {
    const url = process.env.LM_WEBHOOK_URL;
    if (!url) return;
    const headers = { 'Content-Type': 'application/json' };
    if (process.env.LM_WEBHOOK_SECRET) headers['X-LM-Secret'] = process.env.LM_WEBHOOK_SECRET;
    fetch(url, { method: 'POST', headers, body: JSON.stringify(lead) })
      .then((r) => {
        if (!r.ok) console.warn(`[LM webhook] HTTP ${r.status} for lead ${lead.id}`);
      })
      .catch((err) => console.warn('[LM webhook] failed (non-blocking):', err.message));
  }

  /* ── Pull handoff ──────────────────────────────────────────
     Reads the view, so internal tests, partials and already-sent
     leads are filtered by the database rather than by convention.
  ──────────────────────────────────────────────────────────── */
  function queueAuth(req, res, next) {
    const token = process.env.LM_QUEUE_TOKEN;
    if (!token) return next(); // unset = open, for internal-only use
    const given = req.query.token || req.headers['x-lm-token'];
    if (given !== token) return res.status(401).json({ error: 'unauthorized' });
    next();
  }

  router.get('/lm/queue', queueAuth, async (req, res) => {
    const limit = Math.min(parseInt(req.query.limit, 10) || 100, 500);
    try {
      const { rows } = await pool.query(
        'SELECT * FROM lead_magnet_queue LIMIT $1',
        [limit]
      );
      res.json({ ok: true, count: rows.length, leads: rows });
    } catch (err) {
      console.error('[LM /queue]', err.message);
      res.status(500).json({ error: 'queue read failed' });
    }
  });

  router.post('/lm/queue/:id/delivered', queueAuth, async (req, res) => {
    const id = parseInt(req.params.id, 10);
    if (!id) return res.status(400).json({ error: 'valid id required' });
    try {
      const { rowCount } = await pool.query(
        `UPDATE lead_magnet_leads
            SET delivered = true, delivered_at = NOW(),
                delivery_note = COALESCE($2, delivery_note), updated_at = NOW()
          WHERE id = $1 AND delivered = false`,
        [id, s(req.body && req.body.note, 300) || null]
      );
      res.json({ ok: true, updated: rowCount });
    } catch (err) {
      console.error('[LM /delivered]', err.message);
      res.status(500).json({ error: 'update failed' });
    }
  });

  /* ── Dashboard data ────────────────────────────────────────
     Consumed by the Lead Magnet tab in /monitor.
  ──────────────────────────────────────────────────────────── */
  router.get('/monitor/lm-metrics', async (req, res) => {
    res.setHeader('Cache-Control', 'no-store');
    const days = Math.min(parseInt(req.query.days, 10) || 30, 365);
    const scope = `created_at > NOW() - INTERVAL '${days} days' AND is_internal IS NOT TRUE`;
    try {
      const [funnel, industries, custom, daily] = await Promise.all([
        pool.query(`
          SELECT
            COUNT(*)                                              AS views,
            COUNT(*) FILTER (WHERE step_reached >= 2)             AS modal_opens,
            COUNT(*) FILTER (WHERE email IS NOT NULL)             AS emails,
            COUNT(*) FILTER (WHERE completed)                     AS submitted,
            COUNT(*) FILTER (WHERE completed AND is_free_email)   AS free_email,
            COUNT(*) FILTER (WHERE completed AND NOT is_free_email) AS business_email,
            COUNT(*) FILTER (WHERE completed AND NOT delivered)   AS pending_delivery
          FROM lead_magnet_leads WHERE ${scope}`),
        pool.query(`
          SELECT industry_category AS label, COUNT(*)::int AS n
            FROM lead_magnet_leads
           WHERE ${scope} AND completed AND industry_category IS NOT NULL
             AND industry_is_custom IS NOT TRUE
           GROUP BY 1 ORDER BY n DESC LIMIT 15`),
        pool.query(`
          SELECT industry_category AS label, COUNT(*)::int AS n, MAX(submitted_at) AS last_seen
            FROM lead_magnet_leads
           WHERE ${scope} AND completed AND industry_is_custom = true
           GROUP BY 1 ORDER BY n DESC, last_seen DESC LIMIT 50`),
        pool.query(`
          SELECT to_char(date_trunc('day', created_at), 'YYYY-MM-DD') AS day,
                 COUNT(*)::int AS views,
                 COUNT(*) FILTER (WHERE completed)::int AS submitted
            FROM lead_magnet_leads WHERE ${scope}
           GROUP BY 1 ORDER BY 1`),
      ]);
      res.json({
        funnel: funnel.rows[0],
        industries: industries.rows,
        custom_categories: custom.rows,
        daily: daily.rows,
      });
    } catch (err) {
      console.error('[LM /monitor/lm-metrics]', err.message);
      res.status(500).json({ error: err.message });
    }
  });

  router.get('/monitor/lm-leads', async (req, res) => {
    res.setHeader('Cache-Control', 'no-store');
    const limit = Math.min(parseInt(req.query.limit, 10) || 200, 1000);
    const onlyPending = req.query.pending === '1';
    try {
      const { rows } = await pool.query(
        `SELECT id, email, website, website_source, industry_category, industry_is_custom,
                product_or_service, sell_to, is_free_email, is_internal,
                utm_source, utm_campaign, landing_page, previous_page, referrer,
                delivered, delivered_at, submitted_at, created_at
           FROM lead_magnet_leads
          WHERE completed = true ${onlyPending ? 'AND delivered = false' : ''}
          ORDER BY submitted_at DESC LIMIT $1`,
        [limit]
      );
      res.json({ leads: rows });
    } catch (err) {
      console.error('[LM /monitor/lm-leads]', err.message);
      res.status(500).json({ error: err.message });
    }
  });

  console.log('[LM] Lead-magnet routes mounted — /lm/track, /lm/submit, /lm/queue' +
              (process.env.LM_WEBHOOK_URL ? ' | webhook ON' : ' | webhook off (LM_WEBHOOK_URL unset)'));

  return router;
};
