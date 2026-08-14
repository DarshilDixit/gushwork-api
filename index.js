require('dotenv').config();
const crypto    = require('crypto');
const dnsPromises = require('dns').promises;
const express   = require('express');
const cors      = require('cors');
const helmet    = require('helmet');
const rateLimit = require('express-rate-limit');
const { Pool }  = require('pg');
const { pool, initDB } = require('./db');
const { pushToSalesforce, findSFLeadByEmail, updateSFLead } = require('./salesforce');
const { pushFormEventsToMeta, pushStartTrialToMeta } = require('./meta-capi');
const createLeadMagnetRouter = require('./lead-magnet');

const app  = express();
const PORT = process.env.PORT || 3000;

// Free/personal mailbox domains — shared by /enrich, webhook enrichment,
// and the /partial StartTrial gate (Meta CAPI fires for business emails only)
const FREE_EMAIL_DOMAINS = ['gmail.com','yahoo.com','hotmail.com','outlook.com','icloud.com','protonmail.com','aol.com','mail.com','yahoo.in','rediffmail.com','ymail.com','live.com','msn.com','me.com','mac.com','googlemail.com'];

app.set('trust proxy', 1);

app.use((req, res, next) => {
  res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate');
  next();
});

app.use(helmet({
  contentSecurityPolicy: false
}));

const allowedOrigins = (process.env.ALLOWED_ORIGIN || '')
  .split(',').map(o => o.trim()).filter(Boolean);

app.use(cors({
  origin: function (origin, callback) {
    if (!origin) return callback(null, true);
    if (allowedOrigins.includes(origin)) return callback(null, true);
    callback(new Error(`CORS blocked: ${origin}`));
  },
  methods: ['GET', 'POST', 'OPTIONS'],
  allowedHeaders: ['Content-Type']
}));

app.use('/booking-confirmed-webhook-rh', express.raw({ type: 'application/json' }));
app.use(express.json({ limit: '10kb' }));

const globalLimiter = rateLimit({
  windowMs: 15 * 60 * 1000, max: 100,
  message: { error: 'Too many requests — please try again later.' },
  standardHeaders: true, legacyHeaders: false
});
const strictLimiter = rateLimit({
  windowMs: 60 * 60 * 1000, max: 50,
  message: { error: 'Rate limit exceeded — please try again later.' },
  standardHeaders: true, legacyHeaders: false
});
// UPDATED (2026-07): global limiter scoped to public-facing form
// endpoints only. Previously applied to ALL routes, which meant the
// monitor dashboard's own auto-refresh (multiple endpoints/min) tripped
// the 100-req/15-min limit and caused the recurring "dashboard not
// loading" error. /monitor/* is now exempt; webhooks are protected by
// HMAC signatures, not rate limits.
app.use('/partial',           globalLimiter);
app.use('/submit',            globalLimiter);
app.use('/session',           globalLimiter);
app.use('/booking-confirmed', globalLimiter);
app.use('/verify-email', strictLimiter);
app.use('/enrich',       strictLimiter);
app.use('/verify-website', strictLimiter);
// Lead magnet. /lm/queue is deliberately NOT limited (token-guarded instead) —
// the same mistake that made the monitor dashboard trip its own limiter.
app.use('/lm/track',  globalLimiter);
app.use('/lm/submit', globalLimiter);

let awsPool = null;

if (process.env.AWS_PG_HOST) {
  awsPool = new Pool({
    host:     process.env.AWS_PG_HOST,
    port:     parseInt(process.env.AWS_PG_PORT) || 5432,
    user:     process.env.AWS_PG_USER,
    password: process.env.AWS_PG_PASSWORD,
    database: process.env.AWS_PG_DATABASE,
    ssl:      { rejectUnauthorized: false },
    connectionTimeoutMillis: 5000,
    idleTimeoutMillis:       10000,
    max:                     3
  });
  console.log('[AWS] Pool configured for', process.env.AWS_PG_HOST);
} else {
  console.warn('[AWS] AWS_PG_HOST not set — AWS sync disabled');
}

async function initAWSTable() {
  if (!awsPool) return;
  try {
    await awsPool.query(`
      CREATE TABLE IF NOT EXISTS gw_form_leads (
        id                      SERIAL PRIMARY KEY,
        session_id              TEXT UNIQUE NOT NULL,
        page_url                TEXT,
        email                   TEXT,
        website                 TEXT,
        sell_to                 TEXT,
        first_name              TEXT,
        last_name               TEXT,
        phone                   TEXT,
        company                 TEXT,
        hear_about_us           TEXT,
        utm_source              TEXT,
        utm_medium              TEXT,
        utm_campaign            TEXT,
        utm_content             TEXT,
        referrer                TEXT,
        prefill_source          TEXT,
        fbc                     TEXT,
        fbp                     TEXT,
        landing_page            TEXT,
        previous_page           TEXT,
        utm_term                TEXT,
        enriched_title          TEXT,
        enriched_company_size   TEXT,
        enriched_industry       TEXT,
        enriched_linkedin       TEXT,
        enriched_city           TEXT,
        enriched_state          TEXT,
        enriched_country        TEXT,
        enriched_seniority      TEXT,
        enriched_departments    TEXT,
        enriched_email_status   TEXT,
        enriched_founded_year   TEXT,
        enriched_annual_revenue TEXT,
        enriched_funding_events TEXT,
        enriched_alexa_ranking  TEXT,
        enriched_keywords       TEXT,
        enriched_org_hq         TEXT,
        enriched_total_funding  TEXT,
        enriched_funding_stage  TEXT,
        disqualified            BOOLEAN DEFAULT FALSE,
        disqualified_reason     TEXT,
        step_reached            INT DEFAULT 1,
        completed               BOOLEAN DEFAULT FALSE,
        submitted_at            TIMESTAMPTZ,
        booking_uid             TEXT,
        start_time              TEXT,
        end_time                TEXT,
        event_type              TEXT,
        booked_at               TIMESTAMPTZ,
        loops_sent              BOOLEAN DEFAULT FALSE,
        created_at              TIMESTAMPTZ DEFAULT NOW(),
        updated_at              TIMESTAMPTZ DEFAULT NOW()
      )
    `);

    const migrations = [
      `ALTER TABLE gw_form_leads ADD COLUMN IF NOT EXISTS disqualified BOOLEAN DEFAULT FALSE`,
      `ALTER TABLE gw_form_leads ADD COLUMN IF NOT EXISTS disqualified_reason TEXT`,
      `ALTER TABLE gw_form_leads ADD COLUMN IF NOT EXISTS submitted_at TIMESTAMPTZ`,
      `ALTER TABLE gw_form_leads ADD COLUMN IF NOT EXISTS booking_uid TEXT`,
      `ALTER TABLE gw_form_leads ADD COLUMN IF NOT EXISTS start_time TEXT`,
      `ALTER TABLE gw_form_leads ADD COLUMN IF NOT EXISTS end_time TEXT`,
      `ALTER TABLE gw_form_leads ADD COLUMN IF NOT EXISTS event_type TEXT`,
      `ALTER TABLE gw_form_leads ADD COLUMN IF NOT EXISTS booked_at TIMESTAMPTZ`,
      `ALTER TABLE gw_form_leads ADD COLUMN IF NOT EXISTS page_url TEXT`,
      `ALTER TABLE gw_form_leads ADD COLUMN IF NOT EXISTS enriched_title TEXT`,
      `ALTER TABLE gw_form_leads ADD COLUMN IF NOT EXISTS enriched_company_size TEXT`,
      `ALTER TABLE gw_form_leads ADD COLUMN IF NOT EXISTS enriched_industry TEXT`,
      `ALTER TABLE gw_form_leads ADD COLUMN IF NOT EXISTS enriched_linkedin TEXT`,
      `ALTER TABLE gw_form_leads ADD COLUMN IF NOT EXISTS loops_sent BOOLEAN DEFAULT FALSE`,
      `ALTER TABLE gw_form_leads ADD COLUMN IF NOT EXISTS enriched_city TEXT`,
      `ALTER TABLE gw_form_leads ADD COLUMN IF NOT EXISTS enriched_state TEXT`,
      `ALTER TABLE gw_form_leads ADD COLUMN IF NOT EXISTS enriched_country TEXT`,
      `ALTER TABLE gw_form_leads ADD COLUMN IF NOT EXISTS enriched_seniority TEXT`,
      `ALTER TABLE gw_form_leads ADD COLUMN IF NOT EXISTS enriched_departments TEXT`,
      `ALTER TABLE gw_form_leads ADD COLUMN IF NOT EXISTS enriched_email_status TEXT`,
      `ALTER TABLE gw_form_leads ADD COLUMN IF NOT EXISTS enriched_founded_year TEXT`,
      `ALTER TABLE gw_form_leads ADD COLUMN IF NOT EXISTS enriched_annual_revenue TEXT`,
      `ALTER TABLE gw_form_leads ADD COLUMN IF NOT EXISTS enriched_funding_events TEXT`,
      `ALTER TABLE gw_form_leads ADD COLUMN IF NOT EXISTS enriched_alexa_ranking TEXT`,
      `ALTER TABLE gw_form_leads ADD COLUMN IF NOT EXISTS enriched_keywords TEXT`,
      `ALTER TABLE gw_form_leads ADD COLUMN IF NOT EXISTS enriched_org_hq TEXT`,
      `ALTER TABLE gw_form_leads ADD COLUMN IF NOT EXISTS enriched_total_funding TEXT`,
      `ALTER TABLE gw_form_leads ADD COLUMN IF NOT EXISTS enriched_funding_stage TEXT`,
      `ALTER TABLE gw_form_leads ADD COLUMN IF NOT EXISTS fbc TEXT`,
      `ALTER TABLE gw_form_leads ADD COLUMN IF NOT EXISTS fbp TEXT`,
      `ALTER TABLE gw_form_leads ADD COLUMN IF NOT EXISTS landing_page TEXT`,
      `ALTER TABLE gw_form_leads ADD COLUMN IF NOT EXISTS utm_term TEXT`,
      `ALTER TABLE gw_form_leads ADD COLUMN IF NOT EXISTS previous_page TEXT`,
    ];

    for (const sql of migrations) {
      await awsPool.query(sql);
    }

    console.log('[AWS] gw_form_leads table ready');
  } catch (err) {
    console.warn('[AWS] Table init failed (non-blocking):', err.message);
  }
}

function syncToAWS(data) {
  if (!awsPool) return;
  awsPool.query(`
    INSERT INTO gw_form_leads
      (session_id, page_url,
       email, website, sell_to,
       first_name, last_name, phone, company, hear_about_us,
       utm_source, utm_medium, utm_campaign, utm_content,
       referrer, prefill_source,
       fbc, fbp, landing_page, previous_page, utm_term,
       enriched_title, enriched_company_size, enriched_industry, enriched_linkedin,
       enriched_city, enriched_state, enriched_country,
       enriched_seniority, enriched_departments, enriched_email_status,
       enriched_founded_year, enriched_annual_revenue,
       enriched_funding_events, enriched_alexa_ranking, enriched_keywords,
       enriched_org_hq, enriched_total_funding, enriched_funding_stage,
       disqualified, disqualified_reason,
       step_reached, completed, submitted_at, loops_sent, updated_at)
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34,$35,$36,$37,$38,$39,$40,$41,$42,$43,$44,$45,NOW())
    ON CONFLICT (session_id) DO UPDATE SET
      page_url                = COALESCE(EXCLUDED.page_url,                gw_form_leads.page_url),
      email                   = COALESCE(EXCLUDED.email,                   gw_form_leads.email),
      website                 = COALESCE(EXCLUDED.website,                 gw_form_leads.website),
      sell_to                 = COALESCE(EXCLUDED.sell_to,                 gw_form_leads.sell_to),
      first_name              = COALESCE(EXCLUDED.first_name,              gw_form_leads.first_name),
      last_name               = COALESCE(EXCLUDED.last_name,              gw_form_leads.last_name),
      phone                   = COALESCE(EXCLUDED.phone,                   gw_form_leads.phone),
      company                 = COALESCE(EXCLUDED.company,                 gw_form_leads.company),
      hear_about_us           = COALESCE(EXCLUDED.hear_about_us,           gw_form_leads.hear_about_us),
      utm_source              = COALESCE(EXCLUDED.utm_source,              gw_form_leads.utm_source),
      utm_medium              = COALESCE(EXCLUDED.utm_medium,              gw_form_leads.utm_medium),
      utm_campaign            = COALESCE(EXCLUDED.utm_campaign,            gw_form_leads.utm_campaign),
      utm_content             = COALESCE(EXCLUDED.utm_content,             gw_form_leads.utm_content),
      referrer                = COALESCE(EXCLUDED.referrer,                gw_form_leads.referrer),
      prefill_source          = COALESCE(EXCLUDED.prefill_source,          gw_form_leads.prefill_source),
      fbc                     = COALESCE(EXCLUDED.fbc,                     gw_form_leads.fbc),
      fbp                     = COALESCE(EXCLUDED.fbp,                     gw_form_leads.fbp),
      landing_page            = COALESCE(EXCLUDED.landing_page,            gw_form_leads.landing_page),
      previous_page           = COALESCE(EXCLUDED.previous_page,           gw_form_leads.previous_page),
      utm_term                = COALESCE(EXCLUDED.utm_term,                gw_form_leads.utm_term),
      enriched_title          = COALESCE(EXCLUDED.enriched_title,          gw_form_leads.enriched_title),
      enriched_company_size   = COALESCE(EXCLUDED.enriched_company_size,   gw_form_leads.enriched_company_size),
      enriched_industry       = COALESCE(EXCLUDED.enriched_industry,       gw_form_leads.enriched_industry),
      enriched_linkedin       = COALESCE(EXCLUDED.enriched_linkedin,       gw_form_leads.enriched_linkedin),
      enriched_city           = COALESCE(EXCLUDED.enriched_city,           gw_form_leads.enriched_city),
      enriched_state          = COALESCE(EXCLUDED.enriched_state,          gw_form_leads.enriched_state),
      enriched_country        = COALESCE(EXCLUDED.enriched_country,        gw_form_leads.enriched_country),
      enriched_seniority      = COALESCE(EXCLUDED.enriched_seniority,      gw_form_leads.enriched_seniority),
      enriched_departments    = COALESCE(EXCLUDED.enriched_departments,    gw_form_leads.enriched_departments),
      enriched_email_status   = COALESCE(EXCLUDED.enriched_email_status,   gw_form_leads.enriched_email_status),
      enriched_founded_year   = COALESCE(EXCLUDED.enriched_founded_year,   gw_form_leads.enriched_founded_year),
      enriched_annual_revenue = COALESCE(EXCLUDED.enriched_annual_revenue, gw_form_leads.enriched_annual_revenue),
      enriched_funding_events = COALESCE(EXCLUDED.enriched_funding_events, gw_form_leads.enriched_funding_events),
      enriched_alexa_ranking  = COALESCE(EXCLUDED.enriched_alexa_ranking,  gw_form_leads.enriched_alexa_ranking),
      enriched_keywords       = COALESCE(EXCLUDED.enriched_keywords,       gw_form_leads.enriched_keywords),
      enriched_org_hq         = COALESCE(EXCLUDED.enriched_org_hq,         gw_form_leads.enriched_org_hq),
      enriched_total_funding  = COALESCE(EXCLUDED.enriched_total_funding,  gw_form_leads.enriched_total_funding),
      enriched_funding_stage  = COALESCE(EXCLUDED.enriched_funding_stage,  gw_form_leads.enriched_funding_stage),
      disqualified            = EXCLUDED.disqualified,
      disqualified_reason     = COALESCE(EXCLUDED.disqualified_reason,     gw_form_leads.disqualified_reason),
      step_reached            = GREATEST(EXCLUDED.step_reached,            gw_form_leads.step_reached),
      completed               = (COALESCE(gw_form_leads.completed, false) OR COALESCE(EXCLUDED.completed, false)),
      submitted_at            = COALESCE(EXCLUDED.submitted_at,            gw_form_leads.submitted_at),
      loops_sent              = COALESCE(EXCLUDED.loops_sent,              gw_form_leads.loops_sent),
      updated_at              = NOW()
  `, [
    data.session_id,                        data.page_url                  || null,
    data.email                   || null,   data.website                   || null,
    data.sell_to                 || null,   data.first_name                || null,
    data.last_name               || null,   data.phone                     || null,
    data.company                 || null,   data.hear_about_us             || null,
    data.utm_source              || null,   data.utm_medium                || null,
    data.utm_campaign            || null,   data.utm_content               || null,
    data.referrer                || null,   data.prefill_source            || null,
    data.fbc                     || null,   data.fbp                       || null,
    data.landing_page            || null,   data.previous_page             || null,
    data.utm_term                || null,   data.enriched_title            || null,
    data.enriched_company_size   || null,   data.enriched_industry         || null,
    data.enriched_linkedin       || null,   data.enriched_city             || null,
    data.enriched_state          || null,   data.enriched_country          || null,
    data.enriched_seniority      || null,   data.enriched_departments      || null,
    data.enriched_email_status   || null,   data.enriched_founded_year     || null,
    data.enriched_annual_revenue || null,   data.enriched_funding_events   || null,
    data.enriched_alexa_ranking  || null,   data.enriched_keywords         || null,
    data.enriched_org_hq         || null,   data.enriched_total_funding    || null,
    data.enriched_funding_stage  || null,   data.disqualified              ?? false,
    data.disqualified_reason     || null,   data.step_reached              || 1,
    data.completed               || false,  data.completed ? new Date() : null,
    data.loops_sent              || false
  ]).then(() => {
    console.log(`[AWS] ✅ Synced session ${data.session_id}`);
  }).catch(err => {
    console.warn(`[AWS] ⚠ Sync failed for ${data.session_id}:`, err.message); recordFailure('AWS sync', data.session_id, err.message);
  });
}

function syncBookingToAWS(session_id, booking_uid, start_time, end_time, event_type) {
  if (!awsPool) return;
  awsPool.query(`
    UPDATE gw_form_leads SET booking_uid=$2, start_time=$3, end_time=$4, event_type=$5, booked_at=NOW(), completed=true, updated_at=NOW()
    WHERE session_id = $1
  `, [session_id, booking_uid, start_time || null, end_time || null, event_type || null])
  .then(() => console.log(`[AWS] ✅ Booking synced for session ${session_id}`))
  .catch(err => { console.warn(`[AWS] ⚠ Booking sync failed:`, err.message); recordFailure('AWS sync', 'booking sync', err.message); });
}

function sendSlack(blocks, fallbackText) {
  const webhookUrl = process.env.SLACK_WEBHOOK_URL;
  if (!webhookUrl) { console.warn('[Slack] SLACK_WEBHOOK_URL not set — skipping'); alertSlackBroken('SLACK_WEBHOOK_URL is not configured'); return; }
  const cleanBlocks = Array.isArray(blocks) ? blocks.filter(Boolean) : null;
  const payload = cleanBlocks && cleanBlocks.length > 0
    ? { text: fallbackText || 'Gushwork notification', blocks: cleanBlocks }
    : { text: fallbackText || 'Gushwork notification' };
  fetch(webhookUrl, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload) })
  .then(r => r.text().then(t => console.log(`[Slack] ✅ Sent — status: ${r.status} | response: ${t.substring(0, 50)}`)))
  .catch(err => { console.warn('[Slack] ⚠ Failed:', err.message); alertSlackBroken(err.message); });
}
function bHeader(text)  { return { type: 'header', text: { type: 'plain_text', text, emoji: true } }; }
function bSection(text) { return { type: 'section', text: { type: 'mrkdwn', text } }; }
function bFields(fields) {
  const filtered = fields.filter(f => f.value);
  if (!filtered.length) return null;
  return { type: 'section', fields: filtered.map(f => ({ type: 'mrkdwn', text: `*${f.label}*\n${f.value}` })) };
}
function bDivider() { return { type: 'divider' }; }
function bContext(text) { return { type: 'context', elements: [{ type: 'mrkdwn', text }] }; }

/* ─────────────────────────────────────────────────────────────────
   OPS ALERTING
   Generic on purpose: ELV is the first consumer, but Apollo, Meta
   CAPI, Salesforce, AWS sync and the cron can each be wired in with
   a one-line alertOps() call in their existing catch blocks.

   Routing (per team decision): Slack gets EVERY alert; email gets
   CRITICAL only, so the inbox stays meaningful.

   Cooldowns are in-memory and keyed per source+title, so a flapping
   dependency can't spam either channel. They reset on redeploy —
   fine for monitoring, and worth knowing: a freshly deployed
   instance starts with a clean slate. Multiple instances would each
   keep their own window.
   ───────────────────────────────────────────────────────────────── */

/* ── Website verification gate for Meta CAPI ────────────────────────
   Per team decision: Meta events fire ONLY when the website check
   definitively PASSED. Anything else — failed, parked, nonexistent,
   DNS unreachable, or simply "we couldn't check" — suppresses Meta.

   Previously only website_check_failed===true suppressed, so the
   indeterminate cases (DNS timeout / SERVFAIL / backend hiccup) fired
   Meta and looked identical to a clean lead. prestigelending.com hit
   exactly that path: its nameservers never answered, so we failed
   open, and the lead reached Meta with no flag anywhere.

   NULL/absent reason means the lead predates this field — those still
   fire, so we never retroactively suppress historic leads.
   ─────────────────────────────────────────────────────────────────── */
// social_profile_url deliberately EXCLUDED: those leads convert poorly, so
// sending Meta a Lead event for them trains the algorithm to find more of the
// same. They still pass the form and are tagged for visibility.
// v5.3.0 — new PASS reasons from the redirect-aware website check:
//   forwarded_to_live_site  domain 301s to a DIFFERENT domain that has real
//                           content. A legitimate forward (afgmmoving.com ->
//                           afewgoodmenmoving.com), never a parked domain.
//   live_despite_dns_hint   DNS said "parking infrastructure" but the page has
//                           real substance. Content is ground truth; DNS is a
//                           prior. This is what stops a wrong IP in the hint
//                           list from ever suppressing a real lead again.
//   thin_content /          Page has little/no server-rendered substance. Kept
//   thin_content_wildcard   VERIFIED on purpose: client-rendered SPAs look
//                           identical to parked pages over HTTP, so this is
//                           surfaced for humans in Slack/monitor but must not
//                           cost a real lead its Meta event.
//   nxdomain_contradicted   Website domain == the lead's own verified email
//                           domain, so the domain provably exists and the
//                           NXDOMAIN was a resolver blip.
const WEBSITE_VERIFIED_REASONS = [
  'resolved', 'mx_only', 'content_clean', 'test_email_skipped', 'ok',
  'forwarded_to_live_site', 'live_despite_dns_hint',
  'thin_content', 'thin_content_wildcard', 'nxdomain_contradicted',
];

// Reasons that mean "we looked and it is genuinely not a company website".
// These set website_check_failed and suppress Meta. They do NOT block the
// form — blocking is form.js's WEBSITE_BLOCKING_REASONS, unchanged.
const WEBSITE_NEGATIVE_REASONS = ['for_sale_lander', 'marketplace_redirect', 'parked_confirmed'];

function isWebsiteVerified(row) {
  if (!row) return true;
  if (row.website_check_failed === true) return false;
  const reason = row.website_check_reason;
  if (reason === null || reason === undefined || reason === '') return true; // pre-feature rows
  return WEBSITE_VERIFIED_REASONS.includes(reason);
}

function websiteCheckNote(row) {
  if (!row) return '';
  if (row.website_check_failed === true) return row.website_check_reason || 'failed';
  if (!isWebsiteVerified(row)) return 'unverified (' + (row.website_check_reason || 'unknown') + ')';
  return '';
}

const ALERT_COOLDOWN_MS   = { critical: 3 * 60 * 60 * 1000, warning: 60 * 60 * 1000 };
const ALERT_EMAIL_TO      = ['darshil.dixit@gushwork.ai', 'darshil@darshildixit.com'];
const _alertLastSent      = new Map(); // `${severity}:${source}:${title}` -> ms

function sendOpsSlack(blocks, fallbackText) {
  // Falls back to the main webhook if the alerts one isn't configured,
  // so alerts are never silently lost just because an env var is missing.
  const webhookUrl = process.env.SLACK_ALERTS_WEBHOOK_URL || process.env.SLACK_WEBHOOK_URL;
  if (!webhookUrl) { console.warn('[alertOps] No Slack webhook configured — alert not sent'); return; }
  const cleanBlocks = Array.isArray(blocks) ? blocks.filter(Boolean) : null;
  const payload = cleanBlocks && cleanBlocks.length > 0
    ? { text: fallbackText || 'Gushwork alert', blocks: cleanBlocks }
    : { text: fallbackText || 'Gushwork alert' };
  fetch(webhookUrl, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload) })
    .then(r => console.log(`[alertOps] ✅ Slack sent — status: ${r.status}`))
    .catch(err => console.warn('[alertOps] ⚠ Slack failed:', err.message));
}

async function sendAlertEmail(subject, details) {
  if (!process.env.GMAIL_USER || !process.env.GMAIL_APP_PASSWORD) {
    console.warn('[alertOps] GMAIL credentials not set — alert email skipped');
    return;
  }
  const lines = Object.entries(details || {}).map(([k, v]) => `${k}: ${v ?? '—'}`).join('\n');
  try {
    const transport = getGmailTransport();
    const result = await transport.sendMail({
      from:    `"Gushwork Alerts" <${process.env.GMAIL_USER}>`,
      to:      ALERT_EMAIL_TO.join(', '),
      subject,
      text:    `${subject}\n\n${lines}\n\nTime: ${new Date().toISOString()}`,
    });
    console.log(`[alertOps] ✅ Alert email sent | messageId: ${result.messageId}`);
  } catch (err) {
    console.warn('[alertOps] ⚠ Alert email failed:', err.message);
  }
}

const _alertSuppressed = new Map();        // same key -> count hidden by cooldown
const _alertSuppressedIds = new Map();     // same key -> identifiers hidden by cooldown

function alertOps(severity, source, title, details) {
  // Fully defensive: alerting must NEVER throw into a request path.
  try {
    const sev  = severity === 'critical' ? 'critical' : 'warning';
    const key  = `${sev}:${source}:${title}`;
    const now  = Date.now();
    const last = _alertLastSent.get(key) || 0;
    if (now - last < ALERT_COOLDOWN_MS[sev]) {
      // Still inside cooldown — stay quiet, but remember it happened so the
      // next alert can report the true scale rather than hiding it.
      _alertSuppressed.set(key, (_alertSuppressed.get(key) || 0) + 1);
      // Remember the identifier too, so the next alert can name what was missed
      const idVal = details && (details.Email || details.Recipient || details.Session || details.Affected);
      if (idVal) {
        const ids = _alertSuppressedIds.get(key) || [];
        if (ids.length < 25) ids.push(String(idVal));
        _alertSuppressedIds.set(key, ids);
      }
      return false;
    }
    _alertLastSent.set(key, now);

    const hidden    = _alertSuppressed.get(key) || 0;
    const hiddenIds = _alertSuppressedIds.get(key) || [];
    _alertSuppressed.set(key, 0);
    _alertSuppressedIds.set(key, []);

    const icon = sev === 'critical' ? '🚨' : '⚠️';
    const heading = `${icon} ${source} — ${title}`;
    const fieldList = Object.entries(details || {}).map(([label, value]) => ({ label, value: value == null ? '' : String(value) }));
    if (hidden > 0) {
      fieldList.push({ label: 'Also occurred', value: `${hidden} more time(s) since the last alert` });
      if (hiddenIds.length > 0) fieldList.push({ label: 'Also affected', value: hiddenIds.join(', ').substring(0, 900) });
    }

    const blocks = [bHeader(heading), bDivider()];
    const f = bFields(fieldList);
    if (f) blocks.push(f);
    blocks.push(bContext(`Severity: *${sev}* · ${new Date().toISOString()}`));
    sendOpsSlack(blocks, heading);

    if (sev === 'critical') sendAlertEmail(heading, Object.assign({}, details, hidden > 0 ? { 'Also occurred': `${hidden} more time(s)` } : {})).catch(() => {});
    console.log(`[alertOps] ${icon} ${source} — ${title}${hidden > 0 ? ` (+${hidden} suppressed)` : ''}`);
    return true;
  } catch (err) {
    console.warn('[alertOps] internal error (ignored):', err && err.message);
    return false;
  }
}

/* ── Slack-failure escalation ───────────────────────────────────────
   Alerting about a broken Slack VIA Slack is circular, so this path is
   email-only. Separate cooldown so it can't be starved by other alerts.
   ─────────────────────────────────────────────────────────────────── */
let _slackFailureLastAlert = 0;
function alertSlackBroken(detail) {
  try {
    const now = Date.now();
    if (now - _slackFailureLastAlert < 3 * 60 * 60 * 1000) return;
    _slackFailureLastAlert = now;
    console.warn('[alertOps] 🚨 Slack delivery broken — escalating by email');
    sendAlertEmail('🚨 Slack — lead notifications are failing', {
      'Detail': detail,
      'Impact': 'Lead notifications are not reaching Slack. Leads are still being saved and pushed to Salesforce.',
      'Action': 'Check the Slack webhook URL / app installation.',
    }).catch(() => {});
  } catch (err) {
    console.warn('[alertOps] slack-escalation error (ignored):', err && err.message);
  }
}

/* ── PHASE 2: rate-based monitoring ────────────────────────────────
   Individual failures for these are normal noise; a sustained RATE is
   the real signal. Same rolling-window idea already proven on ELV.
   ─────────────────────────────────────────────────────────────────── */
const FAILURE_MONITORS = {
  'Meta CAPI': { alertAfter: 5, impact: 'Conversion events are not reaching Meta, so ad optimisation is degrading.' },
  'Apollo':    { alertAfter: 5, impact: 'These leads were saved without enrichment data.' },
  'AWS sync':  { alertAfter: 5, impact: 'The AWS mirror database is drifting out of sync with Railway.' },
  'Email':     { alertAfter: 5, impact: 'Drop-off follow-up emails are not being delivered. If this is all of them, the Gmail connection is likely broken.' },
};
const FAILURE_BUFFER_TTL_MS = 6 * 60 * 60 * 1000; // stale failures expire, so a slow trickle never accumulates
const _failBuffers = new Map(); // source -> [{ id, error, at }]

// Collect failures per integration and alert once a threshold is reached,
// listing exactly WHICH items failed. Deliberately count-based rather than
// percentage-based: "5 failures, here they are" is actionable, "40% of a
// rolling window" is not.
function recordFailure(source, id, error) {
  try {
    const cfg = FAILURE_MONITORS[source];
    if (!cfg) return;
    const now = Date.now();
    let buf = (_failBuffers.get(source) || []).filter(x => now - x.at < FAILURE_BUFFER_TTL_MS);
    buf.push({ id: id || 'unknown', error: String(error || '').substring(0, 120), at: now });
    _failBuffers.set(source, buf);
    if (buf.length >= cfg.alertAfter) {
      const sent = alertOps('warning', source, 'Repeated failures', {
        'Count': `${buf.length} in the last ${Math.round(FAILURE_BUFFER_TTL_MS / 3600000)} hours`,
        'Affected': buf.map(x => x.id).join(', ').substring(0, 900),
        'Last error': buf[buf.length - 1].error,
        'Impact': cfg.impact,
      });
      // Clear ONLY if the alert actually went out. If it was suppressed by
      // the cooldown, keep accumulating so nothing is lost — the next alert
      // then reports the true total rather than just the latest five.
      if (sent) _failBuffers.set(source, []);
    }
  } catch (err) {
    console.warn('[recordFailure] error (ignored):', err && err.message);
  }
}

/* ── PHASE 3: heartbeat ────────────────────────────────────────────
   Some failures produce no error at all — the recovery cron simply
   stops being called, or the form stops sending leads. Nothing throws,
   so catch blocks can't see it. These timestamps + a periodic self
   check are the only way to notice an ABSENCE of activity.
   In-memory, so they reset on redeploy (a fresh instance starts clean).
   ─────────────────────────────────────────────────────────────────── */
let _lastLeadAt    = Date.now();
let _lastCronRunAt = Date.now();
const HEARTBEAT_CHECK_MS   = 30 * 60 * 1000;      // self-check cadence
const CRON_STALE_MS        = 3 * 60 * 60 * 1000;  // cron should run well within 3h
const NO_LEADS_STALE_MS    = 12 * 60 * 60 * 1000; // 12h with zero leads is anomalous

function startHeartbeat() {
  setInterval(() => {
    try {
      const now = Date.now();
      const cronAge = now - _lastCronRunAt;
      if (cronAge > CRON_STALE_MS) {
        alertOps('critical', 'Recovery cron', 'Has not run recently', {
          'Last run': `${Math.round(cronAge / 60000)} minutes ago`,
          'Impact': 'Drop-off follow-up emails are not being sent.',
          'Action': 'Check the scheduler that calls POST /cron/send-partials.',
        });
      }
      const leadAge = now - _lastLeadAt;
      if (leadAge > NO_LEADS_STALE_MS) {
        alertOps('critical', 'Form', 'No leads received', {
          'Last lead': `${Math.round(leadAge / 3600000)} hours ago`,
          'Impact': 'The form may be broken, or the script may not be loading.',
          'Action': 'Open /demo and check the browser console for the init banner.',
        });
      }
    } catch (err) {
      console.warn('[heartbeat] error (ignored):', err && err.message);
    }
  }, HEARTBEAT_CHECK_MS);
  console.log('[heartbeat] ✅ Monitoring started (cron staleness + lead flow)');
}

/* ── PHASE 3: startup configuration audit ──────────────────────────
   A missing env var silently disables a whole integration. One check
   at boot beats discovering it weeks later.
   ─────────────────────────────────────────────────────────────────── */
function auditStartupConfig() {
  try {
    const required = {
      ELV_API_KEY:              'Email verification',
      SLACK_WEBHOOK_URL:        'Lead notifications',
      GMAIL_USER:               'Drop-off follow-up emails',
      GMAIL_APP_PASSWORD:       'Drop-off follow-up emails',
      SLACK_ALERTS_WEBHOOK_URL: 'Ops alerts',
    };
    const missing = Object.entries(required).filter(([k]) => !process.env[k]).map(([k, v]) => `${k} (${v})`);
    if (missing.length > 0) {
      console.warn('[Startup] ⚠ Missing configuration:', missing.join(', '));
      alertOps('critical', 'Startup', 'Configuration incomplete', {
        'Missing': missing.join(', '),
        'Impact': 'The listed features are silently disabled.',
      });
    } else {
      console.log('[Startup] ✅ All monitored environment variables present');
    }
  } catch (err) {
    console.warn('[Startup] audit error (ignored):', err && err.message);
  }
}


function buildEnrichmentBlocks(blocks, e) {
  const hasPersonInfo = e.enriched_title || e.enriched_seniority || e.enriched_departments || e.enriched_email_status;
  const hasOrgInfo    = e.enriched_company_size || e.enriched_industry || e.enriched_founded_year || e.enriched_annual_revenue || e.enriched_alexa_ranking || e.enriched_keywords;
  const hasFunding    = e.enriched_funding_events || e.enriched_total_funding || e.enriched_funding_stage;
  const hasLocation   = e.enriched_city || e.enriched_state || e.enriched_country;
  if (!hasPersonInfo && !hasOrgInfo && !hasFunding && !hasLocation && !e.enriched_org_hq && !e.enriched_linkedin) return;
  blocks.push(bDivider()); blocks.push(bSection('*🔍 Enrichment*'));
  if (hasPersonInfo) { const f = bFields([{label:'Title',value:e.enriched_title},{label:'Seniority',value:e.enriched_seniority},{label:'Department',value:e.enriched_departments},{label:'Email Status',value:e.enriched_email_status}]); if(f) blocks.push(f); }
  if (hasOrgInfo)    { const f = bFields([{label:'Company Size',value:e.enriched_company_size},{label:'Industry',value:e.enriched_industry},{label:'Founded',value:e.enriched_founded_year},{label:'Annual Revenue',value:e.enriched_annual_revenue},{label:'Alexa Rank',value:e.enriched_alexa_ranking},{label:'Keywords',value:e.enriched_keywords}]); if(f) blocks.push(f); }
  if (hasFunding)    { blocks.push(bDivider()); const f = bFields([{label:'💰 Total Funding',value:e.enriched_total_funding},{label:'Funding Stage',value:e.enriched_funding_stage},{label:'Funding Events',value:e.enriched_funding_events}]); if(f) blocks.push(f); }
  if (hasLocation)   { const loc = [e.enriched_city,e.enriched_state,e.enriched_country].filter(Boolean).join(', '); const f = bFields([{label:'📍 Person Location',value:loc}]); if(f) blocks.push(f); }
  if (e.enriched_org_hq)  { const f = bFields([{label:'🏢 Company HQ',value:e.enriched_org_hq}]); if(f) blocks.push(f); }
  if (e.enriched_linkedin) { const f = bFields([{label:'LinkedIn',value:e.enriched_linkedin}]); if(f) blocks.push(f); }
}

function buildJourneyBlocks(blocks, d) {
  const hasAttribution = d.utm_source || d.utm_medium || d.utm_campaign || d.utm_content || d.referrer;
  const hasJourney     = d.landing_page || d.previous_page || d.page_url;

  if (!hasAttribution && !hasJourney) return;

  blocks.push(bDivider());
  blocks.push(bSection('*📊 Attribution & Journey*'));

  if (hasAttribution) {
    const src = [d.utm_source, d.utm_medium].filter(Boolean).join(' / ');
    const f = bFields([
      { label: 'Source',   value: src              },
      { label: 'Campaign', value: d.utm_campaign   },
      { label: 'Content',  value: d.utm_content    },
      { label: 'Referrer', value: d.referrer       },
      { label: 'Prefill',  value: d.prefill_source },
    ]);
    if (f) blocks.push(f);
  }

  if (hasJourney) {
    const parts = [
      d.landing_page  ? `🛬 *Landing:* ${d.landing_page}`   : null,
      d.previous_page ? `⬅️ *Previous:* ${d.previous_page}` : null,
      d.page_url      ? `📄 *Form:* ${d.page_url}`          : null,
    ].filter(Boolean).join('\n');
    blocks.push(bContext(parts));
  }
}

function slackPartial(d) {
  if (d.disqualified) {
    console.log(`[Slack] ⏭ Skipping partial notification for disqualified lead: ${d.email}`);
    return;
  }
  const label = d.completed ? '⏰ Completed Form — Did Not Book' : '👻 Dropped at Step 1';
  const blocks = [];
  blocks.push(bHeader(label));
  blocks.push(bDivider());
  const lf = bFields([
    { label: '📧 Email',   value: d.email    },
    { label: '🎯 Sells to', value: d.sell_to },
    { label: '🏢 Company', value: d.company  },
    { label: '🌐 Website', value: d.website  },
  ]);
  if (lf) blocks.push(lf);
  buildEnrichmentBlocks(blocks, d);
  buildJourneyBlocks(blocks, d);
  sendSlack(blocks, label);
}

function slackSubmit(d) {
  const name = [d.first_name, d.last_name].filter(Boolean).join(' ');
  const blocks = [];
  blocks.push(bHeader(d.website_check_failed ? '⚠️ Lead Form Completed — Website Check Failed' : '✅ Lead Form Completed'));
  blocks.push(bDivider());
  const lf = bFields([
    { label: '👤 Name',             value: name            },
    { label: '📧 Email',            value: d.email         },
    { label: '📞 Phone',            value: d.phone         },
    { label: '🏢 Company',          value: d.company       },
    { label: '🌐 Website',          value: d.website       },
    { label: '🎯 Sells to',         value: d.sell_to       },
    { label: '💬 Heard about us',   value: d.hear_about_us },
  ]);
  if (lf) blocks.push(lf);
  if (d.website_check_failed) {
    const wf = bFields([{ label: '⚠️ Website check', value: d.website_check_reason || 'failed' }]);
    if (wf) blocks.push(wf);
  } else if (d.website_check_reason && !WEBSITE_VERIFIED_REASONS.includes(d.website_check_reason) && d.website_check_reason !== 'social_profile_url') {
    const uf = bFields([{ label: '❓ Website check', value: 'Could not verify (' + d.website_check_reason + ')' }]);
    if (uf) blocks.push(uf);
  } else if (d.website_check_reason === 'social_profile_url') {
    const sf = bFields([{ label: '🔗 Website', value: 'Social profile (no company site)' }]);
    if (sf) blocks.push(sf);
  }
  buildEnrichmentBlocks(blocks, d);
  buildJourneyBlocks(blocks, d);
  sendSlack(blocks, `${d.website_check_failed ? '⚠️ Lead Form Completed (website check failed)' : '✅ Lead Form Completed'} — ${d.email}`);
}

const nodemailer = require('nodemailer');

let _gmailTransport = null;
function getGmailTransport() {
  if (_gmailTransport) return _gmailTransport;
  _gmailTransport = nodemailer.createTransport({
    service: 'gmail',
    auth: {
      user: process.env.GMAIL_USER,
      pass: process.env.GMAIL_APP_PASSWORD,
    },
  });
  return _gmailTransport;
}

async function sendFollowUpEmail(email, firstName) {
  if (!process.env.GMAIL_USER || !process.env.GMAIL_APP_PASSWORD) {
    console.warn('[Email] GMAIL credentials not set — skipping');
    alertOps('critical', 'Email', 'Credentials not configured', { 'Impact': 'No drop-off follow-up emails are being sent at all.' });
    return;
  }
  if (!email) return;

  const name    = firstName || 'there';
  const subject = 'Re: Gushwork Demo';
  const text    = `Hey ${name}, Swapnil from Gushwork here. I saw you filled out the form to book a call with us but didn't end up finding a time to talk.\n\nWere there no available times for you?`;

  try {
    const transport = getGmailTransport();
    const result    = await transport.sendMail({
      from:    `"Swapnil from Gushwork" <${process.env.GMAIL_USER}>`,
      to:      email,
      subject,
      text,
    });
    console.log(`[Email] ✅ Follow-up sent to ${email} | messageId: ${result.messageId}`);
  } catch (err) {
    console.warn(`[Email] ⚠ Failed to send to ${email}:`, err.message);
    recordFailure('Email', email, err.message);
  }
}

function formatRevenue(amount) {
  if (!amount) return null; const n = parseFloat(amount);
  if (isNaN(n)) return amount.toString();
  if (n >= 1_000_000_000) return `$${(n/1_000_000_000).toFixed(1)}B USD`;
  if (n >= 1_000_000)     return `$${(n/1_000_000).toFixed(1)}M USD`;
  if (n >= 1_000)         return `$${(n/1_000).toFixed(0)}K USD`;
  return `$${n} USD`;
}

app.get('/health', (req, res) => { res.json({ status: 'ok', timestamp: new Date().toISOString() }); });

app.get('/monitor/metrics', async (req, res) => {
  const token = process.env.MONITOR_TOKEN;
  if (token && req.query.token !== token) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  try {
    const [totals, people, recovered, byDay, enrichCount, enrichCoverage, pendingPartials, noBooking, recent, today] = await Promise.all([
      pool.query(`
        SELECT
          COUNT(*)                                                          AS total,
          COUNT(*) FILTER (WHERE completed = true)                          AS completed,
          COUNT(*) FILTER (WHERE booking_uid IS NOT NULL)                   AS booked,
          COUNT(*) FILTER (WHERE disqualified = true)                       AS disqualified,
          COUNT(*) FILTER (WHERE loops_sent = true)                         AS loops_sent,
          COUNT(*) FILTER (WHERE completed = true AND booking_uid IS NULL)  AS completed_no_booking_sessions
        FROM leads
      `),
      pool.query(`
        SELECT
          COUNT(DISTINCT LOWER(email))                                        AS people_total,
          COUNT(DISTINCT LOWER(email)) FILTER (WHERE completed = true)        AS people_completed,
          COUNT(DISTINCT LOWER(email)) FILTER (WHERE booking_uid IS NOT NULL) AS people_booked,
          COUNT(DISTINCT LOWER(email)) FILTER (WHERE disqualified = true)     AS people_disqualified
        FROM leads
        WHERE email IS NOT NULL
      `),
      pool.query(`
        SELECT COUNT(*) AS recovered FROM (
          SELECT LOWER(l.email) AS em
          FROM leads l
          WHERE l.email IS NOT NULL
            AND l.completed = true
            AND l.booking_uid IS NULL
            AND EXISTS (
              SELECT 1 FROM leads b
              WHERE LOWER(b.email) = LOWER(l.email)
                AND b.booking_uid IS NOT NULL
                AND COALESCE(b.booked_at, b.created_at) >= l.created_at
            )
          GROUP BY LOWER(l.email)
        ) x
      `),
      pool.query(`
        SELECT to_char(created_at AT TIME ZONE 'Asia/Kolkata', 'Mon DD') AS day_label,
               date_trunc('day', created_at AT TIME ZONE 'Asia/Kolkata') AS day,
               COUNT(*) AS count
        FROM leads
        WHERE created_at >= NOW() - INTERVAL '14 days'
        GROUP BY 1, 2
        ORDER BY 2 ASC
      `),
      pool.query(`SELECT COUNT(*) AS count FROM enrichment_data`),
      pool.query(`
        SELECT
          COUNT(*) AS total,
          COUNT(*) FILTER (WHERE enriched_title IS NOT NULL)         AS has_title,
          COUNT(*) FILTER (WHERE enriched_total_funding IS NOT NULL) AS has_funding,
          COUNT(*) FILTER (WHERE enriched_country IS NOT NULL)       AS has_location
        FROM enrichment_data
      `),
      pool.query(`
        SELECT COUNT(*) AS count
        FROM leads l
        WHERE l.email IS NOT NULL
          AND l.disqualified = false
          AND l.booking_uid IS NULL
          AND l.loops_sent = false
          AND l.created_at < NOW() - INTERVAL '2 hours'
          AND NOT EXISTS (
            SELECT 1 FROM leads booked
            WHERE LOWER(booked.email) = LOWER(l.email)
              AND booked.booking_uid IS NOT NULL
              AND booked.booked_at >= l.created_at
          )
      `),
      pool.query(`
        SELECT COUNT(*) AS count FROM (
          SELECT DISTINCT ON (LOWER(email)) email
          FROM leads
          WHERE completed = true
            AND booking_uid IS NULL
            AND disqualified = false
            AND sell_to ILIKE 'B2B%'
            AND NOT EXISTS (
              SELECT 1 FROM leads booked
              WHERE LOWER(booked.email) = LOWER(leads.email)
                AND booked.booking_uid IS NOT NULL
            )
          ORDER BY LOWER(email), created_at DESC
        ) deduped
      `),
      pool.query(`
        SELECT session_id, email, company, first_name, last_name,
               completed, booking_uid, disqualified, created_at, page_url
        FROM leads ORDER BY created_at DESC LIMIT 50
      `),
      pool.query(`SELECT COUNT(*) AS count FROM leads WHERE created_at >= NOW() - INTERVAL '24 hours'`)
    ]);

    const t = totals.rows[0];
    const total        = parseInt(t.total) || 0;
    const completed    = parseInt(t.completed) || 0;
    const booked       = parseInt(t.booked) || 0;
    const disqualified = parseInt(t.disqualified) || 0;
    const loopsSent    = parseInt(t.loops_sent) || 0;
    const completedNoBookingSessions = parseInt(t.completed_no_booking_sessions) || 0;

    const p = people.rows[0];
    const peopleTotal        = parseInt(p.people_total) || 0;
    const peopleCompleted    = parseInt(p.people_completed) || 0;
    const peopleBooked       = parseInt(p.people_booked) || 0;
    const peopleDisqualified = parseInt(p.people_disqualified) || 0;

    const recoveredBookings = parseInt(recovered.rows[0].recovered) || 0;
    const leadsByDay        = byDay.rows.map(r => ({ day_label: r.day_label, count: parseInt(r.count) || 0 }));

    const enriched     = parseInt(enrichCount.rows[0].count) || 0;
    const pending      = parseInt(pendingPartials.rows[0].count) || 0;
    const noBookingUid = parseInt(noBooking.rows[0].count) || 0;
    const todayCount   = parseInt(today.rows[0].count) || 0;

    const ec = enrichCoverage.rows[0];
    const ecTotal    = parseInt(ec.total) || 0;
    const titlePct   = ecTotal ? Math.round(parseInt(ec.has_title) / ecTotal * 100) : 0;
    const fundingPct = ecTotal ? Math.round(parseInt(ec.has_funding) / ecTotal * 100) : 0;
    const locPct     = ecTotal ? Math.round(parseInt(ec.has_location) / ecTotal * 100) : 0;

    res.json({
      total, completed, booked, disqualified, enriched, loopsSent,
      pendingPartials: pending, noBookingUid, todayCount, awsSynced: !!awsPool,
      enrichTitlePct: titlePct, enrichFundingPct: fundingPct, enrichLocationPct: locPct,
      completedNoBookingSessions,
      peopleTotal, peopleCompleted, peopleBooked, peopleDisqualified,
      peopleNoBooking: noBookingUid,
      recoveredBookings,
      leadsByDay,
      recentLeads: recent.rows, generatedAt: new Date().toISOString()
    });
  } catch (err) {
    console.error('[/monitor/metrics]', err.message);
    res.status(500).json({ error: 'Metrics query failed', detail: err.message });
  }
});

app.get('/monitor/duplicates', async (req, res) => {
  const token = process.env.MONITOR_TOKEN;
  if (token && req.query.token !== token) return res.status(401).json({ error: 'Unauthorized' });

  try {
    const result = await pool.query(`
      SELECT
        l.email,
        COUNT(*) AS session_count,
        MAX(CASE WHEN l.booking_uid IS NOT NULL THEN 1 ELSE 0 END) AS has_booking,
        MAX(CASE WHEN l.completed = true THEN 1 ELSE 0 END) AS has_completed,
        MIN(l.created_at) AS first_seen,
        MAX(l.created_at) AS last_seen,
        json_agg(json_build_object(
          'session_id', l.session_id,
          'created_at', l.created_at,
          'completed',  l.completed,
          'booking_uid', l.booking_uid,
          'booked_at',  l.booked_at,
          'sell_to',    l.sell_to,
          'step_reached', l.step_reached,
          'disqualified', l.disqualified,
          'page_url',   l.page_url
        ) ORDER BY l.created_at DESC) AS sessions
      FROM leads l
      WHERE l.email IS NOT NULL
      GROUP BY LOWER(l.email), l.email
      HAVING COUNT(*) > 1
      ORDER BY COUNT(*) DESC, MAX(l.created_at) DESC
    `);

    res.json({ total: result.rows.length, leads: result.rows });
  } catch (err) {
    console.error('[/monitor/duplicates]', err.message);
    res.status(500).json({ error: 'Duplicates query failed', detail: err.message });
  }
});

app.get('/monitor/leads', async (req, res) => {
  const token = process.env.MONITOR_TOKEN;
  if (token && req.query.token !== token) return res.status(401).json({ error: 'Unauthorized' });

  const page   = Math.max(1, parseInt(req.query.page) || 1);
  const limit  = 25;
  const offset = (page - 1) * limit;
  const stage      = req.query.stage      || 'all';
  const dateFrom   = req.query.dateFrom   || null;
  const dateTo     = req.query.dateTo     || null;
  const search     = req.query.search     || null;
  const sellTo     = req.query.sellTo     || null;
  const utmSource  = req.query.utmSource  || null;
  const hearAbout  = req.query.hearAbout  || null;
  const enrichment = req.query.enrichment || null;
  const websiteCheck = req.query.websiteCheck || 'all';
  const repeatAttempts = req.query.repeatAttempts || 'all';
  const format     = req.query.format     || 'json';

  const sortMap = {
    created_at: 'l.created_at',
    email:      'l.email',
    name:       'l.first_name',
    company:    'l.company',
    sell_to:    'l.sell_to'
  };
  const sortCol = sortMap[req.query.sort] || 'l.created_at';
  const sortDir = (req.query.dir === 'asc') ? 'ASC' : 'DESC';
  const orderBy = `ORDER BY ${sortCol} ${sortDir} NULLS LAST, l.created_at DESC`;

  let conditions = [];
  const params = [];

  if (stage === 'booked')       conditions.push('l.booking_uid IS NOT NULL');
  if (stage === 'completed')    conditions.push('l.completed = true AND l.booking_uid IS NULL');
  if (stage === 'step1')        conditions.push('l.completed = false AND l.disqualified = false');
  if (stage === 'disqualified') conditions.push('l.disqualified = true');

  if (sellTo === '__clarified') {
    // any lead that flipped B2C/Mixed -> B2B at the disqualified step
    conditions.push(`l.sell_to LIKE 'B2B (clarified from%'`);
  } else if (sellTo) { params.push(sellTo);    conditions.push(`l.sell_to = $${params.length}`); }
  if (utmSource) { params.push(utmSource); conditions.push(`l.utm_source = $${params.length}`); }
  if (hearAbout) { params.push(`%${hearAbout.toLowerCase()}%`); conditions.push(`LOWER(COALESCE(l.hear_about_us,'')) LIKE $${params.length}`); }

  if (enrichment === 'yes') {
    conditions.push(`(l.enriched_title IS NOT NULL OR l.enriched_company_size IS NOT NULL OR EXISTS (SELECT 1 FROM enrichment_data ee WHERE ee.session_id = l.session_id AND (ee.enriched_title IS NOT NULL OR ee.enriched_company_size IS NOT NULL OR ee.enriched_company IS NOT NULL)))`);
  }
  if (enrichment === 'no') {
    conditions.push(`(l.enriched_title IS NULL AND l.enriched_company_size IS NULL AND NOT EXISTS (SELECT 1 FROM enrichment_data ee WHERE ee.session_id = l.session_id AND (ee.enriched_title IS NOT NULL OR ee.enriched_company_size IS NOT NULL OR ee.enriched_company IS NOT NULL)))`);
  }
  if (websiteCheck === 'failed') conditions.push(`l.website_check_failed IS TRUE`);
  if (websiteCheck === 'passed') conditions.push(`l.website_check_failed IS NOT TRUE`); // covers false AND null (pre-migration rows)
  if (websiteCheck === 'social') conditions.push(`l.website_check_reason = 'social_profile_url'`);
  // Built from WEBSITE_VERIFIED_REASONS so this can never drift out of sync
  // with the Meta gate. Values are internal literals; the filter below is a
  // belt-and-braces guard so nothing unexpected can reach the SQL string.
  if (websiteCheck === 'unverified') {
    const verifiedSql = WEBSITE_VERIFIED_REASONS.filter((r) => /^[a-z0-9_]+$/.test(r)).map((r) => `'${r}'`).join(',');
    conditions.push(`(l.website_check_failed IS TRUE OR (l.website_check_reason IS NOT NULL AND l.website_check_reason <> '' AND l.website_check_reason NOT IN (${verifiedSql})))`);
  }
  if (repeatAttempts === 'yes') conditions.push(`EXISTS (SELECT 1 FROM leads pa WHERE LOWER(pa.email) = LOWER(l.email) AND pa.created_at < l.created_at)`);
  if (repeatAttempts === 'no')  conditions.push(`NOT EXISTS (SELECT 1 FROM leads pa WHERE LOWER(pa.email) = LOWER(l.email) AND pa.created_at < l.created_at)`);

  if (dateFrom) { params.push(dateFrom); conditions.push(`l.created_at >= $${params.length}::date`); }
  if (dateTo)   { params.push(dateTo);   conditions.push(`l.created_at < ($${params.length}::date + INTERVAL '1 day')`); }
  if (search) {
    params.push(`%${search.toLowerCase()}%`);
    const i = params.length;
    conditions.push(`(LOWER(l.email) LIKE $${i} OR LOWER(COALESCE(l.company,'')) LIKE $${i} OR LOWER(COALESCE(l.first_name,'')) LIKE $${i})`);
  }

  const whereClause = conditions.length > 0 ? 'AND ' + conditions.join(' AND ') : '';

  const baseSelect = `
    SELECT
      l.session_id, l.email, l.first_name, l.last_name, l.phone,
      l.company, l.website, l.sell_to, l.hear_about_us,
      l.completed, l.booking_uid, l.booked_at, l.start_time, l.end_time,
      l.disqualified, l.disqualified_reason, l.step_reached,
      l.loops_sent, l.created_at, l.submitted_at, l.page_url,
      l.landing_page, l.previous_page, l.website_check_failed, l.website_check_reason,
      (SELECT COUNT(*) FROM leads pa WHERE LOWER(pa.email) = LOWER(l.email) AND pa.created_at < l.created_at) AS prior_attempts,
      (SELECT COUNT(*) FROM leads pa WHERE LOWER(pa.email) = LOWER(l.email) AND pa.created_at < l.created_at AND pa.disqualified IS TRUE) AS prior_disqualified,
      l.utm_source, l.utm_medium, l.utm_campaign, l.utm_term, l.referrer, l.prefill_source,
      l.fbc, l.fbp,
      COALESCE(l.enriched_title, e.enriched_title) AS enriched_title,
      COALESCE(l.enriched_company_size, e.enriched_company_size) AS enriched_company_size,
      COALESCE(l.enriched_industry, e.enriched_industry) AS enriched_industry,
      COALESCE(l.enriched_linkedin, e.enriched_linkedin) AS enriched_linkedin,
      COALESCE(l.enriched_city, e.enriched_city) AS enriched_city,
      COALESCE(l.enriched_state, e.enriched_state) AS enriched_state,
      COALESCE(l.enriched_country, e.enriched_country) AS enriched_country,
      COALESCE(l.enriched_seniority, e.enriched_seniority) AS enriched_seniority,
      COALESCE(l.enriched_departments, e.enriched_departments) AS enriched_departments,
      COALESCE(l.enriched_email_status, e.enriched_email_status) AS enriched_email_status,
      COALESCE(l.enriched_founded_year, e.enriched_founded_year) AS enriched_founded_year,
      COALESCE(l.enriched_annual_revenue, e.enriched_annual_revenue) AS enriched_annual_revenue,
      COALESCE(l.enriched_funding_events, e.enriched_funding_events) AS enriched_funding_events,
      COALESCE(l.enriched_alexa_ranking, e.enriched_alexa_ranking) AS enriched_alexa_ranking,
      COALESCE(l.enriched_keywords, e.enriched_keywords) AS enriched_keywords,
      COALESCE(l.enriched_org_hq, e.enriched_org_hq) AS enriched_org_hq,
      COALESCE(l.enriched_total_funding, e.enriched_total_funding) AS enriched_total_funding,
      COALESCE(l.enriched_funding_stage, e.enriched_funding_stage) AS enriched_funding_stage,
      e.enriched_company AS e_company,
      e.enriched_first_name AS e_first_name, e.enriched_last_name AS e_last_name,
      e.enriched_phone AS e_phone, e.enriched_at
    FROM leads l
    LEFT JOIN enrichment_data e ON e.session_id = l.session_id
    WHERE true ${whereClause}
  `;

  try {
    if (format === 'csv') {
      const allRows = await pool.query(baseSelect + ` ${orderBy}`, params);
      const cols = [
        'email','first_name','last_name','company','website','phone','sell_to','hear_about_us',
        'completed','booking_uid','disqualified','step_reached','created_at','submitted_at','booked_at',
        'utm_source','utm_medium','utm_campaign','utm_term','referrer','prefill_source',
        'landing_page','previous_page','page_url','website_check_failed','website_check_reason','prior_attempts','prior_disqualified',
        'enriched_title','enriched_company_size','enriched_industry','enriched_seniority','enriched_departments',
        'enriched_linkedin','enriched_city','enriched_state','enriched_country',
        'enriched_annual_revenue','enriched_total_funding','enriched_funding_stage'
      ];
      const escape = v => {
        if (v === null || v === undefined) return '';
        const s = String(v);
        return s.includes(',') || s.includes('"') || s.includes('\n') ? `"${s.replace(/"/g,'""')}"` : s;
      };
      const csv = [
        cols.join(','),
        ...allRows.rows.map(r => cols.map(c => escape(r[c])).join(','))
      ].join('\n');
      res.setHeader('Content-Type', 'text/csv');
      res.setHeader('Content-Disposition', `attachment; filename="leads-${new Date().toISOString().slice(0,10)}.csv"`);
      return res.send(csv);
    }

    const countResult = await pool.query(
      `SELECT COUNT(*) AS total FROM leads l WHERE true ${whereClause}`, params
    );
    const total = parseInt(countResult.rows[0].total) || 0;

    const limitParam  = params.length + 1;
    const offsetParam = params.length + 2;

    const leadsResult = await pool.query(
      baseSelect + ` ${orderBy} LIMIT $${limitParam} OFFSET $${offsetParam}`,
      [...params, limit, offset]
    );

    res.json({ total, page, pages: Math.ceil(total / limit), leads: leadsResult.rows });
  } catch (err) {
    console.error('[/monitor/leads]', err.message);
    res.status(500).json({ error: 'Query failed', detail: err.message });
  }
});

app.get('/monitor/filter-options', async (req, res) => {
  const token = process.env.MONITOR_TOKEN;
  if (token && req.query.token !== token) return res.status(401).json({ error: 'Unauthorized' });
  try {
    const [hearRows, sourceRows] = await Promise.all([
      pool.query(`SELECT hear_about_us AS v, COUNT(*) AS c FROM leads WHERE hear_about_us IS NOT NULL AND hear_about_us <> '' GROUP BY hear_about_us ORDER BY c DESC, hear_about_us ASC LIMIT 100`),
      pool.query(`SELECT utm_source AS v, COUNT(*) AS c FROM leads WHERE utm_source IS NOT NULL AND utm_source <> '' GROUP BY utm_source ORDER BY c DESC, utm_source ASC LIMIT 100`)
    ]);
    res.json({
      hearAbout: hearRows.rows.map(r => r.v),
      utmSource: sourceRows.rows.map(r => r.v)
    });
  } catch (err) {
    console.error('[/monitor/filter-options]', err.message);
    res.status(500).json({ error: 'Filter options query failed', detail: err.message });
  }
});

app.get('/monitor/sdr', async (req, res) => {
  const token = process.env.MONITOR_TOKEN;
  if (token && req.query.token !== token) return res.status(401).json({ error: 'Unauthorized' });

  const format = req.query.format || 'json';

  try {
    const result = await pool.query(`
      SELECT * FROM (
        SELECT DISTINCT ON (LOWER(l.email))
          l.email,
          COALESCE(l.first_name, e.enriched_first_name)                          AS first_name,
          COALESCE(l.last_name,  e.enriched_last_name)                           AS last_name,
          l.company,
          l.website,
          l.phone,
          l.sell_to,
          l.hear_about_us,
          l.completed,
          l.step_reached,
          l.submitted_at,
          l.created_at,
          l.utm_source,
          l.utm_medium,
          l.utm_campaign,
          l.referrer,
          l.landing_page,
          l.website_check_failed,
          l.website_check_reason,
          COALESCE(l.enriched_title,          e.enriched_title)          AS enriched_title,
          COALESCE(l.enriched_company_size,   e.enriched_company_size)   AS enriched_company_size,
          COALESCE(l.enriched_industry,       e.enriched_industry)       AS enriched_industry,
          COALESCE(l.enriched_seniority,      e.enriched_seniority)      AS enriched_seniority,
          COALESCE(l.enriched_departments,    e.enriched_departments)     AS enriched_departments,
          COALESCE(l.enriched_linkedin,       e.enriched_linkedin)       AS enriched_linkedin,
          COALESCE(l.enriched_city,           e.enriched_city)           AS enriched_city,
          COALESCE(l.enriched_country,        e.enriched_country)        AS enriched_country,
          COALESCE(l.enriched_annual_revenue, e.enriched_annual_revenue) AS enriched_annual_revenue,
          COALESCE(l.enriched_total_funding,  e.enriched_total_funding)  AS enriched_total_funding,
          COALESCE(l.enriched_funding_stage,  e.enriched_funding_stage)  AS enriched_funding_stage
        FROM leads l
        LEFT JOIN enrichment_data e ON e.session_id = l.session_id
        WHERE l.email IS NOT NULL
          AND l.disqualified = false
          AND l.sell_to ILIKE 'B2B%'
          AND NOT EXISTS (
            SELECT 1 FROM leads booked
            WHERE LOWER(booked.email) = LOWER(l.email)
              AND booked.booking_uid IS NOT NULL
          )
        ORDER BY LOWER(l.email), l.created_at DESC
      ) deduped
      ORDER BY created_at DESC
    `);

    const leads = result.rows;

    if (format === 'csv') {
      const cols = [
        'email','first_name','last_name','company','website','phone','sell_to',
        'hear_about_us','completed','step_reached','submitted_at','created_at',
        'utm_source','utm_medium','utm_campaign','referrer','landing_page',
        'website_check_failed','website_check_reason',
        'enriched_title','enriched_company_size','enriched_industry','enriched_seniority',
        'enriched_departments','enriched_linkedin','enriched_city','enriched_country',
        'enriched_annual_revenue','enriched_total_funding','enriched_funding_stage'
      ];
      const escape = v => {
        if (v === null || v === undefined) return '';
        const s = String(v);
        return s.includes(',') || s.includes('"') || s.includes('\n') ? `"${s.replace(/"/g,'""')}"` : s;
      };
      const csv = [
        cols.join(','),
        ...leads.map(r => cols.map(c => escape(r[c])).join(','))
      ].join('\n');
      res.setHeader('Content-Type', 'text/csv');
      res.setHeader('Content-Disposition', `attachment; filename="sdr-list-${new Date().toISOString().slice(0,10)}.csv"`);
      return res.send(csv);
    }

    res.json({ total: leads.length, leads });
  } catch (err) {
    console.error('[/monitor/sdr]', err.message);
    res.status(500).json({ error: 'SDR query failed', detail: err.message });
  }
});

app.get('/monitor', (req, res) => {
  const token = process.env.MONITOR_TOKEN;
  if (token && req.query.token !== token) {
    return res.status(401).send('<h2 style="font-family:sans-serif;padding:2rem">401 — Unauthorized. Add ?token=YOUR_TOKEN to the URL.</h2>');
  }

  const tp = req.query.token ? '?token=' + req.query.token : '';

  const html = '<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Gushwork Monitor</title>' +
  '<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"><\/script>' +
  '<style>' +
  '*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}' +
  'body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;background:#f5f5f5;color:#1a1a1a;font-size:14px;line-height:1.5}' +
  '.topbar{background:#fff;border-bottom:1px solid #e5e5e5;padding:12px 24px;display:flex;align-items:center;justify-content:space-between;position:sticky;top:0;z-index:100}' +
  '.logo{font-size:15px;font-weight:600}' +
  '.apill{display:flex;align-items:center;gap:6px;font-size:12px;padding:4px 10px;border-radius:999px;border:1px solid #e5e5e5;background:#fff;color:#666}' +
  '.dot{width:7px;height:7px;border-radius:50%;background:#ccc;display:inline-block;flex-shrink:0}' +
  '.dot-green{background:#22c55e}.dot-red{background:#ef4444}.dot-amber{background:#f59e0b}' +
  '.btn{font-size:12px;padding:6px 14px;border-radius:6px;border:1px solid #e5e5e5;background:#fff;cursor:pointer;color:#333}' +
  '.btn:hover{background:#f5f5f5}' +
  '.page{max-width:1200px;margin:0 auto;padding:24px}' +
  '.sl{font-size:11px;font-weight:600;color:#888;text-transform:uppercase;letter-spacing:0.07em;margin-bottom:10px}' +
  '.g4{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:12px}' +
  '.g2{display:grid;grid-template-columns:repeat(2,1fr);gap:12px;margin-bottom:24px}' +
  '.card{background:#fff;border:1px solid #e5e5e5;border-radius:10px;padding:16px 20px}' +
  '.mc{background:#fff;border:1px solid #e5e5e5;border-radius:10px;padding:16px;cursor:default}' +
  '.ml{font-size:12px;color:#888;margin-bottom:6px}' +
  '.mv{font-size:28px;font-weight:600;color:#1a1a1a;line-height:1}' +
  '.ms{font-size:11px;color:#aaa;margin-top:6px}' +
  '.recon{font-size:11px;color:#999;margin:0 2px 24px 2px}' +
  '.sr{display:flex;align-items:center;justify-content:space-between;padding:11px 0;border-bottom:1px solid #f0f0f0}' +
  '.sr:last-child{border-bottom:none}' +
  '.sn{font-size:13px;font-weight:500}.sd{font-size:11px;color:#999;margin-top:2px}' +
  '.badge{font-size:11px;font-weight:500;padding:3px 9px;border-radius:5px;white-space:nowrap}' +
  '.bg{background:#f0fdf4;color:#15803d}.br{background:#fef2f2;color:#b91c1c}.ba{background:#fffbeb;color:#b45309}.bx{background:#f5f5f5;color:#666}.bb{background:#eff6ff;color:#1d4ed8}' +
  '.alertbox{border-radius:8px;padding:10px 14px;margin-bottom:8px;font-size:13px;display:flex;align-items:flex-start;gap:8px}' +
  '.ao{background:#f0fdf4;color:#15803d;border:1px solid #bbf7d0}.aw{background:#fffbeb;color:#b45309;border:1px solid #fde68a}.ae{background:#fef2f2;color:#b91c1c;border:1px solid #fecaca}' +
  '.fr{margin-bottom:10px}.fl{display:flex;justify-content:space-between;font-size:12px;color:#666;margin-bottom:4px}' +
  '.fb{height:7px;border-radius:4px;background:#f0f0f0;overflow:hidden}.ff{height:100%;border-radius:4px;transition:width 0.6s ease}' +
  '.filters{display:flex;gap:8px;flex-wrap:wrap;align-items:center;margin-bottom:16px}' +
  '.filters input,.filters select{font-size:13px;padding:7px 10px;border:1px solid #e5e5e5;border-radius:7px;background:#fff;color:#1a1a1a;outline:none}' +
  '.filters input:focus,.filters select:focus{border-color:#999}' +
  '.filters input[type=text]{min-width:200px}' +
  '.sortable{cursor:pointer;user-select:none}.sortable:hover{color:#555}.sar{font-size:10px;color:#bbb;margin-left:2px}' +
  'table{width:100%;border-collapse:collapse;font-size:12px}' +
  'th{text-align:left;padding:9px 10px;font-weight:500;color:#888;border-bottom:1px solid #e5e5e5;font-size:11px;text-transform:uppercase;letter-spacing:0.04em;white-space:nowrap}' +
  'td{padding:10px;border-bottom:1px solid #f5f5f5;color:#333;vertical-align:top}' +
  'tr:hover td{background:#fafafa}tr:last-child td{border-bottom:none}' +
  '.te{font-weight:500;color:#1a1a1a;max-width:180px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}' +
  '.tc{max-width:130px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}' +
  '.xbtn{cursor:pointer;color:#bbb;font-size:13px;padding:10px 8px;text-align:center;user-select:none}' +
  '.xbtn:hover{color:#333}' +
  '.erow{background:#f9f9ff}.erow td{padding:14px;border-bottom:1px solid #eee}' +
  '.egrid{display:grid;grid-template-columns:repeat(auto-fill,minmax(200px,1fr));gap:8px}' +
  '.ef{background:#fff;border:1px solid #eee;border-radius:6px;padding:8px 10px}' +
  '.efl{font-size:10px;font-weight:600;color:#aaa;text-transform:uppercase;letter-spacing:0.05em;margin-bottom:3px}' +
  '.efv{font-size:12px;color:#1a1a1a;word-break:break-word}.efv a{color:#2563eb;text-decoration:none}' +
  '.pg{display:flex;align-items:center;gap:8px;justify-content:center;padding:16px 0;flex-wrap:wrap}' +
  '.pb{padding:5px 12px;border:1px solid #e5e5e5;border-radius:6px;background:#fff;cursor:pointer;font-size:12px;color:#333}' +
  '.pb:hover{background:#f5f5f5}.pb.act{background:#1a1a1a;color:#fff;border-color:#1a1a1a}.pb:disabled{opacity:0.4;cursor:not-allowed}' +
  '.pi{font-size:12px;color:#888}' +
  '.cw{position:relative;width:100%;height:180px}' +
  '.tabs{display:flex;border-bottom:1px solid #e5e5e5;margin-bottom:20px}' +
  '.tab{padding:10px 18px;font-size:13px;cursor:pointer;color:#888;border-bottom:2px solid transparent;font-weight:500}' +
  '.tab:hover{color:#333}.tab.act{color:#1a1a1a;border-bottom-color:#1a1a1a}' +
  '.tp{display:none}.tp.act{display:block}' +
  '.lu{font-size:11px;color:#aaa}' +
  '.nd{text-align:center;padding:40px;color:#999;font-size:13px}' +
  '@media(max-width:700px){.g4{grid-template-columns:repeat(2,1fr)}.g2{grid-template-columns:1fr}}' +
  '</style></head><body>' +
  '<div class="topbar"><div style="display:flex;align-items:center;gap:12px"><span class="logo">Gushwork &#8212; Form Monitor</span>' +
  '<div class="apill"><span class="dot" id="apidot"></span><span id="apist">Checking...</span></div></div>' +
  '<div style="display:flex;align-items:center;gap:10px"><span class="lu" id="lupd">&#8212;</span>' +
  '<button class="btn" onclick="loadAll()">&#8635; Refresh</button></div></div>' +
  '<div class="page">' +
  '<div class="tabs">' +
  '<div class="tab act" id="t-overview" onclick="showTab(\'overview\')">Overview</div>' +
  '<div class="tab" id="t-leads" onclick="showTab(\'leads\')">All Leads</div>' +
  '<div class="tab" id="t-sdr" onclick="showTab(\'sdr\')">SDR List</div>' +
  '<div class="tab" id="t-dupes" onclick="showTab(\'dupes\')" style="color:#aaa">Duplicates</div>' +
  '<div class="tab" id="t-lm" onclick="showTab(\'lm\')">Lead Magnet</div>' +
  '<div class="tab" id="t-health" onclick="showTab(\'health\')">System Health</div>' +
  '</div>' +
  '<div class="tp act" id="tp-overview">' +
  '<div class="sl">Overview &#8212; people (headline) &#183; sessions (small print)</div>' +
  '<div class="g4">' +
  '<div class="mc" title="People = distinct email addresses ever captured. Sessions = individual form visits; one person can have several."><div class="ml">Total people</div><div class="mv" id="m-total">&#8212;</div><div class="ms" id="m-totals">&#8212;</div></div>' +
  '<div class="mc" title="People whose form reached Step 2 (completed) on at least one of their sessions."><div class="ml">People completed</div><div class="mv" id="m-comp">&#8212;</div><div class="ms" id="m-cpct">&#8212;</div></div>' +
  '<div class="mc" title="People with a booking on at least one of their sessions."><div class="ml">People booked</div><div class="mv" id="m-book">&#8212;</div><div class="ms" id="m-bpct">&#8212;</div></div>' +
  '<div class="mc" title="People marked disqualified (B2C / Mixed) on at least one session."><div class="ml">Disqualified</div><div class="mv" id="m-disq">&#8212;</div><div class="ms" id="m-dsq">B2C / Mixed</div></div>' +
  '</div>' +
  '<div class="g4">' +
  '<div class="mc" title="Distinct qualified B2B people who completed the form but have NO booking on ANY of their sessions. This is exactly the SDR List."><div class="ml">No booking yet (SDR)</div><div class="mv" id="m-nb">&#8212;</div><div class="ms" id="m-nbs">&#8212;</div></div>' +
  '<div class="mc" title="People who completed the form without booking, and later booked on another session &#8212; your follow-up emails / prefill links / SDR nudges working."><div class="ml">Recovered bookings</div><div class="mv" id="m-rec">&#8212;</div><div class="ms">booked on a later session</div></div>' +
  '<div class="mc" title="Sessions older than 2 hours with no booking (and no booking on any other session of that email) that the recovery cron has not processed yet."><div class="ml">Pending recovery</div><div class="mv" id="m-pend">&#8212;</div><div class="ms">&gt;2h, awaiting follow-up</div></div>' +
  '<div class="mc" title="Sessions where the drop-off recovery email has been sent (loops_sent = true)."><div class="ml">Recovery emails sent</div><div class="mv" id="m-mail">&#8212;</div><div class="ms">follow-ups dispatched</div></div>' +
  '</div>' +
  '<div class="recon" id="recon">&#8212;</div>' +
  '<div class="g2">' +
  '<div><div class="sl">Alerts</div><div id="alerts"><div class="alertbox" style="background:#f5f5f5;color:#999;border:1px solid #eee">Loading...</div></div></div>' +
  '<div><div class="sl">Conversion funnel (people)</div><div class="card"><div id="funnel">Loading...</div></div></div>' +
  '</div>' +
  '<div class="sl">Form sessions per day &#8212; last 14 days (IST)</div>' +
  '<div class="card" style="margin-bottom:24px"><div class="cw"><canvas id="lchart"></canvas></div></div>' +
  '</div>' +
  '<div class="tp" id="tp-leads">' +
  '<div class="filters">' +
  '<input type="text" id="fsearch" placeholder="Search email, company..." oninput="debounce()">' +
  '<select id="fstage" onchange="loadLeads(1)"><option value="all">All stages</option><option value="booked">Booked</option><option value="completed">Completed (no booking)</option><option value="step1">Step 1 only</option><option value="disqualified">Disqualified</option></select>' +
  '<select id="fsellto" onchange="loadLeads(1)"><option value="all">All sell-to</option><option value="B2B">B2B</option><option value="B2B (clarified from B2C)">B2B (clarified from B2C)</option><option value="B2B (clarified from Mixed)">B2B (clarified from Mixed)</option><option value="B2C">B2C</option><option value="Mixed">Mixed</option><option value="__clarified">Clarified (any)</option></select>' +
  '<select id="fsource" onchange="loadLeads(1)"><option value="all">All sources</option></select>' +
  '<select id="fenrich" onchange="loadLeads(1)"><option value="all">Enrichment: all</option><option value="yes">Enriched</option><option value="no">Not enriched</option></select>' +
  '<select id="fwebsitecheck" onchange="loadLeads(1)"><option value="all">Website check: all</option><option value="failed">Failed</option><option value="passed">Passed</option><option value="social">Social profile</option><option value="unverified">Not verified (any)</option></select>' +
  '<select id="frepeat" onchange="loadLeads(1)"><option value="all">Attempts: all</option><option value="yes">Repeat only</option><option value="no">First-time only</option></select>' +
  '<input type="text" id="fhear" list="hearlist" placeholder="Heard about us..." oninput="debounce()" style="min-width:170px">' +
  '<datalist id="hearlist"></datalist>' +
  '<select id="fpreset" onchange="datePreset(this.value)"><option value="">Any date</option><option value="today">Today</option><option value="7d">Last 7 days</option><option value="30d">Last 30 days</option></select>' +
  '<input type="date" id="ffrom" onchange="dateManual()">' +
  '<input type="date" id="fto" onchange="dateManual()">' +
  '<button class="btn" onclick="clearF()">Clear</button>' +
  '<button class="btn" onclick="exportLeads()" style="background:#1a1a1a;color:#fff;border-color:#1a1a1a">&#8595; Export CSV</button>' +
  '<span id="lcount" style="font-size:12px;color:#888"></span>' +
  '</div>' +
  '<div class="card" style="padding:0;overflow:hidden"><div style="overflow-x:auto"><table><thead><tr>' +
  '<th style="width:30px"></th>' +
  '<th class="sortable" onclick="sortBy(\'email\')">Email <span class="sar" id="sar-email"></span></th>' +
  '<th class="sortable" onclick="sortBy(\'name\')">Name <span class="sar" id="sar-name"></span></th>' +
  '<th class="sortable" onclick="sortBy(\'company\')">Company <span class="sar" id="sar-company"></span></th>' +
  '<th class="sortable" onclick="sortBy(\'sell_to\')">Sells to <span class="sar" id="sar-sell_to"></span></th>' +
  '<th>Stage</th><th>Booked</th><th>Enrichment</th>' +
  '<th class="sortable" onclick="sortBy(\'created_at\')">Created (IST) <span class="sar" id="sar-created_at"></span></th>' +
  '<th>Source</th>' +
  '</tr></thead><tbody id="ltbody"><tr><td colspan="10" class="nd">Loading leads...</td></tr></tbody></table></div></div>' +
  '<div class="pg" id="lpag"></div>' +
  '</div>' +
  '<div class="tp" id="tp-sdr">' +
  '<div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:16px">' +
  '<div><div class="sl" style="margin-bottom:2px">SDR List</div><div style="font-size:12px;color:#888">Qualified B2B leads who have never booked a call &#8212; deduped by email</div></div>' +
  '<div style="display:flex;gap:8px;align-items:center">' +
  '<input type="text" id="sdr-search" placeholder="Search email, company..." oninput="sdrDebounce()" style="font-size:13px;padding:7px 10px;border:1px solid #e5e5e5;border-radius:7px;background:#fff;color:#1a1a1a;outline:none;min-width:220px">' +
  '<span id="sdr-count" style="font-size:12px;color:#888"></span>' +
  '<button class="btn" onclick="exportSDR()" style="background:#1a1a1a;color:#fff;border-color:#1a1a1a">&#8595; Export CSV</button>' +
  '</div></div>' +
  '<div class="card" style="padding:0;overflow:hidden"><div style="overflow-x:auto"><table><thead><tr>' +
  '<th style="width:30px"></th><th>Email</th><th>Name</th><th>Company</th><th>Title</th><th>Industry</th><th>Company Size</th><th>Stage</th><th>LinkedIn</th><th>Date (IST)</th>' +
  '</tr></thead><tbody id="sdr-tbody"><tr><td colspan="9" class="nd">Loading...</td></tr></tbody></table></div></div>' +
  '</div>' +
  '<div class="tp" id="tp-dupes">' +
  '<div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:16px">' +
  '<div><div class="sl" style="margin-bottom:2px">Duplicate Sessions</div><div style="font-size:12px;color:#888">Emails that appear in more than one session &#8212; sorted by session count</div></div>' +
  '<span id="dupes-count" style="font-size:12px;color:#888"></span>' +
  '</div>' +
  '<div class="card" style="padding:0;overflow:hidden"><div style="overflow-x:auto"><table><thead><tr>' +
  '<th style="width:30px"></th><th>Email</th><th>Sessions</th><th>Booked?</th><th>Completed?</th><th>First Seen (IST)</th><th>Last Seen (IST)</th>' +
  '</tr></thead><tbody id="dupes-tbody"><tr><td colspan="7" class="nd">Loading...</td></tr></tbody></table></div></div>' +
  '</div>' +
  '<div class="tp" id="tp-health">' +
  '<div class="sl">Step health</div>' +
  '<div class="card" style="margin-bottom:24px">' +
  '<div class="sr"><div><div class="sn">API uptime</div><div class="sd">/health responding</div></div><span class="badge bx" id="s-api">Checking...</span></div>' +
  '<div class="sr"><div><div class="sn">Step 1 &#8212; /partial</div><div class="sd">Email + lead saved to Railway + AWS</div></div><span class="badge bx" id="s-partial">Checking...</span></div>' +
  '<div class="sr"><div><div class="sn">Step 2 &#8212; /submit</div><div class="sd">Lead completed + Slack fired</div></div><span class="badge bx" id="s-submit">Checking...</span></div>' +
  '<div class="sr"><div><div class="sn">ELV email verification</div><div class="sd">Inconclusive rate, rolling 90-minute window</div></div><span class="badge bx" id="s-elv">Checking...</span></div>' +
  '<div class="sr"><div><div class="sn">Apollo enrichment</div><div class="sd">enrichment_data populated per session</div></div><span class="badge bx" id="s-enrich">Checking...</span></div>' +
  '<div class="sr"><div><div class="sn">Booking &#8212; RevenueHero</div><div class="sd">People booked / people completed</div></div><span class="badge bx" id="s-cal">Checking...</span></div>' +
  '<div class="sr"><div><div class="sn">Cron &#8212; drop-off recovery</div><div class="sd">Leads waiting &gt;2 hours without booking</div></div><span class="badge bx" id="s-cron">Checking...</span></div>' +
  '<div class="sr"><div><div class="sn">AWS sync</div><div class="sd">gw_form_leads mirror</div></div><span class="badge bx" id="s-aws">Checking...</span></div>' +
  '<div class="sr"><div><div class="sn">Email recovery</div><div class="sd">Follow-up emails sent to partial leads</div></div><span class="badge bx" id="s-loops">Checking...</span></div>' +
  '</div>' +
  '<div class="sl">Enrichment coverage</div>' +
  '<div class="g4" style="margin-bottom:24px">' +
  '<div class="mc"><div class="ml">Enriched sessions</div><div class="mv" id="h-enr">&#8212;</div><div class="ms">in enrichment_data</div></div>' +
  '<div class="mc"><div class="ml">With title</div><div class="mv" id="h-tit">&#8212;</div><div class="ms">% of enriched</div></div>' +
  '<div class="mc"><div class="ml">With funding data</div><div class="mv" id="h-fun">&#8212;</div><div class="ms">% of enriched</div></div>' +
  '<div class="mc"><div class="ml">With location</div><div class="mv" id="h-loc">&#8212;</div><div class="ms">% of enriched</div></div>' +
  '</div>' +
  '</div>' +
  '<div class="tp" id="tp-lm">' +

  '<div class="sl">Funnel' +
  '<select id="lm-days" onchange="loadLM()" style="float:right;padding:4px 8px;border:1px solid #e5e5e5;border-radius:6px;font-size:12px">' +
  '<option value="7">Last 7 days</option><option value="30" selected>Last 30 days</option>' +
  '<option value="90">Last 90 days</option><option value="365">Last year</option></select></div>' +
  '<div class="ms" id="lm-people" style="margin:-4px 0 10px">&nbsp;</div>' +
  '<div class="g4">' +
  '<div class="mc"><div class="ml">Page views</div><div class="mv" id="lm-views">&#8212;</div><div class="ms">people who loaded the LP</div></div>' +
  '<div class="mc"><div class="ml">Form opened</div><div class="mv" id="lm-opens">&#8212;</div><div class="ms" id="lm-opens-r">&#8212;</div></div>' +
  '<div class="mc"><div class="ml">Email entered</div><div class="mv" id="lm-emails">&#8212;</div><div class="ms" id="lm-emails-r">&#8212;</div></div>' +
  '<div class="mc"><div class="ml">Submitted</div><div class="mv" id="lm-submitted">&#8212;</div><div class="ms" id="lm-submitted-r">&#8212;</div></div>' +
  '</div>' +

  '<div class="sl">Where people drop off</div>' +
  '<div class="card"><div id="lm-dropoff"><div class="nd">Loading...</div></div></div>' +

  '<div class="g2">' +
  '<div class="card"><div class="sl">Daily volume</div><canvas id="lm-chart" height="90"></canvas></div>' +
  '<div class="card"><div class="sl">Industries</div>' +
  '<div class="ms" style="margin-bottom:8px">Tagged <b>custom</b> where they typed their own instead of picking from the list.</div>' +
  '<div id="lm-inds"><div class="nd">Loading...</div></div></div>' +
  '</div>' +

  '<div class="g2">' +
  '<div class="card"><div class="sl">Where they entered from</div>' +
  '<div class="ms" style="margin-bottom:10px">Which CTA opened the form, and how many of those went on to submit.</div>' +
  '<div id="lm-entries"><div class="nd">Loading...</div></div>' +
  '<div style="height:1px;background:#f0f0f0;margin:16px 0"></div>' +
  '<div class="sl" style="margin-top:0">Email type</div>' +
  '<div style="margin-top:8px" id="lm-emailtype"><div class="nd">Loading...</div></div></div>' +
  '<div class="card"><div class="sl">Custom categories entered</div>' +
  '<div class="ms" style="margin-bottom:8px">What people typed when the list did not fit. Feed recurring ones back into the dropdown.</div>' +
  '<div id="lm-custom"><div class="nd">Loading...</div></div></div>' +
  '</div>' +

  '<div class="sl">Leads</div>' +
  '<div class="card" style="margin-bottom:12px">' +
  '<div style="display:flex;gap:8px;flex-wrap:wrap;align-items:center">' +
  '<input id="lm-search" placeholder="Search email, industry, product, website..." ' +
  'oninput="lmRender()" style="flex:1;min-width:220px;padding:8px 10px;border:1px solid #e5e5e5;border-radius:6px;font-size:13px" />' +
  '<button class="btn" onclick="lmCsv()">Export CSV</button>' +
  '</div>' +
  '<div id="lm-pills" style="display:flex;gap:6px;flex-wrap:wrap;margin-top:10px"></div>' +
  '</div>' +
  '<div class="card" style="padding:0;overflow:hidden"><div style="overflow-x:auto"><table><thead><tr>' +
  '<th style="width:28px"></th><th>Email</th><th>Industry</th><th>Product / service</th><th>Sells to</th>' +
  '<th>Website</th><th>Source</th><th>Status</th><th>When (IST)</th><th></th>' +
  '</tr></thead><tbody id="lm-tbody"><tr><td colspan="10" class="nd">Loading...</td></tr></tbody></table></div></div>' +
  '<div class="ms" id="lm-count" style="margin-top:8px"></div>' +

  '</div>' +
  '</div>';

  const js = '<script>' +
  'var TP="' + tp + '";' +
  'var API=window.location.origin;' +
  'var lChart=null,curPage=1,stimer=null,curSort="created_at",curDir="desc",filterOptsLoaded=false;' +
  'function showTab(n){["overview","leads","sdr","dupes","health","lm"].forEach(function(x){document.getElementById("t-"+x).classList.toggle("act",x===n);document.getElementById("tp-"+x).classList.toggle("act",x===n);});if(n==="leads"){loadFilterOptions();if(document.getElementById("ltbody").textContent.indexOf("Loading")>=0)loadLeads(1);}if(n==="sdr"&&document.getElementById("sdr-tbody").textContent.indexOf("Loading")>=0)loadSDR();if(n==="dupes"&&document.getElementById("dupes-tbody").textContent.indexOf("Loading")>=0)loadDupes();if(n==="lm"&&document.getElementById("lm-tbody").textContent.indexOf("Loading")>=0)loadLM();}' +
  'function badge(id,text,cls){var el=document.getElementById(id);if(!el)return;el.textContent=text;el.className="badge "+cls;}' +
  'function set(id,v){var el=document.getElementById(id);if(el)el.textContent=v;}' +
  'function pct(a,b){return b?Math.round(a/b*100)+"%":"0%";}' +
  'function ist(ts){if(!ts)return"\\u2014";return new Date(ts).toLocaleString("en-IN",{timeZone:"Asia/Kolkata",dateStyle:"short",timeStyle:"short"});}' +
  'function esc(s){if(!s)return"";return String(s).replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;").replace(/"/g,"&quot;");}' +
  'async function checkApi(){try{var r=await fetch(API+"/health",{signal:AbortSignal.timeout(5000)});if(r.ok){document.getElementById("apidot").className="dot dot-green";document.getElementById("apist").textContent="API online";badge("s-api","Online","bg");return true;}throw new Error("HTTP "+r.status);}catch(e){document.getElementById("apidot").className="dot dot-red";document.getElementById("apist").textContent="API offline";badge("s-api","Offline","br");return false;}}' +
  'async function checkElv(){try{var r=await fetch(API+"/monitor/elv-health",{signal:AbortSignal.timeout(5000)});var d=await r.json();if(d.state==="degraded"){badge("s-elv",d.rate+"% inconclusive","br");}else if(d.state==="insufficient_data"){badge("s-elv","Idle ("+d.checks+" checks)","bx");}else{badge("s-elv","Healthy ("+d.rate+"% inconclusive)","bg");}}catch(e){badge("s-elv","Unknown","bx");}}' +
  'function renderAlerts(d){var a=[];if(d.pendingPartials>0)a.push({c:"aw",i:"!",m:d.pendingPartials+" session(s) waiting >2 hours without booking \\u2014 recovery cron will pick them up."});if(d.noBookingUid>0)a.push({c:"aw",i:"!",m:d.noBookingUid+" people (deduped, qualified B2B) completed the form but have no booking on any session \\u2014 see SDR List."});if(!d.awsSynced)a.push({c:"ae",i:"x",m:"AWS sync disabled."});if(d.total>5&&d.enriched<d.total*0.3)a.push({c:"aw",i:"!",m:"Low enrichment rate ("+Math.round(d.enriched/d.total*100)+"% of sessions)."});if(d.todayCount===0)a.push({c:"aw",i:"o",m:"No new sessions in the last 24 hours."});if(a.length===0)a.push({c:"ao",i:"\\u2713",m:"All systems healthy."});document.getElementById("alerts").innerHTML=a.map(function(x){return"<div class=\\"alertbox "+x.c+"\\"><span>"+x.i+"</span><span>"+x.m+"</span></div>";}).join("");}' +
  'function renderFunnel(t,c,b,d){var steps=[{l:"People entered (Step 1)",v:t,p:100,col:"#818cf8"},{l:"People completed (Step 2)",v:c,p:t?Math.round(c/t*100):0,col:"#38bdf8"},{l:"People booked",v:b,p:t?Math.round(b/t*100):0,col:"#34d399"},{l:"People disqualified",v:d,p:t?Math.round(d/t*100):0,col:"#fb923c"}];document.getElementById("funnel").innerHTML=steps.map(function(s){return"<div class=\\"fr\\"><div class=\\"fl\\"><span>"+s.l+"</span><span style=\\"font-weight:500\\">"+s.v+" <span style=\\"color:#aaa\\">("+s.p+"%)</span></span></div><div class=\\"fb\\"><div class=\\"ff\\" style=\\"width:"+s.p+"%;background:"+s.col+"\\"></div></div></div>";}).join("");}' +
  'function renderChart(rows){var labels=(rows||[]).map(function(r){return r.day_label;}),data=(rows||[]).map(function(r){return parseInt(r.count)||0;});if(lChart)lChart.destroy();var ctx=document.getElementById("lchart").getContext("2d");lChart=new Chart(ctx,{type:"bar",data:{labels:labels,datasets:[{data:data,backgroundColor:"#818cf8",borderRadius:4,borderSkipped:false}]},options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false}},scales:{y:{beginAtZero:true,ticks:{stepSize:1,color:"#aaa"},grid:{color:"#f0f0f0"}},x:{ticks:{color:"#aaa",maxRotation:45,autoSkip:false},grid:{display:false}}}}});}' +
  'function stageBadge(l){if(l.booking_uid)return"<span class=\\"badge bg\\">Booked</span>";if(l.disqualified)return"<span class=\\"badge br\\">Disqualified</span>";if(l.completed)return"<span class=\\"badge bb\\">Completed</span>";return"<span class=\\"badge ba\\">Step 1</span>";}' +
  'function enrichBadge(l){return(l.enriched_title||l.enriched_company_size||l.e_company)?"<span class=\\"badge bg\\">Yes</span>":"<span class=\\"badge bx\\">No</span>";}' +
  'function enrichPanel(l){var loc=[l.enriched_city,l.enriched_state,l.enriched_country].filter(Boolean).join(", ");var fields=[' +
  '{lb:"Title",v:l.enriched_title},' +
  '{lb:"Seniority",v:l.enriched_seniority},' +
  '{lb:"Department",v:l.enriched_departments},' +
  '{lb:"Email status",v:l.enriched_email_status},' +
  '{lb:"Company",v:l.company||l.e_company},' +
  '{lb:"Company size",v:l.enriched_company_size},' +
  '{lb:"Industry",v:l.enriched_industry},' +
  '{lb:"Founded",v:l.enriched_founded_year},' +
  '{lb:"Annual revenue",v:l.enriched_annual_revenue},' +
  '{lb:"Total funding",v:l.enriched_total_funding},' +
  '{lb:"Funding stage",v:l.enriched_funding_stage},' +
  '{lb:"Funding events",v:l.enriched_funding_events},' +
  '{lb:"Alexa rank",v:l.enriched_alexa_ranking},' +
  '{lb:"Keywords",v:l.enriched_keywords},' +
  '{lb:"Person location",v:loc||null},' +
  '{lb:"Company HQ",v:l.enriched_org_hq},' +
  '{lb:"LinkedIn",v:l.enriched_linkedin,lnk:true},' +
  '{lb:"Phone",v:l.e_phone||l.phone},' +
  '{lb:"Website",v:l.website,lnk:true},' +
  '{lb:"\\u26A0\\uFE0F Website check",v:l.website_check_failed?("Failed"+(l.website_check_reason?" ("+l.website_check_reason+")":"")):null},' +
  '{lb:"\\uD83D\\uDD17 Website type",v:(!l.website_check_failed&&l.website_check_reason==="social_profile_url")?"Social profile (no company site)":null},' +
  '{lb:"\\uD83D\\uDD01 Attempts",v:(Number(l.prior_attempts)>0)?("Attempt "+(Number(l.prior_attempts)+1)+" \\u2014 "+l.prior_attempts+" prior"+(Number(l.prior_disqualified)>0?", "+l.prior_disqualified+" disqualified":"")):null},' +
  '{lb:"Hear about us",v:l.hear_about_us},' +
  '{lb:"UTM source",v:l.utm_source},' +
  '{lb:"UTM medium",v:l.utm_medium},' +
  '{lb:"UTM campaign",v:l.utm_campaign},' +
  '{lb:"Referrer",v:l.referrer},' +
  '{lb:"Prefill",v:l.prefill_source},' +
  '{lb:"UTM term",v:l.utm_term},' +
  '{lb:"\\uD83D\\uDEEC Landing Page",v:l.landing_page,lnk:true},' +
  '{lb:"\\u2B05\\uFE0F Previous Page",v:l.previous_page,lnk:true},' +
  '{lb:"\\uD83D\\uDCC4 Form Page",v:l.page_url,lnk:true},' +
  '{lb:"Meta fbc",v:l.fbc},' +
  '{lb:"Meta fbp",v:l.fbp},' +
  '{lb:"Submitted",v:ist(l.submitted_at)},' +
  '{lb:"Booked at",v:ist(l.booked_at)},' +
  '{lb:"Meeting",v:l.start_time?ist(l.start_time):null},' +
  '{lb:"Email sent",v:l.loops_sent?"Yes":"No"},' +
  '{lb:"Session ID",v:l.session_id,mono:true},' +
  '{lb:"Enriched at",v:ist(l.enriched_at)}' +
  '].filter(function(f){return f.v;});' +
  'if(!fields.length)return"<div style=\\"color:#999;font-size:12px\\">No enrichment data.</div>";' +
  'return"<div class=\\"egrid\\">"+fields.map(function(f){var val=f.lnk&&f.v?"<a href=\\""+(f.v.startsWith("http")?"":"https://")+esc(f.v)+"\\" target=\\"_blank\\">"+esc(f.v)+"</a>":f.mono?"<code style=\\"font-size:10px\\">"+esc(f.v)+"</code>":esc(f.v);return"<div class=\\"ef\\"><div class=\\"efl\\">"+f.lb+"</div><div class=\\"efv\\">"+val+"</div></div>";}).join("")+"</div>";}' +
  'function debounce(){clearTimeout(stimer);stimer=setTimeout(function(){loadLeads(1);},400);}' +
  'function clearF(){document.getElementById("fsearch").value="";document.getElementById("fstage").value="all";document.getElementById("fsellto").value="all";document.getElementById("fsource").value="all";document.getElementById("fenrich").value="all";document.getElementById("fwebsitecheck").value="all";document.getElementById("frepeat").value="all";document.getElementById("fhear").value="";document.getElementById("fpreset").value="";document.getElementById("ffrom").value="";document.getElementById("fto").value="";curSort="created_at";curDir="desc";renderSortArrows();loadLeads(1);}' +
  'function renderSortArrows(){["email","name","company","sell_to","created_at"].forEach(function(c){var el=document.getElementById("sar-"+c);if(el)el.textContent=(curSort===c)?(curDir==="asc"?"\\u25B2":"\\u25BC"):"";});}' +
  'function sortBy(c){if(curSort===c){curDir=(curDir==="asc")?"desc":"asc";}else{curSort=c;curDir=(c==="created_at")?"desc":"asc";}renderSortArrows();loadLeads(1);}' +
  'function datePreset(v){var ff=document.getElementById("ffrom"),ft=document.getElementById("fto");if(!v){loadLeads(1);return;}function fmt(d){var y=d.getFullYear(),m=("0"+(d.getMonth()+1)).slice(-2),da=("0"+d.getDate()).slice(-2);return y+"-"+m+"-"+da;}var now=new Date(),to=fmt(now),from=to;if(v==="7d"){var d=new Date(now);d.setDate(d.getDate()-6);from=fmt(d);}else if(v==="30d"){var d2=new Date(now);d2.setDate(d2.getDate()-29);from=fmt(d2);}ff.value=from;ft.value=to;loadLeads(1);}' +
  'function dateManual(){var p=document.getElementById("fpreset");if(p)p.value="";loadLeads(1);}' +
  'function exportLeads(){var search=document.getElementById("fsearch").value.trim(),stage=document.getElementById("fstage").value,sellTo=document.getElementById("fsellto").value,source=document.getElementById("fsource").value,enrich=document.getElementById("fenrich").value,websiteCheck=document.getElementById("fwebsitecheck").value,repeatAttempts=document.getElementById("frepeat").value,hear=document.getElementById("fhear").value.trim(),from=document.getElementById("ffrom").value,to=document.getElementById("fto").value;var url=API+"/monitor/leads"+(TP||"?")+(TP?"&":"")+"format=csv&stage="+stage+"&sort="+curSort+"&dir="+curDir;if(sellTo&&sellTo!=="all")url+="&sellTo="+encodeURIComponent(sellTo);if(source&&source!=="all")url+="&utmSource="+encodeURIComponent(source);if(enrich&&enrich!=="all")url+="&enrichment="+encodeURIComponent(enrich);if(websiteCheck&&websiteCheck!=="all")url+="&websiteCheck="+encodeURIComponent(websiteCheck);if(repeatAttempts&&repeatAttempts!=="all")url+="&repeatAttempts="+encodeURIComponent(repeatAttempts);if(hear)url+="&hearAbout="+encodeURIComponent(hear);if(search)url+="&search="+encodeURIComponent(search);if(from)url+="&dateFrom="+from;if(to)url+="&dateTo="+to;window.location.href=url;}' +
  'async function loadFilterOptions(){if(filterOptsLoaded)return;try{var r=await fetch(API+"/monitor/filter-options"+(TP||"?")+(TP?"&":"")+"_="+Date.now(),{signal:AbortSignal.timeout(10000)});if(!r.ok)return;var d=await r.json();var sel=document.getElementById("fsource");if(sel&&d.utmSource){d.utmSource.forEach(function(v){var o=document.createElement("option");o.value=v;o.textContent=v;sel.appendChild(o);});}var dl=document.getElementById("hearlist");if(dl&&d.hearAbout){dl.innerHTML=d.hearAbout.map(function(v){return"<option value=\\""+esc(v)+"\\"></option>";}).join("");}filterOptsLoaded=true;}catch(e){}}' +
  'function toggleRow(sid){var row=document.getElementById("er-"+sid);if(!row)return;var vis=row.style.display!=="none";row.style.display=vis?"none":"table-row";var btn=row.previousElementSibling&&row.previousElementSibling.querySelector(".xbtn");if(btn)btn.textContent=vis?"\\u25B6":"\\u25BC";}' +
  'async function loadLeads(pg){curPage=pg||1;var search=document.getElementById("fsearch").value.trim(),stage=document.getElementById("fstage").value,sellTo=document.getElementById("fsellto").value,source=document.getElementById("fsource").value,enrich=document.getElementById("fenrich").value,websiteCheck=document.getElementById("fwebsitecheck").value,repeatAttempts=document.getElementById("frepeat").value,hear=document.getElementById("fhear").value.trim(),from=document.getElementById("ffrom").value,to=document.getElementById("fto").value;' +
  'var url=API+"/monitor/leads"+(TP||"?")+(TP?"&":"")+"page="+curPage+"&stage="+stage+"&sort="+curSort+"&dir="+curDir;' +
  'if(sellTo&&sellTo!=="all")url+="&sellTo="+encodeURIComponent(sellTo);if(source&&source!=="all")url+="&utmSource="+encodeURIComponent(source);if(enrich&&enrich!=="all")url+="&enrichment="+encodeURIComponent(enrich);if(websiteCheck&&websiteCheck!=="all")url+="&websiteCheck="+encodeURIComponent(websiteCheck);if(repeatAttempts&&repeatAttempts!=="all")url+="&repeatAttempts="+encodeURIComponent(repeatAttempts);if(hear)url+="&hearAbout="+encodeURIComponent(hear);if(search)url+="&search="+encodeURIComponent(search);if(from)url+="&dateFrom="+from;if(to)url+="&dateTo="+to;' +
  'document.getElementById("ltbody").innerHTML="<tr><td colspan=\\"10\\" class=\\"nd\\">Loading...</td></tr>";' +
  'try{var r=await fetch(url,{signal:AbortSignal.timeout(12000)});if(!r.ok)throw new Error("HTTP "+r.status);var d=await r.json();' +
  'set("lcount",d.total+" lead"+(d.total!==1?"s":"")+" found");' +
  'if(!d.leads.length){document.getElementById("ltbody").innerHTML="<tr><td colspan=\\"10\\" class=\\"nd\\">No leads match your filters.</td></tr>";document.getElementById("lpag").innerHTML="";return;}' +
  'var html=d.leads.map(function(l){var sid=esc(l.session_id),name=[l.first_name,l.last_name].filter(Boolean).map(esc).join(" ")||"\\u2014",src=l.utm_source?esc(l.utm_source)+(l.utm_medium?" / "+esc(l.utm_medium):""):(l.referrer?"referral":"\\u2014");' +
  'return"<tr><td class=\\"xbtn\\" onclick=\\"toggleRow(\'"+sid+"\')\\">&#9658;</td><td class=\\"te\\" title=\\""+esc(l.email)+"\\">"+(l.website_check_failed?"<span style=\\"color:#b91c1c\\">&#9888;&#65039; </span>":(l.website_check_reason==="social_profile_url"?"<span style=\\"color:#1d4ed8\\" title=\\"Social profile \\u2014 no company site\\">&#128279; </span>":""))+esc(l.email||"\\u2014")+"</td><td>"+name+"</td><td class=\\"tc\\">"+esc(l.company||"\\u2014")+"</td><td>"+esc(l.sell_to||"\\u2014")+"</td><td>"+stageBadge(l)+"</td><td>"+(l.booking_uid?"<span class=\\"badge bg\\">Yes</span>":"<span class=\\"badge bx\\">No</span>")+"</td><td>"+enrichBadge(l)+"</td><td style=\\"color:#999;white-space:nowrap\\">"+ist(l.created_at)+"</td><td style=\\"color:#999;font-size:11px\\">"+src+"</td></tr>"+' +
  '"<tr class=\\"erow\\" id=\\"er-"+sid+"\\" style=\\"display:none\\"><td></td><td colspan=\\"9\\">"+enrichPanel(l)+"</td></tr>";}).join("");' +
  'document.getElementById("ltbody").innerHTML=html;renderPag(d.page,d.pages);}catch(e){document.getElementById("ltbody").innerHTML="<tr><td colspan=\\"10\\" class=\\"nd\\" style=\\"color:#b91c1c\\">Failed: "+esc(e.message)+"</td></tr>";}}' +
  'function renderPag(pg,pages){if(pages<=1){document.getElementById("lpag").innerHTML="";return;}var h="";h+="<button class=\\"pb\\" onclick=\\"loadLeads("+(pg-1)+")\\""+(pg<=1?" disabled":"")+">&larr;</button>";var s=Math.max(1,pg-2),e=Math.min(pages,pg+2);if(s>1)h+="<button class=\\"pb\\" onclick=\\"loadLeads(1)\\">1</button>"+(s>2?"<span class=\\"pi\\">&#8230;</span>":"");for(var i=s;i<=e;i++)h+="<button class=\\"pb"+(i===pg?" act":"")+ "\\" onclick=\\"loadLeads("+i+")\\" >"+i+"</button>";if(e<pages)h+=(e<pages-1?"<span class=\\"pi\\">&#8230;</span>":"")+"<button class=\\"pb\\" onclick=\\"loadLeads("+pages+")\\" >"+pages+"</button>";h+="<button class=\\"pb\\" onclick=\\"loadLeads("+(pg+1)+")\\"" +(pg>=pages?" disabled":"")+">&rarr;</button><span class=\\"pi\\">Page "+pg+" of "+pages+"</span>";document.getElementById("lpag").innerHTML=h;}' +
  'var lmLeads=[],lmChart=null,lmFilter="all";' +
  'var lmPillDefs=[["all","All"],["awaiting","Awaiting send"],["sent","Sent"],["abandoned","Abandoned"],["internal","Internal tests"]];' +
  'function lmPct(a,b){return b>0?Math.round(a/b*100)+"%":"\\u2014";}' +
  'function lmIST(t){if(!t)return "\\u2014";return new Date(t).toLocaleString("en-IN",{timeZone:"Asia/Kolkata",day:"2-digit",month:"short",hour:"2-digit",minute:"2-digit"});}' +
  'function lmBars(rows,total){if(!rows.length)return "<div class=\\"nd\\">No data yet</div>";' +
  'return rows.map(function(r){var p=total>0?Math.round(r.n/total*100):0;' +
  'return "<div style=\\"margin-bottom:8px\\"><div style=\\"display:flex;justify-content:space-between;font-size:12px;margin-bottom:3px\\"><span>"+esc(r.label)+' +
  '(r.is_custom?" <span style=\\"color:#888;font-size:11px\\">custom</span>":"")+"</span><span style=\\"color:#888\\">"+r.n+"</span></div>"+' +
  '"<div style=\\"height:6px;background:#f0f0f0;border-radius:3px;overflow:hidden\\"><div style=\\"height:100%;width:"+p+"%;background:#1a1a1a\\"></div></div></div>";}).join("");}' +

  'function lmDrop(label,lost,base,hint){var p=base>0?Math.round(lost/base*100):0;' +
  'return "<div style=\\"display:flex;align-items:center;gap:12px;padding:9px 0;border-bottom:1px solid #f2f2f2\\">"+' +
  '"<div style=\\"flex:1\\"><div style=\\"font-size:13px\\">"+label+"</div><div class=\\"ms\\">"+hint+"</div></div>"+' +
  '"<div style=\\"width:130px;height:6px;background:#f0f0f0;border-radius:3px;overflow:hidden\\"><div style=\\"height:100%;width:"+p+"%;background:"+(p>50?"#b91c1c":"#f59e0b")+"\\"></div></div>"+' +
  '"<div style=\\"width:96px;text-align:right;font-size:13px\\"><b>"+lost+"</b> <span style=\\"color:#888\\">("+p+"%)</span></div></div>";}' +

  'async function loadLM(){' +
  'var dsel=document.getElementById("lm-days");var dq=(TP?TP+"&":"?")+"days="+(dsel?dsel.value:30);' +
  'try{var r=await fetch(API+"/monitor/lm-metrics"+dq,{cache:"no-store"});var d=await r.json();var f=d.funnel||{};' +
  'var v=+f.views||0,o=+f.modal_opens||0,e=+f.emails||0,sb=+f.submitted||0;' +
  'set("lm-views",v);set("lm-opens",o);set("lm-emails",e);set("lm-submitted",sb);' +
  'set("lm-opens-r",lmPct(o,v)+" of views");set("lm-emails-r",lmPct(e,o)+" of opens");set("lm-submitted-r",lmPct(sb,e)+" of emails");' +
  'var pp=+f.people||0,ps=+f.people_submitted||0,pa=+f.people_abandoned||0;' +
  'set("lm-people","Sessions = visits \u00B7 People = distinct emails. "+pp+" people entered an email \u2014 "+ps+" completed, "+pa+" did not.");' +
  'document.getElementById("lm-dropoff").innerHTML=' +
  'lmDrop("Left without opening the form",(+f.bounced_before_open||0),v,"Saw the page, never clicked a CTA")+' +
  'lmDrop("Opened the form, no email",(+f.opened_no_email||0),o,"Modal opened but no valid email entered")+' +
  'lmDrop("Entered email, never submitted",(+f.abandoned||0),e,"Verified email captured &#8212; these are recoverable")+' +
  '"<div style=\\"display:flex;padding:9px 0;font-size:13px\\"><div style=\\"flex:1\\">Completed</div>"+' +
  '"<div style=\\"width:96px;text-align:right\\"><b>"+sb+"</b> <span style=\\"color:#888\\">("+lmPct(sb,v)+" of views)</span></div></div>";' +
  'document.getElementById("lm-inds").innerHTML=lmBars(d.industries||[],sb);' +
  'var fr=+f.free_email||0,bz=+f.business_email||0;' +
  'document.getElementById("lm-emailtype").innerHTML=lmBars([{label:"Business email",n:bz},{label:"Free mailbox",n:fr}],fr+bz);' +
  'var eps=d.entry_points||[];var epMax=eps.length?eps[0].n:0;' +
  'document.getElementById("lm-entries").innerHTML=eps.length?eps.map(function(x){' +
  'var w=epMax>0?Math.round(x.n/epMax*100):0;var cvr=x.n>0?Math.round(x.completed/x.n*100):0;' +
  'return "<div style=\\"margin-bottom:9px\\"><div style=\\"display:flex;justify-content:space-between;font-size:12px;margin-bottom:3px\\">"+' +
  '"<span>"+esc(x.label)+"</span><span style=\\"color:#888\\">"+x.n+" opens \\u00B7 "+cvr+"% submitted</span></div>"+' +
  '"<div style=\\"height:6px;background:#f0f0f0;border-radius:3px;overflow:hidden\\"><div style=\\"height:100%;width:"+w+"%;background:#1a1a1a\\"></div></div></div>";' +
  '}).join(""):"<div class=\\"nd\\">No opens recorded yet \\u2014 needs the v4.4 embed.</div>";' +
  'var cc=d.custom_categories||[];' +
  'document.getElementById("lm-custom").innerHTML=cc.length?cc.map(function(x){' +
  'return "<div style=\\"display:flex;justify-content:space-between;padding:5px 0;border-bottom:1px solid #f0f0f0;font-size:13px\\"><span>"+esc(x.label)+"</span><span style=\\"color:#888\\">"+x.n+"</span></div>";}).join(""):' +
  '"<div class=\\"nd\\">None yet \\u2014 the dropdown is covering everyone so far.</div>";' +
  'var dy=d.daily||[],cv=document.getElementById("lm-chart");' +
  'if(cv&&window.Chart){if(lmChart)lmChart.destroy();lmChart=new Chart(cv,{type:"line",data:{labels:dy.map(function(x){return x.day.slice(5);}),' +
  'datasets:[{label:"Views",data:dy.map(function(x){return x.views;}),borderColor:"#d4d4d4",backgroundColor:"#d4d4d4",tension:0.25,pointRadius:0,borderWidth:2},' +
  '{label:"Email entered",data:dy.map(function(x){return x.emails;}),borderColor:"#f59e0b",backgroundColor:"#f59e0b",tension:0.25,pointRadius:0,borderWidth:2},' +
  '{label:"Submitted",data:dy.map(function(x){return x.submitted;}),borderColor:"#1a1a1a",backgroundColor:"#1a1a1a",tension:0.25,pointRadius:0,borderWidth:2}]},' +
  'options:{responsive:true,interaction:{mode:"index",intersect:false},plugins:{legend:{display:true,labels:{boxWidth:10,font:{size:11}}}},scales:{y:{beginAtZero:true,ticks:{precision:0}}}}});}' +
  '}catch(err){console.warn("[LM] metrics failed",err);}' +
  'try{var r2=await fetch(API+"/monitor/lm-leads"+dq,{cache:"no-store"});var d2=await r2.json();lmLeads=d2.leads||[];lmRender();' +
  '}catch(e2){document.getElementById("lm-tbody").innerHTML="<tr><td colspan=\\"10\\" class=\\"nd\\">Failed to load</td></tr>";}}' +

  'function lmMatch(l){' +
  'if(lmFilter==="internal")return l.is_internal;' +
  'if(l.is_internal)return false;' +
  'if(lmFilter==="all")return true;' +
  'return l.status===lmFilter;}' +
  'function lmSearched(){var q=(document.getElementById("lm-search").value||"").toLowerCase().trim();' +
  'var base=lmLeads.filter(lmMatch);if(!q)return base;' +
  'return base.filter(function(l){return [l.email,l.industry_category,l.product_or_service,l.website,l.utm_campaign,l.utm_source].join(" ").toLowerCase().indexOf(q)>=0;});}' +
  'function lmSetFilter(k){lmFilter=k;lmRender();}' +

  'function lmRender(){' +
  'var counts={all:0,awaiting:0,sent:0,abandoned:0,internal:0};' +
  'lmLeads.forEach(function(l){if(l.is_internal){counts.internal++;return;}counts.all++;if(counts[l.status]!==undefined)counts[l.status]++;});' +
  'document.getElementById("lm-pills").innerHTML=lmPillDefs.map(function(p){var on=lmFilter===p[0];' +
  'return "<button onclick=\\"lmSetFilter(\'"+p[0]+"\')\\" style=\\"padding:5px 11px;border-radius:99px;font-size:12px;cursor:pointer;border:1px solid "+' +
  '(on?"#1a1a1a":"#e5e5e5")+";background:"+(on?"#1a1a1a":"#fff")+";color:"+(on?"#fff":"#444")+' +
  '"\\">"+p[1]+" <span style=\\"opacity:.6\\">"+(counts[p[0]]||0)+"</span></button>";}).join("");' +
  'var rows=lmSearched();var tb=document.getElementById("lm-tbody");' +
  'set("lm-count",rows.length+" shown");' +
  'if(!rows.length){tb.innerHTML="<tr><td colspan=\\"10\\" class=\\"nd\\">Nothing matches</td></tr>";return;}' +
  'tb.innerHTML=rows.map(function(l,i){' +
  'var badge=l.is_internal?"<span style=\\"color:#888\\">internal</span>":' +
  '(l.status==="sent"?"<span class=\\"dot dot-green\\"></span> sent":' +
  '(l.status==="awaiting"?"<span class=\\"dot dot-amber\\"></span> awaiting":' +
  '"<span class=\\"dot\\" style=\\"background:#b91c1c\\"></span> abandoned"));' +
  'var act=(l.status==="awaiting"&&!l.is_internal)?"<button class=\\"btn\\" onclick=\\"lmMark("+l.id+",0)\\">Mark sent</button>":' +
  '(l.status==="sent"?"<button class=\\"btn\\" onclick=\\"lmMark("+l.id+",1)\\">Undo</button>":"");' +
  'return "<tr"+(l.is_internal?" style=\\"opacity:.5\\"":"")+">"+' +
  '"<td><span id=\\"lm-x-"+i+"\\" onclick=\\"lmToggle("+i+")\\" style=\\"cursor:pointer;color:#888\\">&#9654;</span></td>"+' +
  '"<td>"+esc(l.email)+(l.is_free_email?" <span style=\\"color:#f59e0b\\" title=\\"Free mailbox\\">&#9679;</span>":"")+' +
  '(l.attempts>1?" <span class=\\"ms\\">&times;"+l.attempts+"</span>":"")+"</td>"+' +
  '"<td>"+esc(l.industry_category||"\\u2014")+(l.industry_is_custom?" <span style=\\"color:#888\\">(custom)</span>":"")+"</td>"+' +
  '"<td>"+esc(l.product_or_service||"\\u2014")+"</td><td>"+esc(l.sell_to||"\\u2014")+"</td>"+' +
  '"<td>"+esc(l.website||"\\u2014")+(l.website_source==="derived_from_email"?" <span style=\\"color:#888\\">(from email)</span>":"")+"</td>"+' +
  '"<td>"+esc(l.utm_source||l.referrer||"direct")+"</td>"+' +
  '"<td>"+badge+"</td>"+' +
  '"<td>"+lmIST(l.submitted_at||l.created_at)+"</td>"+' +
  '"<td>"+act+"</td></tr>"+' +
  '"<tr class=\\"erow\\" id=\\"lm-er-"+i+"\\" style=\\"display:none\\"><td></td><td colspan=\\"9\\">"+lmDetail(l)+"</td></tr>";' +
  '}).join("");}' +

  'function lmCell(k,v,link,raw){if(!v)v="\\u2014";' +
  'var body=raw?v:(link&&v!=="\\u2014"?"<a href=\\""+esc(v)+"\\" target=\\"_blank\\" style=\\"word-break:break-all\\">"+esc(v)+"</a>":"<span style=\\"word-break:break-all\\">"+esc(v)+"</span>");' +
  'return "<div style=\\"background:#fafafa;border-radius:6px;padding:8px 10px\\"><div class=\\"ms\\" style=\\"margin-bottom:2px\\">"+k+"</div><div style=\\"font-size:12px\\">"+body+"</div></div>";}' +
  'function lmDetail(l){' +
  'return "<div style=\\"display:grid;grid-template-columns:repeat(auto-fit,minmax(190px,1fr));gap:8px;padding:10px 0\\">"+' +
  'lmCell("Attempts",l.attempts>1?l.attempts+" sessions from this email":"First attempt")+' +
  'lmCell("Entered from",l.entry_point)+lmCell("ELV status",l.elv_status)+' +
  'lmCell("Reached step",l.step_reached+" of 4")+' +
  'lmCell("UTM source",l.utm_source)+lmCell("UTM medium",l.utm_medium)+' +
  'lmCell("UTM campaign",l.utm_campaign)+lmCell("UTM content",l.utm_content)+lmCell("UTM term",l.utm_term)+' +
  'lmCell("Referrer",l.referrer)+' +
  'lmCell("Landing page",l.landing_page,1)+lmCell("Previous page",l.previous_page,1)+lmCell("Form page",l.page_url,1)+' +
  'lmCell("Meta fbc",l.fbc)+lmCell("Meta fbp",l.fbp)+' +
  'lmCell("Meta Contact sent",l.capi_contact_sent?"Yes":"No")+' +
  'lmCell("Loops",l.loops_sent?("Sent "+lmIST(l.loops_sent_at)):(l.loops_error?("Failed: "+l.loops_error):"Not sent"))+' +
  '(l.loops_error||(!l.loops_sent&&l.completed)?lmCell("Retry","<button class=\\"btn\\" onclick=\\"lmLoopsRetry("+l.id+")\\">Push to Loops</button>",0,1):"")+' +
  'lmCell("Submitted",lmIST(l.submitted_at))+lmCell("First seen",lmIST(l.created_at))+' +
  'lmCell("Delivered at",lmIST(l.delivered_at))+' +
  'lmCell("Session ID",l.session_id)+"</div>";}' +
  'function lmToggle(i){var r=document.getElementById("lm-er-"+i);if(!r)return;' +
  'var vis=r.style.display!=="none";r.style.display=vis?"none":"table-row";' +
  'var x=document.getElementById("lm-x-"+i);if(x)x.innerHTML=vis?"&#9654;":"&#9660;";}' +

  'async function lmLoopsRetry(id){' +
  'try{var r=await fetch(API+"/monitor/lm-loops-retry/"+id+TP,{method:"POST"});var d=await r.json();' +
  'alert(d.ok?"Pushed to Loops":"Failed: "+(d.error||"unknown"));loadLM();' +
  '}catch(e){alert("Retry failed: "+e.message);}}' +
  'async function lmMark(id,undo){' +
  'try{var u=API+"/monitor/lm-delivered/"+id+(TP?TP+"&":"?")+(undo?"undo=1":"undo=0");' +
  'var r=await fetch(u,{method:"POST"});if(!r.ok)throw new Error("HTTP "+r.status);' +
  'var l=lmLeads.filter(function(x){return x.id===id;})[0];' +
  'if(l){l.delivered=!undo;l.status=undo?"awaiting":"sent";l.delivered_at=undo?null:new Date().toISOString();}' +
  'lmRender();loadLM();' +
  '}catch(e){alert("Could not update: "+e.message);}}' +

  'function lmCsv(){var rows0=lmSearched();if(!rows0.length)return;' +
  'var cols=["email","status","industry_category","industry_is_custom","product_or_service","sell_to","website","website_source","is_free_email","elv_status","entry_point","attempts","utm_source","utm_medium","utm_campaign","utm_content","utm_term","referrer","landing_page","previous_page","page_url","submitted_at","delivered","delivered_at","session_id"];' +
  'var Q=String.fromCharCode(34);' +
  'var q=function(v){return Q+String(v==null?"":v).split(Q).join(Q+Q)+Q;};' +
  'var out=[cols.join(",")].concat(rows0.map(function(l){return cols.map(function(c){return q(l[c]);}).join(",");}));' +
  'var a=document.createElement("a");' +
  'a.href=URL.createObjectURL(new Blob([out.join(String.fromCharCode(10))],{type:"text/csv"}));' +
  'a.download="lead-magnet-"+new Date().toISOString().slice(0,10)+".csv";a.click();}' +
  'async function loadAll(){set("lupd","Refreshing...");var ok=await checkApi();if(!ok){document.getElementById("alerts").innerHTML="<div class=\\"alertbox ae\\"><span>x</span><span>API offline.</span></div>";set("lupd","API offline");return;}checkElv();' +
  'try{var r=await fetch(API+"/monitor/metrics"+TP,{signal:AbortSignal.timeout(12000)});if(!r.ok)throw new Error("HTTP "+r.status);var d=await r.json();' +
  'set("m-total",d.peopleTotal);set("m-totals",d.total+" sessions \\u00B7 "+d.todayCount+" in last 24h");' +
  'set("m-comp",d.peopleCompleted);set("m-cpct",pct(d.peopleCompleted,d.peopleTotal)+" of people \\u00B7 "+d.completed+" sessions");' +
  'set("m-book",d.peopleBooked);set("m-bpct",pct(d.peopleBooked,d.peopleCompleted)+" of completed \\u00B7 "+d.booked+" sessions");' +
  'set("m-disq",d.peopleDisqualified);set("m-dsq","B2C / Mixed \\u00B7 "+d.disqualified+" sessions");' +
  'set("m-nb",d.peopleNoBooking);set("m-nbs",d.completedNoBookingSessions+" completed sessions w/o booking");' +
  'set("m-rec",d.recoveredBookings);set("m-pend",d.pendingPartials);set("m-mail",d.loopsSent);' +
  'set("recon","Sessions = form visits \\u00B7 People = distinct emails. "+d.completedNoBookingSessions+" completed sessions without a booking \\u2192 "+d.noBookingUid+" actionable people after dedup, cross-session bookings & B2B filter.");' +
  'var er=d.total?Math.round(d.enriched/d.total*100):0,brP=d.peopleCompleted?Math.round(d.peopleBooked/d.peopleCompleted*100):0;' +
  'badge("s-partial",d.total+" sessions saved","bg");badge("s-submit",d.completed>0?d.completed+" completed sessions":"No completions",d.completed>0?"bg":"ba");badge("s-enrich",er+"% enriched",er>=60?"bg":er>=30?"ba":"br");badge("s-cal",brP+"% booking rate (people)",brP>=50?"bg":brP>=20?"ba":"bx");badge("s-cron",d.pendingPartials===0?"No pending":d.pendingPartials+" pending",d.pendingPartials===0?"bg":"ba");badge("s-aws",d.awsSynced?"Active":"Disabled",d.awsSynced?"bg":"br");badge("s-loops",d.loopsSent+" emails sent",d.loopsSent>0?"bg":"bx");' +
  'set("h-enr",d.enriched);set("h-tit",d.enrichTitlePct!==undefined?d.enrichTitlePct+"%":"\\u2014");set("h-fun",d.enrichFundingPct!==undefined?d.enrichFundingPct+"%":"\\u2014");set("h-loc",d.enrichLocationPct!==undefined?d.enrichLocationPct+"%":"\\u2014");' +
  'renderAlerts(d);renderFunnel(d.peopleTotal,d.peopleCompleted,d.peopleBooked,d.peopleDisqualified);renderChart(d.leadsByDay||[]);' +
  'set("lupd","Updated "+new Date().toLocaleTimeString("en-IN",{timeZone:"Asia/Kolkata"})+" IST");' +
  '}catch(e){document.getElementById("alerts").innerHTML="<div class=\\"alertbox ae\\"><span>x</span><span>Failed: "+esc(e.message)+"</span></div>";set("lupd","Error");}' +
  'if(document.getElementById("tp-leads").classList.contains("act"))loadLeads(curPage);}' +
  'var sdrData=[],sdrTimer=null;' +
  'function sdrDebounce(){clearTimeout(sdrTimer);sdrTimer=setTimeout(function(){renderSDRTable(sdrData);},300);}' +
  'async function loadSDR(){' +
  'document.getElementById("sdr-tbody").innerHTML="<tr><td colspan=\\"9\\" class=\\"nd\\">Loading...</td></tr>";' +
  'try{' +
  'var r=await fetch(API+"/monitor/sdr"+(TP||"?")+(TP?"&":"")+"_="+Date.now(),{signal:AbortSignal.timeout(15000)});' +
  'if(!r.ok)throw new Error("HTTP "+r.status);' +
  'var d=await r.json();' +
  'sdrData=d.leads||[];' +
  'renderSDRTable(sdrData);' +
  '}catch(e){document.getElementById("sdr-tbody").innerHTML="<tr><td colspan=\\"9\\" class=\\"nd\\" style=\\"color:#b91c1c\\">Failed: "+esc(e.message)+"</td></tr>";}}' +
  'function sdrPanel(l){' +
  'var fields=[' +
  '{lb:"\\uD83D\\uDCDE Phone",v:l.phone},' +
  '{lb:"\\uD83D\\uDCAC Heard about us",v:l.hear_about_us},' +
  '{lb:"\\uD83C\\uDF10 Website",v:l.website,lnk:true},' +
  '{lb:"Source",v:l.utm_source?([l.utm_source,l.utm_medium].filter(Boolean).join(" / ")):null},' +
  '{lb:"Campaign",v:l.utm_campaign},' +
  '{lb:"Referrer",v:l.referrer},' +
  '{lb:"\\uD83D\\uDEEC Landing Page",v:l.landing_page,lnk:true},' +
  '{lb:"Seniority",v:l.enriched_seniority},' +
  '{lb:"Department",v:l.enriched_departments},' +
  '{lb:"Location",v:l.enriched_city&&l.enriched_country?l.enriched_city+", "+l.enriched_country:l.enriched_country||null},' +
  '{lb:"Annual Revenue",v:l.enriched_annual_revenue},' +
  '{lb:"Total Funding",v:l.enriched_total_funding},' +
  '{lb:"Funding Stage",v:l.enriched_funding_stage},' +
  '{lb:"Submitted",v:ist(l.submitted_at)},' +
  '].filter(function(f){return f.v;});' +
  'if(!fields.length)return"<div style=\\"color:#999;font-size:12px\\">No additional details.</div>";' +
  'return"<div class=\\"egrid\\">"+fields.map(function(f){var val=f.lnk&&f.v?"<a href=\\""+(f.v.startsWith("http")?"":"https://")+esc(f.v)+"\\" target=\\"_blank\\">"+esc(f.v)+"</a>":esc(f.v);return"<div class=\\"ef\\"><div class=\\"efl\\">"+f.lb+"</div><div class=\\"efv\\">"+val+"</div></div>";}).join("")+"</div>";}' +
  'function toggleSDRRow(idx){var row=document.getElementById("sdr-er-"+idx);if(!row)return;var vis=row.style.display!=="none";row.style.display=vis?"none":"table-row";var btn=document.getElementById("sdr-xbtn-"+idx);if(btn)btn.textContent=vis?"\\u25B6":"\\u25BC";}' +
  'function renderSDRTable(allLeads){' +
  'var q=(document.getElementById("sdr-search")||{}).value||"";' +
  'var leads=q?allLeads.filter(function(l){var s=q.toLowerCase();return(l.email||"").toLowerCase().includes(s)||(l.company||"").toLowerCase().includes(s)||(l.first_name||"").toLowerCase().includes(s)||(l.enriched_industry||"").toLowerCase().includes(s);}):allLeads;' +
  'set("sdr-count",leads.length+" lead"+(leads.length!==1?"s":""));' +
  'if(!leads.length){document.getElementById("sdr-tbody").innerHTML="<tr><td colspan=\\"10\\" class=\\"nd\\">No leads found.</td></tr>";return;}' +
  'var html=leads.map(function(l,i){' +
  'var name=[l.first_name,l.last_name].filter(Boolean).map(esc).join(" ")||"\\u2014";' +
  'var stage=l.completed?"<span class=\\"badge bb\\">Completed</span>":"<span class=\\"badge ba\\">Step 1</span>";' +
  'var li=l.enriched_linkedin?"<a href=\\""+esc(l.enriched_linkedin)+"\\" target=\\"_blank\\" style=\\"color:#2563eb;text-decoration:none\\">View</a>":"\\u2014";' +
  'return"<tr><td class=\\"xbtn\\" id=\\"sdr-xbtn-"+i+"\\" onclick=\\"toggleSDRRow("+i+")\\">&#9658;</td><td class=\\"te\\" title=\\""+esc(l.email)+"\\">"+esc(l.email||"\\u2014")+"</td><td>"+name+"</td><td class=\\"tc\\">"+esc(l.company||"\\u2014")+"</td><td style=\\"color:#555\\">"+esc(l.enriched_title||"\\u2014")+"</td><td>"+esc(l.enriched_industry||"\\u2014")+"</td><td>"+esc(l.enriched_company_size||"\\u2014")+"</td><td>"+stage+"</td><td>"+li+"</td><td style=\\"color:#999;white-space:nowrap\\">"+ist(l.created_at)+"</td></tr>"' +
  '+"<tr class=\\"erow\\" id=\\"sdr-er-"+i+"\\" style=\\"display:none\\"><td></td><td colspan=\\"9\\">"+sdrPanel(l)+"</td></tr>";' +
  '}).join("");' +
  'document.getElementById("sdr-tbody").innerHTML=html;}' +
  'function exportSDR(){window.location.href=API+"/monitor/sdr"+(TP||"?")+(TP?"&":"")+"format=csv";}' +
  'async function loadDupes(){' +
  'document.getElementById("dupes-tbody").innerHTML="<tr><td colspan=\\"7\\" class=\\"nd\\">Loading...</td></tr>";' +
  'try{' +
  'var r=await fetch(API+"/monitor/duplicates"+(TP||"?")+(TP?"&":"")+"_="+Date.now(),{signal:AbortSignal.timeout(15000)});' +
  'if(!r.ok)throw new Error("HTTP "+r.status);' +
  'var d=await r.json();' +
  'set("dupes-count",d.total+" email"+(d.total!==1?"s":"")+" with multiple sessions");' +
  'if(!d.leads.length){document.getElementById("dupes-tbody").innerHTML="<tr><td colspan=\\"7\\" class=\\"nd\\">No duplicates found.</td></tr>";return;}' +
  'var html=d.leads.map(function(l,i){' +
  'var booked=l.has_booking?"<span class=\\"badge bg\\">Yes</span>":"<span class=\\"badge bx\\">No</span>";' +
  'var comp=l.has_completed?"<span class=\\"badge bb\\">Yes</span>":"<span class=\\"badge bx\\">No</span>";' +
  'var sessRows=(l.sessions||[]).map(function(s){' +
  'var sb=s.booking_uid?"<span class=\\"badge bg\\">Booked</span>":s.disqualified?"<span class=\\"badge br\\">Disqualified</span>":s.completed?"<span class=\\"badge bb\\">Completed</span>":"<span class=\\"badge ba\\">Step "+s.step_reached+"</span>";' +
  'return"<div style=\\"display:flex;gap:12px;align-items:center;padding:5px 0;border-bottom:1px solid #f5f5f5;font-size:11px;color:#555\\">"+' +
  '"<span style=\\"color:#aaa;font-family:monospace\\">"+esc(s.session_id.slice(0,16))+"...</span>"+' +
  '"<span>"+sb+"</span>"+' +
  '"<span style=\\"color:#aaa\\">"+ist(s.created_at)+"</span>"+' +
  '"<span style=\\"color:#aaa\\">"+esc(s.page_url||"")+"</span>"+' +
  '"</div>";}).join("");' +
  'return"<tr><td class=\\"xbtn\\" id=\\"dupe-xbtn-"+i+"\\" onclick=\\"toggleDupeRow("+i+")\\">&#9658;</td>"+' +
  '"<td class=\\"te\\">"+esc(l.email)+"</td>"+' +
  '"<td><span class=\\"badge br\\">"+l.session_count+" sessions</span></td>"+' +
  '"<td>"+booked+"</td><td>"+comp+"</td>"+' +
  '"<td style=\\"color:#999;white-space:nowrap\\">"+ist(l.first_seen)+"</td>"+' +
  '"<td style=\\"color:#999;white-space:nowrap\\">"+ist(l.last_seen)+"</td>"+' +
  '"</tr>"+' +
  '"<tr class=\\"erow\\" id=\\"dupe-er-"+i+"\\" style=\\"display:none\\"><td></td><td colspan=\\"6\\"><div style=\\"padding:4px 0\\">"+sessRows+"</div></td></tr>";' +
  '}).join("");' +
  'document.getElementById("dupes-tbody").innerHTML=html;' +
  '}catch(e){document.getElementById("dupes-tbody").innerHTML="<tr><td colspan=\\"7\\" class=\\"nd\\" style=\\"color:#b91c1c\\">Failed: "+esc(e.message)+"</td></tr>";}}' +
  'function toggleDupeRow(i){var row=document.getElementById("dupe-er-"+i);if(!row)return;var vis=row.style.display!=="none";row.style.display=vis?"none":"table-row";var btn=document.getElementById("dupe-xbtn-"+i);if(btn)btn.textContent=vis?"\\u25B6":"\\u25BC";}' +
  'renderSortArrows();loadAll();setInterval(loadAll,60000);' +
  '<\/script></body></html>';

  res.setHeader('Content-Type', 'text/html');
  res.send(html + js);
});

/* ── ELV email verification + health tracking ──────────────────────
   REBUILT 2026-08-07. Four defects fixed, in order of impact:

   1. THE STATUS BUCKETS OVERLAPPED. 'error' — ELV's standard response
      for a genuinely invalid mailbox — was in BOTH blockedStatuses and
      ELV_INDETERMINATE. So every correctly-rejected bad email ALSO
      counted as "the check learned nothing", inflating the degradation
      rate. The alert was substantially measuring bad leads, not a
      broken ELV. Buckets are now mutually exclusive and audited at boot.

   2. THE WINDOW WAS COUNT-BASED, NOT TIME-BASED. 20 checks at ~10
      leads/day spans nearly two days, so a bad patch overnight kept
      alerting through the next morning while it aged out one slot per
      lead (12/20 at 06:50 → 8/20 at 10:05 was exactly four leads
      later). The window is now 90 minutes and self-clears.

   3. IT RE-ALERTED WHILE ALREADY DEGRADED. Every cooldown expiry
      re-fired the same standing condition. It is now a state machine:
      one alert entering degraded, one on recovery, silence between.
      Hysteresis (50% in, 25% out) stops flapping across the threshold —
      the old 40% line was being straddled, which is why it repeated.

   4. UNKNOWN STATUSES PASSED INVISIBLY. ELV returns 'invalid_syntax'
      (and p_/t_ prefixed variants of several others) which weren't in
      the known list, so a malformed address was treated as a pass AND
      never counted toward health. Prefixes are normalised, unknowns now
      count as indeterminate, and invalid_syntax blocks.

   Added: a local syntax + MX gate so obvious garbage is rejected in
   ~50ms without spending an ELV credit or making the lead wait 8s, and
   a 24h result cache so one address is never verified twice in a session.
   ───────────────────────────────────────────────────────────────── */

// ELV prefixes "probable" (p_) and "temporary" (t_) variants of several
// statuses — p_antispam_system, t_email_disabled, p_unknown_email etc.
// Strip the prefix before matching, or none of them ever match.
function normaliseElvStatus(raw) {
  return String(raw == null ? '' : raw).trim().toLowerCase().replace(/^(?:p_|t_)/, '');
}

// ── THREE MUTUALLY EXCLUSIVE BUCKETS ──
// BLOCK — ELV reached a definitive negative. Reject the address.
const ELV_BLOCK = [
  'error', 'invalid', 'invalid_syntax', 'syntax_error', 'invalid_mx',
  'unknown_email', 'email_disabled', 'domain_error', 'dead_server',
  'disposable', 'spamtrap', 'attempt_rejected', 'relay_error',
];
// PASS — definitive positive, or a benign catch-all / role result.
// ok_for_all and accept_all are catch-all domains: extremely common in
// B2B and NOT a sign of anything wrong, so they must not read as noise.
const ELV_PASS = ['ok', 'ok_for_all', 'accept_all', 'role'];
// INDETERMINATE — the check itself did not conclude. Fails open, and this
// is the ONLY bucket that feeds the degradation signal.
const ELV_INDETERMINATE = [
  'smtp_protocol', 'antispam_system', 'smtp_error', 'unknown', 'no_connect',
  'http_error', 'timeout', 'network_error', 'skipped',
];

// Boot-time audit. This is precisely the class of bug that produced the
// false degradation alerts, so it now fails loudly instead of silently.
(function auditElvBuckets() {
  const seen = new Map();
  const overlaps = [];
  [['BLOCK', ELV_BLOCK], ['PASS', ELV_PASS], ['INDETERMINATE', ELV_INDETERMINATE]].forEach(([name, list]) => {
    list.forEach(s => {
      if (seen.has(s)) overlaps.push(`"${s}" is in both ${seen.get(s)} and ${name}`);
      else seen.set(s, name);
    });
  });
  if (overlaps.length) {
    console.error('[ELV] ❌ STATUS BUCKET OVERLAP —', overlaps.join('; '));
    alertOps('critical', 'ELV', 'Status buckets overlap', {
      'Overlaps': overlaps.join('; '),
      'Impact': 'Blocking decisions and health metrics disagree — degradation alerts will be wrong',
    });
  } else {
    console.log(`[ELV] ✅ Status buckets clean — ${ELV_BLOCK.length} block / ${ELV_PASS.length} pass / ${ELV_INDETERMINATE.length} indeterminate`);
  }
})();

const ELV_WINDOW_MS       = Number(process.env.ELV_WINDOW_MS)       || 90 * 60 * 1000; // time-bounded, not count-bounded
const ELV_MIN_SAMPLE      = Number(process.env.ELV_MIN_SAMPLE)      || 8;              // never judge on thin traffic
const ELV_DEGRADED_RATE   = Number(process.env.ELV_DEGRADED_RATE)   || 0.5;
const ELV_RECOVERED_RATE  = Number(process.env.ELV_RECOVERED_RATE)  || 0.25;           // hysteresis gap prevents flapping
const ELV_TIMEOUT_MS      = Number(process.env.ELV_TIMEOUT_MS)      || 8000;
const ELV_WINDOW_MAX      = 200; // hard cap so a traffic burst can't grow the window unbounded

// Internal and throwaway test traffic must never move ops state. Manual
// testing of utsav,singh@gushwork.ai fired a real Slack page at 4:22pm —
// that was noise, not an incident.
const ELV_EXCLUDED_DOMAINS = ['gushwork.ai', 'test.com', 'example.com', 'example.org'];

const _elvWindow    = [];   // [{ t: ms, bad: bool }]
let   _elvDegraded  = false;
let   _elvLastStatus = null;
let   _elvLastCheckAt = 0;

function elvIsInternal(email) {
  const domain = String(email || '').split('@')[1] || '';
  return ELV_EXCLUDED_DOMAINS.includes(domain.toLowerCase());
}

function pruneElvWindow(now) {
  const cutoff = now - ELV_WINDOW_MS;
  while (_elvWindow.length && _elvWindow[0].t < cutoff) _elvWindow.shift();
  while (_elvWindow.length > ELV_WINDOW_MAX) _elvWindow.shift();
}

function elvHealthSnapshot() {
  const now = Date.now();
  pruneElvWindow(now);
  const checks = _elvWindow.length;
  const bad    = _elvWindow.filter(e => e.bad).length;
  return {
    state:         _elvDegraded ? 'degraded' : (checks < ELV_MIN_SAMPLE ? 'insufficient_data' : 'healthy'),
    inconclusive:  bad,
    checks,
    rate:          checks ? Math.round((bad / checks) * 100) : 0,
    windowMinutes: Math.round(ELV_WINDOW_MS / 60000),
    minSample:     ELV_MIN_SAMPLE,
    cacheSize:     _elvCache.size,
    lastStatus:    _elvLastStatus,
    lastCheckAt:   _elvLastCheckAt ? new Date(_elvLastCheckAt).toISOString() : null,
  };
}

function recordElvOutcome(status, email) {
  try {
    const now = Date.now();
    _elvLastStatus  = status;
    _elvLastCheckAt = now;
    if (email && elvIsInternal(email)) return; // own testing never moves health state

    _elvWindow.push({ t: now, bad: ELV_INDETERMINATE.includes(status) });
    pruneElvWindow(now);

    const checks = _elvWindow.length;
    if (checks < ELV_MIN_SAMPLE) {
      // Too little recent signal to make a claim. If we were degraded and
      // traffic has since dried up, clear silently rather than hold a
      // stale state overnight — the old code's core failure.
      _elvDegraded = false;
      return;
    }
    const rate = _elvWindow.filter(e => e.bad).length / checks;

    if (!_elvDegraded && rate >= ELV_DEGRADED_RATE) {
      _elvDegraded = true;
      alertOps('warning', 'ELV', 'Verification degraded', {
        'Inconclusive': `${Math.round(rate * 100)}% of ${checks} checks in the last ${Math.round(ELV_WINDOW_MS / 60000)} min`,
        'Impact': 'Inconclusive results are passing unverified. Definitively bad emails are STILL being blocked.',
        'Action': 'Check ELV status/credits. Self-recovers — no action needed if it clears.',
      });
    } else if (_elvDegraded && rate <= ELV_RECOVERED_RATE) {
      _elvDegraded = false;
      // Recovery goes straight to Slack so it reads ✅ and can't be
      // swallowed by the warning-severity cooldown.
      const heading = '✅ ELV — Verification recovered';
      sendOpsSlack([
        bHeader(heading),
        bDivider(),
        bFields([{ label: 'Inconclusive', value: `${Math.round(rate * 100)}% of ${checks} recent checks` }]),
        bContext(`Severity: *info* · ${new Date().toISOString()}`),
      ].filter(Boolean), heading);
      console.log('[ELV] ✅ Recovered — degradation cleared');
    }
  } catch (err) {
    console.warn('[ELV] health tracking error (ignored):', err && err.message);
  }
}

/* ── Result cache ──────────────────────────────────────────────────
   janene.kingsley@gmail.com was verified twice inside one session in
   the 07/08 logs. Every duplicate is a wasted credit and 1-8s of extra
   wait for the lead. Only DEFINITIVE outcomes are cached — inconclusive
   ones must be retried, never remembered.
   ───────────────────────────────────────────────────────────────── */
const ELV_CACHE_TTL_MS = Number(process.env.ELV_CACHE_TTL_MS) || 24 * 60 * 60 * 1000;
const ELV_CACHE_MAX    = 5000;
const _elvCache        = new Map(); // email -> { valid, status, at }

function elvCacheGet(email) {
  const hit = _elvCache.get(email);
  if (!hit) return null;
  if (Date.now() - hit.at > ELV_CACHE_TTL_MS) { _elvCache.delete(email); return null; }
  return hit;
}
function elvCacheSet(email, valid, status) {
  if (!ELV_BLOCK.includes(status) && !ELV_PASS.includes(status)) return;
  if (_elvCache.size >= ELV_CACHE_MAX) _elvCache.delete(_elvCache.keys().next().value);
  _elvCache.set(email, { valid, status, at: Date.now() });
}

/* ── Local pre-checks ──────────────────────────────────────────────
   Deliberately conservative: these only reject what cannot possibly be
   a working address. Anything arguable is handed to ELV. The point is
   latency — abctest@test.com and hshbhs@jsbjbd.com should not cost the
   lead an 8-second wait to learn what a regex and a DNS lookup already
   know.
   ───────────────────────────────────────────────────────────────── */
const EMAIL_SYNTAX_RE = /^[^\s@,;:<>()[\]\\"]+@[^\s@,;:<>()[\]\\"]+\.[A-Za-z]{2,}$/;

async function localMxCheck(domain) {
  try {
    const records = await Promise.race([
      dnsPromises.resolveMx(domain),
      new Promise((_, rej) => setTimeout(() => rej(new Error('mx_timeout')), 2500)),
    ]);
    return Array.isArray(records) && records.length > 0 ? 'has_mx' : 'no_mx';
  } catch (err) {
    // NXDOMAIN is definitive: the domain does not exist at all.
    // Everything else (SERVFAIL, timeout) is inconclusive and MUST NOT
    // block — same fail-open philosophy as the website check.
    if (err && (err.code === 'ENOTFOUND' || err.code === 'NXDOMAIN')) return 'nxdomain';
    return 'inconclusive';
  }
}

/* ── Did-you-mean: domain typo suggestion ──────────────────────────
   Two REAL leads in the 07/08 logs were rejected on a gmail typo
   (b@gmai.com, utsavsingh5600@gmai.com). Both got a generic error and
   presumably left. Naming the likely fix turns a dead end into a
   one-tap correction.

   This NEVER changes the block/pass decision — it only supplies copy.
   ───────────────────────────────────────────────────────────────── */
function damerauLevenshtein(a, b) {
  // Counts a transposition as ONE edit. Essential here: swapping two
  // adjacent letters (gmial/gmail) is the commonest typing mistake and
  // plain Levenshtein scores it 2, which would miss it.
  const m = a.length, n = b.length;
  if (Math.abs(m - n) > 3) return 99;
  const d = Array.from({ length: m + 1 }, () => new Array(n + 1).fill(0));
  for (let i = 0; i <= m; i++) d[i][0] = i;
  for (let j = 0; j <= n; j++) d[0][j] = j;
  for (let i = 1; i <= m; i++) {
    for (let j = 1; j <= n; j++) {
      const cost = a[i - 1] === b[j - 1] ? 0 : 1;
      d[i][j] = Math.min(d[i - 1][j] + 1, d[i][j - 1] + 1, d[i - 1][j - 1] + cost);
      if (i > 1 && j > 1 && a[i - 1] === b[j - 2] && a[i - 2] === b[j - 1]) {
        d[i][j] = Math.min(d[i][j], d[i - 2][j - 2] + cost);
      }
    }
  }
  return d[m][n];
}

// Derived from FREE_EMAIL_DOMAINS, never hand-maintained alongside it.
// An exact match short-circuits to "no suggestion", so every provider the
// system already recognises is automatically immune from being "corrected".
// This matters: ymail.com is a real Yahoo domain, sits one edit from
// gmail.com, and a hand-written list missed it — those leads would have
// been told "Did you mean b@gmail.com?" about a perfectly valid address.
const TYPO_CANDIDATE_DOMAINS = Array.from(new Set([
  ...FREE_EMAIL_DOMAINS,
  'yahoo.co.in', 'yahoo.co.uk', 'proton.me', 'zoho.com', 'zoho.in',
  'yandex.com', 'gmx.com', 'comcast.net', 'verizon.net', 'sbcglobal.net',
  'outlook.in', 'hotmail.co.uk', 'qq.com', 'naver.com', 'web.de',
]));
const TYPO_BAD_TLDS = { '.con': '.com', '.cmo': '.com', '.ocm': '.com', '.vom': '.com', '.xom': '.com', '.comm': '.com', '.cm': '.com', '.om': '.com', '.couk': '.co.uk' };

function suggestDomainFix(domain) {
  const d = String(domain || '').toLowerCase();
  if (!d || TYPO_CANDIDATE_DOMAINS.includes(d)) return null;
  for (const [bad, good] of Object.entries(TYPO_BAD_TLDS)) {
    if (d.endsWith(bad)) return d.slice(0, -bad.length) + good;
  }
  let best = null, bestDist = 99;
  for (const c of TYPO_CANDIDATE_DOMAINS) {
    const dist = damerauLevenshtein(d, c);
    if (dist < bestDist) { bestDist = dist; best = c; }
  }
  // 1 edit always. 2 edits only on longer names, where two slips are
  // proportionally small and collisions with real domains are rarer.
  if (bestDist === 1 || (bestDist === 2 && best.length >= 10)) return best;
  return null;
}

/* ── User-facing copy ──────────────────────────────────────────────
   Lives server-side so the wording is consistent and can be changed
   without a Webflow republish. gushwork-form.js should render
   `message`, and offer `suggestion` as a one-tap fix when present.
   ───────────────────────────────────────────────────────────────── */
const ELV_MESSAGES = {
  invalid_syntax: "That email address doesn't look quite right — check for a typo.",
  syntax_error:   "That email address doesn't look quite right — check for a typo.",
  domain_error:   "We couldn't find that email domain. Check the spelling after the @.",
  invalid_mx:     "That domain isn't set up to receive email. Check the spelling after the @.",
  dead_server:    "That domain isn't accepting email right now. Try another address?",
  unknown_email:  "We couldn't find that mailbox. Check the spelling?",
  email_disabled: "That mailbox looks inactive. Try another address?",
  disposable:     "Please use your permanent work email.",
  spamtrap:       "Please use a different email address.",
  relay_error:    "That mailbox couldn't be reached. Try another address?",
  attempt_rejected: "That mailbox couldn't be reached. Try another address?",
  error:          "That email address doesn't appear to be valid. Mind double-checking it?",
  invalid:        "That email address doesn't appear to be valid. Mind double-checking it?",
};

function elvRejection(email, status) {
  const raw       = String(email);
  const localPart = raw.split('@')[0] || '';
  const domain    = raw.split('@')[1] || '';
  const base      = ELV_MESSAGES[status] || "That email address doesn't appear to be valid.";
  const fixed     = suggestDomainFix(domain);

  // Only offer a fix that actually FIXES it. We correct the domain and
  // never the local part, so a compound error — say utsav,singh@gmai.com,
  // where BOTH halves are wrong — would otherwise produce
  // "Did you mean utsav,singh@gmail.com?", still invalid, and tapping it
  // would loop on the same broken suggestion forever.
  const candidate = fixed ? `${localPart}@${fixed}` : null;
  const usable    = candidate && EMAIL_SYNTAX_RE.test(candidate);

  return {
    valid: false,
    status,
    message: usable ? `Did you mean ${candidate}?` : base,
    suggestion: usable ? candidate : null,
  };
}

const ELV_HTTP_MEANING = { 401: 'API key rejected', 402: 'Credits exhausted', 403: 'Access forbidden', 429: 'Rate limited' };

app.post('/verify-email', async (req, res) => {
  const email = (req.body.email || '').toString().trim().slice(0, 254).toLowerCase();
  if (!email) return res.status(400).json({ valid: false, error: 'email required' });

  // 1 — Syntax. Instant, free, no credit. Catches utsav,singh@gushwork.ai.
  if (!EMAIL_SYNTAX_RE.test(email)) {
    console.log(`[ELV] BLOCKED ${email} — local: invalid_syntax`);
    return res.json(Object.assign(elvRejection(email, 'invalid_syntax'), { source: 'local' }));
  }

  // 2 — Cache.
  const cached = elvCacheGet(email);
  if (cached) {
    console.log(`[ELV] ${email} → "${cached.status}" (cached)`);
    return res.json(cached.valid
      ? { valid: true, status: cached.status, source: 'cache' }
      : Object.assign(elvRejection(email, cached.status), { source: 'cache' }));
  }

  const domain = email.split('@')[1];

  const apiKey = process.env.ELV_API_KEY;
  if (!apiKey) {
    console.warn('[ELV] ELV_API_KEY not set — skipping, allowing through');
    alertOps('critical', 'ELV', 'API key not configured', { 'Impact': 'All emails passing unverified' });
    recordElvOutcome('skipped', email);
    return res.json({ valid: true, status: 'skipped' });
  }

  const startedAt = Date.now();
  try {
    // 3 — DNS and ELV run CONCURRENTLY, not in sequence.
    //     Sequentially this was worst-case 2.5s (DNS) + 8s (ELV) = 10.5s.
    //     Raced, the worst case is 8s and the typical case is whichever
    //     answers first. DNS usually wins by a wide margin, so a dead
    //     domain is rejected in ~50ms instead of after the full ELV wait.
    const controller = new AbortController();
    const timeout    = setTimeout(() => controller.abort(), ELV_TIMEOUT_MS);
    const url        = `https://apps.emaillistverify.com/api/verifyEmail?secret=${apiKey}&email=${encodeURIComponent(email)}`;
    const elvPromise = fetch(url, { signal: controller.signal });
    const mxPromise  = localMxCheck(domain);

    // A definitive NXDOMAIN is enough on its own — the domain does not
    // exist, so nothing ELV says can change the answer. Bail immediately
    // and let the in-flight ELV call be discarded.
    //     'no_mx' deliberately does NOT block: RFC 5321 permits A-record
    //     fallback for mail, so that case waits for ELV.
    const mx = await mxPromise;
    if (mx === 'nxdomain') {
      clearTimeout(timeout);
      controller.abort();
      elvPromise.catch(() => {}); // discard the aborted request quietly
      console.log(`[ELV] BLOCKED ${email} — local: nxdomain | ${Date.now() - startedAt}ms`);
      elvCacheSet(email, false, 'domain_error');
      return res.json(Object.assign(elvRejection(email, 'domain_error'), { source: 'local' }));
    }

    const response = await elvPromise;
    clearTimeout(timeout);

    // Non-200 means the CHECK failed, not that the email is bad. Fail open,
    // but loudly — this is the silent-failure hole.
    if (!response.ok) {
      const meaning  = ELV_HTTP_MEANING[response.status] || `HTTP ${response.status}`;
      const severity = [401, 402, 403].includes(response.status) ? 'critical' : 'warning';
      console.warn(`[ELV] ⚠ HTTP ${response.status} (${meaning}) — failing open`);
      alertOps(severity, 'ELV', meaning, {
        'HTTP status': response.status,
        'Impact': 'Emails are passing unverified',
        'Action': severity === 'critical' ? 'Renew key / top up credits in the ELV dashboard' : 'Monitor — may self-resolve',
      });
      recordElvOutcome('http_error', email);
      return res.json({ valid: true, status: 'http_error' });
    }

    const rawText = (await response.text()).trim().toLowerCase();
    const status  = normaliseElvStatus(rawText);
    const ms      = Date.now() - startedAt;
    console.log(`[ELV] ${email} → "${status}"${status !== rawText ? ` (raw: "${rawText}")` : ''} | ${ms}ms`);

    const known = ELV_BLOCK.includes(status) || ELV_PASS.includes(status) || ELV_INDETERMINATE.includes(status);
    if (!known) {
      // Unknown genuinely means we don't know. Still fails open — but it
      // now counts toward the health signal instead of being invisible.
      console.warn(`[ELV] ⚠ Unrecognised status "${status.substring(0, 60)}" — treating as indeterminate (passes)`);
      if (!elvIsInternal(email)) {
        alertOps('warning', 'ELV', 'Unrecognised status', {
          'Status': status.substring(0, 100),
          'Domain': domain,
          'Impact': 'Passed through. Add it to ELV_BLOCK or ELV_PASS if it should be definitive.',
        });
      }
    }

    const valid = !ELV_BLOCK.includes(status);
    if (!valid) console.log(`[ELV] BLOCKED ${email} — status: "${status}"`);
    recordElvOutcome(known ? status : 'unknown', email);
    elvCacheSet(email, valid, status);
    res.json(valid ? { valid: true, status, ms } : Object.assign(elvRejection(email, status), { ms }));
  } catch (err) {
    const isTimeout = err.name === 'AbortError';
    if (isTimeout) console.warn(`[ELV] Timeout after ${Date.now() - startedAt}ms for ${email} — failing open`);
    else console.warn('[ELV] Error:', err.message, '— failing open');
    recordElvOutcome(isTimeout ? 'timeout' : 'network_error', email);
    res.json({ valid: true, status: 'error_fallback' });
  }
});

// Live ELV health for the dashboard's System Health tab.
app.get('/monitor/elv-health', (req, res) => {
  res.setHeader('Cache-Control', 'no-store');
  res.json(elvHealthSnapshot());
});

// ── /verify-website — server-level for-sale/parked-lander content check ──
// Free, no external API: fetches the entered URL and scans the rendered
// HTML for domain-marketplace phrasing (Atom, HugeDomains, Sedo, GoDaddy
// Auctions, etc all use near-identical wording). Complements form.js's
// client-side DNS/IP/NS checks, which can't see PAGE CONTENT and so miss
// marketplace landers fronted by shared CDN IPs (test.com/Atom was the
// real case that exposed this). Same fail-open philosophy as ELV: any
// timeout, block, redirect loop, or bot-challenge response passes through
// rather than risk rejecting a real company's site.
// PHRASES — v5.3.0. Every entry must be unambiguous when read as ordinary
// English on a real company's website, and is matched against VISIBLE TEXT
// only (scripts/styles/comments stripped), never raw HTML.
//
// REMOVED and why — each of these is normal copy on a real business site:
//   'make an offer'          Standard SEC/FINRA disclaimer wording ("does not
//                            intend to make an offer or solicitation..."), on
//                            essentially every RIA / broker-dealer / wealth
//                            manager site. Also real estate and e-commerce.
//                            This is what flagged seasoninvestments.com.
//   'this domain is available' Common on hosting / web-agency / registrar copy.
//   'purchase this domain'   Same.
//   'domain broker'          Legitimate on hosting and web-services sites.
const FOR_SALE_PHRASES = [
  'this domain is for sale',
  'this domain name is for sale',
  'domain is for sale',
  'domain name is for sale',
  'this domain may be for sale',
  'buy this domain',
  'inquire about this domain',
  'this web page is parked',
  'this domain is parked',
  'premium domain for sale',
  'parked domain name',
  'the domain you are looking for is for sale',
  'interested in this domain',
];

// Placeholder pages are NOT for-sale landers — somebody owns the domain and
// hasn't finished setting it up. Separated so the reason label tells the truth.
const PLACEHOLDER_PHRASES = [
  'this domain is not configured',
  'hostinger dns system',
  'website coming soon',
  'future home of something quite cool',
  'if you are the owner of this website',
  'default web site page',
  'apache2 ubuntu default page',
  'welcome to nginx',
];

// Domain marketplaces. A domain that REDIRECTS to one of these is for sale —
// this is host identity, not English text, so unlike phrase matching it cannot
// false-positive on a real company's copy. Highest-precision signal we have.
const MARKETPLACE_DOMAINS = [
  'dan.com', 'afternic.com', 'sedo.com', 'sedoparking.com', 'hugedomains.com',
  'atom.com', 'squadhelp.com', 'brandbucket.com', 'buydomains.com',
  'undeveloped.com', 'efty.com', 'brandpa.com', 'saw.com', 'sav.com',
  'domainmarket.com', 'domainagents.com', 'namerific.com', 'flippa.com',
  'parklogic.com', 'bodis.com', 'above.com', 'abovedomains.com',
  'uniregistrymarket.link', 'epik.com', 'dynadot.com', 'namesilo.com',
];

// Public-suffix handling without pulling in the full PSL. Covers the
// multi-part suffixes that actually show up in inbound leads.
const MULTI_PART_SUFFIXES = new Set([
  'co.uk','org.uk','me.uk','ac.uk','gov.uk','net.uk','plc.uk','ltd.uk',
  'co.in','net.in','org.in','firm.in','gen.in','ind.in','ac.in','edu.in','gov.in',
  'com.au','net.au','org.au','edu.au','gov.au','co.nz','net.nz','org.nz','govt.nz',
  'com.br','net.br','org.br','com.mx','com.ar','com.co','com.pe','com.ec',
  'com.uy','com.py','com.bo','com.do','com.gt','com.pa','com.ve','com.sv',
  'co.za','org.za','com.ng','co.ke','com.gh','com.eg','com.sa','com.ae',
  'com.sg','com.my','com.ph','co.th','com.vn','com.pk','com.bd','com.hk',
  'com.tw','com.cn','net.cn','org.cn','co.jp','or.jp','ne.jp','ac.jp',
  'co.kr','com.tr','com.ua','com.pl','com.ro','com.gr','com.cy','com.mt',
  'co.id','co.il','org.il','com.es','com.pt','com.it','com.de','com.fr','com.ru',
]);

function registrableDomain(hostname) {
  const h = String(hostname || '').toLowerCase().replace(/\.$/, '');
  if (!h || /^\d{1,3}(\.\d{1,3}){3}$/.test(h)) return h;
  const parts = h.split('.').filter(Boolean);
  if (parts.length <= 2) return parts.join('.');
  const lastTwo = parts.slice(-2).join('.');
  if (MULTI_PART_SUFFIXES.has(lastTwo)) return parts.slice(-3).join('.');
  return lastTwo;
}

function isMarketplaceHost(hostname) {
  const reg = registrableDomain(hostname);
  return MARKETPLACE_DOMAINS.includes(reg) ? reg : null;
}

/* ── Structural substance analysis ─────────────────────────────────
   Judges "is there a real website here" by SHAPE rather than wording:
   a parked page has near-zero visible text, no internal navigation, and
   a title that is just the bare domain. A real site has nav and prose.
   Deliberately lexicon-free, so it works on parking pages in any
   language and on providers we've never seen.
   ───────────────────────────────────────────────────────────────── */
function analyzeSubstance(html, finalHost) {
  const visible = String(html || '')
    .replace(/<!--[\s\S]*?-->/g, ' ')
    .replace(/<script\b[^>]*>[\s\S]*?<\/script>/gi, ' ')
    .replace(/<style\b[^>]*>[\s\S]*?<\/style>/gi, ' ')
    .replace(/<noscript\b[^>]*>[\s\S]*?<\/noscript>/gi, ' ')
    .replace(/<svg\b[^>]*>[\s\S]*?<\/svg>/gi, ' ')
    .replace(/<[^>]+>/g, ' ')
    .replace(/&(?:[a-z]+|#\d+|#x[0-9a-f]+);/gi, ' ')
    .replace(/\s+/g, ' ')
    .trim();

  const titleMatch = String(html || '').match(/<title[^>]*>([\s\S]{0,300}?)<\/title>/i);
  const title = titleMatch ? titleMatch[1].replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ').trim() : '';

  const reg = registrableDomain(finalHost);
  let internalLinks = 0;
  let externalLinks = 0;
  const hrefRe = /<a\b[^>]*?href\s*=\s*(?:"([^"]*)"|'([^']*)')/gi;
  let m;
  while ((m = hrefRe.exec(html)) !== null) {
    const href = (m[1] || m[2] || '').trim();
    if (!href || /^(?:#|mailto:|tel:|javascript:|data:)/i.test(href)) continue;
    if (/^https?:\/\//i.test(href)) {
      try {
        if (registrableDomain(new URL(href).hostname) === reg) internalLinks++;
        else externalLinks++;
      } catch { /* malformed href — ignore */ }
    } else {
      internalLinks++; // relative link = same site
    }
  }

  // A title that is just the domain (with or without TLD) is a parking tell.
  const bareTitle = title.toLowerCase().replace(/[^a-z0-9]/g, '');
  const bareHost = reg.replace(/[^a-z0-9]/g, '');
  const titleIsJustDomain = !!title && (bareTitle === bareHost || bareTitle === bareHost.replace(/(com|net|org|us|co|io|ai|in)$/, ''));

  const textLen = visible.length;
  // Substantial: enough prose OR real navigation. Either alone is sufficient —
  // an image-heavy one-pager has few words but many links, and a long text
  // page may have little nav.
  const substantial = textLen >= 400 || internalLinks >= 5;
  // Thin: almost nothing rendered server-side AND no navigation at all.
  const thin = textLen < 140 && internalLinks <= 1;

  return { textLen, internalLinks, externalLinks, title, titleIsJustDomain, substantial, thin, visible };
}

function findPhrase(list, visibleLower) {
  return list.find((p) => visibleLower.includes(p)) || null;
}

// Blocks requests aimed at internal/private infrastructure so this route
// can't be used as an SSRF pivot into Railway's own network.
function isPrivateOrLocalHost(hostname) {
  const h = hostname.toLowerCase();
  if (h === 'localhost' || h.endsWith('.local') || h.endsWith('.internal')) return true;
  const ipMatch = h.match(/^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})$/);
  if (ipMatch) {
    const [a, b] = ipMatch.slice(1).map(Number);
    if (a === 10 || a === 127 || a === 0) return true;
    if (a === 169 && b === 254) return true; // link-local / cloud metadata
    if (a === 172 && b >= 16 && b <= 31) return true;
    if (a === 192 && b === 168) return true;
  }
  return false;
}

// One attempt at a candidate URL. Returns the Response (response.url is the
// final, redirect-resolved URL — free from fetch's own redirect following).
// Throws on hard failure or on our own timeout (distinguished by err.name).
async function attemptFetch(urlString, timeoutMs) {
  const controller = new AbortController();
  const t = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(urlString, {
      signal: controller.signal,
      redirect: 'follow',
      // A real browser UA, deliberately. Domain monetisation networks serve
      // BOTS a "this domain is for sale" page while forwarding real visitors to
      // the actual site — that's how afgmmoving.com (a working forward) got
      // mislabelled. The whole point of this check is "does this URL work for a
      // person", so we must see what a person sees.
      headers: { 'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36' },
    });
    clearTimeout(t);
    return response;
  } catch (err) {
    clearTimeout(t);
    throw err;
  }
}

function flipWww(hostname) {
  return hostname.toLowerCase().startsWith('www.') ? hostname.slice(4) : 'www.' + hostname;
}

/* ── evaluateWebsite ──────────────────────────────────────────────
   The whole tiered decision. Extracted from the /verify-website route in
   v5.3.1 so the route AND the historical recheck run the SAME code path.
   Two copies of this logic is exactly the drift that produced the
   duplicated-verified-reasons bug, so there is deliberately only one.

   Returns the verdict object the route used to res.json() directly.
   ───────────────────────────────────────────────────────────────── */
async function evaluateWebsite({ raw, parkingHint = null, hasMX = false }) {
  const startedAt = Date.now(); // budgets the optional wildcard probe below
  if (!raw) return ({ ok: true, reason: 'empty' }); // fail open, form's own required-check owns this
  let url;
  try {
    url = new URL(raw.startsWith('http') ? raw : 'https://' + raw);
  } catch {
    return ({ ok: true, reason: 'unparseable' }); // format errors are the browser's job
  }
  if (!['http:', 'https:'].includes(url.protocol) || isPrivateOrLocalHost(url.hostname)) {
    return ({ ok: true, reason: 'skipped_unsafe_target' }); // never fetch internal/local targets; fail open
  }

  // Fallback ladder: as-typed -> www/bare flip (same protocol) -> http downgrade.
  // A hard connection failure (DNS/refused/TLS) is fast, so trying the next
  // candidate is cheap. OUR OWN timeout means the site is just slow, not
  // wrong — we stop immediately rather than compounding wait time on what's
  // likely the same latency again.
  const candidates = [
    { u: url.toString(), timeout: 9000 },
    { u: `${url.protocol}//${flipWww(url.hostname)}${url.pathname}${url.search}`, timeout: 5000 },
    { u: `http://${url.hostname}${url.pathname}${url.search}`, timeout: 5000 },
  ];

  let response = null;
  for (const c of candidates) {
    try {
      response = await attemptFetch(c.u, c.timeout);
      break; // got a real HTTP response (any status) — stop the ladder
    } catch (err) {
      if (err.name === 'AbortError') {
        console.warn(`[verify-website] Timeout for ${c.u} — failing open`);
        return ({ ok: true, reason: 'timeout' });
      }
      console.warn(`[verify-website] Connection failed for ${c.u} (${err.message}) — trying next candidate`);
    }
  }

  if (!response) {
    console.warn(`[verify-website] All candidates unreachable for ${url.hostname} — failing open`);
    return ({ ok: true, reason: 'unreachable' });
  }

  // fetch() resolves response.url to the FINAL address after following
  // every redirect — this is the one reliable URL, captured for free.
  const canonical_url = response.url || undefined;

  // ── Where did we actually land? ──────────────────────────────────
  // This is the highest-value signal in the whole check, and until v5.3.0
  // it was computed and thrown away. DNS cannot distinguish a domain that
  // is PARKED from one that FORWARDS to the owner's real site: Above.com,
  // GoDaddy, Afternic and Namecheap all serve both from the same IPs and
  // the same nameservers. Only the redirect destination separates them.
  let finalHost = url.hostname;
  try { if (canonical_url) finalHost = new URL(canonical_url).hostname; } catch { /* keep typed host */ }
  const typedReg = registrableDomain(url.hostname);
  const finalReg = registrableDomain(finalHost);
  const redirectedOffDomain = !!finalReg && !!typedReg && finalReg !== typedReg;

  // TIER 1 — host identity. A domain that redirects to a domain marketplace
  // is for sale, full stop. No text matching, so no false positives on a
  // real company's copy.
  const marketplace = isMarketplaceHost(finalHost);
  if (marketplace && redirectedOffDomain) {
    console.log(`[verify-website] NEGATIVE ${url.hostname} → redirects to marketplace ${marketplace}`);
    return ({ ok: false, reason: 'marketplace_redirect', matched: marketplace, canonical_url });
  }

  if (!response.ok) {
    console.log(`[verify-website] ${url.hostname} → HTTP ${response.status} — failing open`);
    return ({ ok: true, reason: 'http_' + response.status, canonical_url, redirected_off_domain: redirectedOffDomain });
  }
  const contentType = response.headers.get('content-type') || '';
  if (!contentType.includes('html')) return ({ ok: true, reason: 'non_html', canonical_url }); // fail open — not a page we can scan

  const rawHtml = (await response.text()).slice(0, 200000); // cap read size
  const sub = analyzeSubstance(rawHtml, finalHost);
  const visibleLower = sub.visible.toLowerCase();

  // TIER 1 — unambiguous for-sale wording, VISIBLE TEXT ONLY. Scanning raw
  // HTML (pre-v5.3.0) meant a phrase inside a <script> blob, a meta tag or
  // an analytics payload counted as a for-sale page.
  const saleHit = findPhrase(FOR_SALE_PHRASES, visibleLower);
  if (saleHit) {
    console.log(`[verify-website] NEGATIVE ${url.hostname} — for-sale phrase in visible text: "${saleHit}"`);
    return ({ ok: false, reason: 'for_sale_lander', matched: saleHit, canonical_url });
  }

  // TIER 2 — a forward onto a DIFFERENT domain that has real content is a
  // legitimate forward. This is afgmmoving.com -> afewgoodmenmoving.com, and
  // it outranks any DNS parking hint: nobody parks a domain by pointing it
  // at a working business website.
  if (redirectedOffDomain && sub.substantial) {
    console.log(`[verify-website] PASS ${url.hostname} → forwards to live site ${finalReg} (text=${sub.textLen}, links=${sub.internalLinks})`);
    return ({ ok: true, reason: 'forwarded_to_live_site', canonical_url, forwarded_to: finalReg, substance: { textLen: sub.textLen, internalLinks: sub.internalLinks } });
  }

  const placeholderHit = findPhrase(PLACEHOLDER_PHRASES, visibleLower);

  // TIER 2 — real content wins over a DNS hint, ALWAYS. This is the guard
  // that makes the hint list safe: a mislabelled IP (216.198.79.1 was Vercel,
  // flagged as a Hostinger placeholder, and quietly marked four live
  // businesses as parked) can no longer cost a lead its Meta event.
  if (sub.substantial && !placeholderHit) {
    const reason = parkingHint ? 'live_despite_dns_hint' : 'content_clean';
    if (parkingHint) console.log(`[verify-website] PASS ${url.hostname} — DNS hint "${parkingHint}" overridden by real content (text=${sub.textLen}, links=${sub.internalLinks})`);
    return ({ ok: true, reason, canonical_url, substance: { textLen: sub.textLen, internalLinks: sub.internalLinks } });
  }

  // TIER 3 — thin page. Corroborated by parking infrastructure, this is a
  // confident parked domain (theroutermill.us: GoDaddy parking IPs, page
  // painted entirely in JS so no phrase is ever present in the HTML).
  if (sub.thin || placeholderHit) {
    if (parkingHint && !hasMX) {
      console.log(`[verify-website] NEGATIVE ${url.hostname} — thin page + parking infra "${parkingHint}", no MX`);
      return ({ ok: false, reason: 'parked_confirmed', matched: placeholderHit || parkingHint, canonical_url });
    }
    if (parkingHint) {
      console.log(`[verify-website] NEGATIVE ${url.hostname} — thin page + parking infra "${parkingHint}" (has MX)`);
      return ({ ok: false, reason: 'parked_confirmed', matched: placeholderHit || parkingHint, canonical_url });
    }

    // No corroboration. A client-rendered SPA is indistinguishable from a
    // parked page over plain HTTP, so this must NEVER cost a real lead its
    // Meta event. One cheap extra probe raises confidence for the human
    // reading Slack: parking serves identical content for EVERY path
    // because there is no site behind it. Informational only.
    let wildcard = false;
    if (Date.now() - startedAt < 6000) {
      try {
        const probeUrl = `${new URL(canonical_url || url.toString()).origin}/gw-probe-${Math.random().toString(36).slice(2, 10)}`;
        const probe = await attemptFetch(probeUrl, 3500);
        if (probe.status === 200) {
          const probeHtml = (await probe.text()).slice(0, 200000);
          const probeSub = analyzeSubstance(probeHtml, finalHost);
          wildcard = Math.abs(probeSub.textLen - sub.textLen) < 40 && probeSub.title === sub.title;
        }
      } catch { /* probe is a bonus signal — never let it change the outcome */ }
    }
    const reason = wildcard ? 'thin_content_wildcard' : 'thin_content';
    console.log(`[verify-website] FLAG ${url.hostname} — ${reason} (text=${sub.textLen}, links=${sub.internalLinks}, titleIsDomain=${sub.titleIsJustDomain})`);
    // ok:true and a VERIFIED reason: surfaced for humans, costs the lead nothing.
    return ({ ok: true, reason, canonical_url, substance: { textLen: sub.textLen, internalLinks: sub.internalLinks, titleIsJustDomain: sub.titleIsJustDomain, wildcard } });
  }

  // Neither substantial nor thin — a small but real page. Pass clean.
  return { ok: true, reason: parkingHint ? 'live_despite_dns_hint' : 'content_clean', canonical_url, substance: { textLen: sub.textLen, internalLinks: sub.internalLinks } };
}

/* ⚠ KEEP THE THREE LISTS BELOW IN SYNC WITH gushwork-form.js (SECTION 3C).
   Duplicated only because form.js resolves over DNS-over-HTTPS in the browser
   while the historical recheck resolves via node. A hint is never a verdict —
   evaluateWebsite() arbitrates — so drift here degrades a label, it cannot
   wall out a lead. */
const PARKING_IP_HINTS = [
  '162.255.119.', // Namecheap parking / URL-forwarding (shared with real hosting)
  '34.102.136.180', // GoDaddy parking (exact)
  '3.33.130.190', // GoDaddy parking / forwarding anycast (exact)
  '15.197.148.33', // GoDaddy parking / forwarding anycast (exact)
  '91.195.240.', '91.195.241.', // Sedo
  '185.53.177.', '185.53.178.', '185.53.179.', // ParkingCrew
  '199.59.242.', '199.59.243.', // Bodis
  '208.91.197.', // Confluence Networks parking
  '103.224.182.', // Trellian / Above.com parking AND forwarding
];
const PARKING_NS_STRICT = ['sedoparking.com', 'parkingcrew.net', 'bodis.com', 'above.com', 'parklogic.com', 'uniregistrymarket.link', 'afternic.com', 'dan.com', 'abovedomains.com'];
const PARKING_NS_SOFT = ['namebrightdns.com', 'safesecureweb.com'];

/* ── resolveWebsiteDns ────────────────────────────────────────────
   Server-side twin of STAGE 1, which normally runs in the browser over
   DNS-over-HTTPS in gushwork-form.js.

   v5.3.2 — added because the first recheck dry run exposed that the route
   was running STAGE 2 ONLY. With no existence check it simply tried to
   fetch a non-existent domain, failed open, and wrote 'unreachable' over
   six perfectly good 'nxdomain' verdicts — a strict downgrade, since
   nxdomain is accurate AND blocking while unreachable is neither.

   Node distinguishes the two cases we care about:
     ENOTFOUND → NXDOMAIN, the name does not exist
     ENODATA   → NOERROR with no records of that type (domain DOES exist)
   ───────────────────────────────────────────────────────────────── */
async function resolveWebsiteDns(domain) {
  const out = { status: 'resolved', parkingHint: null, hasMX: false, ips: [], ns: [] };
  if (!domain) return out;

  let apexCode = null, wwwCode = null;
  try { out.ips = await dnsPromises.resolve4(domain); }
  catch (e) {
    apexCode = e.code;
    try { out.ips = await dnsPromises.resolve4('www.' + domain); }
    catch (e2) { wwwCode = e2.code; }
  }

  try { out.hasMX = (await dnsPromises.resolveMx(domain)).some((r) => r.exchange && r.exchange !== '.'); }
  catch { /* no MX */ }

  if (!out.ips.length) {
    try { if ((await dnsPromises.resolve6(domain)).length) return out; } catch { /* no AAAA */ }
    if (out.hasMX) { out.status = 'mx_only'; return out; } // email-only company — legitimate
    // Both lookups must agree the name is absent before we call it NXDOMAIN.
    out.status = (apexCode === 'ENOTFOUND' && wwwCode === 'ENOTFOUND') ? 'nxdomain' : 'no_dns_records';
    return out;
  }

  const hintIp = out.ips.find((ip) => PARKING_IP_HINTS.some((p) => (p.split('.').length === 4 && p.slice(-1) !== '.' ? ip === p : ip.indexOf(p) === 0)));
  if (hintIp) { out.parkingHint = 'ip:' + hintIp; return out; }

  try { out.ns = (await dnsPromises.resolveNs(domain)).map((h) => String(h).toLowerCase().replace(/\.$/, '')); }
  catch { /* no NS */ }
  const nsMatch = (list) => out.ns.reduce((acc, h) => acc || list.find((s) => h === s || h.endsWith('.' + s)) || null, null);
  const strict = nsMatch(PARKING_NS_STRICT);
  if (strict) { out.parkingHint = 'ns:' + strict; return out; }
  const soft = nsMatch(PARKING_NS_SOFT);
  if (soft && !out.hasMX) out.parkingHint = 'ns_soft:' + soft;
  return out;
}

/* Verdicts the recheck is allowed to WRITE. Anything absent from this list
   means "we did not get a real answer" — a timeout, a connection failure, a
   bot wall, an HTTP error. Those are states of the network at one moment,
   not facts about the domain, and persisting one is strictly worse than
   keeping whatever is already recorded. */
const RECHECK_WRITEABLE = [
  'content_clean', 'live_despite_dns_hint', 'forwarded_to_live_site',
  'thin_content', 'thin_content_wildcard',
  'parked_confirmed', 'for_sale_lander', 'marketplace_redirect',
  'nxdomain', 'no_dns_records', 'mx_only',
];

/* Verdicts the recheck must NEVER overwrite. These come from
   localWebsiteVerdict() in gushwork-form.js and depend on the lead's EMAIL,
   which this route deliberately does not re-derive. Left alone, they stay
   correct; recomputed from content alone, a LinkedIn profile URL comes back
   as 'content_clean' because linkedin.com serves a real page. */
const RECHECK_PROTECTED = ['brand_mismatch', 'mailbox_domain', 'social_profile_url', 'test_email_skipped', 'unparseable'];

/* Thin HTTP wrapper. Behaviour is identical to pre-v5.3.1 — the regression
   suite asserts this. */
app.post('/verify-website', async (req, res) => {
  const raw = (req.body.website || '').toString().trim().slice(0, 300);
  const parkingHint = (req.body.parking_hint || '').toString().slice(0, 40) || null;
  const hasMX = req.body.has_mx === true || req.body.has_mx === 'true';
  if (!raw) return res.status(400).json({ ok: true, reason: 'empty' }); // fail open, form's own required-check owns this
  try {
    const verdict = await evaluateWebsite({ raw, parkingHint, hasMX });
    res.json(verdict);
  } catch (err) {
    console.error('[verify-website] Unexpected error — failing open:', err.message);
    res.json({ ok: true, reason: 'backend_error' }); // never let a checker fault cost a lead
  }
});

/* =======================================================
   HISTORICAL WEBSITE RECHECK  (v5.3.1)

   Re-runs the CURRENT check against domains already in the DB and reports
   what changed. Exists because four separate bugs shipped without anything
   ever re-examining a verdict after the fact:
     - a Vercel IP mislabelled as a parking IP, marking live businesses parked
     - 'make an offer' matching the standard SEC/FINRA disclaimer
     - phrase scanning raw HTML, so <script> contents counted
     - JS-rendered parked pages passing clean (no phrase in the HTML to find)

   Corrects BOTH directions: false positives sitting in "Not verified", and
   genuinely-parked domains sitting in "Passed" as content_clean.

     GET /monitor/website-recheck?token=…               dry run (DEFAULT)
     GET /monitor/website-recheck?token=…&apply=1       writes
     &scope=unverified|clean|all   default all
     &limit=200   &offset=0   &format=json

   Dry run is the default and the ONLY thing that writes is apply=1.
   Never touches social_profile_url rows or rows with no reason recorded.
   Sequential with a delay — this must not look like a burst of scraping.
   ======================================================= */
app.get('/monitor/website-recheck', async (req, res) => {
  const token = process.env.MONITOR_TOKEN;
  if (token && req.query.token !== token) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  if (!token) {
    // A route that can rewrite lead data must never be open.
    return res.status(403).json({ error: 'MONITOR_TOKEN must be set before using this endpoint' });
  }

  const apply = req.query.apply === '1';
  const scope = ['unverified', 'clean', 'all'].includes(req.query.scope) ? req.query.scope : 'all';
  const limit = Math.min(parseInt(req.query.limit, 10) || 200, 500);
  const offset = Math.max(parseInt(req.query.offset, 10) || 0, 0);
  const asJson = req.query.format === 'json';

  const verifiedSql = WEBSITE_VERIFIED_REASONS.filter((r) => /^[a-z0-9_]+$/.test(r)).map((r) => `'${r}'`).join(',');
  // 'unverified' mirrors the monitor filter exactly; 'clean' catches the
  // opposite error — a parked domain that the old phrase scan waved through.
  const scopeSql = {
    unverified: `(website_check_failed IS TRUE OR website_check_reason NOT IN (${verifiedSql}))`,
    clean: `website_check_reason IN ('content_clean','resolved')`,
    all: `(website_check_failed IS TRUE OR website_check_reason NOT IN (${verifiedSql}) OR website_check_reason IN ('content_clean','resolved'))`,
  }[scope];

  try {
    if (apply) {
      // Preserve the original verdict. Self-creating so there is no separate
      // migration step to remember.
      await pool.query(`ALTER TABLE leads ADD COLUMN IF NOT EXISTS website_check_reason_prev TEXT`);
      await pool.query(`ALTER TABLE leads ADD COLUMN IF NOT EXISTS website_rechecked_at TIMESTAMPTZ`);
    }

    const { rows } = await pool.query(
      `SELECT session_id, email, company, website, website_check_failed, website_check_reason, booking_uid
         FROM leads
        WHERE website IS NOT NULL AND website <> ''
          AND website_check_reason IS NOT NULL AND website_check_reason <> ''
          AND website_check_reason <> 'social_profile_url'
          AND ${scopeSql}
        ORDER BY created_at DESC
        LIMIT $1 OFFSET $2`,
      [limit, offset]
    );

    // One evaluation per DISTINCT domain — several leads often share one.
    const byDomain = new Map();
    for (const r of rows) {
      const key = (r.website || '').trim().toLowerCase();
      if (!byDomain.has(key)) byDomain.set(key, []);
      byDomain.get(key).push(r);
    }

    const results = [];
    for (const [website, leads] of byDomain) {
      let verdict, hint = null, skip = null;
      try {
        const host = new URL(website.startsWith('http') ? website : 'https://' + website).hostname.replace(/^www\./, '');
        // STAGE 1 — existence and parking hint. Must run FIRST: without it a
        // non-existent domain merely fails to fetch and looks 'unreachable',
        // which silently downgrades an accurate, blocking 'nxdomain'.
        const dns = await resolveWebsiteDns(host);
        hint = dns.parkingHint;
        if (dns.status === 'nxdomain') verdict = { ok: false, reason: 'nxdomain', matched: 'ENOTFOUND on apex + www' };
        else if (dns.status === 'no_dns_records') verdict = { ok: true, reason: 'no_dns_records' };
        else if (dns.status === 'mx_only') verdict = { ok: true, reason: 'mx_only', matched: 'MX only — email-only company' };
        // STAGE 2 — only meaningful once we know the domain resolves.
        else verdict = await evaluateWebsite({ raw: website, parkingHint: dns.parkingHint, hasMX: dns.hasMX });
      } catch (err) {
        verdict = { ok: true, reason: 'recheck_error', error: err.message };
      }
      if (!RECHECK_WRITEABLE.includes(verdict.reason)) skip = 'no decisive answer (' + verdict.reason + ') — keeping existing verdict';

      const nowVerified = WEBSITE_VERIFIED_REASONS.includes(verdict.reason);
      for (const l of leads) {
        const rowSkip = RECHECK_PROTECTED.includes(l.website_check_reason)
          ? 'email-dependent verdict — not re-derivable here'
          : skip;
        const wasVerified = l.website_check_failed !== true && WEBSITE_VERIFIED_REASONS.includes(l.website_check_reason);
        const changed = !rowSkip && (l.website_check_reason !== verdict.reason || (l.website_check_failed === true) !== !verdict.ok);
        results.push({
          session_id: l.session_id, email: l.email, company: l.company, website,
          was: l.website_check_reason, was_failed: l.website_check_failed === true,
          now: rowSkip ? l.website_check_reason : verdict.reason,
          now_failed: rowSkip ? l.website_check_failed === true : !verdict.ok,
          probed: verdict.reason, skip: rowSkip, changed,
          direction: rowSkip ? 'SKIPPED — left as-is' : (wasVerified === nowVerified ? 'same' : (nowVerified ? 'FALSE POSITIVE — was suppressed, is real' : 'MISSED — passed before, is not a real site')),
          hint, why: verdict.matched || (verdict.substance ? `text=${verdict.substance.textLen} links=${verdict.substance.internalLinks}` : '') || verdict.forwarded_to || '',
          booked: !!l.booking_uid,
        });
      }
      await new Promise((r) => setTimeout(r, 400)); // be a polite citizen
    }

    let written = 0;
    if (apply) {
      for (const r of results) {
        if (!r.changed || r.now === 'recheck_error') continue;
        await pool.query(
          `UPDATE leads
              SET website_check_reason_prev = COALESCE(website_check_reason_prev, website_check_reason),
                  website_check_reason      = $1,
                  website_check_failed      = $2,
                  website_rechecked_at      = NOW()
            WHERE session_id = $3`,
          [r.now, r.now_failed, r.session_id]
        );
        written++;
      }
      console.log(`[website-recheck] APPLIED ${written} correction(s) across ${byDomain.size} domain(s)`);
    } else {
      console.log(`[website-recheck] DRY RUN — ${results.filter((r) => r.changed).length} of ${results.length} lead(s) would change`);
    }

    const summary = {
      mode: apply ? 'APPLIED' : 'DRY RUN',
      scope, leads_examined: results.length, distinct_domains: byDomain.size,
      would_change: results.filter((r) => r.changed).length,
      skipped: results.filter((r) => r.skip).length,
      false_positives: results.filter((r) => r.direction.startsWith('FALSE')).length,
      missed: results.filter((r) => r.direction.startsWith('MISSED')).length,
      written,
    };

    if (asJson) return res.json({ summary, results });

    const esc = (s) => String(s == null ? '' : s).replace(/[&<>"]/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));
    const colour = (d) => (d.startsWith('FALSE') ? '#047857' : d.startsWith('MISSED') ? '#b91c1c' : d.startsWith('SKIPPED') ? '#a16207' : '#6b7280');
    res.set('Content-Type', 'text/html').send(`<!doctype html><meta charset="utf-8"><title>Website recheck — ${esc(summary.mode)}</title>
<style>body{font:14px -apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;margin:24px;color:#111}h1{font-size:19px;margin:0 0 4px}
.sum{background:#f9fafb;border:1px solid #e5e7eb;border-radius:8px;padding:12px 14px;margin:12px 0;display:flex;gap:22px;flex-wrap:wrap}
.sum b{display:block;font-size:20px}table{border-collapse:collapse;width:100%;font-size:13px}
th{text-align:left;background:#f3f4f6;padding:8px;border-bottom:2px solid #e5e7eb;position:sticky;top:0}
td{padding:8px;border-bottom:1px solid #f3f4f6;vertical-align:top}tr.chg{background:#fffbeb}
code{background:#f3f4f6;padding:1px 5px;border-radius:4px;font-size:12px}
.warn{background:#fef2f2;border:1px solid #fecaca;color:#991b1b;padding:10px 14px;border-radius:8px;margin:12px 0}
a.btn{display:inline-block;background:#111;color:#fff;padding:9px 15px;border-radius:7px;text-decoration:none;font-weight:600}</style>
<h1>Website recheck — ${esc(summary.mode)}</h1>
<div style="color:#6b7280">scope=<code>${esc(scope)}</code> · limit=${limit} · offset=${offset}</div>
<div class="sum">
<div>Leads examined<b>${summary.leads_examined}</b></div><div>Distinct domains<b>${summary.distinct_domains}</b></div>
<div>Would change<b>${summary.would_change}</b></div><div>Skipped (left as-is)<b>${summary.skipped}</b></div>
<div style="color:#047857">False positives<b>${summary.false_positives}</b></div>
<div style="color:#b91c1c">Missed<b>${summary.missed}</b></div>
${apply ? `<div>Rows written<b>${written}</b></div>` : ''}
</div>
${apply
  ? `<div class="sum" style="background:#ecfdf5;border-color:#a7f3d0">Applied. Previous values kept in <code>website_check_reason_prev</code>.</div>`
  : `<div class="warn"><b>Nothing has been written.</b> Review the rows below, then re-run with <code>&amp;apply=1</code> to save. Previous values are preserved in <code>website_check_reason_prev</code>.</div>
     <a class="btn" href="?token=${esc(req.query.token)}&scope=${esc(scope)}&limit=${limit}&offset=${offset}&apply=1">Apply ${summary.would_change} correction(s)</a>`}
<table><thead><tr><th>Email</th><th>Company</th><th>Website</th><th>Was</th><th>Now</th><th>Direction</th><th>Probe / Why</th><th>Booked</th></tr></thead><tbody>
${results.map((r) => `<tr class="${r.changed ? 'chg' : ''}"><td>${esc(r.email)}</td><td>${esc(r.company)}</td><td>${esc(r.website)}</td>
<td><code>${esc(r.was)}</code>${r.was_failed ? ' ⚠️' : ''}</td><td><code>${esc(r.now)}</code>${r.now_failed ? ' ⚠️' : ''}</td>
<td style="color:${colour(r.direction)}">${esc(r.direction)}</td><td style="color:#6b7280">${esc(r.hint ? r.hint + ' · ' : '')}${esc(r.why)}${r.skip ? `<br><i>probed <code>${esc(r.probed)}</code> — ${esc(r.skip)}</i>` : ''}</td>
<td>${r.booked ? 'Yes' : '—'}</td></tr>`).join('')}
</tbody></table>`);
  } catch (err) {
    console.error('[website-recheck] Failed:', err.message);
    res.status(500).json({ error: err.message });
  }
});



app.post('/session', async (req, res) => {
  const session_id = (req.body.session_id || '').toString().trim().slice(0, 100);
  if (!session_id) return res.status(400).json({ error: 'session_id required' });
  res.json({ ok: true });
});

app.post('/enrich', async (req, res) => {
  const email      = (req.body.email      || '').toString().trim().slice(0, 254).toLowerCase();
  const session_id = (req.body.session_id || '').toString().trim().slice(0, 100);
  if (!email || !session_id) return res.status(400).json({ error: 'email and session_id required' });
  const personalDomains = FREE_EMAIL_DOMAINS;
  const domain = email.split('@')[1]?.toLowerCase() || '';
  if (personalDomains.includes(domain)) { console.log(`[/enrich] Skipping Apollo for personal email: ${email}`); return res.json({ first_name:'',last_name:'',title:'',company:'',company_size:'',industry:'',linkedin_url:'',website:'' }); }
  try {
    const apolloRes  = await fetch('https://api.apollo.io/api/v1/people/match', { method:'POST', headers:{'Content-Type':'application/json','Cache-Control':'no-cache','X-Api-Key':process.env.APOLLO_API_KEY}, body:JSON.stringify({email,reveal_personal_emails:false,reveal_phone_number:false}) });
    const apolloData = await apolloRes.json();
    const person = apolloData.person || {}; const org = person.organization || {};
    const city=person.city||null, state=person.state||null, country=person.country||null;
    const orgHQ = [org.city,org.state,org.country].filter(Boolean).join(', ') || null;
    const seniority=person.seniority||null;
    const deptRaw=person.departments||person.person_departments||null;
    const departments = Array.isArray(deptRaw)&&deptRaw.length>0 ? deptRaw.join(', ') : null;
    const emailStatus=person.email_status||null, foundedYear=org.founded_year?.toString()||null;
    const annualRevenue = org.annual_revenue_printed ? `$${org.annual_revenue_printed} USD` : (org.annual_revenue ? formatRevenue(org.annual_revenue) : null);
    const totalFunding  = org.total_funding_printed ? `$${org.total_funding_printed}` : null;
    const fundingStage  = org.latest_funding_stage || null;
    const fundingEvents = Array.isArray(org.funding_events)&&org.funding_events.length>0 ? org.funding_events.map(f=>[f.date?f.date.substring(0,10):'',f.type||f.series||'',f.amount?`${f.currency||'$'}${f.amount}`:''].filter(Boolean).join(' ')).join(' | ') : null;
    const alexaRanking  = org.alexa_ranking?.toString() || null;
    const keywords      = Array.isArray(org.keywords) ? org.keywords.slice(0,8).join(', ') : (org.keywords||null);
    console.log(`[/enrich] Apollo — seniority: ${seniority} | dept: ${departments} | revenue: ${annualRevenue} | funding: ${totalFunding} (${fundingStage}) | location: ${city||country||'n/a'} | org HQ: ${orgHQ}`);
    await pool.query(`
      INSERT INTO enrichment_data (session_id,email,enriched_first_name,enriched_last_name,enriched_title,enriched_company,enriched_company_size,enriched_industry,enriched_linkedin,enriched_city,enriched_state,enriched_country,enriched_seniority,enriched_departments,enriched_email_status,enriched_founded_year,enriched_annual_revenue,enriched_funding_events,enriched_alexa_ranking,enriched_keywords,enriched_org_hq,enriched_total_funding,enriched_funding_stage,raw_response)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24)
      ON CONFLICT (session_id) DO UPDATE SET email=EXCLUDED.email,enriched_first_name=EXCLUDED.enriched_first_name,enriched_last_name=EXCLUDED.enriched_last_name,enriched_title=EXCLUDED.enriched_title,enriched_company=EXCLUDED.enriched_company,enriched_company_size=EXCLUDED.enriched_company_size,enriched_industry=EXCLUDED.enriched_industry,enriched_linkedin=EXCLUDED.enriched_linkedin,enriched_city=EXCLUDED.enriched_city,enriched_state=EXCLUDED.enriched_state,enriched_country=EXCLUDED.enriched_country,enriched_seniority=EXCLUDED.enriched_seniority,enriched_departments=EXCLUDED.enriched_departments,enriched_email_status=EXCLUDED.enriched_email_status,enriched_founded_year=EXCLUDED.enriched_founded_year,enriched_annual_revenue=EXCLUDED.enriched_annual_revenue,enriched_funding_events=EXCLUDED.enriched_funding_events,enriched_alexa_ranking=EXCLUDED.enriched_alexa_ranking,enriched_keywords=EXCLUDED.enriched_keywords,enriched_org_hq=EXCLUDED.enriched_org_hq,enriched_total_funding=EXCLUDED.enriched_total_funding,enriched_funding_stage=EXCLUDED.enriched_funding_stage,raw_response=EXCLUDED.raw_response,enriched_at=NOW()
    `, [session_id,email,person.first_name||null,person.last_name||null,person.title||null,org.name||null,org.estimated_num_employees?.toString()||null,org.industry||null,person.linkedin_url||null,city,state,country,seniority,departments,emailStatus,foundedYear,annualRevenue,fundingEvents,alexaRanking,keywords,orgHQ,totalFunding,fundingStage,apolloData]);
    await pool.query(`UPDATE leads SET enriched_city=$2,enriched_state=$3,enriched_country=$4,enriched_seniority=$5,enriched_departments=$6,enriched_email_status=$7,enriched_founded_year=$8,enriched_annual_revenue=$9,enriched_funding_events=$10,enriched_alexa_ranking=$11,enriched_keywords=$12,enriched_org_hq=$13,enriched_total_funding=$14,enriched_funding_stage=$15,updated_at=NOW() WHERE session_id=$1`, [session_id,city,state,country,seniority,departments,emailStatus,foundedYear,annualRevenue,fundingEvents,alexaRanking,keywords,orgHQ,totalFunding,fundingStage]);
    res.json({ first_name:person.first_name||'',last_name:person.last_name||'',title:person.title||'',company:org.name||'',company_size:org.estimated_num_employees?.toString()||'',industry:org.industry||'',linkedin_url:person.linkedin_url||'',website:org.website_url||'' });
  } catch (err) { console.error('[/enrich] Error:', err.message, err.detail||''); recordFailure('Apollo', email || 'unknown', err.message); res.json({ first_name:'',last_name:'',title:'',company:'',company_size:'',industry:'',linkedin_url:'',website:'' }); }
});

app.post('/partial', async (req, res) => {
  _lastLeadAt = Date.now(); // heartbeat: form traffic is flowing
  const session_id         = (req.body.session_id         || '').toString().trim().slice(0, 100);
  const page_url           = (req.body.page_url           || '').toString().trim().slice(0, 500);
  const email              = (req.body.email              || '').toString().trim().slice(0, 254).toLowerCase();
  const website            = (req.body.website            || '').toString().trim().slice(0, 500);
  const sell_to            = (req.body.sell_to            || '').toString().trim().slice(0, 50);
  const first_name         = (req.body.first_name         || '').toString().trim().slice(0, 100);
  const last_name          = (req.body.last_name          || '').toString().trim().slice(0, 100);
  const phone              = (req.body.phone              || '').toString().trim().slice(0, 30);
  const company            = (req.body.company            || '').toString().trim().slice(0, 200);
  const hear_about_us      = (req.body.hear_about_us      || '').toString().trim().slice(0, 200);
  const utm_source         = (req.body.utm_source         || '').toString().trim().slice(0, 100);
  const utm_medium         = (req.body.utm_medium         || '').toString().trim().slice(0, 100);
  const utm_campaign       = (req.body.utm_campaign       || '').toString().trim().slice(0, 100);
  const utm_content        = (req.body.utm_content        || '').toString().trim().slice(0, 100);
  const utm_term           = (req.body.utm_term           || '').toString().trim().slice(0, 100);
  const referrer           = (req.body.referrer           || '').toString().trim().slice(0, 500);
  const prefill_source     = (req.body.prefill_source     || '').toString().trim().slice(0, 100);
  const fbc                = (req.body.fbc                || '').toString().trim().slice(0, 500);
  const fbp                = (req.body.fbp                || '').toString().trim().slice(0, 200);
  const landing_page       = (req.body.landing_page       || '').toString().trim().slice(0, 500);
  const previous_page      = (req.body.previous_page      || '').toString().trim().slice(0, 500);
  const enriched_title     = (req.body.enriched_title     || '').toString().trim().slice(0, 200);
  const enriched_company_size = (req.body.enriched_company_size || '').toString().trim().slice(0, 50);
  const enriched_industry  = (req.body.enriched_industry  || '').toString().trim().slice(0, 200);
  const enriched_linkedin  = (req.body.enriched_linkedin  || '').toString().trim().slice(0, 500);
  const disqualified       = req.body.disqualified === true || req.body.disqualified === 'true';
  const disqualified_reason = (req.body.disqualified_reason || '').toString().trim().slice(0, 100);
  const step_reached       = parseInt(req.body.step_reached) || 1;
  const website_check_failed = req.body.website_check_failed === true || req.body.website_check_failed === 'true';
  const website_check_reason = (req.body.website_check_reason || '').toString().trim().slice(0, 100);

  if (!session_id) return res.status(400).json({ error: 'session_id required' });

  try {
    await pool.query(`
      INSERT INTO leads (session_id,page_url,email,website,sell_to,first_name,last_name,phone,company,hear_about_us,utm_source,utm_medium,utm_campaign,utm_content,utm_term,referrer,prefill_source,fbc,fbp,landing_page,previous_page,enriched_title,enriched_company_size,enriched_industry,enriched_linkedin,disqualified,disqualified_reason,step_reached,completed,updated_at,website_check_failed,website_check_reason)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,false,NOW(),$29,$30)
      ON CONFLICT (session_id) DO UPDATE SET
        page_url              = COALESCE(EXCLUDED.page_url,              leads.page_url),
        email                 = COALESCE(EXCLUDED.email,                 leads.email),
        website               = COALESCE(EXCLUDED.website,               leads.website),
        sell_to               = COALESCE(EXCLUDED.sell_to,               leads.sell_to),
        first_name            = COALESCE(EXCLUDED.first_name,            leads.first_name),
        last_name             = COALESCE(EXCLUDED.last_name,             leads.last_name),
        phone                 = COALESCE(EXCLUDED.phone,                 leads.phone),
        company               = COALESCE(EXCLUDED.company,               leads.company),
        hear_about_us         = COALESCE(EXCLUDED.hear_about_us,         leads.hear_about_us),
        utm_source            = COALESCE(EXCLUDED.utm_source,            leads.utm_source),
        utm_medium            = COALESCE(EXCLUDED.utm_medium,            leads.utm_medium),
        utm_campaign          = COALESCE(EXCLUDED.utm_campaign,          leads.utm_campaign),
        utm_content           = COALESCE(EXCLUDED.utm_content,           leads.utm_content),
        utm_term              = COALESCE(EXCLUDED.utm_term,              leads.utm_term),
        referrer              = COALESCE(EXCLUDED.referrer,              leads.referrer),
        prefill_source        = COALESCE(EXCLUDED.prefill_source,        leads.prefill_source),
        fbc                   = COALESCE(EXCLUDED.fbc,                   leads.fbc),
        fbp                   = COALESCE(EXCLUDED.fbp,                   leads.fbp),
        landing_page          = COALESCE(EXCLUDED.landing_page,          leads.landing_page),
        previous_page         = COALESCE(EXCLUDED.previous_page,         leads.previous_page),
        enriched_title        = COALESCE(EXCLUDED.enriched_title,        leads.enriched_title),
        enriched_company_size = COALESCE(EXCLUDED.enriched_company_size, leads.enriched_company_size),
        enriched_industry     = COALESCE(EXCLUDED.enriched_industry,     leads.enriched_industry),
        enriched_linkedin     = COALESCE(EXCLUDED.enriched_linkedin,     leads.enriched_linkedin),
        disqualified          = EXCLUDED.disqualified,
        disqualified_reason   = COALESCE(EXCLUDED.disqualified_reason,   leads.disqualified_reason),
        step_reached          = GREATEST(EXCLUDED.step_reached,          leads.step_reached),
        updated_at            = NOW(),
        website_check_failed  = EXCLUDED.website_check_failed,
        website_check_reason  = COALESCE(EXCLUDED.website_check_reason,  leads.website_check_reason)
    `, [session_id,page_url||null,email||null,website||null,sell_to||null,first_name||null,last_name||null,phone||null,company||null,hear_about_us||null,utm_source||null,utm_medium||null,utm_campaign||null,utm_content||null,utm_term||null,referrer||null,prefill_source||null,fbc||null,fbp||null,landing_page||null,previous_page||null,enriched_title||null,enriched_company_size||null,enriched_industry||null,enriched_linkedin||null,disqualified,disqualified_reason||null,step_reached,website_check_failed,website_check_reason||null]);

    await pool.query(`UPDATE leads SET enriched_city=e.enriched_city,enriched_state=e.enriched_state,enriched_country=e.enriched_country,enriched_seniority=e.enriched_seniority,enriched_departments=e.enriched_departments,enriched_email_status=e.enriched_email_status,enriched_founded_year=e.enriched_founded_year,enriched_annual_revenue=e.enriched_annual_revenue,enriched_funding_events=e.enriched_funding_events,enriched_alexa_ranking=e.enriched_alexa_ranking,enriched_keywords=e.enriched_keywords,enriched_org_hq=e.enriched_org_hq,enriched_total_funding=e.enriched_total_funding,enriched_funding_stage=e.enriched_funding_stage,updated_at=NOW() FROM enrichment_data e WHERE leads.session_id=e.session_id AND leads.session_id=$1`, [session_id]).catch(err => console.warn('[/partial] Enrichment sync failed (non-blocking):', err.message));

    syncToAWS({session_id,page_url,email,website,sell_to,first_name,last_name,phone,company,hear_about_us,utm_source,utm_medium,utm_campaign,utm_content,utm_term,referrer,prefill_source,fbc,fbp,landing_page,previous_page,enriched_title,enriched_company_size,enriched_industry,enriched_linkedin,disqualified,disqualified_reason,step_reached,completed:false});

    // StartTrial fires ONLY for qualified (B2B) leads on BUSINESS emails —
    // free-mailbox leads (gmail/yahoo/...) are skipped so Meta optimises
    // on higher-intent signals. Email is already lowercased at parse time
    // and already ELV-verified by the form before /partial is called.
    const isBusinessEmail = !!email && !FREE_EMAIL_DOMAINS.includes(email.split('@')[1] || '');
    if (!disqualified && isBusinessEmail) {
      pushStartTrialToMeta({session_id,email,sell_to,page_url,fbc,fbp,landing_page}, {clientIpAddress:req.headers['x-forwarded-for']||req.ip||'',clientUserAgent:req.headers['user-agent']||''}).catch(err => { console.warn('[/partial] Meta CAPI StartTrial failed (non-blocking):', err.message); recordFailure('Meta CAPI', email + ' (StartTrial)', err.message); });
    } else if (!disqualified) {
      console.log(`[/partial] ⏭ StartTrial skipped — free email domain: ${email}`);
    }

    console.log(`[/partial] ✅ Saved session ${session_id} | step ${step_reached} | disqualified: ${disqualified} | email ${email}`);
    res.json({ ok: true });
  } catch (err) { console.error('[/partial]', err.message); res.status(500).json({ error: 'Partial save failed' }); }
});

app.post('/submit', async (req, res) => {
  const session_id         = (req.body.session_id         || '').toString().trim().slice(0, 100);
  const page_url           = (req.body.page_url           || '').toString().trim().slice(0, 500);
  const email              = (req.body.email              || '').toString().trim().slice(0, 254).toLowerCase();
  const website            = (req.body.website            || '').toString().trim().slice(0, 500);
  const sell_to            = (req.body.sell_to            || '').toString().trim().slice(0, 50);
  const first_name         = (req.body.first_name         || '').toString().trim().slice(0, 100);
  const last_name          = (req.body.last_name          || '').toString().trim().slice(0, 100);
  const phone              = (req.body.phone              || '').toString().trim().slice(0, 30);
  const company            = (req.body.company            || '').toString().trim().slice(0, 200);
  const hear_about_us      = (req.body.hear_about_us      || '').toString().trim().slice(0, 200);
  const utm_source         = (req.body.utm_source         || '').toString().trim().slice(0, 100);
  const utm_medium         = (req.body.utm_medium         || '').toString().trim().slice(0, 100);
  const utm_campaign       = (req.body.utm_campaign       || '').toString().trim().slice(0, 100);
  const utm_content        = (req.body.utm_content        || '').toString().trim().slice(0, 100);
  const utm_term           = (req.body.utm_term           || '').toString().trim().slice(0, 100);
  const referrer           = (req.body.referrer           || '').toString().trim().slice(0, 500);
  const prefill_source     = (req.body.prefill_source     || '').toString().trim().slice(0, 100);
  const fbc                = (req.body.fbc                || '').toString().trim().slice(0, 500);
  const fbp                = (req.body.fbp                || '').toString().trim().slice(0, 200);
  const landing_page       = (req.body.landing_page       || '').toString().trim().slice(0, 500);
  const previous_page      = (req.body.previous_page      || '').toString().trim().slice(0, 500);
  const enriched_title     = (req.body.enriched_title     || '').toString().trim().slice(0, 200);
  const enriched_company_size = (req.body.enriched_company_size || '').toString().trim().slice(0, 50);
  const enriched_industry  = (req.body.enriched_industry  || '').toString().trim().slice(0, 200);
  const enriched_linkedin  = (req.body.enriched_linkedin  || '').toString().trim().slice(0, 500);
  const disqualified       = req.body.disqualified === true || req.body.disqualified === 'true';
  const disqualified_reason = (req.body.disqualified_reason || '').toString().trim().slice(0, 100);
  const website_check_failed = req.body.website_check_failed === true || req.body.website_check_failed === 'true';
  const website_check_reason = (req.body.website_check_reason || '').toString().trim().slice(0, 100);

  if (!session_id) return res.status(400).json({ error: 'session_id required' });

  try {
    const existing        = await pool.query('SELECT completed FROM leads WHERE session_id=$1', [session_id]);
    const alreadyCompleted = existing.rows[0]?.completed === true;
    const enrichRow       = await pool.query('SELECT * FROM enrichment_data WHERE session_id=$1', [session_id]);
    const enrich          = enrichRow.rows[0] || {};

    await pool.query(`
      INSERT INTO leads (session_id,page_url,email,website,sell_to,first_name,last_name,phone,company,hear_about_us,utm_source,utm_medium,utm_campaign,utm_content,utm_term,referrer,prefill_source,fbc,fbp,landing_page,previous_page,enriched_title,enriched_company_size,enriched_industry,enriched_linkedin,disqualified,disqualified_reason,step_reached,completed,submitted_at,updated_at,website_check_failed,website_check_reason)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,2,true,NOW(),NOW(),$28,$29)
      ON CONFLICT (session_id) DO UPDATE SET
        page_url              = COALESCE(EXCLUDED.page_url,              leads.page_url),
        email                 = COALESCE(EXCLUDED.email,                 leads.email),
        website               = COALESCE(EXCLUDED.website,               leads.website),
        sell_to               = COALESCE(EXCLUDED.sell_to,               leads.sell_to),
        first_name            = COALESCE(EXCLUDED.first_name,            leads.first_name),
        last_name             = COALESCE(EXCLUDED.last_name,             leads.last_name),
        phone                 = COALESCE(EXCLUDED.phone,                 leads.phone),
        company               = COALESCE(EXCLUDED.company,               leads.company),
        hear_about_us         = COALESCE(EXCLUDED.hear_about_us,         leads.hear_about_us),
        utm_source            = COALESCE(EXCLUDED.utm_source,            leads.utm_source),
        utm_medium            = COALESCE(EXCLUDED.utm_medium,            leads.utm_medium),
        utm_campaign          = COALESCE(EXCLUDED.utm_campaign,          leads.utm_campaign),
        utm_content           = COALESCE(EXCLUDED.utm_content,           leads.utm_content),
        utm_term              = COALESCE(EXCLUDED.utm_term,              leads.utm_term),
        referrer              = COALESCE(EXCLUDED.referrer,              leads.referrer),
        prefill_source        = COALESCE(EXCLUDED.prefill_source,        leads.prefill_source),
        fbc                   = COALESCE(EXCLUDED.fbc,                   leads.fbc),
        fbp                   = COALESCE(EXCLUDED.fbp,                   leads.fbp),
        landing_page          = COALESCE(EXCLUDED.landing_page,          leads.landing_page),
        previous_page         = COALESCE(EXCLUDED.previous_page,         leads.previous_page),
        enriched_title        = COALESCE(EXCLUDED.enriched_title,        leads.enriched_title),
        enriched_company_size = COALESCE(EXCLUDED.enriched_company_size, leads.enriched_company_size),
        enriched_industry     = COALESCE(EXCLUDED.enriched_industry,     leads.enriched_industry),
        enriched_linkedin     = COALESCE(EXCLUDED.enriched_linkedin,     leads.enriched_linkedin),
        disqualified          = EXCLUDED.disqualified,
        disqualified_reason   = COALESCE(EXCLUDED.disqualified_reason,   leads.disqualified_reason),
        step_reached          = 2,
        completed             = true,
        submitted_at          = NOW(),
        updated_at            = NOW(),
        website_check_failed  = EXCLUDED.website_check_failed,
        website_check_reason  = COALESCE(EXCLUDED.website_check_reason,  leads.website_check_reason)
    `, [session_id,page_url||null,email||null,website||null,sell_to||null,first_name||null,last_name||null,phone||null,company||null,hear_about_us||null,utm_source||null,utm_medium||null,utm_campaign||null,utm_content||null,utm_term||null,referrer||null,prefill_source||null,fbc||null,fbp||null,landing_page||null,previous_page||null,enriched_title||null,enriched_company_size||null,enriched_industry||null,enriched_linkedin||null,disqualified,disqualified_reason||null,website_check_failed,website_check_reason||null]);

    await pool.query(`UPDATE leads SET enriched_city=e.enriched_city,enriched_state=e.enriched_state,enriched_country=e.enriched_country,enriched_seniority=e.enriched_seniority,enriched_departments=e.enriched_departments,enriched_email_status=e.enriched_email_status,enriched_founded_year=e.enriched_founded_year,enriched_annual_revenue=e.enriched_annual_revenue,enriched_funding_events=e.enriched_funding_events,enriched_alexa_ranking=e.enriched_alexa_ranking,enriched_keywords=e.enriched_keywords,enriched_org_hq=e.enriched_org_hq,enriched_total_funding=e.enriched_total_funding,enriched_funding_stage=e.enriched_funding_stage,updated_at=NOW() FROM enrichment_data e WHERE leads.session_id=e.session_id AND leads.session_id=$1`, [session_id]).catch(err => console.warn('[/submit] Enrichment sync failed (non-blocking):', err.message));

    syncToAWS({session_id,page_url,email,website,sell_to,first_name,last_name,phone,company,hear_about_us,utm_source,utm_medium,utm_campaign,utm_content,utm_term,referrer,prefill_source,fbc,fbp,landing_page,previous_page,enriched_title:enrich.enriched_title,enriched_company_size:enrich.enriched_company_size,enriched_industry:enrich.enriched_industry,enriched_linkedin:enrich.enriched_linkedin,enriched_city:enrich.enriched_city,enriched_state:enrich.enriched_state,enriched_country:enrich.enriched_country,enriched_seniority:enrich.enriched_seniority,enriched_departments:enrich.enriched_departments,enriched_email_status:enrich.enriched_email_status,enriched_founded_year:enrich.enriched_founded_year,enriched_annual_revenue:enrich.enriched_annual_revenue,enriched_funding_events:enrich.enriched_funding_events,enriched_alexa_ranking:enrich.enriched_alexa_ranking,enriched_keywords:enrich.enriched_keywords,enriched_org_hq:enrich.enriched_org_hq,enriched_total_funding:enrich.enriched_total_funding,enriched_funding_stage:enrich.enriched_funding_stage,disqualified,disqualified_reason,step_reached:2,completed:true});

    if (!alreadyCompleted) {
      slackSubmit({first_name,last_name,email,phone,company,website,sell_to,hear_about_us,landing_page,previous_page,page_url,referrer,utm_source,utm_medium,utm_campaign,utm_content,prefill_source,website_check_failed,website_check_reason,enriched_title:enrich.enriched_title,enriched_company_size:enrich.enriched_company_size,enriched_industry:enrich.enriched_industry,enriched_linkedin:enrich.enriched_linkedin,enriched_city:enrich.enriched_city,enriched_state:enrich.enriched_state,enriched_country:enrich.enriched_country,enriched_seniority:enrich.enriched_seniority,enriched_departments:enrich.enriched_departments,enriched_email_status:enrich.enriched_email_status,enriched_founded_year:enrich.enriched_founded_year,enriched_annual_revenue:enrich.enriched_annual_revenue,enriched_funding_events:enrich.enriched_funding_events,enriched_alexa_ranking:enrich.enriched_alexa_ranking,enriched_keywords:enrich.enriched_keywords,enriched_org_hq:enrich.enriched_org_hq,enriched_total_funding:enrich.enriched_total_funding,enriched_funding_stage:enrich.enriched_funding_stage});

      pushToSalesforce({first_name,last_name,email,phone,company,website,sell_to,hear_about_us,page_url,fbc,fbp,utm_source,utm_medium,utm_campaign,utm_content,utm_term,referrer,landing_page,enriched_title:enrich.enriched_title,enriched_company_size:enrich.enriched_company_size,enriched_industry:enrich.enriched_industry,enriched_linkedin:enrich.enriched_linkedin,enriched_seniority:enrich.enriched_seniority,enriched_departments:enrich.enriched_departments,enriched_city:enrich.enriched_city,enriched_state:enrich.enriched_state,enriched_country:enrich.enriched_country,enriched_annual_revenue:enrich.enriched_annual_revenue,enriched_total_funding:enrich.enriched_total_funding,enriched_funding_stage:enrich.enriched_funding_stage,enriched_founded_year:enrich.enriched_founded_year,step_reached:2,booked:false}).catch(err => { console.warn('[/submit] SF push failed (non-blocking):', err.message); alertOps('critical', 'Salesforce', 'Lead not created', { 'Email': email, 'Stage': 'form completed', 'Error': err.message, 'Impact': 'This lead is NOT in Salesforce. Add it manually.' }); });

      // Meta CAPI Lead — suppressed when the website check failed (temporary
      // non-blocking mode still lets the lead through, but keeps the Lead
      // event clean). Slack/SF above still fire normally either way.
      if (isWebsiteVerified({ website_check_failed, website_check_reason })) {
        pushFormEventsToMeta({session_id,email,phone,first_name,last_name,company,website,sell_to,page_url,fbc,fbp,landing_page,enriched_city:enrich.enriched_city,enriched_state:enrich.enriched_state,enriched_country:enrich.enriched_country,enriched_company_size:enrich.enriched_company_size,enriched_industry:enrich.enriched_industry,enriched_seniority:enrich.enriched_seniority,enriched_funding_stage:enrich.enriched_funding_stage}, {clientIpAddress:req.headers['x-forwarded-for']||req.ip||'',clientUserAgent:req.headers['user-agent']||''}).catch(err => { console.warn('[/submit] Meta CAPI failed (non-blocking):', err.message); recordFailure('Meta CAPI', email + ' (Lead)', err.message); });
      } else {
        console.log(`[/submit] ⏭ Meta CAPI Lead skipped — website not verified (${website_check_reason || 'failed'}): ${email}`);
      }

      console.log(`[/submit] ✅ Lead completed: ${email} | session: ${session_id}`);
    } else {
      console.log(`[/submit] ⏭ Slack skipped — already completed: ${email} | session: ${session_id}`);
    }
    res.json({ ok: true });
  } catch (err) { console.error('[/submit]', err.message); res.status(500).json({ error: 'Submit failed' }); }
});

app.post('/booking-confirmed', async (req, res) => {
  const session_id  = (req.body.session_id  || '').toString().trim().slice(0, 100);
  const booking_uid = (req.body.booking_uid || '').toString().trim().slice(0, 100);
  const start_time  = req.body.start_time   || null;
  const end_time    = req.body.end_time     || null;
  const event_type  = (req.body.event_type  || '').toString().trim().slice(0, 100);
  if (!session_id || !booking_uid) return res.status(400).json({ error: 'session_id and booking_uid required' });

  try {
    const existing = await pool.query('SELECT booking_uid FROM leads WHERE session_id=$1', [session_id]);

    if (existing.rows[0]?.booking_uid) {
      console.log(`[/booking-confirmed] ⏭ Booking already recorded for session ${session_id} (booking_uid: ${existing.rows[0].booking_uid}) — skipping duplicate write`);
      return res.json({ ok: true, skipped: true, reason: 'already_booked' });
    }

    await pool.query('UPDATE leads SET booking_uid=$2,start_time=$3,end_time=$4,event_type=$5,completed=true,booked_at=NOW(),updated_at=NOW() WHERE session_id=$1', [session_id,booking_uid,start_time,end_time,event_type||null]);
    syncBookingToAWS(session_id,booking_uid,start_time,end_time,event_type);
    const leadRow = await pool.query('SELECT email FROM leads WHERE session_id=$1', [session_id]);
    const email   = leadRow.rows[0]?.email;
    if (email) {
      findSFLeadByEmail(email).then(leadId => {
        if (leadId) return updateSFLead(leadId, { booking_uid__c: booking_uid, booking_start_time__c: start_time || '', booking_event_type__c: event_type || '', completed__c: true });
      }).catch(err => { console.warn('[/booking-confirmed] SF update failed (non-blocking):', err.message); alertOps('warning', 'Salesforce', 'Booking not recorded', { 'Session': session_id, 'Error': err.message, 'Impact': 'The lead exists in Salesforce but the booking is missing.' }); });
      pool.query('SELECT * FROM leads l LEFT JOIN enrichment_data e ON e.session_id=l.session_id WHERE l.session_id=$1', [session_id]).then(r => {
        const fullLead = r.rows[0] || {};
        if (!isWebsiteVerified(fullLead)) { console.log(`[/booking-confirmed] ⏭ Meta CAPI Schedule skipped — website not verified: session ${session_id}`); return; }
        return pushFormEventsToMeta({...fullLead, booking_uid}, {clientIpAddress:req.headers['x-forwarded-for']||req.ip||'',clientUserAgent:req.headers['user-agent']||''});
      }).catch(err => { console.warn('[/booking-confirmed] Meta CAPI failed (non-blocking):', err.message); recordFailure('Meta CAPI', session_id + ' (Schedule)', err.message); });
    }
    console.log(`[/booking-confirmed] ✅ Booked: ${booking_uid} | session: ${session_id} | email: ${email}`);
    res.json({ ok: true });
  } catch (err) { console.error('[/booking-confirmed]', err.message); res.status(500).json({ error: 'Booking update failed' }); }
});

app.post('/booking-confirmed-webhook', async (req, res) => {
  const calSecret = process.env.CAL_WEBHOOK_SECRET;
  if (calSecret) {
    const signature = req.headers['x-cal-signature-256'] || req.headers['cal-signature'];
    if (signature) {
      const expected = crypto.createHmac('sha256', calSecret).update(JSON.stringify(req.body)).digest('hex');
      if (signature !== expected) {
        console.warn('[/cal-webhook] ⚠ Invalid signature — rejecting');
        return res.status(401).json({ error: 'Invalid signature' });
      }
    }
  }

  try {
    const payload      = req.body.payload || req.body;
    const triggerEvent = req.body.triggerEvent || '';

    if (triggerEvent && triggerEvent !== 'BOOKING_CREATED') {
      console.log(`[/cal-webhook] Ignoring event: ${triggerEvent}`);
      return res.json({ ok: true, skipped: true });
    }

    const attendees  = payload.attendees || [];
    const attendee   = attendees[0] || {};
    const email      = (attendee.email || payload.responses?.email?.value || '').toString().trim().toLowerCase();
    const TEST_EMAILS_CAL = ['b@g.ai'];
    if (TEST_EMAILS_CAL.includes(email)) {
      console.log(`[/cal-webhook] ⏭ Test email — skipping all processing`);
      return res.json({ ok: true, skipped: true, reason: 'test_email' });
    }
    const calName    = attendee.name || payload.responses?.name?.value || '';
    const bookingUid = payload.uid || payload.bookingUid || '';
    const startTime  = payload.startTime || '';
    const endTime    = payload.endTime   || '';
    const eventType  = payload.type || payload.eventTypeSlug || '';

    if (!email || !bookingUid) {
      console.warn('[/cal-webhook] Missing email or booking_uid — skipping');
      return res.status(400).json({ error: 'email and booking_uid required' });
    }

    const eventSlug = payload.eventTypeSlug || payload.type || '';
    if (eventSlug && !eventSlug.toLowerCase().includes('demo')) {
      console.log(`[/cal-webhook] Skipping non-demo event: ${eventSlug} | email: ${email}`);
      return res.json({ ok: true, skipped: true, reason: 'non-demo event' });
    }

    console.log(`[/cal-webhook] Received booking: ${bookingUid} | email: ${email} | name: ${calName} | event: ${eventType}`);

    const existingLead = await pool.query('SELECT session_id, email, booking_uid FROM leads WHERE LOWER(email)=LOWER($1) ORDER BY created_at DESC LIMIT 1', [email]);

    if (existingLead.rows.length > 0) {
      const lead = existingLead.rows[0];
      if (!lead.booking_uid) {
        await pool.query('UPDATE leads SET booking_uid=$2,start_time=$3,end_time=$4,event_type=$5,completed=true,booked_at=NOW(),updated_at=NOW() WHERE session_id=$1', [lead.session_id, bookingUid, startTime || null, endTime || null, eventType || null]);
        syncBookingToAWS(lead.session_id, bookingUid, startTime, endTime, eventType);
        findSFLeadByEmail(email).then(leadId => {
          if (leadId) return updateSFLead(leadId, { booking_uid__c: bookingUid, booking_start_time__c: startTime || '', booking_event_type__c: eventType || '', completed__c: true });
        }).catch(err => { console.warn('[/cal-webhook] SF update failed (non-blocking):', err.message); alertOps('warning', 'Salesforce', 'Booking not recorded', { 'Email': email, 'Error': err.message, 'Impact': 'The lead exists in Salesforce but the booking is missing.' }); });
        pool.query('SELECT * FROM leads l LEFT JOIN enrichment_data e ON e.session_id=l.session_id WHERE l.session_id=$1', [lead.session_id]).then(r => {
          const fullLead = r.rows[0] || {};
          if (!isWebsiteVerified(fullLead)) { console.log(`[/cal-webhook] ⏭ Meta CAPI Schedule skipped — website not verified: session ${lead.session_id}`); return; }
          return pushFormEventsToMeta({...fullLead, booking_uid: bookingUid}, {clientIpAddress:'',clientUserAgent:''});
        }).catch(err => { console.warn('[/cal-webhook] Meta CAPI failed (non-blocking):', err.message); recordFailure('Meta CAPI', email + ' (Schedule)', err.message); });
        console.log(`[/cal-webhook] ✅ Updated existing lead: ${email} | session: ${lead.session_id}`);
      } else {
        console.log(`[/cal-webhook] ⏭ Lead already booked: ${email} | existing booking: ${lead.booking_uid}`);
      }
      return res.json({ ok: true, action: 'updated_existing' });
    }

    const enrichRow = await pool.query('SELECT * FROM enrichment_data WHERE LOWER(email)=LOWER($1) ORDER BY enriched_at DESC LIMIT 1', [email]);
    const enrich    = enrichRow.rows[0] || {};

    const nameParts  = calName.split(' ');
    const firstName  = enrich.enriched_first_name || nameParts[0] || '';
    const lastName   = enrich.enriched_last_name  || nameParts.slice(1).join(' ') || '';
    const company    = enrich.enriched_company || '';
    const webhookSessionId = 'cal-webhook-' + bookingUid;

    await pool.query(`
      INSERT INTO leads (session_id,email,first_name,last_name,company,enriched_title,enriched_company_size,enriched_industry,enriched_linkedin,enriched_city,enriched_state,enriched_country,enriched_seniority,enriched_departments,enriched_email_status,enriched_founded_year,enriched_annual_revenue,enriched_funding_events,enriched_alexa_ranking,enriched_keywords,enriched_org_hq,enriched_total_funding,enriched_funding_stage,step_reached,completed,submitted_at,booking_uid,start_time,end_time,event_type,booked_at,prefill_source,sell_to,updated_at)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,2,true,NOW(),$24,$25,$26,$27,NOW(),'cal_webhook','B2B',NOW())
      ON CONFLICT (session_id) DO NOTHING
    `, [webhookSessionId, email, firstName||null, lastName||null, company||null, enrich.enriched_title||null, enrich.enriched_company_size||null, enrich.enriched_industry||null, enrich.enriched_linkedin||null, enrich.enriched_city||null, enrich.enriched_state||null, enrich.enriched_country||null, enrich.enriched_seniority||null, enrich.enriched_departments||null, enrich.enriched_email_status||null, enrich.enriched_founded_year||null, enrich.enriched_annual_revenue||null, enrich.enriched_funding_events||null, enrich.enriched_alexa_ranking||null, enrich.enriched_keywords||null, enrich.enriched_org_hq||null, enrich.enriched_total_funding||null, enrich.enriched_funding_stage||null, bookingUid, startTime||null, endTime||null, eventType||null]);

    let enrichData = enrich;
    if (!enrich.enriched_title) {
      const personalDomains = FREE_EMAIL_DOMAINS;
      const domain = email.split('@')[1]?.toLowerCase() || '';
      if (!personalDomains.includes(domain) && process.env.APOLLO_API_KEY) {
        console.log(`[/cal-webhook] Awaiting Apollo enrichment for: ${email}`);
        try {
          const enrichRes = await fetch(`http://localhost:${PORT}/enrich`, { method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({email,session_id:webhookSessionId}) });
          if (enrichRes.ok) {
            const freshEnrich = await pool.query('SELECT * FROM enrichment_data WHERE session_id=$1', [webhookSessionId]);
            if (freshEnrich.rows[0]) {
              enrichData = freshEnrich.rows[0];
              await pool.query(`UPDATE leads SET first_name=COALESCE(leads.first_name,$2),last_name=COALESCE(leads.last_name,$3),company=COALESCE(leads.company,$4),enriched_title=$5,enriched_company_size=$6,enriched_industry=$7,enriched_linkedin=$8,enriched_city=$9,enriched_state=$10,enriched_country=$11,enriched_seniority=$12,enriched_departments=$13,enriched_email_status=$14,enriched_founded_year=$15,enriched_annual_revenue=$16,enriched_funding_events=$17,enriched_alexa_ranking=$18,enriched_keywords=$19,enriched_org_hq=$20,enriched_total_funding=$21,enriched_funding_stage=$22,updated_at=NOW() WHERE session_id=$1`, [webhookSessionId, enrichData.enriched_first_name||null, enrichData.enriched_last_name||null, enrichData.enriched_company||null, enrichData.enriched_title||null, enrichData.enriched_company_size||null, enrichData.enriched_industry||null, enrichData.enriched_linkedin||null, enrichData.enriched_city||null, enrichData.enriched_state||null, enrichData.enriched_country||null, enrichData.enriched_seniority||null, enrichData.enriched_departments||null, enrichData.enriched_email_status||null, enrichData.enriched_founded_year||null, enrichData.enriched_annual_revenue||null, enrichData.enriched_funding_events||null, enrichData.enriched_alexa_ranking||null, enrichData.enriched_keywords||null, enrichData.enriched_org_hq||null, enrichData.enriched_total_funding||null, enrichData.enriched_funding_stage||null]);
              console.log(`[/cal-webhook] Apollo enrichment applied for: ${email}`);
            }
          }
        } catch (err) { console.warn('[/cal-webhook] Apollo enrichment failed (non-blocking):', err.message); }
      }
    }

    const slackFirstName = enrichData.enriched_first_name || firstName;
    const slackLastName  = enrichData.enriched_last_name  || lastName;
    const slackCompany   = enrichData.enriched_company    || company;

    syncToAWS({ session_id:webhookSessionId, email, first_name:slackFirstName, last_name:slackLastName, company:slackCompany, sell_to:'B2B', completed:true, step_reached:2, enriched_title:enrichData.enriched_title, enriched_company_size:enrichData.enriched_company_size, enriched_industry:enrichData.enriched_industry, enriched_linkedin:enrichData.enriched_linkedin, enriched_city:enrichData.enriched_city, enriched_state:enrichData.enriched_state, enriched_country:enrichData.enriched_country, enriched_seniority:enrichData.enriched_seniority, enriched_departments:enrichData.enriched_departments, enriched_email_status:enrichData.enriched_email_status, enriched_founded_year:enrichData.enriched_founded_year, enriched_annual_revenue:enrichData.enriched_annual_revenue, enriched_funding_events:enrichData.enriched_funding_events, enriched_alexa_ranking:enrichData.enriched_alexa_ranking, enriched_keywords:enrichData.enriched_keywords, enriched_org_hq:enrichData.enriched_org_hq, enriched_total_funding:enrichData.enriched_total_funding, enriched_funding_stage:enrichData.enriched_funding_stage, prefill_source:'cal_webhook' });

    slackSubmit({ first_name:slackFirstName, last_name:slackLastName, email, company:slackCompany, sell_to:'B2B', phone:attendee.phone||'', enriched_title:enrichData.enriched_title, enriched_company_size:enrichData.enriched_company_size, enriched_industry:enrichData.enriched_industry, enriched_linkedin:enrichData.enriched_linkedin, enriched_city:enrichData.enriched_city, enriched_state:enrichData.enriched_state, enriched_country:enrichData.enriched_country, enriched_seniority:enrichData.enriched_seniority, enriched_departments:enrichData.enriched_departments, enriched_email_status:enrichData.enriched_email_status, enriched_founded_year:enrichData.enriched_founded_year, enriched_annual_revenue:enrichData.enriched_annual_revenue, enriched_funding_events:enrichData.enriched_funding_events, enriched_alexa_ranking:enrichData.enriched_alexa_ranking, enriched_keywords:enrichData.enriched_keywords, enriched_org_hq:enrichData.enriched_org_hq, enriched_total_funding:enrichData.enriched_total_funding, enriched_funding_stage:enrichData.enriched_funding_stage, prefill_source:'cal_webhook' });

    pushToSalesforce({ first_name:slackFirstName, last_name:slackLastName, email, phone:attendee.phone||'', company:slackCompany, sell_to:'B2B', booking_uid:bookingUid, start_time:startTime, event_type:eventType, enriched_title:enrichData.enriched_title, enriched_company_size:enrichData.enriched_company_size, enriched_industry:enrichData.enriched_industry, enriched_linkedin:enrichData.enriched_linkedin, enriched_seniority:enrichData.enriched_seniority, enriched_departments:enrichData.enriched_departments, enriched_city:enrichData.enriched_city, enriched_state:enrichData.enriched_state, enriched_country:enrichData.enriched_country, enriched_annual_revenue:enrichData.enriched_annual_revenue, enriched_total_funding:enrichData.enriched_total_funding, enriched_funding_stage:enrichData.enriched_funding_stage, enriched_founded_year:enrichData.enriched_founded_year, step_reached:2, booked:true }).catch(err => { console.warn('[/cal-webhook] SF push failed (non-blocking):', err.message); alertOps('critical', 'Salesforce', 'Lead not created', { 'Email': email, 'Stage': 'booking webhook', 'Error': err.message, 'Impact': 'This lead is NOT in Salesforce. Add it manually.' }); });

    console.log(`[/cal-webhook] ✅ Created new lead: ${email} | session: ${webhookSessionId}`);
    res.json({ ok: true, action: 'created_new', session_id: webhookSessionId });

  } catch (err) {
    console.error('[/cal-webhook] Error:', err.message);
    res.status(500).json({ error: 'Webhook processing failed' });
  }
});

app.post('/cron/send-partials', async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT l.session_id, l.email, l.first_name, l.last_name, l.company, l.website, l.sell_to,
             l.utm_source, l.utm_medium, l.utm_campaign, l.utm_content, l.referrer, l.page_url,
             l.landing_page, l.previous_page,
             l.disqualified, l.disqualified_reason, l.completed,
             l.enriched_title, l.enriched_company_size, l.enriched_industry, l.enriched_linkedin,
             l.enriched_city, l.enriched_state, l.enriched_country, l.enriched_seniority,
             l.enriched_departments, l.enriched_email_status, l.enriched_founded_year,
             l.enriched_annual_revenue, l.enriched_funding_events, l.enriched_alexa_ranking,
             l.enriched_keywords, l.created_at
      FROM leads l
      WHERE l.email IS NOT NULL
        AND l.disqualified = false
        AND l.booking_uid IS NULL
        AND l.loops_sent = false
        AND l.created_at < NOW() - INTERVAL '2 hours'
        AND NOT EXISTS (
          SELECT 1 FROM leads booked
          WHERE LOWER(booked.email) = LOWER(l.email)
            AND booked.booking_uid IS NOT NULL
            AND booked.booked_at >= l.created_at
        )
    `);

    const leads = result.rows;
    _lastCronRunAt = Date.now(); // heartbeat: the scheduler reached us
    console.log(`[Cron] Found ${leads.length} leads to process`);

    for (const lead of leads) {
      if (lead.disqualified) {
        console.log(`[Cron] ⏭ Skipping disqualified lead: ${lead.email}`);
        await pool.query('UPDATE leads SET loops_sent=true WHERE session_id=$1', [lead.session_id]);
        if (awsPool) awsPool.query('UPDATE gw_form_leads SET loops_sent=true,updated_at=NOW() WHERE session_id=$1', [lead.session_id]).catch(err => console.warn('[AWS] ⚠ loops_sent sync failed:', err.message));
        continue;
      }

      const bookedCheck = await pool.query(`
        SELECT 1 FROM leads
        WHERE LOWER(email) = LOWER($1)
          AND booking_uid IS NOT NULL
          AND booked_at >= $2
        LIMIT 1
      `, [lead.email, lead.created_at]);

      if (bookedCheck.rows.length > 0) {
        console.log(`[Cron] ⏭ Skipping — email booked after drop-off (race guard): ${lead.email}`);
        await pool.query('UPDATE leads SET loops_sent=true WHERE session_id=$1', [lead.session_id]);
        if (awsPool) awsPool.query('UPDATE gw_form_leads SET loops_sent=true,updated_at=NOW() WHERE session_id=$1', [lead.session_id]).catch(err => console.warn('[AWS] ⚠ loops_sent sync failed:', err.message));
        continue;
      }

      const enrichRow = await pool.query('SELECT * FROM enrichment_data WHERE session_id=$1', [lead.session_id]);
      const enrich    = enrichRow.rows[0] || {};

      slackPartial({
        ...lead,
        enriched_title:          enrich.enriched_title,
        enriched_company_size:   enrich.enriched_company_size,
        enriched_industry:       enrich.enriched_industry,
        enriched_linkedin:       enrich.enriched_linkedin,
        enriched_city:           enrich.enriched_city,
        enriched_state:          enrich.enriched_state,
        enriched_country:        enrich.enriched_country,
        enriched_seniority:      enrich.enriched_seniority,
        enriched_departments:    enrich.enriched_departments,
        enriched_email_status:   enrich.enriched_email_status,
        enriched_founded_year:   enrich.enriched_founded_year,
        enriched_annual_revenue: enrich.enriched_annual_revenue,
        enriched_funding_events: enrich.enriched_funding_events,
        enriched_alexa_ranking:  enrich.enriched_alexa_ranking,
        enriched_keywords:       enrich.enriched_keywords,
        enriched_org_hq:         enrich.enriched_org_hq,
        enriched_total_funding:  enrich.enriched_total_funding,
        enriched_funding_stage:  enrich.enriched_funding_stage
      });

      await sendFollowUpEmail(lead.email, lead.first_name);

      await pool.query('UPDATE leads SET loops_sent=true WHERE session_id=$1', [lead.session_id]);
      if (awsPool) awsPool.query('UPDATE gw_form_leads SET loops_sent=true,updated_at=NOW() WHERE session_id=$1', [lead.session_id]).catch(err => console.warn('[AWS] ⚠ loops_sent sync failed:', err.message));

      console.log(`[Cron] ✅ Processed partial for ${lead.email} | completed: ${lead.completed}`);
    }

    res.json({ ok: true, processed: leads.length });
  } catch (err) {
    console.error('[Cron] Error:', err.message);
    res.status(500).json({ error: 'Cron failed' });
  }
});

app.post('/booking-confirmed-webhook-rh', async (req, res) => {
  console.log('[/rh-webhook] ── Incoming request ──');

  const rawBody = req.body.toString('utf8');
  let payload;
  try {
    payload = JSON.parse(rawBody);
  } catch (err) {
    console.error('[/rh-webhook] ❌ Failed to parse raw body:', err.message);
    return res.status(400).json({ error: 'Invalid JSON' });
  }

  const rhSecret = process.env.RH_WEBHOOK_SECRET;
  if (rhSecret) {
    const sigHeader = req.headers['x-rh-signature'] || '';
    const match = sigHeader.match(/t=(\d+),\s*sha256=([a-f0-9]+)/);

    if (match) {
      const timestamp     = match[1];
      const receivedHash  = match[2];
      const expectedHash  = crypto.createHmac('sha256', rhSecret).update(rawBody).digest('hex');

      console.log(`[/rh-webhook] 🔐 Signature check — timestamp: ${timestamp}`);
      console.log(`[/rh-webhook] 🔐 expected: ${expectedHash}`);
      console.log(`[/rh-webhook] 🔐 received: ${receivedHash}`);

      if (receivedHash !== expectedHash) {
        console.warn('[/rh-webhook] ⚠ Signature MISMATCH — rejecting');
        return res.status(401).json({ error: 'Invalid signature' });
      }
      console.log('[/rh-webhook] ✅ Signature verified — proceeding');
    } else {
      console.warn('[/rh-webhook] ⚠ Signature header missing or unrecognized format:', sigHeader, '— rejecting');
      return res.status(401).json({ error: 'Malformed signature header' });
    }
  } else {
    console.warn('[/rh-webhook] ⚠ RH_WEBHOOK_SECRET not set — skipping signature check entirely');
  }

  try {
    console.log('[/rh-webhook] 📦 Payload id:', payload.id, '| prospect email:', payload.prospect?.email, '| status:', payload.status, '| meeting_type:', payload.meeting_type_name);

    if (!payload.id || !payload.prospect?.email) {
      console.log('[/rh-webhook] ⏭ No meeting payload or email — skipping');
      return res.json({ ok: true, skipped: true });
    }
if (payload.router_name && payload.router_name !== 'Inbound Router - Website') {
  console.log(`[/rh-webhook] ⏭ Skipping — router: ${payload.router_name} (not inbound website router)`);
  return res.json({ ok: true, skipped: true, reason: 'non_website_router' });
}

    const email      = (payload.prospect.email || '').toString().trim().toLowerCase();
    const TEST_EMAILS_RH = ['b@g.ai'];
    if (TEST_EMAILS_RH.includes(email)) {
      console.log(`[/rh-webhook] ⏭ Test email — skipping all processing`);
      return res.json({ ok: true, skipped: true, reason: 'test_email' });
    }
    const rhName     = payload.prospect.name || '';
    const bookingUid = payload.id || '';
    const startTime  = payload.meeting_time || '';
    const eventType  = payload.meeting_type_name || 'demo';
    const status     = payload.status || '';

    if (!email || !bookingUid) {
      console.warn('[/rh-webhook] ⚠ Missing email or booking_uid — skipping');
      return res.status(400).json({ error: 'email and booking_uid required' });
    }

    if (status === 'cancelled') {
      console.log(`[/rh-webhook] ⏭ Skipping cancelled meeting: ${bookingUid} | email: ${email}`);
      return res.json({ ok: true, skipped: true, reason: 'cancelled' });
    }

    console.log(`[/rh-webhook] 📨 Received booking: ${bookingUid} | email: ${email} | name: ${rhName} | event: ${eventType} | meeting_time: ${startTime}`);

    const existingLead = await pool.query('SELECT session_id, email, booking_uid FROM leads WHERE LOWER(email)=LOWER($1) ORDER BY created_at DESC LIMIT 1', [email]);
    console.log(`[/rh-webhook] 🔎 Existing lead lookup for ${email}: ${existingLead.rows.length > 0 ? 'FOUND (session ' + existingLead.rows[0].session_id + ')' : 'NOT FOUND'}`);

    if (existingLead.rows.length > 0) {
      const lead = existingLead.rows[0];
      if (!lead.booking_uid) {
        console.log(`[/rh-webhook] ✏️ No existing booking_uid on this lead — writing booking_uid: ${bookingUid}`);
        await pool.query('UPDATE leads SET booking_uid=$2,start_time=$3,event_type=$4,completed=true,booked_at=NOW(),updated_at=NOW() WHERE session_id=$1', [lead.session_id, bookingUid, startTime || null, eventType || null]);
        syncBookingToAWS(lead.session_id, bookingUid, startTime, null, eventType);

        findSFLeadByEmail(email).then(leadId => {
          console.log(`[/rh-webhook] 🔗 SF lookup for ${email}: ${leadId ? 'Found ' + leadId : 'Not found'}`);
          if (leadId) return updateSFLead(leadId, { booking_uid__c: bookingUid, booking_start_time__c: startTime || '', booking_event_type__c: eventType || '', completed__c: true });
        }).catch(err => { console.warn('[/rh-webhook] ⚠ SF update failed (non-blocking):', err.message); alertOps('warning', 'Salesforce', 'Booking not recorded', { 'Email': email, 'Error': err.message, 'Impact': 'The lead exists in Salesforce but the booking is missing.' }); });

        pool.query('SELECT * FROM leads l LEFT JOIN enrichment_data e ON e.session_id=l.session_id WHERE l.session_id=$1', [lead.session_id]).then(r => {
          const fullLead = r.rows[0] || {};
          if (!isWebsiteVerified(fullLead)) { console.log(`[/rh-webhook] ⏭ Meta CAPI Schedule skipped — website not verified: session ${lead.session_id}`); return; }
          return pushFormEventsToMeta({...fullLead, booking_uid: bookingUid}, {clientIpAddress:'',clientUserAgent:''});
        }).catch(err => { console.warn('[/rh-webhook] ⚠ Meta CAPI failed (non-blocking):', err.message); recordFailure('Meta CAPI', email + ' (Schedule)', err.message); });

        console.log(`[/rh-webhook] ✅ Updated existing lead: ${email} | session: ${lead.session_id} | booking_uid: ${bookingUid}`);
      } else {
        console.log(`[/rh-webhook] ⏭ Lead already booked: ${email} | existing booking: ${lead.booking_uid} — skipping (dedup)`);
      }
      return res.json({ ok: true, action: 'updated_existing' });
    }

    console.log(`[/rh-webhook] ⚠ No existing session found for ${email} — falling into safety-net "create new" branch`);

    const enrichRow = await pool.query('SELECT * FROM enrichment_data WHERE LOWER(email)=LOWER($1) ORDER BY enriched_at DESC LIMIT 1', [email]);
    const enrich    = enrichRow.rows[0] || {};
    console.log(`[/rh-webhook] 🔎 Enrichment lookup for ${email}: ${enrichRow.rows.length > 0 ? 'FOUND' : 'NOT FOUND'}`);

    const nameParts  = rhName.split(' ');
    const firstName  = enrich.enriched_first_name || nameParts[0] || '';
    const lastName   = enrich.enriched_last_name  || nameParts.slice(1).join(' ') || '';
    const company    = enrich.enriched_company || '';
    const webhookSessionId = crypto.randomUUID();

    console.log(`[/rh-webhook] 🆕 Creating fallback lead — session_id: ${webhookSessionId} | name: ${firstName} ${lastName} | company: ${company}`);

    await pool.query(`
      INSERT INTO leads (session_id,email,first_name,last_name,company,enriched_title,enriched_company_size,enriched_industry,enriched_linkedin,enriched_city,enriched_state,enriched_country,enriched_seniority,enriched_departments,enriched_email_status,enriched_founded_year,enriched_annual_revenue,enriched_funding_events,enriched_alexa_ranking,enriched_keywords,enriched_org_hq,enriched_total_funding,enriched_funding_stage,step_reached,completed,submitted_at,booking_uid,start_time,event_type,booked_at,prefill_source,sell_to,updated_at)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,2,true,NOW(),$24,$25,$26,NOW(),'rh_webhook','B2B',NOW())
      ON CONFLICT (session_id) DO NOTHING
    `, [webhookSessionId, email, firstName||null, lastName||null, company||null, enrich.enriched_title||null, enrich.enriched_company_size||null, enrich.enriched_industry||null, enrich.enriched_linkedin||null, enrich.enriched_city||null, enrich.enriched_state||null, enrich.enriched_country||null, enrich.enriched_seniority||null, enrich.enriched_departments||null, enrich.enriched_email_status||null, enrich.enriched_founded_year||null, enrich.enriched_annual_revenue||null, enrich.enriched_funding_events||null, enrich.enriched_alexa_ranking||null, enrich.enriched_keywords||null, enrich.enriched_org_hq||null, enrich.enriched_total_funding||null, enrich.enriched_funding_stage||null, bookingUid, startTime||null, eventType||null]);

    syncToAWS({ session_id:webhookSessionId, email, first_name:firstName, last_name:lastName, company, sell_to:'B2B', completed:true, step_reached:2, prefill_source:'rh_webhook' });
    slackSubmit({ first_name:firstName, last_name:lastName, email, company, sell_to:'B2B', prefill_source:'rh_webhook' });
    pushToSalesforce({ first_name:firstName, last_name:lastName, email, company, sell_to:'B2B', booking_uid:bookingUid, start_time:startTime, event_type:eventType, step_reached:2, booked:true }).catch(err => { console.warn('[/rh-webhook] ⚠ SF push failed (non-blocking):', err.message); alertOps('critical', 'Salesforce', 'Lead not created', { 'Email': email, 'Stage': 'booking webhook', 'Error': err.message, 'Impact': 'This lead is NOT in Salesforce. Add it manually.' }); });

    console.log(`[/rh-webhook] ✅ Created new lead (fallback): ${email} | session: ${webhookSessionId}`);
    res.json({ ok: true, action: 'created_new', session_id: webhookSessionId });

  } catch (err) {
    console.error('[/rh-webhook] ❌ Error:', err.message);
    res.status(500).json({ error: 'Webhook processing failed' });
  }
});

/* Lead-magnet routes. Mounted HERE, not at the top: FREE_EMAIL_DOMAINS is a
   const declared further up the file, so mounting above it would throw a TDZ
   ReferenceError at boot. */
app.use(createLeadMagnetRouter({ pool, elvIsInternal, FREE_EMAIL_DOMAINS }));

async function start() {
  try {
    await initDB();
    await initAWSTable();
    app.listen(PORT, () => {
      console.log(`[GW API] Running on port ${PORT}`);
      console.log(`[GW API] Allowed origins: ${allowedOrigins.join(', ')}`);
      auditStartupConfig();
      startHeartbeat();
    });
  } catch (err) { console.error('[GW API] Failed to start:', err); process.exit(1); }
}

start();
