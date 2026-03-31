require('dotenv').config();
const express   = require('express');
const cors      = require('cors');
const helmet    = require('helmet');
const rateLimit = require('express-rate-limit');
const { Pool }  = require('pg');
const { pool, initDB } = require('./db');

const app  = express();
const PORT = process.env.PORT || 3000;

app.set('trust proxy', 1);

/* --------------------------------------------------------
   SECURITY
-------------------------------------------------------- */
app.use(helmet({
  contentSecurityPolicy: false // disabled so monitor HTML page loads scripts
}));

/* --------------------------------------------------------
   CORS
-------------------------------------------------------- */
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

app.use(express.json({ limit: '10kb' }));

/* --------------------------------------------------------
   RATE LIMITING
-------------------------------------------------------- */
const globalLimiter = rateLimit({
  windowMs: 15 * 60 * 1000, max: 100,
  message: { error: 'Too many requests — please try again later.' },
  standardHeaders: true, legacyHeaders: false
});
const strictLimiter = rateLimit({
  windowMs: 60 * 60 * 1000, max: 10,
  message: { error: 'Rate limit exceeded — please try again later.' },
  standardHeaders: true, legacyHeaders: false
});
app.use(globalLimiter);
app.use('/verify-email', strictLimiter);
app.use('/enrich',       strictLimiter);

/* --------------------------------------------------------
   AWS RDS POOL
-------------------------------------------------------- */
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

/* --------------------------------------------------------
   AWS HELPER — initAWSTable
-------------------------------------------------------- */
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
    ];

    for (const sql of migrations) {
      await awsPool.query(sql);
    }

    console.log('[AWS] gw_form_leads table ready');
  } catch (err) {
    console.warn('[AWS] Table init failed (non-blocking):', err.message);
  }
}

/* --------------------------------------------------------
   AWS HELPER — syncToAWS
-------------------------------------------------------- */
function syncToAWS(data) {
  if (!awsPool) return;
  awsPool.query(`
    INSERT INTO gw_form_leads
      (session_id, page_url,
       email, website, sell_to,
       first_name, last_name, phone, company, hear_about_us,
       utm_source, utm_medium, utm_campaign, utm_content,
       referrer, prefill_source,
       enriched_title, enriched_company_size, enriched_industry, enriched_linkedin,
       enriched_city, enriched_state, enriched_country,
       enriched_seniority, enriched_departments, enriched_email_status,
       enriched_founded_year, enriched_annual_revenue,
       enriched_funding_events, enriched_alexa_ranking, enriched_keywords,
       enriched_org_hq, enriched_total_funding, enriched_funding_stage,
       disqualified, disqualified_reason,
       step_reached, completed, submitted_at, loops_sent, updated_at)
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34,$35,$36,$37,$38,$39,$40,NOW())
    ON CONFLICT (session_id) DO UPDATE SET
      page_url                = COALESCE(EXCLUDED.page_url,                gw_form_leads.page_url),
      email                   = COALESCE(EXCLUDED.email,                   gw_form_leads.email),
      website                 = COALESCE(EXCLUDED.website,                 gw_form_leads.website),
      sell_to                 = COALESCE(EXCLUDED.sell_to,                 gw_form_leads.sell_to),
      first_name              = COALESCE(EXCLUDED.first_name,              gw_form_leads.first_name),
      last_name               = COALESCE(EXCLUDED.last_name,               gw_form_leads.last_name),
      phone                   = COALESCE(EXCLUDED.phone,                   gw_form_leads.phone),
      company                 = COALESCE(EXCLUDED.company,                 gw_form_leads.company),
      hear_about_us           = COALESCE(EXCLUDED.hear_about_us,           gw_form_leads.hear_about_us),
      utm_source              = COALESCE(EXCLUDED.utm_source,              gw_form_leads.utm_source),
      utm_medium              = COALESCE(EXCLUDED.utm_medium,              gw_form_leads.utm_medium),
      utm_campaign            = COALESCE(EXCLUDED.utm_campaign,            gw_form_leads.utm_campaign),
      utm_content             = COALESCE(EXCLUDED.utm_content,             gw_form_leads.utm_content),
      referrer                = COALESCE(EXCLUDED.referrer,                gw_form_leads.referrer),
      prefill_source          = COALESCE(EXCLUDED.prefill_source,          gw_form_leads.prefill_source),
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
      disqualified            = COALESCE(EXCLUDED.disqualified,            gw_form_leads.disqualified),
      disqualified_reason     = COALESCE(EXCLUDED.disqualified_reason,     gw_form_leads.disqualified_reason),
      step_reached            = GREATEST(EXCLUDED.step_reached,            gw_form_leads.step_reached),
      completed               = COALESCE(EXCLUDED.completed,               gw_form_leads.completed),
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
    data.enriched_title          || null,   data.enriched_company_size     || null,
    data.enriched_industry       || null,   data.enriched_linkedin         || null,
    data.enriched_city           || null,   data.enriched_state            || null,
    data.enriched_country        || null,   data.enriched_seniority        || null,
    data.enriched_departments    || null,   data.enriched_email_status     || null,
    data.enriched_founded_year   || null,   data.enriched_annual_revenue   || null,
    data.enriched_funding_events || null,   data.enriched_alexa_ranking    || null,
    data.enriched_keywords       || null,   data.enriched_org_hq           || null,
    data.enriched_total_funding  || null,   data.enriched_funding_stage    || null,
    data.disqualified            || false,  data.disqualified_reason       || null,
    data.step_reached            || 1,      data.completed                 || false,
    data.completed ? new Date() : null,     data.loops_sent                || false
  ]).then(() => {
    console.log(`[AWS] ✅ Synced session ${data.session_id}`);
  }).catch(err => {
    console.warn(`[AWS] ⚠ Sync failed for ${data.session_id}:`, err.message);
  });
}

/* --------------------------------------------------------
   AWS HELPER — syncBookingToAWS
-------------------------------------------------------- */
function syncBookingToAWS(session_id, booking_uid, start_time, end_time, event_type) {
  if (!awsPool) return;
  awsPool.query(`
    UPDATE gw_form_leads SET
      booking_uid  = $2,
      start_time   = $3,
      end_time     = $4,
      event_type   = $5,
      booked_at    = NOW(),
      completed    = true,
      updated_at   = NOW()
    WHERE session_id = $1
  `, [session_id, booking_uid, start_time || null, end_time || null, event_type || null])
  .then(() => console.log(`[AWS] ✅ Booking synced for session ${session_id}`))
  .catch(err => console.warn(`[AWS] ⚠ Booking sync failed:`, err.message));
}

/* --------------------------------------------------------
   SLACK HELPER — sendSlack
-------------------------------------------------------- */
function sendSlack(blocks, fallbackText) {
  const webhookUrl = process.env.SLACK_WEBHOOK_URL;
  if (!webhookUrl) { console.warn('[Slack] SLACK_WEBHOOK_URL not set — skipping'); return; }
  const cleanBlocks = Array.isArray(blocks) ? blocks.filter(Boolean) : null;
  const payload = cleanBlocks && cleanBlocks.length > 0
    ? { text: fallbackText || 'Gushwork notification', blocks: cleanBlocks }
    : { text: fallbackText || 'Gushwork notification' };
  fetch(webhookUrl, {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload)
  })
  .then(r => r.text().then(t => console.log(`[Slack] ✅ Sent — status: ${r.status} | response: ${t.substring(0, 50)}`)))
  .catch(err => console.warn('[Slack] ⚠ Failed:', err.message));
}

/* --------------------------------------------------------
   SLACK BLOCK HELPERS
-------------------------------------------------------- */
function bHeader(text) { return { type: 'header', text: { type: 'plain_text', text, emoji: true } }; }
function bSection(text) { return { type: 'section', text: { type: 'mrkdwn', text } }; }
function bFields(fields) {
  const filtered = fields.filter(f => f.value);
  if (!filtered.length) return null;
  return { type: 'section', fields: filtered.map(f => ({ type: 'mrkdwn', text: `*${f.label}*\n${f.value}` })) };
}
function bDivider() { return { type: 'divider' }; }
function bContext(text) { return { type: 'context', elements: [{ type: 'mrkdwn', text }] }; }

/* --------------------------------------------------------
   SLACK ENRICHMENT BLOCK BUILDER
-------------------------------------------------------- */
function buildEnrichmentBlocks(blocks, e) {
  const hasPersonInfo = e.enriched_title || e.enriched_seniority || e.enriched_departments || e.enriched_email_status;
  const hasOrgInfo    = e.enriched_company_size || e.enriched_industry || e.enriched_founded_year || e.enriched_annual_revenue || e.enriched_alexa_ranking || e.enriched_keywords;
  const hasFunding    = e.enriched_funding_events || e.enriched_total_funding || e.enriched_funding_stage;
  const hasLocation   = e.enriched_city || e.enriched_state || e.enriched_country;
  const hasOrgHQ      = e.enriched_org_hq;
  const hasLinkedIn   = e.enriched_linkedin;

  if (!hasPersonInfo && !hasOrgInfo && !hasFunding && !hasLocation && !hasOrgHQ && !hasLinkedIn) return;

  blocks.push(bDivider());
  blocks.push(bSection('*🔍 Enrichment*'));

  if (hasPersonInfo) {
    const f = bFields([
      { label: 'Title',        value: e.enriched_title },
      { label: 'Seniority',    value: e.enriched_seniority },
      { label: 'Department',   value: e.enriched_departments },
      { label: 'Email Status', value: e.enriched_email_status },
    ]);
    if (f) blocks.push(f);
  }

  if (hasOrgInfo) {
    const f = bFields([
      { label: 'Company Size',   value: e.enriched_company_size },
      { label: 'Industry',       value: e.enriched_industry },
      { label: 'Founded',        value: e.enriched_founded_year },
      { label: 'Annual Revenue', value: e.enriched_annual_revenue },
      { label: 'Alexa Rank',     value: e.enriched_alexa_ranking },
      { label: 'Keywords',       value: e.enriched_keywords },
    ]);
    if (f) blocks.push(f);
  }

  if (hasFunding) {
    blocks.push(bDivider());
    const f = bFields([
      { label: '💰 Total Funding', value: e.enriched_total_funding },
      { label: 'Funding Stage',    value: e.enriched_funding_stage },
      { label: 'Funding Events',   value: e.enriched_funding_events },
    ]);
    if (f) blocks.push(f);
  }

  if (hasLocation) {
    const location = [e.enriched_city, e.enriched_state, e.enriched_country].filter(Boolean).join(', ');
    const f = bFields([{ label: '📍 Person Location', value: location }]);
    if (f) blocks.push(f);
  }

  if (hasOrgHQ) {
    const f = bFields([{ label: '🏢 Company HQ', value: e.enriched_org_hq }]);
    if (f) blocks.push(f);
  }

  if (hasLinkedIn) {
    const f = bFields([{ label: 'LinkedIn', value: e.enriched_linkedin }]);
    if (f) blocks.push(f);
  }
}

/* --------------------------------------------------------
   SLACK FORMATTER — partial
-------------------------------------------------------- */
function slackPartial(d) {
  const label    = d.completed ? '⏰ Reached Cal — Did Not Book' : '👻 Dropped at Step 1';
  const disqNote = d.disqualified ? ` • ⚠️ ${d.disqualified_reason || 'Disqualified'}` : '';
  const blocks   = [];
  blocks.push(bHeader(label + disqNote));
  blocks.push(bDivider());
  const leadFields = bFields([
    { label: '📧 Email',    value: d.email },
    { label: '🎯 Sells to', value: d.sell_to },
    { label: '🏢 Company',  value: d.company },
    { label: '🌐 Website',  value: d.website },
  ]);
  if (leadFields) blocks.push(leadFields);
  buildEnrichmentBlocks(blocks, d);
  const hasAttribution = d.utm_source || d.utm_medium || d.utm_campaign || d.utm_content || d.referrer;
  if (hasAttribution) {
    blocks.push(bDivider());
    blocks.push(bSection('*📊 Attribution*'));
    const source = [d.utm_source, d.utm_medium].filter(Boolean).join(' / ');
    const f = bFields([
      { label: 'Source',   value: source },
      { label: 'Campaign', value: d.utm_campaign },
      { label: 'Content',  value: d.utm_content },
      { label: 'Referrer', value: d.referrer },
    ]);
    if (f) blocks.push(f);
  }
  if (d.page_url) blocks.push(bContext(`📄 ${d.page_url}`));
  sendSlack(blocks, label);
}

/* --------------------------------------------------------
   SLACK FORMATTER — submit
-------------------------------------------------------- */
function slackSubmit(d) {
  const name   = [d.first_name, d.last_name].filter(Boolean).join(' ');
  const blocks = [];
  blocks.push(bHeader('✅ Lead Form Completed'));
  blocks.push(bDivider());
  const leadFields = bFields([
    { label: '👤 Name',           value: name },
    { label: '📧 Email',          value: d.email },
    { label: '📞 Phone',          value: d.phone },
    { label: '🏢 Company',        value: d.company },
    { label: '🌐 Website',        value: d.website },
    { label: '🎯 Sells to',       value: d.sell_to },
    { label: '💬 Heard about us', value: d.hear_about_us },
  ]);
  if (leadFields) blocks.push(leadFields);
  buildEnrichmentBlocks(blocks, d);
  const hasAttribution = d.utm_source || d.utm_medium || d.utm_campaign || d.utm_content || d.referrer || d.prefill_source;
  if (hasAttribution) {
    blocks.push(bDivider());
    blocks.push(bSection('*📊 Attribution*'));
    const source = [d.utm_source, d.utm_medium].filter(Boolean).join(' / ');
    const f = bFields([
      { label: 'Source',   value: source },
      { label: 'Campaign', value: d.utm_campaign },
      { label: 'Content',  value: d.utm_content },
      { label: 'Referrer', value: d.referrer },
      { label: 'Prefill',  value: d.prefill_source },
    ]);
    if (f) blocks.push(f);
  }
  if (d.page_url) blocks.push(bContext(`📄 ${d.page_url}`));
  sendSlack(blocks, `✅ Lead Form Completed — ${d.email}`);
}

/* --------------------------------------------------------
   LOOPS HELPER — sendLoopsEvent
-------------------------------------------------------- */
async function sendLoopsEvent(email, firstName, lastName, company, website) {
  const apiKey = process.env.LOOPS_API_KEY;
  if (!apiKey) { console.warn('[Loops] LOOPS_API_KEY not set — skipping'); return; }
  if (!email) return;
  try {
    const upsertRes = await fetch('https://app.loops.so/api/v1/contacts/update', {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${apiKey}` },
      body: JSON.stringify({ email, firstName: firstName || '', lastName: lastName || '', company: company || '', website: website || '', formCompleted: false })
    });
    const upsertText = await upsertRes.text();
    console.log(`[Loops] Upsert ${email} → ${upsertRes.status} | ${upsertText.substring(0, 120)}`);
  } catch (err) { console.warn('[Loops] Upsert failed:', err.message); }
  try {
    const eventRes = await fetch('https://app.loops.so/api/v1/events/send', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${apiKey}` },
      body: JSON.stringify({ email, eventName: 'form_partial_capture' })
    });
    const eventText = await eventRes.text();
    console.log(`[Loops] Event ${email} → ${eventRes.status} | ${eventText.substring(0, 120)}`);
  } catch (err) { console.warn('[Loops] Event send failed:', err.message); }
}

/* --------------------------------------------------------
   LOOPS HELPER — cancelLoopsSequence
-------------------------------------------------------- */
async function cancelLoopsSequence(email) {
  const apiKey = process.env.LOOPS_API_KEY;
  if (!apiKey || !email) return;
  try {
    const res  = await fetch('https://app.loops.so/api/v1/contacts/update', {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${apiKey}` },
      body: JSON.stringify({ email, formCompleted: true })
    });
    const text = await res.text();
    console.log(`[Loops] Cancel ${email} → ${res.status} | ${text.substring(0, 120)}`);
  } catch (err) { console.warn('[Loops] Cancel failed:', err.message); }
}

/* --------------------------------------------------------
   HELPER — formatRevenue
-------------------------------------------------------- */
function formatRevenue(amount) {
  if (!amount) return null;
  const n = parseFloat(amount);
  if (isNaN(n)) return amount.toString();
  if (n >= 1_000_000_000) return `$${(n / 1_000_000_000).toFixed(1)}B USD`;
  if (n >= 1_000_000)     return `$${(n / 1_000_000).toFixed(1)}M USD`;
  if (n >= 1_000)         return `$${(n / 1_000).toFixed(0)}K USD`;
  return `$${n} USD`;
}

/* --------------------------------------------------------
   HEALTH CHECK
-------------------------------------------------------- */
app.get('/health', (req, res) => {
  res.json({ status: 'ok', timestamp: new Date().toISOString() });
});

/* --------------------------------------------------------
   GET /monitor/metrics  — protected by token
-------------------------------------------------------- */
app.get('/monitor/metrics', async (req, res) => {
  const token = process.env.MONITOR_TOKEN;
  if (token && req.query.token !== token) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  try {
    const [totals, enrichCount, pendingPartials, noBooking, recent, today] = await Promise.all([
      pool.query(`
        SELECT
          COUNT(*)                                             AS total,
          COUNT(*) FILTER (WHERE completed = true)            AS completed,
          COUNT(*) FILTER (WHERE booking_uid IS NOT NULL)     AS booked,
          COUNT(*) FILTER (WHERE disqualified = true)         AS disqualified,
          COUNT(*) FILTER (WHERE loops_sent = true)           AS loops_sent
        FROM leads WHERE ctid >= '(0,1)'
      `),
      pool.query(`SELECT COUNT(*) AS count FROM enrichment_data WHERE ctid >= '(0,1)'`),
      pool.query(`
        SELECT COUNT(*) AS count FROM leads
        WHERE ctid >= '(0,1)'
          AND email IS NOT NULL
          AND disqualified = false
          AND booking_uid IS NULL
          AND loops_sent = false
          AND created_at < NOW() - INTERVAL '30 minutes'
      `),
      pool.query(`
        SELECT COUNT(*) AS count FROM leads
        WHERE ctid >= '(0,1)'
          AND completed = true
          AND booking_uid IS NULL
      `),
      pool.query(`
        SELECT session_id, email, company, first_name, last_name,
               completed, booking_uid, disqualified, created_at, page_url
        FROM leads WHERE ctid >= '(0,1)'
        ORDER BY created_at DESC LIMIT 10
      `),
      pool.query(`
        SELECT COUNT(*) AS count FROM leads
        WHERE ctid >= '(0,1)'
          AND created_at >= NOW() - INTERVAL '24 hours'
      `)
    ]);

    const t = totals.rows[0];
    const total       = parseInt(t.total) || 0;
    const completed   = parseInt(t.completed) || 0;
    const booked      = parseInt(t.booked) || 0;
    const disqualified = parseInt(t.disqualified) || 0;
    const loopsSent   = parseInt(t.loops_sent) || 0;
    const enriched    = parseInt(enrichCount.rows[0].count) || 0;
    const pending     = parseInt(pendingPartials.rows[0].count) || 0;
    const noBookingUid = parseInt(noBooking.rows[0].count) || 0;
    const todayCount  = parseInt(today.rows[0].count) || 0;

    res.json({
      total, completed, booked, disqualified,
      enriched, loopsSent,
      pendingPartials: pending,
      noBookingUid,
      todayCount,
      awsSynced: !!awsPool,
      recentLeads: recent.rows,
      generatedAt: new Date().toISOString()
    });
  } catch (err) {
    console.error('[/monitor/metrics]', err.message);
    res.status(500).json({ error: 'Metrics query failed', detail: err.message });
  }
});

/* --------------------------------------------------------
   GET /monitor  — full dashboard HTML page
-------------------------------------------------------- */
app.get('/monitor', (req, res) => {
  const token = process.env.MONITOR_TOKEN;
  if (token && req.query.token !== token) {
    return res.status(401).send('<h2 style="font-family:sans-serif;padding:2rem">401 — Unauthorized. Add ?token=YOUR_TOKEN to the URL.</h2>');
  }

  const tokenParam = req.query.token ? `?token=${req.query.token}` : '';

  res.setHeader('Content-Type', 'text/html');
  res.send(`<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Gushwork Form Monitor</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
<style>
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: #f5f5f5; color: #1a1a1a; font-size: 14px; }
  .topbar { background: #fff; border-bottom: 1px solid #e5e5e5; padding: 14px 24px; display: flex; align-items: center; justify-content: space-between; position: sticky; top: 0; z-index: 10; }
  .topbar-left { display: flex; align-items: center; gap: 12px; }
  .logo { font-size: 15px; font-weight: 600; color: #1a1a1a; }
  .api-pill { display: flex; align-items: center; gap: 6px; font-size: 12px; padding: 4px 10px; border-radius: 999px; border: 1px solid #e5e5e5; background: #fff; color: #666; }
  .dot { width: 7px; height: 7px; border-radius: 50%; background: #ccc; display: inline-block; }
  .dot-green { background: #22c55e; }
  .dot-red { background: #ef4444; }
  .dot-amber { background: #f59e0b; }
  .refresh-btn { font-size: 12px; padding: 6px 14px; border-radius: 6px; border: 1px solid #e5e5e5; background: #fff; cursor: pointer; color: #333; }
  .refresh-btn:hover { background: #f5f5f5; }
  .page { max-width: 1100px; margin: 0 auto; padding: 24px; }
  .section-label { font-size: 11px; font-weight: 600; color: #888; text-transform: uppercase; letter-spacing: 0.07em; margin-bottom: 10px; }
  .grid-4 { display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; margin-bottom: 24px; }
  .grid-2 { display: grid; grid-template-columns: repeat(2, 1fr); gap: 12px; margin-bottom: 24px; }
  .card { background: #fff; border: 1px solid #e5e5e5; border-radius: 10px; padding: 16px 20px; }
  .metric-card { background: #fff; border: 1px solid #e5e5e5; border-radius: 10px; padding: 16px; }
  .metric-label { font-size: 12px; color: #888; margin-bottom: 6px; }
  .metric-value { font-size: 28px; font-weight: 600; color: #1a1a1a; line-height: 1; }
  .metric-sub { font-size: 11px; color: #aaa; margin-top: 6px; }
  .step-row { display: flex; align-items: center; justify-content: space-between; padding: 12px 0; border-bottom: 1px solid #f0f0f0; }
  .step-row:last-child { border-bottom: none; }
  .step-name { font-size: 13px; font-weight: 500; color: #1a1a1a; }
  .step-desc { font-size: 11px; color: #999; margin-top: 2px; }
  .badge { font-size: 11px; font-weight: 500; padding: 3px 9px; border-radius: 5px; }
  .badge-green  { background: #f0fdf4; color: #15803d; }
  .badge-red    { background: #fef2f2; color: #b91c1c; }
  .badge-amber  { background: #fffbeb; color: #b45309; }
  .badge-gray   { background: #f5f5f5; color: #666; }
  .alert-box { border-radius: 8px; padding: 10px 14px; margin-bottom: 8px; font-size: 13px; display: flex; align-items: flex-start; gap: 8px; }
  .alert-ok   { background: #f0fdf4; color: #15803d; border: 1px solid #bbf7d0; }
  .alert-warn { background: #fffbeb; color: #b45309; border: 1px solid #fde68a; }
  .alert-err  { background: #fef2f2; color: #b91c1c; border: 1px solid #fecaca; }
  .alert-icon { font-size: 14px; flex-shrink: 0; margin-top: 1px; }
  .funnel-row { margin-bottom: 12px; }
  .funnel-labels { display: flex; justify-content: space-between; font-size: 12px; color: #666; margin-bottom: 4px; }
  .funnel-bar { height: 8px; border-radius: 4px; background: #f0f0f0; overflow: hidden; }
  .funnel-fill { height: 100%; border-radius: 4px; transition: width 0.6s ease; }
  table { width: 100%; border-collapse: collapse; font-size: 12px; }
  th { text-align: left; padding: 8px 10px; font-weight: 500; color: #888; border-bottom: 1px solid #f0f0f0; font-size: 11px; text-transform: uppercase; letter-spacing: 0.04em; }
  td { padding: 10px; border-bottom: 1px solid #f9f9f9; color: #333; }
  tr:last-child td { border-bottom: none; }
  td.email { font-weight: 500; color: #1a1a1a; max-width: 200px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .last-updated { font-size: 11px; color: #aaa; }
  .chart-wrap { position: relative; width: 100%; height: 200px; margin-top: 16px; }
  @media (max-width: 700px) {
    .grid-4 { grid-template-columns: repeat(2, 1fr); }
    .grid-2 { grid-template-columns: 1fr; }
  }
</style>
</head>
<body>

<div class="topbar">
  <div class="topbar-left">
    <span class="logo">Gushwork — Form Monitor</span>
    <div class="api-pill">
      <span class="dot" id="api-dot"></span>
      <span id="api-status">Checking...</span>
    </div>
  </div>
  <div style="display:flex;align-items:center;gap:12px;">
    <span class="last-updated" id="last-updated">—</span>
    <button class="refresh-btn" onclick="load()">↻ Refresh</button>
  </div>
</div>

<div class="page">

  <div class="section-label" style="margin-top:4px;">Overview</div>
  <div class="grid-4">
    <div class="metric-card">
      <div class="metric-label">Total leads</div>
      <div class="metric-value" id="m-total">—</div>
      <div class="metric-sub" id="m-today">— today</div>
    </div>
    <div class="metric-card">
      <div class="metric-label">Step 2 completed</div>
      <div class="metric-value" id="m-completed">—</div>
      <div class="metric-sub" id="m-completed-pct">of leads</div>
    </div>
    <div class="metric-card">
      <div class="metric-label">Calls booked</div>
      <div class="metric-value" id="m-booked">—</div>
      <div class="metric-sub" id="m-booked-pct">of completed</div>
    </div>
    <div class="metric-card">
      <div class="metric-label">Disqualified</div>
      <div class="metric-value" id="m-disq">—</div>
      <div class="metric-sub">B2C / Mixed</div>
    </div>
  </div>

  <div class="grid-2">
    <div>
      <div class="section-label">Alerts</div>
      <div id="alerts-container">
        <div class="alert-box alert-gray" style="background:#f5f5f5;color:#666;border:1px solid #e5e5e5;">
          <span class="alert-icon">○</span> Loading alerts...
        </div>
      </div>
    </div>
    <div>
      <div class="section-label">Conversion funnel</div>
      <div class="card" style="padding:16px;">
        <div id="funnel-container">Loading...</div>
      </div>
    </div>
  </div>

  <div class="section-label">Step health</div>
  <div class="card" style="margin-bottom:24px;">
    <div class="step-row">
      <div><div class="step-name">API uptime</div><div class="step-desc">/health endpoint responding</div></div>
      <span class="badge badge-gray" id="s-api">Checking...</span>
    </div>
    <div class="step-row">
      <div><div class="step-name">Step 1 — /partial</div><div class="step-desc">Email verified + lead saved to DB + AWS</div></div>
      <span class="badge badge-gray" id="s-partial">Checking...</span>
    </div>
    <div class="step-row">
      <div><div class="step-name">Step 2 — /submit</div><div class="step-desc">Lead completed + Slack fired</div></div>
      <span class="badge badge-gray" id="s-submit">Checking...</span>
    </div>
    <div class="step-row">
      <div><div class="step-name">Apollo enrichment</div><div class="step-desc">Enrichment data saved per lead</div></div>
      <span class="badge badge-gray" id="s-enrich">Checking...</span>
    </div>
    <div class="step-row">
      <div><div class="step-name">Cal booking confirmation</div><div class="step-desc">Completed leads with booking_uid</div></div>
      <span class="badge badge-gray" id="s-cal">Checking...</span>
    </div>
    <div class="step-row">
      <div><div class="step-name">Cron — partial recovery</div><div class="step-desc">Leads pending Loops/Slack notification</div></div>
      <span class="badge badge-gray" id="s-cron">Checking...</span>
    </div>
    <div class="step-row">
      <div><div class="step-name">AWS sync</div><div class="step-desc">gw_form_leads mirror active</div></div>
      <span class="badge badge-gray" id="s-aws">Checking...</span>
    </div>
  </div>

  <div class="section-label">Recent leads</div>
  <div class="card" style="margin-bottom:24px;overflow-x:auto;">
    <div id="recent-leads-container">Loading...</div>
  </div>

  <div class="section-label">Chart — leads over time</div>
  <div class="card" style="margin-bottom:24px;">
    <div class="chart-wrap">
      <canvas id="leadsChart"></canvas>
    </div>
  </div>

</div>

<script>
const TOKEN_PARAM = '${tokenParam}';
const API = window.location.origin;
let leadsChart = null;

function setBadge(id, text, color) {
  const el = document.getElementById(id);
  if (!el) return;
  el.textContent = text;
  el.className = 'badge badge-' + color;
}

function setMetric(id, val) {
  const el = document.getElementById(id);
  if (el) el.textContent = val;
}

function pct(a, b) {
  if (!b) return '0%';
  return Math.round(a / b * 100) + '%';
}

async function checkApi() {
  try {
    const r = await fetch(API + '/health', { signal: AbortSignal.timeout(5000) });
    if (r.ok) {
      document.getElementById('api-dot').className = 'dot dot-green';
      document.getElementById('api-status').textContent = 'API online';
      setBadge('s-api', 'Online', 'green');
      return true;
    }
    throw new Error('HTTP ' + r.status);
  } catch(e) {
    document.getElementById('api-dot').className = 'dot dot-red';
    document.getElementById('api-status').textContent = 'API offline';
    setBadge('s-api', 'Offline', 'red');
    return false;
  }
}

function renderAlerts(d) {
  const alerts = [];

  if (d.pendingPartials > 0) {
    alerts.push({ type: 'warn', icon: '⚠', msg: d.pendingPartials + ' lead(s) waiting >30 mins — Loops/Slack not yet sent. Cron may be delayed.' });
  }
  if (d.noBookingUid > 0) {
    alerts.push({ type: 'warn', icon: '⚠', msg: d.noBookingUid + ' completed lead(s) have no booking — Cal callback may not be firing reliably.' });
  }
  if (!d.awsSynced) {
    alerts.push({ type: 'err', icon: '✕', msg: 'AWS sync is disabled — AWS_PG_HOST not set in Railway env vars.' });
  }
  if (d.enriched < d.total * 0.3 && d.total > 5) {
    alerts.push({ type: 'warn', icon: '⚠', msg: 'Enrichment rate is low (' + Math.round(d.enriched/d.total*100) + '%) — Apollo may be returning empty responses.' });
  }
  if (d.todayCount === 0) {
    alerts.push({ type: 'warn', icon: '○', msg: 'No new leads in the last 24 hours — check if form is live and accessible.' });
  }

  if (alerts.length === 0) {
    alerts.push({ type: 'ok', icon: '✓', msg: 'All systems healthy — no issues detected.' });
  }

  document.getElementById('alerts-container').innerHTML = alerts.map(a => \`
    <div class="alert-box alert-\${a.type}">
      <span class="alert-icon">\${a.icon}</span>
      <span>\${a.msg}</span>
    </div>
  \`).join('');
}

function renderFunnel(total, completed, booked, disq) {
  const steps = [
    { label: 'Step 1 submitted', val: total, pct: 100, color: '#818cf8' },
    { label: 'Step 2 completed', val: completed, pct: total ? Math.round(completed/total*100) : 0, color: '#38bdf8' },
    { label: 'Call booked',      val: booked,    pct: total ? Math.round(booked/total*100) : 0,    color: '#34d399' },
    { label: 'Disqualified',     val: disq,      pct: total ? Math.round(disq/total*100) : 0,      color: '#fb923c' },
  ];
  document.getElementById('funnel-container').innerHTML = steps.map(s => \`
    <div class="funnel-row">
      <div class="funnel-labels">
        <span>\${s.label}</span>
        <span style="font-weight:500;color:#1a1a1a;">\${s.val} <span style="color:#aaa;font-weight:400;">(\${s.pct}%)</span></span>
      </div>
      <div class="funnel-bar">
        <div class="funnel-fill" style="width:\${s.pct}%;background:\${s.color};"></div>
      </div>
    </div>
  \`).join('');
}

function renderRecentLeads(leads) {
  if (!leads || !leads.length) {
    document.getElementById('recent-leads-container').innerHTML = '<p style="color:#999;font-size:13px;">No leads found.</p>';
    return;
  }
  document.getElementById('recent-leads-container').innerHTML = \`
    <table>
      <thead>
        <tr>
          <th>Email</th>
          <th>Company</th>
          <th>Status</th>
          <th>Booked</th>
          <th>Created (IST)</th>
        </tr>
      </thead>
      <tbody>
        \${leads.map(l => {
          const d = new Date(l.created_at);
          const ist = d.toLocaleString('en-IN', { timeZone: 'Asia/Kolkata', dateStyle: 'short', timeStyle: 'short' });
          return \`<tr>
            <td class="email">\${l.email || '—'}</td>
            <td>\${l.company || '—'}</td>
            <td>\${l.completed ? '<span class="badge badge-green">Completed</span>' : '<span class="badge badge-amber">Step 1</span>'}\${l.disqualified ? ' <span class="badge badge-red">Disq</span>' : ''}</td>
            <td>\${l.booking_uid ? '<span class="badge badge-green">Yes</span>' : '<span class="badge badge-gray">No</span>'}</td>
            <td style="color:#999;">\${ist}</td>
          </tr>\`;
        }).join('')}
      </tbody>
    </table>
  \`;
}

function renderChart(leads) {
  const counts = {};
  leads.forEach(l => {
    const d = new Date(l.created_at);
    const key = d.toLocaleDateString('en-IN', { timeZone: 'Asia/Kolkata', month: 'short', day: 'numeric' });
    counts[key] = (counts[key] || 0) + 1;
  });
  const labels = Object.keys(counts);
  const data   = Object.values(counts);

  if (leadsChart) leadsChart.destroy();
  const ctx = document.getElementById('leadsChart').getContext('2d');
  leadsChart = new Chart(ctx, {
    type: 'bar',
    data: {
      labels,
      datasets: [{
        label: 'Leads',
        data,
        backgroundColor: '#818cf8',
        borderRadius: 4,
        borderSkipped: false,
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: { legend: { display: false } },
      scales: {
        y: { beginAtZero: true, ticks: { stepSize: 1, color: '#aaa' }, grid: { color: '#f0f0f0' } },
        x: { ticks: { color: '#aaa', maxRotation: 45 }, grid: { display: false } }
      }
    }
  });
}

async function load() {
  document.getElementById('last-updated').textContent = 'Refreshing...';

  const apiOk = await checkApi();
  if (!apiOk) {
    document.getElementById('alerts-container').innerHTML = \`
      <div class="alert-box alert-err"><span class="alert-icon">✕</span><span>API is offline — all form submissions are failing. Check Railway deployment immediately.</span></div>
    \`;
    return;
  }

  try {
    const r = await fetch(API + '/monitor/metrics' + TOKEN_PARAM, { signal: AbortSignal.timeout(10000) });
    if (!r.ok) throw new Error('HTTP ' + r.status);
    const d = await r.json();

    setMetric('m-total',     d.total);
    setMetric('m-completed', d.completed);
    setMetric('m-booked',    d.booked);
    setMetric('m-disq',      d.disqualified);
    setMetric('m-today',     d.todayCount + ' today');
    setMetric('m-completed-pct', pct(d.completed, d.total) + ' of leads');
    setMetric('m-booked-pct',    pct(d.booked, d.completed) + ' of completed');

    const enrichRate = d.total ? Math.round(d.enriched / d.total * 100) : 0;
    const bookingRate = d.completed ? Math.round(d.booked / d.completed * 100) : 0;

    setBadge('s-partial', d.total + ' leads saved', 'green');
    setBadge('s-submit',  d.completed > 0 ? d.completed + ' completed' : 'No completions yet', d.completed > 0 ? 'green' : 'amber');
    setBadge('s-enrich',  enrichRate + '% enriched', enrichRate >= 60 ? 'green' : enrichRate >= 30 ? 'amber' : 'red');
    setBadge('s-cal',     bookingRate + '% booking rate', bookingRate >= 50 ? 'green' : bookingRate >= 20 ? 'amber' : 'gray');
    setBadge('s-cron',    d.pendingPartials === 0 ? 'No pending' : d.pendingPartials + ' pending', d.pendingPartials === 0 ? 'green' : 'amber');
    setBadge('s-aws',     d.awsSynced ? 'Active' : 'Disabled', d.awsSynced ? 'green' : 'red');

    renderAlerts(d);
    renderFunnel(d.total, d.completed, d.booked, d.disqualified);
    renderRecentLeads(d.recentLeads);
    if (d.recentLeads && d.recentLeads.length) renderChart(d.recentLeads);

    document.getElementById('last-updated').textContent = 'Updated ' + new Date().toLocaleTimeString('en-IN', { timeZone: 'Asia/Kolkata' }) + ' IST';
  } catch(e) {
    document.getElementById('alerts-container').innerHTML = \`
      <div class="alert-box alert-err"><span class="alert-icon">✕</span><span>Failed to load metrics: \${e.message}</span></div>
    \`;
    document.getElementById('last-updated').textContent = 'Error loading';
  }
}

load();
setInterval(load, 60000);
</script>
</body>
</html>`);
});

/* --------------------------------------------------------
   POST /verify-email
-------------------------------------------------------- */
app.post('/verify-email', async (req, res) => {
  const email = (req.body.email || '').toString().trim().slice(0, 254).toLowerCase();
  if (!email) return res.status(400).json({ valid: false, error: 'email required' });
  const apiKey = process.env.ELV_API_KEY;
  if (!apiKey) {
    console.warn('[ELV] ELV_API_KEY not set — skipping, allowing through');
    return res.json({ valid: true, status: 'skipped' });
  }
  try {
    const controller = new AbortController();
    const timeout    = setTimeout(() => controller.abort(), 8000);
    const url        = `https://apps.emaillistverify.com/api/verifyEmail?secret=${apiKey}&email=${encodeURIComponent(email)}`;
    const response   = await fetch(url, { signal: controller.signal });
    clearTimeout(timeout);
    const text   = await response.text();
    const status = text.trim().toLowerCase();
    console.log(`[ELV] ${email} → "${status}"`);
    const allowedStatuses = ['ok', 'catch_all', 'ok_for_all', 'antispam_system', 'accept_all'];
    const valid = allowedStatuses.includes(status);
    if (!valid) console.log(`[ELV] BLOCKED ${email} — status: "${status}"`);
    res.json({ valid, status });
  } catch (err) {
    if (err.name === 'AbortError') {
      console.warn(`[ELV] Timeout for ${email} — failing open`);
    } else {
      console.warn('[ELV] Error:', err.message, '— failing open');
    }
    res.json({ valid: true, status: 'error_fallback' });
  }
});

/* --------------------------------------------------------
   POST /session
-------------------------------------------------------- */
app.post('/session', async (req, res) => {
  const session_id = (req.body.session_id || '').toString().trim().slice(0, 100);
  if (!session_id) return res.status(400).json({ error: 'session_id required' });
  res.json({ ok: true });
});

/* --------------------------------------------------------
   POST /enrich
-------------------------------------------------------- */
app.post('/enrich', async (req, res) => {
  const email      = (req.body.email      || '').toString().trim().slice(0, 254).toLowerCase();
  const session_id = (req.body.session_id || '').toString().trim().slice(0, 100);
  if (!email || !session_id) return res.status(400).json({ error: 'email and session_id required' });

  const personalDomains = [
    'gmail.com','yahoo.com','hotmail.com','outlook.com','icloud.com',
    'protonmail.com','aol.com','mail.com','yahoo.in','rediffmail.com',
    'ymail.com','live.com','msn.com','me.com','mac.com','googlemail.com'
  ];
  const domain = email.split('@')[1]?.toLowerCase() || '';
  if (personalDomains.includes(domain)) {
    console.log(`[/enrich] Skipping Apollo for personal email: ${email}`);
    return res.json({ first_name: '', last_name: '', title: '', company: '', company_size: '', industry: '', linkedin_url: '', website: '' });
  }

  try {
    const apolloRes = await fetch('https://api.apollo.io/api/v1/people/match', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Cache-Control': 'no-cache', 'X-Api-Key': process.env.APOLLO_API_KEY },
      body: JSON.stringify({ email, reveal_personal_emails: false, reveal_phone_number: false })
    });

    const apolloData = await apolloRes.json();
    const person     = apolloData.person || {};
    const org        = person.organization || {};

    const city    = person.city    || null;
    const state   = person.state   || null;
    const country = person.country || null;

    const orgCity    = org.city    || null;
    const orgState   = org.state   || null;
    const orgCountry = org.country || null;
    const orgHQ      = [orgCity, orgState, orgCountry].filter(Boolean).join(', ') || null;

    const seniority  = person.seniority || null;
    const deptRaw    = person.departments || person.person_departments || null;
    const departments = Array.isArray(deptRaw) && deptRaw.length > 0 ? deptRaw.join(', ') : null;
    const emailStatus = person.email_status || null;
    const foundedYear = org.founded_year?.toString() || null;

    const annualRevenue = org.annual_revenue_printed
      ? `$${org.annual_revenue_printed} USD`
      : (org.annual_revenue ? formatRevenue(org.annual_revenue) : null);

    const totalFunding = org.total_funding_printed ? `$${org.total_funding_printed}` : null;
    const fundingStage = org.latest_funding_stage || null;

    const fundingEvents = Array.isArray(org.funding_events) && org.funding_events.length > 0
      ? org.funding_events.map(f =>
          [f.date ? f.date.substring(0, 10) : '', f.type || f.series || '', f.amount ? `${f.currency || '$'}${f.amount}` : ''].filter(Boolean).join(' ')
        ).join(' | ')
      : null;

    const alexaRanking = org.alexa_ranking?.toString() || null;
    const keywords = Array.isArray(org.keywords) ? org.keywords.slice(0, 8).join(', ') : (org.keywords || null);

    console.log(`[/enrich] Apollo — seniority: ${seniority} | dept: ${departments} | revenue: ${annualRevenue} | funding: ${totalFunding} (${fundingStage}) | location: ${city || country || 'n/a'} | org HQ: ${orgHQ}`);

    await pool.query(`
      INSERT INTO enrichment_data
        (session_id, email,
         enriched_first_name, enriched_last_name,
         enriched_title, enriched_company, enriched_company_size,
         enriched_industry, enriched_linkedin,
         enriched_city, enriched_state, enriched_country,
         enriched_seniority, enriched_departments, enriched_email_status,
         enriched_founded_year, enriched_annual_revenue,
         enriched_funding_events, enriched_alexa_ranking, enriched_keywords,
         enriched_org_hq, enriched_total_funding, enriched_funding_stage,
         raw_response)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24)
      ON CONFLICT (session_id) DO UPDATE SET
        email                   = EXCLUDED.email,
        enriched_first_name     = EXCLUDED.enriched_first_name,
        enriched_last_name      = EXCLUDED.enriched_last_name,
        enriched_title          = EXCLUDED.enriched_title,
        enriched_company        = EXCLUDED.enriched_company,
        enriched_company_size   = EXCLUDED.enriched_company_size,
        enriched_industry       = EXCLUDED.enriched_industry,
        enriched_linkedin       = EXCLUDED.enriched_linkedin,
        enriched_city           = EXCLUDED.enriched_city,
        enriched_state          = EXCLUDED.enriched_state,
        enriched_country        = EXCLUDED.enriched_country,
        enriched_seniority      = EXCLUDED.enriched_seniority,
        enriched_departments    = EXCLUDED.enriched_departments,
        enriched_email_status   = EXCLUDED.enriched_email_status,
        enriched_founded_year   = EXCLUDED.enriched_founded_year,
        enriched_annual_revenue = EXCLUDED.enriched_annual_revenue,
        enriched_funding_events = EXCLUDED.enriched_funding_events,
        enriched_alexa_ranking  = EXCLUDED.enriched_alexa_ranking,
        enriched_keywords       = EXCLUDED.enriched_keywords,
        enriched_org_hq         = EXCLUDED.enriched_org_hq,
        enriched_total_funding  = EXCLUDED.enriched_total_funding,
        enriched_funding_stage  = EXCLUDED.enriched_funding_stage,
        raw_response            = EXCLUDED.raw_response,
        enriched_at             = NOW()
    `, [
      session_id, email,
      person.first_name || null, person.last_name || null,
      person.title || null, org.name || null,
      org.estimated_num_employees?.toString() || null,
      org.industry || null, person.linkedin_url || null,
      city, state, country,
      seniority, departments, emailStatus,
      foundedYear, annualRevenue,
      fundingEvents, alexaRanking, keywords,
      orgHQ, totalFunding, fundingStage,
      apolloData
    ]);

    await pool.query(`
      UPDATE leads SET
        enriched_city           = $2,
        enriched_state          = $3,
        enriched_country        = $4,
        enriched_seniority      = $5,
        enriched_departments    = $6,
        enriched_email_status   = $7,
        enriched_founded_year   = $8,
        enriched_annual_revenue = $9,
        enriched_funding_events = $10,
        enriched_alexa_ranking  = $11,
        enriched_keywords       = $12,
        enriched_org_hq         = $13,
        enriched_total_funding  = $14,
        enriched_funding_stage  = $15,
        updated_at              = NOW()
      WHERE session_id = $1
    `, [session_id, city, state, country, seniority, departments, emailStatus, foundedYear, annualRevenue, fundingEvents, alexaRanking, keywords, orgHQ, totalFunding, fundingStage]);

    res.json({
      first_name:   person.first_name || '',
      last_name:    person.last_name  || '',
      title:        person.title      || '',
      company:      org.name          || '',
      company_size: org.estimated_num_employees?.toString() || '',
      industry:     org.industry      || '',
      linkedin_url: person.linkedin_url || '',
      website:      org.website_url   || ''
    });

  } catch (err) {
    console.error('[/enrich] Error:', err.message, err.detail || '');
    res.json({ first_name: '', last_name: '', title: '', company: '', company_size: '', industry: '', linkedin_url: '', website: '' });
  }
});

/* --------------------------------------------------------
   POST /partial
-------------------------------------------------------- */
app.post('/partial', async (req, res) => {
  const session_id            = (req.body.session_id          || '').toString().trim().slice(0, 100);
  const page_url              = (req.body.page_url            || '').toString().trim().slice(0, 500);
  const email                 = (req.body.email               || '').toString().trim().slice(0, 254).toLowerCase();
  const website               = (req.body.website             || '').toString().trim().slice(0, 500);
  const sell_to               = (req.body.sell_to             || '').toString().trim().slice(0, 50);
  const first_name            = (req.body.first_name          || '').toString().trim().slice(0, 100);
  const last_name             = (req.body.last_name           || '').toString().trim().slice(0, 100);
  const phone                 = (req.body.phone               || '').toString().trim().slice(0, 30);
  const company               = (req.body.company             || '').toString().trim().slice(0, 200);
  const hear_about_us         = (req.body.hear_about_us       || '').toString().trim().slice(0, 200);
  const utm_source            = (req.body.utm_source          || '').toString().trim().slice(0, 100);
  const utm_medium            = (req.body.utm_medium          || '').toString().trim().slice(0, 100);
  const utm_campaign          = (req.body.utm_campaign        || '').toString().trim().slice(0, 100);
  const utm_content           = (req.body.utm_content         || '').toString().trim().slice(0, 100);
  const referrer              = (req.body.referrer            || '').toString().trim().slice(0, 500);
  const prefill_source        = (req.body.prefill_source      || '').toString().trim().slice(0, 100);
  const enriched_title        = (req.body.enriched_title      || '').toString().trim().slice(0, 200);
  const enriched_company_size = (req.body.enriched_company_size || '').toString().trim().slice(0, 50);
  const enriched_industry     = (req.body.enriched_industry   || '').toString().trim().slice(0, 200);
  const enriched_linkedin     = (req.body.enriched_linkedin   || '').toString().trim().slice(0, 500);
  const disqualified          = Boolean(req.body.disqualified);
  const disqualified_reason   = (req.body.disqualified_reason || '').toString().trim().slice(0, 100);
  const step_reached          = parseInt(req.body.step_reached) || 1;

  if (!session_id) return res.status(400).json({ error: 'session_id required' });

  try {
    await pool.query(`
      INSERT INTO leads
        (session_id, page_url,
         email, website, sell_to,
         first_name, last_name, phone, company, hear_about_us,
         utm_source, utm_medium, utm_campaign, utm_content,
         referrer, prefill_source,
         enriched_title, enriched_company_size, enriched_industry, enriched_linkedin,
         disqualified, disqualified_reason,
         step_reached, completed, updated_at)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,false,NOW())
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
        referrer              = COALESCE(EXCLUDED.referrer,              leads.referrer),
        prefill_source        = COALESCE(EXCLUDED.prefill_source,        leads.prefill_source),
        enriched_title        = COALESCE(EXCLUDED.enriched_title,        leads.enriched_title),
        enriched_company_size = COALESCE(EXCLUDED.enriched_company_size, leads.enriched_company_size),
        enriched_industry     = COALESCE(EXCLUDED.enriched_industry,     leads.enriched_industry),
        enriched_linkedin     = COALESCE(EXCLUDED.enriched_linkedin,     leads.enriched_linkedin),
        disqualified          = COALESCE(EXCLUDED.disqualified,          leads.disqualified),
        disqualified_reason   = COALESCE(EXCLUDED.disqualified_reason,   leads.disqualified_reason),
        step_reached          = GREATEST(EXCLUDED.step_reached,          leads.step_reached),
        updated_at            = NOW()
    `, [
      session_id,       page_url              || null,
      email    || null, website               || null,
      sell_to  || null, first_name            || null,
      last_name|| null, phone                 || null,
      company  || null, hear_about_us         || null,
      utm_source||null, utm_medium            || null,
      utm_campaign||null, utm_content         || null,
      referrer ||null,  prefill_source        || null,
      enriched_title||null, enriched_company_size||null,
      enriched_industry||null, enriched_linkedin||null,
      disqualified, disqualified_reason||null, step_reached
    ]);

    syncToAWS({
      session_id, page_url, email, website, sell_to,
      first_name, last_name, phone, company, hear_about_us,
      utm_source, utm_medium, utm_campaign, utm_content,
      referrer, prefill_source,
      enriched_title, enriched_company_size, enriched_industry, enriched_linkedin,
      disqualified, disqualified_reason, step_reached, completed: false
    });

    console.log(`[/partial] ✅ Saved session ${session_id} | step ${step_reached} | email ${email}`);
    res.json({ ok: true });

  } catch (err) {
    console.error('[/partial]', err.message);
    res.status(500).json({ error: 'Partial save failed' });
  }
});

/* --------------------------------------------------------
   POST /submit
-------------------------------------------------------- */
app.post('/submit', async (req, res) => {
  const session_id            = (req.body.session_id          || '').toString().trim().slice(0, 100);
  const page_url              = (req.body.page_url            || '').toString().trim().slice(0, 500);
  const email                 = (req.body.email               || '').toString().trim().slice(0, 254).toLowerCase();
  const website               = (req.body.website             || '').toString().trim().slice(0, 500);
  const sell_to               = (req.body.sell_to             || '').toString().trim().slice(0, 50);
  const first_name            = (req.body.first_name          || '').toString().trim().slice(0, 100);
  const last_name             = (req.body.last_name           || '').toString().trim().slice(0, 100);
  const phone                 = (req.body.phone               || '').toString().trim().slice(0, 30);
  const company               = (req.body.company             || '').toString().trim().slice(0, 200);
  const hear_about_us         = (req.body.hear_about_us       || '').toString().trim().slice(0, 200);
  const utm_source            = (req.body.utm_source          || '').toString().trim().slice(0, 100);
  const utm_medium            = (req.body.utm_medium          || '').toString().trim().slice(0, 100);
  const utm_campaign          = (req.body.utm_campaign        || '').toString().trim().slice(0, 100);
  const utm_content           = (req.body.utm_content         || '').toString().trim().slice(0, 100);
  const referrer              = (req.body.referrer            || '').toString().trim().slice(0, 500);
  const prefill_source        = (req.body.prefill_source      || '').toString().trim().slice(0, 100);
  const enriched_title        = (req.body.enriched_title      || '').toString().trim().slice(0, 200);
  const enriched_company_size = (req.body.enriched_company_size || '').toString().trim().slice(0, 50);
  const enriched_industry     = (req.body.enriched_industry   || '').toString().trim().slice(0, 200);
  const enriched_linkedin     = (req.body.enriched_linkedin   || '').toString().trim().slice(0, 500);
  const disqualified          = Boolean(req.body.disqualified);
  const disqualified_reason   = (req.body.disqualified_reason || '').toString().trim().slice(0, 100);

  if (!session_id) return res.status(400).json({ error: 'session_id required' });

  try {
    const existing = await pool.query(
      `SELECT completed FROM leads WHERE session_id = $1`, [session_id]
    );
    const alreadyCompleted = existing.rows[0]?.completed === true;

    const enrichRow = await pool.query(
      `SELECT * FROM enrichment_data WHERE session_id = $1`, [session_id]
    );
    const enrich = enrichRow.rows[0] || {};

    await pool.query(`
      INSERT INTO leads
        (session_id, page_url,
         email, website, sell_to,
         first_name, last_name, phone, company, hear_about_us,
         utm_source, utm_medium, utm_campaign, utm_content,
         referrer, prefill_source,
         enriched_title, enriched_company_size, enriched_industry, enriched_linkedin,
         disqualified, disqualified_reason,
         step_reached, completed, submitted_at, updated_at)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,2,true,NOW(),NOW())
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
        referrer              = COALESCE(EXCLUDED.referrer,              leads.referrer),
        prefill_source        = COALESCE(EXCLUDED.prefill_source,        leads.prefill_source),
        enriched_title        = COALESCE(EXCLUDED.enriched_title,        leads.enriched_title),
        enriched_company_size = COALESCE(EXCLUDED.enriched_company_size, leads.enriched_company_size),
        enriched_industry     = COALESCE(EXCLUDED.enriched_industry,     leads.enriched_industry),
        enriched_linkedin     = COALESCE(EXCLUDED.enriched_linkedin,     leads.enriched_linkedin),
        disqualified          = COALESCE(EXCLUDED.disqualified,          leads.disqualified),
        disqualified_reason   = COALESCE(EXCLUDED.disqualified_reason,   leads.disqualified_reason),
        step_reached          = 2,
        completed             = true,
        submitted_at          = NOW(),
        updated_at            = NOW()
    `, [
      session_id,       page_url         || null,
      email    || null, website          || null,
      sell_to  || null, first_name       || null,
      last_name|| null, phone            || null,
      company  || null, hear_about_us    || null,
      utm_source||null, utm_medium       || null,
      utm_campaign||null, utm_content    || null,
      referrer ||null,  prefill_source   || null,
      enriched_title||null, enriched_company_size||null,
      enriched_industry||null, enriched_linkedin||null,
      disqualified, disqualified_reason  || null
    ]);

    syncToAWS({
      session_id, page_url, email, website, sell_to,
      first_name, last_name, phone, company, hear_about_us,
      utm_source, utm_medium, utm_campaign, utm_content,
      referrer, prefill_source,
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
      enriched_funding_stage:  enrich.enriched_funding_stage,
      disqualified, disqualified_reason, step_reached: 2, completed: true
    });

    if (!alreadyCompleted) {
      slackSubmit({
        first_name, last_name, email, phone, company, website,
        sell_to, hear_about_us,
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
        enriched_funding_stage:  enrich.enriched_funding_stage,
        utm_source, utm_medium, utm_campaign, utm_content,
        referrer, prefill_source, page_url
      });
      console.log(`[/submit] ✅ Lead completed: ${email} | session: ${session_id}`);
    } else {
      console.log(`[/submit] ⏭ Slack skipped — already completed: ${email} | session: ${session_id}`);
    }

    res.json({ ok: true });

  } catch (err) {
    console.error('[/submit]', err.message);
    res.status(500).json({ error: 'Submit failed' });
  }
});

/* --------------------------------------------------------
   POST /booking-confirmed
-------------------------------------------------------- */
app.post('/booking-confirmed', async (req, res) => {
  const session_id  = (req.body.session_id  || '').toString().trim().slice(0, 100);
  const booking_uid = (req.body.booking_uid || '').toString().trim().slice(0, 100);
  const start_time  = req.body.start_time || null;
  const end_time    = req.body.end_time   || null;
  const event_type  = (req.body.event_type || '').toString().trim().slice(0, 100);

  if (!session_id || !booking_uid) {
    return res.status(400).json({ error: 'session_id and booking_uid required' });
  }

  try {
    await pool.query(`
      UPDATE leads SET
        booking_uid = $2, start_time = $3, end_time = $4,
        event_type  = $5, booked_at  = NOW(), updated_at = NOW()
      WHERE session_id = $1
    `, [session_id, booking_uid, start_time, end_time, event_type || null]);

    syncBookingToAWS(session_id, booking_uid, start_time, end_time, event_type);

    const leadRow = await pool.query('SELECT email FROM leads WHERE session_id = $1', [session_id]);
    const email   = leadRow.rows[0]?.email;
    if (email) cancelLoopsSequence(email);

    console.log(`[/booking-confirmed] ✅ Booked: ${booking_uid} | session: ${session_id} | email: ${email}`);
    res.json({ ok: true });

  } catch (err) {
    console.error('[/booking-confirmed]', err.message);
    res.status(500).json({ error: 'Booking update failed' });
  }
});

/* --------------------------------------------------------
   POST /cron/send-partials
-------------------------------------------------------- */
app.post('/cron/send-partials', async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT session_id, email, first_name, last_name,
             company, website, sell_to,
             utm_source, utm_medium, utm_campaign, utm_content,
             referrer, page_url,
             disqualified, disqualified_reason, completed,
             enriched_title, enriched_company_size, enriched_industry, enriched_linkedin,
             enriched_city, enriched_state, enriched_country,
             enriched_seniority, enriched_departments, enriched_email_status,
             enriched_founded_year, enriched_annual_revenue,
             enriched_funding_events, enriched_alexa_ranking, enriched_keywords
      FROM leads
      WHERE email IS NOT NULL
        AND disqualified = false
        AND booking_uid IS NULL
        AND loops_sent = false
        AND created_at < NOW() - INTERVAL '30 minutes'
    `);

    const leads = result.rows;
    console.log(`[Cron] Found ${leads.length} leads to process`);

    for (const lead of leads) {
      const enrichRow = await pool.query(
        'SELECT * FROM enrichment_data WHERE session_id = $1', [lead.session_id]
      );
      const enrich = enrichRow.rows[0] || {};

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
        enriched_funding_stage:  enrich.enriched_funding_stage,
      });

      await sendLoopsEvent(lead.email, lead.first_name, lead.last_name, lead.company, lead.website);

      await pool.query('UPDATE leads SET loops_sent = true WHERE session_id = $1', [lead.session_id]);

      if (awsPool) {
        awsPool.query(
          'UPDATE gw_form_leads SET loops_sent = true, updated_at = NOW() WHERE session_id = $1',
          [lead.session_id]
        ).catch(err => console.warn('[AWS] ⚠ loops_sent sync failed:', err.message));
      }

      console.log(`[Cron] ✅ Processed partial for ${lead.email} | completed: ${lead.completed}`);
    }

    res.json({ ok: true, processed: leads.length });
  } catch (err) {
    console.error('[Cron] Error:', err.message);
    res.status(500).json({ error: 'Cron failed' });
  }
});

/* --------------------------------------------------------
   START
-------------------------------------------------------- */
async function start() {
  try {
    await initDB();
    await initAWSTable();
    app.listen(PORT, () => {
      console.log(`[GW API] Running on port ${PORT}`);
      console.log(`[GW API] Allowed origins: ${allowedOrigins.join(', ')}`);
    });
  } catch (err) {
    console.error('[GW API] Failed to start:', err);
    process.exit(1);
  }
}

start();
