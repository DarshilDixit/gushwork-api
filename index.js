require('dotenv').config();
const crypto    = require('crypto');
const express   = require('express');
const cors      = require('cors');
const helmet    = require('helmet');
const rateLimit = require('express-rate-limit');
const { Pool }  = require('pg');
const { pool, initDB } = require('./db');
const { pushToSalesforce, findSFLeadByEmail, updateSFLead } = require('./salesforce');
const { pushFormEventsToMeta, pushStartTrialToMeta } = require('./meta-capi');

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
  windowMs: 60 * 60 * 1000, max: 50,
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
    console.warn(`[AWS] ⚠ Sync failed for ${data.session_id}:`, err.message);
  });
}

function syncBookingToAWS(session_id, booking_uid, start_time, end_time, event_type) {
  if (!awsPool) return;
  awsPool.query(`
    UPDATE gw_form_leads SET booking_uid=$2, start_time=$3, end_time=$4, event_type=$5, booked_at=NOW(), completed=true, updated_at=NOW()
    WHERE session_id = $1
  `, [session_id, booking_uid, start_time || null, end_time || null, event_type || null])
  .then(() => console.log(`[AWS] ✅ Booking synced for session ${session_id}`))
  .catch(err => console.warn(`[AWS] ⚠ Booking sync failed:`, err.message));
}

/* --------------------------------------------------------
   SLACK HELPERS
-------------------------------------------------------- */
function sendSlack(blocks, fallbackText) {
  const webhookUrl = process.env.SLACK_WEBHOOK_URL;
  if (!webhookUrl) { console.warn('[Slack] SLACK_WEBHOOK_URL not set — skipping'); return; }
  const cleanBlocks = Array.isArray(blocks) ? blocks.filter(Boolean) : null;
  const payload = cleanBlocks && cleanBlocks.length > 0
    ? { text: fallbackText || 'Gushwork notification', blocks: cleanBlocks }
    : { text: fallbackText || 'Gushwork notification' };
  fetch(webhookUrl, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload) })
  .then(r => r.text().then(t => console.log(`[Slack] ✅ Sent — status: ${r.status} | response: ${t.substring(0, 50)}`)))
  .catch(err => console.warn('[Slack] ⚠ Failed:', err.message));
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

/* --------------------------------------------------------
   slackPartial — safety net: never fire for disqualified leads
-------------------------------------------------------- */
function slackPartial(d) {
  if (d.disqualified) {
    console.log(`[Slack] ⏭ Skipping partial notification for disqualified lead: ${d.email}`);
    return;
  }
  const label = d.completed ? '⏰ Reached Cal — Did Not Book' : '👻 Dropped at Step 1';
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
  blocks.push(bHeader('✅ Lead Form Completed'));
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
  buildEnrichmentBlocks(blocks, d);
  buildJourneyBlocks(blocks, d);
  sendSlack(blocks, `✅ Lead Form Completed — ${d.email}`);
}

/* --------------------------------------------------------
   FOLLOW-UP EMAIL (Gmail SMTP)
-------------------------------------------------------- */
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

/* --------------------------------------------------------
   GET /monitor/metrics  — protected by token
-------------------------------------------------------- */
app.get('/monitor/metrics', async (req, res) => {
  const token = process.env.MONITOR_TOKEN;
  if (token && req.query.token !== token) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  try {
    const [totals, enrichCount, enrichCoverage, pendingPartials, noBooking, recent, today] = await Promise.all([
      pool.query(`
        SELECT
          COUNT(*)                                          AS total,
          COUNT(*) FILTER (WHERE completed = true)          AS completed,
          COUNT(*) FILTER (WHERE booking_uid IS NOT NULL)   AS booked,
          COUNT(*) FILTER (WHERE disqualified = true)       AS disqualified,
          COUNT(*) FILTER (WHERE loops_sent = true)         AS loops_sent
        FROM leads
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
      // ── UPDATED: pendingPartials now uses 2-hour threshold + cross-session booking check
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
      recentLeads: recent.rows, generatedAt: new Date().toISOString()
    });
  } catch (err) {
    console.error('[/monitor/metrics]', err.message);
    res.status(500).json({ error: 'Metrics query failed', detail: err.message });
  }
});

/* --------------------------------------------------------
   GET /monitor/duplicates  — emails with multiple sessions
-------------------------------------------------------- */
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

/* --------------------------------------------------------
   GET /monitor/leads  — paginated leads + enrichment
   UPDATED: added Sell-to / Source (utm_source) / Enrichment /
            Heard-about-us filters, sortable columns, and CSV
            export (?format=csv). Form flow untouched.
-------------------------------------------------------- */
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
  const enrichment = req.query.enrichment || null;   // 'yes' | 'no'
  const format     = req.query.format     || 'json'; // 'json' | 'csv'

  // Sort whitelist — only known columns/directions are ever interpolated.
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

  if (sellTo)    { params.push(sellTo);    conditions.push(`l.sell_to = $${params.length}`); }
  if (utmSource) { params.push(utmSource); conditions.push(`l.utm_source = $${params.length}`); }
  if (hearAbout) { params.push(`%${hearAbout.toLowerCase()}%`); conditions.push(`LOWER(COALESCE(l.hear_about_us,'')) LIKE $${params.length}`); }

  // Enrichment yes/no mirrors the dashboard's "Enrichment" badge logic.
  if (enrichment === 'yes') {
    conditions.push(`(l.enriched_title IS NOT NULL OR l.enriched_company_size IS NOT NULL OR EXISTS (SELECT 1 FROM enrichment_data ee WHERE ee.session_id = l.session_id AND (ee.enriched_title IS NOT NULL OR ee.enriched_company_size IS NOT NULL OR ee.enriched_company IS NOT NULL)))`);
  }
  if (enrichment === 'no') {
    conditions.push(`(l.enriched_title IS NULL AND l.enriched_company_size IS NULL AND NOT EXISTS (SELECT 1 FROM enrichment_data ee WHERE ee.session_id = l.session_id AND (ee.enriched_title IS NOT NULL OR ee.enriched_company_size IS NOT NULL OR ee.enriched_company IS NOT NULL)))`);
  }

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
      l.landing_page, l.previous_page,
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
    // ── CSV export: same filters + sort, no pagination
    if (format === 'csv') {
      const allRows = await pool.query(baseSelect + ` ${orderBy}`, params);
      const cols = [
        'email','first_name','last_name','company','website','phone','sell_to','hear_about_us',
        'completed','booking_uid','disqualified','step_reached','created_at','submitted_at','booked_at',
        'utm_source','utm_medium','utm_campaign','utm_term','referrer','prefill_source',
        'landing_page','previous_page','page_url',
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

/* --------------------------------------------------------
   GET /monitor/filter-options  — distinct values for filters
   Powers the Source dropdown + Heard-about-us autocomplete.
-------------------------------------------------------- */
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

/* --------------------------------------------------------
   GET /monitor/sdr  — email-deduped unbooked qualified leads
   Includes: B2B leads who never booked on ANY session
   - completed form (step 2) OR dropped at step 1 but qualified (sell_to = B2B)
   - deduped by email — one row per person, most recent session wins
   - CSV export supported via ?format=csv
-------------------------------------------------------- */
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

/* --------------------------------------------------------
   GET /monitor  — full dashboard HTML page
   UPDATED: alert threshold changed from 30 mins to 2 hours
   UPDATED: All Leads tab now has Sell-to / Source / Enrichment /
            Heard-about-us filters, date presets, sortable columns,
            and CSV export.
-------------------------------------------------------- */
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
  '.g4{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:24px}' +
  '.g2{display:grid;grid-template-columns:repeat(2,1fr);gap:12px;margin-bottom:24px}' +
  '.card{background:#fff;border:1px solid #e5e5e5;border-radius:10px;padding:16px 20px}' +
  '.mc{background:#fff;border:1px solid #e5e5e5;border-radius:10px;padding:16px}' +
  '.ml{font-size:12px;color:#888;margin-bottom:6px}' +
  '.mv{font-size:28px;font-weight:600;color:#1a1a1a;line-height:1}' +
  '.ms{font-size:11px;color:#aaa;margin-top:6px}' +
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
  '<div class="tab" id="t-health" onclick="showTab(\'health\')">System Health</div>' +
  '</div>' +
  '<div class="tp act" id="tp-overview">' +
  '<div class="sl">Overview</div>' +
  '<div class="g4">' +
  '<div class="mc"><div class="ml">Total leads</div><div class="mv" id="m-total">&#8212;</div><div class="ms" id="m-today">&#8212; today</div></div>' +
  '<div class="mc"><div class="ml">Step 2 completed</div><div class="mv" id="m-comp">&#8212;</div><div class="ms" id="m-cpct">of leads</div></div>' +
  '<div class="mc"><div class="ml">Calls booked</div><div class="mv" id="m-book">&#8212;</div><div class="ms" id="m-bpct">of completed</div></div>' +
  '<div class="mc"><div class="ml">Disqualified</div><div class="mv" id="m-disq">&#8212;</div><div class="ms">B2C / Mixed</div></div>' +
  '</div>' +
  '<div class="g2">' +
  '<div><div class="sl">Alerts</div><div id="alerts"><div class="alertbox" style="background:#f5f5f5;color:#999;border:1px solid #eee">Loading...</div></div></div>' +
  '<div><div class="sl">Conversion funnel</div><div class="card"><div id="funnel">Loading...</div></div></div>' +
  '</div>' +
  '<div class="sl">Leads over time</div>' +
  '<div class="card" style="margin-bottom:24px"><div class="cw"><canvas id="lchart"></canvas></div></div>' +
  '</div>' +
  '<div class="tp" id="tp-leads">' +
  '<div class="filters">' +
  '<input type="text" id="fsearch" placeholder="Search email, company..." oninput="debounce()">' +
  '<select id="fstage" onchange="loadLeads(1)"><option value="all">All stages</option><option value="booked">Booked</option><option value="completed">Completed (no booking)</option><option value="step1">Step 1 only</option><option value="disqualified">Disqualified</option></select>' +
  '<select id="fsellto" onchange="loadLeads(1)"><option value="all">All sell-to</option><option value="B2B">B2B</option><option value="B2B (clarified from B2C)">B2B (clarified from B2C)</option><option value="B2B (clarified from Mixed)">B2B (clarified from Mixed)</option><option value="B2C">B2C</option></select>' +
  '<select id="fsource" onchange="loadLeads(1)"><option value="all">All sources</option></select>' +
  '<select id="fenrich" onchange="loadLeads(1)"><option value="all">Enrichment: all</option><option value="yes">Enriched</option><option value="no">Not enriched</option></select>' +
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
  '<div><div class="sl" style="margin-bottom:2px">SDR List</div><div style="font-size:12px;color:#888">Qualified B2B leads who have never booked a call — deduped by email</div></div>' +
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
  '<div><div class="sl" style="margin-bottom:2px">Duplicate Sessions</div><div style="font-size:12px;color:#888">Emails that appear in more than one session — sorted by session count</div></div>' +
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
  '<div class="sr"><div><div class="sn">Apollo enrichment</div><div class="sd">enrichment_data populated per session</div></div><span class="badge bx" id="s-enrich">Checking...</span></div>' +
  '<div class="sr"><div><div class="sn">Cal booking</div><div class="sd">Completed leads with booking_uid</div></div><span class="badge bx" id="s-cal">Checking...</span></div>' +
  '<div class="sr"><div><div class="sn">Cron &#8212; drop-off recovery</div><div class="sd">Leads waiting >2 hours without booking</div></div><span class="badge bx" id="s-cron">Checking...</span></div>' +
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
  '</div>';

  const js = '<script>' +
  'var TP="' + tp + '";' +
  'var API=window.location.origin;' +
  'var lChart=null,curPage=1,stimer=null,curSort="created_at",curDir="desc",filterOptsLoaded=false;' +
  'function showTab(n){["overview","leads","sdr","dupes","health"].forEach(function(x){document.getElementById("t-"+x).classList.toggle("act",x===n);document.getElementById("tp-"+x).classList.toggle("act",x===n);});if(n==="leads"){loadFilterOptions();if(document.getElementById("ltbody").textContent.indexOf("Loading")>=0)loadLeads(1);}if(n==="sdr"&&document.getElementById("sdr-tbody").textContent.indexOf("Loading")>=0)loadSDR();if(n==="dupes"&&document.getElementById("dupes-tbody").textContent.indexOf("Loading")>=0)loadDupes();}' +
  'function badge(id,text,cls){var el=document.getElementById(id);if(!el)return;el.textContent=text;el.className="badge "+cls;}' +
  'function set(id,v){var el=document.getElementById(id);if(el)el.textContent=v;}' +
  'function pct(a,b){return b?Math.round(a/b*100)+"%":"0%";}' +
  'function ist(ts){if(!ts)return"\\u2014";return new Date(ts).toLocaleString("en-IN",{timeZone:"Asia/Kolkata",dateStyle:"short",timeStyle:"short"});}' +
  'function esc(s){if(!s)return"";return String(s).replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;").replace(/"/g,"&quot;");}' +
  'async function checkApi(){try{var r=await fetch(API+"/health",{signal:AbortSignal.timeout(5000)});if(r.ok){document.getElementById("apidot").className="dot dot-green";document.getElementById("apist").textContent="API online";badge("s-api","Online","bg");return true;}throw new Error("HTTP "+r.status);}catch(e){document.getElementById("apidot").className="dot dot-red";document.getElementById("apist").textContent="API offline";badge("s-api","Offline","br");return false;}}' +
  'function renderAlerts(d){var a=[];if(d.pendingPartials>0)a.push({c:"aw",i:"!",m:d.pendingPartials+" lead(s) waiting >2 hours without booking."});if(d.noBookingUid>0)a.push({c:"aw",i:"!",m:d.noBookingUid+" B2B lead(s) completed form but not booked — check SDR List."});if(!d.awsSynced)a.push({c:"ae",i:"x",m:"AWS sync disabled."});if(d.total>5&&d.enriched<d.total*0.3)a.push({c:"aw",i:"!",m:"Low enrichment rate ("+Math.round(d.enriched/d.total*100)+"%)."});if(d.todayCount===0)a.push({c:"aw",i:"o",m:"No new leads in 24 hours."});if(a.length===0)a.push({c:"ao",i:"\\u2713",m:"All systems healthy."});document.getElementById("alerts").innerHTML=a.map(function(x){return"<div class=\\"alertbox "+x.c+"\\"><span>"+x.i+"</span><span>"+x.m+"</span></div>";}).join("");}' +
  'function renderFunnel(t,c,b,d){var steps=[{l:"Step 1 submitted",v:t,p:100,col:"#818cf8"},{l:"Step 2 completed",v:c,p:t?Math.round(c/t*100):0,col:"#38bdf8"},{l:"Call booked",v:b,p:t?Math.round(b/t*100):0,col:"#34d399"},{l:"Disqualified",v:d,p:t?Math.round(d/t*100):0,col:"#fb923c"}];document.getElementById("funnel").innerHTML=steps.map(function(s){return"<div class=\\"fr\\"><div class=\\"fl\\"><span>"+s.l+"</span><span style=\\"font-weight:500\\">"+s.v+" <span style=\\"color:#aaa\\">("+s.p+"%)</span></span></div><div class=\\"fb\\"><div class=\\"ff\\" style=\\"width:"+s.p+"%;background:"+s.col+"\\"></div></div></div>";}).join("");}' +
  'function renderChart(leads){var counts={};(leads||[]).forEach(function(l){var k=new Date(l.created_at).toLocaleDateString("en-IN",{timeZone:"Asia/Kolkata",month:"short",day:"numeric"});counts[k]=(counts[k]||0)+1;});var labels=Object.keys(counts).reverse(),data=Object.values(counts).reverse();if(lChart)lChart.destroy();var ctx=document.getElementById("lchart").getContext("2d");lChart=new Chart(ctx,{type:"bar",data:{labels:labels,datasets:[{data:data,backgroundColor:"#818cf8",borderRadius:4,borderSkipped:false}]},options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false}},scales:{y:{beginAtZero:true,ticks:{stepSize:1,color:"#aaa"},grid:{color:"#f0f0f0"}},x:{ticks:{color:"#aaa",maxRotation:45,autoSkip:false},grid:{display:false}}}}});}' +
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
  'function clearF(){document.getElementById("fsearch").value="";document.getElementById("fstage").value="all";document.getElementById("fsellto").value="all";document.getElementById("fsource").value="all";document.getElementById("fenrich").value="all";document.getElementById("fhear").value="";document.getElementById("fpreset").value="";document.getElementById("ffrom").value="";document.getElementById("fto").value="";curSort="created_at";curDir="desc";renderSortArrows();loadLeads(1);}' +
  'function renderSortArrows(){["email","name","company","sell_to","created_at"].forEach(function(c){var el=document.getElementById("sar-"+c);if(el)el.textContent=(curSort===c)?(curDir==="asc"?"\\u25B2":"\\u25BC"):"";});}' +
  'function sortBy(c){if(curSort===c){curDir=(curDir==="asc")?"desc":"asc";}else{curSort=c;curDir=(c==="created_at")?"desc":"asc";}renderSortArrows();loadLeads(1);}' +
  'function datePreset(v){var ff=document.getElementById("ffrom"),ft=document.getElementById("fto");if(!v){loadLeads(1);return;}function fmt(d){var y=d.getFullYear(),m=("0"+(d.getMonth()+1)).slice(-2),da=("0"+d.getDate()).slice(-2);return y+"-"+m+"-"+da;}var now=new Date(),to=fmt(now),from=to;if(v==="7d"){var d=new Date(now);d.setDate(d.getDate()-6);from=fmt(d);}else if(v==="30d"){var d2=new Date(now);d2.setDate(d2.getDate()-29);from=fmt(d2);}ff.value=from;ft.value=to;loadLeads(1);}' +
  'function dateManual(){var p=document.getElementById("fpreset");if(p)p.value="";loadLeads(1);}' +
  'function exportLeads(){var search=document.getElementById("fsearch").value.trim(),stage=document.getElementById("fstage").value,sellTo=document.getElementById("fsellto").value,source=document.getElementById("fsource").value,enrich=document.getElementById("fenrich").value,hear=document.getElementById("fhear").value.trim(),from=document.getElementById("ffrom").value,to=document.getElementById("fto").value;var url=API+"/monitor/leads"+(TP||"?")+(TP?"&":"")+"format=csv&stage="+stage+"&sort="+curSort+"&dir="+curDir;if(sellTo&&sellTo!=="all")url+="&sellTo="+encodeURIComponent(sellTo);if(source&&source!=="all")url+="&utmSource="+encodeURIComponent(source);if(enrich&&enrich!=="all")url+="&enrichment="+encodeURIComponent(enrich);if(hear)url+="&hearAbout="+encodeURIComponent(hear);if(search)url+="&search="+encodeURIComponent(search);if(from)url+="&dateFrom="+from;if(to)url+="&dateTo="+to;window.location.href=url;}' +
  'async function loadFilterOptions(){if(filterOptsLoaded)return;try{var r=await fetch(API+"/monitor/filter-options"+(TP||"?")+(TP?"&":"")+"_="+Date.now(),{signal:AbortSignal.timeout(10000)});if(!r.ok)return;var d=await r.json();var sel=document.getElementById("fsource");if(sel&&d.utmSource){d.utmSource.forEach(function(v){var o=document.createElement("option");o.value=v;o.textContent=v;sel.appendChild(o);});}var dl=document.getElementById("hearlist");if(dl&&d.hearAbout){dl.innerHTML=d.hearAbout.map(function(v){return"<option value=\\""+esc(v)+"\\"></option>";}).join("");}filterOptsLoaded=true;}catch(e){}}' +
  'function toggleRow(sid){var row=document.getElementById("er-"+sid);if(!row)return;var vis=row.style.display!=="none";row.style.display=vis?"none":"table-row";var btn=row.previousElementSibling&&row.previousElementSibling.querySelector(".xbtn");if(btn)btn.textContent=vis?"\\u25B6":"\\u25BC";}' +
  'async function loadLeads(pg){curPage=pg||1;var search=document.getElementById("fsearch").value.trim(),stage=document.getElementById("fstage").value,sellTo=document.getElementById("fsellto").value,source=document.getElementById("fsource").value,enrich=document.getElementById("fenrich").value,hear=document.getElementById("fhear").value.trim(),from=document.getElementById("ffrom").value,to=document.getElementById("fto").value;' +
  'var url=API+"/monitor/leads"+(TP||"?")+(TP?"&":"")+"page="+curPage+"&stage="+stage+"&sort="+curSort+"&dir="+curDir;' +
  'if(sellTo&&sellTo!=="all")url+="&sellTo="+encodeURIComponent(sellTo);if(source&&source!=="all")url+="&utmSource="+encodeURIComponent(source);if(enrich&&enrich!=="all")url+="&enrichment="+encodeURIComponent(enrich);if(hear)url+="&hearAbout="+encodeURIComponent(hear);if(search)url+="&search="+encodeURIComponent(search);if(from)url+="&dateFrom="+from;if(to)url+="&dateTo="+to;' +
  'document.getElementById("ltbody").innerHTML="<tr><td colspan=\\"10\\" class=\\"nd\\">Loading...</td></tr>";' +
  'try{var r=await fetch(url,{signal:AbortSignal.timeout(12000)});if(!r.ok)throw new Error("HTTP "+r.status);var d=await r.json();' +
  'set("lcount",d.total+" lead"+(d.total!==1?"s":"")+" found");' +
  'if(!d.leads.length){document.getElementById("ltbody").innerHTML="<tr><td colspan=\\"10\\" class=\\"nd\\">No leads match your filters.</td></tr>";document.getElementById("lpag").innerHTML="";return;}' +
  'var html=d.leads.map(function(l){var sid=esc(l.session_id),name=[l.first_name,l.last_name].filter(Boolean).map(esc).join(" ")||"\\u2014",src=l.utm_source?esc(l.utm_source)+(l.utm_medium?" / "+esc(l.utm_medium):""):(l.referrer?"referral":"\\u2014");' +
  'return"<tr><td class=\\"xbtn\\" onclick=\\"toggleRow(\'"+sid+"\')\\">&#9658;</td><td class=\\"te\\" title=\\""+esc(l.email)+"\\">"+esc(l.email||"\\u2014")+"</td><td>"+name+"</td><td class=\\"tc\\">"+esc(l.company||"\\u2014")+"</td><td>"+esc(l.sell_to||"\\u2014")+"</td><td>"+stageBadge(l)+"</td><td>"+(l.booking_uid?"<span class=\\"badge bg\\">Yes</span>":"<span class=\\"badge bx\\">No</span>")+"</td><td>"+enrichBadge(l)+"</td><td style=\\"color:#999;white-space:nowrap\\">"+ist(l.created_at)+"</td><td style=\\"color:#999;font-size:11px\\">"+src+"</td></tr>"+' +
  '"<tr class=\\"erow\\" id=\\"er-"+sid+"\\" style=\\"display:none\\"><td></td><td colspan=\\"9\\">"+enrichPanel(l)+"</td></tr>";}).join("");' +
  'document.getElementById("ltbody").innerHTML=html;renderPag(d.page,d.pages);}catch(e){document.getElementById("ltbody").innerHTML="<tr><td colspan=\\"10\\" class=\\"nd\\" style=\\"color:#b91c1c\\">Failed: "+esc(e.message)+"</td></tr>";}}' +
  'function renderPag(pg,pages){if(pages<=1){document.getElementById("lpag").innerHTML="";return;}var h="";h+="<button class=\\"pb\\" onclick=\\"loadLeads("+(pg-1)+")\\""+(pg<=1?" disabled":"")+">&larr;</button>";var s=Math.max(1,pg-2),e=Math.min(pages,pg+2);if(s>1)h+="<button class=\\"pb\\" onclick=\\"loadLeads(1)\\">1</button>"+(s>2?"<span class=\\"pi\\">&#8230;</span>":"");for(var i=s;i<=e;i++)h+="<button class=\\"pb"+(i===pg?" act":"")+ "\\" onclick=\\"loadLeads("+i+")\\" >"+i+"</button>";if(e<pages)h+=(e<pages-1?"<span class=\\"pi\\">&#8230;</span>":"")+"<button class=\\"pb\\" onclick=\\"loadLeads("+pages+")\\" >"+pages+"</button>";h+="<button class=\\"pb\\" onclick=\\"loadLeads("+(pg+1)+")\\"" +(pg>=pages?" disabled":"")+">&rarr;</button><span class=\\"pi\\">Page "+pg+" of "+pages+"</span>";document.getElementById("lpag").innerHTML=h;}' +
  'async function loadAll(){set("lupd","Refreshing...");var ok=await checkApi();if(!ok){document.getElementById("alerts").innerHTML="<div class=\\"alertbox ae\\"><span>x</span><span>API offline.</span></div>";set("lupd","API offline");return;}' +
  'try{var r=await fetch(API+"/monitor/metrics"+TP,{signal:AbortSignal.timeout(12000)});if(!r.ok)throw new Error("HTTP "+r.status);var d=await r.json();' +
  'set("m-total",d.total);set("m-comp",d.completed);set("m-book",d.booked);set("m-disq",d.disqualified);set("m-today",d.todayCount+" today");set("m-cpct",pct(d.completed,d.total)+" of leads");set("m-bpct",pct(d.booked,d.completed)+" of completed");' +
  'var er=d.total?Math.round(d.enriched/d.total*100):0,br=d.completed?Math.round(d.booked/d.completed*100):0;' +
  'badge("s-partial",d.total+" leads saved","bg");badge("s-submit",d.completed>0?d.completed+" completed":"No completions",d.completed>0?"bg":"ba");badge("s-enrich",er+"% enriched",er>=60?"bg":er>=30?"ba":"br");badge("s-cal",br+"% booking rate",br>=50?"bg":br>=20?"ba":"bx");badge("s-cron",d.pendingPartials===0?"No pending":d.pendingPartials+" pending",d.pendingPartials===0?"bg":"ba");badge("s-aws",d.awsSynced?"Active":"Disabled",d.awsSynced?"bg":"br");badge("s-loops",d.loopsSent+" emails sent",d.loopsSent>0?"bg":"bx");' +
  'set("h-enr",d.enriched);set("h-tit",d.enrichTitlePct!==undefined?d.enrichTitlePct+"%":"\\u2014");set("h-fun",d.enrichFundingPct!==undefined?d.enrichFundingPct+"%":"\\u2014");set("h-loc",d.enrichLocationPct!==undefined?d.enrichLocationPct+"%":"\\u2014");' +
  'renderAlerts(d);renderFunnel(d.total,d.completed,d.booked,d.disqualified);if(d.recentLeads&&d.recentLeads.length)renderChart(d.recentLeads);' +
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
  '{lb:"📞 Phone",v:l.phone},' +
  '{lb:"💬 Heard about us",v:l.hear_about_us},' +
  '{lb:"🌐 Website",v:l.website,lnk:true},' +
  '{lb:"Source",v:l.utm_source?([l.utm_source,l.utm_medium].filter(Boolean).join(" / ")):null},' +
  '{lb:"Campaign",v:l.utm_campaign},' +
  '{lb:"Referrer",v:l.referrer},' +
  '{lb:"🛬 Landing Page",v:l.landing_page,lnk:true},' +
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

/* --------------------------------------------------------
   POST /verify-email
-------------------------------------------------------- */
app.post('/verify-email', async (req, res) => {
  const email = (req.body.email || '').toString().trim().slice(0, 254).toLowerCase();
  if (!email) return res.status(400).json({ valid: false, error: 'email required' });
  const apiKey = process.env.ELV_API_KEY;
  if (!apiKey) { console.warn('[ELV] ELV_API_KEY not set — skipping, allowing through'); return res.json({ valid: true, status: 'skipped' }); }
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
    if (err.name === 'AbortError') console.warn(`[ELV] Timeout for ${email} — failing open`);
    else console.warn('[ELV] Error:', err.message, '— failing open');
    res.json({ valid: true, status: 'error_fallback' });
  }
});

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
  const personalDomains = ['gmail.com','yahoo.com','hotmail.com','outlook.com','icloud.com','protonmail.com','aol.com','mail.com','yahoo.in','rediffmail.com','ymail.com','live.com','msn.com','me.com','mac.com','googlemail.com'];
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
  } catch (err) { console.error('[/enrich] Error:', err.message, err.detail||''); res.json({ first_name:'',last_name:'',title:'',company:'',company_size:'',industry:'',linkedin_url:'',website:'' }); }
});

/* --------------------------------------------------------
   POST /partial  — with enrichment sync
-------------------------------------------------------- */
app.post('/partial', async (req, res) => {
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

  if (!session_id) return res.status(400).json({ error: 'session_id required' });

  try {
    await pool.query(`
      INSERT INTO leads (session_id,page_url,email,website,sell_to,first_name,last_name,phone,company,hear_about_us,utm_source,utm_medium,utm_campaign,utm_content,utm_term,referrer,prefill_source,fbc,fbp,landing_page,previous_page,enriched_title,enriched_company_size,enriched_industry,enriched_linkedin,disqualified,disqualified_reason,step_reached,completed,updated_at)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,false,NOW())
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
        updated_at            = NOW()
    `, [session_id,page_url||null,email||null,website||null,sell_to||null,first_name||null,last_name||null,phone||null,company||null,hear_about_us||null,utm_source||null,utm_medium||null,utm_campaign||null,utm_content||null,utm_term||null,referrer||null,prefill_source||null,fbc||null,fbp||null,landing_page||null,previous_page||null,enriched_title||null,enriched_company_size||null,enriched_industry||null,enriched_linkedin||null,disqualified,disqualified_reason||null,step_reached]);

    await pool.query(`UPDATE leads SET enriched_city=e.enriched_city,enriched_state=e.enriched_state,enriched_country=e.enriched_country,enriched_seniority=e.enriched_seniority,enriched_departments=e.enriched_departments,enriched_email_status=e.enriched_email_status,enriched_founded_year=e.enriched_founded_year,enriched_annual_revenue=e.enriched_annual_revenue,enriched_funding_events=e.enriched_funding_events,enriched_alexa_ranking=e.enriched_alexa_ranking,enriched_keywords=e.enriched_keywords,enriched_org_hq=e.enriched_org_hq,enriched_total_funding=e.enriched_total_funding,enriched_funding_stage=e.enriched_funding_stage,updated_at=NOW() FROM enrichment_data e WHERE leads.session_id=e.session_id AND leads.session_id=$1`, [session_id]).catch(err => console.warn('[/partial] Enrichment sync failed (non-blocking):', err.message));

    syncToAWS({session_id,page_url,email,website,sell_to,first_name,last_name,phone,company,hear_about_us,utm_source,utm_medium,utm_campaign,utm_content,utm_term,referrer,prefill_source,fbc,fbp,landing_page,previous_page,enriched_title,enriched_company_size,enriched_industry,enriched_linkedin,disqualified,disqualified_reason,step_reached,completed:false});

    if (!disqualified) {
      pushStartTrialToMeta({session_id,email,sell_to,page_url,fbc,fbp,landing_page}, {clientIpAddress:req.headers['x-forwarded-for']||req.ip||'',clientUserAgent:req.headers['user-agent']||''}).catch(err => console.warn('[/partial] Meta CAPI StartTrial failed (non-blocking):', err.message));
    }

    console.log(`[/partial] ✅ Saved session ${session_id} | step ${step_reached} | disqualified: ${disqualified} | email ${email}`);
    res.json({ ok: true });
  } catch (err) { console.error('[/partial]', err.message); res.status(500).json({ error: 'Partial save failed' }); }
});

/* --------------------------------------------------------
   POST /submit  — with enrichment sync
-------------------------------------------------------- */
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

  if (!session_id) return res.status(400).json({ error: 'session_id required' });

  try {
    const existing        = await pool.query('SELECT completed FROM leads WHERE session_id=$1', [session_id]);
    const alreadyCompleted = existing.rows[0]?.completed === true;
    const enrichRow       = await pool.query('SELECT * FROM enrichment_data WHERE session_id=$1', [session_id]);
    const enrich          = enrichRow.rows[0] || {};

    await pool.query(`
      INSERT INTO leads (session_id,page_url,email,website,sell_to,first_name,last_name,phone,company,hear_about_us,utm_source,utm_medium,utm_campaign,utm_content,utm_term,referrer,prefill_source,fbc,fbp,landing_page,previous_page,enriched_title,enriched_company_size,enriched_industry,enriched_linkedin,disqualified,disqualified_reason,step_reached,completed,submitted_at,updated_at)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,2,true,NOW(),NOW())
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
        updated_at            = NOW()
    `, [session_id,page_url||null,email||null,website||null,sell_to||null,first_name||null,last_name||null,phone||null,company||null,hear_about_us||null,utm_source||null,utm_medium||null,utm_campaign||null,utm_content||null,utm_term||null,referrer||null,prefill_source||null,fbc||null,fbp||null,landing_page||null,previous_page||null,enriched_title||null,enriched_company_size||null,enriched_industry||null,enriched_linkedin||null,disqualified,disqualified_reason||null]);

    await pool.query(`UPDATE leads SET enriched_city=e.enriched_city,enriched_state=e.enriched_state,enriched_country=e.enriched_country,enriched_seniority=e.enriched_seniority,enriched_departments=e.enriched_departments,enriched_email_status=e.enriched_email_status,enriched_founded_year=e.enriched_founded_year,enriched_annual_revenue=e.enriched_annual_revenue,enriched_funding_events=e.enriched_funding_events,enriched_alexa_ranking=e.enriched_alexa_ranking,enriched_keywords=e.enriched_keywords,enriched_org_hq=e.enriched_org_hq,enriched_total_funding=e.enriched_total_funding,enriched_funding_stage=e.enriched_funding_stage,updated_at=NOW() FROM enrichment_data e WHERE leads.session_id=e.session_id AND leads.session_id=$1`, [session_id]).catch(err => console.warn('[/submit] Enrichment sync failed (non-blocking):', err.message));

    syncToAWS({session_id,page_url,email,website,sell_to,first_name,last_name,phone,company,hear_about_us,utm_source,utm_medium,utm_campaign,utm_content,utm_term,referrer,prefill_source,fbc,fbp,landing_page,previous_page,enriched_title:enrich.enriched_title,enriched_company_size:enrich.enriched_company_size,enriched_industry:enrich.enriched_industry,enriched_linkedin:enrich.enriched_linkedin,enriched_city:enrich.enriched_city,enriched_state:enrich.enriched_state,enriched_country:enrich.enriched_country,enriched_seniority:enrich.enriched_seniority,enriched_departments:enrich.enriched_departments,enriched_email_status:enrich.enriched_email_status,enriched_founded_year:enrich.enriched_founded_year,enriched_annual_revenue:enrich.enriched_annual_revenue,enriched_funding_events:enrich.enriched_funding_events,enriched_alexa_ranking:enrich.enriched_alexa_ranking,enriched_keywords:enrich.enriched_keywords,enriched_org_hq:enrich.enriched_org_hq,enriched_total_funding:enrich.enriched_total_funding,enriched_funding_stage:enrich.enriched_funding_stage,disqualified,disqualified_reason,step_reached:2,completed:true});

    if (!alreadyCompleted) {
      slackSubmit({first_name,last_name,email,phone,company,website,sell_to,hear_about_us,landing_page,previous_page,page_url,referrer,utm_source,utm_medium,utm_campaign,utm_content,prefill_source,enriched_title:enrich.enriched_title,enriched_company_size:enrich.enriched_company_size,enriched_industry:enrich.enriched_industry,enriched_linkedin:enrich.enriched_linkedin,enriched_city:enrich.enriched_city,enriched_state:enrich.enriched_state,enriched_country:enrich.enriched_country,enriched_seniority:enrich.enriched_seniority,enriched_departments:enrich.enriched_departments,enriched_email_status:enrich.enriched_email_status,enriched_founded_year:enrich.enriched_founded_year,enriched_annual_revenue:enrich.enriched_annual_revenue,enriched_funding_events:enrich.enriched_funding_events,enriched_alexa_ranking:enrich.enriched_alexa_ranking,enriched_keywords:enrich.enriched_keywords,enriched_org_hq:enrich.enriched_org_hq,enriched_total_funding:enrich.enriched_total_funding,enriched_funding_stage:enrich.enriched_funding_stage});

      pushToSalesforce({first_name,last_name,email,phone,company,website,sell_to,hear_about_us,page_url,fbc,fbp,utm_source,utm_medium,utm_campaign,utm_content,utm_term,referrer,landing_page,enriched_title:enrich.enriched_title,enriched_company_size:enrich.enriched_company_size,enriched_industry:enrich.enriched_industry,enriched_linkedin:enrich.enriched_linkedin,enriched_seniority:enrich.enriched_seniority,enriched_departments:enrich.enriched_departments,enriched_city:enrich.enriched_city,enriched_state:enrich.enriched_state,enriched_country:enrich.enriched_country,enriched_annual_revenue:enrich.enriched_annual_revenue,enriched_total_funding:enrich.enriched_total_funding,enriched_funding_stage:enrich.enriched_funding_stage,enriched_founded_year:enrich.enriched_founded_year,step_reached:2,booked:false}).catch(err => console.warn('[/submit] SF push failed (non-blocking):', err.message));

      pushFormEventsToMeta({session_id,email,phone,first_name,last_name,company,website,sell_to,page_url,fbc,fbp,landing_page,enriched_city:enrich.enriched_city,enriched_state:enrich.enriched_state,enriched_country:enrich.enriched_country,enriched_company_size:enrich.enriched_company_size,enriched_industry:enrich.enriched_industry,enriched_seniority:enrich.enriched_seniority,enriched_funding_stage:enrich.enriched_funding_stage}, {clientIpAddress:req.headers['x-forwarded-for']||req.ip||'',clientUserAgent:req.headers['user-agent']||''}).catch(err => console.warn('[/submit] Meta CAPI failed (non-blocking):', err.message));

      console.log(`[/submit] ✅ Lead completed: ${email} | session: ${session_id}`);
    } else {
      console.log(`[/submit] ⏭ Slack skipped — already completed: ${email} | session: ${session_id}`);
    }
    res.json({ ok: true });
  } catch (err) { console.error('[/submit]', err.message); res.status(500).json({ error: 'Submit failed' }); }
});

/* --------------------------------------------------------
   POST /booking-confirmed  — browser-side Cal callback
-------------------------------------------------------- */
app.post('/booking-confirmed', async (req, res) => {
  const session_id  = (req.body.session_id  || '').toString().trim().slice(0, 100);
  const booking_uid = (req.body.booking_uid || '').toString().trim().slice(0, 100);
  const start_time  = req.body.start_time   || null;
  const end_time    = req.body.end_time     || null;
  const event_type  = (req.body.event_type  || '').toString().trim().slice(0, 100);
  if (!session_id || !booking_uid) return res.status(400).json({ error: 'session_id and booking_uid required' });
  try {
    await pool.query('UPDATE leads SET booking_uid=$2,start_time=$3,end_time=$4,event_type=$5,booked_at=NOW(),updated_at=NOW() WHERE session_id=$1', [session_id,booking_uid,start_time,end_time,event_type||null]);
    syncBookingToAWS(session_id,booking_uid,start_time,end_time,event_type);
    const leadRow = await pool.query('SELECT email FROM leads WHERE session_id=$1', [session_id]);
    const email   = leadRow.rows[0]?.email;
    if (email) {
      findSFLeadByEmail(email).then(leadId => {
        if (leadId) return updateSFLead(leadId, { booking_uid__c: booking_uid, booking_start_time__c: start_time || '', booking_event_type__c: event_type || '', completed__c: true });
      }).catch(err => console.warn('[/booking-confirmed] SF update failed (non-blocking):', err.message));
      pool.query('SELECT * FROM leads l LEFT JOIN enrichment_data e ON e.session_id=l.session_id WHERE l.session_id=$1', [session_id]).then(r => {
        const fullLead = r.rows[0] || {};
        return pushFormEventsToMeta({...fullLead, booking_uid}, {clientIpAddress:req.headers['x-forwarded-for']||req.ip||'',clientUserAgent:req.headers['user-agent']||''});
      }).catch(err => console.warn('[/booking-confirmed] Meta CAPI failed (non-blocking):', err.message));
    }
    console.log(`[/booking-confirmed] ✅ Booked: ${booking_uid} | session: ${session_id} | email: ${email}`);
    res.json({ ok: true });
  } catch (err) { console.error('[/booking-confirmed]', err.message); res.status(500).json({ error: 'Booking update failed' }); }
});

/* --------------------------------------------------------
   POST /booking-confirmed-webhook  — Cal.com server-side webhook
-------------------------------------------------------- */
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

    const existingLead = await pool.query('SELECT session_id, email, booking_uid FROM leads WHERE email=$1 ORDER BY created_at DESC LIMIT 1', [email]);

    if (existingLead.rows.length > 0) {
      const lead = existingLead.rows[0];
      if (!lead.booking_uid) {
        await pool.query('UPDATE leads SET booking_uid=$2,start_time=$3,end_time=$4,event_type=$5,booked_at=NOW(),updated_at=NOW() WHERE session_id=$1', [lead.session_id, bookingUid, startTime || null, endTime || null, eventType || null]);
        syncBookingToAWS(lead.session_id, bookingUid, startTime, endTime, eventType);
        findSFLeadByEmail(email).then(leadId => {
          if (leadId) return updateSFLead(leadId, { booking_uid__c: bookingUid, booking_start_time__c: startTime || '', booking_event_type__c: eventType || '', completed__c: true });
        }).catch(err => console.warn('[/cal-webhook] SF update failed (non-blocking):', err.message));
        pool.query('SELECT * FROM leads l LEFT JOIN enrichment_data e ON e.session_id=l.session_id WHERE l.session_id=$1', [lead.session_id]).then(r => {
          const fullLead = r.rows[0] || {};
          return pushFormEventsToMeta({...fullLead, booking_uid: bookingUid}, {clientIpAddress:'',clientUserAgent:''});
        }).catch(err => console.warn('[/cal-webhook] Meta CAPI failed (non-blocking):', err.message));
        console.log(`[/cal-webhook] ✅ Updated existing lead: ${email} | session: ${lead.session_id}`);
      } else {
        console.log(`[/cal-webhook] ⏭ Lead already booked: ${email} | existing booking: ${lead.booking_uid}`);
      }
      return res.json({ ok: true, action: 'updated_existing' });
    }

    const enrichRow = await pool.query('SELECT * FROM enrichment_data WHERE email=$1 ORDER BY enriched_at DESC LIMIT 1', [email]);
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
      const personalDomains = ['gmail.com','yahoo.com','hotmail.com','outlook.com','icloud.com','protonmail.com','aol.com','mail.com','yahoo.in','rediffmail.com','ymail.com','live.com','msn.com','me.com','mac.com','googlemail.com'];
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

    pushToSalesforce({ first_name:slackFirstName, last_name:slackLastName, email, phone:attendee.phone||'', company:slackCompany, sell_to:'B2B', booking_uid:bookingUid, start_time:startTime, event_type:eventType, enriched_title:enrichData.enriched_title, enriched_company_size:enrichData.enriched_company_size, enriched_industry:enrichData.enriched_industry, enriched_linkedin:enrichData.enriched_linkedin, enriched_seniority:enrichData.enriched_seniority, enriched_departments:enrichData.enriched_departments, enriched_city:enrichData.enriched_city, enriched_state:enrichData.enriched_state, enriched_country:enrichData.enriched_country, enriched_annual_revenue:enrichData.enriched_annual_revenue, enriched_total_funding:enrichData.enriched_total_funding, enriched_funding_stage:enrichData.enriched_funding_stage, enriched_founded_year:enrichData.enriched_founded_year, step_reached:2, booked:true }).catch(err => console.warn('[/cal-webhook] SF push failed (non-blocking):', err.message));

    console.log(`[/cal-webhook] ✅ Created new lead: ${email} | session: ${webhookSessionId}`);
    res.json({ ok: true, action: 'created_new', session_id: webhookSessionId });

  } catch (err) {
    console.error('[/cal-webhook] Error:', err.message);
    res.status(500).json({ error: 'Webhook processing failed' });
  }
});

/* --------------------------------------------------------
   POST /cron/send-partials
   UPDATED:
   - Interval widened from 30 mins → 2 hours
   - Cross-session booking check added: skips email + Slack
     if this email has booked on ANY session after this
     session's created_at
-------------------------------------------------------- */
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
    console.log(`[Cron] Found ${leads.length} leads to process`);

    for (const lead of leads) {
      // Belt-and-suspenders: skip disqualified (should never reach here but guard anyway)
      if (lead.disqualified) {
        console.log(`[Cron] ⏭ Skipping disqualified lead: ${lead.email}`);
        await pool.query('UPDATE leads SET loops_sent=true WHERE session_id=$1', [lead.session_id]);
        if (awsPool) awsPool.query('UPDATE gw_form_leads SET loops_sent=true,updated_at=NOW() WHERE session_id=$1', [lead.session_id]).catch(err => console.warn('[AWS] ⚠ loops_sent sync failed:', err.message));
        continue;
      }

      // Belt-and-suspenders: re-check booking in case of race condition between query and now
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

      // Fire Slack partial notification
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

      // Send follow-up email
      await sendFollowUpEmail(lead.email, lead.first_name);

      // Mark as processed so cron never picks this session up again
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

/* --------------------------------------------------------
   POST /booking-confirmed-webhook-rh  — RevenueHero server-side webhook
-------------------------------------------------------- */
app.post('/booking-confirmed-webhook-rh', async (req, res) => {
  const rhSecret = process.env.RH_WEBHOOK_SECRET;
  if (rhSecret) {
    const signature = req.headers['x-rh-signature'] || req.headers['x-revenuehero-signature'];
    if (signature) {
      const expected = crypto.createHmac('sha256', rhSecret).update(JSON.stringify(req.body)).digest('hex');
      if (signature !== expected) {
        console.warn('[/rh-webhook] ⚠ Invalid signature — rejecting');
        return res.status(401).json({ error: 'Invalid signature' });
      }
    } else {
      console.warn('[/rh-webhook] ⚠ No signature header found on request — proceeding unverified for now');
    }
  }

  try {
    const payload = req.body;
    console.log('[/rh-webhook] Raw payload received:', JSON.stringify(payload).substring(0, 500));

    if (!payload.id || !payload.prospect?.email) {
      console.log('[/rh-webhook] No meeting payload or email — skipping');
      return res.json({ ok: true, skipped: true });
    }

    const email      = (payload.prospect.email || '').toString().trim().toLowerCase();
    const rhName     = payload.prospect.name || '';
    const bookingUid = payload.id || '';
    const startTime  = payload.meeting_time || '';
    const eventType  = payload.meeting_type_name || 'demo';
    const status     = payload.status || '';

    if (!email || !bookingUid) {
      console.warn('[/rh-webhook] Missing email or booking_uid — skipping');
      return res.status(400).json({ error: 'email and booking_uid required' });
    }

    if (status === 'cancelled') {
      console.log(`[/rh-webhook] Skipping cancelled meeting: ${bookingUid} | email: ${email}`);
      return res.json({ ok: true, skipped: true, reason: 'cancelled' });
    }

    console.log(`[/rh-webhook] Received booking: ${bookingUid} | email: ${email} | name: ${rhName} | event: ${eventType}`);

    const existingLead = await pool.query('SELECT session_id, email, booking_uid FROM leads WHERE email=$1 ORDER BY created_at DESC LIMIT 1', [email]);

    if (existingLead.rows.length > 0) {
      const lead = existingLead.rows[0];
      if (!lead.booking_uid) {
        await pool.query('UPDATE leads SET booking_uid=$2,start_time=$3,event_type=$4,booked_at=NOW(),updated_at=NOW() WHERE session_id=$1', [lead.session_id, bookingUid, startTime || null, eventType || null]);
        syncBookingToAWS(lead.session_id, bookingUid, startTime, null, eventType);
        findSFLeadByEmail(email).then(leadId => {
          if (leadId) return updateSFLead(leadId, { booking_uid__c: bookingUid, booking_start_time__c: startTime || '', booking_event_type__c: eventType || '', completed__c: true });
        }).catch(err => console.warn('[/rh-webhook] SF update failed (non-blocking):', err.message));
        pool.query('SELECT * FROM leads l LEFT JOIN enrichment_data e ON e.session_id=l.session_id WHERE l.session_id=$1', [lead.session_id]).then(r => {
          const fullLead = r.rows[0] || {};
          return pushFormEventsToMeta({...fullLead, booking_uid: bookingUid}, {clientIpAddress:'',clientUserAgent:''});
        }).catch(err => console.warn('[/rh-webhook] Meta CAPI failed (non-blocking):', err.message));
        console.log(`[/rh-webhook] ✅ Updated existing lead: ${email} | session: ${lead.session_id}`);
      } else {
        console.log(`[/rh-webhook] ⏭ Lead already booked: ${email} | existing booking: ${lead.booking_uid}`);
      }
      return res.json({ ok: true, action: 'updated_existing' });
    }

    // Safety net fallback — create new lead if somehow no session exists
    const enrichRow = await pool.query('SELECT * FROM enrichment_data WHERE email=$1 ORDER BY enriched_at DESC LIMIT 1', [email]);
    const enrich    = enrichRow.rows[0] || {};

    const nameParts  = rhName.split(' ');
    const firstName  = enrich.enriched_first_name || nameParts[0] || '';
    const lastName   = enrich.enriched_last_name  || nameParts.slice(1).join(' ') || '';
    const company    = enrich.enriched_company || '';
    const webhookSessionId = crypto.randomUUID(); // fixed — must be valid UUID

    await pool.query(`
      INSERT INTO leads (session_id,email,first_name,last_name,company,enriched_title,enriched_company_size,enriched_industry,enriched_linkedin,enriched_city,enriched_state,enriched_country,enriched_seniority,enriched_departments,enriched_email_status,enriched_founded_year,enriched_annual_revenue,enriched_funding_events,enriched_alexa_ranking,enriched_keywords,enriched_org_hq,enriched_total_funding,enriched_funding_stage,step_reached,completed,submitted_at,booking_uid,start_time,event_type,booked_at,prefill_source,sell_to,updated_at)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,2,true,NOW(),$24,$25,$26,NOW(),'rh_webhook','B2B',NOW())
      ON CONFLICT (session_id) DO NOTHING
    `, [webhookSessionId, email, firstName||null, lastName||null, company||null, enrich.enriched_title||null, enrich.enriched_company_size||null, enrich.enriched_industry||null, enrich.enriched_linkedin||null, enrich.enriched_city||null, enrich.enriched_state||null, enrich.enriched_country||null, enrich.enriched_seniority||null, enrich.enriched_departments||null, enrich.enriched_email_status||null, enrich.enriched_founded_year||null, enrich.enriched_annual_revenue||null, enrich.enriched_funding_events||null, enrich.enriched_alexa_ranking||null, enrich.enriched_keywords||null, enrich.enriched_org_hq||null, enrich.enriched_total_funding||null, enrich.enriched_funding_stage||null, bookingUid, startTime||null, eventType||null]);

    syncToAWS({ session_id:webhookSessionId, email, first_name:firstName, last_name:lastName, company, sell_to:'B2B', completed:true, step_reached:2, prefill_source:'rh_webhook' });
    slackSubmit({ first_name:firstName, last_name:lastName, email, company, sell_to:'B2B', prefill_source:'rh_webhook' });
    pushToSalesforce({ first_name:firstName, last_name:lastName, email, company, sell_to:'B2B', booking_uid:bookingUid, start_time:startTime, event_type:eventType, step_reached:2, booked:true }).catch(err => console.warn('[/rh-webhook] SF push failed (non-blocking):', err.message));

    console.log(`[/rh-webhook] ✅ Created new lead (fallback): ${email} | session: ${webhookSessionId}`);
    res.json({ ok: true, action: 'created_new', session_id: webhookSessionId });

  } catch (err) {
    console.error('[/rh-webhook] Error:', err.message);
    res.status(500).json({ error: 'Webhook processing failed' });
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
  } catch (err) { console.error('[GW API] Failed to start:', err); process.exit(1); }
}

start();
