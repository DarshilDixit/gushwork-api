require('dotenv').config();
const crypto    = require('crypto');
const dnsPromises = require('dns').promises;
const express   = require('express');
const cors      = require('cors');
const helmet    = require('helmet');
const rateLimit = require('express-rate-limit');
const { Pool }  = require('pg');
const { pool, initDB } = require('./db');
const { sendConversion, fetchPartnership, sendAction, fetchCustomer } = require('./partnerstack');
const { pushToSalesforce, findSFLeadByEmail, updateSFLead, findQualifiedDemoOpportunities, findOpportunityDomains } = require('./salesforce');
const { pushFormEventsToMeta, pushStartTrialToMeta } = require('./meta-capi');
const createLeadMagnetRouter = require('./lead-magnet');

const app  = express();
const PORT = process.env.PORT || 3000;

// Free/personal mailbox domains — shared by /enrich, webhook enrichment,
// and the /partial StartTrial gate (Meta CAPI fires for business emails only)
const FREE_EMAIL_DOMAINS = ['gmail.com','yahoo.com','hotmail.com','outlook.com','icloud.com','protonmail.com','aol.com','mail.com','yahoo.in','rediffmail.com','ymail.com','live.com','msn.com','me.com','mac.com','googlemail.com'];

/* ── Free-provider detection, typo-tolerant ───────────────────────
   v5.6.0. FREE_EMAIL_DOMAINS is an exact-match list, and that is a hole:
   typosquatters register the common misspellings and point a catch-all
   mail server at them. darshildixit21@gmailc.com sailed through as a
   BUSINESS email and fired a StartTrial to Meta — the exact opposite of
   what the free-email filter exists to do, since Meta then optimises
   toward more consumer traffic.

   Confirmed live mail on gmailc.com, gmai.com, gnail.com and
   hotmial.com, so this is not hypothetical. Matching by edit distance
   instead of an exact list catches the squats nobody has registered yet
   too, rather than playing whack-a-mole with a growing list.

   Deliberately strict: distance 1 only, and only against domains of 8+
   characters. 'me.com' vs 'we.com' is one edit but they are unrelated,
   whereas 'gmailc.com' vs 'gmail.com' at that length is unmistakable. */
function freeEmailMatch(domain) {
  const d = String(domain || '').toLowerCase().trim();
  if (!d) return null;
  if (FREE_EMAIL_DOMAINS.includes(d)) return { domain: d, exact: true };
  for (const c of FREE_EMAIL_DOMAINS) {
    if (c.length < 8) continue;              // too short for a safe near-match
    if (Math.abs(d.length - c.length) > 1) continue;
    if (damerauLevenshtein(d, c) === 1) return { domain: c, exact: false };
  }
  return null;
}

// True for a real free provider OR a near-miss typo of one.
function isFreeEmailDomain(domain) {
  return freeEmailMatch(domain) !== null;
}

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
        ps_xid                  TEXT,
        ps_partner_key          TEXT,
        ps_partner_name         TEXT,
        ps_partner_email        TEXT,
        ps_customer_key         TEXT,
        ps_click_at             TIMESTAMPTZ,
        ps_click_history        JSONB,
        ps_signup_sent_at       TIMESTAMPTZ,
        ps_signup_verified_at   TIMESTAMPTZ,
        ps_signup_skipped_reason TEXT,
        ps_signup_skipped_at    TIMESTAMPTZ,
        ps_signup_failed_at     TIMESTAMPTZ,
        ps_signup_fail_reason   TEXT,
        ps_qualify_failed_at    TIMESTAMPTZ,
        ps_qualify_fail_reason  TEXT,
        ps_qualified_sent_at    TIMESTAMPTZ,
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
      /* PartnerStack affiliate attribution. Mirrored so the dialer and anything
         else reading gw_form_leads can see that a lead came from a partner
         without going back to Railway. The two _sent_at stamps are written by
         the conversion and qualification calls, which do not exist yet. */
      `ALTER TABLE gw_form_leads ADD COLUMN IF NOT EXISTS ps_xid TEXT`,
      `ALTER TABLE gw_form_leads ADD COLUMN IF NOT EXISTS ps_partner_key TEXT`,
      `ALTER TABLE gw_form_leads ADD COLUMN IF NOT EXISTS ps_partner_name TEXT`,
      `ALTER TABLE gw_form_leads ADD COLUMN IF NOT EXISTS ps_partner_email TEXT`,
      `ALTER TABLE gw_form_leads ADD COLUMN IF NOT EXISTS ps_customer_key TEXT`,
      `ALTER TABLE gw_form_leads ADD COLUMN IF NOT EXISTS ps_click_at TIMESTAMPTZ`,
      `ALTER TABLE gw_form_leads ADD COLUMN IF NOT EXISTS ps_click_history JSONB`,
      `ALTER TABLE gw_form_leads ADD COLUMN IF NOT EXISTS ps_signup_sent_at TIMESTAMPTZ`,
      `ALTER TABLE gw_form_leads ADD COLUMN IF NOT EXISTS ps_signup_verified_at TIMESTAMPTZ`,
      `ALTER TABLE gw_form_leads ADD COLUMN IF NOT EXISTS ps_signup_skipped_reason TEXT`,
      `ALTER TABLE gw_form_leads ADD COLUMN IF NOT EXISTS ps_signup_skipped_at TIMESTAMPTZ`,
      `ALTER TABLE gw_form_leads ADD COLUMN IF NOT EXISTS ps_signup_failed_at TIMESTAMPTZ`,
      `ALTER TABLE gw_form_leads ADD COLUMN IF NOT EXISTS ps_signup_fail_reason TEXT`,
      `ALTER TABLE gw_form_leads ADD COLUMN IF NOT EXISTS ps_qualify_failed_at TIMESTAMPTZ`,
      `ALTER TABLE gw_form_leads ADD COLUMN IF NOT EXISTS ps_qualify_fail_reason TEXT`,
      `ALTER TABLE gw_form_leads ADD COLUMN IF NOT EXISTS ps_qualified_sent_at TIMESTAMPTZ`,
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
       step_reached, completed, submitted_at, loops_sent,
       ps_xid, ps_partner_key, ps_partner_name, ps_partner_email, ps_customer_key,
       ps_click_at, ps_click_history,
       ps_signup_sent_at, ps_signup_verified_at, ps_qualified_sent_at, updated_at)
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34,$35,$36,$37,$38,$39,$40,$41,$42,$43,$44,$45,$46,$47,$48,$49,$50,$51,$52,$53,$54,$55,NOW())
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
      ps_xid                  = COALESCE(EXCLUDED.ps_xid,                  gw_form_leads.ps_xid),
      ps_partner_key          = COALESCE(EXCLUDED.ps_partner_key,          gw_form_leads.ps_partner_key),
      ps_partner_name         = COALESCE(EXCLUDED.ps_partner_name,         gw_form_leads.ps_partner_name),
      ps_partner_email        = COALESCE(EXCLUDED.ps_partner_email,        gw_form_leads.ps_partner_email),
      ps_customer_key         = COALESCE(EXCLUDED.ps_customer_key,         gw_form_leads.ps_customer_key),
      ps_click_at             = COALESCE(EXCLUDED.ps_click_at,             gw_form_leads.ps_click_at),
      ps_click_history        = COALESCE(EXCLUDED.ps_click_history,        gw_form_leads.ps_click_history),
      /* These three exist on the mirror and were never written by this
         function — they are only ever stamped by targeted UPDATEs on Railway,
         so gw_form_leads showed NULL for every row and the mirror could not
         answer "did this convert?" at all. COALESCE so a later partial sync
         cannot wipe a stamp that a targeted write has already set. */
      ps_signup_sent_at       = COALESCE(EXCLUDED.ps_signup_sent_at,       gw_form_leads.ps_signup_sent_at),
      ps_signup_verified_at   = COALESCE(EXCLUDED.ps_signup_verified_at,   gw_form_leads.ps_signup_verified_at),
      ps_qualified_sent_at    = COALESCE(EXCLUDED.ps_qualified_sent_at,    gw_form_leads.ps_qualified_sent_at),
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
    data.loops_sent              || false,
    data.ps_xid                  || null,   data.ps_partner_key            || null,
    data.ps_partner_name         || null,   data.ps_partner_email          || null,
    data.ps_customer_key         || null,
    data.ps_click_at             || null,
    data.ps_click_history ? JSON.stringify(data.ps_click_history) : null,
    data.ps_signup_sent_at       || null,   data.ps_signup_verified_at     || null,
    data.ps_qualified_sent_at    || null
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

/* The partner name resolves AFTER the row has already been mirrored, so the
   ordinary syncToAWS upsert has been and gone by the time we know it. Same
   shape as syncBookingToAWS above and for the same reason: a late-arriving
   fact needs its own targeted write or the mirror never sees it. */
function syncPartnerIdentityToAWS(session_id, name, email) {
  if (!awsPool) return;
  awsPool.query(
    `UPDATE gw_form_leads
        SET ps_partner_name = COALESCE($2, ps_partner_name),
            ps_partner_email = COALESCE($3, ps_partner_email),
            updated_at = NOW()
      WHERE session_id = $1`,
    [session_id, name || null, email || null]
  ).catch(err => console.warn('[AWS] ⚠ Partner identity sync failed:', err.message));
}

/* A targeted UPDATE, NOT syncToAWS, and the distinction matters.
   syncToAWS is a whole-row upsert whose conflict clause sets
   `disqualified = EXCLUDED.disqualified` UNCONDITIONALLY — no COALESCE. Handing
   it a partial object therefore passes disqualified as false and CLEARS a real
   disqualification on the mirror, which the dialer reads. Any late-arriving
   single field needs its own write, like the booking and identity syncs above. */
function syncHearAboutUsToAWS(session_id, hear_about_us) {
  if (!awsPool) return;
  awsPool.query(
    `UPDATE gw_form_leads SET hear_about_us = $2, updated_at = NOW() WHERE session_id = $1`,
    [session_id, hear_about_us || null]
  ).catch(err => console.warn('[AWS] ⚠ hear_about_us sync failed:', err.message));
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
//   check_blocked           The site's bot protection served us an interstitial
//                           instead of the page (SiteGround captcha and the
//                           like). VERIFIED ON PURPOSE, and this is a revenue
//                           decision worth stating plainly: these leads land on
//                           'thin_content' today, which fires Meta, so keeping
//                           check_blocked verified changes the WORDING an SDR
//                           reads and nothing else. Moving it out of this list
//                           would silently stop Meta events for real
//                           businesses — marathontechnology.com and reiser.com
//                           are both live sites. A captcha wall is also
//                           positive evidence: nobody puts bot protection in
//                           front of a parked domain. Move it if the team
//                           decides otherwise; it is a one-line change.
const WEBSITE_VERIFIED_REASONS = [
  'resolved', 'mx_only', 'content_clean', 'test_email_skipped', 'ok',
  'forwarded_to_live_site', 'live_despite_dns_hint',
  'thin_content', 'thin_content_wildcard', 'nxdomain_contradicted',
  'check_blocked',
];

// Reasons that mean "we looked and it is genuinely not a company website".
// These set website_check_failed and suppress Meta. They do NOT block the
// form — blocking is form.js's WEBSITE_BLOCKING_REASONS, unchanged.
const WEBSITE_NEGATIVE_REASONS = ['for_sale_lander', 'marketplace_redirect', 'parked_confirmed', 'hosting_placeholder'];

/* ── Verdicts that mean "we never got a real answer about this site" ──
   NOT a fourth list to keep in sync with gushwork-form.js. This one is
   read-only and server-side: nothing here changes a decision, blocks a
   lead, or gates Meta. It exists so isUnverifiablePair() (in the ELV
   section) can ask "did the website check actually conclude anything?"
   without re-deriving it from the other three lists by elimination.

   Deliberately OUT, each for a reason:
     check_blocked, http_403/401/429/999  a bot wall is positive evidence.
                                          Nobody puts captcha protection in
                                          front of a domain that isn't real.
                                          Same argument as the check_blocked
                                          note above WEBSITE_VERIFIED_REASONS.
     thin_content, non_html               we reached the site and saw
                                          something. That is an answer.
     parked_confirmed, for_sale_lander,   real answers, already surfaced and
     nxdomain, no_dns_records             already suppressing Meta. Flagging
                                          them a second time is noise.
     null / ''                            no check ran, or the row predates
                                          the field. Absence of a verdict is
                                          not a verdict. */
const WEBSITE_UNREACHABLE_REASONS = [
  'dns_unresolved', 'dns_indeterminate', 'doh_error',
  'timeout', 'unreachable', 'fetch_error', 'backend_error',
  'skipped_no_backend', 'skipped_unsafe_target', 'unknown',
];

/* ── Plain-English labels ─────────────────────────────────────────
   Slack and the monitor used to print the raw reason code — 'parked_ns',
   'doh_error' — which tells an SDR nothing. Same verdict, readable wording.
   Purely presentational: nothing here changes a decision. */
const WEBSITE_REASON_LABELS = {
  nxdomain:               "Domain doesn't exist — likely a typo",
  no_dns_records:         'Domain registered but nothing set up on it',
  hosting_placeholder:    'No website yet — domain points to a hosting setup page',
  parked_confirmed:       'Domain registered but no website on it',
  parked:                 'Domain registered but no website on it',
  parked_ns:              'Domain registered but no website on it',
  parked_suspect:         'Looks like a parked domain — could not confirm',
  for_sale_lander:        'Domain is listed for sale',
  marketplace_redirect:   'Domain is for sale on a domain marketplace',
  mailbox_domain:         'Typed an email provider instead of their website',
  brand_mismatch:         "Typed a well-known brand's site, not their own",
  social_profile_url:     'Gave a social profile instead of a website',
  thin_content:           'Page looked mostly empty to us — worth a manual look',
  thin_content_wildcard:  'Page looked mostly empty to us — worth a manual look',
  check_blocked:          'Site blocked our check — the page itself looks fine',
  dns_unresolved:         'Could not look up the domain — DNS gave no answer',
  forwarded_to_live_site: 'Redirects to their live site — checked OK',
  live_despite_dns_hint:  'Live site (an early parking signal was overruled)',
  mx_only:                'Email-only company — no website, but mail works',
  nxdomain_contradicted:  'DNS blip — domain matches their verified email domain',
  content_clean:          'Live website',
  resolved:               'Domain resolves',
  dns_indeterminate:      'Could not reach the site to check it',
  doh_error:              'Could not reach the site to check it',
  timeout:                'Could not reach the site to check it',
  unreachable:            'Could not reach the site to check it',
  non_html:               'Address did not return a web page',
  backend_error:          'Our check errored — not the website’s fault',
  fetch_error:            'Our check errored — not the website’s fault',
  skipped_no_backend:     'Check was skipped',
  skipped_unsafe_target:  'Address pointed at an internal network — skipped',
  test_email_skipped:     'Internal test — check skipped',
  ok:                     'Website checked OK',
};

// SEO is the product, so "they have no website yet" is a sales fact, not just
// a data note. These get an extra line an SDR can actually use on the call.
const WEBSITE_SALES_HINTS = {
  hosting_placeholder: 'They may need a site built before SEO can help',
  no_dns_records:      'They may need a site built before SEO can help',
  mx_only:             'They may need a site built before SEO can help',
};

function websiteReasonLabel(reason) {
  if (!reason) return 'Unknown';
  if (WEBSITE_REASON_LABELS[reason]) return WEBSITE_REASON_LABELS[reason];
  if (String(reason).startsWith('http_')) {
    const code = String(reason).slice(5);
    // 999 is LinkedIn's bespoke "no automated access" code; 401/403 are the
    // site refusing our checker. None of these say anything about the business.
    if (['999', '403', '401', '429'].includes(code)) return `Site blocked our check (${code})`;
    return `Site returned an error (${code})`;
  }
  return String(reason).replace(/_/g, ' '); // unknown code — at least make it readable
}

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

/* ── Eastern Time, everywhere ──────────────────────────────────────
   The dashboard, the Slack alerts and the export filenames are all read
   by people on US Eastern time. Before this, three different zones were
   in play at once: the Postgres session is Etc/UTC (confirmed), so a bare
   date_trunc bucketed days at 05:30 IST; the dashboard rendered every
   timestamp in Asia/Kolkata; and the date-preset buttons used
   getFullYear/getMonth/getDate, which read whatever the viewer's laptop
   was set to. Same lead, three different "days".

   ALWAYS the IANA name, never a fixed offset. 'EST' and '-05:00' are
   wrong for eight months of the year, and a hardcoded offset is the
   classic way a report silently shifts by an hour on the second Sunday
   in March. */
const DASH_TZ = 'America/New_York';

// 'en-CA' because it formats as YYYY-MM-DD, which is what filenames and
// SQL date literals want. Built once — Intl construction is not cheap and
// these run per request.
const _etDateFmt  = new Intl.DateTimeFormat('en-CA', { timeZone: DASH_TZ, year: 'numeric', month: '2-digit', day: '2-digit' });
const _etStampFmt = new Intl.DateTimeFormat('en-CA', {
  timeZone: DASH_TZ, year: 'numeric', month: '2-digit', day: '2-digit',
  hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false,
});

// Calendar date in ET. Used for CSV filenames, which used to be
// toISOString().slice(0,10) — so a 02:00 ET export was stamped with the
// previous day.
function etDateOnly(d) {
  return _etDateFmt.format(d instanceof Date ? d : new Date(d || Date.now()));
}

// Human timestamp for Slack and alert emails. Replaces toISOString(), which
// put UTC next to a dashboard showing ET and made cross-referencing an
// incident an arithmetic exercise.
function etStamp(d) {
  return _etStampFmt.format(d instanceof Date ? d : new Date(d || Date.now())) + ' ET';
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
      text:    `${subject}\n\n${lines}\n\nTime: ${etStamp()}`,
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
    blocks.push(bContext(`Severity: *${sev}* · ${etStamp()}`));
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
/* v5.5.0 — alertAfter lowered 5 → 3. At 30-40 leads/day a threshold of 5
   needed most of a working day to trip, and off-peak it never did. The
   consecutive-streak and auth-failure rules in recordFailure now catch the
   sharp cases; this count stays as the slow-trickle backstop.
   Loops added: it holds an API key that can be revoked, and had no failure
   alerting at all.
   Deliberately NOT here, because both already alert correctly:
     Salesforce — alertOps at every call site, critical, first failure.
     ELV        — alertOps at critical for HTTP 401/402/403 (key rejected /
                  credits exhausted), and timeouts feed recordElvOutcome,
                  which has its own consecutive-failure escape hatch. */
const FAILURE_MONITORS = {
  'Meta CAPI': { alertAfter: 3, impact: 'Conversion events are not reaching Meta, so ad optimisation is degrading.' },
  'Apollo':    { alertAfter: 3, impact: 'These leads were saved without enrichment data.' },
  'AWS sync':  { alertAfter: 3, impact: 'The AWS mirror database is drifting out of sync with Railway.' },
  'Email':     { alertAfter: 3, impact: 'Drop-off follow-up emails are not being delivered. If this is all of them, the Gmail connection is likely broken.' },
  'Loops':     { alertAfter: 3, impact: 'Lead-magnet contacts are not reaching the Loops mailing list.' },
};
const FAILURE_BUFFER_TTL_MS = 6 * 60 * 60 * 1000; // stale failures expire, so a slow trickle never accumulates
const _failBuffers = new Map(); // source -> [{ id, error, at }]

// Collect failures per integration and alert once a threshold is reached,
// listing exactly WHICH items failed. Deliberately count-based rather than
// percentage-based: "5 failures, here they are" is actionable, "40% of a
// rolling window" is not.
/* ── Credential failures alert INSTANTLY ──────────────────────────
   v5.5.0. A count-based threshold is right for flaky failures — one bad
   Apollo lookup shouldn't page anyone. It is wrong for a rejected
   credential, which never recovers on its own and stays broken until a
   human fixes it.

   Real case: on 19 Aug the growth@gushwork.ai password was reset at
   17:23 IST, which silently revokes every Gmail app password. The 18:30
   cron produced 535-5.7.8 twice. 'Email' alerts after 5 failures, so
   nothing fired — and because the send flag was set regardless of
   outcome, both leads (one a Founder & CEO) were marked as emailed and
   would never have been retried. Found only by reading logs.

   Any error matching these patterns now pages immediately, at critical,
   bypassing both the count threshold and the warning cooldown. */
const AUTH_FAILURE_PATTERNS = [
  /\b535\b/,                       // Gmail SMTP: Username and Password not accepted
  /\b534\b/,                       // Gmail SMTP: application-specific password required
  /username and password not accepted/i,
  /application-specific password required/i,
  /invalid[ _-]?login/i,
  /bad ?credentials/i,
  /invalid_grant/i,                // OAuth refresh token dead
  /invalid_client/i,
  /INVALID_SESSION_ID/i,           // Salesforce session expired
  /invalid[ _-]?api[ _-]?key/i,
  /api key rejected/i,             // ELV
  /credits exhausted/i,            // ELV — not auth, but equally terminal
  /\bunauthorized\b/i,             // HTTP 401 text
  /\bforbidden\b/i,                // HTTP 403 text
  /\b401\b/,
  /\b403\b/,
  /access[ _-]?token/i,            // Meta: expired/invalid access token
  /OAuth/i,
  /authenticat(e|ion|ed) fail/i,
  /permission denied/i,
];

function isAuthFailure(error) {
  const msg = String(error || '');
  if (!msg) return false;
  return AUTH_FAILURE_PATTERNS.some((re) => re.test(msg));
}

/* Plain-English meaning per service, so the Slack message says what to DO
   rather than making whoever is on call decode an SMTP code at 11pm. */
const AUTH_FAILURE_GUIDANCE = {
  'Email':      'Gmail rejected our login. An app password is revoked whenever the account password changes — generate a new one at myaccount.google.com/apppasswords and update GMAIL_APP_PASSWORD.',
  'Meta CAPI':  'Meta rejected the access token. Check META_ACCESS_TOKEN — tokens expire, and are revoked when the generating user loses access.',
  'Apollo':     'Apollo rejected the API key. Check APOLLO_API_KEY and the account credit balance.',
  'Loops':      'Loops rejected the API key. Check LOOPS_API_KEY. Lead-magnet contacts are not reaching the mailing list.',
  'Salesforce': 'Salesforce rejected the session. The refresh token may be dead — check SF_REFRESH_TOKEN.',
  'AWS sync':   'AWS Postgres rejected the connection. Check the AWS_PG_* credentials.',
};

// Consecutive-failure streak per service. A run of failures is an outage at
// ANY volume — the count threshold needs traffic to trip, and quiet hours
// are exactly when nobody is watching the logs.
const _failStreaks = new Map();
const CONSECUTIVE_FAILURE_ALERT = Number(process.env.CONSECUTIVE_FAILURE_ALERT) || 3;

/* Hard ceiling on follow-up retries. Guarantees the retry loop terminates
   even for an SMTP error shape we do not classify, so no address is ever
   retried forever. Cron runs every 30 min, so 3 attempts spans ~1 hour —
   long enough to ride out a transient outage, short enough that a lead is
   never left waiting. */
const FOLLOWUP_MAX_ATTEMPTS = Number(process.env.FOLLOWUP_MAX_ATTEMPTS) || 3;

/* Call on SUCCESS to clear a service's streak, so an outage that recovers
   doesn't leave a stale count that trips on the next unrelated blip. */
function recordSuccess(source) {
  if (_failStreaks.get(source)) _failStreaks.set(source, 0);
}

function recordFailure(source, id, error) {
  try {
    const cfg = FAILURE_MONITORS[source];
    if (!cfg) return;
    const now = Date.now();
    const errStr = String(error || '').substring(0, 200);

    // ── Credential failure: page NOW, skip every threshold ──
    if (isAuthFailure(errStr)) {
      alertOps('critical', source, 'Authentication failed', {
        'Affected': id || 'unknown',
        'Error': errStr,
        'What this means': AUTH_FAILURE_GUIDANCE[source] || 'A credential for this service has been rejected. It will stay broken until it is replaced.',
        'Impact': cfg.impact,
      });
      _failStreaks.set(source, 0); // the alert is out; don't double-report via the streak
      return;
    }

    // ── Consecutive failures: an outage regardless of volume ──
    const streak = (_failStreaks.get(source) || 0) + 1;
    _failStreaks.set(source, streak);

    // The window buffer is maintained either way, so the slow-trickle path
    // below still has an accurate picture.
    let buf = (_failBuffers.get(source) || []).filter(x => now - x.at < FAILURE_BUFFER_TTL_MS);
    buf.push({ id: id || 'unknown', error: errStr.substring(0, 120), at: now });
    _failBuffers.set(source, buf);

    if (streak >= CONSECUTIVE_FAILURE_ALERT) {
      const sent = alertOps('warning', source, 'Consecutive failures', {
        'Count': `${streak} in a row`,
        'Affected': id || 'unknown',
        'Last error': errStr,
        'Impact': cfg.impact,
      });
      if (sent) {
        _failStreaks.set(source, 0);
        // Both thresholds are 3, so they trip on the same failure. Clearing
        // the window here stops one incident producing two Slack messages —
        // the streak alert is the more precise of the two, so it wins.
        _failBuffers.set(source, []);
      }
      return;
    }

    /* ── Slow trickle: N failures in the window, not consecutive ──
       Still worth keeping alongside the streak check, because a success
       between failures resets the streak but not the window. Three failed
       Apollo lookups in six hours matters even if they were interleaved
       with successes. */
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
/* _lastCronRunAt is seeded to boot time so the heartbeat does not alert the
   instant an instance starts. That seeding is fine for an alert with a 3h
   window and wrong for a health BADGE, which would read green for three hours
   after every redeploy without the scheduler ever having called us. These two
   let the health check tell "has run" apart from "has just started". */
let _cronRanThisProcess = false;
const _processStartedAt = Date.now();
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
    /* The nine System Health rows run here rather than on the dashboard's
       60-second poll, so a failure notifies with the tab closed. Transitions
       only — see evaluateHealthAlerts. */
    runHealthChecks()
      .then(h => evaluateHealthAlerts(h.checks))
      .catch(err => console.warn('[heartbeat] health checks failed (ignored):', err && err.message));
  }, HEARTBEAT_CHECK_MS);
  console.log('[heartbeat] ✅ Monitoring started (cron staleness + lead flow + system health)');
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
      PARTNERSTACK_TRACKING_TOKEN: 'PartnerStack affiliate conversions',
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
  const hasPartner     = !!d.ps_partner_key;

  if (!hasAttribution && !hasJourney && !hasPartner) return;

  blocks.push(bDivider());
  blocks.push(bSection('*📊 Attribution & Journey*'));

  /* The partner line goes FIRST in this section, above the UTMs. An SDR
     picking up a partner-sourced lead needs to know that before anything
     else — it changes who owns the relationship and what they open with.

     Falls back to the raw key when the name has not resolved. That is the
     first lead from a brand-new partner only: the resolver runs after the
     response and every later lead reads the cache. A key is ugly but it is
     true, and an SDR can still search it. */
  if (hasPartner) {
    /* name -> email -> raw key. An email tells an SDR who they are dealing
       with; a hex key tells them nothing they can act on or search for. */
    const who = d.ps_partner_name
      ? `*${d.ps_partner_name}*` + (d.ps_partner_email ? ` (${d.ps_partner_email})` : '')
      : d.ps_partner_email
        ? `*${d.ps_partner_email}*  _(name not resolved yet)_`
        : `\`${d.ps_partner_key}\`  _(partner not resolved yet)_`;
    const clicked = d.ps_click_at ? ` — clicked ${etStamp(d.ps_click_at)}` : '';
    blocks.push(bSection(`🤝 *Partner:* ${who}${clicked}`));
  }

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
  // Flagged here as well as in /submit, and this is the case that mattered:
  // yo@yoyo.com never submitted. It dropped out, came through the partials
  // cron, and got a follow-up email. A flag that only exists on the submit
  // path would have missed the lead that prompted it.
  if (isUnverifiablePair(d)) {
    const nf = bFields([{ label: '⚠️ Nothing verified this lead', value: UNVERIFIABLE_PAIR_NOTE }]);
    if (nf) blocks.push(nf);
  }
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
  // Above the website-check line on purpose: this says nothing checked out
  // at all, which is a bigger fact than which check said what.
  if (isUnverifiablePair(d)) {
    const nf = bFields([{ label: '⚠️ Nothing verified this lead', value: UNVERIFIABLE_PAIR_NOTE }]);
    if (nf) blocks.push(nf);
  }
  if (d.website_check_failed) {
    const hint = WEBSITE_SALES_HINTS[d.website_check_reason];
    const wf = bFields([{
      label: '⚠️ Website check',
      value: websiteReasonLabel(d.website_check_reason) + (hint ? `\n→ ${hint}` : ''),
    }]);
    if (wf) blocks.push(wf);
  } else if (d.website_check_reason === 'thin_content' || d.website_check_reason === 'thin_content_wildcard') {
    // v5.4.0 — these are VERIFIED reasons, so they fell through to the final
    // else and were never shown at all. 25 of them in three days existed only
    // in the Railway logs. The design intent was always "flag it for a human";
    // without this branch there was no human-visible flag.
    const tf = bFields([{ label: 'ℹ️ Website check', value: websiteReasonLabel(d.website_check_reason) }]);
    if (tf) blocks.push(tf);
  } else if (d.website_check_reason && !WEBSITE_VERIFIED_REASONS.includes(d.website_check_reason) && d.website_check_reason !== 'social_profile_url') {
    const uf = bFields([{ label: '❓ Website check', value: websiteReasonLabel(d.website_check_reason) }]);
    if (uf) blocks.push(uf);
  } else if (d.website_check_reason === 'forwarded_to_live_site' || d.website_check_reason === 'live_despite_dns_hint' || d.website_check_reason === 'mx_only') {
    // Explains an odd-looking-but-fine result so nobody re-investigates it.
    const gf = bFields([{
      label: '✅ Website check',
      value: websiteReasonLabel(d.website_check_reason) + (WEBSITE_SALES_HINTS[d.website_check_reason] ? `\n→ ${WEBSITE_SALES_HINTS[d.website_check_reason]}` : ''),
    }]);
    if (gf) blocks.push(gf);
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

/* ── SMTP permanent-failure detection ─────────────────────────────
   v5.5.0. Distinguishing "our side is broken" from "that address does not
   exist" is what makes retrying safe. Retry the first — nothing is wrong
   with the recipient. NEVER retry the second: the address will not start
   existing, and repeatedly sending to dead addresses damages the sending
   reputation of the whole domain.
   SMTP 5xx = permanent (except the auth codes, which are OUR problem);
   4xx = transient. */
const SMTP_PERMANENT_PATTERNS = [
  /\b5[05][0-9]\b(?!.*\b53[45]\b)/,     // 550/551/552/553/554 etc — recipient rejected
  /no such user/i,
  /user unknown/i,
  /recipient (address )?rejected/i,
  /address (does not|doesn't) exist/i,
  /mailbox (unavailable|not found|does not exist)/i,
  /invalid recipient/i,
  /domain not found/i,
  /unrouteable address/i,
];

function isPermanentSmtpFailure(error) {
  const msg = String(error || '');
  if (!msg) return false;
  if (isAuthFailure(msg)) return false; // 535/534 are OUR credentials, not the recipient
  return SMTP_PERMANENT_PATTERNS.some((re) => re.test(msg));
}

/* Returns { sent, permanent, error }.
   v5.5.0 — this used to swallow its own failure and return undefined, so the
   cron marked every lead as emailed whether or not anything was delivered.
   On 19 Aug that silently burned two real leads, one a Founder & CEO. */
async function sendFollowUpEmail(email, firstName) {
  if (!process.env.GMAIL_USER || !process.env.GMAIL_APP_PASSWORD) {
    console.warn('[Email] GMAIL credentials not set — skipping');
    alertOps('critical', 'Email', 'Credentials not configured', { 'Impact': 'No drop-off follow-up emails are being sent at all.' });
    return { sent: false, permanent: false, error: 'credentials not configured' };
  }
  if (!email) return { sent: false, permanent: true, error: 'no email address' };

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
    recordSuccess('Email');
    return { sent: true, permanent: false, error: null };
  } catch (err) {
    const permanent = isPermanentSmtpFailure(err.message);
    // A rejected credential invalidates the pooled transport, so drop it —
    // otherwise every later send in this process reuses the dead connection.
    if (isAuthFailure(err.message)) {
      try { if (_gmailTransport && _gmailTransport.close) _gmailTransport.close(); } catch { /* ignore */ }
      _gmailTransport = null;
    }
    console.warn(`[Email] ⚠ Failed to send to ${email}${permanent ? ' (permanent — will not retry)' : ' (transient — will retry)'}:`, err.message);
    recordFailure('Email', email, err.message);
    return { sent: false, permanent, error: err.message };
  }
}

/* ── Booking integrity check ──────────────────────────────────────
   v5.8.0. A booking should always be preceded by /submit. On 18 Aug a spam
   lead (aasnj@meta.com, "lol noway", heard-about-us "porn hub") booked a demo
   with NO /submit and NO website check — and because the Slack notification
   is built inside /submit, nothing was announced. A colleague found it the
   next morning from the calendar invite.

   v5.5.0 added this check to /booking-confirmed only. That endpoint is fired
   by the BROWSER. Bookings also arrive server-to-server from the Cal webhook
   and the RevenueHero webhook, and neither checked anything — so a booking
   that never touched the browser still slipped in silently. Since we still do
   not know how step 2 was bypassed, the honest move is to watch every door
   rather than guess which one was used.

   Read-only and non-blocking. The booking is always honoured; a failure here
   is swallowed. */
async function alertIfBookingWithoutSubmit(session_id, source) {
  if (!session_id) return;
  try {
    // submitted_at is the column /submit stamps. `completed` is ALSO set by
    // the booking write itself, so it must never be used for this test.
    const pre = await pool.query(
      'SELECT email, step_reached, submitted_at, website, website_check_reason FROM leads WHERE session_id=$1',
      [session_id]
    );
    const row = pre.rows[0];
    if (!row || row.submitted_at) return;

    alertOps('warning', 'Form', 'Booking without completed form', {
      'Email': row.email || '(unknown)',
      'Session': session_id,
      'Arrived via': source,
      'Reached step': row.step_reached == null ? '(unknown)' : String(row.step_reached),
      'Website': row.website || '(never entered)',
      'Website check': row.website_check_reason ? websiteReasonLabel(row.website_check_reason) : '(never ran)',
      'Impact': 'This demo was booked without passing step 2, so the website check never ran and no lead notification was posted. Verify it is genuine before the call.',
    });
    console.warn(`[${source}] ⚠ Booking with no completed submit — session ${session_id} | email ${row.email} | step ${row.step_reached}`);
  } catch (err) {
    console.warn(`[${source}] pre-booking integrity check failed (ignored):`, err.message);
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

/* ══ SYSTEM HEALTH — nine rows that now check something ═══════════════
   Seven of the nine rows on the Health tab were lifetime counters wearing
   health-check clothing. The whole story was one line:

     badge("s-partial", d.total+" sessions saved", "bg")

   A literal green class with no condition attached. /partial read healthy
   whether it worked or had been 500ing for a week. /submit went green on
   the first submission in July and had no way back. AWS sync was
   !!awsPool — true whenever an env var was set, so wrong credentials, an
   unreachable host and every mirror write failing all read "Active".

   THIS IS THE INVERSION OF THE FAIL-OPEN RULE, AND IT IS DELIBERATE.
   Checkers on the lead path fail open because a lead is worth more than a
   verdict. No lead depends on a health probe, so these fail LOUD instead:
   a probe that cannot verify reports red, never green and never a calm
   grey. See "Health checks fail LOUD" in CLAUDE.md.

   Every windowed check carries a minimum denominator — the same idea as
   ELV_MIN_SAMPLE, which exists so a couple of unlucky checks cannot cry
   wolf. Below the floor the answer is insufficient_data, so a quiet night
   reads grey rather than a green nobody earned or a red nothing supports.
   The AWS check is the deliberate exception and cannot return grey at
   all; its own comment says why.

   Two rows are NOT rebuilt here because they were already real: API
   uptime (the browser's own fetch of /health is the only honest probe of
   "can a client reach this") and ELV (elvHealthSnapshot, which already
   owns its enter/exit alerting in recordElvOutcome — routing it through
   this path as well would alert twice for one incident).
   ═══════════════════════════════════════════════════════════════════ */

const HEALTH_MIN_SAMPLE            = Number(process.env.HEALTH_MIN_SAMPLE) || 8;  // ELV_MIN_SAMPLE is the precedent
const HEALTH_PARTIAL_WINDOW_H      = 2;
const HEALTH_SUBMIT_WINDOW_H       = 24;
const HEALTH_SUBMIT_MIN_STEP1      = 10;
const HEALTH_APOLLO_WINDOW_H       = 24;
const HEALTH_BOOKING_WINDOW_D      = 7;
const HEALTH_BOOKING_MIN_COMPLETED = 10;
const HEALTH_AWS_WINDOW_H          = 24;
const HEALTH_AWS_DRIFT_MIN         = 2;     // absolute floor, so one in-flight write is not an incident
const HEALTH_AWS_DRIFT_PCT         = 0.10;
const HEALTH_AWS_TIMEOUT_MS        = 8000;  // a hung mirror must resolve to red, not hang the dashboard
const HEALTH_RECOVERY_LOOKBACK_D   = 7;
/* 2h to become eligible for a follow-up, plus the 3h window in which the cron
   is expected to have run. Anything still unsent past that was not picked up. */
const HEALTH_RECOVERY_STUCK_H      = 5;

function hc(id, state, text, detail) {
  return { id, state, text, detail: detail || null };
}

function fmtAge(ms) {
  const min = Math.round(ms / 60000);
  if (min < 90) return min + ' min';
  const h = Math.round(min / 60);
  if (h < 48) return h + 'h';
  return Math.round(h / 24) + 'd';
}

function withTimeout(promise, ms, label) {
  let timer;
  return Promise.race([
    promise.finally(() => clearTimeout(timer)),
    new Promise((_, reject) => { timer = setTimeout(() => reject(new Error(label + ' timed out after ' + ms + 'ms')), ms); }),
  ]);
}

/* ── Step 1 — /partial ──────────────────────────────────────────────
   Sessions arriving with zero leads created means the form is broken at
   the very top, which is where most volume is already lost. Bots are
   excluded with the same BOT_RE the funnel uses, because a night of
   crawler traffic would otherwise push the denominator over the floor
   and manufacture a red. */
async function checkPartialHealth(db) {
  try {
    const r = await db.query(`
      SELECT
        (SELECT COUNT(*) FROM form_sessions
           WHERE created_at >= NOW() - INTERVAL '${HEALTH_PARTIAL_WINDOW_H} hours'
             AND (user_agent IS NULL OR user_agent !~* $1))            AS sessions,
        (SELECT COUNT(*) FROM leads
           WHERE created_at >= NOW() - INTERVAL '${HEALTH_PARTIAL_WINDOW_H} hours') AS leads
    `, [BOT_RE]);
    const sessions = parseInt(r.rows[0].sessions) || 0;
    const leads    = parseInt(r.rows[0].leads)    || 0;
    const win      = HEALTH_PARTIAL_WINDOW_H + 'h';

    if (leads > 0) return hc('partial', 'green', leads + ' leads saved in the last ' + win, sessions + ' sessions in the same window');
    if (sessions >= HEALTH_MIN_SAMPLE) {
      return hc('partial', 'red', sessions + ' sessions, 0 leads in the last ' + win,
        'Visitors are reaching the form and nothing is being written. Check POST /partial.');
    }
    return hc('partial', 'insufficient_data', 'Quiet — ' + sessions + ' sessions, no leads in the last ' + win,
      'Below the ' + HEALTH_MIN_SAMPLE + '-session floor, so this is not evidence either way.');
  } catch (err) {
    return hc('partial', 'red', 'Could not check', err && err.message);
  }
}

/* ── PartnerStack — the money path ──────────────────────────────────
   System Health covered /partial, /submit, ELV, Apollo, RevenueHero, cron, the
   AWS mirror and email recovery — and nothing for the path that actually pays
   affiliates. Today's 400 on the qualification would have been caught here.

   RED ON ANY FAILURE IN THE WINDOW, deliberately, and this is a real
   trade-off: a single transient 4xx that the claim-release already recovered
   from will still turn the row red for 24 hours. That is the right way round
   for money — a failed conversion means an affiliate is not being paid and
   somebody should look — but it WILL fire on a blip, and if it becomes noisy
   the fix is to alert on unresolved failures rather than to raise the
   threshold. The failure stamps are cleared on a later success, so a
   self-healing failure does clear itself once that domain converts again.

   Green with zero activity is NOT claimed: with no partner traffic at all
   there is nothing to verify, so it reports insufficient_data rather than a
   green badge that means "we checked nothing". */
const HEALTH_PARTNERSTACK_WINDOW_H = 24;

async function checkPartnerStackHealth(db) {
  try {
    const r = await db.query(`
      SELECT
        COUNT(DISTINCT ps_customer_key) FILTER (
          WHERE ps_signup_sent_at >= NOW() - INTERVAL '${HEALTH_PARTNERSTACK_WINDOW_H} hours')    AS conversions,
        COUNT(DISTINCT ps_customer_key) FILTER (
          WHERE ps_qualified_sent_at >= NOW() - INTERVAL '${HEALTH_PARTNERSTACK_WINDOW_H} hours') AS qualifications,
        COUNT(DISTINCT ps_customer_key) FILTER (
          WHERE ps_signup_failed_at >= NOW() - INTERVAL '${HEALTH_PARTNERSTACK_WINDOW_H} hours')  AS signup_failures,
        COUNT(DISTINCT ps_customer_key) FILTER (
          WHERE ps_qualify_failed_at >= NOW() - INTERVAL '${HEALTH_PARTNERSTACK_WINDOW_H} hours') AS qualify_failures,
        COUNT(*) FILTER (
          WHERE ps_xid IS NOT NULL
            AND created_at >= NOW() - INTERVAL '${HEALTH_PARTNERSTACK_WINDOW_H} hours')           AS partner_leads
        FROM leads
    `);
    const q    = r.rows[0];
    const conv = parseInt(q.conversions)      || 0;
    const qual = parseInt(q.qualifications)   || 0;
    const f1   = parseInt(q.signup_failures)  || 0;
    const f2   = parseInt(q.qualify_failures) || 0;
    const lds  = parseInt(q.partner_leads)    || 0;
    const win  = HEALTH_PARTNERSTACK_WINDOW_H + 'h';
    const fails = f1 + f2;
    const summary = `${conv} conversions · ${qual} qualifications in the last ${win}`;

    if (fails > 0) {
      const parts = [];
      if (f1) parts.push(`${f1} conversion${f1 > 1 ? 's' : ''}`);
      if (f2) parts.push(`${f2} qualification${f2 > 1 ? 's' : ''}`);
      return hc('partnerstack', 'red', `${parts.join(' and ')} failed in the last ${win}`,
        summary + '. A failed conversion means the affiliate is not credited; a failed qualification means the $50 did not fire. Claims are released, so these retry.');
    }
    if (conv || qual || lds) return hc('partnerstack', 'green', summary, `${lds} partner lead${lds === 1 ? '' : 's'} in the same window, no failures`);
    return hc('partnerstack', 'insufficient_data', `No partner activity in the last ${win}`,
      'Nothing to verify — this is not evidence the money path works.');
  } catch (err) {
    return hc('partnerstack', 'red', 'Could not check', err && err.message);
  }
}

/* ── Step 2 — /submit ───────────────────────────────────────────────
   The old row asked whether there had EVER been a completion, so it went
   green in July and stayed there. This asks about the last 24 hours, and
   only calls it red when enough people reached step 1 for zero
   completions to mean something. */
async function checkSubmitHealth(db) {
  try {
    const r = await db.query(`
      SELECT
        (SELECT COUNT(*) FROM leads
           WHERE created_at >= NOW() - INTERVAL '${HEALTH_SUBMIT_WINDOW_H} hours')   AS step1,
        (SELECT COUNT(*) FROM leads
           WHERE submitted_at >= NOW() - INTERVAL '${HEALTH_SUBMIT_WINDOW_H} hours') AS completions
    `);
    const step1       = parseInt(r.rows[0].step1)       || 0;
    const completions = parseInt(r.rows[0].completions) || 0;
    const win         = HEALTH_SUBMIT_WINDOW_H + 'h';

    if (completions > 0) return hc('submit', 'green', completions + ' completions in the last ' + win, step1 + ' reached step 1 in the same window');
    if (step1 >= HEALTH_SUBMIT_MIN_STEP1) {
      return hc('submit', 'red', step1 + ' step-1 leads, 0 completions in the last ' + win,
        'People are entering an email and nobody is getting through step 2. Check POST /submit.');
    }
    return hc('submit', 'insufficient_data', 'Quiet — ' + step1 + ' step-1 leads, no completions in the last ' + win,
      'Below the ' + HEALTH_SUBMIT_MIN_STEP1 + '-lead floor, so this is not evidence either way.');
  } catch (err) {
    return hc('submit', 'red', 'Could not check', err && err.message);
  }
}

/* ── Apollo enrichment ──────────────────────────────────────────────
   Was a lifetime ratio of enrichment_data rows to leads rows: if Apollo
   died today, a year of good history kept it green indefinitely. Now
   scoped to a window, so yesterday cannot vouch for today. */
async function checkApolloHealth(db) {
  try {
    const r = await db.query(`
      SELECT
        (SELECT COUNT(*) FROM leads
           WHERE created_at >= NOW() - INTERVAL '${HEALTH_APOLLO_WINDOW_H} hours'
             AND email IS NOT NULL)                                                   AS leads,
        (SELECT COUNT(*) FROM enrichment_data
           WHERE enriched_at >= NOW() - INTERVAL '${HEALTH_APOLLO_WINDOW_H} hours')   AS enriched,
        (SELECT MAX(enriched_at) FROM enrichment_data)                                AS last_enriched
    `);
    const leads    = parseInt(r.rows[0].leads)    || 0;
    const enriched = parseInt(r.rows[0].enriched) || 0;
    const last     = r.rows[0].last_enriched ? new Date(r.rows[0].last_enriched).getTime() : null;
    const win      = HEALTH_APOLLO_WINDOW_H + 'h';
    const lastNote = last ? 'Last enrichment ' + fmtAge(Date.now() - last) + ' ago' : 'Nothing has ever been enriched';

    if (leads < HEALTH_MIN_SAMPLE) {
      return hc('apollo', 'insufficient_data', 'Quiet — ' + leads + ' leads in the last ' + win, lastNote);
    }
    const rate = Math.round(enriched / leads * 100);
    if (enriched === 0)  return hc('apollo', 'red',   '0 of ' + leads + ' enriched in the last ' + win, lastNote);
    if (rate >= 60)      return hc('apollo', 'green', rate + '% enriched in the last ' + win, enriched + ' of ' + leads + ' · ' + lastNote);
    if (rate >= 30)      return hc('apollo', 'amber', rate + '% enriched in the last ' + win, enriched + ' of ' + leads + ' · ' + lastNote);
    return hc('apollo', 'red', rate + '% enriched in the last ' + win, enriched + ' of ' + leads + ' · ' + lastNote);
  } catch (err) {
    return hc('apollo', 'red', 'Could not check', err && err.message);
  }
}

/* ── Booking — RevenueHero ──────────────────────────────────────────
   Also a lifetime ratio before, and inflated on both sides by webhook
   rows. Now a 7-day window over people, not sessions.

   Only ONE thing here is red: zero bookings recorded against a real
   sample of completions, which is what a dead booking webhook looks
   like. A merely low booking rate is a business outcome, not an outage,
   so it is amber and stays out of the alerting path. */
async function checkBookingHealth(db) {
  try {
    const r = await db.query(`
      SELECT
        COUNT(DISTINCT LOWER(email)) FILTER (
          WHERE submitted_at >= NOW() - INTERVAL '${HEALTH_BOOKING_WINDOW_D} days'
        ) AS completed_people,
        COUNT(DISTINCT LOWER(email)) FILTER (
          WHERE booking_uid IS NOT NULL
            AND COALESCE(booked_at, created_at) >= NOW() - INTERVAL '${HEALTH_BOOKING_WINDOW_D} days'
        ) AS booked_people
      FROM leads
      WHERE email IS NOT NULL
    `);
    const completed = parseInt(r.rows[0].completed_people) || 0;
    const booked    = parseInt(r.rows[0].booked_people)    || 0;
    const win       = HEALTH_BOOKING_WINDOW_D + 'd';

    if (completed < HEALTH_BOOKING_MIN_COMPLETED) {
      return hc('booking', 'insufficient_data', 'Quiet — ' + completed + ' people completed in the last ' + win,
        booked + ' bookings recorded in the same window');
    }
    const rate = Math.round(booked / completed * 100);
    if (booked === 0) {
      return hc('booking', 'red', '0 bookings in the last ' + win + ', from ' + completed + ' completions',
        'No booking has been recorded by any of the three routes. Check the RevenueHero and Cal webhooks.');
    }
    const text = rate + '% booked in the last ' + win;
    const detail = booked + ' of ' + completed + ' people';
    if (rate >= 50) return hc('booking', 'green', text, detail);
    return hc('booking', 'amber', text, detail);
  } catch (err) {
    return hc('booking', 'red', 'Could not check', err && err.message);
  }
}

/* ── Cron — drop-off recovery ───────────────────────────────────────
   The old row was green whenever pendingPartials was 0, which is exactly
   what a dead cron looks like on a quiet night: nothing arrives, nothing
   is pending, the row says fine. It could not go red at all.

   This reads the heartbeat timestamp instead. Pure function of the
   clock so it can be tested without a database.

   _lastCronRunAt is seeded to boot time, so "recent" alone would read
   green for three hours after every redeploy without the scheduler ever
   calling us. _cronRanThisProcess separates "has run" from "has just
   started", and the not-yet-run case is grey until the window elapses
   and then red — never green. */
function checkCronHealth(now, lastRunAt, ranThisProcess, startedAt) {
  const staleH = Math.round(CRON_STALE_MS / 3600000);
  if (!ranThisProcess) {
    const up = now - startedAt;
    if (up > CRON_STALE_MS) {
      return hc('cron', 'red', 'No run in ' + fmtAge(up) + ' since restart',
        'The scheduler has not called POST /cron/send-partials since this instance started. Drop-off follow-ups are not going out.');
    }
    return hc('cron', 'insufficient_data', 'No run yet — restarted ' + fmtAge(up) + ' ago',
      'Expected within ' + staleH + 'h of a restart.');
  }
  const age = now - lastRunAt;
  if (age > CRON_STALE_MS) {
    return hc('cron', 'red', 'Last run ' + fmtAge(age) + ' ago',
      'Expected within ' + staleH + 'h. Check the scheduler that calls POST /cron/send-partials.');
  }
  return hc('cron', 'green', 'Last run ' + fmtAge(age) + ' ago', 'Expected within ' + staleH + 'h.');
}

/* ── AWS sync — the mirror the dialer reads ─────────────────────────
   THIS FUNCTION RETURNS green OR red AND NOTHING ELSE. There is no code
   path to insufficient_data and there must never be one; a test asserts
   it, both by reading this source and by driving the function with a
   pool that throws.

   The reason is the whole point of the row. A mirror we cannot reach is
   indistinguishable from a mirror that has gone stale, and both starve
   the sdr-calling dialer with no error anywhere else in the system:
   syncToAWS is fire-and-forget at all five call sites and only
   console.warns what it drops. Grey here would render as a calm "we are
   not sure", next to eight rows where grey means "quiet night".

   Both zero IS green, and the text says why: we connected, we queried,
   and the two sides agree. That is a verified positive about the mirror,
   just not one about traffic. */
async function checkAwsHealth(awsDb, railwayDb) {
  if (!awsDb) {
    return hc('aws', 'red', 'Disabled — AWS_PG_HOST is not set',
      'No mirror is being written at all. The sdr-calling dialer reads gw_form_leads and will never see a new lead.');
  }
  let mirror, railway;
  try {
    [mirror, railway] = await withTimeout(Promise.all([
      awsDb.query(`
        SELECT
          COUNT(*) FILTER (WHERE created_at >= NOW() - INTERVAL '${HEALTH_AWS_WINDOW_H} hours') AS recent,
          MAX(updated_at) AS last_write
        FROM gw_form_leads
      `),
      railwayDb.query(`
        SELECT COUNT(*) AS recent
        FROM leads
        WHERE created_at >= NOW() - INTERVAL '${HEALTH_AWS_WINDOW_H} hours'
      `),
    ]), HEALTH_AWS_TIMEOUT_MS, 'AWS mirror query');
  } catch (err) {
    return hc('aws', 'red', 'Cannot reach the mirror',
      (err && err.message ? err.message + ' — ' : '') + 'A mirror we cannot query is indistinguishable from one that has stopped being written.');
  }

  const mirrorRows  = parseInt(mirror.rows[0].recent)  || 0;
  const railwayRows = parseInt(railway.rows[0].recent) || 0;
  const lastWrite   = mirror.rows[0].last_write ? new Date(mirror.rows[0].last_write).getTime() : null;
  const win         = HEALTH_AWS_WINDOW_H + 'h';
  const missing     = railwayRows - mirrorRows;
  const tolerance   = Math.max(HEALTH_AWS_DRIFT_MIN, Math.ceil(railwayRows * HEALTH_AWS_DRIFT_PCT));

  if (railwayRows === 0 && mirrorRows === 0) {
    return hc('aws', 'green', 'Reachable — no leads in the last ' + win + ' to mirror',
      lastWrite ? 'Last mirror write ' + fmtAge(Date.now() - lastWrite) + ' ago' : 'The mirror has never been written');
  }
  if (railwayRows > 0 && mirrorRows === 0) {
    return hc('aws', 'red', 'Mirror empty — ' + railwayRows + ' leads on Railway in the last ' + win + ', 0 mirrored',
      'Every mirror write in the window was lost. syncToAWS swallows its errors, so this is the only place it shows.');
  }
  if (missing > tolerance) {
    return hc('aws', 'red', missing + ' rows behind in the last ' + win,
      mirrorRows + ' mirrored of ' + railwayRows + ' on Railway · tolerance ' + tolerance);
  }
  return hc('aws', 'green', 'In sync — ' + mirrorRows + ' of ' + railwayRows + ' rows in the last ' + win,
    lastWrite ? 'Last mirror write ' + fmtAge(Date.now() - lastWrite) + ' ago' : null);
}

/* ── Email recovery ─────────────────────────────────────────────────
   Was d.loopsSent > 0 — a lifetime count, green forever after the first
   send in July.

   The predicate below deliberately MIRRORS the recovery cron's own
   SELECT, including the booked_at >= created_at test and including
   equals-false rather than IS NOT TRUE. That is not sloppiness inherited
   from the cron: this row asks whether the cron is clearing its own
   queue, so it has to count the same rows the cron would pick up. Widen
   it to IS NOT TRUE and a row with a null flag would count as stuck here
   while the cron never selects it, and the row would sit red forever.

   Stuck means eligible for longer than the cron's own staleness window,
   so a lead that crossed the 2h line a minute after the last run is not
   an incident. */
async function checkRecoveryHealth(db) {
  try {
    const r = await db.query(`
      SELECT
        (SELECT COUNT(*) FROM leads
           WHERE loops_sent = true
             AND created_at >= NOW() - INTERVAL '${HEALTH_RECOVERY_LOOKBACK_D} days') AS processed,
        (SELECT COUNT(*) FROM leads l
           WHERE l.email IS NOT NULL
             AND l.disqualified = false
             AND l.booking_uid IS NULL
             AND l.loops_sent = false
             AND l.created_at < NOW() - INTERVAL '${HEALTH_RECOVERY_STUCK_H} hours'
             AND NOT EXISTS (
               SELECT 1 FROM leads booked
               WHERE LOWER(booked.email) = LOWER(l.email)
                 AND booked.booking_uid IS NOT NULL
                 AND booked.booked_at >= l.created_at
             )) AS stuck
    `);
    const processed = parseInt(r.rows[0].processed) || 0;
    const stuck     = parseInt(r.rows[0].stuck)     || 0;

    if (stuck > 0) {
      return hc('recovery', 'red', stuck + ' waiting more than ' + HEALTH_RECOVERY_STUCK_H + 'h',
        'These are eligible for a follow-up and have not been picked up. Either the cron is not running or the sends are failing.');
    }
    if (processed > 0) {
      return hc('recovery', 'green', processed + ' follow-ups processed in the last ' + HEALTH_RECOVERY_LOOKBACK_D + 'd', 'Nothing stuck in the queue');
    }
    return hc('recovery', 'insufficient_data', 'No drop-offs to follow up in the last ' + HEALTH_RECOVERY_LOOKBACK_D + 'd', 'Nothing stuck in the queue');
  } catch (err) {
    return hc('recovery', 'red', 'Could not check', err && err.message);
  }
}

/* One place that runs them all. Each check already catches its own
   failure and reports red; the wrapper is a second net so one broken
   probe cannot take the other six down with it. */
function safeCheck(id, fn) {
  return Promise.resolve().then(fn).catch(err => hc(id, 'red', 'Could not check', err && err.message));
}

async function runHealthChecks() {
  const now = Date.now();
  const results = await Promise.all([
    safeCheck('partial',  () => checkPartialHealth(pool)),
    safeCheck('submit',   () => checkSubmitHealth(pool)),
    safeCheck('apollo',   () => checkApolloHealth(pool)),
    safeCheck('booking',  () => checkBookingHealth(pool)),
    safeCheck('cron',     () => checkCronHealth(now, _lastCronRunAt, _cronRanThisProcess, _processStartedAt)),
    safeCheck('aws',      () => checkAwsHealth(awsPool, pool)),
    safeCheck('recovery', () => checkRecoveryHealth(pool)),
    safeCheck('partnerstack', () => checkPartnerStackHealth(pool)),
  ]);
  const checks = {};
  for (const c of results) checks[c.id] = c;
  return { checks, generatedAt: new Date().toISOString() };
}

/* ── Alerting on state transitions ──────────────────────────────────
   Mirrors the ELV enter/exit pattern exactly, and goes through the same
   alertOps that already carries per-key cooldown and suppression
   counting. No second alerting system.

   Only green and red move state. amber and insufficient_data are inert
   on purpose: grey means "not enough evidence", and alerting on an
   absence of evidence is the same mistake as recording "we could not
   check" as "we checked and it is bad".

   Severity split is the owner's, Aug 2026: sessions arriving with zero
   leads means the form is broken at the very top, which is where most
   volume is already lost, so /partial and /submit page by email. The AWS
   mirror joins them because the sdr-calling dialer reads it and nothing
   else in the system notices when it stops. */
const HEALTH_SEVERITY = {
  partial:  'critical',
  submit:   'critical',
  aws:      'critical',
  apollo:   'warning',
  booking:  'warning',
  cron:     'warning',
  recovery: 'warning',
  /* WARNING, not critical, and that is a deliberate downgrade from the first
     draft. Critical pages by email, and the three rows that do are a recorded
     owner decision from Aug 2026 — adding a fourth was not asked for. The
     urgent notification already exists: recordPartnerStackFailure fires an
     alertOps at the MOMENT of failure, so this row is a backstop, not the
     primary signal. It will also go red on a single transient 4xx that the
     claim-release already recovered from, which is the right way round for
     money but the wrong thing to page someone about at 3am. */
  partnerstack: 'warning',
};

const HEALTH_ALERT_META = {
  partial:  { source: 'Step 1 /partial',  title: 'Leads are not being saved',        impact: 'Visitors are reaching the form and nothing is being written to Railway.', action: 'Check POST /partial in the Railway logs.' },
  submit:   { source: 'Step 2 /submit',   title: 'Nobody is completing the form',    impact: 'People are entering an email and none are getting through step 2.',      action: 'Check POST /submit in the Railway logs.' },
  aws:      { source: 'AWS sync',         title: 'Mirror is not being written',      impact: 'The sdr-calling dialer reads gw_form_leads and is not seeing new leads.', action: 'Check the AWS_PG_* credentials and that the host is reachable.' },
  apollo:   { source: 'Apollo',           title: 'Enrichment has stopped',           impact: 'New leads are arriving with no enrichment attached.',                    action: 'Check the Apollo API key and credit balance.' },
  booking:  { source: 'Booking',          title: 'No bookings are being recorded',   impact: 'People are completing the form and no booking is landing on any lead.',  action: 'Check the RevenueHero and Cal webhooks — booking arrives by three routes.' },
  cron:     { source: 'Recovery cron',    title: 'Has not run recently',             impact: 'Drop-off follow-up emails are not being sent.',                          action: 'Check the scheduler that calls POST /cron/send-partials.' },
  recovery: { source: 'Email recovery',   title: 'Follow-ups are stuck in the queue', impact: 'Leads are eligible for a follow-up and are not being picked up.',        action: 'Check the Gmail credentials and the cron logs.' },
  partnerstack: { source: 'PartnerStack', title: 'The money path is failing',        impact: 'A conversion or qualification did not land, so an affiliate is not being credited.', action: 'Check the [PartnerStack] lines in the Railway logs, and the Partner gaps card. Claims are released, so these retry.' },
};

const _healthState = new Map(); // check id -> the last state we reported on: 'green' or 'red'

function evaluateHealthAlerts(checks) {
  try {
    for (const id of Object.keys(checks || {})) {
      const c    = checks[id];
      const meta = HEALTH_ALERT_META[id];
      if (!c || !meta) continue;
      if (c.state !== 'green' && c.state !== 'red') continue;

      const prev = _healthState.get(id);
      if (prev === c.state) continue;   // unchanged — fires once, not once per poll
      _healthState.set(id, c.state);

      if (c.state === 'red') {
        alertOps(HEALTH_SEVERITY[id] || 'warning', meta.source, meta.title, {
          'Signal': c.text,
          'Detail': c.detail || '—',
          'Impact': meta.impact,
          'Action': meta.action,
        });
      } else if (prev === 'red') {
        /* Recovery is informational, so it goes straight to Slack rather
           than through alertOps — same as the ELV exit path, and for the
           same reason: a cooldown on a recovery message can swallow the
           one line that tells you the incident is over. */
        const heading = '✅ ' + meta.source + ' — recovered';
        sendOpsSlack([
          bHeader(heading),
          bDivider(),
          bFields([
            { label: 'Now', value: c.text },
            c.detail ? { label: 'Detail', value: c.detail } : null,
          ].filter(Boolean)),
          bContext('Severity: *info* · ' + etStamp()),
        ].filter(Boolean), heading);
        console.log('[health] ✅ ' + meta.source + ' recovered — ' + c.text);
      }
    }
  } catch (err) {
    console.warn('[health] alert evaluation error (ignored):', err && err.message);
  }
}

app.get('/monitor/health', async (req, res) => {
  const token = process.env.MONITOR_TOKEN;
  if (token && req.query.token !== token) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  try {
    const out = await runHealthChecks();
    res.json(out);
  } catch (err) {
    console.error('[/monitor/health]', err.message);
    res.status(500).json({ error: 'Health checks failed', detail: err.message });
  }
});


/* ── "Does this person hold a call slot?" ────────────────────────────
   Question 1 of the two in the Definitions section of CLAUDE.md, and the
   one with NO time comparison. An SDR ringing someone who already has a
   call on the calendar is wrong whether that call was booked yesterday or
   in May, so when they booked is irrelevant here.

   Do not reach for this at the recovery cron. That asks a different
   question — "does THIS session's drop-off still need an email" — where
   the time comparison is required and deliberate. The cron has a long
   comment saying so and a test guarding it.

   Takes the outer email expression because the two call sites alias the
   leads table differently. One definition so the SDR List and the "No
   booking yet" headline cannot drift into disagreeing about who has a
   booking while both claiming to exclude bookers. */
const noBookingAnywhereSql = (emailExpr) => `NOT EXISTS (
              SELECT 1 FROM leads booked
              WHERE LOWER(booked.email) = LOWER(${emailExpr})
                AND booked.booking_uid IS NOT NULL
            )`;

app.get('/monitor/metrics', async (req, res) => {
  const token = process.env.MONITOR_TOKEN;
  if (token && req.query.token !== token) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  try {
    const [totals, people, recovered, byDay, enrichCount, enrichCoverage, pendingPartials, noBooking, recent, today, topFunnel] = await Promise.all([
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
        /* "Form entries per day" — a deliberate ROW count over the leads table,
           people count and not a session count. Daily inbound volume is the
           question; deduping by email would flatten exactly the repeat-attempt
           spikes that make a bad day visible. Named as an exception in
           CLAUDE.md's Definitions so it doesn't read as a rule violation.

           Two fixes here beyond the ET move:
           1. generate_series zero-fills. Without it a day with no entries was
              ABSENT rather than zero, so the bars either side sat adjacent and
              a dead day looked like continuity. Same fix the lead-magnet chart
              already had.
           2. Exactly 14 ET calendar days. The old rolling NOW() minus 14 days
              window was 336 hours, so it drew 15 bars with the oldest one
              starting mid-day — a partial bucket that read as a real one. */
        SELECT to_char(d.day, 'Mon DD')  AS day_label,
               d.day                     AS day,
               COALESCE(COUNT(l.id), 0)::int AS count
        FROM generate_series(
               date_trunc('day', (NOW() AT TIME ZONE '${DASH_TZ}') - INTERVAL '13 days'),
               date_trunc('day', (NOW() AT TIME ZONE '${DASH_TZ}')),
               INTERVAL '1 day') AS d(day)
        LEFT JOIN leads l
          ON date_trunc('day', l.created_at AT TIME ZONE '${DASH_TZ}') = d.day
        GROUP BY d.day
        ORDER BY d.day ASC
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
            AND ${noBookingAnywhereSql('leads.email')}
          ORDER BY LOWER(email), created_at DESC
        ) deduped
      `),
      pool.query(`
        SELECT session_id, email, company, first_name, last_name,
               completed, booking_uid, disqualified, created_at, page_url
        FROM leads ORDER BY created_at DESC LIMIT 50
      `),
      pool.query(`SELECT COUNT(*) AS count FROM leads WHERE created_at >= NOW() - INTERVAL '24 hours'`),
      /* ── Top-of-funnel, for the Overview funnel widget ──
         Scoped to the SESSION-TRACKED WINDOW, not all time, and that is the
         whole point. form_sessions starts at go_live (21 Aug 2026 10:32 UTC);
         `leads` goes back to July. Comparing an all-time step-1 count against
         a since-August session count is what produced step1_rate = 266.7 on
         /monitor/funnel — the numerator outliving its denominator. So every
         stage here is measured from go_live forward, and when there is no
         go_live at all the route returns nulls rather than zeros: "not
         tracked" and "nobody converted" are different statements.

         UNIT CHANGE AT THE FIRST STAGE, stated on screen. Sessions are VISITS
         (form_sessions has no email, so it cannot be deduped to people);
         everything below is PEOPLE. Sessions -> Step 1 is therefore
         visits-to-people and is not a pure conversion rate. Same denominator
         /monitor/funnel uses, same caveat. */
      pool.query(`
        WITH gl AS (SELECT MIN(created_at) AS go_live FROM form_sessions),
        s AS (
          SELECT COUNT(*) FILTER (WHERE user_agent IS NULL OR user_agent !~* $1) AS sessions,
                 COUNT(*) FILTER (WHERE user_agent ~* $1)                        AS bot_sessions
            FROM form_sessions
        ),
        p AS (
          SELECT COUNT(DISTINCT LOWER(l.email))                                         AS step1,
                 COUNT(DISTINCT LOWER(l.email)) FILTER (WHERE l.completed IS TRUE)       AS completed,
                 COUNT(DISTINCT LOWER(l.email)) FILTER (WHERE l.booking_uid IS NOT NULL) AS booked
            FROM leads l CROSS JOIN gl
           WHERE l.email IS NOT NULL
             AND gl.go_live IS NOT NULL
             AND l.created_at >= gl.go_live
        )
        SELECT gl.go_live, s.sessions, s.bot_sessions, p.step1, p.completed, p.booked
          FROM gl CROSS JOIN s CROSS JOIN p
      `, [BOT_RE])
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

    /* Nulls, not zeros, when session tracking never ran for this period.
       A zero here reads as "nobody visited", which is a claim about demand;
       the truth is "we were not counting". Same rule /monitor/funnel applies
       to its orphan_leads and step1_rate. */
    const tf = topFunnel.rows[0] || {};
    const topFunnelOut = tf.go_live
      ? {
          coverage:    'full',
          since:       tf.go_live,
          sessions:    parseInt(tf.sessions)      || 0,
          botSessions: parseInt(tf.bot_sessions)  || 0,
          step1:       parseInt(tf.step1)         || 0,
          completed:   parseInt(tf.completed)     || 0,
          booked:      parseInt(tf.booked)        || 0,
        }
      : { coverage: 'none', since: null, sessions: null, botSessions: null,
          step1: null, completed: null, booked: null };

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
      topFunnel: topFunnelOut,
      leadsByDay,
      recentLeads: recent.rows, generatedAt: new Date().toISOString()
    });
  } catch (err) {
    console.error('[/monitor/metrics]', err.message);
    res.status(500).json({ error: 'Metrics query failed', detail: err.message });
  }
});

/* ── Top-of-funnel, day by day ────────────────────────────────────
   v5.8.0. Reads form_sessions (page loads) against leads (everything from
   step 1 onward). Nothing here existed before /session started writing, so
   days prior to that show sessions=0 — the go_live date below tells you
   where the real data starts, rather than leaving someone to conclude the
   site had no traffic in July.

   For the period BEFORE go-live, GA4 pageviews on /demo are the
   denominator. This route deliberately does not try to blend the two.

   ── Why step1_rate came back 266.7 ──
   Three separate ways the numerator can outlive its denominator. The first
   two are fixed below; the third is now measured rather than fixed,
   because it is a symptom and not something a read query can correct.

   1. PARTIAL FIRST DAY. Session recording began at 10:32 UTC; leads count
      from midnight. The zero-sessions guard handled "no denominator" but
      nothing handled "denominator covers nine fewer hours than the
      numerator". That day's rate is arithmetic on two different windows,
      so it is now null. Only the FIRST day is affected — for today, both
      counts run midnight-to-now and the coverage is symmetric.

   2. WEBHOOK LEADS HAVE NO SESSION, EVER. The RevenueHero and Cal
      safety-net branches create a `leads` row for someone who booked
      without the form: prefill_source is 'rh_webhook' or 'cal_webhook',
      the session_id is minted server-side, and no form_sessions row
      exists or ever will. They are real leads and they belong in the
      totals; they are not form starts and they inflate step1 forever.
      Excluded from step1, submitted AND booked — all three, because
      these rows arrive with submitted_at and booking_uid already set, so
      excluding them from step1 alone would push submit_rate past 100%
      and trade one wrong number for another. Reported separately as
      webhook_leads / webhook_booked.
      Only the fallback branches carry these values: when a webhook finds
      an existing lead it updates that row and never touches
      prefill_source, so a real form lead that books keeps its own.

   3. ORPHAN LEADS — reported, not fixed. saveSession() in both form files
      is fire-and-forget with a swallowed error and a short timeout, while
      /partial is awaited and visible to the visitor. So any page load
      where /session was dropped but the person went on to type an email
      leaves a lead with no session row: step1 inflated, permanently,
      silently. orphan_leads counts them. Near zero means the rate is
      trustworthy on covered days; anything else is the next thing to
      look at, and now it is visible instead of inferred from a number
      that looked impossible.

      ── and it shipped measuring the wrong thing ──
      First run: orphan_leads exactly equalled step1 on every day before
      go-live (29/29, 30/30, 26/26, 32/32, 33/33, 40/40, 8/8) and 198 of
      213 came from days when form_sessions had no rows at all. Of course
      they did — a lead cannot match a session row that was never
      written. The metric would have read 213 with nothing dropped, and
      grown with the window, which is the opposite of a health signal.
      That is the same error as recording "we could not check" as "we
      checked and it is bad", so it gets the same treatment the rate
      itself got — no coverage, no number. Null on 'none' days, and on
      the go-live day only leads created at or after the first session
      row are counted; the earlier ones could never have had one.
      Residual on the go-live day only: someone who loaded the page at
      10:30 and typed their email at 10:35 counts as an orphan and is not
      one. One day, a handful of rows, and visible as 'partial'.

   multi_page_sessions and bot_sessions do NOT get this treatment, and the
   difference is worth being precise about. They count rows that exist in
   form_sessions. On an uncovered day there are no such rows, so zero is
   the literal truth — the same zero `sessions` already reports, and it
   cannot be mistaken for evidence because it is an input, not a derived
   signal. orphan_leads was the other kind: a claim about leads MISSING a
   session row, which is only meaningful where sessions were being
   recorded at all.

   multi_page_sessions answers its own question: session_id lives in
   sessionStorage, so an ads-landing-page -> /demo journey in one tab
   reuses it and the /session upsert bumps hits instead of adding a row.
   hits > 1 is that journey, and it confirms the denominator is counting
   visitors rather than page loads. */
// Assumes the `leads` table is aliased `l`. Qualified on purpose: the query
// below joins form_sessions, and an unqualified column reference there is one
// added column away from being ambiguous.
const WEBHOOK_LEAD_SQL = "COALESCE(l.prefill_source, '') IN ('rh_webhook', 'cal_webhook')";

/* Obvious automated traffic, excluded at READ time so the rule can change
   without losing rows. Empty user agents are KEPT and counted separately — an
   empty UA is more often a privacy-hardened browser than a crawler, and
   throwing those away would quietly shrink the top of the funnel.

   Module scope so the Overview funnel and /monitor/funnel share ONE regex. A
   second copy would drift, and the two would then disagree about how many
   sessions exist while both claiming to exclude bots. */
const BOT_RE = "(bot|crawl|spider|slurp|headless|python-requests|curl|wget|monitor|preview|scrape|lighthouse|pingdom|semrush|ahrefs)";

app.get('/monitor/funnel', async (req, res) => {
  const token = process.env.MONITOR_TOKEN;
  if (token && req.query.token !== token) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  const days = Math.min(Math.max(parseInt(req.query.days, 10) || 30, 1), 180);
  try {
    const [byDay, totals] = await Promise.all([
      pool.query(`
        WITH s AS (
          /* ── THIS ROUTE STAYS ON UTC DAY BUCKETS. DELIBERATE. ──
             Everything else on the dashboard moved to America/New_York. This
             one did not, and must not be "brought into line" without redoing
             the coverage logic below.

             Why: every judgement in this query is anchored to a single UTC
             INSTANT — go_live, the moment session recording started (21 Aug
             2026, 10:32 UTC). session_coverage, the null-ing of step1_rate on
             a partial day, and the orphan_leads cutoff all ask "was recording
             already running at the START of this day". Re-bucketing to ET moves
             every day boundary 4-5 hours earlier, which changes WHICH day is
             the partial one and silently makes a covered day read partial (or
             worse, the reverse). The instant comparisons stay correct because
             pg hands both sides back as absolute times.

             Consequence to know when reading this next to the dashboard: this
             route's per-day rows will NOT line up with the Overview chart. That
             is expected, not a bug. Proper ET session charting is separate work
             and this route is not fetched by the UI today. */
          SELECT date_trunc('day', created_at) AS d,
                 COUNT(*) FILTER (WHERE user_agent IS NULL OR user_agent !~* $2) AS sessions,
                 COUNT(*) FILTER (WHERE user_agent ~* $2)                        AS bot_sessions,
                 -- Same bot exclusion as the sessions count, or the two
                 -- cannot be read against each other.
                 COUNT(*) FILTER (WHERE hits > 1 AND (user_agent IS NULL OR user_agent !~* $2)) AS multi_page_sessions
            FROM form_sessions
           WHERE created_at > NOW() - ($1 || ' days')::interval
           GROUP BY 1
        ),
        -- MIN(created_at) as a single-row CTE rather than a scalar subquery,
        -- because a subquery cannot be referenced from inside a FILTER
        -- clause. CROSS JOINed below: exactly one row (MIN over an empty
        -- table still returns one, holding NULL), so it cannot multiply
        -- anything.
        gl AS (SELECT MIN(created_at) AS go_live FROM form_sessions),
        l AS (
          // UTC day buckets on purpose — see the note on the sessions CTE above.
          // Both CTEs must use the SAME zone or the FULL OUTER JOIN on s.d = l.d
          // silently stops matching and every day splits into two half-rows.
          SELECT date_trunc('day', l.created_at) AS d,
                 COUNT(*) FILTER (WHERE l.email IS NOT NULL AND l.email <> '' AND NOT (${WEBHOOK_LEAD_SQL})) AS step1,
                 COUNT(*) FILTER (WHERE l.submitted_at IS NOT NULL AND NOT (${WEBHOOK_LEAD_SQL}))            AS submitted,
                 COUNT(*) FILTER (WHERE l.booking_uid IS NOT NULL AND NOT (${WEBHOOK_LEAD_SQL}))             AS booked,
                 COUNT(*) FILTER (WHERE ${WEBHOOK_LEAD_SQL})                                                 AS webhook_leads,
                 COUNT(*) FILTER (WHERE ${WEBHOOK_LEAD_SQL} AND l.booking_uid IS NOT NULL)                    AS webhook_booked,
                 -- A LEFT JOIN rather than NOT EXISTS inside FILTER, because
                 -- Postgres rejects a subquery there ("cannot use subquery in
                 -- FILTER"). form_sessions.session_id is UNIQUE, so the join
                 -- cannot multiply rows and the other counts above stay exact.
                 -- The go_live cutoff is what stops this counting leads from
                 -- before session tracking existed, which is every lead in
                 -- July and August up to 21 Aug 10:32.
                 COUNT(*) FILTER (
                   WHERE l.email IS NOT NULL AND l.email <> ''
                     AND NOT (${WEBHOOK_LEAD_SQL})
                     AND fs.session_id IS NULL
                     AND gl.go_live IS NOT NULL
                     AND l.created_at >= gl.go_live
                 ) AS orphan_leads
            FROM leads l
            CROSS JOIN gl
            -- Cast to text: form_sessions.session_id is TEXT while
            -- leads.session_id is declared UUID. The cast is a no-op if the
            -- live column is really TEXT, and it keeps the join working for
            -- the webhook rows whose ids are not UUID-shaped.
            LEFT JOIN form_sessions fs ON fs.session_id = l.session_id::text
           WHERE l.created_at > NOW() - ($1 || ' days')::interval
           GROUP BY 1
        )
        SELECT COALESCE(s.d, l.d) AS day,
               COALESCE(s.sessions, 0)             AS sessions,
               COALESCE(s.bot_sessions, 0)         AS bot_sessions,
               COALESCE(s.multi_page_sessions, 0)  AS multi_page_sessions,
               COALESCE(l.step1, 0)                AS step1,
               COALESCE(l.submitted, 0)            AS submitted,
               COALESCE(l.booked, 0)               AS booked,
               COALESCE(l.webhook_leads, 0)        AS webhook_leads,
               COALESCE(l.webhook_booked, 0)       AS webhook_booked,
               COALESCE(l.orphan_leads, 0)         AS orphan_leads
          FROM s FULL OUTER JOIN l ON s.d = l.d
         ORDER BY 1 DESC
      `, [String(days), BOT_RE]),
      pool.query(`SELECT MIN(created_at) AS go_live, COUNT(*) AS total_sessions FROM form_sessions`),
    ]);

    // Instant comparison, so no assumption about the database's timezone:
    // date_trunc bucketed each day in the server's zone and pg hands both
    // values back as absolute times.
    const goLive = totals.rows[0].go_live ? new Date(totals.rows[0].go_live).getTime() : null;

    const rows = byDay.rows.map((r) => {
      const sessions = Number(r.sessions), step1 = Number(r.step1);
      // Was session recording already running at the START of this day? If
      // not, the two counts cover different windows and their ratio is not
      // a conversion rate. sessions > 0 with an uncovered day is only ever
      // the go-live day itself.
      const covered  = goLive !== null && goLive <= new Date(r.day).getTime();
      const coverage = covered ? 'full' : (sessions > 0 ? 'partial' : 'none');
      return {
        day: r.day, sessions, bot_sessions: Number(r.bot_sessions),
        multi_page_sessions: Number(r.multi_page_sessions),
        step1, submitted: Number(r.submitted), booked: Number(r.booked),
        webhook_leads: Number(r.webhook_leads), webhook_booked: Number(r.webhook_booked),
        // Null on an uncovered day, for the same reason step1_rate is: with
        // no session rows at all, "leads with no session row" is every lead,
        // which is a fact about the tracking and not about dropped writes.
        // The SQL already excludes pre-go-live leads, so this would read 0 —
        // but 0 says "checked, nothing dropped", which we have not earned.
        orphan_leads: coverage === 'none' ? null : Number(r.orphan_leads),
        session_coverage: coverage,
        // Null, not 0 — "we have no usable denominator" is not "nobody
        // converted". Now nulled for partial coverage as well as none.
        step1_rate: covered && sessions > 0 ? +(step1 / sessions * 100).toFixed(1) : null,
        // Numerator and denominator both come from `leads`, so coverage is
        // symmetric and this was never affected by the first-day problem.
        submit_rate: step1 > 0 ? +(Number(r.submitted) / step1 * 100).toFixed(1) : null,
      };
    });

    const sum = (k) => rows.reduce((a, r) => a + r[k], 0);
    // Sum only the days where the number means something. A window total
    // built over nulls would be either NaN or a quiet zero, and a quiet zero
    // is the reading that gets trusted.
    const measured = rows.filter((r) => r.orphan_leads !== null);

    res.json({
      days,
      session_tracking_live_since: totals.rows[0].go_live,
      total_sessions_recorded: Number(totals.rows[0].total_sessions),
      note: 'sessions are only recorded from session_tracking_live_since onward; use GA4 /demo pageviews for earlier periods',
      rate_note: 'step1_rate is null unless session_coverage is "full" — a partially covered day divides two different windows. orphan_leads is null wherever session_coverage is "none": with no session rows to match against, every lead looks orphaned, so the count would measure when tracking started rather than whether writes are being dropped. On the "partial" go-live day only leads created at or after session_tracking_live_since are counted. webhook_leads (RevenueHero/Cal fallback rows) are excluded from step1, submitted and booked because they never touched the form. multi_page_sessions and bot_sessions are plain counts of recorded rows, so 0 on an uncovered day is literally true and is not nulled.',
      webhook_leads_in_window: sum('webhook_leads'),
      // Read these two together: a total of 0 over 1 measured day of 30 is
      // not the same statement as 0 over 30.
      orphan_leads_in_window:  measured.reduce((a, r) => a + r.orphan_leads, 0),
      orphan_leads_days_measured: measured.length,
      orphan_leads_days_in_window: rows.length,
      rows,
    });
  } catch (err) {
    console.error('[/monitor/funnel]', err.message);
    res.status(500).json({ error: err.message });
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
  const partner      = req.query.partner      || null;
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

  /* THE STAGE LADDER — the one in the Definitions section of CLAUDE.md.
     Four stages, mutually exclusive and exhaustive, resolved in priority
     order, so every lead is in exactly one and the four always sum to the
     total.

     What was here did not do that. 'disqualified' was a bare
     disqualified = true, so a lead who was disqualified AND booked appeared
     under both Booked and Disqualified. 'completed' did not exclude
     disqualified, so it overlapped too. And 'step1' was
     completed = false AND disqualified = false, which both let booked leads
     through and dropped every row where the flag is NULL rather than false —
     those rows fell out of all four filters and could not be found under any
     stage.

     Hence IS TRUE / IS NOT TRUE throughout, never = true / = false: a null
     flag on an old row has to land in a stage rather than vanishing. */
  if (stage === 'booked')       conditions.push('l.booking_uid IS NOT NULL');
  if (stage === 'disqualified') conditions.push('l.booking_uid IS NULL AND l.disqualified IS TRUE');
  if (stage === 'completed')    conditions.push('l.booking_uid IS NULL AND l.disqualified IS NOT TRUE AND l.completed IS TRUE');
  if (stage === 'step1')        conditions.push('l.booking_uid IS NULL AND l.disqualified IS NOT TRUE AND l.completed IS NOT TRUE');

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
  /* Partner as a DIMENSION on the existing view, not a tab of its own.
     '__any' is every partner-sourced lead; a specific value matches the key,
     the resolved name or the partner's email, because whoever is filtering
     may have any one of the three to hand and will not know which we
     resolved. */
  if (partner === '__any')      conditions.push(`l.ps_partner_key IS NOT NULL`);
  else if (partner === '__none') conditions.push(`l.ps_partner_key IS NULL`);
  else if (partner) {
    params.push(`%${partner.toLowerCase()}%`);
    const i = params.length;
    conditions.push(`(LOWER(COALESCE(l.ps_partner_key,'')) LIKE $${i} OR LOWER(COALESCE(l.ps_partner_name,'')) LIKE $${i} OR LOWER(COALESCE(l.ps_partner_email,'')) LIKE $${i})`);
  }
  if (repeatAttempts === 'yes') conditions.push(`EXISTS (SELECT 1 FROM leads pa WHERE LOWER(pa.email) = LOWER(l.email) AND pa.created_at < l.created_at)`);
  if (repeatAttempts === 'no')  conditions.push(`NOT EXISTS (SELECT 1 FROM leads pa WHERE LOWER(pa.email) = LOWER(l.email) AND pa.created_at < l.created_at)`);

  /* The picked date is an ET calendar day, so the boundary has to be ET
     midnight. `$n::date` alone produced a naive date that Postgres resolved in
     the session zone — Etc/UTC — putting the cut at 20:00 the previous evening
     ET. A lead at 21:00 ET on the 24th was labelled "24" in the table and
     returned by a "25" filter, and a "Today" filter dropped everything from
     20:00 the night before. Applying AT TIME ZONE to the naive timestamp
     INTERPRETS it as ET and hands back a timestamptz, which is the comparison
     we actually want. */
  if (dateFrom) { params.push(dateFrom); conditions.push(`l.created_at >= ($${params.length}::date::timestamp AT TIME ZONE '${DASH_TZ}')`); }
  if (dateTo)   { params.push(dateTo);   conditions.push(`l.created_at < (($${params.length}::date + INTERVAL '1 day')::timestamp AT TIME ZONE '${DASH_TZ}')`); }
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
      l.elv_status, l.elv_checked_at,
      l.ps_partner_key, l.ps_partner_name, l.ps_partner_email,
      l.ps_xid, l.ps_customer_key, l.ps_click_at, l.ps_click_history,
      l.ps_signup_sent_at, l.ps_signup_verified_at, l.ps_qualified_sent_at,
      l.ps_signup_skipped_reason, l.ps_signup_fail_reason, l.ps_qualify_fail_reason,
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
        'elv_status','unverifiable_pair',
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
        // Same derived flag as the JSON path, from the same function.
        ...allRows.rows.map(r => cols.map(c => escape(c === 'unverifiable_pair' ? isUnverifiablePair(r) : r[c])).join(','))
      ].join('\n');
      res.setHeader('Content-Type', 'text/csv');
      res.setHeader('Content-Disposition', `attachment; filename="leads-${etDateOnly()}.csv"`);
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

    /* Computed here, not in SQL and not in the browser: the rule lives in
       one place (isUnverifiablePair) and the dashboard just renders the
       answer. Duplicating the verdict list into the dashboard's JS string
       is exactly how the label map ended up with two copies. */
    const leadRows = leadsResult.rows.map(r => ({ ...r, unverifiable_pair: isUnverifiablePair(r) }));

    res.json({ total, page, pages: Math.ceil(total / limit), leads: leadRows });
  } catch (err) {
    console.error('[/monitor/leads]', err.message);
    res.status(500).json({ error: 'Query failed', detail: err.message });
  }
});

app.get('/monitor/filter-options', async (req, res) => {
  const token = process.env.MONITOR_TOKEN;
  if (token && req.query.token !== token) return res.status(401).json({ error: 'Unauthorized' });
  try {
    const [hearRows, sourceRows, partnerRows] = await Promise.all([
      pool.query(`SELECT hear_about_us AS v, COUNT(*) AS c FROM leads WHERE hear_about_us IS NOT NULL AND hear_about_us <> '' GROUP BY hear_about_us ORDER BY c DESC, hear_about_us ASC LIMIT 100`),
      pool.query(`SELECT utm_source AS v, COUNT(*) AS c FROM leads WHERE utm_source IS NOT NULL AND utm_source <> '' GROUP BY utm_source ORDER BY c DESC, utm_source ASC LIMIT 100`),
      /* One row per partner KEY, carrying the best name and email we have
         resolved for it. MAX() rather than a join: the name lands on later
         rows than the first lead from that partner, so grouping on the key and
         taking whatever resolved is what shows a name instead of a blank. */
      pool.query(`SELECT ps_partner_key AS k, MAX(ps_partner_name) AS n, MAX(ps_partner_email) AS e, COUNT(*) AS c
                    FROM leads WHERE ps_partner_key IS NOT NULL AND ps_partner_key <> ''
                   GROUP BY ps_partner_key ORDER BY c DESC LIMIT 100`)
    ]);
    res.json({
      hearAbout: hearRows.rows.map(r => r.v),
      utmSource: sourceRows.rows.map(r => r.v),
      partners:  partnerRows.rows.map(r => ({ key: r.k, name: r.n, email: r.e }))
    });
  } catch (err) {
    console.error('[/monitor/filter-options]', err.message);
    res.status(500).json({ error: 'Filter options query failed', detail: err.message });
  }
});

/* The four fields the SDR search matches on. The dashboard's client-side
   filter reads the same four out of SDR_SEARCH_FIELDS, and a test lifts both
   lists and asserts they are equal.

   Applied server-side ONLY for the CSV export. The table keeps its
   client-side filter: this query is unbounded and a round trip per keystroke
   would be worse than filtering rows already in the browser. Before this,
   exportSDR sent format=csv and nothing else, so someone who searched "acme",
   saw four rows and hit Export got the entire list. */
const SDR_SEARCH_COLUMNS = ['email', 'company', 'first_name', 'enriched_industry'];

app.get('/monitor/sdr', async (req, res) => {
  const token = process.env.MONITOR_TOKEN;
  if (token && req.query.token !== token) return res.status(401).json({ error: 'Unauthorized' });

  const format = req.query.format || 'json';
  const search = String(req.query.search || '').trim().toLowerCase();

  /* Filtered in an outer SELECT over the deduped set, not inside it. Pushing
     it in would change WHICH row survives DISTINCT ON per email, so a search
     could return a different row than the unsearched table shows for the same
     person. Parameterised, never interpolated. */
  const searchParams = [];
  let searchSql = '';
  if (search) {
    searchParams.push('%' + search + '%');
    const ph = '$' + searchParams.length;
    searchSql = 'WHERE (' + SDR_SEARCH_COLUMNS
      .map((c) => 'LOWER(COALESCE(' + c + ", '')) LIKE " + ph)
      .join(' OR ') + ')';
  }

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
          AND ${noBookingAnywhereSql('l.email')}
        ORDER BY LOWER(l.email), l.created_at DESC
      ) deduped
      ${searchSql}
      ORDER BY created_at DESC
    `, searchParams);

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
      res.setHeader('Content-Disposition', `attachment; filename="sdr-list-${etDateOnly()}.csv"`);
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
  '.an{background:#fafafa;color:#666;border:1px solid #eee}.ao{background:#f0fdf4;color:#15803d;border:1px solid #bbf7d0}.aw{background:#fffbeb;color:#b45309;border:1px solid #fde68a}.ae{background:#fef2f2;color:#b91c1c;border:1px solid #fecaca}' +
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
  '.psb{margin-bottom:12px;padding:10px;border:1px solid #ddd6fe;background:#faf8ff;border-radius:6px}' +
  '.psh{font-weight:600;font-size:12px;margin-bottom:6px;color:#5b21b6}' +
  '.psm{font-size:11px;color:#666;margin:8px 0 4px}' +
  '.pst{width:100%;border-collapse:collapse;font-size:11px}' +
  '.pst td{padding:2px 6px}' +
  '.pslost{color:#aaa}' +
  '.pswon{font-weight:600}' +
  '.psna{color:#999}' +
  '.pschip{display:inline-block;font-size:10px;font-weight:600;padding:2px 7px;border-radius:4px;background:#eef;color:#334;margin:2px 4px 2px 0;white-space:nowrap}' +
  '.pschip.bad{background:#fee2e2;color:#b91c1c}' +
  '.psattn{color:#b91c1c}' +
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
  '<div class="apill"><span class="dot" id="apidot"></span><span id="apist">Checking...</span></div>' +
  '<div class="apill" title="Every timestamp and every day boundary on this dashboard is US Eastern, and follows daylight saving automatically.">&#128340; All times ET</div></div>' +
  '<div style="display:flex;align-items:center;gap:10px"><span class="lu" id="lupd">&#8212;</span>' +
  '<button class="btn" onclick="loadAll()">&#8635; Refresh</button></div></div>' +
  '<div class="page">' +
  '<div class="tabs">' +
  '<div class="tab act" id="t-overview" onclick="showTab(\'overview\')">Overview</div>' +
  '<div class="tab" id="t-leads" onclick="showTab(\'leads\')">All Leads</div>' +
  '<div class="tab" id="t-sdr" onclick="showTab(\'sdr\')">SDR List</div>' +
  '<div class="tab" id="t-dupes" onclick="showTab(\'dupes\')" style="color:#aaa">Duplicates</div>' +
  '<div class="tab" id="t-lm" onclick="showTab(\'lm\')">Lead Magnet</div>' +
  '<div class="tab" id="t-partners" onclick="showTab(\'partners\')">Partners</div>' +
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
  '<div class="mc" title="Distinct qualified B2B people who COMPLETED the form and have no booking on any of their sessions. The SDR List is deliberately wider &#8212; it has no completed filter, so it also carries people who entered an email and never finished. Expect the SDR List to be the larger number."><div class="ml">No booking yet (SDR)</div><div class="mv" id="m-nb">&#8212;</div><div class="ms" id="m-nbs">&#8212;</div></div>' +
  '<div class="mc" title="People who completed the form without booking, and later booked on another session &#8212; your follow-up emails / prefill links / SDR nudges working."><div class="ml">Recovered bookings</div><div class="mv" id="m-rec">&#8212;</div><div class="ms">booked on a later session</div></div>' +
  '<div class="mc" title="Partner-referred leads that will never pay the affiliate unless someone acts. Two separate failures: no conversion was ever sent for that domain, or the demo happened and no Opportunity exists for an AE to mark qualified. Leads simply waiting on an AE are NOT counted &#8212; that is normal latency, not a gap."><div class="ml">Partner gaps</div><div class="mv" id="m-psgap">&#8212;</div><div class="ms" id="m-psgap-sub">unpaid partner referrals</div></div>' +
  '<div class="mc" title="Sessions older than 2 hours, not yet emailed, where nobody has booked with that address SINCE the session started. A booking made before the session does not count as resolving it &#8212; the person came back, started again and dropped again. That is why this number and the SDR List can disagree about the same address."><div class="ml">Pending recovery</div><div class="mv" id="m-pend">&#8212;</div><div class="ms">&gt;2h, no booking since the session</div></div>' +
  '<div class="mc" title="Sessions where the drop-off recovery email has been sent (loops_sent = true)."><div class="ml">Recovery emails sent</div><div class="mv" id="m-mail">&#8212;</div><div class="ms">follow-ups dispatched</div></div>' +
  '</div>' +
  '<div class="recon" id="recon">&#8212;</div>' +
  '<div class="g2">' +
  '<div><div class="sl">Alerts</div><div id="alerts"><div class="alertbox" style="background:#f5f5f5;color:#999;border:1px solid #eee">Loading...</div></div></div>' +
  '<div><div class="sl">Conversion funnel <span id="fnl-toggle" style="float:right;font-weight:400"></span></div><div class="card"><div id="funnel">Loading...</div></div></div>' +
  '</div>' +
  /* "Form entries" not "sessions": this counts rows in `leads`, i.e. everyone
     who reached step 1. It was labelled "sessions" while querying leads, which
     is a different table with a deliberately different meaning. */
  '<div class="sl">Form entries per day &#8212; last 14 days (ET)</div>' +
  '<div class="ms" style="margin:-6px 0 8px">One bar per person who reached step 1 that day. Not deduplicated &#8212; a repeat attempt counts again.</div>' +
  '<div class="card" style="margin-bottom:24px"><div class="cw"><canvas id="lchart"></canvas></div></div>' +
  '</div>' +
  '<div class="tp" id="tp-leads">' +
  '<div class="filters">' +
  '<input type="text" id="fsearch" placeholder="Search email, company..." oninput="debounce()">' +
  '<select id="fstage" onchange="loadLeads(1)"><option value="all">All stages</option><option value="booked">Booked</option><option value="completed">Completed (not booked, not disqualified)</option><option value="step1">Step 1 only</option><option value="disqualified">Disqualified (not booked)</option></select>' +
  '<select id="fsellto" onchange="loadLeads(1)"><option value="all">All sell-to</option><option value="B2B">B2B</option><option value="B2B (clarified from B2C)">B2B (clarified from B2C)</option><option value="B2B (clarified from Mixed)">B2B (clarified from Mixed)</option><option value="B2C">B2C</option><option value="Mixed">Mixed</option><option value="__clarified">Clarified (any)</option></select>' +
  '<select id="fsource" onchange="loadLeads(1)"><option value="all">All sources</option></select>' +
  '<select id="fenrich" onchange="loadLeads(1)"><option value="all">Enrichment: all</option><option value="yes">Enriched</option><option value="no">Not enriched</option></select>' +
  '<select id="fpartner" onchange="loadLeads(1)"><option value="all">Partner: all</option><option value="__any">Any partner</option><option value="__none">No partner</option></select>' +
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
  '<th class="sortable" onclick="sortBy(\'created_at\')">Created (ET) <span class="sar" id="sar-created_at"></span></th>' +
  '<th>Source</th>' +
  '<div id="psgapbox" style="display:none"></div>' +
  '</tr></thead><tbody id="ltbody"><tr><td colspan="10" class="nd">Loading leads...</td></tr></tbody></table></div></div>' +
  '<div class="pg" id="lpag"></div>' +
  '</div>' +
  '<div class="tp" id="tp-partners">' +
  '<div class="mgrid">' +
  '<div class="mc" title="Partner domains in a FAILED lifecycle state \u2014 a conversion or a qualification that did not land. This is the only number here that means someone has to do something today. Sums the two red states of the lifecycle ladder."><div class="ml">Needs attention</div><div class="mv" id="p-attn">&#8212;</div><div class="ms" id="p-attn-sub">failed conversions or qualifications</div></div>' +
  '<div class="mc" title="Every partner-sourced domain, in exactly one lifecycle state. The states always sum to this total."><div class="ml">Partner domains</div><div class="mv" id="p-domains">&#8212;</div><div class="ms" id="p-states">&#8212;</div></div>' +
  '<div class="mc" title="Salesforce state per partner domain, refreshed every 15 minutes for EVERY partner domain. &quot;Waiting on an AE&quot; is the row to action daily: the Opportunity exists but nobody has ticked Qualified_Demo__c, so the $50 cannot fire yet. &quot;Opportunity never created&quot; means sfopp failed and no AE can ever tick it."><div class="ml">Waiting on an AE</div><div class="mv" id="p-sfwait">&#8212;</div><div class="ms" id="p-sfstates">&#8212;</div></div>' +
  '<div class="mc" title="Distinct COMPANIES that arrived on a partner link. Companies, not people, so this reconciles with the funnel and the lifecycle states below."><div class="ml">Partner companies</div><div class="mv" id="p-leads">&#8212;</div><div class="ms" id="p-leads24">&#8212;</div></div>' +
  '<div class="mc" title="Distinct customer DOMAINS with a conversion sent. Per domain, not per person &#8212; PartnerStack counts one conversion per customer key, ever."><div class="ml">Conversions sent</div><div class="mv" id="p-conv">&#8212;</div><div class="ms">domains, one per customer</div></div>' +
  '<div class="mc" title="Distinct customer DOMAINS with a qualified_demo action sent. This is the event that pays the affiliate."><div class="ml">Qualified demos fired</div><div class="mv" id="p-qual">&#8212;</div><div class="ms">domains, one per customer</div></div>' +
  '</div>' +
  '<div class="card"><div class="ml" style="margin-bottom:8px">Per domain <span class="psna" style="font-weight:400;font-size:11px">&#8212; one row, one lifecycle state. The states above are counts of this column.</span></div>' +
  '<div style="overflow-x:auto"><table class="lt"><thead><tr><th>Domain</th><th>State</th><th>Partner</th><th>Salesforce</th><th>Detail</th><th>Last seen</th></tr></thead>' +
  '<tbody id="pdtbody"><tr><td colspan="6" class="nd">Loading...</td></tr></tbody></table></div></div>' +
  '<div class="card"><div class="ml" style="margin-bottom:8px">Per partner <span class="psna" style="font-weight:400;font-size:11px">&#8212; companies, not people. Click a row to see that partner\'s leads.</span></div>' +
  '<div class="psm">Clicks that never reached the form are NOT in our data at all &#8212; only PartnerStack has them. This funnel starts at step 1, not at the click.</div>' +
  '<div style="overflow-x:auto"><table class="lt"><thead><tr>' +
  '<th class="sortable" onclick="sortPartners(\'partner_name\')">Partner <span id="psar-partner_name"></span></th>' +
  '<th>Email</th><th>Key</th>' +
  '<th class="sortable" onclick="sortPartners(\'clicks\')" title="OUR clicks, from ps_click_history &#8212; counted as distinct xid, because the cookie is cumulative per visitor. Clicks that never reached the form are NOT in our data at all, only PartnerStack has those. This is the one column that is not a company count.">Clicks <span id="psar-clicks"></span></th>' +
  '<th class="sortable" onclick="sortPartners(\'step1\')" title="Companies that reached step 1. Every column in this funnel counts COMPANIES, so they nest: step 1 &#8805; completed &#8805; converted &#8805; booked &#8805; qualified.">Step 1 <span id="psar-step1"></span></th>' +
  '<th class="sortable" onclick="sortPartners(\'completed\')">Completed <span id="psar-completed"></span></th>' +
  '<th class="sortable" onclick="sortPartners(\'conversions\')">Converted <span id="psar-conversions"></span></th>' +
  '<th class="sortable" onclick="sortPartners(\'booked\')">Booked <span id="psar-booked"></span></th>' +
  '<th class="sortable" onclick="sortPartners(\'qualified\')">Qualified <span id="psar-qualified"></span></th>' +
  '<th class="sortable" onclick="sortPartners(\'last_click\')">Last click <span id="psar-last_click"></span></th>' +
  '</tr></thead><tbody id="ptbody"><tr><td colspan="10" class="nd">Loading...</td></tr></tbody></table></div></div>' +
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
  '<th style="width:30px"></th><th>Email</th><th>Name</th><th>Company</th><th>Title</th><th>Industry</th><th>Company Size</th><th>Stage</th><th>LinkedIn</th><th>Date (ET)</th>' +
  '</tr></thead><tbody id="sdr-tbody"><tr><td colspan="10" class="nd">Loading...</td></tr></tbody></table></div></div>' +
  '</div>' +
  '<div class="tp" id="tp-dupes">' +
  '<div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:16px">' +
  '<div><div class="sl" style="margin-bottom:2px">Duplicate Sessions</div><div style="font-size:12px;color:#888">Emails that appear in more than one session &#8212; sorted by session count</div></div>' +
  '<span id="dupes-count" style="font-size:12px;color:#888"></span>' +
  '</div>' +
  '<div class="card" style="padding:0;overflow:hidden"><div style="overflow-x:auto"><table><thead><tr>' +
  '<th style="width:30px"></th><th>Email</th><th>Sessions</th><th>Booked?</th><th>Completed?</th><th>First Seen (ET)</th><th>Last Seen (ET)</th>' +
  '</tr></thead><tbody id="dupes-tbody"><tr><td colspan="7" class="nd">Loading...</td></tr></tbody></table></div></div>' +
  '</div>' +
  '<div class="tp" id="tp-health">' +
  '<div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:8px">' +
  '<div class="sl" style="margin:0">Step health</div>' +
  '<button class="btn" onclick="checkHealth()">&#8635; Re-check</button></div>' +
  '<div class="card" style="margin-bottom:24px">' +
  '<div class="sr"><div><div class="sn">API uptime</div><div class="sd">/health responding</div></div><span class="badge bx" id="s-api">Checking...</span></div>' +
  '<div class="sr"><div><div class="sn">Step 1 &#8212; /partial</div><div class="sd">Leads written in the last 2 hours, against form sessions</div></div><span class="badge bx" id="s-partial">Checking...</span></div>' +
  '<div class="sr"><div><div class="sn">Step 2 &#8212; /submit</div><div class="sd">Completions in the last 24 hours</div></div><span class="badge bx" id="s-submit">Checking...</span></div>' +
  '<div class="sr"><div><div class="sn">ELV email verification</div><div class="sd">Inconclusive rate, rolling 90-minute window</div></div><span class="badge bx" id="s-elv">Checking...</span></div>' +
  '<div class="sr"><div><div class="sn">Apollo enrichment</div><div class="sd">Leads enriched in the last 24 hours</div></div><span class="badge bx" id="s-enrich">Checking...</span></div>' +
  '<div class="sr"><div><div class="sn">Booking &#8212; RevenueHero</div><div class="sd">People booked / people completed, last 7 days</div></div><span class="badge bx" id="s-cal">Checking...</span></div>' +
  '<div class="sr"><div><div class="sn">Cron &#8212; drop-off recovery</div><div class="sd">Time since the scheduler last called us</div></div><span class="badge bx" id="s-cron">Checking...</span></div>' +
  '<div class="sr"><div><div class="sn">AWS sync</div><div class="sd">gw_form_leads mirror, queried live against Railway</div></div><span class="badge bx" id="s-aws">Checking...</span></div>' +
  '<div class="sr"><div><div class="sn">PartnerStack</div><div class="sd">Conversions and qualifications in the last 24h &#8212; red on any failure</div></div><span class="badge bx" id="s-ps">Checking...</span></div>' +
  '<div class="sr"><div><div class="sn">Email recovery</div><div class="sd">Follow-up queue &#8212; anything stuck past 5 hours</div></div><span class="badge bx" id="s-loops">Checking...</span></div>' +
  '</div>' +
  '<div class="ms" style="margin:-16px 0 24px 2px"><span id="hupd">&#8212;</span> &#183; grey means there was not enough traffic to judge, never that a check was skipped.</div>' +
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
  '<th>Website</th><th>Source</th><th>Status</th><th>When (ET)</th><th></th>' +
  '</tr></thead><tbody id="lm-tbody"><tr><td colspan="10" class="nd">Loading...</td></tr></tbody></table></div></div>' +
  '<div class="ms" id="lm-count" style="margin-top:8px"></div>' +

  '</div>' +
  '</div>';

  const js = '<script>' +
  'var TP="' + tp + '";' +
  /* One definition of the dashboard zone for every client-side formatter.
     IANA name, never a fixed offset — 'EST' is wrong for eight months of the
     year and a hardcoded -05:00 shifts everything by an hour each March. */
  'var TZ="' + DASH_TZ + '";' +
  'var API=window.location.origin;' +
  'var lChart=null,curPage=1,stimer=null,curSort="created_at",curDir="desc",filterOptsLoaded=false;' +
  'function showTab(n){["overview","leads","sdr","dupes","health","lm","partners"].forEach(function(x){document.getElementById("t-"+x).classList.toggle("act",x===n);document.getElementById("tp-"+x).classList.toggle("act",x===n);});if(n==="leads"){loadFilterOptions();if(document.getElementById("ltbody").textContent.indexOf("Loading")>=0)loadLeads(1);}if(n==="partners"&&document.getElementById("ptbody").textContent.indexOf("Loading")>=0)loadPartners();if(n==="sdr"&&document.getElementById("sdr-tbody").textContent.indexOf("Loading")>=0)loadSDR();if(n==="dupes"&&document.getElementById("dupes-tbody").textContent.indexOf("Loading")>=0)loadDupes();if(n==="lm"&&document.getElementById("lm-tbody").textContent.indexOf("Loading")>=0)loadLM();if(n==="health")checkHealth();}' +
  'var WLBL={"nxdomain": "Domain doesn\'t exist \u2014 likely a typo", "no_dns_records": "Domain registered but nothing set up on it", "hosting_placeholder": "No website yet \u2014 domain points to a hosting setup page", "parked_confirmed": "Domain registered but no website on it", "parked": "Domain registered but no website on it", "parked_ns": "Domain registered but no website on it", "parked_suspect": "Looks like a parked domain \u2014 could not confirm", "for_sale_lander": "Domain is listed for sale", "marketplace_redirect": "Domain is for sale on a domain marketplace", "mailbox_domain": "Typed an email provider instead of their website", "brand_mismatch": "Typed a well-known brand\'s site, not their own", "social_profile_url": "Gave a social profile instead of a website", "thin_content": "Page looked mostly empty to us \u2014 worth a manual look", "thin_content_wildcard": "Page looked mostly empty to us \u2014 worth a manual look", "check_blocked": "Site blocked our check \u2014 the page itself looks fine", "dns_unresolved": "Could not look up the domain \u2014 DNS gave no answer", "forwarded_to_live_site": "Redirects to their live site \u2014 checked OK", "live_despite_dns_hint": "Live site (an early parking signal was overruled)", "mx_only": "Email-only company \u2014 no website, but mail works", "nxdomain_contradicted": "DNS blip \u2014 domain matches their verified email domain", "content_clean": "Live website", "resolved": "Domain resolves", "dns_indeterminate": "Could not reach the site to check it", "doh_error": "Could not reach the site to check it", "timeout": "Could not reach the site to check it", "unreachable": "Could not reach the site to check it", "non_html": "Address did not return a web page", "backend_error": "Our check errored \u2014 not the website\u2019s fault", "fetch_error": "Our check errored \u2014 not the website\u2019s fault", "skipped_no_backend": "Check was skipped", "skipped_unsafe_target": "Address pointed at an internal network \u2014 skipped", "test_email_skipped": "Internal test \u2014 check skipped", "ok": "Website checked OK"};' +
  'function wlabel(r){if(!r)return"Unknown";if(WLBL[r])return WLBL[r];if(String(r).indexOf("http_")===0){var c=String(r).slice(5);return ["999","403","401","429"].indexOf(c)>=0?("Site blocked our check ("+c+")"):("Site returned an error ("+c+")");}return String(r).replace(/_/g," ");}' +
  'function badge(id,text,cls){var el=document.getElementById(id);if(!el)return;el.textContent=text;el.className="badge "+cls;}' +
  'function set(id,v){var el=document.getElementById(id);if(el)el.textContent=v;}' +
  'function pct(a,b){return b?Math.round(a/b*100)+"%":"0%";}' +
  'function et(ts){if(!ts)return"\\u2014";return new Date(ts).toLocaleString("en-US",{timeZone:TZ,dateStyle:"short",timeStyle:"short"});}' +
  /* Calendar date in ET as YYYY-MM-DD. en-CA formats that way natively, which
     is what the date inputs and the CSV filename both want. */
  'function etDay(d){return new Intl.DateTimeFormat("en-CA",{timeZone:TZ,year:"numeric",month:"2-digit",day:"2-digit"}).format(d);}' +
  /* Shift by whole calendar days from TODAY IN ET. Anchored at noon UTC on
     purpose: shifting midnight by 24h multiples lands on the wrong date when
     it crosses a DST change, and the presets are the one place a reader would
     never notice being off by one. */
  'function etDayShift(n){var p=etDay(new Date()).split("-");var d=new Date(Date.UTC(+p[0],+p[1]-1,+p[2],12,0,0));d.setUTCDate(d.getUTCDate()+n);return d.toISOString().slice(0,10);}' +
  'function esc(s){if(!s)return"";return String(s).replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;").replace(/"/g,"&quot;");}' +
  'async function checkApi(){try{var r=await fetch(API+"/health",{signal:AbortSignal.timeout(5000)});if(r.ok){document.getElementById("apidot").className="dot dot-green";document.getElementById("apist").textContent="API online";badge("s-api","Online","bg");return true;}throw new Error("HTTP "+r.status);}catch(e){document.getElementById("apidot").className="dot dot-red";document.getElementById("apist").textContent="API offline";badge("s-api","Offline","br");return false;}}' +
  'async function checkElv(){try{var r=await fetch(API+"/monitor/elv-health"+TP,{signal:AbortSignal.timeout(5000)});if(!r.ok)throw new Error("HTTP "+r.status);var d=await r.json();var age=(d.minutesSinceLastCheck!=null&&d.minutesSinceLastCheck>=60)?" \\u00b7 last check "+Math.round(d.minutesSinceLastCheck/60)+"h ago":"";if(d.state==="degraded"){badge("s-elv","Degraded \\u2014 "+d.rate+"% of "+d.checks+" inconclusive"+age,"br");}else if(d.state==="insufficient_data"){if(d.rate>=50||d.consecutiveInconclusive>=2){badge("s-elv","Low traffic \\u2014 "+d.rate+"% of "+d.checks+" inconclusive"+age,"ba");}else{badge("s-elv","Quiet \\u2014 "+d.checks+" checks, "+d.rate+"% inconclusive"+age,"bx");}}else{badge("s-elv","Healthy ("+d.rate+"% inconclusive)","bg");}}catch(e){badge("s-elv","Could not check","br");}}' +
  /* SEVEN LIVE CHECKS, one round trip. Deliberately NOT on the 60-second
     loadAll poll: the AWS row queries a database across a WAN and would
     make every refresh wait on it. Runs at load, every five minutes, when
     the Health tab is opened, and on the Re-check button.

     A failed fetch paints every row RED, not grey. Grey on this tab means
     "not enough traffic to judge"; it must never also mean "we could not
     ask". Same rule the server side follows. */
  'var HCLS={green:"bg",amber:"ba",red:"br",insufficient_data:"bx"};' +
  'var HIDS={partial:"s-partial",submit:"s-submit",apollo:"s-enrich",booking:"s-cal",cron:"s-cron",aws:"s-aws",recovery:"s-loops",partnerstack:"s-ps"};' +
  'function hkeys(){return Object.keys(HIDS);}' +
  'function paintHealth(d){hkeys().forEach(function(k){var c=d&&d.checks&&d.checks[k];' +
  'if(!c||!HCLS[c.state]){badge(HIDS[k],"No result","br");return;}' +
  'var el=document.getElementById(HIDS[k]);if(el&&c.detail)el.title=c.detail;' +
  'badge(HIDS[k],c.text,HCLS[c.state]);});}' +
  'async function checkHealth(){' +
  'try{var r=await fetch(API+"/monitor/health"+(TP||"?")+(TP?"&":"")+"_="+Date.now(),{signal:AbortSignal.timeout(20000)});' +
  'if(!r.ok)throw new Error("HTTP "+r.status);var d=await r.json();paintHealth(d);' +
  'set("hupd","Checked "+new Date().toLocaleTimeString("en-US",{timeZone:TZ})+" ET");' +
  '}catch(e){hkeys().forEach(function(k){badge(HIDS[k],"Could not check","br");});' +
  'set("hupd","Health check unreachable: "+e.message);}}' +
  /* The Overview alerts panel is fed by five Overview metrics. When all
     five are quiet it used to render a green tick reading "All systems
     healthy." — a claim about the whole system made by a box that has
     never looked at one. Now it says what it actually knows and points at
     the tab that does the checking. */
  'function renderAlerts(d){var a=[];if(d.pendingPartials>0)a.push({c:"aw",i:"!",m:d.pendingPartials+" session(s) waiting >2 hours without booking \\u2014 recovery cron will pick them up."});if(d.noBookingUid>0)a.push({c:"aw",i:"!",m:d.noBookingUid+" people (deduped, qualified B2B) completed the form but have no booking on any session. The SDR List is wider still \\u2014 it does not filter on completed."});if(!d.awsSynced)a.push({c:"ae",i:"x",m:"AWS sync disabled \\u2014 AWS_PG_HOST is not set, so nothing is reaching the gw_form_leads mirror."});if(d.total>5&&d.enriched<d.total*0.3)a.push({c:"aw",i:"!",m:"Low enrichment rate ("+Math.round(d.enriched/d.total*100)+"% of sessions)."});if(d.todayCount===0)a.push({c:"aw",i:"o",m:"No new form entries in the last 24 hours."});if(a.length===0)a.push({c:"an",i:"\\u00b7",m:"Nothing flagged by the Overview metrics. Live service checks are on the System Health tab."});document.getElementById("alerts").innerHTML=a.map(function(x){return"<div class=\\"alertbox "+x.c+"\\"><span>"+x.i+"</span><span>"+x.m+"</span></div>";}).join("");}' +
  /* Four stages, one window, one top-of-funnel denominator.
     Was: four bars all measured as a % of step 1, with "disqualified" sitting
     below "booked" as if it were a later stage — it is not a stage, a
     disqualified person can also be booked, so the bars overlapped and did not
     sum. Disqualified moved out to its own card; the funnel is now a real
     ladder. Every bar is a % of the top so the widths are comparable.
     coverage==="none" renders "not tracked" rather than zeros. */
  'function fRow(label,unit,val,top,col,note,nullMsg){' +
  'if(val===null||val===undefined)return"<div class=\\"fr\\"><div class=\\"fl\\"><span>"+label+" <span style=\\"color:#aaa;font-size:11px\\">"+unit+"</span></span><span style=\\"color:#999\\">"+(nullMsg||"not tracked")+"</span></div><div class=\\"fb\\"></div></div>";' +
  'var p=(top&&top>0)?Math.round(val/top*100):0;' +
  'return"<div class=\\"fr\\"><div class=\\"fl\\"><span>"+label+" <span style=\\"color:#aaa;font-size:11px\\">"+unit+"</span>"+(note?" <span style=\\"color:#aaa;font-size:11px\\">"+note+"</span>":"")+"</span><span style=\\"font-weight:500\\">"+val+" <span style=\\"color:#aaa\\">("+p+"%)</span></span></div><div class=\\"fb\\"><div class=\\"ff\\" style=\\"width:"+p+"%;background:"+col+"\\"></div></div></div>";}' +
  /* TWO MODES, TWO DENOMINATORS, and the denominator is printed because the
     percentages are NOT comparable across modes. "Since 21 Aug" divides by
     sessions; "All time" divides by step 1, which is what the widget has
     always done. Someone reading 12% in one mode and 48% in the other is
     looking at the same people over different bases.

     Default is "Since 21 Aug" so the top-of-funnel loss is what you see
     first. All-time stays one click away because it is the only view that
     covers the full history (roughly March 2026 onward) and those are the
     numbers people already know.

     ALL-TIME IS BYTE-FOR-BYTE THE OLD CALCULATION: peopleTotal at 100%, the
     other two as a share of it, straight off the same payload fields. No new
     query, nothing re-derived, so the existing numbers still reconcile.
     Deliberately not "improved" while it was open. */
  'var funnelMode="tracked",lastMetrics=null;' +
  'function setFunnelMode(m){funnelMode=m;if(lastMetrics)renderFunnel(lastMetrics);}' +
  'function renderFunnelToggle(){var el=document.getElementById("fnl-toggle");if(!el)return;' +
  'el.innerHTML=[["tracked","Since 21 Aug"],["all","All time"]].map(function(o){var on=funnelMode===o[0];' +
  'return "<button onclick=\\"setFunnelMode(\'"+o[0]+"\')\\" style=\\"padding:3px 9px;margin-left:5px;border-radius:99px;font-size:11px;cursor:pointer;border:1px solid "+(on?"#1a1a1a":"#e5e5e5")+";background:"+(on?"#1a1a1a":"#fff")+";color:"+(on?"#fff":"#666")+"\\">"+o[1]+"</button>";}).join("");}' +
  'function renderFunnel(d){d=d||{};lastMetrics=d;var f=d.topFunnel||{};var html,sub;' +
  'if(funnelMode==="all"){' +
  'var t=d.peopleTotal;' +
  'html=fRow("Sessions","visits",null,null,"#a5b4fc","","not tracked before 21 Aug 2026")' +
  '+fRow("Entered step 1","people",t,t,"#818cf8","")' +
  '+fRow("Completed step 2","people",d.peopleCompleted,t,"#38bdf8","")' +
  '+fRow("Booked","people",d.peopleBooked,t,"#34d399","");' +
  'sub="All leads, full history (the form predates session tracking by about five months). <b>Percentages are % of step 1</b> \\u2014 not comparable with the Since-21-Aug view, which divides by sessions.";' +
  '}else{' +
  'var top=f.sessions;' +
  'html=fRow("Sessions","visits",f.sessions,top,"#a5b4fc",(f.botSessions?"&#183; "+f.botSessions+" bots excluded":""))' +
  '+fRow("Entered step 1","people",f.step1,top,"#818cf8","&#183; visits &rarr; people")' +
  '+fRow("Completed step 2","people",f.completed,top,"#38bdf8","")' +
  '+fRow("Booked","people",f.booked,top,"#34d399","");' +
  'sub=(f.coverage==="none")' +
  '?"Session tracking was not running for this period \\u2014 the top of the funnel cannot be measured. Switch to All time for the full history."' +
  ':"Since session tracking began, "+et(f.since)+". <b>Percentages are % of sessions</b> \\u2014 not comparable with the All-time view. Sessions are visits; the three stages below are distinct people, so the first rate compares visits to people.";' +
  '}' +
  'renderFunnelToggle();' +
  'document.getElementById("funnel").innerHTML=html+"<div class=\\"ms\\" style=\\"margin-top:10px\\">"+sub+"</div>";}' +
  'function renderChart(rows){var labels=(rows||[]).map(function(r){return r.day_label;}),data=(rows||[]).map(function(r){return parseInt(r.count)||0;});if(lChart)lChart.destroy();var ctx=document.getElementById("lchart").getContext("2d");lChart=new Chart(ctx,{type:"bar",data:{labels:labels,datasets:[{data:data,backgroundColor:"#818cf8",borderRadius:4,borderSkipped:false}]},options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false}},scales:{y:{beginAtZero:true,ticks:{stepSize:1,color:"#aaa"},grid:{color:"#f0f0f0"}},x:{ticks:{color:"#aaa",maxRotation:45,autoSkip:false},grid:{display:false}}}}});}' +
  'function stageBadge(l){if(l.booking_uid)return"<span class=\\"badge bg\\">Booked</span>";if(l.disqualified)return"<span class=\\"badge br\\">Disqualified</span>";if(l.completed)return"<span class=\\"badge bb\\">Completed</span>";return"<span class=\\"badge ba\\">Step 1</span>";}' +
  'function enrichBadge(l){return(l.enriched_title||l.enriched_company_size||l.e_company)?"<span class=\\"badge bg\\">Yes</span>":"<span class=\\"badge bx\\">No</span>";}' +
  /* The partner panel — rendered above the enrichment grid on a partner lead,
     and absent entirely otherwise so an organic lead looks exactly as today.
     ps_click_history is every partner click this visitor made, oldest first.
     Attribution uses the LAST click and only the last click, so that row is
     badged WON and the others greyed: the point of keeping the history is
     answering "why partner A and not B?" without opening the database. */
  'function psPanel(l){if(!l.ps_partner_key)return "";' +
  'function C(v){return "<code>"+esc(v)+"</code>";}' +
  /* name -> email -> raw key, the same chain Slack and hear_about_us use. */
  'var who=l.ps_partner_name?esc(l.ps_partner_name):(l.ps_partner_email?(esc(l.ps_partner_email)+" <span class=\'psna\'>(name not resolved)</span>"):(C(l.ps_partner_key)+" <span class=\'psna\'>(partner not resolved)</span>"));' +
  'var rows=[["Partner",who],' +
  '["Partner email",l.ps_partner_email?esc(l.ps_partner_email):null],' +
  '["Partner key",l.ps_partner_name?C(l.ps_partner_key):null],' +
  '["Clicked",l.ps_click_at?esc(et(l.ps_click_at)):null],' +
  '["Customer key",l.ps_customer_key?C(l.ps_customer_key):null],' +
  /* Sent and VERIFIED are different facts. "Sent" only means PartnerStack
     answered 200 to an empty-bodied endpoint; "verified" means we read the
     customer back. Showing sent alone is why the mirror question had to be
     answered from logs. And "not sent" now says WHY. */
  '["Conversion sent",l.ps_signup_sent_at?esc(et(l.ps_signup_sent_at)):("<span class=\'psna\'>not sent</span>"+(l.ps_signup_fail_reason?" <span class=\'psattn\'>("+esc(l.ps_signup_fail_reason)+")</span>":(l.ps_signup_skipped_reason?" <span class=\'psna\'>(skipped: "+esc(l.ps_signup_skipped_reason)+")</span>":"")))],' +
  '["Conversion verified",l.ps_signup_verified_at?esc(et(l.ps_signup_verified_at)):(l.ps_signup_sent_at?"<span class=\'psna\'>awaiting read-back</span>":null)],' +
  '["Qualified sent",l.ps_qualified_sent_at?esc(et(l.ps_qualified_sent_at)):(l.ps_qualify_fail_reason?("<span class=\'psattn\'>failed: "+esc(l.ps_qualify_fail_reason)+"</span>"):null)]' +
  '].filter(function(r){return r[1];});' +
  'var h="<div class=\'psb\'><div class=\'psh\'>\u{1F91D} Partner</div><div class=\'egrid\'>";' +
  'h+=rows.map(function(r){return "<div class=\'ef\'><div class=\'efl\'>"+r[0]+"</div><div>"+r[1]+"</div></div>";}).join("");' +
  'h+="</div>";' +
  'var hist=l.ps_click_history;if(typeof hist==="string"){try{hist=JSON.parse(hist);}catch(e){hist=null;}}' +
  'if(hist&&hist.length){' +
  'h+="<div class=\'psm\'>Click history ("+hist.length+", oldest first — the last click wins attribution)</div><table class=\'pst\'>";' +
  'h+=hist.map(function(c,i){var won=(i===hist.length-1);' +
  'return "<tr class=\'"+(won?"pswon":"pslost")+"\'>"' +
  '+"<td>"+(won?"<span class=\'badge bg\'>WON</span>":"")+"</td>"' +
  '+"<td>"+esc(c.at?et(c.at):"—")+"</td>"' +
  '+"<td>"+C(c.pk||"—")+"</td>"' +
  '+"<td class=\'psna\'>"+C(c.xid||"—")+"</td></tr>";}).join("");' +
  'h+="</table>";}' +
  'return h+"</div>";}' +
  'function enrichPanel(l){var loc=[l.enriched_city,l.enriched_state,l.enriched_country].filter(Boolean).join(", ");var fields=[' +
  '{g:1,lb:"Title",v:l.enriched_title},' +
  '{g:1,lb:"Seniority",v:l.enriched_seniority},' +
  '{g:1,lb:"Department",v:l.enriched_departments},' +
  '{g:1,lb:"Email status",v:l.enriched_email_status},' +
  '{g:1,lb:"Company",v:l.company||l.e_company},' +
  '{g:1,lb:"Company size",v:l.enriched_company_size},' +
  '{g:1,lb:"Industry",v:l.enriched_industry},' +
  '{g:1,lb:"Founded",v:l.enriched_founded_year},' +
  '{g:1,lb:"Annual revenue",v:l.enriched_annual_revenue},' +
  '{g:1,lb:"Total funding",v:l.enriched_total_funding},' +
  '{g:1,lb:"Funding stage",v:l.enriched_funding_stage},' +
  '{g:1,lb:"Funding events",v:l.enriched_funding_events},' +
  '{g:1,lb:"Alexa rank",v:l.enriched_alexa_ranking},' +
  '{g:1,lb:"Keywords",v:l.enriched_keywords},' +
  '{g:1,lb:"Person location",v:loc||null},' +
  '{g:1,lb:"Company HQ",v:l.enriched_org_hq},' +
  '{g:1,lb:"LinkedIn",v:l.enriched_linkedin,lnk:true},' +
  '{g:1,lb:"Phone",v:l.e_phone||l.phone},' +
  '{g:1,lb:"Website",v:l.website,lnk:true},' +
  '{g:1,lb:"\\u26A0\\uFE0F Website check",v:l.website_check_failed?wlabel(l.website_check_reason):null},' +
  '{g:1,lb:"\\u2139\\uFE0F Website check",v:(!l.website_check_failed&&l.website_check_reason&&l.website_check_reason!=="social_profile_url"&&["content_clean","resolved","ok","test_email_skipped"].indexOf(l.website_check_reason)===-1)?wlabel(l.website_check_reason):null},' +
  '{g:1,lb:"\\uD83D\\uDD17 Website type",v:(!l.website_check_failed&&l.website_check_reason==="social_profile_url")?"Social profile (no company site)":null},' +
  // The flag arrives pre-computed as l.unverifiable_pair. Deliberately not
  // re-derived here from elv_status + website_check_reason: that would put a
  // second copy of the verdict list in a JS string, which is exactly the
  // trap the label map fell into.
  '{g:1,lb:"\\u26A0\\uFE0F Nothing verified",v:l.unverifiable_pair?"Catch-all email, unreachable website. Nothing confirmed this lead.":null},' +
  '{g:1,lb:"Email check",v:l.elv_status},' +
  '{g:1,lb:"\\uD83D\\uDD01 Attempts",v:(Number(l.prior_attempts)>0)?("Attempt "+(Number(l.prior_attempts)+1)+" \\u2014 "+l.prior_attempts+" prior"+(Number(l.prior_disqualified)>0?", "+l.prior_disqualified+" disqualified":"")):null},' +
  '{g:1,lb:"Hear about us",v:l.hear_about_us},' +
  '{g:2,lb:"UTM source",v:l.utm_source},' +
  '{g:2,lb:"UTM medium",v:l.utm_medium},' +
  '{g:2,lb:"UTM campaign",v:l.utm_campaign},' +
  '{g:2,lb:"Referrer",v:l.referrer},' +
  '{g:2,lb:"Prefill",v:l.prefill_source},' +
  '{g:2,lb:"UTM term",v:l.utm_term},' +
  '{g:2,lb:"\\uD83D\\uDEEC Landing Page",v:l.landing_page,lnk:true},' +
  '{g:2,lb:"\\u2B05\\uFE0F Previous Page",v:l.previous_page,lnk:true},' +
  '{g:2,lb:"\\uD83D\\uDCC4 Form Page",v:l.page_url,lnk:true},' +
  '{g:3,lb:"Meta fbc",v:l.fbc},' +
  '{g:3,lb:"Meta fbp",v:l.fbp},' +
  '{g:2,lb:"Submitted",v:et(l.submitted_at)},' +
  '{g:2,lb:"Booked at",v:et(l.booked_at)},' +
  '{g:2,lb:"Meeting",v:l.start_time?et(l.start_time):null},' +
  '{g:2,lb:"Email sent",v:l.loops_sent?"Yes":"No"},' +
  '{g:3,lb:"Session ID",v:l.session_id,mono:true},' +
  '{g:3,lb:"Enriched at",v:et(l.enriched_at)}' +
  '].filter(function(f){return f.v;});' +
  'var pp=psPanel(l);' +
  'if(!fields.length)return pp||"<div style=\\"color:#999;font-size:12px\\">No enrichment data.</div>";' +
  /* Grouped rather than one flat run of ~20 fields. Partner first, because on
     a partner lead that is the thing that changes who owns the conversation.
     A field with no group falls into Form data rather than disappearing. */
  'var GRP=[[1,"Form &amp; enrichment"],[2,"Journey &amp; attribution"],[3,"Technical"]];' +
  'function efCell(f){var val=f.lnk&&f.v?"<a href=\\""+(f.v.startsWith("http")?"":"https://")+esc(f.v)+"\\" target=\\"_blank\\">"+esc(f.v)+"</a>":f.mono?"<code style=\\"font-size:10px\\">"+esc(f.v)+"</code>":esc(f.v);return "<div class=\\"ef\\"><div class=\\"efl\\">"+f.lb+"</div><div class=\\"efv\\">"+val+"</div></div>";}' +
  'var out=pp;' +
  'GRP.forEach(function(g){var inGroup=fields.filter(function(f){return (f.g||1)===g[0];});' +
  'if(!inGroup.length)return;' +
  'out+="<div class=\\"psm\\">"+g[1]+"</div><div class=\\"egrid\\">"+inGroup.map(efCell).join("")+"</div>";});' +
  'return out;}' +
  /* Loaded on its OWN cadence, not the 60-second metrics poll: check B hits
     Salesforce across the network, exactly like /monitor/health. At load, then
     every 10 minutes, matching the server-side cache. */
  'async function loadPartnerGaps(){try{' +
  'var r=await fetch(API+"/monitor/partner-gaps"+(TP||"?")+(TP?"&":"")+"_="+Date.now(),{signal:AbortSignal.timeout(30000)});' +
  'if(!r.ok)throw new Error("HTTP "+r.status);var d=await r.json();' +
  'var mc=(d.missedConversions||[]).length,mo=(d.missingOpportunity||[]).length,tot=mc+mo;' +
  'var el=document.getElementById("m-psgap"),sub=document.getElementById("m-psgap-sub");' +
  /* Salesforce unreachable must not read as "no gaps". A number we could not
     compute is shown as unknown, never as zero. */
  'var sfBad=d.opportunityCheck&&!d.opportunityCheck.ok;' +
  'if(el)el.textContent=sfBad?(mc+"+?"):String(tot);' +
  /* Three distinct states, three distinct labels. "0 no Opportunity" used to
     render identically whether Salesforce came back clean or was never asked. */
  'var oc=d.opportunityCheck||{};' +
  'var oppTxt=sfBad?"Opportunity check unavailable":(oc.checked===false?"none eligible to check yet":(mo+" no Opportunity of "+(oc.candidates||0)+" checked"));' +
  'if(sub)sub.textContent=mc+" no conversion · "+oppTxt;' +
  'var box=document.getElementById("psgapbox");if(!box)return;' +
  'if(!tot&&!sfBad){box.style.display="none";box.innerHTML="";return;}' +
  'var h="<div class=\'card\' style=\'margin-bottom:16px\'><div class=\'ml\' style=\'margin-bottom:8px\'>\u{1F91D} Partner revenue gaps</div>";' +
  'if(sfBad)h+="<div style=\'font-size:12px;color:#b91c1c;margin-bottom:8px\'>Opportunity check unavailable ("+esc(d.opportunityCheck.reason||"unknown")+"). This is NOT a clean result — the second check did not run.</div>";' +
  'function tbl(title,rows,note){if(!rows.length)return "";' +
  'var t="<div class=\'psm\'>"+title+" ("+rows.length+")"+(note?" — "+note:"")+"</div><table class=\'pst\'>";' +
  't+=rows.map(function(g){return "<tr><td><code>"+esc(g.customer_key)+"</code></td>"' +
  '+"<td>"+esc(g.partner_name||g.partner_key||"—")+"</td>"' +
  '+"<td>"+esc(g.email||"—")+"</td>"' +
  '+"<td class=\'psna\'>"+esc(et(g.met_at||g.first_seen))+"</td></tr>";}).join("");' +
  'return t+"</table>";}' +
  'h+=tbl("No conversion sent",d.missedConversions||[],"the affiliate gets nothing, and the $50 can never fire either");' +
  'h+=tbl("Demo happened, no Opportunity exists",d.missingOpportunity||[],"sfopp gap — no AE can mark these qualified, so the $50 never fires");' +
  /* Reported, not alarming: these are the system working correctly. */
  'if((d.skipped||[]).length)h+="<div class=\'psm\'>Skipped, correctly ("+d.skipped.length+") — not counted as gaps: "+d.skipped.map(function(x){return esc(x.customer_key)+" ("+esc(x.reason||"?")+")";}).join(", ")+"</div>";' +
  'if(d.awaitingQualification)h+="<div class=\'psna\' style=\'font-size:11px;margin-top:6px\'>"+d.awaitingQualification+" partner demo(s) past the "+d.graceDays+"-day mark in total; the ones with an Opportunity are waiting on an AE and are not listed.</div>";' +
  'box.innerHTML=h+"</div>";box.style.display="block";' +
  '}catch(e){var el2=document.getElementById("m-psgap");if(el2)el2.textContent="?";' +
  'var s2=document.getElementById("m-psgap-sub");if(s2)s2.textContent="could not check";}}' +
  /* Partners tab. Sorting is client-side over a single fetched page, like the
     SDR list — there are tens of partners, not thousands, and a round trip per
     column click would be worse than useless. */
  'var partnerRows=[],pSort="step1",pDir="desc";' +
  'async function loadPartners(){try{' +
  'var r=await fetch(API+"/monitor/partners"+(TP||"?")+(TP?"&":"")+"_="+Date.now(),{signal:AbortSignal.timeout(20000)});' +
  'if(!r.ok)throw new Error("HTTP "+r.status);var d=await r.json();var t=d.totals||{};' +
  'set("p-leads",t.leads);set("p-leads24",(t.leads24h||0)+" in the last 24h");' +
  'set("p-conv",t.conversions);set("p-qual",t.qualified);' +
  /* The ladder. Every figure here is a sum of ONE column, so the chips always
     add up to the domain total and cannot disagree with the cards above. */
  'var lc=d.lifecycle||{};var bs=lc.byState||{};var failed=lc.failedStates||[];' +
  'var attn=document.getElementById("p-attn");' +
  'if(attn){attn.textContent=String(lc.needsAttention||0);attn.className="mv"+((lc.needsAttention||0)>0?" psattn":"");}' +
  'set("p-attn-sub",(lc.needsAttention||0)>0?"act on these today":"nothing failing");' +
  'set("p-domains",lc.totalDomains||0);' +
  'var order=["qualified","qualification_failed","conversion_failed","demo_done_not_qualified","awaiting_demo","converted","skipped","conversion_pending"];' +
  'var lbl={qualified:"qualified",qualification_failed:"qualification failed",conversion_failed:"conversion failed",demo_done_not_qualified:"demo done, not qualified",awaiting_demo:"awaiting demo",converted:"converted",skipped:"skipped",conversion_pending:"pending"};' +
  'var chips=order.filter(function(k){return bs[k];}).map(function(k){' +
  'return "<span class=\'pschip"+(failed.indexOf(k)>=0?" bad":"")+"\'>"+bs[k]+" "+lbl[k]+"</span>";}).join("");' +
  /* The unit seam, stated on screen rather than left in a comment: everything
     else on this tab counts COMPANIES, this one counts leads, because a lead
     with no usable domain cannot be keyed by one. */
  'if(lc.noCustomerKeyLeads)chips+="<span class=\'pschip\' title=\'Counted as LEADS, not companies \\u2014 these have no usable domain to key by, so they cannot appear in the domain states above.\'>"+lc.noCustomerKeyLeads+" no customer key (leads, not companies)</span>";' +
  'var se=document.getElementById("p-states");if(se)se.innerHTML=chips||"no partner domains yet";' +
  /* Salesforce state. Read from the table the poller owns; a domain with no
     row simply has not been checked yet, and says so rather than implying a
     clean result. */
  'var sf=lc.bySfState||{};set("p-sfwait",lc.sfActionable||0);' +
  /* "will fire" was wrong for a domain whose $50 has already landed. */
  'var sfl={exists_unticked:"waiting on an AE",create_errored:"Opportunity never created",no_opportunity:"no Opportunity yet"};' +
  'var sfo=["exists_unticked","create_errored","ticked","no_opportunity"];' +
  'var dl0=lc.domains||[];' +
  'var firedN=dl0.filter(function(x){return x.sf_state==="ticked"&&x.qualified_sent;}).length;' +
  'var tickedN=(sf.ticked||0)-firedN;' +
  'var sfc=sfo.filter(function(k){return sf[k];}).map(function(k){' +
  'if(k==="ticked"){var out="";' +
  'if(firedN)out+="<span class=\'pschip\'>"+firedN+" ticked, $50 fired</span>";' +
  'if(tickedN>0)out+="<span class=\'pschip\'>"+tickedN+" ticked, will fire next poll</span>";' +
  'return out;}' +
  /* Only the actionable ones read as an action; the rest are informational,
     because a qualification cannot succeed where no conversion was sent. */
  'if(k==="exists_unticked"){var out2="";' +
  'if(lc.sfActionable)out2+="<span class=\'pschip\'>"+lc.sfActionable+" waiting on an AE</span>";' +
  'if(lc.sfUnactionable)out2+="<span class=\'pschip\' title=\'An Opportunity exists and is unticked, but no conversion was sent for this domain, so a qualification could not succeed. Not an action item.\'>"+lc.sfUnactionable+" unticked, no conversion sent (not actionable)</span>";' +
  'return out2;}' +
  'return "<span class=\'pschip"+(k==="create_errored"?" bad":"")+"\'>"+sf[k]+" "+sfl[k]+"</span>";}).join("");' +
  'var unchecked=(lc.totalDomains||0)-Object.values(sf).reduce(function(a,b){return a+b;},0);' +
  'if(unchecked>0)sfc+="<span class=\'pschip\' title=\'The poller has not checked these yet. NOT the same as having no Opportunity.\'>"+unchecked+" not checked yet</span>";' +
  /* The completeness signals, on screen. Every one of these was already
     computed somewhere and thrown away; a number nobody can audit is a number
     that will be wrong silently. */
  'var lr=lc.sfLastRead||{};' +
  'if(lr.ok===false)sfc+="<span class=\'pschip bad\' title=\'The Opportunity list could not be read completely, so these states are stale. Nothing was overwritten with a guess.\'>Salesforce read FAILED ("+esc(lr.reason||"unknown")+")</span>";' +
  'else if(lr.ok===true)sfc+="<span class=\'pschip\' title=\'Every Opportunity in the window was read, across paged requests. If this is ever short of the total, the refresh refuses to write rather than reporting a partial list as complete.\'>read "+lr.records+"/"+lr.totalSize+" opportunities · "+lr.pages+" pages</span>";' +
  /* A frozen checked_at is what a failed refresh looks like from here. */
  'if(lc.sfNewestCheckedAt){var ageMin=Math.round((Date.now()-new Date(lc.sfNewestCheckedAt).getTime())/60000);' +
  'if(ageMin>=(lc.sfStaleAfterMin||45))sfc+="<span class=\'pschip bad\' title=\'The newest Salesforce check is older than the refresh interval allows. The states above are stale and a refresh is probably failing.\'>STALE \u2014 last checked "+ageMin+" min ago</span>";' +
  'else sfc+="<span class=\'pschip\'>checked "+ageMin+" min ago</span>";}' +
    'if(lc.domainsCapped)sfc+="<span class=\'pschip bad\' title=\'More partner domains exist than this view returns. The states above are a page, not the population.\'>capped at "+lc.domainsLimit+" domains</span>";' +
  'var sfe=document.getElementById("p-sfstates");if(sfe)sfe.innerHTML=sfc||"not checked yet";' +
  'partnerRows=d.partners||[];renderPartners();' +
  /* The per-domain table. Same source as the chips above, so a domain cannot
     appear in one and not the other. */
  'var dtb=document.getElementById("pdtbody");' +
  'if(dtb){var dl=lc.domains||[];' +
  'if(!dl.length){dtb.innerHTML="<tr><td colspan=\'6\' class=\'nd\'>No partner domains yet.</td></tr>";}else{' +
  /* Per ROW, so it can say whether the $50 actually landed — the summary chip
     alone was not enough, this is the line you read for a specific domain.
     Also distinguishes an unticked Opportunity that could never be qualified
     because no conversion was sent. */
  'function sfLabel(x){var st=x.sf_state;' +
  'if(st==="ticked")return x.qualified_sent?"ticked, $50 fired":"ticked, will fire next poll";' +
  'if(st==="exists_unticked")return x.signup_sent?"waiting on an AE":"unticked \u2014 no conversion sent, not actionable";' +
  'if(st==="create_errored")return "Opportunity never created";' +
  'if(st==="no_opportunity")return "no Opportunity yet";' +
  'return st||"not checked yet";}' +
  'dtb.innerHTML=dl.map(function(x){' +
  'var bad=(failed.indexOf(x.state)>=0);' +
  'var det=x.signup_fail_reason||x.qualify_fail_reason||x.skipped_reason||"";' +
  'var sfs=x.sf_state?sfLabel(x):"not checked yet";' +
  'return "<tr><td><code>"+esc(x.customer_key)+"</code></td>"' +
  '+"<td><span class=\'pschip"+(bad?" bad":"")+"\'>"+esc((lbl[x.state]||x.state))+"</span></td>"' +
  '+"<td>"+esc(x.partner_name||x.partner_email||x.partner_key||"—")+"</td>"' +
  '+"<td"+(x.sf_state==="create_errored"?" class=\'psattn\'":"")+">"+esc(sfs)+"</td>"' +
  '+"<td class=\'psna\'>"+esc(det||"—")+"</td>"' +
  '+"<td class=\'psna\' style=\'white-space:nowrap\'>"+esc(et(x.last_seen))+"</td></tr>";}).join("");}}' +
  '}catch(e){document.getElementById("ptbody").innerHTML="<tr><td colspan=\'8\' class=\'nd\' style=\'color:#b91c1c\'>Failed: "+esc(e.message)+"</td></tr>";}}' +
  'function sortPartners(c){if(pSort===c)pDir=(pDir==="asc"?"desc":"asc");else{pSort=c;pDir=(c==="partner_name")?"asc":"desc";}renderPartners();}' +
  'function renderPartners(){var tb=document.getElementById("ptbody");' +
  'if(!partnerRows.length){tb.innerHTML="<tr><td colspan=\'8\' class=\'nd\'>No partner-sourced leads yet.</td></tr>";return;}' +
  'var rows=partnerRows.slice().sort(function(a,b){var x=a[pSort],y=b[pSort];' +
  'if(pSort==="partner_name"){x=(a.partner_name||a.partner_email||a.partner_key||"").toLowerCase();y=(b.partner_name||b.partner_email||b.partner_key||"").toLowerCase();}' +
  'else if(pSort==="last_click"){x=x?new Date(x).getTime():0;y=y?new Date(y).getTime():0;}' +
  'else{x=Number(x)||0;y=Number(y)||0;}' +
  'if(x<y)return pDir==="asc"?-1:1;if(x>y)return pDir==="asc"?1:-1;return 0;});' +
  '["partner_name","clicks","step1","completed","conversions","booked","qualified","last_click"].forEach(function(c){var el=document.getElementById("psar-"+c);if(el)el.textContent=(pSort===c)?(pDir==="asc"?"▲":"▼"):"";});' +
  /* name -> email -> key, the same chain as Slack and the detail panel. */
  /* Event delegation rather than an inline onclick: the key would otherwise
     need quotes nested three deep (HTML attribute inside a client JS string
     inside this server-side JS string), which is precisely how the row markup
     broke the first time. data-pk carries it instead. */
  'tb.innerHTML=rows.map(function(p){var label=p.partner_name||p.partner_email||p.partner_key;' +
  'return "<tr class=\'prow\' data-pk=\'"+esc(p.partner_key)+"\' style=\'cursor:pointer\' title=\'Show this partner\\u2019s leads\'>"' +
  '+"<td>"+esc(label)+"</td>"' +
  '+"<td>"+esc(p.partner_email||"—")+"</td>"' +
  '+"<td><code style=\'font-size:10px\'>"+esc(p.partner_key)+"</code></td>"' +
  '+"<td>"+(p.clicks===null||p.clicks===undefined?"&#8212;":p.clicks)+"</td>"' +
  '+"<td>"+(p.step1||0)+"</td>"' +
  '+"<td>"+(p.completed||0)+"</td>"' +
  '+"<td>"+(p.conversions||0)+"</td>"' +
  '+"<td>"+(p.booked||0)+"</td>"' +
  '+"<td>"+(p.qualified||0)+"</td>"' +
  '+"<td style=\'color:#999;white-space:nowrap\'>"+(p.last_click?esc(et(p.last_click)):"—")+"</td></tr>";}).join("");' +
  'Array.prototype.forEach.call(tb.querySelectorAll(".prow"),function(tr){tr.onclick=function(){partnerDrill(tr.getAttribute("data-pk"));};});}' +
  /* Drill-down REUSES the All Leads view and its existing partner filter
     rather than building a second leads table. The detail panel, the stage
     ladder and the CSV export all come along for free, and there is only one
     place where "a lead row" is rendered. */
  'async function partnerDrill(key){showTab("leads");await loadFilterOptions();' +
  'var sel=document.getElementById("fpartner");' +
  'if(sel){var has=Array.prototype.some.call(sel.options,function(o){return o.value===key;});' +
  'if(!has){var o=document.createElement("option");o.value=key;o.textContent="Partner: "+key;sel.appendChild(o);}' +
  'sel.value=key;}loadLeads(1);}' +
  'function debounce(){clearTimeout(stimer);stimer=setTimeout(function(){loadLeads(1);},400);}' +
  'function clearF(){document.getElementById("fsearch").value="";document.getElementById("fstage").value="all";document.getElementById("fsellto").value="all";document.getElementById("fsource").value="all";document.getElementById("fenrich").value="all";document.getElementById("fwebsitecheck").value="all";document.getElementById("frepeat").value="all";document.getElementById("fpartner").value="all";document.getElementById("fhear").value="";document.getElementById("fpreset").value="";document.getElementById("ffrom").value="";document.getElementById("fto").value="";curSort="created_at";curDir="desc";renderSortArrows();loadLeads(1);}' +
  'function renderSortArrows(){["email","name","company","sell_to","created_at"].forEach(function(c){var el=document.getElementById("sar-"+c);if(el)el.textContent=(curSort===c)?(curDir==="asc"?"\\u25B2":"\\u25BC"):"";});}' +
  'function sortBy(c){if(curSort===c){curDir=(curDir==="asc")?"desc":"asc";}else{curSort=c;curDir=(c==="created_at")?"desc":"asc";}renderSortArrows();loadLeads(1);}' +
  'function datePreset(v){var ff=document.getElementById("ffrom"),ft=document.getElementById("fto");if(!v){loadLeads(1);return;}var to=etDayShift(0),from=to;if(v==="7d")from=etDayShift(-6);else if(v==="30d")from=etDayShift(-29);ff.value=from;ft.value=to;loadLeads(1);}' +
  'function dateManual(){var p=document.getElementById("fpreset");if(p)p.value="";loadLeads(1);}' +
  'function exportLeads(){var search=document.getElementById("fsearch").value.trim(),stage=document.getElementById("fstage").value,sellTo=document.getElementById("fsellto").value,source=document.getElementById("fsource").value,enrich=document.getElementById("fenrich").value,websiteCheck=document.getElementById("fwebsitecheck").value,repeatAttempts=document.getElementById("frepeat").value,hear=document.getElementById("fhear").value.trim(),partner=document.getElementById("fpartner").value,from=document.getElementById("ffrom").value,to=document.getElementById("fto").value;var url=API+"/monitor/leads"+(TP||"?")+(TP?"&":"")+"format=csv&stage="+stage+"&sort="+curSort+"&dir="+curDir;if(sellTo&&sellTo!=="all")url+="&sellTo="+encodeURIComponent(sellTo);if(source&&source!=="all")url+="&utmSource="+encodeURIComponent(source);if(enrich&&enrich!=="all")url+="&enrichment="+encodeURIComponent(enrich);if(websiteCheck&&websiteCheck!=="all")url+="&websiteCheck="+encodeURIComponent(websiteCheck);if(repeatAttempts&&repeatAttempts!=="all")url+="&repeatAttempts="+encodeURIComponent(repeatAttempts);if(partner&&partner!=="all")url+="&partner="+encodeURIComponent(partner);if(hear)url+="&hearAbout="+encodeURIComponent(hear);if(search)url+="&search="+encodeURIComponent(search);if(from)url+="&dateFrom="+from;if(to)url+="&dateTo="+to;window.location.href=url;}' +
  'async function loadFilterOptions(){if(filterOptsLoaded)return;try{var r=await fetch(API+"/monitor/filter-options"+(TP||"?")+(TP?"&":"")+"_="+Date.now(),{signal:AbortSignal.timeout(10000)});if(!r.ok)return;var d=await r.json();var sel=document.getElementById("fsource");if(sel&&d.utmSource){d.utmSource.forEach(function(v){var o=document.createElement("option");o.value=v;o.textContent=v;sel.appendChild(o);});}var ps=document.getElementById("fpartner");if(ps&&d.partners){d.partners.forEach(function(p){var o=document.createElement("option");o.value=p.key;o.textContent="Partner: "+(p.name||p.key)+(p.email?" <"+p.email+">":"");ps.appendChild(o);});}var dl=document.getElementById("hearlist");if(dl&&d.hearAbout){dl.innerHTML=d.hearAbout.map(function(v){return"<option value=\\""+esc(v)+"\\"></option>";}).join("");}filterOptsLoaded=true;}catch(e){}}' +
  'function toggleRow(sid){var row=document.getElementById("er-"+sid);if(!row)return;var vis=row.style.display!=="none";row.style.display=vis?"none":"table-row";var btn=row.previousElementSibling&&row.previousElementSibling.querySelector(".xbtn");if(btn)btn.textContent=vis?"\\u25B6":"\\u25BC";}' +
  'async function loadLeads(pg){curPage=pg||1;var search=document.getElementById("fsearch").value.trim(),stage=document.getElementById("fstage").value,sellTo=document.getElementById("fsellto").value,source=document.getElementById("fsource").value,enrich=document.getElementById("fenrich").value,websiteCheck=document.getElementById("fwebsitecheck").value,repeatAttempts=document.getElementById("frepeat").value,hear=document.getElementById("fhear").value.trim(),partner=document.getElementById("fpartner").value,from=document.getElementById("ffrom").value,to=document.getElementById("fto").value;' +
  'var url=API+"/monitor/leads"+(TP||"?")+(TP?"&":"")+"page="+curPage+"&stage="+stage+"&sort="+curSort+"&dir="+curDir;' +
  'if(sellTo&&sellTo!=="all")url+="&sellTo="+encodeURIComponent(sellTo);if(source&&source!=="all")url+="&utmSource="+encodeURIComponent(source);if(enrich&&enrich!=="all")url+="&enrichment="+encodeURIComponent(enrich);if(websiteCheck&&websiteCheck!=="all")url+="&websiteCheck="+encodeURIComponent(websiteCheck);if(repeatAttempts&&repeatAttempts!=="all")url+="&repeatAttempts="+encodeURIComponent(repeatAttempts);if(partner&&partner!=="all")url+="&partner="+encodeURIComponent(partner);if(hear)url+="&hearAbout="+encodeURIComponent(hear);if(search)url+="&search="+encodeURIComponent(search);if(from)url+="&dateFrom="+from;if(to)url+="&dateTo="+to;' +
  'document.getElementById("ltbody").innerHTML="<tr><td colspan=\\"10\\" class=\\"nd\\">Loading...</td></tr>";' +
  'try{var r=await fetch(url,{signal:AbortSignal.timeout(12000)});if(!r.ok)throw new Error("HTTP "+r.status);var d=await r.json();' +
  'set("lcount",d.total+" lead"+(d.total!==1?"s":"")+" found");' +
  'if(!d.leads.length){document.getElementById("ltbody").innerHTML="<tr><td colspan=\\"10\\" class=\\"nd\\">No leads match your filters.</td></tr>";document.getElementById("lpag").innerHTML="";return;}' +
  'var html=d.leads.map(function(l){var sid=esc(l.session_id),name=[l.first_name,l.last_name].filter(Boolean).map(esc).join(" ")||"\\u2014",src=l.utm_source?esc(l.utm_source)+(l.utm_medium?" / "+esc(l.utm_medium):""):(l.referrer?"referral":"\\u2014");' +
  'return"<tr><td class=\\"xbtn\\" onclick=\\"toggleRow(\'"+sid+"\')\\">&#9658;</td><td class=\\"te\\" title=\\""+esc(l.email)+"\\">"+(l.website_check_failed?"<span style=\\"color:#b91c1c\\">&#9888;&#65039; </span>":(l.website_check_reason==="social_profile_url"?"<span style=\\"color:#1d4ed8\\" title=\\"Social profile \\u2014 no company site\\">&#128279; </span>":""))+esc(l.email||"\\u2014")+"</td><td>"+name+"</td><td class=\\"tc\\">"+esc(l.company||"\\u2014")+"</td><td>"+esc(l.sell_to||"\\u2014")+"</td><td>"+stageBadge(l)+"</td><td>"+(l.booking_uid?"<span class=\\"badge bg\\">Yes</span>":"<span class=\\"badge bx\\">No</span>")+"</td><td>"+enrichBadge(l)+"</td><td style=\\"color:#999;white-space:nowrap\\">"+et(l.created_at)+"</td><td style=\\"color:#999;font-size:11px\\">"+src+"</td></tr>"+' +
  '"<tr class=\\"erow\\" id=\\"er-"+sid+"\\" style=\\"display:none\\"><td></td><td colspan=\\"9\\">"+enrichPanel(l)+"</td></tr>";}).join("");' +
  'document.getElementById("ltbody").innerHTML=html;renderPag(d.page,d.pages);}catch(e){document.getElementById("ltbody").innerHTML="<tr><td colspan=\\"10\\" class=\\"nd\\" style=\\"color:#b91c1c\\">Failed: "+esc(e.message)+"</td></tr>";}}' +
  'function renderPag(pg,pages){if(pages<=1){document.getElementById("lpag").innerHTML="";return;}var h="";h+="<button class=\\"pb\\" onclick=\\"loadLeads("+(pg-1)+")\\""+(pg<=1?" disabled":"")+">&larr;</button>";var s=Math.max(1,pg-2),e=Math.min(pages,pg+2);if(s>1)h+="<button class=\\"pb\\" onclick=\\"loadLeads(1)\\">1</button>"+(s>2?"<span class=\\"pi\\">&#8230;</span>":"");for(var i=s;i<=e;i++)h+="<button class=\\"pb"+(i===pg?" act":"")+ "\\" onclick=\\"loadLeads("+i+")\\" >"+i+"</button>";if(e<pages)h+=(e<pages-1?"<span class=\\"pi\\">&#8230;</span>":"")+"<button class=\\"pb\\" onclick=\\"loadLeads("+pages+")\\" >"+pages+"</button>";h+="<button class=\\"pb\\" onclick=\\"loadLeads("+(pg+1)+")\\"" +(pg>=pages?" disabled":"")+">&rarr;</button><span class=\\"pi\\">Page "+pg+" of "+pages+"</span>";document.getElementById("lpag").innerHTML=h;}' +
  'var lmLeads=[],lmChart=null,lmFilter="all";' +
  'var lmPillDefs=[["all","All"],["awaiting","Awaiting send"],["sent","Sent"],["abandoned","Abandoned"],["internal","Internal tests"]];' +
  /* TRUE TOTALS, from the server. These used to be counted client-side over
     whatever /monitor/lm-leads returned — and that route is LIMIT 500 while
     the dashboard never sends a limit, so past 500 rows in the window every
     pill silently understated while reading as a total of the whole funnel. */
  'var lmTotals=null,lmShownOf=null;' +
  'function lmPct(a,b){return b>0?Math.round(a/b*100)+"%":"\\u2014";}' +
  'function lmET(t){if(!t)return "\\u2014";return new Date(t).toLocaleString("en-US",{timeZone:TZ,day:"2-digit",month:"short",hour:"2-digit",minute:"2-digit"});}' +
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
  'lmTotals=d.statusTotals||null;' +
  'var dy=d.daily||[],cv=document.getElementById("lm-chart");' +
  'if(cv&&window.Chart){if(lmChart)lmChart.destroy();lmChart=new Chart(cv,{type:"line",data:{labels:dy.map(function(x){return x.day.slice(5);}),' +
  'datasets:[{label:"Views",data:dy.map(function(x){return x.views;}),borderColor:"#d4d4d4",backgroundColor:"#d4d4d4",tension:0.25,pointRadius:0,borderWidth:2},' +
  '{label:"Email entered",data:dy.map(function(x){return x.emails;}),borderColor:"#f59e0b",backgroundColor:"#f59e0b",tension:0.25,pointRadius:0,borderWidth:2},' +
  '{label:"Submitted",data:dy.map(function(x){return x.submitted;}),borderColor:"#1a1a1a",backgroundColor:"#1a1a1a",tension:0.25,pointRadius:0,borderWidth:2}]},' +
  'options:{responsive:true,interaction:{mode:"index",intersect:false},plugins:{legend:{display:true,labels:{boxWidth:10,font:{size:11}}}},scales:{y:{beginAtZero:true,ticks:{precision:0}}}}});}' +
  '}catch(err){console.warn("[LM] metrics failed",err);}' +
  'try{var r2=await fetch(API+"/monitor/lm-leads"+dq,{cache:"no-store"});var d2=await r2.json();lmLeads=d2.leads||[];' +
  'lmShownOf=(typeof d2.total==="number")?d2.total:null;lmRender();' +
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
  /* Server totals when we have them. The client fallback counts only the
     loaded page, so it is LABELLED as such rather than presented as a total —
     which is the whole failure this replaces. */
  'var counts,fromPage=!lmTotals;' +
  'if(lmTotals){counts={all:lmTotals.all,awaiting:lmTotals.awaiting,sent:lmTotals.sent,abandoned:lmTotals.abandoned,internal:lmTotals.internal};}' +
  'else{counts={all:0,awaiting:0,sent:0,abandoned:0,internal:0};' +
  'lmLeads.forEach(function(l){if(l.is_internal){counts.internal++;return;}counts.all++;if(counts[l.status]!==undefined)counts[l.status]++;});}' +
  'document.getElementById("lm-pills").innerHTML=lmPillDefs.map(function(p){var on=lmFilter===p[0];' +
  'return "<button onclick=\\"lmSetFilter(\'"+p[0]+"\')\\" style=\\"padding:5px 11px;border-radius:99px;font-size:12px;cursor:pointer;border:1px solid "+' +
  '(on?"#1a1a1a":"#e5e5e5")+";background:"+(on?"#1a1a1a":"#fff")+";color:"+(on?"#fff":"#444")+' +
  '"\\">"+p[1]+" <span style=\\"opacity:.6\\">"+(counts[p[0]]||0)+"</span></button>";}).join("");' +
  'var rows=lmSearched();var tb=document.getElementById("lm-tbody");' +
  /* "N shown" next to a 500-row page reads as "N exist". */
  'var cap=(lmShownOf!==null&&lmShownOf>lmLeads.length)?" \u00b7 table shows the most recent "+lmLeads.length+" of "+lmShownOf+" \u2014 the pill counts above are full totals":"";' +
  'set("lm-count",rows.length+" shown"+(fromPage?" \u00b7 pill counts are for the loaded rows only, totals unavailable":"")+cap);' +
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
  '"<td>"+lmET(l.submitted_at||l.created_at)+"</td>"+' +
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
  'lmCell("Loops",l.loops_sent?("Sent "+lmET(l.loops_sent_at)):(l.loops_error?("Failed: "+l.loops_error):"Not sent"))+' +
  '(l.loops_error||(!l.loops_sent&&l.completed)?lmCell("Retry","<button class=\\"btn\\" onclick=\\"lmLoopsRetry("+l.id+")\\">Push to Loops</button>",0,1):"")+' +
  'lmCell("Submitted",lmET(l.submitted_at))+lmCell("First seen",lmET(l.created_at))+' +
  'lmCell("Delivered at",lmET(l.delivered_at))+' +
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
  'a.download="lead-magnet-"+etDay(new Date())+".csv";a.click();}' +
  'async function loadAll(){set("lupd","Refreshing...");var ok=await checkApi();if(!ok){document.getElementById("alerts").innerHTML="<div class=\\"alertbox ae\\"><span>x</span><span>API offline.</span></div>";set("lupd","API offline");return;}checkElv();' +
  'try{var r=await fetch(API+"/monitor/metrics"+TP,{signal:AbortSignal.timeout(12000)});if(!r.ok)throw new Error("HTTP "+r.status);var d=await r.json();' +
  'set("m-total",d.peopleTotal);set("m-totals",d.total+" sessions \\u00B7 "+d.todayCount+" in last 24h");' +
  'set("m-comp",d.peopleCompleted);set("m-cpct",pct(d.peopleCompleted,d.peopleTotal)+" of people \\u00B7 "+d.completed+" sessions");' +
  'set("m-book",d.peopleBooked);set("m-bpct",pct(d.peopleBooked,d.peopleCompleted)+" of completed \\u00B7 "+d.booked+" sessions");' +
  'set("m-disq",d.peopleDisqualified);set("m-dsq","B2C / Mixed \\u00B7 "+d.disqualified+" sessions");' +
  'set("m-nb",d.peopleNoBooking);set("m-nbs",d.completedNoBookingSessions+" completed sessions w/o booking");' +
  'set("m-rec",d.recoveredBookings);set("m-pend",d.pendingPartials);set("m-mail",d.loopsSent);' +
  'set("recon","Sessions = form visits \\u00B7 People = distinct emails. "+d.completedNoBookingSessions+" completed sessions without a booking \\u2192 "+d.noBookingUid+" actionable people after dedup, cross-session bookings & B2B filter.");' +
  'set("h-enr",d.enriched);set("h-tit",d.enrichTitlePct!==undefined?d.enrichTitlePct+"%":"\\u2014");set("h-fun",d.enrichFundingPct!==undefined?d.enrichFundingPct+"%":"\\u2014");set("h-loc",d.enrichLocationPct!==undefined?d.enrichLocationPct+"%":"\\u2014");' +
  'renderAlerts(d);renderFunnel(d);renderChart(d.leadsByDay||[]);' +
  'set("lupd","Updated "+new Date().toLocaleTimeString("en-US",{timeZone:TZ})+" ET");' +
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
  '{lb:"Submitted",v:et(l.submitted_at)},' +
  '].filter(function(f){return f.v;});' +
  'if(!fields.length)return"<div style=\\"color:#999;font-size:12px\\">No additional details.</div>";' +
  'return"<div class=\\"egrid\\">"+fields.map(function(f){var val=f.lnk&&f.v?"<a href=\\""+(f.v.startsWith("http")?"":"https://")+esc(f.v)+"\\" target=\\"_blank\\">"+esc(f.v)+"</a>":esc(f.v);return"<div class=\\"ef\\"><div class=\\"efl\\">"+f.lb+"</div><div class=\\"efv\\">"+val+"</div></div>";}).join("")+"</div>";}' +
  'function toggleSDRRow(idx){var row=document.getElementById("sdr-er-"+idx);if(!row)return;var vis=row.style.display!=="none";row.style.display=vis?"none":"table-row";var btn=document.getElementById("sdr-xbtn-"+idx);if(btn)btn.textContent=vis?"\\u25B6":"\\u25BC";}' +
  'function renderSDRTable(allLeads){' +
  'var q=(document.getElementById("sdr-search")||{}).value||"";' +
  'var leads=q?allLeads.filter(function(l){var s=q.toLowerCase();' +
  'return SDR_SEARCH_FIELDS.some(function(f){return String(l[f]||"").toLowerCase().includes(s);});}):allLeads;' +
  'set("sdr-count",leads.length+" lead"+(leads.length!==1?"s":""));' +
  'if(!leads.length){document.getElementById("sdr-tbody").innerHTML="<tr><td colspan=\\"10\\" class=\\"nd\\">No leads found.</td></tr>";return;}' +
  'var html=leads.map(function(l,i){' +
  'var name=[l.first_name,l.last_name].filter(Boolean).map(esc).join(" ")||"\\u2014";' +
  'var stage=l.completed?"<span class=\\"badge bb\\">Completed</span>":"<span class=\\"badge ba\\">Step 1</span>";' +
  'var li=l.enriched_linkedin?"<a href=\\""+esc(l.enriched_linkedin)+"\\" target=\\"_blank\\" style=\\"color:#2563eb;text-decoration:none\\">View</a>":"\\u2014";' +
  'return"<tr><td class=\\"xbtn\\" id=\\"sdr-xbtn-"+i+"\\" onclick=\\"toggleSDRRow("+i+")\\">&#9658;</td><td class=\\"te\\" title=\\""+esc(l.email)+"\\">"+esc(l.email||"\\u2014")+"</td><td>"+name+"</td><td class=\\"tc\\">"+esc(l.company||"\\u2014")+"</td><td style=\\"color:#555\\">"+esc(l.enriched_title||"\\u2014")+"</td><td>"+esc(l.enriched_industry||"\\u2014")+"</td><td>"+esc(l.enriched_company_size||"\\u2014")+"</td><td>"+stage+"</td><td>"+li+"</td><td style=\\"color:#999;white-space:nowrap\\">"+et(l.created_at)+"</td></tr>"' +
  '+"<tr class=\\"erow\\" id=\\"sdr-er-"+i+"\\" style=\\"display:none\\"><td></td><td colspan=\\"9\\">"+sdrPanel(l)+"</td></tr>";' +
  '}).join("");' +
  'document.getElementById("sdr-tbody").innerHTML=html;}' +
  /* SDR_SEARCH_FIELDS — the client half. The server half is
     SDR_SEARCH_COLUMNS in the /monitor/sdr route, and the tests lift both
     lists and assert they are equal. Two copies of a field list is exactly
     how an export quietly stops matching what is on screen. */
  'var SDR_SEARCH_FIELDS=["email","company","first_name","enriched_industry"];' +
  'function exportSDR(){var q=((document.getElementById("sdr-search")||{}).value||"").trim();' +
  'window.location.href=API+"/monitor/sdr"+(TP||"?")+(TP?"&":"")+"format=csv"+(q?"&search="+encodeURIComponent(q):"");}' +
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
  '"<span style=\\"color:#aaa\\">"+et(s.created_at)+"</span>"+' +
  '"<span style=\\"color:#aaa\\">"+esc(s.page_url||"")+"</span>"+' +
  '"</div>";}).join("");' +
  'return"<tr><td class=\\"xbtn\\" id=\\"dupe-xbtn-"+i+"\\" onclick=\\"toggleDupeRow("+i+")\\">&#9658;</td>"+' +
  '"<td class=\\"te\\">"+esc(l.email)+"</td>"+' +
  '"<td><span class=\\"badge br\\">"+l.session_count+" sessions</span></td>"+' +
  '"<td>"+booked+"</td><td>"+comp+"</td>"+' +
  '"<td style=\\"color:#999;white-space:nowrap\\">"+et(l.first_seen)+"</td>"+' +
  '"<td style=\\"color:#999;white-space:nowrap\\">"+et(l.last_seen)+"</td>"+' +
  '"</tr>"+' +
  '"<tr class=\\"erow\\" id=\\"dupe-er-"+i+"\\" style=\\"display:none\\"><td></td><td colspan=\\"6\\"><div style=\\"padding:4px 0\\">"+sessRows+"</div></td></tr>";' +
  '}).join("");' +
  'document.getElementById("dupes-tbody").innerHTML=html;' +
  '}catch(e){document.getElementById("dupes-tbody").innerHTML="<tr><td colspan=\\"7\\" class=\\"nd\\" style=\\"color:#b91c1c\\">Failed: "+esc(e.message)+"</td></tr>";}}' +
  'function toggleDupeRow(i){var row=document.getElementById("dupe-er-"+i);if(!row)return;var vis=row.style.display!=="none";row.style.display=vis?"none":"table-row";var btn=document.getElementById("dupe-xbtn-"+i);if(btn)btn.textContent=vis?"\\u25B6":"\\u25BC";}' +
  'renderSortArrows();loadAll();setInterval(loadAll,60000);' +
  'checkHealth();setInterval(checkHealth,300000);' +
  'loadPartnerGaps();setInterval(loadPartnerGaps,600000);' +
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
// v5.4.0 — ELV_MIN_SAMPLE exists so a couple of unlucky checks can't cry wolf.
// But it also meant a TOTAL outage was invisible whenever traffic was thin:
// 16 Aug 13:14 → 17 Aug 05:33, 49 consecutive failures, and the overnight
// stretch never reached 8 checks in a 90-minute window, so nothing fired.
// A run of consecutive failures is an outage at ANY volume, so it bypasses
// the sample floor entirely.
const ELV_CONSECUTIVE_FAIL = Number(process.env.ELV_CONSECUTIVE_FAIL) || 4;
// Recovery needs more than one lucky result, or the state flaps on thin traffic.
const ELV_RECOVER_STREAK   = Number(process.env.ELV_RECOVER_STREAK)   || 2;
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
let   _elvConsecBad  = 0;   // running streak of inconclusive results
let   _elvConsecGood = 0;   // running streak of conclusive results
let   _elvDegradedSince = 0;

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
  const rate   = checks ? Math.round((bad / checks) * 100) : 0;
  // v5.4.0 — 'insufficient_data' used to render as a calm grey "Idle", which
  // reads as "all fine" when it actually means "I am not allowed to have an
  // opinion". A live snapshot showed rate:40 checks:5 badged as Idle. The
  // rate is now always reported so a thin sample can still look worrying.
  let state;
  if (_elvDegraded) state = 'degraded';
  else if (checks < ELV_MIN_SAMPLE) state = 'insufficient_data';
  else state = 'healthy';
  return {
    state,
    inconclusive:  bad,
    checks,
    rate,
    consecutiveInconclusive: _elvConsecBad,
    degradedSince: _elvDegradedSince ? new Date(_elvDegradedSince).toISOString() : null,
    windowMinutes: Math.round(ELV_WINDOW_MS / 60000),
    minSample:     ELV_MIN_SAMPLE,
    consecutiveFailThreshold: ELV_CONSECUTIVE_FAIL,
    cacheSize:     _elvCache.size,
    lastStatus:    _elvLastStatus,
    lastCheckAt:   _elvLastCheckAt ? new Date(_elvLastCheckAt).toISOString() : null,
    minutesSinceLastCheck: _elvLastCheckAt ? Math.round((now - _elvLastCheckAt) / 60000) : null,
  };
}

function recordElvOutcome(status, email) {
  try {
    const now = Date.now();
    _elvLastStatus  = status;
    _elvLastCheckAt = now;
    if (email && elvIsInternal(email)) return; // own testing never moves health state

    const bad = ELV_INDETERMINATE.includes(status);
    if (bad) { _elvConsecBad += 1; _elvConsecGood = 0; }
    else     { _elvConsecGood += 1; _elvConsecBad = 0; }

    _elvWindow.push({ t: now, bad });
    pruneElvWindow(now);

    const checks = _elvWindow.length;
    const rate   = checks ? _elvWindow.filter(e => e.bad).length / checks : 0;

    /* ── ENTER degraded ──
       Two independent triggers. The rate test needs a decent sample so a
       couple of unlucky checks can't cry wolf. The streak test has no
       sample requirement at all — a run of consecutive failures means the
       upstream is down no matter how quiet the night is. */
    const byStreak = _elvConsecBad >= ELV_CONSECUTIVE_FAIL;
    const byRate   = checks >= ELV_MIN_SAMPLE && rate >= ELV_DEGRADED_RATE;

    if (!_elvDegraded && (byStreak || byRate)) {
      _elvDegraded = true;
      _elvDegradedSince = now;
      alertOps('warning', 'ELV', 'Verification degraded', {
        'Signal': byStreak
          ? `${_elvConsecBad} inconclusive results in a row`
          : `${Math.round(rate * 100)}% of ${checks} checks in the last ${Math.round(ELV_WINDOW_MS / 60000)} min`,
        'Impact': 'Inconclusive results are passing unverified. Definitively bad emails are STILL being blocked.',
        'Action': 'Check ELV status/credits. Self-recovers — you will get a follow-up when it clears.',
      });
      return;
    }

    /* ── EXIT degraded ──
       v5.4.0: the old code did `_elvDegraded = false; return;` whenever the
       sample was thin. That silently reset the state AND skipped the
       recovery message — which is why 16 Aug alerted twice for one incident
       (state cleared overnight, then re-crossed the threshold) and why no
       "recovered" message was ever sent. State now ONLY leaves degraded
       through this branch, which always announces itself. */
    if (_elvDegraded) {
      const streakClear = _elvConsecGood >= ELV_RECOVER_STREAK;
      // On a thin sample the rate is not meaningful, so a clean streak is
      // enough. With a real sample, hysteresis applies.
      const rateClear = checks >= ELV_MIN_SAMPLE ? rate <= ELV_RECOVERED_RATE : true;
      if (streakClear && rateClear) {
        const downFor = _elvDegradedSince ? Math.round((now - _elvDegradedSince) / 60000) : null;
        _elvDegraded = false;
        _elvDegradedSince = 0;
        const heading = '✅ ELV — Verification recovered';
        sendOpsSlack([
          bHeader(heading),
          bDivider(),
          bFields([
            { label: 'Now', value: `${_elvConsecGood} conclusive results in a row · ${Math.round(rate * 100)}% inconclusive of ${checks} recent checks` },
            downFor != null ? { label: 'Degraded for', value: downFor >= 60 ? `${Math.round(downFor / 60)}h ${downFor % 60}m` : `${downFor} min` } : null,
          ].filter(Boolean)),
          bContext(`Severity: *info* · ${etStamp()}`),
        ].filter(Boolean), heading);
        console.log(`[ELV] ✅ Recovered — degradation cleared after ${downFor} min`);
      }
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

/* ── Durable store ────────────────────────────────────────────────
   The cache above is in-memory, so every Railway deploy or restart
   silently drops the verdict for anyone mid-form. A flag that is
   SOMETIMES missing is worse than no flag, because its absence reads as
   "we checked and nothing was wrong". So Postgres is the source of
   truth and the cache goes back to being what it was always good for:
   not spending a second ELV credit on one address.

   Same guard as elvCacheSet, and it is the whole point: only DEFINITIVE
   verdicts are written. 'timeout', 'http_error', 'network_error',
   'unknown' and 'skipped' write NOTHING, so an empty column always
   means "we deliberately do not know" and never "we checked, it was
   fine". Reading it back is only safe because of this line.

   Fire-and-forget on purpose. /verify-email has a lead waiting on it,
   and a lost write costs nothing: the in-memory cache covers the
   seconds until /submit, and /submit re-checks rather than storing a
   blank. Two independent chances plus a re-check. */
function persistElvVerdict(email, status, valid, source) {
  if (!ELV_BLOCK.includes(status) && !ELV_PASS.includes(status)) return;
  pool.query(`
    INSERT INTO email_verifications (email, status, valid, source, checked_at)
    VALUES ($1,$2,$3,$4,NOW())
    ON CONFLICT (email) DO UPDATE SET
      status     = EXCLUDED.status,
      valid      = EXCLUDED.valid,
      source     = EXCLUDED.source,
      checked_at = NOW()
  `, [email, status, valid, source || 'elv'])
    .catch(err => console.warn(`[ELV] verdict not persisted for ${email} (ignored):`, err.message));
}

// One place that knows the endpoint and the key, so the re-check below
// and the /verify-email handler can never drift apart on it.
function elvCheckUrl(email, apiKey) {
  return `https://apps.emaillistverify.com/api/verifyEmail?secret=${apiKey}&email=${encodeURIComponent(email)}`;
}

/* Status-only re-check. Deliberately NOT the /verify-email handler: that
   one races DNS, aborts the in-flight call on NXDOMAIN and composes
   user-facing copy, none of which applies when nobody is waiting for an
   answer. Returns a verdict or null — never a guess. */
async function elvRecheckStatusOnly(email) {
  const apiKey = process.env.ELV_API_KEY;
  if (!apiKey) return null;
  const controller = new AbortController();
  const timeout    = setTimeout(() => controller.abort(), ELV_TIMEOUT_MS);
  try {
    const response = await fetch(elvCheckUrl(email, apiKey), { signal: controller.signal });
    if (!response.ok) { recordElvOutcome('http_error', email); return null; }
    const status = normaliseElvStatus((await response.text()).trim().toLowerCase());
    const known  = ELV_BLOCK.includes(status) || ELV_PASS.includes(status) || ELV_INDETERMINATE.includes(status);
    recordElvOutcome(known ? status : 'unknown', email);
    const valid = !ELV_BLOCK.includes(status);
    elvCacheSet(email, valid, status);
    persistElvVerdict(email, status, valid, 'submit_recheck');
    console.log(`[ELV] re-checked ${email} at submit time → "${status}"`);
    // Indeterminate stays unwritten and unreturned, same rule as everywhere.
    return ELV_BLOCK.includes(status) || ELV_PASS.includes(status) ? { status, valid } : null;
  } catch (err) {
    recordElvOutcome(err && err.name === 'AbortError' ? 'timeout' : 'network_error', email);
    return null;
  } finally {
    clearTimeout(timeout);
  }
}

/* Read the stored verdict. DB first — that is the source of truth. The
   memory cache is second and covers only the sub-second window where a
   fire-and-forget write has not landed yet. Never calls ELV: the
   re-check runs off the critical path, after the response. */
async function lookupElvStatus(email) {
  if (!email) return null;
  try {
    const r = await pool.query('SELECT status, checked_at FROM email_verifications WHERE email=$1', [email]);
    if (r.rows[0]) return { status: r.rows[0].status, checked_at: r.rows[0].checked_at, from: 'db' };
  } catch (err) {
    console.warn(`[ELV] verdict lookup failed for ${email} (continuing):`, err.message);
  }
  const hit = elvCacheGet(email);
  if (hit) return { status: hit.status, checked_at: new Date(hit.at), from: 'memory' };
  return null;
}

/* The flag. Fires once, from /submit, when BOTH checks came back unable
   to tell us anything — see isUnverifiablePair(). Never blocks, never
   touches Meta: it adds a line an SDR can act on before the call. */
function alertUnverifiablePair(row) {
  if (!isUnverifiablePair(row)) return false;
  if (elvIsInternal(row.email)) return false; // our own testing is not an incident
  console.warn(`[Form] ⚠ ${row.email} — catch-all mailbox (${row.elv_status}) AND website unreachable (${row.website_check_reason}); nothing verified`);
  alertOps('warning', 'Form', 'Nothing verified this lead', {
    'Email': row.email,
    'Why': UNVERIFIABLE_PAIR_NOTE,
    'Email check': `${row.elv_status} — mail server accepts every address`,
    'Website check': websiteReasonLabel(row.website_check_reason),
    'Impact': 'The lead passed and can book, exactly as before. Worth a look before the call — this is the shape the yo@yoyo.com lead had.',
  });
  return true;
}

/* Fills leads.elv_status when nothing was stored by verify time. Runs
   AFTER the response has gone out: a lead is waiting on /submit and one
   ELV call is up to 8 seconds. At 30-40 leads/day the extra credit on
   the miss path is cheap; the wait is not. Writes only over NULL, so a
   verdict that landed in the meantime wins. */
async function finaliseElvVerdict({ session_id, email, website_check_reason }) {
  if (!email || elvIsInternal(email)) return;
  try {
    const fresh = await elvRecheckStatusOnly(email);
    if (!fresh) {
      console.log(`[/submit] elv_status left empty for ${email} — the re-check did not conclude either`);
      return;
    }
    await pool.query(
      'UPDATE leads SET elv_status=$2, elv_checked_at=NOW(), updated_at=NOW() WHERE session_id=$1 AND elv_status IS NULL',
      [session_id, fresh.status]
    );
    alertUnverifiablePair({ email, elv_status: fresh.status, website_check_reason });
  } catch (err) {
    console.warn(`[ELV] late verdict fill failed for ${email} (ignored):`, err.message);
  }
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

/* ── Typo hint on a PASSING address ───────────────────────────────
   v5.6.0. suggestDomainFix has only ever run inside elvRejection, i.e.
   when an address FAILS. That is precisely backwards for typosquats:
   gmailc.com has a live catch-all mail server, so ELV answered
   "ok_for_all" (a pass), elvRejection never ran, and no "did you mean
   gmail.com?" was ever offered. The moment a typo domain becomes real,
   the typo detection went silent.

   This is a HINT, never a block. The address may genuinely be valid, and
   the person can ignore it. But mail sent to a squatted domain reaches
   the squatter rather than the lead, so it is worth surfacing. */
/* ── Nothing verified this lead ────────────────────────────────────
   'ok_for_all' / 'accept_all' mean the mail server accepts EVERY address
   without checking whether it exists. That is not verification — it is
   the absence of it. On its own it means nothing: plenty of real
   companies run catch-alls, and B2B is full of them.

   v5.6.0 flagged this only at ~30 hand-listed household-name domains,
   after the 18 Aug spam lead was blocked on a made-up domain, retried on
   meta.com — a catch-all at a big brand — and passed. That list could
   never be finished, the same way the free-email list could not before
   edit-distance matching replaced it.

   yo@yoyo.com is what the gap looked like from the other side. ELV
   returned 'ok_for_all' (yoyo.com accepts anything through
   amazon-smtp.amazon.com). The website check timed out and failed open,
   correctly. Neither half verified a thing, both halves passed, no flag
   anywhere, and the lead got a follow-up email.

   So the rule is not about which domain it is. It is: the email check
   could not confirm the mailbox AND the website check could not reach
   the site. Two "we could not tell"s at once, at ANY domain. Either
   signal alone is ordinary and stays silent.

   FLAG, NEVER BLOCK. Real people work at companies with catch-all mail
   and flaky websites, and both checks still fail open exactly as before.
   Derived at read time, never stored as a boolean: the recheck tool can
   change website_check_reason later, and a flag that has quietly gone
   stale is worse than no flag — you would trust its absence. */
const CATCHALL_STATUSES = ['ok_for_all', 'accept_all'];

function isUnverifiablePair(row) {
  if (!row) return false;
  const elv = normaliseElvStatus(row.elv_status);
  if (!CATCHALL_STATUSES.includes(elv)) return false;
  const reason = row.website_check_reason;
  if (!reason) return false; // no verdict is not a verdict
  return WEBSITE_UNREACHABLE_REASONS.includes(reason);
}

// One sentence, plain English, for Slack and the dashboard. SDRs read this.
const UNVERIFIABLE_PAIR_NOTE =
  'Their mail server accepts any address, so we could not confirm this mailbox exists — '
  + 'and we could not reach their website either. Nothing has verified this lead.';

function elvSoftTypoHint(email) {
  const raw       = String(email || '');
  const localPart = raw.split('@')[0] || '';
  const domain    = (raw.split('@')[1] || '').toLowerCase();
  if (!domain) return null;

  const match = freeEmailMatch(domain);
  // Only hint on a NEAR-MISS of a free provider. An exact match is a real
  // provider, and an unrelated business domain must never be "corrected".
  if (!match || match.exact) return null;

  const candidate = `${localPart}@${match.domain}`;
  if (!EMAIL_SYNTAX_RE.test(candidate)) return null;
  return { suggestion: candidate, suggestedDomain: match.domain };
}

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
    const elvPromise = fetch(elvCheckUrl(email, apiKey), { signal: controller.signal });
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
      persistElvVerdict(email, 'domain_error', false, 'local');
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
    // Written here so /partial and /submit can read back HOW a lead passed,
    // not just that it did. Not awaited — the lead is waiting on this call.
    persistElvVerdict(email, status, valid, 'elv');
    if (valid) {
      // v5.7.1 — the user-facing hint now lives in gushwork-form.js, computed
      // locally so an ELV timeout cannot swallow it (gmailc.com timed out at
      // 8002ms on 20 Aug and no hint appeared). Kept here purely as a log
      // line: one source of truth for what the lead sees, and still visible
      // to us in Railway.
      const hint = elvSoftTypoHint(email);
      if (hint) console.log(`[ELV] 💡 ${email} passed but looks like a typo of ${hint.suggestedDomain}`);
      // A catch-all no longer alerts from here. It cannot: on its own it
      // means nothing, and the website check that decides whether it means
      // anything has not run yet at email-blur time. The verdict is stored
      // above and the pair is judged in /submit — see isUnverifiablePair.
      if (CATCHALL_STATUSES.includes(status)) {
        console.log(`[ELV] ${email} — catch-all domain, mailbox existence NOT verified (stored; judged with the website check at submit)`);
      }
      return res.json({ valid: true, status, ms });
    }
    res.json(Object.assign(elvRejection(email, status), { ms }));
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
  /* Was the only /monitor* route with no token check. It reports
     verification volume, the inconclusive rate and whether the upstream is
     degraded right now — operational state, and a map of when we are least
     able to verify an address. Gated like the rest. */
  const token = process.env.MONITOR_TOKEN;
  if (token && req.query.token !== token) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
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

/* ── PartnerStack: the customer key ──────────────────────────────────
   PartnerStack counts ONE conversion per customer key, for the life of the
   account. Get the key wrong twice for the same company and either the
   affiliate is paid twice for one customer, or the second real referral is
   silently swallowed as a duplicate. So it is derived here and nowhere else.

   Normalised root domain: lowercase, no protocol, no port, no path, no www,
   no subdomain. acme.com — never Acme.com, www.acme.com or mail.acme.com.
   Subdomain stripping rides on registrableDomain above, so acme.co.uk stays
   acme.co.uk rather than collapsing to co.uk.

   Accepts a website URL, a bare domain or an email address, because the two
   places that need a key have different fields to hand.

   Returns null rather than a guess when there is nothing usable. A free-mail
   address is deliberately null: gmail.com as a customer key would merge every
   Gmail lead into one PartnerStack customer, and since the conversion fires
   once per key forever, the first one would permanently burn the conversion
   for everybody after it. */
function partnerStackCustomerKey(input) {
  let raw = String(input || '').trim().toLowerCase();
  if (!raw) return null;

  // An email: take what is after the last @, so display names cannot confuse it.
  if (raw.includes('@')) raw = raw.slice(raw.lastIndexOf('@') + 1);

  raw = raw.replace(/^[a-z][a-z0-9+.-]*:\/\//, '');   // scheme
  raw = raw.replace(/^[^/@]*@/, '');                   // userinfo
  raw = raw.split(/[/?#]/)[0];                         // path, query, fragment
  raw = raw.split(':')[0];                             // port
  /* registrableDomain below already collapses a leading www. and a trailing
     dot, so these two are belt-and-braces rather than load-bearing — a
     mutation test on this line survives. Kept because the intent of the
     function is "no www, no trailing dot" and a reader should not have to
     derive that from the suffix table. */
  raw = raw.replace(/^www\./, '').replace(/\.$/, '');
  if (!raw || !raw.includes('.')) return null;
  if (!/^[a-z0-9.-]+$/.test(raw)) return null;
  /* An IP literal is not a company. registrableDomain deliberately passes IPv4
     straight through (a website check cares that the host resolves), but as a
     customer key it would be nonsense — and shared hosting means several
     unrelated companies can sit behind one address. */
  if (/^\d{1,3}(\.\d{1,3}){3}$/.test(raw)) return null;

  const key = registrableDomain(raw);
  if (!key || !key.includes('.')) return null;
  if (isFreeEmailDomain(key)) return null;
  return key;
}

function isMarketplaceHost(hostname) {
  const reg = registrableDomain(hostname);
  return MARKETPLACE_DOMAINS.includes(reg) ? reg : null;
}

// Hosting PLACEHOLDER hosts — the temporary address a host gives you before
// your real domain is connected. Detected the same way as marketplaces: by
// where the redirect LANDS, not by reading the page. That matters because a
// placeholder page has plenty of content — luctd.com lands on
// luctd.bgi.ufj.mybluehost.me showing a heading, a paragraph, a signup form
// and footer links, so every "does this page have substance" test passes it.
// It is still not a website.
//
// Deliberately SHORT. Only addresses that exist purely during setup, which
// nobody would choose to stay on. Website builder subdomains (wixsite.com,
// godaddysites.com, myshopify.com…) are NOT here: a small business genuinely
// running on theirshop.wixsite.com is real, just unpolished, and flagging
// them would be wrong. Add more only with a real example in the logs.
const HOSTING_PLACEHOLDER_DOMAINS = ['mybluehost.me', 'hostingersite.com'];

function isHostingPlaceholder(hostname) {
  const reg = registrableDomain(hostname);
  return HOSTING_PLACEHOLDER_DOMAINS.includes(reg) ? reg : null;
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

/* ── Search-result URL unwrapping ─────────────────────────────────
   v5.8.0. A lead pasted this into the website field:

     https://www.google.com/url?q=https://phpagency.com/&sa=u&ved=...

   That is what you get when you copy a link out of a Google results page
   instead of the address bar. The verdict was 'brand_mismatch' — we told a
   real agency they had typed a well-known brand's site. Their actual
   domain was sitting inside the URL the whole time.

   Only unwraps when the wrapper is a KNOWN redirector and the extracted
   target is an absolute http(s) URL. Anything else is returned untouched,
   so a normal address can never be rewritten into something else.

   NOTE: the form blocks 'brand_mismatch' client-side, so for live traffic
   the same unwrap has to happen in gushwork-form.js to spare the lead the
   error. This copy covers /verify-website, /resolve-website and the
   historical recheck. */
const REDIRECT_WRAPPERS = {
  'google.com': ['q', 'url'],
  'www.google.com': ['q', 'url'],
  'l.facebook.com': ['u'],
  'lm.facebook.com': ['u'],
  'l.instagram.com': ['u'],
  't.umblr.com': ['z'],
  'out.reddit.com': ['url'],
  'href.li': [],
};

function unwrapRedirectUrl(raw) {
  const value = String(raw || '').trim();
  if (!value) return value;
  let u;
  try { u = new URL(value.startsWith('http') ? value : 'https://' + value); }
  catch { return value; }

  const params = REDIRECT_WRAPPERS[u.hostname.toLowerCase()];
  if (!params) return value;

  const candidates = params.map((p) => u.searchParams.get(p)).filter(Boolean);
  for (const c of candidates) {
    try {
      const target = new URL(c);
      if (target.protocol === 'http:' || target.protocol === 'https:') return target.toString();
    } catch { /* not a usable URL — keep looking */ }
  }
  return value;
}

/* ── Bot wall detection ───────────────────────────────────────────
   v5.8.0. marathontechnology.com and reiser.com — both real, both live in
   a browser — were labelled "page looked mostly empty". They were not
   empty. SiteGround's bot protection had intercepted us and served a
   168-byte interstitial whose entire content is a meta refresh:

     status=202  server=nginx  html(168)
     <meta http-equiv="refresh" content="0;/.well-known/sgcaptcha/?r=%2F&y=ipr:...">

   Note the 202: it passes response.ok, so the ladder above never catches
   it and the stub falls through to the thin-content branch.

   Two rules, narrow to broad:
     1. a refresh pointing at SiteGround's captcha path — unambiguous
     2. ANY page small enough that the refresh IS the whole page

   Rule 2 can also match an old-fashioned "we moved" redirect stub. That
   costs nothing: both land on the same ok:true verdict thin content
   would have produced, so a wrong match changes the wording in Slack and
   nothing else. Returns null for a normal page. */
function detectCheckWall(html) {
  const s = String(html || '');
  const tag = s.match(/<meta[^>]+http-equiv\s*=\s*["']?refresh["']?[^>]*>/i);
  if (!tag) return null;
  /* Matched against the WHOLE tag, not a parsed url= parameter. SiteGround
     writes content="0;/.well-known/sgcaptcha/?r=%2F&y=ipr:..." — a bare path
     after the semicolon with no url= at all, which an url=-based parser
     misses entirely. Testing the tag also survives single quotes, missing
     quotes and upper-case attributes without a parser for each variation. */
  if (/\/\.well-known\/sgcaptcha\//i.test(tag[0])) return 'sgcaptcha';
  if (s.length <= 1024) return 'meta_refresh_stub';
  return null;
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
      // v5.4.0 — a real browser UA was already here, deliberately, because
      // domain monetisation networks serve BOTS a "for sale" page while
      // forwarding real visitors to the actual site (that's how afgmmoving.com
      // got mislabelled). But the UA was the ONLY header we sent, and Chrome
      // sends about ten. Claiming to be Chrome while sending none of Chrome's
      // other headers is itself a bot signature, and security plugins
      // fingerprint exactly that. Six unrelated real businesses on six
      // different hosts all returned byte-identical 73-character stubs in
      // three days — sites that load perfectly in a browser. Sending the
      // full set is the same intent as the original UA line, carried through.
      //
      // Accept-Encoding is DELIBERATELY omitted: undici sets and handles it,
      // and overriding it risks getting undecompressed bytes back as text.
      headers: {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Language': 'en-US,en;q=0.9',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1',
        'Sec-CH-UA': '"Chromium";v="126", "Google Chrome";v="126", "Not-A.Brand";v="99"',
        'Sec-CH-UA-Mobile': '?0',
        'Sec-CH-UA-Platform': '"macOS"',
      },
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
async function evaluateWebsite({ raw: rawInput, parkingHint = null, hasMX = false }) {
  const startedAt = Date.now(); // budgets the optional wildcard probe below
  // Repeated here because /verify-website calls this directly, bypassing
  // serverSideWebsiteCheck. unwrapRedirectUrl is idempotent — a plain
  // address is returned untouched — so unwrapping twice is harmless.
  const raw = unwrapRedirectUrl(rawInput);
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

  // TIER 1 — hosting placeholder. Tested HERE, before any content measurement,
  // because these pages are FULL of content — luctd.com lands on
  // luctd.bgi.ufj.mybluehost.me with a heading, a paragraph, a signup form and
  // footer links. Every "does this page have substance" test passes it, so it
  // sailed through as forwarded_to_live_site. Judged by destination identity,
  // never by reading the page.
  const placeholderHost = isHostingPlaceholder(finalHost);
  if (placeholderHost && redirectedOffDomain) {
    console.log(`[verify-website] NEGATIVE ${url.hostname} → hosting placeholder at ${placeholderHost} — site not built yet`);
    return ({ ok: false, reason: 'hosting_placeholder', matched: placeholderHost, canonical_url });
  }

  if (!response.ok) {
    console.log(`[verify-website] ${url.hostname} → HTTP ${response.status} — failing open`);
    return ({ ok: true, reason: 'http_' + response.status, canonical_url, redirected_off_domain: redirectedOffDomain });
  }
  const contentType = response.headers.get('content-type') || '';
  if (!contentType.includes('html')) return ({ ok: true, reason: 'non_html', canonical_url }); // fail open — not a page we can scan

  const rawHtml = (await response.text()).slice(0, 200000); // cap read size

  /* Tested BEFORE any content measurement. A bot wall is not a thin page —
     it is the site declining to show us the page at all, which says nothing
     about the business. Measuring it produces a verdict about our own
     blocked request. */
  const wall = detectCheckWall(rawHtml);
  if (wall) {
    console.log(`[verify-website] BLOCKED-BY-SITE ${url.hostname} — ${wall} (status=${response.status}, html=${rawHtml.length}) — site refused our check`);
    return ({ ok: true, reason: 'check_blocked', matched: wall, canonical_url });
  }

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

    /* ── DIAGNOSTIC (v5.4.0, temporary) ──────────────────────────────
       Six unrelated real businesses on six different hosts returned
       byte-identical 73-character stubs in three days — sites that load
       perfectly in a browser. The likely cause is that we were sending a
       Chrome User-Agent and none of Chrome's other headers, which is itself
       a bot signature; that is fixed in attemptFetch above. But "likely" is
       not "known", and writing a rule against an unread string is exactly
       how the Vercel IP got mislabelled as a Hostinger placeholder.
       So: print what we were actually served. If the header fix worked,
       these lines stop appearing. If they don't, we can finally read the
       page instead of theorising about it.
       REMOVE once the cause is confirmed — this logs page content. */
    try {
      const serverHdr = response.headers.get('server') || '-';
      const cfRay     = response.headers.get('cf-ray') ? 'yes' : 'no';
      const cfMit     = response.headers.get('cf-mitigated') || '-';
      const setCookie = response.headers.get('set-cookie') ? 'yes' : 'no';
      const bodySample = rawHtml.replace(/\s+/g, ' ').trim().slice(0, 300);
      const textSample = sub.visible.slice(0, 200);
      console.log(`[verify-website][diag] ${url.hostname} | final=${finalHost} | status=${response.status} | server=${serverHdr} | cf-ray=${cfRay} | cf-mitigated=${cfMit} | set-cookie=${setCookie} | title="${(sub.title || '').slice(0, 80)}"`);
      console.log(`[verify-website][diag] ${url.hostname} | text(${sub.textLen})="${textSample}"`);
      console.log(`[verify-website][diag] ${url.hostname} | html(${rawHtml.length})="${bodySample}"`);
    } catch (e) {
      console.log(`[verify-website][diag] ${url.hostname} — diagnostic failed (ignored): ${e && e.message}`);
    }

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

    /* v5.8.0 — three outcomes, not two. 'no_dns_records' used to be the
       catch-all for "not ENOTFOUND on both", which quietly merged a FACT
       with a NON-ANSWER:

         ENODATA    the domain exists, its nameservers answered, and there
                    is no address record. gslgraphics.com: live NS at
                    Media Temple, real SOA, no A record. A real finding.
         ESERVFAIL  the resolver could not get an answer at all — usually
                    dead or misconfigured nameservers, sometimes a resolver
                    hiccup. rosaainslie.com and nowebsite.com both do this.

       The second is not a fact about the domain, and 'no_dns_records'
       returns ok:false, so a momentary resolver failure was being written
       to the database as a verdict. That is the one rule this system is
       built on: we could not check must never be stored as we checked and
       it is bad. 'dns_unresolved' fails open and is deliberately absent
       from RECHECK_WRITEABLE, so it can never be persisted. */
    const NO_ANSWER = ['ESERVFAIL', 'ETIMEOUT', 'ETIMEDOUT', 'EREFUSED', 'ECONNREFUSED', 'EAI_AGAIN'];
    if (apexCode === 'ENOTFOUND' && wwwCode === 'ENOTFOUND') out.status = 'nxdomain';
    else if (NO_ANSWER.includes(apexCode) || NO_ANSWER.includes(wwwCode)) out.status = 'dns_unresolved';
    else out.status = 'no_dns_records';
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
  'parked_confirmed', 'for_sale_lander', 'marketplace_redirect', 'hosting_placeholder',
  'nxdomain', 'no_dns_records', 'mx_only',
];

/* Verdicts the recheck must NEVER overwrite. These come from
   localWebsiteVerdict() in gushwork-form.js and depend on the lead's EMAIL,
   which this route deliberately does not re-derive. Left alone, they stay
   correct; recomputed from content alone, a LinkedIn profile URL comes back
   as 'content_clean' because linkedin.com serves a real page. */
const RECHECK_PROTECTED = ['brand_mismatch', 'mailbox_domain', 'social_profile_url', 'test_email_skipped', 'unparseable'];

/* ── serverSideWebsiteCheck ───────────────────────────────────────
   Both stages, run entirely on the server: resolve the domain, then
   fetch the page. Used by the historical recheck AND by the live form's
   DNS fallback, so there is exactly one copy of the sequencing.

   v5.7.0 — the live form needs this because STAGE 1 normally runs in the
   VISITOR'S BROWSER over DNS-over-HTTPS. When their network blocks that,
   the check fails for reasons that have nothing to do with their website:
     firstcitizens.com  an 18,000-person bank; corporate networks routinely
                        block DoH because it bypasses their DNS monitoring
     ydrapid.com        Shenzhen; dns.google and cloudflare-dns.com are both
                        blocked in mainland China
   Both are real businesses with live sites and live mail. Railway has no
   such restrictions, so when the browser lookup fails we ask the server. */
async function serverSideWebsiteCheck(website) {
  // Unwrapped once, HERE, so the DNS lookup and the page fetch below both
  // operate on the lead's real domain rather than the redirector's.
  const raw = unwrapRedirectUrl(String(website || '').trim());
  if (!raw) return { ok: true, reason: 'empty' };

  let host;
  try {
    host = new URL(raw.startsWith('http') ? raw : 'https://' + raw).hostname.replace(/^www\./, '');
  } catch {
    return { ok: true, reason: 'unparseable' }; // format errors belong to the form
  }

  const dns = await resolveWebsiteDns(host);
  if (dns.status === 'nxdomain')       return { ok: false, reason: 'nxdomain', matched: 'ENOTFOUND on apex + www', hint: dns.parkingHint };
  if (dns.status === 'no_dns_records') return { ok: false, reason: 'no_dns_records', hint: dns.parkingHint };
  // Fails OPEN. Meta is still suppressed (dns_unresolved is not in
  // WEBSITE_VERIFIED_REASONS, exactly like doh_error and timeout), so this
  // costs attribution, never a lead.
  if (dns.status === 'dns_unresolved')  return { ok: true,  reason: 'dns_unresolved', matched: 'DNS gave no answer on apex + www', hint: dns.parkingHint };
  if (dns.status === 'mx_only')        return { ok: true,  reason: 'mx_only', matched: 'MX only — email-only company', hint: dns.parkingHint };

  const verdict = await evaluateWebsite({ raw, parkingHint: dns.parkingHint, hasMX: dns.hasMX });
  return Object.assign({}, verdict, { hint: dns.parkingHint });
}

/* ── /resolve-website — the form's DNS fallback ───────────────────
   Called ONLY when the browser's own DNS lookup failed or was
   inconclusive. Same fail-open policy as everything else: any error here
   returns a passing verdict rather than costing a real lead. */
app.post('/resolve-website', strictLimiter, async (req, res) => {
  const raw = (req.body.website || '').toString().trim().slice(0, 300);
  if (!raw) return res.json({ ok: true, reason: 'empty' });
  try {
    const verdict = await serverSideWebsiteCheck(raw);
    console.log(`[resolve-website] ${raw} → ${verdict.reason}${verdict.hint ? ` (hint ${verdict.hint})` : ''} — browser DNS was blocked`);
    res.json(verdict);
  } catch (err) {
    console.error('[resolve-website] Unexpected error — failing open:', err.message);
    res.json({ ok: true, reason: 'backend_error' });
  }
});

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

     POST /monitor/website-recheck?token=…              dry run (DEFAULT)
     POST /monitor/website-recheck?token=…&apply=1      writes
     &scope=unverified|clean|all   default all
     &limit=200   &offset=0   &format=json

   POST, not GET, and that is the point. It was a GET that rewrites lead
   rows and runs two ALTER TABLEs, so a link prefetch, a chat client
   unfurling the URL or a browser restoring the tab could have fired it
   with apply=1 still in the query string. Nothing in the UI calls it, so
   the change costs nothing on the client. Run it with:
     curl -X POST "https://…/monitor/website-recheck?token=…&apply=1"

   Dry run is the default and the ONLY thing that writes is apply=1.
   Never touches social_profile_url rows or rows with no reason recorded.
   Sequential with a delay — this must not look like a burst of scraping.
   ======================================================= */
app.post('/monitor/website-recheck', async (req, res) => {
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
        // v5.7.0 — both stages now live in serverSideWebsiteCheck(), shared
        // with the live form's /resolve-website fallback. One copy of the
        // sequencing: resolve first (or a non-existent domain merely fails to
        // fetch and looks 'unreachable', silently downgrading an accurate,
        // blocking 'nxdomain'), then fetch.
        // no_dns_records returns ok:false to match form.js — they disagreed,
        // which is why gslgraphics.com showed flagged while RosaAinslie.com
        // did not for the identical verdict.
        verdict = await serverSideWebsiteCheck(website);
        hint = verdict.hint || null;
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



/* ============================================================
   PARTNERSTACK — ELIGIBILITY

   Runs BEFORE any PartnerStack call. Nothing in here touches the lead:
   eligibility decides whether an AFFILIATE gets paid, never whether a
   person gets a demo. A lead that fails every check below still books,
   still fires Slack, Salesforce and Meta exactly as it does today.

   Three rejection rules, in the order they are cheapest to answer:

     c) test address        — our own testing must not pay a partner
     b) current customer    — they were already ours
     a) prior contact       — we had already reached this domain before
                              the click, so the partner did not source them

   Every rejection carries a stable reason string. That is not decoration:
   the affiliate agreement obliges us to say WHY a referral was rejected,
   and "the code said no" is not an answer three months later. The reasons
   are written to leads.ps_ineligible_reason so the record outlives the log.

   FAILS CLOSED. If a check cannot run — database unreachable, query
   timeout — this returns ineligible with reason 'check_failed' rather than
   waving the conversion through. That is the opposite of the fail-open rule
   the lead path follows, and deliberately so: the conversion call fires ONCE
   per customer key for the life of the account and cannot be recalled, while
   a conversion we skipped is still sitting in the log to be sent by hand.
   Wrong in the cautious direction is recoverable; wrong in the other
   direction is not.
   ============================================================ */

/* Our own test traffic. Deliberately a PartnerStack-only guard rather than a
   change to the shared exclusion lists: ELV_EXCLUDED_DOMAINS today governs
   ELV health and alerting only, and every lead count on the dashboard still
   deliberately INCLUDES internal addresses. Widening that list here would
   move historical numbers across the whole dashboard as a side effect. */
const PS_TEST_EMAILS = ['b@g.ai'];

function isPartnerStackTestEmail(email) {
  const e = String(email || '').trim().toLowerCase();
  if (!e) return false;
  if (PS_TEST_EMAILS.includes(e)) return true;
  const domain = e.slice(e.lastIndexOf('@') + 1);
  return ELV_EXCLUDED_DOMAINS.includes(domain);
}

/* ── Rule (a): prior contact ─────────────────────────────────────────
   The source is behind this registry ON PURPOSE. Today "contact" means a
   prior inbound form lead on the same root domain, in Railway leads. The
   warehouse also holds two live outbound logs — gist.gtm_coldemail_sends_master
   (cold email, indexed on domain) and gist.gtm_outbound_multisource (dials,
   currently unindexed on website_url) — and either may be switched on later.

   Measured before choosing, against 90 days of real leads: prior form lead
   9.7%, cold email sent in the 90 days before the submit 25.6%, dials 4.9%.
   A naive "emailed in the last 90 calendar days" reading looked like 39.6%,
   but 17.4 points of that was our own sequencer following UP on an inbound
   lead, which is not prior contact at all. Adding a source without measuring
   it the same way will silently change how many affiliates get rejected.

   To add one: write the function, add it to PS_CONTACT_SOURCES, and put its
   key in PS_CONTACT_ACTIVE. Nothing else changes. */
const PS_CONTACT_LOOKBACK_DAYS = 90;

const PS_CONTACT_SOURCES = {
  /* Prior inbound form leads on the same root domain.
     Normalisation happens in JS, not SQL, and that is the whole point: the
     SQL we could write here strips www but not subdomains, so mail.acme.com
     would fail to match acme.com and the rule would quietly under-reject.
     Reading the window and normalising each row through the same helper the
     key itself came from is the only way the two sides can agree. The window
     is bounded and leads is small; leads_created_at_idx covers it. */
  form_leads: async (customerKey, since, until) => {
    const { rows } = await pool.query(
      `SELECT email, website, created_at FROM leads
        WHERE created_at >= $1 AND created_at < $2`,
      [since, until]
    );
    for (const r of rows) {
      const k = partnerStackCustomerKey(r.website) || partnerStackCustomerKey(r.email);
      if (k && k === customerKey) {
        return { hit: true, at: r.created_at, detail: 'prior form lead' };
      }
    }
    return { hit: false };
  },
};

const PS_CONTACT_ACTIVE = ['form_leads'];

async function partnerStackPriorContact(customerKey, clickAt) {
  const until = clickAt instanceof Date ? clickAt : new Date(clickAt || Date.now());
  const since = new Date(until.getTime() - PS_CONTACT_LOOKBACK_DAYS * 86400000);
  for (const name of PS_CONTACT_ACTIVE) {
    const src = PS_CONTACT_SOURCES[name];
    if (!src) continue;
    const out = await src(customerKey, since, until);
    if (out && out.hit) return { ...out, source: name };
  }
  return { hit: false };
}

/* ── Rule (b): current customer ──────────────────────────────────────
   Three warehouse tables hold customer domains and none of them agrees with
   the others on format: customer_contract_terms has clean domains,
   customer_enrichment stores things like https://www.example.com/ and
   gist_accountsmaster stores example.com/ with a trailing slash. They are
   unioned here and every value is put through partnerStackCustomerKey, so
   the comparison happens on one normalised form on both sides.

   Cached rather than queried per submit. It is 853 rows across the three
   tables, it changes on a human timescale, and the warehouse is across a WAN
   from Railway — the same reason /monitor/health is kept off the 60s poll.

   THE 12-MONTH HALF OF THIS RULE IS NOT IMPLEMENTED, because it cannot be.
   Nothing in the warehouse can date a churn: customer_contract_terms has
   zero churned rows at all, gist_accountsmaster has an End_Date on 2 of its
   330 rows, and public.subscriptions has no row with a future billing date.
   So "was a customer in the last 12 months" is reported as an explicit gap
   for manual review rather than silently passing as "not a customer". The
   clause stays in the affiliate terms; we just cannot yet enforce it here. */
const PS_CUSTOMER_CACHE_TTL_MS = 30 * 60 * 1000;

/* The query crosses a WAN to the warehouse and the pool it uses has NO
   statement_timeout — connectionTimeoutMillis caps acquiring a connection, not
   a query that has already started. An RDS instance that accepts connections
   but answers slowly is exactly what a degraded WAN produces, and unbounded
   that hangs forever. Three of those exhaust awsPool (max: 3), which also
   starves the mirror writes in syncToAWS.

   8s to match HEALTH_AWS_TIMEOUT_MS, for the same reason: a hung warehouse has
   to resolve to a verdict rather than hang the caller. A timeout surfaces as a
   rejected promise, so the fail-closed path in partnerStackEligibility already
   handles it as 'check_failed' — note that fail-closed alone does NOT cover
   this, because a hang never reaches the catch. Measured cost of the query
   when healthy: 0.8-3.4s including the round trip. */
const PS_CUSTOMER_QUERY_TIMEOUT_MS = 8000;

let _psCustomerCache = { at: 0, keys: null };

async function partnerStackCustomerDomains() {
  if (_psCustomerCache.keys && Date.now() - _psCustomerCache.at < PS_CUSTOMER_CACHE_TTL_MS) {
    return _psCustomerCache.keys;
  }
  if (!awsPool) throw new Error('AWS warehouse pool not configured');
  const { rows } = await withTimeout(awsPool.query(`
    SELECT domain AS d, status FROM gist.customer_contract_terms WHERE domain IS NOT NULL
    UNION ALL
    SELECT url,          status FROM gist.gist_accountsmaster     WHERE url    IS NOT NULL
    UNION ALL
    SELECT domain,       NULL   FROM gist.customer_enrichment     WHERE domain IS NOT NULL
  `), PS_CUSTOMER_QUERY_TIMEOUT_MS, 'PartnerStack customer domains');
  const keys = new Set();
  for (const r of rows) {
    // Churned, inactive and the test rows are not CURRENT customers. They are
    // exactly the population the unenforceable 12-month clause would cover.
    if (r.status && /^(churn|inactive|dummy)/i.test(String(r.status).trim())) continue;
    const k = partnerStackCustomerKey(r.d);
    if (k) keys.add(k);
  }
  _psCustomerCache = { at: Date.now(), keys };
  console.log(`[PartnerStack] Customer domain cache refreshed: ${keys.size} domains`);
  return keys;
}

/* Warmed at boot and on a timer so no LEAD ever pays for the fetch. Filling
   this cache from inside a submit put a multi-second cross-WAN query on the
   critical path of someone waiting on a form, which is the thing this codebase
   exists not to do.

   Deliberately swallows its error. A cold cache is not an outage: the next
   eligibility check falls back to fetching on demand, bounded by the timeout
   above, and rejects closed if that fails too. Warming must never be able to
   take the process down or spam alerts on a schedule. */
function refreshPartnerStackCustomerCache(reason) {
  if (!awsPool) return;
  partnerStackCustomerDomains()
    .catch(err => console.warn(`[PartnerStack] Customer cache warm failed (${reason}, non-blocking):`, err.message));
}

function startPartnerStackCacheWarm() {
  /* Nothing reads this cache while the eligibility check is off, and warming it
     anyway is a pointless cross-WAN query every 30 minutes forever. */
  if (!PS_ELIGIBILITY_ENABLED) {
    console.log('[PartnerStack] Eligibility check disabled — customer cache not warmed');
    return;
  }
  refreshPartnerStackCustomerCache('boot');
  const t = setInterval(() => refreshPartnerStackCustomerCache('scheduled'), PS_CUSTOMER_CACHE_TTL_MS);
  if (t.unref) t.unref();
}

/* The verdict. Reason strings are stable identifiers — they are stored, and
   they are what an affiliate is eventually told. Do not reword them casually. */
async function partnerStackEligibility({ email, website, customer_key, click_at } = {}) {
  const stamp = (out) => ({ ...out, checked_at: new Date() });

  if (isPartnerStackTestEmail(email)) {
    return stamp({ eligible: false, reason: 'test_email',
      detail: 'Internal or test address — our own testing must not pay a partner.' });
  }

  const key = customer_key || partnerStackCustomerKey(website) || partnerStackCustomerKey(email);
  if (!key) {
    return stamp({ eligible: false, reason: 'no_customer_key',
      detail: 'No usable company domain on the lead (missing, malformed, or a free mail provider).' });
  }

  let customers;
  try {
    customers = await partnerStackCustomerDomains();
  } catch (err) {
    console.warn('[PartnerStack] Customer check FAILED — failing closed:', err.message);
    recordFailure('PartnerStack', key + ' (customer check)', err.message);
    return stamp({ eligible: false, reason: 'check_failed', customer_key: key,
      detail: 'Could not read the customer list, so eligibility could not be decided. Re-run before telling the affiliate anything.' });
  }
  if (customers.has(key)) {
    return stamp({ eligible: false, reason: 'existing_customer', customer_key: key,
      detail: `${key} is on the current customer list.` });
  }

  let contact;
  try {
    contact = await partnerStackPriorContact(key, click_at);
  } catch (err) {
    console.warn('[PartnerStack] Prior-contact check FAILED — failing closed:', err.message);
    recordFailure('PartnerStack', key + ' (contact check)', err.message);
    return stamp({ eligible: false, reason: 'check_failed', customer_key: key,
      detail: 'Could not read prior contact history, so eligibility could not be decided. Re-run before telling the affiliate anything.' });
  }
  if (contact.hit) {
    return stamp({ eligible: false, reason: 'prior_contact_90d', customer_key: key,
      detail: `We already had contact with ${key} within ${PS_CONTACT_LOOKBACK_DAYS} days before the click (${contact.detail}).` });
  }

  return stamp({
    eligible: true, reason: 'eligible', customer_key: key,
    /* Surfaced on every PASS, not buried. An affiliate paid on a lead that
       turns out to have been a customer eleven months ago is a conversation
       we want to have from a record, not from memory. */
    unverified: ['customer_last_12_months'],
  });
}

/* ── Reading the PartnerStack fields off the form payload ────────────
   One reader for /partial and /submit. They took separate copies of every
   other field and that is exactly how the two forms drifted apart before;
   there is no reason to repeat it for a field set this fiddly.

   ps_click_history is the only thing on the whole payload that arrives as
   caller-supplied JSON destined for a JSONB column, so it is rebuilt entry by
   entry rather than passed through. A visitor can set their own cookies, and
   "store as-is" cannot mean "store whatever bytes turn up": unbounded, this
   is a free write amplifier into our database.

   The cap of 10 mirrors the cookie's own cap. Entries are kept oldest-first,
   as the cookie writes them, and the LAST one is the click that won — but
   nothing here depends on that, because attribution reads ps_xid and only
   ps_xid. This is for reporting and dispute resolution. */
const PS_CLICK_HISTORY_MAX = 10;

function parsePartnerStackClickAt(raw) {
  if (raw === null || raw === undefined || raw === '') return null;
  let d;
  if (typeof raw === 'number' || /^\d{9,14}$/.test(String(raw).trim())) {
    const n = Number(raw);
    // 10-digit values are seconds, 13-digit are milliseconds.
    d = new Date(n < 1e11 ? n * 1000 : n);
  } else {
    d = new Date(String(raw).trim());
  }
  if (isNaN(d.getTime())) return null;
  /* A click cannot be in the future or before the integration existed. A bad
     value here would silently move the 90-day contact window, so it is dropped
     rather than trusted; the caller then falls back to submit time. */
  const now = Date.now();
  if (d.getTime() > now + 86400000) return null;
  if (d.getTime() < Date.UTC(2026, 0, 1)) return null;
  return d;
}

/* The gw_ps_clicks cookie has carried the partner key BOTH ways: the lead of
   4 Sept 11:48 stored Nzg1ZWM3OGUxZWU0Njg4 (the base64 URL-param form) and
   every lead from 12:07 stored 785ec78e1ee4688 (decoded), because the
   site-wide script changed between them. Same partner, two strings, one JSONB
   column — anything grouping on it splits one partner into two.

   The ps_partner_key COLUMN was decoded throughout, so nothing today groups on
   the history and the damage was confined to one row. Normalising on write is
   defence against the cookie regressing again rather than a fix for a live
   bug.

   ROUND-TRIP GUARDED: only decode when re-encoding reproduces the input
   exactly AND the result looks like a key. Otherwise an already-decoded value
   that happens to be valid base64 would be mangled — decoding is not safe to
   attempt blindly. */
function normalisePartnerKey(pk) {
  if (!pk) return '';
  if (!/^[A-Za-z0-9+/]+={0,2}$/.test(pk) || pk.length % 4 !== 0) return pk.slice(0, 120);
  try {
    const decoded = Buffer.from(pk, 'base64').toString('utf8');
    const reencoded = Buffer.from(decoded, 'utf8').toString('base64');
    if (reencoded !== pk) return pk.slice(0, 120);
    if (!/^[A-Za-z0-9._-]{6,120}$/.test(decoded)) return pk.slice(0, 120);
    return decoded;
  } catch {
    return pk.slice(0, 120);
  }
}

function parsePartnerStackClickHistory(raw) {
  let arr = raw;
  if (typeof arr === 'string') {
    try { arr = JSON.parse(arr); } catch { return null; }
  }
  if (!Array.isArray(arr) || arr.length === 0) return null;
  const out = [];
  for (const item of arr.slice(0, PS_CLICK_HISTORY_MAX)) {
    if (!item || typeof item !== 'object' || Array.isArray(item)) continue;
    const xid = (item.xid || '').toString().trim().slice(0, 200);
    const pk  = normalisePartnerKey((item.pk || '').toString().trim().slice(0, 200));
    const at  = parsePartnerStackClickAt(item.at);
    if (!xid && !pk) continue;
    out.push({ xid: xid || null, pk: pk || null, at: at ? at.toISOString() : null });
  }
  return out.length ? out : null;
}

/* The request-level signals PartnerStack's fraud detection wants. None of these
   is stored on the lead — `leads` has no IP or user-agent column — so they are
   read off the request at the moment of submit.

   ip_address takes the FIRST entry of x-forwarded-for, not the whole header.
   app.set('trust proxy', 1) means req.ip is already resolved, but Railway sits
   behind a proxy chain and the raw header is a comma-separated list; sending
   "1.2.3.4, 10.0.0.1" as an IP address is worse than sending nothing, because
   it looks like a value and will never match anything.

   origin is the FULL page URL the form was on, not the scheme+host of it and
   not the browser's Origin header. The Origin header is absent on same-origin
   non-CORS posts, and a bare scheme+host cannot tell /demo from an ads lander,
   which is exactly the distinction a fraud review wants to see. */
function readPartnerStackRequestContext(req, page_url) {
  const fwd = (req.headers['x-forwarded-for'] || '').toString();
  const ip_address = (fwd.split(',')[0] || '').trim() || req.ip || null;
  const user_agent = (req.headers['user-agent'] || '').toString().slice(0, 500) || null;
  const origin = (page_url || '').toString().trim().slice(0, 1000) || null;
  return { ip_address, user_agent, origin };
}

function readPartnerStackPayload(body, { email, website } = {}) {
  const ps_xid         = (body.ps_xid         || '').toString().trim().slice(0, 200);
  const ps_partner_key = (body.ps_partner_key || '').toString().trim().slice(0, 120);
  const ps_click_at    = parsePartnerStackClickAt(body.click_at);
  const ps_click_history = parsePartnerStackClickHistory(body.ps_click_history);
  /* Derived here, once, and stored. Everything downstream joins on it, and
     re-deriving it at each call site is how two call sites end up disagreeing
     about which domain a company is. */
  const ps_customer_key = partnerStackCustomerKey(website) || partnerStackCustomerKey(email);
  return {
    ps_xid: ps_xid || null,
    ps_partner_key: ps_partner_key || null,
    ps_customer_key: ps_customer_key || null,
    ps_click_at,
    ps_click_history,
  };
}

/* MVP ships WITHOUT the automated eligibility check. Rejections are decided by
   hand at payout approval instead, which is a deliberate v2 deferral and not an
   oversight — every piece of the check below is built, tested and dormant.
   Flip PS_ELIGIBILITY_ENABLED=true in the Railway env to turn it on; no rebuild,
   no code change. Default off.

   The conversion call in runPartnerStackSignup does NOT consult it and must not
   start to without that being a deliberate decision: today every partner lead
   that is not a test address converts. */
const PS_ELIGIBILITY_ENABLED = process.env.PS_ELIGIBILITY_ENABLED === 'true';

/* ── Running the check WITHOUT making the lead wait ──────────────────
   Called after res.json(), never before it. Two separate reasons, and both
   matter:

     - Rule (b) reads the warehouse across a WAN. Even warmed and bounded at
       8s, that has no business sitting between a visitor and their booking.
     - Nothing about the answer changes what the lead sees. Eligibility decides
       whether an AFFILIATE is paid. The lead has already been written,
       Slack has fired, Salesforce has fired, Meta has fired.

   Fire-and-forget, same shape as finaliseElvVerdict and syncToAWS above it. A
   thrown promise here must not turn a successful submit into a 500, so the
   catch is total.

   Runs ONLY for partner-referred leads. Without ps_xid there is no affiliate
   to pay or reject, and a Railway query per submit for every organic lead is
   pure cost.

   NOTE for step 5: this is where the conversion call goes, AFTER this verdict
   and only when eligible. Do not move it earlier to "save a round trip".

   The stored verdict is the audit record. ps_ineligible_reason carries the
   reason on a rejection and stays null on a pass; the unenforceable 12-month
   clause is a constant on every pass, so "SELECT ... WHERE ps_eligible IS TRUE"
   IS the manual-review list without needing its own column. */
function runPartnerStackEligibility({ session_id, email, website, ps }) {
  if (!PS_ELIGIBILITY_ENABLED) return;
  if (!ps || !ps.ps_xid) return;
  partnerStackEligibility({
    email,
    website,
    customer_key: ps.ps_customer_key,
    click_at: ps.ps_click_at,
  })
    .then((verdict) => {
      logPartnerStackEligibility(verdict, { email, session_id });
      return pool.query(
        `UPDATE leads
            SET ps_eligible = $2, ps_ineligible_reason = $3, ps_checked_at = $4, updated_at = NOW()
          WHERE session_id = $1`,
        [session_id, verdict.eligible, verdict.eligible ? null : verdict.reason, verdict.checked_at]
      );
    })
    .catch((err) => {
      console.warn('[PartnerStack] Eligibility check failed (non-blocking):', err.message);
      recordFailure('PartnerStack', (email || session_id) + ' (eligibility)', err.message);
    });
}

/* ── STEP 6: who is the partner? ─────────────────────────────────────
   The cookie gives us a key like 785ec78e1ee4688, which is meaningless to an
   SDR reading Slack. This turns it into a name and an email, once per key.

   THREE layers, cheapest first, because the same handful of partners will
   send most of the traffic and re-asking the API for each of their leads is
   pure waste:
     1. process memory, for the life of the dyno
     2. any earlier lead row already carrying a resolved name for that key
     3. the v2 API — Basic base64(public_key:secret_key), NOT the tracking
        token that the conversion endpoint uses

   Deliberately does NOT gate anything. An unresolved partner is a cosmetic
   problem: Slack falls back to the raw key, the dashboard falls back to the
   raw key, and the conversion has already fired regardless. So every failure
   path here is a warning and a return, never a throw. */
const _psPartnerCache = new Map();

/* What to SHOW for a partner, in falling order of usefulness. An email tells an
   SDR who they are dealing with; a hex key tells them nothing and cannot be
   searched for in any system they use. One chain, used by Slack, the dashboard
   and hear_about_us, so the three cannot disagree about the same partner. */
function partnerDisplayName(identity, partnerKey) {
  return (identity && identity.name) || (identity && identity.email) || partnerKey || null;
}

/* Memory, then the database. NO network — this is the version that is safe to
   await before Slack fires.

   The memory-only peek this replaces was the bug: every deploy clears the Map,
   so the first partner lead after a restart posted to Slack with a raw hex key
   even though the row from an earlier lead already carried the name. The API
   call is the slow part and stays deferred; the DB layer is a single indexed
   lookup (leads_ps_partner_key_resolved_idx) and only runs when there is a
   partner key at all, so organic leads pay nothing. */
async function partnerIdentityNoNetwork(partnerKey) {
  if (!partnerKey) return null;
  if (_psPartnerCache.has(partnerKey)) return _psPartnerCache.get(partnerKey);
  try {
    const { rows } = await pool.query(
      `SELECT ps_partner_name, ps_partner_email
         FROM leads
        WHERE ps_partner_key = $1
          AND (ps_partner_name IS NOT NULL OR ps_partner_email IS NOT NULL)
        LIMIT 1`,
      [partnerKey]
    );
    if (rows[0]) {
      const known = { name: rows[0].ps_partner_name, email: rows[0].ps_partner_email };
      _psPartnerCache.set(partnerKey, known);
      return known;
    }
  } catch (err) {
    console.warn('[PartnerStack] Partner identity lookup failed (non-blocking):', err.message);
  }
  return null;
}

async function resolvePartnerIdentity(partnerKey) {
  if (!partnerKey) return null;
  // Memory, then any earlier lead row that already paid for this lookup.
  const known = await partnerIdentityNoNetwork(partnerKey);
  if (known) return known;

  const out = await fetchPartnership(partnerKey);
  if (!out.ok) {
    console.warn(`[PartnerStack] Could not resolve partner ${partnerKey}: ${out.reason}`);
    /* NOT cached. A failure is usually transient (rate limit, blip) and
       caching it would pin every future lead from this partner to "unknown"
       for the life of the process. */
    return null;
  }
  const identity = { name: out.name, email: out.email };
  _psPartnerCache.set(partnerKey, identity);
  console.log(`[PartnerStack] Resolved partner ${partnerKey} -> ${identity.name || '(no name)'} <${identity.email || 'no email'}>`);
  return identity;
}

/* Resolve and stamp the row. Returns the identity so the caller can put it in
   Slack without a second lookup. */
async function runPartnerStackIdentity({ session_id, ps }) {
  if (!ps || !ps.ps_partner_key) return null;
  const identity = await resolvePartnerIdentity(ps.ps_partner_key);
  if (!identity || (!identity.name && !identity.email)) return null;
  try {
    await pool.query(
      `UPDATE leads
          SET ps_partner_name  = COALESCE($2, ps_partner_name),
              ps_partner_email = COALESCE($3, ps_partner_email),
              updated_at = NOW()
        WHERE session_id = $1`,
      [session_id, identity.name || null, identity.email || null]
    );
  } catch (err) {
    console.warn('[PartnerStack] Could not store the partner identity (non-blocking):', err.message);
  }
  syncPartnerIdentityToAWS(session_id, identity.name, identity.email);
  return identity;
}

/* ── STEP 7: hear_about_us on a partner lead ─────────────────────────
   "Partner - Jane Smith" is what an AE sees in Salesforce, and it is the only
   place the partner shows up in their workflow at all.

   AN EXISTING REFERRAL WINS. gw_ref_email is a named human vouching for this
   lead, set by prefillHearAboutUs in both form files as "Referral - <email>",
   and it is a stronger signal than an affiliate link. If someone arrives on a
   referral link AND a partner link, the referral is what the AE should see.
   Anything already starting with "Referral -" is therefore left untouched.

   Falls back to the raw partner key when the name has not resolved yet, which
   happens for the very first lead from a brand-new partner: the resolver runs
   after res.json() and the Salesforce push has already gone. That row is
   upgraded in place by upgradePartnerHearAboutUs once the name lands. */
const PS_HEAR_PREFIX = 'Partner - ';

function partnerHearAboutUs({ hear_about_us, ps, identity }) {
  if (!ps || !ps.ps_partner_key) return null;
  const current = (hear_about_us || '').trim();
  if (/^referral\s*-/i.test(current)) return null;          // a human referral wins
  const label = partnerDisplayName(identity, ps.ps_partner_key);
  if (!label) return null;
  const next = PS_HEAR_PREFIX + label;
  return next === current ? null : next;
}

/* Once the name resolves, replace the key-shaped placeholder — in our row and
   in Salesforce, where the AE is actually looking. Only ever rewrites a value
   this code wrote: a referral, or anything a human typed, is left alone. */
async function upgradePartnerHearAboutUs({ session_id, email, ps, identity }) {
  if (!identity || !ps || !ps.ps_partner_key) return;
  const resolved = PS_HEAR_PREFIX + partnerDisplayName(identity, ps.ps_partner_key);
  /* Both weaker rungs of the display chain are upgradeable: a row may be
     carrying the raw key (nothing known yet) or the email (email known, name
     not). Only values THIS code wrote are candidates — a referral, or anything
     a human typed, is never touched. */
  const weaker = [PS_HEAR_PREFIX + ps.ps_partner_key];
  if (identity.email) weaker.push(PS_HEAR_PREFIX + identity.email);
  const candidates = weaker.filter(v => v !== resolved);
  if (!candidates.length) return;
  try {
    const { rowCount } = await pool.query(
      `UPDATE leads SET hear_about_us = $2, updated_at = NOW()
        WHERE session_id = $1 AND hear_about_us = ANY($3)`,
      [session_id, resolved, candidates]
    );
    if (!rowCount) return;                                   // nothing of ours to upgrade
    console.log(`[PartnerStack] hear_about_us upgraded to "${resolved}"`);
    syncHearAboutUsToAWS(session_id, resolved);
    const leadId = await findSFLeadByEmail(email);
    if (leadId) await updateSFLead(leadId, { hear_about_us__c: resolved });
  } catch (err) {
    console.warn('[PartnerStack] Could not upgrade hear_about_us (non-blocking):', err.message);
  }
}

/* The custom CUSTOMER field names these must match in PartnerStack Settings.
   Named as constants because a typo here is invisible: PartnerStack drops an
   unrecognised meta key silently, which looks identical to the integration
   working. If you rename a field there, rename it here in the same change. */
const PS_META_COMPANY = 'company_name';
const PS_META_WEBSITE = 'website';
const PS_META_PHONE   = 'phone';
const PS_CONVERSION_META_FIELDS = [PS_META_COMPANY, PS_META_WEBSITE, PS_META_PHONE];

/* ── Recording WHY a conversion or qualification did not happen ───────
   A SKIP is usually correct behaviour — a test address, a disqualified lead.
   A FAILURE is money not being paid. Before these were stored the two were
   indistinguishable on screen: both rendered as "not sent", and today's 400
   on the qualification wrote nothing anywhere at all.

   Every write is best-effort. None of this is on a lead's critical path and a
   failure to record a failure must not become a second failure. */
async function recordPartnerStackSkip(session_id, reason) {
  try {
    await pool.query(
      `UPDATE leads
          SET ps_signup_skipped_reason = $2, ps_signup_skipped_at = NOW(), updated_at = NOW()
        WHERE session_id = $1`,
      [session_id, reason]
    );
  } catch (err) {
    console.warn('[PartnerStack] Could not record skip reason (non-blocking):', err.message);
  }
}

/* Stamped where the claim is RELEASED, so the two always move together: a
   released claim without a recorded reason is exactly the invisible state
   this batch exists to remove.

   Alerts immediately rather than waiting for someone to read a dashboard.
   Today's 400 was invisible because nobody was watching, and a state you have
   to remember to check is half a fix. alertOps de-duplicates on
   severity:source:title with a 3h cooldown for critical, so a systemic outage
   sends one alert naming the scale rather than one per lead. */
async function recordPartnerStackFailure(kind, { session_id, customer_key, email, reason, detail }) {
  const isSignup = kind === 'signup';
  const cols = isSignup
    ? 'ps_signup_failed_at = NOW(), ps_signup_fail_reason = $2'
    : 'ps_qualify_failed_at = NOW(), ps_qualify_fail_reason = $2';
  try {
    await pool.query(
      `UPDATE leads SET ${cols}, updated_at = NOW() WHERE session_id = $1`,
      [session_id, String(reason || 'unknown').slice(0, 200)]
    );
  } catch (err) {
    console.warn('[PartnerStack] Could not record failure reason (non-blocking):', err.message);
  }
  alertOps('critical', 'PartnerStack',
    isSignup ? 'Conversion failed — affiliate not credited'
             : 'Qualification failed — the $50 did not fire',
    {
      'Domain':  customer_key || '(unknown)',
      'Email':   email || '(unknown)',
      'Reason':  reason || 'unknown',
      'Detail':  (detail || '').toString().slice(0, 300) || '—',
      'Impact':  isSignup
        ? 'PartnerStack does not know this customer exists, so the qualification can never fire either. The claim has been released, so a later submit from this domain will retry.'
        : 'The commission for this customer has not been recorded. The claim has been released, so the next poll will retry.',
    });
}

/* Cleared whenever the matching step later succeeds, so a domain that failed
   once and recovered does not sit in a red state forever. */
async function clearPartnerStackFailure(kind, session_id) {
  const cols = kind === 'signup'
    ? 'ps_signup_failed_at = NULL, ps_signup_fail_reason = NULL, ps_signup_skipped_reason = NULL, ps_signup_skipped_at = NULL'
    : 'ps_qualify_failed_at = NULL, ps_qualify_fail_reason = NULL';
  try {
    await pool.query(`UPDATE leads SET ${cols}, updated_at = NOW() WHERE session_id = $1`, [session_id]);
  } catch (err) {
    console.warn('[PartnerStack] Could not clear failure reason (non-blocking):', err.message);
  }
}

/* ── STEP 5: the signup conversion ───────────────────────────────────
   Fires for any partner-referred lead that is not one of our own test
   addresses. There is NO eligibility gate on this path for the MVP — rejections
   are decided by hand at payout approval instead. The automated check exists
   and is dormant behind PS_ELIGIBILITY_ENABLED; wiring it in here is a
   deliberate decision for someone to make, not a tidy-up.

   Deferred, like everything else on this path: called after res.json(), never
   awaited. The lead has been written, Slack, Salesforce and Meta have all
   fired, and nobody is waiting on an affiliate being credited.

   ONCE PER DOMAIN, EVER — and the claim happens BEFORE the HTTP call, not
   after. Checking "has this domain been sent?" and then sending is a race:
   two submits for the same domain arriving together both read no stamp, both
   fire, and PartnerStack credits the affiliate twice for one customer with no
   way to undo it. So the stamp is taken first, as an atomic conditional
   UPDATE backed by leads_ps_signup_once_idx, and only the winner sends.

   If the send then fails, the claim is RELEASED so a later submit can retry.
   The alternative — leaving the stamp on a conversion that never arrived — is
   the worse failure: it is silent, permanent, and costs the affiliate a real
   payout with nothing in the system saying so. */
async function runPartnerStackSignup({ session_id, email, website, company, phone, first_name, last_name, disqualified, ps, ctx }) {
  /* Logged on EVERY submit, including organic ones. Without this an organic
     lead produces no PartnerStack line at all, and the logs cannot tell
     "no partner traffic yet" apart from "capture is broken" — which is exactly
     the ambiguity we hit watching the first deploy. */
  if (!ps || !ps.ps_xid) {
    console.log(`[PartnerStack] No partner on this submit (${email || 'no email'}) — nothing to send`);
    return;
  }

  /* Disqualified leads must never pay an affiliate. A B2C or waitlist signup
     is not a customer, and $50 is a real cost.

     Today no disqualified lead reaches /submit at all: both b2c_or_mixed and
     waitlist call savePartial(1) and then show a terminal step, so the
     conversion is already unreachable for them. That is a property of the
     FRONTEND FLOW, not a guard — and the frontend is two forked files that
     have drifted apart before. This makes it a guard. */
  if (disqualified) {
    console.log(`[PartnerStack] Skipped conversion — lead is disqualified: ${email}`);
    await recordPartnerStackSkip(session_id, 'disqualified');
    return;
  }

  if (!ps.ps_customer_key) {
    console.log(`[PartnerStack] Skipped conversion — no usable company domain: ${email}`);
    await recordPartnerStackSkip(session_id, 'no_customer_key');
    return;
  }
  if (isPartnerStackTestEmail(email)) {
    console.log(`[PartnerStack] Skipped conversion — internal or test address: ${email}`);
    await recordPartnerStackSkip(session_id, 'test_email');
    return;
  }

  let claimed = false;
  try {
    /* Claim the domain. The NOT EXISTS covers the ordinary case; the unique
       index covers the concurrent one, surfacing as a 23505 we read as
       "somebody else already sent it". */
    const claim = await pool.query(
      `UPDATE leads
          SET ps_signup_sent_at = NOW(), updated_at = NOW()
        WHERE session_id = $1
          AND ps_signup_sent_at IS NULL
          AND NOT EXISTS (
                SELECT 1 FROM leads other
                 WHERE other.ps_customer_key = $2
                   AND other.ps_signup_sent_at IS NOT NULL)
        RETURNING id`,
      [session_id, ps.ps_customer_key]
    );
    if (claim.rowCount === 0) {
      console.log(`[PartnerStack] Skipped conversion — ${ps.ps_customer_key} already sent`);
      await recordPartnerStackSkip(session_id, 'already_sent');
      return;
    }
    claimed = true;
  } catch (err) {
    if (err && err.code === '23505') {
      console.log(`[PartnerStack] Skipped conversion — ${ps.ps_customer_key} claimed concurrently`);
      return;
    }
    console.warn('[PartnerStack] Could not claim the domain (conversion NOT sent):', err.message);
    recordFailure('PartnerStack', ps.ps_customer_key + ' (claim)', err.message);
    return;
  }

  /* The CONTACT's name, not the company. `name` titles the record in
     PartnerStack and whoever approves payouts opens that record — sending the
     company there made every customer read as the company, with Company Name,
     Website and Phone all showing "Not Available". Falls back to the company
     only when we have no human name at all, since an untitled record is worse
     than a company-titled one. */
  const contactName = [first_name, last_name].map(v => (v || '').trim()).filter(Boolean).join(' ')
    || (company || '').trim()
    || null;

  const result = await sendConversion({
    xid: ps.ps_xid,
    customer_key: ps.ps_customer_key,
    email,
    name: contactName,
    // Fraud-detection signals. Absent rather than empty when we do not have them.
    ip_address: ctx && ctx.ip_address,
    user_agent: ctx && ctx.user_agent,
    origin:     ctx && ctx.origin,
    /* There is no company or website parameter on this endpoint. These keys
       must exist as custom CUSTOMER fields in PartnerStack Settings or they
       are dropped silently — see PS_CONVERSION_META_FIELDS. */
    /* Phone is OPTIONAL on our form — only required for free-mail addresses —
       so it is absent more often than not. sendConversion drops empty meta
       values, so a missing phone omits the key entirely rather than sending a
       blank, and can never fail or block the conversion. */
    meta: {
      [PS_META_COMPANY]: company,
      [PS_META_WEBSITE]: website,
      [PS_META_PHONE]:   phone,
    },
  });

  if (result.ok) {
    console.log(`[PartnerStack] ✅ Conversion sent: ${ps.ps_customer_key} | xid=${ps.ps_xid} | ${email}`);
    await clearPartnerStackFailure('signup', session_id);
    return;
  }

  // Release the claim so this domain can be retried rather than silently lost.
  try {
    await pool.query(
      `UPDATE leads SET ps_signup_sent_at = NULL, updated_at = NOW() WHERE session_id = $1`,
      [session_id]
    );
  } catch (err) {
    console.error('[PartnerStack] ⚠ Conversion failed AND the claim could not be released:', err.message);
    recordFailure('PartnerStack', ps.ps_customer_key + ' (stuck claim)', err.message);
  }
  console.warn(`[PartnerStack] ⛔ Conversion NOT sent (${result.reason}): ${ps.ps_customer_key}`);
  if (result.reason !== 'no_token') {
    recordFailure('PartnerStack', ps.ps_customer_key + ' (conversion)', result.reason + (result.body ? ' — ' + String(result.body).slice(0, 200) : ''));
    /* Recorded AFTER the release above, never instead of it. The release is
       what kept hello.com retryable when the qualification 400'd; this only
       adds the reason and the alert on top of it. */
    await recordPartnerStackFailure('signup', {
      session_id, customer_key: ps.ps_customer_key, email,
      reason: result.reason, detail: result.body,
    });
  }
}

/* ── READ-BACK: did the conversion actually create a customer? ────────
   /conversion/xid answers 200 with an EMPTY body. There is nothing in the
   response to check, so a 200 that created nothing would still stamp
   ps_signup_sent_at — and the once-per-domain rule would then burn that domain
   PERMANENTLY, with no error anywhere. The only real proof is reading the
   customer back.

   A SWEEP, not a setTimeout after the send. A timer dies with the process, and
   a deploy in the wrong ten minutes would lose the verification silently,
   which is the same class of failure this exists to catch. The sweep survives
   restarts and picks up anything left unverified.

   THE GRACE PERIOD IS THE WHOLE DESIGN. PartnerStack's dashboard and API lag
   behind a conversion — measured at under 2 minutes for one record and about 6
   for another on 4 Sept 2026. Checking immediately would report healthy
   conversions as missing and release claims that were fine, causing duplicate
   conversions on the retry. 15 minutes is comfortably past the worst lag seen.

   Only a definitive 404 releases a claim. A 5xx, a timeout or a network error
   means WE COULD NOT TELL, and the row is left alone for the next sweep —
   treating "could not tell" as "missing" would un-stamp every pending
   conversion during a PartnerStack outage and re-fire them all. */
const PS_VERIFY_GRACE_MIN   = 15;
const PS_VERIFY_INTERVAL_MS = 15 * 60 * 1000;
const PS_VERIFY_BATCH       = 25;
let _psVerifyRunning = false;

async function runPartnerStackConversionVerify() {
  if (_psVerifyRunning) return;
  _psVerifyRunning = true;
  try {
    const { rows } = await pool.query(
      `SELECT session_id, email, ps_customer_key, ps_signup_sent_at
         FROM leads
        WHERE ps_signup_sent_at IS NOT NULL
          AND ps_signup_verified_at IS NULL
          AND ps_customer_key IS NOT NULL
          AND ps_signup_sent_at < NOW() - INTERVAL '${PS_VERIFY_GRACE_MIN} minutes'
        ORDER BY ps_signup_sent_at
        LIMIT ${PS_VERIFY_BATCH}`
    );
    if (!rows.length) return;
    console.log(`[PartnerStack] Verifying ${rows.length} conversion(s) against PartnerStack`);

    for (const r of rows) {
      const out = await fetchCustomer(r.ps_customer_key);

      if (!out.ok) {
        // Could not tell. Leave the claim alone and try again next sweep.
        console.warn(`[PartnerStack] Could not verify ${r.ps_customer_key} (${out.reason}) — will retry`);
        continue;
      }

      if (out.exists) {
        await pool.query(
          `UPDATE leads SET ps_signup_verified_at = NOW(), updated_at = NOW() WHERE session_id = $1`,
          [r.session_id]
        ).catch(err => console.warn('[PartnerStack] Could not stamp verification:', err.message));
        console.log(`[PartnerStack] ✅ Conversion verified: ${r.ps_customer_key}` +
          (out.test === true ? ' ⚠ record is flagged test=true' : ''));
        /* A production integration writing test records would pay nobody and
           look completely healthy from here, so it is called out rather than
           quietly accepted. */
        if (out.test === true) {
          recordFailure('PartnerStack', r.ps_customer_key + ' (test record)',
            'Conversion landed as a TEST record — check PARTNERSTACK_TRACKING_TOKEN is the production token');
        }
        continue;
      }

      /* A definitive 404. PartnerStack accepted the call and created nothing.
         Release the claim so the domain can convert again rather than staying
         burned forever, and make noise — this is money not being paid. */
      console.error(`[PartnerStack] ⛔ Conversion 200'd but NO customer exists: ${r.ps_customer_key} — releasing the claim`);
      await pool.query(
        `UPDATE leads SET ps_signup_sent_at = NULL, updated_at = NOW() WHERE session_id = $1`,
        [r.session_id]
      ).catch(err => console.error('[PartnerStack] ⚠ Could not release the claim:', err.message));
      recordFailure('PartnerStack', r.ps_customer_key + ' (phantom conversion)',
        'PartnerStack returned 200 but no customer was created. Claim released so it can retry.');
      await recordPartnerStackFailure('signup', {
        session_id: r.session_id, customer_key: r.ps_customer_key, email: r.email,
        reason: 'phantom_200', detail: 'PartnerStack answered 200 but no customer exists',
      });
    }
  } catch (err) {
    console.warn('[PartnerStack] Conversion verify sweep failed (non-blocking):', err.message);
    recordFailure('PartnerStack', 'conversion verify sweep', err.message);
  } finally {
    _psVerifyRunning = false;
  }
}

function startPartnerStackConversionVerify() {
  const t = setInterval(runPartnerStackConversionVerify, PS_VERIFY_INTERVAL_MS);
  if (t.unref) t.unref();
  console.log(`[PartnerStack] Conversion read-back started (every ${PS_VERIFY_INTERVAL_MS / 60000} min, ${PS_VERIFY_GRACE_MIN} min grace)`);
}

/* ── STEP 10: the qualification action ───────────────────────────────
   An AE ticks Qualified_Demo__c on the Opportunity after the call; we poll
   every 15 minutes and tell PartnerStack, which is what actually triggers the
   affiliate's commission.

   A POLLER, not a Salesforce Flow callout, deliberately: a Flow that calls out
   fails inside Salesforce where nobody on this team would ever see it, and it
   couples an AE ticking a box to our service being up at that instant. Polling
   means a missed window is just a later window.

   DOMAIN is the join. It is the only identifier both systems share —
   PartnerStack knows the customer by the customer_key we sent at signup, which
   came from the lead's website. Account.Website first, the primary contact's
   email domain as a fallback, both normalised through partnerStackCustomerKey
   so the two sides cannot disagree about what acme.com is called.

   CLAIM-FIRST and once per domain, exactly like the signup conversion, and for
   the same reason: two poller runs overlapping, or one Opportunity per contact
   on the same account, would otherwise pay the affiliate twice for one
   qualification. leads_ps_qualified_once_idx makes that impossible; the claim
   is released if the send fails so the next run retries. */
const PS_QUALIFY_INTERVAL_MS = 15 * 60 * 1000;
const PS_QUALIFY_ACTION_TYPE = 'qualified_demo';
let _psQualifyRunning = false;

async function runPartnerStackQualificationPoll() {
  /* One at a time. A slow Salesforce round trip must not let a second run
     start and race the first for the same claims. */
  if (_psQualifyRunning) {
    console.log('[PartnerStack] Qualification poll already running — skipping this tick');
    return;
  }
  _psQualifyRunning = true;
  try {
    const opps = await findQualifiedDemoOpportunities();
    if (!opps.length) return;

    /* Collapse to distinct domains first. Several Opportunities can point at
       one account, and the action is per customer, not per Opportunity. */
    const byKey = new Map();
    for (const o of opps) {
      const key = partnerStackCustomerKey(o.website) || partnerStackCustomerKey(o.contactEmail);
      if (!key) {
        console.log(`[PartnerStack] Qualified demo with no usable domain — Opportunity ${o.id} (${o.accountName || o.name || 'unnamed'})`);
        continue;
      }
      if (!byKey.has(key)) byKey.set(key, o);
    }
    if (!byKey.size) return;

    /* Only domains we actually told PartnerStack about at signup can be
       qualified: an action for a customer_key it has never seen is a no-op at
       best. ps_signup_sent_at IS NOT NULL is that filter. */
    const { rows } = await pool.query(
      `SELECT DISTINCT ps_customer_key
         FROM leads
        WHERE ps_customer_key = ANY($1)
          AND ps_signup_sent_at IS NOT NULL
          AND ps_qualified_sent_at IS NULL`,
      [Array.from(byKey.keys())]
    );
    if (!rows.length) return;

    for (const r of rows) {
      await sendQualificationForDomain(r.ps_customer_key);
    }
  } catch (err) {
    console.warn('[PartnerStack] Qualification poll failed (non-blocking):', err.message);
    recordFailure('PartnerStack', 'qualification poll', err.message);
  } finally {
    _psQualifyRunning = false;
  }
}

async function sendQualificationForDomain(customerKey) {
  let claimedSession = null;
  try {
    // Claim before sending — see the note above runPartnerStackSignup.
    const claim = await pool.query(
      `UPDATE leads
          SET ps_qualified_sent_at = NOW(), updated_at = NOW()
        WHERE session_id = (
              SELECT session_id FROM leads
               WHERE ps_customer_key = $1 AND ps_signup_sent_at IS NOT NULL
               ORDER BY ps_signup_sent_at ASC LIMIT 1)
          AND ps_qualified_sent_at IS NULL
          AND NOT EXISTS (
                SELECT 1 FROM leads o
                 WHERE o.ps_customer_key = $1 AND o.ps_qualified_sent_at IS NOT NULL)
        RETURNING session_id`,
      [customerKey]
    );
    if (claim.rowCount === 0) return;                 // already qualified, or nothing to claim
    claimedSession = claim.rows[0].session_id;
  } catch (err) {
    if (err && err.code === '23505') return;          // concurrent claim won
    console.warn(`[PartnerStack] Could not claim ${customerKey} for qualification:`, err.message);
    recordFailure('PartnerStack', customerKey + ' (qualify claim)', err.message);
    return;
  }

  const result = await sendAction({
    customer_key: customerKey,
    type: PS_QUALIFY_ACTION_TYPE,
    value: 1,
  });

  if (result.ok) {
    console.log(`[PartnerStack] ✅ Qualification sent: ${customerKey}`);
    await clearPartnerStackFailure('qualify', claimedSession);
    return;
  }

  try {
    await pool.query(
      `UPDATE leads SET ps_qualified_sent_at = NULL, updated_at = NOW() WHERE session_id = $1`,
      [claimedSession]
    );
  } catch (err) {
    console.error(`[PartnerStack] ⚠ Qualification failed AND the claim could not be released for ${customerKey}:`, err.message);
    recordFailure('PartnerStack', customerKey + ' (stuck qualify claim)', err.message);
  }
  console.warn(`[PartnerStack] ⛔ Qualification NOT sent (${result.reason}): ${customerKey}`);
  if (result.reason !== 'no_credentials') {
    recordFailure('PartnerStack', customerKey + ' (qualification)', result.reason + (result.body ? ' — ' + String(result.body).slice(0, 200) : ''));
    /* Recorded after the release above. This is the case that was completely
       invisible: a 400 released the claim correctly and nothing anywhere said
       the $50 had not fired. */
    await recordPartnerStackFailure('qualify', {
      session_id: claimedSession, customer_key: customerKey,
      reason: result.reason, detail: result.body,
    });
  }
}

/* ── PARTNER REVENUE GAPS — money we are quietly not collecting ──────
   Two ways a partner-referred lead silently never pays, and they are separate
   failures with separate fixes:

     A. NO CONVERSION. The lead completed, we had a domain, and no row for
        that domain ever got ps_signup_sent_at. PartnerStack does not know the
        customer exists, so the affiliate gets nothing and step 10 can never
        fire for them either. Pure DB, unambiguous: we either sent it or we
        did not.

     B. NO OPPORTUNITY. The demo happened and sfopp never created an
        Opportunity (its log shows these stuck at not_in_sf or error), so no
        AE can tick Qualified_Demo__c and the qualification can never fire.

   WHY NOT "booked but no ps_qualified_sent_at": that conflates three states
   and only one is a bug — no Opportunity (broken), Opportunity awaiting an AE
   (normal), and Opportunity the AE deliberately did not tick (correct, and
   PERMANENT). The third never clears, so within weeks the queue is mostly
   correct non-payments and the real failures are invisible inside it.

   Deliberately NOT a System Health check. A green badge there means "verified
   working, just now", and a lead waiting on an AE is not a system failure —
   putting this in health would leave the dashboard permanently amber for
   normal business latency. It is a work queue, like Pending recovery.

   Check A is keyed by DOMAIN, not by lead. The conversion fires once per
   domain ever, so the second lead from a domain legitimately has a null
   ps_signup_sent_at; only a domain where NO row was ever sent is a miss. */
const PS_GAP_CONVERSION_GRACE_H = 1;     // the conversion is deferred, not slow
const PS_GAP_QUALIFY_GRACE_D    = 3;     // days after the meeting, for the AE
const PS_GAP_SF_LOOKBACK_D      = 180;
const PS_GAP_CACHE_TTL_MS       = 10 * 60 * 1000;
let _psGapCache = { at: 0, data: null };

async function partnerRevenueGaps() {
  if (_psGapCache.data && Date.now() - _psGapCache.at < PS_GAP_CACHE_TTL_MS) {
    return _psGapCache.data;
  }

  /* A — derived from THE LADDER, not from a second bespoke query.
     Two independent queries told two different stories about the same four
     domains: this card said "2 no conversion" while the Partners tab said
     one failure and one correct skip. Reading the ladder means they cannot
     disagree, because there is only one classification.

     A SKIP IS NOT A GAP. gushwork.ai hit the test-address guard, which is
     correct behaviour — counting it here meant it sat in the alert forever
     and could never clear. Skips are reported separately and are not
     actionable. */
  const lifecycle = await partnerLifecycle();
  const graceMs = PS_GAP_CONVERSION_GRACE_H * 3600000;
  const missed = lifecycle.domains.filter((d) => {
    if (d.state === 'conversion_failed') return true;                 // tried, failed
    // Never attempted at all, and past the grace window.
    return d.state === 'conversion_pending'
        && d.first_seen && (Date.now() - new Date(d.first_seen).getTime()) > graceMs;
  });
  const skippedDomains = lifecycle.domains.filter((d) => d.state === 'skipped');
  const missedConversions = { rows: missed };

  /* B candidates — the demo has happened and we have not qualified them.
     start_time is TEXT, so the cast is wrapped in a CASE: a WHERE clause
     alone does not guarantee the regex runs before the cast, and one
     malformed row would take the whole query down. */
  const qualifyCandidates = await pool.query(`
    SELECT ps_customer_key                       AS customer_key,
           MAX(ps_partner_key)                   AS partner_key,
           MAX(ps_partner_name)                  AS partner_name,
           MAX(email)                            AS email,
           MIN(CASE WHEN start_time ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
                    THEN start_time::timestamptz END) AS met_at
      FROM leads
     WHERE ps_xid IS NOT NULL
       AND ps_customer_key IS NOT NULL
       AND booking_uid IS NOT NULL
       AND disqualified IS NOT TRUE
     GROUP BY ps_customer_key
    HAVING COUNT(ps_qualified_sent_at) = 0
       AND MIN(CASE WHEN start_time ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
                    THEN start_time::timestamptz END) < NOW() - INTERVAL '${PS_GAP_QUALIFY_GRACE_D} days'
     ORDER BY 5
     LIMIT 200
  `);

  let missingOpportunity = [];
  let opportunityCheck = { ok: false, reason: 'not_run' };
  if (qualifyCandidates.rows.length) {
    const sf = await findOpportunityDomains({ sinceDays: PS_GAP_SF_LOOKBACK_D });
    if (sf.ok) {
      /* Normalised with the SAME helper as everything else, so the two sides
         cannot disagree about what acme.com is called. */
      const have = new Set();
      for (const r of sf.records) {
        const k = partnerStackCustomerKey(r.website) || partnerStackCustomerKey(r.contactEmail);
        if (k) have.add(k);
      }
      missingOpportunity = qualifyCandidates.rows.filter(r => !have.has(r.customer_key));
      opportunityCheck = { ok: true, checked: true, opportunityDomains: have.size, candidates: qualifyCandidates.rows.length, truncated: !!sf.truncated };
    } else {
      /* "We could not check" is NOT "we checked and it is fine". Reporting an
         unreachable Salesforce as zero gaps would be the exact inversion this
         codebase refuses to make elsewhere. */
      opportunityCheck = { ok: false, reason: sf.reason || 'unavailable' };
    }
  } else {
    /* NOT the same as "we checked and everything has an Opportunity".
       Nothing was eligible to check, so Salesforce was never called — and
       rendering that identically to a clean result is the same conflation
       this card exists to avoid. `checked: false` is what the UI reads. */
    opportunityCheck = { ok: true, checked: false, reason: 'no_candidates', opportunityDomains: 0, truncated: false };
  }

  const data = {
    missedConversions: missedConversions.rows,
    missingOpportunity,
    /* Reported, never counted as a gap — these are the system working. */
    skipped: skippedDomains.map((d) => ({ customer_key: d.customer_key, reason: d.skipped_reason })),
    awaitingQualification: qualifyCandidates.rows.length,
    opportunityCheck,
    graceHours: PS_GAP_CONVERSION_GRACE_H,
    graceDays: PS_GAP_QUALIFY_GRACE_D,
    generatedAt: new Date().toISOString(),
  };
  _psGapCache = { at: Date.now(), data };
  return data;
}

/* ── PARTNERS — the operational view ─────────────────────────────────
   Partner gaps on Overview is the ALERT ("someone is not getting paid").
   This is the day-to-day picture: who is sending, how much, and how it
   converts.

   Counting follows the Definitions section of CLAUDE.md. Leads and bookings
   are PEOPLE — COUNT(DISTINCT lower(email)) — because those are headline
   numbers. Conversions and qualified demos are per DOMAIN, because that is the
   unit PartnerStack itself counts: one conversion per customer key, ever. The
   two are deliberately different units and the column headers say so, rather
   than quietly presenting a domain count next to a people count as if they
   were comparable.

   Booked uses COALESCE(booked_at, created_at) per CLAUDE.md — comparing a null
   booked_at yields null and the row silently drops out of the count. */
/* ── THE PARTNERSTACK LIFECYCLE LADDER ───────────────────────────────
   One state per partner DOMAIN, mutually exclusive and exhaustive, resolved
   in the order below. Every counter on the tab is a COUNT FILTER over this one
   column, so they cannot disagree with each other — the same reason the lead
   stage ladder exists. See the Definitions section of CLAUDE.md.

   Keyed by DOMAIN because that is the unit PartnerStack pays on: one
   conversion and one qualification per customer key, ever. Leads with no
   usable domain cannot be keyed that way and are counted SEPARATELY, as
   leads, and the UI says so — mixing a lead count into a domain count is
   exactly the arithmetic that made the old counters irreconcilable.

   RESOLUTION ORDER, and why it is not simply the progression order:

     1 qualified              terminal success
     2 qualification_failed   failed, and not since qualified
     3 conversion_failed      failed, and NOT since converted
     4 demo_done_not_qualified
     5 awaiting_demo
     6 converted
     7 skipped
     8 conversion_pending     nothing has happened yet

   A success always outranks its OWN failure, because a domain that failed and
   later succeeded is fine — and the failure stamps are cleared on success
   anyway, so this is belt and braces. But an UNRESOLVED conversion failure
   outranks every later stage it blocks: a domain whose conversion never landed
   can never be qualified, so showing it as "awaiting demo" would hide the one
   fact worth acting on. That is the case that was invisible today.

   A domain matching two states therefore takes the FIRST match in this order,
   never a blend, and the eight always sum to the domain total. */
const PS_LADDER_SQL = `
  CASE
    WHEN BOOL_OR(ps_qualified_sent_at IS NOT NULL)                          THEN 'qualified'
    WHEN BOOL_OR(ps_qualify_failed_at IS NOT NULL)                          THEN 'qualification_failed'
    WHEN BOOL_OR(ps_signup_failed_at IS NOT NULL)
     AND NOT BOOL_OR(ps_signup_sent_at IS NOT NULL)                         THEN 'conversion_failed'
    WHEN MIN(CASE WHEN start_time ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
                  THEN start_time::timestamptz END) < NOW()                 THEN 'demo_done_not_qualified'
    WHEN BOOL_OR(booking_uid IS NOT NULL)                                   THEN 'awaiting_demo'
    WHEN BOOL_OR(ps_signup_sent_at IS NOT NULL)                             THEN 'converted'
    WHEN BOOL_OR(ps_signup_skipped_reason IS NOT NULL)                      THEN 'skipped'
    ELSE 'conversion_pending'
  END`;

/* The two red states. Summed into one "Needs attention" number, which is the
   only figure on the tab that means someone has to do something today. */
const PS_LADDER_FAILED = ['conversion_failed', 'qualification_failed'];

/* The ladder groups every partner-sourced lead ever seen, so it needs a bound
   before that is hundreds of domains. 180 days matches PS_GAP_SF_LOOKBACK_D.

   BUT the bound must never hide a failure. An unresolved conversion or
   qualification failure is included regardless of age — otherwise a domain
   that failed 200 days ago and was never fixed would silently drop out of
   "Needs attention", which is the one number on the tab that has to be
   complete. leads_ps_failed_idx covers that arm of the OR. */
const PS_LADDER_WINDOW_D = 180;
const PS_LADDER_LIMIT    = 500;
/* Three refresh intervals. Past this the column is stale enough that a silent
   failure is the likelier explanation than a slow tick. */
const PS_SF_STALE_MIN    = 45;

async function partnerLifecycle() {
  const [domains, noKey, sfState] = await Promise.all([
    pool.query(`
      SELECT ps_customer_key                    AS customer_key,
             ${PS_LADDER_SQL}                   AS state,
             MAX(ps_partner_key)                AS partner_key,
             MAX(ps_partner_name)               AS partner_name,
             MAX(ps_partner_email)              AS partner_email,
             MAX(email)                         AS email,
             MAX(ps_signup_fail_reason)         AS signup_fail_reason,
             MAX(ps_qualify_fail_reason)        AS qualify_fail_reason,
             MAX(ps_signup_skipped_reason)      AS skipped_reason,
             BOOL_OR(ps_signup_sent_at IS NOT NULL)     AS signup_sent,
             BOOL_OR(ps_qualified_sent_at IS NOT NULL)  AS qualified_sent,
             BOOL_OR(ps_signup_verified_at IS NOT NULL) AS signup_verified,
             MAX(created_at)                    AS last_seen,
             MIN(created_at)                    AS first_seen
        FROM leads
       WHERE ps_xid IS NOT NULL AND ps_customer_key IS NOT NULL
         AND (created_at >= NOW() - INTERVAL '${PS_LADDER_WINDOW_D} days'
              OR ps_signup_failed_at IS NOT NULL
              OR ps_qualify_failed_at IS NOT NULL)
       GROUP BY ps_customer_key
       ORDER BY MAX(created_at) DESC
       LIMIT ${PS_LADDER_LIMIT}`),
    /* Counted as LEADS, not domains — there is no domain to key them by.
       Reported separately so the domain counts stay one honest unit. */
    pool.query(`
      SELECT COUNT(*) AS leads
        FROM leads
       WHERE ps_xid IS NOT NULL AND ps_customer_key IS NULL
         AND created_at >= NOW() - INTERVAL '${PS_LADDER_WINDOW_D} days'`),
    /* Read, never recomputed here: the poller owns this table. Missing rows
       simply mean the poller has not run since that domain appeared. */
    pool.query(`SELECT customer_key, sf_state, sf_opportunity_id, sf_error, checked_at
                  FROM partner_domain_sf_state`).catch(() => ({ rows: [] })),
  ]);

  const sfByDomain = new Map(sfState.rows.map((r) => [r.customer_key, r]));
  for (const d of domains.rows) {
    const sf = sfByDomain.get(d.customer_key);
    d.sf_state = sf ? sf.sf_state : null;
    d.sf_opportunity_id = sf ? sf.sf_opportunity_id : null;
    d.sf_error = sf ? sf.sf_error : null;
  }
  const bySfState = {};
  for (const d of domains.rows) if (d.sf_state) bySfState[d.sf_state] = (bySfState[d.sf_state] || 0) + 1;

  /* "Waiting on an AE" is only ACTIONABLE where a qualification could actually
     succeed — which means the conversion reached PartnerStack. gushwork.ai
     reads exists_unticked because an Opportunity exists, but its conversion was
     never sent (test address), so ticking the box would fire an action against
     a customer PartnerStack has never heard of. Showing it as an action item
     is a false errand.

     The state is still shown per domain; it just does not count here. */
  const sfActionable = domains.rows.filter((d) =>
    d.sf_state === 'exists_unticked' && d.signup_sent === true && d.qualified_sent !== true).length;
  const sfUnactionable = domains.rows.filter((d) =>
    d.sf_state === 'exists_unticked' && d.signup_sent !== true).length;

  /* Staleness. A refresh that fails leaves checked_at frozen and every row
     looking current — the failure is invisible unless the AGE is shown. */
  const checkedAts = sfState.rows.map((r) => r.checked_at).filter(Boolean).map((d) => new Date(d).getTime());
  const newestCheck = checkedAts.length ? Math.max(...checkedAts) : null;

  const byState = {};
  for (const r of domains.rows) byState[r.state] = (byState[r.state] || 0) + 1;
  const needsAttention = PS_LADDER_FAILED.reduce((n, k) => n + (byState[k] || 0), 0);

  return {
    domains: domains.rows,
    byState,
    totalDomains: domains.rows.length,
    needsAttention,
    /* Deliberately its own field and its own unit. */
    noCustomerKeyLeads: Number(noKey.rows[0].leads) || 0,
    failedStates: PS_LADDER_FAILED,
    bySfState,
    sfActionable,
    sfUnactionable,
    sfNewestCheckedAt: newestCheck ? new Date(newestCheck).toISOString() : null,
    sfStaleAfterMin: PS_SF_STALE_MIN,
    sfStates: PS_SF_STATES,
    sfLastRead: _psSfLastRead,
    /* The domain list is capped. Say so rather than letting a page-1 view read
       as the whole population — same failure as the Opportunity truncation. */
    domainsCapped: domains.rows.length >= PS_LADDER_LIMIT,
    domainsLimit: PS_LADDER_LIMIT,
  };
}

async function partnerOverview() {
  const [totals, clicksRow, rows] = await Promise.all([
    pool.query(`
      /* DOMAINS throughout, matching the funnel and the ladder. These counted
         people while the funnel counted companies, which put two units on one
         screen again — the thing the per-domain rework exists to remove. */
      SELECT
        COUNT(DISTINCT ps_customer_key)                                     AS leads,
        COUNT(DISTINCT ps_customer_key) FILTER (
          WHERE created_at >= NOW() - INTERVAL '24 hours')                  AS leads_24h,
        COUNT(DISTINCT ps_customer_key) FILTER (
          WHERE ps_signup_sent_at IS NOT NULL)                              AS conversions,
        COUNT(DISTINCT ps_customer_key) FILTER (
          WHERE ps_qualified_sent_at IS NOT NULL)                           AS qualified,
        COUNT(DISTINCT ps_customer_key) FILTER (WHERE booking_uid IS NOT NULL) AS booked,
        COUNT(DISTINCT ps_partner_key)                                      AS partners
        FROM leads
       WHERE ps_partner_key IS NOT NULL AND ps_customer_key IS NOT NULL
    `),
    /* OUR clicks, from ps_click_history. COUNT(DISTINCT xid), never
       SUM(jsonb_array_length): the cookie is cumulative per VISITOR, so a
       person who clicks, submits, clicks again and submits again carries the
       first click in both leads' histories. Against real data the naive sum
       said 5 and the truth was 4 — one xid appeared under two domains.

       This is OUR count and it is not PartnerStack's. Clicks that never
       reached the form are not in our data at all. */
    pool.query(`
      SELECT COUNT(DISTINCT e->>'xid') AS clicks
        FROM leads l, LATERAL jsonb_array_elements(l.ps_click_history) AS e
       WHERE l.ps_partner_key IS NOT NULL
         AND jsonb_typeof(l.ps_click_history) = 'array'
         AND e->>'xid' IS NOT NULL
    `).catch(() => ({ rows: [{ clicks: null }] })),
    pool.query(`
      /* EVERY column counts DOMAINS, so the funnel actually nests:
         step 1 >= completed >= converted >= booked >= qualified. Mixing people
         and domains is what made the old row impossible to read left to right
         — a domain count sitting next to a people count looks like a funnel
         and is not one. Companies is also the truthful unit here, since
         PartnerStack pays per customer. */
      SELECT ps_partner_key                                                 AS partner_key,
             MAX(ps_partner_name)                                           AS partner_name,
             MAX(ps_partner_email)                                          AS partner_email,
             COUNT(DISTINCT ps_customer_key)                                AS step1,
             COUNT(DISTINCT ps_customer_key) FILTER (
               WHERE completed IS TRUE)                                     AS completed,
             COUNT(DISTINCT ps_customer_key) FILTER (
               WHERE ps_signup_sent_at IS NOT NULL)                         AS conversions,
             COUNT(DISTINCT ps_customer_key) FILTER (
               WHERE booking_uid IS NOT NULL)                               AS booked,
             COUNT(DISTINCT ps_customer_key) FILTER (
               WHERE ps_qualified_sent_at IS NOT NULL)                      AS qualified,
             MAX(ps_click_at)                                               AS last_click,
             (SELECT COUNT(DISTINCT e->>'xid')
                FROM leads c, LATERAL jsonb_array_elements(c.ps_click_history) AS e
               WHERE c.ps_partner_key = l.ps_partner_key
                 AND jsonb_typeof(c.ps_click_history) = 'array'
                 AND e->>'xid' IS NOT NULL)                                   AS clicks
        FROM leads l
       WHERE ps_partner_key IS NOT NULL AND ps_customer_key IS NOT NULL
       GROUP BY ps_partner_key
       ORDER BY COUNT(DISTINCT ps_customer_key) DESC, MAX(ps_click_at) DESC NULLS LAST
       LIMIT 200
    `),
  ]);
  const t = totals.rows[0] || {};
  const leads = Number(t.leads) || 0;
  const booked = Number(t.booked) || 0;
  return {
    totals: {
      leads,
      leads24h:   Number(t.leads_24h)   || 0,
      conversions:Number(t.conversions) || 0,
      qualified:  Number(t.qualified)   || 0,
      booked,
      partners:   Number(t.partners)    || 0,
      /* null, not 0, when the query could not run — "not measured" and "none"
         are different answers. */
      clicks: clicksRow.rows[0].clicks === null ? null : Number(clicksRow.rows[0].clicks),
      // Guarded: a partner programme with no leads yet must show a dash, not NaN.
      bookingRate: leads ? Math.round((booked / leads) * 1000) / 10 : null,
    },
    partners: rows.rows,
  };
}

app.get('/monitor/partners', async (req, res) => {
  const token = process.env.MONITOR_TOKEN;
  if (token && req.query.token !== token) return res.status(401).json({ error: 'Unauthorized' });
  try {
    const [overview, lifecycle] = await Promise.all([partnerOverview(), partnerLifecycle()]);
    res.json({ ...overview, lifecycle });
  } catch (err) {
    console.error('[/monitor/partners]', err.message);
    res.status(500).json({ error: 'Partner overview query failed', detail: err.message });
  }
});

app.get('/monitor/partner-gaps', async (req, res) => {
  const token = process.env.MONITOR_TOKEN;
  if (token && req.query.token !== token) return res.status(401).json({ error: 'Unauthorized' });
  try {
    res.json(await partnerRevenueGaps());
  } catch (err) {
    console.error('[/monitor/partner-gaps]', err.message);
    res.status(500).json({ error: 'Partner gap query failed', detail: err.message });
  }
});

/* ── PER-DOMAIN SALESFORCE STATE ─────────────────────────────────────
   Refreshed on every poll for EVERY partner domain, not only the ones already
   eligible to qualify. The row worth acting on daily is "Opportunity exists,
   checkbox unticked" — an AE has not marked the demo, and until they do the
   $50 cannot fire. That row is invisible if you only look at domains that
   already passed every other filter.

   Four states:
     ticked           an AE has marked it; the poller will fire the $50
     exists_unticked  waiting on an AE — the daily action list
     create_errored   sfopp failed to create the Opportunity, so no AE can
                      ever tick it. A silent missed $50
     no_opportunity   nothing in Salesforce yet

   COST is one extra query, not one per domain: Qualified_Demo__c rides along
   on the Opportunity query already being made, and the error state comes from
   gist.sf_lead_conversion_log on the WAREHOUSE, which costs no Salesforce API
   call at all.

   That log is keyed by prospect_email, NOT by domain — so the join is on the
   lead's email, not on the customer key. Assuming a domain key here would
   silently match nothing. */
/* What the last Opportunity read actually saw. Surfaced on the tab, because
   the bug this fixes was a completeness signal that was computed and dropped:
   findOpportunityDomains has always returned `truncated`, it has always been
   true, and nothing ever looked at it. A number nobody can audit is a number
   that will be wrong silently. */
let _psSfLastRead = { ok: null, at: null };

const PS_SF_STATES = ['ticked', 'exists_unticked', 'create_errored', 'no_opportunity'];

async function refreshPartnerDomainSfState() {
  const { rows: domains } = await pool.query(`
    SELECT ps_customer_key AS customer_key, ARRAY_AGG(DISTINCT LOWER(email)) AS emails
      FROM leads
     WHERE ps_xid IS NOT NULL AND ps_customer_key IS NOT NULL AND email IS NOT NULL
     GROUP BY ps_customer_key
     LIMIT 1000`);
  if (!domains.length) {
    console.log('[PartnerStack] SF state refresh: no partner domains yet');
    return { ok: true, updated: 0 };
  }

  const sf = await findOpportunityDomains({ sinceDays: PS_GAP_SF_LOOKBACK_D });
  if (!sf.ok) {
    /* Leave the table alone. A stale row that says what we last verified is
       more useful than one overwritten with a guess during an outage — and
       'incomplete' now lands here too, so a partial Opportunity list can never
       be written as if it were the whole picture. */
    console.warn(`[PartnerStack] SF state refresh skipped — ${sf.reason}` +
      (sf.totalSize ? ` (${sf.fetched} of ${sf.totalSize})` : ''));
    _psSfLastRead = { ok: false, reason: sf.reason, at: new Date().toISOString() };
    return { ok: false, reason: sf.reason };
  }
  _psSfLastRead = { ok: true, records: sf.records.length, totalSize: sf.totalSize, pages: sf.pages, at: new Date().toISOString() };

  // domain -> { qualified, id }, keeping a ticked Opportunity over an unticked one
  const byDomain = new Map();
  for (const r of sf.records) {
    const k = partnerStackCustomerKey(r.website) || partnerStackCustomerKey(r.contactEmail);
    if (!k) continue;
    const prev = byDomain.get(k);
    if (!prev || (r.qualified && !prev.qualified)) byDomain.set(k, { qualified: !!r.qualified, id: r.id });
  }

  /* The sfopp failures, from the warehouse rather than Salesforce. Keyed by
     prospect_email, so this is matched against the lead emails above. */
  const errorsByEmail = new Map();
  if (awsPool) {
    try {
      const { rows } = await withTimeout(awsPool.query(`
        SELECT LOWER(prospect_email) AS email, status, error_message
          FROM gist.sf_lead_conversion_log
         WHERE status IN ('error', 'not_in_sf')
      `), PS_CUSTOMER_QUERY_TIMEOUT_MS, 'sfopp conversion log');
      for (const r of rows) errorsByEmail.set(r.email, r.error_message || r.status);
    } catch (err) {
      /* Non-fatal: without it a genuinely errored domain reads as
         no_opportunity, which is still a gap, just less specific. */
      console.warn('[PartnerStack] Could not read sf_lead_conversion_log:', err.message);
    }
  }

  let updated = 0;
  for (const d of domains) {
    const opp = byDomain.get(d.customer_key);
    let state, oppId = null, error = null;
    if (opp) {
      state = opp.qualified ? 'ticked' : 'exists_unticked';
      oppId = opp.id;
    } else {
      const hit = (d.emails || []).map((e) => errorsByEmail.get(e)).find(Boolean);
      if (hit) { state = 'create_errored'; error = String(hit).slice(0, 300); }
      else     { state = 'no_opportunity'; }
    }
    try {
      await pool.query(`
        INSERT INTO partner_domain_sf_state (customer_key, sf_state, sf_opportunity_id, sf_error, checked_at)
        VALUES ($1,$2,$3,$4,NOW())
        ON CONFLICT (customer_key) DO UPDATE SET
          sf_state = EXCLUDED.sf_state,
          sf_opportunity_id = EXCLUDED.sf_opportunity_id,
          sf_error = EXCLUDED.sf_error,
          checked_at = NOW()`,
        [d.customer_key, state, oppId, error]);
      updated++;
    } catch (err) {
      console.warn(`[PartnerStack] Could not store SF state for ${d.customer_key}:`, err.message);
    }
  }
  console.log(`[PartnerStack] SF state refreshed for ${updated} domain(s)`);
  return { ok: true, updated };
}

/* ITS OWN JOB, not chained onto the qualification poll.

   It was chained, and it never ran once. runPartnerStackQualificationPoll has
   three `return`s inside its try block, and a return there exits the WHOLE
   function — the `finally` still fires, so the flag resets and everything
   looks healthy, but any call placed after the try/finally is skipped. The
   common case takes an early return: once everything qualified has been sent,
   the pending-domains query is empty and the poll returns at that line on
   every subsequent tick, forever.

   Adding a boot-time call would have "fixed" it in the most misleading way
   possible — it would have run once per deploy, populated the column, and
   looked correct. Separate scheduling is the actual fix, and it matches
   startPartnerStackCacheWarm and startPartnerStackConversionVerify, which both
   already do boot-then-interval. */
const PS_SF_REFRESH_INTERVAL_MS = 15 * 60 * 1000;

function startPartnerStackSfStateRefresh() {
  const run = (why) => refreshPartnerDomainSfState()
    .catch((err) => console.warn(`[PartnerStack] SF state refresh failed (${why}, non-blocking):`, err.message));
  run('boot');
  const t = setInterval(() => run('scheduled'), PS_SF_REFRESH_INTERVAL_MS);
  if (t.unref) t.unref();
  console.log(`[PartnerStack] SF state refresh started (boot + every ${PS_SF_REFRESH_INTERVAL_MS / 60000} min)`);
}

function startPartnerStackQualificationPoll() {
  const t = setInterval(runPartnerStackQualificationPoll, PS_QUALIFY_INTERVAL_MS);
  if (t.unref) t.unref();
  console.log(`[PartnerStack] Qualification poller started (every ${PS_QUALIFY_INTERVAL_MS / 60000} min)`);
}

/* Rejections are logged in one place so the format cannot drift between call
   sites. Written to the lead row by the caller; this is the operator's view. */
function logPartnerStackEligibility(verdict, ctx = {}) {
  const who = ctx.email || ctx.session_id || 'unknown';
  if (verdict.eligible) {
    console.log(`[PartnerStack] ✅ eligible: ${who} | key=${verdict.customer_key}` +
      (verdict.unverified ? ` | UNVERIFIED: ${verdict.unverified.join(',')}` : ''));
  } else {
    console.log(`[PartnerStack] ⛔ rejected: ${who} | key=${verdict.customer_key || '-'} | ` +
      `reason=${verdict.reason} | ${verdict.detail}`);
  }
  return verdict;
}

/* v5.8.0 — /session used to validate the id and reply 'ok' without writing
   anything, so the first record of any visitor was /partial, AFTER they had
   typed an email. Everyone who saw the form and left was invisible, which
   meant top-of-funnel drop-off could not be measured at all — the long
   verification wait was SUSPECTED to be costing leads and there was no way
   to prove it either way.

   Both form.js and the ads variant already call this on init with exactly
   these fields, so nothing on the frontend has to change.

   The reply goes out BEFORE the write. This runs on page load, on the
   critical path of a visitor who has not yet given us anything, and no
   amount of analytics is worth adding latency there. A failed write costs
   one row of reporting; a slow one could cost the lead. */
app.post('/session', async (req, res) => {
  const session_id = (req.body.session_id || '').toString().trim().slice(0, 100);
  if (!session_id) return res.status(400).json({ error: 'session_id required' });
  res.json({ ok: true });

  const s = (v, n) => (v || '').toString().trim().slice(0, n) || null;
  try {
    await pool.query(
      `INSERT INTO form_sessions
         (session_id, page_url, referrer, utm_source, utm_medium, utm_campaign, utm_content, utm_term, user_agent)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
       ON CONFLICT (session_id) DO UPDATE SET
         hits         = form_sessions.hits + 1,
         page_url     = COALESCE(form_sessions.page_url,     EXCLUDED.page_url),
         referrer     = COALESCE(form_sessions.referrer,     EXCLUDED.referrer),
         utm_source   = COALESCE(form_sessions.utm_source,   EXCLUDED.utm_source),
         utm_medium   = COALESCE(form_sessions.utm_medium,   EXCLUDED.utm_medium),
         utm_campaign = COALESCE(form_sessions.utm_campaign, EXCLUDED.utm_campaign),
         utm_content  = COALESCE(form_sessions.utm_content,  EXCLUDED.utm_content),
         utm_term     = COALESCE(form_sessions.utm_term,     EXCLUDED.utm_term),
         user_agent   = COALESCE(form_sessions.user_agent,   EXCLUDED.user_agent),
         updated_at   = NOW()`,
      [
        session_id,
        s(req.body.page_url, 500),
        s(req.body.referrer, 500),
        s(req.body.utm_source, 100),
        s(req.body.utm_medium, 100),
        s(req.body.utm_campaign, 100),
        s(req.body.utm_content, 100),
        s(req.body.utm_term, 100),
        s(req.headers['user-agent'], 500),
      ]
    );
  } catch (err) {
    // Response already sent — never rethrow, and never alert: a reporting
    // row is not worth waking anyone up for.
    console.warn('[/session] visit not recorded (ignored):', err.message);
  }
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
  const page_url           = (req.body.page_url           || '').toString().trim().slice(0, 1000);
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
  const referrer           = (req.body.referrer           || '').toString().trim().slice(0, 1000);
  const prefill_source     = (req.body.prefill_source     || '').toString().trim().slice(0, 100);
  const fbc                = (req.body.fbc                || '').toString().trim().slice(0, 1000);
  const fbp                = (req.body.fbp                || '').toString().trim().slice(0, 200);
  const landing_page       = (req.body.landing_page       || '').toString().trim().slice(0, 1000);
  const previous_page      = (req.body.previous_page      || '').toString().trim().slice(0, 1000);
  const enriched_title     = (req.body.enriched_title     || '').toString().trim().slice(0, 200);
  const enriched_company_size = (req.body.enriched_company_size || '').toString().trim().slice(0, 50);
  const enriched_industry  = (req.body.enriched_industry  || '').toString().trim().slice(0, 200);
  const enriched_linkedin  = (req.body.enriched_linkedin  || '').toString().trim().slice(0, 500);
  const disqualified       = req.body.disqualified === true || req.body.disqualified === 'true';
  const disqualified_reason = (req.body.disqualified_reason || '').toString().trim().slice(0, 100);
  const step_reached       = parseInt(req.body.step_reached) || 1;
  const website_check_failed = req.body.website_check_failed === true || req.body.website_check_failed === 'true';
  const website_check_reason = (req.body.website_check_reason || '').toString().trim().slice(0, 100);
  /* PartnerStack attribution, captured at step 1 as well as at submit.
     The lead row is CREATED here, so a partner-referred visitor who reaches
     step 1 and drops would otherwise have no partner on the row at all —
     including for the drop-off recovery cron. */
  const ps = readPartnerStackPayload(req.body, { email, website });
  /* Step 7. peek only — never an API call here. The identity resolver runs
     after res.json(); a brand-new partner key lands as the key and is upgraded
     in place once the name arrives. */
  const psIdentity = await partnerIdentityNoNetwork(ps.ps_partner_key);
  const psHear = partnerHearAboutUs({ hear_about_us, ps, identity: psIdentity });
  const hearAboutUsFinal = psHear || hear_about_us;

  if (!session_id) return res.status(400).json({ error: 'session_id required' });

  try {
    /* How this lead's email verified, read back from the durable store.
       /verify-email ran on blur — before this row existed — so the verdict
       could not be written here at the time. A primary-key lookup on a tiny
       table; no ELV call, nothing that can slow the form down. */
    const elv = await lookupElvStatus(email);

    await pool.query(`
      INSERT INTO leads (session_id,page_url,email,website,sell_to,first_name,last_name,phone,company,hear_about_us,utm_source,utm_medium,utm_campaign,utm_content,utm_term,referrer,prefill_source,fbc,fbp,landing_page,previous_page,enriched_title,enriched_company_size,enriched_industry,enriched_linkedin,disqualified,disqualified_reason,step_reached,completed,updated_at,website_check_failed,website_check_reason,elv_status,elv_checked_at,ps_xid,ps_partner_key,ps_customer_key,ps_click_at,ps_click_history)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,false,NOW(),$29,$30,$31,$32,$33,$34,$35,$36,$37)
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
        website_check_reason  = COALESCE(EXCLUDED.website_check_reason,  leads.website_check_reason),
        elv_status            = COALESCE(EXCLUDED.elv_status,            leads.elv_status),
        elv_checked_at        = COALESCE(EXCLUDED.elv_checked_at,        leads.elv_checked_at),
        /* COALESCE for the same reason as /submit: /partial fires repeatedly as
           the visitor moves through step 1, and a later call with an expired
           cookie must not wipe attribution captured by an earlier one. */
        ps_xid                = COALESCE(EXCLUDED.ps_xid,                leads.ps_xid),
        ps_partner_key        = COALESCE(EXCLUDED.ps_partner_key,        leads.ps_partner_key),
        ps_customer_key       = COALESCE(EXCLUDED.ps_customer_key,       leads.ps_customer_key),
        ps_click_at           = COALESCE(EXCLUDED.ps_click_at,           leads.ps_click_at),
        ps_click_history      = COALESCE(EXCLUDED.ps_click_history,      leads.ps_click_history)
    `, [session_id,page_url||null,email||null,website||null,sell_to||null,first_name||null,last_name||null,phone||null,company||null,hearAboutUsFinal||null,utm_source||null,utm_medium||null,utm_campaign||null,utm_content||null,utm_term||null,referrer||null,prefill_source||null,fbc||null,fbp||null,landing_page||null,previous_page||null,enriched_title||null,enriched_company_size||null,enriched_industry||null,enriched_linkedin||null,disqualified,disqualified_reason||null,step_reached,website_check_failed,website_check_reason||null,elv?.status||null,elv?.checked_at||null,ps.ps_xid,ps.ps_partner_key,ps.ps_customer_key,ps.ps_click_at,ps.ps_click_history?JSON.stringify(ps.ps_click_history):null]);

    await pool.query(`UPDATE leads SET enriched_city=e.enriched_city,enriched_state=e.enriched_state,enriched_country=e.enriched_country,enriched_seniority=e.enriched_seniority,enriched_departments=e.enriched_departments,enriched_email_status=e.enriched_email_status,enriched_founded_year=e.enriched_founded_year,enriched_annual_revenue=e.enriched_annual_revenue,enriched_funding_events=e.enriched_funding_events,enriched_alexa_ranking=e.enriched_alexa_ranking,enriched_keywords=e.enriched_keywords,enriched_org_hq=e.enriched_org_hq,enriched_total_funding=e.enriched_total_funding,enriched_funding_stage=e.enriched_funding_stage,updated_at=NOW() FROM enrichment_data e WHERE leads.session_id=e.session_id AND leads.session_id=$1`, [session_id]).catch(err => console.warn('[/partial] Enrichment sync failed (non-blocking):', err.message));

    syncToAWS({session_id,page_url,email,website,sell_to,first_name,last_name,phone,company,hear_about_us:hearAboutUsFinal,utm_source,utm_medium,utm_campaign,utm_content,utm_term,referrer,prefill_source,fbc,fbp,landing_page,previous_page,enriched_title,enriched_company_size,enriched_industry,enriched_linkedin,disqualified,disqualified_reason,step_reached,completed:false,...ps});

    // StartTrial fires ONLY for qualified (B2B) leads on BUSINESS emails —
    // free-mailbox leads (gmail/yahoo/...) are skipped so Meta optimises
    // on higher-intent signals. Email is already lowercased at parse time
    // and already ELV-verified by the form before /partial is called.
    // v5.6.0 — was an exact-list check, so a typosquat of a free provider
    // (gmailc.com) counted as a business email and fired StartTrial. That
    // teaches Meta to find MORE consumer traffic, which is precisely what
    // this gate exists to prevent.
    const freeMatch = email ? freeEmailMatch(email.split('@')[1] || '') : null;
    const isBusinessEmail = !!email && !freeMatch;
    if (!disqualified && isBusinessEmail) {
      pushStartTrialToMeta({session_id,email,sell_to,page_url,fbc,fbp,landing_page}, {clientIpAddress:req.headers['x-forwarded-for']||req.ip||'',clientUserAgent:req.headers['user-agent']||''}).catch(err => { console.warn('[/partial] Meta CAPI StartTrial failed (non-blocking):', err.message); recordFailure('Meta CAPI', email + ' (StartTrial)', err.message); });
    } else if (!disqualified) {
      console.log(`[/partial] ⏭ StartTrial skipped — ${freeMatch && !freeMatch.exact ? `likely typo of free provider ${freeMatch.domain}` : 'free email domain'}: ${email}`);
    }

    console.log(`[/partial] ✅ Saved session ${session_id} | step ${step_reached} | disqualified: ${disqualified} | email ${email}`);
    res.json({ ok: true });
  } catch (err) { console.error('[/partial]', err.message); res.status(500).json({ error: 'Partial save failed' }); }
});

app.post('/submit', async (req, res) => {
  const session_id         = (req.body.session_id         || '').toString().trim().slice(0, 100);
  const page_url           = (req.body.page_url           || '').toString().trim().slice(0, 1000);
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
  const referrer           = (req.body.referrer           || '').toString().trim().slice(0, 1000);
  const prefill_source     = (req.body.prefill_source     || '').toString().trim().slice(0, 100);
  const fbc                = (req.body.fbc                || '').toString().trim().slice(0, 1000);
  const fbp                = (req.body.fbp                || '').toString().trim().slice(0, 200);
  const landing_page       = (req.body.landing_page       || '').toString().trim().slice(0, 1000);
  const previous_page      = (req.body.previous_page      || '').toString().trim().slice(0, 1000);
  const enriched_title     = (req.body.enriched_title     || '').toString().trim().slice(0, 200);
  const enriched_company_size = (req.body.enriched_company_size || '').toString().trim().slice(0, 50);
  const enriched_industry  = (req.body.enriched_industry  || '').toString().trim().slice(0, 200);
  const enriched_linkedin  = (req.body.enriched_linkedin  || '').toString().trim().slice(0, 500);
  const disqualified       = req.body.disqualified === true || req.body.disqualified === 'true';
  const disqualified_reason = (req.body.disqualified_reason || '').toString().trim().slice(0, 100);
  const website_check_failed = req.body.website_check_failed === true || req.body.website_check_failed === 'true';
  const website_check_reason = (req.body.website_check_reason || '').toString().trim().slice(0, 100);
  // PartnerStack attribution. Read once, stored below and mirrored to AWS.
  const ps = readPartnerStackPayload(req.body, { email, website });
  /* Step 7. peek only — never an API call here. The identity resolver runs
     after res.json(); a brand-new partner key lands as the key and is upgraded
     in place once the name arrives. */
  const psIdentity = await partnerIdentityNoNetwork(ps.ps_partner_key);
  const psHear = partnerHearAboutUs({ hear_about_us, ps, identity: psIdentity });
  const hearAboutUsFinal = psHear || hear_about_us;

  if (!session_id) return res.status(400).json({ error: 'session_id required' });

  try {
    const existing        = await pool.query('SELECT completed FROM leads WHERE session_id=$1', [session_id]);
    const alreadyCompleted = existing.rows[0]?.completed === true;
    const enrichRow       = await pool.query('SELECT * FROM enrichment_data WHERE session_id=$1', [session_id]);
    const enrich          = enrichRow.rows[0] || {};

    /* The stored ELV verdict. This is the half of the picture the DB never
       had: with website_check_reason it answers "did anything actually
       verify this lead?" rather than only "did it pass?". A miss is filled
       by a re-check after the response — see finaliseElvVerdict. */
    const elv = await lookupElvStatus(email);

    await pool.query(`
      INSERT INTO leads (session_id,page_url,email,website,sell_to,first_name,last_name,phone,company,hear_about_us,utm_source,utm_medium,utm_campaign,utm_content,utm_term,referrer,prefill_source,fbc,fbp,landing_page,previous_page,enriched_title,enriched_company_size,enriched_industry,enriched_linkedin,disqualified,disqualified_reason,step_reached,completed,submitted_at,updated_at,website_check_failed,website_check_reason,elv_status,elv_checked_at,ps_xid,ps_partner_key,ps_customer_key,ps_click_at,ps_click_history)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,2,true,NOW(),NOW(),$28,$29,$30,$31,$32,$33,$34,$35,$36)
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
        website_check_reason  = COALESCE(EXCLUDED.website_check_reason,  leads.website_check_reason),
        elv_status            = COALESCE(EXCLUDED.elv_status,            leads.elv_status),
        elv_checked_at        = COALESCE(EXCLUDED.elv_checked_at,        leads.elv_checked_at),
        /* COALESCE, not overwrite: a second submit in the same session arrives
           with whatever cookies the browser still has. A partner cookie that
           has since expired must not erase the attribution we already captured. */
        ps_xid                = COALESCE(EXCLUDED.ps_xid,                leads.ps_xid),
        ps_partner_key        = COALESCE(EXCLUDED.ps_partner_key,        leads.ps_partner_key),
        ps_customer_key       = COALESCE(EXCLUDED.ps_customer_key,       leads.ps_customer_key),
        ps_click_at           = COALESCE(EXCLUDED.ps_click_at,           leads.ps_click_at),
        ps_click_history      = COALESCE(EXCLUDED.ps_click_history,      leads.ps_click_history)
    `, [session_id,page_url||null,email||null,website||null,sell_to||null,first_name||null,last_name||null,phone||null,company||null,hearAboutUsFinal||null,utm_source||null,utm_medium||null,utm_campaign||null,utm_content||null,utm_term||null,referrer||null,prefill_source||null,fbc||null,fbp||null,landing_page||null,previous_page||null,enriched_title||null,enriched_company_size||null,enriched_industry||null,enriched_linkedin||null,disqualified,disqualified_reason||null,website_check_failed,website_check_reason||null,elv?.status||null,elv?.checked_at||null,ps.ps_xid,ps.ps_partner_key,ps.ps_customer_key,ps.ps_click_at,ps.ps_click_history?JSON.stringify(ps.ps_click_history):null]);

    await pool.query(`UPDATE leads SET enriched_city=e.enriched_city,enriched_state=e.enriched_state,enriched_country=e.enriched_country,enriched_seniority=e.enriched_seniority,enriched_departments=e.enriched_departments,enriched_email_status=e.enriched_email_status,enriched_founded_year=e.enriched_founded_year,enriched_annual_revenue=e.enriched_annual_revenue,enriched_funding_events=e.enriched_funding_events,enriched_alexa_ranking=e.enriched_alexa_ranking,enriched_keywords=e.enriched_keywords,enriched_org_hq=e.enriched_org_hq,enriched_total_funding=e.enriched_total_funding,enriched_funding_stage=e.enriched_funding_stage,updated_at=NOW() FROM enrichment_data e WHERE leads.session_id=e.session_id AND leads.session_id=$1`, [session_id]).catch(err => console.warn('[/submit] Enrichment sync failed (non-blocking):', err.message));

    syncToAWS({session_id,page_url,email,website,sell_to,first_name,last_name,phone,company,hear_about_us:hearAboutUsFinal,utm_source,utm_medium,utm_campaign,utm_content,utm_term,referrer,prefill_source,fbc,fbp,landing_page,previous_page,enriched_title:enrich.enriched_title,enriched_company_size:enrich.enriched_company_size,enriched_industry:enrich.enriched_industry,enriched_linkedin:enrich.enriched_linkedin,enriched_city:enrich.enriched_city,enriched_state:enrich.enriched_state,enriched_country:enrich.enriched_country,enriched_seniority:enrich.enriched_seniority,enriched_departments:enrich.enriched_departments,enriched_email_status:enrich.enriched_email_status,enriched_founded_year:enrich.enriched_founded_year,enriched_annual_revenue:enrich.enriched_annual_revenue,enriched_funding_events:enrich.enriched_funding_events,enriched_alexa_ranking:enrich.enriched_alexa_ranking,enriched_keywords:enrich.enriched_keywords,enriched_org_hq:enrich.enriched_org_hq,enriched_total_funding:enrich.enriched_total_funding,enriched_funding_stage:enrich.enriched_funding_stage,disqualified,disqualified_reason,step_reached:2,completed:true,...ps});

    if (!alreadyCompleted) {
      slackSubmit({first_name,last_name,email,phone,company,website,sell_to,hear_about_us:hearAboutUsFinal,ps_partner_key:ps.ps_partner_key,ps_partner_name:(psIdentity||{}).name,ps_partner_email:(psIdentity||{}).email,ps_click_at:ps.ps_click_at,landing_page,previous_page,page_url,referrer,utm_source,utm_medium,utm_campaign,utm_content,prefill_source,website_check_failed,website_check_reason,elv_status:elv?.status||null,enriched_title:enrich.enriched_title,enriched_company_size:enrich.enriched_company_size,enriched_industry:enrich.enriched_industry,enriched_linkedin:enrich.enriched_linkedin,enriched_city:enrich.enriched_city,enriched_state:enrich.enriched_state,enriched_country:enrich.enriched_country,enriched_seniority:enrich.enriched_seniority,enriched_departments:enrich.enriched_departments,enriched_email_status:enrich.enriched_email_status,enriched_founded_year:enrich.enriched_founded_year,enriched_annual_revenue:enrich.enriched_annual_revenue,enriched_funding_events:enrich.enriched_funding_events,enriched_alexa_ranking:enrich.enriched_alexa_ranking,enriched_keywords:enrich.enriched_keywords,enriched_org_hq:enrich.enriched_org_hq,enriched_total_funding:enrich.enriched_total_funding,enriched_funding_stage:enrich.enriched_funding_stage});

      pushToSalesforce({first_name,last_name,email,phone,company,website,sell_to,hear_about_us:hearAboutUsFinal,page_url,fbc,fbp,utm_source,utm_medium,utm_campaign,utm_content,utm_term,referrer,landing_page,enriched_title:enrich.enriched_title,enriched_company_size:enrich.enriched_company_size,enriched_industry:enrich.enriched_industry,enriched_linkedin:enrich.enriched_linkedin,enriched_seniority:enrich.enriched_seniority,enriched_departments:enrich.enriched_departments,enriched_city:enrich.enriched_city,enriched_state:enrich.enriched_state,enriched_country:enrich.enriched_country,enriched_annual_revenue:enrich.enriched_annual_revenue,enriched_total_funding:enrich.enriched_total_funding,enriched_funding_stage:enrich.enriched_funding_stage,enriched_founded_year:enrich.enriched_founded_year,step_reached:2,booked:false}).catch(err => { console.warn('[/submit] SF push failed (non-blocking):', err.message); alertOps('critical', 'Salesforce', 'Lead not created', { 'Email': email, 'Stage': 'form completed', 'Error': err.message, 'Impact': 'This lead is NOT in Salesforce. Add it manually.' }); });

      // Meta CAPI Lead — suppressed when the website check failed (temporary
      // non-blocking mode still lets the lead through, but keeps the Lead
      // event clean). Slack/SF above still fire normally either way.
      if (isWebsiteVerified({ website_check_failed, website_check_reason })) {
        pushFormEventsToMeta({session_id,email,phone,first_name,last_name,company,website,sell_to,page_url,fbc,fbp,landing_page,enriched_city:enrich.enriched_city,enriched_state:enrich.enriched_state,enriched_country:enrich.enriched_country,enriched_company_size:enrich.enriched_company_size,enriched_industry:enrich.enriched_industry,enriched_seniority:enrich.enriched_seniority,enriched_funding_stage:enrich.enriched_funding_stage}, {clientIpAddress:req.headers['x-forwarded-for']||req.ip||'',clientUserAgent:req.headers['user-agent']||''}).catch(err => { console.warn('[/submit] Meta CAPI failed (non-blocking):', err.message); recordFailure('Meta CAPI', email + ' (Lead)', err.message); });
      } else {
        console.log(`[/submit] ⏭ Meta CAPI Lead skipped — website not verified (${website_check_reason || 'failed'}): ${email}`);
      }

      // Both checks came back unable to tell us anything. Informational
      // only: everything above has already fired, Meta included.
      alertUnverifiablePair({ email, elv_status: elv?.status, website_check_reason });

      console.log(`[/submit] ✅ Lead completed: ${email} | session: ${session_id} | email check: ${elv?.status || 'not stored'}`);
    } else {
      console.log(`[/submit] ⏭ Slack skipped — already completed: ${email} | session: ${session_id}`);
    }
    res.json({ ok: true });

    // Off the critical path on purpose — the lead is no longer waiting.
    if (!elv && !alreadyCompleted) finaliseElvVerdict({ session_id, email, website_check_reason });
    if (!alreadyCompleted) runPartnerStackIdentity({ session_id, ps })
      .then(identity => upgradePartnerHearAboutUs({ session_id, email, ps, identity }))
      .catch(err => console.warn('[PartnerStack] Partner identity failed (non-blocking):', err.message));
    if (!alreadyCompleted) runPartnerStackEligibility({ session_id, email, website, ps });
    if (!alreadyCompleted) runPartnerStackSignup({ session_id, email, website, company, phone, first_name, last_name, disqualified, ps, ctx: readPartnerStackRequestContext(req, page_url) })
      .catch(err => console.warn('[PartnerStack] Signup conversion failed (non-blocking):', err.message));
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

    // Booking with no completed step 2 — see alertIfBookingWithoutSubmit.
    await alertIfBookingWithoutSubmit(session_id, '/booking-confirmed');

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
        // Checked BEFORE the update — the write below sets completed=true,
        // which would mask the very thing we are looking for.
        await alertIfBookingWithoutSubmit(lead.session_id, '/cal-webhook');
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
  // Self-creating so there is no separate migration step to remember.
  // Idempotent, so running it every cron costs nothing.
  try {
    await pool.query('ALTER TABLE leads ADD COLUMN IF NOT EXISTS followup_attempts INTEGER DEFAULT 0');
  } catch (err) {
    console.warn('[Cron] followup_attempts column check failed (continuing):', err.message);
  }
  try {
    const result = await pool.query(`
      SELECT l.session_id, l.email, l.first_name, l.last_name, l.company, l.website, l.sell_to,
             l.utm_source, l.utm_medium, l.utm_campaign, l.utm_content, l.referrer, l.page_url,
             l.landing_page, l.previous_page,
             l.disqualified, l.disqualified_reason, l.completed,
             -- Both halves of "did anything verify this lead?", so slackPartial
             -- can flag the pair. Neither was selected here before.
             l.elv_status, l.website_check_failed, l.website_check_reason,
             l.enriched_title, l.enriched_company_size, l.enriched_industry, l.enriched_linkedin,
             l.enriched_city, l.enriched_state, l.enriched_country, l.enriched_seniority,
             l.enriched_departments, l.enriched_email_status, l.enriched_founded_year,
             l.enriched_annual_revenue, l.enriched_funding_events, l.enriched_alexa_ranking,
             l.enriched_keywords, l.created_at, COALESCE(l.followup_attempts, 0) AS followup_attempts
      FROM leads l
      WHERE l.email IS NOT NULL
        AND l.disqualified = false
        AND l.booking_uid IS NULL
        AND l.loops_sent = false
        AND l.created_at < NOW() - INTERVAL '2 hours'
        /* The booked_at >= l.created_at test IS DELIBERATE — May 2026. Do not relax it
           to "has this email ever booked", and do not COALESCE it.

           The question here is NOT "is this person an SDR target" (which has no
           time component — if they hold a call slot, nobody should ring them).
           It is "does THIS session's drop-off still need an email", and a booking
           that PREDATES this session does not resolve it: they came back weeks
           later, started the form again, and dropped again. Suppressing on
           ever-booked silently kills follow-ups that should go out.

           No COALESCE on purpose either: production has zero rows with a null
           booked_at, so there is nothing to defend against, and adding one would
           make a null read as created_at and re-introduce the ever-booked
           behaviour through the back door.

           The monitor's "Pending recovery" card reports this same population but
           is labelled for what it counts rather than as a mirror of this query —
           see the Definitions section in CLAUDE.md. */
        AND NOT EXISTS (
          SELECT 1 FROM leads booked
          WHERE LOWER(booked.email) = LOWER(l.email)
            AND booked.booking_uid IS NOT NULL
            AND booked.booked_at >= l.created_at
        )
    `);

    const leads = result.rows;
    _lastCronRunAt = Date.now(); // heartbeat: the scheduler reached us
    _cronRanThisProcess = true;  // and the health badge can now say so honestly
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

      const outcome = await sendFollowUpEmail(lead.email, lead.first_name);

      /* v5.5.0 — the flag used to be set unconditionally, so a failed send
         was recorded as delivered and never retried. It now only moves on
         success, on a permanent recipient failure (the address does not
         exist — retrying is pointless and harms sending reputation), or
         once the attempt cap is reached.
         The cap is the safety net: it guarantees termination even for an
         error shape isPermanentSmtpFailure() does not recognise, so no
         address can ever be retried indefinitely. */
      const attempts = (Number(lead.followup_attempts) || 0) + 1;
      const giveUp   = outcome.sent || outcome.permanent || attempts >= FOLLOWUP_MAX_ATTEMPTS;

      if (giveUp) {
        await pool.query('UPDATE leads SET loops_sent=true, followup_attempts=$2 WHERE session_id=$1', [lead.session_id, attempts]);
        if (awsPool) awsPool.query('UPDATE gw_form_leads SET loops_sent=true,updated_at=NOW() WHERE session_id=$1', [lead.session_id]).catch(err => console.warn('[AWS] ⚠ loops_sent sync failed:', err.message));
      } else {
        // Leave loops_sent false so the next cron run picks it up again.
        await pool.query('UPDATE leads SET followup_attempts=$2 WHERE session_id=$1', [lead.session_id, attempts]);
      }

      if (outcome.sent) {
        console.log(`[Cron] ✅ Processed partial for ${lead.email} | completed: ${lead.completed}`);
      } else if (outcome.permanent) {
        console.log(`[Cron] ⏭ Giving up on ${lead.email} — address rejected permanently (${outcome.error})`);
      } else if (attempts >= FOLLOWUP_MAX_ATTEMPTS) {
        console.warn(`[Cron] ⏭ Giving up on ${lead.email} after ${attempts} attempts — last error: ${outcome.error}`);
      } else {
        console.warn(`[Cron] ⚠ Send failed for ${lead.email} (attempt ${attempts}/${FOLLOWUP_MAX_ATTEMPTS}) — will retry: ${outcome.error}`);
      }
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
        // Checked BEFORE the update — the write below sets completed=true,
        // which would mask the very thing we are looking for.
        await alertIfBookingWithoutSubmit(lead.session_id, '/rh-webhook');
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
app.use(createLeadMagnetRouter({ pool, elvIsInternal, FREE_EMAIL_DOMAINS, recordFailure, recordSuccess, DASH_TZ }));

async function start() {
  try {
    await initDB();
    await initAWSTable();
    app.listen(PORT, () => {
      console.log(`[GW API] Running on port ${PORT}`);
      console.log(`[GW API] Allowed origins: ${allowedOrigins.join(', ')}`);
      auditStartupConfig();
      startHeartbeat();
      startPartnerStackCacheWarm();
      startPartnerStackQualificationPoll();
      startPartnerStackConversionVerify();
      startPartnerStackSfStateRefresh();
    });
  } catch (err) { console.error('[GW API] Failed to start:', err); process.exit(1); }
}

start();
