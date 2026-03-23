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
app.use(helmet());

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
        id                    SERIAL PRIMARY KEY,
        session_id            TEXT UNIQUE NOT NULL,
        page_url              TEXT,
        email                 TEXT,
        website               TEXT,
        sell_to               TEXT,
        first_name            TEXT,
        last_name             TEXT,
        phone                 TEXT,
        company               TEXT,
        hear_about_us         TEXT,
        utm_source            TEXT,
        utm_medium            TEXT,
        utm_campaign          TEXT,
        utm_content           TEXT,
        referrer              TEXT,
        prefill_source        TEXT,
        enriched_title        TEXT,
        enriched_company_size TEXT,
        enriched_industry     TEXT,
        enriched_linkedin     TEXT,
        disqualified          BOOLEAN DEFAULT FALSE,
        disqualified_reason   TEXT,
        step_reached          INT DEFAULT 1,
        completed             BOOLEAN DEFAULT FALSE,
        submitted_at          TIMESTAMPTZ,
        booking_uid           TEXT,
        start_time            TEXT,
        end_time              TEXT,
        event_type            TEXT,
        booked_at             TIMESTAMPTZ,
        created_at            TIMESTAMPTZ DEFAULT NOW(),
        updated_at            TIMESTAMPTZ DEFAULT NOW()
      )
    `);
    console.log('[AWS] gw_form_leads table ready');
  } catch (err) {
    console.warn('[AWS] Table init failed (non-blocking):', err.message);
  }
}

/* --------------------------------------------------------
   AWS HELPER — syncToAWS
   Fire-and-forget — never blocks response
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
       disqualified, disqualified_reason,
       step_reached, completed, updated_at)
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,NOW())
    ON CONFLICT (session_id) DO UPDATE SET
      page_url              = COALESCE(EXCLUDED.page_url,              gw_form_leads.page_url),
      email                 = COALESCE(EXCLUDED.email,                 gw_form_leads.email),
      website               = COALESCE(EXCLUDED.website,               gw_form_leads.website),
      sell_to               = COALESCE(EXCLUDED.sell_to,               gw_form_leads.sell_to),
      first_name            = COALESCE(EXCLUDED.first_name,            gw_form_leads.first_name),
      last_name             = COALESCE(EXCLUDED.last_name,             gw_form_leads.last_name),
      phone                 = COALESCE(EXCLUDED.phone,                 gw_form_leads.phone),
      company               = COALESCE(EXCLUDED.company,               gw_form_leads.company),
      hear_about_us         = COALESCE(EXCLUDED.hear_about_us,         gw_form_leads.hear_about_us),
      utm_source            = COALESCE(EXCLUDED.utm_source,            gw_form_leads.utm_source),
      utm_medium            = COALESCE(EXCLUDED.utm_medium,            gw_form_leads.utm_medium),
      utm_campaign          = COALESCE(EXCLUDED.utm_campaign,          gw_form_leads.utm_campaign),
      utm_content           = COALESCE(EXCLUDED.utm_content,           gw_form_leads.utm_content),
      referrer              = COALESCE(EXCLUDED.referrer,              gw_form_leads.referrer),
      prefill_source        = COALESCE(EXCLUDED.prefill_source,        gw_form_leads.prefill_source),
      enriched_title        = COALESCE(EXCLUDED.enriched_title,        gw_form_leads.enriched_title),
      enriched_company_size = COALESCE(EXCLUDED.enriched_company_size, gw_form_leads.enriched_company_size),
      enriched_industry     = COALESCE(EXCLUDED.enriched_industry,     gw_form_leads.enriched_industry),
      enriched_linkedin     = COALESCE(EXCLUDED.enriched_linkedin,     gw_form_leads.enriched_linkedin),
      disqualified          = COALESCE(EXCLUDED.disqualified,          gw_form_leads.disqualified),
      disqualified_reason   = COALESCE(EXCLUDED.disqualified_reason,   gw_form_leads.disqualified_reason),
      step_reached          = GREATEST(EXCLUDED.step_reached,          gw_form_leads.step_reached),
      updated_at            = NOW()
  `, [
    data.session_id,                   data.page_url              || null,
    data.email             || null,    data.website               || null,
    data.sell_to           || null,    data.first_name            || null,
    data.last_name         || null,    data.phone                 || null,
    data.company           || null,    data.hear_about_us         || null,
    data.utm_source        || null,    data.utm_medium            || null,
    data.utm_campaign      || null,    data.utm_content           || null,
    data.referrer          || null,    data.prefill_source        || null,
    data.enriched_title    || null,    data.enriched_company_size || null,
    data.enriched_industry || null,    data.enriched_linkedin     || null,
    data.disqualified      || false,   data.disqualified_reason   || null,
    data.step_reached      || 1,       data.completed             || false
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
      booking_uid = $2,
      start_time  = $3,
      end_time    = $4,
      event_type  = $5,
      booked_at   = NOW(),
      updated_at  = NOW()
    WHERE session_id = $1
  `, [session_id, booking_uid, start_time || null, end_time || null, event_type || null])
  .then(() => console.log(`[AWS] ✅ Booking synced for session ${session_id}`))
  .catch(err => console.warn(`[AWS] ⚠ Booking sync failed:`, err.message));
}

/* --------------------------------------------------------
   SLACK HELPER — sendSlack
   Fire-and-forget. Reads from SLACK_WEBHOOK_URL env var.
-------------------------------------------------------- */
function sendSlack(text) {
  const webhookUrl = process.env.SLACK_WEBHOOK_URL;
  if (!webhookUrl) {
    console.warn('[Slack] SLACK_WEBHOOK_URL not set — skipping');
    return;
  }
  fetch(webhookUrl, {
    method:  'POST',
    headers: { 'Content-Type': 'application/json' },
    body:    JSON.stringify({ text })
  })
  .then(() => console.log('[Slack] ✅ Notification sent'))
  .catch(err => console.warn('[Slack] ⚠ Failed:', err.message));
}

/* --------------------------------------------------------
   SLACK FORMATTER — partial (genuine drop-off, fired from cron)
   Available: email, sell_to, UTMs, referrer, page_url
   completed flag tells us if they reached Cal or dropped at step 1
-------------------------------------------------------- */
function slackPartial({ email, sell_to, utm_source, utm_medium, utm_campaign, utm_content, referrer, page_url, disqualified, disqualified_reason, completed }) {
  const source   = [utm_source, utm_medium].filter(Boolean).join(' / ') || '—';
  const campaign = utm_campaign || '—';
  const content  = utm_content  || '—';
  const ref      = referrer     || '—';
  const page     = page_url     || '—';
  const disqNote = disqualified ? `\n⚠️ Disqualified: ${disqualified_reason || 'unknown'}` : '';

  // completed = true means they reached Cal but didn't book
  // completed = false means they dropped at step 1
  const label = completed
    ? '⏰ Reached Cal — Did Not Book'
    : '👻 Dropped at Step 1';

  sendSlack(
`${label}${disqNote}

👤 ${email || '—'}
🎯 Sells to: ${sell_to || '—'}

📊 Attribution
├ Source: ${source}
├ Campaign: ${campaign}
├ Content: ${content}
├ Referrer: ${ref}
└ Page: ${page}`
  );
}

/* --------------------------------------------------------
   SLACK FORMATTER — submit (step 2 complete)
   Available: everything — name, phone, company, website,
   hear_about_us, sell_to, enrichment, full attribution
-------------------------------------------------------- */
function slackSubmit({ first_name, last_name, email, phone, company, website, sell_to, hear_about_us, enriched_title, enriched_company_size, enriched_industry, enriched_linkedin, utm_source, utm_medium, utm_campaign, utm_content, referrer, prefill_source, page_url }) {
  const name     = [first_name, last_name].filter(Boolean).join(' ') || '—';
  const source   = [utm_source, utm_medium].filter(Boolean).join(' / ') || '—';
  const campaign = utm_campaign   || '—';
  const content  = utm_content    || '—';
  const ref      = referrer       || '—';
  const prefill  = prefill_source || '—';
  const page     = page_url       || '—';

  const enrichmentBlock = (enriched_title || enriched_company_size || enriched_industry || enriched_linkedin)
    ? `\n🔍 Enrichment\n├ Title: ${enriched_title || '—'}\n├ Company Size: ${enriched_company_size || '—'}\n├ Industry: ${enriched_industry || '—'}\n└ LinkedIn: ${enriched_linkedin || '—'}\n`
    : '';

  sendSlack(
`✅ Lead Form Completed

👤 ${name} — ${email || '—'}
📞 ${phone || '—'}
🏢 ${company || '—'}
🌐 ${website || '—'}
🎯 Sells to: ${sell_to || '—'}
💬 Heard about us: ${hear_about_us || '—'}
${enrichmentBlock}
📊 Attribution
├ Source: ${source}
├ Campaign: ${campaign}
├ Content: ${content}
├ Referrer: ${ref}
├ Prefill: ${prefill}
└ Page: ${page}`
  );
}

/* --------------------------------------------------------
   SLACK FORMATTER — booking confirmed
   Available: full lead from DB + booking details
   website, hear_about_us, sell_to all available since
   they were saved at step 2 before booking
-------------------------------------------------------- */
function slackBooking({ first_name, last_name, email, phone, company, website, sell_to, hear_about_us, booking_uid, start_time, event_type, utm_source, utm_medium, utm_campaign, utm_content, referrer, page_url }) {
  const name     = [first_name, last_name].filter(Boolean).join(' ') || '—';
  const source   = [utm_source, utm_medium].filter(Boolean).join(' / ') || '—';
  const campaign = utm_campaign || '—';
  const content  = utm_content  || '—';
  const ref      = referrer     || '—';
  const page     = page_url     || '—';

  let formattedTime = '—';
  if (start_time) {
    try {
      formattedTime = new Date(start_time).toLocaleString('en-IN', {
        timeZone: 'Asia/Kolkata',
        weekday:  'short', year: 'numeric', month: 'short', day: 'numeric',
        hour: '2-digit', minute: '2-digit'
      });
    } catch { formattedTime = start_time; }
  }

  sendSlack(
`🎉 Demo Booked!

👤 ${name} — ${email || '—'}
📞 ${phone || '—'}
🏢 ${company || '—'}
🌐 ${website || '—'}
🎯 Sells to: ${sell_to || '—'}
💬 Heard about us: ${hear_about_us || '—'}

📅 Booking
├ ID: ${booking_uid || '—'}
├ Time: ${formattedTime}
└ Event: ${event_type || '—'}

📊 Attribution
├ Source: ${source}
├ Campaign: ${campaign}
├ Content: ${content}
├ Referrer: ${ref}
└ Page: ${page}`
  );
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
      body: JSON.stringify({
        email,
        firstName:     firstName || '',
        lastName:      lastName  || '',
        company:       company   || '',
        website:       website   || '',
        formCompleted: false
      })
    });
    const upsertText = await upsertRes.text();
    console.log(`[Loops] Upsert ${email} → ${upsertRes.status} | ${upsertText.substring(0, 120)}`);
  } catch (err) {
    console.warn('[Loops] Upsert failed (non-blocking):', err.message);
  }

  try {
    const eventRes = await fetch('https://app.loops.so/api/v1/events/send', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${apiKey}` },
      body: JSON.stringify({ email, eventName: 'form_partial_capture' })
    });
    const eventText = await eventRes.text();
    console.log(`[Loops] Event ${email} → ${eventRes.status} | ${eventText.substring(0, 120)}`);
  } catch (err) {
    console.warn('[Loops] Event send failed:', err.message);
  }
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
  } catch (err) {
    console.warn('[Loops] Cancel failed:', err.message);
  }
}

/* --------------------------------------------------------
   HEALTH CHECK
-------------------------------------------------------- */
app.get('/health', (req, res) => {
  res.json({ status: 'ok', timestamp: new Date().toISOString() });
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
   POST /session  (page load — acknowledge only)
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
      headers: {
        'Content-Type':  'application/json',
        'Cache-Control': 'no-cache',
        'X-Api-Key':     process.env.APOLLO_API_KEY
      },
      body: JSON.stringify({ email, reveal_personal_emails: false, reveal_phone_number: false })
    });

    const apolloData = await apolloRes.json();
    const person     = apolloData.person || {};
    const org        = person.organization || {};

    await pool.query(`
      INSERT INTO enrichment_data
        (session_id, email, enriched_first_name, enriched_last_name,
         enriched_title, enriched_company, enriched_company_size,
         enriched_industry, enriched_linkedin, raw_response)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
      ON CONFLICT (session_id) DO UPDATE SET
        email                 = EXCLUDED.email,
        enriched_first_name   = EXCLUDED.enriched_first_name,
        enriched_last_name    = EXCLUDED.enriched_last_name,
        enriched_title        = EXCLUDED.enriched_title,
        enriched_company      = EXCLUDED.enriched_company,
        enriched_company_size = EXCLUDED.enriched_company_size,
        enriched_industry     = EXCLUDED.enriched_industry,
        enriched_linkedin     = EXCLUDED.enriched_linkedin,
        raw_response          = EXCLUDED.raw_response,
        enriched_at           = NOW()
    `, [
      session_id, email,
      person.first_name                       || null,
      person.last_name                        || null,
      person.title                            || null,
      org.name                                || null,
      org.estimated_num_employees?.toString() || null,
      org.industry                            || null,
      person.linkedin_url                     || null,
      JSON.stringify(apolloData)
    ]);

    res.json({
      first_name:   person.first_name                       || '',
      last_name:    person.last_name                        || '',
      title:        person.title                            || '',
      company:      org.name                                || '',
      company_size: org.estimated_num_employees?.toString() || '',
      industry:     org.industry                            || '',
      linkedin_url: person.linkedin_url                     || '',
      website:      org.website_url                         || ''
    });

  } catch (err) {
    console.error('[/enrich]', err.message);
    res.json({ first_name: '', last_name: '', title: '', company: '', company_size: '', industry: '', linkedin_url: '', website: '' });
  }
});

/* --------------------------------------------------------
   POST /partial
   Railway write + AWS sync + Slack partial + Loops
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
    const existing  = await pool.query(
      'SELECT id FROM leads WHERE session_id = $1 AND email = $2',
      [session_id, email]
    );
    const isNewLead = existing.rows.length === 0;

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
      session_id,                    page_url              || null,
      email             || null,     website               || null,
      sell_to           || null,     first_name            || null,
      last_name         || null,     phone                 || null,
      company           || null,     hear_about_us         || null,
      utm_source        || null,     utm_medium            || null,
      utm_campaign      || null,     utm_content           || null,
      referrer          || null,     prefill_source        || null,
      enriched_title    || null,     enriched_company_size || null,
      enriched_industry || null,     enriched_linkedin     || null,
      disqualified,                  disqualified_reason   || null,
      step_reached
    ]);

    // AWS sync — non-blocking
    syncToAWS({
      session_id, page_url, email, website, sell_to,
      first_name, last_name, phone, company, hear_about_us,
      utm_source, utm_medium, utm_campaign, utm_content,
      referrer, prefill_source,
      enriched_title, enriched_company_size, enriched_industry, enriched_linkedin,
      disqualified, disqualified_reason, step_reached, completed: false
    });

    // Slack partial is NOT fired here — cron handles it after 30 mins
    // so we only notify for genuine partials who didn't complete

    // Loops is NOT fired here — cron job handles it after 30 mins
    // Only non-bookers who are still partial after 30 mins get pushed to Loops
    console.log(`[/partial] ✅ Saved session ${session_id} | step ${step_reached} | email ${email}`);

    res.json({ ok: true });
  } catch (err) {
    console.error('[/partial]', err.message);
    res.status(500).json({ error: 'Partial save failed' });
  }
});

/* --------------------------------------------------------
   POST /submit
   Railway write + AWS sync + Slack submit
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
      session_id,                    page_url              || null,
      email             || null,     website               || null,
      sell_to           || null,     first_name            || null,
      last_name         || null,     phone                 || null,
      company           || null,     hear_about_us         || null,
      utm_source        || null,     utm_medium            || null,
      utm_campaign      || null,     utm_content           || null,
      referrer          || null,     prefill_source        || null,
      enriched_title    || null,     enriched_company_size || null,
      enriched_industry || null,     enriched_linkedin     || null,
      disqualified,                  disqualified_reason   || null
    ]);

    // AWS sync — non-blocking
    syncToAWS({
      session_id, page_url, email, website, sell_to,
      first_name, last_name, phone, company, hear_about_us,
      utm_source, utm_medium, utm_campaign, utm_content,
      referrer, prefill_source,
      enriched_title, enriched_company_size, enriched_industry, enriched_linkedin,
      disqualified, disqualified_reason, step_reached: 2, completed: true
    });

    // Slack — non-blocking
    slackSubmit({
      first_name, last_name, email, phone, company, website,
      sell_to, hear_about_us,
      enriched_title, enriched_company_size, enriched_industry, enriched_linkedin,
      utm_source, utm_medium, utm_campaign, utm_content,
      referrer, prefill_source, page_url
    });

    // Loops is NOT fired here — cron job handles it
    console.log(`[/submit] ✅ Lead completed: ${email} | session: ${session_id}`);
    res.json({ ok: true });

  } catch (err) {
    console.error('[/submit]', err.message);
    res.status(500).json({ error: 'Submit failed' });
  }
});

/* --------------------------------------------------------
   POST /booking-confirmed
   Railway update + AWS sync + Loops cancel + Slack booking
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
        booking_uid = $2,
        start_time  = $3,
        end_time    = $4,
        event_type  = $5,
        booked_at   = NOW(),
        updated_at  = NOW()
      WHERE session_id = $1
    `, [session_id, booking_uid, start_time, end_time, event_type || null]);

    // AWS booking sync — non-blocking
    syncBookingToAWS(session_id, booking_uid, start_time, end_time, event_type);

    // Fetch full lead for Slack + Loops
    const leadRow = await pool.query(
      `SELECT email, first_name, last_name, phone, company, website,
              sell_to, hear_about_us,
              utm_source, utm_medium, utm_campaign, utm_content,
              referrer, page_url
       FROM leads WHERE session_id = $1`,
      [session_id]
    );
    const lead  = leadRow.rows[0] || {};
    const email = lead.email;

    // Slack booking notification — non-blocking
    slackBooking({
      first_name:    lead.first_name,
      last_name:     lead.last_name,
      email:         lead.email,
      phone:         lead.phone,
      company:       lead.company,
      website:       lead.website,
      sell_to:       lead.sell_to,
      hear_about_us: lead.hear_about_us,
      booking_uid,
      start_time,
      event_type,
      utm_source:    lead.utm_source,
      utm_medium:    lead.utm_medium,
      utm_campaign:  lead.utm_campaign,
      utm_content:   lead.utm_content,
      referrer:      lead.referrer,
      page_url:      lead.page_url
    });

    // Cancel Loops recovery sequence
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
   Called by Railway cron every 30 mins.
   Finds leads who:
     - filled the form (have an email)
     - are NOT disqualified (waitlist)
     - have NOT booked a call (booking_uid IS NULL)
     - were created more than 30 mins ago
     - haven't been sent to Loops yet (loops_sent = false)
   For each: sends Slack partial notification + pushes to Loops
   Covers both step 1 only AND step 1+2 without booking.
-------------------------------------------------------- */
app.post('/cron/send-partials', async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT session_id, email, first_name, last_name,
             company, website, sell_to,
             utm_source, utm_medium, utm_campaign, utm_content,
             referrer, page_url,
             disqualified, disqualified_reason,
             completed
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
      // Send Slack partial notification
      slackPartial({
        email:               lead.email,
        sell_to:             lead.sell_to,
        utm_source:          lead.utm_source,
        utm_medium:          lead.utm_medium,
        utm_campaign:        lead.utm_campaign,
        utm_content:         lead.utm_content,
        referrer:            lead.referrer,
        page_url:            lead.page_url,
        disqualified:        lead.disqualified,
        disqualified_reason: lead.disqualified_reason,
        completed:           lead.completed
      });

      // Push to Loops
      await sendLoopsEvent(
        lead.email,
        lead.first_name,
        lead.last_name,
        lead.company,
        lead.website
      );

      // Mark as sent so it never fires again
      await pool.query(
        'UPDATE leads SET loops_sent = true WHERE session_id = $1',
        [lead.session_id]
      );

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
