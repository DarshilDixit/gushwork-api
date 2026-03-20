require('dotenv').config();
const express   = require('express');
const cors      = require('cors');
const helmet    = require('helmet');
const rateLimit = require('express-rate-limit');
const { pool, initDB } = require('./db');

const app  = express();
const PORT = process.env.PORT || 3000;

/* --------------------------------------------------------
   SECURITY — Helmet (standard HTTP security headers)
   Sets X-Frame-Options, X-Content-Type-Options, HSTS etc.
-------------------------------------------------------- */
app.use(helmet());

/* --------------------------------------------------------
   CORS — only allow requests from your domains
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

/* --------------------------------------------------------
   BODY PARSER — cap at 10kb to prevent large payload attacks
-------------------------------------------------------- */
app.use(express.json({ limit: '10kb' }));

/* --------------------------------------------------------
   RATE LIMITING

   Global limiter — 100 requests per 15 mins per IP
   Applied to all routes as baseline protection.

   Strict limiter — 10 requests per hour per IP
   Applied only to expensive endpoints that call paid APIs
   (/verify-email → ELV credits, /enrich → Apollo credits)
   Prevents credential draining if someone finds the URL.
-------------------------------------------------------- */
const globalLimiter = rateLimit({
  windowMs:        15 * 60 * 1000, // 15 minutes
  max:             100,
  message:         { error: 'Too many requests — please try again later.' },
  standardHeaders: true,
  legacyHeaders:   false
});

const strictLimiter = rateLimit({
  windowMs:        60 * 60 * 1000, // 1 hour
  max:             10,
  message:         { error: 'Rate limit exceeded — please try again later.' },
  standardHeaders: true,
  legacyHeaders:   false
});

// Apply global limiter to everything
app.use(globalLimiter);

// Apply strict limiter to paid API endpoints
app.use('/verify-email', strictLimiter);
app.use('/enrich',       strictLimiter);

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
   Whitelist approach — only explicitly valid statuses pass.
   8 second timeout. Fails open on error.
   Rate limited to 10/hr per IP via strictLimiter above.
-------------------------------------------------------- */
app.post('/verify-email', async (req, res) => {
  // Sanitize input — max 254 chars (RFC email limit), strip whitespace
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

    const url      = `https://apps.emaillistverify.com/api/verifyEmail?secret=${apiKey}&email=${encodeURIComponent(email)}`;
    const response = await fetch(url, { signal: controller.signal });
    clearTimeout(timeout);

    const text   = await response.text();
    const status = text.trim().toLowerCase();

    console.log(`[ELV] ${email} → "${status}"`);

    // Whitelist — only confirmed valid statuses pass through
    const allowedStatuses = [
      'ok',              // confirmed valid mailbox
      'catch_all',       // domain accepts all emails
      'ok_for_all',      // valid, possible catch-all
      'antispam_system', // real domain blocking SMTP checks
      'accept_all'       // alias for catch_all
    ];

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
  // Sanitize inputs
  const session_id    = (req.body.session_id    || '').toString().trim().slice(0, 100);
  const page_url      = (req.body.page_url      || '').toString().trim().slice(0, 500);
  const utm_source    = (req.body.utm_source    || '').toString().trim().slice(0, 100);
  const utm_medium    = (req.body.utm_medium    || '').toString().trim().slice(0, 100);
  const utm_campaign  = (req.body.utm_campaign  || '').toString().trim().slice(0, 100);
  const utm_content   = (req.body.utm_content   || '').toString().trim().slice(0, 100);
  const referrer      = (req.body.referrer      || '').toString().trim().slice(0, 500);
  const prefill_source = (req.body.prefill_source || '').toString().trim().slice(0, 100);

  if (!session_id) return res.status(400).json({ error: 'session_id required' });

  try {
    await pool.query(`
      INSERT INTO form_sessions
        (session_id, page_url,
         utm_source, utm_medium, utm_campaign, utm_content,
         referrer, prefill_source, ip_address, user_agent)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
      ON CONFLICT (session_id) DO UPDATE SET
        page_url       = COALESCE(EXCLUDED.page_url,       form_sessions.page_url),
        utm_source     = COALESCE(EXCLUDED.utm_source,     form_sessions.utm_source),
        utm_medium     = COALESCE(EXCLUDED.utm_medium,     form_sessions.utm_medium),
        utm_campaign   = COALESCE(EXCLUDED.utm_campaign,   form_sessions.utm_campaign),
        utm_content    = COALESCE(EXCLUDED.utm_content,    form_sessions.utm_content),
        referrer       = COALESCE(EXCLUDED.referrer,       form_sessions.referrer),
        prefill_source = COALESCE(EXCLUDED.prefill_source, form_sessions.prefill_source)
    `, [
      session_id,
      page_url      || null,
      utm_source    || null,
      utm_medium    || null,
      utm_campaign  || null,
      utm_content   || null,
      referrer      || null,
      prefill_source || null,
      req.headers['x-forwarded-for'] || req.socket.remoteAddress || null,
      req.headers['user-agent']?.slice(0, 500) || null
    ]);
    res.json({ ok: true });
  } catch (err) {
    console.error('[/session]', err.message);
    res.status(500).json({ error: 'Session save failed' });
  }
});

/* --------------------------------------------------------
   POST /enrich
   Personal emails skipped — no Apollo credits wasted.
   Rate limited to 10/hr per IP via strictLimiter above.
-------------------------------------------------------- */
app.post('/enrich', async (req, res) => {
  // Sanitize inputs
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
-------------------------------------------------------- */
app.post('/partial', async (req, res) => {
  // Sanitize all string inputs
  const session_id          = (req.body.session_id          || '').toString().trim().slice(0, 100);
  const page_url            = (req.body.page_url            || '').toString().trim().slice(0, 500);
  const email               = (req.body.email               || '').toString().trim().slice(0, 254).toLowerCase();
  const website             = (req.body.website             || '').toString().trim().slice(0, 500);
  const sell_to             = (req.body.sell_to             || '').toString().trim().slice(0, 50);
  const first_name          = (req.body.first_name          || '').toString().trim().slice(0, 100);
  const last_name           = (req.body.last_name           || '').toString().trim().slice(0, 100);
  const phone               = (req.body.phone               || '').toString().trim().slice(0, 30);
  const company             = (req.body.company             || '').toString().trim().slice(0, 200);
  const hear_about_us       = (req.body.hear_about_us       || '').toString().trim().slice(0, 200);
  const utm_source          = (req.body.utm_source          || '').toString().trim().slice(0, 100);
  const utm_medium          = (req.body.utm_medium          || '').toString().trim().slice(0, 100);
  const utm_campaign        = (req.body.utm_campaign        || '').toString().trim().slice(0, 100);
  const utm_content         = (req.body.utm_content         || '').toString().trim().slice(0, 100);
  const referrer            = (req.body.referrer            || '').toString().trim().slice(0, 500);
  const prefill_source      = (req.body.prefill_source      || '').toString().trim().slice(0, 100);
  const enriched_title      = (req.body.enriched_title      || '').toString().trim().slice(0, 200);
  const enriched_company_size = (req.body.enriched_company_size || '').toString().trim().slice(0, 50);
  const enriched_industry   = (req.body.enriched_industry   || '').toString().trim().slice(0, 200);
  const enriched_linkedin   = (req.body.enriched_linkedin   || '').toString().trim().slice(0, 500);
  const disqualified        = Boolean(req.body.disqualified);
  const disqualified_reason = (req.body.disqualified_reason || '').toString().trim().slice(0, 100);
  const step_reached        = parseInt(req.body.step_reached) || 1;

  if (!session_id) return res.status(400).json({ error: 'session_id required' });

  try {
    // Check BEFORE upsert — match session AND email so changing email fires Loops again
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
      session_id,
      page_url              || null,
      email                 || null,
      website               || null,
      sell_to               || null,
      first_name            || null,
      last_name             || null,
      phone                 || null,
      company               || null,
      hear_about_us         || null,
      utm_source            || null,
      utm_medium            || null,
      utm_campaign          || null,
      utm_content           || null,
      referrer              || null,
      prefill_source        || null,
      enriched_title        || null,
      enriched_company_size || null,
      enriched_industry     || null,
      enriched_linkedin     || null,
      disqualified,
      disqualified_reason   || null,
      step_reached
    ]);

    await pool.query(`
      INSERT INTO step_events (session_id, step_number, action)
      VALUES ($1, $2, 'completed')
    `, [session_id, step_reached]);

    if (step_reached === 1 && email && !disqualified && isNewLead) {
      sendLoopsEvent(email, first_name, last_name, company, website);
      console.log(`[/partial] ✅ Loops queued for ${email} (new session+email)`);
    } else if (step_reached === 1 && !isNewLead) {
      console.log(`[/partial] ⏭ Loops skipped — existing session+email ${session_id}`);
    } else if (step_reached === 1 && disqualified) {
      console.log(`[/partial] ⏭ Loops skipped — disqualified (${disqualified_reason}): ${email}`);
    }

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
  const session_id          = (req.body.session_id          || '').toString().trim().slice(0, 100);
  const page_url            = (req.body.page_url            || '').toString().trim().slice(0, 500);
  const email               = (req.body.email               || '').toString().trim().slice(0, 254).toLowerCase();
  const website             = (req.body.website             || '').toString().trim().slice(0, 500);
  const sell_to             = (req.body.sell_to             || '').toString().trim().slice(0, 50);
  const first_name          = (req.body.first_name          || '').toString().trim().slice(0, 100);
  const last_name           = (req.body.last_name           || '').toString().trim().slice(0, 100);
  const phone               = (req.body.phone               || '').toString().trim().slice(0, 30);
  const company             = (req.body.company             || '').toString().trim().slice(0, 200);
  const hear_about_us       = (req.body.hear_about_us       || '').toString().trim().slice(0, 200);
  const utm_source          = (req.body.utm_source          || '').toString().trim().slice(0, 100);
  const utm_medium          = (req.body.utm_medium          || '').toString().trim().slice(0, 100);
  const utm_campaign        = (req.body.utm_campaign        || '').toString().trim().slice(0, 100);
  const utm_content         = (req.body.utm_content         || '').toString().trim().slice(0, 100);
  const referrer            = (req.body.referrer            || '').toString().trim().slice(0, 500);
  const prefill_source      = (req.body.prefill_source      || '').toString().trim().slice(0, 100);
  const enriched_title      = (req.body.enriched_title      || '').toString().trim().slice(0, 200);
  const enriched_company_size = (req.body.enriched_company_size || '').toString().trim().slice(0, 50);
  const enriched_industry   = (req.body.enriched_industry   || '').toString().trim().slice(0, 200);
  const enriched_linkedin   = (req.body.enriched_linkedin   || '').toString().trim().slice(0, 500);
  const disqualified        = Boolean(req.body.disqualified);
  const disqualified_reason = (req.body.disqualified_reason || '').toString().trim().slice(0, 100);

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
      session_id,
      page_url              || null,
      email                 || null,
      website               || null,
      sell_to               || null,
      first_name            || null,
      last_name             || null,
      phone                 || null,
      company               || null,
      hear_about_us         || null,
      utm_source            || null,
      utm_medium            || null,
      utm_campaign          || null,
      utm_content           || null,
      referrer              || null,
      prefill_source        || null,
      enriched_title        || null,
      enriched_company_size || null,
      enriched_industry     || null,
      enriched_linkedin     || null,
      disqualified,
      disqualified_reason   || null
    ]);

    console.log(`[/submit] ✅ Lead completed: ${email} | session: ${session_id} | page: ${page_url}`);
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
        booking_uid = $2,
        start_time  = $3,
        end_time    = $4,
        event_type  = $5,
        booked_at   = NOW(),
        updated_at  = NOW()
      WHERE session_id = $1
    `, [session_id, booking_uid, start_time, end_time, event_type || null]);

    const leadRow = await pool.query(
      'SELECT email FROM leads WHERE session_id = $1',
      [session_id]
    );
    const email = leadRow.rows[0]?.email;
    if (email) cancelLoopsSequence(email);

    console.log(`[/booking-confirmed] ✅ Booked: ${booking_uid} | session: ${session_id} | email: ${email}`);
    res.json({ ok: true });

  } catch (err) {
    console.error('[/booking-confirmed]', err.message);
    res.status(500).json({ error: 'Booking update failed' });
  }
});

/* --------------------------------------------------------
   START
-------------------------------------------------------- */
async function start() {
  try {
    await initDB();
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
