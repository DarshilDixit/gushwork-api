require('dotenv').config();
const express = require('express');
const cors    = require('cors');
const { pool, initDB } = require('./db');

const app  = express();
const PORT = process.env.PORT || 3000;

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

app.use(express.json());

/* --------------------------------------------------------
   LOOPS HELPER — sendLoopsEvent
   Fixed endpoint: createOrUpdate (was upsert which 404'd)
   Separate try/catch so upsert failure never blocks event.
   Single-fire handled in /partial via isNewLead check.
-------------------------------------------------------- */
async function sendLoopsEvent(email, firstName, lastName, company, website) {
  const apiKey = process.env.LOOPS_API_KEY;
  if (!apiKey) { console.warn('[Loops] LOOPS_API_KEY not set — skipping'); return; }
  if (!email) return;

  // Step 1 — Upsert contact + set formCompleted=false
  try {
    const upsertRes  = await fetch('https://app.loops.so/api/v1/contacts/createOrUpdate', {
      method: 'POST',
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

  // Step 2 — Fire the event to trigger the sequence
  try {
    const eventRes  = await fetch('https://app.loops.so/api/v1/events/send', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${apiKey}` },
      body: JSON.stringify({
        email,
        eventName: 'form_partial_capture'
      })
    });
    const eventText = await eventRes.text();
    console.log(`[Loops] Event ${email} → ${eventRes.status} | ${eventText.substring(0, 120)}`);
  } catch (err) {
    console.warn('[Loops] Event send failed:', err.message);
  }
}

/* --------------------------------------------------------
   LOOPS HELPER — cancelLoopsSequence
   Fixed endpoint: createOrUpdate (was upsert which 404'd)
   Called ONLY from /booking-confirmed.
   Sets formCompleted=true so if Audience filter is ever
   re-added, it will correctly suppress the recovery email.
-------------------------------------------------------- */
async function cancelLoopsSequence(email) {
  const apiKey = process.env.LOOPS_API_KEY;
  if (!apiKey || !email) return;

  try {
    const res  = await fetch('https://app.loops.so/api/v1/contacts/createOrUpdate', {
      method: 'POST',
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
   POST /session
-------------------------------------------------------- */
app.post('/session', async (req, res) => {
  const {
    session_id, page_url,
    utm_source, utm_medium, utm_campaign, utm_content,
    referrer, prefill_source
  } = req.body;

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
      page_url       || null,
      utm_source     || null,
      utm_medium     || null,
      utm_campaign   || null,
      utm_content    || null,
      referrer       || null,
      prefill_source || null,
      req.headers['x-forwarded-for'] || req.socket.remoteAddress || null,
      req.headers['user-agent'] || null
    ]);
    res.json({ ok: true });
  } catch (err) {
    console.error('[/session]', err.message);
    res.status(500).json({ error: 'Session save failed' });
  }
});

/* --------------------------------------------------------
   POST /enrich
-------------------------------------------------------- */
app.post('/enrich', async (req, res) => {
  const { email, session_id } = req.body;
  if (!email || !session_id) return res.status(400).json({ error: 'email and session_id required' });

  const blocked = ['gmail.com','yahoo.com','hotmail.com','outlook.com',
                   'icloud.com','protonmail.com','aol.com','mail.com',
                   'yahoo.in','rediffmail.com','ymail.com'];
  const domain = email.split('@')[1]?.toLowerCase() || '';
  if (blocked.includes(domain)) {
    return res.status(400).json({ error: 'Please use a work email address' });
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
   Saves progress to DB.
   Fires Loops ONCE per session for non-disqualified step 1.
-------------------------------------------------------- */
app.post('/partial', async (req, res) => {
  const {
    session_id, page_url,
    email, website, sell_to,
    first_name, last_name, phone, company, hear_about_us,
    utm_source, utm_medium, utm_campaign, utm_content,
    referrer, prefill_source,
    enriched_title, enriched_company_size, enriched_industry, enriched_linkedin,
    disqualified, disqualified_reason,
    step_reached
  } = req.body;

  if (!session_id) return res.status(400).json({ error: 'session_id required' });

  try {
    // Check BEFORE upsert — existing session means Loops already fired
    const existing  = await pool.query('SELECT id FROM leads WHERE session_id = $1', [session_id]);
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
      disqualified          || false,
      disqualified_reason   || null,
      step_reached          || 1
    ]);

    await pool.query(`
      INSERT INTO step_events (session_id, step_number, action)
      VALUES ($1, $2, 'completed')
    `, [session_id, step_reached || 1]);

    // Fire Loops only once per session, only for non-disqualified step 1
    if (step_reached === 1 && email && !disqualified && isNewLead) {
      sendLoopsEvent(email, first_name, last_name, company, website);
      console.log(`[/partial] ✅ Loops queued for ${email} (new session)`);
    } else if (step_reached === 1 && !isNewLead) {
      console.log(`[/partial] ⏭ Loops skipped — existing session ${session_id}`);
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
   Marks lead complete.
   Does NOT cancel Loops — reaching Cal without booking
   still warrants a recovery email.
-------------------------------------------------------- */
app.post('/submit', async (req, res) => {
  const {
    session_id, page_url,
    email, website, sell_to,
    first_name, last_name, phone, company, hear_about_us,
    utm_source, utm_medium, utm_campaign, utm_content,
    referrer, prefill_source,
    enriched_title, enriched_company_size, enriched_industry, enriched_linkedin,
    disqualified, disqualified_reason
  } = req.body;

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
      disqualified          || false,
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
   Only place Loops is cancelled.
   Sets formCompleted=true on contact so if Audience filter
   is ever re-added it will suppress the recovery email.
-------------------------------------------------------- */
app.post('/booking-confirmed', async (req, res) => {
  const { session_id, booking_uid, start_time, end_time, event_type } = req.body;
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
    `, [session_id, booking_uid, start_time||null, end_time||null, event_type||null]);

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
