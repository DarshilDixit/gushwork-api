require('dotenv').config();
const express = require('express');
const cors    = require('cors');
const { pool, initDB } = require('./db');

const app  = express();
const PORT = process.env.PORT || 3000;

/* --------------------------------------------------------
   CORS — only allow requests from your Webflow domains
-------------------------------------------------------- */
const allowedOrigins = (process.env.ALLOWED_ORIGIN || '')
  .split(',')
  .map(o => o.trim())
  .filter(Boolean);

app.use(cors({
  origin: function (origin, callback) {
    // Allow requests with no origin (e.g. Postman, Railway healthcheck)
    if (!origin) return callback(null, true);
    if (allowedOrigins.includes(origin)) return callback(null, true);
    callback(new Error(`CORS blocked: ${origin}`));
  },
  methods: ['GET', 'POST', 'OPTIONS'],
  allowedHeaders: ['Content-Type']
}));

app.use(express.json());

/* --------------------------------------------------------
   HEALTH CHECK — Railway uses this to confirm service is up
-------------------------------------------------------- */
app.get('/health', (req, res) => {
  res.json({ status: 'ok', timestamp: new Date().toISOString() });
});

/* --------------------------------------------------------
   POST /session
   Called on page load — creates a session row with UTMs
-------------------------------------------------------- */
app.post('/session', async (req, res) => {
  const {
    session_id,
    utm_source, utm_medium, utm_campaign, utm_content,
    referrer, prefill_source
  } = req.body;

  if (!session_id) return res.status(400).json({ error: 'session_id required' });

  try {
    await pool.query(`
      INSERT INTO form_sessions
        (session_id, utm_source, utm_medium, utm_campaign, utm_content,
         referrer, prefill_source, ip_address, user_agent)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
      ON CONFLICT (session_id) DO UPDATE SET
        utm_source    = EXCLUDED.utm_source,
        utm_medium    = EXCLUDED.utm_medium,
        utm_campaign  = EXCLUDED.utm_campaign,
        utm_content   = EXCLUDED.utm_content,
        referrer      = EXCLUDED.referrer,
        prefill_source = EXCLUDED.prefill_source
    `, [
      session_id,
      utm_source    || null,
      utm_medium    || null,
      utm_campaign  || null,
      utm_content   || null,
      referrer      || null,
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
   Called on email blur — looks up person data via Apollo
   Returns safe enrichment data to prefill step 2
-------------------------------------------------------- */
app.post('/enrich', async (req, res) => {
  const { email, session_id } = req.body;
  if (!email || !session_id) return res.status(400).json({ error: 'email and session_id required' });

  // Basic disposable email block
  const blocked = ['gmail.com','yahoo.com','hotmail.com','outlook.com',
                   'icloud.com','protonmail.com','aol.com','mail.com',
                   'yahoo.in','rediffmail.com','ymail.com'];
  const domain = email.split('@')[1]?.toLowerCase() || '';
  if (blocked.includes(domain)) {
    return res.status(400).json({ error: 'Please use a work email address' });
  }

  try {
    // Call Apollo people/match endpoint
    const apolloRes = await fetch('https://api.apollo.io/api/v1/people/match', {
      method: 'POST',
      headers: {
        'Content-Type':  'application/json',
        'Cache-Control': 'no-cache',
        'X-Api-Key':     process.env.APOLLO_API_KEY
      },
      body: JSON.stringify({
        email,
        reveal_personal_emails: false,
        reveal_phone_number:    false
      })
    });

    const apolloData = await apolloRes.json();
    const person     = apolloData.person || {};
    const org        = person.organization || {};

    // Store enrichment result in DB
    await pool.query(`
      INSERT INTO enrichment_data
        (session_id, email, enriched_first_name, enriched_last_name,
         enriched_title, enriched_company, enriched_company_size,
         enriched_industry, enriched_linkedin, raw_response)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
      ON CONFLICT (session_id) DO UPDATE SET
        email                = EXCLUDED.email,
        enriched_first_name  = EXCLUDED.enriched_first_name,
        enriched_last_name   = EXCLUDED.enriched_last_name,
        enriched_title       = EXCLUDED.enriched_title,
        enriched_company     = EXCLUDED.enriched_company,
        enriched_company_size = EXCLUDED.enriched_company_size,
        enriched_industry    = EXCLUDED.enriched_industry,
        enriched_linkedin    = EXCLUDED.enriched_linkedin,
        raw_response         = EXCLUDED.raw_response,
        enriched_at          = NOW()
    `, [
      session_id,
      email,
      person.first_name             || null,
      person.last_name              || null,
      person.title                  || null,
      org.name                      || null,
      org.estimated_num_employees?.toString() || null,
      org.industry                  || null,
      person.linkedin_url           || null,
      JSON.stringify(apolloData)
    ]);

    // Return ONLY safe fields to the browser — never return raw Apollo response
    res.json({
      first_name:   person.first_name   || '',
      last_name:    person.last_name    || '',
      title:        person.title        || '',
      company:      org.name            || '',
      company_size: org.estimated_num_employees?.toString() || '',
      industry:     org.industry        || '',
      linkedin_url: person.linkedin_url || '',
      website:      org.website_url     || ''
    });

  } catch (err) {
    console.error('[/enrich]', err.message);
    // Return empty enrichment — never block the user
    res.json({
      first_name: '', last_name: '', title: '',
      company: '', company_size: '', industry: '', linkedin_url: ''
    });
  }
});

/* --------------------------------------------------------
   POST /partial
   Called on every Next click — saves progress to DB
   Never blocks the user even if it fails
-------------------------------------------------------- */
app.post('/partial', async (req, res) => {
  const {
    session_id, email, website, sell_to,
    first_name, last_name, phone, company, hear_about_us,
    step_reached
  } = req.body;

  if (!session_id) return res.status(400).json({ error: 'session_id required' });

  try {
    await pool.query(`
      INSERT INTO leads
        (session_id, email, website, sell_to,
         first_name, last_name, phone, company, hear_about_us,
         step_reached, completed, updated_at)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,false,NOW())
      ON CONFLICT (session_id) DO UPDATE SET
        email         = COALESCE(EXCLUDED.email,         leads.email),
        website       = COALESCE(EXCLUDED.website,       leads.website),
        sell_to       = COALESCE(EXCLUDED.sell_to,       leads.sell_to),
        first_name    = COALESCE(EXCLUDED.first_name,    leads.first_name),
        last_name     = COALESCE(EXCLUDED.last_name,     leads.last_name),
        phone         = COALESCE(EXCLUDED.phone,         leads.phone),
        company       = COALESCE(EXCLUDED.company,       leads.company),
        hear_about_us = COALESCE(EXCLUDED.hear_about_us, leads.hear_about_us),
        step_reached  = GREATEST(EXCLUDED.step_reached,  leads.step_reached),
        updated_at    = NOW()
    `, [
      session_id,
      email         || null,
      website       || null,
      sell_to       || null,
      first_name    || null,
      last_name     || null,
      phone         || null,
      company       || null,
      hear_about_us || null,
      step_reached  || 1
    ]);

    // Track step event
    await pool.query(`
      INSERT INTO step_events (session_id, step_number, action)
      VALUES ($1, $2, 'completed')
    `, [session_id, step_reached || 1]);

    res.json({ ok: true });
  } catch (err) {
    console.error('[/partial]', err.message);
    res.status(500).json({ error: 'Partial save failed' });
  }
});

/* --------------------------------------------------------
   POST /submit
   Called when step 2 Next is clicked — marks lead complete
-------------------------------------------------------- */
app.post('/submit', async (req, res) => {
  const {
    session_id, email, website, sell_to,
    first_name, last_name, phone, company, hear_about_us,
    utm_source, utm_medium, utm_campaign, utm_content,
    referrer, prefill_source,
    enriched_title, enriched_company_size, enriched_industry
  } = req.body;

  if (!session_id) return res.status(400).json({ error: 'session_id required' });

  try {
    await pool.query(`
      INSERT INTO leads
        (session_id, email, website, sell_to,
         first_name, last_name, phone, company, hear_about_us,
         step_reached, completed, submitted_at, updated_at)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9, 2, true, NOW(), NOW())
      ON CONFLICT (session_id) DO UPDATE SET
        email         = COALESCE(EXCLUDED.email,         leads.email),
        website       = COALESCE(EXCLUDED.website,       leads.website),
        sell_to       = COALESCE(EXCLUDED.sell_to,       leads.sell_to),
        first_name    = COALESCE(EXCLUDED.first_name,    leads.first_name),
        last_name     = COALESCE(EXCLUDED.last_name,     leads.last_name),
        phone         = COALESCE(EXCLUDED.phone,         leads.phone),
        company       = COALESCE(EXCLUDED.company,       leads.company),
        hear_about_us = COALESCE(EXCLUDED.hear_about_us, leads.hear_about_us),
        step_reached  = 2,
        completed     = true,
        submitted_at  = NOW(),
        updated_at    = NOW()
    `, [
      session_id,
      email         || null,
      website       || null,
      sell_to       || null,
      first_name    || null,
      last_name     || null,
      phone         || null,
      company       || null,
      hear_about_us || null
    ]);

    // Update session with latest UTM data if provided
    if (utm_source || utm_campaign) {
      await pool.query(`
        UPDATE form_sessions SET
          utm_source    = COALESCE($2, utm_source),
          utm_medium    = COALESCE($3, utm_medium),
          utm_campaign  = COALESCE($4, utm_campaign),
          utm_content   = COALESCE($5, utm_content),
          referrer      = COALESCE($6, referrer),
          prefill_source = COALESCE($7, prefill_source)
        WHERE session_id = $1
      `, [session_id, utm_source||null, utm_medium||null,
          utm_campaign||null, utm_content||null,
          referrer||null, prefill_source||null]);
    }

    console.log(`[/submit] Lead completed: ${email} | session: ${session_id}`);
    res.json({ ok: true });

  } catch (err) {
    console.error('[/submit]', err.message);
    res.status(500).json({ error: 'Submit failed' });
  }
});

/* --------------------------------------------------------
   POST /booking-confirmed
   Called when Cal.com booking is successful
   Links booking UID to the lead record
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

    console.log(`[/booking-confirmed] Booked: ${booking_uid} | session: ${session_id}`);
    res.json({ ok: true });

  } catch (err) {
    console.error('[/booking-confirmed]', err.message);
    res.status(500).json({ error: 'Booking update failed' });
  }
});

/* --------------------------------------------------------
   START SERVER
-------------------------------------------------------- */
async function start() {
  try {
    await initDB(); // create tables if they don't exist
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
