const { Pool } = require('pg');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL?.includes('localhost') ? false : { rejectUnauthorized: false }
});

// Create all tables if they don't exist
async function initDB() {
  const client = await pool.connect();
  try {
    await client.query(`

      -- Every form visit gets a session row created on page load
      CREATE TABLE IF NOT EXISTS form_sessions (
        id            SERIAL PRIMARY KEY,
        session_id    UUID UNIQUE NOT NULL,
        utm_source    TEXT,
        utm_medium    TEXT,
        utm_campaign  TEXT,
        utm_content   TEXT,
        referrer      TEXT,
        prefill_source TEXT,
        ip_address    TEXT,
        user_agent    TEXT,
        created_at    TIMESTAMPTZ DEFAULT NOW()
      );

      -- One lead row per visitor, upserted as they progress through steps
      CREATE TABLE IF NOT EXISTS leads (
        id            SERIAL PRIMARY KEY,
        session_id    UUID UNIQUE NOT NULL,
        -- Step 1
        email         TEXT,
        website       TEXT,
        sell_to       TEXT,
        page_url      TEXT,
        -- Step 2
        first_name    TEXT,
        last_name     TEXT,
        phone         TEXT,
        company       TEXT,
        hear_about_us TEXT,
        -- Status
        step_reached  INT DEFAULT 1,
        completed     BOOLEAN DEFAULT FALSE,
        submitted_at  TIMESTAMPTZ,
        -- Booking
        booking_uid   TEXT,
        start_time    TEXT,
        end_time      TEXT,
        event_type    TEXT,
        booked_at     TIMESTAMPTZ,
        -- Timestamps
        created_at    TIMESTAMPTZ DEFAULT NOW(),
        updated_at    TIMESTAMPTZ DEFAULT NOW()
      );

      -- Enrichment data stored separately, linked to session
      CREATE TABLE IF NOT EXISTS enrichment_data (
        id                  SERIAL PRIMARY KEY,
        session_id          UUID UNIQUE NOT NULL,
        email               TEXT,
        enriched_first_name TEXT,
        enriched_last_name  TEXT,
        enriched_title      TEXT,
        enriched_company    TEXT,
        enriched_company_size TEXT,
        enriched_industry   TEXT,
        enriched_linkedin   TEXT,
        enriched_phone      TEXT,
        raw_response        JSONB,
        enriched_at         TIMESTAMPTZ DEFAULT NOW()
      );

      -- Every step view/completion tracked for funnel analytics
      CREATE TABLE IF NOT EXISTS step_events (
        id          SERIAL PRIMARY KEY,
        session_id  UUID NOT NULL,
        step_number INT,
        action      TEXT, -- 'viewed', 'completed', 'abandoned'
        created_at  TIMESTAMPTZ DEFAULT NOW()
      );

    `);
    console.log('[DB] Tables ready');
  } catch (err) {
    console.error('[DB] Init error:', err);
    throw err;
  } finally {
    client.release();
  }
}

module.exports = { pool, initDB };
