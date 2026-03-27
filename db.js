const { Pool } = require('pg');
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL?.includes('localhost') ? false : { rejectUnauthorized: false }
});

async function initDB() {
  const client = await pool.connect();
  try {
    await client.query(`
      -- One lead row per visitor, upserted as they progress through steps
      CREATE TABLE IF NOT EXISTS leads (
        id             SERIAL PRIMARY KEY,
        session_id     UUID UNIQUE NOT NULL,
        -- Step 1
        email          TEXT,
        website        TEXT,
        sell_to        TEXT,
        page_url       TEXT,
        -- Step 2
        first_name     TEXT,
        last_name      TEXT,
        phone          TEXT,
        company        TEXT,
        hear_about_us  TEXT,
        -- UTM / attribution
        utm_source     TEXT,
        utm_medium     TEXT,
        utm_campaign   TEXT,
        utm_content    TEXT,
        referrer       TEXT,
        prefill_source TEXT,
        -- Enrichment — person
        enriched_title        TEXT,
        enriched_company_size TEXT,
        enriched_industry     TEXT,
        enriched_linkedin     TEXT,
        enriched_city         TEXT,
        enriched_state        TEXT,
        enriched_country      TEXT,
        enriched_seniority    TEXT,
        enriched_departments  TEXT,
        enriched_email_status TEXT,
        -- Enrichment — org
        enriched_founded_year   TEXT,
        enriched_annual_revenue TEXT,
        enriched_funding_events TEXT,
        enriched_alexa_ranking  TEXT,
        enriched_keywords       TEXT,
        -- Disqualification
        disqualified        BOOLEAN DEFAULT FALSE,
        disqualified_reason TEXT,
        -- Status
        step_reached   INT DEFAULT 1,
        completed      BOOLEAN DEFAULT FALSE,
        submitted_at   TIMESTAMPTZ,
        loops_sent     BOOLEAN DEFAULT FALSE,
        -- Booking
        booking_uid    TEXT,
        start_time     TEXT,
        end_time       TEXT,
        event_type     TEXT,
        booked_at      TIMESTAMPTZ,
        -- Timestamps
        created_at     TIMESTAMPTZ DEFAULT NOW(),
        updated_at     TIMESTAMPTZ DEFAULT NOW()
      );

      -- Enrichment data stored separately, linked to session
      CREATE TABLE IF NOT EXISTS enrichment_data (
        id                      SERIAL PRIMARY KEY,
        session_id              UUID UNIQUE NOT NULL,
        email                   TEXT,
        enriched_first_name     TEXT,
        enriched_last_name      TEXT,
        enriched_title          TEXT,
        enriched_company        TEXT,
        enriched_company_size   TEXT,
        enriched_industry       TEXT,
        enriched_linkedin       TEXT,
        enriched_phone          TEXT,
        -- New enrichment fields
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
        raw_response            JSONB,
        enriched_at             TIMESTAMPTZ DEFAULT NOW()
      );
    `);

    /* -------------------------------------------------------
       MIGRATIONS — runs on every startup, safe due to IF NOT EXISTS
    ------------------------------------------------------- */
    const migrations = [
      // Original fields
      `ALTER TABLE leads ADD COLUMN IF NOT EXISTS page_url TEXT`,
      `ALTER TABLE leads ADD COLUMN IF NOT EXISTS utm_source TEXT`,
      `ALTER TABLE leads ADD COLUMN IF NOT EXISTS utm_medium TEXT`,
      `ALTER TABLE leads ADD COLUMN IF NOT EXISTS utm_campaign TEXT`,
      `ALTER TABLE leads ADD COLUMN IF NOT EXISTS utm_content TEXT`,
      `ALTER TABLE leads ADD COLUMN IF NOT EXISTS referrer TEXT`,
      `ALTER TABLE leads ADD COLUMN IF NOT EXISTS prefill_source TEXT`,
      `ALTER TABLE leads ADD COLUMN IF NOT EXISTS enriched_title TEXT`,
      `ALTER TABLE leads ADD COLUMN IF NOT EXISTS enriched_company_size TEXT`,
      `ALTER TABLE leads ADD COLUMN IF NOT EXISTS enriched_industry TEXT`,
      `ALTER TABLE leads ADD COLUMN IF NOT EXISTS enriched_linkedin TEXT`,
      `ALTER TABLE leads ADD COLUMN IF NOT EXISTS disqualified BOOLEAN DEFAULT FALSE`,
      `ALTER TABLE leads ADD COLUMN IF NOT EXISTS disqualified_reason TEXT`,
      `ALTER TABLE leads ADD COLUMN IF NOT EXISTS submitted_at TIMESTAMPTZ`,
      `ALTER TABLE leads ADD COLUMN IF NOT EXISTS loops_sent BOOLEAN DEFAULT FALSE`,
      // New enrichment fields — person
      `ALTER TABLE leads ADD COLUMN IF NOT EXISTS enriched_city TEXT`,
      `ALTER TABLE leads ADD COLUMN IF NOT EXISTS enriched_state TEXT`,
      `ALTER TABLE leads ADD COLUMN IF NOT EXISTS enriched_country TEXT`,
      `ALTER TABLE leads ADD COLUMN IF NOT EXISTS enriched_seniority TEXT`,
      `ALTER TABLE leads ADD COLUMN IF NOT EXISTS enriched_departments TEXT`,
      `ALTER TABLE leads ADD COLUMN IF NOT EXISTS enriched_email_status TEXT`,
      // New enrichment fields — org
      `ALTER TABLE leads ADD COLUMN IF NOT EXISTS enriched_founded_year TEXT`,
      `ALTER TABLE leads ADD COLUMN IF NOT EXISTS enriched_annual_revenue TEXT`,
      `ALTER TABLE leads ADD COLUMN IF NOT EXISTS enriched_funding_events TEXT`,
      `ALTER TABLE leads ADD COLUMN IF NOT EXISTS enriched_alexa_ranking TEXT`,
      `ALTER TABLE leads ADD COLUMN IF NOT EXISTS enriched_keywords TEXT`,
      // New org/funding fields
      `ALTER TABLE leads ADD COLUMN IF NOT EXISTS enriched_org_hq TEXT`,
      `ALTER TABLE leads ADD COLUMN IF NOT EXISTS enriched_total_funding TEXT`,
      `ALTER TABLE leads ADD COLUMN IF NOT EXISTS enriched_funding_stage TEXT`,
      // enrichment_data table new fields
      `ALTER TABLE enrichment_data ADD COLUMN IF NOT EXISTS enriched_city TEXT`,
      `ALTER TABLE enrichment_data ADD COLUMN IF NOT EXISTS enriched_state TEXT`,
      `ALTER TABLE enrichment_data ADD COLUMN IF NOT EXISTS enriched_country TEXT`,
      `ALTER TABLE enrichment_data ADD COLUMN IF NOT EXISTS enriched_seniority TEXT`,
      `ALTER TABLE enrichment_data ADD COLUMN IF NOT EXISTS enriched_departments TEXT`,
      `ALTER TABLE enrichment_data ADD COLUMN IF NOT EXISTS enriched_email_status TEXT`,
      `ALTER TABLE enrichment_data ADD COLUMN IF NOT EXISTS enriched_founded_year TEXT`,
      `ALTER TABLE enrichment_data ADD COLUMN IF NOT EXISTS enriched_annual_revenue TEXT`,
      `ALTER TABLE enrichment_data ADD COLUMN IF NOT EXISTS enriched_funding_events TEXT`,
      `ALTER TABLE enrichment_data ADD COLUMN IF NOT EXISTS enriched_alexa_ranking TEXT`,
      `ALTER TABLE enrichment_data ADD COLUMN IF NOT EXISTS enriched_keywords TEXT`,
      `ALTER TABLE enrichment_data ADD COLUMN IF NOT EXISTS enriched_org_hq TEXT`,
      `ALTER TABLE enrichment_data ADD COLUMN IF NOT EXISTS enriched_total_funding TEXT`,
      `ALTER TABLE enrichment_data ADD COLUMN IF NOT EXISTS enriched_funding_stage TEXT`,
    ];

    for (const sql of migrations) {
      await client.query(sql);
    }

    console.log('[DB] Tables ready');
  } catch (err) {
    console.error('[DB] Init error:', err);
    throw err;
  } finally {
    client.release();
  }
}

module.exports = { pool, initDB };
