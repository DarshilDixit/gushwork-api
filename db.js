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
        utm_term       TEXT,
        referrer       TEXT,
        prefill_source TEXT,
        -- Meta ads attribution
        fbc            TEXT,
        fbp            TEXT,
        landing_page   TEXT,
        previous_page  TEXT,
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
       LEAD MAGNET — 150 buyer questions LP
       Separate table. Never joined to `leads` at write time.

       One row per SESSION, not per lead: the row is created on page
       load, so most rows have no email. Those are the funnel
       denominator — they are what lets you see view -> open -> email
       -> submit instead of guessing. Anything that reads this as
       "leads" filters on completed = true (see the view below).

       Wrapped in try/catch ON PURPOSE. start() does `await initDB()`
       then process.exit(1) on throw, so an unwrapped failure here
       would take down /verify-email and /submit for the demo form
       too. Worst case now: /lm/* returns 500s and everything already
       in production keeps running.
    ------------------------------------------------------- */
    try {
      await client.query(`
        CREATE TABLE IF NOT EXISTS lead_magnet_leads (
          id                 SERIAL PRIMARY KEY,
          session_id         TEXT UNIQUE NOT NULL,
          linked_session_id  TEXT,
          -- captured
          email              TEXT,
          is_free_email      BOOLEAN DEFAULT FALSE,
          is_internal        BOOLEAN DEFAULT FALSE,
          website            TEXT,
          website_source     TEXT,          -- 'entered' | 'derived_from_email'
          industry_category  TEXT,
          industry_is_custom BOOLEAN DEFAULT FALSE,
          product_or_service TEXT,
          sell_to            TEXT,
          -- verification
          elv_status         TEXT,
          elv_checked_at     TIMESTAMPTZ,
          -- attribution
          page_url           TEXT,
          landing_page       TEXT,
          previous_page      TEXT,
          referrer           TEXT,
          utm_source         TEXT,
          utm_medium         TEXT,
          utm_campaign       TEXT,
          utm_content        TEXT,
          utm_term           TEXT,
          fbc                TEXT,
          fbp                TEXT,
          user_agent         TEXT,
          -- funnel
          step_reached       INT DEFAULT 1,
          completed          BOOLEAN DEFAULT FALSE,
          submitted_at       TIMESTAMPTZ,
          -- handoff
          delivered          BOOLEAN DEFAULT FALSE,
          delivered_at       TIMESTAMPTZ,
          delivery_note      TEXT,
          -- meta
          capi_contact_sent  BOOLEAN DEFAULT FALSE,
          created_at         TIMESTAMPTZ DEFAULT NOW(),
          updated_at         TIMESTAMPTZ DEFAULT NOW()
        );

        CREATE INDEX IF NOT EXISTS lm_pending_idx
          ON lead_magnet_leads (submitted_at)
          WHERE completed = true AND delivered = false;
        CREATE INDEX IF NOT EXISTS lm_email_idx   ON lead_magnet_leads (email);
        CREATE INDEX IF NOT EXISTS lm_created_idx ON lead_magnet_leads (created_at);

        -- The handoff surface. The email team reads THIS, not the table.
        -- Partials, internal tests and already-sent leads are filtered by
        -- the database, so nobody downstream can forget a WHERE clause.
        CREATE OR REPLACE VIEW lead_magnet_queue AS
          SELECT id, session_id, email, website, website_source,
                 industry_category, industry_is_custom,
                 product_or_service, sell_to, is_free_email,
                 utm_source, utm_campaign, submitted_at
            FROM lead_magnet_leads
           WHERE completed = true
             AND delivered = false
             AND is_internal IS NOT TRUE
           ORDER BY submitted_at;
      `);
      console.log('[DB] Lead-magnet table ready');
    } catch (err) {
      console.error('[DB] Lead-magnet table init FAILED (non-fatal):', err.message);
    }

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
      // Meta ads attribution fields
      `ALTER TABLE leads ADD COLUMN IF NOT EXISTS fbc TEXT`,
      `ALTER TABLE leads ADD COLUMN IF NOT EXISTS fbp TEXT`,
      `ALTER TABLE leads ADD COLUMN IF NOT EXISTS landing_page TEXT`,
      `ALTER TABLE leads ADD COLUMN IF NOT EXISTS utm_term TEXT`,
      // Journey tracking — new
      `ALTER TABLE leads ADD COLUMN IF NOT EXISTS previous_page TEXT`,
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
      // Lead magnet — add new columns here, same pattern as above
      `ALTER TABLE lead_magnet_leads ADD COLUMN IF NOT EXISTS website_source TEXT`,
      `ALTER TABLE lead_magnet_leads ADD COLUMN IF NOT EXISTS entry_point TEXT`,
      `ALTER TABLE lead_magnet_leads ADD COLUMN IF NOT EXISTS loops_sent BOOLEAN DEFAULT FALSE`,
      `ALTER TABLE lead_magnet_leads ADD COLUMN IF NOT EXISTS loops_sent_at TIMESTAMPTZ`,
      `ALTER TABLE lead_magnet_leads ADD COLUMN IF NOT EXISTS loops_contact_id TEXT`,
      `ALTER TABLE lead_magnet_leads ADD COLUMN IF NOT EXISTS loops_error TEXT`,
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
