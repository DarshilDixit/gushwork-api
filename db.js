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
       FORM SESSIONS — top-of-funnel denominator

       One row per page load, written by POST /session. This is
       DELIBERATELY NOT the `leads` table. Most visitors never type an
       email, so writing them into `leads` would fill it with blank rows
       and silently break every query that already assumes "a row = a
       person who at least reached step 1" — the monitor metrics, the SDR
       list, the duplicate report, the recheck tool and the AWS mirror all
       make that assumption today.

       Kept separate, `leads` behaves exactly as it does now and the funnel
       is a JOIN on session_id when you want it.

       user_agent is stored but never filtered at write time: crawlers will
       hit the page, and deciding what counts as a bot is a read-time
       question we can change our minds about. Throwing the data away at
       write time is the one thing we could not undo.

       Wrapped in try/catch for the same reason as the lead-magnet block
       below it: start() exits the process if initDB throws, so an
       unwrapped failure here would take the live form down with it.
    ------------------------------------------------------- */
    try {
      await client.query(`
        CREATE TABLE IF NOT EXISTS form_sessions (
          id             SERIAL PRIMARY KEY,
          session_id     TEXT UNIQUE NOT NULL,
          page_url       TEXT,
          referrer       TEXT,
          utm_source     TEXT,
          utm_medium     TEXT,
          utm_campaign   TEXT,
          utm_content    TEXT,
          utm_term       TEXT,
          user_agent     TEXT,
          hits           INT DEFAULT 1,
          created_at     TIMESTAMPTZ DEFAULT NOW(),
          updated_at     TIMESTAMPTZ DEFAULT NOW()
        );

        CREATE INDEX IF NOT EXISTS form_sessions_created_idx ON form_sessions (created_at);
      `);
      console.log('[DB] Form-sessions table ready');
    } catch (err) {
      console.error('[DB] Form-sessions table init FAILED (non-fatal):', err.message);
    }

    /* One row per page load that reaches /session -- the DETAIL behind
       form_sessions.hits, which is only a counter. Reconstructing what a
       visitor actually did previously meant reading Railway deploy logs,
       which roll off with the deployment.

       SCOPE, and the name says it: gushwork-form.js is on the 16
       form-bearing pages ONLY, and it is the only thing that calls
       /session. Measured 9 Sep 2026 -- the homepage, /pricing and
       /who-its-for have ZERO rows here while 990, 263 and 76 leads
       respectively arrived at the form FROM them. So these are loads of
       form pages, not of the site. Non-form hops reach the database only
       as first-touch attribution on the lead row.

       NO REFERRER COLUMN, deliberately. The only referrer in the /session
       payload today is gw_referrer, which the site-wide Webflow script
       writes once per session -- first-touch, and therefore the same value
       on every hit. Storing it per page view would repeat one value down
       the whole column, which is worse than not having it: it reads as a
       per-hit fact and is not one. The form now sends page_referrer (the
       real per-hit value) so the column can be added here later without
       another Webflow re-pin.

       source is NOT NULL and says how the row was observed.
       'session_route' means a request actually reached this server at this
       timestamp. Anything client-reported must arrive under a different
       value rather than being blended into the same column. */
    try {
      await client.query(`
        CREATE TABLE IF NOT EXISTS form_page_views (
          id             BIGSERIAL PRIMARY KEY,
          session_id     TEXT NOT NULL,
          page_url       TEXT,
          hit_no         INT,
          source         TEXT NOT NULL,
          created_at     TIMESTAMPTZ DEFAULT NOW()
        );

        CREATE INDEX IF NOT EXISTS form_page_views_session_idx
          ON form_page_views (session_id, created_at);
        /* For pruning later. There is no retention job in v1 on purpose:
           16k rows in three weeks needs none, and any per-year projection
           is really a projection of Facebook ad spend on /start, which was
           78.7 percent of all sessions when this was written. */
        CREATE INDEX IF NOT EXISTS form_page_views_created_idx
          ON form_page_views (created_at);
      `);
      console.log('[DB] Form-page-views table ready');
    } catch (err) {
      console.error('[DB] Form-page-views table init FAILED (non-fatal):', err.message);
    }

    /* Append-only log of the identity fields changing on a lead row.

       Both upserts are last-write-wins: 35 columns are
       COALESCE(EXCLUDED.x, leads.x), and only hear_about_us_raw keeps the
       first value. So a visitor who reaches step 2, goes back and edits
       their email, and submits again silently replaces who the row is
       about. The old value is gone and nothing anywhere records that it
       existed -- which on 9 Sep 2026 meant a lead row reading one address
       while the Slack post, the Salesforce Lead and the Meta event had all
       gone out under a different one.

       SEVEN FIELDS ONLY: the ones that change WHO the lead is. The other
       28 last-write-wins columns are enrichment and attribution, where
       last-write-wins is the right behaviour and a change log would be
       noise.

       A CHANGE, NOT A FIRST SET. A row is written only when the old value
       and the new value are both non-null and differ. Recording the
       initial set of every field would roughly double the table and carry
       no signal -- nobody needs telling that a blank became a value.

       booking_uid_present is read from BEFORE the upsert, so it answers
       the question that actually matters: had this person already booked
       when they changed it? Somebody switching email after taking a
       calendar slot is a different event from somebody fixing a typo at
       step 1.

       A CHANGE THE PROSPECT MADE, not a change WE made. On 10 Sep 2026 a
       "changed after booking" alert fired for
       www.datapartnerinc.com -> https://www.datapartnerinc.com/ and the
       visitor had not touched the field: /partial stored Apollo's guess
       with the scheme stripped by our own code, and /submit stored the
       website check's resolved canonical URL with the scheme back on.
       Two of our own normalisations, in opposite directions, one round
       trip apart. So the diff now folds exactly what our own code varies
       (see normaliseIdentityValue in index.js) and every row carries an
       attribution saying whether it could have been the person at all.

       WHERE IT HAPPENED. step_reached is the row's high-water mark AFTER
       the upsert, which is the wrong number for this question: a step-1
       /partial arriving on a row already at step 2 records 2 and the
       out-of-step signal disappears. arrived_step is the step of the
       write itself, prev_step is where the row had got to before it, and
       field_step is where the field lives in the form. Website is a
       step-2 field that /partial can write from step 1 via enrichment,
       which is exactly the bug above, and those three columns are what
       makes that readable without opening the code. step_reached is left
       alone rather than redefined -- existing rows mean what they meant.

       NOT A TRIGGER, deliberately. A BEFORE UPDATE trigger would catch
       every write path including the booking routes and backfill-sf.js,
       and needs no application change at all -- but it puts behaviour
       somewhere a grep of this repo will never find it. Three separate
       times in one investigation the answer turned out to be in a place
       nobody could grep: the Webflow site-wide script, the Webflow script
       pin, and deploy logs that had rolled off. The CTE is more code and
       stays visible. */
    try {
      await client.query(`
        CREATE TABLE IF NOT EXISTS lead_field_changes (
          id                  BIGSERIAL PRIMARY KEY,
          session_id          TEXT NOT NULL,
          field               TEXT NOT NULL,
          old_value           TEXT,
          new_value           TEXT,
          source_route        TEXT NOT NULL,
          step_reached        INT,
          booking_uid_present BOOLEAN NOT NULL DEFAULT false,
          changed_at          TIMESTAMPTZ DEFAULT NOW()
        );

        CREATE INDEX IF NOT EXISTS lead_field_changes_session_idx
          ON lead_field_changes (session_id, changed_at);
        CREATE INDEX IF NOT EXISTS lead_field_changes_changed_idx
          ON lead_field_changes (changed_at);

        /* Added 10 Sep 2026. The table already exists in production, so
           these are ALTERs rather than table columns. Every one is
           nullable with no default: a row written before this deploy
           genuinely does not know its arrived_step, and NULL says that.
           Backfilling them from step_reached would put an inferred value
           in an observational column, which is the mistake
           docs/partnerstack.md records for first_ticked_at. */
        ALTER TABLE lead_field_changes
          ADD COLUMN IF NOT EXISTS attribution     TEXT,
          ADD COLUMN IF NOT EXISTS field_step      INT,
          ADD COLUMN IF NOT EXISTS arrived_step    INT,
          ADD COLUMN IF NOT EXISTS prev_step       INT,
          ADD COLUMN IF NOT EXISTS back_navigation BOOLEAN,
          ADD COLUMN IF NOT EXISTS hit_no          INT;
      `);
      console.log('[DB] Lead-field-changes table ready');
    } catch (err) {
      console.error('[DB] Lead-field-changes table init FAILED (non-fatal):', err.message);
    }

    /* -------------------------------------------------------
       EMAIL VERIFICATIONS — the ELV verdict, keyed by email

       /verify-email computed a rich verdict, returned it to the browser
       and threw it away. yo@yoyo.com is what that costs: ELV said
       'ok_for_all' (yoyo.com runs a catch-all through
       amazon-smtp.amazon.com and accepts any address), the website check
       timed out and failed open, and the lead looked clean because
       neither half of what we knew was written down anywhere.

       Keyed by EMAIL, not session_id, for two reasons that are not going
       to change:
         - /verify-email runs on email BLUR, before /partial has created
           any lead row. There is nothing to UPDATE yet, and inserting a
           bare row into `leads` would break the "a row means someone
           reached step 1" invariant that form_sessions exists to protect.
         - the blur prewarm verifies more than one address per session
           (type a typo, tab away, fix it, tab away). Session-keyed
           storage is last-write-wins and would sometimes keep the
           verdict for an address the lead abandoned.

       Only DEFINITIVE verdicts are ever written here — see
       persistElvVerdict() in index.js. A timeout is not a fact about
       someone's mailbox, so it is absent rather than recorded, and an
       empty column means "we deliberately did not know".

       Wrapped in try/catch for the same reason as the two blocks above:
       start() exits the process if initDB throws, so an unwrapped
       failure here would take the live form down with it. If this table
       is missing, /submit's fallback re-check still fills
       leads.elv_status — degraded, not broken.
    ------------------------------------------------------- */
    try {
      await client.query(`
        CREATE TABLE IF NOT EXISTS email_verifications (
          email       TEXT PRIMARY KEY,
          status      TEXT NOT NULL,
          valid       BOOLEAN,
          source      TEXT,          -- 'elv' | 'local' | 'submit_recheck'
          checked_at  TIMESTAMPTZ DEFAULT NOW()
        );

        CREATE INDEX IF NOT EXISTS email_verifications_checked_idx
          ON email_verifications (checked_at);
      `);
      console.log('[DB] Email-verifications table ready');
    } catch (err) {
      console.error('[DB] Email-verifications table init FAILED (non-fatal):', err.message);
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
      /* Website check — v5.8.0.
         These four were never declared here, yet index.js writes them in the
         /partial and /submit INSERTs and reads them in the monitor, the Meta
         gate and the recheck tool. They exist on the live database because
         they were added out of band, so nothing is broken today — but a
         database built from scratch (new environment, staging, a restore)
         would start up reporting "Tables ready" and then fail on the first
         form submission. ADD COLUMN IF NOT EXISTS is a no-op where they
         already exist, so this costs nothing and makes the schema
         reproducible.
         _prev and _rechecked_at are also self-created by /monitor/website-recheck;
         declaring them here means the recheck tool is no longer the only
         thing that knows they should exist. */
      `ALTER TABLE leads ADD COLUMN IF NOT EXISTS website_check_failed BOOLEAN DEFAULT FALSE`,
      `ALTER TABLE leads ADD COLUMN IF NOT EXISTS website_check_reason TEXT`,
      `ALTER TABLE leads ADD COLUMN IF NOT EXISTS website_check_reason_prev TEXT`,
      `ALTER TABLE leads ADD COLUMN IF NOT EXISTS website_rechecked_at TIMESTAMPTZ`,
      /* Email verification — batch 2.
         The ELV verdict copied onto the lead row from email_verifications,
         so the DB records not just that a lead passed but HOW. Read with
         website_check_reason it answers the question that matters: did
         anything actually verify this person? Declared here rather than
         self-created at write time — see the website_check note above for
         what that omission cost last time. */
      `ALTER TABLE leads ADD COLUMN IF NOT EXISTS elv_status TEXT`,
      `ALTER TABLE leads ADD COLUMN IF NOT EXISTS elv_checked_at TIMESTAMPTZ`,
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
      /* PartnerStack affiliate attribution.
         ps_xid and ps_partner_key are read from cookies set site-wide at click
         time and arrive with the form payload. The cookie carries the DECODED
         partner key (785ec78e1ee4688); the URL param is base64 of it, and we
         never store that form. The v2 partnerships API takes the decoded key
         directly.
         ps_customer_key is the normalised root domain and is the join key for
         everything downstream: PartnerStack counts one conversion per customer
         key FOREVER, so it has to be derived one way in one place. See
         partnerStackCustomerKey in index.js.
         ps_click_at is the winning click. The site-wide script restamps
         gw_ps_seen_at whenever the click id changes, so this timestamp always
         belongs to the click that won attribution, which is the anchor the
         90-day eligibility lookback measures back from.
         ps_click_history is every partner click this visitor made, oldest
         first, capped at 10. Reporting and dispute resolution only. Attribution
         reads ps_xid (last click) and nothing else. */
      `ALTER TABLE leads ADD COLUMN IF NOT EXISTS ps_xid TEXT`,
      `ALTER TABLE leads ADD COLUMN IF NOT EXISTS ps_partner_key TEXT`,
      `ALTER TABLE leads ADD COLUMN IF NOT EXISTS ps_partner_name TEXT`,
      `ALTER TABLE leads ADD COLUMN IF NOT EXISTS ps_partner_email TEXT`,
      `ALTER TABLE leads ADD COLUMN IF NOT EXISTS ps_customer_key TEXT`,
      `ALTER TABLE leads ADD COLUMN IF NOT EXISTS ps_click_at TIMESTAMPTZ`,
      `ALTER TABLE leads ADD COLUMN IF NOT EXISTS ps_click_history JSONB`,
      `ALTER TABLE leads ADD COLUMN IF NOT EXISTS ps_signup_sent_at TIMESTAMPTZ`,
      /* Proof the conversion actually created a customer, not just that
         PartnerStack answered 200 with an empty body. Null while unverified;
         the sweep in index.js fills it or releases the claim. */
      `ALTER TABLE leads ADD COLUMN IF NOT EXISTS ps_signup_verified_at TIMESTAMPTZ`,
      /* WHY a conversion did not fire, recorded at the moment we decide.
         Deliberately NOT ps_ineligible_reason: that means "the eligibility
         check rejected this", and the two will be confused the moment
         eligibility is switched on. A skip is usually correct behaviour
         (test address, disqualified); a FAILURE is money not being paid, and
         the dashboard has to tell them apart. */
      `ALTER TABLE leads ADD COLUMN IF NOT EXISTS ps_signup_skipped_reason TEXT`,
      `ALTER TABLE leads ADD COLUMN IF NOT EXISTS ps_signup_skipped_at TIMESTAMPTZ`,
      /* The two failure paths. Before these, today's 400 on the qualification
         wrote nothing anywhere: the claim released correctly and the dashboard
         showed a 0 that looked identical to "no demo has happened yet". */
      `ALTER TABLE leads ADD COLUMN IF NOT EXISTS ps_signup_failed_at TIMESTAMPTZ`,
      `ALTER TABLE leads ADD COLUMN IF NOT EXISTS ps_signup_fail_reason TEXT`,
      /* How many times the retry sweep has re-attempted this conversion.
         Bounded, because the failure reasons are not all transient: a 400 on a
         bad payload will fail identically forever, and retrying it every 15
         minutes would bury the rows that could still succeed. When the bound
         is reached the row stops retrying and stays RED for a human, which is
         the correct end state — the affiliate is still owed and the red chip
         plus the acknowledge flow is how someone picks it up. */
      `ALTER TABLE leads ADD COLUMN IF NOT EXISTS ps_signup_retry_count INTEGER DEFAULT 0`,
      `ALTER TABLE leads ADD COLUMN IF NOT EXISTS ps_signup_retry_at TIMESTAMPTZ`,
      `ALTER TABLE leads ADD COLUMN IF NOT EXISTS ps_qualify_failed_at TIMESTAMPTZ`,
      `ALTER TABLE leads ADD COLUMN IF NOT EXISTS ps_qualify_fail_reason TEXT`,
      /* Which product this lead came in for: the slug resolved from the form
         page, aeo (/demo) or crm (/ai-demo). Resolved server-side by
         resolveProduct in meta-capi.js — the SAME function that decides the
         Meta content_ids — so the column and the event can never disagree.
         Never the raw page and never a value a page author typed.

         NULL means the form page was not one we recognise. Rows predating
         this column were backfilled to aeo by hand, once: there was only one
         product then. Deliberately NOT backfilled here — a boot migration
         runs on every deploy, and after this column exists a NULL means
         "unrecognised page", which must not quietly become aeo. */
      `ALTER TABLE leads ADD COLUMN IF NOT EXISTS product TEXT`,
      /* Free text from the About-your-business textarea, added Sept 2026.
         Capped at 1000 chars at parse time — the same number as the
         maxlength on the textarea, so what the visitor can see on screen is
         what the column keeps. Storage only for now: not in Slack, not on
         the dashboard, not in the CSV export or SDR search. */
      `ALTER TABLE leads ADD COLUMN IF NOT EXISTS about_business TEXT`,
      /* What the visitor actually came in saying, before the partner overwrite.
         hear_about_us is a single column with three possible authors — the ad
         prefill, the visitor, and partnerHearAboutUs — and the last one wins,
         so the first two were being DESTROYED, not hidden. A partner-referred
         lead who arrived on a paid ad is two real facts and we kept one.
         Written once and never overwritten: the first non-empty value sticks. */
      `ALTER TABLE leads ADD COLUMN IF NOT EXISTS hear_about_us_raw TEXT`,
      /* Acknowledging a failure. NOT clearing it — the stamp and the reason
         stay, so the history is intact and the row keeps its state; this only
         removes it from the alert and the Needs attention count.
         test.com's phantom_200 was a real 200-with-no-customer, but the cause
         was a customer deleted in PartnerStack by hand, not a lost $50.
         Housekeeping and a genuinely missed payout produce the same stamp, and
         they should not demand the same attention. */
      `ALTER TABLE leads ADD COLUMN IF NOT EXISTS ps_failure_ack_at TIMESTAMPTZ`,
      `ALTER TABLE leads ADD COLUMN IF NOT EXISTS ps_failure_ack_note TEXT`,
      /* The lifecycle ladder groups by domain and filters on the failure
         stamps; both are read on every dashboard load. */
      /* The retry sweep's selector. Partial, so it covers only the handful of
         rows that are actually retryable rather than the whole table. */
      `CREATE INDEX IF NOT EXISTS leads_ps_signup_retryable_idx
         ON leads (ps_signup_failed_at)
         WHERE ps_signup_failed_at IS NOT NULL AND ps_signup_sent_at IS NULL`,
      `CREATE INDEX IF NOT EXISTS leads_ps_failed_idx
         ON leads (ps_customer_key)
         WHERE ps_signup_failed_at IS NOT NULL OR ps_qualify_failed_at IS NOT NULL`,
      `ALTER TABLE leads ADD COLUMN IF NOT EXISTS ps_qualified_sent_at TIMESTAMPTZ`,
      /* The eligibility verdict, stamped on the lead row.
         We are contractually required to tell an affiliate why a referral was
         rejected, and a console line does not survive that conversation three
         months later. Same habit as website_check_reason and elv_status: the
         verdict lives on the row that caused it. */
      `ALTER TABLE leads ADD COLUMN IF NOT EXISTS ps_eligible BOOLEAN`,
      `ALTER TABLE leads ADD COLUMN IF NOT EXISTS ps_ineligible_reason TEXT`,
      `ALTER TABLE leads ADD COLUMN IF NOT EXISTS ps_checked_at TIMESTAMPTZ`,
      /* ── Non-ICP block (V1, Sept 2026) ──────────────────────────────
         DELIBERATELY NOT leads.disqualified. That column means exactly one
         thing today -- the prospect told us they sell B2C or asked for the
         waitlist -- and five things read it: runPartnerStackSignup, the
         recovery cron, slackPartial, the SDR list and the stage ladder.
         Folding our verdict into it would make every historical DQ count
         two different things with no way to split them later.

         It is also CLEARABLE. 2,389 of 5,123 leads (47%) reached step 2 via
         the "actually we are B2B" button, which sets disqualified=false --
         and 74 of the 84 known realtor/insurance leads took exactly that
         path. A realtor must not be able to talk their way past a block, so
         the block lives in a column nothing in the form can clear.

         non_icp_reason holds the matched brand domain (e.g. 'kw.com'), not a
         category. It is what Slack prints and it is how a bad block gets
         spotted by a human. */
      /* When the conversion was last RE-checked against PartnerStack, as
         opposed to ps_signup_verified_at which is when it was FIRST seen to
         exist. Two different observations, so two columns: overloading the
         first would turn "we saw this land" into "we looked recently", and
         the next person would read a rolling timestamp as the landing time.
         Same reasoning as first_ticked_at vs sf_state. */
      `ALTER TABLE leads ADD COLUMN IF NOT EXISTS ps_signup_recheck_at TIMESTAMPTZ`,
      `ALTER TABLE leads ADD COLUMN IF NOT EXISTS non_icp_blocked BOOLEAN DEFAULT FALSE`,
      `ALTER TABLE leads ADD COLUMN IF NOT EXISTS non_icp_reason TEXT`,
      `CREATE INDEX IF NOT EXISTS leads_non_icp_blocked_idx ON leads (non_icp_blocked) WHERE non_icp_blocked IS TRUE`,
      /* One conversion per customer key, ever, is enforced by looking this up
         on every partner submit. Without the index that is a seq scan of leads
         on the critical path. */
      `CREATE INDEX IF NOT EXISTS leads_ps_customer_key_idx ON leads (ps_customer_key) WHERE ps_customer_key IS NOT NULL`,
      `CREATE INDEX IF NOT EXISTS leads_ps_xid_idx ON leads (ps_xid) WHERE ps_xid IS NOT NULL`,
      /* The eligibility check reads a 90-day window of leads to find prior
         contact on the same domain. created_at was unindexed. */
      `CREATE INDEX IF NOT EXISTS leads_created_at_idx ON leads (created_at)`,
      /* "One conversion per domain, EVER" enforced by the database rather than
         by a SELECT-then-send in application code, which races: two submits for
         the same domain arriving together both see no stamp and both fire, and
         the affiliate is credited twice for one customer. PartnerStack has no
         way to undo that.
         UNIQUE and PARTIAL: only stamped rows participate, so any number of
         rows may share a customer key while unsent, and at most one can ever
         carry ps_signup_sent_at. The claim in runPartnerStackSignup relies on
         this to turn a race into a unique violation it can treat as
         "already sent". */
      `CREATE UNIQUE INDEX IF NOT EXISTS leads_ps_signup_once_idx
         ON leads (ps_customer_key)
         WHERE ps_customer_key IS NOT NULL AND ps_signup_sent_at IS NOT NULL`,
      /* Same rule, same enforcement, for the qualification action: one per
         domain ever. Separate index because the two stamps are independent —
         a domain can have converted at signup and not yet qualified. */
      `CREATE UNIQUE INDEX IF NOT EXISTS leads_ps_qualified_once_idx
         ON leads (ps_customer_key)
         WHERE ps_customer_key IS NOT NULL AND ps_qualified_sent_at IS NOT NULL`,
      /* The read-back sweep looks for conversions sent but not yet verified. */
      `CREATE INDEX IF NOT EXISTS leads_ps_signup_unverified_idx
         ON leads (ps_signup_sent_at)
         WHERE ps_signup_sent_at IS NOT NULL AND ps_signup_verified_at IS NULL`,
      /* The partner-identity resolver looks up "have we already resolved this
         key?" on first sight of each new key. */
      `CREATE INDEX IF NOT EXISTS leads_ps_partner_key_resolved_idx
         ON leads (ps_partner_key)
         WHERE ps_partner_key IS NOT NULL AND ps_partner_name IS NOT NULL`,
    ];

    for (const sql of migrations) {
      await client.query(sql);
    }

    /* -------------------------------------------------------
       PARTNER DOMAIN — SALESFORCE STATE

       Per-DOMAIN, refreshed by the qualification poller for every partner
       domain rather than only the ones already eligible to qualify. The row
       worth acting on daily is "Opportunity exists, checkbox unticked" — an AE
       has not marked the demo, and until they do the $50 cannot fire. That row
       is invisible if you only look at domains that already passed every other
       filter.

       Its own table, not columns on `leads`: the state is per domain and
       `leads` is per lead, so columns there would be written N times and read
       inconsistently.

       Wrapped in try/catch like the other optional tables — initDB throwing
       exits the process, and a reporting table must never take the form down.
    ------------------------------------------------------- */
    try {
      await client.query(`
        CREATE TABLE IF NOT EXISTS partner_domain_sf_state (
          customer_key      TEXT PRIMARY KEY,
          sf_state          TEXT,          -- ticked | exists_unticked | create_errored | no_opportunity
          sf_opportunity_id TEXT,
          sf_error          TEXT,
          checked_at        TIMESTAMPTZ DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS partner_sf_state_idx ON partner_domain_sf_state (sf_state);

        /* ── SET ONCE, NEVER CLEARED ──────────────────────────────────
           sf_state is a SNAPSHOT of what Salesforce says right now, and it is
           the only thing in this integration that is not a record of an event.
           Everything else keys off an immutable stamp, which is why every
           other funnel stage is monotonic and this one was not: an AE unticked
           Qualified_Demo__c on hello.com on 7 Sept 2026 and the funnel's
           "Qualified Demo ticked" went DOWN to 1 while "The $50 fired" stayed
           at 2. One stage cannot go backwards while the rest cannot.

           These two make "was this ever ticked" and "did an Opportunity ever
           exist" answerable as facts. They also make the untick itself
           observable rather than something you infer from a number moving.

           NOT backfilled from leads.ps_qualified_sent_at, deliberately: these
           columns mean "we OBSERVED this", and writing an inferred timestamp
           into an observational column is how a reconstruction ends up being
           read as a measurement. The funnel ORs the two sources instead — see
           PS_FUNNEL_STAGE_SQL, where ps_qualified_sent_at is the stronger and
           older evidence for a domain that was ticked before these columns
           existed. */
        ALTER TABLE partner_domain_sf_state ADD COLUMN IF NOT EXISTS first_ticked_at      TIMESTAMPTZ;
        ALTER TABLE partner_domain_sf_state ADD COLUMN IF NOT EXISTS first_opportunity_at TIMESTAMPTZ;

        /* What we last wrote into Opportunity.Partner_Source__c, and when.
           Not decoration — it is the idempotence key. Without it the refresh
           would PATCH every partner Opportunity every 15 minutes, which is
           2,880 pointless Salesforce writes a day at 30 domains and grows
           linearly. The write only fires when the value has changed or was
           never written. */
        ALTER TABLE partner_domain_sf_state ADD COLUMN IF NOT EXISTS sf_partner_source    TEXT;
        ALTER TABLE partner_domain_sf_state ADD COLUMN IF NOT EXISTS sf_partner_source_at TIMESTAMPTZ;

        /* Same-table, idempotent, and only so a currently-true state is
           stamped now rather than on the next successful refresh — which is
           15 minutes away and can fail. Guarded on IS NULL, so it can never
           overwrite an earlier observation and can never re-fire. */
        UPDATE partner_domain_sf_state
           SET first_ticked_at = COALESCE(checked_at, NOW())
         WHERE first_ticked_at IS NULL AND sf_state = 'ticked';
        UPDATE partner_domain_sf_state
           SET first_opportunity_at = COALESCE(checked_at, NOW())
         WHERE first_opportunity_at IS NULL AND sf_state IN ('exists_unticked', 'ticked');
      `);
      console.log('[DB] Partner SF-state table ready');
    } catch (err) {
      console.error('[DB] Partner SF-state table init FAILED (non-fatal):', err.message);
    }

    /* -------------------------------------------------------
       ONE-OFF BACKFILL — the four partner leads that predate the
       skip/failure columns (batch A, 4 Sept 2026).

       Without this, two rows render in the WRONG lifecycle state on day one:
       test.com falls to conversion_pending (grey) when its conversion really
       was a phantom 200 that the read-back sweep caught and released — a red
       state showing grey is precisely the bug this batch exists to fix. And
       gushwork.ai reads conversion_pending when it was correctly skipped as a
       test address. Neither self-corrects: nobody is going to submit from
       test.com again.

       IDEMPOTENT by construction, not by a migration ledger. Each statement
       only touches rows where the target column is still NULL and no send has
       since succeeded, and only rows created before the cutoff. Once set it
       cannot re-fire, and a genuine later lead from either domain is outside
       the cutoff and untouched.

       Timestamps are the honest ones available rather than NOW():
         - test.com uses updated_at, which IS the moment the sweep released
           the claim, because that release was the row's last write.
         - gushwork.ai uses created_at, because the skip happened at submit
           and we have nothing more precise. Approximate, and said so.

       Wrapped in its own try/catch: initDB throwing exits the process, and a
       cosmetic backfill must never be able to take the service down.
    ------------------------------------------------------- */
    try {
      const CUTOFF = `TIMESTAMPTZ '2026-09-04 13:00:00+00'`;
      const phantom = await client.query(`
        UPDATE leads
           SET ps_signup_failed_at   = updated_at,
               ps_signup_fail_reason = 'phantom_200'
         WHERE ps_customer_key = 'test.com'
           AND ps_xid IS NOT NULL
           AND ps_signup_failed_at IS NULL
           AND ps_signup_sent_at IS NULL
           AND created_at < ${CUTOFF}`);
      const skipped = await client.query(`
        UPDATE leads
           SET ps_signup_skipped_reason = 'test_email',
               ps_signup_skipped_at     = created_at
         WHERE ps_customer_key = 'gushwork.ai'
           AND ps_xid IS NOT NULL
           AND ps_signup_skipped_reason IS NULL
           AND ps_signup_sent_at IS NULL
           AND created_at < ${CUTOFF}`);
      /* The one row whose ps_click_history carries the BASE64 partner key.
         The gw_ps_clicks cookie changed shape mid-morning on 4 Sept, so this
         row stores Nzg1ZWM3OGUxZWU0Njg4 where every later row stores
         785ec78e1ee4688 — same partner, two strings in one JSONB column.

         Rewritten by decoding each entry's pk, and ONLY where the decode
         round-trips exactly, so an already-normalised row cannot be mangled.
         The WHERE clause makes it idempotent: once no entry looks like base64
         the row stops matching. */
      /* NESTED CASE, not a flat AND chain. Postgres does not guarantee the
         regex and length checks run before decode() in an AND, so a 15-char
         key like 785ec78e1ee4688 reaches decode and raises
         "invalid base64 end sequence" — which would have failed at boot. Same
         evaluation-order trap as the start_time::timestamptz cast in the
         ladder. The outer CASE gates the decode; only values that already look
         like base64 ever reach it. */
      const clicks = await client.query(`
        UPDATE leads
           SET ps_click_history = (
                 SELECT jsonb_agg(
                          CASE WHEN e->>'pk' ~ '^[A-Za-z0-9+/]+={0,2}$'
                                AND length(e->>'pk') % 4 = 0
                               THEN CASE WHEN encode(decode(e->>'pk','base64'),'base64') = e->>'pk'
                                          AND convert_from(decode(e->>'pk','base64'),'UTF8') ~ '^[A-Za-z0-9._-]{6,120}$'
                                         THEN jsonb_set(e, '{pk}', to_jsonb(convert_from(decode(e->>'pk','base64'),'UTF8')))
                                         ELSE e END
                               ELSE e END
                          ORDER BY ord)
                   FROM jsonb_array_elements(ps_click_history) WITH ORDINALITY AS t(e, ord))
         WHERE ps_click_history IS NOT NULL
           AND jsonb_typeof(ps_click_history) = 'array'
           AND EXISTS (
                 SELECT 1 FROM jsonb_array_elements(ps_click_history) AS e
                  WHERE CASE WHEN e->>'pk' ~ '^[A-Za-z0-9+/]+={0,2}$'
                              AND length(e->>'pk') % 4 = 0
                             THEN encode(decode(e->>'pk','base64'),'base64') = e->>'pk'
                              AND convert_from(decode(e->>'pk','base64'),'UTF8') ~ '^[A-Za-z0-9._-]{6,120}$'
                             ELSE false END)`);
      /* One row is recoverable. The partner overwrite destroyed hear_about_us
         on all four test leads, and it is gone from Railway, the AWS mirror and
         form_sessions (which has no such column). test.com is the exception:
         its Salesforce Lead still reads "Testing RevenueHero" because that
         record was written before the partner logic shipped.

         Guarded to that one address and only while the column is empty, so it
         cannot fire twice or touch a real lead. */
      const rawBf = await client.query(`
        UPDATE leads
           SET hear_about_us_raw = 'Testing RevenueHero'
         WHERE LOWER(email) = 'this.is.darshil@gmail.com'
           AND ps_xid IS NOT NULL
           AND hear_about_us_raw IS NULL
           AND created_at < ${CUTOFF}`);
      if (rawBf.rowCount) console.log(`[DB] PartnerStack backfill: ${rawBf.rowCount} hear_about_us_raw recovered from Salesforce`);
      if (phantom.rowCount || skipped.rowCount || clicks.rowCount) {
        console.log(`[DB] PartnerStack backfill: ${phantom.rowCount} phantom, ${skipped.rowCount} skipped, ${clicks.rowCount} click-history normalised`);
      }
    } catch (err) {
      console.warn('[DB] PartnerStack backfill failed (non-fatal):', err.message);
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
