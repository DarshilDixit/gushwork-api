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
      `ALTER TABLE leads ADD COLUMN IF NOT EXISTS ps_qualify_failed_at TIMESTAMPTZ`,
      `ALTER TABLE leads ADD COLUMN IF NOT EXISTS ps_qualify_fail_reason TEXT`,
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
