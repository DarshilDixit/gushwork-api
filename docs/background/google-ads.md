# Background: The Google Ads conversion upload

Moved here from `CLAUDE.md` on 7 Oct 2026, word for word, to bring that file under its size limit. `CLAUDE.md` keeps the rule for each of these as one line with a link here; this file keeps the history, measurements and worked examples behind it. Dates and counts are as of when each paragraph was written, and "today" means that day. Line numbers in the headings are `CLAUDE.md` at commit b924ce7.

## The Google Ads conversion upload (was `CLAUDE.md` lines 2104-2160)

**THE GOOGLE ADS CONVERSION UPLOAD — 6 Oct 2026, and its traps.**
`google-ads-conversions.js` sends booked Google Ads leads to account
5442288209, action 7825004775 ("CRM - Qualified Demo Request").

- **Data Manager API, NOT the Google Ads API.** `UploadClickConversions`
  refuses new adopters since 15 June 2026
  (`CUSTOMER_NOT_ALLOWLISTED_FOR_THIS_FEATURE`), and this repo never adopted
  it. Data Manager needs **no developer token** and the scope
  `https://www.googleapis.com/auth/datamanager` — not `adwords`. Credential:
  `GADS_SERVICE_ACCOUNT_JSON_B64`, the key of
  `gads-conversion-uploader@gushwork-crm.iam.gserviceaccount.com`, which is a
  user DIRECTLY on 5442288209, so no login account is sent.
- **Off, then validate-only, by default.** `GADS_UPLOAD_ENABLED=true` turns
  the sweep on; it stays validate-only (Google checks, records nothing) until
  `GADS_UPLOAD_VALIDATE_ONLY=false`. **No `GADS_UPLOAD_CUTOVER` means nothing
  sends**, and a cutover without its offset is refused — Lorenzo's sheet
  covers everything before it, so the boundary must be exact.
- **A validate-only 200 DOES check the action exists** (a made-up ID answers
  404 `INVALID_CONVERSION_ACTION_ID`, which is permanent and alerts) but
  proves nothing about the click: matching and the 6-hour rules only happen
  on a real send.
- **It skips more than Meta does, on purpose, and the lists are Google-only.**
  Flighted and Upraw (`GADS_EXCLUDED_DOMAINS`) and any other `*.webflow.io`
  or loopback host (`GADS_EXCLUDED_HOSTS`) are NOT in `INTERNAL_TEST_EMAILS`
  or `ELV_EXCLUDED_DOMAINS`, because those also gate Meta, Salesforce and the
  dialer. Both env lists EXTEND their defaults and cannot shrink them.
- **`non_icp_llm_flagged` is read in CODE at upload time, never as a SQL
  filter** — section 10f of `test-non-icp.js` forbids a flagged lead being
  kept out of Salesforce, PartnerStack, the SDR list or the dialer. This is
  an ad signal, like Meta, and it skips flagged leads whatever
  `NON_ICP_LLM_META` says. The **fresh verdict read fails CLOSED** (waits a
  sweep) — the opposite of the lead path, because an upload can wait and a
  realtor sent to Google's bidder cannot be taken back.
- **Phone is E.164 WITH the `+`.** Meta's `normalizePhone` strips it; reusing
  it would hash a different string and match nobody, silently.
- **Built inside `start()`, never at the top level** — it reads
  `DROPOFF_SOURCE_SQL`, a `const`, which is the temporal-dead-zone break.
- **Value is the product lookup, not `meta_predicted_ltv`**, which is empty
  whenever Meta did not fire.
- **A 200 from `events:ingest` is NOT a conversion.** After every REAL send
  the sweep asks `GET /v1/requestStatus:retrieve?requestId=…` (from 30 minutes
  after, backing off to daily) and records the outcome per row. One event per
  request is what makes a request's status that event's result. Unmatched,
  duplicate, dropped and never-answered each alert once, as a warning; the
  write that finalises a row is conditional, so two sweeps cannot alert twice.
  Google's reference gives no timing for PROCESSING or for how long a request
  ID stays readable, hence the 7-day give-up rather than a guessed number.
  Validate-only requests cannot be asked about at all (Google's own 400).
- **Consent is a switch, `GADS_CONSENT_GRANTED`, off by default.** On, every
  event carries `consent: { adUserData: CONSENT_GRANTED, adPersonalization:
  CONSENT_GRANTED }` — the Data Manager reference's names, the same claim
  Lorenzo's sheet makes. Off, the field is ABSENT, never denied. It sits on
  the EVENT, so it is frozen with the payload: a row claimed before the
  switch is flipped retries without it, while a `validated` row is rebuilt
  when real sending starts and picks up the current setting. Validate-only
  accepted it on 6 Oct 2026 (200).
