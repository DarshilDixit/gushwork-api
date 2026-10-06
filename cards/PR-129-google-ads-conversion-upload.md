# PR 129: Google Ads conversion upload, off and validate-only by default

Branch `feat/google-ads-conversion-upload`, from `main` after #128. Written
6 Oct 2026. **Not merged: waiting for your review. Nothing has been sent for
real.**

## What it does

It sends each booked Google Ads lead to Google Ads as a conversion:
- **Account:** 5442288209.
- **Conversion action:** 7825004775, "CRM - Qualified Demo Request".
- **When:** a sweep runs every 10 minutes and looks only at bookings at
  least 30 minutes old.
- **What each conversion carries:**
  - one click ID;
  - the booking time in New York time;
  - the product's value;
  - the person's email and phone, scrambled (hashed) the way Google
    requires.

**It uses Google's Data Manager API, not the Google Ads API.** The Google
Ads API stopped accepting new users of this upload on 15 June 2026, and we
never used it, so it would refuse us. Data Manager takes the same data and
needs no developer token.

## Nothing changes when this merges

- **Off** until `GADS_UPLOAD_ENABLED` is `true`.
- **Even when on, it only asks Google to check each conversion** ("validate
  only"). Google records nothing until `GADS_UPLOAD_VALIDATE_ONLY` is set to
  exactly `false`.
- **It sends nothing at all without `GADS_UPLOAD_CUTOVER`**, and refuses a
  cutover that doesn't include its time-zone offset.
- **Nothing here changes which leads are blocked or which Meta events
  fire.** It reads leads and writes only to its own new table,
  `gads_conversion_uploads`. No lead, Meta event, Salesforce record or
  dialer row is touched.

## Which leads it sends

It sends a Google Ads lead (the same Google rule the Dropoff tab uses) that
booked after the cutover and passes **every** check below. Each check is made
at upload time, and the first one a lead fails is recorded as its reason:

| Reason recorded | What it means |
|---|---|
| `internal` | Our own or test address, through `isInternalSubmission`, unchanged |
| `staging` | The staging site, any other `*.webflow.io` site, or `localhost` |
| `agency` | Email domain **or** website matches `flighted.co` or `uprawmedia.com`, subdomains and `www.` included |
| `disqualified` | Sells to consumers, or chose the waitlist |
| `non_icp` | Blocked, model-flagged, **or** a verdict read fresh at upload time says so |
| `website` | Fails `isWebsiteVerified`, exactly as Meta does |
| `free_email` | Only when `GADS_EXCLUDE_FREE_EMAIL` is `true`. Off by default |
| `no_click_id`, `too_old`, `click_after_booking` | No usable click ID |

`INTERNAL_TEST_EMAILS`, `ELV_EXCLUDED_DOMAINS` and `isInternalSubmission`
are **unchanged**. The agency and extra-host lists are separate settings that
only this upload reads.

**Other staging or preview sites:** I searched the repo and every lead ever
written. The only staging host is `gushwork.webflow.io`, which is already
excluded. Every lead came from `www.gushwork.ai` or there. So the
Google-only list (`*.webflow.io`, `localhost`, `127.0.0.1`) matches nothing
today. It's there for a future preview copy of the site.

## Dry run: last 90 days, read-only

`tools/gads-upload-dry-run.js`, run on 6 Oct 2026 against the real leads
table. It used a read-only connection and no network, and ignored the
cutover.

| | Free email included (default) | Free email excluded |
|---|---|---|
| Google Ads bookings considered | 196 | 196 |
| **Would send** | **155** | **134** |
| Skipped: non-ICP | 21 | 21 |
| Skipped: no click ID | 15 | 12 |
| Skipped: website | 3 | 3 |
| Skipped: free email | — | 24 |
| Waiting (click under 6 hours old) | 2 | 2 |
| Internal, staging, agency, disqualified | 0 | 0 |

- **The non-ICP 21:**
  - **9** are leads already flagged by the model.
  - **12** are caught only by the **fresh read at upload time**:
    - 3 brand-list matches;
    - 4 model verdicts of a type that blocks (3 insurance, 1 real estate);
    - 5 model verdicts of a type that only withholds Meta (2 home services,
      1 print, 1 restaurant, 1 spa).
  - None of those 12 was marked on the lead row.
- **Of the 155 it would send:**
  - **Click ID:** 130 gclid, 13 gbraid, 12 wbraid.
  - **Which field it came from:** 73 `previous_page`, 41 `page_url`,
    41 `landing_page`.
  - **Value:** 12,000 for 141, 15,000 for 13, 5,000 for 1.
  - **Phone:** 145 also carry a hashed phone.
- **11 click IDs were refused as cut short.** All 6 to 17 characters, all
  from bookings before 21 Aug, when the server cut URLs at 500 characters.
  A cut ID points at the wrong click, so they aren't sent.
- **Agency 0 here** only because no Flighted or Upraw booking in these 90
  days came in as a Google Ads lead.

## What I verified, and how

- **Tests: 15 suites, 5,201 assertions, all passing** (`measure.js --check`,
  read in full). The new suite `test-gads-upload.js` has 152:
  - **Google's rules:** the email and phone formats, and the New York offset
    across the November clock change (01:30 EDT and 01:30 EST).
  - **The click choice.**
  - **Every exclusion, using the real functions lifted out of `index.js`:**
    - agency by email domain and by website domain;
    - our own addresses, including `b@g.ai`, `test.com` and `example.com`;
    - the staging site and other `*.webflow.io` sites;
    - the free-email switch on and off;
    - the cutover;
    - the 30-minute hold.
  - **The real app booted with the upload on:** it skipped an Upraw lead as
    `agency`, claimed the clean lead **before** sending, and sent exactly one
    request. I read it back: validate-only, the right account and action, no
    login account, no consent, and a token with the `datamanager` scope.
  - **The dry-run tool**, run against a stub: it writes nothing and calls
    nobody.
- **Mutation testing: 28 of 28 caught.** I broke each guard on purpose and
  watched the suite fail. One first **crashed** instead of failing (turning
  the upload on by default). That was a weakness in the test; I fixed the
  test and re-measured.
- **Every SQL statement was executed against real Postgres.** That means the
  table definition from `db.js` and every read and write the uploader makes.
  The run used temporary tables on the AWS mirror, made-up leads and a
  stubbed Google, all inside one transaction that was **rolled back**: 21 of
  21 checks passed. It covered:
  - the primary key refusing a second claim;
  - a 503 becoming a retry;
  - a 404 bad action becoming permanent, with a critical alert;
  - a retry re-sending the **identical bytes**;
  - switching to real mode sending each lead exactly once;
  - a send that died halfway being picked up again.
  
  **That run found a real bug, now fixed:** the stored payload was JSONB,
  which reorders keys, so a retry sent the same event in different bytes. It
  is now stored as plain text.
- **Earlier today, before writing this code:** the service account signed in
  with the `datamanager` scope and reached 5442288209. A validate-only
  request with action 7825004775 was accepted (200), and the same request
  with a made-up action was refused (404). So a validate-only send does
  confirm the action exists.

## Not verified

- **Nothing has been sent for real, by this code or anything else.** Real
  mode has only run against a stubbed Google.
- **The code has never called Google.** The 200 and 404 above came from
  scratch scripts earlier today. With this code, validate-only mode will be
  the first real contact.
- **Whether real clicks match, and Google's 6-hour rules.** Validate-only
  can't show these. The code waits 6 hours after the click anyway.
- **The Data Manager error codes I classify** are inferred from the Google
  Ads API's list. Only `INVALID_CONVERSION_ACTION_ID` has been seen live.
- **After a real send there's no follow-up check yet.** Google can accept a
  request (200) and drop the event later. Its status lookup
  (`requestStatus:retrieve`) isn't built; the request ID is stored for it.

## Decisions I made that you might not want

1. **One click ID per event, as you asked.** Google's own guide *recommends*
   sending a gclid and a gbraid together when both exist. 77 of the 118
   bookings in the last 30 days carry both.
2. **Model-flagged leads are skipped whatever `NON_ICP_LLM_META` says.** The
   flag is read in code at upload time and never as a database filter, which
   keeps it inside the rule in `test-non-icp.js` section 10f.
3. **If the fresh non-ICP read fails, the upload waits for the next sweep
   rather than sending.** That's the opposite of the lead path: a late
   upload costs ten minutes, and a wrong one can't be taken back.
4. **A skip is final.** If a lead is skipped (say, an unverified website) and
   that later changes, it isn't re-sent unless its row is deleted. Leads
   still waiting (the hold, a click under 6 hours old, or a failed read) get
   no row until they're decided.
5. **Value:** 12,000 / 5,000 / 15,000 from the product, sent as the
   **conversion value**. Unlike Meta, which gets value 0 plus a separate
   predicted value, this number *is* the value Google's bidding sees. That
   matters if the account uses value-based bidding.
6. **No consent field**, as you asked. Google says conversions without it
   *may* not be attributed.

## Noticed, not changed

`/partial` stamps `meta_predicted_ltv` without the internal-submission
check, so **5 internal leads (2 since 19 Sept) show a Meta value Meta never
received**. This PR doesn't read that column. Fixing it is separate.

## To switch it on, in this order

1. Merge. Railway deploys and creates the table. Nothing sends.
2. In Railway, set `GADS_SERVICE_ACCOUNT_JSON_B64` (the key at
   `~/.config/gushwork/gads-sa.json`, base64-encoded), `GADS_UPLOAD_CUTOVER`
   (e.g. `2026-10-20T00:00:00-04:00`) and `GADS_UPLOAD_ENABLED=true`. **Each
   variable change redeploys.** It is still validate-only.
3. Read `gads_conversion_uploads`: rows should be `validated` or `skipped`
   with reasons that match the dry run.
4. Pause Lorenzo's sheet **before** the cutover passes. Confirm what Order ID
   it sent: unless it's our session ID, only the cutover prevents double
   counting.
5. Set `GADS_UPLOAD_VALIDATE_ONLY=false`. Rows marked `validated` are checked
   again and sent once.

I'll stop here until you say merge.
