# PR 131: after a real send, ask Google what happened to each conversion

Branch `feat/gads-request-status`, from `main` after #130. Written 7 Oct
2026. **Not merged: waiting for your review. Nothing has been sent for
real, and no `GADS_` variable is set.**

## Why

When Google answers our upload with **200**, that only means it **accepted
the request**. Matching the booking to an ad click happens afterwards, and
the only way to find out how it went is to ask again later, with the request
ID Google gave us (`GET /v1/requestStatus:retrieve`). Without this, a
conversion that never matched a click looks exactly like one that counted.

## What it does

The same 10-minute sweep now also checks rows that were **really sent**:
- status `sent`;
- not validate-only;
- with a request ID;
- sent at least 30 minutes ago.

It records Google's answer on the row:

| Outcome recorded | What Google said | Alert? |
|---|---|---|
| `accepted` | SUCCESS, one record received | No |
| `accepted_with_warnings` | SUCCESS, but parts of the record were ignored | No (recorded) |
| `unmatched` | Couldn't tie it to an ad click (`CLICK_NOT_FOUND`, `INVALID_CLICK`, a click for another account, …) | **Yes** |
| `duplicate` | Already has this conversion (`DUPLICATE_TRANSACTION_ID` / `DUPLICATE_GCLID`) | **Yes** |
| `dropped` | Any other failure, or "success" with zero records received | **Yes** |
| `unknown` | No final answer after 7 days | **Yes** |

**If the answer isn't final yet** (still `PROCESSING`, unreadable, or an
HTTP error), it asks again later: after 30 minutes, then 1, 3, 6 and 12
hours, then daily.

**The 7-day limit is my choice, not Google's.** Google's reference doesn't
say how long processing takes, or how long a request ID can be looked up.
Rather than guess and treat silence as success, it gives up after 7 days and
says so in an alert.

**Alerts:**
- Each one is a Slack warning from "Google Ads", naming the session, the
  booking time, the click type, Google's reason, and what it means.
- **No email, phone or click ID** goes into an alert.
- **One alert per booking, ever.** The write that records the final outcome
  only succeeds if no outcome is recorded yet, and the alert fires only when
  that write succeeds. So two overlapping sweeps can't alert twice for the
  same booking.

**Validate-only rows are never asked about.** Google refuses to report on
those: *"the status can only be retrieved for requests that succeed and don't
have validate_only=true"*, which we saw from Google directly on 6 Oct.

## What I read in Google's docs (7 Oct)

- **The reply** lists one entry per destination, in the order sent. We send
  one destination and one event per request, so **a request's status is that
  event's result**.
- **`requestStatus`** is `PROCESSING`, `SUCCESS`, `FAILED` or
  `PARTIAL_SUCCESS`.
- **Errors and warnings** come as counts per reason, and are empty while
  processing.
- **`eventsIngestionStatus.recordCount`** is how many records Google
  received. Google's guide says to check it.
- **The 43 error reasons** include `CLICK_NOT_FOUND`, `INVALID_CLICK`,
  `TOO_RECENT_CLICK`, `EVENT_TOO_OLD`, `DUPLICATE_TRANSACTION_ID` and
  `DENIED_CONSENT`.
- **No timing is given anywhere.**

## Database

**10 new columns** on `gads_conversion_uploads`, all empty to start:
`google_status`, `google_outcome`, `google_record_count`, `google_errors`,
`google_warnings`, `google_status_attempts`, `google_status_checked_at`,
`google_status_next_check_at`, `google_status_final_at`,
`google_status_last_error`.

They're added with "add if missing" in the same non-fatal block as the
table, so a failure there can't stop the app booting.

## What I checked

- **Tests: all 15 suites pass, 5,219 → 5,262 assertions** (`measure.js
  --check`, read in full). The Google suite went from 170 to 213. The new
  tests cover:
  - every outcome;
  - "not final" cases staying open;
  - the URL-encoded request ID and the bearer token;
  - only real sends being asked about;
  - one alert each for unmatched, duplicate and dropped, and none for
    accepted, processing or an HTTP 500;
  - **no second alert when another sweep already recorded the outcome**;
  - giving up after 7 days;
  - credential failures reaching `recordFailure`.
- **Mutation testing: 14 of 14 caught.** One survived the first run because
  my test compared the 30-minute delay with the module's own constant, so
  changing one changed the other. I now assert 30 minutes as a number, and
  that mutation is caught.
- **Every SQL statement was executed against real Postgres.** That means the
  new `ALTER`, run twice as every boot will, and every read and write of the
  check. The run used temporary tables on the AWS mirror and a stubbed
  Google, all **rolled back: 22 of 22** checks passed. It covered:
  - each outcome landing in its row;
  - a validated row, a too-new row and a row with no request ID never being
    asked about;
  - exactly the expected alerts;
  - a second sweep asking nothing new;
  - a processing row settling later without a fresh alert;
  - a final outcome refusing to be overwritten.

## Not verified

- **No real request ID has ever been looked up.** We have none: nothing has
  been sent for real, and validate-only ones can't be asked about. The reply
  shapes come from Google's reference, not from a live answer.
- **How long Google takes to settle a request**, so whether 30 minutes is
  early or late for the first check. The backoff covers either.

## Things to know

- **The checks run whenever the upload is on**, validate-only or not. If you
  switch real sending off again later, conversions already sent still get
  their outcome.
- **Turning the whole upload off stops these checks too.** They live in the
  same sweep.
- **A `duplicate` alert is usually harmless**: our own retry after a
  timeout. If the booking was only ever sent once, another source (Lorenzo's
  sheet?) sent it too. The alert says both.

## Health after merging #130 (7 Oct, read-only)

- **`/health`:** 200.
- **`/monitor/health`:** 7 rows green. cron and nonicpllm show "not enough
  data yet", only because of the restart.
- **Boot log:** the table is ready and the upload is OFF.
- **No `GADS_` variables** are set.
- **The same pre-existing PartnerStack backfill warning appears.** Nothing
  new.

I'll stop here until you say merge.
