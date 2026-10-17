# PR: synthetic checks fail closed (gushwork-api v5.24.0)

Branch `api/v5.24-synthetic-fail-closed`, from `main` at `c2220b1` (v5.23.0). 18 Oct 2026.
**Not merged, not deployed: waiting for your review.** No Railway variable,
Webflow page or gushwork-web pin was touched.

**The form scripts do not change, so nothing needs re-pinning.** All six form
files (`gushwork-form.js`, `gushwork-form-popup.js`, both `.min.js` and both
maps) are byte-identical to `main`, blob for blob. "v5.24.0" is the API's
release only; the form banners still read v5.23.0 / v5.23.0-ads.

**For a visitor (no header) nothing changes.** That is checked byte for byte
against v5.23.0 below. Who is blocked and which leads fire Meta: unchanged.

## What changed (index.js only)

### 1. Fail closed on the nine synthetic routes

| Request carries | v5.23.0 | v5.24.0 |
|---|---|---|
| no `X-GW-Synthetic` | a visitor | **a visitor, unchanged** |
| the right token | the synthetic check | **the synthetic check, unchanged** |
| a wrong token | **a visitor: a real lead, Salesforce, Slack, mirror, Meta** | **401**, nothing runs |
| an empty header (`""`) | a visitor | **401**, nothing runs |
| any header while `SYNTHETIC_TOKEN` is unset or under 32 characters | a visitor | **401**, nothing runs |

The 401 body is exactly `{"ok":false,"error":"synthetic token rejected","synthetic":false}`.

**Each of the nine routes answers a wrong or empty token the same way:**
`/session`, `/partial`, `/submit`, `/enrich`, `/verify-email`, `/non-icp-check`,
`/verify-website`, `/resolve-website` and `/booking-confirmed` all give 401 with
that body.
- The refusal comes before any handler, any rate limiter and any database call.
  So a refused request also spends nothing from the visitors' per-IP budgets,
  where in v5.23.0 it spent one each.
- `cors` has already run, so an allowed origin can read the 401.
- One log line, without the value: `refused (401): the token did not match
  SYNTHETIC_TOKEN`, or `refused (401): synthetic checks are off`.
- Routes outside the nine ignore the header, as before.

### 2. The monitor's own address: `SYNTHETIC_EMAILS`

A Railway variable, comma-separated, empty by default. Each entry is trimmed and
lower-cased. On five routes, a request for a listed address **without** a valid
synthetic token gets **403** `{"ok":false,"error":"monitor address outside a synthetic check"}`
before the route runs. Nothing is saved or sent. With a valid token it is the
ordinary synthetic check.

**Where the address arrives:**

| Route | Address | How it is checked |
|---|---|---|
| `/partial`, `/submit`, `/enrich`, `/verify-email` | `email` in the body | read exactly as each route reads it (trimmed, 254 characters, lower-cased) |
| `/booking-confirmed` | **not in the body** (the form sends `session_id`, `booking_uid`, `start_time`, `end_time`, `event_type`) | read off that session's lead row with one `SELECT lower(email)`, **only while the list is non-empty**, so with it unset a booking's database calls are exactly as before. An unreadable row fails open: it is not evidence the booking is the monitor's |
| `/session`, `/verify-website`, `/resolve-website` | none | not covered |
| `/non-icp-check` | `email` in the body | **not covered** (not in your list). It saves no lead; as a visitor it can warm the model (a scrape, a model call, a verdict-cache row) |

One addition beyond the spec: a monitor-address 403 is also left out of the
`/submit` counter, for the same reason as item 4 (it is not a visitor's
rejected submit). It carries no header, so the 403 marks `res.locals` and the
counter skips it.

### 3. The token before the budget

`/health/deep` and `/metrics/submissions` now check the token **before**
`syntheticOpsLimiter`, so a stranger cannot spend the monitor's 120 an hour.
- Unauthenticated calls get the same 401 body as before.
- They have their own budget: `SYNTHETIC_OPS_UNAUTH_PER_HOUR`, default 30 per
  client address.
- That budget is keyed on **`clientIpOf(req)`**, not `req.ip`. With
  `trust proxy` at 1, `req.ip` is a Railway proxy hop (CLAUDE.md), which would
  have put every caller in one bucket.

### 4. Counts

The `/submit` outcome counter now skips **any** request carrying the header,
matching or not. It used to skip only matching ones, so a refused check would
have shown up as a visitor's rejected submit.

## Set this

| Variable | Value |
|---|---|
| `SYNTHETIC_EMAILS` | the monitor's address(es), comma-separated. **Unset: the address guard is off** |
| `SYNTHETIC_OPS_UNAUTH_PER_HOUR` | optional, default 30 |
| `SYNTHETIC_TOKEN` | already set on 17 Oct. Unchanged |

**The monitor must send the right token from the first request after this
deploys**, or every form check it runs gets 401. That is the point: it fails
loudly instead of creating leads.

## Shipping it (agreed 18 Oct, done only after you approve the merge and deploy)

1. Merge (merge commit). Railway deploys gushwork-api.
2. **One variable change, so the service restarts once.** Read both current
   values first and show them before and after, never printing
   `SYNTHETIC_TOKEN`:
   - `SYNTHETIC_EMAILS` = `website-monitor@gushwork.ai`
   - `INTERNAL_TEST_EMAILS`: add `website-monitor@gushwork.ai` to what is
     there, comma-separated.
3. Confirm `/health` answers 200 after the restart.

`website-monitor@gushwork.ai` is **already** "ours" today without step 2's
second half, because `gushwork.ai` is in the hard-coded
`ELV_EXCLUDED_DOMAINS` (so `isInternalLead` matches it). The
`INTERNAL_TEST_EMAILS` entry is a second lock in case that domain list ever
changes. It costs nothing. The address has no mailbox, and nothing is sent to
it in synthetic mode.

## Verified (run, not just read)

- **Full bar, run bare after the last commit: 19 suites, 8,631 assertions, 0
  failed.** Before: 8,356. Only the two suites I changed moved:
  `test-synthetic` 234 → 376 and `test-server-differential` 117 → 250.
- **`test-server-differential` now locks against v5.23.0** (`c2220b1`). It
  boots both servers with identical stubs and sends every form route three ways:
  - **No header:** same status, headers, database statements and outbound
    requests.
  - **The right token:** same again, so the synthetic path is unchanged.
  - **A wrong or empty token** (the intended difference): v5.23.0 ran each as a
    visitor; v5.24.0 answers 401 with the refusal body, with the CORS header,
    and **its trace shows nothing at all ran: 0 statements, 0 requests, on all
    22 refused requests.**

  Body comparisons, counted in the last run:
  - 23 bodies were byte for byte. **Every right-token body was byte for byte:
    11 of 11.**
  - The other 4 differed only in generated values: `/verify-email`'s `ms`
    stopwatch, `/health`'s timestamp, and the stack-trace path on express's
    413 and CORS error pages.

  One header moves, and is pinned exactly: `ratelimit-remaining` on later
  visitor requests is higher by exactly the number of refused requests on the
  same per-IP budget, because v5.23.0 spent one per wrong-token request and
  v5.24.0 spends none.
- **`test-synthetic`, for each of the nine routes:**
  - A wrong token, an empty header and a 32-character prefix of the token each
    give 401 with the exact body.
  - **Nothing ran**, proved by the spies: no database statement (the stubbed
    pool), no outbound request (stubbed fetch), the pool guard never hit, and no
    choke point (`refuseIfSynthetic`) reached.
  - The CORS header is on the 401.
  - Exactly one log line, and the value is never logged.

  Also covered:
  - With `SYNTHETIC_TOKEN` unset or too short, any header gets 401 and the log
    says checks are off, while a request with no header is still a visitor.
  - `SYNTHETIC_EMAILS`: 403, with nothing saved or sent, on the four body
    routes and on `/booking-confirmed` (after exactly one read of the row).
    Case and spaces do not get past it, every listed address is covered, and
    with a valid token each route gives the synthetic answer.
  - Ops: twenty unauthenticated calls from different addresses leave the
    monitor's budget untouched. One client gets 429 after 10, another client
    behind the same proxy hop still gets 401, and the monitor is unaffected.
  - The counter: two refused header requests and the monitor-address 403s are
    not in `rejected`; the visitor's 400 is.
  - Routes in the earlier sections behave as in v5.23.0 with the right token
    and with no header.
- **8 mutations, all CAUGHT** by `measure.js --mutation`, each committed first
  and restored from a byte-exact backup:
  - Wrong token handled as a visitor again: 58 + 66 failures across 2 suites.
  - Empty header treated as no header: 2 suites.
  - Counter counting refused requests.
  - Monitor-address guard skipped.
  - Booking route never looking the address up.
  - Ops budget spent before the token check.
  - Monitor-address 403 counted as a rejection.
  - Strangers keyed on `req.ip`.

  **The last one SURVIVED the first run**: the test sent a one-entry
  X-Forwarded-For, where `req.ip` and `clientIpOf` agree. It now sends
  Railway's two-entry form (client, then proxy hop). Re-run: CAUGHT.
- **The form files are byte-identical to `main`.** Six blobs compared:
  `19f18e2`, `fd88b42`, `43cfd87`, `76ba624`, `09ad405`, `7135533`.
  The form differential still passes unchanged (2,013).

## NOT verified (never run)

- Production: nothing deployed, no variable read or changed.
- The real monitor sending the right token, or a real proxy chain. The
  two-entry X-Forwarded-For is modelled on CLAUDE.md's account of Railway, not
  read off production for this PR.
