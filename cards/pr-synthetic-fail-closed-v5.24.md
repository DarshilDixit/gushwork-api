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

FILLED IN AFTER THE MUTATION RUN.
