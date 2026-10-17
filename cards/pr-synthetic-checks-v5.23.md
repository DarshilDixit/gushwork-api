# PR: synthetic website-monitor checks, one list of origins, /health/deep, /metrics/submissions (form v5.23.0)

Branch `forms/v5.23-synthetic-checks`, from `main` at `5db62fb` (v5.22.0). 17 Oct 2026.
**Not merged: waiting for your review.** No Webflow page, no gushwork-web pin and
no Railway variable was touched.

**For a real visitor nothing changes.** Every new path needs either a valid
`X-GW-Synthetic` token (server) or a `gw_synthetic` key in sessionStorage
(form), and no visitor has either. That is checked byte for byte below, on the
server as well as in the forms. Nothing about who is blocked or which leads fire
Meta changes for visitors. **One exception, loudly: a WRONG token is an ordinary
request** (your spec). A monitor configured with a stale token creates real
leads, real Slack posts and real Meta events. See "Set up the monitor safely".

## Set this

| Where | What |
|---|---|
| Railway, gushwork-api | `SYNTHETIC_TOKEN` = a random string of **at least 32 characters** (`openssl rand -hex 32`). Shorter, or unset: the feature is OFF and the header is ignored |
| optional | `SYNTHETIC_RATE_PER_HOUR` (default 60), `SYNTHETIC_OPS_RATE_PER_HOUR` (default 120), `DEEP_HEALTH_TIMEOUT_MS` (default 4000) |
| Railway, gushwork-api | `ALLOWED_ORIGIN` **is no longer read** and can be deleted. The origins are in `allowed-origins.js` |
| the monitor (gushwork-web) | the same token, sent as `X-GW-Synthetic` to `/health/deep` and `/metrics/submissions`, and put into the form page's `sessionStorage.gw_synthetic` before the form runs |

### Set up the monitor safely
- **Do not use `b@g.ai`.** The form treats it as a test address and never calls
  `/submit`, so the check would skip the step it exists to check.
- **Use one fixed address of its own, and add it to `INTERNAL_TEST_EMAILS`** on
  Railway. If the token is ever wrong, that address is then "ours": no Meta, no
  Salesforce, no dialer row. A Slack lead post and a lead row would still
  happen, marked as ours.
- To check the block path, use a brand-list domain (for example `kw.com`). A
  synthetic check never starts the model warm (below), so a model verdict is
  only seen if the domain was already judged.

## 1. One list of origins

- **Before:** `ALLOWED_ORIGIN` lived in the Railway env and v5.22.0 put a
  hand-kept copy in each form file.
- **Now:** `allowed-origins.js` is the only list (the same three origins the
  env held today).
  - `index.js` builds CORS from it, and says at boot if `ALLOWED_ORIGIN` is
    still set to something different.
  - `tools/build-form-min.js` writes its hostnames into the `SESSION_HOSTS`
    line of **both readable files**, then builds the minified ones.
  - **Edit the API's list only, then rebuild.** It says so beside both lists.
- **Tests:**
  - `test-synthetic.js` G checks every copy against the list (readable and
    minified, both files).
  - A **deliberate mismatch** fails in both directions: an origin added only to
    the API's list, and a host added only to a form file.
  - The build refuses a file with no hosts line.
  - The form differential fails on a stale minified file.

## 2. Synthetic mode (server)

`synthetic.js` holds the token check:
- Constant time, over the SHA-256 of both sides, so neither the token nor its
  length leaks.
- An AsyncLocalStorage flag that follows the request into its fire-and-forget
  work.

Only the nine routes the form calls honour the header. A matching request is:
- rate-limited on its own shared budget (429 with `synthetic: true`);
- marked `req.synthetic`;
- answered with `synthetic: true` added to every JSON answer;
- logged with every line prefixed `[synthetic]`.

**Two layers:**

1. **Explicit branches** in the routes that write. Each sits after the same
   parsing and decisions the real path makes:
   - `/session`: no `form_sessions` or page-view row.
   - `/enrich`: Apollo asked exactly as for a visitor, neither row written.
   - `/partial`: no lead row, no mirror row, no StartTrial, no IP lookup, no
     partner lookup. The form-traffic heartbeat is not fed by it.
   - `/submit`: see below.
   - `/booking-confirmed`: nothing recorded.

   `/verify-email`, `/non-icp-check`, `/verify-website` and `/resolve-website`
   have **no branch**: they run unchanged, and the helpers that would leave
   something behind step aside. Those are the ELV cache, verdict store and
   health window, the model warm, alerts, and the success and failure
   recorders.
2. **A net under them.** Each choke point opens with one guard line:
   - in `index.js`: `sendSlack`, `sendOpsSlack`, `sendAlertEmail`, `alertOps`,
     `recordFailure`, `syncToAWS`, `persistElvVerdict`, `warmNonIcpLlm`,
     `finaliseIpGeo`, `recordLeadFieldChanges`;
   - in the modules: Salesforce `pushToSalesforce` / `updateSFLead` /
     `updateOpportunityFields`, Meta `sendEvent`, PartnerStack `sendConversion`
     / `sendAction`;
   - both database pools.

   **A blocked write resolves empty rather than throwing**, because several
   writes are fire-and-forget without a catch, and a safety net that can crash
   the process is worse than the bug. It is loud and counted, and the tests
   require the count to stay zero.

**Known differences in a synthetic answer** (asserted):
- `/non-icp-check` answers `pending: false`, because no warm is started.
- **The paid lookups still run**:
  - ELV: about 1 credit per synthetic email check, never cached, so every run
    pays.
  - Apollo: 1 credit only if the person is FOUND, so use an address Apollo
    does not know.
  - The non-ICP model is NOT called.

### /submit, synthetic: the exact shape

The visitor's four keys, unchanged and computed the same way, plus two:

```json
{
  "ok": true,
  "non_icp_blocked": false,
  "non_icp_reason": null,
  "non_icp_industry": null,
  "would": {
    "team": "aeo",
    "product_interest": null,
    "slack": "lead_post",
    "salesforce": "push",
    "meta_lead": "fire",
    "model_flag_post": false,
    "partnerstack": "run",
    "meta_predicted_ltv": 12000
  },
  "synthetic": true
}
```

Possible values of each `would` field:

| Field | Values |
|---|---|
| `slack` | `lead_post` / `non_icp_blocked_post` / `skip:already_submitted` |
| `salesforce` | `push` / `skip:non_icp` / `skip:internal` / `skip:agency` / `skip:already_submitted` |
| `meta_lead` | `fire` / `suppress:non_icp` / `skip:internal` / `suppress:model_flagged` / `skip:website_not_verified` / `skip:agency` / `skip:already_submitted` |
| `partnerstack` | `run` / `skip:no_partner` / `skip:disqualified` / `skip:non_icp` / `skip:no_customer_key` / `skip:test_email` / `skip:already_submitted` |

`partnerstack: run` still has the once-per-domain claim ahead of it.

- **`team` is the routing slug the server stores (`crm` or `aeo`). The
  RevenueHero router id is not in it, because this service has never known
  it:** the form reads it from the page's markup. The form reports it (below).
- **Parity:** `test-synthetic.js` D drives the real `/submit` with six kinds of
  lead (normal, brand-list block, our address, unverified website, agency, free
  mail with no partner). It requires every "would" to match what the real path
  actually fired, so the two cannot drift silently.

Every other synthetic answer is the visitor's answer plus `"synthetic": true`,
for example `/session` → `{ "ok": true, "synthetic": true }`.

## 3. Form scripts (both files, readable and minified)

With a token in `sessionStorage.gw_synthetic`:
- Every one of the nine API calls carries `X-GW-Synthetic: <token>` (trimmed).
- `gw_form_submitted` / `gw_demo_booked` are not pushed.
- RevenueHero is not contacted. A stand-in keeps the step flow identical and
  records what would have happened in `window.gwSynthetic`:

```json
{
  "router_id": "6138",
  "revenuehero_fields": ["Email", "First Name", "last-name", "..."],
  "submit": { "...": "the /submit answer above" },
  "calendar": "would open"
}
```

The token is never logged, put in a URL or stored. Without the key, `apiHeaders`
returns the very same object and nothing else changes. There were no pixel
calls in either file to skip; Meta is server-side and already covered.

## 4. /health/deep and /metrics/submissions

Both need the token in `X-GW-Synthetic` (401 otherwise) and have their own
budget. `/health` is unchanged.

**`GET /health/deep`** → **200 when all ok, 503 otherwise**, so a status-only
check still alerts:

```json
{
  "ok": true,
  "checks": {
    "db":          { "ok": true, "ms": 4 },
    "salesforce":  { "ok": true, "ms": 210 },
    "revenuehero": { "ok": true, "ms": 80 },
    "enrichment":  { "ok": false, "ms": 95, "error": "Apollo says the key is not logged in" }
  }
}
```

| Check | What it does |
|---|---|
| `db` | `SELECT 1` |
| `salesforce` | the cached token, then `GET /services/data/v60.0/limits`, a real authenticated read that touches no record |
| `revenuehero` | **this service has no RevenueHero credentials**, so it checks the scheduler script every form page loads (`HEAD https://assets.revenuehero.io/scheduler.min.js`) |
| `enrichment` | Apollo's `GET /v1/auth/health`: no credit, and it proves the key is accepted, **not** that credits remain |

- Each check gets 4 s, and a check that does not answer in time FAILS.
- I made one real call to each of Apollo's health endpoint and RevenueHero's
  script on 17 Oct: `{"healthy":true,"is_logged_in":true}` and 200.

**`GET /metrics/submissions?hours=24`** (1 to 168):

```json
{
  "ok": true,
  "hours": 24,
  "timezone": "UTC",
  "failures_counted_since": "2026-10-17T16:00:00.000Z",
  "excludes": "our own test submissions; synthetic checks never write a row",
  "totals": { "submitted": 51, "booked": 42, "failed": 0, "rejected": 1 },
  "series": [
    { "hour": "2026-10-16T16:00:00Z", "submitted": 0, "booked": 0, "failed": null, "rejected": null }
  ]
}
```

| Field | Means |
|---|---|
| `submitted` | leads whose `submitted_at` falls in the hour. A repeat submit moves the session to its latest hour |
| `booked` | bookings, in the hour of `COALESCE(booked_at, created_at)` |
| `failed` / `rejected` | `/submit` answers of 5xx / 4xx, counted **in memory from boot**; `null` for hours before it |

- Our own submissions are left out of both database counts with `IS NOT TRUE`.
- Synthetic checks never write, so they are never counted.
- No personal data.
- **The SQL was run on production** in a read-only transaction, rolled back:
  0.9 s for 24 hours and 1.4 s for 168.

## Rate limit, in practice

A synthetic form run makes about **nine API calls**:
- `/session`, `/verify-email` and `/enrich`, one each;
- `/non-icp-check` three times;
- `/verify-website`, `/partial` and `/submit`, one each.

So the default 60 an hour is about **six full runs an hour**. Raise
`SYNTHETIC_RATE_PER_HOUR` if the monitor runs more often.

## Sizes

| File | v5.22.0 | v5.23.0 |
|---|---|---|
| `gushwork-form.min.js`, raw / gzip -9 / brotli 11 | 52,915 / 15,697 / 13,715 | 53,791 / 15,948 / 13,929 |
| `gushwork-form-popup.min.js`, same | 67,264 / 19,255 / 16,586 | 68,140 / 19,509 / 16,823 |

## Verified (run, not just read)

- **Full bar, run bare after the last code commit: 19 suites, 8,356
  assertions, 0 failed.**
  - Before: 17 suites, 7,846.
  - Two new suites: `test-synthetic` (234) and `test-server-differential`
    (117).
  - Existing suites grew where I added checks: `test-session-payload` +24,
    `test-form-differential` +132, `test-non-icp` +2, `test-batch-a` +1.
- **`test-server-differential` (new) boots v5.22.0's `index.js` out of git
  and this branch's, side by side, with identical stubs.** Requests sent to
  both:
  - every form route as a visitor;
  - every form route with a **wrong** token;
  - a missing session id, a 60 KB body, a foreign origin, the Webflow staging
    origin, and `/health`.

  Required, per request: the same status, body and response headers, and the
  same database statements and outbound requests. Results:
  - 18 answers were byte for byte.
  - The rest differed only in generated values: ELV's `ms` stopwatch,
    `/health`'s timestamp, and the stack-trace path on express's own error
    pages, because the release runs from a temp directory.
  - The only intended differences, pinned: the preflight now lists
    `X-GW-Synthetic`, and the two monitor endpoints answer 401 where the
    release had 404.
  - Stable over 8 runs.
- **`test-synthetic` (new) boots the app and drives all nine routes, as a
  visitor and as the monitor, with the same body:**
  - The visitor run must produce its side effects. For `/submit` that is the
    lead row, the Slack post, Salesforce, the Meta Lead event and the
    PartnerStack conversion.
  - The monitor's run must produce none: no database write (and the pool guard
    hit zero times), no outbound write except Apollo's lookup, and nothing a
    missed branch would have pushed into a choke point.
  - Every monitor request is attributed to it, every log line is tagged, the
    token is in no log, and the answer is the visitor's answer field for field.

  Also covered:
  - `/submit`'s "would" against what the real path actually fired, over six
    kinds of lead.
  - A wrong token, an empty one and a 32-character prefix are each an ordinary
    request.
  - The 429 when the budget runs out, with visitors unaffected.
  - `/health/deep`: refused without the token, 200 when all is well, 503 when
    a check fails or times out; no credential in the answer.
  - `/metrics/submissions`: refused without the token; a real 500 and a 400
    are counted, a synthetic 500 is not; no personal data; our own submissions
    left out of each count.
  - The origins list against every copy, and a deliberate mismatch both ways.
  - The guards on their own. Stable over 5 runs.
- **`test-form-differential` now locks against v5.22.0** (`5db62fb`): the
  source and the minified files, over every earlier case plus the preview
  hosts.
  - The synthetic cases cover `/demo` (no tick, CRM campaign, AI-CRM tick),
    `/ai-demo`, `/start`, `/lead-to-order`, a server block and a client
    block. Each requires:
    - the same API calls, in the same order, with the same bodies, each with
      only `X-GW-Synthetic` added;
    - no GTM event;
    - no RevenueHero call;
    - `window.gwSynthetic.router_id` equal to the router v5.22.0 handed
      RevenueHero (6138 or 6804);
    - a blocked lead still redirected exactly as by v5.22.0;
    - the token nowhere but the header.
  - `test-session-payload` drives the real `handleStep2Next` and booking
    listener the same way.
- **19 mutations, all CAUGHT** by `measure.js --mutation`, each committed
  first and restored from a byte-exact backup:
  - Each of the five synthetic branches disabled.
  - The token check accepting anything (caught by 2 suites).
  - The pool guard letting writes through.
  - The Salesforce and the Meta guard removed.
  - "Would" ignoring a block, and forgetting test addresses.
  - CORS no longer allowing the header (2 suites).
  - The monitor endpoints answering without a token (2 suites).
  - The metrics counting our own submissions.
  - `/health/deep` answering 200 on a failure.
  - GTM events pushed on a synthetic check (2 suites).
  - The header sent without the key: 179 failures in the differential.
  - The real RevenueHero used on a synthetic check (3 suites).
  - An origin added to the API's list without rebuilding the forms (2 suites).

  The last one first came out **UNMEASURED**, not caught: `test-synthetic` had
  one assertion per origin, so adding an origin moved its count. Fixed by making
  it one assertion over the list, then re-run: CAUGHT. The other 18 ran
  before that fix, which changed only that one check.
- **Production, read-only:** the `/metrics/submissions` SQL (EXPLAIN, then run,
  rolled back); Apollo's auth-health and RevenueHero's script (one call each);
  the Railway `ALLOWED_ORIGIN` value (the three origins).
- The three fire tools whose lifts I changed: `fire-alert` and
  `fire-non-icp-slack` load and print their usage. `fire-dropoff-digest`
  has no usage guard (run bare, it sends), so it was only syntax-checked.

## NOT verified (never run)

- **A real browser, the real monitor, or gushwork-web.** The forms are checked
  in the fake page only. Whether the Worker can set `sessionStorage` before the
  form script runs is the monitor's side.
- **A real synthetic request against production** (needs the env var and a
  deploy). The first one after deploy should be `/health/deep` with the token.
- ELV and Apollo pricing per synthetic run is from the code's own comments,
  not measured.
- Railway now also installs nothing new at runtime: `synthetic.js` and
  `allowed-origins.js` use only Node built-ins.

## Shipping it

1. Merge. Railway deploys the API. Before `SYNTHETIC_TOKEN` is set, synthetic
   mode is off.
2. Set `SYNTHETIC_TOKEN` (32+ characters) on gushwork-api. Optionally delete
   `ALLOWED_ORIGIN`.
3. Pin the merged commit's 40-character SHA: gushwork-web on the `.min.js`
   paths with new SRI hashes (I report them after the merge), Webflow per page.
   The banner should read `v5.23.0` / `v5.23.0-ads`. **Until the pin, the forms
   do not send the header**, so the monitor's form runs would be real leads.
   Hold the monitor's form checks until then; `/health/deep` and
   `/metrics/submissions` work as soon as the API deploys.
