# PR 128 — the origin check admits the API's own address, exactly

Branch `fix/cors-own-origin`, from `main` after #126. Written 27 Sept 2026.
**Not merged — waiting for your review.**

## What was wrong

The server's origin check (`cors` in `index.js`, there since March) admits
only three sites:
- `www.gushwork.ai`
- `gushwork.ai`
- `gushwork.webflow.io`

For anything else it **throws, which is a 500 before any route runs**.

Chrome sends an `Origin` header on every font request, and every browser
does on every POST, **even to the site the page came from**. Both dashboards are served *by the
API*, so their own requests were rejected:

- **The new dashboard's fonts have never loaded on production.** In three
  fresh Chrome profiles, both fonts answered 500 every time; without the
  `Origin` header, the same URLs answer 200. The dashboard fell back to
  the computer's own fonts. Every screenshot and layout check went through
  the local preview, which has no origin check. That's why nothing caught
  it, and why the screenshots show the right fonts.
- **The three write buttons, on both dashboards, cannot have worked since
  the check existed:** Lead magnet "delivered" and "retry", and Partners
  "acknowledge".

## Your two questions

**Did the classic's write buttons work before the switch? No.**
- They send exactly these requests from exactly this address, so they hit
  the same 500.
- The database agrees: **0** lead magnets have ever been marked delivered
  (of 10 completed), and **0** partner failures ever acknowledged.
- The logs can't say whether anyone *tried*. Railway keeps logs only for
  the live deploy and the one before it.

**Do they work now at `/monitor/classic`? No**, for the same reason. This PR
fixes both dashboards at once, because they call the same routes.

**Marking a lead magnet delivered before this lands.** Both of these work
today, because only browsers send `Origin`:
1. the delivery queue's own route: `POST /lm/queue/<id>/delivered`, with
   the queue token (`?token=` or the `x-lm-token` header);
2. the dashboard's route, from a terminal:
   `POST /monitor/lm-delivered/<id>?token=<MONITOR_TOKEN>&undo=0`.

The lead's id is in `GET /monitor/lm-leads?token=…`.

## The fix

- **`SELF_ORIGIN` is `https://` plus Railway's own `RAILWAY_PUBLIC_DOMAIN`**
  (checked: it is `gushwork-api-production.up.railway.app`).
- **Compared as a whole string.** No wildcard, no other subdomain, no
  `http`, no port.
- **`selfOriginFrom` refuses anything that isn't a bare hostname**, so a
  bad value in the environment can't widen it. Unset (a laptop, a test)
  admits nothing extra.
- **Foreign origins still get the 500, unchanged.** A page on another site
  still can't make any **POST** route run. This check was never cross-site
  protection for GETs, before or after: a link or an image sends no `Origin`,
  and that was always admitted.
- **If a custom domain is attached later,** Railway's variable may name it
  instead. Then the railway.app address is refused again (the safe
  direction), and the fix is the other exact name, never a pattern.

**Nothing here changes which leads are blocked or which Meta events fire.**
- The form's own requests come from `www.gushwork.ai` and are unaffected.
- Only requests whose `Origin` is the API itself change: from rejected to
  admitted.
- A browser only sends that from pages the API serves: the dashboards.

## After it deploys (I'll check before touching anything)

1. **Fonts:** load `/monitor` in a fresh Chrome and record each font's
   status. Both should be 200. Read-only.
2. **The three write buttons, with nothing written:** from the `/monitor`
   page in Chrome, so the browser adds its own `Origin` exactly as the
   buttons do, send each button's request with an id that doesn't exist:
   - `POST /monitor/lm-delivered/999999999?undo=0`: the update matches 0
     rows, so it should answer `{"ok":true,"updated":0}`;
   - `POST /monitor/lm-loops-retry/999999999`: it should answer its own 404
     "lead not found" **before** calling Loops;
   - `POST /monitor/partner-ack` with `customer_key: "origin-check.invalid"`:
     it should answer its own 404 "No failed row".

   Each route's own answer, rather than the plain-HTML 500, proves the
   request got through. None of them can change a row or call out.
3. The same three from `/monitor/classic`, to show the classic's buttons
   work too.

## Verified — actually run

- **The full bar, bare: 14 suites, 5,048 assertions, 0 failures.** Baseline
  saved.
- **New in `test-monitor-next` §2b (19), driven over HTTP with the header a
  browser adds:**
  - both fonts load from the API's own page;
  - all three write routes are reached (their own JSON, and the
    delivered/ack updates are sent);
  - "retry" never calls Loops for an unknown lead.
  - Eight foreign or near-miss origins are **rejected before any query**:
    `https://evil.test`, `http://` (the same host), a port, a subdomain, the
    host with `.evil.test` added, a look-alike prefix, a look-alike with a
    dash, and `null`.
  - The listed site and a request with no `Origin` still work as before.
  - `selfOriginFrom` was run on a wildcard, a scheme, a path, a port, a bare
    name, nothing, a double dot and a space.
- **Mutations: 6 of 6 caught.** "Widened to ends with" and "every origin
  admitted" were re-run after the test's write detector was narrowed, and are
  still caught. They were:
  - the rule removed;
  - widened to "ends with";
  - widened to "starts with";
  - the helper accepting anything;
  - the helper building `http`;
  - every origin admitted.

## The review

One independent reviewer, read-only. **Nothing real found.** Confirmed:
- nothing but the exact address gets through: other subdomains, `http`,
  ports, `null`, case, trailing dots, punycode, repeated headers and a bad
  environment value are all refused;
- no new cross-site path. Every write route authenticates only by the token
  in the query, with no cookies, so a page's own script could always replay
  a write without `Origin`, and form posts send `Origin: null`, which is
  still refused;
- the lead path is unaffected in practice;
- the tests fail without the fix, and the write detector is real.

Taken from its notes:
- the "can't make any route run" wording, now scoped to POST;
- the font claim, now "Chrome";
- the custom-domain case, now documented;
- the test's write detector, narrowed to the one table, so a background
  write can't make it flaky.

## Also in this PR: today's handoff

`docs/monitor-plan.md` records:
- #125, #127 and #126 merged, and what was checked after each deploy;
- your calls: no extra CSV guards, and the digest left to Monday;
- the origin finding, and the workaround above.

CLAUDE.md has a note on the rule, and on why only a real browser on
production could have caught this.

## Process

- Merged nothing. The branch is pushed and the PR is open. **Stopped.**
- **Two corrections to what I told you earlier today:**
  - "The logs only reach back to the latest deploy" came from a command
    that never ran (`timeout` isn't installed on this Mac).
  - "Nobody pressed the buttons 22–26 Sept" came from reading removed
    deploys, which return empty logs.

  Both are withdrawn. The database evidence above is what stands.
