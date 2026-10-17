# PR: /session only from hosts the API accepts, and minified form files (form v5.22.0)

Branch `forms/v5.22-session-origin-and-min`, from `main` at `57f45b5` (v5.21.0). 17 Oct 2026.
**Not merged: waiting for your review.** No Webflow page and no gushwork-web pin
was touched. Nothing about who is blocked, which leads fire Meta, routing, the
B2C rule, any step, field, validation, dataLayer event or redirect changes.

**Why:** a Lighthouse audit of the form pages asked for two things: (1) no
console error from `/session` on preview origins, (2) a minified build.

## What changed

**1. `/session` is only sent from the hosts the API accepts.** Both files,
identical block, in front of `saveSession`'s fetch:

```js
const SESSION_HOSTS = ['www.gushwork.ai', 'gushwork.ai', 'gushwork.webflow.io'];
```

Any other host (a workers.dev preview, localhost, a lookalike) sends nothing and
logs one plain `console.log`: `[GW] /session not sent: <host> is not a host the
API accepts.` Exact names, no pattern.

**A decision for you: `gushwork.webflow.io` is on the list, and your brief
named only the two gushwork.ai hosts.** The Railway `ALLOWED_ORIGIN` is exactly
`https://www.gushwork.ai,https://gushwork.ai,https://gushwork.webflow.io`
(read 17 Oct), so `/session` works on Webflow staging today and its page loads
land in `form_sessions`. Leaving staging out would have stopped those rows, so I
kept it, so that this PR changes nothing the server receives. If you want staging
to stop recording sessions, delete that one entry from both files and rebuild.

The list now has to match `ALLOWED_ORIGIN` by hand. If an origin is ever added
there, it has to be added here too, or that site records no page loads. A
comment beside the constant says so. Nothing tests the two against each other,
because the env is not in the repo.

**2. Minified files, at NEW paths beside the sources:**
- `gushwork-form.min.js`, `gushwork-form-popup.min.js`, each with a `.map`
  pointing back to the readable file at the same commit (no source text inside).
- Built by `tools/build-form-min.js` with **terser pinned to 5.51.2** (a new
  exact devDependency; `package-lock.json` gains terser and its 10 dependencies,
  all `dev`). Same input and same terser give the same bytes.
- **Function names are kept** (`keep_fnames`). That costs about 850 bytes brotli per file,
  and in return stack traces still name real functions and the suite compares
  source and min with nothing carved out.
- **Committed, not left to jsDelivr.** jsDelivr minifies `x.min.js` on the fly
  when no such file exists; those would be jsDelivr's bytes and an SRI hash over
  them could break by itself.
- The readable files keep their paths and stay the source. Nothing pinned to
  them changes.

Plus the five-edit version bump, the Ads header's `Tracks /demo v5.22.0`, both
changelogs with the sizes, and `CLAUDE.md` (Layout rows and the rebuild rule).

## Sizes (measured)

| File | v5.21.0 source raw / gzip -9 / br 11 | v5.22.0 .min.js raw / gzip -9 / br 11 | Saved (br) |
|---|---|---|---|
| gushwork-form.js | 172,370 / 53,792 / 44,060 | 52,915 / 15,697 / 13,715 | 30,345 |
| gushwork-form-popup.js | 201,049 / 61,323 / 50,204 | 67,264 / 19,255 / 16,586 | 33,618 |

The popup's 33.6 KB is close to Lighthouse's "~36 KB" (Lighthouse measures
what was sent, which depends on the CDN's compression). **The ~26 KB of unused
code is NOT addressed**: the code is not split, so a page still downloads all of it.

## Verified (run, not just read)

- **Full bar, run bare, after the last code commit: 17 suites, 7,846
  assertions, 0 failed.** Baseline moved intentionally: session-payload
  173 → 195, form-differential 756 → 1,881. Nothing else moved.
- **The differential suite now has a second lock, against v5.21.0 (`57f45b5`).**
  The working-tree source AND each `.min.js` are run whole against v5.21.0 at
  the same latency over every existing case (all form paths, router ids 6138 and
  6804, the exact dataLayer objects with `user_data.email`, `b@g.ai`, the phone
  field and E.164, the `/thank-you` redirects, the head-pinned /ai-demo at three
  page-ready times, a Webflow page with its own jQuery), plus `gushwork.ai` and
  `gushwork.webflow.io`. Required: identical recording, timestamps included, and
  identical scripts requested, page after load, final page, storage, cookies and
  dataLayer. Only the version inside the init banner is normalised.
- **On a preview origin** (workers.dev, localhost; /demo, /ai-demo, /start, both
  files, source and min): the recording must be v5.21.0's with exactly one swap,
  its abort timer plus `/session` POST replaced by the log line, and no API
  request and no console error or warning during page load. The fake network
  refuses every API call from those hosts, as CORS does, so the rest of the
  visit runs the same for both versions.
- **Stale-build check:** the suite rebuilds both `.min.js` and maps with the
  same tool and requires byte equality with what is committed.
- **`test-session-payload.js`** lifts `SESSION_HOSTS` (it lifts `saveSession`
  and crashed without it, which is how it surfaced) and drives `saveSession`
  with 3 accepted hosts (post) and 8 others (post nothing): preview, localhost,
  127.0.0.1, `gushwork.ai.evil.com`, `evilgushwork.ai`, `staging.gushwork.ai`,
  uppercase with a trailing dot, empty.
- **8 mutations, each CAUGHT** by `measure.js --mutation` (all 17 suites
  completed at their usual counts), committed first and restored from a
  byte-exact backup:
  - Source changes, minified files rebuilt to match:
    - `gushwork.ai` dropped from the list: caught by session-payload and differential.
    - The host check disabled: caught by both.
    - The host match turned into a substring match: caught by both.
    - The log line reworded: caught by differential.
  - Source changed, minified files NOT rebuilt (`webflow.io` dropped): caught by both suites, including the stale-build check.
  - Minified files edited by hand:
    - router 6138 → 6139 in `gushwork-form.min.js`: 30 failures, including the identity and absolute router checks, not only the stale check.
    - `gushwork.webflow.io` renamed in the popup's `.min.js`: caught.
    - `user_data` renamed in the popup's `.min.js`: 80 failures.
- **jsDelivr, at the pushed branch SHA `363af03`:** all four JS files and a map
  answered 200 and are **byte-identical** to the commit, so jsDelivr serves our
  minified file and does not minify it again.

**One gap the mutations showed:** `test-ads-parity.js` did not notice a change to
`SESSION_HOSTS` in one file only. That region is not in its comparison. The
two executing suites catch it per file, so I left parity alone.

## NOT verified (never run)

- **A real browser or a real Lighthouse run.** The browser's own CORS console
  error cannot be shown by the fake page. The suite proves the request is no
  longer made, and a request that is not made is not refused. Re-run
  Lighthouse on a preview after the pin to confirm the Best Practices score.
- The `.min.js` in a real browser on a real page. It is checked against the
  fake page only, the same limit as v5.21.0's card.
- gushwork-web itself: its previews and its pinning code were not opened.
- Railway now installs terser as a dev dependency on deploy (unless it prunes
  dev deps). The server never loads it.

## Shipping it

After a merge, the new `main` SHA (40 characters) goes in the pins. **Pick the
paths:**
- gushwork-web: `…@<sha>/gushwork-form.min.js` / `…/gushwork-form-popup.min.js`.
  SRI (sha384 of these bytes, stays valid whatever the SHA is, as long as the bytes don't change):
  - `gushwork-form.min.js`: `sha384-ECVAzPay4KjMEvxgTIDXXkj3JuWp+mMD01ckUqjtpxOCeuFs+8YA2R78pjG4YRLN`
  - `gushwork-form-popup.min.js`: `sha384-Gj9EE7DLw8K0rwUhRHPhtBa3CpolYyQAuT/keQz7sRVSyIyfsj6mkU0HE2Zc0doB`
  
  Recompute from the merged commit before using them.
- Webflow pages: the same `.min.js` paths, or keep the readable ones. Either
  serves v5.22.0. The banner should then read `v5.22.0` / `v5.22.0-ads`.
