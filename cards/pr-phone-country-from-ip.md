# PR 77 — The phone field takes its country from the visitor's IP

Branch `fix/phone-country-from-ip`, off `main`, independent of PR 76.
Two commits. **Not merged.**

---

## The question asked

*"For IP finding and phone number field rendering on form what api are we using?
country.is? Can you check and verify if it is working correctly? I remember me
being in India it loads American flag by default."*

## The answer

**`country.is`, and it is working.** Checked live on 16 Sept 2026:

```
$ curl https://api.country.is
{"ip":"…","country":"IN"}          HTTP 200 in 0.301s
access-control-allow-origin: *
```

Correct country, fast, and CORS-open. I also confirmed the live pages are
running this code rather than something stale: every page pins
`gushwork-api@96360cc…`, and that file is **byte-identical** to `main`'s
`gushwork-form.js`. So the lookup you were seeing fail was not a stale deploy
and not a broken API.

**The wiring was wrong.** `initialCountry` was hardcoded `'us'` and the flag was
corrected *afterwards* by `setCountry`. That is two separate defects:

1. **A guaranteed wrong-flag window, not a flash.** jQuery, `intlTelInput` and
   `utils.js` load lazily one after another, and only *then* does the lookup go
   out. A visitor in India saw a US flag for as long as all of that took. If the
   lookup then failed, the flag stayed US and their number was stored as `+1…`.

2. **A silent override.** `setCountry` fired unconditionally. A visitor who
   opened the dropdown and picked their own country before the lookup landed had
   that choice overwritten underneath them, with no event and no warning.

So the flag was never actually driven by the IP in the way the code read as
doing — it was driven by a hardcoded default that an async call sometimes
corrected.

## The ideal, which is what this does

`initialCountry: 'auto'` with `geoIpLookup` is intl-tel-input's own answer to
both problems: it **waits** for the callback rather than asserting a country
nobody checked, and nothing overrides the visitor afterwards.

| | Before | After |
|---|---|---|
| Initial flag | `us`, always | deferred until the lookup answers |
| Visitor in India | US, then maybe IN | IN |
| Lookup fails | stays US silently | falls back to US, uncached |
| Visitor picks a country first | silently overwritten | respected |
| Second page load in a session | full round trip again | `sessionStorage` hit |
| Lookup hangs | — | 2.5s timeout still produces a flag |

Two details that are load-bearing:

- **The timeout is not belt-and-braces.** With `initialCountry: 'auto'`, a
  callback that never arrives leaves the field with **no country at all**,
  which is worse than the wrong one. The timeout guarantees it fires.
- **A failure is deliberately NOT cached.** Caching the `us` fallback would pin
  the rest of the session to the wrong country after a single blip. Only a real
  answer is written.

Applied to **both** form files identically — the fork has drifted before, and
`test-ads-parity.js` passes.

---

## Verified

**Bar, run bare:** 12 suites, **3407 assertions, 0 failures**.

**The lookup is EXECUTED, not read.** `tests/test-session-payload.js` lifts the
real `lookupCountry` (and its real constants) out of **each** form file and
drives it against a stubbed `fetch`, `sessionStorage` and timer:

- an Indian IP resolves to `in`, lowercased, and is cached
- a cached country answers immediately and makes **no** second round trip
- a rejected fetch falls back to `us` and is **not** cached
- a 200 carrying no country falls back and is **not** cached
- a 503 falls back rather than resolving to nothing
- a hung request still produces a flag once the timeout fires
- a `sessionStorage` that throws does not break the lookup
- a late timeout **cannot** drag an already-resolved visitor back to the US

### Mutation testing — 5 mutations

| # | Mutation | Result |
|---|---|---|
| 1 | Back to a hardcoded `initialCountry: 'us'` | CAUGHT |
| 2 | A failed lookup gets cached, pinning the session to US | **survived → fixed → CAUGHT** |
| 3 | Timeout guard removed, so a hung lookup leaves no flag | CAUGHT |
| 4 | `settle` can fire twice, dragging a resolved visitor back to US | CAUGHT |
| 5 | (covered by 1) `geoIpLookup` unwired | CAUGHT |

**#2 survived on the first pass** and is why the second commit exists. Every
failure fixture I had written *rejects* the fetch, and a rejection goes to
`.catch` — which never reaches the cache write at all. The only response that
reaches it is a **200 carrying no country**. Added, and it now catches.

---

## What this does NOT change

- **No server code.** `index.js` is untouched.
- **No payload change.** The same `phone` value in the same E.164 format still
  reaches `/partial` and `/submit`; `test-session-payload` pins an identical key
  set across both files.
- **Nothing about blocking or Meta.**
- `country.is` stays the provider. It is correct and free; ipinfo 406s browser
  requests, and that note is kept in the code.

## ⚠ Deploy — this one needs the Webflow step

**A `git push` does NOT ship this.** Both form files reach production through a
`<script src>` pinned to an immutable commit SHA, and **the tags are per-page**,
in each page's before-`</body>` footer — not Project Settings.

1. Merge to `main`.
2. Take the new SHA (`git rev-parse HEAD`) and update the tag on **every pinned
   page**, then republish. Today: `/demo` and `/ai-demo` carry
   `gushwork-form.js`; `/start`, `/start-now`, `/lead-gen`, `/seo-leads`, the
   four industry lead-gen landers and the four SEO landers carry
   `gushwork-form-popup.js`.
3. Pin **both** files to the same SHA even though only one page carries each.

Swept on 16 Sept 2026: every page is currently on `96360cc…`, and `/careers` and
`/meeting-booked` now carry **no** tag — those two were removed as planned.
`/start-old` is still pinned to the June commit `d493e92e` and is a 404; it stays
harmless only while unpublished.

Confirm the swap by loading the page and reading the console banner —
`Form initialised v…`, and `… (Google Ads)` on the popup pages.

**I cannot do step 2 from here.** Until it is done, the fix is in `main`, the
tests are green, Railway has redeployed, and every real visitor is still running
the old file.

## Not done

- No visual check in a real browser. The reasoning is from the library's
  documented `auto` behaviour and the executed unit tests; I did not load a page
  and watch the flag.
- The 2.5s timeout is a judgement call, not a measurement. country.is answered in
  300ms when tested; if it is ever slower, a visitor sees no flag for up to 2.5s.
