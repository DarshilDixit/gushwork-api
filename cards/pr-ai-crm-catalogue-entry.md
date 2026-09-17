# /ai-crm is a CRM page everywhere, not just on the calendar

PR 90 - branch `fix/ai-crm-catalogue-entry` - 18 Sept 2026

`/ai-crm` shipped live carrying `data-rh-router="6804"`, so bookings reached the
CRM team — while being absent from `PRODUCT_PATHS` and from both copies of
`B2C_ALLOWED_PATHS`.

## Two consequences

**1. Every lead was stored as `aeo`.** Wrong Salesforce picklist, wrong Meta
event, `predicted_ltv` 12000 instead of 5000. The booking went to the right
team and the record said the wrong thing.

**2. A B2C or Mixed answer dead-ended the prospect** on a page that sells to
B2C happily. Measured on `/ai-demo`, the page `/ai-crm` replaces: **9 of 40
leads answered B2C or Mixed**.

## Why it survived

It half-worked. A visitor arriving on a **CRM campaign** got through the gate by
the other door — `currentOffer() === 'crm'` — so the page looked correct to
anyone testing it from an ad, and failed for direct, organic, brand and AEO-ad
traffic, which is most of it.

| On `/ai-crm`, answering B2C | Before | After |
|---|---|---|
| direct / organic | blocked | allowed |
| AEO ad | blocked | allowed |
| brand search | blocked | allowed |
| CRM ad | allowed | allowed |

Found by answering B2C on the live page, not by a test.

## The change

Three entries, which `test-batch2.js` §21 already required to move together:

```
meta-capi.js            PRODUCT_PATHS       '/ai-crm': 'crm'
gushwork-form.js        B2C_ALLOWED_PATHS   + '/ai-crm'
gushwork-form-popup.js  B2C_ALLOWED_PATHS   + '/ai-crm'
```

**No leads have come from `/ai-crm` yet**, so nothing needs correcting after the
fact.

`/ai-crm` is also the first `PRODUCT_PATHS` entry that makes the
page-beats-campaign guard matter in production — an AEO prospecting campaign
pointed at the CRM page must not demote the lead. That is now pinned directly
rather than through the synthetic catalogue entry the test had to invent.

## ⚠️ Blocking and Meta

Nothing new blocks. A `/ai-crm` lead now stores `product='crm'`, fires
`content_ids ['crm']` at `predicted_ltv` 5000 instead of `['aeo']` at 12000, and
a B2C answer no longer disqualifies. That is the same decision already taken for
`/ai-demo` on 15 Sept, applied to the page replacing it.

## Verification

Full bar green, **3,787 assertions, 12 suites**, run bare. **Three mutations,
all caught:**

- dropping `/ai-crm` from `PRODUCT_PATHS` → caught by 2 suites
- dropping it from **one side of the fork only** → caught
- removing the page-beats-campaign guard → caught

New coverage:

- the catalogue is pinned **by value**, not by count, so adding a page is a
  decision somebody made rather than a diff nobody read;
- both form files driven for a B2C answer on `/ai-crm` across five journeys,
  plus trailing-slash and uppercase paths, with `/ai-crm-pricing` asserted
  **not** to match by prefix;
- `/submit` driven end to end from `/ai-crm` across five journeys, reading back
  the bound column, the `content_ids` that reached `graph.facebook.com`, and the
  `predicted_ltv`.

## Not in this PR — for whoever owns the page

`/ai-crm` has **no `about-business` field**; `/ai-demo` does. **21 of 40
`/ai-demo` leads (53%) fill it in.** If `/ai-crm` replaces `/ai-demo` as it
stands, that answer stops being collected for half of CRM leads. Webflow markup,
not code — add `id="about-business"` with no wrapper around it, exactly as
`/ai-demo` has it.

## What was verified, and what was not

**Executed:** the full bar bare (3,787 assertions); three mutations via
`measure.js --mutation`; `b2cAllowedHere` driven in a stubbed DOM from BOTH form
files across five journeys and three path spellings; `/submit` driven over real
HTTP from `/ai-crm` across five journeys with the bound columns and the Meta
payload read back.

**Measured against production:** zero leads from `/ai-crm`; 9 of 40 `/ai-demo`
leads answered B2C or Mixed; 21 of 40 filled in `about_business`.

**Checked live:** `/ai-crm` carries `data-rh-router="6804"`, loads
`gushwork-form-popup.js`, and has no selector markup and no `about-business`
field.

**Never executed:** nothing new. Every branch this touches is driven.

**Known gap left open deliberately:** the missing `about-business` field on
`/ai-crm`. Webflow markup, not code, and not this PR's to fix.
