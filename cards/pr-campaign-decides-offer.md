# The ad campaign decides the offer, not the pathname

PR 89 - branch `feat/campaign-decides-offer` - 17 Sept 2026

Requested by Swapnil on 17 Sept 2026. Which offer a visitor sees was a pure
function of `window.location.pathname`, so a CRM ad landing anywhere but
`/ai-demo` sold AEO and nothing in the URL said otherwise.

## The rule

`utm_campaign` + `utm_medium` decide, and **every uncertain case asks**:

| Condition | Shows |
|---|---|
| Not `paid`/`cpc` | **Selector** |
| Campaign unreadable — all digits, or `{{…}}` | **Selector** |
| Paid + contains `crm` | **CRM** |
| Paid + contains `brand` | **Selector** |
| Paid, anything else | **AEO** |

Replayed over 90 days of real leads: ~2,129 get a decided AEO offer, ~1,379
keep the selector, 39 CRM (all already on `/ai-demo`).

## Three amendments to the request, and why

**1. `brand`, never `br`.** Measured all-time, every campaign containing "br"
already contains "brand", so the short token earns nothing today — and it is
the trap that just bit `Source_Bucket__c`, where a two-letter `CONTAINS`
routed "client", "link" and the name "Jolian" to LinkedIn.
`Prospecting__Broad__CBO` would have become a selector campaign silently.

**2. Only paid traffic gets an offer decided for it.** "Any other value → AEO"
also catches traffic that never saw an ad — LinkedIn posts, the lead-estimator
drip, email-signature links, backlinks from other sites, a Trustpilot profile
click. 29 leads over 90 days who declared no product intent and would have
lost the question. The request said to read only `utm_campaign`; that was
about CRM appearing in ad *names*, which is `utm_content` and is untouched.

**3. An unreadable campaign is not an AEO campaign.** 17 leads arrive as a
bare Meta campaign id and 6 as an unrendered `{{campaign.name}}`. Reading "no
crm in this string" off a number would hide the CRM offer from people who
clicked a CRM ad — the exact failure this change removes, re-created one
layer down.

## Why this is the fix

The request attributes the confusion to ad → homepage → `/demo` losing
context. Measured, the context is not lost: of 1,715 leads in 90 days whose
landing URL carried `utm_campaign` and who then submitted on `/demo`, **zero**
lost it, including all 110 who came via the homepage. That journey is about
8 Meta prospecting leads in 90 days, and no CRM-campaign lead has ever
submitted anywhere but `/ai-demo`.

The confusion has a different source. In the two days after the selector
shipped, **18 of 44 people ticked both boxes**, and both-ticked routes the
booking to the CRM team. None of the 18 came from a CRM ad; 8 came from AEO
ads; 12 booked. Asking a question somebody already answered by clicking is
what produced those. Hiding the selector from AEO campaigns removes 8 of the
18 immediately.

**What it does not fix:** the other 10 both-tickers are direct, organic and
brand traffic, who keep the selector and can still tick both. The lever there
is a single choice instead of two checkboxes — not in this PR.

## ⚠️ Blocking and Meta behaviour

**Nothing new blocks.** No change to `non_icp_blocked` or any website verdict.

**Meta, Salesforce and calendar routing DO move, for one population.** A paid
CRM-campaign lead on `/demo` now stores `product='crm'`, fires
`content_ids ['crm']` with `predicted_ltv` 5000 instead of `['aeo']` and
12000, books with the CRM team (RevenueHero 6804) and is no longer
disqualified for answering B2C.

**Zero leads hit that path today** — every CRM-campaign lead lands and submits
on `/ai-demo`. It is a safety rail, not a live change.

`product_interest` stays **NULL** throughout. An ad is a guess about someone,
not an answer they gave, and NULL there means "we never asked".

## Pre-existing bug fixed

`SCHEDULE_LEAD_SQL` never selected `product_interest`, so Schedule resolved
its product off the page while Lead used the tick — a both-ticked lead was
reported to Meta as two different products, with no error anywhere. The three
columns are now selected there. **This changes the Schedule event for existing
both-ticked leads**, from `['aeo']` to `['aeo','crm']`.

## The 30-day cookie

Built, as a **third fallback in the existing chain** (`URL → sessionStorage →
cookie`) rather than a separate offer cookie — so the offer on screen and
`leads.utm_campaign` cannot become two different notions of "the campaign".
A campaign in the URL always wins and rewrites it.

It is not for the cross-page case, which already worked. It is for the return
visit: 42 people in 180 days first arrived on a paid ad and came back later
with no campaign, on average 13.5 days later, 40 of them inside 30 days.

## Three copies

The rule lives in `meta-capi.js` and both form files, with no shared module —
same shape as `B2C_ALLOWED_PATHS`. `test-batch2.js` section 27 lifts all three
and **executes** them against real campaign values from `leads.utm_campaign`.

## Verification

Full bar green, **3,706 assertions across 12 suites**, run bare.

**Ten mutations, all caught.** Two survived on the first pass and were real
test gaps, now closed and re-verified:
- the `/ai-demo`-beats-campaign guard is invisible on today's catalogue (one
  entry, `/ai-demo → crm`, which agrees with the only overriding answer), so
  the test now adds a temporary second entry;
- dropping the campaign from **`/partial`**'s resolver survived, because every
  case drove `/submit`. `/partial` writes `leads.product` and fires StartTrial
  with its own `content_ids`.

`SCHEDULE_LEAD_SQL` was **executed** against the real schema via `EXPLAIN`,
not just read.

**Not verified:** nothing has run against production traffic, and the CRM
branch has no real leads to exercise it. After deploy it is confirmable by
hand — load `/demo?utm_campaign=FLI__Prospecting__CRM-Offer__CBO__StartTrial&utm_medium=paid`
and check the router and the stored slug.

## ⚠️ Not deployed by merging this

Forms are **v5.15.0**. A `git push` does not ship a form change — **13 Webflow
page footers** still need re-pinning to the new SHA and the site republished.
Until then every real lead runs v5.14.0.

Check `lastUpdated` vs `lastPublished` before republishing: publishing ships
the whole site, including anyone else's staged Designer work.

## Still open with Swapnil

1. `brand` only instead of `br` — amended here, needs his OK.
2. Reading `utm_medium` — a second parameter, he asked for one.
3. Single-choice selector, for the remaining 10 both-tickers.
4. Ad ops: some ad sets send a campaign **id** or an unrendered
   `{{campaign.name}}`. A CRM ad doing that would silently show the selector.

## What was verified, and what was not

Stated plainly, because a card is trusted rather than checked.

**Executed:** the full bar bare (3,706 assertions, 12 suites); ten mutations
with `measure.js --mutation`; `campaignOffer` called directly in all three
copies against real campaign values; `syncNeedsVisibility`, `wantsCrm`,
`b2cAllowedHere` and `rememberCampaign` driven in a stubbed DOM; `/partial`
and `/submit` driven over real HTTP with the bound columns and the
`content_ids` that reached `graph.facebook.com` read back;
`SCHEDULE_LEAD_SQL` EXPLAINed against the live schema.

**Measured against production data:** every number quoted above comes from a
query run in this session against the Railway database — the campaign
distribution, the 29 non-ad leads, the 23 unreadable campaigns, the 1,715
leads that lost nothing in transit, the 18-of-44 both-tick rate, and the 42
returning ad visitors.

**Asserted only structurally:** nothing new. The three-copy sync is executed,
not read.

**Never executed:** the CRM branch against real traffic — no lead has ever
taken it. Confirmable by hand after deploy with a query-string URL.

**Not done:** the Webflow half. 13 page pins, unchanged.
