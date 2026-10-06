# Background: Attribution: cookies and the Salesforce source bucket

Moved here from `CLAUDE.md` on 7 Oct 2026, word for word, to bring that file under its size limit. `CLAUDE.md` keeps the rule for each of these as one line with a link here; this file keeps the history, measurements and worked examples behind it. Dates and counts are as of when each paragraph was written, and "today" means that day. Line numbers in the headings are `CLAUDE.md` at commit b924ce7.

## The shared cookie namespace and the in-app browser (was `CLAUDE.md` lines 1463-1501)

**THE COOKIE NAMESPACE IS SHARED BETWEEN THIS REPO AND WEBFLOW, AND
`gw_utm_campaign` IS ALREADY TAKEN.** Two scripts write cookies on
`.gushwork.ai` and they live in different places, so neither one's file
shows you the other:

| Cookie | Written by | Lifetime | Job |
|---|---|---|---|
| `gw_utm_campaign` / `gw_utm_medium` | `gushwork-form.js` `rememberCampaign()` | **30 days** | which offer to show a returning visitor |
| `gwa_*` (seven) | Webflow **site-wide footer** | **session** | carry attribution across an in-app-browser webview |
| `gw_ps_*` | Webflow site-wide footer | 90 days | PartnerStack |
| `_fbc` | `gushwork-form.js` | 90 days | Meta click id |

The attribution mirror was written on the `gw_` names first and it broke
both directions at once, caught by executing it rather than reading it:
re-writing `gw_utm_campaign` with no `max-age` **downgrades the 30-day
offer memory to a single session**, and reading that still-live 30-day
cookie back into `sessionStorage` puts a **paid campaign into
`utm_campaign` on an organic return visit, beside an empty
`utm_source`** — which `Source_Bucket__c` reads. That is exactly the
corruption the `offer_*` split exists to prevent, arriving from a
different file. Hence `gwa_`.

**The site-wide attribution script is in Webflow global site settings
(footer), NOT in this repo and NOT in `gushwork-embeds`.** It is the
thing that writes `gw_referrer`, `gw_landing_page` and `gw_utm_*` into
`sessionStorage`, which is where `gushwork-form.js` reads them from. So a
change to how attribution is captured is a **Webflow** change with no
diff anywhere in git — and, unlike a form pin, it needs no re-pin,
because rehydrating into `sessionStorage` leaves every reader untouched.

**Why a cookie at all: sessionStorage does not survive the Facebook and
Instagram in-app browsers.** Each navigation can open a fresh webview,
which keeps cookies and drops `sessionStorage` — which is why `_fbc`
lives through the same journey that loses the UTMs. Measured over 90
days: **31.2% UTM loss in-app, 1.6% on a normal mobile browser, 0% on
desktop, 424 leads.** Those leads still show the campaign in
`previous_page`, which is how the loss was found at all.


## How_Did_You_Hear__c and the substring formula (was `CLAUDE.md` lines 2511-2534)

**`How_Did_You_Hear__c` had NO writer at all until 17 Sept 2026.** Whatever
populated it upstream — most likely the Clientell managed package — stopped in
July 2026, and no commit in this repo had ever written it. Every lead that was
not a paid ad click therefore fell through the whole formula to `Others`.
Coverage was 37%; writing it took it to 70%.

**The formula matched two-letter substrings with no word boundary, and filling
the field made that fire more often.** `CONTAINS(..., "li")` sent "client",
"while", "link" and the name "Jolian" to **LinkedIn**; `CONTAINS(..., "ig")` and
`CONTAINS(..., "book")` sent "Right here", "SIG Investor" and "TEST BOOKING" to
**Meta**, from a branch sitting eight above Invalid/Test. Fixed and deployed
16 Sept 2026 (UTC): the short tokens are now space-padded, `_` and `-` are
normalised to spaces with `SUBSTITUTE` first so `meta_ads` and `diag-test` still
match, and Invalid/Test moved to the top of the ladder.

**Verify a formula change by REPLAYING IT, not by reading it.** Implement both
the old and new logic in JS, run them over every real record, and require the
**old** one to disagree with live Salesforce **zero** times before trusting what
the new one predicts. That harness caught three regressions that a careful read
of the formula did not: space-padding alone broke `meta_ads`, `Testing` and
`diag-test`. A formula recomputes on read, so a bad deploy silently rewrites all
of history at once — and so does a good one, which is why no backfill is needed
after fixing it.

## Source_Bucket_New__c, and the converted-lead join (was `CLAUDE.md` lines 2535-2551)

**`Source_Bucket_New__c` IS NOT A NEWER VERSION OF `Source_Bucket__c`.** The name
says otherwise and that reading is wrong. It is a writable restricted picklist on
**Opportunity** — `Outbound | Cold Email | Meta | Philly | Others` — and it
answers *which sales motion won the deal*, where the formula answers *which
inbound channel the person arrived from*. That is why it has no Google bucket.
It is **live**: 210 Opportunities in the 60 days to 17 Sept 2026, most recent the
day before. Do not "consolidate" or delete it; outbound reporting runs on it.

**A converted Lead does not always have an Opportunity, so
`ConvertedOpportunityId` is the WRONG join for "did this lead become a deal".**
Whoever converts can tick "do not create an opportunity", normally because the
Account already has one. 37 of our converted Website leads are in that state and
36 of them do have an Opportunity — reachable only through
`ConvertedAccountId`. A backfill keyed on `ConvertedOpportunityId` silently skips
every one of them, which is how 13 blank Opportunity buckets were missed on the
first pass.
