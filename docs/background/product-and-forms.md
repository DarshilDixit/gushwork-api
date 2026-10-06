# Background: The /demo product question, routing and form CSS

Moved here from `CLAUDE.md` on 7 Oct 2026, word for word, to bring that file under its size limit. `CLAUDE.md` keeps the rule for each of these as one line with a link here; this file keeps the history, measurements and worked examples behind it. Dates and counts are as of when each paragraph was written, and "today" means that day. Line numbers in the headings are `CLAUDE.md` at commit b924ce7.

## "What are you looking for?" and the two product columns (was `CLAUDE.md` lines 1259-1277)

**"What are you looking for?" ON `/demo` ONLY — and `product` vs
`product_interest` are two columns because they answer two questions.** Two mandatory
checkboxes (`aeo`, `crm`) revealed after `sell_to` is answered. The values
in the markup ARE the product slugs, so the checkbox, `product_interest`,
the Salesforce picklist and the Meta `content_ids` are one vocabulary with
no mapping table to drift.

- **`leads.product` stays SINGLE-VALUED** — the routing slug. One calendar,
  one Meta event, one Salesforce picklist value. CRM wins a both-ticked
  selection. Widening it would break `Product__c` (a restricted picklist)
  and the three `product = 'crm'` equality predicates at once.
- **`leads.product_interest` holds what they ticked** — canonical: lowercase,
  known slugs only, deduped, **sorted**. `'aeo'`, `'crm'`, `'aeo,crm'`.
  **NULL means "we never asked"** and is the honest value for every page but
  `/demo`. Never backfilled.
- **Salesforce gets `product_interest ?? product`.** `Product__c` carries
  three values — `aeo`, `crm`, `aeo,crm` — added and verified against the
  live org on 15 Sept 2026.

## Routing slug vs event slug (was `CLAUDE.md` lines 1278-1293)

**THE ROUTING SLUG AND THE EVENT SLUG ARE DIFFERENT FUNCTIONS.**
`resolveProduct` answers "which calendar, which Salesforce picklist value"
and must be single — `crm` for a both-ticked lead. `resolveEventProduct`
answers "what did we tell Meta this conversion was", and a both-ticked lead
is honestly **both**: `content_ids: ['aeo','crm']` on **one** event.

**One event, never two.** Deduplication is documented as cross-source only
(*"Does not deduplicate events when only using one event source"*), and every
active ad set optimises on conversion **count** — so two events would count
one person twice and corrupt exactly what they bid on.

**The Meta event must match `product_interest`, not `product`** — the same
rule `Product__c` already follows. They agree on every lead except the
both-ticked one, which is the only place the invariant is visible, and the
divergence test exists for that row.

## predicted_ltv config and what was sent (was `CLAUDE.md` lines 1294-1307)

**`predicted_ltv` is CONFIG: `META_LTV_AEO` / `META_LTV_CRM` /
`META_LTV_AEO_CRM`**, defaults 12000 / 5000 / **15000**, all PROVISIONAL.
Combined is 15000 and **not** the 17000 sum: it predicts what the *person* is
worth, where the sum would assert they buy both at full price with certainty.
An unreadable env var falls back to the default and warns once — a `NaN` in a
payload is worse than a stale number.

**`leads.meta_predicted_ltv` records what was actually sent**, NULL where no
Meta event fired. The config can move; without this there is no way back to
what was reported for a cohort, which is exactly what you need before
switching anything to value optimisation. **That switch is blocked on closed-won
revenue joined back to leads, not on the config** — see the value-optimisation
section in `docs/tickets/non-icp-v1-block.md`.

## resolveProduct takes both; an unknown slug loses the lead (was `CLAUDE.md` lines 1308-1325)

**`resolveProduct` TAKES THE SELECTION AND THE PAGE, AND BOTH THE COLUMN AND
THE META EVENT MUST BE RESOLVED FROM BOTH.** `buildEventData` read `page_url`
alone, which was correct only while product was a pure function of the page.
With a checkbox deciding it, a `/demo` lead ticking AI-CRM stores `crm` and
would fire `aeo` — the dashboard saying one thing and Facebook optimising for
another, **with no error anywhere**. `tests/test-non-icp-routes.js` drives
`/submit` across four selections and compares the bound column against the
`content_ids` that actually reached `graph.facebook.com`.

**An unknown `product_interest` loses the whole LEAD, not the field.**
`Product__c` is restricted; an unknown value is
`INVALID_OR_NULL_FOR_RESTRICTED_PICKLIST`, which `sfUnknownFields` does
**not** retry. `canonicalProductInterest` dropping unknown slugs is the only
thing between a tampered checkbox and a lost lead — `test-batch2.js` §24
executes it against 24 adversarial inputs and enumerates every slug subset
against the picklist, so adding a third product without adding its
combinations to Salesforce fails there rather than in production.

## The B2C gate fires at the next click; the /demo markup (was `CLAUDE.md` lines 1326-1346)

**THE B2C GATE FIRES AT THE NEXT CLICK, NOT ON SELECTION**, and the
progressive reveal depends on that. There is no change handler on the
`sell_to` radios that decides anything, `handleStep1Next` has exactly two
triggers, and `showStep('step-disqualified')` is reached from one statement
inside it — so the checkboxes are read one line above the gate in the same
call. Had it fired on selection, a B2C click would jump to the DQ step before
the checkboxes were visible and the CRM exception could never apply. Four
assertions hold it there, including one that the sell-to change handler never
calls `showStep`, `savePartial` or touches `disqualified`.

**Webflow markup this depends on, `/demo` only:** `#needs-wrap`, `#need-aeo`
/ `#need-crm` with `name="needs"` and `value="aeo"`/`"crm"`, `#needs-error`,
`#about-business-wrap`, and `data-rh-router-crm="6804"` on the form wrapper —
**and NOT `data-rh-router`**, whose absence is what keeps the 6138 AEO
default. No markup means no selection, the server resolves from the page as
before, and `product_interest` stays NULL. `/ai-demo` is untouched.

The on-screen heading is **"What are you looking for?"**, changed in Webflow
on 16 Sept. Nothing in the code reads it — it is recorded here only so this
file and the page agree.

## The two router attributes (was `CLAUDE.md` lines 1347-1372)

**THE TWO ROUTER ATTRIBUTES ARE NOT INTERCHANGEABLE, and picking the wrong
one sends a page's traffic to the wrong team.** `rhRouterId()` reads
`data-rh-router-crm` ONLY when `wantsCrm()` is true, then falls through to
`data-rh-router`, then to 6138.

| Attribute | Applies | Use it on |
|---|---|---|
| `data-rh-router` | always | a page that sells ONE product |
| `data-rh-router-crm` | only when this visitor is a CRM lead | a page where the visitor decides |

So `/demo` carries `data-rh-router-crm="6804"` and no `data-rh-router` —
conditional, because the visitor decides there. `/ai-demo` and `/ai-crm`
carry `data-rh-router="6804"` — unconditional, because the page decides.

**A CRM PAGE MUST NOT USE THE `-crm` ONE.** `wantsCrm()` is true only when
the visitor ticked AI-CRM or arrived on a CRM campaign, and a CRM page has
neither the checkboxes nor, for most of its traffic, a campaign — so
direct, organic and brand visitors would fall straight past it to the AEO
team. That is what `/ai-crm` did until 18 Sept, when it had no attribute
at all. The plain attribute asserts a fact about the page; the `-crm` one
depends on detecting something about the person.

The attribute lives on the form wrapper, so it travels with the content
when `/ai-crm` is duplicated into `/ai-demo` — and both already hold the
same value, so that swap is a no-op for routing.

## The collapsible wrappers and the needs cards (was `CLAUDE.md` lines 1373-1413)

**HIDDEN BY A CLASS, NOT `display:none`, AND THAT IS LOAD-BEARING.** Both
wrappers carry `field-wrapper is-collapsible is-hidden`. Webflow cannot store
an inline style, so `style.display = ''` could never reveal anything — that
shipped in PR 75 and meant `#about-business-wrap` could not have appeared at
all. `setWrapHidden` toggles `is-hidden`; `needsVisible()` reads the class, so
it is correct from first paint rather than only after init.

`.field-wrapper.is-collapsible` is the animation: `overflow:hidden`,
`max-height:240px`, and a 180ms transition on max-height, opacity, transform
and margin-bottom. The hidden state is the three-class combo
`.field-wrapper.is-collapsible.is-hidden` — three classes so it beats the
two-class combos whatever order Webflow emits them in.

**The 240px ceiling is the one fragile number.** Measured: the needs block is
about 94px and the textarea wrapper about 110px, so it is roughly 2x headroom.
Grow past it and the content **clips silently** — no scrollbar, no error. A
taller ceiling is not free either: the visible content finishes expanding
before the max-height animation does, which reads as a dead pause. If a third
card or a taller textarea ever lands here, re-measure rather than just raising
the number.

**The cards NEVER wrap — they hold 50/50 at every width, like the sell-to
row.** `.radio-wrap.is-needs` is `flex: 1 1 0%` with `min-width: 0`, and the
row is the plain `.radio-holder`, whose default `flex-wrap` is `nowrap`. An
even share rather than `calc(50% - Npx)` because **the gap is not constant**:
`.radio-holder` is 14px at main and 8px at `tiny`, so any hardcoded half is
wrong at one breakpoint or the other. `flex-basis: 0` divides whatever is left
after the gap, whatever the gap is.

**A `min-width` here is a bug, not a safety net, and it shipped once.** A
200px minimum demanded 408px of row where 390px viewports only give 319px, so
the cards stacked on mobile. Measured at 390: the row is ~319px, each card
~155.5px, leaving ~99.5px of text after the 16px checkbox, its 12px gap and
28px of padding. "Custom AI CRM" at 12px is ~85px and fits on one line; the
10px subtitle wraps to two, exactly as the sell-to subtitles already do in
76.5px. At 320px each card is ~120px and the title wraps to two lines — still
renders, same as sell-to. There is no realistic width where wrapping helps.

The base `.radio-wrap` and `.radio-holder` are **untouched** — shared with 36
elements on other pages.

## The sell_to gate, B2C_ALLOWED_PATHS and the /ai-crm miss (was `CLAUDE.md` lines 1414-1462)

**THE `sell_to` GATE IS CLIENT-SIDE ONLY, and the CRM product is excepted
from it.** B2C or Mixed at step 1 sets `disqualified` and shows a terminal
step — in `gushwork-form.js` and `gushwork-form-popup.js`. The server
receives the boolean and believes it; it never decides. So "allow B2C on
the CRM product" could only be built in the two form files, as
`B2C_ALLOWED_PATHS`, matched against the pathname the same way
`resolveProduct` normalises it.

That makes **FOUR copies of the CRM path set**: `PRODUCT_PATHS` in
`meta-capi.js`, `index.js` importing it, and one in each form file.
`tests/test-batch2.js` section 21 lifts all three files and asserts they
agree. Add a product page to the catalogue without adding it to the forms
and the visitor is disqualified on a page the server has already decided
is CRM — nothing else in the repo would say so.

**AND THE REVERSE HAPPENED ON 18 SEPT 2026: a page live in Webflow and in
NEITHER list.** `/ai-crm` shipped carrying `data-rh-router="6804"`, so its
bookings reached the CRM team, while being absent from `PRODUCT_PATHS` and
from both `B2C_ALLOWED_PATHS`. Every lead from it was stored and reported
as `aeo` — wrong Salesforce picklist, wrong Meta event, 12000 of predicted
value instead of 5000 — and a B2C answer dead-ended the prospect. The
booking went to the right team and the record said the wrong thing.

**IT HALF-WORKED, WHICH IS WHY IT SURVIVED.** A visitor arriving on a CRM
campaign got through the gate by the OTHER door — `currentOffer() === crm`
— so the page looked correct to anyone who tested it from an ad, and
failed for direct, organic, brand and AEO-ad traffic, which is most of it.
Found by a human answering B2C on the live page. Section 21 could not
catch it: that test asserts the three lists AGREE, and they agreed
perfectly about a page none of them had heard of.

**So the check when a new product page appears is "is it in the
catalogue", not "do the lists agree".** A page can be live, correctly
routed and entirely absent from this repo.

**Two `sell_to ILIKE 'B2B%'` predicates exist and they MOVE TOGETHER** —
`/monitor/sdr` and the `noBooking` card in `/monitor/metrics` that counts
it. Both carry `OR product = 'crm'`, because a CRM lead who answers B2C is
no longer disqualified and would otherwise drop out of the SDR list
silently: present in every headline number, absent from the one surface
anybody acts on. Same shape as the `SDR_SEARCH_COLUMNS` /
`SDR_SEARCH_FIELDS` pair.

**A CRM B2C lead now fires Meta, can fire a $50 PartnerStack conversion,
and is pushed to Salesforce.** All three follow from the lead no longer
being disqualified, and all three were authorised explicitly on 15 Sept
2026 — the non-ICP reasoning is about AEO and does not transfer to CRM.
Measured exposure is about two leads a day.

## /ai-crm in the catalogue, and why AEO is the default (was `CLAUDE.md` lines 1556-1572)

**`/ai-crm` is the rebuilt CRM page and will eventually replace
`/ai-demo`'s content wholesale.** Both are mapped, both resolve `crm`, and
the swap therefore changes nothing here. It is also the FIRST entry that
makes the page-beats-campaign rule observable: until there were two, the
only mapping and the only overriding campaign answer were both `crm`, so
the guard could be deleted without any test noticing. Measured — the
mutation survived a full bar.

This is the opposite of how the rest of the repo works, deliberately. An
AEO allowlist rots: the form is live on a dozen pages — `/demo`, `/start`,
`/pricing`, `/consulting-lead-generation` and the SEO landers — and new
landers get added by people who will never open this file. Measured on 90
days of real leads, a `/demo`-only list tagged **82%** and left **631 leads
(408 completed)** sending unlabelled events; the default tags **99.7%**. A
default fails only when a genuinely new product launches, which is rare,
deliberate, and logged once per unmapped path.

## The maxlength mismatch, and the third page (was `CLAUDE.md` lines 1613-1626)

**THAT LAST SENTENCE WAS AN INTENTION, NOT A MEASUREMENT, UNTIL 18 SEPT
2026.** All three pages carrying the field — `/demo`, `/ai-demo` and
`/ai-crm` — rendered `maxlength="5000"`, Webflow's default, while the
server cut at 1000. Anyone writing a long answer would have lost the tail
silently: no warning, no error, and nothing in the row to show it had been
cut. Never actually bit — the longest answer ever recorded is 262
characters and no row sits at exactly 1000 — so this was a trap rather
than a loss. All three are now genuinely 1000.

**THE FIELD IS ON THREE PAGES, and the third is easy to miss.** `/demo`
has it behind the AI-CRM tick inside `#about-business-wrap`; `/ai-demo`
and `/ai-crm` have it permanently visible with no wrapper. A change to
"the about-business textarea" is three edits.
