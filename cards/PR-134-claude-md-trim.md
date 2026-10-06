# PR 134: CLAUDE.md under the size limit — history moved to docs/background/, every rule stays

Branch `docs/claude-md-trim`, from `main` at `b924ce7` (after #133). Written 7 Oct
2026, **updated the same day after review of five rules** (commit `605e004`).
**Not merged: waiting for your review. Nothing was sent anywhere.** Documentation
only: **no code change, and nothing alters which leads are blocked or which fire
Meta.**

## Follow-up commit `605e004`, after your review of five rules

1. **Rule at line 296, the non-ICP name-only fallback.** It no longer
   suggests Swapnil authorised the fallback.
   - **Swapnil's 11 Sept authorisation now names "the non-ICP block as a
     whole".** The fallback dates from 23 Sept.
   - **Added that you set the 0.85 floor on 26 Sept 2026**, down from a
     provisional 0.9.
   - **Added that one floor per kind of evidence serves both block and
     Meta**, with  as the case (insurance at 0.82 from the
     letters "ins", which still withheld Meta).
   - **Added a pointer to the CORRECTION section** at the end of the V1 ticket.
   - **One more fix beyond your list, inside the same rule:** blocking is read
     from the type and floor "as well as from the stored  column".
     My review had flagged that phrase as dropped.
2. **Rule at line 1022, the model layer's four traps.**
   - **Trap three now says "and the schema is not the defence"**, and
     "stripped" became "markup stripped", the moved text's wording.
   - **Trap four now says "no network"**, and that "no timeout" means no
     time-out-and-let-the-lead-through path, with the reason.
3. **Rule at line 1024, the brand-list traps.**
   - **The full SQL is back:** .
   - **Added that a test pins all three sites**, and that  re-fires
     all through step 1. Also added that the B2B button calls
      again.
4. **The Google Ads block is restored word for word** (old lines 2104–2160),
   replacing its one-line rule.
   - **That brings back every item on my omissions list:** the cutover offset
     and Lorenzo's sheet, the two switch names, validate-only proving nothing
     about the click, "skips flagged leads whatever  says",
     the section 10f reason, Meta's  trap, one event per
     request, validate-only read-backs answering 400, the deliberately
     separate exclusion lists, and consent frozen into each payload.
   - ** is deleted**, along with its index
     entry.
5. **The new-dashboard block is restored word for word** (old lines
   1876–1967), replacing its one-line rule. The 44px condition is in it as
   written: "below 1024 wide or on any coarse pointer". That section came out
   of , and its index entry no longer mentions
   PR D.
6. **The index intro now says "about 122k"** instead of "about 113k".
   **Nothing else in CLAUDE.md changed:**  shows 8 lines removed,
   and every added line is one of the above.

**Size: 122,171 characters, over your 120k cap, by your choice.** Restoring
the dashboard block word for word costs 5,488 characters, and you picked the
full restore over a partial one or a rewritten rule. That is still about 28k
under the 150k load limit.

**This card replaces its first version.** The first version said 113,054
characters, 54 rules, 13 files, 55 moved blocks and 154 added lines. Each
number below is re-measured on `605e004`.

**Two stale sentences came back with the restored blocks, kept word for word:**
- The Google Ads block's "Flighted and Upraw (`GADS_EXCLUDED_DOMAINS`)"
  predates the shared `AGENCY_DOMAINS` list. The one rule section has the
  current picture.
- The dashboard block's "one week" fallback window ran out on 3 Oct.

## What the PR does now

| | Before | After |
|---|---:|---:|
| `CLAUDE.md` characters | 171,676 | **122,171** |
| `CLAUDE.md` lines | 2,772 | 1,662 |
| Tokens when loaded (`/context`) | 65.7k | 46.6k |
| `docs/background/` | — | 12 files, 83,383 chars |

1. **Moved, never deleted.** History, measurements and worked examples went
   into 12 topic files under `docs/background/`, **word for word**.
2. **Every warning that moved left a one-line rule behind**, in place, linking
   to its file: **52 rules**. The 17 Sept page-count block left none of its
   own; it is covered by the scan rule above it.
3. **New order:** the intro, then **Deploying (the working agreement and test
   commands), Mutation testing, Style, Working with Darshil**, then the index
   ("Where the background lives"). The rest runs in its old order.
4. **The Google Ads line is its own commit (`4309d34`)**, last bullet of
   Working with Darshil.
5. **`tools/check-claude-md-move.js`** is the copy-count proof, read-only,
   with its own Layout row.

Commits:
- `860040c`: the move;
- `4309d34`: the GADS line;
- `a81a085`: the first card;
- `605e004`: the follow-up.

## Step 1, as shown before editing

- **The biggest section was "Things that will bite you"**: 98,175 characters,
  57% of the file.
- **Nothing was cut off when it loaded**, as far as I could tell.
- **Nothing was running.**
- **Your warning said 162.8k; I measured 171,676.** That stays unreconciled.

## The steering protocol: not found, nothing written

- **`~/.claude/CLAUDE.md` does not exist.**
- **gushwork-web's `CLAUDE.md` and the `AGENTS.md` it imports never use the
  word "steer".** The nearest things are its "How we work" section (never
  merge or deploy; one PR per step; a card per PR, sent straight to `main`;
  "unverified, never confirmed") and *"No secrets in the repo"* under Code.
- **The migration-order warning was skipped**, as you asked.

## Step 3: the copy-count proof, re-run on `605e004`

`node tools/check-claude-md-move.js b924ce7`, exit 0:

```
old CLAUDE.md at b924ce7: 2772 lines, 2365 non-blank, 2341 distinct
new: 13 files, 3041 lines
  CLAUDE.md  122171 chars
  docs/background/alerting.md  3768 chars
  docs/background/attribution.md  5560 chars
  docs/background/dashboard.md  10020 chars
  docs/background/definitions.md  2943 chars
  docs/background/dropoff-and-digest.md  5967 chars
  docs/background/internal-test-submissions.md  3066 chars
  docs/background/meta.md  1609 chars
  docs/background/non-icp.md  11216 chars
  docs/background/partnerstack.md  9822 chars
  docs/background/product-and-forms.md  15068 chars
  docs/background/testing.md  6110 chars
  docs/background/webflow.md  8234 chars
blank lines: old 407, new 529 (not matched)

LOST (in the old file more times than in the new set): 0
DUPLICATED (old line now appears more times than before): 0
old non-blank lines accounted for exactly: 2365 of 2365

ADDED (new lines, not in the old file): 136 distinct, 147 total
```

**How it counts:** every old non-blank line appears **exactly as many times**
across the new CLAUDE.md and `docs/background/` as before. Fewer means lost;
more means copied instead of moved. **Blank lines are counted but not
matched.** On its first run it caught two of my own additions that reused
existing text (a `|---|---|` header and a `---`), and those two lines were
changed rather than the check loosened.

## Every line added (147 in total)

**In CLAUDE.md, 70 lines:**

| # | Line | Name | Links to (in `docs/background/`) |
|---|---:|---|---|
| 1 | 235 | - Never print the Google Ads key at `~/.config/gushwork/gads-sa.json`, or the va… | — |
| 2 | 239 | ## Where the background lives | — |
| 3 | 241 | On 7 Oct 2026 this file was cut from about 172k characters to about 122k, under … | — |
| 4 | 243 | Index entry for `docs/background/non-icp.md` | — |
| 5 | 244 | Index entry for `docs/background/meta.md` | — |
| 6 | 245 | Index entry for `docs/background/webflow.md` | — |
| 7 | 246 | Index entry for `docs/background/product-and-forms.md` | — |
| 8 | 247 | Index entry for `docs/background/attribution.md` | — |
| 9 | 248 | Index entry for `docs/background/testing.md` | — |
| 10 | 249 | Index entry for `docs/background/alerting.md` | — |
| 11 | 250 | Index entry for `docs/background/dashboard.md` | — |
| 12 | 251 | Index entry for `docs/background/internal-test-submissions.md` | — |
| 13 | 252 | Index entry for `docs/background/dropoff-and-digest.md` | — |
| 14 | 253 | Index entry for `docs/background/partnerstack.md` | — |
| 15 | 254 | Index entry for `docs/background/definitions.md` | — |
| 16 | 256 | `node tools/check-claude-md-move.js b924ce7` proves the move: every non-blank li… | — |
| 17 | 296 | The model layer has a second, weaker input since 23 Sept: `NON_ICP_NAME_FALLBACK` | `non-icp.md` |
| 18 | 374 | `nonIcpScheduleSuppressed` is async since 23 Sept and also re-reads `non_icp_domain_verdicts` | `non-icp.md` |
| 19 | 427 | Layout row for `docs/background/*.md` | — |
| 20 | 428 | Layout row for `tools/check-claude-md-move.js` | — |
| 21 | 447 | The tags are PER PAGE, in each page's own custom code, not in Project Settings | `webflow.md` |
| 22 | 468 | Scan BOTH custom-code blocks, head and footer, on EVERY page, through the API, and count pinned BLOCKS, not pages | `webflow.md` |
| 23 | 476 | Repin through the Webflow site API, not the MCP tool | `webflow.md` |
| 24 | 478 | Don't create Webflow form fields headlessly | `webflow.md` |
| 25 | 509 | The console banner is only a check if the version moved | `webflow.md` |
| 26 | 606 | The model layer's dashboard surface is `/monitor/non-icp` and the Model tab, separate from Blocked on purpose | `dashboard.md` |
| 27 | 641 | `leads.ip_address` and the `ip_*` columns | `dashboard.md` |
| 28 | 711 | Two populations are `completed` without being form completions: someone who reac… | `definitions.md` |
| 29 | 867 | 853 session rows from `/careers` and `/meeting-booked`, which carried the form script by mistake until 10 Sept 2026, are staying | `definitions.md` |
| 30 | 916 | A source assertion cannot see runtime behaviour, and a confident zero passes every structural check | `testing.md` |
| 31 | 918 | After a headless Webflow write, the open Designer serves a stale tree, so a snapshot can lie | `webflow.md` |
| 32 | 920 | `leads.disqualified` has fourteen call sites that no single grep reaches, and adding `non_icp_blocked` turned every one into half a guard | `non-icp.md` |
| 33 | 936 | It exists because the cost lands at the MEETING, not the booking (booking to dem… | `non-icp.md` |
| 34 | 953 | Meta was sent the whole `x-forwarded-for` header — two entries on Railway — as `client_ip_address`, and nothing complained | `meta.md` |
| 35 | 966 | Meta never complains | `meta.md` |
| 36 | 1003 | The Visitors tab (`/monitor/visitors`) | `dashboard.md` |
| 37 | 1022 | The model layer's four traps | `non-icp.md` |
| 38 | 1024 | The brand-list block's three traps | `non-icp.md` |
| 39 | 1054 | "What are you looking for?" is on `/demo` only | `product-and-forms.md` |
| 40 | 1056 | The routing slug and the Meta event slug are different functions | `product-and-forms.md` |
| 41 | 1058 | `predicted_ltv` is config: `META_LTV_AEO` / `META_LTV_CRM` / `META_LTV_AEO_CRM`, 12000 / 5000 / 15000, all provisional | `product-and-forms.md` |
| 42 | 1060 | `resolveProduct` takes the selection AND the page, and both the column and the Meta event must be resolved from both | `product-and-forms.md` |
| 43 | 1062 | The B2C gate fires at the next click, never on selection | `product-and-forms.md` |
| 44 | 1064 | The two router attributes are not interchangeable | `product-and-forms.md` |
| 45 | 1066 | The collapsible wrappers are hidden by the `is-hidden` CLASS, never `display:none` | `product-and-forms.md` |
| 46 | 1068 | The `sell_to` gate is client-side only, and CRM pages are excepted through `B2C_ALLOWED_PATHS` | `product-and-forms.md` |
| 47 | 1070 | The cookie namespace on `.gushwork.ai` is shared with Webflow, and `gw_utm_campaign` is taken | `attribution.md` |
| 48 | 1099 | Execute any SQL you touch | `testing.md` |
| 49 | 1107 | `/ai-crm` and `/ai-demo` both resolve `crm`, so swapping `/ai-crm`'s content int… | `product-and-forms.md` |
| 50 | 1149 | The about-business textarea is on THREE pages | `product-and-forms.md` |
| 51 | 1151 | Apollo refuses with a reply, not an exception | `alerting.md` |
| 52 | 1159 | A Meta CAPI failure reaches `recordFailure` only because the push functions THROW | `alerting.md` |
| 53 | 1168 | One row builder renders All Leads and Blocked, both in the DOM at once, so row ids must be namespaced | `dashboard.md` |
| 54 | 1170 | The Model tab keeps one claim per number | `dashboard.md` |
| 55 | 1196 | `b@g.ai` is special-cased in four hardcoded lists and is now in `INTERNAL_TEST_E… | `internal-test-submissions.md` |
| 56 | 1208 | The page is a signal, not only the address | `internal-test-submissions.md` |
| 57 | 1217 | Nothing is lost when it fires: the lead is still written and still on the dashbo… | `internal-test-submissions.md` |
| 58 | 1374 | "Left on step 2" (`step_reached=1`) means the visitor COMPLETED step 1 and reached step 2 | `dropoff-and-digest.md` |
| 59 | 1376 | Source reads `utm_source`, then `hear_about_us` | `dropoff-and-digest.md` |
| 60 | 1378 | The weekly digest | `dropoff-and-digest.md` |
| 61 | 1392 | Why: 2,669 of 2,676 stored phones start with "+". A spreadsheet still reads a nu… | `dashboard.md` |
| 62 | 1489 | Partner identity resolves in three layers (memory, the database, the API) and never blocks a lead | `partnerstack.md` |
| 63 | 1513 | A hand-run API read is not a better oracle than the sweep that waits | `testing.md` |
| 64 | 1556 | Partner revenue gaps (`/monitor/partner-gaps`) is a WORK QUEUE, not a health check; never wire it into System Health | `partnerstack.md` |
| 65 | 1558 | Step 10 is a 2-minute POLLER, not a Salesforce Flow callout | `partnerstack.md` |
| 66 | 1569 | `partner_domain_sf_state.sf_state` is a SNAPSHOT that moves both ways | `partnerstack.md` |
| 67 | 1571 | `Partner_Source__c` is this service's only write to Opportunity, and a permission failure looks exactly like "no Opportunity" | `partnerstack.md` |
| 68 | 1625 | Eligibility rule (a)'s contact source is a registry (`PS_CONTACT_SOURCES` / `PS_CONTACT_ACTIVE`), today prior form leads only; measure before switching an outbound log on | `partnerstack.md` |
| 69 | 1654 | `How_Did_You_Hear__c` had no writer until 17 Sept 2026, and the formula matched two-letter substrings with no word boundary | `attribution.md` |
| 70 | 1656 | `Source_Bucket_New__c` is NOT a newer `Source_Bucket__c` | `attribution.md` |

**In `docs/background/`, 77 lines:**
- **12 titles.**
- **One intro paragraph, the same in all 12 files** (12 copies): moved word
  for word on 7 Oct 2026, dates as written, and the heading line numbers are
  `CLAUDE.md` at `b924ce7`.
- **53 section headings**, one per moved block, which are the rows below.

## Where each moved section went (53 blocks)

| File | Old lines | Section heading |
|---|---|---|
| `non-icp.md` | 59-119 | The model layer's name-only input, floors and history |
| `non-icp.md` | 196-216 | Why the Schedule guard is async |
| `non-icp.md` | 992-1015 | The `disqualified` guards, missed in three waves |
| `non-icp.md` | 1030-1036 | Why the late-verdict sweep has hours, not seconds |
| `non-icp.md` | 1171-1206 | The model layer's four traps |
| `non-icp.md` | 1207-1230 | The brand-list block's three traps |
| `webflow.md` | 286-300 | The tags are per-page (correction of 15 Sept) |
| `webflow.md` | 320-348 | The curl sweep's blind spot, `/start-old`, pins in the head |
| `webflow.md` | 355-362 | The page count was wrong (correction of 17 Sept) |
| `webflow.md` | 363-377 | Repinning through the Webflow API |
| `webflow.md` | 378-409 | Editing page elements headlessly |
| `webflow.md` | 439-446 | The banner that did not move (16 Sept) |
| `webflow.md` | 972-991 | The Designer serves a stale tree |
| `product-and-forms.md` | 1259-1277 | "What are you looking for?" and the two product columns |
| `product-and-forms.md` | 1278-1293 | Routing slug vs event slug |
| `product-and-forms.md` | 1294-1307 | predicted_ltv config and what was sent |
| `product-and-forms.md` | 1308-1325 | resolveProduct takes both; an unknown slug loses the lead |
| `product-and-forms.md` | 1326-1346 | The B2C gate fires at the next click; the /demo markup |
| `product-and-forms.md` | 1347-1372 | The two router attributes |
| `product-and-forms.md` | 1373-1413 | The collapsible wrappers and the needs cards |
| `product-and-forms.md` | 1414-1462 | The sell_to gate, B2C_ALLOWED_PATHS and the /ai-crm miss |
| `product-and-forms.md` | 1556-1572 | /ai-crm in the catalogue, and why AEO is the default |
| `product-and-forms.md` | 1613-1626 | The maxlength mismatch, and the third page |
| `dashboard.md` | 542-554 | The Model tab as a surface |
| `dashboard.md` | 589-616 | The `ip_*` columns on `leads` |
| `dashboard.md` | 1120-1153 | The Visitors tab: tiles, circles, networks |
| `dashboard.md` | 1693-1713 | One row builder, two tables |
| `dashboard.md` | 1714-1748 | The Model tab's numbers, and Blocked counts |
| `dashboard.md` | 2080-2103 | The CSV export: phones and the open limit |
| `partnerstack.md` | 2199-2236 | Partner identity, the blocked lead's Slack post, the display chain |
| `partnerstack.md` | 2320-2348 | Partner revenue gaps |
| `partnerstack.md` | 2349-2366 | The step 10 poller and the three intervals |
| `partnerstack.md` | 2376-2400 | sf_state is a snapshot; first_ticked_at; unticks |
| `partnerstack.md` | 2401-2412 | Partner_Source__c, the first Opportunity write |
| `partnerstack.md` | 2465-2483 | Eligibility rule (a)'s contact sources, and rule (b) |
| `testing.md` | 925-971 | Source assertions, the confident zero, driving the thing |
| `testing.md` | 1529-1549 | The SQL that survived six suites, and /monitor/funnel |
| `testing.md` | 2259-2278 | A hand-run API read is not a better oracle |
| `attribution.md` | 1463-1501 | The shared cookie namespace and the in-app browser |
| `attribution.md` | 2511-2534 | How_Did_You_Hear__c and the substring formula |
| `attribution.md` | 2535-2551 | Source_Bucket_New__c, and the converted-lead join |
| `dropoff-and-digest.md` | 1988-2002 | "Left on step 2", and blocked verdicts leave no trace |
| `dropoff-and-digest.md` | 2003-2034 | Source, leads vs people, the bucket as text |
| `dropoff-and-digest.md` | 2035-2067 | The weekly digest |
| `internal-test-submissions.md` | 1773-1793 | The three harms, b@g.ai, the website gate |
| `internal-test-submissions.md` | 1804-1817 | The page is a signal: the staging host |
| `internal-test-submissions.md` | 1825-1832 | Nothing is lost when it fires |
| `alerting.md` | 1627-1648 | Apollo refuses with a reply |
| `alerting.md` | 1655-1685 | How a Meta CAPI failure reaches recordFailure |
| `meta.md` | 1052-1066 | The whole `x-forwarded-for` header was sent |
| `meta.md` | 1078-1084 | Meta never complains |
| `definitions.md` | 686-699 | The two completed-not-submitted populations |
| `definitions.md` | 854-877 | The internal-submissions measurement, and the /careers session rows |

## Every rule kept verbatim in CLAUDE.md

A script pulled the bold lead-in of each kept paragraph and list item: 161,
including the two restored blocks. Grouped by section:

**The working agreement: branch, push, PR, WAIT** (10): Never merge to `main` without being told to, in that message; Push the BRANCH; Open a PR; Stop; A green bar is not a review; `Bash(git push *)` is allow-listed in `.claude/settings.local.json`; The sixteen dependency-free suites are the bar; NEVER PIPE `measure.js`. Not through `tail`, not through `grep`, not through `head`. Run it bare and read all of it, or do not claim the bar; Nine of the sixteen BOOT A ROUTE; All sixteen suites require `tests/crash-reporter.js` first, and it is not optional

**Mutation testing: use `tests/measure.js`, do not count markers** (6): Do not measure a mutation by counting `✗` lines or by looking at the exit code; A mutation is CAUGHT only if the suite COMPLETED and FAILED and ran its USUAL NUMBER OF ASSERTIONS; An assertion about ORDER is not an assertion about REACHABILITY; A review card is trusted rather than checked, so a wrong one is worse than a missing one; And treat the mutation tallies in review cards and commits before 7 Sept 2026 as UNVERIFIED rather than as evidence; `test-sf-readers.js` is the one that EXECUTES rather than reads

**The one rule everything else follows** (14): A lead is worth more than a verdict; "We could not check" is never recorded as "we checked and it is bad."; Blocking is the highest-risk action in the codebase; There are now TWO server-side blocking mechanisms, and they are the exception that proves this rule rather than a loosening of it; brand-domain list; model layer; Suppressing a Meta event is a real cost, not a safe default; THREE things gate Meta today, and every one was surfaced as a decision rather than taken quietly; The third is `NON_ICP_LLM_META`, and it is a SEPARATE FLAG from `NON_ICP_LLM_BLOCK` on purpose; And two more gate it by WHO the lead is, not what the business is; ONE AGENCY LIST SINCE 7 OCT 2026: `AGENCY_DOMAINS`; Read `suppress_meta` off the verdict, never `blocked`; `Schedule` fires from THREE call sites, so it needs three guards; The three guards are now ONE function, `nonIcpScheduleSuppressed`, called three times

**Layout** (1): `gushwork-form.js` and `gushwork-form-popup.js` are in this repo, not a separate one,

**Deploying a form change — the Webflow step** (16): A `git push` does NOT ship a form change; each page's footer custom code; Why SHA and not `@main`; Use the full 40-character SHA; AND RETRY THE READS; AND SWEEP EVERY PAGE, not just the two you changed; Pin both files to the same SHA; Confirming the swap actually took; So bump the version on EVERY form change; THE BUMP IS FIVE EDITS, NOT THREE — CORRECTED 22 SEPT 2026; The two header edits are caught by a DIFFERENT test; And when the version did not move, these two are what actually discriminate; The SHA in the page HTML; `publish_site` IS ASYNCHRONOUS, and the first sweep after it will lie; Publishing ships the WHOLE SITE, not just your pages; `backfill-sf.js` is a kept tool, not dead code

**Tables** (7): `leads`; `enrichment_data`; `form_page_views`; `leads.non_icp_source` / `non_icp_checked_at` / `non_icp_llm_flagged`; FOUR SOURCE VALUES SINCE 23 SEPT, NOT TWO, AND EVERY CONSUMER THAT COMPARED AGAINST THE LITERAL `'llm'` ANSWERED WRONG FOR THE NEW ONES; Ask `nonIcpSourceIsModel(src)`, never `=== 'llm'`; `leads.ps_signup_recheck_at`

**The four nouns** (7): Session; Lead; Completed; `completed = true` does NOT mean "submitted the form", and `submitted_at` is not set everywhere `completed` is; "Did this person fill the form in?"; On the AWS mirror, `gw_form_leads.submitted_at` is a different thing entirely — a sync timestamp, not a submission time; Booked

**The stage ladder** (4): Booked; Disqualified; Completed; Step 1

**The PartnerStack lifecycle ladder** (11): qualified; qualification_failed; conversion_failed; demo_done_not_qualified; awaiting_demo; converted; skipped; conversion_pending; The order is not the progression order, deliberately; Keyed by DOMAIN, because that is the unit PartnerStack pays on; conversion_failed and qualification_failed are the two red states

**Bookings: two different questions, and they are not interchangeable** (3): 1. "Is this person an SDR target?" — no time comparison; 2. "Should this session get a drop-off recovery email?" — the time comparison is required; 3. "Recovered bookings"

**Default population for dashboard numbers** (5): OUR OWN TEST SUBMISSIONS ARE LEFT OUT OF THE OVERVIEW, DROPOFF AND THE MONDAY DIGEST — Darshil's decision, 26 Sept 2026; The three move TOGETHER; "Form entries per day" is deliberately a row count, not a people count; Webhook-origin leads; 10 rows on the mirror as of 5 Sept 2026, and all 10 are `rh_webhook`

**Timezone** (1): The dashboard is Eastern Time

**Things that will bite you** (76): Run that audit whenever you touch either column; The generalisable rule: a second column that means "we rejected this lead" is not additive; THE LATE-VERDICT SWEEP, AND WHY THE RACE WAS THE WRONG FRAME; IT MARKS AND TELLS A HUMAN. IT NEVER CANCELS; Only BLOCKING verdicts; Normalised in `sendEvent`, not at the call sites; `req.ip` IS ALSO THE WRONG ONE; THE ORIGIN CHECK ADMITS THE API'S OWN ADDRESS, EXACTLY — SINCE 27 SEPT 2026; What that broke; Why nothing caught it; The fix; Foreign origins still get the 500, deliberately; Geo is `ipwho.is`, NOT `ipapi.co`; The lookup NEVER touches the lead path; And the scope is narrower than "non-ICP" sounds; The three lists; `WEBSITE_VERIFIED_REASONS`; `RECHECK_WRITEABLE`; Backticks inside SQL comments break the file; `gushwork-form-popup.js` is a FORK of `gushwork-form.js`, not a sibling; A `//` COMMENT INSIDE SQL TAKES THE STATEMENT DOWN, and this already happened; Product tagging: AEO is the DEFAULT, and only the exceptions are listed; Adding a product; An unreadable `page_url` is NOT the default — it returns null and the event goes untagged; `Contact` is excluded from product tagging by EVENT NAME, not by its page; `predicted_ltv` is the same per product on every event; only `value` varies, and it is 0 on all three upstream events; `leads.product` and `gw_form_leads.product` hold the resolved slug; `about_business` is capped at 1000 chars, and the cap is NOT what protects the request; A lifted `recordFailure` needs every name it reads; `git checkout <file>` restores from HEAD, not from "before my scratch edit"; Our own test submissions are MARKED, never silently excluded; "MARKED, NEVER EXCLUDED" IS ABOUT OUR OWN NUMBERS, AND AS OF 19 SEPT 2026 IT STOPS AT OUR OWN NUMBERS; The AWS guard is INSIDE `syncToAWS`, not at its four call sites; Exact HOST match, never a substring; The dashboard marker and the guards ask the SAME question; Two copies of the label map; Two more pairs that must stay in sync; `/monitor/health` is a real probe, and slow on purpose; `/monitor/website-recheck` is a POST; `/monitor` is not one page; THE DASHBOARD IS `/monitor` SINCE PR D (26 Sept 2026), AND THE OLD ONE IS `/monitor/classic` FOR ONE WEEK; `/monitor/next` now 302-redirects to `/monitor`, keeping its query; Numbers come from the existing routes, plus ONE new read, `overviewReport` / `/monitor/overview`; Every repaint goes through `GW.paint`; nothing floats over content; Eleven tabs as of 25 Sept; THE DROPOFF TAB ANSWERS "where do the people who start the form go", AND ITS LADDER IS THE CONTRACT; THE CSV EXPORTS NEUTRALISE FORMULAS BUT NEVER TOUCH A PHONE NUMBER, and the second half is the hard part; `=`, `@`, a tab or a carriage return at the start; THE GOOGLE ADS CONVERSION UPLOAD — 6 Oct 2026, and its traps; Data Manager API, NOT the Google Ads API; Booking arrives by three routes; PartnerStack: `partnerStackCustomerKey` is the only place a domain becomes a customer key; A disqualified lead never fires a conversion, and that is a GUARD now, not a flow property; Every `/submit` logs whether a partner was present; The PartnerStack handover doc is `docs/partnerstack.md`; Two PartnerStack hosts, two auth schemes, one env; `hear_about_us`: an existing referral outranks a partner; A late-arriving single field needs its own targeted AWS write, NEVER `syncToAWS`; Every alert path gets FIRED ONCE ON PURPOSE before launch; Do not reduce `PS_VERIFY_GRACE_MIN`; `recordFailure(source, …)` is a SILENT NO-OP for any source with no `FAILURE_MONITORS` entry; An ACK means "this failure is understood, leave it alone" — not just "stop alerting me"; "Needs attention" is counted by its OWN unbounded query, not from the capped domain list beside it; A stale `partner_domain_sf_state` is a RED health row; Only domains whose conversion is VERIFIED (`ps_signup_verified_at IS NOT NULL`) can be qualified; Creating a Salesforce custom field through the Tooling API does NOT grant access to it; The automated eligibility check is BUILT AND OFF for the MVP; "Once per domain, ever" is enforced by the DATABASE, not by application code; PartnerStack eligibility FAILS CLOSED, and that is deliberate; PartnerStack eligibility runs AFTER `res.json()`, and must stay there; Step 5's conversion call goes after this verdict, not before it; Six verification columns never reach the AWS mirror; THE CHANNEL ATTRIBUTION LOGIC LIVES IN SALESFORCE, NOT IN THIS REPO — AND WE FEED IT; The consequence that will bite: `hear_about_us` is not just a display string; Known open bug

Kept whole as well:
- the Layout table;
- the jsDelivr URLs and the `curl` sweep;
- the test-command block;
- the stage and PartnerStack ladders;
- the `completed` / `submitted_at` table;
- Timezone;
- every other table or list under a heading above.

## Step 4: does it load without the warning?

- **Measured:** 122,171 characters, about 28k under the 150k limit.
  `/context` shows the whole file loading (46.6k tokens, against 65.7k
  before).
- **Not confirmed: whether the warning itself is gone.** The headless
  `/context` report showed no warning for the OLD file either, so it can't
  tell the two apart, and I'm not claiming it as a check.
- **To confirm:** start a fresh interactive session on this branch and look at
  the startup notices, or run `/memory`.

## Tests

`node tests/measure.js --check`, run bare, after the follow-up: **16 suites,
5,405 assertions, all passing**, the same as the baseline. Nothing in `tests/`
or `tools/` reads CLAUDE.md.

## What to check

1. **The three rewritten rules (lines 296, 1022, 1024).** Read them against
   `docs/background/non-icp.md`; they are still my summaries.
2. **The other 49 one-line rules** are unchanged from the first version and
   were not part of your five-rule review.
3. **Moved text is unedited,** so a "see below" inside `docs/background/` can
   point at nothing. Each block's heading gives its old line range at
   `b924ce7`.

## Not changed

- **No code, test or form file.** No re-pin is needed.
- **Existing docs are untouched.**
- **No kept sentence was reworded,** stale ones included ("all ten suites"
  under Mutation testing; Style's "one large file", which is about
  `index.js`).
