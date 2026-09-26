# The monitor dashboard rebuild — plan, status, handoff

**Read this first if you are picking up the dashboard work.** It is kept
current as the work moves, so a new session, or one resumed after a pause,
can carry on without anyone re-explaining. The rules in `CLAUDE.md` still
apply on top of everything here.

Last updated: **26 Sept 2026 (IST)**. #123 and #124 (PRs B and C) merged, deploys confirmed. Every tab is rebuilt. **PR D waits on Darshil's decisions 1 and 5.**

---

## The plan

The old dashboard at `/monitor` is being rebuilt as `/monitor/next`, which
runs **side by side** with it, behind the same token, until a separate PR
switches them.

| PR | What | State | Links |
|---|---|---|---|
| A | Apollo: page on out-of-credits, an honest System Health row, label fixes | **merged** 25 Sept | [#122](https://github.com/DarshilDixit/gushwork-api/pull/122), `cards/PR-122-apollo-honest-health.md` |
| B | The new dashboard, five tabs: Overview, System health, Dropoff, Duplicates, Lead magnet | **merged** 25 Sept 22:53 UTC (`5bb0479`), deploy confirmed | [#123](https://github.com/DarshilDixit/gushwork-api/pull/123), `cards/PR-123-monitor-next.md` |
| C | The other six tabs: All leads, Blocked, SDR list, Partners, Model, Visitors | **merged** (#124, 26 Sept 11:13 UTC, `c83683a`); deploy confirmed | card `cards/PR-124-monitor-next-c.md` |
| D | The switch: `/monitor` becomes the new page; the old one stays at `/monitor/classic` for a week, then goes. Plus decisions 1 and 5 | **in progress** on `feat/monitor-next-d` | — |

Earlier, related: [#121](https://github.com/DarshilDixit/gushwork-api/pull/121)
(the Dropoff presets fix, 25 Sept).

## Current status

- **`/monitor`**: the classic dashboard. Unchanged except the small edits
  listed in the #123 card:
  - a link to the new dashboard;
  - `#tab=` honoured on load;
  - the Disqualified label;
  - the CSV formula escaping;
  - Duplicates' "ours" marker.
- **`/monitor/next`**: the new dashboard, LIVE in production since #123.
  Its five rebuilt tabs are Overview, System health, Dropoff, Duplicates and
  Lead magnet. Every other nav row is a marked link to the classic tab.
- **Deploy check, read-only, right after the merge** (live about 40s after
  it):
  - `/monitor/next` returns 200 with the new markup.
  - Production's `/monitor/overview` and the preview's lifted copy, asked
    for the same instant, return IDENTICAL numbers for Today, This week and
    All time.
  - The Apollo check carries its new vendor-free `summary`, and System
    health keeps the full text.
  - `/monitor/duplicates` returns `is_internal` (338 addresses).
  - `/monitor/metrics` keeps the same 29 keys.
  - A before/after diff of the classic `/monitor` page shows exactly the
    8 intended fragments and nothing else.
- **#124 (PR C) merged** 26 Sept 11:13 UTC as `c83683a`, on Darshil's word.
  Deployed about 32 seconds later. Confirmed read-only:
  - the classic `/monitor` is **byte-identical** to before the deploy (the
    page, with the token masked, compared byte for byte), and
    `/monitor/metrics` has the same 29 keys;
  - **all 13 views** of `/monitor/next` (11 tabs, Overview in all three
    periods) load on production with live data: no error, nothing left
    loading, no failed or non-GET request;
  - the crosscheck against the classic, pointed at production: **45 of 45**
    (the first run failed 4 live-partition checks because a lead arrived
    mid-run; see learnings).
- **PR D started 26 Sept** on `feat/monitor-next-d` (from the docs branch,
  so this file rides in it), after Darshil decided items 1 and 5: exclude
  our tests from Overview, Dropoff and the digest, and build the CSV rule
  as proposed. Progress is under "PR D" at the end of this file.

## Decisions waiting on Darshil

1. **DECIDED 26 Sept (Darshil): our own test addresses are left out of the
   Overview, Dropoff and the digest, and counted in words there; kept and
   marked where rows are listed.** Built in PR D. Kept below for the record
   of what was measured before deciding. They were in every Overview number
   until then, marked on Duplicates and Lead magnet, never subtracted.

   **Measured 26 Sept, read-only.** The real `overviewReport` was run
   twice, the second time with every read of `leads` swapped for "leads
   minus `internalLeadSqlClause`" (the rule behind the "ours" marker). The
   plain run matched production's `/monitor/overview` exactly. Ours are
   **107 lead rows, 32 addresses**, 1.8% of all rows.

   | Overview | With ours | Without | Moves by |
   |---|---|---|---|
   | All time, leads | 5,848 | 5,741 | 107 (1.8%) |
   | All time, people | 5,343 | 5,315 | 28 (0.5%) |
   | All time, booked rate (people) | 68.20% | 68.33% | 0.13 points |
   | All time, blocked leads | 50 | 44 | **6 (12%)** |
   | This week, people | 308 | 307 | 1 |
   | Last week, people (the comparison) | 223 | 219 | 4, so the week-on-week change reads +38.1% not +40.2% |
   | Today | same | same | 0 |

   Rates barely move (at most about 0.25 points). Small counts do:
   Blocked, and testing days. The worst days were 23 Mar (9 of ours), and
   18 and 30 Jun (7 each). March was 18 of 45 leads, 40%. Sessions cannot be
   separated by address, because a session has no email.

   **Recommendation: EXCLUDE from the Overview (and Dropoff), keep them
   marked everywhere rows are listed.**
   - The Overview describes real demand, and our tests are not demand.
   - The outbound systems already refuse them (Meta, Salesforce and the
     dialer, since 19 Sept), so the dashboard is the odd one out.
   - Moving history costs little: no rate moves by more than about 0.25
     points, and no trend changes.
   - Dropoff must move with it, or the documented "Overview and Dropoff
     agree exactly in Leads mode" breaks, and so must the Monday digest,
     which reads Dropoff.
   - The Overview should say it in words: "excludes N of our own test
     submissions".
   - All leads, Blocked and Duplicates keep them, marked, with the filter.
     Those tabs are where a person reconciles a row.
2. **Apollo top-up.** Apollo has been out of credits since 23 Sept. System
   Health is red and pages every 3 hours by design. He said on 25 Sept he
   was not topping up yet.
3. **Re-enrich scope, once credits exist** (`tools/re-enrich-apollo.js`,
   dry run by default, not yet run). There are two options:

   | Scope | Addresses | Credits |
   |---|---|---|
   | Since 23 Sept | 131 | ~107 (at most 131) |
   | All three outages | 383 | ~314 (at most 383) |

   Apollo charges 1 credit per person FOUND and 0 for no match; the recent
   match rate is 82%. 16 addresses can be copied from an earlier answer for
   free.
4. **The notice to Utsav** (design system). The departures below need
   declaring, as does every new component built for this. The #123 and
   #124 cards list them. #124 adds: the stacking plain table (`U.grid`),
   the card-view Sort select, the inline acknowledge form, the themed map
   controls, and one new alias, `--map-sea` (neutral-200 in light,
   neutral-900 in dark, the tokens nearest the basemaps' sampled ocean).
   - **Light muted text** is neutral-600, not the spec's neutral-500
     (3.41:1, under the 4.5 floor).
   - **The focus ring** is solid primary, not the 40%-alpha token (under 3:1
     on every surface).
   - **Badge labels** are at /600.
   - **The current nav row** is semibold as well as filled.

5. **CSV formula escaping on the All leads and SDR exports.** Proposed 26
   Sept; **not built.** Today those two server exports (the routes both
   dashboards call) only quote commas, quotes and newlines. A cell starting
   with `=` or `@` runs as a formula when the file is opened in Excel or
   Sheets. The Lead magnet export's blanket rule (escape `= + - @`) cannot
   be copied here: measured read-only, **2,669 of 2,676 stored phone
   numbers start with "+"**, so it would put an apostrophe on 99.7% of the
   phone column a dialer imports.

   **The rule:** one `csvCell` shared by both exports.
   - A cell starting with `=`, `@`, a tab or a carriage return ALWAYS gets
     a leading apostrophe.
   - A cell starting with `+` or `-` gets one UNLESS it is only a number:
     digits, spaces, `( ) . -`, and at least one digit.
   - Then quote on a comma, a quote, or `\r`/`\n` (`\r` is missing from
     the quoting today).

   **In real data:** every stored phone shape passes through untouched
   (`+99999999999` x2,493, `+999999999999` x163, `+99 99999 99999`, and the
   rest). Only 4 real values would get the apostrophe: 2 "heard about us"
   answers starting with "-", 1 website and 1 company starting with "@".
   Nothing stored starts with "=".

   | Value | The CSV (what a dialer importing it reads) |
   |---|---|
   | `+19495550123` | `+19495550123` (unchanged) |
   | `+44 20 7946 0958` | `+44 20 7946 0958` (unchanged) |
   | `+1 (949) 555-0123` | `+1 (949) 555-0123` (unchanged) |
   | `-5` | `-5` (unchanged) |
   | `=HYPERLINK("http://evil.test","Click")` | `"'=HYPERLINK(""http://evil.test"",""Click"")"` |
   | `@SUM(A1:A9)` | `'@SUM(A1:A9)` |
   | `+cmd\|' /C calc'!A0` | `'+cmd\|' /C calc'!A0` |
   | `- google search` | `'- google search` |
   | `@acme studio` | `'@acme studio` |

   **Two caveats to decide with:**
   - An apostrophe is hidden by Excel and Sheets, but a plain importer
     keeps it as a real character. That is fine for the 4 odd values
     above, and it is why the rule leaves numbers alone.
   - Excel and Sheets ALREADY turn `+19495550123` into the number
     19495550123 and drop the "+". That is a spreadsheet habit, not ours,
     and this proposal neither fixes nor worsens it. Keeping the "+" in a
     spreadsheet would need the apostrophe, which breaks the dialer. One
     file cannot do both, so this keeps the file right for the dialer.
     The Lead magnet export has no phone column; bringing it onto the same
     rule is a separate one-line choice.

## Rulings so far (Darshil's, and they stand)

- **Side by side.** `/monitor/next` beside `/monitor`, same token. The old
  one keeps working the whole time. The switch is its own PR (D).
- **No framework, no build step.** Real files under `monitor/`: CSS, and
  classic scripts on one `GW` namespace, stitched into one page by
  `monitor-next.js`.
- **The Gushwork design system** (gushwork-design v1.49.0 tokens and fonts).
  New components are allowed where the system has none or a weak one, but
  the theme and fonts stay the same.
- **Themes:** light, dark, and follow-the-computer.
- **The Overview** toggles Today (live) / This week vs last week / All time,
  and People vs Leads. The previous period is a faint grey bar behind the
  blue one.
- **Refresh:** Today every minute, everything else every 5 minutes, paused
  while the tab is hidden.
- **Responsive:** 360, 390, 414, 768, 1024 and 1440px, with 1280 checked as
  well. No sideways scroll, 44px tap targets, and nothing floating over
  content (no bottom dock).
- **Nothing changes which leads are blocked or which Meta events fire.**
- **Merge only on his word, in that message.** He gave it for #123 on
  26 Sept, conditional on a green bar and layout check. PR C is NOT to be
  merged; stop for his review.
- **Quality over speed.** "Take the time C needs."

## Learnings

- **The crosscheck's live-partition half can fail on a moving total.** It
  reads the all-leads total first and the partitions after it. A lead that
  arrives in between makes each partition sum one higher, and four checks
  fail together (26 Sept, on production: 5,847 against 5,848). The
  same-bytes half cannot be affected. A re-run passed 45 of 45. If it
  recurs, re-read the total at the end and report "moved" rather than fail.

### Bugs the tests missed, and why

- **"Last week" bars were always zero, and the test passed.** The test
  checked a `--prev` bar existed; a zero-height bar is still `<path d="">`.
  The server keyed last week by its own dates and the page looked it up by
  this week's. Rule: count bars WITH a shape, and read values back out of
  the painted table.
- **"Page loads" counted sessions.** A definitions error, not a code one,
  found by a reviewer reading CLAUDE.md's four nouns against the query.
  Every label is a claim about a population; check it against the
  Definitions.
- **Two layout bugs only an eye could see:** phone comparison bars shrunk to
  about 70px by a leftover media-query rule, and collapsed rows showing in
  card view because `display:block` beat `[hidden]`. The layout checker was
  green for both.
- **The sidebar "stops partway down" was invisible to the checker,** which
  sizes the viewport to the whole page, so 100vh was the page. Measure at a
  real window height, scrolled to the bottom.
- **The preview served new CSS around old markup** for one run. It re-read
  CSS and scripts per load but built `page()` once. It now re-requires
  `monitor-next.js` on every load.
- **The keyboard check's first run failed on the harness, not the page.**
  Navigating to the same URL with a new `#hash` is not a page load. It now
  loads `about:blank` first.
- **One mutation survived because two guards did the same job.** Removing
  one left the other working. Mutate both at once to prove the behaviour,
  and write down why the single one survives.

### Checks that proved their worth

- **The five-lens review workflow:** numbers, code, visual, wording and
  accessibility, each finding adversarially verified. 79 confirmed
  (including the zero bars), 4 refuted.
- **`tools/check-monitor-layout.mjs`**: every width and theme, plus real key
  presses.
- **Reading the screenshots by eye**, after every layout run.
- **Mutation testing via `measure.js --mutation`**: 14 on B, 7 on its
  follow-up, and 2 on the keyboard checks themselves.
- **The live-data preview**: this branch's queries against production data,
  on connections read-only at the database.
- **`tests/test-monitor-next.js`**: runs the page's own served JavaScript in
  a stubbed DOM, through its real click and input handlers, and compares
  every painted number with the payload.

### Rules to keep

- Read the screenshots by eye. A green layout check is not "it looks right".
- Mutation-test every new guard. Commit first; restore by COPY, never
  `git checkout`.
- Never pipe `measure.js`, and read `--save`'s output too, not only
  `--check`.
- Production reads only read-only: the preview, or
  `SET default_transaction_read_only = on; BEGIN READ ONLY; … ROLLBACK;`.
- **Restart the preview after changing `index.js` or
  `tools/preview-monitor.js`**, because it lifts code once, at start. A
  route the branch CHANGES must be served by the preview from the branch
  (lift it, as with `overviewReport` and `duplicatesReport`), never proxied
  to production, or the screenshots show the old query.
- Screenshots with real lead data stay in the scratchpad, never the repo or
  a published page.

## How to run things

```bash
# the branch against live data, read-only (needs both railway services):
railway run -s Postgres bash -c 'PUB="$DATABASE_PUBLIC_URL" railway run --service gushwork-api bash -c "DATABASE_URL=\"\$PUB\" PREVIEW_READY_FILE=/tmp/preview-ready.json node tools/preview-monitor.js"'
# then, with the preview up:
PREVIEW_READY_FILE=/tmp/preview-ready.json OUT=<scratch dir> WIDTHS=360,390,414,768,1024,1280,1440 node tools/check-monitor-layout.mjs
node tests/measure.js --check      # the bar, bare
```

Never print the token; the ready file holds it for the tools.

## PR C — where it starts and what it needs

**Tabs:** All leads, Blocked, SDR list, Partners, Model, Visitors.

**Branch:** `feat/monitor-next-c`, from `origin/main` after #123 merges.

**Step 1: understand before building.** For each tab, write down:
- the classic loader, and every route and parameter it calls;
- every WRITE it can make (Partners has acknowledgements, and there may be
  others; the preview refuses every non-GET);
- the CLAUDE.md sections that govern it.

**Step 2: build.** Watch for these per tab:
- **All leads + Blocked**: one row renderer, `leadRowsHtml`. Rebuild them
  together with NAMESPACED row keys (CLAUDE.md, "ONE ROW BUILDER, TWO
  TABLES"). Blocked counts carry units: leads vs people.
- **SDR list**: `SDR_SEARCH_COLUMNS` (server) and `SDR_SEARCH_FIELDS`
  (client) must stay equal. The CSV export filters server-side.
- **Partners**: the eight-state PartnerStack ladder. "Needs attention" comes
  from its own unbounded query and says "AT LEAST" when incomplete. Acks are
  writes.
- **Model**: `/monitor/non-icp`. The five-state ladder sums to the lead
  total. The industry table is split by what decided. The scrape panel is
  "latest outcome per domain".
- **Visitors**: `/monitor/visitors`. Leaflet with ArcGIS Canvas tiles (the
  other two tile providers were rejected, and why is in CLAUDE.md). Circle
  AREA scales with count. Networks are merged by name in the browser.
- **Nav:** flip each tab from a classic link to local as it lands.
- **Audits:** the `test-non-icp.js` 10b/10f audits fire if a new query
  touches `disqualified`, `non_icp_blocked` or the model flag.

**Step 3: the bar, the same as B:**
- **Numbers**, number for number against the classic tab on live data,
  read-only: both pages served by the preview, painted values compared.
- **Layout check** at all widths, both themes, with the new tabs added to
  `PAGES`.
- **Screenshots** read by eye.
- **Mutation tests** on every new guard.
- **A full review sweep**, the five-lens workflow, then the fixes.
- **Then** the full bar bare, the card, push, open the PR, and **stop for
  review. Do not merge C.**

### PR C: calls made while building (each easy to reverse; all go on the card)

**No server change in PR C.** Every tab reads its existing route, and the
preview proxies them all unchanged. These things would need one and were NOT
done; they are listed for Darshil instead:

- **Formula escaping on the All leads and SDR CSV exports.** A leading "+"
  would get an apostrophe, which corrupts phone numbers for any dialer that
  imports the file.
- **An "ours" marker** on SDR, Visitors and Partners.
- **Partners' row-level quirks:** Needs attention counts failing rows only,
  and Check A ignores acknowledgements.
- **Model's Decisions rows** borrow an industry from a non-deciding domain.
- **Unbounded totals** behind the Visitors lists.

**Display-only calls:**

- **"Step 1" reads "Left on step 2"**, on All leads, Blocked and SDR,
  following CLAUDE.md's LEFT ON STEP 2 rule. The ladder itself is
  unchanged.
- **Wording that was false for some rows is corrected:**
  - the Meta chip's "still reaches Salesforce", which is false for blocked
    and internal leads;
  - "B2C" meaning every disqualified lead;
  - Blocked's "turned away before the calendar", which is false for
    late-verdict (llm_late) leads;
  - the SDR count saying "leads" when it counts people;
  - Visitors' "Person location = company", when the parser reads Apollo's
    PERSON record (CLAUDE.md is corrected too).
- **Raw slugs get plain labels,** with the slug kept in a tooltip: partner
  failure reasons, scrape statuses and block sources. The labels live in
  ONE new-dashboard file, pinned by tests against the server and classic
  copies.
- **Partner revenue gaps moves to the Partners tab.** Classic hid it inside
  the All Leads table header. It stays a work queue: never System health,
  never the attention strip.
- **The Partners ack no longer uses a browser prompt.** It is an inline
  note and Acknowledge / Cancel; Cancel now does nothing, where classic
  acknowledged anyway. The POST is unchanged: a JSON body with the boolean,
  the token in the query.
- **"ticked, will fire next poll" is shown only when the conversion is
  VERIFIED;** otherwise it says the $50 cannot fire yet.
- **Filters live in the URL hash:** All leads filters, Model days and
  product, Visitors window and map. So a link says what it shows, and
  Partners' drill-down opens All leads filtered on that partner.
- **Refresh:** every 5 minutes on every tab, paused while hidden, keeping
  open rows and loaded change logs. The partner-gaps list keeps its own
  10-minute cadence.
- **Visitors loads Leaflet only when the map is first opened,** pinned
  1.9.4 with an SRI hash; the tables are the fallback. The map survives
  repaints (one container, re-attached). The zoom control is 44px on
  touch. One-finger drags scroll the page on phones.
- **All leads gains the "our own tests" filter** the route already
  supports. The 🧪 tooltip used to point at a filter that did not exist.
- **From the review (display only):**
  - the Meta chip reads **"site not verified"**, never "no website": the
    reason covers timeouts and 403s from live sites;
  - website verdicts read as the server words them (`http_403` is "Site
    blocked our check (403)"), pinned against `websiteReasonLabel`;
  - the model and website Meta reasons no longer claim the lead "books and
    is dialled" -- it may be disqualified or never have submitted; the
    Model tab's Meta group note is overridden in the page for the same
    reason (the server's note still says it; that is a server change);
  - in flag-only mode (`NON_ICP_LLM_META` off) the decision chip, the group
    title and the lede say Meta was still sent, as the ladder already did;
  - the SDR date column is "Newest qualifying attempt", the partner card
    "with a lead in the last 24 hours", the map note no longer claims an
    exact area ratio;
  - a Model row whose page read fine is "page read; the model call failed";
  - card view gets a Sort select for All leads and Per partner; every
    column hidden at mid widths is in its row's detail; the ack note and
    competing partners are visible text, not only a tooltip;
  - the map follows the theme (dark Esri basemap, checked by downloading
    tiles), keeps the reader's zoom across refreshes, and colours the area
    past the world's edge as the tiles' own sea.

### PR C progress

_(kept current as the work moves)_

- [x] Understand pass, per tab. Five read-only agents wrote a build spec
  each. They live in this session's scratchpad (`specs/*.md`); if lost,
  re-run the same read (routes, writes, UI, rules, traps, crosscheck and
  tests per tab) before building.
- [x] Build:
  - [x] **Foundation.** Tab filters in the hash (`G.S.q`); JSON bodies
    for writes; dates that never throw; http(s)-only links; the server's
    label maps carried in the page config; `labels.js`; sortable headers,
    row attributes and a pager in `ui.js`.
  - [x] **All leads and Blocked** (`leads.js`): one renderer, namespaces
    `ld` and `blk`. Checked on live data through the preview: 5,822 leads
    in 233 pages; Blocked shows 51 leads, 42 people and 45 leads excluding
    ours. Tests: painted numbers, markers, panel order, the change log's
    three states by the raw session_id, filters to the request, CSV.
  - [x] **SDR list** (`sdr.js`). 1,027 people to call on live data. The
    search fields are held equal across the server, the classic and the
    new tab; the search is trimmed as the server does it; the export
    carries the same term.
  - [x] **Layout check gained `hscroll`:** a data table wider than its
    card. It found the SDR list's cut-off last column at 1440. Columns
    marked `opt` hide in the 900–1099px content band, their values still in
    the row detail.
  - [x] **Model** (`model.js`). Renders `/monitor/non-icp` without
    recomputing it. The standing cache comes from the TOP-LEVEL `d.cache`;
    all three groups always show. "Meta withheld" becomes "Meta still sent"
    in flag-only mode. The decisions and unreadable lists page at 25. Live:
    375 leads in 7 days, 47 decisions.
  - [x] **Visitors** (`visitors.js`). Leaflet loads only when the map
    opens, pinned 1.9.4 with SRI; Esri Canvas tiles; circles by area,
    largest first, styled by class (tokens). ONE map container, re-attached
    after each repaint. Networks are merged by name in the browser. The
    tests run the drawing code through a stand-in Leaflet. Live: 1,344
    leads in 30 days, 124 places drawn.
  - [x] **Partners** (`partners.js`). The eight-state ladder, the
    Salesforce splits, the funnel (absolute headline, stage-to-stage rates,
    nothing below 10), partner revenue gaps (moved here), per company and
    per partner. The ack is inline and Cancel sends nothing; the POST's JSON
    body is test-driven. The drill-down opens All leads filtered on that
    partner. Live: 8 partner companies, 5 conversions, 2 qualified.
- [x] **Number-for-number checks against the classic, live, read-only**
  (`tools/crosscheck-monitor.mjs`, commit `6181ddf`). Each payload is
  fetched once and served to BOTH pages; 45 checks, all green -- counts,
  row order, stages, the Model ladder and cache, coverage, merged
  networks, every Partners card and funnel headline, plus the partitions
  (stages, blocked, Meta, ours, website, enrichment, repeats) against the
  live total. A deliberate mutation (networks merged by the wrong key) was
  caught and reported. Run it against the preview; it prints numbers only.
- [x] **Layout check, screenshots by eye.** The check now also measures
  plain tables and opens rows (`tab+open`), and Partners is in its default
  list (it was missing). Found and fixed: four plain tables scrolling
  sideways on phones (now `U.grid`, stacking below 560); the rtable card
  rules leaking into tables nested in a detail row; detail links 15px tall
  on touch; the map light on the dark theme (dark Esri basemap, checked by
  downloading tiles); a doubled line under the last row. Light, six widths:
  137 combinations, 0 findings. Read by eye: Partners, Model, Visitors and
  the map, the lead panel's change log, in both themes, phone and desktop.
  **Final run after the review fixes (26 Sept): 288 combinations, 0
  findings** -- every width 360-1280 in both themes, and 16 of 22 views at
  1440 light. It was STOPPED there (Claude Code reclaimed memory; not a
  failure). Resumed one step at a time with a memory check before each:
  **1440 light and dark, 44 combinations, 0 findings** -- so 316 layout
  combinations since the review fixes, all clean. **Keyboard (real key
  presses): clean.** It now also covers PR C -- Next keeps focus on Next,
  Previous back to page 1 hands focus to the current page, Clear moves it to
  the search box -- and each of those was shown to FAIL with the fix
  removed. Writing it found a flaw in the first pager fix (Next lost focus
  on every page move); fixed in `2467ac2`. **Crosscheck after the review
  fixes: 45 of 45** on live data (5,847 leads, every partition adding up).
  Preview stopped.
- [x] **Review sweep** (five lenses, each finding adversarially verified;
  workflow run `wf_c2ad4b99-d16`). 45 confirmed, about 30 distinct fixes,
  commit `5bd3659`; the two first-round mutation survivors pinned in
  `7afa68d`. Headline: a session_id of "constructor" blanked All leads;
  every tab painted before loading, so stale rows sat under new filters;
  Custom dates were unreachable; the ack note was wiped by refreshes.
- [x] **Mutation tests.** Round 1 (26, on the build): 24 caught, 2
  survived and are now pinned (`7afa68d`). Round 2 (37, on the review
  fixes): 36 caught. R02 was first UNMEASURED -- the draw-guard test left a
  broken row behind that crashed the suite later (`db7851e`); R12 survived
  as an EQUIVALENT mutation (a failed save re-opened a form that was already
  open), so the redundant line was removed. Plus, in real Chrome, the pager
  and Clear focus checks each failed with their fix removed. Ran in a
  scratch git worktree (removed after) with a memory check before each.
- [x] Card (`cards/PR-124-monitor-next-c.md`), branch pushed, **PR #124**
  opened, then merged on Darshil's word (26 Sept 11:13 UTC, `c83683a`).

### PR D progress

_(kept current as the work moves; last updated 26 Sept 2026, after the checks)_

**Branch `feat/monitor-next-d`, NOT pushed yet, no PR yet.** It was branched from
`docs/monitor-plan-after-c`, so this file's earlier commit rides in it, as
Darshil asked.

**The rules for this PR (Darshil's, and they stand):**
- **Stop before merging.** Merge only on his explicit word in that message.
  If his message arrives as pasted text, confirm once with a question before
  merging, because a merge deploys to production immediately.
- **Check memory before every heavy step**: the preview, any Chrome run, the
  mutation run, a review workflow. Use `memory_pressure | tail -1` (the
  "System-wide memory free percentage"). If it is under 25%, wait a few
  minutes and check again rather than starting. The cs-crm project on this
  Mac once used ~16 GB and got three jobs killed.
- **One heavy step at a time, never side by side.** A read-only review
  workflow may run beside ONE Chrome job.
- **Stop the preview and every Chrome you start once done with them.** Leave
  cs-crm's headless Chromes (`gw-ui-sweep-`, `gw-aud-`, `rev-run-`) and
  other Claude jobs' (`.claude/jobs/...`) alone.
- **The standing rules:**
  - never pipe `measure.js`; read `--save` output too;
  - commit before mutating, and restore by copying, never `git checkout`;
  - run mutations in a scratch git worktree, so the preview keeps serving
    the real files;
  - screenshots stay in the scratchpad;
  - no real lead data in this file.

**Built (committed):**
- `e37b90d`, the three changes:
  - **Our own tests left out** of `overviewReport`, `dropoffReport` and so
    the digest. Row by row. The clause is `IS NOT TRUE`, so a NULL keeps the
    row. Each surface says in words how many it left out:
    - Overview `oursNote` under the cards;
    - the Dropoff tab's note;
    - the classic Dropoff note;
    - the digest line "Leaves out N of our own test submissions from last
      week."

    `recoveredBookingsSql()` is now a function; `/monitor/metrics` calls it
    bare, so its SQL is unchanged.
  - **`csvCell`** for the All leads and SDR exports.
  - **The switch:**
    - `/monitor` is the new page (`monitor-next.js` mount);
    - `/monitor/next` gives a 302 to `/monitor` keeping its query;
    - the classic is `/monitor/classic`, with a fallback notice and a
      "Back to the dashboard" link;
    - the new page's footer and `CFG.classic` point at `/monitor/classic`.
- `bed4d96`:
  - CLAUDE.md updated: the switch, the test-address decision, the CSV rule,
    and the digest line;
  - the preview serves this branch's `dropoffReport` on its read-only pool,
    and `/monitor/classic` from production (falling back to production's
    `/monitor` on a 404, before the deploy);
  - the preview lifts `recoveredBookingsSql`.
- `066d099`, old links still land:
  - the layout check's key section proves in real Chrome that
    `/monitor/next?token=…#tab=leads&view=week&unit=leads` and `#tab=overview&view=all&unit=leads`
    land right, and that an old classic-style `/monitor#tab=blocked` opens
    Blocked;
  - the suite checks every classic tab name is registered on the new page.

**After the compaction** (`92569da`, the review fixes):
- The review workflow's agents hit the usage limit, so its findings were
  checked by hand. Three were real and are fixed with tests:
  - the "ours" marker said "counted in every total";
  - Dropoff counted rows with no email (0 exist; aligned with the Overview);
  - the CSV docs over-claimed what Excel shows.
- The `;`-locale CSV limit (0 values affected) is left as Darshil's rule and
  raised on the card as his call.

**Verified, on the final code:**
- the bar, bare: 4,970 assertions, 0 failures, baseline saved;
- the real database, read-only: Overview 322 = Dropoff 322 this week in
  Leads mode, both leaving out 1;
- the crosscheck: 45 of 45;
- the layout check: 0 findings across every width and theme (128 + 224
  combinations);
- keyboard and old links: clean;
- the screenshots, read by eye;
- the mutation run over 38 guards, in a scratch worktree (results on the
  card).

**Status (27 Sept):** **PR #125 is open**; the card is
`cards/PR-125-monitor-next-d.md`. **STOP: merge only on Darshil's explicit
word.**

**Final checks:**
- two review passes, with every finding fixed;
- 43 mutations: 41 caught, D27 caught by two suites while four stop at a
  lift marker, and D38 in the preview tool, which no suite covers;
- the bar: 4,979 assertions, 0 failures.

**Opened the same day, separate from the dashboard:**
- **#126:** the non-ICP name-only floor. **Changes blocking and Meta**, as
  decided.
- **#127:** the Apollo backfill carry-out tool and the health-rate fix.
  Its writes are already done.

**All three touch `index.js`, `CLAUDE.md` and `tests/.baseline.json`.**
Merge them one at a time and rebase the rest after each; the baseline file
will conflict every time. Re-save it from a bare bar, never by hand.
After the merge:
- confirm read-only that `/monitor` serves the new page, `/monitor/next`
  redirects, and `/monitor/classic` is the classic;
- then the classic's removal PR, one week later.

**Answered on the side, for the record:**
- Production's Apollo key ends **…zEuA**. Its scope cannot name its owner
  through the API.
- Darshil found another workspace with **10,264 credits** (resets 3 Oct
  4:08 PM IST, shared with the team's exports). Our need is about 30
  credits a day, plus a 107 to 314 one-off backlog.
- The options offered:
  - swap `APOLLO_API_KEY` to a key from that workspace, no code needed;
  - or a fallback-key PR, done separately after D;
  - and check whether …zEuA is listed in that workspace: a per-user credit
    limit is possible.
- Nothing is decided or built on this yet.

**Scratchpad** (`S=/private/tmp/claude-501/-Users-darshil-code-gushwork-api/e28266e9-34f8-4346-94c7-2bdc5f0cbc25/scratchpad`):
- `preview-ready.json` (holds the token; never print it);
- `mutate.py` (driver with memory gate; `MUT_REPO` points it at a worktree);
- `verify-d.js`, `testaddr.js`, `csvscan.js` (the read-only real-data checks);
- `prodtabs.mjs` (every tab on production, read-only).

Preview command (from the repo root):
`railway run -s Postgres bash -c 'PUB="$DATABASE_PUBLIC_URL" railway run --service gushwork-api bash -c "DATABASE_URL=\"\$PUB\" PREVIEW_READY_FILE=$S/preview-ready.json node tools/preview-monitor.js"'`
