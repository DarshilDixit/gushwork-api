# The monitor dashboard rebuild — plan, status, handoff

**Read this first if you are picking up the dashboard work.** It is kept
current as the work moves, so a new session, or one resumed after a pause,
can carry on without anyone re-explaining. The rules in `CLAUDE.md` still
apply on top of everything here.

Last updated: **26 Sept 2026 (IST)**. #123 merged and its deploy confirmed; PR C started.

---

## The plan

The old dashboard at `/monitor` is being rebuilt as `/monitor/next`, which
runs **side by side** with it, behind the same token, until a separate PR
switches them.

| PR | What | State | Links |
|---|---|---|---|
| A | Apollo: page on out-of-credits, an honest System Health row, label fixes | **merged** 25 Sept | [#122](https://github.com/DarshilDixit/gushwork-api/pull/122), `cards/PR-122-apollo-honest-health.md` |
| B | The new dashboard, five tabs: Overview, System health, Dropoff, Duplicates, Lead magnet | **merged** 25 Sept 22:53 UTC (`5bb0479`), deploy confirmed | [#123](https://github.com/DarshilDixit/gushwork-api/pull/123), `cards/PR-123-monitor-next.md` |
| C | The other six tabs: All leads, Blocked, SDR list, Partners, Model, Visitors | **in progress** | branch `feat/monitor-next-c` |
| D | The switch: `/monitor` becomes the new page; the old one stays at `/monitor/classic` for a week, then goes | not started | — |

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
- **Unmerged**: PR C, on `feat/monitor-next-c`. See "PR C" below for exactly
  where it stands.

## Decisions waiting on Darshil

1. **Our own test addresses are in every Overview number.** This is the
   standing, undecided distortion in `CLAUDE.md` ("Internal / test
   addresses: Included"). They are marked on Duplicates and Lead magnet,
   never subtracted. Excluding them moves every historical number at once,
   so it is his call.
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
   declaring, as does every new component built for this. The #123 card
   lists them.
   - **Light muted text** is neutral-600, not the spec's neutral-500
     (3.41:1, under the 4.5 floor).
   - **The focus ring** is solid primary, not the 40%-alpha token (under 3:1
     on every surface).
   - **Badge labels** are at /600.
   - **The current nav row** is semibold as well as filled.

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
  combinations since the review fixes, all clean. Next: the keyboard
  section (`KEYS=only`), then the crosscheck.
- [x] **Review sweep** (five lenses, each finding adversarially verified;
  workflow run `wf_c2ad4b99-d16`). 45 confirmed, about 30 distinct fixes,
  commit `5bd3659`; the two first-round mutation survivors pinned in
  `7afa68d`. Headline: a session_id of "constructor" blanked All leads;
  every tab painted before loading, so stale rows sat under new filters;
  Custom dates were unreachable; the ack note was wiped by refreshes.
- [ ] Mutation tests: round 1 (26, on the build) 24 caught, 2 survived and
  now pinned (`7afa68d`). Round 2 (37, on the review fixes, list in the
  session scratchpad as `muts2.json`): R01-R05 run, all CAUGHT after one
  fix -- R02 was first UNMEASURED because the draw-guard test left a broken
  row behind that crashed the suite later (`db7851e`). **R06-R37 not run**:
  the run was stopped for memory. Run them in a git worktree so the
  preview keeps serving the real tree.
- [ ] Card, PR, stop for review
