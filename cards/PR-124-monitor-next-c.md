# PR 124 — the other six tabs of `/monitor/next` (PR C)

Branch `feat/monitor-next-c`. Written 26 Sept 2026. PR C of the monitor plan
(`docs/monitor-plan.md`). **Not merged — waiting for your review.**

The build is in five commits, one per tab group (`510f9de` to `da5eaea`).
After it:
- `6181ddf`: the crosscheck, and the phone fixes it and the widened layout
  check found;
- `5bd3659`: everything the review confirmed;
- `7afa68d` and `db7851e`: tests that mutation testing showed were
  missing or weak;
- `2467ac2`: the pager, fixed a second time;
- `77e47d3`: a no-op line removed.

## What it does and does not touch

- **Every tab of the new dashboard is now rebuilt.** PR B did five. This PR
  does the other six: All leads, Blocked, SDR list, Model, Visitors and
  Partners. No sidebar row leaves for the classic page any more. The footer
  link back to it stays until PR D.
- **No server route is added or changed.** Every new tab reads the route that
  already exists. The one line changed in `index.js` passes the server's two
  label maps (`WEBSITE_REASON_LABELS`, `META_WITHHELD_LABELS`) into the page's
  config, so the browser has no third copy of them.
- **Nothing here changes which leads are blocked or which Meta events fire.**
  The lead path is not touched.
- **The classic `/monitor` is unchanged.**
- **The one write** on these tabs is the Partners acknowledgement. It sends
  the same POST as the classic (a JSON body, with the flag as a real true or
  false). Cancel now sends nothing; the classic's browser prompt acknowledged
  even on Cancel. The preview refuses every POST, so the write has only been
  run in the test suite.

## The six tabs

- **All leads** — one row per form session, 25 a page, paged on the server
  like the classic. Every classic filter, plus "our own tests", which the
  route supported and the classic's tooltip pointed at but never offered.
  Sortable headers, and a Sort select in card view (no header row there). The
  lead panel opens with *why this row is marked*, in words, then the partner
  box, the visitor's IP location, Apollo's record, the journey, and the change
  log (fetched by the raw session id, three states: loading, loaded, could
  not be read).
- **Blocked** — the same renderer on `nonicp=only`, with its three figures
  each labelled with its unit (leads, people, leads excluding ours) and the
  chip saying *which check* blocked each row.
- **SDR list** — one row per person, counted in people. The search matches
  exactly the server's four export fields, trimmed the same way, so the CSV
  exports what is on screen (the classic did not trim on screen).
- **Model** — renders `/monitor/non-icp` without recomputing it: the flags,
  the five-row ladder, all three groups (empty ones included), every
  decision with its quote, what could not be read, near misses, the cache.
  The all-time cache reads the TOP-LEVEL field, the 15 Sept "0 companies"
  incident.
- **Visitors** — coverage, repeat addresses, places, networks (merged by
  name), time zones, and a map. Leaflet loads only when the map is opened,
  pinned with an integrity hash.
- **Partners** — the eight-state ladder, the Salesforce splits, the funnel,
  partner revenue gaps (moved here from the All leads header), every company
  and every partner. The acknowledgement is an inline note with Acknowledge
  and Cancel.

## Calls I made (display only, each easy to reverse)

- **"Step 1" reads "Left on step 2"** on All leads, Blocked and SDR, per
  CLAUDE.md's rule that a lead row only exists after step 1 is done.
- **Wording that was false for some rows is corrected:**
  - "still reaches Salesforce" on the Meta chip (false for blocked and
    internal leads);
  - "B2C" meaning every disqualified lead (378 of 683 asked for the
    waitlist);
  - Blocked's "turned away before the calendar" (false for late verdicts);
  - the Meta chip "no website" for a site we simply could not check — now
    "site not verified";
  - "it books, reaches Salesforce and is dialled" on the model and website
    reasons (the lead may be disqualified or never have submitted). The
    Model tab's Meta group note says the same on the server; the page
    overrides it, and the server copy is left for a server PR;
  - the SDR date is "Newest qualifying attempt", the partner card "with a
    lead in the last 24 hours", the map note no longer claims an exact area
    ratio.
- **Raw codes get plain words**, with the code kept in a tooltip: partner
  failure reasons, scrape statuses, block sources, and website verdicts
  (`http_403` reads "Site blocked our check (403)", pinned against the
  server's own `websiteReasonLabel`).
- **In flag-only mode** (`NON_ICP_LLM_META` off) the Model tab says Meta was
  still sent — the chip, the group and the lede, not only the ladder.
- **Filters live in the link** (All leads, Model, Visitors), so a link says
  what it shows, and Partners' "See this partner's leads" opens All leads on
  that partner.
- **Refresh every 5 minutes** on every PR C tab, paused while the browser
  tab is hidden; the gaps list keeps its own 10 minutes. (Overview's Today
  view keeps its 1 minute from PR B.)
- **The map follows the theme** (a dark basemap from the same keyless Esri
  service, checked by downloading tiles at three zooms), keeps the reader's
  zoom across refreshes, and colours the area past the world's edge as the
  tiles' own sea.

## Server changes I did NOT make (for you to decide)

- **CSV formula escaping** on the All leads and SDR exports. Escaping a
  leading "+" would add an apostrophe to every phone number a dialer
  imports, so it needs a decision, not a tidy-up.
- **An "ours" marker** on SDR, Visitors and Partners rows (the routes do not
  return one).
- **Partners' row-level quirks:** Needs attention counts failing rows only,
  and Check A ignores acknowledgements.
- **Model decision rows** borrow an industry from a domain that did not
  decide.
- **The Model Meta group's server note** still says those leads "booked and
  are dialled as normal".
- **Unbounded totals** behind the Visitors lists (each is capped, and says
  so).

## The review, and what it found

Five review lenses: numbers, security, robustness, parity with the classic,
and accessibility and layout. A skeptical verifier checked each finding and
was told to refute it. **50 reported, 45 confirmed.** Of the 5 refuted, 3
were deliberate and recorded, and 2 were real bugs that are not security
risks; I fixed those 2 anyway. That came to about 30 distinct fixes
(`5bd3659`), plus one more found while checking them (`2467ac2`). The ones
that mattered:

1. **A lead could blank All leads and Blocked.** The change-log cache was a
   plain object, so a session id of "constructor" found Object's own
   constructor, read it as a cached log, and threw. The loading skeleton
   stayed up forever, with no count and no error, and every refresh threw
   again. Anyone can send /partial that session id. Fixed with a
   null-prototype cache, and every PR C tab now draws its body through a
   guard, so one bad row paints an error instead.
2. **Old rows sat under new filters.** Every tab painted before it loaded, so
   the "updating…" marker never showed and the previous count sat under the
   new filter. Every tab now loads first. Blocked words its count from the
   payload's own mode, and Model and Visitors read the window back from the
   payload.
3. **"Custom" dates could not be chosen.** Picking Custom snapped back to
   "Last 7 days" and the date boxes never appeared.
4. **The acknowledgement note was wiped by the 5-minute refresh**, and a
   failed save hid its error until you pressed Cancel. The draft is now kept
   in state, the form stays open with the error inside it, focus moves on
   purpose, the outcome is announced, and every control names its company.
5. **Columns hidden on a 1280 laptop were in no row detail** on four tables.
   They are now, and the layout check now measures 1280.
6. **Card view had no way to sort.** A Sort select appears where the header
   row goes.
7. **Words that said more than the data:** "Meta: no website" for a site we
   could not check; `http_403` as a raw code; "it books and is dialled" for
   leads that may have done neither; "twice the leads, twice the area".
8. **Smaller ones:**
   - a page past the end read "No leads yet";
   - the pager could strand focus;
   - Clear and the last "Show more" dropped focus;
   - nothing was announced after a filter;
   - long tables were unnamed scroll regions;
   - the map toggle reported the wrong state;
   - the map lost your zoom on every refresh;
   - a failed gaps re-check looked like a fresh all-clear.
9. **The tools:** by default the layout check wrote its screenshots into
   the repo folder, and with rows opened those show real names and phone
   numbers. The repo is public. Both tools also left behind a Chrome profile
   whose history holds the token-bearing URL, and the crosscheck left Chrome
   running if it failed part-way. Both now use the OS temp folder, delete
   the profile, and kill Chrome on any exit. `layout-shots/` is in
   `.gitignore` as a backstop.

**Found after the review, by the new keyboard check:** my first pager fix
put Next's target page into its name, so after every Next or Previous the
refocus looked for a button that no longer existed, and focus fell to the
page body. That was worse than the page-1 case the review found. Previous
and Next are now named by direction.

## Verified — actually run

- **Full bar, run bare and read:** 14 suites, **4,901 assertions, 0
  failures** (4,711 on `main`). `test-monitor-next.js` went from 202 on `main` to **392**.
  The review fixes are driven through the page's real click, input and
  change handlers in a stubbed DOM:
  - the "constructor" lead;
  - the draw guard;
  - Custom dates;
  - "updating…";
  - a page past the end;
  - a same-tab link change;
  - the ack draft surviving a repaint;
  - the failed save;
  - the flag-only wording;
  - the map keeping its zoom;
  - website verdicts compared, word for word, with the server's own
    `websiteReasonLabel`.
- **Number for number against the classic, on live data, read-only**
  (`tools/crosscheck-monitor.mjs`). Each payload is fetched once and served
  to BOTH pages, so they paint from the same bytes. **45 of 45**, run again
  after the review fixes. It covers:
  - counts, row order and stages;
  - every person on the SDR list (1,031, all pages);
  - the Model ladder, groups, cache and decisions;
  - Visitors coverage and merged networks;
  - every Partners card and funnel headline;
  - the partitions: stages, blocked, Meta, ours, website, enrichment and
    repeats, each adding up to the live total of 5,847 leads.

  I proved it can fail: networks merged by the wrong key was reported.
- **Layout check, real Chrome** (`tools/check-monitor-layout.mjs`). On the
  review-fixed code: **316 combinations, 0 findings**. That is every view at
  360, 390, 414, 768, 1024, 1280 and 1440, in both themes, plus each PR C tab
  with its first eight rows opened. It ran before the last two commits. Those
  rename a pager attribute and remove a no-op line; neither moves anything
  on screen, and the keyboard check below ran on the final pager. It gained three checks, each of which
  found real bugs:
  - a plain table wider than its card: four tables scrolled sideways on
    phones;
  - rows opened before measuring: detail links 15px tall on touch, and the
    change log losing its labels because the card rules leaked into nested
    tables;
  - Partners, which was missing from the default page list entirely.
- **Keyboard, with real key presses:** the skip link, focus kept across a
  repaint, the drawer closing, and now PR C too. Next keeps focus on Next,
  Previous back to page 1 hands focus to the current page, and Clear moves
  it to the search box. **I removed each fix and watched the check fail.**
- **Screenshots read by eye.** Every new tab was read at least once on a
  phone and once at a wider width, and in both themes. The shots checked
  (the full set is too many to read one by one):
  - All leads at 390 (both themes), 768 dark (a row opened) and 1280 light;
  - Blocked at 390 light and 1440 dark (rows opened);
  - SDR at 390 light (a row opened) and 768 dark;
  - Model at 390 light, 768 light and 1440 dark;
  - Visitors at 390 (both themes, the map), 1024 light and 1440 dark;
  - Partners at 390 light and 1440 dark (rows opened);
  - the lead's change log stacked on a phone in dark.

## Mutations — each puts one bug back, measured with `measure.js --mutation`

Restored by copying the file back, never with `git checkout`. The second round ran in a scratch git worktree, so the preview kept serving the real files, with a memory check before every mutation.

**Round 1: 26 mutations of the build.** 24 caught. Two survived, and both are now pinned by tests (`7afa68d`). Round 2 re-ran both, as R35 and R36, and both are now caught:
- "AT LEAST" dropped when the Needs-attention count could not run (nothing drove that case);
- a link to another scheme accepted. It still became a harmless `https://` link, so nothing noticed.

Round 1 also covered:
- the Meta chip on blocked rows;
- the change log fetched by the raw session id;
- defaults never sent, and the leads search trimmed only on the way to the server;
- the SDR search trim, fields and export;
- the Model top-level cache, its three groups, and the flag-only relabel;
- the circles largest first and sized by area;
- the dark basemap;
- networks merged by name;
- Cancel sending nothing;
- the JSON false on un-acknowledge;
- "cannot fire" when a conversion is not verified;
- the rate floor;
- the stacked-table labels;
- a repaint reopening rows;
- "Left on step 2";
- the touch-size links;
- the child-scoped card rules.

**Round 2: 37 mutations, one per review fix.** 36 caught, 1 survived:

| # | Mutation (puts back the bug) | Verdict |
|---|---|---|
| R01 | change-log cache back to a plain object | CAUGHT |
| R02 | the draw guard rethrows | CAUGHT (first UNMEASURED — see below) |
| R03 | any sort from the link is sent | CAUGHT |
| R04 | tab check matches Object methods again | CAUGHT |
| R05 | Custom is not remembered | CAUGHT |
| R06 | reload paints before it loads | CAUGHT |
| R07 | a page past the end is not clamped | CAUGHT |
| R08 | Previous/Next share the page attribute again | CAUGHT |
| R09 | a same-tab hash change only repaints | CAUGHT |
| R10 | Blocked words its count from the select | CAUGHT |
| R11 | the ack draft is not written back | CAUGHT |
| R12 | a failed save closes the form | SURVIVED — equivalent, line removed (see below) |
| R13 | Cancel keeps the draft | CAUGHT |
| R14 | claimants leave out the shown partner | CAUGHT |
| R15 | company rows lose their detail | CAUGHT |
| R16 | per-partner detail drops the hidden columns | CAUGHT |
| R17 | a failed gaps re-check looks fresh | CAUGHT |
| R18 | flag-only chip says withheld again | CAUGHT |
| R19 | deploy card drops writes failed | CAUGHT |
| R20 | a window mismatch does not say updating | CAUGHT |
| R21 | a failed model call reads "read fine" | CAUGHT |
| R22 | the near-miss cap is not said | CAUGHT |
| R23 | every repaint redraws the circles | CAUGHT |
| R24 | every draw re-fits the map | CAUGHT |
| R25 | places lose their scroll-region name | CAUGHT |
| R26 | http_NNN back to a raw slug | CAUGHT |
| R27 | the chip says no website again | CAUGHT |
| R28 | search results are not announced | CAUGHT |
| R29 | reload runs on any tab | CAUGHT |
| R30 | screenshots back in the repo | CAUGHT |
| R31 | focus may land on a disabled control | CAUGHT |
| R32 | the card sort select does nothing | CAUGHT |
| R33 | the leads sort select does nothing | CAUGHT |
| R34 | Custom dates force two columns again | CAUGHT |
| R35 | a link to another scheme becomes an https link | CAUGHT |
| R36 | AT LEAST dropped when the count is incomplete | CAUGHT |
| R37 | the model reason claims booking again | CAUGHT |

- **R02 was first UNMEASURED, not caught.** The draw-guard test left a broken row behind. With the guard removed, that row threw again on the tab's next open, outside any try/catch, and the whole suite crashed. The test now puts the real data back (`db7851e`), and R02 is caught.
- **R12 survived because the line did nothing.** On a failed save it re-opened a form that was already open (only a successful save closes it). I removed the line rather than keep a guard nothing can test. The test that a failed save keeps the form open still passes.

**Outside `measure.js`:**
- **Keyboard, in real Chrome:** the pager named by page again, and Clear's focus move removed. Both caught, for the right reason.
- **Crosscheck:** networks merged by the wrong key. Caught.

## Deliberately not fixed, and why

- **The change log is cached for the page's life.** A refresh keeps it, so
  an open row does not refetch every 5 minutes. Recorded in the plan. The
  verifier agreed it is deliberate.
- **"Meta: blocked" is not repeated beside "blocked".** The blocked chip
  already says it, and the panel spells out what that withheld.
- **The ack flow has only run in the test suite.** The preview refuses every
  POST, and live data has no failed partner company to acknowledge today
  (Needs attention is 0).

## Design-system departures, for Utsav

- **New components, built from tokens only:**
  - the plain table that stacks into labelled rows on a phone (`U.grid`);
  - the card-view Sort select;
  - the inline acknowledge form;
  - the map's themed controls.
- **One new alias, `--map-sea`:** neutral-200 in light and neutral-900 in
  dark, the two tokens nearest the basemaps' own sampled ocean colours.
- **Leaflet's controls** take the theme's colours instead of Leaflet's white.

## Process, stated plainly

- Three background jobs were stopped part-way by a memory shortage on this
  machine (the cs-crm dev servers). Nothing was lost. The one mutation that
  was applied at the moment of the kill was in a scratch worktree, and I put
  it back from the backup. The checks were then finished one at a time, with
  a memory check before each.
- Once I filtered the bar's output through a file instead of reading it
  bare. I re-ran it bare straight after, and it is green.
- Merge only on your word, in that message.
