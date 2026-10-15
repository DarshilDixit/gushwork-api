# PR: a clearer Blocked tab, with the person's role on every row

Branch `monitor/blocked-tab-clarity`, from `main` at `de06539` (so it does NOT contain #145, the
role line in the Slack post: see "Merge order"). 15 Oct 2026. **Not merged: waiting for your review.**
A dashboard change. It changes nobody's blocked status, no email, no Meta event.

**Why:** you asked for the title on the dashboard where the block is, and said the section felt
"traffic-like", hard to tell parts apart. I rendered it first (read-only, real Chrome, live data) and
what made it feel that way was specific:
- the two housekeeping panels sat ABOVE the actual blocked list with the same weight;
- the "tried again" table was cut off mid-row (`.tbl` has a 260px max height);
- the panel footers broke into columns (a panel footer is a flex row; my loose text and `<code>`
  became separate flex items);
- in the main list, Product, Sells to and Enriched were the same on nearly every row and said nothing,
  while who the person is and why they were blocked were thin.

**What changed on the Blocked tab:**
1. **Three sections in the order a person works:** **Needs a look** (accented), then **Blocked leads**,
   then **Released companies** (folded, with a count; a release opens it so you see it land).
2. **A map at the top:** three buttons, each with its count, that scroll to the sections.
3. **Needs a look** now covers BOTH reasons Slack tags you for: the person tried again, or the AI check
   blocked a large company (inlandgroup.com, 1,200 staff, a first try, was not in the old tried-again
   list). Per person; the brand list never counts as "large"; one row can carry both reasons. New feed
   `GET /monitor/non-icp-review` replaces `/monitor/non-icp-repeats` (merged yesterday, one consumer).
4. **The role on every row**, on the visible row, not only in the opened panel: title over
   level and department ("Vice President of Leasing / vp · operations"), "No title from Apollo" when
   there is none. In the Needs-a-look rows and in the main list.
5. **The main list shows what varies:** Person (name over email), Role, Company (with staff), Blocked by,
   Stage, When (date over source). Product / Sells to / Enriched are gone from THIS tab only (All
   leads keeps them; every field is still in the opened row).
6. **Emails and domains break at `@`, `.`, `-`, `_`** (soft break points) instead of mid-word.
7. Shorter intro, one-element footers, and a taller scroll area for Needs a look.

**The layout check I owed since #143 has now been run**, and it earned its keep: my first attempt gave
the columns minimum widths and the checker failed it at 768, 1024 and 1280 ("a table is 63 / 79 / 102px
wider than its card"). I removed the minimums, made Stage optional in the mid-width band, and merged
two columns. Final: 14 combinations (390, 768, 1024, 1280, 1440 in light and dark, plus the phone
drawer) with 0 findings.

**Not changed:** who is blocked, the release logic, Slack, Meta, the emails, the other tabs.

**Verified, run and read:**
- Full bar, bare: 16 suites, 5817 assertions (baseline saved here: routes 699 -> 706, monitor-next
  477 -> 496).
- The new SQL EXECUTED against production in a read-only transaction: 26 people (16 tried again, 11
  large, 1 both), and the role splits agents from buyers on real rows.
- Over HTTP: the feed's fields, the reasons decided in code, a brand-list block never "large", the
  200-staff line exact (199 is neither reason), the staff line bound as a parameter, our own tests left out.
- In the dashboard's stubbed DOM: section order, the accent, counts, the map (reads nothing, writes
  nothing, leaves the address alone), role on both lists, "no title" wording, folded-then-open released
  list, one-element footers, soft break points.
- Real Chrome on live data through the read-only preview, screenshots taken and looked at (kept in the
  OS temp folder, never the repo: they photograph real people's details).
- Six mutations, all CAUGHT: a brand-list block reading as large, the Role column dropped, soft breaks
  removed, the released list starting open, the sections in the wrong order, the map removed.

**NOT verified:**
- **A real click** on Release… or the map buttons in a real browser (the check measured layout and I
  looked at screenshots; it does not click).
- **Dark mode by eye.** The checker ran both themes with 0 findings; I only looked at light.

**Merge order:** #145 (role in the Slack post) is a separate branch from the same `main` and also edits
`tests/.baseline.json`. Whichever merges second needs `main` merged in and the bar re-run and re-saved.
The code areas do not overlap.

**Also changed:** `tools/preview-monitor.js` now lifts `nonIcpReviewReport` (a new route has no
production version to proxy to until it deploys), and CLAUDE.md names the new section.
