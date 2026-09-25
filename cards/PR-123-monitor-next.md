# PR 123 — the new dashboard at `/monitor/next`, side by side with `/monitor`

Branch `feat/monitor-next`. Written 26 Sept 2026. PR B of the monitor plan.
Three commits: `f212508` (the build), `e2f600b` (everything the review
confirmed), `def078a` (two more executed guards).

## What it does and does not touch

- **No change to which leads are blocked or which fire Meta events.** No
  route on the lead path is edited. Every number on the new page comes from
  a read.
- **New:** `/monitor/next` (the page) and `/monitor/overview` (one read,
  `overviewReport`), both behind the same token. `/monitor` keeps working
  exactly as before, and it links across both ways.
- **Five tabs rebuilt:** Overview (Today live / This week / All time, People
  or Leads), System health, Dropoff, Duplicates, Lead magnet. Everything
  else is a marked link to the classic tab.
- **Blocked moved to PR C.** It shares the All Leads row renderer
  (`leadRowsHtml`), so it is rebuilt with All Leads rather than twice.
- **What changes on the CLASSIC dashboard, all small:**
  - a "New dashboard →" link;
  - `#tab=` is honoured on load;
  - "Disqualified" relabelled "B2C, mixed or waitlist" in five places, the
    placeholder included (683 people: 306 answered B2C, 378 asked for the
    waitlist);
  - the Lead magnet CSV now escapes a leading `= + - @` (formula injection);
  - `/monitor/duplicates` returns `is_internal`, groups by `lower(email)`
    alone (0 addresses differ today, 338 groups either way), and is now the
    function `duplicatesReport`;
  - `/monitor/metrics` reads the shared `RECOVERED_BOOKINGS_SQL` (same SQL).
- **One line of the Monday Slack digest changes wording.** The Dropoff
  ladder's blocked row read "Real-estate brand or insurance carrier". The
  model layer blocks individual realtors too, so it now reads "Real-estate or
  insurance business". The digest prints that description, so its line
  changes with it. No count changes.

## The review, and what it found

Five review lenses (numbers, code, visual, wording, accessibility), each
finding checked by an adversarial verifier. **79 confirmed, 4 refuted.**
De-duplicated, that is about 60 fixes. The ones that mattered:

1. **This week's grey "last week" bars were always zero.** The server keys
   last week by its own dates and the page looked them up with this week's.
   Every test passed, because a zero-height bar is still a `<path d="">`. The
   fix lines them up by position. The test now counts bars with a shape and
   reads the value back out of the painted table.
2. **"Page loads" counted sessions.** The query counts `form_sessions` rows,
   which are sessions, and ran 6–13% short of the page loads it named.
   Renamed "Sessions" everywhere, the payload field included.
3. **The phone 3-hour blocks read as five hours** ("12a –5a", overlapping
   their neighbours).
4. **Numbers that disagreed with each other on screen:** the Sessions card's
   rate ignored the People/Leads toggle; Disqualified's parts did not add up
   (the remainder is now "N no reason recorded"); "Meta withheld" meant one
   reason here and five on the classic tab (now "Meta withheld — model");
   the comparison bars drew 71% shorter than 67%.
5. **Health could look healthy when it wasn't checking.** `/health` had no
   timeout, so a hang held every row grey. The Overview strip also skipped
   API uptime and email verification (ELV), so "every check is green" could
   be false.
6. **A late response repainted over the tab you had moved to**, and a slow
   earlier filter request could overwrite a newer one.
7. **Keyboard and screen-reader problems:**
   - every repaint dropped focus and closed open rows, every minute on Today;
   - secondary text was 3.41:1 against the 4.5:1 minimum;
   - the focus ring was 40%-transparent blue;
   - the skip link was invisible when focused;
   - the drawer let focus walk under it;
   - 338 expand buttons were all named "Show details".
8. **Layout:**
   - tables scrolled sideways on a tablet beside the menu;
   - every collapsed detail row showed in card view;
   - Duplicates was one ~151,000px page on a phone (now paged and searchable);
   - the laptop range 1280–1439 was cramped;
   - the menu button showed on desktop.

## Verified — actually run

- **Full bar, run bare and read:** 14 suites, **4,704 assertions, 0 failures**.
  `test-monitor-next.js` went 142 → 196.
- **`test-monitor-next.js` executes, not reads.** It boots the app,
  evaluates the page's own served JavaScript in a stubbed DOM, drives every
  rebuilt tab through its real click and input handlers, and compares the
  painted numbers with the payload. Specifically it checks:
  - last week read back from the painted table;
  - the 3-hour blocks and the all-zero chart;
  - a week that crosses a month;
  - Duplicates search and paging;
  - the CSV escaper, new and classic, run on hostile values;
  - the hash reader and `GW.paint`'s focus and row restore;
  - the SQL `/monitor/duplicates` sends.
- **Layout check, real Chrome:** every width asked for (360, 390, 414, 768,
  1024, 1440), plus 1280. Both themes, all nine views, plus the drawer.
  **135 combinations, 0 findings** (134 layouts plus the keyboard section).
  It checks sideways scroll, clipped text, 44px tap targets, overlapping
  chart labels, floating elements, console errors, junk values and fonts.
- **Screenshots read by eye**, which is where two bugs the checker cannot
  see were found: the phone comparison bars shrunk to ~70px, and collapsed
  rows showing in card view.
- **The keyboard, with real key presses** (CDP key events, so the browser
  moves focus itself), now part of the layout check:
  - the first Tab lands on the skip link, visible at 44px;
  - Enter on it moves focus into the page and leaves the `#hash` alone;
  - a repaint keeps focus on the toggle that had it;
  - on a phone, Shift+Tab out of the open menu drawer closes it.

  **Proved it can fail:** with the drawer's focus-out handler removed it
  reports "focus left the drawer and the drawer stayed open over it", and
  with the skip link's focus style removed it reports "not visible at 44px".
  The first run of this section failed on a harness bug, not a page bug.
  Navigating to the same URL with a new `#hash` is not a page load, so focus
  stayed where earlier checks left it. It now loads a blank page first.
- **Against live data, read-only.** The preview runs this branch's
  `overviewReport` and `duplicatesReport` on connections read-only at the
  database. The week payload carries `sessions` (6,019 / 4,779) and last
  week keyed by its own dates, as the fix expects. Duplicates returns 338
  addresses, 14 ours.
- **Mutations** via `measure.js --mutation` against the committed code,
  restored by copy, never `git checkout`:

  | # | Mutation (puts back the bug) | Verdict |
  |---|---|---|
  | M1 | last week looked up by this week's dates | CAUGHT, test-monitor-next, 3 |
  | M2 | the tab's "am I current?" guard removed | survived, see below |
  | M2b | that guard AND `root = null` on leaving removed | CAUGHT, test-monitor-next, 1 |
  | M3 | a 3-hour block ends 3 hours past its last hour | CAUGHT, 2 |
  | M4 | an all-zero chart divides by zero | CAUGHT, 1 |
  | M5 | new CSV export stops escaping formulas | CAUGHT, 2 |
  | M6 | the Sessions rate ignores the unit | CAUGHT, 1 |
  | M7 | the Disqualified remainder dropped | CAUGHT, 1 |
  | M8 | Duplicates grouped by the raw address too | CAUGHT, 1 |
  | M9 | `/health` fetched with no timeout | CAUGHT, 1 (source-level) |
  | M10 | classic CSV export stops escaping formulas | CAUGHT, 1 |
  | M11 | "Biggest leak" hardcoded to one row | CAUGHT, 1 |
  | M12 | `#view` read as a state again | CAUGHT, 1 |
  | M13 | a repaint stops restoring focus | CAUGHT, 1 |

  **M2 survived because the behaviour has two guards, and either one is
  enough.** Leaving a tab clears its reference, so a late render returns
  before it reaches the "am I current?" check. Removing both (M2b) is caught.
  I kept both; the belt is cheap and the braces cover a future caller that
  keeps the reference. M9 is a source assertion only: the stubbed browser
  has no clock to hang a fetch on, so the timeout itself has not been driven.

## Deliberately not fixed, and why

- **The non-ICP health row shows timings** ("Warm: 3509ms avg… Cache hits
  80%"). It is the server's own text, shared with the classic tab, on the
  engineering tab. Changing it is a server change for another PR.
- **Recovered bookings ignores `asof`.** No screen sends `asof`, and bounding
  it would change the query `/monitor/metrics` shares.
- **Two Overview refreshes of the same view can land out of order.** Both
  are at most a minute old. The cross-filter version of this bug (Dropoff,
  Lead magnet) is fixed.
- **Chart bars are not keyboard-focusable.** The Table switch is the
  keyboard alternative, and it has every value.
- **The classic dashboard's copies of the wording fixes** ("reached step 2")
  are left alone. It is being replaced; its behaviour stays exactly as it is.
- **Our own test addresses are still in every Overview number.** That is a
  recorded open decision in CLAUDE.md, not something to change quietly. They
  are marked on Duplicates and Lead magnet.
- **At 360px the attention chips wrap to two rows.** One scrolling row would
  break the no-sideways-scroll rule.

## Design-system departures, for Utsav

- **Light muted text** is neutral-600, not the spec's neutral-500 (3.41:1,
  under the 4.5 floor). The light section qualifier follows suit; dark
  follows the spec (neutral-300).
- **Focus ring** is solid primary-500/400, not the 40%-alpha token, which is
  under 3:1 on every surface.
- **The current nav row** is semibold in full ink, not just the fill (the
  fill is 1.09:1 against the rail).
- **Badge labels** are at /600 (declared in PR B's first commit).
- **New components, built from tokens only:**
  - the change chip;
  - the comparison bars;
  - the attention strip;
  - the Live badge;
  - the responsive table and its detail well;
  - the "Show more" row.

## Process, stated plainly

- One `measure.js --save` had its output sent to /dev/null. The bare
  `--check` straight after it shows all 14 suites passing against the new
  baseline, so the recorded baseline is a green bar.
- Merge only on your word, in that message.
