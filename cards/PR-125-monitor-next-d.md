# PR 125 — the switch: `/monitor` is the new dashboard (PR D)

Branch `feat/monitor-next-d`. Written 26 Sept 2026. PR D of the monitor plan
(`docs/monitor-plan.md`). **Not merged — waiting for your review.**

Your three decisions from this morning, built as asked:
1. our own test submissions left out of the Overview, Dropoff and the
   Monday digest, and each saying in words how many it left out;
2. safe CSV exports on All leads and SDR;
3. the switch, with `/monitor/classic` kept for a week as the fallback.

The handoff file from `docs/monitor-plan-after-c` rides in this PR, as you
asked (`8ace655`).

Commits:
- `e37b90d`: the build;
- `bed4d96`: CLAUDE.md and the preview;
- `066d099`: old links;
- `613be60`: a checker crash fixed;
- `92569da`: the review fixes;
- `29326a9`, `1843a39`, `a61833b`: tests that mutation testing showed were
  missing or stopped early;
- `c7323f1`, `49d1e42`: the second review's fixes and their test;
- the rest are handoff updates.

## What it does and does not touch

- **Nothing here changes which leads are blocked or which Meta events
  fire.** The lead path is not touched. `/partial`, `/submit` and the three
  booking routes are unchanged.
- **Nothing is deleted or rewritten in the database.** Every change is a read
  or a label.
- **`/monitor` now serves the new dashboard.**
  - `/monitor/next` answers with a 302 to `/monitor` and keeps the query
    string (the token). The browser keeps the `#tab=…&view=…&unit=…` part
    by itself, so old links still land on their tab, view and unit. That
    was proven in real Chrome.
  - An old classic-style `/monitor#tab=blocked` link opens Blocked on the new
    page. Every classic tab name is registered there, and a test checks each
    one.
- **The classic dashboard moved to `/monitor/classic`**, unchanged apart from:
  - a line at the top: "This is the classic dashboard, kept for one week as
    a fallback. The dashboard is now at /monitor.";
  - "New dashboard →" is now "← Back to the dashboard".

  Removing it after the week is its own PR.
- **The font route stays at `/monitor/next/asset/…`.** Only the page moved.
  The redirect matches `/monitor/next` exactly, so it never catches the font
  route.

## 1. Our own test submissions: left out, and said so

- **Left out of:** the Overview (every card, the chart, the channels, "last
  lead", "first lead", recovered bookings), the Dropoff tab (both
  dashboards, since they share `dropoffReport`), and so the Monday digest.
- **Left out row by row, before people are counted.** The rule is the same
  `internalLeadSqlClause` that marks rows everywhere else. It is used as
  `IS NOT TRUE`, never `NOT`, the house rule. It could only be NULL for a
  row with no email, and every read now requires one.
- **Each surface says how many it left out, in words:**
  - Overview: "Leaves out 1 of our own test submissions this week, and 5 by
    this point last week. Sessions cannot be told apart by address, so those
    still include ours."
  - Dropoff: "Our own test submissions are left out, as on the Overview — 38
    submissions in this window, not counted above."
  - Classic Dropoff: "Our own test submissions are left out of this tab — N
    submissions in this window, not counted above. (This dashboard's
    Overview still counts them; the new one does not.)"
  - Digest: "Leaves out N of our own test submissions from last week." This
    is **last week's** count, not the whole 12-week window.
- **Still listed wherever rows are listed:** All leads, Blocked, Duplicates,
  the SDR list, Visitors and Partners. **Marked** on the first three. The SDR
  list, Visitors and Partners have no "ours" marker yet (already on the
  plan's to-do list); an earlier draft of this card said they were marked,
  and that was wrong. The "ours" filter works as before.
- **"Last lead" and "first lead" on the Overview skip ours too.** A teammate
  who submits a test to check the form is alive won't see "Last lead" move;
  only the "Leaves out" count moves.
- **Sessions still include ours.** A session has no email, so it cannot be
  told apart. The Overview says so.
- **The classic's `/monitor/metrics` (its Overview) still counts them.** It
  is a one-week fallback, and changing it would move its history in its last
  week. So the two Overviews now differ by our own tests. All time today:
  5,852 on the classic, 5,745 on the new one, 107 ours. The classic's
  Dropoff note says which is which.

**The digest's text changed and I have NOT fired it.** Firing posts to the
leads channel, so that is your call. The test suite runs the real function
and reads the Slack text (`test-batch2.js` §33).

## 2. The CSV exports (All leads and SDR)

Built exactly as you specified:
- **always** an apostrophe before `=`, `@`, a tab or a carriage return at the
  start;
- **`+` or `-`** only when the cell is not just a number (digits, spaces,
  `( ) . -`, and at least one digit);
- then quoted on a comma, a quote, a CR or an LF. CR was missing before, so
  a stray carriage return split a row.

The same `csvCell` builds every cell of both exports.

**On real data, read-only (26 Sept):**
- 2,679 phone numbers, **0 changed**;
- 4 cells get the apostrophe: 1 website, 2 "how did you hear", 1 company;
- 866 cells are quoted, exactly as before.

**Two things to know. Neither is a regression, and I did not build around
either:**
- **"Unchanged" means the bytes the dialer imports, not what Excel shows.**
  Excel still reads a number as a number: `+19495550123` loses its "+".
  A dashed `+1-949-555-0123` would be worked out as a sum (-1626), but no
  stored phone has that shape (0 of 2,679). Both were true before this PR.
- **A spreadsheet set to split on `;`** (common in Europe) splits a cell
  mid-way at a `;` or a line break, even inside quotes. A piece starting
  with `=` there would run. No stored value has that shape (0, 26 Sept).
  Guarding it would put a visible apostrophe inside every bullet list in
  "about the business", so **I left your rule as specified. Say if you want
  it.** It is written up in CLAUDE.md and the code comment.

## The review, and what it found

**First pass.** The review sweep ran 5 lenses plus verifiers, but **5 of its
agents stopped on the usage limit, so it produced no verified findings.** I
checked every finding it had surfaced by hand:

| Finding | Verdict | Fix |
|---|---|---|
| The "ours" marker on All leads (both dashboards) still said "Counted in every total" — false since this PR | **Real** | Now says the Overview, Dropoff and digest leave them out. Asserted off the painted row, and on the classic's source |
| Dropoff counted leads with no email and the Overview did not | **Real, latent**: 0 of 5,851 such rows | Both now keep rows with an email only. Tests read it off the SQL actually sent: all 3 Dropoff reads, every Overview read |
| Excel shows a dashed `+` number as a sum, which the docs did not say | **Real, docs only** | CLAUDE.md and the comment corrected |
| A `;`-splitting spreadsheet can start a formula mid-cell | **Real, narrow**: 0 values | Left as your rule. Recorded above for you |

**Robustness, checked by hand:**
- The classic builds every address absolutely (`API + "/monitor/…"`), so
  moving it breaks none of its calls. Confirmed by the crosscheck, below.
- No Slack post or email links to `/monitor` anywhere in the repo, so the
  switch changes no alert link.
- The digest can never print "undefined": every week of the per-week map
  starts at 0.
- The redirect sends only `/monitor` plus the query, so it cannot become an
  open redirect.

**Second pass, after the limit reset.** Two independent reviewers read the
final diff, read-only: one for correctness and security, one for numbers
and wording.

**Correctness and security found nothing high or medium.** They confirmed:
- every placeholder number, across all the new calls;
- NULL handling;
- that every read of leads in both reports skips ours, and nothing that must
  stay included was changed;
- the redirect: no open redirect, no header injection, no loop, and the font
  route isn't caught by it;
- `csvCell`;
- that nothing touches blocking or Meta.

Fixed from their findings:

| Finding | Fix |
|---|---|
| The classic put the raw token into two new links **and** into `var TP="…"` inside its script (the script part was old). Only exploitable while `MONITOR_TOKEN` is unset | Encoded once where it's built; a test uses the suite's hostile token and finds it only encoded |
| Two CLAUDE.md passages still said "nothing is subtracted from any total" and "counted in every leads figure" | Rewritten to match the decision |
| CLAUDE.md and this card said ours are **marked** on the SDR list; they aren't. It also listed Lead magnet's totals as including them; they never did | Corrected |
| The Blocked tab's intro and the blocked chip said "in every total", false for our own blocked tests since this PR | Now "counted as a lead", and says the Overview and Dropoff leave ours out |
| The classic's "ours" tooltip said "the new dashboard leaves them out", too broad (its All leads counts them) | Now "the new dashboard's Overview" |
| The classic Dropoff note's number had no unit, and reads as people in People mode | Now "N **submissions**" |
| Nothing on the classic's own Overview explained why it reads 107 more than the new one | Its banner now says so |
| A code comment gave the wrong reason for `IS NOT TRUE` | Corrected |
| The handoff file still listed decision 1 as waiting | Marked decided |

**Left as your call:** a cell that starts with a space or line break, then
`=`, isn't prefixed. Excel reads it as text; an importer that trims leading
spaces might not. That's the same family as the `;`-locale gap above, with 0
stored values affected. Say if you want both guarded.

## Verified — actually run

- **The full bar, bare:** 14 suites, **4,979 assertions, 0 failures**.
  Baseline saved on a quiet machine. `test-monitor-next` went 410 → 422 and
  `test-non-icp-routes` 556 → 560, all new assertions.
- **Real database, read-only, on the final code:**
  - This week in Leads mode, Overview **322 = Dropoff 322**, and **both left
    out the same 1**.
  - Blocked this week: 23 = 23.
  - All time: 5,745 leads and 5,319 people, 107 ours left out.
- **Crosscheck against the classic, live data, read-only: 45 of 45.** Every
  tab's numbers, rows and order agree.
- **Layout check, real Chrome:** every tab and view at 360, 390, 414, 768,
  1024, 1280 and 1440, both themes. **0 findings**, over two runs because
  the first crashed:
  - the first run did 128 combinations before the checker itself crashed
    (the page's document was briefly null, nothing wrong on screen);
  - the checker now retries a measurement three times and reports it as a
    finding instead of crashing (`613be60`);
  - the second run covered the other 224, with 0 findings.
- **Keyboard and old links, real Chrome:** clean. I broke the redirect on
  purpose and the check failed for the right reason.
- **Screenshots, read by eye:**
  - the Overview's sentence, 1440 light and 390 dark (the page's address
    read `/monitor` after loading `/monitor/next`);
  - the Dropoff note, 1440 dark;
  - the classic's banner and link, 1440 and 390.

  The classic came from this branch booted on a fake empty database, because
  the preview proxies production's classic.

  At 390, the classic's header runs off the right edge. **It does exactly
  the same on production today** (935px of page at a 390px screen on both),
  so this PR did not cause it.

## Mutations — each puts one bug back, measured with `measure.js --mutation`

**43 mutations**, one per guard, filter, wording and route this PR adds.
Each ran in a scratch worktree and was restored by copying the file back.

| Result | Count | Which |
|---|---|---|
| **Caught** | **41** | every Overview read that leaves ours out (per read); both Dropoff modes and its source list; the count of what was left out; the per-week counts and the digest line; all eight `csvCell` branches; the redirect keeping the token; `/monitor` serving the new page; the classic's banner, token encoding and both notes; both "ours" markers; both email rules; the "every total" wording |
| **Caught by 2 suites, 4 others stop at load** | 1 | D27, the classic moving off `/monitor/classic`. `test-lead-field-changes` and `test-apollo` fail it cleanly. Four suites use that route's own line to cut code out for testing (CLAUDE.md says so), so they stop loudly when it is renamed |
| **Survived** | 1 | D38, the preview tool's classic fallback. No suite covers the preview tool; it was checked by running it |

**How the number got there:**
- **5 survived the first run.** Both halves of the Overview's channel
  query, the lead side of recovered bookings, the Dropoff count in both
  modes, and its source list. The checks had been per query, so a query
  that reads leads twice passed with one half unfiltered. They're now
  caught (`29326a9`).
- **D26 (`/monitor` stops serving the page) crashed the suite** at
  assertion 100 of 418 instead of failing it. The suite now keeps going on
  the page built straight from `monitor-next.js`, and catches it cleanly
  (`1843a39`, `a61833b`).
- **Part of the first run was spoiled by me.** I ran another branch's tests
  beside it, and the booting suites use fixed ports. Every verdict from the
  overlap was re-run on a quiet machine; the numbers above are the clean
  runs.

## Process, stated plainly

- Merged nothing. The branch is pushed and this PR is open. **I stop here.**
- The preview, the fake-database server and every Chrome I started are
  stopped. Their leftover profile folders are deleted.
- Everything ran in the scratchpad or a scratch worktree. Screenshots stay
  there.
- Every production read was read-only at the database. The token was never
  printed.
