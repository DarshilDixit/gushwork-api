# PR 121 — Dropoff: presets that open on a period start, in ET

Branch `fix/dropoff-preset-and-partial-header`. Written 25 Sept 2026.

## What it does and does not touch

**Dashboard browser code only.** No blocking behaviour changes, no Meta
event fires or stops firing, no route, query or write path is touched. The
server's `/monitor/dropoff` is byte-identical.

## The four fixes

1. **"Last 12 weeks" opened mid-week** (commit `3b65a9f`). The preset asked
   for today minus 77 days, a Friday, so the first bucket was clipped: 132
   drawn beside eleven full weeks where the real week was 294. It read as a
   collapse. Week presets now snap back to that Monday, matching
   `date_trunc('week')`.
2. **The `part` badge read as an extra column** (`3b65a9f`). It sat inline
   after the date in a right-aligned header, so the header had fifteen
   headings over fourteen columns. It now stacks under the date.
3. **"Last 12 months" skipped a month on the 29th-31st** (`68ba4ef`). It set
   the month before the day, so on 31 Oct it asked for "31 Nov", rolled to
   1 Dec and showed eleven months. Next occurrence would have been 31 Oct
   2026. Day first, then month.
4. **"Today" came from the viewer's laptop** (`68ba4ef`). CLAUDE.md rules
   that out and the All Leads presets already avoided it via `etDay`. From
   India the boxes ran a day ahead of the dashboard for nine and a half
   hours each morning, and on a Monday that hung an empty future week on
   the end of the table. The maths is now one pure function,
   `dpPresetRange(preset, etToday)`, anchored at noon UTC like
   `etDayShift`.

## Verified — actually run

- **Full bar green, run bare, read in full:** 12 suites, 4,418 assertions.
  `test-non-icp-routes.js` 538 -> 554. Baseline re-saved after reading it.
- **The preset maths is executed, not read.** The real `dpPresetRange` is
  evaluated out of the served dashboard JS and run on **every day of
  2026-2028**: 12 and 26 weeks always open on a Monday exactly 12 / 26
  buckets back, 12 months always opens on the 1st exactly eleven months
  back, every preset ends on the day given. Plus named cases: 31 Oct, 31
  Mar, a Friday, a Sunday, a Monday, This year, Custom.
- **Four mutations, all CAUGHT** via `measure.js --mutation` against the
  committed `68ba4ef`, restored by copy (never `git checkout`):
  1. 12 months back to month-before-day (the original bug) — 3 failed
  2. 12 weeks no longer snapping to Monday — 3 failed
  3. `dpPreset` handing over a non-ET date — 1 failed
  4. `dpMonday` off by one, snapping to Sunday — 5 failed

## Asserted only structurally

- **That `dpPreset` passes `etDay(new Date())`** is read from source. The
  stub DOM returns `''` for every `.value`, so `dpPreset` itself cannot be
  driven; the function it calls is.
- **The `part` badge placement.** The suite asserts the marker text is in
  the header and after a full period's label. It cannot see layout, which
  is how both `3b65a9f` bugs got past it.

## Never executed

- **Not opened in a real browser in this session.** The `3b65a9f` PR text
  says both layout bugs were found that way; that is the earlier session's
  claim, carried forward, not re-checked here.
- **Correction to PR 120's card**, which said the tab had "not been opened
  in a real browser". It had not at the time of writing; it has since,
  which is where fixes 1 and 2 came from.

## After merge, look at

- Open Dropoff, default preset. The first column should be a Monday with
  no `part` marker; only the current week should carry one.
- Switch to Last 12 months. The first column should be October 2025, and
  there should be twelve month columns.
