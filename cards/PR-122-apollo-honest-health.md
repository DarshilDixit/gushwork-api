# PR 122 — Apollo: page on out-of-credits, make System Health honest

Branch `fix/apollo-honest-health`. Written 25 Sept 2026. PR A of the monitor plan.

## What it does and does not touch

- **No change to which leads are blocked or which fire Meta events.**
- `/enrich` still fails open: on any Apollo failure the form gets empty
  fields and the lead carries on exactly as before.
- **Alerting changes.** Apollo now alerts: "Out of credits", critical, then
  the normal 3-hour critical cooldown while credits stay empty.
- **One Health row changes meaning**: Apollo now counts matches over
  business-email leads, and goes red when the latest reply was a refusal.
- Four dashboard strings change. No query behind them changes.

## The finding

Apollo refuses with a JSON body, not an exception, so `fetch` resolved and
nothing threw. `/enrich` wrote the refusal as an empty enrichment, Health
counted rows written, and `recordFailure` was reachable only from the
catch. **413 refused lookups** across three outages, measured read-only:
24 Jun (40), 3–10 Sept (230), 23 Sept onward (143 and rising). Health read
"80% enriched" through the third while Apollo had found 0 of 86.

A second defect in the same route: a refusal went through the upsert, so a
later refused lookup for the same session **overwrote a real enrichment** and
then blanked the lead row's city, seniority and revenue to match. Refusals
are now insert-only.

## Verified — actually run

- **Full bar, run bare and read in full:** 13 suites, 4,500 assertions,
  0 failures. New suite `tests/test-apollo.js` (68). `test-batch-a.js`
  318 → 332.
- **`tests/test-apollo.js` boots the real app** and drives `/enrich` with each
  reply shape Apollo sends: out of credits (the real 413-row wording), a
  found person, no match, a 502 that is not JSON, and a free mailbox. It
  reads back the Slack post, the SQL and the params bound, and the response
  the form gets. It also drives `tools/re-enrich-apollo.js` against a stubbed
  database: dry run makes no writes, one lookup per address, copy at zero
  cost, internal and staging-page submissions skipped, stop on the first
  refusal, `--limit`.
- **12 mutations, all CAUGHT** via `measure.js --mutation` against the
  committed code, restored by copy, never `git checkout`:

  | # | Mutation | Caught by |
  |---|---|---|
  | M1 | refusal not detected | test-apollo, 14 |
  | M2 | refusal written with `DO UPDATE` (overwrites) | test-apollo, 4 |
  | M3 | out-of-credits branch disabled | test-apollo, 8 |
  | M4 | `recordSuccess('Apollo')` dropped | test-apollo, 2 |
  | M5 | health's refusing-now branch disabled | test-batch-a, 5 |
  | M6 | health numerator back to rows written | test-batch-a, 1 |
  | M7 | health denominator back to all leads | test-batch-a, 1 |
  | M8 | step-to-step rate inverted | test-batch-a, 1 |
  | M9 | SDR card label regressed | test-apollo, 1 |
  | T1 | tool carries on after a refusal | test-apollo, 2 |
  | T2 | tool looks up per session, not per address | test-apollo, 6 |
  | T3 | tool stops skipping our own submissions | test-apollo, 6 |

- **Two mutations would have survived the first draft of the tests**, and
  the suite was changed before measuring: T1 (the refused address was last
  in the fixture, so carrying on looked identical to stopping) and M4 (no
  failure preceded a success). M4 is now proven by *which* alert fires at
  E2: "Repeated failures" with the reset, "Consecutive failures" without.
- **The new health check was run against production**, read-only, inside
  `BEGIN READ ONLY`: `red · "Out of credits for 2d" · You have insufficient
  credits! … · 86 refused in the last 24h · Last enrichment 2d ago`, 829 ms
  over the WAN.
- **Every new or moved statement was EXPLAINed against production**, read
  only: the tool's three queries, `ENRICHMENT_UPSERT_SQL` (24 params),
  `ENRICHMENT_LEAD_UPDATE_SQL` (15), `ENRICHMENT_REFUSAL_SQL` (3).
- **The new alert was fired once, for real**, via
  `railway run --service gushwork-api node tools/fire-alert.js apollo-credits`,
  through the real `recordFailure`: "🚨 Apollo — Out of credits", Slack 200,
  email sent (`<d1b84f12-…@gushwork.ai>`). Marked as a deliberate test in the
  error field.

## Asserted only structurally

- **The health SQL's numerator and denominator.** The bar has no database,
  so the query text is asserted: the matched `FILTER` requires `e.found`,
  refusals are counted apart, and free mailboxes are excluded via
  `<> ALL($1::text[])` with `FREE_EMAIL_DOMAINS` bound. The first version of
  the numerator assertion matched the column definition and would have let
  the exact 23 Sept bug through. It was tightened before M6 was measured.
  The real query's output against production is the executed half.
- **The four dashboard labels** are string checks on the served page.

## Never executed

- **`tools/re-enrich-apollo.js` has not been run against production**, per
  the instruction. Its queries were EXPLAINed, not executed. The credit
  figures come from separate read-only counts: 131 addresses since 23 Sept
  (~107 credits, max 131), 383 across all three outages (~314, max 383),
  16 copyable at zero cost. Apollo's billing (1 credit per person found, 0
  for no match, +8 only for a phone reveal, which is off) is from its
  documentation, not observed.
- **The 3-hour repeat has not been watched in production.** It is the
  existing `alertOps` critical cooldown, unchanged.

## A rule I broke, and what I did about it

I piped one `measure.js --save` through `tail`. The code was identical to a
bar read in full moments earlier. I re-ran `--check` bare immediately after
and read all 13 suites `ok` against the saved baseline. Recorded here rather
than left out.

## After merge, look at

1. System Health → Apollo: expect **red, "Out of credits for 2d"** (or 3d),
   with Apollo's words in the detail.
2. The alerts channel: one **warning** "Apollo — Enrichment has stopped" when
   the heartbeat first sees red, and one **critical** "Apollo — Out of credits"
   on the next business-email lead.
3. Overview: "Completed, no booking yet", the chart subtitle, and the
   funnel's grey step-to-step rates.
