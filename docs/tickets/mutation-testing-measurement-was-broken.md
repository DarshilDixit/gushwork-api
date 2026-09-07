# Every mutation-testing number before 7 Sept 2026 was measured with a broken instrument

**Status:** the instrument is fixed; the historical record cannot be, and this
ticket is the record.
**Found:** 7 Sept 2026, while mutation-testing PR 23.
**Affects:** every mutation tally in every review card and commit message in
this project before that date.
**Severity:** no known production impact. The damage is to *confidence* — and
confidence is what mutation testing exists to produce, so a broken measurement
of it is not a small thing.

---

## The claim to correct

Review cards and commit messages in this repo say things like "16 mutations,
16 caught". Those numbers were produced by breaking a line and **counting `✗`
markers** in the suite's output, treating any output with a marker — or
sometimes any non-zero exit — as a caught mutation.

**That measurement cannot distinguish a caught mutation from a suite that never
ran.** A suite that crashes prints a stack trace, **zero `✗` markers**, and
exits 1:

- to a marker-counter, that looks exactly like a clean run;
- to an exit-code checker, it looks exactly like a caught mutation.

It is neither. It is an **unmeasured result**.

**So the correct reading of every earlier tally is "unverified", not "wrong".**
The mutations may well all have been caught — most were plain assertion
failures on suites that completed normally, and nothing here suggests a real
defect slipped through. But the runs were not verified to have been measured,
and there is no way to reconstruct retrospectively which ones crashed. Anyone
reading an older review card should treat its mutation count as an unaudited
claim rather than evidence.

This ticket exists because those numbers are persuasive and will be read at
face value otherwise.

## How it surfaced

Three mutations of the new `tests/test-sf-readers.js` looked like survivors.
They were crashes: `out.records[259].id` throwing a `TypeError` after a broken
reader returned an empty list. The mutations were caught by the code; the
measurement could not see it.

## The exposure across the six suites, measured rather than assumed

Each suite was run against a mutation that breaks a declaration it depends on:

| Suite | Behaviour before the fix |
|---|---|
| `test-batch1.js` | **crashed** — 0 markers, no summary |
| `test-batch2.js` | **crashed** — 0 markers, no summary |
| `test-partnerstack.js` | **crashed** — 0 markers, no summary |
| `test-ads-parity.js` | reported 1 marker — and ran **13 of its 159 assertions** |
| `test-batch-a.js` | not demonstrated; the probe was outside its remit |
| `test-sf-readers.js` | reported correctly (it has the scenario wrapper) |

**Three of six had the exposure outright.** `test-partnerstack.js` is the one
that matters most: it carries the great majority of the assertions in this
project and nearly every PartnerStack mutation was measured against it.

## The third mode, which nobody was looking for

`test-ads-parity.js` did not crash. It **completed, printed a normal summary,
reported one failure — and silently ran 8% of itself.** Every check anyone had
been applying passes that: non-zero exit, a failure line, a summary. The 146
assertions that never ran are invisible.

So there are three modes, not two:

1. **Crash** — no summary, no markers. Unmeasured.
2. **Partial run** — a summary, some markers, far fewer assertions than
   baseline. Also unmeasured, and it looks completely normal.
3. **Reports** — a summary, markers, and the assertion total intact.

## THE RULE

**A mutation counts as CAUGHT only when all three hold:**

1. the suite **completed** — no `SUITE DID NOT COMPLETE` marker;
2. it **failed** — `failed > 0`;
3. it ran its **usual number of assertions** — `passed + failed` equals the
   pre-mutation baseline.

`failed > 0` on its own is not the measurement. Neither is a non-zero exit
code. **The assertion count is a required part of it**, because condition 3 is
the only one that catches mode 2, and mode 2 passes every other check.

Capture the baseline totals *before* mutating, and compare after every run.

## The argument, in case the rule looks like overkill

**It caught the same class of bug in my own harness an hour later.** While
mutation-testing PR 24, the harness snapshotted only the file named at the call
site — so one mutation that targeted a different file was never restored and
silently poisoned every subsequent run. Sixteen mutations all reported the same
bogus verdict against a corrupted working tree.

What exposed it was not the verdicts. It was the **baseline re-check after
restore**, which the new rule requires. Counting markers would have reported
sixteen clean catches from a tree that no longer matched the branch.

Same day, same class, twice: the thing measuring the tests was less trustworthy
than the tests.

## What was changed

- **`tests/crash-reporter.js`**, required first by all six suites. Turns any
  escape — a synchronous throw at module load, or an unhandled rejection in an
  async suite — into a `✗` line **and** an explicit `SUITE DID NOT COMPLETE`
  marker. It deliberately prints no `passed:`/`failed:` totals: a fabricated
  total would be the same class of lie as the one it fixes.
- **`tests/measure.js`**, which applies the rule mechanically so it is not a
  discipline anyone has to remember. See its header.
- The rule in `CLAUDE.md` under Deploying, and the measured table in
  `docs/partnerstack.md`.
- Two test-side crash sources fixed on the way: the one block in
  `test-sf-readers.js` left outside its scenario wrapper, and a `r.reason` read
  after a throw that aborted the rest of a scenario.

**The partial-run mode cannot be fixed generically — only measured.** Hence the
rule rather than a code change.

## A known limitation of the rule: data-driven assertion loops

Condition 3 compares `passed + failed` against the baseline, which makes the
rule **conservative** — it never reports a false CAUGHT — but it *can* report a
false UNMEASURED. That happens when the assertions are DERIVED from the thing
being mutated.

Example, 7 Sept 2026: `tests/test-batch2.js` loops over every source string
passed to `recordFailure` and every `FAILURE_MONITORS` entry, asserting one per
item. Removing an entry changes how many assertions run, so
`--mutation` reported UNMEASURED even though the mutation was cleanly caught
with a precise message.

**When a mutation legitimately changes the assertion count, verify it by hand
and say so.** Run the suite directly and read the `✗` line. Do not "fix" it by
loosening condition 3 — the partial-run mode it catches is real and silent,
and a false UNMEASURED costs one manual check while a false CAUGHT costs
confidence in everything.

## Not done, deliberately

**The three crashing suites were not restructured.** Wrapping
`test-partnerstack.js`'s 953 assertions in per-scenario error boundaries the way
`test-sf-readers.js` does would be a large, risky refactor of the file that
guards the money path, to buy a better failure *message* — the crash reporter
already converts mode 1 into a reported failure, which is what the measurement
needs. If that file is being restructured for another reason, add the
boundaries then.
