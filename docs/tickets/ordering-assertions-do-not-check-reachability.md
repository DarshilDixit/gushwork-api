# Position is not reachability: ~17 ordering assertions cannot see an early return

**Status:** open, diagnosed, **not fixed**. The exposure is measured and the two
candidate fixes are sized below.
**Found:** 7 Sept 2026, in PR 25, then audited across all six suites.
**Severity:** no known defect today. The affected assertions guard the highest
consequence code in the repo — a double affiliate payment and a permanently
burned domain — so the *class* matters more than the current risk.

---

## The shape

Assertions of the form "X happens before Y" are written by comparing **source
character offsets**:

```js
const claimAt = fn.indexOf('ps_signup_sent_at = NOW()');
const sendAt  = fn.indexOf('await sendConversion(');
ok('conversion: the domain is claimed BEFORE the HTTP call', claimAt < sendAt);
```

That reads as a control-flow assertion and is not one. It proves the two
statements appear in that order in the file. It says **nothing** about whether
either is reached.

So every one of these survives:

- an early `return` or `continue` inserted above the ordered statement;
- wrapping it in `if (false) { … }`;
- a new guard clause placed above it that is wrong;
- an unconditional branch taken before it.

None of those move a single offset.

## How it surfaced

In PR 25 an ordering assertion checked that a staleness test ran after the
failure states and *before* the green return. Mutating `if (conv || qual ||
lds) return hc(…'green')` to `if (true) return …` skipped the staleness check
entirely and left every offset unchanged. The assertion passed. The fix there
was to additionally assert the green return is **guarded**, not just where it
sits.

## The audit, measured rather than reasoned about

Reachability mutations were applied to the highest-value ordering assertions —
each one leaving source positions untouched — and measured with
`node tests/measure.js --mutation`:

| Mutation | Result |
|---|---|
| Early `return` above the **conversion claim** in `runPartnerStackSignup` | **SURVIVED** |
| Early `return` above the **qualification claim** in `sendQualificationForDomain` | **SURVIVED** |
| `continue` above the **phantom 404 release** in the read-back sweep | **SURVIVED** |

Three of three. Those are, in order, the guard against paying an affiliate
twice, the guard against paying an affiliate twice, and the guard against
burning a domain's conversion permanently.

A grep across the six suites finds roughly **30 position comparisons**, of
which about **17 are control-flow claims** with this exposure. The rest are
harmless and should not be changed — rendered HTML order, a label sequence, an
array's declared order, column adjacency, declaration-before-use.

The exposed ones, by suite:

- `test-partnerstack.js` — claim-before-send (twice), fetch-before-send,
  count-before-attempt, claim/release ordering, batch-before-PATCH,
  disqualified-guard-before-test-email, runs-after-`res.json` (twice),
  log-before-clear (twice)
- `test-batch1.js` — the booking check before its `UPDATE` (both webhook
  routes), wall detection before substance analysis, `res.json` before the
  deferred work
- `test-batch2.js` — `res.json` before `finaliseElvVerdict`
- `test-ads-parity.js` — the typo hint before `verifyEmail`

## Why this is not merely theoretical

An *unconditional* early return is a bizarre edit nobody would make. A
**conditional** one is ordinary: a new skip reason, a new guard clause, an
extra validation placed at the top of a function. Any of those inserted above a
claim would pass every test in this repo. The claim-first ordering exists
because "check then send" races and PartnerStack cannot undo a double credit —
that reasoning is documented at length and enforced only by an assertion that
cannot see it.

## The two candidate fixes

**1. Assert the early-exit SET, not the ordering.** Enumerate every `return` /
`continue` reachable before the guarded statement and assert the list matches a
known set, so a new exit point fails the test and forces the author to justify
it. Cheap — a few lines per function — brittle in the ordinary way source
assertions are, and it converts "did anything change?" into a decision rather
than a silent pass. This is the same shape as the never-NULL-bind audit in
`test-batch2.js`, which derives a column list from the source rather than
pinning today's four clauses.

**2. EXECUTE the functions against fakes.** The strongest fix and the one that
actually answers the question. `tests/test-partnerstack.js` already does this
for `partnerLifecycle` — lifted with `new Function`, driven with a fake pool —
and `tests/test-sf-readers.js` does it for the Salesforce readers against a
stubbed `fetch`. `runPartnerStackSignup` needs a fake pool plus a stubbed
`sendConversion`; assert the conversion was **called**, and with what, rather
than where it appears. Then an unreachable claim fails immediately and
naturally.

Option 2 for the three money-path functions (`runPartnerStackSignup`,
`sendQualificationForDomain`, `runPartnerStackConversionVerify`) is the
recommendation. Option 1 everywhere else, if at all — most of the 17 guard
ordering that is not worth a fake harness.

**Not started deliberately.** It is a test-architecture change across three
functions that guard the money path, and it should be its own piece of work
with its own review rather than a tail-end addition to a sweep.

## The general rule, now in CLAUDE.md

**An assertion about ORDER must be paired with an assertion about
REACHABILITY**, or it only checks layout. Concretely: alongside "X is before
Y", assert that the branch containing X is still guarded by the condition it
should be, or drive the function and assert X was called.
