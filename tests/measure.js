/* ============================================================
   measure.js — run the suites and apply the mutation-testing rule.

   Mutation results in this project used to be measured by counting "✗" lines.
   That cannot tell a caught mutation from a suite that never ran: a crash
   prints a stack trace, zero markers and exits 1, which reads as a clean run
   to a marker-counter and as a caught mutation to an exit-code checker. Three
   of the six suites did exactly that. A fourth completed, printed a normal
   summary, reported a failure — and ran 13 of its 159 assertions.

   See docs/tickets/mutation-testing-measurement-was-broken.md.

   THE RULE, applied here so it is not something anyone has to remember:
   a mutation is CAUGHT only if the suite COMPLETED and FAILED and ran its
   USUAL NUMBER OF ASSERTIONS. The third condition is the one that catches the
   partial-run mode, and it is the one every ad-hoc check omits.

   Usage
   -----
     node tests/measure.js                  run all suites, print a baseline
     node tests/measure.js --save           ...and write it to tests/.baseline.json
     node tests/measure.js --check          compare against the saved baseline
                                            (this is the normal test bar)
     node tests/measure.js --mutation       compare, and report a per-suite
                                            verdict: CAUGHT / survived /
                                            UNMEASURED

   Exit codes: 0 = everything as expected. 1 = something is not.
   In --mutation mode, exit 0 means at least one suite CAUGHT it and none was
   UNMEASURED — so an unmeasured run can never be mistaken for a catch.

   The baseline file is committed on purpose. A baseline nobody can see is a
   baseline nobody checks, and the counts are exactly the thing that must not
   drift silently.
   ============================================================ */

const { execFileSync } = require('child_process');
const fs = require('fs');
const path = require('path');

const SUITES = [
  'test-batch1.js',
  'test-batch2.js',
  'test-batch-a.js',
  'test-ads-parity.js',
  'test-partnerstack.js',
  'test-sf-readers.js',
  'test-submit-gate.js',
  'test-session-payload.js',
];

const BASELINE = path.join(__dirname, '.baseline.json');

function run(suite) {
  const file = path.join(__dirname, suite);
  let out = '';
  try {
    out = execFileSync(process.execPath, [file], { encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] });
  } catch (err) {
    /* A failing suite exits non-zero; its output is still what we need. */
    out = String((err && (err.stdout || '')) + (err && (err.stderr || '')));
  }
  const incomplete = /SUITE DID NOT COMPLETE/.test(out);
  const p = /passed:\s*(\d+)/.exec(out);
  const f = /failed:\s*(\d+)/.exec(out);
  const passed = p ? Number(p[1]) : null;
  const failed = f ? Number(f[1]) : null;
  return {
    suite,
    incomplete,
    passed,
    failed,
    /* null total is itself a signal: no summary was printed at all. */
    total: passed === null || failed === null ? null : passed + failed,
  };
}

function verdict(r, baseTotal) {
  if (r.incomplete)          return { kind: 'UNMEASURED', why: 'the suite crashed before completing' };
  if (r.total === null)      return { kind: 'UNMEASURED', why: 'no assertion summary was printed' };
  if (baseTotal == null)     return { kind: 'NO-BASELINE', why: 'no saved baseline for this suite' };
  if (r.total !== baseTotal) return { kind: 'UNMEASURED', why: `ran ${r.total} of ${baseTotal} assertions` };
  if (r.failed > 0)          return { kind: 'CAUGHT', why: `${r.failed} assertion(s) failed` };
  return { kind: 'survived', why: 'every assertion still passes' };
}

const args = process.argv.slice(2);
const mode = args.includes('--mutation') ? 'mutation'
  : args.includes('--check') ? 'check'
  : args.includes('--save') ? 'save' : 'report';

const base = fs.existsSync(BASELINE)
  ? JSON.parse(fs.readFileSync(BASELINE, 'utf8'))
  : null;

const results = SUITES.map(run);
let bad = 0, caught = 0, unmeasured = 0;

console.log('');
for (const r of results) {
  const baseTotal = base && base.suites ? base.suites[r.suite] : null;
  if (mode === 'mutation') {
    const v = verdict(r, baseTotal);
    if (v.kind === 'CAUGHT') caught++;
    if (v.kind === 'UNMEASURED' || v.kind === 'NO-BASELINE') unmeasured++;
    console.log(`  ${r.suite.padEnd(24)} ${v.kind.padEnd(12)} ${v.why}`);
  } else {
    const flag = r.incomplete ? 'DID NOT COMPLETE'
      : r.total === null ? 'NO SUMMARY'
      : r.failed > 0 ? `${r.failed} FAILED`
      : baseTotal != null && r.total !== baseTotal ? `total ${r.total}, baseline ${baseTotal}`
      : 'ok';
    if (flag !== 'ok') bad++;
    console.log(`  ${r.suite.padEnd(24)} passed ${String(r.passed ?? '?').padStart(4)}  total ${String(r.total ?? '?').padStart(4)}  ${flag}`);
  }
}

const grand = results.reduce((a, r) => a + (r.total || 0), 0);
console.log('');
console.log(`  ${results.length} suites, ${grand} assertions`);

if (mode === 'save') {
  const payload = { savedAt: new Date().toISOString(), grandTotal: grand,
    suites: Object.fromEntries(results.map((r) => [r.suite, r.total])) };
  fs.writeFileSync(BASELINE, JSON.stringify(payload, null, 2) + '\n');
  console.log(`  baseline written to tests/.baseline.json`);
}

if (mode === 'mutation') {
  console.log('');
  if (unmeasured) {
    console.log(`  UNMEASURED — ${unmeasured} suite(s) could not be measured. This is NOT a catch.`);
    console.log('  Fix the run before drawing a conclusion. See');
    console.log('  docs/tickets/mutation-testing-measurement-was-broken.md');
    process.exit(1);
  }
  console.log(caught ? `  CAUGHT by ${caught} suite(s).` : '  SURVIVED — no suite noticed this mutation.');
  process.exit(caught ? 0 : 1);
}

if (mode === 'check' && (bad || (base && base.grandTotal !== grand))) {
  if (base && base.grandTotal !== grand) {
    console.log(`  Grand total moved: ${base.grandTotal} -> ${grand}.`);
    console.log('  If that was intended, re-run with --save.');
  }
  process.exit(1);
}
process.exit(bad ? 1 : 0);
