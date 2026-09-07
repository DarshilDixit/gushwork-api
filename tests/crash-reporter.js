/* ============================================================
   A crashing suite must REPORT, not vanish.

   Every mutation-testing result in this project has been measured by counting
   "✗" lines in a suite's output. That silently treats a CRASH as a caught
   mutation: a suite that dies on `out.records[259].id` after a broken reader
   returned an empty list prints a stack trace, zero ✗ lines, and exits 1 —
   which to anything counting markers looks exactly like a clean run, and to
   anything counting exit codes looks exactly like a caught mutation. It is
   neither. It is an UNMEASURED result.

   Found 7 Sept 2026 while mutation-testing PR 23. Three "surviving" mutations
   were really crashes; three of the six suites were then confirmed to crash
   with zero markers.

   This installs handlers that turn any escape — a synchronous throw at module
   top level, or an unhandled rejection in an async suite — into a ✗ line AND
   an explicit did-not-complete marker. So:

     - a mutation run that counts ✗ lines now sees the crash;
     - a mutation run that compares assertion totals sees the missing summary.

   It deliberately does NOT print a normal "passed: N / failed: N" summary. A
   fabricated total would be the same class of lie as the one this fixes.

   Require it first in a suite:  require('./crash-reporter')('my-suite');
   ============================================================ */

module.exports = function installCrashReporter(suiteName) {
  let reported = false;
  const report = (kind, err) => {
    if (reported) return;
    reported = true;
    const msg = (err && (err.stack || err.message)) || String(err);
    console.log('');
    console.log('  FAILURES:');
    console.log(`   ✗ SUITE CRASHED before completing (${kind}) — ${String(msg).split('\n')[0]}`);
    console.log('');
    console.log(`  SUITE DID NOT COMPLETE: ${suiteName}`);
    console.log('  No assertion totals are available. This is an UNMEASURED result,');
    console.log('  not a caught mutation and not a clean run.');
    console.log('');
    if (err && err.stack) console.error(err.stack);
    process.exit(1);
  };
  process.on('uncaughtException', (err) => report('uncaughtException', err));
  process.on('unhandledRejection', (err) => report('unhandledRejection', err));
};
