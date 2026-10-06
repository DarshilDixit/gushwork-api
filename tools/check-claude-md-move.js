/* ============================================================
   tools/check-claude-md-move.js — proves that moving history out of
   CLAUDE.md into docs/background/ (7 Oct 2026) lost nothing and copied
   nothing twice. READS ONLY.

   CLAUDE.md had grown past the size Claude Code loads cleanly, and the
   fix was to MOVE text, never delete it. "Every line is still somewhere"
   is the claim a reviewer cannot check by eye across 2,772 lines, so
   this counts it:

     - every non-blank line of CLAUDE.md at <commit> must appear EXACTLY
       as many times across today's CLAUDE.md plus docs/background/*.md
       as it did in the old file. Fewer is a line lost; more is a line
       copied instead of moved. A line can legitimately appear more than
       once in the old file (a table's |---|---| row, a code fence), which
       is why this counts copies rather than asking "is it present".
     - every line that is NEW (the one-line rules left behind, the index,
       each moved file's header) is printed, so the additions are a list
       somebody can read rather than a diff they have to infer.
     - blank lines are counted and reported but not matched, because the
       move adds a blank after each rule it leaves behind.

   Exits 1 on any lost or duplicated line.

   Run:
     node tools/check-claude-md-move.js b924ce7

   b924ce7 is main just before the move. Not mounted anywhere and not
   called by anything.
   ============================================================ */
const fs = require('fs');
const path = require('path');
const { execFileSync } = require('child_process');

const ROOT = path.join(__dirname, '..');
const base = process.argv[2];
if (!base) { console.error('usage: node tools/check-claude-md-move.js <commit>'); process.exit(2); }

const linesOf = (text) => text.replace(/\n$/, '').split('\n');
const oldLines = linesOf(execFileSync('git', ['show', `${base}:CLAUDE.md`], { cwd: ROOT, encoding: 'utf8' }));

const bgDir = path.join(ROOT, 'docs', 'background');
const newFiles = ['CLAUDE.md', ...fs.readdirSync(bgDir).filter(f => f.endsWith('.md')).sort().map(f => path.join('docs', 'background', f))];
const newLines = [];
const where = new Map();                    // line -> [file:lineNo, ...] for the additions report
for (const f of newFiles) {
  linesOf(fs.readFileSync(path.join(ROOT, f), 'utf8')).forEach((l, i) => {
    newLines.push(l);
    if (!where.has(l)) where.set(l, []);
    where.get(l).push(`${f}:${i + 1}`);
  });
}

const count = (arr) => { const m = new Map(); for (const l of arr) if (l.trim() !== '') m.set(l, (m.get(l) || 0) + 1); return m; };
const oldC = count(oldLines), newC = count(newLines);

const lost = [], dup = [], added = [];
for (const [l, n] of oldC) {
  const m = newC.get(l) || 0;
  if (m < n) lost.push({ l, n, m });
  if (m > n) dup.push({ l, n, m });
}
for (const [l, m] of newC) if (!oldC.has(l)) added.push({ l, m });

const blanks = (arr) => arr.filter(l => l.trim() === '').length;
const oldNonBlank = oldLines.length - blanks(oldLines);

console.log(`old CLAUDE.md at ${base}: ${oldLines.length} lines, ${oldNonBlank} non-blank, ${oldC.size} distinct`);
console.log(`new: ${newFiles.length} files, ${newLines.length} lines`);
for (const f of newFiles) console.log(`  ${f}  ${fs.readFileSync(path.join(ROOT, f), 'utf8').length} chars`);
console.log(`blank lines: old ${blanks(oldLines)}, new ${blanks(newLines)} (not matched)`);
console.log('');
console.log(`LOST (in the old file more times than in the new set): ${lost.length}`);
for (const x of lost) console.log(`  old x${x.n}, new x${x.m}: ${x.l}`);
console.log(`DUPLICATED (old line now appears more times than before): ${dup.length}`);
for (const x of dup) console.log(`  old x${x.n}, new x${x.m}: ${x.l}`);
const accounted = [...oldC].reduce((s, [l, n]) => s + Math.min(n, newC.get(l) || 0), 0);
console.log(`old non-blank lines accounted for exactly: ${accounted} of ${oldNonBlank}`);
console.log('');
console.log(`ADDED (new lines, not in the old file): ${added.length} distinct, ${added.reduce((s, x) => s + x.m, 0)} total`);
for (const x of added) console.log(`  ${where.get(x.l).join(', ')}\n    ${x.l.length > 160 ? x.l.slice(0, 157) + '...' : x.l}`);

process.exit(lost.length || dup.length ? 1 : 0);
