/* ============================================================
   Builds the minified form files, and checks they are current.

     node tools/build-form-min.js           write both .min.js and their maps
     node tools/build-form-min.js --check   rebuild in memory, compare, write
                                            nothing; exits 1 if any is stale

   WHY THIS EXISTS. A Lighthouse audit of the form pages (Oct 2026) put
   about 36 KB of the Ads file's transfer down to its comments and
   whitespace. The readable files stay the source: they are what gets
   edited, reviewed and diffed. The .min.js beside each is GENERATED from
   it, committed, and served by jsDelivr at the same commit like the
   source, so a page can pin it by SHA with an SRI hash.

   COMMITTED, NOT LEFT TO jsDelivr. jsDelivr will minify any file on the
   fly when asked for "x.min.js" and no such file exists. Those bytes are
   jsDelivr's, not ours, and nothing promises they stay the same, so an SRI
   hash over them can break on its own. A committed file is served as is.

   NO RENAMING OF FUNCTIONS (keep_fnames, keep_classnames). It costs about
   850 bytes brotli per file and keeps two things true: stack traces in a
   visitor's console still name the real function, and a function's .name
   is the same as in the source, so the differential suite can compare
   source and minified file with no exceptions carved out.

   Terser is pinned to an exact version in package.json, because the same
   input through a different terser is different bytes and a different SRI
   hash. Upgrading it is a deliberate change: rebuild, re-run the bar, and
   the pins move.

   tests/test-form-differential.js runs --check's logic and then runs the
   minified files against the previous release, whole. Not mounted.
   ============================================================ */

const fs = require('fs');
const path = require('path');
const { minify } = require('terser');

const ROOT = path.join(__dirname, '..');
const FILES = ['gushwork-form.js', 'gushwork-form-popup.js'];
const TERSER_VERSION = require('terser/package.json').version;

function minName(file) { return file.replace(/\.js$/, '.min.js'); }

function bannerVersion(src) {
  const m = /Form initialised (v[0-9]+\.[0-9]+\.[0-9]+(?:-ads)?) /.exec(src);
  if (!m) throw new Error('no "Form initialised v..." banner found');
  return m[1];
}

/* One file in, { name, code, mapName, map } out. Everything that decides
   the bytes is in here, so --check and the suite build exactly what a
   real build writes. */
async function buildOne(file, src) {
  const name = minName(file);
  const mapName = name + '.map';
  const preamble = '/*! ' + file + ' ' + bannerVersion(src) + ' -- minified by tools/build-form-min.js'
    + ' (terser ' + TERSER_VERSION + '). Readable source: ' + file + ' at the same commit. */';
  const out = await minify({ [file]: src }, {
    compress: true,
    mangle: true,
    keep_fnames: true,
    keep_classnames: true,
    format: { comments: false, preamble },
    sourceMap: { filename: name, url: mapName },
  });
  return { name, code: out.code + '\n', mapName, map: out.map + '\n' };
}

async function buildAll(readSource) {
  readSource = readSource || ((f) => fs.readFileSync(path.join(ROOT, f), 'utf8'));
  const res = [];
  for (const f of FILES) res.push(Object.assign({ file: f }, await buildOne(f, readSource(f))));
  return res;
}

/* Every generated file that is missing or differs from a fresh build. */
async function staleFiles() {
  const stale = [];
  for (const b of await buildAll()) {
    for (const [n, want] of [[b.name, b.code], [b.mapName, b.map]]) {
      const p = path.join(ROOT, n);
      const have = fs.existsSync(p) ? fs.readFileSync(p, 'utf8') : null;
      if (have !== want) stale.push(n);
    }
  }
  return stale;
}

module.exports = { FILES, minName, buildOne, buildAll, staleFiles, TERSER_VERSION };

if (require.main === module) {
  (async () => {
    if (process.argv.includes('--check')) {
      const stale = await staleFiles();
      if (stale.length) {
        console.log('STALE: ' + stale.join(', ') + ' -- run node tools/build-form-min.js and commit the result.');
        process.exit(1);
      }
      console.log('Minified form files match their sources.');
      return;
    }
    for (const b of await buildAll()) {
      fs.writeFileSync(path.join(ROOT, b.name), b.code);
      fs.writeFileSync(path.join(ROOT, b.mapName), b.map);
      console.log('wrote ' + b.name + ' (' + Buffer.byteLength(b.code) + ' bytes) and ' + b.mapName);
    }
  })().catch((e) => { console.error(e); process.exit(1); });
}
