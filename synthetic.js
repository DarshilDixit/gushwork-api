/* ============================================================
   SYNTHETIC CHECKS. Since v5.23.0 (Oct 2026).

   gushwork-web's website monitor (a Cloudflare Worker driving a headless
   browser) fills the real forms end to end. Those runs must exercise the
   same validation, enrichment and routing as a visitor and leave NOTHING
   behind: no lead row, no Salesforce record, no Slack post, no email, no
   Meta or Google Ads event, no PartnerStack call, no dialer row.

   HOW A REQUEST BECOMES SYNTHETIC. The header X-GW-Synthetic carries
   SYNTHETIC_TOKEN, compared in constant time. A missing or wrong token is
   an ordinary request, exactly as before; there is no third outcome. The
   token must be at least 32 characters, or the feature is off.

   TWO LAYERS, deliberately:
     1. Each form route that writes has an explicit synthetic branch in
        index.js (req.synthetic), placed after the same parsing and
        decisions the real path makes, that answers without writing.
     2. Every outbound choke point (Slack, alert email, Salesforce, Meta,
        PartnerStack, the AWS mirror, a database write) asks isSynthetic()
        and refuses. That is the net under layer 1: a synthetic request
        that reaches one of them is a bug, and it is refused and logged
        rather than turned into a real lead.

   isSynthetic() reads an AsyncLocalStorage store set for the length of
   the request, so it follows the request into fire-and-forget work
   started after res.json() as well.
   ============================================================ */

const crypto = require('crypto');
const { AsyncLocalStorage } = require('async_hooks');

const HEADER = 'X-GW-Synthetic';
const MIN_TOKEN_LENGTH = 32;
const als = new AsyncLocalStorage();

function digest(s) { return crypto.createHash('sha256').update(String(s), 'utf8').digest(); }

/* The configured token, or null when the feature is off (unset or too
   short). Read on every call, so the env is the only switch. */
function configuredToken() {
  const t = String(process.env.SYNTHETIC_TOKEN || '').trim();
  return t.length >= MIN_TOKEN_LENGTH ? t : null;
}

/* Constant time over the SHA-256 of both sides, so neither the content
   nor the length of the real token leaks through timing. */
function tokenMatches(given) {
  const expected = configuredToken();
  if (!expected || given == null) return false;
  const g = String(given).trim();
  if (!g) return false;
  return crypto.timingSafeEqual(digest(g), digest(expected));
}

function isSynthetic() {
  const s = als.getStore();
  return !!(s && s.synthetic);
}

function runSynthetic(fn) { return als.run({ synthetic: true }, fn); }

/* The choke-point guard. Returns true (and logs) when the caller must not
   do `what`. Outside a synthetic request it is false and silent. Each
   skip is counted under `point` (a fixed name per choke point), so the
   tests can tell an expected one -- an ELV alert during a synthetic email
   check -- from one that means a synthetic branch let something through. */
const _skips = new Map();
function skips() { return Object.fromEntries(_skips); }
function refuseIfSynthetic(point, what) {
  if (!isSynthetic()) return false;
  _skips.set(point, (_skips.get(point) || 0) + 1);
  console.warn(`[synthetic] skipped: ${what || point}`);
  return true;
}

/* A read is SELECT / WITH / EXPLAIN / SHOW with no writing keyword
   anywhere once comments and string literals are removed. Everything else
   is a write. Deliberately strict: SELECT ... FOR UPDATE takes row locks
   and is refused too. */
const WRITE_WORDS = /\b(INSERT|UPDATE|DELETE|MERGE|UPSERT|TRUNCATE|DROP|ALTER|CREATE|GRANT|REVOKE|COPY|CALL|DO|LOCK|VACUUM|REINDEX|REFRESH|NEXTVAL|SETVAL|PG_ADVISORY_LOCK|PG_ADVISORY_XACT_LOCK|BEGIN|COMMIT|ROLLBACK|SAVEPOINT)\b/i;
function isWriteStatement(q) {
  const text = typeof q === 'string' ? q : (q && q.text) || '';
  const bare = String(text)
    .replace(/\/\*[\s\S]*?\*\//g, ' ')
    .replace(/--[^\n]*/g, ' ')
    .replace(/'(?:[^']|'')*'/g, "''")
    .trim();
  if (!/^(SELECT|WITH|EXPLAIN|SHOW)\b/i.test(bare)) return true;
  return WRITE_WORDS.test(bare);
}

/* Wraps a pg Pool so that, inside a synthetic request only, a write never
   reaches the database. Outside one, every call goes straight through,
   untouched.

   A REFUSED WRITE RESOLVES EMPTY ({ rows: [], rowCount: 0 }) RATHER THAN
   THROWING. Several writes in index.js are fire-and-forget without a
   catch; a rejection there would be an unhandled rejection, and a safety
   net that can take the process down for real visitors is worse than the
   bug it catches. It is loud instead, and counted: the tests drive every
   synthetic route and require the count to stay at zero, because reaching
   this line means a synthetic branch missed a write. */
let _guardHits = 0;
function guardHits() { return _guardHits; }
function guardPoolWrites(pool, label) {
  if (!pool || pool.__gwSyntheticGuard) return pool;
  const query = pool.query;
  const connect = pool.connect;
  pool.query = function (q, ...rest) {
    if (isSynthetic() && isWriteStatement(q)) {
      _guardHits++;
      const head = String(typeof q === 'string' ? q : (q && q.text) || '').replace(/\/\*[\s\S]*?\*\//g, ' ').replace(/\s+/g, ' ').trim().slice(0, 60);
      console.error(`[synthetic] REFUSED a database write on ${label} (a synthetic branch missed it): ${head}`);
      const empty = { rows: [], rowCount: 0, command: 'SYNTHETIC_SKIPPED' };
      const cb = rest.length && typeof rest[rest.length - 1] === 'function' ? rest[rest.length - 1] : null;
      if (cb) { process.nextTick(() => cb(null, empty)); return undefined; }
      return Promise.resolve(empty);
    }
    return query.apply(this, [q, ...rest]);
  };
  if (typeof connect === 'function') {
    pool.connect = function (...args) {
      if (isSynthetic()) {
        _guardHits++;
        const err = new Error(`synthetic request refused a database transaction (${label})`);
        console.error(`[synthetic] REFUSED a database transaction on ${label} (a synthetic branch missed it)`);
        const cb = args.length && typeof args[args.length - 1] === 'function' ? args[args.length - 1] : null;
        if (cb) { process.nextTick(() => cb(err)); return undefined; }
        return Promise.reject(err);
      }
      return connect.apply(this, args);
    };
  }
  Object.defineProperty(pool, '__gwSyntheticGuard', { value: true });
  return pool;
}

/* "[synthetic] " in front of every console line written while a
   synthetic request is being handled, so a Railway log search for
   [synthetic] finds the whole run and a real-lead search can exclude it.
   Outside a synthetic request the original function is called unchanged. */
function tagConsole(c) {
  if (!c || c.__gwSyntheticTagged) return;
  for (const level of ['log', 'info', 'warn', 'error']) {
    const orig = c[level];
    if (typeof orig !== 'function') continue;
    c[level] = function (...args) {
      if (isSynthetic() && !(typeof args[0] === 'string' && args[0].indexOf('[synthetic]') === 0)) {
        args = typeof args[0] === 'string' ? ['[synthetic] ' + args[0]].concat(args.slice(1)) : ['[synthetic]'].concat(args);
      }
      return orig.apply(this, args);
    };
  }
  Object.defineProperty(c, '__gwSyntheticTagged', { value: true });
}

module.exports = {
  HEADER, MIN_TOKEN_LENGTH,
  configuredToken, tokenMatches, isSynthetic, runSynthetic, refuseIfSynthetic, skips,
  isWriteStatement, guardPoolWrites, guardHits, tagConsole,
};
