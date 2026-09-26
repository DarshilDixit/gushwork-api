/* ============================================================================
   crosscheck-monitor.mjs -- number for number, the new dashboard against the
   classic one, on LIVE data, read-only. Run against tools/preview-monitor.js.

   Two halves, and both matter:

   1. LIVE INVARIANTS. Read-only GETs of the real routes, and the partitions
      every filter must keep: the four stages add up to all leads; blocked
      only + hide blocked = all; Meta withheld + sent = all (the 22 Sept null
      page_url hole, 869 + 4706 against 5582, was exactly a partition that did
      not add up); the blocked total equals the Overview's own counter; the
      Model ladder sums; the Partners states sum. A wrong number on the server
      would otherwise be blamed on the page.

   2. SIDE BY SIDE, SAME BYTES. Each payload is fetched ONCE and served to
      BOTH pages by an in-page fetch override, so a lead arriving between two
      fetches cannot make them differ. Each page then renders through its own
      code, and the numbers each one painted are compared with the payload
      and with each other.

   It prints NUMBERS and pass/fail only -- never an email, a name or an
   address; the rows are compared as ordered keys and reported as match or
   not. Exit 0 only when every check passes.

   Run: PREVIEW_READY_FILE=... node tools/crosscheck-monitor.mjs
   ============================================================================ */
import { spawn } from 'node:child_process';
import { readFileSync, mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

const cfg = JSON.parse(readFileSync(process.env.PREVIEW_READY_FILE, 'utf8'));
const BASE = `http://localhost:${cfg.port}`, TOKEN = cfg.token;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const results = [];
const check = (tab, what, pass, detail) => results.push({ tab, what, pass: !!pass, detail: detail === undefined ? '' : String(detail) });
async function get(path, q = {}) {
  const u = new URL(BASE + path); u.searchParams.set('token', TOKEN); for (const [k, v] of Object.entries(q)) if (v !== undefined) u.searchParams.set(k, v);
  const r = await fetch(u, { headers: { accept: 'application/json' } }); if (!r.ok) throw new Error(path + ' HTTP ' + r.status); return r.json();
}

/* ── 1. live invariants ─────────────────────────────────────────────── */
const L = (q) => get('/monitor/leads', Object.assign({ page: 1, stage: 'all', sort: 'created_at', dir: 'desc' }, q)).then((d) => d.total);
const all = await L({});
const parts = {
  'the four stages': ['booked', 'completed', 'step1', 'disqualified'].map((s) => ({ stage: s })),
  'blocked only + hide blocked': [{ nonicp: 'only' }, { nonicp: 'exclude' }],
  'Meta withheld + sent': [{ meta: 'withheld' }, { meta: 'sent' }],
  'ours only + hide ours': [{ internal: 'only' }, { internal: 'exclude' }],
  'website failed + not failed': [{ websiteCheck: 'failed' }, { websiteCheck: 'passed' }],
  'enriched + not enriched': [{ enrichment: 'yes' }, { enrichment: 'no' }],
  'repeat + first attempts': [{ repeatAttempts: 'yes' }, { repeatAttempts: 'no' }],
};
for (const [name, qs] of Object.entries(parts)) {
  const ns = await Promise.all(qs.map(L)); const sum = ns.reduce((a, b) => a + b, 0);
  check('leads', name + ' add up to all leads', sum === all, ns.join(' + ') + ' = ' + sum + ' of ' + all);
}
const withheld = await L({ meta: 'withheld' });
const reasons = await Promise.all(['blocked', 'model', 'internal', 'website', 'disqualified'].map((m) => L({ meta: m })));
check('leads', 'each Meta reason is at most the withheld total', reasons.every((n) => n <= withheld), reasons.join(',') + ' of ' + withheld);
const metrics = await get('/monitor/metrics');
const blkAll = await L({ nonicp: 'only' }), blkEx = await L({ nonicp: 'only', internal: 'exclude' }), blkOurs = await L({ nonicp: 'only', internal: 'only' });
check('blocked', 'the Blocked total equals the Overview\'s own counter', blkAll === Number(metrics.nonIcpBlocked), blkAll + ' vs ' + metrics.nonIcpBlocked);
check('blocked', 'ours + not ours = all blocked', blkEx + blkOurs === blkAll, blkEx + ' + ' + blkOurs + ' = ' + blkAll);
check('leads', 'All leads equals the Overview\'s total', all === Number(metrics.total), all + ' vs ' + metrics.total);
const sdr = await get('/monitor/sdr');
check('sdr', 'the total is the rows returned, one per person', sdr.total === sdr.leads.length && new Set(sdr.leads.map((l) => String(l.email).toLowerCase())).size === sdr.leads.length, sdr.total);
const model = await get('/monitor/non-icp', { days: 7 });
const lad = model.ladder.rows.reduce((a, r) => a + r.n, 0);
check('model', 'the ladder adds up to the lead total', lad === model.ladder.total, lad + ' of ' + model.ladder.total);
const acted = model.ladder.rows.filter((r) => ['blocked_list', 'blocked_model', 'meta_only'].includes(r.key)).reduce((a, r) => a + r.n, 0);
check('model', 'the three groups add up to the acted rows', model.industries.reduce((a, g) => a + g.leads, 0) === acted, acted);
check('model', 'one decision per acted lead', model.decisions.length === acted, model.decisions.length + ' vs ' + acted);
const w = model.scrape.window;
check('model', 'the scrape window adds up', w.ok + w.unreachable + w.thin + w.other + w.no_verdict === w.total && w.answered === w.total - w.no_verdict, JSON.stringify(w));
check('model', 'the cache splits add up', model.cache.judged + model.cache.unreadable === model.cache.domains, JSON.stringify({ d: model.cache.domains, j: model.cache.judged, u: model.cache.unreadable }));
const vis = await get('/monitor/visitors', { days: 30 });
check('visitors', 'distinct <= with an address <= all leads', vis.coverage.distinct_addresses <= vis.coverage.with_address && vis.coverage.with_address <= vis.coverage.leads, JSON.stringify(vis.coverage));
check('visitors', 'repeats: shown is the rows returned', vis.repeats.shown === vis.repeats.rows.length && vis.repeats.total >= vis.repeats.shown, vis.repeats.shown + ' of ' + vis.repeats.total);
const partners = await get('/monitor/partners'), lc = partners.lifecycle;
check('partners', 'the eight states add up to the companies', Object.values(lc.byState).reduce((a, b) => a + Number(b), 0) === lc.totalDomains, lc.totalDomains);
check('partners', 'the unticked split adds up to exists_unticked', lc.sfActionable + lc.sfUnactionable + lc.sfUntickedAfterPaid === Number(lc.bySfState.exists_unticked || 0), [lc.sfActionable, lc.sfUnactionable, lc.sfUntickedAfterPaid, lc.bySfState.exists_unticked].join(','));
const gaps = await get('/monitor/partner-gaps');

/* ── 2. side by side, same bytes ────────────────────────────────────── */
const leadsP = await get('/monitor/leads', { page: 1, stage: 'all', sort: 'created_at', dir: 'desc' });
const blkP = await get('/monitor/leads', { nonicp: 'only', page: 1, stage: 'all', sort: 'created_at', dir: 'desc' });
const blkExP = await get('/monitor/leads', { nonicp: 'only', internal: 'exclude', page: 1, stage: 'all' });
const fopts = await get('/monitor/filter-options');
/* keyed by path, plus the two parameters that pick a different population */
const P = { '/monitor/leads': leadsP, '/monitor/leads|nonicp=only': blkP, '/monitor/leads|nonicp=only|internal=exclude': blkExP, '/monitor/metrics': metrics, '/monitor/sdr': sdr,
  '/monitor/non-icp': model, '/monitor/visitors': vis, '/monitor/partners': partners, '/monitor/partner-gaps': gaps, '/monitor/filter-options': fopts };
const OVERRIDE = `(() => { const P = ${JSON.stringify(P)}; const real = window.fetch.bind(window);
  window.fetch = (url, init) => { try { const u = new URL(String(url), location.origin); let k = u.pathname;
    if (u.searchParams.get('nonicp')) k += '|nonicp=' + u.searchParams.get('nonicp'); if (u.searchParams.get('internal')) k += '|internal=' + u.searchParams.get('internal');
    if (P[k] && (!init || !init.method || init.method === 'GET')) return Promise.resolve(new Response(JSON.stringify(P[k]), { status: 200, headers: { 'content-type': 'application/json' } }));
  } catch (e) {} return real(url, init); }; })();`;

const PORT = 9400 + Math.floor(Math.random() * 50);
/* a throwaway profile, and Chrome killed and the profile removed on ANY
   exit -- a throw half-way used to leave Chrome running with its debugging
   port open and the token-bearing URL in its history */
const PROFILE = mkdtempSync(join(tmpdir(), 'gw-xcheck-'));
const chrome = spawn('/Applications/Google Chrome.app/Contents/MacOS/Google Chrome', ['--headless=new', '--remote-debugging-port=' + PORT, '--user-data-dir=' + PROFILE, '--no-first-run', 'about:blank'], { stdio: 'ignore' });
const cleanup = () => { try { chrome.kill(); } catch {} try { rmSync(PROFILE, { recursive: true, force: true }); } catch {} };
process.on('exit', cleanup);
process.on('uncaughtException', (e) => { console.error(e); cleanup(); process.exit(2); });
process.on('unhandledRejection', (e) => { console.error(e); cleanup(); process.exit(2); });
let tg; for (let i = 0; i < 60; i++) { try { tg = await (await fetch(`http://127.0.0.1:${PORT}/json`)).json(); if (tg.length) break; } catch {} await sleep(250); }
const ws = new WebSocket(tg.find((x) => x.type === 'page').webSocketDebuggerUrl); await new Promise((r) => ws.addEventListener('open', r));
let id = 0; const pend = new Map();
ws.addEventListener('message', (e) => { const m = JSON.parse(e.data); if (m.id && pend.has(m.id)) { pend.get(m.id)(m); pend.delete(m.id); } });
const send = (method, params = {}) => new Promise((r) => { const i = ++id; pend.set(i, r); ws.send(JSON.stringify({ id: i, method, params })); });
const ev = async (x) => { const r = await send('Runtime.evaluate', { expression: x, awaitPromise: true, returnByValue: true }); if (r.result?.exceptionDetails) throw new Error((r.result.exceptionDetails.exception?.description || 'eval').split('\n')[0]); return r.result?.result?.value; };
await send('Page.enable'); await send('Runtime.enable');
await send('Emulation.setDeviceMetricsOverride', { width: 1440, height: 1000, deviceScaleFactor: 1, mobile: false });
await send('Page.addScriptToEvaluateOnNewDocument', { source: OVERRIDE });
const errors = [];
ws.addEventListener('message', (e) => { const m = JSON.parse(e.data); if (m.method === 'Runtime.exceptionThrown') errors.push((m.params.exceptionDetails.exception?.description || m.params.exceptionDetails.text).split('\n')[0]); });
const waitFor = async (expr, ms = 30000) => { for (let t = 0; t < ms; t += 400) { try { if (await ev(expr)) return true; } catch {} await sleep(400); } return false; };
async function open(url, ready) {
  await send('Page.navigate', { url: 'about:blank' }); await sleep(200); await send('Page.navigate', { url });
  if (!(await waitFor(ready))) throw new Error('the page never became ready (' + ready + ')' + (errors.length ? ': ' + errors.join(' | ') : ''));
  await sleep(1500);
}
const num = (s) => (s === null || s === undefined ? null : Number(String(s).replace(/,/g, '')));

/* CLASSIC */
await open(`${BASE}/monitor?token=${encodeURIComponent(TOKEN)}`, `typeof showTab === 'function'`);
const C = {};
await ev(`showTab('leads')`); await waitFor(`!/Loading/.test(document.getElementById('ltbody').textContent)`);
C.leads = await ev(`(() => ({ total: (document.getElementById('lcount').textContent.match(/(\\d+)/) || [])[1], sids: [...document.querySelectorAll('#ltbody td.xbtn')].map((t) => (t.getAttribute('onclick').match(/toggleRow\\('l-([^']+)'/) || [])[1]),
  stages: [...document.querySelectorAll('#ltbody > tr:not(.erow)')].map((tr) => tr.children[6] && tr.children[6].textContent.trim()) }))()`);
await ev(`showTab('blocked')`); await waitFor(`!/Loading/.test(document.getElementById('blk-tbody').textContent)`);
C.blocked = await ev(`(() => { const t = document.getElementById('blk-count').textContent; const m = t.match(/(\\d+) leads? blocked · (\\d+)/) || []; return { a: m[1], b: m[2], sids: [...document.querySelectorAll('#blk-tbody td.xbtn')].map((x) => (x.getAttribute('onclick').match(/toggleRow\\('b-([^']+)'/) || [])[1]) }; })()`);
await ev(`showTab('sdr')`); await waitFor(`!/Loading/.test(document.getElementById('sdr-tbody').textContent)`);
C.sdr = await ev(`(() => ({ n: (document.getElementById('sdr-count').textContent.match(/(\\d+)/) || [])[1], emails: [...document.querySelectorAll('#sdr-tbody td.te')].map((t) => (t.getAttribute('title') || '').toLowerCase()),
  completed: [...document.querySelectorAll('#sdr-tbody .badge.bb')].length }))()`);
await ev(`showTab('model')`); await waitFor(`!/Loading/.test(document.getElementById('mdl-ladder').textContent)`);
C.model = await ev(`(() => { const h = (id) => document.getElementById(id).innerHTML; const lad = [...h('mdl-ladder').matchAll(/<b>(\\d+)<\\/b>/g)].map((m) => m[1]);
  const sc = h('mdl-scrape'); const cache = sc.match(/(\\d+) companies classified all time — (\\d+) judged, (\\d+) currently unreadable/) || [];
  const groups = [...h('mdl-ind').matchAll(/· (\\d+) leads?/g)].map((m) => m[1]);
  return { ladder: lad.slice(0, 5), total: (h('mdl-ladder').match(/(\\d+) leads in the window/) || [])[1], groups, cache: cache.slice(1, 4), decisions: (h('mdl-dec').match(/class="xbtn"/g) || []).length,
    unread: document.querySelectorAll('#mdl-unread tr').length, nearTotal: (h('mdl-near-note').match(/<b>(\\d+)<\\/b> came close/) || [])[1] }; })()`);
await ev(`showTab('visitors')`); await waitFor(`!/Loading/.test(document.getElementById('vis-cov').textContent)`);
C.vis = await ev(`(() => ({ cov: [...document.querySelectorAll('#vis-cov .efv')].map((e) => (e.textContent.match(/(\\d+)/) || [])[1]), repeats: document.querySelectorAll('#vis-repeats tr').length,
  networks: [...document.querySelectorAll('#vis-networks tr')].map((tr) => tr.children[2] && (tr.children[2].textContent.match(/(\\d+)/) || [])[1]), places: document.querySelectorAll('#vis-places tr').length }))()`);
await ev(`showTab('partners')`); await waitFor(`!/Loading/.test(document.getElementById('ptbody').textContent)`); await sleep(800);
C.partners = await ev(`(() => { const t = (id) => (document.getElementById(id) || {}).textContent || ''; return { cards: ['p-attn', 'p-domains', 'p-sfwait', 'p-leads', 'p-conv', 'p-qual'].map((i) => (t(i).match(/(\\d+)/) || [])[1]),
  states: [...document.querySelectorAll('#p-states .pschip')].map((c) => c.textContent.trim()).sort(), funnel: [...document.querySelectorAll('#pfn .pfsv')].map((e) => e.textContent.trim()),
  domains: document.querySelectorAll('#pdtbody tr').length, partners: document.querySelectorAll('#ptbody tr.prow').length }; })()`);

/* NEW */
await open(`${BASE}/monitor/next?token=${encodeURIComponent(TOKEN)}#tab=leads`, `!!(window.GW && GW.show && GW.TABS && GW.TABS.leads)`);
const N = {};
const settle = async (tab) => { await ev(`GW.show(${JSON.stringify(tab)})`); await waitFor(`!document.querySelector('#view .skel, #view [aria-busy="true"]')`); await sleep(600); };
await settle('leads');
N.leads = await ev(`(() => ({ total: (document.querySelector('.readat').textContent.match(/([\\d,]+)\\s*leads? found/) || [])[1], sids: [...document.querySelectorAll('[data-sid]')].map((x) => x.getAttribute('data-sid')),
  stages: [...document.querySelectorAll('td[data-l="Stage"]')].map((t) => t.textContent.trim()) }))()`);
await settle('blocked');
N.blocked = await ev(`(() => { const m = document.querySelector('.readat').textContent.replace(/,/g, '').match(/(\\d+)\\s*leads? blocked · (\\d+)\\s*people · (\\d+)/) || []; return { a: m[1], people: m[2], b: m[3], sids: [...document.querySelectorAll('[data-sid]')].map((x) => x.getAttribute('data-sid')) }; })()`);
await settle('sdr');
N.sdr = await ev(`(async () => { const n = (document.querySelector('.readat').textContent.replace(/,/g, '').match(/(\\d+)\\s*people/) || [])[1];
  const t = GW.TABS.sdr; const all = []; for (let k = 0; k < 200 && document.querySelector('[data-more="sdr"]'); k++) { document.querySelector('[data-more="sdr"]').click(); await new Promise((r) => setTimeout(r, 30)); }
  return { n, emails: [...document.querySelectorAll('td[data-l="Email"] .em')].map((e) => e.textContent.toLowerCase()), completed: [...document.querySelectorAll('td[data-l="Stage"]')].filter((x) => x.textContent.trim() === 'Completed').length }; })()`);
await settle('model');
N.model = await ev(`(() => { const v = (sel) => [...document.querySelectorAll(sel)].map((e) => e.getAttribute('data-v')); const card = (id) => document.querySelector('[data-card="' + id + '"] .num');
  return { ladder: v('[data-row] b[data-v]'), total: (document.querySelector('#view .foot b[data-v]') || {}).textContent, groups: [...document.querySelectorAll('.mgroup .sh-q')].map((e) => (e.textContent.replace(/,/g, '').match(/(\\d+)/) || [])[1]),
    cache: [card('mdl-cache') && card('mdl-cache').getAttribute('data-v')], decisionsQual: (document.querySelector('#mdl-dec .sh-q').textContent.replace(/,/g, '').match(/(\\d+)/) || [])[1],
    nearTotal: (document.querySelector('#mdl-near b[data-v]') || { getAttribute: () => null }).getAttribute('data-v') }; })()`);
await settle('visitors');
N.vis = await ev(`(() => ({ cov: ['vis-leads', 'vis-addr', 'vis-place', 'vis-distinct'].map((i) => document.querySelector('[data-card="' + i + '"] .num').getAttribute('data-v')), repeats: document.querySelectorAll('[data-x^="visr-"]').length,
  networks: [...document.querySelectorAll('#vis-networks .vbar b')].map((b) => b.getAttribute('data-v')), places: document.querySelectorAll('#vis-places tr:has(td)').length }))()`);
await settle('partners'); await sleep(800);
N.partners = await ev(`(() => ({ cards: ['p-attn', 'p-domains', 'p-sfwait', 'p-leads', 'p-conv', 'p-qual'].map((i) => document.querySelector('[data-card="' + i + '"] .num').getAttribute('data-v')),
  funnel: [...document.querySelectorAll('.pfs .num')].map((e) => e.getAttribute('data-v')), domains: document.querySelectorAll('td.kcell[data-l="Company"]').length, partners: document.querySelectorAll('[data-x^="pp-"]').length }))()`);
ws.close(); cleanup();

/* compare */
const same = (a, b) => JSON.stringify(a) === JSON.stringify(b);
check('leads', 'the count: payload = classic = new', num(C.leads.total) === leadsP.total && num(N.leads.total) === leadsP.total, [leadsP.total, C.leads.total, N.leads.total].join(' / '));
check('leads', 'page 1, same leads in the same order', same(C.leads.sids, leadsP.leads.map((l) => l.session_id)) && same(N.leads.sids, C.leads.sids), C.leads.sids.length + ' rows');
check('leads', 'every stage agrees ("Step 1" is now "Left on step 2")', same(C.leads.stages.map((s) => s === 'Step 1' ? 'Left on step 2' : s), N.leads.stages), N.leads.stages.length);
check('blocked', 'all blocked: payload = classic = new', num(C.blocked.a) === blkP.total && num(N.blocked.a) === blkP.total, [blkP.total, C.blocked.a, N.blocked.a].join(' / '));
check('blocked', 'excluding ours: payload = classic = new', num(C.blocked.b) === blkExP.total && num(N.blocked.b) === blkExP.total, [blkExP.total, C.blocked.b, N.blocked.b].join(' / '));
check('blocked', 'the people figure is the Overview\'s own counter', num(N.blocked.people) === Number(metrics.peopleNonIcp), N.blocked.people + ' vs ' + metrics.peopleNonIcp);
check('blocked', 'same blocked leads, same order', same(C.blocked.sids, N.blocked.sids), C.blocked.sids.length + ' rows');
check('sdr', 'the count: payload = classic = new', num(C.sdr.n) === sdr.total && num(N.sdr.n) === sdr.total, [sdr.total, C.sdr.n, N.sdr.n].join(' / '));
check('sdr', 'every person, in the same order (all pages)', same(C.sdr.emails, N.sdr.emails) && N.sdr.emails.length === sdr.total, N.sdr.emails.length + ' of ' + sdr.total);
check('sdr', 'the Completed count agrees', C.sdr.completed === N.sdr.completed, C.sdr.completed + ' / ' + N.sdr.completed);
check('model', 'the five ladder rows: payload = classic = new', same(C.model.ladder.map(num), model.ladder.rows.map((r) => r.n)) && same(N.model.ladder.map(num), model.ladder.rows.map((r) => r.n)), model.ladder.rows.map((r) => r.n).join(','));
check('model', 'the lead total', num(C.model.total) === model.ladder.total && num(N.model.total) === model.ladder.total, [model.ladder.total, C.model.total, N.model.total].join(' / '));
check('model', 'the three groups', same(C.model.groups.map(num), model.industries.map((g) => g.leads)) && same(N.model.groups.map(num), model.industries.map((g) => g.leads)), model.industries.map((g) => g.leads).join(','));
check('model', 'the all-time cache (the incident): never 0 while it has rows', num(C.model.cache[0]) === model.cache.domains && num(N.model.cache[0]) === model.cache.domains && model.cache.domains > 0, [model.cache.domains, C.model.cache[0], N.model.cache[0]].join(' / '));
check('model', 'decisions: one per acted lead, on both', C.model.decisions === model.decisions.length && num(N.model.decisionsQual) === model.decisions.length, [model.decisions.length, C.model.decisions, N.model.decisionsQual].join(' / '));
check('model', 'near misses total', num(C.model.nearTotal) === model.nearMisses.total && num(N.model.nearTotal) === model.nearMisses.total, [model.nearMisses.total, C.model.nearTotal, N.model.nearTotal].join(' / '));
const vc = vis.coverage;
check('visitors', 'the four coverage numbers', same(C.vis.cov.slice(0, 4).map(num), [vc.leads, vc.with_address, vc.with_place, vc.distinct_addresses]) && same(N.vis.cov.map(num), [vc.leads, vc.with_address, vc.with_place, vc.distinct_addresses]), [vc.leads, vc.with_address, vc.with_place, vc.distinct_addresses].join(','));
check('visitors', 'the merged networks, largest first', same(C.vis.networks.filter(Boolean).map(num), N.vis.networks.map(num)), N.vis.networks.slice(0, 5).join(','));
check('visitors', 'every place is listed', C.vis.places === N.vis.places && N.vis.places === ((vis.places && vis.places.rows) || []).length, ((vis.places && vis.places.rows) || []).length + ' / ' + C.vis.places + ' / ' + N.vis.places);
check('visitors', 'every repeat address is listed', N.vis.repeats === vis.repeats.rows.length, N.vis.repeats + ' of ' + vis.repeats.rows.length);
check('partners', 'the six cards: classic = new = payload', same(C.partners.cards.map(num), N.partners.cards.map(num)) && num(N.partners.cards[0]) === Number(lc.needsAttention), C.partners.cards.join(',') + ' / ' + N.partners.cards.join(','));
check('partners', 'the nine funnel headlines', same(C.partners.funnel.map(num), N.partners.funnel.map(num)), N.partners.funnel.join(','));
check('partners', 'every partner company is listed', C.partners.domains === N.partners.domains && N.partners.domains === lc.domains.length, [lc.domains.length, C.partners.domains, N.partners.domains].join(' / '));
check('partners', 'every partner is listed', C.partners.partners === N.partners.partners && N.partners.partners === partners.partners.length, [partners.partners.length, C.partners.partners, N.partners.partners].join(' / '));

const bad = results.filter((r) => !r.pass);
for (const r of results) console.log((r.pass ? '✓ ' : '✗ ') + r.tab.padEnd(9) + r.what + (r.detail ? '  [' + r.detail + ']' : ''));
console.log(`\n${results.length} checks, ${bad.length} failed`);
process.exit(bad.length ? 1 : 0);
