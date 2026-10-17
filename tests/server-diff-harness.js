/* ============================================================
   Loaded with `node -r` into BOTH servers that test-server-differential.js
   boots: the release from git and the working tree. It stubs pg, fetch and
   DNS identically in each and appends everything a request does to
   TRACE_FILE, one JSON line per event:
     { k: 'q', pool, text, params }        a database statement
     { k: 'f', method, url, headers, body } an outbound request
   Nothing leaves the box. Not a suite: it prints nothing and asserts
   nothing.
   ============================================================ */

const Module = require('module');
const fs = require('fs');
const dns = require('dns');

const TRACE = process.env.TRACE_FILE;
const rows = new Map();
const write = (o) => { try { fs.appendFileSync(TRACE, JSON.stringify(o) + '\n'); } catch (_) {} };

function leadsInsertRow(text, params) {
  const cols = (/INSERT INTO leads \(([^)]*)\)/.exec(text) || [])[1];
  const at = text.indexOf('VALUES (');
  if (!cols || at === -1) return {};
  const v = []; let depth = 0, cur = '';
  for (let i = at + 'VALUES ('.length; i < text.length; i++) {
    const ch = text[i];
    if (ch === '(') depth++;
    if (ch === ')') { if (depth === 0) break; depth--; }
    if (ch === ',' && depth === 0) { v.push(cur.trim()); cur = ''; } else cur += ch;
  }
  v.push(cur.trim());
  const row = {};
  cols.split(',').map((x) => x.trim()).forEach((name, i) => { const m = /^\$(\d+)$/.exec(v[i] || ''); row[name] = m ? params[Number(m[1]) - 1] : null; });
  return row;
}
function answer(text, params) {
  const flat = String(text).replace(/\s+/g, ' ').trim();
  if (/^SELECT submitted_at FROM leads WHERE session_id=\$1$/.test(flat)) return { rows: [{ submitted_at: null }], rowCount: 1 };
  if (/INSERT INTO leads/.test(flat) && /RETURNING/.test(flat)) {
    const r = leadsInsertRow(text, params); rows.set(params[0], r);
    return { rows: [Object.assign({}, r, { step_reached: 2 })], rowCount: 1 };
  }
  if (/UPDATE leads SET ps_signup_sent_at = NOW/.test(flat)) return { rows: [{ session_id: params[0] }], rowCount: 1 };
  if (/^SELECT non_icp_blocked, non_icp_reason FROM leads WHERE session_id = \$1$/.test(flat)) {
    const r = rows.get(params[0]);
    return { rows: r ? [{ non_icp_blocked: r.non_icp_blocked === true, non_icp_reason: r.non_icp_reason || null }] : [], rowCount: r ? 1 : 0 };
  }
  if (/^SELECT email, company, website, phone, non_icp_blocked, non_icp_reason FROM leads WHERE session_id=\$1$/.test(flat)) return { rows: [{ email: 'ada@acmewidgets.com', non_icp_blocked: false }], rowCount: 1 };
  if (/^SELECT email FROM leads WHERE session_id=\$1$/.test(flat)) return { rows: [{ email: 'ada@acmewidgets.com' }], rowCount: 1 };
  if (/FROM leads l LEFT JOIN enrichment_data e/.test(flat)) {
    return { rows: [{ session_id: params[0], email: 'ada@acmewidgets.com', website: 'acmewidgets.com', page_url: 'https://www.gushwork.ai/demo',
      website_check_reason: 'content_clean', website_check_failed: false, non_icp_blocked: false, non_icp_llm_flagged: false }], rowCount: 1 };
  }
  return { rows: [], rowCount: 0 };
}
class StubClient {
  constructor(label) { this.label = label; }
  async query(q, params) { const text = typeof q === 'string' ? q : q.text; write({ k: 'q', pool: this.label, text, params: params || null }); return answer(text, params || []); }
  release() {}
}
class StubPool {
  constructor(cfg) { this.label = cfg && cfg.host ? 'aws' : 'railway'; }
  async connect() { return new StubClient(this.label); }
  async query(q, params) { const text = typeof q === 'string' ? q : q.text; write({ k: 'q', pool: this.label, text, params: params || null }); return answer(text, params || []); }
  on() {} async end() {}
}
const origLoad = Module._load;
Module._load = function (request) {
  if (request === 'pg') return { Pool: StubPool, Client: StubClient, types: { setTypeParser() {} } };
  return origLoad.apply(this, arguments);
};

dns.promises.resolveMx = async () => [{ exchange: 'mx.examplehost.net', priority: 10 }];
dns.promises.resolveNs = async () => ['ns1.examplehost.net'];
dns.promises.resolve   = async () => ['198.51.100.7'];

const realFetch = global.fetch.bind(global);
global.fetch = async function (url, opts) {
  const u = String(url);
  if (/^http:\/\/127\.0\.0\.1:/.test(u)) return realFetch(url, opts);
  opts = opts || {};
  write({ k: 'f', method: String(opts.method || 'GET').toUpperCase(), url: u, headers: opts.headers || null,
          body: typeof opts.body === 'string' ? opts.body : (opts.body ? String(opts.body) : null) });
  const j = (o, status) => ({ ok: (status || 200) < 300, status: status || 200, headers: new Map(), json: async () => o, text: async () => JSON.stringify(o) });
  if (/oauth2\/token/.test(u))           return j({ access_token: 'stub', instance_url: 'https://stub.my.salesforce.com' });
  if (/\/query\/\?q=/.test(u))            return j({ totalSize: 1, done: true, records: [{ Id: '00Qstub' }] });
  if (/salesforce\.com/.test(u))         return j({ id: '00Qstub', success: true });
  if (/graph\.facebook\.com/.test(u))    return j({ events_received: 1 });
  if (/hooks\.slack\.com/.test(u))       return { ok: true, status: 200, text: async () => 'ok', json: async () => ({}) };
  if (/partnerlinks\.io|api\.partnerstack\.com/.test(u)) return { ok: true, status: 200, text: async () => '{}', json: async () => ({}) };
  if (/api\.apollo\.io/.test(u))         return j({ person: { first_name: 'Ada', last_name: 'Lovelace', title: 'Head of Growth', organization: { name: 'Acme Widgets', website_url: 'http://acmewidgets.com', estimated_num_employees: 120, industry: 'manufacturing' } } });
  if (/emaillistverify/.test(u))         return { ok: true, status: 200, text: async () => 'ok', json: async () => ({}) };
  if (/api\.anthropic\.com/.test(u))     return j({ error: { message: 'stub' } }, 500);
  if (/ipwho\.is/.test(u))               return j({ success: true, city: 'Austin', region: 'Texas', country: 'United States', country_code: 'US', latitude: 30.2, longitude: -97.7, timezone: { id: 'America/Chicago' }, connection: { isp: 'Stub', asn: 1, org: 'Stub', domain: 'stub.net' } });
  if (/dns\.google|cloudflare-dns/.test(u)) return j({ Status: 0, Answer: [{ name: 'x', type: 1, data: '198.51.100.7' }] });
  return { ok: true, status: 200, url: u, headers: new Map([['content-type', 'text/html']]),
           text: async () => '<html><head><title>Acme Widgets</title></head><body>Industrial pumps for distributors. Contact sales.</body></html>',
           json: async () => ({}) };
};

for (const level of ['log', 'info', 'warn', 'error']) console[level] = () => {};
