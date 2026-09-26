/* ============================================================================
   check-monitor-layout.mjs -- the layout check the test bar cannot make.

   tests/test-monitor-next.js proves the numbers painted match the payload. It
   cannot see LAYOUT: a column running off a phone, a label drawn on top of
   another, a button too small for a thumb. Two dashboard bugs in one week were
   exactly that, and both passed every assertion. So this drives real Chrome
   over every tab and view, at every width, in both themes, and FAILS on:

     overflow      the page scrolls sideways, or an element runs off-screen
                   outside a container that is meant to scroll
     clipped       text cut off by its own box
     tap           a control smaller than 44x44 at a touch width (<= 1023)
     overlap       two chart labels drawn on top of each other
     floating      a fixed element that could sit over content
     errors        a console error, an exception, or a failed request
     junk          "undefined", "NaN", "[object" or "Infinity" painted on screen
     fonts         a brand font that did not load
     hscroll       a data table wider than its card, so its right-hand columns
                   sit off the edge (Dropoff's period table scrolls on purpose)
     keys          real key presses: the skip link on the first Tab, focus
                   kept across a repaint, the drawer closing when focus leaves

   It reads and clicks nothing that writes: point it at tools/preview-monitor.js,
   which refuses every non-GET.

   Run: node tools/check-monitor-layout.mjs   (preview running; reads its ready file)
        WIDTHS=390,1440 THEMES=dark node tools/check-monitor-layout.mjs
   Exit 0 only when every combination is clean. Screenshots land in OUT.
   ============================================================================ */
import { spawn } from 'node:child_process';
import { readFileSync, writeFileSync, mkdirSync, existsSync } from 'node:fs';

const READY = process.env.PREVIEW_READY_FILE;
const cfg = READY && existsSync(READY) ? JSON.parse(readFileSync(READY, 'utf8')) : {};
const BASE = process.env.BASE || `http://localhost:${cfg.port || 4411}`;
const TOKEN = process.env.MONITOR_TOKEN || cfg.token || '';
const OUT = process.env.OUT || './layout-shots';
const WIDTHS = (process.env.WIDTHS || '360,390,414,768,1024,1440').split(',').map(Number);
const THEMES = (process.env.THEMES || 'light,dark').split(',');
const PAGES = (process.env.PAGES || 'overview:today,overview:week,overview:all,overview:week:leads,overview:today:table,health,dropoff,dupes,lm').split(',');
const SHOTS = process.env.SHOTS !== '0';
mkdirSync(OUT, { recursive: true });

const chrome = spawn('/Applications/Google Chrome.app/Contents/MacOS/Google Chrome', ['--headless=new', '--remote-debugging-port=9335',
  '--user-data-dir=' + OUT + '/.profile', '--no-first-run', '--hide-scrollbars', '--force-color-profile=srgb', 'about:blank'], { stdio: 'ignore' });
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
let t; for (let i = 0; i < 60; i++) { try { t = await (await fetch('http://127.0.0.1:9335/json')).json(); if (t.length) break; } catch {} await sleep(250); }
const ws = new WebSocket(t.find((x) => x.type === 'page').webSocketDebuggerUrl);
await new Promise((r) => ws.addEventListener('open', r));
let id = 0; const pend = new Map(); let events = [];
ws.addEventListener('message', (e) => { const m = JSON.parse(e.data);
  if (m.id && pend.has(m.id)) { pend.get(m.id)(m); pend.delete(m.id); return; }
  if (m.method === 'Runtime.exceptionThrown') events.push('exception: ' + (m.params.exceptionDetails.exception?.description || m.params.exceptionDetails.text).split('\n')[0]);
  if (m.method === 'Runtime.consoleAPICalled' && m.params.type === 'error') events.push('console.error: ' + m.params.args.map((a) => a.value ?? a.description).join(' ').slice(0, 200));
  if (m.method === 'Network.responseReceived' && m.params.response.status >= 400) events.push('HTTP ' + m.params.response.status + ' ' + m.params.response.url.replace(/token=[^&]+/, 'token=***').replace(BASE, ''));
  if (m.method === 'Network.loadingFailed' && !m.params.canceled) events.push('request failed: ' + m.params.errorText); });
const send = (method, params = {}) => new Promise((r) => { const i = ++id; pend.set(i, r); ws.send(JSON.stringify({ id: i, method, params })); });
const ev = async (x) => { const r = await send('Runtime.evaluate', { expression: x, awaitPromise: true, returnByValue: true }); if (r.result?.exceptionDetails) throw new Error(r.result.exceptionDetails.exception?.description || 'eval failed'); return r.result?.result?.value; };
await send('Page.enable'); await send('Runtime.enable'); await send('Network.enable');

/* Everything below runs IN the page and returns a list of findings. */
const AUDIT = `(() => {
  const W = innerWidth, out = [], touch = W <= 1023;
  const vis = (el) => { const cs = getComputedStyle(el); if (cs.display === 'none' || cs.visibility === 'hidden' || +cs.opacity === 0) return false; const r = el.getBoundingClientRect(); return r.width > 0 && r.height > 0; };
  const hiddenAncestor = (el) => { for (let a = el; a; a = a.parentElement) { if (a.hidden) return true; const cs = getComputedStyle(a); if (cs.display === 'none' || cs.visibility === 'hidden') return true; } return false; };
  const inScroller = (el) => { for (let a = el.parentElement; a && a !== document.body; a = a.parentElement) { const ox = getComputedStyle(a).overflowX; if ((ox === 'auto' || ox === 'scroll') && a.scrollWidth > a.clientWidth + 1) return a; } return null; };
  const name = (el) => (el.getAttribute('aria-label') || el.textContent || el.getAttribute('title') || el.tagName).trim().replace(/\\s+/g, ' ').slice(0, 40);
  if (document.documentElement.scrollWidth > W + 1) out.push(['overflow', 'the page scrolls sideways: ' + document.documentElement.scrollWidth + 'px on a ' + W + 'px screen']);
  for (const el of document.querySelectorAll('#view *, .topbar *')) {
    if (el.closest('svg') && el.tagName !== 'svg') continue;
    if (hiddenAncestor(el) || !vis(el)) continue;
    const r = el.getBoundingClientRect();
    if ((r.right > W + 1 || r.left < -1) && !inScroller(el)) { out.push(['overflow', el.tagName.toLowerCase() + '.' + (el.className.baseVal ?? el.className) + ' "' + name(el) + '" runs off-screen (' + Math.round(r.left) + '..' + Math.round(r.right) + ')']); }
    const cs = getComputedStyle(el);
    /* .sr-only is clipped ON PURPOSE -- it is the screen-reader label, visually hidden by design. */
    if ((cs.overflowX === 'hidden' || cs.overflowX === 'clip') && el.scrollWidth > el.clientWidth + 1 && el.textContent.trim() && !el.classList.contains('tr') && !el.closest('.tr') && !el.closest('.sr-only')) out.push(['clipped', '"' + name(el) + '" is cut off by its own box']);
  }
  /* VISUALLY HIDDEN ON PURPOSE -- .sr-only, and the skip link until it is
     focused -- is a 1px clipped box, not a target a thumb has to hit. Judged
     by the rule that hides it, never by a class list, so the next hidden
     control needs no edit here. Focused, the skip link IS measured. */
  const srHidden = (el) => { const r = el.getBoundingClientRect(), cs = getComputedStyle(el); return r.width <= 1 && r.height <= 1 && cs.position === 'absolute' && cs.clip !== 'auto'; };
  if (touch) for (const el of document.querySelectorAll('button, a[href], select, input, summary, [tabindex]:not([tabindex="-1"])')) {
    if (el.classList.contains('sr-only') || el.closest('.sr-only') || srHidden(el) || hiddenAncestor(el) || !vis(el)) continue;
    if (el.closest('#drawer') && !document.getElementById('drawer').classList.contains('open')) continue;
    const r = el.getBoundingClientRect();
    if (r.width < 43.5 || r.height < 43.5) out.push(['tap', '"' + name(el) + '" is ' + Math.round(r.width) + 'x' + Math.round(r.height) + ' (needs 44x44)']);
  }
  /* A DATA TABLE WIDER THAN ITS CARD: the overflow audit above excuses
     anything inside a scroller, which is exactly where a cut-off column
     hides. The SDR list shipped its last column off the edge at 1440 and
     passed every check. */
  for (const w of document.querySelectorAll('#view .rt-wrap')) {
    if (!vis(w) || hiddenAncestor(w)) continue;
    if (w.scrollWidth > w.clientWidth + 1) out.push(['hscroll', 'a table is ' + (w.scrollWidth - w.clientWidth) + 'px wider than its card (' + (w.querySelector('th') ? [...w.querySelectorAll('thead th')].map((t) => t.textContent.trim()).filter(Boolean).slice(-2).join(', ') : '') + ' cut off)']);
  }
  for (const svg of document.querySelectorAll('#view .chart svg')) {
    const ts = [...svg.querySelectorAll('text')].map((x) => ({ x, r: x.getBoundingClientRect() })).filter((o) => o.r.width > 0);
    for (let i = 0; i < ts.length; i++) for (let j = i + 1; j < ts.length; j++) {
      const a = ts[i].r, b = ts[j].r, w = Math.min(a.right, b.right) - Math.max(a.left, b.left), h = Math.min(a.bottom, b.bottom) - Math.max(a.top, b.top);
      if (w > 1 && h > 1) out.push(['overlap', 'chart labels "' + ts[i].x.textContent + '" and "' + ts[j].x.textContent + '" overlap']);
    }
    const sr = svg.getBoundingClientRect();
    for (const o of ts) if (o.r.right > sr.right + 2 || o.r.left < sr.left - 2) out.push(['overflow', 'chart label "' + o.x.textContent + '" is outside its chart']);
  }
  for (const el of document.querySelectorAll('body *')) {
    const cs = getComputedStyle(el); if (cs.position !== 'fixed' || !vis(el)) continue;
    if (el.id === 'drawer' || el.id === 'drawer-catch') continue;
    out.push(['floating', el.tagName.toLowerCase() + '#' + el.id + '.' + el.className + ' is fixed and can sit over content']);
  }
  const junk = (document.getElementById('view').innerText.match(/\\[object|\\bundefined\\b|\\bNaN\\b|Infinity/g) || []);
  if (junk.length) out.push(['junk', 'painted: ' + [...new Set(junk)].join(', ')]);
  if (!document.fonts.check('600 32px "Vert Grotesk Display"') || !document.fonts.check('500 14px Inter')) out.push(['fonts', 'a brand font did not load']);
  const loading = document.querySelectorAll('#view .skel, #view [aria-busy="true"]').length;
  return { out, loading, h: document.documentElement.scrollHeight, theme: document.documentElement.getAttribute('data-theme') };
})()`;

const results = [];
let first = true;
for (const width of WIDTHS) {
  const height = width < 768 ? 800 : width < 1280 ? 1024 : 900;
  await send('Emulation.setDeviceMetricsOverride', { width, height, deviceScaleFactor: 1, mobile: width < 768 });
  await send('Emulation.setTouchEmulationEnabled', { enabled: width <= 1023, maxTouchPoints: width <= 1023 ? 5 : 1 });
  for (const theme of THEMES) {
    if (first) { await send('Page.navigate', { url: `${BASE}/monitor/next?token=${encodeURIComponent(TOKEN)}#tab=overview&view=week` }); await sleep(2500); first = false; }
    await ev(`GW.setTheme(${JSON.stringify(theme)})`);
    for (const pg of PAGES) {
      const [tab, view, mod] = pg.split(':');
      events = [];
      await ev(`(()=>{ GW.S.unit=${JSON.stringify(mod === 'leads' ? 'leads' : 'people')}; GW.S.table=${mod === 'table'}; if (GW.current() !== ${JSON.stringify(tab)}) GW.show(${JSON.stringify(tab)}); ${view ? `GW.TABS.overview.setView(${JSON.stringify(view)});` : ''} if (GW.TABS[${JSON.stringify(tab)}].render) GW.TABS[${JSON.stringify(tab)}].render(); })()`);
      /* Wait for the data: no skeleton left, or give up after 20s and say so. */
      let a; for (let k = 0; k < 40; k++) { await sleep(500); a = await ev(AUDIT); if (!a.loading) break; }
      await sleep(300); a = await ev(AUDIT);
      const issues = a.out.map(([kind, what]) => ({ kind, what }));
      if (a.loading) issues.push({ kind: 'errors', what: 'still loading after 20s' });
      for (const e of events) if (!/favicon/.test(e)) issues.push({ kind: 'errors', what: e });
      if (a.theme !== theme) issues.push({ kind: 'errors', what: 'theme is ' + a.theme + ', expected ' + theme });
      const label = `${width}-${theme}-${pg.replace(/:/g, '-')}`;
      if (SHOTS) { await send('Emulation.setDeviceMetricsOverride', { width, height: Math.min(a.h, 6000), deviceScaleFactor: 1, mobile: width < 768 }); await sleep(250);
        const s = await send('Page.captureScreenshot', { format: 'png' }); writeFileSync(`${OUT}/${label}.png`, Buffer.from(s.result.data, 'base64'));
        await send('Emulation.setDeviceMetricsOverride', { width, height, deviceScaleFactor: 1, mobile: width < 768 }); }
      results.push({ label, issues });
      process.stdout.write((issues.length ? '✗ ' : '✓ ') + label + (issues.length ? '  ' + issues.length + ' issue(s)' : '') + '\n');
    }
    /* The drawer, at every width that uses it: opens, is thumb-sized, closes on Escape, and returns focus. */
    if (width <= 1023) {
      events = [];
      const d = await ev(`(async()=>{ document.querySelector('.menu-trigger').click(); await new Promise(r=>setTimeout(r,300));
        const dr=document.getElementById('drawer'), open=dr.classList.contains('open'), focusIn=dr.contains(document.activeElement);
        const small=[...dr.querySelectorAll('a,button')].filter(e=>{const r=e.getBoundingClientRect();return r.width>0&&(r.width<43.5||r.height<43.5);}).map(e=>(e.getAttribute('aria-label')||e.textContent).trim().slice(0,30));
        const off=[...dr.querySelectorAll('*')].filter(e=>{const r=e.getBoundingClientRect();return r.width>0&&r.right>innerWidth+1;}).length;
        document.dispatchEvent(new KeyboardEvent('keydown',{key:'Escape'})); await new Promise(r=>setTimeout(r,300));
        return {open, focusIn, small, off, closed: !dr.classList.contains('open'), back: document.activeElement===document.querySelector('.menu-trigger')}; })()`);
      const issues = [];
      if (!d.open) issues.push({ kind: 'errors', what: 'the drawer did not open' });
      if (!d.focusIn) issues.push({ kind: 'errors', what: 'focus did not move into the drawer' });
      for (const s of d.small) issues.push({ kind: 'tap', what: 'drawer "' + s + '" is under 44x44' });
      if (d.off) issues.push({ kind: 'overflow', what: d.off + ' drawer element(s) run off-screen' });
      if (!d.closed) issues.push({ kind: 'errors', what: 'Escape did not close the drawer' });
      if (!d.back) issues.push({ kind: 'errors', what: 'focus did not return to the menu button' });
      results.push({ label: `${width}-${theme}-drawer`, issues });
      process.stdout.write((issues.length ? '✗ ' : '✓ ') + `${width}-${theme}-drawer` + (issues.length ? '  ' + issues.length + ' issue(s)' : '') + '\n');
    }
  }
}
/* THE KEYBOARD, with REAL key events (CDP's are trusted, so the browser moves
   focus itself). Four things the audits above cannot see: the first Tab lands
   on a skip link you can SEE; Enter on it moves focus into the page and leaves
   the hash alone; a repaint gives focus back to the control that had it; and
   Shift+Tab out of the open drawer closes it rather than walking focus onto
   what it covers. */
const key = async (k, shift) => { const base = { key: k, code: k, windowsVirtualKeyCode: k === 'Tab' ? 9 : 13, modifiers: shift ? 8 : 0 };
  await send('Input.dispatchKeyEvent', Object.assign({ type: 'rawKeyDown' }, base)); if (k === 'Enter') await send('Input.dispatchKeyEvent', Object.assign({ type: 'char', text: '\r' }, base));
  await send('Input.dispatchKeyEvent', Object.assign({ type: 'keyUp' }, base)); await sleep(150); };
{
  const issues = [];
  await send('Emulation.setDeviceMetricsOverride', { width: 1440, height: 900, deviceScaleFactor: 1, mobile: false });
  await send('Emulation.setTouchEmulationEnabled', { enabled: false, maxTouchPoints: 1 });
  /* about:blank FIRST: the same URL with a different #hash is a same-page
     jump, not a load, so focus would stay wherever the checks above left it
     and the "first Tab" would not be the first. */
  await send('Page.navigate', { url: 'about:blank' }); await sleep(300);
  await send('Page.navigate', { url: `${BASE}/monitor/next?token=${encodeURIComponent(TOKEN)}#tab=overview&view=week&unit=leads` }); await sleep(3000);
  await key('Tab');
  const sk = await ev(`(()=>{ const a=document.activeElement, r=a.getBoundingClientRect(); return { cls: a.className, w: r.width, h: r.height, top: r.top }; })()`);
  if (sk.cls !== 'skip') issues.push({ kind: 'keys', what: 'the first Tab landed on "' + sk.cls + '", not the skip link' });
  else if (sk.w < 44 || sk.h < 44 || sk.top < 0) issues.push({ kind: 'keys', what: 'the focused skip link is not visible at 44px (' + Math.round(sk.w) + 'x' + Math.round(sk.h) + ')' });
  await key('Enter');
  const af = await ev(`({ id: document.activeElement.id, hash: location.hash })`);
  if (af.id !== 'view') issues.push({ kind: 'keys', what: 'Enter on the skip link left focus on "' + af.id + '"' });
  if (!/tab=overview/.test(af.hash) || !/unit=leads/.test(af.hash)) issues.push({ kind: 'keys', what: 'the skip link changed the hash to ' + af.hash });
  const kept = await ev(`(()=>{ const b=document.querySelector('[data-unit="leads"]'); b.focus(); GW.TABS.overview.render(); const a=document.activeElement; return a && a.getAttribute('data-unit'); })()`);
  if (kept !== 'leads') issues.push({ kind: 'keys', what: 'a repaint dropped focus (it is on ' + kept + ')' });
  await send('Emulation.setDeviceMetricsOverride', { width: 390, height: 800, deviceScaleFactor: 1, mobile: true });
  await sleep(400);
  await ev(`document.querySelector('.menu-trigger').click()`); await sleep(350);
  const open1 = await ev(`document.getElementById('drawer').classList.contains('open') && document.getElementById('drawer').contains(document.activeElement)`);
  if (!open1) issues.push({ kind: 'keys', what: 'the drawer did not open with focus inside it' });
  await key('Tab', true); await sleep(250);
  const dr = await ev(`({ open: document.getElementById('drawer').classList.contains('open'), inside: document.getElementById('drawer').contains(document.activeElement) })`);
  if (dr.open && !dr.inside) issues.push({ kind: 'keys', what: 'focus left the drawer and the drawer stayed open over it' });
  if (dr.open && dr.inside) issues.push({ kind: 'keys', what: 'Shift+Tab from the first drawer item stayed inside the drawer' });
  results.push({ label: 'keyboard', issues });
  process.stdout.write((issues.length ? '✗ ' : '✓ ') + 'keyboard' + (issues.length ? '  ' + issues.length + ' issue(s)' : '') + '\n');
}
writeFileSync(`${OUT}/report.json`, JSON.stringify(results, null, 1));
const bad = results.filter((r) => r.issues.length);
const byKind = {}; bad.forEach((r) => r.issues.forEach((i) => { byKind[i.kind] = (byKind[i.kind] || 0) + 1; }));
console.log(`\n${results.length} combinations, ${bad.length} with findings` + (bad.length ? ' — ' + Object.entries(byKind).map(([k, n]) => k + ' ' + n).join(', ') : ''));
for (const r of bad.slice(0, 40)) { console.log(' ✗ ' + r.label); for (const i of r.issues.slice(0, 6)) console.log('     ' + i.kind + ': ' + i.what); }
ws.close(); chrome.kill();
process.exit(bad.length ? 1 : 0);
