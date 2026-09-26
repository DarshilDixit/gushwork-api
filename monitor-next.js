/* ============================================================================
   monitor-next.js -- builds and serves the new dashboard at /monitor/next.

   The dashboard's front end lives in real files under monitor/ -- HTML-free
   CSS and plain classic scripts -- instead of 2,000 lines of HTML written
   inside JavaScript strings with every quote escaped, which is where every
   past dashboard break came from. There is still NO BUILD STEP: the files are
   read once at boot and stitched into one page, the same shape the old
   /monitor has always served, behind the same token.

   SIDE BY SIDE with the old dashboard until it is switched: /monitor is not
   touched by anything here. Tabs not yet rebuilt link across to it.

   Every data call the page makes goes to the existing /monitor/* routes (and
   /monitor/overview), so nothing here reads the database itself.
   ============================================================================ */
const fs = require('fs');
const path = require('path');

const DIR = path.join(__dirname, 'monitor');
/* ORDER MATTERS: core defines GW, ui and chart build on it, health defines the
   check names the Overview's attention strip reads, app boots last. */
const JS_ORDER = ['core.js', 'ui.js', 'chart.js', 'labels.js', 'health.js', 'overview.js', 'dropoff.js', 'dupes.js', 'lm.js', 'leads.js', 'sdr.js', 'model.js', 'app.js'];
/* The only files the asset route will ever send. An allowlist, never a path. */
const ASSETS = {
  'Inter-VariableFont_opsz_wght.ttf': 'font/ttf',
  'Vert_Grotesk_Display_VF.ttf': 'font/ttf',
};

function read(rel) { return fs.readFileSync(path.join(DIR, rel), 'utf8'); }

function buildSprite() {
  return fs.readdirSync(path.join(DIR, 'icons')).filter((f) => f.endsWith('.svg')).sort().map((f) => {
    const svg = read(path.join('icons', f));
    const inner = svg.replace(/^[\s\S]*?<svg[^>]*>/, '').replace(/<\/svg>\s*$/, '');
    return `<symbol id="i-${f.replace(/\.svg$/, '')}" viewBox="0 0 256 256">${inner}</symbol>`;
  }).join('');
}

/* Read ONCE, at require time. A missing file fails the boot loudly rather
   than serving a page with a hole in it. */
const PARTS = {};
function reload() {
  PARTS.tokens = read('tokens.css');
  PARTS.css = read('app.css');
  PARTS.js = JS_ORDER.map((f) => `/* ---- ${f} ---- */\n` + read(path.join('js', f))).join('\n');
  PARTS.sprite = buildSprite();
  PARTS.logo = read('logo-symbol.svg').replace(/<\?xml[^>]*>/, '').replace(/<svg /, '<svg aria-hidden="true" ');
}
reload();

/* The config the page reads. JSON, with "<" escaped so no value can close the
   script tag it sits in. */
function configScript(cfg) {
  return '<script>window.__GW__=' + JSON.stringify(cfg).replace(/</g, '\\u003c') + ';</script>';
}

function page({ token, tz, labels }) {
  const tq = token ? '?token=' + encodeURIComponent(token) : '';
  const css = PARTS.css.split('{{ASSET}}').join('/monitor/next/asset/').split('{{TOKENQ}}').join(tq);
  const themeBtns = '<button data-theme-set="light" aria-label="Light theme" title="Light"><svg class="ic" aria-hidden="true"><use href="#i-sun"/></svg></button>' +
    '<button data-theme-set="dark" aria-label="Dark theme" title="Dark"><svg class="ic" aria-hidden="true"><use href="#i-moon"/></svg></button>' +
    '<button data-theme-set="system" aria-label="Match my computer" title="Match my computer"><svg class="ic" aria-hidden="true"><use href="#i-monitor"/></svg></button>';
  const foot = '<div class="sfoot"><span><b>All times Eastern (ET)</b></span><a href="/monitor' + tq + '">Open the classic dashboard</a></div>';
  return '<!DOCTYPE html><html lang="en" data-theme="light"><head><meta charset="UTF-8">' +
    '<meta name="viewport" content="width=device-width,initial-scale=1">' +
    '<meta name="robots" content="noindex,nofollow"><meta name="color-scheme" content="light dark"><title>Gushwork Monitor</title>' +
    '<style>' + PARTS.tokens + '\n' + css + '</style>' +
    /* Theme before first paint, so a dark-mode viewer never sees a white flash. */
    '<script>try{var c=localStorage.getItem("gw-theme")||"system";var d=c==="dark"||(c==="system"&&matchMedia("(prefers-color-scheme: dark)").matches);document.documentElement.setAttribute("data-theme",d?"dark":"light");}catch(e){}</script>' +
    '</head><body>' +
    '<svg width="0" height="0" style="position:absolute" aria-hidden="true">' + PARTS.sprite + '</svg>' +
    /* VISIBLE WHEN FOCUSED: as .sr-only it stayed a 1px box while it held
       focus, so a sighted keyboard user's first Tab showed focus nowhere. */
    '<a class="skip" href="#view">Skip to content</a>' +
    /* one live region for the whole page: a region inside a tab is replaced
       on every repaint and announces nothing */
    '<div id="gw-live" class="sr-only" role="status" aria-live="polite"></div>' +
    '<header class="topbar"><div class="brand"><div class="logo-tile">' + PARTS.logo + '</div><span class="brand-name">Gushwork Monitor</span><span class="brand-tag">inbound leads</span></div>' +
    '<div class="actions"><button class="btn" data-refresh aria-label="Refresh"><svg class="ic" aria-hidden="true"><use href="#i-arrow-clockwise"/></svg><span class="desktop-only">Refresh</span></button>' +
    '<div class="itg desktop-only" role="group" aria-label="Theme">' + themeBtns + '</div>' +
    '<button class="ibtn menu-trigger" aria-label="Open the menu" aria-expanded="false" aria-controls="drawer"><svg class="ic" aria-hidden="true"><use href="#i-list"/></svg></button></div></header>' +
    '<div class="frame"><aside class="sidebar"><div class="side-in"><nav class="groups" id="nav-side" aria-label="Dashboard"></nav>' + foot + '</div></aside>' +
    '<main class="slot"><div class="content" id="view" tabindex="-1"></div></main></div>' +
    '<div class="drawer-catch" id="drawer-catch"></div>' +
    '<aside class="drawer" id="drawer" aria-label="Menu"><div class="drawer-tools"><span>Theme</span><div class="itg" role="group" aria-label="Theme">' + themeBtns + '</div></div>' +
    '<nav class="groups" id="nav-drawer" aria-label="Dashboard"></nav>' + foot + '</aside>' +
    configScript({ token: token || '', tz, classic: '/monitor', labels: labels || {} }) +
    '<script>' + PARTS.js + '</script></body></html>';
}

function authorised(req) {
  const token = process.env.MONITOR_TOKEN;
  return !token || req.query.token === token;
}

function mount(app, { tz, labels }) {
  app.get('/monitor/next', (req, res) => {
    if (!authorised(req)) return res.status(401).send('<h2 style="font-family:sans-serif;padding:2rem">401 — Unauthorized. Add ?token=YOUR_TOKEN to the URL.</h2>');
    res.set('Cache-Control', 'no-store');
    res.type('html').send(page({ token: req.query.token || '', tz, labels }));
  });
  app.get('/monitor/next/asset/:name', (req, res) => {
    if (!authorised(req)) return res.status(401).end();
    const type = Object.prototype.hasOwnProperty.call(ASSETS, req.params.name) ? ASSETS[req.params.name] : null;
    if (!type) return res.status(404).end();
    res.set('Cache-Control', 'private, max-age=86400');
    res.type(type).sendFile(path.join(DIR, 'fonts', req.params.name));
  });
}

/* reload() re-reads the files. tools/preview-monitor.js goes further and
   re-requires this whole module on every page load, so an edit to the MARKUP
   in page() shows too, not only CSS and scripts. Production reads once. */
module.exports = { mount, page, reload, PARTS, JS_ORDER, ASSETS, DIR };
