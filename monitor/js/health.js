/* ============================================================================
   System health -- ported from the old tab, rule for rule.

   HEALTH CHECKS FAIL LOUD (CLAUDE.md). A green row means "verified working,
   just now"; a check that could not run is RED, never grey -- grey means only
   "too little traffic to judge". A failed fetch paints every row red.

   /monitor/health is slow on purpose (the AWS row crosses a WAN), so it runs
   when the tab opens, every five minutes, and on Re-check -- never on a
   one-minute poll -- and pauses while the tab is hidden.
   ============================================================================ */
/* Names an SDR reads -- they head the Overview's red strip as well as these
   rows. The route or table behind each is in the row's description. */
GW.HEALTH_NAMES = {
  partial: 'Form step 1', submit: 'Form step 2', apollo: 'Apollo enrichment', booking: 'Booking — RevenueHero',
  cron: 'Drop-off recovery schedule', aws: 'Dialer mirror (AWS)', recovery: 'Follow-up emails', partnerstack: 'PartnerStack', nonicpllm: 'Non-ICP model',
};
/* The two rows that do not come from /monitor/health. Kept OUT of
   HEALTH_NAMES on purpose: checkAll marks every HEALTH_NAMES key it cannot
   find in /monitor/health as "No result", which would paint these red. */
GW.HEALTH_EXTRA_NAMES = { api: 'API uptime', elv: 'Email verification (ELV)' };
/* What each red row costs, in the words an SDR reads. Mirrors HEALTH_ALERT_META's impact lines. */
GW.HEALTH_IMPACT = {
  partial: 'Visitors are reaching the form and nothing is being saved.',
  submit: 'People are entering an email and none are getting through step 2.',
  apollo: 'Leads still arrive and book; they arrive without title, company size or industry.',
  booking: 'People are completing the form and no booking is landing on any lead.',
  cron: 'Drop-off follow-up emails are not being sent.',
  aws: 'The dialer reads the mirror and is not seeing new leads.',
  recovery: 'Follow-up emails are stuck in the queue.',
  partnerstack: 'A partner referral may not be credited.',
  nonicpllm: 'The website check cannot classify leads; the brand-domain list still blocks.',
  api: 'The server is not answering: the form cannot save anyone, and nothing else on this page can be trusted.',
  elv: 'Email checks are not reaching a verdict. Leads still go through; they are marked unverified rather than stopped.',
};
GW.TABS.health = (function (G) {
  var U = G.ui, esc = G.esc, fmt = G.fmt;
  var ROWS = [
    ['api', G.HEALTH_EXTRA_NAMES.api, 'The server answering at all · /health'],
    ['partial', G.HEALTH_NAMES.partial, 'Leads saved in the last 2 hours, against form sessions · /partial'],
    ['submit', G.HEALTH_NAMES.submit, 'Completions in the last 24 hours · /submit'],
    ['elv', G.HEALTH_EXTRA_NAMES.elv, 'Share of email checks that came back inconclusive, last 90 minutes'],
    ['apollo', G.HEALTH_NAMES.apollo, 'Business-email leads Apollo found, last 24 hours — red whenever Apollo is refusing lookups'],
    ['booking', G.HEALTH_NAMES.booking, 'People booked / people completed, last 7 days'],
    ['cron', G.HEALTH_NAMES.cron, 'Time since the scheduler last called us for drop-off emails'],
    ['aws', G.HEALTH_NAMES.aws, 'The copy of our leads the dialer reads, compared live with ours · gw_form_leads'],
    ['partnerstack', G.HEALTH_NAMES.partnerstack, 'Conversions and qualifications in the last 24h — red on any failure'],
    ['nonicpllm', G.HEALTH_NAMES.nonicpllm, 'Website classification in the last 24h — red when nothing can be classified. Grey means the layer is switched off'],
    ['recovery', G.HEALTH_NAMES.recovery, 'Follow-up queue — anything stuck past 5 hours'],
  ];
  var CLS = { green: 'b-good', amber: 'b-warn', red: 'b-bad', insufficient_data: 'b-neu' };
  var st = {}, checkedAt = null, cov = null, covErr = null, metrics = null, metricsErr = null, root = null, busy = false;

  /* A TIMEOUT, like every other call here. With none, a /health that hung
     held every row at grey "Checking..." and Re-check disabled for as long
     as it hung -- grey where the rule is RED: a check that cannot verify is
     never allowed to look like anything but a failure. */
  function apiResult() {
    var init = { cache: 'no-store' };
    try { if (typeof AbortSignal !== 'undefined' && AbortSignal.timeout) init.signal = AbortSignal.timeout(8000); } catch (e) {}
    return fetch('/health', init).then(function (r) { return r.ok ? { cls: 'b-good', text: 'Online' } : { cls: 'b-bad', text: 'Offline — HTTP ' + r.status }; })
      .catch(function (e) { return { cls: 'b-bad', text: e && e.name === 'TimeoutError' ? 'Offline — no answer in 8s' : 'Offline' }; });
  }
  /* ELV reports its own state machine; the four shapes are the old tab's, unchanged. */
  function elvResult() {
    return G.api('/monitor/elv-health', {}, { timeout: 8000 }).then(function (d) {
      var age = (d.minutesSinceLastCheck !== null && d.minutesSinceLastCheck !== undefined && d.minutesSinceLastCheck >= 60) ? ' · last check ' + Math.round(d.minutesSinceLastCheck / 60) + 'h ago' : '';
      if (d.state === 'degraded') return { cls: 'b-bad', text: 'Degraded — ' + d.rate + '% of ' + d.checks + ' inconclusive' + age };
      if (d.state === 'insufficient_data') {
        if (d.rate >= 50 || d.consecutiveInconclusive >= 2) return { cls: 'b-warn', text: 'Low traffic — ' + d.rate + '% of ' + d.checks + ' inconclusive' + age };
        return { cls: 'b-neu', text: 'Quiet — ' + d.checks + ' checks, ' + d.rate + '% inconclusive' + age };
      }
      return { cls: 'b-good', text: 'Healthy (' + d.rate + '% inconclusive)' };
    }).catch(function () { return { cls: 'b-bad', text: 'Could not check' }; });
  }
  function checkApi() { return apiResult().then(function (r) { st.api = r; }); }
  function checkElv() { return elvResult().then(function (r) { st.elv = r; }); }
  /* The Overview's strip runs the SAME two checks, so the two tabs can
     never disagree about them. */
  G.healthExtra = function () { return Promise.all([apiResult(), elvResult()]).then(function (r) { return { api: r[0], elv: r[1] }; }); };
  function checkAll() {
    return G.api('/monitor/health', { _: Date.now() }, { timeout: 25000 }).then(function (d) {
      Object.keys(G.HEALTH_NAMES).forEach(function (k) {
        var c = d && d.checks && d.checks[k];
        st[k] = (!c || !CLS[c.state]) ? { cls: 'b-bad', text: 'No result' } : { cls: CLS[c.state], text: c.text, detail: c.detail };
      });
      checkedAt = new Date().toISOString();
    }).catch(function (e) {
      Object.keys(G.HEALTH_NAMES).forEach(function (k) { st[k] = { cls: 'b-bad', text: 'Could not check', detail: e.message }; });
      checkedAt = null; st._err = e.message;
    });
  }
  /* Its own fetch, after the rows: it crosses to Salesforce, and a slow
     Salesforce must never delay or blank the health rows. */
  function loadCoverage() {
    return G.api('/monitor/enrichment-coverage', { _: Date.now() }, { timeout: 45000 }).then(function (d) { cov = d; covErr = null; })
      .catch(function (e) { covErr = e.message; });
  }
  function loadMetrics() { return G.api('/monitor/metrics', {}, { timeout: 15000 }).then(function (d) { metrics = d; metricsErr = null; }).catch(function (e) { metricsErr = e.message; }); }
  /* EACH ROW PAINTS WHEN ITS OWN CHECK SETTLES -- one slow check can no
     longer hold the other ten at "Checking...". busy is cleared whatever
     happens, and even when this tab is no longer showing. */
  function reds() { var n = 0; Object.keys(st).forEach(function (k) { if (st[k] && st[k].cls === 'b-bad') n++; }); return n; }
  function run(asked) {
    if (busy) return Promise.resolve(); busy = true; render();
    var each = function (p) { return p.then(render, render); };
    return Promise.all([each(checkApi()), each(checkElv()), each(checkAll()), each(loadMetrics())])
      .then(function () { busy = false; if (G.navBadge) G.navBadge('health', reds()); render(); if (asked) G.announce('System health re-checked: ' + (reds() ? reds() + ' ' + G.plural(reds(), 'check') + ' red' : 'nothing red')); return loadCoverage(); },
            function () { busy = false; render(); })
      .then(render);
  }
  function activate(el) { root = el; render(); G.every('health', 300000, function () { run(false); }); return run(false); }
  function deactivate() { G.stop('health'); root = null; }

  function coverageHtml() {
    if (metricsErr && !metrics) return U.unavailable('Enrichment coverage', metricsErr);
    if (!metrics) return '<span class="skel"></span>';
    var m = metrics;
    return '<div class="sumgrid four">' +
      U.metricCard({ label: 'Enriched sessions', value: m.enriched, sub: 'sessions Apollo answered for' }) +
      U.metricCard({ label: 'With title', value: m.enrichTitlePct, unit: '%', sub: 'of enriched' }) +
      U.metricCard({ label: 'With funding data', value: m.enrichFundingPct, unit: '%', sub: 'of enriched' }) +
      U.metricCard({ label: 'With location', value: m.enrichLocationPct, unit: '%', sub: 'of enriched' }) + '</div>';
  }
  /* HELD vs SENT. When Salesforce cannot be read the three right-hand counts
     are UNKNOWN, not zero -- a zero reads as "Salesforce has none of it",
     the exact false alarm this row exists to prevent. */
  function sfHtml() {
    if (covErr && !cov) return U.unavailable('Enrichment reaching Salesforce', covErr) + '<div class="foot"><span>These are UNKNOWN, not zero.</span></div>';
    if (!cov) return '<span class="skel"></span>';
    /* "COMPLETED step 2", never "reached": everyone with a lead row reached
       step 2 (the Dropoff ladder's "Left on step 2" did), and these counts are
       the people who SENT it. */
    var d = cov, sub = fmt(d.submitted) + ' completed step 2 in ' + d.days + 'd';
    if (!d.ok) {
      return '<div class="sumgrid four">' + U.metricCard({ label: 'We hold it for', value: d.held, sub: sub }) +
        '<div class="card mc"><div class="klabel">Arrived in Salesforce</div>' + U.unavailable('Salesforce', d.reason || 'unknown') + '</div>' +
        '<div class="card mc"><div class="klabel">Held but never arrived</div>' + U.unavailable('Salesforce', null) + '</div>' +
        '<div class="card mc"><div class="klabel">No Salesforce Lead</div>' + U.unavailable('Salesforce', null) + '</div></div>' +
        '<div class="foot"><span>Salesforce could not be read, so the three counts on the right are UNKNOWN, not zero. We hold enrichment for ' + fmt(d.held) + ' of ' + fmt(d.submitted) + ' people who completed step 2 in the last ' + d.days + ' days.</span></div>';
    }
    var extra = (d.capped ? ' Capped at ' + d.limit + ' people — this is a page, not the population.' : '') + (d.skipped ? ' ' + d.skipped + ' address(es) skipped as unquotable.' : '');
    return '<div class="sumgrid four">' +
      U.metricCard({ label: 'We hold it for', value: d.held, sub: sub, title: 'People who completed step 2 in the window and for whom we hold Apollo enrichment on the submitted row. A population, not a sample.' }) +
      U.metricCard({ label: 'Arrived in Salesforce', value: d.heldAndArrived, sub: d.held ? G.pct1(d.heldAndArrived, d.held) + '% of what we hold' : 'nothing held to send' }) +
      U.metricCard({ label: 'Held but never arrived', value: d.heldNotArrived, sub: d.held ? G.pct1(d.heldNotArrived, d.held) + '% of what we hold' : '—', chip: d.heldNotArrived > 0 ? '<span class="badge b-bad">needs a look</span>' : '' }) +
      U.metricCard({ label: 'No Salesforce Lead', value: d.notInSalesforce, sub: 'completed step 2, no Salesforce Lead at all', chip: d.notInSalesforce > 0 ? '<span class="badge b-bad">needs a look</span>' : '' }) + '</div>' +
      '<div class="foot"><span>Everyone who completed step 2 in the last ' + d.days + ' days (' + fmt(d.submitted) + '), deduped by email. Apollo returning nothing is not a miss here — this compares only enrichment we actually hold. ' + fmt(d.inSalesforce) + ' of ' + fmt(d.submitted) + ' have a Salesforce Lead.' + esc(extra) + '</span></div>';
  }
  function render() {
    if (!root || (G.current && G.current() !== 'health')) return;
    /* The detail runs the FULL WIDTH of the row, left-aligned, under the name
       and the badge -- it was a right-aligned column squeezed beside the
       name, several ragged lines deep on a laptop. */
    var rows = ROWS.map(function (r) {
      var s = st[r[0]];
      var badge = s ? '<span class="badge ' + s.cls + '">' + esc(s.text) + '</span>' : '<span class="badge b-neu">Checking…</span>';
      return '<div class="hrow" data-check="' + r[0] + '"><div class="hn"><div class="n">' + esc(r[1]) + '</div><div class="d">' + esc(r[2]) + '</div></div><div class="s">' + badge + '</div>' +
        (s && s.detail ? '<div class="dt">' + esc(s.detail) + '</div>' : '') + '</div>';
    }).join('');
    G.paint(root, '<section class="ph"><div class="ph-top"><h1 class="title" tabindex="-1">System health</h1><span class="readat">' +
      (checkedAt ? 'Checked ' + esc(G.etTime(checkedAt)) + ' ET' : (st._err ? '<span class="badge b-bad">Health check unreachable</span>' : '')) + '</span></div>' +
      '<p class="lede">Live checks on every step a lead passes through. Grey means we could not call a check healthy or broken — usually too little traffic; the row says why.</p>' +
      '<div class="controls"><div class="filters"><button class="btn" data-recheck' + (busy ? ' disabled' : '') + '>' + G.ic('arrow-clockwise') + (busy ? 'Checking…' : 'Re-check') + '</button></div></div></section>' +
      U.panel({ title: 'Step health', qual: 'every five minutes while this tab is open', body: '<div class="hlist">' + rows + '</div>' }) +
      U.panel({ title: 'Enrichment coverage', qual: 'all time', body: coverageHtml() }) +
      U.panel({ title: 'Enrichment reaching Salesforce', qual: 'people who completed step 2, against the Lead Salesforce holds', body: sfHtml() }));
  }
  if (typeof document !== 'undefined' && document.addEventListener) document.addEventListener('click', function (e) { var b = e.target && e.target.closest ? e.target.closest('[data-recheck]') : null; if (b) run(true); });
  return { title: 'System health', activate: activate, deactivate: deactivate, render: render, run: run, _st: st };
})(GW);
