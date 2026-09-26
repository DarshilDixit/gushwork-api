/* ============================================================================
   Partners -- the PartnerStack programme, counted in COMPANIES (customer
   domains), because PartnerStack pays one conversion and one qualification
   per customer key, ever. Clicks are the one other unit, and say so.
   /monitor/partners and /monitor/partner-gaps, unchanged.

   THE LIFECYCLE LADDER (CLAUDE.md, Definitions): eight states, one per
   domain, in the server's resolution order; the chips add up to the domain
   total. "Needs attention" is the two red states, from its OWN unbounded
   count -- and when that count could not run it says AT LEAST, because a
   floor is never shown as a total.

   THE ONE WRITE: acknowledging a failure (POST /monitor/partner-ack, JSON
   body, token in the query). An ack means "understood, leave it alone" -- it
   stops the alert, the Needs-attention count and the retry sweep, and never
   clears the failure: the row stays red with its history. The classic asked
   with window.prompt, whose Cancel acknowledged anyway; here Cancel does
   nothing. tools/preview-monitor.js refuses every POST, so a preview can
   never send one.

   PARTNER REVENUE GAPS lives here now. The classic hid it inside the All
   Leads table header. It is a WORK QUEUE, not a health check: it never
   feeds System health or the Overview strip, and when Salesforce cannot be
   read it says "unavailable", never zero.
   ============================================================================ */
GW.TABS.partners = (function (G) {
  var U = G.ui, L = G.L, esc = G.esc, fmt = G.fmt;
  var STATE = { qualified: 'qualified', qualification_failed: 'qualification failed', conversion_failed: 'conversion failed', demo_done_not_qualified: 'demo done, not qualified',
                awaiting_demo: 'awaiting demo', converted: 'converted', skipped: 'skipped', conversion_pending: 'pending' };
  var LADDER = ['qualified', 'qualification_failed', 'conversion_failed', 'demo_done_not_qualified', 'awaiting_demo', 'converted', 'skipped', 'conversion_pending'];
  var FAILED = { conversion_failed: 1, qualification_failed: 1 };
  /* keyed by customer_key, which comes from lead data -- so no prototype */
  var data = null, err = null, gaps = null, gapsErr = null, gapsAt = null, root = null, seq = 0, ackOpen = null, ackBusy = null,
      ackErr = Object.create(null), ackDraft = Object.create(null), sort = { key: 'step1', dir: 'desc' };
  function load() {
    var my = ++seq;
    return G.api('/monitor/partners', { _: Date.now() }, { timeout: 20000 })
      .then(function (d) { if (my === seq) { data = d; err = null; } }, function (e) { if (my === seq) err = e.message; });
  }
  /* its own, slower cadence: check B reads Salesforce across the network */
  function loadGaps() {
    return G.api('/monitor/partner-gaps', { _: Date.now() }, { timeout: 30000 }).then(function (d) { gaps = d; gapsErr = null; gapsAt = new Date().toISOString(); }, function (e) { gapsErr = e.message; });
  }
  function activate(el) {
    root = el; var p = Promise.all([load(), loadGaps()]); render();
    G.every('partners', 300000, function () { load().then(render); });
    G.every('partner-gaps', 600000, function () { loadGaps().then(render); });
    return p.then(render);
  }
  function deactivate() { G.stop('partners'); G.stop('partner-gaps'); root = null; ackOpen = null; }
  function N(v) { var n = Number(v); return isNaN(n) ? 0 : n; }   /* pg COUNTs arrive as strings */
  function chip(text, cls, title) { return '<span class="badge ' + (cls || 'b-neu') + '"' + (title ? ' title="' + esc(title) + '"' : '') + '>' + text + '</span>'; }
  function display(x) { return x.partner_display || x.partner_name || x.partner_email || x.partner_key || '—'; }
  function ago(ts) { return ts ? Math.round((Date.now() - Date.parse(ts)) / 60000) : null; }

  /* ── cards ── */
  function cards(d) {
    var lc = d.lifecycle || {}, t = d.totals || {}, na = N(lc.needsAttention);
    var naSub = lc.needsAttentionComplete === false ? 'AT LEAST this many — the full count could not be read'
      : na > 0 ? 'act on these today' : N(lc.acknowledged) > 0 ? fmt(lc.acknowledged) + ' acknowledged, not counted' : 'nothing failing';
    return '<div class="sumgrid six">' +
      U.metricCard({ id: 'p-attn', label: 'Needs attention', value: na, chip: na > 0 ? chip('act today', 'b-bad') : '', sub: naSub, title: 'Partner companies in a FAILED state: a conversion or a qualification that did not land. The only number here that means someone has to act today.' }) +
      U.metricCard({ id: 'p-domains', label: 'Partner companies in the lifecycle', value: lc.totalDomains, sub: 'last 180 days, plus any unresolved failure' + (lc.domainsCapped ? ' · the newest ' + fmt(lc.domainsLimit) + ' only' : '') }) +
      U.metricCard({ id: 'p-sfwait', label: 'Waiting on an AE', value: lc.sfActionable, sub: 'an Opportunity exists and nobody has ticked Qualified Demo yet' }) +
      U.metricCard({ id: 'p-leads', label: 'Partner companies, all time', value: t.leads, sub: fmt(t.leads24h) + ' with a lead in the last 24 hours' }) +
      U.metricCard({ id: 'p-conv', label: 'Conversions sent', value: t.conversions, sub: 'companies — one per customer, ever' }) +
      U.metricCard({ id: 'p-qual', label: 'Qualified demos fired', value: t.qualified, sub: 'companies — the event that pays the partner' }) + '</div>';
  }
  function lifecycleChips(lc) {
    var by = lc.byState || {};
    var chips = LADDER.filter(function (k) { return N(by[k]) > 0; }).map(function (k) { return chip(fmt(by[k]) + ' ' + STATE[k], FAILED[k] ? 'b-bad' : 'b-neu'); });
    if (N(lc.noCustomerKeyLeads) > 0) chips.push(chip(fmt(lc.noCustomerKeyLeads) + ' with no company domain (leads, not companies)', 'b-neu', 'Counted as LEADS: they have no usable domain to key by, so they cannot sit in the states above.'));
    if (lc.domainsCapped) chips.push(chip('capped at ' + fmt(lc.domainsLimit) + ' companies', 'b-bad', 'More partner companies exist than this view returns; these states are a page, not the population.'));
    return chips.length ? '<div class="chips">' + chips.join('') + '</div>' : '<p class="lnote">No partner companies yet.</p>';
  }
  /* SALESFORCE, per company. sf_state is the one SNAPSHOT here and moves both
     ways; every split below adds up to the state it splits. */
  function sfChips(lc) {
    var by = lc.bySfState || {}, ds = lc.domains || [], c = [];
    var tickedPaid = 0, tickedVerified = 0, tickedNot = 0;
    ds.forEach(function (x) { if (x.sf_state !== 'ticked') return; if (x.qualified_sent) tickedPaid++; else if (x.signup_verified) tickedVerified++; else tickedNot++; });
    if (tickedPaid) c.push(chip(fmt(tickedPaid) + ' ticked, $50 fired', 'b-good'));
    if (tickedVerified) c.push(chip(fmt(tickedVerified) + ' ticked, will fire next poll', 'b-neu', 'Ticked and the conversion is verified, so the next poll qualifies it.'));
    /* NOT "will fire": the poller only qualifies a VERIFIED conversion */
    if (tickedNot) c.push(chip(fmt(tickedNot) + ' ticked, cannot fire yet', 'b-warn', 'Ticked, but the conversion is not verified in PartnerStack, so the poller will not qualify it.'));
    if (N(lc.sfActionable)) c.push(chip(fmt(lc.sfActionable) + ' waiting on an AE', 'b-warn'));
    if (N(lc.sfUnactionable)) c.push(chip(fmt(lc.sfUnactionable) + ' unticked, no conversion sent (not actionable)', 'b-neu', 'An Opportunity exists and is unticked, but no conversion was sent, so a qualification could not succeed.'));
    if (N(lc.sfUntickedAfterPaid)) c.push(chip(fmt(lc.sfUntickedAfterPaid) + ' unticked after the $50 fired (not actionable)', 'b-neu', 'Nothing reacts to an untick: the claim is permanent and PartnerStack keeps the commission.'));
    if (N(by.create_errored)) c.push(chip(fmt(by.create_errored) + ' Opportunity never created', 'b-bad'));
    if (N(by.no_opportunity)) c.push(chip(fmt(by.no_opportunity) + ' no Opportunity yet', 'b-neu'));
    var summed = Object.keys(by).reduce(function (a, k) { return a + N(by[k]); }, 0), unchecked = N(lc.totalDomains) - summed;
    if (unchecked > 0) c.push(chip(fmt(unchecked) + ' not checked yet', 'b-neu', 'The poller has not checked these yet. NOT the same as having no Opportunity.'));
    var r = lc.sfLastRead || {};
    if (r.ok === false) c.push(chip('Salesforce read FAILED (' + esc(r.reason || 'unknown') + ')', 'b-bad', 'The Opportunity list could not be read completely, so these states are stale. Nothing was overwritten with a guess.'));
    else if (r.ok === true) c.push(chip('read ' + fmt(r.records) + ' of ' + fmt(r.totalSize) + ' Opportunities · ' + fmt(r.pages) + ' pages', 'b-neu'));
    var age = ago(lc.sfNewestCheckedAt), stale = lc.sfStaleAfterMin || 45;
    if (age !== null) c.push(age >= stale ? chip('STALE — last checked ' + fmt(age) + ' min ago', 'b-bad', 'Older than the refresh allows: these states are stale and the refresh is probably failing.') : chip('checked ' + fmt(age) + ' min ago', 'b-neu'));
    return c.length ? '<div class="chips">' + c.join('') + '</div>' : '<p class="lnote">Not checked yet.</p>';
  }

  /* ── the funnel: the headline is what HAPPENED at a stage (the absolute
     twin where there is one); the rate runs stage to stage on the funnel
     path (the cumulative figures), and is not given under a base of 10 ── */
  function funnel(f) {
    var p = f.programme || {}, stages = f.stages || [], losses = f.losses || {}, min = f.rateMin || 10, prev = null;
    return '<div class="pfunnel">' + stages.map(function (s) {
      var cum = N(p[s.key]), abs = s.abs ? N(p[s.abs]) : null, head = abs !== null ? Math.max(abs, cum) : cum, h = '';
      if (s.unit) h = '<span class="pfr">clicks, not companies</span>';
      else if (prev === null) h = '';
      else h = '<span class="pfr">' + (prev >= min ? G.pct1(cum, prev) + '% of the stage before' : 'too few to rate (n=' + fmt(prev) + ')') + '</span>';
      var off = abs !== null && abs > cum ? '<span class="pfo" title="These companies reached this stage without passing an earlier one — most often a Salesforce Opportunity for a company that never booked through our form. Real, and the money is real, so the big number counts them.">' + fmt(cum) + ' on the funnel path · ' + fmt(abs - cum) + ' skipped an earlier stage</span>' : '';
      var ls = (losses[s.key] || []).filter(function (x) { return N(p[x.key]) > 0; }).map(function (x) { return '<span class="pfl' + (x.bad ? ' bad-t' : '') + '">' + fmt(p[x.key]) + ' ' + esc(x.label) + '</span>'; }).join('');
      if (!s.unit) prev = cum;
      return '<div class="pfs" data-stage="' + esc(s.key) + '"><span class="pfk">' + esc(s.label) + '</span><span class="num" data-v="' + esc(head) + '">' + fmt(head) + '</span>' + h + off + ls + '</div>';
    }).join('') + '</div>';
  }

  /* ── partner revenue gaps ── */
  function gapsHtml() {
    if (gapsErr && !gaps) return U.unavailable('Partner revenue gaps', gapsErr);
    if (!gaps) return '<span class="skel"></span>';
    /* a failed re-check after a good one is NOT a fresh all-clear: the old
       result stays, and says how old it is */
    var stale = gapsErr ? '<div class="readat"><span class="badge b-warn">The last check failed (' + esc(String(gapsErr).slice(0, 40)) + ') — showing the result from ' + esc(G.etTime(gapsAt)) + ' ET</span></div>' : '';
    var g = gaps, oc = g.opportunityCheck || {}, missed = (g.missedConversions || []).length, missing = (g.missingOpportunity || []).length;
    var head = oc.ok === false ? fmt(missed) + '+?' : fmt(missed + missing);
    var sub = fmt(missed) + ' with no conversion · ' + (oc.ok === false ? 'the Opportunity check is unavailable' : oc.checked === false ? 'none old enough to check for an Opportunity yet' : fmt(missing) + ' with no Opportunity, of ' + fmt(oc.candidates) + ' checked');
    var list = function (rows, when) {
      return U.grid([
        { label: 'Company', html: function (x) { return '<code>' + esc(x.customer_key) + '</code>'; } },
        { label: 'Partner', get: function (x) { return display(x); } },
        { label: 'Email', cls: 'wrap', get: function (x) { return x.email || '—'; } },
        { label: when === 'met_at' ? 'Demo (ET)' : 'First seen (ET)', cls: 'day', get: function (x) { return G.et(x[when]); } },
      ], rows, { region: when === 'met_at' ? 'Demo happened, no Opportunity' : 'No conversion sent' });
    };
    var h = stale + '<div class="gaphead"><span class="num" data-v="' + esc(head) + '">' + head + '</span><span class="lnote">' + sub + '</span></div>';
    if (oc.ok === false) h += '<p class="lnote bad-t">The Opportunity check is unavailable (' + esc(oc.reason || 'unknown') + '). This is NOT a clean result — the second check did not run.</p>';
    if (missed) h += '<h3 class="pgh">No conversion sent (' + fmt(missed) + ') — the partner gets nothing, and the $50 can never fire either</h3>' + list(g.missedConversions, 'first_seen');
    if (missing) h += '<h3 class="pgh">Demo happened, no Opportunity (' + fmt(missing) + ') — no AE can mark these qualified, so the $50 never fires</h3>' + list(g.missingOpportunity, 'met_at');
    if ((g.skipped || []).length) h += '<p class="lnote">Skipped correctly, not counted as gaps: ' + g.skipped.map(function (x) { return '<code>' + esc(x.customer_key) + '</code> (' + esc(L.psReason(x.reason)) + ')'; }).join(', ') + '.</p>';
    if (!missed && !missing && oc.ok !== false) h += '<p class="lnote">No revenue gaps: every partner company has its conversion, and every demo past ' + fmt(g.graceDays || 3) + ' days has an Opportunity.</p>';
    if (N(g.awaitingQualification)) h += '<p class="lnote">' + fmt(g.awaitingQualification) + ' partner ' + G.plural(N(g.awaitingQualification), 'demo') + ' past the ' + fmt(g.graceDays || 3) + '-day mark in total; the ones with an Opportunity are waiting on an AE and are not listed.</p>';
    return h;
  }

  /* ── per company ── */
  function sfLabel(x) {
    if (x.sf_state === 'ticked') return x.qualified_sent ? 'ticked, $50 fired' : (x.signup_verified ? 'ticked, will fire next poll' : 'ticked, cannot fire yet — conversion not verified');
    if (x.sf_state === 'exists_unticked') return x.qualified_sent ? 'unticked after the $50 fired — nothing more can' : (x.signup_sent ? 'waiting on an AE' : 'unticked — no conversion sent, not actionable');
    if (x.sf_state === 'create_errored') return '<span class="bad-t">Opportunity never created</span>';
    if (x.sf_state === 'no_opportunity') return 'no Opportunity yet';
    return '<span class="na">not checked yet</span>';
  }
  function detailReason(x) {
    var r = x.signup_fail_reason || x.qualify_fail_reason || x.skipped_reason;
    var h = r ? '<span title="' + esc(r) + '">' + esc(L.psReason(r)) + '</span>' : '—';
    if (x.acknowledged) h += ' ' + chip('acknowledged', 'b-neu', 'Acknowledged' + (x.ack_note ? ': ' + x.ack_note : '') + '. Still shown and still red — it just no longer counts as needing attention.');
    return h;
  }
  function ackCell(x) {
    if (!FAILED[x.state]) return '';
    var k = esc(x.customer_key);
    if (ackBusy === x.customer_key) return '<span class="na" role="status">Saving…</span>';
    /* THE NOTE IS KEPT IN STATE, not only in the box: the five- and ten-minute
       refreshes repaint the row, and a draft that lived only in the DOM was
       wiped mid-sentence with the caret left in the empty box. A failed save
       keeps the form open, with the error inside it and the note intact. */
    var err = ackErr[x.customer_key] ? '<div class="bad-t" role="alert">' + esc(ackErr[x.customer_key]) + '</div>' : '';
    if (ackOpen === x.customer_key) {
      return '<div class="ackf">' + err + '<input class="field" data-ack-note="' + k + '" value="' + esc(ackDraft[x.customer_key] || '') + '" placeholder="Why this is not a real loss (optional)" aria-label="Why the failure for ' + k + ' is not a real loss" maxlength="300">' +
        '<button class="btn sm primary" data-ack-go="' + k + '" aria-label="Acknowledge the failure for ' + k + '">Acknowledge</button><button class="btn sm" data-ack-cancel="' + k + '" aria-label="Cancel acknowledging ' + k + '">Cancel</button></div>';
    }
    return err + '<button class="btn sm" data-ack="' + k + '" data-on="' + (x.acknowledged ? '0' : '1') + '" aria-label="' + (x.acknowledged ? 'Un-acknowledge' : 'Acknowledge') + ' the failure for ' + k + '">' + (x.acknowledged ? 'Un-acknowledge' : 'Acknowledge') + '</button>';
  }
  /* EVERY partner who claimed the company, the one on the row first: the
     server's partner_others leaves that one out */
  function claimants(x) { return [display(x)].concat(x.partner_others || []); }
  /* The row's detail holds everything a hover used to: the Salesforce state
     (dropped from the row at mid widths), the full reason, the ack note, and
     every partner who claimed it -- a phone never shows a title. */
  function domainDetail(x) {
    var r = x.signup_fail_reason || x.qualify_fail_reason || x.skipped_reason;
    return U.kv([['Salesforce', sfLabel(x), 'html'], ['Why', r ? L.psReason(r) + ' (' + r + ')' : null],
      ['Acknowledged', x.acknowledged ? (x.ack_note ? 'Yes — ' + x.ack_note : 'Yes, with no note') : null],
      ['Claimed by', N(x.partner_key_count) > 1 ? claimants(x).join(', ') + ' — PartnerStack credits ONE partner per company for the life of the account' : null],
      ['Last seen (ET)', x.last_seen ? G.et(x.last_seen) : null]]) || '<p class="lnote">Nothing more is recorded for this company.</p>';
  }
  function domainsTable(lc) {
    return U.rtable({ ns: 'pd', rows: lc.domains || [], key: function (x) { return x.customer_key; }, rowName: function (x) { return x.customer_key; }, emptyTitle: 'No partner companies yet',
      detail: domainDetail,
      cols: [
        { label: 'Company', cls: 'kcell', html: function (x) { return '<code>' + esc(x.customer_key) + '</code>'; } },
        { label: 'State', html: function (x) { return chip(esc(STATE[x.state] || x.state), FAILED[x.state] ? 'b-bad' : 'b-neu'); } },
        { label: 'Partner', html: function (x) { return esc(display(x)) + (N(x.partner_key_count) > 1 ? ' ' + chip(fmt(N(x.partner_key_count) - 1) + ' other ' + G.plural(N(x.partner_key_count) - 1, 'partner'), 'b-bad',
          'Claimed by ' + N(x.partner_key_count) + ' partners: ' + claimants(x).join(', ') + '. PartnerStack credits ONE partner per company for the life of the account.') : ''); } },
        { label: 'Salesforce', opt: 1, html: sfLabel },
        { label: 'Why', html: detailReason },
        { label: 'Action', html: ackCell },
        { label: 'Last seen (ET)', cls: 'm', get: function (x) { return G.et(x.last_seen); } },
      ] });
  }

  /* ── per partner: ONE function gives the value shown AND the sort key ── */
  var PCOLS = [['clicks', 'Clicks'], ['step1', 'Step 1'], ['completed', 'Completed'], ['conversions', 'Converted', 'abs_conversions'], ['verified', 'Verified', 'abs_verified'],
               ['booked', 'Booked', 'abs_booked'], ['opportunity', 'Opportunity', 'abs_opportunity'], ['ticked', 'Ticked', 'abs_ticked'], ['qualified', 'Qualified', 'abs_qualified']];
  function pval(x, k) { var c = PCOLS.filter(function (p) { return p[0] === k; })[0]; if (!c) return 0; if (k === 'clicks') return x.clicks === null || x.clicks === undefined ? -1 : N(x.clicks); return c[2] && x[c[2]] !== undefined ? N(x[c[2]]) : N(x[k]); }
  function pcell(x, k) {
    var c = PCOLS.filter(function (p) { return p[0] === k; })[0];
    if (k === 'clicks') return x.clicks === null || x.clicks === undefined ? '—' : fmt(x.clicks);
    var cum = N(x[k]), abs = c[2] && x[c[2]] !== undefined ? N(x[c[2]]) : null;
    return abs !== null && abs > cum ? '<span class="pfo" title="' + fmt(cum) + ' on the funnel path, ' + fmt(abs - cum) + ' skipped an earlier stage. The bigger number is what actually happened.">' + fmt(abs) + ' †</span>' : fmt(abs !== null ? abs : cum);
  }
  /* the headers are the sort buttons, and card view has no header row */
  var PSORTS = [['step1:desc', 'Most step 1'], ['name:asc', 'Name A–Z'], ['clicks:desc', 'Most clicks'], ['conversions:desc', 'Most converted'], ['booked:desc', 'Most booked'], ['qualified:desc', 'Most qualified']];
  function sortSel() {
    var v = sort.key + ':' + sort.dir, list = PSORTS.some(function (o) { return o[0] === v; }) ? PSORTS : PSORTS.concat([[v, 'As sorted by its header']]);
    return '<div class="cardsort"><label class="lfl inline"><span>Sort by</span><select class="field" data-psortsel>' + list.map(function (o) { return '<option value="' + o[0] + '"' + (o[0] === v ? ' selected' : '') + '>' + o[1] + '</option>'; }).join('') + '</select></label></div>';
  }
  function partnersTable(rows) {
    var name = function (x) { return x.partner_name || x.partner_email || x.partner_key || '—'; };
    var sorted = (rows || []).map(function (x, i) { return [x, i]; }).sort(function (a, b) {
      var d = sort.key === 'name' ? String(name(a[0])).localeCompare(String(name(b[0]))) : pval(a[0], sort.key) - pval(b[0], sort.key);
      return (sort.dir === 'asc' ? d : -d) || a[1] - b[1];   /* stable: ties keep the server's order */
    }).map(function (p) { return p[0]; });
    return U.rtable({ ns: 'pp', rows: sorted, key: function (x) { return x.partner_key; }, rowName: function (x) { return name(x); }, emptyTitle: 'No partner-sourced leads yet',
      sort: { key: sort.key, dir: sort.dir, attr: 'data-psort' },
      cols: [{ label: 'Partner', sort: 'name', html: function (x) { return esc(name(x)); } }].concat(PCOLS.map(function (c) {
        return { label: c[1], sort: c[0], r: 1, opt: c[0] === 'clicks' || c[0] === 'verified' || c[0] === 'opportunity' ? 1 : 0, html: function (x) { return pcell(x, c[0]); } }; })),
      /* the detail carries the columns a mid-width screen drops (Clicks,
         Verified, Opportunity) and says in words what a dagger means */
      detail: function (x) {
        var split = PCOLS.filter(function (c) { return c[2] && x[c[2]] !== undefined && N(x[c[2]]) > N(x[c[0]]); }).map(function (c) {
          return [c[1], fmt(x[c[2]]) + ' — ' + fmt(x[c[0]]) + ' on the funnel path, ' + fmt(N(x[c[2]]) - N(x[c[0]])) + ' skipped an earlier stage']; });
        var hidden = [['Clicks', pcell(x, 'clicks')], ['Verified', pcell(x, 'verified').replace(/<[^>]*>/g, '')], ['Opportunity', pcell(x, 'opportunity').replace(/<[^>]*>/g, '')]];
        return U.kv(hidden.concat(split).concat([['Email', x.partner_email], ['Partner key', x.partner_key ? '<code>' + esc(x.partner_key) + '</code>' : null, 'html'], ['Last click (ET)', x.last_click ? G.et(x.last_click) : null]])) +
          '<p><button class="btn sm" data-pdrill="' + esc(x.partner_key) + '">' + G.ic('users') + 'See this partner\u2019s leads</button></p>';
      } });
  }

  function render() {
    if (!root || (G.current && G.current() !== 'partners')) return;
    var d = data;
    var head = '<section class="ph"><div class="ph-top"><h1 class="title" tabindex="-1">Partners</h1><span class="readat">' + (d ? 'Counted in companies — one per customer domain' : '') + '</span></div>' +
      '<p class="lede">The PartnerStack programme, company by company: PartnerStack pays one conversion and one qualified demo per customer company, ever. Clicks are the one number here that is not a company count.</p></section>';
    var body;
    if (!d && err) body = '<section class="card panel">' + U.unavailable('The partner programme', err) + '</section>';
    else if (!d) body = U.loading(5);
    else body = U.drawn('The partner programme', function () {
      var lc = d.lifecycle || {};
      return (err ? '<div class="readat"><span class="badge b-warn">Last refresh failed (' + esc(String(err).slice(0, 40)) + ') — showing the previous read</span></div>' : '') + cards(d) +
        U.panel({ id: 'p-states', title: 'Where every partner company is', qual: 'one state each; they add up to the total', body: lifecycleChips(lc) }) +
        U.panel({ id: 'p-sfstates', title: 'Salesforce', qual: 'checked every 15 minutes, per company', body: sfChips(lc) }) +
        U.panel({ id: 'p-funnel', title: 'Funnel', qual: 'companies', body: funnel(d.funnel || {}),
          foot: '<span>Each stage shows the companies it actually happened to; where some skipped an earlier stage the stage says so. Clicks that never reached the form are not in our data at all — only PartnerStack has those. Rates are withheld below ' + fmt((d.funnel || {}).rateMin || 10) + ', so a tiny base never reads as a percentage.</span>' }) +
        U.panel({ id: 'p-gaps', title: 'Partner revenue gaps', qual: 'referrals that will never pay unless someone acts', body: gapsHtml() }) +
        U.panel({ id: 'p-dom', title: 'Every partner company', qual: 'newest activity first', body: domainsTable(lc) }) +
        U.panel({ id: 'p-per', title: 'Per partner', qual: 'companies, not people' + ((d.partners || []).length >= 200 ? ' · the 200 largest partners' : ''), body: sortSel() + partnersTable(d.partners),
          foot: '<span>A number marked † counts companies that skipped an earlier stage — most often a Salesforce Opportunity for a company that never booked through our form. Open the row for the split.</span>' });
    });
    G.paint(root, head + body);
  }
  /* focus goes where the reader's next step is, and the outcome is said */
  function focusOn(sel) { var el = root && root.querySelector && root.querySelector(sel); if (el && el.focus) { try { el.focus({ preventScroll: true }); } catch (e) { el.focus(); } } }
  function sel(attr, key) { return '[' + attr + '="' + String(key).replace(/["\\]/g, '\\$&') + '"]'; }
  function ack(key, on, note) {
    ackBusy = key; delete ackErr[key]; render();
    /* the boolean is JSON, never a string: "false" in a query ACKNOWLEDGES */
    return G.api('/monitor/partner-ack', {}, { method: 'POST', body: on ? { customer_key: key, note: note || '', acknowledged: true } : { customer_key: key, acknowledged: false }, timeout: 15000 })
      .then(function () { ackBusy = null; ackOpen = null; delete ackDraft[key]; return load().then(function () { render(); focusOn(sel('data-ack', key)); G.announce((on ? 'Acknowledged ' : 'Un-acknowledged ') + key); }); },
        /* a failure leaves the form OPEN with the note in it: only the success
           branch above closes it (a line re-opening it here was a no-op, and
           mutation testing showed nothing could tell it was there) */
        function (e) { ackBusy = null; ackErr[key] = 'Could not update: ' + e.message; render(); focusOn(on ? sel('data-ack-note', key) : sel('data-ack', key)); G.announce(ackErr[key]); });
  }
  if (typeof document !== 'undefined' && document.addEventListener) document.addEventListener('click', function (e) {
    var t = e.target && e.target.closest ? e.target : null; if (!t || G.current() !== 'partners') return;
    var s = t.closest('[data-psort]'); if (s) { var k = s.getAttribute('data-psort'); sort = sort.key === k ? { key: k, dir: sort.dir === 'desc' ? 'asc' : 'desc' } : { key: k, dir: k === 'name' ? 'asc' : 'desc' }; render(); return; }
    var dr = t.closest('[data-pdrill]'); if (dr) { G.TABS.leads.openFiltered({ partner: dr.getAttribute('data-pdrill') }); return; }
    var a = t.closest('[data-ack]'); if (a) { var key = a.getAttribute('data-ack'); if (a.getAttribute('data-on') === '1') { ackOpen = key; render(); focusOn(sel('data-ack-note', key)); } else ack(key, false); return; }
    var go = t.closest('[data-ack-go]'); if (go) { var k2 = go.getAttribute('data-ack-go'), n = root && root.querySelector(sel('data-ack-note', k2)); ack(k2, true, n ? n.value : (ackDraft[k2] || '')); return; }
    var c = t.closest('[data-ack-cancel]'); if (c) { var k3 = c.getAttribute('data-ack-cancel'); ackOpen = null; delete ackErr[k3]; delete ackDraft[k3]; render(); focusOn(sel('data-ack', k3)); }   /* Cancel does NOTHING else */
  });
  if (typeof document !== 'undefined' && document.addEventListener) {
    document.addEventListener('input', function (e) { var el = e.target; if (el && el.hasAttribute && el.hasAttribute('data-ack-note')) ackDraft[el.getAttribute('data-ack-note')] = el.value; });
    document.addEventListener('change', function (e) {
      var el = e.target; if (!el || !el.hasAttribute || !el.hasAttribute('data-psortsel') || G.current() !== 'partners') return;
      var v = String(el.value).split(':'); if (PCOLS.some(function (c) { return c[0] === v[0]; }) || v[0] === 'name') { sort = { key: v[0], dir: v[1] === 'asc' ? 'asc' : 'desc' }; render(); }
    });
  }
  return { title: 'Partners', activate: activate, deactivate: deactivate, render: render, load: load, _set: function (d, g) { data = d; if (g !== undefined) gaps = g; }, _ack: ack, _sfLabel: sfLabel };
})(GW);
