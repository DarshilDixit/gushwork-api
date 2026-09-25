/* ============================================================================
   GW.ui -- the component kit. Every builder returns an HTML string, and every
   piece of text that came from data goes through GW.esc: labels from a
   database column, partner names with apostrophes, anything a human typed.

   NEW COMPONENTS, pending library review (declared to Utsav): the change chip,
   the comparison bars, the attention strip, the "Live" badge, the responsive
   table. Built from tokens only.
   ============================================================================ */
GW.ui = (function (G) {
  var esc = G.esc, fmt = G.fmt, ic = G.ic;

  /* A change chip. Colour = direction x whether up is good; neutral where up
     is neither (blocked, disqualified). A base under 10 prints the absolute
     change, never a percentage of three. A rate prints points. */
  function delta(cur, prev, goodUp, pts) {
    if (prev === null || prev === undefined || cur === null || cur === undefined) return '';
    var d = cur - prev, up = d > 0, flat = d === 0;
    if (!pts && prev === 0 && cur > 0) return '<span class="badge ' + (goodUp === null ? 'b-neu' : goodUp ? 'b-good' : 'b-bad') + '">' + ic('arrow-up') + '+' + fmt(cur) + '</span>';
    var txt = pts ? (up ? '+' : '') + d + ' pts' : (prev < 10 ? (up ? '+' : '') + d : (up ? '+' : '') + Math.round(d / prev * 100) + '%');
    var tone = flat || goodUp === null ? 'b-neu' : ((up === goodUp) ? 'b-good' : 'b-bad');
    return '<span class="badge ' + tone + '">' + (flat ? '' : ic(up ? 'arrow-up' : 'arrow-down')) + (flat ? 'No change' : txt) + '</span>';
  }
  /* Two bars on one scale, labelled. A rate's honest axis is 0-100; a
     count's is the larger of the two. */
  function cmpBlock(curLbl, prevLbl, cur, prev, isRate, goodUp, heading) {
    if (cur === null || cur === undefined || prev === null || prev === undefined) return '';
    var max = isRate ? 100 : (Math.max(cur, prev) || 1), u = isRate ? '%' : '';
    var row = function (lbl, v, col) {
      return '<div class="cb"><span>' + esc(lbl) + '</span><div class="tr"><div class="fi" style="width:' + (v / max * 100) + '%;background:' + col + '"></div></div><span class="v">' + fmt(v) + u + '</span></div>';
    };
    return '<div class="cmpbars">' + (heading ? '<div class="cmph"><span>' + esc(heading) + '</span>' + delta(cur, prev, goodUp, isRate) + '</div>' : '') +
      row(curLbl, cur, 'var(--data)') + row(prevLbl, prev, 'var(--ref)') + '</div>';
  }
  function leadCard(o) {
    return '<div class="card lead"' + (o.id ? ' data-card="' + esc(o.id) + '"' : '') + '><div class="klabel">' + esc(o.label) + '</div>' +
      '<div class="kval"><span class="num" data-v="' + esc(o.big) + '">' + fmt(o.big) + (o.isRate ? '<small>%</small>' : '') + '</span>' + (o.chip || '') + '</div>' +
      '<div class="ksub">' + (o.sub || '') + '</div>' + (o.cmp || '') + '</div>';
  }
  function metricCard(o) {
    return '<div class="card mc"' + (o.id ? ' data-card="' + esc(o.id) + '"' : '') + (o.title ? ' title="' + esc(o.title) + '"' : '') + '><div class="klabel">' + esc(o.label) + '</div>' +
      '<div class="row"><span class="num" data-v="' + esc(o.value) + '">' + fmt(o.value) + (o.unit ? '<small>' + esc(o.unit) + '</small>' : '') + '</span>' + (o.chip || '') + '</div>' +
      '<div class="ksub">' + (o.sub || '') + '</div></div>';
  }
  function panel(o) {
    return '<section class="card panel' + (o.cls ? ' ' + o.cls : '') + '"' + (o.id ? ' id="' + esc(o.id) + '"' : '') + '>' +
      '<div class="sh"><div><span class="sh-t">' + esc(o.title) + '</span>' + (o.qual ? '<span class="sh-q">' + esc(o.qual) + '</span>' : '') + '</div>' +
      (o.right ? '<div class="sh-r">' + o.right + '</div>' : '') + '</div>' + (o.body || '') +
      (o.foot ? '<div class="foot">' + o.foot + '</div>' : '') + '</section>';
  }
  function empty(title, body) { return '<div class="empty"><div class="t">' + esc(title) + '</div>' + (body ? '<div class="b">' + esc(body) + '</div>' : '') + '</div>'; }
  /* UNAVAILABLE, never zeroed (states.md, R ruled 28 Aug): a card we could
     not read keeps its shape and says what failed. */
  function unavailable(what, why) { return '<div class="unavail">' + ic('warning-circle') + '<span>' + esc(what) + ' could not be read</span>' + (why ? '<span class="badge b-bad">' + esc(why) + '</span>' : '') + '</div>'; }
  function loading(lines) { var h = ''; for (var i = 0; i < (lines || 3); i++) h += '<div><span class="skel' + (i === 0 ? ' big' : '') + '"></span></div>'; return '<div class="card panel" aria-busy="true">' + h + '</div>'; }

  /* The funnel. Bars are a share of the first stage; the grey rate beside a
     stage is step to step -- of those who reached the stage above, how many
     made this one. A missing base prints nothing rather than an unmeasured rate. */
  function funnel(top, steps) {
    var base = steps[0] && steps[0][1] || 0, h = '<div class="funnel">';
    if (top !== null && top !== undefined) h += '<div class="fx"><b>' + fmt(top) + '</b> page loads' + (top && base ? ' &rarr; <b>' + G.pct1(base, top) + '%</b> got through step 1' : '') + '</div>';
    steps.forEach(function (s, i) {
      var prev = i ? steps[i - 1][1] : null;
      var rate = i && prev ? '<span class="rate"><b>' + G.pct(s[1], prev) + '%</b> of the step above</span>' : '';
      h += '<div class="fs"><div class="top"><span class="lb">' + esc(s[0]) + '</span>' + rate + '</div><div class="top"><span class="n" data-v="' + esc(s[1]) + '">' + fmt(s[1]) + '</span></div>' +
        '<div class="tr"><div class="fi" style="width:' + (base ? s[1] / base * 100 : 0) + '%"></div></div></div>';
    });
    return h + '</div>';
  }
  function kv(fields) {
    return '<div class="kv">' + fields.filter(function (f) { return f && f[1] !== null && f[1] !== undefined && f[1] !== ''; }).map(function (f) {
      return '<div><div class="k">' + esc(f[0]) + '</div><div class="v">' + (f[2] === 'html' ? f[1] : esc(f[1])) + '</div></div>'; }).join('') + '</div>';
  }

  /* A data table that is a real table from 768 up and a stack of label/value
     cards below: each cell carries its column label in data-l for the phone.
     Expandable rows are real buttons with aria-expanded, 44px on touch. */
  function rtable(o) {
    var cols = o.cols;
    var head = '<tr>' + (o.detail ? '<th class="xcell"><span class="sr-only">Details</span></th>' : '') + cols.map(function (c) { return '<th' + (c.r ? ' class="r"' : '') + '>' + esc(c.label) + '</th>'; }).join('') + '</tr>';
    var body = o.rows.map(function (r, i) {
      var key = o.ns + '-' + i;
      var cells = cols.map(function (c, ci) {
        var v = c.html ? c.html(r) : esc(c.get ? c.get(r) : r[c.k]);
        var cls = [c.r ? 'r' : '', c.cls || '', ci === 0 ? 'lead-cell' : ''].filter(Boolean).join(' ');
        return '<td' + (cls ? ' class="' + cls + '"' : '') + ' data-l="' + esc(c.label) + '">' + (v === '' ? '—' : v) + '</td>';
      }).join('');
      var x = o.detail ? '<td class="xcell"><button class="xb" data-x="' + key + '" aria-expanded="false" aria-controls="' + key + '-d" aria-label="Show details">' + ic('caret-right') + '</button></td>' : '';
      var d = o.detail ? '<tr class="detail" id="' + key + '-d" hidden><td colspan="' + (cols.length + 1) + '">' + o.detail(r) + '</td></tr>' : '';
      return '<tr class="main">' + x + cells + '</tr>' + d;
    }).join('');
    return '<div class="rt-wrap"><table class="rt"><thead>' + head + '</thead><tbody>' + (body || '<tr><td colspan="' + (cols.length + 1) + '">' + empty(o.emptyTitle || 'Nothing here yet', o.emptyBody) + '</td></tr>') + '</tbody></table></div>';
  }
  /* One delegated handler for every expander on the page, so a re-render
     never leaves a dead button behind. */
  if (typeof document !== 'undefined' && document.addEventListener) {
    document.addEventListener('click', function (e) {
      var b = e.target && e.target.closest ? e.target.closest('[data-x]') : null; if (!b) return;
      var d = document.getElementById(b.getAttribute('data-x') + '-d'); if (!d) return;
      var open = b.getAttribute('aria-expanded') === 'true';
      b.setAttribute('aria-expanded', String(!open)); b.setAttribute('aria-label', open ? 'Show details' : 'Hide details');
      if (open) d.setAttribute('hidden', ''); else d.removeAttribute('hidden');
    });
  }
  function pills(defs, current, counts, attr) {
    return '<div class="pills" role="group">' + defs.map(function (p) {
      return '<button ' + attr + '="' + esc(p[0]) + '" aria-pressed="' + (p[0] === current) + '">' + esc(p[1]) + (counts ? '<span>' + fmt(counts[p[0]] || 0) + '</span>' : '') + '</button>'; }).join('') + '</div>';
  }
  function tg(defs, current, attr, label) {
    return '<div class="tg" role="group" aria-label="' + esc(label) + '">' + defs.map(function (d) {
      return '<button ' + attr + '="' + esc(d[0]) + '" aria-pressed="' + (d[0] === current) + '">' + (d[2] || '') + esc(d[1]) + '</button>'; }).join('') + '</div>';
  }
  return { delta: delta, cmpBlock: cmpBlock, leadCard: leadCard, metricCard: metricCard, panel: panel, empty: empty,
           unavailable: unavailable, loading: loading, funnel: funnel, kv: kv, rtable: rtable, pills: pills, tg: tg };
})(GW);
