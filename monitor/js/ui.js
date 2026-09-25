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
    var txt = pts ? (up ? '+' : '') + d + (Math.abs(d) === 1 ? ' pt' : ' pts') : (prev < 10 ? (up ? '+' : '') + d : (up ? '+' : '') + Math.round(d / prev * 100) + '%');
    var tone = flat || goodUp === null ? 'b-neu' : ((up === goodUp) ? 'b-good' : 'b-bad');
    return '<span class="badge ' + tone + '">' + (flat ? '' : ic(up ? 'arrow-up' : 'arrow-down')) + (flat ? 'No change' : txt) + '</span>';
  }
  /* Two bars on one scale, labelled. A rate's honest axis is 0-100; a
     count's is the larger of the two. EACH LABEL SITS ON ITS OWN LINE ABOVE
     ITS BAR: with the label beside the track, the track's width depended on
     how long its label was, so "Last month, same point" got a shorter track
     than "This month so far" and a 71% bar drew at half the length of a 67%
     one -- two bars on two scales, the opposite of the numbers. */
  function cmpBlock(curLbl, prevLbl, cur, prev, isRate, goodUp, heading) {
    if (cur === null || cur === undefined || prev === null || prev === undefined) return '';
    var max = isRate ? 100 : (Math.max(cur, prev) || 1), u = isRate ? '%' : '';
    var row = function (lbl, v, col) {
      return '<div class="cb"><span class="cl">' + esc(lbl) + '</span><div class="tr"><div class="fi" style="width:' + (v / max * 100) + '%;background:' + col + '"></div></div><span class="v">' + fmt(v) + u + '</span></div>';
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
      '<div class="sh"><div><h2 class="sh-t">' + esc(o.title) + '</h2>' + (o.qual ? '<span class="sh-q">' + esc(o.qual) + '</span>' : '') + '</div>' +
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
  /* The top of the funnel is SESSIONS -- form_sessions rows, one per browser
     tab -- never "page loads", which was the label until 26 Sept and named a
     count it was 6-13% short of (CLAUDE.md, the four nouns). */
  function funnel(top, steps) {
    var base = steps[0] && steps[0][1] || 0, h = '<div class="funnel">';
    if (top !== null && top !== undefined) h += '<div class="fx"><b>' + fmt(top) + '</b> ' + G.plural(top, 'session') + (top && base ? ' &rarr; <b>' + G.pct1(base, top) + '%</b> got through step 1' : '') + '</div>';
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

  /* A data table that is a real table when there is room for it and a stack
     of label/value cards when there is not -- decided by the CONTENT width,
     not the viewport, so a tablet beside the rail gets cards rather than a
     table whose Status column is off the edge. Each cell carries its column
     label in data-l for the card view, and its value in ONE .cv element, so
     "text + a badge" stays one thing instead of three items spread across
     the row. Expandable rows are real buttons with aria-expanded, 44px on
     touch.

     ROW KEYS ARE STABLE (o.key), never the row index: an expanded row is
     re-opened after a repaint by its key, and with an index a filter change
     would open somebody else's row. Each expander is NAMED for its row
     ("Details for ann@x.com"), with aria-expanded carrying the state --
     338 buttons all called "Show details" told a screen reader nothing. */
  function keyOf(o, r, i) { return o.ns + '-' + (o.key ? String(o.key(r)).replace(/[^A-Za-z0-9]/g, function (c) { return '_' + c.charCodeAt(0).toString(16); }) : i); }
  function rtable(o) {
    var cols = o.cols;
    var head = '<tr>' + (o.detail ? '<th class="xcell"><span class="sr-only">Details</span></th>' : '') + cols.map(function (c) { return '<th' + (c.r ? ' class="r"' : '') + '>' + esc(c.label) + '</th>'; }).join('') + '</tr>';
    var body = o.rows.map(function (r, i) {
      var key = keyOf(o, r, i);
      var cells = cols.map(function (c, ci) {
        var v = c.html ? c.html(r) : esc(c.get ? c.get(r) : r[c.k]);
        var cls = [c.r ? 'r' : '', c.cls || '', ci === 0 ? 'lead-cell' : ''].filter(Boolean).join(' ');
        return '<td' + (cls ? ' class="' + cls + '"' : '') + ' data-l="' + esc(c.label) + '"><span class="cv">' + (v === '' ? '—' : v) + '</span></td>';
      }).join('');
      var nm = o.rowName ? o.rowName(r) : 'row ' + (i + 1);
      var x = o.detail ? '<td class="xcell"><button class="xb" data-x="' + key + '" aria-expanded="false" aria-controls="' + key + '-d" aria-label="Details for ' + esc(nm) + '">' + ic('caret-right') + '</button></td>' : '';
      var d = o.detail ? '<tr class="detail" id="' + key + '-d" hidden><td colspan="' + (cols.length + 1) + '"><div class="well">' + o.detail(r) + '</div></td></tr>' : '';
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
      b.setAttribute('aria-expanded', String(!open));
      if (open) d.setAttribute('hidden', ''); else d.removeAttribute('hidden');
    });
  }
  function pills(defs, current, counts, attr, label) {
    return '<div class="pills" role="group" aria-label="' + esc(label || 'Filter') + '">' + defs.map(function (p) {
      return '<button ' + attr + '="' + esc(p[0]) + '" aria-pressed="' + (p[0] === current) + '">' + esc(p[1]) + (counts ? '<span>' + fmt(counts[p[0]] || 0) + '</span>' : '') + '</button>'; }).join('') + '</div>';
  }
  function tg(defs, current, attr, label) {
    return '<div class="tg" role="group" aria-label="' + esc(label) + '">' + defs.map(function (d) {
      return '<button ' + attr + '="' + esc(d[0]) + '" aria-pressed="' + (d[0] === current) + '">' + (d[2] || '') + esc(d[1]) + '</button>'; }).join('') + '</div>';
  }
  return { delta: delta, cmpBlock: cmpBlock, leadCard: leadCard, metricCard: metricCard, panel: panel, empty: empty,
           unavailable: unavailable, loading: loading, funnel: funnel, kv: kv, rtable: rtable, keyOf: keyOf, pills: pills, tg: tg };
})(GW);
