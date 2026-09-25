/* ============================================================================
   GW.chart -- one bar chart, drawn as SVG at the container's real width.

   Blue = this period, the one hue that passes the palette validator in both
   themes. The FAINT GREY BAR BEHIND = the previous period, drawn wider so it
   stays visible whichever is taller; a 2px ring in the card colour separates
   them (the gap, never a border). Colour-blind separation measured: dE 31.4
   light, 24.8 dark (target 8). The grey is deliberately low-contrast, so the
   Table switch is the required relief (dataviz: a contrast WARN obligates a
   table view).

   READABLE AT EVERY WIDTH, by rule rather than by luck:
     - axis labels are spaced by measured width, never drawn on top of each other
     - a value label is skipped rather than allowed to collide with its neighbour
     - an hourly chart narrower than 520px groups into 3-hour blocks, and says
       so, so a bar is never squashed below ~20px; the table keeps every hour
     - no text is ever wider than the plot: long notes wrap in HTML, not SVG
   ============================================================================ */
GW.chart = (function (G) {
  var fmt = G.fmt, esc = G.esc;
  var CH_W = 6.2;   // approx width of one 10px Inter glyph, for collision tests (measured, not a token)

  function niceStep(m) { var raw = Math.max(m, 1) / 4, p = Math.pow(10, Math.floor(Math.log10(raw))), n = raw / p; return Math.max(1, (n <= 1 ? 1 : n <= 2 ? 2 : n <= 5 ? 5 : 10) * p); }
  function barPath(x0, w, top, base) { var r = Math.min(4, (base - top) / 2, w / 2); if (base - top < 0.5) return ''; return 'M' + x0 + ',' + base + 'V' + (top + r) + 'Q' + x0 + ',' + top + ' ' + (x0 + r) + ',' + top + 'H' + (x0 + w - r) + 'Q' + (x0 + w) + ',' + top + ' ' + (x0 + w) + ',' + (top + r) + 'V' + base + 'Z'; }

  /* Group hourly slots into blocks of n for a narrow plot. Values SUM; a
     person counted in two hours is counted twice, which the note states. */
  function group(spec, n) {
    var out = { slots: [], cur: [], prev: spec.prev ? [] : null, partialIdx: -1 };
    for (var i = 0; i < spec.slots.length; i += n) {
      var sl = spec.slots.slice(i, i + n), c = null, p = null;
      for (var j = i; j < i + n && j < spec.slots.length; j++) {
        if (spec.cur[j] !== null && spec.cur[j] !== undefined) c = (c || 0) + spec.cur[j];
        if (spec.prev && spec.prev[j] !== null && spec.prev[j] !== undefined) p = (p || 0) + spec.prev[j];
      }
      if (spec.partialIdx >= i && spec.partialIdx < i + n) out.partialIdx = out.slots.length;
      out.slots.push({ label: sl[0].label, sub: sl.length > 1 ? '–' + sl[sl.length - 1].labelEnd : '', head: sl[0].head + ' – ' + sl[sl.length - 1].headEnd });
      out.cur.push(c); if (out.prev) out.prev.push(p);
    }
    return out;
  }

  function table(spec) {
    var hd = '<tr><th>' + esc(spec.slotName) + '</th><th>' + esc(spec.curLabel) + '</th>' + (spec.prev ? '<th>' + esc(spec.prevLabel) + '</th>' : (spec.momText ? '<th>Against the month before</th>' : '')) + '</tr>';
    var rows = spec.slots.map(function (sl, i) {
      var c = spec.cur[i] === null || spec.cur[i] === undefined ? '—' : fmt(spec.cur[i]) + (i === spec.partialIdx ? ' so far' : '');
      var third = spec.prev ? (spec.prev[i] === null || spec.prev[i] === undefined ? '—' : fmt(spec.prev[i])) : (spec.momText ? (spec.momText(i) || '—') : null);
      return '<tr><td>' + esc(sl.head) + '</td><td>' + c + '</td>' + (third !== null ? '<td>' + third + '</td>' : '') + '</tr>';
    }).join('');
    return '<div class="tbl" tabindex="0" role="region" aria-label="' + esc(spec.title) + ', as a table"><table>' + hd + rows + '</table></div>';
  }

  function draw(el, spec) {
    if (!el) return;
    if (spec.table) { el.innerHTML = table(spec); return; }
    var W = Math.max(240, el.clientWidth || 0), H = spec.height || 208, L = 36, R = 8, T = 22, B = 40;
    var s0 = spec, grouped = false;
    if (spec.grain === 'hour' && (W - L - R) < 520) { s0 = Object.assign({}, spec, group(spec, 3)); grouped = true; }
    var n = s0.slots.length, band = (W - L - R) / n;
    var gw = Math.min(30, band * 0.74), bw = Math.max(4, Math.min(18, gw * 0.52));
    var cur = s0.cur, prev = s0.prev;
    var vals = cur.concat(prev || []).filter(function (x) { return x !== null && x !== undefined; });
    var hi = (vals.length ? Math.max.apply(null, vals) : 1) * 1.12, step = niceStep(hi), mx = Math.ceil(hi / step) * step;
    var y = function (v) { return T + (H - T - B) * (1 - v / mx); }, base = y(0);
    /* Axis labels at an interval that fits their measured width. */
    var maxLab = 0; s0.slots.forEach(function (sl) { maxLab = Math.max(maxLab, String(sl.label).length); });
    var every = Math.max(1, Math.ceil((maxLab * CH_W + 8) / band));
    var s = '<svg width="' + W + '" height="' + H + '" role="img" aria-label="' + esc(spec.title) + '">';
    for (var gv = 0; gv <= mx + 1e-9; gv += step) {
      var gy = y(gv);
      s += '<line x1="' + L + '" x2="' + (W - R) + '" y1="' + gy + '" y2="' + gy + '" stroke="var(--grid)" stroke-width="1"/>';
      s += '<text class="axis" x="' + (L - 8) + '" y="' + (gy + 3) + '" text-anchor="end">' + fmt(Math.round(gv)) + '</text>';
    }
    /* Value labels: the partial slot and the peak first, then the rest if
       they clear their neighbours. */
    var capAt = [];
    var order = []; for (var q = 0; q < n; q++) if (cur[q] !== null && cur[q] !== undefined) order.push(q);
    var peak = order.reduce(function (a, b) { return cur[b] > (cur[a] === undefined ? -1 : cur[a]) ? b : a; }, order[0]);
    var pri = order.filter(function (q) { return q === s0.partialIdx || q === peak; }).concat(order.filter(function (q) { return q !== s0.partialIdx && q !== peak; }));
    var capsWanted = spec.caps === 'all' ? pri : pri.filter(function (q) { return q === s0.partialIdx || q === peak; });
    capsWanted.forEach(function (q) {
      var cx = L + band * q + band / 2, w = String(fmt(cur[q])).length * CH_W + 6;
      if (capAt.every(function (c) { return Math.abs(c.x - cx) > (c.w + w) / 2; })) capAt.push({ x: cx, w: w, i: q });
    });
    for (var i = 0; i < n; i++) {
      var cx = L + band * i + band / 2, sl = s0.slots[i];
      var topC = cur[i] !== null && cur[i] !== undefined ? y(cur[i]) : null;
      var topP = prev && prev[i] !== null && prev[i] !== undefined ? y(prev[i]) : null;
      if (topP !== null) s += '<path d="' + barPath(cx - gw / 2, gw, topP, base) + '" fill="var(--prev)"/>';
      if (topC !== null) {
        var d = barPath(cx - bw / 2, bw, topC, base);
        if (d) {
          s += '<path d="' + d + '" fill="var(--card)" stroke="var(--card)" stroke-width="4" stroke-linejoin="round"/>';
          s += '<path d="' + d + '" fill="var(--data)" opacity="' + (i === s0.partialIdx ? 0.5 : 1) + '"/>';
        }
        if (capAt.some(function (c) { return c.i === i; })) s += '<text class="cap" x="' + cx + '" y="' + (Math.min(topC, topP === null ? topC : topP) - 6) + '" text-anchor="middle">' + fmt(cur[i]) + '</text>';
      }
      if (i % every === 0 || i === s0.partialIdx) {
        var clash = i !== s0.partialIdx && s0.partialIdx >= 0 && Math.abs(i - s0.partialIdx) < every;
        if (!clash) s += '<text class="' + (i === s0.partialIdx ? 'axis-strong' : 'axis') + '" x="' + cx + '" y="' + (H - B + 16) + '" text-anchor="middle">' + esc(sl.label) + '</text>';
        var sub = i === s0.partialIdx ? (spec.partialWord || 'so far') : (sl.sub || '');
        if (sub && !clash) s += '<text class="' + (i === s0.partialIdx ? 'axis-strong' : 'axis') + '" x="' + cx + '" y="' + (H - B + 30) + '" text-anchor="middle">' + esc(sub) + '</text>';
      }
      s += '<rect x="' + (L + band * i) + '" y="' + T + '" width="' + band + '" height="' + (H - T - B) + '" fill="transparent" data-i="' + i + '" tabindex="-1"/>';
    }
    s += '<line x1="' + L + '" x2="' + (W - R) + '" y1="' + base + '" y2="' + base + '" stroke="var(--card-line)" stroke-width="1"/></svg><div class="tip" role="status"></div>';
    el.innerHTML = s;
    el.setAttribute('data-grouped', grouped ? '3h' : '');
    var tip = el.querySelector('.tip');
    function show(i) {
      var sl = s0.slots[i];
      var rows = (cur[i] !== null && cur[i] !== undefined ? '<div>' + esc(spec.curLabel) + ': <b>' + fmt(cur[i]) + '</b>' + (i === s0.partialIdx ? ' so far' : '') + '</div>' : '') +
        (prev && prev[i] !== null && prev[i] !== undefined ? '<div>' + esc(spec.prevLabel) + ': <b>' + fmt(prev[i]) + '</b></div>' : '') +
        (!prev && spec.momText && !grouped && spec.momText(i) ? '<div><b>' + spec.momText(i) + '</b></div>' : '');
      tip.innerHTML = '<div><b>' + esc(sl.head) + '</b></div>' + rows;
      tip.style.left = Math.max(80, Math.min(W - 80, L + band * i + band / 2)) + 'px'; tip.style.top = '8px'; tip.style.opacity = 1;
    }
    var rs = el.querySelectorAll('rect[data-i]');
    for (var k = 0; k < rs.length; k++) (function (r) {
      var i = +r.getAttribute('data-i');
      r.addEventListener('mousemove', function () { show(i); });
      r.addEventListener('click', function () { show(i); });     /* a tap shows it too */
      r.addEventListener('mouseleave', function () { tip.style.opacity = 0; });
    })(rs[k]);
    return { grouped: grouped };
  }
  return { draw: draw, table: table, niceStep: niceStep, group: group };
})(GW);
