/* ============================================================
   Ads booking modal — the close affordances (v5.7.2-ads).

   This is the ONE part of gushwork-form-popup.js that is deliberately
   NOT shared with gushwork-form.js: a modal needs a way out and /demo's
   inline column does not. So it gets its own file rather than living in
   test-ads-parity.js, which is about sameness.

   The overlay IIFE is LIFTED out of the shipped file and EXECUTED against
   a hand-rolled DOM stub — not re-implemented here. The repo is
   dependency-free (no jsdom), and a test that exercises a copy of the
   source can pass while production is broken.

   The assertion that matters most is section 3: dismissing must NOT move
   #step-3 in the DOM. Reparenting an iframe discards its browsing
   context, so a display:none dismiss would reload the RevenueHero
   calendar and throw away the date the lead had already chosen. That bug
   would be invisible in any shape-based test.

   Run:  node tests/test-ads-modal.js
   ============================================================ */

const fs = require('fs');
const path = require('path');
const src = fs.readFileSync(path.join(__dirname, '..', 'gushwork-form-popup.js'), 'utf8');

let pass = 0, fail = 0;
const failures = [];
function ok(name, cond, extra) {
  if (cond) { pass++; }
  else { fail++; failures.push(name + (extra ? ' — ' + extra : '')); }
}
function eq(name, actual, expected) {
  ok(name, actual === expected, `got ${JSON.stringify(actual)}, expected ${JSON.stringify(expected)}`);
}

/* ── the DOM stub ────────────────────────────────────────────────────
   Only what the overlay actually touches. Nodes track parentNode and
   childNodes honestly, because "did this element move?" is the whole
   point of the suite. */
function makeClassList() {
  const set = new Set();
  return {
    _set: set,
    add(...c) { c.forEach((x) => set.add(x)); },
    remove(...c) { c.forEach((x) => set.delete(x)); },
    contains(c) { return set.has(c); },
    toString() { return [...set].join(' '); },
  };
}

function makeNode(tag, id) {
  return {
    tagName: (tag || '').toUpperCase(),
    id: id || '',
    className: '',
    textContent: '',
    style: {},
    childNodes: [],
    parentNode: null,
    classList: makeClassList(),
    _attrs: {},
    _on: {},
    setAttribute(k, v) { this._attrs[k] = v; },
    getAttribute(k) { return this._attrs[k]; },
    appendChild(c) {
      if (c.parentNode) c.parentNode.removeChild(c);
      c.parentNode = this; this.childNodes.push(c); return c;
    },
    insertBefore(c, ref) {
      if (c.parentNode) c.parentNode.removeChild(c);
      c.parentNode = this;
      const i = this.childNodes.indexOf(ref);
      this.childNodes.splice(i < 0 ? this.childNodes.length : i, 0, c);
      return c;
    },
    removeChild(c) {
      const i = this.childNodes.indexOf(c);
      if (i > -1) this.childNodes.splice(i, 1);
      c.parentNode = null; return c;
    },
    remove() { if (this.parentNode) this.parentNode.removeChild(this); },
    addEventListener(t, f) { (this._on[t] = this._on[t] || []).push(f); },
    fire(t, ev) { (this._on[t] || []).forEach((f) => f(ev || {})); },
  };
}

/* Rebuilt per test so no test can leak state into the next. */
function buildEnv() {
  const observers = [];
  const byId = {};
  function mk(tag, id) { const n = makeNode(tag, id); if (id) byId[id] = n; return n; }

  const html = mk('html');
  const body = mk('body');
  const head = mk('head');
  // The hero column the form lives in, and #step-3 inside it.
  const hero = mk('div', 'form-wrap-view');
  const wrapper = mk('div', 'main-wrapper');
  const step3 = mk('div', 'step-3');
  const rhEmbed = mk('div', 'rh-embed');
  const nextBtn = mk('button', 'step-2-next');
  nextBtn.className = 'wf-button wf-primary';
  hero.appendChild(step3);
  step3.appendChild(rhEmbed);
  body.appendChild(wrapper);
  body.appendChild(hero);

  const document = {
    readyState: 'complete',
    documentElement: html,
    head, body,
    _on: {},
    createElement: (t) => mk(t),
    createComment: () => makeNode('#comment'),
    createTextNode: (d) => { const n = makeNode('#text'); n.data = d; n.textContent = d; return n; },
    getElementById: (id) => byId[id] || null,
    addEventListener(t, f) { (this._on[t] = this._on[t] || []).push(f); },
    fire(t, ev) { (this._on[t] || []).forEach((f) => f(ev || {})); },
  };

  function MutationObserver(cb) {
    this.observe = (el) => observers.push({ el, cb });
    this.disconnect = () => {};
  }
  const window = {};

  // Lift the overlay IIFE whole, by brace-matching from its signature.
  const marker = '(function rhStepOverlay() {';
  const startIdx = src.indexOf(marker);
  if (startIdx === -1) throw new Error('rhStepOverlay IIFE not found');
  let depth = 0, end = -1;
  for (let i = src.indexOf('{', startIdx); i < src.length; i++) {
    if (src[i] === '{') depth++;
    else if (src[i] === '}') { depth--; if (depth === 0) { end = i + 1; break; } }
  }
  const iife = src.slice(startIdx, end) + ')();';

  new Function('document', 'window', 'MutationObserver', iife)(document, window, MutationObserver);

  // Drive the observer the way a real style change would.
  function setDisplay(v) { step3.style.display = v; observers.forEach((o) => o.cb()); }

  return { document, window, html, body, head, hero, wrapper, step3, rhEmbed, nextBtn, setDisplay,
           api: () => window.__gwRhOverlay,
           closeBtn: () => step3.childNodes.find((n) => n.className === 'gw-rh-close'),
           resume: () => hero.childNodes.find((n) => n.id === 'gw-rh-resume') };
}

/* ============================================================
   1. Opening still works exactly as before
   ============================================================ */
{
  const e = buildEnv();
  ok('open: nothing portaled before the step is shown', e.step3.parentNode === e.hero);
  ok('open: not active before the step is shown', !e.html.classList.contains('gw-rh-active'));

  e.setDisplay('block');
  ok('open: #step-3 is portaled to body', e.step3.parentNode === e.body);
  ok('open: gw-rh-active is set', e.html.classList.contains('gw-rh-active'));
  ok('open: a placeholder is left in the hero column',
     e.hero.childNodes.some((n) => n.tagName === '#COMMENT'));
  ok('open: not dismissed on open', !e.html.classList.contains('gw-rh-dismissed'));
}

/* ============================================================
   2. The close control belongs to #step-3, not #rh-embed

   The RH SDK owns #rh-embed and re-renders it on open, so a button
   parented there would be destroyed the moment the calendar loads.
   ============================================================ */
{
  const e = buildEnv();
  const btn = e.closeBtn();
  ok('close btn: exists', !!btn);
  ok('close btn: is a child of #step-3', !!btn && btn.parentNode === e.step3);
  ok('close btn: is NOT inside #rh-embed',
     !e.rhEmbed.childNodes.some((n) => n.className === 'gw-rh-close'));
  eq('close btn: is a real button', btn && btn.tagName, 'BUTTON');
  eq('close btn: type is button, so it cannot submit a form', btn && btn._attrs.type, undefined);
  ok('close btn: has an accessible label',
     !!btn && typeof btn.getAttribute('aria-label') === 'string' && btn.getAttribute('aria-label').length > 0);
  ok('close btn: renders a glyph', !!btn && btn.childNodes.length === 1);
}

/* ============================================================
   3. THE ONE THAT MATTERS — dismissing must not move #step-3

   Moving it would reparent the RH iframe, discarding its browsing
   context, reloading the calendar and losing the lead's chosen date.
   ============================================================ */
{
  const e = buildEnv();
  e.setDisplay('block');
  const parentWhenOpen = e.step3.parentNode;
  const embedParentWhenOpen = e.rhEmbed.parentNode;

  e.api().dismiss();

  ok('dismiss: gw-rh-dismissed is set', e.html.classList.contains('gw-rh-dismissed'));
  ok('dismiss: #step-3 did NOT move', e.step3.parentNode === parentWhenOpen);
  ok('dismiss: #rh-embed did NOT move', e.rhEmbed.parentNode === embedParentWhenOpen);
  eq('dismiss: inline display is untouched, so the observer stays quiet', e.step3.style.display, 'block');
  ok('dismiss: gw-rh-active is still set — the overlay is hidden, not torn down',
     e.html.classList.contains('gw-rh-active'));
  ok('dismiss: the placeholder is still in the hero column, ready for reopen',
     e.hero.childNodes.some((n) => n.tagName === '#COMMENT'));
}

/* ============================================================
   4. The resume card
   ============================================================ */
{
  const e = buildEnv();
  e.setDisplay('block');
  ok('resume: no card while the calendar is open', !e.resume());

  e.api().dismiss();
  const card = e.resume();
  ok('resume: card is inserted into the hero column', !!card);
  const title = card && card.childNodes[0];
  const btn = card && card.childNodes[1];

  ok('resume: reads as UNFINISHED, not complete',
     !!title && /one step left/i.test(title.textContent), title && title.textContent);
  ok('resume: never claims the lead is done',
     !!title && !/thank|thanks|\bdone\b|received|✓/i.test(title.textContent), title && title.textContent);
  /* Promising outreach is the single most booking-suppressing thing this
     card could say, and it is not what actually happens. */
  ok('resume: never promises anyone will reach out',
     !!title && !/reach out|be in touch|contact you|call you|get back to you/i.test(title.textContent),
     title && title.textContent);
  eq('resume: one button, labelled for booking', btn && btn.textContent, 'Pick a time');
  eq('resume: exactly one action, no alternative', card && card.childNodes.length, 2);
  eq('resume: borrows the real Next button styling', btn && btn.className, e.nextBtn.className);

  e.api().reopen();
  ok('reopen: card is removed', !e.resume());
  ok('reopen: dismissed flag cleared', !e.html.classList.contains('gw-rh-dismissed'));
  ok('reopen: #step-3 STILL has not moved — the calendar is intact',
     e.step3.parentNode === e.body);
  eq('reopen: inline display never changed throughout', e.step3.style.display, 'block');
}

/* ============================================================
   5. A lead who already booked is not nagged
   ============================================================ */
{
  const e = buildEnv();
  e.setDisplay('block');
  e.html.classList.add('gw-rh-booked');   // what MEETING_BOOKED sets
  e.api().dismiss();
  const card = e.resume();
  const title = card && card.childNodes[0];
  const btn = card && card.childNodes[1];
  ok('booked: copy acknowledges the booking',
     !!title && /booked/i.test(title.textContent), title && title.textContent);
  ok('booked: does NOT tell them a step is left',
     !!title && !/one step left/i.test(title.textContent), title && title.textContent);
  ok('booked: the button is not a second booking prompt',
     !!btn && btn.textContent !== 'Pick a time', btn && btn.textContent);
}

/* ============================================================
   6. Every trigger, and the things that must NOT trigger
   ============================================================ */
{
  // cross
  let e = buildEnv();
  e.setDisplay('block');
  e.closeBtn().fire('click', {});
  ok('trigger: the cross dismisses', e.html.classList.contains('gw-rh-dismissed'));

  // backdrop tap — target IS #step-3
  e = buildEnv();
  e.setDisplay('block');
  e.step3.fire('click', { target: e.step3 });
  ok('trigger: a backdrop tap dismisses', e.html.classList.contains('gw-rh-dismissed'));

  // a tap on the panel must NOT dismiss
  e = buildEnv();
  e.setDisplay('block');
  e.step3.fire('click', { target: e.rhEmbed });
  ok('trigger: a tap on the panel does NOT dismiss',
     !e.html.classList.contains('gw-rh-dismissed'));

  // Escape
  e = buildEnv();
  e.setDisplay('block');
  e.document.fire('keydown', { key: 'Escape' });
  ok('trigger: Escape dismisses', e.html.classList.contains('gw-rh-dismissed'));

  // other keys must not
  e = buildEnv();
  e.setDisplay('block');
  e.document.fire('keydown', { key: 'Enter' });
  e.document.fire('keydown', { key: 'a' });
  ok('trigger: Enter and letters do NOT dismiss',
     !e.html.classList.contains('gw-rh-dismissed'));

  // Escape before the calendar is ever open must not paint a resume card
  e = buildEnv();
  e.document.fire('keydown', { key: 'Escape' });
  ok('trigger: Escape while closed is inert', !e.html.classList.contains('gw-rh-dismissed'));
  ok('trigger: Escape while closed paints no card', !e.resume());

  // dismissing twice must not insert two cards
  e = buildEnv();
  e.setDisplay('block');
  e.api().dismiss();
  e.api().dismiss();
  eq('trigger: dismiss is idempotent — one card only',
     e.hero.childNodes.filter((n) => n.id === 'gw-rh-resume').length, 1);
}

/* ============================================================
   7. A genuine navigation away must reset everything

   If gw-rh-dismissed survived a real hide, the next open would paint
   nothing at all.
   ============================================================ */
{
  const e = buildEnv();
  e.setDisplay('block');
  e.api().dismiss();
  e.setDisplay('none');           // a real showStep() to another step

  ok('teardown: un-portaled back into the hero column', e.step3.parentNode === e.hero);
  ok('teardown: gw-rh-active cleared', !e.html.classList.contains('gw-rh-active'));
  ok('teardown: gw-rh-dismissed cleared', !e.html.classList.contains('gw-rh-dismissed'));
  ok('teardown: resume card removed', !e.resume());
  ok('teardown: placeholder comment cleaned up',
     !e.hero.childNodes.some((n) => n.tagName === '#COMMENT'));

  // and it can be reopened cleanly afterwards
  e.setDisplay('block');
  ok('teardown: reopens cleanly afterwards',
     e.step3.parentNode === e.body && !e.html.classList.contains('gw-rh-dismissed'));
}

/* ============================================================
   8. Hero width follows the state
   ============================================================ */
{
  const e = buildEnv();
  e.setDisplay('block');
  e.api().dismiss();
  eq('width: hero narrows for the resume card', e.wrapper.style.maxWidth, '1000px');
  eq('width: form column narrows too', e.hero.style.maxWidth, '600px');
  e.api().reopen();
  eq('width: hero widens again for the calendar', e.wrapper.style.maxWidth, '1100px');
  eq('width: form column widens too', e.hero.style.maxWidth, '1040px');
}

/* ============================================================
   9. Shape — the parts that need a live page, asserted in the text
   ============================================================ */
{
  ok('shape: dismissed rule outranks the active display rule', (() => {
    // Same id + one more class is what guarantees this wins regardless of
    // source order. A single-class rule would depend on ordering.
    const m = /html\.gw-rh-active\.gw-rh-dismissed #step-3 \{ display: none !important; \}/.exec(src);
    return !!m;
  })());
  ok('shape: the close button is styled only while the overlay is up',
     /\.gw-rh-close \{ display: none; \}/.test(src) && /html\.gw-rh-active \.gw-rh-close \{/.test(src));

  /* Back must close the calendar, not walk to step 2 — a second submission
     creates a second lead row, and the duplicate-booking guard reads only
     the newest row per email. */
  ok('shape: popstate closes the calendar before treating it as navigation',
     /if \(window\.__gwRhOverlay && window\.__gwRhOverlay\.isOpen\(\)\) \{\s*\n\s*window\.__gwRhOverlay\.dismiss\(\);/.test(src));
  ok('shape: popstate pushes the step-3 entry back',
     /window\.__gwRhOverlay\.dismiss\(\);\s*\n\s*history\.pushState\(\{ step: 'step-3' \}, '', ''\);\s*\n\s*return;/.test(src));
  ok('shape: the popstate intercept sits BEFORE the step lookup', (() => {
    const p = src.slice(src.indexOf("window.addEventListener('popstate'"));
    return p.indexOf('__gwRhOverlay') < p.indexOf('const targetStep');
  })());

  ok('shape: MEETING_BOOKED sets the booked marker',
     /document\.documentElement\.classList\.add\('gw-rh-booked'\);/.test(src));
  /* The marker must be a STANDALONE statement — never a condition, a
     guard, or anything the booking path branches on. Asserted on the line
     itself rather than by scanning nearby text, which would trip over the
     unrelated GTM block below it. */
  ok('shape: the booked marker is a standalone statement', (() => {
    const b = src.slice(src.indexOf('function initRHBookingListener'));
    const block = b.slice(0, b.indexOf('SECTION 9'));
    const line = block.split('\n').find((l) => l.includes('gw-rh-booked'));
    return !!line && line.trim() === "document.documentElement.classList.add('gw-rh-booked');";
  })());
  ok('shape: the booking POST still runs after the marker', (() => {
    const b = src.slice(src.indexOf('function initRHBookingListener'));
    const block = b.slice(0, b.indexOf('SECTION 9'));
    return block.indexOf('gw-rh-booked') < block.indexOf('/booking-confirmed');
  })());

  /* Nothing in this feature may touch validation or submission. Comments
     are stripped first: the overlay's own commentary NAMES submitLead when
     explaining why closing is safe, and matching prose would make this
     assertion unfailable. Line comments are removed by leading-// only, so
     an https:// inside a string is left alone. */
  const overlay = src.slice(src.indexOf('(function rhStepOverlay()'))
    .replace(/\/\*[\s\S]*?\*\//g, '')
    .split('\n').filter((l) => !/^\s*(\/\/|\*)/.test(l)).join('\n');
  ok('shape: the overlay never submits anything',
     !/submitLead|savePartial|fetch\(|dataLayer/.test(overlay));
  ok('shape: the overlay never touches validation',
     !/validateStep|showError|hideError/.test(overlay));

  ok('shape: version header is a -ads build', /MULTI-STEP FORM  v5\.[0-9]+\.[0-9]+-ads/.test(src));
  /* Assert the header and the banner AGREE rather than hard-coding a
     number — a hard-coded version rots at every release, and the failure
     that actually matters is the two disagreeing. */
  ok('shape: header and init banner report the same version', (() => {
    const h = /MULTI-STEP FORM  (v[0-9.]+-ads)/.exec(src);
    const b = /Form initialised (v[0-9.]+-ads) \(Google Ads\)/.exec(src);
    return h && b && h[1] === b[1];
  })(), (() => {
    const h = /MULTI-STEP FORM  (v[0-9.]+-ads)/.exec(src);
    const b = /Form initialised (v[0-9.]+-ads) \(Google Ads\)/.exec(src);
    return `header=${h && h[1]} banner=${b && b[1]}`;
  })());
}

/* ============================================================ */
console.log('');
if (failures.length) {
  console.log('  FAILURES:');
  failures.forEach((f) => console.log('   ✗ ' + f));
  console.log('');
}
console.log(`  passed: ${pass}`);
console.log(`  failed: ${fail}`);
console.log('');
process.exit(fail ? 1 : 0);
