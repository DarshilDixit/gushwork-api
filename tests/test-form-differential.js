/* ============================================================
   The form files, OLD against NEW, run whole and compared.

   WHY THIS EXISTS. v5.21.0 took jQuery out of both form files. That is
   meant to change NOTHING a visitor, the server, GTM or RevenueHero can
   see -- and "nothing changed" is exactly the claim a source assertion
   cannot make, because it can only look for text it already expects.

   So this runs the WHOLE of each file, twice: the reference version
   pulled out of git (REFERENCE below) and the file in the working tree,
   against the same fake page, the same fake network and the same
   scripted visitor, and requires the two recordings to be IDENTICAL.
   Not similar: deep-equal, in order. What is recorded:

     every fetch (url, method, headers, body), every dataLayer push,
     every RevenueHero call (router id, submit payload, embed, open),
     every navigation (the /thank-you redirect string), history entry,
     sessionStorage / localStorage / cookie write, every console line,
     every DOM change (attribute, class, inline style, value, text,
     insert, remove, listener added), every timer scheduled, every
     intl-tel-input call and its options -- and the final page.

   THE FAKE PAGE IS HAND-ROLLED (the repo has no jsdom, on purpose). It
   is built from the markup the live pages actually serve (/demo, the CRM
   landers, /aeo, /start, a lander with no phone field), read on 17 Oct
   2026. intl-tel-input, its utils and RevenueHero are fakes that record
   what they are asked. jQuery, for the OLD file only, is a STAND-IN that
   implements just the three calls v5.20.0 makes -- $(selector).each,
   $(element).attr and the typeof guard -- the way jQuery 3.6.0 does
   them; any other jQuery call throws, so a file that used more of it
   would fail here rather than be quietly mis-modelled.

   WHAT THIS CANNOT PROVE, said plainly so nobody reads more into it:
   it is not a browser. Real layout, real CSS, the real intl-tel-input
   and real jQuery internals are not here. Two files that do the same
   thing to this fake page do the same thing to a real one only as far
   as the fake page behaves like a real one. It can tell you the two
   versions ASK the page for the same things in the same order; it cannot
   tell you what Chrome draws.

   TWO TIMINGS, because removing a request changes WHEN the phone field
   is set up, and nothing else:
     ALIGNED -- the libraries reach NEW at the moment they reached OLD
       (OLD waited one extra round trip, for jQuery). Everything must
       then match, timestamps included. This is the proof that the code
       paths are the same.
     SAME LATENCY -- every request takes the same time, as on a real
       network, so NEW sets the phone field up one round trip sooner.
       Everything the visitor does after the page has loaded must still
       match exactly, the final page must match, and the page-load
       effects must be the same set, just earlier.

   KNOWN, DOCUMENTED DIFFERENCES, asserted as differences rather than
   hidden: NEW does not request jQuery; NEW does not set window.jQuery;
   and when the jQuery request itself FAILS, OLD never set the phone
   field up at all while NEW (which no longer makes that request) does.

   THIS IS A REFACTOR LOCK. REFERENCE names the commit whose behaviour
   the working tree must reproduce. A later change that is MEANT to
   change behaviour must move REFERENCE in the same PR -- that is the
   moment a reviewer should be asking why -- and the absolute
   expectations near the end of the file change with it.

   Needs git (it reads REFERENCE with `git show`). No network, no
   database, no dependencies. Run:  node tests/test-form-differential.js
   ============================================================ */

require('./crash-reporter')('test-form-differential');

const fs   = require('fs');
const path = require('path');
const vm   = require('vm');
const { execFileSync } = require('child_process');

const ROOT = path.join(__dirname, '..');
/* v5.20.0 / v5.20.0-ads: the merge of PR 149, the last release that
   loaded jQuery. */
const REFERENCE = '08ecc1ef07aa34f6fc3edc3abc2096b7b679abbe';

let pass = 0, fail = 0;
const failures = [];
function ok(name, cond, extra) {
  if (cond) { pass++; } else { fail++; failures.push(name + (extra ? ' — ' + extra : '')); }
}

/* ── the two versions of each file ─────────────────────────────────── */
function readReference(file) {
  return execFileSync('git', ['show', REFERENCE + ':' + file],
    { cwd: ROOT, encoding: 'utf8', maxBuffer: 32 * 1024 * 1024, stdio: ['ignore', 'pipe', 'pipe'] });
}
const FILES = ['gushwork-form.js', 'gushwork-form-popup.js'];
const SOURCES = {};
for (const f of FILES) {
  const oldSrc = readReference(f);
  const newSrc = fs.readFileSync(path.join(ROOT, f), 'utf8');
  SOURCES[f] = {
    old: { src: oldSrc, script: new vm.Script(oldSrc, { filename: 'OLD/' + f }), banner: bannerOf(oldSrc) },
    new: { src: newSrc, script: new vm.Script(newSrc, { filename: 'NEW/' + f }), banner: bannerOf(newSrc) },
  };
}
function bannerOf(src) {
  const m = /Form initialised (v[0-9]+\.[0-9]+\.[0-9]+(?:-ads)?) /.exec(src);
  return m ? m[1] : null;
}

const CDN = 'https://cdnjs.cloudflare.com/ajax/libs/';
const URL_JQUERY = CDN + 'jquery/3.6.0/jquery.min.js';
const URL_ITI    = CDN + 'intl-tel-input/17.0.8/js/intlTelInput.min.js';
const URL_UTILS  = CDN + 'intl-tel-input/17.0.8/js/utils.js';
const API        = 'https://gushwork-api-production.up.railway.app';

/* ════════════════════════════════════════════════════════════════════
   THE FAKE PAGE
   ════════════════════════════════════════════════════════════════════ */
function makeWorld(opts) {
  const trace = [];
  const T0 = 1760000000000;
  let now = T0;
  let phase = 'load';
  let userStart = 0;
  let seq = 0;
  const scripts = [];
  function rec(k, d) { trace.push(Object.assign({ t: now - T0, phase, k }, d || {})); }

  /* ── scheduler: a fake clock, real microtasks ── */
  let timerSeq = 0;
  const timers = [];
  function addTimer(fn, ms, internal, interval) {
    const id = ++timerSeq;
    const delay = Math.max(0, Number(ms) || 0);
    timers.push({ id, at: now + delay, fn, interval: interval ? delay || 1 : 0 });
    if (!internal) rec(interval ? 'interval' : 'timer', { ms: delay });
    return id;
  }
  function clearTimer(id) { const i = timers.findIndex((x) => x.id === id); if (i !== -1) timers.splice(i, 1); }
  const settle = () => new Promise((r) => setImmediate(r));
  function runCb(fn, args, where) {
    try {
      const r = fn.apply(null, args || []);
      if (r && typeof r.then === 'function') r.then(null, (e) => rec('error', { where, msg: String(e && e.message) }));
    } catch (e) { rec('error', { where, msg: String(e && e.message) }); }
  }
  async function advanceTo(target) {
    for (;;) {
      await settle(); await settle();
      let next = null;
      for (const tm of timers) if (tm.at <= target && (!next || tm.at < next.at || (tm.at === next.at && tm.id < next.id))) next = tm;
      if (!next) { now = target; await settle(); return; }
      now = next.at;
      if (next.interval) next.at = now + next.interval;
      else timers.splice(timers.indexOf(next), 1);
      runCb(next.fn, [], 'timer');
    }
  }
  const run = (ms) => advanceTo(now + ms);
  async function idle(cap) {
    const limit = now + (cap || 120000);
    for (;;) {
      await settle(); await settle();
      if (!timers.length) return;
      const nextAt = Math.min.apply(null, timers.map((x) => x.at));
      if (nextAt > limit) throw new Error('page never went idle');
      await advanceTo(nextAt);
    }
  }

  /* ── events ── */
  class FakeEvent {
    constructor(type, init) {
      init = init || {};
      this.type = type;
      this.bubbles = !!init.bubbles;
      this.cancelable = !!init.cancelable;
      this.defaultPrevented = false;
      this.target = null;
      this.currentTarget = null;
      for (const k of Object.keys(init)) if (!(k in this)) this[k] = init[k];
    }
    preventDefault() { this.defaultPrevented = true; }
    stopPropagation() { this._stop = true; }
    stopImmediatePropagation() { this._stop = true; this._stopNow = true; }
  }

  function listen(node, type, fn) {
    if (typeof fn !== 'function') return;
    (node._ls[type] = node._ls[type] || []).push(fn);
    rec('listen', { el: desc(node), type });
  }
  function dispatch(target, ev) {
    if (!ev.target) ev.target = target;
    const pathNodes = [];
    for (let n = target; n; n = n.parentNode) pathNodes.push(n);
    if (pathNodes[pathNodes.length - 1] === doc && target !== win) pathNodes.push(win);
    for (let i = 0; i < pathNodes.length; i++) {
      if (i > 0 && !ev.bubbles) break;
      const node = pathNodes[i];
      ev.currentTarget = node;
      for (const fn of (node._ls[ev.type] || []).slice()) {
        runCb(fn.bind(node), [ev], 'listener:' + ev.type);
        if (ev._stopNow) break;
      }
      if (ev._stop) break;
    }
    return !ev.defaultPrevented;
  }

  /* ── nodes ── */
  const observers = new Set();
  function notifyAttr(el, name) {
    for (const mo of observers) {
      for (const o of mo._targets) {
        if (o.target !== el || !o.opts.attributes) continue;
        if (o.opts.attributeFilter && o.opts.attributeFilter.indexOf(name) === -1) continue;
        mo._queue.push({ type: 'attributes', target: el, attributeName: name });
        if (!mo._sched) {
          mo._sched = true;
          queueMicrotask(() => {
            mo._sched = false;
            const recs = mo._queue.splice(0);
            if (recs.length) runCb(mo._cb, [recs, mo], 'mutationobserver');
          });
        }
      }
    }
  }

  class Node {
    constructor() { this.parentNode = null; this.childNodes = []; this._ls = {}; }
    get parentElement() { return this.parentNode && this.parentNode.nodeType === 1 ? this.parentNode : null; }
    get firstChild() { return this.childNodes[0] || null; }
    get nextSibling() {
      if (!this.parentNode) return null;
      const s = this.parentNode.childNodes; return s[s.indexOf(this) + 1] || null;
    }
    get isConnected() { let n = this; while (n.parentNode) n = n.parentNode; return n === doc; }
    get ownerDocument() { return doc; }
    addEventListener(t, fn) { listen(this, t, fn); }
    removeEventListener(t, fn) { const l = this._ls[t] || []; const i = l.indexOf(fn); if (i !== -1) l.splice(i, 1); rec('unlisten', { el: desc(this), type: t }); }
    dispatchEvent(ev) { return dispatch(this, ev); }
    contains(n) { for (let p = n; p; p = p.parentNode) if (p === this) return true; return false; }
    _detach(c) { if (c.parentNode) { const s = c.parentNode.childNodes; s.splice(s.indexOf(c), 1); c.parentNode = null; } }
    appendChild(c) { return this.insertBefore(c, null); }
    insertBefore(c, ref) {
      if (c === ref) return c;
      this._detach(c);
      if (this._html != null) this._html = null;
      const s = this.childNodes;
      const i = ref ? s.indexOf(ref) : -1;
      if (i === -1) s.push(c); else s.splice(i, 0, c);
      c.parentNode = this;
      rec('insert', Object.assign({ node: desc(c), into: desc(this), before: ref ? desc(ref) : null }, c.tagName === 'SCRIPT' ? { script: true } : {}));
      if (c.isConnected) startScripts(c);
      return c;
    }
    removeChild(c) { rec('remove', { node: desc(c), from: desc(this) }); this._detach(c); return c; }
    remove() { if (this.parentNode) this.parentNode.removeChild(this); }
  }
  class Text extends Node {
    constructor(data) { super(); this.nodeType = 3; this.data = String(data); }
    get textContent() { return this.data; }
    set textContent(v) { this.data = String(v); }
    cloneNode() { return new Text(this.data); }
  }
  class Comment extends Node {
    constructor(data) { super(); this.nodeType = 8; this.data = String(data); }
    cloneNode() { return new Comment(this.data); }
  }

  function kebab(k) { return k.indexOf('-') === 0 ? k : k.replace(/[A-Z]/g, (c) => '-' + c.toLowerCase()); }
  function makeStyle(el, initial) {
    const props = new Map();
    const serial = () => Array.from(props, ([k, v]) => k + ': ' + v + ';').join(' ');
    const parse = (txt) => String(txt || '').split(';').map((x) => x.trim()).filter(Boolean).forEach((d) => {
      const i = d.indexOf(':'); if (i > 0) props.set(d.slice(0, i).trim().toLowerCase(), d.slice(i + 1).trim());
    });
    parse(initial);
    function changed(key, before) {
      const after = props.has(key) ? props.get(key) : null;
      rec('style', { el: desc(el), prop: key, v: after });
      if (before !== after) notifyAttr(el, 'style');
    }
    const api = {
      setProperty(k, v, prio) { const b = props.has(k) ? props.get(k) : null; const s = String(v == null ? '' : v); if (s === '') props.delete(k); else props.set(k, s + (prio ? ' !' + prio : '')); changed(k, b); },
      removeProperty(k) { const b = props.has(k) ? props.get(k) : null; props.delete(k); changed(k, b); return b ? b.replace(/ !important$/, '') : ''; },
      getPropertyValue(k) { return (props.get(k) || '').replace(/ !important$/, ''); },
    };
    return new Proxy(api, {
      get(t, k) {
        if (typeof k !== 'string') return undefined;
        if (k in api) return api[k];
        if (k === 'cssText') return serial();
        return (props.get(kebab(k)) || '').replace(/ !important$/, '');
      },
      set(t, k, v) {
        if (k === 'cssText') { props.clear(); parse(v); rec('style', { el: desc(el), prop: 'cssText', v: serial() }); notifyAttr(el, 'style'); return true; }
        const key = kebab(String(k));
        const b = props.has(key) ? props.get(key) : null;
        const s = v == null ? '' : String(v);
        if (s === '') props.delete(key); else props.set(key, s);
        changed(key, b);
        return true;
      },
    });
  }

  function classListOf(el) {
    const list = () => (el.getAttribute('class') || '').split(/\s+/).filter(Boolean);
    const write = (arr) => el.setAttribute('class', arr.join(' '));
    return {
      add(...c) { const a = list(); c.forEach((x) => { if (a.indexOf(x) === -1) a.push(x); }); write(a); },
      remove(...c) { write(list().filter((x) => c.indexOf(x) === -1)); },
      contains(c) { return list().indexOf(c) !== -1; },
      toggle(c, force) {
        const has = list().indexOf(c) !== -1;
        const want = force === undefined ? !has : !!force;
        if (want && !has) this.add(c); else if (!want && has) this.remove(c);
        return want;
      },
      get length() { return list().length; },
      toString() { return list().join(' '); },
    };
  }

  class Element extends Node {
    constructor(tag) {
      super();
      this.nodeType = 1;
      this.tagName = String(tag).toUpperCase();
      this._attrs = new Map();
      this._html = null;
      this._style = makeStyle(this, '');
      this.classList = classListOf(this);
    }
    get style() { return this._style; }
    getAttribute(n) { n = String(n).toLowerCase(); if (n === 'style') { const c = this._style.cssText; return c || (this._attrs.has('style') ? '' : null); } return this._attrs.has(n) ? this._attrs.get(n) : null; }
    hasAttribute(n) { return this.getAttribute(n) !== null; }
    setAttribute(n, v) {
      n = String(n).toLowerCase(); v = String(v);
      if (n === 'style') { this._style.cssText = v; this._attrs.set('style', ''); return; }
      this._attrs.set(n, v);
      rec('attr', Object.assign({ el: desc(this), name: n, v }, this.tagName === 'SCRIPT' ? { script: true } : {}));
      notifyAttr(this, n);
    }
    removeAttribute(n) {
      n = String(n).toLowerCase();
      const had = this._attrs.has(n);
      this._attrs.delete(n);
      rec('attr', { el: desc(this), name: n, v: null, had });
      if (had) notifyAttr(this, n);
    }
    get id() { return this.getAttribute('id') || ''; }
    set id(v) { this.setAttribute('id', v); }
    get className() { return this.getAttribute('class') || ''; }
    set className(v) { this.setAttribute('class', v); }
    get textContent() { if (this._html != null) return this._html.replace(/<[^>]*>/g, ''); return this.childNodes.map((c) => c.nodeType === 8 ? '' : c.textContent).join(''); }
    set textContent(v) {
      this.childNodes.slice().forEach((c) => this._detach(c));
      this._html = null;
      const s = String(v == null ? '' : v);
      if (s) { const t = new Text(s); t.parentNode = this; this.childNodes.push(t); }
      rec('text', { el: desc(this), v: s });
    }
    get innerHTML() { return this._html != null ? this._html : this.childNodes.map(serialise).join(''); }
    set innerHTML(v) {
      this.childNodes.slice().forEach((c) => this._detach(c));
      this._html = String(v);
      rec('html', { el: desc(this), v: this._html });
    }
    querySelectorAll(sel) { const m = compile(sel); const out = []; walk(this, (n) => { if (n !== this && m(n)) out.push(n); }); return makeNodeList(out); }
    querySelector(sel) { return this.querySelectorAll(sel)[0] || null; }
    matches(sel) { return compile(sel)(this); }
    closest(sel) { const m = compile(sel); for (let n = this; n && n.nodeType === 1; n = n.parentNode) if (m(n)) return n; return null; }
    insertAdjacentElement(where, el) {
      if (where === 'afterend') this.parentNode.insertBefore(el, this.nextSibling);
      else if (where === 'beforebegin') this.parentNode.insertBefore(el, this);
      else if (where === 'afterbegin') this.insertBefore(el, this.firstChild);
      else this.appendChild(el);
      return el;
    }
    cloneNode(deep) {
      const c = createElement(this.tagName.toLowerCase(), true);
      for (const [k, v] of this._attrs) if (k !== 'style') c._attrs.set(k, v);
      c._style.cssText = this._style.cssText;
      if ('_value' in this) c._value = this._value;
      if ('_checked' in this) c._checked = this._checked;
      c._html = this._html;
      if (deep) this.childNodes.forEach((k) => { const kc = k.cloneNode(true); kc.parentNode = c; c.childNodes.push(kc); });
      return c;
    }
    focus() { focusEl(this); }
    blur() { if (doc.activeElement === this) blurActive(); }
    click() { dispatch(this, new FakeEvent('click', { bubbles: true, cancelable: true })); }
    scrollIntoView(o) { rec('scroll', { el: desc(this), o: o || null }); }
    getBoundingClientRect() { return { top: 0, left: 0, right: 0, bottom: 0, width: 0, height: 0 }; }
  }
  class HTMLElement extends Element {}
  class HTMLInputElement extends HTMLElement {
    get type() { return (this.getAttribute('type') || 'text').toLowerCase(); }
    set type(v) { this.setAttribute('type', v); }
    get name() { return this.getAttribute('name') || ''; }
    set name(v) { this.setAttribute('name', v); }
    get checked() { return '_checked' in this ? this._checked : this.hasAttribute('checked'); }
    set checked(v) { this._checked = !!v; rec('checked', { el: desc(this), v: !!v }); }
  }
  /* The VALUE accessor lives on the prototype, as in a browser: the Ads
     file reads this exact descriptor to intercept programmatic writes. */
  Object.defineProperty(HTMLInputElement.prototype, 'value', {
    configurable: true, enumerable: true,
    get() { return '_value' in this ? this._value : (this.getAttribute('value') || ''); },
    set(v) { this._value = v == null ? '' : String(v); rec('value', { el: desc(this), v: this._value }); },
  });
  class HTMLTextAreaElement extends HTMLElement {
    get value() { return '_value' in this ? this._value : ''; }
    set value(v) { this._value = v == null ? '' : String(v); rec('value', { el: desc(this), v: this._value }); }
    get name() { return this.getAttribute('name') || ''; }
  }
  class HTMLScriptElement extends HTMLElement {
    get src() { return this.getAttribute('src') || ''; }
    set src(v) { this.setAttribute('src', v); }
  }
  class HTMLButtonElement extends HTMLElement {
    get type() { return this.getAttribute('type') || 'submit'; }
    set type(v) { this.setAttribute('type', v); }
  }
  class HTMLLinkElement extends HTMLElement {
    get href() { return this.getAttribute('href') || ''; }
    set href(v) { this.setAttribute('href', v); }
    get rel() { return this.getAttribute('rel') || ''; }
    set rel(v) { this.setAttribute('rel', v); }
  }
  function createElement(tag, quiet) {
    const t = String(tag).toLowerCase();
    const C = t === 'input' ? HTMLInputElement : t === 'textarea' ? HTMLTextAreaElement
      : t === 'script' ? HTMLScriptElement : t === 'button' ? HTMLButtonElement
      : t === 'link' ? HTMLLinkElement : HTMLElement;
    const el = new C(t);
    if (!quiet) rec('create', Object.assign({ tag: t }, t === 'script' ? { script: true } : {}));
    return el;
  }
  function serialise(n) {
    if (n.nodeType === 3) return n.data;
    if (n.nodeType === 8) return '<!--' + n.data + '-->';
    const at = Array.from(n._attrs, ([k, v]) => k === 'style' ? ' style="' + n._style.cssText + '"' : ' ' + k + '="' + v + '"').join('');
    return '<' + n.tagName.toLowerCase() + at + '>' + n.innerHTML + '</' + n.tagName.toLowerCase() + '>';
  }
  function makeNodeList(arr) {
    const nl = arr.slice();
    nl.item = (i) => arr[i] || null;
    return nl;
  }
  function walk(n, fn) { for (const c of n.childNodes) { if (c.nodeType === 1) { fn(c); walk(c, fn); } } }

  /* ── selectors: compound selectors and comma lists ONLY. Anything else
     THROWS, so a selector this cannot model fails the suite instead of
     quietly matching the wrong thing. ── */
  const selCache = new Map();
  function compile(selector) {
    if (selCache.has(selector)) return selCache.get(selector);
    const groups = String(selector).split(',').map((s) => s.trim()).filter(Boolean).map(parseCompound);
    const fn = (el) => el && el.nodeType === 1 && groups.some((g) => g(el));
    selCache.set(selector, fn);
    return fn;
  }
  function parseCompound(s) {
    const tests = [];
    let i = 0;
    const m0 = /^[a-zA-Z][a-zA-Z0-9-]*/.exec(s);
    if (m0) { const tag = m0[0].toUpperCase(); tests.push((el) => el.tagName === tag); i = m0[0].length; }
    while (i < s.length) {
      const rest = s.slice(i);
      let m;
      if ((m = /^#([\w-]+)/.exec(rest))) { const v = m[1]; tests.push((el) => el.getAttribute('id') === v); }
      else if ((m = /^\.([\w-]+)/.exec(rest))) { const v = m[1]; tests.push((el) => el.classList.contains(v)); }
      else if ((m = /^\[\s*([\w-]+)\s*(?:([*^$]?=)\s*(?:"([^"]*)"|'([^']*)'|([^\]\s]+))\s*(i)?)?\s*\]/.exec(rest))) {
        const name = m[1].toLowerCase(), op = m[2], val = m[3] != null ? m[3] : m[4] != null ? m[4] : m[5], ci = !!m[6];
        tests.push((el) => {
          const a = el.getAttribute(name);
          if (a === null) return false;
          if (!op) return true;
          const x = ci ? a.toLowerCase() : a, y = ci ? val.toLowerCase() : val;
          if (op === '=') return x === y;
          if (op === '*=') return y !== '' && x.indexOf(y) !== -1;
          if (op === '^=') return y !== '' && x.indexOf(y) === 0;
          if (op === '$=') return y !== '' && x.slice(-y.length) === y;
          return false;
        });
      } else if ((m = /^:checked/.exec(rest))) { tests.push((el) => el.checked === true); }
      else throw new Error('fake DOM: unsupported selector "' + s + '"');
      i += m[0].length;
    }
    return (el) => tests.every((t) => t(el));
  }

  /* ── element descriptors for the trace ── */
  function desc(n) {
    if (!n) return null;
    if (n === win) return 'window';
    if (n === doc) return 'document';
    if (n.nodeType === 3) return '#text';
    if (n.nodeType === 8) return '<!--' + n.data + '-->';
    const parts = [];
    let cur = n;
    while (cur && cur.nodeType === 1 && !cur._attrs.get('id')) {
      const p = cur.parentNode;
      /* Script elements are left out of the index: OLD inserted one more
         (jQuery), and that must not renumber every later sibling. */
      const idx = p ? p.childNodes.filter((c) => c.nodeType === 1 && c.tagName !== 'SCRIPT').indexOf(cur) : -1;
      const cls = cur._attrs.get('class');
      parts.unshift(cur.tagName.toLowerCase() + (cls ? '.' + cls.trim().split(/\s+/).join('.') : '') + '[' + idx + ']');
      cur = p;
    }
    const anchor = !cur ? 'detached' : cur === doc ? 'document' : '#' + cur._attrs.get('id');
    return parts.length ? anchor + '>' + parts.join('>') : anchor;
  }

  /* ── document ── */
  const doc = new Node();
  doc.nodeType = 9;
  doc._ls = {};
  doc.activeElement = null;
  doc.readyState = opts.readyState || 'loading';
  doc.referrer = opts.referrer || '';
  const html = createElement('html', true);
  const head = createElement('head', true);
  const body = createElement('body', true);
  html.parentNode = doc; doc.childNodes.push(html);
  head.parentNode = html; html.childNodes.push(head);
  body.parentNode = html; html.childNodes.push(body);
  const cookieJar = new Map((opts.cookies || []).map((c) => { const i = c.indexOf('='); return [c.slice(0, i), c.slice(i + 1)]; }));
  Object.defineProperties(doc, {
    documentElement: { get: () => html },
    head: { get: () => head },
    body: { get: () => body },
    cookie: {
      get: () => Array.from(cookieJar, ([k, v]) => k + '=' + v).join('; '),
      set: (v) => {
        rec('cookie', { v: String(v) });
        const first = String(v).split(';')[0];
        const i = first.indexOf('=');
        if (i > 0) cookieJar.set(first.slice(0, i).trim(), first.slice(i + 1));
      },
    },
  });
  doc.createElement = (t) => createElement(t);
  doc.createTextNode = (t) => new Text(t);
  doc.createComment = (t) => new Comment(t);
  doc.getElementById = (id) => { let f = null; walk(doc, (n) => { if (!f && n._attrs.get('id') === id) f = n; }); return f; };
  doc.querySelectorAll = (sel) => { const m = compile(sel); const out = []; walk(doc, (n) => { if (m(n)) out.push(n); }); return makeNodeList(out); };
  doc.querySelector = (sel) => doc.querySelectorAll(sel)[0] || null;
  doc.addEventListener = (t, fn) => listen(doc, t, fn);
  doc.removeEventListener = (t, fn) => Node.prototype.removeEventListener.call(doc, t, fn);
  doc.dispatchEvent = (ev) => dispatch(doc, ev);

  function blurActive() {
    const a = doc.activeElement;
    if (!a) return;
    doc.activeElement = null;
    rec('blur', { el: desc(a) });
    dispatch(a, new FakeEvent('blur'));
  }
  function focusEl(el) {
    if (doc.activeElement === el) return;
    blurActive();
    doc.activeElement = el;
    rec('focus', { el: desc(el) });
    dispatch(el, new FakeEvent('focus'));
  }

  /* ── scripts: requested when connected, "loaded" by the fake clock ── */
  function startScripts(node) {
    const list = [];
    if (node.nodeType === 1) { if (node.tagName === 'SCRIPT') list.push(node); walk(node, (n) => { if (n.tagName === 'SCRIPT') list.push(n); }); }
    for (const s of list) {
      if (s._started || !s.getAttribute('src')) continue;
      s._started = true;
      const src = s.getAttribute('src');
      scripts.push(src);
      const latency = opts.latency(src);
      addTimer(() => {
        if (opts.failScripts.some((f) => src.indexOf(f) !== -1)) {
          rec('script.error', { src });
          if (typeof s.onerror === 'function') runCb(s.onerror, [new FakeEvent('error')], 'script.onerror');
          return;
        }
        install(src);
        if (typeof s.onload === 'function') runCb(s.onload.bind(s), [new FakeEvent('load')], 'script.onload');
      }, latency, true);
    }
  }

  /* ── window ── */
  const win = {};
  win._ls = {};
  const loc = new URL('https://www.gushwork.ai' + opts.path + (opts.query || ''));
  const location = {
    get href() { return loc.href; },
    set href(v) { rec('navigate', { href: String(v) }); },
    get pathname() { return loc.pathname; },
    get search() { return loc.search; },
    get hostname() { return loc.hostname; },
    get host() { return loc.host; },
    get origin() { return loc.origin; },
    get protocol() { return loc.protocol; },
    get hash() { return loc.hash; },
    assign(v) { rec('navigate', { href: String(v), via: 'assign' }); },
    replace(v) { rec('navigate', { href: String(v), via: 'replace' }); },
    toString() { return loc.href; },
  };
  function makeStorage(name, initial) {
    const m = new Map(Object.entries(initial || {}));
    return {
      getItem: (k) => (m.has(String(k)) ? m.get(String(k)) : null),
      setItem: (k, v) => { rec('storage', { area: name, key: String(k), v: String(v) }); m.set(String(k), String(v)); },
      removeItem: (k) => { rec('storage', { area: name, key: String(k), v: null }); m.delete(String(k)); },
      _dump: () => Object.fromEntries(m),
    };
  }
  const sessionStorage = makeStorage('session', opts.session);
  const localStorage = makeStorage('local', opts.local);
  let uuid = 0;
  class FakeMO {
    constructor(cb) { this._cb = cb; this._targets = []; this._queue = []; this._sched = false; }
    observe(target, o) { this._targets.push({ target, opts: o || {} }); observers.add(this); rec('mo.observe', { el: desc(target), o: o || null }); }
    disconnect() { this._targets = []; observers.delete(this); }
    takeRecords() { return this._queue.splice(0); }
  }
  class FakeRO { constructor() {} observe(el) { rec('ro.observe', { el: desc(el) }); } disconnect() {} unobserve() {} }

  class FakeDate extends Date {
    constructor(...a) { if (a.length) super(...a); else super(now); }
    static now() { return now; }
  }

  function consoleLine(level) {
    return (...args) => rec('console', { level, text: args.map((a) => {
      if (typeof a === 'string') return a;
      if (a instanceof Error || (a && a.name && a.message)) return a.name + ': ' + a.message;
      try { return JSON.stringify(a); } catch (_) { return String(a); }
    }).join(' ') });
  }

  let dataLayer;
  function recordingArray(arr) {
    const origPush = arr.push;
    arr.push = function (...items) { items.forEach((it) => rec('dataLayer', { v: JSON.parse(JSON.stringify(it)) })); return origPush.apply(this, items); };
    return arr;
  }

  Object.assign(win, {
    window: win, self: win,
    document: doc, location,
    history: {
      state: null,
      pushState(s, t, u) { rec('history.push', { s, u }); this.state = s; },
      replaceState(s, t, u) { rec('history.replace', { s, u }); this.state = s; },
    },
    sessionStorage, localStorage,
    navigator: { userAgent: 'differential-harness' },
    screen: { width: 1280, height: 800 },
    innerWidth: 1280, innerHeight: 800,
    crypto: { randomUUID: () => 'uuid-' + (++uuid) + '-0000-4000-8000-000000000000' },
    console: { log: consoleLine('log'), info: consoleLine('info'), warn: consoleLine('warn'), error: consoleLine('error'), debug: consoleLine('debug') },
    setTimeout: (fn, ms, ...a) => addTimer(() => fn(...a), ms),
    clearTimeout: clearTimer,
    setInterval: (fn, ms) => addTimer(fn, ms, false, true),
    clearInterval: clearTimer,
    requestAnimationFrame: (fn) => addTimer(() => fn(now - T0), 16),
    cancelAnimationFrame: clearTimer,
    getComputedStyle: (el) => ({
      paddingLeft: '16px',
      display: el.style.display || (el.classList.contains('is-hidden') ? 'none' : 'block'),
      getPropertyValue: (k) => (k === 'padding-left' ? '16px' : ''),
    }),
    fetch: (url, o) => fakeFetch(url, o || {}),
    Event: FakeEvent,
    MutationObserver: FakeMO,
    ResizeObserver: FakeRO,
    HTMLElement, HTMLInputElement, HTMLTextAreaElement, Element, Node,
    URL, URLSearchParams, AbortController, TextEncoder,
    Date: FakeDate,
    addEventListener(t, fn) { listen(win, t, fn); },
    removeEventListener(t, fn) { const l = win._ls[t] || []; const i = l.indexOf(fn); if (i !== -1) l.splice(i, 1); },
    dispatchEvent(ev) { return dispatch(win, ev); },
    RevenueHero: class RevenueHero {
      constructor(o) {
        rec('rh.new', { o: JSON.parse(JSON.stringify(o || null)) });
        this.dialog = {
          setEmbedTarget: (s) => rec('rh.embed', { s }),
          open: (d) => rec('rh.open', { d: JSON.parse(JSON.stringify(d === undefined ? null : d)) }),
        };
      }
      submit(p) {
        rec('rh.submit', { p: JSON.parse(JSON.stringify(p)) });
        return new Promise((res, rej) => addTimer(() => (opts.rhFails ? rej(new Error('rh down')) : res({ rh: 'route-data-1' })), 250, true));
      }
    },
  });
  Object.defineProperty(win, 'dataLayer', {
    configurable: true, enumerable: true,
    get: () => dataLayer,
    set: (v) => { dataLayer = Array.isArray(v) ? recordingArray(v) : v; rec('dataLayer.set', { isArray: Array.isArray(v) }); },
  });
  if (opts.dataLayer) dataLayer = recordingArray(opts.dataLayer.slice());

  /* The page's own jQuery (Webflow ships 3.5.1 on every page). */
  if (opts.pageJquery) { const pj = makeJqueryStandIn('page-3.5.1'); win.$ = pj; win.jQuery = pj; }

  /* ── network ── */
  function fakeFetch(url, o) {
    url = String(url);
    const method = String(o.method || 'GET').toUpperCase();
    let bodyOut = o.body == null ? null : String(o.body);
    try { if (bodyOut) bodyOut = JSON.parse(bodyOut); } catch (_) { /* keep the raw string */ }
    rec('fetch', { url, method, headers: o.headers ? JSON.parse(JSON.stringify(o.headers)) : null, body: bodyOut });
    const r = opts.route(url, method, bodyOut) || { status: 404, json: {} };
    return new Promise((resolve, reject) => {
      const abortErr = () => { const e = new Error('The operation was aborted.'); e.name = 'AbortError'; return e; };
      if (o.signal) {
        if (o.signal.aborted) { reject(abortErr()); return; }
        o.signal.addEventListener('abort', () => { rec('fetch.abort', { url }); reject(abortErr()); });
      }
      if (r.hang) return;
      addTimer(() => {
        if (r.networkError) { reject(new TypeError('Failed to fetch')); return; }
        const status = r.status || 200;
        resolve({
          ok: status >= 200 && status < 300,
          status,
          json: () => (r.json === undefined ? Promise.reject(new SyntaxError('Unexpected end of JSON input')) : Promise.resolve(JSON.parse(JSON.stringify(r.json)))),
        });
      }, r.latency == null ? 40 : r.latency, true);
    });
  }

  /* ── the three CDN libraries ── */
  function makeJqueryStandIn(label) {
    /* Exactly the jQuery 3.6.0 behaviour of the three calls v5.20.0 made:
       $(selectorString) is a static, document-ordered querySelectorAll
       (jQuery hands a valid CSS selector to qSA); $(element) wraps one
       element; .each(fn) calls fn with this=element and (index, element),
       stopping if fn returns false; .attr(name) is the FIRST element's
       getAttribute, with null turned into undefined. Anything else
       throws, so a file that used more of jQuery fails here loudly. */
    const $ = function (sel) {
      const els = typeof sel === 'string' ? Array.from(doc.querySelectorAll(sel))
        : sel && sel.nodeType === 1 ? [sel] : [];
      const api = {
        length: els.length,
        each(fn) { for (let i = 0; i < els.length; i++) { if (fn.call(els[i], i, els[i]) === false) break; } return proxy; },
        attr(name) { if (!els.length) return undefined; const v = els[0].getAttribute(name); return v == null ? undefined : v; },
      };
      const proxy = new Proxy(api, {
        get(t, k) {
          if (typeof k === 'symbol') return undefined;
          if (k in api) return api[k];
          if (/^[0-9]+$/.test(k)) return els[Number(k)];
          throw new Error('jQuery stand-in: .' + k + ' is not modelled');
        },
      });
      rec('jquery.call', { label, sel: typeof sel === 'string' ? sel : desc(sel) });
      return proxy;
    };
    $.fn = {};
    $.label = label;
    return $;
  }
  const itiInstances = [];
  function install(src) {
    rec('script.load', { src });
    if (src === URL_JQUERY) { const $ = makeJqueryStandIn('cdnjs-3.6.0'); win.$ = $; win.jQuery = $; return; }
    if (src === URL_UTILS) {
      win.intlTelInputUtils = { numberFormat: { E164: 0, INTERNATIONAL: 1, NATIONAL: 2, RFC3966: 3 } };
      return;
    }
    if (src === URL_ITI) {
      win.intlTelInputGlobals = { instances: {} };
      win.intlTelInput = function (input, o) {
        const shown = {};
        for (const k of Object.keys(o)) {
          const v = o[k];
          shown[k] = typeof v === 'function' ? 'fn:' + (v.name || 'anonymous') : v && v.nodeType ? 'node:' + desc(v) : v;
        }
        rec('iti.init', { el: desc(input), o: JSON.parse(JSON.stringify(shown)) });
        /* The DOM work v17 does that the form can observe: a wrapper
           around the input with the flag container before it. */
        const wrap = createElement('div');
        wrap.setAttribute('class', 'iti iti--allow-dropdown');
        input.parentNode.insertBefore(wrap, input);
        const flags = createElement('div');
        flags.setAttribute('class', 'iti__flag-container');
        wrap.appendChild(flags);
        wrap.appendChild(input);
        const hadPlaceholder = Boolean(input.getAttribute('placeholder'));
        if (o.utilsScript && !win.intlTelInputUtils) {
          const s = createElement('script'); s.src = o.utilsScript; head.appendChild(s);
        }
        if (win.intlTelInputUtils && !hadPlaceholder) input.setAttribute('placeholder', '(201) 555-0123');
        const DIAL = { us: '1', ca: '1', gb: '44', in: '91', au: '61' };
        const inst = {
          country: '',
          getNumber(fmt) {
            if (!win.intlTelInputUtils) { rec('iti.call', { m: 'getNumber', fmt, r: '' }); return ''; }
            const d = input.value.replace(/\D/g, '').replace(/^0+/, '');
            const r = fmt === 0 ? '+' + (DIAL[inst.country] || '') + d
              : fmt === 2 ? (d.length === 10 ? '(' + d.slice(0, 3) + ') ' + d.slice(3, 6) + '-' + d.slice(6) : d)
              : d;
            rec('iti.call', { m: 'getNumber', fmt, r });
            return r;
          },
          isValidNumber() {
            const r = win.intlTelInputUtils ? input.value.replace(/\D/g, '').length === 10 : null;
            rec('iti.call', { m: 'isValidNumber', r });
            return r;
          },
          getSelectedCountryData() { return { iso2: inst.country }; },
        };
        inst.promise = new Promise((resolve) => {
          if (o.initialCountry === 'auto' && typeof o.geoIpLookup === 'function') {
            o.geoIpLookup((cc) => {
              rec('iti.geo', { cc });
              if (!input.value) {
                inst.country = String(cc || '').toLowerCase();
                dispatch(input, new FakeEvent('countrychange'));
              }
              resolve();
            });
          } else { inst.country = o.initialCountry || ''; resolve(); }
        });
        itiInstances.push(inst);
        return inst;
      };
    }
  }

  /* ── build the page ── */
  function build(node, parent) {
    if (typeof node === 'string') { const t = new Text(node); t.parentNode = parent; parent.childNodes.push(t); return; }
    const [tag, attrs, kids] = node;
    const el = createElement(tag, true);
    for (const k of Object.keys(attrs || {})) {
      if (k === 'style') { el._style.cssText = attrs[k]; el._attrs.set('style', ''); }
      else el._attrs.set(k.toLowerCase(), String(attrs[k]));
    }
    el.parentNode = parent; parent.childNodes.push(el);
    (kids || []).forEach((k) => build(k, el));
  }
  opts.markup.forEach((n) => build(n, body));

  const g = win;
  vm.createContext(g);

  return {
    g, doc, trace, scripts, rec, run, idle, FakeEvent, dispatch, focusEl, blurActive,
    cookieJar, sessionStorage, localStorage,
    get now() { return now - T0; },
    beginUserPhase() { phase = 'user'; userStart = now - T0; },
    get userStart() { return userStart; },
    loadScript(compiled) {
      compiled.runInContext(g);
      if (doc.readyState === 'loading') {
        doc.readyState = 'interactive';
        dispatch(doc, new FakeEvent('DOMContentLoaded', { bubbles: true }));
      }
    },
    snapshot() { return snap(html); },
    itiInstances,
  };

  function snap(n) {
    if (n.nodeType === 3) return n.data;
    if (n.nodeType === 8) return { comment: n.data };
    const o = { tag: n.tagName.toLowerCase(), attrs: Array.from(n._attrs).filter(([k]) => k !== 'style'), style: n._style.cssText };
    if (n instanceof HTMLInputElement) { o.value = n.value; o.checked = n.checked; }
    if (n instanceof HTMLTextAreaElement) o.value = n.value;
    if (n._html != null) o.html = n._html;
    const ls = Object.keys(n._ls).filter((k) => n._ls[k].length).map((k) => k + ':' + n._ls[k].length);
    if (ls.length) o.listeners = ls;
    o.kids = n.childNodes.filter((c) => c.tagName !== 'SCRIPT').map(snap);
    return o;
  }
}

/* ════════════════════════════════════════════════════════════════════
   THE PAGES — built from the markup the live pages served on 17 Oct 2026
   ════════════════════════════════════════════════════════════════════ */
const fw = (cls, kids) => ['div', { class: 'field-wrapper' + (cls ? ' ' + cls : '') }, kids];
const inp = (id, attrs) => ['input', Object.assign({ id, class: 'input-field w-input', type: 'text', maxlength: '256' }, attrs)];
const err = (id) => ['div', { id, class: 'error-div' }, []];
const radio = (id, name, value, extra) => ['input', Object.assign({ type: 'radio', id, name, value, class: 'w-form-formradioinput w-radio-input' }, extra || {})];

function formMarkup(kind) {
  const formAttrs = { id: 'wf-form-Main-CTA-Form', class: 'form-wrapper new' };
  if (kind === 'demo' || kind === 'demoNeeds') formAttrs['data-rh-router-crm'] = '6804';
  if (kind === 'crm') formAttrs['data-rh-router'] = '6804';
  const phoneAttrs = { type: 'tel', name: 'phone', placeholder: 'Phone', 'data-typing-placeholder': 'Phone Number' };
  if (kind === 'demo' || kind === 'demoNeeds' || kind === 'start' || kind === 'twoPhones') phoneAttrs['ms-code-phone-number'] = 'us,gb,ca,au,in';

  const step1 = [
    ['div', { class: 'ref-wrapper demo' }, [['span', { reff: 'first_name' }, []]]],
    fw('', [inp('email', { type: 'email', name: 'Email', placeholder: 'Work Email', 'data-typing-placeholder': 'name@company.com' }), err('email-error'),
      ['div', { id: 'email-protip', class: 'error-div pro-tip' }, []]]),
    ['div', { id: 'radio-wrap', class: 'field-wrapper' }, [
      radio('sell-b2b', 'Who-do-you-sell-to', 'B2B'), radio('sell-b2c', 'Who-do-you-sell-to', 'B2C'), radio('sell-mixed', 'Who-do-you-sell-to', 'Mixed'),
      err('sell-error')]],
  ];
  if (kind === 'demoNeeds') {
    step1.push(['div', { id: 'needs-wrap', class: 'field-wrapper is-collapsible is-hidden' }, [
      ['input', { type: 'checkbox', id: 'need-aeo', name: 'needs', value: 'aeo' }],
      ['input', { type: 'checkbox', id: 'need-crm', name: 'needs', value: 'crm' }],
      err('needs-error')]]);
  }
  step1.push(...['session-id', 'utm-source', 'utm-medium', 'utm-campaign', 'utm-content', 'referrer', 'enriched-title',
    'enriched-company-size', 'enriched-industry', 'enriched-linkedin', 'step-reached', 'completed', 'prefill-source']
    .map((id) => ['input', { type: 'hidden', id, name: id }]));
  step1.push(['a', { id: 'step-1-next', href: '#', class: 'next-step-btn w-inline-block' }, ['Next']]);

  const step2 = [
    fw('is-col2 margin-8px', [inp('first-name', { name: 'First-Name', placeholder: 'First Name' }), err('first-name-error'),
      inp('last-name', { name: 'last-name', placeholder: 'Last Name' }), err('last-name-error')]),
    fw('margin-8px', [inp('company', { name: 'Company-Name', placeholder: 'Company Name' }), err('company-error')]),
    fw('margin-8px', [inp('website', { name: 'Website-URL', placeholder: 'Website URL' }), err('website-error')]),
  ];
  if (kind !== 'noPhone') step2.push(fw('margin-8px', [inp('phone', phoneAttrs), err('phone-error')]));
  if (kind === 'twoPhones') step2.push(fw('margin-8px', [inp('alt-phone', Object.assign({}, phoneAttrs, { name: 'alt-phone', 'ms-code-phone-number': 'gb,in' }))]));
  step2.push(fw('margin-8px', [inp('hear-about-us', { name: 'Hear-about-us', placeholder: 'From where did you hear about us?' }), err('hear-about-us-error')]));
  if (kind === 'demo' || kind === 'demoNeeds') {
    step2.push(['div', { id: 'about-business-wrap', class: 'field-wrapper about-biz-wrap is-collapsible is-hidden' }, [
      ['textarea', { id: 'about-business', class: 'input-field textarea w-input', maxlength: '1000', placeholder: 'About your business' }, []]]]);
  }
  if (kind === 'crm') {
    step2.push(fw('margin-8px', [['textarea', { id: 'about-business', class: 'input-field textarea w-input', maxlength: '1000', placeholder: 'About your business' }, []]]));
  }
  step2.push(['a', { id: 'step-2-next', href: '#', class: 'next-step-btn _40px w-inline-block' }, ["All set, let's pick a time"]]);

  return [
    ['div', { id: 'main-wrapper' }, [
      ['div', { id: 'form-wrap-view', class: 'main-form-wrap' }, [
        ['div', { class: 'form-person-holder' }, []], ['div', { class: 'speech-bubble' }, []],
        ['form', formAttrs, [
          ['div', { id: 'step-1', class: 'step-1 padding-40' }, step1],
          ['div', { id: 'step-disqualified', class: 'step-dq no-gap' }, [
            ['div', { class: 'field-wrapper' }, [
              radio('disq-waitlist', 'What-would-you-like-to-do', 'Let me know when you support self-serve businesses'),
              radio('disq-b2b', 'What-would-you-like-to-do', 'Actually, customers do need to talk to us before buying'),
              err('disq-error')]],
            ['a', { id: 'step-disqualified-next', href: '#', class: 'next-step-btn' }, ['Continue']]]],
          ['div', { id: 'step-disqualified-thanks', class: 'step-dq no-gap' }, ['Thanks']],
          ['div', { id: 'step-2', class: 'step-2 padding-40' }, step2],
          ['div', { id: 'step-3', class: 'step-3 modified' }, [['div', { id: 'rh-embed', style: 'min-height: 600px;' }, []]]],
        ]],
      ]],
    ]],
  ];
}

const PAGES = {
  demo:      { path: '/demo',          markup: 'demo',      live: 'gushwork-form.js' },
  demoNeeds: { path: '/demo',          markup: 'demoNeeds', live: 'gushwork-form.js' },
  aiDemo:    { path: '/ai-demo',       markup: 'crm',       live: 'gushwork-form-popup.js' },
  l2o:       { path: '/lead-to-order', markup: 'crm',       live: 'gushwork-form-popup.js' },
  o2c:       { path: '/order-to-cash', markup: 'crm',       live: 'gushwork-form-popup.js' },
  aeo:       { path: '/aeo',           markup: 'aeo',       live: 'gushwork-form-popup.js' },
  start:     { path: '/start',         markup: 'start',     live: 'gushwork-form-popup.js' },
  noPhone:   { path: '/seo-leads',     markup: 'noPhone',   live: 'gushwork-form-popup.js' },
  twoPhones: { path: '/start',         markup: 'twoPhones', live: 'gushwork-form-popup.js' },
};

/* ════════════════════════════════════════════════════════════════════
   THE NETWORK — what each endpoint answers in each scenario
   ════════════════════════════════════════════════════════════════════ */
function makeRoute(lead) {
  let nonIcpCalls = 0;
  return (url, method, body) => {
    if (url === 'https://api.country.is') return { json: { ip: '203.0.113.9', country: lead.geo || 'US' }, latency: 60 };
    if (url.indexOf('https://dns.google/resolve?') === 0 || url.indexOf('https://cloudflare-dns.com/dns-query?') === 0) {
      const q = new URL(url).searchParams;
      const type = q.get('type');
      const ans = type === 'A' ? [{ name: q.get('name'), type: 1, data: '198.51.100.7' }]
        : type === 'NS' ? [{ name: q.get('name'), type: 2, data: 'ns1.examplehost.net.' }]
        : type === 'MX' ? [{ name: q.get('name'), type: 15, data: '10 mx.examplehost.net.' }] : [];
      return { json: { Status: 0, Answer: ans }, latency: 30 };
    }
    if (url === API + '/session') return { json: { ok: true } };
    if (url === API + '/verify-email') return { json: { valid: true, status: 'valid' }, latency: 120 };
    if (url === API + '/enrich') return { json: lead.enrich || {}, latency: 150 };
    if (url === API + '/non-icp-check') { nonIcpCalls++; return lead.nonIcp ? lead.nonIcp(body, nonIcpCalls) : { json: { blocked: false, pending: false } }; }
    if (url === API + '/partial') return { json: { ok: true } };
    if (url === API + '/verify-website') return { json: { ok: true, reason: 'content_clean', canonical_url: 'https://' + String(body && body.website) + '/' }, latency: 200 };
    if (url === API + '/resolve-website') return { json: { ok: true, reason: 'content_clean' } };
    if (url === API + '/submit') return { json: lead.submitRes || { ok: true }, latency: 180 };
    if (url === API + '/booking-confirmed') return { json: { ok: true } };
    return { status: 404, json: {} };
  };
}

/* ════════════════════════════════════════════════════════════════════
   THE VISITORS
   ════════════════════════════════════════════════════════════════════ */
const ENRICH_ACME = { first_name: 'Ada', last_name: 'Lovelace', company: 'Acme Widgets', website: 'https://acmewidgets.com/', title: 'Head of Growth', company_size: '51-200', industry: 'Manufacturing', linkedin_url: 'https://www.linkedin.com/in/ada-example' };
const LEADS = {
  b2bOrganic:   { email: 'ada@acmewidgets.com', sell: 'sell-b2b', enrich: ENRICH_ACME, phone: '2015550123', book: true },
  b2cOrganic:   { email: 'ada@acmewidgets.com', sell: 'sell-b2c', enrich: ENRICH_ACME, phone: '2015550123', dq: 'b2b', book: true },
  mixedWaitlist:{ email: 'ada@acmewidgets.com', sell: 'sell-mixed', enrich: ENRICH_ACME, dq: 'waitlist' },
  crmCpc:       { email: 'ada@acmewidgets.com', sell: 'sell-b2c', enrich: ENRICH_ACME, phone: '2015550123', book: true,
                  query: '?utm_source=google&utm_medium=cpc&utm_campaign=Gushwork_CRM_Search' },
  aeoCpc:       { email: 'ada@acmewidgets.com', sell: 'sell-b2b', enrich: ENRICH_ACME, phone: '2015550123', book: true,
                  query: '?utm_source=google&utm_medium=cpc&utm_campaign=AEO_Search_Generic' },
  l2oSocial:    { email: 'ada@acmewidgets.com', sell: 'sell-mixed', enrich: ENRICH_ACME, phone: '2015550123', book: true,
                  query: '?utm_source=facebook&utm_medium=paid_social&utm_campaign=FLI_L2O_Prospecting' },
  o2cCookie:    { email: 'ada@acmewidgets.com', sell: 'sell-b2b', enrich: ENRICH_ACME, phone: '2015550123', book: true,
                  cookies: ['gw_utm_campaign=O2C_Retarget', 'gw_utm_medium=email'] },
  testAddress:  { email: 'b@g.ai', sell: 'sell-b2b', fill: { 'first-name': 'Test', 'last-name': 'Run', company: 'Gushwork QA', website: 'gushwork-qa.com' }, book: true },
  freeMail:     { email: 'ada.lovelace@gmail.com', sell: 'sell-b2b', phone: '2015550123', geo: 'IN', book: true,
                  fill: { 'first-name': 'Ada', 'last-name': 'Lovelace', company: 'Lovelace Analytics', website: 'lovelace-analytics.com' } },
  serverBlock:  { email: 'ada@acmewidgets.com', sell: 'sell-b2b', enrich: ENRICH_ACME, phone: '2015550123',
                  submitRes: { ok: true, non_icp_blocked: true, non_icp_reason: 'kw.com', non_icp_industry: 'real_estate' } },
  clientBlock:  { email: 'ada@acmewidgets.com', sell: 'sell-b2b', enrich: ENRICH_ACME, phone: '2015550123',
                  nonIcp: (b) => ({ json: { blocked: true, matched_domain: 'statefarm.com', industry: 'insurance', pending: false } }) },
  calendarHold: { email: 'ada@acmewidgets.com', sell: 'sell-b2b', enrich: ENRICH_ACME, phone: '2015550123',
                  nonIcp: (b, n) => (n < 4 ? { json: { blocked: false, pending: true } } : { json: { blocked: true, matched_domain: 'acmewidgets.com', industry: 'insurance' } }) },
  badPhone:     { email: 'ada@acmewidgets.com', sell: 'sell-b2b', enrich: ENRICH_ACME, phone: '12345', fixPhone: '2015550123', book: true, enterKey: true, back: true },
  rhDown:       { email: 'ada@acmewidgets.com', sell: 'sell-b2b', enrich: ENRICH_ACME, phone: '2015550123', rhFails: true },
};

async function visit(w, lead, page) {
  const g = w.g, doc = w.doc;
  const el = (id) => doc.getElementById(id);
  const shown = (id) => { const e = el(id); return !!e && e.style.display !== 'none'; };
  const settle = (ms) => w.run(ms == null ? 400 : ms);
  async function type(id, text) {
    const e = el(id); if (!e) return;
    w.focusEl(e);
    e.value = text;
    e.dispatchEvent(new w.FakeEvent('input', { bubbles: true }));
    e.dispatchEvent(new w.FakeEvent('keyup', { bubbles: true, key: String(text).slice(-1) }));
    await settle(200);
    w.blurActive();
    await settle();
  }
  async function pick(id) {
    const e = el(id); if (!e) return;
    if (e.type === 'radio') doc.querySelectorAll('input[name="' + e.name + '"]').forEach((r) => { if (r !== e) r.checked = false; });
    e.checked = e.type === 'checkbox' ? !e.checked : true;
    e.dispatchEvent(new w.FakeEvent('click', { bubbles: true, cancelable: true }));
    e.dispatchEvent(new w.FakeEvent('input', { bubbles: true }));
    e.dispatchEvent(new w.FakeEvent('change', { bubbles: true }));
    await settle();
  }
  async function click(id, ms) {
    const e = el(id); if (!e) return;
    e.dispatchEvent(new w.FakeEvent('click', { bubbles: true, cancelable: true }));
    await settle(ms == null ? 1500 : ms);
  }
  async function key(k, target) {
    const t = target || doc.activeElement || doc.body;
    t.dispatchEvent(new w.FakeEvent('keydown', { bubbles: true, cancelable: true, key: k }));
    await settle(1500);
  }

  await type('email', lead.email);
  await pick(lead.sell);
  if (el('needs-wrap') && !el('needs-wrap').classList.contains('is-hidden')) await pick(lead.needs || 'need-crm');
  if (lead.enterKey) { w.focusEl(el('email')); await key('Enter'); }
  else await click('step-1-next');

  if (shown('step-disqualified')) {
    if (lead.dq === 'waitlist') { await pick('disq-waitlist'); return; }
    await pick('disq-b2b');
    await settle(1500);
  }
  if (!shown('step-2')) return;

  for (const id of ['first-name', 'last-name', 'company', 'website']) {
    const want = (lead.fill && lead.fill[id]) || null;
    if (want) await type(id, want);
    else if (el(id) && !el(id).value) await type(id, id === 'website' ? 'acmewidgets.com' : 'Filled');
    else if (id === 'website') { w.focusEl(el(id)); await settle(200); w.blurActive(); await settle(); }
  }
  if (lead.phone && el('phone')) await type('phone', lead.phone);
  const hear = el('hear-about-us');
  if (hear && !hear.value && hear.closest('.field-wrapper').style.display !== 'none') await type('hear-about-us', 'Google search');
  const about = el('about-business');
  if (about) {
    const wrap = el('about-business-wrap');
    if (!wrap || !wrap.classList.contains('is-hidden')) await type('about-business', 'We sell industrial pumps to distributors.');
  }

  await click('step-2-next', 6000);

  if (lead.fixPhone && shown('step-2') && !shown('step-3')) {
    await type('phone', lead.fixPhone);
    await click('step-2-next', 6000);
  }
  if (lead.back && shown('step-3')) {
    w.dispatch(g, Object.assign(new w.FakeEvent('popstate'), { state: { step: 'step-2' } }));
    await settle(1500);
    w.dispatch(doc.getElementById('step-2') || doc, new w.FakeEvent('keydown', { bubbles: true, key: 'Escape' }));
    await settle(500);
  }
  if (lead.book) {
    const ev = new w.FakeEvent('message');
    ev.data = { type: 'MEETING_BOOKED', meeting: { id: 'mtg-777', attributes: { id: 'mtg-777', meeting_time: '2026-10-20T15:00:00Z', email: lead.email } } };
    w.dispatch(g, ev);
    await settle(1000);
    w.dispatch(g, ev);               // RevenueHero can post twice
    await settle(1000);
  }
}

/* ════════════════════════════════════════════════════════════════════
   RUNNING ONE VERSION
   ════════════════════════════════════════════════════════════════════ */
const BASE_LATENCY = 100;
async function runOne(which, file, pageKey, leadKey, mode, extra) {
  extra = extra || {};
  const page = PAGES[pageKey];
  const lead = LEADS[leadKey];
  const compiled = SOURCES[file][which].script;
  const failScripts = extra.failScripts || [];
  /* ALIGNED: NEW's first library arrives when OLD's did, i.e. after the
     round trip OLD spent on jQuery. */
  const latency = (src) => (mode === 'aligned' && which === 'new' && src === URL_ITI ? 2 * BASE_LATENCY : BASE_LATENCY);
  const w = makeWorld({
    path: page.path, query: lead.query || '', markup: formMarkup(page.markup),
    referrer: extra.referrer || 'https://www.google.com/',
    session: Object.assign({ gw_referrer: 'https://www.google.com/', gw_landing_page: 'https://www.gushwork.ai' + page.path }, extra.session || {}),
    cookies: (lead.cookies || []).concat(extra.cookies || []),
    route: makeRoute(lead), latency, failScripts, rhFails: !!lead.rhFails,
    dataLayer: extra.dataLayer, pageJquery: !!extra.pageJquery, readyState: extra.readyState,
  });
  w.loadScript(compiled);
  await w.idle();
  const afterLoad = w.snapshot();
  w.beginUserPhase();
  await visit(w, lead, page);
  await w.idle();
  const g = w.g;
  return {
    banner: SOURCES[file][which].banner,
    trace: w.trace, scripts: w.scripts, afterLoad, final: w.snapshot(),
    session: w.sessionStorage._dump(), local: w.localStorage._dump(), cookies: Object.fromEntries(w.cookieJar),
    dataLayer: g.dataLayer ? JSON.parse(JSON.stringify(g.dataLayer)) : null,
    jquery: g.jQuery ? (g.jQuery.label || 'unknown') : null,
    userStart: w.userStart,
  };
}

/* ════════════════════════════════════════════════════════════════════
   COMPARING
   ════════════════════════════════════════════════════════════════════ */
/* The init banner names its own version, which is the one line that is
   SUPPOSED to differ. It is normalised to a placeholder in both, and the
   NEW banner is checked separately below. Nothing else is rewritten. */
function normalise(r) {
  const re = new RegExp('Form initialised ' + r.banner.replace(/\./g, '\\.') + ' ', 'g');
  return r.trace
    /* The script elements themselves (create, src, insert) and the jQuery
       calls are the intended difference; they are compared on their own. */
    .filter((e) => e.k !== 'script.load' && e.k !== 'script.error' && e.k !== 'jquery.call' && !e.script)
    .map((e) => (e.k === 'console' ? Object.assign({}, e, { text: e.text.replace(re, 'Form initialised vX ') }) : e))
    .map((e) => JSON.stringify(e));
}
function firstDiff(a, b) {
  const n = Math.max(a.length, b.length);
  for (let i = 0; i < n; i++) if (a[i] !== b[i]) return 'first difference at entry ' + i + ' of ' + a.length + '/' + b.length + ':\n        OLD ' + (a[i] || '(none)') + '\n        NEW ' + (b[i] || '(none)');
  return '';
}
function stripTimes(lines) { return lines.map((l) => { const o = JSON.parse(l); delete o.t; return JSON.stringify(o); }); }

const EMAILS_IN_PLAY = ['ada@acmewidgets.com', 'ada.lovelace@gmail.com', 'b@g.ai'];
function consoleHasEmail(r) {
  return r.trace.filter((e) => e.k === 'console').some((e) => EMAILS_IN_PLAY.some((m) => e.text.toLowerCase().indexOf(m) !== -1));
}

async function compareCase(file, pageKey, leadKey, extra) {
  const tag = `${file.replace('gushwork-', '').replace('.js', '')} ${PAGES[pageKey].path}${pageKey === 'demoNeeds' ? '(needs)' : pageKey === 'twoPhones' ? '(two phone fields)' : ''} ${leadKey}${extra && extra.label ? ' ' + extra.label : ''}`;
  const out = {};

  /* ALIGNED: the full recording, timestamps and all. */
  const oa = await runOne('old', file, pageKey, leadKey, 'aligned', extra);
  const na = await runOne('new', file, pageKey, leadKey, 'aligned', extra);
  const A = normalise(oa), B = normalise(na);
  ok(`${tag} [aligned]: every recorded effect identical, in order, at the same times (${A.length} entries)`,
     A.length > 0 && A.join('\n') === B.join('\n'), firstDiff(A, B));
  ok(`${tag} [aligned]: the page after load is identical`, JSON.stringify(oa.afterLoad) === JSON.stringify(na.afterLoad));
  ok(`${tag} [aligned]: the final page is identical`, JSON.stringify(oa.final) === JSON.stringify(na.final));
  ok(`${tag} [aligned]: storage, cookies and dataLayer identical`,
     JSON.stringify([oa.session, oa.local, oa.cookies, oa.dataLayer]) === JSON.stringify([na.session, na.local, na.cookies, na.dataLayer]));

  /* SAME LATENCY: NEW sets the phone up a round trip sooner. */
  const os = await runOne('old', file, pageKey, leadKey, 'same', extra);
  const ns = await runOne('new', file, pageKey, leadKey, 'same', extra);
  const relUser = (r) => normalise(r).map((l) => JSON.parse(l)).filter((e) => e.phase === 'user')
    .map((e) => JSON.stringify(Object.assign({}, e, { t: e.t - r.userStart })));
  const OU = relUser(os), NU = relUser(ns);
  ok(`${tag} [same latency]: everything the visitor does after load is identical, in order (${OU.length} entries)`,
     OU.length > 0 && OU.join('\n') === NU.join('\n'), firstDiff(OU, NU));
  const loadSet = (r) => stripTimes(normalise(r).filter((l) => JSON.parse(l).phase === 'load')).sort();
  const OL = loadSet(os), NL = loadSet(ns);
  ok(`${tag} [same latency]: the page-load effects are the same set (${OL.length})`, OL.join('\n') === NL.join('\n'), firstDiff(OL, NL));
  ok(`${tag} [same latency]: the final page is identical`, JSON.stringify(os.final) === JSON.stringify(ns.final));

  /* What the two versions REQUEST from cdnjs: the one intended difference. */
  ok(`${tag}: OLD requested jQuery, intl-tel-input, utils in that order`,
     JSON.stringify(oa.scripts) === JSON.stringify(failingAware([URL_JQUERY, URL_ITI, URL_UTILS], extra)), JSON.stringify(oa.scripts));
  ok(`${tag}: NEW requests intl-tel-input then utils, and no jQuery`,
     JSON.stringify(na.scripts) === JSON.stringify(failingAware([URL_ITI, URL_UTILS], extra)), JSON.stringify(na.scripts));
  ok(`${tag}: no console line carries the lead's email (OLD and NEW)`, !consoleHasEmail(oa) && !consoleHasEmail(na));
  ok(`${tag}: nothing threw inside the page (OLD and NEW)`,
     !oa.trace.concat(na.trace).some((e) => e.k === 'error'),
     JSON.stringify(oa.trace.concat(na.trace).filter((e) => e.k === 'error').slice(0, 3)));

  out.old = oa; out.new = na; out.tag = tag;
  /* GW_DIFF_DUMP=1 prints what each case actually exercised, so a reader
     can see the visitor got where the case says it did (all fixtures). */
  if (process.env.GW_DIFF_DUMP) {
    const kinds = {};
    na.trace.forEach((e) => { kinds[e.k] = (kinds[e.k] || 0) + 1; });
    const key = na.trace.filter((e) => ['fetch', 'dataLayer', 'rh.new', 'rh.submit', 'rh.open', 'navigate', 'iti.init', 'history.push'].indexOf(e.k) !== -1)
      .map((e) => e.k + (e.url ? ' ' + e.url.replace(API, '').replace(/\?.*/, '') : '') + (e.o && e.o.routerId ? ' router=' + e.o.routerId : '') + (e.href ? ' ' + e.href : '') + (e.s && e.s.step ? ' ' + e.s.step : '') + (e.v && e.v.event ? ' ' + e.v.event : ''));
    console.log('\n  ' + tag + '\n    ' + JSON.stringify(kinds) + '\n    ' + key.join(' | '));
  }
  return out;
}
/* A script that fails stops the chain behind it. */
function failingAware(list, extra) {
  const f = (extra && extra.failScripts) || [];
  const outl = [];
  for (const u of list) { outl.push(u); if (f.some((x) => u.indexOf(x) !== -1)) break; }
  return outl;
}

/* ════════════════════════════════════════════════════════════════════
   WHAT THE NEW FILE MUST DO, stated absolutely -- so the suite also
   fails if OLD and NEW were somehow wrong in the same way.
   ════════════════════════════════════════════════════════════════════ */
function eventsOf(r, k) { return r.trace.filter((e) => e.k === k); }
function submitBody(r) { const f = r.trace.find((e) => e.k === 'fetch' && e.url === API + '/submit'); return f ? f.body : null; }

function expectAbsolute(c, exp) {
  const r = c.new, tag = c.tag;
  const rh = eventsOf(r, 'rh.new');
  if ('router' in exp) {
    ok(`${tag}: RevenueHero router is ${exp.router}`, rh.length >= 1 && rh.every((e) => e.o && e.o.routerId === exp.router), JSON.stringify(rh));
  }
  if ('redirect' in exp) {
    const nav = eventsOf(r, 'navigate').map((e) => e.href);
    ok(`${tag}: redirect is exactly ${exp.redirect}`, JSON.stringify(nav) === JSON.stringify(exp.redirect ? [exp.redirect] : []), JSON.stringify(nav));
  }
  if ('dataLayer' in exp) {
    ok(`${tag}: dataLayer is exactly ${JSON.stringify(exp.dataLayer)}`, JSON.stringify(r.dataLayer) === JSON.stringify(exp.dataLayer), JSON.stringify(r.dataLayer));
  }
  if ('phone' in exp) {
    const b = submitBody(r);
    ok(`${tag}: /submit carries phone ${JSON.stringify(exp.phone)}`, !!b && b.phone === exp.phone, JSON.stringify(b && b.phone));
  }
  if ('submitted' in exp) {
    ok(`${tag}: /submit ${exp.submitted ? 'is' : 'is NOT'} called`, !!submitBody(r) === exp.submitted);
  }
  if ('disqualified' in exp) {
    const partials = r.trace.filter((e) => e.k === 'fetch' && e.url === API + '/partial').map((e) => e.body);
    const last = partials[partials.length - 1];
    ok(`${tag}: last /partial says disqualified=${exp.disqualified}`, !!last && last.disqualified === exp.disqualified, JSON.stringify(last && [last.disqualified, last.disqualified_reason]));
  }
  if ('booked' in exp) {
    const posts = r.trace.filter((e) => e.k === 'fetch' && e.url === API + '/booking-confirmed');
    ok(`${tag}: /booking-confirmed posted ${exp.booked} time(s)`, posts.length === exp.booked, String(posts.length));
  }
  if ('itiInits' in exp) {
    ok(`${tag}: intl-tel-input set up ${exp.itiInits} field(s)`, eventsOf(r, 'iti.init').length === exp.itiInits, String(eventsOf(r, 'iti.init').length));
  }
}

/* ════════════════════════════════════════════════════════════════════
   THE MATRIX
   ════════════════════════════════════════════════════════════════════ */
/* Every booking visitor receives MEETING_BOOKED TWICE (RevenueHero can do
   that). The GTM push is once per page; /booking-confirmed is posted on
   every message, in v5.20.0 as now, hence booked: 2. */
const DL = (e) => [{ event: 'gw_form_submitted', user_data: { email: e } }, { event: 'gw_demo_booked', user_data: { email: e } }];
const ADA = 'ada@acmewidgets.com';

(async () => {
  for (const f of FILES) {
    const s = SOURCES[f];
    ok(`${f}: the reference is ${f === 'gushwork-form.js' ? 'v5.20.0' : 'v5.20.0-ads'} from git`, s.old.banner === (f === 'gushwork-form.js' ? 'v5.20.0' : 'v5.20.0-ads'), s.old.banner);
    ok(`${f}: the working tree is a different, later banner`, !!s.new.banner && s.new.banner !== s.old.banner, s.new.banner);
    /* The banner is the ONE place the version may differ. Its format must not. */
    const fmt = (src) => (/console\.log\('\[GW\] ✅ Form initialised v[^']*'(.*)\);/.exec(src) || [])[1];
    ok(`${f}: the init log line has the same format apart from the version`, !!fmt(s.old.src) && fmt(s.old.src) === fmt(s.new.src));
  }

  /* Every page on its live file, every visitor type that page can see. */
  const cases = [];
  const add = (file, page, lead, exp, extra) => cases.push({ file, page, lead, exp, extra });

  add('gushwork-form.js', 'demo', 'b2bOrganic',  { router: '6138', dataLayer: DL(ADA), phone: '+12015550123', booked: 2, itiInits: 1 });
  add('gushwork-form.js', 'demo', 'b2cOrganic',  { router: '6138', disqualified: false, dataLayer: DL(ADA), booked: 2 });
  add('gushwork-form.js', 'demo', 'mixedWaitlist', { submitted: false, disqualified: true, dataLayer: null });
  add('gushwork-form.js', 'demo', 'crmCpc',      { router: '6804', disqualified: false, dataLayer: DL(ADA) });
  add('gushwork-form.js', 'demo', 'aeoCpc',      { router: '6138' });
  add('gushwork-form.js', 'demo', 'l2oSocial',   { router: '6804', disqualified: false });
  add('gushwork-form.js', 'demo', 'o2cCookie',   { router: '6804' });
  add('gushwork-form.js', 'demo', 'testAddress', { router: '6138', submitted: false, dataLayer: null, booked: 0 });
  add('gushwork-form.js', 'demo', 'freeMail',    { phone: '+912015550123', dataLayer: DL('ada.lovelace@gmail.com') });
  add('gushwork-form.js', 'demo', 'serverBlock', { redirect: '/thank-you?attendeeName=Ada&industry=real_estate', dataLayer: null });
  add('gushwork-form.js', 'demo', 'clientBlock', { redirect: '/thank-you?attendeeName=Ada&industry=insurance', dataLayer: null });
  add('gushwork-form.js', 'demo', 'calendarHold',{ redirect: '/thank-you?attendeeName=Ada&industry=insurance', dataLayer: [{ event: 'gw_form_submitted', user_data: { email: ADA } }] });
  add('gushwork-form.js', 'demo', 'badPhone',    { phone: '+12015550123' });
  add('gushwork-form.js', 'demo', 'rhDown',      {});
  add('gushwork-form.js', 'demoNeeds', 'b2bOrganic', { router: '6804' });
  add('gushwork-form.js', 'demoNeeds', 'b2cOrganic', { router: '6804', disqualified: false });
  add('gushwork-form.js', 'demoNeeds', 'aeoCpc', { router: '6138' });

  for (const p of ['aiDemo', 'l2o', 'o2c']) {
    add('gushwork-form-popup.js', p, 'b2bOrganic', { router: '6804', dataLayer: DL(ADA), phone: '+12015550123', booked: 2, itiInits: 1 });
    add('gushwork-form-popup.js', p, 'b2cOrganic', { router: '6804', disqualified: false });
    add('gushwork-form-popup.js', p, 'mixedWaitlist', { router: '6804' });
  }
  add('gushwork-form-popup.js', 'aiDemo', 'serverBlock', { redirect: '/thank-you?attendeeName=Ada&industry=real_estate' });
  add('gushwork-form-popup.js', 'aiDemo', 'testAddress', { router: '6804', submitted: false, dataLayer: null });
  add('gushwork-form-popup.js', 'aeo', 'b2bOrganic', { router: '6138', dataLayer: DL(ADA), phone: '+12015550123', itiInits: 1 });
  add('gushwork-form-popup.js', 'aeo', 'b2cOrganic', { router: '6138', disqualified: false });
  add('gushwork-form-popup.js', 'aeo', 'mixedWaitlist', { submitted: false, disqualified: true });
  add('gushwork-form-popup.js', 'aeo', 'crmCpc', { router: '6138', disqualified: false });
  add('gushwork-form-popup.js', 'aeo', 'l2oSocial', { router: '6138' });
  add('gushwork-form-popup.js', 'aeo', 'clientBlock', { redirect: '/thank-you?attendeeName=Ada&industry=insurance' });
  add('gushwork-form-popup.js', 'start', 'aeoCpc', { router: '6138', itiInits: 1 });
  add('gushwork-form-popup.js', 'start', 'freeMail', { phone: '+912015550123' });
  add('gushwork-form-popup.js', 'start', 'calendarHold', { redirect: '/thank-you?attendeeName=Ada&industry=insurance' });
  add('gushwork-form-popup.js', 'start', 'badPhone', { phone: '+12015550123' });
  add('gushwork-form-popup.js', 'start', 'testAddress', { submitted: false, dataLayer: null });
  add('gushwork-form-popup.js', 'noPhone', 'b2bOrganic', { router: '6138', phone: '+12015550123', itiInits: 1 });
  add('gushwork-form-popup.js', 'twoPhones', 'b2bOrganic', { itiInits: 2 });
  add('gushwork-form-popup.js', 'start', 'rhDown', {});

  /* Each file on the other's pages too: the fork is meant to be the same
     code, and a difference that only shows on markup a file is not live
     on today is still a difference. */
  add('gushwork-form-popup.js', 'demo', 'b2bOrganic', { router: '6138' });
  add('gushwork-form-popup.js', 'demoNeeds', 'crmCpc', { router: '6804' });
  add('gushwork-form.js', 'aiDemo', 'b2cOrganic', { router: '6804', disqualified: false });
  add('gushwork-form.js', 'start', 'freeMail', { phone: '+912015550123' });
  add('gushwork-form.js', 'twoPhones', 'b2bOrganic', { itiInits: 2 });

  /* Page conditions. */
  add('gushwork-form.js', 'demo', 'b2bOrganic', { dataLayer: [{ event: 'gtm.js' }].concat(DL(ADA)) }, { label: '(GTM dataLayer already on the page)', dataLayer: [{ event: 'gtm.js' }] });
  add('gushwork-form-popup.js', 'start', 'b2bOrganic', {}, { label: '(script ran after the page parsed, as with defer)', readyState: 'interactive' });
  add('gushwork-form-popup.js', 'aeo', 'b2bOrganic', {}, { label: '(returning visitor, campaign in a cookie)', cookies: ['gw_utm_campaign=Gushwork_CRM_Search', 'gw_utm_medium=cpc'] });
  /* A CDN library that never loads. Both versions must fail open the same
     way: the phone field stays a plain input and the raw number is sent. */
  add('gushwork-form.js', 'demo', 'b2bOrganic', { phone: '2015550123', itiInits: 0 }, { label: '(utils.js fails to load)', failScripts: ['utils.js'] });
  add('gushwork-form-popup.js', 'start', 'b2bOrganic', { phone: '2015550123', itiInits: 0 }, { label: '(intl-tel-input fails to load)', failScripts: ['intlTelInput.min.js'] });

  for (const c of cases) {
    const res = await compareCase(c.file, c.page, c.lead, c.extra);
    expectAbsolute(res, c.exp);
  }

  /* ── THE INTENDED DIFFERENCES, asserted as such ──────────────────── */

  /* 1. window.jQuery. A Webflow page already has its own (3.5.1). OLD
        replaced it with cdnjs 3.6.0 once that arrived; NEW leaves it. On
        a page with none (the Astro build), OLD created one and NEW does
        not. */
  for (const f of FILES) {
    const tag = f.replace('gushwork-', '').replace('.js', '');
    const o1 = await runOne('old', f, 'start', 'b2bOrganic', 'same', { pageJquery: true });
    const n1 = await runOne('new', f, 'start', 'b2bOrganic', 'same', { pageJquery: true });
    ok(`${tag} on a Webflow page: OLD left window.jQuery as cdnjs 3.6.0 (it overwrote the page's)`, o1.jquery === 'cdnjs-3.6.0', String(o1.jquery));
    ok(`${tag} on a Webflow page: NEW leaves the page's own jQuery in place`, n1.jquery === 'page-3.5.1', String(n1.jquery));
    ok(`${tag} on a Webflow page: NEW never calls jQuery at all`, !n1.trace.some((e) => e.k === 'jquery.call'));
    ok(`${tag} on a Webflow page: everything else identical`,
       JSON.stringify(normalise(o1).filter((l) => JSON.parse(l).phase === 'user').map((l) => { const x = JSON.parse(l); x.t -= o1.userStart; return JSON.stringify(x); }))
       === JSON.stringify(normalise(n1).filter((l) => JSON.parse(l).phase === 'user').map((l) => { const x = JSON.parse(l); x.t -= n1.userStart; return JSON.stringify(x); }))
       && JSON.stringify(o1.final) === JSON.stringify(n1.final));
    const o2 = await runOne('old', f, 'start', 'b2bOrganic', 'same', {});
    const n2 = await runOne('new', f, 'start', 'b2bOrganic', 'same', {});
    ok(`${tag} on a page with no jQuery: OLD created window.jQuery, NEW does not`, o2.jquery === 'cdnjs-3.6.0' && n2.jquery === null, o2.jquery + ' / ' + n2.jquery);
    ok(`${tag}: OLD's only jQuery calls were the phone lookup and the attribute read`,
       JSON.stringify(eventsOf(o2, 'jquery.call').map((e) => e.sel)) === JSON.stringify(['input[ms-code-phone-number]', '#phone']),
       JSON.stringify(eventsOf(o2, 'jquery.call')));
  }

  /* 2. The jQuery request FAILS. OLD's chain stopped there, so the phone
        field was never set up and /submit sent the raw digits. NEW never
        makes that request, so this failure cannot happen to it: the
        field is set up and /submit sends E.164. A behaviour change, but
        only on a path that is now unreachable, and towards the normal
        one. Pinned so it is a decision on record, not an accident. */
  for (const f of FILES) {
    const tag = f.replace('gushwork-', '').replace('.js', '');
    const o = await runOne('old', f, 'start', 'b2bOrganic', 'same', { failScripts: ['jquery'] });
    const n = await runOne('new', f, 'start', 'b2bOrganic', 'same', { failScripts: ['jquery'] });
    ok(`${tag} when cdnjs jQuery fails: OLD stopped there and never set the phone field up`,
       JSON.stringify(o.scripts) === JSON.stringify([URL_JQUERY]) && eventsOf(o, 'iti.init').length === 0);
    ok(`${tag} when cdnjs jQuery fails: OLD sent the raw typed digits`, (submitBody(o) || {}).phone === '2015550123', JSON.stringify(submitBody(o) && submitBody(o).phone));
    ok(`${tag} when cdnjs jQuery fails: NEW is unaffected, sets the field up and sends E.164`,
       eventsOf(n, 'iti.init').length === 1 && (submitBody(n) || {}).phone === '+12015550123', JSON.stringify(submitBody(n) && submitBody(n).phone));
    /* Everything except the phone handling is still the same. */
    const strip = (r) => { const b = Object.assign({}, submitBody(r)); delete b.phone; return JSON.stringify(b); };
    ok(`${tag} when cdnjs jQuery fails: the rest of /submit is identical`, strip(o) === strip(n));
  }

  console.log('');
  if (failures.length) {
    console.log('  FAILURES:');
    for (const f of failures) console.log('   ✗ ' + f);
  }
  console.log(`  passed: ${pass}`);
  console.log(`  failed: ${fail}`);
  console.log('');
  process.exit(fail ? 1 : 0);
})();
