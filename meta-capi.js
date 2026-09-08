// ============================================================
// meta-capi.js — Meta Conversions API
// Active event types, all fired from pushFormEventsToMeta():
//   StartTrial   — B2B lead entered email on Step 1 (called from /partial)
//   Lead         — form completed (called from /submit)
//   Schedule     — demo booked (called from /booking-confirmed-webhook)
//
// UPDATED: phone (ph) is now normalized to digits-only before
// hashing, per Meta's user_data spec — country code included,
// no '+', spaces, dashes, or parentheses. Handles any inbound
// format: E.164 (+916388639290), spaced (+91 63886 39290),
// or legacy DB rows. Without this, hashes never match Meta's
// user graph and ph contributes nothing to match quality.
// ============================================================

const crypto = require('crypto');

function sha256(value) {
  if (!value) return undefined;
  return crypto
    .createHash('sha256')
    .update(value.trim().toLowerCase())
    .digest('hex');
}

/**
 * Normalize phone to Meta's required format before hashing:
 * digits only, including country code — no +, spaces, or symbols.
 * "+91 63886 39290" → "916388639290"
 * "+916388639290"   → "916388639290"
 * Returns undefined for empty/garbage values so the key gets cleaned.
 */
function normalizePhone(value) {
  if (!value) return undefined;
  const digits = value.toString().replace(/\D/g, '');
  return digits.length > 0 ? digits : undefined;
}

/* ── Which product is this form selling? ──────────────────────────
   Meta needs to tell two products apart on the SAME pixel: AEO, the
   original business, and CRM, sold by the form on /ai-demo — a Webflow
   duplicate of /demo posting the same fields.

   AEO IS THE DEFAULT, and the exceptions are listed instead. That is
   deliberate and it is the opposite of how this repo usually works.

   An AEO allowlist would rot. The form is already live on a dozen pages
   — /demo, /start, /pricing, /consulting-lead-generation and the rest of
   the SEO landers — and new landers get added routinely by people who
   will never see this file. Measured on 90 days of real leads: a
   /demo-only list tagged 82% of them and left 631 leads, 408 of them
   completed, sending unlabelled events. Every new lander would silently
   join that pile.

   A default fails in the other direction, and that failure is rare and
   deliberate: it only happens when a genuinely new product launches, and
   whoever launches it is already editing things. To make even that
   visible rather than silent, an unmapped path is LOGGED once per path —
   see noteDefaultedPath.

   Resolved from page_url's PATHNAME and nothing else. Deliberately NOT
   from landing_page: real ad traffic lands on /start and submits on
   /demo, so landing_page describes a different page from the one the
   form is on.

   A page_url we cannot read at all is a different thing from a page we
   have not mapped, and returns null rather than the default. "We could
   not tell which page this was" is not "this was the default page" —
   the same rule the lead-path checkers follow. Never throws: this runs
   inside the lead path.

   predicted_ltv is PROVISIONAL (set 8 Sept 2026) and is the same on every
   event for a product — only value varies, and value is 0 on all three
   upstream events. Changing a number here changes how Meta's algorithm
   weights these conversions, so it is a business decision, not a
   tidy-up. */
const PRODUCTS = {
  aeo: { content_ids: ['aeo'], predicted_ltv: 12000 },
  crm: { content_ids: ['crm'], predicted_ltv: 5000  },
};

/* The exceptions. Everything not listed here is DEFAULT_PRODUCT. */
const PRODUCT_PATHS = {
  '/ai-demo': 'crm',
};
const DEFAULT_PRODUCT = 'aeo';

/* Events that never carry a product, by EVENT NAME rather than by hoping
   their page resolves to null. Contact is a lead-magnet PDF download, not
   a product signup: it must not carry a 12000 LTV, and with a default in
   place its landing page would otherwise resolve to aeo. */
const PRODUCT_EXCLUDED_EVENTS = ['Contact'];

/* A path that fell through to the default is logged ONCE, so a new product
   page that should have been mapped surfaces in the logs instead of
   quietly becoming aeo. Bounded, because a log line per distinct path is
   only cheap while the set of paths is small. */
const _defaultedPaths = new Set();
function noteDefaultedPath(pathname) {
  if (_defaultedPaths.has(pathname)) return;
  if (_defaultedPaths.size >= 200) return;
  _defaultedPaths.add(pathname);
  console.log(
    `[Meta CAPI] Product: "${pathname}" is not in PRODUCT_PATHS — tagging as ` +
    `${DEFAULT_PRODUCT} (the default). If this page sells a different product, ` +
    `add it to PRODUCT_PATHS in meta-capi.js.`
  );
}

/* EXACT pathname match, never a prefix or substring test — '/ai-demo'
   contains 'demo', and a looser match would put CRM leads on the AEO
   product or the other way round. */
function resolveProduct({ page_url } = {}) {
  if (!page_url || typeof page_url !== 'string') return null;
  let pathname;
  try {
    pathname = new URL(page_url).pathname;
  } catch {
    /* Not absolute. Accept it as a path ONLY if it actually looks like one,
       so a bare '/ai-demo' still resolves.

       The leading-slash test is load-bearing now that aeo is the default.
       new URL(x, base) succeeds for almost any string — 'not a url' becomes
       /not%20a%20url and 12345 becomes /12345 — so without it every piece of
       garbage would resolve to a real product and be reported as one. A
       value we cannot read is not a page. */
    if (!page_url.startsWith('/')) return null;
    try {
      pathname = new URL(page_url, 'https://placeholder.invalid').pathname;
    } catch {
      return null;
    }
  }
  // Trailing slash is the same page: /ai-demo and /ai-demo/ must not disagree.
  const normalised = pathname.toLowerCase().replace(/\/+$/, '') || '/';
  const mapped = PRODUCT_PATHS[normalised];
  if (mapped) return mapped;
  noteDefaultedPath(normalised);
  return DEFAULT_PRODUCT;
}

/* Reports each event's outcome to whoever wired one up — index.js uses it to
   call recordSuccess('Meta CAPI'), which nothing ever called before, so the
   failure streak never reset on a good send.

   Injected rather than imported: meta-capi.js must not depend on index.js,
   and a module that reaches back into the app is how a require cycle starts.
   Never throws into the send path. */
let _outcomeReporter = null;
function setMetaOutcomeReporter(fn) {
  _outcomeReporter = typeof fn === 'function' ? fn : null;
}
function reportOutcome(outcome) {
  if (!_outcomeReporter) return;
  try { _outcomeReporter(outcome); } catch { /* reporting must never break a send */ }
}

const FIXED_EVENT_ID_EVENTS = ['StartTrial', 'Contact'];

/* The event payload, built and cleaned but NOT sent.

   Split out of sendEvent so the payload can be asserted directly instead
   of through a network stub — and so "what Contact sends" can be compared
   byte for byte against what it sent before product tagging existed. */
function buildEventData(eventName, payload, options = {}) {
  // StartTrial uses fixed event_id per session (Meta deduplicates repeated partials)
  // Other events use random suffix to stay unique
  const eventId = FIXED_EVENT_ID_EVENTS.includes(eventName)
    ? `${eventName}_${payload.session_id || Date.now()}`
    : `${eventName}_${payload.session_id || Date.now()}_${Math.random().toString(36).slice(2, 7)}`;

  const eventData = {
    event_name: eventName,
    event_time: Math.floor(Date.now() / 1000),
    event_id: eventId,
    event_source_url: payload.page_url || payload.landing_page || '',
    action_source: 'website',

    user_data: {
      em: sha256(payload.email),
      ph: sha256(normalizePhone(payload.phone)),
      fn: sha256(payload.first_name),
      ln: sha256(payload.last_name),
      ct: sha256(payload.enriched_city),
      st: sha256(payload.enriched_state),
      country: sha256(payload.enriched_country),
      fbc: payload.fbc || undefined,
      fbp: payload.fbp || undefined,
      client_ip_address: options.clientIpAddress || undefined,
      client_user_agent: options.clientUserAgent || undefined,
    },

    custom_data: {
      company_name: payload.company || undefined,
      company_size: payload.enriched_company_size || undefined,
      industry: payload.enriched_industry || undefined,
      seniority: payload.enriched_seniority || undefined,
      funding_stage: payload.enriched_funding_stage || undefined,
      sell_to: payload.sell_to || undefined,
      industry_category:  payload.industry_category  || undefined,
      product_or_service: payload.product_or_service || undefined,
      is_free_email:      payload.is_free_email === undefined ? undefined : String(payload.is_free_email),
    },
  };

  // Clean undefined values
  Object.keys(eventData.user_data).forEach((key) => {
    if (eventData.user_data[key] === undefined) delete eventData.user_data[key];
  });
  Object.keys(eventData.custom_data).forEach((key) => {
    if (eventData.custom_data[key] === undefined) delete eventData.custom_data[key];
  });

  /* Product tagging, added LAST.

     Two ways an event stays untagged, and they are different things:

       - the EVENT is excluded by name. Contact is a lead-magnet download,
         not a product signup, and with a default in place its page would
         otherwise resolve to aeo and hand it a 12000 LTV.
       - the PAGE could not be read at all, so resolveProduct returned
         null rather than guessing. An unmapped page is not this case:
         it takes the default and is logged.

     An untagged event carries NONE of the five keys — byte for byte the
     event that shipped before this existed. Not an empty content_ids and
     not a bare value of 0: Meta reads either as a real signal about a
     real product, which is worse than saying nothing.

     value is 0 on all three upstream events by spec; the per-product
     number lives in predicted_ltv and is the same on every event.

     content_ids is copied, not shared: handing out the catalogue's own
     array would let one caller mutate every future event. */
  const slug = PRODUCT_EXCLUDED_EVENTS.includes(eventName)
    ? null
    : resolveProduct({ page_url: payload.page_url });
  if (slug) {
    eventData.custom_data.content_ids   = [...PRODUCTS[slug].content_ids];
    eventData.custom_data.content_type  = 'product';
    eventData.custom_data.value         = 0;
    eventData.custom_data.currency      = 'USD';
    eventData.custom_data.predicted_ltv = PRODUCTS[slug].predicted_ltv;
  }

  return eventData;
}

/**
 * Send a single event to Meta CAPI
 */
async function sendEvent(eventName, payload, options = {}) {
  const pixelId = process.env.META_PIXEL_ID;
  const accessToken = process.env.META_ACCESS_TOKEN;

  if (!pixelId || !accessToken) {
    console.warn('[Meta CAPI] Missing credentials, skipping');
    return { success: false, error: 'Missing credentials' };
  }

  const eventData = buildEventData(eventName, payload, options);

  const url = `https://graph.facebook.com/v21.0/${pixelId}/events?access_token=${accessToken}`;

  /* test_event_code is a TOP-LEVEL field on the request, a sibling of data —
     not a property of the event. Putting it inside the event object is the
     usual mistake and Meta ignores it silently there.

     Off unless META_TEST_EVENT_CODE is set, and the body is byte-identical
     when it is unset: the key is added, never assigned undefined, because
     JSON.stringify drops an undefined value but the object identity check in
     the tests would not notice a subtler change.

     An event carrying this code is routed to the Test Events tab and is NOT
     used for optimisation or attribution, so it cannot pollute the pixel.
     Leaving the variable set in production would therefore send every real
     conversion to the test tab instead of to the ad algorithm — set it for a
     verification run and unset it afterwards. */
  const body = { data: [eventData] };
  const testEventCode = process.env.META_TEST_EVENT_CODE;
  if (testEventCode) body.test_event_code = testEventCode;

  const res = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });

  const result = await res.json();

  if (!res.ok || result.error) {
    console.error(`[Meta CAPI] [${eventName}] error:`, JSON.stringify(result));
    return { success: false, error: result };
  }

  console.log(`[Meta CAPI] ✅ [${eventName}] sent: ${result.events_received} events received`);
  reportOutcome({ ok: true, eventName });
  return { success: true, eventName, eventsReceived: result.events_received };
}

/* Turn "some events did not reach Meta" into a rejection.

   Every push function below ends in Promise.allSettled, which NEVER
   rejects. So for the life of this file the .catch(...) at each call site
   in index.js — the one calling recordFailure('Meta CAPI', ...) — could
   not fire. Five call sites, all of them looking like alerting, none of
   them alerting. 'Meta CAPI' has always had a FAILURE_MONITORS entry, so
   unlike the PartnerStack case the table was never the problem; the
   promise shape was.

   Proven by execution rather than by reading: with global.fetch stubbed
   to throw, pushFormEventsToMeta RESOLVED with status 'rejected' inside
   the results array, and the call-site handler never ran.

   A failure has TWO shapes and both have to arrive, because reporting
   only the first would leave the second exactly as silent as before:

     - the promise REJECTED — network, DNS, or a non-JSON body from
       res.json();
     - it RESOLVED with success:false — Meta answered 4xx/5xx, or the
       pixel is not configured at all. This one printed at console.log
       level in a line that reads like a success.

   Thrown AFTER the per-event logging below, so the log still names each
   event before the handler sees one combined error. Every call site is
   fire-and-forget with a .catch already attached, so this can only reach
   a handler that is already there — nothing new can become an unhandled
   rejection. */
function throwIfAnyFailed(eventNames, results) {
  const failures = results
    .map((r, i) => {
      if (r.status === 'rejected') {
        return `${eventNames[i]}: ${(r.reason && r.reason.message) || r.reason}`;
      }
      if (r.value && r.value.success === false) {
        const e = r.value.error;
        return `${eventNames[i]}: ${typeof e === 'string' ? e : JSON.stringify(e)}`;
      }
      return null;
    })
    .filter(Boolean);
  if (failures.length) throw new Error(failures.join('; '));
}

/**
 * Determine which events to fire based on the payload and send them all.
 *
 * Logic:
 *   - no booking_uid       → Lead (initial form submit)
 *   - booking_uid present  → Schedule (demo booked)
 *   - sell_to includes B2B → StartTrial, fires IN ADDITION to Lead or Schedule
 */
async function pushFormEventsToMeta(payload, options = {}) {
  const events = [];

  if (payload.booking_uid) {
    events.push('Schedule');
  } else {
    events.push('Lead');
  }

  const results = await Promise.allSettled(
    events.map((eventName) => sendEvent(eventName, payload, options))
  );

  results.forEach((r, i) => {
    if (r.status === 'fulfilled') {
      console.log(`[Meta CAPI] [${events[i]}]:`, r.value);
    } else {
      console.error(`[Meta CAPI] [${events[i]}] failed:`, r.reason);
    }
  });

  throwIfAnyFailed(events, results);
  return results;
}

/**
 * Send StartTrial event for B2B leads on Step 1 partial.
 * Only fires for B2B / clarified B2B leads.
 * Uses fixed event_id per session so Meta deduplicates repeated partials.
 */
async function pushStartTrialToMeta(payload, options = {}) {
  // Only fire for B2B leads
  if (!payload.sell_to || !payload.sell_to.toUpperCase().includes('B2B')) {
    return [];
  }

  const results = await Promise.allSettled([
    sendEvent('StartTrial', payload, options)
  ]);

  results.forEach((r, i) => {
    if (r.status === 'fulfilled') {
      console.log('[Meta CAPI] [StartTrial]:', r.value);
    } else {
      console.error('[Meta CAPI] [StartTrial] failed:', r.reason);
    }
  });

  throwIfAnyFailed(['StartTrial'], results);
  return results;
}

async function pushContactToMeta(payload, options = {}) {
  const results = await Promise.allSettled([sendEvent('Contact', payload, options)]);
  results.forEach((r) => {
    if (r.status === 'fulfilled') console.log('[Meta CAPI] [Contact]:', r.value);
    else console.error('[Meta CAPI] [Contact] failed:', r.reason);
  });
  throwIfAnyFailed(['Contact'], results);
  return results;
}

module.exports = {
  pushFormEventsToMeta,
  setMetaOutcomeReporter,
  pushStartTrialToMeta,
  pushContactToMeta,
  /* Exported for index.js, which resolves the same slug to persist it on the
     lead, and for the tests, which assert the payload without a network. One
     catalogue, one resolver — so the column and the event cannot disagree
     about which product a lead came from. */
  PRODUCTS,
  PRODUCT_PATHS,
  DEFAULT_PRODUCT,
  PRODUCT_EXCLUDED_EVENTS,
  resolveProduct,
  buildEventData,
};
