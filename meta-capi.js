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
/* ── PREDICTED LTV, IN CONFIG ────────────────────────────────────────
   ALL THREE ARE PROVISIONAL AND ALL THREE ARE ENV-SETTABLE, so the day
   the agency or Swapnil comes back with real numbers it is a Railway
   variable change, not a deploy.

   These are what Meta bids against under value optimisation. Nothing
   optimises on value today -- every active ad set is
   OFFSITE_CONVERSIONS, which is count-based, verified against the
   Marketing API on 15 Sept 2026 -- so today these are recorded and
   reported and change no delivery. That will stop being true the moment
   somebody switches a campaign to value, which is why the numbers are
   worth getting right before anyone does.

   COMBINED IS 15000, NOT 17000. predicted_ltv is a prediction of what
   this PERSON is worth, not the sum of a price list. 17000 would assert
   they buy both products at full price with certainty; 15000 carries
   the genuine upside of wanting both without claiming that. */
const LTV_DEFAULTS = { aeo: 12000, crm: 5000, 'aeo,crm': 15000 };

/* A misspelt or empty env var must not become NaN in a payload Meta
   parses -- it would either reject the event or, worse, accept it and
   learn from nothing. Anything unreadable falls back to the default and
   says so once. */
const _ltvWarned = new Set();
function ltvFromEnv(key, envName) {
  const raw = process.env[envName];
  if (raw === undefined || raw === '') return LTV_DEFAULTS[key];
  const n = Number(raw);
  if (!Number.isFinite(n) || n < 0) {
    if (!_ltvWarned.has(envName)) {
      _ltvWarned.add(envName);
      console.warn(`[Meta CAPI] ${envName}="${raw}" is not a usable number — ` +
                   `falling back to ${LTV_DEFAULTS[key]} for ${key}.`);
    }
    return LTV_DEFAULTS[key];
  }
  return n;
}

const PREDICTED_LTV = {
  aeo:       ltvFromEnv('aeo',     'META_LTV_AEO'),
  crm:       ltvFromEnv('crm',     'META_LTV_CRM'),
  'aeo,crm': ltvFromEnv('aeo,crm', 'META_LTV_AEO_CRM'),
};

function predictedLtvFor(slug) {
  return Object.prototype.hasOwnProperty.call(PREDICTED_LTV, slug) ? PREDICTED_LTV[slug] : null;
}

/* ── THE EVENT CATALOGUE ─────────────────────────────────────────────
   KEYED BY THE EVENT SLUG, which is not the same as the routing slug.
   'aeo,crm' is a real key here and never a value of leads.product:
   routing has to pick one calendar, reporting does not have to pick one
   product.

   content_ids is an ARRAY by design -- it is how Meta expresses a
   conversion covering more than one thing -- so a both-ticked lead is
   ONE event carrying both ids rather than two events. Two events would
   count one person as two conversions: deduplication is documented as
   cross-source only ("Does not deduplicate events when only using one
   event source"), and every active ad set optimises on conversion
   COUNT, so a duplicate would corrupt exactly what they bid on. */
const PRODUCTS = {
  aeo:       { content_ids: ['aeo'] },
  crm:       { content_ids: ['crm'] },
  'aeo,crm': { content_ids: ['aeo', 'crm'] },
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

/* ── WHAT THEY TICKED ────────────────────────────────────────────────
   The slugs are the SAME strings as PRODUCTS' keys, deliberately: the
   checkbox value, leads.product_interest, the Salesforce Product__c
   picklist value and the Meta content_ids are all one vocabulary, so
   there is no mapping table anywhere to drift.

   CANONICAL means lowercase, known, deduped and SORTED. Sorting is what
   makes 'aeo,crm' a single comparable value instead of two spellings of
   one answer -- the Salesforce picklist is restricted and would reject
   'crm,aeo' outright, losing the whole record.

   Unknown values are DROPPED rather than stored. The input is a form
   field: anything that is not a product we sell is not a product they
   asked for. */
/* THE TICKABLE PRODUCTS, which is deliberately NOT Object.keys(PRODUCTS)
   any more: the catalogue has a combined key ('aeo,crm') that is an
   OUTPUT of ticking both, never something you can tick. Deriving one
   from the other would let 'aeo,crm' through as a single checkbox
   value and straight into the restricted Salesforce picklist as a
   duplicate. */
const PRODUCT_INTEREST_SLUGS = ['aeo', 'crm'];

function canonicalProductInterest(raw) {
  if (raw == null) return null;
  const parts = (Array.isArray(raw) ? raw : String(raw).split(','))
    .map((x) => String(x).trim().toLowerCase())
    .filter((x) => PRODUCT_INTEREST_SLUGS.includes(x));
  const uniq = [...new Set(parts)].sort();
  return uniq.length ? uniq.join(',') : null;
}

/* ── WHICH OFFER THE VISITOR IS SHOWN ────────────────────────────────
   Until 17 Sept 2026 the offer was a pure function of the pathname, so
   somebody who clicked a CRM ad and arrived anywhere except /ai-demo was
   sold AEO, and there was nothing in the URL to say otherwise. Swapnil
   asked for the campaign to decide instead.

   THE MEDIUM GATE IS NOT DECORATION. "Any other campaign means AEO"
   sounds exhaustive and is not: utm_campaign is also carried by traffic
   that never saw an ad -- LinkedIn social posts (gushwork, nb), the
   lead-estimator drip (email/drip), email-signature links
   (ai-agents-cta), backlinks from other people's sites
   (footer-backlink, whose utm_source is the referring domain) and a
   Trustpilot profile click. Measured over 90 days that is 29 leads who
   declared no product intent at all and would have lost the question.
   Only paid/cpc gets an offer decided for it.

   AN UNREADABLE CAMPAIGN IS NOT AN AEO CAMPAIGN. 17 leads arrive with a
   bare Meta campaign ID (120241181781830373) and 6 with an unrendered
   {{campaign.name}} macro. Reading "no crm in this string" off a number
   and forcing AEO would silently hide the CRM offer from people who
   clicked a CRM ad. Same rule the website checkers follow: "we could not
   tell" is its own answer, and here it means ask.

   BRAND SEARCH IS NOT A PRODUCT SIGNAL. Somebody googling our name has
   told us they want us, not which thing they want.

   MATCHED ON "brand", NEVER ON "br". Every campaign we have that
   contains br already contains brand, so the short token earns nothing
   today -- and it is the trap that just bit Source_Bucket__c in
   Salesforce, where CONTAINS(..,"li") routed "client", "link" and the
   name "Jolian" to LinkedIn. The first campaign named
   Prospecting__Broad__CBO would silently have become a selector
   campaign with nothing anywhere to say so.

   ORDER IS crm BEFORE brand, matching the table in the request: a
   CRM-offer campaign running on brand search is a CRM campaign.

   THIS IS A THIRD COPY OF A RULE THAT MUST STAY IN SYNC with
   gushwork-form.js and gushwork-form-popup.js, the same shape as
   B2C_ALLOWED_PATHS. tests/test-batch2.js lifts all three and asserts
   they agree, because a list this shape has drifted three times here. */
const OFFER_CRM      = 'crm';
const OFFER_AEO      = 'aeo';
const OFFER_SELECTOR = 'selector';
const OFFER_AD_MEDIUMS = ['paid', 'cpc'];

function campaignOffer(utm_campaign, utm_medium) {
  const m = String(utm_medium == null ? '' : utm_medium).trim().toLowerCase();
  if (!OFFER_AD_MEDIUMS.includes(m)) return OFFER_SELECTOR;
  const c = String(utm_campaign == null ? '' : utm_campaign).trim().toLowerCase();
  /* Empty, a bare id, or a template macro the ad platform never filled in. */
  if (!c || /^[0-9]+$/.test(c) || c.includes('{{') || c.includes('}}')) return OFFER_SELECTOR;
  if (c.includes('crm'))   return OFFER_CRM;
  if (c.includes('brand')) return OFFER_SELECTOR;
  return OFFER_AEO;
}

/* EXACT pathname match, never a prefix or substring test — '/ai-demo'
   contains 'demo', and a looser match would put CRM leads on the AEO
   product or the other way round.

   WHAT THEY TICKED OUTRANKS THE PAGE THEY TICKED IT ON, which is the
   entire point of the question: somebody who saw a CRM ad, did not
   click, searched and landed on /demo is a CRM lead on an AEO page, and
   the page is the thing that was wrong about them.

   CRM WINS A BOTH-TICKED SELECTION. This is the ROUTING slug -- one
   calendar, one Salesforce picklist value, one Meta event -- and it
   agrees with the booking router by construction. The fuller answer
   lives in leads.product_interest, which keeps both.

   Selection is ignored where it is absent, so /ai-demo, the ad landers
   and every historical lead resolve exactly as they did before. */
function resolveProduct({ page_url, product_interest, utm_campaign, utm_medium } = {}) {
  const ticked = canonicalProductInterest(product_interest);
  if (ticked) return ticked.includes('crm') ? 'crm' : 'aeo';
  /* THE CAMPAIGN RANKS BELOW THE PAGE'S OWN MAPPING AND ABOVE THE
     DEFAULT, and that middle position is the whole design.

     Above the default: a CRM ad that lands anywhere but /ai-demo is why
     this exists at all, and the default would call that lead aeo.

     Below an explicit mapping: /ai-demo SELLS the CRM product. A visitor
     who arrives there on an AEO prospecting campaign is on the CRM page,
     and letting the campaign win would demote a real /ai-demo lead to
     aeo -- routing them to the wrong calendar, on the one page in the
     catalogue that has never been ambiguous.

     Only the crm answer is consulted. The aeo answer IS the default, so
     reading it here would change nothing and would quietly move the
     unreadable-page case (null) onto a product. */
  if (campaignCrmApplies(page_url, utm_campaign, utm_medium)) return 'crm';
  return resolveProductFromPage({ page_url });
}

/* ── THE EVENT SLUG, WHICH IS NOT THE ROUTING SLUG ───────────────────
   resolveProduct answers "which calendar, which Salesforce picklist
   value" and must be single -- it returns crm for a both-ticked lead.
   This answers "what did we tell Meta this conversion was", and a
   both-ticked lead is honestly BOTH.

   SAME RULE SALESFORCE ALREADY HAS: what they ticked, falling back to
   the page when nothing was. So the Meta event matches
   product_interest, never leads.product, and a test pins that -- it is
   the invariant that would otherwise drift silently, because the two
   agree on every lead except the both-ticked one. */
function resolveEventProduct({ page_url, product_interest, utm_campaign, utm_medium } = {}) {
  const ticked = canonicalProductInterest(product_interest);
  if (ticked) return ticked;
  /* Same precedence as resolveProduct, for the same reasons -- the two
     must not disagree about a lead nobody ticked anything for. */
  if (campaignCrmApplies(page_url, utm_campaign, utm_medium)) return 'crm';
  return resolveProductFromPage({ page_url });
}

/* Does the campaign get to decide this lead? THREE conditions, kept in
   one place because resolveProduct and resolveEventProduct both ask and
   a copy of this test would be a copy that drifts.

   1. THE PAGE MUST BE READABLE. An unreadable page_url returns null from
      resolveProductFromPage on purpose -- "we could not tell which page
      this was" is not "this was the default page" -- and letting a
      campaign rescue it would quietly convert that honest null into a
      product. The campaign tells us what they clicked, not where they
      landed, and the untagged event is the correct report for a lead we
      cannot place.
   2. THE PAGE MUST NOT SELL A NAMED PRODUCT. /ai-demo is the CRM page;
      an AEO campaign arriving there is on the CRM page.
   3. Only crm. The aeo answer is already the default. */
function campaignCrmApplies(page_url, utm_campaign, utm_medium) {
  const normalised = normalisePagePath(page_url);
  if (!normalised) return false;
  if (PRODUCT_PATHS[normalised]) return false;
  return campaignOffer(utm_campaign, utm_medium) === OFFER_CRM;
}

/* The page's OWN entry, with no default and no logging: "does this
   pathname sell a named product", as opposed to "what should we tag
   this event". Split out so the campaign can be ranked between the two
   without resolveProductFromPage's default swallowing the question. */
function explicitProductForPage(page_url) {
  const normalised = normalisePagePath(page_url);
  return normalised ? (PRODUCT_PATHS[normalised] || null) : null;
}

function resolveProductFromPage({ page_url } = {}) {
  const normalised = normalisePagePath(page_url);
  if (!normalised) return null;
  const mapped = PRODUCT_PATHS[normalised];
  if (mapped) return mapped;
  noteDefaultedPath(normalised);
  return DEFAULT_PRODUCT;
}

/* The pathname, lowercased and stripped of a trailing slash, or null
   when the value is not a page at all. Lifted out of
   resolveProductFromPage unchanged so explicitProductForPage cannot
   normalise a second way -- one normaliser, the same rule CLAUDE.md
   applies to partnerStackCustomerKey. */
function normalisePagePath(page_url) {
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
  return pathname.toLowerCase().replace(/\/+$/, '') || '/';
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
    /* THE SELECTION IS PASSED IN, NOT RE-DERIVED FROM THE PAGE, and it
       resolves to the EVENT slug rather than the routing one. This read
       page_url alone until 15 Sept 2026, which was safe only while
       product was a pure function of the page; a /demo lead ticking
       AI-CRM would have stored 'crm' and fired 'aeo' with nothing
       anywhere to reconcile them. */
    : resolveEventProduct({
        page_url:         payload.page_url,
        product_interest: payload.product_interest,
        /* THE CAMPAIGN HAS TO REACH HERE OR THE EVENT DISAGREES WITH THE
           COLUMN. index.js resolves leads.product with these two; if the
           event did not get them, a CRM-campaign lead on /demo would be
           stored crm and reported to Meta as aeo, with nothing anywhere
           to reconcile the two. Same failure the ticked-selection fix
           closed on 15 Sept, one input later. */
        utm_campaign:     payload.utm_campaign,
        utm_medium:       payload.utm_medium,
      });
  if (slug) {
    eventData.custom_data.content_ids   = [...PRODUCTS[slug].content_ids];
    eventData.custom_data.content_type  = 'product';
    eventData.custom_data.value         = 0;
    eventData.custom_data.currency      = 'USD';
    /* From config, so the number is a Railway change rather than a
       deploy. Never written when it cannot be read as a number. */
    const ltv = predictedLtvFor(slug);
    if (ltv != null) eventData.custom_data.predicted_ltv = ltv;
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
  PRODUCT_INTEREST_SLUGS,
  canonicalProductInterest,
  resolveEventProduct,
  campaignOffer,
  OFFER_CRM,
  OFFER_AEO,
  OFFER_SELECTOR,
  OFFER_AD_MEDIUMS,
  predictedLtvFor,
  PREDICTED_LTV,
  DEFAULT_PRODUCT,
  PRODUCT_EXCLUDED_EVENTS,
  resolveProduct,
  buildEventData,
};
