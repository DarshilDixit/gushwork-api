// ============================================================
// partnerstack.js — the S2S conversion call
// ------------------------------------------------------------
// One job: tell PartnerStack that a partner-referred visitor became a
// signup, so the affiliate gets credited.
//
// POST https://partnerlinks.io/conversion/xid
//   Authorization: Bearer <PARTNERSTACK_TRACKING_TOKEN>
//   { xid, customer_key, email, name, ip_address, user_agent, origin, meta }
//
// `name` is the CONTACT's name, not the company. It is what titles the
// record in PartnerStack, and whoever approves payouts opens that record —
// sending the company there made every customer read as the company with
// Company Name / Website / Phone all "Not Available".
//
// There is no company or website parameter on this endpoint. They go in
// `meta`, whose keys must match custom CUSTOMER fields configured in
// PartnerStack Settings first; keys with no matching field are dropped
// silently, which looks exactly like the integration working.
//
// xid and customer_key are required; the rest are optional. ip_address,
// user_agent and origin feed PartnerStack's fraud detection, so they are
// sent when we have them and OMITTED when we do not — an empty string is
// worse than an absent field, because it looks like a real value that
// failed to match.
//
// None of the three lives on the lead row: `leads` stores no IP and no
// user agent (form_sessions stores the UA, keyed by session). They come
// off the /submit request itself, which is the same source the Meta CAPI
// call at that site already uses.
//
// NOT POST /v2/customers. That endpoint cannot attach a click, so the
// customer is created with no partner against it and the attribution is
// silently lost — the affiliate sees nothing and there is no error to
// tell you why. The two endpoints look interchangeable and are not.
//
// Note the auth split, which is easy to get wrong because both live in
// the same env:
//   - this endpoint takes the TRACKING TOKEN as a Bearer token
//   - the v2 API (partnerships lookup, actions) takes Basic
//     base64(public_key:secret_key)
// Using one where the other belongs returns a 401 that reads like a bad
// credential rather than the wrong scheme.
//
// Everything is optional. No PARTNERSTACK_TRACKING_TOKEN and this module
// is inert: it reports back that it did not send, the caller does not
// stamp, and nothing else in the system changes.
//
// This module NEVER throws. The caller runs after res.json() on a lead
// who has already been served, and a rejected promise there is noise at
// best. Failures come back as { ok: false, reason } and are logged.
// ============================================================

const CONVERSION_URL = 'https://partnerlinks.io/conversion/xid';
const TIMEOUT_MS = 10000;

/* Every call and every response is logged, in full, on purpose. This is the
   only record that an affiliate was credited, and "the conversion did not
   arrive" is a conversation you cannot have from a log line that says
   "PartnerStack failed". Bodies are truncated, not omitted. */
function logCall(direction, payload) {
  console.log(`[PartnerStack] ${direction} ${JSON.stringify(payload).slice(0, 800)}`);
}

/**
 * Send one signup conversion.
 * Resolves { ok, status, body, reason } — never rejects.
 */
async function sendConversion({ xid, customer_key, email, name, ip_address, user_agent, origin, meta }) {
  const token = process.env.PARTNERSTACK_TRACKING_TOKEN;
  if (!token) {
    console.warn('[PartnerStack] PARTNERSTACK_TRACKING_TOKEN not set — conversion NOT sent');
    return { ok: false, reason: 'no_token' };
  }
  if (!xid)          return { ok: false, reason: 'no_xid' };
  if (!customer_key) return { ok: false, reason: 'no_customer_key' };

  const payload = {
    xid,
    customer_key,
    email:      email      || undefined,
    name:       name       || undefined,
    ip_address: ip_address || undefined,
    user_agent: user_agent || undefined,
    origin:     origin     || undefined,
  };
  /* Only sent when there is something in it. An empty object is not a
     meaningful payload and PartnerStack has no reason to see one. */
  const metaClean = {};
  for (const [k, v] of Object.entries(meta || {})) {
    const val = (v === null || v === undefined) ? '' : String(v).trim();
    if (val) metaClean[k] = val.slice(0, 500);
  }
  if (Object.keys(metaClean).length) payload.meta = metaClean;
  logCall('-> POST /conversion/xid', payload);

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), TIMEOUT_MS);
  try {
    const res = await fetch(CONVERSION_URL, {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${token}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(payload),
      signal: controller.signal,
    });
    const text = await res.text().catch(() => '');
    logCall(`<- ${res.status} ${res.ok ? 'OK' : 'FAIL'}`, { status: res.status, body: text.slice(0, 800) });
    if (!res.ok) {
      return { ok: false, status: res.status, body: text, reason: `http_${res.status}` };
    }
    return { ok: true, status: res.status, body: text };
  } catch (err) {
    const reason = err && err.name === 'AbortError' ? 'timeout' : 'network_error';
    console.warn(`[PartnerStack] <- ${reason}: ${err && err.message}`);
    return { ok: false, reason, error: err && err.message };
  } finally {
    clearTimeout(timer);
  }
}

/* ============================================================
   The v2 API — a DIFFERENT host and a DIFFERENT auth scheme
   ------------------------------------------------------------
   The conversion endpoint above is partnerlinks.io with a Bearer tracking
   token. Everything below is api.partnerstack.com with Basic
   base64(public_key:secret_key). Both credentials sit in the same env and
   using one where the other belongs returns a 401 that reads like a bad
   password rather than the wrong scheme — which is a genuinely expensive
   hour to lose, so the split is stated at every call site.
   ============================================================ */
const V2_BASE = 'https://api.partnerstack.com/api/v2';

function v2AuthHeader() {
  const pub = process.env.PARTNERSTACK_PUBLIC_KEY;
  const sec = process.env.PARTNERSTACK_SECRET_KEY;
  if (!pub || !sec) return null;
  return 'Basic ' + Buffer.from(`${pub}:${sec}`).toString('base64');
}

/* GET /v2/partnerships/{unique_identifier}
   The identifier may be a partner_key, an internal partnership_key or an
   email. We pass the DECODED partner key straight from the cookie — the
   base64 form in the URL param is never stored and never sent here.

   Resolves { ok, name, email, raw } — never rejects. */
async function fetchPartnership(partnerKey) {
  const auth = v2AuthHeader();
  if (!auth) {
    console.warn('[PartnerStack] PARTNERSTACK_PUBLIC_KEY / _SECRET_KEY not set — partner not resolved');
    return { ok: false, reason: 'no_credentials' };
  }
  if (!partnerKey) return { ok: false, reason: 'no_partner_key' };

  const url = `${V2_BASE}/partnerships/${encodeURIComponent(partnerKey)}`;
  logCall('-> GET /v2/partnerships', { partner_key: partnerKey });

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), TIMEOUT_MS);
  try {
    const res = await fetch(url, {
      method: 'GET',
      headers: { 'Authorization': auth, 'Accept': 'application/json' },
      signal: controller.signal,
    });
    const text = await res.text().catch(() => '');
    logCall(`<- ${res.status} ${res.ok ? 'OK' : 'FAIL'}`, { status: res.status, body: text.slice(0, 800) });
    if (!res.ok) return { ok: false, status: res.status, body: text, reason: `http_${res.status}` };

    let json;
    try { json = JSON.parse(text); } catch { return { ok: false, reason: 'bad_json', body: text }; }

    /* The response wraps the partnership in { status, message, data }, and
       `data` has been seen both as the partnership itself and as a container
       for it. Unwrapping defensively costs nothing and means a shape change
       degrades to "name not resolved" rather than to a crash on the lead path. */
    const d = (json && json.data) || json || {};
    const p = d.partnership || d;
    const name = [p.first_name, p.last_name].map(v => (v || '').trim()).filter(Boolean).join(' ')
      || (p.name || '').trim()
      || (p.company_name || '').trim()
      || null;
    const email = (p.email || '').trim() || null;
    if (!name && !email) return { ok: false, reason: 'no_identity_in_response', body: text.slice(0, 400) };
    return { ok: true, name, email, raw: p };
  } catch (err) {
    const reason = err && err.name === 'AbortError' ? 'timeout' : 'network_error';
    console.warn(`[PartnerStack] <- ${reason}: ${err && err.message}`);
    return { ok: false, reason, error: err && err.message };
  } finally {
    clearTimeout(timer);
  }
}

/* POST /v2/actions — the qualification event.
   Basic auth, like the partnerships lookup and unlike the conversion.

   FOUR required fields: type, value, target_type, target_key. There is NO
   customer_key field on this endpoint — that name belongs to
   /conversion/xid, and the two endpoints do not share a schema. Sending
   customer_key here returns 400 "'target_type' is a required property",
   which reads like one missing field and is actually two missing plus one
   unrecognised.

   target_type is "customer" here, not "partnership": the action attaches to
   the customer the conversion created, and PartnerStack resolves the partner
   from that customer's existing attribution. Targeting the partnership would
   attach a partner-level event with no customer context — a different event
   that would still return 200.

   Resolves { ok, status, body, reason } — never rejects. */
async function sendAction({ customer_key, type, value }) {
  const auth = v2AuthHeader();
  if (!auth) {
    console.warn('[PartnerStack] PARTNERSTACK_PUBLIC_KEY / _SECRET_KEY not set — action NOT sent');
    return { ok: false, reason: 'no_credentials' };
  }
  if (!customer_key) return { ok: false, reason: 'no_customer_key' };
  if (!type)         return { ok: false, reason: 'no_type' };

  const payload = {
    type,
    value: value === undefined ? 1 : value,
    target_type: 'customer',
    target_key: customer_key,
  };
  logCall('-> POST /v2/actions', payload);

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), TIMEOUT_MS);
  try {
    const res = await fetch(`${V2_BASE}/actions`, {
      method: 'POST',
      headers: { 'Authorization': auth, 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
      signal: controller.signal,
    });
    const text = await res.text().catch(() => '');
    logCall(`<- ${res.status} ${res.ok ? 'OK' : 'FAIL'}`, { status: res.status, body: text.slice(0, 800) });
    if (!res.ok) return { ok: false, status: res.status, body: text, reason: `http_${res.status}` };
    return { ok: true, status: res.status, body: text };
  } catch (err) {
    const reason = err && err.name === 'AbortError' ? 'timeout' : 'network_error';
    console.warn(`[PartnerStack] <- ${reason}: ${err && err.message}`);
    return { ok: false, reason, error: err && err.message };
  } finally {
    clearTimeout(timer);
  }
}

/* --------------------------------------------------------
   fetchCustomer — did the conversion actually create anything?

   /conversion/xid answers 200 with an EMPTY body, so there is nothing in the
   response to check. A 200 that creates no customer would still stamp
   ps_signup_sent_at, and the once-per-domain rule would then burn that domain
   permanently with nothing surfacing anywhere. The only real proof is reading
   the customer back.

   Three outcomes, and the difference between the last two matters more than
   anything else here:
     { ok: true,  exists: true  }  the record is there
     { ok: true,  exists: false }  a definitive 404 — it was NOT created
     { ok: false, ... }           we could not tell (5xx, network, timeout)

   "We could not tell" must never be treated as "it is missing". Releasing a
   claim on a PartnerStack outage would un-stamp every pending conversion and
   fire them all again on the next submit.
-------------------------------------------------------- */
async function fetchCustomer(customerKey) {
  const auth = v2AuthHeader();
  if (!auth) return { ok: false, reason: 'no_credentials' };
  if (!customerKey) return { ok: false, reason: 'no_customer_key' };

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), TIMEOUT_MS);
  try {
    const res = await fetch(`${V2_BASE}/customers/${encodeURIComponent(customerKey)}`,
      { method: 'GET', headers: { Authorization: auth, Accept: 'application/json' }, signal: controller.signal });
    if (res.status === 404) {
      logCall('<- 404 customer NOT found', { customer_key: customerKey });
      return { ok: true, exists: false, status: 404 };
    }
    const text = await res.text().catch(() => '');
    if (!res.ok) {
      logCall(`<- ${res.status} customer read failed`, { customer_key: customerKey, body: text.slice(0, 300) });
      return { ok: false, status: res.status, reason: `http_${res.status}` };
    }
    let test = null;
    try { const d = (JSON.parse(text).data) || {}; test = d.test; } catch { /* shape change: still exists */ }
    return { ok: true, exists: true, status: res.status, test };
  } catch (err) {
    const reason = err && err.name === 'AbortError' ? 'timeout' : 'network_error';
    return { ok: false, reason, error: err && err.message };
  } finally {
    clearTimeout(timer);
  }
}

module.exports = { sendConversion, fetchPartnership, sendAction, fetchCustomer, CONVERSION_URL, V2_BASE };
