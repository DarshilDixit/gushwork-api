// ============================================================
// partnerstack.js — the S2S conversion call
// ------------------------------------------------------------
// One job: tell PartnerStack that a partner-referred visitor became a
// signup, so the affiliate gets credited.
//
// POST https://partnerlinks.io/conversion/xid
//   Authorization: Bearer <PARTNERSTACK_TRACKING_TOKEN>
//   { xid, customer_key, email, name }
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
async function sendConversion({ xid, customer_key, email, name }) {
  const token = process.env.PARTNERSTACK_TRACKING_TOKEN;
  if (!token) {
    console.warn('[PartnerStack] PARTNERSTACK_TRACKING_TOKEN not set — conversion NOT sent');
    return { ok: false, reason: 'no_token' };
  }
  if (!xid)          return { ok: false, reason: 'no_xid' };
  if (!customer_key) return { ok: false, reason: 'no_customer_key' };

  const payload = { xid, customer_key, email: email || undefined, name: name || undefined };
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

module.exports = { sendConversion, CONVERSION_URL };
