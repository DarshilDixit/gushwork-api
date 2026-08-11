// ============================================================
// loops.js — Loops.so contact push for the lead-magnet LP
// ------------------------------------------------------------
// Bigrah's flow:
//   form submit -> webhook hits his workflow -> questions get
//   populated from the contact's variables -> contact enrolled
//   in the Loops audience -> email sent
//
// So the contact we push must ALREADY carry the variables. It
// does — everything needed for personalisation is on the row
// before we call Loops.
//
// Two calls, in order:
//   1. PUT  /contacts/update   upsert + mailing list + properties
//   2. POST /events/send       fires the workflow trigger
//
// We use `update` rather than `create` because it upserts:
// `create` errors on an existing contact, and repeat submitters
// are normal here (someone requesting a second list).
//
// Everything is optional. No LOOPS_API_KEY = this module is inert
// and the rest of the system behaves exactly as it does today.
// ============================================================

const API = 'https://app.loops.so/api/v1';
const TIMEOUT_MS = 10000;

/* Loops requires custom properties to exist before you can set them —
   an unknown property is silently dropped, which would look like the
   integration working while every personalisation variable came through
   empty. We create them at boot instead of relying on someone having
   clicked through the Audience UI correctly. */
const CUSTOM_PROPERTIES = [
  { name: 'industryCategory', type: 'string' },
  { name: 'industryIsCustom', type: 'boolean' },
  { name: 'productOrService', type: 'string' },
  { name: 'sellTo', type: 'string' },
  { name: 'companyWebsite', type: 'string' },
  { name: 'websiteSource', type: 'string' },
  { name: 'isFreeEmail', type: 'boolean' },
  { name: 'leadMagnetId', type: 'number' },
  { name: 'entryPoint', type: 'string' },
  { name: 'utmSource', type: 'string' },
  { name: 'utmCampaign', type: 'string' },
];

function enabled() {
  return !!process.env.LOOPS_API_KEY;
}

async function loopsFetch(path, method, body) {
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), TIMEOUT_MS);
  try {
    const res = await fetch(API + path, {
      method,
      headers: {
        Authorization: `Bearer ${process.env.LOOPS_API_KEY}`,
        'Content-Type': 'application/json',
      },
      body: body ? JSON.stringify(body) : undefined,
      signal: ctrl.signal,
    });
    const text = await res.text();
    let json = null;
    try { json = text ? JSON.parse(text) : null; } catch (e) { /* non-JSON */ }
    return { ok: res.ok, status: res.status, json, text };
  } finally {
    clearTimeout(timer);
  }
}

/* Run once at boot. Already-existing properties come back as an error,
   which is the expected steady state — hence the quiet handling. */
async function ensureLoopsProperties() {
  if (!enabled()) return { skipped: true };
  let created = 0, existed = 0, failed = 0;
  for (const prop of CUSTOM_PROPERTIES) {
    try {
      const r = await loopsFetch('/contacts/properties', 'POST', prop);
      if (r.ok) created++;
      else if (r.status === 400 || r.status === 409) existed++;
      else { failed++; console.warn(`[Loops] property ${prop.name}: HTTP ${r.status} ${r.text}`); }
    } catch (err) {
      failed++;
      console.warn(`[Loops] property ${prop.name} failed:`, err.message);
    }
  }
  console.log(`[Loops] contact properties — ${created} created, ${existed} already present, ${failed} failed`);
  return { created, existed, failed };
}

/* Loops has no `website` default property, and `firstName` is genuinely
   unknown here — the LP never asks for a name. Sending a guessed name
   would be worse than sending none, so we leave it out and let the
   email template fall back. */
function buildContact(lead, listId) {
  const body = {
    email: lead.email,
    source: 'Lead Magnet LP - Buyers Questions',
    subscribed: true,
    userGroup: 'Lead Magnet',

    industryCategory: lead.industry_category || '',
    industryIsCustom: lead.industry_is_custom === true,
    productOrService: lead.product_or_service || '',
    sellTo: lead.sell_to || '',
    companyWebsite: lead.website || '',
    websiteSource: lead.website_source || '',
    isFreeEmail: lead.is_free_email === true,
    leadMagnetId: typeof lead.id === 'number' ? lead.id : 0,
    entryPoint: lead.entry_point || '',
    utmSource: lead.utm_source || '',
    utmCampaign: lead.utm_campaign || '',
  };
  if (listId) body.mailingLists = { [listId]: true };
  return body;
}

/**
 * Upsert the contact, add to the audience, then fire the workflow event.
 * Never throws — returns a result the caller stores on the row.
 */
async function pushContactToLoops(lead) {
  if (!enabled()) return { ok: false, skipped: true, error: 'LOOPS_API_KEY not set' };
  if (!lead || !lead.email) return { ok: false, error: 'no email' };

  const listId = process.env.LOOPS_MAILING_LIST_ID || '';
  const eventName = process.env.LOOPS_EVENT_NAME || 'lead_magnet_requested';

  try {
    const contact = await loopsFetch('/contacts/update', 'PUT', buildContact(lead, listId));
    if (!contact.ok) {
      const msg = `contacts/update HTTP ${contact.status}: ${(contact.text || '').slice(0, 200)}`;
      console.error('[Loops] ✗', lead.email, msg);
      return { ok: false, error: msg };
    }

    const contactId = (contact.json && contact.json.id) || null;

    /* The event is what actually starts Bigrah's workflow. If it fails the
       contact still exists and is on the list, so this is reported but not
       treated as a total failure — he can re-trigger from Loops. */
    let eventError = null;
    if (process.env.LOOPS_SEND_EVENT !== 'false') {
      const ev = await loopsFetch('/events/send', 'POST', {
        email: lead.email,
        eventName,
        eventProperties: {
          industryCategory: lead.industry_category || '',
          productOrService: lead.product_or_service || '',
          sellTo: lead.sell_to || '',
          companyWebsite: lead.website || '',
          leadMagnetId: typeof lead.id === 'number' ? lead.id : 0,
        },
      });
      if (!ev.ok) {
        eventError = `events/send HTTP ${ev.status}: ${(ev.text || '').slice(0, 160)}`;
        console.warn('[Loops] event failed for', lead.email, '—', eventError);
      }
    }

    console.log(`[Loops] ✅ ${lead.email} pushed${listId ? ' + added to list' : ''}${eventError ? ' (event failed)' : ''}`);
    return { ok: true, contactId, eventError };
  } catch (err) {
    const msg = err.name === 'AbortError' ? 'timeout after 10s' : err.message;
    console.error('[Loops] ✗', lead.email, msg);
    return { ok: false, error: msg };
  }
}

/** Sanity check used by /monitor/lm-loops-health */
async function testLoopsKey() {
  if (!enabled()) return { configured: false };
  try {
    const r = await loopsFetch('/api-key', 'GET');
    return {
      configured: true,
      valid: r.ok,
      team: (r.json && r.json.teamName) || null,
      listConfigured: !!process.env.LOOPS_MAILING_LIST_ID,
      eventName: process.env.LOOPS_EVENT_NAME || 'lead_magnet_requested',
    };
  } catch (err) {
    return { configured: true, valid: false, error: err.message };
  }
}

module.exports = { pushContactToLoops, ensureLoopsProperties, testLoopsKey, CUSTOM_PROPERTIES };
