// ============================================================
// meta-capi.js — Meta Conversions API
// Active event types, all fired from pushFormEventsToMeta():
//   Lead         — initial form submitted (called from /submit)
//   StartTrial   — sell_to includes B2B, fires once with Lead only (not on booking)
//   Schedule     — demo booked (called from /booking-confirmed-webhook)
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
 * Send a single event to Meta CAPI
 */
async function sendEvent(eventName, payload, options = {}) {
  const pixelId = process.env.META_PIXEL_ID;
  const accessToken = process.env.META_ACCESS_TOKEN;

  if (!pixelId || !accessToken) {
    console.warn('[Meta CAPI] Missing credentials, skipping');
    return { success: false, error: 'Missing credentials' };
  }

  const eventData = {
    event_name: eventName,
    event_time: Math.floor(Date.now() / 1000),
    event_id: `${eventName}_${payload.session_id || Date.now()}_${Math.random().toString(36).slice(2, 7)}`,
    event_source_url: payload.page_url || payload.landing_page || '',
    action_source: 'website',

    user_data: {
      em: sha256(payload.email),
      ph: sha256(payload.phone),
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
    },
  };

  // Clean undefined values
  Object.keys(eventData.user_data).forEach((key) => {
    if (eventData.user_data[key] === undefined) delete eventData.user_data[key];
  });
  Object.keys(eventData.custom_data).forEach((key) => {
    if (eventData.custom_data[key] === undefined) delete eventData.custom_data[key];
  });

  const url = `https://graph.facebook.com/v21.0/${pixelId}/events?access_token=${accessToken}`;

  const res = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ data: [eventData] }),
  });

  const result = await res.json();

  if (!res.ok || result.error) {
    console.error(`[Meta CAPI] [${eventName}] error:`, JSON.stringify(result));
    return { success: false, error: result };
  }

  console.log(`[Meta CAPI] ✅ [${eventName}] sent: ${result.events_received} events received`);
  return { success: true, eventName, eventsReceived: result.events_received };
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
    // StartTrial fires only once — on form completion, not on booking
    if (payload.sell_to && payload.sell_to.toUpperCase().includes('B2B')) {
      events.push('StartTrial');
    }
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

  return results;
}

module.exports = { pushFormEventsToMeta };
