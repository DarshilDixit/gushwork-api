// ============================================================
// salesforce.js — Salesforce REST API (Username-Password Auth)
// No Connected App or External Client App needed.
// Just your username, password, and security token.
//
// Exports:
//   pushToSalesforce(payload)        — create a new Lead
//   findSFLeadByEmail(email)         — find Lead ID by email
//   updateSFLead(leadId, fields)     — update existing Lead
// ============================================================

let sfAccessToken = null;
let sfInstanceUrl = null;
let sfTokenExpiresAt = 0;

async function getSalesforceToken() {
  if (sfAccessToken && Date.now() < sfTokenExpiresAt - 300000) {
    return { accessToken: sfAccessToken, instanceUrl: sfInstanceUrl };
  }

  const loginUrl = process.env.SF_LOGIN_URL || 'https://login.salesforce.com';

  const res = await fetch(
    `${loginUrl}/services/oauth2/token`,
    {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: new URLSearchParams({
        grant_type: 'password',
        client_id: process.env.SF_CLIENT_ID,
        client_secret: process.env.SF_CLIENT_SECRET,
        username: process.env.SF_USERNAME,
        password: process.env.SF_PASSWORD + process.env.SF_SECURITY_TOKEN,
      }),
    }
  );

  if (!res.ok) {
    const err = await res.text();
    throw new Error(`Salesforce token error: ${res.status} — ${err}`);
  }

  const data = await res.json();
  sfAccessToken = data.access_token;
  sfInstanceUrl = data.instance_url;
  sfTokenExpiresAt = Date.now() + 7200000;

  return { accessToken: sfAccessToken, instanceUrl: sfInstanceUrl };
}

// Your webhook key → Salesforce STANDARD field API name
const STANDARD_FIELD_MAP = {
  first_name: 'FirstName',
  last_name: 'LastName',
  email: 'Email',
  phone: 'Phone',
  company: 'Company',
  website: 'Website',
};

// Your webhook key → Salesforce CUSTOM field API name
const CUSTOM_FIELD_MAP = {
  // --- Form fields ---
  sell_to: 'sell_to__c',
  hear_about_us: 'hear_about_us__c',
  page_url: 'page_url__c',

  // --- Meta tracking ---
  fbc: 'fbc__c',
  fbp: 'fbp__c',

  // --- Attribution ---
  utm_source: 'utm_source__c',
  utm_medium: 'utm_medium__c',
  utm_campaign: 'utm_campaign__c',
  utm_content: 'utm_content__c',
  utm_term: 'utm_term__c',
  referrer: 'referrer__c',
  landing_page: 'landing_page__c',

  // --- Apollo enrichment ---
  enriched_title: 'enriched_title__c',
  enriched_company_size: 'enriched_company_size__c',
  enriched_industry: 'enriched_industry__c',
  enriched_linkedin: 'enriched_linkedin__c',
  enriched_seniority: 'enriched_seniority__c',
  enriched_departments: 'enriched_departments__c',
  enriched_city: 'enriched_city__c',
  enriched_state: 'enriched_state__c',
  enriched_country: 'enriched_country__c',
  enriched_annual_revenue: 'enriched_annual_revenue__c',
  enriched_total_funding: 'enriched_total_funding__c',
  enriched_funding_stage: 'enriched_funding_stage__c',
  enriched_founded_year: 'enriched_founded_year__c',

  // --- Booking ---
  booking_uid: 'booking_uid__c',
  start_time: 'booking_start_time__c',
  event_type: 'booking_event_type__c',

  // --- Status ---
  step_reached: 'step_reached__c',
};

/* --------------------------------------------------------
   pushToSalesforce — Create a new SF Lead
-------------------------------------------------------- */
async function pushToSalesforce(payload) {
  try {
    const { accessToken, instanceUrl } = await getSalesforceToken();

    const lead = {};

    // Map standard fields
    for (const [srcKey, sfKey] of Object.entries(STANDARD_FIELD_MAP)) {
      if (payload[srcKey]) lead[sfKey] = String(payload[srcKey]);
    }

    // Map custom fields
    for (const [srcKey, sfKey] of Object.entries(CUSTOM_FIELD_MAP)) {
      if (payload[srcKey] !== undefined && payload[srcKey] !== null && payload[srcKey] !== '') {
        lead[sfKey] = String(payload[srcKey]);
      }
    }

    // Salesforce requires Company and LastName — set fallbacks
    if (!lead.Company) lead.Company = '[Not Provided]';
    if (!lead.LastName) lead.LastName = 'Unknown';

    // Set Lead Source
    lead.LeadSource = 'Website';

    // completed__c = false on creation (set to true when booked)
    lead.completed__c = payload.booked === true;

    const res = await fetch(
      `${instanceUrl}/services/data/v60.0/sobjects/Lead/`,
      {
        method: 'POST',
        headers: {
          Authorization: `Bearer ${accessToken}`,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(lead),
      }
    );

    const result = await res.json();

    if (!res.ok) {
      console.error('[SF] Lead creation failed:', JSON.stringify(result));
      return { success: false, error: result };
    }

    console.log(`[SF] ✅ Lead created: ${result.id} | email: ${payload.email || 'n/a'}`);
    return { success: true, leadId: result.id };
  } catch (err) {
    console.error('[SF] Create error:', err.message);
    return { success: false, error: err.message };
  }
}

/* --------------------------------------------------------
   findSFLeadByEmail — Query SF for a Lead by email
   Returns leadId string or null
-------------------------------------------------------- */
async function findSFLeadByEmail(email) {
  if (!email) return null;
  try {
    const { accessToken, instanceUrl } = await getSalesforceToken();

    const query = encodeURIComponent(`SELECT Id FROM Lead WHERE Email = '${email.replace(/'/g, "\\'")}' ORDER BY CreatedDate DESC LIMIT 1`);
    const res = await fetch(
      `${instanceUrl}/services/data/v60.0/query/?q=${query}`,
      {
        method: 'GET',
        headers: { Authorization: `Bearer ${accessToken}` },
      }
    );

    if (!res.ok) {
      const err = await res.text();
      console.warn('[SF] Lead query failed:', err);
      return null;
    }

    const data = await res.json();
    if (data.records && data.records.length > 0) {
      console.log(`[SF] Found Lead ${data.records[0].Id} for email: ${email}`);
      return data.records[0].Id;
    }

    console.log(`[SF] No Lead found for email: ${email}`);
    return null;
  } catch (err) {
    console.warn('[SF] Find error:', err.message);
    return null;
  }
}

/* --------------------------------------------------------
   updateSFLead — Patch an existing SF Lead by ID
   fields = { booking_uid__c: '...', completed__c: true, ... }
-------------------------------------------------------- */
async function updateSFLead(leadId, fields) {
  if (!leadId) return { success: false, error: 'No leadId' };
  try {
    const { accessToken, instanceUrl } = await getSalesforceToken();

    const res = await fetch(
      `${instanceUrl}/services/data/v60.0/sobjects/Lead/${leadId}`,
      {
        method: 'PATCH',
        headers: {
          Authorization: `Bearer ${accessToken}`,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(fields),
      }
    );

    // SF returns 204 No Content on successful PATCH
    if (res.status === 204) {
      console.log(`[SF] ✅ Lead updated: ${leadId}`);
      return { success: true, leadId };
    }

    const result = await res.json();
    console.error('[SF] Lead update failed:', JSON.stringify(result));
    return { success: false, error: result };
  } catch (err) {
    console.error('[SF] Update error:', err.message);
    return { success: false, error: err.message };
  }
}

module.exports = { pushToSalesforce, findSFLeadByEmail, updateSFLead };
