// ============================================================
// salesforce.js — Salesforce REST API (Refresh Token Auth)
// Uses External Client App OAuth credentials.
// Needs Client ID, Client Secret, and Refresh Token.
//
// Exports:
//   pushToSalesforce(payload)        — upsert Lead (create or update by email)
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
        grant_type: 'refresh_token',
        client_id: process.env.SF_CLIENT_ID,
        client_secret: process.env.SF_CLIENT_SECRET,
        refresh_token: process.env.SF_REFRESH_TOKEN,
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

// Fields that have a max length enforced by SF
const FIELD_MAX_LENGTHS = {
  landing_page: 255,
  utm_campaign: 255,
  utm_content: 255,
  utm_term: 255,
  referrer: 255,
  page_url: 255,
  fbc: 255,
};

/* --------------------------------------------------------
   buildLeadFields — shared field builder used by create + update
-------------------------------------------------------- */
function buildLeadFields(payload) {
  const lead = {};

  // Map standard fields
  for (const [srcKey, sfKey] of Object.entries(STANDARD_FIELD_MAP)) {
    if (payload[srcKey]) lead[sfKey] = String(payload[srcKey]);
  }

  // Map custom fields, applying max length truncation where needed
  for (const [srcKey, sfKey] of Object.entries(CUSTOM_FIELD_MAP)) {
    if (payload[srcKey] !== undefined && payload[srcKey] !== null && payload[srcKey] !== '') {
      let value = String(payload[srcKey]);
      if (FIELD_MAX_LENGTHS[srcKey]) {
        value = value.substring(0, FIELD_MAX_LENGTHS[srcKey]);
      }
      lead[sfKey] = value;
    }
  }

  return lead;
}

/* --------------------------------------------------------
   pushToSalesforce — Upsert SF Lead (create or update by email)
   - Looks up existing Lead by email first
   - If found: updates it (no duplicate created)
   - If not found: creates new Lead
   This makes it safe even if a separate SF/Cal integration
   has already created a Lead for this email.
-------------------------------------------------------- */
async function pushToSalesforce(payload) {
  try {
    const lead = buildLeadFields(payload);

    // Salesforce requires Company and LastName — set fallbacks
    if (!lead.Company) lead.Company = '[Not Provided]';
    if (!lead.LastName) lead.LastName = 'Unknown';

    lead.LeadSource = 'Website';
    lead.completed__c = payload.booked === true;

    // Check if a Lead already exists for this email — avoid duplicates
    const existingLeadId = payload.email ? await findSFLeadByEmail(payload.email) : null;

    if (existingLeadId) {
      // Lead already exists (ours or from SF team's integration) — update it
      console.log(`[SF] Lead already exists for ${payload.email} — updating instead of creating`);
      return await updateSFLead(existingLeadId, lead);
    }

    // No existing Lead — create new
    const { accessToken, instanceUrl } = await getSalesforceToken();

    const res = await fetch(
      `${instanceUrl}/services/data/v60.0/sobjects/Lead/`,
      {
        method: 'POST',
        headers: {
          Authorization: `Bearer ${accessToken}`,
          'Content-Type': 'application/json',
          'Sforce-Duplicate-Rule-Header': 'allowSave=true',
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
   Also applies max length truncation to any string fields.
-------------------------------------------------------- */
async function updateSFLead(leadId, fields) {
  if (!leadId) return { success: false, error: 'No leadId' };
  try {
    const { accessToken, instanceUrl } = await getSalesforceToken();

    // Apply truncation to any landing_page__c or other long fields passed directly
    const safeFields = { ...fields };
    if (safeFields['landing_page__c']) {
      safeFields['landing_page__c'] = safeFields['landing_page__c'].substring(0, 255);
    }

    const res = await fetch(
      `${instanceUrl}/services/data/v60.0/sobjects/Lead/${leadId}`,
      {
        method: 'PATCH',
        headers: {
          Authorization: `Bearer ${accessToken}`,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(safeFields),
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

/* --------------------------------------------------------
   findQualifiedDemoOpportunities — the step 10 poller's read side.

   Qualified_Demo__c is a checkbox on Opportunity, ticked by an AE after the
   call. We pull every ticked one and match it back to our leads by DOMAIN,
   because the domain is the only identifier both systems share: PartnerStack
   knows the customer by the customer_key we sent at signup, and that key was
   derived from the lead's website.

   Account.Website is the primary source. Some Opportunities have no Account
   website, so the Contact's email domain is taken as a fallback via
   OpportunityContactRole — a lead that reached an Opportunity almost always
   has a contact on it, and their work email is the same company.

   Returns [] on any failure rather than throwing. The poller runs on a timer
   with nothing waiting on it, and a Salesforce blip must not become an
   unhandled rejection in the process.
-------------------------------------------------------- */
async function findQualifiedDemoOpportunities(limit = 200) {
  try {
    const { accessToken, instanceUrl } = await getSalesforceToken();
    const soql =
      `SELECT Id, Name, Account.Website, Account.Name, ` +
      `(SELECT Contact.Email FROM OpportunityContactRoles ORDER BY IsPrimary DESC LIMIT 1) ` +
      `FROM Opportunity WHERE Qualified_Demo__c = true LIMIT ${parseInt(limit, 10) || 200}`;
    const res = await fetch(
      `${instanceUrl}/services/data/v60.0/query/?q=${encodeURIComponent(soql)}`,
      { method: 'GET', headers: { Authorization: `Bearer ${accessToken}` } }
    );
    if (!res.ok) {
      const err = await res.text();
      console.warn('[SF] Qualified-demo query failed:', err.slice(0, 400));
      return [];
    }
    const data = await res.json();
    const out = (data.records || []).map((r) => {
      const roles = r.OpportunityContactRoles && r.OpportunityContactRoles.records;
      const contactEmail = roles && roles[0] && roles[0].Contact && roles[0].Contact.Email;
      return {
        id: r.Id,
        name: r.Name || null,
        website: (r.Account && r.Account.Website) || null,
        accountName: (r.Account && r.Account.Name) || null,
        contactEmail: contactEmail || null,
      };
    });
    console.log(`[SF] Qualified demos found: ${out.length}`);
    return out;
  } catch (err) {
    console.warn('[SF] Qualified-demo query error:', err.message);
    return [];
  }
}

/* --------------------------------------------------------
   findOpportunityDomains — "does an Opportunity exist at all?"

   The step 10 poller asks which Opportunities are QUALIFIED. This asks a
   different question: which domains have an Opportunity of any kind. The gap
   between the two is the sfopp failure mode — a show-up stuck at not_in_sf or
   error means no Opportunity was ever created, so no AE can tick the box, so
   a partner is silently never paid.

   Returns { ok, records } rather than a bare array, because the caller has to
   tell "no Opportunity exists" apart from "Salesforce did not answer". Those
   are opposite conclusions and collapsing them would report a broken
   integration as a clean bill of health.
-------------------------------------------------------- */
async function findOpportunityDomains({ sinceDays = 180, limit = 2000 } = {}) {
  try {
    const { accessToken, instanceUrl } = await getSalesforceToken();
    const days = parseInt(sinceDays, 10) || 180;
    const soql =
      `SELECT Id, Account.Website, ` +
      `(SELECT Contact.Email FROM OpportunityContactRoles ORDER BY IsPrimary DESC LIMIT 1) ` +
      `FROM Opportunity WHERE CreatedDate = LAST_N_DAYS:${days} ` +
      `LIMIT ${parseInt(limit, 10) || 2000}`;
    const res = await fetch(
      `${instanceUrl}/services/data/v60.0/query/?q=${encodeURIComponent(soql)}`,
      { method: 'GET', headers: { Authorization: `Bearer ${accessToken}` } }
    );
    if (!res.ok) {
      const err = await res.text();
      console.warn('[SF] Opportunity-domain query failed:', err.slice(0, 300));
      return { ok: false, reason: `http_${res.status}`, records: [] };
    }
    const data = await res.json();
    const records = (data.records || []).map((r) => {
      const roles = r.OpportunityContactRoles && r.OpportunityContactRoles.records;
      const contactEmail = roles && roles[0] && roles[0].Contact && roles[0].Contact.Email;
      return {
        website: (r.Account && r.Account.Website) || null,
        contactEmail: contactEmail || null,
      };
    });
    return { ok: true, records, truncated: data.totalSize > records.length };
  } catch (err) {
    console.warn('[SF] Opportunity-domain query error:', err.message);
    return { ok: false, reason: 'error', error: err.message, records: [] };
  }
}

module.exports = { pushToSalesforce, findSFLeadByEmail, updateSFLead, getSalesforceToken, findQualifiedDemoOpportunities, findOpportunityDomains };
