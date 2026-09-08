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
  /* What the visitor came in SAYING, before partnerHearAboutUs() overwrote it
     with "Partner - <name>". A partner-referred lead who arrived on a paid ad
     is two real facts and hear_about_us__c can only hold one — this is the
     other one, and it is the half an AE actually wants on the call.

     Never overwritten once set, on either side: leads.hear_about_us_raw
     COALESCEs the STORED value first (the opposite direction from every other
     column in that upsert) so the first non-empty value sticks. */
  hear_about_us_raw: 'hear_about_us_raw__c',
  /* Created 8 Sept 2026 via the Tooling API, with FieldPermissions granted
     and a real write/read round-trip — creating a field does NOT grant
     access to it, which is the 4 Sept lesson. */
  product: 'Product__c',
  about_business: 'About_Business__c',
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
/* Salesforce rejects the ENTIRE record when it does not recognise one field.
   Not the field — the record. So a Product__c that is missing, renamed, or
   simply not visible to the integration user does not cost us a column, it
   costs us the whole lead.

   The fields exist and are permissioned today, verified by round-trip. This
   guard is for the day that stops being true: a sandbox refresh, a profile
   change, a field renamed by someone in Setup. It strips only the fields
   Salesforce actually named and retries ONCE, so a lead still lands with
   everything else intact.

   INVALID_FIELD_FOR_INSERT_UPDATE is what a missing field-level permission
   returns; INVALID_FIELD is what a genuinely absent field returns. Both are
   handled, because from here they are the same problem.

   Deliberately narrow: only these two error codes, only fields named in the
   error, only one retry. A blanket "strip anything and keep trying" would
   quietly post half a lead forever and nobody would find out. */
const SF_UNKNOWN_FIELD_CODES = ['INVALID_FIELD_FOR_INSERT_UPDATE', 'INVALID_FIELD'];

function sfUnknownFields(result) {
  const arr = Array.isArray(result) ? result : [result];
  const named = new Set();
  for (const e of arr) {
    if (!e || !SF_UNKNOWN_FIELD_CODES.includes(e.errorCode)) continue;
    /* Salesforce names the offender in two shapes:
         fields: ['Product__c']
         message: "No such column 'Product__c' on sobject of type Lead" */
    for (const f of e.fields || []) named.add(f);
    for (const m of String(e.message || '').matchAll(/'([A-Za-z0-9_]+__c)'/g)) named.add(m[1]);
  }
  return [...named];
}

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

    const post = async (body) => {
      const r = await fetch(
        `${instanceUrl}/services/data/v60.0/sobjects/Lead/`,
        {
          method: 'POST',
          headers: {
            Authorization: `Bearer ${accessToken}`,
            'Content-Type': 'application/json',
            'Sforce-Duplicate-Rule-Header': 'allowSave=true',
          },
          body: JSON.stringify(body),
        }
      );
      return { ok: r.ok, result: await r.json() };
    };

    let { ok: postOk, result } = await post(lead);

    /* One retry, with only the fields Salesforce named removed. Losing a
       column beats losing the lead. */
    if (!postOk) {
      const unknown = sfUnknownFields(result);
      if (unknown.length) {
        const retry = { ...lead };
        for (const f of unknown) delete retry[f];
        console.warn(`[SF] Lead rejected for unknown field(s) ${unknown.join(', ')} — retrying WITHOUT them so the lead is not lost. Check field-level security in Setup.`);
        ({ ok: postOk, result } = await post(retry));
        if (postOk) {
          console.warn(`[SF] ✅ Lead created after dropping ${unknown.join(', ')} — those values are NOT in Salesforce for ${payload.email || 'n/a'}`);
          return { success: true, leadId: result.id, droppedFields: unknown };
        }
      }
    }

    if (!postOk) {
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

/* Shared by all three paginated readers below. Salesforce pages at ~1,250
   records whatever you ask for, so 25 pages is ~31k Opportunities before we
   refuse to answer. Declared here because it is used by the first of them —
   it sat below its first use, which was legal and read as a mistake. */
const SF_MAX_PAGES = 25;

/* --------------------------------------------------------
   updateOpportunityFields — THE FIRST WRITE THIS SERVICE MAKES TO OPPORTUNITY

   Everything else this module does on Opportunity is read-only, so a write
   rejection here has never been exercised once. That matters more than it
   sounds: the integration user is a full System Administrator today (see
   docs/tickets/salesforce-integration-user-is-a-system-administrator.md), so
   this will succeed — right up until someone does the right thing and reduces
   that profile, at which point it starts 403ing.

   And a silent failure would look EXACTLY like a partner with no Opportunity,
   which is a state the Partners tab already renders for real reasons. So this
   returns a discriminated result and the caller alerts on it. It never throws:
   the caller is a 15-minute sweep with nothing awaiting it.

   Note the field-level-security trap recorded in that ticket: creating a custom
   field through the Tooling API does NOT grant access to it. Both fields
   created on 4 Sept came back 201 and were then invisible to the user that
   created them until FieldPermissions rows were added. So a 400
   INVALID_FIELD_FOR_INSERT_UPDATE here means "no field-level access", not
   "no such field", and the two read identically from the outside.
-------------------------------------------------------- */
async function updateOpportunityFields(opportunityId, fields) {
  if (!opportunityId) return { ok: false, reason: 'no_opportunity_id' };
  if (!fields || !Object.keys(fields).length) return { ok: false, reason: 'no_fields' };
  try {
    const { accessToken, instanceUrl } = await getSalesforceToken();
    const res = await fetch(
      `${instanceUrl}/services/data/v60.0/sobjects/Opportunity/${encodeURIComponent(opportunityId)}`,
      {
        method: 'PATCH',
        headers: { Authorization: `Bearer ${accessToken}`, 'Content-Type': 'application/json' },
        body: JSON.stringify(fields),
      }
    );
    /* A successful PATCH is 204 with no body. */
    if (res.status === 204) return { ok: true, status: 204 };
    const text = await res.text().catch(() => '');
    /* Separated because they need different actions from a human. A permission
       or field-security problem affects EVERY domain and needs Salesforce
       setup changed; a per-record failure affects one. */
    const permission = res.status === 403 || res.status === 401 ||
      /INSUFFICIENT_ACCESS|INVALID_FIELD_FOR_INSERT_UPDATE|FIELD_INTEGRITY_EXCEPTION/i.test(text);
    console.warn(`[SF] Opportunity update failed (${res.status})${permission ? ' — PERMISSION or FIELD SECURITY' : ''}: ${text.slice(0, 300)}`);
    return { ok: false, status: res.status, reason: permission ? 'permission' : `http_${res.status}`, body: text.slice(0, 300) };
  } catch (err) {
    console.warn('[SF] Opportunity update error:', err.message);
    return { ok: false, reason: 'error', error: err.message };
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

   THIS USED TO BE `LIMIT 200` WITH NO PAGINATION AND NO COMPLETENESS CHECK,
   and it is the third time that exact shape has been found in this repo.
   Nobody unticks Qualified_Demo__c, so the ticked set only ever grows: past
   200 across the whole org, Salesforce would return an arbitrary 200 in an
   undefined order and a newly ticked partner Opportunity could sit outside
   them. The affiliate is never paid and nothing anywhere says so. The trigger
   was not hypothetical — it is the direct consequence of telling AEs the
   checkbox exists, which is the point of the field.

   So: no LIMIT, paginated, and the count is checked against totalSize. A
   LIMIT caps totalSize as well as the rows, which is how `records.length ===
   totalSize` once passed on 2,000 of 5,898 records in findOpportunityDomains
   below — the LIMIT defeats the very guard meant to catch it. Pagination plus
   SF_MAX_PAGES is the bound.

   RETURNS { ok, records }, NOT a bare array. It returned `[]` on every
   failure, so "no AE has ticked anything" and "Salesforce did not answer"
   were the same value to the caller — the same conflation findOpportunityDomains
   was rewritten to remove. Those are opposite conclusions: one is a quiet
   Tuesday, the other is money not moving.

   KEPT SEPARATE from findOpportunityDomains on purpose, and it is worth saying
   why because folding the two together looks like an obvious win. That one
   scans every Opportunity in the window (5,898 today, 6 pages) because it has
   to tell "no Opportunity exists" from "exists but unticked". This one reads
   only the ticked ones — 1 page for years — and it is the query the money
   waits on, so it runs every couple of minutes. Driving the poller off the
   other query's results would put the cheap, latency-sensitive question on the
   expensive question's 15-minute schedule.

   NO DATE BOUND, deliberately, and this is the one place the two queries
   differ in a way that matters. findOpportunityDomains bounds to 180 days
   (PS_GAP_SF_LOOKBACK_D) because it is sizing a population. Here a bound would
   mean an Opportunity created before the window and ticked today is invisible
   forever, which is a silently unpaid affiliate. The ticked set is small enough
   that reading all of it costs nothing, so it reads all of it.
-------------------------------------------------------- */
async function findQualifiedDemoOpportunities() {
  try {
    const { accessToken, instanceUrl } = await getSalesforceToken();
    const soql =
      `SELECT Id, Name, Account.Website, Account.Name, ` +
      `(SELECT Contact.Email FROM OpportunityContactRoles ORDER BY IsPrimary DESC LIMIT 1) ` +
      `FROM Opportunity WHERE Qualified_Demo__c = true`;
    /* The LIMIT inside the subquery is a different thing and is correct: it
       picks the ONE primary contact per Opportunity, and it does not bound the
       outer result set at all. */

    const map = (data) => (data.records || []).map((r) => {
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

    let url = `${instanceUrl}/services/data/v60.0/query/?q=${encodeURIComponent(soql)}`;
    let records = [];
    let totalSize = null;
    let pages = 0;
    while (url && pages < SF_MAX_PAGES) {
      const res = await fetch(url, { method: 'GET', headers: { Authorization: `Bearer ${accessToken}` } });
      if (!res.ok) {
        const err = await res.text();
        console.warn('[SF] Qualified-demo query failed:', err.slice(0, 400));
        return { ok: false, reason: `http_${res.status}`, records: [] };
      }
      const data = await res.json();
      if (totalSize === null) totalSize = data.totalSize;
      records = records.concat(map(data));
      pages++;
      url = data.done === false && data.nextRecordsUrl ? instanceUrl + data.nextRecordsUrl : null;
    }

    /* FAIL LOUDLY rather than hand back a partial set. A short read here is
       indistinguishable from "those AEs have not ticked yet", and the caller
       would skip exactly the domains it could not see. */
    if (url) {
      console.warn(`[SF] Qualified-demo pagination hit the page cap (${SF_MAX_PAGES}) with ${records.length} of ${totalSize} — refusing to return a partial set`);
      return { ok: false, reason: 'pagination_incomplete', records: [], totalSize, fetched: records.length };
    }
    if (totalSize !== null && records.length < totalSize) {
      console.warn(`[SF] Qualified-demo query returned ${records.length} of ${totalSize} — refusing to return a partial set`);
      return { ok: false, reason: 'incomplete', records: [], totalSize, fetched: records.length };
    }

    console.log(`[SF] Qualified demos found: ${records.length} over ${pages} page(s)`);
    return { ok: true, records, pages, totalSize };
  } catch (err) {
    console.warn('[SF] Qualified-demo query error:', err.message);
    return { ok: false, reason: 'error', error: err.message, records: [] };
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

async function findOpportunityDomains({ sinceDays = 180 } = {}) {
  try {
    const { accessToken, instanceUrl } = await getSalesforceToken();
    const days = parseInt(sinceDays, 10) || 180;
    const soql =
      /* Qualified_Demo__c comes back on the SAME query that establishes
         existence, so "does an Opportunity exist" and "has the AE ticked it"
         cost one call between them, not two. */
      `SELECT Id, Account.Website, Qualified_Demo__c, ` +
      `(SELECT Contact.Email FROM OpportunityContactRoles ORDER BY IsPrimary DESC LIMIT 1) ` +
      `FROM Opportunity WHERE CreatedDate = LAST_N_DAYS:${days}`;
    /* NO `LIMIT`. A LIMIT caps totalSize as well as the rows, so
       records.length === totalSize is satisfied by a truncated result and the
       completeness check below passes on an incomplete set — the LIMIT defeats
       the very guard meant to catch it. Measured: with LIMIT 2000 the API
       reported totalSize 2000 and we "completed" at 2,000 of 5,898, with the
       one ticked Opportunity we cared about outside the set. Pagination plus
       SF_MAX_PAGES is the bound. */
    /* PAGINATED. Salesforce returns ~1,250 records per page regardless of the
       LIMIT, and this used to take the first page and stop: 5,898 Opportunities
       existed and we read 1,252 of them — 21%. Every domain among the other
       79% reported "no Opportunity", which is a clean-looking wrong answer, and
       it is how a domain we had just qualified came back as having none.

       nextRecordsUrl is the mechanism Salesforce provides for this. */
    const map = (data) => (data.records || []).map((r) => {
      const roles = r.OpportunityContactRoles && r.OpportunityContactRoles.records;
      const contactEmail = roles && roles[0] && roles[0].Contact && roles[0].Contact.Email;
      return {
        id: r.Id,
        website: (r.Account && r.Account.Website) || null,
        contactEmail: contactEmail || null,
        qualified: r.Qualified_Demo__c === true,
      };
    });

    let url = `${instanceUrl}/services/data/v60.0/query/?q=${encodeURIComponent(soql)}`;
    let records = [];
    let totalSize = null;
    let pages = 0;
    while (url && pages < SF_MAX_PAGES) {
      const res = await fetch(url, { method: 'GET', headers: { Authorization: `Bearer ${accessToken}` } });
      if (!res.ok) {
        const err = await res.text();
        console.warn('[SF] Opportunity-domain query failed:', err.slice(0, 300));
        return { ok: false, reason: `http_${res.status}`, records: [] };
      }
      const data = await res.json();
      if (totalSize === null) totalSize = data.totalSize;
      records = records.concat(map(data));
      pages++;
      url = data.done === false && data.nextRecordsUrl ? instanceUrl + data.nextRecordsUrl : null;
    }

    /* FAIL LOUDLY rather than return a partial set. An incomplete list read as
       complete is what caused the bug this pagination fixes; handing back
       ok:true with 40% of the records would reproduce it one page later. */
    if (url) {
      console.warn(`[SF] Opportunity pagination hit the page cap (${SF_MAX_PAGES}) with ${records.length} of ${totalSize} — refusing to return a partial set`);
      return { ok: false, reason: 'pagination_incomplete', records: [], totalSize, fetched: records.length };
    }
    const incomplete = totalSize !== null && records.length < totalSize;
    if (incomplete) {
      console.warn(`[SF] Opportunity query returned ${records.length} of ${totalSize} — refusing to return a partial set`);
      return { ok: false, reason: 'incomplete', records: [], totalSize, fetched: records.length };
    }
    console.log(`[SF] Opportunity domains: ${records.length} record(s) over ${pages} page(s)`);
    return { ok: true, records, pages, totalSize, truncated: false };
  } catch (err) {
    console.warn('[SF] Opportunity-domain query error:', err.message);
    return { ok: false, reason: 'error', error: err.message, records: [] };
  }
}

/* ── Which of these emails has enrichment on its Lead? ──────────────
   Answers the held-vs-sent question: we hold Apollo enrichment for roughly
   half our leads and nothing anywhere compared that against what Salesforce
   actually received. On 4 Sept that gap was reported as 0 of 200 Leads
   enriched, i.e. a total outage, and it was a measurement error — the
   denominator was a LIMIT over the whole Lead object, which in this org is
   ~99.5% outbound list imports that never touched Apollo. The real figure
   was 46.5%.

   So this exists to make the comparison cheap and permanent rather than
   something someone reconstructs by hand under time pressure and gets wrong.

   Batched because SOQL takes an IN list, not a join, and paginated per batch
   for the same reason findOpportunityDomains is: Salesforce pages at ~1,250
   records whatever you ask for.

   Returns ok:false and NO records on any incomplete read. A partial join
   reads as "Salesforce is missing these leads" when the truth is "we did not
   finish asking", which is the exact failure this whole area keeps producing. */
const SF_EMAIL_BATCH = 200;

async function findEnrichmentByEmails(emails) {
  const deduped = Array.from(new Set((emails || [])
    .map((e) => String(e || '').trim().toLowerCase())
    .filter(Boolean)));
  /* A quote would break out of the IN list. Dropped rather than escaped, and
     counted SEPARATELY from the dedupe: skipped is rendered as "unquotable",
     and folding case-duplicates into it made that label a lie the first time
     this ran against real addresses. */
  const list = deduped.filter((e) => !/['\\]/.test(e));
  const skipped = deduped.length - list.length;
  if (!list.length) return { ok: true, found: new Map(), skipped, batches: 0 };

  try {
    const { accessToken, instanceUrl } = await getSalesforceToken();
    const found = new Map();
    let batches = 0;

    for (let i = 0; i < list.length; i += SF_EMAIL_BATCH) {
      const chunk = list.slice(i, i + SF_EMAIL_BATCH);
      const soql =
        `SELECT Id, Email, enriched_title__c, enriched_industry__c, enriched_company_size__c ` +
        `FROM Lead WHERE Email IN (${chunk.map((e) => `'${e}'`).join(',')})`;
      /* No LIMIT, for the reason spelled out in findOpportunityDomains: a
         LIMIT caps totalSize as well as the rows, so the completeness check
         below would be satisfied by a truncated answer. */
      let url = `${instanceUrl}/services/data/v60.0/query/?q=${encodeURIComponent(soql)}`;
      let records = [];
      let totalSize = null;
      let pages = 0;
      while (url && pages < SF_MAX_PAGES) {
        const res = await fetch(url, { method: 'GET', headers: { Authorization: `Bearer ${accessToken}` } });
        if (!res.ok) {
          const err = await res.text();
          console.warn('[SF] Enrichment-by-email query failed:', err.slice(0, 300));
          return { ok: false, reason: `http_${res.status}`, found: new Map() };
        }
        const data = await res.json();
        if (totalSize === null) totalSize = data.totalSize;
        records = records.concat(data.records || []);
        pages++;
        url = data.done === false && data.nextRecordsUrl ? instanceUrl + data.nextRecordsUrl : null;
      }
      if (url) {
        console.warn(`[SF] Enrichment-by-email hit the page cap (${SF_MAX_PAGES}) on batch ${batches + 1}`);
        return { ok: false, reason: 'pagination_incomplete', found: new Map() };
      }
      if (totalSize !== null && records.length < totalSize) {
        console.warn(`[SF] Enrichment-by-email read ${records.length} of ${totalSize} — refusing to return a partial set`);
        return { ok: false, reason: 'incomplete', found: new Map() };
      }
      for (const r of records) {
        const k = String(r.Email || '').toLowerCase();
        if (!k || found.has(k)) continue;
        found.set(k, {
          id: r.Id,
          /* ANY of the three counts as arrived. Apollo can return a company
             size with no title, so keying on title alone would report a
             partially enriched Lead as an empty one. */
          enriched: !!(r.enriched_title__c || r.enriched_industry__c || r.enriched_company_size__c),
        });
      }
      batches++;
    }
    console.log(`[SF] Enrichment-by-email: ${found.size} Lead(s) matched from ${list.length} address(es) over ${batches} batch(es)`);
    return { ok: true, found, skipped, batches };
  } catch (err) {
    console.warn('[SF] Enrichment-by-email error:', err.message);
    return { ok: false, reason: 'error', error: err.message, found: new Map() };
  }
}

module.exports = { pushToSalesforce, findSFLeadByEmail, updateSFLead, updateOpportunityFields, getSalesforceToken, findQualifiedDemoOpportunities, findOpportunityDomains, findEnrichmentByEmails, SF_EMAIL_BATCH };
