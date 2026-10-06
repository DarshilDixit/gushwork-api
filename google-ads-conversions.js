/* ============================================================
   GOOGLE ADS CONVERSION UPLOAD — offline click conversions for booked
   Google Ads leads, sent through the Data Manager API.

   WHY THE DATA MANAGER API AND NOT UploadClickConversions. The Google Ads
   API closed offline conversion imports to new adopters on 15 June 2026:
   "new adopters will receive the error CUSTOMER_NOT_ALLOWLISTED_FOR_THIS_
   FEATURE when attempting to use the ConversionUploadService.
   UploadClickConversions method" (Google Ads Developer Blog, 15 May 2026).
   This repo had never imported one, so it is a new adopter. Data Manager's
   POST /v1/events:ingest takes the same data, needs NO developer token, and
   wants the datamanager scope -- "The scope
   https://www.googleapis.com/auth/datamanager is required for all services
   in the Data Manager API." Verified on 6 Oct 2026 with two validate-only
   requests against account 5442288209: the real action 7825004775 answered
   200, a made-up one answered 404 INVALID_CONVERSION_ACTION_ID, so a
   validate-only request DOES check the action exists.

   SHAPED LIKE meta-capi.js, deliberately: small pure functions that can be
   asserted without a network, one send function on plain fetch, nothing
   that reaches back into index.js. Everything index.js owns -- the internal
   test-address rule, the website gate, the non-ICP read, the product value,
   the alert plumbing -- is INJECTED into createGadsUploader rather than
   required, for the same reason setMetaOutcomeReporter is injected: a
   module reaching back into index.js is how a require cycle starts. It also
   lets tools/gads-upload-dry-run.js drive the SAME evaluation with the real
   functions lifted out of index.js, so the dry run cannot answer a
   different question to the sweep.

   WHAT THIS NEVER DOES, by construction:
     - it never touches a lead. It reads leads and writes only its own
       table, gads_conversion_uploads. No lead is blocked, delayed or
       changed, and no Meta event, Salesforce write or dialer row moves.
     - it never sends for real unless GADS_UPLOAD_ENABLED is 'true' AND
       GADS_UPLOAD_VALIDATE_ONLY is 'false'. Both defaults are the safe ones.
     - it never sends the same lead twice: one row per session, claimed by
       the database before any request leaves, the session id as the
       transaction id, and the identical frozen payload on every retry.
   ============================================================ */

const crypto = require('crypto');
/* The host helpers now live in agency-domains.js, unchanged, so every agency
   list in the repo matches the same way. Re-exported here under their old
   names for the tests and tools that already use them. */
const { hostOf: gadsHostOf, hostMatches: gadsHostMatches, matchList: gadsMatchList,
  AGENCY_DEFAULT_DOMAINS, agencyDomainsPlus } = require('./agency-domains');

const DATA_MANAGER_INGEST_URL = 'https://datamanager.googleapis.com/v1/events:ingest';
const DATA_MANAGER_SCOPE      = 'https://www.googleapis.com/auth/datamanager';
const TABLE = 'gads_conversion_uploads';

/* The account and the action are configuration, with today's values as the
   defaults so an unset variable cannot point uploads somewhere else. */
const GADS_DEFAULT_CUSTOMER_ID          = '5442288209';
const GADS_DEFAULT_CONVERSION_ACTION_ID = '7825004775';   // "CRM - Qualified Demo Request"

/* AGENCY DOMAINS, Google-only. Flighted and Upraw submit the form while
   setting campaigns up; 16 and 7 lead rows respectively as of 6 Oct 2026,
   4 and 3 of them booked. They are NOT added to INTERNAL_TEST_EMAILS or
   ELV_EXCLUDED_DOMAINS, because those lists also gate Meta, Salesforce and
   the dialer, and taking agencies out of those is a different decision to
   taking them out of Google's bidding signal. The env EXTENDS this list and
   never replaces it, so a typo in Railway cannot quietly let them back in. */
/* Since 7 Oct 2026 this IS the shared agency list (agency-domains.js);
   GADS_EXCLUDED_DOMAINS extends it for Google only, AGENCY_DOMAINS for all. */
const GADS_DEFAULT_EXCLUDED_DOMAINS = AGENCY_DEFAULT_DOMAINS;

/* OTHER STAGING OR PREVIEW HOSTS, Google-only, matched exact-or-subdomain.
   Searched on 6 Oct 2026: the repo names one staging host,
   gushwork.webflow.io, already covered by isInternalSubmission, and every
   lead ever written came from www.gushwork.ai or gushwork.webflow.io (11
   webhook rows have no page at all). So nothing here matches a real lead
   today. 'webflow.io' catches any OTHER Webflow staging site a page is
   duplicated to; the loopback names catch a developer's laptop. */
const GADS_DEFAULT_EXCLUDED_HOSTS = ['webflow.io', 'localhost', '127.0.0.1'];

const GADS_HOLD_MS            = 30 * 60 * 1000;          // booking must be this old before it is looked at
const GADS_MIN_CLICK_AGE_MS   = 6 * 60 * 60 * 1000;      // Google: TOO_RECENT_EVENT under 6 hours
const GADS_MAX_CLICK_AGE_MS   = 90 * 24 * 60 * 60 * 1000;
const GADS_SWEEP_INTERVAL_MS  = 10 * 60 * 1000;
const GADS_STALE_CLAIM_MS     = 15 * 60 * 1000;          // a 'sending' row older than this died mid-send
const GADS_REQUEST_TIMEOUT_MS = 15000;
const GADS_BATCH_LIMIT        = 50;
/* Backoff after attempt 1, 2, 3, ... The last value repeats. Eight attempts
   over roughly three days, then the row is given up on and somebody is told. */
const GADS_RETRY_BACKOFF_MS = [15, 60, 360, 1440].map((m) => m * 60 * 1000);
const GADS_MAX_ATTEMPTS     = 8;

const SKIP_REASONS = ['internal', 'staging', 'agency', 'disqualified', 'non_icp', 'website',
  'free_email', 'no_click_id', 'too_old', 'click_after_booking', 'before_cutover'];

/* ── Settings ─────────────────────────────────────────────────────────── */

/* THE CUTOVER MUST CARRY ITS OFFSET. Lorenzo's sheet backfills everything
   before it and is then unlinked; a cutover read in the wrong zone moves
   the boundary by four or five hours and double-counts, or drops, every
   booking in that gap. So a bare "2026-10-20 00:00" is refused rather than
   guessed at, and an unreadable value sends NOTHING. That is the opposite
   of the lead path on purpose: a double count cannot be taken back, a
   missed conversion can be sent later. */
const CUTOVER_RE = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(:\d{2}(\.\d{1,9})?)?(Z|[+-]\d{2}:\d{2})$/;
function parseCutover(raw) {
  const s = String(raw == null ? '' : raw).trim();
  if (!s) return { at: null, error: 'GADS_UPLOAD_CUTOVER is not set' };
  if (!CUTOVER_RE.test(s)) return { at: null, error: 'GADS_UPLOAD_CUTOVER must be a date-time with its offset, e.g. 2026-10-20T00:00:00-04:00' };
  const at = new Date(s);
  if (Number.isNaN(at.getTime())) return { at: null, error: 'GADS_UPLOAD_CUTOVER is not a real date' };
  return { at, error: null };
}

function listFromEnv(raw, defaults) {
  const extra = String(raw || '').split(',').map((x) => x.trim().toLowerCase().replace(/^www\./, '').replace(/\.$/, '')).filter(Boolean);
  return [...new Set([...defaults, ...extra])];
}

function gadsSettings(env = process.env) {
  const cut = parseCutover(env.GADS_UPLOAD_CUTOVER);
  return {
    enabled:            env.GADS_UPLOAD_ENABLED === 'true',
    /* ON unless it is exactly 'false'. A typo leaves it validating. */
    validateOnly:       env.GADS_UPLOAD_VALIDATE_ONLY !== 'false',
    cutover:            cut.at,
    cutoverError:       cut.error,
    customerId:         String(env.GADS_CUSTOMER_ID || GADS_DEFAULT_CUSTOMER_ID).replace(/-/g, '').trim(),
    conversionActionId: String(env.GADS_CONVERSION_ACTION_ID || GADS_DEFAULT_CONVERSION_ACTION_ID).trim(),
    excludedDomains:    agencyDomainsPlus(env.GADS_EXCLUDED_DOMAINS, env),
    excludedHosts:      listFromEnv(env.GADS_EXCLUDED_HOSTS, GADS_DEFAULT_EXCLUDED_HOSTS),
    excludeFreeEmail:   env.GADS_EXCLUDE_FREE_EMAIL === 'true',
    /* OFF unless exactly 'true'. See GADS_CONSENT_GRANTED below. */
    consentGranted:     env.GADS_CONSENT_GRANTED === 'true',
    credentialsB64:     env.GADS_SERVICE_ACCOUNT_JSON_B64 || '',
  };
}

/* What stops an enabled uploader from running at all, in words. Never the
   credential itself -- only whether it could be read. */
function gadsConfigProblems(s) {
  const out = [];
  if (!s.credentialsB64) out.push('GADS_SERVICE_ACCOUNT_JSON_B64 is not set');
  else if (!parseServiceAccount(s.credentialsB64).ok) out.push('GADS_SERVICE_ACCOUNT_JSON_B64 is not a readable service-account key');
  if (s.cutoverError) out.push(s.cutoverError);
  if (!/^\d{10}$/.test(s.customerId)) out.push('GADS_CUSTOMER_ID is not a 10-digit Google Ads customer ID');
  if (!/^\d+$/.test(s.conversionActionId)) out.push('GADS_CONVERSION_ACTION_ID is not a numeric conversion action ID');
  return out;
}

/* ── Hosts and domains ────────────────────────────────────────────────── */


/* ── User data: email and phone, Google's normalisation ───────────────── */

function sha256Hex(s) {
  return crypto.createHash('sha256').update(s, 'utf8').digest('hex');
}

/* Google's rule, not Meta's: "Remove leading and trailing whitespace.
   Convert the entire email address to lowercase." And for gmail.com and
   googlemail.com only, remove every dot from the part before the @ and
   anything from a + onward -- "cloudy.sanfrancisco+shopping@gmail.com ->
   cloudysanfrancisco@gmail.com". Any other domain keeps its dots and plus.
   Intermediate whitespace is removed too ("Trim leading, trailing and
   intermediate whitespace"). Returns null when there is no usable address. */
function normaliseEmailForGoogle(email) {
  const e = String(email == null ? '' : email).replace(/\s+/g, '').toLowerCase();
  const at = e.lastIndexOf('@');
  if (at <= 0 || at === e.length - 1) return null;
  let local = e.slice(0, at);
  const domain = e.slice(at + 1);
  if (domain === 'gmail.com' || domain === 'googlemail.com') {
    local = local.split('+')[0].replace(/\./g, '');
    if (!local) return null;
  }
  return local + '@' + domain;
}

/* E.164 WITH THE PLUS, which is exactly what meta-capi.js's normalizePhone
   throws away -- Meta wants digits only, Google wants "+18005550100".
   Reusing Meta's normaliser here would hash a different string, match
   nobody, and raise no error anywhere. So this is its own function, and a
   test asserts the two disagree.

   Separators a person or a phone widget adds are removed; nothing else is
   repaired. A number with no + is NOT given a country code by guesswork:
   the form stores E.164 (all 2,679 stored phones were on 26 Sept 2026), and
   a guessed +1 on a UK number is a hash of somebody else's phone. */
function normalisePhoneForGoogle(phone) {
  const p = String(phone == null ? '' : phone).replace(/[\s().-]/g, '');
  return /^\+[1-9]\d{6,14}$/.test(p) ? p : null;
}

function googleUserIdentifiers({ email, phone }) {
  const out = [];
  const e = normaliseEmailForGoogle(email);
  if (e) out.push({ emailAddress: sha256Hex(e) });
  const p = normalisePhoneForGoogle(phone);
  if (p) out.push({ phoneNumber: sha256Hex(p) });
  return out;
}

/* ── Time ─────────────────────────────────────────────────────────────── */

/* RFC 3339 in America/New_York with THAT DAY's offset: -04:00 in summer,
   -05:00 in winter. A hardcoded -04:00 is an hour wrong all winter, and an
   hour early can put a conversion before its own click, which Google
   rejects outright. The instant is identical whatever the zone; Eastern is
   only how it reads in the Google Ads UI, which is how the team reads
   everything else. Seconds are truncated, never rounded up, so the
   conversion can never land after the instant we recorded. */
function etRfc3339(date) {
  const d = new Date(date);
  if (Number.isNaN(d.getTime())) return null;
  const secs = Math.floor(d.getTime() / 1000) * 1000;
  const parts = Object.fromEntries(new Intl.DateTimeFormat('en-US', {
    timeZone: 'America/New_York', hourCycle: 'h23',
    year: 'numeric', month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit', second: '2-digit',
  }).formatToParts(new Date(secs)).map((p) => [p.type, p.value]));
  const asUtc = Date.UTC(+parts.year, +parts.month - 1, +parts.day, +parts.hour, +parts.minute, +parts.second);
  const offMin = Math.round((asUtc - secs) / 60000);
  const sign = offMin < 0 ? '-' : '+';
  const a = Math.abs(offMin);
  return `${parts.year}-${parts.month}-${parts.day}T${parts.hour}:${parts.minute}:${parts.second}` +
         `${sign}${String(Math.floor(a / 60)).padStart(2, '0')}:${String(a % 60).padStart(2, '0')}`;
}

/* ── Click IDs ────────────────────────────────────────────────────────── */

const CLICK_ID_TYPES = ['gclid', 'gbraid', 'wbraid'];
/* NEWEST FIRST. Measured on 6 Oct 2026 over the 20 Google leads whose
   fields carried different click IDs, ordering each pair by when each ID
   was first seen: page_url was newer than landing_page in 16 of 17 (one
   tie), previous_page newer than landing_page in 1 of 3 (two ties), and no
   pair ever went the other way. landing_page is the FIRST page of the tab,
   kept by Webflow's sessionStorage, so it carries an older click from an
   earlier visit; page_url is the page the form loaded on. */
const CLICK_ID_FIELDS = ['page_url', 'previous_page', 'landing_page'];

/* Web-safe base64 and nothing else, and long enough to be whole. Before
   21 Aug 2026 the server cut URLs at 500 characters and left gclids as short
   as ONE character -- a cut ID is a different, wrong ID, so it is refused
   rather than sent. Since the cut moved to 1000 every stored gclid is 55-92
   characters. */
const CLICK_ID_SHAPE = /^[A-Za-z0-9_-]{20,512}$/;

function clickIdsInUrl(url) {
  const out = {};
  const s = String(url == null ? '' : url);
  for (const type of CLICK_ID_TYPES) {
    const m = new RegExp('[?&#]' + type + '=([^&#]*)').exec(s);
    if (!m) continue;
    let v = m[1];
    try { v = decodeURIComponent(v); } catch (e) { /* keep it raw; the shape test decides */ }
    out[type] = { value: v, valid: CLICK_ID_SHAPE.test(v) };
  }
  return out;
}

/* One candidate per field: gclid, else gbraid, else wbraid -- the first
   VALID one. A malformed gclid does not hide a valid gbraid behind it. */
function clickCandidates(lead) {
  const out = [];
  let malformed = 0;
  CLICK_ID_FIELDS.forEach((field, order) => {
    const ids = clickIdsInUrl(lead[field]);
    for (const type of CLICK_ID_TYPES) {
      if (!ids[type]) continue;
      if (!ids[type].valid) { malformed++; continue; }
      if (!out.some((c) => c.value === ids[type].value)) out.push({ field, order, type, value: ids[type].value });
      return;
    }
  });
  return { candidates: out, malformed };
}

/* THE CHOICE, given when each candidate was first seen anywhere.

   - a click must be seen no later than the booking, and within 90 days
     before it;
   - among those, the NEWEST click wins, across fields;
   - a tie goes to field order (page_url, previous_page, landing_page);
   - a click under 6 hours old is not skipped, it is WAITED for -- Google
     refuses it with TOO_RECENT_EVENT and "Retry after 6 hours have passed".

   "First seen" is when our own page first loaded with that ID: seconds
   after Google's real click time, never before it, and always defined,
   because the lead row itself carries the ID and has a created_at. */
function chooseClickId(cands, firstSeen, bookedAt, now) {
  const booked = new Date(bookedAt).getTime();
  if (!cands.length) return { skip: 'no_click_id' };
  const timed = cands.map((c) => ({ ...c, seenAt: firstSeen.get(c.value) || null }));
  const usable = timed.filter((c) => c.seenAt && c.seenAt.getTime() <= booked && booked - c.seenAt.getTime() <= GADS_MAX_CLICK_AGE_MS);
  if (!usable.length) {
    const anyOld = timed.some((c) => c.seenAt && booked - c.seenAt.getTime() > GADS_MAX_CLICK_AGE_MS);
    return { skip: anyOld ? 'too_old' : 'click_after_booking' };
  }
  usable.sort((a, b) => (b.seenAt.getTime() - a.seenAt.getTime()) || (a.order - b.order));
  const pick = usable[0];
  if (now - pick.seenAt.getTime() < GADS_MIN_CLICK_AGE_MS) return { wait: 'click_too_recent', pick };
  return { pick };
}

/* ── The event and the request ────────────────────────────────────────── */

/* ONE click ID per event, by instruction -- even though Google's own guide
   recommends sending a gclid and a gbraid together when both exist. That
   is a deliberate departure, recorded in the review card. */

/* CONSENT IS A CLAIM, so it is a switch and not a default. With
   GADS_CONSENT_GRANTED=true every event says the person granted both Ad User
   Data and Ad Personalization -- what Lorenzo's sheet sends today. Off, the
   field is ABSENT, never "denied" and never "unspecified": absent is what
   shipped in PR 129, and Google's own guide warns that without it "it's
   possible that your conversions won't be attributable".

   Field names and values from the Data Manager reference, read 6 Oct 2026:
   Consent is { "adUserData": ConsentStatus, "adPersonalization":
   ConsentStatus }, ConsentStatus is CONSENT_GRANTED, CONSENT_DENIED or
   CONSENT_STATUS_UNSPECIFIED. It can sit on the request or on each Event,
   and "User-level consent overrides request-level consent" -- it goes on the
   EVENT here, so it is part of the payload frozen at the claim and a retry
   sends exactly what the first attempt sent. */
const GADS_CONSENT_GRANTED = Object.freeze({ adUserData: 'CONSENT_GRANTED', adPersonalization: 'CONSENT_GRANTED' });

function buildGadsEvent({ sessionId, click, bookedAt, value, email, phone, consentGranted = false }) {
  const ev = {
    adIdentifiers:  { [click.type]: click.value },
    eventTimestamp: etRfc3339(bookedAt),
    transactionId:  String(sessionId),
    eventSource:    'WEB',
  };
  if (typeof value === 'number' && Number.isFinite(value) && value >= 0) {
    ev.conversionValue = value;
    ev.currency = 'USD';
  }
  const ids = googleUserIdentifiers({ email, phone });
  if (ids.length) ev.userData = { userIdentifiers: ids };
  /* Strictly true: anything else, including the string 'true', leaves the
     field out. A copy, so one event can never mutate another's consent. */
  if (consentGranted === true) ev.consent = { ...GADS_CONSENT_GRANTED };
  return ev;
}

function buildIngestRequest(event, settings, validateOnly) {
  return {
    destinations: [{
      operatingAccount: { accountType: 'GOOGLE_ADS', accountId: settings.customerId },
      productDestinationId: settings.conversionActionId,
      /* NO loginAccount, by instruction: the service account was added to
         5442288209 directly. Through a manager account this would have to
         name the manager, and leaving it out would then be refused. */
    }],
    encoding: 'HEX',
    validateOnly: validateOnly === true,
    events: [event],
  };
}

/* ── Credentials ──────────────────────────────────────────────────────── */

function parseServiceAccount(b64) {
  try {
    const j = JSON.parse(Buffer.from(String(b64 || ''), 'base64').toString('utf8'));
    if (j && j.type === 'service_account' && j.client_email && j.private_key && j.token_uri) {
      return { ok: true, key: j };
    }
    return { ok: false };
  } catch (e) {
    return { ok: false };
  }
}

const b64u = (b) => Buffer.from(b).toString('base64').replace(/=+$/, '').replace(/\+/g, '-').replace(/\//g, '_');

/* A signed JWT swapped for a one-hour token, cached until a minute before it
   expires. Nothing here is ever logged: not the key, not the assertion, not
   the token. An error carries the HTTP status and Google's error CODE only. */
function createTokenSource(fetchImpl, nowFn = Date.now) {
  let cached = null;
  return async function getToken(b64) {
    if (cached && cached.b64 === b64 && cached.expiresAt - 60000 > nowFn()) return cached.token;
    const sa = parseServiceAccount(b64);
    if (!sa.ok) throw Object.assign(new Error('service-account key is missing or unreadable'), { kind: 'config' });
    const k = sa.key;
    const iat = Math.floor(nowFn() / 1000);
    const head  = b64u(JSON.stringify({ alg: 'RS256', typ: 'JWT', kid: k.private_key_id }));
    const claim = b64u(JSON.stringify({ iss: k.client_email, scope: DATA_MANAGER_SCOPE, aud: k.token_uri, iat, exp: iat + 3600 }));
    const sig   = b64u(crypto.createSign('RSA-SHA256').update(head + '.' + claim).sign(k.private_key));
    const res = await fetchImpl(k.token_uri, {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: new URLSearchParams({ grant_type: 'urn:ietf:params:oauth:grant-type:jwt-bearer', assertion: `${head}.${claim}.${sig}` }).toString(),
    });
    let j = {};
    try { j = await res.json(); } catch (e) { /* a non-JSON reply is handled below */ }
    if (!res.ok || !j.access_token) {
      throw Object.assign(new Error(`token request refused: HTTP ${res.status} ${j.error || ''}`.trim()),
        { kind: res.status >= 500 ? 'retryable' : 'auth', status: res.status });
    }
    cached = { b64, token: j.access_token, expiresAt: nowFn() + (Number(j.expires_in) || 3600) * 1000 };
    return cached.token;
  };
}

/* ── Sending, and what a reply means ──────────────────────────────────── */

/* PERMANENT means "sending these bytes again cannot work": a malformed
   payload, or an action ID that does not exist (404
   INVALID_CONVERSION_ACTION_ID, measured 6 Oct 2026). RETRYABLE is a
   network failure, a timeout, a rate limit or Google having a bad minute.
   401 and 403 are retryable too -- a revoked key or a removed user is fixed
   in Google, not in the payload, and the next attempt after the fix goes
   through -- but they also page, through recordFailure's credential rule. */
function classifyIngestReply(status, body) {
  const err = (body && body.error) || {};
  const reasons = (err.details || []).map((d) => d && d.reason).filter(Boolean);
  for (const d of err.details || []) for (const v of (d && d.fieldViolations) || []) if (v.reason) reasons.push(v.reason);
  if (status >= 200 && status < 300) return { kind: 'ok', requestId: body && body.requestId, reasons };
  if (reasons.includes('INVALID_CONVERSION_ACTION_ID')) return { kind: 'permanent', action: true, reasons, message: err.message };
  if (status === 401 || status === 403) return { kind: 'retryable', auth: true, reasons, message: err.message };
  if (status === 408 || status === 429 || status >= 500) return { kind: 'retryable', reasons, message: err.message };
  return { kind: 'permanent', reasons, message: err.message };
}

async function postIngest(fetchImpl, token, request, timeoutMs = GADS_REQUEST_TIMEOUT_MS) {
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), timeoutMs);
  try {
    const res = await fetchImpl(DATA_MANAGER_INGEST_URL, {
      method: 'POST',
      headers: { Authorization: 'Bearer ' + token, 'Content-Type': 'application/json' },
      body: JSON.stringify(request),
      signal: ctrl.signal,
    });
    let body = null;
    try { body = await res.json(); } catch (e) { body = null; }
    return { status: res.status, body, ...classifyIngestReply(res.status, body) };
  } catch (e) {
    /* A timeout is the one reply that leaves us not knowing. Retried with the
       SAME transaction id and the SAME bytes, so if Google did receive it the
       retry is a duplicate it discards rather than a second conversion. */
    return { status: 0, body: null, kind: 'retryable', reasons: [], message: e && e.name === 'AbortError' ? 'timed out' : String(e && e.message || e) };
  } finally {
    clearTimeout(t);
  }
}

/* ── After a real send: did Google actually keep it? ──────────────────── */

/* A 200 FROM events:ingest IS NOT A CONVERSION. It means Google accepted the
   request; matching the click happens afterwards, and the outcome is only
   available from GET /v1/requestStatus:retrieve?requestId=... -- the same
   shape Meta taught this repo, where a 200 carried an empty messages array
   for months. Read on 7 Oct 2026 from the Data Manager reference:

   - one entry per destination, in request order. We send ONE destination and
     ONE event per request, so a request's status IS that event's result;
   - requestStatus is PROCESSING, SUCCESS, FAILED or PARTIAL_SUCCESS;
   - errorInfo.errorCounts[] and warningInfo.warningCounts[] carry
     { recordCount, reason }, and are empty while PROCESSING;
   - eventsIngestionStatus.recordCount is how many records Google received;
   - "the status can only be retrieved for requests that succeed and don't
     have validate_only=true" (Google's own 400, seen 6 Oct 2026), so a
     validate-only row is never asked about.

   The reference gives NO timing -- not how long PROCESSING lasts, not how
   long a request ID stays retrievable -- so the checks back off and give up
   after GADS_STATUS_GIVE_UP_MS with an alert, rather than guessing a number
   and calling silence a success. */
const DATA_MANAGER_STATUS_URL   = 'https://datamanager.googleapis.com/v1/requestStatus:retrieve';
const GADS_STATUS_FIRST_CHECK_MS = 30 * 60 * 1000;
const GADS_STATUS_BACKOFF_MS     = [30, 60, 180, 360, 720, 1440].map((m) => m * 60 * 1000);
const GADS_STATUS_GIVE_UP_MS     = 7 * 24 * 60 * 60 * 1000;

/* UNMATCHED: Google received the event and could not tie it to an ad click,
   so it counts for nothing. The click-shaped reasons, and only those. */
const GADS_UNMATCHED_REASONS = new Set([
  'PROCESSING_ERROR_REASON_CLICK_NOT_FOUND',
  'PROCESSING_ERROR_REASON_INVALID_CLICK',
  'PROCESSING_ERROR_REASON_INVALID_OPERATING_ACCOUNT_FOR_CLICK',
  'PROCESSING_ERROR_REASON_USER_ID_NOT_FOUND',
  'PROCESSING_ERROR_REASON_USER_ID_NOT_FOUND_FOR_GCLID',
  'PROCESSING_ERROR_REASON_MATCH_ID_NOT_FOUND',
]);
/* DUPLICATE: Google already holds this conversion -- our own earlier attempt
   after a timeout, or another source with the same order ID. Not a loss of
   the conversion, but still an event Google dropped, so it is recorded and
   reported in its own words rather than folded into "dropped". */
const GADS_DUPLICATE_REASONS = new Set([
  'PROCESSING_ERROR_REASON_DUPLICATE_GCLID',
  'PROCESSING_ERROR_REASON_DUPLICATE_TRANSACTION_ID',
]);

const countsByReason = (list) => {
  const out = {};
  for (const c of list || []) {
    if (!c || !c.reason) continue;
    out[c.reason] = (out[c.reason] || 0) + (Number(c.recordCount) || 0);
  }
  return out;
};

/* Google's reply -> our outcome, for a one-event request.
     accepted               SUCCESS, one record, no warnings
     accepted_with_warnings SUCCESS, one record, but parts of it were ignored
     unmatched              failed for a click-shaped reason
     duplicate              failed only because Google already has it
     dropped                failed for any other reason, or "succeeded" with
                            zero records received
     processing             not final yet: ask again later
     unreadable             no status we can read: ask again later, never
                            treated as an answer */
function classifyRequestStatus(body) {
  const d = body && Array.isArray(body.requestStatusPerDestination) ? body.requestStatusPerDestination[0] : null;
  if (!d || !d.requestStatus) return { final: false, outcome: 'unreadable', googleStatus: null, recordCount: null, errors: {}, warnings: {} };
  const googleStatus = d.requestStatus;
  const recordCount = d.eventsIngestionStatus && d.eventsIngestionStatus.recordCount != null ? Number(d.eventsIngestionStatus.recordCount) : null;
  const errors = countsByReason(d.errorInfo && d.errorInfo.errorCounts);
  const warnings = countsByReason(d.warningInfo && d.warningInfo.warningCounts);
  const base = { googleStatus, recordCount, errors, warnings };
  if (googleStatus === 'PROCESSING') return { ...base, final: false, outcome: 'processing' };
  if (googleStatus === 'SUCCESS') {
    /* "Check the record_count ... to confirm that the total number of records
       received matches your expectations." One event went in; zero received
       is an event that vanished, whatever the status word says. */
    if (recordCount === 0) return { ...base, final: true, outcome: 'dropped' };
    return { ...base, final: true, outcome: Object.keys(warnings).length ? 'accepted_with_warnings' : 'accepted' };
  }
  if (googleStatus === 'FAILED' || googleStatus === 'PARTIAL_SUCCESS') {
    const reasons = Object.keys(errors);
    if (reasons.some((r) => GADS_UNMATCHED_REASONS.has(r))) return { ...base, final: true, outcome: 'unmatched' };
    if (reasons.length && reasons.every((r) => GADS_DUPLICATE_REASONS.has(r))) return { ...base, final: true, outcome: 'duplicate' };
    return { ...base, final: true, outcome: 'dropped' };
  }
  return { ...base, final: false, outcome: 'unreadable' };
}

async function getRequestStatus(fetchImpl, token, requestId, timeoutMs = GADS_REQUEST_TIMEOUT_MS) {
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), timeoutMs);
  try {
    const res = await fetchImpl(DATA_MANAGER_STATUS_URL + '?requestId=' + encodeURIComponent(requestId), {
      method: 'GET',
      headers: { Authorization: 'Bearer ' + token },
      signal: ctrl.signal,
    });
    let body = null;
    try { body = await res.json(); } catch (e) { body = null; }
    return { status: res.status, body };
  } catch (e) {
    return { status: 0, body: null, error: e && e.name === 'AbortError' ? 'timed out' : String(e && e.message || e) };
  } finally {
    clearTimeout(t);
  }
}

/* ── The uploader ─────────────────────────────────────────────────────── */

/* Columns the evaluation reads. Named once so the sweep, the retry re-read
   and the dry run cannot select different shapes. */
const LEAD_COLUMNS = `l.session_id, l.email, l.phone, l.website, l.page_url, l.previous_page, l.landing_page,
  l.created_at, l.booked_at, l.disqualified, l.non_icp_blocked, l.non_icp_llm_flagged,
  l.website_check_failed, l.website_check_reason, l.product_interest, l.utm_campaign, l.utm_medium,
  l.offer_campaign, l.offer_medium`;

/* Earliest sighting of one click ID anywhere: a page load, a session's first
   page, or any lead row. strpos, not LIKE -- an underscore is a LIKE
   wildcard and gclids are full of them. LEAST ignores NULLs. */
const FIRST_SEEN_SQL = `
  SELECT LEAST(
    (SELECT min(created_at) FROM form_page_views WHERE strpos(page_url, $1) > 0),
    (SELECT min(created_at) FROM form_sessions   WHERE strpos(page_url, $1) > 0),
    (SELECT min(created_at) FROM leads
      WHERE strpos(landing_page, $1) > 0 OR strpos(page_url, $1) > 0 OR strpos(previous_page, $1) > 0)
  ) AS first_seen`;

function createGadsUploader(deps) {
  const {
    pool, sourceSql,
    isInternalLead, isStagingSubmission, isInternalSubmission,
    isWebsiteVerified, freeEmailMatch, nonIcpFresh, valueFor,
    alertOps = () => {}, recordFailure = () => {}, recordSuccess = () => {},
    log = console, env = process.env, now = Date.now, fetchImpl = (...a) => fetch(...a),
  } = deps;
  const getToken = createTokenSource(fetchImpl, now);
  let running = false;

  /* Google Ads leads, by the ONE source rule the Dropoff tab and the digest
     use (DROPOFF_SOURCE_SQL = 'Google'): utm_source google, or no utm at all
     and the form recorded "Google Ads" from the referrer. Not restated. */
  const googleSql = `(${sourceSql}) = 'Google'`;

  async function firstSeenMap(candidates) {
    const m = new Map();
    for (const c of candidates) {
      const r = await pool.query(FIRST_SEEN_SQL, [c.value]);
      const v = r.rows[0] && r.rows[0].first_seen;
      if (v) m.set(c.value, new Date(v));
    }
    return m;
  }

  /* EVERY GATE, IN ONE ORDER, and the first that matches is the recorded
     reason. Checked at upload time, never cached from /submit. Returns
     { reason, detail } to skip, { wait } to try again next sweep, or null. */
  async function gate(lead, s) {
    const email = lead.email || '';
    if (isInternalSubmission(email, lead.page_url)) {
      return { reason: isInternalLead(email) ? 'internal' : 'staging', detail: isInternalLead(email) ? 'internal or test address' : 'staging site' };
    }
    for (const field of ['page_url', 'landing_page']) {
      const hit = gadsMatchList(gadsHostOf(lead[field]), s.excludedHosts);
      if (hit) return { reason: 'staging', detail: `${field} host matches ${hit}` };
    }
    const emailHit = gadsMatchList(gadsHostOf(email), s.excludedDomains);
    if (emailHit) return { reason: 'agency', detail: `email domain matches ${emailHit}` };
    const siteHit = gadsMatchList(gadsHostOf(lead.website), s.excludedDomains);
    if (siteHit) return { reason: 'agency', detail: `website matches ${siteHit}` };
    /* Stated explicitly, not left to the flow: a disqualified lead should
       never have booked, and the CLAUDE.md audit of every disqualified
       guard exists because "it never happens" kept turning out false. */
    if (lead.disqualified === true) return { reason: 'disqualified', detail: 'sells to consumers or chose the waitlist' };
    /* non_icp_llm_flagged is read HERE, in code, at upload time, and never
       as a SQL filter. tests/test-non-icp.js section 10f forbids a flagged
       lead being kept out of Salesforce, PartnerStack, the SDR list or the
       dialer; this is an AD SIGNAL, the same kind of thing as Meta, which
       already withholds flagged leads -- and the instruction for Google was
       to skip "blocked or flagged". Unlike Meta it does not depend on
       NON_ICP_LLM_META: the flag is a fact about the lead, and whether to
       reshape Meta's audience is a separate switch for a separate system. */
    if (lead.non_icp_blocked === true) return { reason: 'non_icp', detail: 'blocked' };
    if (lead.non_icp_llm_flagged === true) return { reason: 'non_icp', detail: 'model flagged the industry' };
    /* THE FRESH READ, because the column above is stamped at /submit from a
       cache that can still be cold. 2.6 and 11.7 seconds late on 18 Sept.
       FAILS CLOSED, unlike the lead path: a throw here costs a ten-minute
       delay, and sending a realtor to Google's bidder cannot be undone. */
    let fresh;
    try {
      fresh = await nonIcpFresh({ email, website: lead.website });
    } catch (e) {
      return { wait: 'non_icp_read_failed' };
    }
    if (fresh) return { reason: 'non_icp', detail: `fresh verdict: ${fresh.action}${fresh.business_type ? ' (' + fresh.business_type + ')' : ''}` };
    if (!isWebsiteVerified(lead)) return { reason: 'website', detail: lead.website_check_failed === true ? 'website check failed' : `unverified (${lead.website_check_reason || 'unknown'})` };
    if (s.excludeFreeEmail) {
      const fm = freeEmailMatch(email.slice(email.lastIndexOf('@') + 1));
      if (fm) return { reason: 'free_email', detail: fm.exact ? fm.domain : `typo of ${fm.domain}` };
    }
    return null;
  }

  /* The whole decision for one lead, writing nothing. */
  async function evaluate(lead, s) {
    const booked = lead.booked_at ? new Date(lead.booked_at) : null;
    if (!booked) return { wait: 'no_booking_time' };
    if (s.cutover && booked.getTime() < s.cutover.getTime()) return { skip: 'before_cutover' };
    if (now() - booked.getTime() < GADS_HOLD_MS) return { wait: 'hold' };
    const g = await gate(lead, s);
    if (g && g.wait) return { wait: g.wait };
    if (g) return { skip: g.reason, detail: g.detail };
    const { candidates, malformed } = clickCandidates(lead);
    const choice = chooseClickId(candidates, await firstSeenMap(candidates), booked, now());
    if (choice.skip) return { skip: choice.skip, detail: malformed ? `${malformed} malformed click ID(s)` : null };
    if (choice.wait) return { wait: choice.wait };
    const value = valueFor(lead);
    const event = buildGadsEvent({ sessionId: lead.session_id, click: choice.pick, bookedAt: booked, value, email: lead.email, phone: lead.phone, consentGranted: s.consentGranted });
    return { send: { event, click: choice.pick, value: event.conversionValue == null ? null : event.conversionValue, booked } };
  }

  /* ── table writes. Each is a single statement; the claim is the PRIMARY
        KEY plus a conditional UPDATE, so two sweeps -- or a sweep and a
        deploy's boot run -- can never both send one lead. ── */

  async function recordSkip(sessionId, v, s, fromValidated) {
    if (fromValidated) {
      await pool.query(
        `UPDATE ${TABLE} SET status = 'skipped', skip_reason = $2, skip_detail = $3, cutover_at = $4, updated_at = NOW()
          WHERE session_id = $1 AND status = 'validated'`,
        [sessionId, v.skip, v.detail || null, s.cutover]);
      return;
    }
    await pool.query(
      `INSERT INTO ${TABLE} (session_id, status, skip_reason, skip_detail, cutover_at)
       VALUES ($1, 'skipped', $2, $3, $4)
       ON CONFLICT (session_id) DO NOTHING`,
      [sessionId, v.skip, v.detail || null, s.cutover]);
  }

  async function claimNew(sessionId, send, s, fromValidated) {
    const params = [sessionId, send.click.type, send.click.value, send.click.field, send.click.seenAt,
      send.booked, send.event.eventTimestamp, send.value, send.value == null ? null : 'USD',
      send.event.transactionId, JSON.stringify(send.event), s.cutover];
    const r = fromValidated
      ? await pool.query(
        `UPDATE ${TABLE} SET status = 'sending', skip_reason = NULL, skip_detail = NULL,
                click_id_type = $2, click_id = $3, click_field = $4, click_first_seen_at = $5, booked_at = $6,
                event_timestamp = $7, conversion_value = $8, currency = $9, transaction_id = $10,
                payload = $11, cutover_at = $12, claimed_at = NOW(), updated_at = NOW()
          WHERE session_id = $1 AND status = 'validated'
          RETURNING session_id`, params)
      : await pool.query(
        `INSERT INTO ${TABLE} (session_id, status, click_id_type, click_id, click_field, click_first_seen_at,
                booked_at, event_timestamp, conversion_value, currency, transaction_id, payload, cutover_at, claimed_at)
         VALUES ($1, 'sending', $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, NOW())
         ON CONFLICT (session_id) DO NOTHING
         RETURNING session_id`, params);
    return r.rowCount === 1;
  }

  async function recordOutcome(sessionId, reply, attemptsBefore, validateOnly) {
    const attempts = attemptsBefore + 1;
    const errText = reply.kind === 'ok' ? null
      : `HTTP ${reply.status}${reply.reasons && reply.reasons.length ? ' ' + reply.reasons.join(',') : ''}${reply.message ? ': ' + String(reply.message).slice(0, 300) : ''}`;
    if (reply.kind === 'ok') {
      await pool.query(
        `UPDATE ${TABLE} SET status = $2, attempts = $3, request_id = $4, validate_only = $5,
                last_error = NULL, last_error_kind = NULL, next_attempt_at = NULL,
                sent_at = CASE WHEN $2 = 'sent' THEN NOW() ELSE sent_at END, updated_at = NOW()
          WHERE session_id = $1`,
        [sessionId, validateOnly ? 'validated' : 'sent', attempts, reply.requestId || null, validateOnly]);
      recordSuccess('Google Ads');
      return validateOnly ? 'validated' : 'sent';
    }
    const exhausted = reply.kind === 'retryable' && attempts >= GADS_MAX_ATTEMPTS;
    const status = reply.kind === 'retryable' && !exhausted ? 'failed_retryable' : 'failed_permanent';
    const backoff = GADS_RETRY_BACKOFF_MS[Math.min(attempts - 1, GADS_RETRY_BACKOFF_MS.length - 1)];
    await pool.query(
      `UPDATE ${TABLE} SET status = $2, attempts = $3, last_error = $4, last_error_kind = $5, validate_only = $6,
              next_attempt_at = CASE WHEN $2 = 'failed_retryable' THEN NOW() + ($7::bigint * INTERVAL '1 millisecond') ELSE NULL END,
              updated_at = NOW()
        WHERE session_id = $1`,
      [sessionId, status, attempts, errText, exhausted ? 'retries_exhausted' : reply.kind, validateOnly, backoff]);
    if (reply.action) {
      /* The one failure that is wrong for EVERY lead at once. */
      alertOps('critical', 'Google Ads', 'Conversion action not found', {
        'Session': sessionId, 'Error': errText,
        'What this means': 'Google says the conversion action ID in GADS_CONVERSION_ACTION_ID does not exist in this account. Nothing will upload until it is corrected.',
        'Action': 'Check the ID of "CRM - Qualified Demo Request" in Google Ads (Goals, Conversions, the ctId in the address bar) and fix the Railway variable. This lead is not retried.',
      });
    } else if (exhausted) {
      alertOps('warning', 'Google Ads', 'Conversion upload gave up after retries', {
        'Session': sessionId, 'Attempts': String(attempts), 'Error': errText,
        'Impact': 'This booking is not in Google Ads. It can be re-sent by setting its row back to failed_retryable once the cause is fixed.',
      });
    } else {
      recordFailure('Google Ads', sessionId, errText);
    }
    return status;
  }

  async function send(sessionId, event, attemptsBefore, s) {
    let token;
    try {
      token = await getToken(s.credentialsB64);
    } catch (e) {
      const reply = { kind: e.kind === 'config' ? 'permanent' : 'retryable', status: e.status || 0, reasons: [], message: e.message };
      if (e.kind === 'auth') reply.message = 'Authentication failed: ' + e.message;
      return recordOutcome(sessionId, reply, attemptsBefore, s.validateOnly);
    }
    const reply = await postIngest(fetchImpl, token, buildIngestRequest(event, s, s.validateOnly));
    return recordOutcome(sessionId, reply, attemptsBefore, s.validateOnly);
  }

  async function readLead(sessionId) {
    const r = await pool.query(`SELECT ${LEAD_COLUMNS} FROM leads l WHERE l.session_id = $1`, [sessionId]);
    return r.rows[0] || null;
  }

  /* Retries and stale claims. The PAYLOAD is frozen -- same click, same
     time, same value, same hashes -- but the GATES are asked again, so a
     non-ICP verdict that landed since the last attempt still stops it. */
  async function runRetries(s, tally) {
    const due = await pool.query(
      `SELECT session_id, attempts FROM ${TABLE}
        WHERE (status = 'failed_retryable' AND next_attempt_at <= NOW())
           OR (status = 'sending' AND claimed_at < NOW() - ($1::bigint * INTERVAL '1 millisecond'))
        ORDER BY COALESCE(next_attempt_at, claimed_at) LIMIT ${GADS_BATCH_LIMIT}`,
      [GADS_STALE_CLAIM_MS]);
    for (const row of due.rows) {
      const claim = await pool.query(
        `UPDATE ${TABLE} SET status = 'sending', claimed_at = NOW(), updated_at = NOW()
          WHERE session_id = $1
            AND ((status = 'failed_retryable' AND next_attempt_at <= NOW())
              OR (status = 'sending' AND claimed_at < NOW() - ($2::bigint * INTERVAL '1 millisecond')))
          RETURNING payload, booked_at, attempts`,
        [row.session_id, GADS_STALE_CLAIM_MS]);
      if (claim.rowCount !== 1) continue;
      const c = claim.rows[0];
      const lead = await readLead(row.session_id);
      if (!lead || (s.cutover && new Date(c.booked_at).getTime() < s.cutover.getTime())) {
        await pool.query(`UPDATE ${TABLE} SET status = 'skipped', skip_reason = $2, updated_at = NOW() WHERE session_id = $1`,
          [row.session_id, lead ? 'before_cutover' : 'lead_missing']);
        tally.skipped++;
        continue;
      }
      const g = await gate(lead, s);
      if (g && g.wait) {
        await pool.query(`UPDATE ${TABLE} SET status = 'failed_retryable', next_attempt_at = NOW() + INTERVAL '10 minutes', updated_at = NOW() WHERE session_id = $1`, [row.session_id]);
        continue;
      }
      if (g) {
        await pool.query(`UPDATE ${TABLE} SET status = 'skipped', skip_reason = $2, skip_detail = $3, updated_at = NOW() WHERE session_id = $1`,
          [row.session_id, g.reason, g.detail || null]);
        tally.skipped++;
        continue;
      }
      const out = await send(row.session_id, JSON.parse(c.payload), Number(c.attempts) || 0, s);
      tally[out] = (tally[out] || 0) + 1;
    }
  }

  async function runNew(s, tally) {
    /* validated rows are picked up again only when validate-only is OFF:
       a row that was only ever checked has not been sent, and switching to
       real sending must send it -- after asking every gate again. */
    const r = await pool.query(
      `SELECT ${LEAD_COLUMNS}, g.status AS upload_status
         FROM leads l
         LEFT JOIN ${TABLE} g ON g.session_id = l.session_id
        WHERE l.booking_uid IS NOT NULL
          AND ${googleSql}
          AND l.booked_at IS NOT NULL
          AND l.booked_at >= $1
          AND l.booked_at <= NOW() - ($2::bigint * INTERVAL '1 millisecond')
          AND (g.session_id IS NULL OR (g.status = 'validated' AND $3::boolean IS FALSE))
        ORDER BY l.booked_at
        LIMIT ${GADS_BATCH_LIMIT}`,
      [s.cutover, GADS_HOLD_MS, s.validateOnly]);
    for (const lead of r.rows) {
      const fromValidated = lead.upload_status === 'validated';
      const v = await evaluate(lead, s);
      if (v.wait) { tally.waiting++; continue; }
      if (v.skip) { await recordSkip(lead.session_id, v, s, fromValidated); tally.skipped++; continue; }
      if (!(await claimNew(lead.session_id, v.send, s, fromValidated))) continue;   // someone else has it
      const out = await send(lead.session_id, v.send.event, 0, s);
      tally[out] = (tally[out] || 0) + 1;
    }
  }

  /* ── After a real send: ask Google what became of each event ──
     Only rows that were REALLY sent: status 'sent', validate_only false, a
     request ID. A validate-only row cannot be asked about at all. */
  const OUTCOME_ALERTS = {
    unmatched: ['Conversion not matched to a click', 'Google received this booking but could not tie it to an ad click, so it counts for nothing in Google Ads.'],
    duplicate: ['Conversion dropped as a duplicate', 'Google already holds this conversion. Usually an earlier attempt of ours after a timeout -- but if this booking was only ever sent once, another source (the sheet?) uploaded it too.'],
    dropped:   ['Conversion dropped by Google', 'Google received the request but did not record the conversion.'],
    unknown:   ['Conversion status never arrived', 'Google never reported a final status for this booking, so we do not know whether it counts.'],
  };

  async function recordStatusFinal(row, outcome, cls, note) {
    /* CONDITIONAL on google_outcome IS NULL, and the alert fires only if THIS
       write won -- so two overlapping sweeps can never alert twice for one
       booking. */
    const r = await pool.query(
      `UPDATE ${TABLE} SET google_status = $2, google_outcome = $3, google_record_count = $4,
              google_errors = $5, google_warnings = $6, google_status_attempts = google_status_attempts + 1,
              google_status_checked_at = NOW(), google_status_final_at = NOW(), google_status_next_check_at = NULL,
              google_status_last_error = $7, updated_at = NOW()
        WHERE session_id = $1 AND google_outcome IS NULL`,
      [row.session_id, cls.googleStatus || null, outcome, cls.recordCount == null ? null : cls.recordCount,
        JSON.stringify(cls.errors || {}), JSON.stringify(cls.warnings || {}), note || null]);
    if (r.rowCount !== 1) return false;
    if (OUTCOME_ALERTS[outcome]) {
      const [title, meaning] = OUTCOME_ALERTS[outcome];
      const reasons = Object.keys(cls.errors || {}).map((x) => x.replace(/^PROCESSING_ERROR_REASON_/, '')).join(', ');
      alertOps('warning', 'Google Ads', title, {
        'Session': row.session_id,
        'Booked': row.booked_at ? etRfc3339(row.booked_at) : 'unknown',
        'Click': row.click_id_type || 'unknown',
        'Google status': cls.googleStatus || 'none',
        'Reason': reasons || note || 'none given',
        'What this means': meaning,
      });
    }
    return true;
  }

  async function scheduleStatusRetry(row, attemptsBefore, errText, cls) {
    const wait = GADS_STATUS_BACKOFF_MS[Math.min(attemptsBefore, GADS_STATUS_BACKOFF_MS.length - 1)];
    await pool.query(
      `UPDATE ${TABLE} SET google_status = $2, google_status_attempts = google_status_attempts + 1,
              google_status_checked_at = NOW(),
              google_status_next_check_at = NOW() + ($3::bigint * INTERVAL '1 millisecond'),
              google_status_last_error = $4, updated_at = NOW()
        WHERE session_id = $1 AND google_outcome IS NULL`,
      [row.session_id, (cls && cls.googleStatus) || null, wait, errText || null]);
  }

  async function runStatusChecks(s, tally) {
    const due = await pool.query(
      `SELECT session_id, request_id, sent_at, booked_at, click_id_type, google_status_attempts
         FROM ${TABLE}
        WHERE status = 'sent' AND validate_only IS FALSE AND request_id IS NOT NULL
          AND google_outcome IS NULL
          AND sent_at <= NOW() - ($1::bigint * INTERVAL '1 millisecond')
          AND (google_status_next_check_at IS NULL OR google_status_next_check_at <= NOW())
        ORDER BY sent_at
        LIMIT ${GADS_BATCH_LIMIT}`,
      [GADS_STATUS_FIRST_CHECK_MS]);
    if (!due.rows.length) return;
    let token;
    try {
      token = await getToken(s.credentialsB64);
    } catch (e) {
      recordFailure('Google Ads', 'status check', (e.kind === 'auth' ? 'Authentication failed: ' : '') + e.message);
      return;
    }
    for (const row of due.rows) {
      const attempts = Number(row.google_status_attempts) || 0;
      const reply = await getRequestStatus(fetchImpl, token, row.request_id);
      tally.status_checked++;
      if (reply.status === 200) {
        const cls = classifyRequestStatus(reply.body);
        if (cls.final) {
          if (await recordStatusFinal(row, cls.outcome, cls)) tally[`status_${cls.outcome}`] = (tally[`status_${cls.outcome}`] || 0) + 1;
          continue;
        }
        if (now() - new Date(row.sent_at).getTime() > GADS_STATUS_GIVE_UP_MS) {
          if (await recordStatusFinal(row, 'unknown', cls, `still ${cls.outcome} after ${Math.round(GADS_STATUS_GIVE_UP_MS / 86400000)} days`)) tally.status_unknown = (tally.status_unknown || 0) + 1;
          continue;
        }
        await scheduleStatusRetry(row, attempts, null, cls);
        continue;
      }
      const err = reply.body && reply.body.error;
      const errText = `HTTP ${reply.status}${err && err.status ? ' ' + err.status : ''}${reply.error ? ': ' + reply.error : err && err.message ? ': ' + String(err.message).slice(0, 200) : ''}`;
      if (reply.status === 401 || reply.status === 403) recordFailure('Google Ads', 'status check ' + row.session_id, errText);
      if (now() - new Date(row.sent_at).getTime() > GADS_STATUS_GIVE_UP_MS) {
        if (await recordStatusFinal(row, 'unknown', { googleStatus: null, errors: {}, warnings: {} }, errText)) tally.status_unknown = (tally.status_unknown || 0) + 1;
        continue;
      }
      await scheduleStatusRetry(row, attempts, errText, null);
    }
  }

  let _configAlerted = '';
  async function runSweep() {
    if (running) return { busy: true };
    running = true;
    const tally = { sent: 0, validated: 0, skipped: 0, waiting: 0, failed_retryable: 0, failed_permanent: 0, status_checked: 0 };
    try {
      const s = gadsSettings(env);
      if (!s.enabled) return { off: true };
      const problems = gadsConfigProblems(s);
      if (problems.length) {
        /* ON BUT UNABLE TO RUN is loud; OFF is silent. Re-alerting is left
           to alertOps's own cooldown; the log line is once per change. */
        const key = problems.join('; ');
        if (key !== _configAlerted) { _configAlerted = key; (log.warn || log.log)('[Google Ads] ⚠ Upload is ON but cannot run: ' + key); }
        alertOps('critical', 'Google Ads', 'Conversion upload is on but cannot run', {
          'Problem': key,
          'Impact': 'No booking is being sent to Google Ads. Nothing is lost: once fixed, the sweep picks up every booking since the cutover.',
        });
        return { problems };
      }
      _configAlerted = '';
      await runRetries(s, tally);
      await runNew(s, tally);
      /* Last, and whatever the validate switch says: rows really sent earlier
         still need their outcome even if sending has been switched back. */
      await runStatusChecks(s, tally);
      const moved = tally.sent + tally.validated + tally.skipped + tally.failed_retryable + tally.failed_permanent + tally.status_checked;
      if (moved) (log.log || log.info)(`[Google Ads] Sweep (${s.validateOnly ? 'validate-only' : 'REAL'}): ${JSON.stringify(tally)}`);
      return tally;
    } catch (err) {
      recordFailure('Google Ads', 'sweep', err && err.message);
      (log.warn || log.log)('[Google Ads] Sweep failed (non-blocking): ' + (err && err.message));
      return { error: err && err.message };
    } finally {
      running = false;
    }
  }

  /* READ-ONLY. What the sweep would decide for every Google Ads booking in a
     window, ignoring the cutover and anything already in the table, with no
     write and no network. Used by tools/gads-upload-dry-run.js; it never
     names the upload table, so it runs before the migration has deployed. */
  async function dryRun({ sinceDays = 90 } = {}) {
    const s = { ...gadsSettings(env), cutover: null };
    const r = await pool.query(
      `SELECT ${LEAD_COLUMNS}
         FROM leads l
        WHERE l.booking_uid IS NOT NULL
          AND ${googleSql}
          AND COALESCE(l.booked_at, l.created_at) >= NOW() - ($1::int * INTERVAL '1 day')
        ORDER BY COALESCE(l.booked_at, l.created_at)`,
      [sinceDays]);
    const out = { considered: r.rows.length, send: 0, skip: {}, skipDetail: {}, wait: {}, value: {}, clickType: {}, clickField: {}, withPhone: 0, malformedIds: 0, noBookedAt: 0 };
    for (const lead of r.rows) {
      if (!lead.booked_at) { out.noBookedAt++; lead.booked_at = lead.created_at; }
      out.malformedIds += clickCandidates(lead).malformed;
      const v = await evaluate(lead, s);
      if (v.wait) { out.wait[v.wait] = (out.wait[v.wait] || 0) + 1; continue; }
      if (v.skip) {
        out.skip[v.skip] = (out.skip[v.skip] || 0) + 1;
        /* Details are reason codes and industries only -- never an address,
           a phone, a click ID or a URL. */
        const k = v.skip + (v.detail ? ': ' + v.detail : '');
        out.skipDetail[k] = (out.skipDetail[k] || 0) + 1;
        continue;
      }
      out.send++;
      const val = v.send.value == null ? 'none' : String(v.send.value);
      out.value[val] = (out.value[val] || 0) + 1;
      out.clickType[v.send.click.type] = (out.clickType[v.send.click.type] || 0) + 1;
      out.clickField[v.send.click.field] = (out.clickField[v.send.click.field] || 0) + 1;
      if ((v.send.event.userData && v.send.event.userData.userIdentifiers || []).some((u) => u.phoneNumber)) out.withPhone++;
    }
    return out;
  }

  return { runSweep, dryRun, evaluate, gate, runStatusChecks };
}

function startGadsUploadSweep(uploader, env = process.env, log = console) {
  const s = gadsSettings(env);
  if (!s.enabled) {
    log.log('[Google Ads] Conversion upload is OFF (GADS_UPLOAD_ENABLED is not "true") — nothing will be sent');
    return null;
  }
  log.log(`[Google Ads] Conversion upload ON, ${s.validateOnly ? 'VALIDATE-ONLY (nothing is recorded by Google)' : 'SENDING FOR REAL'}, ` +
    `account ${s.customerId}, action ${s.conversionActionId}, cutover ${s.cutover ? s.cutover.toISOString() : 'UNSET — nothing will send'}, ` +
    `free email ${s.excludeFreeEmail ? 'excluded' : 'included'}, consent ${s.consentGranted ? 'GRANTED on every event' : 'not sent'}, ` +
    `sweep every ${GADS_SWEEP_INTERVAL_MS / 60000} min`);
  const run = () => uploader.runSweep().catch((e) => log.warn('[Google Ads] Sweep error (non-blocking):', e && e.message));
  run();
  const t = setInterval(run, GADS_SWEEP_INTERVAL_MS);
  if (t.unref) t.unref();
  return t;
}

module.exports = {
  createGadsUploader,
  startGadsUploadSweep,
  gadsSettings,
  gadsConfigProblems,
  parseCutover,
  gadsHostOf,
  gadsHostMatches,
  gadsMatchList,
  normaliseEmailForGoogle,
  normalisePhoneForGoogle,
  googleUserIdentifiers,
  sha256Hex,
  etRfc3339,
  clickIdsInUrl,
  clickCandidates,
  chooseClickId,
  buildGadsEvent,
  GADS_CONSENT_GRANTED,
  buildIngestRequest,
  parseServiceAccount,
  createTokenSource,
  classifyIngestReply,
  postIngest,
  classifyRequestStatus,
  getRequestStatus,
  GADS_UNMATCHED_REASONS,
  GADS_DUPLICATE_REASONS,
  GADS_STATUS_FIRST_CHECK_MS,
  GADS_STATUS_BACKOFF_MS,
  GADS_STATUS_GIVE_UP_MS,
  DATA_MANAGER_STATUS_URL,
  CLICK_ID_FIELDS,
  CLICK_ID_TYPES,
  SKIP_REASONS,
  GADS_DEFAULT_EXCLUDED_DOMAINS,
  GADS_DEFAULT_EXCLUDED_HOSTS,
  GADS_HOLD_MS,
  GADS_MIN_CLICK_AGE_MS,
  GADS_MAX_CLICK_AGE_MS,
  GADS_MAX_ATTEMPTS,
  DATA_MANAGER_INGEST_URL,
  DATA_MANAGER_SCOPE,
  TABLE,
};
