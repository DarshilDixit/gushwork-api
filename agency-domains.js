/* ============================================================
   AGENCY DOMAINS — one list of the agencies that run our campaigns, and
   the one place that decides whether a lead's email or website matches it.

   Flighted and Upraw submit the form while setting campaigns up. Their
   leads are real rows, but they must not feed Meta's audience, Google's
   bidding, Salesforce or an SDR's call list. Until 7 Oct 2026 each of those
   had its own list (META_EXCLUDED_DOMAINS, GADS_EXCLUDED_DOMAINS) and
   Salesforce had none, so adding an agency meant remembering three places.

   AGENCY_DOMAINS is now the shared list. META_EXCLUDED_DOMAINS and
   GADS_EXCLUDED_DOMAINS still work and EXTEND it for their own system only
   -- so with AGENCY_DOMAINS unset every list is exactly what it was, and a
   system can still carry an extra domain the others do not need. Every list
   EXTENDS its defaults and can never shrink them: a typo in Railway cannot
   quietly let an agency back in.

   NOT the dialer. sdr-calling (Ishaan's repo) keeps its own deny list in
   lib/qualification-lists.json, which already has both agencies. Two repos,
   two lists, and nothing here can read the other.

   THE MATCHING HELPERS LIVE HERE, moved unchanged from
   google-ads-conversions.js, which re-exports them under their old names.
   One host normaliser for every list, so no two systems can disagree about
   what "matches" means -- the rule hostMatchesDomain in index.js taught
   this repo with paycompass.com: exact or a real subdomain, never a
   substring.
   ============================================================ */

const AGENCY_DEFAULT_DOMAINS = Object.freeze(['flighted.co', 'uprawmedia.com']);

/* A host from an email, a URL or a bare domain: lowercase, no scheme, no
   userinfo, no path, no port, no www, no trailing dot. Subdomains are KEPT,
   because matching is exact-or-subdomain and collapsing first would make
   "agents.flighted.co" and "flighted.co" the same string by accident
   rather than by rule. Same stripping steps as nonIcpHostForms, without
   its free-mailbox refusal (an agency could in principle sit on any host). */
function hostOf(raw) {
  let h = String(raw == null ? '' : raw).trim().toLowerCase();
  if (!h) return '';
  if (h.includes('@') && !/^[a-z][a-z0-9+.-]*:\/\//.test(h)) h = h.slice(h.lastIndexOf('@') + 1);
  h = h.replace(/^[a-z][a-z0-9+.-]*:\/\//, '')
       .replace(/^[^/@]*@/, '')
       .split(/[/?#]/)[0]
       .split(':')[0]
       .replace(/^www\./, '')
       .replace(/\.$/, '');
  return h;
}

/* Exact or a real subdomain -- never a substring. "notflighted.co" and
   "flighted.co.example.com" are not Flighted. */
function hostMatches(host, domain) {
  if (!host || !domain) return false;
  return host === domain || (host.length > domain.length && host.endsWith('.' + domain));
}

function matchList(host, list) {
  return (list || []).find((d) => hostMatches(host, d)) || null;
}

/* A comma-separated env value as a clean list of domains. */
function parseDomainList(raw) {
  return String(raw || '').split(',')
    .map((x) => x.trim().toLowerCase().replace(/^www\./, '').replace(/\.$/, ''))
    .filter(Boolean);
}

/* The shared list: the defaults plus AGENCY_DOMAINS. */
function agencyDomains(env = process.env) {
  return [...new Set([...AGENCY_DEFAULT_DOMAINS, ...parseDomainList(env.AGENCY_DOMAINS)])];
}

/* The shared list plus one system's own extras (META_EXCLUDED_DOMAINS,
   GADS_EXCLUDED_DOMAINS). Order is stable: defaults, AGENCY_DOMAINS, extras. */
function agencyDomainsPlus(extraRaw, env = process.env) {
  return [...new Set([...agencyDomains(env), ...parseDomainList(extraRaw)])];
}

/* { domain, via } when the email's domain or the website's host is on
   `list`, otherwise null. Email first: it is the address the lead gave us. */
function domainMatch({ email, website } = {}, list) {
  const byEmail = matchList(hostOf(email), list);
  if (byEmail) return { domain: byEmail, via: 'email' };
  const bySite = matchList(hostOf(website), list);
  if (bySite) return { domain: bySite, via: 'website' };
  return null;
}

/* Against the shared list only. Salesforce and the SDR list use this. */
function agencyDomainMatch(lead = {}, env = process.env) {
  return domainMatch(lead, agencyDomains(env));
}

module.exports = {
  AGENCY_DEFAULT_DOMAINS,
  hostOf,
  hostMatches,
  matchList,
  parseDomainList,
  agencyDomains,
  agencyDomainsPlus,
  domainMatch,
  agencyDomainMatch,
};
