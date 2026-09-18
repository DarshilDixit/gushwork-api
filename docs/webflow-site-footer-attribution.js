/* A COPY FOR READING, NOT THE SOURCE. The live version of this is in
   Webflow global site settings -> Custom Code -> Footer, and that is the
   only place editing it has any effect. Kept here because nothing else in
   git records what that script does, and it is the thing that populates
   the sessionStorage keys gushwork-form.js reads.

   Snapshotted 19 Sept 2026. If you change it in Webflow, update this too
   or delete it -- a stale copy is worse than none. Deliberately NOT a
   test fixture: a suite asserting against this file would be testing a
   duplicate of the source, which is the failure mode CLAUDE.md warns
   about throughout. */

(function() {
  var GW_ATTR_MIRROR = [
    ['gw_referrer',     'gwa_referrer'],
    ['gw_landing_page', 'gwa_landing_page'],
    ['gw_utm_source',   'gwa_utm_source'],
    ['gw_utm_medium',   'gwa_utm_medium'],
    ['gw_utm_campaign', 'gwa_utm_campaign'],
    ['gw_utm_content',  'gwa_utm_content'],
    ['gw_utm_term',     'gwa_utm_term']
  ];
  var GW_ATTR_MAX = 1200;
  function gwSetSessionCookie(name, value) {
    document.cookie = name + '=' + encodeURIComponent(value) +
      ';domain=.gushwork.ai;path=/;SameSite=Lax';
  }
  GW_ATTR_MIRROR.forEach(function(pair) {
    try {
      if (!sessionStorage.getItem(pair[0])) {
        var v = readCookie(pair[1]);
        if (v) sessionStorage.setItem(pair[0], v);
      }
    } catch (e) {}
  });

  if (!sessionStorage.getItem('gw_referrer')) {
    var ref = document.referrer || '';
    if (ref &&
        ref.indexOf('gushwork.ai') === -1 &&
        ref.indexOf('gushwork.webflow.io') === -1) {
      sessionStorage.setItem('gw_referrer', ref);
    }
  }
  if (!sessionStorage.getItem('gw_landing_page')) {
    sessionStorage.setItem('gw_landing_page', window.location.href);
  }
  var p = new URLSearchParams(window.location.search);
  ['utm_source', 'utm_medium', 'utm_campaign', 'utm_content', 'utm_term'].forEach(function(key) {
    var val = p.get(key);
    if (val) sessionStorage.setItem('gw_' + key, val);
  });

  GW_ATTR_MIRROR.forEach(function(pair) {
    try {
      var v = sessionStorage.getItem(pair[0]);
      if (v && v.length <= GW_ATTR_MAX) gwSetSessionCookie(pair[1], v);
    } catch (e) {}
  });

  // PartnerStack affiliate attribution
  function readCookie(name) {
    var m = document.cookie.match('(^|;)\\s*' + name + '\\s*=\\s*([^;]+)');
    return m ? decodeURIComponent(m[2]) : null;
  }
  function setCookie(name, value) {
    document.cookie = name + '=' + encodeURIComponent(value) +
      ';domain=.gushwork.ai;path=/;max-age=7776000;SameSite=Lax';
  }
  var prevXid   = readCookie('gw_ps_xid');
  var psPartner = readCookie('ps_partner_key') || p.get('ps_partner_key') || readCookie('gw_ps_partner_key');
  var psXid     = readCookie('ps_xid')         || p.get('ps_xid')         || prevXid;
  if (psXid) {
    if (psXid !== prevXid) {
      var hist = [];
      try { hist = JSON.parse(readCookie('gw_ps_clicks') || '[]'); } catch (e) {}
      hist.push({ xid: psXid, pk: psPartner || null, at: new Date().toISOString() });
      if (hist.length > 10) hist = hist.slice(-10);
      setCookie('gw_ps_clicks', JSON.stringify(hist));
      setCookie('gw_ps_seen_at', new Date().toISOString());
    }
    setCookie('gw_ps_xid', psXid);
    sessionStorage.setItem('gw_ps_xid', psXid);
    if (psPartner) {
      setCookie('gw_ps_partner_key', psPartner);
      sessionStorage.setItem('gw_ps_partner_key', psPartner);
    }
    sessionStorage.setItem('gw_ps_seen_at', readCookie('gw_ps_seen_at'));
    sessionStorage.setItem('gw_ps_clicks', readCookie('gw_ps_clicks') || '[]');
  }
})();
