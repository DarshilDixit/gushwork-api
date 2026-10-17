/* ============================================================
   THE ONE LIST OF ORIGINS THE API ACCEPTS. Since v5.23.0 (Oct 2026).

   EDIT THIS LIST ONLY, THEN REBUILD: node tools/build-form-min.js.

   Read by:
     index.js                 the CORS check, so a browser request from any
                              other origin is refused before a route runs
     tools/build-form-min.js  which writes these origins' hostnames into
                              SESSION_HOSTS in BOTH form files (readable and
                              minified), so the forms send /session from
                              exactly the hosts that can receive it

   WHY ONE LIST. Until v5.23.0 the CORS list lived in the Railway env
   (ALLOWED_ORIGIN) and v5.22.0 put a hand-kept copy in each form file.
   An origin added to the env and not to both form files would have
   accepted /session from that site while the form never sent it: the
   site would silently record no page loads. The env var is no longer
   read; index.js warns at boot if it is still set to something else.
   It held exactly these three on 17 Oct 2026.

   Exact origins: scheme, host and port as the browser sends them. Never a
   pattern, never a wildcard. tests/test-synthetic.js fails if any copy
   differs from this list.
   ============================================================ */

const ALLOWED_ORIGINS = Object.freeze([
  'https://www.gushwork.ai',
  'https://gushwork.ai',
  'https://gushwork.webflow.io',
]);

/* The hostnames, in list order, without duplicates: what the form files
   compare window.location.hostname against. */
function allowedHostnames(origins = ALLOWED_ORIGINS) {
  const out = [];
  for (const o of origins) {
    const h = new URL(o).hostname;
    if (out.indexOf(h) === -1) out.push(h);
  }
  return out;
}

module.exports = { ALLOWED_ORIGINS, allowedHostnames };
