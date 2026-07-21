/* ==========================================================
  GUSHWORK — MULTI-STEP FORM  v4.9  (/demo PAGE VERSION - thru github/jsdlivr)

  /* --------------------------------------------------------
  INJECT STYLES
  -------------------------------------------------------- */
  (function injectStyles() {
    const css = `
  [id$="-error"] {
  display: none;
  color: #e53e3e;
  font-size: 13px;
  margin-top: 4px;
  }
  .gw-input-warning {
  border-color: #FF6A00 !important;
  }
  .gw-input-error {
  border-color: #e53e3e !important;
  }
  #email-protip {
  display: none;
  align-items: flex-start;
  gap: 4px;
  color: #FF6A00;
  font-size: 12px;
  font-weight: 500;
  line-height: 1.4;
  margin-top: 4px;
  }
  #email-protip img {
  width: 14px;
  height: 14px;
  display: block;
  margin-top: 1px;
  flex-shrink: 0;
  }
  #main-wrapper {
  transition: max-width 0.5s ease;
  }
  #form-wrap-view {
  transition: max-width 0.5s ease, padding 0.5s ease;
  }
  `;
    const style = document.createElement('style');
    style.textContent = css;
    document.head.appendChild(style);
  })();

  /* --------------------------------------------------------
  PHONE INPUT — intl-tel-input v17
  Targets input[ms-code-phone-number].
  dropdownContainer: document.body fixes mobile stacking;
  CSS is z-index ONLY — position:fixed breaks desktop when scrolled.
  -------------------------------------------------------- */
  (function injectPhoneDeps() {
    const css = `
  .iti--container { z-index: 999999999 !important; }
  .iti__country-list { -webkit-overflow-scrolling: touch; }
  .iti { width: 100%; display: block; }
  `;
    const style = document.createElement('style');
    style.textContent = css;
    document.head.appendChild(style);

    const link = document.createElement('link');
    link.rel = 'stylesheet';
    link.href = 'https://cdnjs.cloudflare.com/ajax/libs/intl-tel-input/17.0.8/css/intlTelInput.min.css';
    document.head.appendChild(link);

    function loadScript(src, onload) {
      const s = document.createElement('script');
      s.src = src;
      s.onload = onload;
      document.head.appendChild(s);
    }

    loadScript('https://cdnjs.cloudflare.com/ajax/libs/jquery/3.6.0/jquery.min.js', function () {
      loadScript('https://cdnjs.cloudflare.com/ajax/libs/intl-tel-input/17.0.8/js/intlTelInput.min.js', function () {
        loadScript('https://cdnjs.cloudflare.com/ajax/libs/intl-tel-input/17.0.8/js/utils.js', function () {
          initPhoneInputs();
        });
      });
    });

    function initPhoneInputs() {
      if (typeof $ === 'undefined' || typeof window.intlTelInput === 'undefined') return;
      $('input[ms-code-phone-number]').each(function () {
        var input = this;
        var preferredCountries = $(input).attr('ms-code-phone-number').split(',');
        var iti = window.intlTelInput(input, {
          preferredCountries: preferredCountries,
          initialCountry: 'us', // shown before country.is lookup resolves
          dropdownContainer: document.body, // fix: render outside form stacking context
          utilsScript: 'https://cdnjs.cloudflare.com/ajax/libs/intl-tel-input/17.0.8/js/utils.js',
        });
        input._iti = iti;

        // Float-label fix: re-sync --input-padding-left after iti adds
        // flag padding, so the label starts where the text starts
        var flWrapper = input.closest('.float-label-wrapper');
        function syncFloatLabel() {
          if (!flWrapper) return;
          flWrapper.style.setProperty('--input-padding-left', window.getComputedStyle(input).paddingLeft);
        }
        syncFloatLabel();
        setTimeout(syncFloatLabel, 100);
        setTimeout(syncFloatLabel, 500);
        input.addEventListener('countrychange', syncFloatLabel);

        // Country detection via country.is (free, CORS-friendly).
        // Don't switch to ipinfo — it 406s browser requests.
        fetch('https://api.country.is')
          .then(function (r) {
            if (!r.ok) throw new Error('country.is ' + r.status);
            return r.json();
          })
          .then(function (data) {
            if (data && data.country) {
              try {
                iti.setCountry(data.country.toLowerCase());
              } catch (e) {}
              syncFloatLabel();
            }
          })
          .catch(function () {}); // lookup failed — fallback stays 'us'

        input.addEventListener('change', function () {
          if (typeof intlTelInputUtils !== 'undefined') input.value = iti.getNumber(intlTelInputUtils.numberFormat.NATIONAL);
        });
        input.addEventListener('keyup', function () {
          if (typeof intlTelInputUtils !== 'undefined') input.value = iti.getNumber(intlTelInputUtils.numberFormat.NATIONAL);
        });
      });
    }
  })();

  /* --------------------------------------------------------
  FORM LOGIC
  -------------------------------------------------------- */
  (function () {
    const RAILWAY_API_URL = 'https://gushwork-api-production.up.railway.app';
    const RH_ROUTER_ID = '6138';
    const ENRICHMENT_TTL_MS = 7 * 24 * 60 * 60 * 1000;
    // TEMPORARY (per team decision, July 2026): website check still runs
    // and still shows the red error, but no longer blocks progression.
    // Flip to true to restore blocking — no other code changes needed.
    const WEBSITE_CHECK_BLOCKING = false;

    const formState = {
      session_id: '',
      page_url: '',
      email: '',
      website: '',
      sell_to: '',
      first_name: '',
      last_name: '',
      phone: '',
      company: '',
      hear_about_us: '',
      utm_source: '',
      utm_medium: '',
      utm_campaign: '',
      utm_content: '',
      utm_term: '',
      referrer: '',
      prefill_source: '',
      enriched_title: '',
      enriched_company_size: '',
      enriched_industry: '',
      enriched_linkedin: '',
      fbc: '',
      fbp: '',
      landing_page: '',
      previous_page: '',
      step_reached: 1,
      completed: false,
      disqualified: false,
      disqualified_reason: '',
      website_check_failed: false,
      website_check_reason: '',
    };

    let _enrichedForEmail = '';
    let _lastVerifiedEmail = '';
    let _submitting = false;
    let _isPopstateNav = false;

    /* =======================================================
    SECTION 1 — INITIALISATION
    ======================================================= */

    function getCookie(name) {
      var match = document.cookie.match(new RegExp('(^| )' + name + '=([^;]+)'));
      return match ? decodeURIComponent(match[2]) : '';
    }

    function initSession() {
      let sid = sessionStorage.getItem('gw_session_id');
      if (!sid) {
        sid =
          typeof crypto !== 'undefined' && crypto.randomUUID
            ? crypto.randomUUID()
            : 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, (c) => {
                const r = (Math.random() * 16) | 0;
                return (c === 'x' ? r : (r & 0x3) | 0x8).toString(16);
              });
        sessionStorage.setItem('gw_session_id', sid);
      }
      formState.session_id = sid;
      formState.page_url = window.location.href;
      setHidden('session-id', sid);
    }

    function captureUTMs() {
      const p = new URLSearchParams(window.location.search);
      formState.utm_source = p.get('utm_source') || sessionStorage.getItem('gw_utm_source') || '';
      formState.utm_medium = p.get('utm_medium') || sessionStorage.getItem('gw_utm_medium') || '';
      formState.utm_campaign = p.get('utm_campaign') || sessionStorage.getItem('gw_utm_campaign') || '';
      formState.utm_content = p.get('utm_content') || sessionStorage.getItem('gw_utm_content') || '';
      formState.utm_term = p.get('utm_term') || sessionStorage.getItem('gw_utm_term') || '';
      formState.referrer = sessionStorage.getItem('gw_referrer') || 'direct';

      const currentReferrer = document.referrer || '';
      const isInternal = currentReferrer && (currentReferrer.includes('gushwork.ai') || currentReferrer.includes('gushwork.webflow.io'));
      if (isInternal) {
        formState.previous_page = currentReferrer;
        sessionStorage.setItem('gw_previous_page', currentReferrer);
      } else {
        formState.previous_page = sessionStorage.getItem('gw_previous_page') || '';
      }

      setHidden('utm-source', formState.utm_source);
      setHidden('utm-medium', formState.utm_medium);
      setHidden('utm-campaign', formState.utm_campaign);
      setHidden('utm-content', formState.utm_content);
      setHidden('utm-term', formState.utm_term);
      setHidden('referrer', formState.referrer);
    }

    function captureMetaAttribution() {
      const p = new URLSearchParams(window.location.search);
      var fbclid = p.get('fbclid') || '';
      if (fbclid && !getCookie('_fbc')) {
        var fbc = 'fb.1.' + Date.now() + '.' + fbclid;
        document.cookie = '_fbc=' + fbc + ';max-age=7776000;path=/;SameSite=Lax';
      }
      var fbcValue = getCookie('_fbc');
      if (!fbcValue && fbclid) fbcValue = 'fb.1.' + Date.now() + '.' + fbclid;
      formState.fbc = fbcValue || '';
      formState.fbp = getCookie('_fbp') || '';
      formState.landing_page = sessionStorage.getItem('gw_landing_page') || window.location.href;
    }

    function prefillHearAboutUs() {
      const src = (formState.utm_source || '').toLowerCase();
      const ref = (formState.referrer || '').toLowerCase();

      let prevSrc = '',
        prevMedium = '';
      if (!src && formState.previous_page) {
        try {
          const prevUrl = new URL(formState.previous_page);
          prevSrc = (prevUrl.searchParams.get('utm_source') || '').toLowerCase();
          prevMedium = (prevUrl.searchParams.get('utm_medium') || '').toLowerCase();
        } catch {}
      }

      const effectiveSrc = src || prevSrc;
      const effectiveMedium = (formState.utm_medium || '').toLowerCase() || prevMedium;

      const refEmail = getCookie('gw_ref_email');
      const refName = getCookie('gw_ref_name');

      if (refEmail) {
        const input = document.getElementById('hear-about-us');
        if (input) {
          input.value = 'Referral - ' + decodeURIComponent(refEmail);
          formState.hear_about_us = 'Referral - ' + decodeURIComponent(refEmail);
          const wrapper = input.closest('.field-wrapper');
          if (wrapper) wrapper.style.display = 'none';
        }
        const name = refName ? decodeURIComponent(refName).charAt(0).toUpperCase() + decodeURIComponent(refName).slice(1).toLowerCase() : '';
        document.querySelectorAll('.ref-wrapper').forEach((el) => {
          el.style.display = 'inline-flex';
        });
        if (name)
          document.querySelectorAll('[reff="first_name"]').forEach((el) => {
            el.textContent = name;
          });
        return;
      }

      const isFacebook = effectiveSrc.includes('facebook') || effectiveSrc.includes('fb');
      const isInstagram = effectiveSrc.includes('instagram') || effectiveSrc.includes('ig');
      const isUGC = effectiveMedium.includes('ugc');

      const creatorRaw = (formState.utm_campaign || '').trim();
      const creator = creatorRaw ? creatorRaw.charAt(0).toUpperCase() + creatorRaw.slice(1).toLowerCase() : '';

      let prefill = '';
      if (effectiveSrc.includes('cold_email')) {
        prefill = 'email';
      } else if (isFacebook && isUGC) {
        prefill = 'Facebook (UGC)' + (creator ? ' — ' + creator : '');
      } else if (isInstagram && isUGC) {
        prefill = 'Instagram (UGC)' + (creator ? ' — ' + creator : '');
      } else if (isFacebook || ref.includes('facebook.com')) {
        prefill = 'Facebook (Paid)';
      } else if (isInstagram || ref.includes('instagram.com')) {
        prefill = 'Instagram (Paid)';
      } else if (effectiveSrc.includes('linkedin') || ref.includes('linkedin.com')) {
        prefill = 'linkedin';
      } else if (effectiveSrc.includes('google') && (effectiveMedium.includes('cpc') || effectiveMedium.includes('paid'))) {
        prefill = 'Google Ads';
      }

      if (prefill) {
        const input = document.getElementById('hear-about-us');
        if (input) {
          input.value = prefill;
          formState.hear_about_us = prefill;
          const wrapper = input.closest('.field-wrapper');
          if (wrapper) wrapper.style.display = 'none';
        }
      }
    }

    function saveSession() {
      if (!isRailwayReady()) return;
      fetch(`${RAILWAY_API_URL}/session`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          session_id: formState.session_id,
          page_url: formState.page_url,
          utm_source: formState.utm_source,
          utm_medium: formState.utm_medium,
          utm_campaign: formState.utm_campaign,
          utm_content: formState.utm_content,
          referrer: formState.referrer,
        }),
      }).catch(() => {});
    }

    function prefillFromURL() {
      const p = new URLSearchParams(window.location.search);
      const email = p.get('email') || localStorage.getItem('gw_email') || '';
      const first = p.get('first_name') || '';
      const last = p.get('last_name') || '';
      const co = p.get('company') || '';

      if (email) {
        setField('email', email);
        formState.email = email;
        formState.prefill_source = p.get('email') ? 'url_param' : 'returning_visitor';
        setHidden('prefill-source', formState.prefill_source);
        if (isValidEmail(email) && isWorkEmail(email)) {
          const cached = getEnrichmentCache(email);
          if (cached) applyEnrichment(email, cached);
        }
      }
      if (first) {
        setField('first-name', first);
        formState.first_name = first;
      }
      if (last) {
        setField('last-name', last);
        formState.last_name = last;
      }
      if (co) {
        setField('company', co);
        formState.company = co;
      }
    }

    /* =======================================================
    SECTION 3 — VALIDATION
    ======================================================= */

    function isValidEmail(email) {
      return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email);
    }

    // Shared with SECTION 3C (website check) — keep one source of truth
    const PERSONAL_EMAIL_DOMAINS = ['gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'icloud.com', 'protonmail.com', 'aol.com', 'mail.com', 'yahoo.in', 'rediffmail.com', 'ymail.com', 'live.com', 'msn.com', 'me.com', 'mac.com', 'googlemail.com'];

    // ── Junk-text plausibility gate (v4.9.4) ──────────────────────────
    // Catches single-character / repeated-character / obvious-placeholder
    // input on free-text fields (email local-part, company, hear-about-us).
    // Deliberately conservative: only blocks near-certain junk so real
    // short names ("Jo", "Tes") and real functional mailboxes
    // (info@, sales@, support@) are never touched.
    function isRepeatedChar(v) {
      return /^(.)\1*$/.test(v);
    }
    function isJunkText(value, junkWords) {
      const v = (value || '').trim();
      if (v.length < 2) return true; // single character — never a real name/word
      if (isRepeatedChar(v)) return true; // "aaaa", "xx" etc.
      return junkWords.has(v.toLowerCase());
    }
    const JUNK_WORDS_GENERIC = new Set(['test', 'testing', 'asdf', 'asdfg', 'asdfgh', 'qwerty', 'qwertyuiop', 'xxx', 'xxxx', 'none', 'na', 'n/a', 'sample', 'example', 'abc', 'abcd', 'foo', 'bar', 'foobar', 'placeholder', 'fake', 'dummy', 'temp', 'temporary', 'delete', 'notreal']);
    const JUNK_WORDS_COMPANY = new Set([...JUNK_WORDS_GENERIC, 'company', 'business name', 'yourcompany']);
    // Deliberately does NOT include info/sales/support/contact/hello/admin/hr —
    // all common, legitimate functional business mailboxes.
    const JUNK_WORDS_EMAIL_LOCAL = new Set(['test', 'testing', 'asdf', 'asdfg', 'asdfgh', 'qwerty', 'qwertyuiop', 'xxx', 'xxxx', 'none', 'na', 'sample', 'example', 'foo', 'bar', 'foobar', 'placeholder', 'fake', 'dummy', 'temp', 'delete', 'notreal', 'abc', 'abcd', '123456', '12345', '111111', '000000']);

    function isWorkEmail(email) {
      return !PERSONAL_EMAIL_DOMAINS.includes(email.split('@')[1]?.toLowerCase() || '');
    }

    function isValidURL(url) {
      try {
        const u = new URL(url.startsWith('http') ? url : 'https://' + url);
        return u.hostname.includes('.');
      } catch {
        return false;
      }
    }

    function validateStep1() {
      let valid = true;
      const email = getField('email');

      if (!email) {
        showError('email-error', 'Email is required.');
        valid = false;
      } else if (!isValidEmail(email)) {
        showError('email-error', 'Please enter a valid email address.');
        valid = false;
      } else if (!isTestEmail(email) && isJunkText(email.split('@')[0], JUNK_WORDS_EMAIL_LOCAL)) {
        showError('email-error', "This doesn't look like a real email address. Please double-check.");
        valid = false;
      } else hideError('email-error');

      const sellTo = document.querySelector('input[name="sell-to"]:checked')?.value || (document.getElementById('sell-b2b')?.checked ? 'B2B' : '') || (document.getElementById('sell-b2c')?.checked ? 'B2C' : '') || (document.getElementById('sell-mixed')?.checked ? 'Mixed' : '');

      if (!sellTo) {
        showError('sell-error', 'Please select who you sell to.');
        valid = false;
      } else hideError('sell-error');

      return { valid, sellTo };
    }

    function validateStep2() {
      let valid = true;

      const firstName = getField('first-name');
      if (!firstName) {
        showError('first-name-error', 'First name is required.');
        valid = false;
      } else hideError('first-name-error');

      const lastName = getField('last-name');
      if (!lastName) {
        showError('last-name-error', 'Last name is required.');
        valid = false;
      } else hideError('last-name-error');

      const company = getField('company');
      if (!company) {
        showError('company-error', 'Company name is required.');
        valid = false;
      } else if (isJunkText(company, JUNK_WORDS_COMPANY)) {
        showError('company-error', 'Please enter your actual company name.');
        valid = false;
      } else hideError('company-error');

      const website = getField('website');
      if (!website) {
        showError('website-error', 'Website URL is required.');
        valid = false;
      } else if (!isValidURL(website)) {
        showError('website-error', 'Please enter a valid URL (e.g. acme.com).');
        valid = false;
      } else hideError('website-error');

      // ── Phone — OPTIONAL: only validate when the user typed something ──
      const phoneEl = document.getElementById('phone');
      if (phoneEl && phoneEl.value.trim() !== '' && phoneEl._iti && typeof phoneEl._iti.isValidNumber === 'function') {
        if (!phoneEl._iti.isValidNumber()) {
          showError('phone-error', 'Please enter a valid phone number.');
          valid = false;
        } else hideError('phone-error');
      } else hideError('phone-error');

      const hearAboutUs = getField('hear-about-us');
      const hearAboutUsHidden = document.getElementById('hear-about-us')?.closest('.field-wrapper')?.style.display === 'none';
      if (!hearAboutUs && !hearAboutUsHidden) {
        showError('hear-about-us-error', 'Please let us know how you heard about us.');
        valid = false;
      } else if (!hearAboutUsHidden && isJunkText(hearAboutUs, JUNK_WORDS_GENERIC)) {
        // Only checked when visible/manually typed — prefilled hidden
        // values (e.g. "Facebook (Paid)", "Referral - x") are trusted.
        showError('hear-about-us-error', 'Please let us know how you heard about us.');
        valid = false;
      } else hideError('hear-about-us-error');

      return valid;
    }

    /* =======================================================
    SECTION 3B — WORK-EMAIL NUDGE (non-blocking soft warning)
    Shows #email-protip + orange input border when a valid
    personal email is typed. Advisory only — never blocks
    progression. Red error always wins the slot AND the border.
    ======================================================= */

    function hideProTip() {
      const tip = document.getElementById('email-protip');
      if (tip) tip.style.display = 'none';
      const emailInput = document.getElementById('email');
      if (emailInput) emailInput.classList.remove('gw-input-warning');
    }

    function initEmailProTip() {
      const tip = document.getElementById('email-protip');
      const emailInput = document.getElementById('email');
      if (!tip || !emailInput) return;

      tip.innerHTML = '<img src="https://cdn.prod.website-files.com/65c292289fb0ea1ff3a84bd3/6a573b62ef8929dda9d988f1_WarningCircle.svg" alt="">' + '<span>Business email preferred over personal email.</span>';

      function updateProTip() {
        const email = emailInput.value.trim();
        // Live-clear a stale error once the email becomes format-valid —
        // without this, an error shown on a previous Next click stays
        // visible while typing and permanently suppresses the nudge
        if (isValidEmail(email)) hideError('email-error');
        const errVisible = document.getElementById('email-error')?.style.display === 'block';
        if (!errVisible && isValidEmail(email) && !isWorkEmail(email)) {
          tip.style.display = 'flex';
          emailInput.classList.add('gw-input-warning');
        } else {
          tip.style.display = 'none';
          emailInput.classList.remove('gw-input-warning');
        }
      }

      emailInput.addEventListener('input', updateProTip);
      emailInput.addEventListener('blur', updateProTip);
      updateProTip(); // covers URL-param / returning-visitor prefill
    }

    /* =======================================================
    SECTION 3C — WEBSITE EXISTENCE CHECK (client-side DoH)
    Fully front-end — nothing touches Railway/Slack/monitor.
    DNS-over-HTTPS via dns.google, cloudflare-dns.com fallback
    (both free, CORS-enabled, no key). Hard blocks:
      1. Free-mailbox domain typed as website (gmail.com...)
      2. Brand domain that doesn't match the lead's email domain
      3. NXDOMAIN — domain doesn't exist (www. variant retried)
      4. Parked domain — resolved IP in a known registrar
         parking range (Namecheap/GoDaddy/Sedo/ParkingCrew/...)
    Allows: email-only companies (MX records but no A record).
    Fail-open on any DoH/network error — a validation outage
    must never lose a real lead (same policy as ELV).
    Verdicts cached per domain; blur prewarms so Next is instant.
    ======================================================= */

    const _websiteVerdicts = new Map(); // domain -> {ok, reason, msg}
    const _websiteInFlight = new Map(); // domain -> Promise<verdict>

    // Big brands lazily typed to get past the field. Allowed ONLY
    // when the lead's email domain matches (real @google.com passes).
    const BRAND_DOMAINS = ['google.com', 'youtube.com', 'facebook.com', 'fb.com', 'instagram.com', 'linkedin.com', 'twitter.com', 'x.com', 'amazon.com', 'amazon.in', 'microsoft.com', 'apple.com', 'netflix.com', 'wikipedia.org', 'whatsapp.com', 'tiktok.com', 'reddit.com', 'openai.com', 'chatgpt.com', 'flipkart.com', 'gushwork.ai'];

    // Known registrar parking / domain-for-sale IP ranges.
    // 3-octet entries are prefix matches; full IPs are exact.
    const PARKING_IP_PREFIXES = [
      '162.255.119.', // Namecheap parking / URL-forwarding
      '34.102.136.180', // GoDaddy parking (exact IP)
      '91.195.240.', // Sedo
      '91.195.241.', // Sedo
      '185.53.177.', // ParkingCrew
      '185.53.178.', // ParkingCrew
      '185.53.179.', // ParkingCrew
      '199.59.242.', // Bodis
      '199.59.243.', // Bodis
      '208.91.197.', // Confluence Networks parking
    ];

    // Nameservers used ONLY for parking / domain-sale landers → always block.
    // Match is suffix-based, so ns1.sedoparking.com etc. all hit.
    const PARKING_NS_STRICT = ['sedoparking.com', 'parkingcrew.net', 'bodis.com', 'above.com', 'parklogic.com', 'uniregistrymarket.link', 'afternic.com', 'dan.com'];

    // Registrar DNS heavily used for for-sale inventory (NameBright =
    // HugeDomains) but also by some real retail customers → block only
    // when the domain ALSO has no MX records. A genuine company domain
    // virtually always has email; a sale lander never does.
    const PARKING_NS_SOFT = ['namebrightdns.com', 'safesecureweb.com'];

    function extractWebsiteDomain(raw) {
      try {
        const u = new URL(raw.startsWith('http') ? raw : 'https://' + raw);
        return u.hostname
          .replace(/^www\./i, '')
          .replace(/\.$/, '')
          .toLowerCase();
      } catch {
        return '';
      }
    }

    function emailDomainOf(email) {
      return ((email || '').split('@')[1] || '').toLowerCase();
    }

    function domainsMatch(a, b) {
      if (!a || !b) return false;
      return a === b || a.endsWith('.' + b) || b.endsWith('.' + a);
    }

    /* =======================================================
    SECTION 3E — EMAIL/WEBSITE DOMAIN MISMATCH NUDGE (v4.9.4)
    Non-blocking, orange, same visual language as the email pro-tip.
    Some real leads genuinely enter a different domain than their
    email (agencies filing for a client, subsidiary emails vs a
    parent brand's site) — so this only ever NUDGES, never blocks.
    Element is created dynamically; no Webflow HTML change needed.
    Vacates automatically for the red existence-check error via
    showError()'s hook below.
    ======================================================= */

    function ensureWebsiteMismatchTip() {
      let tip = document.getElementById('website-mismatch-protip');
      if (tip) return tip;
      const websiteInput = document.getElementById('website');
      if (!websiteInput) return null;
      tip = document.createElement('div');
      tip.id = 'website-mismatch-protip';
      tip.style.cssText = 'display:none;align-items:flex-start;gap:4px;color:#FF6A00;font-size:12px;font-weight:500;line-height:1.4;margin-top:4px;';
      tip.innerHTML = '<img src="https://cdn.prod.website-files.com/65c292289fb0ea1ff3a84bd3/6a573b62ef8929dda9d988f1_WarningCircle.svg" style="width:14px;height:14px;display:block;margin-top:1px;flex-shrink:0;" alt="">' + "<span>Doesn't match your email domain — just checking.</span>";
      const errEl = document.getElementById('website-error');
      if (errEl && errEl.parentNode) errEl.parentNode.insertBefore(tip, errEl.nextSibling);
      else websiteInput.insertAdjacentElement('afterend', tip);
      return tip;
    }

    function hideWebsiteMismatchTip() {
      const tip = document.getElementById('website-mismatch-protip');
      if (tip) tip.style.display = 'none';
    }

    function updateWebsiteMismatchTip() {
      const tip = ensureWebsiteMismatchTip();
      if (!tip) return;
      const websiteDomain = extractWebsiteDomain(getField('website'));
      const eDomain = emailDomainOf(getField('email'));
      const errVisible = document.getElementById('website-error')?.style.display === 'block';
      const showNudge = !errVisible && websiteDomain && eDomain && !PERSONAL_EMAIL_DOMAINS.includes(eDomain) && !domainsMatch(websiteDomain, eDomain);
      tip.style.display = showNudge ? 'flex' : 'none';
    }

    async function dohQuery(name, type) {
      // Google first, Cloudflare fallback. Throws only if BOTH fail.
      // 'accept' is a CORS-safelisted header — no preflight fired.
      const providers = ['https://dns.google/resolve?name=' + encodeURIComponent(name) + '&type=' + type, 'https://cloudflare-dns.com/dns-query?name=' + encodeURIComponent(name) + '&type=' + type];
      let lastErr;
      for (const url of providers) {
        try {
          const controller = new AbortController();
          const t = setTimeout(() => controller.abort(), 4000);
          const res = await fetch(url, { signal: controller.signal, headers: { accept: 'application/dns-json' } });
          clearTimeout(t);
          if (!res.ok) throw new Error('DoH ' + res.status);
          return await res.json();
        } catch (e) {
          lastErr = e;
        }
      }
      throw lastErr || new Error('DoH failed');
    }

    function dohARecords(json) {
      // type 1 = A record. CNAME chains arrive flattened in Answer,
      // so filtering on type 1 yields the final IPs.
      return ((json && json.Answer) || []).filter((r) => r.type === 1).map((r) => r.data);
    }

    function localWebsiteVerdict(domain) {
      // Email-dependent checks — must run FRESH on every call and must
      // never be cached: the correct verdict changes when the lead goes
      // back and edits their email (e.g. google.com is wrong for a
      // gmail user but right for priya@google.com). No network cost.
      const email = getField('email') || formState.email || '';

      // 1. Free mailbox providers are never a company website
      if (PERSONAL_EMAIL_DOMAINS.includes(domain)) {
        return { ok: false, reason: 'mailbox_domain', msg: "Please enter your company's website — not an email provider." };
      }

      // 2. Brand domains — valid only when the email domain matches
      if (BRAND_DOMAINS.includes(domain) && !domainsMatch(domain, emailDomainOf(email))) {
        return { ok: false, reason: 'brand_mismatch', msg: "Please enter your own company's website." };
      }

      return null;
    }

    async function verifyWebsiteDomain(domain) {
      // 3 + 4 + 5. DNS existence + parking-IP + parking-NS via DoH.
      // Verdicts from here are email-independent and safe to cache.
      try {
        const json = await dohQuery(domain, 'A');
        let ips = dohARecords(json);

        // No A record on the apex — retry the www. variant before judging
        let wwwJson = null;
        if (!ips.length) {
          wwwJson = await dohQuery('www.' + domain, 'A').catch(() => null);
          const wwwIps = wwwJson ? dohARecords(wwwJson) : [];
          if (wwwIps.length) ips = wwwIps;
        }

        if (!ips.length) {
          // Only block on DECISIVE answers: NXDOMAIN (Status 3) or a
          // clean empty NOERROR (Status 0). Anything else — SERVFAIL
          // (Status 2, e.g. a real domain with broken DNSSEC), REFUSED,
          // etc. — is indeterminate and must fail open, never block.
          const decisive = (j) => !!j && (j.Status === 0 || j.Status === 3);
          if (!decisive(json) && !decisive(wwwJson)) {
            return { ok: true, reason: 'dns_indeterminate' };
          }
          // Email-only companies are real — allow if the apex has MX records.
          const mxJson = await dohQuery(domain, 'MX').catch(() => null);
          const hasMX = !!(mxJson && (mxJson.Answer || []).some((r) => r.type === 15));
          if (hasMX) return { ok: true, reason: 'mx_only' };
          return { ok: false, reason: 'nxdomain', msg: "This website doesn't appear to exist. Please check the URL." };
        }

        // Parked — every path resolved, but into a known parking range
        const parked = ips.some((ip) => PARKING_IP_PREFIXES.some((p) => (p.split('.').length === 4 && p.slice(-1) !== '.' ? ip === p : ip.indexOf(p) === 0)));
        if (parked) {
          return { ok: false, reason: 'parked', msg: "This doesn't appear to be a live company website. Please check the URL." };
        }

        // For-sale lander via NAMESERVERS (v4.9.1) — sale pages sit on
        // generic cloud IPs the IP list can't see, but the NS records
        // fingerprint the parking service. Fail-open if the query errors.
        const nsJson = await dohQuery(domain, 'NS').catch(() => null);
        const nsHosts = nsJson ? (nsJson.Answer || []).filter((r) => r.type === 2).map((r) => String(r.data).toLowerCase().replace(/\.$/, '')) : [];
        const nsMatches = (list) => nsHosts.some((h) => list.some((s) => h === s || h.endsWith('.' + s)));
        if (nsMatches(PARKING_NS_STRICT)) {
          return { ok: false, reason: 'parked_ns', msg: "This doesn't appear to be a live company website. Please check the URL." };
        }
        if (nsMatches(PARKING_NS_SOFT)) {
          const softMxJson = await dohQuery(domain, 'MX').catch(() => null);
          const softHasMX = !!(softMxJson && (softMxJson.Answer || []).some((r) => r.type === 15));
          if (!softHasMX) return { ok: false, reason: 'parked_ns', msg: "This doesn't appear to be a live company website. Please check the URL." };
        }

        return { ok: true, reason: 'resolved' };
      } catch (err) {
        console.warn('[GW] Website check failed — allowing through:', err && err.message);
        return { ok: true, reason: 'doh_error' }; // fail-open, same policy as ELV
      }
    }

    // Verdicts safe to cache — decisive, email-independent DNS results.
    // Transient/indeterminate outcomes must be re-checked next time.
    const CACHEABLE_REASONS = ['nxdomain', 'parked', 'parked_ns', 'resolved', 'mx_only', 'for_sale_lander'];

    // Stage 2 — server-level content check (v4.9.5). DNS/IP/NS alone can't
    // see PAGE CONTENT, so marketplace landers on shared CDN IPs (Atom,
    // GoDaddy Auctions, Sedo) slip through stage 1 as "resolved" — the
    // real test.com case. Only called when stage 1 found a genuinely live
    // domain (reason === 'resolved'); skipped for mx_only/indeterminate/
    // test-email, since there's either nothing to scan or nothing to gain.
    // Same fail-open contract as everything else: any backend hiccup,
    // timeout, or bot-wall passes the lead through rather than blocking it.
    async function verifyWebsiteContent(rawValue) {
      if (!isRailwayReady()) return { ok: true, reason: 'skipped_no_backend' };
      try {
        const controller = new AbortController();
        const t = setTimeout(() => controller.abort(), 7000);
        const res = await fetch(`${RAILWAY_API_URL}/verify-website`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          signal: controller.signal,
          body: JSON.stringify({ website: rawValue }),
        });
        clearTimeout(t);
        if (!res.ok) return { ok: true, reason: 'backend_error' };
        const data = await res.json();
        return { ok: data.ok !== false, reason: data.reason || 'checked' };
      } catch (err) {
        console.warn('[GW] Website content check failed — allowing through:', err && err.message);
        return { ok: true, reason: 'fetch_error' };
      }
    }

    function checkWebsite(rawValue) {
      // Cached + deduped wrapper (blur prewarm and Next click share
      // one in-flight promise). Returns Promise<verdict>.
      const domain = extractWebsiteDomain(rawValue);
      if (!domain) return Promise.resolve({ ok: true, reason: 'unparseable' }); // format errors are validateStep2's job

      // Email-dependent checks first, fresh every time, never cached
      const local = localWebsiteVerdict(domain);
      if (local) return Promise.resolve(local);

      if (_websiteVerdicts.has(domain)) return Promise.resolve(_websiteVerdicts.get(domain));
      if (_websiteInFlight.has(domain)) return _websiteInFlight.get(domain);
      const p = verifyWebsiteDomain(domain)
        .then(async (v) => {
          // Only worth a content probe when stage 1 found a real, live
          // website — mx_only/nxdomain/parked/indeterminate need no scan
          if (v.reason === 'resolved' && !isTestEmail(getField('email'))) {
            const cv = await verifyWebsiteContent(rawValue);
            if (!cv.ok) return { ok: false, reason: 'for_sale_lander', msg: "This doesn't appear to be a live company website. Please check the URL." };
          }
          return v;
        })
        .then((v) => {
          if (CACHEABLE_REASONS.indexOf(v.reason) !== -1) _websiteVerdicts.set(domain, v);
          return v;
        })
        .finally(() => {
          _websiteInFlight.delete(domain);
        });
      _websiteInFlight.set(domain, p);
      return p;
    }

    function initWebsiteCheck() {
      const el = document.getElementById('website');
      if (!el) return;

      // Editing the field clears a stale verdict error immediately
      el.addEventListener('input', () => {
        hideError('website-error');
        updateWebsiteMismatchTip();
      });

      // Blur — prewarm the cache and surface the error early so the
      // lead fixes it before ever clicking Next
      el.addEventListener('blur', async () => {
        updateWebsiteMismatchTip();
        const val = el.value.trim();
        if (!val || !isValidURL(val)) return; // required/format errors shown on Next
        if (isTestEmail(getField('email'))) return;
        const v = await checkWebsite(val);
        // Only show if the field still holds the value we checked
        if (!v.ok && el.value.trim() === val) {
          showError('website-error', v.msg);
        } else {
          updateWebsiteMismatchTip(); // re-evaluate now the block cleared
        }
      });

      // Email can be edited after the website field is already filled
      // (e.g. via back button) — keep the nudge in sync either way
      const emailInput = document.getElementById('email');
      if (emailInput) {
        emailInput.addEventListener('blur', updateWebsiteMismatchTip);
        emailInput.addEventListener('input', updateWebsiteMismatchTip);
      }
    }

    /* =======================================================
    SECTION 4 — EMAIL VERIFICATION
    ======================================================= */

    const TEST_EMAILS = ['b@g.ai'];

    function isTestEmail(email) {
      return TEST_EMAILS.includes(email.toLowerCase());
    }

    async function verifyEmail(email) {
      if (isTestEmail(email)) {
        console.log('[GW] Test email — skipping ELV');
        return true;
      }
      if (!isRailwayReady()) return true;
      if (email === _lastVerifiedEmail) return true;
      try {
        const res = await fetch(`${RAILWAY_API_URL}/verify-email`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ email }),
        });
        const data = await res.json();
        if (data.valid) _lastVerifiedEmail = email;
        return data.valid;
      } catch (err) {
        console.warn('[GW] ELV failed — allowing through:', err.message);
        return true;
      }
    }

    /* =======================================================
    SECTION 5 — STEP NAVIGATION
    ======================================================= */

    function showStep(stepId) {
      const allSteps = ['step-1', 'step-2', 'step-3', 'step-disqualified', 'step-disqualified-thanks', 'step-success'];
      const currentEl = allSteps.map((id) => document.getElementById(id)).find((el) => el && el.style.display !== 'none');
      const nextEl = document.getElementById(stepId);
      if (!nextEl) return;

      if (!_isPopstateNav) history.pushState({ step: stepId }, '', '');
      _isPopstateNav = false;

      const mainWrapper = document.getElementById('main-wrapper');
      const formWrapView = document.getElementById('form-wrap-view');
      if (stepId === 'step-3') {
        if (mainWrapper) mainWrapper.style.maxWidth = '1100px';
        if (formWrapView) formWrapView.style.maxWidth = '1040px';
      } else {
        if (mainWrapper) mainWrapper.style.maxWidth = '1000px';
        if (formWrapView) formWrapView.style.maxWidth = '600px';
      }

      if (currentEl && currentEl !== nextEl) {
        currentEl.style.transition = 'opacity 0.3s ease';
        currentEl.style.opacity = '0';
        setTimeout(() => {
          currentEl.style.display = 'none';
          currentEl.style.opacity = '1';
          nextEl.style.opacity = '0';
          nextEl.style.display = 'block';
          nextEl.style.transition = 'opacity 0.3s ease';
          nextEl.scrollIntoView({ behavior: 'smooth', block: 'start' });
          requestAnimationFrame(() =>
            requestAnimationFrame(() => {
              nextEl.style.opacity = '1';
            }),
          );
        }, 300);
      } else {
        nextEl.style.display = 'block';
        nextEl.style.opacity = '1';
        nextEl.scrollIntoView({ behavior: 'smooth', block: 'start' });
      }
    }

    /* =======================================================
    SECTION 6 — RAILWAY API CALLS
    ======================================================= */

    function isRailwayReady() {
      return RAILWAY_API_URL && !RAILWAY_API_URL.includes('your-api');
    }

    async function savePartial(step) {
      formState.step_reached = step;
      setHidden('step-reached', step);
      if (isTestEmail(formState.email)) {
        console.log('[GW] Test email — skipping savePartial');
        return true;
      }
      if (!isRailwayReady()) return true;
      try {
        const res = await fetch(`${RAILWAY_API_URL}/partial`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(formState),
        });
        return res.ok;
      } catch (err) {
        console.warn('[GW] Partial capture failed:', err);
        return true;
      }
    }

    async function submitLead() {
      formState.completed = true;
      setHidden('completed', 'true');
      if (isTestEmail(formState.email)) {
        console.log('[GW] Test email — skipping submitLead');
        return true;
      }
      if (!isRailwayReady()) return true;
      try {
        const res = await fetch(`${RAILWAY_API_URL}/submit`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(formState),
        });
        return res.ok;
      } catch (err) {
        console.warn('[GW] Submit failed:', err);
        return true;
      }
    }

    /* =======================================================
    SECTION 7 — ENRICHMENT
    ======================================================= */

    function getEnrichmentCache(email) {
      try {
        const raw = localStorage.getItem('gw_enrich_' + email);
        if (!raw) return null;
        const { data, ts } = JSON.parse(raw);
        if (Date.now() - ts > ENRICHMENT_TTL_MS) {
          localStorage.removeItem('gw_enrich_' + email);
          return null;
        }
        return data;
      } catch {
        return null;
      }
    }

    function setEnrichmentCache(email, data) {
      try {
        localStorage.setItem('gw_enrich_' + email, JSON.stringify({ data, ts: Date.now() }));
      } catch {}
    }

    const _enrichInFlight = new Set();

    async function enrichEmail(email) {
      if (!isRailwayReady()) return null;
      if (_enrichInFlight.has(email)) return null;
      _enrichInFlight.add(email);
      try {
        const res = await fetch(`${RAILWAY_API_URL}/enrich`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ email, session_id: formState.session_id }),
        });
        if (!res.ok) return null;
        return await res.json();
      } catch {
        return null;
      } finally {
        _enrichInFlight.delete(email);
      }
    }

    function clearEnrichedFields() {
      setField('first-name', '');
      formState.first_name = '';
      setField('last-name', '');
      formState.last_name = '';
      setField('company', '');
      formState.company = '';
      setField('website', '');
      formState.website = '';
      formState.enriched_title = '';
      formState.enriched_company_size = '';
      formState.enriched_industry = '';
      formState.enriched_linkedin = '';
      setHidden('enriched-title', '');
      setHidden('enriched-company-size', '');
      setHidden('enriched-industry', '');
      setHidden('enriched-linkedin', '');
      _enrichedForEmail = '';
    }

    function applyEnrichment(email, data) {
      if (!data) return;
      formState.enriched_title = data.title || '';
      formState.enriched_company_size = data.company_size || '';
      formState.enriched_industry = data.industry || '';
      formState.enriched_linkedin = data.linkedin_url || '';
      setHidden('enriched-title', formState.enriched_title);
      setHidden('enriched-company-size', formState.enriched_company_size);
      setHidden('enriched-industry', formState.enriched_industry);
      setHidden('enriched-linkedin', formState.enriched_linkedin);

      if (data.first_name && !getField('first-name')) {
        setField('first-name', data.first_name);
        formState.first_name = data.first_name;
      }
      if (data.last_name && !getField('last-name')) {
        setField('last-name', data.last_name);
        formState.last_name = data.last_name;
      }
      if (data.company && !getField('company')) {
        setField('company', data.company);
        formState.company = data.company;
      }
      if (data.website && !getField('website')) {
        const clean = data.website.replace(/^https?:\/\//, '').replace(/\/$/, '');
        setField('website', clean);
        formState.website = clean;
      }
      setEnrichmentCache(email, data);
      _enrichedForEmail = email;
    }

    async function triggerEnrichment(email) {
      if (isTestEmail(email)) {
        console.log('[GW] Test email — skipping enrichment');
        return;
      }
      if (!email || !isValidEmail(email) || !isWorkEmail(email)) return;
      if (email === _enrichedForEmail) return;
      const cached = getEnrichmentCache(email);
      if (cached) {
        applyEnrichment(email, cached);
      } else {
        const data = await enrichEmail(email);
        applyEnrichment(email, data);
      }
    }

    /* =======================================================
SECTION 8 — REVENUEHERO MEETING_BOOKED LISTENER
Listens for MEETING_BOOKED postMessage from RH inline embed.
Fires /booking-confirmed (browser-side) — same endpoint Cal uses.
Server-side redundancy handled by /booking-confirmed-webhook-rh.
======================================================= */

    function initRHBookingListener() {
      window.addEventListener('message', async (ev) => {
        if (ev.data?.type !== 'MEETING_BOOKED') return;

        const meeting = ev.data.meeting?.attributes || {};

        console.log('[GW] ✅ RH MEETING_BOOKED event received:', meeting);

        // GTM — Demo Booked
        if (!isTestEmail(formState.email)) {
          window.dataLayer = window.dataLayer || [];
          window.dataLayer.push({ event: 'gw_demo_booked' });
          console.log('[GW] ✅ GTM Demo Booked event pushed');
        }

        // Fire existing /booking-confirmed endpoint — same one Cal uses
        if (isRailwayReady() && !isTestEmail(formState.email)) {
          await fetch(`${RAILWAY_API_URL}/booking-confirmed`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              session_id: formState.session_id,
              booking_uid: meeting.id || ev.data.meeting?.id || '',
              start_time: meeting.meeting_time || '',
              end_time: '',
              event_type: 'demo',
            }),
          }).catch(() => {});
        }
      });
    }

    /* =======================================================
    SECTION 9 — STEP HANDLERS
    ======================================================= */

    async function handleStep1Next() {
      if (_submitting) return;
      const { valid, sellTo } = validateStep1();
      if (!valid) return;

      const email = getField('email');
      if (email !== _enrichedForEmail) clearEnrichedFields();

      _submitting = true;
      setLoading('step-1-next', true, 'Verifying...');

      try {
        const isVerified = await verifyEmail(email);
        if (!isVerified) {
          showError('email-error', 'This email address appears to be invalid. Please use a real email.');
          return;
        }

        hideError('email-error');
        formState.email = email;
        formState.sell_to = sellTo;
        localStorage.setItem('gw_email', formState.email);

        if (sellTo === 'B2C' || sellTo === 'Mixed') {
          formState.disqualified = true;
          formState.disqualified_reason = 'b2c_or_mixed';
        } else {
          formState.disqualified = false;
          formState.disqualified_reason = '';
        }

        setLoading('step-1-next', true, 'Loading...');
        await triggerEnrichment(formState.email);
        await savePartial(1);

        if (sellTo === 'B2C' || sellTo === 'Mixed') showStep('step-disqualified');
        else showStep('step-2');
      } finally {
        _submitting = false;
        setLoading('step-1-next', false);
      }
    }

    async function handleDisqualifiedNext(choice) {
      if (_submitting) return;
      if (!choice) {
        const v = validateDisqualified();
        if (!v.valid) return;
        choice = v.choice;
      }
      _submitting = true;
      try {
        if (choice === 'waitlist') {
          formState.disqualified = true;
          formState.disqualified_reason = 'waitlist';
          await savePartial(1);
          showStep('step-disqualified-thanks');
        } else if (choice === 'b2b') {
          formState.disqualified = false;
          formState.disqualified_reason = 'b2b_clarified';
          formState.sell_to = 'B2B (clarified from ' + formState.sell_to + ')';
          await triggerEnrichment(formState.email);
          await savePartial(1);
          showStep('step-2');
        }
      } finally {
        _submitting = false;
      }
    }

    async function handleStep2Next() {
      if (_submitting) return;
      const valid = validateStep2();
      if (!valid) return;

      _submitting = true;
      setLoading('step-2-next', true, 'Checking website...');

      try {
        // ── Website existence check (SECTION 3C) ──────────
        // Usually already resolved by the blur prewarm, so this
        // is a cache hit and the button barely flickers
        if (!isTestEmail(getField('email'))) {
          const wv = await checkWebsite(getField('website'));
          formState.website_check_failed = !wv.ok;
          formState.website_check_reason = wv.reason || (wv.ok ? 'ok' : 'unknown');
          if (!wv.ok) {
            showError('website-error', wv.msg);
            // WEBSITE_CHECK_BLOCKING=false (temporary, team decision): the
            // red error still shows, but the lead is allowed to continue.
            // formState.website_check_failed rides along to Railway, which
            // suppresses Meta CAPI and flags it for Slack/monitor.
            if (WEBSITE_CHECK_BLOCKING) return; // finally-block resets the button
          }
        } else {
          formState.website_check_failed = false;
          formState.website_check_reason = 'test_email_skipped';
        }
        setLoading('step-2-next', true, 'Please wait...');

        formState.first_name = getField('first-name');
        formState.last_name = getField('last-name');
        formState.company = getField('company');
        formState.website = getField('website');
        formState.hear_about_us = getField('hear-about-us');

        // Phone (optional) — E.164, no spaces (+916388639290);
        // raw value fallback if utils.js hasn't loaded yet
        const phoneEl = document.getElementById('phone');
        formState.phone = '';
        if (phoneEl && phoneEl.value.trim()) {
          formState.phone = phoneEl._iti && typeof intlTelInputUtils !== 'undefined' ? phoneEl._iti.getNumber(intlTelInputUtils.numberFormat.E164) : phoneEl.value.trim();
        }

        // ── Fire Railway + RH in parallel ──────────────────
        // hero.submit() starts immediately alongside submitLead()
        // Both resolve concurrently — eliminates sequential lag
        // ───────────────────────────────────────────────────
        const hero = new RevenueHero({ routerId: RH_ROUTER_ID });
        const rhPromise = hero.submit({
          Email: formState.email,
          'First Name': formState.first_name,
          'last-name': formState.last_name,
          'Company Name': formState.company,
          'Website URL': formState.website,
          'Hear about us': formState.hear_about_us,
          phone: formState.phone, // key matches RH Form Mapping field "phone"
        });

        await submitLead();

        // GTM — Form Submitted
        if (!isTestEmail(formState.email)) {
          window.dataLayer = window.dataLayer || [];
          window.dataLayer.push({ event: 'gw_form_submitted' });
          console.log('[GW] ✅ GTM Form Submitted event pushed');
        }

        // Transition to step-3 (300ms animation)
        showStep('step-3');

        // Await RH — likely already resolved due to parallel execution
        const rhData = await rhPromise;

        // Wait for step animation to complete, then render inline
        setTimeout(() => {
          hero.dialog.setEmbedTarget('#rh-embed');
          hero.dialog.open(rhData);
          console.log('[GW] ✅ RH inline embed opened');
        }, 350);
      } catch (error) {
        console.error('[GW] RH error:', error);
      } finally {
        _submitting = false;
        setLoading('step-2-next', false);
      }
    }

    /* =======================================================
    SECTION 10 — ENTER KEY SUPPORT
    ======================================================= */

    function initEnterKey() {
      document.addEventListener('keydown', (e) => {
        if (e.key !== 'Enter') return;
        if (e.target.tagName === 'TEXTAREA') return;
        const step1 = document.getElementById('step-1');
        const step2 = document.getElementById('step-2');
        if (step1 && step1.style.display !== 'none') {
          e.preventDefault();
          handleStep1Next();
        } else if (step2 && step2.style.display !== 'none') {
          e.preventDefault();
          handleStep2Next();
        }
      });
    }

    /* =======================================================
    SECTION 11 — BROWSER BACK BUTTON
    ======================================================= */

    function initBrowserBack() {
      history.replaceState({ step: 'step-1' }, '', '');
      window.addEventListener('popstate', (e) => {
        const targetStep = e.state?.step;
        if (targetStep) {
          _isPopstateNav = true;
          setLoading('step-1-next', false);
          setLoading('step-2-next', false);
          showStep(targetStep);
        }
      });
    }

    /* =======================================================
    SECTION 12 — BUTTON LISTENERS
    ======================================================= */

    function initButtons() {
      const btn1 = document.getElementById('step-1-next');
      const btn2 = document.getElementById('step-2-next');
      const btnDq = document.getElementById('step-disqualified-next');

      if (btn1)
        btn1.addEventListener('click', (e) => {
          e.preventDefault();
          handleStep1Next();
        });
      if (btn2)
        btn2.addEventListener('click', (e) => {
          e.preventDefault();
          handleStep2Next();
        });
      if (btnDq)
        btnDq.addEventListener('click', (e) => {
          e.preventDefault();
          handleDisqualifiedNext();
        });

      const disqWaitlist = document.getElementById('disq-waitlist');
      const disqB2b = document.getElementById('disq-b2b');
      if (disqWaitlist)
        disqWaitlist.addEventListener('change', () => {
          if (disqWaitlist.checked) handleDisqualifiedNext('waitlist');
        });
      if (disqB2b)
        disqB2b.addEventListener('change', () => {
          if (disqB2b.checked) handleDisqualifiedNext('b2b');
        });
    }

    /* =======================================================
    UTILITIES
    ======================================================= */

    function getField(id) {
      return (document.getElementById(id)?.value || '').trim();
    }
    function setField(id, value) {
      const el = document.getElementById(id);
      if (el) el.value = value;
    }
    function setHidden(id, value) {
      const el = document.getElementById(id);
      if (el) el.value = value;
    }

    const ERROR_INPUT_MAP = {
      'email-error': 'email',
      'sell-error': 'radio-wrap',
      'first-name-error': 'first-name',
      'last-name-error': 'last-name',
      'company-error': 'company',
      'website-error': 'website',
      'phone-error': 'phone',
      'hear-about-us-error': 'hear-about-us',
    };

    function showError(id, msg) {
      const el = document.getElementById(id);
      if (!el) return;
      if (id === 'email-error') hideProTip(); // error takes the slot + border
      if (id === 'website-error') hideWebsiteMismatchTip(); // error takes the slot
      el.textContent = msg;
      el.style.display = 'block';
      const input = document.getElementById(ERROR_INPUT_MAP[id]);
      if (input) input.classList.add('gw-input-error');
    }

    function hideError(id) {
      const el = document.getElementById(id);
      if (!el) return;
      el.textContent = '';
      el.style.display = 'none';
      const input = document.getElementById(ERROR_INPUT_MAP[id]);
      if (input) input.classList.remove('gw-input-error');
    }

    function setLoading(btnId, isLoading, loadingText) {
      const btn = document.getElementById(btnId);
      if (!btn) return;
      if (isLoading) {
        if (!btn.hasAttribute('data-original-html')) btn.setAttribute('data-original-html', btn.innerHTML);
        btn.innerHTML = loadingText || 'Please wait...';
        btn.style.opacity = '0.7';
        btn.style.pointerEvents = 'none';
      } else {
        const orig = btn.getAttribute('data-original-html');
        if (orig) {
          btn.innerHTML = orig;
          btn.removeAttribute('data-original-html');
        }
        btn.style.opacity = '1';
        btn.style.pointerEvents = 'auto';
      }
    }

    function init() {
      ['step-2', 'step-3', 'step-disqualified', 'step-disqualified-thanks'].forEach((id) => {
        const el = document.getElementById(id);
        if (el) el.style.display = 'none';
      });

      initSession();
      captureUTMs();
      captureMetaAttribution();
      prefillHearAboutUs();
      saveSession();
      prefillFromURL();
      initEmailProTip();
      initWebsiteCheck();
      initButtons();
      initEnterKey();
      initBrowserBack();
      initRHBookingListener();

      console.log('[GW] ✅ Form initialised v4.9.7 (/demo).', 'Session:', formState.session_id, '| Page:', formState.page_url, '| Landing:', formState.landing_page, '| Previous:', formState.previous_page || 'none', '| Referrer:', formState.referrer, formState.fbc ? '| fbc: ' + formState.fbc.substring(0, 20) + '...' : '', formState.fbp ? '| fbp: ' + formState.fbp : '');
    }

    if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init);
    else init();
  })();
