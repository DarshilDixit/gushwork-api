/* ==========================================================
   GUSHWORK — MULTI-STEP FORM  v3.4
   Hosted on GitHub — reference via jsDelivr CDN
   https://cdn.jsdelivr.net/gh/DarshilDixit/gushwork-api@main/gushwork-form.js

   Contains:
   - Styles (error messages + step 3 full width + Cal embed)
   - Phone input (Memberstack intl-tel-input)
   - Form logic (validation, enrichment, ELV, Cal, Railway)

   v3.4 changes:
   - Double submit fix — _submitting debounce added to all step handlers
   - HubSpot synthetic form submit fired on Cal booking
========================================================== */

/* --------------------------------------------------------
   INJECT STYLES
-------------------------------------------------------- */
(function injectStyles() {
  const css = `
    /* Error messages */
    [id$="-error"] {
      display: none;
      color: #e53e3e;
      font-size: 13px;
      margin-top: 4px;
    }

    /* Smooth transition on left panel */
    .cta_testimonial-wrapper {
      transition: opacity 0.4s ease, max-width 0.4s ease, padding 0.4s ease;
      overflow: hidden;
    }

    /* Collapse left panel on step 3 */
    .cta_component.step3-active .cta_testimonial-wrapper {
      opacity:        0;
      max-width:      0 !important;
      min-width:      0 !important;
      width:          0 !important;
      padding:        0 !important;
      margin:         0 !important;
      pointer-events: none;
      flex:           0 0 0 !important;
    }

    /* Override grid/flex on parent to let form-holder take full width */
    .cta_component.step3-active {
      display:               flex !important;
      grid-template-columns: none !important;
    }

    /* form-holder takes all remaining space */
    .cta_component.step3-active .form-holder {
      flex:      1 1 100% !important;
      width:     100% !important;
      max-width: 100% !important;
    }

    /* Cal embed fills the space */
    .cta_component.step3-active #my-cal-inline-demo-testing {
      width:  100% !important;
      height: 600px !important;
    }
  `;
  const style = document.createElement('style');
  style.textContent = css;
  document.head.appendChild(style);
})();

/* --------------------------------------------------------
   INJECT PHONE INPUT DEPENDENCIES
   Memberstack intl-tel-input v17
-------------------------------------------------------- */
(function injectPhoneDeps() {
  const link = document.createElement('link');
  link.rel  = 'stylesheet';
  link.type = 'text/css';
  link.href = 'https://cdnjs.cloudflare.com/ajax/libs/intl-tel-input/17.0.8/css/intlTelInput.min.css';
  document.head.appendChild(link);

  function loadScript(src, onload) {
    const s = document.createElement('script');
    s.src = src;
    s.onload = onload;
    document.head.appendChild(s);
  }

  loadScript('https://cdnjs.cloudflare.com/ajax/libs/jquery/3.6.0/jquery.min.js', function() {
    loadScript('https://cdnjs.cloudflare.com/ajax/libs/intl-tel-input/17.0.8/js/intlTelInput.min.js', function() {
      loadScript('https://cdnjs.cloudflare.com/ajax/libs/intl-tel-input/17.0.8/js/utils.js', function() {
        initPhoneInputs();
      });
    });
  });

  function initPhoneInputs() {
    if (typeof $ === 'undefined' || typeof window.intlTelInput === 'undefined') return;
    $('input[ms-code-phone-number]').each(function() {
      var input = this;
      var preferredCountries = $(input).attr('ms-code-phone-number').split(',');
      var iti = window.intlTelInput(input, {
        preferredCountries: preferredCountries,
        utilsScript: 'https://cdnjs.cloudflare.com/ajax/libs/intl-tel-input/17.0.8/js/utils.js'
      });
      input._iti = iti;

      $.get('https://ipinfo.io', function(response) {
        iti.setCountry(response.country);
      }, 'jsonp');

      input.addEventListener('change', function() {
        input.value = iti.getNumber(intlTelInputUtils.numberFormat.NATIONAL);
      });
      input.addEventListener('keyup', function() {
        input.value = iti.getNumber(intlTelInputUtils.numberFormat.NATIONAL);
      });

      var form = $(input).closest('form');
      form.submit(function() {
        input.value = iti.getNumber(intlTelInputUtils.numberFormat.INTERNATIONAL);
      });
    });
  }
})();

/* --------------------------------------------------------
   FORM LOGIC
-------------------------------------------------------- */
(function () {

  /* -------------------------------------------------------
     ⚙️  CONFIG
  ------------------------------------------------------- */
  const RAILWAY_API_URL   = 'https://gushwork-api-production.up.railway.app';
  const CAL_NAMESPACE     = 'demo-testing';
  const CAL_LINK          = 'team/gushwork/demo';
  const CAL_ELEMENT       = '#my-cal-inline-demo-testing';
  const ENRICHMENT_TTL_MS = 7 * 24 * 60 * 60 * 1000;

  /* -------------------------------------------------------
     FORM STATE
  ------------------------------------------------------- */
  const formState = {
    session_id:            '',
    page_url:              '',
    email:                 '',
    website:               '',
    sell_to:               '',
    first_name:            '',
    last_name:             '',
    phone:                 '',
    company:               '',
    hear_about_us:         '',
    utm_source:            '',
    utm_medium:            '',
    utm_campaign:          '',
    utm_content:           '',
    referrer:              '',
    prefill_source:        '',
    enriched_title:        '',
    enriched_company_size: '',
    enriched_industry:     '',
    enriched_linkedin:     '',
    step_reached:          1,
    completed:             false,
    disqualified:          false,
    disqualified_reason:   ''
  };

  let _enrichedForEmail  = '';
  let _lastVerifiedEmail = '';
  let _submitting        = false; // global debounce flag — prevents double submit on any step

  /* =======================================================
     SECTION 1 — INITIALISATION
  ======================================================= */

  function initSession() {
    let sid = sessionStorage.getItem('gw_session_id');
    if (!sid) {
      sid = (typeof crypto !== 'undefined' && crypto.randomUUID)
        ? crypto.randomUUID()
        : 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, c => {
            const r = Math.random() * 16 | 0;
            return (c === 'x' ? r : (r & 0x3 | 0x8)).toString(16);
          });
      sessionStorage.setItem('gw_session_id', sid);
    }
    formState.session_id = sid;
    formState.page_url   = window.location.href;
    setHidden('session-id', sid);
  }

  function captureUTMs() {
    const p = new URLSearchParams(window.location.search);
    formState.utm_source   = p.get('utm_source')   || sessionStorage.getItem('gw_utm_source')   || '';
    formState.utm_medium   = p.get('utm_medium')   || sessionStorage.getItem('gw_utm_medium')   || '';
    formState.utm_campaign = p.get('utm_campaign') || sessionStorage.getItem('gw_utm_campaign') || '';
    formState.utm_content  = p.get('utm_content')  || sessionStorage.getItem('gw_utm_content')  || '';
    formState.referrer     = document.referrer || '';

    if (formState.utm_source)   sessionStorage.setItem('gw_utm_source',   formState.utm_source);
    if (formState.utm_medium)   sessionStorage.setItem('gw_utm_medium',   formState.utm_medium);
    if (formState.utm_campaign) sessionStorage.setItem('gw_utm_campaign', formState.utm_campaign);
    if (formState.utm_content)  sessionStorage.setItem('gw_utm_content',  formState.utm_content);

    setHidden('utm-source',   formState.utm_source);
    setHidden('utm-medium',   formState.utm_medium);
    setHidden('utm-campaign', formState.utm_campaign);
    setHidden('utm-content',  formState.utm_content);
    setHidden('referrer',     formState.referrer);
  }

  function saveSession() {
    if (!isRailwayReady()) return;
    fetch(`${RAILWAY_API_URL}/session`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        session_id:   formState.session_id,
        page_url:     formState.page_url,
        utm_source:   formState.utm_source,
        utm_medium:   formState.utm_medium,
        utm_campaign: formState.utm_campaign,
        utm_content:  formState.utm_content,
        referrer:     formState.referrer
      })
    }).catch(() => {});
  }

  function prefillFromURL() {
    const p     = new URLSearchParams(window.location.search);
    const email = p.get('email') || localStorage.getItem('gw_email') || '';
    const first = p.get('first_name') || '';
    const last  = p.get('last_name')  || '';
    const co    = p.get('company')    || '';

    if (email) {
      setField('email', email);
      formState.email          = email;
      formState.prefill_source = p.get('email') ? 'url_param' : 'returning_visitor';
      setHidden('prefill-source', formState.prefill_source);

      if (isValidEmail(email) && isWorkEmail(email)) {
        const cached = getEnrichmentCache(email);
        if (cached) {
          applyEnrichment(email, cached);
        } else {
          setTimeout(() => {
            enrichEmail(email).then(data => applyEnrichment(email, data));
          }, 800);
        }
      }
    }

    if (first) { setField('first-name', first); formState.first_name = first; }
    if (last)  { setField('last-name',  last);  formState.last_name  = last;  }
    if (co)    { setField('company',    co);     formState.company    = co;    }
  }

  /* =======================================================
     SECTION 2 — PHONE
  ======================================================= */

  function initPhone() {
    setTimeout(() => {
      const wrapper = document.querySelector('#phone')?.closest('.iti');
      if (wrapper) { wrapper.style.width = '100%'; wrapper.style.display = 'block'; }
    }, 500);
  }

  function getPhoneNumber() {
    const phoneEl = document.getElementById('phone');
    if (!phoneEl) return '';

    if (phoneEl._iti) {
      const full = phoneEl._iti.getNumber();
      if (full) return full;
    }

    const value = phoneEl.value.trim();
    if (!value) return '';
    if (value.startsWith('+')) return value;
    const dialCodeEl = document.querySelector('.iti__selected-dial-code');
    const dialCode   = dialCodeEl ? dialCodeEl.textContent.trim() : '';
    return dialCode ? `${dialCode}${value.replace(/[\s\-\(\)]/g, '')}` : value;
  }

  /* =======================================================
     SECTION 3 — VALIDATION
  ======================================================= */

  function isValidEmail(email) {
    return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email);
  }

  function isWorkEmail(email) {
    const blocked = [
      'gmail.com','yahoo.com','hotmail.com','outlook.com','icloud.com',
      'protonmail.com','aol.com','mail.com','yahoo.in','rediffmail.com',
      'ymail.com','live.com','msn.com','me.com','mac.com','googlemail.com'
    ];
    return !blocked.includes(email.split('@')[1]?.toLowerCase() || '');
  }

  function isValidURL(url) {
    try {
      const u = new URL(url.startsWith('http') ? url : 'https://' + url);
      return u.hostname.includes('.');
    } catch { return false; }
  }

  function validateStep1() {
    let valid = true;
    const email = getField('email');

    if (!email)                    { showError('email-error', 'Email is required.');                  valid = false; }
    else if (!isValidEmail(email)) { showError('email-error', 'Please enter a valid email address.'); valid = false; }
    else                             hideError('email-error');

    const sellTo =
      document.querySelector('input[name="sell-to"]:checked')?.value ||
      (document.getElementById('sell-b2b')?.checked   ? 'B2B'   : '') ||
      (document.getElementById('sell-b2c')?.checked   ? 'B2C'   : '') ||
      (document.getElementById('sell-mixed')?.checked ? 'Mixed' : '');

    if (!sellTo) { showError('sell-error', 'Please select who you sell to.'); valid = false; }
    else           hideError('sell-error');

    return { valid, sellTo };
  }

  function validateStep2() {
    let valid = true;

    const firstName = getField('first-name');
    if (!firstName) { showError('first-name-error', 'First name is required.');  valid = false; } else hideError('first-name-error');

    const lastName = getField('last-name');
    if (!lastName)  { showError('last-name-error',  'Last name is required.');   valid = false; } else hideError('last-name-error');

    const rawPhone = (document.getElementById('phone')?.value || '').replace(/\D/g, '');
    if (!rawPhone || rawPhone.length < 6) { showError('phone-error', 'Please enter a valid phone number.'); valid = false; } else hideError('phone-error');

    const company = getField('company');
    if (!company)   { showError('company-error', 'Company name is required.');   valid = false; } else hideError('company-error');

    const website = getField('website');
    if (!website)                  { showError('website-error', 'Website URL is required.');                  valid = false; }
    else if (!isValidURL(website)) { showError('website-error', 'Please enter a valid URL (e.g. acme.com).'); valid = false; }
    else                             hideError('website-error');

    const hearAboutUs = getField('hear-about-us');
    if (!hearAboutUs) { showError('hear-about-us-error', 'Please let us know how you heard about us.'); valid = false; } else hideError('hear-about-us-error');

    return valid;
  }

  function validateDisqualified() {
    const chosen =
      document.getElementById('disq-waitlist')?.checked ? 'waitlist' :
      document.getElementById('disq-b2b')?.checked      ? 'b2b'      : '';

    if (!chosen) {
      showError('disq-error', 'Please select an option to continue.');
      return { valid: false, choice: '' };
    }
    hideError('disq-error');
    return { valid: true, choice: chosen };
  }

  /* =======================================================
     SECTION 4 — EMAIL VERIFICATION
  ======================================================= */

  async function verifyEmail(email) {
    if (!isRailwayReady()) return true;
    if (email === _lastVerifiedEmail) return true;

    try {
      const res  = await fetch(`${RAILWAY_API_URL}/verify-email`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ email })
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

    const currentEl = allSteps
      .map(id => document.getElementById(id))
      .find(el => el && el.style.display !== 'none');

    const nextEl = document.getElementById(stepId);
    if (!nextEl) return;

    const ctaComponent = document.querySelector('.cta_component');
    if (ctaComponent) {
      if (stepId === 'step-3') {
        ctaComponent.classList.add('step3-active');
      } else {
        ctaComponent.classList.remove('step3-active');
      }
    }

    if (currentEl && currentEl !== nextEl) {
      currentEl.style.transition = 'opacity 0.3s ease';
      currentEl.style.opacity    = '0';
      setTimeout(() => {
        currentEl.style.display = 'none';
        currentEl.style.opacity = '1';
        nextEl.style.opacity    = '0';
        nextEl.style.display    = 'block';
        nextEl.style.transition = 'opacity 0.3s ease';
        nextEl.scrollIntoView({ behavior: 'smooth', block: 'start' });
        requestAnimationFrame(() => {
          requestAnimationFrame(() => { nextEl.style.opacity = '1'; });
        });
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
    if (!isRailwayReady()) return true;
    try {
      const res = await fetch(`${RAILWAY_API_URL}/partial`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(formState)
      });
      return res.ok;
    } catch (err) { console.warn('[GW] Partial capture failed:', err); return true; }
  }

  async function submitLead() {
    formState.completed = true;
    setHidden('completed', 'true');
    if (!isRailwayReady()) return true;
    try {
      const res = await fetch(`${RAILWAY_API_URL}/submit`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(formState)
      });
      return res.ok;
    } catch (err) { console.warn('[GW] Submit failed:', err); return true; }
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
    } catch { return null; }
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
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ email, session_id: formState.session_id })
      });
      if (!res.ok) return null;
      return await res.json();
    } catch { return null; }
    finally { _enrichInFlight.delete(email); }
  }

  function clearEnrichedFields() {
    setField('first-name', ''); formState.first_name = '';
    setField('last-name',  ''); formState.last_name  = '';
    setField('company',    ''); formState.company    = '';
    setField('website',    ''); formState.website    = '';
    formState.enriched_title         = '';
    formState.enriched_company_size  = '';
    formState.enriched_industry      = '';
    formState.enriched_linkedin      = '';
    setHidden('enriched-title',        '');
    setHidden('enriched-company-size', '');
    setHidden('enriched-industry',     '');
    setHidden('enriched-linkedin',     '');
    _enrichedForEmail = '';
  }

  function applyEnrichment(email, data) {
    if (!data) return;
    formState.enriched_title         = data.title        || '';
    formState.enriched_company_size  = data.company_size || '';
    formState.enriched_industry      = data.industry     || '';
    formState.enriched_linkedin      = data.linkedin_url || '';
    setHidden('enriched-title',        formState.enriched_title);
    setHidden('enriched-company-size', formState.enriched_company_size);
    setHidden('enriched-industry',     formState.enriched_industry);
    setHidden('enriched-linkedin',     formState.enriched_linkedin);

    if (data.first_name && !getField('first-name')) { setField('first-name', data.first_name); formState.first_name = data.first_name; }
    if (data.last_name  && !getField('last-name'))  { setField('last-name',  data.last_name);  formState.last_name  = data.last_name;  }
    if (data.company    && !getField('company'))    { setField('company',    data.company);    formState.company    = data.company;    }

    if (data.website && !getField('website')) {
      const clean = data.website.replace(/^https?:\/\//, '').replace(/\/$/, '');
      setField('website', clean);
      formState.website = clean;
    }

    setEnrichmentCache(email, data);
    _enrichedForEmail = email;
  }

  function triggerEnrichment(email) {
    if (!email || !isValidEmail(email) || !isWorkEmail(email)) return;
    if (email === _enrichedForEmail) return;
    const cached = getEnrichmentCache(email);
    if (cached) {
      applyEnrichment(email, cached);
    } else {
      enrichEmail(email).then(data => applyEnrichment(email, data));
    }
  }

  /* =======================================================
     SECTION 8 — CAL.COM EMBED
  ======================================================= */

  function initCalLoader() {
    (function (C, A, L) {
      let p = function (a, ar) { a.q.push(ar); };
      let d = C.document;
      C.Cal = C.Cal || function () {
        let cal = C.Cal; let ar = arguments;
        if (!cal.loaded) {
          cal.ns = {}; cal.q = cal.q || [];
          d.head.appendChild(d.createElement('script')).src = A;
          cal.loaded = true;
        }
        if (ar[0] === L) {
          const api = function () { p(api, arguments); };
          const namespace = ar[1];
          api.q = api.q || [];
          if (typeof namespace === 'string') {
            cal.ns[namespace] = cal.ns[namespace] || api;
            p(cal.ns[namespace], ar);
            p(cal, ['initNamespace', namespace]);
          } else p(cal, ar);
          return;
        }
        p(cal, ar);
      };
    })(window, 'https://app.cal.com/embed/embed.js', 'init');
    Cal('init', CAL_NAMESPACE, { origin: 'https://app.cal.com' });
  }

  let _calMounted = false;

  function resetCalEmbed() {
    const el = document.querySelector(CAL_ELEMENT);
    if (el) el.innerHTML = '';
    _calMounted = false;
  }

  function mountCalEmbed() {
    if (_calMounted) return;
    if (!window.Cal || !window.Cal.ns?.[CAL_NAMESPACE]) {
      setTimeout(mountCalEmbed, 500);
      return;
    }
    _calMounted = true;

    const calName    = `${formState.first_name} ${formState.last_name}`.trim();
    const calEmail   = formState.email;
    const calWebsite = formState.website
      ? (formState.website.startsWith('http') ? formState.website : 'https://' + formState.website)
      : '';

    Cal.ns[CAL_NAMESPACE]('inline', {
      elementOrSelector: CAL_ELEMENT,
      calLink: CAL_LINK,
      config: {
        layout:              'month_view',
        theme:               'light',
        name:                calName,
        email:               calEmail,
        attendeePhoneNumber: formState.phone,
        company:             formState.company,
        website:             calWebsite,
        source:              formState.hear_about_us
      }
    });
    Cal.ns[CAL_NAMESPACE]('ui', { hideEventTypeDetails: true, layout: 'month_view' });
    Cal.ns[CAL_NAMESPACE]('on', {
      action: 'bookingSuccessfulV2',
      callback: async (e) => {
        const data = e?.detail?.data || {};
        if (!data.uid) return;
        console.log('[GW] ✅ Booking confirmed:', data.uid);

        // Fire synthetic form submit for HubSpot tracking
        // Populate fields first so HubSpot sees real values
        const hsEmail = document.getElementById('email');
        const hsFirst = document.getElementById('first-name');
        const hsLast  = document.getElementById('last-name');
        const hsPhone = document.getElementById('phone');
        const hsCo    = document.getElementById('company');
        const hsWeb   = document.getElementById('website');
        if (hsEmail) hsEmail.value = formState.email;
        if (hsFirst) hsFirst.value = formState.first_name;
        if (hsLast)  hsLast.value  = formState.last_name;
        if (hsPhone) hsPhone.value = formState.phone;
        if (hsCo)    hsCo.value    = formState.company;
        if (hsWeb)   hsWeb.value   = formState.website;
        const form = document.querySelector('form');
        if (form) {
          form.dispatchEvent(new Event('submit', { bubbles: true, cancelable: true }));
          console.log('[GW] HubSpot synthetic submit fired');
        }

        if (isRailwayReady()) {
          await fetch(`${RAILWAY_API_URL}/booking-confirmed`, {
            method: 'POST', headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              session_id:  formState.session_id,
              booking_uid: data.uid,
              start_time:  data.startTime     || '',
              end_time:    data.endTime       || '',
              event_type:  data.eventTypeSlug || ''
            })
          }).catch(() => {});
        }
      }
    });
  }

  /* =======================================================
     SECTION 9 — STEP HANDLERS
     v3.4: _submitting flag on ALL handlers prevents any
     double submission from double clicks or Enter key.
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
      formState.email   = email;
      formState.sell_to = sellTo;
      localStorage.setItem('gw_email', formState.email);

      triggerEnrichment(formState.email);

      if (sellTo === 'B2C' || sellTo === 'Mixed') {
        showStep('step-disqualified');
      } else {
        await savePartial(1);
        showStep('step-2');
      }
    } finally {
      _submitting = false;
      setLoading('step-1-next', false);
    }
  }

  async function handleDisqualifiedNext() {
    if (_submitting) return;
    const { valid, choice } = validateDisqualified();
    if (!valid) return;

    _submitting = true;
    setLoading('step-disqualified-next', true);

    try {
      formState.disqualified_reason = choice;

      if (choice === 'waitlist') {
        formState.disqualified = true;
        await savePartial(1);
        showStep('step-disqualified-thanks');
      } else if (choice === 'b2b') {
        formState.disqualified = false;
        formState.sell_to = 'B2B (clarified from ' + formState.sell_to + ')';
        await savePartial(1);
        showStep('step-2');
      }
    } finally {
      _submitting = false;
      setLoading('step-disqualified-next', false);
    }
  }

  async function handleStep2Next() {
    if (_submitting) return;
    const valid = validateStep2();
    if (!valid) return;

    _submitting = true;
    setLoading('step-2-next', true);

    try {
      formState.first_name    = getField('first-name');
      formState.last_name     = getField('last-name');
      formState.phone         = getPhoneNumber();
      formState.company       = getField('company');
      formState.website       = getField('website');
      formState.hear_about_us = getField('hear-about-us');

      await submitLead();

      showStep('step-3');
      resetCalEmbed();
      mountCalEmbed();
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
      const step1  = document.getElementById('step-1');
      const stepDq = document.getElementById('step-disqualified');
      const step2  = document.getElementById('step-2');
      if      (step1  && step1.style.display  !== 'none') { e.preventDefault(); handleStep1Next(); }
      else if (stepDq && stepDq.style.display !== 'none') { e.preventDefault(); handleDisqualifiedNext(); }
      else if (step2  && step2.style.display  !== 'none') { e.preventDefault(); handleStep2Next(); }
    });
  }

  /* =======================================================
     SECTION 11 — BUTTON LISTENERS
  ======================================================= */

  function initButtons() {
    const btn1   = document.getElementById('step-1-next');
    const btn2   = document.getElementById('step-2-next');
    const back2  = document.getElementById('step-2-back');
    const back3  = document.getElementById('step-3-back');
    const btnDq  = document.getElementById('step-disqualified-next');
    const backDq = document.getElementById('step-disqualified-back');

    if (btn1)   btn1.addEventListener('click',   (e) => { e.preventDefault(); handleStep1Next(); });
    if (btn2)   btn2.addEventListener('click',   (e) => { e.preventDefault(); handleStep2Next(); });
    if (back2)  back2.addEventListener('click',  (e) => { e.preventDefault(); showStep('step-1'); });
    if (back3)  back3.addEventListener('click',  (e) => { e.preventDefault(); showStep('step-2'); });
    if (btnDq)  btnDq.addEventListener('click',  (e) => { e.preventDefault(); handleDisqualifiedNext(); });
    if (backDq) backDq.addEventListener('click', (e) => { e.preventDefault(); showStep('step-1'); });
  }

  /* =======================================================
     UTILITIES
  ======================================================= */

  function getField(id)        { return (document.getElementById(id)?.value || '').trim(); }
  function setField(id, value) { const el = document.getElementById(id); if (el) el.value = value; }
  function setHidden(id, value){ const el = document.getElementById(id); if (el) el.value = value; }
  function showError(id, msg)  { const el = document.getElementById(id); if (!el) return; el.textContent = msg; el.style.display = 'block'; }
  function hideError(id)       { const el = document.getElementById(id); if (!el) return; el.textContent = ''; el.style.display = 'none'; }

  function setLoading(btnId, isLoading, loadingText) {
    const btn = document.getElementById(btnId);
    if (!btn) return;
    if (isLoading) {
      btn.setAttribute('data-original-text', btn.textContent);
      btn.textContent         = loadingText || 'Please wait...';
      btn.style.opacity       = '0.7';
      btn.style.pointerEvents = 'none';
    } else {
      btn.textContent         = btn.getAttribute('data-original-text') || 'Next';
      btn.style.opacity       = '1';
      btn.style.pointerEvents = 'auto';
    }
  }

  /* =======================================================
     INIT
  ======================================================= */

  function init() {
    ['step-2', 'step-3', 'step-disqualified', 'step-disqualified-thanks'].forEach(id => {
      const el = document.getElementById(id);
      if (el) el.style.display = 'none';
    });

    initSession();
    captureUTMs();
    saveSession();
    prefillFromURL();
    initPhone();
    initButtons();
    initEnterKey();
    initCalLoader();

    console.log('[GW] ✅ Form initialised. Session:', formState.session_id, '| Page:', formState.page_url);
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }

})();
