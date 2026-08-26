/* ==========================================================
  GUSHWORK — MULTI-STEP FORM  v5.7.2-ads  (ADS PAGE VERSION)

  Tracks /demo v5.7.1. Full feature parity with /demo, EXCEPT the
  booking step, which keeps the Ads page's fullscreen modal
  presentation — opened after step 2 — instead of /demo's inline
  column render, AND the close affordances that modal needs (v5.7.2).
  Those are the only intended divergences: everything else here is a
  port of gushwork-form.js and should be kept in step with it. A
  modal needs a way out and an inline column does not, so this
  section has no /demo counterpart to track.

  v5.7.2-ads — THE MODAL CAN BE CLOSED.
    It previously had no exit at all. Not a cross, not Escape, not a
    backdrop tap, and not even a successful booking — MEETING_BOOKED
    fires its events and leaves the overlay up. The only way out was
    the browser back button, which navigated to step 2 and invited a
    second submission.
      - Cross, backdrop tap, and Escape all dismiss it. Back does too,
        and no longer walks to step 2: a second submission creates a
        second lead row, and the duplicate-booking guard reads only the
        newest row per email, which is how one person ends up holding
        two calendar slots.
      - DISMISS IS A CLASS, NOT display:none. Hiding via style would
        trip the observer's un-portal branch, and reparenting an iframe
        discards its browsing context — the calendar would reload and
        lose the date they had already picked. A class leaves the inline
        style and DOM position alone, so reopening resumes exactly where
        they were.
      - On dismiss the hero shows a resume card, built by this script so
        no Webflow republish is needed. It reads as UNFINISHED on
        purpose: no tick, no thanks, and no promise that anyone will be
        in touch. Outreach is not standard here, and implying it is
        gives a lead a reason not to book. One button, no alternative.
      - A lead who has already booked gets different copy, so nobody is
        nagged to book a slot they already hold.
    Changes no validation, no submission, and no Meta CAPI behaviour.

  v5.7.1-ads — PARITY CATCH-UP with /demo v5.6.0 and v5.7.0/v5.7.1.
    This file forked from /demo v5.3.0 on 14 Aug and missed the two
    releases that followed, so all four items below are ports, not new
    work. No change to the modal, the step ladder, or any Ads-specific
    presentation. Nothing here alters which leads get blocked or which
    fire Meta CAPI events.
      - SERVER-SIDE DNS FALLBACK (v5.7.0). Stage 1 runs in the VISITOR'S
        browser over DNS-over-HTTPS, so it inherits their network
        restrictions — a corporate firewall or the Great Firewall blocks
        it and a real business is marked unverified. When the browser
        lookup is blocked we now ask the server, which has no such
        restrictions, via POST /resolve-website. Fails closed to the
        browser's original passing verdict.
      - EMAIL IN THE WEBSITE FIELD (v5.7.0). user@domain.com PASSES
        isValidURL, because it is a legal URL with a username — so it
        reached the backend and threw a credentials error. Caught at
        validation now, with the domain offered as a one-tap fix.
      - SOFT EMAIL TYPO NUDGE (v5.6.0, made local in v5.7.1). A
        typosquat like gmailc.com verifies cleanly, so the rejection-only
        suggester never fired. This adds a non-blocking, tap-to-accept
        nudge in its OWN element (#gw-email-typo-hint), computed locally
        so an ELV timeout cannot swallow it. `valid` stays true.
      - WEBSITE TYPO FROM THE EMAIL LOCAL PART (v5.7.0). The existing
        suggester needs a business email domain to compare against, so a
        Gmail user got nothing. Rule 2 in suggestWebsiteDomainFix()
        reconstructs the domain from the local part, guarded to one edit.

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
  .gw-email-fix {
  color: #2f6bff;
  text-decoration: underline;
  text-underline-offset: 2px;
  cursor: pointer;
  padding: 6px 3px;
  margin: -6px -3px;
  border-radius: 4px;
  }
  .gw-email-fix:hover,
  .gw-email-fix:focus-visible {
  background: rgba(47, 107, 255, 0.08);
  outline: none;
  }
  #gw-email-typo-hint {
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
  SECTION 0 — MARKUP BACKFILL  (ADS-PAGE SPECIFIC)

  /demo's page markup carries a phone field and a full set of error
  divs. The Ads pages do not. Rather than making that a manual Webflow
  checklist, this block creates anything missing — same pattern
  ensureWebsiteMismatchTip() already uses for its own element.

  Runs before SECTION 0A so injected inputs get float-label wrapped,
  and long before initPhoneInputs() (which waits on 3 CDN scripts).

  Everything here is a no-op when the element already exists, so adding
  the fields properly in Webflow later changes nothing.
  -------------------------------------------------------- */
  (function markupBackfill() {
    // Set to false if you add the phone field in Webflow instead.
    const AUTO_INJECT_PHONE = true;
    // intl-tel-input preferred-country order.
    const PHONE_PREFERRED = 'us,in,gb';

    // error div id -> the input it belongs after
    const ERROR_FIELDS = {
      'email-error': 'email',
      'disq-error': 'disq-waitlist',
      'sell-error': 'radio-wrap',
      'first-name-error': 'first-name',
      'last-name-error': 'last-name',
      'company-error': 'company',
      'website-error': 'website',
      'phone-error': 'phone',
      'hear-about-us-error': 'hear-about-us',
    };

    // Insert after the input's outermost field wrapper when there is one,
    // so the error sits below the styled box rather than inside it.
    function outerOf(el) {
      return el.closest('.float-label-wrapper') || el;
    }

    // Everything downstream looks the field up by id 'phone' and needs the
    // ms-code-phone-number attribute for intl-tel-input to bind. A field
    // added in Webflow may have neither, so normalise before deciding
    // whether anything needs creating at all.
    function normalisePhoneField(el) {
      if (!el) return;
      if (el.id !== 'phone') {
        console.log('[GW] adopted existing phone field (id was "' + el.id + '")');
        el.id = 'phone';
      }
      if (!el.hasAttribute('ms-code-phone-number')) {
        el.setAttribute('ms-code-phone-number', PHONE_PREFERRED);
        console.log('[GW] added ms-code-phone-number to existing phone field');
      }
      if (el.type !== 'tel') el.type = 'tel';
      if (!el.classList.contains('input-field')) el.classList.add('input-field');
      if (!el.getAttribute('autocomplete')) el.setAttribute('autocomplete', 'tel');
      // A Webflow "required" attribute would fire native validation before
      // validateStep2 runs, bypassing the free-mailbox-only rule.
      el.removeAttribute('required');
    }

    // Find a phone field the page already has, whatever it was named.
    // Ordered most to least reliable; never matches the other known fields.
    function findExistingPhoneField() {
      const exact = document.getElementById('phone');
      if (exact) return exact;
      const selectors = ['input[ms-code-phone-number]', 'input[type="tel"]', 'input[name*="phone" i]', 'input[id*="phone" i]', 'input[autocomplete="tel"]', 'input[placeholder*="phone" i]', 'input[placeholder*="mobile" i]'];
      const taken = ['email', 'first-name', 'last-name', 'company', 'website', 'hear-about-us'];
      for (let i = 0; i < selectors.length; i++) {
        const found = document.querySelectorAll(selectors[i]);
        for (let j = 0; j < found.length; j++) {
          if (taken.indexOf(found[j].id) === -1) return found[j];
        }
      }
      return null;
    }

    function ensurePhoneField() {
      const existing = findExistingPhoneField();
      if (existing) {
        normalisePhoneField(existing);
        return;
      }
      if (!AUTO_INJECT_PHONE) return;

      // Clone an existing field's wrapper so the injected input inherits
      // the page's exact Webflow styling instead of guessing at CSS.
      const donorInput = document.getElementById('website') || document.getElementById('company') || document.getElementById('last-name');
      if (!donorInput) return;

      const donorWrapper = donorInput.closest('.field-wrapper');
      let phoneInput;

      if (donorWrapper) {
        const clone = donorWrapper.cloneNode(true);
        // strip anything carried over from the donor
        clone.querySelectorAll('[id$="-error"]').forEach((e) => e.remove());
        clone.querySelectorAll('.float-label-wrapper').forEach((w) => {
          const inner = w.querySelector('input');
          if (inner) w.parentNode.insertBefore(inner, w);
          w.remove();
        });
        phoneInput = clone.querySelector('input');
        if (!phoneInput) return;
        phoneInput.removeAttribute('value');
        phoneInput.value = '';
        clone.style.removeProperty('display'); // donor may have been hidden
        donorWrapper.insertAdjacentElement('afterend', clone);
      } else {
        phoneInput = donorInput.cloneNode(true);
        phoneInput.value = '';
        donorInput.insertAdjacentElement('afterend', phoneInput);
      }

      phoneInput.id = 'phone';
      phoneInput.name = 'phone';
      phoneInput.setAttribute('placeholder', 'Phone number');
      phoneInput.removeAttribute('data-typing-placeholder');
      normalisePhoneField(phoneInput);
      console.log('[GW] phone field injected (page had none)');
    }

    function ensureErrorEls() {
      Object.keys(ERROR_FIELDS).forEach(function (errId) {
        if (document.getElementById(errId)) return;
        const input = document.getElementById(ERROR_FIELDS[errId]);
        if (!input) return; // no field, no error slot needed
        const div = document.createElement('div');
        div.id = errId;
        // styling comes from the injected [id$="-error"] rule
        div.style.display = 'none';
        outerOf(input).insertAdjacentElement('afterend', div);
      });
    }

    function run() {
      ensurePhoneField();
      ensureErrorEls();
    }

    if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', run);
    else run();
  })();

  /* --------------------------------------------------------
  SECTION 0A — FLOAT LABEL  (ADS-PAGE SPECIFIC)
  Wraps every .input-field, promotes its placeholder to a floating
  label, and keeps the label state in sync with the value however
  that value arrives: typing, paste, browser autofill, or a direct
  programmatic assignment from Apollo enrichment.
  Runs before the form IIFE's init so wrappers exist when
  initPhoneInputs() calls closest('.float-label-wrapper').
  -------------------------------------------------------- */
  (function injectFloatLabelStyles() {
    const css = `
  .float-label-wrapper {
  position: relative;
  width: 100%;
  }
  .float-label-wrapper .float-label {
  position: absolute;
  left: var(--input-padding-left, 16px);
  top: 50%;
  transform: translateY(-50%);
  font-size: 14px;
  font-weight: 500;
  color: #9a9a9a;
  pointer-events: none;
  transition: top 0.2s ease, transform 0.2s ease, font-size 0.2s ease;
  line-height: 1;
  background: transparent;
  margin: 0;
  padding: 0;
  }
  .float-label-wrapper.has-value .float-label,
  .float-label-wrapper.is-focused .float-label {
  top: 12px;
  transform: translateY(0);
  font-size: 12px;
  font-weight: 500;
  }
  .float-label-wrapper.has-value .input-field,
  .float-label-wrapper.is-focused .input-field {
  padding-top: 20px !important;
  padding-bottom: 4px !important;
  }
  .float-label-wrapper:not(.has-value):not(.is-focused) .input-field {
  padding-top: 0 !important;
  padding-bottom: 0 !important;
  }
  .float-label-wrapper:not(.is-focused) .input-field::placeholder {
  color: transparent !important;
  }
  .float-label-wrapper.is-focused .input-field::placeholder {
  color: #bbbec4 !important;
  }
  .input-field:focus {
  outline: none !important;
  box-shadow: none !important;
  }
  .float-label-wrapper.no-transition .float-label {
  transition: none !important;
  }
  @keyframes onAutoFillStart { from {} to {} }
  .input-field:-webkit-autofill { animation-name: onAutoFillStart; }
  `;
    const style = document.createElement('style');
    style.textContent = css;
    document.head.appendChild(style);
  })();

  (function initFloatLabels() {
    function build() {
      const inputs = document.querySelectorAll('.input-field');

      inputs.forEach(function (input) {
        // Guard against double-wrapping if the old v4.5 paste is still
        // on the page. Remove that block; this only stops the damage.
        if (input.closest('.float-label-wrapper')) return;

        const originalLabel = input.getAttribute('placeholder');
        const typingPlaceholder = input.getAttribute('data-typing-placeholder') || 'name@company.com';
        if (!originalLabel) return;

        const inputPaddingLeft = window.getComputedStyle(input).paddingLeft;

        const wrapper = document.createElement('div');
        wrapper.classList.add('float-label-wrapper');
        wrapper.style.setProperty('--input-padding-left', inputPaddingLeft);
        input.parentNode.insertBefore(wrapper, input);
        wrapper.appendChild(input);

        const label = document.createElement('label');
        label.classList.add('float-label');
        label.textContent = originalLabel;
        wrapper.appendChild(label);

        wrapper.classList.add('no-transition');

        function checkValue() {
          if (input.value.trim() !== '') {
            wrapper.classList.add('has-value');
            input.setAttribute('placeholder', '');
            input.style.removeProperty('color');
          } else {
            wrapper.classList.remove('has-value');
          }
        }

        // Intercept programmatic value assignment (input.value = '...').
        // This is what makes the label float when Apollo enrichment or
        // intl-tel-input reformatting writes a value directly.
        const nativeInputValueSetter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, 'value')?.set;
        if (nativeInputValueSetter) {
          const originalDescriptor = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, 'value');
          Object.defineProperty(input, 'value', {
            set: function (val) {
              nativeInputValueSetter.call(this, val);
              setTimeout(checkValue, 0);
            },
            get: function () {
              return originalDescriptor.get.call(this);
            },
            configurable: true,
          });
        }

        // Staggered checks — catches enrichment landing at any timing
        checkValue();
        setTimeout(checkValue, 50);
        setTimeout(function () { wrapper.classList.remove('no-transition'); }, 100);
        [200, 400, 800, 1500, 3000].forEach(function (ms) { setTimeout(checkValue, ms); });

        var pollCount = 0;
        var lastValue = input.value;
        var poll = setInterval(function () {
          if (input.value !== lastValue) {
            lastValue = input.value;
            checkValue();
          }
          pollCount++;
          if (pollCount >= 20) clearInterval(poll); // 20 x 250ms = 5s
        }, 250);

        new MutationObserver(checkValue).observe(input, { attributes: true, attributeFilter: ['value'] });

        input.addEventListener('animationstart', function (e) {
          if (e.animationName === 'onAutoFillStart') {
            wrapper.classList.add('has-value');
            input.setAttribute('placeholder', '');
            input.style.removeProperty('color');
          }
        });

        input.addEventListener('input', function () {
          if (input.value.trim() !== '') {
            wrapper.classList.add('has-value');
            input.setAttribute('placeholder', '');
            input.style.removeProperty('color');
          } else {
            wrapper.classList.remove('has-value');
            if (wrapper.classList.contains('is-focused')) input.setAttribute('placeholder', typingPlaceholder);
          }
        });

        input.addEventListener('change', checkValue);

        input.addEventListener('focus', function () {
          wrapper.classList.add('is-focused');
          if (input.value.trim() === '') input.setAttribute('placeholder', typingPlaceholder);
          else input.style.removeProperty('color');
        });

        input.addEventListener('blur', function () {
          wrapper.classList.remove('is-focused');
          input.setAttribute('placeholder', '');
          if (input.value.trim() !== '') {
            wrapper.classList.add('has-value');
            input.style.removeProperty('color');
          } else {
            wrapper.classList.remove('has-value');
          }
        });
      });

      // Autofocus email — Ads page behaviour, /demo does not do this
      const emailInput = document.getElementById('email');
      if (emailInput) emailInput.focus();
    }

    // readyState guard: v4.5 used a bare DOMContentLoaded listener, which
    // silently never fired if the script loaded after the event.
    if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', build);
    else build();
  })();

  /* --------------------------------------------------------
  SECTION 0B — SPEECH BUBBLES  (ADS-PAGE SPECIFIC)
  -------------------------------------------------------- */
  (function initSpeechBubbles() {
    function bind() {
      const holders = document.querySelectorAll('.form-person-holder');
      const bubbles = document.querySelectorAll('.speech-bubble');
      holders.forEach((holder, i) => {
        if (!bubbles[i]) return;
        holder.addEventListener('mouseenter', () => { bubbles[i].style.opacity = '1'; });
        holder.addEventListener('mouseleave', () => { bubbles[i].style.opacity = '0'; });
      });
    }
    if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', bind);
    else bind();
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
    // Reasons that HARD BLOCK. All three mean "this isn't a website you own",
    // which has no legitimate case — as opposed to parked/for-sale/page-won't-
    // load, which mean "the site may be broken" and can false-positive on real
    // businesses (see afgmmoving.com). Add or remove a reason here to change
    // scope; nothing else needs touching.
    const WEBSITE_BLOCKING_REASONS = ['nxdomain', 'brand_mismatch', 'mailbox_domain'];
    // ── ADS-PAGE POLICY SWITCH ────────────────────────────────────────
    // v4.5 on this page hard-blocked gmail/yahoo/etc at step 1 with
    // "Please use your work email." /demo does the opposite: it lets them
    // through and makes the PHONE field mandatory instead, because those
    // leads get routed to SDR calling (JustCall) where a number is the
    // whole point. This file follows /demo.
    // Set to true to restore the old Ads-page hard block. Nothing else
    // needs changing — the phone requirement keys off the same isWorkEmail().
    const BLOCK_PERSONAL_EMAILS = false;


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
      fetchWithTimeout(`${RAILWAY_API_URL}/session`, {
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
      }, NET_TIMEOUT_MS.session).catch(() => {});
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

    // Requires a REAL TLD, not merely "contains a dot somewhere".
    // The old `hostname.includes('.')` accepted www.malbecgrillcom because
    // the `www.` supplied the only dot — a real lead's typo then got
    // misfiled as a fake domain downstream. Strip www. first, then demand
    // at least two labels with a final alphabetic label of 2+ chars.
    // Also rejects raw IPs, `localhost`, and bare single-label hosts.
    // (Trade-off: the literal domain `www.com` would be rejected. Nobody
    // submits that as a company site, and stripping conditionally would
    // reopen the malbecgrillcom hole.)
    /* An email address typed into the WEBSITE field PASSES isValidURL,
       because new URL('https://user@domain.com') is a legal URL with a
       username — so it slipped straight through to the backend, which then
       threw "Request cannot be constructed from a URL that includes
       credentials" (seen in the logs on 18 Aug).
       Returns the domain when it is worth offering as a fix, '' otherwise. */
    function emailInWebsiteField(value) {
      const v = String(value || '').trim();
      if (v.indexOf('@') === -1) return '';
      try {
        const u = new URL(/^https?:\/\//i.test(v) ? v : 'https://' + v);
        if (!u.username) return '';
        const host = u.hostname.replace(/^www\./i, '').toLowerCase();
        // A mailbox provider is not their company website, so there is
        // nothing useful to suggest for someone@gmail.com.
        if (PERSONAL_EMAIL_DOMAINS.indexOf(host) !== -1) return '';
        return /^[a-z0-9-]+(\.[a-z0-9-]+)*\.[a-z]{2,}$/i.test(host) ? host : '';
      } catch (e) { return ''; }
    }

    function isValidURL(url) {
      try {
        const u = new URL(url.startsWith('http') ? url : 'https://' + url);
        const bare = u.hostname.replace(/\.$/, '').replace(/^www\./i, '');
        return /^[a-z0-9-]+(\.[a-z0-9-]+)*\.[a-z]{2,}$/i.test(bare);
      } catch {
        return false;
      }
    }

    // Missing-dot typo recovery: malbecgrillcom -> malbecgrill.com.
    // Only 3+ char TLDs, so we never produce silly suggestions like
    // mumbai -> mumb.ai. Returns '' when there's nothing confident to say.
    // v5.3.0: was 12 entries with no country TLDs at all, so a missing dot on
    // e.g. "acmeco.us" or "acme.io" could not be suggested. Sorted longest
    // first at match time, so 'online' still wins over 'in'.
    // Two-letter entries are safe only because suggestUrlFix() requires the
    // host to be longer than the TLD + 1, which stops absurd splits.
    const TYPO_TLDS = [
      'online', 'store', 'agency', 'digital', 'tech', 'site', 'info', 'shop', 'cloud',
      'com', 'net', 'org', 'biz', 'app', 'dev', 'xyz', 'pro', 'ltd', 'inc',
      'io', 'ai', 'co', 'us', 'uk', 'in', 'ca', 'au', 'nz', 'de', 'fr', 'es', 'it', 'nl', 'me', 'tv',
    ];

    function suggestUrlFix(raw) {
      try {
        const u = new URL(raw.startsWith('http') ? raw : 'https://' + raw);
        const host = u.hostname.replace(/\.$/, '').replace(/^www\./i, '').toLowerCase();
        if (host.includes('.')) return ''; // has a dot already — different problem
        // longest first so "online" wins over "in"-style shorter matches
        const sorted = TYPO_TLDS.slice().sort((a, b) => b.length - a.length);
        for (const tld of sorted) {
          if (host.length > tld.length + 1 && host.endsWith(tld)) {
            return host.slice(0, host.length - tld.length) + '.' + tld;
          }
        }
        return '';
      } catch {
        return '';
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
      } else if (BLOCK_PERSONAL_EMAILS && !isTestEmail(email) && !isWorkEmail(email)) {
        showError('email-error', 'Please use your work email.');
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
      } else if (website.indexOf('@') !== -1) {
        // Checked BEFORE isValidURL, which accepts user@domain.com as a
        // valid URL-with-credentials and lets it straight through.
        const emailFix = emailInWebsiteField(website);
        showWebsiteVerdictError(emailFix
          ? { msg: 'Did you mean ' + emailFix + '?', suggestion: emailFix }
          : { msg: 'That looks like an email address. Please enter your website (e.g. acme.com).' });
        valid = false;
      } else if (!isValidURL(website)) {
        const suggestion = suggestUrlFix(website);
        showError('website-error', suggestion ? 'Did you mean ' + suggestion + '?' : 'Please enter a valid URL (e.g. acme.com).');
        valid = false;
      } else hideError('website-error');

      // ── Phone — REQUIRED for free-mailbox leads, optional otherwise ──
      // Free-mail leads are funnelled to SDR calling (JustCall campaign), so a
      // reachable number is the whole point. Business-email leads keep it
      // optional, as before. Test emails are exempt like every other check.
      const phoneEl    = document.getElementById('phone');
      const phoneEmail = getField('email') || formState.email || '';
      const phoneRequired = !!phoneEmail && isValidEmail(phoneEmail) && !isWorkEmail(phoneEmail) && !isTestEmail(phoneEmail);
      const phoneVal   = phoneEl ? phoneEl.value.trim() : '';
      if (phoneEl && phoneRequired && phoneVal === '') {
        showError('phone-error', 'Phone number is required so our team can reach you.');
        valid = false;
      } else if (phoneEl && phoneVal !== '' && phoneEl._iti && typeof phoneEl._iti.isValidNumber === 'function') {
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

    /* Builds #email-protip if the page doesn't have one, exactly as
       ensureWebsiteMismatchTip() does for its own element. /demo gets this
       div from its page markup; the Ads pages don't, and a missing div
       must not be the difference between a working nudge and silence.
       Inline styles mirror the #email-protip rule in injectStyles(), so it
       looks identical whether the div came from Webflow or from here. */
    function ensureEmailProTip() {
      let tip = document.getElementById('email-protip');
      if (tip) return tip;
      const emailInput = document.getElementById('email');
      if (!emailInput) return null;
      tip = document.createElement('div');
      tip.id = 'email-protip';
      tip.style.cssText = 'display:none;align-items:flex-start;gap:4px;color:#FF6A00;font-size:12px;font-weight:500;line-height:1.4;margin-top:4px;';
      const errEl = document.getElementById('email-error');
      if (errEl && errEl.parentNode) errEl.parentNode.insertBefore(tip, errEl.nextSibling);
      else (emailInput.closest('.float-label-wrapper') || emailInput).insertAdjacentElement('afterend', tip);
      return tip;
    }

    function initEmailProTip() {
      const tip = ensureEmailProTip();
      const emailInput = document.getElementById('email');
      if (!tip || !emailInput) return;

      tip.innerHTML = '<img src="https://cdn.prod.website-files.com/65c292289fb0ea1ff3a84bd3/6a573b62ef8929dda9d988f1_WarningCircle.svg" style="width:14px;height:14px;display:block;margin-top:1px;flex-shrink:0;" alt="">' + '<span>Business email preferred over personal email.</span>';

      // allowClear is TRUE only on input (they actually typed something).
      // On blur nothing changed, so clearing there would wipe a verification
      // error that prewarmEmail had just set for this same address — a
      // latent ordering dependency between two blur listeners.
      function updateProTip(allowClear) {
        const email = emailInput.value.trim();
        // Live-clear a stale error once the email becomes format-valid —
        // without this, an error shown on a previous Next click stays
        // visible while typing and permanently suppresses the nudge
        if (allowClear === true && isValidEmail(email)) hideError('email-error');
        const errVisible = document.getElementById('email-error')?.style.display === 'block';
        if (!errVisible && isValidEmail(email) && !isWorkEmail(email)) {
          tip.style.display = 'flex';
          emailInput.classList.add('gw-input-warning');
        } else {
          tip.style.display = 'none';
          emailInput.classList.remove('gw-input-warning');
        }
      }

      emailInput.addEventListener('input', function () { updateProTip(true); });
      emailInput.addEventListener('blur', function () { updateProTip(false); });
      updateProTip(false); // covers URL-param / returning-visitor prefill
    }

    /* =======================================================
    SECTION 3C — WEBSITE EXISTENCE CHECK
    STAGE 1 (here, client-side DoH via dns.google with a
    cloudflare-dns.com fallback — both free, CORS-enabled, no key).
    STAGE 2 is the server-side content check at /verify-website.

    HARD BLOCKS (WEBSITE_BLOCKING_REASONS, unchanged since July 2026 —
    each means "this is not a website you own"):
      1. mailbox_domain  — free-mailbox domain typed as a website
      2. brand_mismatch  — brand domain not matching the email domain
      3. nxdomain        — domain does not exist (www. variant retried,
                           BOTH resolvers must agree, and it is waived
                           when the domain is the lead's own verified
                           email domain — see nxdomain_contradicted)

    NOT a block, only a FLAG (v5.3.0): parking. A registrar parking IP
    or nameserver is shared with registrar URL FORWARDING, so DNS cannot
    distinguish a for-sale lander from a working forward to the owner's
    real site. Stage 1 therefore emits a `parkingHint` and stage 2, which
    follows redirects and measures the page, returns the verdict.
    Real content always outranks the hint.

    Allows: email-only companies (MX records but no A record).
    Fail-open on any DoH/network error — a validation outage must never
    lose a real lead (same policy as ELV).
    Base verdicts cached per domain; email-dependent rules recomputed
    every call. Blur prewarms so the Next click is a cache hit.
    ======================================================= */

    const _websiteVerdicts = new Map(); // domain -> {ok, reason, msg}
    const _websiteInFlight = new Map(); // domain -> Promise<verdict>

    // Big brands lazily typed to get past the field. Allowed ONLY
    // when the lead's email domain matches (real @google.com passes).
    const BRAND_DOMAINS = ['google.com', 'youtube.com', 'facebook.com', 'fb.com', 'instagram.com', 'linkedin.com', 'twitter.com', 'x.com', 'amazon.com', 'amazon.in', 'microsoft.com', 'apple.com', 'netflix.com', 'wikipedia.org', 'whatsapp.com', 'tiktok.com', 'reddit.com', 'openai.com', 'chatgpt.com', 'flipkart.com', 'gushwork.ai'];

    // Subset of BRAND_DOMAINS where a specific PATH is a legitimate stand-in
    // company site — solo professionals / small businesses without their
    // own domain commonly link a real profile/page (e.g. a lawyer's
    // linkedin.com/in/username). A bare domain (no path) is still the
    // lazy-fake pattern and stays blocked exactly as before.
    const SOCIAL_PROFILE_DOMAINS = ['linkedin.com', 'facebook.com', 'fb.com', 'instagram.com', 'twitter.com', 'x.com', 'youtube.com', 'tiktok.com'];

    // LinkedIn and YouTube use STRUCTURED paths — /in/, /company/, /@handle.
    // A bare path like linkedin.com/xyz is not a real profile URL, so it must
    // not earn a clean pass. Facebook / Instagram / X / TikTok genuinely do use
    // bare handles, so they stay permissive (no pattern = any path accepted).
    const SOCIAL_PATH_PATTERNS = {
      'linkedin.com': /^\/(in|company|school|showcase|pub)\/[^\/]+/i,
      'youtube.com': /^\/(@[^\/]+|c\/[^\/]+|channel\/[^\/]+|user\/[^\/]+)/i,
    };

    function getUrlPathname(raw) {
      try {
        const u = new URL(raw.startsWith('http') ? raw : 'https://' + raw);
        return u.pathname || '/';
      } catch {
        return '/';
      }
    }

    /* v5.3.0 — THESE ARE HINTS, NOT VERDICTS.
       Registrar parking IPs are shared with registrar URL-FORWARDING. The same
       Above.com IP serves a for-sale lander and a working forward to the
       owner's real site (afgmmoving.com -> afewgoodmenmoving.com); the same
       GoDaddy anycast pair serves a parked placeholder and a plain redirect.
       DNS cannot tell those apart — only the HTTP redirect destination can.
       So a match here no longer decides anything. It sets `parkingHint`, and
       the server-side content check (which follows redirects and measures the
       page) returns the authoritative verdict. Real content always wins.

       That inversion is what makes this list SAFE to extend, and why a bad
       entry can no longer cost a lead its Meta event. Removed in v5.3.0:
         '216.198.79.1' — commented as a "Hostinger placeholder". It is
         actually VERCEL's anycast apex IP (216.198.79.0/24 is Vercel-owned),
         so every lead whose site is deployed on modern Vercel was stamped
         "parked": ryanlaw.us, tupii.co, grannyathome.com, infinitytech.rw —
         all live businesses, all of whom booked a demo. Note that Vercel's
         other apex IP (64.29.17.1) was never in the list, so the flag fired
         at random depending on resolver ordering.
       3-octet entries are prefix matches; full IPs are exact. */
    const PARKING_IP_HINTS = [
      '162.255.119.', // Namecheap parking / URL-forwarding (shared with real hosting)
      '34.102.136.180', // GoDaddy parking (exact IP)
      '3.33.130.190', // GoDaddy parking / forwarding anycast (exact IP)
      '15.197.148.33', // GoDaddy parking / forwarding anycast (exact IP)
      '91.195.240.', // Sedo
      '91.195.241.', // Sedo
      '185.53.177.', // ParkingCrew
      '185.53.178.', // ParkingCrew
      '185.53.179.', // ParkingCrew
      '199.59.242.', // Bodis
      '199.59.243.', // Bodis
      '208.91.197.', // Confluence Networks parking
      '103.224.182.', // Trellian / Above.com parking AND forwarding
    ];

    // Nameservers used ONLY for parking / domain-sale landers → always block.
    // Match is suffix-based, so ns1.sedoparking.com etc. all hit.
    const PARKING_NS_STRICT = ['sedoparking.com', 'parkingcrew.net', 'bodis.com', 'above.com', 'parklogic.com', 'uniregistrymarket.link', 'afternic.com', 'dan.com', 'abovedomains.com'];
    // NOTE: dns-parking.com was briefly here (v4.9.10) and REMOVED in v4.11.0.
    // It is Hostinger's nameserver hostname for ALL customers, live sites
    // included (processwithbryant.com: real site + Google Workspace MX, was
    // wrongly flagged). Hostinger placeholder pages are caught by the
    // server-side content check instead, which reads the actual page.

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

    /* =======================================================
    SECTION 3D — WEBSITE DOMAIN TYPO SUGGESTER (v5.3.0)

    suggestUrlFix() (SECTION 2) only handles a COMPLETELY MISSING DOT
    ("malbecgrillcom" -> "malbecgrill.com") and runs pre-DNS as a format
    error. It cannot help the cases that actually reach a hard NXDOMAIN
    block, all of which are well-formed URLs:
      - misspelled second level: "chrishenenssyteam.com", typed by
        chris@chrishennessyteam.com. Comparing against the lead's OWN
        email domain is the highest-precision signal available and was
        going unused.
      - wrong TLD: ".con" / ".cmo" / ".ocm" pass isValidURL (they match
        [a-z]{2,}), reach DNS, and hard-block with no suggestion offered.

    Mirrors the server-side suggestDomainFix() in index.js. Duplicated
    deliberately: the NXDOMAIN verdict is computed client-side, and a
    round trip on a blocking path would cost the lead a visible delay.
    Keep the two in step if either is edited.
    ======================================================= */

    function damerauLevenshtein(a, b) {
      // Transposition counts as ONE edit — swapping adjacent letters
      // (hennessy/henenssy) is the commonest typing slip and plain
      // Levenshtein scores it 2, which would miss it.
      const m = a.length, n = b.length;
      if (Math.abs(m - n) > 3) return 99;
      const d = [];
      for (let i = 0; i <= m; i++) { d[i] = new Array(n + 1).fill(0); d[i][0] = i; }
      for (let j = 0; j <= n; j++) d[0][j] = j;
      for (let i = 1; i <= m; i++) {
        for (let j = 1; j <= n; j++) {
          const cost = a[i - 1] === b[j - 1] ? 0 : 1;
          d[i][j] = Math.min(d[i - 1][j] + 1, d[i][j - 1] + 1, d[i - 1][j - 1] + cost);
          if (i > 1 && j > 1 && a[i - 1] === b[j - 2] && a[i - 2] === b[j - 1]) {
            d[i][j] = Math.min(d[i][j], d[i - 2][j - 2] + cost);
          }
        }
      }
      return d[m][n];
    }

    // Mistyped TLDs, longest key first at match time so '.co.m' can't be
    // shadowed by a shorter key. Values must be real TLDs.
    const WEBSITE_BAD_TLDS = {
      '.con': '.com', '.cmo': '.com', '.ocm': '.com', '.vom': '.com', '.xom': '.com',
      '.comm': '.com', '.copm': '.com', '.co.m': '.com', '.cim': '.com', '.cpm': '.com',
      '.ner': '.net', '.nte': '.net', '.nett': '.net', '.het': '.net',
      '.ogr': '.org', '.orgg': '.org', '.rog': '.org', '.or': '.org',
      '.couk': '.co.uk', '.co.ku': '.co.uk',
      '.iu': '.io', '.oi': '.io', '.ai.': '.ai',
    };

    function suggestWebsiteDomainFix(domain, emailDomain, emailLocalPart) {
      const d = String(domain || '').toLowerCase().replace(/\.$/, '');
      if (!d || d.indexOf('.') === -1) return '';

      // 1. The lead's own email domain. Highest precision by far — if they
      //    can receive mail at it, it exists, and a near-match on the
      //    website field is almost certainly the same domain mistyped.
      const ed = String(emailDomain || '').toLowerCase();
      if (ed && ed !== d && ed.indexOf('.') !== -1 && PERSONAL_EMAIL_DOMAINS.indexOf(ed) === -1) {
        const dist = damerauLevenshtein(d, ed);
        // 1 edit always. 2 edits only on longer names, where two slips are
        // proportionally small and a collision with a different real
        // domain is much less likely.
        if (dist === 1 || (dist === 2 && ed.length >= 10)) return ed;
      }

      /* 2. The email's LOCAL PART, when the email is a personal one (v5.7.0).
            Rule 1 needs a business email domain to compare against, so a
            Gmail user gets nothing from it. But the local part often IS the
            company name:
              typed:  gslgraphics.com          (no A record, no mail)
              email:  glsgraphics1@gmail.com
              real:   glsgraphics.com          (live, Microsoft 365 mail)
            GSL vs GLS — one transposition. Reconstructing the local part
            with the typed TLD produces exactly the right answer.
            Guarded hard by edit distance: john.smith@gmail.com typing
            acme.com yields 'johnsmith.com', which is nowhere near, so
            nothing is suggested. */
      if (ed && PERSONAL_EMAIL_DOMAINS.indexOf(ed) !== -1) {
        const local = String(emailLocalPart || '').toLowerCase()
          .replace(/[^a-z0-9]/g, '')  // drop dots, plus-tags, underscores
          .replace(/\d+$/, '');       // and a trailing counter: glsgraphics1 -> glsgraphics
        const typedTld = d.slice(d.indexOf('.'));
        if (local.length >= 5) {
          const candidate = local + typedTld;
          if (candidate !== d && damerauLevenshtein(d, candidate) === 1) return candidate;
        }
      }

      // 3. Mistyped TLD. Longest suffix first so overlapping keys resolve
      //    to the most specific match.
      const bad = Object.keys(WEBSITE_BAD_TLDS).sort((a, b) => b.length - a.length);
      for (let i = 0; i < bad.length; i++) {
        if (d.length > bad[i].length && d.slice(-bad[i].length) === bad[i]) {
          return d.slice(0, d.length - bad[i].length) + WEBSITE_BAD_TLDS[bad[i]];
        }
      }
      return '';
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

    /* -------------------------------------------------------
    Renders a website rejection, plus a tap-to-accept fix when a likely
    typo was identified. Same interaction and same single-blue-control
    rule as showEmailVerdictError() — the corrected domain itself is the
    tap target, inline in the sentence, never a competing button.
    ------------------------------------------------------- */
    function showWebsiteVerdictError(verdict) {
      const errEl = document.getElementById('website-error');
      const websiteInput = document.getElementById('website');
      const msg = verdict.msg || "This website doesn't appear to exist. Please check the URL.";

      if (!verdict.suggestion || !errEl || !websiteInput) {
        showError('website-error', msg);
        return;
      }

      showError('website-error', msg); // sets state: red border, display block
      const marker = 'Did you mean ';
      if (msg.indexOf(marker) !== 0) return; // wording changed; leave as plain text

      // DOM nodes, never innerHTML — the domain is user input and must
      // never be parsed as markup.
      errEl.textContent = '';
      errEl.appendChild(document.createTextNode(marker));
      const link = document.createElement('span');
      link.id = 'website-suggestion';
      link.className = 'gw-email-fix'; // reuse the existing underlined-link style
      link.setAttribute('role', 'button');
      link.setAttribute('tabindex', '0');
      link.textContent = verdict.suggestion;
      const apply = function () {
        websiteInput.value = verdict.suggestion;
        hideError('website-error');
        websiteInput.dispatchEvent(new Event('input', { bubbles: true }));
        websiteInput.focus();
        // Re-check the corrected domain immediately so the cache is warm and
        // Next doesn't stall. Cannot loop: the only email-domain suggestion
        // resolves to 'nxdomain_contradicted' (a pass) if it is itself
        // unresolvable, and a TLD fix always changes the string.
        checkWebsite(verdict.suggestion).then(function (v) {
          if (!v.ok && websiteInput.value.trim() === verdict.suggestion) showWebsiteVerdictError(v);
        }).catch(function () { /* fail open, same as everywhere else */ });
      };
      link.addEventListener('click', apply);
      link.addEventListener('keydown', function (e) {
        if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); apply(); }
      });
      errEl.appendChild(link);
      errEl.appendChild(document.createTextNode('?'));
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

    const DOH_PROVIDERS = ['https://dns.google/resolve?', 'https://cloudflare-dns.com/dns-query?'];

    // Query ONE named provider. Needed for the second-opinion check before a
    // hard block — dohQuery() returns the first success, so on its own it can't
    // tell us whether two independent resolvers actually agree.
    // 'accept' is CORS-safelisted, so no preflight is fired.
    async function dohQueryProvider(idx, name, type) {
      const url = DOH_PROVIDERS[idx] + 'name=' + encodeURIComponent(name) + '&type=' + type;
      const controller = new AbortController();
      const t = setTimeout(() => controller.abort(), 4000);
      try {
        const res = await fetch(url, { signal: controller.signal, headers: { accept: 'application/dns-json' } });
        clearTimeout(t);
        if (!res.ok) throw new Error('DoH ' + res.status);
        return await res.json();
      } catch (e) {
        clearTimeout(t);
        throw e;
      }
    }

    async function dohQuery(name, type) {
      // Google first, Cloudflare fallback. Throws only if BOTH fail.
      let lastErr;
      for (let i = 0; i < DOH_PROVIDERS.length; i++) {
        try {
          return await dohQueryProvider(i, name, type);
        } catch (e) {
          lastErr = e;
        }
      }
      throw lastErr || new Error('DoH failed');
    }

    // Second opinion before hard-blocking. Both resolvers must independently
    // answer AND both must say NXDOMAIN. If either fails to respond or
    // disagrees we do NOT block — one resolver having a bad moment, or a domain
    // mid-nameserver-transfer, must never wall out a real lead.
    async function bothResolversSayNxdomain(domain) {
      try {
        const answers = await Promise.all(DOH_PROVIDERS.map((_, i) => dohQueryProvider(i, domain, 'A').catch(() => null)));
        return answers.length > 0 && answers.every((j) => j && j.Status === 3);
      } catch {
        return false;
      }
    }

    function dohARecords(json) {
      // type 1 = A record. CNAME chains arrive flattened in Answer,
      // so filtering on type 1 yields the final IPs.
      return ((json && json.Answer) || []).filter((r) => r.type === 1).map((r) => r.data);
    }

    function localWebsiteVerdict(domain, rawValue) {
      // Email-dependent checks — must run FRESH on every call and must
      // never be cached: the correct verdict changes when the lead goes
      // back and edits their email (e.g. google.com is wrong for a
      // gmail user but right for priya@google.com). No network cost.
      const email = getField('email') || formState.email || '';

      // 1. Free mailbox providers are never a company website
      if (PERSONAL_EMAIL_DOMAINS.includes(domain)) {
        return { ok: false, reason: 'mailbox_domain', msg: "Please enter your company's website — not an email provider." };
      }

      // 1b. Social/profile domains WITH a real path — pass immediately,
      // tagged informationally (not a failure). No DNS check needed
      // either: linkedin.com etc. are obviously real, live domains.
      if (SOCIAL_PROFILE_DOMAINS.includes(domain) && getUrlPathname(rawValue).length > 1) {
        const pathPattern = SOCIAL_PATH_PATTERNS[domain];
        // No pattern defined = platform uses bare handles, accept any path.
        // Pattern defined = the path must actually look like a profile URL.
        if (!pathPattern || pathPattern.test(getUrlPathname(rawValue))) {
          return { ok: true, reason: 'social_profile_url' };
        }
        // Structurally invalid for this platform — fall through to the brand
        // check below, which will flag it.
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
          // IPv6-only sites exist. Without this, a domain served purely over
          // AAAA looks nonexistent to us and would be hard-blocked.
          const aaaaJson = await dohQuery(domain, 'AAAA').catch(() => null);
          if (aaaaJson && (aaaaJson.Answer || []).some((r) => r.type === 28)) {
            return { ok: true, reason: 'resolved' };
          }

          // Only block on DECISIVE answers. SERVFAIL (Status 2, e.g. broken
          // DNSSEC on a real domain), REFUSED, etc. are indeterminate and must
          // fail open rather than block.
          const decisive = (j) => !!j && (j.Status === 0 || j.Status === 3);
          if (!decisive(json) && !decisive(wwwJson)) {
            return { ok: true, reason: 'dns_indeterminate' };
          }
          // Email-only companies are real — allow if the apex has MX records.
          const mxJson = await dohQuery(domain, 'MX').catch(() => null);
          const hasMX = !!(mxJson && (mxJson.Answer || []).some((r) => r.type === 15));
          if (hasMX) return { ok: true, reason: 'mx_only' };

          // DNS distinguishes "this domain does not exist" (NXDOMAIN, Status 3)
          // from "it exists but has no records yet" (NOERROR + empty answer).
          // The second usually means somebody owns it and is mid-setup, so it
          // gets flagged rather than blocked.
          if (json && json.Status === 0) {
            return { ok: false, reason: 'no_dns_records', msg: "This website doesn't appear to be set up yet. Please check the URL." };
          }

          // True NXDOMAIN — require BOTH resolvers to agree before blocking.
          const confirmed = await bothResolversSayNxdomain(domain);
          if (!confirmed) return { ok: false, reason: 'nxdomain_unconfirmed', msg: "This website doesn't appear to exist. Please check the URL." };
          return { ok: false, reason: 'nxdomain', msg: "This website doesn't appear to exist. Please check the URL." };
        }

        // ── Parking HINTS (v5.3.0) ────────────────────────────────────
        // Previously these returned a terminal `parked` / `parked_ns`
        // verdict, which also meant stage 2 never ran (it only fires on
        // 'resolved') — so the browser User-Agent added to the backend
        // specifically to rescue afgmmoving.com became unreachable for
        // afgmmoving.com the moment '103.224.182.' was added here. Two
        // fixes for one bug, the second silently undoing the first.
        // Now: collect a hint, let the content check decide.
        let parkingHint = null;
        const hintIp = ips.find((ip) => PARKING_IP_HINTS.some((p) => (p.split('.').length === 4 && p.slice(-1) !== '.' ? ip === p : ip.indexOf(p) === 0)));
        if (hintIp) parkingHint = 'ip:' + hintIp;

        // Nameserver fingerprint — sale pages sit on generic cloud IPs the
        // IP list can't see. Fail-open if the query errors.
        if (!parkingHint) {
          const nsJson = await dohQuery(domain, 'NS').catch(() => null);
          const nsHosts = nsJson ? (nsJson.Answer || []).filter((r) => r.type === 2).map((r) => String(r.data).toLowerCase().replace(/\.$/, '')) : [];
          const nsMatches = (list) => nsHosts.reduce((acc, h) => acc || list.find((s) => h === s || h.endsWith('.' + s)) || null, null);
          const strictHit = nsMatches(PARKING_NS_STRICT);
          if (strictHit) parkingHint = 'ns:' + strictHit;
          else {
            const softHit = nsMatches(PARKING_NS_SOFT);
            if (softHit) parkingHint = 'ns_soft:' + softHit;
          }
        }

        if (parkingHint) {
          // MX is a liveness prior the backend uses to weigh a thin page: a
          // genuine company domain virtually always receives email, a sale
          // lander never does. Queried ONLY on the hint path, so the happy
          // path costs no extra round trip.
          const hintMxJson = await dohQuery(domain, 'MX').catch(() => null);
          const hintHasMX = !!(hintMxJson && (hintMxJson.Answer || []).some((r) => r.type === 15));
          return { ok: true, reason: 'parked_suspect', parkingHint, hasMX: hintHasMX };
        }

        return { ok: true, reason: 'resolved' };
      } catch (err) {
        console.warn('[GW] Website check failed — allowing through:', err && err.message);
        return { ok: true, reason: 'doh_error' }; // fail-open, same policy as ELV
      }
    }

    // Verdicts safe to cache — decisive, email-independent results.
    // Transient/indeterminate outcomes must be re-checked next time.
    // 'nxdomain_unconfirmed' was REMOVED in v5.3.0: it means the two resolvers
    // disagreed, i.e. explicitly indeterminate, and caching it stopped a lead
    // from recovering by simply retrying in the same session.
    const CACHEABLE_REASONS = [
      'nxdomain', 'no_dns_records', 'resolved', 'mx_only',
      'for_sale_lander', 'marketplace_redirect', 'parked_confirmed',
      'forwarded_to_live_site', 'live_despite_dns_hint', 'content_clean',
      'thin_content', 'thin_content_wildcard', 'parked_suspect',
    ];

    // Stage-2 reasons that mean the backend ACTUALLY FETCHED the page and so
    // its answer is authoritative. Anything else means it couldn't look, in
    // which case a DNS parking hint must survive as a flag rather than
    // silently passing as clean.
    /* Server answers that mean it actually determined something. Anything
       absent from this list means the server could not look either, so the
       browser's original (passing) verdict stands. */
    const SERVER_DNS_DECISIVE = [
      'content_clean', 'live_despite_dns_hint', 'forwarded_to_live_site',
      'thin_content', 'thin_content_wildcard',
      'parked_confirmed', 'for_sale_lander', 'marketplace_redirect', 'hosting_placeholder',
      'nxdomain', 'no_dns_records', 'mx_only',
    ];

    const STAGE2_DECISIVE = ['content_clean', 'live_despite_dns_hint', 'forwarded_to_live_site', 'thin_content', 'thin_content_wildcard'];

    // Stage 2 — server-level content check (v4.9.5). DNS/IP/NS alone can't
    // see PAGE CONTENT, so marketplace landers on shared CDN IPs (Atom,
    // GoDaddy Auctions, Sedo) slip through stage 1 as "resolved" — the
    // real test.com case. Only called when stage 1 found a genuinely live
    // domain (reason === 'resolved'); skipped for mx_only/indeterminate/
    // test-email, since there's either nothing to scan or nothing to gain.
    // Same fail-open contract as everything else: any backend hiccup,
    // timeout, or bot-wall passes the lead through rather than blocking it.
    /* v5.7.0 — SERVER-SIDE DNS FALLBACK.
       Stage 1 runs in the VISITOR'S browser over DNS-over-HTTPS, so it
       inherits their network restrictions — a corporate firewall or the
       Great Firewall blocks it and a real business gets marked unverified.
       The server has no such restrictions, so when the browser lookup is
       blocked we ask it instead. Returns null on any failure, which means
       "keep whatever the browser concluded". */
    async function resolveWebsiteViaServer(rawValue) {
      if (!isRailwayReady()) return null;
      try {
        const controller = new AbortController();
        const t = setTimeout(() => controller.abort(), 15000); // matches the content check's budget
        const res = await fetch(`${RAILWAY_API_URL}/resolve-website`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          signal: controller.signal,
          body: JSON.stringify({ website: rawValue }),
        });
        clearTimeout(t);
        if (!res.ok) return null;
        const data = await res.json();
        if (!data || !data.reason) return null;
        return { ok: data.ok !== false, reason: data.reason, canonical_url: data.canonical_url || null };
      } catch (err) {
        console.warn('[GW] Server DNS fallback failed — keeping original verdict:', err && err.message);
        return null;
      }
    }

    async function verifyWebsiteContent(rawValue, parkingHint, hasMX) {
      if (!isRailwayReady()) return { ok: true, reason: 'skipped_no_backend' };
      try {
        const controller = new AbortController();
        // Bumped from 7s: the backend's own fallback ladder (as-typed ->
        // www/bare flip -> http downgrade) can legitimately take longer,
        // especially when the first candidate is a slow-but-real redirect
        // chain (e.g. www.site.com -> site.com). Must exceed the backend's
        // own worst-case budget or we'd abort before it finishes.
        const t = setTimeout(() => controller.abort(), 15000);
        const res = await fetch(`${RAILWAY_API_URL}/verify-website`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          signal: controller.signal,
          body: JSON.stringify({ website: rawValue, parking_hint: parkingHint || null, has_mx: hasMX === true }),
        });
        clearTimeout(t);
        if (!res.ok) return { ok: true, reason: 'backend_error' };
        const data = await res.json();
        return { ok: data.ok !== false, reason: data.reason || 'checked', canonical_url: data.canonical_url || null };
      } catch (err) {
        console.warn('[GW] Website content check failed — allowing through:', err && err.message);
        return { ok: true, reason: 'fetch_error' };
      }
    }

    const NOT_A_LIVE_SITE_MSG = "This doesn't appear to be a live company website. Please check the URL.";

    /* Base verdict: DNS + content only. Email-INDEPENDENT, therefore safe to
       cache. Email-dependent rules are layered on afterwards so they can never
       be frozen into the cache (a lead may go back and edit their email). */
    function baseWebsiteVerdict(domain, rawValue) {
      if (_websiteVerdicts.has(domain)) return Promise.resolve(_websiteVerdicts.get(domain));
      if (_websiteInFlight.has(domain)) return _websiteInFlight.get(domain);
      const p = verifyWebsiteDomain(domain)
        .then(async (v) => {
          // v5.3.0: stage 2 now also arbitrates 'parked_suspect'. Previously
          // it ran only on 'resolved', so a DNS hint was final and
          // unappealable — the one layer that can actually see the page
          // never got asked. Content is ground truth; DNS is a prior.
          /* v5.7.0 — the browser's own DNS was blocked or inconclusive, so
             we learned nothing about this domain. Ask the server, which is
             not behind the visitor's corporate firewall or the Great
             Firewall. Its answer is authoritative because it ran BOTH
             stages; if it can't answer either, we keep what we had. */
          const dnsWasBlocked = (v.reason === 'doh_error' || v.reason === 'dns_indeterminate');
          if (dnsWasBlocked && !isTestEmail(getField('email'))) {
            const sv = await resolveWebsiteViaServer(rawValue);
            if (sv && SERVER_DNS_DECISIVE.indexOf(sv.reason) !== -1) {
              console.log('[GW] Browser DNS blocked (' + v.reason + ') — server resolved it as ' + sv.reason);
              return sv;
            }
            return v; // server couldn't help either; the original already passes
          }

          const needsContentCheck = (v.reason === 'resolved' || v.reason === 'parked_suspect');
          if (!needsContentCheck || isTestEmail(getField('email'))) return v;

          const cv = await verifyWebsiteContent(rawValue, v.parkingHint, v.hasMX);
          if (!cv.ok) {
            return { ok: false, reason: cv.reason || 'for_sale_lander', msg: NOT_A_LIVE_SITE_MSG, canonical_url: cv.canonical_url || null };
          }
          if (STAGE2_DECISIVE.indexOf(cv.reason) !== -1) {
            // The backend fetched the page — its verdict supersedes stage 1.
            return { ok: true, reason: cv.reason, canonical_url: cv.canonical_url || null };
          }
          // Backend couldn't look (no backend, timeout, unreachable, bot wall).
          // Keep a parking hint alive as a FLAG rather than passing it as
          // clean — but still never block on it.
          if (v.parkingHint) return { ok: true, reason: 'parked_suspect', canonical_url: cv.canonical_url || null };
          if (cv.canonical_url) return { ...v, canonical_url: cv.canonical_url };
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

    /* Email-dependent layer, recomputed on every call, never cached. */
    function applyEmailDependentWebsiteRules(domain, verdict) {
      if (!verdict || verdict.reason !== 'nxdomain') return verdict;
      const emailDomain = emailDomainOf(getField('email') || formState.email || '');

      // SAFETY VALVE. If the typed website domain is the lead's own email
      // domain, the domain provably exists — they receive mail on it and the
      // address already cleared verification at step 1. An NXDOMAIN here is a
      // resolver blip, and blocking it would wall out a real customer.
      if (emailDomain && PERSONAL_EMAIL_DOMAINS.indexOf(emailDomain) === -1 && domainsMatch(domain, emailDomain)) {
        return { ok: true, reason: 'nxdomain_contradicted' };
      }

      // Offer a one-tap fix instead of a dead end. Six of the seven real
      // NXDOMAIN cases in the Jul/Aug sample were typos by real businesses
      // that went on to book, not junk leads.
      const emailLocal = (getField('email') || formState.email || '').split('@')[0] || '';
      const suggestion = suggestWebsiteDomainFix(domain, emailDomain, emailLocal);
      if (suggestion) {
        return { ...verdict, suggestion, msg: 'Did you mean ' + suggestion + '?' };
      }
      return verdict;
    }

    function checkWebsite(rawValue) {
      // Cached + deduped wrapper (blur prewarm and Next click share
      // one in-flight promise). Returns Promise<verdict>.
      const domain = extractWebsiteDomain(rawValue);
      if (!domain) return Promise.resolve({ ok: true, reason: 'unparseable' }); // format errors are validateStep2's job

      // Email-dependent checks first, fresh every time, never cached
      const local = localWebsiteVerdict(domain, rawValue);
      if (local) return Promise.resolve(local);

      return baseWebsiteVerdict(domain, rawValue).then((v) => applyEmailDependentWebsiteRules(domain, v));
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
        if (!val) return; // required-field nagging stays on Next
        if (!isValidURL(val)) {
          // Surface a CONFIDENT typo correction early, while they're still
          // in the field. Stay quiet for a generic malformed URL — that
          // would just nag someone mid-edit; Next still catches it.
          const suggestion = suggestUrlFix(val);
          if (suggestion) showError('website-error', 'Did you mean ' + suggestion + '?');
          return; // never DNS-check a malformed domain
        }
        if (isTestEmail(getField('email'))) return;
        const v = await checkWebsite(val);
        // Only show if the field still holds the value we checked
        if (!v.ok && el.value.trim() === val) {
          showWebsiteVerdictError(v);
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

    /* -------------------------------------------------------
    NETWORK — every backend call gets a deadline.
    Six of the seven Railway fetches had no AbortController, so a
    stalled request left the Next button stuck on "Verifying..."
    with no recovery path. Fail-open on timeout is deliberate and
    matches the ELV/website policy: a backend blip must never cost
    a real lead.
    ------------------------------------------------------- */
    const NET_TIMEOUT_MS = {
      verifyEmail: 10000,  // backend caps ELV at 8s; allow overhead
      enrich:      8000,
      partial:     8000,
      submit:      12000,  // the one call we most want to land
      session:     5000,
      booking:     8000,
    };

    async function fetchWithTimeout(url, options, timeoutMs) {
      const controller = new AbortController();
      const t = setTimeout(() => controller.abort(), timeoutMs);
      try {
        return await fetch(url, Object.assign({}, options, { signal: controller.signal }));
      } finally {
        clearTimeout(t);
      }
    }

    /* -------------------------------------------------------
    EMAIL VERIFICATION — cached, deduped, prewarmed on blur.

    Was: one fetch fired on the Next click, no timeout, and a
    single-slot cache that only ever remembered PASSES — so every
    rejected address was re-verified on each click. Now mirrors the
    website-check pattern: blur prewarms, Next reuses the in-flight
    promise, and definitive verdicts are cached per address.

    Verdict shape: { valid, status, message, suggestion }
    ------------------------------------------------------- */
    const _emailVerdicts = new Map(); // email -> verdict
    const _emailInFlight = new Map(); // email -> Promise<verdict>

    // Fail-open outcomes must NEVER be cached — a transient blip would
    // otherwise be remembered as a pass for the rest of the session.
    const EMAIL_UNCACHEABLE = ['skipped', 'http_error', 'error_fallback', 'backend_error', 'fetch_error', 'no_backend', 'timeout', 'empty'];

    function verifyEmail(rawEmail) {
      const email = (rawEmail || '').trim().toLowerCase();
      if (!email) return Promise.resolve({ valid: true, status: 'empty' });
      if (isTestEmail(email)) {
        console.log('[GW] Test email — skipping ELV');
        return Promise.resolve({ valid: true, status: 'test_email' });
      }
      if (!isRailwayReady()) return Promise.resolve({ valid: true, status: 'no_backend' });
      if (_emailVerdicts.has(email)) return Promise.resolve(_emailVerdicts.get(email));
      if (_emailInFlight.has(email)) return _emailInFlight.get(email);

      const p = (async () => {
        try {
          const res = await fetchWithTimeout(`${RAILWAY_API_URL}/verify-email`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ email }),
          }, NET_TIMEOUT_MS.verifyEmail);
          if (!res.ok) return { valid: true, status: 'backend_error' };
          const data = await res.json();
          return {
            valid:      data.valid !== false,
            status:     data.status || 'checked',
            message:    data.message || null,
            suggestion: data.suggestion || null,
          };
        } catch (err) {
          const timedOut = err && err.name === 'AbortError';
          console.warn('[GW] ELV ' + (timedOut ? 'timed out' : 'failed') + ' — allowing through:', err && err.message);
          return { valid: true, status: timedOut ? 'timeout' : 'fetch_error' };
        }
      })()
        .then((v) => {
          if (EMAIL_UNCACHEABLE.indexOf(v.status) === -1) _emailVerdicts.set(email, v);
          return v;
        })
        .finally(() => { _emailInFlight.delete(email); });

      _emailInFlight.set(email, p);
      return p;
    }

    /* -------------------------------------------------------
    Renders a rejection, plus a tap-to-accept fix when the backend
    identified a likely domain typo. The element is created here
    rather than in Webflow so no republish is needed.
    ------------------------------------------------------- */
    /* -------------------------------------------------------
    SOFT TYPO NUDGE (v5.7.1)

    Computed ENTIRELY LOCALLY — no network, no ELV. That matters:
    v5.6.0 attached this to the ELV response, so when ELV timed out
    (darshildixit21@gmailc.com, 8002ms, 20 Aug) the hint never
    appeared at all. A typosquat is detectable with arithmetic we
    already have, so it must not depend on a service being up.

    Uses its OWN element. #email-protip is an existing Webflow node
    with its own icon and copy that updateProTip() only shows and
    hides — writing into it would destroy the work-email nudge
    permanently.

    Advisory only. The address is valid and they can ignore this.
    ------------------------------------------------------- */
    function localEmailTypoHint(email) {
      const raw    = String(email || '').trim();
      const at     = raw.lastIndexOf('@');
      if (at < 1) return '';
      const local  = raw.slice(0, at);
      const domain = raw.slice(at + 1).toLowerCase();
      if (!domain || domain.indexOf('.') === -1) return '';
      // An exact free provider is correct as typed; a business domain must
      // never be "corrected" toward a mailbox provider.
      if (PERSONAL_EMAIL_DOMAINS.indexOf(domain) !== -1) return '';
      for (let i = 0; i < PERSONAL_EMAIL_DOMAINS.length; i++) {
        const c = PERSONAL_EMAIL_DOMAINS[i];
        // 8+ chars only: at short lengths one edit is not distinctive
        // (me.com vs we.com are unrelated domains).
        if (c.length < 8) continue;
        if (Math.abs(domain.length - c.length) > 1) continue;
        if (damerauLevenshtein(domain, c) === 1) {
          const candidate = local + '@' + c;
          return isValidEmail(candidate) ? candidate : '';
        }
      }
      return '';
    }

    function typoHintEl() {
      let el = document.getElementById('gw-email-typo-hint');
      if (el) return el;
      const input = document.getElementById('email');
      if (!input || !input.parentNode) return null;
      el = document.createElement('div');
      el.id = 'gw-email-typo-hint';
      input.parentNode.insertBefore(el, input.nextSibling);
      return el;
    }

    function hideEmailTypoHint() {
      const el = document.getElementById('gw-email-typo-hint');
      if (el) el.style.display = 'none';
    }

    function showEmailTypoHint(email) {
      const el    = typoHintEl();
      const input = document.getElementById('email');
      if (!el || !input) return;
      const suggestion = localEmailTypoHint(email);
      if (!suggestion || suggestion === String(email || '').trim()) { el.style.display = 'none'; return; }

      // DOM nodes, never innerHTML — this is user input.
      el.textContent = '';
      el.appendChild(document.createTextNode('Did you mean '));
      const link = document.createElement('span');
      link.className = 'gw-email-fix';
      link.setAttribute('role', 'button');
      link.setAttribute('tabindex', '0');
      link.textContent = suggestion;
      const apply = function () {
        input.value = suggestion;
        el.style.display = 'none';
        input.dispatchEvent(new Event('input', { bubbles: true }));
        input.focus();
        prewarmEmail();
      };
      link.addEventListener('click', apply);
      link.addEventListener('keydown', function (e) {
        if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); apply(); }
      });
      el.appendChild(link);
      el.appendChild(document.createTextNode('?'));
      el.style.display = 'flex';
    }

    function showEmailVerdictError(verdict) {
      const errEl = document.getElementById('email-error');
      const emailInput = document.getElementById('email');
      const msg = verdict.message || 'This email address appears to be invalid. Please use a real email.';

      // No suggestion — plain text path, identical to every other error.
      if (!verdict.suggestion || !errEl || !emailInput) {
        showError('email-error', msg);
        return;
      }

      // Suggestion path: the corrected address itself becomes the tap
      // target, inline in the sentence. Deliberately NOT a separate
      // button — the form has exactly one blue control ("Pick a time")
      // and a second one competing beside it costs more than it gains.
      showError('email-error', msg); // sets state: hides protip, red border, display block
      const marker = 'Did you mean ';
      if (msg.indexOf(marker) !== 0) return; // wording changed server-side; leave as plain text

      // Built with DOM nodes, never innerHTML — the local part is user
      // input and must never be parsed as markup.
      errEl.textContent = '';
      errEl.appendChild(document.createTextNode(marker));
      const link = document.createElement('span');
      link.id = 'email-suggestion';
      link.className = 'gw-email-fix';
      link.setAttribute('role', 'button');
      link.setAttribute('tabindex', '0');
      link.textContent = verdict.suggestion;
      const apply = function () {
        emailInput.value = verdict.suggestion;
        hideError('email-error');
        emailInput.dispatchEvent(new Event('input', { bubbles: true }));
        emailInput.focus();
        prewarmEmail(); // re-verify the corrected address immediately
      };
      link.addEventListener('click', apply);
      link.addEventListener('keydown', function (e) {
        if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); apply(); }
      });
      errEl.appendChild(link);
      errEl.appendChild(document.createTextNode('?'));
    }

    function hideEmailSuggestion() {
      const errEl = document.getElementById('email-error');
      if (errEl && errEl.querySelector('#email-suggestion')) hideError('email-error');
    }

    /* -------------------------------------------------------
    OPTION A — blur prewarm.
    Step 1 is email + sell-to. Firing verification the moment they
    leave the email field means it resolves while they pick their
    sell-to option, so the Next click is usually a cache hit.
    Enrichment is chained behind a PASS so Apollo is warm too and
    no credit is spent on an address that failed.
    ------------------------------------------------------- */
    function prewarmEmail() {
      const el = document.getElementById('email');
      if (!el) return;
      const val = el.value.trim();
      if (!val || !isValidEmail(val) || isTestEmail(val)) return;
      // Apply the SAME local junk check validateStep1 does, before spending
      // an ELV credit. Without this, test@gushwork.ai gets verified on blur
      // (showing ELV's message) and then re-judged on the Next click by
      // validateStep1 (showing a different message) — the copy visibly
      // changes under the lead for one address.
      if (isJunkText(val.split('@')[0], JUNK_WORDS_EMAIL_LOCAL)) {
        showError('email-error', "This doesn't look like a real email address. Please double-check.");
        return;
      }
      /* v5.7.1 — shown BEFORE the ELV round trip and independently of it.
         Attaching this to the ELV response meant a timeout swallowed it
         entirely: darshildixit21@gmailc.com timed out at 8002ms on 20 Aug
         and no hint ever appeared. The check is local arithmetic, so it
         should never have depended on a service being reachable. */
      showEmailTypoHint(val);

      verifyEmail(val).then((v) => {
        // Only surface it if they're still on this address — they may
        // have kept typing since the check started.
        const current = (document.getElementById('email')?.value || '').trim().toLowerCase();
        if (current !== val.toLowerCase()) return;
        if (!v.valid) { hideEmailTypoHint(); showEmailVerdictError(v); return; }
        hideEmailSuggestion();
        triggerEnrichment(val).catch(() => {});
      }).catch(() => {});
    }

    function initEmailPrewarm() {
      const el = document.getElementById('email');
      if (!el) return;
      el.addEventListener('blur', prewarmEmail);
      el.addEventListener('input', function () { hideEmailSuggestion(); hideEmailTypoHint(); });
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
        const res = await fetchWithTimeout(`${RAILWAY_API_URL}/partial`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(formState),
        }, NET_TIMEOUT_MS.partial);
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
        const res = await fetchWithTimeout(`${RAILWAY_API_URL}/submit`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(formState),
        }, NET_TIMEOUT_MS.submit);
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
        const res = await fetchWithTimeout(`${RAILWAY_API_URL}/enrich`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ email, session_id: formState.session_id }),
        }, NET_TIMEOUT_MS.enrich);
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

        /* Marker only, read by the overlay's resume card so a booked lead
           is not told there is a step left. Touches no booking logic. */
        document.documentElement.classList.add('gw-rh-booked');

        // GTM — Demo Booked
        if (!isTestEmail(formState.email)) {
          window.dataLayer = window.dataLayer || [];
          window.dataLayer.push({ event: 'gw_demo_booked' });
          console.log('[GW] ✅ GTM Demo Booked event pushed');
        }

        // Fire existing /booking-confirmed endpoint — same one Cal uses
        if (isRailwayReady() && !isTestEmail(formState.email)) {
          await fetchWithTimeout(`${RAILWAY_API_URL}/booking-confirmed`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              session_id: formState.session_id,
              booking_uid: meeting.id || ev.data.meeting?.id || '',
              start_time: meeting.meeting_time || '',
              end_time: '',
              event_type: 'demo',
            }),
          }, NET_TIMEOUT_MS.booking).catch(() => {});
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
        // Normally already resolved by the blur prewarm, so this is a
        // cache hit and the button barely flickers.
        const verdict = await verifyEmail(email);
        if (!verdict.valid) {
          showEmailVerdictError(verdict);
          return;
        }

        hideError('email-error');
        hideEmailSuggestion();
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

    /* Called by handleDisqualifiedNext only when no choice was passed — i.e.
       from a #step-disqualified-next button click. /demo's disqualified step
       is radios-only, so this was never defined there and never reached. If
       the Ads page markup DOES have that button, the undefined call throws a
       ReferenceError and the button dies silently with no feedback. Defined
       here so the file is correct either way; the radio path passes `choice`
       explicitly and never touches this. */
    function validateDisqualified() {
      const waitlist = document.getElementById('disq-waitlist');
      const b2b = document.getElementById('disq-b2b');
      if (b2b && b2b.checked) { hideError('disq-error'); return { valid: true, choice: 'b2b' }; }
      if (waitlist && waitlist.checked) { hideError('disq-error'); return { valid: true, choice: 'waitlist' }; }
      showError('disq-error', 'Please select one of the options above.');
      return { valid: false, choice: '' };
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
        let canonicalWebsite = null;
        if (!isTestEmail(getField('email'))) {
          const wv = await checkWebsite(getField('website'));
          formState.website_check_failed = !wv.ok;
          formState.website_check_reason = wv.reason || (wv.ok ? 'ok' : 'unknown');
          // Safety net: only trust the resolved canonical URL if it's
          // recognizably the SAME domain (www/bare, subdomain) as typed —
          // never silently store wherever an unrelated redirect landed.
          if (wv.canonical_url) {
            try {
              const canonicalHost = extractWebsiteDomain(wv.canonical_url);
              const typedHost = extractWebsiteDomain(getField('website'));
              if (domainsMatch(canonicalHost, typedHost)) canonicalWebsite = wv.canonical_url;
            } catch {}
          }
          if (!wv.ok) {
            showWebsiteVerdictError(wv);
            // Only WEBSITE_BLOCKING_REASONS stop submission; everything else
            // shows the red error but is allowed to continue.
            // formState.website_check_failed rides along to Railway, which
            // suppresses Meta CAPI and flags it for Slack/monitor.
            if (WEBSITE_BLOCKING_REASONS.indexOf(wv.reason) !== -1) return; // finally-block resets the button
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
        // One reliable URL downstream: if the site redirected (e.g. www ->
        // bare, http -> https), store the actual resolved address instead
        // of whatever the lead happened to type.
        if (canonicalWebsite) formState.website = canonicalWebsite;
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
        /* v5.7.2-ads — with the calendar up, back CLOSES it rather than
           navigating. Landing on step 2 would let them submit a second
           time, and the duplicate-booking guard reads only the newest
           lead row per email, so a second row is how one person ends up
           holding two calendar slots. The step-3 entry is pushed back
           because the pop already consumed it. */
        if (window.__gwRhOverlay && window.__gwRhOverlay.isOpen()) {
          window.__gwRhOverlay.dismiss();
          history.pushState({ step: 'step-3' }, '', '');
          return;
        }
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
      'disq-error': 'disq-waitlist',
      'sell-error': 'radio-wrap',
      'first-name-error': 'first-name',
      'last-name-error': 'last-name',
      'company-error': 'company',
      'website-error': 'website',
      'phone-error': 'phone',
      'hear-about-us-error': 'hear-about-us',
    };

    function showError(id, msg) {
      let el = document.getElementById(id);
      // Last resort: an error with nowhere to render is a dead button —
      // validation returns false and the lead sees no reason why. Build
      // the slot rather than fail silently. SECTION 0 normally has this
      // covered; this catches anything added to ERROR_INPUT_MAP later.
      if (!el) {
        const host = document.getElementById(ERROR_INPUT_MAP[id]);
        if (!host) return;
        el = document.createElement('div');
        el.id = id;
        el.style.display = 'none';
        (host.closest('.float-label-wrapper') || host).insertAdjacentElement('afterend', el);
      }
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
      initEmailPrewarm();
      initWebsiteCheck();
      initButtons();
      initEnterKey();
      initBrowserBack();
      initRHBookingListener();

      console.log('[GW] ✅ Form initialised v5.7.2-ads (Google Ads).', 'Session:', formState.session_id, '| Page:', formState.page_url, '| Landing:', formState.landing_page, '| Previous:', formState.previous_page || 'none', '| Referrer:', formState.referrer, formState.fbc ? '| fbc: ' + formState.fbc.substring(0, 20) + '...' : '', formState.fbp ? '| fbp: ' + formState.fbp : '');
    }

    if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init);
    else init();
  })();

/* ============================================================
  SECTION 13 — RH BOOKING STEP: FULLSCREEN MODAL  (ADS-PAGE ONLY)

  This is the one deliberate divergence from /demo.

  On /demo, step-3 renders in place and showStep() just widens
  #main-wrapper to 1100px. On the Ads pages the form sits inside a
  constrained hero column, so an in-place render is cramped. This
  block portals #step-3 out to document.body the moment it becomes
  visible, then the html.gw-rh-active CSS below paints it as a
  centered, viewport-wide modal over a dimmed backdrop.

  Fully reversible: on hide, #step-3 goes back to its original
  position via a comment placeholder. No form logic is touched —
  showStep() keeps using getElementById, which is position-agnostic.
============================================================ */
(function rhStepOverlay() {
  (function injectOverlayStyles() {
    const css = `
  html.gw-rh-active #step-3 {
  position: fixed !important;
  inset: 0 !important;
  z-index: 99999 !important;
  background: rgba(15, 23, 42, 0.45) !important;
  backdrop-filter: blur(2px);
  -webkit-backdrop-filter: blur(2px);
  display: block !important;
  margin: 0 !important;
  padding: 0 !important;
  }
  html.gw-rh-active #rh-embed {
  position: fixed !important;
  top: 50% !important;
  left: 50% !important;
  transform: translate(-50%, -50%) !important;
  width: min(1040px, calc(100vw - 48px)) !important;
  max-height: calc(100dvh - 48px) !important;
  margin: 0 !important;
  background: #ffffff !important;
  border-radius: 16px !important;
  box-shadow: 0 24px 60px rgba(0, 0, 0, 0.22) !important;
  overflow: auto !important;
  -webkit-overflow-scrolling: touch;
  min-height: 500px !important;
  }
  html.gw-rh-active #rh-embed iframe {
  width: 100% !important;
  min-height: 500px !important;
  display: block !important;
  border: 0 !important;
  }
  html.gw-rh-active #rh-embed * { scrollbar-width: none; }
  html.gw-rh-active #rh-embed *::-webkit-scrollbar { width: 0 !important; height: 0 !important; }

  /* v5.7.2-ads — DISMISSED STATE.
     Two classes deep on purpose. This must outrank the display:block above
     whatever the source order, and matching the same id with one more class
     is what guarantees that. */
  html.gw-rh-active.gw-rh-dismissed #step-3 { display: none !important; }

  /* The close control is a child of #step-3, never of #rh-embed: the RH SDK
     owns #rh-embed and re-renders it on open, which would take the button
     with it. Hidden unless the overlay is actually up. */
  .gw-rh-close { display: none; }
  html.gw-rh-active .gw-rh-close {
  display: flex !important;
  align-items: center;
  justify-content: center;
  position: fixed !important;
  top: 18px !important;
  right: 18px !important;
  z-index: 100000 !important;
  width: 36px !important;
  height: 36px !important;
  padding: 0 !important;
  margin: 0 !important;
  border: 0 !important;
  border-radius: 999px !important;
  background: #ffffff !important;
  color: #0f172a !important;
  font-size: 22px !important;
  line-height: 1 !important;
  cursor: pointer;
  box-shadow: 0 2px 10px rgba(0, 0, 0, 0.25);
  }
  #gw-rh-resume {
  text-align: center;
  padding: 28px 20px;
  }
  #gw-rh-resume-title {
  margin: 0 0 16px 0;
  font-size: 18px;
  font-weight: 600;
  line-height: 1.4;
  color: #0f172a;
  }
  #gw-rh-resume-btn { cursor: pointer; }
  `;
    const style = document.createElement('style');
    style.textContent = css;
    document.head.appendChild(style);
  })();

  function start() {
    var step3 = document.getElementById('step-3');
    if (!step3) return;

    var placeholder = document.createComment('gw-step3-anchor');
    var moved = false;

    /* =========================================================
    v5.7.2-ads — CLOSING THE CALENDAR

    Before this, the modal had NO exit. Not a cross, not Escape, not
    a backdrop tap — and not even a successful booking, which fires
    its events and leaves the overlay up. The only way out was the
    browser back button, which navigated to step 2 and invited a
    resubmit; see the popstate note in initBrowserBack.

    THE LEAD IS ALREADY SAVED before this overlay ever appears —
    submitLead() is awaited in handleStep2Next() before
    showStep('step-3'). So closing costs no lead data. It costs a
    BOOKING, which is the only thing this page is for, and that is
    what shapes everything below.

    Dismiss is a CLASS, never style.display. Setting display:none
    would satisfy the observer's hide branch, which un-portals
    #step-3 back into the hero column — and reparenting an iframe
    discards its browsing context, so the RH calendar would reload
    and lose whatever date they had already picked. Toggling a class
    leaves the inline style and the DOM position untouched, so the
    iframe never moves and reopening resumes exactly where they were.
    ========================================================= */
    var closeBtn = document.createElement('button');
    closeBtn.type = 'button';
    closeBtn.className = 'gw-rh-close';
    closeBtn.setAttribute('aria-label', 'Close the booking calendar');
    closeBtn.appendChild(document.createTextNode('\u00d7'));
    step3.appendChild(closeBtn);

    /* The resume card. Deliberately reads as UNFINISHED: no tick, no
       thanks, and no promise that anyone will be in touch — outreach is
       not standard here, and telling someone it is gives them a reason
       not to book. One button, no second option. */
    var resume = document.createElement('div');
    resume.id = 'gw-rh-resume';
    var resumeTitle = document.createElement('p');
    resumeTitle.id = 'gw-rh-resume-title';
    var resumeBtn = document.createElement('button');
    resumeBtn.type = 'button';
    resumeBtn.id = 'gw-rh-resume-btn';
    /* Borrow the real Next button's Webflow classes so this matches the
       form instead of approximating it. Falls back to the plain styling
       above if that button is not on the page. */
    var btnModel = document.getElementById('step-2-next');
    if (btnModel && btnModel.className) resumeBtn.className = btnModel.className;
    resume.appendChild(resumeTitle);
    resume.appendChild(resumeBtn);

    function setHeroWidth(wide) {
      var mw = document.getElementById('main-wrapper');
      var fw = document.getElementById('form-wrap-view');
      if (mw) mw.style.maxWidth = wide ? '1100px' : '1000px';
      if (fw) fw.style.maxWidth = wide ? '1040px' : '600px';
    }

    function isOpen() {
      var c = document.documentElement.classList;
      return c.contains('gw-rh-active') && !c.contains('gw-rh-dismissed');
    }

    function dismiss() {
      if (!isOpen()) return;
      document.documentElement.classList.add('gw-rh-dismissed');
      /* A lead who has already booked must not be nagged to book again.
         initRHBookingListener sets gw-rh-booked when RH reports
         MEETING_BOOKED. */
      var booked = document.documentElement.classList.contains('gw-rh-booked');
      resumeTitle.textContent = booked
        ? 'You are booked \u2014 see you then.'
        : 'One step left \u2014 choose your time.';
      resumeBtn.textContent = booked ? 'View your booking' : 'Pick a time';
      if (placeholder.parentNode && !resume.parentNode) {
        placeholder.parentNode.insertBefore(resume, placeholder);
      }
      setHeroWidth(false);
    }

    function reopen() {
      document.documentElement.classList.remove('gw-rh-dismissed');
      if (resume.parentNode) resume.parentNode.removeChild(resume);
      setHeroWidth(true);
    }

    closeBtn.addEventListener('click', dismiss);
    resumeBtn.addEventListener('click', reopen);

    /* Backdrop tap. #step-3 IS the dimmed backdrop and #rh-embed is the
       panel inside it, so "outside" is simply the event landing on
       #step-3 itself. Clicks inside the RH iframe never reach this
       document at all, so using the calendar cannot trigger it. */
    step3.addEventListener('click', function (e) {
      if (e.target === step3) dismiss();
    });

    document.addEventListener('keydown', function (e) {
      if (e.key === 'Escape' && isOpen()) dismiss();
    });

    /* initBrowserBack lives in the form's own IIFE and needs to ask
       whether the calendar is up before it treats a back press as step
       navigation. */
    window.__gwRhOverlay = { dismiss: dismiss, reopen: reopen, isOpen: isOpen };

    function sync() {
      var visible = step3.style.display === 'block';
      if (visible && !moved) {
        step3.parentNode.insertBefore(placeholder, step3);
        document.body.appendChild(step3);
        document.documentElement.classList.add('gw-rh-active');
        moved = true;
      } else if (!visible && moved) {
        if (resume.parentNode) resume.parentNode.removeChild(resume);
        if (placeholder.parentNode) {
          placeholder.parentNode.insertBefore(step3, placeholder);
          placeholder.remove();
        }
        /* Drop gw-rh-dismissed too, or a genuine navigation away from
           step 3 would leave it set and the next open would paint
           nothing. */
        document.documentElement.classList.remove('gw-rh-active', 'gw-rh-dismissed');
        moved = false;
      }
    }

    new MutationObserver(sync).observe(step3, { attributes: true, attributeFilter: ['style'] });
    sync();
  }

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', start);
  else start();
})();
