/* ============================================================================
   GW.L -- every plain-language label the new dashboard shows for a STORED
   value, in one place. SDRs read these, not engineers (CLAUDE.md, Style), so
   a raw slug never reaches the screen on its own: where one is useful to an
   engineer it rides in a tooltip beside the words.

   Two kinds of label, kept apart on purpose:
   - The server's own maps, WEBSITE_REASON_LABELS and META_WITHHELD_LABELS,
     arrive in the page config (monitor-next.js). They are NOT copied here:
     the classic already carries a second copy of the website map (var WLBL=)
     and a copy is how the two drifted before.
   - Labels that exist only in the browser. Where the classic has its own
     copy (the block-source wording), tests/test-monitor-next.js runs both
     over every input and requires the same words.
   ============================================================================ */
GW.L = (function (G) {
  var LB = (G.CFG && G.CFG.labels) || {};
  function website(r) { return r ? ((LB.website || {})[r] || r) : ''; }
  function metaLong(r) { return r ? ((LB.meta || {})[r] || r) : ''; }

  /* THE META CHIP. "ours", not "internal", in the words Duplicates and Lead
     magnet already use; "disqualified", not "B2C" -- the classic's "B2C"
     covered the waitlist too, 378 of the 683. */
  var META_SHORT = { internal: 'ours', blocked: 'blocked', model: 'model', website: 'no website', disqualified: 'disqualified' };
  /* What ELSE follows from the reason, stated per reason. The classic tooltip
     said "it still books, still reaches Salesforce" for every one of them,
     which is false for a blocked lead and for one of ours. */
  var META_AFTER = {
    internal: 'It is one of our own tests, so it does not reach Salesforce or the dialer either.',
    blocked: 'It is blocked as not our market; the Blocked tab lists what that stops.',
    model: 'Nothing else changes: it books, reaches Salesforce and is dialled as normal.',
    website: 'Nothing else changes: it books, reaches Salesforce and is dialled as normal.',
    disqualified: 'It answered B2C or mixed, or asked for the waitlist, at step 1.',
  };
  function metaShort(r) { return META_SHORT[r] || ''; }
  function metaWhy(r) { return r ? 'No Meta conversion was sent for this lead: ' + metaLong(r) + '. ' + (META_AFTER[r] || '') : ''; }

  /* WHICH CHECK BLOCKED THEM. FOUR stored values plus null, never "=== 'llm'"
     (CLAUDE.md, FOUR SOURCE VALUES). Same words as the classic's
     nonIcpSourceShort / nonIcpSourceWhy; a test holds the two together. */
  function sourceShort(src) {
    if (src === 'llm') return 'AI check';
    if (src === 'llm_name_only') return 'AI check (name only)';
    if (src === 'llm_late') return 'AI check (after booking)';
    return 'Brand list';
  }
  function sourceWhy(src) {
    if (src === 'llm') return 'The AI check read this company website and classified it as real estate or insurance. The Model tab shows the exact quote it relied on.';
    if (src === 'llm_name_only') return 'We could not load their website -- it refused us, failed, or needs JavaScript -- so the AI check judged the domain NAME alone. Weaker evidence than reading a page, so it needs higher confidence before it counts.';
    if (src === 'llm_late') return 'The AI check finished after this person had already submitted, so nothing could stop them booking. They were marked afterwards and a Slack post asked somebody to decide about the meeting. The booking was NOT cancelled.';
    if (src === 'domain_list') return 'This domain is on our list of national real-estate and insurance brands, so it was turned away without anything needing to read the site.';
    return 'This domain is on our list of national real-estate and insurance brands. (Blocked before we started recording which check fired -- the brand list was the only one that existed then.)';
  }

  /* THE STAGE LADDER (CLAUDE.md, Definitions): Booked > Disqualified >
     Completed > the rest. The rest reads "Left on step 2", never "Step 1":
     a lead row is written only after step 1 is done, so everyone in that
     bucket reached step 2 (the LEFT ON STEP 2 rule). Same words as the
     Duplicates tab. */
  function stage(l) {
    if (l.booking_uid) return { t: 'Booked', cls: 'b-good' };
    if (l.disqualified) return { t: 'Disqualified', cls: 'b-warn' };
    if (l.completed) return { t: 'Completed', cls: 'b-neu' };
    return { t: 'Left on step 2', cls: 'b-neu' };
  }
  function stageBadge(l) { var s = stage(l); return '<span class="badge ' + s.cls + '">' + s.t + '</span>'; }

  /* What happened when the Model layer tried to read a site. */
  var SCRAPE = {
    ok: 'read fine', unreachable: 'could not connect', thin: 'too little text to judge',
    blocked_by_site: 'the site refused us', private_host: 'points at a private network, not fetched',
  };
  function scrape(s) { return s ? (SCRAPE[s] || s) : 'no reason recorded'; }

  /* Why a PartnerStack conversion or qualification did not happen. A SKIP
     is usually correct; a FAILURE is money not paid (index.js, "Recording
     WHY"). http_NNN is whatever PartnerStack answered. */
  var PS_REASON = {
    phantom_200: 'PartnerStack said OK but never recorded it',
    test_email: 'our own test address, skipped',
    already_sent: 'already converted for this company',
    non_icp_blocked: 'blocked as not our market, skipped',
    disqualified: 'disqualified, skipped',
    no_customer_key: 'no usable company domain',
    no_credentials: 'PartnerStack keys missing on our side',
    no_token: 'PartnerStack token missing on our side',
    no_xid: 'no partner click id',
    no_partner_key: 'no partner key',
    bad_json: 'PartnerStack answered with something unreadable',
    no_identity_in_response: 'PartnerStack did not say who the partner is',
    no_type: 'no action type',
  };
  function psReason(r) {
    if (!r) return '';
    if (PS_REASON[r]) return PS_REASON[r];
    var m = /^http_(\d+)$/.exec(r); if (m) return 'PartnerStack answered HTTP ' + m[1];
    return r;
  }

  /* WHO CHANGED A FIELD (lead_field_changes.attribution). A row from before
     10 Sept 2026 recorded nobody, and says so rather than blaming the
     prospect. Same words as the classic's ATTR. */
  function changeWho(a) {
    if (!a) return { t: 'not recorded', badge: false };
    if (a === 'prospect_edit') return { t: 'they changed it', badge: true };
    if (a === 'maybe_our_canonical') return { t: 'them, or our own check resolved it', badge: true };
    if (a === 'ours_earlier_step') return { t: 'us — filled in before they saw the field', badge: false };
    if (a === 'ours_replacing_guess') return { t: 'us — they typed over our guess', badge: false };
    if (a === 'ours_sell_to_clarified') return { t: 'us — they said “actually B2B”', badge: false };
    return { t: a, badge: false };
  }
  /* WHERE in the form, only when it says something (the classic's WHERE_). */
  function changeWhere(c) {
    var b = [];
    if (c.arrived_step != null && c.field_step != null && c.arrived_step < c.field_step) b.push('step ' + c.field_step + ' field, written at step ' + c.arrived_step);
    else if (c.arrived_step != null) b.push('at step ' + c.arrived_step);
    if (c.back_navigation === true) b.push('came back from step ' + c.prev_step);
    if (c.hit_no != null && c.hit_no > 1) b.push('page load ' + c.hit_no);
    return b.join(' · ');
  }

  return { website: website, metaLong: metaLong, metaShort: metaShort, metaWhy: metaWhy, sourceShort: sourceShort, sourceWhy: sourceWhy,
           stage: stage, stageBadge: stageBadge, scrape: scrape, psReason: psReason, changeWho: changeWho, changeWhere: changeWhere };
})(GW);
