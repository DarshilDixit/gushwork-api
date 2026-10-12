# /order-to-cash and /lead-to-order are CRM pages from day one

Branch `feat/order-to-cash-lead-to-order-paths` - 12 Oct 2026

Two new CRM product ad landers (Webflow drafts, duplicated from `/ai-crm`, form
carries `data-rh-router="6804"`). Without this change they repeat the
`/ai-crm` launch bug exactly: booking reaches the CRM team while the record says
`aeo`, and a B2C or Mixed answer dead-ends the prospect.

## The change

Four entries, which `test-batch2.js` section 21 requires to move together:

```
meta-capi.js            PRODUCT_PATHS       + '/order-to-cash': 'crm', '/lead-to-order': 'crm'
gushwork-form.js        B2C_ALLOWED_PATHS   + both   (v5.18.1)
gushwork-form-popup.js  B2C_ALLOWED_PATHS   + both   (v5.18.1-ads)
CLAUDE.md               product catalogue line + the pin-sweep path list
```

Version strings bumped in the header, the init banner and `test-partnerstack.js`
(which pins the banner).

## ⚠️ Blocking and Meta/Salesforce: this changes behaviour

On these two paths only, once the pages are live:

- a **B2C or Mixed** answer no longer disqualifies (same decision as `/ai-demo`
  on 15 Sept and `/ai-crm` on 18 Sept);
- leads store `product='crm'`, fire Meta `content_ids ['crm']` at
  `predicted_ltv` 5000 (not `['aeo']` at 12000) and take the CRM Salesforce
  picklist;
- CRM B2C leads can fire PartnerStack, and the two `sell_to ILIKE 'B2B%'`
  predicates keep them via `OR product = 'crm'`.

No other path moves. `/demo` is untouched and still decides by campaign.

## Verification

Full bar run bare: **5,533 assertions, 16 suites, all passing**. The grand total
moved 5,435 -> 5,533 (+48 batch2, +50 routes) and the baseline was re-saved after
reading the green bar.

Two mutations, both caught:

- dropping `/lead-to-order` from the popup's `B2C_ALLOWED_PATHS` only: caught by
  section 21 (agreement) and section 27 (behaviour);
- dropping `/lead-to-order` from `PRODUCT_PATHS`: caught by batch2 (14 failing
  assertions) and non-icp-routes (12).

New coverage: catalogue pinned by value (now four pages); both form files driven
for B2C on each new path across three journeys plus trailing slash, uppercase and
a `-pricing` suffix that must not match; `/submit` driven end to end from each new
page across five journeys (stored product, `content_ids`, `predicted_ltv`,
no server-side disqualification).

## Not verified / sequencing

- **Nothing is live yet.** The pages are Webflow drafts with noindex. Their
  footer `<script>` is pinned to an old commit (`82adc5f`) and must be repinned to
  the merge commit **after this deploys to Railway**, so the page never loads a
  form that the server cannot classify.
- Whether `crm` is the right product tag for these two *messages* (order-to-cash,
  lead-to-order) rather than a new catalogue entry is an assumption: the catalogue
  only has `aeo`, `crm`, `aeo,crm`, and the pages route to the CRM team.
- Not run against production data; no leads exist for these paths yet.
