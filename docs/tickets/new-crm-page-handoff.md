# Adding a new CRM page

Written 17 Sept 2026 for whoever builds the new CRM landing page. It is a
popup-style page like the ad landers, but it sells the CRM product, so the
booking must reach the CRM team rather than the AEO team.

Everything you need is already built. There is no new code to write — four
things to add, and two things to deliberately leave out.

---

## The four things to add

Replace `/your-new-path` with the real pathname, lowercase, no trailing slash.

**1. The catalogue — `meta-capi.js`**

```js
const PRODUCT_PATHS = {
  '/ai-demo': 'crm',
  '/your-new-path': 'crm',     // <- add
};
```

**2 and 3. Both form files — `gushwork-form.js` AND `gushwork-form-popup.js`**

```js
const B2C_ALLOWED_PATHS = ['/ai-demo', '/your-new-path'];   // <- same list in both
```

Both files, even though only the popup one runs on this page. They are a fork
of each other and a list that lives in two places drifts; a test compares them.

**4. Webflow — the form wrapper**

```
data-rh-router="6804"
```

That attribute is the whole routing change. `/ai-demo` already does exactly
this. It sends the booking to the CRM team unconditionally, whatever ad the
visitor came from.

Do **not** use `data-rh-router-crm`. That one is for `/demo`, where the visitor
picks the product with a checkbox and the router has to follow the pick. This
page has already decided.

**The script tag** is `gushwork-form-popup.js`, pinned to a full 40-character
commit SHA like every other page. See CLAUDE.md, "Deploying a form change".

---

## The two things to leave out

**No "What are you looking for?" checkboxes.** No `#needs-wrap`, `#need-aeo` or
`#need-crm`. That question exists on `/demo` because `/demo` does not know which
product the visitor wants. This page does. With the markup absent the whole
block switches itself off and `leads.product_interest` stays NULL, which is the
honest value — it means "we never asked".

**No `#about-business-wrap`.** Put the `about-business` field on the page with
no wrapper around it, exactly as `/ai-demo` does, so it is always visible.

The wrapper is a reveal mechanism: it hides the textarea until the visitor ticks
AI-CRM. Nobody ticks anything on this page, so adding the wrapper would hide the
field permanently on a page that needs it.

---

## What you do NOT have to do

Salesforce, Meta, the value numbers, the dashboards, the non-ICP block, UTM
capture. All of it keys off the catalogue entry in step 1 and is already
correct once that lands.

- `Product__c` already accepts `crm`
- `predicted_ltv` becomes 5000 automatically (`META_LTV_CRM`)
- Meta `content_ids` becomes `['crm']` automatically

---

## Why step 1 is the one that bites

Steps 2 and 3 are caught by the test suite: `tests/test-batch2.js` section 21
reads the CRM paths out of `PRODUCT_PATHS` and requires both form files to list
exactly the same set. Miss one and the bar goes red before you can merge.

**Nothing catches a missing step 1.** `PRODUCT_PATHS` has no entry for the new
page, so every path falls through to the default, which is `aeo`. The page would
work, the form would submit, the booking would go to the CRM team because of the
Webflow attribute — and every single lead would be recorded as an AEO lead:
wrong product in Salesforce, wrong Meta event, 12000 of predicted value instead
of 5000. No error anywhere.

**So do step 1 first, and do not pin the page in Webflow until it is live on
Railway.** Between those two moments, every lead from the page is mis-tagged.

---

## Start your branch after PR 89 merges

PR 89 (`feat/campaign-decides-offer`) changes which offer a visitor sees: the ad
campaign decides it now, not the pathname.

It adds a rule you want: **a page that names its own product beats the
campaign.** So an AEO ad pointed at your CRM page still resolves to `crm`,
because your `PRODUCT_PATHS` entry outranks the ad.

That rule does not exist on `main` yet. If you branch before PR 89 merges, an
AEO campaign landing on your page would resolve to `aeo` and you would have the
mis-tagging problem above for that traffic. Branch after.

---

## Checklist

- [ ] `PRODUCT_PATHS` entry in `meta-capi.js`
- [ ] `B2C_ALLOWED_PATHS` in `gushwork-form.js`
- [ ] `B2C_ALLOWED_PATHS` in `gushwork-form-popup.js`
- [ ] `node tests/measure.js --check`, run bare, all twelve suites green
- [ ] Merged and deployed to Railway
- [ ] Webflow: `data-rh-router="6804"` on the form wrapper
- [ ] Webflow: `gushwork-form-popup.js` script tag, full 40-char SHA, in the
      page's own before-`</body>` footer
- [ ] No needs checkboxes, no `#about-business-wrap`
- [ ] Add the page to the sweep list in CLAUDE.md — it becomes page 14
- [ ] After publishing, load the page and check the console banner, then submit
      one real test lead and confirm `leads.product` reads `crm`
