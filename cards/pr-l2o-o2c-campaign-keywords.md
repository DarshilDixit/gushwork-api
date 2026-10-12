# A campaign naming L2O or O2C is a CRM campaign

Branch `feat/l2o-o2c-campaign-keywords` - 12 Oct 2026

Swapnil asked that the two campaigns for the new landers (`/lead-to-order`,
`/order-to-cash`) carry the words **L2O** and **O2C**, so booking routers can be
set up per campaign. This makes those words decide the offer.

## The change

`campaignOffer` is one rule in three byte-identical copies (`meta-capi.js`,
`gushwork-form.js`, `gushwork-form-popup.js`). All three gain one line, placed
**before** the medium gate:

```
if (c.includes('l2o') || c.includes('o2c')) return OFFER_CRM;
```

- Matched **anywhere in the name, in any case** (`FLI__O2C__CBO`, `gw_l2o_us`).
- **Any medium.** The name alone decides, so a medium tagged differently, or a
  cookie that kept the campaign and lost the medium, cannot send a CRM lead to
  the AEO team. The existing `crm` word keeps its paid/cpc gate.
- Forms bumped to v5.18.2 / v5.18.2-ads (banner, header, and the pin in
  `test-partnerstack.js`).

## What it changes

On `/demo` a visitor whose URL, cookie or internal referrer carries such a
campaign now books with the **CRM team (router 6804)**, sees the about-business
field, is allowed to answer B2C or Mixed, and is stored as `product='crm'` (Meta
`content_ids ['crm']`, `predicted_ltv` 5000, CRM Salesforce picklist). Everything
else is unchanged: AEO and brand campaigns still get 6138.

## Not in this PR

- No selector change. `/demo` no longer has the selector markup; brand
  campaigns resolve to "not decided", which with no markup behaves as AEO.
- Whole-word matching. A campaign merely containing `o2c` inside a longer word
  (for example `Pro2Cloud`) would match. Judged unlikely; say if you want word
  boundaries instead, at the cost of missing `L2OSearch`.

## Verification

Full bar run bare: **16 suites, 5,582 assertions, all passing** (5,533 -> 5,582;
baseline re-saved after reading the green bar). Two mutations, both caught:
removing the line from the server (20 failing assertions) and from the `/demo`
form (21 across two suites).

New coverage: eight `campaignOffer` rows asserted equal across all three copies;
`resolveProduct` on `/demo` for four L2O/O2C names; the router driven for L2O/O2C
campaigns in both form files, including no medium, plus a brand campaign that
must still get 6138.

## Sequencing

Nothing reaches the pages until their pinned script moves. After merge and the
Railway deploy, `/demo` and the two new landers are repinned to the merge SHA.
