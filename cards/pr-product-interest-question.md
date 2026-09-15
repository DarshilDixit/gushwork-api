# PR 75 — "What do you need?" on /demo

**Branch:** `feat/product-interest-question` · **Not merged.**
**Date:** 15 September 2026

/demo only. /ai-demo and the ad landers are untouched.

---

## What was VERIFIED by running it

- **All twelve suites, bare, zero failures.** 3208 → 3273 assertions.
- **Four mutations.** Three caught; one SURVIVED and drove a new test
  section — see below. Re-run after: caught.
- **The Salesforce picklist value was added and then verified by a fresh
  describe against the live org**, before any code that can emit it. Not
  the 204 from the PATCH — a separate read.
- **Both leads upserts were EXECUTED**, not counted:
  `CREATE TEMP TABLE leads_probe (LIKE leads)`, add the column, EXPLAIN
  both INSERTs, ROLLBACK. Touches nothing real and is the only thing that
  can tell you a 48-placeholder statement still binds.
- **The canonicaliser and the resolver were executed** over the full
  input matrix before anything was wired to them.
- **The router is DRIVEN** in test-ads-parity across six cases including
  both-ticked and the missing-attribute fallback.
- **The divergence test drives /submit for real** and compares the bound
  column against the content_ids that reached graph.facebook.com.

## What was asserted STRUCTURALLY but never executed

- **Nothing has run in a browser.** The reveal, the checkbox listener, the
  about-business clearing and the router selection are all asserted
  against lifted source and stubbed DOM. No one has ticked a box.
- **The Webflow markup does not exist yet**, so the /demo path has never
  run end to end. Until it does, `needsAsked()` is false everywhere and
  every piece of this is inert.
- **No lead has been pushed to Salesforce with `aeo,crm`.** The picklist
  accepts it; nothing has sent it.
- **The mirror column** does not exist in AWS yet — the boot migration
  creates it on deploy. Confirmed absent, which is expected.

---

## The decisions, and why

**Two columns, not one.** `product` stays the single routing slug because
`Product__c` is a RESTRICTED picklist and three predicates compare
`product = 'crm'` with plain equality. Widening it breaks all four at
once. `product_interest` holds what they ticked.

**The markup values ARE the slugs.** Checkbox value, column, Salesforce
picklist and Meta content_ids are one vocabulary. The alternative was a
mapping table, which is another sync pair in a repo that has been bitten
by every one it has.

**Salesforce gets `product_interest ?? product`** — confirmed with
Darshil, because it makes SF deliberately richer than `leads.product`
while Meta must match it exactly. Two opposite rules, both defensible,
and the fallback keeps every historical lead sending what it sends now.

## The mutation that survived

**Removing the allow-list filter from `canonicalProductInterest`
SURVIVED the entire suite.** It is the only failure in this change that
loses a LEAD rather than a field: an unknown value in a restricted
picklist returns INVALID_OR_NULL_FOR_RESTRICTED_PICKLIST, which
`sfUnknownFields` does not retry, so Salesforce rejects the whole record
and the alert says to add it by hand.

Section 24 now executes the canonicaliser against 24 adversarial inputs
and enumerates every non-empty slug subset against the picklist — so
adding a third product without adding its combinations in Setup fails in
the suite rather than in production on the first both-ticked lead.

This is the test Darshil asked for by name, and it only exists because
the mutation was run.

## Two things I got wrong in passing

**`JSON.stringify(undefined)` is `undefined`, not a string** — my own
hostile-input test crashed the suite on its first run. Caught by
crash-reporter as UNMEASURED rather than passing quietly.

**I piped `measure.js` through `tail` twice** while iterating and both
times it hid real failures — three in batch2, then two more. The rule is
in CLAUDE.md and I broke it anyway. Everything reported here was read
bare.

## A fragility retired

The test stub decoded the leads upsert by counting five columns back
from the end, its own comment called that "a known fragility", it had
already broken once, and it broke again here. The stub AND the
assertions now walk the column list and the VALUES list together and
resolve by name — append, insert or reorder all keep working.

---

## Still open — Darshil's call

**Meta for a both-ticked lead.** Built as option A: `product` is `crm`,
the event fires `crm`, column and event agree by construction. Option C
is an addition to PRODUCTS plus one resolver line.

From the campaign audit: no ad set filters on product or content_ids and
none uses value optimisation, so this affects reporting, not delivery.

## Not done

- **The form half does not ship on merge.** Webflow re-pin to the new
  SHA, both tags, republish, sweep 14 pages, banner must read v5.13.0.
- **The /demo markup** has to be built by hand. Contract in CLAUDE.md.
- **`/start` is still not covered** — a CRM-intent visitor who submits on
  the ad lander is still recorded as aeo. Deliberately out of scope.
