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

## Option C, added 15 Sept after the first build

**Both-ticked is ONE event carrying `content_ids: ['aeo','crm']`** — not
two. Meta's deduplication is cross-source only, and every active ad set
optimises on conversion count, so two events would count one person twice
and corrupt exactly what they bid on.

**The routing slug and the event slug are now different functions.**
`resolveProduct` stays single (`crm` for both-ticked — one calendar, one
picklist value). `resolveEventProduct` follows `product_interest`, the same
rule `Product__c` already had. They agree on every lead except the
both-ticked one, which is why the divergence test's shape changed: it now
asserts **Meta matches what they ticked**, not `leads.product`.

**`predicted_ltv` is config** — `META_LTV_AEO` / `META_LTV_CRM` /
`META_LTV_AEO_CRM`, defaults 12000 / 5000 / 15000, all PROVISIONAL. An
unreadable env var falls back and warns once; a `NaN` in a payload is worse
than a stale number. Combined is 15000 and not the 17000 sum, and a test
asserts it is neither the sum nor below the higher single product.

**`leads.meta_predicted_ltv` persists what was actually sent**, NULL where
no event fired. Both routes compute it from the same helper the event uses,
gated on their own Meta conditions, and the divergence test asserts the
stored number equals the number on the wire.

### Four more mutations, all CAUGHT

| | |
|---|---|
| combined collapses to one `content_id` | CAUGHT ×2 |
| event slug reverts to the routing slug | CAUGHT |
| combined LTV becomes the naive 17000 sum | CAUGHT ×2 |
| persisted number stops matching what was sent | CAUGHT |

**One run came back UNMEASURED and was not counted.** A background
mutation loop was still alive and competing for ports with the foreground
run, so `test-lead-field-changes` printed no summary. Killed the stray
process, confirmed the suite healthy, and re-ran all four in the
foreground. The rule did its job — an unmeasurable run is not a catch.

### Ticket, not built

`docs/tickets/non-icp-v1-block.md` now records that **value optimisation is
blocked on closed-won revenue joined back to leads, not on the config**.
`Customer_Status__c` unmaintained since June, no per-account revenue in the
warehouse — the same gap that blocked the retention analysis. Sequence:
revenue join → validate the three numbers → then consider the switch.

## Still open — Darshil's call

**The three LTV numbers themselves.** All PROVISIONAL, none measured
against realised revenue. They are now env vars so the agency's numbers
are a Railway change — but see the ticket: validating them needs the
revenue join first.

From the campaign audit: no ad set filters on product or content_ids and
none uses value optimisation, so today this affects reporting, not
delivery.

## Not done

- **The form half does not ship on merge.** Webflow re-pin to the new
  SHA, both tags, republish, sweep 14 pages, banner must read v5.13.0.
- **The /demo markup** has to be built by hand. Contract in CLAUDE.md.
- **`/start` is still not covered** — a CRM-intent visitor who submits on
  the ad lander is still recorded as aeo. Deliberately out of scope.

---

# Addendum, 15 Sept — the Webflow half, and a bug it exposed

## The /demo markup exists now

Built on the **live** `/demo` tree (branching is Enterprise-only on this
site — `list_branches` returns 403 `not_enterprise_plan_site`; the page
field `canBranch: true` is not an entitlement signal and says `true`
here regardless). **Nothing published.** Hidden markup only.

`#needs-wrap` (`.field-wrapper.is-hidden`) → `.field-label` "What do you
need?" → `.radio-holder` → two `.radio-wrap.is-needs` cards carrying
`#need-aeo` / `#need-crm`, `name="needs"`, values `aeo` / `crm` →
`#needs-error`. Inserted after the sell-to wrapper, before
`#step-1-next`. `#about-business-wrap` already existed. Verified by
reading the tree back, not by assuming the write landed.

## The bug the markup exposed — this is the part that mattered

**`syncAboutBusiness` could never have revealed the textarea, and it is
already in this PR.**

Both reveals were written `wrap.style.display = show ? '' : 'none'`.
Webflow **cannot store an inline style** — the Designer converts one
into a generated combo class, and the Data API rejects a `style`
attribute outright. Both confirmed by attempt, not by reading docs. So
every wrapper is hidden by a *class*, and `''` does not restore a
default: it removes an inline declaration that was never there. The
class keeps winning. `#about-business` would have stayed hidden for
every AI-CRM lead, silently.

Fixed to `'block'` in **both** functions in **both** form files.
`.field-wrapper` is `margin-bottom` and nothing else, so `block` is its
natural display and the fix changes no layout.

## The test, and why it is a lint

**Section 25** bans the shape: no empty-string `style.display`
assignment in either form file, plus per-function assertions that each
reveal names a real display keyword and still targets its own wrapper.

It is deliberately a lint rather than a driven test, and that is the
honest framing. The bug lives between our JavaScript and CSS **that is
not in this repo** — it is in Webflow. A stubbed DOM has no stylesheet:
`style.display = ''` then reading `style.display` returns `''`, and
every behavioural assertion passes. There is no fixture that makes this
visible, because the suite cannot see the rule that wins. Same ceiling
as the SQL lint in section 13.

**Section 21 had hardcoded the buggy literal** — `/wrap\.style\.display
= chosen \? '' : 'none';/` — so the suite was actively pinning the bug
in place. It now asserts the gate (`chosen`) and leaves the value to
section 25. Worth noting as its own lesson: an assertion that quotes the
code verbatim tests nothing except that nobody has changed it.

### Four mutations, all CAUGHT

| | |
|---|---|
| about-business reveal back to `''` | CAUGHT |
| needs reveal back to `''`, ads fork only | CAUGHT |
| double-quoted `""` instead of `''` | CAUGHT |
| reveal repointed at the wrong wrapper | CAUGHT |

One earlier run came back **UNMEASURED** and was correctly not counted —
the baseline was stale by the 18 assertions section 25 adds. Restored
the tree, confirmed green, re-baselined, then mutated. `--save` was
never run with a mutation applied.

**Bar: 12 suites, 3312 assertions, zero failures, read bare.**
3294 → 3312 baselined deliberately.

## What was VERIFIED by running it

- The four mutations above, each after a clean committed baseline.
- The Webflow tree read back after every write.
- The snapshot — **after forcing a canvas reload**. The Designer serves
  a stale tree after headless writes: the first snapshot returned
  "Element not found" for markup that demonstrably existed, and then a
  style change rendered at its old value with no error. `switch_page`
  away and back fixes it. Now in CLAUDE.md, because a stale snapshot is
  a photograph of the previous state under the word *verified*.
- The pin inventory: all 81 pages read through the API, plus the
  published-page `curl` sweep as an independent check.

## What was NOT verified

- **Still nothing in a browser.** No one has ticked a box. The reveal,
  the mandatory check and the router selection have never run against
  the real markup, because the script pinned in Webflow is v5.12.0 and
  knows nothing about any of this.
- **The `'block'` fix is asserted, not observed.** The lint proves the
  shape; only a browser proves the field appears.
- **Selected-state styling does not exist** on the two cards. Ticking
  shows the checkmark and does not colour the card. Darshil is doing
  that and the row layout in Webflow.

## Pin — surveyed, NOT written

Waiting on merge, deliberately. Two findings from the survey:

**The pin is per-page, not Project Settings.** No `gushwork-api` tag in
either site-wide block, zero registered scripts, twelve page-level
pins. CLAUDE.md was wrong and is corrected. The re-pin is **twelve page
edits**, not one.

**Production is v5.12.0 (`4e9d39e6`), not the v5.11.0 the ticket
claimed.** Newer, not missed — the doc went stale when PR #72 shipped.
Corrected rather than left to look like a missed deploy.

**`/start-old` is pinned to `d493e92e`, 26 June.** Outside the
fourteen-path sweep list, so that sweep could never have found it.
404s today. Needs a decision: delete or re-pin.

## Not done

- **Not published, not merged, not pinned.** Staging publish and the
  re-pin are both yours.
- The form half still does not ship on merge; the twelve page pins do
  that, afterwards.
