# PR 74 — Four dashboard corrections, and the lesson behind the first

**Branch:** `fix/dashboard-numbers-and-partner-key` · **Not merged.**
**Date:** 15 September 2026

Dashboard plus one Slack label. No lead-path behaviour, no flags, no totals
moved, neither form file touched, no Webflow step.

---

## What was VERIFIED by running it

- **All twelve suites, bare, zero failures.** 3172 → 3206 assertions.
- **Four mutations, all CAUGHT**, measured against a committed baseline.
  One survived first — below.
- **The new number assertions were verified against the real bug**: reverting
  item 1's fix is CAUGHT. That is the whole point of the discipline and it
  would have been worth nothing unproven.
- **Every claim in the report was read from production before building**: the
  partner key's resolution state, the ten blocked leads' reconciliation, the
  three farmersagent.com rows and which domain gave them their industry.
- **The Slack label helper is EXECUTED**, not asserted — lifted and driven
  over eight inputs including a resolved name, a resolved email, a referral,
  free text, a null key and a different partner's key.

## What was asserted STRUCTURALLY but never executed

- **Nobody has opened the rebuilt Model tab in a browser.** The grouped
  tables are rendered in a stubbed DOM and every number is compared to the
  payload, which is more than before, but it has not been looked at.
- **The Slack post has not been fired.** `tools/fire-non-icp-slack.js` would
  do it for real; it hardcodes `hear_about_us`, so firing it would exercise
  the message and not the new label. Proving the label end to end needs a
  real submit carrying an unresolved partner key.
- **`INTERNAL_TEST_EMAILS` from the environment** is still unset on Railway.

---

## 1. The cache summary read zero — mine, and the tests could not see it

`mdlScrapeHtml` read `d.scrape.cache`; `cache` is top-level. Introduced when
I moved the cache into its own block and updated the call site but not the
helper.

**Every existing check passed on it.** Painted, non-empty, not an error
string, parsed, nothing thrown. A confident, well-formatted zero. The only
thing that can see this is reading the number back out and comparing it to
the payload — which the suite now does for every number on the tab, with
fixture values distinctive enough that they cannot match by coincidence.

Passing the cache in explicitly also makes the helper a pure function of its
arguments, which is what makes that comparison possible at all.

## 2. Industry table: split by source, and no borrowing

The split makes `blocked_model: 0` visible where the table previously
implied the opposite. Three groups always render, empty included.

**The borrowing fix is the one that mattered.** Three `farmersagent.com`
blocks were filed under Insurance on the strength of the lead's *website*
being `farmers.com` — a different domain from the one that blocked them.
Benign in those three; on an email-side brokerage block with an unrelated
website it files the block under the wrong industry entirely.

The fixture proves it by construction rather than by coincidence: the
blocked lead is blocked on a domain with no verdict **while carrying a
website whose domain has one**, so a returning fallback files it under
Software / technology and the assertion says so by name.

## 3. Units on every count

10 leads / 6 people / 5 leads-excluding-ours. All three correct, all three
previously unlabelled and adjacent.

## 4. The unresolved partner key

Display only. The stored column keeps the placeholder
`upgradePartnerHearAboutUs` matches on — a test pins that, and it is the
half that separates a cosmetic fix from a permanent one.

`(b)`, the follow-up Slack message, was deliberately not built.

---

## The mutation that survived, and the suite that crashed

**Dropping empty groups SURVIVED.** The fixture had all three groups
populated, so the filter changed nothing. Now driven on its own fixture with
a single list-blocked lead so two groups are genuinely empty — which is also
the production shape.

**A suite CRASHED rather than failed**, and `crash-reporter.js` is the only
reason it was visible as a crash rather than as a clean run. The new
helper's first name, `partnerHearAboutUsDisplay`, is a prefix-extension of
the `partnerHearAboutUs` lift marker and sits earlier in the file, so
`test-partnerstack` lifted a span containing a second `PS_HEAR_PREFIX`
declaration. Renamed, and the marker tightened with its open paren.

**Generalisable:** a marker-based lift is a prefix match. A new function
whose name extends an existing marker silently re-points it, and the failure
surfaces as a syntax error in a file that is perfectly fine.

## A process note, third time

Backgrounded mutation loops running `git checkout -- .` between runs are
destructive to anything uncommitted. This session that cost one edit
earlier; this time the discipline held — no file was touched while a loop
was live, at the cost of a lot of waiting. The right fix is to run them in
the foreground or commit first, which is also the correct order anyway.

---

## Not done

- No form change, no Webflow re-pin.
- `becky.gerig` — the half-typed website reaching `partnerStackCustomerKey`
  — still untouched, still needs a decision about that function.
- The Slack label has never been seen in Slack. It will next appear on the
  first lead from a third partner; there are two.
