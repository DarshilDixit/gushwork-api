# PR 73 — Blocked rows would not expand; the Model tab mixed two claims

**Branch:** `fix/blocked-row-expand-and-model-scope` · **Not merged.**
**Date:** 15 September 2026

Dashboard only. No lead-path behaviour, no flags, no totals moved, neither
form file touched.

---

## What was VERIFIED by running it

- **All twelve suites, bare, zero failures.** 3125 → 3172 assertions.
- **Seven mutations, all CAUGHT**, measured with `measure.js --mutation`
  against a committed baseline. One survived first — see below.
- **The bug was reproduced against production data before any fix**, by
  fetching the real `/monitor/leads` and `?nonicp=only` payloads and
  comparing session ids. Exactly the two reported rows collide; the other
  eight do not.
- **The real `enrichPanel` was executed over all ten real blocked rows** —
  every one renders 2,075–6,693 chars. That is how the second theory was
  killed rather than assumed.
- **New and changed SQL EXPLAINed** read-only against the real schema. The
  internal-lead filter plans onto `leads_non_icp_blocked_idx`.
- **The live vs cache scrape rates were measured**, not estimated: 26.3%
  over the 38 verdicts written since the flags went on, 0.3% cache-wide.

## What was asserted STRUCTURALLY but never executed

- **Nobody has clicked the fixed row in a browser.** The ids are asserted to
  differ and each onclick is asserted to address the row it is inside, which
  is the defect precisely — but the actual click is untested.
- **The Blocked tab's second fetch** (the excluding-ours count) is exercised
  only against a stub. Its failure path renders "(could not separate our own
  tests)" rather than a number, which is asserted structurally.
- **`INTERNAL_TEST_EMAILS` from the environment** has never been set on
  Railway. The default list is what ran.

---

## 1. The Blocked tab — one root cause, and it was neither candidate

Not the row builder, and not enrichment.

`leadRowsHtml` emits `id="er-<session_id>"` and renders **both** tabs.
`showTab` toggles a class and never clears a panel, so both tables are in the
document together. A lead that is blocked AND on the loaded All Leads page
had that id twice; `getElementById` returns the first in document order;
`tp-leads` precedes `tp-blocked`. The click on Blocked expanded the hidden
copy in the inactive All Leads panel.

"The top two" because All Leads page 1 is the newest 25 leads — a blocked
lead breaks while it is new enough to be there and fixes itself once it falls
off. A moving window, so it has been intermittently broken since the Blocked
tab shipped and presents differently every day.

**The instruction not to stop at the first plausible explanation was the
right one.** Three theories were checked and discarded before this: a null or
malformed `session_id` (the column is `uuid`, all ten valid and unique), an
unescaped apostrophe in the onclick (same reason), and an empty panel for
un-enriched rows (the real `enrichPanel` renders all ten fine). Enrichment
correlated with nothing — four *working* rows have none.

**The fix separates two things that were one value:** `toggleRow(key, sid)`,
where the key addresses the DOM and the session id addresses the lead.
Conflating them caused the collision and would also have sent a namespaced
key to `/monitor/lead-changes` as a session id.

## 2. The Model tab — one claim per number

Industry table is now leads **acted on** in the window. The standing cache
moved to its own block, counted in **companies, all time, with no rate** — a
percentage over a population selected for success is what misled, and
printing it smaller would not fix it. The scrape rate is scoped to the
domains behind the window's leads, which needs no cutoff date to maintain.

**"Never tried" is deliberately outside the rate denominator.** A domain the
warm path has not reached is not a scrape that failed; folding them together
would make a quiet day read as a broken scraper.

The ladder was left alone. It reconciled and it was honest.

## 3. Test rows marked, nothing subtracted

`INTERNAL_TEST_EMAILS` + `ELV_EXCLUDED_DOMAINS` behind `isInternalLead`.
Marker on the row, excluding-ours figure printed beside the count, opt-in
filter both ways. No total changes, per the CLAUDE.md rule that excluding
internal addresses would move every historical number at once.

**Never keyed on a person's name.** A real prospect may be called Darshil,
and `allstate.com` is a real brokerage domain — which is why it is on the
block list in the first place.

**It is five, not four.** `agent@allstate.com` appears five times in the
blocked set; the fifth is 09-15 06:35 under the name "Swapnil Sinha", after
the V2 flags went on. So the quotable figure is **10 blocked, 5 external**,
and that fifth row is worth confirming with Swapnil before it is quoted.

---

## The mutation that survived, and why

**Folding never-tried into the scrape denominator changed no number**, because
every domain in the fixture had a verdict row — `answered` and `total` were
equal, so the mutation was a no-op against the test. The fixture, not the
code, was wrong.

Added a lead whose domain has **no verdict row of any kind**, which is a
different state from one with a failure row and is exactly the distinction
the rate depends on. Also added a lead of our own, so the internal counting
is executed rather than asserted structurally. Re-run: CAUGHT.

## Found by the tests, not by me

The cache-inventory rollup produced `NaN` when a row lacked `n` — which
serialises to `null` and would have reached the tab as a blank where a count
belongs, with nothing saying why. Surfaced because a stub regex stopped
matching a changed query. Coerced with a comment.

## A process note, repeated

A backgrounded mutation loop running `git checkout -- .` between runs is
destructive to anything uncommitted. It cost an edit earlier in this session.
This time the fix was to stop editing while one was running and wait for it,
which is the correct discipline and worth writing down: the danger is the
concurrency, not the command.

---

## Not done

- No form change, so **no Webflow re-pin** is needed for this PR.
- The junk-domain artefact from this morning (`becky.gerig`, a half-typed
  website reaching `partnerStackCustomerKey`) is untouched. It needs a
  decision about that function, which PartnerStack keys through.
- `agent@allstate.com` is in the list because it is the address that caused
  this. Other test addresses go in `INTERNAL_TEST_EMAILS` on Railway,
  comma-separated, no deploy.
