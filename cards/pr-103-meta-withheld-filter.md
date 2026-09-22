# PR 103 — All Leads: "why was Meta withheld", and the window labels

Branch `feat/meta-withheld-filter`. Two commits. **Not merged.**

## What was actually verified

- **Full bar, run bare, read in full**: 12 suites, **4019 assertions**, zero failures.
  Baseline re-saved twice (+88, then +1).
- **Six mutations, all CAUGHT** — table in the PR body. One of them
  (`last_name` dropped from `LEADS_SEARCH_COLUMNS`) **survived the route suite on
  the first run**: the assertion read `includes('l.last_name')`, which stayed true
  because the column still appears inside the concatenated-names clause. The
  assertion was tightened to require the standalone `LIKE` and re-measured as
  caught. Recorded here because the first measurement was wrong, not because the
  final one was.
- **`metaWithheldReason` is EXECUTED**, not read: lifted with the real
  `isWebsiteVerified` and the real `WEBSITE_VERIFIED_REASONS`, driven over
  fixtures including a lead that trips four arms at once (priority), a
  `check_blocked` site (a bot wall is positive evidence, so not withheld), and
  `NON_ICP_LLM_META` off (a flagged lead is then NOT withheld).
- **Every filter value is driven over HTTP** and the SQL the route built is read
  back out. That is what catches the wiring — a control the loader never sends
  moves no source offset.
- **The marker is read back out of the painted HTML**, not asserted to exist.

## What was NOT verified — read this before trusting the feature

- **No filter has run against real rows.** The suites take no database, so the
  SQL↔JS equivalence is structural plus generated-SQL inspection. A predicate
  that compiles and returns the wrong set would pass everything here.
- **Nothing was run against production.** The numbers quoted in the PR (17/14,
  32/22) came from querying Railway directly during the investigation, before
  any code changed.
- **The UX changes have not been seen in a browser.** The active-filter count,
  the amber Clear, the empty state and the 📉 marker are asserted through a
  stubbed DOM only. Somebody should open the tab.

## The judgement call that needs a second opinion

`tests/test-non-icp.js` §10f forbade `non_icp_llm_flagged` in any WHERE. I did
not delete that rule — I added a **third category** to it, enumerated and pinned
like the existing counters, for a predicate that narrows what a human sees.

The reasoning: 10f's own comment draws the line at "decides who gets a
conversion, a Salesforce record or an SDR call" versus "decides what a number on
a dashboard says and reaches no lead at all". A display filter on `/monitor/leads`
is the second thing. The section also says forbidding the flag outright is what
kept 4.2% of leads invisible until 15 Sept — which argues for surfacing them.

The new safety property is the caller check: `metaWithheldSql` may be called only
from `/monitor/leads`, and mutating a call into `/submit` is caught. **If you
disagree with that reading, the caller check is the thing to look at first** — it
is what stops the exemption becoming a way onto the lead path.

§10b also wanted a decision on a new `disqualified` predicate; that one is a
straightforward DELIBERATE entry with a written reason.

## Blocking / Meta behaviour

**Unchanged.** Every predicate added is read-only and lives in `/monitor/leads`.
No lead's block status, Meta event, Salesforce push or SDR eligibility is touched.

---

# Addendum — the Model tab product filter (third commit)

`63e7a60`. Added after the first review card was written.

## What it does

A product dropdown on the Model tab, same vocabulary as All Leads (`all`,
`aeo`, `crm`, `__none`). It narrows **every panel** — ladder, industry groups,
decisions, scrape blind spot — because they all derive from the same `perLead`
array.

**Narrowed in SQL, not after the fetch.** The row cap is a `LIMIT`, so a
JavaScript filter would cap the population first and narrow it second: "2 CRM
leads" could then come out of a window whose newest rows were all AEO. In the
`WHERE`, the cap and the filter compose the right way round.

**The caption reads the server's echo, never the dropdown.** An unrecognised
value falls back to the whole population; a caption built from the control would
then describe a filter that was never applied.

## What was verified

- **Full bar: 12 suites, 4052 assertions, zero failures**, run bare. Baseline
  re-saved (+33).
- **The ladder-sum property is re-asserted under every filter value** against a
  deliberately lopsided mixed-product population (3 AEO, 2 CRM, 1 untagged).
  That is the property the whole tab rests on and a filter is exactly what can
  break it.
- **The stub was taught to honour the product predicate.** Before that it
  returned the whole population whatever the WHERE said, which could only ever
  prove the SQL *carried* a filter — never that the sum survived it.
- **Four mutations, all CAUGHT**: predicate removed from the WHERE (7
  assertions), the echo made to lie, `__none` folded into "no filter", and the
  loader never sending the param.
- Two of my own assertions were **wrong on first run** — `/l\.product/` matched
  the SELECT list, so they passed with no filter applied. Anchored to
  `AND l.product` and re-run. Recorded because the first version was wrong.

## Something I found and did NOT change

**The Model tab's "ours" count uses a different rule to All Leads.**
`nonIcpModelReport` calls `isInternalLead(lead.email)` — email only. All Leads
calls `isInternalSubmission(email, page_url)`, which also catches the staging
host. So a submission from `gushwork.webflow.io` is marked as ours on All Leads
and **not** marked as ours on the Model tab.

That predates this work — it is the 19 Sept staging arm not having reached this
function. I left it alone because fixing it changes a number already on screen,
and CLAUDE.md says to flag that rather than quietly fix it. **It is a one-line
change (`page_url` would need adding to the report's SELECT) and it is worth
doing** — say so and I will.

## Scope note

I added **product only**. An "exclude our own tests" filter is the obvious
companion — the ladder already prints "N of them are our own test submissions" —
but it would have to resolve the inconsistency above first, so it is an offer,
not something I did unasked.
