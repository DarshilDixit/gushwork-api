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
