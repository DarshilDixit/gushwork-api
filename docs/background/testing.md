# Background: Testing lessons

Moved here from `CLAUDE.md` on 7 Oct 2026, word for word, to bring that file under its size limit. `CLAUDE.md` keeps the rule for each of these as one line with a link here; this file keeps the history, measurements and worked examples behind it. Dates and counts are as of when each paragraph was written, and "today" means that day. Line numbers in the headings are `CLAUDE.md` at commit b924ce7.

## Source assertions, the confident zero, driving the thing (was `CLAUDE.md` lines 925-971)

**A SOURCE ASSERTION CANNOT SEE RUNTIME BEHAVIOUR, and on 11-12 Sept that
cost THREE production breaks in one night.** This is the repo's oldest lesson
arriving in three new disguises, so the disguises are worth naming:

| Break | Why the suite could not see it |
|---|---|
| `/monitor/metrics` 500, blank Overview | A **temporal dead zone** — `const` read above its declaration. `node --check` passes; a TDZ violation is a runtime error, not a syntax one |
| Blocked tab: `leadRowsHtml is not defined` | A **scope** error. The function was declared inside `loadLeads`, so All Leads could see it and Blocked could not. Valid syntax, correct text, wrong scope |
| A Meta guard that never fired | An `if (false)` around it moves no source offset, so every ordering assertion still passed |

**A CONFIDENT ZERO PASSES EVERY STRUCTURAL CHECK.** On 15 Sept 2026 the
Model tab's standing-cache summary rendered **"0 companies classified all
time — 0 judged, 0 currently unreadable"** directly above a table listing
224 home services, 164 insurance and 141 real estate. `mdlScrapeHtml` read
`d.scrape.cache`; `cache` is a **top-level** field of the payload. One
reader wrong, one right, in the same feature.

Every check passed. The element was painted, the content was non-empty, it
was not an error string, it parsed, the loader threw nothing. The number was
simply wrong — and a wrong number on a dashboard is the failure mode the
dashboard exists to prevent.

**So: anywhere a number is displayed, assert it MATCHES THE PAYLOAD, not
that it exists.** Read the value back out of the painted HTML and compare it
to the object that was handed in, with fixture values distinctive enough
that they cannot match by accident. `tests/test-non-icp-routes.js` does this
for every number on the Model tab. "It rendered" is the same class of claim
as "we asserted it alerts" — it is one level short of the thing you actually
care about, and the gap is where this bug lived.

**The fix is not "write more assertions", it is "drive the thing".**
`tests/test-non-icp-routes.js` boots the real app and (a) drives every
`/monitor/*` route for a 200, and (b) **evaluates the dashboard's inline
script in a stubbed DOM**, asserts every shared helper is defined at TOP
LEVEL, calls every tab loader, and **records what each table painted**.

It also stubs **Leaflet**, and that was not optional: `visDrawMap` returns
at its first line when `L` is absent, so a mutation that stopped the map
naming its unmappable places SURVIVED until the stub existed. The degraded
path alone tested almost nothing.

That last part is the one that matters and it is the least obvious. Each
loader wraps its render in a `try/catch` that paints the error into the
table — so a scope error arrives as a tidy "Could not load:" message, not a
crash, and **a probe for a thrown error misses it entirely.** Assert on what
the user sees.

## The SQL that survived six suites, and /monitor/funnel (was `CLAUDE.md` lines 1529-1549)

**IT SURVIVED SIX GREEN SUITES, A REVIEW CARD AND A MERGE — because every
SQL assertion in this repo reads the query as TEXT.** That is the real
lesson and it generalises past this one bug: *a source-level assertion
cannot tell you whether a query parses.* The placeholder arithmetic in
section 11 was careful, correct, and checking a statement that could never
execute. The same blind spot is documented above for ordering assertions
and for the 21 dead PartnerStack call sites; this is its third appearance.

When you touch SQL, **execute it**. It costs nothing: build a `CREATE TEMP
TABLE` from the real migrations, run the real statement against it inside a
transaction, and `ROLLBACK`. That runs against the AWS mirror, needs no
production write, and would have caught this in seconds. For a read-only
query, `EXPLAIN` is enough — a syntax error is `42601` and an absent table
is `42P01`, so the two are distinguishable without any schema at all.

**`/monitor/funnel` had been broken since the Eastern Time migration
(`eb50c08`) for exactly the same reason** — three `//` lines sitting among
correct `--` ones. Nobody noticed because a dashboard tab erroring is
quieter than a lead path erroring. Found by the section 13 lint on the day
it was written, not by anyone opening the tab.

## A hand-run API read is not a better oracle (was `CLAUDE.md` lines 2259-2278)

**A HAND-RUN API READ IS NOT A BETTER ORACLE THAN THE SWEEP THAT WAITS.**
This is the most useful thing learned on 7 Sept 2026 and it generalises past
PartnerStack. A `test.com` conversion was created at 16:38:00. A direct
`GET /v2/customers/test.com` at 16:49 returned **404** — eleven minutes after
the record existed. That 404 was read, in the moment, as proof the conversion
had failed and would keep failing, and it was used to argue for intervening.
The sweep asked at 17:06, got a clean 200, and verified correctly.

Acting on the manual read would have released a good claim and re-fired a
conversion that had already landed — **the exact duplicate credit the grace
period exists to prevent, arrived at by a human being more impatient than the
code.** PartnerStack cannot undo a double credit.

So: when a checker is deliberately built to wait before believing a negative,
a quicker manual check of the same thing is **evidence of nothing**. It has
strictly less information than the check you already wrote. If you find
yourself about to override a grace period with a fresh `curl`, you are about to
reintroduce the bug the grace period documents. Wait for the sweep, or read
what the sweep last concluded — never race it.
