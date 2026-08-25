# Monitor system audit

Read-only audit of `/monitor` and every `/monitor*` route. No files changed, no
queries run against the database by me, no endpoints called. Everything below is
from reading `index.js`, `lead-magnet.js` and `db.js`.

One external fact has since been confirmed and is now stated as fact throughout:
the Postgres session timezone is **`Etc/UTC`** (`SHOW TimeZone`, run by the repo
owner in the Railway Postgres console, 2026-08-25). Everything that depended on
that assumption — every 05:30 IST day boundary — is settled.

Where something is wrong I say so. Where the code alone can't settle it,
[§13](#13-open-items-and-how-to-settle-them) gives the exact read-only check,
what a correct versus broken result looks like, and what it costs if it's broken.

---

## Contents

1. [The five things that are actually wrong](#1-the-five-things-that-are-actually-wrong)
2. [Route inventory](#2-route-inventory)
3. [Cross-cutting: populations](#3-cross-cutting-populations)
4. [Cross-cutting: timezones](#4-cross-cutting-timezones)
5. [Overview tab](#5-overview-tab)
6. [All Leads tab](#6-all-leads-tab)
7. [SDR List tab](#7-sdr-list-tab)
8. [Duplicates tab](#8-duplicates-tab)
9. [Lead Magnet tab](#9-lead-magnet-tab)
10. [System Health tab](#10-system-health-tab)
11. [Routes the UI never fetches](#11-routes-the-ui-never-fetches)
12. [What breaks as data grows](#12-what-breaks-as-data-grows)
13. [Open items and how to settle them](#13-open-items-and-how-to-settle-them)

---

## 1. The five things that are actually wrong

Ordered by how likely they are to mislead someone making a decision.

**1. Six of the nine System Health rows are lifetime counters wearing health-check
clothing.** `index.js:2300`. `Step 1 — /partial` is hardcoded green — `badge("s-partial",
d.total+" sessions saved","bg")` — with no condition of any kind. It says "Online"-green
whether `/partial` is working perfectly or has been returning 500s for a week. `Step 2 —
/submit` is green if `completed > 0` **ever**, so it went green on the first-ever
submission in July and can never go back. `AWS sync: Active` is `!!awsPool`
(`index.js:1290` → `index.js:111-122`), which is true whenever the `AWS_PG_HOST`
env var is set — it never touches the AWS database, so a mirror that is refusing
every write reads "Active" in green. Only `API uptime` (`index.js:2073`) and `ELV`
(`index.js:2074`) actually verify anything live. Detail in [§10](#10-system-health-tab).

**2. The Overview tooltip on "No booking yet (SDR)" claims a number equals the SDR
List. It doesn't.** `index.js:1922` says *"Distinct qualified B2B people who completed
the form … This is exactly the SDR List."* The Overview number
(`index.js:1232-1246`) filters `completed = true`. The SDR List query
(`index.js:1783-1791`) has **no `completed` filter at all**. So the SDR List is a
strict superset — it includes people who only ever reached step 1. The two numbers
will not match, and the tooltip says they will. The alert at `index.js:2075` repeats
the same false claim in prose: *"N people … completed the form but have no booking on
any session — see SDR List."*

**3. The Overview chart is labelled "Form sessions per day" and counts leads.**
`index.js:1932` is the label; the query at `index.js:1199-1207` is `FROM leads`.
`form_sessions` is a real table with a genuinely different meaning — `CLAUDE.md`
is explicit that a `leads` row means someone reached step 1 while a `form_sessions`
row means a page load. The chart is showing step-1 entries and calling them page
loads. The alert text at `index.js:2075` makes the same mistake: *"No new sessions
in the last 24 hours"* is driven by `todayCount`, which is `COUNT(*) FROM leads`
(`index.js:1253`).

**4. Three different definitions of "already booked on another session" coexist on
the Overview tab.** All three are visible in the same 8-card grid:

| Card | Rule | Line |
|---|---|---|
| No booking yet (SDR) | `NOT EXISTS (booking_uid IS NOT NULL)` — any session, any time | `index.js:1240-1244` |
| Pending recovery | `NOT EXISTS (booking_uid IS NOT NULL AND booked_at >= l.created_at)` | `index.js:1224-1229` |
| Recovered bookings | `EXISTS (booking_uid IS NOT NULL AND COALESCE(booked_at, created_at) >= l.created_at)` | `index.js:1189-1194` |

The middle one is the defect: a booked row whose `booked_at` is NULL makes
`booked_at >= l.created_at` evaluate to NULL, the `NOT EXISTS` passes, and the lead
is counted as *pending recovery* despite having booked. The third query handles
exactly this case with `COALESCE(booked_at, created_at)`. `booked_at` was added by
migration (`db.js:308` region) and every current write path sets it, so this only
bites rows that predate it — but those rows are the oldest and quietest, and the
number silently over-reports rather than erroring.

**5. The All Leads stage filter and the stage badge use different rules, so a filtered
view can contradict its own rows.** Filters at `index.js:1592-1595`; badge at
`index.js:2078`. The badge is a priority chain (booked → disqualified → completed →
step 1). The filters are independent predicates. A lead with `completed = true`,
`booking_uid IS NULL`, `disqualified = true` matches the `completed` filter
("Completed (no booking)") but renders a red **Disqualified** badge. The same lead
also matches the `disqualified` filter. The four stage options are not mutually
exclusive and do not sum to the total.

---

## 2. Route inventory

| Route | Line | Token-gated | Fetched by the UI |
|---|---|---|---|
| `GET /monitor` | `index.js:1830` | yes (`1831-1834`) | — (is the UI) |
| `GET /monitor/metrics` | `index.js:1157` | yes | yes, `2290`, every 60s |
| `GET /monitor/leads` | `index.js:1557` | yes | yes, `2144` |
| `GET /monitor/filter-options` | `index.js:1724` | yes | yes, `2138` |
| `GET /monitor/sdr` | `index.js:1742` | yes | yes, `2310` |
| `GET /monitor/duplicates` | `index.js:1519` | yes | yes, `2360` |
| `GET /monitor/funnel` | `index.js:1383` | yes | **no** |
| `GET /monitor/elv-health` | `index.js:3089` | **no** | yes, `2074` |
| `GET /monitor/website-recheck` | `index.js:3831` | yes, and refuses if unset | **no** |
| `GET /monitor/lm-metrics` | `lead-magnet.js:458` | yes | yes, `2170` |
| `GET /monitor/lm-leads` | `lead-magnet.js:544` | yes | yes, `2203` |
| `POST /monitor/lm-delivered/:id` | `lead-magnet.js:584` | yes | yes, `2280` |
| `GET /monitor/lm-loops-health` | `lead-magnet.js:604` | yes | **no** |
| `POST /monitor/lm-loops-retry/:id` | `lead-magnet.js:623` | yes | yes, `2274` |

**`/monitor/elv-health` is the only ungated route** (`index.js:3089-3092` — no token
check, unlike every sibling). It returns no lead data, but it does leak
`cacheSize`, `lastStatus`, `lastCheckAt` and the degradation state. Every other
`/monitor*` route checks the token; this one was missed.

**`/monitor/website-recheck` is the only route that writes.** It correctly refuses
to run at all when `MONITOR_TOKEN` is unset (`index.js:3837-3840`) rather than
falling open, which is the right call. But it is a **`GET` with a `?apply=1`
side effect** (`index.js:3842`, writes at `index.js:3926-3936`) plus two
`ALTER TABLE`s (`index.js:3860-3862`). A GET that mutates lead rows can be
triggered by anything that follows a link — a browser prefetch, a chat client
unfurling a pasted URL, a crawler with the token in the path. The dry-run default
limits the blast radius but does not remove it.

---

## 3. Cross-cutting: populations

This is where most of the on-screen inconsistency lives.

**Nothing on the `leads` side excludes internal or test addresses.** `ELV_EXCLUDED_DOMAINS`
(`index.js:2488` — `gushwork.ai`, `test.com`, `example.com`, `example.org`) is used
only to keep internal testing out of ELV health state (`index.js:2545`) and out of
alerting (`index.js:2723, 2741, 3045`). No `/monitor*` route on the `leads` table
references it. The `b@g.ai` test address is skipped client-side and in the two
booking webhooks (`index.js:4369, 4651`) but its row still exists and still counts.
So **every number on Overview, All Leads, SDR List and Duplicates includes your own
team's test leads.** The Lead Magnet tab is the only one that has an internal
concept at all (`is_internal`), and it applies it inconsistently — see
[§9](#9-lead-magnet-tab).

**Webhook-origin leads are included everywhere except `/monitor/funnel`.** Rows with
`prefill_source IN ('rh_webhook','cal_webhook')` are created by the booking
safety-net branches (`index.js:4429, 4721`) for people who booked without touching
the form. `/monitor/funnel` excludes them deliberately and explains why at length
(`index.js:1327-1338`, `WEBHOOK_LEAD_SQL` at `index.js:1381`). Every route the UI
actually fetches includes them. On Overview that inflates `booked`, `completed` and
the booking rate — and since these rows arrive with `submitted_at` and `booking_uid`
already set, they land in the numerator of `people booked / people completed`
without ever having been a form completion. **This looks accidental, not
deliberate**: the funnel route's author clearly identified the problem, wrote it up,
and fixed it in one route without propagating the fix to `/monitor/metrics`.

**Session vs people dedup is mixed on purpose and mostly labelled well.**
`index.js:1174-1182` gives the `COUNT(DISTINCT LOWER(email))` figures, `index.js:1164-1173`
the raw row counts, and the UI puts people in the headline and sessions in the
small print with a reconciliation line at `index.js:2298`. This part is good and
the labelling is honest.

One gap: `total` (`index.js:1166`) counts all `leads` rows including any with a NULL
email, while `people_total` (`index.js:1176`) is scoped `WHERE email IS NOT NULL`
(`index.js:1180`). If NULL-email rows exist, "N sessions" counts them and
"Total people" doesn't, with no note. Whether any exist is a data question — see
[§13](#13-numbers-i-cannot-verify-from-code-alone).

**Dedup key is `LOWER(email)` on the `leads` side** (`index.js:1176, 1187, 1193, 1234,
1242, 1548, 1784, 1789, 1640-1641, 1618`) and **case-sensitive raw `email` on the
lead-magnet side** (`lead-magnet.js:561-562`, the `attempts` subquery). The LM
attempts count will treat `A@x.com` and `a@x.com` as two different people; nothing
else in the system does.

---

## 4. Cross-cutting: timezones

**Settled fact, not inference:** the Postgres session timezone is `Etc/UTC`
(`SHOW TimeZone`, Railway Postgres console, 2026-08-25). Nothing in the repo
overrides it. Every statement below is therefore definite — the dashboard really
does operate in two timezones at once, and the 05:30 IST boundaries are real, not
a risk to check.

- **Everything displayed is forced to IST and is viewer-independent.** `ist()`
  (`index.js:2071`) and `lmIST()` (`index.js:2155`) both pass
  `timeZone:"Asia/Kolkata"` explicitly. The `(IST)` column headers — All Leads
  "Created (IST)" (`index.js:1957`), SDR "Date (IST)" (`index.js:1972`),
  Duplicates "First/Last Seen (IST)" (`index.js:1982`), LM "When (IST)"
  (`index.js:2067`) — are all accurate.
- **The "Updated … IST" pill** (`index.js:2303`) is also explicit IST and correct.
  It reflects when `loadAll` last completed, not when the data was queried — a
  sub-second difference, not worth flagging.
- **The 14-day chart is the only IST-bucketed query** (`index.js:1200-1201`, explicit
  `AT TIME ZONE 'Asia/Kolkata'`). Its day boundary is 00:00 IST, matching the label.
- **Every other day boundary falls at 05:30 IST.** These use bare `date_trunc` or
  `::date`, which resolve in the Postgres session timezone. Nothing in the repo
  sets it — no `TZ`, no `PGTZ`, no `PGOPTIONS`, no `SET TimeZone`, and the pool is
  built with `connectionString` and `ssl` only (`db.js:2-5`) — so it is the server
  default, and **`SHOW TimeZone` returns `Etc/UTC`, confirmed 2026-08-25 in the
  Railway Postgres console.** Midnight UTC is 05:30 IST. Affects
  `/monitor/funnel` (`index.js:1396, 1413`), the All Leads date filter
  (`index.js:1621-1622`), the LM daily chart (`lead-magnet.js:514-517`).
- **The date preset buttons use the viewer's laptop timezone**, not IST
  (`index.js:2135`, `getFullYear/getMonth/getDate` are local-time getters).
- **Consequence, now a confirmed fact rather than a risk**: clicking "Today" on All
  Leads hides every lead created between 00:00 and 05:30 IST, while the table's
  "Created (IST)" column labels those same leads with today's date. A 5½-hour blind
  spot, every day. The Overview chart (00:00 IST buckets) and the All Leads date
  filter (05:30 IST buckets) can never be reconciled for the same named day.
- **CSV filenames are UTC** (`index.js:1694, 1819`, `2289`), so an export at 02:00
  IST is named with yesterday's date.
- The rolling windows — `NOW() - INTERVAL '14 days'` (`1204`), `'24 hours'`
  (`1253`), `'2 hours'` (`1224`), `($1 || ' days')::interval` (`1403, 1440`) — have
  no day boundary and are timezone-independent. These are fine.

---

## 5. Overview tab

Data source: `/monitor/metrics` (`index.js:1157`), one `Promise.all` of ten queries.
Refreshed every 60 seconds (`index.js:2382`).

### Row 1 — the four headline cards

**Total people** — `index.js:1176`, `COUNT(DISTINCT LOWER(email)) FROM leads WHERE
email IS NOT NULL`. Sub-line `index.js:2292`: `total + " sessions · " + todayCount +
" in last 24h"`.
*Assumption*: distinct humans who have started the form. *Correct*, with the caveats
that it includes internal/test addresses and webhook-origin bookers who never
touched the form. Cannot double-count. `todayCount` (`index.js:1253`) is a rolling
24h count of `leads` rows, not sessions and not calendar-today — the label says
"in last 24h", which is accurate.

**People completed** — `index.js:1177`, `COUNT(DISTINCT LOWER(email)) FILTER (WHERE
completed = true)`. Percentage at `index.js:2293` is `peopleCompleted / peopleTotal`.
*Assumption*: people who finished the form. *Correct*, except webhook rows arrive
with `completed = true` (`index.js:4429, 4721`) and so count as completions without
a form completion existing.

**People booked** — `index.js:1178`, `FILTER (WHERE booking_uid IS NOT NULL)`.
Percentage at `index.js:2294` is `peopleBooked / peopleCompleted` — deliberately
*of completed*, and the sub-label says so. *Correct as labelled.* Same webhook
inflation, and here it is doubly distorting: webhook rows add 1 to both numerator
and denominator, pushing the ratio toward 100%.

**Disqualified** — `index.js:1179`, `FILTER (WHERE disqualified = true)`, sub-label
"B2C / Mixed". *Assumption*: people whose answer disqualified them. Mostly correct,
but a lead who is disqualified and then clarifies to B2B has `disqualified` set back
to `false` (`gushwork-form.js:2062-2064`), so this counts only people who *stayed*
disqualified. That is arguably the right number, but "Disqualified" doesn't say it,
and the `__clarified` filter on All Leads exists precisely because that population
matters.

### Row 2 — the four action cards

**No booking yet (SDR)** — `index.js:1232-1246`. `SELECT COUNT(*) FROM (SELECT
DISTINCT ON (LOWER(email)) email FROM leads WHERE completed = true AND booking_uid
IS NULL AND disqualified = false AND sell_to ILIKE 'B2B%' AND NOT EXISTS (any row
with same lower(email) and a booking_uid))`.
Population: completed only, B2B only (`ILIKE 'B2B%'` correctly catches
`B2B (clarified from B2C)`), not disqualified, deduped by lowercased email,
excluding anyone who has ever booked on any session.
*Assumption from the tooltip*: this equals the SDR List. **Wrong** — see
[§1 item 2](#1-the-five-things-that-are-actually-wrong). Sub-line at `index.js:2299`
shows `completedNoBookingSessions` (`index.js:1171`), the un-deduped, un-filtered
session count, which is a much larger number; the reconciliation line at
`index.js:2298` explains the gap in words, which is good practice.

**Recovered bookings** — `index.js:1183-1197`. Distinct emails that have a completed
session with no booking *and* some session with a booking whose
`COALESCE(booked_at, created_at)` is at or after that session's `created_at`.
*Assumption*: follow-up worked. Reasonable. Two things to know: it counts a person
once no matter how many recovery events (`GROUP BY LOWER(l.email)` then
`COUNT(*)`), so no double-counting; and `>=` rather than `>` means a booking
recorded in the same instant as the completed row counts as a recovery, which will
catch same-session bookings where `booked_at` was never written and `created_at`
is reused. Minor over-count on old rows.

**Pending recovery** — `index.js:1217-1230`. This is the one with the NULL bug
([§1 item 4](#1-the-five-things-that-are-actually-wrong)). Otherwise: `leads` rows
with an email, not disqualified, no booking, `loops_sent = false`, older than 2
hours, and no later booking by the same email. Tooltip at `index.js:1923` says
"Sessions … that the recovery cron has not processed yet" — accurate, and correctly
says *sessions* rather than people.

**Recovery emails sent** — `index.js:1170`, `COUNT(*) FILTER (WHERE loops_sent =
true)` over all `leads`, all time. Label "Recovery emails sent / follow-ups
dispatched". *Assumption*: how many emails went out. Correct-ish — it counts rows
flagged as sent, which is one row per session, so a person emailed across two
sessions counts twice. It is a lifetime total with no window, so it only ever goes
up and is useless as a health signal, which is exactly how `s-loops` uses it
([§10](#10-system-health-tab)).

### Alerts panel

`renderAlerts`, `index.js:2075`. Five conditions, all computed in the browser from
the metrics payload:

| Condition | Correct? |
|---|---|
| `pendingPartials > 0` → "N session(s) waiting >2 hours" | Correct wording, inherits the NULL `booked_at` over-count |
| `noBookingUid > 0` → "… see SDR List" | **Wrong** — asserts equality with the SDR List |
| `!awsSynced` → "AWS sync disabled." | Correct — this is genuinely what `!!awsPool` means |
| `total > 5 && enriched < total*0.3` → "Low enrichment rate (X% of sessions)" | Ratio compares `COUNT(*) FROM enrichment_data` to `COUNT(*) FROM leads`, all-time. Can exceed 100% if an enrichment row exists for a session whose `leads` row was never written. Not wrong, but it is a lifetime average that will never recover from a bad early period |
| `todayCount === 0` → "No new sessions in the last 24 hours" | Says sessions, counts leads |
| none of the above → "All systems healthy." | **This is the dangerous one.** It fires whenever those five specific conditions are quiet. It is not a statement about system health and it renders in green with a tick |

### Conversion funnel (people)

`renderFunnel`, `index.js:2076`, fed `peopleTotal, peopleCompleted, peopleBooked,
peopleDisqualified` at `index.js:2302`. Percentages are all *of `peopleTotal`* — so
"People booked" shows booked-as-%-of-everyone, whereas the card above it in the same
screen shows booked-as-%-of-completed (`index.js:2294`). **Two different denominators
for the same metric, six inches apart, both unlabelled.** Reading them as the same
number is the natural mistake.

Also: the four bars are not a funnel. Disqualified is not a stage after booked, and
a disqualified person can also be in the completed and booked bars. The bars overlap
and don't sum.

### 14-day chart

`index.js:1199-1207` + `renderChart` at `index.js:2077`. Counts `leads` rows grouped
by IST day, labelled "Form sessions per day" ([§1 item 3](#1-the-five-things-that-are-actually-wrong)).
The `WHERE created_at >= NOW() - INTERVAL '14 days'` window is rolling, so the
oldest bucket starts at the current wall-clock time 14 days ago rather than at
00:00 IST — **the chart normally draws 15 bars, with the first and last both
partial**, and nothing on screen says so. `autoSkip:false` on the x-axis
(`index.js:2077`) means all labels always render, so a growing window would crowd,
but the window is fixed at 14 days so this is fine.

Days with zero leads are **absent, not zero** — there is no `generate_series`
zero-fill here, unlike the LM chart which does it deliberately
(`lead-magnet.js:511-513` and its comment). So a dead day is skipped and the bars
either side sit adjacent, making a gap look like continuity. The LM chart's author
identified this exact failure mode and fixed it there; this chart still has it.

### Enrichment coverage (rendered on the Health tab, sourced here)

`index.js:1209-1216` + `index.js:1274-1279`. Four numbers over `enrichment_data`
only: total rows, and the % with a title / funding / location. Denominator is
enriched rows, and the sub-labels say "% of enriched" — accurate. Note
`enriched_total_funding IS NOT NULL` is a presence test on a TEXT column
(`db.js:96` region), so an Apollo response containing the string `"0"` or `""`
counts as having funding data. Whether Apollo ever writes an empty string rather
than NULL is a data question.

---

## 6. All Leads tab

Data source: `/monitor/leads` (`index.js:1557`).

### The filters

All filters are pushed onto one `conditions` array and joined with **`AND`**
(`index.js:1629`: `conditions.join(' AND ')`). There is no OR anywhere between
filters. Combining any two narrows.

| Control | Line | What it actually matches | Surprises |
|---|---|---|---|
| Stage | `1592-1595` | Four independent predicates | **Not mutually exclusive**; contradicts the badge — [§1 item 5](#1-the-five-things-that-are-actually-wrong). `step1` = `completed = false AND disqualified = false`, so it silently includes anything with those flags regardless of `step_reached`, and excludes NULLs |
| Sell-to | `1601` | `l.sell_to = $n` exact | Picking `B2B` does **not** match `B2B (clarified from B2C)`. That's why `__clarified` exists (`1597-1599`, `LIKE 'B2B (clarified from%'`), but a user picking "B2B" expecting all B2B gets a smaller set than they think |
| Source | `1602` | `l.utm_source = $n` exact | Rows with NULL/empty `utm_source` are excluded, as expected. Options come from `/monitor/filter-options` (`1729`) which is `LIMIT 100` by `COUNT(*) DESC` — **a source outside the top 100 is unselectable and silently absent from the dropdown** |
| Heard about us | `1603` | `LOWER(COALESCE(hear_about_us,'')) LIKE '%q%'` | **Substring, not exact.** It's a `datalist` (`index.js:1945-1946`) so it *looks* like a picker: choosing "Google" also returns "Google Ads", "Googled it", etc. |
| Enrichment | `1605-1610` | yes: `l.enriched_title IS NOT NULL OR l.enriched_company_size IS NOT NULL OR EXISTS(enrichment_data row with title/size/company)` | Consistent with `enrichBadge` (`index.js:2079`), which tests `enriched_title \|\| enriched_company_size \|\| e_company`. These genuinely agree — worth noting since so little else does |
| Website check | `1612-1620` | `failed`: `website_check_failed IS TRUE`. `passed`: `IS NOT TRUE` (covers false and NULL — commented, deliberate). `social`: `reason = 'social_profile_url'`. `unverified`: failed OR (reason non-empty AND NOT IN `WEBSITE_VERIFIED_REASONS`) | `unverified` is built from the live constant (`1619`) so it can't drift from the Meta gate — good. But "Passed" includes every pre-migration NULL row, i.e. "we never checked" is filed under "Passed" |
| Attempts | `1622-1623` | `EXISTS`/`NOT EXISTS` a row with the same lowered email and an earlier `created_at` | Correct. Expensive — see [§12](#12-what-breaks-as-data-grows) |
| Date from | `1625` | `l.created_at >= $n::date` | Boundary is **05:30 IST**, confirmed (session TZ is `Etc/UTC`) — [§4](#4-cross-cutting-timezones) |
| Date to | `1626` | `l.created_at < ($n::date + INTERVAL '1 day')` | **05:30 IST**; includes the small hours of the *following* IST day |
| Presets | `2135` | Sets the two date inputs from the **browser's** local clock | Different results for viewers in different timezones |
| Search | `1627-1631` | `LOWER(email) LIKE '%q%' OR LOWER(COALESCE(company,'')) LIKE OR LOWER(COALESCE(first_name,'')) LIKE` | Placeholder says "Search email, company…". It also searches first name (undocumented, harmless) but **not last name, website, phone or session_id**. Searching for a surname returns nothing and looks like "no such lead" |

Note the three-way OR inside search is correctly parenthesised (`index.js:1630`),
so it can't leak past the surrounding ANDs. No SQL injection surface: every user
value is a bound parameter, and the only interpolated strings are the sort column
(whitelisted via `sortMap`, `index.js:1576-1584`) and the verdict list
(regex-filtered, `index.js:1619`).

`clearF()` (`index.js:2132`) resets all eleven controls and the sort. It is complete
— I checked each id against the markup.

### "N leads found"

`index.js:2146`: `set("lcount", d.total + " lead(s) found")`, where `d.total` comes
from `index.js:1699-1702`:

```sql
SELECT COUNT(*) AS total FROM leads l WHERE true ${whereClause}
```

**This is the true count of matching rows, not capped.** It is a separate query from
the data query and does **not** include the `LEFT JOIN enrichment_data`
(`index.js:1667`). That would normally be a row-multiplication risk, but
`enrichment_data.session_id` is `UUID UNIQUE NOT NULL` (`db.js:78`), so the join
cannot fan out and the count is exactly the number of rows the data query would
return unpaginated. This one is correct.

Agreement between the three surfaces:

- **"N leads found"** = true total matching the filters.
- **Rows rendered** = 25 (`index.js:1562`, `limit = 25`, hardcoded, not user-adjustable),
  with pagination below (`renderPag`, `index.js:2151`).
- **Export CSV** = `baseSelect + orderBy` with **no LIMIT and no OFFSET**
  (`index.js:1671`), same `whereClause`, same params. So the CSV row count should
  equal "N leads found" exactly.

`exportLeads()` (`index.js:2137`) forwards every filter plus `sort` and `dir`, and
deliberately omits `page`. Verified control-by-control against `loadLeads`
(`index.js:2140-2142`) — the two build identical query strings apart from
`format=csv` vs `page=`. **The CSV matches the screen's filter set. It does not
match the screen's columns**: the table shows 9 (`index.js:1949-1958`), the CSV
writes 42 (`index.js:1672-1682`). The CSV is a superset except for one omission —
`session_id` is in the expandable row panel (`index.js:2126`) but **not** in the
CSV column list, so you cannot join an exported row back to a session.

CSV escaping (`index.js:1684-1688`) quotes on comma, quote or newline and doubles
internal quotes. Correct. It does not guard against a leading `=`/`+`/`-`/`@`,
so a company name starting with `=` becomes a formula in Excel. Low severity,
worth knowing since these files go to SDRs.

### Sorting

**Server-side, over the full result set.** `index.js:1582-1584` builds
`ORDER BY <col> <dir> NULLS LAST, l.created_at DESC` and it is appended before
`LIMIT/OFFSET` (`index.js:1706`). So page 2 of an email-ascending sort is genuinely
the second 25 of the whole matching set, not a re-sort of a loaded page. Correct.

Only five columns are sortable (`sortMap`, `index.js:1576-1582`); Stage, Booked,
Enrichment and Source have no `onclick` in the markup (`index.js:1955`), which
matches. `name` sorts by `first_name` only (`index.js:1579`) while the column
renders `first_name + last_name` (`index.js:2147`) — so a "Name" sort is really a
first-name sort. `NULLS LAST` is applied consistently. `renderSortArrows`
(`index.js:2133`) covers exactly the five sortable columns.

An unrecognised `?sort=` value falls back to `created_at` silently
(`index.js:1583`) rather than erroring. Fine.

### Pagination

`limit = 25` hardcoded (`index.js:1562`), `pages = Math.ceil(total/limit)`
(`index.js:1719`). `renderPag` (`index.js:2151`) renders a windowed pager (±2 pages
plus first/last). No upper bound on page number: requesting `?page=99999` on a
small dataset returns an empty `leads` array, and the UI renders "No leads match
your filters" (`index.js:2147`) — misleading but harmless, and not reachable
through the UI's own controls.

### The expandable detail panel

`enrichPanel`, `index.js:2081-2130`. ~40 fields, `.filter(f => f.v)` at
`index.js:2128` so falsy values are dropped. Two consequences worth naming:

- A genuine `0` or `false` is dropped along with null. `{lb:"Email sent",
  v: l.loops_sent?"Yes":"No"}` (`index.js:2125`) sidesteps this by stringifying
  first, which is the right pattern; most other fields don't need it.
- The website-check rows are a three-way chain (`index.js:2100-2102`) that mirrors
  the Slack logic at `index.js:987-1013` but is a **second copy of the rule** in a
  JS string. The `WLBL` label map at `index.js:2070` is a third copy of
  `WEBSITE_REASON_LABELS` (`index.js:467`). `CLAUDE.md` already warns about the
  two label copies; the "which icon do I show" rule is now duplicated too. The
  `unverifiable_pair` flag was deliberately *not* duplicated (`index.js:2103-2106`
  comment, computed server-side at `index.js:1712`) — that's the right call and the
  contrast is instructive.

`{lb:"Meeting", v: l.start_time ? ist(l.start_time) : null}` (`index.js:2124`) runs
`new Date()` over a **TEXT** column (`db.js:65`) populated straight from webhook
payloads (`index.js:4376, 4658`). If a provider ever sends a string without a UTC
offset, the browser parses it in the viewer's local zone and then relabels it IST.
Silently wrong, no error.

---

## 7. SDR List tab

Data source: `/monitor/sdr` (`index.js:1742`). Single query, `index.js:1749-1793`.

**What it selects**: `DISTINCT ON (LOWER(l.email))` over `leads` LEFT JOINed to
`enrichment_data`, `ORDER BY LOWER(l.email), l.created_at DESC` — so the newest row
per email wins — then re-sorted `ORDER BY created_at DESC` in the outer query
(`index.js:1792`).

**Population** (`index.js:1783-1791`): email not null, `disqualified = false`,
`sell_to ILIKE 'B2B%'`, and no session of that email has a `booking_uid`.

**There is no `completed` filter.** The header says "Qualified B2B leads who have
never booked a call — deduped by email" (`index.js:1967`), which is accurate. The
Overview tooltip claiming this equals the completed-only number
(`index.js:1922`) is the thing that's wrong, not this query. But the tab's own
Stage column (`index.js:2340`) renders `Completed` or `Step 1`, so the presence of
step-1-only rows is at least visible once you look.

**No date filter, no window.** This is the full lifetime backlog. An SDR opening
the tab sees leads from July alongside yesterday's.

**No LIMIT.** `index.js:1749` returns every matching row as one JSON array
(`index.js:1825`). See [§12](#12-what-breaks-as-data-grows).

**Search is client-side** (`renderSDRTable`, `index.js:2332-2333`) over the already
loaded array, matching `email`, `company`, `first_name`, `enriched_industry`.
Note it searches industry — which the All Leads search doesn't — and doesn't search
title, which is a visible column.

**The count and the CSV disagree.** `sdr-count` shows `leads.length` *after* the
client-side search filter (`index.js:2334`), so it correctly matches the rendered
rows. But `exportSDR()` (`index.js:2354`) is:

```js
window.location.href = API+"/monitor/sdr"+(TP||"?")+(TP?"&":"")+"format=csv";
```

**No search parameter.** Type a search, see "3 leads", click Export CSV, get the
entire list. The button is right next to the count that it ignores. This is a real
trap for anyone building a call list.

CSV columns (`index.js:1798-1806`) are 30 fields vs the table's 10
(`index.js:1971-1972`) — superset, same query, so the row *set* is the same as the
unfiltered table.

**Empty-state colspan bug**: the table has 10 columns (`index.js:1971-1972`) but the
loading and empty rows use `colspan="9"` (`index.js:1973`, `index.js:2335`) while
the detail row uses `colspan="9"` after a spacer `<td>` (`index.js:2345`), which is
correct. Cosmetic only.

---

## 8. Duplicates tab

Data source: `/monitor/duplicates` (`index.js:1519`). Query `index.js:1524-1548`.

**Definition of a duplicate**: `GROUP BY LOWER(l.email), l.email HAVING COUNT(*) > 1`
(`index.js:1546-1547`) — an email with more than one `leads` row. Ordered by session
count then most recent.

**The `GROUP BY` has a subtle flaw.** It groups by *both* `LOWER(l.email)` and
`l.email`. That means `Bob@x.com` and `bob@x.com` form two separate groups, each
possibly with a count below 2, so **a person who typed their email with different
capitalisation across sessions may not appear as a duplicate at all** — or may
appear as two rows. Grouping by `LOWER(l.email)` alone and selecting
`MIN(l.email)` would be the fix. Every other dedup in the system keys on
`LOWER(email)` only (`index.js:1176, 1234, 1784`), so this route is the odd one out
and under-reports.

**Does this definition match how duplicates are handled elsewhere? No, and the
mismatch is the known open bug.** `CLAUDE.md` records it: the duplicate-booking
guard looks up the *newest* lead row per email and asks whether that row has a
booking, so a second form submission creates a newer row and the same person can
take two calendar slots. This tab defines a duplicate as "more than one row per
email" — which is exactly the condition that defeats the guard — but it does not
surface the thing that matters. It shows `has_booking` as a single boolean
(`index.js:1528`, `MAX(CASE WHEN booking_uid IS NOT NULL THEN 1 ELSE 0 END)`), so
**an email with two distinct `booking_uid`s — a genuine double-booking, the actual
incident — renders identically to an email with one booking.** The data needed to
spot it is right there in the `json_agg` (`index.js:1531-1542`, each session's
`booking_uid`) but nothing counts distinct non-null values. Adding
`COUNT(DISTINCT booking_uid)` would turn this tab into the detector for the open
bug. I have not changed it — flagging per `CLAUDE.md`.

**Other properties**: cannot double-count (grouped). Cannot miss rows other than
via the case-sensitivity issue above. `total` (`index.js:1550`) is
`result.rows.length` — the true group count, and it matches the rendered rows
because there's no pagination. Population is all `leads` rows with an email:
includes internal/test addresses, includes webhook rows (which mint a server-side
`session_id` and will pair with a real form row for the same person, so **a
webhook-origin booking legitimately creates a "duplicate" that isn't a data
problem** — nothing on screen distinguishes it, though `page_url` in the expanded
panel gives a hint).

**No LIMIT, and `json_agg` inlines every session per email** (`index.js:1531-1542`).
Payload grows with the square of repeat behaviour. See [§12](#12-what-breaks-as-data-grows).

No CSV export on this tab.

---

## 9. Lead Magnet tab

Two sources: `/monitor/lm-metrics` (`lead-magnet.js:458`) and `/monitor/lm-leads`
(`lead-magnet.js:544`). Both honour a `days` selector (7/30/90/365,
`index.js:2010-2012`), default 30.

### Population mismatch between the two routes

`lm-metrics` scopes everything with
`created_at > NOW() - INTERVAL 'N days' AND is_internal IS NOT TRUE`
(`lead-magnet.js:461`).

`lm-leads` scopes `email IS NOT NULL AND created_at > NOW() - INTERVAL 'N days'`
(`lead-magnet.js:568-569`) — **no `is_internal` filter**. Internal rows are returned
and filtered in the browser (`lmMatch`, `index.js:2207-2211`).

Net effect: the funnel cards exclude internal tests, the leads table excludes them
too (client-side), and there's an "Internal tests" pill to see them. That actually
works out. But the pill counts (`index.js:2218`) are computed from the **loaded
array only**, which is capped — see below.

### Funnel cards

All from `lead-magnet.js:464-489`, one row of `COUNT(*) FILTER` over
`lead_magnet_leads`.

| Card | SQL | Label honest? |
|---|---|---|
| Page views | `COUNT(*)` | **No, twice over.** The table is one row per page load (`db.js:123` region comment), so this is *sessions*. The card says "Page views" and the sub-label says "people who loaded the LP". It is neither views nor people |
| Form opened | `COUNT(*) FILTER (WHERE step_reached >= 2)` | Yes |
| Email entered | `COUNT(*) FILTER (WHERE email IS NOT NULL)` | Yes |
| Submitted | `COUNT(*) FILTER (WHERE completed)` | Yes |

Rates at `index.js:2173` are chained correctly — opens/views, emails/opens,
submitted/emails — each of the previous step, and each sub-label says which
("% of opens" etc.). Good.

The people line (`index.js:2176`, from `lead-magnet.js:479-489`) uses
`COUNT(DISTINCT email)` and computes `people_abandoned` as a set difference rather
than a per-row filter, with a comment explaining why. That's correct and carefully
done — someone who abandons Monday and completes Friday lands only in
`people_submitted`.

### Where people drop off

`lmDrop`, `index.js:2162`, fed `bounced_before_open`, `opened_no_email`, `abandoned`
(`lead-magnet.js:469-471`). Each is a `COUNT(*) FILTER` and each is divided by a
different, correct base (`v`, `o`, `e` respectively at `index.js:2178-2180`).
`abandoned` = `email IS NOT NULL AND NOT completed`. Consistent with the funnel.
The three drop-offs plus `submitted` should sum to `views`; they do, because the
predicates partition on `step_reached` and `email`/`completed`. This section is sound.

### Daily volume chart

`lead-magnet.js:511-519`. `generate_series` zero-fills — deliberately, with a good
comment. Bare `date_trunc`, so the buckets break at **05:30 IST** — confirmed, not
inferred ([§4](#4-cross-cutting-timezones)).
Labels are `x.day.slice(5)` (`index.js:2197`) on a `to_char(...,'YYYY-MM-DD')`
string (`lead-magnet.js:509`), giving `MM-DD`. At `days=365` that's 366 x-axis
labels on a `height:90` canvas — unreadable, but Chart.js will thin them since
`autoSkip` isn't disabled here (unlike the Overview chart).

The join `ON date_trunc('day', l.created_at) = d.day AND l.is_internal IS NOT TRUE`
(`lead-magnet.js:517-518`) applies the internal filter but **not** the days scope to
`l`; the equality join bounds it implicitly. Correct, if indirect.

### Industries / Custom categories / Entry points

- Industries (`lead-magnet.js:494-501`): `WHERE completed AND industry_category IS
  NOT NULL`, `LIMIT 15`. Rendered by `lmBars` with `total = sb` (submitted)
  (`index.js:2183`), so percentages are of all submissions — correct denominator.
  **`LIMIT 15` is silent**: a 16th industry vanishes with no "and N more".
- Custom categories (`lead-magnet.js:503-507`): `industry_is_custom = true`,
  `LIMIT 50`, silent again.
- Entry points (`lead-magnet.js:520-526`): `step_reached >= 2`, `LIMIT 10`, silent.
  Bar widths are relative to the largest (`epMax`, `index.js:2189`), not to the
  total — so the top bar is always 100% wide. The `% submitted` figure next to it
  is `completed/n` for that entry point, which is right.
- Email type (`index.js:2186-2187`): `free_email`/`business_email` from
  `lead-magnet.js:472-473`, both `FILTER (WHERE completed AND ...)`. Denominator is
  `fr+bz`, so it's % of submissions with a classified email. Fine.

### Leads table

`/monitor/lm-leads`, `lead-magnet.js:544-577`.

**`LIMIT 500` by default, max 2000** (`lead-magnet.js:546`). The dashboard never
sends a `limit` (`index.js:2203` passes only `days` and the token), so **it is
always 500**. Once the LM funnel exceeds 500 email-bearing sessions in the selected
window, the table silently shows the most recent 500 and:

- `lm-count` says "N shown" (`index.js:2222`) where N is the client-filtered count
  of the loaded 500 — it never says "of M total", because the route doesn't return
  a total.
- The status pills' counts (`index.js:2218`) are computed over the loaded 500 only,
  so "Awaiting send: 40" can be wrong when there are 600 awaiting.
- The CSV (`lmCsv`, `index.js:2283`) exports `lmSearched()` — the client-filtered
  loaded rows. So it matches the screen exactly, and is capped at the same 500.
  This is the only tab where the CSV honours the search box.

**This is the clearest "silently returns a wrong number" on the whole dashboard**:
nothing anywhere indicates truncation, and the pill counts read as totals.

`attempts` (`lead-magnet.js:561-562`) is a correlated subquery with **no days
scope** — lifetime session count for that email — displayed inside a 30-day list.
And it's `a.email = l.email`, case-sensitive, unlike everything on the `leads` side.

`status` (`lead-magnet.js:563-567`) is a three-way CASE: `sent` (completed +
delivered), `awaiting` (completed, not delivered), `abandoned` (everything else).
Exhaustive and non-overlapping. Correct.

### Mark sent / Undo / Retry

- `lmMark` (`index.js:2276`) → `POST /monitor/lm-delivered/:id`
  (`lead-magnet.js:584`). The **set** path guards `AND completed = true`
  (`lead-magnet.js:594`) so you can't mark an abandoned lead delivered; the
  **undo** path has no such guard (`lead-magnet.js:592-593`), which is right.
  Returns `{ok:true, updated: rowCount}` — and `rowCount` can be **0** while
  `ok` is `true`. The client checks only `r.ok` (`index.js:2277`), so a no-op
  update (bad id, or set-on-incomplete) reports success and the UI optimistically
  flips the row (`index.js:2278-2279`) before `loadLM()` silently flips it back.
- `lmLoopsRetry` (`index.js:2274`) → `POST /monitor/lm-loops-retry/:id`
  (`lead-magnet.js:623`). Correctly 404s on a non-completed lead
  (`lead-magnet.js:632`). Writes `loops_error` on failure. Reasonable.

---

## 10. System Health tab

Nine rows, `index.js:1989-1997`. This is the weakest part of the system and the
part most likely to be trusted.

| Row | Sub-label | What it actually checks | Can it read healthy while broken? |
|---|---|---|---|
| API uptime | "/health responding" | Real: `fetch(API+"/health")` with a 5s timeout (`index.js:2073`) | No. This one is honest |
| Step 1 — /partial | "Email + lead saved to Railway + AWS" | **Nothing.** `badge("s-partial", d.total+" sessions saved", "bg")` (`index.js:2300`) — the class is the literal string `"bg"`, green, unconditionally | **Yes, always.** It is a lifetime row count coloured green. It cannot ever report a problem, and it claims to cover the AWS write too |
| Step 2 — /submit | "Lead completed + Slack fired" | `d.completed > 0` — has there *ever* been a completion (`index.js:1167`) | **Yes.** Green forever after the first submit in July. Also says nothing about Slack |
| ELV email verification | "Inconclusive rate, rolling 90-minute window" | Real: `/monitor/elv-health` → `elvHealthSnapshot()` (`index.js:3089`, snapshot at the `elvHealthSnapshot` definition). 90-minute window (`index.js:2469`), min sample 8 (`index.js:2470`) | Mostly no. It is in-memory and per-instance, so it resets on redeploy — but that shows as `insufficient_data` → grey "Quiet", not green. The v5.4.0 comment shows this was a deliberate fix. Good |
| Apollo enrichment | "enrichment_data populated per session" | `COUNT(*) FROM enrichment_data / COUNT(*) FROM leads`, all time (`index.js:1208`, `1166`); green ≥60%, amber ≥30%, red below (`index.js:2300`) | **Yes.** A lifetime ratio. If Apollo died today, a year of good history keeps it green indefinitely |
| Booking — RevenueHero | "People booked / people completed" | Exactly that, all time (`index.js:1178`, `1177`); green ≥50% | **Yes.** Lifetime ratio, same problem. Also inflated by webhook rows, which add to both sides |
| Cron — drop-off recovery | "Leads waiting >2 hours without booking" | `pendingPartials === 0` → green, else amber (`index.js:2300`) | **Yes, in the worst way.** If the cron is dead *and* no new leads arrive, pending stays 0 and the row is green. It also **never goes red** — a permanently dead cron with a growing backlog shows amber |
| AWS sync | "gw_form_leads mirror" | `!!awsPool` (`index.js:1290`), i.e. `AWS_PG_HOST` is set (`index.js:111`) | **Yes.** Pure config presence. Wrong credentials, unreachable host, every mirror write failing — all read "Active" green. The individual write failures are caught and only `console.warn`ed (`index.js:4516, 4531, 4575`) |
| Email recovery | "Follow-up emails sent to partial leads" | `d.loopsSent > 0` (`index.js:1170`) | **Yes.** Lifetime count, green forever after the first send |

So: **two real checks, seven counters.** The header "Step health" and the
green/amber/red badge vocabulary make all nine read as live status. A reader would
reasonably assume green means "working now"; for seven of them it means "worked at
least once, ever" or "the env var is set".

Below the badges, the Enrichment coverage cards (`index.js:1999-2005`) are honest —
labelled "% of enriched" and sourced from `index.js:1209-1216`.

One more: `pct()` (`index.js:2071`) returns `"0%"` when the denominator is zero
rather than `"—"`. So a fresh database shows "0% of people" instead of "no data",
and `renderFunnel` (`index.js:2076`) shows `0%` bars. A quiet zero reading as a
real measurement is the same failure mode the funnel route's author wrote a long
comment about avoiding (`index.js:1478-1484`) — the fix was applied there and not here.

---

## 11. Routes the UI never fetches

**`/monitor/funnel`** (`index.js:1383`). The most carefully built query in the file
— `WEBHOOK_LEAD_SQL` exclusion, go-live coverage flags, `orphan_leads` nulled where
it would be meaningless, `step1_rate` nulled on partial days, a `rate_note` in the
response explaining all of it (`index.js:1505`). None of it is on screen. Grep for
`monitor/funnel` finds only the route (`1383`) and its error log (`1514`). Its
`orphan_leads` metric is the only thing in the system that measures dropped
`/session` writes, and nobody can see it without curling the endpoint.

It also buckets days in UTC while the one chart that *is* on screen buckets in IST,
so if it were wired up as-is the two would disagree by 5½ hours.

**`/monitor/lm-loops-health`** (`lead-magnet.js:604`). Calls `testLoopsKey()` and
returns sent/pending/errored counts over `lead_magnet_leads`. The LM tab shows
Loops state per-row in the detail panel (`index.js:2261`) but never the aggregate,
so "how many LM leads are stuck un-pushed" is invisible. Note its `sent` count has
no `is_internal` guard while `pending` does (`lead-magnet.js:610-612`) — the two
aren't comparable.

**`/monitor/website-recheck`** (`index.js:3831`) — intentionally manual, documented
at `index.js:3819-3830`. Correctly refuses when `MONITOR_TOKEN` is unset. `limit`
capped at 500, `offset` supported, groups by distinct domain before checking
(`index.js:3876-3882`) so shared domains cost one fetch. Dry-run by default. The
one real concern is that it's a mutating GET — [§2](#2-route-inventory).

---

## 12. What breaks as data grows

**Unbounded responses.** Three endpoints return every matching row with no LIMIT:

- `/monitor/sdr` (`index.js:1749`) — the entire lifetime B2B-never-booked backlog,
  ~30 fields per row, plus a client-side search over the whole array. This only ever
  grows, because a lead leaves the set only by booking or being disqualified.
- `/monitor/duplicates` (`index.js:1524`) — every repeat email, with a `json_agg` of
  every session for each (`index.js:1531-1542`). Grows super-linearly with repeat
  behaviour.
- `/monitor/leads?format=csv` (`index.js:1671`) — by design for an export, but it
  runs the two correlated `prior_attempts`/`prior_disqualified` subqueries
  (`index.js:1640-1641`) for **every** exported row.

The dashboard fetch timeouts are 15s for SDR and Duplicates (`index.js:2311, 2361`)
and 12s for metrics and leads (`index.js:2145, 2291`). When these are exceeded the
tab renders "Failed: …" — which is at least honest, unlike a silent truncation.

**O(n²) query patterns.** `prior_attempts` and `prior_disqualified`
(`index.js:1640-1641`) and the `repeatAttempts` filter (`index.js:1622-1623`) all
correlate `LOWER(pa.email) = LOWER(l.email)`. There is **no index on `LOWER(email)`**
— `db.js` creates indexes on `form_sessions(created_at)` (`db.js:233`),
`lead_magnet_leads(email)` and `(created_at)` (`db.js:170-171`), and
`email_verifications` (`db.js:282`), but nothing on `leads(email)` or
`LOWER(leads.email)`. Every email-dedup query in the monitor — `index.js:1176,
1187, 1193, 1234, 1242, 1548, 1784, 1789` — is doing a sequential scan with a
function call per row. This is the thing most likely to turn the dashboard from
slow into timing-out, and it will happen on the counts first because they scan the
whole table.

**Silent caps that report as totals.**

| Cap | Line | Reported? |
|---|---|---|
| `lm-leads` LIMIT 500 | `lead-magnet.js:546` | **No** — pill counts and "N shown" read as totals |
| filter-options LIMIT 100 | `index.js:1729-1730` | **No** — a source outside the top 100 is unselectable |
| LM industries LIMIT 15 | `lead-magnet.js:501` | No |
| LM custom LIMIT 50 | `lead-magnet.js:507` | No |
| LM entry points LIMIT 10 | `lead-magnet.js:526` | No |
| Overview "recent leads" LIMIT 50 | `index.js:1251` | Returned as `recentLeads` and **never rendered** — dead payload |
| All Leads LIMIT 25 | `index.js:1562` | **Yes** — pagination and a true total |

**Depends on data older rows may not have.** `website_check_failed` /
`website_check_reason` (added by migration), `booked_at`, `submitted_at`,
`elv_status`, `landing_page`, `previous_page`, `prefill_source`. The website-check
filter handles this explicitly (`index.js:1613`, "covers false AND null
(pre-migration rows)") and `isWebsiteVerified` treats a NULL reason as a pass
(`index.js:527`). `booked_at` NULL is handled in one place (`index.js:1193`) and
not in another (`index.js:1228`) — [§1 item 4](#1-the-five-things-that-are-actually-wrong).
`form_sessions` did not exist before 21 Aug, which only `/monitor/funnel` accounts
for.

---

## 13. Open items and how to settle them

Item 1 of the original thirteen — the Postgres session timezone — is **settled**:
`SHOW TimeZone` returns `Etc/UTC`, so every 05:30 IST boundary in this document is
now stated as fact. See [§4](#4-cross-cutting-timezones).

The remaining twelve are below, **ordered by how much it matters if the answer comes
back broken**, then grouped by where you'd run them. Everything here is read-only:
`SELECT`, `EXPLAIN` (no `ANALYZE` on a write — see the note in A6), and `GET`
requests. Nothing mutates.

### Severity ordering

| Rank | # | Item | If broken, the cost |
|---|---|---|---|
| 1 | 5 | Has the double-booking bug fired? | **Someone took two calendar slots.** Real revenue-team time already lost, and it tells you whether the deferred bug is theoretical or active |
| 2 | 3 | Booked rows with NULL `booked_at` | "Pending recovery" over-reports, and the recovery cron may be re-chasing people who already booked. Directly wastes SDR effort and risks emailing a booked customer |
| 3 | 8 | Is the LM table truncated at 500? | Every LM pill count silently understates. An "Awaiting send" queue that reads 40 when it's 600 means leads never get their magnet |
| 4 | 6 | Internal/test lead volume | Sets the error bar on *every* number on Overview, All Leads, SDR and Duplicates. If it's large, the dashboard has been miscalibrating decisions for months |
| 5 | 7 | Webhook-origin lead volume | Inflates the booking rate on both sides of the ratio. This is the number the Booking health badge is built on |
| 6 | 12 | Are `LOWER(email)` scans already slow? | Determines whether the dashboard degrades gracefully or starts timing out. No wrong numbers, but a monitor nobody can load is a monitor nobody uses |
| 7 | 4 | Email capitalisation variance | Duplicates under-reports, and by exactly the amount that hides the item-1 double-bookings from view |
| 8 | 11 | Is the "Meeting" time ever misparsed? | A meeting shown at the wrong hour to an SDR. Low volume, high embarrassment |
| 9 | 2 | NULL-email `leads` rows | "N sessions" and "Total people" count different populations with no note. Cosmetic unless the count is non-trivial |
| 10 | 9 | Orphan `enrichment_data` rows | "Low enrichment rate" could read >100%. Confusing, not decision-changing |
| 11 | 10 | Empty-string `enriched_total_funding` | Overstates one coverage percentage on the Health tab. Nobody acts on it |
| 12 | 13 | Has the ungated ELV endpoint been probed? | No lead data is exposed. Worth knowing, changes nothing you'd do — you'd gate it regardless |

### Group A — Railway Postgres console

Run in severity order. Each is a single statement.

**A1 · item 5 · Has the double-booking bug fired?** (rank 1)

```sql
SELECT lower(email) AS person,
       count(DISTINCT booking_uid) AS distinct_bookings,
       min(created_at) AS first_seen,
       max(created_at) AS last_seen
  FROM leads
 WHERE booking_uid IS NOT NULL AND email IS NOT NULL
 GROUP BY 1
HAVING count(DISTINCT booking_uid) > 1
 ORDER BY 2 DESC;
```

- **Correct**: zero rows. The guard's known weakness has never been exercised.
- **Broken**: any row. Each one is a person holding `distinct_bookings` calendar
  slots. `CLAUDE.md` marks the guard as deferred by you, so this is a
  *measurement*, not a prompt to fix it — but the count changes whether "deferred"
  is still the right call.

**A2 · item 3 · Booked rows with NULL `booked_at`** (rank 2)

```sql
SELECT count(*) FILTER (WHERE booked_at IS NULL)  AS null_booked_at,
       count(*)                                    AS all_booked,
       min(created_at) FILTER (WHERE booked_at IS NULL) AS oldest_affected,
       max(created_at) FILTER (WHERE booked_at IS NULL) AS newest_affected
  FROM leads
 WHERE booking_uid IS NOT NULL;
```

- **Correct**: `null_booked_at = 0`. The `COALESCE` gap at `index.js:1228` is
  latent and "Pending recovery" is accurate.
- **Broken**: any non-zero. That many booked leads are invisible to the
  `booked_at >= l.created_at` test, so they inflate "Pending recovery" and are
  eligible for the recovery cron (`index.js:4499` uses the same shape). Check
  `newest_affected`: if it is recent, a live write path is failing to set
  `booked_at` and this is not just a pre-migration artefact.

**A3 · item 8 · Is the Lead Magnet table truncated?** (rank 3)

```sql
SELECT count(*) FILTER (WHERE created_at > now() - interval '7 days')   AS d7,
       count(*) FILTER (WHERE created_at > now() - interval '30 days')  AS d30,
       count(*) FILTER (WHERE created_at > now() - interval '90 days')  AS d90,
       count(*) FILTER (WHERE created_at > now() - interval '365 days') AS d365
  FROM lead_magnet_leads
 WHERE email IS NOT NULL;
```

- **Correct**: every bucket under 500. The `LIMIT 500` at `lead-magnet.js:546` is
  not biting and the pill counts are true totals.
- **Broken**: any bucket ≥ 500 — that day-range selector silently shows only the
  most recent 500. Given the selector defaults to 30 days, `d30 >= 500` is the one
  that matters today; `d90`/`d365` tell you when the 7- and 30-day views will
  follow.

**A4 · item 6 · Internal and test lead volume** (rank 4)

```sql
SELECT count(*)                                                        AS total_leads,
       count(*) FILTER (WHERE email ILIKE '%@gushwork.ai')             AS gushwork,
       count(*) FILTER (WHERE email ILIKE '%@test.com'
                           OR email ILIKE '%@example.com'
                           OR email ILIKE '%@example.org')             AS example_domains,
       count(*) FILTER (WHERE lower(email) = 'b@g.ai')                 AS test_address,
       count(*) FILTER (WHERE booking_uid IS NOT NULL
                          AND email ILIKE '%@gushwork.ai')             AS internal_bookings
  FROM leads;
```

The four filtered columns mirror `ELV_EXCLUDED_DOMAINS` (`index.js:2488`) plus the
`b@g.ai` test address (`index.js:4369, 4651`) — the exclusion list the monitor
routes never apply.

- **Correct**: internal rows are a rounding error against `total_leads` (say <1%),
  so every dashboard number is off by less than its own noise.
- **Broken**: anything material. `internal_bookings` is the sharp one — internal
  bookings land in the numerator *and* denominator of the Booking health badge
  (`index.js:2294`, `2300`), so they push the booking rate up twice.

**A5 · item 7 · Webhook-origin lead volume** (rank 5)

```sql
SELECT count(*) FILTER (WHERE coalesce(prefill_source,'') = 'rh_webhook')  AS rh,
       count(*) FILTER (WHERE coalesce(prefill_source,'') = 'cal_webhook') AS cal,
       count(*) FILTER (WHERE coalesce(prefill_source,'')
                              IN ('rh_webhook','cal_webhook')
                          AND booking_uid IS NOT NULL)                     AS webhook_booked,
       count(*) FILTER (WHERE completed)                                   AS all_completed,
       count(*) FILTER (WHERE booking_uid IS NOT NULL)                     AS all_booked
  FROM leads;
```

- **Correct**: `rh + cal` is small relative to `all_completed`. The Overview
  booking rate is roughly honest.
- **Broken**: `webhook_booked / all_booked` is a large fraction. These rows never
  touched the form (`index.js:4429, 4721`) but arrive with `completed = true` and a
  `booking_uid`, so they inflate the ratio `peopleBooked / peopleCompleted` from
  both directions. `/monitor/funnel` already excludes them and explains why
  (`index.js:1327-1338`); this tells you the size of the fix that route made and
  `/monitor/metrics` didn't.

**A6 · item 12 · Are the `LOWER(email)` scans already slow?** (rank 6)

```sql
SELECT count(*) AS lead_rows FROM leads;
```

```sql
EXPLAIN (ANALYZE, BUFFERS)
SELECT count(DISTINCT lower(email)) AS people_total,
       count(DISTINCT lower(email)) FILTER (WHERE completed = true) AS people_completed
  FROM leads
 WHERE email IS NOT NULL;
```

`EXPLAIN ANALYZE` does execute the statement, so it is only safe here because the
statement is a pure `SELECT` — this is the exact query at `index.js:1174-1182`.
Don't reach for `ANALYZE` on anything that writes.

- **Correct**: total runtime comfortably under ~1s. The dashboard's 12s fetch
  timeout (`index.js:2291`) has plenty of headroom.
- **Broken**: a `Seq Scan on leads` taking seconds. There is no index on
  `LOWER(email)` anywhere in `db.js`, and eight monitor queries depend on it
  (`index.js:1176, 1187, 1193, 1234, 1242, 1548, 1784, 1789`). This is the metric
  that predicts when Overview starts failing to load rather than loading wrongly.

**A7 · item 4 · Email capitalisation variance** (rank 7)

```sql
SELECT count(DISTINCT email)        AS distinct_raw,
       count(DISTINCT lower(email)) AS distinct_lowered,
       count(DISTINCT email) - count(DISTINCT lower(email)) AS case_variants
  FROM leads
 WHERE email IS NOT NULL;
```

- **Correct**: `case_variants = 0`. The `GROUP BY LOWER(l.email), l.email` at
  `index.js:1546` behaves identically to grouping on lowercase alone, and the
  Duplicates tab is complete.
- **Broken**: any non-zero. That many people are split across capitalisation
  groups, so Duplicates under-reports — and it under-reports in exactly the
  population A1 is looking at, which means a case-variant double-booker would be
  invisible on *both* screens.

**A8 · item 11 · Is the "Meeting" timestamp parseable?** (rank 8)

```sql
SELECT prefill_source,
       count(*) AS n,
       min(start_time) AS sample_min,
       max(start_time) AS sample_max
  FROM leads
 WHERE start_time IS NOT NULL AND start_time <> ''
 GROUP BY 1
 ORDER BY 2 DESC;
```

- **Correct**: every sample ends in `Z` or carries an explicit `+HH:MM` / `-HH:MM`
  offset. `new Date()` in the browser (`index.js:2124`) then resolves an absolute
  instant and the IST relabel is right.
- **Broken**: any sample with no offset (e.g. `2026-08-25 14:30:00` or
  `2026-08-25T14:30:00`). The browser parses those in the *viewer's* local zone and
  the panel then labels the result IST — wrong by the viewer's UTC offset, with no
  error. Group by `prefill_source` because Cal (`payload.startTime`,
  `index.js:4376`) and RevenueHero (`payload.meeting_time`, `index.js:4658`) are
  independent formats and only one needs to be sloppy. The column is `TEXT`
  (`db.js:65`), so nothing validated it on the way in.

**A9 · item 2 · NULL-email `leads` rows** (rank 9)

```sql
SELECT count(*) FILTER (WHERE email IS NULL)  AS null_email,
       count(*) FILTER (WHERE email = '')     AS empty_email,
       count(*)                                AS total
  FROM leads;
```

- **Correct**: both zero. "N sessions" (`index.js:1166`) and "Total people"
  (`index.js:1176`, scoped `WHERE email IS NOT NULL`) cover the same population and
  the missing note doesn't matter.
- **Broken**: non-zero. The Overview sub-line counts rows the headline excludes.
  Also worth knowing because `CLAUDE.md` states a `leads` row means someone reached
  step 1 and entered an email — a NULL here means a write path exists that
  contradicts that invariant, which is more interesting than the display gap.

**A10 · item 9 · Orphan `enrichment_data` rows** (rank 10)

```sql
SELECT (SELECT count(*) FROM enrichment_data) AS enrichment_rows,
       (SELECT count(*) FROM leads)           AS lead_rows,
       (SELECT count(*) FROM enrichment_data e
          LEFT JOIN leads l ON l.session_id = e.session_id
         WHERE l.session_id IS NULL)          AS orphan_enrichment;
```

- **Correct**: `orphan_enrichment = 0` and `enrichment_rows <= lead_rows`. The
  "Low enrichment rate" alert (`index.js:2075`) and the `s-enrich` badge
  (`index.js:2300`) divide by a denominator that genuinely contains the numerator.
- **Broken**: `enrichment_rows > lead_rows`. The ratio can exceed 100% and the
  badge logic (`er>=60?"bg":...`) will read green for a reason that has nothing to
  do with enrichment working.

**A11 · item 10 · Empty-string funding values** (rank 11)

```sql
SELECT count(*)                                                   AS enriched_rows,
       count(*) FILTER (WHERE enriched_total_funding IS NOT NULL) AS counted_as_present,
       count(*) FILTER (WHERE enriched_total_funding IN ('', '0', '0.0', 'null')) AS hollow
  FROM enrichment_data;
```

- **Correct**: `hollow = 0`. The "With funding data" percentage
  (`index.js:1213`, rendered `index.js:2001`) means what it says.
- **Broken**: `hollow > 0`. That fraction of the percentage is counting the
  *presence of a string* rather than the presence of funding data, because the
  column is `TEXT` and the test is `IS NOT NULL`.

### Group B — curl against a monitor endpoint

Both are plain `GET`s. Substitute your `MONITOR_TOKEN`.

**B1 · item 7, cross-check · the funnel route's own webhook and orphan counts**

```bash
curl -s "https://gushwork-api-production.up.railway.app/monitor/funnel?token=$MONITOR_TOKEN&days=30" \
  | python3 -m json.tool | head -40
```

This is the fastest read on A5 and it also surfaces the only orphan-lead
measurement in the system. Look at `webhook_leads_in_window`,
`orphan_leads_in_window`, `orphan_leads_days_measured` and
`session_tracking_live_since` (`index.js:1499-1509`).

- **Correct**: `orphan_leads_in_window` near zero across a healthy
  `orphan_leads_days_measured`. `/session` writes are landing, so `step1_rate` is
  trustworthy.
- **Broken**: a material `orphan_leads_in_window`. `saveSession()` is
  fire-and-forget in both form files, so leads are arriving with no session row and
  the funnel's step-1 rate is inflated. Read the two orphan numbers together —
  "0 over 1 measured day of 30" is not the same statement as "0 over 30", which is
  why the route reports both.

Note this route buckets days in **UTC** (`index.js:1396, 1413`) while the Overview
chart buckets in IST, so don't expect its per-day rows to line up with the chart.

**B2 · item 8, cross-check · confirm the LM cap from the outside**

```bash
curl -s "https://gushwork-api-production.up.railway.app/monitor/lm-leads?token=$MONITOR_TOKEN&days=30" \
  | python3 -c 'import json,sys; print(len(json.load(sys.stdin)["leads"]))'
```

- **Correct**: a number below 500.
- **Broken**: exactly `500`. The route has no `total` in its response
  (`lead-magnet.js:576`), so a returned length of exactly the limit is the only
  signal available that truncation is happening — which is precisely why the pill
  counts can't be trusted.

You can also compare `/monitor/lm-loops-health` (never rendered anywhere) against
the LM tab's per-row Loops state:

```bash
curl -s "https://gushwork-api-production.up.railway.app/monitor/lm-loops-health?token=$MONITOR_TOKEN"
```

Its `pending` count excludes internal rows but its `sent` count doesn't
(`lead-magnet.js:610-612`), so treat the two as separate numbers rather than a
ratio.

### Group C — neither: Railway logs and env

**C1 · item 13 · Has the ungated ELV endpoint been probed?** (rank 12)

Not a query. In the Railway dashboard, filter HTTP logs for path
`/monitor/elv-health` and look for requests with no `token` query parameter, and
for source IPs outside your team's.

- **Correct**: only your own dashboard's requests. `/monitor/elv-health`
  (`index.js:3089`) is missing the token check every sibling route has, but nobody
  has found it.
- **Broken**: unfamiliar sources. Still no lead data exposed — the payload is
  `state`, `rate`, `checks`, `cacheSize`, `lastStatus`, `lastCheckAt` — so this is
  an information-disclosure footnote, not an incident. You'd add the token check
  either way; this only tells you whether it's urgent.

**C2 · supporting check for [§4](#4-cross-cutting-timezones) · confirm nothing overrides the session TZ**

`SHOW TimeZone` already settled the value. The one remaining way it could differ
per-connection is a timezone smuggled into the connection string, so check the
Railway variable itself:

```
DATABASE_URL — does it contain "options=" or "timezone=" ?
```

- **Correct**: neither substring present. `Etc/UTC` applies to every pooled
  connection `db.js:2-5` opens, and §4 holds unconditionally.
- **Broken**: an `options=-c timezone=…` fragment. Then the app's connections use
  that zone while the console you ran `SHOW TimeZone` in uses the server default,
  and the boundaries would need recomputing from the fragment's value.

---

### What none of these can tell you

The seven fake health checks in [§10](#10-system-health-tab) are not on this list,
because no query settles them — they are wrong by construction. `badge("s-partial",
…, "bg")` passes a literal green class; there is no data value that makes it report
a problem. Same for `!!awsPool` as a proxy for AWS sync health. Those need a code
change, not a check.

---

## Summary of everything I'd change, in order

1. Make the seven fake health checks either real or clearly labelled as counters
   (`index.js:2300`, `1990-1997`).
2. Fix the "This is exactly the SDR List" tooltip and alert, or add
   `completed = true` to `/monitor/sdr` — one or the other, not both
   (`index.js:1922`, `2075`, `1783-1791`).
3. Relabel the Overview chart to "Leads per day" or repoint it at `form_sessions`
   (`index.js:1932`, `1199-1207`). Same for the "No new sessions" alert.
4. Make the day boundaries agree. Now that the session timezone is confirmed
   `Etc/UTC`, the All Leads date filter breaks days at 05:30 IST while the chart
   above it breaks them at 00:00 IST, so "Today" provably drops leads the table
   labels as today. Either add `AT TIME ZONE 'Asia/Kolkata'` to the date filter
   and the funnel/LM buckets (`index.js:1621-1622, 1396, 1413`,
   `lead-magnet.js:514-517`), or pin the session timezone once at connection
   (`db.js:2-5`). Also switch the presets off the viewer's local clock
   (`index.js:2135`).
5. `COALESCE(booked_at, created_at)` in the Pending recovery query
   (`index.js:1228`) to match `index.js:1193`.
6. Reconcile the stage filters with `stageBadge`, or make the filters mutually
   exclusive (`index.js:1592-1595`, `2078`).
7. Return a total from `/monitor/lm-leads` and show "N of M" (`lead-magnet.js:544`,
   `index.js:2222`).
8. Pass the search term to the SDR CSV export (`index.js:2354`).
9. `GROUP BY LOWER(l.email)` alone in Duplicates, and add
   `COUNT(DISTINCT booking_uid)` so the tab can detect the known open bug
   (`index.js:1546`, `1528`).
10. Add an index on `LOWER(email)` for `leads`.
11. Token-gate `/monitor/elv-health` (`index.js:3089`).
12. Either wire up `/monitor/funnel` and `/monitor/lm-loops-health` or note in
    `CLAUDE.md` that they're deliberately curl-only.
13. Move `/monitor/website-recheck?apply=1` to POST (`index.js:3831`).

Items 6 and 9 touch behaviour `CLAUDE.md` flags as deliberate or deferred — the
duplicate-booking guard is the owner's known open bug, so item 9 is a
*visibility* change only and does not alter the guard. **Nothing here affects
which leads get blocked or which fire Meta CAPI events.** No file in this repo was
modified other than the creation of this document.
