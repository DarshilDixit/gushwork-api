# `completed` is not "submitted a form", and the mirror disagrees about it

**Status:** the live half is CLOSED — the 14 mirror rows were re-synced on
5 Sept 2026 and the two code defects behind them are fixed. What remains is
documentation-grade: correcting CLAUDE.md's safety-net caveat, and a decision
nobody needs to make today about renaming `gw_form_leads.submitted_at`.
**Found:** 5 Sept 2026, while sizing the missing-Salesforce-Lead gap. The
investigator (Claude) used `completed IS TRUE` as the test for "a Salesforce
Lead is owed", got one false positive, and only caught it by inspecting the row.
**Severity:** low today — the affected population is 6 rows — but the *shape*
is the same as the open `disqualified` inconsistency, and one of the two fixes
touches a recovery tool that writes to Salesforce.

## The two facts

**1. `completed = true` does not mean the visitor submitted the form.**
CLAUDE.md's Definitions section says so, and says the Cal and RevenueHero
safety-net branches create such rows "for someone who booked without ever
touching the form". Confirmed on Railway:

```
aasnj@meta.com   completed=true   submitted_at=NULL   booking_uid=1975833
```

**CORRECTED IN CLAUDE.md, 5 Sept 2026** — and the diagnosis was subtler than
"the doc is wrong". All seven branches that write either column were read:

| Branch | `completed` | `submitted_at` |
|---|---|---|
| `/partial` (~7632) | `false` on insert, absent from the conflict clause | never written |
| `/submit` (~7760) | `true` | `NOW()` |
| `/booking-confirmed` (~7869) | `true` | left alone |
| `/booking-confirmed-webhook` (~7945) | `true` | left alone |
| `/booking-confirmed-webhook` safety net (~7972) | `true` | `NOW()` |
| `/booking-confirmed-webhook-rh` (~8247) | `true` | left alone |
| `/booking-confirmed-webhook-rh` safety net (~8283) | `true` | `NOW()` |

**The old caveat was not wrong about the safety-net branches — it was
incomplete.** Those two `INSERT`s really do set `submitted_at`, verified
empirically: all 10 webhook-origin rows on the mirror have it populated.

The shape it failed to mention is the one that actually bites: the **three
booking `UPDATE`s** set `completed = true` on a pre-existing row and leave
`submitted_at` alone. That is `aasnj@meta.com` — someone who reached step 1,
dropped, and booked through a link later. All 6 rows with
`completed = true AND submitted_at IS NULL` are this shape, all 6 booked, all 6
form traffic (`prefill_source` null or `url_param`), and none of them is a
safety-net insert.

So a reader who took the caveat at face value would conclude that `completed`
implies `submitted_at` — because the only exception the doc named is one where
both are set. That inference is exactly what produced the false positive in
this ticket's own investigation. The corrected caveat is a table of all seven
branches, so there is nothing left to infer.

Also corrected while there: the neighbouring "Only 9 rows today" figure for
webhook-origin leads is now 10, and **all 10 are `rh_webhook`** — the Cal
safety net has never fired.

Population, from the AWS mirror (a floor — see fact 2): **6 rows, 6 people, all
6 with a booking**, spread 12 Apr – 17 Aug 2026.

**2. The AWS mirror disagrees with Railway about `completed`.**
14 people on the mirror have `submitted_at` set and `completed` **false**.
Six of six sampled have `completed = true` on Railway:

```
abhisheklokapur@gmail.com     mirror false / railway TRUE
techadmin@langslide.ai        mirror false / railway TRUE
toden@mcgrawrealtors.com      mirror false / railway TRUE
mark.mathai@kellypartners.com mirror false / railway TRUE
harsha@abc.com                mirror false / railway TRUE
pratyush@maino.ai             mirror false / railway TRUE
```

So this is **mirror drift, not a Railway fact**. It matters because the
sdr-calling dialer reads `gw_form_leads`: 14 people who completed the form look
like step-1 drop-offs to it.

Two candidate causes, both already documented in CLAUDE.md and neither
confirmed for this column:

- `syncToAWS` is fire-and-forget with no retry (known gap 2 in
  `partnerstack.md`), so a failed write leaves the mirror stale forever.
- More likely, and worth checking first: the upsert's conflict clause. For
  `disqualified` it is `= EXCLUDED.disqualified` with **no COALESCE**, which is
  why a partial object clears a real flag and why `syncBookingToAWS`,
  `syncPartnerIdentityToAWS` and `syncHearAboutUsToAWS` exist. `completed` is
  written as `completed = (COALESCE(gw_form_leads.completed, false) OR
  COALESCE(EXCLUDED.completed, false))` at `index.js:342` — an OR, which can
  only ever turn it *on*. So a partial object cannot clear it, and that points
  away from the conflict clause and back at a missed or out-of-order write.
  **Check the ordering:** if a `/partial` sync lands after a `/submit` sync, the
  OR still protects it. If the row was never re-synced after `/submit`, it
  would be stale exactly like this.

## What the audit actually found — the blast radius is small

Every read of `completed` in `index.js` was checked. **Nothing outward-facing
is currently wrong**, and that is worth stating plainly rather than leaving the
impression of a widespread bug:

| Site | Reads `completed` as | Verdict |
|---|---|---|
| Stage ladder (`index.js` ~2449) | a stage, `IS TRUE` / `IS NOT TRUE` | **correct**, and the reference implementation |
| Recovery cron (~8005) | does **not** filter on it | **correct** — selects it only to pass to Slack |
| Slack label (~1119) | "Completed Form — Did Not Book" | technically wrong for safety-net rows, but **unreachable**: the cron requires `booking_uid IS NULL` and all 6 such rows have a booking |
| Dashboard "Completed" counts (~1911) | `completed = true` | **knowingly** includes safety-net rows; CLAUDE.md documents it |
| Booking health (~1583) | completed as the denominator | safety-net rows inflate numerator and denominator alike; ratio unaffected |
| `backfill-sf.js` (~85) | "submitted a form" | **WRONG, and it has a consequence** — see below |

### The one fix with teeth: `backfill-sf.js`

```sql
SELECT * FROM leads WHERE completed = true AND <ts> >= $1 AND <ts> <= $2
```

Two problems in one line:

- **Wrong predicate.** The file's own header says it "replays completed leads",
  but its purpose is replaying *form submissions* into Salesforce. As written it
  will also create Salesforce Leads for booking-webhook rows where nobody filled
  the form. That may even be desirable — they booked, an AE should see them —
  but it should be a decision, not a side effect of the wrong column.
- **`= true`, not `IS TRUE`.** Same shape as the open `disqualified` ticket. The
  mirror has zero NULL `completed` today so nothing is lost right now; that is
  luck, not a guarantee, and `DEFAULT FALSE` makes it unlikely, not impossible.

## Suggested fixes

1. **DONE 5 Sept** — `backfill-sf.js` selects on `submitted_at IS NOT NULL`
   instead of `completed = true`, so it no longer replays booking-webhook rows
   as though they were form submissions.
2. **DONE 5 Sept** — `runBackfill` takes an `emails` allow-list, so it can
   target named rows instead of a whole window. Without it, backfilling six
   leads meant replaying every completed lead in the window — ~2,180 people for
   a mid-June start. Shipped **separately from any run**: the six leads it was
   written for were deliberately not backfilled, and
   `apollo-enrichment-not-reaching-salesforce.md` records why.
3. **The 14 drift rows are residue from a bug that was already fixed on
   18 July 2026** — diagnosed 5 Sept, see the section below. They need a
   one-off backfill, not a code fix. Nothing is drifting today.
4. **Correct CLAUDE.md's caveat** about whether the safety-net branches set
   `submitted_at`. They do not always.

## Related

- `disqualified` read inconsistently across six sites — gap 3 in
  `../partnerstack.md`. Same class: a flag read with `= true` in some places and
  `IS TRUE` in others, plus a flag read as a proxy for something it does not
  mean. Deliberately kept as separate tickets so a review is about one thing.
- The lesson from the same evening, in `../partnerstack.md`: a denominator is a
  population, not whatever a `LIMIT` returned. This ticket is its sibling — a
  *predicate* is a definition, not the nearest available column.

---

## The mirror drift: diagnosed 5 Sept 2026

**Cause: a `COALESCE` against a value that can never be NULL, which makes it a
no-op. Already fixed; the 14 rows are residue.**

Before commit `fb09128` (18 July 2026, 16:30 IST) the `syncToAWS` conflict
clause read:

```sql
completed = COALESCE(EXCLUDED.completed, gw_form_leads.completed)
```

and the value bound into it is, at `index.js:385`:

```js
data.completed || false,          // completed      — NEVER null
data.completed ? new Date() : null,  // submitted_at — null on a partial
```

`EXCLUDED.completed` is therefore always `true` or `false` and never NULL, so
the `COALESCE` never falls through to the stored value. **The incoming value
always won.** A `/partial` sync landing after a `/submit` sync — a visitor who
submits and then goes back and edits step 1 — wrote `completed = false` over a
completed row.

`submitted_at` survived the same statement because its bound value is
*genuinely* null on a partial, so its `COALESCE` worked exactly as intended.
**That asymmetry is the drift signature**: `submitted_at` set, `completed`
false, in a row where both are written from the same field.

It was fixed on 18 July by replacing the `COALESCE` with an OR, which can only
ever turn the flag on:

```sql
completed = (COALESCE(gw_form_leads.completed, false) OR COALESCE(EXCLUDED.completed, false))
```

### The evidence

| | |
|---|---|
| Drift rows created **before** the 18 Jul fix | **14 of 14** |
| Drift rows created **after** it | **0** |
| Submitted rows created since the fix, for scale | 1,315 |
| Oldest / newest drift row | 9 Apr / 13 Jul 2026 |

So it was **not** a missed sync and **not** out-of-order writes in the sense of
a lost message. The write arrived and did the wrong thing, deterministically,
and the ordering that triggered it (a partial after a submit) is ordinary
visitor behaviour.

### The sibling check — every never-NULL bind in the same statement

Exactly four columns bind a value that can never be NULL. The audit:

| Column | Bound value | Conflict clause | Verdict |
|---|---|---|---|
| `completed` | `\|\| false` | `(old OR new)` | **fixed** 18 Jul; residue above |
| `loops_sent` | `\|\| false` | `COALESCE(EXCLUDED, old)` | **same latent bug, not firing** |
| `disqualified` | `?? false` | `EXCLUDED.disqualified` | known; CLAUDE.md documents it |
| `step_reached` | `\|\| 1` | `GREATEST(...)` | safe, monotonic |

Every other column binds `|| null`, so its `COALESCE` behaves as written.

**`loops_sent` carries the identical defect and is not currently firing.** Its
only writer is a *targeted* `UPDATE gw_form_leads SET loops_sent = true` in the
recovery cron (`index.js` ~8042/8057/8101) — correct per CLAUDE.md's rule for a
late-arriving single field — and it runs at least two hours after the form
session, by which point nothing calls `syncToAWS` for that session again.
Verified empirically: Railway reports 79 follow-ups processed in the last 7
days and the mirror holds exactly 79. It would bite the moment any path ran a
full `syncToAWS` for a session after Loops had fired. Worth fixing to an OR
while someone is in there, but it is not an incident.

### A separate trap found on the way, worth knowing

**`gw_form_leads.submitted_at` is not the lead's submission time.** It is
`new Date()` at sync time, written only when `data.completed` is truthy. Its
*presence* is a reliable "this row was synced while completed", which is what
this ticket and the held-vs-sent join use it for. Its *value* is a sync
timestamp, and any analysis treating it as when the form was submitted is
reading the wrong clock. Railway's `leads.submitted_at` is the real one.

### Recommended, not done

- **DONE 5 Sept 2026 — the 14 rows were re-synced.** Approved explicitly
  before running. `UPDATE gw_form_leads SET completed = true, updated_at =
  NOW()` over the 14 known `session_id`s, parameterised as
  `= ANY($1::text[])`, doubly guarded by `submitted_at IS NOT NULL AND
  completed IS NOT TRUE` so it was idempotent and could not touch a row
  outside the drift state even if an id had been wrong.

  Verified per row before running: 14 of 14 were `completed = true` on
  Railway, checked individually rather than inferred from a sample. Dry run
  matched exactly 14 rows, and 0 rows outside the id list.

  Result: `UPDATE 14`, mirror-wide drift predicate went 14 → 0, and an
  immediate re-run returned `UPDATE 0`, proving idempotence rather than
  asserting it. Nothing but `completed` moved — `submitted_at` intact on all
  14, `disqualified` still 4, `booking_uid` still 5. The 6 rows with
  `completed = true AND submitted_at IS NULL` are the safety-net webhook rows
  and are correct; they were untouched.

  Safe to repeat if it ever recurs, though it cannot: the cause was fixed on
  18 July 2026 and `loops_sent`, its last remaining sibling, on 5 Sept.
- **DONE 5 Sept** — `loops_sent` changed to the same OR form, so the latent
  version cannot wake up. `tests/test-batch2.js` now derives the never-NULL
  column list from the bind site and asserts structurally that none of them is
  guarded by a no-op `COALESCE`, so a future column added with the same shape
  fails a test.
- **DONE 5 Sept** — the `submitted_at` semantics are documented in
  `../partnerstack.md` under the mirror traps, at the bind site, at the DDL,
  and as a `COMMENT ON COLUMN` on the table itself so a SQL client shows it.

### Renaming `gw_form_leads.submitted_at` — what it would cost

Assessed 5 Sept, **not done**. The blockers are all outside this repo.

**Inside the repo it is trivial** — three sites, all in `syncToAWS`: the
`ADD COLUMN` migration, the INSERT column list, and the conflict clause.
Nothing in this repo ever reads the column back off the mirror; it is
write-only from here.

**In the warehouse it is nearly free too.** Exactly one object depends on
`gw_form_leads` — the view `gist.v_meta_ad_bookings_classified` — and it does
**not** reference `submitted_at`. A `RENAME COLUMN` in Postgres is a metadata
operation: instant, no table rewrite, no lock of consequence at this size.

**The cost is entirely the unenumerable readers.** `gw_form_leads` is read by
the sdr-calling dialer, which lives outside this repo and is not versioned
here, plus any saved BI queries, dashboards or spreadsheets nobody can list
from a SQL prompt. A rename breaks each of them **at once and silently** — a
`SELECT submitted_at` starts erroring rather than degrading.

So the honest options, cheapest first:

1. **`COMMENT ON COLUMN`** — done. Discoverable in any SQL client or BI tool,
   zero risk, zero coordination.
2. **Add, dual-write, deprecate.** Add `synced_completed_at`, write both,
   backfill `synced_completed_at = submitted_at`, tell the dialer owner, and
   drop `submitted_at` after a window. Costs one migration, one deploy and one
   conversation; breaks nothing at any point.
3. **Straight `RENAME COLUMN`.** One statement, instant — and it needs the
   dialer owner to ship a change in the same window, which is the whole cost.
   Not worth it for a column whose presence is what everyone actually uses.

Recommendation: option 1 is already in, and option 2 only if someone finds a
consumer genuinely misreading the value.
