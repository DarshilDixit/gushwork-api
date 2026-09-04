# `completed` is not "submitted a form", and the mirror disagrees about it

**Status:** open, audited, two small fixes identified
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

**CLAUDE.md is slightly wrong about this row's shape.** It says those branches
create rows with "`submitted_at` already set". This one has `submitted_at =
NULL`, so a reader who trusts the caveat and keys on `submitted_at` expecting
it to be populated gets a different answer than one who keys on `completed`.
Worth correcting in CLAUDE.md when someone confirms which branch produces which.

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
3. **Investigate the 14 drift rows** and re-sync them, so the dialer stops
   seeing completers as drop-offs. **This is the live half of this ticket.**
   Everything else here is correctness and clarity — the stage ladder is right,
   the cron does not read the flag, the bad Slack label is unreachable. The
   mirror drift is the only part that is outward-facing and happening now,
   because the sdr-calling dialer reads `gw_form_leads`. Under investigation as
   of 5 Sept.
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
