# Batch A — remaining work (commits 3–7)

## SESSION STATE — read this first

| | |
|---|---|
| **Branch** | `feat/monitor-batch-a` (pushed to origin as a backup; **not** merged, no PR) |
| **Last code commit** | `216687a` — funnel top stage + window toggle |
| **`main`** | untouched at `60a2ea8` |
| **Tests** | 109 + 179 + 94 passing (`test-batch1`, `test-batch2`, `test-batch-a`) |
| **Next up** | **Commit 3 — System Health rebuild.** Nine real checks; seven currently pass a hardcoded green class. The AWS mirror check is the owner's top priority and must go **red** on a connection error, never grey |
| **Before writing code** | Propose the plan and wait for approval. That is how this batch has been run |

Three commits are done: Definitions in `CLAUDE.md`, the Eastern Time migration,
and the Overview funnel top stage. Commits 3–7 are specified below.

---

Handoff document. Written to be picked up cold, with no other conversation
context.

Read `CLAUDE.md` (especially the **Definitions** section) and
`MONITOR-AUDIT.md` (the audit this batch is fixing — it is the spec) before
starting.

---

## Where things stand

Branch **`feat/monitor-batch-a`**, pushed to origin as a backup only. Three
commits ahead of `main`. No PR. `main` is untouched at `60a2ea8`.

| Commit | Subject |
|---|---|
| `fd3baa6` | `docs:` Definitions section in CLAUDE.md |
| `eb50c08` | `fix(monitor):` Eastern Time end to end |
| `216687a` | `fix(monitor):` Overview funnel top stage + window toggle |

Working tree clean. Test state at pause:

| Suite | Result | Needs DB? |
|---|---|---|
| `node tests/test-batch1.js` | 109 passed, 0 failed | no |
| `node tests/test-batch2.js` | 179 passed, 0 failed | no |
| `node tests/test-batch-a.js` | 94 passed, 0 failed | no |
| `tests/test-batch1-db.js` | not run | yes |
| `tests/test-batch1-e2e.js` | not run | yes |

**The dependency-free three are the bar.** Do not install Postgres and do not
point anything at the production database from a feature branch.

---

## The goal, restated

**Correctness only. No new features.** Every number on the monitor either
matches its label or the label changes. Nothing here may alter which leads get
blocked or which fire Meta CAPI events — if a change would, stop and flag it.

---

## Working agreement

- **Plan before writing code, then wait for approval.** This batch has been run
  as: propose → get sign-off → implement → report. Keep that.
- **Full dependency-free suite before every commit.** All three must pass.
- **Mutation-test every guard**: break the line, watch the specific assertion
  fail, restore, confirm green again. Do not claim coverage without doing this.
- **Do not push. Do not merge. Do not open a PR** unless explicitly asked.
  (The branch itself is already pushed as a backup; that was a one-off.)
- Say what changed and what did **not**. Flag every number the owner has
  already seen that will move.

---

## The two decisions already made

### Decision 1 — recovery cron: dashboard only

The dashboard's "Pending recovery" card and the recovery cron ask **two
different questions**, and they must not be unified:

- *"Is this person an SDR target?"* → no time comparison. They hold a call
  slot, don't ring them.
- *"Should this session get a drop-off recovery email?"* → the time comparison
  is **correct and required**. A booking that predates the session does not
  resolve that session's drop-off; the person came back, started again, and
  dropped again.

**`index.js:4735` (`AND booked.booked_at >= l.created_at`) is off limits.** It
is deliberate, dates to a May 2026 fix, and relaxing it to "has ever booked"
would suppress follow-ups that should be sent. No `COALESCE` either —
production has zero rows with a null `booked_at`, so there is nothing to defend
against, and adding one would re-introduce ever-booked behaviour by the back
door.

A protective comment is already in place above it, and `test-batch-a.js` group 6
asserts the comparison survives, that no `COALESCE` appears, and that the
comment still explains why. **Those tests failing means someone "tidied up" a
deliberate behaviour.**

Consequence for commit 4: relabel the Pending recovery card so it describes what
it counts instead of claiming to mirror the cron. Do not change the cron.

### Decision 2 — health alert severity split

- **`critical`** (Slack **+ email** to `ALERT_EMAIL_TO`, 3h cooldown):
  **AWS sync stale**, **`/submit` dead**, **`/partial` dead**.
  Rationale given by the owner: sessions arriving with zero leads created means
  the form is broken at the very top, which is where most volume is already lost.
- **`warning`** (Slack only, 1h cooldown): everything else.

`ALERT_COOLDOWN_MS` and the severity routing already exist in `alertOps`
(`index.js:618`) — use them, don't add a parallel path.

---

## Commit 3 — System Health rebuild (the big one)

**Problem.** Seven of nine rows are lifetime counters wearing health-check
clothing. `index.js:2513` is the whole story:

```js
badge("s-partial", d.total+" sessions saved", "bg")   // literal green, no condition
```

`/partial` reports green whether it works or has been 500ing for a week.
`/submit` is green if `completed > 0` **ever** — it went green on the first
submission and cannot go back. `AWS sync: Active` is `!!awsPool`
(`index.js:1394`), true whenever the `AWS_PG_HOST` env var is set; it never
touches the AWS database. Only `API uptime` and `ELV` verify anything live.

**Row markup:** `index.js:2140–2148`. **Badge assignment:** `index.js:2513`.

**Design.** New `/monitor/health` route running real probes, kept **off** the
60-second `/monitor/metrics` poll because the AWS query is slow. Each check
returns `green | amber | red | insufficient_data`.

| Row | Real check |
|---|---|
| API | unchanged, already real (`checkApi`, client-side) |
| Step 1 `/partial` | sessions in last 2h > 0 **and** leads in last 2h = 0 → **red** |
| Step 2 `/submit` | step-1 leads in last 24h above a floor **and** completions = 0 → **red** |
| ELV | unchanged, already real (`elvHealthSnapshot`, `index.js:2722`) |
| Apollo | enrichments in a recent window vs leads arrived, plus last-enriched age. Delete the lifetime ratio |
| Booking | recent-window booking rate + last-booking age. Delete the lifetime ratio |
| Cron | `_lastCronRunAt` vs `CRON_STALE_MS` — reuse the existing heartbeat state |
| **AWS sync** | **see below** |
| Email recovery | pending > 0 **while** the cron has run recently → the cron ran and sent nothing → **red** |

**Every windowed check needs an ELV-style minimum denominator** so a quiet night
reports grey `insufficient_data` — never green, never red. `ELV_MIN_SAMPLE`
(`index.js`, ~8) is the precedent.

### The AWS check — the owner's top priority

> "It must fail red on a connection error, not degrade to grey — a mirror we
> can't reach is indistinguishable from a mirror that's stale, and both starve
> the sdr-calling dialer."

Requirements:

- Actually query `awsPool`: `count(*)` and `max(updated_at)` from
  `gw_form_leads`, compared against `leads` over the same window.
- **A connection error, timeout or auth failure is RED.** Not grey, not
  "unknown". This row must be *structurally incapable* of returning
  `insufficient_data` — assert that in a test so it cannot be softened later.
- Lag beyond threshold is RED.
- `AWS_PG_HOST` unset is a separate, honest state ("disabled"), not green.

This is not a contradiction of the project's fail-open rule. `CLAUDE.md`'s
**"Health checks fail LOUD"** section already states the inversion: fail-open
governs the lead path, no lead depends on a health probe, and a probe that
cannot verify must not report green. The AWS table schema is at
`index.js:128`+ (`initAWSTable`).

### Alerting — reuse, don't rebuild

`runHealthChecks()` is called from the existing **`startHeartbeat()`**
(`index.js:873`, 30-minute cadence) so failures notify with the tab closed.

State transitions mirror the ELV enter/exit pattern **exactly** — read
`index.js:2779` (enter degraded) and `index.js:2805` (exit degraded) first:

- a `_healthState` Map keyed by check id
- `alertOps(severity, …)` on **green→red only** (fires once, not per poll)
- a `sendOpsSlack` recovery message on **red→green**

`alertOps` already carries per-key cooldown and suppression counting. Do not add
a second alerting system.

Also fix the alerts panel at `index.js:2238`: `"All systems healthy."` currently
renders green with a tick whenever five specific conditions are quiet. It is not
a statement about system health.

**Tests:** every row can reach red; the AWS row cannot return grey; a green→red
transition alerts exactly once and a second poll in the same state does not;
red→green sends recovery; severity split matches Decision 2.

---

## Commit 4 — stage ladder + booking definitions

**Stage ladder.** `CLAUDE.md` Definitions is the spec. Four stages, mutually
exclusive and exhaustive, priority-ordered:

1. **Booked** — `booking_uid IS NOT NULL`
2. **Disqualified** — not booked, `disqualified IS TRUE`
3. **Completed** — not booked, not disqualified, `completed IS TRUE`
4. **Step 1** — everything else

Use `IS TRUE` / `IS NOT TRUE`, never `= true` / `= false`, so a null flag on an
old row lands in a stage instead of vanishing from all four.

- Filters: `index.js:1728`+ — rewrite onto the ladder.
- Badge: `index.js:2291` — already matches this order, leave it.
- **Required test:** the four predicates partition a generated matrix of all 16
  flag combinations *including nulls*; the four counts sum to the row count for
  every combination.

**Booking definitions.** One shared SQL fragment for "has any booking" (no time
comparison), used by the two sites that ask question 1:

- "No booking yet (SDR)" — `index.js:1290`
- the SDR route — `index.js:1888`

**"Pending recovery" is NOT one of them.** An earlier draft of this list
included it and predicted its number would drop, which contradicted both
Decision 1 above and the Definitions section of `CLAUDE.md`. Settled with the
owner, 25 Aug 2026: the card's job is to size the queue the recovery cron will
actually send to, so it must count the cron's population — question 2, with the
time comparison. It keeps its query and gets a truthful label. **Its number does
not move.**

"Recovered bookings" (`index.js:1222`) keeps `COALESCE(booked_at, created_at) >=
l.created_at` as the documented exception — it is definitionally about ordering.

**Numbers that move:** the "Disqualified" filter drops (now excludes booked).
"Completed" drops (now excludes disqualified). "Step 1 only" may rise
(`IS NOT TRUE` includes rows `= false` excluded, and rows with a null flag that
were previously in no stage at all). Relabel the Pending recovery card per
Decision 1.

---

## Commit 5 — tooltip, LM totals, SDR CSV search

**Tooltip** (`index.js:2069`) asserts *"This is exactly the SDR List."* It is
false: the Overview number filters `completed = true`, the SDR route
(`index.js:1888`) has **no completed filter**, so SDR List is a strict superset.
Fix the tooltip and the same claim in the alert prose at `index.js:2238`.

**LM true totals, not pagination.** `lead-magnet.js:556` is `LIMIT 500` and the
dashboard never sends a `limit`, so it is always 500. The status pill counts are
computed client-side over the loaded 500 and read as totals. Add a counts query
to `/monitor/lm-metrics`, have the pills read from it, and show "showing N of M".
This is the clearest silently-wrong number left on the dashboard.

**SDR CSV ignores the search box.** `exportSDR` (`index.js:2563`) sends only
`format=csv`. Add a server-side `search` param to `/monitor/sdr` matching the
same four fields as the client filter (`email`, `company`, `first_name`,
`enriched_industry`) and pass it. Keep the table's search client-side — the
query is unbounded and per-keystroke round trips would be worse. **Add a test
that lifts both field lists and asserts they match**, so the two cannot drift.

---

## Commit 6 — route hardening

- **`/monitor/website-recheck` → POST** (`index.js:4044`). Currently a GET with
  `?apply=1` that writes lead rows and runs two `ALTER TABLE`s — a link
  prefetch or a chat client unfurling the URL could trigger it. Update the doc
  block above the route too. Not fetched by any UI, so no client change.
- **Token-gate `/monitor/elv-health`** (`index.js:3302`) — the only `/monitor*`
  route with no token check. Also update `checkElv()` in the dashboard JS to
  send the token.

---

## Commit 7 — final pass

Re-run everything, re-read the diff end to end, and update `MONITOR-AUDIT.md`
to mark what Batch A fixed and what remains. Report every number that moved.

---

## Explicitly out of scope, and why

| Item | Why |
|---|---|
| **The double-booking fix** | `CLAUDE.md` records it as a known open bug, deliberately deferred by the owner. Batch A may improve its *visibility* (e.g. `COUNT(DISTINCT booking_uid)` in Duplicates) but must not change the guard |
| **Webhook-lead propagation to `/monitor/metrics`** | Only 9 rows; negligible distortion. `/monitor/funnel` already excludes them and documents why |
| **The recovery cron (`index.js:4735`)** | Decision 1. Deliberate, dates to May 2026, protected by comment and tests |
| **Excluding internal / test addresses** | Documented in Definitions as a known distortion, *not* a decision anyone made. Excluding them moves every historical number at once — needs its own decision, not a side effect |
| **Repointing the Overview chart at `form_sessions`** | Owner's call: it stays a row count over `leads`, relabelled "Form entries per day (ET)". Proper session charting is **Batch B** |
| **`index.js:725`** | A comment recording a real incident at 17:23 IST. Converting it would falsify the record. Leave as IST |

---

## Test conventions

New assertions go in **`tests/test-batch-a.js`**.

**Lift real code out of the source; never copy it.** A test that exercises a
duplicate can pass while production is broken. Two helpers already in the file:

- `between(s, startMarker, endMarker)` — slice source between two literals.
- `liftClientJs(startMarker, endMarker)` — the dashboard ships its JS as a run
  of single-quoted literals joined with `+`. This unquotes each line by letting
  JS itself evaluate the literal, so the test runs **exactly** what the browser
  runs, escapes and all. Used to execute `fRow` / `renderFunnel` against a DOM
  stub. Reuse it for the health-badge renderer.

Then `new Function(lifted + 'return { … };')()` to get real callables.

For SQL, assert by **shape** — which timezone expression or predicate is present
at which site. The failure mode that actually ships is a boundary or a flag
resolving wrongly, and that is visible in the text.

Existing groups in the file, for orientation:

1. ET helpers, executed against real EDT/EST instants and the DST gap
2. Which SQL sites bucket in ET — and which must **not** (`/monitor/funnel`)
3. Client formatting reads ET, never the laptop clock
4. Slack / email / filename stamps
5. Overview funnel: ladder order, both windows, not-tracked handling
6. **Guard group** — the recovery cron is not part of this batch

Group 6 is a *guard*, not a feature test. Keep that distinction for anything
protecting deliberate behaviour.

---

## Traps already hit — don't repeat them

**Backticks inside SQL comments break the file.** The SQL lives in JS template
literals and the house style is a long `/* … */` comment inside the query. A
backtick in that comment — writing `` `leads` `` or quoting an expression —
**terminates the literal**, and the error surfaces as `SyntaxError: missing )
after argument list` pointing at the `pool.query(` line, not the comment. This
cost four syntax errors in one sitting. Now documented in `CLAUDE.md` under
"Things that will bite you". **Run `node --check index.js` before every commit.**

**Don't collapse a decorative label onto a missing-value message.** In
`fRow`, reusing the `note` parameter for the null-row text made "Entered step 1"
render its decoration (`· visits → people`) as its **value** when coverage was
missing — which reads as a real measurement. Fixed by separating `note` from
`nullMsg`; there is a named regression guard. The same shape will come up in the
health badges: "cannot verify" needs its own message, never a fallback that
looks like a reading.

**Mutation strings need care in shell heredocs.** Two mutation tests initially
failed to *apply* (not to catch) because `—` escaping was mangled. Verify
the mutation actually landed — the helper asserts `count == 1` — before trusting
a green or red result.

---

## Quick reference — line numbers as of `216687a`

| What | Where |
|---|---|
| `DASH_TZ`, `etDateOnly`, `etStamp` | `index.js` ~539 |
| `alertOps` | `index.js:618` |
| `startHeartbeat` | `index.js:873` |
| `/monitor/metrics` | `index.js:1157`+ |
| `completed_no_booking_sessions` | `index.js:1209` |
| Recovered bookings query | `index.js:1222` |
| Pending recovery query | `index.js:1274`–`1285` |
| No-booking-yet (SDR) query | `index.js:1290` |
| `awsSynced: !!awsPool` | `index.js:1394` |
| `BOT_RE` (module scope) | `index.js:1447` |
| Stage filters | `index.js:1728`+ |
| `/monitor/sdr` | `index.js:1888` |
| SDR tooltip (false claim) | `index.js:2069` |
| Health row markup | `index.js:2140`–`2148` |
| `renderAlerts` | `index.js:2238` |
| `stageBadge` | `index.js:2291` |
| Health badge assignment | `index.js:2513` |
| `exportSDR` | `index.js:2563` |
| `elvHealthSnapshot` | `index.js:2722` |
| ELV enter / exit degraded | `index.js:2779` / `2805` |
| `/monitor/elv-health` (ungated) | `index.js:3302` |
| `/monitor/website-recheck` (mutating GET) | `index.js:4044` |
| Recovery cron booking test — **off limits** | `index.js:4735` |
| LM `LIMIT 500` | `lead-magnet.js:556` |

Line numbers drift as soon as you edit. Grep for the identifier, don't trust the
number.
