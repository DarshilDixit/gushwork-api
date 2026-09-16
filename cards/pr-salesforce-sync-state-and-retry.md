# PR — Salesforce writes are durable: sync state + a retry sweep

Branch `feat/salesforce-sync-state-and-retry`. **Not merged.**

The follow-up to PR 82. That one made the *alert* honest; this makes the
*write* durable.

---

## The problem

A maintenance window on 16 Sept 2026 dropped `gregory.ingalls@gmail.com` — a
lead that had **booked a demo** — and the only trace was a Slack alert telling a
human to add it by hand. Nothing retried. Nothing recorded. So:

- **"Which leads are missing from Salesforce?"** had no answer at all.
- Recovery was one manual Salesforce record per lost lead.
- Ten leads instead of one and that is somebody's afternoon.

## Five columns

| column | meaning |
|---|---|
| `sf_synced_at` | when a write last **succeeded** |
| `sf_sync_failed_at` | when one last **failed** |
| `sf_sync_attempts` | what the sweep has spent |
| `sf_sync_error` | the last error, so a human can read it |
| `sf_sync_retryable` | whether that failure can ever succeed |

Two stamps rather than one nullable boolean, because a boolean cannot tell
*"never tried"* from *"tried and failed"* — and never-tried is the state every
historical row is in. Same reasoning as `ps_signup_verified_at` against
`ps_signup_recheck_at`.

### Nothing is backfilled — that is the design

Every pre-existing row has all five NULL, which reads as **"we never observed
this one"**. True, and *not* "it failed". The sweep keys off
`sf_sync_failed_at IS NOT NULL`, so it can only ever retry a write we actually
watched fail.

Inferring that history had failed would have queued **thousands** of re-pushes
on the first boot after deploy. This is the same rule the repo already applies
to `first_ticked_at`: do not put an inferred value in an observational column.

## What may be retried, and what may never be

`sfIsRetryable` decides. An outage, a 5xx, a dropped socket, an expired token —
those succeed the moment Salesforce is back, which is exactly the case that cost
a booked demo. A **converted lead** or a **rejected picklist value** fails
identically forever.

A sweep that retries terminal failures burns its budget and refills the queue
with entries no retry can clear — precisely why the *"booked but no
`ps_qualified_sent_at`"* check was rejected in the PartnerStack work.

**The default is TERMINAL.** An error nobody has classified is left for a human
rather than hammered. The lead is not lost by that: the alert still fires and
the row keeps its error text.

## The clause that would have been missed

```sql
AND non_icp_blocked IS NOT TRUE
```

A blocked lead is **deliberately** absent from Salesforce (Swapnil, 11 Sept).
This sweep is a **new consumer** of that rule, and CLAUDE.md is explicit that a
second column meaning "we rejected this lead" silently re-scopes every consumer
of the first — the lesson from the PartnerStack conversion that cost money.

**Without this clause the sweep would have pushed every realtor we turned away.**

## Shape

- **Claims the attempt before the push**, so overlapping runs cannot
  double-spend and a crash mid-push costs one attempt rather than looping.
- **Reuses `pushToSalesforce`** rather than reimplementing the write, so a
  recovered lead is byte-identical to one that succeeded first time.
- **Pages only once retries are exhausted**, not every tick — an alert per tick
  trains people to ignore it.
- **One at a time** (`_sfRetryRunning`), fire-and-forget, `unref`'d.
- Every 10 min, 5 attempts, 10 min backoff, batch of 25 — all env-overridable.

---

## Verified by execution, not by reading

**The migration was run** against a `CREATE TEMP TABLE leads (LIKE public.leads
INCLUDING ALL)` shadow and rolled back. All five columns land with the right
types, and re-running every statement is a no-op — safe on every boot.

**The sweep query was run against real Postgres** with fixtures proving each
guard excludes what it should:

```
PASS  should be picked up            expected 1, got 1
PASS  terminal (not retryable)       expected 0, got 0
PASS  non_icp_blocked                expected 0, got 0
PASS  attempts exhausted             expected 0, got 0
PASS  backoff holds a fresh failure  expected 0, got 0
PASS  never submitted is excluded    expected 0, got 0
```

**`sfIsRetryable` was executed** against nine real error shapes before a single
test was written.

**Bar green, run bare:** 12 suites, **3547 assertions** (+36).

### Mutation testing — 4 mutations, all CAUGHT

| # | Mutation | Result |
|---|---|---|
| 1 | The sweep stops excluding non-ICP blocked leads | CAUGHT |
| 2 | Terminal failures get retried after all | CAUGHT |
| 3 | A converted lead becomes retryable | CAUGHT |
| 4 | Unknown errors default to retryable instead of terminal | CAUGHT |

---

## What this does NOT change

- **No lead is blocked or unblocked**, and the non-ICP rule is strengthened
  rather than touched.
- **No Meta event changes.**
- **Nothing on the request path waits.** Both marking helpers are
  fire-and-forget and swallow their own errors — a failure to *record* a
  failure must not become a second failure.
- **The first failure does not spend a retry**, so the budget means what it says.

## Not done / open

1. **Only the `/submit` push is marked.** The three booking-webhook pushes and
   the booking `updateSFLead` calls still alert without recording state. Those
   are the "lead exists, booking missing" case rather than "lead missing
   entirely", so the sweep's population is the important one — but it is a
   deliberate first cut, not completeness.
2. **No dashboard surface.** The state is queryable and the exhausted case
   pages, but there is no "leads missing from Salesforce" card yet. Worth adding
   once there is real data in the columns.
3. **Leads that failed BEFORE this ships are not covered**, by design — the
   columns are not backfilled. `gregory.ingalls@gmail.com` was recovered by hand
   already.

## Deploy

Server-side only. `git push` to `main` → Railway, and `initDB()` runs the
migration on boot. **No Webflow step.**
