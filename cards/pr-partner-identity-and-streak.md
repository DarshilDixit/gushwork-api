# PR 76 — Partner identity on the dashboard, and a streak that resets

Branch `fix/partner-identity-and-streak`. Three commits. **Not merged.**

Started from two screenshots and one Slack alert. Everything below was confirmed
against production data before it was changed, and the fix was driven against
that same data afterwards.

---

## What was wrong

### 1. The per-domain row described a partner who does not exist

`partnerLifecycle` read partner identity as three **independent** `MAX()`
aggregates over a group keyed by `ps_customer_key`. A domain can carry leads
from two partners; three independent aggregates then resolve independently.

Run against production on 16 Sept 2026:

```
domain        | max_key         | max_name     | max_email
allstate.com  | cd34e586561e    | Test Account | partners@reviews.guide
```

`'7' < 'c'` so the key came back Reviews Guide's; `'R' < 'T'` so the name came
back the other partner's; `'g' < 'p'` so the email came back Reviews Guide's.
The dashboard printed **"Test Account"** over Reviews Guide's key and email.

The contradiction was on screen already: the per-partner table showed Reviews
Guide with 1 lead, while the per-domain table showed it owning no domains.

### 2. `abc.com` rendered a raw hex key that was already resolved

Identity was read off *that domain's own rows*. `785ec78e1ee4688` resolves to
"Test Account" on other rows — and in the table directly below it on the same
screenshot. Identity belongs to the KEY.

### 3. The root cause under both: step 1 never wrote the columns

`runPartnerStackIdentity` was called from `/submit` and nowhere else.

```
submitted | name resolved
t (6)     | t — all 6
f (4)     | f — all 4
```

A clean split across every partner lead in the table. `/partial` **already
resolved the name** (the `partnerIdentityNoNetwork` peek) and spent it on
`hear_about_us`, then dropped it — so the same row read `Partner - Test Account`
with a null `ps_partner_name` beside it.

This is what Darshil hit with `darshildixit21@gmail.com` on 16 Sept: a step-1
lead, so no name, so the dashboard showed hex.

**Swapnil's allstate case in Slack is a different and already-documented one** —
the first ever lead from a brand-new partner, where memory and the database both
legitimately miss and the post goes out with the key before the API answers.
CLAUDE.md records it for `cd34e586561e`. Not changed here.

### 4. The streak counter

`recordSuccess('PartnerStack')` was called nowhere. The only resets of
`_failStreaks` are `recordSuccess` and an alert actually sending, so
"N consecutive failures" meant "N failures since the last alert, ever".

The Railway log for the alert window:

```
09:18 FAIL  09:20 FAIL  09:22 OK  09:24 OK  09:26 OK  09:28 OK
09:30 OK    09:32 OK    09:34 FAIL ← alert fired here, claiming "8 in a row"
```

About ten failures in thirty-five reads, **never more than two consecutive**.
Salesforce was in a maintenance window, serving its own
"We are down for maintenance" HTML with a 503.

The alert also carried the money-path impact line — *"nothing retries a
conversion whose attempts are exhausted"* — for a failure that fully self-heals
two minutes later.

### 5. `escq` (found while fixing the above)

Every HTML attribute in the dashboard is single-quoted. `esc` escapes
`& < > "` but **not** `'`. Two attribute values carry text a human can put an
apostrophe in: the acknowledgement note (typed into a `prompt()`) and a partner
display name (PartnerStack `first_name` + `last_name`). The ack note case is
pre-existing.

---

## What changed

| Area | Change |
|---|---|
| `partnerLifecycle` | One authoritative `partner_key` per domain; name/email derived from it; all claiming partners carried |
| `partnerRevenueGaps` | Same fix on check B; check A inherits from the ladder |
| Per-domain renderer | `partner_display`, plus a red chip naming the other partner(s) |
| Gaps renderer | Now uses the full name → email → key chain (it skipped email) |
| `/partial` upsert | Writes `ps_partner_name` / `ps_partner_email` from the peek it already had |
| `/partial` deferred | Calls the API resolver after `res.json()` when the peek missed |
| `/submit` upsert | Same two columns, so a failed deferred resolve no longer leaves them null |
| `FAILURE_MONITORS` | New `'PartnerStack SF read'` source with its own impact text |
| `recordSuccess` | Called on a landed conversion, a landed qualification, and a good read |
| Dashboard | `escq`, applied to the contention tooltip and the ack note |

**Order of the authoritative pick is the money order:** `ps_signup_sent_at`
first (that partner was actually credited), then earliest `created_at` (that
partner would win the claim next). Ordering by recency alone would name the
wrong partner for any domain whose first lead was not the one that converted.

---

## Verified

**Bar, run bare (never piped):** 12 suites, **3405 assertions, 0 failures**.
Baseline moved 3357 → 3405 and was re-saved.

**SQL was EXECUTED, not read.** Both rewritten upserts ran against a
`CREATE TEMP TABLE leads (LIKE public.leads INCLUDING ALL)` shadow inside a
transaction, then `ROLLBACK` — production untouched. Both rewritten SELECTs ran
against production read-only. Column/placeholder arity was also checked
programmatically: `/partial` 51 columns / 51 slots / `$1..$49` contiguous;
`/submit` 52 / 52 / `$1..$48`.

**The fix driven against real production data** (lifting the real
`partnerLifecycle` against the live pool, read-only):

```
abc.com       shows "Test Account"   key=785ec78e1ee4688  email=growth@gushwork.ai
allstate.com  shows "Test Account"   key=785ec78e1ee4688  email=growth@gushwork.ai
              ⚠ ALSO CLAIMED BY: Reviews Guide
```

Both defects gone; contention now stated.

**`country.is` checked live** (for the separate PR): returns
`{"ip":"…","country":"IN"}` in 300ms with `access-control-allow-origin: *`.
The live pages serve `96360cc`, whose `gushwork-form.js` is byte-identical to
`main`. The API is fine; the wiring is not.

### Mutation testing — 10 mutations

Committed first, then mutated, measured with `measure.js --mutation`.

| # | Mutation | Result |
|---|---|---|
| 1 | `recordSuccess` removed from the SF read (the original bug) | CAUGHT |
| 2 | Back to three independent `MAX()`es | **survived → fixed → CAUGHT** |
| 3 | Identity resolved off the domain's rows, not the key | CAUGHT |
| 4 | Contention silently dropped | CAUGHT |
| 5 | `/partial` binds nulls instead of the resolved identity | CAUGHT |
| 6 | `recordSuccess` removed from the conversion send | CAUGHT |
| 7 | Read failure filed back under the money-path source | see below |
| 8 | `if (false)` around the deferred step-1 resolver | **survived → fixed → CAUGHT** |
| 9 | `escq` stops escaping the apostrophe | CAUGHT |
| 10 | Sticky `COALESCE` removed, so a later tick can blank a name | CAUGHT |

**Two survived on the first pass**, and both are the reason the two test commits
exist:

- **#2** survived because the executed assertions drive a fake pool that returns
  the same rows whatever the SQL says — a fixture cannot see a query. Fixed with
  query-level assertions, including that the pick prefers the conversion holder.
- **#8** survived because an `if (false)` binds the same nulls, answers the same
  200 and rejects nothing. Fixed by giving the suite v2 credentials so
  `fetchPartnership` actually calls out and the stubbed fetch can see it — plus
  the negative, that an already-resolved partner must **not** call the API.

**#7 is UNMEASURED by the rule, and is reported as that rather than as a catch.**
`test-partnerstack` completes and fails 2 assertions, which is a real catch. But
`test-batch2` derives its assertion count from the number of distinct
`recordFailure` sources, so removing a source moves its total 746 → 745 and
`measure.js` correctly refuses to call the run measured. Recorded honestly.

---

## What this does NOT change

- **No lead is blocked or unblocked.** Nothing here touches `non_icp_blocked`,
  `disqualified`, or any blocking path.
- **No Meta event changes.** No event fires, stops firing, or changes payload.
- **No conversion or qualification fires differently.** `recordSuccess` only
  lowers an alerting counter; the identity work writes two display columns.
- **Alerting gets quieter, never silent.** The streak resets on success, but the
  3-in-6-hours window path is deliberately not reset, and
  `recordPartnerStackFailure` still calls `alertOps` directly for the two
  headline money failures.

## Asserted only structurally

- That `recordSuccess` sits after the `!sf.ok` guard is an **ordering**
  assertion over source offsets. The executed streak test proves the mechanism
  (a success resets, and the two sources do not share a streak); the ordering
  assertion pins the wiring. Per
  `docs/tickets/ordering-assertions-do-not-check-reachability.md`, an early
  return above it would survive.

## Not done / open

1. **The Slack raw-key case is unchanged.** The first lead from a brand-new
   partner still posts the key, because only memory and the database are awaited
   before Slack fires. `slackPartnerHearLabel` already relabels it in words.
   Making Slack wait on the API would put a third party in front of an alert.
2. **No backfill.** The four existing step-1 rows keep their null
   `ps_partner_name`. The dashboard resolves them correctly anyway, because it
   now resolves by key — which is why a backfill is not needed to fix the
   screen. Say the word if you want the columns filled too.
3. **`/partial` now makes an outbound call** it did not before, for a brand-new
   partner key only, once per partner per process, after `res.json()`. Measured
   exposure today: two partner keys total.
4. **Broader dashboard audit not attempted.** I fixed what the screenshots
   pointed at and what I found underneath it. I did not sweep every tab.

## Deploy notes

Server-side only. `git push` to `main` → Railway. **No Webflow step** — neither
form file is touched, so the twelve page pins are unaffected.
