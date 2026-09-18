# The model verdict can arrive after the lead has already submitted

Raised 18 Sept 2026, after an insurance lead reached the calendar with both
blocks switched on and the model having classified the domain correctly.

**Nothing here is a logic bug.** Every component did what it is specified to
do. The specification loses a race it does not know it is in.

---

## The lead

```
Pamela Steenhoek · steenhoekinsurance@outlook.com
Steenhoek Insurance · https://steenhoekinsurance.com/
Facebook (Paid) · landed /start, submitted on /demo · product aeo
```

Booked a demo for 21 Sept. Not blocked, not flagged, `non_icp_source` null.

The model was not wrong about it:

```
steenhoekinsurance.com → business_type=insurance, confidence 0.97, blocking=true
evidence: "Medicare and Health Insurance for Texans"   scrape_status=ok
```

---

## What actually happened

```
12:05:53.077   lead created          /partial at step 1 — email only, no website yet
12:07:49.847   verdict cached for    steenhoekins.com      <- partial value, mid-typing,
                                                              unreachable, wasted warm
12:09:30.300   SUBMITTED             /submit reads non_icp_domain_verdicts, finds nothing
12:09:32.898   verdict cached for    steenhoekinsurance.com <- 2.6 s too late
12:09:46.783   booked
```

`/submit` is a **cache read and nothing else** — no scrape, no model call, no
network, deliberately no timeout to fall through. A miss is "we could not
check", which fails open. That is exactly what CLAUDE.md specifies, and it is
the right shape: a lead reaching the calendar because our own request was slow
is the failure that design exists to prevent.

So the lead was not let through by mistake. It was let through by the rule,
because at 12:09:30.300 the cache genuinely had no answer.

**The warm was late because it was spent on the wrong domain first.** A verdict
exists for `steenhoekins.com` — a prefix of the real domain, captured while the
visitor was still typing. That warm cycle resolved to `unreachable`. The real
domain's warm only began after the correction, and scrape-plus-model does not
finish in the seconds that remained.

---

## Scope: this is not a one-off

Leads submitted since 15 Sept whose website domain has a **blocking** verdict:

| | |
|---|---|
| Blocked correctly | **10** |
| Missed — verdict written after `submitted_at` | **2** |
| Of the missed, booked | **2** |

**When the verdict was in the cache at submit time the block worked 10 times
out of 10.** There are no false negatives from the matcher, the enum, the
confidence floor or the guards. Every miss is the same race.

Both misses are from 18 Sept:

| Lead | Type | Conf. | Verdict late by | Booked after submit | Demo |
|---|---|---|---|---|---|
| `dla1972@me.com` · Aliff & Associates | insurance | 0.98 | **11.7 s** | 40.6 s | 22 Sept |
| `steenhoekinsurance@outlook.com` · Steenhoek | insurance | 0.97 | **2.6 s** | 16.5 s | 21 Sept |

**Both demos are still in the future at the time of writing.** Whether anyone
should be told before those calls happen is a separate decision, not a code
change, and it is the only part of this that expires.

---

## Why the existing booking guard does not catch it

Tempting to assume the three booking routes already cover this. They do not,
for two independent reasons:

1. **`nonIcpScheduleSuppressed` reads `fullLead.non_icp_blocked`** — the column
   stamped on the lead row at submit time. It does not re-read
   `non_icp_domain_verdicts`. A verdict that landed afterwards is invisible to
   it. Confirmed: `SCHEDULE_LEAD_SQL` does not reference the verdict table at
   all.
2. **It only withholds the Meta Schedule event.** It does not block, does not
   cancel, does not notify. Even reading a fresh verdict, it would suppress a
   conversion signal for someone who already has a call on the calendar.

So "the booking route will catch it" is false today in both senses.

---

## Options, with what each is actually worth

Not recommending one. Each has a real cost and the choice is a business call.

**A. Re-read the verdict at booking.**
Both misses would have been caught: the verdict existed 14.0 s and 28.9 s
before the respective bookings. But it is only a *wider* race, not a closed
one — measured over 30 days, median booking is 19.9 s after submit and
**237 of 778 booked within 15 seconds**. It also raises the question option A
cannot answer on its own: what do you *do* with a lead who has already taken a
slot?

**B. Do not warm on a partial domain.**
Requiring a resolvable host before spending a warm cycle would have started
Steenhoek's real check roughly 100 seconds earlier — comfortably before the
submit. Cheap, narrow, and it fixes the cause rather than the symptom. Does
nothing for a visitor who types the domain correctly and submits fast.

**C. Warm earlier than the website field.**
The email domain is known at step 1, ~3.5 minutes before submit here. For
`steenhoekinsurance@outlook.com` that is a free-mail domain and worthless — but
for the many leads whose email domain *is* their company domain it would warm
minutes ahead.

**D. Leave `/submit` alone.**
Stated explicitly so it is a decision rather than an omission. Making the
route wait on a scrape or a model call is the one thing this design refuses,
and nothing here is an argument for changing that.

---

## What is NOT wrong, and should not be "fixed"

- **The fail-open on a cache miss.** Working as designed and correct.
- **The block logic, matcher, enum and confidence floor.** 10 for 10 when the
  data was there.
- **The brand-domain list.** `steenhoekinsurance.com` is an independent local
  agency, not a national carrier. CLAUDE.md is explicit that local agencies are
  not on the list — roughly 70 of them exist in the data. The list behaved
  correctly by not matching.
- **`non_icp_checked_at` being stamped while `non_icp_source` is null.** A
  decision was reached — "no blocking match" — so the timestamp is honest.

---

## Open questions for whoever picks this up

1. Is a ~2-to-12 second window worth closing at all, at 2 leads in 3 days?
2. If a verdict lands after booking, what is the intended action — nothing, an
   internal alert, or something the AE sees before the call?
3. Do the two already-booked demos on 21 and 22 Sept need a human decision
   before they happen?
