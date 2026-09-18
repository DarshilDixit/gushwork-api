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

## CORRECTION, 18 Sept 2026 — two of the four options below were wrong

Written into the first version of this ticket and checked afterwards. Both
claims were wrong, and the corrections change what is worth building.

**Option B does not work, and the "~100 seconds earlier" claim was false.**
`steenhoekins.com` is a syntactically valid domain with a real TLD. A "does
this look like a domain" guard passes it; only DNS rejects it, and by then the
fetch is already paid for. It would not have caught this lead. It is also worth
very little on its own: **39 of 3,127** warms in 30 days ended `unreachable`,
`thin` or `blocked_by_site` — 1.2%.

**Option C already exists.** `nonIcpCandidateDomains` already returns the email
domain alongside the website domain, and the warm already fires on the step-1
email blur — the code comment states the intent outright: *"The step-1 email
blur buys roughly the thirty to sixty seconds a visitor spends on step 2."* It
did nothing for these two leads because both used free mail (`@outlook.com`,
`@me.com`). Free-mail domains are also already filtered: only 3 of 3,127
verdict rows are providers.

So neither is worth building, and the ticket should not have proposed them.

---

## THE RACE IS THE WRONG FRAME

The first version treated this as a timing problem to be won — get the verdict
into the cache before the click. Measured, that framing is wrong.

**The cost this feature exists to avoid is an AE spending an hour with a
non-ICP prospect. That cost is incurred at the MEETING, not at the booking.**
And the meeting is not imminent:

| Booking to demo | |
|---|---|
| Median | **35 hours** |
| 10th percentile | **3.3 hours** |
| Demos within 1 hour of booking | 60 of 1,543 (3.9%) |
| Demos more than 24 hours out | 920 of 1,543 (60%) |

There is no 2.6-second race to win. There is a **3.3-hour window at the tenth
percentile** in which to reach the same conclusion, by which time the cache is
warm — both of these verdicts existed within 12 seconds of the submit.

Every option that touches the lead path is trying to win a race the business
does not need won.

---

## Options, re-framed

**A. A sweep over recently booked leads. (Recommended shape.)**
Re-read `non_icp_domain_verdicts` for leads booked in the last N minutes, and
act on any that now resolve blocking. Touches nothing on the lead path: no
redirect, no wait, no form change, no re-pin. A sweep on a 5-minute cadence
would have caught both of these with hours to spare. Converts a 2.6-second
race into a multi-hour one.

**B. Re-check when the calendar step loads.**
Would have to fire within ~2.6 s of the submit to catch Steenhoek, which is
roughly when step 3 renders — so it is still a race, just a slightly longer
one. And the action is bad: pulling a visitor off a calendar they are already
looking at, or worse, after they have chosen a slot.

**C. Re-check inside the three booking routes.**
Cheaper than a sweep and catches both (their verdicts predate the bookings by
14.0 s and 28.9 s). But it fires exactly once, at the only moment the verdict
might still be missing — a lead whose warm is slower than their booking is
missed permanently, with no second look.

**D. Make `/submit` wait.**
Stated only to rule it out. This is the one thing the design refuses, and
nothing here argues for changing it.

**The open question is not which mechanism — it is what ACTION is appropriate
once a booked lead is found to be non-ICP.** That is a business decision, not
a code one, and it is the thing this ticket actually needs answered:

1. Stamp `non_icp_blocked` only — the lead drops out of Salesforce pushes and
   the SDR list, the demo still happens.
2. Stamp it and alert a human to cancel or reassign.
3. Cancel the booking automatically.

Option 1 is nearly free and does not remove the AE's hour. Option 3 removes it
but is the most aggressive action in the codebase and would be taken on a
model verdict without a human in the loop.

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


---

## WHAT WAS ACTUALLY BUILT, 18 Sept 2026 — PR 94 and 95

Not option A. The sweep above was framed around a booking that has
already happened, and the objection to it was the right one: an AE's slot
is the cost, and cancelling on somebody who has already chosen a time is
a bad answer even when the classification is correct.

So the gate moved earlier instead, to the last moment anything can
prevent the booking at all.

**RevenueHero commits the slot before it tells us.**
`initRHBookingListener` fires on `MEETING_BOOKED`, past tense, so the ten
to forty seconds a visitor spends choosing a time cannot be used. The
only remaining window is between `/submit` returning and the calendar
rendering — which is where the hold now sits, behind a calendar skeleton,
so the slot is never taken and nobody loses a calendar they were already
looking at.

**It holds for exactly one state**: a warm in flight for one of this
lead's domains. A cached verdict decides with no hold (61% of traffic);
nothing in flight shows the calendar immediately, because waiting for a
decision nobody is computing is pure delay. Capped at 4s, failing open on
every path.

Two supporting changes, neither of which prevents anything on its own:
a speculative warm from a free-mail local part (warm-only, never a
decision input) and warming on debounced website input as well as blur.

**The residual is unchanged and still needs an answer.** A 19.6-second
scrape still beats the cap, and the question this ticket raised — what
action is appropriate when a booked lead turns out to be non-ICP — was
not answered by building the hold. It was avoided for the common case.

**And the hold leaves a gap of its own**: a lead blocked there still has
`non_icp_blocked=false`, a fired Meta `Lead` and a Salesforce push,
because `/submit` ran before the verdict existed. Strictly better than
the same lead also taking a slot, but not clean. The fix is server-side
and is still open.
