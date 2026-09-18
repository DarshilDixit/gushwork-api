# Hold the calendar while an answer is seconds away

PR 94 - branch `feat/non-icp-calendar-hold` - 18 Sept 2026

Two insurance leads booked demos on 18 Sept with both non-ICP blocks on and the
model having classified both domains **correctly** — 0.97 and 0.98. Nothing
misfired: `/submit` is a cache read and nothing else, a miss fails open, and
both verdicts were written **2.6 s and 11.7 s after the submit**.

## Why the gate is where it is

RevenueHero commits the slot **before** it tells us — `initRHBookingListener`
fires on `MEETING_BOOKED`, past tense. So the 10–40 seconds a visitor spends
choosing a time cannot be used. The last moment anything can prevent the
booking is between `/submit` returning and the calendar rendering.

Holding there means **the slot is never taken**. Every later option means
cancelling on somebody who has already booked.

## It holds for exactly one state

| State | Frequency | Action |
|---|---|---|
| Verdict cached | **61%** (live cache-hit rate) | decided at `/submit`, no hold |
| **Warm in flight** | the case that matters | **hold, ≤4s** |
| Nothing in flight | the rest | show the calendar — waiting for a decision nobody is computing is pure delay |

That last row is the one a careless version gets wrong by holding "just in
case" and taxing every lead. Fails open on every path: cap, throw, timeout,
old backend without the field.

## Three changes — only the first prevents anything

**1. The hold**, behind an injected calendar skeleton (shimmer in the shape of
RevenueHero's two panes, `prefers-reduced-motion` respected). Not Webflow
markup, so it versions with the code that shows it — and it fills a gap that is
blank today, since `#rh-embed` sits empty while RevenueHero loads.

**2. Speculative warm from a free-mail local part.** 79% of leads use a
business email and were never at risk — their domain has been warming since
step-1 email blur. The affected class is the 21% on free mail, and for **37 of
those 535** the local part *is* the domain: `steenhoekinsurance@outlook.com` →
`steenhoekinsurance.com`. **Warm-only, never a decision input** — the guess is
wrong 93% of the time, and a test asserts it can never reach
`nonIcpCandidateDomains`. Wrong guesses cost one failed fetch and never reach
the model.

**3. Warm on debounced website input as well as blur.** Neither trigger
dominates: type-and-tab blurs in ~0.2 s, faster than the 800 ms debounce;
pausing to re-read blurs seconds later. Both, deduped per domain, beats
choosing.

## ⚠️ Known gap, deliberately left

A blocked verdict at the hold redirects the visitor, but the lead row still
says `non_icp_blocked=false` — `/submit` ran first, so it fired Meta `Lead` and
pushed Salesforce for somebody we then turned away.

Left alone on purpose and commented in both files. Without the hold that same
lead is recorded **identically and takes an AE's slot**, so this is strictly
better rather than a regression. Correcting it from the client would mean an
unsupervised write into the money path to fix ~2 leads a week. The proper fix
is server-side and is its own decision.

## Verification

Full bar green — **3,838 assertions, 12 suites**, run bare.

**The hold is driven, not read**: six states per form file including the cap
actually elapsing, a thrown check, and that every poll asks with `fresh: true`.
That last one matters — `checkNonIcp` memoises per `email|website`, so without
it the poll would be handed back the not-blocked answer it is trying to
supersede and would spin to the cap reading its own reply.

**A reachability hole was found and closed.** Replacing the
`awaitNonIcpVerdict` call in the submit flow with `Promise.resolve('clear')` —
the function present, correct, and never consulted — **survived the entire
bar**. Every behaviour test drove the function in isolation and none could see
that nothing called it. Six wiring assertions per file now pin it: awaited,
inside the `Promise.all` with `rhPromise`, redirects on blocked, `return`s so
the calendar never opens, redirect precedes `setEmbedTarget`, skeleton shown at
step 3. Re-verified: the mutation is now **CAUGHT**.

Also caught in review: those assertions matched inside a byte window, so the
next comment added between the branch and its redirect failed a correct file.
They now strip comments first.

## Not deployed by merging

Forms **v5.16.0**. 15 Webflow page pins still to move.

## What was verified, and what was not

**Executed:** full bar bare (3,838 assertions, 12 suites); `awaitNonIcpVerdict`
driven against a stubbed check for six states in BOTH form files, including the
4s cap actually elapsing on a wall clock; `nonIcpSpeculativeDomain` driven over
8 inputs including both real missed leads; `nonIcpWarmPending` driven over four
in-flight shapes; three mutations (hold never awaited, hold fires with nothing
in flight, and the wiring removed) — all CAUGHT after the wiring gap was closed.

**Measured against production:** 3.3 s average warm and 19.6 s worst (live
`/monitor/non-icp`); 61% in-process cache-hit rate; 79% business email vs 21%
free mail over 90 days; 37 of 535 free-mail local parts matching the site
domain; RevenueHero's `MEETING_BOOKED` confirmed post-commit by reading the
listener.

**Asserted only structurally:** the wiring of the hold into the submit flow —
six assertions per file. Their ceiling is known and stated: they prove the call
is present and ordered, not that a browser executes it.

**Never executed:** the hold against a real browser and a real RevenueHero
embed. The skeleton's appearance has not been seen by anyone — it is injected
CSS and markup that no test renders.

**Known gap, deliberately left:** a lead blocked at the hold still has
`non_icp_blocked=false`, a fired Meta `Lead` and a Salesforce push. Commented
in both files; the server-side fix is its own decision.
