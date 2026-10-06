# Background: Apollo and Meta failure alerting

Moved here from `CLAUDE.md` on 7 Oct 2026, word for word, to bring that file under its size limit. `CLAUDE.md` keeps the rule for each of these as one line with a link here; this file keeps the history, measurements and worked examples behind it. Dates and counts are as of when each paragraph was written, and "today" means that day. Line numbers in the headings are `CLAUDE.md` at commit b924ce7.

## Apollo refuses with a reply (was `CLAUDE.md` lines 1627-1648)

**APOLLO REFUSES WITH A REPLY, NOT AN EXCEPTION, AND IT WENT UNNOTICED
THREE TIMES.** Out of credits it answers `{"error":"You have insufficient
credits!"}`: `fetch` resolves, `.json()` parses, nothing throws. `/enrich`
read that as "no match" and wrote an empty row, System Health counted rows
written and read **80% enriched** while Apollo had found **0 of 86**, and
`recordFailure` sat in the catch where it could never run. 413 lookups
were refused across 24 Jun, 3–10 Sept and 23 Sept onward before anyone
looked.

`apolloReplyError` now reads the reply, **"insufficient credits" pages
straight away as "Out of credits"** (not "Authentication failed" — the key
is fine), and a refusal is recorded **insert-only**, because the old upsert
also wrote a refusal straight over a real enrichment and blanked the lead
row to match. Health counts what Apollo FOUND, over **business-email**
leads (free mailboxes never reach Apollo, so counting them made a perfect
run read ~60%), and a latest-reply-was-a-refusal is red before any rate is
looked at.

**The critical repeats every 3 hours** (the normal critical cooldown)
while credits stay empty and leads keep arriving. Re-enrich afterwards
with `tools/re-enrich-apollo.js`.

## How a Meta CAPI failure reaches recordFailure (was `CLAUDE.md` lines 1655-1685)

**A Meta CAPI failure only reaches `recordFailure` because the push
functions THROW.** They end in `Promise.allSettled`, which never rejects,
so until `throwIfAnyFailed` existed the `.catch(...)` at all five call
sites — every one calling `recordFailure('Meta CAPI', ...)` — could not
fire. Same class as the 21 silent PartnerStack call sites, reached from the
opposite direction: there the `FAILURE_MONITORS` entry was missing, here it
was always present and the promise shape swallowed the failure. Two shapes
must both keep arriving: a rejected promise, and a resolved
`{success: false}`. Every caller is fire-and-forget with a `.catch` and
none `await`; a caller without one turns a Meta outage into an unhandled
rejection, and a test asserts that.

**Meta auth failures match FOUR CODES, not `/OAuth/i`.** `isAuthFailure`
bypasses both thresholds and pages critical — Slack and email — on the
first occurrence. Meta stamps `type: "OAuthException"` on nearly every
Graph API error, including a bad parameter (100), a rate limit (80004) and
a transient server error (2), so the generic list made all of them page
instantly: measured, 8 of 11 realistic bodies, of which only 4 were
credential problems. `isAuthFailure(error, source)` now takes the source
and matches only 190/102/463/467 for Meta. **Every other integration still
uses the full list** — the three non-Meta callers pass no source at all.

**`recordSuccess('Meta CAPI')` is wired through an injected reporter**, and
before that it was never called anywhere, so the streak only reset when an
alert fired — "3 consecutive failures" actually meant "3 failures since the
last alert, ever". `meta-capi.js` reports each landed event via
`setMetaOutcomeReporter`; injected rather than imported, because a module
reaching back into `index.js` is how a require cycle starts. Success only:
failures already arrive through the call-site `.catch`, and reporting from
both would double-count.
