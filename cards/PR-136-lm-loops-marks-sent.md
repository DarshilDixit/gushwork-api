# PR 136: a lead the Loops automation emailed reads as sent

Branch `lm/loops-marks-sent`, from `main` at `95cf62f` (after #135). 7 Oct 2026.
**Not merged: waiting for your review.**

**Why:** the first real signup (`darshil@darshildixit.com`, row 140) showed
"Awaiting" because "sent" was a button a person pressed after building the
150-questions list by hand. The "20 prompts" Loop sends the email now.

**What:** when Loops accepts the contact AND the `lead_magnet_requested` event,
the lead is marked delivered with the note "Sent by the Loops automation".
`loops.js` now returns `eventSent`, so a refused event or
`LOOPS_SEND_EVENT=false` never reads as sent. A lead is never un-marked, and a
hand-written note is never overwritten. Signup and the retry button share one
statement (`LOOPS_RESULT_SQL`).

**Limit:** "sent" means handed to Loops with its trigger accepted. Loops does
not report the actual send back, and a paused Loop still accepts the event.

**Not changed:** Meta, Salesforce, the demo form, blocking.

**Verified:** full bar, bare: 16 suites, 5435 assertions, 0 failed
(test-batch-a 356 to 369, re-baselined). The new checks execute both routes
and the real `loops.js`. Mutations: ignoring eventSent CAUGHT, `loops.js`
always reporting sent CAUGHT. The retry-route mutation was **not run**. The
statement was PREPAREd on Railway read-only, typed and untyped.

**Not done:** row 140 stays "Awaiting" (only new signups get marked). Press
Mark sent on it once.
