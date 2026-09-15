# PR 72 — CRM allows B2C, and the model layer gets a surface

**Branch:** `feat/crm-b2c-and-model-visibility` · **Not merged.**
**Date:** 15 September 2026

Three things. Two were asked for, one was found while looking.

---

## What was VERIFIED by running it

- **All twelve suites, bare, zero failures.** 3063 → 3125 assertions.
  Read in full, not piped.
- **Twelve mutations, all CAUGHT** — measured with `measure.js --mutation`
  against a committed baseline, never by counting markers. Two of the twelve
  did not start out caught; see below.
- **Every new and changed SQL statement EXPLAINed against the real schema**,
  read-only, on the Railway database. Five statements: the report's lead read,
  its verdict read, both rollups, and the patched SDR predicate. All plan
  clean. Done because a source assertion cannot tell you whether a query
  parses — the `//`-in-SQL lesson.
- **The overnight production numbers** in the summary below are from real
  queries against the live database and the live `/monitor/health`, not
  inferred.

## What was asserted STRUCTURALLY but never executed

- **The form change has never run in a browser.** `node --check` passes on
  both files and section 21 asserts the constant, the single computed gate
  and the absence of a leftover raw condition — but nobody has loaded
  `/ai-demo`, answered B2C and watched step 2 appear. It cannot be checked
  from here: the files reach production through Webflow.
- **The Model tab has never been looked at by a human.** The route is driven
  over real HTTP with crafted rows and the tab's JS is evaluated in a stubbed
  DOM with every container asserted to have painted content rather than an
  error — which is strictly more than "it renders" — but no one has seen it.
- **The three downstream consequences of item 1 have not fired in
  production.** No CRM B2C lead has yet reached `/submit`, so no Meta event,
  no PartnerStack conversion and no Salesforce push has actually happened
  for one. The code paths are the existing ones; what changed is which leads
  reach them.

---

## 1. `/ai-demo` no longer disqualifies B2C or Mixed

**Where the gate actually is, which is not where it looks.** Entirely
client-side: `gushwork-form.js` sets `disqualified` and shows a terminal
step, and the server believes the boolean it is handed. `product` IS
resolved server-side in the same request (`resolveProduct({ page_url })`,
before the upsert at both `/partial` and `/submit`), so the server could
override — but an override changes only what we record, not the step the
visitor sees. Wrong shape for the ask.

So: `B2C_ALLOWED_PATHS` in both form files, matched against the pathname with
the same normalisation `resolveProduct` uses. The condition is computed
**once** per file and drives both the flag and the step.

**Four copies of the CRM path set now exist.** `PRODUCT_PATHS`, `index.js`
importing it, one per form file. `test-batch2.js` §21 lifts all three files
and asserts agreement — agreement, not a literal, so it is not a fifth copy.

**Both `sell_to ILIKE 'B2B%'` predicates moved together** — `/monitor/sdr`
and the `noBooking` card that counts it. Without it a CRM B2C lead is in
every headline number and absent from the one surface anybody acts on.

### This alters Meta, PartnerStack and Salesforce

Authorised explicitly in the request. Recorded here because CLAUDE.md
requires it said out loud:

| | Before | After |
|---|---|---|
| Meta | no StartTrial / Lead / Schedule | all three fire |
| PartnerStack | conversion skipped | **can fire a $50 conversion, irreversibly** |
| Salesforce | never pushed | pushed |
| Recovery cron | skipped | sends drop-off emails |

Measured exposure: **17 of 29 `/ai-demo` leads all-time hit the gate**, 14 by
pressing "actually we're B2B". About two a day.

## 2. `/monitor/non-icp` and the Model tab

Four panels. The two decisions worth defending:

**The join is in JavaScript, not SQL.** A `regexp_replace` approximating
`partnerStackCustomerKey` would be a second normaliser beside the one
function CLAUDE.md says must stay the only one, and would disagree with
production on exactly the subdomain and free-mailbox cases that matter.

**The scrape panel reports latest-outcome-per-domain and says so on
screen.** `nonIcpWriteVerdictRow` is `ON CONFLICT DO UPDATE`, so a domain
that failed and later succeeded overwrites its own failure. No historical
rate is computable from that table and none is rendered. Per the decision on
option (a); revisit in a week if too coarse. An append-only attempts table is
the option (b) that was not built.

**One thing the ladder had to work around, worth knowing.** `nonIcpStamp`
stamps `non_icp_checked_at` for any verdict that is not `check_failed` or
`disabled` — a plain no-match included. So "decided, no source" mixes "the
model read their site and had no objection" with "the model never answered".
On 15 Sept that was 23 of 27 leads in one bucket. Only the verdict join
splits them, which is why the last two ladder rows need it.

## 3. The flagged branch in `/submit` said the wrong thing

Its comment claimed it was only reachable with `NON_ICP_LLM_BLOCK` off.
False: `blocked` is `blocks && NON_ICP_LLM_BLOCK`, and `blocks` is false for
the four industries that suppress Meta without blocking, so those arrive
there with BLOCK on. It fired twice on the first night live. The Slack post
already branched correctly; the log line printed `Would have blocked` about
a lead that was never a blocking candidate — the exact falsehood the comment
inside that function forbids. Comment and log line only.

---

## The two mutations that did NOT start out caught

Reported because a card that only lists catches is not evidence of anything.

**A broken `mdlLadderHtml` came back UNMEASURED.** The suite *did* catch it —
four failures carrying the real production symptom, `Could not load: ... is
not defined` painted into the panel — but ran 167 assertions instead of 168,
because the assertion loop iterated over whatever had been painted and the
throw meant one container was never reached. `measure.js` was right to refuse
it: a count that moves with the failure cannot distinguish a catch from a
suite that quietly ran different checks. That is the `test-ads-parity`
failure mode (one failure, 13 of 159) in a new place. Fixed with a fixed list
of six containers, each asserted painted **and** not an error.

**Deleting the `meta_only` branch from the ladder SURVIVED the whole suite.**
Route still answered 200, tab still painted, one population silently folded
into another — the only failure mode the ladder exists to prevent. The route
is now driven with five crafted leads, one per state, including the one the
lead columns cannot answer alone.

Both were fixed, re-baselined and re-run: CAUGHT.

## A process note

A backgrounded mutation loop running `git checkout -- .` between runs wiped
an uncommitted edit to the V2 ticket while it was being written. Nothing
committed was lost and the edit was redone. It is the exact trap already
written down — `git checkout` restores from HEAD — arriving through a
background task rather than a foreground typo.

---

## Not done, and it is half the change

**`git push` does not ship the form half.** Both files are bumped to
**v5.12.0** / **v5.12.0-ads** and pinned by `test-partnerstack.js`, but
Webflow serves them from a commit SHA. After merge:

1. `git rev-parse HEAD`
2. Update **both** `<script src>` tags in Webflow → Project Settings → Custom Code
3. Republish, then sweep all 14 pages
4. Confirm the console banner reads `v5.12.0`

Until then `/ai-demo` still disqualifies B2C however green this repo looks.

`/ai-demo` serves `gushwork-form.js`, not the popup fork — verified live
today. The fork carries the change for parity, not for traffic.

**Deliberately not built:** the append-only scrape-attempts table (option b);
any change to which industries suppress Meta — that is the week-long
observation the ticket's new probation section sets up.
