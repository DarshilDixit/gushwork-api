# gushwork-api

Inbound lead capture, verification and routing for gushwork.ai. Node + Express on
Railway, Postgres, no build step. `index.js` is ~6,000 lines and holds most of the
system.

Explain things in plain language — no jargon, no making things sound more complex
than they are.

---

## The one rule everything else follows

**A lead is worth more than a verdict.**

Every check in this system exists to add information, never to stand between a real
person and a demo booking. When a checker breaks, times out, gets rate limited, or
hits something it doesn't understand, it **fails open** — the lead goes through and
the check reports that it couldn't decide.

Three things follow from that, and they are not negotiable:

1. **"We could not check" is never recorded as "we checked and it is bad."**
   A DNS resolver failure is not a fact about someone's domain. A captcha wall is
   not a thin page. A timeout is not a dead site. Each of these gets its own
   verdict that says what actually happened.

2. **Blocking is the highest-risk action in the codebase.** Three *website
   verdicts* block, all decided client-side in `gushwork-form.js`: `nxdomain`,
   `brand_mismatch`, `mailbox_domain`. Do not add a fourth without asking.

   **There are now TWO server-side blocking mechanisms, and they are the
   exception that proves this rule rather than a loosening of it.** Both are
   decided by `nonIcpVerdict` in `index.js`, stamped on
   `leads.non_icp_blocked`, and enforced in `/partial` and `/submit`.

   1. `NON_ICP_BLOCK` (default OFF, **`true` in Railway**) — the
      **brand-domain list**. A pure string comparison against the national
      real-estate brokerage and insurance carrier domains in
      `NON_ICP_DOMAINS`. **Checked FIRST**, because it is deterministic,
      re-derivable by reading a list, and it still works when a site
      refuses a scraper. This said "41 domains" and was already wrong by
      one before four more were added on 23 Sept; **count the constant,
      do not trust a number written down here.**
   2. `NON_ICP_LLM_BLOCK` (default OFF, **`true` in Railway**) — the
      **model layer**. Reads the company's own website and classifies it
      into one enumerated `business_type`; exactly two of those block,
      `real_estate` and `insurance`. Stamped with `non_icp_source='llm'`.

   **THE MODEL LAYER HAS TWO INPUTS SINCE 23 SEPT, NOT ONE, AND THE SECOND
   IS WEAKER ON PURPOSE.** `NON_ICP_NAME_FALLBACK` (default OFF, **`true`
   in Railway**) runs ONLY when the scrape failed, and judges the **domain
   name alone** — no page, no company field. Measured: 36 of 254 domains
   came back unreadable, and three of the five insurance / real-estate
   demos that leaked through were exactly those. None was fixable by
   fetching harder — `longandfoster.com` answers 403 behind a bot wall,
   `westexinsurance.com` does not connect, `adrianadearaujorealtor.com`
   returns 200 with 64 characters because it renders client-side.

   It carries **its own higher confidence floor (0.9 against 0.75)**, its
   own `non_icp_source='llm_name_only'` and its own prompt version, because
   a 0.75 meaning "the page says we sell insurance" and a 0.75 meaning "the
   hostname contains those letters" are not the same claim. `unknown`
   returns null rather than a verdict, so the row keeps the six-hour
   failure TTL instead of a 180-day non-answer.

   **The domain, never the company field the visitor typed.** The cache is
   keyed by domain; a per-lead input would make the stored verdict depend
   on whichever lead warmed it first. Validated against 60 real unreadable
   domains before switching on: 6 would block, all real estate, no false
   positives, and the near-misses (`creativelendersllc.com` →
   mortgage_lending, `tcwealthadvisors.com` → financial_advisory at 0.90)
   correctly did not — the **enum** kept them safe, not the prompt.

   **NEITHER REPLACES THE OTHER AND THE LIST IS NOT BEING RETIRED.** An
   earlier version of the V1 ticket said it would be, attributed to Swapnil.
   It was not his, and it is wrong on the measurement: `farmers.com` and
   `farmersagent.com` both read `scrape_status=failed` in the warehouse
   classifier and are three of the first four real blocks. National brands
   are exactly the sites a scraper cannot read. See the CORRECTION section
   at the end of `docs/tickets/non-icp-v1-block.md`.

   It exists because AEs reported State Farm agents and realtors taking demo
   slots, and because the measurement backed them: those leads book at 71%
   against 64% overall and then attend only 47% of the time against 66%. It
   was authorised explicitly by Swapnil on 11 Sept 2026, and it **reverses two
   written positions in the Non-ICP doc** — see
   `docs/tickets/non-icp-v1-block.md`, which is the record of that.

   Everything else here still holds. The block **fails open** on any throw,
   timeout or cold cache, it checks the warehouse customer tables first so a
   paying customer is never blocked, and it is the only server-side check that
   blocks. Every other server-side check still informs and never blocks.

3. **Suppressing a Meta event is a real cost, not a safe default.** It removes a
   conversion signal from the ad algorithm. Treat "should this fire Meta?" as a
   business decision to surface, not a judgement call to make quietly.

   **THREE things gate Meta today, and every one was surfaced as a decision
   rather than taken quietly.** `isWebsiteVerified` is the oldest. The second
   is `non_icp_blocked`: a blocked lead fires none of `StartTrial`, `Lead` or
   `Schedule`. That was the explicit point of the change — an ad audience
   optimised toward realtors is what produced the complaint — and it was
   authorised on 11 Sept 2026.

   **The third is `NON_ICP_LLM_META`, and it is a SEPARATE FLAG from
   `NON_ICP_LLM_BLOCK` on purpose.** The model layer can run in flag-only
   mode: it stamps `non_icp_llm_flagged`, the lead reaches the calendar and
   goes to Salesforce exactly as today, and Meta is untouched. Switching on
   observation must never reshape an ad audience as a side effect — that is
   this rule, applied to the switch itself. `NON_ICP_LLM_BLOCK` implies
   `NON_ICP_LLM_META`, because a lead we turn away that still feeds the
   algorithm a conversion is incoherent.

   **Read `suppress_meta` off the verdict, never `blocked`.** A flagged
   lead is not blocked and still has to stop firing Meta when META is on.
   Three call sites read it: `/partial` (StartTrial), `/submit` (Lead), and
   `nonIcpScheduleSuppressed` (Schedule, for all three booking routes).

   **`Schedule` fires from THREE call sites, so it needs three guards.** They
   are `/booking-confirmed`, `/booking-confirmed-webhook` and
   `/booking-confirmed-webhook-rh`. Counting event *names* and concluding
   "three call sites" is wrong and is how one leaks: there are six in the
   repo, five in `index.js` plus `Contact` in `lead-magnet.js`.

   **The three guards are now ONE function, `nonIcpScheduleSuppressed`,
   called three times.** They were three copies reading `non_icp_blocked`
   off `SCHEDULE_LEAD_SQL`, and they stayed in step by luck. Adding the
   second condition (the model flag) to a guard that exists in triplicate is
   exactly when the third copy gets missed, so the condition moved into one
   place. `tests/test-non-icp.js` section 8 both asserts the three call
   sites and **executes** the function, which is what closes the
   reachability hole an `if (false)` inside it would otherwise leave.

   **IT IS ASYNC SINCE 23 SEPT, AND IT RE-READS THE VERDICT TABLE.** The
   two conditions above read a COLUMN stamped at `/submit`, and that column
   can be wrong in one direction: `/submit` is a cache read and nothing
   else, so when the warm has not finished it correctly stamps
   `non_icp_blocked=false` — "we could not check" — and the verdict lands
   seconds later with nobody listening. Measured on 18 Sept:
   `steenhoekinsurance.com` came back `insurance` at **0.97 confidence 2.6
   SECONDS after** the submit, and `dla1972@me.com` 11.7 seconds after.
   Both booked.

   So the guard now also asks `non_icp_domain_verdicts` directly, and
   **fails open on any throw**. It withholds the Meta `Schedule` event and
   NOTHING else — the booking is already real and the person is looking at
   a confirmation. All three routes `await` it.

   Turning it async is what broke `tests/test-non-icp.js` §8, which LIFTS
   AND RUNS the function: `new Function` cannot hold an `await`, so the
   suite died at load. That is the test doing its job on a signature change
   touching all three call sites; it now builds through the AsyncFunction
   constructor.

When a change would alter which leads get blocked or which fire Meta events, say so
explicitly in your summary. Never let that happen as a side effect.

---

## Layout

Every file in the repo root, so this list can't quietly go stale the way it did
before — a file missing from here reads as "forgotten," not "not documented yet."

| File | What it's for |
|---|---|
| `index.js` | Routes, website checking, email verification, alerting, the monitor dashboard, cron |
| `db.js` | Schema + migrations. Runs on every boot; everything is `IF NOT EXISTS` |
| `salesforce.js` | Lead upsert by email. Refresh-token OAuth |
| `meta-capi.js` | Conversions API — `Lead`, `Schedule`, `StartTrial`, `Contact`. Also owns the product catalogue (`PRODUCTS`, `resolveProduct`), which `index.js` imports |
| `loops.js` | Loops.so contact push for the lead-magnet landing page |
| `partnerstack.js` | PartnerStack API. TWO hosts and TWO auth schemes: `partnerlinks.io` conversion (Bearer tracking token) and `api.partnerstack.com` v2 partnerships + actions (Basic public:secret) |
| `lead-magnet.js` | `/lm/*` routes. Separate table, deliberately not joined to `leads` |
| `backfill-sf.js` | Manual recovery tool for re-syncing leads to Salesforce after a broken connection or outage. Not mounted by default — see below |
| `tools/non-icp-validate.js` | Scores the model layer against history — scrapes every lead domain once, replays the same bytes to three models, joins to paying customers and showed-up bookings. LIFTS the real classifier out of `index.js` rather than copying it. Not mounted, run by hand |
| `tools/fire-non-icp-slack.js` | Fires the non-ICP Slack paths for real: `blocked`, `llm-blocked`, `llm-meta`, `late-block`, `booking`. Lifts them out of `index.js` like `fire-alert.js`. `late-block` is the sweep's post and was fired by hand on 23 Sept — a stamped lead is NOT in Salesforce and NOT on the SDR list, so that post is the only thing telling a human a booked meeting turned out non-ICP. Not mounted, not called |
| `tools/fire-dropoff-digest.js` | Sends the weekly dropoff digest for real, to the **leads** channel. **Its own file on purpose:** it first went into `fire-non-icp-slack.js` because that tool already had the lifting machinery, which is the wrong reason — the digest has nothing to do with the non-ICP block, and a tool named for one feature that quietly fires another is how nobody finds either later. **The only fire tool that reads a DATABASE**, so on a developer machine it needs the Postgres service's `DATABASE_PUBLIC_URL`, not `gushwork-api`'s private-network `DATABASE_URL`. The numbers are real and only the timing is not, so its marker says that rather than calling the data a test — and the marker is appended AFTER the digest is composed, because the message must stay byte-identical to what Monday sends. **`liftDecl` cannot take `DROPOFF_STAGES`** (it brace-matches one declaration and truncates an array of objects at its first element), so this lifts by REGION with a `between()` helper. Not mounted, not called |
| `tools/sf-mark-internal-test-leads.js` | Marks our own test submissions as **Invalid / Test** in Salesforce, by writing the `How_Did_You_Hear__c` the `Source_Bucket__c` formula already reads. **Marks, never deletes** — 141 Lead records match `isInternalLead` and ~100 are NOT ours. Provenance comes from `tools/internal-test-emails.json`, not from the address. Dry run by default; `--apply` writes a manifest and `--revert` undoes it. Not mounted, run by hand |
| `tools/internal-test-emails.json` | The provenance list for the tool above: addresses our OWN form actually submitted. Point-in-time, carries the SQL that regenerates it |
| `tools/re-enrich-apollo.js` | Re-runs the Apollo lookups that were REFUSED — the three out-of-credit windows (24 Jun, 3–10 Sept, 23 Sept on). Dry run by default: prices it (1 credit per person FOUND, 0 for no match), one lookup per address, copies from an earlier answer for the same address at zero cost, skips our own submissions. `--since`, `--limit`, `--apply`. **Stops at the first refusal.** Writes `enrichment_data` and the lead row exactly as `/enrich` does; never Salesforce, never the mirror. Lifts the parser out of `index.js`. Not mounted, not yet run |
| `tools/backfill-ip-coords.js` | Fills `ip_latitude` / `ip_longitude` for leads resolved BEFORE those columns existed — they have a city and no point, so they are complete in every table and invisible on the map. Only touches rows that already resolved and have no coordinates. Lifts `resolveIpGeo` out of `index.js`. Dry run by default; `--apply` writes. Run once on 23 Sept (17 rows). Not mounted |
| `tools/fire-alert.js` | Fires ONE real alert on purpose, to satisfy the fire-every-alert-path-once rule. Sends for real (Slack + email on a critical). Lifts `alertOps` out of `index.js` rather than reimplementing it, so what arrives is what production sends. Not mounted, not called by anything |
| `gushwork-form.js` | The `/demo` form frontend. Lives here and is served live by jsDelivr — see below |
| `gushwork-form-popup.js` | The Google Ads popup/modal form frontend. Lives here and is served live by jsDelivr — see below |
| `package.json` | Dependencies, scripts, Node engine constraint |
| `package-lock.json` | Locked dependency versions, committed so Railway installs exactly what was tested |
| `.gitignore` | Keeps `node_modules/`, `.env`, logs, and local Claude settings out of the repo |
| `README.md` | Repo landing blurb, not living documentation. This file is |
| `tests/` | The test files described under Deploying, plus `crash-reporter.js` (required first by every suite), `measure.js` (the test bar and the mutation-testing rule) and the committed `.baseline.json` |
| `docs/partnerstack.md` | PartnerStack handover: the two-step model, every ps_ column, env vars, test procedure, known gaps |
| `docs/OPEN-ITEMS.md` | What is still open or deliberately decided in THIS repo, as of 9 Sept 2026. The meta-capi repo has its own; neither is complete alone |
| `docs/tickets/non-icp-v1-block.md` | The non-ICP block: what the Non-ICP doc says, the two positions this reverses, the domain list with per-domain evidence, and the `sdr-calling` dependency |
| `docs/tickets/non-icp-verdict-arrives-after-submit.md` | Why a correct model verdict can land after the lead has already booked, the four options considered, and the measurement that reframed it — booking→demo median is 35 hours, so there is no 2.6-second race to win. Records that the calendar HOLD was built and the sweep was not, and that the sweep landed on 23 Sept |
| `docs/Non-ICP-flagging-rules-*.pdf` | Swapnil's Non-ICP flagging rules, as exported. **A screenshot with no text layer** — it does not grep. The ticket above quotes the parts that matter |
| `CLAUDE.md` | This file |

**`gushwork-form.js` and `gushwork-form-popup.js` are in this repo, not a separate
one,** and jsDelivr serves both from it. **They are pinned to a commit SHA, not to
`main`** — see below, because it changes what "deploying a form fix" means.
`darshildixit.github.io/gushwork-embeds` is a genuinely different repo — it holds
the Webflow CSS/JS embeds, not these two files. Don't confuse the two.

### Deploying a form change — the Webflow step

**A `git push` does NOT ship a form change.** The two form files reach production
through a `<script src>` that names an immutable commit SHA:

```
https://cdn.jsdelivr.net/gh/DarshilDixit/gushwork-api@<40-char-sha>/gushwork-form.js
https://cdn.jsdelivr.net/gh/DarshilDixit/gushwork-api@<40-char-sha>/gushwork-form-popup.js
```

**THE TAGS ARE PER-PAGE, NOT IN PROJECT SETTINGS — CORRECTED 15 SEPT 2026.**
This section used to say the tags live in **Project Settings → Custom Code**.
They do not, and have not for as long as anyone can check. Measured by reading
all 81 pages through the Webflow API on 15 Sept: the site-wide head and footer
blocks contain **no `gushwork-api` tag at all**, there are **zero registered
scripts**, and every pin lives in an individual page's **before-`</body>`**
footer block. Twelve pages carry one.

That is not a detail, it is the whole reason a page goes stale. There is no
single place to edit, so "update both script tags" is **twelve page edits**,
and a Project-Settings republish genuinely cannot touch any of them. Anyone
following the old wording would look in Project Settings, find nothing, and
either give up or ADD a tag there — which would load the form script twice, at
two different SHAs, with no error anywhere.

So shipping a form fix is **two** steps, and the second one is outside this repo:

1. Merge to `main` as usual.
2. Take the new `main` SHA (`git rev-parse HEAD`) and update the script tag in
   **each page's footer custom code**, then republish. Both files move together
   even though no page carries both — `/demo` and `/ai-demo` carry
   `gushwork-form.js`, the ten ad landers carry `gushwork-form-popup.js`.

Miss step 2 and the fix is in `main`, the tests pass, Railway has redeployed — and
every real lead is still running the old file. Nothing in this repo will tell you.

**Why SHA and not `@main`.** jsDelivr treats a SHA path as immutable and caches it
permanently, so it either serves those exact bytes or 404s. `@main` is a mutable
ref served best-effort, which needs a cache purge — and purges do not reliably
take. Pinning removes the purge from the process entirely.

**Use the full 40-character SHA.** Short SHAs work today but are ambiguous as the
repo grows, and a collision resolves to the wrong file rather than erroring.

**The `curl` sweep below has a blind spot the API closes.** Its page list is
maintained by hand, so it cannot find a page carrying a pin that nobody
remembered to add to it. Reading every page's footer block found exactly that:
**`/start-old` was pinned to `d493e92e`, a 26 June commit** — v4.4-era, months
behind. It is a 404 today and therefore harmless, which is also why no sweep
would ever have caught it. Publish that page and it would have served June's
form to real visitors. Prefer the API sweep; see the section at the end of
`docs/tickets/non-icp-v1-block.md`.

**`/start-old` was re-pinned with everything else on 16 Sept 2026** and is no
longer stale — but it is still a draft, so it is still invisible to the `curl`
sweep, and it will go stale again at the next form deploy unless the API sweep
is the one you run.

**AND A PIN CAN LIVE IN THE HEAD, NOT ONLY THE FOOTER — 18 SEPT 2026.**
Everything below says "footer", and a repin script written to that
assumption silently reported "no pin found" for `/ai-demo` while the live
page was plainly loading a form script. The page had been reworked and
its tag now sits in the **head** block.

A read that finds nothing is not proof of nothing. **Scan both blocks on
every page**, and treat "no pin" on a page you know serves the form as a
bug in the scan rather than a fact about the page.

**The set is 17 pinned BLOCKS across 85 pages as of 18 Sept**, up from 15
across 83 two days earlier — `/aeo-new` and `/ai-demo-old` appeared, and
`/ai-demo` moved head-ward. Counting pages is the wrong unit; count
blocks.

**AND RETRY THE READS.** A dry run over 85 pages hit `429 Too Many
Requests` on four of them and skipped them with a printed warning. On a
dry run that is noise; on an apply run it is a stale pin nobody will ever
notice. Back off and retry, sleep between pages, and make a read failure
fatal rather than a line of output.

**THE AUTHORITATIVE SET IS 15 PAGES, NOT 13 — CORRECTED 17 SEPT 2026.** This
said 13 and it was wrong by two: `/aeo` and `/ai-crm` both carry
`gushwork-form-popup.js` and neither was in the list or in the `curl` sweep
below. They were found only by reading all 83 pages' footers through the API,
which is the sweep this section already told you to prefer and which nobody had
actually run. A count written down here is a claim with a shelf life; the API
scan is the measurement. **Run the scan, do not trust this number either.**

**THE API DOES THE WHOLE JOB, and that is worth knowing before you start.** The
MCP tool only accepts footer content inline, so a repin through it means
re-typing each page's entire custom-code block — 13k to 26k characters — to
change 40. With a site API token (Site settings → Apps & integrations → API
access; scopes: Custom Code, Pages, Sites, all read+write) it is a scripted
read-replace-write over exact bytes:

    GET /v2/pages/{page_id}/custom_code/freeform          -> [{location, content}, ...]
    PUT /v2/pages/{page_id}/custom_code/freeform/{footer} -> {location, content}

The PUT is location-scoped: writing the footer leaves the head block untouched,
verified. There is no PUT on the collection path, only on `/{location}` — the
collection path answers 404 for every write method, which reads as "no access"
and is not.

**EDITING PAGE ELEMENTS HEADLESSLY: WHAT WORKS AND WHAT DOES NOT.** Learned
on 18 Sept 2026 adding the about-business textarea to `/ai-crm`. The MCP
Designer tools can create the element and most of its properties, but two
form-field properties are Designer-only and will silently stay at Webflow's
defaults:

| Property | Headless? |
|---|---|
| element + position + classes (`data_element_builder`) | yes |
| DOM id (`set_settings` key `domId`) | yes |
| custom attributes, incl. **`maxlength`** (`set_attributes`) | yes — overrides Webflow's own, no duplicate |
| field **Name** (`set_settings` key `name`) | **stores but never renders** |
| **Placeholder** | **not settable at all** — *"not applicable to this element"* |

**The `name` one is the trap: the write succeeds, `get_settings` reads the
new value straight back, and the published HTML keeps `name="field"`.**
Verified across two republishes and five minutes of polling, so it is not a
compile race. Stored is not rendered. Do not trust a read-back here.

**Placeholder is the one that matters**, because the form script turns it
into the visible floating label — a field created headlessly shows
**"Example Text"** to real visitors until somebody opens the Designer. Set
it there, or do not create form fields headlessly at all.

**AND THE TEXTAREA NEEDS PAGE-LEVEL CSS THAT DOES NOT COME WITH IT.** The
float-label wrapper centres its label vertically, which is right for a
one-line input and wrong for a tall textarea — the label floats in the
middle of the box. `/ai-demo` has four rules in its **head** custom code
pinning it to the top and fixing the padding; they must be ported to any
page that gains the field. `/ai-crm` went live without them and looked
broken.

**AND SWEEP EVERY PAGE, not just the two you changed.** The pin lives in a
`<script src>`, and Webflow lets a *page* carry its own script tag that a
Project-Settings republish never touches. Two were found stale on 10 Sept, both
invisible from inside this repo:

```bash
for p in /demo /ai-demo /aeo /ai-crm /start /start-now /lead-gen /seo-leads \
         /consulting-lead-generation /consulting-seo-services \
         /financial-services-lead-generation /financial-services-seo \
         /manufacturing-lead-generation /manufacturing-seo-services; do
  echo "$p -> $(curl -s "https://www.gushwork.ai$p" | grep -oE 'gushwork-api@[0-9a-f]{7,40}' | sort -u | tr '\n' ' ')"
done
```

Anything not on the SHA you just pinned is serving different code to real
visitors. A page with **no** `@sha` at all is worse than a stale one: jsDelivr
then serves the default branch best-effort, so what a visitor gets depends on
that CDN's cache and drifts on its own.

**Pin both files to the same SHA**, even when only one of them changed. The bytes
would be identical either way — a commit SHA names a snapshot of the whole repo,
so asking for an untouched file at a newer SHA returns the same blob — but pinning
them together is the only thing that records that the pair was *tested* together.

**Confirming the swap actually took**: load the page and read the console banner.
`/demo` logs `Form initialised v…`, the Ads page logs `… (Google Ads)`. If the
version there is not the one you just merged, Webflow is still serving the old
pin — the deploy is not done, however green this repo looks.

**THE BANNER IS ONLY A CHECK IF YOU BUMPED THE VERSION, and on 16 Sept 2026 it
was not.** The phone-country fix shipped without touching the version string, so
both files read `v5.13.0` before AND after — the banner was identical on the old
pin and the new one. Anyone following the paragraph above would have read
v5.13.0, concluded nothing had changed, and been wrong in whichever direction
they guessed. A confirmation step that returns the same answer either way is not
a confirmation step.

**So bump the version on EVERY form change**, even a one-line one. It costs
nothing and it is the only thing that makes the banner mean something. The
version lives in the `Form initialised v…` string in both files and must move in
both, like everything else in the fork.

**THE BUMP IS FIVE EDITS, NOT THREE — CORRECTED 22 SEPT 2026.** This said
three and it was wrong by two, because the version does not live only in the
banner: each form file carries it in a **header comment at the top** as well.

| Where | Count |
|---|---|
| `Form initialised v…` banner, one per form file | 2 |
| Header comment at the top of each form file | 2 |
| The pinned assertion in `tests/test-partnerstack.js` | 1 |

`test-partnerstack.js` asserts the banner reads the current version in both
files. Bump the files without bumping the assertion and the bar goes red on
`main` — which has happened here before, and sat there for days as a red bar
nobody read rather than as a caught bug.

**The two header edits are caught by a DIFFERENT test**, and that is the only
reason this correction exists: `test-ads-parity.js` asserts *"init banner
agrees with the header"* in each file, so bumping the banner alone fails with
`header=v5.16.0 banner=v5.17.0`. Found on the v5.17.0 bump by following this
very paragraph and getting a red bar. Do all five in one commit.

**And when the version did not move, these two are what actually discriminate:**

1. **The SHA in the page HTML** — `curl` the page and read `gushwork-api@<sha>`.
   Exact, and it is the thing you just changed.
2. **A distinctive string in the file that SHA serves** — fetch the jsDelivr URL
   and grep for something only the new code has. For the phone fix that was
   `initialCountry: 'auto'` against the old `initialCountry: 'us'`.

Check 2 matters on its own: check 1 only proves the page asks for the right
bytes, not that jsDelivr can serve them. Fetch the new SHA BEFORE editing any
page — a pin to a SHA jsDelivr 404s takes the form off every page at once.

**`publish_site` IS ASYNCHRONOUS, and the first sweep after it will lie.**
Webflow returns success immediately and then compiles; on 16 Sept the live pages
still served the old HTML for about two minutes after the call returned. Poll the
page until the SHA flips rather than reading one red sweep as a failed publish
and starting to undo things.

**Publishing ships the WHOLE SITE, not just your pages.** Single-page publishing
needs Enterprise, which this site is not on — the same entitlement that blocks
branching. So a republish also releases anything anyone else has staged in the
Designer since the last publish. Check `lastUpdated` against `lastPublished` on
the site record first, and if they differ, ask whose work is in there before
publishing.

**`backfill-sf.js` is a kept tool, not dead code.** It re-syncs leads to Salesforce
after a broken connection or outage. Its `/admin/backfill-sf` route is deliberately
*not* mounted in `index.js` — it should only run when someone decides to run it.
Mount it temporarily when a recovery is needed, then remove the route again. Don't
delete the file.

## Tables

- **`leads`** — one row per form session, upserted as the visitor progresses.
  A row here means someone reached at least step 1 (entered an email).
- **`form_sessions`** — one row per page load. Kept separate on purpose: writing
  page loads into `leads` would break every query that assumes a row means someone
  reached step 1. Join on `session_id` when you want a funnel.
- **`lead_magnet_leads`** — the LP funnel. Never joined to `leads` at write time.
- **`enrichment_data`** — Apollo responses.
- **`form_page_views`** — one row per `/session` call, i.e. per load of a
  form-bearing page. The detail behind `form_sessions.hits`, which is only a
  counter. **Named for its scope on purpose:** `gushwork-form.js` is on 16
  pages only, so these are loads of form pages, not of the site — the
  homepage and `/pricing` have zero rows here while hundreds of leads arrived
  at the form from them. **No referrer column in v1**, deliberately: the only
  referrer in the payload is first-touch and identical on every hit, so
  storing it per page view would repeat one value down the column and read as
  a per-hit fact it is not. `source` is `NOT NULL` and today only ever
  `'session_route'` — see `docs/OPEN-ITEMS.md` for why anything
  client-reported must never share that value.
- **`leads.non_icp_blocked` / `non_icp_reason`** — the non-ICP block. **NOT
  `disqualified`**, deliberately: that column means exactly one thing (the
  prospect self-declared B2C/Mixed) and is clearable by the "actually we're
  B2B" button, which 47% of leads press. `non_icp_reason` holds the matched
  brand domain, e.g. `kw.com`. **Blocked leads are counted in every headline
  number** — they are still leads — and are marked rather than hidden on All
  Leads. The dedicated surface is the **Blocked** tab. Mirrored to
  `gw_form_leads` for the dialer, which does not yet read them (see
  `docs/tickets/non-icp-v1-block.md` OPEN ITEMS #1).
- **`non_icp_domain_verdicts`** — the model layer's cache. One row per
  registrable domain, keyed through `partnerStackCustomerKey` like
  everything else here. **The model is never asked at request time**:
  `/submit` reads this table and nothing else, and a miss is "we could not
  check", which fails open. Caching per domain is also what makes the block
  deterministic — a model asked twice can answer twice differently, and
  `temperature` cannot be pinned on current models (it is rejected). **Two
  TTLs**: a real verdict lasts 180 days, a failure row six hours, so a brief
  outage cannot pin a domain to "could not check" until spring.
- **The model layer's dashboard surface is `/monitor/non-icp` and the
  **Model** tab** — separate from **Blocked**, deliberately. Blocked means
  one thing, "we turned these people away", and a flagged-not-blocked lead
  is not that. Four panels: a five-state ladder that is mutually exclusive
  and exhaustive and **sums to the lead total**, per-industry actions read
  from `NON_ICP_BUSINESS_TYPES` rather than restated, every decision with
  its evidence quote, and the scrape blind spot. The join from lead to
  verdict is done **in JavaScript**, through `nonIcpCandidateDomains`, so
  there is no second domain normaliser beside `partnerStackCustomerKey`.
  The scrape panel reports **latest outcome per domain, never a historical
  rate** — `nonIcpWriteVerdictRow` is `ON CONFLICT DO UPDATE`, so a domain
  that failed and later succeeded overwrites its own failure — and it says
  so on screen, not only in a comment.
- **`leads.non_icp_source` / `non_icp_checked_at` / `non_icp_llm_flagged`** —
  provenance for the block. `non_icp_reason` holds a domain for **both**
  mechanisms, so without `non_icp_source` a reader cannot tell a string
  comparison they can re-derive from a model verdict they cannot.
  `non_icp_checked_at` closes OPEN ITEM #3 and is **only stamped when
  something was actually decided** — never for `check_failed` or `disabled`,
  because an inferred timestamp in an observational column reads as a
  measurement to the next person. **`non_icp_llm_flagged` is NOT
  `non_icp_blocked`**: a flagged lead in flag-only mode books, goes to
  Salesforce and gets dialled exactly as today. Folding the two together
  would make every existing guard on `non_icp_blocked` start refusing leads
  nobody decided to refuse — the V1 incident, arriving one column earlier.

  **FOUR SOURCE VALUES SINCE 23 SEPT, NOT TWO, AND EVERY CONSUMER THAT
  COMPARED AGAINST THE LITERAL `'llm'` ANSWERED WRONG FOR THE NEW ONES.**

  | value | means |
  |---|---|
  | `domain_list` | the brand list |
  | `llm` | the model, having read the page |
  | `llm_name_only` | the model, having read only the domain name |
  | `llm_late` | a verdict that landed AFTER `/submit`, stamped by the recheck sweep. The lead row carries this; the verdict row under it is `llm` or `llm_name_only` |

  A null is a pre-feature row and means the brand list, which was the only
  mechanism that existed then.

  **Ask `nonIcpSourceIsModel(src)`, never `=== 'llm'`.** Adding two values
  silently re-scoped six readers: `nonIcpSourceShort` labelled a model
  block **"Brand list"** to an SDR and the sentence under it claimed the
  domain was on a list it is not on, and all three Model-tab buckets
  counted an `llm_late` block under the brand list. A test now forbids the
  bare literal appearing anywhere. Same shape as "a second column that
  means we rejected this lead is not additive" below — arriving as a
  second enum value instead.
- **`leads.ip_address` and the nine `ip_*` columns** — where the visitor
  actually was, from their IP. Added 23 Sept. **Different from
  `enriched_city` / `enriched_state` / `enriched_country`**, which are
  Apollo's record of where the COMPANY is and cover 44% of leads. On the 22
  Sept lead those read Woburn and this read Boston — both correct, ten
  miles apart. The lead panel puts them under separate headings for exactly
  that reason.

  `ip_address` **is personal data**, unlike everything else on that table
  except the contact fields. **Kept indefinitely, decided 23 Sept** — the
  same treatment every other column gets, but a decision rather than a
  default, and a deletion request has to clear this and the mirror.

  **The mirror gets the PLACE, not the ADDRESS.** `gw_form_leads` has
  `ip_city` / `ip_region` / `ip_country` / `ip_timezone` and no
  `ip_address`: an SDR benefits from the city and the timezone, nobody
  across the WAN has a use for the IP itself, and copying personal data
  into a second database without a reason for it being there is how it ends
  up somewhere nobody remembers.

  `ip_checked_at` is stamped **only when a lookup decided something**,
  never for a failure or a skip — the same rule `non_icp_checked_at`
  follows.
- **`leads.ps_signup_recheck_at`** — when a verified PartnerStack conversion
  was last RE-checked, as opposed to `ps_signup_verified_at` which is when it
  was first seen to exist. Two observations, two columns.
- **`lead_field_changes`** — append-only log of the seven identity fields
  (`email`, `company`, `website`, `phone`, `first_name`, `last_name`,
  `sell_to`) changing on a lead row, because both upserts are last-write-wins
  on 35 columns and the old value is otherwise gone. A row is written only
  when old and new are both non-null and **differ** — a first set is not a
  change. `booking_uid_present` is read from **before** the upsert, so it
  answers "had they already booked when they changed it?".

---

## Definitions

Every number on the monitor dashboard must conform to this section. Where a number
deliberately departs from it, the label on screen has to say so in words a
non-engineer reads correctly. Added because the dashboard had accumulated numbers
whose labels and calculations disagreed — a chart called "sessions" that counted
leads, a tooltip claiming two different populations were identical.

### The four nouns

**Session** — one row in `form_sessions`. One visitor arriving on a form page.
The id lives in `sessionStorage`, so a landing-page → `/demo` journey in one tab is
**one** session with `hits` incremented, not two. Keys on `form_sessions.created_at`
(first seen). Session recording only began **21 Aug 2026, 10:32 UTC**; there is no
session data before that and queries must say "not tracked" rather than 0.

**Lead** — one row in `leads`. A session that got as far as entering an email
(step 1). Keys on `leads.created_at`. One person can have several leads. A lead is
never deleted, so counts only go up.

**Completed** — `leads.completed = true`. Keys on **`submitted_at`** where a time
is needed.

**`completed = true` does NOT mean "submitted the form", and `submitted_at` is
not set everywhere `completed` is.** Seven branches write one or both. Read this
table before using either column as a proxy for the other — the previous version
of this caveat named only one of the two shapes below, and that omission is what
misled a reader on 5 Sept 2026 with the doc open in front of them.

| Branch | `completed` | `submitted_at` |
|---|---|---|
| `/partial` (~7632) | `false` on insert, and **absent from the conflict clause** | **never written** |
| `/submit` (~7760) | `true` | `NOW()` |
| `/booking-confirmed` (~7869) | `true` | **left alone** |
| `/booking-confirmed-webhook` (~7945) | `true` | **left alone** |
| `/booking-confirmed-webhook` safety net (~7972) | `true` | `NOW()` |
| `/booking-confirmed-webhook-rh` (~8247) | `true` | **left alone** |
| `/booking-confirmed-webhook-rh` safety net (~8283) | `true` | `NOW()` |

`/submit` is the **only** branch that means "this person filled the form in".
Note also that `/partial` leaving `completed` out of its conflict clause is
load-bearing: it is why a visitor who submits and then goes back to edit step 1
does not lose the flag on Railway.

That produces **two** populations that are `completed` without being form
completions, and they are not the same shape:

1. **`completed = true` with `submitted_at IS NULL`** — reached step 1, dropped,
   then booked through a link later. One of the three booking `UPDATE`s set
   `completed` and left `submitted_at` alone. **6 rows today, all 6 booked, all
   of them form traffic** (`prefill_source` null or `url_param`), e.g.
   `aasnj@meta.com`. **This is the shape the old caveat did not mention**, and it
   is the one that breaks "completed implies submitted".
2. **`completed = true` with `submitted_at` set, never touched the form** — the
   two safety-net `INSERT`s, for someone who booked with no form row at all.
   **10 rows today, all `rh_webhook`, all with `submitted_at` set.** The old
   caveat described these correctly.

So:

- **"Did this person fill the form in?"** → `submitted_at IS NOT NULL`. Never
  `completed`.
- **"Which stage is this lead in?"** → the stage ladder above, which uses
  `completed IS TRUE` and is correct as written.
- They are **not** interchangeable, in either direction.

Both populations are real leads and neither is a form completion. Counting them
under "Completed" on the dashboard is deliberate and documented; using
`completed` to mean "submitted" in a *query* is a bug, and it is why
`backfill-sf.js` selects on `submitted_at IS NOT NULL`.

**On the AWS mirror, `gw_form_leads.submitted_at` is a different thing entirely
— a sync timestamp, not a submission time.** Its presence is meaningful, its
value is our clock. See the mirror traps in `docs/partnerstack.md`.

**Booked** — `leads.booking_uid IS NOT NULL`. Keys on **`booked_at`**, falling back
to `created_at` where `booked_at` is null (rows predating that column). Always use
`COALESCE(booked_at, created_at)` when ordering by when a booking happened —
comparing a null `booked_at` yields null, the comparison quietly fails, and the row
is counted as un-booked.

### The stage ladder

Exactly four stages, **mutually exclusive and exhaustive**, resolved in this
priority order. Any lead is in exactly one, so the four always sum to the total:

1. **Booked** — `booking_uid IS NOT NULL`
2. **Disqualified** — not booked, and `disqualified IS TRUE`
3. **Completed** — not booked, not disqualified, and `completed IS TRUE`
4. **Step 1** — everything else

Use `IS TRUE` / `IS NOT TRUE`, never `= true` / `= false`: a null flag on an old row
must land in a stage rather than vanishing from all four. The stage filter, the
stage badge and any stage count all read from this one ladder. If you add a stage,
it goes in the ladder or it doesn't exist.

### The PartnerStack lifecycle ladder

Exactly eight states, **mutually exclusive and exhaustive**, one per partner
**domain**, resolved in this priority order. Every counter on the Partners tab
is a `COUNT FILTER` over this one column, so the numbers cannot disagree with
each other. Same rule as the stage ladder above: if you add a state, it goes in
the ladder or it does not exist.

1. **qualified** — `ps_qualified_sent_at IS NOT NULL`
2. **qualification_failed** — `ps_qualify_failed_at IS NOT NULL`
3. **conversion_failed** — `ps_signup_failed_at IS NOT NULL` **and not since converted**
4. **demo_done_not_qualified** — earliest `start_time` is in the past
5. **awaiting_demo** — `booking_uid IS NOT NULL`
6. **converted** — `ps_signup_sent_at IS NOT NULL`
7. **skipped** — `ps_signup_skipped_reason IS NOT NULL`
8. **conversion_pending** — everything else

**The order is not the progression order, deliberately.** A success always
outranks its own failure, because a domain that failed and later succeeded is
fine. But an *unresolved* conversion failure outranks every later stage it
blocks: a domain whose conversion never landed can never be qualified, so
showing it as "awaiting demo" would hide the only fact worth acting on. That is
exactly what happened on 4 Sept — a 400 on the qualification released the claim
correctly and nothing on any card moved.

A domain matching two states takes the **first** match, never a blend.

**Keyed by DOMAIN, because that is the unit PartnerStack pays on** — one
conversion and one qualification per customer key, ever. Leads with **no usable
domain** cannot be keyed that way, so they are counted **separately, as leads**,
in their own field, and the UI says "leads, not companies" on that chip. Folding
them into a domain count would reintroduce the mixed-unit arithmetic that made
the old counters irreconcilable.

The query is bounded to `PS_LADDER_WINDOW_D` (180 days, matching the Salesforce
lookback) **except for unresolved failures, which are included regardless of
age** — otherwise a domain that failed months ago and was never fixed would
silently drop out of "Needs attention", the one number that has to be complete.

**conversion_failed and qualification_failed are the two red states** and are
summed into "Needs attention" — the only number on the tab that means someone
has to act today. Both also fire a Slack alert at the moment of failure, via
`alertOps` with its normal cooldown, because a state you have to remember to
check is half a fix.

### Bookings: two different questions, and they are not interchangeable

These look like one question and are not. Collapsing them onto a single rule
breaks whichever one you didn't have in mind.

**1. "Is this person an SDR target?" — no time comparison.**
Does any lead row sharing this `lower(email)` have a `booking_uid`? If yes, they
have a booking; don't call them. When they booked is irrelevant — an SDR ringing
someone who already has a call on the calendar is wrong whether that call was
booked yesterday or in May. Used by the SDR List and by "No booking yet".

**2. "Should this session get a drop-off recovery email?" — the time comparison is
required.** `NOT EXISTS (a booking by this email with booked_at >= this session's
created_at)`. A booking that *predates* the session does not resolve that session's
drop-off: the person came back, started again, and dropped again. Suppressing on
"has ever booked" would silently kill legitimate follow-ups. This is deliberate,
dates from a May 2026 fix, and lives at the recovery cron (`index.js` ~4499) with a
comment saying so. **Do not "unify" it.**

**3. "Recovered bookings"** is a third shape — a completed session with no booking,
followed *later* by one — and uses `COALESCE(booked_at, created_at) >= l.created_at`.

Note the asymmetry between 2 and 3: the cron compares bare `booked_at` and does
**not** COALESCE, because production has zero rows with a null `booked_at` and
there is nothing to defend against. Recovered bookings does COALESCE because it
reads the full history including rows that predate the column. Both are correct for
what they ask.

The dashboard's "Pending recovery" card reports question 2's population, so its
label says what it counts and no longer claims to mirror the cron's exact rule.

### Default population for dashboard numbers

Unless a label says otherwise:

| Question | Default |
|---|---|
| All leads, or completed only? | **All leads.** Filter to completed only where the label says "completed" |
| Deduped by email, or by session? | **Headline numbers are people** — `COUNT(DISTINCT lower(email))`. Session counts are legitimate but must be labelled "sessions" every time they appear |
| Named exception | **"Form entries per day"** on Overview is a deliberate ROW count — see below |
| Dedup key | `lower(email)`, always. Never raw `email` |
| Internal / test addresses | **Included.** See below |
| Webhook-origin leads | **Included**, except `/monitor/funnel` |

**Internal and test addresses are currently INCLUDED in every `leads` number.**
`ELV_EXCLUDED_DOMAINS` (`gushwork.ai`, `test.com`, `example.com`, `example.org`) and
the `b@g.ai` test address are excluded from ELV health and from alerting, and from
nothing else. So Overview, All Leads, SDR List and Duplicates all count our own
testing. This is a known distortion, not a decision anyone made — flag it, don't
quietly "fix" it, because excluding them moves every historical number at once.

**853 session rows come from two pages that were never meant to have the
form, and they are staying.** `/careers` (629 non-bot) and `/meeting-booked`
(224) carried the form script by mistake until 10 Sept 2026. `/careers` has no
form elements and never produced a lead; `/meeting-booked` produced exactly one
row, in March, by reading its own post-booking redirect's query string back in
for somebody who had already booked. Both tags are being removed.

They are **5.7% of the Sessions card**, which reads 14,848 with them and 13,995
without — so Sessions to Step 1 shows **4.98%** where the form-pages-only figure
is **5.28%**. The rows are not deleted: they are true, somebody did load a page
carrying the script, and rewriting them would move every historical session
number at once to correct a 0.30-point error. Same trade as the internal
addresses above, and the population is closed and dated once the tags go.

Also biases the `partial` health row toward a **false red** — it reddens on
8+ sessions with zero leads in 2 hours, and these two contributed about four
such sessions per window that could never produce a lead. Safe direction, and
it ends with the tags. See `docs/OPEN-ITEMS.md` item 12.

**"Form entries per day" is deliberately a row count, not a people count.** It
counts rows in `leads` per ET day — everyone who reached step 1, undeduped, all
stages. That is a departure from the people-by-default rule and it is intentional:
the chart's job is daily inbound volume, and deduping by email would flatten the
repeat-attempt spikes that make a bad day visible. The label says "entries", not
"people" and not "sessions", so it reads correctly. Do not "fix" it to
`COUNT(DISTINCT lower(email))`. Proper session-based charting is separate work.

**Webhook-origin leads** (`prefill_source IN ('rh_webhook','cal_webhook')`) are
included everywhere except `/monitor/funnel`, which excludes them from step1,
submitted and booked and explains why at length. They never touched the form, so
they inflate any form-conversion rate from both sides. Left in deliberately.

**10 rows on the mirror as of 5 Sept 2026, and all 10 are `rh_webhook`** — the
Cal safety net has never fired once. The filter keeps `cal_webhook` because the
branch exists and could fire tomorrow, not because it has. Was recorded as
"9 rows" before 5 Sept; a count in a doc goes stale, so treat it as an order of
magnitude and re-run the query rather than quoting it.

### Timezone

**The dashboard is Eastern Time.** Use the IANA name `America/New_York`, never a
fixed offset — DST has to move on its own.

- Every displayed timestamp and every day boundary is ET.
- The Postgres session timezone is `Etc/UTC` (confirmed). So a bare `date_trunc`
  or `::date` buckets in UTC, which is **not** a correct day boundary for this
  dashboard. Write `AT TIME ZONE 'America/New_York'` explicitly.
- Never derive a calendar date in browser code from `getFullYear/getMonth/getDate`
  — those read the viewer's laptop. Format through
  `Intl.DateTimeFormat` with an explicit `timeZone`.
- `/monitor/funnel` is the one deliberate exception and keeps UTC day buckets. Its
  go-live and coverage reasoning is anchored to a UTC instant, and re-bucketing it
  would silently change which day counts as partially covered. Its comments say so.

### Health checks fail LOUD

"A lead is worth more than a verdict" governs the lead path: checkers fail open.
**Health checks are the opposite and must be, because no lead depends on them.** A
health probe that cannot reach its dependency reports red, never green and never
"unknown-styled-as-fine". A green badge means "verified working, just now". If it
can't verify, it must not be green.

---

## Things that will bite you

**A SOURCE ASSERTION CANNOT SEE RUNTIME BEHAVIOUR, and on 11-12 Sept that
cost THREE production breaks in one night.** This is the repo's oldest lesson
arriving in three new disguises, so the disguises are worth naming:

| Break | Why the suite could not see it |
|---|---|
| `/monitor/metrics` 500, blank Overview | A **temporal dead zone** — `const` read above its declaration. `node --check` passes; a TDZ violation is a runtime error, not a syntax one |
| Blocked tab: `leadRowsHtml is not defined` | A **scope** error. The function was declared inside `loadLeads`, so All Leads could see it and Blocked could not. Valid syntax, correct text, wrong scope |
| A Meta guard that never fired | An `if (false)` around it moves no source offset, so every ordering assertion still passed |

**A CONFIDENT ZERO PASSES EVERY STRUCTURAL CHECK.** On 15 Sept 2026 the
Model tab's standing-cache summary rendered **"0 companies classified all
time — 0 judged, 0 currently unreadable"** directly above a table listing
224 home services, 164 insurance and 141 real estate. `mdlScrapeHtml` read
`d.scrape.cache`; `cache` is a **top-level** field of the payload. One
reader wrong, one right, in the same feature.

Every check passed. The element was painted, the content was non-empty, it
was not an error string, it parsed, the loader threw nothing. The number was
simply wrong — and a wrong number on a dashboard is the failure mode the
dashboard exists to prevent.

**So: anywhere a number is displayed, assert it MATCHES THE PAYLOAD, not
that it exists.** Read the value back out of the painted HTML and compare it
to the object that was handed in, with fixture values distinctive enough
that they cannot match by accident. `tests/test-non-icp-routes.js` does this
for every number on the Model tab. "It rendered" is the same class of claim
as "we asserted it alerts" — it is one level short of the thing you actually
care about, and the gap is where this bug lived.

**The fix is not "write more assertions", it is "drive the thing".**
`tests/test-non-icp-routes.js` boots the real app and (a) drives every
`/monitor/*` route for a 200, and (b) **evaluates the dashboard's inline
script in a stubbed DOM**, asserts every shared helper is defined at TOP
LEVEL, calls every tab loader, and **records what each table painted**.

It also stubs **Leaflet**, and that was not optional: `visDrawMap` returns
at its first line when `L` is absent, so a mutation that stopped the map
naming its unmappable places SURVIVED until the stub existed. The degraded
path alone tested almost nothing.

That last part is the one that matters and it is the least obvious. Each
loader wraps its render in a `try/catch` that paints the error into the
table — so a scope error arrives as a tidy "Could not load:" message, not a
crash, and **a probe for a thrown error misses it entirely.** Assert on what
the user sees.

**THE WEBFLOW DESIGNER SERVES A STALE TREE AFTER A HEADLESS WRITE, AND
THAT MAKES A SNAPSHOT LIE.** The MCP data tools write through the API; the
open Designer session renders from its own in-memory copy and does not
reload. On 15 Sept 2026 `element_snapshot_tool` returned **"Element not
found"** for markup that had just been created and could be read back
through `query_elements` in the same minute. Worse than the error is the
quiet case: after the element exists, a **style** change made through the
API kept rendering at its old value, so the snapshot showed a stale layout
with no error at all.

`switch_page` to another page and back forces the reload; `select_element`
on the new element confirms the canvas can see it. Do that before every
snapshot you intend to treat as evidence.

This is the repo's oldest lesson wearing a picture instead of an
assertion. "It rendered" and "I photographed what is actually there" are
different claims, and the gap between them is a screenshot of the previous
state pasted under the word *verified*. A snapshot is only evidence if the
canvas was refreshed after the last write.

**A GUARD ADDED TO THE OBVIOUS SITE MISSES ITS SIBLINGS. This happened three
times in one night, to the same column.** `leads.disqualified` has **fourteen
call sites** across routes, crons, sweeps, health checks and metrics queries,
and **no single grep reaches them all** — they are spread over `/submit`, the
recovery cron, two PartnerStack sweeps, the SDR list, three Overview cards, a
work queue, the stage ladder and a per-email history subquery.

Introducing `non_icp_blocked` — a second column meaning "we rejected this
lead" — turned **every** existing `disqualified` guard into half a guard
overnight. Found in three waves: the PartnerStack conversion (cost money, fired
in production), then the SDR list and recovery health, then three Overview
counters a day later.

**COUNTING PREDICATES IS NOT DECIDING THEM, and that mistake shipped too.**
The first audit pinned the *number* of `disqualified` predicates at 13. The
number was correct and it let two of the three waves through.

`tests/test-non-icp.js` §10b is now a real audit: it resolves every predicate
to its enclosing route, reads the **enclosing query** rather than a byte
window, and requires each to be **either guarded or named in a `DELIBERATE`
list with a written reason**. Two are deliberately exempt — the
`/monitor/metrics` disqualified counters, and `/monitor/leads`' stage ladder
and `prior_disqualified`.

**Run that audit whenever you touch either column.** A new predicate added
without a decision fails the suite, which is the only mechanism in this repo
that reaches all fourteen sites.

**The generalisable rule: a second column that means "we rejected this lead"
is not additive.** It silently re-scopes every consumer of the first one. The
work is not "update the guard I am thinking about", it is "enumerate which
predicates on the old column now answer only half the question".

**THE LATE-VERDICT SWEEP, AND WHY THE RACE WAS THE WRONG FRAME.**
`runNonIcpBookedRecheck` runs at boot and every 5 minutes over leads booked
in the last 48 hours, re-reads `non_icp_domain_verdicts`, and stamps
anything that now resolves blocking.

**It exists because the cost is incurred at the MEETING, not at the
booking.** Measured: booking→demo median **35 hours**, 10th percentile
**3.3 hours**, only 3.9% of demos within an hour of booking. There is no
2.6-second race to win — there are hours. The 4s calendar hold in the two
form files covers the seconds; this covers everything slower, including a
19.6s scrape, a cold cache after a deploy, and the name-only fallback.

**IT MARKS AND TELLS A HUMAN. IT NEVER CANCELS.** Authorised shape,
Darshil, 23 Sept 2026. Cancelling on a model verdict with nobody in the
loop would be the most aggressive action in this codebase, and the person
is already holding a confirmation email. `slackNonIcpLateBlock` posts
direct rather than through `alertOps`, whose cooldown keys on
severity+source+title and would fold a second person into a counter.

**Only BLOCKING verdicts.** A Meta-only flag is already handled at booking
time by `nonIcpScheduleSuppressed` and does not need a human — those
people keep their slot by design, so waking somebody for one trains them
to ignore the channel. The `non_icp_blocked` stamp is its own cursor, so
nothing is alerted twice and no new column was needed.

---

**META WAS SENT THE WHOLE `x-forwarded-for` HEADER, NOT THE CLIENT IP, AND
IT WAS A LIVE BUG.** Every Meta call site passed
`req.headers['x-forwarded-for']` raw. Railway sends **two entries**, proved
in production on 23 Sept:

```
entries=2 | first=97.208.126.x | last=152.233.40.x
req.ip=152.233.40.x | sent_to_meta_would_be=97.208.126.x
```

So `client_ip_address` carried `"97.208.126.x, 152.233.40.x"` — a comma
list where an address belongs. **`readPartnerStackRequestContext` had
already fixed exactly this, ~4,000 lines above, with a comment explaining
why.** One integration was fixed and the other was not.

**Normalised in `sendEvent`, not at the call sites.** There are six and a
seventh will exist one day; a choke point every event already passes
through cannot be bypassed by a new caller. `normalizeClientIp` drops junk
rather than forwarding it, so the key is absent from `user_data` instead of
holding a value that matches nobody.

**`req.ip` IS ALSO THE WRONG ONE.** It resolves to the LAST entry, a proxy
hop, so the `|| req.ip` fallback would have been wrong too had the header
been absent. `clientIpOf(req)` is now the ONE definition and PartnerStack
reads it too.

**META NEVER COMPLAINS, SO NOTHING DOWNSTREAM CAN CATCH THIS.** Checked
against the live API: a clean IP, a two-hop list and a three-hop list each
returned HTTP 200, `events_received` 1, and an **empty `messages` array**.
That array was also being discarded — only `events_received` was logged —
so a malformed field could be wrong for months while every line read like a
success. It is now printed when non-empty.

---

**THE VISITORS TAB, AND THE THREE TILE PROVIDERS IT TOOK.**
`/monitor/visitors` plus the **Visitors** tab: coverage, repeat addresses,
places, networks, timezones, and a map toggle.

**Repeat addresses is what earns the tab.** The other panels could live on
a lead row; "which address sent more than one lead" cannot — it is a
question about the set. When `pt.lancon@gmail.com` was investigated on 22
Sept there was no way to ask it.

**A 200 IS NOT A WORKING TILE, and that cost two deploys.**

| provider | what happened |
|---|---|
| `tile.openstreetmap.org` | 200 to curl, **403 in a browser** — usage policy blocks embeds by Referer |
| `basemaps.cartocdn.com` | 200, a real 6.5KB PNG — with **"API KEY REQUIRED" printed across it** |
| `services.arcgisonline.com` Canvas | clean, keyless, desaturated for data overlay |

Both failures were invisible to a status-code check and were settled by
**downloading a tile and looking at it**, at three zoom levels. Both are
named in a rejected list in `tests/test-non-icp-routes.js` with the reason.
If a fourth is ever needed, check it the same way before adding it.

**Circle AREA scales with lead count, never radius** — a radius
proportional to the count makes twice the leads look four times as busy.
Drawn largest-first so small circles land on top, with a white ring,
because eighteen same-coloured circles over the US north-east otherwise
render as one blob.

**Networks are merged by NAME in the browser.** The provider reports one
network under several domains: live data had "Verizon Business" twice, on
`verizonbusiness.com` and `frontiernet.net`, and "AT&T Enterprises, LLC"
twice. Four rows, two networks — a wrong number no label could fix. Merged
client-side so the API keeps reporting what the provider actually said.

**Geo is `ipwho.is`, NOT `ipapi.co`.** ipapi.co answers `RateLimited` on
the first request of the day from this machine and from Railway's egress
alike, so its free tier is not a tier. ipwho.is allows 1,000/day against
our ~41 leads/day, and returns more: an IANA timezone, and the ISP, ASN and
org domain that are the only corporate-versus-residential signal here.
`IP_GEO_URL` swaps it from the env, because a free geo service is exactly
the dependency that stops being free.

**The lookup NEVER touches the lead path.** Fire-and-forget after
`res.json()` in BOTH `/partial` and `/submit` — 41 leads a day reach step 1
and 11 never submit, so resolving only at submit would leave every drop-off
with no location. `/partial` fires repeatedly, so `finaliseIpGeo` does at
most ONE lookup per session, guarded in process and by `ip_checked_at`. The
ADDRESS is written on every call because it costs no network; only the
network half is rationed. **Two writes, not one** — a geo outage must not
lose the address too.

**THE MODEL LAYER'S OWN TRAPS.** Four, and the first is the one that
decides whether this feature is safe at all.

*One.* **Blocking is decided in CODE from the enum, never by the model.**
The model is only ever asked what the company *is*; `NON_ICP_BUSINESS_TYPES`
in `index.js` says which types block, and exactly two do. If the model could
return its own verdict, a page could talk its way out of a block in one
sentence — and, worse, talk somebody else into one. The output schema does
not even offer a `blocking` field, and a test asserts that a model-supplied
one is ignored.

*Two.* **Never key a block on the free-text `category`.** The warehouse
classifier's `category` column holds **65 distinct spellings** of real
estate and insurance — `Real Estate Tech`, `Real Estate / PropTech`,
`Software / Insurance Tech`. A block on `category ILIKE '%real estate%'`
turns a proptech SaaS company into a blocked lead. That is `paycompass.com`
one layer up, and it is why `business_type` is a closed enum declared to the
API as a structured output.

*Three.* **The page text is untrusted and the schema is not the defence.**
It goes in a user message inside `<untrusted_page_text>` delimiters, never
in the system prompt, markup stripped and capped at 12k chars. What actually
bounds the damage is the *direction* of the error: an injection that makes
us **not** block is the status quo, and the costly direction — a wrong block
of a real prospect — is one an attacker has no reason to aim at. So the
guardrails point at accidental false blocks: the confidence floor, the
known-customer bypass checked **before** any block, and a Slack post
carrying the verbatim evidence quote so a wrong block is visible in minutes.

*Four.* **The verdict is a CACHE READ at the moment of decision.** No
scrape, no model call, no network, and deliberately no timeout to fall
through — a lead who reaches the calendar because our own request was slow
is the failure this shape exists to prevent. Warming happens on blur, via
the `/non-icp-check` calls `gushwork-form.js` **already makes** for V1,
which is why **neither form file changes** and the Ads fork needs no port.

**The non-ICP block has three traps, and two of them look like working code.**

*One.* `partnerStackCustomerKey` **collapses subdomains** — it ends in
`registrableDomain`, so `agents.farmers.com` becomes `farmers.com`. Matching
only on its output makes every subdomain entry on `NON_ICP_DOMAINS`
(`agents.allstate.com`, `ft.newyorklife.com`, `agents.farmers.com`) permanently
dead while looking live. `nonIcpHostForms` keeps the full host alongside the
collapsed one and both are matched. Caught by a test, not by reading.

*Two.* **`non_icp_blocked` is STICKY in all three upserts**
(`leads.non_icp_blocked IS TRUE OR EXCLUDED.non_icp_blocked IS TRUE`), and that
is load-bearing rather than tidy. `/partial` fires repeatedly through step 1,
and the "actually we're B2B" button calls `savePartial(1)` again — **74 of the
84 known realtor and insurance leads reached the calendar through exactly that
button**. An `= EXCLUDED` assignment lets a realtor clear their own block by
clicking it. A test pins all three sites.

*Three.* **Never match a brand domain by substring.** Measured on 5,123 real
leads: substring caught 116 where exact-or-subdomain caught 110, and five of
the six extra were wrong — `paycompass.com` (a payments company, contains
`compass.com`), `charleslegalpl.com` (a law firm, contains `lpl.com`),
`theimagecreatornm.com`, `krevera.com` and `ceterainvestors.com`. All five are
pinned as negative fixtures in `tests/test-non-icp.js`.

**And the scope is narrower than "non-ICP" sounds.** V1 is national real-estate
brokerage and insurance carrier brands only. Financial advisors (Edward Jones,
LPL, Northwestern Mutual, Primerica, Cetera), mortgage and lending are **in
ICP by name in the Non-ICP doc** and are not blocked; nor are independent local
agencies and brokerages, of which we have ~70. The other four rule-6 industries
— restaurants, spas and salons, home services, print and sign shops — are not
covered by V1 at all and fire Meta normally.

**The three lists.** `WEBSITE_VERIFIED_REASONS` (line ~424), `RECHECK_WRITEABLE`
(~3386) and `RECHECK_PROTECTED` (~3398) must stay in sync with `gushwork-form.js`
SECTION 3C **and `gushwork-form-popup.js`, which carries its own copy of the same
lists** — see the fork note below. There's a warning comment above them. Adding a
website verdict means deciding its place in all three:

- **`WEBSITE_VERIFIED_REASONS`** → does Meta fire?
- **`RECHECK_WRITEABLE`** → can the historical recheck tool overwrite it?
  Anything meaning "we didn't get a real answer" stays **out**.
- **`RECHECK_PROTECTED`** → verdicts that depend on the lead's email and can't be
  re-derived from the domain alone.

**Backticks inside SQL comments break the file.** The SQL in this repo lives in
JS template literals, and the house style is a long `/* ... */` comment inside the
query explaining the incident behind it. A backtick in that comment — writing
`` `leads` `` or quoting an expression — **terminates the template literal**, and
the error surfaces as `SyntaxError: missing ) after argument list` pointing at the
`pool.query(` line, not at the comment. Four of these happened in one sitting. Use
plain words inside SQL comments, and run `node --check index.js` before committing.

**"What are you looking for?" ON `/demo` ONLY — and `product` vs
`product_interest` are two columns because they answer two questions.** Two mandatory
checkboxes (`aeo`, `crm`) revealed after `sell_to` is answered. The values
in the markup ARE the product slugs, so the checkbox, `product_interest`,
the Salesforce picklist and the Meta `content_ids` are one vocabulary with
no mapping table to drift.

- **`leads.product` stays SINGLE-VALUED** — the routing slug. One calendar,
  one Meta event, one Salesforce picklist value. CRM wins a both-ticked
  selection. Widening it would break `Product__c` (a restricted picklist)
  and the three `product = 'crm'` equality predicates at once.
- **`leads.product_interest` holds what they ticked** — canonical: lowercase,
  known slugs only, deduped, **sorted**. `'aeo'`, `'crm'`, `'aeo,crm'`.
  **NULL means "we never asked"** and is the honest value for every page but
  `/demo`. Never backfilled.
- **Salesforce gets `product_interest ?? product`.** `Product__c` carries
  three values — `aeo`, `crm`, `aeo,crm` — added and verified against the
  live org on 15 Sept 2026.

**THE ROUTING SLUG AND THE EVENT SLUG ARE DIFFERENT FUNCTIONS.**
`resolveProduct` answers "which calendar, which Salesforce picklist value"
and must be single — `crm` for a both-ticked lead. `resolveEventProduct`
answers "what did we tell Meta this conversion was", and a both-ticked lead
is honestly **both**: `content_ids: ['aeo','crm']` on **one** event.

**One event, never two.** Deduplication is documented as cross-source only
(*"Does not deduplicate events when only using one event source"*), and every
active ad set optimises on conversion **count** — so two events would count
one person twice and corrupt exactly what they bid on.

**The Meta event must match `product_interest`, not `product`** — the same
rule `Product__c` already follows. They agree on every lead except the
both-ticked one, which is the only place the invariant is visible, and the
divergence test exists for that row.

**`predicted_ltv` is CONFIG: `META_LTV_AEO` / `META_LTV_CRM` /
`META_LTV_AEO_CRM`**, defaults 12000 / 5000 / **15000**, all PROVISIONAL.
Combined is 15000 and **not** the 17000 sum: it predicts what the *person* is
worth, where the sum would assert they buy both at full price with certainty.
An unreadable env var falls back to the default and warns once — a `NaN` in a
payload is worse than a stale number.

**`leads.meta_predicted_ltv` records what was actually sent**, NULL where no
Meta event fired. The config can move; without this there is no way back to
what was reported for a cohort, which is exactly what you need before
switching anything to value optimisation. **That switch is blocked on closed-won
revenue joined back to leads, not on the config** — see the value-optimisation
section in `docs/tickets/non-icp-v1-block.md`.

**`resolveProduct` TAKES THE SELECTION AND THE PAGE, AND BOTH THE COLUMN AND
THE META EVENT MUST BE RESOLVED FROM BOTH.** `buildEventData` read `page_url`
alone, which was correct only while product was a pure function of the page.
With a checkbox deciding it, a `/demo` lead ticking AI-CRM stores `crm` and
would fire `aeo` — the dashboard saying one thing and Facebook optimising for
another, **with no error anywhere**. `tests/test-non-icp-routes.js` drives
`/submit` across four selections and compares the bound column against the
`content_ids` that actually reached `graph.facebook.com`.

**An unknown `product_interest` loses the whole LEAD, not the field.**
`Product__c` is restricted; an unknown value is
`INVALID_OR_NULL_FOR_RESTRICTED_PICKLIST`, which `sfUnknownFields` does
**not** retry. `canonicalProductInterest` dropping unknown slugs is the only
thing between a tampered checkbox and a lost lead — `test-batch2.js` §24
executes it against 24 adversarial inputs and enumerates every slug subset
against the picklist, so adding a third product without adding its
combinations to Salesforce fails there rather than in production.

**THE B2C GATE FIRES AT THE NEXT CLICK, NOT ON SELECTION**, and the
progressive reveal depends on that. There is no change handler on the
`sell_to` radios that decides anything, `handleStep1Next` has exactly two
triggers, and `showStep('step-disqualified')` is reached from one statement
inside it — so the checkboxes are read one line above the gate in the same
call. Had it fired on selection, a B2C click would jump to the DQ step before
the checkboxes were visible and the CRM exception could never apply. Four
assertions hold it there, including one that the sell-to change handler never
calls `showStep`, `savePartial` or touches `disqualified`.

**Webflow markup this depends on, `/demo` only:** `#needs-wrap`, `#need-aeo`
/ `#need-crm` with `name="needs"` and `value="aeo"`/`"crm"`, `#needs-error`,
`#about-business-wrap`, and `data-rh-router-crm="6804"` on the form wrapper —
**and NOT `data-rh-router`**, whose absence is what keeps the 6138 AEO
default. No markup means no selection, the server resolves from the page as
before, and `product_interest` stays NULL. `/ai-demo` is untouched.

The on-screen heading is **"What are you looking for?"**, changed in Webflow
on 16 Sept. Nothing in the code reads it — it is recorded here only so this
file and the page agree.

**THE TWO ROUTER ATTRIBUTES ARE NOT INTERCHANGEABLE, and picking the wrong
one sends a page's traffic to the wrong team.** `rhRouterId()` reads
`data-rh-router-crm` ONLY when `wantsCrm()` is true, then falls through to
`data-rh-router`, then to 6138.

| Attribute | Applies | Use it on |
|---|---|---|
| `data-rh-router` | always | a page that sells ONE product |
| `data-rh-router-crm` | only when this visitor is a CRM lead | a page where the visitor decides |

So `/demo` carries `data-rh-router-crm="6804"` and no `data-rh-router` —
conditional, because the visitor decides there. `/ai-demo` and `/ai-crm`
carry `data-rh-router="6804"` — unconditional, because the page decides.

**A CRM PAGE MUST NOT USE THE `-crm` ONE.** `wantsCrm()` is true only when
the visitor ticked AI-CRM or arrived on a CRM campaign, and a CRM page has
neither the checkboxes nor, for most of its traffic, a campaign — so
direct, organic and brand visitors would fall straight past it to the AEO
team. That is what `/ai-crm` did until 18 Sept, when it had no attribute
at all. The plain attribute asserts a fact about the page; the `-crm` one
depends on detecting something about the person.

The attribute lives on the form wrapper, so it travels with the content
when `/ai-crm` is duplicated into `/ai-demo` — and both already hold the
same value, so that swap is a no-op for routing.

**HIDDEN BY A CLASS, NOT `display:none`, AND THAT IS LOAD-BEARING.** Both
wrappers carry `field-wrapper is-collapsible is-hidden`. Webflow cannot store
an inline style, so `style.display = ''` could never reveal anything — that
shipped in PR 75 and meant `#about-business-wrap` could not have appeared at
all. `setWrapHidden` toggles `is-hidden`; `needsVisible()` reads the class, so
it is correct from first paint rather than only after init.

`.field-wrapper.is-collapsible` is the animation: `overflow:hidden`,
`max-height:240px`, and a 180ms transition on max-height, opacity, transform
and margin-bottom. The hidden state is the three-class combo
`.field-wrapper.is-collapsible.is-hidden` — three classes so it beats the
two-class combos whatever order Webflow emits them in.

**The 240px ceiling is the one fragile number.** Measured: the needs block is
about 94px and the textarea wrapper about 110px, so it is roughly 2x headroom.
Grow past it and the content **clips silently** — no scrollbar, no error. A
taller ceiling is not free either: the visible content finishes expanding
before the max-height animation does, which reads as a dead pause. If a third
card or a taller textarea ever lands here, re-measure rather than just raising
the number.

**The cards NEVER wrap — they hold 50/50 at every width, like the sell-to
row.** `.radio-wrap.is-needs` is `flex: 1 1 0%` with `min-width: 0`, and the
row is the plain `.radio-holder`, whose default `flex-wrap` is `nowrap`. An
even share rather than `calc(50% - Npx)` because **the gap is not constant**:
`.radio-holder` is 14px at main and 8px at `tiny`, so any hardcoded half is
wrong at one breakpoint or the other. `flex-basis: 0` divides whatever is left
after the gap, whatever the gap is.

**A `min-width` here is a bug, not a safety net, and it shipped once.** A
200px minimum demanded 408px of row where 390px viewports only give 319px, so
the cards stacked on mobile. Measured at 390: the row is ~319px, each card
~155.5px, leaving ~99.5px of text after the 16px checkbox, its 12px gap and
28px of padding. "Custom AI CRM" at 12px is ~85px and fits on one line; the
10px subtitle wraps to two, exactly as the sell-to subtitles already do in
76.5px. At 320px each card is ~120px and the title wraps to two lines — still
renders, same as sell-to. There is no realistic width where wrapping helps.

The base `.radio-wrap` and `.radio-holder` are **untouched** — shared with 36
elements on other pages.

**THE `sell_to` GATE IS CLIENT-SIDE ONLY, and the CRM product is excepted
from it.** B2C or Mixed at step 1 sets `disqualified` and shows a terminal
step — in `gushwork-form.js` and `gushwork-form-popup.js`. The server
receives the boolean and believes it; it never decides. So "allow B2C on
the CRM product" could only be built in the two form files, as
`B2C_ALLOWED_PATHS`, matched against the pathname the same way
`resolveProduct` normalises it.

That makes **FOUR copies of the CRM path set**: `PRODUCT_PATHS` in
`meta-capi.js`, `index.js` importing it, and one in each form file.
`tests/test-batch2.js` section 21 lifts all three files and asserts they
agree. Add a product page to the catalogue without adding it to the forms
and the visitor is disqualified on a page the server has already decided
is CRM — nothing else in the repo would say so.

**AND THE REVERSE HAPPENED ON 18 SEPT 2026: a page live in Webflow and in
NEITHER list.** `/ai-crm` shipped carrying `data-rh-router="6804"`, so its
bookings reached the CRM team, while being absent from `PRODUCT_PATHS` and
from both `B2C_ALLOWED_PATHS`. Every lead from it was stored and reported
as `aeo` — wrong Salesforce picklist, wrong Meta event, 12000 of predicted
value instead of 5000 — and a B2C answer dead-ended the prospect. The
booking went to the right team and the record said the wrong thing.

**IT HALF-WORKED, WHICH IS WHY IT SURVIVED.** A visitor arriving on a CRM
campaign got through the gate by the OTHER door — `currentOffer() === crm`
— so the page looked correct to anyone who tested it from an ad, and
failed for direct, organic, brand and AEO-ad traffic, which is most of it.
Found by a human answering B2C on the live page. Section 21 could not
catch it: that test asserts the three lists AGREE, and they agreed
perfectly about a page none of them had heard of.

**So the check when a new product page appears is "is it in the
catalogue", not "do the lists agree".** A page can be live, correctly
routed and entirely absent from this repo.

**Two `sell_to ILIKE 'B2B%'` predicates exist and they MOVE TOGETHER** —
`/monitor/sdr` and the `noBooking` card in `/monitor/metrics` that counts
it. Both carry `OR product = 'crm'`, because a CRM lead who answers B2C is
no longer disqualified and would otherwise drop out of the SDR list
silently: present in every headline number, absent from the one surface
anybody acts on. Same shape as the `SDR_SEARCH_COLUMNS` /
`SDR_SEARCH_FIELDS` pair.

**A CRM B2C lead now fires Meta, can fire a $50 PartnerStack conversion,
and is pushed to Salesforce.** All three follow from the lead no longer
being disqualified, and all three were authorised explicitly on 15 Sept
2026 — the non-ICP reasoning is about AEO and does not transfer to CRM.
Measured exposure is about two leads a day.

**THE COOKIE NAMESPACE IS SHARED BETWEEN THIS REPO AND WEBFLOW, AND
`gw_utm_campaign` IS ALREADY TAKEN.** Two scripts write cookies on
`.gushwork.ai` and they live in different places, so neither one's file
shows you the other:

| Cookie | Written by | Lifetime | Job |
|---|---|---|---|
| `gw_utm_campaign` / `gw_utm_medium` | `gushwork-form.js` `rememberCampaign()` | **30 days** | which offer to show a returning visitor |
| `gwa_*` (seven) | Webflow **site-wide footer** | **session** | carry attribution across an in-app-browser webview |
| `gw_ps_*` | Webflow site-wide footer | 90 days | PartnerStack |
| `_fbc` | `gushwork-form.js` | 90 days | Meta click id |

The attribution mirror was written on the `gw_` names first and it broke
both directions at once, caught by executing it rather than reading it:
re-writing `gw_utm_campaign` with no `max-age` **downgrades the 30-day
offer memory to a single session**, and reading that still-live 30-day
cookie back into `sessionStorage` puts a **paid campaign into
`utm_campaign` on an organic return visit, beside an empty
`utm_source`** — which `Source_Bucket__c` reads. That is exactly the
corruption the `offer_*` split exists to prevent, arriving from a
different file. Hence `gwa_`.

**The site-wide attribution script is in Webflow global site settings
(footer), NOT in this repo and NOT in `gushwork-embeds`.** It is the
thing that writes `gw_referrer`, `gw_landing_page` and `gw_utm_*` into
`sessionStorage`, which is where `gushwork-form.js` reads them from. So a
change to how attribution is captured is a **Webflow** change with no
diff anywhere in git — and, unlike a form pin, it needs no re-pin,
because rehydrating into `sessionStorage` leaves every reader untouched.

**Why a cookie at all: sessionStorage does not survive the Facebook and
Instagram in-app browsers.** Each navigation can open a fresh webview,
which keeps cookies and drops `sessionStorage` — which is why `_fbc`
lives through the same journey that loses the UTMs. Measured over 90
days: **31.2% UTM loss in-app, 1.6% on a normal mobile browser, 0% on
desktop, 424 leads.** Those leads still show the campaign in
`previous_page`, which is how the loss was found at all.


**`gushwork-form-popup.js` is a FORK of `gushwork-form.js`, not a sibling.**
The Ads file exists only to present the booking step as a fullscreen modal
opened after step 2. Every other line is meant to be the same code, and there is
no shared module — the two files are edited independently, so a fix applied to
one is a fix applied to half the traffic. This has already gone wrong once: the
Ads file forked at `/demo` v5.3.0 on 14 Aug 2026 and silently missed v5.6.0 and
v5.7.0/v5.7.1, so for twelve days Google Ads leads got no DNS fallback, no
email-in-website-field catch, and no typo nudge. **A change to `gushwork-form.js`
is not finished until it is in `gushwork-form-popup.js` too.**
`node tests/test-ads-parity.js` now enforces that — it lifts both files and
compares them, and it also pins the modal as deliberate so a future sync cannot
"tidy" the fork's own presentation away.

**A `//` COMMENT INSIDE SQL TAKES THE STATEMENT DOWN, and this already
happened.** Postgres has no `//`. On 8 Sept 2026 a single line —
`// COALESCE for the same reason as /partial.` — shipped inside the
`/submit` INSERT and broke **every form completion**: SQLSTATE 42601,
`syntax error at or near "//"`, a 500 from the route, and nothing after
the INSERT ran. No `completed`, no `submitted_at`, no step-2 fields, no
Slack, no Salesforce, no Meta `Lead`, no PartnerStack conversion. It was
live for 31 minutes and caught one real lead.

Sibling of the backtick trap above, and worse, because a backtick fails at
**parse time in Node** where you cannot miss it. This is valid JavaScript
and invalid SQL, so it fails only when the query runs. Use `/* ... */`
inside SQL, always. `tests/test-batch2.js` section 13 lints for it.

**IT SURVIVED SIX GREEN SUITES, A REVIEW CARD AND A MERGE — because every
SQL assertion in this repo reads the query as TEXT.** That is the real
lesson and it generalises past this one bug: *a source-level assertion
cannot tell you whether a query parses.* The placeholder arithmetic in
section 11 was careful, correct, and checking a statement that could never
execute. The same blind spot is documented above for ordering assertions
and for the 21 dead PartnerStack call sites; this is its third appearance.

When you touch SQL, **execute it**. It costs nothing: build a `CREATE TEMP
TABLE` from the real migrations, run the real statement against it inside a
transaction, and `ROLLBACK`. That runs against the AWS mirror, needs no
production write, and would have caught this in seconds. For a read-only
query, `EXPLAIN` is enough — a syntax error is `42601` and an absent table
is `42P01`, so the two are distinguishable without any schema at all.

**`/monitor/funnel` had been broken since the Eastern Time migration
(`eb50c08`) for exactly the same reason** — three `//` lines sitting among
correct `--` ones. Nobody noticed because a dashboard tab erroring is
quieter than a lead path erroring. Found by the section 13 lint on the day
it was written, not by anyone opening the tab.

**Product tagging: AEO is the DEFAULT, and only the exceptions are listed.**
`PRODUCTS`, `PRODUCT_PATHS`, `DEFAULT_PRODUCT` and `resolveProduct` live in
`meta-capi.js` and are imported by `index.js` — one catalogue, so the
stored column and the Meta event cannot disagree. Today `PRODUCT_PATHS` is
`{'/ai-demo': 'crm', '/ai-crm': 'crm'}`; everything else is `aeo`.

**`/ai-crm` is the rebuilt CRM page and will eventually replace
`/ai-demo`'s content wholesale.** Both are mapped, both resolve `crm`, and
the swap therefore changes nothing here. It is also the FIRST entry that
makes the page-beats-campaign rule observable: until there were two, the
only mapping and the only overriding campaign answer were both `crm`, so
the guard could be deleted without any test noticing. Measured — the
mutation survived a full bar.

This is the opposite of how the rest of the repo works, deliberately. An
AEO allowlist rots: the form is live on a dozen pages — `/demo`, `/start`,
`/pricing`, `/consulting-lead-generation` and the SEO landers — and new
landers get added by people who will never open this file. Measured on 90
days of real leads, a `/demo`-only list tagged **82%** and left **631 leads
(408 completed)** sending unlabelled events; the default tags **99.7%**. A
default fails only when a genuinely new product launches, which is rare,
deliberate, and logged once per unmapped path.

**Adding a product** means one entry in `PRODUCT_PATHS` and one in
`PRODUCTS`. **Adding a new AEO landing page means nothing at all**, which
is the entire point.

**An unreadable `page_url` is NOT the default — it returns null and the
event goes untagged.** "We could not tell which page this was" is not "this
was the default page", the same rule the lead-path checkers follow. The
leading-slash guard in `resolveProduct` is load-bearing: `new URL(x, base)`
succeeds for almost any string, so without it `'not a url'` becomes
`/not%20a%20url` and reaches Meta as a real AEO lead.

**`Contact` is excluded from product tagging by EVENT NAME, not by its
page.** `PRODUCT_EXCLUDED_EVENTS`. With a default in place the lead-magnet
LP resolves to `aeo` like any other unmapped page, so this list is the only
thing keeping a PDF download off a 12000 `predicted_ltv`. A test asserts it
from the page that WOULD tag, so a regression to page-based exclusion
fails.

**`predicted_ltv` is the same per product on every event; only `value`
varies, and it is 0 on all three upstream events.** 12000 aeo / 5000 crm,
PROVISIONAL. Changing one changes how Meta weights these conversions — a
business decision to surface, not a tidy-up.

**`leads.product` and `gw_form_leads.product` hold the resolved slug**,
never a raw page and never anything a page author typed; nothing reads
`req.body.product`. Both conflict clauses COALESCE it. Historical rows were
backfilled to `aeo` once by hand; that backfill is deliberately NOT a boot
migration, because those run on every deploy and a NULL now means
"page_url was unreadable".

**`about_business` is capped at 1000 chars, and the cap is NOT what
protects the request.** `express.json({ limit: '50kb' })` is. A
server-side `slice` runs inside the route, and express rejects an oversized
body with a 413 **before any handler runs** — so an unbounded textarea
loses the whole lead, not the one field. The limit was raised from 10kb
when the textarea landed; measured, a worst-case body was already 12,755
bytes without it. The `slice(0, 1000)` is a backstop for the column and
matches `maxlength="1000"` on the Webflow textarea, so what the visitor
sees on screen is what the column keeps.

**THAT LAST SENTENCE WAS AN INTENTION, NOT A MEASUREMENT, UNTIL 18 SEPT
2026.** All three pages carrying the field — `/demo`, `/ai-demo` and
`/ai-crm` — rendered `maxlength="5000"`, Webflow's default, while the
server cut at 1000. Anyone writing a long answer would have lost the tail
silently: no warning, no error, and nothing in the row to show it had been
cut. Never actually bit — the longest answer ever recorded is 262
characters and no row sits at exactly 1000 — so this was a trap rather
than a loss. All three are now genuinely 1000.

**THE FIELD IS ON THREE PAGES, and the third is easy to miss.** `/demo`
has it behind the AI-CRM tick inside `#about-business-wrap`; `/ai-demo`
and `/ai-crm` have it permanently visible with no wrapper. A change to
"the about-business textarea" is three edits.

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

**A lifted `recordFailure` needs every name it reads.** Two suites and one
tool lift it; adding `isCreditsExhausted` made the PartnerStack streak
harness throw inside `recordFailure`'s own try/catch, which swallowed it
and left the streak silently at zero. Five assertions caught it. Add the
name to every lift, or that is what a new dependency looks like.

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

**`git checkout <file>` restores from HEAD, not from "before my scratch
edit".** Used as an undo for a mutation-test tweak while the real change
was still uncommitted, it silently deletes the work. That happened four
times in one session. **Commit before mutation testing** — which is also
the correct order, because a mutation must be measured against a committed
baseline.

**ONE ROW BUILDER, TWO TABLES, AND THE ROW ID MUST BE NAMESPACED.**
`leadRowsHtml(leads, ns)` renders **All Leads and Blocked**, and `showTab`
toggles a class — it never clears a panel — so both tables sit in the
document at once. An unscoped `id="er-<session_id>"` therefore existed
**twice** for any lead that was blocked *and* on the loaded All Leads page,
and `getElementById` returns the first in document order. `tp-leads`
precedes `tp-blocked`, so the click on Blocked expanded the hidden copy in
the inactive All Leads panel and **nothing happened on screen**.

It presented as "the top two rows won't expand" because All Leads page 1 is
the newest 25 leads: a blocked lead breaks while it is new enough to be
there and silently starts working again once it falls off. A moving window,
which is why it read as a property of those rows — and why enrichment
looked relevant and was not.

`toggleRow(key, sid)` and `loadChanges(key, sid)` take **two** arguments:
the key addresses the DOM, the session id addresses the lead. Collapsing
them is what caused this, and it would also send a namespaced key to
`/monitor/lead-changes` as a session id. Every caller passes a distinct
namespace and a test pins it.

**The Model tab keeps ONE claim per number, and its industry table is
SPLIT BY WHAT DECIDED.** "Insurance 8 / Blocks" read as the model having
categorised and blocked eight leads, while the ladder beside it said
`blocked_model: 0` — the list had done all ten. Three groups now render
always, **empty ones included**, because an empty "Blocked by the model"
is the clearest statement on the tab that the model has turned nobody away.

**And the industry comes from the domain that ACTUALLY BLOCKED, or from
nowhere.** It used to fall back to any candidate domain with a verdict,
which filed three `farmersagent.com` blocks (no verdict for that domain)
under Insurance because the lead's *website* was `farmers.com`. Harmless
there — both are insurance — but on a lead whose email is a brokerage and
whose website is a software company it files a block under the wrong
industry, and a wrong industry here is something somebody acts on without
knowing it is wrong. A block we cannot categorise now says **"not
categorised"** rather than borrowing.

**Blocked counts: three different units, and every label says which.**
Overview's big number is **leads**, its sub-line is **people**, and the
Blocked tab's "excluding our own tests" is **leads** again. 10 leads are 6
people because five of those leads are one address of ours. The three sat
adjacent with no units and invited subtraction.

**The Model tab keeps ONE claim per number.** Its industry table counts
**leads acted on in the window** and nothing else; the standing cache is a
separate block counted in **companies, all time, with no rate**. They were
one table until 15 Sept 2026, and because ~2,937 of the cache is a backfill
that loaded only successfully-judged rows, it read "Insurance 19" in a week
with five insurance blocks and a 0.3% scrape-failure rate against a live
rate near 26%. Both had an explanation in small print under the number
people actually quote. The scrape rate is scoped to the domains behind the
window's leads, and **"never tried" is its own number, deliberately outside
the denominator** — a domain the warm path has not reached is not a scrape
that failed.

**Our own test submissions are MARKED, never silently excluded.**
`INTERNAL_TEST_EMAILS` (extensible from the Railway env) plus
`ELV_EXCLUDED_DOMAINS`, behind `isInternalLead`. Five of the first ten
non-ICP blocks were ours. Nothing is subtracted from any total — the rule
above about internal addresses still holds — so rows carry a marker, the
Blocked tab and the ladder print the excluding-ours figure **beside** the
count, and the filter is opt-in. **Never inferred from a person's name**: a
real prospect may be called Darshil, and `allstate.com` is a real brokerage
domain that is on the block list for that reason.

**"MARKED, NEVER EXCLUDED" IS ABOUT OUR OWN NUMBERS, AND AS OF 19 SEPT
2026 IT STOPS AT OUR OWN NUMBERS.** Internal submissions are still
counted in every `leads` figure, still on the dashboard, still booking —
that rule is unchanged and is about reconciling a number against the
database. But they no longer leave the building. Three outbound systems
now refuse them:

| System | Guard | Why |
|---|---|---|
| Meta CAPI (5 call sites) | `internalLeadSuppressesMeta` | a conversion tells Facebook to find more people like us |
| Salesforce (`/submit`) | `isInternalLead` | `Source_Bucket__c` recomputes on read, so each junk Lead is reported as real inbound |
| AWS mirror (`syncToAWS`) | inside the function | `gw_form_leads` is the DIALER FEED — a row is a person somebody may ring |

**The three harms are different and only the first is about signal.**
Measured before the fix: 21 addresses, 81 rows since March, 61
StartTrial / 31 Lead / 14 Schedule (1.14% / 0.81% / 0.41%), 41
Salesforce Leads. Under 1% dilutes rather than misdirects; the
Salesforce records and the dialer rows cost people's time instead.

**`b@g.ai` WAS THE LEAST PROTECTED ADDRESS, NOT THE MOST.** It is
special-cased in **four** hardcoded lists — both form files,
`PS_TEST_EMAILS`, and the two booking webhooks — and was in none of
`INTERNAL_TEST_EMAILS`, so `isInternalLead('b@g.ai')` answered **false**
and it was not even marked. Now listed. Four special cases and no
membership of the one list that decides anything is the shape to look
for elsewhere.

**And it was the website gate that let them through, not a missing
one.** `isWebsiteVerified` returns **true** for a null reason
("pre-feature rows") and `test_email_skipped` is itself on
`WEBSITE_VERIFIED_REASONS` — so skipping email verification for a test
address is *precisely* what marks it verified. The gate was working; it
was answering a different question to the one everyone assumed.

**The AWS guard is INSIDE `syncToAWS`, not at its four call sites**,
because two of those are the Cal and RevenueHero **safety nets** — the
pair most likely to be missed by someone guarding the two obvious
routes. The three targeted writes (`syncBookingToAWS`,
`syncPartnerIdentityToAWS`, `syncHearAboutUsToAWS`) need no guard: each
is a plain `UPDATE ... WHERE session_id`, so with no mirror row they
match nothing and no-op. `syncBookingToAWS` now **reads `rowCount`** and
says which happened — it used to log a tick unconditionally, so an
update that matched nothing read as success.

**AND THE PAGE IS A SIGNAL, NOT ONLY THE ADDRESS — 19 SEPT 2026.**
`isInternalSubmission(email, page_url)` is what every outbound guard now
asks; `isInternalLead(email)` is only half of it. **40 lead rows were
submitted from `gushwork.webflow.io`**, the Webflow staging host, and
**19 carried addresses no list could ever catch** — the team's personal
Gmails (`swapnilsinha07@`, `utsavsingh5600@`, `darshildixit21@`), plus
`honey@apple.com`, `ywhs@gggg.com` and `johnlennon@abc.com` whose website
was `heheheh.com`. 15 were submitted, so each fired Meta, created a
Salesforce Lead and reached the dialer.

**Nobody FINDS the staging site**, so everyone on it was handed the URL.
That makes the page a stronger signal than the address, and it is the
signal a list of addresses can never become.

**Exact HOST match, never a substring.** `INTERNAL_STAGING_HOSTS` is
compared against `new URL(page_url).hostname`, so
`gushwork.webflow.io.evil.com` and `evil.com/?x=gushwork.webflow.io` do
not match. A bare path resolves to no host and is therefore **not**
evidence of staging — same rule as `resolveProduct`: what we cannot read
is not a default.

**Nothing is lost when it fires.** The lead is still written to `leads`
and still appears on the dashboard; it is only not propagated outward. So
a real person who is sent a staging link is visible to us, just not
auto-pushed. The one row that looked like a real company was checked
rather than waved through — `hari@productledsales.io` landed directly on
`/demo-testing-rh` with referrer "direct" on 18 June, the same day two
staff were testing that exact page.

**The dashboard marker and the guards ask the SAME question**, and that
is the fix for how this stayed hidden: Meta was suppressed by address and
the dashboard agreed with it, so nobody could see that **neither had ever
consulted the page**. `internalLeadSqlClause(emailCol, pageCol, params)`
carries the third arm too, as an exact `SPLIT_PART` host match rather
than a `LIKE`.

`tests/test-batch2.js` §28 and §29 execute all of this rather than
reading it, and all five mutations are caught.

**Two copies of the label map.** `WEBSITE_REASON_LABELS` is a normal JS object.
The monitor dashboard has a second copy (`var WLBL=`) inside a JS string that gets
sent to the browser. Both need updating, and the string one uses `\u2014` for em
dashes with a **single** backslash. Getting that wrong renders a literal `\u2014`
in the dashboard.

**Two more pairs that must stay in sync.** Same shape as the label map above:

- `SDR_SEARCH_COLUMNS` (server, in the `/monitor/sdr` route) and
  `SDR_SEARCH_FIELDS` (client, in the dashboard JS) are the fields the SDR
  search matches. The table filters in the browser; the CSV export filters on
  the server. If they drift, the export silently stops matching what is on
  screen. A test lifts both and asserts they are equal.
- The System Health check ids in `HEALTH_SEVERITY` / `HEALTH_ALERT_META`
  (server) and `HIDS` (client) map checks to dashboard rows. A check with no
  entry in `HIDS` renders nowhere; a row id with no check paints red as
  "No result". Both are asserted.

**`/monitor/health` is a real probe, and slow on purpose.** It queries the AWS
mirror across a WAN, so it is deliberately kept OFF the dashboard's 60-second
poll — it runs at load, every five minutes, on tab open and on the Re-check
button. The same checks run from `startHeartbeat()` every 30 minutes so a
failure alerts with the tab closed. Do not fold it into `/monitor/metrics`.

**`/monitor/website-recheck` is a POST.** It was a GET that rewrites lead rows
and runs two `ALTER TABLE`s, which a link prefetch or an unfurled URL could
have fired. Nothing in the UI calls it; run it with
`curl -X POST`.

**`/monitor` is not one page.** It's the dashboard plus several sub-routes that
feed it data. A reader who greps for a single `/monitor` handler expecting to find
everything will miss most of it.

**Eleven tabs as of 25 Sept** — the list lives in `showTab`, and `Dropoff`
is the newest. A tab needs a `t-<name>` button, a `tp-<name>` panel, an entry
in that array and a loader guard, or it renders nowhere and nothing says
so. **Count the array, do not trust this number** — it said ten the day
before.

**THE DROPOFF TAB ANSWERS "where do the people who start the form go", AND
ITS LADDER IS THE CONTRACT.** `/monitor/dropoff`, filtered by date range,
week or month, source, and leads-or-people. Seven outcomes in
`DROPOFF_STAGES`, resolved top-down by the single `DROPOFF_STAGE_SQL`
expression, mutually exclusive and exhaustive — so the rows **always sum to
the period total**, and `test-non-icp-routes.js` asserts that sum per period
and overall rather than trusting it. A second copy of that CASE anywhere is
how two consumers start disagreeing; it is the `disqualified` /
`non_icp_blocked` lesson waiting to arrive a third time.

Built 25 Sept because the question had been asked twice by hand in one
Slack thread and a hand-answer is wrong the next morning. It changes no
blocking behaviour and fires no Meta events — it is a read.

**"LEFT ON STEP 2" IS NOT "left at step 1", AND THE LABEL DECIDES WHICH
SCREEN SOMEBODY GOES AND FIXES.** A `leads` row is written by
`savePartial(1)` at the **end** of `handleStep1Next`, after the email and
`sell_to` validate — so a row exists only once the visitor completed step 1
and pressed Next, and **everyone in that bucket reached step 2**. Confirmed
in data: all 578 such leads in the 12 weeks to 25 Sept carry `sell_to` (a
step-1 field) and **none carries a phone** (a step-2 field). It is the
biggest single bucket, every week since July.

**And a lead stopped ON step 2 by a blocking website verdict leaves NO
TRACE.** `handleStep2Next` returns before `submitLead()` on `nxdomain`,
`brand_mismatch` or `mailbox_domain`, so nothing is persisted: measured, 0
of those 578 rows carry a `website_check_reason`. You cannot tell from
`leads` who was turned back from who simply left.

**SOURCE READS TWO FIELDS, AND READING ONLY `utm_source` UNDERSTATES META BY
ABOUT A QUARTER.** `DROPOFF_SOURCE_SQL` takes `utm_source` first, then falls
back to `hear_about_us` — which `gushwork-form.js` prefills from the
**referrer** as well as the utm, and then hides. So when the UTMs are lost in
the Facebook and Instagram in-app browsers (31.2% in-app against 1.6% on a
normal mobile browser) the referrer still names the platform. Measured over
12 weeks to 25 Sept: **469 leads carried no `utm_source` at all** and were
recorded by the form as paid — 459 Facebook, 9 Instagram, 1 Google. Reading
`utm_source` alone gives Meta 2,051 and Direct/organic 991; reading both
gives **2,519 and 486**.

**The human free text in that column is deliberately NOT bucketed.** It runs
to dozens of spellings plus junk, and keying a channel off free text is
exactly the substring trap `Source_Bucket__c` already fell into. It stays in
`Direct / organic`, which therefore means "no trackable click" rather than
"arrived directly".

**LEADS OR PEOPLE IS A TOGGLE, NOT A CORRECTION.** `mode=people` resolves the
ladder per `lower(email)`, placing a person in the period they **first**
arrived and carrying the **best** outcome any of their sessions reached.
Measured over the same 12 weeks: **3,309 leads against 3,096 people, booking
65.8% against 69.1%** — a repeat attempt is usually somebody who got there in
the end, so per-person always reads better and neither is the honest number
alone.

**THE BUCKET COMES BACK AS TEXT, and that is load-bearing.**
`node-postgres` parses a `date` column into a JS `Date` in the **process's**
local zone, so `toISOString()` shifted every bucket a day west — silently
empty on an IST laptop and correct on Railway. An environment-dependent bug
is worse than a broken one. `to_char(..., 'YYYY-MM-DD')` removes the parse.
Found by executing the query, not by reading it.

**The weekly digest was FIRED ON PURPOSE on 25 Sept** — `node tools/fire-non-icp-slack.js dropoff-digest`, Slack 200, real numbers over the real table. "We asserted it alerts" and "we watched it alert" are different claims, and the gap between them hid 21 dead call sites here.

**The weekly digest reports the COMPLETED week, never the current one.**
`runDropoffDigest`, Mondays in the **09:00 ET hour** (`DROPOFF_DIGEST_HOUR_ET`,
`DROPOFF_DIGEST_ENABLED=false` to stop it). Pinned to Eastern, so the local
time moves with US daylight saving — **18:30 IST in summer, 19:30 in winter**.
Sending a partial week to a channel is how a reader concludes the funnel
collapsed on a Monday morning, and no caveat survives being read on a phone.
It leads with whether the week sits **inside** the prior eleven weeks' range,
because the answer is usually "this is normal" and a digest that always reads
like an alarm gets muted along with the week that matters.

**IT GOES TO THE LEADS CHANNEL (`sendSlack`), NOT THE ALERTS ONE.** Authorised
by Darshil on 25 Sept after the first hand-fired one landed in
`bot-n8n-alerts`. Nothing in it is broken and nobody has to act on it, so it
is not an alert; the alerts channel is where things that need fixing go, and a
recurring no-action post there is how that channel starts being muted — taking
the week that matters with it. The near-miss digest still uses `sendOpsSlack`,
so the two deliberately differ.

**THE TICK IS 15 MINUTES, NOT 60, AND THAT IS NOT FUSSINESS.** `setInterval`
starts counting at **boot**, not on the hour, so with an hourly tick a deploy
at 09:05 on a Monday puts the ticks at 10:05 and 11:05 and **that week is
silently skipped**. A missed weekly digest is invisible where a duplicate is
merely annoying, so the trade goes toward sending. The guard keys on the ET
**day** stamp, so four ticks inside the 09:00 hour still send exactly once.
**The near-miss digest had the identical gap and was fixed in the same change** — both now tick at 15 minutes, and they should stay in step. Its own week guard is unchanged, so the documented double-send on a deploy inside the hour still applies to both: that trade deliberately favours sending, because a duplicate is visible and a miss is not.

**Booking arrives by three routes.** `/booking-confirmed` (browser-fired),
`/booking-confirmed-webhook` (Cal), `/booking-confirmed-webhook-rh` (RevenueHero).
Any change to booking behaviour has to be applied to all three. A fix on one is a
fix on one third.

**PartnerStack: `partnerStackCustomerKey` is the only place a domain becomes a
customer key.** PartnerStack counts one conversion per customer key for the life
of the account, so two spellings of one company means either an affiliate paid
twice or a real referral swallowed as a duplicate. Website, email and the three
warehouse customer tables all go through this one function. Do not normalise a
domain inline anywhere near this integration.

**A disqualified lead never fires a conversion, and that is a GUARD now, not a
flow property.** No disqualified lead reaches `/submit` today — `b2c_or_mixed`
and `waitlist` both call `savePartial(1)` and then show a terminal step — but
that lives in two forked frontend files which have drifted apart before, and the
cost of the drift here is paying an affiliate $50 for a B2C waitlist signup.
`runPartnerStackSignup` checks the flag explicitly and a test asserts it runs
before the domain and test-email checks.

**Every `/submit` logs whether a partner was present.** An organic lead used to
produce no PartnerStack line at all, so the logs could not tell "no partner
traffic yet" from "capture is broken" — which cost real time on the first
deploy. `[PartnerStack] No partner on this submit (…)` is the negative case, and
each skip says which guard stopped it.

**The PartnerStack handover doc is `docs/partnerstack.md`.** Anything below is
the short version; that file has the full picture including the known gaps.

**Two PartnerStack hosts, two auth schemes, one env.** The conversion goes to
`partnerlinks.io/conversion/xid` with `PARTNERSTACK_TRACKING_TOKEN` as a
**Bearer** token. The partnerships lookup and the qualification action go to
`api.partnerstack.com/api/v2/*` with **Basic** base64(`PARTNERSTACK_PUBLIC_KEY`:
`PARTNERSTACK_SECRET_KEY`). Both credentials sit in the same environment and
using one where the other belongs returns a 401 that reads like a bad password
rather than the wrong scheme. A test asserts `sendConversion` never reaches for
the key pair.

**Partner identity resolves in three layers and never blocks a lead.** Process
memory, then any earlier lead row already carrying the name, then the v2 API. A
FAILED lookup is deliberately not cached — caching it would pin every future
lead from that partner to "unknown" for the life of the dyno.

`partnerIdentityNoNetwork()` is the first two layers only and is awaited BEFORE
Slack fires. An earlier version peeked at the in-memory Map alone, which meant
every deploy cleared it and the first partner lead after a restart posted a raw
hex key to Slack even though the database already had the name from an earlier
lead. The database layer is one indexed lookup
(`leads_ps_partner_key_resolved_idx`) and only runs when a partner key is
present, so organic leads pay nothing. The API call is the slow part and stays
deferred; `upgradePartnerHearAboutUs` corrects the row afterwards — in our
table, on the AWS mirror, and in Salesforce where the AE is looking.

**A BLOCKED LEAD'S SLACK POST IS ITS ONLY HUMAN-FACING RECORD**, which
changes what an unresolved partner key costs there. Partner identity
resolves in three layers — memory, the database, then the API — and only
the first two are awaited before Slack fires. For the **first ever** lead
from a new partner both legitimately miss, so the post gets the raw hex
key; the deferred API call then corrects the row, the mirror and
Salesforce. For a normal lead that is fine, because Salesforce is where the
AE looks. A blocked lead is deliberately **not** pushed to Salesforce and
is **not** on the SDR list, so the one surface the upgrade cannot reach is
the only surface there is. `slackPartnerHearLabel` therefore relabels the
unresolved case in words — **display only**: the stored column keeps the
exact `Partner - <key>` placeholder, because `upgradePartnerHearAboutUs`
matches on that precise string and rewriting it would leave the row holding
the key forever.

**One display chain, three surfaces: name → email → raw key.**
`partnerDisplayName()` is used by Slack, the dashboard and `hear_about_us` so
the same partner cannot read three different ways. An email tells an SDR who
they are dealing with; a hex key tells them nothing they can search for. The
`hear_about_us` upgrade treats BOTH weaker rungs as replaceable, so a row
showing the email is lifted to the name when it resolves — but only values this
code wrote, never a referral or anything a human typed.

**`hear_about_us`: an existing referral outranks a partner.** `gw_ref_email` is a
named human vouching for the lead and is a stronger signal than an affiliate
link, so anything already starting with `Referral -` is left alone. The upgrade
only ever rewrites the exact `Partner - <key>` placeholder this code wrote; a
referral or anything a human typed is never touched.

**A late-arriving single field needs its own targeted AWS write, NEVER
`syncToAWS`.** That upsert's conflict clause sets
`disqualified = EXCLUDED.disqualified` with no COALESCE, so handing it a partial
object passes `disqualified` as false and CLEARS a real disqualification on the
mirror the dialer reads. `syncBookingToAWS`, `syncPartnerIdentityToAWS` and
`syncHearAboutUsToAWS` all exist for this reason. A test catches the regression.

**Every alert path gets FIRED ONCE ON PURPOSE before launch** — `node
tools/fire-alert.js <name>` does it, and it sends for real.**  "We asserted it
alerts" and "we watched it alert" are different claims, and the gap between
them hid 21 dead alert call sites in this repo — all of them tested, all of the
tests passing, none of them ever producing an alert. No assertion about a call
site can tell you the call did anything; that is the ceiling of a source-level
assertion, not a flaw in a particular test. Only executing the code or watching
the real thing arrive gets past it.

**A HAND-RUN API READ IS NOT A BETTER ORACLE THAN THE SWEEP THAT WAITS.**
This is the most useful thing learned on 7 Sept 2026 and it generalises past
PartnerStack. A `test.com` conversion was created at 16:38:00. A direct
`GET /v2/customers/test.com` at 16:49 returned **404** — eleven minutes after
the record existed. That 404 was read, in the moment, as proof the conversion
had failed and would keep failing, and it was used to argue for intervening.
The sweep asked at 17:06, got a clean 200, and verified correctly.

Acting on the manual read would have released a good claim and re-fired a
conversion that had already landed — **the exact duplicate credit the grace
period exists to prevent, arrived at by a human being more impatient than the
code.** PartnerStack cannot undo a double credit.

So: when a checker is deliberately built to wait before believing a negative,
a quicker manual check of the same thing is **evidence of nothing**. It has
strictly less information than the check you already wrote. If you find
yourself about to override a grace period with a fresh `curl`, you are about to
reintroduce the bug the grace period documents. Wait for the sweep, or read
what the sweep last concluded — never race it.

**Do not reduce `PS_VERIFY_GRACE_MIN`.** PartnerStack's read-after-write lag
was measured at **over 11 minutes** on 7 Sept 2026 — a customer created at
16:38 still 404'd on a direct API read at 16:49. The margin at 15 minutes is
about **four** minutes, not the nine a 2-to-6 minute range implied. A manual API read is
also not a better oracle than the sweep: that 404 was nearly used as grounds to
release a claim for a conversion that had in fact landed, which is the exact
duplicate-credit failure the grace period prevents.

**`recordFailure(source, …)` is a SILENT NO-OP for any source with no
`FAILURE_MONITORS` entry** — it opens with `if (!cfg) return;`. `'PartnerStack'`
had no entry from the day the integration shipped, so 21 call sites across the
money path had never produced a single alert, several of which were described
as "loud". A test in `tests/test-batch2.js` now derives every source used
anywhere and requires an entry, so this cannot recur for a new source. Keep the
two paths straight: `recordPartnerStackFailure` calls `alertOps` **directly**
and always worked; the health rows run their own queries and never touched the
table.

**An ACK means "this failure is understood, leave it alone" — not just "stop
alerting me".** Four things act on a partner failure and all four must respect
`ps_failure_ack_at`: the unbounded Needs-attention count, its page-derived
fallback, the `partnerstack` health row, and the **conversion retry sweep**.
The retry was missed when it shipped, because the ack's scope had been written
back when only an alert acted on a failure — and the sweep then re-created a
PartnerStack customer that had been deleted by hand. If you add a fifth
consumer, it respects the ack or an acknowledged failure starts demanding
action again through it. An ack never clears the failure stamp: the row keeps
its state, its red chip and its history.

**"Needs attention" is counted by its OWN unbounded query, not from the
capped domain list beside it.** The ladder includes an unresolved failure
regardless of age so it cannot drop out of the one number somebody must act on
— and `LIMIT 500` on the domain list would undo that. When the unbounded query
cannot run, `needsAttentionComplete: false` reaches the screen as "AT LEAST
this many". A floor is never rendered as a total.

**A stale `partner_domain_sf_state` is a RED health row.** When that column
freezes, "waiting on an AE", "no Opportunity" and the funnel's Opportunity and
ticked stages all keep rendering their last values as if current. Checked after
the failure states but before green.

**Partner revenue gaps is a WORK QUEUE, not a health check.** `/monitor/partner-gaps`
finds the two ways a partner referral silently never pays: (A) no conversion was
ever sent for that domain, and (B) the demo happened and no Opportunity exists
for an AE to tick. It is deliberately NOT in System Health — a green badge there
means "verified working, just now", and a lead waiting on an AE is normal
latency, so wiring it in would leave the dashboard permanently amber and train
people to ignore it. A test asserts `runHealthChecks` never calls it.

The tempting version of this check — "booked but no `ps_qualified_sent_at`" —
is wrong and was rejected. It conflates three states and only one is a bug: no
Opportunity (broken), Opportunity awaiting an AE (normal), and Opportunity the
AE deliberately did not tick (correct, and PERMANENT). The third never clears,
so the queue fills with correct non-payments and the real failures become
invisible inside it.

Check A is keyed by DOMAIN, not by lead: the conversion fires once per domain
ever, so the second lead from a domain legitimately has a null
`ps_signup_sent_at`. Only a domain where NO row was ever sent is a miss.

`leads.start_time` is TEXT, so every cast to timestamptz there is wrapped in a
`CASE WHEN start_time ~ '^[0-9]{4}...'`. A bare cast in a WHERE clause is not
safe: Postgres does not guarantee the regex runs first, and one malformed row
takes the whole query down.

When Salesforce cannot be reached, check B reports **unavailable**, never zero.
The card shows `N+?` rather than a total. "We could not check" is not "we
checked and it is fine" — the same rule the lead-path checkers follow, pointed
the other way.

**Step 10 is a POLLER, not a Salesforce Flow callout.** A Flow that calls out
fails inside Salesforce where nobody on this team would see it, and it couples an
AE ticking `Qualified_Demo__c` to our service being up at that instant. Polling
means a missed window is just a later window. The join between
the two systems is the DOMAIN — `Account.Website` first, the primary contact's
email domain as fallback, both through `partnerStackCustomerKey`.

**It polls every 2 minutes, and there are three separate intervals here that
must NOT be collapsed onto each other.** This one is cheap — it reads only the
ticked Opportunities, one page. `PS_SF_REFRESH_INTERVAL_MS` (15 min) scans every
Opportunity in 180 days across six growing pages and is right to be slow.
`PS_VERIFY_GRACE_MIN` (15) is not an interval at all but the read-back grace,
and it is load-bearing: PartnerStack's own indexing lags a conversion by 2–6
minutes, so shortening it releases good claims and re-fires conversions. Also
do not "consolidate" the poller onto `partner_domain_sf_state` — a 2-minute
poll over a table that changes every 15 minutes sees the same rows seven times.
`docs/partnerstack.md` has the full reasoning.

**Only domains whose conversion is VERIFIED (`ps_signup_verified_at IS NOT
NULL`) can be qualified** — not merely sent. `ps_signup_sent_at` only means
PartnerStack answered 200, and `/conversion/xid` answers 200 with an empty body.
Qualifying on the weaker stamp and then having the read-back sweep 404 releases
the conversion claim and leaves the qualification claim stamped forever, so the
domain re-converts and can never be qualified again: $50 gone with no error
anywhere. Fixed 7 Sept 2026. And beyond that, an action for a `customer_key`
PartnerStack has never seen is a no-op at best.

**`partner_domain_sf_state.sf_state` is the only SNAPSHOT in this integration,
and it moves in BOTH directions.** Everything else keys off an immutable stamp,
which is why every other funnel stage is monotonic. An AE unticked
`Qualified_Demo__c` on 7 Sept 2026 and "Qualified Demo ticked" went *down* to 1
sitting under "The $50 fired" at 2, while a domain whose commission had already
landed read "waiting on an AE" — a false action item for something that can
never fire again.

**Where you want a fact, read `first_ticked_at` / `first_opportunity_at`, never
`sf_state`.** Those two are stamped once by the refresh and never cleared, and
the funnel ORs them with `ps_qualified_sent_at`, which is stronger and older
evidence — a qualification can only ever have fired because the poller saw the
box ticked. That is also what makes the payment tail nest by construction:
Opportunity ≥ ticked ≥ $50, always. `sf_state` stays for the one question it
genuinely answers, which is what Salesforce says right now.

Do **not** backfill those two stamps from `ps_qualified_sent_at`. They mean "we
observed this", and an inferred timestamp in an observational column is read as
a measurement by the next person. The OR in the query is the honest form.

**Nothing reacts to an untick, and that is deliberate** — one-shot claim,
PartnerStack keeps the commission, and re-ticking fires nothing at all. Four
independent things enforce it; `docs/partnerstack.md` lists them. An AE
expecting a reversal is a real support question, not a bug.

**`Partner_Source__c` is the FIRST write this service makes to Opportunity,
and a permission failure looks exactly like a partner with no Opportunity.**
Everything else on that object is read-only, so a rejection has never been
exercised — it succeeds today only because the integration user is a full
System Administrator, which is an open ticket. `updateOpportunityFields`
returns a discriminated result, classifies 403/401 and the field-security error
codes as `permission` (affects every domain, needs Salesforce setup changed)
apart from a per-record failure, and never throws. `PS_SF_OPP_WRITE=false`
stops it from the env without a deploy. It writes only when the value changed,
because otherwise every partner Opportunity is PATCHed every 15 minutes
forever.

**Creating a Salesforce custom field through the Tooling API does NOT grant
access to it.** Both fields created on 4 Sept came back 201 and were invisible
to the user that created them until `FieldPermissions` rows were added. So a
`400 INVALID_FIELD_FOR_INSERT_UPDATE` means "no field-level access", not "no
such field". Create, then grant, then verify with a real round-trip.

**The automated eligibility check is BUILT AND OFF for the MVP.** Rejections are
decided by hand at payout approval. Everything below is dormant behind
`PS_ELIGIBILITY_ENABLED`, default off — set it to the string `true` in the
Railway env to turn it on, no rebuild needed. The conversion call deliberately
does **not** consult it: today every partner lead that is not one of our own
test addresses converts. Wiring the check into that path is a decision for
someone to make, not a tidy-up, and a test asserts the two stay unconnected.

**"Once per domain, ever" is enforced by the DATABASE, not by application
code.** `leads_ps_signup_once_idx` is a UNIQUE PARTIAL index on
`ps_customer_key WHERE ps_signup_sent_at IS NOT NULL`. `runPartnerStackSignup`
CLAIMS the domain with a conditional UPDATE *before* the HTTP call and only the
winner sends; a concurrent claim surfaces as `23505` and is read as
"already sent". Checking then sending would race — two submits for one domain
arriving together would both fire, and PartnerStack cannot undo a double credit.
If the send fails the claim is RELEASED, because a stamp on a conversion that
never arrived is silent, permanent, and costs the affiliate a real payout.

**PartnerStack eligibility FAILS CLOSED, and that is deliberate.** It is the one
check in the repo that does not follow "a lead is worth more than a verdict" —
because it does not touch the lead at all. It decides whether an *affiliate* gets
paid. The conversion call fires once per key forever and cannot be recalled,
while a conversion we skipped is still in the log to send by hand, so a check
that cannot run returns `check_failed` rather than waving it through. No lead is
ever blocked, delayed or de-Meta'd by it.

**PartnerStack eligibility runs AFTER `res.json()`, and must stay there.** It is
fire-and-forget in `/submit`, next to `finaliseElvVerdict`, and it only runs when
`ps_xid` is present. Three things keep the warehouse off a lead's critical path
and all three are load-bearing:

- the rule (b) query is wrapped in `withTimeout(..., PS_CUSTOMER_QUERY_TIMEOUT_MS)`
  — `awsPool` has **no** `statement_timeout`, so an RDS instance that accepts
  connections but answers slowly hangs forever otherwise. Fail-closed does not
  save you here: a hang never reaches the catch. Three hung queries also exhaust
  `awsPool` (`max: 3`) and starve `syncToAWS`.
- the customer cache is warmed at boot and every 30 min by
  `startPartnerStackCacheWarm()`, so no lead ever pays for the fetch. Measured
  cost when healthy: 0.8–3.4s across the WAN.
- eligibility is never awaited in the route.

**Step 5's conversion call goes after this verdict, not before it.** Moving it
earlier to "save a round trip" puts a third-party HTTP call in front of a waiting
lead. `tests/test-partnerstack.js` asserts the ordering and that it is not
awaited; both mutations are caught.

**The contact source behind eligibility rule (a) is a registry, on purpose.**
`PS_CONTACT_SOURCES` / `PS_CONTACT_ACTIVE` in `index.js`. Today it is prior
inbound form leads only. The warehouse also holds two live outbound logs
(`gist.gtm_coldemail_sends_master`, indexed on domain;
`gist.gtm_outbound_multisource`, NOT indexed on `website_url`) which may be
switched on later. Measure before switching one on: against 90 days of real
leads, prior form lead rejects 9.7%, cold email in the 90 days before the submit
25.6%, dials 4.9%. A naive "emailed in the last 90 calendar days" reading looks
like 39.6%, but 17.4 points of that is our own sequencer following UP on an
inbound lead, which is not prior contact.

**Rule (b)'s 12-month clause is unenforceable and says so out loud.** Nothing in
`gw_prod` can date a churn: `customer_contract_terms` has zero churned rows,
`gist_accountsmaster` has an `End_Date` on 2 of 330 rows, and
`public.subscriptions` has no row with a future billing date. Current-customer is
checked; the 12-month half is returned as `unverified: ['customer_last_12_months']`
on every pass so it is visible rather than silently passing. The clause stays in
the affiliate terms — we just cannot enforce it here yet.

**Six verification columns never reach the AWS mirror.** `elv_status`,
`elv_checked_at`, `website_check_failed`, `website_check_reason`,
`website_check_reason_prev` and `website_rechecked_at` exist on Railway `leads`
and not on `gw_form_leads`. The dialer cannot see whether a lead was verified.
Known, not fixed here.

**THE CHANNEL ATTRIBUTION LOGIC LIVES IN SALESFORCE, NOT IN THIS REPO — AND WE
FEED IT.** `Lead.Source_Bucket__c` is a **formula field**. There is no stored
value; it recomputes every time anyone reads it, from exactly two inputs:

| Input | Written by |
|---|---|
| `utm_source__c` | this repo, from the ad click |
| `How_Did_You_Hear__c` | this repo, via `MIRROR_FIELDS` in `salesforce.js` |

So **nothing in this repo decides a bucket, and nothing in this repo can be
grepped to find out what one means.** The logic is Salesforce metadata, read and
written through the Tooling API on `CustomField` `Source_Bucket` /
`TableEnumOrId='Lead'`. Read it there before reasoning about attribution.

**The consequence that will bite: `hear_about_us` is not just a display string.**
It is a production input to somebody else's reporting. Changing its format,
prefixing it, or reusing it for a new purpose silently re-buckets leads with **no
error anywhere** — the formula keeps returning a value, it is just the wrong one.
The `Partner - <key>` and `Referral - <email>` placeholders already flow into it,
which is exactly the kind of prefix that can start matching a branch by accident.

**`How_Did_You_Hear__c` had NO writer at all until 17 Sept 2026.** Whatever
populated it upstream — most likely the Clientell managed package — stopped in
July 2026, and no commit in this repo had ever written it. Every lead that was
not a paid ad click therefore fell through the whole formula to `Others`.
Coverage was 37%; writing it took it to 70%.

**The formula matched two-letter substrings with no word boundary, and filling
the field made that fire more often.** `CONTAINS(..., "li")` sent "client",
"while", "link" and the name "Jolian" to **LinkedIn**; `CONTAINS(..., "ig")` and
`CONTAINS(..., "book")` sent "Right here", "SIG Investor" and "TEST BOOKING" to
**Meta**, from a branch sitting eight above Invalid/Test. Fixed and deployed
16 Sept 2026 (UTC): the short tokens are now space-padded, `_` and `-` are
normalised to spaces with `SUBSTITUTE` first so `meta_ads` and `diag-test` still
match, and Invalid/Test moved to the top of the ladder.

**Verify a formula change by REPLAYING IT, not by reading it.** Implement both
the old and new logic in JS, run them over every real record, and require the
**old** one to disagree with live Salesforce **zero** times before trusting what
the new one predicts. That harness caught three regressions that a careful read
of the formula did not: space-padding alone broke `meta_ads`, `Testing` and
`diag-test`. A formula recomputes on read, so a bad deploy silently rewrites all
of history at once — and so does a good one, which is why no backfill is needed
after fixing it.

**`Source_Bucket_New__c` IS NOT A NEWER VERSION OF `Source_Bucket__c`.** The name
says otherwise and that reading is wrong. It is a writable restricted picklist on
**Opportunity** — `Outbound | Cold Email | Meta | Philly | Others` — and it
answers *which sales motion won the deal*, where the formula answers *which
inbound channel the person arrived from*. That is why it has no Google bucket.
It is **live**: 210 Opportunities in the 60 days to 17 Sept 2026, most recent the
day before. Do not "consolidate" or delete it; outbound reporting runs on it.

**A converted Lead does not always have an Opportunity, so
`ConvertedOpportunityId` is the WRONG join for "did this lead become a deal".**
Whoever converts can tick "do not create an opportunity", normally because the
Account already has one. 37 of our converted Website leads are in that state and
36 of them do have an Opportunity — reachable only through
`ConvertedAccountId`. A backfill keyed on `ConvertedOpportunityId` silently skips
every one of them, which is how 13 blank Opportunity buckets were missed on the
first pass.

**Known open bug:** the duplicate-booking guard looks up the *newest* lead row per
email and asks whether that row has a booking. A second form submission creates a
newer row, so the same person can take two calendar slots. Known, deferred by the
owner — do not "fix" it without asking.

---

## Deploying

`git push` to `main` → Railway builds and deploys. **There is no staging and no CI.**
A push is a production release.

So: work on a branch, run the tests, and only merge when they pass.

### The working agreement: branch, push, PR, WAIT

**Never merge to `main` without being told to, in that message.** Not "the tests
are green", not "the card is written", not "it's behind a flag". The merge is
Darshil's call and it is made on a diff he has read.

The flow, every time:

1. Work on a branch.
2. Run the full bar — `node tests/measure.js --check`, bare, never piped.
3. **Push the BRANCH** (`git push -u origin <branch>`), never `main`.
4. **Open a PR** (`gh pr create`), so there is a diff to review rather than a
   description of one.
5. Write the review card to `cards/` and say it is ready.
6. **Stop.** Merge only when Darshil says merge, in words, in a later message.

Two things this is guarding against, both of which have already happened here:

- **A green bar is not a review.** PR 51 and PR 52 both merged with
  `test-batch2.js` failing, because the bar was piped and nobody read it. A PR
  gives a second surface where that is visible.
- **A push to `main` deploys instantly.** There is no staging, so "merged but
  not released" does not exist in this repo. The moment it lands on `main` it is
  serving real leads, which is why the merge and the release are one decision
  and one person's.

**`Bash(git push *)` is allow-listed in `.claude/settings.local.json`**, so a
push is not gated by a permission prompt — the discipline is the agreement, not
the tooling. If a push is ever refused anyway, run it as a BARE command rather
than chained after other git commands with `;`; a compound command does not
match the allow rule cleanly and falls through to the classifier.

```bash
node tests/test-batch1.js       # logic, no dependencies
node tests/test-batch2.js       # logic, no dependencies
node tests/test-batch-a.js      # logic, no dependencies
node tests/test-ads-parity.js   # the two form files against each other, no dependencies
node tests/test-partnerstack.js # PartnerStack steps 1-10, no dependencies
node tests/test-sf-readers.js   # EXECUTES the Salesforce readers against a stubbed fetch
node tests/test-submit-gate.js       # BOOTS /submit and watches all five announcements fire
node tests/test-session-payload.js   # EXECUTES the real form-file functions, both files
node tests/test-session-page-views.js # BOOTS /session, incl. what happens when the write fails
node tests/test-lead-field-changes.js # BOOTS /partial + /submit, and parses the dashboard JS
node tests/test-non-icp.js           # the non-ICP block: list, matcher, guards, the disqualified AUDIT,
                                    #   and section 13, which EXECUTES the model classifier against a
                                    #   stubbed fetch — fail-open, the enum, injection handling
node tests/test-non-icp-routes.js    # BOOTS every /monitor route AND evaluates the dashboard JS
node tests/test-apollo.js            # BOOTS /enrich against every shape Apollo replies with, and drives
                                    #   tools/re-enrich-apollo.js against a stubbed database

node tests/measure.js --check   # or just this: runs all thirteen and checks the totals
node tests/test-batch1-db.js    # needs DATABASE_URL
node tests/test-batch1-e2e.js   # boots the real server, needs DATABASE_URL
```

**The thirteen dependency-free suites are the bar.** They run anywhere in about a
second each — run all thirteen after any change to `index.js`, `lead-magnet.js`,
or either form file, always. Do not install Postgres and do not point anything at
the production database from a feature branch.

**NEVER PIPE `measure.js`. Not through `tail`, not through `grep`, not through
`head`. Run it bare and read all of it, or do not claim the bar.**

This is a hard rule, not advice, because **a truncated pass and a truncated
failure are indistinguishable**. The grand-total line is the LAST thing
`measure.js` prints, so any pipe short enough to be convenient shows a
plausible total with every per-suite result hidden above it. `tail -3` on a
green bar and `tail -3` on a red bar look identical — and the total does not
move when a suite fails, because a failed assertion still counts toward it.

`--save` compounds it: it re-baselines whether or not suites failed, so one
truncated read plus a `--save` records a red bar as the new normal.

It happened **three times on 10 Sept**, twice after this warning was already
written. Two of those shipped: PR 51 and PR 52 both merged with
`test-batch2.js` failing. Neither changed production behaviour, but neither bar
had actually been read.

If the output is genuinely too long to read, that is a reason to fix the
output, not to pipe it.

**Six of the thirteen BOOT A ROUTE** rather than reading source text —
`test-submit-gate`, `test-session-page-views`, `test-lead-field-changes`,
`test-session-payload`, `test-apollo` and `test-non-icp-routes`. The last one goes furthest:
it also **evaluates the dashboard's inline JavaScript** in a stubbed DOM and
calls every tab loader, because three production breaks in one night were
runtime behaviour no source assertion could see. They stub `pg` and `global.fetch` and drive the real
express stack over HTTP, so they need no database and no network. They exist
because a source assertion cannot tell a reachable statement from an
unreachable one, and cannot tell you whether the dashboard's inline JavaScript
parses at all.

Tests read the real functions out of `index.js` rather than a copy. A test that
exercises a duplicate of the source can pass while production is broken. Keep it
that way.

**All thirteen suites require `tests/crash-reporter.js` first, and it is not
optional.** A suite that crashes prints a stack trace, zero `✗` lines and exits
1 — which reads as a clean run to anything counting markers and as a caught
mutation to anything counting exit codes. Three of the six did exactly that
before 7 Sept 2026, so every mutation result measured by counting markers may
have been counting crashes. The reporter turns any escape into a `✗` line plus
an explicit `SUITE DID NOT COMPLETE` marker, and deliberately prints no totals.

### Mutation testing: use `tests/measure.js`, do not count markers

**Do not measure a mutation by counting `✗` lines or by looking at the exit
code.** Both were the practice here until 7 Sept 2026 and both are broken:

```bash
node tests/measure.js --check       # the normal bar: all ten suites + totals
node tests/measure.js --mutation    # after breaking a line, for a verdict
node tests/measure.js --save        # re-baseline, when a count intentionally moves
```

**A mutation is CAUGHT only if the suite COMPLETED and FAILED and ran its USUAL
NUMBER OF ASSERTIONS.** All three, and the third is the one every ad-hoc check
omits. `--mutation` applies the rule and prints `CAUGHT` / `survived` /
`UNMEASURED`; it exits non-zero on `UNMEASURED`, so a run that could not be
measured can never be recorded as a catch.

`failed > 0` alone is not enough: `test-ads-parity.js` once reported one
failure while silently running **13 of its 159** assertions, which passes every
check based on exit code and failure lines.

**An assertion about ORDER is not an assertion about REACHABILITY.** Roughly
17 assertions in these suites check that X happens before Y by comparing source
character offsets — including claim-before-send on both PartnerStack payment
paths. Every one of them survives an early `return` above the guarded
statement, an `if (false)` around it, or a new wrong guard clause placed above
it, because none of those move an offset. Measured, not assumed: three of three
reachability mutations on the money-path guards survived. So pair the ordering
assertion with a reachability one — assert the enclosing branch is still
guarded by the condition it should be, or drive the function and assert the
call happened. See
`docs/tickets/ordering-assertions-do-not-check-reachability.md`.

**A review card is trusted rather than checked, so a wrong one is worse than a
missing one.** These cards are the record of what was verified and they get
believed. Before writing one: re-read what you actually ran, and do not carry a
claim forward from a previous card without re-checking it. This has already
gone wrong once — PR 25's card said an earlier fix had been made "in passing"
when it had not, because a boot run had been added to a *different* sweep and
the two were conflated. State plainly what was verified, what was asserted only
structurally, and what has never executed; correct an earlier card in the next
one rather than leaving it standing.

**And treat the mutation tallies in review cards and commits before 7 Sept 2026
as UNVERIFIED rather than as evidence.** They were produced with the broken
measurement — three of the six suites crashed with zero markers, so a crash and
a catch were indistinguishable. The catches were probably real; they were never
verified, and which runs crashed cannot be reconstructed. The full account,
including the measured per-suite table, is
`docs/tickets/mutation-testing-measurement-was-broken.md`.

`tests/.baseline.json` is committed on purpose: a baseline nobody can see is a
baseline nobody checks.

**`test-sf-readers.js` is the one that EXECUTES rather than reads.** The others
assert on source text, which keeps them pointed at the real code but cannot
tell you whether it works. That suite requires `salesforce.js` — which needs no
database and no server — and drives it against a stubbed `global.fetch`, so
pagination, the completeness refusals, the `ok: false` branches and the SOQL
that actually goes over the wire are all exercised. Three defects it catches
are invisible to every source-based assertion: a `nextRecordsUrl` resolved
relative instead of against the instance, `records = map(data)` instead of
`concat`, and a paged request that drops the bearer header after page one.

When you add a Salesforce reader, it goes in there too.

---

## Style

Comments explain **why**, anchored to the incident that caused the code. Real
examples in the file: the spam lead that booked without submitting, the Shenzhen
manufacturer blocked by a DNS-over-HTTPS failure, the SiteGround captcha that looked
like a thin page. Match that. A comment restating what the line does is noise; a
comment naming the lead that broke is worth keeping.

Keep the existing structure. This is one large file on purpose for now — don't
reorganise it as a side effect of another change. Splitting it up is a real idea,
but a deliberate one, on its own.

Plain, direct language in Slack alerts and dashboard labels. They're read by SDRs,
not engineers. "Domain registered but no website on it", not `parked_confirmed`.

---

## Working with Darshil

- Say what you changed and what you did **not** change.
- Flag anything that alters blocking or Meta behaviour, loudly.
- If a fix has a half you can't do here (anything in the two frontend files), say
  which half is missing rather than implying it's complete.
- Don't claim something is verified unless it was actually run. "Syntax checks" and
  "tests pass" are different statements.
- When you're unsure whether something is a bug or intentional, ask. This codebase
  has a lot of deliberate-looking oddities that really are deliberate.
