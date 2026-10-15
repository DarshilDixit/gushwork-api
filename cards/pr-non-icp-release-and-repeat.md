# PR: release a wrongly blocked company, and see who tried again

Branch `non-icp/release-and-repeat-alert`, from `main` at `9908e3f`. 15 Oct 2026.
**Not merged: waiting for your review.** This adds a way to UNBLOCK named companies,
which is the only change to who gets blocked, and it takes effect only when someone
runs the tool. A second table is created when it deploys.

**Why:** specialtyprogramgroup.com (300 staff, an insurance group) was blocked three
times in two weeks, its contact wrote in to ask why nobody had replied, and the only
remedy anyone had was `NON_ICP_LLM_BLOCK=false`, which stops ALL model blocking.
Nothing told a human that the same person kept coming back.

**What:**
1. **Release one company.** New table `non_icp_exemptions`. A company in it is not
   blocked, flagged or withheld from Meta, by the brand list OR the model. It is
   checked first in `nonIcpVerdict` and inside `nonIcpLlmCachedVerdict`, which the
   late-verdict sweep, the Schedule guard and the Google upload all read through, so
   a released company is not re-stamped blocked the moment it books. The warm path
   skips released companies. Cached for a minute.
2. **`tools/non-icp-release.js`**: dry run by default, `--list` shows who they are,
   `--apply` writes (the exemption and clears the block and flag on that company's
   existing leads, in one transaction). Refuses a shared brand-list domain
   (farmersagent.com is 7 people in our data, thousands in the world) without
   `--allow-brand`.
3. **Slack:** the existing blocked-lead post says "Blocked person tried again",
   "Attempt N from this person", the first date and the exact release command,
   when the same email has been blocked and submitted before. Still ONE post.
4. **Dashboard:** a "People who tried again" panel on the Blocked tab, above the
   leads: person, company, tries, last try, what we judged, staff. Its own read, so
   if it fails the leads still draw.

**By person, not by company, and that was measured:** my first version counted by
company domain. Against production it showed `farmersagent.com` at 9 "tries" and
`allstate.com` at 8, which are different agents, not one person retrying. Counting
the same email gives 16 people; SPG's three tries were one person.

**Not changed:** who is blocked, the prompt, the floors, Meta, Salesforce, the
brand list. Nothing is released until someone runs the tool.

**Verified, run and read:**
- Full bar, bare: all 16 suites pass, 5689 assertions (baseline saved: non-icp 751 -> 775,
  routes 631 -> 653, monitor-next 441 -> 453).
- Over HTTP, each with a control that is the same request without the release: model
  block released; brand-list block released (email side and website side); a different
  brand still blocks; Meta Lead fires again for a released company; no blocked post.
- The repeat banner over HTTP (attempt 3 vs a first attempt), still one post.
- The exemption functions EXECUTED: website side, email side, subdomain, lookalike,
  free mailbox, empty list, one read for three leads, and a failing database keeps
  the last good list.
- The new SQL EXECUTED against production in a read-only transaction (the report
  returned 16 people, SPG's contact at 3). The tool's dry run against production
  for specialtyprogramgroup.com: 3 leads, 1 person, 0 booked, and for
  farmersagent.com: refused without `--allow-brand`.
- Six mutations, all CAUGHT: release not applied in `nonIcpVerdict`, not applied in
  the shared reader, warm path re-judging, the repeat banner never shown, the
  dashboard counting our own tests, the Blocked tab dropping the panel.

**NOT verified, never executed:**
- **`--apply` has never run.** The table does not exist in production until this
  deploys, so the tool refuses `--apply` now (tested). Its first real use is the
  first test of the write. Run the dry run, then `--apply` for one company.
- **The dashboard panel in a real browser.** It is tested in the suite's stubbed
  DOM only. CLAUDE.md asks for `tools/preview-monitor.js` and
  `tools/check-monitor-layout.mjs` before merging anything under `monitor/`; I did
  NOT run them. The preview proxies unknown routes to production, so it would need
  `nonIcpRepeatsReport` lifted into it first.
- **The Slack post in real Slack.** The text is asserted; the Block Kit message has
  not been seen rendered, and no real blocked person has triggered it yet.

**Known gaps, said plainly:**
- **The Model tab still counts a released company's leads by what the model
  said** (it reads the verdict table, not the exemptions). The Blocked tab, Meta,
  Schedule, the sweep and the Google upload all respect the release.
- **A release does not push the leads to Salesforce** (`backfill-sf.js` does that),
  does not touch the AWS mirror (the dialer feed still says blocked, and does not
  read it yet), does not fire Meta, and does not message the person.
- **If the database cannot be read on a cold start, a released company is blocked
  again until it can.** A failing read mid-run keeps the last good list. That is the
  one direction this fails closed.
- **Brand-list repeaters appear in the panel** (an Allstate agent who tried four
  times). That is intended: a person who keeps trying deserves a reply.
