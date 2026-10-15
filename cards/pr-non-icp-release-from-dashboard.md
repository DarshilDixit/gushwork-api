# PR: release a wrongly blocked company from the Blocked tab

Branch `non-icp/release-from-dashboard`, **stacked on #142** (`non-icp/release-and-repeat-alert`),
which must merge first: this needs its `non_icp_exemptions` table and release logic.
15 Oct 2026. **Not merged: waiting for your review.** This lets a person click to unblock a
company, which changes who is blocked, but only for companies a person chooses.

**Why:** #142 added the release as a command in a terminal. You asked for the decision to be
makeable from the dashboard, where the people who need to make it already are.

**What, on the Blocked tab:**
1. **Release…** on each row of "People who tried again". Clicking it READS what a release would
   do and shows it: "3 blocked leads from 1 person (0 already booked) will stop being blocked.
   From now on this company is never blocked, and its Meta events are no longer held back. It
   does not send them to Salesforce and does not message them." Then an optional note and one
   **Release this company** button. Nothing is written before that button. Cancel does nothing.
2. **Released companies** panel: each company with when, by, why and how many leads, an
   **Undo** button, and a box to release by typing a domain ("Check…" shows the same
   confirmation first).
3. **Slack is told** on every release and every undo, because the dashboard link is shared and
   cannot say who clicked. The post is the record. `released_by` is stored as "dashboard".

**Brand-list companies CANNOT be released from here.** Releasing farmersagent.com (7 people in
our data, thousands in the world) would unblock every Farmers agent for good. The row shows
"Brand list: command line only" instead of a button; typing one in shows an explanation with no
Confirm button; and the server refuses it with a 409 regardless of what the page sends. Only
`node tools/non-icp-release.js … --allow-brand` can.

**One implementation.** The routes and the command line use the same functions
(`nonIcpReleasePlan`, `nonIcpReleaseApply`, `nonIcpUnrelease`); the tool lifts them out of
`index.js`. Four new token-gated routes: `GET non-icp-release-plan`, `POST non-icp-release`,
`POST non-icp-unrelease`, `GET non-icp-released`.

**Not changed:** who is blocked, until someone releases a company. The prompt, the floors, the
brand list, Meta, Salesforce. Undo only removes the exemption: leads already cleared stay cleared.

**An audit was extended, in the open.** `test-non-icp.js` 10f forbids any query that filters on
`non_icp_llm_flagged`. The release's two statements legitimately do (they find a released
company's rows in order to CLEAR the flag). They are now a named fourth category with a written
reason, pinned at exactly two, and the test checks they are called only from the two release
routes, so a call from `/submit` would fail the bar.

**Verified, run and read:**
- Full bar, bare, exit 0: 16 suites, 5776 assertions (baseline saved: non-icp 775 -> 803,
  routes 667 -> 689, monitor-next 453 -> 476).
- The shared functions EXECUTED against a recording database: plan reads only; apply is BEGIN,
  insert, update, COMMIT in that order; a failure rolls back and gives the connection back; a
  brand-list domain is refused BEFORE anything reaches the database; undo never touches leads.
- The four routes over HTTP: all 401 without the token; plan, release (transaction, "dashboard",
  the note, the Slack post), brand 409 with nothing written and nobody told, free mailbox 400,
  unrelease, the released list.
- The dashboard flow in the suite's stubbed DOM: Release reads the plan as a GET and writes
  nothing; Cancel writes nothing; Confirm POSTs the domain and note as JSON in the BODY, once;
  a failed write leaves the box OPEN with the note in it; a brand domain, an already released
  company, a company with no blocked leads and a rejected domain each say the right thing and
  offer no Confirm where none belongs; Undo; and a failed refresh of the released list keeps the
  last read and the leads still draw.
- The command line, dry run against production read-only: specialtyprogramgroup.com (3 leads, 1
  person, 0 booked) and farmersagent.com (9 leads, 7 people, "will REFUSE"). `--apply` with a
  brand domain anywhere in the list refuses before writing any of them.
- Seven mutations, all CAUGHT: the dashboard route allowing brand domains, the shared function
  dropping the brand refusal, no rollback on failure, no Slack post, a Release button for
  brand-list people, a Confirm button on a brand domain, and the domain sent in the query
  string instead of the body. (One first read UNMEASURED because the suite crashed on the
  broken request; the test now fails an assertion instead, and it was re-run: CAUGHT.)

**NOT verified, never executed:**
- **Nothing has been clicked in a real browser.** All of the UI is tested in the suite's stubbed
  DOM. CLAUDE.md asks for `tools/preview-monitor.js` and `tools/check-monitor-layout.mjs` before
  merging anything under `monitor/`; I did NOT run them (the preview would need these new routes
  lifted in first). The first real click is the first test of the layout, the focus handling
  and the phone width.
- **`--apply` and the POST release have never written to a real database.** The table does not
  exist until #142 deploys.
- **The Slack posts in real Slack.**

**Known gaps, said plainly:**
- **Anyone with the dashboard link can release or undo.** The token is shared, so there is no
  identity and no permission level. Slack is the record; the confirm box is the only brake.
- The #142 gaps stand: the Model tab still counts a released company by what the model said; a
  release does not push to Salesforce, touch the AWS mirror, fire Meta or message the person.
