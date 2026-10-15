# PR: highlight likely buyers on the Blocked tab, and show where each person came from

Branch `monitor/blocked-buyer-hints`, from `main` at `7073d91`. 15 Oct 2026.
**Not merged: waiting for your review.** A dashboard change. Nobody's blocked status changes, no email,
no Meta event.

**What you asked for:** highlight the role ones, the eligible ones, on the dashboard; and show the source
or "heard about us" in that section.

**What it does:**
1. **A role hint.** `roleHint(title, seniority, departments, size)` marks a person **Likely buyer** or
   **Agent-type title**, or neither.
   - AGENT-TYPE: the title says agent, broker, realtor, associate, specialist, producer, licensed, loan
     officer, advisor or intern. Checked first, so "CEO / Mortgage Broker" is an agent whatever the level.
   - LIKELY BUYER: not agent-type, and a corporate level (VP, director, head, C-suite, manager) or a
     marketing/growth department, at a company of 20+ staff when Apollo knows its size ("President" of a
     2-person agency reads as VP-level and is not a buyer).
   - Owners, founders and partners get NO hint: they are the people the block is for.
2. **Highlighted:** a likely buyer's row is tinted (the existing "good" colour) with a "Likely buyer"
   badge, in **Needs a look** and in the **main Blocked list**. Agent-type titles get a quiet grey badge.
3. **A filter in Needs a look:** Everyone / Likely buyers, with counts. Client-side, reads nothing new.
4. **"Came from" column in Needs a look:** the ad source and medium over what they said when asked how
   they heard of us ("google / cpc" over "Heard: Google Ads"), falling back to the referrer. The campaign
   name is in the tooltip: a branded search says Brand (inlandgroup.com came from `UR_G_S_US_BR_Brand`).
   The feed (`/monitor/non-icp-review`) gained the five fields, latest non-empty answer each.
   To keep the table inside its card, the last-try date moved under "Why it is here".

**IT IS A HINT, AND IT SAYS SO:** in the badge tooltip, in the footer, and here. I earlier declined to put
an agent/buyer label in the Slack post for exactly this reason: a title heuristic is a guess. On the
dashboard you asked for it, a person reads every row, and it decides nothing. **It is imperfect:** on real
data it marks "Vice President" of a small insurance consultancy whose size Apollo does not know, and it
marks the Director at Anthem (98,000 staff), a national carrier your original rule was written to stop.
It describes the PERSON, not whether the company should be let in.

**Measured on the 15 large blocked companies (15 Oct):** it marks the VP of Leasing, the CMO, the
Director, both Product / Marketing Managers and the VP of Benefits, and none of the nine agents,
brokers and specialists.

**Not changed:** who is blocked, the release logic, Slack, Meta, the emails. The hint is dashboard-only.

**Verified, run and read:**
- Full bar, bare: 16 suites, 5855 assertions (baseline saved: routes 712 -> 715, monitor-next 496 -> 517).
- The changed SQL EXECUTED against production, read-only: 26 people, with sources (e.g. Inland on
  `google / cpc`, branded campaign; most agents on `facebook / paid`).
- The hint tested on the real titles from our data: buyers, agents (even at a huge company), "CEO /
  Mortgage Broker" an agent, a 2-person "President" not a buyer, unknown size still a buyer, owners and
  founders no hint, nothing known gives null.
- Dashboard (stubbed DOM): tinted rows, badges, the tooltip wording, the footer, the filter with counts
  (pressed state, only buyers, no server read, a plain sentence when there are none), the Came from
  column (source/medium, heard-about-us, campaign tooltip, referrer fallback, Unknown), the date under the
  reasons, and the main list tinting the VP row.
- Real Chrome on live data via the read-only preview: **14 layout combinations (390 to 1440px, light and
  dark) with 0 findings**; screenshots taken and looked at (light), kept in the OS temp folder, not the repo.
- Six mutations, all CAUGHT: the agent check dropped, the size line dropped, buyers not tinted in Needs a
  look, the filter showing everyone, the Came from column dropped, the main list not tinting.

**NOT verified:** a real click on the filter in a real browser; dark mode by eye (the checker ran it,
I only looked at light).
