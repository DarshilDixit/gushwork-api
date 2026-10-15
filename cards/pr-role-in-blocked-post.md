# PR: show the person's role in the blocked-lead Slack post

Branch `non-icp/role-in-blocked-post`, from `main` at `de06539`. 15 Oct 2026.
**Not merged: waiting for your review.** Changes what a Slack post SAYS. Changes nobody's blocked
status, no Meta event, no email.

**Why:** on inlandgroup.com (1,200 staff) Swapnil's first question was "is the guy booking an agent
or not?" and the post made you go and look. I told him in the thread I would add it.

**What:**
- A **💼 Role** field under Company on every blocked-lead post (not only the large-company one):
  Apollo's title, seniority and department, e.g. `Vice President of Leasing (vp, operations)`.
  `master_` and underscores are tidied. With no title it says `Title unknown (owner, operations)`; with
  nothing from Apollo the field is simply absent.
- It is **text, not a verdict.** No "agent" or "buyer" label: a title heuristic would be one more thing
  deciding on a guess. The blocked posts so far suggest the split is visible to a human at a glance
  (agents read Agent / Licensed Real Estate Agent / Luxury Property Specialist; buyers read VP,
  Director, Marketing).
- The release instruction now says the dashboard first ("type the domain in the Released companies box
  and press Check…") and the command line second. The Release button is only on repeat attempts, so a
  first-time large company has to be typed in.

**ESCAPED FOR SLACK.** The title is not ours: Apollo reads it from LinkedIn, which the person controls,
and `<!channel>` in Slack markup pings everyone. `& < >` are escaped in title, seniority and
department.

**Not changed:** who is blocked, the floors, the prompt, Meta, Salesforce, the emails.

**Verified, run and read:**
- Full bar, bare: 16 suites, 5805 assertions (baseline saved: non-icp 807 -> 815, routes 699 -> 705).
- The role function EXECUTED: full line, tidied departments, title alone, no title, nothing from Apollo
  (null), non-string inputs, and a hostile title, seniority and department (`<!channel>`, `<!here>`,
  `<!everyone>`, `<@U123|x>`, `&`) all escaped.
- Over HTTP: the role shows in a large-company post; in an ordinary first-attempt post (199 staff); is
  absent when Apollo has no title; and a hostile LinkedIn title cannot ping the channel.
- Three mutations, all CAUGHT: escaping removed, the field dropped from the post, the submit route no
  longer passing the role.

**NOT verified, never executed:**
- **The post in real Slack.** The text and field are asserted; I have not seen the rendered message.

**An existing gap I found and did NOT fix (from reading the code, not tested):** the post's other
free-text fields (name, company, about-business, "heard about us") are not escaped, and the visitor
types them. Someone could put `<!channel>` in the company box on the demo form and ping
#bot-inbound-leads. It predates this change and affects the other lead posts too. Worth its own PR;
not widened here.
