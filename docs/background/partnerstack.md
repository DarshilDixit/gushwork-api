# Background: PartnerStack sweeps, identity and eligibility

Moved here from `CLAUDE.md` on 7 Oct 2026, word for word, to bring that file under its size limit. `CLAUDE.md` keeps the rule for each of these as one line with a link here; this file keeps the history, measurements and worked examples behind it. Dates and counts are as of when each paragraph was written, and "today" means that day. Line numbers in the headings are `CLAUDE.md` at commit b924ce7.

## Partner identity, the blocked lead's Slack post, the display chain (was `CLAUDE.md` lines 2199-2236)

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

## Partner revenue gaps (was `CLAUDE.md` lines 2320-2348)

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

## The step 10 poller and the three intervals (was `CLAUDE.md` lines 2349-2366)

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

## sf_state is a snapshot; first_ticked_at; unticks (was `CLAUDE.md` lines 2376-2400)

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

## Partner_Source__c, the first Opportunity write (was `CLAUDE.md` lines 2401-2412)

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

## Eligibility rule (a)'s contact sources, and rule (b) (was `CLAUDE.md` lines 2465-2483)

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
