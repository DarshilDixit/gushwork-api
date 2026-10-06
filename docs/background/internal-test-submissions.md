# Background: Our own test submissions

Moved here from `CLAUDE.md` on 7 Oct 2026, word for word, to bring that file under its size limit. `CLAUDE.md` keeps the rule for each of these as one line with a link here; this file keeps the history, measurements and worked examples behind it. Dates and counts are as of when each paragraph was written, and "today" means that day. Line numbers in the headings are `CLAUDE.md` at commit b924ce7.

## The three harms, b@g.ai, the website gate (was `CLAUDE.md` lines 1773-1793)

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

## The page is a signal: the staging host (was `CLAUDE.md` lines 1804-1817)

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

## Nothing is lost when it fires (was `CLAUDE.md` lines 1825-1832)

**Nothing is lost when it fires.** The lead is still written to `leads`
and still appears on the dashboard; it is only not propagated outward. So
a real person who is sent a staging link is visible to us, just not
auto-pushed. The one row that looked like a real company was checked
rather than waved through — `hari@productledsales.io` landed directly on
`/demo-testing-rh` with referrer "direct" on 18 June, the same day two
staff were testing that exact page.
