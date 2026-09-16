# PR — Salesforce: an outage is not a lost lead, a converted lead is not a missing one

Branch `fix/salesforce-outage-and-converted-lead`. **Not merged.**

From four real alerts on 16 Sept 2026. Two causes, and **both messages were
wrong** — one of them dangerously.

---

## What actually happened

| time (ET) | alert | real cause |
|---|---|---|
| 08:19 | 🚨 Lead not created — `Unexpected token < in JSON at position 0` | Salesforce maintenance window |
| 08:33 | ⚠️ PartnerStack SF read — `http_503` ×3 | same outage (this one was correct) |
| 11:01 | 🚨 Lead not created — `CANNOT_UPDATE_CONVERTED_LEAD` | returning customer |
| 11:01 | ⚠️ Booking not recorded — same error | same |

### One — an HTML maintenance page parsed as JSON

Salesforce answers an outage with `We are down for maintenance` and a 503. Both
write paths called `res.json()` on it, so `JSON.parse` choked on the `<` and the
critical alert read **"Unexpected token < in JSON at position 0"** — which reads
as a bug in this repo rather than a third party being down.

`getSalesforceToken` always got this right: it checks `res.ok` and reads
`.text()`. The write paths never did. So one path reported the outage in plain
words and the others reported a parse error, **for the same minute of the same
outage**.

The lead it named was genuinely lost: `gregory.ingalls@gmail.com` had **booked a
demo** and reached Salesforce not at all. There is no retry and no sync-state
column, so nothing would ever have picked it up.

### Two — a converted Lead reported as a missing one

`findSFLeadByEmail` takes the newest Lead for an address with **no
`IsConverted` filter**. `mdorf@performancemediastrategies.com` first came in on
23 June; that Lead was **converted on 29 June** to a Contact, Account and
Opportunity. They returned on 16 Sept, we found the converted Lead, and
Salesforce refused the update.

The alert said **"This lead is NOT in Salesforce. Add it manually."** That is
false, and acting on it creates a duplicate Lead against a live Account. The
warning fired in the same second and said the opposite — "the lead exists in
Salesforce but the booking is missing" — which was true.

**Measured:** 403 of 807 people who submitted in the last 30 days have a
converted newest Lead, but the write only fails when they submit *after* the
conversion date — **3 submits and 1 booking in 30 days**. Rare, recurring, and
growing as more Leads convert.

---

## What changed

| | |
|---|---|
| `readSfBody` | reads the body **once as text**, parses it itself, and returns a named `SALESFORCE_UNAVAILABLE` shaped like Salesforce's own error array so `sfUnknownFields` and every caller work unchanged |
| create + update paths | both use it; neither parses an unchecked body |
| `sfConvertedLeadError` | recognises `CANNOT_UPDATE_CONVERTED_LEAD` |
| `updateSFLead` | throws a **flagged** error for it, whose message itself warns against adding a duplicate |
| `salesforceFailureAlert` | **one function, six call sites** |

**Six** call sites raised these alerts — three "Lead not created", three
"Booking not recorded" — and every one hardcoded the same two impact sentences.
They are now one function, for the reason this repo keeps relearning: *a guard
added to the obvious site misses its siblings*. Same shape as the
`nonIcpScheduleSuppressed` trio.

**Both cases are downgraded from critical**, because nothing is lost in either.
A genuine failure is still critical and still says "add it manually".

**This changes what we SAY, not what we write.** No lead is created, skipped or
routed differently.

---

## Verified

**Bar green, run bare:** 12 suites, **3511 assertions** (+24).

**Executed, not read** — `readSfBody` is driven against four real response
shapes (maintenance HTML, non-maintenance HTML, a genuine Salesforce error
array, a success), and `salesforceFailureAlert` is driven for all four branches
with the resulting severity and impact sentence read back.

### A test fixture that had stopped modelling reality

The section 16 stubs defined `.json()` but **no `.text()`** — so the moment the
code read text, they were no longer `Response` objects at all and the suite went
red. That is the fixture being wrong, not the change: a real `fetch` Response
carries both, and a body can only be consumed once. The harness now derives
`text` from `json`, so a new fixture cannot forget it.

### Mutation testing — 6 mutations, all CAUGHT

| # | Mutation | Result |
|---|---|---|
| 1 | Create path parses an unchecked body again | CAUGHT |
| 2 | A maintenance page is no longer named as an outage | CAUGHT |
| 3 | The converted lead stops being recognised | CAUGHT |
| 4 | The converted case says "add it manually" again | CAUGHT |
| 5 | A converted lead is paged as critical again | CAUGHT |
| 6 | One call site reverts to a hardcoded alert | CAUGHT |

---

## Done outside this PR, in production Salesforce

- **`gregory.ingalls@gmail.com` recovered** — Lead `00QOX00000rYCE22AO`, owner
  Nayrhit B, `LeadSource: Website`, `Product__c: aeo,crm`, booking 18 Sept
  attached, `completed__c: true`.
- **`mdorf@…`'s booking logged** — Task `00TOX000011YdMM2A0` on Contact
  `003OX00000hsWgXYAU`, against the Opportunity, **assigned to Akshaya S**,
  due 22 Sept.

Also confirmed while checking: `Product__c` is populated correctly for **all 7**
both-ticked leads that reached Salesforce. The restricted picklist accepts
`aeo,crm`. No product bug.

## Not done / open

1. **No retry and no sync-state column.** A Salesforce outage still loses leads
   silently; recovery is `backfill-sf.js`, which is not mounted. This PR makes
   the alert honest, it does not make the write durable.
2. **`findSFLeadByEmail` still has no `IsConverted` filter.** Deliberate — making
   it create a fresh Lead for a returning customer is an AE workflow decision,
   not a bug fix.
3. **The booking fields exist only on `Lead`.** Nothing on Contact or Opportunity
   can hold `booking_uid__c` et al., which is why the recovery above is a Task.
4. **Jess Pinote is a deactivated Salesforce user** and still owns mdorf's
   Opportunity *and* Contact — a live account with a demo next Tuesday. Outside
   this repo; worth checking how many other records she owns.

## Deploy

Server-side only. `git push` to `main` → Railway. **No Webflow step.**
