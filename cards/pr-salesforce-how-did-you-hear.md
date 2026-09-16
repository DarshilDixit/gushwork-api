# PR review card — write `How_Did_You_Hear__c` to Salesforce

Branch: `fix/salesforce-how-did-you-hear` · commit `bfadab4`

## What this changes

One thing, in `salesforce.js`: a `MIRROR_FIELDS` map that copies the lead's
`hear_about_us` answer into the Salesforce field `How_Did_You_Hear__c`.

```js
const MIRROR_FIELDS = { hear_about_us: 'How_Did_You_Hear__c' };
```

It is applied at the END of `buildLeadFields`, after the `CUSTOM_FIELD_MAP`
loop, so it inherits the same truncation the mapped fields already get. When the
answer is empty the key is omitted entirely rather than sent as `null` — an
empty string would overwrite a value a human may have typed in Salesforce.

Nothing else moves. No route changes, no blocking change, no Meta change.

## Why

`Lead.Source_Bucket__c` in Salesforce is a **formula** that reads exactly two
inputs: `utm_source__c` and `How_Did_You_Hear__c`. We have always written the
first and **never** written the second — not in any commit in this repo's
history. Whatever used to populate it upstream (most likely the Clientell
package; Neil Clientell modified 2,403 of our 3,555 leads) stopped in July 2026.

So every lead whose channel is not a paid ad click fell through the formula to
`Others`. Coverage of `How_Did_You_Hear__c` was **37%**.

## What was actually run

**Verified by execution:**

- The full bar, bare, not piped: **12 suites, 3561 assertions, all passed.**
- `tests/test-batch2.js` gained assertions that drive `buildLeadFields` and
  read the produced payload — the empty-answer omission, the truncation, and
  that the mirror lands under the Salesforce API name.
- The backfill itself ran against the live org: **1,179 of 1,180** unconverted
  leads updated. The one failure was `CANNOT_UPDATE_CONVERTED_LEAD`.
- **278 of 278** Opportunity corrections applied, under the rule *only where the
  Opportunity's current value still equals the old Lead formula value* — so a
  value a human had edited was never overwritten. **0 closed-won touched.**
- End state read back from Salesforce: Others 767→498, Meta 1341→2515,
  Google 120→238, LinkedIn 0→111, Referral 0→91, Email 0→38,
  Organic Search 0→24. Coverage **37% → 70%**.

**Asserted structurally only:** nothing specific to this change.

**Never executed:** no new alert path is introduced, so nothing needed firing.

## Rollback

`scratchpad/backfill-snapshot.json` holds the pre-backfill values.
`scratchpad/opp-plan.json` holds the Opportunity plan as applied.

## What this does NOT fix — read this before merging

Writing the input does not fix the formula that consumes it, and **the formula
has precision bugs that this change makes fire more often.** It matches
two-letter substrings with no word boundary:

- `CONTAINS(..., "li")` in the LinkedIn branch — so "client", "while", "link"
  and the name "Jolian" all resolve to **LinkedIn**.
- `CONTAINS(..., "ig")` and `CONTAINS(..., "book")` in the Meta branch — so
  "Right here", "SIG Investor" and "TEST BOOKING" all resolve to **Meta**.
  The Meta branch sits eight branches above the Invalid/Test branch.

Measured on live data after this backfill: **49 leads** reach their bucket
through one of those short substrings, of which about 14 distinct answers are
genuinely misfiled. That is ~2% today and it grows with every lead, because the
field this PR fills is the field those substrings are matched against.

The corrected formula is written and simulated but **NOT deployed** — the
deploy was blocked by the local permission classifier. See
`scratchpad/formula-new.txt` (the replacement),
`scratchpad/formula-ROLLBACK.json` (the current one, for reverting) and
`scratchpad/deploy-formula.js` (the deploy, which needs to be run by hand).

Simulated against all 4,708 leads carrying either input, the replacement moves
**18 leads**, every one of them a correction. The simulation was validated by
replaying the OLD formula over the same 4,708 leads and confirming **zero**
disagreements with what Salesforce actually shows.

## Also still open

- The dead upstream writer (~41,300 leads/month of Salesforce-wide volume is
  affected, not just our 426) has not been restarted or replaced.
- `Source_Bucket_New__c` — restricted picklist, bulk-loaded 186,724 rows in
  June, 13 rows in September, no Google bucket. Dead weight; not removed.
- 37 converted leads have no Opportunity at all, so their Lead bucket cannot be
  corrected through any writable surface. They stay wrong.
