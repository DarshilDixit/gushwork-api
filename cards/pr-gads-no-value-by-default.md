# PR: the Google upload sends no conversion value by default

Branch `gads/no-value-by-default`, from `main` at `1138b5a`. 13 Oct 2026.
**Not merged: waiting for your review.**

**Why:** Lorenzo (Upraw) said he can set a flat default value on the new
action for uploads that carry none. Until now every Google event carried the
product's predicted value (12000 AEO / 5000 CRM / 15000 both, all guesses),
and Google keeps a sent value for good: it cannot be edited afterwards.

**What:** `gadsValueFor` returns null unless the Railway variable
`GADS_SEND_VALUE` is exactly `true`. With no value, `buildGadsEvent` already
leaves out `conversionValue` and `currency`. Setting `GADS_SEND_VALUE=true`
brings the old product values back.

**Not changed:** Meta (its `value: 0` and `predicted_ltv`), blocking, which
leads are uploaded, the cutover, the click-ID rules, validate-only. Nothing
in this PR sends anything: the upload is still validate-only.

**Verified:** full bar, bare: 16 suites, 5584 assertions, 0 failed
(test-gads-upload 213 to 215, re-baselined). The suite runs the real
`gadsValueFor`: default sends no value and no currency; any value other than
the exact string `true` sends none; `true` sends 5000 CRM and 12000 AEO.
Mutation: deleting the guard line CAUGHT (2 failures).

**Never verified:** that Google applies the action's default value when an
event has no value. Lorenzo says it does. The first real events will show it
in the Google Ads conversion value column.

**Existing rows:** the 171 `validated` rows are rebuilt when real sending
starts, so they pick this up. Rows already `sending` or `sent` keep their payload.
