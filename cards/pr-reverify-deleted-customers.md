# PR review card — re-verify deleted PartnerStack customers

**Branch:** `fix/reverify-deleted-customers` · **12 Sept 2026** · **Not merged.**

One code change plus one production data edit that has already been made. Written
for someone who was not in the session.

---

## 0. What was wrong

`runPartnerStackConversionVerify` selects `WHERE ps_signup_verified_at IS NULL`.
The moment a row verifies it is never looked at again.

And **the PartnerStack UI is the only place a conversion can be reversed** — this
repo sends no negative, no void and no delete (`docs/partnerstack.md`). So the
one supported way to undo a mistake left us asserting a conversion that no longer
exists, permanently, with nothing anywhere to notice.

That happened on 11 Sept: `allstate.com` was deleted from PartnerStack and the
Partners tab went on reporting it as converted and verified.

---

## 1. There is a better shape, and this is deliberately not it

**PartnerStack emits a `customer_deleted` webhook** (`POST /v2/webhooks` to
subscribe; the payload carries `key`, `email`, `partner_key`). Event-driven would
be instant and cost nothing per domain.

**Not built here, for three reasons:**

- It needs a public unauthenticated endpoint, signature verification, and a
  subscription managed outside this repo — materially more surface than the
  problem warrants for an event that has happened once.
- **Webhook delivery is best-effort.** A missed POST leaves exactly the permanent
  desync this exists to prevent, so a poll is still required underneath it.
- The poll is therefore not throwaway work under either decision. It is the
  backstop whether or not the webhook is added.

**Recommendation: add the webhook later as the fast path, keep this as the
backstop.** Not as a replacement.

---

## 2. Cadence: 7 days per domain, on the existing 15-minute tick

The timer is not the cadence — the **query** is. A row is eligible when its
`ps_signup_recheck_at` is null or older than 7 days, and each tick takes at most
`PS_VERIFY_BATCH` (25) of them.

**Why 7 days:**

- A hand-cleanup in the PartnerStack UI is a rare, deliberate act. A week is soon
  enough to correct a dashboard number and far too slow to matter for anything
  else.
- It is **self-throttling**: the batch cap means cost cannot spike with the
  domain count, only latency can.
- It is far enough from the write that the read-after-write lag which caused the
  7 Sept incident (over 11 minutes) cannot possibly be in play.

**Cost:**

| Verified domains | Re-checks per day | Ticks used (of 96/day) |
|---|---|---|
| 4 (today) | **< 1** | ~0 |
| 100 | ~14 | 1 |
| 1,000 | ~143 | ~6 |

At today's scale this is under one API call a day.

---

## 3. The load-bearing design decision: it demotes, it does not release

On a 404 the sweep clears **only `ps_signup_verified_at`**. That drops the row
back into the *original* verify sweep's population, which then applies its own
grace and, on a second definitive 404 fifteen minutes later, releases the claim
through the path that is already tested and already alerts.

**Two independent 404s, from two different sweeps, before anything is released.**

This matters because of 7 Sept: a customer created at 16:38 still 404'd on a
direct read at 16:49 and verified cleanly at 17:06. **One 404 is never enough to
act on.** A re-check that released claims directly would have reintroduced
exactly the duplicate-credit bug the grace period documents — and worse, a
released claim with a failure stamp is the retry sweep's selection criteria, so
it would have re-fired the conversion too.

Tests assert the negative explicitly: the re-check must **not** contain
`ps_signup_sent_at = NULL` and must **not** stamp `ps_signup_failed_at`.

### The other rule it follows

A non-OK read (timeout, 5xx, network) leaves the row **completely alone** — not
stamped, not demoted. "We could not tell" is never "it is gone". An outage must
not look like a mass deletion.

---

## 4. A new column, not an overloaded one

`ps_signup_recheck_at` is added rather than rotating `ps_signup_verified_at`.

Those are two different observations: *when we first saw it land* and *when we
last looked*. Overloading the first would turn a landing time into a rolling
timestamp, and the next person would read it as the former. Same reasoning
`docs/partnerstack.md` already records for `first_ticked_at` vs `sf_state`.

A test asserts the sweep writes `ps_signup_recheck_at` and never
`ps_signup_verified_at = NOW()`.

---

## 5. The data edit, already applied

**`agent@allstate.com` — my own test row, not a real lead.** Two columns were
asserting something false after the PartnerStack customer was deleted.

| Column | Before | After | Why |
|---|---|---|---|
| `ps_signup_sent_at` | `2026-09-11 18:59:55` | `NULL` | Asserted a conversion exists. `GET /v2/customers/allstate.com` → **404**. |
| `ps_signup_verified_at` | `2026-09-11 19:23:14` | `NULL` | Asserted we read it back. True at 19:23, false by 20:0x. |
| `ps_signup_skipped_reason` | `NULL` | `non_icp_blocked` | Without it the ladder shows `conversion_pending` — "we still owe this partner a conversion", also false. |
| `ps_signup_skipped_at` | `NULL` | `NOW()` | Pairs with the reason. |

**Deliberately left alone:** `non_icp_blocked`, `non_icp_reason`, `ps_xid`,
`ps_partner_key`, `ps_customer_key`. The partner click really happened and the
lead really was blocked — those are still true, and the lead should still read as
a partner lead on the tab.

**The end state is not invented.** It is exactly what `runPartnerStackSignup`
writes today for a blocked lead: no stamps, skipped, reason `non_icp_blocked`.

Applied to Railway **and** the AWS mirror, pinned three ways (email, blocked
flag, customer key) so it could only ever hit one row, dry-run with `ROLLBACK`
first. One row affected on each side.

**Safety check before clearing:** nothing re-fires as a result. The retry sweep
needs `ps_signup_failed_at IS NOT NULL` (null, and now also excluded by
`non_icp_blocked IS NOT TRUE`); the verify sweep needs `ps_signup_sent_at IS NOT
NULL` (now null); `runPartnerStackSignup` only runs from `/submit`.

---

## 6. Partners tab — before and after

| | Before | After |
|---|---|---|
| `totals.conversions` | **5** | **4** |
| `allstate.com` state | `converted` | `skipped` |
| `signup_sent` | `true` | `false` |
| `signup_verified` | `true` | `false` |
| `skipped_reason` | `null` | `non_icp_blocked` |
| **"Waiting on an AE"** (`sfActionable`) | **1** | **0** |
| `byState` | `converted:3, qualified:2, skipped:1` | `converted:2, qualified:2, skipped:2` |

All four things asked about now read correctly.

`bySfState` still shows `exists_unticked: 3`, which is **correct and not a bug** —
there genuinely is an unticked Opportunity on `allstate.com`. That is a
Salesforce fact, and `sf_state` is the one snapshot in this integration that
reports what Salesforce says right now. What changed is that it is no longer
counted as *actionable*, because the ladder state is `skipped` rather than a
payable one.

---

## 7. Verification

- **Bar: 12 suites, 2,785 assertions**, green, run bare.
- **5 mutations, all CAUGHT by 3 suites each:**

| Mutation | Result |
|---|---|
| Releases the claim directly instead of demoting | CAUGHT |
| A failed read treated as a deletion | CAUGHT |
| Demotion removed (verified rows never cleared) | CAUGHT |
| Overloads `ps_signup_verified_at` instead of its own column | CAUGHT |
| Not started at boot | CAUGHT |

**Not verified:** the sweep has never run against a real deleted customer other
than by reasoning — `allstate.com`'s stamps were cleared by hand before this
shipped, so there is nothing left for it to find. The first genuine exercise will
be the next time somebody deletes a customer. That is the honest state.

---

## 8. Weak points

1. **The sweep has never fired in anger.** Every assertion is executed or
   mutation-tested, but no real disappearance has passed through it.
2. **Seven days is a judgement, not a measurement.** The event has happened once.
   If hand-cleanups turn out to be common, shorten it; the cost table above says
   what that buys.
3. **The webhook remains the better primary.** This is a backstop shipped as a
   whole solution, and it will read as sufficient to whoever comes next unless
   the follow-up is actually picked up.
4. **`ps_signup_recheck_at` is not mirrored to AWS.** Nothing off-box reads it,
   but it is one more column that exists on Railway and not on `gw_form_leads`,
   which is a list that is already six long.

---

## 9. Rollback

The sweep is additive and idempotent. To stop it, remove the
`startPartnerStackConversionRecheck();` line and deploy — nothing else reads
`ps_signup_recheck_at`, and leaving the column costs nothing.

The data edit is not automatically reversible. The prior values are recorded in
§5 above if anyone ever needs to restore them, though restoring them would only
reinstate two false assertions.
