# PR: print / sign withholds Meta for LOCAL shops only (prompt v2)

Branch `non-icp/local-print-only`, from `main` at `4ef8f45`. 14 Oct 2026.
**Not merged: waiting for your review. This CHANGES WHICH LEADS FIRE META.**

**Why:** coverlabel.com (a label and sticker manufacturer, 14 staff) was judged
"Print / sign shop" at 95% and its Meta events were withheld. Swapnil: it
should apply to local printers only. The prompt gave the model no boundary
for print_sign, so any label, sign or promo company could land there.

**What:**
- `NON_ICP_SYSTEM_PROMPT` now says print_sign is ONLY a local print or sign
  shop (one business or one franchise location serving its own city or
  region), names what it is NOT (manufacturers, trade-only suppliers,
  promo-products distributors, online or nationwide brands, print software
  or procurement) and says "if the page does not show a local shop, do not
  pick print_sign". `NON_ICP_PROMPT_VERSION` is `v2-2026-10-14`.
- `tools/non-icp-rejudge-print.js`: re-judges the cached print_sign verdicts
  under the new prompt. Dry run by default.

**Not changed:** the enum, `suppresses`/`blocks` per type, the floors, the
name-only prompt, blocking (print_sign never blocked), restaurants, spas and
home services. The other three industries have the same "local" question
and are untouched until Swapnil answers.

**IMPORTANT: merging alone does not change the 78 cached verdicts.** The
cache ignores `prompt_version`; a page verdict lasts 180 days. New domains
are judged under v2 from the deploy. The old ones change only when the tool
is run with `--apply`, which is a separate step and a production write.

**Measured (dry run on 14 Oct, live pages, current model, nothing written):**
78 cached print_sign page verdicts. Suppressing Meta (at or above the 0.75
floor): **42 before, 16 after.** 51 moved to another type (mostly
b2b_services, some manufacturing), 19 stayed print_sign, 8 could not be
re-read this time (4 of them are suppressing today and keep their old row,
so "16" includes those 4). coverlabel.com: print_sign 0.95 -> manufacturing
0.85, no longer suppressed. pfifashions.com (the September false positive)
-> manufacturing, no longer suppressed.

**Things the replay shows that need a human:**
- **Some genuinely local shops fell BELOW the floor** and stop suppressing:
  bannerville.com 0.72, nycbanners.com 0.60, savorasigns.com 0.50. The model
  is less sure of "local" than it was of "print shop". Not fixed by lowering
  the floor (that is a decision, not a tune).
- **Dallas Sign & Lighting** (1,200 staff, $370M) stays print_sign at 0.88.
  The prompt has no size rule: headcount comes from enrichment after Meta
  decides, and Cover Label shows size alone cannot separate them.
- Promo-products resellers now read b2b_services, so they no longer suppress.
  That is my reading of "local printers"; Swapnil has not confirmed it.

**Verified:** full bar, bare: 16 suites, 5586 assertions, 0 failed
(test-non-icp 709 to 711, re-baselined). The two new assertions pin the
WORDING and the version; they cannot show model behaviour. Mutation:
deleting the "do NOT pick print_sign" line CAUGHT. Model behaviour is
verified only by the replay above, on one run. Models can answer differently
on a second run (temperature cannot be pinned), so borderline domains could
move.

**Not done:** `--apply` has not been run. The tool has run dry only.
