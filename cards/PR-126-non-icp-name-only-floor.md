# PR 126 — a name-only guess answers to its own floor, for Meta too; floor 0.85

Branch `fix/non-icp-name-only-floor`, from `main`. Written 26 Sept 2026.
**Not merged — waiting for your review.** Your decisions (a), (b) and (c)
from this afternoon, built as asked.

## What happened (hernandezins.com, 26 Sept)

- The email domain `hernandezins.com` doesn't load, so the model judged the
  **name alone**: insurance, 82%, from the letters "ins". That's below the
  90% needed to block a name-only guess, so it correctly did **not** block.
- The website they typed, `elena-hernandez.com`, is a real page: a
  business-funding coach (mortgage lending, 45%).
- **But it still lost its Meta events.** The Meta-only check used the
  page's 75% floor for every verdict.
- **Slack called insurance** "one of the four that suppress Meta but never
  block". Insurance is one of the two that do block.
- **The lead row said `llm`** ("read their website").

## ⚠ THIS CHANGES WHICH LEADS ARE BLOCKED AND WHICH FIRE META

Measured over every cached verdict (read-only, 26 Sept).

**(a) Meta: a name-only guess needs the name floor, as blocking does.**
Of the 40 name-only verdicts so far, **3 would now fire Meta** where they
did not:
- `hernandezins.com`, insurance, 0.82;
- `planrightlegacyins.com`, insurance, 0.82;
- `cbmovingandstorage.com`, home services, 0.80.

**(c) Name floor 0.9 → 0.85.** **2 more would block:**
- `homes.com`, real estate, 0.88: a property-listing site;
- `wwwplanrightlegacyins.com`, insurance, 0.85: the www-typo of the agency
  that slipped through on 23 Sept.

**All 3,427 verdicts read from a real page decide exactly as before:**
315 blocks, 253 Meta-only, the rest nothing.

**No existing block goes away.** Railway sets neither floor, so the code's
default is what applies.

**The new floor applies at once, not after six hours.** A cached verdict's
stored "blocking" flag was set when the domain was judged. Blocking is now
also read from the type and its floor at decision time, as Meta already was.
Without that, a name-only row stored under 0.9 at 0.85–0.9 would have
withheld Meta without blocking until its six hours ran out. Page verdicts
read the same either way (measured). No stored row is rewritten.

**The booking routes follow the floor too.** Their code isn't changed, but
the no-form-row booking refusal reads the same verdict. So a name-only guess
at 0.85–0.9 now also refuses a booking that arrives with no form row.

## What changed (all in `index.js`)

- **`nonIcpFloorFor(source)`** is the one place a read picks its floor:
  - the Meta-only check uses it (the fix for (a));
  - so do the Model tab's near misses.

  Blocking at write time already used the right floor for each source.
- **`NON_ICP_NAME_CONFIDENCE_FLOOR`** now defaults to 0.85, with the misses
  recorded in the comment. Railway can still override it.
- **The lead row records `llm_name_only`** for a name-only verdict. The
  dashboards already had a label for it ("AI check (name only)"); nothing
  ever wrote it.
- **The blocked Slack post** asks `nonIcpSourceIsModel`, never `=== 'llm'`.
  Otherwise the new source value would have filed a name-only block under
  the brand list. For name-only it says "Their website did not load, so we
  judged the domain name alone", and labels the quote "The part of the name
  it read".
- **The Meta-only Slack post** says the same for name-only. Its reason is now
  built from the verdict: it names the industry ("restaurant / food service
  is one of the four…"), so it can't claim the wrong one again.
- **The late-verdict post** (the sweep's "Booked lead is non-ICP", the only
  thing that tells a human about a booked meeting) said "Read their website"
  and "quoted from their site" for every verdict. For name-only it now says
  the name was judged. This third post was missed in the first version and
  found by the review.
- **The blocked post's wording follows the evidence it shows.** The quote
  comes from the fresh verdict, while the lead row keeps its first decision.
  So a lead recorded name-only at step 1 and blocked from its website at
  submit no longer reads "judged the domain name alone" above a quote from
  the page.
- **`tools/fire-non-icp-slack.js`** copies the two names the blocked post
  now calls. Without them, its `blocked` and `llm-blocked` fires died with
  "not defined". Also found by the review.
- **CLAUDE.md** is corrected: the floor, the one-floor-per-evidence rule, and
  the six-hour cache.

**Not changed:**
- the page classifier and its 75% floor;
- the brand-domain list;
- the known-customer bypass;
- the booking routes;
- both form files.

## A correction to what I told you

I asked whether six hours was right for a name-only verdict. It's already a
documented decision: the comment above the cache's time limit says a
name-only guess exists only because the site didn't load, so the site should
be tried again soon. CLAUDE.md implied 180 days; it now says six hours and
why.

## Verified — actually run

- **The full bar, bare: 14 suites, 4,931 assertions, 0 failures.** The
  baseline was saved on a quiet machine. (An earlier save ran beside a
  mutation run and recorded a suite with no summary. That one was thrown
  away and re-saved; see Process.)
- **New tests, driven through the real routes** (`test-non-icp-routes`, 15):
  - the exact incident: a 0.82 name-only insurance guess, then no block, no
    flag, and StartTrial fires;
  - the controls: the same 0.82 from a page still withholds Meta, and 0.86
    name-only withholds it;
  - a name-only block is recorded as `llm_name_only` and a page block as
    `llm`;
  - both Slack posts, read as sent.
- **Executed in `test-non-icp`** (9): `nonIcpFloorFor` for every source, the
  0.85 default and the env override, and the cache read at 0.82 and 0.86,
  name-only against page.
- **Real data, read-only:** the impact table above, and the 5 changed
  domains looked at by name.

## Mutations

**15 mutations, all 15 caught.** Each ran in a scratch copy with
`measure.js --mutation` and was restored by copying the file back.

| | What was broken on purpose | Caught by |
|---|---|---|
| N01 | the Meta read back on the page floor for every source (the incident) | `test-non-icp`, `test-non-icp-routes` |
| N02 | `nonIcpFloorFor` ignoring the source | both |
| N03 | the name floor back to 0.9 | both |
| N04 | the lead row always saying `llm` | `test-non-icp-routes` |
| N05–N07 | the blocked post: brand-list branch, name-only wording, off switch | `test-non-icp-routes` |
| N08–N10 | the Meta-only post: source not passed, industry not named, name-only wording | `test-non-icp-routes` |
| N11 | the Model tab's near misses on one floor | `test-non-icp` |
| N12 | the read-time block removed | `test-non-icp` |
| N13 | the late-verdict post never saying name-only | `test-non-icp` |
| N14–N15 | the blocked post's wording following the row instead of the evidence | `test-non-icp-routes` |

**N14 and N15 SURVIVED at first.** My test for them couldn't produce the
case it names: the route stub hands back freshly bound values, so the lead
row never kept an earlier decision. The stub now simulates that, and both
are caught (`53ef390`).

## The review

Four independent reviewers read the three branches, read-only. On this
branch they found:
- **Two medium issues:** the fire tool crashing, and the late-verdict post's
  wording. Both are fixed above.
- **One low issue:** the mixed-evidence wording. Fixed.
- **A window after deploy** where an old row would withhold Meta without
  blocking. Closed by the read-time block.

They found **no path where a lead is blocked or loses Meta that this card
doesn't disclose.**

**The fire tool was run, sending nothing.** I ran all four posts (`blocked`,
`llm-blocked`, `llm-meta`, `late-block`) into a catcher on this machine.
All four built and posted there. None was sent to Slack.

## Process, stated plainly

- Merged nothing. The branch is pushed and the PR is open.
- **Found and corrected:** I ran this branch's tests in a second worktree
  while PR D's mutation run was going. The booting suites use fixed ports,
  so the two collided. This branch's baseline was saved with one suite
  showing "NO SUMMARY"; it was re-saved on a quiet machine and the commit
  amended before any push. It is saved as a rule for next time.
