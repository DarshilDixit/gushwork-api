# PR: /thank-you names the industry we do not serve yet (form v5.19.0)

Branch `non-icp/thank-you-industry`, from `main` at `51cc98b`. 15 Oct 2026.
**Not merged: waiting for your review.** Nothing about WHO is blocked or which
leads fire Meta changes. This changes what a blocked visitor READS.

**Why:** Swapnil's copy ask. The page for turned-away leads said "someone from
the team will reach out", which is the opposite of what happens. It should say
which industry we do not serve yet and that we will get back to them.

**What:**
- **Server (`index.js`).** `/non-icp-check` now answers with `industry`
  (`real_estate`, `insurance` or `null`) and `/submit` answers with
  `non_icp_industry`. A model block reads its `business_type`; a brand-list
  block is looked up by its domain (`NON_ICP_INSURANCE_DOMAINS`, a set naming
  the insurance half of the list, because the list itself is one object); a
  prefix rule (bhhs, berkshirehathaway) is real estate. `/submit` reads the
  verdict cache once, only for a blocked lead, with a 1.5 second limit, and
  falls to null on any failure.
- **Both form files, v5.19.0 and v5.19.0-ads, identical change.** The form
  remembers the industry from a BLOCKED answer and adds `industry=` to the
  `/thank-you` link beside `attendeeName`. Only the two allowed values travel;
  anything else is dropped. All four redirects carry it, including the one from
  the calendar hold, which has no verdict in hand.
- **Webflow, already STAGED, not published** (see below).

**Not changed:** which leads are blocked, the floors, the prompt, Meta,
Salesforce, the SDR list, the brand list's contents. The redirect URL for a
visitor with no known industry is byte-for-byte what it was.

**Webflow, staged on the "Thank You DQ" page (`/thank-you`) and NOT live:**
- The paragraph now reads: "We don't work with your industry yet, so we can't
  set up a demo right now. As soon as that changes, we'll get back to you."
  That is the fallback a visitor sees with no industry.
- The page footer gained a small script (16 lines added, none removed, checked
  by diff against a backup) that swaps in "We don't work with real estate
  businesses yet…" or "…insurance businesses yet…" from `?industry=`. It
  honours only those two values and sets the text with `textContent`.
- The old "explore AI feeds" sentence and its link are gone from that paragraph.
- The heading is untouched: the page's existing script already turns it into
  "Thank you, {first name}!" from `attendeeName`.
- Backup of the footer before the edit is in the scratchpad, not the repo.

**Order of going live (the page copy is safe on its own, and so is each step):**
1. Merge this PR (Railway deploys the server; nothing visible changes yet).
2. Re-pin the 16 pages to the new `main` SHA, then check the banner reads
   `v5.19.0` / `v5.19.0-ads` and the SHA in each page's HTML.
3. Publish Webflow. **Publishing ships the whole site.** "Thank You DQ" and
   "Thank You 2" both show a last-updated of 15 Oct 08:52 UTC against a last
   publish of 12 Oct, so someone else has unpublished changes. Ask whose before
   publishing.
Until step 3, visitors keep today's page. Until step 2, the new page shows the
industry-free sentence. Neither order breaks anything.

**Verified (run, not just read):**
- Full bar, bare, read in full: all 16 suites pass, 5631 assertions
  (baseline moved 5586 -> 5631, saved). `test-non-icp` 711 -> 751,
  `test-non-icp-routes` 626 -> 631.
- The REAL redirect function is lifted out of BOTH form files and executed:
  name + industry, industry alone, no industry (old URL unchanged), bare path,
  a hostile value dropped, the remembered industry on the calendar-hold path,
  an off-list value remembered as nothing.
- `/non-icp-check` over HTTP: brokerage -> `real_estate`, carrier -> `insurance`,
  a model block -> from `business_type`, a clean lead -> `null`. `/submit`
  over HTTP -> `non_icp_industry: real_estate` for a brokerage.
- The insurance set is compared against the SOURCE: it must equal exactly the
  keys under the Insurance comment, so a carrier added to the list and not the
  set fails the bar.
- Four mutations, each CAUGHT: allowlist removed on the redirect, one carrier
  dropped from the set, `/non-icp-check` no longer sends `industry`, `/submit`
  no longer sends it.
- Webflow: paragraph re-read after the edit; footer diffed before and after;
  the live page re-fetched and confirmed unchanged.

**NOT verified (never executed):**
- The page script in a real browser. I did not load `/thank-you?industry=…`,
  because the staged page is not live. It is 8 lines of plain DOM code and I
  ran nothing of it. Check it once after publishing, with `?industry=insurance`
  and `?industry=real_estate`, and with a junk value.
- A real form submission end to end into the new page.
- Whether the page flickers from the fallback sentence to the industry one. The
  script runs at the end of the body, so it should be too fast to see; not
  looked at.

**Known gaps, said plainly:**
- **Nothing is wired to the promise "we'll get back to you".** A blocked lead
  is stamped in `leads` and is not in Salesforce or on the SDR list, so when we
  start serving these industries someone has to pull that list. A query for it
  is not written.
- Only real estate and insurance can name an industry, because only those two
  block. The other four industries never reach this page.
- The two form files gained the identical code, but `test-ads-parity` did NOT
  catch a one-sided mutation of this block (it survived). The executed redirect
  test runs against both files separately, which is what covers it.
