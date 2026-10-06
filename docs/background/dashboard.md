# Background: The monitor dashboard

Moved here from `CLAUDE.md` on 7 Oct 2026, word for word, to bring that file under its size limit. `CLAUDE.md` keeps the rule for each of these as one line with a link here; this file keeps the history, measurements and worked examples behind it. Dates and counts are as of when each paragraph was written, and "today" means that day. Line numbers in the headings are `CLAUDE.md` at commit b924ce7.

## The Model tab as a surface (was `CLAUDE.md` lines 542-554)

- **The model layer's dashboard surface is `/monitor/non-icp` and the
  **Model** tab** — separate from **Blocked**, deliberately. Blocked means
  one thing, "we turned these people away", and a flagged-not-blocked lead
  is not that. Four panels: a five-state ladder that is mutually exclusive
  and exhaustive and **sums to the lead total**, per-industry actions read
  from `NON_ICP_BUSINESS_TYPES` rather than restated, every decision with
  its evidence quote, and the scrape blind spot. The join from lead to
  verdict is done **in JavaScript**, through `nonIcpCandidateDomains`, so
  there is no second domain normaliser beside `partnerStackCustomerKey`.
  The scrape panel reports **latest outcome per domain, never a historical
  rate** — `nonIcpWriteVerdictRow` is `ON CONFLICT DO UPDATE`, so a domain
  that failed and later succeeded overwrites its own failure — and it says
  so on screen, not only in a comment.

## The `ip_*` columns on `leads` (was `CLAUDE.md` lines 589-616)

- **`leads.ip_address` and the eleven `ip_*` columns** — where the visitor
  actually was, from their IP. Added 23 Sept; this said "nine" until 26
  Sept, written before `ip_latitude` / `ip_longitude` existed, so **count
  the migration in `db.js`, not this sentence.** **Different from
  `enriched_city` / `enriched_state` / `enriched_country`**, which are
  Apollo's record of where the PERSON is based — `apolloEnrichmentFields`
  reads `person.city`, not the organisation's — and cover 44% of leads.
  **Corrected 26 Sept 2026: this used to say those were where the COMPANY
  is.** The company's own address is a third thing, `enriched_org_hq`. On
  the 22 Sept lead the person read Woburn and the IP read Boston — both
  correct, ten miles apart. The lead panel keeps all three under separate
  labels for exactly that reason.

  `ip_address` **is personal data**, unlike everything else on that table
  except the contact fields. **Kept indefinitely, decided 23 Sept** — the
  same treatment every other column gets, but a decision rather than a
  default, and a deletion request has to clear this and the mirror.

  **The mirror gets the PLACE, not the ADDRESS.** `gw_form_leads` has
  `ip_city` / `ip_region` / `ip_country` / `ip_timezone` and no
  `ip_address`: an SDR benefits from the city and the timezone, nobody
  across the WAN has a use for the IP itself, and copying personal data
  into a second database without a reason for it being there is how it ends
  up somewhere nobody remembers.

  `ip_checked_at` is stamped **only when a lookup decided something**,
  never for a failure or a skip — the same rule `non_icp_checked_at`
  follows.

## The Visitors tab: tiles, circles, networks (was `CLAUDE.md` lines 1120-1153)

**THE VISITORS TAB, AND THE THREE TILE PROVIDERS IT TOOK.**
`/monitor/visitors` plus the **Visitors** tab: coverage, repeat addresses,
places, networks, timezones, and a map toggle.

**Repeat addresses is what earns the tab.** The other panels could live on
a lead row; "which address sent more than one lead" cannot — it is a
question about the set. When `pt.lancon@gmail.com` was investigated on 22
Sept there was no way to ask it.

**A 200 IS NOT A WORKING TILE, and that cost two deploys.**

| provider | what happened |
|---|---|
| `tile.openstreetmap.org` | 200 to curl, **403 in a browser** — usage policy blocks embeds by Referer |
| `basemaps.cartocdn.com` | 200, a real 6.5KB PNG — with **"API KEY REQUIRED" printed across it** |
| `services.arcgisonline.com` Canvas | clean, keyless, desaturated for data overlay |

Both failures were invisible to a status-code check and were settled by
**downloading a tile and looking at it**, at three zoom levels. Both are
named in a rejected list in `tests/test-non-icp-routes.js` with the reason.
If a fourth is ever needed, check it the same way before adding it.

**Circle AREA scales with lead count, never radius** — a radius
proportional to the count makes twice the leads look four times as busy.
Drawn largest-first so small circles land on top, with a white ring,
because eighteen same-coloured circles over the US north-east otherwise
render as one blob.

**Networks are merged by NAME in the browser.** The provider reports one
network under several domains: live data had "Verizon Business" twice, on
`verizonbusiness.com` and `frontiernet.net`, and "AT&T Enterprises, LLC"
twice. Four rows, two networks — a wrong number no label could fix. Merged
client-side so the API keeps reporting what the provider actually said.

## One row builder, two tables (was `CLAUDE.md` lines 1693-1713)

**ONE ROW BUILDER, TWO TABLES, AND THE ROW ID MUST BE NAMESPACED.**
`leadRowsHtml(leads, ns)` renders **All Leads and Blocked**, and `showTab`
toggles a class — it never clears a panel — so both tables sit in the
document at once. An unscoped `id="er-<session_id>"` therefore existed
**twice** for any lead that was blocked *and* on the loaded All Leads page,
and `getElementById` returns the first in document order. `tp-leads`
precedes `tp-blocked`, so the click on Blocked expanded the hidden copy in
the inactive All Leads panel and **nothing happened on screen**.

It presented as "the top two rows won't expand" because All Leads page 1 is
the newest 25 leads: a blocked lead breaks while it is new enough to be
there and silently starts working again once it falls off. A moving window,
which is why it read as a property of those rows — and why enrichment
looked relevant and was not.

`toggleRow(key, sid)` and `loadChanges(key, sid)` take **two** arguments:
the key addresses the DOM, the session id addresses the lead. Collapsing
them is what caused this, and it would also send a namespaced key to
`/monitor/lead-changes` as a session id. Every caller passes a distinct
namespace and a test pins it.

## The Model tab's numbers, and Blocked counts (was `CLAUDE.md` lines 1714-1748)

**The Model tab keeps ONE claim per number, and its industry table is
SPLIT BY WHAT DECIDED.** "Insurance 8 / Blocks" read as the model having
categorised and blocked eight leads, while the ladder beside it said
`blocked_model: 0` — the list had done all ten. Three groups now render
always, **empty ones included**, because an empty "Blocked by the model"
is the clearest statement on the tab that the model has turned nobody away.

**And the industry comes from the domain that ACTUALLY BLOCKED, or from
nowhere.** It used to fall back to any candidate domain with a verdict,
which filed three `farmersagent.com` blocks (no verdict for that domain)
under Insurance because the lead's *website* was `farmers.com`. Harmless
there — both are insurance — but on a lead whose email is a brokerage and
whose website is a software company it files a block under the wrong
industry, and a wrong industry here is something somebody acts on without
knowing it is wrong. A block we cannot categorise now says **"not
categorised"** rather than borrowing.

**Blocked counts: three different units, and every label says which.**
Overview's big number is **leads**, its sub-line is **people**, and the
Blocked tab's "excluding our own tests" is **leads** again. 10 leads are 6
people because five of those leads are one address of ours. The three sat
adjacent with no units and invited subtraction.

**The Model tab keeps ONE claim per number.** Its industry table counts
**leads acted on in the window** and nothing else; the standing cache is a
separate block counted in **companies, all time, with no rate**. They were
one table until 15 Sept 2026, and because ~2,937 of the cache is a backfill
that loaded only successfully-judged rows, it read "Insurance 19" in a week
with five insurance blocks and a 0.3% scrape-failure rate against a live
rate near 26%. Both had an explanation in small print under the number
people actually quote. The scrape rate is scoped to the domains behind the
window's leads, and **"never tried" is its own number, deliberately outside
the denominator** — a domain the warm path has not reached is not a scrape
that failed.

## The CSV export: phones and the open limit (was `CLAUDE.md` lines 2080-2103)

**Why the second rule:** measured read-only, **2,669 of 2,676 stored phone
numbers start with "+"**. The Lead magnet export's blanket
`= + - @` rule would have put an apostrophe on 99.7% of the column a dialer
imports. A number-only cell cannot call a function or reach another cell, so
it is safe to leave alone.

**"Unchanged" means the BYTES, which is what the dialer imports — not what a
spreadsheet shows.** Excel and Sheets still read a number-only cell as a
number: `+19495550123` loses its "+", and a dashed one like
`+1-949-555-0123` is worked out as a SUM and shows -1626. No stored phone
has the dashed shape (0 of 2,679, 26 Sept), and both were true before this
change. Keeping them readable in a spreadsheet would need the apostrophe,
which breaks the dialer, and one file cannot do both.

**One limit, left open on purpose:** a spreadsheet set to split columns on
`;` (common in European locales) splits a cell mid-way at a `;` or a line
break, and a piece starting with `=` there would run. Quoting cannot fix
that — the whole file is misread in that setting. No stored value has a `;`
or a line break followed by a formula character (0, 26 Sept). Guarding it
would put a visible apostrophe inside bullet lists in `about_business`, so it
is a decision for Darshil, not a quiet widening of his rule.
`tests/test-batch2.js` §32 runs the real function over every stored phone
shape and the hostile ones.
