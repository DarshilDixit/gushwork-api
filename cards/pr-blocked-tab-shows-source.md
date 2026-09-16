# PR 80 — The Blocked tab says WHICH mechanism blocked each lead

Branch `fix/blocked-tab-shows-source`. One commit. **Not merged.**

Found while verifying the Model and Blocked tabs after PRs 76–79, not by a
report. Nothing here was broken by that work; it is a gap the model layer left
behind when it shipped.

---

## The gap

`non_icp_reason` holds a **domain** for *both* blocking mechanisms, so a blocked
row reading `kw.com` looks identical whether the brand-list string comparison or
the model produced it. `/monitor/leads` selected `non_icp_blocked` and
`non_icp_reason` and **not** `non_icp_source` — so the one surface where somebody
reviews a specific block could not tell the two apart.

That is exactly the distinction CLAUDE.md says the column exists for:

> `non_icp_reason` holds a domain for **both** mechanisms, so without
> `non_icp_source` a reader cannot tell a string comparison they can re-derive
> from a model verdict they cannot.

The split was visible on the Model tab's ladder (11 list / 5 model) but nowhere
per-row. It reads as an omission rather than a decision: the Blocked tab predates
the model layer.

## Three states in production, not two

```
domain_list    3   15-16 Sept
llm            5   15-16 Sept
(null)         8   11-14 Sept
```

The nulls are leads blocked between the **11 Sept** brand list and the
**14 Sept** model layer, before the column existed.

**The Model tab's ladder counts NULL as a list block, and is RIGHT to** — the
model could not block anything before it shipped, so by construction those were
list blocks. But that is an *inference about when a row was written*, and a
per-row label is precisely where this repo refuses to present an inference as a
record. The same rule as `first_ticked_at`: *"an inferred timestamp in an
observational column is read as a measurement by the next person."*

So the chip says **`unrecorded`**, and its tooltip explains why the ladder is
still correct to count it with the list. The two surfaces do not contradict each
other; one states the record, the other states a sound inference, and each says
which it is.

## What changed

| | |
|---|---|
| `/monitor/leads` | selects `l.non_icp_source` |
| CSV export | carries `non_icp_source`, so it is analysable rather than hover-only |
| `leadRowsHtml` | the 🚫 tooltip now names the mechanism and explains it |
| Blocked tab only | a visible `list` / `model` / `unrecorded` chip |

`nonIcpSourceShort` and `nonIcpSourceWhy` are declared at **top level**, like
every other shared helper — the scope bug that once made Blocked rows silently
unexpandable is the reason that matters.

The visible chip is Blocked-tab only (`ns === "b"`). One row builder serves both
tables, and that column means nothing on All Leads, which keeps it in the tooltip.

---

## Verified

**Bar green, run bare:** 12 suites, **3482 assertions** (+13).

**The query was EXECUTED, not read** — run against production read-only; it
returns `non_icp_source` in the result set.

**The renderer was DRIVEN against the real 16 blocked rows**, with their real
stored sources:

```
painted chip counts: { list: 3, model: 5, unrecorded: 8 }
```

Every row painted exactly what the database stores. All Leads painted no chip and
kept the mechanism in its tooltip.

That run also showed the point of the change: the 5 model blocks are
`assurance-network.com`, `steadfastlifehealth.com`, `denesha.net`,
`legacyinsuranceagency.com` and `chaseinternational.com` — none of which the
brand-domain list could ever have caught. Previously indistinguishable on screen
from an `allstate.com` list match.

### Mutation testing — 3 mutations, all CAUGHT

| # | Mutation | Result |
|---|---|---|
| 1 | The column is no longer selected (the original gap) | CAUGHT |
| 2 | An unrecorded source claims to be the list | CAUGHT |
| 3 | The visible chip is dropped from the Blocked tab | CAUGHT |

---

## What this does NOT change

- **No lead is blocked or unblocked.** This is display and export only; no
  blocking predicate, no `non_icp_blocked`, no `nonIcpVerdict`.
- **No Meta event changes.**
- **No count moves.** The Blocked tab still reads 16 leads / 10 excluding our own
  tests, and the Model tab ladder is untouched.

## Not done

- **No backfill of the 8 nulls.** They are genuinely unrecorded, and writing
  `domain_list` into them now would be inventing provenance — the thing the chip
  exists to avoid. They are correctly inferable from their dates and the tooltip
  says so.
- The Model tab ladder still labels those 8 as "Blocked — brand list" with no
  caveat on that panel. Defensible, and I left it alone rather than widening
  scope; if you want the ladder to carry the same nuance, say so.

## Deploy

Server-side only. `git push` to `main` → Railway. **No Webflow step** — neither
form file is touched.
