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

## What it looks like

The Blocked tab's email column now carries one of **two** words:

```
EMAIL                            DOMAIN THAT MATCHED        WHY
agent@allstate.com               allstate.com               🚫 [Brand list]
tl@assurance-network.com         assurance-network.com      🚫 [AI check]
nick@steadfastlifehealth.com     steadfastlifehealth.com    🚫 [AI check]
beckygerig@thegerigteam.net      remax.com                  🚫 [Brand list]
dporter1@farmersagent.com        farmersagent.com           🚫 [Brand list]
...
totals: Brand list 11 · AI check 5   (16 blocked leads)
```

Hover gives the sentence:

- **Brand list** — *"This domain is on our list of national real-estate and
  insurance brands, so it was turned away without anything needing to read the
  site."*
- **AI check** — *"The AI check read this company website and classified it as
  real estate or insurance. The Model tab shows the exact quote it relied on."*

11 + 5 = 16, which is exactly what the Model tab's ladder says. Nothing to
reconcile between the two tabs.

## A correction to the first cut of this PR

The first version shipped the stored values almost raw — `list` / `model` /
`unrecorded` — and invented a **third** state. Both were wrong for a screen SDRs
read, and Darshil called it: *"what is null here — don't get you."*

`unrecorded` was the worse half. Eight leads blocked between the 11 Sept brand
list and the 14 Sept model layer have no source stored, because the column did
not exist yet. I treated an empty column as *"we don't know"* — but the model
could not block anything before it shipped, so those **are** brand-list blocks
and the dates prove it. Showing a third state made a reader stop and decode
something for no gain, and made the Blocked tab disagree with the ladder.

The rule I reached for — *don't present an inference as a record* — is about
values you never observed, like a backfilled timestamp. It does not apply to a
fact the system's own history makes certain. Applying it here cost clarity and
bought nothing.

**No backfill either way.** The column stays null on those eight rows because
that is what was recorded; the *display* says what we know. The provenance
footnote survives on hover for anyone auditing, without making the chip itself
something to work out.

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

**Bar green, run bare:** 12 suites, **3487 assertions** (+18).

**The query was EXECUTED, not read** — run against production read-only; it
returns `non_icp_source` in the result set.

**The renderer was DRIVEN against the real 16 blocked rows**, with their real
stored sources:

```
painted chip counts: { "Brand list": 11, "AI check": 5 }
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
| 2 | A third label creeps back in | CAUGHT |
| 3 | The raw stored slug leaks to the screen | CAUGHT |
| 4 | The visible chip is dropped from the Blocked tab | CAUGHT |

Tests pin that **exactly two labels are reachable** across every input including
unknown future values, and that no stored slug (`llm`, `domain_list`) can reach
the screen.

---

## What this does NOT change

- **No lead is blocked or unblocked.** This is display and export only; no
  blocking predicate, no `non_icp_blocked`, no `nonIcpVerdict`.
- **No Meta event changes.**
- **No count moves.** The Blocked tab still reads 16 leads / 10 excluding our own
  tests, and the Model tab ladder is untouched.

## Not done

- **No backfill of the 8 nulls.** The column stays null because that is what was
  recorded; only the display asserts what the dates make certain.
- The Model tab's ladder is untouched — it already read 11 / 5, and the Blocked
  tab now agrees with it rather than needing reconciliation.

## Deploy

Server-side only. `git push` to `main` → Railway. **No Webflow step** — neither
form file is touched.
