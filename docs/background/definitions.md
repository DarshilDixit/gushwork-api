# Background: Definitions: measurements behind the rules

Moved here from `CLAUDE.md` on 7 Oct 2026, word for word, to bring that file under its size limit. `CLAUDE.md` keeps the rule for each of these as one line with a link here; this file keeps the history, measurements and worked examples behind it. Dates and counts are as of when each paragraph was written, and "today" means that day. Line numbers in the headings are `CLAUDE.md` at commit b924ce7.

## The two completed-not-submitted populations (was `CLAUDE.md` lines 686-699)

That produces **two** populations that are `completed` without being form
completions, and they are not the same shape:

1. **`completed = true` with `submitted_at IS NULL`** — reached step 1, dropped,
   then booked through a link later. One of the three booking `UPDATE`s set
   `completed` and left `submitted_at` alone. **6 rows today, all 6 booked, all
   of them form traffic** (`prefill_source` null or `url_param`), e.g.
   `aasnj@meta.com`. **This is the shape the old caveat did not mention**, and it
   is the one that breaks "completed implies submitted".
2. **`completed = true` with `submitted_at` set, never touched the form** — the
   two safety-net `INSERT`s, for someone who booked with no form row at all.
   **10 rows today, all `rh_webhook`, all with `submitted_at` set.** The old
   caveat described these correctly.

## The internal-submissions measurement, and the /careers session rows (was `CLAUDE.md` lines 854-877)

Measured before deciding, read-only, by running the real `overviewReport`
twice: ours are 107 rows and 32 addresses, 1.8% of all rows. Rates moved by
at most about 0.25 points. Blocked moved by 12%, and a testing day by up to 40%
of its month (March).

**853 session rows come from two pages that were never meant to have the
form, and they are staying.** `/careers` (629 non-bot) and `/meeting-booked`
(224) carried the form script by mistake until 10 Sept 2026. `/careers` has no
form elements and never produced a lead; `/meeting-booked` produced exactly one
row, in March, by reading its own post-booking redirect's query string back in
for somebody who had already booked. Both tags are being removed.

They are **5.7% of the Sessions card**, which reads 14,848 with them and 13,995
without — so Sessions to Step 1 shows **4.98%** where the form-pages-only figure
is **5.28%**. The rows are not deleted: they are true, somebody did load a page
carrying the script, and rewriting them would move every historical session
number at once to correct a 0.30-point error. Same trade as the internal
addresses above, and the population is closed and dated once the tags go.

Also biases the `partial` health row toward a **false red** — it reddens on
8+ sessions with zero leads in 2 hours, and these two contributed about four
such sessions per window that could never produce a lead. Safe direction, and
it ends with the tags. See `docs/OPEN-ITEMS.md` item 12.
