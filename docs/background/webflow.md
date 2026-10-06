# Background: Webflow pins and headless editing

Moved here from `CLAUDE.md` on 7 Oct 2026, word for word, to bring that file under its size limit. `CLAUDE.md` keeps the rule for each of these as one line with a link here; this file keeps the history, measurements and worked examples behind it. Dates and counts are as of when each paragraph was written, and "today" means that day. Line numbers in the headings are `CLAUDE.md` at commit b924ce7.

## The tags are per-page (correction of 15 Sept) (was `CLAUDE.md` lines 286-300)

**THE TAGS ARE PER-PAGE, NOT IN PROJECT SETTINGS — CORRECTED 15 SEPT 2026.**
This section used to say the tags live in **Project Settings → Custom Code**.
They do not, and have not for as long as anyone can check. Measured by reading
all 81 pages through the Webflow API on 15 Sept: the site-wide head and footer
blocks contain **no `gushwork-api` tag at all**, there are **zero registered
scripts**, and every pin lives in an individual page's **before-`</body>`**
footer block. Twelve pages carry one.

That is not a detail, it is the whole reason a page goes stale. There is no
single place to edit, so "update both script tags" is **twelve page edits**,
and a Project-Settings republish genuinely cannot touch any of them. Anyone
following the old wording would look in Project Settings, find nothing, and
either give up or ADD a tag there — which would load the form script twice, at
two different SHAs, with no error anywhere.

## The curl sweep's blind spot, `/start-old`, pins in the head (was `CLAUDE.md` lines 320-348)

**The `curl` sweep below has a blind spot the API closes.** Its page list is
maintained by hand, so it cannot find a page carrying a pin that nobody
remembered to add to it. Reading every page's footer block found exactly that:
**`/start-old` was pinned to `d493e92e`, a 26 June commit** — v4.4-era, months
behind. It is a 404 today and therefore harmless, which is also why no sweep
would ever have caught it. Publish that page and it would have served June's
form to real visitors. Prefer the API sweep; see the section at the end of
`docs/tickets/non-icp-v1-block.md`.

**`/start-old` was re-pinned with everything else on 16 Sept 2026** and is no
longer stale — but it is still a draft, so it is still invisible to the `curl`
sweep, and it will go stale again at the next form deploy unless the API sweep
is the one you run.

**AND A PIN CAN LIVE IN THE HEAD, NOT ONLY THE FOOTER — 18 SEPT 2026.**
Everything below says "footer", and a repin script written to that
assumption silently reported "no pin found" for `/ai-demo` while the live
page was plainly loading a form script. The page had been reworked and
its tag now sits in the **head** block.

A read that finds nothing is not proof of nothing. **Scan both blocks on
every page**, and treat "no pin" on a page you know serves the form as a
bug in the scan rather than a fact about the page.

**The set is 17 pinned BLOCKS across 85 pages as of 18 Sept**, up from 15
across 83 two days earlier — `/aeo-new` and `/ai-demo-old` appeared, and
`/ai-demo` moved head-ward. Counting pages is the wrong unit; count
blocks.

## The page count was wrong (correction of 17 Sept) (was `CLAUDE.md` lines 355-362)

**THE AUTHORITATIVE SET IS 15 PAGES, NOT 13 — CORRECTED 17 SEPT 2026.** This
said 13 and it was wrong by two: `/aeo` and `/ai-crm` both carry
`gushwork-form-popup.js` and neither was in the list or in the `curl` sweep
below. They were found only by reading all 83 pages' footers through the API,
which is the sweep this section already told you to prefer and which nobody had
actually run. A count written down here is a claim with a shelf life; the API
scan is the measurement. **Run the scan, do not trust this number either.**

## Repinning through the Webflow API (was `CLAUDE.md` lines 363-377)

**THE API DOES THE WHOLE JOB, and that is worth knowing before you start.** The
MCP tool only accepts footer content inline, so a repin through it means
re-typing each page's entire custom-code block — 13k to 26k characters — to
change 40. With a site API token (Site settings → Apps & integrations → API
access; scopes: Custom Code, Pages, Sites, all read+write) it is a scripted
read-replace-write over exact bytes:

    GET /v2/pages/{page_id}/custom_code/freeform          -> [{location, content}, ...]
    PUT /v2/pages/{page_id}/custom_code/freeform/{footer} -> {location, content}

The PUT is location-scoped: writing the footer leaves the head block untouched,
verified. There is no PUT on the collection path, only on `/{location}` — the
collection path answers 404 for every write method, which reads as "no access"
and is not.

## Editing page elements headlessly (was `CLAUDE.md` lines 378-409)

**EDITING PAGE ELEMENTS HEADLESSLY: WHAT WORKS AND WHAT DOES NOT.** Learned
on 18 Sept 2026 adding the about-business textarea to `/ai-crm`. The MCP
Designer tools can create the element and most of its properties, but two
form-field properties are Designer-only and will silently stay at Webflow's
defaults:

| Property | Headless? |
|---|---|
| element + position + classes (`data_element_builder`) | yes |
| DOM id (`set_settings` key `domId`) | yes |
| custom attributes, incl. **`maxlength`** (`set_attributes`) | yes — overrides Webflow's own, no duplicate |
| field **Name** (`set_settings` key `name`) | **stores but never renders** |
| **Placeholder** | **not settable at all** — *"not applicable to this element"* |

**The `name` one is the trap: the write succeeds, `get_settings` reads the
new value straight back, and the published HTML keeps `name="field"`.**
Verified across two republishes and five minutes of polling, so it is not a
compile race. Stored is not rendered. Do not trust a read-back here.

**Placeholder is the one that matters**, because the form script turns it
into the visible floating label — a field created headlessly shows
**"Example Text"** to real visitors until somebody opens the Designer. Set
it there, or do not create form fields headlessly at all.

**AND THE TEXTAREA NEEDS PAGE-LEVEL CSS THAT DOES NOT COME WITH IT.** The
float-label wrapper centres its label vertically, which is right for a
one-line input and wrong for a tall textarea — the label floats in the
middle of the box. `/ai-demo` has four rules in its **head** custom code
pinning it to the top and fixing the padding; they must be ported to any
page that gains the field. `/ai-crm` went live without them and looked
broken.

## The banner that did not move (16 Sept) (was `CLAUDE.md` lines 439-446)

**THE BANNER IS ONLY A CHECK IF YOU BUMPED THE VERSION, and on 16 Sept 2026 it
was not.** The phone-country fix shipped without touching the version string, so
both files read `v5.13.0` before AND after — the banner was identical on the old
pin and the new one. Anyone following the paragraph above would have read
v5.13.0, concluded nothing had changed, and been wrong in whichever direction
they guessed. A confirmation step that returns the same answer either way is not
a confirmation step.

## The Designer serves a stale tree (was `CLAUDE.md` lines 972-991)

**THE WEBFLOW DESIGNER SERVES A STALE TREE AFTER A HEADLESS WRITE, AND
THAT MAKES A SNAPSHOT LIE.** The MCP data tools write through the API; the
open Designer session renders from its own in-memory copy and does not
reload. On 15 Sept 2026 `element_snapshot_tool` returned **"Element not
found"** for markup that had just been created and could be read back
through `query_elements` in the same minute. Worse than the error is the
quiet case: after the element exists, a **style** change made through the
API kept rendering at its old value, so the snapshot showed a stale layout
with no error at all.

`switch_page` to another page and back forces the reload; `select_element`
on the new element confirms the canvas can see it. Do that before every
snapshot you intend to treat as evidence.

This is the repo's oldest lesson wearing a picture instead of an
assertion. "It rendered" and "I photographed what is actually there" are
different claims, and the gap between them is a screenshot of the previous
state pasted under the word *verified*. A snapshot is only evidence if the
canvas was refreshed after the last write.
