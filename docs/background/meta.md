# Background: Meta Conversions API

Moved here from `CLAUDE.md` on 7 Oct 2026, word for word, to bring that file under its size limit. `CLAUDE.md` keeps the rule for each of these as one line with a link here; this file keeps the history, measurements and worked examples behind it. Dates and counts are as of when each paragraph was written, and "today" means that day. Line numbers in the headings are `CLAUDE.md` at commit b924ce7.

## The whole `x-forwarded-for` header was sent (was `CLAUDE.md` lines 1052-1066)

**META WAS SENT THE WHOLE `x-forwarded-for` HEADER, NOT THE CLIENT IP, AND
IT WAS A LIVE BUG.** Every Meta call site passed
`req.headers['x-forwarded-for']` raw. Railway sends **two entries**, proved
in production on 23 Sept:

```
entries=2 | first=97.208.126.x | last=152.233.40.x
req.ip=152.233.40.x | sent_to_meta_would_be=97.208.126.x
```

So `client_ip_address` carried `"97.208.126.x, 152.233.40.x"` — a comma
list where an address belongs. **`readPartnerStackRequestContext` had
already fixed exactly this, ~4,000 lines above, with a comment explaining
why.** One integration was fixed and the other was not.

## Meta never complains (was `CLAUDE.md` lines 1078-1084)

**META NEVER COMPLAINS, SO NOTHING DOWNSTREAM CAN CATCH THIS.** Checked
against the live API: a clean IP, a two-hop list and a three-hop list each
returned HTTP 200, `events_received` 1, and an **empty `messages` array**.
That array was also being discarded — only `events_received` was logged —
so a malformed field could be wrong for months while every line read like a
success. It is now printed when non-empty.
