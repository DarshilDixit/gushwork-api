# A red health row re-alerts on every deploy

**Status:** open, diagnosed, not fixed
**Found:** 4 Sept 2026 — the PartnerStack health row alerted twice in 40
minutes with identical content, about a failure already known and understood.
**Affects:** every System Health check, not just PartnerStack.

## Cause

```js
const _alertLastSent = new Map();   // `${severity}:${source}:${title}` -> ms
const _healthState   = new Map();   // check id -> last reported state
```

Both are **in-memory and die with the process**. On boot:

- `_healthState` is empty, so `prev` is `undefined`, a red row reads as a state
  *change*, and `evaluateHealthAlerts` alerts.
- `_alertLastSent` is empty, so the cooldown that would otherwise suppress it
  (3h critical / 1h warning) has nothing to compare against.

There is a test asserting "red on the first observation still alerts", and that
is **deliberate and correct** for a genuine outage — a restart must not hide a
real red. The problem is only that it cannot distinguish a restart from a
recovery-then-relapse.

## Why it surfaced now and never before

Eight deploys in one evening. On a normal day deploys are rare enough that the
re-alert is indistinguishable from the first alert. The heartbeat runs every 30
minutes (`HEARTBEAT_CHECK_MS`), so the giveaway was two alerts **40** minutes
apart — a fresh timer after a restart, not a steady one.

## Fix

Persist both maps. A small table keyed by alert key and by check id, read at
boot and written on each transition, would make the cooldown and the state
survive a restart.

**Take care with the trade-off.** Persisting `_healthState` means a row that is
still red after a restart will NOT re-alert — which is the goal, but it also
means a genuinely ongoing incident stops re-announcing itself. The cooldown
already handles that (it re-alerts after 1–3h and reports how many were
suppressed), so persisting is safe *provided* the cooldown is persisted too.
Persisting one without the other would be worse than neither.

## Interim

The PartnerStack acknowledge flag (`POST /monitor/partner-ack`, shipped in
PR 1.8) removes an understood failure from the health row entirely, so it stops
alerting. That is a per-failure fix, not a fix for this.
