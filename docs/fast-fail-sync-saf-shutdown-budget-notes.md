# Thinking out loud: fast-fail SYNC/SAF + shutdown budget cross-cuts

Not a prescriptive plan — discussion notes from comparing the fast-fail
plan (`phoenix-ha-fast-fail-sync-saf.md`) against industry circuit-breaker
patterns and the shutdown-budget plan (`shutdown-budget.md`).

## What the industry literature validates

The fast-fail plan is already a circuit breaker (SYNC → SAF = open;
SAF → abort = unrecoverable). The principles that line up:

- **Timeouts derived from observed latency, not picked.** 7s SYNC
  per-attempt = 5s socket detection + ~2s recovery. The earlier 500ms
  proposal would cancel successful pipeline recoveries — the exact
  failure mode Brooker warns about.
- **Bulkheading.** Scoped `Configuration` for the standby HDFS isolates
  the failure domain from the local FS. Worth being explicit in the plan
  that this is *why* the scoping matters, not just code tidiness.
- **Constant work / wake-the-idle.** The synthetic swap event makes
  rotation work the same whether the consumer is busy or idle — fixes
  the prod symptom (44s idle gap) and matches the principle.
- **Load shedding as graceful degradation.** SAF is degradation, not
  failure. Justifies the looser 15s SAF per-attempt: the degraded path
  should ride out larger transients.

## Cold-fallback (Gabrielson) — does NOT apply

`ReplicationLogGroup.init()` calls `createLocalShardManager()` eagerly
at `ReplicationLogGroup.java:476`, which opens the local FS client and
`mkdirs` the fallback directory at boot. SAF entry has no cold-start
work. Drop the "pre-warm SAF" item — not needed.

## What's worth tightening in the fast-fail plan

1. **State the bulkheading rationale explicitly** in the scoped-config
   section: not just "don't mutate parent," but "isolate standby
   failure domain from local FS client."
2. **Hysteresis is intentionally absent.** Plan should say so:
   single-threaded disruptor consumer serializes calls, so 2 consecutive
   timeouts already imply sustained failure. Reviewers will ask.
3. **SAF → SYNC recovery path** is undocumented. Either reference where
   it lives or state that recovery is operator-driven. Most common
   production trap with circuit breakers.

## Cross-cuts with shutdown-budget plan

1. **New per-attempt-timeout executor must be drained on shutdown.**
   Shutdown-budget plan lists 4 executors; this adds a 5th. Add
   `d.drainExecutor(...)` for it inside `ReplicationLog.close`.
2. **Daemon threads required.** `future.cancel(true)` leaks a worker
   for ~9–11s (HDFS doesn't always honor interrupt). With small operator
   budgets (e.g. 5s), `close()` returns while the worker is still alive.
   Worker must be a daemon so it doesn't block JVM exit.
3. **Drain the new executor from `close()` directly, not via `onExit`.**
   Decouples it from the deferred `onShutdown`-not-firing bug. One-line
   change in shutdown phase 2 — removes orphan risk.

## Deadline abstraction here? No.

`ShutdownDeadline` fits shutdown because phases are heterogeneous and
fast phases can donate to slow ones. Per-attempt retries are the
opposite shape:

- Each attempt has a hard minimum (~6s) — shrinking attempt 2's budget
  based on attempt 1's spend would cancel pipeline recoveries.
- A deadline that refuses to start attempt 2 below minimum just
  re-derives `per-attempt × max-attempts` with extra plumbing.

So the multiplicative model is the *correct* model for this shape, not
a simplification at the cost of expressiveness.

Implicit deadlines that already exist and are fine:

- **Reader round-buffer (9s)** — config-time invariant, not runtime
  budget. Already documented.
- **330s `WALSyncTimeoutIOException` backstop** — real wall-clock
  deadline on `syncFuture.get()`. Open question #4 asks whether to
  shorten; only deadline-tuning decision worth having.

## Net: what to actually change

If/when the fast-fail plan is updated, three small additions:

- Bulkheading rationale line in the scoped-config section.
- "No hysteresis because consumer is serialized" line in retry policy.
- "Per-attempt executor uses daemon threads, drained from
  `ReplicationLogGroup.close()` phase 2 directly" line in the executor
  section.

Nothing else from this discussion needs to land. No `Deadline` object.
No SAF pre-warming.
