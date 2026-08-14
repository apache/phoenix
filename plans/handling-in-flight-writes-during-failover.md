# Plan: Fix graceful-failover deadlock — suspend rotation + fail-stop during cutover

## Context

**The bug.** A graceful failover deadlocks. The demoting (active) cluster gets stuck in
`ACTIVE_IN_SYNC_TO_STANDBY` and the promoting (standby) cluster gets stuck in `STANDBY_TO_ACTIVE`.

**Root cause.** In SYNC mode the active writes directly into the peer's shard directory, and its log
rotation is **unconditional and time-based**: `ReplicationLog.startRotationExecutor` schedules
`requestRotation()` every round (`ReplicationLog.java:175`), and `LogRotationTask.run()` stages a
fresh writer every tick with no emptiness guard (`ReplicationLog.java:430-475`). The header is always
written, so each round drops a new (possibly header-only) `.plog` into the peer's shard dir. The
standby's promotion gate `shouldTriggerFailover()` requires condition #4 —
`getNewFiles(nextRound, currentRound).isEmpty()` (`ReplicationLogDiscoveryReplay.java:511-527`) — to
hold. Because the active keeps minting new files every round, that condition **never** holds, the
standby never promotes, the active never advances to `STANDBY`, and both spin forever.

**The fix (chosen through design discussion).** When the local cluster enters the in-sync cutover
gate `ACTIVE_IN_SYNC_TO_STANDBY`, set a group-level **`failoverPending`** flag on
`ReplicationLogGroup`. That single flag drives two behaviors:
1. **Suspend log rotation** so no new files appear, while **keeping the current writer open** so
   in-flight writes finish. New writes are already blocked by the mutation block (only
   `ACTIVE_TO_STANDBY` sets `isMutationBlocked()`), so the in-flight set is finite.
2. **Fail-stop on a cutover-time SYNC failure.** If an in-flight SYNC write genuinely fails while
   `failoverPending`, abort the region server instead of attempting the illegal
   SYNC→STORE_AND_FORWARD fallback (which targets `ACTIVE_NOT_IN_SYNC`, not an allowed transition from
   `ACTIVE_IN_SYNC_TO_STANDBY`). The RS dies rather than silently dropping a locally-committed mutation
   the peer never received.

Once files stop appearing, the standby's condition #4 eventually holds → standby promotes → the active
reaches terminal `STANDBY`, where the **existing** demotion listener closes the group (writer finalized
then).

**Why this is safe (the drain-budget argument).** The mutation block closes the in-flight set at the
instant the flag is set (`flag_time`). The open file is pinned to the round R it was in; the standby
reclaims R's file at the fixed wall-clock time `R.end + buffer` (`getNextRoundToProcess`,
`ReplicationLogDiscovery.java:245`; `buffer` = 15% of round, ~9s at the 60s default). So the whole
in-flight set has a drain budget of `(R.end − flag_time) + buffer`, whose **floor is `buffer`** (~9s).
Every in-flight write pairs `append()` + `sync()` (`IndexRegionObserver.java:3490-3491`) and the
write-path thread blocks on the sync future (`ReplicationLogGroup.java:769`). Safety does **not** rely
on sync consolidation (`processPendingSyncs` only batches syncs that happen to be co-queued at
`endOfBatch`, `ReplicationLogGroup.java:1348`; under low arrival each sync is its own fsync). It rests
on two bounds: the in-flight set is **finite and closed** at `flag_time` (bounded by the RPC handler
count, since the mutation block admits no new batches), and **each sync is time-bounded** by
`syncTimeoutMs` (a sync that overruns aborts rather than hanging, `ReplicationLogGroup.java:769-784`).
Worst case the N syncs drain serially as N fsyncs through the single consumer — N × a few ms, still
well under the ≥`buffer` (~9s) floor. The only way to approach the floor is a large N combined with a
degraded peer's high per-fsync latency, which is the pre-existing `syncTimeout`/abort concern, not
something rotation policy governs.

Note: with rotation suspended, a **failing** in-flight sync loses its retry-roll-to-a-fresh-file path
— `apply()`'s retry calls `requestRotation()` (`ReplicationLog.java:339`), which is rejected while
suspended (`:252-254`), so the retry falls back to the same open writer. A transient failure can still
succeed on retry; a **persistent** one is caught by the fail-stop (step 2 / design §7): rather than
the illegal SYNC→SAF transition, the RS aborts, preserving the invariant that a locally-committed
mutation is never silently lost.

**Why "suspend" and not "close" or "stop the executor".** Closing the writer on cutover races the
write path: a batch that captured the group before the mutation block, committed locally (step 2), and
reaches `append`/`sync` (step 3) after the close would hit `IOException("Closed")` — a locally
committed mutation the peer never receives (divergence). Keeping the writer open lets that straggler
land. Suspending via a flag (rather than `stopRotationExecutor()`) keeps the executor alive so **abort
resume is a pure flag-clear** — the next tick simply resumes staging writers, with no executor
restart and no blocking work on the ZK cache-event thread.

**Abort ownership (verified constraint).** The active cannot self-abort. `ABORT_TO_STANDBY` is written
only on the standby, operator-initiated (`PhoenixHAAdminTool.executeAbortFailover`, gated on local
`STANDBY_TO_ACTIVE`). The active reaches `ABORT_TO_ACTIVE_IN_SYNC` only by reacting to the peer's
`ABORT_TO_STANDBY` (`HAGroupStoreManager.createPeerStateTransitions`, lines 132-135), then a local
listener advances it to `ACTIVE_IN_SYNC`. So the fix does **not** attempt any active-side abort; it
only needs to *resume rotation* when the state returns to `ACTIVE_IN_SYNC`.

---

## Current (uncommitted) state to correct

`ReplicationLogGroup.java` currently has edits from a **superseded close-on-cutover design** that must
be reworked:
- `subscribeToDemotion()` (`ReplicationLogGroup.java:575-594`) subscribes `ACTIVE_IN_SYNC_TO_STANDBY`
  and, on fire, spawns a daemon thread that calls `close()`. **Wrong** — closing on cutover is the
  unsafe race above. This must instead set `failoverPending` (non-blocking, no thread, no close).
- `close()` (`ReplicationLogGroup.java:831-836`) unsubscribes only `STANDBY` + `DEGRADED_STANDBY`; the
  new `ACTIVE_IN_SYNC_TO_STANDBY` subscription is not unsubscribed. Fix the unsubscribe set.
- The tightened `init()` role gate (`ReplicationLogGroup.java:528-533`) and the
  `initializeReplicationMode` mapping (`:614-622`) interplay with RS-restart-in-cutover — keep the
  committed `ACTIVE_IN_SYNC_TO_STANDBY → SYNC` behavior (commit d69277ab55) so a restart in cutover
  starts SYNC **and** starts with rotation already suspended (see step 4).

## Design — a single group-level `failoverPending` flag

The flag lives on `ReplicationLogGroup` (not `ReplicationLog`) because both consumers already hold a
group reference: `ReplicationLog` keeps a back-reference `logGroup` (`ReplicationLog.java:67`), and the
mode impls hold `logGroup` too. This is the framing the user chose ("set a failoverpending flag in
replicationloggroup"). No per-log suspend flag, no suspend/resume delegation.

### 1. `failoverPending` flag on `ReplicationLogGroup` (`ReplicationLogGroup.java`)
- Add `private final AtomicBoolean failoverPending = new AtomicBoolean(false)` with
  `setFailoverPending(boolean)` and `isFailoverPending()`.

### 2. Suspend rotation — guard `LogRotationTask.run()` (`ReplicationLog.java`)
- In `LogRotationTask.run()` (`ReplicationLog.java:432-435`), extend the existing early-return guard:
  `if (closed.get() || logGroup.isFailoverPending()) return;`. This is the **single chokepoint** — all
  three rotation triggers (scheduled tick, size-based `requestRotationIfOversized`, and the `apply()`
  retry at `:339`) funnel through `LogRotationTask`, so one guard covers them uniformly. The executor
  keeps running; ticks just no-op while pending. The current writer is untouched → stays open.

### 3. Fail-stop on a cutover-time SYNC failure — guard `SyncModeImpl.onFailure` (`SyncModeImpl.java:56-60`)
- `SyncModeImpl.onFailure` currently always returns `transitionToStoreAndForward()`. Guard it: if
  `logGroup.isFailoverPending()`, **throw** the failure (`throw ReplicationLogGroup.asIOException(...)`,
  or rethrow the cause) instead of transitioning.
- Path this rides (already wired, verified): the throw propagates from `onFailure` →
  `updateModeOnFailure` (`ReplicationLogGroup.java:1170`) → the consumer's `onFailure`
  (`:1259-1269`) → the inner `catch` at `:1356` → `setFatalException` + `failPendingSyncs(:1360)` →
  the blocked producer in `syncInternal()` gets the `ExecutionException` and calls `abort()`
  (`:773-777`, `abort` at `:1097`) → `abortable.abort` shuts the RS down. Producer-thread abort is the
  required contract (`abort` javadoc `:1092`: "Must be called from a producer thread").
- Why not transition: SYNC→STORE_AND_FORWARD sets persisted state `ACTIVE_NOT_IN_SYNC`, which is not an
  allowed transition from `ACTIVE_IN_SYNC_TO_STANDBY` (`HAGroupStoreRecord.java:122-123`), and the
  active has no self-abort lever. Fail-stop is the only way to avoid silently losing a committed
  mutation.

### 4. Set the flag on cutover — rework `subscribeToDemotion()` (`ReplicationLogGroup.java:575-594`)
- Split the listener behavior by target state:
  - `ACTIVE_IN_SYNC_TO_STANDBY` → `setFailoverPending(true)` **only** (non-blocking; no thread handoff,
    no `close()`). Writer stays open; in-flight writes finish; no new files created.
  - `STANDBY` / `DEGRADED_STANDBY` → unchanged: hand off `close()` to the daemon thread (terminal
    teardown; writer finalized here).
- Rewrite the javadoc to describe set-flag-not-close for the cutover state.

### 5. Clear the flag on abort — hook LOCAL `ACTIVE_IN_SYNC`
- When the failover aborts, the state returns `ACTIVE_IN_SYNC_TO_STANDBY → ABORT_TO_ACTIVE_IN_SYNC →
  ACTIVE_IN_SYNC` on the active. The group is **not** closed during cutover under the new design (only
  the flag set), so the same live group must clear the flag and resume rotation.
- Subscribe LOCAL `ACTIVE_IN_SYNC` in `ReplicationLogGroup` (extend `subscribeToDemotion`'s
  registration) whose handler calls `setFailoverPending(false)`. Idempotent: clearing an unset flag is
  a no-op, so a spurious/duplicate `ACTIVE_IN_SYNC` event is harmless.
- Note: the forwarder already subscribes LOCAL `ACTIVE_IN_SYNC` for a mode flip
  (`ReplicationLogDiscoveryForwarder.java:122-139`); that is mode-transition concern, orthogonal to the
  flag. Keep the flag-clear listener in `ReplicationLogGroup` next to the flag it owns.

### 6. RS-restart-in-cutover — reject the writer (chosen)
- The `failoverPending` flag serves the **live** demotion path (an existing ACTIVE_IN_SYNC writer
  being demoted). RS-restart-in-cutover is handled differently: **never create a writer during
  cutover.** `init()`'s fail-fast gate rejects the mutation-blocked role (`!role.isActive() ||
  role.isMutationBlocked()`), and `ACTIVE_IN_SYNC_TO_STANDBY` is the only state mapping to
  `ACTIVE_TO_STANDBY` (mutation-blocked). So a restart in cutover throws in `init()`, creates no
  writer, and mints no files — the deadlock cannot re-arm. `computeIfAbsent` does not cache a throwing
  factory, so the first write after returning to pure ACTIVE re-runs `init()` successfully.
- This reverts the committed d69277ab55 (which mapped `ACTIVE_IN_SYNC_TO_STANDBY → SYNC` to start a
  writer at restart). `initializeReplicationMode` maps only `ACTIVE_IN_SYNC → SYNC`; the committed
  `testInitInSyncToStandbyStartsInSync` flips to `testInitFailsFastInCutover` (expects init failure).
- The IRO explicit mutation-block check (`IndexRegionObserver.java:886`) stays: on the **cached**
  live-demotion path `get()` returns the existing group and never re-runs `init()`, so that check is
  the only block on a mutation once the writer already exists.

### 7. `close()` unsubscribe fix (`ReplicationLogGroup.java:831-836`)
- Ensure the unsubscribe set matches every state `subscribeToDemotion()` subscribed —
  `ACTIVE_IN_SYNC_TO_STANDBY`, `STANDBY`, `DEGRADED_STANDBY`, and the new `ACTIVE_IN_SYNC` flag-clear
  listener — so a closed group leaks no ZK watchers.

## Files
- `phoenix-core-server/.../replication/ReplicationLog.java` — `LogRotationTask.run()` guard reads
  `logGroup.isFailoverPending()`.
- `phoenix-core-server/.../replication/SyncModeImpl.java` — `onFailure` fail-stop when
  `failoverPending`.
- `phoenix-core-server/.../replication/ReplicationLogGroup.java` — `failoverPending` flag +
  set/get; rework `subscribeToDemotion` (set flag on cutover, not close); `ACTIVE_IN_SYNC` flag-clear
  listener; restart-in-cutover flag-set; fix `close()` unsubscribe set.

## Tests
Unit — `ReplicationLogGroupTest` (extends `ReplicationLogBaseTest`, uses the `HAGroupState` ctor +
Mockito `haGroupStoreManager` + `ArgumentCaptor<HAGroupStateListener>` on `subscribeToTargetState`,
per the existing cutover-listener tests at `ReplicationLogGroupTest.java:2466-2550`):
- `testCutoverSetsFailoverPending`: capture the `ACTIVE_IN_SYNC_TO_STANDBY` listener, fire it, assert
  `isFailoverPending()` and that a subsequent `forceRotation()`/tick stages **no** new writer and the
  group is **not** closed (writer still open, appends still succeed).
- `testAbortClearsFailoverPending`: after set, fire the `ACTIVE_IN_SYNC` listener, assert
  `!isFailoverPending()` and rotation resumes (next tick stages a writer again).
- `testCutoverKeepsWriterOpenForInflight`: after set, `append()` + `sync()` still succeed on the
  open writer (no `IOException("Closed")`).
- `testCutoverSyncFailureAborts`: with `failoverPending` set, drive a SYNC failure and assert `sync()`
  throws (fail-stop, no SYNC→SAF transition) and `setHAGroupStatusToStoreAndForward` is never called,
  and the mode is not STORE_AND_FORWARD.
- `testInitFailsFastInCutover`: build a group with persisted state `ACTIVE_IN_SYNC_TO_STANDBY`, assert
  `init()` throws (mutation-blocked role rejected). Replaces the committed
  `testInitInSyncToStandbyStartsInSync` (which expected SYNC — reverted with d69277ab55).
- `testCloseUnsubscribesListeners`: extend the existing unsubscribe test to verify
  `ACTIVE_IN_SYNC_TO_STANDBY` and `ACTIVE_IN_SYNC` (the latter `times(2)`: forwarder mode listener +
  group abort-resume listener).

IT — `ReplicationLogGroupIT`: drive a real ZK transition to `ACTIVE_IN_SYNC_TO_STANDBY`, assert the
shard directory stops gaining new `.plog` files across >1 round while the writer stays open; then a
real graceful failover completes (standby promotes, active reaches STANDBY, group closes). An abort
variant: transition to cutover then abort back to `ACTIVE_IN_SYNC`, assert rotation resumes (new files
appear again after the flag clears).

## Verify
```
mvn spotless:apply
mvn install -pl phoenix-core-server -DskipTests -am        # IT runs against the installed server jar
mvn test   -pl phoenix-core -Dtest=ReplicationLogGroupTest,ReplicationLogTest
mvn verify -pl phoenix-core -Dit.test=ReplicationLogGroupIT
```

## Out of scope (documented residuals, not fixed here)
- **`isFileClosed` handshake.** `ReplicationLogProcessor.isFileClosed`
  (`ReplicationLogProcessor.java:347-356`, currently dead code) could give the standby an authoritative
  "is the active still writing" signal to replace the timing margin — but only under a seal-at-boundary
  design, and it requires standby/replay-side changes. Not in scope for this active-side fix.
