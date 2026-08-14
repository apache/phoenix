# Reconcile convergent CAS races in `setHAGroupStatusIfNeeded` (PHOENIX-7990)

**Status: DONE (2026-08-18). Two-layer reconcile (client reorder + server swallow) implemented
and amended into the PHOENIX-7990 commit (HEAD `da5d1f02d9` on `PHOENIX-7562-feature-new`). All
convergent-race ITs pass.**

## Problem

When >=2 co-active RegionServers drive the shared HA-status znode to the same target state,
each does an optimistic CAS whose expected version comes from the watch-lagged local cache. The
first writer wins; the losers were aborting the RegionServer even though the shared status was
already at the target. Two paths: the peer-degrade path (`ACTIVE_IN_SYNC -> ACTIVE_NOT_IN_SYNC`)
and, more commonly, the forwarders racing `ACTIVE_NOT_IN_SYNC -> ACTIVE_IN_SYNC`.

## Design (as landed)

The convergent losers split into two subcases by whether the winner's write reached the loser's
cache **before** it fired. Each is reconciled from a source that is authoritative **for its
path** — never the watch-lagged cache used as a false-success signal. (This is the invariant the
earlier "pre-validate cache short-circuit" expansion violated and why it was abandoned: it could
conclude SYNC success from a stale cache while ZK was still ANIS.)

### Subcase A — cache-stale loser (client fix)

Cache still at the predecessor when it fires: the CAS **is attempted** and loses stale; the loop
re-reads fresh from ZK and retries, bounded by `SET_HA_GROUP_STATUS_MAX_ATTEMPTS = 3`.

Fix: in `HAGroupStoreClient.setHAGroupStatusIfNeeded` (`HAGroupStoreClient.java:411`), the
existing `attempt > 1 && current == target` no-op was **moved ahead of**
`validateTransitionAndGetWaitTime`. On the retry path `currentHAGroupStoreRecord` is the fresh ZK
re-read (authoritative), so a converged non-self-transitionable target (e.g. `ACTIVE_IN_SYNC`)
returns a no-op success instead of throwing `InvalidClusterRoleTransitionException` on the
`X -> X` self-transition. Attempt 1 never short-circuits, so the periodic S&F heartbeat still
writes its znode mtime bump that gates SYNC promotion (see mtime-gate note below).

### Subcase B — watch-won loser (server fix)

Winner's write already propagated to the cache: `validate(AIS -> AIS)` throws
`InvalidClusterRoleTransitionException` **before any CAS is attempted** — no stale-version
exception is ever thrown, so the client cannot catch it.

Fix: in `HAGroupStoreManager.setHAGroupStatusToSync` (`HAGroupStoreManager.java:357`), catch
`InvalidClusterRoleTransitionException` and return `0L` **only when**
`isStateAlreadyUpdated(client, name, targetHAGroupState)` (`HAGroupStoreManager.java:769`, the
precise `current == target` check the failover listener already uses at :726). A genuinely
invalid transition (current != target) still propagates.

This swallow is semantically honest because the `ACTIVE_IN_SYNC` LOCAL listener in
`ReplicationLogGroup.subscribeToStateChanges` (`ReplicationLogGroup.java:712`) already drives
every co-active RS to the correct end state when the winner's write propagates:
`setFailoverPending(false)` (resume rotation) + `checkAndSetModeAndNotify(SYNC_AND_FORWARD ->
SYNC)`. So the loser's `0L` reflects a goal already achieved, not a missed action — and the
loser need not retry its own write.

### mtime gate constraint (must not break)

The `ACTIVE_NOT_IN_SYNC` heartbeat (`StoreAndForwardModeImpl.startHAGroupStoreUpdateTask`,
interval `ZK_SESSION_TIMEOUT x 0.7`) re-writes the same state to bump the znode mtime, which
feeds the SYNC-promotion gate in `validateTransitionAndGetWaitTime`: `ANIS -> AIS` is deferred
until `mtime + waitTimeForSyncModeInMs (x 1.1) <= now`. Because `0.7x < 1.1x`, the gate stays
shut while any RS still heartbeats in S&F. The client reorder keeps this intact: `ANIS` is
self-transitionable and attempt 1 never short-circuits, so the heartbeat write always lands.

Rejected alternatives (kept for the record): pre-validate cache short-circuit (false SYNC
success from stale cache — the reason this design was walked back); uniform same-state no-op
(suppresses heartbeat mtime bump, breaks gate); AIS self-edges in the transition table
(redundant znode write + peer `FailoverManagementListener` re-fire per convergent tick).

## Tests

- `HAGroupStoreManagerIT#testSetHAGroupStatusToSyncConvergentRaceIsNoOp` (NEW) — record already
  at `ACTIVE_IN_SYNC`, cache caught up; `setHAGroupStatusToSync` returns `0L` with **no ZK
  write** instead of throwing. Covers subcase B.
- `HAGroupStoreClientIT` (existing baseline, kept): `...SameStateRefreshBumpsVersion` (attempt-1
  ANIS heartbeat bumps version), `...ConvergentRaceReconciles` (stale-CAS loser converges — the
  subcase-A path), `testConcurrentStoreAndForwardHeartbeatBumpsVersionAndMtimeEachCycle`
  (version + mtime advance each cycle, no abort). The prior expansion tests (watch-won in the
  client, retry-bound exhaustion via Mockito spy, herd-collapse "+1/cycle") were reverted along
  with the abandoned client-side expansion.

## Verification (done)

```
mvn -q -pl phoenix-core-client compile
mvn -q spotless:apply -pl phoenix-core-client,phoenix-core
mvn install -pl phoenix-core-client,phoenix-core-server -DskipTests
mvn verify -pl phoenix-core -Dit.test='HAGroupStoreManagerIT#testSetHAGroupStatusToSyncConvergentRaceIsNoOp+testSetHAGroupStatusToSync'
mvn verify -pl phoenix-core -Dit.test='HAGroupStoreClientIT#testSetHAGroupStatusIfNeededSameStateRefreshBumpsVersion+testSetHAGroupStatusIfNeededConvergentRaceReconciles+testConcurrentStoreAndForwardHeartbeatBumpsVersionAndMtimeEachCycle+testSetHAGroupStatusIfNeededDeleteZKAndSystemTableRecord'
```

All pass. Amended into the PHOENIX-7990 commit (single commit, HEAD `da5d1f02d9`).

Full analysis: `docs/HA_Status_CAS_Stale_Cache_BadVersion.md`.