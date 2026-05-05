# Admin -- Operator-Initiated Actions

**Source:** [`Admin.tla`](../Admin.tla)

## Overview

`Admin` models the human operator (Admin actor) who drives failover and abort via the `PhoenixHAAdminTool` CLI, which delegates to `HAGroupStoreManager` coprocessor endpoints. This module contains four actions: `AdminStartFailover` (initiate failover), `AdminAbortFailover` (abort an in-progress failover), `AdminGoOffline` (take a standby cluster offline), and `AdminForceRecover` (force-recover from OFFLINE). The last two are gated on the `UseOfflinePeerDetection` feature flag and model the proactive design for peer OFFLINE detection using `PhoenixHAAdminTool update --force`.

These are the only actions in the specification that represent deliberate human decisions rather than automated system behavior. All four (`AdminStartFailover`, `AdminAbortFailover`, `AdminGoOffline`, `AdminForceRecover`) receive no fairness in the liveness specifications. The admin is genuinely non-deterministic. The admin might never initiate a failover, or might abort every failover attempt. Imposing fairness on admin actions would force unrealistic guarantees about human behavior.

### Modeling Choice: Direct ZK Writes

Unlike the peer-reactive transitions in [HAGroupStore.md](HAGroupStore.md), admin actions are direct ZK writes -- they are not watcher-dependent. The admin CLI writes directly to the local ZK znode via the coprocessor endpoint. No `zkPeerConnected` or `zkLocalConnected` guard is needed because the admin tool manages its own ZK connection independently of the `HAGroupStoreClient` watcher infrastructure. `AdminGoOffline` and `AdminForceRecover` also use the `--force` path which writes directly to ZK, so no `zkLocalConnected` guard is needed for those actions either.

## Implementation Traceability

| TLA+ Action | Java Source |
|---|---|
| `AdminStartFailover(c)` | `HAGroupStoreManager.initiateFailoverOnActiveCluster()` L375-400 |
| `AdminAbortFailover(c)` | `HAGroupStoreManager.setHAGroupStatusToAbortToStandby()` L419-425; also clears `failoverPending` (models `abortFailoverListener` L173-185) |
| `AdminGoOffline(c)` | `PhoenixHAAdminTool update --state OFFLINE` (gated on `UseOfflinePeerDetection`) |
| `AdminForceRecover(c)` | `PhoenixHAAdminTool update --force --state STANDBY` (OFFLINE -> S; gated on `UseOfflinePeerDetection`) |

```tla
EXTENDS SpecState, Types
```

## AdminStartFailover -- Initiate Failover

The admin initiates failover on the active cluster. Two paths depending on current state:

### AIS Path: AIS -> ATS

The cluster is fully in sync. The OUT directory must be empty and all live RS must be in SYNC mode. DEAD RSes are allowed -- an RS can crash while the cluster is AIS without changing the HA group state. The implementation checks `clusterState = AIS`, not per-RS modes; a DEAD RS is not writing, so the remaining SYNC RSes and empty OUT dir ensure safety.

### ANIS Path: ANIS -> ANISTS

The cluster is not in sync (at least one RS is in STORE_AND_FWD). The implementation only validates the current state (ANIS) and peer state -- no `outDirEmpty` or writer-mode guards are needed because the forwarder will drain OUT after the transition. The ANISTS -> ATS transition (`ANISTSToATS` in [HAGroupStore.md](HAGroupStore.md)) guards on `outDirEmpty` and the anti-flapping gate.

### Peer-State Guard (Both Paths)

The peer must be in a stable standby state (S or DS) to prevent initiating a new failover during the non-atomic window of a previous failover (where the peer may still be in ATS). Without this guard, the admin could produce an irrecoverable `(ATS, ATS)` or `(ANISTS, ATS)` deadlock where both clusters are transitioning to standby with mutations blocked on both sides.

### Post-Condition

Cluster `c` transitions to ATS or ANISTS, both of which map to the ACTIVE_TO_STANDBY role, blocking mutations (`isMutationBlocked() = true`).

Source: `initiateFailoverOnActiveCluster()` L375-400 checks current state and selects AIS -> ATS or ANIS -> ANISTS. Peer-state guard: `getHAGroupStoreRecordFromPeer()` (`HAGroupStoreClient` L421).

```tla
AdminStartFailover(c) ==
    /\ clusterState[Peer(c)] \in {"S", "DS"}
    /\ \/ /\ clusterState[c] = "AIS"
          /\ outDirEmpty[c]
          /\ \A rs \in RS : writerMode[c][rs] \in {"SYNC", "DEAD"}
          /\ clusterState' = [clusterState EXCEPT ![c] = "ATS"]
       \/ /\ clusterState[c] = "ANIS"
          /\ clusterState' = [clusterState EXCEPT ![c] = "ANISTS"]
    /\ UNCHANGED <<writerMode, outDirEmpty, hdfsAvailable, antiFlapTimer,
                   replayState, lastRoundInSync, lastRoundProcessed,
                   failoverPending, inProgressDirEmpty,
                   zkPeerConnected, zkPeerSessionAlive, zkLocalConnected>>
```

## AdminAbortFailover -- Abort In-Progress Failover

The admin aborts an in-progress failover from the standby side. The cluster transitions from STA (STANDBY_TO_ACTIVE) to AbTS (ABORT_TO_STANDBY). The peer (in ATS) will react via `PeerReactToAbTS` in [HAGroupStore.md](HAGroupStore.md), transitioning to AbTAIS. Both then auto-complete back to their pre-failover states.

### Why Abort Must Originate from the STA Side

Abort must originate from the STA side to prevent dual-active races. If abort could originate from the ATS side, the following race would be possible:

1. ATS writes AbTAIS (abort on active side)
2. Meanwhile, STA completes failover and writes AIS
3. Result: (AbTAIS, AIS) -- both clusters briefly in ACTIVE role

By requiring abort to originate from STA (writing AbTS), the standby explicitly declares it is returning to standby. The active (in ATS) detects the peer's AbTS via watcher and transitions to AbTAIS. This ordering is safe because the STA -> AbTS transition means the standby has abandoned the failover. This is the `AbortSafety` invariant in [ConsistentFailover.md](ConsistentFailover.md).

### failoverPending Side-Effect

Also clears `failoverPending[c]`, modeling the `abortFailoverListener` (`ReplicationLogDiscoveryReplay.java` L173-185) which fires on LOCAL ABORT_TO_STANDBY, calling `failoverPending.set(false)`. This ensures the replay state machine does not attempt to trigger failover after the abort.

Source: `setHAGroupStatusToAbortToStandby()` L419-425.

```tla
AdminAbortFailover(c) ==
    /\ clusterState[c] = "STA"
    /\ clusterState' = [clusterState EXCEPT ![c] = "AbTS"]
    /\ failoverPending' = [failoverPending EXCEPT ![c] = FALSE]
    /\ UNCHANGED <<writerMode, outDirEmpty, hdfsAvailable, antiFlapTimer,
                   replayState, lastRoundInSync, lastRoundProcessed,
                   inProgressDirEmpty,
                   zkPeerConnected, zkPeerSessionAlive, zkLocalConnected>>
```

## AdminGoOffline -- Take Standby Cluster Offline

Admin takes a standby cluster offline. Gated on `UseOfflinePeerDetection`.

Pre: Cluster `c` is in S or DS (a standby state).
Post: Cluster `c` transitions to OFFLINE.

In the implementation, entering OFFLINE requires `PhoenixHAAdminTool update --force --state OFFLINE`, which bypasses `isTransitionAllowed()`. The operator decides when to take a cluster offline for maintenance or decommissioning.

No ZK connectivity guard: the `--force` path writes directly to ZK, bypassing the `isHealthy` check used by `setHAGroupStatusIfNeeded()`.

Source: `PhoenixHAAdminTool update --state OFFLINE (--force)`.

```tla
AdminGoOffline(c) ==
    /\ UseOfflinePeerDetection = TRUE
    /\ clusterState[c] \in {"S", "DS"}
    /\ clusterState' = [clusterState EXCEPT ![c] = "OFFLINE"]
    /\ UNCHANGED <<writerMode, outDirEmpty, hdfsAvailable, antiFlapTimer,
                   replayState, lastRoundInSync, lastRoundProcessed,
                   failoverPending, inProgressDirEmpty,
                   zkPeerConnected, zkPeerSessionAlive, zkLocalConnected>>
```

## AdminForceRecover -- Force-Recover from OFFLINE

Admin force-recovers a cluster from OFFLINE. Gated on `UseOfflinePeerDetection`.

Pre: Cluster `c` is in OFFLINE.
Post: Cluster `c` transitions to S (STANDBY).

Recovery from OFFLINE requires `PhoenixHAAdminTool update --force --state STANDBY`, which bypasses `isTransitionAllowed()` (OFFLINE has no allowed outbound transitions in the implementation).

The S-entry side effects mirror the pattern used by `PeerReactToAIS` (ATS->S) and `AutoComplete` (AbTS->S):
- `writerMode` reset to INIT for all RS (replication subsystem restart on standby entry)
- `outDirEmpty` set to TRUE (OUT directory cleared)
- `replayState` set to SYNCED_RECOVERY (recoveryListener fold)

No ZK connectivity guard: the `--force` path writes directly to ZK.

Source: `PhoenixHAAdminTool update --force --state STANDBY`.

```tla
AdminForceRecover(c) ==
    /\ UseOfflinePeerDetection = TRUE
    /\ clusterState[c] = "OFFLINE"
    /\ clusterState' = [clusterState EXCEPT ![c] = "S"]
    /\ writerMode' = [writerMode EXCEPT ![c] =
            [rs \in RS |-> "INIT"]]
    /\ outDirEmpty' = [outDirEmpty EXCEPT ![c] = TRUE]
    /\ replayState' = [replayState EXCEPT ![c] = "SYNCED_RECOVERY"]
    /\ UNCHANGED <<hdfsAvailable, antiFlapTimer,
                   lastRoundInSync, lastRoundProcessed,
                   failoverPending, inProgressDirEmpty,
                   zkPeerConnected, zkPeerSessionAlive, zkLocalConnected>>
```
