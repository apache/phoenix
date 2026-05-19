# ZK -- ZooKeeper Coordination Substrate

**Source:** [`ZK.tla`](../ZK.tla)

## Overview

`ZK` models the ZK session lifecycle and connection state as environment actions. Two independent `PathChildrenCache` instances per `HAGroupStoreClient` drive the protocol:

- **`pathChildrenCache` (LOCAL):** Watches the local cluster's ZK znode. Connection loss sets `isHealthy = false`, blocking all `setHAGroupStatusIfNeeded()` calls (auto-completion, heartbeat, writer cluster-state transitions, failover trigger).
- **`peerPathChildrenCache` (PEER):** Watches the peer cluster's ZK znode via a separate `CuratorFramework`/ZK connection. Connection loss or session expiry suppresses all peer-reactive transitions (`PeerReact*` actions in [HAGroupStore.md](HAGroupStore.md)).

### ZK Failure Modes

Three failure modes are modeled:

1. **Peer disconnection (transient):** `peerPathChildrenCache` loses TCP connection. Peer-reactive transitions suppressed. On reconnect, Curator re-syncs and fires synthetic events.
2. **Peer session expiry (permanent until recovery):** ZK session expires. All watches permanently lost. Curator must establish a new session via retry policy. Session expiry implies disconnection.
3. **Local disconnection:** `pathChildrenCache` loses connection. `isHealthy = false`, blocking all `setHAGroupStatusIfNeeded()` calls.

### ZK Liveness Assumption (ZLA)

The liveness specifications encode the ZK Liveness Assumption via WF on `ZKPeerReconnect`, `ZKPeerSessionRecover`, and `ZKLocalReconnect` (Tier 2 fairness). This encodes the assumption that ZK sessions are eventually alive and connected. Without this assumption, the adversary could permanently disconnect ZK, preventing all watcher-driven transitions and violating every liveness property.

### Post-Abort ATS Reconciliation

`ZKPeerReconnect` and `ZKPeerSessionRecover` fold a post-abort ATS reconciliation: when the local cluster is in ATS and the peer is in S or DS at the moment of reconnect, the `PathChildrenCache` rebuild fires a synthetic event that triggers the `FailoverManagementListener`. No existing `PeerReact*` action handles (ATS, S/DS) -- the transient AbTS state was missed during the partition. The reconciliation transitions ATS -> AbTAIS, which auto-completes to AIS via `AutoComplete` in [HAGroupStore.md](HAGroupStore.md).

This is folded into the reconnect action (rather than modeled as a separate action with a boolean flag) because the CONNECTION_RECONNECTED -> `PathChildrenCache` rebuild -> `handleStateChange()` -> `FailoverManagementListener` chain is synchronous on the same event thread, following the same listener-effect folding pattern used for `recoveryListener` and `degradedListener` in [HAGroupStore.md](HAGroupStore.md).

Both `ZKPeerReconnect` and `ZKPeerSessionRecover` reuse the identical reconciliation fold (Curator rebuild is the same whether triggered by reconnection or session recovery), so it is extracted into a module-local operator:

```tla
ATSReconcileEffect(c) ==
    IF clusterState[c] = "ATS" /\ clusterState[Peer(c)] \in {"S", "DS"}
    THEN clusterState' = [clusterState EXCEPT ![c] = "AbTAIS"]
    ELSE UNCHANGED clusterState
```

This keeps the two actions' reconciliation branches from drifting apart.

**Race safety:** `ZKPeerReconnect` requires `zkPeerConnected[c] = FALSE`, so it cannot fire during normal operation when the connection is healthy. The normal transient (ATS, S) state during happy-path failover is handled by `PeerReactToATS` on the peer side.

### Retry Exhaustion

Retry exhaustion of the `FailoverManagementListener` (2-retry limit) is modeled in [HAGroupStore.md](HAGroupStore.md) as `ReactiveTransitionFail(c)`, not in this module. The ZK module models only the connection/session lifecycle, not application-level retry behavior.

## Implementation Traceability

| TLA+ Action | Java Source |
|---|---|
| `ZKPeerDisconnect(c)` | `HAGroupStoreClient.createCacheListener()` L894-898 -- `peerPathChildrenCache` CONNECTION_LOST/CONNECTION_SUSPENDED (no effect on `isHealthy` for PEER cache) |
| `ZKPeerReconnect(c)` | `HAGroupStoreClient.createCacheListener()` L903-906 -- `peerPathChildrenCache` CONNECTION_RECONNECTED; Curator re-syncs `PathChildrenCache`, fires synthetic CHILD_UPDATED events |
| `ZKPeerSessionExpiry(c)` | Curator maps SESSION_EXPIRED to CONNECTION_LOST internally; no explicit SESSION_EXPIRED handling in Phoenix |
| `ZKPeerSessionRecover(c)` | Curator retry policy establishes new session; `PathChildrenCache` rebuilds |
| `ZKLocalDisconnect(c)` | `HAGroupStoreClient.createCacheListener()` L894-898 -- `pathChildrenCache` (LOCAL) CONNECTION_LOST sets `isHealthy = false` |
| `ZKLocalReconnect(c)` | `HAGroupStoreClient.createCacheListener()` L903-906 -- `pathChildrenCache` (LOCAL) CONNECTION_RECONNECTED sets `isHealthy = true` |

```tla
EXTENDS SpecState, Types
```

## ZKPeerDisconnect -- Peer ZK Connection Drops

The `peerPathChildrenCache` loses its TCP connection to the peer ZK quorum. During disconnection, no watcher notifications are delivered, so peer-reactive transitions for cluster `c` are suppressed.

The implementation does NOT set `isHealthy = false` for PEER cache disconnection -- only LOCAL cache disconnection affects `isHealthy`. This means peer disconnection suppresses watcher delivery but does not block local ZK writes (auto-completion, heartbeat, etc.).

**Fairness:** No fairness (Tier 4). ZK disconnections are genuinely non-deterministic environment events.

Source: `HAGroupStoreClient.createCacheListener()` L894-898 (CONNECTION_LOST/CONNECTION_SUSPENDED for PEER cache).

```tla
ZKPeerDisconnect(c) ==
    /\ zkPeerConnected[c] = TRUE
    /\ zkPeerConnected' = [zkPeerConnected EXCEPT ![c] = FALSE]
    /\ UNCHANGED <<clusterState, writerMode, outDirEmpty, hdfsAvailable,
                   antiFlapTimer, replayState, lastRoundInSync,
                   lastRoundProcessed, failoverPending, inProgressDirEmpty,
                   zkPeerSessionAlive, zkLocalConnected>>
```

## ZKPeerReconnect -- Peer ZK Connection Re-Established

The `peerPathChildrenCache` re-establishes its TCP connection to the peer ZK quorum. Curator re-syncs `PathChildrenCache` by re-reading children and generating synthetic CHILD_UPDATED events. `handleStateChange()` compares against `lastKnownPeerState` and only fires notifications if the peer state differs from the last known value -- same-state suppression. In the TLA+ model, this is naturally handled: `PeerReact*` actions are re-enabled by the `zkPeerConnected` guard and fire when their peer-state guard is satisfied.

Reconnection requires a live session -- if the session is expired, a new session must be established first via `ZKPeerSessionRecover`.

### Post-Abort ATS Reconciliation

When the local cluster is in ATS and the peer is in S or DS at the moment of reconnect, the `PathChildrenCache` rebuild fires a synthetic event that triggers the `FailoverManagementListener`. No existing `PeerReact*` action handles (ATS, S/DS). This happens when:

1. A failover was initiated: (AIS, S) -> (ATS, S)
2. The peer ZK connection was lost during the failover window
3. The standby detected ATS and moved to STA, then AbTS (admin abort), then back to S
4. The transient AbTS state was missed by the active cluster because the peer ZK connection was down
5. On reconnect, the active sees the peer is in S (or DS if degradation occurred), but the active is still stuck in ATS

The reconciliation transitions ATS -> AbTAIS, which auto-completes to AIS via `AutoComplete`, recovering the stuck-ATS cluster.

**Fairness:** WF (Tier 2). Encodes the ZK Liveness Assumption.

Source: `HAGroupStoreClient.createCacheListener()` L903-906 (CONNECTION_RECONNECTED for PEER cache).

```tla
ZKPeerReconnect(c) ==
    /\ zkPeerConnected[c] = FALSE
    /\ zkPeerSessionAlive[c] = TRUE
    /\ zkPeerConnected' = [zkPeerConnected EXCEPT ![c] = TRUE]
    /\ IF clusterState[c] = "ATS" /\ clusterState[Peer(c)] \in {"S", "DS"}
       THEN clusterState' = [clusterState EXCEPT ![c] = "AbTAIS"]
       ELSE UNCHANGED clusterState
    /\ UNCHANGED <<writerMode, outDirEmpty, hdfsAvailable,
                   antiFlapTimer, replayState, lastRoundInSync,
                   lastRoundProcessed, failoverPending, inProgressDirEmpty,
                   zkPeerSessionAlive, zkLocalConnected>>
```

## ZKPeerSessionExpiry -- Peer ZK Session Expires

The ZK server evicts cluster `c`'s peer session after the session timeout elapses without heartbeats. All watches are permanently lost. The client must establish a new session before any watcher notifications can be delivered.

**Session expiry implies disconnection:** When the session dies, the TCP connection is also considered dead. Both `zkPeerSessionAlive` and `zkPeerConnected` are set to FALSE. This invariant relationship is verified by the `ZKSessionConsistency` invariant in [ConsistentFailover.md](ConsistentFailover.md).

The implementation has no explicit SESSION_EXPIRED handling. Curator maps session expiry to CONNECTION_LOST internally, then attempts to create a new session via its retry policy.

**Fairness:** No fairness (Tier 4). ZK session expiry is a genuinely non-deterministic environment event.

Source: Curator internal session management; no explicit Phoenix SESSION_EXPIRED handler.

```tla
ZKPeerSessionExpiry(c) ==
    /\ zkPeerSessionAlive[c] = TRUE
    /\ zkPeerSessionAlive' = [zkPeerSessionAlive EXCEPT ![c] = FALSE]
    /\ zkPeerConnected' = [zkPeerConnected EXCEPT ![c] = FALSE]
    /\ UNCHANGED <<clusterState, writerMode, outDirEmpty, hdfsAvailable,
                   antiFlapTimer, replayState, lastRoundInSync,
                   lastRoundProcessed, failoverPending, inProgressDirEmpty,
                   zkLocalConnected>>
```

## ZKPeerSessionRecover -- Peer ZK Session Recovered

Curator's retry policy establishes a new ZK session for the peer connection. `PathChildrenCache` rebuilds its internal state by re-reading all children and fires synthetic CHILD_ADDED events, effectively re-syncing.

**Session recovery implies reconnection:** The new session comes with a live TCP connection. Both `zkPeerSessionAlive` and `zkPeerConnected` are set to TRUE.

### Post-Abort ATS Reconciliation

Same folded reconciliation as `ZKPeerReconnect`. Session recovery triggers a full `PathChildrenCache` rebuild with synthetic CHILD_ADDED events, which invokes the `FailoverManagementListener` synchronously. The reconciliation logic is identical: ATS + peer in {S, DS} -> AbTAIS.

**Fairness:** WF (Tier 2). Encodes the ZK Liveness Assumption.

Source: Curator retry policy -> new session -> `PathChildrenCache` rebuild.

```tla
ZKPeerSessionRecover(c) ==
    /\ zkPeerSessionAlive[c] = FALSE
    /\ zkPeerSessionAlive' = [zkPeerSessionAlive EXCEPT ![c] = TRUE]
    /\ zkPeerConnected' = [zkPeerConnected EXCEPT ![c] = TRUE]
    /\ IF clusterState[c] = "ATS" /\ clusterState[Peer(c)] \in {"S", "DS"}
       THEN clusterState' = [clusterState EXCEPT ![c] = "AbTAIS"]
       ELSE UNCHANGED clusterState
    /\ UNCHANGED <<writerMode, outDirEmpty, hdfsAvailable,
                   antiFlapTimer, replayState, lastRoundInSync,
                   lastRoundProcessed, failoverPending, inProgressDirEmpty,
                   zkLocalConnected>>
```

## ZKLocalDisconnect -- Local ZK Connection Drops

The `pathChildrenCache` (LOCAL) loses its connection to the local ZK quorum. The implementation sets `isHealthy = false`, which blocks all `setHAGroupStatusIfNeeded()` calls with IOException. This suppresses auto-completion, heartbeat, writer cluster-state transitions, and failover trigger.

**Fairness:** No fairness (Tier 4). ZK disconnections are genuinely non-deterministic environment events.

Source: `HAGroupStoreClient.createCacheListener()` L894-898 (CONNECTION_LOST/CONNECTION_SUSPENDED for LOCAL cache).

```tla
ZKLocalDisconnect(c) ==
    /\ zkLocalConnected[c] = TRUE
    /\ zkLocalConnected' = [zkLocalConnected EXCEPT ![c] = FALSE]
    /\ UNCHANGED <<clusterState, writerMode, outDirEmpty, hdfsAvailable,
                   antiFlapTimer, replayState, lastRoundInSync,
                   lastRoundProcessed, failoverPending, inProgressDirEmpty,
                   zkPeerConnected, zkPeerSessionAlive>>
```

## ZKLocalReconnect -- Local ZK Connection Re-Established

The `pathChildrenCache` (LOCAL) re-establishes its connection to the local ZK quorum. The implementation sets `isHealthy = true`, re-enabling all `setHAGroupStatusIfNeeded()` calls.

**Fairness:** WF (Tier 2). Encodes the ZK Liveness Assumption. This is the basis for SF on all actions guarded by `zkLocalConnected`.

Source: `HAGroupStoreClient.createCacheListener()` L903-906 (CONNECTION_RECONNECTED for LOCAL cache).

```tla
ZKLocalReconnect(c) ==
    /\ zkLocalConnected[c] = FALSE
    /\ zkLocalConnected' = [zkLocalConnected EXCEPT ![c] = TRUE]
    /\ UNCHANGED <<clusterState, writerMode, outDirEmpty, hdfsAvailable,
                   antiFlapTimer, replayState, lastRoundInSync,
                   lastRoundProcessed, failoverPending, inProgressDirEmpty,
                   zkPeerConnected, zkPeerSessionAlive>>
```
