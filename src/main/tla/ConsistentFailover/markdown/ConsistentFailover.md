# ConsistentFailover -- Root Orchestrator Module

**Source:** [`ConsistentFailover.tla`](../ConsistentFailover.tla)

## Overview

`ConsistentFailover` is the root orchestrator module of the Phoenix Consistent Failover TLA+ specification. State variables are declared in [`SpecState.tla`](../SpecState.tla); the root module has `EXTENDS SpecState, Types`. The root module defines the initial state (`Init`), the next-state relation (`Next`), the specification formulas (`SafetySpec`, `Spec`), and all safety invariants, action constraints, liveness properties, and fairness assumptions. It composes actor-driven actions from sub-modules via `INSTANCE`.

The module models the HA group state machine for two paired Phoenix/HBase clusters. Each cluster maintains its HA group state in ZooKeeper. State transitions are driven by five categories of actors:

1. **Admin actions** -- human operator initiates or aborts failover
2. **Peer-reactive transitions** -- ZK watcher notifications from the peer cluster trigger state changes
3. **Writer/reader state changes** -- per-RS replication writer mode transitions and standby-side replay progress
4. **HDFS availability incidents** -- NameNode crash and recovery
5. **ZK coordination failures** -- connection loss, session expiry, retry exhaustion

### ZK Coordination Model

ZK connection and session lifecycle are modeled explicitly. Peer-reactive transitions (`PeerReact*` actions) are guarded on `zkPeerConnected[c]` and `zkPeerSessionAlive[c]`. Auto-completion, heartbeat, writer ZK writes, and failover trigger are guarded on `zkLocalConnected[c]`. Retry exhaustion of the `FailoverManagementListener` (2-retry limit) is modeled as `ReactiveTransitionFail(c)` in [HAGroupStore.md](HAGroupStore.md).

```tla
EXTENDS SpecState, Types
```

## Implementation Traceability

| Modeled Concept | Java Class / Field |
|---|---|
| `clusterState` | `HAGroupStoreRecord` per-cluster ZK znode |
| `PeerReact*` actions | `FailoverManagementListener` (`HAGroupStoreManager.java` L633-706), delivered via `peerPathChildrenCache` |
| `ReactiveTransitionFail` | `FailoverManagementListener` 2-retry exhaustion (L653-704) |
| `TriggerFailover` | `Reader.TriggerFailover` via `shouldTriggerFailover()` L500-533 + `triggerFailover()` L535-548 |
| `AutoComplete` | `createLocalStateTransitions()` L140-150, delivered via local `pathChildrenCache` |
| `ANISTSToATS` | `HAGroupStoreManager.setHAGroupStatusToSync()` L341-355 |
| `AdminStartFailover` | `initiateFailoverOnActiveCluster()` L375-400 |
| `AdminAbortFailover` | `setHAGroupStatusToAbortToStandby()` L419-425 |
| `AdminGoOffline` | `PhoenixHAAdminTool update --state OFFLINE` (gated on `UseOfflinePeerDetection`) |
| `AdminForceRecover` | `PhoenixHAAdminTool update --force --state STANDBY` (OFFLINE -> S) (gated on `UseOfflinePeerDetection`) |
| `PeerReactToOFFLINE` | intended peer OFFLINE detection: AIS->AWOP, ANIS->ANISWOP; gated on `UseOfflinePeerDetection` |
| `PeerRecoverFromOFFLINE` | intended peer OFFLINE recovery: AWOP/ANISWOP->ANIS; gated on `UseOfflinePeerDetection` |
| `Init (AIS, S)` | Default initial states per team confirmation (PHOENIX_HA_TLA_PLAN.md Appendix A.6) |
| `MutualExclusion` | Architecture safety argument: at most one cluster in ACTIVE role |
| `AbortSafety` | Abort originates from STA side; AbTAIS only reachable via peer AbTS detection |
| `AllowedTransitions` | `HAGroupStoreRecord.java` L99-123 |
| `writerMode` | `ReplicationLogGroup` per-RS mode |
| `outDirEmpty` | `ReplicationLogDiscoveryForwarder.processNoMoreRoundsLeft()` L155-184 |
| `hdfsAvailable` | Abstract: NameNode availability per cluster (detected via IOException) |
| `RSCrash` | JVM crash, OOM, kill signal |
| `RSAbortOnLocalHDFSFailure` | `StoreAndForwardModeImpl.onFailure()` L115-123 |
| `HDFSDown`/`HDFSUp` | NameNode crash/recovery; `SyncModeImpl.onFailure()` L61-74 |
| `antiFlapTimer` | Countdown timer (Lamport CHARME 2005); `validateTransitionAndGetWaitTime()` L1027-1046 |
| `Tick` | Passage of wall-clock time |
| `ANISHeartbeat` | `StoreAndForwardModeImpl.startHAGroupStoreUpdateTask()` L71-87 |
| `replayState` | `ReplicationLogDiscoveryReplay` replay state (L550-555) |
| `lastRoundInSync` | `ReplicationLogDiscoveryReplay` L336-343 |
| `lastRoundProcessed` | `ReplicationLogDiscoveryReplay` L336-351 |
| `failoverPending` | `ReplicationLogDiscoveryReplay` L159-171 |
| `inProgressDirEmpty` | `ReplicationLogDiscoveryReplay` L500-533 |
| `ReplayAdvance` | `replay()` L336-343 (SYNC) and L345-351 (DEGRADED) |
| `ReplayRewind` | `replay()` L323-333 (CAS to SYNC) |
| Listener folds | `degradedListener` L136-145 and `recoveryListener` L147-157 folded into HAGroupStore S/DS-entry actions |
| `TriggerFailover` | `shouldTriggerFailover()` L500-533 + `triggerFailover()` L535-548 |
| `FailoverTriggerCorrectness` | Action constraint: STA->AIS requires replay-completeness conditions |
| `NoDataLoss` | Action constraint: zero RPO property |
| `zkPeerConnected` | `peerPathChildrenCache` TCP connection state (`HAGroupStoreClient` L110-112) |
| `zkPeerSessionAlive` | Peer ZK session state (Curator internal) |
| `zkLocalConnected` | `pathChildrenCache` TCP connection state; maps to `HAGroupStoreClient.isHealthy` (L878-911) |
| `ZKPeerDisconnect` | `peerPathChildrenCache` CONNECTION_LOST |
| `ZKPeerReconnect` | `peerPathChildrenCache` CONNECTION_RECONNECTED |
| `ZKPeerSessionExpiry` | Curator session expiry -> CONNECTION_LOST |
| `ZKPeerSessionRecover` | Curator retry -> new session |
| `ZKLocalDisconnect` | `pathChildrenCache` CONNECTION_LOST |
| `ZKLocalReconnect` | `pathChildrenCache` CONNECTION_RECONNECTED |

### failoverPending Lifecycle

| Event | Variable Effect | Source |
|---|---|---|
| Set TRUE | `PeerReactToATS` ([HAGroupStore.md](HAGroupStore.md)) | Standby detects peer ATS |
| Set FALSE | `TriggerFailover` ([Reader.md](Reader.md)) | Failover completes successfully |
| Set FALSE | `AdminAbortFailover` ([Admin.md](Admin.md)) | Operator aborts failover |

## Variables

The specification uses 13 variables, declared in [`SpecState.tla`](../SpecState.tla). The subsections below describe each variable’s role; see also [SpecState.md](SpecState.md).

### Cluster State

```tla
VARIABLE clusterState
```

`clusterState[c]` is the current HA group state of cluster `c`. Each cluster maintains its state as a ZK znode, updated via `setData().withVersion()` (optimistic locking). This is the primary state variable of the protocol -- almost every action reads or writes it.

Source: `HAGroupStoreRecord` per-cluster ZK znode at `phoenix/consistentHA/<group>`.

### Writer State

```tla
VARIABLE writerMode
```

`writerMode[c][rs]` is the current replication writer mode of region server `rs` on cluster `c`. The writer state machine is per-RS, reflecting the implementation where each `ReplicationLogGroup` independently manages its mode. Multiple RS on the same cluster can be in different modes simultaneously (e.g., one in SYNC and another in STORE_AND_FWD after an HDFS failure race).

Source: `ReplicationLogGroup` per-RS mode (`SyncModeImpl`, `StoreAndForwardModeImpl`, `SyncAndForwardModeImpl`).

```tla
VARIABLE outDirEmpty
```

`outDirEmpty[c]` is TRUE when the OUT directory on cluster `c` contains no buffered replication log files. FALSE when writes are accumulating locally. This is a per-cluster boolean (not per-RS) because the OUT directory is shared -- `ReplicationLogDiscoveryForwarder.processNoMoreRoundsLeft()` (L155-184) checks the entire directory.

Source: `ReplicationLogDiscoveryForwarder.processNoMoreRoundsLeft()` L155-184 checks `getInProgressFiles().isEmpty() && getNewFilesForRound(nextRound).isEmpty()`.

### Environment State

```tla
VARIABLE hdfsAvailable
```

`hdfsAvailable[c]` is TRUE when cluster `c`'s HDFS (NameNode) is accessible. FALSE after a NameNode crash. This is modeled as an abstract boolean flag rather than per-file HDFS state because the specification focuses on the protocol's reaction to HDFS availability, not on HDFS internals. The flag is not explicitly tracked in the implementation -- HDFS unavailability is detected reactively via IOException from HDFS write operations.

### Anti-Flapping Timer

```tla
VARIABLE antiFlapTimer
```

`antiFlapTimer[c]` is the per-cluster anti-flapping countdown timer. Counts down from `WaitTimeForSync` toward 0. The ANIS -> AIS transition is blocked while the timer is positive (gate closed). The S&F heartbeat resets the timer to `WaitTimeForSync`; the `Tick` action decrements it. See [Types.md](Types.md) for helper operator documentation and the Lamport CHARME 2005 countdown timer pattern.

Source: `HAGroupStoreClient.validateTransitionAndGetWaitTime()` L1027-1046.

### Replay State

```tla
VARIABLE replayState
VARIABLE lastRoundInSync
VARIABLE lastRoundProcessed
VARIABLE failoverPending
VARIABLE inProgressDirEmpty
```

These five variables model the standby-side replication replay state machine:

- `replayState[c]` -- the current replay state (NOT_INITIALIZED / SYNC / DEGRADED / SYNCED_RECOVERY). Source: `ReplicationLogDiscoveryReplay.java` L550-555.
- `lastRoundInSync[c]` -- the last round processed while in SYNC state; frozen during DEGRADED. Source: L336-343 (advance), L389 (rewind target).
- `lastRoundProcessed[c]` -- the last round processed regardless of state; rewinds to `lastRoundInSync` during SYNCED_RECOVERY. Source: L336-351.
- `failoverPending[c]` -- TRUE when the standby has received an STA notification and is waiting for replay to complete. Source: L159-171.
- `inProgressDirEmpty[c]` -- TRUE when no partially-processed replication log files exist. Source: `shouldTriggerFailover()` L500-533.

### ZK Coordination State

```tla
VARIABLE zkPeerConnected
VARIABLE zkPeerSessionAlive
VARIABLE zkLocalConnected
```

These three booleans per cluster model the ZK coordination substrate:

- `zkPeerConnected[c]` -- TRUE when the `peerPathChildrenCache` has a live TCP connection to the peer ZK quorum. When FALSE, no watcher notifications from the peer are delivered, suppressing all `PeerReact*` transitions. Source: `HAGroupStoreClient.createCacheListener()` L894-906.
- `zkPeerSessionAlive[c]` -- TRUE when the peer ZK session is alive. Session expiry permanently loses all watches until a new session is established. Session expiry implies disconnection. Source: Curator internal session management.
- `zkLocalConnected[c]` -- TRUE when the `pathChildrenCache` (local) has a live connection. When FALSE, `isHealthy = false`, blocking all `setHAGroupStatusIfNeeded()` calls. Source: `HAGroupStoreClient.createCacheListener()` L894-906.

### Variable Tuple

The `vars` tuple aggregates all 13 variables for use in temporal formulas (`[][Next]_vars`, `WF_vars(...)`, `SF_vars(...)`). It is defined in [`SpecState.tla`](../SpecState.tla) as a composition of four variable-group tuples -- `writerVars`, `clusterVars`, `replayVars`, `envVars` -- so every sub-module shares the same groups when writing `UNCHANGED` clauses. See [SpecState.md](SpecState.md) for the group definitions.

```tla
\* Defined in SpecState.tla:
\*   writerVars  == <<writerMode>>
\*   clusterVars == <<clusterState, outDirEmpty, antiFlapTimer,
\*                    failoverPending, inProgressDirEmpty>>
\*   replayVars  == <<replayState, lastRoundInSync, lastRoundProcessed>>
\*   envVars     == <<hdfsAvailable,
\*                    zkPeerConnected, zkPeerSessionAlive, zkLocalConnected>>
\*   vars        == <<writerVars, clusterVars, replayVars, envVars>>

STAtoAISTriggerReplayGuards(c) ==
    /\ failoverPending[c]
    /\ inProgressDirEmpty[c]
    /\ replayState[c] = "SYNC"
```

`STAtoAISTriggerReplayGuards` is the shared replay-completeness conjunction for STA -> AIS. `FailoverTriggerCorrectness` and `NoDataLoss` both use it so they cannot drift apart.

## Sub-Module Instances

```tla
haGroupStore == INSTANCE HAGroupStore
admin == INSTANCE Admin
writer == INSTANCE Writer
hdfs == INSTANCE HDFS
rs == INSTANCE RS
clk == INSTANCE Clock
reader == INSTANCE Reader
zk == INSTANCE ZK
```

Each sub-module is instantiated with default parameter passing (all variables and constants are shared by name). The instance names (`haGroupStore`, `admin`, `writer`, etc.) serve as namespace prefixes in the `Next` relation: `haGroupStore!PeerReactToATS(c)`, `admin!AdminStartFailover(c)`, etc.

## Initial State

```tla
Init ==
    LET active == CHOOSE x \in Cluster : TRUE
    IN /\ clusterState = [c \in Cluster |->
                            IF c = active THEN "AIS" ELSE "S"]
       /\ writerMode = [c \in Cluster |-> [r \in RS |-> "INIT"]]
       /\ outDirEmpty = [c \in Cluster |-> TRUE]
       /\ hdfsAvailable = [c \in Cluster |-> TRUE]
       /\ antiFlapTimer = [c \in Cluster |-> 0]
       /\ replayState = [c \in Cluster |->
                            IF c = active THEN "NOT_INITIALIZED"
                            ELSE "SYNCED_RECOVERY"]
       /\ lastRoundInSync = [c \in Cluster |-> 0]
       /\ lastRoundProcessed = [c \in Cluster |-> 0]
       /\ failoverPending = [c \in Cluster |-> FALSE]
       /\ inProgressDirEmpty = [c \in Cluster |-> TRUE]
       /\ zkPeerConnected = [c \in Cluster |-> TRUE]
       /\ zkPeerSessionAlive = [c \in Cluster |-> TRUE]
       /\ zkLocalConnected = [c \in Cluster |-> TRUE]
```

The system starts with one cluster active and in sync (AIS) and the other in standby (S). The choice of which cluster is active is deterministic: `CHOOSE` picks an arbitrary but fixed element of `Cluster` as the initial active.

### Modeling Choices in Init

**Standby starts in SYNCED_RECOVERY, not SYNC:** The standby starts with `replayState = SYNCED_RECOVERY`, modeling the `recoveryListener` having already fired during startup. In the implementation, `NOT_INITIALIZED -> SYNCED_RECOVERY` is synchronous with S entry on the local `PathChildrenCache` event thread. The active starts `NOT_INITIALIZED` because the reader is dormant until the cluster first enters S after a failover.

**All writers in INIT:** All RS start in INIT mode, reflecting the pre-initialization state before the `ReplicationLogGroup` is created and modes are assigned based on HDFS availability and cluster state.

**All ZK connections alive:** The system starts with all ZK connections healthy. Failures are introduced non-deterministically by the `ZKPeerDisconnect`, `ZKPeerSessionExpiry`, and `ZKLocalDisconnect` actions.

**Anti-flapping timers at zero:** Timers start at 0 (gate open), reflecting a clean startup with no prior degradation history.

## Next-State Relation

In each step, exactly one cluster performs one actor-driven action. Actions are factored by actor:

```tla
Next ==
    \/ clk!Tick
    \/ \E c \in Cluster :
        \/ haGroupStore!PeerReactToATS(c)
        \/ haGroupStore!PeerReactToANIS(c)
        \/ haGroupStore!PeerReactToAbTS(c)
        \/ haGroupStore!AutoComplete(c)
        \/ reader!TriggerFailover(c)
        \/ haGroupStore!PeerReactToAIS(c)
        \/ haGroupStore!ANISHeartbeat(c)
        \/ haGroupStore!ANISToAIS(c)
        \/ haGroupStore!ANISTSToATS(c)
        \/ haGroupStore!ReactiveTransitionFail(c)
        \/ haGroupStore!PeerReactToOFFLINE(c)
        \/ haGroupStore!PeerRecoverFromOFFLINE(c)
        \/ admin!AdminStartFailover(c)
        \/ admin!AdminAbortFailover(c)
        \/ admin!AdminGoOffline(c)
        \/ admin!AdminForceRecover(c)
        \/ hdfs!HDFSDown(c)
        \/ hdfs!HDFSUp(c)
        \/ zk!ZKPeerDisconnect(c)
        \/ zk!ZKPeerReconnect(c)
        \/ zk!ZKPeerSessionExpiry(c)
        \/ zk!ZKPeerSessionRecover(c)
        \/ zk!ZKLocalDisconnect(c)
        \/ zk!ZKLocalReconnect(c)
        \/ reader!ReplayAdvance(c)
        \/ reader!ReplayRewind(c)
        \/ reader!ReplayBeginProcessing(c)
        \/ reader!ReplayFinishProcessing(c)
        \/ \E r \in RS :
            \/ writer!WriterInit(c, r)
            \/ writer!WriterInitToStoreFwd(c, r)
            \/ writer!WriterInitToStoreFwdFail(c, r)
            \/ writer!WriterSyncToSyncFwd(c, r)
            \/ writer!WriterStoreFwdToSyncFwd(c, r)
            \/ writer!WriterSyncFwdToSync(c, r)
            \/ writer!WriterToStoreFwd(c, r)
            \/ writer!WriterSyncFwdToStoreFwd(c, r)
            \/ writer!WriterToStoreFwdFail(c, r)
            \/ writer!WriterSyncFwdToStoreFwdFail(c, r)
            \/ rs!RSRestart(c, r)
            \/ rs!RSCrash(c, r)
            \/ rs!RSAbortOnLocalHDFSFailure(c, r)
```

### Action Categories

The 42 action schemas decompose into:

- **Timer:** `Tick` -- global, not per-cluster
- **ZK watcher (peer):** `PeerReactToATS`, `PeerReactToANIS`, `PeerReactToAbTS`, `PeerReactToAIS`, `PeerReactToOFFLINE`, `PeerRecoverFromOFFLINE` -- require `zkPeerConnected` and `zkPeerSessionAlive`
- **ZK watcher (local):** `AutoComplete`, `ANISHeartbeat`, `ANISToAIS`, `ANISTSToATS`, `TriggerFailover` -- require `zkLocalConnected`
- **Retry exhaustion:** `ReactiveTransitionFail` -- requires peer ZK connectivity (same as PeerReact*)
- **Direct ZK write:** `AdminStartFailover`, `AdminAbortFailover`, `AdminGoOffline`, `AdminForceRecover` -- not watcher-dependent
- **Environment:** `HDFSDown`, `HDFSUp`, `ZKPeer*`, `ZKLocal*` -- environment actions
- **Reader:** `ReplayAdvance`, `ReplayRewind`, `ReplayBeginProcessing`, `ReplayFinishProcessing` -- replay state machine
- **Writer (per-RS):** 10 writer mode transitions -- some require `zkLocalConnected`
- **RS lifecycle (per-RS):** `RSRestart`, `RSCrash`, `RSAbortOnLocalHDFSFailure`

## Specification Formulas

### Safety-Only Specification

```tla
SafetySpec == Init /\ [][Next]_vars
```

Initial state, followed by zero or more `Next` steps (or stuttering). No fairness -- used for fast safety-only model checking without temporal overhead. This is the specification used by the exhaustive and simulation safety configurations.

### Full Specification

```tla
Spec == Init /\ [][Next]_vars /\ Fairness
```

Safety conjoined with the complete fairness formula. Documents the full fairness design but has 43 temporal clauses -- too large for TLC's Buchi automaton construction. Used only in THEOREM declarations.

## Fairness

The fairness formula classifies every action in `Next` into one of four tiers. The guiding principle: any action whose guard depends on an environment variable that oscillates without fairness needs strong fairness (SF), because the adversary can cycle the environment variable once per lasso cycle to break weak fairness (WF)'s continuous-enablement requirement.

### Tier 1: WF on Protocol-Internal Steps

Guards depend only on protocol state; continuous enablement is guaranteed by protocol progress.

```tla
    /\ WF_vars(clk!Tick)
    /\ \A c \in Cluster :
        /\ WF_vars(haGroupStore!ANISHeartbeat(c))
        /\ WF_vars(reader!ReplayAdvance(c))
        /\ WF_vars(reader!ReplayRewind(c))
        /\ WF_vars(reader!ReplayBeginProcessing(c))
        /\ WF_vars(reader!ReplayFinishProcessing(c))
```

**Exception: ANISHeartbeat** keeps WF despite its `zkLocalConnected` guard because suppressing the heartbeat *helps* liveness (the anti-flap gate opens sooner). SF would be counterproductive -- it would force the heartbeat to fire, keeping the gate closed.

### Tier 2: WF on ZK Recovery (ZK Liveness Assumption)

```tla
        /\ WF_vars(zk!ZKPeerReconnect(c))
        /\ WF_vars(zk!ZKPeerSessionRecover(c))
        /\ WF_vars(zk!ZKLocalReconnect(c))
```

Encodes the ZK Liveness Assumption (ZLA): ZK sessions are eventually alive and connected. These recovery actions are the basis for SF on all actions guarded by `zkPeerConnected` or `zkLocalConnected`.

### Tier 3: SF on Actions Guarded by Environment Variables

Actions guarded by environment variables that oscillate without fairness (`zkPeerConnected`, `zkPeerSessionAlive`, `zkLocalConnected`, `hdfsAvailable`). Grouped by mutual exclusivity to keep TLC's temporal formula within its DNF size limit.

When at most one disjunct is ENABLED in any state, `SF(A1 \/ ... \/ An)` is equivalent to `SF(A1) /\ ... /\ SF(An)`, because the only disjunct that can fire is the one that is enabled. Mutual exclusivity is guaranteed by the single-valued nature of `clusterState` (per-cluster groups) and `writerMode` (per-RS groups).

```tla
        /\ SF_vars(haGroupStore!PeerReactToATS(c)
                   \/ haGroupStore!PeerReactToANIS(c)
                   \/ haGroupStore!PeerReactToAbTS(c)
                   \/ haGroupStore!PeerReactToAIS(c)
                   \/ haGroupStore!PeerReactToOFFLINE(c)
                   \/ haGroupStore!PeerRecoverFromOFFLINE(c))
        /\ SF_vars(haGroupStore!AutoComplete(c)
                   \/ haGroupStore!ANISToAIS(c)
                   \/ haGroupStore!ANISTSToATS(c)
                   \/ reader!TriggerFailover(c))
        /\ SF_vars(hdfs!HDFSUp(c))
        /\ \A r \in RS :
            /\ WF_vars(writer!WriterInit(c, r))
            /\ WF_vars(writer!WriterSyncToSyncFwd(c, r))
            /\ SF_vars(writer!WriterToStoreFwd(c, r)
                       \/ writer!WriterSyncFwdToStoreFwd(c, r)
                       \/ writer!WriterInitToStoreFwd(c, r))
            /\ SF_vars(writer!WriterStoreFwdToSyncFwd(c, r)
                       \/ writer!WriterSyncFwdToSync(c, r))
            /\ SF_vars(rs!RSAbortOnLocalHDFSFailure(c, r)
                       \/ rs!RSRestart(c, r))
```

### Tier 4: No Fairness

No fairness on non-deterministic environmental faults (`HDFSDown`, `RSCrash`, `ZKPeerDisconnect`, `ZKPeerSessionExpiry`, `ZKLocalDisconnect`, `ReactiveTransitionFail`), operator actions (`AdminStartFailover`, `AdminAbortFailover`, `AdminGoOffline`, `AdminForceRecover`), and CAS failures (`WriterToStoreFwdFail`, `WriterSyncFwdToStoreFwdFail`, `WriterInitToStoreFwdFail`). These are genuinely non-deterministic; imposing fairness would force unrealistic guarantees.

## Per-Property Liveness Specifications

Each per-property specification conjoins only the fairness clauses on the critical path for one liveness property, keeping the temporal formula small enough for TLC's Buchi automaton construction.

### SpecAC -- AbortCompletion

```tla
FairnessAC ==
    /\ WF_vars(clk!Tick)
    /\ \A c \in Cluster :
        /\ WF_vars(zk!ZKLocalReconnect(c))
        /\ SF_vars(haGroupStore!AutoComplete(c))

SpecAC == Init /\ [][Next]_vars /\ FairnessAC
```

Critical path: `AutoComplete` (SF, `zkLocalConnected` guard), `ZKLocalReconnect` (WF, re-enables `zkLocalConnected`), `Tick` (WF). 5 temporal clauses total.

### SpecFC -- FailoverCompletion

```tla
FairnessFC ==
    /\ WF_vars(clk!Tick)
    /\ \A c \in Cluster :
        /\ WF_vars(zk!ZKLocalReconnect(c))
        /\ WF_vars(reader!ReplayAdvance(c))
        /\ WF_vars(reader!ReplayRewind(c))
        /\ WF_vars(reader!ReplayBeginProcessing(c))
        /\ WF_vars(reader!ReplayFinishProcessing(c))
        /\ SF_vars(haGroupStore!AutoComplete(c)
                   \/ reader!TriggerFailover(c))
        /\ SF_vars(hdfs!HDFSUp(c))

SpecFC == Init /\ [][Next]_vars /\ FairnessFC
```

Critical path: `AutoComplete` + `TriggerFailover` (SF, grouped by `clusterState` exclusivity), `HDFSUp` (SF), `ZKLocalReconnect` (WF), replay machine including `ReplayRewind` (WF), `Tick` (WF). 15 temporal clauses total.

### SpecDR -- DegradationRecovery

```tla
FairnessDR ==
    /\ WF_vars(clk!Tick)
    /\ \A c \in Cluster :
        /\ WF_vars(zk!ZKLocalReconnect(c))
        /\ WF_vars(haGroupStore!ANISHeartbeat(c))
        /\ SF_vars(haGroupStore!ANISToAIS(c))
        /\ SF_vars(hdfs!HDFSUp(c))
        /\ \A r \in RS :
            /\ WF_vars(writer!WriterInit(c, r))
            /\ WF_vars(writer!WriterSyncToSyncFwd(c, r))
            /\ SF_vars(writer!WriterStoreFwdToSyncFwd(c, r)
                       \/ writer!WriterSyncFwdToSync(c, r))
            /\ SF_vars(rs!RSAbortOnLocalHDFSFailure(c, r)
                       \/ rs!RSRestart(c, r))

SpecDR == Init /\ [][Next]_vars /\ FairnessDR
```

Critical path: `ANISToAIS` (SF), `HDFSUp` (SF), `ZKLocalReconnect` (WF), `Tick` (WF), `ANISHeartbeat` (WF), per-RS writer recovery chain (SF) and lifecycle (SF), `WriterInit` and `WriterSyncToSyncFwd` (WF). 25 temporal clauses total with 2 RS.

## Liveness Properties

### FailoverCompletion

```tla
FailoverCompletion ==
    \A c \in Cluster :
        clusterState[c] \in FailoverCompletionAntecedentStates
        ~> clusterState[c] \in StableClusterStates
```

Standby-side and abort transient states eventually resolve to a stable state. Resolution paths:

- `STA -> AIS` (TriggerFailover) or `STA -> AbTS -> S` (abort)
- `AbTAIS -> AIS/ANIS`, `AbTANIS -> ANIS`, `AbTS -> S` (auto-completion)

**ATS and ANISTS are excluded** from this property. Their resolution depends on the peer completing failover (`PeerReactToAIS`/`PeerReactToANIS`) or on abort propagation (`PeerReactToAbTS`). Both require the peer to reach a specific state AND the ZK peer connection to be alive at the right moment. With no fairness on admin actions (the admin can abort every failover attempt) and no fairness on ZK disconnect (the scheduler can disconnect exactly when the peer is in AbTS), ATS can remain indefinitely. ATS does have a resolution path via the reconciliation fold in `ZKPeerReconnect`/`ZKPeerSessionRecover` (ATS -> AbTAIS -> AIS when peer is in S/DS at reconnect), but adding ATS here would require extending `FairnessFC` with the peer-reactive SF group.

### DegradationRecovery

```tla
DegradationRecovery ==
    \A c \in Cluster :
        (clusterState[c] = "ANIS" /\ hdfsAvailable[Peer(c)])
        ~> clusterState[c] \in NotANISClusterStates
```

ANIS with available peer HDFS eventually progresses out of ANIS. The recovery chain is: S&F -> S&FWD (`WriterStoreFwdToSyncFwd`) -> SYNC (`WriterSyncFwdToSync`, sets `outDirEmpty`) -> anti-flap timer expires (`Tick`) -> ANIS -> AIS (`ANISToAIS`). The cluster may also leave ANIS via failover (ANIS -> ANISTS), which satisfies the consequent.

### AbortCompletion

```tla
AbortCompletion ==
    \A c \in Cluster :
        clusterState[c] \in AbortCompletionAntecedentStates
        ~> clusterState[c] \in StableClusterStates
```

Every abort state eventually auto-completes to a stable state. Under WF on `AutoComplete`, each abort state deterministically resolves. Requires `zkLocalConnected` (`AutoComplete` guard).

## Type Invariant

```tla
TypeOK ==
    /\ clusterState \in [Cluster -> HAGroupState]
    /\ writerMode \in [Cluster -> [RS -> WriterMode]]
    /\ outDirEmpty \in [Cluster -> BOOLEAN]
    /\ hdfsAvailable \in [Cluster -> BOOLEAN]
    /\ antiFlapTimer \in [Cluster -> 0..WaitTimeForSync]
    /\ replayState \in [Cluster -> ReplayStateSet]
    /\ lastRoundInSync \in [Cluster -> Nat]
    /\ lastRoundProcessed \in [Cluster -> Nat]
    /\ failoverPending \in [Cluster -> BOOLEAN]
    /\ inProgressDirEmpty \in [Cluster -> BOOLEAN]
    /\ zkPeerConnected \in [Cluster -> BOOLEAN]
    /\ zkPeerSessionAlive \in [Cluster -> BOOLEAN]
    /\ zkLocalConnected \in [Cluster -> BOOLEAN]
```

All specification variables have valid types. TLC checks this invariant in every reachable state.

## Safety Invariants

### ZKSessionConsistency

```tla
ZKSessionConsistency ==
    \A c \in Cluster :
        zkPeerSessionAlive[c] = FALSE => zkPeerConnected[c] = FALSE
```

ZK session/connection structural consistency: if the peer ZK session is expired, the peer connection must also be dead. Session expiry implies disconnection -- the `ZKPeerSessionExpiry` action sets both `zkPeerSessionAlive` and `zkPeerConnected` to FALSE. `ZKPeerReconnect` requires `zkPeerSessionAlive = TRUE`, so a reconnect cannot happen without a live session. This invariant verifies that the ZK actions correctly maintain the session/connection relationship across all reachable states.

### MutualExclusion

```tla
MutualExclusion ==
    ~(\E c1, c2 \in Cluster :
        /\ c1 # c2
        /\ RoleOf(clusterState[c1]) = "ACTIVE"
        /\ RoleOf(clusterState[c2]) = "ACTIVE")
```

**The primary safety property of the failover protocol.** Two clusters never both in the ACTIVE role simultaneously. The ACTIVE role includes: AIS, ANIS, AbTAIS, AbTANIS, AWOP, ANISWOP. Transitional states ATS and ANISTS map to the ACTIVE_TO_STANDBY role (not ACTIVE), which is the mechanism by which safety is maintained during the non-atomic failover window -- `isMutationBlocked() = true` for ACTIVE_TO_STANDBY.

Source: Architecture safety argument; `ClusterRoleRecord.java` L84 -- ACTIVE_TO_STANDBY has `isMutationBlocked() = true`.

### AbortSafety

```tla
AbortSafety ==
    \A c \in Cluster :
        clusterState[c] = "AbTAIS" =>
            clusterState[Peer(c)] \in {"AbTS", "S", "DS", "OFFLINE"}
```

If a cluster is in AbTAIS, the peer must be in AbTS, S, DS, or OFFLINE. AbTAIS is reached via three paths:

1. **Abort path:** `PeerReactToAbTS` (peer = AbTS). The peer can auto-complete AbTS -> S before the local AbTAIS auto-completes.
2. **Reconciliation path:** `ZKPeerReconnect`/`ZKPeerSessionRecover` with local = ATS and peer in {S, DS}. DS is reachable when the peer degraded (S -> DS via `PeerReactToANIS`) before the failover partition.
3. **OFFLINE path:** When the peer transitions to OFFLINE (via `AdminGoOffline`) while the local cluster is in AbTAIS, safety is preserved because OFFLINE is a non-active state.

All four peer states (AbTS, S, DS, OFFLINE) map to STANDBY role, so MutualExclusion is preserved in all cases.

### AISImpliesInSync

```tla
AISImpliesInSync ==
    \A c \in Cluster :
        clusterState[c] = "AIS" =>
            /\ outDirEmpty[c]
            /\ \A r \in RS : writerMode[c][r] \in {"INIT", "SYNC", "DEAD"}
```

Whenever a cluster is in AIS, the OUT directory must be empty and all RS must be in SYNC, INIT, or DEAD. DEAD is allowed because an RS can crash while the cluster is AIS -- `RSCrash` sets `writerMode` to DEAD but does not change `clusterState`.

### WriterClusterConsistency

```tla
WriterClusterConsistency ==
    \A c \in Cluster :
        (\E r \in RS : writerMode[c][r] \in {"STORE_AND_FWD", "SYNC_AND_FWD"}) =>
            clusterState[c] \in {"ANIS", "ANISTS", "ATS", "ANISWOP",
                                  "AbTANIS", "AbTAIS", "AWOP"}
```

Degraded writer modes (S&F, SYNC_AND_FWD) can only appear on active clusters that are NOT in AIS, on the ANISTS/ATS transitional states, or on abort states where HDFS failure can degrade writers. AIS is excluded by the AIS->ANIS coupling. ATS is included because the ANIS failover path enters ATS via `ANISTSToATS` which does NOT snap writer modes. Standby states are excluded because writer modes are reset to INIT on ATS -> S entry.

## Action Constraints

### TransitionValid

```tla
TransitionValid ==
    \A c \in Cluster :
        clusterState'[c] # clusterState[c] =>
            <<clusterState[c], clusterState'[c]>> \in AllowedTransitions
```

Every state change follows the `AllowedTransitions` table from [Types.md](Types.md). Source: `HAGroupStoreRecord.java` L99-123, `isTransitionAllowed()` L130.

### WriterTransitionValid

`AllowedWriterTransitions` is defined in [Types.md](Types.md).

```tla
WriterTransitionValid ==
    \A c \in Cluster :
        \A r \in RS :
            writerMode'[c][r] # writerMode[c][r] =>
                <<writerMode[c][r], writerMode'[c][r]>> \in AllowedWriterTransitions
```

The `X -> INIT` transitions (SYNC, STORE_AND_FWD, SYNC_AND_FWD) model the replication subsystem restart on ATS -> S (standby entry). These are lifecycle resets, not `ReplicationLogGroup` mode CAS transitions.

### AIStoATSPrecondition

```tla
AIStoATSPrecondition ==
    \A c \in Cluster :
        clusterState[c] = "AIS" /\ clusterState'[c] = "ATS"
        => outDirEmpty[c] /\ \A r \in RS : writerMode[c][r] \in {"SYNC", "DEAD"}
```

Failover can only begin from AIS when the OUT directory is empty and all live RS are in SYNC mode. DEAD RSes are allowed -- an RS can crash while the cluster is AIS without changing the HA group state.

### AntiFlapGate

```tla
AntiFlapGate ==
    \A c \in Cluster :
        clusterState[c] = "ANIS" /\ clusterState'[c] = "AIS"
        => AntiFlapGateOpen(antiFlapTimer[c])
```

ANIS -> AIS never fires while the countdown timer is still running.

### ANISTStoATSPrecondition

```tla
ANISTStoATSPrecondition ==
    \A c \in Cluster :
        clusterState[c] = "ANISTS" /\ clusterState'[c] = "ATS"
        => /\ outDirEmpty[c]
           /\ AntiFlapGateOpen(antiFlapTimer[c])
```

ANISTS -> ATS requires empty OUT directory and open anti-flapping gate.

### FailoverTriggerCorrectness

```tla
FailoverTriggerCorrectness ==
    \A c \in Cluster :
        clusterState[c] = "STA" /\ clusterState'[c] = "AIS"
        => STAtoAISTriggerReplayGuards(c)
```

STA -> AIS requires replay-completeness conditions. Cross-checks the `TriggerFailover` action's guards. `hdfsAvailable` is excluded because it is an environmental/liveness guard, not a replay-completeness condition.

### NoDataLoss

```tla
NoDataLoss ==
    \A c \in Cluster :
        clusterState[c] = "STA" /\ clusterState'[c] = "AIS"
        => STAtoAISTriggerReplayGuards(c)
```

**Zero RPO property.** When the standby completes STA -> AIS, replay must have been in SYNC (no pending SYNCED_RECOVERY rewind), the in-progress directory must be empty, and the failover must have been properly initiated.

### ReplayRewindCorrectness

```tla
ReplayRewindCorrectness ==
    \A c \in Cluster :
        replayState[c] = "SYNCED_RECOVERY" /\ replayState'[c] = "SYNC"
        => lastRoundProcessed'[c] = lastRoundInSync'[c]
```

The SYNCED_RECOVERY -> SYNC transition equalizes the replay counters. Together with `NoDataLoss`, this guarantees zero RPO: the rewind closes the counter gap, and `TriggerFailover` (which requires `replayState = SYNC`) cannot fire until the rewind completes.

### ReplayTransitionValid

`AllowedReplayTransitions` is defined in [Types.md](Types.md).

```tla
ReplayTransitionValid ==
    \A c \in Cluster :
        replayState'[c] # replayState[c] =>
            <<replayState[c], replayState'[c]>> \in AllowedReplayTransitions
```

Every replay state change follows the allowed transitions. Source: `ReplicationLogDiscoveryReplay.java` L131-206 (listeners), L323-333 (CAS), L336-351 (replay loop).

## State Constraint

```tla
ReplayCounterBound ==
    \A c \in Cluster : lastRoundProcessed[c] <= 3
```

Bounds replay counters for exhaustive search tractability. The abstract counter values only matter relationally (`lastRoundProcessed >= lastRoundInSync`), so small bounds suffice.

## Symmetry

```tla
Symmetry == Permutations(RS)
```

RS identifiers are interchangeable (all start in INIT, identical action sets). Cluster identifiers remain asymmetric (AIS vs S at Init).

## Theorems

```tla
THEOREM Spec => []TypeOK
THEOREM Spec => []MutualExclusion
THEOREM Spec => []AbortSafety
THEOREM Spec => []AISImpliesInSync
THEOREM Spec => []WriterClusterConsistency
THEOREM Spec => []ZKSessionConsistency
THEOREM Spec => [][ANISTStoATSPrecondition]_vars
THEOREM Spec => [][FailoverTriggerCorrectness]_vars
THEOREM Spec => [][NoDataLoss]_vars
THEOREM Spec => [][ReplayRewindCorrectness]_vars
THEOREM SpecFC => FailoverCompletion
THEOREM SpecDR => DegradationRecovery
THEOREM SpecAC => AbortCompletion
```

These theorem declarations document the intended proof obligations. TLC checks the safety theorems via the exhaustive and simulation configurations; the liveness theorems are checked via the per-property simulation configurations.
