# HAGroupStore -- Peer-Reactive Transitions and Auto-Completion

**Source:** [`HAGroupStore.tla`](../HAGroupStore.tla)

## Overview

`HAGroupStore` models the peer-reactive transitions and auto-completion actions of the Phoenix Consistent Failover protocol. These actions correspond to the `FailoverManagementListener` (`HAGroupStoreManager.java` L633-706), which reacts to peer ZK state changes via `PathChildrenCache` watchers, and the local auto-completion resolvers from `createLocalStateTransitions()` (L140-150).

This is the largest sub-module by action count, containing 11 action schemas that handle:

- **Peer-reactive transitions:** Detecting the peer's state change via ZK watcher and transitioning accordingly
- **Auto-completion:** Returning from abort states to stable states via local ZK writes
- **S&F heartbeat:** Refreshing the anti-flapping timer during STORE_AND_FWD degradation
- **Recovery transitions:** ANIS -> AIS when all RS recover, ANISTS -> ATS when OUT drains
- **Peer OFFLINE detection:** Reacting to peer entering or leaving OFFLINE state (gated on `UseOfflinePeerDetection`)
- **Retry exhaustion:** Modeling the case where the `FailoverManagementListener`'s 2-retry limit is exceeded

### ZK Watcher Delivery Dependency

All `PeerReact*` actions depend on the peer ZK connection and session being alive, guarded by `zkPeerConnected[c]` and `zkPeerSessionAlive[c]`. `AutoComplete` actions depend on the local ZK connection, guarded by `zkLocalConnected[c]`. Without these connections, watcher notifications cannot be delivered, and the corresponding transitions are suppressed.

This models a critical implementation detail: the `FailoverManagementListener` is invoked by the `PathChildrenCache` watcher chain, not by polling. If the watcher connection is down, no notifications arrive, and the transition never fires. There is no polling fallback in the implementation.

### Notification Chains

**Peer-reactive transitions:**
```
Peer ZK znode change
  -> Curator peerPathChildrenCache
    -> HAGroupStoreClient.handleStateChange() [L1088-1110]
      -> notifySubscribers() [L1119-1151]
        -> FailoverManagementListener.onStateChange() [L653-705]
          -> setHAGroupStatusIfNeeded() (2-retry limit)
```

**Auto-completion transitions:**
```
Local ZK znode change
  -> Curator pathChildrenCache (local)
    -> HAGroupStoreClient.handleStateChange()
      -> notifySubscribers()
        -> FailoverManagementListener.onStateChange()
```

### Listener Effect Folding

The `recoveryListener` (L147-157) and `degradedListener` (L136-145) from `ReplicationLogDiscoveryReplay` fire synchronously on the local `PathChildrenCache` event thread during state entry. Their effects are folded atomically into the S-entry and DS-entry actions:

- **S entry** (`PeerReactToANIS` ATS->S, `PeerReactToAIS` ATS->S / DS->S, `AutoComplete` AbTS->S): sets `replayState = SYNCED_RECOVERY`
- **DS entry** (`PeerReactToANIS` S->DS): sets `replayState = DEGRADED`

This folding is sound because the listener fires deterministically and synchronously on every state entry -- there is no observable intermediate state between the cluster state change and the replay state change.

The ATS->S side-effect bundle (live writers reset to INIT preserving DEAD, `outDirEmpty` cleared, `replayState` set to SYNCED_RECOVERY) is shared by `PeerReactToAIS` (ATS->S) and `PeerReactToANIS` (ATS->S) and extracted into a module-local operator:

```tla
ResetToStandbyEntry(c) ==
    /\ writerMode' = [writerMode EXCEPT ![c] =
            [rs \in RS |-> IF writerMode[c][rs] = "DEAD"
                           THEN "DEAD"
                           ELSE "INIT"]]
    /\ outDirEmpty' = [outDirEmpty EXCEPT ![c] = TRUE]
    /\ replayState' = [replayState EXCEPT ![c] = "SYNCED_RECOVERY"]
```

`ResetToStandbyEntry` is intentionally NOT applied to `AdminForceRecover` (which resets all writers to INIT with no DEAD-preservation) or to `AutoComplete` AbTS->S (which only sets `replayState`; no writer/OUT reset is needed because AbTS was never active).

### Retry Exhaustion

The `FailoverManagementListener` retries each reactive transition exactly 2 times (`HAGroupStoreManager.java` L653-704). After exhaustion, the method returns silently. This is modeled by the `ReactiveTransitionFail(c)` action, which non-deterministically "consumes" a pending peer-reactive transition without updating `clusterState`.

The retry-exhaustion action shadows every `PeerReact*` action's enabling condition. To keep the two from drifting apart, the peer-state/local-state disjunction is factored out into a module-local predicate `PeerReactWouldFire(c)`:

```tla
PeerReactWouldFire(c) ==
    \/ /\ clusterState[Peer(c)] = "ATS"
       /\ clusterState[c] \in {"S", "DS"}
    \/ /\ clusterState[Peer(c)] = "ANIS"
       /\ clusterState[c] \in {"S", "ATS"}
    \/ /\ clusterState[Peer(c)] = "AbTS"
       /\ clusterState[c] = "ATS"
    \/ /\ clusterState[Peer(c)] = "AIS"
       /\ clusterState[c] \in {"ATS", "DS"}
    \/ /\ UseOfflinePeerDetection = TRUE
       /\ clusterState[Peer(c)] = "OFFLINE"
       /\ clusterState[c] \in {"AIS", "ANIS"}
    \/ /\ UseOfflinePeerDetection = TRUE
       /\ clusterState[Peer(c)] # "OFFLINE"
       /\ clusterState[c] \in {"AWOP", "ANISWOP"}
```

`ReactiveTransitionFail(c)` combines `PeerZKHealthy(c)` (the ZK connectivity guard, defined in [`SpecState.tla`](../SpecState.tla)) with `PeerReactWouldFire(c)`. The `PeerReact*` action bodies keep their inline peer-state/local-state guards so each action's enabling condition remains readable at the definition site.

## Implementation Traceability

| TLA+ Action | Java Source |
|---|---|
| `PeerReactToATS(c)` | `createPeerStateTransitions()` L109 |
| `PeerReactToANIS(c)` | `createPeerStateTransitions()` L123, L126 |
| `PeerReactToAbTS(c)` | `createPeerStateTransitions()` L132 |
| `PeerReactToAIS(c)` | `createPeerStateTransitions()` L112-120 |
| `AutoComplete(c)` | `createLocalStateTransitions()` L144, L145, L147 |
| `ANISTSToATS(c)` | `HAGroupStoreManager.setHAGroupStatusToSync()` L341-355 |
| `PeerReactToOFFLINE(c)` | peer OFFLINE detection; no impl trigger yet; gated on `UseOfflinePeerDetection` |
| `PeerRecoverFromOFFLINE(c)` | peer OFFLINE recovery detection; no impl trigger yet; gated on `UseOfflinePeerDetection` |
| `ReactiveTransitionFail(c)` | `FailoverManagementListener.onStateChange()` L653-704 (2 retries exhausted) |

Failover completion (STA -> AIS) is modeled in [Reader.md](Reader.md) (`TriggerFailover` action), not in this module.

```tla
EXTENDS SpecState, Types
```

## PeerReactToATS -- Standby Detects Peer ATS

When the standby detects its peer has entered ATS (ACTIVE_IN_SYNC_TO_STANDBY), it begins the failover process by transitioning to STA (STANDBY_TO_ACTIVE). This fires from either S or DS -- the DS case supports the ANIS failover path where the standby is in DEGRADED_STANDBY when failover proceeds.

**ZK watcher dependency:** Delivered via `peerPathChildrenCache`. Guarded on `zkPeerConnected[c]` and `zkPeerSessionAlive[c]`. If the peer ZK session expires or the notification is lost, the standby never learns of the failover. The active cluster remains in ATS with mutations blocked indefinitely. There is no polling fallback.

**failoverPending side-effect:** Also sets `failoverPending[c] = TRUE`, modeling the `triggerFailoverListener` (`ReplicationLogDiscoveryReplay.java` L159-171) which fires on LOCAL STANDBY_TO_ACTIVE entry. This is folded into `PeerReactToATS` because the listener fires deterministically on every STA entry and `PeerReactToATS` is the sole producer of STA.

Source: `createPeerStateTransitions()` L109 -- resolver is unconditional: `currentLocal -> STANDBY_TO_ACTIVE`.

```tla
PeerReactToATS(c) ==
    /\ zkPeerConnected[c] = TRUE
    /\ zkPeerSessionAlive[c] = TRUE
    /\ clusterState[Peer(c)] = "ATS"
    /\ clusterState[c] \in {"S", "DS"}
    /\ clusterState' = [clusterState EXCEPT ![c] = "STA"]
    /\ failoverPending' = [failoverPending EXCEPT ![c] = TRUE]
    /\ UNCHANGED <<writerMode, outDirEmpty, hdfsAvailable, antiFlapTimer,
                   replayState, lastRoundInSync, lastRoundProcessed,
                   inProgressDirEmpty,
                   zkPeerConnected, zkPeerSessionAlive, zkLocalConnected>>
```

## PeerReactToANIS -- Peer Enters ANIS

Two reactive transitions triggered by the peer entering ANIS (ACTIVE_NOT_IN_SYNC):

1. **Local S -> DS:** Standby degrades because the peer's replication is degraded. Atomically sets `replayState = DEGRADED` (degradedListener fold). Source: L126.
2. **Local ATS -> S:** Old active (in failover) completes transition to standby when peer is ANIS. Atomically sets `replayState = SYNCED_RECOVERY` (recoveryListener fold) and resets live writer modes to INIT. Source: L123.

**ZK watcher dependency:** If lost: (1) standby stays in S when it should be DS -- consistency point tracking is incorrect; (2) old active stays in ATS with mutations blocked.

**Writer lifecycle reset (ATS -> S):** When the old active enters standby, the `FailoverManagementListener` triggers a replication subsystem restart on each live RS. Live writer modes reset to INIT (the `ReplicationLogGroup` is destroyed and will be recreated when the cluster next becomes active). The OUT directory is cleared. DEAD writers are preserved: a crashed RS (JVM dead) cannot process the state change notification; the process supervisor restart (`RSRestart` in [RS.md](RS.md)) handles DEAD -> INIT independently.

```tla
PeerReactToANIS(c) ==
    /\ zkPeerConnected[c] = TRUE
    /\ zkPeerSessionAlive[c] = TRUE
    /\ clusterState[Peer(c)] = "ANIS"
    /\ \/ /\ clusterState[c] = "S"
          /\ clusterState' = [clusterState EXCEPT ![c] = "DS"]
          /\ replayState' = [replayState EXCEPT ![c] = "DEGRADED"]
          /\ UNCHANGED <<writerMode, outDirEmpty, hdfsAvailable, antiFlapTimer,
                         lastRoundInSync, lastRoundProcessed,
                         failoverPending, inProgressDirEmpty,
                         zkPeerConnected, zkPeerSessionAlive, zkLocalConnected>>
       \/ /\ clusterState[c] = "ATS"
          /\ clusterState' = [clusterState EXCEPT ![c] = "S"]
          /\ writerMode' = [writerMode EXCEPT ![c] =
                  [rs \in RS |-> IF writerMode[c][rs] = "DEAD"
                                  THEN "DEAD"
                                  ELSE "INIT"]]
          /\ outDirEmpty' = [outDirEmpty EXCEPT ![c] = TRUE]
          /\ replayState' = [replayState EXCEPT ![c] = "SYNCED_RECOVERY"]
          /\ UNCHANGED <<hdfsAvailable, antiFlapTimer,
                         lastRoundInSync, lastRoundProcessed,
                         failoverPending, inProgressDirEmpty,
                         zkPeerConnected, zkPeerSessionAlive, zkLocalConnected>>
```

## PeerReactToAbTS -- Active Detects Peer AbTS

When the active cluster (in ATS during failover) detects its peer has entered AbTS (abort initiated from the standby side), it transitions to AbTAIS (ABORT_TO_ACTIVE_IN_SYNC). This is the mechanism by which abort propagates from the standby to the active side.

**ZK watcher dependency:** If lost, the active stays in ATS with mutations blocked; abort does not propagate. No polling fallback.

Source: `createPeerStateTransitions()` L132.

```tla
PeerReactToAbTS(c) ==
    /\ zkPeerConnected[c] = TRUE
    /\ zkPeerSessionAlive[c] = TRUE
    /\ clusterState[Peer(c)] = "AbTS"
    /\ clusterState[c] = "ATS"
    /\ clusterState' = [clusterState EXCEPT ![c] = "AbTAIS"]
    /\ UNCHANGED <<writerMode, outDirEmpty, hdfsAvailable, antiFlapTimer,
                   replayState, lastRoundInSync, lastRoundProcessed,
                   failoverPending, inProgressDirEmpty,
                   zkPeerConnected, zkPeerSessionAlive, zkLocalConnected>>
```

## AutoComplete -- Local Auto-Completion Transitions

These transitions fire automatically once the cluster enters the corresponding abort state. They return the cluster to its pre-failover state. Despite being "local" (no peer trigger), these transitions are driven by the local `pathChildrenCache` watcher chain, not an in-process event bus. Guarded on `zkLocalConnected[c]`.

Three sub-cases:

**AbTS -> S:** Returns the standby to its pre-failover state. Atomically sets `replayState = SYNCED_RECOVERY` (recoveryListener fold). Source: L144.

**AbTAIS -> AIS or ANIS:** Conditional -- completes to AIS if all writers are clean (INIT or SYNC) and OUT dir is empty, otherwise completes to ANIS. This prevents AIS from coexisting with degraded writers when HDFS fails during the abort window. Source: L145.

**AbTANIS -> ANIS:** Returns to ANIS. Resets the anti-flapping timer to `StartAntiFlapWait`, keeping the gate closed so the cluster must wait before attempting ANIS -> AIS recovery. Source: L147.

```tla
AutoComplete(c) ==
    /\ zkLocalConnected[c] = TRUE
    /\ \/ /\ clusterState[c] = "AbTS"
          /\ clusterState' = [clusterState EXCEPT ![c] = "S"]
          /\ replayState' = [replayState EXCEPT ![c] = "SYNCED_RECOVERY"]
          /\ UNCHANGED <<writerMode, outDirEmpty, hdfsAvailable, antiFlapTimer,
                         lastRoundInSync, lastRoundProcessed,
                         failoverPending, inProgressDirEmpty,
                         zkPeerConnected, zkPeerSessionAlive, zkLocalConnected>>
       \/ /\ clusterState[c] = "AbTAIS"
          /\ clusterState' = [clusterState EXCEPT ![c] =
                 IF outDirEmpty[c] /\ \A rs \in RS : writerMode[c][rs] \in {"INIT", "SYNC"}
                 THEN "AIS"
                 ELSE "ANIS"]
          /\ UNCHANGED <<writerMode, outDirEmpty, hdfsAvailable, antiFlapTimer,
                         replayState, lastRoundInSync, lastRoundProcessed,
                         failoverPending, inProgressDirEmpty,
                         zkPeerConnected, zkPeerSessionAlive, zkLocalConnected>>
       \/ /\ clusterState[c] = "AbTANIS"
          /\ clusterState' = [clusterState EXCEPT ![c] = "ANIS"]
          /\ antiFlapTimer' = [antiFlapTimer EXCEPT ![c] = StartAntiFlapWait]
          /\ UNCHANGED <<writerMode, outDirEmpty, hdfsAvailable,
                         replayState, lastRoundInSync, lastRoundProcessed,
                         failoverPending, inProgressDirEmpty,
                         zkPeerConnected, zkPeerSessionAlive, zkLocalConnected>>
```

## PeerReactToAIS -- Peer Enters AIS

Two reactive transitions triggered by the peer entering AIS (ACTIVE_IN_SYNC):

1. **Local ATS -> S:** Old active completes failover to standby when the peer (the new active) enters AIS. Atomically sets `replayState = SYNCED_RECOVERY` (recoveryListener fold). This is the critical transition that resolves the non-atomic failover window.
2. **Local DS -> S:** Standby recovers from degraded when peer returns to AIS. Atomically sets `replayState = SYNCED_RECOVERY` (recoveryListener fold).

**Writer lifecycle reset (ATS -> S):** Same as `PeerReactToANIS` ATS -> S. Live writer modes reset to INIT, OUT directory cleared. DEAD writers preserved for `RSRestart`. This is critical for the ANIS failover path where SYNC_AND_FWD or STORE_AND_FWD writers may persist through ANISTS -> ATS (`ANISTSToATS` does not snap writer modes).

**ZK watcher dependency:** This is the critical transition that resolves the non-atomic failover window. If lost: old active stays in ATS with mutations blocked indefinitely (the (ATS, AIS) state persists). Safety holds (ATS maps to ACTIVE_TO_STANDBY, `isMutationBlocked() = true`) but liveness requires eventual watcher delivery. Curator `PathChildrenCache` re-queries on reconnect, providing eventual delivery if the ZK session survives.

Source: `createPeerStateTransitions()` L112-120 -- conditional resolver for peer ACTIVE_IN_SYNC.

```tla
PeerReactToAIS(c) ==
    /\ zkPeerConnected[c] = TRUE
    /\ zkPeerSessionAlive[c] = TRUE
    /\ clusterState[Peer(c)] = "AIS"
    /\ \/ /\ clusterState[c] = "ATS"
          /\ clusterState' = [clusterState EXCEPT ![c] = "S"]
          /\ writerMode' = [writerMode EXCEPT ![c] =
                  [rs \in RS |-> IF writerMode[c][rs] = "DEAD"
                                  THEN "DEAD"
                                  ELSE "INIT"]]
          /\ outDirEmpty' = [outDirEmpty EXCEPT ![c] = TRUE]
          /\ replayState' = [replayState EXCEPT ![c] = "SYNCED_RECOVERY"]
          /\ UNCHANGED <<hdfsAvailable, antiFlapTimer,
                         lastRoundInSync, lastRoundProcessed,
                         failoverPending, inProgressDirEmpty,
                         zkPeerConnected, zkPeerSessionAlive, zkLocalConnected>>
       \/ /\ clusterState[c] = "DS"
          /\ clusterState' = [clusterState EXCEPT ![c] = "S"]
          /\ replayState' = [replayState EXCEPT ![c] = "SYNCED_RECOVERY"]
          /\ UNCHANGED <<writerMode, outDirEmpty, hdfsAvailable, antiFlapTimer,
                         lastRoundInSync, lastRoundProcessed,
                         failoverPending, inProgressDirEmpty,
                         zkPeerConnected, zkPeerSessionAlive, zkLocalConnected>>
```

## ANISHeartbeat -- S&F Heartbeat Timer Reset

The S&F heartbeat runs while at least one RS is in STORE_AND_FWD mode. It periodically re-writes ANIS to the ZK znode, refreshing mtime. In the countdown timer model, this resets the timer to `StartAntiFlapWait`, keeping the anti-flapping gate closed.

The heartbeat stops when the last RS exits STORE_AND_FWD (enters SYNC_AND_FWD). At that point the timer begins counting down via `Tick` (in [Clock.md](Clock.md)), and the gate opens when it reaches 0.

**Fairness classification:** WF despite its `zkLocalConnected` guard, because suppressing the heartbeat *helps* liveness (the anti-flap gate opens sooner). SF would be counterproductive -- it would force the heartbeat to fire, keeping the gate closed longer.

Source: `StoreAndForwardModeImpl.startHAGroupStoreUpdateTask()` L71-87; `HAGroupStoreRecord.java` L101 (ANIS self-transition).

```tla
ANISHeartbeat(c) ==
    /\ zkLocalConnected[c] = TRUE
    /\ clusterState[c] = "ANIS"
    /\ \E rs \in RS : writerMode[c][rs] = "STORE_AND_FWD"
    /\ antiFlapTimer' = [antiFlapTimer EXCEPT ![c] = StartAntiFlapWait]
    /\ UNCHANGED <<clusterState, writerMode, outDirEmpty, hdfsAvailable,
                   replayState, lastRoundInSync, lastRoundProcessed,
                   failoverPending, inProgressDirEmpty,
                   zkPeerConnected, zkPeerSessionAlive, zkLocalConnected>>
```

## ANISToAIS -- Recovery from ANIS

When all RS on the cluster are in SYNC or SYNC_AND_FWD, the OUT directory is empty, and the anti-flapping gate has opened (countdown timer reached 0), the cluster recovers from ANIS to AIS.

The writer guard includes SYNC_AND_FWD (not just SYNC) because the anti-flapping gate ensures all RS have exited S&F before this action fires. Any remaining SYNC_AND_FWD RS are atomically transitioned to SYNC, modeling the ACTIVE_IN_SYNC ZK event at `ReplicationLogDiscoveryForwarder.init()` L113-123.

The `AISImpliesInSync` invariant in [ConsistentFailover.md](ConsistentFailover.md) verifies that AIS is only reached with all RS in SYNC or INIT.

Source: `setHAGroupStatusToSync()` L341-355, after forwarder drain.

```tla
ANISToAIS(c) ==
    /\ zkLocalConnected[c] = TRUE
    /\ clusterState[c] = "ANIS"
    /\ AntiFlapGateOpen(antiFlapTimer[c])
    /\ \A rs \in RS : writerMode[c][rs] \in {"SYNC", "SYNC_AND_FWD"}
    /\ outDirEmpty[c]
    /\ clusterState' = [clusterState EXCEPT ![c] = "AIS"]
    /\ writerMode' = [writerMode EXCEPT ![c] =
            [rs \in RS |-> IF writerMode[c][rs] = "SYNC_AND_FWD"
                           THEN "SYNC"
                           ELSE writerMode[c][rs]]]
    /\ UNCHANGED <<outDirEmpty, hdfsAvailable, antiFlapTimer,
                   replayState, lastRoundInSync, lastRoundProcessed,
                   failoverPending, inProgressDirEmpty,
                   zkPeerConnected, zkPeerSessionAlive, zkLocalConnected>>
```

## ANISTSToATS -- Drain Completion

When the forwarder has drained the OUT directory and the anti-flapping gate has opened, the cluster advances from ANISTS to ATS, joining the normal AIS failover path. The standby reacts to ATS (not ANISTS), so this transition is the bridge that lets the ANIS failover path converge with the AIS failover path.

**Writer modes are NOT snapped here.** In the implementation, `setHAGroupStatusToSync()` only writes the cluster-level ZK znode (ANISTS -> ATS); it does not modify per-RS writer modes. SYNC_AND_FWD writers may persist into ATS. They are cleaned up when the cluster transitions ATS -> S (replication subsystem restart on standby entry -- see `PeerReactToAIS`, `PeerReactToANIS`).

**Anti-flapping gate:** Confirmed by implementation -- `validateTransitionAndGetWaitTime()` L1035-1036 applies the same `waitTimeForSyncModeInMs` to ANISTS -> ATS as to ANIS -> AIS. The forwarder handles the wait via `syncUpdateTS` deferral (`processNoMoreRoundsLeft()` L169-172).

Source: `HAGroupStoreManager.setHAGroupStatusToSync()` L341-355; `HAGroupStoreClient.validateTransitionAndGetWaitTime()` L1027-1046.

```tla
ANISTSToATS(c) ==
    /\ zkLocalConnected[c] = TRUE
    /\ clusterState[c] = "ANISTS"
    /\ AntiFlapGateOpen(antiFlapTimer[c])
    /\ outDirEmpty[c]
    /\ clusterState' = [clusterState EXCEPT ![c] = "ATS"]
    /\ UNCHANGED <<writerMode, outDirEmpty, hdfsAvailable, antiFlapTimer,
                   replayState, lastRoundInSync, lastRoundProcessed,
                   failoverPending, inProgressDirEmpty,
                   zkPeerConnected, zkPeerSessionAlive, zkLocalConnected>>
```

## PeerReactToOFFLINE -- Active Detects Peer OFFLINE

Gated on `UseOfflinePeerDetection` (Iteration 18, proactive modeling).

When the active cluster detects its peer has entered OFFLINE, it transitions to AWOP or ANISWOP depending on its current state:
- AIS -> AWOP (peer went offline while active is in sync)
- ANIS -> ANISWOP (peer went offline while active is not in sync)

Both AWOP and ANISWOP map to `ClusterRole.ACTIVE` via `getClusterRole()` (`isMutationBlocked() = false`), so the active cluster continues serving mutations while its peer is offline.

No writer or timer side effects: the transition is purely a cluster-state annotation recording the peer's unavailability.

**ZK watcher dependency:** Delivered via `peerPathChildrenCache`. Guarded on `zkPeerConnected[c]` and `zkPeerSessionAlive[c]`.

**NOTE:** This models intended protocol behavior. No `FailoverManagementListener` entry for peer OFFLINE currently exists in the implementation (`createPeerStateTransitions()` has no OFFLINE entry). The TLA+ model verifies the design ahead of implementation.

Source: (proactive) AIS->AWOP from `allowedTransitions` L103; ANIS->ANISWOP from `allowedTransitions` L101.

```tla
PeerReactToOFFLINE(c) ==
    /\ UseOfflinePeerDetection = TRUE
    /\ zkPeerConnected[c] = TRUE
    /\ zkPeerSessionAlive[c] = TRUE
    /\ clusterState[Peer(c)] = "OFFLINE"
    /\ \/ /\ clusterState[c] = "AIS"
          /\ clusterState' = [clusterState EXCEPT ![c] = "AWOP"]
       \/ /\ clusterState[c] = "ANIS"
          /\ clusterState' = [clusterState EXCEPT ![c] = "ANISWOP"]
    /\ UNCHANGED <<writerMode, outDirEmpty, hdfsAvailable, antiFlapTimer,
                   replayState, lastRoundInSync, lastRoundProcessed,
                   failoverPending, inProgressDirEmpty,
                   zkPeerConnected, zkPeerSessionAlive, zkLocalConnected>>
```

## PeerRecoverFromOFFLINE -- Active Detects Peer Left OFFLINE

Gated on `UseOfflinePeerDetection` (Iteration 18, proactive modeling).

When the active cluster (in AWOP or ANISWOP) detects its peer has left OFFLINE (re-entered a non-OFFLINE state via manual `--force` recovery), the active returns to ANIS:
- AWOP -> ANIS (per `AWOP.allowedTransitions = {ANIS}`)
- ANISWOP -> ANIS (per `ANISWOP.allowedTransitions = {ANIS}`)

Both paths enter ANIS because peer recovery is treated as a new peer entering sync -- the active must first synchronize, so it enters ANIS (not AIS). The anti-flap timer is reset to `StartAntiFlapWait` on ANIS entry.

**ZK watcher dependency:** Delivered via `peerPathChildrenCache`. Guarded on `zkPeerConnected[c]` and `zkPeerSessionAlive[c]`.

**NOTE:** This models intended protocol behavior. See `PeerReactToOFFLINE` comment for implementation status.

Source: (proactive) AWOP->ANIS from `allowedTransitions` L113; ANISWOP->ANIS from `allowedTransitions` L123.

```tla
PeerRecoverFromOFFLINE(c) ==
    /\ UseOfflinePeerDetection = TRUE
    /\ zkPeerConnected[c] = TRUE
    /\ zkPeerSessionAlive[c] = TRUE
    /\ clusterState[Peer(c)] # "OFFLINE"
    /\ clusterState[c] \in {"AWOP", "ANISWOP"}
    /\ clusterState' = [clusterState EXCEPT ![c] = "ANIS"]
    /\ antiFlapTimer' = [antiFlapTimer EXCEPT ![c] = StartAntiFlapWait]
    /\ UNCHANGED <<writerMode, outDirEmpty, hdfsAvailable,
                   replayState, lastRoundInSync, lastRoundProcessed,
                   failoverPending, inProgressDirEmpty,
                   zkPeerConnected, zkPeerSessionAlive, zkLocalConnected>>
```

## ReactiveTransitionFail -- Retry Exhaustion

Models the `FailoverManagementListener` (`HAGroupStoreManager.java` L653-704) where both retries of `setHAGroupStatusIfNeeded()` fail and the method returns silently. The watcher notification was delivered, the listener was invoked, but the local ZK write failed. The transition is permanently lost for this notification.

This action is enabled whenever any `PeerReact*` action would be enabled (same ZK connectivity and peer-state guards), including `PeerReactToOFFLINE` and `PeerRecoverFromOFFLINE` retry exhaustion (gated on `UseOfflinePeerDetection`). Its effect is to leave `clusterState` unchanged -- the local transition was not applied. TLC explores both the success path (the actual `PeerReact*` actions) and this failure path non-deterministically.

**Soundness:** The model is slightly more permissive than the implementation: the same `PeerReact*` action remains enabled after `ReactiveTransitionFail` (the model does not track `lastKnownPeerState`). In the implementation, `handleStateChange()` updates `lastKnownPeerState` before calling `notifySubscribers()`, and after retry failure, if the peer state is re-written with the same value, `handleStateChange()` suppresses the notification (same-state check). This is sound for safety: if safety holds when the transition can non-deterministically succeed or fail, it holds a fortiori when failures are permanent.

```tla
ReactiveTransitionFail(c) ==
    /\ zkPeerConnected[c] = TRUE
    /\ zkPeerSessionAlive[c] = TRUE
    /\ \/ /\ clusterState[Peer(c)] = "ATS"
          /\ clusterState[c] \in {"S", "DS"}
       \/ /\ clusterState[Peer(c)] = "ANIS"
          /\ clusterState[c] \in {"S", "ATS"}
       \/ /\ clusterState[Peer(c)] = "AbTS"
          /\ clusterState[c] = "ATS"
       \/ /\ clusterState[Peer(c)] = "AIS"
          /\ clusterState[c] \in {"ATS", "DS"}
       \* Iteration 18 (proactive): mirrors PeerReactToOFFLINE
       \/ /\ UseOfflinePeerDetection = TRUE
          /\ clusterState[Peer(c)] = "OFFLINE"
          /\ clusterState[c] \in {"AIS", "ANIS"}
       \* Iteration 18 (proactive): mirrors PeerRecoverFromOFFLINE
       \/ /\ UseOfflinePeerDetection = TRUE
          /\ clusterState[Peer(c)] # "OFFLINE"
          /\ clusterState[c] \in {"AWOP", "ANISWOP"}
    /\ UNCHANGED <<clusterState, writerMode, outDirEmpty, hdfsAvailable,
                   antiFlapTimer, replayState, lastRoundInSync,
                   lastRoundProcessed, failoverPending, inProgressDirEmpty,
                   zkPeerConnected, zkPeerSessionAlive, zkLocalConnected>>
```
