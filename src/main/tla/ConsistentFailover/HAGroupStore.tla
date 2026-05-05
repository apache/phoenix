-------------------- MODULE HAGroupStore ----------------------------------------
(*
 * Peer-reactive transitions and auto-completion actions for the
 * Phoenix Consistent Failover protocol.
 *
 * Actions model the FailoverManagementListener (HAGroupStoreManager.java
 * L633-706) which reacts to peer ZK state changes via PathChildrenCache
 * watchers, and the local auto-completion resolvers from
 * createLocalStateTransitions() (L140-150).
 *
 * ZK WATCHER DELIVERY DEPENDENCY: All PeerReact* actions depend on
 * the peer ZK connection and session being alive (guarded by
 * zkPeerConnected[c] and zkPeerSessionAlive[c]). AutoComplete
 * actions depend on the local ZK connection (guarded by
 * zkLocalConnected[c]). Without these connections, watcher
 * notifications cannot be delivered.
 *
 * RETRY EXHAUSTION: The FailoverManagementListener retries each
 * reactive transition exactly 2 times (HAGroupStoreManager.java
 * L653-704). After exhaustion, the method returns silently. This
 * is modeled by the ReactiveTransitionFail(c) action, which
 * non-deterministically "consumes" a pending peer-reactive
 * transition without updating clusterState.
 *
 * Notification chain (peer-reactive transitions):
 *   Peer ZK znode change
 *     -> Curator peerPathChildrenCache
 *       -> HAGroupStoreClient.handleStateChange() [L1088-1110]
 *         -> notifySubscribers() [L1119-1151]
 *           -> FailoverManagementListener.onStateChange() [L653-705]
 *             -> setHAGroupStatusIfNeeded() (2-retry limit)
 *
 * Notification chain (auto-completion transitions):
 *   Local ZK znode change
 *     -> Curator pathChildrenCache (local)
 *       -> HAGroupStoreClient.handleStateChange()
 *         -> notifySubscribers()
 *           -> FailoverManagementListener.onStateChange()
 *
 * LISTENER FOLDS: The recoveryListener (L147-157) and degradedListener
 * (L136-145) from ReplicationLogDiscoveryReplay fire synchronously on
 * the local PathChildrenCache event thread during state entry. Their
 * effects are folded atomically into the S-entry and DS-entry actions:
 *   - S entry (PeerReactToANIS ATS->S, PeerReactToAIS ATS->S,
 *     PeerReactToAIS DS->S, AutoComplete AbTS->S): sets replayState
 *     to SYNCED_RECOVERY.
 *   - DS entry (PeerReactToANIS S->DS): sets replayState to DEGRADED.
 *
 * Implementation traceability:
 *
 *   TLA+ action               | Java source
 *   --------------------------+-----------------------------------------------
 *   PeerReactToATS(c)         | createPeerStateTransitions() L109
 *   PeerReactToANIS(c)        | createPeerStateTransitions() L123, L126
 *   PeerReactToAbTS(c)        | createPeerStateTransitions() L132
 *   PeerReactToAIS(c)         | createPeerStateTransitions() L112-120
 *   AutoComplete(c)           | createLocalStateTransitions() L144, L145, L147
 *   ANISTSToATS(c)            | HAGroupStoreManager.setHAGroupStatusToSync()
 *                             |   L341-355 (ANISTS -> ATS drain completion)
 *   PeerReactToOFFLINE(c)     | (proactive, Iteration 18) intended peer
 *                             |   OFFLINE detection; no impl trigger yet;
 *                             |   gated on UseOfflinePeerDetection
 *   PeerRecoverFromOFFLINE(c) | (proactive, Iteration 18) intended peer
 *                             |   OFFLINE recovery detection; no impl
 *                             |   trigger yet; gated on
 *                             |   UseOfflinePeerDetection
 *   ReactiveTransitionFail(c) | FailoverManagementListener.onStateChange()
 *                             |   L653-704 (2 retries exhausted, returns
 *                             |   silently)
 *
 * Failover completion (STA -> AIS) is modeled in Reader.tla
 * (TriggerFailover action), not in this module.
 *)
EXTENDS SpecState, Types

---------------------------------------------------------------------------

(*
 * Standby entry from ATS (S-entry side effects).
 *
 * Shared by the ATS -> S branches of PeerReactToAIS and
 * PeerReactToANIS. On standby entry from ATS, three side effects
 * fire atomically:
 *
 *   1. Live writers reset to INIT (DEAD preserved). Models the
 *      replication subsystem restart on standby entry: the entire
 *      ReplicationLogGroup is destroyed, so any SYNC/SYNC_AND_FWD/
 *      STORE_AND_FWD writers become INIT. DEAD writers (from
 *      RSCrash or CAS failure) are preserved because crashed RSes
 *      cannot process the state-change notification; they are
 *      handled by RSRestart independently.
 *
 *   2. OUT directory cleared. The forwarder drains before ATS
 *      auto-completes, so outDirEmpty is TRUE by the time the
 *      peer's AIS triggers the local S entry.
 *
 *   3. recoveryListener (L147-157) fires synchronously on the
 *      local PathChildrenCache event thread during S entry,
 *      unconditionally setting replayState to SYNCED_RECOVERY.
 *
 * Not applied to AdminForceRecover (resets all writers to INIT
 * with no DEAD-preservation) or AutoComplete AbTS->S (only sets
 * replayState; no writer/OUT reset needed because AbTS was never
 * active).
 *)
ResetToStandbyEntry(c) ==
    /\ writerMode' = [writerMode EXCEPT ![c] =
            [rs \in RS |-> IF writerMode[c][rs] = "DEAD"
                           THEN "DEAD"
                           ELSE "INIT"]]
    /\ outDirEmpty' = [outDirEmpty EXCEPT ![c] = TRUE]
    /\ replayState' = [replayState EXCEPT ![c] = "SYNCED_RECOVERY"]

---------------------------------------------------------------------------

(*
 * Peer transitions to ATS (ACTIVE_IN_SYNC_TO_STANDBY).
 *
 * When the standby detects its peer has entered ATS, it begins the
 * failover process by transitioning to STA (STANDBY_TO_ACTIVE).
 * This fires from either S or DS -- the DS case supports the ANIS
 * failover path where the standby is in DEGRADED_STANDBY when
 * failover proceeds.
 *
 * ZK watcher dependency: Delivered via peerPathChildrenCache.
 * Guarded on zkPeerConnected[c] and zkPeerSessionAlive[c].
 * If the peer ZK session expires or the notification is lost, the
 * standby never learns of the failover. The active cluster remains
 * in ATS with mutations blocked indefinitely. No polling fallback.
 *
 * Source: createPeerStateTransitions() L109 -- resolver is
 *         unconditional: currentLocal -> STANDBY_TO_ACTIVE.
 *
 * Also sets failoverPending[c] = TRUE, modeling the
 * triggerFailoverListener (ReplicationLogDiscoveryReplay.java
 * L159-171) which fires on LOCAL STANDBY_TO_ACTIVE. Folded
 * into PeerReactToATS because the listener fires
 * deterministically on every STA entry and PeerReactToATS is
 * the sole producer of STA.
 *)
PeerReactToATS(c) ==
    /\ PeerZKHealthy(c)
    /\ clusterState[Peer(c)] = "ATS"
    /\ clusterState[c] \in {"S", "DS"}
    /\ clusterState' = [clusterState EXCEPT ![c] = "STA"]
    /\ failoverPending' = [failoverPending EXCEPT ![c] = TRUE]
    /\ UNCHANGED <<writerVars, replayVars, envVars,
                   outDirEmpty, antiFlapTimer, inProgressDirEmpty>>

---------------------------------------------------------------------------

(*
 * Peer transitions to ANIS (ACTIVE_NOT_IN_SYNC).
 *
 * Two reactive transitions triggered by peer entering ANIS:
 *   1. Local S -> DS: standby degrades because peer's replication is
 *      degraded. Atomically sets replayState = DEGRADED (degraded-
 *      Listener fold). Source: L126.
 *   2. Local ATS -> S: old active (in failover) completes transition
 *      to standby when peer is ANIS. Atomically sets replayState =
 *      SYNCED_RECOVERY (recoveryListener fold). Source: L123.
 *
 * ZK watcher dependency: Delivered via peerPathChildrenCache.
 * Guarded on zkPeerConnected[c] and zkPeerSessionAlive[c].
 * If lost: (1) standby stays in S when it should be DS -- consistency
 * point tracking is incorrect; (2) old active stays in ATS with
 * mutations blocked. No polling fallback.
 *)
PeerReactToANIS(c) ==
    /\ PeerZKHealthy(c)
    /\ clusterState[Peer(c)] = "ANIS"
    /\ \/ /\ clusterState[c] = "S"
          /\ clusterState' = [clusterState EXCEPT ![c] = "DS"]
          \* degradedListener: unconditional set(DEGRADED) fires
          \* synchronously on local PathChildrenCache thread during
          \* DS entry. Counter advance is handled by ReplayAdvance.
          /\ replayState' = [replayState EXCEPT ![c] = "DEGRADED"]
          /\ UNCHANGED <<writerVars, envVars,
                         outDirEmpty, antiFlapTimer,
                         failoverPending, inProgressDirEmpty,
                         lastRoundInSync, lastRoundProcessed>>
       \/ /\ clusterState[c] = "ATS"
          /\ clusterState' = [clusterState EXCEPT ![c] = "S"]
          /\ ResetToStandbyEntry(c)
          /\ UNCHANGED <<envVars,
                         antiFlapTimer, failoverPending, inProgressDirEmpty,
                         lastRoundInSync, lastRoundProcessed>>

---------------------------------------------------------------------------

(*
 * Peer transitions to AbTS (ABORT_TO_STANDBY).
 *
 * When the active cluster (in ATS during failover) detects its peer
 * has entered AbTS (abort initiated from the standby side), the
 * active transitions to AbTAIS (ABORT_TO_ACTIVE_IN_SYNC).
 *
 * ZK watcher dependency: Delivered via peerPathChildrenCache.
 * Guarded on zkPeerConnected[c] and zkPeerSessionAlive[c].
 * If lost: active stays in ATS with mutations blocked; abort does
 * not propagate. No polling fallback.
 *
 * Source: createPeerStateTransitions() L132.
 *)
PeerReactToAbTS(c) ==
    /\ PeerZKHealthy(c)
    /\ clusterState[Peer(c)] = "AbTS"
    /\ clusterState[c] = "ATS"
    /\ clusterState' = [clusterState EXCEPT ![c] = "AbTAIS"]
    /\ UNCHANGED <<writerVars, replayVars, envVars,
                   outDirEmpty, antiFlapTimer,
                   failoverPending, inProgressDirEmpty>>

---------------------------------------------------------------------------

(*
 * Auto-completion transitions (local, no peer trigger).
 *
 * These transitions fire automatically once the cluster enters the
 * corresponding abort state. They return the cluster to its pre-
 * failover state.
 *
 * ZK watcher dependency: Despite being "local" (no peer trigger),
 * these transitions are driven by the local pathChildrenCache
 * watcher chain, not an in-process event bus. Guarded on
 * zkLocalConnected[c]. If the local ZK connection is lost, the
 * cluster remains in the AbTS/AbTAIS/AbTANIS state indefinitely.
 *
 * AbTAIS auto-completion: conditional -- completes to AIS if all
 * writers are clean (INIT or SYNC) and OUT dir is empty, otherwise
 * completes to ANIS. This prevents AIS from coexisting with
 * degraded writers when HDFS fails during the abort window.
 *
 * Source: createLocalStateTransitions() L140-150
 *   AbTS   -> S    (L144) -- atomically sets replayState =
 *                            SYNCED_RECOVERY (recoveryListener fold)
 *   AbTAIS -> AIS or ANIS  (L145) -- conditional on writer/outDir state
 *   AbTANIS -> ANIS (L147)
 *)
AutoComplete(c) ==
    /\ LocalZKHealthy(c)
    /\ \/ /\ clusterState[c] = "AbTS"
          /\ clusterState' = [clusterState EXCEPT ![c] = "S"]
          \* recoveryListener: unconditional set(SYNCED_RECOVERY)
          \* fires synchronously on local PathChildrenCache thread
          \* during S entry.
          /\ replayState' = [replayState EXCEPT ![c] = "SYNCED_RECOVERY"]
          /\ UNCHANGED <<writerVars, envVars,
                         outDirEmpty, antiFlapTimer,
                         failoverPending, inProgressDirEmpty,
                         lastRoundInSync, lastRoundProcessed>>
       \/ /\ clusterState[c] = "AbTAIS"
          /\ clusterState' = [clusterState EXCEPT ![c] =
                 IF outDirEmpty[c] /\ \A rs \in RS : writerMode[c][rs] \in {"INIT", "SYNC"}
                 THEN "AIS"
                 ELSE "ANIS"]
          /\ UNCHANGED <<writerVars, replayVars, envVars,
                         outDirEmpty, antiFlapTimer,
                         failoverPending, inProgressDirEmpty>>
       \/ /\ clusterState[c] = "AbTANIS"
          /\ clusterState' = [clusterState EXCEPT ![c] = "ANIS"]
          /\ antiFlapTimer' = [antiFlapTimer EXCEPT ![c] = StartAntiFlapWait]
          /\ UNCHANGED <<writerVars, replayVars, envVars,
                         outDirEmpty,
                         failoverPending, inProgressDirEmpty>>

---------------------------------------------------------------------------

(*
 * Peer transitions to AIS (ACTIVE_IN_SYNC).
 *
 * Two reactive transitions triggered by peer entering AIS:
 *   1. Local ATS -> S: old active completes failover to standby
 *      when peer (the new active) enters AIS. Atomically sets
 *      replayState = SYNCED_RECOVERY (recoveryListener fold).
 *   2. Local DS -> S: standby recovers from degraded when peer
 *      returns to AIS. Atomically sets replayState =
 *      SYNCED_RECOVERY (recoveryListener fold).
 *
 * WRITER LIFECYCLE RESET (ATS -> S): When the old active enters
 * standby, the FailoverManagementListener triggers a replication
 * subsystem restart on each live RS. Live writer modes reset to
 * INIT (the ReplicationLogGroup is destroyed and will be
 * recreated when the cluster next becomes active). The OUT
 * directory is cleared (outDirEmpty = TRUE). DEAD writers are
 * preserved: a crashed RS (JVM dead) cannot process the state
 * change notification; the process supervisor restart (RSRestart)
 * handles DEAD -> INIT independently. This is critical for the
 * ANIS failover path where SYNC_AND_FWD or STORE_AND_FWD writers
 * may persist through ANISTS -> ATS (ANISTSToATS does not snap
 * writer modes).
 *
 * ZK watcher dependency: Delivered via peerPathChildrenCache.
 * Guarded on zkPeerConnected[c] and zkPeerSessionAlive[c].
 * This is the critical transition that resolves the non-atomic
 * failover window. If lost: old active stays in ATS with mutations
 * blocked indefinitely (the (ATS, AIS) state persists). Safety
 * holds (ATS maps to ACTIVE_TO_STANDBY, isMutationBlocked()=true)
 * but liveness requires eventual watcher delivery. No polling
 * fallback. Curator PathChildrenCache re-queries on reconnect,
 * providing eventual delivery if the ZK session survives.
 *
 * Source: createPeerStateTransitions() L112-120 -- conditional
 *         resolver for peer ACTIVE_IN_SYNC.
 *)
PeerReactToAIS(c) ==
    /\ PeerZKHealthy(c)
    /\ clusterState[Peer(c)] = "AIS"
    /\ \/ /\ clusterState[c] = "ATS"
          /\ clusterState' = [clusterState EXCEPT ![c] = "S"]
          /\ ResetToStandbyEntry(c)
          /\ UNCHANGED <<envVars,
                         antiFlapTimer, failoverPending, inProgressDirEmpty,
                         lastRoundInSync, lastRoundProcessed>>
       \/ /\ clusterState[c] = "DS"
          /\ clusterState' = [clusterState EXCEPT ![c] = "S"]
          \* recoveryListener: unconditional set(SYNCED_RECOVERY)
          \* fires synchronously on local PathChildrenCache thread
          \* during S entry.
          /\ replayState' = [replayState EXCEPT ![c] = "SYNCED_RECOVERY"]
          /\ UNCHANGED <<writerVars, envVars,
                         outDirEmpty, antiFlapTimer,
                         failoverPending, inProgressDirEmpty,
                         lastRoundInSync, lastRoundProcessed>>

---------------------------------------------------------------------------

(*
 * ANIS self-transition (heartbeat): refreshes the anti-flapping
 * countdown timer without changing cluster state.
 *
 * The S&F heartbeat runs while at least one RS is in STORE_AND_FWD
 * mode. It periodically re-writes ANIS to the ZK znode, which
 * refreshes mtime. In the countdown timer model, this resets the
 * timer to StartAntiFlapWait, keeping the anti-flapping gate closed.
 *
 * The heartbeat stops when the last RS exits STORE_AND_FWD (enters
 * SYNC_AND_FWD). At that point the timer begins counting down via
 * Tick, and the gate opens when it reaches 0.
 *
 * Guarded on zkLocalConnected[c] because the heartbeat calls
 * setHAGroupStatusToStoreAndForward() which goes through
 * setHAGroupStatusIfNeeded(), requiring isHealthy = true.
 *
 * Source: StoreAndForwardModeImpl.startHAGroupStoreUpdateTask()
 *         L71-87; HAGroupStoreRecord.java L101 (ANIS self-transition).
 *)
ANISHeartbeat(c) ==
    /\ LocalZKHealthy(c)
    /\ clusterState[c] = "ANIS"
    /\ \E rs \in RS : writerMode[c][rs] = "STORE_AND_FWD"
    /\ antiFlapTimer' = [antiFlapTimer EXCEPT ![c] = StartAntiFlapWait]
    /\ UNCHANGED <<writerVars, replayVars, envVars,
                   clusterState, outDirEmpty,
                   failoverPending, inProgressDirEmpty>>

---------------------------------------------------------------------------

(*
 * Recovery: ANIS -> AIS.
 *
 * When all RS on the cluster are in SYNC or SYNC_AND_FWD, the OUT
 * directory is empty, and the anti-flapping gate has opened
 * (countdown timer reached 0), the cluster recovers from ANIS to AIS.
 *
 * The writer guard includes SYNC_AND_FWD (not just SYNC) because
 * the anti-flapping gate ensures all RS have
 * exited S&F (the heartbeat stops, and WaitTimeForSync ticks must
 * elapse) before this action fires. Any remaining SYNC_AND_FWD RS
 * are atomically transitioned to SYNC, modeling the ACTIVE_IN_SYNC
 * ZK event at ReplicationLogDiscoveryForwarder.init() L113-123.
 *
 * The AISImpliesInSync invariant verifies that AIS is only reached
 * with all RS in SYNC or INIT.
 *
 * Guarded on zkLocalConnected[c] because this calls
 * setHAGroupStatusToSync() which requires isHealthy = true.
 *
 * Source: setHAGroupStatusToSync() L341-355, after forwarder drain.
 *)
ANISToAIS(c) ==
    /\ LocalZKHealthy(c)
    /\ clusterState[c] = "ANIS"
    /\ AntiFlapGateOpen(antiFlapTimer[c])
    /\ \A rs \in RS : writerMode[c][rs] \in {"SYNC", "SYNC_AND_FWD"}
    /\ outDirEmpty[c]
    /\ clusterState' = [clusterState EXCEPT ![c] = "AIS"]
    /\ writerMode' = [writerMode EXCEPT ![c] =
            [rs \in RS |-> IF writerMode[c][rs] = "SYNC_AND_FWD"
                           THEN "SYNC"
                           ELSE writerMode[c][rs]]]
    /\ UNCHANGED <<replayVars, envVars,
                   outDirEmpty, antiFlapTimer,
                   failoverPending, inProgressDirEmpty>>

---------------------------------------------------------------------------

(*
 * Drain completion: ANISTS -> ATS.
 *
 * When the forwarder has drained the OUT directory and the anti-
 * flapping gate has opened, the cluster advances from ANISTS
 * (ACTIVE_NOT_IN_SYNC_TO_STANDBY) to ATS
 * (ACTIVE_IN_SYNC_TO_STANDBY), joining the normal AIS failover
 * path. The standby reacts to ATS (not ANISTS), so this
 * transition is the bridge that lets the ANIS failover path
 * converge with the AIS failover path.
 *
 * Writer modes are NOT snapped here. In the implementation,
 * setHAGroupStatusToSync() only writes the cluster-level ZK
 * znode (ANISTS -> ATS); it does not modify per-RS writer modes.
 * SYNC_AND_FWD writers may persist into ATS. They are cleaned
 * up when the cluster transitions ATS -> S (replication subsystem
 * restart on standby entry -- see PeerReactToAIS, PeerReactToANIS).
 *
 * Anti-flapping gate: confirmed by implementation --
 * validateTransitionAndGetWaitTime() L1035-1036 applies the same
 * waitTimeForSyncModeInMs to ANISTS -> ATS as to ANIS -> AIS.
 * The forwarder handles the wait via syncUpdateTS deferral
 * (processNoMoreRoundsLeft() L169-172).
 *
 * Guarded on zkLocalConnected[c] because this calls
 * setHAGroupStatusToSync() -> setHAGroupStatusIfNeeded() which
 * requires isHealthy = true.
 *
 * Source: HAGroupStoreManager.setHAGroupStatusToSync() L341-355 --
 *         if current state is ANISTS, target is ATS.
 *         HAGroupStoreClient.validateTransitionAndGetWaitTime()
 *         L1027-1046 (anti-flapping gate).
 *)
ANISTSToATS(c) ==
    /\ LocalZKHealthy(c)
    /\ clusterState[c] = "ANISTS"
    /\ AntiFlapGateOpen(antiFlapTimer[c])
    /\ outDirEmpty[c]
    /\ clusterState' = [clusterState EXCEPT ![c] = "ATS"]
    /\ UNCHANGED <<writerVars, replayVars, envVars,
                   outDirEmpty, antiFlapTimer,
                   failoverPending, inProgressDirEmpty>>

---------------------------------------------------------------------------

(*
 * Peer transitions to OFFLINE.
 *
 * Gated on UseOfflinePeerDetection (Iteration 18, proactive modeling).
 *
 * When the active cluster detects its peer has entered OFFLINE, it
 * transitions to AWOP or ANISWOP depending on its current state:
 *   AIS  -> AWOP    (peer went offline while active is in sync)
 *   ANIS -> ANISWOP (peer went offline while active is not in sync)
 *
 * Both AWOP and ANISWOP map to ClusterRole.ACTIVE via
 * getClusterRole() (isMutationBlocked()=false), so the active
 * cluster continues serving mutations while its peer is offline.
 *
 * No writer or timer side effects: the transition is purely a
 * cluster-state annotation recording the peer's unavailability.
 *
 * ZK watcher dependency: Delivered via peerPathChildrenCache.
 * Guarded on zkPeerConnected[c] and zkPeerSessionAlive[c].
 *
 * NOTE: This models intended protocol behavior. No
 * FailoverManagementListener entry for peer OFFLINE currently
 * exists in the implementation (createPeerStateTransitions()
 * has no OFFLINE entry). The TLA+ model verifies the design
 * ahead of implementation.
 *
 * Source: (proactive) AIS->AWOP from allowedTransitions L103;
 *         ANIS->ANISWOP from allowedTransitions L101.
 *)
PeerReactToOFFLINE(c) ==
    /\ UseOfflinePeerDetection = TRUE
    /\ PeerZKHealthy(c)
    /\ clusterState[Peer(c)] = "OFFLINE"
    /\ \/ /\ clusterState[c] = "AIS"
          /\ clusterState' = [clusterState EXCEPT ![c] = "AWOP"]
       \/ /\ clusterState[c] = "ANIS"
          /\ clusterState' = [clusterState EXCEPT ![c] = "ANISWOP"]
    /\ UNCHANGED <<writerVars, replayVars, envVars,
                   outDirEmpty, antiFlapTimer,
                   failoverPending, inProgressDirEmpty>>

---------------------------------------------------------------------------

(*
 * Peer recovers from OFFLINE.
 *
 * Gated on UseOfflinePeerDetection (Iteration 18, proactive modeling).
 *
 * When the active cluster (in AWOP or ANISWOP) detects its peer
 * has left OFFLINE (re-entered a non-OFFLINE state via manual
 * --force recovery), the active returns to ANIS:
 *   AWOP    -> ANIS (per AWOP.allowedTransitions = {ANIS})
 *   ANISWOP -> ANIS (per ANISWOP.allowedTransitions = {ANIS})
 *
 * Both paths enter ANIS because peer recovery is treated as a
 * new peer entering sync -- the active must first synchronize,
 * so it enters ANIS (not AIS). The anti-flap timer is reset to
 * StartAntiFlapWait on ANIS entry.
 *
 * ZK watcher dependency: Delivered via peerPathChildrenCache.
 * Guarded on zkPeerConnected[c] and zkPeerSessionAlive[c].
 *
 * NOTE: This models intended protocol behavior. See
 * PeerReactToOFFLINE comment for implementation status.
 *
 * Source: (proactive) AWOP->ANIS from allowedTransitions L113;
 *         ANISWOP->ANIS from allowedTransitions L123.
 *)
PeerRecoverFromOFFLINE(c) ==
    /\ UseOfflinePeerDetection = TRUE
    /\ PeerZKHealthy(c)
    /\ clusterState[Peer(c)] # "OFFLINE"
    /\ clusterState[c] \in {"AWOP", "ANISWOP"}
    /\ clusterState' = [clusterState EXCEPT ![c] = "ANIS"]
    /\ antiFlapTimer' = [antiFlapTimer EXCEPT ![c] = StartAntiFlapWait]
    /\ UNCHANGED <<writerVars, replayVars, envVars,
                   outDirEmpty, failoverPending, inProgressDirEmpty>>

---------------------------------------------------------------------------

(*
 * Reactive transition retry exhaustion.
 *
 * Models the FailoverManagementListener (HAGroupStoreManager.java
 * L653-704) where both retries of setHAGroupStatusIfNeeded() fail
 * and the method returns silently. The watcher notification was
 * delivered, the listener was invoked, but the local ZK write
 * failed. The transition is permanently lost for this notification.
 *
 * This action is enabled whenever any PeerReact* action would be
 * enabled (same ZK connectivity and peer-state guards). Its effect
 * is to leave clusterState unchanged -- the local transition was
 * not applied. TLC explores both the success path (the actual
 * PeerReact actions) and this failure path non-deterministically.
 *
 * In the implementation, handleStateChange() updates
 * lastKnownPeerState before calling notifySubscribers(). After
 * retry failure, if the peer state is re-written with the same
 * value, handleStateChange() suppresses the notification (same-
 * state check). The model is slightly more permissive: the same
 * PeerReact* action remains enabled after ReactiveTransitionFail
 * (the model does not track lastKnownPeerState). This is sound
 * for safety: if safety holds when the transition can non-
 * deterministically succeed or fail, it holds a fortiori when
 * failures are permanent.
 *
 * Source: FailoverManagementListener.onStateChange()
 *         (HAGroupStoreManager.java L653-704) --
 *         2-retry exhaustion, method returns silently.
 *)

\* Guard disjunction shared by ReactiveTransitionFail. Mirrors the
\* peer-state/local-state enabling conditions of PeerReactToATS,
\* PeerReactToANIS, PeerReactToAbTS, PeerReactToAIS,
\* PeerReactToOFFLINE, and PeerRecoverFromOFFLINE so the retry-
\* exhaustion action cannot drift from the reactive transitions
\* it shadows. Intentionally does NOT include the PeerZKHealthy(c)
\* guard: that is applied at the call site uniformly for all
\* PeerReact* actions and ReactiveTransitionFail itself.
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

ReactiveTransitionFail(c) ==
    /\ PeerZKHealthy(c)
    /\ PeerReactWouldFire(c)
    /\ UNCHANGED vars

============================================================================
