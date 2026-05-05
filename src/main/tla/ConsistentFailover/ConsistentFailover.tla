-------------------- MODULE ConsistentFailover --------------------------------
(*
 * TLA+ specification of the Phoenix Consistent Failover protocol.
 *
 * Root orchestrator module: EXTENDS SpecState (variables), defines Init, Next,
 * Spec, invariants, and action constraints. Composes actor-driven
 * actions from sub-modules via INSTANCE.
 *
 * Models the HA group state machine for two paired Phoenix/HBase
 * clusters. Each cluster maintains an HA group state in ZooKeeper.
 * State transitions are driven by admin actions, peer-reactive
 * listeners, writer/reader state changes, HDFS availability
 * incidents, and ZK coordination failures.
 *
 * ZK COORDINATION MODEL: ZK connection and session
 * lifecycle are modeled explicitly. Peer-reactive transitions
 * (PeerReact actions) are guarded on zkPeerConnected[c] and
 * zkPeerSessionAlive[c]. Auto-completion, heartbeat, writer ZK
 * writes, and failover trigger are guarded on zkLocalConnected[c].
 * Retry exhaustion of the FailoverManagementListener (2-retry
 * limit) is modeled as ReactiveTransitionFail(c).
 *
 * Sub-modules:
 *   - Admin.tla: operator-initiated failover/abort
 *   - Clock.tla: anti-flapping countdown timer (Tick action)
 *   - HAGroupStore.tla: peer-reactive transitions, auto-completion,
 *                       retry exhaustion
 *   - HDFS.tla: HDFS availability incident actions
 *   - Reader.tla: standby-side replication replay state machine
 *   - RS.tla: RS lifecycle (crash, abort on local HDFS failure,
 *             restart after abort)
 *   - Writer.tla: per-RS replication writer mode state machine
 *   - ZK.tla: ZK connection/session lifecycle environment actions
 *
 * Implementation traceability:
 *
 *   Modeled concept         | Java class / field
 *   ------------------------+---------------------------------------------
 *   clusterState            | HAGroupStoreRecord per-cluster ZK znode
 *   PeerReact* actions      | FailoverManagementListener
 *                           |   (HAGroupStoreManager.java L633-706)
 *                           |   Delivered via peerPathChildrenCache
 *                           |   (ZK watcher -- conditional delivery)
 *   ReactiveTransitionFail  | FailoverManagementListener 2-retry
 *                           |   exhaustion (L653-704); method returns
 *                           |   silently, transition permanently lost
 *   TriggerFailover          | Reader.TriggerFailover -- guarded
 *                           |   STA->AIS via shouldTriggerFailover()
 *                           |   L500-533 + triggerFailover() L535-548
 *   AutoComplete            | createLocalStateTransitions() L140-150
 *                           |   Delivered via local pathChildrenCache
 *                           |   (ZK watcher -- conditional delivery)
 *   ANISTSToATS             | HAGroupStoreManager
 *                           |   .setHAGroupStatusToSync() L341-355
 *                           |   ANISTS -> ATS (drain completion)
 *   AdminStartFailover      | initiateFailoverOnActiveCluster() L375-400
 *                           |   AIS -> ATS or ANIS -> ANISTS
 *   AdminAbortFailover      | setHAGroupStatusToAbortToStandby() L419-425
 *   AdminGoOffline          | PhoenixHAAdminTool update --state OFFLINE
 *                           |   (gated on UseOfflinePeerDetection)
 *   AdminForceRecover       | PhoenixHAAdminTool update --force
 *                           |   --state STANDBY (OFFLINE -> S)
 *                           |   (gated on UseOfflinePeerDetection)
 *   PeerReactToOFFLINE      | intended peer OFFLINE detection;
 *                           |   gated on UseOfflinePeerDetection
 *   PeerRecoverFromOFFLINE  | intended peer OFFLINE recovery;
 *                           |   gated on UseOfflinePeerDetection
 *   Init (AIS, S)           | Default initial states per team confirmation
 *                           |   (see PHOENIX_HA_TLA_PLAN.md Appendix A.6)
 *   MutualExclusion         | Architecture safety argument: at most one
 *                           |   cluster in ACTIVE role at any time
 *   AbortSafety             | Abort originates from STA side; AbTAIS
 *                           |   only reachable via peer AbTS detection
 *   AllowedTransitions      | HAGroupStoreRecord.java L99-123
 *   writerMode              | ReplicationLogGroup per-RS mode
 *                           |   (SYNC/STORE_AND_FWD/SYNC_AND_FWD)
 *   outDirEmpty             | ReplicationLogDiscoveryForwarder
 *                           |   .processNoMoreRoundsLeft() L155-184
 *                           |   Boolean: OUT dir empty/non-empty
 *   hdfsAvailable           | Abstract: NameNode availability per cluster
 *                           |   (no explicit field in implementation;
 *                           |   detected via IOException)
 *   RSCrash                 | JVM crash, OOM, kill signal
 *   RSAbortOnLocalHDFS-     | StoreAndForwardModeImpl.onFailure()
 *     Failure               |   L115-123 -> logGroup.abort()
 *   HDFSDown/HDFSUp         | NameNode crash/recovery incidents;
 *                           |   SyncModeImpl.onFailure() L61-74
 *   antiFlapTimer           | Countdown timer (Lamport CHARME 2005);
 *                           |   models validateTransitionAndGetWait-
 *                           |   Time() L1027-1046 anti-flapping gate
 *   Tick                    | Passage of wall-clock time
 *   ANISHeartbeat           | StoreAndForwardModeImpl
 *                           |   .startHAGroupStoreUpdateTask() L71-87
 *   replayState             | ReplicationLogDiscoveryReplay replay state
 *                           |   (NOT_INITIALIZED/SYNC/DEGRADED/
 *                           |   SYNCED_RECOVERY)
 *   lastRoundInSync         | ReplicationLogDiscoveryReplay L336-343
 *   lastRoundProcessed      | ReplicationLogDiscoveryReplay L336-351
 *   failoverPending         | ReplicationLogDiscoveryReplay L159-171
 *   inProgressDirEmpty      | ReplicationLogDiscoveryReplay L500-533
 *   ReplayAdvance           | replay() L336-343 (SYNC) and L345-351
 *                           |   (DEGRADED) round processing
 *   ReplayRewind            | replay() L323-333 (CAS to SYNC)
 *   [listener folds]        | degradedListener L136-145 and
 *                           |   recoveryListener L147-157 are folded
 *                           |   into HAGroupStore S/DS-entry actions
 *   TriggerFailover         | shouldTriggerFailover() L500-533 +
 *                           |   triggerFailover() L535-548
 *   FailoverTriggerCorrectness | Action constraint: STA->AIS requires
 *                           |   failoverPending /\ inProgressDirEmpty
 *                           |   /\ replayState = SYNC
 *   NoDataLoss              | Action constraint: zero RPO property
 *                           |   for failover (STA->AIS)
 *   zkPeerConnected         | peerPathChildrenCache TCP connection
 *                           |   state (HAGroupStoreClient L110-112)
 *   zkPeerSessionAlive      | Peer ZK session state (Curator internal)
 *   zkLocalConnected        | pathChildrenCache TCP connection state;
 *                           |   maps to HAGroupStoreClient.isHealthy
 *                           |   (L878-911)
 *   ZKPeerDisconnect        | peerPathChildrenCache CONNECTION_LOST
 *   ZKPeerReconnect         | peerPathChildrenCache CONNECTION_RECONNECTED
 *   ZKPeerSessionExpiry     | Curator session expiry -> CONNECTION_LOST
 *   ZKPeerSessionRecover    | Curator retry -> new session
 *   ZKLocalDisconnect       | pathChildrenCache CONNECTION_LOST
 *   ZKLocalReconnect        | pathChildrenCache CONNECTION_RECONNECTED
 *
 * failoverPending lifecycle:
 *   Set TRUE:  PeerReactToATS (HAGroupStore.tla)
 *   Set FALSE: TriggerFailover (Reader.tla)
 *   Set FALSE: AdminAbortFailover (Admin.tla)
 *)
EXTENDS SpecState, Types

---------------------------------------------------------------------------

\* The variable-group tuples (writerVars, clusterVars, replayVars,
\* envVars) and the full `vars` tuple used in temporal formulas
\* ([][Next]_vars, WF_vars, SF_vars) are defined in SpecState.tla
\* so they are shared by all sub-modules via EXTENDS.

(*
 * Replay-completeness guards for STA -> AIS (TriggerFailover /
 * shouldTriggerFailover). Shared by FailoverTriggerCorrectness and
 * NoDataLoss so the two action constraints cannot drift apart.
 *)
STAtoAISTriggerReplayGuards(c) ==
    /\ failoverPending[c]
    /\ inProgressDirEmpty[c]
    /\ replayState[c] = "SYNC"

---------------------------------------------------------------------------

(* Sub-module instances *)

\* Peer-reactive transitions and auto-completion.
haGroupStore == INSTANCE HAGroupStore

\* Operator-initiated failover and abort.
admin == INSTANCE Admin

\* Per-RS replication writer mode state machine.
writer == INSTANCE Writer

\* HDFS availability incident actions.
hdfs == INSTANCE HDFS

\* RS lifecycle (crash, local HDFS abort, restart).
rs == INSTANCE RS

\* Anti-flapping countdown timer.
clk == INSTANCE Clock

\* Replication replay state machine (standby-side reader).
reader == INSTANCE Reader

\* ZK connection/session lifecycle environment actions.
zk == INSTANCE ZK

---------------------------------------------------------------------------

(* Initial state *)

\* The system starts with one cluster active and in sync (AIS)
\* and the other in standby (S). The choice of which cluster is
\* active is deterministic: CHOOSE picks an arbitrary but fixed
\* element of Cluster as the initial active.
\*
\* The standby starts with replayState = SYNCED_RECOVERY, modeling
\* the recoveryListener having already fired during startup
\* (NOT_INITIALIZED -> SYNCED_RECOVERY is synchronous with S
\* entry on the local PathChildrenCache event thread). The active
\* starts NOT_INITIALIZED (reader is dormant until the cluster
\* first enters S after a failover).
Init ==
    \* Deterministically assign one cluster to AIS and the other to S.
    \* CHOOSE x \in Cluster : TRUE picks an arbitrary fixed cluster.
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

---------------------------------------------------------------------------

(* Next-state relation *)

(*
 * In each step, exactly one cluster performs one actor-driven action.
 * Actions are factored by actor:
 *   - haGroupStore: peer-reactive transitions and auto-completion
 *     (FailoverManagementListener + local resolvers)
 *     ALL of these depend on ZK watcher notification chains for
 *     delivery. See HAGroupStore.tla module header for details.
 *   - admin: operator-initiated failover and abort
 *     These are direct ZK writes (not watcher-dependent).
 *   - hdfs: HDFS NameNode crash/recovery incidents
 *     HDFSDown sets the availability flag for any cluster's HDFS;
 *     per-RS degradation is handled by writer actions with CAS
 *     success/failure. Local HDFS failure with S&F writers
 *     triggers RS abort (RS.tla).
 *   - writer: per-RS writer mode transitions (startup, recovery,
 *     drain complete, HDFS failure degradation, CAS failure)
 *   - rs: RS lifecycle (crash, local HDFS abort, restart)
 *
 * Each action encodes the precise guard (peer state or local state)
 * under which the transition fires, modeling the implementation's
 * actual trigger conditions.
 *)
Next ==
    \* [Timer] Anti-flapping countdown timer tick (global).
    \/ clk!Tick
    \/ \E c \in Cluster :
        \* [ZK watcher] Peer-reactive: standby detects peer ATS.
        \/ haGroupStore!PeerReactToATS(c)
        \* [ZK watcher] Peer-reactive: cluster detects peer ANIS.
        \/ haGroupStore!PeerReactToANIS(c)
        \* [ZK watcher] Peer-reactive: active detects peer AbTS.
        \/ haGroupStore!PeerReactToAbTS(c)
        \* [ZK watcher] Local auto-completion: AbTS->S, etc.
        \/ haGroupStore!AutoComplete(c)
        \* [Reader-driven] Standby completes failover: STA->AIS (guarded).
        \/ reader!TriggerFailover(c)
        \* [ZK watcher] Peer-reactive: cluster detects peer AIS.
        \/ haGroupStore!PeerReactToAIS(c)
        \* [S&F heartbeat] ANIS self-transition: resets anti-flap timer.
        \/ haGroupStore!ANISHeartbeat(c)
        \* [Writer-driven] All RS synced + OUT empty + gate open: ANIS->AIS.
        \/ haGroupStore!ANISToAIS(c)
        \* [Writer-driven] OUT drained + anti-flap gate open: ANISTS->ATS.
        \/ haGroupStore!ANISTSToATS(c)
        \* [Retry exhaustion] PeerReact retry failure: transition lost.
        \/ haGroupStore!ReactiveTransitionFail(c)
        \* [ZK watcher] Peer-reactive: active detects peer OFFLINE.
        \* (gated on UseOfflinePeerDetection)
        \/ haGroupStore!PeerReactToOFFLINE(c)
        \* [ZK watcher] Peer-reactive: active detects peer left OFFLINE.
        \* (gated on UseOfflinePeerDetection)
        \/ haGroupStore!PeerRecoverFromOFFLINE(c)
        \* [Direct ZK write] Admin initiates failover: AIS->ATS or ANIS->ANISTS.
        \/ admin!AdminStartFailover(c)
        \* [Direct ZK write] Admin aborts failover: STA->AbTS.
        \/ admin!AdminAbortFailover(c)
        \* [Direct ZK write] Admin takes standby offline: S/DS->OFFLINE.
        \* (gated on UseOfflinePeerDetection)
        \/ admin!AdminGoOffline(c)
        \* [Direct ZK write] Admin force-recovers from OFFLINE: OFFLINE->S.
        \* (gated on UseOfflinePeerDetection)
        \/ admin!AdminForceRecover(c)
        \* HDFS NameNode crash/recovery incidents.
        \/ hdfs!HDFSDown(c)
        \/ hdfs!HDFSUp(c)
        \* ZK connection/session lifecycle (environment actions).
        \/ zk!ZKPeerDisconnect(c)
        \/ zk!ZKPeerReconnect(c)
        \/ zk!ZKPeerSessionExpiry(c)
        \/ zk!ZKPeerSessionRecover(c)
        \/ zk!ZKLocalDisconnect(c)
        \/ zk!ZKLocalReconnect(c)
        \* Standby-side replay state machine (reader).
        \* Listener effects (degradedListener, recoveryListener) are
        \* folded into the S-entry and DS-entry actions in HAGroupStore.
        \/ reader!ReplayAdvance(c)
        \/ reader!ReplayRewind(c)
        \* In-progress directory dynamics (reader round processing).
        \/ reader!ReplayBeginProcessing(c)
        \/ reader!ReplayFinishProcessing(c)
        \* Per-RS writer mode transitions and RS lifecycle.
        \/ \E r \in RS :
            \* Writer startup.
            \/ writer!WriterInit(c, r)
            \/ writer!WriterInitToStoreFwd(c, r)
            \/ writer!WriterInitToStoreFwdFail(c, r)
            \* Writer mode transitions (recovery, drain, forwarder).
            \/ writer!WriterSyncToSyncFwd(c, r)
            \/ writer!WriterStoreFwdToSyncFwd(c, r)
            \/ writer!WriterSyncFwdToSync(c, r)
            \* Per-RS HDFS failure degradation (CAS success).
            \/ writer!WriterToStoreFwd(c, r)
            \/ writer!WriterSyncFwdToStoreFwd(c, r)
            \* Per-RS HDFS failure degradation (CAS failure -> DEAD).
            \/ writer!WriterToStoreFwdFail(c, r)
            \/ writer!WriterSyncFwdToStoreFwdFail(c, r)
            \* RS lifecycle: crash, local HDFS abort, restart.
            \/ rs!RSRestart(c, r)
            \/ rs!RSCrash(c, r)
            \/ rs!RSAbortOnLocalHDFSFailure(c, r)

---------------------------------------------------------------------------

(* Specification *)

\* Safety-only specification: initial state, followed by zero or
\* more Next steps (or stuttering). No fairness — used for fast
\* safety-only model checking (no temporal overhead).
SafetySpec == Init /\ [][Next]_vars

---------------------------------------------------------------------------

(* Fairness *)

(*
 * Fairness assumptions for liveness checking.
 *
 * Classifies every action in Next into one of four fairness tiers.
 * The guiding principle: any action whose guard depends on an
 * environment variable that oscillates without fairness needs SF,
 * because the adversary can cycle the env var once per lasso cycle
 * to break WF's continuous-enablement requirement.
 *
 * Tier 3 actions are grouped into disjunctions under a single
 * SF_vars to keep the temporal formula within TLC's DNF size
 * limit. When at most one disjunct is ENABLED in any state,
 * SF(A1\/...\/An) ≡ SF(A1)/\.../\SF(An): the only disjunct
 * that can fire is the one that is enabled, so the scheduler
 * cannot satisfy the disjunction by firing a different disjunct.
 * Mutual exclusivity is guaranteed by the single-valued nature
 * of clusterState (per-cluster groups) and writerMode (per-RS
 * groups).
 *
 *   1. WF on protocol-internal steps whose guards depend only on
 *      protocol state variables (no env var guards). Continuous
 *      enablement is guaranteed by protocol progress. Includes
 *      Tick, replay state machine, WriterInit, WriterSyncToSyncFwd.
 *      Exception: ANISHeartbeat keeps WF despite its zkLocal-
 *      Connected guard because suppressing the heartbeat HELPS
 *      liveness (the anti-flap gate opens sooner); SF would be
 *      counterproductive.
 *
 *   2. WF on ZK recovery actions (ZKPeerReconnect, ZKPeerSession-
 *      Recover, ZKLocalReconnect): encodes the ZK Liveness
 *      Assumption (ZLA, §4.2). ZK sessions are eventually alive
 *      and connected. These recovery actions are the basis for
 *      SF on all actions guarded by zkPeerConnected or
 *      zkLocalConnected.
 *
 *   3. SF on all actions guarded by environment variables that
 *      oscillate without fairness: zkPeerConnected, zkPeerSession-
 *      Alive, zkLocalConnected, hdfsAvailable. Grouped as:
 *        - Peer-reactive (exclusive by clusterState[Peer(c)]):
 *          PeerReactToATS/ANIS/AIS/AbTS
 *        - Local transitions (exclusive by clusterState[c]):
 *          AutoComplete, ANISToAIS, ANISTSToATS, TriggerFailover
 *        - HDFSUp (standalone)
 *        - Writer degradation (exclusive by writerMode):
 *          WriterToStoreFwd, WriterSyncFwdToStoreFwd,
 *          WriterInitToStoreFwd
 *        - Writer recovery (exclusive by writerMode):
 *          WriterStoreFwdToSyncFwd, WriterSyncFwdToSync
 *        - RS lifecycle (exclusive by writerMode):
 *          RSAbortOnLocalHDFSFailure, RSRestart
 *
 *   4. No fairness on non-deterministic environmental faults
 *      (HDFSDown, RSCrash, ZKPeerDisconnect, ZKPeerSessionExpiry,
 *      ZKLocalDisconnect, ReactiveTransitionFail), operator actions
 *      (AdminStartFailover, AdminAbortFailover, AdminGoOffline,
 *      AdminForceRecover), and CAS failures
 *      (WriterToStoreFwdFail, WriterSyncFwdToStoreFwdFail,
 *      WriterInitToStoreFwdFail). These are genuinely non-
 *      deterministic; imposing fairness would force unrealistic
 *      guarantees.
 *)
Fairness ==
    \* --- Tier 1: WF on protocol-internal steps ---
    \* Guards depend only on protocol state; continuous enablement
    \* is guaranteed by protocol progress.
    /\ WF_vars(clk!Tick)
    /\ \A c \in Cluster :
        \* [S&F heartbeat] Anti-flap timer reset. Keeps WF despite
        \* zkLocalConnected guard: suppressing the heartbeat HELPS
        \* liveness (gate opens sooner), so SF would be counterproductive.
        /\ WF_vars(haGroupStore!ANISHeartbeat(c))
        \* [Reader] Replay state machine (no env var guards).
        /\ WF_vars(reader!ReplayAdvance(c))
        /\ WF_vars(reader!ReplayRewind(c))
        /\ WF_vars(reader!ReplayBeginProcessing(c))
        /\ WF_vars(reader!ReplayFinishProcessing(c))
        \* --- Tier 2: WF on ZK recovery (encodes ZLA §4.2) ---
        \* ZK sessions are eventually alive and connected. These
        \* recovery actions are the basis for SF on all actions
        \* guarded by zkPeerConnected/zkLocalConnected.
        /\ WF_vars(zk!ZKPeerReconnect(c))
        /\ WF_vars(zk!ZKPeerSessionRecover(c))
        /\ WF_vars(zk!ZKLocalReconnect(c))
        \* --- Tier 3: SF on actions guarded by env vars ---
        \* Grouped by mutual exclusivity to keep TLC's temporal
        \* formula within its DNF size limit. When at most one
        \* disjunct is ENABLED in any state, SF(A1\/...\/An) is
        \* equivalent to SF(A1)/\.../\SF(An), because the only
        \* disjunct that can fire is the one that is enabled.
        \*
        \* Peer-reactive group (exclusive by clusterState[Peer(c)]
        \* and clusterState[c]: ATS, ANIS, AbTS, AIS, OFFLINE are
        \* mutually exclusive peer states; AWOP/ANISWOP are mutually
        \* exclusive with S/DS/ATS local states of other PeerReact
        \* actions).
        /\ SF_vars(haGroupStore!PeerReactToATS(c)
                   \/ haGroupStore!PeerReactToANIS(c)
                   \/ haGroupStore!PeerReactToAbTS(c)
                   \/ haGroupStore!PeerReactToAIS(c)
                   \/ haGroupStore!PeerReactToOFFLINE(c)
                   \/ haGroupStore!PeerRecoverFromOFFLINE(c))
        \* Local cluster transition group (exclusive by
        \* clusterState[c]: AbTS/AbTAIS/AbTANIS, ANIS, ANISTS,
        \* STA are mutually exclusive).
        /\ SF_vars(haGroupStore!AutoComplete(c)
                   \/ haGroupStore!ANISToAIS(c)
                   \/ haGroupStore!ANISTSToATS(c)
                   \/ reader!TriggerFailover(c))
        \* [Environmental] HDFS recovery.
        /\ SF_vars(hdfs!HDFSUp(c))
        \* --- Per-RS actions ---
        /\ \A r \in RS :
            \* Writer startup (no env var guard).
            /\ WF_vars(writer!WriterInit(c, r))
            \* Forwarder-driven SYNC->S&FWD (no env var guard).
            /\ WF_vars(writer!WriterSyncToSyncFwd(c, r))
            \* Writer degradation group (exclusive by writerMode:
            \* SYNC, SYNC_AND_FWD, INIT are mutually exclusive;
            \* all guarded on zkLocalConnected, ~hdfsAvailable[Peer]).
            /\ SF_vars(writer!WriterToStoreFwd(c, r)
                       \/ writer!WriterSyncFwdToStoreFwd(c, r)
                       \/ writer!WriterInitToStoreFwd(c, r))
            \* Writer recovery group (exclusive by writerMode:
            \* STORE_AND_FWD, SYNC_AND_FWD are mutually exclusive;
            \* guarded on hdfsAvailable[Peer]).
            /\ SF_vars(writer!WriterStoreFwdToSyncFwd(c, r)
                       \/ writer!WriterSyncFwdToSync(c, r))
            \* RS lifecycle group (exclusive by writerMode:
            \* STORE_AND_FWD, DEAD are mutually exclusive).
            /\ SF_vars(rs!RSAbortOnLocalHDFSFailure(c, r)
                       \/ rs!RSRestart(c, r))

---------------------------------------------------------------------------

(* Liveness specifications *)

\* Full specification: safety conjoined with the complete fairness
\* formula. Documents the full fairness design; too large for TLC's
\* Buchi automaton construction (43 temporal clauses). Used only in
\* THEOREM declarations.
Spec == Init /\ [][Next]_vars /\ Fairness

\* Per-property specifications: each conjoins only the fairness
\* clauses on the critical path for one liveness property, keeping
\* the temporal formula small enough for TLC.

\* AbortCompletion: AutoComplete (SF, zkLocalConnected guard),
\* ZKLocalReconnect (WF, re-enables zkLocalConnected), Tick (WF).
FairnessAC ==
    /\ WF_vars(clk!Tick)
    /\ \A c \in Cluster :
        /\ WF_vars(zk!ZKLocalReconnect(c))
        /\ SF_vars(haGroupStore!AutoComplete(c))

SpecAC == Init /\ [][Next]_vars /\ FairnessAC

\* FailoverCompletion: AutoComplete + TriggerFailover (SF, grouped
\* by clusterState exclusivity), HDFSUp (SF), ZKLocalReconnect (WF),
\* replay machine including ReplayRewind (WF), Tick (WF).
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

\* DegradationRecovery: ANISToAIS (SF), HDFSUp (SF),
\* ZKLocalReconnect (WF), Tick (WF), ANISHeartbeat (WF),
\* per-RS writer recovery chain (SF) and lifecycle (SF),
\* WriterInit and WriterSyncToSyncFwd (WF).
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

---------------------------------------------------------------------------

(* Liveness properties *)

(*
 * Failover completion: standby-side and abort transient states
 * eventually resolve to a stable state. Resolution paths:
 *   STA -> AIS (TriggerFailover) or STA -> AbTS -> S (abort)
 *   AbTAIS -> AIS/ANIS, AbTANIS -> ANIS, AbTS -> S
 *       (auto-completion)
 *
 * ATS and ANISTS are excluded: their resolution depends on the
 * peer completing failover (PeerReactToAIS/PeerReactToANIS) or
 * on abort propagation (PeerReactToAbTS). Both require the peer
 * to reach a specific state AND the ZK peer connection to be alive
 * at the right moment. With no fairness on admin actions (the
 * admin can abort every failover attempt) and no fairness on ZK
 * disconnect (the scheduler can disconnect exactly when the peer
 * is in AbTS), ATS can remain indefinitely. ATS does have a
 * resolution path via the reconciliation fold in ZKPeerReconnect/
 * ZKPeerSessionRecover (ATS -> AbTAIS -> AIS when peer is in
 * S/DS at reconnect), but adding ATS here would require
 * extending FairnessFC with the peer-reactive SF group.
 *
 * Predicated on ZLA (encoded in Fairness).
 *)
FailoverCompletion ==
    \A c \in Cluster :
        clusterState[c] \in FailoverCompletionAntecedentStates
        ~> clusterState[c] \in StableClusterStates

---------------------------------------------------------------------------

(*
 * Degradation recovery: ANIS with available peer HDFS eventually
 * progresses out of ANIS. The recovery chain is:
 *   S&F -> S&FWD (WriterStoreFwdToSyncFwd)
 *     -> SYNC (WriterSyncFwdToSync, sets outDirEmpty)
 *     -> anti-flap timer expires (Tick)
 *     -> ANIS -> AIS (ANISToAIS)
 *
 * The cluster may also leave ANIS via failover (ANIS -> ANISTS),
 * which satisfies the consequent. Under SF on HDFSUp, HDFS
 * cannot be permanently down.
 *
 * Predicated on ZLA (encoded in Fairness).
 *)
DegradationRecovery ==
    \A c \in Cluster :
        (clusterState[c] = "ANIS" /\ hdfsAvailable[Peer(c)])
        ~> clusterState[c] \in NotANISClusterStates

---------------------------------------------------------------------------

(*
 * Abort completion: every abort state eventually auto-completes
 * to a stable state.
 *   AbTS -> S    (AutoComplete)
 *   AbTAIS -> AIS or ANIS (AutoComplete, conditional on
 *             writer/outDir state)
 *   AbTANIS -> ANIS (AutoComplete)
 *
 * Under WF on AutoComplete, each abort state deterministically
 * resolves. Requires zkLocalConnected (AutoComplete guard).
 *
 * Predicated on ZLA (encoded in Fairness).
 *)
AbortCompletion ==
    \A c \in Cluster :
        clusterState[c] \in AbortCompletionAntecedentStates
        ~> clusterState[c] \in StableClusterStates

---------------------------------------------------------------------------

(* Type invariant *)

\* All specification variables have valid types.
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

---------------------------------------------------------------------------

(* Safety invariants *)

(*
 * ZK session/connection structural consistency: if the peer ZK
 * session is expired, the peer connection must also be dead.
 * Session expiry implies disconnection -- the ZKPeerSessionExpiry
 * action sets both zkPeerSessionAlive and zkPeerConnected to FALSE.
 * ZKPeerReconnect requires zkPeerSessionAlive = TRUE, so a
 * reconnect cannot happen without a live session.
 *
 * This invariant verifies that the ZK actions correctly maintain
 * the session/connection relationship across all reachable states.
 *)
ZKSessionConsistency ==
    \A c \in Cluster :
        zkPeerSessionAlive[c] = FALSE => zkPeerConnected[c] = FALSE

\* Mutual exclusion: two clusters never both in the ACTIVE role
\* simultaneously. This is the primary safety property of the
\* failover protocol.
\*
\* The ACTIVE role includes: AIS, ANIS, AbTAIS, AbTANIS, AWOP,
\* ANISWOP. Transitional states ATS and ANISTS map to the
\* ACTIVE_TO_STANDBY role (not ACTIVE), which is the mechanism
\* by which safety is maintained during the non-atomic failover
\* window -- isMutationBlocked()=true for ACTIVE_TO_STANDBY.
\*
\* Source: Architecture safety argument; ClusterRoleRecord.java
\*         L84 -- ACTIVE_TO_STANDBY has isMutationBlocked()=true.
MutualExclusion ==
    ~(\E c1, c2 \in Cluster :
        \* Two distinct clusters ...
        /\ c1 # c2
        \* ... both in the ACTIVE role.
        /\ RoleOf(clusterState[c1]) = "ACTIVE"
        /\ RoleOf(clusterState[c2]) = "ACTIVE")

---------------------------------------------------------------------------

(*
 * Abort safety: if a cluster is in AbTAIS (ABORT_TO_ACTIVE_IN_SYNC),
 * the peer must be in AbTS, S, or DS.
 *
 * AbTAIS is reached via two paths:
 *   1. Abort path: PeerReactToAbTS (peer = AbTS). The peer can
 *      auto-complete AbTS -> S before the local AbTAIS auto-completes.
 *   2. Reconciliation path: ZKPeerReconnect/ZKPeerSessionRecover
 *      with local = ATS and peer in {S, DS}. DS is reachable when
 *      the peer degraded (S -> DS via PeerReactToANIS) before the
 *      failover partition.
 *
 * All three peer states (AbTS, S, DS) map to STANDBY role, so
 * MutualExclusion is preserved in all cases.
 *
 * The abort protocol is:
 *   (ATS, STA) --[Admin]--> (ATS, AbTS) --[PeerReact]--> (AbTAIS, AbTS)
 *   then auto-complete both sides back to (AIS, S).
 *
 * The reconciliation protocol is:
 *   (ATS, S/DS) --[ZKPeerReconnect]--> (AbTAIS, S/DS)
 *   then auto-complete AbTAIS -> AIS/ANIS.
 *
 * Source: Architecture safety argument; abort originates from
 *         setHAGroupStatusToAbortToStandby() (L419-425) on the
 *         STA side; active detects via FailoverManagementListener
 *         peer AbTS resolver (L132). Reconciliation path is in
 *         ZK.tla (ZKPeerReconnect/ZKPeerSessionRecover).
 *)
AbortSafety ==
    \A c \in Cluster :
        clusterState[c] = "AbTAIS" =>
            clusterState[Peer(c)] \in {"AbTS", "S", "DS", "OFFLINE"}

---------------------------------------------------------------------------

(* Action constraints *)

\* Every state change in every step follows the AllowedTransitions
\* table. This is an action constraint checked by TLC: it verifies
\* that the Next relation only produces transitions that are in the
\* implementation's allowedTransitions set.
\*
\* Source: HAGroupStoreRecord.java L99-123, isTransitionAllowed() L130.
TransitionValid ==
    \A c \in Cluster :
        \* If the state changed for this cluster ...
        clusterState'[c] # clusterState[c] =>
            \* ... then the (old, new) pair must be allowed.
            <<clusterState[c], clusterState'[c]>> \in AllowedTransitions

---------------------------------------------------------------------------

(*
 * Every writer mode change follows the allowed writer transitions.
 * Action constraint checked by TLC analogous to TransitionValid.
 *
 * The X -> INIT transitions (SYNC, STORE_AND_FWD, SYNC_AND_FWD)
 * model the replication subsystem restart on ATS -> S (standby
 * entry). These are lifecycle resets, not ReplicationLogGroup
 * mode CAS transitions: the entire ReplicationLogGroup is
 * destroyed when the cluster becomes standby.
 *
 * Source: ReplicationLogGroup.java mode transitions;
 *         FailoverManagementListener replication subsystem restart.
 *
 * AllowedWriterTransitions is defined in Types.tla.
 *)
WriterTransitionValid ==
    \A c \in Cluster :
        \A r \in RS :
            writerMode'[c][r] # writerMode[c][r] =>
                <<writerMode[c][r], writerMode'[c][r]>> \in AllowedWriterTransitions

---------------------------------------------------------------------------

(*
 * AIS-to-ATS precondition: failover can only begin from AIS when
 * the OUT directory is empty and all live RS are in SYNC mode.
 *
 * DEAD RSes are allowed: an RS can crash while the cluster is AIS
 * without changing the HA group state. A DEAD RS is not writing,
 * so the remaining SYNC RSes and empty OUT dir ensure safety.
 * The implementation checks clusterState = AIS, not per-RS modes.
 *
 * Source: initiateFailoverOnActiveCluster() L375-400 (validates
 *         current state is AIS or ANIS); the precondition holds
 *         because AIS is only reachable when OUT dir is empty and
 *         all writers have returned to SYNC. RS crash does not
 *         change clusterState.
 *)
AIStoATSPrecondition ==
    \A c \in Cluster :
        clusterState[c] = "AIS" /\ clusterState'[c] = "ATS"
        => outDirEmpty[c] /\ \A r \in RS : writerMode[c][r] \in {"SYNC", "DEAD"}

---------------------------------------------------------------------------

(*
 * Anti-flapping gate: ANIS -> AIS never fires while the countdown
 * timer is still running. This is a cross-check on the ANISToAIS
 * action's AntiFlapGateOpen guard, analogous to how AIStoATS-
 * Precondition cross-checks AdminStartFailover.
 *
 * Source: HAGroupStoreClient.validateTransitionAndGetWaitTime()
 *         L1027-1046
 *)
AntiFlapGate ==
    \A c \in Cluster :
        clusterState[c] = "ANIS" /\ clusterState'[c] = "AIS"
        => AntiFlapGateOpen(antiFlapTimer[c])

---------------------------------------------------------------------------

(*
 * ANISTS-to-ATS precondition: the ANISTS -> ATS transition
 * (forwarder drain completion during ANIS failover) can only
 * proceed when the OUT directory is empty and the anti-flapping
 * gate is open. Cross-checks the ANISTSToATS action's guards,
 * analogous to how AIStoATSPrecondition cross-checks
 * AdminStartFailover and AntiFlapGate cross-checks ANISToAIS.
 *
 * Source: HAGroupStoreManager.setHAGroupStatusToSync() L341-355;
 *         HAGroupStoreClient.validateTransitionAndGetWaitTime()
 *         L1027-1046.
 *)
ANISTStoATSPrecondition ==
    \A c \in Cluster :
        clusterState[c] = "ANISTS" /\ clusterState'[c] = "ATS"
        => /\ outDirEmpty[c]
           /\ AntiFlapGateOpen(antiFlapTimer[c])

---------------------------------------------------------------------------

(*
 * Failover trigger correctness: STA -> AIS requires replay-
 * completeness conditions. Cross-checks the TriggerFailover
 * action's guards -- if TLC finds a step where STA->AIS happens
 * without the required conditions, the action constraint fires.
 *
 * hdfsAvailable is excluded: it is an environmental/liveness
 * guard (without HDFS, the action cannot fire), not a replay-
 * completeness condition.
 *
 * Source: shouldTriggerFailover() L500-533 (implementation guards)
 *)
FailoverTriggerCorrectness ==
    \A c \in Cluster :
        clusterState[c] = "STA" /\ clusterState'[c] = "AIS"
        => STAtoAISTriggerReplayGuards(c)

---------------------------------------------------------------------------

(*
 * No data loss (zero RPO): the high-level safety property for
 * failover. When the standby completes STA -> AIS, replay must
 * have been in SYNC (no pending SYNCED_RECOVERY rewind), the
 * in-progress directory must be empty, and the failover must
 * have been properly initiated.
 *)
NoDataLoss ==
    \A c \in Cluster :
        clusterState[c] = "STA" /\ clusterState'[c] = "AIS"
        => STAtoAISTriggerReplayGuards(c)

---------------------------------------------------------------------------

(*
 * Replay rewind correctness: the SYNCED_RECOVERY -> SYNC
 * transition (ReplayRewind CAS) equalizes the replay counters.
 *
 * After a degradation period, lastRoundProcessed can advance
 * beyond lastRoundInSync (ReplayAdvance in DEGRADED only
 * increments lastRoundProcessed). When the cluster recovers
 * (DS -> S or ATS -> S), recoveryListener sets replayState to
 * SYNCED_RECOVERY. ReplayRewind then resets lastRoundProcessed
 * back to lastRoundInSync, ensuring re-processing of all rounds
 * from the last known in-sync point before replay resumes in
 * SYNC mode.
 *
 * This property verifies the mechanism; NoDataLoss verifies the
 * safety outcome. Together they guarantee zero RPO: the rewind
 * closes the counter gap, and TriggerFailover (which requires
 * replayState = SYNC) cannot fire until the rewind completes.
 *
 * Source: replay() L323-333 -- compareAndSet(SYNCED_RECOVERY, SYNC);
 *         getFirstRoundToProcess() L389 -- rewinds to lastRoundInSync
 *)
ReplayRewindCorrectness ==
    \A c \in Cluster :
        replayState[c] = "SYNCED_RECOVERY" /\ replayState'[c] = "SYNC"
        => lastRoundProcessed'[c] = lastRoundInSync'[c]

---------------------------------------------------------------------------

(*
 * Every replay state change follows the allowed replay transitions.
 * Action constraint checked by TLC analogous to TransitionValid
 * and WriterTransitionValid.
 *
 * Source: ReplicationLogDiscoveryReplay.java L131-206 (listeners),
 *         L323-333 (CAS), L336-351 (replay loop)
 *
 * AllowedReplayTransitions is defined in Types.tla.
 *)
ReplayTransitionValid ==
    \A c \in Cluster :
        replayState'[c] # replayState[c] =>
            <<replayState[c], replayState'[c]>> \in AllowedReplayTransitions

---------------------------------------------------------------------------

(*
 * AIS implies in-sync: whenever a cluster is in AIS, the OUT
 * directory must be empty and all RS must be in SYNC, INIT, or
 * DEAD.
 *
 * DEAD is allowed because an RS can crash while the cluster is
 * AIS. RSCrash sets writerMode to DEAD but does not change
 * clusterState. The HA group state in ZK is independent of RS
 * process lifecycle.
 *)
AISImpliesInSync ==
    \A c \in Cluster :
        clusterState[c] = "AIS" =>
            /\ outDirEmpty[c]
            /\ \A r \in RS : writerMode[c][r] \in {"INIT", "SYNC", "DEAD"}

---------------------------------------------------------------------------

(*
 * Writer-cluster consistency: degraded writer modes (S&F,
 * SYNC_AND_FWD) can only appear on active clusters that are
 * NOT in AIS, on the ANISTS/ATS transitional states, or on
 * abort states where HDFS failure can degrade writers.
 *
 * AIS is excluded: prevented by the AIS->ANIS coupling
 * (WriterToStoreFwd, WriterInitToStoreFwd atomically transition
 * AIS -> ANIS when a writer degrades).
 *
 * ATS is included: the AIS failover path enters ATS with all
 * writers in SYNC/DEAD (AdminStartFailover guard), but the ANIS
 * failover path enters ATS via ANISTSToATS which does NOT snap
 * writer modes -- SYNC_AND_FWD writers persist into ATS. Also,
 * if HDFS goes down during ATS, WriterSyncFwdToStoreFwd can
 * re-degrade S&FWD writers to S&F. These degraded writers are
 * cleaned up on ATS -> S (replication subsystem restart).
 *
 * Standby states (S, DS, AbTS) are excluded: live writer modes
 * are reset to INIT on ATS -> S entry (PeerReactToAIS,
 * PeerReactToANIS lifecycle reset). DEAD writers are preserved
 * through ATS -> S (crashed RSes cannot process state change
 * notifications) and handled by RSRestart independently.
 *
 * DEAD is excluded from this check because RSCrash can set
 * writerMode to DEAD on any cluster state (an RS can crash
 * at any time). DEAD writers from CAS failure also appear
 * only on non-AIS active states, but RSCrash is unconstrained.
 *
 * The allowed set includes AbTAIS and AWOP because HDFS can go
 * down while the cluster is in these states; the AIS->ANIS
 * coupling only fires for AIS, so other active states retain
 * their state while writers degrade.
 *)
WriterClusterConsistency ==
    \A c \in Cluster :
        (\E r \in RS : writerMode[c][r] \in {"STORE_AND_FWD", "SYNC_AND_FWD"}) =>
            clusterState[c] \in {"ANIS", "ANISTS", "ATS", "ANISWOP", "AbTANIS", "AbTAIS", "AWOP"}

---------------------------------------------------------------------------

(* State constraint *)

\* Bound replay counters for exhaustive search tractability.
\* The abstract counter values only matter relationally
\* (lastRoundProcessed >= lastRoundInSync), so small bounds suffice.
ReplayCounterBound ==
    \A c \in Cluster : lastRoundProcessed[c] <= 3

---------------------------------------------------------------------------

(* Symmetry *)

\* RS identifiers are interchangeable (all start in INIT, identical
\* action sets). Cluster identifiers remain asymmetric (AIS vs S).
Symmetry == Permutations(RS)

---------------------------------------------------------------------------

(* Theorems *)

\* Safety: all variables have valid types.
THEOREM Spec => []TypeOK

\* Safety: mutual exclusion holds in every reachable state.
THEOREM Spec => []MutualExclusion

\* Safety: abort is always initiated from the correct side.
THEOREM Spec => []AbortSafety

\* Safety: AIS implies in-sync (derived invariant).
THEOREM Spec => []AISImpliesInSync

\* Safety: degraded writer modes only on degraded-active clusters.
THEOREM Spec => []WriterClusterConsistency

\* Safety: ZK session/connection consistency (session expiry implies disconnection).
THEOREM Spec => []ZKSessionConsistency

\* Safety: ANISTS->ATS requires outDirEmpty and anti-flapping gate open (action property).
THEOREM Spec => [][ANISTStoATSPrecondition]_vars

\* Safety: STA->AIS requires replay-completeness conditions (action property).
THEOREM Spec => [][FailoverTriggerCorrectness]_vars

\* Safety: zero RPO -- no data loss on failover (action property).
THEOREM Spec => [][NoDataLoss]_vars

\* Safety: replay rewind equalizes counters (action property).
THEOREM Spec => [][ReplayRewindCorrectness]_vars

\* Liveness: failover/abort transient states eventually resolve.
THEOREM SpecFC => FailoverCompletion

\* Liveness: ANIS with available HDFS eventually recovers.
THEOREM SpecDR => DegradationRecovery

\* Liveness: abort states eventually auto-complete.
THEOREM SpecAC => AbortCompletion

============================================================================
