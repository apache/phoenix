-------------------------- MODULE Admin ----------------------------------------
(*
 * Operator-initiated actions for the Phoenix Consistent Failover
 * protocol: failover initiation, abort, and OFFLINE lifecycle.
 *
 * These actions model the human operator (Admin actor) who drives
 * failover and abort via the PhoenixHAAdminTool CLI, which delegates
 * to HAGroupStoreManager coprocessor endpoints.
 *
 * AdminGoOffline and AdminForceRecover are gated on the
 * UseOfflinePeerDetection feature flag and model
 * the proactive design for peer OFFLINE detection. These actions
 * use PhoenixHAAdminTool update with --force, bypassing the
 * normal isTransitionAllowed() check.
 *
 * Implementation traceability:
 *
 *   TLA+ action              | Java source
 *   -------------------------+----------------------------------------------
 *   AdminStartFailover(c)    | HAGroupStoreManager
 *                            |   .initiateFailoverOnActiveCluster() L375-400
 *   AdminAbortFailover(c)    | HAGroupStoreManager
 *                            |   .setHAGroupStatusToAbortToStandby() L419-425
 *                            |   Also clears failoverPending (models
 *                            |   abortFailoverListener L173-185)
 *   AdminGoOffline(c)        | PhoenixHAAdminTool update --state OFFLINE
 *                            |   (gated on UseOfflinePeerDetection)
 *   AdminForceRecover(c)     | PhoenixHAAdminTool update --force
 *                            |   --state STANDBY (OFFLINE -> S)
 *                            |   (gated on UseOfflinePeerDetection)
 *)
EXTENDS SpecState, Types

---------------------------------------------------------------------------

(*
 * Admin initiates failover on the active cluster.
 *
 * Two paths depending on current state:
 *
 *   AIS path:  AIS -> ATS   (in-sync, ready to hand off immediately)
 *   ANIS path: ANIS -> ANISTS (not-in-sync, forwarder must drain OUT
 *              before ANISTS can advance to ATS)
 *
 * AIS path guards:
 *   The OUT directory must be empty and all live RS must be in SYNC
 *   mode. DEAD RSes are allowed -- an RS can crash while the cluster
 *   is AIS without changing the HA group state. The implementation
 *   checks clusterState = AIS, not per-RS modes; a DEAD RS is not
 *   writing, so the remaining SYNC RSes and empty OUT dir ensure
 *   safety.
 *
 * ANIS path guards:
 *   The implementation only validates the current state (ANIS) and
 *   peer state. No outDirEmpty or writer-mode guards -- the
 *   forwarder will drain OUT after the transition. The ANISTS ->
 *   ATS transition (ANISTSToATS in HAGroupStore.tla) guards on
 *   outDirEmpty and the anti-flapping gate.
 *
 * Both paths:
 *   Peer must be in a stable standby state (S or DS) to prevent
 *   initiating a new failover during the non-atomic window of a
 *   previous failover (where the peer may still be in ATS).
 *   Without this guard, the admin could produce an irrecoverable
 *   (ATS, ATS) or (ANISTS, ATS) deadlock.
 *
 * Post: Cluster c transitions to ATS or ANISTS, both of which
 *       map to the ACTIVE_TO_STANDBY role, blocking mutations
 *       (isMutationBlocked()=true).
 *
 * Source: initiateFailoverOnActiveCluster() L375-400 checks current
 *         state and selects AIS -> ATS or ANIS -> ANISTS.
 *         Peer-state guard: getHAGroupStoreRecordFromPeer()
 *         (HAGroupStoreClient L421).
 *)
AdminStartFailover(c) ==
    /\ clusterState[Peer(c)] \in {"S", "DS"}
    /\ \/ /\ clusterState[c] = "AIS"
          /\ outDirEmpty[c]
          /\ \A rs \in RS : writerMode[c][rs] \in {"SYNC", "DEAD"}
          /\ clusterState' = [clusterState EXCEPT ![c] = "ATS"]
       \/ /\ clusterState[c] = "ANIS"
          /\ clusterState' = [clusterState EXCEPT ![c] = "ANISTS"]
    /\ UNCHANGED <<writerVars, replayVars, envVars,
                   outDirEmpty, antiFlapTimer,
                   failoverPending, inProgressDirEmpty>>

---------------------------------------------------------------------------

(*
 * Admin aborts an in-progress failover from the standby side.
 *
 * Pre:  Cluster c is in STA (STANDBY_TO_ACTIVE).
 * Post: Cluster c transitions to AbTS (ABORT_TO_STANDBY).
 *       The peer (in ATS) will react via PeerReactToAbTS,
 *       transitioning to AbTAIS. Both then auto-complete back
 *       to their pre-failover states.
 *
 * Abort must originate from the STA side to prevent dual-active
 * races -- this is the AbortSafety property.
 *
 * Also clears failoverPending[c], modeling the abortFailoverListener
 * (ReplicationLogDiscoveryReplay.java L173-185) which fires on LOCAL
 * ABORT_TO_STANDBY, calling failoverPending.set(false).
 *
 * Source: setHAGroupStatusToAbortToStandby() L419-425.
 *)
AdminAbortFailover(c) ==
    /\ clusterState[c] = "STA"
    /\ clusterState' = [clusterState EXCEPT ![c] = "AbTS"]
    /\ failoverPending' = [failoverPending EXCEPT ![c] = FALSE]
    /\ UNCHANGED <<writerVars, replayVars, envVars,
                   outDirEmpty, antiFlapTimer, inProgressDirEmpty>>

---------------------------------------------------------------------------

(*
 * Admin takes a standby cluster offline.
 *
 * Gated on UseOfflinePeerDetection (Iteration 18, proactive modeling).
 *
 * Pre:  Cluster c is in S or DS (a standby state).
 * Post: Cluster c transitions to OFFLINE.
 *
 * In the implementation, entering OFFLINE requires
 * PhoenixHAAdminTool update --force --state OFFLINE, which
 * bypasses isTransitionAllowed(). The operator decides when
 * to take a cluster offline for maintenance or decommissioning.
 *
 * No ZK connectivity guard: the --force path writes directly
 * to ZK, bypassing the isHealthy check used by
 * setHAGroupStatusIfNeeded().
 *
 * Source: PhoenixHAAdminTool update --state OFFLINE (--force)
 *)
AdminGoOffline(c) ==
    /\ UseOfflinePeerDetection = TRUE
    /\ clusterState[c] \in {"S", "DS"}
    /\ clusterState' = [clusterState EXCEPT ![c] = "OFFLINE"]
    /\ UNCHANGED <<writerVars, replayVars, envVars,
                   outDirEmpty, antiFlapTimer,
                   failoverPending, inProgressDirEmpty>>

---------------------------------------------------------------------------

(*
 * Admin force-recovers a cluster from OFFLINE.
 *
 * Gated on UseOfflinePeerDetection.
 *
 * Pre:  Cluster c is in OFFLINE.
 * Post: Cluster c transitions to S (STANDBY).
 *
 * Recovery from OFFLINE requires PhoenixHAAdminTool update --force
 * --state STANDBY, which bypasses isTransitionAllowed() (OFFLINE
 * has no allowed outbound transitions in the implementation).
 *
 * The S-entry side effects mirror the pattern used by
 * PeerReactToAIS (ATS->S) and AutoComplete (AbTS->S):
 *   - writerMode reset to INIT for all RS (replication subsystem
 *     restart on standby entry)
 *   - outDirEmpty set to TRUE (OUT directory cleared)
 *   - replayState set to SYNCED_RECOVERY (recoveryListener fold)
 *
 * No ZK connectivity guard: the --force path writes directly
 * to ZK.
 *
 * Source: PhoenixHAAdminTool update --force --state STANDBY
 *)
AdminForceRecover(c) ==
    /\ UseOfflinePeerDetection = TRUE
    /\ clusterState[c] = "OFFLINE"
    /\ clusterState' = [clusterState EXCEPT ![c] = "S"]
    /\ writerMode' = [writerMode EXCEPT ![c] =
            [rs \in RS |-> "INIT"]]
    /\ outDirEmpty' = [outDirEmpty EXCEPT ![c] = TRUE]
    /\ replayState' = [replayState EXCEPT ![c] = "SYNCED_RECOVERY"]
    /\ UNCHANGED <<envVars,
                   antiFlapTimer, failoverPending, inProgressDirEmpty,
                   lastRoundInSync, lastRoundProcessed>>

============================================================================
