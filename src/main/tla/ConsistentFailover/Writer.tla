-------------------------- MODULE Writer ----------------------------------------
(*
 * Replication writer mode state machine for the Phoenix Consistent
 * Failover specification.
 *
 * Each RegionServer on the active cluster maintains a writer mode
 * that determines how mutations are replicated: directly to standby
 * HDFS (SYNC), locally buffered (STORE_AND_FWD), or draining local
 * queue while also writing synchronously (SYNC_AND_FWD). An RS that
 * aborts due to a ZK CAS failure enters DEAD mode.
 *
 * HDFS-failure-driven degradation (SYNC -> S&F, SYNC_AND_FWD -> S&F)
 * is modeled as individual per-RS actions that each perform their
 * own ZK CAS write. HDFSDown in HDFS.tla only sets the availability
 * flag; per-RS degradation and CAS failure are handled here.
 *
 * CAS FAILURE SEMANTICS: When an RS detects HDFS unavailability via
 * IOException, it attempts a ZK CAS write (setData().withVersion())
 * to transition AIS->ANIS (or ANIS->ANIS self-transition). If another
 * RS has already bumped the ZK version (stale PathChildrenCache),
 * BadVersionException is thrown. SyncModeImpl.onFailure() and
 * SyncAndForwardModeImpl.onFailure() treat this as fatal: abort()
 * throws RuntimeException, halting the Disruptor -- the RS is dead.
 * CAS failure is only possible when clusterState /= "AIS" because
 * the first RS to write faces no concurrent version bump.
 *
 * ZK LOCAL CONNECTIVITY: Actions that perform ZK writes
 * (setHAGroupStatusToStoreAndForward, setHAGroupStatusToSync)
 * require isHealthy = true, modeled by the zkLocalConnected[c]
 * guard. Actions that are purely mode transitions driven by HDFS
 * operations or forwarder events (WriterInit, WriterSyncToSyncFwd,
 * WriterStoreFwdToSyncFwd) do NOT require a ZK connection.
 *
 * Implementation traceability:
 *
 *   TLA+ action                      | Java source
 *   ---------------------------------+----------------------------------------
 *   WriterInit(c, rs)                | Normal startup -> SyncModeImpl
 *   WriterInitToStoreFwd(c, rs)      | Startup with peer unavailable ->
 *                                    |   StoreAndForwardModeImpl; CAS
 *                                    |   success path
 *   WriterInitToStoreFwdFail(c, rs)  | Startup CAS failure -> abort
 *   WriterToStoreFwd(c, rs)          | SyncModeImpl.onFailure() L61-74 ->
 *                                    |   setHAGroupStatusToStoreAndForward();
 *                                    |   CAS success path
 *   WriterToStoreFwdFail(c, rs)      | SyncModeImpl.onFailure() CAS
 *                                    |   failure -> abort
 *   WriterSyncToSyncFwd(c, rs)       | Forwarder ACTIVE_NOT_IN_SYNC event
 *                                    |   L98-108 while RS in SYNC
 *   WriterStoreFwdToSyncFwd(c, rs)   | Forwarder processFile() L133-152
 *                                    |   throughput threshold or drain start
 *   WriterSyncFwdToSync(c, rs)       | Forwarder drain complete; queue empty
 *                                    |   -> setHAGroupStatusToSync() L171
 *   WriterSyncFwdToStoreFwd(c, rs)   | SyncAndForwardModeImpl.onFailure()
 *                                    |   L66-78; CAS success path
 *   WriterSyncFwdToStoreFwdFail(c,rs)| SyncAndForwardModeImpl.onFailure()
 *                                    |   CAS failure -> abort
 *   CanDegradeToStoreFwd(c, rs)      | Guard predicate: RS is in a mode
 *                                    |   that writes to standby HDFS
 *)
EXTENDS SpecState, Types

---------------------------------------------------------------------------

(* Predicates *)

(*
 * Guard predicate: RS is in a mode that writes to standby HDFS
 * and would degrade to STORE_AND_FWD on an HDFS failure.
 *
 * Used by the per-RS degradation actions (WriterToStoreFwd,
 * WriterSyncFwdToStoreFwd) and their CAS failure variants.
 *)
CanDegradeToStoreFwd(c, rs) ==
    writerMode[c][rs] \in {"SYNC", "SYNC_AND_FWD"}

---------------------------------------------------------------------------

(* Actions wired into Next *)

(*
 * Normal startup: INIT -> SYNC.
 *
 * RS initializes and begins writing directly to standby HDFS.
 * Writers only run on the active cluster.
 * No ZK write -- pure mode transition.
 *
 * Source: Normal startup -> SyncModeImpl
 *)
WriterInit(c, rs) ==
    /\ clusterState[c] \in ActiveStates
    /\ writerMode[c][rs] = "INIT"
    /\ writerMode' = [writerMode EXCEPT ![c][rs] = "SYNC"]
    /\ UNCHANGED <<clusterVars, replayVars, envVars>>

---------------------------------------------------------------------------

(*
 * Startup with peer unavailable: INIT -> STORE_AND_FWD.
 *
 * RS initializes but standby HDFS is unreachable; begins
 * buffering locally in the OUT directory. Also transitions
 * cluster AIS -> ANIS (setHAGroupStatusToStoreAndForward).
 * Writers only run on the active cluster.
 *
 * AWOP/ANISWOP handling: same as WriterToStoreFwd.
 *
 * Guarded on zkLocalConnected[c] because this calls
 * setHAGroupStatusToStoreAndForward() which requires
 * isHealthy = true.
 *
 * Source: StoreAndForwardModeImpl.onEnter() L54-64
 *)
WriterInitToStoreFwd(c, rs) ==
    /\ LocalZKHealthy(c)
    /\ clusterState[c] \in ActiveStates
    /\ writerMode[c][rs] = "INIT"
    /\ hdfsAvailable[Peer(c)] = FALSE
    /\ writerMode' = [writerMode EXCEPT ![c][rs] = "STORE_AND_FWD"]
    /\ outDirEmpty' = [outDirEmpty EXCEPT ![c] = FALSE]
    /\ clusterState' = IF clusterState[c] \in AISLikeStates
                        THEN [clusterState EXCEPT ![c] = "ANIS"]
                        ELSE clusterState
    /\ antiFlapTimer' = IF clusterState[c] \in AISLikeStates
                         THEN [antiFlapTimer EXCEPT ![c] = StartAntiFlapWait]
                         ELSE antiFlapTimer
    /\ UNCHANGED <<replayVars, envVars,
                   failoverPending, inProgressDirEmpty>>

---------------------------------------------------------------------------

(*
 * Forwarder started while in sync: SYNC -> SYNC_AND_FWD.
 *
 * On an ACTIVE_NOT_IN_SYNC event (L98-108), region servers
 * currently in SYNC learn that the cluster has entered ANIS
 * and transition to SYNC_AND_FWD. This event fires once when
 * the cluster enters ANIS. ANISTS does not produce a new
 * ACTIVE_NOT_IN_SYNC event -- it is a different ZK state
 * change (ACTIVE_NOT_IN_SYNC_TO_STANDBY). A SYNC writer that
 * has not yet received the event when ANIS -> ANISTS fires
 * will remain in SYNC (harmlessly: SYNC writers write directly
 * to standby HDFS, not to the OUT directory).
 * No ZK write -- mode transition driven by forwarder event.
 *
 * Source: ReplicationLogDiscoveryForwarder.init() L98-108
 *)
WriterSyncToSyncFwd(c, rs) ==
    /\ clusterState[c] = "ANIS"
    /\ writerMode[c][rs] = "SYNC"
    /\ writerMode' = [writerMode EXCEPT ![c][rs] = "SYNC_AND_FWD"]
    /\ UNCHANGED <<clusterVars, replayVars, envVars>>

---------------------------------------------------------------------------

(*
 * Recovery detected; standby available again:
 * STORE_AND_FWD -> SYNC_AND_FWD.
 *
 * The forwarder successfully copies a file from the OUT directory
 * to the standby's IN directory. If throughput exceeds the
 * threshold, the writer transitions to SYNC_AND_FWD to begin
 * draining the queue while also writing synchronously.
 * The forwarder runs on active clusters and during the ANISTS
 * transitional state (draining OUT before ANISTS->ATS).
 * No ZK write -- mode transition driven by forwarder file copy.
 *
 * Source: ReplicationLogDiscoveryForwarder.processFile() L133-152
 *         throughput threshold or drain start.
 *)
WriterStoreFwdToSyncFwd(c, rs) ==
    /\ clusterState[c] \in ActiveStates \union TransitionalActiveStates
    /\ writerMode[c][rs] = "STORE_AND_FWD"
    /\ hdfsAvailable[Peer(c)] = TRUE
    /\ writerMode' = [writerMode EXCEPT ![c][rs] = "SYNC_AND_FWD"]
    /\ UNCHANGED <<clusterVars, replayVars, envVars>>

---------------------------------------------------------------------------

(*
 * All stored logs forwarded; queue empty:
 * SYNC_AND_FWD -> SYNC.
 *
 * The forwarder has drained all buffered files from the OUT
 * directory. The OUT directory is now empty.
 * The forwarder runs on active clusters and during the ANISTS
 * transitional state (draining OUT before ANISTS->ATS).
 *
 * Per-RS vs per-cluster semantics: processNoMoreRoundsLeft()
 * (ReplicationLogDiscoveryForwarder.java L155-184) is a per-
 * cluster forwarder check that examines the global OUT directory
 * -- it only fires when the entire OUT directory is empty, not
 * when a single RS finishes. The guard
 * \A rs2 \in RS : writerMode[c][rs2] \notin {"STORE_AND_FWD"}
 * prevents setting outDirEmpty = TRUE while any RS is still
 * actively writing to the OUT directory.
 *
 * HDFS guard: processNoMoreRoundsLeft() can only fire after
 * processFile() has successfully copied all remaining files from
 * OUT to the peer's IN directory, which requires the peer's HDFS
 * to be accessible. If the peer's HDFS is down, processFile()
 * throws IOException and the forwarder retries -- it never
 * reaches processNoMoreRoundsLeft().
 *
 * Guarded on zkLocalConnected[c] because this calls
 * setHAGroupStatusToSync() which requires isHealthy = true.
 *
 * Source: ReplicationLogDiscoveryForwarder.processFile() L133-152
 *         copies to peer HDFS; processNoMoreRoundsLeft() L155-184
 *         only fires after all files are forwarded.
 *         setHAGroupStatusToSync() L171
 *)
WriterSyncFwdToSync(c, rs) ==
    /\ LocalZKHealthy(c)
    /\ clusterState[c] \in ActiveStates \union TransitionalActiveStates
    /\ writerMode[c][rs] = "SYNC_AND_FWD"
    /\ hdfsAvailable[Peer(c)] = TRUE
    /\ \A rs2 \in RS : writerMode[c][rs2] \notin {"STORE_AND_FWD"}
    /\ writerMode' = [writerMode EXCEPT ![c][rs] = "SYNC"]
    /\ outDirEmpty' = [outDirEmpty EXCEPT ![c] = TRUE]
    /\ UNCHANGED <<replayVars, envVars,
                   clusterState, antiFlapTimer,
                   failoverPending, inProgressDirEmpty>>

---------------------------------------------------------------------------

(* Per-RS HDFS failure degradation -- CAS success paths *)

(*
 * Per-RS HDFS failure degradation: SYNC -> STORE_AND_FWD (CAS success).
 *
 * Models a single RS detecting standby HDFS unavailability via
 * IOException and successfully CAS-writing the ZK state. The ZK
 * CAS write is synchronous and happens BEFORE the mode change
 * (SyncModeImpl.onFailure() L61-74). On success, the writer
 * transitions to STORE_AND_FWD and the cluster transitions
 * AIS -> ANIS (if still AIS). Writers only run on the active cluster.
 *
 * AWOP/ANISWOP handling: when AWOP or ANISWOP
 * are reachable (UseOfflinePeerDetection = TRUE), HDFS failure
 * during these states triggers setHAGroupStatusToStoreAndForward()
 * which CAS-writes ANIS. AWOP.allowedTransitions = {ANIS} and
 * ANISWOP.allowedTransitions = {ANIS}, so the transition succeeds.
 * When UseOfflinePeerDetection = FALSE, AWOP/ANISWOP are
 * unreachable and the extended IF is a no-op.
 *
 * Guarded on zkLocalConnected[c] because the CAS write goes through
 * setHAGroupStatusIfNeeded() which requires isHealthy = true.
 *
 * Source: SyncModeImpl.onFailure() L61-74 ->
 *         setHAGroupStatusToStoreAndForward()
 *)
WriterToStoreFwd(c, rs) ==
    /\ LocalZKHealthy(c)
    /\ clusterState[c] \in ActiveStates
    /\ writerMode[c][rs] = "SYNC"
    /\ hdfsAvailable[Peer(c)] = FALSE
    /\ writerMode' = [writerMode EXCEPT ![c][rs] = "STORE_AND_FWD"]
    /\ outDirEmpty' = [outDirEmpty EXCEPT ![c] = FALSE]
    /\ clusterState' = IF clusterState[c] \in AISLikeStates
                        THEN [clusterState EXCEPT ![c] = "ANIS"]
                        ELSE clusterState
    /\ antiFlapTimer' = IF clusterState[c] \in AISLikeStates
                         THEN [antiFlapTimer EXCEPT ![c] = StartAntiFlapWait]
                         ELSE antiFlapTimer
    /\ UNCHANGED <<replayVars, envVars,
                   failoverPending, inProgressDirEmpty>>

---------------------------------------------------------------------------

(*
 * Re-degradation during drain: SYNC_AND_FWD -> STORE_AND_FWD
 * (CAS success).
 *
 * Models standby HDFS becoming unavailable again while the
 * forwarder is draining the local queue. The RS falls back to
 * pure local buffering. The forwarder runs on active clusters
 * and during the ANISTS transitional state.
 * No AIS -> ANIS coupling needed: if RS is in SYNC_AND_FWD,
 * cluster is already ANIS or ANISTS (cannot be AIS).
 *
 * Guarded on zkLocalConnected[c] because the CAS write goes through
 * setHAGroupStatusIfNeeded() which requires isHealthy = true.
 *
 * Source: SyncAndForwardModeImpl.onFailure() L66-78 ->
 *         setHAGroupStatusToStoreAndForward()
 *)
WriterSyncFwdToStoreFwd(c, rs) ==
    /\ LocalZKHealthy(c)
    /\ clusterState[c] \in ActiveStates \union TransitionalActiveStates
    /\ writerMode[c][rs] = "SYNC_AND_FWD"
    /\ hdfsAvailable[Peer(c)] = FALSE
    /\ writerMode' = [writerMode EXCEPT ![c][rs] = "STORE_AND_FWD"]
    /\ outDirEmpty' = [outDirEmpty EXCEPT ![c] = FALSE]
    /\ UNCHANGED <<replayVars, envVars,
                   clusterState, antiFlapTimer,
                   failoverPending, inProgressDirEmpty>>

---------------------------------------------------------------------------

(* Per-RS HDFS failure degradation -- CAS failure paths (RS abort) *)

(*
 * CAS failure during SYNC degradation: SYNC -> DEAD.
 *
 * RS detects IOException, reads stale AIS/version N from
 * PathChildrenCache, attempts CAS write AIS->ANIS with version N,
 * but another RS already bumped the version to N+1. ZK throws
 * BadVersionException -> StaleHAGroupStoreRecordVersionException ->
 * abort() -> RuntimeException -> Disruptor halts -> RS dead.
 *
 * Guard: clusterState[c] /= "AIS" -- CAS failure is only possible
 * when another RS has already changed the cluster state, meaning
 * the ZK version has been bumped beyond the cached value.
 *
 * Guarded on zkLocalConnected[c] because the CAS write requires
 * a live ZK connection (isHealthy = true).
 *
 * Source: SyncModeImpl.onFailure() L61-74 catch block -> abort()
 *)
WriterToStoreFwdFail(c, rs) ==
    /\ LocalZKHealthy(c)
    /\ clusterState[c] \in ActiveStates \ {"AIS"}
    /\ writerMode[c][rs] = "SYNC"
    /\ hdfsAvailable[Peer(c)] = FALSE
    /\ writerMode' = [writerMode EXCEPT ![c][rs] = "DEAD"]
    /\ UNCHANGED <<clusterVars, replayVars, envVars>>

---------------------------------------------------------------------------

(*
 * CAS failure during SYNC_AND_FWD re-degradation:
 * SYNC_AND_FWD -> DEAD.
 *
 * Same CAS failure pattern as WriterToStoreFwdFail but from
 * SYNC_AND_FWD mode. If RS is in SYNC_AND_FWD, the cluster is
 * already ANIS or ANISTS (not AIS), so another RS or the S&F
 * heartbeat may have bumped the ZK version.
 *
 * Guarded on zkLocalConnected[c] because the CAS write requires
 * a live ZK connection (isHealthy = true).
 *
 * Source: SyncAndForwardModeImpl.onFailure() L66-78 catch block
 *         -> abort()
 *)
WriterSyncFwdToStoreFwdFail(c, rs) ==
    /\ LocalZKHealthy(c)
    /\ clusterState[c] \in ActiveStates \union TransitionalActiveStates
    /\ writerMode[c][rs] = "SYNC_AND_FWD"
    /\ hdfsAvailable[Peer(c)] = FALSE
    /\ writerMode' = [writerMode EXCEPT ![c][rs] = "DEAD"]
    /\ UNCHANGED <<clusterVars, replayVars, envVars>>

---------------------------------------------------------------------------

(*
 * CAS failure during init degradation: INIT -> DEAD.
 *
 * RS starts up, SyncModeImpl.onEnter() fails (HDFS unavailable),
 * updateModeOnFailure -> SyncModeImpl.onFailure() -> CAS write
 * fails -> abort(). Same CAS race as WriterToStoreFwdFail but
 * from the INIT state during startup.
 *
 * Guard: clusterState[c] /= "AIS" -- same rationale: another RS
 * must have already bumped the version for CAS to fail.
 *
 * Guarded on zkLocalConnected[c] because the CAS write requires
 * a live ZK connection (isHealthy = true).
 *
 * Source: SyncModeImpl.onFailure() L61-74 via
 *         LogEventHandler.initializeMode() failure path
 *)
WriterInitToStoreFwdFail(c, rs) ==
    /\ LocalZKHealthy(c)
    /\ clusterState[c] \in ActiveStates \ {"AIS"}
    /\ writerMode[c][rs] = "INIT"
    /\ hdfsAvailable[Peer(c)] = FALSE
    /\ writerMode' = [writerMode EXCEPT ![c][rs] = "DEAD"]
    /\ UNCHANGED <<clusterVars, replayVars, envVars>>

============================================================================
