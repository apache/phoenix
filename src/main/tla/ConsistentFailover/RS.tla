------------------------------ MODULE RS ----------------------------------------
(*
 * RegionServer lifecycle actions for the Phoenix Consistent
 * Failover specification.
 *
 * Models RS crash (fail-stop) and process supervisor restart.
 *
 * Crash modeling: An RS can crash at any time (JVM crash, OOM, kill
 * signal, process supervisor termination). The crash sets writerMode
 * to DEAD but does not change clusterState -- the HA group state in
 * ZK is independent of RS process lifecycle. A special-case crash,
 * RSAbortOnLocalHDFSFailure, models the abort triggered when the
 * active cluster's own HDFS fails while the writer is in
 * STORE_AND_FWD mode (writing to local HDFS).
 *
 * Restart modeling: When an RS dies (writer mode DEAD), the process
 * supervisor (Kubernetes/YARN) detects the dead pod and creates a
 * new one. HBase assigns regions and the writer re-initializes in
 * INIT mode, ready to follow the normal startup path (WriterInit
 * or WriterInitToStoreFwd).
 *
 * Implementation traceability:
 *
 *   TLA+ action                     | Java source
 *   --------------------------------+----------------------------------
 *   RSCrash(c, rs)                  | JVM crash, OOM, kill signal,
 *                                   |   process supervisor termination
 *   RSAbortOnLocalHDFSFailure(c,rs) | StoreAndForwardModeImpl
 *                                   |   .onFailure() L115-123 ->
 *                                   |   logGroup.abort()
 *   RSRestart(c, rs)                | Kubernetes/YARN pod restart ->
 *                                   |   HBase RS startup ->
 *                                   |   ReplicationLogGroup
 *                                   |   .initializeReplicationMode()
 *                                   |   -> setMode(SYNC) or
 *                                   |   setMode(STORE_AND_FORWARD)
 *)
EXTENDS SpecState, Types

---------------------------------------------------------------------------

(*
 * Process supervisor restarts a dead RS: DEAD -> INIT.
 *
 * The restarted RS enters INIT mode. Subsequent writer actions
 * (WriterInit or WriterInitToStoreFwd) handle the actual mode
 * initialization based on HDFS availability and cluster state.
 *
 * Pre:  writerMode[c][rs] = "DEAD".
 * Post: writerMode[c][rs] = "INIT".
 *
 * Source: Kubernetes/YARN pod restart -> HBase RS startup ->
 *         ReplicationLogGroup.initializeReplicationMode()
 *)
RSRestart(c, rs) ==
    /\ writerMode[c][rs] = "DEAD"
    /\ writerMode' = [writerMode EXCEPT ![c][rs] = "INIT"]
    /\ UNCHANGED <<clusterVars, replayVars, envVars>>

---------------------------------------------------------------------------

(*
 * Non-deterministic RS crash: any mode -> DEAD.
 *
 * Models general RS failure (JVM crash, OOM, killed by process
 * supervisor, etc.). The RS can crash at any time regardless of
 * writer mode. The crash does not change clusterState -- the HA
 * group state in ZK is independent of RS process lifecycle.
 *
 * Pre:  writerMode[c][rs] /= "DEAD" (RS is alive).
 * Post: writerMode[c][rs] = "DEAD".
 *
 * Source: JVM crash, OOM, kill signal, process supervisor
 *         termination -- environment event.
 *)
RSCrash(c, rs) ==
    /\ writerMode[c][rs] /= "DEAD"
    /\ writerMode' = [writerMode EXCEPT ![c][rs] = "DEAD"]
    /\ UNCHANGED <<clusterVars, replayVars, envVars>>

---------------------------------------------------------------------------

(*
 * RS abort on local HDFS failure: STORE_AND_FWD -> DEAD.
 *
 * In STORE_AND_FWD mode, the writer targets the active cluster's
 * own (local/fallback) HDFS. If that HDFS fails,
 * StoreAndForwardModeImpl.onFailure() treats the error as fatal
 * and calls logGroup.abort(), killing the RS.
 *
 * This is distinct from HDFSDown(c) (which models the *peer's*
 * HDFS failing and degrades writers on the active side):
 * RSAbortOnLocalHDFSFailure models the active cluster's *own*
 * HDFS failing while the RS is already in fallback mode.
 *
 * Note: hdfsAvailable[c] is the cluster's OWN HDFS, not Peer(c).
 * RS in SYNC or SYNC_AND_FWD write to the peer's HDFS, so they
 * are not affected by their own cluster's HDFS failure.
 *
 * Depends on HDFSDown in HDFS.tla allowing any cluster's HDFS to
 * fail, so that hdfsAvailable[c] = FALSE is reachable for active
 * clusters.
 *
 * Pre:  writerMode[c][rs] = "STORE_AND_FWD" and
 *       hdfsAvailable[c] = FALSE (own HDFS is down).
 * Post: writerMode[c][rs] = "DEAD".
 *
 * Source: StoreAndForwardModeImpl.onFailure() L115-123 ->
 *         logGroup.abort()
 *)
RSAbortOnLocalHDFSFailure(c, rs) ==
    /\ writerMode[c][rs] = "STORE_AND_FWD"
    /\ hdfsAvailable[c] = FALSE
    /\ writerMode' = [writerMode EXCEPT ![c][rs] = "DEAD"]
    /\ UNCHANGED <<clusterVars, replayVars, envVars>>

============================================================================
