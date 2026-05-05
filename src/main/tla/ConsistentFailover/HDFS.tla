---------------------------- MODULE HDFS ------------------------------------------
(*
 * HDFS availability incident actions for the Phoenix Consistent
 * Failover specification.
 *
 * Models NameNode crash and recovery as environment incidents.
 * HDFSDown(c) sets the availability flag to FALSE; per-RS writer
 * degradation and ZK CAS writes are handled individually by
 * WriterToStoreFwd / WriterSyncFwdToStoreFwd in Writer.tla.
 * This decomposition enables modeling of the ZK CAS race where
 * multiple RS on the same cluster race to update the ZK state
 * and the loser gets BadVersionException -> abort.
 *
 * Recovery is asymmetric: HDFSUp(c) only sets the availability
 * flag. Per-RS recovery happens gradually via the forwarder path
 * (WriterStoreFwdToSyncFwd), which is guarded on hdfsAvailable.
 *
 * Implementation traceability:
 *
 *   TLA+ action     | Java source
 *   ----------------+------------------------------------------------
 *   HDFSDown(c)     | NameNode crash; detected reactively via
 *                   |   IOException from ReplicationLog.apply()
 *   HDFSUp(c)       | NameNode recovery; forwarder detects via
 *                   |   successful FileUtil.copy() in processFile()
 *                   |   L132-152
 *)
EXTENDS SpecState, Types

---------------------------------------------------------------------------

(*
 * NameNode of cluster c crashes.
 *
 * Sets the HDFS availability flag to FALSE. Per-RS writer
 * degradation (SYNC -> S&F, SYNC_AND_FWD -> S&F) is handled
 * individually by WriterToStoreFwd and WriterSyncFwdToStoreFwd
 * in Writer.tla, which are guarded on hdfsAvailable[Peer(c)]
 * = FALSE. Those actions also handle the AIS -> ANIS cluster
 * state transition and CAS failure (-> DEAD).
 *
 * Any cluster's HDFS can fail at any time. Two consequences:
 *   1. HDFSDown(c_standby): standby HDFS fails -> active writers
 *      detect via IOException and degrade (SYNC -> S&F).
 *   2. HDFSDown(c_active): active cluster's own HDFS fails ->
 *      S&F writers on the active cluster abort (modeled by
 *      RSAbortOnLocalHDFSFailure in RS.tla).
 *
 * Pre:  c's HDFS is currently available.
 * Post: hdfsAvailable[c] = FALSE.
 *       All other variables unchanged -- per-RS effects deferred
 *       to writer actions (case 1) or RS.tla (case 2).
 *
 * Source: NameNode crash (environment event)
 *)
HDFSDown(c) ==
    /\ hdfsAvailable[c] = TRUE
    /\ hdfsAvailable' = [hdfsAvailable EXCEPT ![c] = FALSE]
    /\ UNCHANGED <<writerVars, clusterVars, replayVars,
                   zkPeerConnected, zkPeerSessionAlive, zkLocalConnected>>

---------------------------------------------------------------------------

(*
 * NameNode of cluster c recovers.
 *
 * Sets hdfsAvailable[c] = TRUE. No immediate writer effect --
 * recovery is per-RS via the forwarder path. The forwarder
 * detects connectivity by successfully copying a file from
 * OUT to the peer's IN directory; if throughput exceeds the
 * threshold, it transitions the writer S&F -> SYNC_AND_FWD.
 *
 * Pre:  c's HDFS is currently unavailable.
 * Post: hdfsAvailable[c] = TRUE. Writer modes unchanged.
 *
 * Source: ReplicationLogDiscoveryForwarder.processFile() L132-152
 *)
HDFSUp(c) ==
    /\ hdfsAvailable[c] = FALSE
    /\ hdfsAvailable' = [hdfsAvailable EXCEPT ![c] = TRUE]
    /\ UNCHANGED <<writerVars, clusterVars, replayVars,
                   zkPeerConnected, zkPeerSessionAlive, zkLocalConnected>>

============================================================================
