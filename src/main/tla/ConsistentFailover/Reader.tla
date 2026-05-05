-------------------------- MODULE Reader ----------------------------------------
(*
 * Replication replay state machine for the Phoenix Consistent
 * Failover specification.
 *
 * The standby cluster's reader replays replication logs round-by-round,
 * tracking two counters (lastRoundProcessed, lastRoundInSync) and a
 * replay state that determines how the counters advance.
 *
 * REPLAY STATE SEMANTICS:
 *   SYNC:             Both counters advance together (in-sync replay).
 *   DEGRADED:         Only lastRoundProcessed advances; lastRoundInSync
 *                     is frozen (degraded replay).
 *   SYNCED_RECOVERY:  Rewinds lastRoundProcessed to lastRoundInSync,
 *                     then CAS-transitions to SYNC.
 *   NOT_INITIALIZED:  Pre-init on the active side; transitions to
 *                     SYNCED_RECOVERY on first S entry after failover.
 *
 * LISTENER EFFECTS: The degradedListener and recoveryListener use
 * unconditional .set() (not .compareAndSet()). These fire
 * synchronously on the local PathChildrenCache event thread during
 * the cluster state transition and are modeled as atomic with the
 * triggering state-entry actions in HAGroupStore.tla:
 *   - S entry:  set(SYNCED_RECOVERY) -- folded into PeerReactToAIS,
 *               PeerReactToANIS (ATS->S), AutoComplete (AbTS->S)
 *   - DS entry: set(DEGRADED) -- folded into PeerReactToANIS (S->DS)
 *
 * CAS SEMANTICS: The SYNCED_RECOVERY -> SYNC transition uses
 * compareAndSet(SYNCED_RECOVERY, SYNC) at L332-333. The CAS can
 * only fail if a concurrent set(DEGRADED) fires first (the cluster
 * re-degrades before replay() can CAS). TLC's interleaving semantics
 * model this race: either ReplayRewind fires first (CAS succeeds)
 * or the DS-entry fold in PeerReactToANIS fires first (state becomes
 * DEGRADED, ReplayRewind is no longer enabled).
 *
 * Implementation traceability:
 *
 *   TLA+ action                | Java source
 *   --------------------------+--------------------------------------------
 *   ReplayAdvance(c)          | replay() L336-343 (SYNC) and L345-351
 *                             |   (DEGRADED) -- round processing loop
 *   ReplayRewind(c)           | replay() L323-333 --
 *                             |   compareAndSet(SYNCED_RECOVERY, SYNC);
 *                             |   getFirstRoundToProcess() rewinds to
 *                             |   lastRoundInSync (L389)
 *   ReplayBeginProcessing(c)  | replay() round processing start --
 *                             |   in-progress files created when a
 *                             |   round is picked up for processing
 *   ReplayFinishProcessing(c) | replay() round processing end --
 *                             |   in-progress files cleaned up after
 *                             |   round is fully processed
 *   TriggerFailover(c)        | shouldTriggerFailover() L500-533 (guards);
 *                             |   triggerFailover() L535-548 (effect);
 *                             |   setHAGroupStatusToSync() L341-355
 *                             |   (ZK write)
 *)
EXTENDS SpecState, Types

---------------------------------------------------------------------------

(*
 * Replay advance: round processing in SYNC or DEGRADED state.
 *
 * The reader processes the next round of replication logs.
 *   - SYNC: both lastRoundProcessed and lastRoundInSync advance,
 *     maintaining the invariant that they are equal.
 *   - DEGRADED: only lastRoundProcessed advances; lastRoundInSync
 *     is frozen, modeling degraded replay where rounds are processed
 *     but the in-sync consistency point does not advance.
 *
 * Guard: cluster is in a standby state or STA (replay continues
 * during failover pending) and replay is in SYNC or DEGRADED.
 *
 * Source: replay() L336-343 (SYNC), L345-351 (DEGRADED)
 *)
ReplayAdvance(c) ==
    /\ clusterState[c] \in StandbyStates \union {"STA"}
    /\ replayState[c] \in {"SYNC", "DEGRADED"}
    /\ lastRoundProcessed' = [lastRoundProcessed EXCEPT ![c] = @ + 1]
    /\ lastRoundInSync' = [lastRoundInSync EXCEPT ![c] =
           IF replayState[c] = "SYNC" THEN @ + 1 ELSE @]
    /\ UNCHANGED <<writerVars, clusterVars, envVars, replayState>>

---------------------------------------------------------------------------

(*
 * Replay rewind and CAS to SYNC from SYNCED_RECOVERY.
 *
 * In SYNCED_RECOVERY, replay() rewinds lastRoundProcessed to
 * lastRoundInSync (via getFirstRoundToProcess() at L389), then
 * attempts compareAndSet(SYNCED_RECOVERY, SYNC) at L332-333.
 *
 * The CAS can only fail if a concurrent set(DEGRADED) fires first
 * (the cluster re-degrades before replay() can CAS). TLC's
 * interleaving semantics model this race naturally: either this
 * action fires (CAS succeeds, state becomes SYNC) or the DS-entry
 * fold in PeerReactToANIS fires first (state becomes DEGRADED,
 * this action is no longer enabled).
 *
 * Source: replay() L323-333 -- compareAndSet(SYNCED_RECOVERY, SYNC);
 *         getFirstRoundToProcess() L389 -- rewinds to lastRoundInSync
 *)
ReplayRewind(c) ==
    /\ replayState[c] = "SYNCED_RECOVERY"
    /\ replayState' = [replayState EXCEPT ![c] = "SYNC"]
    /\ lastRoundProcessed' = [lastRoundProcessed EXCEPT ![c] = lastRoundInSync[c]]
    /\ UNCHANGED <<writerVars, clusterVars, envVars, lastRoundInSync>>

---------------------------------------------------------------------------

(*
 * Begin round processing: in-progress directory becomes non-empty.
 *
 * When the reader picks up a new round for processing, it creates
 * in-progress files in the IN-PROGRESS directory. This makes the
 * directory non-empty, blocking the failover trigger until
 * processing completes.
 *
 * Guard: cluster is in a standby state or STA (replay continues
 * during failover pending -- the replay() loop does not stop when
 * the cluster enters STA) and the in-progress directory is
 * currently empty.
 *
 * Source: replay() L307-310 -- getFirstRoundToProcess() returns a
 *         round; processing begins, creating in-progress files.
 *)
ReplayBeginProcessing(c) ==
    /\ clusterState[c] \in StandbyStates \union {"STA"}
    /\ inProgressDirEmpty[c] = TRUE
    /\ inProgressDirEmpty' = [inProgressDirEmpty EXCEPT ![c] = FALSE]
    /\ UNCHANGED <<writerVars, replayVars, envVars,
                   clusterState, outDirEmpty, antiFlapTimer, failoverPending>>

---------------------------------------------------------------------------

(*
 * Finish round processing: in-progress directory becomes empty.
 *
 * When the reader finishes processing a round, it cleans up
 * in-progress files. The directory becomes empty, allowing the
 * failover trigger to proceed (if other guards are satisfied).
 *
 * Guard: in-progress directory is currently non-empty.
 *
 * Source: replay() L336-351 -- round processing completes,
 *         in-progress files are cleaned up.
 *)
ReplayFinishProcessing(c) ==
    /\ inProgressDirEmpty[c] = FALSE
    /\ inProgressDirEmpty' = [inProgressDirEmpty EXCEPT ![c] = TRUE]
    /\ UNCHANGED <<writerVars, replayVars, envVars,
                   clusterState, outDirEmpty, antiFlapTimer, failoverPending>>

---------------------------------------------------------------------------

(*
 * Failover trigger: STA -> AIS when replay is complete.
 *
 * The standby cluster writes ACTIVE_IN_SYNC to its own ZK znode
 * after the replication log reader determines replay is complete.
 * This is driven by the reader component, not a peer-reactive
 * transition.
 *
 * Four guards model the conditions under which failover is safe:
 *   1. failoverPending[c] -- set by triggerFailoverListener (L159-171)
 *      when the local cluster enters STA.
 *   2. inProgressDirEmpty[c] -- no partially-processed replication
 *      log files (getInProgressFiles().isEmpty() at L508).
 *   3. replayState[c] = "SYNC" -- the SYNCED_RECOVERY rewind must
 *      have completed. Without this guard, failover could proceed
 *      with degraded rounds not re-processed from the sync point.
 *   4. hdfsAvailable[c] = TRUE -- the standby's own HDFS must be
 *      accessible; shouldTriggerFailover() performs HDFS reads
 *      (getInProgressFiles, getNewFiles) that throw IOException
 *      if HDFS is unavailable, blocking the trigger.
 *
 * Guarded on zkLocalConnected[c] because triggerFailover() calls
 * setHAGroupStatusToSync() which requires isHealthy = true.
 *
 * The effect also clears failoverPending, modeling triggerFailover()
 * L538 (failoverPending.set(false)).
 *
 * Source: shouldTriggerFailover() L500-533 (guards);
 *         triggerFailover() L535-548 (effect);
 *         setHAGroupStatusToSync() L341-355 (ZK write)
 *)
TriggerFailover(c) ==
    /\ LocalZKHealthy(c)
    /\ clusterState[c] = "STA"
    /\ failoverPending[c]
    /\ inProgressDirEmpty[c]
    /\ replayState[c] = "SYNC"
    /\ hdfsAvailable[c] = TRUE
    /\ clusterState' = [clusterState EXCEPT ![c] = "AIS"]
    /\ failoverPending' = [failoverPending EXCEPT ![c] = FALSE]
    /\ UNCHANGED <<writerVars, replayVars, envVars,
                   outDirEmpty, antiFlapTimer, inProgressDirEmpty>>

============================================================================
