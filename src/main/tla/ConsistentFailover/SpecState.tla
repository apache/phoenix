------------------------ MODULE SpecState -------------------------------------
(*
 * Shared state variables and state-dependent helper operators for the
 * Phoenix Consistent Failover specification.
 *
 * The root module and all sub-modules EXTEND SpecState so the full
 * variable list, variable-group tuples, and predicates that reference
 * variables live in one place. See ConsistentFailover.tla module
 * header for implementation traceability per variable.
 *
 * Variable groups partition the 13 specification variables by actor:
 *   writerVars  -- per-RS replication writer mode
 *   clusterVars -- cluster-level HA group state and per-cluster
 *                  protocol state (outDirEmpty, antiFlapTimer,
 *                  failoverPending, inProgressDirEmpty)
 *   replayVars  -- standby-side replay state and counters
 *   envVars     -- environment substrate (HDFS availability,
 *                  ZK connection/session state)
 *
 * UNCHANGED clauses reference these groups whenever a group is fully
 * unchanged. Partially-changed groups list the unchanged members
 * individually.
 *)
EXTENDS Types

VARIABLE clusterState, writerMode, outDirEmpty, hdfsAvailable, antiFlapTimer,
         replayState, lastRoundInSync, lastRoundProcessed,
         failoverPending, inProgressDirEmpty,
         zkPeerConnected, zkPeerSessionAlive, zkLocalConnected

---------------------------------------------------------------------------

(* Variable-group tuples *)

\* Per-RS replication writer mode.
writerVars == <<writerMode>>

\* Cluster-level HA group state and per-cluster protocol state.
clusterVars == <<clusterState, outDirEmpty, antiFlapTimer,
                 failoverPending, inProgressDirEmpty>>

\* Standby-side replay state and counters.
replayVars == <<replayState, lastRoundInSync, lastRoundProcessed>>

\* Environment substrate: HDFS availability and ZK coordination state.
envVars == <<hdfsAvailable,
             zkPeerConnected, zkPeerSessionAlive, zkLocalConnected>>

\* Full variable tuple for use in temporal formulas
\* ([][Next]_vars, WF_vars(...), SF_vars(...)).
vars == <<writerVars, clusterVars, replayVars, envVars>>

---------------------------------------------------------------------------

(* ZK connectivity health predicates *)

\* Peer ZK path-children cache is delivering notifications: the
\* TCP connection is alive and the ZK session has not expired.
\* Peer-reactive transitions (PeerReact*) depend on this predicate.
PeerZKHealthy(c) ==
    /\ zkPeerConnected[c] = TRUE
    /\ zkPeerSessionAlive[c] = TRUE

\* Local ZK path-children cache is healthy; isHealthy = true in
\* HAGroupStoreClient, which gates setHAGroupStatusIfNeeded() and
\* all local ZK writes.
LocalZKHealthy(c) == zkLocalConnected[c] = TRUE

============================================================================
