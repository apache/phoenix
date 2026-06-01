# SpecState -- Shared Variables and Groupings

**Source:** [`SpecState.tla`](../SpecState.tla)

## Overview

`SpecState` declares the 13 specification variables in one place. The root module [`ConsistentFailover.tla`](../ConsistentFailover.tla) and every sub-module extend `SpecState` (which itself extends [`Types.tla`](../Types.tla)), so adding a variable requires editing this module and the relevant variable-group tuple, not repeating a long `VARIABLE` list in every actor module.

`SpecState` also defines the variable-group tuples used in every action's `UNCHANGED` clause and two ZK-health predicates shared by every sub-module that guards on the ZK substrate.

Implementation traceability for each variable remains documented in the `ConsistentFailover.tla` module header and in [ConsistentFailover.md](ConsistentFailover.md).

## Variable groups

Every action's `UNCHANGED` clause is written in terms of these tuples. When a group is fully unchanged, the group name stands in for the full variable list; when a group is partially changed, the unchanged members are listed individually.

- **`writerVars`** == `<<writerMode>>` -- per-RS replication writer mode.
- **`clusterVars`** == `<<clusterState, outDirEmpty, antiFlapTimer, failoverPending, inProgressDirEmpty>>` -- cluster-level HA group state and per-cluster protocol state.
- **`replayVars`** == `<<replayState, lastRoundInSync, lastRoundProcessed>>` -- standby-side replay state and counters.
- **`envVars`** == `<<hdfsAvailable, zkPeerConnected, zkPeerSessionAlive, zkLocalConnected>>` -- environment substrate (HDFS availability, ZK connection/session state).
- **`vars`** == `<<writerVars, clusterVars, replayVars, envVars>>` -- full variable tuple for temporal formulas (`[][Next]_vars`, `WF_vars(...)`, `SF_vars(...)`).

## ZK-health predicates

- **`PeerZKHealthy(c)`** == `zkPeerConnected[c] = TRUE /\ zkPeerSessionAlive[c] = TRUE` -- peer `PathChildrenCache` is delivering notifications. Used as the ZK-watcher guard for every `PeerReact*` action and `ReactiveTransitionFail` in [`HAGroupStore.tla`](../HAGroupStore.tla).
- **`LocalZKHealthy(c)`** == `zkLocalConnected[c] = TRUE` -- local `PathChildrenCache` is healthy (`isHealthy = true` in `HAGroupStoreClient`). Used to gate every action that calls `setHAGroupStatusIfNeeded()` (all local ZK writes): `AutoComplete`, `ANISHeartbeat`, `ANISToAIS`, `ANISTSToATS` in [`HAGroupStore.tla`](../HAGroupStore.tla); all writer mode-change actions in [`Writer.tla`](../Writer.tla); and `TriggerFailover` in [`Reader.tla`](../Reader.tla).

Admin actions (`AdminStartFailover`, `AdminAbortFailover`, `AdminGoOffline`, `AdminForceRecover`) intentionally bypass `LocalZKHealthy` -- they model direct ZK writes that do not depend on the `PathChildrenCache` isHealthy signal.

## TLA+ Source

```tla
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

writerVars == <<writerMode>>

clusterVars == <<clusterState, outDirEmpty, antiFlapTimer,
                 failoverPending, inProgressDirEmpty>>

replayVars == <<replayState, lastRoundInSync, lastRoundProcessed>>

envVars == <<hdfsAvailable,
             zkPeerConnected, zkPeerSessionAlive, zkLocalConnected>>

vars == <<writerVars, clusterVars, replayVars, envVars>>

---------------------------------------------------------------------------

(* ZK connectivity health predicates *)

PeerZKHealthy(c) ==
    /\ zkPeerConnected[c] = TRUE
    /\ zkPeerSessionAlive[c] = TRUE

LocalZKHealthy(c) == zkLocalConnected[c] = TRUE

============================================================================
```
