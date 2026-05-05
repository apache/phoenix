# RS -- RegionServer Lifecycle Actions

**Source:** [`RS.tla`](../RS.tla)

## Overview

`RS` models RegionServer crash (fail-stop) and process supervisor restart. These are environment and lifecycle actions that interact with the writer state machine in [Writer.md](Writer.md) through the `writerMode` variable.

### Crash Modeling

An RS can crash at any time (JVM crash, OOM, kill signal, process supervisor termination). The crash sets `writerMode` to DEAD but does **not** change `clusterState` -- the HA group state in ZK is independent of RS process lifecycle. This is a critical modeling decision: RS crashes are common operational events that should not destabilize the HA group state machine.

A special-case crash, `RSAbortOnLocalHDFSFailure`, models the abort triggered when the active cluster's own HDFS fails while the writer is in STORE_AND_FWD mode (writing to local HDFS). This is distinct from `HDFSDown` in [HDFS.md](HDFS.md) (which models the *peer's* HDFS failing and degrades writers on the active side).

### Restart Modeling

When an RS dies (writer mode DEAD), the process supervisor (Kubernetes/YARN) detects the dead pod and creates a new one. HBase assigns regions and the writer re-initializes in INIT mode, ready to follow the normal startup path (`WriterInit` or `WriterInitToStoreFwd` in [Writer.md](Writer.md)).

### DEAD Writer Preservation Through Standby Entry

When a cluster transitions ATS -> S (becoming standby), the replication subsystem restart resets live writer modes to INIT but preserves DEAD writers. A crashed RS (JVM dead) cannot process the state change notification -- the process supervisor restart handles DEAD -> INIT independently. See `PeerReactToAIS` and `PeerReactToANIS` in [HAGroupStore.md](HAGroupStore.md).

## Implementation Traceability

| TLA+ Action | Java Source |
|---|---|
| `RSCrash(c, rs)` | JVM crash, OOM, kill signal, process supervisor termination |
| `RSAbortOnLocalHDFSFailure(c, rs)` | `StoreAndForwardModeImpl.onFailure()` L115-123 -> `logGroup.abort()` |
| `RSRestart(c, rs)` | Kubernetes/YARN pod restart -> HBase RS startup -> `ReplicationLogGroup.initializeReplicationMode()` |

```tla
EXTENDS SpecState, Types
```

## RSRestart -- Process Supervisor Restarts Dead RS: DEAD -> INIT

The restarted RS enters INIT mode. Subsequent writer actions (`WriterInit` or `WriterInitToStoreFwd` in [Writer.md](Writer.md)) handle the actual mode initialization based on HDFS availability and cluster state.

**Fairness:** SF (Tier 3), grouped with `RSAbortOnLocalHDFSFailure` by mutual exclusivity (DEAD and STORE_AND_FWD are mutually exclusive writer modes). SF is needed because the guard depends on `writerMode` which can be changed by environment events.

Source: Kubernetes/YARN pod restart -> HBase RS startup -> `ReplicationLogGroup.initializeReplicationMode()`.

```tla
RSRestart(c, rs) ==
    /\ writerMode[c][rs] = "DEAD"
    /\ writerMode' = [writerMode EXCEPT ![c][rs] = "INIT"]
    /\ UNCHANGED <<clusterState, outDirEmpty, hdfsAvailable, antiFlapTimer,
                   replayState, lastRoundInSync, lastRoundProcessed,
                   failoverPending, inProgressDirEmpty,
                   zkPeerConnected, zkPeerSessionAlive, zkLocalConnected>>
```

## RSCrash -- Non-Deterministic RS Crash: Any Mode -> DEAD

Models general RS failure (JVM crash, OOM, killed by process supervisor, etc.). The RS can crash at any time regardless of writer mode. The crash does **not** change `clusterState` -- the HA group state in ZK is independent of RS process lifecycle.

This means a cluster can be in AIS with a DEAD RS. The `AISImpliesInSync` invariant in [ConsistentFailover.md](ConsistentFailover.md) explicitly allows DEAD alongside SYNC and INIT in AIS. A DEAD RS is not writing, so the remaining SYNC RSes maintain the in-sync property.

**Fairness:** No fairness (Tier 4). RS crashes are genuinely non-deterministic environment events.

Source: JVM crash, OOM, kill signal, process supervisor termination -- environment event.

```tla
RSCrash(c, rs) ==
    /\ writerMode[c][rs] /= "DEAD"
    /\ writerMode' = [writerMode EXCEPT ![c][rs] = "DEAD"]
    /\ UNCHANGED <<clusterState, outDirEmpty, hdfsAvailable, antiFlapTimer,
                   replayState, lastRoundInSync, lastRoundProcessed,
                   failoverPending, inProgressDirEmpty,
                   zkPeerConnected, zkPeerSessionAlive, zkLocalConnected>>
```

## RSAbortOnLocalHDFSFailure -- S&F Writer Aborts on Own HDFS Failure: STORE_AND_FWD -> DEAD

In STORE_AND_FWD mode, the writer targets the active cluster's own (local/fallback) HDFS. If that HDFS fails, `StoreAndForwardModeImpl.onFailure()` treats the error as fatal and calls `logGroup.abort()`, killing the RS.

### Distinction from HDFSDown

This is distinct from `HDFSDown(c)` in [HDFS.md](HDFS.md):

- `HDFSDown(c)` sets the availability flag for cluster `c`'s HDFS. When `c` is the standby, this triggers *peer* HDFS failure -- active writers degrade from SYNC to S&F.
- `RSAbortOnLocalHDFSFailure` models the active cluster's *own* HDFS failing while the RS is already in S&F mode (writing to local HDFS as fallback).

Note: `hdfsAvailable[c]` is the cluster's OWN HDFS, not `Peer(c)`. RS in SYNC or SYNC_AND_FWD write to the *peer's* HDFS, so they are not affected by their own cluster's HDFS failure. Only STORE_AND_FWD writers are vulnerable because they write to local HDFS.

**Fairness:** SF (Tier 3), grouped with `RSRestart` by mutual exclusivity (STORE_AND_FWD and DEAD are mutually exclusive writer modes).

Source: `StoreAndForwardModeImpl.onFailure()` L115-123 -> `logGroup.abort()`.

```tla
RSAbortOnLocalHDFSFailure(c, rs) ==
    /\ writerMode[c][rs] = "STORE_AND_FWD"
    /\ hdfsAvailable[c] = FALSE
    /\ writerMode' = [writerMode EXCEPT ![c][rs] = "DEAD"]
    /\ UNCHANGED <<clusterState, outDirEmpty, hdfsAvailable, antiFlapTimer,
                   replayState, lastRoundInSync, lastRoundProcessed,
                   failoverPending, inProgressDirEmpty,
                   zkPeerConnected, zkPeerSessionAlive, zkLocalConnected>>
```
