# HDFS -- HDFS Availability Incident Actions

**Source:** [`HDFS.tla`](../HDFS.tla)

## Overview

`HDFS` models NameNode crash and recovery as environment incidents. These are pure environment actions -- they set a boolean availability flag and leave all other variables unchanged. The per-RS effects of HDFS unavailability (writer degradation, CAS races, RS aborts) are handled by the [Writer](Writer.md) and [RS](RS.md) modules.

### Modeling Choice: Flag vs. Per-File HDFS State

HDFS availability is modeled as a single boolean per cluster rather than per-file or per-directory state. This abstraction is appropriate because:

1. **NameNode is the single point of failure.** When a NameNode crashes, all HDFS operations on that cluster fail. There is no partial HDFS failure in the scope of this model.
2. **The protocol reacts to HDFS availability, not individual file operations.** Writer degradation is triggered by IOException from any HDFS write, not by specific file-level failures.
3. **State space reduction.** Per-file HDFS state would explode the state space without adding any safety-relevant behavior.

### Asymmetric Decomposition

The decomposition between `HDFSDown`/`HDFSUp` and per-RS writer actions is asymmetric:

- **HDFSDown(c)** sets `hdfsAvailable[c] = FALSE`. No immediate writer effect -- per-RS degradation happens individually when each RS attempts its next HDFS write and gets IOException. This enables modeling the CAS race where multiple RS on the same cluster independently detect the failure and race to update the ZK state.
- **HDFSUp(c)** sets `hdfsAvailable[c] = TRUE`. No immediate writer effect -- recovery is per-RS via the forwarder path (`WriterStoreFwdToSyncFwd` in [Writer.md](Writer.md)), which is guarded on `hdfsAvailable`.

### Two Failure Scenarios

Any cluster's HDFS can fail at any time, producing two distinct scenarios:

1. **Standby HDFS fails (`HDFSDown(c_standby)`):** Active writers detect via IOException and degrade (SYNC -> S&F). This is the primary degradation path handled by `WriterToStoreFwd` in [Writer.md](Writer.md).
2. **Active cluster's own HDFS fails (`HDFSDown(c_active)`):** S&F writers on the active cluster abort because they are writing to local (own) HDFS. This is handled by `RSAbortOnLocalHDFSFailure` in [RS.md](RS.md).

## Implementation Traceability

| TLA+ Action | Java Source |
|---|---|
| `HDFSDown(c)` | NameNode crash; detected reactively via IOException from `ReplicationLog.apply()` |
| `HDFSUp(c)` | NameNode recovery; forwarder detects via successful `FileUtil.copy()` in `processFile()` L132-152 |

```tla
EXTENDS SpecState, Types
```

## HDFSDown -- NameNode Crash

Sets the HDFS availability flag to FALSE for cluster `c`. Per-RS writer degradation (SYNC -> S&F, SYNC_AND_FWD -> S&F) is handled individually by `WriterToStoreFwd` and `WriterSyncFwdToStoreFwd` in [Writer.md](Writer.md), which are guarded on `hdfsAvailable[Peer(c)] = FALSE`. Those actions also handle the AIS -> ANIS cluster state transition and CAS failure (-> DEAD).

**Fairness:** No fairness (Tier 4). HDFS crashes are genuinely non-deterministic environment events.

Source: NameNode crash (environment event).

```tla
HDFSDown(c) ==
    /\ hdfsAvailable[c] = TRUE
    /\ hdfsAvailable' = [hdfsAvailable EXCEPT ![c] = FALSE]
    /\ UNCHANGED <<clusterState, writerMode, outDirEmpty, antiFlapTimer,
                   replayState, lastRoundInSync, lastRoundProcessed,
                   failoverPending, inProgressDirEmpty,
                   zkPeerConnected, zkPeerSessionAlive, zkLocalConnected>>
```

## HDFSUp -- NameNode Recovery

Sets `hdfsAvailable[c] = TRUE`. No immediate writer effect -- recovery is per-RS via the forwarder path. The forwarder detects connectivity by successfully copying a file from OUT to the peer's IN directory; if throughput exceeds the threshold, it transitions the writer S&F -> SYNC_AND_FWD (`WriterStoreFwdToSyncFwd` in [Writer.md](Writer.md)).

**Fairness:** SF (Tier 3). Under SF on `HDFSUp`, HDFS cannot be permanently down. This is needed for the `DegradationRecovery` liveness property -- without it, the adversary could keep HDFS down indefinitely, preventing the writer recovery chain from completing.

Source: `ReplicationLogDiscoveryForwarder.processFile()` L132-152.

```tla
HDFSUp(c) ==
    /\ hdfsAvailable[c] = FALSE
    /\ hdfsAvailable' = [hdfsAvailable EXCEPT ![c] = TRUE]
    /\ UNCHANGED <<clusterState, writerMode, outDirEmpty, antiFlapTimer,
                   replayState, lastRoundInSync, lastRoundProcessed,
                   failoverPending, inProgressDirEmpty,
                   zkPeerConnected, zkPeerSessionAlive, zkLocalConnected>>
```
