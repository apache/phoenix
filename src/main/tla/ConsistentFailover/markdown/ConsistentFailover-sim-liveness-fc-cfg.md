# ConsistentFailover-sim-liveness-fc.cfg -- FailoverCompletion Liveness Configuration

**Source:** [`ConsistentFailover-sim-liveness-fc.cfg`](../ConsistentFailover-sim-liveness-fc.cfg)

## Overview

This is the per-property simulation liveness configuration for the `FailoverCompletion` property. It uses `FairnessFC` (8 temporal clauses per cluster, 15 total with `Tick`) to verify that standby-side and abort transient states eventually resolve to a stable state.

### FailoverCompletion Critical Path

The `FailoverCompletion` property states:

```
FailoverCompletion == \A c \in Cluster :
    clusterState[c] \in {"STA", "AbTAIS", "AbTANIS", "AbTS"}
    ~> clusterState[c] \in {"AIS", "ANIS", "S"}
```

This is the most complex liveness property, requiring the most fairness clauses. The critical paths are:

**STA resolution:**
1. Replay machine completes (`ReplayAdvance`, `ReplayRewind`, `ReplayBeginProcessing`, `ReplayFinishProcessing` -- all WF)
2. HDFS becomes available (`HDFSUp` -- SF, needed for `shouldTriggerFailover()` HDFS reads)
3. `TriggerFailover` fires (SF, grouped with `AutoComplete` by `clusterState` exclusivity)

**Abort state resolution:**
1. `AutoComplete` fires (SF, requires `zkLocalConnected`)
2. `ZKLocalReconnect` re-enables `zkLocalConnected` (WF, ZLA)

**Timer:**
1. `Tick` advances anti-flapping timer (WF)

The `AutoComplete` and `TriggerFailover` are grouped under a single SF because they guard on mutually exclusive `clusterState` values (AbTS/AbTAIS/AbTANIS vs STA).

### Run Command

```bash
java -XX:+UseParallelGC \
  -Dtlc2.TLC.stopAfter=28800 \
  -cp tla2tools.jar:CommunityModules-deps.jar \
  tlc2.TLC ConsistentFailover.tla \
  -config ConsistentFailover-sim-liveness-fc.cfg \
  -simulate -depth 10000 -workers auto
```

## Configuration

```
SPECIFICATION SpecFC
```

Uses `SpecFC` = `Init /\ [][Next]_vars /\ FairnessFC`.

### Constants

```
CONSTANTS
    Cluster = {c1, c2}
    RS = {rs1, rs2}
    WaitTimeForSync = 2
    UseOfflinePeerDetection = FALSE
```

Same as the exhaustive safety model.

### Liveness Property

```
PROPERTY
    FailoverCompletion
```

### Invariants and Action Constraints

Same 6 invariants and 9 action constraints as the safety models.
