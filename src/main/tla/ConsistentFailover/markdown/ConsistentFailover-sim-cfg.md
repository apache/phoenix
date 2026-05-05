# ConsistentFailover-sim.cfg -- Simulation Safety Model Configuration

**Source:** [`ConsistentFailover-sim.cfg`](../ConsistentFailover-sim.cfg)

## Overview

This is the simulation (random trace sampling) TLC model configuration for the Phoenix Consistent Failover specification. It samples random behaviors at production-scale RS count (9 RS per cluster) to stress per-RS writer interleaving. Safety-only (no fairness) -- liveness simulation uses separate configurations with smaller RS counts.

### Model Checking Strategy

**Simulation (random trace sampling)** generates random execution traces up to depth 10,000, sufficient for ~100 complete failover cycles with 9 RS. The 9-RS model is too large for exhaustive search (the branching factor of 38 action schemas x 9 RS makes the state space intractable) but ideal for simulation: the high branching factor is sampled efficiently.

The simulation complements the exhaustive model by:

1. **Production-scale RS count:** 9 RS exercises more complex per-RS writer interleavings (e.g., 4 RS in S&F, 3 in SYNC_AND_FWD, 2 in SYNC simultaneously).
2. **Larger WaitTimeForSync:** 5 ticks (vs 2 in the exhaustive model) opens a wider anti-flapping window during which HDFS failures, ZK disruptions, and RS crashes can occur while the gate is closed.
3. **No state constraint:** Replay counters grow organically along each trace without state-space tractability concerns.
4. **No symmetry:** Symmetry reduction provides no benefit for random trace sampling.

### Run Command

```bash
java -XX:+UseParallelGC \
  -Dtlc2.TLC.stopAfter=28800 \
  -cp tla2tools.jar:CommunityModules-deps.jar \
  tlc2.TLC ConsistentFailover.tla -config ConsistentFailover-sim.cfg \
  -simulate -depth 10000 -workers auto
```

The `-Dtlc2.TLC.stopAfter=28800` flag limits the run to 8 hours (28800 seconds).

## Configuration

```
SPECIFICATION SafetySpec
```

Uses `SafetySpec` -- no fairness. Same safety-only strategy as the exhaustive model.

### Constants

```
CONSTANTS
    Cluster = {c1, c2}
    RS = {rs1, rs2, rs3, rs4, rs5, rs6, rs7, rs8, rs9}
    WaitTimeForSync = 5
    UseOfflinePeerDetection = FALSE
```

**`RS = {rs1, ..., rs9}`:** 9 RS exercises per-RS writer interleaving at production scale. With 9 RS, the CAS race during HDFS failure produces rich interleavings: multiple RS detect the failure at different times, some succeed at the CAS write, some fail and abort, and the resulting mix of SYNC, STORE_AND_FWD, SYNC_AND_FWD, and DEAD modes across 9 RS creates scenarios not reachable with only 2 RS.

**`WaitTimeForSync = 5`:** Larger than the exhaustive model to explore richer interleavings during the anti-flapping wait window. With 5 ticks, the system has more time for HDFS failures, ZK disruptions, RS crashes, heartbeat resets, and forwarder drain events to interleave while the anti-flapping gate is closed.

**`UseOfflinePeerDetection = FALSE`:** Feature gate for proactive AWOP/ANISWOP modeling. Set to TRUE to verify the OFFLINE peer detection lifecycle.

### Invariants

```
INVARIANT
    TypeOK
    MutualExclusion
    AbortSafety
    AISImpliesInSync
    WriterClusterConsistency
    ZKSessionConsistency
```

Same 6 invariants as the exhaustive model.

### Action Constraints

```
ACTION_CONSTRAINT
    TransitionValid
    WriterTransitionValid
    AIStoATSPrecondition
    AntiFlapGate
    ANISTStoATSPrecondition
    ReplayTransitionValid
    FailoverTriggerCorrectness
    NoDataLoss
    ReplayRewindCorrectness
```

Same 9 action constraints as the exhaustive model.

### No State Constraint

Unlike the exhaustive model, no state constraint is applied. Simulation samples random traces, and counters grow organically along each trace without state-space tractability concerns.

### No Symmetry

Symmetry reduction is not used because it provides no benefit for random trace sampling. TLC's simulation mode generates random traces by choosing enabled actions uniformly at random -- symmetry reduction affects the state graph structure, not the sampling process.
