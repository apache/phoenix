# ConsistentFailover.cfg -- Exhaustive Safety Model Configuration

**Source:** [`ConsistentFailover.cfg`](../ConsistentFailover.cfg)

## Overview

This is the primary (exhaustive) TLC model configuration for the Phoenix Consistent Failover specification. It performs a complete state-space exploration with 2 clusters and 2 region servers, verifying all 6 state invariants and 9 action constraints over every reachable state. This is the strongest verification mode: if TLC completes without finding a violation, the safety properties hold for all possible interleavings of all actions.

### Model Checking Strategy

**Exhaustive (breadth-first) search** explores every reachable state. The state space is bounded by:

- 2 clusters (fixed by the protocol architecture)
- 2 RS per cluster (minimum to exercise per-RS CAS races)
- `WaitTimeForSync = 2` (minimum to exercise timer counting behavior)
- `lastRoundProcessed[c] <= 3` (state constraint to bound replay counter growth)
- RS symmetry reduction (`Permutations(RS)`)

These choices keep the state space tractable (~95M distinct states, ~12 minutes on 16 workers with `UseOfflinePeerDetection = FALSE`). With `UseOfflinePeerDetection = TRUE`, the state space grows to ~171M distinct states, ~24 minutes on 16 workers. Both configurations exercise all safety-relevant interleavings.

### Run Command

```bash
java -XX:+UseParallelGC \
  -cp tla2tools.jar:CommunityModules-deps.jar \
  tlc2.TLC ConsistentFailover.tla -config ConsistentFailover.cfg \
  -workers auto -cleanup
```

## Configuration

```
SPECIFICATION SafetySpec
```

Uses `SafetySpec` (`Init /\ [][Next]_vars`) -- no fairness. Safety-only model checking avoids the temporal overhead of Buchi automaton construction, which is exponential in the number of fairness clauses.

### Constants

```
CONSTANTS
    Cluster = {c1, c2}
    RS = {rs1, rs2}
    WaitTimeForSync = 2
    UseOfflinePeerDetection = FALSE
```

**`Cluster = {c1, c2}`:** Exactly 2 clusters forming the HA pair, matching the protocol's architectural requirement.

**`RS = {rs1, rs2}`:** 2 region servers per cluster. This is the minimum needed to exercise the ZK CAS race: when HDFS fails, two RS independently detect the failure and race to CAS-write AIS -> ANIS. The first succeeds; the second gets `BadVersionException` and aborts. With only 1 RS, CAS failure is unreachable.

**`WaitTimeForSync = 2`:** The minimum value that exercises the timer's counting behavior (the timer can be at 0, 1, or 2). Larger values add more timer states without exercising new protocol interleavings.

**`UseOfflinePeerDetection = FALSE`:** Feature gate for proactive AWOP/ANISWOP modeling.

### Symmetry

```
SYMMETRY Symmetry
```

RS identifiers are interchangeable: all RS start in INIT with identical action sets. Permutation symmetry reduces the effective state space by `|RS|!` (factor of 2 with 2 RS). Cluster identifiers remain asymmetric because the initial state is asymmetric (one cluster starts AIS, the other S).

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

All 6 state invariants are checked in every reachable state. See [ConsistentFailover.md](ConsistentFailover.md) for detailed descriptions of each invariant.

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

All 9 action constraints are checked on every state transition. These verify that the `Next` relation only produces transitions consistent with the implementation's transition tables and preconditions.

### State Constraint

```
CONSTRAINT
    ReplayCounterBound
```

Bounds `lastRoundProcessed[c] <= 3` for exhaustive search tractability. The abstract counter values only matter relationally (`lastRoundProcessed >= lastRoundInSync`), so small bounds suffice. Without this constraint, the counters would grow unboundedly, making exhaustive search infeasible.
