# ConsistentFailover-sim-liveness-dr.cfg -- DegradationRecovery Liveness Configuration

**Source:** [`ConsistentFailover-sim-liveness-dr.cfg`](../ConsistentFailover-sim-liveness-dr.cfg)

## Overview

This is the per-property simulation liveness configuration for the `DegradationRecovery` property. It uses `FairnessDR` (17 temporal clauses with 2 RS) to verify that ANIS with available peer HDFS eventually progresses out of ANIS.

### DegradationRecovery Critical Path

The `DegradationRecovery` property states:

```
DegradationRecovery == \A c \in Cluster :
    (clusterState[c] = "ANIS" /\ hdfsAvailable[Peer(c)])
    ~> clusterState[c] # "ANIS"
```

This is the most clause-intensive property because the recovery chain involves per-RS writer actions. The critical path is:

**Writer recovery chain (per-RS):**
1. S&F -> S&FWD (`WriterStoreFwdToSyncFwd` -- SF, guarded on `hdfsAvailable`)
2. S&FWD -> SYNC (`WriterSyncFwdToSync` -- SF, guarded on `hdfsAvailable` and `zkLocalConnected`)

**Dead RS recovery (per-RS):**
1. `RSAbortOnLocalHDFSFailure` kills S&F writers on local HDFS failure (SF)
2. `RSRestart` restarts dead RS (SF)
3. `WriterInit` initializes restarted RS in SYNC mode (WF)

**Cluster recovery:**
1. `ANISToAIS` fires when all RS are in SYNC/S&FWD, OUT is empty, and gate is open (SF, guarded on `zkLocalConnected`)
2. `HDFSUp` ensures HDFS is eventually available (SF)

**Timer and ZK:**
1. `Tick` advances anti-flapping timer (WF)
2. `ANISHeartbeat` resets timer while S&F writers exist (WF)
3. `ZKLocalReconnect` re-enables `zkLocalConnected` (WF, ZLA)
4. `WriterSyncToSyncFwd` transitions SYNC writers to S&FWD when cluster is ANIS (WF)

With 2 RS, the per-RS clauses contribute 2x6 = 12 clauses, plus 5 cluster-level clauses = 17 total. With 9 RS, this would be 59 clauses, far too large for TLC's Buchi automaton.

### Run Command

```bash
java -XX:+UseParallelGC \
  -Dtlc2.TLC.stopAfter=28800 \
  -cp tla2tools.jar:CommunityModules-deps.jar \
  tlc2.TLC ConsistentFailover.tla \
  -config ConsistentFailover-sim-liveness-dr.cfg \
  -simulate -depth 10000 -workers auto
```

## Configuration

```
SPECIFICATION SpecDR
```

Uses `SpecDR` = `Init /\ [][Next]_vars /\ FairnessDR`.

### Constants

```
CONSTANTS
    Cluster = {c1, c2}
    RS = {rs1, rs2}
    WaitTimeForSync = 2
    UseOfflinePeerDetection = FALSE
```

Same as the exhaustive safety model. 2 RS is the maximum feasible for `DegradationRecovery` liveness checking due to the per-RS fairness clause multiplication.

### Liveness Property

```
PROPERTY
    DegradationRecovery
```

### Invariants and Action Constraints

Same 6 invariants and 9 action constraints as the safety models.
