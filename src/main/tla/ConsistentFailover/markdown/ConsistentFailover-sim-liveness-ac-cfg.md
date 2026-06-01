# ConsistentFailover-sim-liveness-ac.cfg -- AbortCompletion Liveness Configuration

**Source:** [`ConsistentFailover-sim-liveness-ac.cfg`](../ConsistentFailover-sim-liveness-ac.cfg)

## Overview

This is the per-property simulation liveness configuration for the `AbortCompletion` property. It uses `FairnessAC` (3 temporal clauses per cluster, 5 total with `Tick`) to keep the Buchi automaton tractable while checking that every abort state eventually auto-completes to a stable state.

### Why Per-Property Liveness

The full `Fairness` formula has 43 temporal clauses, which would cause TLC's Buchi automaton construction to blow up (the automaton size is exponential in the number of temporal clauses). Per-property formulas include only the fairness clauses on the critical path for one liveness property, keeping the automaton manageable.

### AbortCompletion Critical Path

The `AbortCompletion` property states:

```
AbortCompletion == \A c \in Cluster :
    clusterState[c] \in {"AbTS", "AbTAIS", "AbTANIS"}
    ~> clusterState[c] \in {"AIS", "ANIS", "S"}
```

The critical path for abort resolution is:

1. `AutoComplete` fires (requires `zkLocalConnected = TRUE`)
2. If `zkLocalConnected` was FALSE, `ZKLocalReconnect` re-enables it
3. `Tick` advances the anti-flapping timer (needed if AbTANIS -> ANIS resets the timer)

The minimal fairness formula is:
- WF on `Tick`
- WF on `ZKLocalReconnect` (ZLA)
- SF on `AutoComplete` (guarded by `zkLocalConnected`)

### Run Command

```bash
java -XX:+UseParallelGC \
  -Dtlc2.TLC.stopAfter=28800 \
  -cp tla2tools.jar:CommunityModules-deps.jar \
  tlc2.TLC ConsistentFailover.tla \
  -config ConsistentFailover-sim-liveness-ac.cfg \
  -simulate -depth 10000 -workers auto
```

## Configuration

```
SPECIFICATION SpecAC
```

Uses `SpecAC` = `Init /\ [][Next]_vars /\ FairnessAC`.

### Constants

```
CONSTANTS
    Cluster = {c1, c2}
    RS = {rs1, rs2}
    WaitTimeForSync = 2
    UseOfflinePeerDetection = FALSE
```

Same as the exhaustive safety model. Small RS count keeps the Buchi automaton tractable.

### Liveness Property

```
PROPERTY
    AbortCompletion
```

### Invariants and Action Constraints

Same 6 invariants and 9 action constraints as the safety models. Liveness checking does not disable safety checking.
