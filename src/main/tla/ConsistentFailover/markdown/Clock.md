# Clock -- Anti-Flapping Countdown Timer

**Source:** [`Clock.tla`](../Clock.tla)

## Overview

`Clock` provides a single `Tick` action that advances all per-cluster anti-flapping countdown timers by one tick toward 0. This follows the explicit-time pattern from Lamport, "Real Time is Really Simple" (CHARME 2005, Section 2).

### The Lamport Countdown Timer Pattern

Time is modeled as an ordinary variable, and lower-bound timing constraints (an action cannot fire until enough time passes) are expressed as enabling conditions on the guarded action using a countdown timer that ticks to 0.

The key insight is that we do not need a global clock or real-time semantics. Instead:

1. A countdown timer variable starts at a known value (`WaitTimeForSync`).
2. A `Tick` action decrements the timer by 1 (floor at 0).
3. The guarded action (ANIS -> AIS, ANISTS -> ATS) is enabled only when the timer reaches 0.
4. The S&F heartbeat (`ANISHeartbeat` in [HAGroupStore.md](HAGroupStore.md)) resets the timer to `WaitTimeForSync`, keeping the gate closed.

This pattern models the wall-clock waiting period without introducing continuous time. The timer counts ticks, not seconds -- the relationship between ticks and real time is abstracted away.

### Why a Separate Module?

The `Tick` action is global (not per-cluster) -- it decrements all cluster timers simultaneously. This models the passage of time uniformly across the system. Factoring it into its own module clarifies that time advance is an independent system-wide action, not associated with any particular cluster's behavior.

### Guard Against Stuttering

The `Tick` action is guarded so it only fires when at least one timer is still counting down (`AntiFlapGateClosed`). This prevents useless stuttering ticks when all timers have already expired, which would inflate the state space without producing new reachable states.

## Implementation Traceability

| TLA+ Action | Java Source |
|---|---|
| `Tick` | Passage of wall-clock time; no direct Java counterpart. Models the interval between `HAGroupStoreClient.validateTransitionAndGetWaitTime()` checks (L1027-1046). |

In the implementation, the anti-flapping gate is implemented via timestamp comparison: `validateTransitionAndGetWaitTime()` reads the ZK znode's `mtime` and computes the elapsed time since the last ANIS write. If the elapsed time is less than `waitTimeForSyncModeInMs`, the transition is deferred. The TLA+ countdown timer abstracts this timestamp-based mechanism into discrete ticks.

```tla
EXTENDS SpecState, Types
```

## Tick -- Advance All Countdown Timers

Each cluster's anti-flapping timer is decremented via `DecrementTimer` (floor at 0). The action is enabled only when at least one cluster has a timer still counting down (`AntiFlapGateClosed`), preventing infinite stuttering at zero.

**Fairness:** WF (Tier 1). The guard depends only on protocol state (the `antiFlapTimer` variable), not on environment variables. Continuous enablement is guaranteed: once a timer is positive, it stays positive until `Tick` fires (no other action decrements it; `ANISHeartbeat` resets it to `WaitTimeForSync`, which is also positive). WF guarantees `Tick` eventually fires, ensuring the gate eventually opens.

See [Types.md](Types.md) for the `AntiFlapGateClosed` and `DecrementTimer` helper operator definitions.

```tla
Tick ==
    /\ \E c \in Cluster : AntiFlapGateClosed(antiFlapTimer[c])
    /\ antiFlapTimer' = [c \in Cluster |-> DecrementTimer(antiFlapTimer[c])]
    /\ UNCHANGED <<clusterState, writerMode, outDirEmpty, hdfsAvailable,
                   replayState, lastRoundInSync, lastRoundProcessed,
                   failoverPending, inProgressDirEmpty,
                   zkPeerConnected, zkPeerSessionAlive, zkLocalConnected>>
```
