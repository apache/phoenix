# Developer Guide: Modeling Changes with the TLA+ Specification

This guide shows how to use the Phoenix Consistent Failover TLA+ specification to validate proposed design and architecture changes **before** writing implementation code. It covers the five most common change patterns, the end-to-end verification workflow, and annotated guidance on which invariants to check for each kind of change.

---

## 1. Verification Workflow

For any proposed change to the Consistent Failover protocol or its supporting subsystems, follow this workflow:

1. **Identify the feature area** -- HA group transitions, writer modes, replay, ZK coordination, HDFS availability, or RS lifecycle.
2. **Find the corresponding spec module** -- `HAGroupStore.tla`, `Writer.tla`, `Reader.tla`, `ZK.tla`, `HDFS.tla`, `RS.tla`, `Clock.tla`, or `Admin.tla`. See the module-implementation mapping in Section 4.
3. **Model the proposed change in TLA+** -- add or modify actions, adjust guards, add or modify type definitions in `Types.tla`.
4. **Add or modify invariants** if the change introduces new safety requirements.
5. **Run exhaustive verification** (2 clusters, 2 RS, ~12 min) for fast feedback on core safety properties.
6. **Run simulation** (2 clusters, 9 RS, configurable duration) for deeper coverage at production-scale RS count.
7. **Run liveness simulation** (2 clusters, 2 RS, per property) to verify progress properties under fair scheduling.
8. **If a violation is found**, the counterexample trace shows the exact failure sequence. Refine the design and repeat from step 3.

### Running Verification

**Exhaustive (2 clusters, 2 RS):**

```sh
java -XX:+UseParallelGC \
  -cp tla2tools.jar:CommunityModules-deps.jar \
  tlc2.TLC ConsistentFailover.tla -config ConsistentFailover.cfg \
  -workers auto -cleanup
```

**Simulation (2 clusters, 9 RS, configurable duration):**

```sh
java -XX:+UseParallelGC \
  -Dtlc2.TLC.stopAfter=3600 \
  -cp tla2tools.jar:CommunityModules-deps.jar \
  tlc2.TLC ConsistentFailover.tla -config ConsistentFailover-sim.cfg \
  -simulate -depth 10000 -workers auto
```

**Liveness (per-property, example: AbortCompletion):**

```sh
java -XX:+UseParallelGC \
  -Dtlc2.TLC.stopAfter=3600 \
  -cp tla2tools.jar:CommunityModules-deps.jar \
  tlc2.TLC ConsistentFailover.tla \
  -config ConsistentFailover-sim-liveness-ac.cfg \
  -simulate -depth 10000 -workers auto
```

### Recommended Durations for Simulation

| Tier | Duration | Use Case |
|------|----------|----------|
| Quick feedback | 300s (5 min) | Feedback during development |
| Post-change | 900s (15 min) | Validation after completing a change |
| Post-phase | 3600s (1 hr) | Milestone verification |
| Nightly CI | 28800s (8 hr) | Continuous overnight run for rare interleavings |

---

## 2. Common Change Patterns

### Pattern A: Adding or Modifying an HA Group State Transition

**When:** You are adding a new HA group state, adding a new transition between existing states, or modifying the guards on an existing transition.

**Where to edit:**

1. **`Types.tla`** — Add the new state to `HAGroupState` and the new transition pair to `AllowedTransitions`. Update `ActiveStates`, `StandbyStates`, or  `TransitionalActiveStates` if the new state maps to one of those roles. Update `RoleOf` accordingly. If liveness uses named state sets (`StableClusterStates`, `FailoverCompletionAntecedentStates`, `AbortCompletionAntecedentStates`, `NotANISClusterStates`), extend them when a new state should appear in a `~>` antecedent or consequent.
2. **`HAGroupStore.tla`** or **`Admin.tla`** — Add or modify the action that produces the new transition. Set appropriate ZK connectivity guards (`zkPeerConnected`, `zkPeerSessionAlive`, `zkLocalConnected`).
3. **`ConsistentFailover.tla`** — Wire the new action into the `Next` disjunction and, if appropriate, into the `Fairness` condition. Add a new invariant if the transition introduces new safety requirements.

**What to verify (primary invariants and constraints):**

| Invariant / Constraint | Why |
|---|---|
| `MutualExclusion` | The new state must not allow two clusters in the ACTIVE role simultaneously |
| `TransitionValid` | The new `(from, to)` pair must be in `AllowedTransitions` |
| `AbortSafety` | If the new state participates in the abort protocol, AbTAIS peer constraints must hold |
| `TypeOK` | The new state must be a member of `HAGroupState` |

---

### Pattern B: Modifying Writer Mode Transitions

**When:** You are adding a new writer mode, changing the degradation/recovery paths, modifying CAS failure behavior, or changing the AIS-to-ANIS coupling.

**Where to edit:**

1. **`Types.tla`** — Add the new mode to `WriterMode` if adding a new mode. Add any new `(from, to)` pair to `AllowedWriterTransitions`.
2. **`Writer.tla`** — Add or modify the action. Set `zkLocalConnected` guard on any action that performs a ZK CAS write. Ensure the AIS-to-ANIS coupling fires atomically when the first RS degrades.
3. **`ConsistentFailover.tla`** — Wire new actions into `Next` and `Fairness`.

**What to verify (primary invariants and constraints):**

| Invariant / Constraint | Why |
|---|---|
| `AISImpliesInSync` | AIS must never coexist with degraded writer modes |
| `WriterClusterConsistency` | Degraded writer modes only on appropriate cluster states |
| `WriterTransitionValid` | Every writer mode change follows `AllowedWriterTransitions` |
| `MutualExclusion` | Writer mode changes must not break the cluster-level mutual exclusion invariant |

---

### Pattern C: Modifying the Replay State Machine or Failover Trigger

**When:** You are changing the replay advance/rewind logic, modifying the `shouldTriggerFailover()` guards, changing how `failoverPending` or `inProgressDirEmpty` are managed, or modifying the replay state transitions.

**Where to edit:**

1. **`Types.tla`** — Update `ReplayStateSet` if adding a new replay state. Update `AllowedReplayTransitions` for any new replay `(from, to)` pair.
2. **`Reader.tla`** — Modify `ReplayAdvance`, `ReplayRewind`, `ReplayBeginProcessing`, `ReplayFinishProcessing`, or `TriggerFailover`.
3. **`HAGroupStore.tla`** — If changing listener folds (recoveryListener or degradedListener effects on S-entry or DS-entry actions), modify the `replayState'` assignments in `PeerReactToAIS`, `PeerReactToANIS`, and `AutoComplete`.

**What to verify (primary invariants and constraints):**

| Invariant / Constraint | Why |
|---|---|
| `FailoverTriggerCorrectness` | STA->AIS requires failoverPending, inProgressDirEmpty, replayState=SYNC |
| `NoDataLoss` | Zero RPO: the core safety property for failover |
| `ReplayRewindCorrectness` | SYNCED_RECOVERY->SYNC must equalize replay counters |
| `ReplayTransitionValid` | Every replay state change follows `AllowedReplayTransitions` |
| `FailoverCompletion` (liveness) | STA and abort states must eventually resolve |

---

### Pattern D: Modifying ZK Coordination or Adding a ZK Failure Mode

**When:** You are changing how ZK connection loss or session expiry affects the protocol, adding a new ZK failure mode, modifying the ATS reconciliation fold, or changing which actions are guarded by ZK connectivity.

**Where to edit:**

1. **`ZK.tla`** — Add or modify ZK lifecycle actions. If adding a new failure mode, add both the failure and recovery actions. Ensure session expiry implies disconnection (`ZKSessionConsistency` invariant).
2. **`HAGroupStore.tla`**, **`Writer.tla`**, **`Reader.tla`** — Update `zkPeerConnected`/`zkPeerSessionAlive`/`zkLocalConnected` guards on actions affected by the ZK change.
3. **`ConsistentFailover.tla`** — Wire new ZK actions into `Next`. Classify ZK failure actions in Tier 4 (no fairness) and ZK recovery actions in Tier 2 (WF, encoding the ZK Liveness Assumption).

**What to verify (primary invariants and constraints):**

| Invariant / Constraint | Why |
|---|---|
| `ZKSessionConsistency` | Session expiry must imply disconnection |
| `MutualExclusion` | ZK failures must not create a dual-active window |
| `AbortSafety` | AbTAIS peer constraints must hold after reconciliation |
| `AbortCompletion` (liveness) | Abort states must eventually resolve under ZK recovery |
| `FailoverCompletion` (liveness) | Failover must eventually complete under ZK recovery |

---

### Pattern E: Adding a New Invariant or Action Constraint

**When:** Your proposed change introduces new safety requirements that are not covered by existing invariants or action constraints.

**Where to edit:**

1. **`ConsistentFailover.tla`** — Define the invariant predicate or action constraint. Place it near semantically related invariants. Add a `THEOREM` declaration.
2. **All `.cfg` files** — Add the new invariant to the `INVARIANT` section or the new action constraint to the `ACTION_CONSTRAINT` section in all five configuration files.

**What to verify:**

- The new invariant passes under the current specification (no false positives).
- All existing invariants still pass (no regressions from any spec changes needed to support the new invariant).
- If the invariant is a state invariant, add it to `INVARIANT` in all `.cfg` files.
- If the invariant is an action constraint, add it to `ACTION_CONSTRAINT` in all `.cfg` files.

---

## 3. Invariant Reference: When to Check What

The following tables map each invariant and action constraint to the change areas where it is most relevant. When modifying a given area, prioritize the invariants listed for that area.

### Core Safety Invariants

| Invariant | Check When Changing |
|---|---|
| `MutualExclusion` | HA group state transitions, ZK watcher delivery, abort protocol, ATS reconciliation, any `RoleOf` mapping |
| `AbortSafety` | Abort protocol (AdminAbortFailover, PeerReactToAbTS), ATS reconciliation fold (ZKPeerReconnect, ZKPeerSessionRecover) |
| `ZKSessionConsistency` | ZK session expiry/recovery, peer connection lifecycle |
| `AISImpliesInSync` | Writer degradation (WriterToStoreFwd, WriterInitToStoreFwd), ANISToAIS recovery, RSCrash |
| `WriterClusterConsistency` | Writer mode transitions, standby entry lifecycle reset, ANISTSToATS (writer mode preservation) |

### Action Constraints

| Constraint | Check When Changing |
|---|---|
| `TransitionValid` | Any HA group state transition, AllowedTransitions table |
| `WriterTransitionValid` | Any writer mode transition, AllowedWriterTransitions table, standby entry lifecycle reset |
| `ReplayTransitionValid` | Any replay state transition, listener folds (recoveryListener, degradedListener) |
| `AIStoATSPrecondition` | AdminStartFailover guards, outDirEmpty semantics, writer mode guards for AIS path |
| `AntiFlapGate` | Anti-flapping timer, ANISToAIS guards, WaitTimeForSync semantics |
| `ANISTStoATSPrecondition` | ANISTSToATS guards, forwarder drain, anti-flapping timer |
| `FailoverTriggerCorrectness` | TriggerFailover guards, shouldTriggerFailover() conditions, failoverPending lifecycle |
| `NoDataLoss` | Replay state machine, failover trigger, rewind correctness |
| `ReplayRewindCorrectness` | ReplayRewind action, SYNCED_RECOVERY->SYNC CAS, lastRoundProcessed/lastRoundInSync counters |

### Liveness Properties

| Property | Check When Changing |
|---|---|
| `FailoverCompletion` | AutoComplete, TriggerFailover, replay state machine, ZK recovery, HDFS recovery |
| `DegradationRecovery` | Writer recovery chain (S&F->S&FWD->SYNC), ANISToAIS, anti-flapping timer, HDFSUp, RS restart |
| `AbortCompletion` | AutoComplete, ZK local connectivity, abort state transitions |

---

## 4. Module-Implementation Mapping

Use this to quickly locate which spec module corresponds to the implementation
code you are changing:

| Implementation Class / Method | Spec Module | Key Actions |
|---|---|---|
| `HAGroupStoreManager` | `ConsistentFailover.tla` | `Init`, `Next`, `Fairness`, invariants |
| `FailoverManagementListener.onStateChange()` L653-706 | `HAGroupStore.tla` | `PeerReactToATS`, `PeerReactToANIS`, `PeerReactToAbTS`, `PeerReactToAIS`, `ReactiveTransitionFail` |
| `createLocalStateTransitions()` L140-150 | `HAGroupStore.tla` | `AutoComplete` (AbTS->S, AbTAIS->AIS/ANIS, AbTANIS->ANIS) |
| `StoreAndForwardModeImpl.startHAGroupStoreUpdateTask()` L71-87 | `HAGroupStore.tla` | `ANISHeartbeat` |
| `HAGroupStoreManager.setHAGroupStatusToSync()` L341-355 | `HAGroupStore.tla` | `ANISToAIS`, `ANISTSToATS` |
| `initiateFailoverOnActiveCluster()` L375-400 | `Admin.tla` | `AdminStartFailover` (AIS->ATS or ANIS->ANISTS) |
| `setHAGroupStatusToAbortToStandby()` L419-425 | `Admin.tla` | `AdminAbortFailover` (STA->AbTS) |
| `SyncModeImpl.onFailure()` L61-74 | `Writer.tla` | `WriterToStoreFwd`, `WriterToStoreFwdFail` |
| `SyncAndForwardModeImpl.onFailure()` L66-78 | `Writer.tla` | `WriterSyncFwdToStoreFwd`, `WriterSyncFwdToStoreFwdFail` |
| `ReplicationLogDiscoveryForwarder.init()` L98-108 | `Writer.tla` | `WriterSyncToSyncFwd` |
| `ReplicationLogDiscoveryForwarder.processFile()` L133-152 | `Writer.tla` | `WriterStoreFwdToSyncFwd` |
| `ReplicationLogDiscoveryForwarder.processNoMoreRoundsLeft()` L155-184 | `Writer.tla` | `WriterSyncFwdToSync` |
| `ReplicationLogDiscoveryReplay.replay()` L323-351 | `Reader.tla` | `ReplayAdvance`, `ReplayRewind` |
| `ReplicationLogDiscoveryReplay.shouldTriggerFailover()` L500-533 | `Reader.tla` | `TriggerFailover` |
| NameNode crash/recovery | `HDFS.tla` | `HDFSDown`, `HDFSUp` |
| JVM crash, OOM, kill signal | `RS.tla` | `RSCrash` |
| `StoreAndForwardModeImpl.onFailure()` L115-123 | `RS.tla` | `RSAbortOnLocalHDFSFailure` |
| Kubernetes/YARN pod restart | `RS.tla` | `RSRestart` |
| `HAGroupStoreClient.validateTransitionAndGetWaitTime()` L1027-1046 | `Clock.tla` | `Tick` |
| `HAGroupStoreClient.createCacheListener()` L894-906 | `ZK.tla` | `ZKPeerDisconnect`, `ZKPeerReconnect`, `ZKLocalDisconnect`, `ZKLocalReconnect` |
| Curator session management | `ZK.tla` | `ZKPeerSessionExpiry`, `ZKPeerSessionRecover` |
| `HAGroupStoreRecord.HAGroupState` enum L51-65 | `Types.tla` | `HAGroupState`, `AllowedTransitions` |
| `ClusterRoleRecord.ClusterRole` enum L59-107 | `Types.tla` | `ClusterRole`, `RoleOf` |
| `ReplicationLogGroup` mode classes | `Types.tla` | `WriterMode` |

---

## 5. Tips for Effective Spec-Driven Development

1. **Start small.** Make the minimal change to the spec that captures your proposed design. Run exhaustive verification first (~12 min). Add complexity incrementally.

2. **Read counterexample traces carefully.** When TLC reports a violation, it produces a step-by-step trace. Each step is an action name + the values of every state variable. The trace is the exact sequence of events that breaks your invariant.

3. **Use simulation for production-scale RS counts.** The exhaustive model uses 2 RS per cluster -- sufficient for CAS race coverage but not for complex multi-RS writer interleavings. The simulation model uses 9 RS to exercise production-scale scenarios like 4 RS in S&F, 3 in SYNC_AND_FWD, 2 in SYNC simultaneously.

4. **Check liveness after safety.** Liveness verification is more expensive (Buchi automaton construction) and only meaningful if safety holds. Always pass exhaustive and simulation safety first.

5. **Preserve symmetry reduction.** RS identifiers are interchangeable (all start in INIT, identical action sets). When adding RS-level behavior, keep the symmetry. Cluster identifiers are asymmetric (AIS vs S at Init) and cannot use symmetry.

6. **Check both tiers.** A change that passes exhaustive (2 RS) may fail in simulation (9 RS) due to deeper per-RS interleavings. Always run both.

7. **New state variables.** Declare them in `SpecState.tla` (single `VARIABLE` line), add them to the `vars` tuple in `ConsistentFailover.tla`, extend every sub-module action’s `UNCHANGED` / primed update lists as needed, and wire new actions into `Next` and `Fairness`.

8. **Update all five `.cfg` files.** When adding a new invariant or action constraint, add it to `ConsistentFailover.cfg`, `ConsistentFailover-sim.cfg`, and all three liveness configs.

9. **Classify new actions by fairness tier.** Every new action must be
   classified into one of four tiers:
   - **Tier 1 (WF):** Guards depend only on protocol state, no env var guards
   - **Tier 2 (WF):** ZK recovery actions (encodes ZK Liveness Assumption)
   - **Tier 3 (SF):** Guards depend on oscillating environment variables
   - **Tier 4 (none):** Non-deterministic faults, operator actions, CAS failures

---

## 6. How To: Understanding a TLA+ Counterexample Trace

When TLC finds an invariant violation it produces a counterexample trace, the exact sequence of states from `Init` to the violating state. Each state shows the action that produced it and the values of every state variable. This is the spec's most valuable output, the precise interleaving that breaks your invariant.

### 6.1 Anatomy of a Trace Step

```
Error: Invariant <InvariantName> is violated.
Error: The behavior up to this point is:
State 1: <Initial predicate>
/\ clusterState = (c1 :> "AIS" @@ c2 :> "S")
/\ writerMode = ...
...
State N: <Action line L, col C to line L2, col C2 of module Module>
/\ clusterState = ...       ← the state that violates the invariant
```

TLC prints all 13 variables in every state. Diff consecutive states manually to see what changed. Start from the last state and work backwards to find the action that introduced the bad value.

### 6.2 Key Variables to Watch

For different invariant violations, focus on different variables:

| Invariant Violated | Key Variables to Track |
|---|---|
| `MutualExclusion` | `clusterState` for both clusters -- look for both in `ActiveStates` |
| `AISImpliesInSync` | `clusterState`, `writerMode`, `outDirEmpty` -- look for AIS with S&F or non-empty OUT |
| `NoDataLoss` / `FailoverTriggerCorrectness` | `clusterState` (STA->AIS), `failoverPending`, `inProgressDirEmpty`, `replayState` |
| `ZKSessionConsistency` | `zkPeerSessionAlive`, `zkPeerConnected` -- look for session dead but connected |
| `TransitionValid` | `clusterState` old and new values -- look for pair not in `AllowedTransitions` |
| `WriterTransitionValid` | `writerMode` old and new values for the changing RS |
| `AbortSafety` | `clusterState` for both clusters -- look for AbTAIS with unexpected peer state |

### 6.3 Common Patterns in Counterexample Traces

| Pattern | What to Look For | Typical Invariants Violated |
|---|---|---|
| **ZK partition during failover** | Peer disconnect between ATS write and STA detection | `MutualExclusion` (if reconciliation is wrong) |
| **CAS race** | Two RS detect HDFS failure, second gets BadVersionException | `WriterTransitionValid`, `AISImpliesInSync` |
| **Missed watcher** | `ReactiveTransitionFail` fires, transition permanently lost | Liveness violations |
| **Anti-flap bypass** | ANIS->AIS fires while timer is positive | `AntiFlapGate` |
| **Stale writer after standby entry** | S&F/S&FWD writer persists through ATS->S | `WriterClusterConsistency` |
| **Replay rewind race** | DS-entry fold fires before ReplayRewind CAS | `ReplayRewindCorrectness` |
| **Failover without replay completion** | STA->AIS without replayState=SYNC | `NoDataLoss`, `FailoverTriggerCorrectness` |
| **Dual active via abort** | AbTAIS reached with peer in active state | `AbortSafety`, `MutualExclusion` |

### 6.4 Using an LLM to Analyze Traces

Paste the trace into an LLM with relevant context. Use this template:

````
I have a TLA+ counterexample trace from the Phoenix Consistent Failover
specification.

## Violated Invariant
[Paste invariant definition from ConsistentFailover.tla]

## Counterexample Trace
[Paste full TLC output starting from "Error: Invariant ..."]

## Relevant Spec Module(s)
[Paste action definitions from the .tla files named in state headers]

## Questions
1. What is the root cause of this invariant violation?
2. At which state does the critical event occur?
3. Is this a spec bug or an implementation bug?
4. What change would fix this?
````

A good analysis will identify the critical state (often 2-3 steps before the violation), explain the interleaving, and suggest concrete fixes.

Apply the fix, re-run TLC exhaustive, re-run simulation. If a new violation appears, paste the new trace and repeat.
