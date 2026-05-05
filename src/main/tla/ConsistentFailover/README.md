# Phoenix Consistent Failover -- TLA+ Specification

Formal specification of the Phoenix Consistent Failover protocol using TLA+ and the TLC model checker.  The spec verifies safety properties (mutual exclusion, zero RPO, abort correctness) under arbitrary interleavings of admin actions, HDFS failures, RS crashes, ZK connection/session failures, watcher retry exhaustion, and the anti-flapping timer.

## Literate Specification

Literate programming versions of all specification files are available in the [`markdown/`](markdown/) directory. Each file includes the complete TLA+ code with comments converted to prose that discusses modeling choices, tradeoffs, and implementation traceability in depth.

### Root Orchestrator

| Literate Version | Source | Description |
|-----------------|--------|-------------|
| [`ConsistentFailover.md`](markdown/ConsistentFailover.md) | [`ConsistentFailover.tla`](ConsistentFailover.tla) | Init, Next, Spec, invariants, action constraints, fairness, liveness properties |
| [`SpecState.md`](markdown/SpecState.md) | [`SpecState.tla`](SpecState.tla) | Shared `VARIABLE` declarations (13 state functions) for the root and all sub-modules |

### Pure Definitions

| Literate Version | Source | Description |
|-----------------|--------|-------------|
| [`Types.md`](markdown/Types.md) | [`Types.tla`](Types.tla) | Constants, 14 HA group states, allowed transitions, cluster roles, writer modes, replay states, liveness state sets, writer/replay transition tables, anti-flapping timer helpers |

### Actor Modules

| Literate Version | Source | Description |
|-----------------|--------|-------------|
| [`HAGroupStore.md`](markdown/HAGroupStore.md) | [`HAGroupStore.tla`](HAGroupStore.tla) | Peer-reactive transitions, auto-completion, S&F heartbeat, ANIS recovery, ANISTS drain, retry exhaustion |
| [`Admin.md`](markdown/Admin.md) | [`Admin.tla`](Admin.tla) | Operator-initiated failover (AIS->ATS, ANIS->ANISTS) and abort (STA->AbTS) |
| [`Writer.md`](markdown/Writer.md) | [`Writer.tla`](Writer.tla) | Per-RS writer mode state machine: startup, degradation (CAS success/failure), recovery, drain |
| [`Reader.md`](markdown/Reader.md) | [`Reader.tla`](Reader.tla) | Standby replay state machine: advance, rewind, in-progress directory, failover trigger |

### Environment Modules

| Literate Version | Source | Description |
|-----------------|--------|-------------|
| [`HDFS.md`](markdown/HDFS.md) | [`HDFS.tla`](HDFS.tla) | NameNode crash/recovery environment actions |
| [`RS.md`](markdown/RS.md) | [`RS.tla`](RS.tla) | RS crash, local HDFS abort, process supervisor restart |
| [`Clock.md`](markdown/Clock.md) | [`Clock.tla`](Clock.tla) | Anti-flapping countdown timer (Lamport CHARME 2005) |
| [`ZK.md`](markdown/ZK.md) | [`ZK.tla`](ZK.tla) | ZK peer/local connection lifecycle, session expiry/recovery, ATS reconciliation |

### TLC Configurations

| Literate Version | Source | Description |
|-----------------|--------|-------------|
| [`ConsistentFailover-cfg.md`](markdown/ConsistentFailover-cfg.md) | [`ConsistentFailover.cfg`](ConsistentFailover.cfg) | Exhaustive safety: 2 clusters, 2 RS, full state-space exploration |
| [`ConsistentFailover-sim-cfg.md`](markdown/ConsistentFailover-sim-cfg.md) | [`ConsistentFailover-sim.cfg`](ConsistentFailover-sim.cfg) | Simulation safety: 2 clusters, 9 RS, random trace sampling |
| [`ConsistentFailover-sim-liveness-ac-cfg.md`](markdown/ConsistentFailover-sim-liveness-ac-cfg.md) | [`ConsistentFailover-sim-liveness-ac.cfg`](ConsistentFailover-sim-liveness-ac.cfg) | AbortCompletion liveness (5 fairness clauses) |
| [`ConsistentFailover-sim-liveness-fc-cfg.md`](markdown/ConsistentFailover-sim-liveness-fc-cfg.md) | [`ConsistentFailover-sim-liveness-fc.cfg`](ConsistentFailover-sim-liveness-fc.cfg) | FailoverCompletion liveness (15 fairness clauses) |
| [`ConsistentFailover-sim-liveness-dr-cfg.md`](markdown/ConsistentFailover-sim-liveness-dr-cfg.md) | [`ConsistentFailover-sim-liveness-dr.cfg`](ConsistentFailover-sim-liveness-dr.cfg) | DegradationRecovery liveness (25 fairness clauses) |

## Solution Design

Phoenix clusters are deployed in pairs across distinct failure domains. The Consistent Failover protocol provides zero-RPO failover between a Primary (active) and Standby cluster using Phoenix Synchronous Replication. Every committed mutation on the active cluster is synchronously written to a replication log file on the standby cluster's HDFS before the mutation is acknowledged. A set of replay threads on the standby asynchronously consumes these log files round-by-round, applying changes to local HBase tables so the standby remains close to in-sync with the active.

```
 ┌──────────────────────────────────────────────────────────────────┐
 │                      Active Cluster (FD 1)                       │
 │                                                                  │
 │  Admin ──► HAGroupStoreManager ─ setData (CAS) ─► ZK Quorum 1    │
 │                                                        ▲         │
 │            HAGroupStoreClient ── watch (local) ────────┘         │
 │                 ·                                                │
 │                 · watch (peer) ···············► ZK Quorum 2      │
 │                                                 (remote)         │
 │                                                                  │
 │  ReplicationLogWriter (per RS)                                   │
 │    ├── SYNC ─────────────────────────► Standby HDFS / IN         │
 │    └── STORE_AND_FORWARD ──► HDFS / OUT                          │
 │                                   │                              │
 │                              Forwarder ──► Standby HDFS / IN     │
 └──────────────────────────────────────────────────────────────────┘

 ┌──────────────────────────────────────────────────────────────────┐
 │                     Standby Cluster (FD 2)                       │
 │                                                                  │
 │            HAGroupStoreManager ─ setData (CAS) ─► ZK Quorum 2    │
 │                                                        ▲         │
 │            HAGroupStoreClient ── watch (local) ────────┘         │
 │                 ·                                                │
 │                 · watch (peer) ···············► ZK Quorum 1      │
 │                                                 (remote)         │
 │                                                                  │
 │  HDFS / IN ──► ReplicationLogReader ──► HBase Tables             │
 │                  (round-by-round replay)                         │
 └──────────────────────────────────────────────────────────────────┘
```

### Actors

| Actor | Role |
|-------|------|
| **Admin** | Human operator; initiates or aborts failover via CLI |
| **HAGroupStoreManager** | Per-cluster coprocessor endpoint; automates peer-reactive state transitions via `FailoverManagementListener` with up to 2 retries |
| **ReplicationLogWriter** | Per-RegionServer on the active cluster; captures mutations post-WAL-commit and writes them to standby HDFS (`SYNC` mode) or local HDFS (`STORE_AND_FORWARD` mode) |
| **ReplicationLogReader** | On the standby cluster; replays replication logs round-by-round, manages the consistency point, and triggers the final STA-to-AIS transition |
| **HAGroupStoreClient** | Per-RS ZK interaction layer; caches state, enforces anti-flapping, validates transitions |

### State Machines

The protocol is governed by six interrelated state machines, all modeled in this specification:

**HA Group State (14 states).**
Each cluster's lifecycle: `ACTIVE_IN_SYNC`, `ACTIVE_NOT_IN_SYNC`, `ACTIVE_IN_SYNC_TO_STANDBY`, `ACTIVE_NOT_IN_SYNC_TO_STANDBY`, `STANDBY`, `STANDBY_TO_ACTIVE`, `DEGRADED_STANDBY`, `ABORT_TO_ACTIVE_IN_SYNC`, `ABORT_TO_ACTIVE_NOT_IN_SYNC`, `ABORT_TO_STANDBY`, `ACTIVE_WITH_OFFLINE_PEER` (reachable when `UseOfflinePeerDetection = TRUE`), `ACTIVE_NOT_IN_SYNC_WITH_OFFLINE_PEER` (reachable when `UseOfflinePeerDetection = TRUE`), `OFFLINE` (reachable when `UseOfflinePeerDetection = TRUE`), `UNKNOWN`. States map to roles visible to clients: ACTIVE (serves reads/writes), ACTIVE_TO_STANDBY (mutations blocked), `STANDBY`, `STANDBY_TO_ACTIVE`, `OFFLINE`, `UNKNOWN`.

**Replication Writer Mode (4 modes per RS).**
`INIT` (pre-initialization), `SYNC` (writing directly to standby HDFS), `STORE_AND_FORWARD` (writing locally when standby is unavailable), `SYNC_AND_FORWARD` (draining local queue while also writing synchronously). A write error in `STORE_AND_FORWARD` mode triggers RS abort (fail-stop).

**Replication Replay State (4 states per cluster).**
`NOT_INITIALIZED`, `SYNC` (fully in sync), `DEGRADED` (active peer in `ANIS`; `lastRoundInSync` frozen), `SYNCED_RECOVERY` (active returned to `AIS`; replay rewinds to `lastRoundInSync`).

**Combined Product State.**
The (ActiveClusterState, StandbyClusterState) pair progresses through a well-defined sequence during failover. AIS path: `(AIS,S)` -> `(ATS,S)` -> `(ATS,STA)` -> `(ATS,AIS)` -> `(S,AIS)`. ANIS path: `(ANIS,DS)` -> `(ANISTS,DS)` -> `(ATS,DS)` -> `(ATS,STA)` -> `(ATS,AIS)` -> `(S,AIS)`.

**Anti-Flapping Timer.**
A countdown timer (Lamport CHARME 2005) gates the `ANIS` -> `AIS` transition: the OUT directory must remain empty and all RS must be in SYNC for a configurable number of ticks before the cluster may return to `ACTIVE_IN_SYNC`.

**ZK Connection/Session Lifecycle.**
Three per-cluster booleans model ZK health: peer connection state, peer session liveness, and local connection state. Peer-reactive transitions are guarded on peer connectivity and session liveness. Auto-completion, heartbeat, and writer ZK writes are guarded on local connectivity. Watcher-driven transitions may be permanently lost after retry exhaustion.

### Coordination via ZooKeeper

Each cluster stores its own HA group state as a ZooKeeper znode.  Peer clusters observe each other's state changes via Curator watchers connected to the remote ZK quorum.  State updates use ZK's versioned `setData` via optimistic CAS locking. A writer reads the current version, computes a new state, and writes with the read version. A `BadVersionException` triggers re-read and retry.

The final failover step is two independent ZK writes. The new active writes `ACTIVE_IN_SYNC` to its own ZK, then the old active's `FailoverManagementListener` reactively writes `STANDBY` to its own ZK. Safety during this window is maintained because the old active is in `ACTIVE_IN_SYNC_TO_STANDBY`, a role that blocks all client mutations.

### Failover Sequence

1. **Initiation.** Admin transitions the active cluster from `AIS` to `ATS` (AIS failover path) or from `ANIS` to `ANISTS` (ANIS failover path). The `ACTIVE_TO_STANDBY` role blocks all mutations. On the ANIS path, the forwarder must drain the OUT directory and the anti-flapping gate must open before `ANISTS` advances to `ATS`.

2. **Standby detection.** The standby's `FailoverManagementListener` detects the peer's ATS via a ZK watcher and reactively transitions the standby from `S` (or `DS` on the ANIS path) to `STA`. This sets `failoverPending = true`.

3. **Log replay.** The standby replays all outstanding replication logs. The failover trigger requires: `failoverPending`, in-progress directory empty, and no new files in the time window (`lastRoundProcessed >= lastRoundInSync`).

4. **Activation.** The standby writes `ACTIVE_IN_SYNC` to its own ZK.

5. **Completion.** The old active's listener detects the peer's AIS and reactively writes `STANDBY` to its own ZK.

### Why Formal Verification

Exhaustive model checking with TLC explores every reachable state under all possible interleavings of these actors and failure modes, proving that the crucial safety properties of mutual exclusion, zero RPO, and abort correctness hold universally.

The exhaustive model check verifies the following over the full reachable state space:

**State invariants** (hold in every reachable state):

| Invariant | Property |
|-----------|----------|
| `TypeOK` | All 13 variables have valid types |
| `MutualExclusion` | At most one cluster in the ACTIVE role at any time |
| `AbortSafety` | AbTAIS requires peer in AbTS, S, DS, or OFFLINE (abort, post-partition reconciliation, or offline peer) |
| `AISImpliesInSync` | AIS implies outDirEmpty and all RS in SYNC/INIT/DEAD |
| `WriterClusterConsistency` | Degraded writer modes only on non-AIS active or transitional states |
| `ZKSessionConsistency` | Peer session expiry implies peer disconnection |

**Action constraints** (hold on every state transition):

| Constraint | Property |
|------------|----------|
| `TransitionValid` | Every cluster state change follows `AllowedTransitions` |
| `WriterTransitionValid` | Every writer mode change follows `AllowedWriterTransitions` |
| `ReplayTransitionValid` | Every replay state change follows `AllowedReplayTransitions` |
| `AIStoATSPrecondition` | AIS->ATS requires outDirEmpty and all RS in SYNC/DEAD |
| `AntiFlapGate` | ANIS->AIS blocked while anti-flapping timer is positive |
| `ANISTStoATSPrecondition` | ANISTS->ATS requires outDirEmpty and anti-flapping gate open |
| `FailoverTriggerCorrectness` | STA->AIS requires failoverPending, inProgressDirEmpty, replayState=SYNC |
| `NoDataLoss` | Zero RPO: STA->AIS only when replay is complete |
| `ReplayRewindCorrectness` | SYNCED_RECOVERY->SYNC equalizes replay counters (lastRoundProcessed = lastRoundInSync) |

**Liveness properties** (verified via simulation with per-property fairness):

Liveness properties guarantee progress. Transient states eventually resolve to stable states under fair scheduling. Each property is checked with a per-property fairness formula containing only the temporal clauses on its critical path. The full `Fairness` formula has 43 temporal clauses, which would cause TLC's Buchi automaton construction to blow up, because it is exponential. Per-property formulas keep this tractable. Exhaustive liveness checking is infeasible at this state-space scale because TLC's SCC algorithm requires the full product graph (behavior graph x automaton) in memory, so probabilistic assurance is provided through simulation.

| Property | Clauses | Description |
|----------|---------|-------------|
| `FailoverCompletion` | 15 | Standby-side and abort transient states (`STA`, `AbTAIS`, `AbTANIS`, `AbTS`) eventually resolve to a stable state (`AIS`, `ANIS`, `S`). ATS/ANISTS excluded: resolution depends on peer state and ZK connectivity at the right moment, with no fairness on admin actions or ZK disconnect. |
| `DegradationRecovery` | 25 | ANIS with available peer HDFS eventually progresses out of ANIS via the writer recovery chain: `S&F` -> `S&FWD` -> `SYNC`, anti-flap timer expires, `ANIS` -> `AIS`. May also leave ANIS via failover (`ANIS` -> `ANISTS`). |
| `AbortCompletion` | 5 | Every abort state (`AbTS`, `AbTAIS`, `AbTANIS`) eventually auto-completes to a stable state (`AIS`, `ANIS`, `S`). Deterministic under WF on `AutoComplete`. |

## Module Architecture

```
ConsistentFailover.tla          (root orchestrator: Init, Next, SafetySpec/Spec, invariants)
  |
  +-- SpecState.tla             (shared VARIABLE declarations for root + all sub-modules)
  |
  +-- Types.tla                 (pure definitions: states, transitions, roles, helpers)
  |
  +-- HAGroupStore.tla          (peer-reactive transitions, auto-completion, retry exhaustion)
  +-- Admin.tla                 (operator-initiated failover and abort)
  +-- Writer.tla                (per-RS replication writer mode state machine)
  +-- Reader.tla                (standby-side replication replay state machine)
  +-- HDFS.tla                  (HDFS NameNode crash/recovery)
  +-- RS.tla                    (RS crash, local HDFS abort, process supervisor restart)
  +-- Clock.tla                 (anti-flapping countdown timer)
  +-- ZK.tla                    (ZK connection/session lifecycle)
```

All sub-modules extend `SpecState.tla` and `Types.tla` for shared variables and definitions. `ConsistentFailover.tla` composes them via `INSTANCE`.

## Modules

| Module | Description |
|--------|-------------|
| `SpecState.tla` | Declares the 13 specification variables once; root and sub-modules `EXTEND SpecState, Types`. |
| `Types.tla` | Pure definitions: 14 HA group states, allowed transitions, cluster roles, writer modes, replay states, liveness state sets, writer/replay transition tables, anti-flapping timer helpers. No variables. |
| `ConsistentFailover.tla` | Root orchestrator. Defines Init/Next/SafetySpec/Spec, instances sub-modules, defines all invariants and action constraints. |
| `HAGroupStore.tla` | 11 action schemas. Peer-reactive transitions (`PeerReactToATS`, `PeerReactToANIS`, `PeerReactToAbTS`, `PeerReactToAIS`), local auto-completion (`AutoComplete`), STORE_AND_FORWARD heartbeat (`ANISHeartbeat`), ANIS recovery (`ANISToAIS`), ANISTS drain completion (`ANISTSToATS`), retry exhaustion (`ReactiveTransitionFail`), peer OFFLINE detection (`PeerReactToOFFLINE`, `PeerRecoverFromOFFLINE`). ATS->S transitions include writer lifecycle reset (live writers reset to INIT, OUT directory cleared; DEAD writers preserved for RSRestart). S-entry actions atomically set `replayState = SYNCED_RECOVERY` (recoveryListener fold); DS-entry sets `replayState = DEGRADED` (degradedListener fold). All peer-reactive actions guarded on `zkPeerConnected` and `zkPeerSessionAlive`; OFFLINE lifecycle peer-reactive actions additionally guarded on `UseOfflinePeerDetection`. Auto-completion, heartbeat, recovery, and drain completion guarded on `zkLocalConnected`. |
| `Admin.tla` | `AdminStartFailover` (AIS->ATS or ANIS->ANISTS with peer-state guard), `AdminAbortFailover` (STA->AbTS, clears failoverPending), `AdminGoOffline` (S/DS->OFFLINE, gated on `UseOfflinePeerDetection`), and `AdminForceRecover` (OFFLINE->S, gated on `UseOfflinePeerDetection`). |
| `Writer.tla` | Per-RS writer mode transitions: startup (`WriterInit`, `WriterInitToStoreFwd`, `WriterInitToStoreFwdFail`), degradation (`WriterToStoreFwd`, `WriterToStoreFwdFail`, `WriterSyncFwdToStoreFwd`, `WriterSyncFwdToStoreFwdFail`), recovery (`WriterSyncToSyncFwd`, `WriterStoreFwdToSyncFwd`), drain complete (`WriterSyncFwdToSync`). ZK-writing actions guarded on `zkLocalConnected`. |
| `Reader.tla` | Replay advance (SYNC and DEGRADED), rewind, in-progress directory dynamics, failover trigger (`TriggerFailover` guarded on `zkLocalConnected`). Listener effects (degradedListener, recoveryListener) are folded into HAGroupStore S/DS-entry actions. |
| `HDFS.tla` | `HDFSDown` and `HDFSUp` -- environment actions for NameNode crash/recovery. |
| `RS.tla` | `RSCrash` (any mode->DEAD), `RSAbortOnLocalHDFSFailure` (STORE_AND_FORWARD->DEAD when own HDFS down), `RSRestart` (DEAD->INIT via process supervisor). |
| `Clock.tla` | `Tick` -- advances all per-cluster anti-flapping countdown timers by one tick toward zero. Guarded: only fires when at least one timer is positive. |
| `ZK.tla` | Peer ZK lifecycle (`ZKPeerDisconnect`, `ZKPeerReconnect`, `ZKPeerSessionExpiry`, `ZKPeerSessionRecover`) and local ZK lifecycle (`ZKLocalDisconnect`, `ZKLocalReconnect`). `ZKPeerReconnect` and `ZKPeerSessionRecover` fold a post-abort ATS reconciliation: when local = ATS and peer in {S, DS} at reconnect, `clusterState` is atomically set to AbTAIS (auto-completes to AIS via `AutoComplete`). This resolves the stuck-ATS scenario after abort during inter-cluster partition. |

**Total: 42 action schemas** (some parameterized over cluster and RS).

## Variables

| Variable | Type | Source |
|----------|------|--------|
| `clusterState[c]` | `[Cluster -> HAGroupState]` | HAGroupStoreRecord per-cluster ZK znode |
| `writerMode[c][rs]` | `[Cluster -> [RS -> WriterMode]]` | ReplicationLogGroup per-RS mode |
| `outDirEmpty[c]` | `[Cluster -> BOOLEAN]` | Replication OUT directory state |
| `hdfsAvailable[c]` | `[Cluster -> BOOLEAN]` | NameNode availability (detected via IOException) |
| `antiFlapTimer[c]` | `[Cluster -> 0..WaitTimeForSync]` | Countdown timer (Lamport CHARME 2005) |
| `replayState[c]` | `[Cluster -> ReplayStateSet]` | Standby replay state (NOT_INITIALIZED/SYNC/DEGRADED/SYNCED_RECOVERY) |
| `lastRoundInSync[c]` | `[Cluster -> Nat]` | Last replay round processed while in SYNC |
| `lastRoundProcessed[c]` | `[Cluster -> Nat]` | Last replay round processed (any state) |
| `failoverPending[c]` | `[Cluster -> BOOLEAN]` | STA notification received, waiting for replay completion |
| `inProgressDirEmpty[c]` | `[Cluster -> BOOLEAN]` | No partially-processed replication log files |
| `zkPeerConnected[c]` | `[Cluster -> BOOLEAN]` | peerPathChildrenCache TCP connection state |
| `zkPeerSessionAlive[c]` | `[Cluster -> BOOLEAN]` | Peer ZK session liveness |
| `zkLocalConnected[c]` | `[Cluster -> BOOLEAN]` | pathChildrenCache (local) connection; maps to `isHealthy` |

## Configuration

Five TLC configurations are provided:

### Safety Checking

#### Exhaustive (`ConsistentFailover.cfg`)

| Parameter | Value | Notes |
|-----------|-------|-------|
| `Cluster` | `{c1, c2}` | Exactly 2 clusters forming the HA pair |
| `RS` | `{rs1, rs2}` | 2 region servers per cluster |
| `WaitTimeForSync` | `2` | Anti-flapping timer ticks (small value sufficient for verification) |
| `UseOfflinePeerDetection` | `FALSE` | Feature gate for proactive AWOP/ANISWOP modeling; set `TRUE` to verify OFFLINE peer detection |
| Symmetry | `Permutations(RS)` | RS identifiers are interchangeable; clusters are asymmetric (AIS vs S at Init) |
| State constraint | `lastRoundProcessed[c] <= 3` | Bounds replay counters for tractability |
| Specification | `SafetySpec` | `Init /\ [][Next]_vars` (no fairness) |

#### Simulation (`ConsistentFailover-sim.cfg`)

| Parameter | Value | Notes |
|-----------|-------|-------|
| `Cluster` | `{c1, c2}` | Exactly 2 clusters forming the HA pair |
| `RS` | `{rs1, rs2, ..., rs9}` | 9 region servers per cluster (production-scale per-RS interleaving) |
| `WaitTimeForSync` | `5` | Larger window exercises richer interleavings during anti-flapping wait |
| Symmetry | None | No benefit for random trace sampling |
| State constraint | None | Counters grow organically along each trace |
| Specification | `SafetySpec` | `Init /\ [][Next]_vars` (no fairness) |

### Liveness Checking (Per-Property Simulation)

All runs use 2 clusters, 2 RS per cluster, WaitTimeForSync=2, depth 10000. See the liveness properties table above for property descriptions and critical paths.

| Config File | Specification | Property | Fairness Clauses |
|-------------|---------------|----------|-----------------|
| `ConsistentFailover-sim-liveness-ac.cfg` | `SpecAC` | `AbortCompletion` | 5 |
| `ConsistentFailover-sim-liveness-fc.cfg` | `SpecFC` | `FailoverCompletion` | 15 |
| `ConsistentFailover-sim-liveness-dr.cfg` | `SpecDR` | `DegradationRecovery` | 25 |

### Common Parameters

The initial state is deterministic: one cluster starts in AIS, the other in S.  All writers start in INIT, all HDFS available, all ZK connections alive, anti-flapping timers at zero, replay in SYNCED_RECOVERY (standby) / NOT_INITIALIZED (active).

## Running

Requires JDK 11+ (JDK 17 recommended).  The `tla2tools.jar` must be on the classpath.

**Syntax check (SANY):**

```
java -cp tla2tools.jar tla2sany.SANY ConsistentFailover.tla
```

### Safety Checking

**Exhaustive model check (TLC):**

```
java -XX:+UseParallelGC \
  -cp tla2tools.jar:CommunityModules-deps.jar \
  tlc2.TLC ConsistentFailover.tla -config ConsistentFailover.cfg \
  -workers auto -cleanup
```

**Simulation (8-hour random trace sampling):**

```
java -XX:+UseParallelGC \
  -Dtlc2.TLC.stopAfter=28800 \
  -cp tla2tools.jar:CommunityModules-deps.jar \
  tlc2.TLC ConsistentFailover.tla -config ConsistentFailover-sim.cfg \
  -simulate -depth 10000 -workers auto
```

Simulation generates random execution traces up to depth 10000 (sufficient for ~100 complete failover cycles with 9 RS). The 9-RS model is too large for exhaustive search but ideal for simulation: 40 action schemas × 9 RS create a high branching factor that simulation samples efficiently. The `-Dtlc2.TLC.stopAfter=28800` flag limits the run to 8 hours.

### Liveness Checking

**All 3 properties (8-hour run each):**

```bash
for prop in ac fc dr; do
  java -XX:+UseParallelGC \
    -Dtlc2.TLC.stopAfter=28800 \
    -cp tla2tools.jar:CommunityModules-deps.jar \
    tlc2.TLC ConsistentFailover.tla \
    -config ConsistentFailover-sim-liveness-${prop}.cfg \
    -simulate -depth 10000 -workers auto
done
```

**Single property (example: AbortCompletion):**

```
java -XX:+UseParallelGC \
  -Dtlc2.TLC.stopAfter=28800 \
  -cp tla2tools.jar:CommunityModules-deps.jar \
  tlc2.TLC ConsistentFailover.tla \
  -config ConsistentFailover-sim-liveness-ac.cfg \
  -simulate -depth 10000 -workers auto
```

## Latest Results

### Exhaustive

| Metric | Value |
|--------|-------|
| Configuration | 2 clusters, 2 RS per cluster, WaitTimeForSync=2 |
| Workers | 16 |
| States generated | 2,718,437,761 |
| Distinct states | 170,978,688 |
| Depth | 55 |
| Duration | 24 min 19 sec |
| Date | 2026-04-21 |
| Result | Success |

All 9 state invariants and 9 action constraints verified.  No violations.

### Simulation

| Metric | Value |
|--------|-------|
| Configuration | 2 clusters, 9 RS per cluster, WaitTimeForSync=5 |
| Workers | 128 |
| States checked | 70,448,924,768 |
| Traces generated | 7,044,645 |
| Trace length | 10,000 |
| Seed | -5836228587005873350 |
| Duration | 8 hr |
| Date | 2026-04-21 |
| Result | Success |

All 9 state invariants and 9 action constraints verified at production-scale RS count.  No violations.

### Liveness (Per-Property Simulation)

All runs: 2 clusters, 2 RS per cluster, WaitTimeForSync=2, 128 workers, depth 10000, 1 hr.

| Property | Config | States Checked | Traces | Seed | Date | Result |
|----------|--------|---------------|--------|------|------|--------|
| `AbortCompletion` | `SpecAC` | 2,671,855,331 | 267,165 | -3508296420780792285 | 2026-04-21 | Success |
| `DegradationRecovery` | `SpecDR` | 454,322,354 | 45,430 | 650174316504703997 | 2026-04-21 | Success |
| `FailoverCompletion` | `SpecFC` | 1,107,505,130 | 110,740 | 3654016485672320894 | 2026-04-21 | Success |

All 3 liveness properties verified.  No violations.
