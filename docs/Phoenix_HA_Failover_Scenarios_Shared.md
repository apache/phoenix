# Phoenix HA Failover — Test Scenarios (Team Reference)

**Purpose.** This document enumerates every failover scenario the Phoenix
High-Availability (HA) design is expected to handle, and gives a
reproducible recipe and pass/fail criteria for each. It is meant to be run against a
**staging cluster** where controlled fault injection is permitted.

**Scope.** The clusters are assumed to run on Kubernetes and the recipes use
`kubectl`. Adapt names to your environment — the doc uses these placeholders
throughout:

- `<ns-a>` — namespace of the Primary cluster (starts **Active**)
- `<ns-b>` — namespace of the Secondary cluster (starts **Standby**)
- `<rs>` — a RegionServer pod (e.g. `regionserver-0`)
- `<hmaster>` — an HMaster pod (e.g. `hmaster-0`)
- `<client>` — a pod with the Phoenix client (`sqlline.py`) installed
- `<zk-a>`, `<zk-b>` — the ZooKeeper quorum (JDBC connect string) of each
  cluster, e.g. `zk-0.zk:2181:/hbase`
- `<table>` — the replicated test table

> **All data reads and writes must go through a Phoenix client**, not the
> HBase shell. Replication is driven by Phoenix's server-side coprocessor,
> which only fires on mutations that arrive through the Phoenix write path.
> A raw `hbase shell put` bypasses it entirely and will **not** replicate.
> These recipes use `sqlline.py`; a JDBC/psql-style client works equally
> well. Only genuine HBase admin operations (e.g. a region `split`) use the
> HBase shell.

Nothing here should be run against a production cluster carrying live
traffic.

> **How to read the status labels.** Each scenario is tagged:
> **Ready** (the behavior is implemented and testable today),
> **Not yet** (the code path is still landing — the scenario documents
> expected behavior but will not pass yet), or
> **Design** (the operator surface is still being designed).

---

## 1. Background: what Phoenix HA does

Phoenix HA runs a pair of clusters as a single logical service. At any
moment one cluster is **Active** (serves reads and writes) and the other is
**Standby** (receives a continuous stream of change records so it can take
over with **zero data loss**).

The mechanism is a **replication log**. As the Active cluster commits each
batch of writes, it also writes those changes to a log on shared storage
(HDFS) that the Standby cluster continuously consumes and applies. Two
storage locations matter:

- **Inbound queue** — on the Standby cluster; the Active cluster ships sync
  logs here and the Standby drains them.
- **Store-and-forward queue** — on the Active cluster; used as a local
  buffer when the Standby is temporarily unreachable.

The single most important design goal is **zero RPO** (Recovery Point
Objective): any write the client was told succeeded must survive a failover.

### 1.1 Roles (states)

| Role | Accepts writes | Accepts reads | Replication behavior |
|---|---|---|---|
| **Active** | yes | yes | Primary; ships sync logs to the standby |
| **Standby** | no | lookback reads only | Consumes sync logs from the active |
| **ActiveToStandby** | no (rejects with an HA error) | yes | Draining pending logs before yielding |
| **StandbyToActive** | no | no | Replaying all received logs before promotion |
| **DegradedStandby** | no | no | Peer active is store-and-forwarding; catching up |
| **AbortToActive** | briefly no | yes | Reverting a failover that was started but abandoned |
| **Offline** | no | no | Cluster removed from the HA group |

### 1.2 Replication modes (internal to the Active cluster)

When the Standby (or the path to it) is unhealthy, the Active cluster's
replication writer moves through modes without ever losing an
acknowledged write:

- **Sync** — normal operation; each committed batch is synchronously shipped
  to the standby.
- **Store-and-forward** — the standby path is down; changes are buffered in
  the local store-and-forward queue and the client still gets success.
- **Sync-and-forward** — the path is back; the writer simultaneously drains
  the backlog and resumes live sync, then settles back to Sync.

### 1.3 Two invariants worth calling out

- **No ghost writes.** A change is written to the replication log only
  **after** the local commit has been durably persisted. If the local commit
  fails, replication never fires — so the standby can never hold a record the
  primary doesn't have.
- **Crash-safe re-ship.** If the local commit succeeds but shipping to the
  standby fails, the change is recovered and re-shipped during the Active
  cluster's normal crash-recovery (log replay) — no acknowledged write is
  lost even across a RegionServer crash.

---

## 2. Prerequisites for the staging environment

Before running any scenario, confirm:

1. **Two clusters, healthy.** A Primary (referred to below as **A**) and a
   Secondary (**B**), each fully up, in the same HA group.
2. **Phoenix HA build deployed** on both clusters, with the HA coprocessors
   registered on the RegionServers and Master.
3. **Shared-storage replication root exists** on each cluster, owned by the
   service account. The per-HA-group subdirectories (the inbound and
   store-and-forward queues) are created automatically — you only need the
   root.
4. **HA group is configured** so each cluster knows its peer and the peer's
   storage endpoint, and each cluster resolves its own role (A = Active,
   B = Standby to start).
5. **A replicated test table exists** on both clusters (schemas are not
   auto-created on the standby — pre-create the mirror).
6. **Fault-injection tooling available** for the network and cluster-down
   scenarios. On our EKS staging clusters this is **AWS Fault Injection
   Service (FIS)** — see §6 for the full setup and per-scenario action
   mapping. In short:
   - **cluster-ops faults need nothing extra** — `kubectl` alone covers the
     process-kill and scale-to-zero scenarios;
   - **network and disk faults use AWS FIS**, which does not ship enabled by
     default — it requires a one-time IAM + service-account setup (§6.1).

**Baseline check.** Before each scenario, insert one row into the test table
on A using the sqlline client (pointed at A's quorum), then — after ~one
rotation interval — query for it on B using the sqlline client (pointed at
B's quorum). If the row does not appear on B, nothing below will work.

---

## 3. Key tunables

You do not need source-level detail to run these, but three knobs change
timing behavior and are worth setting deliberately on staging:

| Tunable | Default | Why you'd change it |
|---|---|---|
| Log rotation interval | ~60 s | How often a sync log round closes and becomes visible to the standby. Lower it to make replication observable faster. |
| Log rotation size | 128 MB | A round also closes at this size. Rarely relevant for functional tests. |
| **Sync timeout** | ~5 min (WAL sync timeout) + ZK session timeout, computed | The wait before a hung remote sync forces a RegionServer abort (see S17). **Lower this** (e.g. 30 s) to make S17 finish in a reasonable time. |
| Failover timeout | ~10 s | Upper bound on how long any single failover step should take. Used as a pass/fail threshold. |

---

## 4. Scenario matrix

| ID | Scenario | Category | Status |
|---|---|---|---|
| S1 | Graceful failover with fully-synced standby | Happy path | Ready |
| S10 | Planned failover under sustained write load | Happy path | Ready |
| S2 | Standby recovery after transient network partition | Network fault | Ready |
| S7 | Standby cluster fully offline during active writes | Network fault | Ready |
| S17 | Remote sync hangs past the timeout → RegionServer abort | Network fault | Ready |
| S6 | Active RegionServer crash during sync replication | Cluster down | Ready |
| S11 | Local commit fails → replication must NOT fire | Invariant | Ready |
| S12 | Remote sync fails after local commit → re-ship on replay | Invariant | Ready |
| S3 | Failover while a store-and-forward backlog exists | Cluster down | Not yet |
| S5 | Abort a failover mid-transition | Race | Not yet |
| S8 | Peer state-transition race under flapping network | Race | Ready |
| S9 | Region split/merge during replication | Race | Ready |
| S4 | Forced failover from a degraded standby | Operator-forced | Design |

---

## 5. Scenarios

Each scenario follows the same shape: **What it proves → Setup → Inject the
fault → Expected result (pass/fail)**.

### 5.1 Happy-path failover

#### S1 — Graceful failover with a fully-synced standby · *Ready*

**What it proves.** A clean, operator-driven failover promotes the standby
to active and demotes the old active, atomically and with zero data loss.

**Setup.**
1. Fresh clusters, replication working, queues empty.
2. Insert a known row `r-pre-failover` into the test table on A using the
   sqlline client, then wait one log round (~65 s) so it is guaranteed
   synced to B.

**Failover.** Issue the planned-failover operator action on the HA group
(via the HA admin tooling). A moves to *ActiveToStandby*, drains, and then A
and B swap atomically: A becomes *Standby*, B becomes *Active*. Once each
cluster's role has settled (A shows Standby, B shows Active), use the
sqlline client against B to insert a new row `r-post-failover` and to query
back the pre-failover row `r-pre-failover`.

**Expected result (pass).**
- Total failover wall-clock is under ~2 minutes and no single step exceeds
  the failover timeout (~10 s).
- After the swap, **A rejects new writes** with an HA "not active" error.
- **B accepts writes**, and reading `r-pre-failover` on B returns the value
  written on A (**zero RPO**).
- The standby inbound queue is empty at the moment of promotion — all logs
  were replayed before the swap.

**Fail signals.** `r-pre-failover` missing or stale on B; both clusters
accept writes simultaneously (split-brain); the swap takes far longer than
the failover timeout without an OUT backlog to explain it.

---

#### S10 — Planned failover under sustained write load · *Ready*

**What it proves.** The same clean failover as S1 holds up when writes are
in flight during the transition — no acknowledged write is lost, and writes
that raced the switch fail cleanly rather than silently vanishing.

**Setup.** S1 setup, plus a steady background write loop against A (e.g.
sequentially numbered rows).

**Failover.** After ~30 s of load, trigger the planned failover exactly as
in S1. Keep the load running through the transition.

**Expected result (pass).**
- Failover still completes under ~2 minutes despite the load.
- Post-failover, B holds **every row the client was told succeeded**. The
  highest row number present on B equals the last write that returned
  success before the client started seeing HA errors.
- Writes that raced the transition received the HA "not active" error — that
  is expected and correct.

**Fail signals.** A row that returned success to the client is missing on B;
a write returned success on A after A had already yielded.

---

### 5.2 Network-fault scenarios

#### S2 — Standby recovery after a transient network partition · *Ready*

**What it proves.** When the path to the standby's storage blips, the active
transparently buffers writes (store-and-forward), then drains and returns to
normal sync when the path recovers — with no client-visible errors and no
data loss or reordering.

**Setup.** Replication working. FIS configured (§6.1).

**Inject the fault.**
1. Run a FIS experiment that **blackholes the active RegionServers' egress to
   the standby storage port** — `aws:eks:pod-network-blackhole-port` on the
   `<ns-a>` RegionServer pods, `trafficType=egress`, `protocol=tcp`,
   `port=<standby-storage-port>` (see §6.2). Set `duration` to ~2 min.
   ```bash
   aws fis start-experiment --experiment-template-id <blackhole-standby-port-tmpl>
   ```
2. Write continuously on A for the duration of the experiment.
3. Confirm the active has switched to **store-and-forward** — its local
   store-and-forward queue is accumulating log files.
4. Let the experiment expire (or stop it) to restore connectivity.

**Expected result (pass).**
- Every client write during the outage **returned success** (no timeouts
  surfaced to the client).
- Within ~30 s of recovery the store-and-forward queue starts draining; within
  ~60 s it is empty and the writer is back in normal sync.
- The standby's role goes *Standby → DegradedStandby → Standby* over the
  episode.
- **Data-consistency check:** after the drain, every row written during the
  outage is present on B with its **original timestamp** — no reordering, no
  duplicates, none missing. Sample the first, middle, and last rows
  explicitly.

**Fail signals.** Client errors during the outage; queue never drains;
a row on B carries a different value/timestamp than the last write on A.

---

#### S7 — Standby cluster fully offline during active writes · *Ready*

**What it proves.** A hard, sustained standby outage (not just a network
blip) keeps the active fully available; buffered writes drain intact once
the standby returns.

**Setup.** Replication working; confirm one write replicates.

**Inject the fault.**
1. Take the standby's storage tier fully down (scale its storage nodes to
   zero).
   ```bash
   kubectl -n <ns-b> scale sts/datanode --replicas=0
   kubectl -n <ns-b> scale sts/namenode --replicas=0
   ```
2. Using the sqlline client against A, insert ~100 rows into the test table
   (e.g. `r-offline-1` … `r-offline-100`). They should all succeed locally
   while the store-and-forward queue grows.
3. Bring the standby storage back up.
   ```bash
   kubectl -n <ns-b> scale sts/namenode --replicas=1
   kubectl -n <ns-b> scale sts/datanode --replicas=1
   kubectl -n <ns-b> rollout status sts/namenode sts/datanode --timeout=120s
   ```

**Expected result (pass).**
- No client write returned an error during the outage.
- The store-and-forward queue drains within ~2 minutes of the standby's return.
- After the drain, B holds all ~100 rows.
- Role moved *Standby → DegradedStandby → Standby*.

**Fail signals.** Client errors on the active during the outage; missing
rows on B after the drain.

---

#### S17 — Remote sync hangs past the timeout → RegionServer abort · *Ready*

**What it proves.** If a remote sync hangs indefinitely (storage reachable
at the TCP level but never responding), the active does **not** hang
forever. After the configured sync timeout it deliberately aborts the
affected RegionServer, which recovers on another node — and the in-flight
write is recovered via log replay (zero RPO preserved).

**Setup.** FIS configured (§6.1). **Lower the sync timeout** (e.g. to 30 s)
so the test finishes quickly. Confirm one row replicates with no fault
applied.

**Inject the fault.** The abort requires a **mid-stream stall** — the peer-DN
connection must stay *established* with a block open, and its ACKs withheld, so
the active's in-flight `waitForAckedSeqno()` hangs and blocks the application
write thread's `syncFuture`. A connection **drop** does NOT work: it fails at
pipeline *setup* (`createBlockOutputStream`), which is handled on a background
mode-init thread and gracefully demotes the group to STORE_AND_FORWARD — it
never blocks an app thread, so the sync-timeout never fires. (Verified on the
test-bed 2026-08-08; see `S17_Sync_Timeout_RS_Abort_Needs_MidStream_Stall.md`.)
- **Block layer (required for the abort):** run `aws:ebs:pause-volume-io` on the
  EBS volume backing the **standby's** storage (discover it per §6.1) — the
  standby accepts the connection but its I/O never completes, so the active's
  remote sync hangs mid-stream. `aws:ebs:volume-io-latency` with a large delay
  is the equivalent latency variant.
- **Do NOT use `aws:eks:pod-network-blackhole-port` for this scenario.** A
  blackhole is a silent drop → setup-phase failure → graceful SAF, not an abort.
  (It is the correct fault for **S17b**, the false-success RPO variant, where
  the block-data `output.sync()` *throws*.)

Give the experiment a `duration` comfortably **longer than the sync
timeout** — the hang must not resolve on its own before the timeout fires.
Then write a single row on A; the remote sync will block.

**Expected result (pass).**
- Once the sync timeout elapses, the affected RegionServer on A **aborts and
  restarts** (it does not hang indefinitely).
  ```bash
  kubectl -n <ns-a> get pod <rs> -w        # watch it go Terminating → Ready
  kubectl -n <ns-a> logs <rs> --previous | grep -Ei 'abort|sync.*timed out'
  ```
- The client that issued the write receives a specific **sync-timeout**
  error, not a generic failure.
- After the RegionServer returns and its regions reopen: rows written
  **before** the hang are on B; the **in-flight row that triggered the
  abort is re-shipped to B via log replay** within ~60 s of recovery.
- **No silent data loss.**

**Fail signals.** RegionServer hangs past the timeout without aborting; the
in-flight row never reaches B; a generic error is returned instead of the
sync-timeout error.

---

### 5.3 Cluster-down scenarios

#### S6 — Active RegionServer crash during sync replication · *Ready*

**What it proves.** A hard crash of an active RegionServer mid-batch loses no
replicated data: any change that was locally committed but not yet shipped is
re-shipped during crash recovery.

**Setup.** Replication working; a high-rate background write loop on A.

**Inject the fault.**
1. Let ~10 s of writes build up in-flight batches.
2. *(Recommended for determinism)* Start a brief FIS
   `aws:eks:pod-network-blackhole-port` experiment (§6.2) on the RegionServer
   so at least one batch is stuck mid-sync at the moment of the crash — this
   forces the recovery path to be the only route for that batch. Use a short
   `duration` (a few seconds).
3. **Hard-kill** the active RegionServer (no graceful shutdown).
   ```bash
   kubectl -n <ns-a> delete pod <rs> --grace-period=0 --force
   ```
4. Wait for it to come back and stop the load.
   ```bash
   kubectl -n <ns-a> rollout status sts/regionserver --timeout=120s
   ```

**Expected result (pass).**
- After recovery, the row count on B equals the row count on A — **no
  mutations lost** on the standby due to the crash.
- The active never left the *Active* role (no HA error was raised to
  clients).

**Fail signals.** B is short rows that A has; the cluster changed roles due
to the crash.

---

#### S11 — Local commit fails → replication must NOT fire · *Ready*

**What it proves.** The no-ghost-writes invariant: if the **local** commit
fails (local storage error), replication is never initiated, so the standby
cannot end up with a record the primary lacks.

**Setup.** Replication working. This scenario needs the active's write to its
**own** (local) storage to *fail*, not merely slow down — so we disrupt the
active's access to its local storage rather than the standby path.

**Inject the fault.** Three options:
- **kubectl (simplest):** scale the active's local storage below the minimum
  replication factor for a short window, so local commits error out:
  ```bash
  kubectl -n <ns-a> scale sts/datanode --replicas=0   # or below dfs.replication.min
  ```
- **FIS (network):** run `aws:eks:pod-network-blackhole-port` on the `<ns-a>`
  RegionServer pods targeting the **local** storage port (egress, tcp),
  which makes the local WAL sync fail.
- **FIS (block layer):** run `aws:ebs:pause-volume-io` on the EBS volume
  backing the active's local storage (discover it per §6.1). Note this
  **hangs** local I/O rather than returning an error, so the client write
  *blocks* rather than failing fast — it ultimately does not get an ack, and
  if the pause outlasts the sync timeout this crosses into S17 (RS abort).
  Either way the invariant below still holds.

Then:
1. Attempt a write `r-ghost-guard` on A during the fault window.
2. Restore local storage (`kubectl -n <ns-a> scale sts/datanode --replicas=<n>`,
   or let the FIS experiment expire).

**Expected result (pass).**
- The write **did not return success** — it errored (network/kubectl option)
  or blocked without an ack (EBS pause option). Either way the client was
  never told the write succeeded.
- **No** record for `r-ghost-guard` exists on B — reading it on B returns
  empty.
- After the fault clears, a fresh write on A replicates normally — the
  cluster did **not** abort (a routine local-commit error is not fatal; only
  the sync-timeout in S17 aborts).

**Fail signals.** `r-ghost-guard` appears on B; the cluster aborts on a
routine local error.

---

#### S12 — Remote sync fails after local commit → re-ship on replay · *Ready*

**What it proves.** The crash-safe re-ship invariant: if the local commit
succeeds but the remote sync fails, and the RegionServer then bounces before
the backlog drains, the change is still delivered to the standby via log
replay.

**Setup.** Replication working; FIS configured (§6.1).

**Inject the fault.**
1. Start a short-window FIS `aws:eks:pod-network-blackhole-port` experiment on
   the `<ns-a>` RegionServer pods (egress, tcp, standby storage port).
2. Write `r-replay-resend` on A. The local commit succeeds; the remote sync
   fails after retries.
3. **Immediately hard-kill** the active RegionServer (before the backlog can
   drain on its own).
   ```bash
   kubectl -n <ns-a> delete pod <rs> --grace-period=0 --force
   ```
4. Remove the timeout as the RegionServer starts to recover.
5. Wait for recovery and log replay to complete (~60–90 s).

**Expected result (pass).**
- Reading `r-replay-resend` on B returns the written value within ~90 s of
  recovery — the change was re-shipped during log replay.
- (Either delivery route — backlog drain or replay — satisfies zero RPO. The
  **only** failure is *neither* route delivering the record.)

**Fail signals.** `r-replay-resend` never reaches B.

---

#### S3 — Failover while a store-and-forward backlog exists · *Not yet*

**What it proves (target behavior).** A planned failover started while the
active still holds an un-forwarded backlog must **drain that backlog before
promoting** — so the promoted cluster is fully consistent, at the cost of a
longer failover.

**Status.** The store-and-forward buffering itself works, but the
"drain-before-promote" interlock is still landing. This scenario documents
the intended behavior; it will not pass until that interlock ships.

**Setup.** Replication working; FIS configured (§6.1).

**Inject the fault.**
1. Start a FIS `aws:eks:pod-network-blackhole-port` experiment on the
   `<ns-a>` RegionServers (egress, tcp, standby storage port) and write
   ~1000 rows on A to build a backlog.
2. Trigger a planned failover.
3. Stop the experiment so the backlog can drain.

**Expected result (target).**
- Failover takes noticeably longer than S1 — roughly (backlog ÷ drain
  throughput) plus the S1 baseline — because it waits for the drain.
- After promotion, **every** backlogged row is present on the new active.

**Fail signals.** Promotion completes while the backlog is still
un-forwarded (rows missing on the promoted cluster).

---

### 5.4 Transitional / race scenarios

#### S5 — Abort a failover mid-transition · *Not yet*

**What it proves (target behavior).** An operator who starts a failover and
then aborts it (while the standby is still replaying) cleanly reverts: the
original active resumes and the standby never reaches *Active*.

**Status.** The abort states exist in the model, but the drain/abort
interlock is being finalized — treat as target behavior.

**Setup.** Sustained background load on A.

**Inject the fault.**
1. Start a failover.
2. After ~5 s (A in *ActiveToStandby*, B in *StandbyToActive*), issue the
   abort.
3. Watch both clusters return to *Active* (A) / *Standby* (B).

**Expected result (target).**
- Background writes resume on A within ~10 s of the abort.
- Any write that saw an HA error during the abort window was retried by the
  client and eventually succeeded.
- **B never reached *Active*** — no split-brain window.

**Fail signals.** B briefly became active; writes lost during the abort
window.

---

#### S8 — Peer state-transition race under a flapping network · *Ready*

**What it proves.** Under an intermittently lossy link that makes the active
flap between sync and store-and-forward, the standby degrades and recovers
gracefully — and the two clusters never both believe they are active.

**Setup.** Sustained background load on A; FIS configured (§6.1).

**Inject the fault.** Run a FIS `aws:eks:pod-network-packet-loss` experiment
on the `<ns-a>` RegionServer pods for ~3 minutes, `lossPercent=40`, with
`sources` scoped to the standby storage endpoint (or its resolved
CIDR/domain). This makes the active flap between sync and store-and-forward.

**Expected result (pass).**
- The standby's role **oscillates** between *Standby* and *DegradedStandby*
  during the window (capture it with a ~1 s poll).
- The active **stays *Active*** the entire time — no flap to any other role.
- **No HA error** is raised to clients (writes keep succeeding via sync or
  store-and-forward).
- Within ~60 s after the chaos clears, both clusters settle at
  *Active* / *Standby*.

**Fail signals.** The active changes role; both clusters active at once;
client errors during the flapping.

---

#### S9 — Region split/merge during replication · *Ready*

**What it proves.** When a table region splits mid-write, a single row's
change records can land in multiple log files within one round. The standby
must still apply them in the correct order and idempotently, so the final
value is always the last-written one.

**Setup.** A pre-split replicated table on both clusters; sustained write
load spanning the split boundary.

**Inject the fault.**
1. While load runs, trigger a region split on A.
   ```bash
   kubectl -n <ns-a> exec -i <hmaster> -- /hbase/bin/hbase shell -n <<'EOF'
   split '<table>', 't'
   EOF
   ```
2. Continue load for another minute after the split completes.
3. Stop load and wait ~2 minutes for the next round to drain.

**Expected result (pass).**
- Row count on B equals A.
- For every row written **during** the split, B holds the **last-written
  value** from A — check a sample of ~10 keys explicitly. No stale value
  winning over a newer one.

**Fail signals.** Any sampled row on B holds an older value than A's last
write (ordering violation).

---

### 5.5 Operator-forced failover

#### S4 — Forced failover from a degraded standby · *Design*

**What it proves (target behavior).** When the active is lost
**unrecoverably** while it still held an un-forwarded backlog, an operator
can force-promote the degraded standby immediately — explicitly accepting the
loss of the un-forwarded rows in exchange for restoring service.

**Status.** The forced-promotion edge exists in the model, but the operator
CLI/flag that drives it is still in design. This scenario defines the
acceptance criteria for when it lands.

**Setup.** Reproduce S7 until the active holds a substantial backlog. Do
**not** restore the standby storage yet.

**Inject the fault.**
1. Simulate catastrophic, unrecoverable loss of the active (e.g. delete its
   environment entirely). **Capture the contents of the active's inbound and
   store-and-forward queues immediately before doing so** — this is the
   ground truth for the data-loss reconciliation below.
2. Restore the standby storage.
3. Issue the **forced** failover.

**Expected result (target).**
- The standby reaches *Active* within the forced-failover SLA (seconds — not
  bounded by any drain).
- A **`FORCED FAILOVER` audit entry** is recorded with operator identity and
  timestamp.
- **Data-loss reconciliation — the critical check:** rows that were only in
  the lost active's store-and-forward queue **may be missing** (the accepted
  trade-off). But **every row that had already been synced to the standby
  must be present.** The missing set must be a **strict subset** of the
  active's store-and-forward queue at the moment of loss — never a superset. A
  previously-synced row going missing is a **correctness bug**, not an
  accepted trade-off.
- New writes on the promoted cluster succeed.

**Fail signals.** A previously-synced row is missing; no audit entry; forced
promotion blocks on a drain it was supposed to skip.

---

## 6. Fault injection with AWS FIS

Our EKS staging clusters use **AWS Fault Injection Service (FIS)** for
network and disk faults. FIS is a managed, IAM-gated service — there is no
in-cluster proxy or privileged DaemonSet to install and babysit. It injects
pod-level faults by launching a short-lived, privileged **fault-orchestration
pod** on the target node that applies `tc`/`iptables`-style rules for the
experiment's `duration`, then removes them automatically. This
self-reverting, time-boxed model is why FIS is preferable to a self-hosted
injector on a shared staging cluster.

### 6.1 One-time setup (does NOT ship enabled by default)

FIS requires a small amount of setup before any network scenario will run:

1. **FIS experiment role (IAM).** An IAM role FIS assumes to run experiments,
   with the FIS-managed policy plus `eks:DescribeCluster` and the EC2/EKS
   permissions listed for each action in the AWS FIS Actions reference. For
   the **EBS** actions (§6.2) also grant `ec2:DescribeVolumes`,
   `tag:GetResources`, and `ec2:PauseVolumeIO` (for `pause-volume-io`) or
   `ec2:InjectVolumeIOLatency` (for `volume-io-latency`).
2. **Kubernetes service account + RBAC.** The EKS pod actions run *inside*
   the cluster, so they authenticate via a Kubernetes **service account**
   (the `kubernetesServiceAccount` parameter), not IAM. Create it and bind
   the RBAC role AWS documents for the pod actions. (The EBS actions target
   AWS volumes directly and need no in-cluster service account.)
3. **Privileged pod security.** The network and blackhole actions are **only
   compatible with the `privileged` Pod Security Standard** — confirm the
   target namespace admits privileged fault-orchestration pods.
4. **Target discovery.** The EKS pod actions target pods by cluster +
   namespace + label selector — have the RegionServer pod labels for `<ns-a>`
   handy. The **EBS** actions instead target the EBS *volume* backing a pod's
   PersistentVolumeClaim, so you must map PVC → EBS volume first. With the EBS
   CSI driver each volume carries a `kubernetes.io/created-for/pvc/name` tag;
   find the volume for the active RegionServer's storage PVC and target it by
   that tag:
   ```bash
   # PVC → PV name
   kubectl -n <ns-a> get pvc -o custom-columns=PVC:.metadata.name,VOL:.spec.volumeName
   # PV → EBS volume id (or filter by the created-for-pvc tag directly)
   aws ec2 describe-volumes \
     --filters Name=tag:kubernetes.io/created-for/pvc/name,Values=<pvc-name> \
     --query 'Volumes[].VolumeId'
   ```
   Target EBS volumes must be **Nitro-based** and (for `pause-volume-io`) in
   the **same Availability Zone**; they can't be on Outposts.

> **Approval.** Because FIS runs privileged pods and needs an IAM role, the
> initial setup usually needs a sign-off from whoever owns the staging AWS
> account / EKS cluster. Do this once, up front.

### 6.2 Action → scenario mapping

All identifiers below are current FIS action names. The **EKS pod** actions
target pods directly (no rerouting of Phoenix's peer-storage URL needed) — you
scope them to the `<ns-a>` RegionServer pods and, for the network actions,
narrow the blast radius with `sources`/`port` so only the standby-storage (or
local-storage) traffic is affected. The **EBS** actions instead target the EBS
volume backing a pod's PVC (see the discovery step in §6.1) and act at the
block layer, below the file system.

| Fault | FIS action | Key parameters | Scenarios |
|---|---|---|---|
| Drop traffic to standby storage (full "timeout") | `aws:eks:pod-network-blackhole-port` | `trafficType=egress`, `protocol=tcp`, `port=<standby storage port>`, `duration` | S2, S3, S6, S12, **S17b** (not S17 — a drop → graceful SAF, not the abort) |
| Drop traffic to **local** storage (force local commit failure) | `aws:eks:pod-network-blackhole-port` | `trafficType=egress`, `protocol=tcp`, `port=<local storage port>` | S11 (FIS option) |
| Intermittent packet loss toward standby | `aws:eks:pod-network-packet-loss` | `lossPercent=40`, `sources=<standby endpoint/CIDR>`, `duration` | S8 |
| Added latency/jitter toward standby | `aws:eks:pod-network-latency` | `delayMilliseconds`, `sources=<standby endpoint>`, `duration` | S2 (latency variant) |
| Subnet-level partition (coarser) | `aws:network:disrupt-connectivity` | `aws:ec2:subnet` target, `scope=prefix-list` (standby storage) | S3/S8 alt |
| Hard pod kill | `aws:eks:pod-delete` | `gracePeriodSeconds=0` | S6, S12 (alternative to `kubectl delete pod`) |
| **Pause I/O on the active's local storage volume** | `aws:ebs:pause-volume-io` | `aws:ec2:ebs-volume` target, `duration` | S11 (block-layer option), S17 (block-layer variant) |
| **Slow the active's local storage volume** | `aws:ebs:volume-io-latency` | `aws:ec2:ebs-volume` target, `writeIOLatencyMilliseconds`, `writeIOPercentage`, `duration` | S2/S17 (latency variant) |

**Notes on scoping.**
- `sources` accepts an IPv4 address, CIDR, domain name, or AZ. For a domain,
  FIS resolves it ~10 times — DNS rotation means it may not cover *every* IP
  the name resolves to. For a hard, deterministic cut prefer
  **blackhole-port** on the specific storage port over packet-loss-by-domain.
- `duration` is ISO 8601 (`PT2M` = 2 minutes). For S17 make it comfortably
  longer than the (lowered) sync timeout; for S6 make it a few seconds.
- **EBS actions pause or slow I/O — they do NOT return hard I/O errors.**
  `pause-volume-io` *hangs* the volume rather than failing writes, so pausing
  the active's WAL/local-storage volume makes the local commit **block** (no
  ack to the client, so replication never fires — the S11 invariant still
  holds). Because the write **hangs** rather than erroring, pausing the
  **standby's** storage volume is the way to drive **S17** (hung storage → the
  in-flight remote sync hangs mid-stream → the app-thread `syncFuture` blocks →
  RS self-aborts after the sync timeout). This *hang* semantics is essential: a
  connection **drop**/blackhole fails at pipeline setup on a background thread →
  graceful SAF, never the abort. Don't expect a clean error return.
- Cluster-ops faults (process kill, scale-to-zero) need **no FIS** — plain
  `kubectl` as shown in S6/S7/S12 is simpler and just as effective.

### 6.3 Running an experiment

FIS experiments run from a reusable **experiment template**. Create one per
fault (console or `aws fis create-experiment-template`), then:

```bash
# start
aws fis start-experiment --experiment-template-id <tmpl-id>

# check status
aws fis get-experiment --id <experiment-id>

# stop early (also triggers FIS's automatic cleanup / rule removal)
aws fis stop-experiment --id <experiment-id>
```

An experiment auto-stops and cleans up when its `duration` elapses; you don't
need a manual "remove the fault" step the way an in-path proxy would require.

---

## 7. Quick pass/fail summary

The whole suite is really testing three promises. Every scenario maps back to
one of them:

1. **Zero RPO** — no acknowledged write is ever lost across any failure
   (S1, S2, S6, S7, S10, S12, S17; and the bounded, operator-accepted
   exception in S4).
2. **No split-brain** — the two clusters are never both active
   (S1, S5, S8, S10).
3. **No ghost writes / correct ordering** — the standby never holds a record
   the primary lacks, and always converges to the primary's last-written
   values (S9, S11).

If a run violates any of these three, it is a correctness failure regardless
of which scenario surfaced it.
