# Fast-fail SYNC → STORE_AND_FORWARD detection + scoped HDFS config for the standby FS

> **Reconstruction note (2026-07-27).** The original `phoenix-ha-fast-fail-sync-saf.md`
> lived only in the pruned `~/.claude/plans/` folder and was recycled; no copy survived
> in git, stashes, backups, or session transcripts. This file is rebuilt from three
> durable sources: the discussion notes in
> `docs/fast-fail-sync-saf-shutdown-budget-notes.md`, the `pending-plans` /
> `saf-detection-latency` memory entries, and verified code/config facts re-derived from
> the current tree and the sibling `hadoop/` repo. Treat parameter values as the last
> agreed proposal, not as freshly re-validated numbers — **open questions are flagged
> inline. Re-confirm before implementing.**

## Problem

When the standby (peer) HDFS becomes unreachable, a SYNC-mode replication write can block
for **multiple minutes** before the group detects the failure and falls back to
STORE_AND_FORWARD (SAF). Two independent causes:

1. **Blind idle window (detection is write-triggered).** Detection of a dead peer only
   happens when a mutation flows: `ReplicationLog.apply()` evaluates `isClosed()` and the
   per-attempt failure path only on an actual write (`ReplicationLog.java:329-343`). If the
   active is idle, the group keeps advertising healthy SYNC until the next write. (This idle
   sub-case is tracked separately and was ruled **WON'T FIX** on its own — see
   `project_saf_detection_latency` memory. The plan here targets the *active-write* stall.)

2. **Stacked HDFS retry loops on the standby FS client.** A single stuck NameNode operation
   (e.g. the peer `create`/`getAdditionalDatanode`) stacks two default retry loops:
   - **Per-NN socket connect:** `ipc.client.connect.timeout` (20 s) retried up to
     `ipc.client.connect.max.retries.on.timeouts` (45) *within one RPC to one NN*.
   - **HA failover across NNs:** up to `dfs.client.failover.max.attempts` (15) rounds,
     sleeping `min(500·2^n, 15000)` ms each.

   The failover backoff sum alone across 15 attempts is ≈ **190 s**, plus a 20 s connect on
   each dead-host round — the observed ~2.5 min stalls in production (calls #211167,
   #211260). Because the RPC handler thread waits on `syncFuture.get(syncTimeoutMs)` and
   `syncTimeoutMs = WAL_SYNC_TIMEOUT (5 min) + zkTimeout` (`calculateSyncTimeout`,
   `ReplicationLogGroup.java:702-708`), the handler can be parked for minutes.

**Goal:** collapse a single stuck peer-NN operation from ~150 s to **~10 s**, matching the
existing `REPLICATION_LOG_PEER_INIT_TIMEOUT_MS` = 10 s bound
(`ReplicationLogGroup.java:185`) so all peer-facing paths fail fast consistently — while
**preserving legitimate pipeline recovery** (a slow-but-alive DN must not be cancelled).

## Verified config defaults (from sibling `hadoop/`, not overridden in our config layers)

| Config key | Default | Source |
|---|---|---|
| `ipc.client.connect.timeout` | 20000 ms | `CommonConfigurationKeysPublic.java:396` |
| `ipc.client.connect.max.retries.on.timeouts` | 45 | `CommonConfigurationKeysPublic.java:423` |
| `ipc.client.connect.max.retries` (non-timeout) | 10 | `:405` |
| `ipc.client.connect.retry.interval` | 1000 ms | `:414` |
| `dfs.client.failover.max.attempts` | 15 | `HdfsClientConfigKeys.java:521` |
| `dfs.client.failover.sleep.base.millis` | 500 ms | `:523` |
| `dfs.client.failover.sleep.max.millis` | 15000 ms | `:525` |
| `dfs.client.retry.max.attempts` | 10 | `:517` |

Verified as not overridden anywhere in the `hbase-configs` stack (`firstparty_hdfs-conf`,
`firstparty_hbase-conf`, `core-conf`, `falcon-conf`, cluster files) — the only replication
key set there is `rotation.size.bytes`. So stock defaults apply in production.

## Design

### Part A — Scoped `Configuration` for the standby (peer) FileSystem

The peer FS client must fail fast **without touching the local RS FS client** — the local
FS write path must keep its generous, correct timeouts. This is **bulkheading**: isolate the
standby failure domain from the local FS client. A scoped clone is already how the peer
manager is built (`ReplicationLogGroup.java:541`, `new Configuration()`), so this rides that
seam rather than mutating the parent conf.

Proposed overrides on the peer-scoped conf (target ≈10 s per stuck op):

| Key | Default | Proposed | Effect |
|---|---|---|---|
| `dfs.client.failover.max.attempts` | 15 | **2** | 3 NNs, one try each; stop instead of 15 rounds |
| `dfs.client.failover.sleep.base.millis` | 500 | 500 | keep |
| `dfs.client.failover.sleep.max.millis` | 15000 | **2000** | cap backoff at 2 s, not 15 s |
| `ipc.client.connect.timeout` | 20000 | **5000** | 5 s per socket connect |
| `ipc.client.connect.max.retries.on.timeouts` | 45 | **1** | one connect retry, not 45 |
| `dfs.client.retry.policy.spec` | (default) | **short spec** | bound the DFS-level retry loop; also ensures leaked-thread cleanup |

> The `dfs.client.retry.policy.spec` addition (2026-05-19 update) is load-bearing for
> **leaked-thread cleanup** on `future.cancel(true)` — HDFS does not always honor interrupt,
> and an unbounded retry policy keeps a cancelled worker alive. **Open question:** exact
> spec string was in the original plan and is not recovered — needs to be re-derived
> (`pause,retries:pause,retries` form).

### Part B — Per-attempt timeout on the SYNC/SAF write, preserving pipeline recovery

Wrap each peer write attempt in a bounded future so a single stuck attempt fails after a
**per-attempt** budget instead of the full `syncTimeoutMs`. Distinct budgets per mode:

- **SYNC per-attempt = 7 s** = ~5 s socket detection + ~2 s pipeline recovery. This is
  derived from observed latency, **not picked** — the earlier 500 ms proposal would cancel
  *successful* pipeline recoveries (the exact failure mode to avoid).
- **SAF per-attempt = 15 s.** SAF is graceful degradation, not failure, so the degraded
  path rides out larger transients before aborting.

The retry count stays as-is: `apply()` uses `maxAttempts = SYNC_RETRIES(1) + 1 = 2`
(`ReplicationLog.java:106-107, 329`). On SYNC exhaustion, `SyncModeImpl.onFailure`
(`SyncModeImpl.java:56-71`) transitions to SAF (or, during the in-sync cutover gate where SAF
is illegal, fail-stops and aborts the RS — that branch is unchanged here).

**No hysteresis — intentional.** The disruptor consumer is single-threaded and serializes
calls, so 2 consecutive per-attempt timeouts already imply sustained failure. Document this
so reviewers don't ask for a hysteresis counter.

### Part C — Executor for the bounded attempts (cross-cut with shutdown-budget)

Bounded attempts run on a dedicated executor so a hung HDFS call can be abandoned via
`future.cancel(true)` without parking the disruptor consumer.

- **Daemon threads required.** `future.cancel(true)` can leak a worker for ~9–11 s (HDFS
  ignores interrupt); with a small operator shutdown budget (e.g. 5 s) `close()` returns
  while the worker is still alive. Daemon threads ensure the JVM can still exit.
- **Drain from `ReplicationLogGroup.close()` phase 2 directly**, not via `onExit`/`onShutdown`
  — decouples it from the deferred `onShutdown`-not-firing bug and removes orphan risk. This
  is the 5th executor the shutdown-budget plan must drain (it currently lists 4); add a
  `drainExecutor(...)` call for it.

### Ruled out (from the design discussion)

- **`Deadline` abstraction for per-attempt retries — NO.** `ShutdownDeadline` fits shutdown
  (heterogeneous phases, fast phases donate to slow ones). Per-attempt retries are the
  opposite shape: each attempt has a hard ~6 s minimum, so shrinking attempt 2's budget by
  attempt 1's spend would cancel pipeline recoveries. The multiplicative
  `per-attempt × max-attempts` model is the *correct* model, not a simplification.
- **SAF pre-warming (cold-fallback) — NO.** `ReplicationLogGroup.init()` eagerly calls
  `createLocalShardManager()` (opens local FS, `mkdirs` fallback dir) at boot, so SAF entry
  has no cold-start work.

## Interaction constraints (do not break)

- **Reader round-buffer (9 s).** Config-time invariant; the per-attempt budgets must stay
  coherent with it (2026-05-19 note added this coordination constraint). Re-check that
  SYNC 7 s + SAF 15 s attempts fit under the reader's assumptions.
- **`PhoenixWALSyncTimeoutException` backstop (`ReplicationLogGroup.java:879-892`).** Real
  wall-clock deadline on `syncFuture.get(syncTimeoutMs)`. With per-attempt budgets in place,
  `syncTimeoutMs` should still bound the *total* (SYNC attempts + flip + SAF attempts +
  abort). **Open question #4:** whether to shorten it — the only deadline-tuning decision
  worth debating.

## Files (expected touch points — re-verify)

- `ReplicationLogGroup.java` — peer-scoped conf overrides (~`:541`); new bounded-attempt
  executor + drain in `close()` phase 2; possibly `calculateSyncTimeout` interplay.
- `ReplicationLog.java` — wrap each attempt in `apply()` (`:329-343`) with the per-attempt
  future/budget.
- `SyncModeImpl.java` / SAF mode impl — per-mode budget selection (SYNC 7 s vs SAF 15 s).
- Config constants for the two per-attempt budgets + peer-conf keys.

## Tests

1. **SYNC per-attempt timeout fires** — stub a peer write to block > 7 s; assert the attempt
   is cancelled and the group transitions to SAF within ~14 s (2 × 7 s), not minutes.
2. **Pipeline recovery is NOT cancelled** — stub a write that returns at ~2 s; assert it
   succeeds and the group stays in SYNC (guards against the 500 ms regression).
3. **SAF per-attempt uses the 15 s budget** — assert the degraded path waits longer before
   aborting.
4. **Scoped conf isolation** — assert peer manager's conf has the fast-fail overrides and the
   local FS conf is untouched (bulkheading).
5. **Executor drained on close** — assert no leaked non-daemon threads after
   `ReplicationLogGroup.close()`; worker threads are daemons.
6. **Cutover fail-stop unchanged** — with `failoverPending`, a SYNC failure still throws
   (aborts RS) rather than dropping to SAF (`SyncModeImpl.java:59-69`).

## Verify

```bash
mvn spotless:apply
mvn test -pl phoenix-core -Dtest=ReplicationLogGroupTest,ReplicationLogTest
```

## Open questions to resolve before implementing

1. Exact `dfs.client.retry.policy.spec` string (not recovered).
2. Final SYNC/SAF per-attempt values — 7 s / 15 s are the last agreed proposal; re-derive
   against current production latency before committing.
3. Reader round-buffer (9 s) coherence with the chosen budgets.
4. Whether to shorten the 5-min `WAL_SYNC_TIMEOUT` backstop now that per-attempt budgets
   bound the stall.