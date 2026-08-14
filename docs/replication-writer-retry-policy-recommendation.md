# Recommendation: Bound Replication Log Writer Retry Behavior by Wall Clock

## Context

Under the Phoenix HA synchronous replication design (`Phoenix_HA_ReArchitecture_for_Consistent_Failover.md`, §Dual Log Coordination, §Handling Synchronous Replication Failures, §Replication Log Writer), the active cluster's Phoenix coprocessor must complete a synchronous write to the standby cluster's HDFS before acknowledging a client mutation. When that remote write fails, the writer retries a configurable number of times before flipping to Store-and-Forward (SAF) / DegradedStandby.

The current retry policy is **5 retries, 100ms apart**, with each attempt running to the underlying HDFS RPC timeout. The 100ms gap only controls the spacing between attempts; it does not cap how long an individual attempt can block.

## The Problem

The retry budget is entirely dominated by the per-attempt timeout, which inherits HDFS client defaults rather than being capped at the Phoenix layer. This produces client-latency tails that are inconsistent with Phoenix HA's "failover in seconds" objective.

Relevant HDFS client defaults:

- `dfs.client.socket-timeout`: 60s per socket
- `ipc.client.connect.timeout`: 20s per connect attempt
- `dfs.client.block.write.retries`: 3 retries on `addBlock` failures

### Worst-case timing against a degraded standby

**Standby's JN quorum slow but reachable** (e.g., 2.5s–5.5s per batch, observed in production on `aws-prod0-uswest2 / sam-bigdata1 / core1` on 2026-04-30 19:12–19:15 UTC): each Phoenix writer attempt returns in ~5s as a slow success/failure. 5 attempts × ~5s + 4 × 100ms gaps ≈ **25–30s** of blocked client writes before SAF kicks in.

**Standby's JN quorum unreachable** (IPC connection exceptions, NN hangs on `addBlock`): each attempt runs to `dfs.client.socket-timeout`. 5 × 60s + 4 × 100ms ≈ **5 minutes** of blocked client writes before SAF kicks in.

Both cases exceed typical client-side RPC timeouts. The first produces visible latency regressions; the second produces user-visible errors before the design's SAF fallback has a chance to engage.

### Why this matters specifically for Phoenix HA

Under asynchronous replication today, standby-side HDFS or JN issues are invisible to active-cluster clients — replication lag grows and the peer catches up later. Under Phoenix HA synchronous replication, **standby-side problems become client-visible on the active cluster** during the retry window. This is a new failure surface the retry policy must explicitly bound, not inherit.

Observed production baseline: slow-JN-on-standby events are not rare tail behavior. Any maintenance window, patch rollout, or transient JN disruption on the standby side will trigger them. The retry policy is hit on the regular operational path, not only during incidents.

## Recommendation

Bound the retry budget by **wall clock**, not by a count of attempts whose individual duration is unbounded.

### Primary recommendation: per-attempt timeout + total wall-clock budget

1. **Per-attempt timeout at the Phoenix layer: 500ms.** Short enough that a slow-but-responsive standby HDFS is treated as failed before it can serialize a client's latency budget. Long enough to tolerate normal network variance.
2. **Total wall-clock budget: 2 seconds.** Across all attempts and gaps. Once the budget is exhausted, the writer transitions to SAF immediately regardless of how many attempts completed.
3. **Retry count: keep at 5** as an upper bound, but expect the budget to trip first in slow cases.

With these bounds, the worst-case client-visible latency spike during a standby HDFS disruption is ~2 seconds, independent of how bad the disruption is.

### Alternative: circuit-breaker-style trip

Equivalent effect, different framing: treat the writer's mode transition as a circuit breaker rather than a retry loop.

- In SYNC mode, attempt the remote write with a short per-attempt timeout.
- On N consecutive failures *or* a wall-clock exhaustion (whichever trips first), open the breaker and transition to SAF.
- While open, writes go only to the local SAF queue; a background prober attempts a cheap health-check against the standby HDFS at a coarse interval (e.g., every 10s).
- On probe success, enter half-open state: send the next real write synchronously. If it succeeds, close the breaker (SYNC). If it fails, re-open.

This avoids the cost of burning client-facing retries on a standby HDFS that is clearly unhealthy, and naturally decouples failure detection from recovery detection.

## Configuration surface

Expose the timeouts as configurable rather than hardcoded:

- `phoenix.replication.writer.attempt.timeout.ms` (default 500)
- `phoenix.replication.writer.budget.ms` (default 2000)
- `phoenix.replication.writer.max.retries` (default 5)

Operators should be able to tune these per deployment based on observed standby HDFS latency characteristics without a code change.

## Metrics

To validate the policy in production and to detect when it's firing, the writer should emit:

- Count of SYNC → SAF transitions per unit time.
- Count of retries attempted before each transition.
- Distribution of time-to-trip (wall clock from first attempt to SAF).
- Distribution of per-attempt latency in SYNC mode.
- Count and duration of SAF → SYNC transitions (drain + probe latency).

These metrics make it possible to distinguish "retry budget is too tight, tripping on normal variance" from "retry budget is too loose, burning client latency before tripping."

## Open questions for the Phoenix team

1. Is there already an implementation in progress that caps per-attempt time at the Phoenix layer, or does the current code path inherit HDFS client defaults?
2. How is the drain from SAF back to SYNC prioritized against live SYNC-mode traffic? If drain contends with live writes for throughput, that extends the window during which failover consistency is degraded.
3. The design's default rotation interval for replication log files is one minute per source RS. Is file creation (which requires a standby-NN RPC) the main point at which the retry policy is hit, or does every hsync also go through the retry path? The answer affects how often the retry budget is exercised under normal load.
