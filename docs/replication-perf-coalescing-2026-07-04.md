# Phoenix HA Replication-Log Perf — Coalescing-Only Run, 2026-07-04

Post-coalescing production run. **PHOENIX-7931 (`32f4b65`, #2540 — coalesce
per-batch mutations into a single record) is deployed; index-replication
elimination (full v2) is NOT.** This is the coalescing-only re-run that
`docs/replication-perf-baseline-2026-06-11.md` flagged as pending — the clean
delta against the June pre-coalescing baseline.

Analyzed with the `phoenix-ha-metrics` skill (`scripts/analyze.py`, threshold
100). Dump: `phoenix-ha-metrics-1783142877`.

## Run context

| Field | Value |
|---|---|
| Window (UTC) | 2026-07-04 05:27–07:59 (2.53 h) |
| Cluster / domain | dev-phoenix-hbase5a / dev1-uswest2 / core002 |
| Table (replSync scope) | PHSCEX.FEED_COMPOSITION |
| Active RS | 14 of 28 |
| Busiest RS | rs-24 @ 22.6 sync/s |

**Load is lighter than the June busy runs** (busiest 22.6/s here vs 56–82/s in
runs 4–5). This confounds any syncTime-latency attribution — see caveat below.

---

## Headline: the coalescing signature is unmistakable

The one metric coalescing directly targets — BatchSize — collapsed:

| Metric | June (pre-coalescing) | July-04 (coalescing) |
|--------|-----------------------|----------------------|
| BatchSize p50 | 6.6 – 9.8 | **1.1** |
| BatchSize p99 | 112 – 165 | **1.4** |
| BatchSize p99/p50 | **15–17×** | **1.3×** |
| BatchSize max | 5,266 – 29,131 | **40** |

`MutationCellGrouper` coalesces a batch's mutations into a **single log record**
before it reaches the ring buffer, so the writer no longer sees the
multi-thousand-entry batches that produced June's catch-up spikes.
**BatchSize ≈ 1 across the fleet is coalescing working as designed** — this
signature is load-independent in shape, so unlike the latency numbers it is a
clean before/after. The "Consumer in catch-up?" heuristic flips from **Yes**
(every June run) to **No** (p99/p50 = 1.3×).

PendingSyncCount is unchanged (cluster p99 1.75 vs June 1.5–2.1) — coalescing did
not deepen the sync queue, as expected.

---

## Decomposition (still FsSync-bound at p50)

Cluster-mean p50 fractions of `PhoenixWALSyncTime`:

| | RingBuffer | FsSync | PendingWait | p50 closure |
|-|-----------|--------|-------------|-------------|
| July-04 | 21% | **76%** | 0% | **+3%** |
| June (typ.) | 18–26% | 66–85% | 0–4% | +3…+7% |

**The steady-state decomposition did not move.** p50 still closes within ±15%
(no hidden component); FsSync still dominates (~76%); RingBuffer ~21%. Coalescing
removed burstiness, not the steady-state syncTime shape — because at this load
RingBuffer was never the bottleneck.

p99 closure is **−73%** (worse than June's −28…−47%), driven by two RSes whose
RingBuffer p99 bucket is large and doesn't co-occur with the other terms in time
(**rs-22 RB p99 = 52.65 ms**, closure −181%; rs-24 RB p99 = 15.56 ms). This is
the known structural overshoot amplified by a couple of tail spikes — **not** a
missing component. Read p50. The script's "v2 leaves ~119% of p99" line is an
artifact of that rs-22 spike inflating the RB term; ignore it.

Absolute Phoenix FsSync: **~2.0–3.4 ms p50, ~9–17 ms p99** — in line with June.

---

## Phoenix FsSync vs HBase WAL syncTime (same RS)

Phoenix FsSync p50 runs **+32% to +200%** above HBase WAL p50; **negative at
p99** on 4 of 5 busiest RSes (Phoenix faster in the tail). Same confounded
pattern as June — HBase syncTime is RS-wide / group-committed, Phoenix is
single-stream. Not clean HDFS-regression evidence. HDFS cluster looks healthy.

---

## Conclusion & what it does / doesn't prove

- **Proven (clean before/after):** coalescing eliminated the catch-up bursts.
  BatchSize p99/p50 went 15–17× → 1.3×, max 29k → 40. This is the direct,
  load-independent effect of `MutationCellGrouper`.
- **Not moved:** steady-state p50 syncTime decomposition (~21% RB / ~76% Fs).
  Coalescing did not change where time goes at p50, because RingBuffer was not
  the p50 bottleneck at this load.
- **Cannot attribute from this data:** a syncTime *latency* win from coalescing.
  This window (busiest 22.6/s) is lighter than June's busy runs (56–82/s), so the
  syncTime/RingBuffer numbers are confounded by load. In June, RingBuffer became
  co-dominant (36–44% of p50) only on the busy RSes (rs-7, rs-26) — this window
  has no RS in that load band, so there is nothing here to show a
  coalescing-driven RingBuffer reduction against.

**To close the loop we still need a post-coalescing window at comparable
busy-RS load (~50–80 sync/s)** — then compare RingBuffer p50 on a busy RS
against June's rs-7 (RB p50 = 6.24 ms). That is the measurement that would
confirm or refute the coalescing latency benefit. This run confirms the
mechanism (batches collapse); it does not yet quantify the busy-RS latency win.

---

## Caveats (from the skill)

- Cluster numbers are mean-of-per-pod-nonzero-mean, not merged percentiles —
  trend shape only. Per-RS histogram percentiles are precise.
- `phxwalsync_num_ops` oscillates; deltas use the threshold-100 filter. Naive
  forward-delta inflates ops 5–500× (visible in the ACTIVITY table).
- 14 pods `below-thresh?` had real traffic but no `num_ops` reading ≥100 in the
  window — short/quiet-window artifact, not idle RSes.
- ReplicationSyncTime is per-table; all other metrics are HA-group-wide.
