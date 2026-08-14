# Phoenix HA Replication-Log Perf Baseline — 2026-06-11/12

Pre-v2 production baseline. Five metric dumps from `dev-phoenix-hbase5a`
(dev1-uswest2 / core002) analyzed with the `phoenix-ha-metrics` skill
(`scripts/analyze.py`, threshold 100). Goal framework: `docs/perf-goals.txt`.
Decomposition framework: `docs/replication-perf-analysis-and-metrics.md`.

> All five runs are **pre-v2** (ring-buffer coalescing not deployed). This is
> the baseline the post-v2 run will be validated against — not a v1-vs-v2
> comparison.

## Runs analyzed

| # | Dir | Window (UTC) | Dur | Table (replSync scope) | Active RS | Busiest RS rate |
|---|-----|--------------|-----|------------------------|-----------|-----------------|
| 1 | `...-1781298030` | 06-11 10:00–12:15 | 2.25h | PHSCEX.FEED_COMPOSITION | 13 | rs-22 @ 28.6/s |
| 2 | `...-1781301903` | 06-11 12:50–14:34 | 1.73h | PHSCEX.FEED_COMPOSITION | 22 | rs-3 @ 30.3/s |
| 3 | `...-1781303408` | 06-11 14:35–15:11 | 0.60h | PHSCEX.TOP_ENTITY_1M | 1 | rs-5 @ 340/s |
| 4 | `...-1781307515` | 06-11 18:16–21:22 | 3.10h | PHSCEX.FEED_COMPOSITION | 21 | rs-26 @ 82.5/s |
| 5 | `...-1781309068` | 06-12 01:58–05:23 | 3.42h | PHSCEX.FEED_COMPOSITION | 23 | rs-7 @ 56.6/s |

Run 3 is a **single-RS high-rate run** on a different (smaller, few-RS-hosted)
table — TOP_ENTITY_1M lands on only one active RS at 340 sync/s. It is the only
"saturation-ish" sample; treat its cluster row as one-RS, not a cluster mean.

Run 1 had no manifest and used the older `_numops` filenames + bundled
`analyze.py`/`analyze_v2.py`; the skill's `resolve()` handled the naming, and the
data shape is identical to the others.

---

## 1. Decomposition closure — does `syncTime ≈ RB + PW + Fs`?

Cluster-mean closure (`(LHS − ΣRHS)/LHS`), per run:

| Run | p50 closure | p99 closure |
|-----|-------------|-------------|
| 1 | **+3%** | −34% |
| 2 | **−3%** | −47% |
| 3 | **+7%** | −7% |
| 4 | **+4%** | −31% |
| 5 | **+7%** | −28% |

**Verdict: the equation closes at p50 in every run** (±7%, well inside the ±15%
tolerance). There is **no hidden component** — the residual-gap question is
resolved. The p99 overshoot (−7% to −47%) is the known structural artifact:
per-metric p99 buckets don't co-occur in time, so summing them overcounts. Use
**p50 fractions** for trustworthy decomposition; treat the p99 overshoot as a
noise floor, not a missing term.

---

## 2. Where the time goes — p50 component fractions

Cluster-mean p50 fractions of `PhoenixWALSyncTime`:

| Run | RingBuffer | FsSync | PendingWait |
|-----|-----------|--------|-------------|
| 1 | 18% | **79%** | 0% |
| 2 | 18% | **85%** | 0% |
| 3 | 18% | **75%** | 0% |
| 4 | 23% | **69%** | 4% |
| 5 | 26% | **66%** | 1% |

**FsSync dominates p50 cluster-wide (66–85%). RingBuffer is 18–26%.**
PendingSyncWait is negligible at p50 (≤4%) — coalescing is not waiting in steady
state.

### The load-dependent twist (this is the important part)

The cluster mean is dominated by quiet RSes. On the **busiest** RSes the picture
inverts — RingBuffer grows with load and becomes co-dominant with (run 4) or
larger than (run 5) FsSync:

| Run | Busiest RS | rate | syncTime p50 | RB% | Fs% | PW% |
|-----|-----------|------|--------------|-----|-----|-----|
| 4 | rs-26 | 82.5/s | 8.21 ms | **36%** | 39% | 2% |
| 5 | rs-7 | 56.6/s | **14.17 ms** | **44%** | 28% | 3% |
| 5 | rs-20 | 30.1/s | 4.80 ms | 34% | 42% | 1% |
| 5 | rs-10 | 28.7/s | 4.29 ms | 36% | 62% | 0% |

On rs-7 (run 5), the heaviest single-RS sample in the steady-state runs,
RingBuffer p50 is **6.24 ms** — larger than FsSync (4.00 ms). This is the regime
where v2 pays off: **ring-buffer time scales with load, and the worst-loaded RSes
are exactly where it dominates.** Quiet RSes (the bulk of the cluster) are
FsSync-bound and v2 barely touches them.

---

## 3. Phoenix FsSync vs HBase WAL syncTime (same RS)

p50 deltas (`(PhxFs − HBase)/HBase`), busiest 5 per run:

| Run | range of p50 delta | range of p99 delta |
|-----|--------------------|--------------------|
| 1 | +64% … +119% | −24% … +60% |
| 2 | +40% … +119% | −35% … +35% |
| 3 | +50% | +75% |
| 4 | +50% … +124% | −48% … −4% |
| 5 | +55% … +118% | −39% … +47% |

Phoenix `FsSyncTime` p50 sits **+50% to +124% above HBase WAL syncTime p50**
consistently. **This is NOT clean apples-to-apples evidence of an HDFS-layer
regression**, contrary to the prediction in `perf-goals.txt` that the two would
track within 10–20%:

- HBase WAL syncTime is **RS-wide** (all tables, all WALs, batched group-commit
  across the whole RS).
- Phoenix FsSyncTime is scoped to the **single HA-replicated table's log
  stream** — a different file, a different HDFS pipeline, a thinner per-stream
  write rate.

A thinner, single-stream fsync genuinely costs more per-op than an RS-wide
group-committed WAL fsync; the +50–120% is at least partly that, not "Phoenix
HDFS is slow." At p99 the delta even goes **negative** on many RSes (Phoenix
faster), because the RS-wide HBase WAL absorbs more tail contention. **The
actionable number is the absolute Phoenix FsSync latency** (~2 ms p50, ~8–12 ms
p99) — that is what the HA path actually pays. Use the HBase number only as an
order-of-magnitude check that the HDFS cluster is healthy (it is). Isolating a
true Phoenix-vs-HBase HDFS delta needs per-WAL-file scoped HBase syncTime, which
`MetricsWALSource` does not expose.

---

## 4. Load regime

| Run | sync rate/RS (min/med/max) | PendCount p99 (mean/worst) | BatchSize p99/p50 | Coalescing? | Catch-up? |
|-----|----------------------------|----------------------------|-------------------|-------------|-----------|
| 1 | 0.9 / 6.4 / 28.6 | 1.6 / 3.0 (rs-22) | 16.9× | No | Yes |
| 2 | 0.7 / 6.0 / 30.3 | 1.5 / 3.0 (rs-9) | 17.1× | No | Yes |
| 3 | 340 / 340 / 340 | 4.5 / 4.5 (rs-5) | 0.5× | No | No |
| 4 | 1.1 / 5.6 / 82.5 | 2.0 / 4.3 (rs-26) | 15.8× | No | Yes |
| 5 | 0.2 / 4.7 / 56.6 | 2.1 / 4.9 (rs-7) | 15.2× | No | Yes |

- **Coalescing is not engaging in steady state.** PendingSyncCount p99 stays
  1.5–2.1 cluster-wide; even the worst RS only reaches ~4–5. The consumer
  almost always fsyncs one pending request at a time. (Run 3's single RS at
  340/s only reaches pc99=4.5 — still not deep queueing.)
- **BatchSize p99/p50 ≈ 15–17×** in every steady run → the consumer periodically
  enters **catch-up bursts** (replay/backlog drain), with p99 batches of
  100s–1000s vs a p50 of ~6–9. The bursts are real but rare (p99, not p50), so
  they don't dominate the latency budget — but they are the source of the
  multi-thousand BatchSize max values (5k–29k).
- Steady-state production load is **light-to-moderate**: busy RSes 28–82 sync/s,
  median RS ~5–6/s. This is the `pc≈10` regime the simulator modeled, not
  saturation.

---

## 5. One-line conclusion & v2 prediction

**Cluster baseline (steady-state runs 1,2,4,5):** at p50 the residual gap is
~**18–26% RingBuffer, ~66–85% FsSync, ~0–4% PendingWait**. A v2 that cuts
ring-buffer time ~5× removes only ~15–21% of cluster-mean p50 syncTime → the
script's p99 projection leaves **~89–100% of current syncTime** on a
cluster-average basis. **On a cluster average, v2 buys little — FsSync-bound.**

**But the cluster average hides the win.** On the busiest RSes (rs-7, rs-26,
rs-20) RingBuffer is **36–44% of p50** and the absolute syncTime is 2–5× the
cluster median. A 5× ring-buffer cut on rs-7 (run 5) would take RingBuffer p50
from 6.24 ms → ~1.25 ms, dropping syncTime p50 from ~14 ms toward ~9 ms
(**~35% on the worst RS**). **v2's benefit is concentrated on the loaded RSes
that matter** — it flattens the busy-RS tail rather than moving the quiet-RS
median.

**Cross-validation vs simulator:** the simulator predicted ~2–3× syncTime
improvement at light-moderate load *if* syncTime is RingBuffer-dominated. The
baseline shows syncTime is RingBuffer-dominated **only on the busiest RSes**, and
FsSync-dominated everywhere else. So the simulator's 2–3× will hold on the
hot RSes and shrink to ~1.1–1.2× cluster-wide. The post-v2 run should be read
**per-busy-RS**, not on the cluster mean, or the win will look smaller than it is.

---

## Caveats (from the skill)

- Cluster numbers are **mean-of-per-pod-nonzero-mean**, not true merged
  percentiles — trend shape only. Per-RS histogram percentiles are the precise
  numbers.
- `PhoenixWALSyncTime_num_ops` oscillates (0/1 stale ↔ cumulative); deltas use
  the threshold-100 filter. The naive forward-delta inflates ops counts 5–500×
  (visible in every run's ACTIVITY table) — do not use it.
- Histogram value series are sparse (~3–20 of ~27–42 buckets non-zero per pod).
  Zero buckets = "no sample reported," not "0 ms."
- `below-thresh?` pods in the diagnostic had real traffic but no `num_ops`
  reading ≥100 in the window — short-window scrape artifact, not idle RSes.
  Run 3 (35 min) marked 27 of 28 RSes below-thresh for exactly this reason.
- ReplicationSyncTime is **per-table**; all other metrics are HA-group-wide. Do
  not compare them numerically as "caller overhead" where the table/HA-group
  ratio diverges from 1.
