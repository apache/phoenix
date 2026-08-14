# WAL Sync Time Baseline and Phoenix HA Budget Recommendation

## Purpose

Phoenix HA synchronous replication (per `Phoenix_HA_ReArchitecture_for_Consistent_Failover.md`, §Dual Log Coordination) serializes two durable writes on the active cluster's write path: local HBase WAL sync, then remote replication log write to the standby HDFS. Client ACK happens only after both succeed. The client-visible latency under Phoenix HA is therefore bounded below by (local sync) + (remote sync).

To size the sync time budget realistically, this document captures the current production baseline for local WAL sync latency — both normal-case and tail — and proposes a budget and per-attempt cap for Phoenix HA's remote replication log write.

## Method

Two complementary data sources over a 24-hour window across all prod falcon_instances (aws-prod*):

1. **Argus time-series**: `hbase.regionserver.SyncTime_95th_percentile` per RS pod, sampled at 15-minute resolution. This captures the per-bucket p95 of local WAL sync time and is the authoritative view of "normal" sync latency — tail events are smoothed out within each 15-minute bucket, so the metric reports what the typical sync costs.
2. **Splunk RS logs**: `"Slow sync cost"` WARN lines with per-event durations. HBase's default `hbase.regionserver.wal.slowsync.ms` threshold is 100 ms — every logged event represents an individual sync that exceeded 100 ms. This captures the tail frequency and distribution that the Argus p95 metric does not surface.

Both are necessary. The Argus metric shows that syncs are fast most of the time; the Splunk logs show that even at fleet scale, tail syncs above 100 ms happen millions of times per day. Neither alone tells the whole story.

## Baseline: Normal-Case Sync Time

From Argus `SyncTime_95th_percentile`, 3,096 RS pods across all prod over 24 h at 15-minute resolution:

| Distribution | Per-pod p95 WAL sync time |
|---|---|
| Median pod's mean p95 | **2.4 ms** |
| 95th-percentile pod's mean p95 | 4.9 ms |
| 99th-percentile pod's mean p95 | 10.3 ms |
| Worst 15-min bucket, median pod | 7 ms |
| Worst 15-min bucket, p95 pod | 42 ms |
| Worst 15-min bucket, p99 pod | 229 ms |
| Absolute worst 15-min bucket, any pod | 1,686 ms |

Interpretation: local WAL sync p95 in steady state is **~2–5 ms** across the fleet. Even the 99th-percentile pod's worst 15-minute window sits at 229 ms, and only a handful of pods ever see a 15-minute p95 above a second.

## Baseline: Slow-Sync Tail Frequency

From Splunk `"Slow sync cost"` log lines, fleet-wide, 24 h:

| Falcon | Events | p50 ms | p95 ms | p99 ms | max ms | RS pods |
|---|---|---|---|---|---|---|
| **aws-prod5-uswest2** | **7,294,938** | 177 | 307 | 451 | 16,689 | 402 |
| aws-prod1-useast1 | 369,061 | 152 | 321 | 614 | 25,088 | 374 |
| aws-prod21-useast2 | 83,470 | 158 | 345 | 568 | 4,025 | 285 |
| aws-prod9-apnortheast1 | 29,720 | 194 | 270 | 350 | 2,919 | 64 |
| aws-prod16-eunorth1 | 27,189 | 143 | 849 | 1,609 | 5,379 | 167 |
| aws-prod2-apsouth1 | 21,349 | 154 | 342 | 505 | 2,938 | 102 |
| aws-prod3-eucentral1 | 11,064 | 146 | 462 | 777 | 4,112 | 250 |
| aws-prod13-euwest2 | 10,523 | 124 | 566 | 980 | 5,037 | 136 |
| (16 additional falcons) | <10,000 each | varied | varied | varied | up to 13,168 | varied |

Fleet total: roughly **8 million slow-sync events in 24 hours** across 24 prod falcons. Most individual events are 100–500 ms. Worst observed: 25 seconds on prod1-useast1.

### prod5-uswest2 outlier

prod5-uswest2 generates 7.29M slow-sync events in 24 h across 402 RS pods — roughly **17,800 slow syncs per RS per day, or one every five seconds per RS on average**. This is one to two orders of magnitude higher than other prod falcons. Either the cluster has genuinely higher tail sync latency than the rest of the fleet, or the slow-sync WARN threshold is configured lower on prod5. Either way, prod5 is the environment most likely to stress Phoenix HA's tail-latency behavior and is the natural candidate for any pre-rollout perf validation.

## Reconciling the Two Pictures

The Argus p95 (~2–5 ms) and the Splunk slow-sync count (millions/day) do not contradict each other. They measure different things:

- The Argus metric computes **p95 within each 15-minute bucket per pod**. In a heavy-write bucket with thousands of syncs, a hundred slow outliers get diluted — 95 % of syncs are fast, and the p95 stays low.
- The Splunk log fires on **each individual slow sync**. It surfaces the tail the p95 metric averages away.

Both observations are true simultaneously: normal sync is fast, and tail events above 100 ms are routine at fleet scale.

## Implication for Phoenix HA

The Phoenix HA write path serializes local WAL sync with remote replication log write. Assuming standby-side write-path stability is comparable to the active's (which matches operator experience and current evidence from the hard-failure investigation), the remote sync's latency distribution should resemble the local one.

- **Normal case (p95)**: 2.4 ms local + ~2.4 ms remote ≈ **~5 ms** end-to-end p95. Small and defensible.
- **Tail case**: a single-sided 300 ms tail event already happens millions of times per day on the active cluster. Under Phoenix HA, the same operation must also wait for the remote write. If the remote independently produces a similar tail on the same request, combined tail latency compounds: a 300 ms local tail plus a 300 ms remote tail yields ~600 ms end-to-end, at the same frequency the tail occurs today on a single side.
- **Pathological tail**: today's observed max is 25 s local. Under Phoenix HA, independent slow-sync events on both sides of the same request would produce client-visible blocks well past typical RPC timeouts. This is the class of event the retry policy and SAF fallback exist to absorb.

## Stress-Test Case: Slow-Disk Incident on prod17-apsoutheast3

A concrete example of what happens when the baseline breaks down. On 2026-04-25, `aws-prod17-apsoutheast3 / sam-bigdata1 / core1 / fkp-hbase1a` experienced 744 WAL-sync-timeout RS aborts across 20 RS pods over a ~6-hour window (04:31–10:30 UTC). This is the only prod cluster that saw any WAL-sync-timeout aborts in the preceding 7 days.

### What happened

Two DataNodes in a single availability zone (`dn-ap-southeast-3c-1` and `dn-ap-southeast-3c-2`, both in `ap-southeast-3c`) developed severe local disk slowness. During the 6-hour window they produced 99.99% of the cluster's slow-op events:

| DataNode | AZ | Slow events (6h) | avg ms | p95 ms | max ms |
|---|---|---|---|---|---|
| dn-ap-southeast-3c-1 | 3c | 227,312 | 1,053 | 3,518 | 13,514 |
| dn-ap-southeast-3c-2 | 3c | 184,334 | 1,153 | 3,882 | 30,610 |
| (7 other DNs in 3a, 3b) | — | 39 total | — | — | — |

The other 7 DataNodes in the cluster logged fewer than 10 slow events each in 6 hours. Exhibit:

```
2026-04-25 10:59:59,065 WARN  [...blk_1074861755_1154516]] datanode.DataNode - Slow flushOrSync took 2115ms (threshold=300ms), isSync:false, flushTotalNanos=2115404650ns, volume=file:/mnt/disk2/hdfs/, blockId=1074861755
```

### The RS view

During the same 6 hours, all 33 RS pods saw extreme WAL sync latency (from Argus at 5-minute resolution):

- `SyncTime_95th_percentile` per RS — per-RS peak: median 20.8 s, p95 43.1 s, max 54.8 s (on regionserver-11 at 05:20 UTC)
- `SyncTime_max` per RS — per-RS peak: median 27.7 s, p95 60.9 s, max 65.6 s

Mean p95 sync time across the full 6 h was 3+ seconds per RS — roughly **1,000× the normal fleet baseline of 2.4 ms**. The NameNodes on this cluster logged no upstream cause signal (no slow JournalNode batches, no block-placement failures); the slowness was downstream of the NN, localized to the two DNs in AZ `3c`.

### Why DN slowness escalates to a 5-minute WAL sync timeout

There is a gap between observed DN flush latency (p95 3.5 s, max 30 s) and the 5-minute WAL sync timeout that actually fired 744 times. A sequential "slow DN flush" does not, by itself, produce a 5-minute sync. The plausible mechanisms that close the gap:

1. **FSHLog backpressure stacking.** HBase serializes syncs through a small pool of `SyncRunner` threads. If each sync takes seconds and many are queued, an individual transaction's call waits for its position in queue plus its own sync. At a busy RS, cumulative pipeline wait compounds.
2. **DFSClient pipeline recovery loops.** When a DN in the pipeline becomes unresponsive (not merely slow), DFSClient waits `dfs.client.socket-timeout` (default 60 s) before declaring it dead and running pipeline recovery. Recovery contacts the NN, picks a replacement DN, and re-sends unsynced packets. With 2 of 9 DNs bad and 1 of 3 AZs affected, the replacement DN could land on the bad AZ again, causing recovery to loop. Three to five 60-s socket timeouts reach 5 minutes.
3. **`dfs.datanode.socket.write.timeout`.** Default is 480 s. A pipeline write stuck on a hung DN can block for 8 minutes before timing out — longer than the WAL timeout on its own.
4. **Argus bucketing hides the worst case.** `SyncTime_max` reports the max of syncs that *completed and were sampled* in the 5-minute bucket. Syncs that hit the WAL timeout and caused RS abort may never be recorded; the 744 aborts are partly invisible to the metric.

The most likely dominant mechanism is pipeline recovery looping on an unresponsive (not merely slow) DN. The observable `Slow flushOrSync` events are the visible symptom of disk pressure; the invisible symptom is brief periods where a DN is not flushing at all and the write pipeline is stuck on a 60 s socket timeout.

### Implications for Phoenix HA

This incident reinforces the design's asymmetric protection:

- **Active-side event**: Phoenix HA does not help. Local WAL sync itself exceeds 5 minutes, so step 1 of the Phoenix HA write fails before step 2 is attempted. Clients see errors today; they would see the same errors under Phoenix HA.
- **Standby-side event** (hypothetical mirror of this incident): if these two slow DNs had been on the standby HDFS instead of the active, Phoenix HA would flip to SAF within the proposed 2-second wall-clock budget. Client latency on the active would stay bounded even though the standby was grossly unhealthy. SAF would run for the full 6 hours; drain would begin after the 3c DNs recovered. The active cluster continues serving clients unaffected.
- **AZ-level concentration**: two DNs in one AZ were sufficient to take down the write path because default HDFS block placement puts one replica per AZ. The Phoenix HA SAF mechanism is the correct response — converting a 6-hour standby outage from an availability event into a DegradedStandby event.

### What this stress case teaches about the budget

The budget proposed below is sized for normal-case and typical-tail operation, not for incident-scale events. Under a standby-side equivalent of this incident:

- Initial writes see ~seconds of stall before SAF trips (write latency budget exhausted, not a gradual degradation).
- SAF absorbs the full incident duration.
- Drain resumes when the standby recovers, with recovery time proportional to incident duration.

The budget is *not* required to keep remote sync p99 low *during* incidents. It is required to (a) keep normal-case and typical-tail latency bounded, and (b) flip to SAF fast enough that an incident on the standby does not cascade into active-cluster client-visible latency. The slow-disk incident is exactly the shape of event the 2-second SAF trigger is designed to catch.

### Validation implication

Any Phoenix HA soak test should include a scenario where 1–2 DNs on the standby HDFS develop disk slowness (or become unresponsive). The test should confirm that:

- SAF trip happens within the wall-clock budget regardless of how the standby degrades (slow-but-responsive vs unresponsive).
- Client-visible active-cluster latency stays within the p99 budget during the degradation onset.
- Drain behavior on recovery is bounded and does not starve live writes.

## Recommended Sync Time Budget

Targets for the combined (local + remote) sync latency under Phoenix HA synchronous replication:

| Budget | Value | Basis |
|---|---|---|
| p50 end-to-end | **10 ms** | ~2× current local p50–p95 with headroom |
| p95 end-to-end | **50 ms** | ~2× current p99 local bucket; remains under default `slowsync.ms=100` |
| p99 end-to-end | **500 ms** | Aligns with typical client RPC timeout expectations |
| Per-attempt cap on remote write | **500 ms** | Sits above current p99 single-sided tail (~451 ms on prod5); short enough to trip SAF on a hung standby in a bounded time |
| Total wall-clock ceiling before SAF transition | **2 s** | Matches the retry-policy recommendation |

The per-attempt cap and wall-clock ceiling align with the retry-policy recommendation in `replication-writer-retry-policy-recommendation.md`. The two documents should be read together: this one establishes what the budget *should* be based on data; the retry-policy doc establishes how to enforce it.

## Why These Numbers

The 500 ms per-attempt cap is chosen to sit above the current single-sided p99 tail on the busiest prod falcon (451 ms on prod5-uswest2). That means normal traffic will not trip the retry budget under steady-state load — the writer stays in SYNC mode. But it is short enough that a genuinely hung standby is detected and flipped to SAF within ~2 seconds of wall clock, keeping client-visible latency bounded during incidents.

A tighter cap (e.g., 200 ms) would trip on normal tails and cause flapping between SYNC and SAF. A looser cap (e.g., 5 s) would let a slow-but-not-hung standby burn through client RPC timeouts before SAF engages.

## Validation Plan

Before Phoenix HA rollout:

1. **prod5 perf validation.** Given prod5-uswest2's ~1,000× higher slow-sync event rate, run Phoenix HA synchronous replication soak tests against prod5-scale workload and measure end-to-end write latency. The budget above assumes a standby roughly as fast as the active; prod5's tail profile is what would stress-test that assumption.
2. **Remote sync p50/p95/p99 metric emission.** The writer should emit per-attempt remote sync time as a histogram. Once collected, compare against this budget and revise if the remote's tail is materially worse than the local's.
3. **SAF-transition counters.** Count SYNC → SAF transitions per unit time and their time-to-trip distribution. If transitions happen frequently outside of genuine incidents, the per-attempt cap is too tight; if they happen rarely during known standby-side disruptions, the cap is too loose.

## Scope Caveats

- Data is 24 hours across all prod falcons. Longer windows (7 d) timed out during collection; the fleet-wide 24 h sample is a reasonable proxy for steady state but would not capture weekly maintenance cycles or known monthly patterns.
- Standby-side latency is assumed comparable to active-side. This assumption is grounded in operator experience but unverified by direct measurement against a real standby HDFS path. Validation (§Validation Plan, item 2) is needed before the budget is treated as authoritative.
- The 100 ms slow-sync threshold is an HBase default. If prod5's threshold is configured lower, the prod5 event volume is partially explained and the interpretation of prod5's "tail rate" should be revised. Worth confirming.
