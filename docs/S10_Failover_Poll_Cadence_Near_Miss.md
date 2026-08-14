t# S10 Failover-Under-Load: The Poll-Cadence Near-Miss

**Status:** analysis of a real test-bed run (2026-07-25, kind two-cluster DR
test-bed, Phoenix build with the PHOENIX-7562 rotation-suspend fix).
**Companion to:** the S10 section of `Phoenix_HA_Failover_Test_Scenarios.md`.

## TL;DR

A *planned* failover under sustained write load converged **correctly** — no
deadlock, no dual-active, zero RPO, data byte-for-byte identical — but took
**~131 s**, over the documented **120 s** SLA ceiling. The excess is **not** a
drain-volume problem and **not** the old deadlock. It is a **scheduling
artifact**: the standby's replay poller runs on a rigid fixed-rate grid, and on
this run it woke **~20 ms before** the final round became eligible for replay,
so it saw "nothing to do," went back to sleep, and did not look again for a
**full ~60 s cycle**. A 20 ms miss cost ~59 s of wall-clock.

## The setup that produced it

- Two clusters, `cluster-a` = ACTIVE, `cluster-b` = STANDBY.
- Sustained HA-connection write load pinned to the ACTIVE (continuous
  ~500-row UPSERT rounds).
- `initiate-failover` fired **mid-stream** at **06:24:00**.
- Roles flipped atomically (a→STANDBY, b→ACTIVE) at **06:26:12**.
- Elapsed: **~131 s**.

Correctness was clean throughout: rotation on the demoting cluster suspended at
cutover and minted zero new `.plog` files; promotion + demotion committed in the
same ~2 ms ZK op (no dual-active); both clusters ended with the identical row
set; `validate-replication` PASSed byte-for-byte.

## The two independent clocks

The failover clock under load is governed by two schedules that do **not** know
about each other.

### Clock 1 — when a round becomes *eligible* for replay

A replication round covers a fixed 60 s window. The standby's replay reader
refuses to process a round until it is provably complete — it waits a round
duration plus a safety buffer past the round's end:

```
eligible when:  now − roundEndTime  ≥  roundTimeMills + bufferMillis
```

Source: `ReplicationLogDiscoveryReplay.getFirstRoundToProcess()`
(`ReplicationLogDiscoveryReplay.java:388`).

Defaults:

| Quantity | Value | Source |
|---|---|---|
| `roundTimeMills` | 60 000 ms | shard-directory round duration |
| `bufferMillis` | 15 % of round = **9 000 ms** | `DEFAULT_WAITING_BUFFER_PERCENTAGE = 15.0`, `ReplicationLogDiscoveryReplay.java:104` |

So a round that **starts** at time `T` (window `T` → `T+60s`) is not eligible
until:

```
T + 60s (round close) + 9s (buffer)  =  T + 69s
```

For this run, the final data-bearing round started at **06:24:00**, so it became
eligible at:

```
06:24:00 + 69s  =  06:25:09
```

> An **unloaded** failover has no data-bearing trailing round, so it never waits
> on this gate and converges in seconds. This ~69 s cost appears **only** under
> load, when the cutover leaves a final round that still carries writes.

### Clock 2 — when the poller next *looks*

The replay service polls on a rigid, fixed-rate grid:

```java
scheduler.scheduleAtFixedRate(() -> { startReplicationReplay(); },
    0, executorFrequencySeconds, TimeUnit.SECONDS);
```

Source: `ReplicationLogReplayService.java:193,199`.
Interval knob: `phoenix.replication.replay.service.executor.frequency.seconds`,
default **60**.

`scheduleAtFixedRate` fires on an **immovable grid** anchored to service start —
here at roughly the `:08` mark each minute: …`06:24:08`, `06:25:08`,
`06:26:08`… Nothing shifts that grid; it fires regardless of what any round is
doing.

## The near-miss, to the millisecond

```
   time (UTC)        event
   ─────────────────────────────────────────────────────────────
   06:24:00.010      cutover; cluster-a mints its final writer, then
                     suspends rotation (0 new files afterward)
   06:24:08.9xx      poller tick — final round not closed yet → empty
   06:25:08.980      poller tick — asks "any round to process?"
                        eligibility needs 06:25:09.000  →  NOT YET
                        logs: "Found first round to process as Optional.empty"
   06:25:09.000      round BECOMES eligible ......... missed by ~20 ms
        ⋮            poller asleep on its fixed grid (nothing happens)
   06:26:08.9xx      poller tick — now eligible → processes the file
   06:26:08.983      HDFS lease recovery: attempt=0 fails
   06:26:12.984      lease recovered attempt=1 after 4001 ms;
                     "Invalid Trailer … proceeding"; 0 mutations
                     (it was the header-only suspended-rotation file)
   06:26:12.99x      promotion + demotion commit atomically
   ─────────────────────────────────────────────────────────────
```

The poller checked at **06:25:08.980**, the gate said *not eligible* (it was
~20 ms early), so it logged `Optional.empty` and went back to sleep. The round
became eligible at **06:25:09.000** — but the poller, being fixed-**rate** (not
fixed-delay), had no "check again shortly" behavior. Its next look was the
next grid tick at **06:26:08**. **A 20 ms miss cost a full ~60 s cycle.**

## Raw log evidence

Every timestamp above is taken verbatim from the run. Lines are lightly trimmed
(long `.plog` paths abbreviated with `…`); thread names retained so they can be
grepped.

**cluster-a `regionserver-0` — cutover, rotation suspends, minting stops, demote**
(the demoting cluster; `ReplicationLogRotation-testHAGroup-0` is the rotation
timer, `Curator-PathChildrenCache-0` is the ZK state watcher):

```
06:24:00,010 [ReplicationLogRotation-testHAGroup-0] replication.ReplicationLog: Created new writer: … path=…/in/shard/000/1784960640001_regionserver-0.…plog … generation=3]
06:24:01,375 [Curator-PathChildrenCache-0] jdbc.HAGroupStoreClient: Detected state transition for HA group testHAGroup from ACTIVE_IN_SYNC to ACTIVE_IN_SYNC_TO_STANDBY on LOCAL cluster
06:24:01,375 [Curator-PathChildrenCache-0] replication.ReplicationLogGroup: HAGroup testHAGroup entered cutover gate; suspending rotation
06:25:00,001 [ReplicationLogRotation-testHAGroup-0] replication.ReplicationLog: HAGroup testHAGroup rotation suspended: failover pending
06:26:00,000 [ReplicationLogRotation-testHAGroup-0] replication.ReplicationLog: HAGroup testHAGroup rotation suspended: failover pending
06:26:12,994 [Curator-PathChildrenCache-0] jdbc.HAGroupStoreClient: Detected state transition for HA group testHAGroup from ACTIVE_IN_SYNC_TO_STANDBY to STANDBY on LOCAL cluster
```

The writer minted at `06:24:00,010` is the **last** one; the two
`rotation suspended: failover pending` lines are the 60 s rotation ticks
(`06:25:00`, `06:26:00`) firing and minting **nothing** — proof the
PHOENIX-7562 guard held and no new `.plog` files appeared after cutover.

**cluster-b `regionserver-0` — the empty tick, the 60 s dead cycle, then replay**
(the promoting cluster; `Phoenix-ReplicationLogDiscoveryReplay-0` is the replay
poller):

```
06:25:08,980 [Phoenix-ReplicationLogDiscoveryReplay-0] reader.ReplicationLogDiscoveryReplay: Found first round to process as Optional.empty for haGroup: testHAGroup
06:25:08,981 [Phoenix-ReplicationLogDiscoveryReplay-0] replication.ReplicationLogTracker: Number of new files found 1        ← the file IS present…
06:25:08,982 [Phoenix-ReplicationLogDiscoveryReplay-0] replication.ReplicationLogTracker: Number of new files found 0        …but its round is not yet eligible (needs 06:25:09.000)
        ⋮   (poller asleep on its fixed 60 s grid — nothing between here and the next tick)
06:26:08,979 [Phoenix-ReplicationLogDiscoveryReplay-0] reader.ReplicationLogDiscoveryReplay: Found first round to process as Optional[ReplicationRound{startTime=1784960640000, endTime=1784960700000}] for haGroup: testHAGroup
06:26:08,982 [Phoenix-ReplicationLogDiscoveryReplay-0] reader.ReplicationLogDiscoveryReplay: Starting to process file …/in_progress/1784960640001_regionserver-0.…_f75fd788-….plog
06:26:08,983 [Phoenix-ReplicationLogDiscoveryReplay-0] reader.RecoverLeaseFSUtils: Failed to recover lease, attempt=0 on file=…_f75fd788-….plog after 0ms
06:26:12,984 [Phoenix-ReplicationLogDiscoveryReplay-0] reader.RecoverLeaseFSUtils: Recovered lease, attempt=1 on file=…_f75fd788-….plog after 4001ms
06:26:12,986 [Phoenix-ReplicationLogDiscoveryReplay-0] reader.ReplicationLogProcessor: Invalid Trailer for file …_f75fd788-….plog
06:26:12,988 [Phoenix-ReplicationLogDiscoveryReplay-0] reader.ReplicationLogProcessor: Completed processing log file …_f75fd788-….plog. Total mutations processed: 0
06:26:12,992 [Curator-PathChildrenCache-0] jdbc.HAGroupStoreClient: Detected state transition for HA group testHAGroup from STANDBY_TO_ACTIVE to ACTIVE_IN_SYNC on LOCAL cluster
```

The `06:25:08,980` `Optional.empty` line is the near-miss itself: the file was
already on disk (`Number of new files found 1` the very next millisecond), but
`getFirstRoundToProcess()` returned empty because `now (06:25:08.980)` had not
yet reached `roundEndTime + roundTimeMills + bufferMillis (06:25:09.000)`. There
is then **nothing in the log for a full minute** until the next grid tick at
`06:26:08,979`. Note the promotion commit on cluster-b (`06:26:12,992`) and the
demotion on cluster-a (`06:26:12,994`) land within **2 ms** of each other — the
atomic cutover, no dual-active window.

## Why fixed-*rate* is what makes it hurt

Two scheduling styles would behave very differently here:

- **Fixed-delay** waits N seconds *after each run finishes*. A run that found
  nothing could be followed by another attempt one interval later — but more
  importantly, the phase drifts, so you would not deterministically re-miss the
  same eligibility instant.
- **Fixed-rate** (what is used) fires on the immovable grid regardless. When a
  tick returns empty there is no short retry — the code just waits for the next
  grid slot, ~60 s away.

The poller has no notion of "the thing I'm waiting for will be ready in 20 ms,
let me look again soon." It looked, saw not-ready, and its next opportunity was
a minute out. That ~59 s of dead time is a pure scheduling artifact: nothing was
draining, nothing was stuck, no data was at risk. The round sat
**eligible-and-unprocessed** from 06:25:09 to 06:26:08 solely because the two
clocks were 20 ms out of phase.

## Decomposition of the ~131 s

| Segment | Duration | Cause |
|---|---:|---|
| Round-close + buffer | ~69 s | final round (start 06:24:00) not eligible until 06:25:09 — structural, load-only |
| Poll-cadence near-miss | ~59 s | poller tick at 06:25:08.980 was ~20 ms early → lost a full 60 s grid cycle to 06:26:08 |
| HDFS lease recovery | ~4 s | `attempt=0` fail → `attempt=1` after 4001 ms on the open, header-only `.plog` |
| **Total** | **~131 s** | — |

## Why this is a real (if benign) design smell

The phase relationship between the **poll grid** and the **round/buffer grid**
is effectively arbitrary per deployment (it depends on when the replay service
happened to start). Depending on that phase:

- **Best case** — the tick lands just *after* eligibility → near-zero extra wait.
- **Worst case** — the tick lands just *before* eligibility (this run) → you
  lose almost a full poll interval, every time.

So the worst-case failover-under-load latency structurally includes
**"up to one full poll interval"** on top of the round-close+buffer wait:

```
worst case  ≈  wait-for-final-round-to-close
             + bufferMillis (9 s)
             + up-to-one-full-poll-interval (≤ 60 s)
             + lease-recovery (~4 s)
```

That sum can exceed **120 s with everything working correctly**. The 120 s SLA
ceiling is therefore in direct tension with
`bufferMillis + executor.frequency.seconds`.

## Recommendations

1. **Make the poller eligibility-aware.** Instead of a blind fixed-rate 60 s
   grid, compute the time until the next round becomes eligible
   (`roundEndTime + roundTimeMills + bufferMillis − now`) and schedule the next
   wake-up for *that* instant (plus a small epsilon). This removes the near-miss
   class entirely — you never wake 20 ms too early and then sleep a full cycle.
2. **Or** switch to `scheduleWithFixedDelay` with a **short** interval (e.g.
   5–10 s) so a near-miss costs seconds, not a minute. Cheap because an
   ineligible check is a couple of metadata reads.
3. **Or** reduce `phoenix.replication.replay.service.executor.frequency.seconds`
   for HA groups where fast planned-failover convergence matters (trade: more
   idle polling).
4. **Revisit the 120 s SLA** against `bufferMillis + poll_interval`. Either the
   ceiling should account for the structural worst case, or the knobs above
   should be tuned so the worst case fits under it.

## Caveats on the numbers

- The **round-grid arithmetic** (69 s eligibility, ≤60 s poll interval) is
  config-driven and **cluster-independent** — it will reproduce anywhere with
  these defaults.
- The **absolute lease-recovery time** (~4 s) and exact replay throughput are
  **kind-specific** (single-node HDFS). On a real cluster these differ, so the
  *absolute* 131 s should be re-measured on real hardware — but the *shape* of
  the problem (near-miss → lost poll cycle) does not depend on the environment.
- This was a **planned** failover (`initiate-failover`). Unplanned failover
  (crash of the whole ACTIVE) follows a different promotion path and is not
  characterized here.