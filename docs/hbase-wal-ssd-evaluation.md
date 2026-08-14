# Evaluating HBase WAL Writes Against the SSD Unwritten Contract

## Context

This document maps HBase WAL write traffic — flowing through HDFS to dedicated
SSD volumes on the DataNodes — onto the five rules of the *unwritten contract*
of solid-state drives formalized by He et al. (EuroSys '17). Those rules are:

1. **Request Scale** — issue large requests or many concurrent requests.
2. **Locality** — access with locality so FTL translation caches hit.
3. **Aligned Sequentiality** — start writes at block-aligned offsets and
   write low-to-high.
4. **Grouping by Death Time** — co-locate data that will be invalidated
   together so garbage collection can erase whole blocks cheaply.
5. **Uniform Data Lifetime** — keep data lifetimes similar to reduce
   wear-leveling cost.

The paper's central methodological move is *zombie curve* analysis — a
distribution of valid pages across blocks — which captures grouping-by-death-time
violations that simpler labels like "random vs. sequential" miss. We adopt
that frame here.

The motivation for writing this down: WAL latency and SSD wear come up
repeatedly in our HA-stack discussions, and we don't have a single reference
that grounds the discussion in the actual code path or in the paper's
vocabulary. This is that reference.

## Deployment assumptions

The analysis is specific to the topology this team runs in production. Three
assumptions are load-bearing:

1. **Dedicated WAL SSD volumes.** Each DataNode has one or more SSD volumes
   used *exclusively* for HBase WAL replicas. HFiles, MapReduce intermediate
   output, and other HDFS data live on separate volumes. WAL placement is
   pinned via HDFS heterogeneous storage policy (`ALL_SSD` or `ONE_SSD`) on
   the WAL root directory and `[SSD]` storage tags in `dfs.datanode.data.dir`.

2. **Production WAL writer is FSHLog.** The upstream-default `AsyncFSWAL` is
   *not* used; our build pins `hbase.wal.provider=filesystem`. FSHLog has a
   meaningfully different concurrency model — Disruptor ring buffer fronted
   by RPC handlers, with a pool of `SyncRunner` threads consuming sync work.

3. **Durability mode is `hflush`, not `hsync`.** WAL appends are flushed onto
   the HDFS pipeline (in-memory buffers on all three replicas plus an ack
   from each DataNode), but the DataNodes do **not** call
   `FileChannel.force()` on the local block file. The OS page cache on each
   DataNode is the durability boundary as far as HBase is concerned; the SSD
   only sees writes when the kernel's writeback machinery decides to flush
   dirty pages.

These assumptions reframe the analysis substantially:

- The SSD sees a homogeneous workload of short-lived, append-only files
  owned by a single writer.
- The SSD sees **no synchronous flush barriers** from the WAL path. Writes
  arrive opportunistically via writeback in whatever sized I/Os the kernel
  picks — typically multi-MB merged sequential I/Os. This dramatically
  softens the "frequent barriers" problem the paper highlights for
  `fdatasync`-bound applications like LevelDB and SQLite (Observations #4
  and #6 in the paper).

Non-goals:

- HFile and compaction write traffic on co-tenant SSDs.
- MemStore or block-cache tuning.
- Network-pipeline analysis (DataNode-to-DataNode replication).
- Deployments running with `hsync` durability — the §3.1 conclusion would
  flip toward the paper's pessimistic case.

## The path under evaluation

```
RegionServer thread
  └── WAL.append()           ─┐
        Disruptor ring buffer │  HBase
        SyncRunner × 5        ─┘
        ProtobufLogWriter
          └── DFSOutputStream.hflush()  ─┐
                pipeline ack             │  DFSClient
                packet (64 KB)           │
                ↓                        │
        DataNode BlockReceiver           │  HDFS
          └── ReplicaOutputStreams.write() ─┐
                page cache ──────────────────┤  Linux page cache
                kernel writeback             │
                ext4/XFS                    ─┘
                ↓
        Block layer / NCQ
          └── NAND program operations on SSD
```

Code anchors (file paths are within sibling repos at `/Users/tkhurana/soma/root/`):

**HBase WAL writer:**

- `hbase/hbase-server/src/main/java/org/apache/hadoop/hbase/regionserver/wal/FSHLog.java`
  — Disruptor + `SyncRunner` pool. `SYNCER_COUNT = "hbase.regionserver.hlog.syncer.count"`,
  `DEFAULT_SYNCER_COUNT = 5` at `FSHLog.java:115-118`. Pool wired in at
  `FSHLog.java:984-986`.
- `hbase/hbase-server/src/main/java/org/apache/hadoop/hbase/regionserver/wal/AbstractFSWAL.java`
  — roll/archive logic. `WAL_ROLL_MULTIPLIER` default `0.5f` at
  `AbstractFSWAL.java:495`; `MAX_LOGS = "hbase.regionserver.maxlogs"` at
  `AbstractFSWAL.java:148`.
- `hbase/hbase-server/src/main/java/org/apache/hadoop/hbase/regionserver/wal/ProtobufLogWriter.java`
  — frame format and the actual `hflush` vs `hsync` branch:
  `ProtobufLogWriter.java:76-88` (`forceSync == true → hsync()`,
  `forceSync == false → hflush()`).
- `hbase/hbase-server/src/main/java/org/apache/hadoop/hbase/wal/WALFactory.java`
  — provider dispatch. Verify our build sets `hbase.wal.provider=filesystem`.
- `hbase/hbase-server/src/main/java/org/apache/hadoop/hbase/wal/RegionGroupingProvider.java`
  — multi-WAL.

**HDFS DataNode local write:**

- `hadoop/hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/BlockReceiver.java`
  — `flushOrSync(boolean isSync)` at lines 426-467. Under `hflush` the caller
  passes `isSync=false`, so neither `streams.syncDataOut()` nor
  `streams.syncChecksumOut()` runs. `FileChannel.force()` is *not* called.
- `hadoop/hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/ReplicaOutputStreams.java`
  — `writeDataToDisk()` at line 146 is a buffered `dataOut.write(b, off, len)`.
- `hadoop/hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/FileIoProvider.java`
  — `fsync()` at line 152 calls `IOUtils.fsync(fos.getChannel(), false)`. Only
  reached when `isSync=true`.
- `hadoop/hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/fsdataset/RoundRobinVolumeChoosingPolicy.java`
- `hadoop/hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/fsdataset/AvailableSpaceVolumeChoosingPolicy.java`
  — neither honors the `storageId` hint passed in.
- `hadoop/hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/BlockPoolSliceStorage.java`
  — block deletion path. `FileIoProvider.delete()` → `File.delete()` → plain
  `unlink(2)`. **No `FITRIM`, no `fallocate(PUNCH_HOLE)` is issued anywhere
  in the Hadoop tree.**
- `hadoop/hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/StorageType.java`
  — `SSD` is one of the declared storage types but isn't enforced in volume
  selection.

## Rule-by-rule evaluation

### 3.1 Request Scale — *strongly conforming*

FSHLog uses a Disruptor ring buffer fronted by RPC handlers and consumed by
a `RingBufferEventHandler` that fans pending sync work out to **N
`SyncRunner` threads** (`hbase.regionserver.hlog.syncer.count`, default
**5** — `FSHLog.java:115-118`, wired at `FSHLog.java:984-986`). Each
`SyncRunner` issues `hflush` on the DFS output stream for a batch of
`SyncFuture`s, with batch size governed by
`hbase.regionserver.wal.sync.batch.count`.

The crucial fact in our deployment is that `hflush` does **not** trigger
`BlockReceiver.flushOrSync(true)` and therefore does **not** call
`FileChannel.force()` on the local block file (`BlockReceiver.java:426-467`;
`FileIoProvider.fsync` only fires on the `syncBlock=true` path). From the
SSD's point of view, the WAL path issues a continuous stream of buffered,
asynchronously-written-back writes with no synchronous flush barriers. This
is the *opposite* of the LevelDB/RocksDB `fdatasync` pattern in the paper —
there is no per-batch NCQ drain.

Effective request scale is excellent: kernel writeback typically coalesces
dirty pages into multi-MB sequential I/Os (governed by
`vm.dirty_writeback_centisecs`, `vm.dirty_ratio`, and the FS flusher), and
the five concurrent `SyncRunner`s keep multiple WAL streams flowing. This
is one of the rules where our deployment looks materially better than the
paper's worst case.

### 3.2 Locality — *strongly conforming*

WAL files are written sequentially within a single HDFS block until rolled
(`logrollsize = blocksize × hbase.regionserver.logroll.multiplier`, default
`0.5` per `AbstractFSWAL.java:495`, → roughly 32 MB for a 64 MB HDFS block).
HDFS lays each block out as one contiguous local file.

Because the SSD is dedicated to WAL traffic, the working set is tightly
bounded:

```
working set ≈ maxlogs × logrollsize     (default ≈ 32 × 32 MB ≈ 1 GB per RS)
            + archive lag
```

That fits comfortably in any modern FTL's translation cache, so on-demand
FTL miss ratio should be near zero — the equivalent of the leftmost
(low-cache-coverage) end of the paper's miss-ratio curves staying near
zero. Multi-WAL via `RegionGroupingProvider` increases the number of
parallel sequential streams; on a dedicated SSD this still has good
locality but raises the count of concurrently open extents the FTL must
track.

### 3.3 Aligned Sequentiality — *conforming*

`ProtobufLogWriter` writes a fixed magic header plus a delimited
`WALHeader` at file creation, then framed `WALKey + cells` records, and a
trailer. The header is small and not block-aligned, but each WAL file
lives in its own HDFS block — the DataNode creates a fresh `blk_<id>` file
via `unlink`-then-create on the local FS — so the SSD sees a fresh logical
extent that is written low-to-high sequentially.

On a dedicated WAL volume there is no interleaving with HFile writers.
Under `hflush`-only durability, kernel writeback further smooths the
on-device pattern — dirty pages of the data block file and its `.meta`
checksum file are flushed together by the kernel, often as larger merged
I/Os than the 64 KB packet size. That is even more friendly to hybrid-FTL
merges than the paper assumes.

### 3.4 Grouping by Death Time — *largely conforming, contingent on TRIM*

This is the rule the paper warns is most often violated. With WAL traffic
isolated to its own SSD, the picture is the cleanest of any rule. A WAL
file dies as a unit — it is rolled, archived, then deleted once all
regions have flushed past its sequence ID (`AbstractFSWAL.cleanOldLogs` /
`archiveLogFile`). All edits in the file share a similar death time, and
all data on the device shares a similar lifetime since there is no HFile
or compaction interference. This produces close to the ideal zombie-curve
cliff the paper describes.

Two residual issues remain:

- **Two-step archive.** A roll renames the closed WAL into the archive
  directory; later, a cleanup pass actually unlinks it. The SSD does not
  learn the data is dead until the unlink. Archive lag is bounded by
  `hbase.master.logcleaner.ttl` and replication catch-up; on a healthy
  cluster this is minutes, on a degraded one it can be much longer.

- **Hadoop never issues TRIM.** Block deletion is a plain `unlink(2)`;
  there is no `FITRIM`, no `fallocate(PUNCH_HOLE)`, no anything that tells
  the SSD "these LBAs are dead." Whether the FTL learns a WAL block is
  dead depends entirely on the local filesystem's mount option (`discard`)
  or a periodic `fstrim` cron. Without one of those, the SSD believes its
  logical space is full of valid data even though the WAL volume is mostly
  garbage by capacity. This is the same "ghost data" effect the paper
  describes for F2FS (Observations #15-17), but caused here by the storage
  stack rather than the FS. Even on a dedicated WAL volume this is a real
  sustainable-performance risk because the device sees high write traffic
  against an apparently full logical space.

### 3.5 Uniform Data Lifetime — *conforming*

With dedicated WAL volumes, all data on the device shares the same
workload class — short-lived (minutes to hours, gated by MemStore flush)
and roughly uniform across files. There is no co-tenancy with long-lived
HFile data, so the wear-leveling pressure described in paper Observation
#22 (which compares short-lived journal data against long-lived database
data) does not apply at the cross-workload level.

Within WAL traffic itself, lifetime variance is small: most WAL files have
similar sizes (capped by `logrollsize`) and similar dwell times. Two
minor sources of variance to be aware of:

- The currently-open WAL is appended to much more frequently than archived
  WALs are read or rewritten.
- Cross-DC replication lag can hold a small subset of archived WALs alive
  much longer than the rest.

The dedicated-volume assumption directly solves what would otherwise be
the biggest violation in the stack.

## Cross-cutting findings

- **The dedicated-WAL-volume topology already addresses the contract's
  hardest two rules** (Grouping by Death Time and Uniform Data Lifetime)
  by construction — there is no co-tenant workload to mix lifetimes with.

- **`hflush`-only durability is friendly to SSDs.** There is no per-batch
  fsync barrier on the WAL volume, so the paper's "frequent barriers
  degrade NCQ utilization" finding (Observations #4 and #6) does not
  apply. Effective request scale is governed by kernel writeback, not by
  HBase's sync cadence. This is a real advantage of our durability mode
  for sustainable SSD performance — and a real durability tradeoff worth
  being explicit about: a simultaneous kernel panic on all three DN
  replicas before writeback completes would lose unflushed WAL pages.
  HBase relies on at least one DN replica's page cache surviving long
  enough for writeback to fire.

- **The chief remaining risk is that Hadoop never issues TRIM.** Even on
  a perfectly homogeneous WAL volume, the FTL believes the device is full
  unless the local FS propagates discards. The `discard` mount option (or
  a periodic `fstrim`) is therefore load-bearing for sustainable
  performance.

- **Multi-WAL is not a free win.** It improves request scale but at some
  cost to locality and grouping. Under `hflush`-only durability on a
  dedicated WAL SSD, single-WAL is likely already device-bound. Evaluate
  empirically rather than enabling by default.

## Recommendations

Ordered by expected impact on this topology.

1. **Mount the dedicated WAL SSD volume with `discard`** (or run `fstrim`
   hourly via cron). Closes the §3.4 gap. Without this, the FTL sees
   apparent 100 % utilization regardless of how cleanly WAL files die.
   Single biggest sustainable-performance win on this topology.

2. **Verify HDFS storage policy is actually pinning WAL writes to SSD.**
   Run `hdfs storagepolicies -getStoragePolicy -path <hbase.wal.dir>`;
   confirm it returns `ALL_SSD` or `ONE_SSD`. Confirm `dfs.datanode.data.dir`
   tags the WAL volume `[SSD]`. A misconfigured policy invalidates the
   entire premise of this evaluation.

3. **Verify `hbase.wal.hsync` is `false` (i.e., `hflush`-only)** in
   production config. Document the durability tradeoff alongside the
   config: WAL durability depends on at least one surviving DN replica's
   page cache being flushed by writeback. This is the deliberate tradeoff
   that lets §3.1 be cheap; an operator who flips it without realizing
   what they're giving up will regress latency.

4. **Consider raising `vm.dirty_expire_centisecs` and
   `vm.dirty_writeback_centisecs`** modestly on DataNode hosts so
   writeback issues larger merged I/Os to the SSD, further amortizing FTL
   overhead. Don't raise so high that `vm.dirty_ratio` is hit often — that
   causes synchronous stalls. Pair with monitoring of `/proc/meminfo`
   `Dirty:` and writeback latency.

5. **Tune the FSHLog syncer pool only if WAL volume is under-utilized.**
   Raise `hbase.regionserver.hlog.syncer.count` (default 5) toward 8–10.
   With `hflush` durability the per-syncer pipeline is not a hard barrier,
   so this matters less than under `hsync`; measure first.

6. **Raise `hbase.regionserver.logroll.multiplier`** toward 0.95 so each
   WAL file fills its HDFS block before rolling. Reduces metadata churn
   and keeps the dedicated SSD's working set tight (§3.2, §3.3) without
   affecting durability.

7. **Right-size the WAL volume.** With `maxlogs × logrollsize` as a
   working-set lower bound, over-provision generously (≥4×) so the FTL
   has plenty of headroom even if archive cleanup lags. Especially
   important until recommendation #1 lands.

8. **Evaluate multi-WAL empirically** rather than defaulting it on. On a
   dedicated SSD with deep NCQ, single-WAL throughput is often already
   device-bound; multi-WAL adds parallel streams at some cost to grouping.

9. **Tighten WAL archive retention** (`hbase.master.logcleaner.ttl`) so
   archived files reach `unlink` quickly, shortening the window during
   which the FTL holds dead data alive. Coordinate with
   replication-lag SLOs.

10. **Watch SSD endurance metrics** (`smartctl -A` `Wear_Leveling_Count`,
    `Total_LBAs_Written`, vendor WAF counters). Even a "good" workload can
    wear the device fast if write amplification is poor; monitor and
    rotate proactively.

## Measurement plan

How to validate the analysis on a real cluster. Several items are paired
before/after recommendation #1 (`discard`) since that is the
highest-leverage change and the easiest to A/B.

- **Per-volume `iostat -xmt 1`** on the dedicated WAL SSD during
  steady-state write load. Track `await`, `aqu-sz`, `%util`, `wMB/s`.
  Pre/post recommendation #1.

- **`blktrace` + `btt`** on the WAL volume for a 60-second window.
  Convert to a request-size histogram and an NCQ-depth-over-time plot —
  directly comparable to Figures 3 and 4 in the paper. Validates §3.1.
  We expect the histogram to skew toward larger sizes and the NCQ-depth
  plot to *not* show frequent drains to zero (unlike the paper's LevelDB
  trace), reflecting `hflush`-only durability.

- **HBase metrics (JMX):** `SyncTime_*`, `AppendTime_*`, `slowAppendCount`,
  `slowSyncCount`. Track p50/p95/p99 across the recommendation rollout.

- **HDFS DataNode metrics (JMX):** `BytesWritten`, `BlocksWritten`,
  `FsyncCount` per volume. `FsyncCount` should remain near zero on the WAL
  volume under `hflush` durability — if it isn't, something is forcing
  fsync behind our backs.

- **SSD-side telemetry.** `smartctl -A` for `Total_LBAs_Written`,
  `Wear_Leveling_Count`, `Percent_Used_Endurance`, vendor-specific WAF if
  exposed. Capture daily. The most direct measurement of the §3.4 claim:
  WAF should drop noticeably once TRIM is in play because the FTL no
  longer thinks the device is full.

- **Filesystem-level.** `df` on the WAL volume vs. `Total_LBAs_Written`
  over time. The gap between filesystem utilization and device-perceived
  utilization is exactly the ghost-data effect §3.4 describes; closing it
  is what `discard`/`fstrim` does.

- **Synthetic test.** A YCSB heavy-write workload run twice — once with
  `discard` mount, once without — after the device has been
  pre-conditioned (paper's "sustainable performance" regime). Compare
  sustained throughput and p99 sync latency.

## Open questions / follow-ups

- Confirm where in our config we pin `hbase.wal.provider=filesystem`
  (forces FSHLog instead of upstream-default AsyncFSWAL). Document the
  rationale alongside this analysis so a future operator doesn't
  unknowingly flip back to AsyncFSWAL — that would change §3.1
  substantially (single consumer vs. five `SyncRunner`s).

- Across our production environments, is the dedicated-WAL-volume topology
  *actually* in place, and are storage policies set correctly? The whole
  evaluation rests on this. Worth a one-off audit.

- For deployments where the topology is *not* dedicated, a follow-up
  document should re-run §3.4 and §3.5 under the co-tenant assumption.
  WAL + HFile interleaving on the same volume is a well-known
  sustainable-performance hazard that the paper's Observation #20 maps
  directly onto.

- Is there appetite to land a `storageId`-aware volume-choosing policy in
  our Hadoop fork? The hint is plumbed through but ignored by both
  shipped policies; a small patch could enforce WAL→SSD even if the HDFS
  storage policy is misconfigured.
