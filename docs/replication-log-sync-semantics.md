# ReplicationLog Sync Semantics: hsync to hflush Migration

**Date:** 2026-04-24
**JIRA:** PHOENIX-7562
**Scope:** `phoenix-core-server/src/main/java/org/apache/phoenix/replication/log/`

---

## 1. Summary

This change switches the Phoenix ReplicationLog's default sync behavior from `hsync()` (fsync to disk) to `hflush()` (flush to datanode memory), matching HBase WAL's default durability semantics. A configuration knob (`hbase.wal.hsync`) is reused from HBase so that operators control both HBase WAL and Phoenix ReplicationLog durability with a single setting.

---

## 2. Background: hflush vs hsync in HDFS

HDFS exposes two durability levels through `FSDataOutputStream`:

| | hflush | hsync |
|---|---|---|
| **HDFS call** | `FSDataOutputStream.hflush()` | `FSDataOutputStream.hsync()` |
| **Packet flag** | `syncBlock = false` | `syncBlock = true` |
| **Data reaches** | Datanode OS page cache (kernel buffers) | Physical disk (via `fsync` system call) |
| **Visible to readers** | Yes | Yes |
| **Durable on disk** | No | Yes |
| **POSIX equivalent** | `write()` + `flush()` | `write()` + `fsync()` |

### What happens on the datanode

The key code path is in `BlockReceiver.flushOrSync(boolean isSync)` (hadoop `BlockReceiver.java`):

**hflush (isSync=false):**
1. `checksumOut.flush()` -- flush checksum data to kernel buffers
2. `streams.flushDataOut()` -- flush block data to kernel buffers
3. ACK enqueued **immediately** (before flushOrSync returns)
4. No `FileChannel.force()` calls

**hsync (isSync=true):**
1. `checksumOut.flush()` -- flush checksum data to kernel buffers
2. `streams.syncChecksumOut()` -- **fsync checksum file** (`FileChannel.force(true)`)
3. `streams.flushDataOut()` -- flush block data to kernel buffers
4. `streams.syncDataOut()` -- **fsync data file** (`FileChannel.force(true)`)
5. `replicaInfo.fsyncDirectory()` -- **fsync directory metadata** (once per block)
6. ACK enqueued **after** all fsync calls complete

### HBase WAL default behavior

HBase WAL uses hflush by default, controlled by `hbase.wal.hsync` (default `false`), defined in `HRegion.java`. The flag is read in `AbstractFSWAL` and threaded through `AsyncProtobufLogWriter.sync(boolean forceSync)` to `FanOutOneBlockAsyncDFSOutput.flush(boolean syncBlock)`, which embeds the `syncBlock` flag in the HDFS packet header.

### Prior Phoenix ReplicationLog behavior

Phoenix ReplicationLog **hardcoded hsync** in two places:
- `HDFSDataOutput.sync()` called `delegate.hsync()` unconditionally
- `AsyncFSDataOutput.sync()` called `delegate.flush(true)` unconditionally

There was no configuration to change this.

---

## 3. Change Description

### Approach

Construction-time configuration: the `useHsync` boolean is read from `hbase.wal.hsync` config, passed through `LogFileWriterContext`, and set on `HDFSDataOutput` / `AsyncFSDataOutput` at construction time. The `sync()` method dispatches to either `hsync()` or `hflush()` based on that flag.

Per-call parameterization (as HBase does internally) was considered and rejected because Phoenix ReplicationLog has no per-mutation durability concept -- all mutations in a batch share the same durability requirement.

### Files modified

| File | Change |
|------|--------|
| `LogFileWriterContext.java` | Reads `HRegion.WAL_HSYNC_CONF_KEY` from config; exposes `getUseHsync()`/`setUseHsync()` |
| `HDFSDataOutput.java` | Constructor takes `useHsync`; `sync()` dispatches to `hsync()` or `hflush()` |
| `AsyncFSDataOutput.java` | Constructor takes `useHsync`; `sync()` dispatches to `hsync()` or `hflush()` |
| `LogFileWriter.java` | Passes `context.getUseHsync()` to `HDFSDataOutput` constructor |
| `LogFileWriterSyncTest.java` | Existing tests set `hbase.wal.hsync=true`; new `testSyncWithHflush()` test |
| `LogFileFormatTest.java` | Updated `HDFSDataOutput` constructor call |

### Configuration

| Key | Default | Effect |
|-----|---------|--------|
| `hbase.wal.hsync` | `false` | When `false`: ReplicationLog uses `hflush()`. When `true`: uses `hsync()`. Same key controls HBase WAL behavior. |

---

## 4. Expected Performance Improvements

### Per-sync latency reduction

Each ReplicationLog sync previously triggered **three fsync system calls per datanode** in the replication pipeline:

1. `FileChannel.force(true)` on the block data file
2. `FileChannel.force(true)` on the checksum file
3. `FileChannel.force(true)` on the directory (once per block)

With HDFS replication factor 3, these calls execute sequentially through the pipeline. Each fsync must wait for the disk controller to confirm the write is on persistent storage.

| Metric | hsync (before) | hflush (after) | Improvement |
|--------|----------------|----------------|-------------|
| Single sync (HDD) | 5-15ms | <1ms | ~10-15x |
| Single sync (SSD) | 0.5-2ms | <1ms | ~2-5x |
| P99 sync (HDD) | 50-200ms | 2-5ms | ~20-50x |
| P99 sync (SSD) | 5-20ms | 1-3ms | ~5-10x |

The largest improvement is at **P99/P999 tail latencies**. fsync latency has a long tail due to:
- Disk queue depth contention (other writes sharing the disk)
- Write amplification in SSDs (garbage collection pauses)
- OS I/O scheduler reordering
- Journaling overhead in the datanode's local filesystem (ext4/xfs)

With hflush, sync latency becomes a function of **network RTT + memory copy**, which is far more predictable.

### ACK timing improvement

With hsync, the datanode enqueues the pipeline ACK **after** fsync completes. The client blocks until the ACK propagates back through all 3 datanodes.

With hflush, the datanode enqueues the ACK **immediately** after flushing to kernel buffers, before any disk I/O. This eliminates disk wait time from the critical path entirely.

### Impact on Phoenix write path

The Phoenix ReplicationLog write path is:

```
RPC threads --> ring buffer --> consumer thread --> ReplicationLog.sync() --> HDFS
                                                         |
                                            RPC threads block on syncFuture.get()
```

Multiple RPC threads' sync requests are batched into a single HDFS sync call. Every RPC thread blocks on its `syncFuture` until the consumer thread completes the sync. Therefore:

- **Lower RPC latency**: Every write RPC pays the sync cost. Reducing sync from 5-15ms to <1ms directly reduces write RPC time.
- **Reduced tail latency**: P99 write latency drops significantly since fsync tail latency (50-200ms) is eliminated from the critical path.
- **Higher throughput**: The single consumer thread spends less time blocked on HDFS I/O, allowing it to drain the ring buffer faster. This raises the throughput ceiling before ring buffer backpressure kicks in.
- **Less contention during rotation**: Log rotation creates a new HDFS file and closes the old one. Both operations involve sync calls. Faster syncs mean shorter rotation windows and less impact on in-flight writes.

### Estimated end-to-end improvement

For a write-heavy workload where ReplicationLog sync is on the critical path:

| Workload | Expected improvement |
|----------|---------------------|
| Write RPC P50 latency | 10-30% reduction (sync is one component of total RPC time) |
| Write RPC P99 latency | 40-70% reduction (eliminates fsync tail) |
| Peak write throughput | 20-50% increase (consumer thread less I/O-bound) |

Actual numbers depend on storage hardware, HDFS cluster load, and the ratio of sync time to total RPC processing time.

---

## 5. Durability Analysis

### What is lost

With hflush, data that has been "synced" exists in:
- The HDFS client's pipeline (acknowledged)
- All 3 datanodes' OS page cache (kernel buffers)

It does **not** exist on persistent storage until the OS flushes dirty pages, which happens:
- Periodically via `dirty_writeback_centisecs` (default 5 seconds on most Linux distributions)
- When kernel dirty page thresholds are reached (`dirty_ratio` / `dirty_background_ratio`)
- On clean shutdown

### Failure scenarios

| Failure | Data loss? | Explanation |
|---------|-----------|-------------|
| RegionServer process crash | No | Data is in datanode page cache, not RS memory |
| Single datanode crash (machine/disk) | No | Data exists in other 2 replicas' page cache |
| Two datanode crashes | No | Data exists in 1 remaining replica's page cache |
| **All 3 datanodes crash simultaneously** | **Possible** | If OS hasn't flushed page cache to disk yet (window: typically <5 seconds) |
| Network partition | No | Data already acknowledged by all datanodes |
| Datacenter power loss (all machines) | **Possible** | Same as all-datanode crash |

### Why this is acceptable for ReplicationLog

1. **Replication logs are a secondary copy.** The primary mutations are already committed and durable in the HBase WAL on the source cluster. The replication log is used to replay mutations to the standby cluster. If replication log data is lost, it can be reconstructed from the source cluster's WAL or tables.

2. **Probability is extremely low.** Simultaneous failure of all 3 datanodes hosting a block's replicas, within the ~5 second window before OS page cache flush, is an extraordinarily unlikely event. HDFS rack-awareness policies typically place replicas across different racks, further reducing correlated failure probability.

3. **HBase itself accepts this tradeoff.** HBase WAL -- which stores the **primary** copy of mutations, not a secondary one -- defaults to hflush. Phoenix ReplicationLog has strictly weaker durability requirements than HBase WAL, so it should be at least as relaxed.

4. **Consistency with HBase.** Using the same `hbase.wal.hsync` config means the ReplicationLog durability guarantee is always at least as strong as the HBase WAL's. If an operator decides they need hsync for their HBase WAL, the ReplicationLog automatically gets it too.

---

## 6. Configuration Guidance

| Scenario | Recommended `hbase.wal.hsync` |
|----------|-------------------------------|
| Standard deployments (default) | `false` (hflush) |
| Latency-sensitive write workloads | `false` (hflush) |
| Regulatory/compliance requiring fsync | `true` (hsync) |
| Single-rack deployments (correlated failure risk) | `true` (hsync) |

No separate Phoenix configuration is needed. The `hbase.wal.hsync` setting applies uniformly to both HBase WAL and Phoenix ReplicationLog.
