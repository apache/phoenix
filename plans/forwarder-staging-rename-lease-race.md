# Forwarder Staging-then-Rename to Eliminate the Cross-Cluster Lease Race

## Context

Analysis of the production logs (`fwd_1.csv`, `fwd_2.csv`, destination cluster `dev_phoenix_hbase5b`)
surfaced a cross-cluster HDFS lease race in the replication log forwarder.

`ReplicationLogDiscoveryForwarder.processFile` copies a fallback-cluster log file to the peer
standby cluster with a single `FileUtil.copy(..., dst, deleteSource=false, overwrite=false)` at
`ReplicationLogDiscoveryForwarder.java:150`, writing directly onto the final replay-eligible name
`dst = <ts>_<origin>.plog` (`getWriterPath`, line 148). `FileUtil.copy` does
`dstFS.create(dst)` → stream → `close()`, so the file is **published at its final `.plog` name
before `close()` completes**.

The standby's replay consumer lists that directory through `isValidLogFile`
(`ReplicationLogTracker.java:408-411`), a pure `endsWith(".plog")` check. It picks up the
half-written file, force-recovers the HDFS lease (single-writer model), and the forwarder's
`close()` then fails with `LeaseExpiredException`. Because `processFile` throws at the copy,
execution never reaches the throughput-based `checkAndSetModeAndNotify(STORE_AND_FORWARD,
SYNC_AND_FORWARD)` at lines 155-161 — so the RS is stuck in STORE_AND_FORWARD longer than needed.
No data is lost (the source stays in `out_progress` and is retried ~60s later), but the ERROR is
spurious and the mode transition is delayed.

**Fix:** copy to a `.fwd`-suffixed staging name (invisible to replay), then atomic same-directory
`rename` to the final `.plog`. Replay only ever sees the fully-written, atomically-published file.

This change is predicated on the prior commit that keys `dst` on the **origin** server name
(`getServerName`, `ReplicationLogTracker.java:431-437`) — that is what makes the "rename returned
false ⟹ this exact logical file was already delivered" reasoning sound, since distinct logical
files never share a `dst`.

## Scope decisions (confirmed with user)

- **Forwarder-only.** No standby-side (`ReplicationLogDiscoveryReplay`) changes.
- **No new failover scan** (`getStagingFiles`) and **no consistency-point capping.** The existing
  `out_progress`-empty gate already covers the forwarder-side transition (proof below), and the
  standby-side cross-cluster exposure is pre-existing and not worsened by this change.
- **Orphan handling = deterministic name + `overwrite=true` (self-healing).** No dedicated sweep.

## Change 1 — `processFile` staging-then-rename

File: `phoenix-core-server/src/main/java/org/apache/phoenix/replication/ReplicationLogDiscoveryForwarder.java`
(method `processFile`, lines 138-162).

Keep lines 140-148 (compute `srcFS`, `srcStat`, `ts`, `originServerName`, `remoteShardManager`,
stable `dst`). Replace the single copy at line 150 with copy-to-staging + rename:

```java
FileSystem dstFS = remoteShardManager.getFileSystem();
Path staging = new Path(dst.getParent(),
    dst.getName() + ReplicationShardDirectoryManager.STAGING_FILE_EXTENSION); // <ts>_<origin>.plog.fwd

long startTime = EnvironmentEdgeManager.currentTimeMillis();
try {
  // (1) copy bytes to the staging name; overwrite=true reclaims any orphan from a prior crash.
  //     Suffix is not .plog, so replay never sees it (isValidLogFile == false).
  FileUtil.copy(srcFS, srcStat, dstFS, staging, /*deleteSource*/ false, /*overwrite*/ true, conf);

  // (2) atomic same-dir publish under the replay-eligible name.
  if (!dstFS.rename(staging, dst)) {
    // (3) rename returned false. If dst already exists, a prior attempt of THIS logical file
    //     already published it and replay has not yet consumed+deleted it — the retry raced ahead
    //     of replay. dst is keyed on the stable getWriterPath(ts, originServerName), so it can only
    //     belong to this same file. Don't throw (avoids the FileAlreadyExists retry-forever loop
    //     the current overwrite=false copy suffers). NOTE: this is NOT exactly-once dedup — if
    //     replay has already deleted dst, exists(dst) is false and we fall through to publish
    //     again (see "Delivery semantics" below). That re-publish is safe because replay is
    //     idempotent.
    if (dstFS.exists(dst)) {
      LOG.info("Destination {} already present (retry raced ahead of replay) for src={}", dst, src);
      dstFS.delete(staging, false); // best-effort drop of the redundant staging copy
    } else {
      throw new IOException("Failed to rename staging file " + staging + " to " + dst);
    }
  }
} catch (IOException | RuntimeException e) {
  // (4) best-effort cleanup on any failure before a successful rename.
  try {
    dstFS.delete(staging, false);
  } catch (IOException cleanupEx) {
    LOG.warn("Failed to clean up staging file {} after error", staging, cleanupEx);
  }
  throw e; // rethrow so processOneRandomFile leaves the source in out_progress for retry
}

long copyTime = EnvironmentEdgeManager.currentTimeMillis() - startTime;
LOG.info("Copying file src={} dst={} size={} took {}ms", src, dst, srcStat.getLen(), copyTime);
if (logGroup.getMode() == STORE_AND_FORWARD
    && isLogCopyThroughputAboveThreshold(srcStat.getLen(), copyTime)) {
  checkAndSetModeAndNotify(STORE_AND_FORWARD, SYNC_AND_FORWARD);
}
```

Notes:
- The throughput / mode-switch block (lines 155-161) is preserved and now reliably reached on the
  happy path — this fixes the "stuck in STORE_AND_FORWARD" symptom.
- `deleteSource=false` is essential: the source stays in `out_progress` and is deleted only by
  `markCompleted` after `processFile` returns.
- No new imports (`FileSystem`, `Path`, `FileUtil`, `IOException` already imported at lines 24-28).

## Change 2 — staging suffix constant + `isValidLogFile` hardening

File: `phoenix-core-server/src/main/java/org/apache/phoenix/replication/ReplicationShardDirectoryManager.java`
(next to `LOG_FILE_EXTENSION`, line 68):

```java
/**
 * Suffix for in-flight forwarded log files staged on the peer cluster. A file with this suffix is
 * mid-copy and MUST NOT be treated as replay-eligible. The forwarder writes <ts>_<server>.plog.fwd
 * and atomically renames to <ts>_<server>.plog once fully written.
 */
public static final String STAGING_FILE_EXTENSION = ".fwd";
```

The forwarder appends this to the final name (`dst.getName() + ".fwd"`), producing
`<ts>_<origin>.plog.fwd`. That name fails `endsWith(".plog")`, so staging files are
already invisible to every listing that gates on `isValidLogFile` — `getNewFilesForRound`,
`getInProgressFiles`, `getOlderInProgressFiles`, `getNewFiles`.

Harden `isValidLogFile` (`ReplicationLogTracker.java:408-411`) to reject staging explicitly — this
documents intent at the one chokepoint all listings share, and is load-bearing if the suffix is
ever shortened:

```java
protected boolean isValidLogFile(Path file) {
  final String fileName = file.getName();
  if (fileName.endsWith(ReplicationShardDirectoryManager.STAGING_FILE_EXTENSION)) {
    return false; // staging file: mid-copy forward, not yet published
  }
  return fileName.endsWith(ReplicationShardDirectoryManager.LOG_FILE_EXTENSION);
}
```

## Why no failover changes are needed (invariant)

**Invariant:** a `.fwd` staging file on the peer exists only while its source is still present in
the fallback cluster's `out_progress`.

Proof from the code:
1. `processOneRandomFile` (`ReplicationLogDiscovery.java:356-373`) calls `processFile` at line 363,
   then `markCompleted` at line 364 — only on normal return.
2. `markCompleted` (`ReplicationLogTracker.java:268-348`) is the sole deleter of the source.
3. The revised `processFile` removes the `.fwd` via `rename` (or cleanup/`delete` on error) before
   returning. A surviving `.fwd` ⟹ rename didn't complete ⟹ `processFile` didn't return normally
   ⟹ `markCompleted` never ran ⟹ source still in `out_progress`.

Contrapositive: `out_progress` empty ⟹ no `.fwd` in flight.

The forwarder-driven SYNC transition already gates on exactly this: `processNoMoreRoundsLeft`
(`ReplicationLogDiscoveryForwarder.java:169-193`) requires
`replicationLogTracker.getInProgressFiles().isEmpty()`, and that tracker lists the fallback's
`out_progress` (forwarder tracker built from `getLocalShardManager()`, lines 76-78). "out_progress
empty" is a strict superset of "no `.fwd` pending" — the existing gate suffices.

Because `dst` is the stable `getWriterPath(ts, originServerName)`, every retry of one logical file
maps to the identical `.fwd` path, and staging uses `overwrite=true` — exactly one `.fwd` per
logical source file, no fan-out across retries or forwarding RegionServers.

**Out of scope (documented, not fixed):** the standby-side `shouldTriggerFailover`
(`ReplicationLogDiscoveryReplay.java:490-531`, `STANDBY_TO_ACTIVE`) runs on the peer and cannot
observe the fallback's `out_progress`. A mid-flight `.fwd` there is data acked against the
fallback's `out`-write in STORE_AND_FORWARD but not yet replayed. This is the **same** exposure the
current in-place copy already has (a mid-write `.plog` carries the origin timestamp — backlog, so
`getNewFiles(nextRound, currentRound)` at line 521 wouldn't catch it either) and only matters in
the narrow window where the fallback keeps forwarding while the peer promotes. Staging-then-rename
does not worsen it — it removes the lease-corruption window. Any cross-cluster failover handshake
hardening is a separate work item. `getConsistencyPoint` needs no change: `.fwd` files are filtered
from every listing, so they neither advance nor corrupt the point.

## Failure-mode table

S = source in fallback `out_progress`; D = final `.plog` on peer; F = `.fwd` staging on peer.

| # | Crash point | State after crash | Correctness | Recovery |
|---|---|---|---|---|
| 1 | Before staging copy starts | S; no F; no D | Not delivered | `processInProgressDirectory` re-picks S; full retry |
| 2 | During staging copy (step 1) | S; F partial; no D | Not delivered; F invisible | Retry re-copies F `overwrite=true`, renames. Orphan self-healed |
| 3 | Copy done, before rename | S; F complete; no D | Not delivered; F invisible | Retry overwrites F, renames. Self-healed |
| 4 | During rename (step 2, atomic) | (S,F,no D) or (S,no D→D) | Never a half-file at D | Pre-rename → retry as #3; post-rename → #5 |
| 5 | After rename, before markCompleted | S; no F; D | Delivered, source uncleaned | Retry: if replay hasn't yet deleted D, `rename==false` + `dst exists` → drop F, return success → markCompleted deletes S. If replay already deleted D, retry re-publishes → **at-least-once re-replay** (safe, replay idempotent — see Delivery semantics) |
| 6 | During markCompleted delete | S maybe; no F; D | Delivered, cleanup pending | Existing markCompleted retry/prefix-match; idempotent |

Key property: replay never sees a half-written `.plog` — it only ever sees D, published atomically.

## Delivery semantics (at-least-once)

Forwarding is **at-least-once**, and this fix does not change that — it only closes the torn-file /
lease-recovery window.

The standby replay consumer **deletes D after replaying it**: replay reuses the same
`processOneRandomFile` driver (`ReplicationLogDiscovery.java:356-373`); its `processFile`
(`ReplicationLogDiscoveryReplay.java:197-202`) only reads, then `markCompleted`
(`ReplicationLogTracker.java:282`) deletes the file. There is **no filename-level dedup** on the
standby — a recreated same-name file is suppressed only by the monotonic round pointers
(`lastRoundInSync` / `lastRoundProcessed`, `ReplicationLogDiscoveryReplay.java:378-394`, `312-323`).

Consequence for failure-mode row #5/#6 (source S survives after D was published — `markCompleted`
delete failed, or the RS died between `processFile` returning and `markCompleted`): if replay has
already consumed **and deleted** D, the S retry sees `exists(dst) == false`, re-publishes D, and
replay may process it a **second time**. This double-replay window is **pre-existing** — the current
`overwrite=false` in-place copy has the identical exposure (S survives → D deleted by replay → retry
recreates D). Staging-then-rename neither introduces nor worsens it.

This is safe because **replay is idempotent**: each mutation is applied with its original cell
timestamp, so re-applying the same file converges to the same state. Exactly-once dedup (a persisted
processed-file marker or consistency-point gating on the standby) is deliberately **out of scope** —
it lands on the replay path, is orthogonal to the lease race, and is a separate work item.

## Orphan handling

A `.fwd` orphan survives only on a process kill between the staging copy and the rename (the
`catch` at step (4) covers ordinary exceptions). Self-heal requires no new code: the source stays
in `out_progress`, is re-picked by the age-gated probabilistic `processInProgressDirectory`, and the
retry recomputes the identical staging path and re-copies with `overwrite=true` — truncating and
rewriting the orphan in place. One logical file ⟹ one `.fwd` path ⟹ no accumulation.

Residual leak (deferred, harmless): if after a crash the source `out_progress` file is *also* never
reprocessed (independently deleted, or its shard permanently stops being visited), the `.fwd` is
never overwritten or renamed. It stays invisible to replay but consumes an inode indefinitely. A
peer-side age-based `.fwd` sweep (analogous to `getOlderInProgressFiles`) could reclaim it later —
**explicitly out of scope** for this change.

## Test plan

Both suites use real local FileSystem (`FileSystem.getLocal`) via `TemporaryFolder`, wrappable in
Mockito spies for stubbing `rename`/`exists`/`delete`. No new FS abstraction needed.

In `ReplicationLogDiscoveryForwarderTest` (follow `testForwardPreservesOriginServerIdentity`,
lines 164-199 — direct `processFile` call + `peerFs` inspection):

1. **`testForwardPublishesOnlyAfterRename`** (core regression guard). Spy the peer FS; stub
   `rename(staging, dst)` with a `doAnswer` asserting, before delegating: `exists(staging)` true,
   `exists(dst)` false, and a tracker over the peer shard sees no new files. After return: `dst`
   exists, `staging` gone, listing = exactly `<ts>_<origin>.plog`.
2. **`testForwardRetryOntoExistingDestinationSucceeds`** (mode #5). Pre-create final `dst`. Call
   `processFile` — asserts no throw, no `.fwd` remains, `dst` unchanged (already-delivered branch).
3. **`testForwardReclaimsOrphanStagingFile`** (mode #2/#3). Pre-create a stale `.fwd` with sentinel
   bytes. Call `processFile` — `dst` exists with source content, no `.fwd` remains.
4. **`testForwardRenameFailureLeavesSourceForRetry`** (step (4)). Stub `rename` → false and
   `exists(dst)` → false. Assert `IOException` thrown, `.fwd` cleaned up, `dst` not created, source
   in-progress file untouched.
5. **`testForwarderReachesSyncAfterStaging`** (mode transition). Reuse the existing
   `testLogForwardingAndTransitionBackToSyncMode` harness — verifies staging+rename still drains all
   files and transitions back to SYNC (throughput block + `processNoMoreRoundsLeft` gate still fire).

In `ReplicationLogTrackerTest`:

6. Extend **`testIsValidLogFile`** (lines 1094-1122): `<ts>_<server>.plog.fwd` is invalid;
   `<ts>_<server>.plog` remains valid.
7. **`testStagingFilesExcludedFromListings`**: shard dir + in_progress dir with mixed
   `.plog` / `.plog.fwd`; assert all four entry points — `getNewFilesForRound`, `getNewFiles`
   (shard dir), `getInProgressFiles`, `getOlderInProgressFiles` (in_progress dir) — list the
   published `.plog` and exclude the staging file.

## Sequencing

1. Add `STAGING_FILE_EXTENSION` to `ReplicationShardDirectoryManager` (no deps).
2. Harden `isValidLogFile` in `ReplicationLogTracker` (deps: 1).
3. Rewrite `processFile` in `ReplicationLogDiscoveryForwarder` (deps: 1).
4. Tests (deps: 1-3).

## Verification

```bash
mvn spotless:apply
mvn test -pl phoenix-core -Dtest=ReplicationLogDiscoveryForwarderTest,ReplicationLogTrackerTest
```

End-to-end: `testForwardPublishesOnlyAfterRename` reproduces the lease race (asserts nothing is
replay-eligible while bytes are under `.fwd`) and `testForwarderReachesSyncAfterStaging` confirms
the STORE_AND_FORWARD → SYNC_AND_FORWARD → SYNC path is no longer stalled by a copy-time failure.
