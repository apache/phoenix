# Store-and-Forward: The In-Progress Retry Lottery Delays SYNC Re-entry

**Status:** analysis of a real test-bed run (S7-mtime, 2026-07-25, kind
two-cluster DR test-bed, Phoenix build with the PHOENIX-7562 origin-identity +
rotation-suspend fixes).
**Companion to:** the S7 / S7-mtime sections of
`Phoenix_HA_Failover_Test_Scenarios.md` and
`S10_Failover_Poll_Cadence_Near_Miss.md`.

## TL;DR

When the peer HDFS is transiently unavailable, the store-and-forward **forwarder**
renames each pending `.plog` into `out_progress/` *before* attempting the
peer-side copy. If the copy fails (peer down), the file is **left stranded in
`out_progress/`**. From that moment the only code path that will ever retry it is
`processInProgressDirectory()`, which runs on a **5 % per-round random draw**
(`phoenix.replication.forwarder.in.progress.processing.probability`, default
`5.0`). So even after the peer HDFS is fully healthy again and the file is
trivially copyable, it sits unshipped for a **geometrically-distributed** number
of rounds — expected ~20 rounds ≈ **~20 minutes** at the default 60 s round
cadence. Because SYNC re-entry is gated on `out_progress/` being empty, the HA
group stays `ACTIVE_NOT_IN_SYNC` / `DEGRADED_STANDBY` for that whole window even
though RPO is already zero and nothing is actually wrong.

**Observed:** in the S7-mtime run, 7 real data files (465 KB–1 MB each) were
stranded by copy failures during the outage; the peer came back at **07:24:59**
but the retry draw did not come up until **07:51:09** — **~26 minutes of pure
lottery latency after the peer was healthy**. The actual drain, once it fired,
took **~0.5 s** for all 7 files.

**Fix under consideration:** raise
`phoenix.replication.forwarder.in.progress.processing.probability` from `5` to
`50`, collapsing the expected wait from ~20 rounds to ~2 rounds (~1–2 min). This
is a mitigation for *transient* copy failures; it is not a substitute for fixing
a *deterministic* copy failure (see "What this does and does not fix").

## How a file gets stranded

The forwarder processes each round in two distinct passes
(`ReplicationLogDiscovery.processRound`, `ReplicationLogDiscovery.java:260-272`):

```java
protected void processRound(ReplicationRound replicationRound) throws IOException {
  ...
  processNewFilesForRound(replicationRound);            // :266  — EVERY round, unconditional
  if (shouldProcessInProgressDirectory()) {             // :267  — random gate
    processInProgressDirectory();                       // :269
  }
  ...
}
```

The stranding happens inside `processOneRandomFile`
(`ReplicationLogDiscovery.java:356-373`), which is called by *both* passes:

```java
private Optional<Path> processOneRandomFile(final List<Path> files) throws IOException {
  Path file = files.get(...);
  Optional<Path> optionalInProgressFilePath = Optional.empty();
  try {
    optionalInProgressFilePath = replicationLogTracker.markInProgress(file);   // :361 — RENAME into out_progress/ FIRST
    if (optionalInProgressFilePath.isPresent()) {
      processFile(optionalInProgressFilePath.get());                            // :363 — THEN copy to peer
      replicationLogTracker.markCompleted(optionalInProgressFilePath.get());    // :364
    }
  } catch (IOException exception) {
    LOG.error("Failed to process the file {}", file, exception);
    optionalInProgressFilePath.ifPresent(replicationLogTracker::markFailed);    // :368 — stays in out_progress/
    return Optional.of(file);                                                   // :370 — reported failed, retried later
  }
  return Optional.empty();
}
```

`markInProgress` (`ReplicationLogTracker.java:356-388`) is a **rename** of the
source file into the in-progress (`out_progress/`) directory, stamping it with a
UUID and a rename-timestamp suffix (`<ts>_<server>_<UUID>_<renameTs>.plog`).
This rename is step **one**, before any bytes are copied to the peer.

The peer copy itself is in the forwarder's `processFile`
(`ReplicationLogDiscoveryForwarder.java:97-155`):

```java
FileUtil.copy(srcFS, srcStat, dstFS, staging, false, true, conf);   // :117 — throws if peer HDFS unreachable
if (!dstFS.rename(staging, dst)) { ... }                            // :118 — publish
```

When the peer namenode is down, `FileUtil.copy` throws; the exception unwinds to
`processOneRandomFile`'s catch, which calls `markFailed` and leaves the file in
`out_progress/`. The file has **already been moved out of `out/shard/`**, so:

## Why the trap is a trap

`processNewFilesForRound` — the pass that runs **every** round — only scans
`out/shard/NNN/`. A file sitting in `out_progress/` is invisible to it. The
**only** path that revisits `out_progress/` is `processInProgressDirectory()`
(`ReplicationLogDiscovery.java:312`), and that pass is gated behind a random
draw:

```java
protected boolean shouldProcessInProgressDirectory() {                       // :279-282
  return ThreadLocalRandom.current().nextDouble(100.0)
      < getInProgressDirectoryProcessProbability();
}
```

For the forwarder, `getInProgressDirectoryProcessProbability()` resolves to
`phoenix.replication.forwarder.in.progress.processing.probability`, defaulting to
the generic `DEFAULT_IN_PROGRESS_DIRECTORY_PROCESSING_PROBABILITY = 5.0`
(`ReplicationLogDiscoveryForwarder.java:57,219-221`;
`ReplicationLogDiscovery.java:76,497-498`).

So each round, there is only a **5 % chance** the forwarder even *looks* at the
stranded file. On top of that, when the pass does run, the file must be older
than `in.progress.file.min.age.seconds` (default **60 s**,
`ReplicationLogDiscovery.java:100`) and gets at most
`in.progress.file.max.retries` (default **1**,
`ReplicationLogDiscovery.java:90`) attempt that round.

The number of rounds until the first successful draw is geometric with p = 0.05:
**expected 1/p = 20 rounds**. At a 60 s round cadence that is **~20 minutes** of
latency contributed by nothing but the draw.

## Why this blocks SYNC re-entry

The forwarder only re-admits the group to SYNC from
`processNoMoreRoundsLeft` (`ReplicationLogDiscoveryForwarder.java:158-187`), and
its precondition is an **empty** in-progress directory:

```java
if (replicationLogTracker.getInProgressFiles().isEmpty()                     // :163
      && replicationLogTracker.getNewFilesForRound(...).isEmpty()) {         // :164-165
  ...
  long waitTime = logGroup.setHAGroupStatusToSync();                         // :174
  ...
}
```

This gate is **correct** — it must not declare SYNC while a file is genuinely
unforwarded (the store-and-forward safety invariant). The problem is not the
gate; it is that a copyable file is *needlessly* kept unforwarded by the retry
lottery, so the gate correctly refuses SYNC for the entire ~20-minute expected
wait. The group stays `ACTIVE_NOT_IN_SYNC` on the writer side and
`DEGRADED_STANDBY` on the reader side that whole time.

## Evidence: the S7-mtime run

**Fault window.** cluster-b's namenode **and** all datanodes scaled to 0 at
`07:18:31`; restored (`namenode=1`, `datanode=3`) at `07:24:59`. cluster-a stayed
ACTIVE under sustained HA-connection write load throughout.

**7 files stranded during the outage.** All were renamed into `out_progress/`
between 07:19 and 07:25 (rename-timestamp suffixes below), their peer copies
having thrown because cluster-b's namenode was unreachable. Listing at 07:35,
long after the peer was healthy:

```
465348  07:19  out_progress/1784963912609_regionserver-0...,16020,1784958326773_e7ea462f-..._1784963949004.plog
972948  07:20  out_progress/1784963940001_regionserver-0...,16020,1784958326773_5092ecb3-..._1784964009001.plog
972948  07:21  out_progress/1784964000000_regionserver-0...,16020,1784958326773_7dfc9df9-..._1784964069002.plog
972948  07:22  out_progress/1784964060002_regionserver-0...,16020,1784958326773_ae6464d5-..._1784964129003.plog
1034148 07:23  out_progress/1784964120002_regionserver-0...,16020,1784958326773_9b6483d8-..._1784964189004.plog
993648  07:24  out_progress/1784964180003_regionserver-0...,16020,1784958326773_d74dfb63-..._1784964249003.plog
950448  07:25  out_progress/1784964240004_regionserver-0...,16020,1784958326773_b8fc1007-..._1784964309006.plog
```

These are **real data files** (465 KB–1 MB), not the 48-byte header-only orphans
of the S7-orphan run.

**The dead window.** From the peer's return (07:24:59) the files were fully
copyable, but the forwarder walked past `out_progress/` round after round —
every live round logged `Number of new files ... 0` and the 5 % draw for the
in-progress pass never came up. The group held `ACTIVE_NOT_IN_SYNC` /
`DEGRADED_STANDBY`. Polls at 07:42, 07:43, 07:45, 07:46, 07:47, 07:48, 07:49,
07:50 all showed `out_progress=7`, `lastSweepStart=00:47:09` (i.e. **no
in-progress sweep had fired at all** in the interval).

**The draw finally comes up at 07:51:09** and drains all 7 in ~0.5 s — every
copy `took 11–425ms`, each to a **distinct shard** (054–060) with origin
identity preserved (the PHOENIX-7562 fix — no dst collision, no lease fight):

```
07:51:09,050 [...Forwarder-testHAGroup-0] ...ReplicationLogDiscoveryForwarder: Copying file src=.../out_progress/1784963940001_regionserver-0...,16020,1784958326773_57892911-..._1784965869030.plog dst=.../in/shard/055/1784963940001_regionserver-0...,16020,1784958326773.plog size=972948 took 18ms
07:51:09,051 [...Forwarder-testHAGroup-0] ...ReplicationLogTracker: Successfully deleted completed file: .../out_progress/1784963940001_..._57892911-..._1784965869030.plog
07:51:09,065 [...Forwarder-testHAGroup-0] ...ReplicationLogDiscoveryForwarder: Copying file src=.../out_progress/1784964180003_..._5777b861-... dst=.../in/shard/059/1784964180003_... size=993648 took 12ms
07:51:09,087 [...Forwarder-testHAGroup-0] ...ReplicationLogDiscoveryForwarder: Copying file src=.../out_progress/1784964120002_..._0e5567c1-... dst=.../in/shard/058/1784964120002_... size=1034148 took 19ms
07:51:09,515 [...Forwarder-testHAGroup-0] ...ReplicationLogDiscoveryForwarder: Copying file src=.../out_progress/1784964060002_..._99da0b88-... dst=.../in/shard/057/1784964060002_... size=972948 took 425ms
07:51:09,531 [...Forwarder-testHAGroup-0] ...ReplicationLogDiscoveryForwarder: Copying file src=.../out_progress/1784964240004_..._adaa14e0-... dst=.../in/shard/060/1784964240004_... size=950448 took 14ms
07:51:09,544 [...Forwarder-testHAGroup-0] ...ReplicationLogDiscoveryForwarder: Copying file src=.../out_progress/1784963912609_..._cc9e4b90-... dst=.../in/shard/054/1784963912609_... size=465348 took 11ms
07:51:09,564 [...Forwarder-testHAGroup-0] ...ReplicationLogDiscoveryForwarder: Copying file src=.../out_progress/1784964000000_..._fab3eefa-... dst=.../in/shard/056/1784964000000_... size=972948 took 17ms
07:51:09,566 [...Forwarder-testHAGroup-0] ...ReplicationLogDiscoveryForwarder: Processed all the replication log files for testHAGroup
07:51:09,569 [...Forwarder-testHAGroup-0] ...ReplicationLogDiscoveryForwarder: HAGroup testHAGroup updated HA state to SYNC
07:51:09,570 [ReplicationLogGroup-testHAGroup-0] ...SyncModeImpl: HAGroup testHAGroup entered mode SYNC
```

## The numbers

| Quantity | Value | Source / note |
|---|---:|---|
| Peer HDFS restored | 07:24:59 | files copyable from here on |
| In-progress sweep fires | 07:51:09 | first successful 5 % draw |
| **Stranded-after-healthy latency** | **~26 min** | 07:24:59 → 07:51:09, pure lottery |
| Oldest file total strand | ~32 min | renamed 07:19 → drained 07:51:09 |
| Actual drain of all 7 files | **~0.5 s** | 07:51:09,050 → 07:51:09,566 |
| Expected wait at p = 0.05 | ~20 rounds ≈ ~20 min | geometric mean 1/p, 60 s rounds |
| Expected wait at p = 0.50 | ~2 rounds ≈ ~1–2 min | geometric mean 1/p, 60 s rounds |

The recovery time of the HA group was dominated **entirely** by the retry
lottery, not by the outage (~6.5 min) and not by the drain (~0.5 s).

## Recommendation

Raise the forwarder's in-progress processing probability:

```
phoenix.replication.forwarder.in.progress.processing.probability = 50.0
```

(key: `REPLICATION_FORWARDER_IN_PROGRESS_PROCESSING_PROBABILITY_KEY`,
`ReplicationLogDiscoveryForwarder.java:57`; default `5.0`.)

Rationale:

- Expected wait for a stranded file drops from ~20 rounds to ~2 rounds — SYNC
  re-entry then tracks the *actual* drain (seconds) instead of the lottery
  (~20 min).
- It is a **forwarder-specific** key on purpose. The forwarder is the side that
  *creates* these orphans (rename-then-copy-then-maybe-fail); the standby replay
  reader does not share that failure mode, so its own in-progress probability can
  stay low.
- The cost is bounded: `processInProgressDirectory` does a `listStatus` of
  `out_progress/` (cheap when empty) and only re-copies when there is actually a
  backlog. At 50 % you pay the list about half the rounds; the expensive
  re-copy work only materializes when there is something stranded — which is
  exactly when you want the sweep aggressive.

## What this does and does not fix

- **Fixes:** latency after a *transient* copy failure (peer briefly unreachable,
  as in S7-mtime). The file is copyable; it just needs to be looked at sooner.
- **Does not fix:** a *deterministic* copy failure. If a file's copy fails every
  time (e.g. the historical dst-collision wedge — see the S7-orphan analysis and
  PHOENIX-7562), raising the probability only makes it churn and fail more often,
  faster. The probability knob is the wrong lever for persistent failures.

## Deeper design options (beyond the knob)

The knob is a mitigation; the underlying smell is that a failed forward
demotes a file into a directory that is only swept probabilistically. Options
worth considering:

1. **Deterministic backoff instead of a probability.** Track a per-file
   next-retry time and sweep `out_progress/` whenever a file is due, rather than
   flipping a coin every round. Removes the geometric tail entirely.
2. **Sweep `out_progress/` unconditionally when it is non-empty.** The list is
   cheap; the whole point of the probability is to *avoid* scanning an empty
   directory every round. Gate the probability on "is `out_progress/` empty?"
   instead of applying it blindly.
3. **Don't rename before the copy succeeds** (stage in place / copy first, move
   on success). This removes the "invisible to the every-round pass" property
   that makes stranding possible at all — though it changes the crash-recovery
   story, so it needs care.

## Caveats

- The **round arithmetic** (5 % → ~20 rounds, 60 s rounds) is config-driven and
  cluster-independent — it reproduces anywhere with these defaults.
- The **absolute copy times** (11–425 ms for 465 KB–1 MB) are kind-specific
  (single-node HDFS). On real hardware the drain differs, but it is still orders
  of magnitude below the lottery latency, so the *shape* of the problem — a
  seconds-long drain gated behind a ~20-minute expected wait — does not depend on
  the environment.
- Data safety was never at risk in this run: 787 load rounds all `rc=0`, RPO
  zero, and `validate-replication` PASSed byte-for-byte (480040 rows) once the
  files drained. The defect is **recovery latency / misleading DEGRADED
  duration**, not data loss.
