# Forwarder Shared-Queue Starvation: a Co-Active RegionServer Can Never Reconverge from STORE_AND_FORWARD to SYNC_AND_FORWARD

**Status:** **FIXED** in commit `e9226c93c9` ("Gate forwarder mode promotion on
in-progress-empty, not the shared next-round scan", 2026-08-08), and **VERIFIED
on the test-bed** the same day by deploying the fixed jar
(`phoenix-server-5.3.0-consistent_failover-14.1.11.jar`, md5 `bc8d5017…`) against
the exact preserved wedge. Root-caused on the local kind two-cluster DR test-bed
(2026-08-08, group `toxiHAGroup`, 3 co-active cluster-a RegionServers). This is a
**liveness / fairness defect** in the multi-forwarder design, not a data-loss
defect — there is no RPO event from this path. It surfaced while investigating
S17 (a wedged HA pair that would not leave `ACTIVE_NOT_IN_SYNC`). See "The fix
(shipped)" and "Fix verified on the test-bed" at the end.

**Companion to:** `HA_Status_CAS_Stale_Cache_BadVersion.md` (the *other* S17
finding — the fatal convergent-CAS abort), the S17 sections of
`Phoenix_HA_Failover_Test_Scenarios.md`, `S7_Forwarder_InProgress_Retry_Latency.md`,
and `handling-exceptions-saf.md`.

---

## TL;DR

When several co-active RegionServers share one HA group, they all run a
`ReplicationLogDiscoveryForwarder` that scans the **same** `out/` queue on the
active cluster's HDFS and claims files with an atomic `out → out_progress`
rename. A file is claimed by whichever forwarder renames it first.

An RS in `STORE_AND_FORWARD` has only two ways to promote itself back to
`SYNC_AND_FORWARD`:

1. **After a successful copy** — `processFile` promotes if the RS is still in
   `STORE_AND_FORWARD` and copy throughput cleared the threshold.
2. **After a fully-drained scan** — `processNoMoreRoundsLeft` self-promotes
   *unconditionally*, but only when both the in-progress directory **and** the
   next round's new-file scan come back empty.

A single lagging RS can have **both** paths closed at once:

- Its peers (already in `SYNC_AND_FORWARD`) write their own data straight to the
  standby `in/` and put nothing in the shared `out/`. So the shared `out/` queue
  contains **only the laggard's own files**, and those peers act as pure
  *consumers* of it. Whenever the laggard wakes even slightly later than a peer,
  the peer has already claimed and drained the laggard's files → the laggard's
  `listStatus` returns empty → `processFile` never runs → **path 1 never fires.**
- The laggard's own live rotation writer (a fresh empty `.plog` minted every
  round even under zero client load) always sits in the shard that
  `getNextRound(lastRoundProcessed)` points at, so
  `getNewFilesForRound(nextRound)` is never empty → the caught-up guard in
  `processNoMoreRoundsLeft` never passes → **path 2 never fires.**

Both roads to `SYNC_AND_FORWARD` are structurally closed. The RS stays in
`STORE_AND_FORWARD` forever, the shared group status stays `ACTIVE_NOT_IN_SYNC`,
and the pair never reconverges — even though the fault that first triggered the
demotion is long gone and the standby is fully caught up.

**Observed:** across the wedged run, copy counts were **rs-0 = 103, rs-1 = 57,
rs-2 = 0**. rs-2 (in `STORE_AND_FORWARD`) claimed **zero of ~160** files; every
one of its own files was drained by rs-0/rs-1 before rs-2's forwarder listed the
directory. rs-2 never crashed (restart count 0); it simply never promoted.

---

## Background: the pieces involved

### The shared `out/` queue and claim-by-rename

On the active cluster, all replication log files awaiting forwarding live under
`/phoenixHA/{group}/out/shard/NNN`. Every co-active RS runs a
`ReplicationLogDiscoveryForwarder` whose tracker points at that **same** OUT
directory (`ReplicationLogDiscoveryForwarder.createLogTracker` uses
`logGroup.getLocalShardManager()`). There is no per-RS partitioning of the queue
— all forwarders scan the whole thing.

A forwarder claims a file by **marking it in progress**: an atomic rename from
`out/` into `out_progress/` (`ReplicationLogTracker.markInProgress`, driven from
`ReplicationLogDiscovery.processOneRandomFile`). The rename is the mutual-exclusion
point — whoever renames first owns the file; everyone else re-lists and no longer
sees it. This is correct for at-most-once forwarding, but it is **claim-by-first**
with no fairness and no anti-starvation.

### The two promotion paths (STORE_AND_FORWARD → SYNC_AND_FORWARD)

`ReplicationLogDiscoveryForwarder`:

**Path 1 — after a successful copy** (`processFile`, ~lines 148-154):

```java
// after "Copying file src=... dst=... size=... took {}ms"
if (logGroup.getMode() == ReplicationMode.STORE_AND_FORWARD
    && isLogCopyThroughputAboveThreshold(srcStat.getLen(), copyTime)) {
  logGroup.checkAndSetModeAndNotify(
      ReplicationMode.STORE_AND_FORWARD, ReplicationMode.SYNC_AND_FORWARD);
}
```

This can only fire if the RS actually copies a file. If it never wins a claim, it
never reaches here.

**Path 2 — after a fully-drained scan** (`processNoMoreRoundsLeft`, ~lines 157-187):

```java
if (replicationLogTracker.getInProgressFiles().isEmpty()
    && replicationLogTracker.getNewFilesForRound(
         replicationLogTracker.getReplicationShardDirectoryManager()
           .getNextRound(getLastRoundProcessed())).isEmpty()) {
  LOG.info("Processed all the replication log files for {}", logGroup);
  // if this RS is still in STORE_AND_FORWARD mode like when it didn't process
  // any file, move this RS to SYNC_AND_FORWARD
  logGroup.checkAndSetModeAndNotify(
      ReplicationMode.STORE_AND_FORWARD, ReplicationMode.SYNC_AND_FORWARD);  // unconditional self-promote
  ...
}
```

This is the deliberate handling for "I never processed any file" — the
unconditional self-promote. But it is **nested inside the caught-up guard**: it
only runs when the in-progress dir is empty *and* the next round has no new files.

### The round scheduler

`ReplicationLogDiscovery` schedules `replay()` on a `scheduleAtFixedRate` timer.
The initial delay is grid-aligned so all RSes are *intended* to wake at the same
wall-clock instant (`computeAlignedInitialDelay`, plus a 1s `ELIGIBILITY_OFFSET_MS`
nudge past the boundary). With defaults `roundTimeMills = 60000` and
`bufferMillis = 15% × 60000 = 9000`, the eligible grid is `:09 + 1s = :10` each
minute. `getNextRoundToProcess` enforces a deliberate one-round + buffer lag
before a round becomes eligible.

### Rotation mints a file every round, even idle

Log rotation mints a new empty writer (6-byte header, `recordCount = 0`) every
round regardless of client load, and that writer stays `OPENFORWRITE`
(undrainable) until the *next* rotation. So a live RS **always** has a current
file sitting in the shard that its own `getNextRound(lastRoundProcessed)` selects.

---

## How the two paths get closed simultaneously

Consider a group where RS-lag (call it `rs-2`) is in `STORE_AND_FORWARD` and its
peers `rs-0`/`rs-1` are in `SYNC_AND_FORWARD`:

1. **`rs-0`/`rs-1` produce nothing into `out/`.** In `SYNC_AND_FORWARD` they
   append their own data directly to the standby `in/`. The only producer into
   the shared `out/` is `rs-2` itself (its idle rotation files, once rotated
   closed and eligible).

2. **`rs-0`/`rs-1` consume `rs-2`'s files.** Their forwarders scan the same
   `out/`, and whichever wakes first claims `rs-2`'s file via the atomic
   `out → out_progress` rename and copies it to the standby. This is why the copy
   counts are lopsided toward the peers and **zero** for the producer.

3. **Path 1 closed for `rs-2`.** By the time `rs-2`'s forwarder lists
   `out/shard/N`, its file is already in `out_progress` (claimed by a peer). It
   logs `Number of new files for round ... is 0`, never calls `processFile`, never
   reaches the post-copy promotion.

4. **Path 2 closed for `rs-2`.** `rs-2`'s own live rotation writer occupies the
   shard `getNextRound(lastRoundProcessed)` points at, so
   `getNewFilesForRound(nextRound)` is non-empty. The caught-up guard in
   `processNoMoreRoundsLeft` fails, so the unconditional self-promote on the line
   inside it is unreachable.

Net: `rs-2` can neither promote-by-copy nor promote-by-drained-scan. It is pinned
in `STORE_AND_FORWARD`. Because the shared group status only leaves
`ACTIVE_NOT_IN_SYNC` when **all** co-active RSes are back in a sync mode, one
pinned RS wedges the whole group.

## Why "it's just a race, rs-2 will win sometimes" is wrong

A pure listing race would hand `rs-2` roughly its share of the files over time.
The observed **0 / ~160** is not a race outcome — it is deterministic given any
*persistent* wake-phase skew between the forwarders:

- The grid intends all forwarders to wake at the same instant, but `fixedRate`
  timers pin whatever sub-second phase each RS was seeded with at scheduler start
  (and re-pin it after any long stall — see the test-bed artifact note below).
- The claim is a single ~1-2 ms NameNode rename RPC. So even a **few-millisecond**
  head start is enough for the earlier waker to sweep the queue clean before the
  laggard's `listStatus` returns.
- With `rs-2` the *sole producer* and `rs-0`/`rs-1` pure *consumers*, a consumer
  that is consistently even slightly earlier drains every file, every round.

The starvation is therefore a property of **shared-queue + claim-by-first +
no-fairness**, and any durable phase skew (however it arises) makes it total, not
partial.

## Impact

- **Liveness / availability, not durability.** No acknowledged write is lost — the
  standby stays caught up via the peers' forwarding, and `rs-2`'s own rotation
  files are empty headers. The cost is that the HA pair **never reconverges**:
  the group is stuck in `ACTIVE_NOT_IN_SYNC` indefinitely after a transient fault
  that has fully cleared, and it will not return to `ACTIVE_IN_SYNC` / provide a
  clean failover posture without operator intervention (recycle the pinned RS).
- **Amplified by having more co-active RSes.** The more peers already in
  `SYNC_AND_FORWARD`, the more reliably a laggard is out-claimed.
- It composes badly with `HA_Status_CAS_Stale_Cache_BadVersion.md`: a single
  peer-degrade can abort N−1 co-active RSes (that finding) and then strand the
  survivor in `STORE_AND_FORWARD` (this finding).

## Recommended fix: split the guard — mode flip on `in-progress empty`, status flip on the full caught-up check

The root cause is that `processNoMoreRoundsLeft` gates **two different decisions**
on **one** guard:

```java
if (inProgress.isEmpty() && getNewFilesForRound(nextRound).isEmpty()) {
  checkAndSetModeAndNotify(STORE_AND_FORWARD, SYNC_AND_FORWARD); // (A) this RS's own mode
  setHAGroupStatusToSync();                                      // (B) the shared group znode
}
```

These are not the same statement:

- **(A) the mode flip** asserts only "*this RS* can write directly to the peer
  now." It writes nothing to ZK, and it is inherently self-validating:
  `SyncAndForwardModeImpl.onEnter` has to reach the peer to build its log, so an
  optimistic promotion against a dead peer simply bounces back to
  `STORE_AND_FORWARD` — it cannot leave the RS falsely in a sync mode.
- **(B) the status flip** advertises `ACTIVE_IN_SYNC` on the shared znode — a
  cluster-wide claim that the standby is caught up and failover is clean. That
  legitimately requires the queue actually drained.

The fix is to **evaluate the in-progress check once and gate the two decisions
separately**:

```java
protected void processNoMoreRoundsLeft() throws IOException {
  boolean inProgressEmpty = replicationLogTracker.getInProgressFiles().isEmpty();
  // (A) An RS that has no claimed-but-stuck files has a healthy forward path;
  //     promote its own mode regardless of what is sitting in the next round's
  //     shard (which is polluted by every co-active RS's live rotation writer).
  if (inProgressEmpty) {
    logGroup.checkAndSetModeAndNotify(STORE_AND_FORWARD, SYNC_AND_FORWARD);
  }
  // (B) The shared in-sync claim still requires the full caught-up guard.
  if (
    inProgressEmpty && replicationLogTracker.getNewFilesForRound(replicationLogTracker
      .getReplicationShardDirectoryManager().getNextRound(getLastRoundProcessed())).isEmpty()
  ) {
    LOG.info("Processed all the replication log files for {}", logGroup);
    if (syncUpdateTS <= EnvironmentEdgeManager.currentTimeMillis()) {
      // ... existing setHAGroupStatusToSync() / syncUpdateTS bookkeeping unchanged
    }
  }
}
```

### Why the split works where the earlier ideas failed

The key asymmetry: **`in-progress-empty` is a clean per-RS signal;
`new-files-empty` is polluted.** The in-progress dir holds only files that were
*claimed and are stuck failing to forward* (`markFailed` is a no-op —
`ReplicationLogTracker.markFailed` leaves the file in place). The next round's
shard, by contrast, contains **every** co-active RS's live `OPENFORWRITE` rotation
writer. Gating the mode flip on the in-progress check alone sidesteps exactly that
pollution.

Case analysis (all confirmed against source):

- **Single laggard, peer healthy** (the observed wedge): peers drain the
  laggard's files → its in-progress dir empties → **(A) promotes it.** On exit,
  SAF's `onExit → closeReplicationLog` closes the laggard's live `out/` writer and
  `SyncAndForwardModeImpl.onEnter` moves writes to the peer, so it stops minting
  `out/` files entirely → clause (B) drains a round later → group goes in-sync.
- **Multiple idle RSes, peer healthy** (the case that kills "exclude own files",
  see below): every idle RS has an empty in-progress dir, so **all promote** via
  (A). There is no dependence on the shared new-files scan, so no symmetric
  blocking.
- **Loaded, peer down:** claimed files pile up failing in the in-progress dir →
  in-progress **non-empty** → (A) blocked → no promote, **no flap.**
- **Idle, peer down:** in-progress empty → (A) attempts promotion → `onEnter`
  cannot reach the peer → mode bounces straight back to SAF. One cheap
  self-healing bounce per round, no client write involved; (B) never fires, so the
  status stays honestly `ACTIVE_NOT_IN_SYNC`.

## Alternatives considered and rejected

1. **Exclude the RS's own live `OPENFORWRITE` writer from `getNewFilesForRound`.**
   Fixes the single-laggard wedge, but **fails when multiple RSes are idle**: the
   shard directory is shared and keyed only by timestamp
   (`ReplicationShardDirectoryManager.getShardDirectory(fileTimestamp)` — no server
   component; files are distinguished by server name *within* the dir). So each
   idle RS excludes only its own writer and is still blocked by every peer's live
   writer → the wedge becomes total and symmetric. Making it robust would require
   excluding *all* live writers, which means probing each foreign file's
   writer-open state — fragile and racy.

2. **Promote (A) unconditionally in `processNoMoreRoundsLeft`.** Correct and safe
   from a data-loss standpoint, and immune to the multi-idle symmetric blocking.
   But in the **loaded, peer-down** case it promotes → `onEnter` fails → demotes,
   every round — a bounded but noisy flap plus a ZK status write per demote. The
   recommended split avoids this by keeping (A) behind `in-progress-empty`, which
   is exactly the peer-down backlog detector.

3. **Decouple mode from winning a claim via a health probe** (promote on "a recent
   forward copy succeeded" or a lightweight reachability probe to the peer `in/`).
   This is the general form of the recommended fix and would also eliminate the
   idle-peer-down bounce. Deferred as a larger change; the guard split reuses the
   in-progress dir as a zero-cost health signal already present in the code.

4. **Fairness / anti-starvation or partitioning the `out/` queue by producing RS.**
   Address the claim-race starvation (Path 1) directly rather than the promotion
   guard. Larger design changes; orthogonal to the reconvergence wedge, which the
   guard split closes on its own.

The recommended split is the narrowest change that closes **both** the
single-laggard and multi-idle wedges without introducing peer-down flapping, and
it preserves the honesty of the shared in-sync claim (B). It works because the
self-promote-when-idle path was clearly *intended* to handle the "didn't process
any file" case (see the in-code comment), and it was defeated only by coupling
that per-RS mode decision to a shared-queue scan polluted by every RS's idle
rotation writer.

## How it was observed (S17 test-bed run, 2026-08-08)

Group `toxiHAGroup`, three co-active cluster-a RegionServers. Status znode
`/phoenix/consistentHA/toxiHAGroup` = `ACTIVE_NOT_IN_SYNC`; standby
`b = DEGRADED_STANDBY`. Poll after poll showed no reconvergence.

- **Copy counts (literal `Copying file` log lines):** rs-0 = **103**, rs-1 = **57**,
  rs-2 = **0**. All of rs-0/rs-1's copied `src=` paths were
  `.../out_progress/<ts>_regionserver-2_...plog` — i.e. they were draining
  **rs-2's** files.
- **Direct proof of the closed path 1:** for round
  `{start=1786174680000, end=1786174740000}`, rs-0 copied `src=.../out_progress/
  1786174698132_regionserver-2_...plog` (ts 1786174698132 falls in that round),
  while rs-2's own forwarder logged, for the *same* round,
  `Number of new files for round ReplicationRound{startTime=1786174680000,
  endTime=1786174740000} is 0`. rs-2 found the queue already emptied.
- **rs-2 restart count 0** — it never aborted; last mode transition was the
  05:24:10 demotion, and `Processed all the replication log files` last fired at
  05:24:09,992. rs-0/rs-1 had restarted at 04:49 and promoted to
  `SYNC_AND_FORWARD` before the shared-queue asymmetry set in.
- iptables clean, DataNodes healthy, SAF forwarding working (data was reaching the
  standby) — confirming this is a pure reconvergence-liveness wedge, not a fault
  still in progress.

### Test-bed artifact worth calling out (so it is not mistaken for the bug)

While tracing *why* rs-2 was consistently the last waker, the forwarder wake phase
was found to shift abruptly from `:10` (dead-on the intended grid, for 67 minutes)
to `:28` at **05:56:28**. The cause was a **host suspend**: all three RSes logged
the *identical* `util.Sleeper: We slept 321029ms instead of 3000ms` at 05:55:43-45
(a 321 s freeze across three independent JVMs is a laptop sleeping, not GC). On
resume, each RS's `scheduleAtFixedRate` burst-fired its missed ticks and re-pinned
to a new sub-minute phase (rs-2 at `:28`, rs-0/rs-1 at `:38`), which is what
injected the durable ~13 ms wake skew between them in that capture.

**The suspend is a test-bed artifact and Phoenix's grid alignment is working
correctly** (rs-2 sat exactly on the `:10` grid the whole healthy run). The
artifact only *supplied* a persistent phase skew; the starvation defect is that
the shared-queue design converts *any* such skew — from a restart, a GC pause, a
suspend, or normal scheduler jitter — into total, permanent starvation of the
laggard, with no fairness or health-based promotion to recover. Do not "fix" the
suspend and consider this closed; the design gap stands on its own.

## Repro sketch (test-bed)

1. Fully-synced HA pair; active cluster has ≥2 co-active RegionServers holding live
   replication log groups for the same HA group (write through the HA connection so
   each RS has an eager group).
2. Trigger a transient peer-sync fault that demotes the group to
   `ACTIVE_NOT_IN_SYNC` (e.g. the S17 peer-DN `:9866` hang, or the S17b block-write
   stall), then clear the fault. Some RSes promote back to `SYNC_AND_FORWARD`;
   arrange for at least one to lag (a restart, or a scheduler-phase skew, is enough).
3. With the fault cleared, observe the lagging RS stay in `STORE_AND_FORWARD`
   indefinitely: `Copying file` count 0 for that RS, `Number of new files ... is 0`
   every round, and `Processed all the replication log files` never firing (its own
   rotation writer keeps the next-round scan non-empty).
4. Confirm the peers are draining that RS's files (their copied `src=` paths carry
   the laggard's `regionserver-N` identity) and that the group status never leaves
   `ACTIVE_NOT_IN_SYNC`.
5. Recycle the pinned RS to force reconvergence.

Pod-log time = host time + 7h.

## The fix (shipped)

Commit `e9226c93c9` — "Gate forwarder mode promotion on in-progress-empty, not
the shared next-round scan" — implements suggested fixes (1)+(2) together in
`ReplicationLogDiscoveryForwarder.processNoMoreRoundsLeft`. It **splits the
guard** into two levels:

- **The RS's own mode flip** (`STORE_AND_FORWARD → SYNC_AND_FORWARD`) is now gated
  *only* on an empty in-progress directory — the per-RS forward-health signal. It
  no longer scans the shared next-round shard, so the RS's own live
  `OPENFORWRITE` rotation writer can no longer keep it pinned.
- **The shared in-sync status claim** (`setHAGroupStatusToSync`) keeps the full
  caught-up guard (in-progress empty *and* next-round scan empty).

```java
protected void processNoMoreRoundsLeft() throws IOException {
  // A non-empty in-progress directory means this RS has claimed-but-stuck files,
  // so its forward path to the peer is unhealthy: neither promote nor claim in-sync.
  if (!replicationLogTracker.getInProgressFiles().isEmpty()) {
    return;
  }
  // Promote this RS's own mode on that signal alone. The flip is self-validating —
  // SyncAndForwardModeImpl.onEnter must reach the peer, so a bad promotion against
  // a dead peer bounces back to STORE_AND_FORWARD.
  logGroup.checkAndSetModeAndNotify(STORE_AND_FORWARD, SYNC_AND_FORWARD);

  // The shared in-sync claim additionally requires no new files for the ongoing round.
  if (!replicationLogTracker.getNewFilesForRound(...getNextRound(getLastRoundProcessed())).isEmpty()) {
    return;
  }
  LOG.info("Processed all the replication log files for {}", logGroup);
  ... setHAGroupStatusToSync() ...
}
```

The promotion being **self-validating** is what makes it safe to gate on the
weaker signal: `SyncAndForwardModeImpl.onEnter` must actually reach the peer HDFS,
so if the peer is still down an optimistic promotion simply bounces back to
`STORE_AND_FORWARD` — no risk of falsely claiming sync while the peer is
unreachable. The commit also adds a `@VisibleForTesting` tracker-injecting
constructor and three unit tests: the wedge-fix case, the peer-down backlog case
(no promotion), and the fully-caught-up case.

## Fix verified on the test-bed

Deployed `phoenix-server-5.3.0-consistent_failover-14.1.11.jar` (md5
`bc8d5017088d4eb7a0be43e842c724e3`, built 2026-08-08) into the unified image and
scaled the **preserved wedge fixture** back up — the status znode was still
`ACTIVE_NOT_IN_SYNC` / peer `DEGRADED_STANDBY`, `peerHdfsUrl` through toxiproxy
(confirmed pass-through, zero toxics), `/phoenixHA` intact from the wedged run.

Reconvergence happened on its own within ~100 s (about two forwarder round
cadences), no writes and no RS recycle needed:

```
ZK poll: a=ACTIVE_NOT_IN_SYNC b=DEGRADED_STANDBY   (polls 1-5)
ZK poll: a=ACTIVE_IN_SYNC     b=STANDBY            (poll 6)   <- reconverged, stable thereafter
```

All three cluster-a RSes ran the recovery chain in lockstep (host time 19:46:10),
including rs-2 which had been pinned at **0 copies** under the old jar:

```
rs-{0,1,2}: ReplicationLogDiscoveryForwarder: Processed all the replication log files for toxiHAGroup
rs-{0,1,2}: ReplicationLogGroup: conditionally switched from SYNC_AND_FORWARD to SYNC
rs-{0,1,2}: Mode switched at sequence 3 from SYNC_AND_FORWARD to SYNC
rs-{0,1,2}: SyncAndForwardModeImpl: exiting mode SYNC_AND_FORWARD graceful=true
```

That rs-2 promoted at all — despite still winning no shared-queue claims —
confirms the fix decouples an RS's own mode from winning the claim race: the flip
now rides the per-RS `getInProgressFiles().isEmpty()` health signal, reaches the
peer to self-validate, and the group advances all the way back to plain `SYNC`.