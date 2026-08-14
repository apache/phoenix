# Stale-cache `BadVersionException` on the shared HA-status CAS aborts co-active RegionServers

**Status:** confirmed finding, root-caused on the local kind DR test-bed
(2026-08-07). This is a general concurrency defect in the way co-active
RegionServers write the *shared* per-group HA status znode, not a fault specific
to any one scenario. It surfaced organically while investigating S17, but the
same pattern (`BadVersionException` from an optimistic CAS whose expected version
came from an asynchronously-updated local cache) recurs anywhere the HA record is
updated under contention.

Companion to [S20b](S20b_Failover_ZooKeeper_Down_No_Forced_Promotion.md) and to
the S17 investigation notes.

---

## One-line summary

When more than one co-active RegionServer reacts to the same peer-degrade at the
same time, each independently tries to move the **shared** HA-group status znode
`ACTIVE_IN_SYNC → ACTIVE_NOT_IN_SYNC` with an optimistic CAS. The expected
version for that CAS is read from the local Curator `PathChildrenCache`, which is
updated asynchronously by watch events. The first writer wins and bumps the znode
version; every other writer is still holding the **pre-bump cached version**, so
its CAS fails with `BadVersionException`. That failure is wrapped as a **fatal**
replication error and the RegionServer **aborts** — even though the shared status
is already at exactly the value it was trying to write. With _N_ co-active RSes
reacting to one event, the CAS winner survives and the other _N−1_ abort.

## The defect is convergent contention treated as conflict

`BadVersionException` on this CAS does **not** mean "someone wrote a value that
conflicts with mine." Every co-active RS is trying to write the *same* target
state (`ACTIVE_NOT_IN_SYNC` / STORE_AND_FORWARD). A `BadVersion` here means
precisely: **"a peer RegionServer already advanced the shared record to the state
I wanted."** The desired group-level outcome is already achieved. The correct
reaction is to re-read the record, observe it is already at (or past) the target,
treat the update as a no-op success, complete the RS's own local mode transition,
and continue. Instead the loser treats it as unrecoverable and aborts.

## Exact call chain (code-confirmed)

Trigger (any peer-degrade event; in the S17 run it was a local sync failure):

1. Some RS's replication log hits a sync/append error and moves its group
   `SYNC → STORE_AND_FORWARD`, which writes the shared status
   `ACTIVE_IN_SYNC → ACTIVE_NOT_IN_SYNC` and **wins** the CAS
   (znode version `v → v+1`).
2. Every co-active RS's local-cache watch fires the `ACTIVE_NOT_IN_SYNC`
   subscription
   (`ReplicationLogGroup.java:709`):
   ```java
   subscribeLocal(HAGroupState.ACTIVE_NOT_IN_SYNC, () -> {
     // When any RS drops to STORE_AND_FORWARD, others must leave SYNC.
     checkAndSetModeAndNotify(ReplicationMode.SYNC, ReplicationMode.SYNC_AND_FORWARD);
   });
   ```
   This does an in-memory mode flip and then calls `sync()` to notify its own
   disruptor (`checkAndSetModeAndNotify:735-746`). That self-notify sync event is
   what drives the disruptor into the transition machinery. (In the S17 run the
   co-active RS's SYNC-mode log was already closed, so its disruptor's `sync()`
   threw `IOException("Closed")` and routed through `SyncModeImpl.onFailure` —
   but any independent local sync failure gets there the same way.)
3. The disruptor's mode-transition path calls
   `ReplicationModeImpl.transitionToStoreAndForward()`
   (`ReplicationModeImpl.java:95-106`):
   ```java
   protected ReplicationMode transitionToStoreAndForward() throws IOException {
     logGroup.getMetrics().incrementSyncToSafTransitions();
     try {
       logGroup.setHAGroupStatusToStoreAndForward();
     } catch (Exception ex) {                         // <-- catches EVERYTHING, incl. stale-version
       String message = String.format(
         "HAGroup %s could not update status to STORE_AND_FORWARD", logGroup);
       LOG.error(message, ex);
       throw ReplicationLogGroup.asIOException(message, ex);   // <-- FATAL -> RS abort
     }
     return ReplicationMode.STORE_AND_FORWARD;
   }
   ```
4. `setHAGroupStatusToStoreAndForward` →
   `HAGroupStoreManager.setHAGroupStatusToStoreAndForward` →
   `HAGroupStoreClient.setHAGroupStatusIfNeeded` (`HAGroupStoreClient.java:407`).
   The expected version handed to the CAS comes from the **local cache**:
   ```java
   Pair<HAGroupStoreRecord, Stat> cacheRecord = fetchLocalRecordAndPopulateZKIfNeeded(); // :414
   Stat currentHAGroupStoreRecordStat = cacheRecord.getRight();
   ...
   phoenixHaAdmin.updateHAGroupStoreRecordInZooKeeper(
       haGroupName, newHAGroupStoreRecord,
       currentHAGroupStoreRecordStat.getVersion());   // :455-456  expected version = CACHED version
   ```
   `fetchLocalRecordAndPopulateZKIfNeeded` (`:988`) reads
   `HAGroupStoreCacheUtil.recordAndStatAt(pathChildrenCache, targetPath)` — i.e.
   the `Stat`/version comes straight from the Curator `PathChildrenCache`, **not**
   a fresh `getData` against ZK. The cache is updated asynchronously by watch
   delivery, so between step 1's winning write and this RS's watch actually
   landing, the cache still holds the **pre-bump** version.
5. The CAS in `PhoenixHAAdmin.updateHAGroupStoreRecordInZooKeeper`
   (`PhoenixHAAdmin.java:527-540`) runs
   `setData().withVersion(cachedVersion)` against a znode already at
   `cachedVersion+1`, so ZK throws `BadVersionException`, which is rethrown as
   `StaleHAGroupStoreRecordVersionException`:
   ```java
   } catch (KeeperException.BadVersionException e) {
     throw new StaleHAGroupStoreRecordVersionException(
       "Failed to set HAGroupStoreRecord for HA group " + haGroupName
         + " with cached stat version " + currentStatVersion, e);
   }
   ```
6. Back in step 3, `transitionToStoreAndForward`'s broad `catch (Exception ex)`
   turns that stale-version exception into the fatal
   `IOException: could not update status to STORE_AND_FORWARD` → the RegionServer
   aborts.

## Why the cached version is the wrong thing to CAS against

Optimistic locking needs the expected version to be **the version this writer
actually observed the znode at, freshly enough that a lost CAS means a genuine
concurrent modification it must reconcile with.** Here the version is read from a
watch-populated cache that is, by construction, behind the authoritative znode
under contention. So a lost CAS conflates two very different situations:

- **Genuine conflict** — someone wrote a *different*, incompatible value; the
  writer must re-read and re-decide. (Rare here; all co-active RSes want the same
  target.)
- **Convergent race** — someone already wrote the *same* value the loser wanted;
  nothing to reconcile, the goal is met. (This is the common case, and it is what
  the fatal path punishes with an abort.)

The status write is also **idempotent at the semantic level**: the group only
needs the shared status set to `ACTIVE_NOT_IN_SYNC` once, by anyone. Making each
co-active RS race to be the writer, then killing the losers, is the opposite of
what an idempotent, converge-to-target update should do.

Note `setHAGroupStatusIfNeeded` already has the right instinct one layer up: it
calls `validateTransitionAndGetWaitTime` (`:422`) and returns early
("Not updating…") when the transition isn't needed. But that check runs against
the **same stale cache**, so a loser still sees the old `ACTIVE_IN_SYNC` and
proceeds to CAS. A fresh read would have short-circuited it as already-done.

## Impact

- **Availability, not durability.** These are aborts on writes that were never
  acknowledged (the triggering sync failed), so there is no RPO event from this
  path itself. The cost is that a single peer-degrade can take out every
  co-active RS except the CAS winner — an availability amplification (one blip →
  _N−1_ RS aborts) exactly when the cluster is already degraded.
- The aborts are self-inflicted by the **correct** design reaction ("when any RS
  drops to STORE_AND_FORWARD, others must leave SYNC",
  `ReplicationLogGroup.java:711`). The reaction is right; the shared-status write
  underneath it is not concurrency-safe.

## Suggested fix

Make the shared-status update converge-to-target instead of fatal-on-stale.
Options, roughly in order of preference:

1. **Reconcile on stale version in the status-write path.** On
   `StaleHAGroupStoreRecordVersionException`, re-read the record *fresh from ZK*
   (not the cache); if it is already at — or past — the intended state, treat the
   update as a successful no-op and let the RS complete its local transition. Only
   propagate a failure if the fresh record is in a genuinely incompatible state.
   This is the narrowest fix and directly targets the convergent-race case.
2. **Bounded CAS retry with a fresh re-read** inside
   `updateHAGroupStoreRecordInZooKeeper` (or its caller): on `BadVersion`,
   re-`getData` for the current version, re-evaluate "is the update still needed?"
   and retry, giving up only after the record settles in a conflicting state.
3. **Do not treat a stale-version status write as fatal in
   `transitionToStoreAndForward`.** The RS can still move its *local* mode to
   STORE_AND_FORWARD (or SYNC_AND_FORWARD, per the watch) without owning the
   shared-status write; the shared status is a group-level fact that any one RS
   setting is sufficient for. Narrow the broad `catch (Exception)` so a
   convergent stale-version loss does not abort.

Fixes (1) and (2) also help every other caller of
`updateHAGroupStoreRecordInZooKeeper` that passes a cache-derived version, since
the stale-cache-vs-CAS pattern is not unique to the SAF transition.

## How it was observed (S17 test-bed run)

Three co-active cluster-a RegionServers (rs-0, rs-1, rs-2), group `toxiHAGroup`,
shared status znode at version 36:

- **rs-2** hit a local sync failure first, ran `transitionToStoreAndForward`,
  **won** the CAS `v36 → v37` (status → `ACTIVE_NOT_IN_SYNC`), moved to
  STORE_AND_FORWARD, re-inited its log, and **survived**.
- **rs-0** and **rs-1** received the resulting `ACTIVE_NOT_IN_SYNC` watch, did the
  in-memory `SYNC → SYNC_AND_FORWARD` flip, and their self-notify `sync()` hit
  their own log error → `transitionToStoreAndForward` → CAS **at the stale cached
  v36** → `BadVersionException` → `StaleHAGroupStoreRecordVersionException` →
  fatal `could not update status to STORE_AND_FORWARD` → **both aborted**.

Log evidence (host time + 7h; all within the same ~20 ms window at
`04:48:22,8xx`):

```
rs-2:  SyncModeImpl: HAGroup toxiHAGroup mode=SYNC got error (IOException: Closed)
rs-2:  ReplicationLogGroup: HAGroup toxiHAGroup switched from SYNC to STORE_AND_FORWARD   <- CAS winner
rs-0:  ReplicationLogGroup: conditionally switched from SYNC to SYNC_AND_FORWARD           <- in-memory flip
rs-0:  ReplicationModeImpl: HAGroup toxiHAGroup could not update status to STORE_AND_FORWARD
       Caused by: StaleHAGroupStoreRecordVersionException: ... with cached stat version 36  <- CAS loser -> abort
rs-1:  (identical to rs-0)
```

The specific way rs-0/rs-1's logs came to be closed in that run (a latent 5/5
rotation-failure `close(false)` from an unrelated NameNode-RPC outage) was a
test-bed artifact and is **not** part of this defect — it merely supplied the
local sync failure. Any independent local sync failure on ≥2 co-active RSes near
a shared-status change reproduces the CAS-loss abort. This is the S8
"peer state transition race" reproduced organically.

## Repro sketch (test-bed) — DELIBERATELY REPRODUCED 2026-08-08

The original 2026-08-07 observation was organic (during S17). It was then
reproduced **on purpose** on 2026-08-08 with the recipe below — the key
realisation being that the defect needs **≥2 RegionServers each holding a live
SYNC-mode writer for the group, failing near-simultaneously**. A single-region
table only ever gives *one* RS a live writer, so the other RSes take the clean
watch-driven `SYNC → SYNC_AND_FORWARD` flip (which does **not** CAS) and no race
occurs. The fix is to fan the write path across ≥2 RSes.

1. Fully-synced pair. **Spread the target table across ≥2 RegionServers** so more
   than one RS gets a live replication log group:
   ```
   split "PHOENIX_HA_T", "m"                 # 2 regions
   move "<region1-encoded>", "<rs-0 servername>"   # ,,→m  on rs-0
   move "<region2-encoded>", "<rs-1 servername>"   # m,→   on rs-1
   ```
2. Drive **two** HA-connection load loops with row-key prefixes on opposite sides
   of the split point (`aaa-*` < `m`, `zzz-*` ≥ `m`) so **both** RSes hold an
   eager, actively-syncing `ReplicationLogGroup` (confirm two distinct
   `...regionserver-N...plog` files landing in the peer `in/` shard).
3. **Simultaneously** break the RS→peer-DataNode `:9866` pipeline on *both* RSes —
   e.g. insert `iptables -I FORWARD 1 -s <rsIP> -d <peerDN> -p tcp --dport 9866 -j
   DROP` for every (rs, peer-DN) pair at the **top** of FORWARD (above kind's
   `KUBE-FORWARD` ESTABLISHED-ACCEPT) and flush conntrack. Both writers' next sync
   fails at nearly the same instant.
4. Observe exactly one RS win the `ACTIVE_IN_SYNC → ACTIVE_NOT_IN_SYNC` status CAS
   and survive; a co-active RS that receives the winner's `ACTIVE_NOT_IN_SYNC`
   watch **while its own writer is throwing** runs `transitionToStoreAndForward`
   inside the cache-lag window, CASes at the stale cached version, and aborts with
   `could not update status to STORE_AND_FORWARD` caused by
   `StaleHAGroupStoreRecordVersionException ... with cached stat version N` /
   `BadVersionException`.

**Correction to the earlier belief that "a DROP always degrades to graceful SAF"
(true for the single-writer S17 abort question):** with **multiple** co-active
writers, a simultaneous DROP *does* reproduce this abort, because the winner's
status flip races the losers' own in-flight sync failures. The "DROP → SAF, never
an app-thread abort" rule is specific to the S17 `syncFuture` timeout (a single
region host); it does not apply to this multi-writer convergent-CAS path.

### Confirmed reproduction log (2026-08-08, `testHAGroup`, rs-2 `10.244.2.43`, previous container)

All inside a single millisecond window — the textbook loser abort:

```
06:39:03,882 [Curator-PathChildrenCache-0] Detected state transition testHAGroup ACTIVE_IN_SYNC → ACTIVE_NOT_IN_SYNC   (a peer RS already WON the CAS)
06:39:03,882 [Curator-PathChildrenCache-0] conditionally switched from SYNC to SYNC_AND_FORWARD                         (in-memory watch flip)
06:39:03,882 [ReplicationLogGroup-testHAGroup-0] Failed to process SYNC event at sequence 91 ... mode=SYNC got error     (its OWN writer broken by the DROP)
06:39:03,883 [ReplicationLogGroup-testHAGroup-0] PhoenixHAAdmin: Failed to set HAGroupStoreRecord ... stale stat version (CAS at stale cached v245)
06:39:03,884 [ReplicationLogGroup-testHAGroup-0] could not update status to STORE_AND_FORWARD → event handler hit fatal exception
06:39:03,885 [Curator-PathChildrenCache-0] ***** ABORTING region server 10.244.2.43,16020: HAGroup testHAGroup sync operation failed *****
  Caused by: StaleHAGroupStoreRecordVersionException: ... with cached stat version 245
  Caused by: KeeperException$BadVersionException: BadVersion for /phoenix/consistentHA/testHAGroup
```

`rs-0` logged the identical `StaleHAGroupStoreRecordVersionException ... cached
stat version 294 / 296` + `BadVersion` on `toxiHAGroup` in the same run. After the
aborts the RSes restarted and both groups self-reconverged to `ACTIVE_IN_SYNC`
(availability-only, no RPO — the triggering syncs were never acked). The abort
firing on the **`Curator-PathChildrenCache-0` thread** is the decisive proof this
is the convergent-race path, not a genuine conflict.

Pod-log time = host time + 7h.