# S20e — forced promotion leaves the promoted group's replay stuck in `DEGRADED` forever

**Status:** confirmed finding, reproduced on the local kind DR test-bed
(2026-08-12). A follow-on to the S20b forced-promotion recovery: after a survivor
is force-promoted with `update … -F` (the only surface available when the active
cluster's ZK is unrecoverable), the promoted cluster keeps running the standby
replay loop for that group, and that loop stays latched in its internal
`DEGRADED` state indefinitely. `lastRoundProcessed` tracks wall-clock while
`lastRoundInSync` — and therefore the replay consistency point that gates
compaction retention — is frozen at the instant of the outage. Nothing self-heals
it because the listeners that would reset the replay state only fire on the
*cooperative* STANDBY→STANDBY_TO_ACTIVE / STANDBY recovery transitions, which
`update -F` bypasses.

Companion to [S20b](S20b_Failover_ZooKeeper_Down_No_Forced_Promotion.md)
(the fence-then-force-promote recovery this scenario continues) and to
[S20c](S20c_Failover_HMaster_Down_SystemTable_Stall.md).

---

## What was observed

Continuing directly from an S20b-style recovery on group `toxiHAGroup`:

1. Active cluster-a's ZooKeeper was down; cluster-b (STANDBY) was force-promoted
   to `ACTIVE_IN_SYNC` with `update -g toxiHAGroup -s ACTIVE_IN_SYNC -av -F`
   against its own healthy ZK. The data plane worked — HA-connection writes
   committed on cluster-b via the url2 fallback.
2. cluster-a's ZK was later restored and cluster-a was force-demoted to `STANDBY`
   (`update -s STANDBY -av -F`). The pair became single-active again:
   cluster-b `ACTIVE_IN_SYNC` (v3), cluster-a `STANDBY` (v2), and cluster-b→
   cluster-a replication verified end-to-end.

With the pair nominally healthy, cluster-b's RegionServers log a continuous
stream, once per replay round:

```
reader.ReplicationLogDiscoveryReplay: Processed round ReplicationRound{startTime=1786573500000, endTime=1786573560000}
  successfully with cluster in DEGRADED state,
  lastRoundProcessed=ReplicationRound{startTime=1786573500000, endTime=1786573560000},
  lastRoundInSync=ReplicationRound{startTime=1786565700000, endTime=1786565760000}
```

`lastRoundProcessed` advances to within ~20 s of wall-clock (it drained a ~2 h
backlog of empty rounds in ~3 min), but `lastRoundInSync` is pinned at
`1786565760000` — roughly the last round cluster-b received while it was still a
healthy standby, i.e. the moment of the ZK outage. It never moves.

### The DEGRADED group is exactly the force-promoted one

Both HA groups on cluster-b (`testHAGroup`, `toxiHAGroup`) share the single
`Phoenix-ReplicationLogDiscoveryReplay-0` thread, and the "Processed round …
DEGRADED" line does **not** name the group. Pairing each status line with its
preceding `Starting to process round … for haGroup: X` line disambiguates them:

```
START group=toxiHAGroup   → PROCESSED … => DEGRADED     (every round)
START group=testHAGroup   → PROCESSED … => SYNC          (healthy)
```

Only `toxiHAGroup` — the group that went through the peer-blind force-promotion —
is stuck. `testHAGroup`, for which cluster-b remained a normal peer-visible
standby throughout, is healthy (`SYNC`, both watermarks advancing in lockstep).
That contrast is the whole diagnosis.

## Why the replay loop runs on an ACTIVE cluster at all

The replay service is **not** gated on HA role. It is started once by the
RegionServer coprocessor and stopped only on RegionServer shutdown:

- `PhoenixRegionServerEndpoint.start(...)` →
  `ReplicationLogReplayService.getInstance(conf).start()`
  (`PhoenixRegionServerEndpoint.java:106`).
- `PhoenixRegionServerEndpoint.stop(...)` →
  `ReplicationLogReplayService.getInstance(conf).stop()`
  (`PhoenixRegionServerEndpoint.java:112`).

The only on/off switch is the global config flag
`phoenix.replication.replay.enabled` (`PHOENIX_REPLICATION_REPLAY_ENABLED`,
default `false`; `true` in this test-bed), checked at
`ReplicationLogReplayService.java:175-181`. When enabled, the service loops over
**all** HA groups returned by `HAGroupStoreManager.getHAGroupNames()` with no
per-role filter (`ReplicationLogReplayService.java:255-261, 320-323`) and
schedules a periodic `replay()` per group
(`ReplicationLogDiscovery.java:140-164`).

**Nothing stops the loop when a cluster becomes ACTIVE.** The only stop paths are
RegionServer shutdown and an explicit `ReplicationLogDiscovery.stop()`
(`:171-197`); no HA-state listener tears the loop down on promotion. So a
promoted cluster keeps running the standby replay for every group — expected, per
today's design, but see the defect below.

## What `DEGRADED` means here and why it latches

The `DEGRADED` in the log is the replay state machine's own enum, **not** the ZK
record state (`ReplicationLogDiscoveryReplay.ReplicationReplayState`,
`ReplicationLogDiscoveryReplay.java:679-684`):

```java
public enum ReplicationReplayState {
  NOT_INITIALIZED,
  SYNC,             // fully in sync / standby
  DEGRADED,         // degraded for writer
  SYNCED_RECOVERY   // came back from degraded → standby, needs rewind
}
```

It is entered by the LOCAL-state listener when the effective local state becomes
`DEGRADED_STANDBY` (`ReplicationLogDiscoveryReplay.java:128-145`), or at init if
the effective state is already `DEGRADED_STANDBY` (`:272-275`). `DEGRADED_STANDBY`
here is the **peer-blind overlay**: a local `STANDBY` that cannot see its peer is
presented as `DEGRADED_STANDBY` (fail-closed), never persisted to ZK
(`HAGroupStoreClient.effectiveLocalState:1017-1021`). That is precisely cluster-b's
condition during the incident — STANDBY + peer ZK down ⇒ effective
`DEGRADED_STANDBY` ⇒ replay CAS'd into `DEGRADED`.

The two watermarks are gated by the `replay()` state switch:

- **SYNC** — both advance (`ReplicationLogDiscoveryReplay.java:456-463`):
  `setLastRoundProcessed(round); setLastRoundInSync(round);`
- **DEGRADED** — only `lastRoundProcessed` advances; `lastRoundInSync` is
  deliberately preserved (`:465-472`):
  ```java
  case DEGRADED:
    // Only update last round processed, and NOT last round in sync
    setLastRoundProcessed(replicationRound);
    LOG.info("Processed round {} successfully with cluster in DEGRADED "
      + "state, lastRoundProcessed={}, lastRoundInSync={}", ...);
    break;
  ```

Escaping `DEGRADED` requires a CAS `DEGRADED → SYNCED_RECOVERY` fired by a LOCAL
transition to either `STANDBY` (recovery listener,
`ReplicationLogDiscoveryReplay.java:147-166`) or `STANDBY_TO_ACTIVE` (failover
listener, `:168-193`), followed by the `SYNCED_RECOVERY` branch rewinding
`lastRoundProcessed` back to `lastRoundInSync` and CAS-ing
`SYNCED_RECOVERY → SYNC` (`:443-454`).

## The defect

**A force-promotion straight to `ACTIVE_IN_SYNC` fires none of the listeners that
reset replay state.** The cooperative failover path
(`STANDBY → STANDBY_TO_ACTIVE → ACTIVE_IN_SYNC`) is what arms
`triggerFailoverListner` / `recoveryListener` and drives `DEGRADED →
SYNCED_RECOVERY → SYNC`. `update -F` jumps directly to `ACTIVE_IN_SYNC`, and there
is **no listener for a transition *into* `ACTIVE_IN_SYNC`** that resets the replay
state or stops the loop. So the group's replay is left permanently in `DEGRADED`:

- `shouldTriggerFailover()` is blocked unless the state is `SYNC`
  (`ReplicationLogDiscoveryReplay.java:628-634`), so the loop cannot self-heal;
  and on an `ACTIVE_IN_SYNC` cluster `failoverPending` is never armed anyway.
- The replay-derived consistency point is `lastRoundInSync.getEndTime()` in the
  DEGRADED/SYNCED_RECOVERY branch (`getConsistencyPoint():729-739`), so a frozen
  `lastRoundInSync` freezes the consistency point that feeds the compaction
  retention guard (`ReplicationLogReplayService.resolveConsistencyPoint:300-318`).
  A frozen/low value means delete-marker retention is held back at the outage
  instant — compaction cannot advance past it.

This is the replay-layer analogue of the S20b/split-brain gap: just as nothing
reconciles the ZK *record* after an unfenced force-promotion, nothing reconciles
the replay *state machine*. The cooperative machinery that would have cleaned both
up is exactly what `update -F` bypasses.

## Impact

- **Data plane: none.** After promotion the promoted cluster's own `in/` shard
  receives no new data (its former peer is now the standby, replicating the other
  direction), so every DEGRADED round it processes is empty (`recordCount=0`). No
  writes are lost or blocked; real writes land on the active and replicate to the
  new standby normally.
- **Latent, real:** the frozen replay consistency point holds back compaction
  retention for the affected group on the promoted cluster indefinitely. Benign on
  a short-lived test-bed; on a long-running production cluster it is an unbounded
  retention hold that only clears on a RegionServer restart or a genuine
  role-transition through the cooperative path.

## Workaround

Recycle the promoted cluster's RegionServers. On restart the replay
re-initializes against the *current* effective state — now `ACTIVE_IN_SYNC` with
the peer visible — so it does not enter the `DEGRADED` branch, and the stuck
watermark evaporates.

## Suggested hardening

- **Reset replay state on entry to `ACTIVE_IN_SYNC`.** Register a LOCAL listener
  for `ACTIVE_IN_SYNC` (mirroring the existing `STANDBY`/`STANDBY_TO_ACTIVE`
  listeners) that CAS-es the replay out of `DEGRADED` — or, more simply, stops the
  replay loop for a group once the local cluster is `ACTIVE_IN_SYNC`, since a
  healthy active is not the replay target for its own group.
- **Reconcile on force-promotion.** The purpose-built `force-promote` command
  proposed in [S20b](S20b_Failover_ZooKeeper_Down_No_Forced_Promotion.md) should
  additionally reset any local replay state for the group to a clean `SYNC`/
  stopped baseline, so the raw `update -F` edit does not leave the replay state
  machine stranded.
- **Alert on a stalled `lastRoundInSync`.** A replay whose `lastRoundProcessed`
  keeps advancing while `lastRoundInSync` (and thus the consistency point) is
  frozen for more than a few rounds is a reliable signal of a stuck-DEGRADED
  group; it is worth a metric/log-warn distinct from the per-round INFO line.

## Repro (test-bed)

1. Complete the S20b recovery drill so a survivor is force-promoted to
   `ACTIVE_IN_SYNC` via `update -F` while its peer's ZK was down (the peer-blind
   `DEGRADED_STANDBY` overlay is the trigger). Restore the peer's ZK and
   force-demote it to `STANDBY`; the pair is now single-active.
2. On the promoted cluster's RegionServers, watch the replay log for the group.
   Pair each `Starting to process round … for haGroup: <group>` with its following
   `Processed round … successfully with cluster in DEGRADED state, … lastRoundInSync=<frozen>`
   to confirm it is the force-promoted group (a co-resident, never-blind group
   will log plain `Processed round … successfully` with both watermarks
   advancing).
3. Confirm `lastRoundInSync` stays pinned at the outage-time round while
   `lastRoundProcessed` climbs to near wall-clock, and that the group never logs a
   `SYNCED_RECOVERY`/return-to-`SYNC`.
4. Clear it by recycling the promoted cluster's RegionServers; verify the group
   comes back up outside the `DEGRADED` branch.

Pod-log time = host time + 7h.