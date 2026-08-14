# Phoenix HA Failover Test Scenarios

Companion to
[`Phoenix_HA_ReArchitecture_for_Consistent_Failover.md`](./Phoenix_HA_ReArchitecture_for_Consistent_Failover.md).
Enumerates every failover scenario the design is expected to handle and gives
a copy-pasteable recipe to reproduce each one on the local kind-based
two-cluster test-bed at `/Users/tkhurana/soma/root/hbase-local-testbed/`.

Audience: Phoenix HA developers and QA. Terms like `DegradedStandby`,
`ActiveToStandby`, `OUT queue`, `preBatchMutate`, `postBatchMutateIndispensably`
are used without re-explanation; consult the design doc.

## 1. Overview

The design describes a state machine with seven roles
(`Active`, `Standby`, `ActiveToStandby`, `StandbyToActive`, `DegradedStandby`,
`AbortToActive`, `Offline`) and transitions driven by a mixture of:

- operator action (`Active → ActiveToStandby`, `ActiveToStandby → AbortToActive`),
- internal replication-writer state (`Standby → DegradedStandby` when the
  active's sync writes fail),
- completion events (`StandbyToActive → Active` when log replay drains),
- forced overrides (the dotted `DegradedStandby → StandbyToActive` edge).

This document maps each of those edges to a reproducible scenario, gives the
minimal fault-injection recipe, and names the observable success criterion.
Scenarios whose code isn't yet wired up on the feature branch are kept in the
matrix but flagged `STATUS: not yet testable` so the full test surface stays
visible.

A review pass from the replication log writer perspective added three
writer-invariant scenarios: S11 (ghost-write prevention — local WAL sync
failure must not trigger replication), S12 (`preWALRestore`-based
re-ship when remote sync fails post-commit), and S17 (sync-timeout →
RS abort). All three now exercise code that has landed on the feature
branch: `preWALRestore` is wired in `IndexRegionObserver`
(`IndexRegionObserver.java:939-962`), and the sync timeout path
(`syncFuture.get(syncTimeoutMs)` → `PhoenixWALSyncTimeoutException` →
`abort()` → `Abortable.abort()`) is implemented in `ReplicationLogGroup`.
These complement the state-machine edge coverage by exercising the
dual-log-coordination invariants directly.

> **Live-test-bed reconciliation (2026-07-16).** This doc was written
> against an earlier state of the test-bed and now understates what is
> runnable. On the current bed:
> - The failover trigger is a real CLI, **not** a `zkCli.sh set` blob:
>   `PhoenixHAAdminTool` ships `initiate-failover` and `abort-failover`
>   subcommands. Every `zkCli.sh set /phoenix/consistentHA/... '{"state":...}'`
>   block below is obsolete — use the CLI (see §2 step 6 and each scenario).
> - The HDFS replication root is **`/phoenixHA`**, not `/phoenix-replication`.
>   There is no config key or hardcoded default for it — the root is the
>   operator-supplied HDFS URL in the HA group record. The test-bed sets it
>   to `hdfs://namenode.<ns>.svc.cluster.local:9000/phoenixHA` via
>   `utils/create-ha-group.sh`. Live layout:
>   `/phoenixHA/<haGroupName>/{in,out}/shard/NNN/*.plog`.
> - The HA group name is **`testHAGroup`** (ZK path
>   `/phoenix/consistentHA/testHAGroup`), not `default`.
> - Phoenix system tables, coprocessor registration, the `SYSTEM.HA_GROUP`
>   bootstrap row, and a SQL client (`/trucke/sqlline.sh`) are **already in
>   place** on the bed — §2 steps 3–6 are largely no-ops there (see notes
>   inline).

## 2. Prerequisites

Before running any scenario:

1. **Test-bed up.** Follow `/Users/tkhurana/soma/root/hbase-local-testbed/README.md` —
   kind cluster named `bdlocal`, namespaces `cluster-a` and `cluster-b`. Each
   runs one zookeeper, namenode, hmaster and (after the scale-up) **3
   datanodes + 3 regionservers** = 9 Ready pods per namespace. (The original
   single-replica bed had 5.) DataNodes/RegionServers are the scalable roles.
   ```
   kubectl get ns cluster-a cluster-b
   kubectl -n cluster-a rollout status sts/hmaster sts/regionserver --timeout=120s
   kubectl -n cluster-b rollout status sts/hmaster sts/regionserver --timeout=120s
   ```
2. **Phoenix server jar on HBase classpath.** Already wired by the test-bed
   Dockerfile — the image symlinks
   `/coprocessors/phoenix/5.3.0/phoenix-server-5.3.0-consistent_failover-14.1.11.jar`
   into `/hbase/lib/phoenix-server-5.3.0.jar`. Verify:
   ```
   kubectl -n cluster-a exec regionserver-0 -- \
     bash -c '/hbase/bin/hbase classpath | tr ":" "\n" | grep phoenix-server'
   ```
   Must print the jar path.
3. **Phoenix coprocessors registered.** *Already done on the bed* — the
   coprocessor registrations in §3 are baked into
   `hbase-local-testbed/conf/hbase-site.xml` and rendered into every pod.
   Verify `PhoenixRegionServerEndpoint` loaded:
   ```
   kubectl -n cluster-a exec regionserver-0 -- \
     bash -lc 'grep -c PhoenixRegionServerEndpoint /data/hbase-logs/regionserver.log'
   ```
   Only re-apply §3 if you changed the template.
4. **HDFS replication root.** The root is **not** a fixed `/phoenix-replication`
   and there is no config key for it — it is the path component of the
   operator-supplied HDFS URL in the HA group record (see step 6). The
   test-bed uses **`/phoenixHA`**. The per-HA-group shard subdirectories
   are created on demand by `ReplicationLogGroup`
   (`createShardManager()` calls `fs.mkdirs(...)`,
   `ReplicationLogGroup.java:952`). The layout is
   `<root>/<haGroupName>/in` (peer's inbound sync logs, `STANDBY_DIR = "in"`)
   and `<root>/<haGroupName>/out` (local store-and-forward queue,
   `FALLBACK_DIR = "out"`) — both **lowercase**
   (`ReplicationLogGroup.java:212-213`). Do **not** hand-create uppercase
   `IN`/`OUT`. On the live bed this is already populated — verify:
   ```
   kubectl -n cluster-b exec namenode-0 -- \
     /hadoop/bin/hdfs dfs -ls /phoenixHA/testHAGroup/in/shard | tail
   ```
5. **Phoenix system tables created.** *Already done on the bed* —
   `SYSTEM.HA_GROUP` (and the rest of `SYSTEM.*`) exists. A SQL client is
   bundled at `/trucke/sqlline.sh` (see `hbase-local-testbed/README.md`
   "Connecting with sqlline"), so the "no client pod" caveat no longer
   applies. Verify:
   ```
   kubectl -n cluster-a exec -i hmaster-0 -- \
     bash -lc 'echo list | /hbase/bin/hbase shell -n 2>/dev/null' | grep HA_GROUP
   ```

6. **`SYSTEM.HA_GROUP` bootstrap row.** This tells each cluster who its peer
   is and where the peer's HDFS lives. Without it, `HAGroupStoreClient`
   can't compute `peerHdfsUrl`, the Replication Log Writer has no target,
   and replication never starts.

   **On the test-bed this is done by `utils/create-ha-group.sh`**, which
   runs `PhoenixHAAdminTool create` against *both* clusters with the same
   args (slot 1/2 is assigned deterministically by ZK-URL ordering, not by
   which cluster is local). Do **not** hand-write the row or `zkCli.sh set`
   the znode. Bootstrap (idempotent):
   ```
   ./utils/create-ha-group.sh          # from the hbase-local-testbed root
   ```
   which is equivalent to running, on each cluster:
   ```
   kubectl -n <ns> exec -i hmaster-0 -- \
     /hbase/bin/hbase org.apache.phoenix.jdbc.PhoenixHAAdminTool create \
       -g testHAGroup -p FAILOVER \
       -zk1 'zookeeper-0.zookeeper.cluster-a.svc.cluster.local:2181:/hbase' \
       -c1  'hmaster-0.hmaster.cluster-a.svc.cluster.local:16000' -cr1 ACTIVE  \
       -hdfs1 'hdfs://namenode.cluster-a.svc.cluster.local:9000/phoenixHA' \
       -zk2 'zookeeper-0.zookeeper.cluster-b.svc.cluster.local:2181:/hbase' \
       -c2  'hmaster-0.hmaster.cluster-b.svc.cluster.local:16000' -cr2 STANDBY \
       -hdfs2 'hdfs://namenode.cluster-b.svc.cluster.local:9000/phoenixHA'
   ```

   Column names come from
   `phoenix-core-client/src/main/java/org/apache/phoenix/jdbc/PhoenixDatabaseMetaData.java:461-471`.
   `HAGroupStoreClient.getSystemTableHAGroupRecord()` matches the local ZK
   URL against `ZK_URL_1` / `ZK_URL_2` and assigns the *other* cluster's
   `HDFS_URL_*` as `peerHdfsUrl`
   (`HAGroupStoreClient.java:607-685`, decision logic at 643-676).

   Confirm the record and current roles on either cluster:
   ```
   kubectl -n cluster-a exec -i hmaster-0 -- \
     /hbase/bin/hbase org.apache.phoenix.jdbc.PhoenixHAAdminTool \
       get-cluster-role-record -g testHAGroup
   ```

7. **Optional chaos tools** (needed for scenarios S2, S3, S4, S7, S8):
   - Toxiproxy — see §6.3.
   - Chaos Mesh — see §6.4.
   - Calico CNI — see §6.2 (needed for any `NetworkPolicy`-based scenario; kind's
     default CNI doesn't enforce NetworkPolicies).

Every scenario below assumes these prereqs are green. Re-check after any
`kind delete cluster` cycle.

## 2.1 SQL client conventions (read before running any scenario)

**All data-plane reads and writes go through the Phoenix client, never the
HBase shell.** Phoenix replication is driven by the server-side coprocessor,
which only fires on mutations that arrive through the Phoenix write path. A
raw `hbase shell put` bypasses it entirely and **will not replicate** — the
scenario would silently "pass" while testing nothing. Only genuine HBase
*admin* operations (a region `split`) use the HBase shell.

The client is `/trucke/sqlline.sh`, baked into the image (see
`hbase-local-testbed/README.md` "Connecting with sqlline"). Two connection
forms are used below; pick per what the scenario is testing.

**(a) Direct single-cluster connection** — for setup (schema creation) and
for reading back on a specific cluster. Uses the pod's `ZOOKEEPER_QUORUM`
env var:
```
kubectl -n cluster-a exec -i hmaster-0 -- bash -lc '
  /trucke/sqlline.sh "$ZOOKEEPER_QUORUM" <<SQL 2>/dev/null
CREATE TABLE IF NOT EXISTS PHOENIX_HA_T (ID BIGINT PRIMARY KEY, V VARCHAR);
UPSERT INTO PHOENIX_HA_T VALUES (1, '\''hello'\'');
SELECT * FROM PHOENIX_HA_T WHERE ID = 1;
!quit
SQL
'
```

**(b) HA (failover) connection** — for scenarios that must exercise the
*client* failover path (`FailoverPhoenixConnection`: HA-error surfacing,
transparent retry across a failover — S1, S5, S10). Feed the statements via
a **SQL file** and pass the HA group as a **`-p` property**:
```
# stage the SQL file in the pod
kubectl -n cluster-a exec -i hmaster-0 -- bash -lc 'cat > /tmp/load.sql' <<'SQL'
UPSERT INTO PHOENIX_HA_T VALUES (1, 'v1');
UPSERT INTO PHOENIX_HA_T VALUES (2, 'v2');
SQL

# run it through the bracketed HA URL + ha group property (non-interactive)
kubectl -n cluster-a exec hmaster-0 -- \
  /trucke/sqlline.sh \
    '[hmaster-0.hmaster.cluster-a.svc.cluster.local\:16000|hmaster-0.hmaster.cluster-b.svc.cluster.local\:16000]' \
    /tmp/load.sql \
    -p phoenix.ha.group.name testHAGroup
```

Mechanics that make form (b) work (all verified against `sqlline.sh`):
- **Positional order is fixed:** `<HA-URL>` first, then the **SQL file**
  (arg 2, must not start with `-`), then `-p <prop> <value>`. Everything from
  the first `-p` onward becomes the `!connect` override props.
- **A SQL file argument triggers non-interactive mode.** The script runs
  `sqlline -e "!connect -p phoenix.ha.group.name testHAGroup jdbc:phoenix:<URL> none none PhoenixDriver" --run=<file>`,
  i.e. it opens the connection *first* (with the HA props), then executes the
  file against it. Without a file arg, `-p` only **prints** a `!connect` line
  and drops to interactive mode ("No current connection" if you then pipe
  SQL) — so always pass a file for scripted runs.
- **The HA group name must be a `-p` property, not a file line.** It is
  consumed when `FailoverPhoenixConnection` is built (connect time); a
  statement inside the file would be too late. The bracketed URL, cluster
  roles, and `policy=FAILOVER` are logged at connect by
  `jdbc.HighAvailabilityGroup` — grep for that line to confirm the failover
  path is actually in use.
- **The port colon is backslash-escaped** inside the brackets (`\:16000`);
  shell single-quotes preserve the backslash.
- **No `!autocommit on` needed** — sqlline defaults `autocommit=true`, so
  every `UPSERT` commits (and thus replicates) on execution.
- **Generating load:** for N rows, build the `.sql` file with a `seq`/printf
  loop and stage it once; a single sqlline invocation runs the whole file.
  Do **not** spawn one sqlline per row — the Phoenix client JVM cold-start +
  connect is ~10 s, so per-row invocation is unusably slow.

**Reading back to verify** always uses form (a) against the target cluster
(you're checking what physically landed there, not exercising failover).
Strip sqlline's ANSI color and connect/close INFO-log noise when scraping
output, e.g. `... | sed 's/\x1b\[[0-9;]*m//g'`.

## 2.2 Cross-cluster data validation (run after every scenario)

Every scenario below ends with a **replication validation** step: confirm that
cluster-a and cluster-b hold the same data. This is the single best catch-all
regression check — it detects lost writes, un-replicated rows, and split-brain
divergence that a "the failover completed" status alone would miss.

Row **count** is the cheap first check, but it is not sufficient on its own: two
clusters can have equal counts yet diverging *contents*, and an off-by-N count
can be either real loss **or** ordinary STANDBY lag (the STANDBY trails the
ACTIVE by up to one replication round, ~60s rotation + replay). So the
validation is two-tier and lag-aware:

1. `COUNT(*)` on both clusters (fast gate).
2. Full ordered content dump (`ID=C` per row, sorted **locally** — never trust
   server-side `ORDER BY` to collate identically across clusters), compared
   byte-for-byte. On any mismatch, wait one round and retry before declaring
   divergence; if it survives the wait, print the exact diverging rows.

The test-bed ships this as `hbase-local-testbed/utils/validate-replication.sh`:
```
# from the hbase-local-testbed root; defaults to PHOENIX_HA_T, cluster-a, cluster-b
./utils/validate-replication.sh
./utils/validate-replication.sh MYTABLE cluster-a cluster-b   # other table/namespaces
```
Exit `0` = in sync, `1` = diverged (diff printed), `2` = query error. Knobs:
`ROUND_WAIT` (seconds per round-wait, default 70), `MAX_RETRIES` (round-waits
tolerated before failing, default 1), `SHOW_DIFF` (max diverging rows printed).
Set `MAX_RETRIES=0` for an *immediate* check when you know writes have already
quiesced (no lag to wait out).

> **A diverging row is usually a test-harness artifact, not a replication bug.**
> The most common cause is a **direct form (a) write to the data plane**: a
> direct-ZK write carries no `_HAGroupName`, bypasses the Phoenix replication
> coprocessor, and never ships to the peer — so it lands on exactly one cluster
> and shows up here forever. This is why §2.1's HARD RULE exists (data-plane
> writes go through the HA connection, form (b); form (a) is for DDL and
> read-back only). If validation localizes a lone diverging row, first check
> whether a direct write created it before suspecting the replication path.

## 3. Configuration

Apply these to **both** `cluster-a` and `cluster-b` as a patch on the
`hbase-site.xml` template used by the hmaster and regionserver pods. The
simplest path on the local test-bed is to edit
`/Users/tkhurana/soma/root/hbase-local-testbed/conf/hbase-site.xml`, rebuild the image,
`kind load`, and recycle the pods — see `hbase-local-testbed/README.md` for the
rebuild loop. Properties:

```xml
<!-- Register Phoenix coprocessors on region servers -->
<property>
  <name>hbase.coprocessor.region.classes</name>
  <value>org.apache.phoenix.hbase.index.IndexRegionObserver,org.apache.phoenix.coprocessor.ScanRegionObserver,org.apache.phoenix.coprocessor.UngroupedAggregateRegionObserver,org.apache.phoenix.coprocessor.GroupedAggregateRegionObserver,org.apache.phoenix.coprocessor.ServerCachingEndpointImpl,org.apache.phoenix.coprocessor.SequenceRegionObserver</value>
</property>
<property>
  <name>hbase.coprocessor.regionserver.classes</name>
  <value>org.apache.phoenix.coprocessor.PhoenixRegionServerEndpoint</value>
</property>
<property>
  <name>hbase.coprocessor.master.classes</name>
  <value>org.apache.phoenix.coprocessor.PhoenixMasterObserver</value>
</property>

<!-- Phoenix replication log knobs (defaults shown; tune for fault scenarios) -->
<property><name>phoenix.replication.log.rotation.time.ms</name><value>60000</value></property>
<property><name>phoenix.replication.log.rotation.size.bytes</name><value>134217728</value></property>
<property><name>phoenix.replication.log.sync.retries</name><value>1</value></property>
<property><name>phoenix.replication.log.ringbuffer.size</name><value>32768</value></property>

<!-- Sync timeout. If unset, ReplicationLogGroup computes it as
     hbase.regionserver.wal.sync.timeout (default 300000 = 5 min) + ZK session
     timeout. Set explicitly to shorten S17's abort wait. -->
<property><name>phoenix.replication.log.sync.timeout.ms</name><value>300000</value></property>

<!-- Phoenix HA group store -->
<property><name>phoenix.ha.failover.timeout.ms</name><value>10000</value></property>
```

Config-key constants and defaults are defined in
`phoenix-core-server/src/main/java/org/apache/phoenix/replication/ReplicationLogGroup.java:152-188`
(`sync.retries` default is `1`, `rotation.size.bytes` default is 128 MB per
PHOENIX-7957/7958). `phoenix.ha.failover.timeout.ms` is
`FailoverPhoenixConnection.FAILOVER_TIMEOUT_MS_ATTR`
(`FailoverPhoenixConnection.java:77`).

**Peer-cluster HDFS URL.** There is **no** `phoenix.replication.*` config
key for this. The URL is stored as a field (`peerHdfsUrl`) on the
`HAGroupStoreRecord` — a row in the `SYSTEM.HA_GROUP` table plus its JSON
copy on the ZK znode. On startup, `HAGroupStoreClient` reads `HDFS_URL_1`
and `HDFS_URL_2` from the system table row and decides which one is "this
cluster" vs "peer" by matching against the local HBase ZK URL
(`phoenix-core-client/src/main/java/org/apache/phoenix/jdbc/HAGroupStoreClient.java:607-685`).
`ReplicationLogGroup.createPeerShardManager()` consumes the value via
`createShardManager(haGroupStoreRecord.getPeerHdfsUrl(), STANDBY_DIR)`
(`phoenix-core-server/src/main/java/org/apache/phoenix/replication/ReplicationLogGroup.java:1016`).

The `SYSTEM.HA_GROUP` row bootstrapping step is the single most important
setup task for every scenario below — see §2 step 6.

**Recycle after config changes:**
```
for ns in cluster-a cluster-b; do
  kubectl -n $ns delete pod hmaster-0 regionserver-0 --grace-period=1
done
```
Wait ~90s and confirm both pods are 1/1 Ready. Check the regionserver log for
`PhoenixRegionServerEndpoint started` (implemented) or any coprocessor load
failure (`ClassNotFoundException`).

## 4. State transition reference

Compact summary of the design's state diagram. Scenarios reference these state
names verbatim.

| State | Accepts writes | Accepts reads | Replication role |
|---|---|---|---|
| `Active` | yes | yes | primary; ships sync logs to standby |
| `Standby` | no | lookback only | consumes sync logs from active |
| `ActiveToStandby` | no (rejects with HA exception) | yes | drains pending sync logs before yielding |
| `StandbyToActive` | no | no | replays all received logs before promotion |
| `DegradedStandby` | no | no | peer active is storing-and-forwarding; catching up |
| `AbortToActive` | no (briefly) | yes | reverting a failover mid-flight |
| `Offline` | no | no | cluster removed from HA group |

Edges exercised by the ten scenarios below:

```
 Active ──────── S1, S10 ───────▶ ActiveToStandby ─────▶ Standby
 Active ──── S2, S7 ────▶ Active (sync→S&F→sync internal)
 Active ───── S6 ─────▶ Active (RS crash, WAL replay)
 Active ──── S11 ────▶ Active (local WAL sync fails; replication must NOT fire)
 Active ──── S12 ────▶ Active (remote sync fails; preWALRestore re-ships)
 Active ──── S17 ────▶ (RS abort on sync-timeout; new RS picks up as Active)
 Standby ─────────────▶ StandbyToActive ─────▶ Active
 Standby ──── S8 ─────▶ DegradedStandby
 DegradedStandby ── S3 ──▶ Standby (recovery, normal failover)
 DegradedStandby ══ S4 ══▶ StandbyToActive (forced, dotted edge)
 ActiveToStandby ── S5, S18 ──▶ AbortToActive ──▶ Active (S18: rotation resumes)
 Active ──── S18 ────▶ ActiveToStandby (rotation suspends; no new files)
 Active ──── S19 ────▶ (RS restart in cutover gate; preWALRestore re-ships, no deadlock re-arm)
 any ──────── S9 ───────▶ any (replay semantics under split)
```

## 5. Scenarios

### 5.1 Happy-path failover

#### S1. Graceful failover with fully synced standby

**Exercises:** `Active → ActiveToStandby → Standby` and
`Standby → StandbyToActive → Active` atomically.

**Hot path:** `ReplicationLogGroup` (SYNC mode),
`FailoverPhoenixConnection`,
`HAGroupStoreManager.initiateFailoverOnActiveCluster()`,
`PhoenixRegionServerEndpoint` state watchers.

**Fault to inject:** none — this is the clean operator-driven path.

**Tool:** `kubectl` only.

**STATUS:** ✅ **COMPLETED — verified end-to-end on the test-bed 2026-07-17**
(Phoenix branch `mode-state-fix`, PHOENIX-7562 deadlock fix). Failover converged
in 50s (< 120s ceiling), roles flipped atomically (a→STANDBY, b→ACTIVE), zero
RPO (pre-failover row replayed to the new ACTIVE), HA-write propagation
confirmed (immediate on ACTIVE, one round later on STANDBY), and cross-cluster
data validation (§2.2) PASSed. The operator trigger is
`PhoenixHAAdminTool initiate-failover` (see below) — the formerly-blocking "CLI
still being wired" caveat is obsolete.

> **How the trigger works.** The consistent-failover record lives under the
> Curator namespace `phoenix/consistentHA`
> (`HAGroupStoreClient.ZK_CONSISTENT_HA_GROUP_RECORD_NAMESPACE`), so the raw
> path is `/phoenix/consistentHA/<haGroupName>` (here **`testHAGroup`**). The
> znode holds a JSON-serialized `HAGroupStoreRecord` whose state names are
> granular (e.g. `ACTIVE_IN_SYNC`, `ACTIVE_IN_SYNC_TO_STANDBY`,
> `STANDBY_TO_ACTIVE`, `DEGRADED_STANDBY` — see `HAGroupStoreRecord.HAGroupState`,
> `HAGroupStoreRecord.java:51-73`). **Do not** `zkCli.sh set` a `{"state":...}`
> blob — it will not deserialize. Drive the transition with
> `PhoenixHAAdminTool initiate-failover -g testHAGroup` (run on the ACTIVE
> cluster) and revert with `abort-failover -g testHAGroup`. Inspect the
> current roles at any time with `get-cluster-role-record -g testHAGroup`.

**Setup**
1. Fresh clusters (prereqs met, `/phoenixHA/testHAGroup` dirs empty).
2. Create the Phoenix table on cluster-a (form (a), §2.1). Leave
   `REPLICATION_SCOPE=0` (the Phoenix default): Phoenix **synchronous**
   replication is driven by the server-side coprocessor on the Phoenix write
   path, not by HBase's per-CF async replication. Setting `REPLICATION_SCOPE=1`
   would additionally enlist the CF in HBase's own async replication and
   double-ship every mutation. `COLUMN_ENCODED_BYTES=0` disables qualifier
   encoding so both clusters agree byte-for-byte on the on-disk layout:
   ```
   kubectl -n cluster-a exec -i hmaster-0 -- bash -lc '
     /trucke/sqlline.sh "$ZOOKEEPER_QUORUM" <<SQL 2>/dev/null
   CREATE TABLE IF NOT EXISTS PHOENIX_HA_T (ID VARCHAR PRIMARY KEY, C VARCHAR)
     COLUMN_ENCODED_BYTES=0;
   !quit
   SQL
   '
   ```
3. Pre-create the identical Phoenix table on cluster-b (schemas are not
   replicated — same DDL, form (a) against cluster-b):
   ```
   kubectl -n cluster-b exec -i hmaster-0 -- bash -lc '
     /trucke/sqlline.sh "$ZOOKEEPER_QUORUM" <<SQL 2>/dev/null
   CREATE TABLE IF NOT EXISTS PHOENIX_HA_T (ID VARCHAR PRIMARY KEY, C VARCHAR)
     COLUMN_ENCODED_BYTES=0;
   !quit
   SQL
   '
   ```

**Execute**
1. Write a known row on cluster-a through the **HA connection** (form (b),
   §2.1) — this is the client failover path under test:
   ```
   kubectl -n cluster-a exec -i hmaster-0 -- bash -lc 'cat > /tmp/s1-pre.sql' <<'SQL'
   UPSERT INTO PHOENIX_HA_T VALUES ('r-pre-failover', 'written-on-A');
   SQL
   kubectl -n cluster-a exec hmaster-0 -- \
     /trucke/sqlline.sh \
       '[hmaster-0.hmaster.cluster-a.svc.cluster.local\:16000|hmaster-0.hmaster.cluster-b.svc.cluster.local\:16000]' \
       /tmp/s1-pre.sql \
       -p phoenix.ha.group.name testHAGroup
   ```
2. Wait until the sync log round has rolled (default 60s):
   ```
   sleep 65
   ```
3. Initiate failover from the ACTIVE cluster (cluster-a):
   ```
   kubectl -n cluster-a exec -i hmaster-0 -- \
     /hbase/bin/hbase org.apache.phoenix.jdbc.PhoenixHAAdminTool \
       initiate-failover -g testHAGroup
   ```
4. Watch cluster-b promote itself. Once it's seen the peer transition, the
   `HAGroupStoreManager` on cluster-b drives its own role to `STANDBY_TO_ACTIVE`,
   replays outstanding logs, then atomically promotes both records (b → ACTIVE,
   a → STANDBY). Poll the role record until it settles:
   ```
   for i in $(seq 1 60); do
     kubectl -n cluster-a exec -i hmaster-0 -- \
       /hbase/bin/hbase org.apache.phoenix.jdbc.PhoenixHAAdminTool \
       get-cluster-role-record -g testHAGroup 2>/dev/null
     echo "--- $(date +%T) ---"
     sleep 2
   done
   # Stop when cluster-a shows STANDBY and cluster-b shows ACTIVE.
   ```

**Verify**
- State timeline (from step 4 log) shows no step remained longer than the
  `phoenix.ha.failover.timeout.ms` (default 10s). Total wall-clock < 120 s.
- **HA-connection writes now route to the new ACTIVE (cluster-b) and succeed —
  they are *not* rejected.** After the roles flip, a form (b) write resolves the
  role record (role1=STANDBY, role2=ACTIVE) and directs the mutation to
  cluster-b. The row is visible on cluster-b **immediately** and on cluster-a
  (STANDBY) **one sync-log round later** (~60s rotation + replay), because the
  STANDBY is a replication *sink*, not the write target. Confirm the propagation:
  ```
  # write through the HA connection (lands on cluster-b, the ACTIVE)
  kubectl -n cluster-a exec -i hmaster-0 -- bash -lc 'cat > /tmp/s1-post.sql' <<'SQL'
  UPSERT INTO PHOENIX_HA_T VALUES ('r-post-failover', 'written-via-HA');
  SQL
  kubectl -n cluster-a exec hmaster-0 -- \
    /trucke/sqlline.sh \
      '[hmaster-0.hmaster.cluster-a.svc.cluster.local\:16000|hmaster-0.hmaster.cluster-b.svc.cluster.local\:16000]' \
      /tmp/s1-post.sql \
      -p phoenix.ha.group.name testHAGroup

  # cluster-b (ACTIVE) has it right away
  kubectl -n cluster-b exec -i hmaster-0 -- bash -lc '
    /trucke/sqlline.sh "$ZOOKEEPER_QUORUM" <<SQL 2>/dev/null
  SELECT * FROM PHOENIX_HA_T WHERE ID = '\''r-post-failover'\'';
  !quit
  SQL
  '
  # cluster-a (STANDBY) does NOT have it yet; after ~65s it does
  sleep 65
  kubectl -n cluster-a exec -i hmaster-0 -- bash -lc '
    /trucke/sqlline.sh "$ZOOKEEPER_QUORUM" <<SQL 2>/dev/null
  SELECT * FROM PHOENIX_HA_T WHERE ID = '\''r-post-failover'\'';
  !quit
  SQL
  '
  ```
  > **Why there is no STANDBY write-rejection to observe here.** The
  > server-side `MutationBlockedIOException`
  > (`IndexRegionObserver.preBatchMutate` → `HAGroupStoreManager.isMutationBlocked()`,
  > `IndexRegionObserver.java:886-892`, `isMutationBlocked` at
  > `HAGroupStoreManager.java:210`) fires **only** in the transitional
  > `ACTIVE_TO_STANDBY` state, **only** when
  > `phoenix.cluster.role.based.mutation.block.enabled=true`, and **only** for a
  > mutation carrying the `_HAGroupName` attribute (i.e. one that arrived through
  > an HA connection). A *terminal* `STANDBY` does not block: `isMutationBlocked()`
  > returns `this == ACTIVE_TO_STANDBY` only (`ClusterRoleRecord.java:85-86`).
  > And a direct form (a) write against the demoted cluster-a is not blocked
  > either — it carries no `_HAGroupName`, so the gate is skipped. Clients are
  > simply expected to reach the ACTIVE via the HA URL, which they do
  > transparently. The transitional block is exercised in the raced-write
  > scenarios (S10), not here.
- **Zero RPO — the pre-failover row survived and replayed to the new ACTIVE.**
  Read cluster-b (form (a)):
  ```
  kubectl -n cluster-b exec -i hmaster-0 -- bash -lc '
    /trucke/sqlline.sh "$ZOOKEEPER_QUORUM" <<SQL 2>/dev/null
  SELECT * FROM PHOENIX_HA_T WHERE ID = '\''r-pre-failover'\'';
  !quit
  SQL
  '
  ```
  `r-pre-failover` must return `written-on-A` — it was written on cluster-a
  before the failover and replayed onto cluster-b during promotion.
- **Rotation-suspend invariant (the regression guard — this is where the
  original deadlock lived).** The moment cluster-a enters
  `ACTIVE_IN_SYNC_TO_STANDBY`, its replication-log rotation is suspended
  (`ReplicationLogGroup.failoverPending` set on the cutover listener;
  `LogRotationTask` no longer runs because the guard in
  `ReplicationLog.requestRotation()` short-circuits before the CAS), so no
  **new** `.plog` files appear in cluster-b's `in` shard dir. Poll the dir
  across the `*_TO_STANDBY` window and confirm the file count **stops
  growing**:
  ```
  # poll cluster-b's in shard dir through the failover window
  for i in $(seq 1 30); do
    kubectl -n cluster-b exec namenode-0 -- \
      /hadoop/bin/hdfs dfs -ls -R /phoenixHA/testHAGroup/in 2>/dev/null \
      | grep -c '\.plog$'
    sleep 2
  done
  ```
  This quiescence is **why** the failover now completes: the standby's
  promotion gate `shouldTriggerFailover()` requires
  `getNewFiles(nextRound, currentRound).isEmpty()`
  (`ReplicationLogDiscoveryReplay.java:~520`), which can only hold once the
  active stops minting a file every round. **Before the fix**, rotation kept
  running through the cutover gate, a fresh (often header-only) `.plog`
  dropped each round, condition #4 never held, and both clusters spun forever
  (cluster-a stuck `ACTIVE_IN_SYNC_TO_STANDBY`, cluster-b stuck
  `STANDBY_TO_ACTIVE`). If the count keeps climbing here and step 4's poll
  never settles, the deadlock has regressed.
- The files present at cutover are consumed at the moment of the atomic
  promotion — log replay drained them. (The dirs themselves persist; watch
  for *new* files ceasing, not the tree disappearing.)
- **Cross-cluster data validation (§2.2).** Confirm both clusters converged to
  identical contents:
  ```
  ./utils/validate-replication.sh          # expect: PASS: clusters in sync
  ```
  Must exit `0`. A lone diverging row here is almost always a direct form (a)
  data-plane write from an earlier step (§2.2 callout) — not a replication bug.

**Teardown**
- `DROP TABLE PHOENIX_HA_T` (form (a), §2.1) on both clusters. To rerun, fail
  back with `PhoenixHAAdminTool initiate-failover -g testHAGroup` from the new
  ACTIVE (cluster-b) so roles return to a→ACTIVE / b→STANDBY. Or just
  `kind delete cluster --name bdlocal && ...` for a clean slate.

---

#### S10. Planned failover with full standby catch-up

**Exercises:** same edges as S1 but under sustained write load, to catch
races between incoming writes and the drain.

**Hot path:** `ReplicationLogGroup` final flush, `ReplicationLogDiscovery`
(target-side reader), `HAGroupStoreManager` atomic ZK multi-op.

**Fault to inject:** none, but concurrent load.

**Tool:** `kubectl` + a background load generator.

**STATUS:** ✅ **COMPLETED — verified on the test-bed 2026-07-17, re-verified
under sustained load 2026-07-25** (build with the PHOENIX-7562 fix). Ran the
raced-write path: HA load started first (pinned to the ACTIVE), failover
triggered mid-stream. In-flight UPSERTs hit
`MutationBlockedIOException` ("Some CRRs are in ACTIVE_TO_STANDBY state …") the
instant the old ACTIVE entered `ACTIVE_IN_SYNC_TO_STANDBY`, the client aborted
the batch cleanly, the failover still converged, all rows committed before the
block were present on the new ACTIVE, and cross-cluster data validation (§2.2)
PASSed. (Depends on the admin CLI, same as S1 — now confirmed working.)
Correctness clean on the 2026-07-25 run too: rotation on the demoting cluster
suspended correctly (`entered cutover gate; suspending rotation` at cutover,
then `rotation suspended: failover pending` on each subsequent 60 s
rotation tick, minting **zero** new `.plog` files), promotion + demotion
committed atomically in the same 2 ms (no dual-active), both clusters held the
identical row set, and §2.2 validation PASSed byte-for-byte.

> **TIMING FINDING (2026-07-25) — failover under sustained load took ~131 s,
> over the documented 120 s ceiling, and the excess is structural, not a
> drain-volume or deadlock problem.** The failover clock is gated by the
> *replay round grid*, not by how much data is in flight. Decomposition of the
> 131 s (cutover fired 06:24:00, converged 06:26:12):
> - **~69 s — round-close + buffer.** The standby's promotion gate can only
>   admit a round once `now − roundEndTime ≥ roundTimeMills + bufferMillis`
>   (`ReplicationLogDiscoveryReplay.getFirstRoundToProcess()`,
>   `ReplicationLogDiscoveryReplay.java:388`). With the defaults
>   `roundTimeMills = 60 000` and `bufferMillis = 15 %` of that `= 9 000`
>   (`DEFAULT_WAITING_BUFFER_PERCENTAGE = 15.0`,
>   `ReplicationLogDiscoveryReplay.java:104`), the *final* data-bearing round
>   (start 06:24:00) is not eligible until **06:25:09** (endTime 06:25:00 + 9 s).
>   An *unloaded* failover has no data-bearing trailing round, so it skips this
>   wait and converges in seconds — this cost only appears under load.
> - **~59 s — poll-cadence near-miss.** The replay poller runs on a rigid
>   `scheduleAtFixedRate(…, 0, 60 s)` grid (`ReplicationLogReplayService.java:193,199`,
>   key `phoenix.replication.replay.service.executor.frequency.seconds`, default
>   **60**). Its tick landed at **06:25:08.980** — ~20 ms *before* the 06:25:09
>   eligibility instant — logged `Found first round to process as
>   Optional.empty`, and because it is fixed-*rate* (not fixed-delay) it did not
>   retry until the next grid tick at **06:26:08**, burning a whole ~60 s cycle.
> - **~4 s — HDFS lease recovery** on the final (open, header-only) `.plog`:
>   `Failed to recover lease attempt=0` → `Recovered lease attempt=1 after
>   4001ms`, `Invalid Trailer … proceeding`, 0 mutations (it was the
>   suspended-rotation file).
>
> So the structural worst case under load is
> `buffer (9 s) + up-to-one-full-poll-interval (≤60 s) + lease-recovery (~4 s)`
> **on top of** waiting for the final round to close — which can exceed 120 s
> with everything working correctly. The 120 s SLA ceiling is therefore in
> tension with `bufferMillis + executor.frequency.seconds`; either retune those
> knobs (shorter poll interval, or make the poller fixed-*delay*/eligibility-aware
> so it doesn't miss the tick by milliseconds) or raise the ceiling. **Validate
> the real ceiling on a real cluster** — kind's single-node HDFS replay/lease
> timings are not representative, but the round-grid arithmetic above is
> config-driven and cluster-independent.

**Setup**
1. Prereqs, table created on both clusters (reuse S1 setup).
2. Stage a bulk load file and start a background write loop against cluster-a
   through the **HA connection** (form (b), §2.1) in a separate terminal. Build
   the `.sql` once (10k UPSERTs) and feed it as the file arg — one JVM, not one
   per row:
   ```
   kubectl -n cluster-a exec -i hmaster-0 -- bash -lc \
     'for i in $(seq 1 10000); do
        printf "UPSERT INTO PHOENIX_HA_T VALUES ('"'"'r-%d'"'"', '"'"'v-%d'"'"');\n" $i $i
      done > /tmp/s10-load.sql'
   kubectl -n cluster-a exec hmaster-0 -- \
     /trucke/sqlline.sh \
       '[hmaster-0.hmaster.cluster-a.svc.cluster.local\:16000|hmaster-0.hmaster.cluster-b.svc.cluster.local\:16000]' \
       /tmp/s10-load.sql \
       -p phoenix.ha.group.name testHAGroup &
   LOAD_PID=$!
   ```
   The single sqlline run streams all 10k UPSERTs through the failover
   connection; when the failover fires mid-file, the remaining statements
   surface `FailoverSQLException` — that is the raced-write signal S10 checks.

**Execute**
1. After ~30s of load, trigger failover exactly as S1 step 3:
   ```
   kubectl -n cluster-a exec -i hmaster-0 -- \
     /hbase/bin/hbase org.apache.phoenix.jdbc.PhoenixHAAdminTool \
       initiate-failover -g testHAGroup
   ```
2. Wait for promotion (S1 step 4).

**Verify**
- Total failover time from `ActiveToStandby` set to `Active` on cluster-b
  is still < 120 s despite load.
- After promotion, `SELECT COUNT(*) FROM PHOENIX_HA_T` on cluster-b (form (a))
  equals the max `i` the load loop committed before it saw its first HA
  exception. No rows lost:
  ```
  kubectl -n cluster-b exec -i hmaster-0 -- bash -lc '
    /trucke/sqlline.sh "$ZOOKEEPER_QUORUM" <<SQL 2>/dev/null
  SELECT COUNT(*) FROM PHOENIX_HA_T;
  !quit
  SQL
  '
  ```
- Client writes that raced the transition saw the server-side block surfaced to
  the client. **Confirmed live (2026-07-17):** the in-flight UPSERTs on the
  connection pinned to the old ACTIVE fail with
  ```
  Error: RetriesExhaustedWithDetailsException: Failed 1 action:
    MutationBlockedIOException: Blocking Mutation as Some CRRs are in
    ACTIVE_TO_STANDBY state and CLUSTER_ROLE_BASED_MUTATION_BLOCK_ENABLED is true
      at IndexRegionObserver.preBatchMutate(IndexRegionObserver.java:695)
  ```
  thrown once cluster-a enters `ACTIVE_IN_SYNC_TO_STANDBY` (the client logs
  `MutationState: Abort successful` and aborts the batch cleanly). This is the
  transitional-state block — it requires **all three** preconditions at once
  (flag on, `ACTIVE_TO_STANDBY` state, `_HAGroupName` on the mutation i.e. an
  HA-connection write), all three visible in that one message. **Ordering
  matters:** the load must be *started before* the failover so its connection is
  established against the still-ACTIVE cluster and is mid-stream when the cutover
  fires. Starting the load *during* the window instead fails at *connect* with
  `HA_NO_ACTIVE_CLUSTER` (no ACTIVE to resolve) and never reaches the server
  block. Grep the block on the region's owning RS (log path is now
  `/data/hbase-logs/hbase.log*` after the log-rollover change):
  ```
  for p in $(kubectl -n cluster-a get pods -l app=regionserver -o name | sed 's|pod/||'); do
    kubectl -n cluster-a exec "$p" -- bash -lc \
      'grep -H "Blocking Mutation" /data/hbase-logs/hbase.log* 2>/dev/null' | sed "s|^|$p: |"
  done
  ```
  Any UPSERT that returned success before the block must be present on cluster-b
  post-failover.
- **Cross-cluster data validation (§2.2).** After the load finishes and writes
  quiesce, confirm no rows were lost or left un-replicated:
  ```
  ./utils/validate-replication.sh          # expect: PASS: clusters in sync
  ```

**Teardown**
- `kill $LOAD_PID`.

---

### 5.2 Network-fault scenarios

#### S2. Standby recovery after transient network partition

**Exercises:** `Active → Active (sync replication degraded internally)` —
writer state machine `SYNC → STORE_AND_FORWARD → SYNC_AND_FORWARD → SYNC`.
Peer state: `Standby → DegradedStandby → Standby`.

**Hot path:** `SyncModeImpl.onFailure()`, `StoreAndForwardModeImpl`,
`SyncAndForwardModeImpl`, `ReplicationShardDirectoryManager` (OUT dir),
`HAGroupStoreManager.setHAGroupStatusToStoreAndForward()`.

**Fault to inject:** block cluster-a → cluster-b NameNode for 2 minutes,
then unblock.

**Tool:** Toxiproxy (preferred — you get clean latency control too) or
NetworkPolicy (binary up/down).

**STATUS:** writer state machine implemented; store-and-forward path under
`org.apache.phoenix.replication.StoreAndForwardModeImpl` is in the jar.
Requires §2 step 6 (`SYSTEM.HA_GROUP` row bootstrapped) before execution.

**Setup**
1. Prereqs met, S1-style table created on both clusters.
2. Deploy Toxiproxy in cluster-a (see §6.3). Point the peer-HDFS URL
   through it by **updating the `SYSTEM.HA_GROUP` row** from §2 step 6:
   set `HDFS_URL_2` (cluster-b's entry, from cluster-a's point of view)
   to `hdfs://toxiproxy.cluster-a.svc.cluster.local:9000/phoenixHA`.
   Recycle the cluster-a RegionServer so `HAGroupStoreClient` picks up
   the new record.
3. With toxiproxy healthy and no toxics, confirm sync replication works by
   writing one row on cluster-a and observing a replication log file appear
   in cluster-b's `/phoenixHA/testHAGroup/in`.

**Execute**
1. Cut the path to cluster-b:
   ```
   kubectl -n cluster-a exec toxiproxy-0 -- \
     /toxiproxy-cli toxic add remote-hdfs -t timeout -a timeout=1
   ```
2. Write on cluster-a across the 2-min outage (form (a), §2.1). This is the
   server-side store-and-forward path, so write **directly** to cluster-a — not
   the HA URL. Per-row sqlline is too slow (~10 s JVM cold-start each), so drive
   the trickle as a handful of batches spaced across the window: 8 batches of 30
   rows, ~15 s apart:
   ```
   kubectl -n cluster-a exec -i hmaster-0 -- bash -lc '
     for b in $(seq 0 7); do
       { for j in $(seq 1 30); do
           i=$((b*30 + j))
           printf "UPSERT INTO PHOENIX_HA_T VALUES ('"'"'r-snf-%d'"'"', '"'"'v-%d'"'"');\n" $i $i
         done; echo "!quit"; } | /trucke/sqlline.sh "$ZOOKEEPER_QUORUM" 2>/dev/null
       sleep 15
     done
   '
   ```
3. Confirm cluster-a switched to `STORE_AND_FORWARD`:
   ```
   kubectl -n cluster-a exec hmaster-0 -- /hadoop/bin/hdfs dfs -ls /phoenixHA/testHAGroup/out
   ```
   Must show one or more log files accumulating.
4. Restore connectivity:
   ```
   kubectl -n cluster-a exec toxiproxy-0 -- /toxiproxy-cli toxic remove remote-hdfs -n timeout
   ```

**Verify**
- Within ~30s of step 4, files in `/phoenixHA/testHAGroup/out` on cluster-a
  start disappearing (drained to cluster-b's IN).
- Within ~60s, cluster-a's OUT is empty and the writer log line
  `SyncAndForwardModeImpl → SyncModeImpl` appears in
  `/data/hbase-logs/regionserver.log`.
- Peer state on cluster-b goes `DEGRADED_STANDBY → STANDBY`:
  ```
  kubectl -n cluster-b exec -i hmaster-0 -- \
    /hbase/bin/hbase org.apache.phoenix.jdbc.PhoenixHAAdminTool \
      get-cluster-role-record -g testHAGroup
  ```
- Client writes during the outage all returned success (no timeouts).
- **Data-consistency check (post-drain):** after OUT drains, every row
  written during the outage is present on cluster-b with its original
  mutation timestamp (no ghost writes from retry-loop reordering, no
  missed records from the SYNC → STORE_AND_FORWARD switch). Sample at
  least the first, middle, and last rows explicitly:
  ```
  kubectl -n cluster-b exec -i hmaster-0 -- bash -lc '
    /trucke/sqlline.sh "$ZOOKEEPER_QUORUM" <<SQL 2>/dev/null
  SELECT * FROM PHOENIX_HA_T WHERE ID IN ('\''r-snf-1'\'', '\''r-snf-120'\'', '\''r-snf-240'\'');
  SELECT COUNT(*) FROM PHOENIX_HA_T WHERE ID LIKE '\''r-snf-%'\'';
  !quit
  SQL
  '
  ```
  All three sampled rows must be present, and the count must equal the number
  of rows the loop committed (240).

**Teardown**
- `kubectl -n cluster-a delete pod toxiproxy-0` if you want to remove the
  proxy entirely.

---

#### S7. Standby cluster offline during active write

**Exercises:** `Active` stays `Active`; writer goes `SYNC → STORE_AND_FORWARD`
and stays there until standby returns.

**Hot path:** same as S2 but the fault is at the cluster level (no HDFS at
all), so the retry loop in `ReplicationLog` exhausts and forces a state
change without an intermediate latency blip.

**Fault to inject:** scale cluster-b's namenode and datanode to 0.

**Tool:** `kubectl scale`.

**STATUS:** same as S2.

**Setup**
1. Prereqs, table created on both clusters.
2. Baseline: verify one write replicates.

**Execute**
1. Tear down cluster-b HDFS:
   ```
   kubectl -n cluster-b scale sts/datanode --replicas=0
   kubectl -n cluster-b scale sts/namenode --replicas=0
   ```
2. Write 100 rows on cluster-a in a single sqlline run (form (a), §2.1 — one
   JVM, not one per row). Should succeed locally, with the OUT queue growing:
   ```
   kubectl -n cluster-a exec -i hmaster-0 -- bash -lc '
     { for i in $(seq 1 100); do
         printf "UPSERT INTO PHOENIX_HA_T VALUES ('"'"'r-offline-%d'"'"', '"'"'v-%d'"'"');\n" $i $i
       done; echo "!quit"; } | /trucke/sqlline.sh "$ZOOKEEPER_QUORUM" 2>/dev/null
   '
   kubectl -n cluster-a exec hmaster-0 -- /hadoop/bin/hdfs dfs -ls /phoenixHA/testHAGroup/out
   ```
3. Bring cluster-b back up:
   ```
   kubectl -n cluster-b scale sts/namenode --replicas=1
   kubectl -n cluster-b scale sts/datanode --replicas=1
   kubectl -n cluster-b rollout status sts/namenode sts/datanode --timeout=120s
   ```

**Verify**
- No client write returned an error during step 2.
- OUT queue on cluster-a drains within ~2 min of cluster-b's return.
- Scan on cluster-b after drain shows all 100 `r-offline-*` rows.
- Peer state moved `Standby → DegradedStandby → Standby` (check the znode).

**Teardown**
- Drop the rows written, or let them persist for S10.

---

#### S7-mtime. The store-and-forward mtime heartbeat and the sync re-entry gate

**What this covers:** *why* the writer does not flip back to SYNC the instant
the backlog drains, and how that gate is driven off the HAGroupStoreRecord
znode's **mtime**. This is a mechanism note attached to S7 (and every
store-and-forward scenario), not a separate fault to inject.

**The heartbeat (mtime keep-alive while degraded).** On entering
`STORE_AND_FORWARD`, `StoreAndForwardModeImpl.onEnter()` starts a single-thread
scheduled executor (`StoreAndForwardStatusUpdate-<haGroup>`) that calls
`ReplicationLogGroup.setHAGroupStatusToStoreAndForward()` at a fixed rate of
**0.7 × `zookeeper.session.timeout`** (`HA_GROUP_STORE_UPDATE_MULTIPLIER`,
`StoreAndForwardModeImpl.java:46,66-84`). With the default 90 s session timeout
this is **63 000 ms**. Each call rewrites the record znode
(`/phoenix/consistentHA/<haGroup>`) to `ACTIVE_NOT_IN_SYNC`, which **bumps the
znode's mtime** (and increments its `dataVersion`). Re-stamping at 0.7× the
session timeout guarantees the mtime never ages past one session's worth while
the cluster is genuinely degraded.

**The stop (mtime starts aging).** On leaving the mode — the
`STORE_AND_FORWARD → SYNC_AND_FORWARD` transition — `onExit()` calls
`stopHAGroupStoreUpdateTask()` (`StoreAndForwardModeImpl.java:89-109`), shutting
the executor down. From that instant the mtime is **no longer refreshed** and
begins to age.

**The gate (`waitTime` off mtime).** When the forwarder finishes draining
(`ReplicationLogDiscoveryForwarder.processNoMoreRoundsLeft()`), it calls
`logGroup.setHAGroupStatusToSync()` → `HAGroupStoreClient.setHAGroupStatusIfNeeded(ACTIVE_IN_SYNC)`
→ `validateTransitionAndGetWaitTime()`
(`HAGroupStoreClient.java:1183-1202`):

```java
if (currentHAGroupState == ACTIVE_NOT_IN_SYNC && newHAGroupState == ACTIVE_IN_SYNC ...)
    waitTime = waitTimeForSyncModeInMs;
long remainingTime = currentHAGroupStoreRecordMtime + waitTime - System.currentTimeMillis();
return Math.max(0, remainingTime);      // 0  ⇒ flip allowed now
```

The flip to sync is admitted only once **`now ≥ mtime + waitTimeForSyncModeInMs`**
— i.e. enough wall-clock has elapsed *since the last mtime bump*. If it has not,
the call returns the positive remaining time **without touching ZK**
(`HAGroupStoreClient.java:404-407`), the forwarder logs
`HAGroup … will try to update HA state to sync at <ts>`
(`ReplicationLogDiscoveryForwarder.java:138`, deferred retry), and it re-checks
next round. If it has, the ZK write goes through, `waitTime == 0` is returned
(`:453`), and the forwarder logs `HAGroup … updated HA state to SYNC` (`:141`).

**Why this design:** the heartbeat + gate together prevent a spurious flap back
to SYNC. As long as the degraded writer is alive it keeps the mtime fresh, so
the gate stays closed; the earliest sync re-entry can happen is
`waitTimeForSyncModeInMs` *after the writer stopped heartbeating* — a settle
window that ensures the backlog is truly drained and the peer is caught up
before live sync resumes.

**Verified live on the test-bed (2026-07-19, cluster-a as ACTIVE):**
- RS log: heartbeat `started haGroupStoreUpdateExecutor with interval 63000ms`
  at `02:58:17` (entered STORE_AND_FORWARD) and
  `stopped haGroupStoreUpdateExecutor` at `03:07:09`
  (SYNC_AND_FORWARD transition). 63 000 ms = 0.7 × 90 s, as computed.
- Znode Stat on `/phoenix/consistentHA/testHAGroup` (read via `zkCli stat`):
  `mtime = Sun Jul 19 03:13:09 UTC`, `dataVersion = 32`,
  `haGroupState = ACTIVE_IN_SYNC`. The rising `dataVersion` is the heartbeat's
  fingerprint (every 63 s re-stamp plus mode transitions); the `mtime` holds
  only the **latest** write, which is the `03:13:09` sync-flip itself.
- The gate ran just *before* that write, when the mtime still reflected the
  `03:07:09` last heartbeat bump. Nothing touched the znode in the 03:07:09 →
  03:13:09 window (heartbeat already dead), so the mtime aged a clean **~6 min**
  — far past `waitTimeForSyncModeInMs` (default 60 s) — hence
  `remainingTime ≤ 0`, `waitTime == 0`, and the `updated HA state to SYNC`
  branch fired (never the deferred `will try to update HA state to sync at`
  line). *That* write is what advanced mtime to 03:13:09.

**Caveat when reading it back:** because mtime is overwritten on every write, a
live `stat` shows only the final flip time, not the intermediate 63 s heartbeat
mtimes. Two ways to see the heartbeats stepping forward:

*(a) Live poll* — poll `stat /phoenix/consistentHA/<haGroup>` on the active
cluster's ZK on a < 63 s cadence while it sits in STORE_AND_FORWARD (see S7
execute step 2), and watch `mtime` / `dataVersion` advance each interval; they
freeze the moment the mode exits.

*(b) After the fact, from the ZK transaction log (no re-run needed)* — every
`setData` is durably recorded in the ZK txn log with its own wall-clock
timestamp and zxid, so the intermediate heartbeats that a live `stat`
overwrites are fully recoverable. On the ZK pod:

```
CP=$(echo /zookeeper/lib/*.jar | tr ' ' ':')
java -cp "$CP" org.apache.zookeeper.server.persistence.TxnLogToolkit \
  -d /data/zk/version-2/log.1 2>/dev/null \
  | tr -c '[:print:]\n' ' ' \
  | grep 'setData' | grep 'consistentHA/<haGroup>'
```

(the `tr` scrubs the binary data payload so grep treats the line as text; the
trailing integer on each line is the resulting `dataVersion`).

**Verified from the txn log (2026-07-19 cycle, session `0x…0e` = the RS holding
the writer):** the heartbeat re-stamped `ACTIVE_NOT_IN_SYNC` on an exact **63 s
cadence** — `02:58:17` (zxid 0x153, enter STORE_AND_FORWARD) → 02:59:20 →
03:00:23 → 03:01:26 → 03:02:29 → 03:03:32 → 03:04:35 → 03:05:38 →
**03:06:41 (zxid 0x15f, last heartbeat)**. The scheduled `03:07:44` tick never
fired because `stopHAGroupStoreUpdateTask` shut the executor at `03:07:09` (RS
log). Then a clean **6 m 28 s** gap with zero writes to the node, and the sync
flip at **03:13:09 (zxid 0x166, `ACTIVE_IN_SYNC`, dataVersion 32)**. The gate
ran against the `03:06:41` mtime; 388 s ≫ 60 s `waitTimeForSyncModeInMs` ⇒
`remainingTime ≤ 0` ⇒ `waitTime == 0` ⇒ immediate flip, no deferred retry. The
final `dataVersion 32` matches the live `stat`.

---

#### S7-orphan. Multi-writer store-and-forward, and the orphaned in-progress plog that wedges SYNC re-entry

**What this covers:** a multi-region variant of S7 that (a) confirms every
RegionServer hosting scoped regions independently drives the store-and-forward
machinery — not just the single-region RS — and (b) surfaces a real defect: a
single orphaned in-progress `.plog` can hold an HA group in `SYNC_AND_FORWARD`
for tens of minutes **after the data is fully replicated**, because the
SYNC re-entry precondition is gated on the in-progress directory being empty.

**Setup that makes it multi-writer.** The earlier S7/S7-mtime runs used a
single-region table (`PHOENIX_HA_T`), so exactly one RS ever built a
`ReplicationLogGroup` and only that RS stamped the znode. To exercise the
multi-writer path, create a **salted** table so its regions spread across all
RSes:

```sql
CREATE TABLE PHOENIX_HA_MR (ID VARCHAR PRIMARY KEY, C VARCHAR)
  SALT_BUCKETS=6, COLUMN_ENCODED_BYTES=0;   -- on BOTH clusters; schemas don't replicate
```

`SALT_BUCKETS=6` → 6 regions, ~2 per RS across a 3-RS cluster; the salt byte
hashes even monotonic load-loop keys across all buckets, so **every** RS takes
concurrent scoped writes (no single hot region). Seed ~500 K rows of ~256 B
values (~130 MB on disk) via the HA connection, then run `utils/ha-loadloop.sh`
with `TABLE=PHOENIX_HA_MR` under sustained load.

**Finding 1 — the flip trigger is `.plog` *rotation*, not the write path, and
it takes 5 consecutive failures.** Take the STANDBY namenode down
(`kubectl -n cluster-b scale sts/namenode --replicas=0`). Live writes on the
ACTIVE keep flowing for **minutes** afterward: the current `.plog` writer holds
an already-allocated HDFS block, and the DFS write pipeline streams straight to
the datanodes with no namenode involvement. The namenode is only consulted to
**rotate** the log (create the next file + `addBlock`). So the failure surfaces
only on the next rotation, and even then not immediately —
`ReplicationLog` retries the rotation once per ~60 s rotation cycle and only
gives up after a bounded budget:

```
04:41:59 ERROR ReplicationLog: Failed to create new writer for rotation (attempt 1/5), retrying...
04:42:59 ERROR ...(attempt 2/5)...   04:43:59 (3/5)   04:44:59 (4/5)
04:45:59 ERROR ReplicationLog: Too many rotation failures (5/5), closing log
04:47:43 INFO  StoreAndForwardModeImpl: HAGroup testHAGroup entered mode STORE_AND_FORWARD
04:47:43 INFO  StoreAndForwardModeImpl: ... started haGroupStoreUpdateExecutor with interval 63000ms
```

i.e. ~4–6 minutes of connection-refused retries elapse between the namenode
going down and the SAF flip. **Do not expect the mode to change the instant the
peer HDFS dies.**

**Finding 2 — the mtime heartbeat is genuinely multi-writer, and CAS
contention on the shared znode is transient/self-healing.** Each RS that enters
SAF starts its **own** `StoreAndForwardStatusUpdate-<haGroup>` executor
(63 000 ms), and all of them stamp the **same** record znode. The write is a
version-checked CAS (`setHAGroupStatusIfNeeded` →
`updateHAGroupStoreRecordInZooKeeper` with the RS's cached stat version). When
two RSes fire near-simultaneously, one wins the CAS and the loser throws:

```
StaleHAGroupStoreRecordVersionException: ... with cached stat version 40
Caused by: KeeperException$BadVersionException: BadVersion for /phoenix/consistentHA/testHAGroup
→ "HAGroup testHAGroup failed to set status to STORE_AND_FORWARD"
```

This is **benign and self-correcting**: `HAGroupStoreClient` holds a ZK watch on
the record, so after a `BadVersion` rejection the loser's cache refreshes and its
next heartbeat succeeds. In the 2026-07-19 run rs-2 failed its CAS exactly twice
(04:46:01, 04:48:07) during the ~2-minute window when all three RSes were
converging into SAF, then never again — once their 63 s phases staggered, their
CAS windows stopped overlapping. `dataVersion` climbing (32 → 76 over the
degraded window) is the *aggregate* of all three racing; the shared `mtime`
stays fresh regardless of which RS wins, so the S7-mtime gate keeps working.
**Caveat:** a reader grepping `failed to set status` could misread this as a
heartbeat outage — it is lock contention, not failure.

**Finding 3 (the defect) — an orphaned in-progress `.plog` wedges SYNC
re-entry long after data is caught up.** Bring the namenode back
(`scale --replicas=1`). All RSes walk `STORE_AND_FORWARD → SYNC_AND_FORWARD`
promptly (writers flip their target path from `.../out/shard/...` — the local
fallback buffer — back to `.../in/shard/...` on the peer), the `out/` buffer
drains to zero, and `lastRoundProcessed == lastRoundInSync` within ~1 minute.
**Yet the group stays in `SYNC_AND_FORWARD` for ~24 minutes** and the definitive
`ReplicationLogDiscoveryForwarder: ... updated HA state to SYNC` line never
appears.

Root cause chain (verified against source + logs):

1. The `SYNC_AND_FORWARD → SYNC` promotion fires **only** inside
   `ReplicationLogDiscoveryForwarder.processNoMoreRoundsLeft()`
   (`ReplicationLogDiscoveryForwarder.java:119-148`), whose precondition is
   `replicationLogTracker.getInProgressFiles().isEmpty()
    && getNewFilesForRound(nextRound).isEmpty()`. It is **not** gated on the
   `out/` file count directly, nor (given the tight, no-sleep `replay()` loop in
   `ReplicationLogDiscovery.java:208-227`) on round catch-up — with `out/` empty
   the pointer sits permanently within the `roundTimeMills + bufferMillis`
   (60 000 + 15 % = 69 000 ms) buffer window, so `processNoMoreRoundsLeft` is in
   fact reached every scheduled run.
2. A single 48-byte in-progress file lingered in
   `/phoenixHA/testHAGroup/out_progress/` —
   `1784436959980_regionserver-0...,16020,1784436416263_...plog`, owned by
   **rs-0's pre-crash server epoch** (`1784436416263`, from the S6 crash + SAF
   handoff). Its mere presence makes `getInProgressFiles().isEmpty()` false, so
   the promotion branch is skipped every round.
3. **Why it could not be forwarded — a destination-name collision baked into
   the forwarder's dst naming (the actual defect, not scan probability).**
   `processFile` (`ReplicationLogDiscoveryForwarder.java:96-116`) computes the
   peer destination as

   ```java
   long ts = replicationLogTracker.getFileTimestamp(srcStat.getPath());          // the SOURCE file's round-ts
   Path dst = remoteShardManager.getWriterPath(ts, forwardingServerName);        // in/shard/NNN/<round-ts>_<FORWARDING-server>.plog
   FileUtil.copy(srcFS, srcStat, remoteFS, dst, false /*deleteSource*/, false /*overwrite*/, conf);
   ```

   The dst filename is keyed on the **source file's round-timestamp** and the
   **forwarding RS's own server name** — it *discards the original writer's
   identity*. Several `.plog`s were rotated in the same ~60 s SAF-handoff window
   and therefore share the round-ts `1784436959980`; on a single forwarding RS
   they all map to the **identical** dst
   `in/shard/039/1784436959980_<that-RS>.plog`. The forwarder copied one of them
   successfully, then every same-ts sibling — including rs-0's 48-byte orphan —
   hit `FileUtil.copy(overwrite=false)` → `checkDest` →
   **`PathExistsException: Target ... already exists`**. This is compounded by a
   **stale HDFS write-lease**: an earlier interrupted copy left a 0-byte phantom
   dst whose lease was never released, so the path was neither overwritable nor
   readable until HDFS lease recovery reclaimed the abandoned lease.
4. **`maxRetries=1` + probabilistic scan set the retry *cadence*, not the
   outcome.** `getInProgressFileMaxRetries()` = 1 (one attempt per round, then
   `reached max retries (1). Skipping for the rest of this round`,
   `ReplicationLogDiscovery.java:326-342`) and the probabilistic
   `shouldProcessInProgressDirectory()` draw vs
   `phoenix.replication.forwarder.in.progress.processing.probability`
   (`:267,279-282`) merely spaced the pickups ~5–10 min apart. The namenode
   returned at **04:56:48**; the 4 pickups of the wedged 48-byte file were:

   ```
   04:56:33 rs-1  ConnectException: Connection refused        (namenode still down; file still in out/)
   05:03:10 rs-2  PathExistsException: Target .../039/1784436959980_10.244.2.10,16020,1784330640875.plog already exists
   05:07:09 rs-1  PathExistsException: Target .../039/1784436959980_10.244.1.10,16020,1784330642120.plog already exists
   05:12:09 rs-2  FileNotFoundException ... inode 18256 [Lease. Holder: DFSClient_hb_rs_10.244.2.10,16020,1784330640875..., pending creates: 1]
                  + "Fail to recover lease for /phoenixHA/testHAGroup/in/shard/039/1784436959980_10.244.2.10,16020,1784330640875.plog"
   ```

   **The exact collision pair (from `cluster-a_regionserver-2` log, 1 ms apart).**
   rs-2, forwarding two *different* source files, computed the *same* dst because
   both share round-ts `1784436959980` and rs-2 stamps its own server name onto
   the dst:

   ```
   # 05:03:09,472  SUCCESS — rs-2 forwards ITS OWN file, creates the dst
   src = out_progress/1784436959980_10.244.2.10,16020,1784330640875_3c100d1c-..._1784437389463.plog   (235262 bytes)
   dst = in/shard/039/1784436959980_10.244.2.10,16020,1784330640875.plog

   # 05:03:10,454  FAIL — rs-2 forwards rs-0's ORPHAN, computes the identical dst
   src = out_progress/1784436959980_regionserver-0...,16020,1784436416263_5eae97d2-..._1784436992579.plog   (48 bytes)
   dst = in/shard/039/1784436959980_10.244.2.10,16020,1784330640875.plog   ← SAME PATH
   → PathExistsException: `.../1784436959980_10.244.2.10,16020,1784330640875.plog': Target ... already exists
   ```

   The two sources are unmistakably distinct — different origin RS
   (`10.244.2.10,...,1784330640875` vs `regionserver-0...,1784436416263`),
   different UUID, different size (235262 vs 48 bytes) — but `getWriterPath(ts,
   forwardingServerName)` keys the dst on the round-ts + the *forwarder's* name
   and drops the origin, so both collapse to one path. Note the dst is
   forwarder-specific: rs-1's 05:07 failure is the *same bug* on a *different*
   path (`..._10.244.1.10,16020,1784330642120.plog`, rs-1's own name), because
   rs-1 had already forwarded its own same-round-ts file to that path. The first
   three attempts were doomed by the collision + stale lease regardless of retry
   budget. Resolution was organic: at **05:22:09**, once HDFS lease recovery had
   freed the phantom dst, rs-2 landed the copy and the precondition cleared in the
   same millisecond:

```
05:22:09,018 Forwarder: Copying file src=...out_progress/1784436959980_regionserver-0...  dst=.../in/shard/039/... size=48 took 8ms
05:22:09,019 Forwarder: Processed all the replication log files for testHAGroup
05:22:09,022 Forwarder: HAGroup testHAGroup updated HA state to SYNC
```

Znode after: `haGroupState=ACTIVE_IN_SYNC`, `mtime=05:22:09`, `dataVersion=77`.

**STATUS:** ✅ **COMPLETED — verified end-to-end on the test-bed 2026-07-19.**
Multi-writer SAF + heartbeat + CAS-contention self-heal all behave as designed.
Finding 3 is **one bug** — a dst-name collision in the forwarder:
`getWriterPath(sourceTimestamp, forwardingServerName)` drops the origin-server
identity, so two `out_progress` files sharing a round-timestamp collapse onto one
dst; with `FileUtil.copy(overwrite=false)` the second is **permanently rejected**
(`PathExistsException`), and a partial prior copy can leave a dangling HDFS lease
(`pending creates: 1`) that blocks recovery until lease expiry. Fix by making the
dst unique per source file (include the original writer identity / a UUID), or
detect an already-forwarded same-content dst and treat it as success.

The prolonged `SYNC_AND_FORWARD` is the **correct symptom** of that one bug, not
a second defect. SYNC re-entry is gated on `getInProgressFiles().isEmpty()`, and
that gate is behaving exactly as designed: it must **not** promote to SYNC while a
file is genuinely unforwarded — that is the store-and-forward safety invariant.
So the group correctly refused to declare SYNC for as long as the collision kept
the orphan stuck. Weakening the gate (e.g. excluding "superseded-epoch" orphans)
would be *wrong* — it would let the group claim SYNC while a real file is still
unforwarded. The only fix is the dst-collision itself; once files forward
promptly, the gate clears promptly. Data was safe throughout (writes never
blocked, 483/483 load rounds rc=0, RPO zero — later confirmed by
`validate-replication` PASS, 644900 rows byte-for-byte); the prolonged state was
an accurate reflection of the stuck orphan, not a misrepresentation. Orphans of
this kind are produced precisely by an RS crash during the degraded window
(S6/S19), so this bug compounds those scenarios.

Logs for this run archived at
`hbase-local-testbed/captured-logs/s7-orphan-2026-07-19/` (both clusters' RS logs
+ a timeline README).

---

#### S17. Sync hangs past the sync timeout → RegionServer abort

**Exercises:** the Phoenix-layer `syncFuture.get(syncTimeoutMs)` timeout →
`PhoenixWALSyncTimeoutException` → explicit `Abortable.abort(...)` path.
Mirrors HBase's own `hbase.regionserver.wal.sync.timeout` semantics but
triggered from `ReplicationLogGroup.syncInternal()` rather than from
`WAL#sync`.

**Hot path:** `ReplicationLogGroup.syncInternal()` wait path
(`syncFuture.get(syncTimeoutMs, MILLISECONDS)`,
`ReplicationLogGroup.java:751`), `PhoenixWALSyncTimeoutException`
(constructed at `ReplicationLogGroup.java:764` and handed to `abort()`,
not thrown directly), `ReplicationLogGroup.abort()`
(`:1079-1090`) → `Abortable.abort()` (`:1087`, the `Abortable` is the
`RegionServerServices` captured via `@CoreCoprocessor` +
`HasRegionServerServices` in `IndexRegionObserver` at `:734-737`).

The timeout is `phoenix.replication.log.sync.timeout.ms`
(`REPLICATION_LOG_SYNC_TIMEOUT_KEY`). If unset it is **computed**, not a
fixed 5 min: `calculateSyncTimeout()` returns
`hbase.regionserver.wal.sync.timeout` (default 300000 ms) + the ZK session
timeout (`ReplicationLogGroup.java:584-589`). There is no
`phoenix.replication.writer.saf.sync.timeout.ms` key.

**Fault to inject:** a **mid-stream stall on the RS→peer-DataNode `:9866`
block stream** — the connection must stay established with a block open and its
ACKs withheld, so the in-flight `waitForAckedSeqno()` hangs and blocks the app
write thread's `syncFuture`. On the test-bed this is a `tc netem delay` on the
region-host RS's peer-DN `:9866` egress (see
`S17_Sync_Timeout_RS_Abort_Needs_MidStream_Stall.md` for the full recipe). Set
`phoenix.replication.log.sync.timeout.ms` low (e.g. 30000) to shorten the wait.

**Do NOT use the toxiproxy `timeout` toxic on `remote-hdfs` (`:9000`) for this
scenario, and do NOT use an `iptables DROP`/blackhole.** Toxiproxy fronts only
the peer **NameNode** `:9000`; block **data** streams RS→DN directly on `:9866`
and is never proxied, so a `:9000` toxic only stalls NN metadata (handled on a
background mode-init thread → SAF). A DROP refuses the peer-DN connection →
pipeline **setup** failure → also SAF. Neither blocks the application thread, so
neither drives the abort. (Confirmed on the test-bed 2026-08-08 — a drop, at any
timing, degrades to graceful SAF.)

**Tool:** `tc netem` (via `nsenter` into the region-host RS's net namespace on
the kind node). The EBS `pause-volume-io` on the standby's storage is the AWS/FIS
equivalent.

**STATUS:** implemented. `syncInternal()` wraps the timeout as
`PhoenixWALSyncTimeoutException` and `abort()` invokes the real RS abort
via the `Abortable` captured from `HasRegionServerServices`
(`IndexRegionObserver.java:734-737`).

**Setup**
1. Prereqs + table.
2. Deploy toxiproxy in cluster-a and point `HDFS_URL_2` at it (§6.3 and
   §2 step 6). Recycle cluster-a RS so the new peer URL takes effect.
3. With no toxics applied, verify one row replicates end-to-end.

**Execute** (full recipe in `S17_Sync_Timeout_RS_Abort_Needs_MidStream_Stall.md`)
1. Identify the RS hosting the `PHOENIX_HA_T` region (all HA-connection writes
   funnel there, so the abortable `syncFuture` lives only on that RS) and its
   host-side PID on the kind node via `crictl inspect`.
2. Apply a mid-stream stall scoped to that RS's peer-DN `:9866` egress only —
   `prio` qdisc + `netem delay` on a band, `u32` filters on the cluster-b
   DataNode IPs — inside the pod's net namespace:
   ```
   docker exec <node> nsenter -t <hpid> -n tc qdisc add dev eth0 root handle 1: prio
   docker exec <node> nsenter -t <hpid> -n tc qdisc add dev eth0 parent 1:3 handle 30: netem delay 120000ms
   for dst in <cluster-b datanode podIPs>; do
     docker exec <node> nsenter -t <hpid> -n tc filter add dev eth0 protocol ip parent 1:0 prio 1 u32 \
       match ip dst $dst/32 match ip dport 9866 0xffff flowid 1:3
   done
   ```
   (Scope to `:9866` only — delaying ZK/HMaster RPC would abort for the wrong
   reason. Verify with `tc -s qdisc show` that the netem backlog holds packets.)
3. Keep HA-connection load running on `toxiHAGroup` so a peer block stays open and
   mid-flush when the delay engages. `syncFuture.get(syncTimeoutMs)` in
   `syncInternal()` then counts down on the app write thread; the abort fires
   ~`syncTimeoutMs` (set it to 30000 to shorten) after the stall lands.

**Verify**
- Once the sync timeout elapses, cluster-a's regionserver pod transitions
  to Terminating / Restarting:
  ```
  kubectl -n cluster-a get pod regionserver-0 -w
  ```
- The regionserver log contains the abort message with
  `PhoenixWALSyncTimeoutException` as the cause and the abort reason string
  from `ReplicationLogGroup.syncInternal()`:
  ```
  kubectl -n cluster-a logs regionserver-0 --previous | \
    grep -E 'ABORT|PhoenixWALSyncTimeoutException'
  ```
- The client that issued the write received a
  `PhoenixWALSyncTimeoutException` (not a generic `IOException`).
- After the RS comes back Ready and regions reassign:
  - Remove the stall: `tc qdisc del dev eth0 root` in the RS's net namespace
    (`docker exec <node> nsenter -t <hpid> -n tc qdisc del dev eth0 root`).
  - Rows written **before** the hang are on cluster-b (synced pre-hang).
  - The in-flight row that triggered the abort is replayed to cluster-b
    via `preWALRestore` on region reopen — verify it lands within
    ~60s of the RS returning.
  - No silent data loss.
  - The group goes `ACTIVE_NOT_IN_SYNC` transiently, then self-reconverges to
    `ACTIVE_IN_SYNC` with no writes or manual recycle (forwarder-fix behaviour).

**Teardown**
- `tc qdisc del dev eth0 root` in the affected RS's net namespace; confirm no
  stray `netem`/`DROP` rules remain on any node.

---

#### S17b. DataNode block-write hang during active sync (peer-DN pipeline timeout, not NameNode/lease)

**Exercises:** the peer-HDFS **block-data write** failure path — distinct from
every toxiproxy-based scenario (S2/S12/S17), which fault only the NameNode. When
the ACTIVE writer's `hsync`/`append` of a replication `.plog` stalls because the
DataNode pipeline to the *standby* cluster goes unresponsive, the DFSClient
`DataStreamer` blocks until its socket/ack timeout, then fails pipeline recovery
→ the sync errors → the writer flips to `STORE_AND_FORWARD`. This is the fault
S17's own text calls out as "still pending" (the HDFS client's
`dfs.datanode.socket.write.timeout`), and it is the write-triggered SAF that
completes the picture alongside S2's reader-triggered (lease-recovery) SAF.

**Why toxiproxy cannot reach this:** toxiproxy fronts only cluster-b's
**NameNode** (`:9000`). A DFSClient uses the NN for metadata RPCs (`addBlock`,
`complete`, lease, rotation) but streams the actual **block data** RS→DataNode
directly on **`:9866`**, learned from the NN and never proxied. That is exactly
why in S2 the in-flight appends kept succeeding until the *reader* revoked the
lease — the data pipeline was never touched. To fault the write itself we must
interpose on `:9866`, which lives outside the proxy.

**Hot path:** `ReplicationLogGroup` sync/rotation → HDFS `DFSOutputStream.hsync`
→ `DataStreamer` pipeline to cluster-b DataNodes on `:9866`. On stall:
`dfs.client.socket-timeout` / ack timeout → `DataStreamer` pipeline error →
`setupPipelineForAppendOrRecovery` also blocked → sync throws → `SyncModeImpl`
error → `SYNC → STORE_AND_FORWARD` (writes buffer to local `out/`; ACTIVE never
blocks). Contrast S2 (reader `recoverLease` → `LeaseExpired` on the NN path) and
S17 (Phoenix `syncTimeoutMs` → RS *abort*). S17b's expected terminal state is
SAF, **not** an abort.

> **Correction (test-bed, 2026-08-08):** an earlier version of this note claimed
> that lowering `phoenix.replication.log.sync.timeout.ms` below the HDFS socket
> timeout would "escalate" this DROP fault into the S17 abort path. That is
> **wrong.** A DROP fails the peer connection at pipeline *setup*
> (`createBlockOutputStream`), which is caught on a background mode-init/rotation
> thread and gracefully demoted to STORE_AND_FORWARD — it never blocks the
> application write thread's `syncFuture`, so the sync-timeout cannot fire, at
> any timeout value. The S17 abort requires a **mid-stream hold** on an
> *established* block (ACKs withheld so `waitForAckedSeqno` hangs), which is a
> `tc netem delay` / EBS `pause-volume-io`, not a drop. See
> `S17_Sync_Timeout_RS_Abort_Needs_MidStream_Stall.md`.

**Fault to inject:** a node-level packet **DROP** (silent, not REJECT — REJECT
would fail fast with connection-refused instead of a *timeout*) on the writer
RS → cluster-b-DataNode `:9866` path, scoped by source+dest so the writer's
**local** WAL pipeline (RS → *cluster-a* DataNodes, also `:9866`) stays healthy.
Blanket-dropping `:9866` would also break the local WAL sync — that is a
different scenario (S11), and would abort the RS on the local-WAL path instead.

**Tool:** node-level `iptables` (or `tc netem` for latency instead of a hard
drop) on the kind node hosting the writer RS. The kind nodes are docker
containers with `iptables`/`tc`/`nft` available and usable (same host access the
S6/S12 SIGKILL uses); the RS **pod** cannot self-inject (bare securityContext,
no `NET_ADMIN`, no `iptables`/`tc`). Chaos Mesh `NetworkChaos`
(`action: delay`/`loss`, direction `to`, scoped to the peer DataNode selector)
is the declarative equivalent once Chaos Mesh is installed (task) — preferred
for repeatability; the raw-iptables recipe below needs no new infra.

**Setup**
1. Prereqs + table; HA load driving through the group under test (the faithful
   divergent-CRR `toxiHAGroup` rig is fine — S17b does not use the proxy, but
   reusing that group keeps the reader healthy). Verify replication
   end-to-end pre-fault; confirm the writer is in `SYNC`.
2. Identify the writer RS pod, its node, its pod IP, and the **cluster-b**
   DataNode pod IPs (the `:9866` targets). Capture the **cluster-a** DataNode
   IPs too — these must NOT be blocked (local WAL path).

**Execute**
1. On the writer RS's kind node, DROP egress to each cluster-b DataNode `:9866`,
   scoped to the writer's pod IP as source:
   ```
   for DN in <cluster-b DN IP list>; do
     docker exec <writer-node> iptables -I FORWARD \
       -s <writerPodIP> -d $DN -p tcp --dport 9866 -j DROP
   done
   ```
   (Use the `FORWARD` chain — inter-pod traffic is routed/forwarded by the node,
   not `OUTPUT`-originated. Verify the counters increment with
   `iptables -L FORWARD -n -v | grep 9866`.)
2. Keep writes flowing. In-flight `.plog` block writes to cluster-b stall; the
   local WAL sync (to cluster-a DataNodes) keeps committing — so writes stay
   `rc=0` at the client.
3. Wait for `DataStreamer` to hit its socket/ack timeout
   (`dfs.client.socket-timeout`, default 60s) and error the pipeline.

**Verify**
- cluster-a RS log shows a **DataNode-pipeline** failure on a
  `toxiHAGroup .../in/... .plog` block — e.g. `DataStreamer: Error Recovery for
  BP-...:blk_... in pipeline [DatanodeInfoWithStorage[<cluster-b DN>:9866,...]]`
  / `Slow ...`/`Broken pipe`/`SocketTimeoutException` — **not** a
  `LeaseExpiredException` and **not** a NameNode RPC failure. This is the
  discriminator proving the fault hit the block-data path, not the metadata/lease
  path.
- Writer flips `SYNC → STORE_AND_FORWARD` (`SyncModeImpl ... got error` at
  `mode=SYNC`), OUT queue grows, writes remain `rc=0`.
- Remove the DROP rules (`iptables -D FORWARD ...` for each, or flush the
  inserted rules); writer recovers `STORE_AND_FORWARD → SYNC_AND_FORWARD → SYNC`
  and OUT drains to 0.
- Zero data loss: prefix count parity on both clusters.
- **NOT an escalation path (corrected 2026-08-08):** lowering
  `phoenix.replication.log.sync.timeout.ms` does **not** turn this DROP into an
  S17 abort. A DROP fails at pipeline *setup* on a background thread → graceful
  SAF, never the app-thread `syncFuture`, at any timeout value. The S17 abort
  needs a mid-stream hold on an established block (`tc netem delay` /
  `pause-volume-io`), not a drop — see
  `S17_Sync_Timeout_RS_Abort_Needs_MidStream_Stall.md`.

**Teardown**
- Delete the inserted `iptables` rules on the node (or
  `iptables -F FORWARD` if no other rules matter — but prefer targeted `-D` so
  we don't wipe kind/CNI rules). Confirm the writer is back in `SYNC` and OUT is
  empty.

**STATUS:** planned — new scenario (2026-08-05), motivated by the observation
that no existing scenario faults the RS→peer-DataNode `:9866` block-write path;
all toxiproxy scenarios fault only the NameNode. Verifies the write-triggered
SAF (DN timeout) as the counterpart to S2's reader-triggered SAF (lease
recovery).

---

### 5.3 Cluster-down scenarios

#### S3. Failover during degraded standby (backlog in OUT)

**Exercises:** `Active → ActiveToStandby` *while* writer is in
`STORE_AND_FORWARD`; peer goes `DegradedStandby → StandbyToActive`
(via the normal, non-forced path — requires OUT to drain first).

**Hot path:** `StoreAndForwardModeImpl`, `ReplicationLogDiscoveryForwarder`
(OUT→peer IN copier), `HAGroupStoreManager` peer-state handlers.

**Fault to inject:** block cluster-a → cluster-b HDFS for ~60s, accumulate
backlog, initiate failover while backlog exists, then restore connectivity
during the drain phase.

**Tool:** NetworkPolicy + `kubectl`.

**STATUS:** store-and-forward drain + failover orchestration are under
active development. The store-and-forward side is wired
(`HAGroupStoreManager.setHAGroupStatusToStoreAndForward()` /
`setHAGroupStatusToSync()`), but the atomic "drain before promote"
interlock (`STANDBY_TO_ACTIVE → ACTIVE_IN_SYNC` gated on OUT draining) does
**not** yet exist as a dedicated method in `HAGroupStoreManager` — there is
no `waitForOutDrain`/`drainAndPromote`. Verify the drain gate has landed
before relying on this scenario's timing expectations.

**Setup**
1. Prereqs + table.
2. Install Calico + apply the "block cluster-b HDFS from cluster-a"
   NetworkPolicy from §6.2.

**Execute**
1. Write 1000 rows on cluster-a while cluster-b HDFS is blocked. Confirm
   OUT has accumulated:
   ```
   kubectl -n cluster-a exec hmaster-0 -- /hadoop/bin/hdfs dfs -count /phoenixHA/testHAGroup/out
   ```
2. Trigger failover (same znode set as S1).
3. *Immediately* remove the NetworkPolicy:
   ```
   kubectl -n cluster-a delete networkpolicy block-cluster-b-hdfs
   ```
4. Watch state machine: cluster-a should park in `ActiveToStandby` until
   OUT is drained; then standby sees the opportunity to go
   `DegradedStandby → StandbyToActive → Active`.

**Verify**
- Failover takes longer than S1 because of the drain — measure wall-clock
  from znode set to `Active` on cluster-b, expect on the order of
  (backlog size / drain throughput) plus S1's baseline.
- After promotion, every row written in step 1 is present on cluster-b.
- Phoenix HA metrics (regionserver JMX, if wired) show non-zero
  `store_and_forward_drain_duration_ms`.

**Teardown**
- `kubectl delete networkpolicy -A --all` if needed.

---

#### S6. Active region server crash during sync replication

**Exercises:** no state change; WAL replay path must re-send the
replication log records for any mutation that was locally WAL-committed
but whose Phoenix replication record was not yet persisted.

**Hot path:** `IndexRegionObserver.postBatchMutateIndispensably` (interrupted),
`preWALRestore` hook (resumes replication during WAL split), region open path.

o**Fault to inject:** a *genuine, uncatchable* `SIGKILL` of the RegionServer
JVM **while a sustained write stream is in flight** — both conditions matter
(see the crash-injection gotcha below).

**Tool:** `docker exec <kind-node> kill -9 <host-pid-of-RS-JVM>` +
`utils/ha-loadloop.sh` for the sustained load. **NOT**
`kubectl delete pod --grace-period=0` (see gotcha).

**STATUS:** ✅ **COMPLETED — verified end-to-end on the test-bed 2026-07-19.**
Confirms the ACTIVE-side counterpart of S19: a crash mid-sync leaves an
unflushed WAL tail, HBase splits it into `recovered.edits`, and region reopen
replays them through `IndexRegionObserver.preWALRestore` →
`getHAGroupFromWALKey`, which (because the local role is `ACTIVE_IN_SYNC`)
takes the live-write `orElseThrow` branch of the S19 fix and re-ships to the
peer — no split-brain rejection, zero RPO. Evidence: WAL split wrote **26 101
edits** to `recovered.edits/0000000000000141076` (RS `RS_LOG_REPLAY_OPS`
worker); reopener logged `Replaying edits from … recovered.edits/…141076` and
`Opened … next sequenceid=141077`; `utils/validate-replication.sh` = **PASS
200638 rows byte-for-byte** across 141 write rounds spanning the crash, 0
client-visible write errors.

**Do NOT expect an `Initializing ReplicationLogGroup` line on the replay
thread.** `ReplicationLogGroup.get()` → `getOrCreate()` memoizes the group in a
per-RS `ConcurrentHashMap` (`INSTANCES.computeIfAbsent(instanceKey(serverName,
haGroupName), …)`); `init()` (and its `Initializing ReplicationLogGroup` log)
runs **only on the first cache miss** for that (serverName, haGroup). In this
run the reopener (regionserver-2) had *already* built and cached the group at
04:05:14 on an `RpcServer…Fifo.handler` write-RPC thread (the region briefly
lived there after the *first* crash). So the 04:15 replay's
`getHAGroupFromWALKey` was a **cache hit** — it returned the cached writer and
re-shipped through it with no init and no log line. Absence of the init line on
the `RS_OPEN_REGION` thread is a cache hit, not a missing re-ship. To *see* the
init on a replay thread you'd need the reopener to be an RS that had never
cached the group (e.g. a freshly restarted RS taking the region for the first
time).

**⚠️ Crash-injection gotcha (cost two false-negative runs):** getting a *real*
crash with an unflushed WAL tail is harder than it looks. Two traps:
1. **`kubectl delete pod --grace-period=0 --force` sends a catchable SIGTERM
   first.** HBase's RS shutdown hook catches it, flushes the memstore, and
   cleanly closes/archives its WAL before dying → the WAL-split dir is **empty**
   (`WAL count=0, no logs to split`), no `recovered.edits`, `preWALRestore`
   never fires. This is a graceful drain in a crash costume.
2. **`kill -9 1` *inside* the pod is silently dropped.** The RS JVM runs as PID
   1 in its container; the Linux PID-namespace pid-1 signal protection means a
   same-namespace sender (a `kubectl exec` shell) cannot deliver even SIGKILL
   to pid 1. The process never dies (restartCount stays 0).

   **The working recipe:** signal from an *ancestor* namespace — the kind node's
   host PID namespace:
   ```
   NODE=$(kubectl -n cluster-a get pod regionserver-0 -o jsonpath='{.spec.nodeName}')
   PID=$(docker exec $NODE bash -lc \
     "ps -eo pid,args | grep proc_regionserver | grep -v grep | grep regionserver-0 | awk '{print \$1}'")
   docker exec $NODE kill -9 $PID
   ```
   And keep `utils/ha-loadloop.sh` running throughout — a *quiesced* RS has
   nothing unflushed, so even a real SIGKILL yields an empty split. Sustained
   load guarantees a fresh unflushed tail at the instant of the kill.

**Note on reading it back:** don't `hdfs dfs -ls recovered.edits/` *after* the
region reopens — replay consumes and removes the edit files, leaving only a
0-byte `<seqid>.seqid` marker, which looks identical to the "empty split" case.
Confirm the split from the **RS log** instead: grep the `RS_LOG_REPLAY_OPS`
worker for `wrote N edits` and the reopener for `Replaying edits from …`.

**Setup**
1. Prereqs + table.
2. Start a high-rate write loop against cluster-a in the background (form (a),
   §2.1). One long-lived sqlline JVM streams UPSERTs indefinitely — the
   generator pipes into a single client, so the per-invocation cold-start cost
   is paid once:
   ```
   kubectl -n cluster-a exec -i hmaster-0 -- bash -lc '
     i=0; while true; do
       printf "UPSERT INTO PHOENIX_HA_T VALUES ('"'"'r-rs-%d'"'"', '"'"'v-%d'"'"');\n" $i $i
       i=$((i+1))
     done | /trucke/sqlline.sh "$ZOOKEEPER_QUORUM" 2>/dev/null
   ' &
   LOAD_PID=$!
   ```

**Execute**
1. Wait 10s to build up in-flight batches.
2. **(Optional but recommended for a deterministic reproduction)** Apply
   a brief toxiproxy `timeout` toxic on cluster-b's HDFS so at least one
   batch is stuck mid-sync at the moment of the kill. This forces the
   `preWALRestore` path to be the only route to cluster-b for that
   batch, rather than letting normal sync flow quietly handle it:
   ```
   kubectl -n cluster-a exec toxiproxy-0 -- \
     /toxiproxy-cli toxic add remote-hdfs -t timeout -a timeout=1
   ```
   Remove the toxic ~3s later so sync replication resumes on the new
   RS:
   ```
   sleep 3
   kubectl -n cluster-a exec toxiproxy-0 -- /toxiproxy-cli toxic remove remote-hdfs -n timeout
   ```
3. Kill the regionserver hard:
   ```
   kubectl -n cluster-a delete pod regionserver-0 --grace-period=0 --force
   ```
4. Wait for the pod to come back Ready (~30-60s).
5. Stop the load loop: `kill $LOAD_PID`.

**Verify**
- Cluster-a's regionserver log shows `preWALRestore` invocations during
  region reopen (grep `/data/hbase-logs/regionserver.log`).
- After the RS is back Ready, wait a minute for sync replication to drain,
  then compare `SELECT COUNT(*)` on both clusters (form (a), §2.1). They must
  match (no mutations lost on the standby due to the crash):
  ```
  for ns in cluster-a cluster-b; do
    echo "== $ns =="
    kubectl -n $ns exec -i hmaster-0 -- bash -lc '
      /trucke/sqlline.sh "$ZOOKEEPER_QUORUM" <<SQL 2>/dev/null
  SELECT COUNT(*) FROM PHOENIX_HA_T;
  !quit
  SQL
    '
  done
  ```
- No `MutationBlockedIOException` was raised — cluster-a never left the
  `Active` state.

**Teardown** — nothing beyond stopping the load.

---

#### S11. Local WAL sync fails — replication must not fire (ghost-write prevention)

**Exercises:** the dual-log coordination invariant from the design doc
§Dual Log Coordination: the Phoenix replication log write must not be
initiated until the local HBase WAL sync has succeeded. A ghost write
(record on standby that is not on the primary) would corrupt the HA
cluster's state.

**Hot path:** `HRegion.doWALAppend` (local WAL sync), the boundary
between local WAL sync completion and
`postBatchMutateIndispensably → replicateMutations()`.

**Fault to inject:** fail the **local** (cluster-a) HDFS during a write
so that the local WAL sync throws `IOException` / times out. This
happens **before** Phoenix replication is invoked.

**Tool:** Chaos Mesh `IOChaos` on cluster-a's DataNode (preferred —
can inject errors on specific file paths) or temporarily scale
cluster-a's DataNodes to `< dfs.replication.min` during a batch.

**STATUS:** tests a design-level invariant that has existed since the
replication path was wired. No plan dependency.

**Setup**
1. Prereqs + table; replication working end-to-end (verify one row
   replicates pre-fault).
2. Install Chaos Mesh (§6.4).

**Execute**
1. Apply IOChaos targeting cluster-a's local HDFS write path:
   ```yaml
   apiVersion: chaos-mesh.org/v1alpha1
   kind: IOChaos
   metadata: { name: local-wal-sync-fail, namespace: cluster-a }
   spec:
     action: fault
     mode: all
     selector:
       namespaces: [cluster-a]
       labelSelectors: { app: datanode }
     volumePath: /data
     path: "/data/hbase/**"
     errno: 5
     percent: 100
     duration: "30s"
   ```
2. Attempt a write on cluster-a during the fault window (form (a), §2.1 —
   direct to cluster-a; this UPSERT is expected to fail on the local WAL sync):
   ```
   kubectl -n cluster-a exec -i hmaster-0 -- bash -lc '
     /trucke/sqlline.sh "$ZOOKEEPER_QUORUM" <<SQL 2>/dev/null
   UPSERT INTO PHOENIX_HA_T VALUES ('\''r-ghost-guard'\'', '\''should-not-appear-on-b'\'');
   !quit
   SQL
   '
   ```
3. Wait for the chaos to clear (30s).

**Verify**
- The write in step 2 returned an error to the client (local WAL sync
  failure surfaces as an IOException via the RPC response).
- **No** replication log record for `r-ghost-guard` exists in cluster-b's
  `/phoenixHA/testHAGroup/in`. Grep a log reader trace for the row key.
  Alternatively, a `SELECT` on cluster-b (form (a), §2.1) must return zero rows:
  ```
  kubectl -n cluster-b exec -i hmaster-0 -- bash -lc '
    /trucke/sqlline.sh "$ZOOKEEPER_QUORUM" <<SQL 2>/dev/null
  SELECT * FROM PHOENIX_HA_T WHERE ID = '\''r-ghost-guard'\'';
  !quit
  SQL
  '
  ```
- Cluster-a's regionserver log shows the local WAL sync error but no
  corresponding `replicateMutations` call for that batch.
- After the fault clears, a fresh write on cluster-a replicates normally
  — the RS did not abort (a regular local-WAL sync IOException is not
  fatal; only a sync-timeout `PhoenixWALSyncTimeoutException` (default ~5 min) aborts per HBase
  semantics).

**Teardown**
- Chaos Mesh auto-removes after duration.

---

#### S12. Replication log write fails after local WAL sync succeeds (WAL-replay re-send)

**Exercises:** the design doc's §"Dual Log Coordination → WAL split
handling" contract: if the Phoenix replication log write fails after the
local WAL sync has already committed, the corresponding mutation must
be re-shipped to standby via `preWALRestore` during WAL replay.

**Hot path:** `postBatchMutateIndispensably → replicateMutations()` (the
remote sync fails here), `IndexRegionObserver.preWALRestore` (re-ship on
region reopen).

**Fault to inject:** toxiproxy `timeout` toxic on cluster-b HDFS during
an individual batch, targeted so the remote write fails but the local
WAL sync (which happens first, in cluster-a) has already succeeded.
Then force a RegionServer bounce to trigger WAL replay — this is what
actually exercises `preWALRestore`.

**Tool:** Toxiproxy + `kubectl delete pod`.

**STATUS:** `preWALRestore` hook wiring is the piece to verify before
running — same STATUS caveat as S6.

**Setup**
1. Prereqs + table; toxiproxy deployed pointing at cluster-b HDFS;
   replication verified end-to-end pre-fault.

**Execute**
1. Apply a short-window timeout on cluster-b HDFS:
   ```
   kubectl -n cluster-a exec toxiproxy-0 -- \
     /toxiproxy-cli toxic add remote-hdfs -t timeout -a timeout=1
   ```
2. Write a row on cluster-a (form (a), §2.1 — direct to cluster-a):
   ```
   kubectl -n cluster-a exec -i hmaster-0 -- bash -lc '
     /trucke/sqlline.sh "$ZOOKEEPER_QUORUM" <<SQL 2>/dev/null
   UPSERT INTO PHOENIX_HA_T VALUES ('\''r-replay-resend'\'', '\''must-appear-after-replay'\'');
   !quit
   SQL
   '
   ```
   The local WAL sync on cluster-a succeeds; the remote Phoenix
   replication write fails after retries exhaust (SAF flip may fire).
3. Immediately kill the active RS before SAF has had time to catch up
   via the forwarder:
   ```
   kubectl -n cluster-a delete pod regionserver-0 --grace-period=0 --force
   ```
4. Remove the toxic once the new RS starts coming back up:
   ```
   kubectl -n cluster-a exec toxiproxy-0 -- /toxiproxy-cli toxic remove remote-hdfs -n timeout
   ```
5. Wait for the RS to be Ready again and for WAL replay to complete
   (~60-90s).

**Verify**
- The regionserver log for the **new** RS contains `preWALRestore`
  invocations during region open, each driving a `replicateMutations`
  call.
- A `SELECT` for `r-replay-resend` on cluster-b (form (a), §2.1) returns the
  written value within ~90s of the RS coming back Ready. The record that was in
  the WAL but not yet replicated is successfully re-shipped via the WAL-replay
  path:
  ```
  kubectl -n cluster-b exec -i hmaster-0 -- bash -lc '
    /trucke/sqlline.sh "$ZOOKEEPER_QUORUM" <<SQL 2>/dev/null
  SELECT * FROM PHOENIX_HA_T WHERE ID = '\''r-replay-resend'\'';
  !quit
  SQL
  '
  ```
- Optional: also verify this works when combined with OUT-queue drain
  (record may travel via SAF forwarder OR `preWALRestore`, depending on
  timing). Either path satisfies zero-RPO — only "neither path ships
  the record" is a failure.

**Teardown**
- Remove any lingering toxics.

---

### 5.4 Transitional / race scenarios

#### S5. Abort failover mid-transition

**Exercises:** `Active → ActiveToStandby → AbortToActive → Active`, with
peer oscillating `Standby → StandbyToActive → Standby`.

**Hot path:** `HAGroupStoreManager.setHAGroupStatusToAbortToStandby()`
(`:435`; there is no `abortFailover` method — the abort is modeled by the
`ABORT_TO_*` states in `HAGroupStoreRecord.HAGroupState`), ZK record update
for the revert, `FailoverPhoenixConnection` (client sees the HA exception
briefly and retries).

**Fault to inject:** operator issues an abort shortly after initiating a
failover, while cluster-b is still replaying logs.

**Tool:** `kubectl` + `PhoenixHAAdminTool` (`initiate-failover` /
`abort-failover`).

**STATUS:** ✅ **COMPLETED — verified on the test-bed 2026-07-17** (Phoenix
branch `mode-state-fix`). Caught the promoting cluster mid-transition in
`STANDBY_TO_ACTIVE`, fired `abort-failover`, and it reverted cleanly to
`STANDBY` — **never reached `ACTIVE`** (`Initial State: STANDBY_TO_ACTIVE →
Final State: STANDBY`, abort completed in 0s). The old ACTIVE returned to
`ACTIVE_IN_SYNC` and logged `HAGroup testHAGroup returned to ACTIVE_IN_SYNC;
resuming rotation and SYNC mode` (`ReplicationLogGroup`) — the mirror of the
PHOENIX-7562 `failoverPending` rotation-suspend, confirming the writer-side
quiesce is reversible and doesn't leave rotation stuck off (which would
re-arm the original deadlock). All background HA-load rounds returned rc=0
(the abort collapsed the `ACTIVE_TO_STANDBY` fence window too fast for a
write to land in it), and §2.2 validation PASSed (115886 rows both,
byte-for-byte).

> **`initiate-failover` BLOCKS until convergence (~47s), it does NOT return
> after kicking off the transition.** So a sequential `initiate; sleep 5;
> abort` CANNOT catch the transitional window — the CLI is still *inside* the
> transition while you'd be sleeping, and by the time it returns the failover
> has already completed. To hit mid-transition, fire the abort from a
> **separate, event-driven watcher on the peer cluster** that polls
> `get-cluster-role-record` fast (~0.5s) and issues `abort-failover` the
> instant it observes a `*_TO_*` role. Run `initiate-failover` concurrently
> (backgrounded).

`abort-failover` drives the `ABORT_TO_*` states via
`HAGroupStoreManager.setHAGroupStatusToAbortToStandby()`
(`HAGroupStoreManager.java:435`). The abort is racy against the drain — if
the peer has already fully replayed and promoted, the abort has nothing to
revert; keep sustained load on so the drain window is wide enough to catch
mid-transition.

**Setup**
1. Prereqs + table.
2. Sustained background write load on cluster-a (same as S10 setup).

**Execute** (ACTIVE = cluster-a, STANDBY = cluster-b for the direction below;
swap namespaces if your current roles are mirrored — check
`get-cluster-role-record` first).
1. Stage a fast **abort-watcher on the STANDBY** (the promoting cluster,
   cluster-b). It polls the role record every 0.5s and fires `abort-failover`
   the instant it sees a transitional `*_TO_*` role:
   ```
   kubectl -n cluster-b exec -i hmaster-0 -- bash -lc 'cat > /tmp/abort-watch.sh' <<'EOF'
   #!/usr/bin/env bash
   set -uo pipefail
   HB=/hbase/bin/hbase; TOOL=org.apache.phoenix.jdbc.PhoenixHAAdminTool
   for i in $(seq 1 90); do
     rec=$($HB $TOOL get-cluster-role-record -g testHAGroup 2>/dev/null \
           | grep -E 'Cluster [12] Role' | tr '\n' ' ')
     echo "$(date +%T)  $rec"
     if echo "$rec" | grep -qE 'TO_STANDBY|TO_ACTIVE'; then
       echo "$(date +%T)  TRANSITIONAL -> abort"
       $HB $TOOL abort-failover -g testHAGroup 2>&1; exit 0
     fi
     sleep 0.5
   done
   EOF
   kubectl -n cluster-b exec -i hmaster-0 -- bash -lc 'chmod +x /tmp/abort-watch.sh'
   ```
2. Start the watcher (background), then immediately initiate failover on the
   ACTIVE (cluster-a). `initiate-failover` blocks ~47s until convergence — the
   watcher fires the abort from underneath it:
   ```
   kubectl -n cluster-b exec -i hmaster-0 -- bash -lc 'nohup /tmp/abort-watch.sh >/tmp/abort.log 2>&1 &'
   kubectl -n cluster-a exec -i hmaster-0 -- \
     /hbase/bin/hbase org.apache.phoenix.jdbc.PhoenixHAAdminTool \
       initiate-failover -g testHAGroup
   ```
3. Poll `get-cluster-role-record -g testHAGroup` until cluster-a is back to
   `ACTIVE` and cluster-b to `STANDBY`; read `/tmp/abort.log` on cluster-b for
   the abort summary.

**Verify**
- **Peer reverted, never promoted.** The abort log shows
  `Initial State: STANDBY_TO_ACTIVE → Final State: STANDBY` and the peer never
  reached `ACTIVE`. (Also grep the HA group znode history for any `Active`
  entry on the peer — there must be none.)
- **Old ACTIVE resumed rotation.** Grep its regionserver logs for the
  resume marker — the mirror of the `failoverPending` suspend, proving the
  writer-side quiesce is reversible and doesn't leave rotation stuck off
  (which would re-arm the PHOENIX-7562 deadlock):
  ```
  for rs in regionserver-0 regionserver-1 regionserver-2; do
    kubectl -n cluster-a exec -i "$rs" -- bash -lc \
      'grep -h "returned to ACTIVE_IN_SYNC; resuming rotation and SYNC mode" /data/hbase-logs/hbase.log* 2>/dev/null'
  done
  # Only the RS holding the Curator PathChildrenCache logs it; the others exit 1 (no match) — expected.
  ```
- **Writes not lost.** Background HA-load rounds keep returning rc=0; if the
  abort window is wide enough that a write *does* hit
  `MutationBlockedIOException`, that write **failed to the caller** — it is
  NOT automatically retried by `FailoverPhoenixConnection`. The blocked write
  must be re-issued explicitly after the role settles back to `ACTIVE`; a
  re-sent write then succeeds.
- **§2.2 cross-cluster data validation** PASSes once the STANDBY drains the
  post-abort backlog (`./utils/validate-replication.sh`, allow a couple of rounds).

**Teardown** — stop the load loop.

---

#### S8. Peer state transition race

**Exercises:** A race where standby sees the active flip to
`ActiveNotInSync` (store-and-forward) *while* the standby is itself still
catching up from an earlier round. Expected: standby transitions
`Standby → DegradedStandby` gracefully, no dual-active.

**Hot path:** `HAGroupStoreClient` ZK watch callback,
`HAGroupStoreManager.FailoverManagementListener`, log-reader backlog.

**Fault to inject:** network chaos that injects intermittent drops between
cluster-a and cluster-b HDFS while sustained load runs, causing the
writer to flap between SYNC and STORE_AND_FORWARD.

**Tool:** Chaos Mesh NetworkChaos (the loss/correlation knobs are easier
than scripting toxiproxy).

**STATUS:** core listener path implemented; observing the peer-initiated
`Standby → DegradedStandby` transition requires the `setHAGroupStatusToStoreAndForward`
writer-side call to propagate — verify before running.

**Setup**
1. Prereqs + table.
2. Install Chaos Mesh (§6.4).
3. Sustained background load on cluster-a.

**Execute**
Apply this `NetworkChaos`:
```yaml
apiVersion: chaos-mesh.org/v1alpha1
kind: NetworkChaos
metadata:
  name: flap-cluster-a-to-b-hdfs
  namespace: cluster-a
spec:
  action: loss
  mode: all
  selector:
    namespaces: [cluster-a]
    labelSelectors: { app: regionserver }
  direction: to
  target:
    mode: all
    selector:
      namespaces: [cluster-b]
      labelSelectors: { app: namenode }
  loss: { loss: "40", correlation: "20" }
  duration: "3m"
```

**Verify**
- Cluster-b's HA group znode oscillates between `Standby` and
  `DegradedStandby` (capture with a 1s-polling loop on the znode for the
  full 3 minutes).
- Cluster-a's HA group znode stays `Active` for the entire window — no
  flap to anything else.
- No `MutationBlockedIOException` raised to clients (writes kept succeeding
  into either SYNC or STORE_AND_FORWARD).
- After Chaos Mesh cleans up, both znodes settle at `Active` / `Standby`
  within ~60s.

**Teardown**
- Chaos Mesh auto-removes the chaos after `duration`. `kubectl delete
  networkchaos -n cluster-a --all` to stop early.

---

#### S9. Region split/merge during replication

**Exercises:** row-level change records for a given row appear in multiple
replication log files within one round because a region split happened
on the source. Standby must apply mutations idempotently and in correct
order regardless.

**Hot path:** `ReplicationLogGroup` (logs batched by target table, not
source region), `ReplicationLogProcessor` (unordered row replay with
MVCC timestamps), Phoenix compaction (max lookback window).

**Fault to inject:** force a region split on cluster-a mid-write.

**Tool:** HBase shell `split` command + load generator.

**STATUS:** replay-ordering guarantees rely on unchanged MVCC timestamps
and Phoenix's max-lookback window. The round-based reader driver is the
piece to verify — see `ReplicationLog$LogRotationTask` and
`ReplicationRound`.

**Setup**
1. Prereqs + a Phoenix table pre-split into 2 regions at `m` to make the later
   split observable (form (a), §2.1 — Phoenix `SPLIT ON`, on both clusters):
   ```
   for ns in cluster-a cluster-b; do
     kubectl -n $ns exec -i hmaster-0 -- bash -lc '
       /trucke/sqlline.sh "$ZOOKEEPER_QUORUM" <<SQL 2>/dev/null
   CREATE TABLE IF NOT EXISTS PHOENIX_HA_SPLIT (ID VARCHAR PRIMARY KEY, C VARCHAR)
     COLUMN_ENCODED_BYTES=0 SPLIT ON ('\''m'\'');
   !quit
   SQL
     '
   done
   ```
2. Start sustained write load on cluster-a spanning the split key — a long-lived
   sqlline JVM (form (a), §2.1) alternating `a*` and `z*` keys:
   ```
   kubectl -n cluster-a exec -i hmaster-0 -- bash -lc '
     i=0; while true; do
       p=$([ $((i % 2)) -eq 0 ] && echo a || echo z)
       printf "UPSERT INTO PHOENIX_HA_SPLIT VALUES ('"'"'%s-%d'"'"', '"'"'v-%d'"'"');\n" $p $i $i
       i=$((i+1))
     done | /trucke/sqlline.sh "$ZOOKEEPER_QUORUM" 2>/dev/null
   ' &
   LOAD_PID=$!
   ```

**Execute**
1. While load is running, trigger a split of the upper region. A region `split`
   is a genuine HBase *admin* op (not a data-plane mutation), so it correctly
   uses the HBase shell against the physical table `PHOENIX_HA_SPLIT`:
   ```
   kubectl -n cluster-a exec -i hmaster-0 -- /hbase/bin/hbase shell -n <<'EOF'
   split 'PHOENIX_HA_SPLIT', 't'
   EOF
   ```
2. Let the load continue for another minute after the split completes.
3. Stop load (`kill $LOAD_PID`), wait 2 min for the next replication round to
   drain.

**Verify**
- `SELECT COUNT(*)` on cluster-b equals cluster-a (form (a), §2.1, run against
  both as in S6's verify).
- For every row key that was written *during* the split, the value on
  cluster-b matches the last-written value on cluster-a — `SELECT` a sample of
  ten keys explicitly on both clusters. No "ghost write" (older value winning
  over newer).

**Teardown** — `DROP TABLE PHOENIX_HA_SPLIT` (form (a)) on both clusters.

---

### 5.5 Operator-forced / unplanned failover

#### S4. Forced failover from DegradedStandby

**Exercises:** the dotted edge in the design's state diagram —
`DegradedStandby → StandbyToActive → Active`, bypassing the "OUT must drain
first" invariant, accepting the resulting application-level inconsistency
on the promise of service recovery.

**Hot path:** the forced `DEGRADED_STANDBY → STANDBY_TO_ACTIVE` transition
(allowed in `HAGroupStoreRecord`: `DEGRADED_STANDBY.allowedTransitions`
includes `STANDBY_TO_ACTIVE`, `HAGroupStoreRecord.java:125`),
`ReplicationLog*` (local queue discarded with a warning log). **Note:**
there is no `HAGroupStoreManager.forceFailover()` method yet — the forced
promotion path and its operator CLI are still in design.

**Fault to inject:** cluster-a becomes unrecoverable (simulated by
deleting its entire namespace) **while** its OUT queue holds
un-forwarded logs.

**Tool:** `kubectl delete ns` + forced-failover CLI / znode flag.

**STATUS:** not yet testable. Forced-failover is the most operator-facing
transition and its CLI surface is still in design (see §"Forced Failover in
Degraded Standby Conditions"). The `DEGRADED_STANDBY → STANDBY_TO_ACTIVE`
edge exists in `HAGroupStoreRecord`, but no `HAGroupStoreManager` method or
znode flag drives it yet.

**Setup**
1. Prereqs + table.
2. Repeat S7 up to the point where cluster-a's OUT has ~100 log files.
3. **Do not** restore cluster-b HDFS yet.

**Execute**
1. Simulate cluster-a catastrophic failure:
   ```
   kubectl delete ns cluster-a
   ```
2. Restore cluster-b HDFS:
   ```
   kubectl -n cluster-b scale sts/namenode --replicas=1
   kubectl -n cluster-b scale sts/datanode --replicas=1
   ```
3. Issue the forced failover. **No CLI or flag exists yet** — there is no
   `forceFailover` method in `HAGroupStoreManager` and `initiate-failover`
   requires a reachable ACTIVE peer (which cluster-a no longer is). The
   `DEGRADED_STANDBY → STANDBY_TO_ACTIVE` edge exists in the state machine
   (`HAGroupStoreRecord.java:125`) but nothing drives it. This step is a
   placeholder until the forced-failover surface lands; see STATUS above.

**Verify**
- Cluster-b reaches `Active` within the forced-failover SLA (per the
  design: seconds, not bounded by drain).
- The audit log in `/data/hbase-logs/regionserver.log` on cluster-b
  contains a `FORCED FAILOVER` entry with operator identity and timestamp
  (per the design's auditing requirement).
- Data loss check (upper bound): rows written on cluster-a that were
  still queued in its OUT dir at the moment of `kubectl delete ns
  cluster-a` are **not** present on cluster-b. This is the expected
  consistency trade-off — document which row IDs are missing for the
  test report.
- Data loss check (lower bound, the critical one): every row that was
  **successfully synced** to cluster-b's IN queue pre-partition **is**
  present on cluster-b. The set of missing rows must be a strict subset
  of those in cluster-a's OUT queue at the moment of namespace delete —
  never a superset. If any row that had been synced is missing on
  cluster-b, that is a correctness bug, not an expected trade-off.
  Concretely: capture `hdfs dfs -ls /phoenixHA/testHAGroup/in` and
  `/phoenixHA/testHAGroup/out` on cluster-a immediately before
  `kubectl delete ns`, and reconcile against cluster-b's final row set.
- New writes on cluster-b succeed.

**Teardown**
- `kubectl apply -k /Users/tkhurana/soma/root/hbase-local-testbed/k8s/cluster-a` to
  re-provision a fresh cluster-a (starts in `Offline` / `Standby` until
  reconciled).

---

#### S20. Failover triggered by an active-cluster master-component failure

**The gap this closes.** Every failover scenario to date is *cooperative*: the
active cluster is healthy and participates in the handoff (S1/S10/S18/S19 planned
cutover, S5 its abort), or the *replication path* is faulted while the active RS
stays up and serving (S7/S12/S17b — no promotion happens). S4 is the only
active-loss case, and it deliberately fails over *with a backlog in OUT* to test
the RPO *boundary*. **None of them tests the DR headline: everything is healthy
and in-sync, then a core infrastructure component on the active dies, and the
operator must fail over to restore service with zero loss.** S20 is that case,
run against each active-side master component in turn.

**Exercises:** the operator-driven failover path (`PhoenixHAAdminTool
initiate-failover`) when the **active** cluster has lost a master component while
`ACTIVE_IN_SYNC` and fully caught up (OUT empty). The load-bearing question is
whether the *designed, cooperative* failover surface can still complete when the
active cannot fully participate — and, where it cannot, that inability is itself
the finding.

**Hot path:** `HAGroupStoreManager.initiateFailoverOnActiveCluster()`
(`phoenix-core-client/.../jdbc/HAGroupStoreManager.java:391`), driven by
`PhoenixHAAdminTool` command `initiate-failover`
(`executeInitiateFailover`, `PhoenixHAAdminTool.java:624`). That command runs
**on the active**, validates the local state is `ACTIVE_IN_SYNC`/
`ACTIVE_NOT_IN_SYNC` (`:665-675`), transitions it to `*_TO_STANDBY`, then **polls
for the pair to converge** — waiting for local `STANDBY` *and* peer `ACTIVE`
(`isStableFailoverPair`, `:1227-1230`; poll loop `:1150-1224`, peer read via
`getPeerHAGroupStoreRecord`). The standby only self-promotes in reaction to the
peer publishing `ACTIVE_IN_SYNC_TO_STANDBY`
(`HAGroupStoreManager.java:109`: `transitions.put(ACTIVE_IN_SYNC_TO_STANDBY,
currentLocal -> STANDBY_TO_ACTIVE)`).

**Topology that decides the outcome** (from `utils/create-ha-group.sh`): each
cluster stores its own `HAGroupStoreRecord` in its **own** ZooKeeper (znode
`/phoenix/consistentHA/testHAGroup`) plus `SYSTEM.HA_GROUP`. cluster-a is slot 1
(ACTIVE), cluster-b is slot 2 (STANDBY). ZooKeeper, NameNode, and HMaster are
**separate pods** in the active namespace, so the three faults degrade different
subsystems and are predicted to diverge:

| Variant | Fault on cluster-a | Active's ZK record still writable/readable? | Predicted outcome (verify, don't assume) |
|---|---|---|---|
| **S20a** | NameNode down (`scale sts/namenode --replicas=0`) | Yes — ZK has its own PVC, survives HDFS loss | HBase on the active loses WAL storage → active RS aborts. `initiate-failover` *may* still drive the ZK transition and cluster-b *can* read it. Open question: can the active's cutover/drain complete with dead HDFS, or does it stall mid-transition? |
| **S20b** | ZooKeeper down (`scale sts/zookeeper --replicas=0`) | **No** — cluster-a can't write its transition; cluster-b can't read the peer | `initiate-failover` cannot even record the source transition. This is the "no first-class unplanned-failover surface" finding made concrete. |
| **S20c** | HMaster down (`scale sts/hmaster --replicas=0`) | Yes — ZK + HDFS alive | The admin tool talks to ZK, not HMaster, so the transition likely records. But you cannot `kubectl exec hmaster-0` when it's gone — run the tool from a surviving RS pod. Region reassignment is frozen while HM is down; observe whether that blocks convergence. |

**Fault to inject:** scale the target master component to zero replicas on
cluster-a **from a clean, fully-synced `ACTIVE_IN_SYNC` state** (OUT empty). One
variant per run; reset between variants.

**Tool:** `kubectl scale` + `PhoenixHAAdminTool initiate-failover` (run from a
pod that survives the fault — `hmaster-0` for S20a/S20b, a `regionserver` pod for
S20c). **Per the run decision: only the designed cooperative path is exercised.
If `initiate-failover` cannot complete, that stall/rejection is recorded as the
result — do NOT force the promotion with a raw `update -s STANDBY_TO_ACTIVE -F`
znode overwrite** (that unguarded last-resort path exists —
`executeUpdate`/`validateUpdate` skip `isTransitionAllowed`,
`PhoenixHAAdmin.updateHAGroupStoreRecordInZooKeeper` writes the znode directly —
but exercising it is explicitly out of scope for S20; it belongs to a future S4
run once a purpose-built forced-failover surface lands).

**STATUS:** not yet run — newly specified. Prediction per the table above; the
whole point is to replace these predictions with observed behavior. This is the
first scenario family to fault the **active's own master infrastructure** (as
opposed to the replication path or the standby), and it doubles as a probe of how
far the *cooperative* failover surface degrades before an operator would need a
forced path.

**Setup**
1. Fresh, healthy clusters; `PHOENIX_HA_T` created on both (form (a), §2.1),
   HA group `testHAGroup` present, cluster-a `ACTIVE_IN_SYNC`, cluster-b
   `STANDBY`.
2. Drive a short HA-connection write burst (form (b), §2.1) and then **stop
   writing**; wait until replication is fully caught up — OUT empty on cluster-a:
   ```
   kubectl -n cluster-a exec hmaster-0 -- \
     /hadoop/bin/hdfs dfs -ls /phoenixHA/testHAGroup/out
   ```
   This "clean in-sync" precondition is what makes the expectation **zero RPO**,
   distinguishing S20 from S4 (which fails over *with* an OUT backlog).
3. Record the baseline row count/checksum on **both** clusters (§2.2) — they must
   already match before the fault.

**Execute** (one variant per run)
1. Confirm the pre-fault role record:
   ```
   kubectl -n cluster-a exec hmaster-0 -- \
     /hbase/bin/hbase org.apache.phoenix.jdbc.PhoenixHAAdminTool \
     get-cluster-role-record -g testHAGroup
   ```
2. Inject the fault:
   - **S20a:** `kubectl -n cluster-a scale sts/namenode --replicas=0`
   - **S20b:** `kubectl -n cluster-a scale sts/zookeeper --replicas=0`
   - **S20c:** `kubectl -n cluster-a scale sts/hmaster --replicas=0`
3. Confirm the active can no longer serve (writes through the direct cluster-a
   connection now fail / the active RS begins aborting for S20a).
4. Issue the designed failover from a **surviving** pod:
   ```
   # S20a / S20b: hmaster-0 still up
   kubectl -n cluster-a exec hmaster-0 -- \
     /hbase/bin/hbase org.apache.phoenix.jdbc.PhoenixHAAdminTool \
     initiate-failover -g testHAGroup
   # S20c: HMaster is the faulted component — run from a regionserver pod instead
   kubectl -n cluster-a exec regionserver-0 -- \
     /hbase/bin/hbase org.apache.phoenix.jdbc.PhoenixHAAdminTool \
     initiate-failover -g testHAGroup
   ```
5. Observe the command: does it converge (`isStableFailoverPair` satisfied), or
   stall in the poll loop / reject at validation? Capture its stdout/stderr and
   the elapsed time verbatim — for S20b/S20c a stall is the expected, informative
   result.

**Verify**
- **If failover converges:** cluster-b reaches `ACTIVE` (verify with
  `get-cluster-role-record` **read from cluster-b's** ZK, since cluster-a's may be
  unreadable), new writes through the HA connection land on cluster-b, and — from
  the clean in-sync precondition — **zero RPO:** every row present pre-fault is
  present on cluster-b (§2.2 count + content parity). No row that was committed
  and synced before the fault may be missing.
- **If failover stalls/rejects:** record exactly where (validation vs. poll
  timeout), the log lines, and the resulting stuck state. The finding is
  "the cooperative `initiate-failover` surface cannot complete an unplanned
  failover under this component loss; an operator would need a forced-promotion
  surface that does not exist today (see S4 STATUS)." Cross-reference which of the
  three components is the hard blocker (predicted: ZK for S20b).
- In **all** variants, confirm the standby never silently loses a
  previously-synced row and never enters a split-brain dual-ACTIVE state
  (`get-cluster-role-record` on both ZKs must not both read `ACTIVE*`).

**Teardown**
- Scale the faulted component back up:
  `kubectl -n cluster-a scale sts/<namenode|zookeeper|hmaster> --replicas=1`
  and wait for the pod to rejoin.
- If a variant left the HA record in a half-transitioned state, revert with
  `abort-failover -g testHAGroup` (works only from `STANDBY_TO_ACTIVE`) or, as a
  fixture reset, re-provision cluster-a
  (`kubectl apply -k .../k8s/cluster-a`) and re-run `utils/create-ha-group.sh`.

---

### 5.6 Rotation-suspend / cutover-gate scenarios

These two scenarios isolate the in-sync-cutover rotation-suspend mechanism
(the fix for the graceful-failover deadlock). S1 exercises the same mechanism
end-to-end, but only as a side effect of the happy path; S18 and S19 pin the
two legs S1 does not — abort-resume and restart-in-cutover.

#### S18. Cutover rotation suspend, then abort resume

**Exercises:** the `failoverPending` flag lifecycle on the active —
`ACTIVE_IN_SYNC → ACTIVE_IN_SYNC_TO_STANDBY` **sets** it (rotation suspends),
then a cutover abort `ACTIVE_IN_SYNC_TO_STANDBY → ABORT_TO_ACTIVE_IN_SYNC →
ACTIVE_IN_SYNC` **clears** it (rotation resumes). This is the manual-testbed
counterpart to the `testCutoverSuspendsAndResumesRotation` IT.

**Hot path:** `ReplicationLogGroup.subscribeLocal(ACTIVE_IN_SYNC_TO_STANDBY)`
sets `failoverPending` (`ReplicationLogGroup.java:604-606`); the guard in
`ReplicationLog.requestRotation()` short-circuits every scheduled tick before
the CAS (`ReplicationLog.java:256-257`); the `ACTIVE_IN_SYNC` listener clears
it (`:619`), and the next tick stages a writer again.

**Fault to inject:** none — operator initiates a failover then aborts it
before the standby promotes. The key is to keep the cutover window open long
enough to observe several suspended rotation rounds; sustained write load
holds cluster-b in `STANDBY_TO_ACTIVE` (still draining) so the abort has
something to revert.

**Tool:** `kubectl` + `PhoenixHAAdminTool` (`initiate-failover` /
`abort-failover`).

**STATUS:** implemented and runnable. Same CLI surface as S1/S5. The
rotation-suspend guard is `ReplicationLog.requestRotation()`
(`ReplicationLog.java:256`) and `forceRotation()` (`:427`); the flag is set/
cleared by the cutover/abort listeners in `ReplicationLogGroup`
(`:604-619`).

**Setup**
1. Prereqs + table on both clusters (reuse S1 setup).
2. Shorten the rotation period so suspended rounds are observable within the
   test window — set `phoenix.replication.log.rotation.time.ms=10000` (§3)
   and recycle both clusters, or accept the 60s default and stretch the abort
   delay accordingly.
3. Start sustained background write load on cluster-a through the HA
   connection (form (b), §2.1 — same generator as S10) so the drain window
   stays wide.

**Execute**
1. Record the baseline `.plog` count in cluster-b's `in` shard dir:
   ```
   kubectl -n cluster-b exec namenode-0 -- \
     /hadoop/bin/hdfs dfs -ls -R /phoenixHA/testHAGroup/in 2>/dev/null \
     | grep -c '\.plog$'
   ```
2. Initiate failover from cluster-a (S1 step 3):
   ```
   kubectl -n cluster-a exec -i hmaster-0 -- \
     /hbase/bin/hbase org.apache.phoenix.jdbc.PhoenixHAAdminTool \
       initiate-failover -g testHAGroup
   ```
3. Confirm cluster-a is in the cutover gate:
   ```
   kubectl -n cluster-a exec -i hmaster-0 -- \
     /hbase/bin/hbase org.apache.phoenix.jdbc.PhoenixHAAdminTool \
       get-cluster-role-record -g testHAGroup
   # cluster-a state must read ACTIVE_IN_SYNC_TO_STANDBY (role ACTIVE_TO_STANDBY)
   ```
4. Poll the `in` shard dir for **at least 3 rotation rounds** while the gate
   holds and confirm the count does **not** grow past the step-1 baseline
   (plus at most the one file open at cutover):
   ```
   for i in $(seq 1 20); do
     kubectl -n cluster-b exec namenode-0 -- \
       /hadoop/bin/hdfs dfs -ls -R /phoenixHA/testHAGroup/in 2>/dev/null \
       | grep -c '\.plog$'
     grep -h 'rotation suspended: failover pending' \
       <(kubectl -n cluster-a logs regionserver-0 --tail=5 2>/dev/null) || true
     sleep 3
   done
   ```
   The `rotation suspended: failover pending` log line
   (`ReplicationLog.java:257`) should appear each round.
5. Abort the failover **before** cluster-b finishes promoting (run against
   the cluster holding the standby-bound role, cluster-b — as in S5):
   ```
   kubectl -n cluster-b exec -i hmaster-0 -- \
     /hbase/bin/hbase org.apache.phoenix.jdbc.PhoenixHAAdminTool \
       abort-failover -g testHAGroup
   ```
6. Poll `get-cluster-role-record` until cluster-a is back to `ACTIVE_IN_SYNC`
   and cluster-b to `STANDBY`.

**Verify**
- **Suspend leg:** across step 4 the `.plog` count in cluster-b's `in` dir
  held flat (no new files per round) and the "rotation suspended" log line
  fired every round.
- **Resume leg:** within ~2 rotation periods of the abort settling, the
  `.plog` count in cluster-b's `in` dir **starts growing again** — rotation
  resumed once `failoverPending` cleared. Re-poll the dir to confirm.
- Background writes resume on cluster-a within ~10s of the abort (as in S5).
- Cluster-b never reached `ACTIVE` — grep its HA group znode history; there
  must be no `ACTIVE` entry.
- No data loss: `SELECT COUNT(*)` on cluster-a equals the committed row count;
  after the writer resumes and a round drains, cluster-b matches.

**Teardown** — stop the load loop; restore
`phoenix.replication.log.rotation.time.ms` if you changed it.

---

#### S19. RegionServer restart while in the cutover gate (preWALRestore re-ship, no deadlock re-arm)

**Exercises:** the `init()` restart-in-cutover path. When the active RS
restarts (or a region reassigns) while the persisted state is already
`ACTIVE_IN_SYNC_TO_STANDBY`, WAL replay drives `preWALRestore`, which reaches
`ReplicationLogGroup.init()`. The fix makes `init()` (a) **admit** the
mutation-blocked cutover role instead of throwing — a throw here aborts WAL
replay and silently drops already-committed edits the peer never received;
(b) start in **SYNC** mode; and (c) **seed `failoverPending`** so the fresh
writer does not immediately mint a new file and re-arm the deadlock.

**Hot path:** `IndexRegionObserver.preWALRestore`
(`IndexRegionObserver.java:939`) → `getHAGroupFromWALKey` (`:927`) →
`ReplicationLogGroup.get()` → `init()`. The init role gate admits the
active-but-mutation-blocked cutover role (`ReplicationLogGroup.java:532`);
`initializeReplicationMode` maps `ACTIVE_IN_SYNC_TO_STANDBY → SYNC`
(`:688`); init seeds the flag when the persisted state is the cutover gate
(`:568-569`). Contrast: on a non-active persisted role `init()` still
fail-fasts.

**Fault to inject:** `SIGKILL` the active RS **after** it has entered
`ACTIVE_IN_SYNC_TO_STANDBY` but **before** the failover completes. To make
the preWALRestore re-ship the only route for a specific edit, stall the
remote sync for one batch with a brief toxiproxy timeout just before the
kill (as in S6/S12), so that edit is WAL-committed on cluster-a but not yet
shipped to cluster-b at the moment of the crash.

**Tool:** `kubectl delete pod` + `PhoenixHAAdminTool` + (optional, for a
deterministic un-shipped edit) Toxiproxy.

**STATUS:** implemented. `preWALRestore` is wired in `IndexRegionObserver`
(`:939`); the init-gate admits the cutover role and seeds `failoverPending`
(`ReplicationLogGroup.java:532`, `:568-569`, `:688`). This scenario is the
testbed counterpart to `testInitInCutoverStartsSyncWithFailoverPending`
(unit) — it additionally proves the durability leg (committed edits
re-ship, not lost) end-to-end.

**✅ COVERAGE GAP found 2026-07-17, FIX VERIFIED end-to-end 2026-07-19.**
The gap: the sibling case (replay on a *terminal* `STANDBY`) was NOT handled
and reproduced the very `init()` fail-fast this fix closes for the cutover
gate. Observed organically as a downstream effect of the S7 namenode outage,
not by design. **Fixed by commits `6bb7885ba0` (move the active-role gate out
of `init()` into the `get()` factory; `get()` returns
`Optional<ReplicationLogGroup>` and returns empty — uncached — on a non-active
role, so the replay path skips re-ship while HBase still applies the edits
locally; the live write path `orElseThrow`s to preserve split-brain
rejection) and `dc287cff71`.** Verified against the SAME on-disk
`recovered.edits` fixture: swapped the fixed jar, rolled cluster-b (PVCs
preserved), the stuck region `6422d065…` opened (`next sequenceid=43439` —
replayed through the unflushed tail), and `validate-replication.sh` returned
**PASS: 116037 rows byte-for-byte identical** on both clusters — no data lost,
full reconciliation. See [[s19-terminal-standby-replay-gap]]. Original repro:

- **What happened.** cluster-b was `ACTIVE` earlier in the session, took
  ~43K client writes (form (b) HA connection → each WAL edit stamped
  `_HAGroupName=testHAGroup`), then failed over to terminal `STANDBY`. The
  S7 NN outage induced a cluster-wide GC/ZK-expiry stall (`We slept
  172582ms`) that crashed RS `10.244.1.19,16020,1784330641995` with ~3,400
  of those edits (seqids 40019–43429) **committed to its WAL but never
  flushed** (last HFile `MAX_SEQ_ID_KEY=40018`). On master restart a
  `ServerCrashProcedure` split the WAL into
  `PHOENIX_HA_T/<region>/recovered.edits/{40014,43429}`.
- **The stuck region.** Region reopen → `HRegion.replayRecoveredEdits` →
  `preWALRestore` → `getHAGroupFromWALKey` sees `_HAGroupName` on every
  replayed edit → `ReplicationLogGroup.init()`. But the persisted role is
  now **terminal `STANDBY`**, which the init-gate does **not** admit (only
  the cutover role is), so it throws:
  ```
  java.io.IOException: HAGroup testHAGroup cannot start a replication log
    writer: local role STANDBY is not active (state STANDBY)
      at ReplicationLogGroup.init(ReplicationLogGroup.java:540)
      at IndexRegionObserver.getHAGroupFromWALKey(IndexRegionObserver.java:927)
      at IndexRegionObserver.preWALRestore(IndexRegionObserver.java:939)
      at HRegion.replayRecoveredEdits(HRegion.java:5409)
      ... AssignRegionHandler.process
  ```
  Region open aborts → `FAILED_OPEN` → master retries forever → the region
  shows `serverName=null` in `hbase:meta` and `COUNT(*)`/scans on the table
  hang (this is why S7 data-validation could not complete on cluster-b).
- **Why the S19 fix does not catch it.** The init-gate comment
  (`ReplicationLogGroup.java:523-539`) assumes any `_HAGroupName`-carrying
  edit reaching `init()` on a non-active role is "a stray/split-brain
  sync-path write" — true for the live write path, but **false for
  crash-recovery replay of a demoted cluster's own formerly-ACTIVE edits**.
  Confirmed three ways: (1) all 43,405 recovered edits carry
  `_HAGroupName=testHAGroup`; (2) row keys are the `blk-*`/`race2-*`
  load-generator writes; (3) cell timestamps fall entirely within
  cluster-b's ACTIVE tenures and *stop at the exact millisecond*
  (`00:10:06`) cluster-b entered `ACTIVE_IN_SYNC_TO_STANDBY` — the signature
  of a live-ACTIVE write path cut off by demotion. The standby replay-apply
  path (`ReplicationLogProcessor.applyMutations`, plain `table.batchAll`)
  never annotates, so these could only have come from the ACTIVE write path.
- **Proposed fix direction.** `preWALRestore` should treat replay as
  apply-only when the local role is a settled `STANDBY`/`DEGRADED_STANDBY`:
  skip `getHAGroupFromWALKey`/writer-init entirely (the edits are applied to
  the region by HBase's normal replay; a standby must not start a
  replication log writer at all), OR the init-gate must admit terminal
  STANDBY for the replay path (not the live write path) and no-op the
  writer. Either way the guard must distinguish "replaying my own
  formerly-ACTIVE unflushed edits after demotion" from "a rogue live
  sync-path write."

**Setup**
1. Prereqs + table on both clusters (reuse S1 setup).
2. (Optional, for the deterministic un-shipped edit) Deploy toxiproxy in
   cluster-a and point `HDFS_URL_2` at it (§6.3, §2 step 6); recycle
   cluster-a RS.
3. Write a known committed row on cluster-a and confirm it replicated to
   cluster-b (baseline — this row must survive the whole scenario):
   ```
   kubectl -n cluster-a exec -i hmaster-0 -- bash -lc '
     /trucke/sqlline.sh "$ZOOKEEPER_QUORUM" <<SQL 2>/dev/null
   UPSERT INTO PHOENIX_HA_T VALUES ('\''r-pre-cutover'\'', '\''synced-before-cutover'\'');
   !quit
   SQL
   '
   sleep 65   # let the round roll and ship
   ```
4. Start sustained background write load on cluster-a (form (b), §2.1) so the
   cutover window stays open (cluster-b keeps draining) and there is in-flight
   traffic at the moment of the kill.

**Execute**
1. Initiate failover from cluster-a and confirm it reaches the cutover gate
   (S18 steps 2-3): `get-cluster-role-record` must show cluster-a
   `ACTIVE_IN_SYNC_TO_STANDBY`.
2. **(Optional, deterministic un-shipped edit)** Apply a short remote-sync
   stall and write one more row so it commits locally but does not ship:
   ```
   kubectl -n cluster-a exec toxiproxy-0 -- \
     /toxiproxy-cli toxic add remote-hdfs -t timeout -a timeout=1
   kubectl -n cluster-a exec -i hmaster-0 -- bash -lc '
     /trucke/sqlline.sh "$ZOOKEEPER_QUORUM" <<SQL 2>/dev/null
   UPSERT INTO PHOENIX_HA_T VALUES ('\''r-in-cutover'\'', '\''committed-not-yet-shipped'\'');
   !quit
   SQL
   '
   ```
3. `SIGKILL` the active RS while still in the cutover gate:
   ```
   kubectl -n cluster-a delete pod regionserver-0 --grace-period=0 --force
   ```
4. If you applied the toxic, remove it as the new RS comes up so replication
   resumes:
   ```
   kubectl -n cluster-a exec toxiproxy-0 -- \
     /toxiproxy-cli toxic remove remote-hdfs -n timeout
   ```
5. Wait for the RS to be Ready and WAL replay to complete (~60-90s).

**Verify**
- **init did not fail-fast.** The new RS's regionserver log shows the group
  starting for the cutover state — grep for
  `started with mode=SYNC` and the absence of any
  `cannot start a replication log writer: local role ... is not active`
  error. (A throw here is the durability bug this fix closes.)
- **preWALRestore re-shipped committed edits.** The new RS log shows
  `preWALRestore` invocations during region reopen. The `r-pre-cutover` row
  (and, if the optional leg ran, `r-in-cutover`) is present on cluster-b —
  no committed edit was silently dropped by the restart:
  ```
  kubectl -n cluster-b exec -i hmaster-0 -- bash -lc '
    /trucke/sqlline.sh "$ZOOKEEPER_QUORUM" <<SQL 2>/dev/null
  SELECT * FROM PHOENIX_HA_T WHERE ID IN ('\''r-pre-cutover'\'', '\''r-in-cutover'\'');
  !quit
  SQL
  '
  ```
- **Deadlock not re-armed.** The fresh writer started with rotation already
  suspended (`failoverPending` seeded in `init()`), so cluster-b's `in` shard
  dir does **not** start gaining new `.plog` files after the RS returns. Poll
  it as in S1's rotation-suspend verify — the count must stay flat while the
  gate holds.
- **Failover still completes.** Once the restarted active is stable in the
  cutover gate with rotation suspended, the standby's file-quiescence gate
  eventually holds and the failover completes (cluster-b → `ACTIVE`,
  cluster-a → `STANDBY`) — the restart did not permanently wedge it.

**Variant S19b — replay on a terminal STANDBY (the coverage gap above).**
Same mechanism, different persisted role, opposite (bug-exposing) outcome.

**Note — stopping the namenode is the natural, low-contrivance way to reach
`preWALRestore`.** You do not need the scripted SIGKILL-mid-cutover +
toxiproxy stall of the primary S19 flow. A namenode outage drives the code
path for free as a chain reaction: **NN down → HDFS unavailable → RS can't
sync/heartbeat → GC/ZK-expiry stall (`We slept …ms`) → RS crash → master
`ServerCrashProcedure` → WAL split → `recovered.edits` → region reopen →
`replayRecoveredEdits` → `preWALRestore`.** The tradeoff vs. the scripted
flow: the primary S19 kill is deterministic and lands on the **admitted**
cutover role (fix works); the NN-down route is opportunistic — the role at
replay is whatever the cluster settles to, so it can land on the **rejected**
terminal-STANDBY branch and expose the gap (which is exactly how it was found,
downstream of S7).

To reproduce deterministically: (1) make cluster-b ACTIVE and write a batch
through the HA connection (form (b)) so its WAL edits carry `_HAGroupName`;
(2) fail cluster-b over to terminal `STANDBY` **without** flushing (or write
more than one memstore-flush's worth so a tail stays unflushed); (3) crash a
cluster-b RS holding one of those regions — either `SIGKILL` it directly, or
(more realistically) stop the cluster-b namenode (S7) and let the induced
stall crash it — so its unflushed WAL is split into `recovered.edits`;
(4) let the region reassign. **Expected (current, buggy):**
region open aborts with `cannot start a replication log writer: local role
STANDBY is not active`, region stuck `FAILED_OPEN` / `serverName=null` in
`hbase:meta`, scans hang. **Expected (after the fix):** replay applies the
edits locally, no writer is started, region opens clean. Inspect the recovered
edits with `hbase org.apache.hadoop.hbase.wal.WALPrettyPrinter -j
<recovered.edits file>` and confirm `_HAGroupName` presence by grepping the raw
file bytes (`hdfs dfs -cat ... | tr -c '[:print:]' '\n' | grep -c
_HAGroupName`).

**Teardown** — stop the load loop; remove any lingering toxics.

## 6. Tool cookbook

### 6.1 `kubectl` primitives

Everything is already in place — no install needed beyond the kind cluster.
Most useful primitives for HA fault injection:

- `kubectl -n <ns> delete pod <pod> --grace-period=0 --force` — SIGKILL, no
  graceful shutdown. Use for crash scenarios.
- `kubectl -n <ns> scale sts/<role> --replicas=<n>` — drop a whole service
  tier. Use for cluster-level outages.
- `kubectl cordon <node>` + `kubectl drain` — isolate a worker node.
  Useful when you need to split a StatefulSet across nodes then partition.
- `kubectl -n <ns> exec <pod> -- <cmd>` — run inside a pod. Referenced
  throughout.

### 6.2 NetworkPolicy (requires Calico)

**Why kind default CNI isn't enough.** kindnet doesn't enforce
`NetworkPolicy` objects — they apply but have no effect. Re-create the kind
cluster with a CNI that does, e.g. Calico:

```yaml
# Replace hbase-local-testbed/kind-cluster.yaml contents, then kind delete & kind create
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
name: bdlocal
networking:
  disableDefaultCNI: true
  podSubnet: "192.168.0.0/16"
nodes:
  - role: control-plane
    extraPortMappings:
      - { containerPort: 30010, hostPort: 30010, protocol: TCP }
      - { containerPort: 30011, hostPort: 30011, protocol: TCP }
      - { containerPort: 30870, hostPort: 30870, protocol: TCP }
      - { containerPort: 30871, hostPort: 30871, protocol: TCP }
  - role: worker
  - role: worker
```

Then install Calico:
```
kubectl create -f https://raw.githubusercontent.com/projectcalico/calico/v3.28.0/manifests/tigera-operator.yaml
kubectl create -f https://raw.githubusercontent.com/projectcalico/calico/v3.28.0/manifests/custom-resources.yaml
kubectl -n calico-system rollout status ds/calico-node --timeout=120s
```

**Example policy — block cluster-b's NameNode from cluster-a:**
```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: block-cluster-b-hdfs
  namespace: cluster-a
spec:
  podSelector: {}
  policyTypes: [Egress]
  egress:
  - to:
    - namespaceSelector:
        matchLabels: { kubernetes.io/metadata.name: cluster-b }
      podSelector:
        matchLabels: { app: namenode }
    ports:
    - { port: 9000, protocol: TCP }
```
Apply with `kubectl apply -f`; remove with `kubectl delete -f`.

### 6.3 Toxiproxy

**Deploy as a pod in cluster-a** (one-time). Manifest checked in at
`hbase-local-testbed/utils/toxiproxy.yaml`; `kubectl apply -f` it.

> **Image note.** `ghcr.io/shopify/toxiproxy` has **no `2.11` / `v2.11.0`
> tag** (only `latest` + dated SHAs), and kind's multi-arch image import
> chokes on the upstream manifest list (`ctr: content digest ... not found`).
> Flatten to a single-arch image and preload it (kind nodes have no registry
> access):
> ```
> docker pull --platform linux/arm64 ghcr.io/shopify/toxiproxy:latest
> mkdir tctx && printf 'FROM ghcr.io/shopify/toxiproxy:latest\n' > tctx/Dockerfile
> docker build --platform linux/arm64 --provenance=false --output type=docker \
>   -t bdlocal-toxiproxy:2.11 tctx
> kind load docker-image bdlocal-toxiproxy:2.11 --name bdlocal
> ```
> The checked-in manifest already references `bdlocal-toxiproxy:2.11` with
> `imagePullPolicy: IfNotPresent`. Both `/toxiproxy` (server) and
> `/toxiproxy-cli` live at the image root, arm64-native.

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata: { name: toxiproxy, namespace: cluster-a }
spec:
  serviceName: toxiproxy
  replicas: 1
  selector:
    matchLabels: { app: toxiproxy }
  template:
    metadata:
      labels: { app: toxiproxy }
    spec:
      containers:
        - name: toxiproxy
          image: ghcr.io/shopify/toxiproxy:2.11
          args: ["-host=0.0.0.0", "-config=/config/proxies.json"]
          ports:
            - { containerPort: 8474 }  # admin
            - { containerPort: 9000 }  # proxied HDFS NN
          volumeMounts:
            - { name: cfg, mountPath: /config }
      volumes:
        - name: cfg
          configMap: { name: toxiproxy-config }
---
apiVersion: v1
kind: ConfigMap
metadata: { name: toxiproxy-config, namespace: cluster-a }
data:
  proxies.json: |
    [{
      "name": "remote-hdfs",
      "listen": "0.0.0.0:9000",
      "upstream": "namenode.cluster-b.svc.cluster.local:9000",
      "enabled": true
    }]
---
apiVersion: v1
kind: Service
metadata: { name: toxiproxy, namespace: cluster-a }
spec:
  clusterIP: None
  selector: { app: toxiproxy }
  ports:
    - { name: admin, port: 8474 }
    - { name: hdfs,  port: 9000 }
```

**Point Phoenix at it** — update the `HDFS_URL_2` column in the
`SYSTEM.HA_GROUP` bootstrap row (see §2 step 6) to
`hdfs://toxiproxy.cluster-a.svc.cluster.local:9000/phoenixHA`,
then recycle cluster-a's regionserver so `HAGroupStoreClient` refreshes
the cached `HAGroupStoreRecord` and the Replication Log Writer picks up
the new peer-HDFS target. There is no hbase-site.xml key for this value.

**Common toxics:**
```
# 2s latency every request
/toxiproxy-cli toxic add remote-hdfs -t latency -a latency=2000

# cap to 100 KB/s
/toxiproxy-cli toxic add remote-hdfs -t bandwidth -a rate=100

# timeout all requests
/toxiproxy-cli toxic add remote-hdfs -t timeout -a timeout=1

# slow close
/toxiproxy-cli toxic add remote-hdfs -t slow_close -a delay=5000

# remove all toxics
/toxiproxy-cli toxic remove remote-hdfs -n <name>
```

### 6.4 Chaos Mesh

**Install** (once, on the kind cluster):
```
helm repo add chaos-mesh https://charts.chaos-mesh.org
helm install chaos-mesh chaos-mesh/chaos-mesh -n chaos-mesh --create-namespace \
  --set chaosDaemon.runtime=containerd \
  --set chaosDaemon.socketPath=/run/containerd/containerd.sock \
  --version 2.7.0
kubectl -n chaos-mesh rollout status ds/chaos-daemon --timeout=180s
```

**UI** (optional): `kubectl -n chaos-mesh port-forward svc/chaos-dashboard 2333:2333`
→ http://localhost:2333.

**Useful chaos kinds:**
- `NetworkChaos` — delay / loss / duplicate / corrupt, between pods or
  to/from external targets. Used in S8.
- `PodChaos` — `pod-kill`, `pod-failure` (sleep-pause) — alternative to
  `kubectl delete` when you want a schedule.
- `IOChaos` — inject latency or errors on specific file paths inside a pod.
  Useful to simulate slow disk on a DataNode without changing the pod spec.
- `TimeChaos` — skew the pod's clock. Useful for probing mutation-timestamp
  ordering assumptions in Phoenix replication.
- `DNSChaos` — return NXDOMAIN for a specific hostname, e.g. make
  `namenode.cluster-b.svc.cluster.local` unresolvable from cluster-a for 30s.

**Example — 500 ms latency from cluster-a regionservers to cluster-b's NN
for 1 minute:**
```yaml
apiVersion: chaos-mesh.org/v1alpha1
kind: NetworkChaos
metadata: { name: slow-remote-hdfs, namespace: cluster-a }
spec:
  action: delay
  mode: all
  selector:
    namespaces: [cluster-a]
    labelSelectors: { app: regionserver }
  direction: to
  target:
    mode: all
    selector:
      namespaces: [cluster-b]
      labelSelectors: { app: namenode }
  delay: { latency: "500ms", correlation: "0", jitter: "0" }
  duration: "60s"
```

### 6.5 `tc` / `iptables` inside pods

No extra install, but requires adding `NET_ADMIN` to the container's
`securityContext.capabilities.add`. Useful for one-off ad-hoc experiments.
Example (after patching the StatefulSet to add `NET_ADMIN`):

```bash
# 500ms latency on all egress
kubectl -n cluster-a exec regionserver-0 -- \
  tc qdisc add dev eth0 root netem delay 500ms

# 10% random drop
kubectl -n cluster-a exec regionserver-0 -- \
  tc qdisc change dev eth0 root netem loss 10%

# block a specific destination
kubectl -n cluster-a exec regionserver-0 -- \
  iptables -A OUTPUT -d <cluster-b-NN-IP> -j DROP

# clear
kubectl -n cluster-a exec regionserver-0 -- tc qdisc del dev eth0 root
kubectl -n cluster-a exec regionserver-0 -- iptables -F OUTPUT
```

Prefer Chaos Mesh for anything you'll run more than once — it self-cleans
and survives pod restarts more gracefully.

## 7. Scenario ↔ tool matrix

| Scenario | `kubectl` | NetworkPolicy | Toxiproxy | Chaos Mesh | `tc`/iptables |
|---|---|---|---|---|---|
| S1  Graceful failover | primary | — | — | — | — |
| S2  Transient partition recovery | — | viable | **primary** | viable | viable |
| S3  Failover with OUT backlog | required | **primary** | viable | viable | — |
| S4  Forced failover | required | **primary** (to induce backlog) | — | — | — |
| S5  Abort mid-transition | primary | — | — | — | — |
| S6  RegionServer crash | **primary** | — | — | viable (`PodChaos`) | — |
| S7  Standby cluster down | **primary** (`scale=0`) | viable | — | viable | — |
| S8  Peer state race | — | — | viable | **primary** | — |
| S9  Region split race | primary | — | — | — | — |
| S10 Failover under load | primary | — | — | — | — |
| S11 Local WAL sync fails (ghost-write guard) | required | — | — | **primary** (`IOChaos`) | viable |
| S12 Remote sync fails → `preWALRestore` re-ship | **primary** | — | **primary** | — | — |
| S17 Sync-timeout → RS abort | required | — | **primary** | — | — |
| S18 Cutover rotation suspend / abort resume | **primary** | — | — | — | — |
| S19 RS restart in cutover (preWALRestore re-ship) | **primary** | — | viable (deterministic edit) | — | — |
| S20 Failover on active master-component loss (NN/ZK/HM) | **primary** (`scale=0`) | — | — | viable (`PodChaos`) | — |

**If you can only install one extra tool, pick Chaos Mesh.** It covers the
largest surface — S2, S6, S7, S8 all become easier with it, and its
`IOChaos` / `TimeChaos` unlock follow-on scenarios (slow disk on standby DN,
clock skew on active) that aren't in the current matrix but will be.

## 7.1 Local-testbed sufficiency vs. real-cluster need

The kind test-bed is the **primary functional-correctness gate for every
scenario** — it runs the same production code paths, and it is where both known
bugs were *found* (PHOENIX-7562 deadlock, and the S7-orphan forwarder
dst-collision) and where fixes are verified against those paths. The question
this table answers is narrower: *for which scenarios does a real cluster surface
signal the local bed structurally cannot?* The bed fakes/shrinks three things —
single-node ZK, 3 DN / 3 RS with no real network jitter, and the
Salesforce-stripped stack (no Kerberos/PKI/SSL, no NameNode HA). A scenario
"needs a real cluster" only when its behavior depends on one of those.

| Scenario | Local sufficient? | Why / what a real cluster adds |
|---|---|---|
| S1  Graceful failover | ✅ local-sufficient | pure state-machine logic; scale-invariant |
| S2  Transient partition recovery | ⚠️ real-cluster recommended | partition-recovery *timing* + catch-up SLA depend on real network + WAL volume |
| S3  Failover with OUT backlog | ⚠️ real-cluster recommended | backlog drain time / lease-recovery scale with region count + WAL volume |
| S4  Forced failover | ✅ local-sufficient | operator-driven edge; logic scale-invariant |
| S5  Abort mid-transition | ✅ local-sufficient | state-machine logic |
| S6  RegionServer crash | ✅ local-sufficient | crash-recovery / WAL-replay path is logic; verified 2026-07-19 |
| S7  Standby cluster down | ✅ local-sufficient | mode-transition logic; verified via S7-mtime/S7-orphan |
| S7-mtime  SAF heartbeat + sync gate | ✅ local-sufficient | ZK CAS + timer logic; verified 2026-07-19 |
| S7-orphan  Orphaned-plog wedge (dst-collision bug) | ✅ local-sufficient | deterministic logic defect; scale only changes *frequency/severity*, not existence. **Fix belongs in a unit/IT test first**, then local S7-orphan re-run |
| S8  Peer state race | 🔴 needs real cluster | timing/concurrency race; local single-node ZK + no jitter *under-samples* the window |
| S9  Region split/merge race | 🔴 needs real cluster | split/merge concurrency + real region churn; local under-samples the race |
| S10 Failover under load | ✅ local-sufficient for correctness; ⚠️ **SLA needs real cluster** | logic verified locally (rotation-suspend, atomic cutover, zero RPO); but under load converged in ~131 s > 120 s ceiling — see the S10 timing finding: excess is structural (round-close+buffer+poll-cadence), so validate the ceiling / retune `executor.frequency.seconds`+`waiting.buffer.percentage` on real hardware |
| S11 Local WAL sync fails (ghost-write guard) | ✅ local-sufficient | design-level invariant; logic path |
| S12 Remote sync fails → preWALRestore re-ship | ✅ local-sufficient | WAL-replay re-send logic |
| S17 Sync-timeout → RS abort | ✅ local-sufficient | timeout→abort logic; timeout tuning is config, not scale |
| S18 Cutover rotation suspend / abort resume | ✅ local-sufficient | rotation state-machine logic |
| S19 RS restart in cutover (preWALRestore re-ship) | ✅ local-sufficient | replay/re-ship logic; see the known replay-gap note in S19 |
| S20 Failover on active master-component loss (NN/ZK/HM) | ✅ local-sufficient | tests whether the *cooperative* `initiate-failover` surface completes under each active-side component loss; the per-component reachability of the ZK record (single-node ZK is enough to prove writable/not-writable) and the CLI convergence/stall are logic, not scale — a real cluster only changes recovery *timing* |

**Legend:** ✅ local-sufficient = the local bed fully covers it, no real-cluster
repeat needed for correctness. ⚠️ real-cluster recommended = local proves the
*mechanism*, a real cluster proves the *SLA/timing* (do before release sign-off,
not for correctness). 🔴 needs real cluster = local structurally under-samples
the race; a real cluster is required to trust the result.

**Not in this doc's scope but real-cluster-only by definition:** any Kerberos /
PKI / SSL auth path and NameNode-HA failover — the test-bed strips these
entirely (see `hbase-local-testbed` README "Known limitations"), so they can
*only* be exercised on a real cluster.

**Bottom line:** do **not** repeat the full suite on a real cluster. Treat local
as the correctness gate for all scenarios; cherry-pick S8, S9 (races) plus S2,
S3 (scale/timing) for real-cluster confirmation, and reserve real clusters
additionally for the auth/NN-HA paths the bed can't represent.

## 8. Change log

- **Initial version** — ten scenarios covering every edge of the design's
  state diagram, tool cookbook for kind-based local test-bed.
- **Writer-component review pass** — added S11 (local WAL sync fails,
  ghost-write prevention), S12 (remote sync fails → `preWALRestore`
  re-ship), and S17 (sync-timeout → RS abort). Tightened S2's verification to
  include post-drain data-consistency (no ghost writes, original
  timestamps preserved). Tightened S4's data-loss check to include the
  upper bound (synced rows must not be lost, only OUT-queued rows may
  be). Tightened S6's fault injection to force the `preWALRestore`
  path deterministically via a brief toxiproxy timeout before the RS
  kill.
- **Code-sync pass (2026-07-13)** — reconciled the doc against the
  feature branch. Corrected config defaults (`rotation.size.bytes`
  256 MB → 128 MB, `sync.retries` 4 → 1); replaced the fictional
  `phoenix.replication.writer.saf.sync.timeout.ms` with the real
  computed `phoenix.replication.log.sync.timeout.ms`. Fixed the HDFS
  shard layout (`<root>/<haGroupName>/in`|`out`, lowercase, auto-created —
  not manual uppercase `IN`/`OUT`). Renamed stale symbols to match code:
  `HAGroupNotActiveException` → server `MutationBlockedIOException` /
  client `FailoverSQLException`; `WALSyncTimeoutIOException` →
  `PhoenixWALSyncTimeoutException`; `HAGroupStoreManager.transitionState`/
  `abortFailover`/`forceFailover` → the actual `initiateFailoverOnActiveCluster`/
  `setHAGroupStatusToAbortToStandby`/(no forced method yet). Fixed the ZK
  path (`/phoenix/consistentHA/<haGroupName>`) and flagged the placeholder
  `zkCli.sh` blobs as illustrative. Flipped S11/S12/S17 STATUS to
  implemented; kept S3 (drain interlock) and S4 (forced failover) as not
  yet testable. Refreshed line-number citations.
- **Live-test-bed reconciliation pass (2026-07-16)** — verified every code
  citation against source and reconciled the doc against the *running*
  test-bed. Key corrections: (1) the failover trigger is a real CLI —
  `PhoenixHAAdminTool initiate-failover` / `abort-failover` /
  `get-cluster-role-record` — so S1, S5, S10 are runnable today; replaced all
  `zkCli.sh set '{"state":...}'` blobs (which never deserialized) with CLI
  invocations. (2) The HDFS root is `/phoenixHA` (operator-supplied via the
  HA group record's HDFS URL), **not** a fixed `/phoenix-replication`;
  rewrote every path to `/phoenixHA/testHAGroup/{in,out}`. (3) HA group name
  is `testHAGroup`, not `default`; ZK path `/phoenix/consistentHA/testHAGroup`.
  (4) Prereq steps 3–6 are already satisfied on the bed (coprocessors baked
  into `conf/hbase-site.xml`, SYSTEM tables + HA group row created by
  `utils/create-ha-group.sh`, `/trucke/sqlline.sh` client bundled) — marked
  as verify-only. (5) Removed the false "there is no `MutationBlockedIOException`
  class" line (the class exists, a `DoNotRetryIOException` subclass). (6) Pod
  count is now 9/namespace after the 3-DN/3-RS scale-up (was 5). (7) Refreshed
  drifted line numbers (config block 152-188, `preBatchMutate` throw 886-892,
  abortable capture 734-737, sync-timeout path 751/764/1079-1090, etc.). S4
  forced-failover confirmed still not-testable (no `forceFailover` method).
- **SQL-client conventions pass (2026-07-16)** — added §2.1 "SQL client
  conventions" defining the two `/trucke/sqlline.sh` connection forms — (a)
  direct single-cluster via `$ZOOKEEPER_QUORUM`, (b) HA failover via a SQL-file
  arg + `-p phoenix.ha.group.name testHAGroup` — with the verified positional
  argument mechanics (a file arg triggers non-interactive `-e "!connect" --run`
  mode; the HA group must be a `-p` property; the port colon is
  backslash-escaped; autocommit defaults on, so no `!autocommit on`). Replaced
  **all** data-plane `hbase shell put`/`get`/`count`/`create` in S1, S2, S6, S7,
  S9, S10, S11, S12 with sqlline recipes: writes exercising the client failover
  path use form (b); setup, reads, and direct-cluster writes use form (a);
  HBase table `create` became Phoenix `CREATE TABLE ... COLUMN_ENCODED_BYTES=0`
  DDL (with `SPLIT ON` for S9). Tables stay at the default `REPLICATION_SCOPE=0`
  — Phoenix synchronous replication is coprocessor-driven, so `=1` would
  wrongly double-ship via HBase's async replication as well. Load generators build a
  single `.sql` and stream it through one long-lived JVM rather than spawning
  sqlline per row (~10 s cold-start each). Only genuine admin ops stay on the
  HBase shell: the `SYSTEM.HA_GROUP` list check (§2) and S9's region `split`.
  Rationale: Phoenix replication fires only on mutations through the Phoenix
  write path — a raw `hbase shell put` bypasses the coprocessor and silently
  fails to replicate, so the old recipes would have "passed" while testing
  nothing.
- **Rotation-suspend / cutover-gate pass (2026-07-17)** — captures the
  graceful-failover deadlock fix (PHOENIX-7562). Root cause: log rotation ran
  unconditionally through the in-sync cutover gate, dropping a fresh `.plog`
  into the standby's `in` dir every round, so the standby's promotion gate
  (`getNewFiles(...).isEmpty()`) never held and both clusters spun forever.
  The deadlock was originally hit while running S1. Tightened **S1**'s verify
  with a "rotation-suspend invariant" bullet — a poll proving cluster-b's
  `in` `.plog` count stops growing once cluster-a enters
  `ACTIVE_IN_SYNC_TO_STANDBY`, and naming this quiescence as *why* the
  failover now completes (so S1 doubles as the deadlock regression guard).
  Added §5.6 with two new scenarios pinning the legs S1 doesn't isolate:
  **S18** (cutover rotation suspend, then abort resume — the manual
  counterpart to the `testCutoverSuspendsAndResumesRotation` IT; verifies the
  `failoverPending` set→clear lifecycle via `initiate-failover`/
  `abort-failover` and the "rotation suspended: failover pending" log line),
  and **S19** (RS restart while in the cutover gate — verifies the `init()`
  restart-in-cutover fix: admit the mutation-blocked role instead of throwing
  and aborting WAL replay, start SYNC, seed `failoverPending`, so
  preWALRestore re-ships committed edits and the fresh writer does not re-arm
  the deadlock). Both run on `kubectl` + `PhoenixHAAdminTool` only (S19's
  deterministic un-shipped edit optionally uses Toxiproxy). Updated the §4
  edge diagram and the §7 scenario↔tool matrix.
- **Unplanned-failover pass (2026-08-07)** — closed the coverage gap that every
  prior failover was *cooperative* (planned cutover with a healthy active, or a
  replication-path fault with the active still serving). Added **S20** (§5.5,
  renamed "Operator-forced / unplanned failover"): failover triggered by an
  **active-cluster master-component failure** from a clean, fully-synced
  `ACTIVE_IN_SYNC` state, with three variants — **S20a** NameNode down, **S20b**
  ZooKeeper down, **S20c** HMaster down (each `scale sts/... --replicas=0` on
  cluster-a). Per the run decision, S20 exercises **only** the designed
  cooperative `initiate-failover` surface and, where it cannot complete, records
  that inability as the finding — it does **not** force promotion via the
  unguarded `update -s STANDBY_TO_ACTIVE -F` znode overwrite (that path exists —
  `PhoenixHAAdmin.updateHAGroupStoreRecordInZooKeeper`, skipping
  `isTransitionAllowed` — but is reserved for a future S4 run once a purpose-built
  forced-failover surface lands). Documented the per-cluster ZK topology (each
  cluster's `HAGroupStoreRecord` in its *own* ZK, from `utils/create-ha-group.sh`)
  that predicts the three variants diverge: ZK-down (S20b) is the hard blocker
  because the active can neither write its transition nor be read by the peer.
  Confirmed via source that no `forceFailover()`/`force-failover` surface exists
  (`HAGroupStoreManager`, `PhoenixHAAdminTool` command switch `:191-231`) and that
  `initiate-failover` requires a live ACTIVE and polls for cooperative
  convergence (`isStableFailoverPair`, `:1227-1230`). Added S20 to both §7
  matrices.
