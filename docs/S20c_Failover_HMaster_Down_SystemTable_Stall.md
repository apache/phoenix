# S20c — `initiate-failover` with the active's HMaster down: SYSTEM.HA_GROUP stall + fragile tolerance

**Status:** potential problem, observed on the local kind DR test-bed (2026-08-07).
The cooperative failover *does* complete with zero RPO, but it (a) incurs a
~2-minute stall waiting on a dead HMaster it does not actually need, and (b)
survives HMaster loss only by accident of initialization ordering — a slightly
different timing would abort the failover.

Companion to [S20 in Phoenix_HA_Failover_Test_Scenarios.md](Phoenix_HA_Failover_Test_Scenarios.md).

---

## What was tested

From a clean, fully-synced pair (active = cluster-b `ACTIVE_IN_SYNC`, standby =
cluster-a `STANDBY`, OUT empty), the active cluster's HMaster was scaled to zero
(`kubectl -n cluster-b scale sts/hmaster --replicas=0`), then the designed
cooperative failover was issued from a **surviving RegionServer pod** on the same
cluster:

```
kubectl -n cluster-b exec regionserver-0 -- \
  /hbase/bin/hbase org.apache.phoenix.jdbc.PhoenixHAAdminTool \
  initiate-failover -g testHAGroup
```

**Outcome:** failover succeeded — clean role flip to cluster-a `ACTIVE`, cluster-b
`STANDBY`, no split-brain, zero RPO (all 650 pre-fault rows preserved,
post-failover writes commit on the new active and replicate). **But the command
took ~2m11s** (18:39:51 → 18:42:02), almost all of it spent inside a failing,
HMaster-dependent call that the failover transition never needed.

## The stack trace

The command printed this to stderr *before* `✓ Failover initiated`, then
continued to success:

```
	at org.apache.hadoop.hbase.client.RpcRetryingCallerImpl.callWithRetries(RpcRetryingCallerImpl.java:141)
	at org.apache.hadoop.hbase.client.HBaseAdmin.executeCallable(HBaseAdmin.java:3025)
	at org.apache.hadoop.hbase.client.HBaseAdmin.getTableDescriptor(HBaseAdmin.java:578)
	at org.apache.hadoop.hbase.client.HBaseAdmin.getDescriptor(HBaseAdmin.java:361)
	at org.apache.phoenix.query.ConnectionQueryServicesImpl.ensureTableCreated(ConnectionQueryServicesImpl.java:1915)
	... 28 more
Caused by: org.apache.hadoop.hbase.MasterNotRunningException: java.io.IOException:
    org.apache.zookeeper.KeeperException$NoNodeException: KeeperErrorCode = NoNode for /hbase/master
	...
Caused by: org.apache.zookeeper.KeeperException$NoNodeException: KeeperErrorCode = NoNode for /hbase/master
```

Note the `... 28 more` and the absence of any `main`/`PhoenixHAAdminTool` frame at
the top: this is a **caught-and-logged** exception, not one that propagated out of
the command.

## Why the failover still worked — two independent code paths

### Path 1 — the failover transition itself is ZK-native (no HMaster)

`PhoenixHAAdminTool.executeInitiateFailover`
(`phoenix-core-client/.../jdbc/PhoenixHAAdminTool.java:624-708`) does its work
entirely against ZooKeeper via a Curator client:

- `PhoenixHAAdmin admin = new PhoenixHAAdmin(zkUrl, ...)` (`:643`) — a ZK client on
  the `/phoenix/consistentHA/...` namespace.
- `admin.getHAGroupStoreRecordInZooKeeper(haGroupName)` (`:647`) — reads the znode.
- `manager.initiateFailoverOnActiveCluster(haGroupName)` (`:684`) →
  `HAGroupStoreClient.setHAGroupStatusIfNeeded(ACTIVE_IN_SYNC_TO_STANDBY)`
  (`HAGroupStoreManager.java:415`) — **writes the znode**.
- `pollForStateTransition(...)` (`:691`) — polls local + peer znodes until
  `isStableFailoverPair` holds.

The HMaster is on none of these steps. This is the intended architecture: the HA
state machine lives in ZK so it survives HBase-master loss. Given ZK is up, the
transition completes in "0 seconds" once initiated.

### Path 2 — an incidental SYSTEM.HA_GROUP read that needs the HMaster, whose failure is swallowed

Building the `HAGroupStoreClient`
(`HAGroupStoreManager.getHAGroupStoreClientAndSetupFailoverManagement:612` →
`getHAGroupStoreClient`) constructs the client, whose constructor
(`HAGroupStoreClient.java:307-344`) does ZK cache setup **and** starts a periodic
ZK→system-table sync job (`startPeriodicSyncJob():330,838`). Any read of
SYSTEM.HA_GROUP goes through a fresh Phoenix JDBC connection:

```java
// HAGroupStoreClient.getSystemTableHAGroupRecord():628-635
PhoenixConnection conn = (PhoenixConnection) DriverManager
    .getConnection(JDBC_PROTOCOL_ZK + JDBC_PROTOCOL_SEPARATOR + zkUrl);
```

On first connect, the Phoenix driver verifies/creates the SYSTEM tables
(`ConnectionQueryServicesImpl.ensureTableCreated:1915` →
`HBaseAdmin.getTableDescriptor` → **HMaster RPC**). With the HMaster dead this
throws `MasterNotRunningException` (root cause `NoNode for /hbase/master`) — but
only after `RpcRetryingCallerImpl.callWithRetries` grinds through the **full HBase
client retry schedule**. That retry loop is the ~2-minute stall.

Two catch sites absorb it so it never reaches the transition:

- The constructor wraps all init in `try { ... } catch (Exception e)`
  (`:338-344`): on failure it logs `"Unexpected error occurred while initializing
  HAGroupStoreClient, marking cache as unhealthy"`, sets `isHealthy = false`, and
  `close()`s — **it does not rethrow**.
- The periodic sync job's `syncZKToSystemTable()` is itself wrapped
  (`:866-912+`, guarded by `isHealthy` and a trailing `catch (Exception e)`), and
  system-table *writes* go through `updateSystemTableHAGroupRecordSilently()`
  (`:710`, "best effort basis … log the error and continue").

## The problem

**1. HMaster-down tolerance is ordering-dependent, not guaranteed.**
The transition needs exactly one thing from the client:
`getHAGroupStoreRecord()` (`HAGroupStoreManager.java:397`), which is served from
the **ZK `pathChildrenCache`**. In this run the cache initialized *before* the
system-table read failed, so `isHealthy` was true and the record was available.
But the broad `catch (Exception)` at `:338` covers the cache setup
(`startCache:314`, `initializeZNodeIfNeeded:316`) too. If the HMaster-dependent
failure had instead surfaced during those earlier steps — or if
`initializeZNodeIfNeeded` had taken its `haGroupStoreRecord == null` branch and
called `getSystemTableHAGroupRecord()` (`:518`) to bootstrap the znode — the
client would be marked `isHealthy = false`, `getHAGroupStoreRecord()` would return
null, and `initiateFailoverOnActiveCluster` would throw
`"Current HAGroupStoreRecord is null for HA group"` (`HAGroupStoreManager.java:399`).
**The unplanned-HMaster-loss failover would then fail** — on exactly the scenario
an operator most needs it.

**2. A ~2-minute stall on the critical failover path, for data the transition
does not use.** The `MasterNotRunningException` is not fast-failed; it is the sum
of the HBase RPC retry backoff against a dead master. During an unplanned failover
this is dead time bolted onto RTO, caused entirely by an incidental system-table
touch.

## Suggested hardening

- **Decouple the failover transition from any SYSTEM.HA_GROUP access.** When the
  ZK znode already exists (the common case), the transition path should never open
  a Phoenix JDBC connection. `initializeZNodeIfNeeded()` already gates the
  system-table read on `znode == null`; the remaining trigger is the periodic
  sync job (and lazy SYSTEM-table verification on first `DriverManager.getConnection`).
  Consider deferring `startPeriodicSyncJob()` until after the client is confirmed
  healthy, and/or making the admin-tool code path construct a ZK-only client
  variant that does not start the sync job at all.
- **Fast-fail the system-table connection.** Cap the HMaster RPC with a short
  operation/retry timeout (or a `MasterNotRunning` fast-fail) for the HA client's
  system-table sync so a dead master costs seconds, not the full retry schedule.
- **Do not let a system-table failure mark the client unhealthy.** The ZK cache is
  the source of truth for the state machine; a SYSTEM.HA_GROUP sync failure should
  degrade the *sync* (already "best effort" for writes) without flipping
  `isHealthy = false`. Narrow the constructor's `catch (Exception)` (`:338`) so
  system-table errors don't tear down an otherwise-healthy ZK client.

## Repro (test-bed)

1. Clean, fully-synced pair; note which cluster is `ACTIVE`.
2. `kubectl -n <active-ns> scale sts/hmaster --replicas=0`; confirm the pod is gone.
3. `kubectl -n <active-ns> exec regionserver-0 -- /hbase/bin/hbase \
   org.apache.phoenix.jdbc.PhoenixHAAdminTool initiate-failover -g testHAGroup`
4. Observe: `MasterNotRunningException` / `NoNode for /hbase/master` stack trace on
   stderr, a multi-minute wall-clock, then `✓ Failover completed successfully`.
5. Verify both ZKs show a clean role flip and zero RPO.
6. Teardown: `kubectl -n <active-ns> scale sts/hmaster --replicas=1`.

Pod-log time = host time + 7h.