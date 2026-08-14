# S20b — active cluster's ZooKeeper down: cooperative failover is impossible, and there is no first-class forced-promotion surface

**Status:** confirmed finding, observed on the local kind DR test-bed (2026-08-07).
Unlike S20a (NameNode) and S20c (HMaster), losing the **active cluster's
ZooKeeper** takes down the failover surface itself. The designed cooperative
`initiate-failover` cannot run, the standby will not self-promote, and clients
cannot route. The only way out is a manual, operator-driven forced promotion of
the survivor — a path that exists but is neither packaged nor guarded for this
use.

Companion to [S20 in Phoenix_HA_Failover_Test_Scenarios.md](Phoenix_HA_Failover_Test_Scenarios.md)
and to [S20c](S20c_Failover_HMaster_Down_SystemTable_Stall.md).

---

## What was tested

From a clean, fully-synced pair (active = cluster-a `ACTIVE_IN_SYNC`, standby =
cluster-b `STANDBY`), the **active** cluster's ZooKeeper was scaled to zero
(`kubectl -n cluster-a scale sts/zookeeper --replicas=0`), then the designed
cooperative failover was issued:

```
kubectl -n cluster-a exec regionserver-0 -- \
  /hbase/bin/hbase org.apache.phoenix.jdbc.PhoenixHAAdminTool \
  initiate-failover -g testHAGroup
```

**Outcome:** the command **fast-fails in ~6 s** at the very first ZK read, with
no state change recorded on either cluster. Confirmed from three angles:

1. **Failover tool** — throws `Failed to get HAGroupStoreRecord` after the
   Curator connect timeout; the source transition is never written.
2. **Standby** — cluster-b, having lost peer visibility, degrades
   `STANDBY → DEGRADED_STANDBY` on its own. It does **not** self-promote.
3. **HA client** — a data-plane connection reads
   `ClusterRoleRecord{... role1=UNKNOWN, ... role2=STANDBY}` and logs
   `Failed to connect to active cluster in HA group testHAGroup`. The FAILOVER
   policy correctly refuses to connect: there is no ACTIVE endpoint, and it will
   not invent one (no split-brain).

There is no RPO event *from the tooling itself* — nothing wrote anything. The
data risk comes entirely from what the (possibly still-alive) old active does
while partitioned; see "Fencing" below.

## Why cooperative failover cannot run

`initiate-failover`
(`PhoenixHAAdminTool.executeInitiateFailover:624-708`) is a pure ZK/Curator
path, and its **first** action is to read and then write the **active
cluster's** ZK:

- `PhoenixHAAdmin admin = new PhoenixHAAdmin(zkUrl, ...)` (`:643`) — a client on
  the active's `/phoenix/consistentHA/...` namespace.
- `admin.getHAGroupStoreRecordInZooKeeper(haGroupName)` (`:647`) — **reads** the
  active znode. With the active's ZK down this blocks on the Curator connect
  timeout, hardcoded at `PHOENIX_HA_ZK_CONNECTION_TIMEOUT_MS_DEFAULT = 4000` ms
  (`HighAvailabilityGroup.java:523`), then throws.
- The intended next step, `manager.initiateFailoverOnActiveCluster(haGroupName)`
  (`:684`), would **write** `ACTIVE_IN_SYNC → ACTIVE_IN_SYNC_TO_STANDBY` back
  into that same dead ZK.

The whole cooperative protocol is anchored in the active recording its own
demotion first. When the active's ZK is the thing that's gone, there is **no
code path** that promotes the standby without it. This is the S20b prediction
made concrete: the safe, guard-checked failover surface has a hard dependency on
the *dying* cluster — precisely when an operator most needs to promote the
survivor, the supported tool cannot execute.

Contrast with S20a (NameNode) and S20c (HMaster): neither NN nor HM is on the
transition path, so cooperative failover still completes. ZooKeeper is the single
hard dependency of the failover surface.

## Recovery in production when the active's ZK is unrecoverable

Recovery is a **manual, operator-driven forced promotion of the survivor**. The
entire risk is split-brain, because the old active cannot be cooperatively
demoted — it may still believe it is `ACTIVE`.

### 1. Fence the old active first — non-negotiable

The active's ZK being unreachable *to the standby* does not mean the active is
dead. Its RegionServers may still be up, still holding an `ACTIVE` record in
their local (now-partitioned) ZK, still accepting HA-connection writes from any
client that can still reach them, still writing `.plog` locally. Before promoting
the survivor you must guarantee the old active cannot serve writes:

- Stop the old active's RegionServers/HMaster, or network-isolate the namespace /
  VIP, **or** confirm the outage that killed its ZK also took its RS tier.
- Remove the old active from client-facing DNS/VIP so no client routes there.

There is no software interlock doing this — the cooperative demotion *was* the
interlock, and it is exactly what's unavailable. Fencing is manual and must
precede step 2. Any writes the old active accepted after the partition but before
fencing are unrecoverable divergence — that is the RPO cost of an
unrecoverable-ZK event and the reason fencing must be aggressive.

### 2. Force-promote the survivor against its own (healthy) ZK

The only existing forced surface is the `update … -F` path, run from a survivor
pod pointed at the survivor's own ZK:

```
kubectl -n cluster-b exec regionserver-0 -- /hbase/bin/hbase \
  org.apache.phoenix.jdbc.PhoenixHAAdminTool \
  update -g testHAGroup -s ACTIVE_IN_SYNC -av -F
```

What `-F` does and doesn't do (`PhoenixHAAdminTool.validateUpdate:1351-1386`):

- It waives **only** Validation #3 — "state change requires a valid transition"
  (`:1366-1371`). This is what lets `DEGRADED_STANDBY`/`STANDBY → ACTIVE_IN_SYNC`
  go through despite not being a legal cooperative transition.
- It does **not** waive Validation #1 (admin version must strictly increment) —
  hence `-av`/`--auto-increment-version`, which reads the current version and
  bumps it. `-F` also does not waive the immutable-field or required-field checks
  (policy, both cluster URLs, peer ZK URL remain present, still naming the old
  active).
- The write goes through `admin.updateHAGroupStoreRecordInZooKeeper(...)` with
  **optimistic locking on the current ZK version** (`:1397`) — a safe
  compare-and-set against the survivor's live znode. It touches only the
  survivor's ZK, so none of the dead-ZK problems apply.

### 3. Repoint clients

Once the survivor's record reads `ACTIVE_IN_SYNC`, the client CRR sees
`role2=ACTIVE` and the FAILOVER policy routes there. Nothing else client-side is
required — that is the whole point of the CRR indirection.

### 4. Rebuild the pair when the old active returns

The old active cannot rejoin as `ACTIVE` — that would be a second active. When
its ZK is rebuilt, force-set its HA record to `STANDBY` so it re-attaches as the
new standby and back-replication re-establishes.

## The product gap

**There is no first-class forced-failover surface.** The safe, guard-checked
promotion path (`initiate-failover`) has a hard dependency on the *dying*
cluster's ZK, so the only way through an unrecoverable-active-ZK event is the raw
`update -F` znode overwrite, which:

- bypasses the transition-legality guard entirely (its job — but also means no
  sanity check that you aren't creating a second active);
- does **no fencing** of the old active — the operator carries 100% of the
  split-brain safety burden manually; and
- isn't documented or packaged as a "promote-survivor" DR action — it's a generic
  admin edit that happens to be usable this way.

### Suggested hardening

- Add a purpose-built `force-promote` command that (a) requires an explicit
  fencing acknowledgement flag, (b) writes only the local ZK, and (c) refuses
  unless the local record is `STANDBY`/`DEGRADED_STANDBY` — turning today's
  freehand `update -F` into an auditable, single-purpose DR operation.
- Consider whether `DEGRADED_STANDBY` should expose an operator-triggered
  (never automatic) promotion, so the survivor's own tooling can drive recovery
  without a generic record edit.
- Document the fence-then-force-promote runbook in the operator guide; it is the
  only recovery for a lost active ZK and is currently tribal knowledge.

## Repro (test-bed)

1. Clean, fully-synced pair; note which cluster is `ACTIVE` (say cluster-a).
2. `kubectl -n cluster-a scale sts/zookeeper --replicas=0`; confirm the pod is gone.
3. `kubectl -n cluster-a exec regionserver-0 -- /hbase/bin/hbase \
   org.apache.phoenix.jdbc.PhoenixHAAdminTool initiate-failover -g testHAGroup`
   → observe `Failed to get HAGroupStoreRecord` fast-fail (~6 s).
4. Observe cluster-b degrade `STANDBY → DEGRADED_STANDBY`; an HA client connection
   reads `role1=UNKNOWN, role2=STANDBY` and refuses to connect.
5. Recovery drill: fence cluster-a (scale RS/HM to 0), then
   `kubectl -n cluster-b exec regionserver-0 -- /hbase/bin/hbase \
   org.apache.phoenix.jdbc.PhoenixHAAdminTool update -g testHAGroup \
   -s ACTIVE_IN_SYNC -av -F`; verify an HA client now routes to cluster-b and
   commits writes.
6. Teardown: restore cluster-a's ZK, force-set cluster-a to `STANDBY`, confirm the
   pair re-syncs.

Pod-log time = host time + 7h.