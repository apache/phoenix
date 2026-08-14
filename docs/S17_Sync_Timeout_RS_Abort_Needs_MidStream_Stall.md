# S17 — Remote Sync Hangs Past Timeout → RegionServer Abort: Reproducible Only With a Mid-Stream Stall, Not a Connection Drop

**Status:** REPRODUCED on the kind two-cluster DR test-bed 2026-08-08
(`toxiHAGroup`, Phoenix build `phoenix-server-5.3.0-consistent_failover`). The
S17 abort path (`syncFuture.get(syncTimeoutMs)` →
`PhoenixWALSyncTimeoutException` → `Abortable.abort()`) fires and the
RegionServer restarts and self-reconverges to `ACTIVE_IN_SYNC`.

**Companion to:** the S17 sections of `Phoenix_HA_Failover_Test_Scenarios.md`
and `Phoenix_HA_Failover_Scenarios_Shared.md` (both of which recommended a
fault type that does **not** reproduce the abort — corrected below), and
`S17b_Sync_Retry_False_Success_RPO_Loss.md` (the RPO variant of the same
peer-DN fault).

## TL;DR — the fault TYPE decides the outcome, not the timing

The S17 abort can only happen if an **application write thread's sync blocks for
the full `syncTimeoutMs` (30 s configured)**. The chain is:

```
HA-connection write
  → disruptor publishes EVENT_TYPE_SYNC
  → app RPC handler thread blocks on syncFuture.get(30000ms)   [ReplicationLogGroup.java:751]
  → consumer thread runs currentModeImpl.sync()
    → HDFS DFSOutputStream flush → waitForAckedSeqno()          (waits for peer-DN pipeline ACK)
```

For the timeout to fire, `waitForAckedSeqno()` must **hang while a block is
already open and packets are in flight** — a *mid-stream* stall. Two fault
families produce very different results:

| Fault | What HDFS sees | Where it's handled | Result |
|---|---|---|---|
| **Connection drop** — `iptables DROP`, FIS `pod-network-blackhole-port` | connection refused → `createBlockOutputStream` / `ConnectTimeoutException` (pipeline **setup** failure) | `SyncModeImpl.onEnter` / `log.init` on a **background** mode-init/rotation thread → `onFailure` → `transitionToStoreAndForward` | **Graceful SAF.** Never reaches the app-thread `syncFuture`. **No abort.** |
| **Mid-stream stall** — `tc netem delay` on an established `:9866` block, EBS `pause-volume-io`/`volume-io-latency` on the standby's storage | connection stays **ESTABLISHED**, block open, packets parked, ACKs never return → `waitForAckedSeqno` hangs | disruptor consumer `sync()` blocks → **app-thread** `syncFuture.get(30000)` times out | **`PhoenixWALSyncTimeoutException` → RS abort.** |

A drop refuses at the connection layer, so it fails *before* a block is open —
that failure is caught on a background thread and gracefully demotes the group
to STORE_AND_FORWARD. It structurally cannot block an application thread. This
was verified exhaustively: even a well-timed drop against a warm, actively-syncing
writer producing real mid-stream `ResponseProcessor` errors still degraded to SAF.

## The proof (test-bed, 2026-08-08)

```
22:07:03,643 ERROR [RpcServer.default.FPBQ.Fifo.handler=29,queue=2,port=16020]
    replication.ReplicationLogGroup: Aborting region server due to replication failure
  org.apache.phoenix.replication.PhoenixWALSyncTimeoutException:
    HAGroup toxiHAGroup replication log sync timed out after 30000 ms
22:07:38,646 ERROR ... regionserver.HRegionServer: ***** ABORTING region server 10.244.2.43,16020 ...
22:07:43,675 INFO  ... regionserver.HRegionServer: STOPPED: Aborting region server due to replication failure
```

The decisive detail: the exception fires on an **`RpcServer...handler`
thread** (`handler=29,port=16020`) — an application write thread blocked in the
sync path, i.e. exactly `syncFuture.get(30000)` timing out. Contrast the drop
outcome, where the failure surfaces on a background mode-init thread and never
touches an app thread. After the abort the container restarted (restart count
1→ back to `Running`) and, once real wall-clock resumed (host-suspend froze the
forwarder rounds in between), the group self-reconverged to
`a=ACTIVE_IN_SYNC / b=STANDBY` with **no writes and no manual recycle** — the
forwarder-fix behaviour (see `S17_Forwarder_Shared_Queue_Starvation_No_Reconverge.md`).

## Why the previously-documented faults do NOT reproduce the abort

- **FIS `aws:eks:pod-network-blackhole-port` (the doc's *default* for S17).** A
  blackhole is a silent drop — same as `iptables DROP`. It refuses the peer-DN
  connection → setup-phase failure → SAF. Correct for driving **S17b** (the
  false-success RPO variant, where `output.sync()` *throws*) or a plain SAF
  fallback, **not** the S17 abort.
- **Toxiproxy `timeout` toxic on `remote-hdfs` (`:9000`) (the test-bed doc's
  recommendation).** Toxiproxy fronts only the peer **NameNode** `:9000` for
  metadata RPCs. Block **data** streams RS→DN directly on `:9866` and is never
  proxied. Hanging `:9000` stalls NN metadata (e.g. `log.init` /
  `createNewWriter` header sync), which runs through `SyncModeImpl.onEnter` on a
  background thread → SAF, not the app-thread sync. So it does not drive the
  abort either.
- **What actually works:** a stall on the established RS→peer-DN `:9866` block —
  the connection must be accepted and a block open, then ACKs withheld. On the
  test-bed this is `tc netem delay`; on AWS FIS it is the EBS
  `pause-volume-io` / `volume-io-latency` variants against the **standby's**
  storage volume (the doc already lists these, but as a secondary "block-layer
  variant" — they are in fact the *only* variant that aborts).

## Test-bed recipe (`tc netem` mid-stream stall)

1. **Find the RS hosting the `PHOENIX_HA_T` region** — all HA-connection writes
   funnel to the single region host, so the abortable `syncFuture` lives only
   there. Shaping any other RS delays only idle rotation-writer dribble and
   nothing blocks.
   ```
   kubectl -n cluster-a exec hmaster-0 -c hmaster -- sh -c \
     '/hbase/bin/hbase shell -n' <<< 'get "hbase:meta","PHOENIX_HA_T,,<id>.","info:server"'
   ```
2. **Get its host-side PID on the right kind node** (the regionserver image is
   distroless — no `tc`/`nsenter`/shell inside; the kind node has them):
   ```
   CID=$(kubectl -n cluster-a get pod <rs> -o jsonpath='{.status.containerStatuses[?(@.name=="regionserver")].containerID}' | sed 's|.*/||')
   HPID=$(docker exec <node> sh -c "crictl inspect $CID | tr ',' '\n' | grep -oE '\"pid\": *[0-9]+' | grep -oE '[0-9]+' | sort -rn | head -1")
   # take the LARGEST pid — the in-container view reports "pid": 1
   ```
3. **Scope netem to peer-DN `:9866` egress only** inside the pod's net namespace
   (never all egress — delaying ZK/HMaster RPC would abort for the wrong reason):
   ```
   docker exec <node> nsenter -t $HPID -n tc qdisc add dev eth0 root handle 1: prio
   docker exec <node> nsenter -t $HPID -n tc qdisc add dev eth0 parent 1:3 handle 30: netem delay 120000ms
   for dst in <cluster-b datanode-0/1/2 podIPs>; do
     docker exec <node> nsenter -t $HPID -n tc filter add dev eth0 protocol ip parent 1:0 prio 1 u32 \
       match ip dst $dst/32 match ip dport 9866 0xffff flowid 1:3
   done
   ```
   Verify the delay is engaging: `tc -s filter show dev eth0` (rule hit/success
   climbing) and `tc -s qdisc show dev eth0` (netem **backlog** holding packets).
   Peer DN IPs drift on pod reschedule — re-check each run, and do **not** use the
   local cluster-a DN IPs the RS also connects to.
4. **Keep HA-connection load running** to `toxiHAGroup` (e.g. `ha-loadloop.sh`
   `BATCH=200`) so a peer block stays open and mid-flush when the delay engages.
   The abort fires ~`syncTimeoutMs` (30 s) after the stall lands on a live sync.
5. **Teardown:** `tc qdisc del dev eth0 root` in the netns; confirm no stray
   netem/DROP anywhere; stop the loadloop. The group goes `ACTIVE_NOT_IN_SYNC`
   transiently, then self-reconverges.

`syncTimeoutMs` = `phoenix.replication.log.sync.timeout.ms` (rendered 30000 on
the test-bed). If unset it is computed as `hbase.regionserver.wal.sync.timeout`
(300000) + ZK session timeout — set it low so the abort is quick.

## Code references

- `ReplicationLogGroup.syncInternal()` wait: `syncFuture.get(syncTimeoutMs, MILLISECONDS)` (`ReplicationLogGroup.java:751`).
- `PhoenixWALSyncTimeoutException` constructed `:764`, handed to `abort()`.
- `ReplicationLogGroup.abort()` (`:1079-1090`) → `Abortable.abort()` (`:1087`),
  the `Abortable` being the `RegionServerServices` captured via `@CoreCoprocessor`
  + `HasRegionServerServices` in `IndexRegionObserver` (`:734-737`).
- `SyncModeImpl.onEnter` / `onFailure` → `transitionToStoreAndForward`
  (`SyncModeImpl.java`) — the background-thread path a connection drop takes.
