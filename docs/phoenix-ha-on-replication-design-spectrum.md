# Phoenix HA on the Data Replication Design Spectrum

Analysis of how Phoenix HA maps to the replication design categories from
[Data Replication Design Spectrum](https://transactional.blog/blog/2024-data-replication-design-spectrum) (2024-07-31).

That blog categorizes consistent replication algorithms into three families:

1. **Failure Masking (Quorums)** — Paxos, ABD; 2f+1 replicas; zero unavailability on failure
2. **Failure Detection (Reconfiguration)** — PacificA, Chain Replication; f+1 replicas; transient unavailability during reconfiguration
3. **Hybrid (Leader-Based Consensus)** — Raft, Multi-Paxos, ZAB; 2f+1 replicas with a designated leader

---

## Where Phoenix Falls: Failure Detection / Reconfiguration (Primary-Backup)

Phoenix HA is closest to the **reconfiguration-based primary-backup** category (like PacificA),
not quorum-based or leader-based consensus.

### Two replicas, not 2f+1

Phoenix uses exactly **2 clusters** (Active + Standby) — the f+1 replica count characteristic
of reconfiguration approaches. There is no quorum. Writes go to the Active cluster, get
synchronously replicated to the Standby's HDFS, then ACK to the client.

This matches the blog's observation that reconfiguration systems need fewer replicas and have
better storage efficiency (~50% for f=1 vs. quorums' ~33%).

### Explicit reconfiguration on failure, not failure masking

When the standby's HDFS becomes unreachable, Phoenix does not silently mask the failure — it
**explicitly reconfigures** by switching from `SYNC` to `STORE_AND_FORWARD` mode.

Cluster role changes (Active <-> Standby) require **operator-triggered reconfiguration** via
`PhoenixHAAdmin`, atomically swapped in ZooKeeper. This is the defining characteristic of the
reconfiguration category: "necessitating explicit reconfiguration around identified failures."

### No consensus protocol between clusters

Unlike Raft/Multi-Paxos, there is no leader election protocol or log consensus between the
two clusters. ZooKeeper serves as an **external configuration oracle** (exactly like PacificA's
"configuration manager"), not as a consensus participant in the data path.

---

## Key Differences from Pure Reconfiguration Systems

Phoenix makes several hybrid choices that deviate from pure reconfiguration:

| Aspect | Pure Reconfiguration (PacificA) | Phoenix HA |
|---|---|---|
| Failure detection | Lease-based heartbeats | Write-path failure (IOException) + ZK watches |
| Failover trigger | Automatic via config manager | Manual (operator-initiated) for role swap |
| Degraded mode | Unavailable until reconfigured | Store-and-Forward — writes continue locally |
| Read on standby | Not typically | Yes — "lookback" reads allowed on standby |

### Store-and-Forward: Avoiding the 100% Unavailability Penalty

The blog identifies the main weakness of reconfiguration systems as **100% transient
unavailability** during failure detection and reconfiguration. Phoenix avoids this through
Store-and-Forward:

- When the standby is unreachable, writes buffer locally on the Active cluster's HDFS.
- The Active cluster **never blocks writes**, even when replication is degraded.
- Trade-off: RPO weakens from zero (linearizable) to bounded-staleness during SAF mode.

This is Phoenix's most distinctive departure from the blog's framework.

---

## Consistency Guarantees

The blog scopes its analysis to **linearizable** systems. Phoenix targets:

- **Zero RPO (linearizable failover)** in SYNC mode — the standby has all committed writes
  before the client gets an ACK.
- **Bounded staleness** in STORE_AND_FORWARD mode — the standby falls behind, and a forced
  failover could lose data from the local queue.
- **Idempotent replay** — replication log records carry full cell coordinates (row, column,
  timestamp), so replays are safe and order-independent. Similar to the blog's mention of
  commutative operations as an optimization.

---

## Metrics Comparison (f=1, 2 clusters)

| Metric | Quorum (blog) | Reconfiguration (blog) | Phoenix HA |
|---|---|---|---|
| Replicas needed | 3 (2f+1) | 2 (f+1) | 2 |
| Storage efficiency | 33% | 50% | 50% |
| Write latency | 1 RTT (parallel) | 2 RTTs (serial) | 2 RTTs (local WAL + remote HDFS) |
| Read bandwidth | 33% | 100% (CRAQ) | ~100% on Active; standby for lookback |
| Unavailability on failure | 0% | 100% (transient) | Mitigated by SAF (writes never block) |

---

## The Operator-in-the-Loop Design Choice

The blog notes that reconfiguration systems need a "configuration manager" to orchestrate
failover. Phoenix puts a **human operator** in this role rather than automating it.

The HA state machine in `HAGroupStoreRecord` has ~13 states — far more complex than simple
Active/Standby — precisely because it models the careful, staged transitions an operator needs
to make safely (e.g., `ACTIVE_IN_SYNC_TO_STANDBY` -> drain logs -> `STANDBY`).

Rationale: **automated failover can cause split-brain** with only 2 replicas and no
tie-breaking quorum. Phoenix uses ZooKeeper's linearizable guarantees to prevent two Active
clusters, but leaves the trigger to a human who can assess whether the situation truly
warrants failover.

---

## Replication Modes State Machine

```
SYNC ──(standby unreachable)──> STORE_AND_FORWARD
  ^                                     |
  |                                     v
  └──(queue drained)──── SYNC_AND_FORWARD
```

- **SYNC**: Normal operation. Mutations written synchronously to standby HDFS before client ACK.
- **STORE_AND_FORWARD**: Degraded. Mutations queued locally; standby marked DEGRADED_STANDBY.
- **SYNC_AND_FORWARD**: Transitional. New writes go to standby synchronously while local queue drains.

---

## Summary

Phoenix HA sits in the **reconfiguration / primary-backup** category, most analogous to
**PacificA** with ZooKeeper as the configuration manager. Its key innovation relative to
the blog's framework is the **Store-and-Forward degraded mode** that avoids the "100%
transient unavailability" penalty — at the cost of weakening consistency from linearizable
to bounded-staleness during degraded operation.

---

## References

- Blog: https://transactional.blog/blog/2024-data-replication-design-spectrum
- PacificA paper: "PacificA: Replication in Log-Based Distributed Storage Systems" (2008)
- Phoenix HA design: `docs/Phoenix_HA_ReArchitecture_for_Consistent_Failover.md`
- Replication sync semantics: `docs/replication-log-sync-semantics.md`
- Store-and-Forward: `phoenix-core-server/.../replication/StoreAndForwardModeImpl.java`
- HA state machine: `phoenix-core-client/.../jdbc/HAGroupStoreRecord.java`
