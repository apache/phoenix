# HDFS IPC Connect Timeout Analysis

Analysis of how long `createPeerShardManager()` blocks when the standby NameNode is unavailable.

## Context

When `SyncModeImpl.onEnter()` calls `logGroup.createPeerShardManager()`, it invokes
`FileSystem.get(uri, conf)` followed by `fs.exists(rootDirPath)`. If the standby NameNode is
down, the HDFS client retries with configurable timeouts before throwing IOException.

## IPC Client Configuration Defaults

| Config Key | Default | Purpose |
|-----------|---------|---------|
| `ipc.client.connect.timeout` | 20,000 ms (20s) | Per-attempt socket connect timeout |
| `ipc.client.connect.max.retries` | 10 | Retries on connection refused |
| `ipc.client.connect.max.retries.on.timeouts` | 45 | Retries on socket timeout |
| `ipc.client.connect.retry.interval` | 1,000 ms (1s) | Fixed sleep between retries |

Source: `CommonConfigurationKeysPublic.java` (lines 393-423)

## HDFS HA Failover Configuration Defaults

When HA NameNodes are configured, `AbstractNNFailoverProxyProvider` overrides the IPC retry
settings to 0 per NameNode and manages retries at the failover level.

| Config Key | Default | Purpose |
|-----------|---------|---------|
| `dfs.client.failover.connection.retries` | 0 | Connection retries per NN (overrides IPC retries) |
| `dfs.client.failover.connection.retries.on.timeouts` | 0 | Timeout retries per NN |
| `dfs.client.failover.max.attempts` | 15 | Total failover attempts across all NNs |
| `dfs.client.failover.sleep.base.millis` | 500 ms | Exponential backoff base |
| `dfs.client.failover.sleep.max.millis` | 15,000 ms (15s) | Max sleep between failover attempts |

Source: `HdfsClientConfigKeys.java` (lines 358-381)

## Total Blocking Time Estimates

### Non-HA (Single NameNode)

**Connection refused (NN process down):**
```
10 retries x 1s sleep = ~10 seconds
```

**Socket timeout (NN unreachable / packets dropped):**
```
45 retries x (20s timeout + 1s sleep) = ~945 seconds (~15.75 minutes)
```

### HA NameNodes (Both Down)

With HA, the failover proxy sets IPC retries to 0 per NN. Each NN gets a single attempt,
then failover occurs with exponential backoff.

**Connection refused (both NN processes down):**

| Component | Calculation | Time |
|-----------|-------------|------|
| 15 connect attempts | 15 x ~instant | ~0s |
| Backoff sleeps (14 intervals) | 500 + 1000 + 2000 + 4000 + 8000 + 9x15000 ms | ~150s |
| **Total** | | **~2.5 minutes** |

**Socket timeout (both NNs unreachable, packets dropped):**

| Component | Calculation | Time |
|-----------|-------------|------|
| 15 connect attempts | 15 x 20s | 300s |
| Backoff sleeps (14 intervals) | 500 + 1000 + 2000 + 4000 + 8000 + 9x15000 ms | ~150s |
| **Total** | | **~450s (~7.5 minutes)** |

## Impact on ReplicationLogGroup

During `SyncModeImpl.onEnter()`, the disruptor event handler thread blocks for the duration
above before IOException triggers degradation to STORE_AND_FORWARD. During this time:

1. Mutations queue in the ring buffer
2. Once the ring buffer is full, application threads block on `append()`
3. After the timeout, `updateModeOnFailure()` transitions to SAF mode
4. Queued mutations drain normally via the local shard manager

## Key Source Files

- `hadoop/hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/CommonConfigurationKeysPublic.java`
- `hadoop/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/client/HdfsClientConfigKeys.java`
- `hadoop/hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/ipc/Client.java` (setupConnection, lines 592-679)
- `hadoop/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/server/namenode/ha/AbstractNNFailoverProxyProvider.java` (lines 69-84)
