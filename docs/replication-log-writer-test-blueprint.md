# Replication Log Writer — Test Blueprint

**Scope.** Concrete test plan for the server-side replication log writer
introduced in PHOENIX-7562, as required by the design document
`Phoenix_HA_ReArchitecture_for_Consistent_Failover.md`. Synthesized from
three exploration passes: (1) existing Phoenix unit/IT coverage,
(2) HBase fault-injection hooks in `hbase-server`, (3) Hadoop/HDFS
fault-injection hooks in `hadoop-hdfs`.

Goal: every state edge in the failover state machine, and every durability
invariant from §"Synchronous Replication Failure Handling" of the design
doc, has either an automated test or a documented external chaos runbook.

---

## A. Tier classification matrix

| ID  | Scenario                                                              | Tier             | Notes                                                                         |
| --- | --------------------------------------------------------------------- | ---------------- | ----------------------------------------------------------------------------- |
| W1  | append/sync mechanics, batching, ring-buffer                          | **unit**         | already covered in `ReplicationLogGroupTest`                                  |
| W2  | rotation (time/size), staged-writer drain, replay                     | **unit**         | already covered                                                               |
| W3  | retry on `writer.sync()`, retry-with-rotation                         | **unit**         | already covered                                                               |
| W4  | SYNC → SAF flip on sync failure, SAF → sync futures                   | **unit**         | already covered                                                               |
| W5  | SAF both attempts fail → abort                                        | **unit**         | already covered                                                               |
| W6  | sync timeout → abort                                                  | **unit**         | already covered (`testSyncTimeout`)                                           |
| W7  | startup degrade when peer init throws / times out                     | **unit**         | already covered                                                               |
| W8  | `LogFileWriter` hsync ordering, codec round-trip                      | **unit**         | already covered (`LogFileWriterSyncTest`, format/codec tests)                 |
| W9  | corrupt / truncated `.plog` replayability                             | **unit**         | already covered in `ReplicationLogProcessorTestIT`; expand with crash variants|
| W10 | **forwarder idempotency on retry**                                    | **unit + 1-cluster IT** | new — spy `processFile`; assert OUT-side rename/skip on copy retry     |
| W11 | **concurrent close + heavy traffic / shutdown timeout**               | **unit** stress  | new — drive `close()` while N producer threads append                         |
| W12 | **mode-transition listener races**                                    | **unit**         | new — `subscribeToTargetState` callback firing during in-flight sync          |
| F1  | **ghost-write guard: local WAL sync fails → no .plog record**         | **1-cluster IT** | new — use HBase `FaultyFSLog`                                                 |
| F2  | **WAL committed, remote sync fails → `preWALRestore` re-ship**        | **2-cluster IT** | new — `DataNodeFaultInjector` against cluster2 DN, kill RS                    |
| F3  | **ACK boundary: client only sees success iff WAL+plog both durable**  | **2-cluster IT** | new — interleave failures, count standby rows                                 |
| F4  | **5-min sync timeout → RS abort (real path)**                         | **2-cluster IT** | new — `delayAckLastPacket` on cluster2 DN, assert RS aborts via `Abortable`   |
| F5  | **Degraded standby recovery (SYNC → SAF → SYNC_AND_FORWARD → SYNC)**  | **2-cluster IT** | new — stop cluster2 DN, write, restart DN, verify drain + state               |
| F6  | **SAF queue survives RS restart, OUT drains on cluster1 restart**     | **2-cluster IT** | new — `restartHBaseCluster(1)` mid-backlog                                    |
| F7  | **Graceful failover under writer load: zero data loss**               | **2-cluster IT** | new — `CLUSTERS.transitClusterRole` during load                               |
| F8  | **Abort failover mid-transition: writer continuity**                  | **2-cluster IT** | new — set ACTIVE→A2S then ABORT_TO_ACTIVE                                     |
| F9  | **Forced failover from degraded standby**                             | **2-cluster IT** | new — kill cluster1 with OUT backlog, force-promote cluster2                  |
| F10 | **Region split/merge during sync replication**                        | **2-cluster IT** | new — `admin.split` mid-load, verify cell equality across clusters            |
| F11 | **DN pipeline failure on peer HDFS triggers single retry then SAF**   | **2-cluster IT** | new — `DataNodeFaultInjector.failPipeline` on cluster2                        |
| F12 | **Peer NN failover mid-write**                                        | **2-cluster IT** | new — HA HDFS via `MiniDFSNNTopology.simpleHATopology` + `transitionToStandby`|
| F13 | **Misconfiguration guard: round duration > HDFS hard lease period**   | **unit** (optional) | rotation is driven by round duration, so the only way to hit lease expiration is operator misconfiguration; a config-validator test is enough |
| F14 | **Slow-DN detection** (`StreamSlowMonitor`)                           | **1-cluster IT** | new — `DataNodeFaultInjector.delaySendingAckToUpstream`                       |
| F15 | **Time-based round drift / scheduler resilience**                     | **unit**         | `EnvironmentEdgeManagerTestHelper.injectEdgeForPackage("org.apache.phoenix.replication", edge)` |
| C1  | Sustained mode flap (3+ min, randomized loss)                         | **external chaos** | chaos-mesh `NetworkChaos` on kind, mirrors S8 in failover doc               |
| C2  | Full cluster reprovision after forced failover                        | **external chaos** | `kind delete` + `kubectl apply`, per S4                                     |
| C3  | Long-running write soak with rolling RS restarts                      | **external chaos** | weekly job, not CI                                                          |
| C4  | DNS chaos, clock skew                                                 | **external chaos** | chaos-mesh `DNSChaos` / `TimeChaos`                                         |

Unit = `phoenix-core/src/test/java`. IT = `phoenix-core/src/it/java` with
`@Category(NeedsOwnMiniClusterTest.class)` so it lands in the
`reuseForks=false` failsafe execution.

---

## B. New Phoenix-side hooks to add

These are small, additive seams needed to make the IT scenarios
deterministic. All follow the existing
`IndexRegionObserver.setIgnoreSyncReplicationForTesting` precedent
(`@VisibleForTesting`, save-and-restore in `finally`).

### B1. Static `*ForTesting` knobs

| Class                                | Hook                                                                                | Used for                                       |
| ------------------------------------ | ----------------------------------------------------------------------------------- | ---------------------------------------------- |
| `ReplicationLog`                     | `static void setFailWriterCreateForTesting(int n)` — fail first N `createNewWriter` | F11, W3                                        |
| `ReplicationLogDiscoveryForwarder`   | `static void setFailFileCopyForTesting(int n)` — fail first N `processFile`         | W10, F5                                        |
| `ReplicationLogGroup`                | `static void setPeerInitDelayForTesting(long ms)` — sleep in `createPeerShardManager` | W7 (deterministic)                           |
| `IndexRegionObserver`                | `static void setFailLocalWALSyncForTesting(boolean)` — bridges to `FaultyFSLog`     | F1                                             |
| `ReplicationLogGroup`                | `static void setAbortHookForTesting(BiConsumer<String, Throwable>)` — observe abort without taking the JVM down | F4                  |

### B2. Protected factory seams

| Class                                       | Existing                                | Add                                                                                              |
| ------------------------------------------- | --------------------------------------- | ------------------------------------------------------------------------------------------------ |
| `ReplicationLog.createNewWriter()`          | `protected`, spied by `TestableLog`     | also expose `protected LogFileWriter newLogFileWriter()` so IT can install `FaultyLogFileWriter` |
| `ReplicationLogGroup.getOrCreatePeerShardManager()` | `protected`                       | also expose `protected FileSystem getFileSystem(URI)` so IT can install a `FaultyFileSystem` per URI scheme |
| `ReplicationLogDiscoveryForwarder.processFile()` | `protected`                        | already overridable; just calls the static knob above                                            |

### B3. Mode-transition observers (no new code)

`HAGroupStoreManager.subscribeToTargetState(...)` is already public — IT
tests use it to deterministically wait for `ACTIVE_NOT_IN_SYNC` /
`ACTIVE_IN_SYNC` instead of polling `logGroup.getMode()`.

---

## C. Reusable hooks from HBase / Hadoop

The exploration surveys turned up ~40 hooks. These are the seven we'll
actually need.

| Hook                                                                                                              | Where                                                | Phoenix tests that need it                       |
| ----------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------- | ------------------------------------------------ |
| `FaultyFSLog.setFailureType(SYNC/APPEND/NONE)`                                                                    | hbase-server test-jar                                | F1 (ghost-write guard via local WAL sync failure)|
| `MiniHBaseCluster.killRegionServer(ServerName)` + `MiniHBaseClusterRegionServer.kill()`                           | hbase-server test-jar                                | F2, F6, F7                                       |
| `HBaseTestingUtility.expireRegionServerSession(int)`                                                              | hbase-server test-jar                                | F8 (ZK session loss without process exit)        |
| `HBaseTestingUtility.restartHBaseCluster(1)`                                                                      | hbase-server test-jar                                | F6                                               |
| `DataNodeFaultInjector.set(injector)` — `delayAckLastPacket`, `failPipeline`, `delaySendingAckToUpstream`, `failMirrorConnection` | hadoop-hdfs main jar (production)            | F4, F11, F14                                     |
| `MiniDFSCluster.stopDataNode / restartDataNode / shutdownNameNode / transitionTo{Active,Standby}`                 | hadoop-hdfs test-jar                                 | F5, F12                                          |
| `EnvironmentEdgeManagerTestHelper.injectEdgeForPackage(edge, "org.apache.phoenix.replication")`                   | hbase-common test-jar                                | F15 (no edge bleed into rest of HBase)           |
| `GenericTestUtils.{LogCapturer, DelayAnswer, SleepAnswer, waitFor}`                                               | hadoop-common test-jar                               | many — log assertions and sync points            |

All HBase/Hadoop test-jar dependencies are already on the Phoenix test
classpath (used by `MiniHBaseCluster` today). The two production-jar
items (`DataNodeFaultInjector`, `EnvironmentEdgeManager`) are no-op in
prod, override-via-`set()` in tests — process-global, so **always wrap in
try/finally with save+restore**.

---

## D. Concrete new test classes

All paths under `phoenix-core/src/test/java/...` (unit) or
`phoenix-core/src/it/java/...` (IT). Each IT carries
`@Category(NeedsOwnMiniClusterTest.class)`.

### D1. Unit tier (extends `ReplicationLogBaseTest`)

```text
ReplicationLogGroupForwarderIdempotencyTest    (W10)
  - testForwarderRetriesDoNotDuplicate
  - testForwarderSkipsAlreadyCopiedFile
  - testForwarderPicksUpInProgressOnRestart

ReplicationLogGroupShutdownTest                (W11)
  - testCloseDuringHeavyAppend
  - testCloseTimesOutGracefully
  - testCloseFromMultipleThreads
  - testCloseAfterAbort

ReplicationLogGroupModeListenerRaceTest        (W12)
  - testStateChangeDuringInflightSync
  - testStateChangeRacesWithRotation

ReplicationLogTimeEdgeTest                     (F15)
  - testRotationFollowsInjectedEdge
  - uses EnvironmentEdgeManagerTestHelper.injectEdgeForPackage(
      new ManualEnvironmentEdge(), "org.apache.phoenix.replication")
```

Skeleton (W11):

```java
public class ReplicationLogGroupShutdownTest extends ReplicationLogBaseTest {
  @Test public void testCloseDuringHeavyAppend() throws Exception {
    int producers = 8;
    CountDownLatch start = new CountDownLatch(1);
    AtomicInteger completed = new AtomicInteger();
    ExecutorService pool = Executors.newFixedThreadPool(producers);
    for (int t = 0; t < producers; t++) {
      pool.submit(() -> {
        start.await();
        for (long i = 0; i < 10_000; i++) {
          try { logGroup.append("T", i, LogFileTestUtil.newPut("r" + i, i, 1)); }
          catch (IOException e) { break; }  // expected after close
        }
        completed.incrementAndGet();
      });
    }
    start.countDown();
    Thread.sleep(50);
    logGroup.close();           // assert: no deadlock, returns within shutdown timeout
    pool.shutdown();
    assertTrue(pool.awaitTermination(30, SECONDS));
    assertEquals(producers, completed.get());
    assertTrue(logGroup.isClosed());
  }
}
```

### D2. 1-cluster IT tier (extends `ParallelStatsDisabledIT` + `@Category(NeedsOwnMiniClusterTest.class)`)

```text
ReplicationLogWriterGhostWriteGuardIT          (F1)
  - testLocalWALSyncFailureDoesNotEmitPlog
  - uses: conf.setClass(WALFactory.WAL_PROVIDER, FaultyFSLog-wrapping-provider, ...)
          IndexRegionObserver.setFailLocalWALSyncForTesting(true)

ReplicationLogWriterSlowDNIT                   (F14)
  - testSlowPeerDNTriggersFlipToSAF
  - uses: DataNodeFaultInjector with delaySendingAckToUpstream(2x sync timeout)

ReplicationLogWriterForwarderRetryIT           (W10 IT counterpart)
  - testForwardingRetriesEventuallySucceed
```

Skeleton (F1):

```java
@Category(NeedsOwnMiniClusterTest.class)
public class ReplicationLogWriterGhostWriteGuardIT extends ParallelStatsDisabledIT {
  @BeforeClass public static void doSetup() throws Exception {
    Map<String,String> props = new HashMap<>();
    props.put(SYNCHRONOUS_REPLICATION_ENABLED, "true");
    // Point peer HDFS at a temp dir on the SAME minicluster (single-cluster IT)
    props.put(/*peer hdfs url*/, tmpPeer.toURI().toString());
    props.put("hbase.regionserver.hlog.provider", FaultyFSLogProvider.class.getName());
    setUpTestDriver(new ReadOnlyProps(props));
  }

  @Test public void testLocalWALSyncFailureDoesNotEmitPlog() throws Exception {
    String table = generateUniqueName();
    createTable(table);
    Path peerDir = getPeerLogDir();
    long preCount = countPlogFiles(peerDir);
    FaultyFSLog.setFailureType(FaultyFSLog.FailureType.SYNC);
    try {
      try (Connection c = DriverManager.getConnection(getUrl())) {
        c.createStatement().execute("upsert into " + table + " values (1, 'x')");
        c.commit();
        fail("Expected WAL sync IOException");
      } catch (SQLException expected) { /* propagated */ }
    } finally {
      FaultyFSLog.setFailureType(FaultyFSLog.FailureType.NONE);
    }
    // Critical assertion: no .plog file was emitted for the failed batch
    assertEquals("Ghost-write guard violated", preCount, countPlogFiles(peerDir));
  }
}
```

### D3. 2-cluster IT tier (extends `HABaseIT`)

```text
ReplicationLogWriterWALReplayIT                (F2)
  - testRemoteFailureReshippedViaPreWALRestore

ReplicationLogWriterACKBoundaryIT              (F3)
  - testNoACKWithoutBothDurabilities  (parameterized over fault stage)

ReplicationLogWriterSyncTimeoutAbortIT         (F4)
  - testRSAbortsAfterSyncTimeout

ReplicationLogWriterDegradedRecoveryIT         (F5)
  - testSyncToSafToSyncAndForwardToSync
  - testBacklogDrainOnPeerReturn

ReplicationLogWriterSAFSurvivesRestartIT       (F6)
  - testOutQueueDrainsAfterActiveRestart

ReplicationLogWriterGracefulFailoverIT         (F7)
  - testZeroDataLossUnderLoad

ReplicationLogWriterAbortFailoverIT            (F8)
  - testWriterContinuesAfterAbortToActive

ReplicationLogWriterForcedFailoverIT           (F9)
  - testForcedPromotionLosesOnlyOutQueue
  - testForcedPromotionPreservesSynced

ReplicationLogWriterPipelineFailureIT          (F11)
  - testPipelineFailureFlipsToSAF
  - testPipelineRecoveryDuringSync

ReplicationLogWriterPeerNNFailoverIT           (F12)
  - testWriterSurvivesPeerNNFailover

ReplicationLogConfigValidationTest             (F13, unit)
  - testRoundDurationAboveHardLeasePeriodRejected
  - (rotation is tied to round duration; no real lease-expiration code path
     unless the operator sets round duration > HDFS hard lease, so a config
     validator is sufficient — no IT needed)

ReplicationLogWriterRegionSplitIT              (F10)
  - testSplitDuringWriteMaintainsCellEquality
```

Skeleton (F2):

```java
@Category(NeedsOwnMiniClusterTest.class)
public class ReplicationLogWriterWALReplayIT extends HABaseIT {
  @BeforeClass public static void doSetup() throws Exception {
    HABaseIT.doBaseSetup();
    conf1.setInt(PHOENIX_REPLICATION_ROUND_DURATION_SECONDS_KEY, 10);
    CLUSTERS.start();
  }

  @Test public void testRemoteFailureReshippedViaPreWALRestore() throws Exception {
    String hag = name.getMethodName();
    CLUSTERS.initClusterRole(hag, HighAvailabilityPolicy.FAILOVER);
    String table = createTable(hag);

    // Snapshot the cluster2 DN fault hook so test bleed is impossible.
    DataNodeFaultInjector prior = DataNodeFaultInjector.get();
    AtomicBoolean failNext = new AtomicBoolean(true);
    DataNodeFaultInjector.set(new DataNodeFaultInjector() {
      @Override public void delayAckLastPacket(/* args */) throws IOException {
        if (failNext.compareAndSet(true, false)) {
          throw new IOException("Injected remote ack failure");
        }
      }
    });
    try {
      try (Connection c = CLUSTERS.getCluster1Connection(hag)) {
        c.createStatement().execute("upsert into " + table + " values (1, 'x')");
        c.commit();  // local WAL OK, remote plog write fails, then SAF or abort
      }
      // Force WAL replay: kill the RS hard before SAF forwarder drains
      ServerName rs = activeRS(CLUSTERS.getHBaseCluster1());
      CLUSTERS.getHBaseCluster1().getHBaseCluster().killRegionServer(rs);
      CLUSTERS.getHBaseCluster1().waitUntilAllRegionsAssigned(TableName.valueOf(table));

      // After region reopen, preWALRestore must have re-shipped the edit
      GenericTestUtils.waitFor(() -> rowExistsOnCluster2(table, "1"), 1000, 60_000);
    } finally {
      DataNodeFaultInjector.set(prior);
    }
  }
}
```

Skeleton (F4 — the trickiest one):

```java
@Test public void testRSAbortsAfterSyncTimeout() throws Exception {
  // Use the Phoenix abort hook to observe instead of letting the JVM die.
  AtomicReference<Throwable> abortCause = new AtomicReference<>();
  ReplicationLogGroup.setAbortHookForTesting((reason, t) -> abortCause.set(t));
  conf1.setLong(REPLICATION_LOG_SYNC_TIMEOUT_KEY, 5_000);

  DataNodeFaultInjector prior = DataNodeFaultInjector.get();
  DataNodeFaultInjector.set(new DataNodeFaultInjector() {
    @Override public void delayAckLastPacket() throws IOException {
      try { Thread.sleep(60_000); } catch (InterruptedException e) { /* ignore */ }
    }
  });
  try {
    try (Connection c = CLUSTERS.getCluster1Connection(hag)) {
      c.createStatement().execute("upsert into " + table + " values (1, 'x')");
      c.commit();
      fail("Expected timeout abort");
    } catch (SQLException expected) { /* propagated */ }
    GenericTestUtils.waitFor(() -> abortCause.get() != null, 100, 30_000);
    assertTrue(abortCause.get() instanceof PhoenixWALSyncTimeoutException);
  } finally {
    DataNodeFaultInjector.set(prior);
    ReplicationLogGroup.setAbortHookForTesting(null);
  }
}
```

---

## E. What stays external (chaos)

These genuinely don't make sense as in-process JUnit:

- **C1 mode flap soak** — needs `NetworkChaos` running for 3+ minutes with
  randomized loss/correlation. Already specced as S8 in the failover
  scenarios doc.
- **C2 forced failover with namespace delete** — `kubectl delete ns` is
  the test. Specced as S4. The closest in-process IT is **F9**, which
  validates the writer-side correctness invariant (synced rows preserved,
  OUT rows lost) but cannot validate the operator CLI surface or the
  namespace reprovisioning flow.
- **C3 multi-day write soak with rolling restarts** — weekly nightly, not
  CI.
- **C4 DNS / clock chaos** — depend on chaos-mesh `DNSChaos` / `TimeChaos`.
  Useful but not required to ship.

Everything else from the failover doc (S1, S2, S3, S5, S6, S7, S10, S11,
S12, S17) maps to an automated IT in section D.

---

## F. Phasing recommendation

Build in this order. Each step is shippable.

1. **Add hooks B1+B2** (~200 LOC, all `@VisibleForTesting`). Single PR.
2. **Wire D1 unit tests** (W10–W12, F15). 1 PR. Validates hooks.
3. **F1 (ghost-write guard)** as 1-cluster IT. Highest-correctness, low
   complexity.
4. **F2, F3, F4** — the zero-RPO trio. One PR each. These are the
   design-doc invariants.
5. **F5–F9** — failover state-machine tests. Group as one PR per state
   edge.
6. **F10–F14** — physical-fault tests (split, pipeline, NN failover,
   lease, slow DN). Group by hook.
7. **C1–C4** — external chaos runbooks (operator-facing, run weekly).

After (1)–(6) we have **~16 new automated tests** covering every state
edge from `Phoenix_HA_ReArchitecture_for_Consistent_Failover.md` that
doesn't require a real Kubernetes environment, plus a maintained set of
operator runbooks for the four that do.

---

## G. Cross-references

- Design: `docs/Phoenix_HA_ReArchitecture_for_Consistent_Failover.md`
- External chaos scenarios (S1–S17): `docs/Phoenix_HA_Failover_Test_Scenarios.md`
- Writer subsystem source: `phoenix-core-server/src/main/java/org/apache/phoenix/replication/`
- Existing unit base: `phoenix-core/src/test/java/org/apache/phoenix/replication/ReplicationLogBaseTest.java`
- Existing 2-cluster IT base: `phoenix-core/src/it/java/org/apache/phoenix/jdbc/HABaseIT.java`
