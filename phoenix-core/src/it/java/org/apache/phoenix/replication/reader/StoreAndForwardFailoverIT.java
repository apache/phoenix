/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.replication.reader;

import static org.apache.phoenix.jdbc.HighAvailabilityGroup.PHOENIX_HA_GROUP_ATTR;
import static org.apache.phoenix.jdbc.HighAvailabilityTestingUtility.getHighAvailibilityGroup;
import static org.apache.phoenix.query.BaseTest.generateUniqueName;
import static org.apache.phoenix.replication.ReplicationShardDirectoryManager.PHOENIX_REPLICATION_ROUND_DURATION_SECONDS_KEY;
import static org.apache.phoenix.replication.reader.ReplicationLogReplayService.PHOENIX_REPLICATION_REPLAY_ENABLED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Optional;
import java.util.Properties;
import java.util.function.BooleanSupplier;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.jdbc.FailoverPhoenixConnection;
import org.apache.phoenix.jdbc.HABaseIT;
import org.apache.phoenix.jdbc.HAGroupStoreClient;
import org.apache.phoenix.jdbc.HAGroupStoreManager;
import org.apache.phoenix.jdbc.HAGroupStoreRecord;
import org.apache.phoenix.jdbc.HighAvailabilityGroup;
import org.apache.phoenix.jdbc.HighAvailabilityPolicy;
import org.apache.phoenix.jdbc.HighAvailabilityTestingUtility;
import org.apache.phoenix.replication.CrossClusterReplicationTestUtil;
import org.apache.phoenix.replication.ReplicationLogGroup;
import org.apache.phoenix.replication.ReplicationLogGroupTestAccess;
import org.apache.phoenix.replication.ReplicationLogTracker;
import org.apache.phoenix.replication.ReplicationShardDirectoryManager;
import org.apache.phoenix.replication.metrics.MetricsReplicationLogTrackerReplayImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * End-to-end zero-RPO test for a direct DEGRADED_STANDBY -&gt; STANDBY_TO_ACTIVE failover in
 * store-and-forward mode (PHOENIX-7920). Cluster1 runs the real writer; Cluster2 runs a
 * manually-driven real ReplicationLogDiscoveryReplay (the auto-scheduler is disabled).
 */
@Category(NeedsOwnMiniClusterTest.class)
public class StoreAndForwardFailoverIT extends HABaseIT {

  private static final Logger LOG = LoggerFactory.getLogger(StoreAndForwardFailoverIT.class);

  private static final int A_START = 0;
  private static final int A_COUNT = 50;
  private static final int B_START = 50;
  private static final int B_COUNT = 50;

  // Cluster2's peer-aware reaction to Cluster1's ACTIVE_NOT_IN_SYNC drives its LOCAL record to
  // DEGRADED_STANDBY asynchronously; bound how long transitionCluster2 waits for that reaction to
  // land (and how often it polls) before attempting its own transition.
  private static final long PEER_REACTION_SETTLE_MILLIS = 10000L;
  private static final long PEER_REACTION_POLL_MILLIS = 500L;

  @Rule
  public TestName name = new TestName();

  private String haGroupName;
  private Properties clientProps;
  private HighAvailabilityGroup haGroup;
  private ReplicationLogGroup logGroup;
  private ReplicationLogDiscoveryReplay discovery;
  private ReplicationLogTracker replayTracker;
  private FileSystem standbyFs;
  private Path standbyInDir;
  private String tableName;
  private long syncPointAfterA;
  private int logFileCountAfterA;

  @BeforeClass
  public static synchronized void doSetup() throws Exception {
    // Short rounds so replay-round eligibility is a few seconds, not the 60s production default.
    conf1.setInt(PHOENIX_REPLICATION_ROUND_DURATION_SECONDS_KEY, 2);
    conf2.setInt(PHOENIX_REPLICATION_ROUND_DURATION_SECONDS_KEY, 2);
    // Disable the auto replay scheduler so our manually-driven instance is the only replayer.
    // (HABaseIT.doBaseSetup enables it; the writer/forwarder are gated on SYNCHRONOUS_REPLICATION,
    // not on this flag, so they are unaffected.)
    // This flag has a second consumer: CompactionScanner also gates the replication
    // consistency-point compaction guard on it, so setting it false disables that guard during
    // major compactions too. Both effects share this one flag and cannot be separated. It is
    // immaterial here: this test triggers no major compaction and asserts cross-cluster
    // cell-equality directly, so the guard's absence does not affect what it verifies.
    conf1.setBoolean(PHOENIX_REPLICATION_REPLAY_ENABLED, false);
    conf2.setBoolean(PHOENIX_REPLICATION_REPLAY_ENABLED, false);
    CLUSTERS.start();
  }

  @Before
  public void beforeTest() throws Exception {
    LOG.info("Starting test {}", name.getMethodName());
    haGroupName = name.getMethodName();
    clientProps = HighAvailabilityTestingUtility.getHATestProperties();
    clientProps.setProperty(PHOENIX_HA_GROUP_ATTR, haGroupName);
    // Cluster1 = ACTIVE_IN_SYNC, Cluster2 = STANDBY, with hdfs urls populated on both records.
    CLUSTERS.initClusterRole(haGroupName, HighAvailabilityPolicy.FAILOVER);
    haGroup = getHighAvailibilityGroup(CLUSTERS.getJdbcHAUrl(), clientProps);

    // The live Cluster1 writer (RS0). Same singleton the RS write path uses, so flipping its mode
    // affects real writes. Cluster1 is ACTIVE_IN_SYNC => this starts in SYNC mode.
    HRegionServer rs = CLUSTERS.getHBaseCluster1().getHBaseCluster().getRegionServer(0);
    logGroup = ReplicationLogGroup.get(conf1, rs.getServerName(), haGroupName).get();

    // Replicated table (REPLICATION_SCOPE=1 + LOCAL INDEX) on BOTH clusters.
    tableName = "T_" + generateUniqueName();
    CLUSTERS.createTableOnClusterPair(haGroup, tableName);

    // Ensure Cluster2 sees its peer, so its effective HA record is STANDBY (not the peer-blind
    // DEGRADED_STANDBY). This guarantees the replay initializes in SYNC (else-branch).
    HAGroupStoreManager c2Manager = HAGroupStoreManager.getInstance(conf2);
    awaitCondition(() -> {
      try {
        Optional<HAGroupStoreRecord> eff = c2Manager.getEffectiveHAGroupStoreRecord(haGroupName);
        return eff.isPresent()
          && eff.get().getHAGroupState() == HAGroupStoreRecord.HAGroupState.STANDBY;
      } catch (IOException e) {
        return false;
      }
    }, 60000L, "cluster2 effective state should settle to STANDBY before constructing replay");

    // Build the REAL replay over the exact standby 'in' directory the writer forwards to, while
    // Cluster2 is still STANDBY, so init() subscribes its LOCAL listeners BEFORE any degrade /
    // failover write and starts in SYNC.
    standbyInDir = ReplicationLogGroupTestAccess.peerStandbyDir(logGroup);
    standbyFs = standbyInDir.getFileSystem(conf2);
    ReplicationShardDirectoryManager shardMgr =
      new ReplicationShardDirectoryManager(conf2, standbyFs, standbyInDir);
    replayTracker = new ReplicationLogTracker(conf2, haGroupName, shardMgr,
      new MetricsReplicationLogTrackerReplayImpl(haGroupName));
    replayTracker.init();
    discovery = new ReplicationLogDiscoveryReplay(replayTracker);
    discovery.init();
  }

  @After
  public void afterTest() throws Exception {
    LOG.info("Cleaning up test {}", name.getMethodName());
    if (replayTracker != null) {
      replayTracker.close();
    }
    if (logGroup != null) {
      logGroup.close();
    }
  }

  @Test
  public void testDirectFailoverInStoreAndForwardModeIsZeroRPO() throws Exception {
    // Stage 0: the replay initialized in SYNC while Cluster2 is STANDBY.
    assertEquals("replay must initialize in SYNC while cluster2 is STANDBY",
      ReplicationLogDiscoveryReplay.ReplicationReplayState.SYNC,
      discovery.getReplicationReplayState());
    // Stage 1: SYNC batch A replicates to cluster2 and advances the sync point.
    stageWriteAndReplayBatchA();
    // Stage 2: enter store-and-forward, write batch B, await it forwarded to cluster2 'in'.
    stageEnterStoreAndForwardAndForwardBatchB();
    // Stage 3: degrade, direct failover, drive replay to promotion, assert zero RPO.
    stageDegradeDirectFailoverAndAssertZeroRPO();
  }

  /** Poll until {@code condition} holds or {@code timeoutMs} elapses, then assert it. */
  private static void awaitCondition(BooleanSupplier condition, long timeoutMs, String message)
    throws InterruptedException {
    long deadline = System.currentTimeMillis() + timeoutMs;
    while (!condition.getAsBoolean() && System.currentTimeMillis() < deadline) {
      Thread.sleep(250L);
    }
    assertTrue(message, condition.getAsBoolean());
  }

  /**
   * Repeatedly call {@code discovery.replay()} (short real-clock rounds) until {@code done} holds
   * or {@code timeoutMs} elapses, then assert {@code done}. This is how the test advances the
   * manually driven replayer in the absence of the auto-scheduler.
   */
  private void driveReplayUntil(BooleanSupplier done, long timeoutMs, String message)
    throws Exception {
    long deadline = System.currentTimeMillis() + timeoutMs;
    while (System.currentTimeMillis() < deadline) {
      discovery.replay();
      if (done.getAsBoolean()) {
        return;
      }
      Thread.sleep(500L);
    }
    discovery.replay();
    assertTrue(message, done.getAsBoolean());
  }

  /**
   * Upsert {@code count} rows [startId, startId+count) into the replicated table via the HA URL.
   */
  private void upsertRows(int startId, int count) throws SQLException {
    try (FailoverPhoenixConnection conn = (FailoverPhoenixConnection) DriverManager
      .getConnection(CLUSTERS.getJdbcHAUrl(), clientProps)) {
      PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + tableName + " VALUES (?, ?)");
      for (int i = 0; i < count; i++) {
        stmt.setInt(1, startId + i);
        stmt.setInt(2, startId + i);
        stmt.executeUpdate();
      }
      conn.commit();
    }
  }

  /**
   * Stage 1: write batch A on Cluster1 in SYNC mode (.plog goes straight to Cluster2's 'in' dir),
   * then drive replay until every A row is present on Cluster2 and lastRoundInSync has advanced to
   * cover A. Records the sync point so Task 5 can assert it later advances to cover B.
   */
  private void stageWriteAndReplayBatchA() throws Exception {
    upsertRows(A_START, A_COUNT);

    // Drive replay until A has been applied to Cluster2 and the sync point advanced past epoch.
    driveReplayUntil(() -> {
      try {
        CrossClusterReplicationTestUtil.assertTablesEqualAcrossClusters(conf1, conf2, tableName);
        return discovery.getLastRoundInSync() != null
          && discovery.getLastRoundInSync().getEndTime() > 0L;
      } catch (AssertionError notYet) {
        return false;
      } catch (Exception e) {
        return false;
      }
    }, 90000L, "batch A must replicate to cluster2 and advance lastRoundInSync");

    syncPointAfterA = discovery.getLastRoundInSync().getEndTime();
    assertTrue("lastRoundInSync must be set after batch A", syncPointAfterA > 0L);
    LOG.info("Stage 1 complete: batch A in sync, syncPointAfterA={}", syncPointAfterA);
  }

  /**
   * Stage 2: flip the live Cluster1 writer SYNC -&gt; STORE_AND_FORWARD (real
   * StoreAndForwardModeImpl onEnter: local 'out' log + forwarder + periodic ACTIVE_NOT_IN_SYNC
   * persistence on Cluster1), write batch B (buffered to Cluster1 'out'), and wait for the real
   * forwarder to copy B's .plog files into Cluster2's 'in' directory. Does NOT drive replay here,
   * so B stays unreplayed.
   */
  private void stageEnterStoreAndForwardAndForwardBatchB() throws Exception {
    logFileCountAfterA =
      CrossClusterReplicationTestUtil.findLogFiles(standbyInDir, standbyFs).size();

    boolean swapped = ReplicationLogGroupTestAccess.forceStoreAndForward(logGroup);
    assertTrue("writer must have been in SYNC and flipped to STORE_AND_FORWARD", swapped);
    assertTrue("writer must now report STORE_AND_FORWARD",
      ReplicationLogGroupTestAccess.isStoreAndForward(logGroup));

    upsertRows(B_START, B_COUNT);

    // The forwarder copies out->in on its own executor; poll (do not race) for B's new files.
    awaitCondition(() -> {
      try {
        return CrossClusterReplicationTestUtil.findLogFiles(standbyInDir, standbyFs).size()
            > logFileCountAfterA;
      } catch (IOException e) {
        return false;
      }
    }, 60000L, "forwarder must copy batch B's .plog files into cluster2 'in'");
    LOG.info("Stage 2 complete: store-and-forward active, batch B forwarded to cluster2 'in'");
  }

  /**
   * Drive Cluster2's LOCAL HA record to {@code target} through the same cached client the real
   * triggerFailover uses. setHAGroupStatusIfNeeded returns a positive dwell-time when the
   * transition is throttled; retry until it applies (returns 0) or the deadline elapses.
   * <p>
   * Cluster2 is peer-aware: when Cluster1 persists {@code ACTIVE_NOT_IN_SYNC} on entering
   * store-and-forward, Cluster2 auto-reacts and drives its own LOCAL record to
   * {@code DEGRADED_STANDBY}. That reaction may still be in flight when this helper runs, so for
   * the {@code DEGRADED_STANDBY} target only we first poll a bounded window for the record to
   * settle and treat "already at target" as a no-op — otherwise a redundant transition (e.g.
   * DEGRADED_STANDBY -&gt; DEGRADED_STANDBY) would throw InvalidClusterRoleTransitionException on a
   * slow box. Other targets (e.g. {@code STANDBY_TO_ACTIVE}) are never peer-driven, so the
   * settle-poll is skipped for them to avoid dead time.
   */
  private void transitionCluster2(HAGroupStoreRecord.HAGroupState target) throws Exception {
    // Only DEGRADED_STANDBY is reached by the peer-aware auto-reaction; let any in-flight reaction
    // settle so the no-op check below fires deterministically. Skipping this for non-peer-driven
    // targets avoids waiting on a condition that can never become true.
    if (target == HAGroupStoreRecord.HAGroupState.DEGRADED_STANDBY) {
      long settleDeadline = System.currentTimeMillis() + PEER_REACTION_SETTLE_MILLIS;
      while (System.currentTimeMillis() < settleDeadline && !cluster2StateIs(target)) {
        Thread.sleep(PEER_REACTION_POLL_MILLIS);
      }
    }
    // Check if already at target - no-op if so (avoid redundant transition exception).
    if (cluster2StateIs(target)) {
      return;
    }
    HAGroupStoreClient client = HAGroupStoreClient.getInstance(conf2, haGroupName);
    long deadline = System.currentTimeMillis() + 60000L;
    long lastWait = -1L;
    while (System.currentTimeMillis() < deadline) {
      lastWait = client.setHAGroupStatusIfNeeded(target);
      if (lastWait == 0L) {
        return;
      }
      Thread.sleep(Math.min(lastWait, 2000L));
    }
    throw new AssertionError("cluster2 transition to " + target
      + " was still throttled at deadline (lastWait=" + lastWait + ")");
  }

  /** True if Cluster2's persisted HA record is currently {@code state}. */
  private boolean cluster2StateIs(HAGroupStoreRecord.HAGroupState state) {
    try {
      Optional<HAGroupStoreRecord> rec =
        HAGroupStoreManager.getInstance(conf2).getHAGroupStoreRecord(haGroupName);
      return rec.isPresent() && rec.get().getHAGroupState() == state;
    } catch (IOException e) {
      return false;
    }
  }

  /**
   * Stage 3: degrade Cluster2 (freezes lastRoundInSync at A), perform the real direct
   * DEGRADED_STANDBY -&gt; STANDBY_TO_ACTIVE, then drive replay until Cluster2 promotes to
   * ACTIVE_IN_SYNC. Assert zero RPO: every A and B row is present cell-for-cell on Cluster2, the
   * replay state is back to SYNC, and lastRoundInSync advanced past its post-A value to cover B.
   * <p>
   * Regression guard: without PHOENIX-7920's triggerFailoverListner CAS(DEGRADED, SYNCED_RECOVERY),
   * the direct transition leaves the replay state at DEGRADED forever, shouldTriggerFailover()
   * never passes, Cluster2 never reaches ACTIVE_IN_SYNC, and the promotion poll below times out
   * (red).
   */
  private void stageDegradeDirectFailoverAndAssertZeroRPO() throws Exception {
    // Degrade: LOCAL -> DEGRADED_STANDBY drives the degradedListener to DEGRADED and freezes
    // lastRoundInSync at the batch-A sync point (B is forwarded but not yet replayed).
    transitionCluster2(HAGroupStoreRecord.HAGroupState.DEGRADED_STANDBY);
    awaitCondition(
      () -> discovery.getReplicationReplayState()
          == ReplicationLogDiscoveryReplay.ReplicationReplayState.DEGRADED,
      30000L, "degradedListener should drive replay state to DEGRADED");
    assertEquals("lastRoundInSync must stay frozen at the batch-A sync point during DEGRADED",
      syncPointAfterA, discovery.getLastRoundInSync().getEndTime());

    // Direct failover: LOCAL -> STANDBY_TO_ACTIVE (no STANDBY hop). triggerFailoverListner must
    // CAS DEGRADED -> SYNCED_RECOVERY and arm failoverPending.
    transitionCluster2(HAGroupStoreRecord.HAGroupState.STANDBY_TO_ACTIVE);
    awaitCondition(
      () -> discovery.getReplicationReplayState()
          == ReplicationLogDiscoveryReplay.ReplicationReplayState.SYNCED_RECOVERY,
      30000L, "direct DEGRADED_STANDBY -> STANDBY_TO_ACTIVE must move replay to SYNCED_RECOVERY");
    assertTrue("failoverPending must be armed after STANDBY_TO_ACTIVE",
      discovery.getFailoverPending());

    // Make sure the client cache reflects STANDBY_TO_ACTIVE before triggerFailover reads it.
    awaitCondition(() -> cluster2StateIs(HAGroupStoreRecord.HAGroupState.STANDBY_TO_ACTIVE), 30000L,
      "cluster2 record should reflect STANDBY_TO_ACTIVE before driving failover");

    logGroup.close();

    // Drive replay: SYNCED_RECOVERY rewinds to lastRoundInSync (A), re-replays B in SYNC. With no
    // new files arriving, the replay drains, shouldTriggerFailover() passes, and triggerFailover()
    // sets ACTIVE_IN_SYNC on Cluster2.
    driveReplayUntil(() -> cluster2StateIs(HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC), 120000L,
      "cluster2 must promote to ACTIVE_IN_SYNC after the direct failover");

    // Zero-RPO assertions.
    CrossClusterReplicationTestUtil.assertTablesEqualAcrossClusters(conf1, conf2, tableName);
    assertEquals("replay state must be SYNC after promotion",
      ReplicationLogDiscoveryReplay.ReplicationReplayState.SYNC,
      discovery.getReplicationReplayState());
    assertTrue("lastRoundInSync must advance past the batch-A sync point to cover batch B",
      discovery.getLastRoundInSync().getEndTime() > syncPointAfterA);
    LOG.info("Stage 3 complete: cluster2 promoted to ACTIVE_IN_SYNC with zero RPO");
  }
}
