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
 * manually-driven real ReplicationLogDiscoveryReplay (the auto-scheduler is disabled). See the
 * design spec: docs/superpowers/specs/2026-07-15-phoenix-7920-store-and-forward-failover-e2e-design.md
 */
@Category(NeedsOwnMiniClusterTest.class)
public class StoreAndForwardFailoverIT extends HABaseIT {

  private static final Logger LOG = LoggerFactory.getLogger(StoreAndForwardFailoverIT.class);

  private static final int A_START = 0;
  private static final int A_COUNT = 50;
  private static final int B_START = 50;
  private static final int B_COUNT = 50;

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

  @BeforeClass
  public static synchronized void doSetup() throws Exception {
    // Short rounds so replay-round eligibility is a few seconds, not the 60s production default.
    conf1.setInt(PHOENIX_REPLICATION_ROUND_DURATION_SECONDS_KEY, 2);
    conf2.setInt(PHOENIX_REPLICATION_ROUND_DURATION_SECONDS_KEY, 2);
    // Disable the auto replay scheduler so our manually-driven instance is the only replayer.
    // (HABaseIT.doBaseSetup enables it; the writer/forwarder are gated on SYNCHRONOUS_REPLICATION,
    // not on this flag, so they are unaffected.)
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
    logGroup = ReplicationLogGroup.get(conf1, rs.getServerName(), haGroupName);

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
    // Stages 2-3 are added in Tasks 4-5.
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
   * Repeatedly call {@code discovery.replay()} (short real-clock rounds) until {@code done} holds or
   * {@code timeoutMs} elapses, then assert {@code done}. This is how the test advances the manually
   * driven replayer in the absence of the auto-scheduler.
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

  /** Upsert {@code count} rows [startId, startId+count) into the replicated table via the HA URL. */
  private void upsertRows(int startId, int count) throws SQLException {
    try (FailoverPhoenixConnection conn = (FailoverPhoenixConnection) DriverManager
        .getConnection(CLUSTERS.getJdbcHAUrl(), clientProps)) {
      PreparedStatement stmt =
          conn.prepareStatement("UPSERT INTO " + tableName + " VALUES (?, ?)");
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
}
