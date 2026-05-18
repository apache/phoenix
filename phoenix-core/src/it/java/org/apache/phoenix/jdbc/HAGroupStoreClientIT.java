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
package org.apache.phoenix.jdbc;

import static org.apache.phoenix.jdbc.HAGroupStoreClient.ZK_CONSISTENT_HA_GROUP_RECORD_NAMESPACE;
import static org.apache.phoenix.jdbc.HighAvailabilityGroup.PHOENIX_HA_ZOOKEEPER_ZNODE_NAMESPACE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_HA_GROUP_NAME;
import static org.apache.phoenix.jdbc.PhoenixHAAdmin.getLocalZkUrl;
import static org.apache.phoenix.jdbc.PhoenixHAAdmin.toPath;
import static org.apache.phoenix.query.QueryServices.PHOENIX_HA_LEGACY_CRR_RECONCILIATION_INTERVAL_SECONDS;
import static org.apache.phoenix.query.QueryServices.PHOENIX_HA_LEGACY_CRR_SYNC_ENABLED;
import static org.apache.phoenix.replication.reader.ReplicationLogReplayService.PHOENIX_REPLICATION_REPLAY_ENABLED;
import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR;
import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL_ZK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.lang.reflect.Field;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.hadoop.conf.Configuration;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.exception.InvalidClusterRoleTransitionException;
import org.apache.phoenix.exception.StaleClusterRoleRecordVersionException;
import org.apache.phoenix.util.HAGroupStoreTestUtil;
import org.apache.phoenix.util.JDBCUtil;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

/**
 * Integration tests for {@link HAGroupStoreClient}
 */
@Category(NeedsOwnMiniClusterTest.class)
public class HAGroupStoreClientIT extends HABaseIT {

  private static final Long ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS = 5000L;
  private PhoenixHAAdmin haAdmin;
  private PhoenixHAAdmin peerHaAdmin;
  // Admin on the legacy /phoenix/ha namespace; used to inspect/seed/corrupt the legacy znode.
  private PhoenixHAAdmin legacyHaAdmin;
  private String zkUrl;
  private String peerZKUrl;
  private String masterUrl;
  private String peerMasterUrl;

  @Rule
  public TestName testName = new TestName();

  @BeforeClass
  public static synchronized void doSetup() throws Exception {
    // some of the test scenarios don't behave correctly because of the interactions
    // between ReplayService and HAGroupStore
    conf1.setBoolean(PHOENIX_REPLICATION_REPLAY_ENABLED, false);
    conf2.setBoolean(PHOENIX_REPLICATION_REPLAY_ENABLED, false);
    CLUSTERS.start();
  }

  @Before
  public void before() throws Exception {
    haAdmin = new PhoenixHAAdmin(CLUSTERS.getHBaseCluster1().getConfiguration(),
      ZK_CONSISTENT_HA_GROUP_RECORD_NAMESPACE);
    peerHaAdmin = new PhoenixHAAdmin(CLUSTERS.getHBaseCluster2().getConfiguration(),
      ZK_CONSISTENT_HA_GROUP_RECORD_NAMESPACE);
    legacyHaAdmin = new PhoenixHAAdmin(CLUSTERS.getHBaseCluster1().getConfiguration(),
      PHOENIX_HA_ZOOKEEPER_ZNODE_NAMESPACE);
    haAdmin.getCurator().delete().quietly().forPath(toPath(testName.getMethodName()));
    peerHaAdmin.getCurator().delete().quietly().forPath(toPath(testName.getMethodName()));
    legacyHaAdmin.getCurator().delete().quietly().forPath(toPath(testName.getMethodName()));
    zkUrl = getLocalZkUrl(CLUSTERS.getHBaseCluster1().getConfiguration());
    // Clean existing records in system table
    List<String> haGroupNames = HAGroupStoreClient.getHAGroupNames(zkUrl);
    for (String groupName : haGroupNames) {
      HAGroupStoreTestUtil.deleteHAGroupRecordInSystemTable(groupName, zkUrl);
    }
    // Insert a HAGroupStoreRecord into the system table
    String haGroupName = testName.getMethodName();
    this.peerZKUrl = CLUSTERS.getZkUrl2();
    this.masterUrl = CLUSTERS.getMasterAddress1();
    this.peerMasterUrl = CLUSTERS.getMasterAddress2();
    HAGroupStoreTestUtil.upsertHAGroupRecordInSystemTable(haGroupName, this.zkUrl, this.peerZKUrl,
      this.masterUrl, this.peerMasterUrl, ClusterRoleRecord.ClusterRole.ACTIVE,
      ClusterRoleRecord.ClusterRole.STANDBY, null, CLUSTERS.getHdfsUrl1(), CLUSTERS.getHdfsUrl2());
  }

  @After
  public void after() throws Exception {
    haAdmin.getCurator().delete().quietly().forPath(toPath(testName.getMethodName()));
    peerHaAdmin.getCurator().delete().quietly().forPath(toPath(testName.getMethodName()));
    legacyHaAdmin.getCurator().delete().quietly().forPath(toPath(testName.getMethodName()));
    haAdmin.close();
    peerHaAdmin.close();
    legacyHaAdmin.close();
  }

  @Test
  public void testHAGroupStoreClientWithBothNullZKUrl() throws Exception {
    String haGroupName = testName.getMethodName();
    // Clean existing record
    HAGroupStoreTestUtil.deleteHAGroupRecordInSystemTable(haGroupName, zkUrl);
    HAGroupStoreTestUtil.upsertHAGroupRecordInSystemTable(haGroupName, null, null, this.masterUrl,
      this.peerMasterUrl, ClusterRoleRecord.ClusterRole.ACTIVE,
      ClusterRoleRecord.ClusterRole.STANDBY, this.zkUrl, CLUSTERS.getHdfsUrl1(),
      CLUSTERS.getHdfsUrl2());
    HAGroupStoreClient haGroupStoreClient = HAGroupStoreClient
      .getInstanceForZkUrl(CLUSTERS.getHBaseCluster1().getConfiguration(), haGroupName, zkUrl);
    assertNull(haGroupStoreClient);
  }

  @Test
  public void testHAGroupStoreClientChangingPeerZKUrlToNullUrlToValidUrlToInvalidUrl()
    throws Exception {
    String haGroupName = testName.getMethodName();
    HAGroupStoreRecord record =
      new HAGroupStoreRecord("v1.0", haGroupName, HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC,
        0L, HighAvailabilityPolicy.FAILOVER.toString(), this.peerZKUrl, this.masterUrl,
        this.peerMasterUrl, CLUSTERS.getHdfsUrl1(), CLUSTERS.getHdfsUrl2(), 0L);
    createOrUpdateHAGroupStoreRecordOnZookeeper(haAdmin, haGroupName, record);
    HAGroupStoreClient haGroupStoreClient = HAGroupStoreClient
      .getInstanceForZkUrl(CLUSTERS.getHBaseCluster1().getConfiguration(), haGroupName, zkUrl);
    HAGroupStoreRecord currentRecord = haGroupStoreClient.getHAGroupStoreRecord();
    assert currentRecord != null
      && currentRecord.getHAGroupState() == HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC;

    // Base case: Check that peerPathChildrenCache is not null in HAGroupStoreClient via reflection
    Field peerPathChildrenCache =
      HAGroupStoreClient.class.getDeclaredField("peerPathChildrenCache");
    peerPathChildrenCache.setAccessible(true);
    assertNotNull(peerPathChildrenCache.get(haGroupStoreClient));

    // Now update peerZKUrl to null and rebuild
    record =
      new HAGroupStoreRecord("v1.0", haGroupName, HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC,
        0L, HighAvailabilityPolicy.FAILOVER.toString(), null, this.masterUrl, null,
        CLUSTERS.getHdfsUrl1(), CLUSTERS.getHdfsUrl2(), 0L);
    createOrUpdateHAGroupStoreRecordOnZookeeper(haAdmin, haGroupName, record);
    Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);
    assertNull(peerPathChildrenCache.get(haGroupStoreClient));

    // Now update System table to contain valid peer ZK URL and also change local cluster role to
    // STANDBY
    record = new HAGroupStoreRecord("v1.0", haGroupName, HAGroupStoreRecord.HAGroupState.STANDBY,
      0L, HighAvailabilityPolicy.FAILOVER.toString(), this.peerZKUrl, this.masterUrl,
      this.peerMasterUrl, CLUSTERS.getHdfsUrl1(), CLUSTERS.getHdfsUrl2(), 0L);
    createOrUpdateHAGroupStoreRecordOnZookeeper(haAdmin, haGroupName, record);
    Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);
    currentRecord = haGroupStoreClient.getHAGroupStoreRecord();
    assertNotNull(currentRecord);
    assertEquals(HAGroupStoreRecord.HAGroupState.STANDBY, currentRecord.getHAGroupState());

    // Check that peerPathChildrenCache is not null now in HAGroupStoreClient via reflection
    assertNotNull(peerPathChildrenCache.get(haGroupStoreClient));

    // Now update local HAGroupStoreRecord to STANDBY to verify that HAGroupStoreClient is working
    // as normal
    record =
      new HAGroupStoreRecord("v1.0", haGroupName, HAGroupStoreRecord.HAGroupState.DEGRADED_STANDBY,
        0L, HighAvailabilityPolicy.FAILOVER.toString(), this.peerZKUrl, this.masterUrl,
        this.peerMasterUrl, CLUSTERS.getHdfsUrl1(), CLUSTERS.getHdfsUrl2(), 0L);
    createOrUpdateHAGroupStoreRecordOnZookeeper(haAdmin, haGroupName, record);
    Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

    currentRecord = haGroupStoreClient.getHAGroupStoreRecord();
    assertNotNull(currentRecord);
    assertEquals(HAGroupStoreRecord.HAGroupState.DEGRADED_STANDBY, currentRecord.getHAGroupState());

    // Now update peerZKUrl to invalid but non-null url and rebuild
    // This URL can also be considered unreachable url due to a connectivity issue.
    String invalidUrl = "invalidURL";
    record = new HAGroupStoreRecord("v1.0", haGroupName, HAGroupStoreRecord.HAGroupState.STANDBY,
      0L, HighAvailabilityPolicy.FAILOVER.toString(), invalidUrl, this.masterUrl, invalidUrl,
      CLUSTERS.getHdfsUrl1(), CLUSTERS.getHdfsUrl2(), 0L);
    createOrUpdateHAGroupStoreRecordOnZookeeper(haAdmin, haGroupName, record);
    Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);
    currentRecord = haGroupStoreClient.getHAGroupStoreRecord();
    assertNotNull(currentRecord);
    assertEquals(HAGroupStoreRecord.HAGroupState.STANDBY, currentRecord.getHAGroupState());

    // Check CRR, the role for peer cluster with invalid zk url should be UNKNOWN for it's cluster
    // url.
    ClusterRoleRecord clusterRoleRecord = haGroupStoreClient.getClusterRoleRecord();
    assertNotNull(clusterRoleRecord);
    ClusterRoleRecord expected = new ClusterRoleRecord(haGroupName, HighAvailabilityPolicy.FAILOVER,
      this.masterUrl, ClusterRoleRecord.ClusterRole.STANDBY, invalidUrl,
      ClusterRoleRecord.ClusterRole.UNKNOWN, 0);
    assertEquals(expected, clusterRoleRecord);

    // Check that peerPathChildrenCache is null now in HAGroupStoreClient via reflection
    assertNull(peerPathChildrenCache.get(haGroupStoreClient));
  }

  @Test
  public void testHAGroupStoreClientWithoutSystemTableRecord() throws Exception {
    String haGroupName = testName.getMethodName();
    // Clean existing record
    HAGroupStoreTestUtil.deleteHAGroupRecordInSystemTable(haGroupName, this.zkUrl);
    assertNull(HAGroupStoreClient
      .getInstanceForZkUrl(CLUSTERS.getHBaseCluster1().getConfiguration(), haGroupName, zkUrl));
  }

  @Test
  public void testHAGroupStoreClientWithSingleHAGroupStoreRecord() throws Exception {
    String haGroupName = testName.getMethodName();
    HAGroupStoreClient haGroupStoreClient = HAGroupStoreClient
      .getInstanceForZkUrl(CLUSTERS.getHBaseCluster1().getConfiguration(), haGroupName, zkUrl);

    // Create and store HAGroupStoreRecord with ACTIVE state
    HAGroupStoreRecord record =
      new HAGroupStoreRecord("v1.0", haGroupName, HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC,
        0L, HighAvailabilityPolicy.FAILOVER.toString(), this.peerZKUrl, this.masterUrl,
        this.peerMasterUrl, CLUSTERS.getHdfsUrl1(), CLUSTERS.getHdfsUrl2(), 0L);
    createOrUpdateHAGroupStoreRecordOnZookeeper(haAdmin, haGroupName, record);
    Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);
    HAGroupStoreRecord currentRecord = haGroupStoreClient.getHAGroupStoreRecord();
    assert currentRecord != null
      && currentRecord.getHAGroupState() == HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC;

    // Now Update HAGroupStoreRecord so that current cluster has state ACTIVE_TO_STANDBY
    record = new HAGroupStoreRecord("v1.0", haGroupName,
      HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC_TO_STANDBY, 0L,
      HighAvailabilityPolicy.FAILOVER.toString(), this.peerZKUrl, this.masterUrl,
      this.peerMasterUrl, CLUSTERS.getHdfsUrl1(), CLUSTERS.getHdfsUrl2(), 0L);
    createOrUpdateHAGroupStoreRecordOnZookeeper(haAdmin, haGroupName, record);

    Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);
    // Check that now the cluster should be in ActiveToStandby
    currentRecord = haGroupStoreClient.getHAGroupStoreRecord();
    assert currentRecord != null && currentRecord.getHAGroupState()
        == HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC_TO_STANDBY;

    // Change it back to ACTIVE so that cluster is not in ACTIVE_TO_STANDBY state
    record =
      new HAGroupStoreRecord("v1.0", haGroupName, HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC,
        0L, HighAvailabilityPolicy.FAILOVER.toString(), this.peerZKUrl, this.masterUrl,
        this.peerMasterUrl, CLUSTERS.getHdfsUrl1(), CLUSTERS.getHdfsUrl2(), 0L);
    createOrUpdateHAGroupStoreRecordOnZookeeper(haAdmin, haGroupName, record);

    Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);
    currentRecord = haGroupStoreClient.getHAGroupStoreRecord();
    assert currentRecord != null
      && currentRecord.getHAGroupState() == HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC;

    // Change it again to ACTIVE_TO_STANDBY so that we can validate watcher works repeatedly
    record = new HAGroupStoreRecord("v1.0", haGroupName,
      HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC_TO_STANDBY, 0L,
      HighAvailabilityPolicy.FAILOVER.toString(), this.peerZKUrl, this.masterUrl,
      this.peerMasterUrl, CLUSTERS.getHdfsUrl1(), CLUSTERS.getHdfsUrl2(), 0L);
    createOrUpdateHAGroupStoreRecordOnZookeeper(haAdmin, haGroupName, record);

    Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);
    currentRecord = haGroupStoreClient.getHAGroupStoreRecord();
    assert currentRecord != null && currentRecord.getHAGroupState()
        == HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC_TO_STANDBY;

    // Change it back to ACTIVE to verify transition works
    record =
      new HAGroupStoreRecord("v1.0", haGroupName, HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC,
        0L, HighAvailabilityPolicy.FAILOVER.toString(), this.peerZKUrl, this.masterUrl,
        this.peerMasterUrl, CLUSTERS.getHdfsUrl1(), CLUSTERS.getHdfsUrl2(), 0L);
    createOrUpdateHAGroupStoreRecordOnZookeeper(haAdmin, haGroupName, record);

    Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);
    currentRecord = haGroupStoreClient.getHAGroupStoreRecord();
    assert currentRecord != null
      && currentRecord.getHAGroupState() == HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC;
  }

  /**
   * Helper method to create or update HAGroupStoreRecord on ZooKeeper
   */
  private void createOrUpdateHAGroupStoreRecordOnZookeeper(PhoenixHAAdmin haAdmin,
    String haGroupName, HAGroupStoreRecord record) throws Exception {
    String path = toPath(haGroupName);
    if (haAdmin.getCurator().checkExists().forPath(path) == null) {
      haAdmin.createHAGroupStoreRecordInZooKeeper(record);
    } else {
      final Pair<HAGroupStoreRecord, Stat> currentRecord =
        haAdmin.getHAGroupStoreRecordInZooKeeper(haGroupName);
      if (currentRecord.getRight() != null && currentRecord.getLeft() != null) {
        haAdmin.updateHAGroupStoreRecordInZooKeeper(haGroupName, record,
          currentRecord.getRight().getVersion());
      } else {
        throw new IOException(
          "Current HAGroupStoreRecord in ZK is null, cannot update HAGroupStoreRecord "
            + haGroupName);
      }
    }
  }

  @Test
  public void testHAGroupStoreClientWithMultipleHAGroupStoreRecords() throws Exception {
    String haGroupName1 = testName.getMethodName() + "1";
    String haGroupName2 = testName.getMethodName() + "2";
    HAGroupStoreTestUtil.upsertHAGroupRecordInSystemTable(haGroupName1, this.zkUrl, this.peerZKUrl,
      this.masterUrl, this.peerMasterUrl, ClusterRoleRecord.ClusterRole.ACTIVE,
      ClusterRoleRecord.ClusterRole.STANDBY, null, CLUSTERS.getHdfsUrl1(), CLUSTERS.getHdfsUrl2());
    HAGroupStoreTestUtil.upsertHAGroupRecordInSystemTable(haGroupName2, this.zkUrl, this.peerZKUrl,
      this.masterUrl, this.peerMasterUrl, ClusterRoleRecord.ClusterRole.ACTIVE,
      ClusterRoleRecord.ClusterRole.STANDBY, null, CLUSTERS.getHdfsUrl1(), CLUSTERS.getHdfsUrl2());

    // Setup initial HAGroupStoreRecords
    HAGroupStoreRecord record1 =
      new HAGroupStoreRecord("v1.0", haGroupName1, HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC,
        0L, HighAvailabilityPolicy.FAILOVER.toString(), this.peerZKUrl, this.masterUrl,
        this.peerMasterUrl, CLUSTERS.getHdfsUrl1(), CLUSTERS.getHdfsUrl2(), 0L);
    HAGroupStoreRecord record2 =
      new HAGroupStoreRecord("v1.0", haGroupName2, HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC,
        0L, HighAvailabilityPolicy.FAILOVER.toString(), this.peerZKUrl, this.masterUrl,
        this.peerMasterUrl, CLUSTERS.getHdfsUrl1(), CLUSTERS.getHdfsUrl2(), 0L);

    createOrUpdateHAGroupStoreRecordOnZookeeper(haAdmin, haGroupName1, record1);
    createOrUpdateHAGroupStoreRecordOnZookeeper(haAdmin, haGroupName2, record2);

    HAGroupStoreClient haGroupStoreClient1 = HAGroupStoreClient
      .getInstanceForZkUrl(CLUSTERS.getHBaseCluster1().getConfiguration(), haGroupName1, zkUrl);
    HAGroupStoreClient haGroupStoreClient2 = HAGroupStoreClient
      .getInstanceForZkUrl(CLUSTERS.getHBaseCluster1().getConfiguration(), haGroupName2, zkUrl);

    Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);
    HAGroupStoreRecord currentRecord1 = haGroupStoreClient1.getHAGroupStoreRecord();
    HAGroupStoreRecord currentRecord2 = haGroupStoreClient2.getHAGroupStoreRecord();
    assert currentRecord1 != null
      && currentRecord1.getHAGroupState() == HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC;
    assert currentRecord2 != null
      && currentRecord2.getHAGroupState() == HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC;

    // Now Update HAGroupStoreRecord so that current cluster has state ACTIVE_TO_STANDBY for only 1
    // record
    record1 = new HAGroupStoreRecord("v1.0", haGroupName1,
      HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC_TO_STANDBY, 0L,
      HighAvailabilityPolicy.FAILOVER.toString(), this.peerZKUrl, this.masterUrl,
      this.peerMasterUrl, CLUSTERS.getHdfsUrl1(), CLUSTERS.getHdfsUrl2(), 0L);
    record2 =
      new HAGroupStoreRecord("v1.0", haGroupName2, HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC,
        0L, HighAvailabilityPolicy.FAILOVER.toString(), this.peerZKUrl, this.masterUrl,
        this.peerMasterUrl, CLUSTERS.getHdfsUrl1(), CLUSTERS.getHdfsUrl2(), 0L);

    createOrUpdateHAGroupStoreRecordOnZookeeper(haAdmin, haGroupName1, record1);
    createOrUpdateHAGroupStoreRecordOnZookeeper(haAdmin, haGroupName2, record2);

    Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);
    // Check that now the cluster should be in ActiveToStandby
    currentRecord1 = haGroupStoreClient1.getHAGroupStoreRecord();
    currentRecord2 = haGroupStoreClient2.getHAGroupStoreRecord();
    assert currentRecord1 != null && currentRecord1.getHAGroupState()
        == HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC_TO_STANDBY;
    assert currentRecord2 != null
      && currentRecord2.getHAGroupState() == HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC;

    // Change it back to ACTIVE so that cluster is not in ACTIVE_TO_STANDBY state
    record1 =
      new HAGroupStoreRecord("v1.0", haGroupName1, HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC,
        0L, HighAvailabilityPolicy.FAILOVER.toString(), this.peerZKUrl, this.masterUrl,
        this.peerMasterUrl, CLUSTERS.getHdfsUrl1(), CLUSTERS.getHdfsUrl2(), 0L);
    record2 =
      new HAGroupStoreRecord("v1.0", haGroupName2, HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC,
        0L, HighAvailabilityPolicy.FAILOVER.toString(), this.peerZKUrl, this.masterUrl,
        this.peerMasterUrl, CLUSTERS.getHdfsUrl1(), CLUSTERS.getHdfsUrl2(), 0L);

    createOrUpdateHAGroupStoreRecordOnZookeeper(haAdmin, haGroupName1, record1);
    createOrUpdateHAGroupStoreRecordOnZookeeper(haAdmin, haGroupName2, record2);

    Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);
    currentRecord1 = haGroupStoreClient1.getHAGroupStoreRecord();
    currentRecord2 = haGroupStoreClient2.getHAGroupStoreRecord();
    assert currentRecord1 != null
      && currentRecord1.getHAGroupState() == HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC;
    assert currentRecord2 != null
      && currentRecord2.getHAGroupState() == HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC;

    // Change other record to ACTIVE_TO_STANDBY and one in ACTIVE state so that we can validate
    // watcher works repeatedly
    record1 =
      new HAGroupStoreRecord("v1.0", haGroupName1, HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC,
        0L, HighAvailabilityPolicy.FAILOVER.toString(), this.peerZKUrl, this.masterUrl,
        this.peerMasterUrl, CLUSTERS.getHdfsUrl1(), CLUSTERS.getHdfsUrl2(), 0L);
    record2 = new HAGroupStoreRecord("v1.0", haGroupName2,
      HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC_TO_STANDBY, 0L,
      HighAvailabilityPolicy.FAILOVER.toString(), this.peerZKUrl, this.masterUrl,
      this.peerMasterUrl, CLUSTERS.getHdfsUrl1(), CLUSTERS.getHdfsUrl2(), 0L);

    createOrUpdateHAGroupStoreRecordOnZookeeper(haAdmin, haGroupName1, record1);
    createOrUpdateHAGroupStoreRecordOnZookeeper(haAdmin, haGroupName2, record2);

    Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);
    currentRecord1 = haGroupStoreClient1.getHAGroupStoreRecord();
    currentRecord2 = haGroupStoreClient2.getHAGroupStoreRecord();
    assert currentRecord1 != null
      && currentRecord1.getHAGroupState() == HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC;
    assert currentRecord2 != null && currentRecord2.getHAGroupState()
        == HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC_TO_STANDBY;
  }

  @Test
  public void testMultiThreadedAccessToHACache() throws Exception {
    String haGroupName = testName.getMethodName();

    // Setup initial HAGroupStoreRecord
    HAGroupStoreRecord record =
      new HAGroupStoreRecord("v1.0", haGroupName, HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC,
        0L, HighAvailabilityPolicy.FAILOVER.toString(), this.peerZKUrl, this.masterUrl,
        this.peerMasterUrl, CLUSTERS.getHdfsUrl1(), CLUSTERS.getHdfsUrl2(), 0L);

    createOrUpdateHAGroupStoreRecordOnZookeeper(haAdmin, haGroupName, record);
    HAGroupStoreClient haGroupStoreClient = HAGroupStoreClient
      .getInstanceForZkUrl(CLUSTERS.getHBaseCluster1().getConfiguration(), haGroupName, zkUrl);

    Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

    int threadCount = 10;
    final CountDownLatch latch = new CountDownLatch(threadCount);
    ExecutorService executor = Executors.newFixedThreadPool(threadCount);
    for (int i = 0; i < threadCount; i++) {
      executor.submit(() -> {
        try {
          HAGroupStoreRecord currentRecord = haGroupStoreClient.getHAGroupStoreRecord();
          assert currentRecord != null
            && currentRecord.getHAGroupState() == HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC;
          latch.countDown();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      });
    }
    assert latch.await(10, TimeUnit.SECONDS);

    // Update HAGroupStoreRecord
    record = new HAGroupStoreRecord("v1.0", haGroupName,
      HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC_TO_STANDBY, 0L,
      HighAvailabilityPolicy.FAILOVER.toString(), this.peerZKUrl, this.masterUrl,
      this.peerMasterUrl, CLUSTERS.getHdfsUrl1(), CLUSTERS.getHdfsUrl2(), 0L);

    createOrUpdateHAGroupStoreRecordOnZookeeper(haAdmin, haGroupName, record);

    Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

    final CountDownLatch latch2 = new CountDownLatch(threadCount);
    executor = Executors.newFixedThreadPool(threadCount);
    for (int i = 0; i < threadCount; i++) {
      executor.submit(() -> {
        try {
          HAGroupStoreRecord currentRecord = haGroupStoreClient.getHAGroupStoreRecord();
          assert currentRecord != null && currentRecord.getHAGroupState()
              == HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC_TO_STANDBY;
          latch2.countDown();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      });
    }
    assert latch2.await(10, TimeUnit.SECONDS);
  }

  @Test
  public void testHAGroupStoreClientWithRootPathDeletion() throws Exception {
    String haGroupName = testName.getMethodName();

    HAGroupStoreRecord record1 =
      new HAGroupStoreRecord("v1.0", haGroupName, HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC,
        0L, HighAvailabilityPolicy.FAILOVER.toString(), this.peerZKUrl, this.masterUrl,
        this.peerMasterUrl, CLUSTERS.getHdfsUrl1(), CLUSTERS.getHdfsUrl2(), 0L);

    createOrUpdateHAGroupStoreRecordOnZookeeper(haAdmin, haGroupName, record1);
    HAGroupStoreClient haGroupStoreClient = HAGroupStoreClient
      .getInstanceForZkUrl(CLUSTERS.getHBaseCluster1().getConfiguration(), haGroupName, zkUrl);
    Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);
    HAGroupStoreRecord currentRecord = haGroupStoreClient.getHAGroupStoreRecord();
    assertNotNull(currentRecord);
    assertEquals(HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC, currentRecord.getHAGroupState());

    haAdmin.getCurator().delete().deletingChildrenIfNeeded().forPath(toPath(StringUtils.EMPTY));
    Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);
    currentRecord = haGroupStoreClient.getHAGroupStoreRecord();

    // The record should be automatically rebuilt from System Table as it is not in ZK
    assertNotNull(currentRecord);
    assertEquals(HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC, currentRecord.getHAGroupState());
    // The record should have a timestamp
    assertNotNull(currentRecord.getLastSyncStateTimeInMs());

    record1 = new HAGroupStoreRecord("v1.0", haGroupName,
      HAGroupStoreRecord.HAGroupState.ACTIVE_NOT_IN_SYNC_TO_STANDBY, 0L,
      HighAvailabilityPolicy.FAILOVER.toString(), this.peerZKUrl, this.masterUrl,
      this.peerMasterUrl, CLUSTERS.getHdfsUrl1(), CLUSTERS.getHdfsUrl2(), 0L);

    createOrUpdateHAGroupStoreRecordOnZookeeper(haAdmin, haGroupName, record1);
    Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);
    currentRecord = haGroupStoreClient.getHAGroupStoreRecord();
    assertNotNull(currentRecord);
    assertEquals(HAGroupStoreRecord.HAGroupState.ACTIVE_NOT_IN_SYNC_TO_STANDBY,
      currentRecord.getHAGroupState());
  }

  @Test
  public void testThrowsExceptionWithZKDisconnectionAndThenConnection() throws Exception {
    String haGroupName = testName.getMethodName();

    // Setup initial HAGroupStoreRecord
    HAGroupStoreRecord record =
      new HAGroupStoreRecord("v1.0", haGroupName, HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC,
        0L, HighAvailabilityPolicy.FAILOVER.toString(), this.peerZKUrl, this.masterUrl,
        this.peerMasterUrl, CLUSTERS.getHdfsUrl1(), CLUSTERS.getHdfsUrl2(), 0L);

    createOrUpdateHAGroupStoreRecordOnZookeeper(haAdmin, haGroupName, record);
    HAGroupStoreClient haGroupStoreClient = HAGroupStoreClient
      .getInstanceForZkUrl(CLUSTERS.getHBaseCluster1().getConfiguration(), haGroupName, zkUrl);

    Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);
    HAGroupStoreRecord currentRecord = haGroupStoreClient.getHAGroupStoreRecord();
    assert currentRecord != null
      && currentRecord.getHAGroupState() == HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC;

    // Shutdown the ZK Cluster to simulate CONNECTION_SUSPENDED event
    CLUSTERS.getHBaseCluster1().shutdownMiniZKCluster();

    Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);
    // Check that HAGroupStoreClient instance is not healthy and throws IOException
    assertThrows(IOException.class, () -> haGroupStoreClient.getHAGroupStoreRecord());
    // Check that the HAGroupStoreClient instance is not healthy via reflection
    Field isHealthyField = HAGroupStoreClient.class.getDeclaredField("isHealthy");
    isHealthyField.setAccessible(true);
    assertFalse((boolean) isHealthyField.get(haGroupStoreClient));

    // Start ZK on the same port to simulate CONNECTION_RECONNECTED event
    CLUSTERS.getHBaseCluster1().startMiniZKCluster(1, Integer.parseInt(
      CLUSTERS.getHBaseCluster1().getConfiguration().get("hbase.zookeeper.property.clientPort")));

    Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);
    // Check that HAGroupStoreClient instance is back to healthy and provides correct response
    currentRecord = haGroupStoreClient.getHAGroupStoreRecord();
    assert currentRecord != null
      && currentRecord.getHAGroupState() == HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC;
    // Check that the HAGroupStoreClient instance is healthy via reflection
    assertTrue((boolean) isHealthyField.get(haGroupStoreClient));
  }

  // Tests for setHAGroupStatusIfNeeded method
  @Test
  public void testSetHAGroupStatusIfNeededDeleteZKAndSystemTableRecord() throws Exception {
    String haGroupName = testName.getMethodName();

    // Create HAGroupStoreClient without any existing record
    HAGroupStoreClient haGroupStoreClient = HAGroupStoreClient
      .getInstanceForZkUrl(CLUSTERS.getHBaseCluster1().getConfiguration(), haGroupName, zkUrl);

    // Delete the record from ZK
    haAdmin.getCurator().delete().deletingChildrenIfNeeded().forPath(toPath(haGroupName));
    System.out.println("Going to sleep after deleting record from zk");
    Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);
    System.out.println("Waking up after deleting record from zk");

    // Delete the record from System Table
    try (
      PhoenixConnection conn = (PhoenixConnection) DriverManager
        .getConnection(JDBC_PROTOCOL_ZK + JDBC_PROTOCOL_SEPARATOR + zkUrl);
      Statement stmt = conn.createStatement()) {
      stmt.execute(
        "DELETE FROM " + SYSTEM_HA_GROUP_NAME + " WHERE HA_GROUP_NAME = '" + haGroupName + "'");
      conn.commit();
    }
    System.out.println("Deleted record from system table");

    // This should fail because no record exists in either ZK or System Table
    try {
      haGroupStoreClient.setHAGroupStatusIfNeeded(HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC);
      fail("Expected IOException for missing system table record");
    } catch (IOException e) {
      assertTrue("Exception should mention system table",
        e.getMessage().contains("Current HAGroupStoreRecordStat in cache is null, "
          + "cannot update HAGroupStoreRecord, the record should be initialized in System Table first"));
    }
  }

  @Test
  public void testSetHAGroupStatusIfNeededUpdateExistingRecord() throws Exception {
    String haGroupName = testName.getMethodName();

    // Create initial record
    HAGroupStoreRecord initialRecord =
      new HAGroupStoreRecord("v1.0", haGroupName, HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC,
        0L, HighAvailabilityPolicy.FAILOVER.toString(), this.peerZKUrl, this.masterUrl,
        this.peerMasterUrl, CLUSTERS.getHdfsUrl1(), CLUSTERS.getHdfsUrl2(), 0L);
    createOrUpdateHAGroupStoreRecordOnZookeeper(haAdmin, haGroupName, initialRecord);

    HAGroupStoreClient haGroupStoreClient = HAGroupStoreClient
      .getInstanceForZkUrl(CLUSTERS.getHBaseCluster1().getConfiguration(), haGroupName, zkUrl);
    Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

    // Verify initial state
    HAGroupStoreRecord currentRecord = haGroupStoreClient.getHAGroupStoreRecord();
    assertEquals(HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC, currentRecord.getHAGroupState());

    // Update to STANDBY (this should succeed as it's a valid transition)
    assertEquals(0L, haGroupStoreClient
      .setHAGroupStatusIfNeeded(HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC_TO_STANDBY));
    Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

    // Verify the record was updated
    currentRecord = haGroupStoreClient.getHAGroupStoreRecord();
    assertEquals(HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC_TO_STANDBY,
      currentRecord.getHAGroupState());
  }

  @Test
  public void testSetHAGroupStatusIfNeededWithExplicitLastSyncTimeUpdateExistingRecord()
    throws Exception {
    String haGroupName = testName.getMethodName();

    // Create initial record
    HAGroupStoreRecord initialRecord =
      new HAGroupStoreRecord("v1.0", haGroupName, HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC,
        0L, HighAvailabilityPolicy.FAILOVER.toString(), this.peerZKUrl, this.masterUrl,
        this.peerMasterUrl, CLUSTERS.getHdfsUrl1(), CLUSTERS.getHdfsUrl2(), 0L);
    createOrUpdateHAGroupStoreRecordOnZookeeper(haAdmin, haGroupName, initialRecord);

    HAGroupStoreClient haGroupStoreClient = HAGroupStoreClient
      .getInstanceForZkUrl(CLUSTERS.getHBaseCluster1().getConfiguration(), haGroupName, zkUrl);
    Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

    // Verify initial state
    HAGroupStoreRecord currentRecord = haGroupStoreClient.getHAGroupStoreRecord();
    assertEquals(HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC, currentRecord.getHAGroupState());

    // Update to STANDBY (this should succeed as it's a valid transition)
    long timestamp = System.currentTimeMillis();
    assertEquals(0L, haGroupStoreClient.setHAGroupStatusIfNeeded(
      HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC_TO_STANDBY, timestamp));
    Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

    // Verify the record was updated
    currentRecord = haGroupStoreClient.getHAGroupStoreRecord();
    assertEquals(HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC_TO_STANDBY,
      currentRecord.getHAGroupState());
    assertEquals(timestamp, (long) currentRecord.getLastSyncStateTimeInMs());
  }

  @Test
  public void testSetHAGroupStatusIfNeededNoUpdateWhenNotNeeded() throws Exception {
    String haGroupName = testName.getMethodName();

    // Create initial record with current timestamp
    HAGroupStoreRecord initialRecord = new HAGroupStoreRecord("v1.0", haGroupName,
      HAGroupStoreRecord.HAGroupState.ACTIVE_NOT_IN_SYNC, 0L,
      HighAvailabilityPolicy.FAILOVER.toString(), this.peerZKUrl, this.masterUrl,
      this.peerMasterUrl, CLUSTERS.getHdfsUrl1(), CLUSTERS.getHdfsUrl2(), 0L);
    createOrUpdateHAGroupStoreRecordOnZookeeper(haAdmin, haGroupName, initialRecord);
    Stat initialRecordInZKStat = haAdmin.getHAGroupStoreRecordInZooKeeper(haGroupName).getRight();
    int initialRecordVersion = initialRecordInZKStat.getVersion();

    HAGroupStoreClient haGroupStoreClient = HAGroupStoreClient
      .getInstanceForZkUrl(CLUSTERS.getHBaseCluster1().getConfiguration(), haGroupName, zkUrl);

    // Try to set to ACTIVE_IN_SYNC immediately (should not update due to timing)
    assert haGroupStoreClient
      .setHAGroupStatusIfNeeded(HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC) > 0;

    // Add sleep if due to any bug the update might have gone through and we can assert below this.
    Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);
    // Verify no update occurred
    HAGroupStoreRecord afterRecord = haGroupStoreClient.getHAGroupStoreRecord();
    Stat afterRecordInZKStat = haAdmin.getHAGroupStoreRecordInZooKeeper(haGroupName).getRight();
    int afterRecordVersion = afterRecordInZKStat.getVersion();

    assertEquals(initialRecordVersion, afterRecordVersion);
    assertEquals("State should not change", HAGroupStoreRecord.HAGroupState.ACTIVE_NOT_IN_SYNC,
      afterRecord.getHAGroupState());
  }

  @Test
  public void testSetHAGroupStatusIfNeededWithTimingLogic() throws Exception {
    String haGroupName = testName.getMethodName();
    // Create initial record
    HAGroupStoreRecord initialRecord = new HAGroupStoreRecord("v1.0", haGroupName,
      HAGroupStoreRecord.HAGroupState.ACTIVE_NOT_IN_SYNC, 0L,
      HighAvailabilityPolicy.FAILOVER.toString(), this.peerZKUrl, this.masterUrl,
      this.peerMasterUrl, CLUSTERS.getHdfsUrl1(), CLUSTERS.getHdfsUrl2(), 0L);
    createOrUpdateHAGroupStoreRecordOnZookeeper(haAdmin, haGroupName, initialRecord);

    HAGroupStoreClient haGroupStoreClient = HAGroupStoreClient
      .getInstanceForZkUrl(CLUSTERS.getHBaseCluster1().getConfiguration(), haGroupName, zkUrl);

    // Find wait time for ACTIVE_NOT_IN_SYNC to ACTIVE_IN_SYNC
    long waitTime =
      haGroupStoreClient.setHAGroupStatusIfNeeded(HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC);
    assert waitTime > 0;
    Thread.sleep(waitTime);
    assertEquals(0L,
      haGroupStoreClient.setHAGroupStatusIfNeeded(HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC));
    Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

    // Verify the record was updated
    HAGroupStoreRecord updatedRecord = haGroupStoreClient.getHAGroupStoreRecord();
    assertEquals(HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC, updatedRecord.getHAGroupState());
  }

  @Test
  public void testSetHAGroupStatusIfNeededWithInvalidTransition() throws Exception {
    String haGroupName = testName.getMethodName();

    // Create initial record with ACTIVE state
    HAGroupStoreRecord initialRecord =
      new HAGroupStoreRecord("v1.0", haGroupName, HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC,
        0L, HighAvailabilityPolicy.FAILOVER.toString(), this.peerZKUrl, this.masterUrl,
        this.peerMasterUrl, CLUSTERS.getHdfsUrl1(), CLUSTERS.getHdfsUrl2(), 0L);
    createOrUpdateHAGroupStoreRecordOnZookeeper(haAdmin, haGroupName, initialRecord);

    HAGroupStoreClient haGroupStoreClient = HAGroupStoreClient
      .getInstanceForZkUrl(CLUSTERS.getHBaseCluster1().getConfiguration(), haGroupName, zkUrl);
    Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

    // Try to transition to STANDBY (invalid transition from ACTIVE)
    try {
      haGroupStoreClient.setHAGroupStatusIfNeeded(HAGroupStoreRecord.HAGroupState.STANDBY);
      fail("Expected InvalidClusterRoleTransitionException");
    } catch (InvalidClusterRoleTransitionException e) {
      // Should get InvalidClusterRoleTransitionException (might be wrapped)
      assertTrue("Exception should be about invalid transition",
        e.getMessage().contains("Cannot transition from ACTIVE_IN_SYNC to STANDBY")
          || e.getCause() != null && e.getCause().getMessage()
            .contains("Cannot transition from ACTIVE_IN_SYNC to STANDBY"));
    }
  }

  @Test
  public void testSetHAGroupStatusIfNeededWithUnhealthyClient() throws Exception {
    String haGroupName = testName.getMethodName();

    HAGroupStoreClient haGroupStoreClient = HAGroupStoreClient
      .getInstanceForZkUrl(CLUSTERS.getHBaseCluster1().getConfiguration(), haGroupName, zkUrl);

    // Make client unhealthy by accessing private field
    Field isHealthyField = HAGroupStoreClient.class.getDeclaredField("isHealthy");
    isHealthyField.setAccessible(true);
    isHealthyField.set(haGroupStoreClient, false);

    // Try to set status on unhealthy client
    try {
      haGroupStoreClient.setHAGroupStatusIfNeeded(HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC);
      fail("Expected IOException for unhealthy client");
    } catch (IOException e) {
      assertTrue("Exception should mention unhealthy client",
        e.getMessage().contains("not healthy"));
    }
  }

  @Test
  public void testSetHAGroupStatusIfNeededMultipleTransitions() throws Exception {
    String haGroupName = testName.getMethodName();

    // Create initial record with old timestamp
    HAGroupStoreRecord initialRecord =
      new HAGroupStoreRecord("v1.0", haGroupName, HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC,
        0L, HighAvailabilityPolicy.FAILOVER.toString(), this.peerZKUrl, this.masterUrl,
        this.peerMasterUrl, CLUSTERS.getHdfsUrl1(), CLUSTERS.getHdfsUrl2(), 0L);
    createOrUpdateHAGroupStoreRecordOnZookeeper(haAdmin, haGroupName, initialRecord);

    HAGroupStoreClient haGroupStoreClient = HAGroupStoreClient
      .getInstanceForZkUrl(CLUSTERS.getHBaseCluster1().getConfiguration(), haGroupName, zkUrl);

    // First transition: ACTIVE -> ACTIVE_TO_STANDBY
    assertEquals(0L, haGroupStoreClient
      .setHAGroupStatusIfNeeded(HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC_TO_STANDBY));
    Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

    HAGroupStoreRecord afterFirst = haGroupStoreClient.getHAGroupStoreRecord();
    assertEquals(HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC_TO_STANDBY,
      afterFirst.getHAGroupState());

    // Wait and make another transition: ACTIVE_TO_STANDBY -> STANDBY
    Thread.sleep(100); // Small delay to ensure timestamp difference
    assertEquals(0L,
      haGroupStoreClient.setHAGroupStatusIfNeeded(HAGroupStoreRecord.HAGroupState.STANDBY));
    Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

    HAGroupStoreRecord afterSecond = haGroupStoreClient.getHAGroupStoreRecord();
    assertEquals(HAGroupStoreRecord.HAGroupState.STANDBY, afterSecond.getHAGroupState());
  }

  /**
   * This test verifies that the updates coming via PathChildrenCacheListener are in order in which
   * updates are sent to ZK
   */
  @Test
  public void testHAGroupStoreClientWithMultiThreadedUpdates() throws Exception {
    String haGroupName = testName.getMethodName();

    // Number of threads to execute
    int threadCount = 5;

    // Capture versions of records in a list(recordEventVersions) in order they are received.
    List<Integer> recordEventVersions = new ArrayList<>();
    CountDownLatch eventsLatch = new CountDownLatch(threadCount);
    PathChildrenCacheListener pathChildrenCacheListener = (client, event) -> {
      if (
        event.getData() != null && event.getData().getData() != null
          && HAGroupStoreRecord.fromJson(event.getData().getData()).isPresent()
      ) {
        HAGroupStoreRecord record = HAGroupStoreRecord.fromJson(event.getData().getData()).get();
        if (record.getHaGroupName().equals(haGroupName)) {
          recordEventVersions.add(event.getData().getStat().getVersion());
          eventsLatch.countDown();
        }
      }
    };

    // Start a new HAGroupStoreClient with custom listener.
    new HAGroupStoreClient(CLUSTERS.getHBaseCluster1().getConfiguration(),
      pathChildrenCacheListener, null, haGroupName, zkUrl);

    // Create multiple threads for update to ZK.
    final CountDownLatch updateLatch = new CountDownLatch(threadCount);
    ExecutorService executor = Executors.newFixedThreadPool(threadCount);

    // List captures the order of events that are sent.
    List<Integer> updateList = new ArrayList<>();

    // Create a queue which can be polled to send updates to ZK.
    ConcurrentLinkedQueue<HAGroupStoreRecord> updateQueue = new ConcurrentLinkedQueue<>();
    for (int i = 0; i < threadCount; i++) {
      updateQueue.add(createHAGroupStoreRecord(haGroupName));
      updateList.add(i);
    }

    // Submit updates to ZK.
    for (int i = 0; i < threadCount; i++) {
      executor.submit(() -> {
        try {
          synchronized (HAGroupStoreClientIT.class) {
            HAGroupStoreRecord record = Objects.requireNonNull(updateQueue.poll());
            createOrUpdateHAGroupStoreRecordOnZookeeper(haAdmin, haGroupName, record);
          }
          updateLatch.countDown();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      });
    }

    // Check if updates are sent and updates are received.
    assert updateLatch.await(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS * threadCount,
      TimeUnit.MILLISECONDS);
    assert eventsLatch.await(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS * threadCount,
      TimeUnit.MILLISECONDS);

    // Assert that the order of updates is same as order of events.
    assert updateList.equals(recordEventVersions);
  }

  @Test
  public void testGetClusterRoleRecordNormalCase() throws Exception {
    String haGroupName = testName.getMethodName();
    HAGroupStoreTestUtil.upsertHAGroupRecordInSystemTable(haGroupName, this.zkUrl, this.peerZKUrl,
      this.masterUrl, this.peerMasterUrl, ClusterRoleRecord.ClusterRole.ACTIVE,
      ClusterRoleRecord.ClusterRole.STANDBY, null, CLUSTERS.getHdfsUrl1(), CLUSTERS.getHdfsUrl2());

    // Create HAGroupStoreRecord for local cluster
    HAGroupStoreRecord localRecord =
      new HAGroupStoreRecord("v1.0", haGroupName, HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC,
        0L, HighAvailabilityPolicy.FAILOVER.toString(), this.peerZKUrl, this.masterUrl,
        this.peerMasterUrl, CLUSTERS.getHdfsUrl1(), CLUSTERS.getHdfsUrl2(), 0L);
    createOrUpdateHAGroupStoreRecordOnZookeeper(haAdmin, haGroupName, localRecord);

    // Create HAGroupStoreRecord for peer cluster
    HAGroupStoreRecord peerRecord =
      new HAGroupStoreRecord("v1.0", haGroupName, HAGroupStoreRecord.HAGroupState.STANDBY, 0L,
        HighAvailabilityPolicy.FAILOVER.toString(), this.peerZKUrl, this.peerMasterUrl,
        this.masterUrl, CLUSTERS.getHdfsUrl1(), CLUSTERS.getHdfsUrl2(), 0L);
    createOrUpdateHAGroupStoreRecordOnZookeeper(peerHaAdmin, haGroupName, peerRecord);

    Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

    HAGroupStoreClient haGroupStoreClient = HAGroupStoreClient
      .getInstanceForZkUrl(CLUSTERS.getHBaseCluster1().getConfiguration(), haGroupName, zkUrl);
    assertNotNull(haGroupStoreClient);

    // Test getClusterRoleRecord
    ClusterRoleRecord clusterRoleRecord = haGroupStoreClient.getClusterRoleRecord();
    assertNotNull(clusterRoleRecord);
    ClusterRoleRecord expectedClusterRoleRecord = new ClusterRoleRecord(haGroupName,
      HighAvailabilityPolicy.FAILOVER, this.masterUrl, ClusterRoleRecord.ClusterRole.ACTIVE,
      this.peerMasterUrl, ClusterRoleRecord.ClusterRole.STANDBY, 0);
    assertEquals(expectedClusterRoleRecord, clusterRoleRecord);
  }

  @Test
  public void testGetClusterRoleRecordWithValidPeerZKUrlButNoPeerRecord() throws Exception {
    String haGroupName = testName.getMethodName();
    HAGroupStoreTestUtil.upsertHAGroupRecordInSystemTable(haGroupName, this.zkUrl, this.peerZKUrl,
      this.masterUrl, this.peerMasterUrl, ClusterRoleRecord.ClusterRole.ACTIVE,
      ClusterRoleRecord.ClusterRole.STANDBY, null, CLUSTERS.getHdfsUrl1(), CLUSTERS.getHdfsUrl2());

    // Create HAGroupStoreRecord for local cluster only (no peer record)
    HAGroupStoreRecord localRecord =
      new HAGroupStoreRecord("v1.0", haGroupName, HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC,
        0L, HighAvailabilityPolicy.FAILOVER.toString(), this.peerZKUrl, this.masterUrl,
        this.peerMasterUrl, CLUSTERS.getHdfsUrl1(), CLUSTERS.getHdfsUrl2(), 0L);
    createOrUpdateHAGroupStoreRecordOnZookeeper(haAdmin, haGroupName, localRecord);

    // Explicitly ensure no peer record exists
    String peerPath = toPath(haGroupName);
    if (peerHaAdmin.getCurator().checkExists().forPath(peerPath) != null) {
      peerHaAdmin.getCurator().delete().quietly().forPath(peerPath);
    }

    Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

    HAGroupStoreClient haGroupStoreClient = HAGroupStoreClient
      .getInstanceForZkUrl(CLUSTERS.getHBaseCluster1().getConfiguration(), haGroupName, zkUrl);
    assertNotNull(haGroupStoreClient);

    // Test getClusterRoleRecord when peer ZK URL is valid but no peer HAGroupStoreRecord exists in
    // peer ZK
    ClusterRoleRecord clusterRoleRecord = haGroupStoreClient.getClusterRoleRecord();
    assertNotNull(clusterRoleRecord);
    ClusterRoleRecord expectedClusterRoleRecord = new ClusterRoleRecord(haGroupName,
      HighAvailabilityPolicy.FAILOVER, this.masterUrl, ClusterRoleRecord.ClusterRole.ACTIVE,
      this.peerMasterUrl, ClusterRoleRecord.ClusterRole.UNKNOWN, 0);
    assertEquals(expectedClusterRoleRecord, clusterRoleRecord);
  }

  private HAGroupStoreRecord createHAGroupStoreRecord(String haGroupName) {
    return new HAGroupStoreRecord("v1.0", haGroupName,
      HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC, 0L,
      HighAvailabilityPolicy.FAILOVER.toString(), this.peerZKUrl, this.masterUrl,
      this.peerMasterUrl, CLUSTERS.getHdfsUrl1(), CLUSTERS.getHdfsUrl2(), 0L);
  }

  // Tests for getHAGroupNames static method
  @Test
  public void testGetHAGroupNamesWithSingleGroup() throws Exception {
    String haGroupName = testName.getMethodName();
    HAGroupStoreTestUtil.upsertHAGroupRecordInSystemTable(haGroupName, this.zkUrl, this.peerZKUrl,
      this.masterUrl, this.peerMasterUrl, ClusterRoleRecord.ClusterRole.ACTIVE,
      ClusterRoleRecord.ClusterRole.STANDBY, null, CLUSTERS.getHdfsUrl1(), CLUSTERS.getHdfsUrl2());

    List<String> haGroupNames = HAGroupStoreClient.getHAGroupNames(zkUrl);

    assertNotNull("HA group names list should not be null", haGroupNames);
    assertTrue("HA group names list should contain the test group",
      haGroupNames.contains(haGroupName));

    // Clean up
    HAGroupStoreTestUtil.deleteHAGroupRecordInSystemTable(haGroupName, zkUrl);
  }

  @Test
  public void testGetHAGroupNamesWithMultipleGroups() throws Exception {
    // Delete old HA entry created by Before method
    HAGroupStoreTestUtil.deleteHAGroupRecordInSystemTable(testName.getMethodName(), this.zkUrl);
    // Verify that there are no groups returned
    assertEquals(0, HAGroupStoreClient.getHAGroupNames(zkUrl).size());

    String haGroupName1 = testName.getMethodName() + "_1";
    String haGroupName2 = testName.getMethodName() + "_2";
    String haGroupName3 = testName.getMethodName() + "_3";
    String haGroupName4 = testName.getMethodName() + "_4";

    // Insert multiple HA group records
    HAGroupStoreTestUtil.upsertHAGroupRecordInSystemTable(haGroupName1, this.zkUrl, this.peerZKUrl,
      this.masterUrl, this.peerMasterUrl, ClusterRoleRecord.ClusterRole.ACTIVE,
      ClusterRoleRecord.ClusterRole.STANDBY, null, CLUSTERS.getHdfsUrl1(), CLUSTERS.getHdfsUrl2());
    HAGroupStoreTestUtil.upsertHAGroupRecordInSystemTable(haGroupName2, this.zkUrl, this.peerZKUrl,
      this.masterUrl, this.peerMasterUrl, ClusterRoleRecord.ClusterRole.STANDBY,
      ClusterRoleRecord.ClusterRole.ACTIVE, null, CLUSTERS.getHdfsUrl1(), CLUSTERS.getHdfsUrl2());
    HAGroupStoreTestUtil.upsertHAGroupRecordInSystemTable(haGroupName3, this.zkUrl, this.peerZKUrl,
      this.masterUrl, this.peerMasterUrl, ClusterRoleRecord.ClusterRole.ACTIVE,
      ClusterRoleRecord.ClusterRole.STANDBY, null, CLUSTERS.getHdfsUrl1(), CLUSTERS.getHdfsUrl2());
    HAGroupStoreTestUtil.upsertHAGroupRecordInSystemTable(haGroupName4, "bad_zk_url",
      this.peerZKUrl, this.masterUrl, this.peerMasterUrl, ClusterRoleRecord.ClusterRole.ACTIVE,
      ClusterRoleRecord.ClusterRole.STANDBY, this.zkUrl, CLUSTERS.getHdfsUrl1(),
      CLUSTERS.getHdfsUrl2());

    List<String> haGroupNames = HAGroupStoreClient.getHAGroupNames(zkUrl);

    assertNotNull("HA group names list should not be null", haGroupNames);
    assertTrue("HA group names list should contain haGroupName1",
      haGroupNames.contains(haGroupName1));
    assertTrue("HA group names list should contain haGroupName2",
      haGroupNames.contains(haGroupName2));
    assertTrue("HA group names list should contain haGroupName3",
      haGroupNames.contains(haGroupName3));
    assertTrue("HA group names list should not contain haGroupName4",
      !haGroupNames.contains(haGroupName4));
    assertEquals(3, haGroupNames.size());

    // Clean up
    HAGroupStoreTestUtil.deleteHAGroupRecordInSystemTable(haGroupName1, zkUrl);
    HAGroupStoreTestUtil.deleteHAGroupRecordInSystemTable(haGroupName2, zkUrl);
    HAGroupStoreTestUtil.deleteHAGroupRecordInSystemTable(haGroupName3, zkUrl);
  }

  @Test
  public void testGetHAGroupNamesWithEmptyTable() throws Exception {
    // First, delete any existing records that might interfere
    List<String> existingGroups = HAGroupStoreClient.getHAGroupNames(zkUrl);
    for (String groupName : existingGroups) {
      if (groupName.startsWith("test")) { // Only clean up test groups
        HAGroupStoreTestUtil.deleteHAGroupRecordInSystemTable(groupName, zkUrl);
      }
    }

    // Verify the table is empty of test records
    List<String> haGroupNames = HAGroupStoreClient.getHAGroupNames(zkUrl);
    long testGroupCount = haGroupNames.stream().filter(name -> name.startsWith("test")).count();
    assertEquals("Should have no test groups", 0, testGroupCount);
  }

  @Test
  public void testGetHAGroupNamesAfterDeletingGroups() throws Exception {
    String haGroupName1 = testName.getMethodName() + "_delete_1";
    String haGroupName2 = testName.getMethodName() + "_delete_2";

    // Insert HA group records
    HAGroupStoreTestUtil.upsertHAGroupRecordInSystemTable(haGroupName1, this.zkUrl, this.peerZKUrl,
      this.masterUrl, this.peerMasterUrl, ClusterRoleRecord.ClusterRole.ACTIVE,
      ClusterRoleRecord.ClusterRole.STANDBY, null, CLUSTERS.getHdfsUrl1(), CLUSTERS.getHdfsUrl2());
    HAGroupStoreTestUtil.upsertHAGroupRecordInSystemTable(haGroupName2, this.zkUrl, this.peerZKUrl,
      this.masterUrl, this.peerMasterUrl, ClusterRoleRecord.ClusterRole.STANDBY,
      ClusterRoleRecord.ClusterRole.ACTIVE, null, CLUSTERS.getHdfsUrl1(), CLUSTERS.getHdfsUrl2());

    // Verify both groups exist
    List<String> haGroupNames = HAGroupStoreClient.getHAGroupNames(zkUrl);
    assertTrue("Should contain haGroupName1", haGroupNames.contains(haGroupName1));
    assertTrue("Should contain haGroupName2", haGroupNames.contains(haGroupName2));

    // Delete one group
    HAGroupStoreTestUtil.deleteHAGroupRecordInSystemTable(haGroupName1, zkUrl);

    // Verify only one group remains
    haGroupNames = HAGroupStoreClient.getHAGroupNames(zkUrl);
    assertFalse("Should not contain deleted haGroupName1", haGroupNames.contains(haGroupName1));
    assertTrue("Should still contain haGroupName2", haGroupNames.contains(haGroupName2));

    // Clean up remaining group
    HAGroupStoreTestUtil.deleteHAGroupRecordInSystemTable(haGroupName2, zkUrl);

    // Verify the group is gone
    haGroupNames = HAGroupStoreClient.getHAGroupNames(zkUrl);
    assertFalse("Should not contain deleted haGroupName2", haGroupNames.contains(haGroupName2));
  }

  @Test
  public void testGetHAGroupNamesWithInvalidZkUrl() throws Exception {
    String invalidZkUrl = "invalid:2181";

    try {
      HAGroupStoreClient.getHAGroupNames(invalidZkUrl);
      fail("Expected SQLException for invalid ZK URL");
    } catch (SQLException e) {
      // Expected - the connection should fail with invalid ZK URL
      assertNotNull("SQLException should have a message", e.getMessage());
    }
  }

  @Test
  public void testGetHAGroupNamesWithNullZkUrl() throws Exception {
    try {
      HAGroupStoreClient.getHAGroupNames(null);
      fail("Expected SQLException for null ZK URL");
    } catch (SQLException e) {
      // Expected - the connection should fail with null ZK URL
      assertNotNull("SQLException should have a message", e.getMessage());
    }
  }

  @Test
  public void testPeriodicSyncJobExecutorStartsAndSyncsData() throws Exception {
    String haGroupName = testName.getMethodName();

    // 1. Setup: Create initial system table record with default values
    HAGroupStoreTestUtil.upsertHAGroupRecordInSystemTable(haGroupName, this.zkUrl, this.peerZKUrl,
      this.masterUrl, this.peerMasterUrl, ClusterRoleRecord.ClusterRole.ACTIVE,
      ClusterRoleRecord.ClusterRole.STANDBY, null, CLUSTERS.getHdfsUrl1(), CLUSTERS.getHdfsUrl2());

    // 2. Create ZK record with DIFFERENT values for testable fields (skip zkUrl changes)
    String updatedClusterUrl = this.masterUrl + ":updated";
    String updatedPeerClusterUrl = this.peerMasterUrl + ":updated";
    HAGroupStoreRecord zkRecord = new HAGroupStoreRecord("v2.0", haGroupName,
      HAGroupStoreRecord.HAGroupState.DEGRADED_STANDBY, System.currentTimeMillis(), // Different
                                                                                    // state and
                                                                                    // sync time
      HighAvailabilityPolicy.FAILOVER.toString(), this.peerZKUrl, // Keep original peer ZK URL
      updatedClusterUrl, // Different cluster URL
      updatedPeerClusterUrl, // Different peer cluster URL
      CLUSTERS.getHdfsUrl1(), CLUSTERS.getHdfsUrl2(), 5L); // Different version
    createOrUpdateHAGroupStoreRecordOnZookeeper(haAdmin, haGroupName, zkRecord);

    // Also create a peer ZK record with STANDBY_TO_ACTIVE role to test peer role sync
    HAGroupStoreRecord peerZkRecord =
      new HAGroupStoreRecord("v2.0", haGroupName, HAGroupStoreRecord.HAGroupState.STANDBY_TO_ACTIVE,
        0L, HighAvailabilityPolicy.FAILOVER.toString(), updatedClusterUrl, this.peerMasterUrl,
        updatedClusterUrl, CLUSTERS.getHdfsUrl1(), CLUSTERS.getHdfsUrl2(), 5L);
    createOrUpdateHAGroupStoreRecordOnZookeeper(peerHaAdmin, haGroupName, peerZkRecord);

    // 3. Create HAGroupStoreClient with short sync interval for testing
    Configuration testConf = new Configuration(CLUSTERS.getHBaseCluster1().getConfiguration());
    testConf.setLong("phoenix.ha.group.store.sync.interval.seconds", 15); // 15 seconds for faster
                                                                          // testing

    try (HAGroupStoreClient haGroupStoreClient =
      new HAGroupStoreClient(testConf, null, null, haGroupName, zkUrl)) {

      // 3. Verify sync executor is running by checking private field via reflection
      Field syncExecutorField = HAGroupStoreClient.class.getDeclaredField("syncExecutor");
      syncExecutorField.setAccessible(true);
      ScheduledExecutorService syncExecutor =
        (ScheduledExecutorService) syncExecutorField.get(haGroupStoreClient);
      assertNotNull("Sync executor should be initialized", syncExecutor);
      assertFalse("Sync executor should not be shutdown", syncExecutor.isShutdown());

      // 4. Wait for at least one sync cycle (with jitter buffer)
      Thread.sleep(25000); // Wait 25 seconds (15s + 10s buffer for jitter)

      // 5. Verify that system table was updated with ZK data (ZK is source of truth)
      // Check system table directly to see if all fields were synced
      try (
        PhoenixConnection conn = (PhoenixConnection) DriverManager
          .getConnection(JDBC_PROTOCOL_ZK + JDBC_PROTOCOL_SEPARATOR + zkUrl);
        Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery("SELECT * FROM "
          + SYSTEM_HA_GROUP_NAME + " WHERE HA_GROUP_NAME = '" + haGroupName + "'")) {
        assertTrue("System table should have record", rs.next());

        // Verify all fields were synced from ZK with the UPDATED values (except zkUrls which remain
        // unchanged)
        assertEquals("HA_GROUP_NAME should match", haGroupName, rs.getString("HA_GROUP_NAME"));
        assertEquals("POLICY should be synced from ZK", "FAILOVER", rs.getString("POLICY"));
        assertEquals("VERSION should be synced from ZK", 5L, rs.getLong("VERSION"));
        assertEquals("ZK_URL_1 should remain unchanged", this.zkUrl, rs.getString("ZK_URL_1"));
        assertEquals("ZK_URL_2 should remain unchanged", this.peerZKUrl, rs.getString("ZK_URL_2"));
        assertEquals("CLUSTER_ROLE_1 should be synced", "STANDBY", rs.getString("CLUSTER_ROLE_1")); // DEGRADED_STANDBY
                                                                                                    // maps
                                                                                                    // to
                                                                                                    // STANDBY
                                                                                                    // role
        assertEquals("CLUSTER_ROLE_2 should be synced", "STANDBY_TO_ACTIVE",
          rs.getString("CLUSTER_ROLE_2")); // Peer role from peer ZK
        assertEquals("CLUSTER_URL_1 should be synced", updatedClusterUrl,
          rs.getString("CLUSTER_URL_1"));
        assertEquals("CLUSTER_URL_2 should be synced", updatedPeerClusterUrl,
          rs.getString("CLUSTER_URL_2"));

        // All fields successfully verified - sync job is working correctly
      }

      // 7. Test that no update happens when system table is already in sync with ZK
      // Wait for another sync cycle to ensure the optimization is working
      Thread.sleep(16000); // Wait for another sync cycle (15s + 1s buffer)

      // Verify system table still has the same data (no redundant updates)
      try (
        PhoenixConnection conn = (PhoenixConnection) DriverManager
          .getConnection(JDBC_PROTOCOL_ZK + JDBC_PROTOCOL_SEPARATOR + zkUrl);
        Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery("SELECT * FROM "
          + SYSTEM_HA_GROUP_NAME + " WHERE HA_GROUP_NAME = '" + haGroupName + "'")) {
        assertTrue("System table should still have record", rs.next());

        // Verify all fields remain the same (no unnecessary update occurred)
        assertEquals("VERSION should remain the same", 5L, rs.getLong("VERSION"));
        assertEquals("CLUSTER_ROLE_1 should remain the same", "STANDBY",
          rs.getString("CLUSTER_ROLE_1"));
        assertEquals("CLUSTER_ROLE_2 should remain the same", "STANDBY_TO_ACTIVE",
          rs.getString("CLUSTER_ROLE_2"));
        assertEquals("CLUSTER_URL_1 should remain the same", updatedClusterUrl,
          rs.getString("CLUSTER_URL_1"));
        assertEquals("CLUSTER_URL_2 should remain the same", updatedPeerClusterUrl,
          rs.getString("CLUSTER_URL_2"));

        // This verifies that the equals() check is working and preventing redundant updates
      }

      // 8. Test cleanup - verify executor shuts down properly when we exit try-with-resources
      // The close() will be called automatically, and we can verify shutdown in a separate
      // assertion
      haGroupStoreClient.close(); // Explicit close to test shutdown
      assertTrue("Sync executor should be shutdown after close", syncExecutor.isShutdown());
    }
  }

  // ============================================================================================
  // Legacy /phoenix/ha CRR sync tests
  // Verify feature-flag gating, derivation, monotonic version, registry-type preservation,
  // deletion mirroring, and short-circuit behavior of HAGroupStoreClient's legacy sync path.
  // ============================================================================================

  @Test
  public void testLegacyCrrSyncFeatureOffByDefault_NoLegacyZnodeWritten() throws Exception {
    String haGroupName = testName.getMethodName();
    Configuration conf = legacyCrrConf(/* legacyEnabled */ false, /* periodicSec */ 60);
    HAGroupStoreClient client = HAGroupStoreClient.getInstanceForZkUrl(conf, haGroupName, zkUrl);
    assertNotNull(client);
    Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);
    Pair<ClusterRoleRecord, Stat> legacy = readLegacyCrr(haGroupName);
    assertNull("Legacy CRR must not exist when feature is off", legacy.getLeft());
    assertNull("Legacy znode stat must be null when feature is off", legacy.getRight());
  }

  @Test
  public void testLegacyCrrSyncFeatureOn_InitialSyncCreatesZkRegistryLegacyZnode()
    throws Exception {
    String haGroupName = testName.getMethodName();
    Configuration conf = legacyCrrConf(true, 60);
    HAGroupStoreClient client = HAGroupStoreClient.getInstanceForZkUrl(conf, haGroupName, zkUrl);
    assertNotNull(client);

    Pair<ClusterRoleRecord, Stat> legacy = awaitLegacyCrrPresent(haGroupName);
    ClusterRoleRecord crr = legacy.getLeft();
    assertEquals("Legacy CRR must use ZK registry type for backward compatibility",
      ClusterRoleRecord.RegistryType.ZK, crr.getRegistryType());
    assertEquals(haGroupName, crr.getHaGroupName());
    assertEquals(HighAvailabilityPolicy.FAILOVER, crr.getPolicy());
    // Local cluster role is ACTIVE per System Table seed.
    assertEquals(ClusterRoleRecord.ClusterRole.ACTIVE,
      crr.getRole(formattedZkUrlFor(ClusterType.LOCAL)));
    assertTrue("CRR version must be > 0 after initial sync", crr.getVersion() > 0);
  }

  @Test
  public void testLegacyCrrSyncRoleChangePropagatesAndIsNewerThanWorks() throws Exception {
    String haGroupName = testName.getMethodName();
    Configuration conf = legacyCrrConf(true, 60);
    HAGroupStoreClient client = HAGroupStoreClient.getInstanceForZkUrl(conf, haGroupName, zkUrl);
    assertNotNull(client);

    ClusterRoleRecord initial = awaitLegacyCrrPresent(haGroupName).getLeft();
    long initialVersion = initial.getVersion();

    // ACTIVE_IN_SYNC -> ACTIVE_IN_SYNC_TO_STANDBY (role change ACTIVE -> ACTIVE_TO_STANDBY).
    client.setHAGroupStatusIfNeeded(HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC_TO_STANDBY);
    Pair<ClusterRoleRecord, Stat> updated = awaitLegacyCrrRole(haGroupName, ClusterType.LOCAL,
      ClusterRoleRecord.ClusterRole.ACTIVE_TO_STANDBY);
    ClusterRoleRecord updatedCRR = updated.getLeft();
    assertTrue("Version must monotonically increase after role change",
      updatedCRR.getVersion() > initialVersion);
    assertTrue("isNewerThan must return true for the updated record",
      updatedCRR.isNewerThan(initial));
    assertEquals(ClusterRoleRecord.RegistryType.ZK, updatedCRR.getRegistryType());
  }

  @Test
  public void testLegacyCrrSyncStateOnlyChangeDoesNotRewriteLegacy() throws Exception {
    String haGroupName = testName.getMethodName();
    Configuration conf = legacyCrrConf(true, 60);
    HAGroupStoreClient client = HAGroupStoreClient.getInstanceForZkUrl(conf, haGroupName, zkUrl);
    assertNotNull(client);

    Pair<ClusterRoleRecord, Stat> initial = awaitLegacyCrrPresent(haGroupName);
    int initialZkVersion = initial.getRight().getVersion();
    long initialCrrVersion = initial.getLeft().getVersion();

    // ACTIVE_IN_SYNC -> ACTIVE_NOT_IN_SYNC: ClusterRole stays ACTIVE.
    client.setHAGroupStatusIfNeeded(HAGroupStoreRecord.HAGroupState.ACTIVE_NOT_IN_SYNC);
    Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

    Pair<ClusterRoleRecord, Stat> after = readLegacyCrr(haGroupName);
    assertNotNull(after.getLeft());
    assertEquals("Legacy CRR ZK stat version must not change on state-only transitions",
      initialZkVersion, after.getRight().getVersion());
    assertEquals("Legacy CRR logical version must not change on state-only transitions",
      initialCrrVersion, after.getLeft().getVersion());
  }

  /** LOCAL CHILD_REMOVED on consistentHA does not delete the legacy znode. */
  @Test
  public void testLegacyCrrSyncLocalChildRemovedDoesNotDeleteLegacy() throws Exception {
    String haGroupName = testName.getMethodName();
    Configuration conf = legacyCrrConf(true, 60);
    HAGroupStoreClient client = HAGroupStoreClient.getInstanceForZkUrl(conf, haGroupName, zkUrl);
    assertNotNull(client);
    Pair<ClusterRoleRecord, Stat> initial = awaitLegacyCrrPresent(haGroupName);
    long initialVersion = initial.getLeft().getVersion();

    haAdmin.deleteHAGroupStoreRecordInZooKeeper(haGroupName);

    // Wait long enough for any potential event-driven delete to have fired.
    Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

    Pair<ClusterRoleRecord, Stat> after = readLegacyCrr(haGroupName);
    assertNotNull("Legacy znode must NOT be deleted on LOCAL CHILD_REMOVED", after.getLeft());
    assertTrue("Legacy CRR version must not regress after LOCAL CHILD_REMOVED",
      after.getLeft().getVersion() >= initialVersion);
  }

  @Test
  public void testLegacyCrrSyncPeriodicDisabledStillSyncsViaEvents() throws Exception {
    String haGroupName = testName.getMethodName();
    Configuration conf = legacyCrrConf(true, 0); // periodic disabled
    HAGroupStoreClient client = HAGroupStoreClient.getInstanceForZkUrl(conf, haGroupName, zkUrl);
    assertNotNull(client);
    ClusterRoleRecord initial = awaitLegacyCrrPresent(haGroupName).getLeft();
    assertEquals("Initial local role should be ACTIVE per @Before seed",
      ClusterRoleRecord.ClusterRole.ACTIVE, initial.getRole(formattedZkUrlFor(ClusterType.LOCAL)));
    assertEquals("Initial registry type must be ZK", ClusterRoleRecord.RegistryType.ZK,
      initial.getRegistryType());

    client.setHAGroupStatusIfNeeded(HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC_TO_STANDBY);
    ClusterRoleRecord updated = awaitLegacyCrrRole(haGroupName, ClusterType.LOCAL,
      ClusterRoleRecord.ClusterRole.ACTIVE_TO_STANDBY).getLeft();
    assertTrue("Updated version must monotonically advance past the initial version",
      updated.getVersion() > initial.getVersion());
    assertTrue("isNewerThan must return true for the post-event record",
      updated.isNewerThan(initial));
    assertEquals("Registry type must remain ZK after an event-driven sync",
      ClusterRoleRecord.RegistryType.ZK, updated.getRegistryType());
  }

  /** PEER CHILD_REMOVED on consistentHA does not delete the legacy znode. */
  @Test
  public void testLegacyCrrSyncPeerChildRemovedDoesNotDeleteLegacy() throws Exception {
    String haGroupName = testName.getMethodName();
    // Seed a peer record so that PEER cache initializes and PEER CHILD_REMOVED can fire later.
    HAGroupStoreRecord peerRecord =
      new HAGroupStoreRecord("v1.0", haGroupName, HAGroupStoreRecord.HAGroupState.STANDBY, 0L,
        HighAvailabilityPolicy.FAILOVER.toString(), this.zkUrl, this.peerMasterUrl, this.masterUrl,
        CLUSTERS.getHdfsUrl2(), CLUSTERS.getHdfsUrl1(), 0L);
    createOrUpdateHAGroupStoreRecordOnZookeeper(peerHaAdmin, haGroupName, peerRecord);

    Configuration conf = legacyCrrConf(true, 0); // periodic disabled to isolate event behavior
    HAGroupStoreClient client = HAGroupStoreClient.getInstanceForZkUrl(conf, haGroupName, zkUrl);
    assertNotNull(client);

    Pair<ClusterRoleRecord, Stat> initial = awaitLegacyCrrPresent(haGroupName);
    long initialCrrVersion = initial.getLeft().getVersion();

    peerHaAdmin.deleteHAGroupStoreRecordInZooKeeper(haGroupName);
    Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

    Pair<ClusterRoleRecord, Stat> after = readLegacyCrr(haGroupName);
    assertNotNull("Legacy znode must NOT be deleted on PEER CHILD_REMOVED", after.getLeft());
    assertTrue("Legacy CRR version must not regress after PEER CHILD_REMOVED",
      after.getLeft().getVersion() >= initialCrrVersion);
  }

  /**
   * Each {@link PhoenixHAAdmin.LegacyCrrWriteMode}: error mapping (BadVersion + NodeExists ->
   * {@link StaleClusterRoleRecordVersionException}), unconditional FORCE_OVERWRITE, and
   * CAS_WITH_VERSION rejecting negative versions. Sequential: ZK serializes versioned writes
   * server-side, so the client retry path is identical to a real race.
   */
  @Test
  public void testLegacyCrrCasErrorMappingAndModeDispatch() throws Exception {
    String haGroupName = testName.getMethodName();

    // Create.
    ClusterRoleRecord initial = new ClusterRoleRecord(haGroupName, HighAvailabilityPolicy.FAILOVER,
      ClusterRoleRecord.RegistryType.ZK, this.zkUrl, ClusterRoleRecord.ClusterRole.ACTIVE,
      this.peerZKUrl, ClusterRoleRecord.ClusterRole.STANDBY, 1L);
    legacyHaAdmin.createOrUpdateClusterRoleRecordWithCAS(haGroupName, initial,
      PhoenixHAAdmin.LegacyCrrWriteMode.CREATE_NEW, 0);

    Pair<ClusterRoleRecord, Stat> existing =
      legacyHaAdmin.getClusterRoleRecordAndStatInZooKeeper(haGroupName);
    assertNotNull(existing.getLeft());
    int sharedVersion = existing.getRight().getVersion();

    // CAS winner.
    ClusterRoleRecord writerA = new ClusterRoleRecord(haGroupName, HighAvailabilityPolicy.FAILOVER,
      ClusterRoleRecord.RegistryType.ZK, this.zkUrl,
      ClusterRoleRecord.ClusterRole.ACTIVE_TO_STANDBY, this.peerZKUrl,
      ClusterRoleRecord.ClusterRole.STANDBY, 2L);
    legacyHaAdmin.createOrUpdateClusterRoleRecordWithCAS(haGroupName, writerA,
      PhoenixHAAdmin.LegacyCrrWriteMode.CAS_WITH_VERSION, sharedVersion);

    // CAS loser: same expected version -> BadVersion -> StaleClusterRoleRecordVersionException.
    ClusterRoleRecord writerB = new ClusterRoleRecord(haGroupName, HighAvailabilityPolicy.FAILOVER,
      ClusterRoleRecord.RegistryType.ZK, this.zkUrl, ClusterRoleRecord.ClusterRole.STANDBY,
      this.peerZKUrl, ClusterRoleRecord.ClusterRole.ACTIVE, 2L);
    assertThrows(StaleClusterRoleRecordVersionException.class,
      () -> legacyHaAdmin.createOrUpdateClusterRoleRecordWithCAS(haGroupName, writerB,
        PhoenixHAAdmin.LegacyCrrWriteMode.CAS_WITH_VERSION, sharedVersion));

    Pair<ClusterRoleRecord, Stat> winner =
      legacyHaAdmin.getClusterRoleRecordAndStatInZooKeeper(haGroupName);
    assertEquals(ClusterRoleRecord.ClusterRole.ACTIVE_TO_STANDBY,
      winner.getLeft().getRole(formattedZkUrlFor(ClusterType.LOCAL)));
    assertTrue(winner.getRight().getVersion() > sharedVersion);

    // CREATE_NEW on an existing znode -> NodeExists -> StaleClusterRoleRecordVersionException.
    ClusterRoleRecord raceCreate =
      new ClusterRoleRecord(haGroupName, HighAvailabilityPolicy.FAILOVER,
        ClusterRoleRecord.RegistryType.ZK, this.zkUrl, ClusterRoleRecord.ClusterRole.STANDBY,
        this.peerZKUrl, ClusterRoleRecord.ClusterRole.STANDBY, 3L);
    assertThrows(StaleClusterRoleRecordVersionException.class,
      () -> legacyHaAdmin.createOrUpdateClusterRoleRecordWithCAS(haGroupName, raceCreate,
        PhoenixHAAdmin.LegacyCrrWriteMode.CREATE_NEW, 0));

    // FORCE_OVERWRITE bypasses CAS and bumps the stat version.
    Stat statBeforeOverwrite =
      legacyHaAdmin.getClusterRoleRecordAndStatInZooKeeper(haGroupName).getRight();
    ClusterRoleRecord overwrite =
      new ClusterRoleRecord(haGroupName, HighAvailabilityPolicy.FAILOVER,
        ClusterRoleRecord.RegistryType.ZK, this.zkUrl, ClusterRoleRecord.ClusterRole.STANDBY,
        this.peerZKUrl, ClusterRoleRecord.ClusterRole.STANDBY, 4L);
    legacyHaAdmin.createOrUpdateClusterRoleRecordWithCAS(haGroupName, overwrite,
      PhoenixHAAdmin.LegacyCrrWriteMode.FORCE_OVERWRITE, 0);
    Pair<ClusterRoleRecord, Stat> afterOverwrite =
      legacyHaAdmin.getClusterRoleRecordAndStatInZooKeeper(haGroupName);
    assertEquals(ClusterRoleRecord.ClusterRole.STANDBY,
      afterOverwrite.getLeft().getRole(formattedZkUrlFor(ClusterType.LOCAL)));
    assertTrue(afterOverwrite.getRight().getVersion() > statBeforeOverwrite.getVersion());

    // CAS_WITH_VERSION rejects negative expectedStatVersion before any ZK call.
    ClusterRoleRecord illegalRecord =
      new ClusterRoleRecord(haGroupName, HighAvailabilityPolicy.FAILOVER,
        ClusterRoleRecord.RegistryType.ZK, this.zkUrl, ClusterRoleRecord.ClusterRole.OFFLINE,
        this.peerZKUrl, ClusterRoleRecord.ClusterRole.OFFLINE, 5L);
    assertThrows(IllegalArgumentException.class,
      () -> legacyHaAdmin.createOrUpdateClusterRoleRecordWithCAS(haGroupName, illegalRecord,
        PhoenixHAAdmin.LegacyCrrWriteMode.CAS_WITH_VERSION, -1));
    assertThrows(IllegalArgumentException.class,
      () -> legacyHaAdmin.createOrUpdateClusterRoleRecordWithCAS(haGroupName, illegalRecord,
        PhoenixHAAdmin.LegacyCrrWriteMode.CAS_WITH_VERSION, Integer.MIN_VALUE));
  }

  /** Peer-side role flip propagates to role2 in the local legacy CRR. */
  @Test
  public void testLegacyCrrSyncPeerRoleFlipUpdatesLegacyRole2() throws Exception {
    String haGroupName = testName.getMethodName();
    // Seed peer with STANDBY before client starts so the initial sync sees role2=STANDBY.
    HAGroupStoreRecord peerStandby =
      new HAGroupStoreRecord("v1.0", haGroupName, HAGroupStoreRecord.HAGroupState.STANDBY, 0L,
        HighAvailabilityPolicy.FAILOVER.toString(), this.zkUrl, this.peerMasterUrl, this.masterUrl,
        CLUSTERS.getHdfsUrl2(), CLUSTERS.getHdfsUrl1(), 0L);
    createOrUpdateHAGroupStoreRecordOnZookeeper(peerHaAdmin, haGroupName, peerStandby);

    Configuration conf = legacyCrrConf(true, 0); // periodic disabled to isolate event-driven path
    HAGroupStoreClient client = HAGroupStoreClient.getInstanceForZkUrl(conf, haGroupName, zkUrl);
    assertNotNull(client);
    awaitLegacyCrrRole(haGroupName, ClusterType.PEER, ClusterRoleRecord.ClusterRole.STANDBY);

    // Flip the peer record to a state whose cluster role is ACTIVE_TO_STANDBY.
    HAGroupStoreRecord peerFlipped = new HAGroupStoreRecord("v1.0", haGroupName,
      HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC_TO_STANDBY, 0L,
      HighAvailabilityPolicy.FAILOVER.toString(), this.zkUrl, this.peerMasterUrl, this.masterUrl,
      CLUSTERS.getHdfsUrl2(), CLUSTERS.getHdfsUrl1(), 0L);
    createOrUpdateHAGroupStoreRecordOnZookeeper(peerHaAdmin, haGroupName, peerFlipped);

    Pair<ClusterRoleRecord, Stat> after = awaitLegacyCrrRole(haGroupName, ClusterType.PEER,
      ClusterRoleRecord.ClusterRole.ACTIVE_TO_STANDBY);
    assertEquals("Registry type must remain ZK after a peer-driven role flip",
      ClusterRoleRecord.RegistryType.ZK, after.getLeft().getRegistryType());
  }

  /** Absent peer record yields role2=UNKNOWN; converges when the peer record appears. */
  @Test
  public void testLegacyCrrSyncPeerAbsentYieldsUnknownAndConvergesOnRecovery() throws Exception {
    String haGroupName = testName.getMethodName();
    // No peer record seeded: peer cache is empty so getHAGroupStoreRecordFromPeer() returns null
    // and role2 falls through to UNKNOWN.
    Configuration conf = legacyCrrConf(true, 0);
    HAGroupStoreClient client = HAGroupStoreClient.getInstanceForZkUrl(conf, haGroupName, zkUrl);
    assertNotNull(client);
    Pair<ClusterRoleRecord, Stat> initial =
      awaitLegacyCrrRole(haGroupName, ClusterType.PEER, ClusterRoleRecord.ClusterRole.UNKNOWN);
    long initialVersion = initial.getLeft().getVersion();

    // Peer "recovers" by writing its consistentHA record. The PEER CHILD_ADDED event triggers
    // the legacy sync to update role2.
    HAGroupStoreRecord peerRecord =
      new HAGroupStoreRecord("v1.0", haGroupName, HAGroupStoreRecord.HAGroupState.STANDBY, 0L,
        HighAvailabilityPolicy.FAILOVER.toString(), this.zkUrl, this.peerMasterUrl, this.masterUrl,
        CLUSTERS.getHdfsUrl2(), CLUSTERS.getHdfsUrl1(), 0L);
    createOrUpdateHAGroupStoreRecordOnZookeeper(peerHaAdmin, haGroupName, peerRecord);

    Pair<ClusterRoleRecord, Stat> recovered =
      awaitLegacyCrrRole(haGroupName, ClusterType.PEER, ClusterRoleRecord.ClusterRole.STANDBY);
    assertTrue("Version must bump when role2 transitions UNKNOWN -> STANDBY",
      recovered.getLeft().getVersion() > initialVersion);
  }

  /** registryType stays ZK across multiple sync cycles (never reverts to RPC). */
  @Test
  public void testLegacyCrrSyncRegistryTypePreservedAcrossMultipleCycles() throws Exception {
    String haGroupName = testName.getMethodName();
    Configuration conf = legacyCrrConf(true, 0);
    HAGroupStoreClient client = HAGroupStoreClient.getInstanceForZkUrl(conf, haGroupName, zkUrl);
    assertNotNull(client);

    Pair<ClusterRoleRecord, Stat> initial = awaitLegacyCrrPresent(haGroupName);
    assertEquals(ClusterRoleRecord.RegistryType.ZK, initial.getLeft().getRegistryType());
    assertEquals("Initial local role should be ACTIVE per @Before seed",
      ClusterRoleRecord.ClusterRole.ACTIVE,
      initial.getLeft().getRole(formattedZkUrlFor(ClusterType.LOCAL)));
    long lastVersion = initial.getLeft().getVersion();

    // Drive a sequence of distinct peer states; each event drives a sync that rewrites the
    // legacy znode (or short-circuits if logically equal). Direct ZK writes intentionally
    // bypass setHAGroupStatusIfNeeded's transition guard.
    HAGroupStoreRecord.HAGroupState[] cycle =
      new HAGroupStoreRecord.HAGroupState[] { HAGroupStoreRecord.HAGroupState.STANDBY,
        HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC_TO_STANDBY,
        HAGroupStoreRecord.HAGroupState.STANDBY_TO_ACTIVE, HAGroupStoreRecord.HAGroupState.OFFLINE,
        HAGroupStoreRecord.HAGroupState.STANDBY };
    for (HAGroupStoreRecord.HAGroupState state : cycle) {
      HAGroupStoreRecord peer = new HAGroupStoreRecord("v1.0", haGroupName, state, 0L,
        HighAvailabilityPolicy.FAILOVER.toString(), this.zkUrl, this.peerMasterUrl, this.masterUrl,
        CLUSTERS.getHdfsUrl2(), CLUSTERS.getHdfsUrl1(), 0L);
      createOrUpdateHAGroupStoreRecordOnZookeeper(peerHaAdmin, haGroupName, peer);
      Pair<ClusterRoleRecord, Stat> after =
        awaitLegacyCrrRole(haGroupName, ClusterType.PEER, state.getClusterRole());
      assertEquals(
        "Local role must remain ACTIVE across peer-driven cycles (peer state=" + state + ")",
        ClusterRoleRecord.ClusterRole.ACTIVE,
        after.getLeft().getRole(formattedZkUrlFor(ClusterType.LOCAL)));
      assertEquals("Registry type must remain ZK after a sync cycle (peer state=" + state + ")",
        ClusterRoleRecord.RegistryType.ZK, after.getLeft().getRegistryType());
      assertTrue(
        "Logical version must monotonically increase across distinct sync cycles (peer state="
          + state + ")",
        after.getLeft().getVersion() > lastVersion);
      lastVersion = after.getLeft().getVersion();
    }
  }

  /** Periodic loop repairs an external divergence with no consistentHA event. */
  @Test
  public void testLegacyCrrSyncPeriodicReconciliationRecoversAfterDivergence() throws Exception {
    String haGroupName = testName.getMethodName();
    Configuration conf = legacyCrrConf(true, 2); // 2s interval; jitter is 0-30s on first run
    HAGroupStoreClient client = HAGroupStoreClient.getInstanceForZkUrl(conf, haGroupName, zkUrl);
    assertNotNull(client);
    Pair<ClusterRoleRecord, Stat> initial = awaitLegacyCrrPresent(haGroupName);
    assertEquals(ClusterRoleRecord.ClusterRole.ACTIVE,
      initial.getLeft().getRole(formattedZkUrlFor(ClusterType.LOCAL)));

    // Externally corrupt the legacy znode; no consistentHA event fires, so only the periodic
    // reconciler can recover.
    ClusterRoleRecord corrupt = new ClusterRoleRecord(haGroupName, HighAvailabilityPolicy.FAILOVER,
      ClusterRoleRecord.RegistryType.ZK, this.zkUrl, ClusterRoleRecord.ClusterRole.STANDBY,
      this.peerZKUrl, ClusterRoleRecord.ClusterRole.ACTIVE, initial.getLeft().getVersion() + 10);
    legacyHaAdmin.createOrUpdateClusterRoleRecordWithCAS(haGroupName, corrupt,
      PhoenixHAAdmin.LegacyCrrWriteMode.CAS_WITH_VERSION, initial.getRight().getVersion());
    Pair<ClusterRoleRecord, Stat> corrupted = readLegacyCrr(haGroupName);
    assertEquals(ClusterRoleRecord.ClusterRole.STANDBY,
      corrupted.getLeft().getRole(formattedZkUrlFor(ClusterType.LOCAL)));

    // Worst-case wait: jitter up to 30s + 2s interval; allow 40s.
    long deadline = System.currentTimeMillis() + 40_000L;
    Pair<ClusterRoleRecord, Stat> after = readLegacyCrr(haGroupName);
    while (
      (after.getLeft() == null || after.getLeft().getRole(formattedZkUrlFor(ClusterType.LOCAL))
          != ClusterRoleRecord.ClusterRole.ACTIVE)
        && System.currentTimeMillis() < deadline
    ) {
      Thread.sleep(500);
      after = readLegacyCrr(haGroupName);
    }
    assertNotNull(after.getLeft());
    assertEquals(ClusterRoleRecord.ClusterRole.ACTIVE,
      after.getLeft().getRole(formattedZkUrlFor(ClusterType.LOCAL)));
    assertTrue(after.getLeft().getVersion() > corrupted.getLeft().getVersion());
    assertEquals(ClusterRoleRecord.RegistryType.ZK, after.getLeft().getRegistryType());
  }

  /**
   * Peer view absent: client preserves the pre-seeded {@code role2} rather than downgrading it to
   * UNKNOWN. Information from a prior write is more authoritative than a transient gap in the local
   * peer cache; another RS with peer visibility (or this client once peer recovers) will overwrite
   * when there is real news.
   */
  @Test
  public void testLegacyCrrSyncPreservesPreSeededRole2WhenPeerMissing() throws Exception {
    String haGroupName = testName.getMethodName();
    // Pre-seed role2=OFFLINE; do NOT create a peer consistentHA record.
    ClusterRoleRecord preSeed = new ClusterRoleRecord(haGroupName, HighAvailabilityPolicy.FAILOVER,
      ClusterRoleRecord.RegistryType.ZK, this.zkUrl, ClusterRoleRecord.ClusterRole.ACTIVE,
      this.peerZKUrl, ClusterRoleRecord.ClusterRole.OFFLINE, 5L);
    legacyHaAdmin.createOrUpdateClusterRoleRecordWithCAS(haGroupName, preSeed,
      PhoenixHAAdmin.LegacyCrrWriteMode.CREATE_NEW, 0);
    Pair<ClusterRoleRecord, Stat> seeded = readLegacyCrr(haGroupName);
    assertNotNull(seeded.getLeft());
    int seededStatVersion = seeded.getRight().getVersion();

    Configuration conf = legacyCrrConf(true, 0); // periodic disabled; initial sync only
    HAGroupStoreClient client = HAGroupStoreClient.getInstanceForZkUrl(conf, haGroupName, zkUrl);
    assertNotNull(client);

    // Allow the initial async sync ample time to run. With peer absent and role2=OFFLINE
    // already in the znode, the equality check must short-circuit and the znode must remain
    // byte-identical.
    Thread.sleep(3_000);

    Pair<ClusterRoleRecord, Stat> after = readLegacyCrr(haGroupName);
    assertNotNull(after.getLeft());
    assertEquals("Pre-seeded role2 must be preserved when peer view is absent",
      ClusterRoleRecord.ClusterRole.OFFLINE,
      after.getLeft().getRole(formattedZkUrlFor(ClusterType.PEER)));
    assertEquals("Znode must not be rewritten when desired record is logically equal",
      seededStatVersion, after.getRight().getVersion());
    assertEquals(seeded.getLeft().getVersion(), after.getLeft().getVersion());
  }

  /**
   * Peer view present: client overwrites a pre-seeded stale {@code role2} with the live peer state
   * on the initial sync, bumping the version.
   */
  @Test
  public void testLegacyCrrSyncOverwritesPreSeededRole2WhenPeerPresent() throws Exception {
    String haGroupName = testName.getMethodName();
    // Pre-seed role2=OFFLINE.
    ClusterRoleRecord preSeed = new ClusterRoleRecord(haGroupName, HighAvailabilityPolicy.FAILOVER,
      ClusterRoleRecord.RegistryType.ZK, this.zkUrl, ClusterRoleRecord.ClusterRole.ACTIVE,
      this.peerZKUrl, ClusterRoleRecord.ClusterRole.OFFLINE, 5L);
    legacyHaAdmin.createOrUpdateClusterRoleRecordWithCAS(haGroupName, preSeed,
      PhoenixHAAdmin.LegacyCrrWriteMode.CREATE_NEW, 0);
    Pair<ClusterRoleRecord, Stat> seeded = readLegacyCrr(haGroupName);
    assertNotNull(seeded.getLeft());

    // Create a peer consistentHA record so the sync can see real peer state.
    HAGroupStoreRecord peerRecord =
      new HAGroupStoreRecord("v1.0", haGroupName, HAGroupStoreRecord.HAGroupState.STANDBY, 0L,
        HighAvailabilityPolicy.FAILOVER.toString(), this.zkUrl, this.peerMasterUrl, this.masterUrl,
        CLUSTERS.getHdfsUrl2(), CLUSTERS.getHdfsUrl1(), 0L);
    createOrUpdateHAGroupStoreRecordOnZookeeper(peerHaAdmin, haGroupName, peerRecord);

    Configuration conf = legacyCrrConf(true, 0); // periodic disabled; initial sync only
    HAGroupStoreClient client = HAGroupStoreClient.getInstanceForZkUrl(conf, haGroupName, zkUrl);
    assertNotNull(client);

    Pair<ClusterRoleRecord, Stat> after =
      awaitLegacyCrrRole(haGroupName, ClusterType.PEER, ClusterRoleRecord.ClusterRole.STANDBY);
    assertEquals(ClusterRoleRecord.RegistryType.ZK, after.getLeft().getRegistryType());
    assertTrue("Version must bump when peer state replaces stale role2",
      after.getLeft().getVersion() > seeded.getLeft().getVersion());
  }

  // ---------- Legacy CRR sync test helpers ----------

  /** Configuration clone with the legacy CRR flag and reconciliation interval set. */
  private Configuration legacyCrrConf(boolean legacyEnabled, long periodicSec) {
    Configuration src = CLUSTERS.getHBaseCluster1().getConfiguration();
    Configuration cloned = new Configuration(src);
    cloned.setBoolean(PHOENIX_HA_LEGACY_CRR_SYNC_ENABLED, legacyEnabled);
    cloned.setLong(PHOENIX_HA_LEGACY_CRR_RECONCILIATION_INTERVAL_SECONDS, periodicSec);
    return cloned;
  }

  private Pair<ClusterRoleRecord, Stat> readLegacyCrr(String haGroupName) throws IOException {
    return legacyHaAdmin.getClusterRoleRecordAndStatInZooKeeper(haGroupName);
  }

  /** Polls the legacy CRR until {@code condition} matches or the propagation deadline elapses. */
  private Pair<ClusterRoleRecord, Stat> awaitLegacyCrr(String haGroupName,
    Predicate<ClusterRoleRecord> condition, String description) throws Exception {
    long deadline = System.currentTimeMillis() + ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS;
    Pair<ClusterRoleRecord, Stat> legacy = readLegacyCrr(haGroupName);
    while (
      (legacy.getLeft() == null || !condition.test(legacy.getLeft()))
        && System.currentTimeMillis() < deadline
    ) {
      Thread.sleep(100);
      legacy = readLegacyCrr(haGroupName);
    }
    assertNotNull("Legacy znode missing while awaiting: " + description, legacy.getLeft());
    assertTrue("Legacy CRR condition not met within timeout: " + description,
      condition.test(legacy.getLeft()));
    return legacy;
  }

  private Pair<ClusterRoleRecord, Stat> awaitLegacyCrrPresent(String haGroupName) throws Exception {
    return awaitLegacyCrr(haGroupName, crr -> true, "znode present");
  }

  /** Polls until the LOCAL or PEER role in the legacy CRR matches {@code expectedRole}. */
  private Pair<ClusterRoleRecord, Stat> awaitLegacyCrrRole(String haGroupName,
    ClusterType clusterType, ClusterRoleRecord.ClusterRole expectedRole) throws Exception {
    String url = formattedZkUrlFor(clusterType);
    return awaitLegacyCrr(haGroupName, crr -> crr.getRole(url) == expectedRole,
      clusterType + " role == " + expectedRole);
  }

  /** LOCAL or PEER ZK URL in the canonical ZK-registry form used by the legacy sync. */
  private String formattedZkUrlFor(ClusterType clusterType) {
    String raw = (clusterType == ClusterType.LOCAL) ? zkUrl : peerZKUrl;
    return JDBCUtil.formatUrl(raw, ClusterRoleRecord.RegistryType.ZK);
  }
}
