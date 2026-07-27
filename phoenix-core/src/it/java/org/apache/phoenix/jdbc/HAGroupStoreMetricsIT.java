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
import static org.apache.phoenix.jdbc.PhoenixHAAdmin.getLocalZkUrl;
import static org.apache.phoenix.jdbc.PhoenixHAAdmin.toPath;
import static org.apache.phoenix.query.QueryServices.HA_GROUP_STORE_SYNC_INTERVAL_SECONDS;
import static org.apache.phoenix.replication.reader.ReplicationLogReplayService.PHOENIX_REPLICATION_REPLAY_ENABLED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.exception.InvalidClusterRoleTransitionException;
import org.apache.phoenix.jdbc.metrics.HAGroupStoreMetricValues;
import org.apache.phoenix.jdbc.metrics.HAGroupStoreMetricsSourceFactory;
import org.apache.phoenix.util.HAGroupStoreTestUtil;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category(NeedsOwnMiniClusterTest.class)
public class HAGroupStoreMetricsIT extends HABaseIT {

  private static final long EVENT_TIMEOUT_MS = 60000L;

  @Rule
  public final TestName testName = new TestName();

  private PhoenixHAAdmin localAdmin;
  private PhoenixHAAdmin peerAdmin;
  private String zkUrl;
  private String peerZkUrl;
  private String masterUrl;
  private String peerMasterUrl;
  private HAGroupStoreClient clientToClose;

  @BeforeClass
  public static synchronized void setUpClass() throws Exception {
    conf1.setBoolean(PHOENIX_REPLICATION_REPLAY_ENABLED, false);
    conf2.setBoolean(PHOENIX_REPLICATION_REPLAY_ENABLED, false);
    CLUSTERS.start();
  }

  @AfterClass
  public static synchronized void tearDownClass() throws Exception {
    CLUSTERS.close();
  }

  @Before
  public void setUp() throws Exception {
    zkUrl = getLocalZkUrl(CLUSTERS.getHBaseCluster1().getConfiguration());
    peerZkUrl = getLocalZkUrl(CLUSTERS.getHBaseCluster2().getConfiguration());
    masterUrl = CLUSTERS.getMasterAddress1();
    peerMasterUrl = CLUSTERS.getMasterAddress2();
    localAdmin = new PhoenixHAAdmin(CLUSTERS.getHBaseCluster1().getConfiguration(),
      ZK_CONSISTENT_HA_GROUP_RECORD_NAMESPACE);
    peerAdmin = new PhoenixHAAdmin(CLUSTERS.getHBaseCluster2().getConfiguration(),
      ZK_CONSISTENT_HA_GROUP_RECORD_NAMESPACE);
    deleteZNodes();
    HAGroupStoreTestUtil.deleteHAGroupRecordInSystemTable(group(), zkUrl);
    HAGroupStoreTestUtil.upsertHAGroupRecordInSystemTable(group(), zkUrl, peerZkUrl, masterUrl,
      peerMasterUrl, ClusterRoleRecord.ClusterRole.ACTIVE, ClusterRoleRecord.ClusterRole.STANDBY,
      null, CLUSTERS.getHdfsUrl1(), CLUSTERS.getHdfsUrl2());
  }

  @After
  public void tearDown() throws Exception {
    if (clientToClose != null) {
      clientToClose.close();
    }
    deleteZNodes();
    HAGroupStoreTestUtil.deleteHAGroupRecordInSystemTable(group(), zkUrl);
    localAdmin.close();
    peerAdmin.close();
  }

  @Test
  public void testInitialAndLocalStateMetrics() throws Exception {
    writeLocal(HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC);
    writePeer(HAGroupStoreRecord.HAGroupState.STANDBY);
    HAGroupStoreClient client = localClient();

    awaitMetrics(values -> values.getLocalCacheHealthStatus() == 0
      && values.getCurrentLocalState()
          == HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC.getMetricCode()
      && values.getCurrentPeerState() == HAGroupStoreRecord.HAGroupState.STANDBY.getMetricCode());

    assertEquals(0L,
      client.setHAGroupStatusIfNeeded(HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC_TO_STANDBY));
    awaitMetrics(values -> values.getCurrentLocalState()
        == HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC_TO_STANDBY.getMetricCode());

    assertEquals(0L, client.setHAGroupStatusIfNeeded(HAGroupStoreRecord.HAGroupState.STANDBY));
    awaitMetrics(values -> values.getCurrentLocalState()
        == HAGroupStoreRecord.HAGroupState.STANDBY.getMetricCode());
  }

  @Test
  public void testLocalZkHealthMetrics() throws Exception {
    writeLocal(HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC);
    HAGroupStoreClient client = localClient();
    awaitMetrics(values -> values.getLocalCacheHealthStatus() == 0);
    HAGroupStoreMetricValues before = metrics();
    int port = Integer.parseInt(
      CLUSTERS.getHBaseCluster1().getConfiguration().get("hbase.zookeeper.property.clientPort"));

    CLUSTERS.getHBaseCluster1().shutdownMiniZKCluster();
    try {
      awaitMetrics(values -> values.getLocalCacheHealthStatus() == 1
        && values.getLocalZkConnectionLostCount() == before.getLocalZkConnectionLostCount() + 1);
      assertThrows(IOException.class, client::getHAGroupStoreRecord);
    } finally {
      CLUSTERS.getHBaseCluster1().startMiniZKCluster(1, port);
    }
    awaitMetrics(values -> values.getLocalCacheHealthStatus() == 0);
  }

  @Test
  public void testLateInitializationFailureResetsGauges() throws Exception {
    writeLocal(HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC);
    Configuration invalidConf = new Configuration(CLUSTERS.getHBaseCluster1().getConfiguration());
    invalidConf.setLong(HA_GROUP_STORE_SYNC_INTERVAL_SECONDS, 0L);

    HAGroupStoreClient failed = new HAGroupStoreClient(invalidConf, null, group(), zkUrl);
    assertThrows(IOException.class, failed::getHAGroupStoreRecord);

    HAGroupStoreMetricValues values = metrics();
    assertEquals(1L, values.getLocalCacheHealthStatus());
    assertEquals(1L, values.getPeerVisibilityStatus());
    assertEquals(0L, values.getDegradedStandbyActive());
    assertEquals(HAGroupStoreRecord.HAGroupState.UNKNOWN.getMetricCode(),
      values.getCurrentLocalState());
    assertEquals(HAGroupStoreRecord.HAGroupState.UNKNOWN.getMetricCode(),
      values.getCurrentPeerState());
  }

  @Test
  public void testPeerVisibilityAndDegradedMetrics() throws Exception {
    writeLocal(HAGroupStoreRecord.HAGroupState.STANDBY);
    writePeer(HAGroupStoreRecord.HAGroupState.STANDBY);
    localClient();
    awaitMetrics(values -> values.getPeerVisibilityStatus() == 0
      && values.getCurrentPeerState() == HAGroupStoreRecord.HAGroupState.STANDBY.getMetricCode());
    HAGroupStoreMetricValues before = metrics();
    int port = Integer.parseInt(
      CLUSTERS.getHBaseCluster2().getConfiguration().get("hbase.zookeeper.property.clientPort"));

    CLUSTERS.getHBaseCluster2().shutdownMiniZKCluster();
    try {
      awaitMetrics(values -> values.getPeerVisibilityStatus() == 1
        && values.getCurrentPeerState() == HAGroupStoreRecord.HAGroupState.UNKNOWN.getMetricCode()
        && values.getDegradedStandbyActive() == 1
        && values.getPeerBlindCount() == before.getPeerBlindCount() + 1
        && values.getDegradedStandbyPresentedCount()
            == before.getDegradedStandbyPresentedCount() + 1);
    } finally {
      CLUSTERS.getHBaseCluster2().startMiniZKCluster(1, port);
    }
    awaitMetrics(values -> values.getPeerVisibilityStatus() == 0
      && values.getCurrentPeerState() == HAGroupStoreRecord.HAGroupState.STANDBY.getMetricCode()
      && values.getDegradedStandbyActive() == 0);
  }

  @Test
  public void testPeerReconfigurationMetrics() throws Exception {
    writeLocal(HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC);
    writePeer(HAGroupStoreRecord.HAGroupState.STANDBY);
    localClient();
    awaitMetrics(values -> values.getCurrentPeerState()
        == HAGroupStoreRecord.HAGroupState.STANDBY.getMetricCode());

    writeLocal(HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC, null);
    awaitMetrics(values -> values.getPeerVisibilityStatus() == 0
      && values.getCurrentPeerState() == HAGroupStoreRecord.HAGroupState.UNKNOWN.getMetricCode());

    writeLocal(HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC);
    awaitMetrics(values -> values.getCurrentPeerState()
        == HAGroupStoreRecord.HAGroupState.STANDBY.getMetricCode());

    writeLocal(HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC, "invalidURL");
    awaitMetrics(values -> values.getPeerVisibilityStatus() == 1
      && values.getCurrentPeerState() == HAGroupStoreRecord.HAGroupState.UNKNOWN.getMetricCode());

    writeLocal(HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC, null);
    awaitMetrics(values -> values.getPeerVisibilityStatus() == 0
      && values.getCurrentPeerState() == HAGroupStoreRecord.HAGroupState.UNKNOWN.getMetricCode());
  }

  @Test
  public void testInvalidTransitionMetric() throws Exception {
    writeLocal(HAGroupStoreRecord.HAGroupState.ACTIVE_NOT_IN_SYNC);
    HAGroupStoreClient client = localClient();
    HAGroupStoreMetricValues before = metrics();

    assertThrows(InvalidClusterRoleTransitionException.class,
      () -> client.setHAGroupStatusIfNeeded(HAGroupStoreRecord.HAGroupState.STANDBY));
    assertEquals(before.getInvalidTransitionRejectedCount() + 1,
      metrics().getInvalidTransitionRejectedCount());
  }

  @Test
  public void testSystemTableSyncFailureMetric() throws Exception {
    writeLocal(HAGroupStoreRecord.HAGroupState.STANDBY);
    HAGroupStoreClient client = localClient();
    awaitMetrics(values -> values.getCurrentLocalState()
        == HAGroupStoreRecord.HAGroupState.STANDBY.getMetricCode());
    client.shutdownSyncExecutor();
    HAGroupStoreMetricValues before = metrics();
    Admin admin = CLUSTERS.getHBaseCluster1().getAdmin();

    admin.disableTable(PhoenixDatabaseMetaData.SYSTEM_HA_GROUP_HBASE_TABLE_NAME);
    try {
      assertEquals(0L,
        client.setHAGroupStatusIfNeeded(HAGroupStoreRecord.HAGroupState.STANDBY_TO_ACTIVE));
      assertEquals(before.getSystemTableSyncFailedCount() + 1,
        metrics().getSystemTableSyncFailedCount());
      Pair<HAGroupStoreRecord, Stat> updated = localAdmin.getHAGroupStoreRecordInZooKeeper(group());
      assertEquals(HAGroupStoreRecord.HAGroupState.STANDBY_TO_ACTIVE,
        updated.getLeft().getHAGroupState());
    } finally {
      admin.enableTable(PhoenixDatabaseMetaData.SYSTEM_HA_GROUP_HBASE_TABLE_NAME);
    }
  }

  @Test
  public void testPeriodicSystemTableSyncFailureBeforeWriteMetric() throws Exception {
    writeLocal(HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC);
    HAGroupStoreClient client = localClient();
    awaitMetrics(values -> values.getLocalCacheHealthStatus() == 0);
    client.shutdownSyncExecutor();

    HAGroupStoreRecord invalidPolicyRecord =
      new HAGroupStoreRecord(HAGroupStoreRecord.DEFAULT_PROTOCOL_VERSION, group(),
        HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC, 0L, "INVALID_POLICY", peerZkUrl, masterUrl,
        peerMasterUrl, CLUSTERS.getHdfsUrl1(), CLUSTERS.getHdfsUrl2(), 0L);
    createOrUpdate(localAdmin, invalidPolicyRecord);
    long before = metrics().getSystemTableSyncFailedCount();

    client.syncZKToSystemTable();

    assertEquals(before + 1, metrics().getSystemTableSyncFailedCount());
  }

  @Test
  public void testListenerMetrics() throws Exception {
    writeLocal(HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC);
    HAGroupStoreClient client = localClient();
    client.subscribeToTargetState(HAGroupStoreRecord.HAGroupState.ACTIVE_NOT_IN_SYNC,
      ClusterType.LOCAL, (group, from, to, mtime, cluster, lastSync) -> {
        throw new IllegalStateException("expected test failure");
      });
    AtomicInteger successfulListeners = new AtomicInteger();
    client.subscribeToTargetState(HAGroupStoreRecord.HAGroupState.ACTIVE_NOT_IN_SYNC,
      ClusterType.LOCAL,
      (group, from, to, mtime, cluster, lastSync) -> successfulListeners.incrementAndGet());
    HAGroupStoreMetricValues before = metrics();

    writeLocal(HAGroupStoreRecord.HAGroupState.ACTIVE_NOT_IN_SYNC);
    awaitMetrics(values -> values.getNotificationListenerErrorCount()
        == before.getNotificationListenerErrorCount() + 1
      && values.getSubscriberNotifyTimeCount() == before.getSubscriberNotifyTimeCount() + 1
      && successfulListeners.get() == 1);
  }

  private String group() {
    return testName.getMethodName();
  }

  private HAGroupStoreClient localClient() {
    HAGroupStoreClient client = HAGroupStoreClient
      .getInstanceForZkUrl(CLUSTERS.getHBaseCluster1().getConfiguration(), group(), zkUrl);
    if (client != null) {
      clientToClose = client;
    }
    return client;
  }

  private HAGroupStoreMetricValues metrics() {
    return HAGroupStoreMetricsSourceFactory.getInstanceForHAGroup(group()).getCurrentMetricValues();
  }

  private void awaitMetrics(Predicate<HAGroupStoreMetricValues> condition) throws Exception {
    long deadline = System.currentTimeMillis() + EVENT_TIMEOUT_MS;
    while (!condition.test(metrics()) && System.currentTimeMillis() < deadline) {
      Thread.sleep(100L);
    }
    assertTrue("Metric condition not met for " + group(), condition.test(metrics()));
  }

  private HAGroupStoreRecord localRecord(HAGroupStoreRecord.HAGroupState state, String peerUrl) {
    return new HAGroupStoreRecord(HAGroupStoreRecord.DEFAULT_PROTOCOL_VERSION, group(), state, 0L,
      HighAvailabilityPolicy.FAILOVER.toString(), peerUrl, masterUrl, peerMasterUrl,
      CLUSTERS.getHdfsUrl1(), CLUSTERS.getHdfsUrl2(), 0L);
  }

  private HAGroupStoreRecord peerRecord(HAGroupStoreRecord.HAGroupState state) {
    return new HAGroupStoreRecord(HAGroupStoreRecord.DEFAULT_PROTOCOL_VERSION, group(), state, 0L,
      HighAvailabilityPolicy.FAILOVER.toString(), zkUrl, peerMasterUrl, masterUrl,
      CLUSTERS.getHdfsUrl2(), CLUSTERS.getHdfsUrl1(), 0L);
  }

  private void writeLocal(HAGroupStoreRecord.HAGroupState state) throws Exception {
    writeLocal(state, peerZkUrl);
  }

  private void writeLocal(HAGroupStoreRecord.HAGroupState state, String peerUrl) throws Exception {
    createOrUpdate(localAdmin, localRecord(state, peerUrl));
  }

  private void writePeer(HAGroupStoreRecord.HAGroupState state) throws Exception {
    createOrUpdate(peerAdmin, peerRecord(state));
  }

  private void createOrUpdate(PhoenixHAAdmin admin, HAGroupStoreRecord record) throws Exception {
    String path = toPath(group());
    if (admin.getCurator().checkExists().forPath(path) == null) {
      admin.createHAGroupStoreRecordInZooKeeper(record);
      return;
    }
    Pair<HAGroupStoreRecord, Stat> current = admin.getHAGroupStoreRecordInZooKeeper(group());
    admin.updateHAGroupStoreRecordInZooKeeper(group(), record, current.getRight().getVersion());
  }

  private void deleteZNodes() throws Exception {
    localAdmin.getCurator().delete().quietly().forPath(toPath(group()));
    peerAdmin.getCurator().delete().quietly().forPath(toPath(group()));
  }
}
