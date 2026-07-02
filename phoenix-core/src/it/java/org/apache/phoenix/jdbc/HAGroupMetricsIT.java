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

import static org.apache.phoenix.jdbc.HighAvailabilityGroup.PHOENIX_HA_GROUP_ATTR;
import static org.apache.phoenix.jdbc.HighAvailabilityTestingUtility.getHighAvailibilityGroup;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_HA_CRR_CACHE_AGE_MS;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_HA_CRR_REFRESH_COUNT;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_HA_FAILOVER_COUNT;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_HA_FAILOVER_DURATION_MS;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_HA_MUTATION_BLOCKED_COUNT;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_HA_POLLER_TICK_COUNT;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_HA_POLLER_TICK_FAILURES;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_HA_STALE_CRR_DETECTED_COUNT;
import static org.apache.phoenix.query.QueryServices.CLUSTER_ROLE_BASED_MUTATION_BLOCK_ENABLED;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.exception.MutationBlockedIOException;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.execute.CommitException;
import org.apache.phoenix.jdbc.ClusterRoleRecord.ClusterRole;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration test for the client-side HA observability metrics. Each test resets the relevant
 * {@link org.apache.phoenix.monitoring.GlobalClientMetrics} entries before exercising the code path
 * that should emit them, then asserts the post-condition counter/gauge value.
 */
@Category(NeedsOwnMiniClusterTest.class)
public class HAGroupMetricsIT extends HABaseIT {
  private static final Logger LOG = LoggerFactory.getLogger(HAGroupMetricsIT.class);

  @Rule
  public final TestName testName = new TestName();

  private Properties clientProperties;
  private HighAvailabilityGroup haGroup;
  private String tableName;
  private String haGroupName;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    CLUSTERS.getHBaseCluster1().getConfiguration()
      .setBoolean(CLUSTER_ROLE_BASED_MUTATION_BLOCK_ENABLED, true);
    CLUSTERS.getHBaseCluster2().getConfiguration()
      .setBoolean(CLUSTER_ROLE_BASED_MUTATION_BLOCK_ENABLED, true);
    CLUSTERS.start();
    DriverManager.registerDriver(PhoenixDriver.INSTANCE);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    DriverManager.deregisterDriver(PhoenixDriver.INSTANCE);
    CLUSTERS.close();
  }

  @Before
  public void setUp() throws Exception {
    haGroupName = testName.getMethodName();
    clientProperties = HighAvailabilityTestingUtility.getHATestProperties();
    clientProperties.setProperty(PHOENIX_HA_GROUP_ATTR, haGroupName);
    CLUSTERS.initClusterRole(haGroupName, HighAvailabilityPolicy.FAILOVER);
    haGroup = getHighAvailibilityGroup(CLUSTERS.getJdbcHAUrl(), clientProperties);
    tableName = testName.getMethodName().toUpperCase();
    CLUSTERS.createTableOnClusterPair(haGroup, tableName);
    resetAllHaMetrics();
  }

  @After
  public void tearDown() throws Exception {
    try {
      haGroup.close();
    } catch (Exception e) {
      LOG.error("Fail to tear down HA group; ignoring", e);
    }
  }

  @Test(timeout = 300000)
  public void testFailoverCountAndDuration() throws Exception {
    long countBefore = GLOBAL_HA_FAILOVER_COUNT.getMetric().getValue();
    long durationBefore = GLOBAL_HA_FAILOVER_DURATION_MS.getMetric().getValue();

    try (Connection conn = DriverManager.getConnection(CLUSTERS.getJdbcHAUrl(), clientProperties)) {
      conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES (1, 1)");
      conn.commit();
      CLUSTERS.transitClusterRole(haGroup, ClusterRole.STANDBY, ClusterRole.ACTIVE);
    }

    long countAfter = GLOBAL_HA_FAILOVER_COUNT.getMetric().getValue();
    long durationAfter = GLOBAL_HA_FAILOVER_DURATION_MS.getMetric().getValue();
    assertTrue("HA_FAILOVER_COUNT should increment on cluster role transition",
      countAfter > countBefore);
    assertTrue("HA_FAILOVER_DURATION_MS sum should grow on transition",
      durationAfter >= durationBefore);
  }

  @Test(timeout = 300000)
  public void testStaleCrrDetectedCount() throws Exception {
    long before = GLOBAL_HA_STALE_CRR_DETECTED_COUNT.getMetric().getValue();
    // Drive a cluster-role transition that the wrapped connection observes as
    // stale-CRR — failover() flags STALE_CLUSTER_ROLE_RECORD and increments the counter.
    try (Connection conn = DriverManager.getConnection(CLUSTERS.getJdbcHAUrl(), clientProperties)) {
      conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES (2, 2)");
      conn.commit();
      CLUSTERS.transitClusterRole(haGroup, ClusterRole.ACTIVE_TO_STANDBY, ClusterRole.STANDBY);
      // doRefreshHAGroup=false: keep haGroup's CRR snapshot stale on purpose so that
      // the next mutation drives StaleClusterRoleRecordException through wrapActionDuringFailover,
      // exercising the GLOBAL_HA_STALE_CRR_DETECTED_COUNT increment path.
      CLUSTERS.transitClusterRole(haGroup, ClusterRole.STANDBY, ClusterRole.ACTIVE, false);
      // Issue another mutation while the connection still holds the pre-transition CRR snapshot.
      // The server's HAGroupStoreManager will detect the version mismatch and throw
      // StaleClusterRoleRecordException, which wrapActionDuringFailover catches to increment the
      // counter (and rethrow FAILOVER_IN_PROGRESS).
      try {
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES (3, 3)");
        conn.commit();
      } catch (SQLException expected) {
        // Stale-CRR surfaces here as a SQLException with FAILOVER_IN_PROGRESS (1990 / F1Q90):
        // wrapActionDuringFailover catches StaleClusterRoleRecordException, increments
        // GLOBAL_HA_STALE_CRR_DETECTED_COUNT, then rethrows a freshly-built SQLException via
        // SQLExceptionInfo.Builder(FAILOVER_IN_PROGRESS).build().buildException(). The error
        // code is the contracted surface — any other SQLException must fail this assertion
        // rather than be silently swallowed (the prior catch(Exception) hid setup bugs by
        // treating ANY failure as the expected path).
        assertTrue(
          "Expected FAILOVER_IN_PROGRESS error code, got " + expected.getErrorCode() + " ("
            + expected.getSQLState() + ")",
          expected.getErrorCode() == SQLExceptionCode.FAILOVER_IN_PROGRESS.getErrorCode());
        LOG.info(
          "Expected FAILOVER_IN_PROGRESS SQLException surfaced from stale-CRR mutation " + "in {}",
          testName.getMethodName(), expected);
      }
    }
    long after = GLOBAL_HA_STALE_CRR_DETECTED_COUNT.getMetric().getValue();
    assertTrue("HA_STALE_CRR_DETECTED_COUNT should strictly increment when CRR is detected stale "
      + "(before=" + before + ", after=" + after + ")", after > before);
  }

  @Test(timeout = 300000)
  public void testMutationBlockedCount() throws Exception {
    long before = GLOBAL_HA_MUTATION_BLOCKED_COUNT.getMetric().getValue();
    String peerZkUrl = CLUSTERS.getZkUrl2();
    PhoenixHAAdmin haAdmin = CLUSTERS.getHaAdmin1();

    try (FailoverPhoenixConnection conn = (FailoverPhoenixConnection) DriverManager
      .getConnection(CLUSTERS.getJdbcHAUrl(), clientProperties)) {
      conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES (3, 3)");
      conn.commit();

      HAGroupStoreRecord blocking =
        new HAGroupStoreRecord(HAGroupStoreRecord.DEFAULT_PROTOCOL_VERSION, haGroupName,
          HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC_TO_STANDBY, 0L,
          HighAvailabilityPolicy.FAILOVER.toString(), peerZkUrl, CLUSTERS.getMasterAddress1(),
          CLUSTERS.getMasterAddress2(), CLUSTERS.getHdfsUrl1(), CLUSTERS.getHdfsUrl2(), 0L);
      haAdmin.updateHAGroupStoreRecordInZooKeeper(haGroupName, blocking, -1);
      Thread.sleep(1000L);

      try {
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES (4, 4)");
        conn.commit();
        fail("Expected MutationBlockedIOException during ACTIVE_TO_STANDBY");
      } catch (CommitException e) {
        Throwable cause = e.getCause();
        assertTrue("Expected MutationBlockedIOException in chain",
          cause instanceof RetriesExhaustedWithDetailsException
            && ((RetriesExhaustedWithDetailsException) cause)
              .getCause(0) instanceof MutationBlockedIOException);
      }
    }
    long after = GLOBAL_HA_MUTATION_BLOCKED_COUNT.getMetric().getValue();
    assertTrue("HA_MUTATION_BLOCKED_COUNT should increment when MBE is observed", after > before);
  }

  @Test(timeout = 300000)
  public void testCrrRefreshCount() throws Exception {
    long before = GLOBAL_HA_CRR_REFRESH_COUNT.getMetric().getValue();
    // Force-refresh both calls: HA_CRR_REFRESH_COUNT only increments on a refresh that
    // actually fetched a CRR from the endpoint — a non-force refresh inside the cache window
    // short-circuits before getClusterRoleRecordFromEndpoint() and intentionally does NOT
    // bump the counter (counter measures "fresh fetches", not "refresh calls").
    haGroup.refreshClusterRoleRecord(true);
    haGroup.refreshClusterRoleRecord(true);
    long after = GLOBAL_HA_CRR_REFRESH_COUNT.getMetric().getValue();
    assertTrue("HA_CRR_REFRESH_COUNT should increment on each force-refresh", after - before >= 2);
  }

  @Test(timeout = 300000)
  public void testCrrCacheAgeMs() throws Exception {
    // Pre-refresh assertion: a connect() against a freshly init()-ed HA group must sample a
    // bounded age; if init() forgets to seed lastClusterRoleRecordRefreshTime the gauge will
    // record currentTimeMillis() - 0 (~1.7e12 ms) and this assertion will fail.
    try (Connection conn = DriverManager.getConnection(CLUSTERS.getJdbcHAUrl(), clientProperties)) {
      conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES (4, 4)");
      conn.commit();
    }
    long ageBeforeRefresh = GLOBAL_HA_CRR_CACHE_AGE_MS.getMetric().getValue();
    assertTrue("HA_CRR_CACHE_AGE_MS gauge must be bounded on init path (was " + ageBeforeRefresh
      + " ms, expected < 60_000)", ageBeforeRefresh < 60_000L);

    // Force a refresh so lastClusterRoleRecordRefreshTime is recent, then sleep so the
    // gauge sample (taken on connect()) records a non-trivial age.
    haGroup.refreshClusterRoleRecord(false);
    Thread.sleep(50L);
    try (Connection conn = DriverManager.getConnection(CLUSTERS.getJdbcHAUrl(), clientProperties)) {
      conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES (5, 5)");
      conn.commit();
    }
    long ageMs = GLOBAL_HA_CRR_CACHE_AGE_MS.getMetric().getValue();
    // Gauge holds the most-recent set() sample; should be > 0 and within a sane bound.
    assertTrue("HA_CRR_CACHE_AGE_MS gauge should be > 0 after a connect()", ageMs > 0L);
    assertTrue("HA_CRR_CACHE_AGE_MS gauge should be < 5 minutes for a fresh HA group",
      ageMs < 5 * 60 * 1000L);
  }

  @Test(timeout = 300000)
  public void testPollerTickCount() throws Exception {
    // The poller starts only when fetchClusterRoleRecord observes both roles non-active under
    // FAILOVER policy. Drive that state, await a couple of ticks, and verify the counter moved.
    long beforeTicks = GLOBAL_HA_POLLER_TICK_COUNT.getMetric().getValue();
    long beforeFailures = GLOBAL_HA_POLLER_TICK_FAILURES.getMetric().getValue();

    CLUSTERS.transitClusterRole(haGroup, ClusterRole.ACTIVE_TO_STANDBY, ClusterRole.STANDBY);
    CLUSTERS.transitClusterRole(haGroup, ClusterRole.STANDBY, ClusterRole.STANDBY);

    // Allow at least 2 poller ticks at default interval to land.
    Thread.sleep(15_000L);

    long afterTicks = GLOBAL_HA_POLLER_TICK_COUNT.getMetric().getValue();
    long afterFailures = GLOBAL_HA_POLLER_TICK_FAILURES.getMetric().getValue();
    assertTrue("HA_POLLER_TICK_COUNT should advance once poller is scheduled",
      afterTicks > beforeTicks);
    // Failures may or may not occur in mini-cluster; just assert non-decreasing.
    assertTrue("HA_POLLER_TICK_FAILURES should be monotonic", afterFailures >= beforeFailures);
    // Verify the poller-tick gauge sample site fires: by the time we've slept 15s and seen
    // multiple ticks, init() has long-since seeded lastClusterRoleRecordRefreshTime, so the
    // gauge holds a real age sample. >= 0 guards against the -1L "never-refreshed" sentinel
    // leaking out via a buggy refactor — if the poller-tick .set() path is broken or the
    // sentinel escapes the init/poller race window, this assertion catches it.
    long ageGauge = GLOBAL_HA_CRR_CACHE_AGE_MS.getMetric().getValue();
    assertTrue("HA_CRR_CACHE_AGE_MS gauge should hold a non-negative age sample after poller ticks "
      + "(got " + ageGauge + ")", ageGauge >= 0L);
  }

  private void resetAllHaMetrics() {
    GLOBAL_HA_FAILOVER_COUNT.getMetric().reset();
    GLOBAL_HA_FAILOVER_DURATION_MS.getMetric().reset();
    GLOBAL_HA_MUTATION_BLOCKED_COUNT.getMetric().reset();
    GLOBAL_HA_STALE_CRR_DETECTED_COUNT.getMetric().reset();
    GLOBAL_HA_CRR_REFRESH_COUNT.getMetric().reset();
    GLOBAL_HA_CRR_CACHE_AGE_MS.getMetric().reset();
    GLOBAL_HA_POLLER_TICK_COUNT.getMetric().reset();
    GLOBAL_HA_POLLER_TICK_FAILURES.getMetric().reset();
  }
}
