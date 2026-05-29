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
package org.apache.phoenix.end2end.index;

import static org.apache.phoenix.jdbc.HighAvailabilityGroup.PHOENIX_HA_GROUP_ATTR;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_HA_GROUP_NAME;
import static org.apache.phoenix.query.BaseTest.generateUniqueName;
import static org.apache.phoenix.query.QueryServices.CLUSTER_ROLE_BASED_MUTATION_BLOCK_ENABLED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.exception.MutationBlockedIOException;
import org.apache.phoenix.execute.CommitException;
import org.apache.phoenix.jdbc.FailoverPhoenixConnection;
import org.apache.phoenix.jdbc.HABaseIT;
import org.apache.phoenix.jdbc.HAGroupStoreRecord;
import org.apache.phoenix.jdbc.HighAvailabilityPolicy;
import org.apache.phoenix.jdbc.HighAvailabilityTestingUtility;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.jdbc.PhoenixHAAdmin;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

/**
 * Integration test for mutation blocking functionality in IndexRegionObserver.preBatchMutate()
 * Tests that MutationBlockedIOException is properly thrown when cluster role-based mutation
 * blocking is enabled and CRRs are in ACTIVE_TO_STANDBY state.
 */
@Category(NeedsOwnMiniClusterTest.class)
public class IndexRegionObserverMutationBlockingIT extends HABaseIT {

  private static final Long ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS = 1000L;
  private PhoenixHAAdmin haAdmin;

  private String peerZkUrl;

  @Rule
  public TestName testName = new TestName();
  private Properties clientProps = new Properties();
  private String haGroupName;

  @BeforeClass
  public static synchronized void doSetup() throws Exception {
    CLUSTERS.getHBaseCluster1().getConfiguration()
      .setBoolean(CLUSTER_ROLE_BASED_MUTATION_BLOCK_ENABLED, true);
    CLUSTERS.getHBaseCluster2().getConfiguration()
      .setBoolean(CLUSTER_ROLE_BASED_MUTATION_BLOCK_ENABLED, true);
    CLUSTERS.start();
    DriverManager.registerDriver(PhoenixDriver.INSTANCE);
  }

  @Before
  public void setUp() throws Exception {
    haGroupName = testName.getMethodName();
    clientProps = HighAvailabilityTestingUtility.getHATestProperties();
    clientProps.setProperty(PHOENIX_HA_GROUP_ATTR, haGroupName);
    haAdmin = CLUSTERS.getHaAdmin1();
    Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);
    CLUSTERS.initClusterRole(haGroupName, HighAvailabilityPolicy.FAILOVER);
    this.peerZkUrl = CLUSTERS.getZkUrl2();
  }

  @Test
  public void testMutationBlockedOnDataTableWithIndex() throws Exception {
    String dataTableName = generateUniqueName();
    String indexName = generateUniqueName();

    try (FailoverPhoenixConnection conn = (FailoverPhoenixConnection) DriverManager
      .getConnection(CLUSTERS.getJdbcHAUrl(), clientProps)) {
      // Create data table and index
      conn.createStatement().execute(
        "CREATE TABLE " + dataTableName + " (id VARCHAR PRIMARY KEY, name VARCHAR, age INTEGER)");
      conn.createStatement()
        .execute("CREATE INDEX " + indexName + " ON " + dataTableName + "(name)");

      // Initially, mutations should work - verify baseline
      conn.createStatement().execute("UPSERT INTO " + dataTableName + " VALUES ('1', 'John', 25)");
      conn.commit();

      // Set up HAGroupStoreRecord that will block mutations (ACTIVE_TO_STANDBY state)
      HAGroupStoreRecord haGroupStoreRecord =
        new HAGroupStoreRecord(HAGroupStoreRecord.DEFAULT_PROTOCOL_VERSION, haGroupName,
          HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC_TO_STANDBY, 0L,
          HighAvailabilityPolicy.FAILOVER.toString(), this.peerZkUrl, CLUSTERS.getMasterAddress1(),
          CLUSTERS.getMasterAddress2(), CLUSTERS.getHdfsUrl1(), CLUSTERS.getHdfsUrl2(), 0L);
      haAdmin.updateHAGroupStoreRecordInZooKeeper(haGroupName, haGroupStoreRecord, -1);

      // Wait for the event to propagate
      Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

      // Test that UPSERT throws MutationBlockedIOException
      try {
        conn.createStatement()
          .execute("UPSERT INTO " + dataTableName + " VALUES ('2', 'Jane', 30)");
        conn.commit();
        fail("Expected MutationBlockedIOException to be thrown");
      } catch (CommitException e) {
        // Verify the exception chain contains MutationBlockedIOException
        assertTrue("Expected MutationBlockedIOException in exception chain",
          containsMutationBlockedException(e));
      }
    }
  }

  @Test
  public void testMutationAllowedWhenNotBlocked() throws Exception {
    String dataTableName = generateUniqueName();
    String indexName = generateUniqueName();

    try (FailoverPhoenixConnection conn = (FailoverPhoenixConnection) DriverManager
      .getConnection(CLUSTERS.getJdbcHAUrl(), clientProps)) {
      // Create data table and index
      conn.createStatement().execute(
        "CREATE TABLE " + dataTableName + " (id VARCHAR PRIMARY KEY, name VARCHAR, age INTEGER)");
      conn.createStatement()
        .execute("CREATE INDEX " + indexName + " ON " + dataTableName + "(name)");

      // Mutations should work fine
      conn.createStatement().execute("UPSERT INTO " + dataTableName + " VALUES ('1', 'Bob', 35)");
      conn.createStatement().execute("UPSERT INTO " + dataTableName + " VALUES ('2', 'Carol', 27)");
      conn.commit();

      // Verify data was inserted successfully
      try (java.sql.ResultSet rs =
        conn.createStatement().executeQuery("SELECT COUNT(*) FROM " + dataTableName)) {
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
      }
    }
  }

  @Test
  public void testMutationBlockingTransition() throws Exception {
    String dataTableName = generateUniqueName();
    String indexName = generateUniqueName();

    try (FailoverPhoenixConnection conn = (FailoverPhoenixConnection) DriverManager
      .getConnection(CLUSTERS.getJdbcHAUrl(), clientProps)) {
      // Create data table and index
      conn.createStatement().execute(
        "CREATE TABLE " + dataTableName + " (id VARCHAR PRIMARY KEY, name VARCHAR, age INTEGER)");
      conn.createStatement()
        .execute("CREATE INDEX " + indexName + " ON " + dataTableName + "(name)");

      // Mutation should work
      conn.createStatement().execute("UPSERT INTO " + dataTableName + " VALUES ('1', 'David', 40)");
      conn.commit();
      // Set up HAGroupStoreRecord that will block mutations (ACTIVE_TO_STANDBY state)
      HAGroupStoreRecord haGroupStoreRecord =
        new HAGroupStoreRecord(HAGroupStoreRecord.DEFAULT_PROTOCOL_VERSION, haGroupName,
          HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC_TO_STANDBY, 0L,
          HighAvailabilityPolicy.FAILOVER.toString(), this.peerZkUrl, CLUSTERS.getMasterAddress1(),
          CLUSTERS.getMasterAddress2(), CLUSTERS.getHdfsUrl1(), CLUSTERS.getHdfsUrl2(), 0L);
      haAdmin.updateHAGroupStoreRecordInZooKeeper(haGroupName, haGroupStoreRecord, -1);
      Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

      // Now mutations should be blocked
      try {
        conn.createStatement().execute("UPSERT INTO " + dataTableName + " VALUES ('2', 'Eve', 33)");
        conn.commit();
        fail("Expected MutationBlockedIOException after transition to ACTIVE_TO_STANDBY");
      } catch (CommitException e) {
        assertTrue("Expected mutation blocked exception after transition",
          containsMutationBlockedException(e));
      }

      // Transition back to ACTIVE (non-blocking state) and peer cluster is in ATS state
      // Set up HAGroupStoreRecord that will block mutations (ACTIVE_TO_STANDBY state)
      haGroupStoreRecord = new HAGroupStoreRecord(HAGroupStoreRecord.DEFAULT_PROTOCOL_VERSION,
        haGroupName, HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC, 0L,
        HighAvailabilityPolicy.FAILOVER.toString(), this.peerZkUrl, CLUSTERS.getMasterAddress1(),
        CLUSTERS.getMasterAddress2(), CLUSTERS.getHdfsUrl1(), CLUSTERS.getHdfsUrl2(), 0L);
      haAdmin.updateHAGroupStoreRecordInZooKeeper(haGroupName, haGroupStoreRecord, -1);
      Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

      // Mutations should work again
      conn.createStatement().execute("UPSERT INTO " + dataTableName + " VALUES ('3', 'Frank', 45)");
      conn.commit();
    }
  }

  @Test
  public void testSystemHAGroupTableMutationsAllowedDuringActiveToStandby() throws Exception {
    String dataTableName = generateUniqueName();
    String indexName = generateUniqueName();
    String haGroupName = testName.getMethodName();

    try (FailoverPhoenixConnection conn = (FailoverPhoenixConnection) DriverManager
      .getConnection(CLUSTERS.getJdbcHAUrl(), clientProps)) {

      // Create data table and index for testing regular table blocking
      conn.createStatement().execute(
        "CREATE TABLE " + dataTableName + " (id VARCHAR PRIMARY KEY, name VARCHAR, age INTEGER)");
      conn.createStatement()
        .execute("CREATE INDEX " + indexName + " ON " + dataTableName + "(name)");

      // Initially, mutations should work on both tables - verify baseline
      conn.createStatement().execute("UPSERT INTO " + dataTableName + " VALUES ('1', 'John', 25)");

      // Update system HA group table (should always work)
      conn.createStatement().execute("UPSERT INTO " + SYSTEM_HA_GROUP_NAME
        + " (HA_GROUP_NAME, POLICY, VERSION) VALUES ('" + haGroupName + "_test', 'FAILOVER', 1)");
      conn.commit();

      // Set up HAGroupStoreRecord in ACTIVE_TO_STANDBY state (should block regular tables)
      HAGroupStoreRecord haGroupStoreRecord =
        new HAGroupStoreRecord(HAGroupStoreRecord.DEFAULT_PROTOCOL_VERSION, haGroupName,
          HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC_TO_STANDBY, 0L,
          HighAvailabilityPolicy.FAILOVER.toString(), this.peerZkUrl, CLUSTERS.getMasterAddress1(),
          CLUSTERS.getMasterAddress2(), CLUSTERS.getHdfsUrl1(), CLUSTERS.getHdfsUrl2(), 0L);
      haAdmin.updateHAGroupStoreRecordInZooKeeper(haGroupName, haGroupStoreRecord, -1);

      // Wait for the event to propagate
      Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS);

      // Test 1: Regular data table mutations should be BLOCKED
      try {
        conn.createStatement()
          .execute("UPSERT INTO " + dataTableName + " VALUES ('2', 'Jane', 30)");
        conn.commit();
        fail("Expected MutationBlockedIOException for regular table during ACTIVE_TO_STANDBY");
      } catch (CommitException e) {
        assertTrue("Expected MutationBlockedIOException for regular table",
          containsMutationBlockedException(e));
      }

      // Test 2: System HA Group table mutations should be ALLOWED
      try {
        conn.createStatement().execute(
          "UPSERT INTO " + SYSTEM_HA_GROUP_NAME + " (HA_GROUP_NAME, POLICY, VERSION) VALUES ('"
            + haGroupName + "_test2', 'FAILOVER', 2)");
        conn.commit();

        // Verify the system table mutation succeeded
        try (Statement stmt = conn.createStatement();
          ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + SYSTEM_HA_GROUP_NAME
            + " WHERE HA_GROUP_NAME LIKE '" + haGroupName + "_test%'")) {
          assertTrue("Should have results", rs.next());
          assertEquals("Should have 2 test records in system table", 2, rs.getInt(1));
        }

      } catch (Exception e) {
        fail("System HA Group table mutations should NOT be blocked during ACTIVE_TO_STANDBY: "
          + e.getMessage());
      }

      // Clean up test records from system table
      conn.createStatement().execute("DELETE FROM " + SYSTEM_HA_GROUP_NAME
        + " WHERE HA_GROUP_NAME LIKE '" + haGroupName + "_test%'");
      conn.commit();
    }
  }

  /**
   * Walks the cause chain of a {@link CommitException} looking for a
   * {@link MutationBlockedIOException}. Handles two paths:
   * <ul>
   *   <li>Direct chain — {@code MutationBlockedIOException} now extends
   *       {@link org.apache.hadoop.hbase.DoNotRetryIOException}, so HBase's RPC retry layers
   *       fail-fast and propagate the exception without wrapping it in
   *       {@link RetriesExhaustedWithDetailsException}.</li>
   *   <li>REWDE-wrapped — the legacy retry path; preserved here for any caller that still
   *       reaches it (defense-in-depth).</li>
   * </ul>
   */
  private boolean containsMutationBlockedException(CommitException e) {
    Throwable t = e.getCause();
    while (t != null) {
      if (t instanceof MutationBlockedIOException) {
        return true;
      }
      if (t instanceof RetriesExhaustedWithDetailsException) {
        RetriesExhaustedWithDetailsException re = (RetriesExhaustedWithDetailsException) t;
        for (int i = 0; i < re.getNumExceptions(); i++) {
          if (re.getCause(i) instanceof MutationBlockedIOException) {
            return true;
          }
        }
      }
      t = t.getCause();
    }
    return false;
  }

  /**
   * Regression test for the fail-fast fix on the batched-mutation path. The fix is the inheritance
   * change on {@link MutationBlockedIOException} — it now extends
   * {@code DoNotRetryIOException}, signaling intent to fail fast.
   *
   * <p>Empirical verification: server-side rehydration (via
   * {@code ProtobufUtil.toException}) delivers a real {@code MutationBlockedIOException} instance
   * to HBase's batched-RPC retry layer ({@code AsyncRequestFutureImpl.manageError}); the
   * {@code instanceof DoNotRetryIOException} check at line 749 returns true post-inheritance, so
   * no retries fire and the failure surfaces immediately. The inheritance change alone is
   * sufficient for the batched path.
   *
   * <p>Test asserts:
   * <ol>
   *   <li>An {@link MutationBlockedIOException} appears somewhere on the {@link CommitException}
   *       cause chain (proves the server gate fired).</li>
   *   <li>Wall-clock duration of the failed commit is well below the pre-fix HBase retry-budget
   *       tail (which was on the order of tens of seconds for default 16 retries × per-retry
   *       timeout). A bound of {@code MAX_FAIL_FAST_DURATION_MS} captures this with generous
   *       headroom for slow CI; on a healthy mini-cluster the actual duration is
   *       sub-second.</li>
   * </ol>
   */
  @Test
  public void testMutationBlockedFailsFastWithDNRIOE() throws Exception {
    String dataTableName = generateUniqueName();
    String indexName = generateUniqueName();

    try (FailoverPhoenixConnection conn = (FailoverPhoenixConnection) DriverManager
      .getConnection(CLUSTERS.getJdbcHAUrl(), clientProps)) {
      conn.createStatement().execute(
        "CREATE TABLE " + dataTableName + " (id VARCHAR PRIMARY KEY, name VARCHAR, age INTEGER)");
      conn.createStatement()
        .execute("CREATE INDEX " + indexName + " ON " + dataTableName + "(name)");

      // Set up HAGroupStoreRecord that will block mutations (ACTIVE_TO_STANDBY state)
      HAGroupStoreRecord haGroupStoreRecord =
        new HAGroupStoreRecord(HAGroupStoreRecord.DEFAULT_PROTOCOL_VERSION, haGroupName,
          HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC_TO_STANDBY, 0L,
          HighAvailabilityPolicy.FAILOVER.toString(), this.peerZkUrl, CLUSTERS.getMasterAddress1(),
          CLUSTERS.getMasterAddress2(), CLUSTERS.getHdfsUrl1(), CLUSTERS.getHdfsUrl2(), 0L);
      haAdmin.updateHAGroupStoreRecordInZooKeeper(haGroupName, haGroupStoreRecord, -1);
      awaitZkPropagation(haGroupName,
        HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC_TO_STANDBY);

      long startMs = System.currentTimeMillis();
      try {
        conn.createStatement()
          .execute("UPSERT INTO " + dataTableName + " VALUES ('1', 'Alice', 28)");
        conn.commit();
        fail("Expected MutationBlockedIOException to be thrown");
      } catch (CommitException e) {
        long durationMs = System.currentTimeMillis() - startMs;
        assertTrue(
          "Expected MutationBlockedIOException somewhere on the cause chain (helper walks both "
            + "direct and REWDE-wrapped layouts).",
          containsMutationBlockedException(e));
        assertTrue("Expected fail-fast: failed commit took " + durationMs
          + "ms which exceeds the bound of " + MAX_FAIL_FAST_DURATION_MS
          + "ms. Pre-fix this took on the order of tens of seconds while HBase exhausted its "
          + "retry budget against the now-blocking server. If this assertion fails, HBase has "
          + "started retrying despite DoNotRetryIOException — investigate whether the inheritance"
          + " change on MutationBlockedIOException is still in place.",
          durationMs < MAX_FAIL_FAST_DURATION_MS);
      }
    }
  }

  /**
   * Wall-clock bound for fail-fast on the batched mutation path. Pre-fix, HBase's default
   * {@code hbase.client.retries.number=16} multiplied by per-retry RPC timeouts produced tails
   * on the order of tens of seconds. Post-fix (server-side DNRIOE inheritance), the first hit
   * propagates immediately. The 10-second bound gives generous headroom for slow CI hardware
   * while staying well below any plausible retry tail.
   */
  private static final long MAX_FAIL_FAST_DURATION_MS = 10_000L;

  /**
   * Empirical proof that the DNRIOE inheritance change alone — with no Phoenix-side client-code
   * changes — is sufficient to deliver fail-fast on the batched mutation path under elevated
   * retry settings.
   *
   * <p>Server-side rehydration via {@code ProtobufUtil.toException} delivers a real
   * {@link MutationBlockedIOException} instance to HBase's batched-RPC retry layer
   * ({@code AsyncRequestFutureImpl.manageError}); the {@code instanceof DoNotRetryIOException}
   * check at line 749 returns true post-inheritance, so no retries fire and the failure surfaces
   * immediately. This test raises {@code hbase.client.retries.number} and
   * {@code hbase.client.pause} to values that would — absent the inheritance — force HBase's
   * batched-RPC retry layer to consume many seconds before surfacing the failure (16 retries ×
   * ~100ms base pause with exponential backoff yields multi-second tails). The wall-clock bound
   * stays under {@link #MAX_FAIL_FAST_DURATION_MS} only because the inheritance change is in
   * place; the assertion-passing IS the empirical proof of inheritance-alone fail-fast.
   *
   * <p>Note: even with elevated retry settings, the mini-cluster batched path may still surface
   * only a single attempt before propagation due to harness internals not faithfully modeling
   * the production-perf retry tail observed on real clusters. In that case, this test functions
   * as additional regression coverage rather than a true behavioral discriminator; real-cluster
   * perf testing remains the authoritative validation of the wall-clock benefit.
   */
  @Test
  public void testMutationBlockedFailsFastUnderElevatedRetries() throws Exception {
    String dataTableName = generateUniqueName();
    String indexName = generateUniqueName();

    Properties retryProps = new Properties();
    retryProps.putAll(clientProps);
    // Elevate HBase client retry budget so absent-inheritance failure tail would scale into the
    // tens of seconds: 16 retries × ~100ms base pause (with exponential backoff in HBase) yields
    // multi-second tails before the per-attempt pause caps out.
    retryProps.setProperty("hbase.client.retries.number", "16");
    retryProps.setProperty("hbase.client.pause", "100");

    try (FailoverPhoenixConnection conn = (FailoverPhoenixConnection) DriverManager
      .getConnection(CLUSTERS.getJdbcHAUrl(), retryProps)) {
      conn.createStatement().execute(
        "CREATE TABLE " + dataTableName + " (id VARCHAR PRIMARY KEY, name VARCHAR, age INTEGER)");
      conn.createStatement()
        .execute("CREATE INDEX " + indexName + " ON " + dataTableName + "(name)");

      HAGroupStoreRecord haGroupStoreRecord =
        new HAGroupStoreRecord(HAGroupStoreRecord.DEFAULT_PROTOCOL_VERSION, haGroupName,
          HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC_TO_STANDBY, 0L,
          HighAvailabilityPolicy.FAILOVER.toString(), this.peerZkUrl, CLUSTERS.getMasterAddress1(),
          CLUSTERS.getMasterAddress2(), CLUSTERS.getHdfsUrl1(), CLUSTERS.getHdfsUrl2(), 0L);
      haAdmin.updateHAGroupStoreRecordInZooKeeper(haGroupName, haGroupStoreRecord, -1);
      awaitZkPropagation(haGroupName,
        HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC_TO_STANDBY);

      long startMs = System.currentTimeMillis();
      try {
        conn.createStatement()
          .execute("UPSERT INTO " + dataTableName + " VALUES ('1', 'Alice', 28)");
        conn.commit();
        fail("Expected MutationBlockedIOException to be thrown");
      } catch (CommitException e) {
        long durationMs = System.currentTimeMillis() - startMs;
        assertTrue(
          "Expected MutationBlockedIOException somewhere on the cause chain.",
          containsMutationBlockedException(e));
        assertTrue("Expected fail-fast under elevated retry settings: failed commit took "
          + durationMs + "ms which exceeds the bound of " + MAX_FAIL_FAST_DURATION_MS
          + "ms. Under hbase.client.retries.number=16 and hbase.client.pause=100, the absence "
          + "of the DNRIOE inheritance change would surface a multi-second retry tail. "
          + "If this assertion fails, the inheritance change is not in place or HBase has "
          + "started retrying despite DoNotRetryIOException — investigate the cause chain.",
          durationMs < MAX_FAIL_FAST_DURATION_MS);
      }
    }
  }

  /**
   * Polls ZooKeeper for the HAGroupStoreRecord to reflect the {@code expectedState} after a
   * {@code haAdmin.updateHAGroupStoreRecordInZooKeeper} write. Returns as soon as the cached
   * state matches the expected, or after a timeout. Replaces the unconditional
   * {@code Thread.sleep(ZK_CURATOR_EVENT_PROPAGATION_TIMEOUT_MS)} pattern with a bounded
   * polling loop (with a short between-poll park) so the wait is condition-driven rather than
   * fixed-duration.
   *
   * <p>The poll interval is short ({@link #ZK_PROPAGATION_POLL_INTERVAL_MS} ms) so the typical
   * wait is well under the propagation budget; the deadline gives generous CI headroom. Throws
   * {@code AssertionError} via {@code fail()} if the state never converges, which surfaces the
   * same failure shape a stale read would (the subsequent UPSERT would observe the old state).
   *
   * @param haGroupName    the HA group whose state to await
   * @param expectedState  the {@link HAGroupStoreRecord.HAGroupState} the cached record must reach
   * @throws Exception if reading the cached record throws
   */
  // java:S2925 (Thread.sleep in tests) suppressed: the sleep here is bounded between condition
  // checks inside a deadline-bounded polling loop, not an unconditional wait. The rule's flake
  // concern (fixed-duration sleeps) does not apply.
  @SuppressWarnings("java:S2925")
  private void awaitZkPropagation(String haGroupName,
      HAGroupStoreRecord.HAGroupState expectedState) throws Exception {
    long deadlineMs = System.currentTimeMillis() + ZK_PROPAGATION_AWAIT_DEADLINE_MS;
    while (System.currentTimeMillis() < deadlineMs) {
      HAGroupStoreRecord cached = haAdmin.getHAGroupStoreRecordInZooKeeper(haGroupName).getLeft();
      if (cached != null && cached.getHAGroupState() == expectedState) {
        return;
      }
      Thread.sleep(ZK_PROPAGATION_POLL_INTERVAL_MS);
    }
    fail("Timed out waiting for ZK to propagate HAGroupStoreRecord state " + expectedState
      + " for HA group " + haGroupName + " within " + ZK_PROPAGATION_AWAIT_DEADLINE_MS + "ms");
  }

  private static final long ZK_PROPAGATION_AWAIT_DEADLINE_MS = 5_000L;
  private static final long ZK_PROPAGATION_POLL_INTERVAL_MS = 50L;
}
