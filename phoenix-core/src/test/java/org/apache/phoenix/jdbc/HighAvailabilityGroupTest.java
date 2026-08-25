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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Properties;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.ClusterRoleRecord.ClusterRole;
import org.apache.phoenix.jdbc.HighAvailabilityGroup.HAGroupInfo;
import org.apache.phoenix.jdbc.HighAvailabilityGroup.State;
import org.apache.phoenix.monitoring.GlobalClientMetrics;
import org.apache.phoenix.monitoring.HAGroupClientMetricsSource;
import org.apache.phoenix.monitoring.HAGroupMetricsManager;
import org.apache.phoenix.monitoring.MetricType;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HighAvailabilityGroupTest {
  private static final Logger LOG = LoggerFactory.getLogger(FailoverPhoenixConnectionTest.class);
  String quorum1 = "master1\\\\:60010,master2\\\\:60000,master3\\\\:60010";
  String quorum2 = "peer_master1\\\\:60010,peer_master2\\\\:60000,peer_master3\\\\:60010";

  @Test
  public void testGetUrlInfo() throws Exception {
    String correctAdditionalParams = "phoenix.ha.group.name=testGetUrlInfo;key2=value2;key3=value3";
    String correctAdditionalParams2 =
      "key1=val1;phoenix.ha.group.name=testGetUrlInfo;key2=value2;key3=value3";
    String correctAdditionalParams3 = "key1=val1;key2=val2;phoenix.ha.group.name=testGetUrlInfo";
    String incorrectAdditionalParams = "key1;key2=val2";
    String missingAdditionalParams = "key1=value1";
    String principal = "principal";
    String urlFormatWithPrincipal = "jdbc:phoenix+rpc:[%s|%s]:%s:%s";
    String urlFormatWithoutPrincipal = "jdbc:phoenix+rpc:[%s|%s]::%s";

    // Test correct additional params with principal
    String url =
      String.format(urlFormatWithPrincipal, quorum1, quorum2, principal, correctAdditionalParams);
    getAndAssertUrlInfo(url, correctAdditionalParams, principal);

    // Test correct additional params without principal
    url = String.format(urlFormatWithoutPrincipal, quorum1, quorum2, correctAdditionalParams);
    getAndAssertUrlInfo(url, correctAdditionalParams);

    // Test another set of correct additional params with principal
    url =
      String.format(urlFormatWithPrincipal, quorum1, quorum2, principal, correctAdditionalParams2);
    getAndAssertUrlInfo(url, correctAdditionalParams2, principal);

    // Test another set of correct additional params without principal
    url = String.format(urlFormatWithoutPrincipal, quorum1, quorum2, correctAdditionalParams2);
    getAndAssertUrlInfo(url, correctAdditionalParams2);

    // Test another set of correct additional params with principal
    url =
      String.format(urlFormatWithPrincipal, quorum1, quorum2, principal, correctAdditionalParams3);
    getAndAssertUrlInfo(url, correctAdditionalParams3, principal);

    // Test another set of correct additional params without principal
    url = String.format(urlFormatWithoutPrincipal, quorum1, quorum2, correctAdditionalParams3);
    getAndAssertUrlInfo(url, correctAdditionalParams3);

    // Test incorrect additional params
    url =
      String.format(urlFormatWithPrincipal, quorum1, quorum2, principal, incorrectAdditionalParams);
    try {
      getAndAssertUrlInfo(url, incorrectAdditionalParams, principal);
    } catch (SQLException e) {
      assertEquals(e.getErrorCode(), SQLExceptionCode.MALFORMED_CONNECTION_URL.getErrorCode());
    }

    // Test incorrect additional params without principal
    url = String.format(urlFormatWithoutPrincipal, quorum1, quorum2, incorrectAdditionalParams);
    try {
      getAndAssertUrlInfo(url, incorrectAdditionalParams);
    } catch (SQLException e) {
      assertEquals(e.getErrorCode(), SQLExceptionCode.MALFORMED_CONNECTION_URL.getErrorCode());
    }

    // Test missing additional params
    url =
      String.format(urlFormatWithPrincipal, quorum1, quorum2, principal, missingAdditionalParams);
    try {
      getAndAssertUrlInfo(url, missingAdditionalParams, principal);
    } catch (SQLException e) {
      assertEquals(e.getErrorCode(), SQLExceptionCode.HA_INVALID_PROPERTIES.getErrorCode());
    }

    // Test missing additional params without principal
    url = String.format(urlFormatWithoutPrincipal, quorum1, quorum2, missingAdditionalParams);
    try {
      getAndAssertUrlInfo(url, missingAdditionalParams);
    } catch (SQLException e) {
      assertEquals(e.getErrorCode(), SQLExceptionCode.HA_INVALID_PROPERTIES.getErrorCode());
    }

  }

  private void getAndAssertUrlInfo(String url, String additionalParams, String principal)
    throws Exception {
    Properties properties = new Properties();
    HAURLInfo haurlInfo = HighAvailabilityGroup.getUrlInfo(url, properties);
    assertEquals(haurlInfo.getName(), "testGetUrlInfo");
    if (principal != null) {
      assertEquals(haurlInfo.getPrincipal(), principal);
    } else {
      assertNull(haurlInfo.getPrincipal());
    }
    assertEquals(haurlInfo.getAdditionalJDBCParams(), additionalParams);
  }

  private void getAndAssertUrlInfo(String url, String additionalParams) throws Exception {
    getAndAssertUrlInfo(url, additionalParams, null);
  }

  /**
   * Verifies the gate decision for {@code HA_FAILOVER_COUNT} — exercised directly via the
   * package-private {@link HighAvailabilityGroup#shouldCountFailover} helper rather than driving a
   * mini-cluster transition. Together these cases pin down that the gate (a) counts a real ACTIVE
   * URL move, (b) does NOT count a no-op (same active URL), (c) does NOT count a transition INTO a
   * no-active state, and (d) does NOT count a transition where the policy callback failed
   * ({@code transitionSucceeded == false}, i.e. {@code future.get()} threw {@code TimeoutException}
   * / {@code ExecutionException}). The (d) negative-path assertion is the regression guard: someone
   * removing the {@code transitionSucceeded &&} clause from
   * {@code HighAvailabilityGroup#shouldCountFailover} would silently start counting failed
   * transitions as successful failovers, and this test would fail.
   */
  @Test
  public void testShouldCountFailoverGate() {
    String haGroupName = "testShouldCountFailoverGate";
    String url1 = "host1\\:60010";
    String url2 = "host2\\:60010";

    ClusterRoleRecord aActiveBStandby = new ClusterRoleRecord(haGroupName,
      HighAvailabilityPolicy.FAILOVER, url1, ClusterRole.ACTIVE, url2, ClusterRole.STANDBY, 1L);
    ClusterRoleRecord aStandbyBActive = new ClusterRoleRecord(haGroupName,
      HighAvailabilityPolicy.FAILOVER, url1, ClusterRole.STANDBY, url2, ClusterRole.ACTIVE, 2L);
    ClusterRoleRecord bothStandby = new ClusterRoleRecord(haGroupName,
      HighAvailabilityPolicy.FAILOVER, url1, ClusterRole.STANDBY, url2, ClusterRole.STANDBY, 3L);

    // (a) Real active-URL move with a successful policy transition → COUNT.
    assertTrue(
      "ACTIVE moving from cluster 1 to cluster 2 with successful policy transition "
        + "should count as a failover",
      HighAvailabilityGroup.shouldCountFailover(true, aActiveBStandby, aStandbyBActive));

    // (b) Same active URL (no-op transition) → DO NOT COUNT, even if policy succeeded.
    assertFalse(
      "Same active URL on both sides should NOT count as a failover even with a "
        + "successful policy transition",
      HighAvailabilityGroup.shouldCountFailover(true, aActiveBStandby, aActiveBStandby));

    // (c) Transition INTO no-active state (both STANDBY) → DO NOT COUNT.
    assertFalse("Transition into a no-active (both STANDBY) state should NOT count as a failover",
      HighAvailabilityGroup.shouldCountFailover(true, aActiveBStandby, bothStandby));

    // (d) NEGATIVE PATH — policy callback failed (transitionSucceeded=false, simulating
    // future.get() throwing TimeoutException or ExecutionException) → DO NOT COUNT, even if
    // the active URL appears to have moved. This is the regression guard for the
    // {@code transitionSucceeded &&} clause; removing it would silently inflate
    // HA_FAILOVER_COUNT on failed transitions.
    assertFalse(
      "Failed policy transition (transitionSucceeded=false) must NOT count as a failover even "
        + "when the candidate new record shows a different active URL",
      HighAvailabilityGroup.shouldCountFailover(false, aActiveBStandby, aStandbyBActive));

    // (e) Recovery from no-active back to having an ACTIVE peer with a successful
    // transition → COUNT (operationally a real failover-recovery event).
    assertTrue(
      "Recovery from no-active back to ACTIVE with a successful policy transition "
        + "should count as a failover",
      HighAvailabilityGroup.shouldCountFailover(true, bothStandby, aStandbyBActive));
  }

  /** Reconciliation prefers non-UNKNOWN over UNKNOWN, then higher version; order-independent. */
  @Test
  public void testReconcileClusterRoleRecords() {
    String haGroupName = "testReconcileClusterRoleRecords";
    String url1 = "host1\\:60010";
    String url2 = "host2\\:60010";

    ClusterRoleRecord v9 = new ClusterRoleRecord(haGroupName, HighAvailabilityPolicy.FAILOVER, url1,
      ClusterRole.ACTIVE, url2, ClusterRole.STANDBY, 9L);
    ClusterRoleRecord v10 = new ClusterRoleRecord(haGroupName, HighAvailabilityPolicy.FAILOVER,
      url1, ClusterRole.STANDBY, url2, ClusterRole.ACTIVE, 10L);
    ClusterRoleRecord unknownV11 = new ClusterRoleRecord(haGroupName,
      HighAvailabilityPolicy.FAILOVER, url1, ClusterRole.UNKNOWN, url2, ClusterRole.STANDBY, 11L);
    ClusterRoleRecord unknownV12 = new ClusterRoleRecord(haGroupName,
      HighAvailabilityPolicy.FAILOVER, url1, ClusterRole.UNKNOWN, url2, ClusterRole.UNKNOWN, 12L);

    // Both usable: higher version wins (the stale-revert regression guard), either order.
    assertSame("Higher version must win when cluster 1 lags the peer", v10,
      HighAvailabilityGroup.reconcileClusterRoleRecords(v9, v10, null));
    assertSame("Higher version must win regardless of argument order", v10,
      HighAvailabilityGroup.reconcileClusterRoleRecords(v10, v9, null));

    // Non-UNKNOWN beats UNKNOWN even at a lower version, either order.
    assertSame("Non-UNKNOWN must beat UNKNOWN even with a lower version (cluster 1 usable)", v9,
      HighAvailabilityGroup.reconcileClusterRoleRecords(v9, unknownV11, null));
    assertSame("Non-UNKNOWN must beat UNKNOWN even with a lower version (cluster 2 usable)", v9,
      HighAvailabilityGroup.reconcileClusterRoleRecords(unknownV11, v9, null));

    // UNKNOWN vs UNKNOWN: higher version wins.
    assertSame("Among two UNKNOWN records the higher version wins", unknownV12,
      HighAvailabilityGroup.reconcileClusterRoleRecords(unknownV11, unknownV12, null));

    // A strictly newer UNKNOWN-tagged record that STILL names an active cluster (one role ACTIVE,
    // peer momentarily UNKNOWN mid-transition) must beat a stale fully-known record: masking it
    // would keep routing to the since-demoted cluster. Either order.
    ClusterRoleRecord activeUnknownV13 = new ClusterRoleRecord(haGroupName,
      HighAvailabilityPolicy.FAILOVER, url1, ClusterRole.ACTIVE, url2, ClusterRole.UNKNOWN, 13L);
    assertSame(
      "Newer UNKNOWN-tagged record that still names an ACTIVE cluster must win (cluster 2)",
      activeUnknownV13,
      HighAvailabilityGroup.reconcileClusterRoleRecords(v9, activeUnknownV13, null));
    assertSame(
      "Newer UNKNOWN-tagged record that still names an ACTIVE cluster must win (cluster 1)",
      activeUnknownV13,
      HighAvailabilityGroup.reconcileClusterRoleRecords(activeUnknownV13, v9, null));

    // But a newer UNKNOWN record with NO active role stays masked behind the usable record: it
    // cannot route a connection, so recovery comes from the next scheduled refresh (the poller runs
    // only if the usable record is itself non-active). unknownV11 (url1 UNKNOWN, url2 STANDBY, no
    // active) is newer than v9 but not routable.
    assertSame("Newer UNKNOWN record with no active role stays masked behind the usable record", v9,
      HighAvailabilityGroup.reconcileClusterRoleRecords(v9, unknownV11, null));

    // Equal-version boundary: an active-tagged UNKNOWN record at the SAME version as a fully-known
    // usable record must NOT win. The newer-UNKNOWN carve-out is strict '>', so only a strictly
    // newer active-unknown record displaces the usable one; a same-version peer cannot. This pins
    // the strict boundary — a '>=' mutant would wrongly route to the active-unknown record. Either
    // order.
    ClusterRoleRecord activeUnknownV9 = new ClusterRoleRecord(haGroupName,
      HighAvailabilityPolicy.FAILOVER, url1, ClusterRole.ACTIVE, url2, ClusterRole.UNKNOWN, 9L);
    assertSame("Equal-version active-unknown must NOT displace the fully-known usable record", v9,
      HighAvailabilityGroup.reconcileClusterRoleRecords(v9, activeUnknownV9, null));
    assertSame("Equal-version active-unknown must NOT displace the usable record (order swapped)",
      v9, HighAvailabilityGroup.reconcileClusterRoleRecords(activeUnknownV9, v9, null));
  }

  /**
   * On an equal-version divergence (endpoints at the same version but different roles, one lagging)
   * reconcile keeps the currently applied record when it sits at that version, deferring the
   * transition until the endpoints converge; once both endpoints agree the same-version record is
   * returned. A genuine version advance is never dropped.
   */
  @Test
  public void testReconcileEqualVersionDivergence() {
    String haGroupName = "testReconcileEqualVersionDivergence";
    String url1 = "host1\\:60010";
    String url2 = "host2\\:60010";

    // Applied record: url1 ACTIVE, url2 STANDBY at v10.
    ClusterRoleRecord current = new ClusterRoleRecord(haGroupName, HighAvailabilityPolicy.FAILOVER,
      url1, ClusterRole.ACTIVE, url2, ClusterRole.STANDBY, 10L);
    // Same version, diverging roles — a lagging/leading peer mid-propagation.
    ClusterRoleRecord divergentV10 = new ClusterRoleRecord(haGroupName,
      HighAvailabilityPolicy.FAILOVER, url1, ClusterRole.STANDBY, url2, ClusterRole.ACTIVE, 10L);
    // Both endpoints agreeing on the new same-version roles (propagation complete).
    ClusterRoleRecord agreedV10a = new ClusterRoleRecord(haGroupName,
      HighAvailabilityPolicy.FAILOVER, url1, ClusterRole.STANDBY, url2, ClusterRole.STANDBY, 10L);
    ClusterRoleRecord agreedV10b = new ClusterRoleRecord(haGroupName,
      HighAvailabilityPolicy.FAILOVER, url1, ClusterRole.STANDBY, url2, ClusterRole.STANDBY, 10L);
    // A genuine version advance both endpoints see.
    ClusterRoleRecord v11 = new ClusterRoleRecord(haGroupName, HighAvailabilityPolicy.FAILOVER,
      url1, ClusterRole.STANDBY, url2, ClusterRole.ACTIVE, 11L);

    // Equal-version divergence with current at that version → keep current (no flap), either order.
    assertSame("Equal-version divergence must keep the applied record (peer diverges)", current,
      HighAvailabilityGroup.reconcileClusterRoleRecords(current, divergentV10, current));
    assertSame("Equal-version divergence defers regardless of which endpoint diverges", current,
      HighAvailabilityGroup.reconcileClusterRoleRecords(divergentV10, current, current));

    // Both endpoints agree on the new same-version roles → apply it (autonomous transition). The
    // equal-values branch returns recordFromCluster2, and the returned record differs from current
    // so the refresh path will apply it.
    assertSame("Both endpoints agreeing on a same-version role change must be applied", agreedV10b,
      HighAvailabilityGroup.reconcileClusterRoleRecords(agreedV10a, agreedV10b, current));
    assertFalse("Agreed same-version record must differ from current so refresh applies it",
      current.equals(agreedV10b));

    // First load (current == null) with an equal-version divergence → deterministic peer record.
    assertSame("First-load equal-version divergence falls back to the peer record", divergentV10,
      HighAvailabilityGroup.reconcileClusterRoleRecords(current, divergentV10, null));

    // Equal-version divergence where the applied record is at a LOWER version than the endpoints:
    // the defer guard (current.version == endpoints' version) does not fire, so this falls through
    // to the deterministic peer record rather than keeping the stale applied record. This is the
    // other branch of the fall-through the first-load case cannot reach (current != null).
    ClusterRoleRecord olderCurrentV9 = new ClusterRoleRecord(haGroupName,
      HighAvailabilityPolicy.FAILOVER, url1, ClusterRole.ACTIVE, url2, ClusterRole.STANDBY, 9L);
    assertSame(
      "Equal-version divergence with the applied record at a lower version falls through to the "
        + "peer record (the defer only fires when current sits at the endpoints' version)",
      divergentV10,
      HighAvailabilityGroup.reconcileClusterRoleRecords(current, divergentV10, olderCurrentV9));

    // A genuine version advance is applied even though current is at the older version.
    assertSame("A strictly newer version must still win over the applied record", v11,
      HighAvailabilityGroup.reconcileClusterRoleRecords(current, v11, current));
  }

  /** Refresh guard rejects a lower version but applies a same-version role change. */
  @Test
  public void testShouldApplyRefreshedRecord() {
    String haGroupName = "testShouldApplyRefreshedRecord";
    String url1 = "host1\\:60010";
    String url2 = "host2\\:60010";

    ClusterRoleRecord v9 = new ClusterRoleRecord(haGroupName, HighAvailabilityPolicy.FAILOVER, url1,
      ClusterRole.ACTIVE, url2, ClusterRole.STANDBY, 9L);
    ClusterRoleRecord v10 = new ClusterRoleRecord(haGroupName, HighAvailabilityPolicy.FAILOVER,
      url1, ClusterRole.STANDBY, url2, ClusterRole.ACTIVE, 10L);
    // Same version as v10 but different roles — models an autonomous transition (roles change,
    // admin version does not).
    ClusterRoleRecord v10RolesChanged = new ClusterRoleRecord(haGroupName,
      HighAvailabilityPolicy.FAILOVER, url1, ClusterRole.STANDBY, url2, ClusterRole.STANDBY, 10L);

    assertFalse("Must not roll back from the applied v10 to a stale fetched v9",
      HighAvailabilityGroup.shouldApplyRefreshedRecord(v10, v9));
    assertTrue("Must apply a strictly newer fetched record",
      HighAvailabilityGroup.shouldApplyRefreshedRecord(v9, v10));
    // A strict '>' guard would wrongly reject this same-version role change.
    assertTrue("Must apply a same-version record with changed roles (autonomous transition)",
      HighAvailabilityGroup.shouldApplyRefreshedRecord(v10, v10RolesChanged));
  }

  /**
   * Pins {@link HighAvailabilityGroup#isCausedByInterrupt}: both interrupt marker types (direct and
   * wrapped) are detected, non-interrupt chains are not, and the depth-16 bound stops the walk so a
   * self-referential (cyclic) cause chain terminates rather than spinning forever.
   */
  @Test
  public void testIsCausedByInterrupt() {
    assertTrue("Direct InterruptedException must be detected",
      HighAvailabilityGroup.isCausedByInterrupt(new InterruptedException()));
    assertTrue("Direct InterruptedIOException must be detected",
      HighAvailabilityGroup.isCausedByInterrupt(new InterruptedIOException()));
    assertTrue("A wrapped interrupt marker must be detected", HighAvailabilityGroup
      .isCausedByInterrupt(new SQLException("rpc", new InterruptedIOException())));
    assertFalse("A non-interrupt cause chain must not be detected",
      HighAvailabilityGroup.isCausedByInterrupt(new SQLException("rpc", new IOException("io"))));
    assertFalse("null must not be detected", HighAvailabilityGroup.isCausedByInterrupt(null));

    // An interrupt marker at chain index 15 is within the depth-16 bound → detected.
    Throwable withinBound = new InterruptedException("deep");
    for (int i = 0; i < 15; i++) {
      withinBound = new RuntimeException("wrap" + i, withinBound);
    }
    assertTrue("An interrupt marker at depth 15 (within the bound) must be detected",
      HighAvailabilityGroup.isCausedByInterrupt(withinBound));

    // An interrupt marker at chain index 16 is beyond the bound → not detected.
    Throwable beyondBound = new InterruptedException("deeper");
    for (int i = 0; i < 16; i++) {
      beyondBound = new RuntimeException("wrap" + i, beyondBound);
    }
    assertFalse("An interrupt marker at depth 16 (beyond the bound) must not be detected",
      HighAvailabilityGroup.isCausedByInterrupt(beyondBound));

    // A self-referential cause chain must terminate (the depth bound is the cycle guard).
    Throwable selfCycle = new RuntimeException() {
      @Override
      public synchronized Throwable getCause() {
        return this;
      }
    };
    assertFalse("A cyclic cause chain must terminate and not be detected",
      HighAvailabilityGroup.isCausedByInterrupt(selfCycle));
  }

  /**
   * Wires the refresh no-rollback branch end to end: when the endpoint serves a strictly older
   * record than the applied one, {@code refreshClusterRoleRecord} keeps the applied record, stays
   * {@code READY}, does not count a failover, and returns {@code true}. A passing
   * {@code shouldApplyRefreshedRecord} unit test alone does not prove this branch is wired — an
   * inverted guard would silently reintroduce the rollback with the helper test still green.
   */
  @Test
  public void testRefreshDoesNotRollBackToOlderRecord() throws Exception {
    String haGroupName = "testRefreshDoesNotRollBackToOlderRecord";
    String url1 = "host1\\:60010";
    String url2 = "host2\\:60010";
    ClusterRoleRecord appliedV10 = new ClusterRoleRecord(haGroupName,
      HighAvailabilityPolicy.FAILOVER, url1, ClusterRole.ACTIVE, url2, ClusterRole.STANDBY, 10L);
    ClusterRoleRecord staleV9 = new ClusterRoleRecord(haGroupName, HighAvailabilityPolicy.FAILOVER,
      url1, ClusterRole.STANDBY, url2, ClusterRole.ACTIVE, 9L);

    HAGroupInfo info = new HAGroupInfo(haGroupName, url1, url2);
    HighAvailabilityGroup group =
      Mockito.spy(new HighAvailabilityGroup(info, new Properties(), appliedV10, State.READY));
    Mockito.doReturn(staleV9).when(group).getClusterRoleRecordFromEndpoint();

    long failoverBefore = GlobalClientMetrics.GLOBAL_HA_FAILOVER_COUNT.getMetric().getValue();
    assertTrue("Refresh that would roll back must be a no-op returning true",
      group.refreshClusterRoleRecord(true));
    assertSame("The applied record must be kept, not rolled back to the older fetched record",
      appliedV10, group.getRoleRecord());
    assertSame("HA group must stay READY after a rejected rollback", State.READY,
      group.getStateForTesting());
    assertEquals("A rejected rollback must not count as a failover", failoverBefore,
      GlobalClientMetrics.GLOBAL_HA_FAILOVER_COUNT.getMetric().getValue());
  }

  /**
   * Wires {@link HighAvailabilityGroup#getClusterRoleRecordFromEndpoint}: because reconcile is
   * order-independent for most cases, a url1/url2 fetch swap or a wrong {@code current} argument
   * would pass every reconcile helper test yet change first-load behavior. On a first-load
   * equal-version divergence reconcile returns the cluster-2 record, so the resolved record must be
   * the url2 fetch (a swap would surface the url1 fetch); once a same-version record is applied the
   * defer must return that applied record (proving {@code this.roleRecord} is threaded as
   * {@code current}).
   */
  @Test
  public void testGetClusterRoleRecordFromEndpointWiring() throws Exception {
    String haGroupName = "testGetClusterRoleRecordFromEndpointWiring";
    String url1 = "host1\\:60010";
    String url2 = "host2\\:60010";
    ClusterRoleRecord fromUrl1 = new ClusterRoleRecord(haGroupName, HighAvailabilityPolicy.FAILOVER,
      url1, ClusterRole.ACTIVE, url2, ClusterRole.STANDBY, 10L);
    ClusterRoleRecord fromUrl2 = new ClusterRoleRecord(haGroupName, HighAvailabilityPolicy.FAILOVER,
      url1, ClusterRole.STANDBY, url2, ClusterRole.ACTIVE, 10L);
    HAGroupInfo info = new HAGroupInfo(haGroupName, url1, url2);

    // First load (current == null): equal-version divergence resolves to the cluster-2 (url2)
    // fetch.
    HighAvailabilityGroup firstLoad =
      Mockito.spy(new HighAvailabilityGroup(info, new Properties(), null, State.UNINITIALIZED));
    Mockito.doReturn(fromUrl1).when(firstLoad).fetchClusterRoleRecord(url1);
    Mockito.doReturn(fromUrl2).when(firstLoad).fetchClusterRoleRecord(url2);
    assertSame("url1 must be fetched as cluster 1 and url2 as cluster 2 (a swap would return the "
      + "url1 record)", fromUrl2, firstLoad.getClusterRoleRecordFromEndpoint());

    // Applied record at the endpoints' version: the equal-version defer must return
    // this.roleRecord,
    // proving current is threaded (otherwise the fall-through would return the url2 record).
    ClusterRoleRecord appliedV10 = new ClusterRoleRecord(haGroupName,
      HighAvailabilityPolicy.FAILOVER, url1, ClusterRole.ACTIVE, url2, ClusterRole.STANDBY, 10L);
    HighAvailabilityGroup applied =
      Mockito.spy(new HighAvailabilityGroup(info, new Properties(), appliedV10, State.READY));
    Mockito.doReturn(fromUrl1).when(applied).fetchClusterRoleRecord(url1);
    Mockito.doReturn(fromUrl2).when(applied).fetchClusterRoleRecord(url2);
    assertSame("Equal-version divergence must defer to the applied record (current threaded)",
      appliedV10, applied.getClusterRoleRecordFromEndpoint());
  }

  /**
   * Cluster-1 fails with CRR-Not-Found but cluster 2 serves a record: the method falls back to the
   * cluster-2 record and returns it (no reconciliation on a single reachable endpoint).
   */
  @Test
  public void testEndpointNotFoundOnCluster1FallsBackToCluster2() throws Exception {
    String haGroupName = "testEndpointNotFoundOnCluster1FallsBackToCluster2";
    String url1 = "host1\\:60010";
    String url2 = "host2\\:60010";
    ClusterRoleRecord fromUrl2 = new ClusterRoleRecord(haGroupName, HighAvailabilityPolicy.FAILOVER,
      url1, ClusterRole.STANDBY, url2, ClusterRole.ACTIVE, 10L);
    HAGroupInfo info = new HAGroupInfo(haGroupName, url1, url2);

    HighAvailabilityGroup group =
      Mockito.spy(new HighAvailabilityGroup(info, new Properties(), null, State.UNINITIALIZED));
    Mockito
      .doThrow(
        new SQLException("not found", SQLExceptionCode.CLUSTER_ROLE_RECORD_NOT_FOUND.getSQLState(),
          SQLExceptionCode.CLUSTER_ROLE_RECORD_NOT_FOUND.getErrorCode()))
      .when(group).fetchClusterRoleRecord(url1);
    Mockito.doReturn(fromUrl2).when(group).fetchClusterRoleRecord(url2);

    assertSame("A cluster-1 Not-Found must fall back to the cluster-2 record", fromUrl2,
      group.getClusterRoleRecordFromEndpoint());
  }

  /**
   * Cluster-1 fails with CRR-Not-Found and cluster 2 also fails: the original Not-Found propagates
   * (so downstream single-cluster fallback can trigger) with the cluster-2 failure attached as a
   * suppressed exception, so the single thrown exception carries both root causes.
   */
  @Test
  public void testEndpointBothFailNotFoundRethrowsNotFoundWithSuppressed() throws Exception {
    String haGroupName = "testEndpointBothFailNotFoundRethrowsNotFoundWithSuppressed";
    String url1 = "host1\\:60010";
    String url2 = "host2\\:60010";
    HAGroupInfo info = new HAGroupInfo(haGroupName, url1, url2);

    HighAvailabilityGroup group =
      Mockito.spy(new HighAvailabilityGroup(info, new Properties(), null, State.UNINITIALIZED));
    SQLException notFound =
      new SQLException("not found", SQLExceptionCode.CLUSTER_ROLE_RECORD_NOT_FOUND.getSQLState(),
        SQLExceptionCode.CLUSTER_ROLE_RECORD_NOT_FOUND.getErrorCode());
    SQLException cluster2Failure = new SQLException("cluster 2 unreachable");
    Mockito.doThrow(notFound).when(group).fetchClusterRoleRecord(url1);
    Mockito.doThrow(cluster2Failure).when(group).fetchClusterRoleRecord(url2);

    try {
      group.getClusterRoleRecordFromEndpoint();
      fail("Expected the cluster-1 Not-Found to propagate when both endpoints fail");
    } catch (SQLException e) {
      assertSame("The original Not-Found must propagate so downstream fallback can trigger",
        notFound, e);
      assertEquals("Propagated Not-Found must keep its error code",
        SQLExceptionCode.CLUSTER_ROLE_RECORD_NOT_FOUND.getErrorCode(), e.getErrorCode());
      assertEquals("The cluster-2 failure must be attached as suppressed", 1,
        e.getSuppressed().length);
      assertSame(cluster2Failure, e.getSuppressed()[0]);
    }
  }

  /**
   * Cluster-1 fails with a non-Not-Found exception and cluster 2 also fails: the cluster-2
   * exception propagates (its distinct error, not the cluster-1 transport failure) with the
   * cluster-1 failure attached as suppressed, so the single thrown exception carries both root
   * causes. This is the else-branch parity with the Not-Found path — the scenario where cluster 1
   * is merely unreachable and cluster 2 then reports Not-Found.
   */
  @Test
  public void testEndpointBothFailNonNotFoundRethrowsCluster2WithSuppressed() throws Exception {
    String haGroupName = "testEndpointBothFailNonNotFoundRethrowsCluster2WithSuppressed";
    String url1 = "host1\\:60010";
    String url2 = "host2\\:60010";
    HAGroupInfo info = new HAGroupInfo(haGroupName, url1, url2);

    HighAvailabilityGroup group =
      Mockito.spy(new HighAvailabilityGroup(info, new Properties(), null, State.UNINITIALIZED));
    SQLException cluster1Failure = new SQLException("cluster 1 unreachable");
    SQLException cluster2NotFound =
      new SQLException("not found", SQLExceptionCode.CLUSTER_ROLE_RECORD_NOT_FOUND.getSQLState(),
        SQLExceptionCode.CLUSTER_ROLE_RECORD_NOT_FOUND.getErrorCode());
    Mockito.doThrow(cluster1Failure).when(group).fetchClusterRoleRecord(url1);
    Mockito.doThrow(cluster2NotFound).when(group).fetchClusterRoleRecord(url2);

    try {
      group.getClusterRoleRecordFromEndpoint();
      fail("Expected the cluster-2 exception to propagate when both endpoints fail");
    } catch (SQLException e) {
      assertSame("The cluster-2 exception must propagate on the non-Not-Found fallback path",
        cluster2NotFound, e);
      assertEquals("The cluster-1 failure must be attached as suppressed", 1,
        e.getSuppressed().length);
      assertSame(cluster1Failure, e.getSuppressed()[0]);
    }
  }

  /**
   * Cluster-1 is reachable but the cluster-2 fetch fails: the method degrades to the cluster-1
   * record (no reconciliation), rather than propagating the cluster-2 failure.
   */
  @Test
  public void testEndpointCluster2FailureDegradesToCluster1() throws Exception {
    String haGroupName = "testEndpointCluster2FailureDegradesToCluster1";
    String url1 = "host1\\:60010";
    String url2 = "host2\\:60010";
    ClusterRoleRecord fromUrl1 = new ClusterRoleRecord(haGroupName, HighAvailabilityPolicy.FAILOVER,
      url1, ClusterRole.ACTIVE, url2, ClusterRole.STANDBY, 10L);
    HAGroupInfo info = new HAGroupInfo(haGroupName, url1, url2);

    HighAvailabilityGroup group =
      Mockito.spy(new HighAvailabilityGroup(info, new Properties(), null, State.UNINITIALIZED));
    Mockito.doReturn(fromUrl1).when(group).fetchClusterRoleRecord(url1);
    Mockito.doThrow(new SQLException("cluster 2 unreachable")).when(group)
      .fetchClusterRoleRecord(url2);

    assertSame("A cluster-2 failure must degrade to the cluster-1 record", fromUrl1,
      group.getClusterRoleRecordFromEndpoint());
  }

  /**
   * A cluster-1 fetch failure wrapping an interruption restores the thread's interrupt status on
   * the fallback path (the flag is cleared by the blocking fetch and must be re-raised so callers
   * still observe cancellation).
   */
  @Test
  public void testEndpointRestoresInterruptStatusOnFallback() throws Exception {
    String haGroupName = "testEndpointRestoresInterruptStatusOnFallback";
    String url1 = "host1\\:60010";
    String url2 = "host2\\:60010";
    ClusterRoleRecord fromUrl2 = new ClusterRoleRecord(haGroupName, HighAvailabilityPolicy.FAILOVER,
      url1, ClusterRole.STANDBY, url2, ClusterRole.ACTIVE, 10L);
    HAGroupInfo info = new HAGroupInfo(haGroupName, url1, url2);

    HighAvailabilityGroup group =
      Mockito.spy(new HighAvailabilityGroup(info, new Properties(), null, State.UNINITIALIZED));
    // Non-Not-Found cluster-1 failure wrapping an interruption; cluster 2 then succeeds.
    Mockito.doThrow(new SQLException("interrupted rpc", new InterruptedIOException())).when(group)
      .fetchClusterRoleRecord(url1);
    Mockito.doReturn(fromUrl2).when(group).fetchClusterRoleRecord(url2);

    // Clear any pre-existing interrupt status so the assertion is meaningful.
    Thread.interrupted();
    try {
      assertSame("Fallback to cluster 2 must still return its record", fromUrl2,
        group.getClusterRoleRecordFromEndpoint());
      assertTrue("The interrupt status must be restored after the fallback fetch",
        Thread.currentThread().isInterrupted());
    } finally {
      // Consume the interrupt flag so it does not leak into a reused fork.
      Thread.interrupted();
    }
  }

  /**
   * A real counted role-flip transition driven through {@code refreshClusterRoleRecord} must both
   * increment {@code HA_FAILOVER_COUNT} and record a sample on {@code HA_FAILOVER_DURATION_MS}.
   * This pins the duration metric to the CRR-write transition path (the path that autonomous
   * failovers actually take) rather than the connection-level
   * {@code FailoverPhoenixConnection.failover()} path, which is never auto-invoked under the
   * default {@code ExplicitFailoverPolicy}. Active URL flips from url1 to url2 so
   * {@code shouldCountFailover} returns true; the {@code URLS} entry is seeded empty so the policy
   * transition is a clean no-op (no real connections needed).
   */
  @Test
  public void testCountedTransitionRecordsFailoverCountAndDuration() throws Exception {
    String haGroupName = "testCountedTransitionRecordsFailoverCountAndDuration";
    String url1 = "host1\\:60010";
    String url2 = "host2\\:60010";
    ClusterRoleRecord aActiveBStandby = new ClusterRoleRecord(haGroupName,
      HighAvailabilityPolicy.FAILOVER, url1, ClusterRole.ACTIVE, url2, ClusterRole.STANDBY, 10L);
    ClusterRoleRecord aStandbyBActive = new ClusterRoleRecord(haGroupName,
      HighAvailabilityPolicy.FAILOVER, url1, ClusterRole.STANDBY, url2, ClusterRole.ACTIVE, 11L);

    HAGroupInfo info = new HAGroupInfo(haGroupName, url1, url2);
    // Seed an empty URL set so the policy-side transition iterates nothing and is a clean no-op.
    HighAvailabilityGroup.URLS.put(info, new HashSet<>());
    try {
      HighAvailabilityGroup group = Mockito
        .spy(new HighAvailabilityGroup(info, new Properties(), aActiveBStandby, State.READY));
      Mockito.doReturn(aStandbyBActive).when(group).getClusterRoleRecordFromEndpoint();

      long countBefore = GlobalClientMetrics.GLOBAL_HA_FAILOVER_COUNT.getMetric().getValue();
      long durationSamplesBefore =
        GlobalClientMetrics.GLOBAL_HA_FAILOVER_DURATION_MS.getMetric().getNumberOfSamples();

      assertTrue("A real role-flip transition must apply and return true",
        group.refreshClusterRoleRecord(true));
      assertSame("The new record must be applied after the transition", aStandbyBActive,
        group.getRoleRecord());

      assertEquals("An active-URL flip must increment HA_FAILOVER_COUNT", countBefore + 1,
        GlobalClientMetrics.GLOBAL_HA_FAILOVER_COUNT.getMetric().getValue());
      assertEquals(
        "The transition must record a HA_FAILOVER_DURATION_MS sample on the CRR-write " + "path",
        durationSamplesBefore + 1,
        GlobalClientMetrics.GLOBAL_HA_FAILOVER_DURATION_MS.getMetric().getNumberOfSamples());

      // Per-group (ha_group-tagged) emission mirrors the JVM-global counters. This group name is
      // unique to this test, so its source starts empty and this single transition yields exactly 1
      // on both the failover and the applied-transition counters.
      HAGroupClientMetricsSource source = HAGroupMetricsManager.getIfPresent(haGroupName);
      assertNotNull("The transition must register the per-group metrics source", source);
      assertEquals("Per-group HA_FAILOVER_COUNT must count the active-URL flip", 1L,
        source.getCounterValue(MetricType.HA_FAILOVER_COUNT));
      assertEquals("Per-group CRR_TRANSITION_COUNT must count the applied transition", 1L,
        source.getCounterValue(MetricType.CRR_TRANSITION_COUNT));
    } finally {
      HighAvailabilityGroup.URLS.remove(info);
      HAGroupMetricsManager.remove(haGroupName);
    }
  }

  /**
   * A failed {@code connectActive} (no active cluster in the record) must increment
   * {@code HA_FAILOVER_CONNECTION_FAILED_COUNTER} on its single SQLException throw funnel.
   */
  @Test
  public void testConnectActiveFailureIncrementsFailedCounter() {
    String haGroupName = "testConnectActiveFailureIncrementsFailedCounter";
    String url1 = "host1\\:60010";
    String url2 = "host2\\:60010";
    // Both STANDBY → no active URL → connectActive takes the HA_NO_ACTIVE_CLUSTER throw path.
    ClusterRoleRecord bothStandby = new ClusterRoleRecord(haGroupName,
      HighAvailabilityPolicy.FAILOVER, url1, ClusterRole.STANDBY, url2, ClusterRole.STANDBY, 10L);
    HAGroupInfo info = new HAGroupInfo(haGroupName, url1, url2);
    HighAvailabilityGroup group =
      new HighAvailabilityGroup(info, new Properties(), bothStandby, State.READY);

    long failedBefore =
      GlobalClientMetrics.GLOBAL_HA_FAILOVER_CONNECTION_FAILED_COUNTER.getMetric().getValue();
    try {
      group.connectActive(new Properties(), new HAURLInfo(haGroupName));
      fail("connectActive must throw when the HA group has no active cluster");
    } catch (SQLException e) {
      assertEquals(SQLExceptionCode.CANNOT_ESTABLISH_CONNECTION.getErrorCode(), e.getErrorCode());
    }
    assertEquals("A failed connectActive must increment the failed counter", failedBefore + 1,
      GlobalClientMetrics.GLOBAL_HA_FAILOVER_CONNECTION_FAILED_COUNTER.getMetric().getValue());
    // Per-group mirror: unique group name → its FAILED counter reflects exactly this one failure.
    HAGroupClientMetricsSource source = HAGroupMetricsManager.getIfPresent(haGroupName);
    assertNotNull("A failed connectActive must register the per-group metrics source", source);
    assertEquals("Per-group HA_FAILOVER_CONNECTION_FAILED_COUNTER must count the failed connect",
      1L, source.getCounterValue(MetricType.HA_FAILOVER_CONNECTION_FAILED_COUNTER));
    HAGroupMetricsManager.remove(haGroupName);
  }

  /**
   * A successful {@code connectActive} must NOT increment
   * {@code HA_FAILOVER_CONNECTION_FAILED_COUNTER}. Guards against the counter being placed on a
   * path that also runs on success (a non-vacuous negative assertion).
   */
  @Test
  public void testConnectActiveSuccessLeavesFailedCounterUnchanged() throws Exception {
    String haGroupName = "testConnectActiveSuccessLeavesFailedCounterUnchanged";
    String url1 = "host1\\:60010";
    String url2 = "host2\\:60010";
    ClusterRoleRecord aActiveBStandby = new ClusterRoleRecord(haGroupName,
      HighAvailabilityPolicy.FAILOVER, url1, ClusterRole.ACTIVE, url2, ClusterRole.STANDBY, 10L);
    HAGroupInfo info = new HAGroupInfo(haGroupName, url1, url2);
    HighAvailabilityGroup group =
      Mockito.spy(new HighAvailabilityGroup(info, new Properties(), aActiveBStandby, State.READY));

    PhoenixConnection conn = Mockito.mock(PhoenixConnection.class);
    Mockito.doReturn(conn).when(group).connectToOneCluster(Mockito.any(String.class),
      Mockito.any(Properties.class), Mockito.any(HAURLInfo.class));
    Mockito.doReturn(true).when(group).isActive(conn);

    long failedBefore =
      GlobalClientMetrics.GLOBAL_HA_FAILOVER_CONNECTION_FAILED_COUNTER.getMetric().getValue();
    assertSame("connectActive must return the established connection", conn,
      group.connectActive(new Properties(), new HAURLInfo(haGroupName)));
    assertEquals("A successful connectActive must not increment the failed counter", failedBefore,
      GlobalClientMetrics.GLOBAL_HA_FAILOVER_CONNECTION_FAILED_COUNTER.getMetric().getValue());
    // Per-group negative check: a successful connect emits no FAILED for this group. Either the
    // source was never created (no emission touched it) or its FAILED counter is still zero.
    HAGroupClientMetricsSource source = HAGroupMetricsManager.getIfPresent(haGroupName);
    assertTrue("A successful connectActive must not emit a per-group failed count", source == null
      || source.getCounterValue(MetricType.HA_FAILOVER_CONNECTION_FAILED_COUNTER) == 0L);
    HAGroupMetricsManager.remove(haGroupName);
  }

  /**
   * The per-group metrics source lifecycle is bound to the HA group: {@code init} pre-registers it
   * (so the {@code ha_group}-tagged series exists as soon as the group is ready) and {@code close}
   * detaches it (freeing the metrics2 source name for a later re-create).
   */
  @Test
  public void testInitRegistersPerGroupSourceAndCloseRemovesIt() throws Exception {
    String haGroupName = "testInitRegistersPerGroupSourceAndCloseRemovesIt";
    String url1 = "host1\\:60010";
    String url2 = "host2\\:60010";
    ClusterRoleRecord record = new ClusterRoleRecord(haGroupName, HighAvailabilityPolicy.FAILOVER,
      url1, ClusterRole.ACTIVE, url2, ClusterRole.STANDBY, 10L);
    HAGroupInfo info = new HAGroupInfo(haGroupName, url1, url2);
    HighAvailabilityGroup group =
      Mockito.spy(new HighAvailabilityGroup(info, new Properties(), record, State.UNINITIALIZED));
    Mockito.doReturn(record).when(group).getClusterRoleRecordFromEndpoint();
    try {
      assertNull("No per-group source should exist before init",
        HAGroupMetricsManager.getIfPresent(haGroupName));
      group.init();
      assertNotNull("init must pre-register the per-group metrics source",
        HAGroupMetricsManager.getIfPresent(haGroupName));
      group.close();
      assertNull("close must detach the per-group metrics source",
        HAGroupMetricsManager.getIfPresent(haGroupName));
    } finally {
      HAGroupMetricsManager.remove(haGroupName);
    }
  }
}
