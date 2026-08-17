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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.sql.SQLException;
import java.util.Properties;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.ClusterRoleRecord.ClusterRole;
import org.junit.Test;
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
    // cannot route a connection, and the non-active poller resolves the true state on its next
    // tick. unknownV11 (url1 UNKNOWN, url2 STANDBY, no active) is newer than v9 but not routable.
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
}
