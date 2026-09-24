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
package org.apache.phoenix.end2end.transform;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.ConnectionQueryServicesImpl;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.transform.SystemTransformRecord;
import org.apache.phoenix.schema.transform.Transform;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Verifies the SYSTEM.TRANSFORM column-add upgrade path in
 * {@link ConnectionQueryServicesImpl#upgradeSystemTransform}. The two lifecycle columns
 * (PENDING_PARTIAL_PASS_UNTIL_TS, CUTOVER_TS) are added by an unconditional, idempotent
 * {@code addColumnsIfNotExists} rather than a timestamp gate, so that a SNAPSHOT cluster whose
 * SYSTEM.TRANSFORM header already advanced past the version timestamp still gets the columns
 * instead of stranding transform reads on a missing-column error. This test pins that contract:
 * running the upgrade on a cluster that already carries the columns takes a snapshot, runs the
 * ungated add as a safe no-op, and leaves the columns intact and readable.
 * <p>
 * Note on coverage: a companion "re-add when the columns are missing" test is deliberately not
 * included. Production never drops these columns, so the only way to simulate the missing state is
 * a test-only DROP COLUMN; and because the upgrade re-adds at exactly
 * {@code MIN_SYSTEM_TABLE_TIMESTAMP_5_4_0} -- which equals the SYSTEM-table upgrade guard ceiling
 * {@code MIN_SYSTEM_TABLE_TIMESTAMP} -- any DROP that actually removes the original column cells
 * must run at that same timestamp, so its delete marker masks the re-add PUT at equal timestamp
 * (HBase delete-wins-at-equal-ts). That collision is a simulation artifact with no production
 * analogue, so re-add coverage is left to the ungated {@code addColumnsIfNotExists} exercised by
 * the TableAlreadyExists branch below.
 * <p>
 * This exercises the same production upgrade helper the real EXECUTE UPGRADE flow calls, so it must
 * boot its own mini-cluster: it mutates the shared SYSTEM.TRANSFORM schema and takes a snapshot of
 * it. As with the other cutover-lifecycle integration tests, the heavy mini-cluster paths are run
 * in CI (they can wedge on some local, e.g. Apple-Silicon, environments during region assignment).
 */
@Category(NeedsOwnMiniClusterTest.class)
public class SystemTransformUpgradeIT extends ParallelStatsDisabledIT {

  private final Properties testProps = PropertiesUtil.deepCopy(TEST_PROPERTIES);

  /**
   * Re-running the upgrade on a cluster that already has the two columns must be a safe no-op: the
   * CREATE throws {@link org.apache.phoenix.schema.TableAlreadyExistsException}, a snapshot is
   * taken, and the idempotent add leaves the columns intact and readable. This is the fresh /
   * SNAPSHOT-cluster path that the gate removal exists to keep safe.
   */
  @Test
  public void testUpgradeIsIdempotentWhenColumnsAlreadyPresent() throws Exception {
    Map<String, String> snapshotMap = new HashMap<>();
    try (PhoenixConnection conn =
      (PhoenixConnection) DriverManager.getConnection(getUrl(), testProps)) {
      conn.setAutoCommit(true);
      ConnectionQueryServicesImpl cqs = (ConnectionQueryServicesImpl) conn.getQueryServices();

      // upgradeSystemTransform closes the connection it is handed (its addColumnsIfNotExists ->
      // addColumn path closes the old meta-connection per that method's contract) and returns a
      // fresh one, so the return must be captured -- reusing the passed-in connection throws
      // CONNECTION_CLOSED. The returned connection is pinned to the system-table upgrade SCN
      // (MIN_SYSTEM_TABLE_TIMESTAMP_5_4_0); it must be closed but must NOT be reused for the
      // round-trip below, since a current-time write/read through it fails the max-lookback-age
      // check. The columns are already present on a fresh cluster, so this must not throw.
      PhoenixConnection upgraded = cqs.upgradeSystemTransform(conn, snapshotMap);
      if (upgraded != conn) {
        upgraded.close();
      }
    }
    assertSnapshotTakenForTransform(snapshotMap);

    // Round-trip on a fresh, current-time connection: the passed-in connection was closed by the
    // upgrade and the returned one is SCN-pinned to the upgrade timestamp, so neither can serve a
    // current-time write/read of SYSTEM.TRANSFORM.
    try (PhoenixConnection conn =
      (PhoenixConnection) DriverManager.getConnection(getUrl(), testProps)) {
      conn.setAutoCommit(true);
      conn.getQueryServices().clearCache();
      assertColumnsRoundTrip(conn);
    }
  }

  /** Asserts the upgrade took a snapshot of SYSTEM.TRANSFORM before altering it. */
  private static void assertSnapshotTakenForTransform(Map<String, String> snapshotMap) {
    assertFalse("The upgrade must snapshot SYSTEM.TRANSFORM before adding columns",
      snapshotMap.isEmpty());
    assertTrue(
      "The snapshot map must key on the SYSTEM.TRANSFORM physical name, was " + snapshotMap,
      snapshotMap.keySet().stream().anyMatch(k -> k.contains("TRANSFORM")));
  }

  /**
   * Round-trips a transform record through both new BIGINT columns to prove they are present,
   * writable, and readable after the upgrade.
   */
  private void assertColumnsRoundTrip(PhoenixConnection conn) throws SQLException {
    String logicalTableName = generateUniqueName();
    long pendingUntil = 4242L;
    long cutover = 1000L;

    SystemTransformRecord.SystemTransformBuilder builder =
      new SystemTransformRecord.SystemTransformBuilder();
    builder.setLogicalTableName(logicalTableName);
    builder.setNewPhysicalTableName(logicalTableName + "_1");
    builder.setTransformType(PTable.TransformType.METADATA_TRANSFORM);
    builder.setTransformStatus(PTable.TransformStatus.PENDING_PARTIAL_PASS.name());
    builder.setPendingPartialPassUntilTs(pendingUntil);
    builder.setCutoverTs(cutover);
    Transform.upsertTransform(builder.build(), conn);

    SystemTransformRecord readBack =
      Transform.getTransformRecord(null, logicalTableName, null, null, conn);
    assertNotNull("The transform record must read back after the upgrade", readBack);
    assertEquals("PENDING_PARTIAL_PASS_UNTIL_TS must round-trip", Long.valueOf(pendingUntil),
      readBack.getPendingPartialPassUntilTs());
    assertEquals("CUTOVER_TS must round-trip", Long.valueOf(cutover), readBack.getCutoverTs());
  }
}
