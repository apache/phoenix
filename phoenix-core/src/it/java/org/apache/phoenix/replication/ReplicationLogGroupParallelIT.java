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
package org.apache.phoenix.replication;

import static org.apache.phoenix.query.BaseTest.generateUniqueName;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.jdbc.HighAvailabilityPolicy;
import org.apache.phoenix.jdbc.ParallelPhoenixConnection;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * PARALLEL-policy coverage for the write-path role/policy branch in
 * {@code IndexRegionObserver.getReplicationLogGroup}. Under PARALLEL the client double-writes to
 * both clusters via a {@link ParallelPhoenixConnection}; the leg that lands on the non-active
 * cluster must commit locally and skip re-ship (the {@code isParallelPolicy} branch returns empty)
 * rather than reject the write as split-brain — which is the FAILOVER behavior covered by
 * {@link ReplicationLogGroupIT}. Both clusters run with the replication feature enabled, so the
 * standby actually reaches this branch.
 */
@Category(NeedsOwnMiniClusterTest.class)
public class ReplicationLogGroupParallelIT extends ReplicationLogGroupBaseIT {

  @BeforeClass
  public static void doSetup() throws Exception {
    setupClusters();
  }

  @Override
  protected HighAvailabilityPolicy getHAPolicy() {
    return HighAvailabilityPolicy.PARALLEL;
  }

  @Test
  public void testParallelDoubleWriteCommitsOnNonActiveClusterWithoutSplitBrainThrow()
    throws Exception {
    final String tableName = "T_" + generateUniqueName();
    final int rowCount = 10;

    // Create the table on both clusters (schema: id INTEGER PK, v INTEGER). replicationScope=false
    // disables HBase-native replication, so cluster2's rows come only from the parallel
    // double-write's standby leg -- isolating the PARALLEL branch under test.
    CLUSTERS.createTableOnClusterPair(haGroup, tableName, false);

    // PARALLEL policy => a ParallelPhoenixConnection that writes to both clusters. The leg landing
    // on the non-active cluster must commit locally (isParallelPolicy branch), not throw
    // split-brain.
    try (Connection conn = DriverManager.getConnection(CLUSTERS.getJdbcHAUrl(), clientProps)) {
      assertTrue("expected a ParallelPhoenixConnection under PARALLEL policy",
        conn instanceof ParallelPhoenixConnection);
      PreparedStatement stmt = conn.prepareStatement("upsert into " + tableName + " values (?, ?)");
      for (int i = 0; i < rowCount; i++) {
        stmt.setInt(1, i);
        stmt.setInt(2, i);
        stmt.executeUpdate();
      }
      conn.commit();
    }

    // Rows present on BOTH clusters => the double-write committed on the non-active cluster too,
    // i.e. its PARALLEL leg took the commit-locally path rather than a split-brain rejection.
    assertRowCount(CLUSTERS.getCluster1Connection(haGroup), tableName, rowCount);
    assertRowCount(CLUSTERS.getCluster2Connection(haGroup), tableName, rowCount);
  }

  private static void assertRowCount(Connection conn, String tableName, int expected)
    throws Exception {
    try (Connection c = conn;
      ResultSet rs = c.createStatement().executeQuery("select count(*) from " + tableName)) {
      assertTrue(rs.next());
      assertEquals(expected, rs.getInt(1));
    }
  }
}
