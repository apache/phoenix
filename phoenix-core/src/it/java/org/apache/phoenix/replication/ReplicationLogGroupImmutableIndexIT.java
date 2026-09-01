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

import static org.apache.phoenix.hbase.index.IndexRegionObserver.PHOENIX_INDEX_CDC_CONSUMER_ENABLED;
import static org.apache.phoenix.query.BaseTest.generateUniqueName;
import static org.apache.phoenix.query.QueryServices.SERVER_SIDE_IMMUTABLE_INDEXES_ENABLED_ATTRIB;
import static org.apache.phoenix.replication.CrossClusterReplicationTestUtil.findLogFiles;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.jdbc.FailoverPhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixResultSet;
import org.apache.phoenix.replication.reader.ReplicationLogProcessor;
import org.apache.phoenix.util.QueryUtil;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Replication coverage for server-side immutable-index maintenance
 * ({@code phoenix.server.side.immutable.indexes.enabled}).
 * <p>
 * CCF replicates only data-table mutations; secondary-index tables have no capture coprocessor, so
 * an index is never shipped as index-table entries. The standby instead regenerates the index from
 * the replayed data mutations via {@code IndexRegionObserver} — but only for batches the active
 * marked as server-maintained. For an IMMUTABLE table that requires this config: when it is enabled
 * the client defers index maintenance to the server, the active stamps the data batch for standby
 * regeneration, and the standby rebuilds the index. When it is disabled the client maintains the
 * immutable index itself; those writes go to the active's index table only (never replicated) and
 * the data batch is not marked for regeneration, so the standby index is left empty after failover
 * — verified by flipping this flag on the client, which leaves the {@code indexName} scan on
 * cluster 2 empty and fails the cross-cluster index equality below.
 * <p>
 * The flag is read from the client connection config in {@code MutationState}, so it is set on the
 * client properties here (as {@code ServerSideImmutableIndexIT}/{@code ClientSideImmutableIndexIT}
 * do), not on the server {@code conf1}/{@code conf2}. The sibling mutable-table case is
 * {@code ReplicationLogGroupIT#testIndexRegenerationOnStandby}; existing immutable-index coverage
 * ({@code ServerSideImmutableIndexIT}) is single-cluster only.
 */
@Category(NeedsOwnMiniClusterTest.class)
public class ReplicationLogGroupImmutableIndexIT extends ReplicationLogGroupBaseIT {

  @BeforeClass
  public static void doSetup() throws Exception {
    // Match the sibling IT: the downstream CDC consumer is out of scope here.
    conf1.setBoolean(PHOENIX_INDEX_CDC_CONSUMER_ENABLED, false);
    conf2.setBoolean(PHOENIX_INDEX_CDC_CONSUMER_ENABLED, false);
    setupClusters();
  }

  /**
   * With server-side immutable-index maintenance enabled on the writing client, an IMMUTABLE
   * table's global index is (a) never present as index-table mutations in the replication log
   * (structural: index tables carry no capture coprocessor) and (b) regenerated on the standby by
   * {@code IndexRegionObserver} from the replayed data mutations. The standby index being fully
   * populated (assertions at the end) is the config-gated behavior: with the flag disabled on the
   * client the standby index would be empty and the cross-cluster index equality would fail.
   */
  @Test
  public void testImmutableIndexRegeneratedOnStandby() throws Exception {
    final String tableName = "T_" + generateUniqueName();
    final String indexName = "I_" + generateUniqueName();
    final int rowCount = 10;

    // The flag is a client-side decision (read in MutationState from the connection config), so it
    // must be set on the client props, not on the server conf1/conf2.
    clientProps.setProperty(SERVER_SIDE_IMMUTABLE_INDEXES_ENABLED_ATTRIB, Boolean.toString(true));

    // Create an immutable table + global index on cluster 1 and insert data. The default storage
    // scheme for IMMUTABLE_ROWS=true is inherited by the index, so schemes match and the flag
    // genuinely governs client- vs server-side maintenance.
    try (FailoverPhoenixConnection conn = (FailoverPhoenixConnection) DriverManager
      .getConnection(CLUSTERS.getJdbcHAUrl(), clientProps)) {
      conn.createStatement()
        .execute(String.format("CREATE TABLE %s (ID1 INTEGER NOT NULL, ID2 INTEGER NOT NULL, "
          + "VAL1 VARCHAR CONSTRAINT PK PRIMARY KEY (ID1, ID2)) IMMUTABLE_ROWS=true", tableName));
      conn.createStatement()
        .execute(String.format("CREATE INDEX %s ON %s (VAL1)", indexName, tableName));
      conn.commit();
      PreparedStatement stmt =
        conn.prepareStatement("UPSERT INTO " + tableName + " VALUES(?, ?, ?)");
      for (int i = 0; i < rowCount; i++) {
        stmt.setInt(1, i);
        stmt.setInt(2, i);
        stmt.setString(3, "val_" + i);
        stmt.executeUpdate();
      }
      conn.commit();
    }

    // Capture the standby log dir before closing the group, then confirm the log carries data-table
    // mutations only. The absence of index-table mutations is structural (index tables have no
    // capture coprocessor), not the config gate.
    Path standByLogDir = logGroup.getOrCreatePeerShardManager().getRootDirectoryPath();
    logGroup.close();
    Map<String, List<Mutation>> logsByTable = groupLogsByTable();
    dumpTableLogCount(logsByTable);
    assertTrue("Replication log should contain data table mutations",
      logsByTable.containsKey(tableName));
    assertFalse("Replication log is data-table-only; index tables are never captured",
      logsByTable.containsKey(indexName));

    // Recreate the schema on cluster 2 and replay the log there.
    try (Connection conn2 = CLUSTERS.getCluster2Connection(haGroup)) {
      conn2.createStatement().execute(
        String.format("CREATE TABLE IF NOT EXISTS %s (ID1 INTEGER NOT NULL, ID2 INTEGER NOT NULL, "
          + "VAL1 VARCHAR CONSTRAINT PK PRIMARY KEY (ID1, ID2)) IMMUTABLE_ROWS=true", tableName));
      conn2.createStatement()
        .execute(String.format("CREATE INDEX IF NOT EXISTS %s ON %s (VAL1)", indexName, tableName));
      conn2.commit();
    }

    FileSystem fs = standByLogDir.getFileSystem(conf2);
    List<Path> logFiles = findLogFiles(standByLogDir, fs);
    LOG.info("Found {} log files to replay", logFiles.size());
    assertTrue("Should have at least one log file", !logFiles.isEmpty());
    ReplicationLogProcessor processor = ReplicationLogProcessor.get(conf2, haGroupName);
    try {
      for (Path logFile : logFiles) {
        LOG.info("Replaying log file: {}", logFile);
        processor.processLogFile(fs, logFile);
      }
    } finally {
      processor.close();
    }

    // Config gate: the standby index must have been regenerated from the replayed data mutations by
    // IndexRegionObserver. With server-side maintenance disabled on the client this would be empty.
    try (Connection conn2 = CLUSTERS.getCluster2Connection(haGroup);
      Statement stmt = conn2.createStatement()) {
      try (ResultSet rs = stmt.executeQuery("SELECT /*+ NO_INDEX */ COUNT(*) FROM " + tableName)) {
        assertTrue(rs.next());
        assertEquals("Data table on cluster 2 should have all rows", rowCount, rs.getInt(1));
      }
      try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + tableName)) {
        PhoenixResultSet prs = rs.unwrap(PhoenixResultSet.class);
        String explainPlan = QueryUtil.getExplainPlan(prs.getUnderlyingIterator());
        assertTrue("Query should be served by the index on cluster 2",
          explainPlan.contains(indexName));
        assertTrue(rs.next());
        assertEquals("Standby index should be regenerated with all rows (server-side maintenance)",
          rowCount, rs.getInt(1));
      }
    }

    // Deep cell-level equality of both the data table and the regenerated index across clusters.
    // The index equality is the strong gate: an empty standby index (flag disabled) fails here.
    assertTablesEqualAcrossClusters(tableName);
    assertTablesEqualAcrossClusters(indexName);
  }
}
