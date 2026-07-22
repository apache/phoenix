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
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CHILD_LINK_NAME;
import static org.apache.phoenix.query.BaseTest.generateUniqueName;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.hbase.index.IndexRegionObserver;
import org.apache.phoenix.jdbc.FailoverPhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixResultSet;
import org.apache.phoenix.query.PhoenixTestBuilder;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.replication.reader.ReplicationLogProcessor;
import org.apache.phoenix.util.CDCUtil;
import org.apache.phoenix.util.QueryUtil;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;

@Category(NeedsOwnMiniClusterTest.class)
public class ReplicationLogGroupIT extends ReplicationLogGroupBaseIT {

  @BeforeClass
  public static void doSetup() throws Exception {
    // Disable the IndexCDCConsumer on both clusters: CDC-index regeneration is verified directly,
    // and the consumer's downstream secondary-index convergence is out of scope for these tests.
    conf1.setBoolean(PHOENIX_INDEX_CDC_CONSUMER_ENABLED, false);
    conf2.setBoolean(PHOENIX_INDEX_CDC_CONSUMER_ENABLED, false);
    setupClusters();
  }

  @Test
  public void testUpsertSelectReplicatesViaCloneConnection() throws Exception {
    final String sourceTable = "T_" + generateUniqueName();
    final String targetTable = "T_" + generateUniqueName();
    final String createSourceDdl = String.format(
      "create table if not exists %s (id integer not null primary key, val varchar)", sourceTable);
    final String createTargetDdl = String.format(
      "create table if not exists %s (id integer not null primary key, val varchar)", targetTable);

    // Must exceed the default MUTATE_BATCH_SIZE (100) so the in-loop flush at
    // UpsertCompiler.upsertSelect line ~288 fires. That flush calls send() on the cloned
    // connection's MutationState — the only path where the missing haGroup field is observable.
    // For smaller row counts the chunk's mutations are joined back to the parent and the parent's
    // commit does the annotation with its (non-null) haGroup, so the bug stays hidden.
    final int rowCount = 250;
    try (FailoverPhoenixConnection conn = (FailoverPhoenixConnection) DriverManager
      .getConnection(CLUSTERS.getJdbcHAUrl(), clientProps)) {
      conn.createStatement().execute(createSourceDdl);
      conn.createStatement().execute(createTargetDdl);
      PreparedStatement insert =
        conn.prepareStatement("upsert into " + sourceTable + " values (?, ?)");
      for (int i = 0; i < rowCount; i++) {
        insert.setInt(1, i);
        insert.setString(2, "v" + i);
        insert.executeUpdate();
      }
      conn.commit();

      conn.setAutoCommit(true);
      conn.createStatement()
        .execute("upsert into " + targetTable + " select id, val from " + sourceTable);
    }

    Map<String, Integer> expected = Maps.newHashMap();
    expected.put(sourceTable, rowCount); // direct upserts on the parent connection
    expected.put(targetTable, rowCount); // upsert-select rows
    expected.put(SYSTEM_CATALOG_NAME, 0);
    expected.put(SYSTEM_CHILD_LINK_NAME, 0);
    verifyReplication(expected);
  }

  @Test
  public void testAppendAndSync() throws Exception {
    final String tableName = "T_" + generateUniqueName();
    final String indexName1 = "I_" + generateUniqueName();
    final String indexName2 = "I_" + generateUniqueName();
    final String indexName3 = "L_" + generateUniqueName();
    final String indexName4 = "U_" + generateUniqueName();
    String createTableDdl = String.format("create table if not exists %s (id1 integer not null, "
      + "id2 integer not null, val1 varchar, val2 varchar "
      + "constraint pk primary key (id1, id2))", tableName);
    String createIndex1Ddl = String
      .format("create index if not exists %s on %s (val1) include (val2)", indexName1, tableName);
    String createIndex2Ddl = String
      .format("create index if not exists %s on %s (val2) include (val1)", indexName2, tableName);
    String createLocalIndexDdl = String.format(
      "create local index if not exists %s on %s (id2,val1) include (val2)", indexName3, tableName);
    // Uncovered index (no INCLUDE): rows are written UNVERIFIED in PRE and never marked VERIFIED in
    // POST (IRO:2248), so the reader joins back to the data table. Both clusters write only
    // UNVERIFIED rows, so cross-cluster cell equality still holds.
    String createUncoveredIndexDdl =
      String.format("create uncovered index if not exists %s on %s (val1)", indexName4, tableName);

    try (FailoverPhoenixConnection conn = (FailoverPhoenixConnection) DriverManager
      .getConnection(CLUSTERS.getJdbcHAUrl(), clientProps)) {
      conn.createStatement().execute(createTableDdl);
      conn.createStatement().execute(createIndex1Ddl);
      conn.createStatement().execute(createIndex2Ddl);
      conn.createStatement().execute(createLocalIndexDdl);
      conn.createStatement().execute(createUncoveredIndexDdl);
      conn.commit();
      PreparedStatement stmt =
        conn.prepareStatement("upsert into " + tableName + " VALUES(?, ?, ?, ?)");
      // upsert 50 rows
      for (int i = 0; i < 5; ++i) {
        for (int j = 0; j < 10; ++j) {
          stmt.setInt(1, i);
          stmt.setInt(2, j);
          stmt.setString(3, "abcdefghijklmnopqrstuvwxyz");
          stmt.setString(4, null);
          stmt.executeUpdate();
        }
        conn.commit();
      }

      // Update existing rows changing only the covered column val2 (val1 unchanged). With cell
      // coalescing each phase's index cells share one record, so this exercises:
      // index1 (on val1, includes val2): index row key UNCHANGED -> PRE unverified Put and POST
      // verified Put target the SAME index row, i.e. two writes to the same empty-column
      // qualifier (UNVERIFIED then VERIFIED) split across the PRE and POST records.
      // index2 (on val2): index row key CHANGES (null -> value) -> Delete(oldKey)+Put(newKey).
      PreparedStatement updateVal2 =
        conn.prepareStatement("upsert into " + tableName + " (id1, id2, val2) VALUES(?, ?, ?)");
      for (int i = 0; i < 5; ++i) {
        for (int j = 0; j < 10; ++j) {
          updateVal2.setInt(1, i);
          updateVal2.setInt(2, j);
          updateVal2.setString(3, "val2_" + i + "_" + j);
          updateVal2.executeUpdate();
        }
      }
      conn.commit();

      // Update existing rows changing the indexed column val1 (val2 unchanged). This flips the
      // roles relative to the previous pass:
      // index1 (on val1): index row key CHANGES -> the PRE record makes the old index row
      // unverified (Put) and the new index row unverified (Put), while the POST record holds a
      // verified Put on the new key and a Delete on the old key -- a Put and a Delete on
      // DIFFERENT rows within one coalesced record, which the grouper must split on the
      // row+type boundary.
      // index2 (on val2): index row key UNCHANGED -> PRE unverified + POST verified on same row.
      PreparedStatement updateVal1 =
        conn.prepareStatement("upsert into " + tableName + " (id1, id2, val1) VALUES(?, ?, ?)");
      for (int i = 0; i < 5; ++i) {
        for (int j = 0; j < 10; ++j) {
          updateVal1.setInt(1, i);
          updateVal1.setInt(2, j);
          updateVal1.setString(3, "newval1_" + i + "_" + j);
          updateVal1.executeUpdate();
        }
      }
      conn.commit();

      // do some atomic upserts which will be ignored and therefore not replicated
      stmt = conn.prepareStatement(
        "upsert into " + tableName + " VALUES(?, ?, ?) " + "ON DUPLICATE KEY IGNORE");
      conn.setAutoCommit(true);
      for (int i = 0; i < 5; ++i) {
        for (int j = 0; j < 2; ++j) {
          stmt.setInt(1, i);
          stmt.setInt(2, j);
          stmt.setString(3, null);
          assertEquals(0, stmt.executeUpdate());
        }
      }

      // Assert the headline invariant of this feature: the index physical tables receive ZERO
      // replication records (the standby regenerates index entries from the data record). System
      // tables likewise never replicate. We deliberately do NOT assert exact data mutation totals
      // here: the multi-pass update workload's per-table counts are dominated by index-maintenance
      // internals (local-index key churn, verified/unverified empty-column writes) rather than by
      // the coalescing under test, and coalescing is mutation-count invariant by construction. The
      // authoritative correctness check for this workload is the cross-cluster cell-level equality
      // below; the record-count contract of coalescing is pinned separately in
      // testSingleBatchRecordCount.
      Map<String, Integer> expected = Maps.newHashMap();
      expected.put(SYSTEM_CATALOG_NAME, 0);
      expected.put(SYSTEM_CHILD_LINK_NAME, 0);
      expected.put(indexName1, 0);
      expected.put(indexName2, 0);
      expected.put(indexName4, 0);
      verifyReplication(expected);

      // Replay on cluster 2 and verify cross-cluster cell-level equality
      replayAndVerifyAcrossClusters(Arrays.asList(createTableDdl, createIndex1Ddl, createIndex2Ddl,
        createLocalIndexDdl, createUncoveredIndexDdl), tableName, indexName1, indexName2,
        indexName4);
    }
  }

  /**
   * Uncovered-index-only variant exercising the path where the active NEVER calls
   * {@code getCurrentRowStates}. The skip at {@code IndexRegionObserver:2513-2519} fires when the
   * only index is uncovered and every mutation already carries the indexed column
   * ({@code isPartialUncoveredIndexMutation == false}). With nothing read into
   * {@code dataRowStates}, the active ships NO pre-image cell; the standby receives a
   * self-contained full mutation, takes the same skip, and regenerates the uncovered index purely
   * from the data cells. {@link #testAppendAndSync}'s uncovered index cannot reach this path: that
   * table has a global index (forcing the read) and does val2-only updates (partial w.r.t. the val1
   * index).
   */
  @Test
  public void testUncoveredIndexNoCurrentRowState() throws Exception {
    final String tableName = "T_" + generateUniqueName();
    final String indexName = "U_" + generateUniqueName();
    String createTableDdl = String.format(
      "create table if not exists %s (id integer not null primary key, val1 varchar, val2 varchar)",
      tableName);
    String createIndexDdl =
      String.format("create uncovered index if not exists %s on %s (val1)", indexName, tableName);

    try (FailoverPhoenixConnection conn = (FailoverPhoenixConnection) DriverManager
      .getConnection(CLUSTERS.getJdbcHAUrl(), clientProps)) {
      conn.createStatement().execute(createTableDdl);
      conn.createStatement().execute(createIndexDdl);
      conn.commit();

      // Full upserts (all columns present), so the indexed column val1 is always supplied and the
      // batch stays non-partial, keeping the active on the skip path.
      PreparedStatement stmt =
        conn.prepareStatement("upsert into " + tableName + " VALUES(?, ?, ?)");
      int rowCount = 10;
      for (int i = 0; i < rowCount; ++i) {
        stmt.setInt(1, i);
        stmt.setString(2, "val1_" + i);
        stmt.setString(3, "val2_" + i);
        stmt.executeUpdate();
      }
      conn.commit();

      // Re-upsert the same rows as full mutations (val1 unchanged, val2 changed) so the index key
      // is stable and the batch remains non-partial.
      for (int i = 0; i < rowCount; ++i) {
        stmt.setInt(1, i);
        stmt.setString(2, "val1_" + i);
        stmt.setString(3, "val2_updated_" + i);
        stmt.executeUpdate();
      }
      conn.commit();

      // The uncovered index physical table must receive zero replication records.
      Map<String, Integer> expected = Maps.newHashMap();
      expected.put(SYSTEM_CATALOG_NAME, 0);
      expected.put(SYSTEM_CHILD_LINK_NAME, 0);
      expected.put(indexName, 0);
      verifyReplication(expected);

      replayAndVerifyAcrossClusters(Arrays.asList(createTableDdl, createIndexDdl), tableName,
        indexName);
    }
  }

  /**
   * Local-index-only variant of {@link #testAppendAndSync}: the table has a local index but no
   * global/uncovered/transform index. Such a table never enters the global-index branch that
   * captures and ships the per-row PRE_IMAGE, so the active must capture it from the local-index
   * prior-row scan instead (see {@code IndexRegionObserver.captureLocalIndexPreImageCells});
   * without that the standby's {@code PreImageLocalTable} would have no prior state and would miss
   * covered columns and old-key tombstones. {@code testAppendAndSync} has both index types and so
   * would not exercise this path. Local-index cells live in the data table's own {@code L#0}
   * family, so verifying the data table cross-cluster also verifies the regenerated local index.
   */
  @Test
  public void testLocalIndexOnly() throws Exception {
    final String tableName = "T_" + generateUniqueName();
    final String localIndexName = "L_" + generateUniqueName();
    String createTableDdl = String.format("create table if not exists %s (id1 integer not null, "
      + "id2 integer not null, val1 varchar, val2 varchar "
      + "constraint pk primary key (id1, id2))", tableName);
    String createLocalIndexDdl =
      String.format("create local index if not exists %s on %s (id2,val1) include (val2)",
        localIndexName, tableName);

    try (FailoverPhoenixConnection conn = (FailoverPhoenixConnection) DriverManager
      .getConnection(CLUSTERS.getJdbcHAUrl(), clientProps)) {
      conn.createStatement().execute(createTableDdl);
      conn.createStatement().execute(createLocalIndexDdl);
      conn.commit();

      // Insert 50 rows (val2 null) across 5 commits.
      PreparedStatement stmt =
        conn.prepareStatement("upsert into " + tableName + " VALUES(?, ?, ?, ?)");
      for (int i = 0; i < 5; ++i) {
        for (int j = 0; j < 10; ++j) {
          stmt.setInt(1, i);
          stmt.setInt(2, j);
          stmt.setString(3, "abcdefghijklmnopqrstuvwxyz");
          stmt.setString(4, null);
          stmt.executeUpdate();
        }
        conn.commit();
      }

      // Update the covered column val2 only (local index row key unchanged): exercises carrying a
      // covered cell forward from the pre-image.
      PreparedStatement updateVal2 =
        conn.prepareStatement("upsert into " + tableName + " (id1, id2, val2) VALUES(?, ?, ?)");
      for (int i = 0; i < 5; ++i) {
        for (int j = 0; j < 10; ++j) {
          updateVal2.setInt(1, i);
          updateVal2.setInt(2, j);
          updateVal2.setString(3, "val2_" + i + "_" + j);
          updateVal2.executeUpdate();
        }
      }
      conn.commit();

      // Update the indexed column val1 (local index row key CHANGES): exercises the old-key
      // DeleteFamily tombstone, which requires the prior row state from the pre-image.
      PreparedStatement updateVal1 =
        conn.prepareStatement("upsert into " + tableName + " (id1, id2, val1) VALUES(?, ?, ?)");
      for (int i = 0; i < 5; ++i) {
        for (int j = 0; j < 10; ++j) {
          updateVal1.setInt(1, i);
          updateVal1.setInt(2, j);
          updateVal1.setString(3, "newval1_" + i + "_" + j);
          updateVal1.executeUpdate();
        }
      }
      conn.commit();

      // Delete some rows: exercises full-row DeleteFamily on both data and local index.
      PreparedStatement deleteStmt =
        conn.prepareStatement("delete from " + tableName + " where id1 = ? and id2 = ?");
      for (int j = 0; j < 10; ++j) {
        deleteStmt.setInt(1, 0);
        deleteStmt.setInt(2, j);
        deleteStmt.executeUpdate();
      }
      conn.commit();

      // Replay on cluster 2 and verify cross-cluster cell-level equality. The data table scan
      // covers the L#0 local-index family, so this verifies the regenerated local index too. A
      // local index shares the data region, so there is no separate index physical table to assert
      // zero replication records on -- cross-cluster equality is the whole check.
      replayAndVerifyAcrossClusters(Arrays.asList(createTableDdl, createLocalIndexDdl), tableName);
    }
  }

  /**
   * Pins the per-batch coalescing contract: one server-side batch on a table with one global index
   * and one local index emits exactly one log record -- the coalesced data-table cell stream
   * carrying the per-row pre-image -- regardless of how many rows the batch contains. Neither index
   * is replicated; the standby regenerates both from the data record plus its pre-image, so the
   * global index table has no replication records of its own and the captured data-table cell
   * stream carries no local-index ({@code L#}) cells. This is the explicit must-have check for the
   * global + local scenario: local-index updates run after pre-image capture, so they are never in
   * the shipped stream and the standby cannot double-write them. Cross-cluster cell equality then
   * confirms the single collapsed record still reconstructs the correct data, global index, and (in
   * the data table's own {@code L#0} family) local index on the standby.
   */
  @Test
  public void testSingleBatchRecordCount() throws Exception {
    final String tableName = "T_" + generateUniqueName();
    final String indexName = "I_" + generateUniqueName();
    final String localIndexName = "L_" + generateUniqueName();
    String createTableDdl = String.format(
      "create table if not exists %s (id integer not null primary key, val1 varchar, val2 varchar)",
      tableName);
    String createIndexDdl = String
      .format("create index if not exists %s on %s (val1) include (val2)", indexName, tableName);
    String createLocalIndexDdl = String.format(
      "create local index if not exists %s on %s (val2) include (val1)", localIndexName, tableName);

    try (FailoverPhoenixConnection conn = (FailoverPhoenixConnection) DriverManager
      .getConnection(CLUSTERS.getJdbcHAUrl(), clientProps)) {
      conn.createStatement().execute(createTableDdl);
      conn.createStatement().execute(createIndexDdl);
      conn.createStatement().execute(createLocalIndexDdl);
      conn.commit();

      // Insert several rows and commit them as a SINGLE batch (autocommit off, one commit()). All
      // rows in this batch coalesce into one data-table record; index entries are regenerated on
      // the standby, so the index table contributes no replication records.
      PreparedStatement stmt =
        conn.prepareStatement("upsert into " + tableName + " VALUES(?, ?, ?)");
      int rowCount = 5;
      for (int i = 0; i < rowCount; ++i) {
        stmt.setInt(1, i);
        stmt.setString(2, "v1_" + i);
        stmt.setString(3, "v2_" + i);
        stmt.executeUpdate();
      }
      conn.commit();

      // Flush the log group so the standby files are complete, then count records per table.
      logGroup.close();
      Map<String, Integer> recordsByTable = countRecordsByTable();
      LOG.info("Records by table: {}", recordsByTable);
      assertEquals("Data table should have exactly one coalesced record for the batch",
        Integer.valueOf(1), recordsByTable.get(tableName));
      assertNull("Global index table should have no replication records; entries are regenerated"
        + " standby", recordsByTable.get(indexName));

      // The captured data-table cell stream must carry no local-index (L#) cells: local-index
      // updates run after pre-image capture and so are never shipped. The standby regenerates them.
      Map<String, List<Mutation>> logsByTable = groupLogsByTable();
      for (Mutation m : logsByTable.get(tableName)) {
        for (Cell cell : m.getFamilyCellMap().values().stream().flatMap(List::stream)
          .collect(Collectors.toList())) {
          String family = Bytes.toString(CellUtil.cloneFamily(cell));
          assertFalse(
            "Captured data record must not contain local-index (L#) cells, found family " + family,
            family.startsWith(QueryConstants.LOCAL_INDEX_COLUMN_FAMILY_PREFIX));
        }
      }

      // Replay on cluster 2 and verify cross-cluster cell-level equality. Verifying the data table
      // also verifies the regenerated local index, whose cells live in the data table's L#0 family.
      replayAndVerifyAcrossClusters(
        Arrays.asList(createTableDdl, createIndexDdl, createLocalIndexDdl), tableName, indexName);
    }
  }

  @Test
  public void testAppendAndSyncNoIndex() throws Exception {
    final String tableName = "T_" + generateUniqueName();
    // Multiple column families: cf1 and cf2
    String createTableDdl = String.format(
      "create table if not exists %s (id1 integer not null, "
        + "id2 integer not null, cf1.val1 varchar, cf1.val2 varchar, "
        + "cf2.val3 varchar, cf2.val4 integer " + "constraint pk primary key (id1, id2))",
      tableName);

    try (FailoverPhoenixConnection conn = (FailoverPhoenixConnection) DriverManager
      .getConnection(CLUSTERS.getJdbcHAUrl(), clientProps)) {
      conn.createStatement().execute(createTableDdl);
      conn.commit();

      // upsert 50 rows across multiple column families
      PreparedStatement stmt =
        conn.prepareStatement("upsert into " + tableName + " VALUES(?, ?, ?, ?, ?, ?)");
      int rowCount = 50;
      for (int i = 0; i < 5; ++i) {
        for (int j = 0; j < 10; ++j) {
          stmt.setInt(1, i);
          stmt.setInt(2, j);
          stmt.setString(3, "abcdefghijklmnopqrstuvwxyz");
          stmt.setString(4, null);
          stmt.setString(5, "val3_" + i + "_" + j);
          stmt.setInt(6, i * 10 + j);
          stmt.executeUpdate();
        }
        conn.commit();
      }

      // Delete some rows
      PreparedStatement deleteStmt =
        conn.prepareStatement("delete from " + tableName + " where id1 = ? and id2 = ?");
      int deleteCount = 10;
      for (int j = 0; j < 10; ++j) {
        deleteStmt.setInt(1, 0);
        deleteStmt.setInt(2, j);
        deleteStmt.executeUpdate();
      }
      conn.commit();

      // verify replication mutation counts
      Map<String, Integer> expected = Maps.newHashMap();
      // Each upsert produces Put + Delete (for null columns), row deletes produce DeleteFamily
      expected.put(tableName, rowCount * 2 + deleteCount);
      verifyReplication(expected);

      // Replay on cluster 2 and verify cross-cluster cell-level equality
      replayAndVerifyAcrossClusters(Collections.singletonList(createTableDdl), tableName);
    }
  }

  /**
   * Verifies cross-cluster cell-level equality after replay when ON DUPLICATE KEY UPDATE rewrites a
   * row. The atomic upsert path produces a Put (and optionally a Delete with DeleteColumn cells)
   * that flow through the coprocessor merge path the codec must round-trip correctly.
   */
  @Test
  public void testOnDuplicateKeyUpdate() throws Exception {
    final String tableName = "T_" + generateUniqueName();
    String createTableDdl = String.format("create table if not exists %s "
      + "(pk varchar primary key, counter1 bigint, counter2 varchar)", tableName);

    try (FailoverPhoenixConnection conn = (FailoverPhoenixConnection) DriverManager
      .getConnection(CLUSTERS.getJdbcHAUrl(), clientProps)) {
      conn.createStatement().execute(createTableDdl);
      conn.commit();

      // Initial inserts for 5 distinct rows
      PreparedStatement insert =
        conn.prepareStatement("upsert into " + tableName + " VALUES(?, 0, 'init')");
      for (int i = 0; i < 5; ++i) {
        insert.setString(1, "row_" + i);
        insert.executeUpdate();
      }
      conn.commit();

      // ON DUPLICATE KEY UPDATE — increment counter1 and update counter2 a few times per row.
      // Each invocation against an existing row triggers the atomic upsert path which generates
      // Put (and possibly Delete) mutations on the server side and merges CP cells.
      String dml = "UPSERT INTO " + tableName + " VALUES(?, 0, ?) "
        + "ON DUPLICATE KEY UPDATE counter1 = counter1 + 1, counter2 = ?";
      PreparedStatement update = conn.prepareStatement(dml);
      conn.setAutoCommit(true);
      for (int round = 0; round < 3; ++round) {
        for (int i = 0; i < 5; ++i) {
          update.setString(1, "row_" + i);
          update.setString(2, "v" + round);
          update.setString(3, "v" + round);
          update.executeUpdate();
        }
      }

      // Set some columns to null via ON DUPLICATE KEY UPDATE — generates DeleteColumn cells
      String dmlNullify =
        "UPSERT INTO " + tableName + " VALUES(?, 0, '') ON DUPLICATE KEY UPDATE counter2 = NULL";
      PreparedStatement nullify = conn.prepareStatement(dmlNullify);
      for (int i = 0; i < 5; ++i) {
        nullify.setString(1, "row_" + i);
        nullify.executeUpdate();
      }

      // Replay on cluster 2 and verify cross-cluster cell-level equality
      replayAndVerifyAcrossClusters(Collections.singletonList(createTableDdl), tableName);
    }
  }

  /**
   * Atomic + global index: ON DUPLICATE KEY UPDATE on a table that also has a global index covering
   * the mutated column. The active resolves the on-dup before pre-image capture, so the
   * post-resolution data cells (including any DeleteColumn cells the on-dup generates) are captured
   * into the row's {@code (row, ts)} group with their pre-image; the global index is not
   * replicated. On the standby the reconstructed mutations carry no {@code ATOMIC_OP_ATTRIB} (it is
   * not a replication attribute), so {@code identifyMutationTypes} leaves {@code hasAtomic} false:
   * the standby does not re-resolve the on-dup and the {@code Preconditions.checkState} guard
   * against active-side resolution flags does not fire. Indexing the on-dup-mutated column makes
   * its index key churn (Delete old key + Put new key) across rounds. Cross-cluster cell equality
   * on the data and index tables confirms the standby regenerates the index consistently with the
   * active.
   * <p>
   * Also covers returnResult + global index: a single-row atomic upsert with {@code RETURNING *}
   * sets {@code RETURN_RESULT} on the active mutation, driving {@code context.returnResult} true on
   * the active. {@code RETURN_RESULT} is likewise not a replication attribute, so the standby
   * leaves {@code returnResult} false and {@code checkState} does not fire; the resolved cells
   * (including the index-key change) replicate and regenerate.
   */
  @Test
  public void testOnDuplicateKeyUpdateWithIndex() throws Exception {
    final String tableName = "T_" + generateUniqueName();
    final String indexName = "I_" + generateUniqueName();
    String createTableDdl = String.format("create table if not exists %s "
      + "(pk varchar primary key, counter1 bigint, counter2 varchar)", tableName);
    String createIndexDdl = String.format(
      "create index if not exists %s on %s (counter2) include (counter1)", indexName, tableName);

    try (FailoverPhoenixConnection conn = (FailoverPhoenixConnection) DriverManager
      .getConnection(CLUSTERS.getJdbcHAUrl(), clientProps)) {
      conn.createStatement().execute(createTableDdl);
      conn.createStatement().execute(createIndexDdl);
      conn.commit();

      // Initial inserts for 5 distinct rows
      PreparedStatement insert =
        conn.prepareStatement("upsert into " + tableName + " VALUES(?, 0, 'init')");
      for (int i = 0; i < 5; ++i) {
        insert.setString(1, "row_" + i);
        insert.executeUpdate();
      }
      conn.commit();

      // ON DUPLICATE KEY UPDATE — increment counter1 and rewrite the indexed counter2 a few times
      // per row. Each round changes the index key, so the atomic path emits Delete(oldKey) +
      // Put(newKey) index work that the standby must regenerate from the captured data cells.
      String dml = "UPSERT INTO " + tableName + " VALUES(?, 0, ?) "
        + "ON DUPLICATE KEY UPDATE counter1 = counter1 + 1, counter2 = ?";
      PreparedStatement update = conn.prepareStatement(dml);
      conn.setAutoCommit(true);
      for (int round = 0; round < 3; ++round) {
        for (int i = 0; i < 5; ++i) {
          update.setString(1, "row_" + i);
          update.setString(2, "v" + round);
          update.setString(3, "v" + round);
          update.executeUpdate();
        }
      }

      // Set the indexed column to null via ON DUPLICATE KEY UPDATE — generates DeleteColumn cells
      // on the data row and deletes the index entry for the prior key.
      String dmlNullify =
        "UPSERT INTO " + tableName + " VALUES(?, 0, '') ON DUPLICATE KEY UPDATE counter2 = NULL";
      PreparedStatement nullify = conn.prepareStatement(dmlNullify);
      for (int i = 0; i < 5; ++i) {
        nullify.setString(1, "row_" + i);
        nullify.executeUpdate();
      }

      // Single-row atomic upsert with RETURNING *. RETURN_RESULT is not a replication
      // attribute, so the standby regenerates the index without re-resolving.
      String dmlReturning = "UPSERT INTO " + tableName + " VALUES('row_0', 0, 'init') "
        + "ON DUPLICATE KEY UPDATE counter1 = counter1 + 1, counter2 = 'returned' RETURNING *";
      if (isSetCorrectResultEnabledOnHBase()) {
        Statement returning = conn.createStatement();
        ResultSet rs = returning.execute(dmlReturning) ? returning.getResultSet() : null;
        assertNotNull("RETURNING * should produce a result set", rs);
        assertTrue("RETURNING * should project the atomically updated row", rs.next());
        assertFalse("Single-row atomic upsert returns exactly one row", rs.next());
      } else {
        conn.createStatement().executeUpdate(dmlReturning);
      }

      // Replay on cluster 2 and verify cross-cluster cell-level equality on data and index.
      replayAndVerifyAcrossClusters(Arrays.asList(createTableDdl, createIndexDdl), tableName,
        indexName);
    }
  }

  /**
   * Verifies cross-cluster cell-level equality after replay for a table with a Conditional TTL
   * expression. Conditional TTL adds coprocessor cells that get merged into the data mutation,
   * exercising the split-merged-mutation path.
   */
  @Test
  public void testConditionalTTL() throws Exception {
    final String tableName = "T_" + generateUniqueName();
    String createTableDdl = String.format("create table if not exists %s (id1 integer not null, "
      + "id2 integer not null, val1 varchar, val2 varchar, expired boolean "
      + "constraint pk primary key (id1, id2)) TTL = 'expired = TRUE'", tableName);

    try (FailoverPhoenixConnection conn = (FailoverPhoenixConnection) DriverManager
      .getConnection(CLUSTERS.getJdbcHAUrl(), clientProps)) {
      conn.createStatement().execute(createTableDdl);
      conn.commit();

      PreparedStatement stmt =
        conn.prepareStatement("upsert into " + tableName + " VALUES(?, ?, ?, ?, ?)");
      for (int i = 0; i < 5; ++i) {
        for (int j = 0; j < 10; ++j) {
          stmt.setInt(1, i);
          stmt.setInt(2, j);
          stmt.setString(3, "val1_" + i + "_" + j);
          stmt.setString(4, j % 2 == 0 ? "val2_" + i + "_" + j : null);
          stmt.setBoolean(5, false);
          stmt.executeUpdate();
        }
        conn.commit();
      }

      // Mark some rows expired
      PreparedStatement expireStmt = conn
        .prepareStatement("upsert into " + tableName + " (id1, id2, expired) VALUES(?, ?, true)");
      for (int j = 0; j < 5; ++j) {
        expireStmt.setInt(1, 0);
        expireStmt.setInt(2, j);
        expireStmt.executeUpdate();
      }
      conn.commit();

      // Update rows expired — conditional TTL triggers extra CP cells on update path
      PreparedStatement updateStmt =
        conn.prepareStatement("upsert into " + tableName + " (id1, id2, val1) VALUES(?, ?, ?)");
      for (int j = 0; j < 5; ++j) {
        updateStmt.setInt(1, 0);
        updateStmt.setInt(2, j);
        updateStmt.setString(3, "val11_" + 0 + "_" + j);
        updateStmt.executeUpdate();
      }
      conn.commit();

      // Replay on cluster 2 and verify cross-cluster cell-level equality
      replayAndVerifyAcrossClusters(Collections.singletonList(createTableDdl), tableName);
    }
  }

  /**
   * Conditional TTL + global index: a table with a Conditional TTL expression and a global index
   * covering the columns the TTL evaluation touches. The active evaluates the TTL before pre-image
   * capture, so the pre-image reflects post-conditional-TTL state and the captured data cells
   * (including the masking Deletes the TTL path generates) carry their {@code (row, ts)} group's
   * pre-image; the global index is not replicated. On the standby the reconstructed mutations carry
   * no {@code TTL} attribute (it is not a replication attribute), so {@code identifyMutationTypes}
   * leaves {@code hasConditionalTTL} false: the standby does not re-evaluate the TTL and the
   * {@code Preconditions.checkState} guard against active-side resolution flags does not fire.
   * Cross-cluster cell equality on data and index confirms the standby regenerates the index
   * consistently with the active's post-TTL state.
   */
  @Test
  public void testConditionalTTLWithIndex() throws Exception {
    final String tableName = "T_" + generateUniqueName();
    final String indexName = "I_" + generateUniqueName();
    String createTableDdl = String.format("create table if not exists %s (id1 integer not null, "
      + "id2 integer not null, val1 varchar, val2 varchar, expired boolean "
      + "constraint pk primary key (id1, id2)) TTL = 'expired = TRUE'", tableName);
    // Conditional TTL requires every column the TTL expression references (here: expired) to be
    // present in the index, so it is covered alongside the indexed val1 / included val2.
    String createIndexDdl = String.format(
      "create index if not exists %s on %s (val1) include (val2, expired)", indexName, tableName);

    try (FailoverPhoenixConnection conn = (FailoverPhoenixConnection) DriverManager
      .getConnection(CLUSTERS.getJdbcHAUrl(), clientProps)) {
      conn.createStatement().execute(createTableDdl);
      conn.createStatement().execute(createIndexDdl);
      conn.commit();

      PreparedStatement stmt =
        conn.prepareStatement("upsert into " + tableName + " VALUES(?, ?, ?, ?, ?)");
      for (int i = 0; i < 5; ++i) {
        for (int j = 0; j < 10; ++j) {
          stmt.setInt(1, i);
          stmt.setInt(2, j);
          stmt.setString(3, "val1_" + i + "_" + j);
          stmt.setString(4, j % 2 == 0 ? "val2_" + i + "_" + j : null);
          stmt.setBoolean(5, false);
          stmt.executeUpdate();
        }
        conn.commit();
      }

      // Mark some rows expired
      PreparedStatement expireStmt = conn
        .prepareStatement("upsert into " + tableName + " (id1, id2, expired) VALUES(?, ?, true)");
      for (int j = 0; j < 5; ++j) {
        expireStmt.setInt(1, 0);
        expireStmt.setInt(2, j);
        expireStmt.executeUpdate();
      }
      conn.commit();

      // Update the indexed column on expired rows — conditional TTL triggers extra CP cells on the
      // update path and the index key churns (Delete old val1 + Put new val1).
      PreparedStatement updateStmt =
        conn.prepareStatement("upsert into " + tableName + " (id1, id2, val1) VALUES(?, ?, ?)");
      for (int j = 0; j < 5; ++j) {
        updateStmt.setInt(1, 0);
        updateStmt.setInt(2, j);
        updateStmt.setString(3, "val11_" + 0 + "_" + j);
        updateStmt.executeUpdate();
      }
      conn.commit();

      // Replay on cluster 2 and verify cross-cluster cell-level equality on data and index.
      replayAndVerifyAcrossClusters(Arrays.asList(createTableDdl, createIndexDdl), tableName,
        indexName);
    }
  }

  /**
   * Plain CDC index (no downstream EVENTUAL secondary index). A CDC index is a STRONG-consistency
   * uncovered index written inline on the data write path; its rowkey leads with
   * {@code PARTITION_ID()} = the encoded data-table region name. The active ships only data cells +
   * per-(row,ts) pre-image (no index records); the standby regenerates the CDC index rowkey with
   * its OWN partition_id. With no EVENTUAL index, {@code IndexCDCConsumer} stays dormant on both
   * clusters, so replay is deterministic. The data table is verified byte-equal; the CDC index is
   * verified equal modulo its leading partition_id (which differs across clusters by design).
   */
  @Test
  public void testCDCIndex() throws Exception {
    final String tableName = "T_" + generateUniqueName();
    final String cdcName = "CDC_" + generateUniqueName();
    final String cdcIndexName = CDCUtil.getCDCIndexName(cdcName);
    String createTableDdl = String.format(
      "create table if not exists %s (pk integer not null " + "primary key, a varchar, b varchar)",
      tableName);
    String createCdcDdl = String.format("create cdc if not exists %s on %s", cdcName, tableName);

    try (FailoverPhoenixConnection conn = (FailoverPhoenixConnection) DriverManager
      .getConnection(CLUSTERS.getJdbcHAUrl(), clientProps)) {
      conn.createStatement().execute(createTableDdl);
      conn.createStatement().execute(createCdcDdl);
      conn.commit();

      // Inserts across several commits so the active produces multiple batches.
      PreparedStatement upsert =
        conn.prepareStatement("upsert into " + tableName + " VALUES(?, ?, ?)");
      for (int i = 0; i < 10; ++i) {
        upsert.setInt(1, i);
        upsert.setString(2, "a_" + i);
        upsert.setString(3, "b_" + i);
        upsert.executeUpdate();
        conn.commit();
      }

      // Update a column on some rows (new CDC change events for those rows).
      PreparedStatement update =
        conn.prepareStatement("upsert into " + tableName + " (pk, a) VALUES(?, ?)");
      for (int i = 0; i < 5; ++i) {
        update.setInt(1, i);
        update.setString(2, "a2_" + i);
        update.executeUpdate();
      }
      conn.commit();

      // Delete a few rows (CDC delete events).
      PreparedStatement delete =
        conn.prepareStatement("delete from " + tableName + " where pk = ?");
      for (int i = 5; i < 8; ++i) {
        delete.setInt(1, i);
        delete.executeUpdate();
      }
      conn.commit();

      // Replay on cluster 2 and verify the data table byte-equal; verify the CDC index modulo
      // its leading partition_id.
      replayAndVerifyAcrossClusters(Arrays.asList(createTableDdl, createCdcDdl), tableName);
      assertCDCIndexEqualAcrossClusters(cdcIndexName);
      // The cross-cluster compare above is symmetric, so it passes whether or not the serialized
      // downstream-index payload is present. Assert it positively against the live config so a
      // serialize=true run that failed to propagate would fail loudly rather than pass
      // degenerately.
      assertCDCIndexPayloadMatchesConfig(cdcIndexName);
    }
  }

  /**
   * This test simulates RS crashes in the middle of write transactions after the edits have been
   * written to the WAL but before they have been replicated to the standby cluster. Those edits
   * will be replicated when the WAL is replayed.
   */
  @Test
  public void testWALRestore() throws Exception {
    HBaseTestingUtility util = CLUSTERS.getHBaseCluster1();
    MiniHBaseCluster cluster = util.getHBaseCluster();
    final String tableName = "T_" + generateUniqueName();
    final String indexName = "I_" + generateUniqueName();
    TableName table = TableName.valueOf(tableName);
    try (FailoverPhoenixConnection conn = (FailoverPhoenixConnection) DriverManager
      .getConnection(CLUSTERS.getJdbcHAUrl(), clientProps)) {
      String ddl = String.format("create table %s (id1 integer not null, "
        + "id2 integer not null, val1 varchar, val2 varchar "
        + "constraint pk primary key (id1, id2))", tableName);
      conn.createStatement().execute(ddl);
      ddl = String.format("create index %s on %s (val1) include (val2)", indexName, tableName);
      conn.createStatement().execute(ddl);
      conn.commit();
    }
    // Mini cluster by default comes with only 1 RS. Starting a second RS so that
    // we can kill the RS
    JVMClusterUtil.RegionServerThread rs2 = cluster.startRegionServer();
    ServerName sn2 = rs2.getRegionServer().getServerName();
    // Assign some table regions to the new RS we started above
    moveRegionToServer(table, sn2);
    moveRegionToServer(TableName.valueOf(SYSTEM_CATALOG_NAME), sn2);
    moveRegionToServer(TableName.valueOf(SYSTEM_CHILD_LINK_NAME), sn2);
    int rowCount = 50;
    try (FailoverPhoenixConnection conn = (FailoverPhoenixConnection) DriverManager
      .getConnection(CLUSTERS.getJdbcHAUrl(), clientProps)) {
      PreparedStatement stmt =
        conn.prepareStatement("upsert into " + tableName + " VALUES(?, ?, ?, ?)");
      // upsert 50 rows
      for (int i = 0; i < 5; ++i) {
        for (int j = 0; j < 10; ++j) {
          stmt.setInt(1, i);
          stmt.setInt(2, j);
          stmt.setString(3, "abcdefghijklmnopqrstuvwxyz");
          stmt.setString(4, null); // Generate a DeleteColumn cell
          stmt.executeUpdate();
        }
        // we want to simulate RS crash after updating memstore and WAL
        IndexRegionObserver.setIgnoreSyncReplicationForTesting(true);
        conn.commit();
      }
      // Create tenant views for syscat and child link replication
      // Mutations on SYSTEM.CATALOG and SYSTEM.CHILD_LINK are generated on the server side
      // and don't have the HAGroup attribute set
      // createViewHierarchy();
    } finally {
      IndexRegionObserver.setIgnoreSyncReplicationForTesting(false);
    }
    // Kill the RS
    cluster.killRegionServer(rs2.getRegionServer().getServerName());
    Threads.sleep(20000); // just to be sure that the kill has fully started.
    // Regions will be re-opened and the WAL will be replayed
    util.waitUntilAllRegionsAssigned(table);
    try (FailoverPhoenixConnection conn = (FailoverPhoenixConnection) DriverManager
      .getConnection(CLUSTERS.getJdbcHAUrl(), clientProps)) {
      Map<String, Integer> expected = Maps.newHashMap();
      // For each row 1 Put + 1 Delete (DeleteColumn).
      // Index mutations are not replicated; the standby regenerates them.
      expected.put(tableName, rowCount * 2);
      // 1 tenant view was created
      // expected.put(SYSTEM_CHILD_LINK_NAME, 1);
      // atleast 1 log entry for syscat
      // expected.put(SYSTEM_CATALOG_NAME, 1);
      verifyReplication(expected);
    }
  }

  @Ignore("Mutations on SYSTEM.CATALOG and SYSTEM.CHILD_LINK are generated on the server side and don't have the HAGroup attribute set")
  public void testSystemTables() throws Exception {
    createViewHierarchy();
    Map<String, List<Mutation>> logsByTable = groupLogsByTable();
    dumpTableLogCount(logsByTable);
    // find all the log entries for system tables
    Map<String,
      List<Mutation>> systemTables = logsByTable.entrySet().stream()
        .filter(entry -> entry.getKey().startsWith(QueryConstants.SYSTEM_SCHEMA_NAME))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    // there should be only 2 entries CATALOG, CHILD_LINK
    assertEquals(2, systemTables.size());
    assertEquals(1, getCountForTable(systemTables, SYSTEM_CHILD_LINK_NAME));
    assertTrue(getCountForTable(systemTables, SYSTEM_CATALOG_NAME) > 0);
  }

  /**
   * Verifies that when data mutations are replayed on the standby via ReplicationLogProcessor,
   * IndexRegionObserver on the standby generates index mutations from the data mutations. The
   * replication log contains only data table mutations (no index mutations).
   */
  @Test
  public void testIndexRegenerationOnStandby() throws Exception {
    final String tableName = "T_" + generateUniqueName();
    final String indexName = "I_" + generateUniqueName();
    int rowCount = 10;

    // Create table and index on cluster 1 and insert data
    try (FailoverPhoenixConnection conn = (FailoverPhoenixConnection) DriverManager
      .getConnection(CLUSTERS.getJdbcHAUrl(), clientProps)) {
      conn.createStatement()
        .execute(String.format("CREATE TABLE %s (ID1 INTEGER NOT NULL, ID2 INTEGER NOT NULL, "
          + "VAL1 VARCHAR CONSTRAINT PK PRIMARY KEY (ID1, ID2))", tableName));
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

    // Get the standby log dir path before closing the logGroup
    Path standByLogDir = logGroup.getOrCreatePeerShardManager().getRootDirectoryPath();

    // Verify replication log has only data table mutations (no index mutations)
    logGroup.close();
    Map<String, List<Mutation>> logsByTable = groupLogsByTable();
    dumpTableLogCount(logsByTable);
    assertTrue("Replication log should contain data table mutations",
      logsByTable.containsKey(tableName));
    assertFalse("Replication log should NOT contain index table mutations",
      logsByTable.containsKey(indexName));

    // Debug: dump cell timestamps from first deserialized mutation
    List<Mutation> dataMutations = logsByTable.get(tableName);
    if (dataMutations != null && !dataMutations.isEmpty()) {
      Mutation firstMut = dataMutations.get(0);
      LOG.info("First mutation type={} ts={}", firstMut.getClass().getSimpleName(),
        firstMut.getTimestamp());
      for (Map.Entry<byte[], List<Cell>> entry : firstMut.getFamilyCellMap().entrySet()) {
        for (Cell cell : entry.getValue()) {
          LOG.info("  Cell: cf={} qual={} ts={} type={}",
            Bytes.toStringBinary(CellUtil.cloneFamily(cell)),
            Bytes.toStringBinary(CellUtil.cloneQualifier(cell)), cell.getTimestamp(),
            cell.getType());
        }
      }
    }

    // Create the same table and index on cluster 2
    try (Connection conn2 = CLUSTERS.getCluster2Connection(haGroup)) {
      conn2.createStatement().execute(
        String.format("CREATE TABLE IF NOT EXISTS %s (ID1 INTEGER NOT NULL, ID2 INTEGER NOT NULL, "
          + "VAL1 VARCHAR CONSTRAINT PK PRIMARY KEY (ID1, ID2))", tableName));
      conn2.createStatement()
        .execute(String.format("CREATE INDEX IF NOT EXISTS %s ON %s (VAL1)", indexName, tableName));
      conn2.commit();
    }

    // Replay the replication log on cluster 2
    FileSystem fs = standByLogDir.getFileSystem(conf2);
    List<Path> logFiles = findLogFiles(standByLogDir, fs);
    LOG.info("Found {} log files to replay", logFiles.size());
    assertTrue("Should have at least one log file", logFiles.size() > 0);

    ReplicationLogProcessor processor = ReplicationLogProcessor.get(conf2, haGroupName);
    try {
      for (Path logFile : logFiles) {
        LOG.info("Replaying log file: {}", logFile);
        processor.processLogFile(fs, logFile);
      }
    } finally {
      processor.close();
    }

    try (Connection conn1 = CLUSTERS.getCluster1Connection(haGroup);
      Statement stmt = conn1.createStatement()) {
      // Query the data table
      try (ResultSet rs = stmt.executeQuery("SELECT /*+ NO_INDEX */ COUNT(*) FROM " + tableName)) {
        assertTrue(rs.next());
        assertEquals("Data table on cluster 1 should have all rows", rowCount, rs.getInt(1));
      }

      try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + tableName)) {
        PhoenixResultSet prs = rs.unwrap(PhoenixResultSet.class);
        String explainPlan = QueryUtil.getExplainPlan(prs.getUnderlyingIterator());
        assertTrue(explainPlan.contains(indexName));
        assertTrue(rs.next());
        assertEquals("Index table on cluster 1 should have all rows", rowCount, rs.getInt(1));
      }
    }

    // Verify the index table on cluster 2 has data (generated by IRO during replay)
    try (Connection conn2 = CLUSTERS.getCluster2Connection(haGroup);
      Statement stmt = conn2.createStatement()) {
      // Query the data table
      try (ResultSet rs = stmt.executeQuery("SELECT /*+ NO_INDEX */ COUNT(*) FROM " + tableName)) {
        assertTrue(rs.next());
        assertEquals("Data table on cluster 2 should have all rows", rowCount, rs.getInt(1));
      }

      try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + tableName)) {
        PhoenixResultSet prs = rs.unwrap(PhoenixResultSet.class);
        String explainPlan = QueryUtil.getExplainPlan(prs.getUnderlyingIterator());
        assertTrue(explainPlan.contains(indexName));
        assertTrue(rs.next());
        assertEquals("Index table on cluster 2 should have all rows", rowCount, rs.getInt(1));
      }
    }

    // Deep cell-level comparison of data and index tables across clusters
    assertTablesEqualAcrossClusters(tableName);
    assertTablesEqualAcrossClusters(indexName);
  }

  /**
   * Concurrent same-row writes on the active (modeled on
   * {@code ConditionalTTLExpressionIT#testConcurrentUpserts}): many threads hammer a small row set
   * with randomized null/value columns, so the active produces a large, interleaved stream of
   * overlapping batches -- the same data row updated at many timestamps, often within a single
   * coalesced standby mini-batch. Replaying that stream on the standby exercises the
   * per-{@code (row,
   * ts)} grouping under contention: each group must fold only its own pre-image and cells, with no
   * leak across rows or timestamps. The active's outcome is nondeterministic, so the invariant is
   * cross-cluster cell equality -- the standby must reproduce exactly whatever the active
   * committed, for both the data table and the global index.
   */
  @Test
  public void testConcurrentUpserts() throws Exception {
    final String tableName = "T_" + generateUniqueName();
    final String indexName = "I_" + generateUniqueName();
    String createTableDdl =
      String.format("create table if not exists %s (id1 integer not null, id2 integer not null, "
        + "val1 varchar, val2 varchar constraint pk primary key (id1, id2))", tableName);
    String createIndexDdl = String
      .format("create index if not exists %s on %s (val1) include (val2)", indexName, tableName);

    try (FailoverPhoenixConnection conn = (FailoverPhoenixConnection) DriverManager
      .getConnection(CLUSTERS.getJdbcHAUrl(), clientProps)) {
      conn.createStatement().execute(createTableDdl);
      conn.createStatement().execute(createIndexDdl);
      conn.commit();
    }

    runConcurrentUpsertWorkload(tableName);

    // The global index physical table must receive zero replication records.
    Map<String, Integer> expected = Maps.newHashMap();
    expected.put(SYSTEM_CATALOG_NAME, 0);
    expected.put(SYSTEM_CHILD_LINK_NAME, 0);
    expected.put(indexName, 0);
    verifyReplication(expected);

    // Replay the per-round log files concurrently to simulate multiple region servers draining
    // shard files in parallel within the same round.
    replayAndVerifyAcrossClusters(4, Arrays.asList(createTableDdl, createIndexDdl), tableName,
      indexName);
  }

  /**
   * Local-index counterpart to {@link #testConcurrentUpserts}: the same concurrent same-row
   * workload, but the table carries only a local index. This drives the {@code PreImageLocalTable}
   * replay path under contention -- overlapping batches for one row spread across several round log
   * files, then replayed in parallel. Each thread randomizes the indexed column {@code val1}, so a
   * large fraction of updates move the local-index row key and must emit an old-key
   * {@code DeleteFamily} tombstone built from that group's own pre-image; getting the
   * per-{@code (row, ts)} grouping wrong would either leak a stale index row or drop a tombstone.
   * The active's outcome is nondeterministic, so the invariant is cross-cluster cell equality: the
   * standby must reproduce exactly what the active committed. Local-index cells live in the data
   * table's own {@code L#0} family, so verifying the data table cross-cluster also verifies the
   * regenerated local index.
   */
  @Test
  public void testConcurrentUpsertsLocalIndex() throws Exception {
    final String tableName = "T_" + generateUniqueName();
    final String localIndexName = "L_" + generateUniqueName();
    String createTableDdl =
      String.format("create table if not exists %s (id1 integer not null, id2 integer not null, "
        + "val1 varchar, val2 varchar constraint pk primary key (id1, id2))", tableName);
    String createLocalIndexDdl = String.format(
      "create local index if not exists %s on %s (val1) include (val2)", localIndexName, tableName);

    try (FailoverPhoenixConnection conn = (FailoverPhoenixConnection) DriverManager
      .getConnection(CLUSTERS.getJdbcHAUrl(), clientProps)) {
      conn.createStatement().execute(createTableDdl);
      conn.createStatement().execute(createLocalIndexDdl);
      conn.commit();
    }

    runConcurrentUpsertWorkload(tableName);

    // Replay the per-round log files concurrently to simulate multiple region servers draining
    // shard files in parallel within the same round. The data table scan covers the L#0 local-index
    // family, so this verifies the regenerated local index too. A local index shares the data
    // region, so there is no separate index physical table to assert zero replication records on --
    // cross-cluster equality is the whole check.
    replayAndVerifyAcrossClusters(4, Arrays.asList(createTableDdl, createLocalIndexDdl), tableName);
  }

  /**
   * Drives the concurrent same-row upsert workload shared by {@link #testConcurrentUpserts} and
   * {@link #testConcurrentUpsertsLocalIndex} against a table with columns {@code (id1, id2, val1,
   * val2)}. Eight threads hammer a 20-row set with randomized null/value columns (the indexed
   * column {@code val1} included, so index-key-moving updates and old-key tombstones both occur).
   * The workload runs by wall clock rather than a fixed iteration count: a short burst would land
   * in one 5s replication round, leaving a single populated log file and no concurrent same-row
   * replay; committing continuously for several rounds spreads the same rows across several files,
   * which is what makes the caller's parallel replay do real overlapping work. Fails the calling
   * test if any thread errors or the workload does not finish within the timeout.
   */
  private void runConcurrentUpsertWorkload(String tableName) throws InterruptedException {
    final int nThreads = 8;
    final int batchSize = 50;
    final int nRows = 20;
    final long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(25);
    final CountDownLatch doneSignal = new CountDownLatch(nThreads);
    final AtomicReference<Throwable> firstError = new AtomicReference<>();
    for (int t = 0; t < nThreads; t++) {
      final int seed = t;
      Thread thread = new Thread(() -> {
        Random rand = new Random(seed);
        try (
          FailoverPhoenixConnection conn = (FailoverPhoenixConnection) DriverManager
            .getConnection(CLUSTERS.getJdbcHAUrl(), clientProps);
          PreparedStatement ps =
            conn.prepareStatement("upsert into " + tableName + " VALUES(?, ?, ?, ?)")) {
          int i = 0;
          while (System.currentTimeMillis() < deadline) {
            ps.setInt(1, i % nRows);
            ps.setInt(2, 0);
            ps.setString(3, rand.nextBoolean() ? null : "v1_" + rand.nextInt(nRows));
            ps.setString(4, rand.nextBoolean() ? null : "v2_" + rand.nextInt());
            ps.executeUpdate();
            if ((i % batchSize) == 0) {
              conn.commit();
            }
            i++;
          }
          conn.commit();
        } catch (Throwable e) {
          firstError.compareAndSet(null, e);
        } finally {
          doneSignal.countDown();
        }
      });
      thread.start();
    }
    assertTrue("Ran out of time waiting for concurrent upserts",
      doneSignal.await(120, TimeUnit.SECONDS));
    if (firstError.get() != null) {
      throw new AssertionError("A concurrent upsert thread failed", firstError.get());
    }
  }

  private PhoenixTestBuilder.SchemaBuilder createViewHierarchy() throws Exception {
    // Define the test schema.
    // 1. Table with columns => (ORG_ID, KP, COL1, COL2, COL3), PK => (ORG_ID, KP)
    // 2. GlobalView with columns => (ID, COL4, COL5, COL6), PK => (ID)
    // 3. Tenant with columns => (ZID, COL7, COL8, COL9), PK => (ZID)
    final PhoenixTestBuilder.SchemaBuilder schemaBuilder =
      new PhoenixTestBuilder.SchemaBuilder(CLUSTERS.getJdbcHAUrl());
    PhoenixTestBuilder.SchemaBuilder.ConnectOptions connectOptions =
      new PhoenixTestBuilder.SchemaBuilder.ConnectOptions();
    connectOptions.setConnectProps(clientProps);
    PhoenixTestBuilder.SchemaBuilder.TableOptions tableOptions =
      PhoenixTestBuilder.SchemaBuilder.TableOptions.withDefaults();
    PhoenixTestBuilder.SchemaBuilder.GlobalViewOptions globalViewOptions =
      PhoenixTestBuilder.SchemaBuilder.GlobalViewOptions.withDefaults();
    PhoenixTestBuilder.SchemaBuilder.TenantViewOptions tenantViewWithOverrideOptions =
      PhoenixTestBuilder.SchemaBuilder.TenantViewOptions.withDefaults();
    PhoenixTestBuilder.SchemaBuilder.TenantViewIndexOptions tenantViewIndexOverrideOptions =
      PhoenixTestBuilder.SchemaBuilder.TenantViewIndexOptions.withDefaults();
    schemaBuilder.withConnectOptions(connectOptions).withTableOptions(tableOptions)
      .withGlobalViewOptions(globalViewOptions).withTenantViewOptions(tenantViewWithOverrideOptions)
      .withTenantViewIndexOptions(tenantViewIndexOverrideOptions).buildWithNewTenant();
    return schemaBuilder;
  }
}
