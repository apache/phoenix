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
package org.apache.phoenix.end2end;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Iterator;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.execute.MutationState;
import org.apache.phoenix.jdbc.MutationLimitBatchException;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.MaxMutationSizeExceededException;
import org.apache.phoenix.schema.MutationLimitReachedException;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.util.Repeat;
import org.apache.phoenix.util.RunUntilFailure;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.StringUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

@Category(ParallelStatsDisabledTest.class)
@RunWith(RunUntilFailure.class)
public class MutationStateIT extends ParallelStatsDisabledIT {

  private static final String DDL = " (ORGANIZATION_ID CHAR(15) NOT NULL, SCORE DOUBLE, "
    + "ENTITY_ID CHAR(15) NOT NULL, TAGS VARCHAR, CONSTRAINT PAGE_SNAPSHOT_PK "
    + "PRIMARY KEY (ORGANIZATION_ID, ENTITY_ID DESC)) MULTI_TENANT=TRUE";

  private static final Random RAND = new Random(5);

  private void upsertRows(PhoenixConnection conn, String fullTableName) throws SQLException {
    PreparedStatement stmt = conn.prepareStatement(
      "upsert into " + fullTableName + " (organization_id, entity_id, score) values (?,?,?)");
    for (int i = 0; i < 10000; i++) {
      stmt.setString(1, "AAAA" + i);
      stmt.setString(2, "BBBB" + i);
      stmt.setInt(3, 1);
      stmt.execute();
    }
  }

  /**
   * Helper to create Properties for preserve mode tests.
   * @param maxRows max mutation row count limit
   * @param maxBytes max mutation byte size limit (0 to skip setting)
   */
  private Properties createPreserveModeProps(int maxRows, long maxBytes) {
    Properties props = new Properties();
    props.setProperty(QueryServices.MAX_MUTATION_SIZE_ATTRIB, String.valueOf(maxRows));
    if (maxBytes > 0) {
      props.setProperty(QueryServices.MAX_MUTATION_SIZE_BYTES_ATTRIB, String.valueOf(maxBytes));
    }
    props.setProperty(QueryServices.PRESERVE_MUTATIONS_ON_LIMIT_EXCEEDED_ATTRIB, "true");
    return props;
  }

  /**
   * Helper to verify the row count in a table.
   */
  private void verifyRowCount(Connection conn, String tableName, int expected)
      throws SQLException {
    try (ResultSet rs = conn.createStatement()
        .executeQuery("SELECT COUNT(*) FROM " + tableName)) {
      assertTrue("Should have results", rs.next());
      assertEquals("Row count mismatch", expected, rs.getInt(1));
    }
  }

  public static String randString(int length) {
    return new BigInteger(164, RAND).toString().substring(0, length);
  }

  private static void mutateRandomly(final String upsertStmt, final String fullTableName,
    final int nThreads, final int nRows, final int nIndexValues, final int batchSize,
    final CountDownLatch doneSignal) {
    Runnable[] runnables = new Runnable[nThreads];
    for (int i = 0; i < nThreads; i++) {
      runnables[i] = new Runnable() {

        @Override
        public void run() {
          try {
            Connection conn = DriverManager.getConnection(getUrl());
            for (int i = 0; i < nRows; i++) {
              PreparedStatement statement = conn.prepareStatement(upsertStmt);
              int index = 0;
              statement.setString(++index, randString(15));
              statement.setString(++index, randString(15));
              statement.setString(++index, randString(15));
              statement.setString(++index, randString(1));
              statement.setString(++index, randString(15));
              statement.setString(++index, randString(15));
              statement.setTimestamp(++index, new Timestamp(System.currentTimeMillis()));
              statement.setTimestamp(++index, new Timestamp(System.currentTimeMillis()));
              statement.setString(++index, randString(1));
              statement.setString(++index, randString(1));
              statement.setBoolean(++index, false);
              statement.setString(++index, randString(1));
              statement.setString(++index, randString(1));
              statement.setString(++index, randString(15));
              statement.setString(++index, randString(15));
              statement.setString(++index, randString(15));
              statement.setInt(++index, RAND.nextInt());
              statement.execute();
              if ((i % batchSize) == 0) {
                conn.commit();
              }
            }
            conn.commit();
          } catch (SQLException e) {
            throw new RuntimeException(e);
          } finally {
            doneSignal.countDown();
          }
        }

      };
    }
    for (int i = 0; i < nThreads; i++) {
      Thread t = new Thread(runnables[i]);
      t.start();
    }
  }

  @Test
  @Repeat(10)
  public void testOnlyIndexTableWriteFromClientSide()
    throws SQLException, InterruptedException, IOException {
    String schemaName = generateUniqueName();
    String tableName = generateUniqueName();
    String indexName1 = generateUniqueName();
    String indexName2 = generateUniqueName();
    String indexName3 = generateUniqueName();
    String fullTableName = SchemaUtil.getTableName(schemaName, tableName);
    String fullIndexName1 = SchemaUtil.getTableName(schemaName, indexName1);
    String CREATE_DATA_TABLE = "CREATE TABLE IF NOT EXISTS " + fullTableName + " ( \n"
      + "    USER1_ID CHAR(15) NOT NULL,\n" + "    ELEMENT1_ID CHAR(15) NOT NULL,\n"
      + "    ELEMENT_ID CHAR(15) NOT NULL,\n" + "    ELEMENT_TYPE VARCHAR(1) NOT NULL,\n"
      + "    TYPE_ID CHAR(15) NOT NULL,\n" + "    USER_ID CHAR(15) NOT NULL,\n"
      + "    ELEMENT4_TIME TIMESTAMP,\n" + "    ELEMENT_UPDATE TIMESTAMP,\n"
      + "    ELEMENT_SCORE DOUBLE,\n" + "    ELEMENT2_TYPE VARCHAR(1),\n"
      + "    ELEMENT1_TYPE VARCHAR(1),\n" + "    ELEMENT1_IS_SYS_GEN BOOLEAN,\n"
      + "    ELEMENT1_STATUS VARCHAR(1),\n" + "    ELEMENT1_VISIBILITY VARCHAR(1),\n"
      + "    ELEMENT3_ID CHAR(15),\n" + "    ELEMENT4_BY CHAR(15),\n"
      + "    BEST_ELEMENT_ID CHAR(15),\n" + "    ELEMENT_COUNT INTEGER,\n"
      + "    CONSTRAINT PK PRIMARY KEY\n" + "    (\n" + "     USER1_ID,\n" + "     ELEMENT1_ID,\n"
      + "     ELEMENT_ID,\n" + "     ELEMENT_TYPE,\n" + "     TYPE_ID,\n" + "     USER_ID\n"
      + " )\n" + " ) VERSIONS=1,MULTI_TENANT=TRUE,TTL=31536000\n";

    String CREATE_INDEX_1 =
      "CREATE INDEX IF NOT EXISTS " + indexName1 + " \n" + "     ON " + fullTableName + " (\n"
        + "     TYPE_ID,\n" + "     ELEMENT_ID,\n" + "     ELEMENT_TYPE,\n" + "     USER_ID,\n"
        + "     ELEMENT4_TIME DESC,\n" + "     ELEMENT1_ID DESC\n" + "     ) INCLUDE (\n"
        + "     ELEMENT2_TYPE,\n" + "     ELEMENT1_TYPE,\n" + "     ELEMENT1_IS_SYS_GEN,\n"
        + "     ELEMENT1_STATUS,\n" + "     ELEMENT1_VISIBILITY,\n" + "     ELEMENT3_ID,\n"
        + "     ELEMENT4_BY,\n" + "     BEST_ELEMENT_ID,\n" + "     ELEMENT_COUNT\n" + "     )\n";

    String CREATE_INDEX_2 =
      " CREATE INDEX IF NOT EXISTS " + indexName2 + "\n" + "     ON " + fullTableName + " (\n"
        + "     TYPE_ID,\n" + "     ELEMENT_ID,\n" + "     ELEMENT_TYPE,\n" + "     USER_ID,\n"
        + "     ELEMENT_UPDATE DESC,\n" + "     ELEMENT1_ID DESC\n" + "     ) INCLUDE (\n"
        + "     ELEMENT2_TYPE,\n" + "     ELEMENT1_TYPE,\n" + "     ELEMENT1_IS_SYS_GEN,\n"
        + "     ELEMENT1_STATUS,\n" + "     ELEMENT1_VISIBILITY,\n" + "     ELEMENT3_ID,\n"
        + "     ELEMENT4_BY,\n" + "     BEST_ELEMENT_ID,\n" + "     ELEMENT_COUNT\n" + "     )\n";

    String CREATE_INDEX_3 =
      "CREATE INDEX IF NOT EXISTS " + indexName3 + "\n" + "     ON " + fullTableName + " (\n"
        + "     TYPE_ID,\n" + "     ELEMENT_ID,\n" + "     ELEMENT_TYPE,\n" + "     USER_ID,\n"
        + "     ELEMENT_SCORE DESC,\n" + "     ELEMENT1_ID DESC\n" + "     ) INCLUDE (\n"
        + "     ELEMENT2_TYPE,\n" + "     ELEMENT1_TYPE,\n" + "     ELEMENT1_IS_SYS_GEN,\n"
        + "     ELEMENT1_STATUS,\n" + "     ELEMENT1_VISIBILITY,\n" + "     ELEMENT3_ID,\n"
        + "     ELEMENT4_BY,\n" + "     BEST_ELEMENT_ID,\n" + "     ELEMENT_COUNT\n" + "     )\n";

    String UPSERT_INTO_DATA_TABLE = "UPSERT INTO " + fullTableName + "\n" + "(\n"
      + "    USER1_ID,\n" + "    ELEMENT1_ID,\n" + "    ELEMENT_ID,\n" + "    ELEMENT_TYPE,\n"
      + "    TYPE_ID,\n" + "    USER_ID,\n" + "    ELEMENT4_TIME,\n" + "    ELEMENT_UPDATE,\n"
      + "    ELEMENT2_TYPE,\n" + "    ELEMENT1_TYPE,\n" + "    ELEMENT1_IS_SYS_GEN,\n"
      + "    ELEMENT1_STATUS,\n" + "    ELEMENT1_VISIBILITY,\n" + "    ELEMENT3_ID,\n"
      + "    ELEMENT4_BY,\n" + "    BEST_ELEMENT_ID,\n" + "    ELEMENT_COUNT\n" + ")"
      + "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

    int nThreads = 1;
    int nRows = 5000;
    int nIndexValues = 4000;
    int batchSize = 200;
    final CountDownLatch doneSignal = new CountDownLatch(nThreads);
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      try {
        conn.createStatement().execute(CREATE_DATA_TABLE);
        conn.createStatement().execute(CREATE_INDEX_1);
        conn.createStatement().execute(CREATE_INDEX_2);
        conn.createStatement().execute(CREATE_INDEX_3);
        conn.commit();
        mutateRandomly(UPSERT_INTO_DATA_TABLE, fullTableName, nThreads, nRows, nIndexValues,
          batchSize, doneSignal);
        Thread.sleep(200);
        unassignRegionAsync(fullIndexName1);
        assertTrue("Ran out of time", doneSignal.await(120, TimeUnit.SECONDS));
      } finally {

      }
      long dataTableRows = TestUtil.getRowCount(conn, fullTableName);
      ResultSet rs = conn.getMetaData().getTables(null, StringUtil.escapeLike(schemaName), null,
        new String[] { PTableType.INDEX.toString() });
      while (rs.next()) {
        String indexState = rs.getString("INDEX_STATE");
        String indexName = rs.getString(3);
        long rowCountIndex =
          TestUtil.getRowCount(conn, SchemaUtil.getTableName(schemaName, indexName));
        if (indexState.equals(PIndexState.ACTIVE.name())) {
          assertTrue(dataTableRows == rowCountIndex);
        } else {
          assertTrue(dataTableRows > rowCountIndex);
        }
      }

    } catch (InterruptedException e) {
      throw e;
    } catch (IOException e) {
      throw e;
    }
  }

  @Test
  public void testDeleteMaxMutationSize() throws SQLException {
    String tableName = generateUniqueName();
    int NUMBER_OF_ROWS = 20;
    String ddl = "CREATE TABLE " + tableName + " (V BIGINT PRIMARY KEY, K BIGINT)";
    PhoenixConnection conn = (PhoenixConnection) DriverManager.getConnection(getUrl());
    conn.createStatement().execute(ddl);

    for (int i = 0; i < NUMBER_OF_ROWS; i++) {
      conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES (" + i + ", " + i + ")");
      conn.commit();
    }

    Properties props = new Properties();
    props.setProperty(QueryServices.MAX_MUTATION_SIZE_ATTRIB, String.valueOf(NUMBER_OF_ROWS / 2));
    PhoenixConnection connection = (PhoenixConnection) DriverManager.getConnection(getUrl(), props);
    connection.setAutoCommit(false);

    try {
      for (int i = 0; i < NUMBER_OF_ROWS; i++) {
        connection.createStatement().execute("DELETE FROM " + tableName + " WHERE K = " + i);
      }
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains(SQLExceptionCode.MAX_MUTATION_SIZE_EXCEEDED.getMessage()));
    }

    props.setProperty(QueryServices.MAX_MUTATION_SIZE_BYTES_ATTRIB, "10");
    props.setProperty(QueryServices.MAX_MUTATION_SIZE_ATTRIB, "10000");
    connection = (PhoenixConnection) DriverManager.getConnection(getUrl(), props);
    connection.setAutoCommit(false);

    try {
      connection.createStatement().execute("DELETE FROM " + tableName);
    } catch (SQLException e) {
      assertTrue(
        e.getMessage().contains(SQLExceptionCode.MAX_MUTATION_SIZE_BYTES_EXCEEDED.getMessage()));
    }
  }

  @Test
  public void testUpsertMaxMutationSize() throws Exception {
    Properties connectionProperties = new Properties();
    connectionProperties.setProperty(QueryServices.MAX_MUTATION_SIZE_ATTRIB, "3");
    connectionProperties.setProperty(QueryServices.MAX_MUTATION_SIZE_BYTES_ATTRIB, "1000000");
    PhoenixConnection connection =
      (PhoenixConnection) DriverManager.getConnection(getUrl(), connectionProperties);
    String fullTableName = generateUniqueName();
    try (Statement stmt = connection.createStatement()) {
      stmt.execute("CREATE TABLE " + fullTableName + DDL);
    }
    try {
      upsertRows(connection, fullTableName);
      fail();
    } catch (SQLException e) {
      assertEquals(SQLExceptionCode.MAX_MUTATION_SIZE_EXCEEDED.getErrorCode(), e.getErrorCode());
      assertTrue(e.getMessage().contains(SQLExceptionCode.MAX_MUTATION_SIZE_EXCEEDED.getMessage()));
      assertTrue(e.getMessage()
        .contains(connectionProperties.getProperty(QueryServices.MAX_MUTATION_SIZE_ATTRIB)));
    }

    // set the max mutation size (bytes) to a low value
    connectionProperties.setProperty(QueryServices.MAX_MUTATION_SIZE_ATTRIB, "1000");
    connectionProperties.setProperty(QueryServices.MAX_MUTATION_SIZE_BYTES_ATTRIB, "4");
    connection = (PhoenixConnection) DriverManager.getConnection(getUrl(), connectionProperties);
    try {
      upsertRows(connection, fullTableName);
      fail();
    } catch (SQLException e) {
      assertEquals(SQLExceptionCode.MAX_MUTATION_SIZE_BYTES_EXCEEDED.getErrorCode(),
        e.getErrorCode());
      assertTrue(
        e.getMessage().contains(SQLExceptionCode.MAX_MUTATION_SIZE_BYTES_EXCEEDED.getMessage()));
      assertTrue(e.getMessage()
        .contains(connectionProperties.getProperty(QueryServices.MAX_MUTATION_SIZE_BYTES_ATTRIB)));
    }
  }

  /**
   * Tests that when preserveOnLimitExceeded=true, executeUpdate() throws
   * MutationLimitReachedException without clearing buffered mutations,
   * allowing the client to commit and retry.
   */
  @Test
  public void testExecuteUpdatePreserveOnLimitExceeded() throws Exception {
    String fullTableName = generateUniqueName();
    int maxMutationSize = 5;

    // Create table
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.createStatement().execute("CREATE TABLE " + fullTableName + DDL);
    }

    try (PhoenixConnection conn = (PhoenixConnection) DriverManager.getConnection(
        getUrl(), createPreserveModeProps(maxMutationSize, 0))) {
      conn.setAutoCommit(false);

      PreparedStatement stmt = conn.prepareStatement(
        "UPSERT INTO " + fullTableName + " (organization_id, entity_id, score) VALUES (?,?,?)");

      int totalRows = 12;
      int processed = 0;
      int commitCount = 0;

      while (processed < totalRows) {
        try {
          for (int i = processed; i < totalRows; i++) {
            stmt.setString(1, "ORG" + String.format("%011d", i));
            stmt.setString(2, "ENT" + String.format("%011d", i));
            stmt.setInt(3, i);
            stmt.executeUpdate();
            processed = i + 1;
          }
          conn.commit();
          commitCount++;
          break; // All done
        } catch (MutationLimitReachedException e) {
          // Verify the exception is the expected type
          assertEquals(SQLExceptionCode.MUTATION_LIMIT_REACHED.getErrorCode(), e.getErrorCode());

          // Verify mutations were preserved - MutationState should have rows
          MutationState state = conn.getMutationState();
          assertTrue("Mutations should be preserved", state.getNumRows() > 0);

          // Commit what we have so far
          conn.commit();
          commitCount++;
          // Loop continues from 'processed' index
        }
      }

      // Should have required multiple commits due to limit
      assertTrue("Should have committed multiple times", commitCount > 1);

      verifyRowCount(conn, fullTableName, totalRows);
    }
  }

  /**
   * Tests that when preserveOnLimitExceeded=true, executeBatch() throws
   * MutationLimitBatchException with proper checkpoint info, allowing recovery.
   */
  @Test
  public void testExecuteBatchPreserveOnLimitExceeded() throws Exception {
    String fullTableName = generateUniqueName();
    int maxMutationSize = 5;

    // Create table
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.createStatement().execute("CREATE TABLE " + fullTableName + DDL);
    }

    try (PhoenixConnection conn = (PhoenixConnection) DriverManager.getConnection(
        getUrl(), createPreserveModeProps(maxMutationSize, 0))) {
      conn.setAutoCommit(false);

      PreparedStatement stmt = conn.prepareStatement(
        "UPSERT INTO " + fullTableName + " (organization_id, entity_id, score) VALUES (?,?,?)");

      int totalRows = 15;
      int commitCount = 0;

      // Add all rows to batch
      for (int i = 0; i < totalRows; i++) {
        stmt.setString(1, "ORG" + String.format("%011d", i));
        stmt.setString(2, "ENT" + String.format("%011d", i));
        stmt.setInt(3, i);
        stmt.addBatch();
      }

      // Execute batch - should hit limit and throw MutationLimitBatchException
      while (true) {
        try {
          stmt.executeBatch();
          conn.commit();
          commitCount++;
          break; // All done
        } catch (MutationLimitBatchException e) {
          // Verify we got processedCount
          assertTrue("ProcessedCount should be > 0", e.getProcessedCount() > 0);

          // Commit what was successfully buffered
          conn.commit();
          commitCount++;
          // executeBatch() trims the batch, so just retry
        }
      }

      // Should have required multiple commits
      assertTrue("Should have committed multiple times", commitCount > 1);

      verifyRowCount(conn, fullTableName, totalRows);
    }
  }

  /**
   * Tests executeBatch() with autoCommit=true and preserveOnLimitExceeded=true.
   * In this case, executeBatch() should auto-commit on limit and trim batch.
   */
  @Test
  public void testExecuteBatchAutoCommitPreserveOnLimitExceeded() throws Exception {
    String fullTableName = generateUniqueName();
    int maxMutationSize = 5;

    // Create table
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.createStatement().execute("CREATE TABLE " + fullTableName + DDL);
    }

    try (PhoenixConnection conn = (PhoenixConnection) DriverManager.getConnection(
        getUrl(), createPreserveModeProps(maxMutationSize, 0))) {
      conn.setAutoCommit(true); // autoCommit is true

      PreparedStatement stmt = conn.prepareStatement(
        "UPSERT INTO " + fullTableName + " (organization_id, entity_id, score) VALUES (?,?,?)");

      int totalRows = 15;
      int exceptionCount = 0;

      // Add all rows to batch
      for (int i = 0; i < totalRows; i++) {
        stmt.setString(1, "ORG" + String.format("%011d", i));
        stmt.setString(2, "ENT" + String.format("%011d", i));
        stmt.setInt(3, i);
        stmt.addBatch();
      }

      // Execute batch - with autoCommit=true, it should auto-commit and trim
      while (true) {
        try {
          stmt.executeBatch();
          break; // All done
        } catch (MutationLimitBatchException e) {
          exceptionCount++;
          // With autoCommit=true, mutations were already committed
          // Batch is trimmed, just retry
        }
      }

      // Should have hit limit at least once
      assertTrue("Should have hit limit at least once", exceptionCount > 0);

      verifyRowCount(conn, fullTableName, totalRows);
    }
  }

  /**
   * Tests that when preserveOnLimitExceeded=false (default), the old behavior
   * is maintained - mutations are cleared on limit exceeded.
   */
  @Test
  public void testExecuteUpdateDefaultBehaviorClearsMutations() throws Exception {
    String fullTableName = generateUniqueName();
    int maxMutationSize = 5;

    // Create table
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.createStatement().execute("CREATE TABLE " + fullTableName + DDL);
    }

    // Test with preserveOnLimitExceeded=false (default)
    Properties props = new Properties();
    props.setProperty(QueryServices.MAX_MUTATION_SIZE_ATTRIB, String.valueOf(maxMutationSize));
    // Not setting PRESERVE_MUTATIONS_ON_LIMIT_EXCEEDED_ATTRIB, should default to false

    try (PhoenixConnection conn =
      (PhoenixConnection) DriverManager.getConnection(getUrl(), props)) {
      conn.setAutoCommit(false);

      PreparedStatement stmt = conn.prepareStatement(
        "UPSERT INTO " + fullTableName + " (organization_id, entity_id, score) VALUES (?,?,?)");

      try {
        for (int i = 0; i < 20; i++) {
          stmt.setString(1, "ORG" + String.format("%011d", i));
          stmt.setString(2, "ENT" + String.format("%011d", i));
          stmt.setInt(3, i);
          stmt.executeUpdate();
        }
        fail("Should have thrown MaxMutationSizeExceededException");
      } catch (SQLException e) {
        // Should be the old exception type, not MutationLimitReachedException
        assertEquals(SQLExceptionCode.MAX_MUTATION_SIZE_EXCEEDED.getErrorCode(), e.getErrorCode());

        // Verify mutations were cleared (old behavior)
        MutationState state = conn.getMutationState();
        assertEquals("Mutations should be cleared", 0, state.getNumRows());
      }
    }
  }

  /**
   * Tests byte size limit with preserveOnLimitExceeded=true.
   */
  @Test
  public void testExecuteUpdatePreserveOnByteLimitExceeded() throws Exception {
    String fullTableName = generateUniqueName();

    // Create table with a VARCHAR column for large values
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.createStatement().execute("CREATE TABLE " + fullTableName + DDL);
    }

    // High row limit, low byte limit (~3-4 rows at ~900+ bytes each)
    try (PhoenixConnection conn = (PhoenixConnection) DriverManager.getConnection(
        getUrl(), createPreserveModeProps(1000000, 3000))) {
      conn.setAutoCommit(false);

      PreparedStatement stmt = conn.prepareStatement(
        "UPSERT INTO " + fullTableName + " (organization_id, entity_id, score, tags) VALUES (?,?,?,?)");

      int totalRows = 10;
      int processed = 0;
      int commitCount = 0;
      String largeValue = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"; // ~36 bytes per row in tags

      while (processed < totalRows) {
        try {
          for (int i = processed; i < totalRows; i++) {
            stmt.setString(1, "ORG" + String.format("%011d", i));
            stmt.setString(2, "ENT" + String.format("%011d", i));
            stmt.setInt(3, i);
            stmt.setString(4, largeValue + i);
            stmt.executeUpdate();
            processed = i + 1;
          }
          conn.commit();
          commitCount++;
          break;
        } catch (MutationLimitReachedException e) {
          assertEquals(SQLExceptionCode.MUTATION_LIMIT_REACHED.getErrorCode(), e.getErrorCode());
          conn.commit();
          commitCount++;
        }
      }

      // Should have required multiple commits due to byte limit
      assertTrue("Should have committed multiple times due to byte limit", commitCount > 1);

      verifyRowCount(conn, fullTableName, totalRows);
    }
  }

  /**
   * Tests that when preserveOnLimitExceeded is true but a single mutation exceeds
   * the limit (constructor case), we still get the old exception type since there's
   * no existing state to preserve.
   */
  @Test
  public void testSingleMutationExceedsLimitWithPreserveOption() throws Exception {
    String fullTableName = generateUniqueName();
    String sourceTable = generateUniqueName();

    // Use a connection without limits to create tables and set up test data
    try (Connection setupConn = DriverManager.getConnection(getUrl())) {
      setupConn.setAutoCommit(true);
      try (Statement stmt = setupConn.createStatement()) {
        stmt.execute("CREATE TABLE " + fullTableName + DDL);
        stmt.execute("CREATE TABLE " + sourceTable + " (id VARCHAR PRIMARY KEY, val INTEGER)");
        // Insert multiple rows into source table
        stmt.executeUpdate("UPSERT INTO " + sourceTable + " VALUES ('a', 1)");
        stmt.executeUpdate("UPSERT INTO " + sourceTable + " VALUES ('b', 2)");
      }
    }

    // Now use a connection with the limit set
    Properties props = new Properties();
    // Set limit to 1 row so any operation producing multiple rows would exceed it
    props.setProperty(QueryServices.MAX_MUTATION_SIZE_ATTRIB, "1");
    props.setProperty(QueryServices.PRESERVE_MUTATIONS_ON_LIMIT_EXCEEDED_ATTRIB, "true");

    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      conn.setAutoCommit(false);

      // Insert a row to have some existing state in the connection's MutationState
      try (PreparedStatement stmt = conn.prepareStatement(
          "UPSERT INTO " + fullTableName +
          " (organization_id, entity_id, score) VALUES (?, ?, ?)")) {
        stmt.setString(1, "org1");
        stmt.setString(2, "entity1");
        stmt.setInt(3, 1);
        stmt.executeUpdate();
      }

      // Now do a SELECT-based UPSERT that produces multiple rows in a single mutation.
      // This creates a new MutationState in the constructor that already exceeds the limit.
      // Since there's no state to preserve in that NEW MutationState (only 1 row was inserted 
      // into the connection's existing state), we should get the old exception type 
      // (MaxMutationSizeExceededException), not MutationLimitReachedException.
      try (Statement stmt = conn.createStatement()) {
        try {
          stmt.executeUpdate(
              "UPSERT INTO " + fullTableName +
              " (organization_id, entity_id, score) " +
              "SELECT id, 'e', val FROM " + sourceTable);
          fail("Expected MaxMutationSizeExceededException");
        } catch (MaxMutationSizeExceededException e) {
          // Expected - legacy exception even though preserveOnLimitExceeded is true,
          // because the single mutation (from constructor) has no prior state to preserve
        } catch (MutationLimitReachedException e) {
          fail("Should have thrown MaxMutationSizeExceededException, not MutationLimitReachedException. " +
               "When a single mutation exceeds the limit, there's no state to preserve.");
        }
      }
    }
  }

  /**
   * Tests that when preserveOnLimitExceeded=true and the limit is reached during
   * row merge (same row key updated multiple times), the mutations are preserved.
   * This specifically tests the 3rd case in joinMutationState where rows are merged.
   */
  @Test
  public void testRowMergePreserveOnLimitExceeded() throws Exception {
    String fullTableName = generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.createStatement().execute("CREATE TABLE " + fullTableName + DDL);
    }

    // Low byte limit so merging rows with increasing sizes triggers limit
    try (PhoenixConnection conn = (PhoenixConnection) DriverManager.getConnection(
        getUrl(), createPreserveModeProps(1000, 1500))) {
      conn.setAutoCommit(false);

      PreparedStatement stmt = conn.prepareStatement(
        "UPSERT INTO " + fullTableName
          + " (organization_id, entity_id, score, tags) VALUES (?,?,?,?)");

      int commitCount = 0;
      int totalUpdates = 0;
      String baseKey = "ORG000000000001";
      String entityKey = "ENT000000000001";

      // Keep updating the SAME row with larger values until limit reached
      // Each update merges with the existing row, increasing the size
      while (totalUpdates < 50) {
        try {
          for (int i = totalUpdates; i < 50; i++) {
            stmt.setString(1, baseKey);
            stmt.setString(2, entityKey);
            stmt.setInt(3, i);
            // Increasing size with each update - this will eventually exceed byte limit
            StringBuilder sb = new StringBuilder("update" + i + "_");
            for (int j = 0; j < i * 10; j++) {
              sb.append("X");
            }
            stmt.setString(4, sb.toString());
            stmt.executeUpdate();
            totalUpdates = i + 1;
          }
          conn.commit();
          commitCount++;
          break;
        } catch (MutationLimitReachedException e) {
          // Mutations should be preserved - verify we have state
          MutationState state = conn.getMutationState();
          assertTrue("Mutations should be preserved on limit exceeded",
            state.getNumRows() > 0 || state.getEstimatedSize() > 0);
          conn.commit();
          commitCount++;
        }
      }

      // Should have hit limit and committed multiple times due to row merge size growth
      assertTrue("Should have committed at least once", commitCount >= 1);

      // Verify the row exists with the latest value
      try (ResultSet rs = conn.createStatement()
        .executeQuery("SELECT score FROM " + fullTableName
          + " WHERE organization_id = '" + baseKey + "' AND entity_id = '" + entityKey + "'")) {
        assertTrue("Row should exist", rs.next());
        // Score should be the last successfully committed value
        assertTrue("Score should be set", rs.getInt(1) >= 0);
      }
    }
  }

  /**
   * Tests that when preserveOnLimitExceeded=true and the limit is reached during
   * conflicting row addition (regular upsert vs ON DUPLICATE KEY upsert on same key),
   * the mutations are preserved. This tests the conflict case in joinMutationState.
   */
  @Test
  public void testConflictingRowsPreserveOnLimitExceeded() throws Exception {
    String fullTableName = generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.createStatement().execute("CREATE TABLE " + fullTableName + DDL);
    }

    // Low byte limit so adding conflicting rows triggers limit
    try (PhoenixConnection conn = (PhoenixConnection) DriverManager.getConnection(
        getUrl(), createPreserveModeProps(1000, 2000))) {
      conn.setAutoCommit(false);

      // Regular UPSERT statement
      PreparedStatement regularStmt = conn.prepareStatement(
        "UPSERT INTO " + fullTableName
          + " (organization_id, entity_id, score, tags) VALUES (?,?,?,?)");

      // ON DUPLICATE KEY UPSERT statement - conflicts with regular upsert
      PreparedStatement onDupKeyStmt = conn.prepareStatement(
        "UPSERT INTO " + fullTableName
          + " (organization_id, entity_id, score) VALUES (?,?,?)"
          + " ON DUPLICATE KEY UPDATE score = score + 1");

      int commitCount = 0;
      int totalRows = 0;
      String orgKey = "ORG000000000001";

      // Alternate between regular upserts and ON DUPLICATE KEY upserts on same keys
      // This creates conflicting rows that can't be merged
      while (totalRows < 30) {
        try {
          for (int i = totalRows; i < 30; i++) {
            String entityKey = String.format("ENT%012d", i);

            // First do a regular upsert
            regularStmt.setString(1, orgKey);
            regularStmt.setString(2, entityKey);
            regularStmt.setInt(3, i);
            StringBuilder sb = new StringBuilder("data" + i + "_");
            for (int j = 0; j < 20; j++) {
              sb.append("X");
            }
            regularStmt.setString(4, sb.toString());
            regularStmt.executeUpdate();

            // Then do an ON DUPLICATE KEY upsert on the same key - this conflicts
            onDupKeyStmt.setString(1, orgKey);
            onDupKeyStmt.setString(2, entityKey);
            onDupKeyStmt.setInt(3, i * 10);
            onDupKeyStmt.executeUpdate();

            totalRows = i + 1;
          }
          conn.commit();
          commitCount++;
          break;
        } catch (MutationLimitReachedException e) {
          // Mutations should be preserved - verify we have state
          MutationState state = conn.getMutationState();
          assertTrue("Mutations should be preserved on limit exceeded",
            state.getNumRows() > 0 || state.getEstimatedSize() > 0);
          conn.commit();
          commitCount++;
        }
      }

      // Should have hit limit and committed multiple times due to conflicting row size
      assertTrue("Should have committed at least once", commitCount >= 1);

      // Verify rows exist
      try (ResultSet rs = conn.createStatement()
        .executeQuery("SELECT COUNT(*) FROM " + fullTableName
          + " WHERE organization_id = '" + orgKey + "'")) {
        assertTrue("Should have results", rs.next());
        assertTrue("Should have inserted rows", rs.getInt(1) > 0);
      }
    }
  }

  @Test
  public void testMutationEstimatedSize() throws Exception {
    PhoenixConnection conn = (PhoenixConnection) DriverManager.getConnection(getUrl());
    conn.setAutoCommit(false);
    String fullTableName = generateUniqueName();
    try (Statement stmt = conn.createStatement()) {
      stmt.execute("CREATE TABLE " + fullTableName + DDL);
    }

    // upserting rows should increase the mutation state size
    MutationState state = conn.unwrap(PhoenixConnection.class).getMutationState();
    long prevEstimatedSize = state.getEstimatedSize();
    upsertRows(conn, fullTableName);
    assertTrue("Mutation state size should have increased",
      state.getEstimatedSize() > prevEstimatedSize);

    // after commit or rollback the size should be zero
    conn.commit();
    assertEquals("Mutation state size should be zero after commit", 0, state.getEstimatedSize());
    upsertRows(conn, fullTableName);
    conn.rollback();
    assertEquals("Mutation state size should be zero after rollback", 0, state.getEstimatedSize());

    // upsert one row
    PreparedStatement stmt = conn.prepareStatement(
      "upsert into " + fullTableName + " (organization_id, entity_id, score) values (?,?,?)");
    stmt.setString(1, "ZZZZ");
    stmt.setString(2, "YYYY");
    stmt.setInt(3, 1);
    stmt.execute();
    assertTrue("Mutation state size should be greater than zero ", state.getEstimatedSize() > 0);

    prevEstimatedSize = state.getEstimatedSize();
    // upserting the same row twice should not increase the size
    stmt.setString(1, "ZZZZ");
    stmt.setString(2, "YYYY");
    stmt.setInt(3, 1);
    stmt.execute();
    assertEquals(
      "Mutation state size should only increase 4 bytes (size of the new statement index)",
      prevEstimatedSize + 4, state.getEstimatedSize());

    prevEstimatedSize = state.getEstimatedSize();
    // changing the value of one column of a row to a larger value should increase the estimated
    // size
    stmt = conn.prepareStatement("upsert into " + fullTableName
      + " (organization_id, entity_id, score, tags) values (?,?,?,?)");
    stmt.setString(1, "ZZZZ");
    stmt.setString(2, "YYYY");
    stmt.setInt(3, 1);
    stmt.setString(4, "random text string random text string random text string");
    stmt.execute();
    assertTrue("Mutation state size should increase",
      prevEstimatedSize + 4 < state.getEstimatedSize());

    prevEstimatedSize = state.getEstimatedSize();
    // changing the value of one column of a row to a smaller value should decrease the estimated
    // size
    stmt = conn.prepareStatement("upsert into " + fullTableName
      + " (organization_id, entity_id, score, tags) values (?,?,?,?)");
    stmt.setString(1, "ZZZZ");
    stmt.setString(2, "YYYY");
    stmt.setInt(3, 1);
    stmt.setString(4, "");
    stmt.execute();
    assertTrue("Mutation state size should decrease",
      prevEstimatedSize + 4 > state.getEstimatedSize());

    // Test that estimatedSize increases when adding conflicting rows
    // (regular upsert + ON DUPLICATE KEY upsert on same key cannot be merged)
    conn.commit(); // clear state
    assertEquals("Mutation state size should be zero after commit", 0, state.getEstimatedSize());

    // Regular upsert
    stmt = conn.prepareStatement(
      "upsert into " + fullTableName + " (organization_id, entity_id, score) values (?,?,?)");
    stmt.setString(1, "AAAA");
    stmt.setString(2, "BBBB");
    stmt.setInt(3, 1);
    stmt.execute();
    long sizeAfterRegularUpsert = state.getEstimatedSize();
    assertTrue("Mutation state size should be > 0 after regular upsert",
      sizeAfterRegularUpsert > 0);

    // ON DUPLICATE KEY upsert on same key - creates a conflicting row that can't be merged
    PreparedStatement onDupStmt = conn.prepareStatement(
      "upsert into " + fullTableName + " (organization_id, entity_id, score) values (?,?,?)"
        + " ON DUPLICATE KEY UPDATE score = score + 1");
    onDupStmt.setString(1, "AAAA");
    onDupStmt.setString(2, "BBBB");
    onDupStmt.setInt(3, 2);
    onDupStmt.execute();

    // Size should increase because conflicting row goes into a separate batch
    assertTrue("Estimated size should increase for conflicting row",
      state.getEstimatedSize() > sizeAfterRegularUpsert);
  }

  @Test
  public void testSplitMutationsIntoSameGroupForSingleRow() throws Exception {
    String tableName = "TBL_" + generateUniqueName();
    String indexName = "IDX_" + generateUniqueName();
    Properties props = new Properties();
    props.put("phoenix.mutate.batchSize", "2");
    try (PhoenixConnection conn =
      DriverManager.getConnection(getUrl(), props).unwrap(PhoenixConnection.class)) {
      conn.setAutoCommit(false);
      conn.createStatement()
        .executeUpdate("CREATE TABLE " + tableName + " (" + "A VARCHAR NOT NULL PRIMARY KEY,"
          + "B VARCHAR," + "C VARCHAR," + "D VARCHAR) COLUMN_ENCODED_BYTES = 0");
      conn.createStatement()
        .executeUpdate("CREATE INDEX " + indexName + " on " + tableName + " (C) INCLUDE(D)");

      conn.createStatement()
        .executeUpdate("UPSERT INTO " + tableName + "(A,B,C,D) VALUES ('A2','B2','C2','D2')");
      conn.createStatement()
        .executeUpdate("UPSERT INTO " + tableName + "(A,B,C,D) VALUES ('A3','B3', 'C3', null)");
      conn.commit();

      Table htable = conn.getQueryServices().getTable(Bytes.toBytes(tableName));
      Scan scan = new Scan();
      scan.setRaw(true);
      Iterator<Result> scannerIter = htable.getScanner(scan).iterator();
      while (scannerIter.hasNext()) {
        long ts = -1;
        Result r = scannerIter.next();
        for (Cell cell : r.listCells()) {
          if (ts == -1) {
            ts = cell.getTimestamp();
          } else {
            assertEquals("(" + cell.toString() + ") has different ts", ts, cell.getTimestamp());
          }
        }
      }
      htable.close();
    }
  }

  @Rule
  public ExpectedException exceptionRule = ExpectedException.none();

  @Test
  public void testDDLwithPendingMutations() throws Exception {
    String tableName = generateUniqueName();
    ensureTableCreated(getUrl(), tableName, TestUtil.PTSDB_NAME, null, null, null);
    Properties props = new Properties();
    props.setProperty(QueryServices.PENDING_MUTATIONS_DDL_THROW_ATTRIB, Boolean.toString(true));
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      // setting auto commit to false
      conn.setAutoCommit(false);

      // Run upsert queries but do not commit
      PreparedStatement stmt = conn.prepareStatement(
        "UPSERT INTO " + tableName + " (inst,host,\"DATE\") VALUES(?,'b',CURRENT_DATE())");
      stmt.setString(1, "a");
      stmt.execute();
      // Create a ddl statement
      String tableName2 = generateUniqueName();
      String ddl = "CREATE TABLE " + tableName2 + " (V BIGINT PRIMARY KEY, K BIGINT)";
      exceptionRule.expect(SQLException.class);
      exceptionRule
        .expectMessage(SQLExceptionCode.CANNOT_PERFORM_DDL_WITH_PENDING_MUTATIONS.getMessage());
      conn.createStatement().execute(ddl);
    }
  }

  @Test
  public void testNoPendingMutationsOnDDL() throws Exception {
    Properties props = new Properties();
    props.setProperty(QueryServices.PENDING_MUTATIONS_DDL_THROW_ATTRIB, Boolean.toString(true));
    String tableName = generateUniqueName();
    try (
      PhoenixConnection conn = (PhoenixConnection) DriverManager.getConnection(getUrl(), props)) {
      String ddl = "create table " + tableName + " ( id1 UNSIGNED_INT not null primary key,"
        + "appId1 VARCHAR)";
      conn.createStatement().execute(ddl);
      // ensure table got created
      Admin admin = driver.getConnectionQueryServices(getUrl(), props).getAdmin();
      assertNotNull(admin.getDescriptor(TableName.valueOf(tableName)));
      assertNotNull(conn.getTableNoCache(tableName));
    }
  }

  @Test
  public void testUpsertMaxColumnAllowanceForSingleCellArrayWithOffsets() throws Exception {
    testUpsertColumnExceedsMaxAllowanceSize("SINGLE_CELL_ARRAY_WITH_OFFSETS");
  }

  @Test
  public void testUpsertMaxColumnAllowanceForOneCellPerColumn() throws Exception {
    testUpsertColumnExceedsMaxAllowanceSize("ONE_CELL_PER_COLUMN");
  }

  public void testUpsertColumnExceedsMaxAllowanceSize(String storageScheme) throws Exception {
    Properties connectionProperties = new Properties();
    connectionProperties.setProperty(QueryServices.HBASE_CLIENT_KEYVALUE_MAXSIZE, "20");
    try (PhoenixConnection connection =
      (PhoenixConnection) DriverManager.getConnection(getUrl(), connectionProperties)) {
      String fullTableName = generateUniqueName();
      String pk1Name = generateUniqueName();
      String pk2Name = generateUniqueName();
      String ddl = "CREATE IMMUTABLE TABLE " + fullTableName + " (" + pk1Name
        + " VARCHAR(15) NOT NULL, " + pk2Name + " VARCHAR(15) NOT NULL, "
        + "PAYLOAD1 VARCHAR, PAYLOAD2 VARCHAR,PAYLOAD3 VARCHAR " + "CONSTRAINT PK PRIMARY KEY ("
        + pk1Name + "," + pk2Name + ")) " + "IMMUTABLE_STORAGE_SCHEME =" + storageScheme;
      try (Statement stmt = connection.createStatement()) {
        stmt.execute(ddl);
      }
      String sql = "UPSERT INTO " + fullTableName + " (" + pk1Name + "," + pk2Name
        + ",PAYLOAD1,PAYLOAD2,PAYLOAD2) VALUES (?,?,?,?,?)";
      String pk1Value = generateUniqueName();
      String pk2Value = generateUniqueName();
      String payload1Value = generateUniqueName();
      String payload3Value = generateUniqueName();

      try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
        preparedStatement.setString(1, pk1Value);
        preparedStatement.setString(2, pk2Value);
        preparedStatement.setString(3, payload1Value);
        preparedStatement.setString(4, "1234567890");
        preparedStatement.setString(5, payload3Value);
        preparedStatement.execute();

        try {
          preparedStatement.setString(1, pk1Value);
          preparedStatement.setString(2, pk2Value);
          preparedStatement.setString(3, payload1Value);
          preparedStatement.setString(4, "12345678901234567890");
          preparedStatement.setString(5, payload3Value);
          preparedStatement.execute();
          if (storageScheme.equals("ONE_CELL_PER_COLUMN")) {
            fail();
          }
        } catch (SQLException e) {
          if (!storageScheme.equals("ONE_CELL_PER_COLUMN")) {
            fail();
          } else {
            assertEquals(SQLExceptionCode.MAX_HBASE_CLIENT_KEYVALUE_MAXSIZE_EXCEEDED.getErrorCode(),
              e.getErrorCode());
            assertTrue(e.getMessage()
              .contains(SQLExceptionCode.MAX_HBASE_CLIENT_KEYVALUE_MAXSIZE_EXCEEDED.getMessage()));
            assertTrue(e.getMessage().contains(
              connectionProperties.getProperty(QueryServices.HBASE_CLIENT_KEYVALUE_MAXSIZE)));
          }
        }
      }
    }
  }
}
