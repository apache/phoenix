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

import static org.apache.phoenix.end2end.VectorIndexIT.assertIndexVerifies;
import static org.apache.phoenix.end2end.VectorIndexIT.loadClusteredVectors;
import static org.apache.phoenix.hbase.index.IndexCDCConsumer.INDEX_CDC_CONSUMER_STARTUP_DELAY_MS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.execute.CommitException;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A region server can cache a data table before an IVF index exists on it. A write that uses server
 * metadata then finds no index on the server. The write must fail with INDEX_METADATA_NOT_FOUND, so
 * that the application can retry it. With ALWAYS, the region server reads the table again and
 * maintains the new index.
 */
@Category(NeedsOwnMiniClusterTest.class)
public class VectorIvfStaleServerMetadataIT extends BaseTest {

  private static final Logger LOGGER =
    LoggerFactory.getLogger(VectorIvfStaleServerMetadataIT.class);

  // The index consumer reads the data table this long after the region opens.
  private static final long CONSUMER_DELAY_MS = 500;

  private static final int ROWS = 100;

  // The server cache entry from before the index must not expire before the test commits.
  private static final String STALE_UCF = "600000";

  // A commit of more rows than the batch threshold uses server metadata also with ALWAYS.
  private static final int BATCH_ROWS = 5;

  @BeforeClass
  public static synchronized void doSetup() throws Exception {
    Map<String, String> props = new HashMap<>(4);
    props.put(BaseScannerRegionObserverConstants.PHOENIX_MAX_LOOKBACK_AGE_CONF_KEY,
      Integer.toString(60 * 60));
    props.put(QueryServices.USE_STATS_FOR_PARALLELIZATION, Boolean.toString(false));
    props.put(QueryServices.TASK_HANDLING_INITIAL_DELAY_MS_ATTRIB, Long.toString(Long.MAX_VALUE));
    props.put(INDEX_CDC_CONSUMER_STARTUP_DELAY_MS, Long.toString(CONSUMER_DELAY_MS));
    setUpTestDriver(new ReadOnlyProps(props));
  }

  private static Connection connect() throws SQLException {
    Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
    return DriverManager.getConnection(getUrl(), props);
  }

  /**
   * Creates and loads the table, and waits for the region server to cache it. Then adds a trained
   * IVF index to the table.
   */
  private static void createTableThenIndex(String table, String index, String ucf)
    throws Exception {
    try (Connection conn = connect()) {
      conn.createStatement()
        .execute("CREATE TABLE " + table
          + " (ID VARCHAR NOT NULL PRIMARY KEY, V VECTOR(FLOAT, 4), LABEL VARCHAR)"
          + " UPDATE_CACHE_FREQUENCY=" + ucf);
      loadClusteredVectors(conn, table, ROWS);
      Thread.sleep(CONSUMER_DELAY_MS + 2500);
      conn.createStatement()
        .execute("CREATE VECTOR INDEX " + index + " ON " + table
          + " (V) INCLUDE (LABEL) WITH (algorithm = 'IVF', metric = 'L2', lists = 4,"
          + " sample_size = 100)");
      PTable pindex = conn.unwrap(PhoenixConnection.class).getTableNoCache(index);
      assertEquals(PIndexState.ACTIVE, pindex.getIndexState());
      assertNotNull("training records a generation", pindex.getVectorCentroidGeneration());
    }
  }

  /** Upserts the given number of new rows in one commit. Returns the commit failure or null. */
  private static SQLException upsert(String table, int firstId, int rows) throws SQLException {
    try (Connection conn = connect(); PreparedStatement ps =
      conn.prepareStatement("UPSERT INTO " + table + " (ID, V, LABEL) VALUES (?, ?, ?)")) {
      for (int id = firstId; id < firstId + rows; id++) {
        ps.setString(1, String.format("new_%03d", id));
        Array vec = conn.createArrayOf("FLOAT", new Float[] { 1.0f, 2.0f, 3.0f, 0.5f });
        ps.setArray(2, vec);
        ps.setString(3, "new_" + id);
        ps.executeUpdate();
      }
      try {
        conn.commit();
        return null;
      } catch (SQLException e) {
        LOGGER.info("Commit of {} rows into {} failed", rows, table, e);
        return e;
      }
    }
  }

  private static String describe(Throwable t) {
    if (t == null) {
      return "none";
    }
    StringWriter sw = new StringWriter();
    t.printStackTrace(new PrintWriter(sw));
    return sw.toString();
  }

  private static long count(String sql) throws SQLException {
    try (Connection conn = connect(); ResultSet rs = conn.createStatement().executeQuery(sql)) {
      assertTrue(rs.next());
      return rs.getLong(1);
    }
  }

  @Test
  public void testWriteAfterVectorIndexCreateWithStaleServerMetadataFails() throws Exception {
    String table = "T_" + generateUniqueName();
    String index = "I_" + generateUniqueName();
    createTableThenIndex(table, index, STALE_UCF);
    SQLException failure = upsert(table, 1, 1);
    if (failure == null) {
      // On a slow host the region server can read the table first after the index exists. Then
      // the commit must also write the index row, but the test cannot check the stale entry.
      assertEquals(ROWS + 1, count("SELECT /*+ NO_INDEX */ COUNT(*) FROM " + table));
      assertEquals(ROWS + 1, count("SELECT COUNT(*) FROM " + index));
    }
    assumeTrue("the region server did not cache the table before the index", failure != null);
    assertTrue("commit failed: " + describe(failure), failure instanceof CommitException);
    Throwable cause = failure.getCause();
    assertTrue("cause: " + describe(cause), cause instanceof SQLException);
    assertEquals(describe(cause), SQLExceptionCode.INDEX_METADATA_NOT_FOUND.getErrorCode(),
      ((SQLException) cause).getErrorCode());
    // The mini-cluster has one region server, so the failed commit writes no data row and no
    // index row.
    assertEquals(ROWS, count("SELECT /*+ NO_INDEX */ COUNT(*) FROM " + table));
    assertEquals(ROWS, count("SELECT COUNT(*) FROM " + index));
    assertIndexVerifies(table, index, ROWS);
  }

  @Test
  public void testWriteAfterVectorIndexCreateWithUpdateCacheFrequencyAlways() throws Exception {
    String table = "T_" + generateUniqueName();
    String index = "I_" + generateUniqueName();
    createTableThenIndex(table, index, "ALWAYS");
    SQLException failure = upsert(table, 1, 1);
    assertNull("commit failed: " + describe(failure), failure);
    failure = upsert(table, 2, BATCH_ROWS);
    assertNull("commit failed: " + describe(failure), failure);
    long rows = ROWS + 1 + BATCH_ROWS;
    assertEquals(rows, count("SELECT /*+ NO_INDEX */ COUNT(*) FROM " + table));
    assertEquals(rows, count("SELECT COUNT(*) FROM " + index));
    assertIndexVerifies(table, index, rows);
  }
}
