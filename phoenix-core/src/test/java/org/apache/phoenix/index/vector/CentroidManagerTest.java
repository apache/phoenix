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
package org.apache.phoenix.index.vector;

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.CLUSTER_SIZE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.LAST_REBUILD_TIME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.LAST_SCORECARD_UPDATE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.REASSIGN_COUNT;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.REBUILD_STATE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SENTINEL_CENTROID_ID;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SKEW_METRICS;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TRIGGER_REASON;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.phoenix.schema.types.PVectorFloat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class CentroidManagerTest {

  private Connection mockConnection;

  @Before
  public void setUp() {
    mockConnection = mock(Connection.class);
    CentroidManager.clearThreadLocalConnection();
    CentroidManager.setDefaultConnection(null);
  }

  @After
  public void tearDown() {
    CentroidManager.clearThreadLocalConnection();
    CentroidManager.setDefaultConnection(null);
  }

  @Test
  public void testConstructorAndGetter() {
    CentroidManager manager = new CentroidManager(mockConnection);
    assertSame(mockConnection, manager.getConnection());

    try {
      new CentroidManager(null);
      fail("Constructor with null connection should throw NPE");
    } catch (NullPointerException expected) {
    }
  }

  @Test
  public void testThreadLocalAndDefaultConnection() {
    assertNull(CentroidManager.getThreadLocalConnection());
    assertNull(CentroidManager.getDefaultConnection());

    CentroidManager.setDefaultConnection(mockConnection);
    assertSame(mockConnection, CentroidManager.getDefaultConnection());

    Connection threadConn = mock(Connection.class);
    CentroidManager.setThreadLocalConnection(threadConn);
    assertSame(threadConn, CentroidManager.getThreadLocalConnection());

    CentroidManager.clearThreadLocalConnection();
    assertNull(CentroidManager.getThreadLocalConnection());
    assertSame(mockConnection, CentroidManager.getDefaultConnection());
  }

  @Test
  public void testResolveConnectionThrowsWhenNoneConfigured() {
    try {
      CentroidManager.get().persistCentroids("test_idx", 1L, Collections.<byte[]> emptyList());
      fail("Should throw IllegalStateException when no connection is configured");
    } catch (IllegalStateException expected) {
    } catch (SQLException e) {
      fail("Unexpected SQLException: " + e.getMessage());
    }

    CentroidManager.setDefaultConnection(mockConnection);
    assertSame(mockConnection, CentroidManager.get().getConnection());
    assertSame(mockConnection, CentroidManager.get(mockConnection).getConnection());
  }

  @Test
  public void testValidationPersistCentroids() throws Exception {
    try {
      CentroidManager.persistCentroids(null, "test_idx", 1L, Collections.<byte[]> emptyList());
      fail("Should fail on null connection");
    } catch (IllegalArgumentException expected) {
    }

    try {
      CentroidManager.persistCentroids(mockConnection, null, 1L, Collections.<byte[]> emptyList());
      fail("Should fail on null indexName");
    } catch (IllegalArgumentException expected) {
    }

    try {
      CentroidManager.persistCentroids(mockConnection, "   ", 1L, Collections.<byte[]> emptyList());
      fail("Should fail on empty indexName");
    } catch (IllegalArgumentException expected) {
    }

    try {
      CentroidManager.persistCentroids(mockConnection, "test_idx", -1L,
        Collections.<byte[]> emptyList());
      fail("Should fail on negative generation");
    } catch (IllegalArgumentException expected) {
    }

    try {
      CentroidManager.persistCentroids(mockConnection, "test_idx", 1L, (List<byte[]>) null);
      fail("Should fail on null centroids list");
    } catch (IllegalArgumentException expected) {
    }

    try {
      CentroidManager.persistCentroids(mockConnection, "test_idx", 1L, (KMeansResult) null);
      fail("Should fail on null KMeansResult");
    } catch (IllegalArgumentException expected) {
    }

    try {
      CentroidManager.persistCentroidsFromFloatList(mockConnection, "test_idx", 1L,
        (List<float[]>) null);
      fail("Should fail on null float centroids list");
    } catch (IllegalArgumentException expected) {
    }
  }

  @Test
  public void testValidationLoadCentroids() throws Exception {
    try {
      CentroidManager.loadCentroids(null, "test_idx", 1L);
      fail("Should fail on null connection");
    } catch (IllegalArgumentException expected) {
    }

    try {
      CentroidManager.loadCentroids(mockConnection, null, 1L);
      fail("Should fail on null indexName");
    } catch (IllegalArgumentException expected) {
    }

    try {
      CentroidManager.loadCentroids(mockConnection, "", 1L);
      fail("Should fail on empty indexName");
    } catch (IllegalArgumentException expected) {
    }
  }

  @Test
  public void testValidationIncrementAndGetGeneration() throws Exception {
    try {
      CentroidManager.incrementGeneration(null, "test_idx");
      fail("Should fail on null connection");
    } catch (IllegalArgumentException expected) {
    }

    try {
      CentroidManager.incrementGeneration(mockConnection, null);
      fail("Should fail on null indexName");
    } catch (IllegalArgumentException expected) {
    }

    try {
      CentroidManager.getGeneration(null, "test_idx");
      fail("Should fail on null connection");
    } catch (IllegalArgumentException expected) {
    }

    try {
      CentroidManager.getGeneration(mockConnection, "  ");
      fail("Should fail on empty indexName");
    } catch (IllegalArgumentException expected) {
    }
  }

  @Test
  public void testValidationDeleteGeneration() throws Exception {
    try {
      CentroidManager.deleteGeneration(null, "test_idx", 1L);
      fail("Should fail on null connection");
    } catch (IllegalArgumentException expected) {
    }

    try {
      CentroidManager.deleteGeneration(mockConnection, null, 1L);
      fail("Should fail on null indexName");
    } catch (IllegalArgumentException expected) {
    }

    try {
      CentroidManager.deleteAllCentroids(null, "test_idx");
      fail("Should fail on null connection");
    } catch (IllegalArgumentException expected) {
    }

    try {
      CentroidManager.deleteAllCentroids(mockConnection, "");
      fail("Should fail on empty indexName");
    } catch (IllegalArgumentException expected) {
    }
  }

  @Test
  public void testVectorFloatSerializationHelper() {
    float[] v0 = new float[] { 1.5f, 2.5f, -3.5f, 0.0f };
    byte[] bytes = PVectorFloat.INSTANCE.toBytes(v0);
    float[] deserialized = (float[]) PVectorFloat.INSTANCE.toObject(bytes);
    assertArrayEquals(v0, deserialized, 1e-6f);
  }

  @Test
  public void testGenerationSummarySkewMetricsRoundTrip() {
    ClusterSkewMetrics skew = ClusterSkewMetrics.compute(new int[] { 10, 20, 30, 40 });

    GenerationSummary fromObject = new GenerationSummary.Builder().setIndexName("MY_IDX")
      .setGenerationId(1L).setSkewMetrics(skew).setRebuildState("A").build();
    byte[] encoded = fromObject.getSkewMetricsBytes();
    assertNotNull(encoded);

    GenerationSummary fromBytes = new GenerationSummary.Builder().setIndexName("MY_IDX")
      .setGenerationId(1L).setSkewMetricsBytes(encoded).setRebuildState("A").build();
    assertEquals(skew, fromBytes.getSkewMetrics());
    assertNull("a blob that decoded is not a decode failure",
      fromBytes.getSkewMetricsDecodeError());
    assertEquals(fromObject, fromBytes);
    assertEquals(fromObject.hashCode(), fromBytes.hashCode());
    assertArrayEquals("re-encoding must reproduce the stored bytes", encoded,
      fromBytes.getSkewMetricsBytes());
  }

  @Test
  public void testUndecodableSkewMetricsAreDistinguishableFromAbsent() {
    byte[] valid = new GenerationSummary.Builder()
      .setSkewMetrics(ClusterSkewMetrics.compute(new int[] { 1, 9 })).build().getSkewMetricsBytes();
    byte[] truncated = Arrays.copyOf(valid, valid.length / 2);

    GenerationSummary undecodable = new GenerationSummary.Builder().setIndexName("MY_IDX")
      .setGenerationId(1L).setSkewMetricsBytes(truncated).build();
    assertNull(undecodable.getSkewMetrics());
    assertNotNull("a truncated blob must be reported, not swallowed",
      undecodable.getSkewMetricsDecodeError());
    assertNull("an undecodable blob must not be written back as metrics",
      undecodable.getSkewMetricsBytes());

    GenerationSummary absent = new GenerationSummary.Builder().setIndexName("MY_IDX")
      .setGenerationId(1L).setSkewMetricsBytes(null).build();
    assertNull(absent.getSkewMetrics());
    assertNull("absent metrics are not a decode failure", absent.getSkewMetricsDecodeError());

    assertFalse("the two cases must not compare equal", undecodable.equals(absent));
  }

  /** Tests that partial writes omit absent columns from the statement. */
  @Test
  public void testPartialWritesOmitAbsentColumns() throws Exception {
    PreparedStatement ps = mock(PreparedStatement.class);
    Connection conn = mock(Connection.class);
    ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
    when(conn.prepareStatement(anyString())).thenReturn(ps);

    CentroidManager.persistScorecardRow(conn, "idx", 1L, 7, 42L, null, null);
    verify(conn).prepareStatement(sqlCaptor.capture());
    String sql = sqlCaptor.getValue();
    assertTrue(sql, sql.contains(CLUSTER_SIZE));
    assertFalse(sql, sql.contains(REASSIGN_COUNT));
    assertFalse(sql, sql.contains(LAST_SCORECARD_UPDATE));
    assertEquals(sql, 4, countPlaceholders(sql));
    verify(ps, never()).setNull(anyInt(), anyInt());

    reset(conn, ps);
    when(conn.prepareStatement(anyString())).thenReturn(ps);
    sqlCaptor = ArgumentCaptor.forClass(String.class);

    CentroidManager.persistGenerationSummary(conn, "idx", 1L, null, "A", null, null, 9000L);
    verify(conn).prepareStatement(sqlCaptor.capture());
    sql = sqlCaptor.getValue();
    assertTrue(sql, sql.contains(REBUILD_STATE));
    assertTrue(sql, sql.contains(LAST_SCORECARD_UPDATE));
    assertFalse(sql, sql.contains(SKEW_METRICS));
    assertFalse(sql, sql.contains(TRIGGER_REASON));
    assertFalse(sql, sql.contains(LAST_REBUILD_TIME));
    // CENTROID_ID is inlined; INDEX_NAME, GENERATION_ID, REBUILD_STATE and LAST_SCORECARD_UPDATE
    // are bound
    assertEquals(sql, 4, countPlaceholders(sql));
    verify(ps, never()).setNull(anyInt(), anyInt());
  }

  /** Tests that batch scorecard persistence compiles once and executes in a single commit. */
  @Test
  public void testPersistScorecardBatchesStatementsAndCommits() throws Exception {
    PreparedStatement ps = mock(PreparedStatement.class);
    Connection conn = mock(Connection.class);
    when(conn.prepareStatement(anyString())).thenReturn(ps);

    List<ScorecardRow> rows = new ArrayList<>();
    for (int i = 0; i < 64; i++) {
      rows.add(new ScorecardRow("idx", 3L, i, 100L + i, 2L, 5000L));
    }
    CentroidManager.persistScorecard(conn, rows);

    verify(conn, times(1)).prepareStatement(anyString());
    verify(ps, times(64)).executeUpdate();
    verify(conn, times(1)).commit();
  }

  @Test
  public void testPersistScorecardRejectsSentinelRowBeforeCommitting() throws Exception {
    PreparedStatement ps = mock(PreparedStatement.class);
    Connection conn = mock(Connection.class);
    when(conn.prepareStatement(anyString())).thenReturn(ps);

    try {
      CentroidManager.persistScorecard(conn,
        Arrays.asList(new ScorecardRow("idx", 1L, 0, 10L, 0L, null),
          new ScorecardRow("idx", 1L, SENTINEL_CENTROID_ID, 10L, 0L, null)));
      fail("Should reject the generation summary's sentinel ID in a scorecard batch");
    } catch (IllegalArgumentException expected) {
    }
    verify(conn, never()).commit();

    CentroidManager.persistScorecard(conn, Collections.<ScorecardRow> emptyList());
    verify(conn, never()).commit();
  }

  private static int countPlaceholders(String sql) {
    int count = 0;
    for (int i = 0; i < sql.length(); i++) {
      if (sql.charAt(i) == '?') {
        count++;
      }
    }
    return count;
  }

  @Test
  public void testValidationPersistScorecardRow() throws Exception {
    try {
      CentroidManager.persistScorecardRow(null, new ScorecardRow("idx", 1L, 0, 10L, 0L, null));
      fail("Should fail on null connection");
    } catch (IllegalArgumentException expected) {
    }

    try {
      CentroidManager.persistScorecardRow(mockConnection, null);
      fail("Should fail on null row");
    } catch (IllegalArgumentException expected) {
    }

    try {
      CentroidManager.persistScorecardRow(mockConnection, null, 1L, 0, 10L, 0L, null);
      fail("Should fail on null indexName");
    } catch (IllegalArgumentException expected) {
    }

    try {
      CentroidManager.persistScorecardRow(mockConnection, "   ", 1L, 0, 10L, 0L, null);
      fail("Should fail on empty indexName");
    } catch (IllegalArgumentException expected) {
    }

    try {
      CentroidManager.persistScorecardRow(mockConnection, "idx", -1L, 0, 10L, 0L, null);
      fail("Should fail on negative generation");
    } catch (IllegalArgumentException expected) {
    }

    try {
      CentroidManager.persistScorecardRow(mockConnection, "idx", 1L, -1, 10L, 0L, null);
      fail("Should fail on negative centroidId");
    } catch (IllegalArgumentException expected) {
    }
  }

  @Test
  public void testValidationLoadScorecard() throws Exception {
    try {
      CentroidManager.loadScorecard(null, "idx", 1L);
      fail("Should fail on null connection");
    } catch (IllegalArgumentException expected) {
    }

    try {
      CentroidManager.loadScorecard(mockConnection, null, 1L);
      fail("Should fail on null indexName");
    } catch (IllegalArgumentException expected) {
    }

    try {
      CentroidManager.loadScorecard(mockConnection, "", 1L);
      fail("Should fail on empty indexName");
    } catch (IllegalArgumentException expected) {
    }

    try {
      CentroidManager.loadScorecard(mockConnection, "idx", -1L);
      fail("Should fail on negative generation");
    } catch (IllegalArgumentException expected) {
    }
  }

  @Test
  public void testValidationPersistGenerationSummary() throws Exception {
    try {
      CentroidManager.persistGenerationSummary(null,
        new GenerationSummary.Builder().setIndexName("idx").setGenerationId(1L).build());
      fail("Should fail on null connection");
    } catch (IllegalArgumentException expected) {
    }

    try {
      CentroidManager.persistGenerationSummary(mockConnection, null);
      fail("Should fail on null summary");
    } catch (IllegalArgumentException expected) {
    }

    try {
      CentroidManager.persistGenerationSummary(mockConnection, null, 1L, null, "A", "INIT", null,
        null);
      fail("Should fail on null indexName");
    } catch (IllegalArgumentException expected) {
    }

    try {
      CentroidManager.persistGenerationSummary(mockConnection, "   ", 1L, null, "A", "INIT", null,
        null);
      fail("Should fail on empty indexName");
    } catch (IllegalArgumentException expected) {
    }

    try {
      CentroidManager.persistGenerationSummary(mockConnection, "idx", -1L, null, "A", "INIT", null,
        null);
      fail("Should fail on negative generation");
    } catch (IllegalArgumentException expected) {
    }
  }

  @Test
  public void testValidationLoadGenerationSummary() throws Exception {
    try {
      CentroidManager.loadGenerationSummary(null, "idx", 1L);
      fail("Should fail on null connection");
    } catch (IllegalArgumentException expected) {
    }

    try {
      CentroidManager.loadGenerationSummary(mockConnection, null, 1L);
      fail("Should fail on null indexName");
    } catch (IllegalArgumentException expected) {
    }

    try {
      CentroidManager.loadGenerationSummary(mockConnection, "", 1L);
      fail("Should fail on empty indexName");
    } catch (IllegalArgumentException expected) {
    }

    try {
      CentroidManager.loadGenerationSummary(mockConnection, "idx", -1L);
      fail("Should fail on negative generation");
    } catch (IllegalArgumentException expected) {
    }
  }

  @Test
  public void testValidationListGenerations() throws Exception {
    try {
      CentroidManager.listGenerations(null, "idx");
      fail("Should fail on null connection");
    } catch (IllegalArgumentException expected) {
    }

    try {
      CentroidManager.listGenerations(mockConnection, null);
      fail("Should fail on null indexName");
    } catch (IllegalArgumentException expected) {
    }

    try {
      CentroidManager.listGenerations(mockConnection, "   ");
      fail("Should fail on empty indexName");
    } catch (IllegalArgumentException expected) {
    }
  }
}
