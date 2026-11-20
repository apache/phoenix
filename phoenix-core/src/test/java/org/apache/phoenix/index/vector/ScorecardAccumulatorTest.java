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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.phoenix.hbase.index.metrics.MetricsIndexerSourceFactory;
import org.apache.phoenix.hbase.index.metrics.MetricsVectorIndexSource;
import org.apache.phoenix.hbase.index.metrics.MetricsVectorIndexSourceImpl;
import org.apache.phoenix.index.IndexMaintainer;
import org.apache.phoenix.index.vector.ScorecardAccumulator.Key;
import org.apache.phoenix.query.QueryServices;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class ScorecardAccumulatorTest {

  private static long assignments(String indexName) {
    return ((MetricsVectorIndexSourceImpl) MetricsIndexerSourceFactory.getInstance()
      .getMetricsVectorIndexSource()).getMetricsRegistry()
        .getCounter(MetricsVectorIndexSource.VECTOR_CENTROID_ASSIGNMENTS + "." + indexName, 0L)
        .value();
  }

  /**
   * Verifies that each vector written to a posting list counts as an assignment. The size deltas to
   * one centroid in a batch still add up to one net value.
   */
  @Test
  public void testAssignmentsCountEveryVectorAddedToAPostingList() {
    IndexMaintainer maintainer = mock(IndexMaintainer.class);
    when(maintainer.getLogicalIndexName()).thenReturn("IDX_ASSIGN");
    when(maintainer.getCentroidGeneration()).thenReturn(1L);
    // The first byte of each index row key in this test is the centroid ID
    when(maintainer.getCentroidId(any(byte[].class)))
      .thenAnswer(invocation -> (int) invocation.<byte[]> getArgument(0)[0]);
    Map<Key, long[]> batch = new HashMap<>();
    // An insert into centroid 0, a delete from centroid 0, and a move from centroid 0 to 1
    ScorecardAccumulator.collect(maintainer, null, null,
      Collections.singletonList(new Put(new byte[] { 0, 'a' })), batch);
    ScorecardAccumulator.collect(maintainer, null, null,
      Collections.singletonList(new Delete(new byte[] { 0, 'b' })), batch);
    ScorecardAccumulator.collect(maintainer, null, null,
      Arrays.asList(new Put(new byte[] { 1, 'c' }), new Delete(new byte[] { 0, 'c' })), batch);
    assertEquals(-1, batch.get(new Key("IDX_ASSIGN", 1L, 0))[0]);
    assertEquals(1, batch.get(new Key("IDX_ASSIGN", 1L, 1))[0]);

    Configuration conf = new Configuration(false);
    conf.setLong(QueryServices.VECTOR_SCORECARD_FLUSH_INTERVAL_MS_ATTRIB, 0);
    long before = assignments("IDX_ASSIGN");
    ScorecardAccumulator.getInstance(conf).accumulate(batch, 1000L);
    assertEquals(2, assignments("IDX_ASSIGN") - before);
  }

  /**
   * Verifies that a merge of buffered entries adds the deltas and keeps the oldest write time. A
   * recount fence then drops the merged population delta if any part of it is older than the fence.
   */
  @Test
  public void testMergeKeepsOldestWriteTime() {
    Configuration conf = new Configuration(false);
    conf.setLong(QueryServices.VECTOR_SCORECARD_FLUSH_INTERVAL_MS_ATTRIB, 0);
    ScorecardAccumulator accumulator = ScorecardAccumulator.getInstance(conf);
    Key key = new Key("IDX_MERGE", 4L, 2);
    long[][] batches = { { 3, 1, 3 }, { -1, 0, 0 }, { 2, 2, 2 } };
    long[] times = { 2000L, 1000L, 3000L };
    for (int i = 0; i < batches.length; i++) {
      Map<Key, long[]> batch = new HashMap<>();
      batch.put(key, batches[i]);
      accumulator.accumulate(batch, times[i]);
    }
    assertArrayEquals(new long[] { 4, 3, 1000L }, accumulator.buffered(key));
    assertArrayEquals(new long[] { 1, 2, 5L },
      ScorecardAccumulator.combine(new long[] { 3, 2, 5L }, new long[] { -2, 0, 9L }));
  }

  /** Verifies that a batch with no population or reassignment change buffers nothing. */
  @Test
  public void testZeroDeltaIsNotBuffered() {
    Configuration conf = new Configuration(false);
    conf.setLong(QueryServices.VECTOR_SCORECARD_FLUSH_INTERVAL_MS_ATTRIB, 0);
    ScorecardAccumulator accumulator = ScorecardAccumulator.getInstance(conf);
    Key key = new Key("IDX_ZERO", 4L, 0);
    Map<Key, long[]> batch = new HashMap<>();
    batch.put(key, new long[] { 0, 0, 1 });
    accumulator.accumulate(batch, 1000L);
    assertNull(accumulator.buffered(key));
  }

  /**
   * Verifies that a flush is one fenced update for each centroid, with the oldest write time of the
   * entry. The update never creates a row, so the flush needs no liveness read and no compensating
   * delete for a deleted generation.
   */
  @Test
  public void testFlushIsAFencedUpdateOnly() throws Exception {
    Connection conn = mock(Connection.class);
    PreparedStatement upsert = mock(PreparedStatement.class);
    ArgumentCaptor<String> sql = ArgumentCaptor.forClass(String.class);
    when(conn.prepareStatement(sql.capture())).thenReturn(upsert);
    Map<Key, long[]> deltas = new HashMap<>();
    deltas.put(new Key("IDX_FLUSH", 7L, 3), new long[] { 5, 2, 1234L });
    ScorecardAccumulator.flush(conn, new Key("IDX_FLUSH", 7L, 0), deltas);

    assertEquals(1, sql.getAllValues().size());
    String statement = sql.getValue();
    assertTrue(statement, statement.startsWith("UPSERT"));
    assertTrue(statement, statement.contains("ON DUPLICATE KEY UPDATE_ONLY"));
    assertTrue(statement,
      statement.contains("CLUSTER_SIZE = CASE WHEN COALESCE(SCORECARD_FENCE, 0) <= ?"));
    assertTrue(statement, statement.contains("REASSIGN_COUNT = COALESCE(REASSIGN_COUNT, 0) + ?"));
    verify(upsert).setString(1, "IDX_FLUSH");
    verify(upsert).setLong(2, 7L);
    verify(upsert).setInt(3, 3);
    verify(upsert).setLong(4, 1234L);
    verify(upsert).setLong(5, 5L);
    verify(upsert).setLong(6, 2L);
    verify(upsert).executeUpdate();
    verify(upsert, never()).executeQuery();
    verify(conn).commit();
  }
}
