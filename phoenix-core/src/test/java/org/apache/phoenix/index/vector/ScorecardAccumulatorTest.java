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

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
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
    ScorecardAccumulator.getInstance(conf).accumulate(batch);
    assertEquals(2, assignments("IDX_ASSIGN") - before);
  }

  /** Returns a mock connection whose generation liveness checks return {@code live} in turn. */
  private static Connection connection(PreparedStatement upsert, PreparedStatement delete,
    Boolean... live) throws Exception {
    Connection conn = mock(Connection.class);
    PreparedStatement check = mock(PreparedStatement.class);
    ResultSet[] results = new ResultSet[live.length];
    for (int i = 0; i < live.length; i++) {
      results[i] = mock(ResultSet.class);
      when(results[i].next()).thenReturn(live[i]);
    }
    when(check.executeQuery()).thenReturn(results[0],
      Arrays.copyOfRange(results, 1, results.length));
    when(conn.prepareStatement(anyString())).thenAnswer(invocation -> {
      String sql = invocation.getArgument(0);
      return sql.startsWith("SELECT") ? check : sql.startsWith("UPSERT") ? upsert : delete;
    });
    return conn;
  }

  private static Map<Key, long[]> deltas() {
    Map<Key, long[]> deltas = new HashMap<>();
    deltas.put(new Key("IDX_FLUSH", 7L, 3), new long[] { 1, 0 });
    return deltas;
  }

  /**
   * Verifies that a flush removes the scorecard rows that its increments create again for a deleted
   * generation. An index drop or a retirement can delete the generation between the liveness check
   * and the commit.
   */
  @Test
  public void testFlushRemovesRowsItRecreatesForADeletedGeneration() throws Exception {
    PreparedStatement upsert = mock(PreparedStatement.class);
    PreparedStatement delete = mock(PreparedStatement.class);
    ScorecardAccumulator.flush(connection(upsert, delete, true, false), new Key("IDX_FLUSH", 7L, 0),
      deltas());
    verify(upsert).executeUpdate();
    verify(delete).setString(1, "IDX_FLUSH");
    verify(delete).setLong(2, 7L);
    verify(delete).executeUpdate();
  }

  @Test
  public void testFlushToALiveGenerationDeletesNothing() throws Exception {
    PreparedStatement upsert = mock(PreparedStatement.class);
    PreparedStatement delete = mock(PreparedStatement.class);
    ScorecardAccumulator.flush(connection(upsert, delete, true, true), new Key("IDX_FLUSH", 7L, 0),
      deltas());
    verify(upsert).executeUpdate();
    verify(delete, never()).executeUpdate();
  }

  @Test
  public void testFlushToADeletedGenerationWritesNothing() throws Exception {
    PreparedStatement upsert = mock(PreparedStatement.class);
    PreparedStatement delete = mock(PreparedStatement.class);
    ScorecardAccumulator.flush(connection(upsert, delete, false), new Key("IDX_FLUSH", 7L, 0),
      deltas());
    verify(upsert, never()).executeUpdate();
    verify(delete, never()).executeUpdate();
  }
}
