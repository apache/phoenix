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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PNameFactory;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.types.PVectorDouble;
import org.junit.Test;

public class VectorIndexTrainerTest {

  /**
   * Verifies that the training sample does not include a VECTOR(DOUBLE) value with an element
   * beyond float range. That element narrows to infinity and can corrupt the trained centroids.
   */
  @Test
  public void testOutOfFloatRangeVectorLeftOutOfSample() throws SQLException {
    KMeansResult result = VectorIndexTrainer
      .train(connection(new double[] { 0, 0 }, new double[] { 0, 1 }, new double[] { 1e300, 0 },
        new double[] { 10, 10 }, new double[] { 10, 11 }), dataTable(), index(2));
    for (float[] centroid : result.getCentroids()) {
      for (float x : centroid) {
        assertTrue(Float.isFinite(x));
      }
    }
    assertEquals(4, result.getAssignments().length);
  }

  /**
   * Verifies that train() returns null, and so defers training, when the sample has fewer trainable
   * vectors than the index has lists. The row count is sufficient here, but only one vector is
   * finite in float range.
   */
  @Test
  public void testTooFewTrainableVectorsDefersTraining() throws SQLException {
    assertNull(VectorIndexTrainer.train(
      connection(new double[] { 1e300, 0 }, new double[] { 0, -1e300 }, new double[] { 1, 1 }),
      dataTable(), index(2)));
  }

  private static PhoenixConnection connection(Object... vectors) throws SQLException {
    Statement stmt = mock(Statement.class);
    when(stmt.executeQuery(anyString()))
      .thenAnswer(inv -> ((String) inv.getArgument(0)).startsWith("SELECT COUNT(*)")
        ? rows((long) vectors.length)
        : rows(vectors));
    PhoenixConnection conn = mock(PhoenixConnection.class);
    when(conn.createStatement()).thenReturn(stmt);
    return conn;
  }

  private static ResultSet rows(Object... values) throws SQLException {
    ResultSet rs = mock(ResultSet.class);
    AtomicInteger row = new AtomicInteger(-1);
    when(rs.next()).thenAnswer(inv -> row.incrementAndGet() < values.length);
    when(rs.getObject(1)).thenAnswer(inv -> values[row.get()]);
    when(rs.getLong(1)).thenAnswer(inv -> (Long) values[row.get()]);
    return rs;
  }

  private static PTable dataTable() {
    PTable table = mock(PTable.class);
    when(table.getName()).thenReturn(PNameFactory.newName("T"));
    return table;
  }

  private static PTable index(int lists) {
    PColumn column = mock(PColumn.class);
    when(column.getFamilyName()).thenReturn(PNameFactory.newName("0"));
    when(column.getDataType()).thenReturn(PVectorDouble.INSTANCE);
    when(column.getExpressionStr()).thenReturn("V");
    PTable index = mock(PTable.class);
    when(index.getName()).thenReturn(PNameFactory.newName("IDX"));
    when(index.getColumns()).thenReturn(Collections.singletonList(column));
    when(index.getVectorIvfLists()).thenReturn(lists);
    when(index.getVectorIvfSampleSize()).thenReturn(100);
    when(index.getVectorDistanceMetric()).thenReturn("L2");
    return index;
  }
}
