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
package org.apache.phoenix.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PColumnImpl;
import org.apache.phoenix.schema.PNameFactory;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.schema.types.PVectorFloat;
import org.junit.Test;

/**
 * Unit tests verifying selection of the indexed vector column in the presence of covered vector
 * columns.
 */
public class IndexUtilVectorColumnTest {

  private static PColumn column(String name, PDataType<?> type, int position,
    String expressionStr) {
    return new PColumnImpl(PNameFactory.newName(name), PNameFactory.newName("0"), type,
      type == PVectorFloat.INSTANCE ? 4 : null, null, true, position, SortOrder.getDefault(), null,
      null, false, expressionStr, false, false, Bytes.toBytes(name), 0L);
  }

  /** Helper creating an indexed vector column with an expression in COLUMN_DEF. */
  private static PColumn indexedVectorColumn(String name, int position) {
    return column(name, PVectorFloat.INSTANCE, position, "\"0\".\"" + name + "\"");
  }

  /** Helper creating a covered vector column without an expression. */
  private static PColumn coveredVectorColumn(String name, int position) {
    return column(name, PVectorFloat.INSTANCE, position, null);
  }

  @Test
  public void testPicksIndexedColumnOverCoveredVectorColumn() {
    PColumn indexed = indexedVectorColumn("0:V", 3);
    PColumn covered = coveredVectorColumn("0:COV_V", 4);

    // Selection order must remain deterministic irrespective of candidate sequence.
    assertEquals(indexed, IndexUtil.selectVectorColumn(Arrays.asList(indexed, covered)));
    assertEquals(indexed, IndexUtil.selectVectorColumn(Arrays.asList(covered, indexed)));
  }

  @Test
  public void testPicksIndexedColumnWhenCoveredColumnSortsFirst() {
    // Indexed column precedence applies even if a covered column has a smaller ordinal position.
    PColumn covered = coveredVectorColumn("A:COV_V", 1);
    PColumn indexed = indexedVectorColumn("0:V", 9);
    assertEquals(indexed, IndexUtil.selectVectorColumn(Arrays.asList(covered, indexed)));
  }

  @Test
  public void testFallsBackToLowestPositionWhenNoColumnCarriesAnExpression() {
    // Fall back to lowest ordinal position when no candidates define expressions.
    PColumn first = coveredVectorColumn("0:V", 2);
    PColumn second = coveredVectorColumn("0:COV_V", 7);
    assertEquals(first, IndexUtil.selectVectorColumn(Arrays.asList(second, first)));
  }

  @Test
  public void testPrefersLowestPositionAmongExpressionColumns() {
    // When multiple candidates contain expressions, break ties by ordinal position.
    PColumn indexed = indexedVectorColumn("0:V", 3);
    PColumn coveredWithDefault = column("0:COV_V", PVectorFloat.INSTANCE, 8, "ARRAY[1.0,2.0]");
    assertEquals(indexed, IndexUtil.selectVectorColumn(Arrays.asList(coveredWithDefault, indexed)));
  }

  @Test
  public void testIgnoresNonVectorColumns() {
    PColumn varchar = column("0:LABEL", PVarchar.INSTANCE, 1, null);
    PColumn indexed = indexedVectorColumn("0:V", 5);
    assertEquals(indexed, IndexUtil.selectVectorColumn(Arrays.asList(varchar, indexed)));
    assertNull(IndexUtil.selectVectorColumn(Collections.singletonList(varchar)));
  }

  @Test
  public void testNoCandidates() {
    List<PColumn> none = Collections.emptyList();
    assertNull(IndexUtil.selectVectorColumn(none));
    assertNull(IndexUtil.findVectorColumn(null));
  }
}
