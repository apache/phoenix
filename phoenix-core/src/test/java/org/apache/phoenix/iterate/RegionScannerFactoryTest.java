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
package org.apache.phoenix.iterate;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PTable.QualifierEncodingScheme;
import org.apache.phoenix.schema.tuple.EncodedColumnQualiferCellsList;
import org.junit.Test;

/** Unit tests for the position of the server parsed projection cell in a scanned row. */
public class RegionScannerFactoryTest {

  private static final byte[] ROW = Bytes.toBytes("r");
  private static final byte[] CF = Bytes.toBytes("0");
  private static final QualifierEncodingScheme SCHEME =
    QualifierEncodingScheme.FOUR_BYTE_QUALIFIERS;

  private static Cell column(int qualifier) {
    return new KeyValue(ROW, CF, SCHEME.encode(qualifier), Bytes.toBytes(qualifier));
  }

  private static Cell arrayCell() {
    return new KeyValue(ROW, QueryConstants.ARRAY_VALUE_COLUMN_FAMILY,
      QueryConstants.ARRAY_VALUE_COLUMN_QUALIFIER, Bytes.toBytes("projected"));
  }

  @Test
  public void testArrayCellFollowsColumnsInOrdinaryList() {
    List<Cell> cells = new ArrayList<>();
    cells.add(column(QueryConstants.ENCODED_EMPTY_COLUMN_NAME));
    cells.add(column(11));
    cells.add(arrayCell());
    assertEquals(2, RegionScannerFactory.getArrayCellPosition(cells));
  }

  @Test
  public void testArrayCellFoundWithoutIndexedAccessToEncodedList() {
    List<Cell> cells =
      spy(new EncodedColumnQualiferCellsList(QueryConstants.ENCODED_CQ_COUNTER_INITIAL_VALUE,
        QueryConstants.ENCODED_CQ_COUNTER_INITIAL_VALUE + 40, SCHEME));
    cells.add(column(QueryConstants.ENCODED_EMPTY_COLUMN_NAME));
    for (int q = QueryConstants.ENCODED_CQ_COUNTER_INITIAL_VALUE; q <= 50; q++) {
      cells.add(column(q));
    }
    // The reserved qualifier of the array cell sorts it after the empty column cell and before the
    // other column cells
    cells.add(arrayCell());
    assertEquals(1, RegionScannerFactory.getArrayCellPosition(cells));
    // The method must not use indexed access, because each indexed access scans the encoded list
    // from the start
    verify(cells, never()).get(anyInt());
    assertEquals(arrayCell().getValueLength(), cells.get(1).getValueLength());
  }
}
