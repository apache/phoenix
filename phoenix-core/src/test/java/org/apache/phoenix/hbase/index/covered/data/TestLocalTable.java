/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.hbase.index.covered.data;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

public class TestLocalTable {
    private static final byte[] ROW = Bytes.toBytes("test_row");

    @Test
    public void testGetOldestTimestamp() {
        LocalTable localTable = new LocalTable(null);

        List<Cell> cellList1 = getCellList(new KeyValue(ROW, 5L), new KeyValue(ROW, 4L));
        assertEquals(4L, localTable.getOldestTimestamp(Collections.singletonList(cellList1)));

        List<Cell> cellList2 = getCellList(new KeyValue(ROW, 5L), new KeyValue(ROW, 2L));
        List<List<Cell>> set1 = new ArrayList<>(Arrays.asList(cellList1, cellList2));
        assertEquals(2L, localTable.getOldestTimestamp(set1));

        List<Cell> cellList3 = getCellList(new KeyValue(ROW, 1L));
        set1.add(cellList3);
        assertEquals(1L, localTable.getOldestTimestamp(set1));

        List<Cell> cellList4 =
                getCellList(new KeyValue(ROW, 3L), new KeyValue(ROW, 1L), new KeyValue(ROW, 0L));
        set1.add(cellList4);
        assertEquals(0L, localTable.getOldestTimestamp(set1));
    }

    private List<Cell> getCellList(KeyValue... kvs) {
        List<Cell> cellList = new ArrayList<>();
        for (KeyValue kv : kvs) {
            cellList.add(kv);
        }
        return cellList;
    }
}
