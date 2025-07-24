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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import org.apache.hadoop.hbase.ByteBufferKeyValue;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.security.visibility.CellVisibility;
import org.apache.hadoop.hbase.util.ByteBufferUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.junit.Test;

public class MutationUtilTest {

  @Test
  public void testPutCopy() throws IOException {
    final byte[] rowKey = Bytes.toBytes("row1");
    final byte[] family = Bytes.toBytes("f");
    long ts = EnvironmentEdgeManager.currentTimeMillis();
    final int numCols = 5;
    final int priority = 255;
    Put source = new Put(rowKey);
    source.setTimestamp(ts);
    source.setDurability(Durability.SKIP_WAL);
    source.setPriority(priority);
    // add the cells
    for (int i = 0; i < numCols; i++) {
      source.add(
        createOffHeapCell(rowKey, family, Bytes.toBytes("col" + i), ts, Bytes.toBytes("v_" + i)));
    }
    // set some known attributes
    source.setTTL(345);
    source.setCellVisibility(new CellVisibility("secret"));
    // set some custom attributes
    source.setAttribute(BaseScannerRegionObserverConstants.CLIENT_VERSION, Bytes.toBytes("5.2.1"));
    Put copied = MutationUtil.copyPut(source);
    assertArrayEquals(source.getRow(), copied.getRow());
    assertEquals(source.getTimestamp(), copied.getTimestamp());
    assertEquals(source.getDurability(), copied.getDurability());
    assertEquals(source.getPriority(), copied.getPriority());
    assertTrue(areEqual(source.getAttributesMap(), copied.getAttributesMap()));
    NavigableMap<byte[], List<Cell>> sourceFamilyCellMap = source.getFamilyCellMap();
    NavigableMap<byte[], List<Cell>> copiedFamilyCellMap = copied.getFamilyCellMap();
    assertEquals(sourceFamilyCellMap.size(), copiedFamilyCellMap.size());
    List<Cell> sourceCells = sourceFamilyCellMap.get(family);
    List<Cell> copiedCells = copiedFamilyCellMap.get(family);
    assertEquals(sourceCells.size(), copiedCells.size());
    sourceCells.stream().allMatch(cell -> cell instanceof ByteBufferKeyValue);
    copiedCells.stream().allMatch(cell -> cell instanceof KeyValue);
    copied = MutationUtil.copyPut(source, true);
    assertTrue(copied.getAttributesMap().isEmpty());
  }

  private Cell createOffHeapCell(byte[] rowKey, byte[] family, byte[] qualifier, long ts,
    byte[] value) {
    KeyValue kv = new KeyValue(rowKey, family, qualifier, ts, KeyValue.Type.Put);
    ByteBuffer dbb = ByteBuffer.allocateDirect(kv.getBuffer().length);
    ByteBufferUtils.copyFromArrayToBuffer(dbb, kv.getBuffer(), 0, kv.getBuffer().length);
    ByteBufferKeyValue offheapKV = new ByteBufferKeyValue(dbb, 0, kv.getBuffer().length, 0);
    return offheapKV;
  }

  private boolean areEqual(Map<String, byte[]> first, Map<String, byte[]> second) {
    if (first.size() != second.size()) {
      return false;
    }
    return first.entrySet().stream()
      .allMatch(e -> Arrays.equals(e.getValue(), second.get(e.getKey())));
  }
}
