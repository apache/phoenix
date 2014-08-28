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
package org.apache.phoenix.schema.stat;
import java.util.Arrays;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.util.SchemaUtil;
/**
 * Simple utility class for managing multiple key parts of the statistic
 */
public class StatisticsUtils {

  private StatisticsUtils() {
    // private ctor for utility classes
  }

  /** Number of parts in our complex key */
  protected static final int NUM_KEY_PARTS = 3;

  /**
   * Get the prefix based on the region, column and name of the statistic
   * 
   * @param table
   *          name of the source table
   * @param region
   *          name of the region where the statistic was gathered
   * @param column
   *          column for which the statistic was gathered
   * @return the row key that should be used for this statistic
   */
  public static byte[] getRowKey(byte[] table, byte[] region, byte[] column) {
    // always starts with the source table
    byte[] prefix = new byte[0];
    // then append each part of the key and
    byte[][] parts = new byte[][] { table, region, column };
    int[] sizes = new int[NUM_KEY_PARTS];
    for (int i = 0; i < NUM_KEY_PARTS; i++) {
      prefix = Bytes.add(prefix, parts[i]);
      sizes[i] = parts[i].length;
    }
    // then we add on the sizes to the end of the key
    // We could use this size to later retrieve the number of bytes used for
    // each part so that
    // the stats could be grouped
    for (int size : sizes) {
      prefix = Bytes.add(prefix, Bytes.toBytes(size));
    }
    return prefix;
  }

  /**
   * Extracts the region name from the cell's row using the table name
   * @param table
   * @param cell
   * @return
   */
  public static byte[] getRegionFromRowKey(byte[] table, Cell cell) {
    int lengthOfTableKey = Bytes.toInt(cell.getRowArray(),
        (cell.getRowOffset() + cell.getRowLength()) - ((Bytes.SIZEOF_INT * 3)), Bytes.SIZEOF_INT);
    int lengthOfRegionKey = Bytes.toInt(cell.getRowArray(),
        (cell.getRowOffset() + cell.getRowLength()) - ((Bytes.SIZEOF_INT * 2)), Bytes.SIZEOF_INT);
    byte[] region = new byte[lengthOfRegionKey];
    System.arraycopy(cell.getRowArray(), cell.getRowOffset() + lengthOfTableKey, region, 0,
        lengthOfRegionKey);
    return region;
  }

  /**
   * Extracts the table name from the cell's row.
   * @param cell
   * @return
   */
  public static byte[] getTableNameFromRowKey(Cell cell) {
    int lengthOfTableKey = Bytes.toInt(cell.getRowArray(),
        (cell.getRowOffset() + cell.getRowLength()) - ((Bytes.SIZEOF_INT * 3)), Bytes.SIZEOF_INT);
    byte[] table = new byte[lengthOfTableKey];
    System.arraycopy(cell.getRowArray(), cell.getRowOffset(), table, 0,
        lengthOfTableKey);
    String tableNameFromFullName = SchemaUtil.getTableNameFromFullName(table);
    return Bytes.toBytes(tableNameFromFullName);
  }

  public static byte[] copyRow(KeyValue kv) {
    return Arrays.copyOfRange(kv.getRowArray(), kv.getRowOffset(),
        kv.getRowOffset() + kv.getRowLength());
  }
}