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
package org.apache.phoenix.mapreduce.index.fsck;

import java.util.List;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.RowKeySchema;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.types.PDataType;

/** Utility for formatting row keys as decoded column expressions or hex strings. */
public final class RowKeyFormatter {

  public enum KeyFormat {
    DECODED,
    HEX;

    public static KeyFormat fromString(String val) {
      if (val == null) {
        return DECODED;
      }
      return KeyFormat.valueOf(val.trim().toUpperCase());
    }
  }

  private RowKeyFormatter() {
  }

  /**
   * Formats a row key according to the requested KeyFormat.
   * @param rowKey binary row key bytes
   * @param table  target PTable
   * @param format DECODED or HEX
   * @return formatted string
   */
  public static String format(byte[] rowKey, PTable table, KeyFormat format) {
    if (rowKey == null) {
      return "null";
    }
    if (rowKey.length == 0) {
      return "";
    }
    if (format == KeyFormat.HEX) {
      return Hex.encodeHexString(rowKey);
    }

    // Decode primary key column values using the table's row key schema
    if (table == null || table.getRowKeySchema() == null) {
      return Bytes.toStringBinary(rowKey);
    }

    try {
      RowKeySchema schema = table.getRowKeySchema();
      List<PColumn> pkColumns = table.getPKColumns();
      ImmutableBytesWritable ptr = new ImmutableBytesWritable();
      int maxOffset = schema.iterator(rowKey, ptr);
      StringBuilder sb = new StringBuilder();
      sb.append("(");
      boolean first = true;
      for (int i = 0; i < schema.getFieldCount(); i++) {
        Boolean hasValue = schema.next(ptr, i, maxOffset);
        if (hasValue == null) {
          break;
        }
        if (!first) {
          sb.append(", ");
        }
        first = false;
        String colName =
          (i < pkColumns.size()) ? pkColumns.get(i).getName().getString() : ("COL_" + i);

        if (Boolean.TRUE.equals(hasValue)) {
          PDataType dataType = schema.getField(i).getDataType();
          SortOrder sortOrder =
            (i < pkColumns.size()) ? pkColumns.get(i).getSortOrder() : SortOrder.ASC;
          Object val = dataType.toObject(ptr, sortOrder);
          sb.append(colName).append("=").append(val != null ? val.toString() : "null");
        } else {
          sb.append(colName).append("=null");
        }
      }
      sb.append(")");
      return sb.toString();
    } catch (Exception e) {
      return Bytes.toStringBinary(rowKey);
    }
  }
}
