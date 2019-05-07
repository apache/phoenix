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

package org.apache.phoenix.hbase.index.wal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.phoenix.util.PhoenixKeyValueUtil;

/**
 * Codec to encode/decode {@link KeyValue}s and {@link IndexedKeyValue}s within a {@link WALEdit}
 */
public class KeyValueCodec {
  public static final byte NORMAL_CELL_MARKER = 0;
  public static final byte INDEXED_KEYVALUE_MARKER = 1;
  public static final byte INDEXED_CELL_MARKER = 2;

  /**
   * Read a single {@link KeyValue} from the input stream - may either be a regular {@link KeyValue}
   * or an {@link IndexedKeyValue}.
   * @param in to read from
   * @return the next {@link KeyValue}, if one is available
   * @throws IOException if the next {@link KeyValue} cannot be read
   */
  public static Cell readKeyValue(DataInput in) throws IOException {
      byte marker = in.readByte();
      if (marker == NORMAL_CELL_MARKER) {
          return PhoenixKeyValueUtil.deserializeCell(in);
      } else if (marker == INDEXED_CELL_MARKER) {
          IndexedCell ic = new IndexedCell();
          ic.readFields(in);
          return ic;
      } else if (marker == INDEXED_KEYVALUE_MARKER) {
          IndexedKeyValue ikv = new IndexedKeyValue();
          ikv.readFields(in);
          return ikv;
      }
      throw new IllegalArgumentException("Don't know how to deserialize cell after reading marker: " + (int)marker);
  }

  /**
   * Write a {@link KeyValue} or an {@link IndexedKeyValue} to the output stream. These can be read
   * back via {@link #readKeyValue(DataInput)} or {@link #readKeyValues(DataInput)}.
   * @param out to write to
   * @param kv {@link KeyValue} to which to write
   * @throws IOException if there is an error writing
   */
  public static void write(DataOutput out, Cell c) throws IOException {
      if (c instanceof IndexedCell) {
          out.write(INDEXED_CELL_MARKER);
          ((IndexedCell)c).writeData(out);
      } else if (c instanceof IndexedKeyValue) {
          out.write(INDEXED_KEYVALUE_MARKER);
          ((IndexedKeyValue)c).writeData(out);
      } else {
          out.write(NORMAL_CELL_MARKER);
          PhoenixKeyValueUtil.serializeCell(out, c);
      }
  }
}