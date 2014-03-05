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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;

/**
 * Codec to encode/decode {@link KeyValue}s and {@link IndexedKeyValue}s within a {@link WALEdit}
 */
public class KeyValueCodec {

  /**
   * KeyValue length marker specifying that its actually an {@link IndexedKeyValue} rather than a
   * regular {@link KeyValue}.
   */
  public static final int INDEX_TYPE_LENGTH_MARKER = -1;

  /**
   * Read a {@link List} of {@link KeyValue} from the input stream - may contain regular
   * {@link KeyValue}s or {@link IndexedKeyValue}s.
   * @param in to read from
   * @return the next {@link KeyValue}s
   * @throws IOException if the next {@link KeyValue} cannot be read
   */
  public static List<KeyValue> readKeyValues(DataInput in) throws IOException {
    int size = in.readInt();
    if (size == 0) {
      return Collections.<KeyValue>emptyList();
    }
    List<KeyValue> kvs = new ArrayList<KeyValue>(size);
    for (int i = 0; i < size; i++) {
      kvs.add(readKeyValue(in));
    }
    return kvs;
  }

  /**
   * Read a single {@link KeyValue} from the input stream - may either be a regular {@link KeyValue}
   * or an {@link IndexedKeyValue}.
   * @param in to read from
   * @return the next {@link KeyValue}, if one is available
   * @throws IOException if the next {@link KeyValue} cannot be read
   */
  public static KeyValue readKeyValue(DataInput in) throws IOException {
    int length = in.readInt();
    // its a special IndexedKeyValue
    if (length == INDEX_TYPE_LENGTH_MARKER) {
      IndexedKeyValue kv = new IndexedKeyValue();
      kv.readFields(in);
      return kv;
    } else {
      return KeyValue.create(length, in);
    }
  }

  /**
   * Write a {@link KeyValue} or an {@link IndexedKeyValue} to the output stream. These can be read
   * back via {@link #readKeyValue(DataInput)} or {@link #readKeyValues(DataInput)}.
   * @param out to write to
   * @param kv {@link KeyValue} to which to write
   * @throws IOException if there is an error writing
   */
  public static void write(DataOutput out, KeyValue kv) throws IOException {
    if (kv instanceof IndexedKeyValue) {
      out.writeInt(INDEX_TYPE_LENGTH_MARKER);
      ((IndexedKeyValue) kv).writeData(out);
    } else {
        KeyValue.write(kv, out);
    }
  }
}