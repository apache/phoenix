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
package org.apache.phoenix.hbase.index;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellBuilder;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;

public class MultiMutation extends Mutation {

  private ImmutableBytesPtr rowKey;

  public MultiMutation(ImmutableBytesPtr rowkey) {
    this.rowKey = rowkey;
  }

  /**
   */
  public void addAll(Mutation stored) {
    // add all the kvs
    for (Entry<byte[], List<Cell>> kvs : stored.getFamilyCellMap().entrySet()) {
      byte[] family = kvs.getKey();
      List<Cell> list = getKeyValueList(family, kvs.getValue().size());
      list.addAll(kvs.getValue());
      // override generics to fix the Cell/ExtendedCell type changes between HBase 2/3
      familyMap.put(family, (List) list);
    }

    // add all the attributes, not overriding already stored ones
    for (Entry<String, byte[]> attrib : stored.getAttributesMap().entrySet()) {
      if (this.getAttribute(attrib.getKey()) == null) {
        this.setAttribute(attrib.getKey(), attrib.getValue());
      }
    }
  }

  private List<Cell> getKeyValueList(byte[] family, int hint) {
    // override generics to fix the Cell/ExtendedCell type changes between HBase 2/3
    List<Cell> list = (List) (familyMap.get(family));
    if (list == null) {
      list = new ArrayList<Cell>(hint);
    }
    return list;
  }

  // No @Override to maintain Hadoop 2 compatibility
  public CellBuilder getCellBuilder(CellBuilderType cellBuilderType) {
    throw new IllegalArgumentException("MultiMutation does not implement a CellBuilder");
  }

  @Override
  public byte[] getRow() {
    return this.rowKey.copyBytesIfNecessary();
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((rowKey == null) ? 0 : rowKey.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (getClass() != obj.getClass()) return false;
    MultiMutation other = (MultiMutation) obj;
    return rowKey.equals(other.rowKey);
  }
}
