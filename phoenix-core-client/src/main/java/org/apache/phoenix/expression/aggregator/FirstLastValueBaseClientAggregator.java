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
package org.apache.phoenix.expression.aggregator;

import java.util.LinkedList;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes.ByteArrayComparator;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.tuple.SingleKeyValueTuple;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PArrayDataType;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PVarbinary;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.FirstLastNthValueDataContainer;

/**
 * Base client aggregator for (FIRST|LAST|NTH)_VALUE and (FIRST|LAST)_VALUES functions
 */
public class FirstLastValueBaseClientAggregator extends BaseAggregator {

  protected boolean useOffset = false;
  protected int offset = -1;
  protected BinaryComparator topOrder = new BinaryComparator(ByteUtil.EMPTY_BYTE_ARRAY);
  protected byte[] topValue = null;
  protected TreeMap<byte[], LinkedList<byte[]>> topValues =
    new TreeMap<byte[], LinkedList<byte[]>>(new ByteArrayComparator());
  protected boolean isAscending;
  protected PDataType dataType;

  // Set to true for retrieving multiple top values for FIRST_VALUES or LAST_VALUES
  protected boolean isArrayReturnType = false;

  public FirstLastValueBaseClientAggregator() {
    super(SortOrder.getDefault());
    this.dataType = PVarbinary.INSTANCE;
  }

  public FirstLastValueBaseClientAggregator(PDataType type) {
    super(SortOrder.getDefault());
    this.dataType = (type == null) ? PVarbinary.INSTANCE : type;
  }

  @Override
  public void reset() {
    topOrder = new BinaryComparator(ByteUtil.EMPTY_BYTE_ARRAY);
    topValue = null;
    topValues.clear();
  }

  @Override
  public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
    if (useOffset) {
      if (topValues.size() == 0) {
        return false;
      }

      Set<Map.Entry<byte[], LinkedList<byte[]>>> entrySet;
      if (isAscending) {
        entrySet = topValues.entrySet();
      } else {
        entrySet = topValues.descendingMap().entrySet();
      }

      int counter = 0;
      ImmutableBytesWritable arrPtr = new ImmutableBytesWritable(ByteUtil.EMPTY_BYTE_ARRAY);
      for (Map.Entry<byte[], LinkedList<byte[]>> entry : entrySet) {
        ListIterator<byte[]> it = entry.getValue().listIterator();
        while (it.hasNext()) {
          if (isArrayReturnType) {
            ImmutableBytesWritable newArrPtr = new ImmutableBytesWritable(it.next());
            PArrayDataType.appendItemToArray(newArrPtr, arrPtr.getLength(), arrPtr.getOffset(),
              arrPtr.get(), PDataType.fromTypeId(dataType.getSqlType() - PDataType.ARRAY_TYPE_BASE),
              counter, null, sortOrder);
            arrPtr = newArrPtr;

            if (++counter == offset) {
              break;
            }
          } else {
            if (++counter == offset) {
              ptr.set(it.next());
              return true;
            }
            it.next();
          }
        }
      }

      if (isArrayReturnType) {
        ptr.set(arrPtr.get());
        return true;
      }

      ptr.set(ByteUtil.EMPTY_BYTE_ARRAY);
      return true;
    }

    if (topValue == null) {
      return false;
    }

    ptr.set(topValue);
    return true;
  }

  @Override
  public void aggregate(Tuple tuple, ImmutableBytesWritable ptr) {

    // if is called cause aggregation in ORDER BY clause
    if (tuple instanceof SingleKeyValueTuple) {
      topValue = ptr.copyBytes();
      return;
    }

    FirstLastNthValueDataContainer payload = new FirstLastNthValueDataContainer();

    payload.setPayload(ptr.copyBytes());
    isAscending = payload.getIsAscending();
    TreeMap<byte[], LinkedList<byte[]>> serverAggregatorResult = payload.getData();

    if (useOffset) {
      // merge topValues
      for (Entry<byte[], LinkedList<byte[]>> entry : serverAggregatorResult.entrySet()) {
        byte[] itemKey = entry.getKey();
        LinkedList<byte[]> itemList = entry.getValue();

        if (topValues.containsKey(itemKey)) {
          topValues.get(itemKey).addAll(itemList);
        } else {
          topValues.put(itemKey, itemList);
        }
      }
    } else {
      Entry<byte[], LinkedList<byte[]>> valueEntry = serverAggregatorResult.firstEntry();
      byte[] currentOrder = valueEntry.getKey();

      boolean isBetter;
      if (isAscending) {
        isBetter = topOrder.compareTo(currentOrder) > 0;
      } else {
        isBetter = topOrder.compareTo(currentOrder) < 0; // desc
      }
      if (topOrder.getValue().length < 1 || isBetter) {
        topOrder = new BinaryComparator(currentOrder);
        topValue = valueEntry.getValue().getFirst();
      }
    }
  }

  @Override
  public PDataType getDataType() {
    return dataType;
  }

  public void init(int offset, boolean isArrayReturnType) {
    if (offset > 0) {
      useOffset = true;
      this.offset = offset;
    }

    this.isArrayReturnType = isArrayReturnType;
  }
}
