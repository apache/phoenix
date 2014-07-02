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
package org.apache.phoenix.expression.aggregator;

import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.schema.PDataType;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.SizedUtil;
import java.io.IOException;
import java.util.List;
import java.util.TreeMap;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.util.FirstLastNthValueDataContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base server aggregator for (FIRST|LAST|NTH)_VALUE functions
 *
 */
public class FirstLastValueServerAggregator extends BaseAggregator {

    private static final Logger logger = LoggerFactory.getLogger(FirstLastValueServerAggregator.class);
    protected List<Expression> children;
    protected BinaryComparator topOrder = new BinaryComparator(ByteUtil.EMPTY_BYTE_ARRAY);
    protected byte[] topValue;
    protected boolean useOffset = false;
    protected int offset = -1;
    protected TreeMap<byte[], byte[]> topValues = new TreeMap<byte[], byte[]>(new Bytes.ByteArrayComparator());
    protected boolean isAscending;
    protected boolean hasValueDescSortOrder;
    protected Expression orderByColumn;
    protected Expression dataColumn;

    public FirstLastValueServerAggregator() {
        super(SortOrder.getDefault());
    }

    @Override
    public void reset() {
        topOrder = new BinaryComparator(ByteUtil.EMPTY_BYTE_ARRAY);
        topValue = null;
        topValues.clear();
        offset = -1;
        useOffset = false;
    }

    @Override
    public int getSize() {
        return super.getSize() + SizedUtil.IMMUTABLE_BYTES_WRITABLE_SIZE;
    }

    @Override
    public void aggregate(Tuple tuple, ImmutableBytesWritable ptr) {
        //set pointer to ordering by field
        orderByColumn.evaluate(tuple, ptr);
        byte[] currentOrder = ptr.copyBytes();

        if (!dataColumn.evaluate(tuple, ptr)) {
            return;
        }

        if (useOffset) {
            boolean addFlag = false;
            if (topValues.size() < offset) {
                try {
                    addFlag = true;
                } catch (Exception e) {
                    logger.error(e.getMessage());
                }
            } else {
                if (isAscending) {
                    byte[] lowestKey = topValues.lastKey();
                    if (Bytes.compareTo(currentOrder, lowestKey) < 0) {
                        topValues.remove(lowestKey);
                        addFlag = true;
                    }
                } else { //desc
                    byte[] highestKey = topValues.firstKey();
                    if (Bytes.compareTo(currentOrder, highestKey) > 0) {
                        topValues.remove(highestKey);
                        addFlag = true;
                    }
                }
            }
            if (addFlag) {
                //invert bytes if is SortOrder set
                if (hasValueDescSortOrder) {
                    topValues.put(currentOrder, SortOrder.invert(ptr.get(), ptr.getOffset(), ptr.getLength()));
                } else {
                    topValues.put(currentOrder, ptr.copyBytes());
                }
            }
        } else {
            boolean isHigher;
            if (isAscending) {
                isHigher = topOrder.compareTo(currentOrder) > 0;
            } else {
                isHigher = topOrder.compareTo(currentOrder) < 0;//desc
            }
            if (topOrder.getValue().length < 1 || isHigher) {
                if (hasValueDescSortOrder) {
                    topValue = SortOrder.invert(ptr.get(), ptr.getOffset(), ptr.getLength());
                } else {
                    topValue = ptr.copyBytes();
                }

                topOrder = new BinaryComparator(currentOrder);
            }
        }

    }

    @Override
    public String toString() {
        StringBuilder out = new StringBuilder("FirstLastValueServerAggregator"
                + " is ascending: " + isAscending + " value=");
        if (useOffset) {
            for (byte[] key : topValues.keySet()) {
                out.append(topValues.get(key));
            }
            out.append(" offset = ").append(offset);
        } else {
            out.append(topValue);
        }

        return out.toString();
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {

        FirstLastNthValueDataContainer payload = new FirstLastNthValueDataContainer();
        payload.setIsAscending(isAscending);

        payload.setFixedWidthOrderValues(orderByColumn.getDataType().isFixedWidth());
        payload.setFixedWidthDataValues(dataColumn.getDataType().isFixedWidth());

        if (useOffset) {
            payload.setOffset(offset);

            if (topValues.size() == 0) {
                return false;
            }
        } else {
            if (topValue == null) {
                return false;
            }
            topValues.put(topOrder.getValue(), topValue);
        }
        payload.setData(topValues);

        try {
            ptr.set(payload.getPayload());
        } catch (IOException ex) {
            logger.error(ex.getMessage());
            return false;
        }
        return true;
    }

    @Override
    public PDataType getDataType() {
        return PDataType.VARBINARY;
    }

    public void init(List<Expression> children, boolean isAscending, int offset) {
        this.children = children;
        this.offset = offset;
        if (offset > 0) {
            useOffset = true;
        }

        orderByColumn = children.get(0);
        dataColumn = children.get(2);

        //set order if modified
        hasValueDescSortOrder = (dataColumn.getSortOrder() == SortOrder.DESC);

        if (orderByColumn.getSortOrder() == SortOrder.DESC) {
            this.isAscending = !isAscending;
        } else {
            this.isAscending = isAscending;
        }
    }
}
