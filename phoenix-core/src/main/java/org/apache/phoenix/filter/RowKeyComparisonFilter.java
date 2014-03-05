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
package org.apache.phoenix.filter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.io.WritableUtils;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.schema.tuple.BaseTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 *
 * Filter for use when expressions only reference row key columns
 *
 * 
 * @since 0.1
 */
public class RowKeyComparisonFilter extends BooleanExpressionFilter {
    private static final Logger logger = LoggerFactory.getLogger(RowKeyComparisonFilter.class);

    private boolean evaluate = true;
    private boolean keepRow = false;
    private RowKeyTuple inputTuple = new RowKeyTuple();
    private byte[] essentialCF;

    public RowKeyComparisonFilter() {
    }

    public RowKeyComparisonFilter(Expression expression, byte[] essentialCF) {
        super(expression);
        this.essentialCF = essentialCF;
    }

    @Override
    public void reset() {
        this.keepRow = false;
        this.evaluate = true;
        super.reset();
    }

    /**
     * Evaluate in filterKeyValue instead of filterRowKey, because HBASE-6562 causes filterRowKey
     * to be called with deleted or partial row keys.
     */
    @Override
    public ReturnCode filterKeyValue(Cell v) {
        if (evaluate) {
            inputTuple.setKey(v.getRowArray(), v.getRowOffset(), v.getRowLength());
            this.keepRow = Boolean.TRUE.equals(evaluate(inputTuple));
            if (logger.isDebugEnabled()) {
                logger.debug("RowKeyComparisonFilter: " + (this.keepRow ? "KEEP" : "FILTER")  + " row " + inputTuple);
            }
            evaluate = false;
        }
        return keepRow ? ReturnCode.INCLUDE : ReturnCode.NEXT_ROW;
    }

    private final class RowKeyTuple extends BaseTuple {
        private byte[] buf;
        private int offset;
        private int length;

        public void setKey(byte[] buf, int offset, int length) {
            this.buf = buf;
            this.offset = offset;
            this.length = length;
        }

        @Override
        public void getKey(ImmutableBytesWritable ptr) {
            ptr.set(buf, offset, length);
        }

        @Override
        public KeyValue getValue(byte[] cf, byte[] cq) {
            return null;
        }

        @Override
        public boolean isImmutable() {
            return true;
        }

        @Override
        public String toString() {
            return Bytes.toStringBinary(buf, offset, length);
        }

        @Override
        public int size() {
            return 0;
        }

        @Override
        public KeyValue getValue(int index) {
            throw new IndexOutOfBoundsException(Integer.toString(index));
        }

        @Override
        public boolean getValue(byte[] family, byte[] qualifier,
                ImmutableBytesWritable ptr) {
            return false;
        }
    }

    @Override
    public boolean filterRow() {
        return !this.keepRow;
    }

    @SuppressWarnings("all") // suppressing missing @Override since this doesn't exist for HBase 0.94.4
    public boolean isFamilyEssential(byte[] name) {
        // We only need our "guaranteed to have a key value" column family,
        // which we pass in and serialize through. In the case of a VIEW where
        // we don't have this, we have to say that all families are essential.
        return this.essentialCF.length == 0 ? true : Bytes.compareTo(this.essentialCF, name) == 0;
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        super.readFields(input);
        this.essentialCF = WritableUtils.readCompressedByteArray(input);
    }

    @Override
    public void write(DataOutput output) throws IOException {
        super.write(output);
        WritableUtils.writeCompressedByteArray(output, this.essentialCF);
    }
    
    public static RowKeyComparisonFilter parseFrom(final byte [] pbBytes) throws DeserializationException {
        try {
            return (RowKeyComparisonFilter)Writables.getWritable(pbBytes, new RowKeyComparisonFilter());
        } catch (IOException e) {
            throw new DeserializationException(e);
        }
    }
}
