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
package org.apache.phoenix.util;

import static org.apache.phoenix.query.QueryConstants.SINGLE_COLUMN;
import static org.apache.phoenix.query.QueryConstants.SINGLE_COLUMN_FAMILY;

import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableUtils;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.tuple.Tuple;


/**
 * 
 * Utilities for Tuple
 *
 * 
 * @since 0.1
 */
public class TupleUtil {
    private TupleUtil() {
    }
    
    public static boolean equals(Tuple t1, Tuple t2, ImmutableBytesWritable ptr) {
        t1.getKey(ptr);
        byte[] buf = ptr.get();
        int offset = ptr.getOffset();
        int length = ptr.getLength();
        t2.getKey(ptr);
        return Bytes.compareTo(buf, offset, length, ptr.get(), ptr.getOffset(), ptr.getLength()) == 0;
    }
    
    public static int compare(Tuple t1, Tuple t2, ImmutableBytesWritable ptr) {
        return compare(t1, t2, ptr, 0);
    }
    
    public static int compare(Tuple t1, Tuple t2, ImmutableBytesWritable ptr, int keyOffset) {
        t1.getKey(ptr);
        byte[] buf = ptr.get();
        int offset = ptr.getOffset() + keyOffset;
        int length = ptr.getLength() - keyOffset;
        t2.getKey(ptr);
        return Bytes.compareTo(buf, offset, length, ptr.get(), ptr.getOffset() + keyOffset, ptr.getLength() - keyOffset);
    }
    
    /**
     * Set ptr to point to the value contained in the first KeyValue without
     * exploding Result into KeyValue array.
     * @param r
     * @param ptr
     */
    public static void getAggregateValue(Tuple r, ImmutableBytesWritable ptr) {
        if (r.size() == 1) {
            Cell kv = r.getValue(0); // Just one KV for aggregation
            if (Bytes.compareTo(SINGLE_COLUMN_FAMILY, 0, SINGLE_COLUMN_FAMILY.length, kv.getFamilyArray(), kv.getFamilyOffset(), kv.getFamilyLength()) == 0) {
                if (Bytes.compareTo(SINGLE_COLUMN, 0, SINGLE_COLUMN.length, kv.getQualifierArray(), kv.getQualifierOffset(), kv.getQualifierLength()) == 0) {
                    ptr.set(kv.getValueArray(), kv.getValueOffset(), kv.getValueLength());
                    return;
                }
            }
        }
        throw new IllegalStateException("Expected single, aggregated KeyValue from coprocessor, but instead received " + r + ". Ensure aggregating coprocessors are loaded correctly on server");
    }
    
    /** Concatenate results evaluated against a list of expressions
     * 
     * @param result the tuple for expression evaluation
     * @param expressions
     * @return the concatenated byte array as ImmutableBytesWritable
     * @throws IOException
     */
    public static ImmutableBytesPtr getConcatenatedValue(Tuple result, List<Expression> expressions) throws IOException {
        ImmutableBytesPtr value = new ImmutableBytesPtr(ByteUtil.EMPTY_BYTE_ARRAY);
        Expression expression = expressions.get(0);
        boolean evaluated = expression.evaluate(result, value);
        
        if (expressions.size() == 1) {
            if (!evaluated) {
                value.set(ByteUtil.EMPTY_BYTE_ARRAY);
            }
            return value;
        } else {
            TrustedByteArrayOutputStream output = new TrustedByteArrayOutputStream(value.getLength() * expressions.size());
            try {
                if (evaluated) {
                    output.write(value.get(), value.getOffset(), value.getLength());
                }
                for (int i = 1; i < expressions.size(); i++) {
                    if (!expression.getDataType().isFixedWidth()) {
                        output.write(QueryConstants.SEPARATOR_BYTE);
                    }
                    expression = expressions.get(i);
                    // TODO: should we track trailing null values and omit the separator bytes?
                    if (expression.evaluate(result, value)) {
                        output.write(value.get(), value.getOffset(), value.getLength());
                    } else if (i < expressions.size()-1 && expression.getDataType().isFixedWidth()) {
                        // This should never happen, because any non terminating nullable fixed width type (i.e. INT or LONG) is
                        // converted to a variable length type (i.e. DECIMAL) to allow an empty byte array to represent null.
                        throw new DoNotRetryIOException("Non terminating null value found for fixed width expression (" + expression + ") in row: " + result);
                    }
                }
                byte[] outputBytes = output.getBuffer();
                value.set(outputBytes, 0, output.size());
                return value;
            } finally {
                output.close();
            }
        }
    }
    
    @SuppressWarnings("deprecation")
    public static int write(Tuple result, DataOutput out) throws IOException {
        int size = 0;
        for(int i = 0; i < result.size(); i++) {
            KeyValue kv = org.apache.hadoop.hbase.KeyValueUtil.ensureKeyValue(result.getValue(i));
            size += kv.getLength();
            size += Bytes.SIZEOF_INT; // kv.getLength
          }

        WritableUtils.writeVInt(out, size);
        for(int i = 0; i < result.size(); i++) {
            KeyValue kv = org.apache.hadoop.hbase.KeyValueUtil.ensureKeyValue(result.getValue(i));
            out.writeInt(kv.getLength());
            out.write(kv.getBuffer(), kv.getOffset(), kv.getLength());
          }
        return size;
    }
}
