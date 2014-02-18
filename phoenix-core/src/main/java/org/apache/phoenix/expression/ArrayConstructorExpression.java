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
package org.apache.phoenix.expression;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.sql.Types;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableUtils;
import org.apache.phoenix.schema.PArrayDataType;
import org.apache.phoenix.schema.PDataType;
import org.apache.phoenix.schema.PhoenixArray;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.util.TrustedByteArrayOutputStream;

/**
 * Creates an expression for Upsert with Values/Select using ARRAY
 */
public class ArrayConstructorExpression extends BaseCompoundExpression {
    private PDataType baseType;
    private int position = -1;
    private Object[] elements;
    
    
    public ArrayConstructorExpression(List<Expression> children, PDataType baseType) {
        super(children);
        init(baseType);
    }

    private void init(PDataType baseType) {
        this.baseType = baseType;
        elements = new Object[getChildren().size()];
    }
    
    @Override
    public PDataType getDataType() {
        return PDataType.fromTypeId(baseType.getSqlType() + Types.ARRAY);
    }

    @Override
    public void reset() {
        super.reset();
        position = 0;
        Arrays.fill(elements, null);
    }
    
    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        // For varlength basetype this would be 0
        int estimatedSize = PArrayDataType.estimateSize(this.children.size(), this.baseType);
        TrustedByteArrayOutputStream byteStream = null;
        DataOutputStream oStream = null;
        try {
            boolean varLength = false;
            for (Expression child : children) {
                if (!child.getDataType().isFixedWidth() || child.getDataType().isCoercibleTo(PDataType.VARCHAR)) {
                    varLength = true;
                    break;
                }
            }
            if (!varLength) {
                byteStream = new TrustedByteArrayOutputStream(estimatedSize + Bytes.SIZEOF_BYTE + Bytes.SIZEOF_INT);
                oStream = new DataOutputStream(byteStream);
            }
            for (int i = position >= 0 ? position : 0; i < elements.length; i++) {
                Expression child = children.get(i);
                if (!child.evaluate(tuple, ptr)) {
                    if (tuple != null && !tuple.isImmutable()) {
                        if (position >= 0) position = i;
                        return false;
                    }
                } else {
                    if (!varLength) {
                        if (!child.isStateless()) {
                            byte[] val = new byte[ptr.getLength()];
                            // One copy is needed here
                            System.arraycopy(ptr.get(), ptr.getOffset(), val, 0, ptr.getLength());
                            oStream.write(val);
                        } else {
                            oStream.write(ptr.get());
                        }
                    } else {
                        elements[i] = baseType.toObject(ptr, child.getDataType(), child.getSortOrder());
                    }
                }
            }
            if (position >= 0) position = elements.length;
            if (varLength) {
                // For variable length .. still not done the offset tracking. Using the method of create an array and
                // again serializing back to bytes
                PhoenixArray array = PArrayDataType.instantiatePhoenixArray(baseType, elements);
                // FIXME: Need to see if this creation of an array and again back to byte[] can be avoided
                ptr.set(getDataType().toBytes(array));
            } else {
                // No of elements
                oStream.writeInt(children.size());
                // Version of the array
                oStream.write(PArrayDataType.ARRAY_SERIALIZATION_VERSION);
                ptr.set(byteStream.toByteArray());
            }
            return true;
        } catch (Exception e) {
            throw new RuntimeException("Exception while serializing the byte array");
        } finally {
            try {
                byteStream.close();
                oStream.close();
            } catch (Exception e) {
                // Should not happen
            }
        }
    }
    
    @Override
    public void readFields(DataInput input) throws IOException {
        super.readFields(input);
        int baseTypeOrdinal = WritableUtils.readVInt(input);
        init(PDataType.values()[baseTypeOrdinal]);
    }

    @Override
    public void write(DataOutput output) throws IOException {
        super.write(output);
        WritableUtils.writeVInt(output, baseType.ordinal());
    }

}