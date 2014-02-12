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
import java.io.IOException;
import java.sql.Types;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.WritableUtils;

import org.apache.phoenix.schema.PArrayDataType;
import org.apache.phoenix.schema.PDataType;
import org.apache.phoenix.schema.PhoenixArray;
import org.apache.phoenix.schema.tuple.Tuple;

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
        for (int i = position >= 0 ? position : 0; i < elements.length; i++) {
            Expression child = children.get(i);
            if (!child.evaluate(tuple, ptr)) {
                if (tuple != null && !tuple.isImmutable()) {
                    if (position >= 0) position = i;
                    return false;
                }
            } else {
                elements[i] = baseType.toObject(ptr, child.getDataType(), child.getSortOrder());
            }
        }
        if (position >= 0) position = elements.length;
        PhoenixArray array = PArrayDataType.instantiatePhoenixArray(baseType, elements);
        // FIXME: Need to see if this creation of an array and again back to byte[] can be avoided
        ptr.set(getDataType().toBytes(array));
        return true;
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