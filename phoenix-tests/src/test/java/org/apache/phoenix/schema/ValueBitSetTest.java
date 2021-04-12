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
package org.apache.phoenix.schema;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.schema.KeyValueSchema.KeyValueSchemaBuilder;
import org.apache.phoenix.schema.types.PDataType;
import org.junit.Test;


public class ValueBitSetTest {
    private KeyValueSchema generateSchema(int nFields, int nRepeating, final int nNotNull) {
        KeyValueSchemaBuilder builder = new KeyValueSchemaBuilder(nNotNull);
        for (int i = 0; i < nFields; i++) {
            final int fieldIndex = i;
            for (int j = 0; j < nRepeating; j++) {
                PDatum datum = new PDatum() {
                    @Override
                    public boolean isNullable() {
                        return fieldIndex <= nNotNull;
                    }
                    @Override
                    public PDataType getDataType() {
                        return PDataType.values()[fieldIndex % PDataType.values().length];
                    }
                    @Override
                    public Integer getMaxLength() {
                        return null;
                    }
                    @Override
                    public Integer getScale() {
                        return null;
                    }
					@Override
					public SortOrder getSortOrder() {
						return SortOrder.getDefault();
					}
                };
                builder.addField(datum);
            }
        }
        KeyValueSchema schema = builder.build();
        return schema;
    }
    
    private static void setValueBitSet(KeyValueSchema schema, ValueBitSet valueSet) {
        for (int i = 0; i < schema.getFieldCount() - schema.getMinNullable(); i++) {
            if ((i & 1) == 1) {
                valueSet.set(i);
            }
        }
    }
    
    @Test
    public void testMinNullableIndex() {
        final int minNullableIndex = 4; // first 4 fields are not nullable.
        int numFields = 6;
        KeyValueSchemaBuilder builder = new KeyValueSchemaBuilder(minNullableIndex);
        for (int i = 0; i < numFields; i++) {
            final int fieldIndex = i;
            builder.addField(new PDatum() {
                @Override
                public boolean isNullable() {
                    // not nullable till index reaches minNullableIndex
                    return fieldIndex < minNullableIndex;
                }

                @Override
                public SortOrder getSortOrder() {
                    return SortOrder.getDefault();
                }

                @Override
                public Integer getScale() {
                    return null;
                }

                @Override
                public Integer getMaxLength() {
                    return null;
                }

                @Override
                public PDataType getDataType() {
                    return PDataType.values()[fieldIndex % PDataType.values().length];
                }
            });
        }
        KeyValueSchema kvSchema = builder.build();
        assertFalse(kvSchema.getFields().get(0).isNullable());
        assertFalse(kvSchema.getFields().get(minNullableIndex - 1).isNullable());
        assertTrue(kvSchema.getFields().get(minNullableIndex).isNullable());
        assertTrue(kvSchema.getFields().get(minNullableIndex + 1).isNullable());
    }
    
    @Test
    public void testNullCount() {
        int nFields = 32;
        int nRepeating = 5;
        int nNotNull = 8;
        KeyValueSchema schema = generateSchema(nFields, nRepeating, nNotNull);
        ValueBitSet valueSet = ValueBitSet.newInstance(schema);
        setValueBitSet(schema, valueSet);
        
        // From beginning, not spanning longs
        assertEquals(5, valueSet.getNullCount(0, 10));
        // From middle, not spanning longs
        assertEquals(5, valueSet.getNullCount(10, 10));
        // From middle, spanning to middle of next long
        assertEquals(10, valueSet.getNullCount(64 - 5, 20));
        // from end, not spanning longs
        assertEquals(5, valueSet.getNullCount(nFields*nRepeating-nNotNull-10, 10));
        // from beginning, spanning long entirely into middle of next long
        assertEquals(64, valueSet.getNullCount(2, 128));
    }
    
    @Test
    public void testSizing() {
        int nFields = 32;
        int nRepeating = 5;
        int nNotNull = 8;
        KeyValueSchema schema = generateSchema(nFields, nRepeating, nNotNull);
        ValueBitSet valueSet = ValueBitSet.newInstance(schema);
        // Since no bits are set, it stores the long array length only
        assertEquals(Bytes.SIZEOF_SHORT, valueSet.getEstimatedLength());
        setValueBitSet(schema, valueSet);
        assertEquals(Bytes.SIZEOF_SHORT + Bytes.SIZEOF_LONG * 3, valueSet.getEstimatedLength());
        
        nFields = 18;
        nRepeating = 1;
        nNotNull = 2;
        schema = generateSchema(nFields, nRepeating, nNotNull);
        valueSet = ValueBitSet.newInstance(schema);
        assertEquals(Bytes.SIZEOF_SHORT, valueSet.getEstimatedLength());
        setValueBitSet(schema, valueSet);
        assertEquals(Bytes.SIZEOF_SHORT, valueSet.getEstimatedLength());
        
        nFields = 19;
        nRepeating = 1;
        nNotNull = 2;
        schema = generateSchema(nFields, nRepeating, nNotNull);
        valueSet = ValueBitSet.newInstance(schema);
        assertEquals(Bytes.SIZEOF_SHORT, valueSet.getEstimatedLength());
        setValueBitSet(schema, valueSet);
        assertEquals(Bytes.SIZEOF_SHORT + Bytes.SIZEOF_LONG, valueSet.getEstimatedLength());
        
        nFields = 19;
        nRepeating = 1;
        nNotNull = 19;
        schema = generateSchema(nFields, nRepeating, nNotNull);
        valueSet = ValueBitSet.newInstance(schema);
        assertEquals(0, valueSet.getEstimatedLength());
        
        nFields = 129;
        nRepeating = 1;
        nNotNull = 0;
        schema = generateSchema(nFields, nRepeating, nNotNull);
        valueSet = ValueBitSet.newInstance(schema);
        assertEquals(Bytes.SIZEOF_SHORT, valueSet.getEstimatedLength());
        setValueBitSet(schema, valueSet);
        assertEquals(Bytes.SIZEOF_SHORT + Bytes.SIZEOF_LONG * 2, valueSet.getEstimatedLength());
        valueSet.set(128);
        assertEquals(Bytes.SIZEOF_SHORT + Bytes.SIZEOF_LONG * 3, valueSet.getEstimatedLength());
    }
    
    @Test
    public void testMaxSetBit() {        
        int nFields = 19;
        int nRepeating = 1;
        int nNotNull = 2;
        KeyValueSchema schema = generateSchema(nFields, nRepeating, nNotNull);
        ValueBitSet valueSet = ValueBitSet.newInstance(schema);
        setValueBitSet(schema, valueSet);
        int length = valueSet.getEstimatedLength();
        byte[] buf = new byte[length];
        valueSet.toBytes(buf, 0);
        ValueBitSet copyValueSet = ValueBitSet.newInstance(schema);
        copyValueSet.or(new ImmutableBytesWritable(buf));
        assertTrue(copyValueSet.getMaxSetBit() >= valueSet.getMaxSetBit());
    }

}
