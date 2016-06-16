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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.fail;

import java.sql.SQLException;
import org.apache.hadoop.hbase.util.Base64;
import java.util.Collection;
import java.util.List;

import org.apache.phoenix.schema.*;
import org.apache.phoenix.schema.types.*;
import org.apache.phoenix.schema.RowKeySchema.RowKeySchemaBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.collect.Lists;
import org.mockito.Mockito;

/*Test the getTenantIdBytes method in ScanUtil*/
@RunWith(Parameterized.class)
public class TenantIdByteConversionTest {

    private RowKeySchema schema;
    private boolean isSalted;
    private PName tenantId;
    private byte[] expectedTenantIdBytes;


    public TenantIdByteConversionTest(
            RowKeySchema schema,
            boolean isSalted,
            PName tenantId,
            byte[] expectedTenantIdBytes ) {
        this.schema = schema;
        this.isSalted = isSalted;
        this.tenantId = tenantId;
        this.expectedTenantIdBytes = expectedTenantIdBytes;
    }

    @Test
    public void test() {
        try {
            byte[] actualTenantIdBytes = ScanUtil.getTenantIdBytes(schema, isSalted, tenantId, false);
            assertArrayEquals(expectedTenantIdBytes, actualTenantIdBytes);
        } catch (SQLException ex) {
            fail(ex.getMessage());
        }
    }

    @Parameters
    public static Collection<Object[]> data() {
        List<Object[]> testCases = Lists.newArrayList();
        // Varchar
        testCases.add(new Object[] {
                getDataSchema(PVarchar.INSTANCE, SortOrder.getDefault()),
                false,
                PNameFactory.newName("NameOfTenant"),
                PVarchar.INSTANCE.toBytes("NameOfTenant")
        });

        // Char
        testCases.add(new Object[] {
                getDataSchema(PChar.INSTANCE, SortOrder.getDefault()),
                false,
                PNameFactory.newName("N"),
                PChar.INSTANCE.toBytes(PChar.INSTANCE.toObject("N"))
        });

        //Int
        testCases.add(new Object[] {
                getDataSchema(PInteger.INSTANCE, SortOrder.getDefault()),
                false,
                PNameFactory.newName("2147483646"),
                PInteger.INSTANCE.toBytes(PInteger.INSTANCE.toObject("2147483646"))
        });

        // UnsignedInt
        testCases.add(new Object[] {
                getDataSchema(PUnsignedInt.INSTANCE, SortOrder.getDefault()),
                false,
                PNameFactory.newName("2147483646"),
                PUnsignedInt.INSTANCE.toBytes(PUnsignedInt.INSTANCE.toObject("2147483646"))
        });

        //BigInt
        testCases.add(new Object[] {
                getDataSchema(PLong.INSTANCE, SortOrder.getDefault()),
                false,
                PNameFactory.newName("9223372036854775806"),
                PLong.INSTANCE.toBytes(PLong.INSTANCE.toObject("9223372036854775806"))
        });

        //UnsignedLong
        testCases.add(new Object[] {
                getDataSchema(PUnsignedLong.INSTANCE, SortOrder.getDefault()),
                false,
                PNameFactory.newName("9223372036854775806"),
                PUnsignedLong.INSTANCE.toBytes(PUnsignedLong.INSTANCE.toObject("9223372036854775806"))
        });

        //TinyInt
        testCases.add(new Object[] {
                getDataSchema(PTinyint.INSTANCE, SortOrder.getDefault()),
                false,
                PNameFactory.newName("126"),
                PTinyint.INSTANCE.toBytes(PTinyint.INSTANCE.toObject("126"))
        });

        //UnsignedTinyInt
        testCases.add(new Object[] {
                getDataSchema(PUnsignedTinyint.INSTANCE, SortOrder.getDefault()),
                false,
                PNameFactory.newName("126"),
                PUnsignedTinyint.INSTANCE.toBytes(PUnsignedTinyint.INSTANCE.toObject("126"))
        });

        //SmallInt
        testCases.add(new Object[] {
                getDataSchema(PSmallint.INSTANCE, SortOrder.getDefault()),
                false,
                PNameFactory.newName("32766"),
                PSmallint.INSTANCE.toBytes(PSmallint.INSTANCE.toObject("32766"))
        });

        //UnsignedSmallInt
        testCases.add(new Object[] {
                getDataSchema(PUnsignedSmallint.INSTANCE, SortOrder.getDefault()),
                false,
                PNameFactory.newName("32766"),
                PUnsignedSmallint.INSTANCE.toBytes(PUnsignedSmallint.INSTANCE.toObject("32766"))
        });

        //Float
        testCases.add(new Object[] {
                getDataSchema(PFloat.INSTANCE, SortOrder.getDefault()),
                false,
                PNameFactory.newName("3.402823466"),
                PFloat.INSTANCE.toBytes(PFloat.INSTANCE.toObject("3.402823466"))
        });

        //UnsignedFloat
        testCases.add(new Object[] {
                getDataSchema(PUnsignedFloat.INSTANCE, SortOrder.getDefault()),
                false,
                PNameFactory.newName("3.402823466"),
                PUnsignedFloat.INSTANCE.toBytes(PUnsignedFloat.INSTANCE.toObject("3.402823466"))
        });

        //Double
        testCases.add(new Object[] {
                getDataSchema(PDouble.INSTANCE, SortOrder.getDefault()),
                false,
                PNameFactory.newName("1.7976931348623158"),
                PDouble.INSTANCE.toBytes(PDouble.INSTANCE.toObject("1.7976931348623158"))
        });

        //UnsignedDouble
        testCases.add(new Object[] {
                getDataSchema(PUnsignedDouble.INSTANCE, SortOrder.getDefault()),
                false,
                PNameFactory.newName("1.7976931348623158"),
                PUnsignedDouble.INSTANCE.toBytes(PUnsignedDouble.INSTANCE.toObject("1.7976931348623158"))
        });

        //UnsignedDecimal
        testCases.add(new Object[] {
                getDataSchema(PDecimal.INSTANCE, SortOrder.getDefault()),
                false,
                PNameFactory.newName("3.402823466"),
                PDecimal.INSTANCE.toBytes(PDecimal.INSTANCE.toObject("3.402823466"))
        });

        //Boolean
        testCases.add(new Object[] {
                getDataSchema(PBoolean.INSTANCE, SortOrder.getDefault()),
                false,
                PNameFactory.newName("true"),
                PBoolean.INSTANCE.toBytes(PBoolean.INSTANCE.toObject("true"))
        });

        //Binary
        byte[] bytes = new byte[] {0, 1, 2, 3};
        String byteString = new String( Base64.encodeBytes(bytes) );
        testCases.add(new Object[] {
                getDataSchema(PBinary.INSTANCE, SortOrder.getDefault()),
                false,
                PNameFactory.newName(byteString),
                PBinary.INSTANCE.toBytes(PBinary.INSTANCE.toObject(byteString))
        });

        //Descending TenantId
        testCases.add(new Object[] {
                getDataSchema(PUnsignedInt.INSTANCE, SortOrder.DESC),
                false,
                PNameFactory.newName("2147483646"),
                PUnsignedInt.INSTANCE.toBytes(PUnsignedInt.INSTANCE.toObject("2147483646"))
        });

        return testCases;
    }

    public static RowKeySchema getDataSchema (final PDataType data, final SortOrder sortOrder) {
        RowKeySchemaBuilder builder = new RowKeySchemaBuilder(3);

        builder.addField(new PDatum() {
            @Override public boolean isNullable() {
                return false;
            }

            @Override public PDataType getDataType() {
                return data;
            }

            @Override public Integer getMaxLength() {
                return 1;
            }

            @Override public Integer getScale() {
                return null;
            }

            @Override public SortOrder getSortOrder() {
                return sortOrder;
            }
        }, false, sortOrder);

        builder.addField(new PDatum() {
            @Override public boolean isNullable() {
                return false;
            }

            @Override public PDataType getDataType() {
                return PUnsignedInt.INSTANCE;
            }

            @Override public Integer getMaxLength() {
                return 3;
            }

            @Override public Integer getScale() {
                return null;
            }

            @Override public SortOrder getSortOrder() {
                return sortOrder;
            }
        }, false, sortOrder);

        builder.addField(new PDatum() {
            @Override public boolean isNullable() {
                return true;
            }

            @Override public PDataType getDataType() {
                return PVarchar.INSTANCE;
            }

            @Override public Integer getMaxLength() {
                return 3;
            }

            @Override public Integer getScale() {
                return null;
            }

            @Override public SortOrder getSortOrder() {
                return sortOrder;
            }
        }, false, sortOrder);

        return builder.build();
    }
}
