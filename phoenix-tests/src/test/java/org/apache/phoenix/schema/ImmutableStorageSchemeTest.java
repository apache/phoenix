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

import static org.apache.phoenix.schema.PTable.ImmutableStorageScheme.SINGLE_CELL_ARRAY_WITH_OFFSETS;
import static org.apache.phoenix.schema.types.PArrayDataType.IMMUTABLE_SERIALIZATION_V2;
import static org.apache.phoenix.schema.types.PArrayDataType.IMMUTABLE_SERIALIZATION_VERSION;
import static org.apache.phoenix.util.ByteUtil.EMPTY_BYTE_ARRAY;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.DelegateExpression;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.expression.SingleCellConstructorExpression;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PTable.ImmutableStorageScheme;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PSmallint;
import org.apache.phoenix.schema.types.PTinyint;
import org.apache.phoenix.schema.types.PUnsignedTinyint;
import org.apache.phoenix.schema.types.PVarbinary;
import org.apache.phoenix.util.ByteUtil;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;

@RunWith(Parameterized.class)
public class ImmutableStorageSchemeTest {
    
    protected static final LiteralExpression CONSTANT_EXPRESSION = LiteralExpression.newConstant(QueryConstants.EMPTY_COLUMN_VALUE_BYTES);
    protected static final byte[] BYTE_ARRAY1 = new byte[]{1,2,3,4,5};
    protected static final byte[] BYTE_ARRAY2 = new byte[]{6,7,8};
    protected Expression FALSE_EVAL_EXPRESSION = new DelegateExpression(LiteralExpression.newConstant(null)) {
        @Override
        public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
            return false;
        }
    };
    private ImmutableStorageScheme immutableStorageScheme;
    byte serializationVersion;
    
    @Parameters(name="ImmutableStorageSchemeTest_immutableStorageScheme={0},serializationVersion={1}}") // name is used by failsafe as file name in reports
    public static synchronized List<Object[]> data() {
        return Arrays.asList(new Object[][] {
                { SINGLE_CELL_ARRAY_WITH_OFFSETS,
                        IMMUTABLE_SERIALIZATION_VERSION },
                { SINGLE_CELL_ARRAY_WITH_OFFSETS,
                        IMMUTABLE_SERIALIZATION_V2 }
                        });
    }
    
    public ImmutableStorageSchemeTest(ImmutableStorageScheme immutableStorageScheme, byte serializationVersion) {
        this.immutableStorageScheme = immutableStorageScheme;
        this.immutableStorageScheme.setSerializationVersion(serializationVersion);
        this.serializationVersion = serializationVersion;
    }

    @Test
    public void testWithExpressionsThatEvaluatetoFalse() throws Exception {
        List<Expression> children = Lists.newArrayListWithExpectedSize(4);
        children.add(CONSTANT_EXPRESSION);
        children.add(FALSE_EVAL_EXPRESSION);
        children.add(LiteralExpression.newConstant(BYTE_ARRAY1, PVarbinary.INSTANCE));
        children.add(FALSE_EVAL_EXPRESSION);
        children.add(LiteralExpression.newConstant(BYTE_ARRAY2, PVarbinary.INSTANCE));
        ImmutableBytesPtr ptr = evaluate(children);
        
        ImmutableBytesPtr ptrCopy = new ImmutableBytesPtr(ptr);
        ColumnValueDecoder decoder = immutableStorageScheme.getDecoder();
        assertTrue(decoder.decode(ptrCopy, 0));
        assertArrayEquals(QueryConstants.EMPTY_COLUMN_VALUE_BYTES, ptrCopy.copyBytesIfNecessary());
        ptrCopy = new ImmutableBytesPtr(ptr);
        assertFalse(decoder.decode(ptrCopy, 1));
        assertArrayEquals(ByteUtil.EMPTY_BYTE_ARRAY, ptrCopy.copyBytesIfNecessary());
        ptrCopy = new ImmutableBytesPtr(ptr);
        assertTrue(decoder.decode(ptrCopy, 2));
        assertArrayEquals(BYTE_ARRAY1, ptrCopy.copyBytesIfNecessary());
        ptrCopy = new ImmutableBytesPtr(ptr);
        assertFalse(decoder.decode(ptrCopy, 3));
        assertArrayEquals(ByteUtil.EMPTY_BYTE_ARRAY, ptrCopy.copyBytesIfNecessary());
        ptrCopy = new ImmutableBytesPtr(ptr);
        assertTrue(decoder.decode(ptrCopy, 4));
        assertArrayEquals(BYTE_ARRAY2, ptrCopy.copyBytesIfNecessary());
    }
    
    @Test
    public void testWithMaxOffsetLargerThanShortMax() throws Exception {
        int numElements = Short.MAX_VALUE+2;
        List<Expression> children = Lists.newArrayListWithExpectedSize(numElements);
        for (int i=0; i<numElements; ++i) {
            children.add(CONSTANT_EXPRESSION);
        }
        SingleCellConstructorExpression singleCellConstructorExpression = new SingleCellConstructorExpression(immutableStorageScheme, children);
        ImmutableBytesPtr ptr = new ImmutableBytesPtr();
        singleCellConstructorExpression.evaluate(null, ptr);

        ImmutableBytesPtr ptrCopy = new ImmutableBytesPtr(ptr);
        ColumnValueDecoder decoder = immutableStorageScheme.getDecoder();
        assertTrue(decoder.decode(ptrCopy, 0));
        assertArrayEquals(QueryConstants.EMPTY_COLUMN_VALUE_BYTES, ptrCopy.copyBytesIfNecessary());
        
        ptrCopy = new ImmutableBytesPtr(ptr);
        assertTrue(decoder.decode(ptrCopy, 14999));
        assertArrayEquals(QueryConstants.EMPTY_COLUMN_VALUE_BYTES, ptrCopy.copyBytesIfNecessary());
        
        ptrCopy = new ImmutableBytesPtr(ptr);
        assertTrue(decoder.decode(ptrCopy, numElements-1));
        assertArrayEquals(QueryConstants.EMPTY_COLUMN_VALUE_BYTES, ptrCopy.copyBytesIfNecessary());
    }
    
    @Test
    public void testWithMaxOffsetSmallerThanShortMin() throws Exception {
        int numElements = Short.MAX_VALUE+2;
        List<Expression> children = Lists.newArrayListWithExpectedSize(numElements);
        for (int i=0; i<=numElements; i+=2) {
            children.add(CONSTANT_EXPRESSION);
            children.add(FALSE_EVAL_EXPRESSION);
        }
        SingleCellConstructorExpression singleCellConstructorExpression = new SingleCellConstructorExpression(immutableStorageScheme, children);
        ImmutableBytesPtr ptr = new ImmutableBytesPtr();
        singleCellConstructorExpression.evaluate(null, ptr);

        ImmutableBytesPtr ptrCopy = new ImmutableBytesPtr(ptr);
        ColumnValueDecoder decoder = immutableStorageScheme.getDecoder();
        assertTrue(decoder.decode(ptrCopy, 0));
        assertArrayEquals(QueryConstants.EMPTY_COLUMN_VALUE_BYTES, ptrCopy.copyBytesIfNecessary());
        
        ptrCopy = new ImmutableBytesPtr(ptr);
        assertFalse(decoder.decode(ptrCopy, 1));
        assertArrayEquals(ByteUtil.EMPTY_BYTE_ARRAY, ptrCopy.copyBytesIfNecessary());
        
        ptrCopy = new ImmutableBytesPtr(ptr);
        assertTrue(decoder.decode(ptrCopy, numElements-1));
        assertArrayEquals(QueryConstants.EMPTY_COLUMN_VALUE_BYTES, ptrCopy.copyBytesIfNecessary());
        
        ptrCopy = new ImmutableBytesPtr(ptr);
        assertFalse(decoder.decode(ptrCopy, numElements));
        assertArrayEquals(ByteUtil.EMPTY_BYTE_ARRAY, ptrCopy.copyBytesIfNecessary());
    }
    
    @Test
    public void testLeadingNulls() throws Exception {
        List<Expression> children = Lists.newArrayListWithExpectedSize(4);
        LiteralExpression nullExpression = LiteralExpression.newConstant(null);
        children.add(nullExpression);
        children.add(nullExpression);
        children.add(LiteralExpression.newConstant(BYTE_ARRAY1, PVarbinary.INSTANCE));
        children.add(LiteralExpression.newConstant(BYTE_ARRAY2, PVarbinary.INSTANCE));
        ImmutableBytesPtr ptr = evaluate(children);

        assertDecodedContents(ptr, new byte[][] {EMPTY_BYTE_ARRAY, EMPTY_BYTE_ARRAY, BYTE_ARRAY1, BYTE_ARRAY2});
    }

    @Test
    public void testTrailingNulls() throws Exception {
        List<Expression> children = Lists.newArrayListWithExpectedSize(4);
        LiteralExpression nullExpression = LiteralExpression.newConstant(null);
        children.add(LiteralExpression.newConstant(BYTE_ARRAY1, PVarbinary.INSTANCE));
        children.add(LiteralExpression.newConstant(BYTE_ARRAY2, PVarbinary.INSTANCE));
        children.add(nullExpression);
        children.add(nullExpression);
        ImmutableBytesPtr ptr = evaluate(children);

        assertDecodedContents(ptr, new byte[][] {BYTE_ARRAY1, BYTE_ARRAY2, EMPTY_BYTE_ARRAY, EMPTY_BYTE_ARRAY});
    }

    @Test
    public void testManyNulls() throws Exception {
        List<Expression> children = Lists.newArrayListWithExpectedSize(4);
        LiteralExpression nullExpression = LiteralExpression.newConstant(null);
        byte[][] testData = new byte[300][];
        children.add(LiteralExpression.newConstant(BYTE_ARRAY1, PVarbinary.INSTANCE));
        testData[0] = BYTE_ARRAY1;
        for (int i = 1; i < testData.length - 1; i++) {
            children.add(nullExpression);
            testData[i] = EMPTY_BYTE_ARRAY;
        }
        children.add(LiteralExpression.newConstant(BYTE_ARRAY2, PVarbinary.INSTANCE));
        testData[299] = BYTE_ARRAY2;
        ImmutableBytesPtr ptr = evaluate(children);

        assertDecodedContents(ptr, testData);
    }

    @Test
    public void testSingleLeadingTrailingNull() throws Exception {
        List<Expression> children = Lists.newArrayListWithExpectedSize(4);
        LiteralExpression nullExpression = LiteralExpression.newConstant(null);
        children.add(nullExpression);
        children.add(LiteralExpression.newConstant(BYTE_ARRAY1, PVarbinary.INSTANCE));
        children.add(nullExpression);
        ImmutableBytesPtr ptr = evaluate(children);

        assertDecodedContents(ptr,
            new byte[][] { EMPTY_BYTE_ARRAY, BYTE_ARRAY1, EMPTY_BYTE_ARRAY });
    }

    @Test
    public void testSingleMiddleNull() throws Exception {
        List<Expression> children = Lists.newArrayListWithExpectedSize(4);
        LiteralExpression nullExpression = LiteralExpression.newConstant(null);
        children.add(LiteralExpression.newConstant(BYTE_ARRAY1, PVarbinary.INSTANCE));
        children.add(nullExpression);
        children.add(LiteralExpression.newConstant(BYTE_ARRAY2, PVarbinary.INSTANCE));
        ImmutableBytesPtr ptr = evaluate(children);

        assertDecodedContents(ptr, new byte[][] { BYTE_ARRAY1, EMPTY_BYTE_ARRAY, BYTE_ARRAY2 });
    }

    @Test
    public void testAllShortValues() throws Exception {
        int curr = Short.MIN_VALUE;
        List<Expression> children = Lists.newArrayListWithExpectedSize(1);
        List<Integer> failedValues = Lists.newArrayList();
        while (curr <= Short.MAX_VALUE) {
            children.add(LiteralExpression.newConstant(curr, PSmallint.INSTANCE));
            ImmutableBytesPtr ptr = evaluate(children);
            ColumnValueDecoder decoder = immutableStorageScheme.getDecoder();
            assertTrue(decoder.decode(ptr, 0));
            if (ptr.getLength() == 0) {
                failedValues.add(curr);
            } else {
                if (curr != PSmallint.INSTANCE.getCodec().decodeShort(ptr.copyBytesIfNecessary(), 0,
                    SortOrder.ASC)) {
                    failedValues.add(curr);
                }
            }
            children.remove(0);
            curr++;
        }
        // in v1, we can't distinguish a null from two short values
        if (serializationVersion == IMMUTABLE_SERIALIZATION_VERSION) {
            assertTrue(failedValues.size() + " values were not properly decoded: " + failedValues,
                failedValues.size() == 2);
        } else {
            assertTrue(failedValues.size() + " values were not properly decoded: " + failedValues,
                failedValues.size() == 0);
        }
    }

    @Test
    public void testSingleByteValues() throws Exception {
        List<Expression> children = Lists.newArrayListWithExpectedSize(4);
        LiteralExpression nullExpression = LiteralExpression.newConstant(null);
        children.add(nullExpression);
        children.add(LiteralExpression.newConstant((byte) -128, PTinyint.INSTANCE));
        children.add(nullExpression);
        children.add(LiteralExpression.newConstant((byte) 0, PUnsignedTinyint.INSTANCE));
        children.add(nullExpression);
        children.add(LiteralExpression.newConstant((byte) 127, PUnsignedTinyint.INSTANCE));
        ImmutableBytesPtr ptr = evaluate(children);

        assertNullAtIndex(ptr, 0);
        assertValueAtIndex(ptr, 1, (byte) -128, PTinyint.INSTANCE);
        assertNullAtIndex(ptr, 2);
        assertValueAtIndex(ptr, 3, (byte) 0, PUnsignedTinyint.INSTANCE);
        assertNullAtIndex(ptr, 4);
        assertValueAtIndex(ptr, 5, (byte) 127, PUnsignedTinyint.INSTANCE);
    }

    @Test
    public void testSeparatorByteValues() throws Exception {
        List<Expression> children = Lists.newArrayListWithExpectedSize(4);
        LiteralExpression nullExpression = LiteralExpression.newConstant(null);
        children.add(nullExpression);
        children.add(LiteralExpression.newConstant((short) -32513, PSmallint.INSTANCE));
        children.add(nullExpression);
        children.add(LiteralExpression.newConstant((short) 32767, PSmallint.INSTANCE));
        children.add(nullExpression);
        children.add(LiteralExpression.newConstant(Integer.MAX_VALUE, PInteger.INSTANCE));
        children.add(nullExpression);
        children.add(LiteralExpression.newConstant(Integer.MIN_VALUE, PInteger.INSTANCE));
        // see if we can differentiate two nulls and {separatorByte, 2}
        children.add(nullExpression);
        children.add(nullExpression);
        children.add(LiteralExpression.newConstant((short) -32514, PSmallint.INSTANCE));

        ImmutableBytesPtr ptr = evaluate(children);

        assertNullAtIndex(ptr, 0);
        try {
            assertValueAtIndex(ptr, 1, (short) -32513, PSmallint.INSTANCE);
        } catch (Exception e) {
            if (serializationVersion != IMMUTABLE_SERIALIZATION_VERSION) {
                fail("Failed on exception " + e);
            }
        }
        assertNullAtIndex(ptr, 2);
        try {
            assertValueAtIndex(ptr, 3, (short) 32767, PSmallint.INSTANCE);
        } catch (Exception e) {
            if (serializationVersion != IMMUTABLE_SERIALIZATION_VERSION) {
                fail("Failed on exception " + e);
            }
        }
        assertNullAtIndex(ptr, 4);
        assertValueAtIndex(ptr, 5, Integer.MAX_VALUE, PInteger.INSTANCE);
        assertNullAtIndex(ptr, 6);
        assertValueAtIndex(ptr, 7, Integer.MIN_VALUE, PInteger.INSTANCE);
        assertNullAtIndex(ptr, 8);
        assertNullAtIndex(ptr, 9);
        assertValueAtIndex(ptr, 10, (short) -32514, PSmallint.INSTANCE);
    }

    private void assertNullAtIndex(ImmutableBytesPtr ptr, int index) {
        assertValueAtIndex(ptr, index, null, null);
    }

    private void assertValueAtIndex(ImmutableBytesPtr ptr, int index, Object value,
            PDataType type) {
        ImmutableBytesPtr ptrCopy = new ImmutableBytesPtr(ptr);
        ColumnValueDecoder decoder = immutableStorageScheme.getDecoder();
        assertTrue(decoder.decode(ptrCopy, index));
        if (value == null) {
            assertArrayEquals(EMPTY_BYTE_ARRAY, ptrCopy.copyBytesIfNecessary());
            return;
        }
        Object decoded;
        if (type.equals(PSmallint.INSTANCE)) {
            decoded = type.getCodec().decodeShort(ptrCopy.copyBytesIfNecessary(), 0, SortOrder.ASC);
        } else if (type.equals(PInteger.INSTANCE)) {
            decoded = type.getCodec().decodeInt(ptrCopy.copyBytesIfNecessary(), 0, SortOrder.ASC);
        } else { // assume byte for all other types
            decoded = type.getCodec().decodeByte(ptrCopy.copyBytesIfNecessary(), 0, SortOrder.ASC);
        }
        assertEquals(value, decoded);
    }

    private ImmutableBytesPtr evaluate(List<Expression> children) {
        SingleCellConstructorExpression singleCellConstructorExpression =
                new SingleCellConstructorExpression(immutableStorageScheme, children);
        ImmutableBytesPtr ptr = new ImmutableBytesPtr();
        singleCellConstructorExpression.evaluate(null, ptr);
        return ptr;
    }

    private void assertDecodedContents(ImmutableBytesPtr ptr, byte[]... contents) {
        ColumnValueDecoder decoder = immutableStorageScheme.getDecoder();
        for (int i = 0; i < contents.length; i++) {
            ImmutableBytesPtr ptrCopy = new ImmutableBytesPtr(ptr);
            assertTrue(decoder.decode(ptrCopy, i));
            assertArrayEquals(contents[i], ptrCopy.copyBytesIfNecessary());
        }
    }

}
