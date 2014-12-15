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
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.schema.types.PArrayDataType;
import org.apache.phoenix.schema.types.PBoolean;
import org.apache.phoenix.schema.types.PBooleanArray;
import org.apache.phoenix.schema.types.PChar;
import org.apache.phoenix.schema.types.PCharArray;
import org.apache.phoenix.schema.types.PDate;
import org.apache.phoenix.schema.types.PDateArray;
import org.apache.phoenix.schema.types.PDecimal;
import org.apache.phoenix.schema.types.PDecimalArray;
import org.apache.phoenix.schema.types.PDouble;
import org.apache.phoenix.schema.types.PDoubleArray;
import org.apache.phoenix.schema.types.PFloat;
import org.apache.phoenix.schema.types.PFloatArray;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PIntegerArray;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.schema.types.PLongArray;
import org.apache.phoenix.schema.types.PSmallint;
import org.apache.phoenix.schema.types.PSmallintArray;
import org.apache.phoenix.schema.types.PTime;
import org.apache.phoenix.schema.types.PTimeArray;
import org.apache.phoenix.schema.types.PTimestamp;
import org.apache.phoenix.schema.types.PTimestampArray;
import org.apache.phoenix.schema.types.PTinyint;
import org.apache.phoenix.schema.types.PTinyintArray;
import org.apache.phoenix.schema.types.PUnsignedDate;
import org.apache.phoenix.schema.types.PUnsignedDateArray;
import org.apache.phoenix.schema.types.PUnsignedDouble;
import org.apache.phoenix.schema.types.PUnsignedDoubleArray;
import org.apache.phoenix.schema.types.PUnsignedFloat;
import org.apache.phoenix.schema.types.PUnsignedFloatArray;
import org.apache.phoenix.schema.types.PUnsignedInt;
import org.apache.phoenix.schema.types.PUnsignedIntArray;
import org.apache.phoenix.schema.types.PUnsignedLong;
import org.apache.phoenix.schema.types.PUnsignedLongArray;
import org.apache.phoenix.schema.types.PUnsignedSmallint;
import org.apache.phoenix.schema.types.PUnsignedSmallintArray;
import org.apache.phoenix.schema.types.PUnsignedTime;
import org.apache.phoenix.schema.types.PUnsignedTimeArray;
import org.apache.phoenix.schema.types.PUnsignedTimestamp;
import org.apache.phoenix.schema.types.PUnsignedTimestampArray;
import org.apache.phoenix.schema.types.PUnsignedTinyint;
import org.apache.phoenix.schema.types.PUnsignedTinyintArray;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.schema.types.PVarcharArray;
import org.apache.phoenix.schema.types.PhoenixArray;
import org.junit.Ignore;
import org.junit.Test;

public class PDataTypeForArraysTest {
	@Test
	public void testForIntegerArray() {
		Integer[] intArr = new Integer[2];
		intArr[0] = 1;
		intArr[1] = 2;
		PhoenixArray arr = PArrayDataType.instantiatePhoenixArray(
        PInteger.INSTANCE, intArr);
		PIntegerArray.INSTANCE.toObject(arr, PIntegerArray.INSTANCE);
		byte[] bytes = PIntegerArray.INSTANCE.toBytes(arr);
		PhoenixArray resultArr = (PhoenixArray) PIntegerArray.INSTANCE
				.toObject(bytes, 0, bytes.length);
		assertEquals(arr, resultArr);
	}

	@Test
	public void testForBooleanArray() {
		Boolean[] boolArr = new Boolean[2];
		boolArr[0] = true;
		boolArr[1] = false;
		PhoenixArray arr = PArrayDataType.instantiatePhoenixArray(
				PBoolean.INSTANCE, boolArr);
		PBooleanArray.INSTANCE.toObject(arr, PBooleanArray.INSTANCE);
		byte[] bytes = PBooleanArray.INSTANCE.toBytes(arr);
		PhoenixArray resultArr = (PhoenixArray) PBooleanArray.INSTANCE
				.toObject(bytes, 0, bytes.length);
		assertEquals(arr, resultArr);
	}

	@Test
	public void testForVarCharArray() {
		String[] strArr = new String[2];
		strArr[0] = "abc";
		strArr[1] = "klmnop";
		PhoenixArray arr = PArrayDataType.instantiatePhoenixArray(
				PVarchar.INSTANCE, strArr);
		byte[] bytes = PVarcharArray.INSTANCE.toBytes(arr);
		PhoenixArray resultArr = (PhoenixArray) PVarcharArray.INSTANCE
				.toObject(bytes, 0, bytes.length);
		assertEquals(arr, resultArr);
	}
	
	@Test
	public void testVarCharArrayWithNullValues1() {
	    String[] strArr = new String[6];
        strArr[0] = "abc";
        strArr[1] = null;
        strArr[2] = "bcd";
        strArr[3] = null;
        strArr[4] = null;
        strArr[5] = "b";
        PhoenixArray arr = PArrayDataType.instantiatePhoenixArray(
                PVarchar.INSTANCE, strArr);
        byte[] bytes = PVarcharArray.INSTANCE.toBytes(arr);
        PhoenixArray resultArr = (PhoenixArray) PVarcharArray.INSTANCE
                .toObject(bytes, 0, bytes.length);
        assertEquals(arr, resultArr);
	}
	
    @Test
    public void testVarCharArrayWithNullValues2() {
        String[] strArr = new String[6];
        strArr[0] = "abc";
        strArr[1] = null;
        strArr[2] = "bcd";
        strArr[3] = null;
        strArr[4] = "cde";
        strArr[5] = null;
        PhoenixArray arr = PArrayDataType.instantiatePhoenixArray(PVarchar.INSTANCE, strArr);
        byte[] bytes = PVarcharArray.INSTANCE.toBytes(arr);
        PhoenixArray resultArr = (PhoenixArray) PVarcharArray.INSTANCE.toObject(bytes, 0, bytes.length);
        assertEquals(arr, resultArr);
    }
    
    @Test
    public void testVarCharArrayWithNullValues3() {
        String[] strArr = new String[6];
        strArr[0] = "abc";
        strArr[1] = null;
        strArr[2] = null;
        strArr[3] = null;
        strArr[4] = null;
        strArr[5] = null;
        PhoenixArray arr = PArrayDataType.instantiatePhoenixArray(PVarchar.INSTANCE, strArr);
        byte[] bytes = PVarcharArray.INSTANCE.toBytes(arr);
        PhoenixArray resultArr = (PhoenixArray) PVarcharArray.INSTANCE.toObject(bytes, 0, bytes.length);
        assertEquals(arr, resultArr);
    }
    
    @Test
    public void testVarCharArrayWithNullValues4() {
        String[] strArr = new String[7];
        strArr[0] = "abc";
        strArr[1] = null;
        strArr[2] = null;
        strArr[3] = null;
        strArr[4] = null;
        strArr[5] = null;
        strArr[6] = "xys";
        PhoenixArray arr = PArrayDataType.instantiatePhoenixArray(PVarchar.INSTANCE, strArr);
        byte[] bytes = PVarcharArray.INSTANCE.toBytes(arr);
        PhoenixArray resultArr = (PhoenixArray) PVarcharArray.INSTANCE.toObject(bytes, 0, bytes.length);
        assertEquals(arr, resultArr);
    }
    
    
    @Test
    public void testVarCharArrayWithNullValues5() {
        String[] strArr = new String[6];
        strArr[0] = "abc";
        strArr[1] = "bcd";
        strArr[2] = "cde";
        strArr[3] = null;
        strArr[4] = null;
        strArr[5] = null;
        PhoenixArray arr = PArrayDataType.instantiatePhoenixArray(PVarchar.INSTANCE, strArr);
        byte[] bytes = PVarcharArray.INSTANCE.toBytes(arr);
        PhoenixArray resultArr = (PhoenixArray) PVarcharArray.INSTANCE.toObject(bytes, 0, bytes.length);
        assertEquals(arr, resultArr);
    }
    
    @Test
    public void testVarCharArrayWithNullValues6() {
        String[] strArr = new String[6];
        strArr[0] = "abc";
        strArr[1] = null;
        strArr[2] = "cde";
        strArr[3] = "bcd";
        strArr[4] = null;
        strArr[5] = null;
        PhoenixArray arr = PArrayDataType.instantiatePhoenixArray(PVarchar.INSTANCE, strArr);
        byte[] bytes = PVarcharArray.INSTANCE.toBytes(arr);
        PhoenixArray resultArr = (PhoenixArray) PVarcharArray.INSTANCE.toObject(bytes, 0, bytes.length);
        assertEquals(arr, resultArr);
    }
    
    @Test
    public void testVarCharArrayWithNullValues7() {
        String[] strArr = new String[6];
        strArr[0] = null;
        strArr[1] = "abc";
        strArr[2] = null;
        strArr[3] = "bcd";
        strArr[4] = null;
        strArr[5] = "cde";
        PhoenixArray arr = PArrayDataType.instantiatePhoenixArray(PVarchar.INSTANCE, strArr);
        byte[] bytes = PVarcharArray.INSTANCE.toBytes(arr);
        PhoenixArray resultArr = (PhoenixArray) PVarcharArray.INSTANCE.toObject(bytes, 0, bytes.length);
        assertEquals(arr, resultArr);
    }

	@Test
	public void testForCharArray() {
		String[] strArr = new String[2];
		strArr[0] = "a";
		strArr[1] = "d";
		PhoenixArray arr = PArrayDataType.instantiatePhoenixArray(
				PChar.INSTANCE, strArr);
		byte[] bytes = PCharArray.INSTANCE.toBytes(arr);
		PhoenixArray resultArr = (PhoenixArray) PCharArray.INSTANCE.toObject(
				bytes, 0, bytes.length, PCharArray.INSTANCE, null, 1, null);
		assertEquals(arr, resultArr);
	}

	@Test
	public void testForLongArray() {
		Long[] longArr = new Long[2];
		longArr[0] = 1l;
		longArr[1] = 2l;
		PhoenixArray arr = PArrayDataType.instantiatePhoenixArray(
				PLong.INSTANCE, longArr);
		PLongArray.INSTANCE.toObject(arr, PLongArray.INSTANCE);
		byte[] bytes = PLongArray.INSTANCE.toBytes(arr);
		PhoenixArray resultArr = (PhoenixArray) PLongArray.INSTANCE.toObject(
				bytes, 0, bytes.length);
		assertEquals(arr, resultArr);
	}

	@Test
	public void testForSmallIntArray() {
		Short[] shortArr = new Short[2];
		shortArr[0] = 1;
		shortArr[1] = 2;
		PhoenixArray arr = PArrayDataType.instantiatePhoenixArray(
				PSmallint.INSTANCE, shortArr);
		PSmallintArray.INSTANCE.toObject(arr, PSmallintArray.INSTANCE);
		byte[] bytes = PSmallintArray.INSTANCE.toBytes(arr);
		PhoenixArray resultArr = (PhoenixArray) PSmallintArray.INSTANCE
				.toObject(bytes, 0, bytes.length);
		assertEquals(arr, resultArr);
	}

	@Test
	public void testForVarCharArrayForOddNumber() {
		String[] strArr = new String[3];
		strArr[0] = "abx";
		strArr[1] = "ereref";
		strArr[2] = "random";
		PhoenixArray arr = PArrayDataType.instantiatePhoenixArray(
				PVarchar.INSTANCE, strArr);
		byte[] bytes = PVarcharArray.INSTANCE.toBytes(arr);
		PhoenixArray resultArr = (PhoenixArray) PVarcharArray.INSTANCE
				.toObject(bytes, 0, bytes.length);
		assertEquals(arr, resultArr);
	}

    @Test
    public void testForVarCharArrayOneElement() {
        String[] strArr = new String[1];
        strArr[0] = "ereref";
        PhoenixArray arr = PArrayDataType.instantiatePhoenixArray(
                PVarchar.INSTANCE, strArr);
        byte[] bytes = PVarcharArray.INSTANCE.toBytes(arr);
        PhoenixArray resultArr = (PhoenixArray) PVarcharArray.INSTANCE
                .toObject(bytes, 0, bytes.length);
        assertEquals(arr, resultArr);
    }

    @Test
    public void testForVarcharArrayWith1ElementInLargerBuffer() {
        String[] strArr = new String[1];
        strArr[0] = "abx";
        PhoenixArray arr = PArrayDataType.instantiatePhoenixArray(PVarchar.INSTANCE, strArr);
        byte[] bytes = PVarcharArray.INSTANCE.toBytes(arr);
        byte[] moreBytes = new byte[bytes.length + 20];
        // Generate some garbage
        for (int i = 0; i < moreBytes.length; i++) {
            moreBytes[i] = (byte)-i;
        }
        System.arraycopy(bytes, 0, moreBytes, 10, bytes.length);
        PhoenixArray resultArr = (PhoenixArray) PVarcharArray.INSTANCE.toObject(moreBytes, 10, bytes.length);
        assertEquals(arr, resultArr);
    }
    
    
	@Test
	public void testForVarCharArrayForEvenNumberWithIndex() {
		String[] strArr = new String[5];
		strArr[0] = "abx";
		strArr[1] = "ereref";
		strArr[2] = "random";
		strArr[3] = "random12";
		strArr[4] = "ranzzz";
		PhoenixArray arr = PArrayDataType.instantiatePhoenixArray(
				PVarchar.INSTANCE, strArr);
		byte[] bytes = PVarcharArray.INSTANCE.toBytes(arr);
		ImmutableBytesWritable ptr = new ImmutableBytesWritable(bytes);
        PArrayDataType.positionAtArrayElement(ptr, 4, PVarchar.INSTANCE, PVarchar.INSTANCE.getByteSize());
		int offset = ptr.getOffset();
		int length = ptr.getLength();
		byte[] bs = ptr.get();
		byte[] res = new byte[length];
		System.arraycopy(bs, offset, res, 0, length);
		assertEquals("ranzzz", Bytes.toString(res));
	}
	
    
    @Test
    public void testForVarCharArrayWithOneElementIndex() {
        String[] strArr = new String[1];
        strArr[0] = "abx";
        PhoenixArray arr = PArrayDataType.instantiatePhoenixArray(
                PVarchar.INSTANCE, strArr);
        byte[] bytes = PVarcharArray.INSTANCE.toBytes(arr);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable(bytes);
        PArrayDataType.positionAtArrayElement(ptr, 0, PVarchar.INSTANCE, PVarchar.INSTANCE.getByteSize());
        int offset = ptr.getOffset();
        int length = ptr.getLength();
        byte[] bs = ptr.get();
        byte[] res = new byte[length];
        System.arraycopy(bs, offset, res, 0, length);
        assertEquals("abx", Bytes.toString(res));
    }
	
	@Ignore
	public void testVariableLengthArrayWithElementsMoreThanShortMax() {
	    String[] strArr = new String[(2 * Short.MAX_VALUE) + 100]; 
	    for(int i = 0 ; i < (2 * Short.MAX_VALUE) + 100; i++ ) {
	        String str = "abc";
	        for(int j = 0 ; j <= i ;j++) {
	            str += "-";
	        }
	        strArr[i] = str;
	    }
	    PhoenixArray arr = PArrayDataType.instantiatePhoenixArray(
                PVarchar.INSTANCE, strArr);
        byte[] bytes = PVarcharArray.INSTANCE.toBytes(arr);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable(bytes);
        PArrayDataType.positionAtArrayElement(ptr, 3, PVarchar.INSTANCE, PVarchar.INSTANCE.getByteSize());
        int offset = ptr.getOffset();
        int length = ptr.getLength();
        byte[] bs = ptr.get();
        byte[] res = new byte[length];
        System.arraycopy(bs, offset, res, 0, length);
        assertEquals("abc---", Bytes.toString(res));
	}
	
	@Test
	public void testGetArrayLengthForVariableLengthArray() {
		String[] strArr = new String[5];
		strArr[0] = "abx";
		strArr[1] = "ereref";
		strArr[2] = "random";
		strArr[3] = "random12";
		strArr[4] = "ranzzz";
		PhoenixArray arr = PArrayDataType.instantiatePhoenixArray(
				PVarchar.INSTANCE, strArr);
		byte[] bytes = PVarcharArray.INSTANCE.toBytes(arr);
		ImmutableBytesWritable ptr = new ImmutableBytesWritable(bytes);
		int result = PArrayDataType.getArrayLength(ptr, PVarchar.INSTANCE, null);
		assertEquals(5, result);
	}

	@Test
	public void testForVarCharArrayForOddNumberWithIndex() {
		String[] strArr = new String[5];
		strArr[0] = "abx";
		strArr[1] = "ereref";
		strArr[2] = "random";
		strArr[3] = "random12";
		strArr[4] = "ran";
		PhoenixArray arr = PArrayDataType.instantiatePhoenixArray(
				PVarchar.INSTANCE, strArr);
		byte[] bytes = PVarcharArray.INSTANCE.toBytes(arr);
		ImmutableBytesWritable ptr = new ImmutableBytesWritable(bytes);
		PArrayDataType.positionAtArrayElement(ptr, 3, PVarchar.INSTANCE, PVarchar.INSTANCE.getByteSize());
		int offset = ptr.getOffset();
		int length = ptr.getLength();
		byte[] bs = ptr.get();
		byte[] res = new byte[length];
		System.arraycopy(bs, offset, res, 0, length);
		assertEquals("random12", Bytes.toString(res));
	}

    @Test
    public void testPositionSearchWithVarLengthArrayWithNullValue1() {
        String[] strArr = new String[5];
        strArr[0] = "abx";
        strArr[1] = "ereref";
        strArr[2] = "random";
        strArr[3] = null;
        strArr[4] = "ran";
        PhoenixArray arr = PArrayDataType.instantiatePhoenixArray(
                PVarchar.INSTANCE, strArr);
        byte[] bytes = PVarcharArray.INSTANCE.toBytes(arr);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable(bytes);
        PArrayDataType.positionAtArrayElement(ptr, 2, PVarchar.INSTANCE, PVarchar.INSTANCE.getByteSize());
        int offset = ptr.getOffset();
        int length = ptr.getLength();
        byte[] bs = ptr.get();
        byte[] res = new byte[length];
        System.arraycopy(bs, offset, res, 0, length);
        assertEquals("random", Bytes.toString(res));
    }
    
    @Test
    public void testPositionSearchWithVarLengthArrayWithNullValue2() {
        String[] strArr = new String[5];
        strArr[0] = "abx";
        strArr[1] = "ereref";
        strArr[2] = "random";
        strArr[3] = "random12";
        strArr[4] = null;
        PhoenixArray arr = PArrayDataType.instantiatePhoenixArray(
                PVarchar.INSTANCE, strArr);
        byte[] bytes = PVarcharArray.INSTANCE.toBytes(arr);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable(bytes);
        PArrayDataType.positionAtArrayElement(ptr, 2, PVarchar.INSTANCE, PVarchar.INSTANCE.getByteSize());
        int offset = ptr.getOffset();
        int length = ptr.getLength();
        byte[] bs = ptr.get();
        byte[] res = new byte[length];
        System.arraycopy(bs, offset, res, 0, length);
        assertEquals("random", Bytes.toString(res));
    }
    @Test
    public void testForVarCharArrayForOddNumberWithIndex3() {
        String[] strArr = new String[5];
        strArr[0] = "abx";
        strArr[1] = "ereref";
        strArr[2] = "random";
        strArr[3] = "random12";
        strArr[4] = null;
        PhoenixArray arr = PArrayDataType.instantiatePhoenixArray(
                PVarchar.INSTANCE, strArr);
        byte[] bytes = PVarcharArray.INSTANCE.toBytes(arr);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable(bytes);
        PArrayDataType.positionAtArrayElement(ptr, 4, PVarchar.INSTANCE, PVarchar.INSTANCE.getByteSize());
        int offset = ptr.getOffset();
        int length = ptr.getLength();
        byte[] bs = ptr.get();
        byte[] res = new byte[length];
        System.arraycopy(bs, offset, res, 0, length);
        assertEquals("", Bytes.toString(res));
    }
    
    @Test
    public void testForVarCharArrayForOddNumberWithIndex4() {
        String[] strArr = new String[5];
        strArr[0] = "abx";
        strArr[1] = "ereref";
        strArr[2] = "random";
        strArr[3] = null;
        strArr[4] = null;
        PhoenixArray arr = PArrayDataType.instantiatePhoenixArray(
                PVarchar.INSTANCE, strArr);
        byte[] bytes = PVarcharArray.INSTANCE.toBytes(arr);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable(bytes);
        PArrayDataType.positionAtArrayElement(ptr, 3, PVarchar.INSTANCE, PVarchar.INSTANCE.getByteSize());
        int offset = ptr.getOffset();
        int length = ptr.getLength();
        byte[] bs = ptr.get();
        byte[] res = new byte[length];
        System.arraycopy(bs, offset, res, 0, length);
        assertEquals("", Bytes.toString(res));
    }
    
    @Test
    public void testForVarCharArrayForOddNumberWithIndex5() {
        String[] strArr = new String[5];
        strArr[0] = "abx";
        strArr[1] = "ereref";
        strArr[2] = "random";
        strArr[3] = null;
        strArr[4] = "random12";
        PhoenixArray arr = PArrayDataType.instantiatePhoenixArray(
                PVarchar.INSTANCE, strArr);
        byte[] bytes = PVarcharArray.INSTANCE.toBytes(arr);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable(bytes);
        PArrayDataType.positionAtArrayElement(ptr, 4, PVarchar.INSTANCE, PVarchar.INSTANCE.getByteSize());
        int offset = ptr.getOffset();
        int length = ptr.getLength();
        byte[] bs = ptr.get();
        byte[] res = new byte[length];
        System.arraycopy(bs, offset, res, 0, length);
        assertEquals("random12", Bytes.toString(res));
    }
    
    @Test
    public void testForVarCharArrayForOddNumberWithIndex6() {
        String[] strArr = new String[6];
        strArr[0] = "abx";
        strArr[1] = "ereref";
        strArr[2] = "random";
        strArr[3] = null;
        strArr[4] = "random12";
        strArr[5] = "random17";
        PhoenixArray arr = PArrayDataType.instantiatePhoenixArray(
                PVarchar.INSTANCE, strArr);
        byte[] bytes = PVarcharArray.INSTANCE.toBytes(arr);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable(bytes);
        PArrayDataType.positionAtArrayElement(ptr, 4, PVarchar.INSTANCE, PVarchar.INSTANCE.getByteSize());
        int offset = ptr.getOffset();
        int length = ptr.getLength();
        byte[] bs = ptr.get();
        byte[] res = new byte[length];
        System.arraycopy(bs, offset, res, 0, length);
        assertEquals("random12", Bytes.toString(res));
    }
    @Test
    public void testPositionSearchWithVarLengthArrayWithNullValue5() {
        String[] strArr = new String[5];
        strArr[0] = "abx";
        strArr[1] = "ereref";
        strArr[2] = "random";
        strArr[3] = null;
        strArr[4] = "ran";
        PhoenixArray arr = PArrayDataType.instantiatePhoenixArray(
                PVarchar.INSTANCE, strArr);
        byte[] bytes = PVarcharArray.INSTANCE.toBytes(arr);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable(bytes);
        PArrayDataType.positionAtArrayElement(ptr, 3, PVarchar.INSTANCE, PVarchar.INSTANCE.getByteSize());
        int offset = ptr.getOffset();
        int length = ptr.getLength();
        byte[] bs = ptr.get();
        byte[] res = new byte[length];
        System.arraycopy(bs, offset, res, 0, length);
        assertEquals("", Bytes.toString(res));
    }
    
    @Test
    public void testPositionSearchWithVarLengthArrayWithNullValueAtTheStart1() {
        String[] strArr = new String[5];
        strArr[0] = null;
        strArr[1] = "ereref";
        strArr[2] = "random";
        strArr[3] = null;
        strArr[4] = "ran";
        PhoenixArray arr = PArrayDataType.instantiatePhoenixArray(
                PVarchar.INSTANCE, strArr);
        byte[] bytes = PVarcharArray.INSTANCE.toBytes(arr);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable(bytes);
        PArrayDataType.positionAtArrayElement(ptr, 3, PVarchar.INSTANCE, PVarchar.INSTANCE.getByteSize());
        int offset = ptr.getOffset();
        int length = ptr.getLength();
        byte[] bs = ptr.get();
        byte[] res = new byte[length];
        System.arraycopy(bs, offset, res, 0, length);
        assertEquals("", Bytes.toString(res));
    }
    
    @Test
    public void testPositionSearchWithVarLengthArrayWithNullValueAtTheStart2() {
        String[] strArr = new String[5];
        strArr[0] = null;
        strArr[1] = "ereref";
        strArr[2] = "random";
        strArr[3] = null;
        strArr[4] = "ran";
        PhoenixArray arr = PArrayDataType.instantiatePhoenixArray(
                PVarchar.INSTANCE, strArr);
        byte[] bytes = PVarcharArray.INSTANCE.toBytes(arr);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable(bytes);
        PArrayDataType.positionAtArrayElement(ptr, 0, PVarchar.INSTANCE, PVarchar.INSTANCE.getByteSize());
        int offset = ptr.getOffset();
        int length = ptr.getLength();
        byte[] bs = ptr.get();
        byte[] res = new byte[length];
        System.arraycopy(bs, offset, res, 0, length);
        assertEquals("", Bytes.toString(res));
    }
    
    @Test
    public void testPositionSearchWithVarLengthArrayWithNullValueAtTheStart3() {
        String[] strArr = new String[5];
        strArr[0] = null;
        strArr[1] = "ereref";
        strArr[2] = "random";
        strArr[3] = null;
        strArr[4] = "ran";
        PhoenixArray arr = PArrayDataType.instantiatePhoenixArray(
                PVarchar.INSTANCE, strArr);
        byte[] bytes = PVarcharArray.INSTANCE.toBytes(arr);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable(bytes);
        PArrayDataType.positionAtArrayElement(ptr, 4, PVarchar.INSTANCE, PVarchar.INSTANCE.getByteSize());
        int offset = ptr.getOffset();
        int length = ptr.getLength();
        byte[] bs = ptr.get();
        byte[] res = new byte[length];
        System.arraycopy(bs, offset, res, 0, length);
        assertEquals("ran", Bytes.toString(res));
    }
    
    @Test
    public void testPositionSearchWithVarLengthArrayWithAllNulls() {
        String[] strArr = new String[5];
        strArr[0] = null;
        strArr[1] = null;
        strArr[2] = null;
        strArr[3] = null;
        strArr[4] = null;
        PhoenixArray arr = PArrayDataType.instantiatePhoenixArray(
                PVarchar.INSTANCE, strArr);
        byte[] bytes = PVarcharArray.INSTANCE.toBytes(arr);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable(bytes);
        PArrayDataType.positionAtArrayElement(ptr, 4, PVarchar.INSTANCE, PVarchar.INSTANCE.getByteSize());
        int offset = ptr.getOffset();
        int length = ptr.getLength();
        byte[] bs = ptr.get();
        byte[] res = new byte[length];
        System.arraycopy(bs, offset, res, 0, length);
        assertEquals("", Bytes.toString(res));
    }

	@Test
	public void testForVarCharArrayForOneElementArrayWithIndex() {
		String[] strArr = new String[1];
		strArr[0] = "abx";
		PhoenixArray arr = PArrayDataType.instantiatePhoenixArray(
				PVarchar.INSTANCE, strArr);
		byte[] bytes = PVarcharArray.INSTANCE.toBytes(arr);
		ImmutableBytesWritable ptr = new ImmutableBytesWritable(bytes);
		PArrayDataType.positionAtArrayElement(ptr, 0, PVarchar.INSTANCE, PVarchar.INSTANCE.getByteSize());
		int offset = ptr.getOffset();
		int length = ptr.getLength();
		byte[] bs = ptr.get();
		byte[] res = new byte[length];
		System.arraycopy(bs, offset, res, 0, length);
		assertEquals("abx", Bytes.toString(res));
	}

	@Test
	public void testForVarCharArrayForWithTwoelementsElementArrayWithIndex() {
		String[] strArr = new String[2];
		strArr[0] = "abx";
		strArr[1] = "ereref";
		PhoenixArray arr = PArrayDataType.instantiatePhoenixArray(
				PVarchar.INSTANCE, strArr);
		byte[] bytes = PVarcharArray.INSTANCE.toBytes(arr);
		ImmutableBytesWritable ptr = new ImmutableBytesWritable(bytes);
		PArrayDataType.positionAtArrayElement(ptr, 1, PVarchar.INSTANCE, PVarchar.INSTANCE.getByteSize());
		int offset = ptr.getOffset();
		int length = ptr.getLength();
		byte[] bs = ptr.get();
		byte[] res = new byte[length];
		System.arraycopy(bs, offset, res, 0, length);
		assertEquals("ereref", Bytes.toString(res));
	}

	@Test
	public void testLongArrayWithIndex() {
		Long[] longArr = new Long[4];
		longArr[0] = 1l;
		longArr[1] = 2l;
		longArr[2] = 4l;
		longArr[3] = 5l;
		PhoenixArray arr = PArrayDataType.instantiatePhoenixArray(
				PLong.INSTANCE, longArr);
		PLongArray.INSTANCE.toObject(arr, PLongArray.INSTANCE);
		byte[] bytes = PLongArray.INSTANCE.toBytes(arr);
		ImmutableBytesWritable ptr = new ImmutableBytesWritable(bytes);
		PArrayDataType.positionAtArrayElement(ptr, 2, PLong.INSTANCE, PLong.INSTANCE.getByteSize());
		int offset = ptr.getOffset();
		int length = ptr.getLength();
		byte[] bs = ptr.get();
		byte[] res = new byte[length];
		System.arraycopy(bs, offset, res, 0, length);
		long result = (Long) PLong.INSTANCE.toObject(res);
		assertEquals(4l, result);
	}
	
	@Test
	public void testGetArrayLengthForFixedLengthArray() {
		Long[] longArr = new Long[4];
		longArr[0] = 1l;
		longArr[1] = 2l;
		longArr[2] = 4l;
		longArr[3] = 5l;
		PhoenixArray arr = PArrayDataType.instantiatePhoenixArray(
				PLong.INSTANCE, longArr);
		PLongArray.INSTANCE.toObject(arr, PLongArray.INSTANCE);
		byte[] bytes = PLongArray.INSTANCE.toBytes(arr);
		ImmutableBytesWritable ptr = new ImmutableBytesWritable(bytes);
		int length = PArrayDataType.getArrayLength(ptr, PLong.INSTANCE, null);
		assertEquals(4, length);
	}

	@Test
	public void testForVarcharArrayBiggerArraysNumber() {
		String[] strArr = new String[101];
		for (int i = 0; i <= 100; i++) {
			strArr[i] = "abc" + i;
		}
		PhoenixArray arr = PArrayDataType.instantiatePhoenixArray(
				PVarchar.INSTANCE, strArr);
		byte[] bytes = PVarcharArray.INSTANCE.toBytes(arr);
		PhoenixArray resultArr = (PhoenixArray) PVarcharArray.INSTANCE
				.toObject(bytes, 0, bytes.length);
		assertEquals(arr, resultArr);
	}

	@Test
	public void testForTinyIntArray() {
		Byte[] byteArr = new Byte[2];
		byteArr[0] = 1;
		byteArr[1] = 2;
		PhoenixArray arr = PArrayDataType.instantiatePhoenixArray(
				PTinyint.INSTANCE, byteArr);
		PTinyintArray.INSTANCE.toObject(arr, PTinyintArray.INSTANCE);
		byte[] bytes = PTinyintArray.INSTANCE.toBytes(arr);
		PhoenixArray resultArr = (PhoenixArray) PTinyintArray.INSTANCE
				.toObject(bytes, 0, bytes.length);
		assertEquals(arr, resultArr);
	}

	@Test
	public void testForFloatArray() {
		Float[] floatArr = new Float[2];
		floatArr[0] = 1.06f;
		floatArr[1] = 2.89f;
		PhoenixArray arr = PArrayDataType.instantiatePhoenixArray(
				PFloat.INSTANCE, floatArr);
		PFloatArray.INSTANCE.toObject(arr, PFloatArray.INSTANCE);
		byte[] bytes = PFloatArray.INSTANCE.toBytes(arr);
		PhoenixArray resultArr = (PhoenixArray) PFloatArray.INSTANCE.toObject(
				bytes, 0, bytes.length);
		assertEquals(arr, resultArr);
	}

	@Test
	public void testForDoubleArray() {
		Double[] doubleArr = new Double[2];
		doubleArr[0] = 1.06;
		doubleArr[1] = 2.89;
		PhoenixArray arr = PArrayDataType.instantiatePhoenixArray(
				PDouble.INSTANCE, doubleArr);
		PDoubleArray.INSTANCE.toObject(arr, PDoubleArray.INSTANCE);
		byte[] bytes = PDoubleArray.INSTANCE.toBytes(arr);
		PhoenixArray resultArr = (PhoenixArray) PDoubleArray.INSTANCE
				.toObject(bytes, 0, bytes.length);
		assertEquals(arr, resultArr);
	}

	@Test
	public void testForDecimalArray() {
		BigDecimal[] bigDecimalArr = new BigDecimal[2];
		bigDecimalArr[0] = new BigDecimal(89997);
		bigDecimalArr[1] = new BigDecimal(8999.995f);
		PhoenixArray arr = PArrayDataType.instantiatePhoenixArray(
				PDecimal.INSTANCE, bigDecimalArr);
		PDecimalArray.INSTANCE.toObject(arr, PDecimalArray.INSTANCE);
		byte[] bytes = PDecimalArray.INSTANCE.toBytes(arr);
		PhoenixArray resultArr = (PhoenixArray) PDecimalArray.INSTANCE
				.toObject(bytes, 0, bytes.length);
		assertEquals(arr, resultArr);
	}

	@Test
	public void testForTimeStampArray() {
		Timestamp[] timeStampArr = new Timestamp[2];
		timeStampArr[0] = new Timestamp(System.currentTimeMillis());
		timeStampArr[1] = new Timestamp(900000l);
		PhoenixArray arr = PArrayDataType.instantiatePhoenixArray(
				PTimestamp.INSTANCE, timeStampArr);
		PTimestampArray.INSTANCE.toObject(arr, PTimestampArray.INSTANCE);
		byte[] bytes = PTimestampArray.INSTANCE.toBytes(arr);
		PhoenixArray resultArr = (PhoenixArray) PTimestampArray.INSTANCE
				.toObject(bytes, 0, bytes.length);
		assertEquals(arr, resultArr);
	}

	@Test
	public void testForUnSignedTimeStampArray() {
		Timestamp[] timeStampArr = new Timestamp[2];
		timeStampArr[0] = new Timestamp(System.currentTimeMillis());
		timeStampArr[1] = new Timestamp(900000l);
		PhoenixArray arr = PArrayDataType.instantiatePhoenixArray(
				PUnsignedTimestamp.INSTANCE, timeStampArr);
		PUnsignedTimestampArray.INSTANCE.toObject(arr,
				PUnsignedTimestampArray.INSTANCE);
		byte[] bytes = PUnsignedTimestampArray.INSTANCE.toBytes(arr);
		PhoenixArray resultArr = (PhoenixArray) PUnsignedTimestampArray.INSTANCE
				.toObject(bytes, 0, bytes.length);
		assertEquals(arr, resultArr);
	}

	@Test
	public void testForTimeArray() {
		Time[] timeArr = new Time[2];
		timeArr[0] = new Time(System.currentTimeMillis());
		timeArr[1] = new Time(900000l);
		PhoenixArray arr = PArrayDataType.instantiatePhoenixArray(
        PTime.INSTANCE, timeArr);
		PTimeArray.INSTANCE.toObject(arr, PTimeArray.INSTANCE);
		byte[] bytes = PTimeArray.INSTANCE.toBytes(arr);
		PhoenixArray resultArr = (PhoenixArray) PTimeArray.INSTANCE.toObject(
				bytes, 0, bytes.length);
		assertEquals(arr, resultArr);
	}

	@Test
	public void testForUnsignedTimeArray() {
		Time[] timeArr = new Time[2];
		timeArr[0] = new Time(System.currentTimeMillis());
		timeArr[1] = new Time(900000l);
		PhoenixArray arr = PArrayDataType.instantiatePhoenixArray(
				PUnsignedTime.INSTANCE, timeArr);
		PUnsignedTimeArray.INSTANCE.toObject(arr,
				PUnsignedTimeArray.INSTANCE);
		byte[] bytes = PUnsignedTimeArray.INSTANCE.toBytes(arr);
		PhoenixArray resultArr = (PhoenixArray) PUnsignedTimeArray.INSTANCE
				.toObject(bytes, 0, bytes.length);
		assertEquals(arr, resultArr);
	}

	@Test
	public void testForDateArray() {
		Date[] dateArr = new Date[2];
		dateArr[0] = new Date(System.currentTimeMillis());
		dateArr[1] = new Date(System.currentTimeMillis()
				+ System.currentTimeMillis());
		PhoenixArray arr = PArrayDataType.instantiatePhoenixArray(
        PDate.INSTANCE, dateArr);
		PDateArray.INSTANCE.toObject(arr, PDateArray.INSTANCE);
		byte[] bytes = PDateArray.INSTANCE.toBytes(arr);
		PhoenixArray resultArr = (PhoenixArray) PDateArray.INSTANCE.toObject(
				bytes, 0, bytes.length);
		assertEquals(arr, resultArr);
	}

	@Test
	public void testForUnSignedDateArray() {
		Date[] dateArr = new Date[2];
		dateArr[0] = new Date(System.currentTimeMillis());
		dateArr[1] = new Date(System.currentTimeMillis()
				+ System.currentTimeMillis());
		PhoenixArray arr = PArrayDataType.instantiatePhoenixArray(
				PUnsignedDate.INSTANCE, dateArr);
		PUnsignedDateArray.INSTANCE.toObject(arr,
				PUnsignedDateArray.INSTANCE);
		byte[] bytes = PUnsignedDateArray.INSTANCE.toBytes(arr);
		PhoenixArray resultArr = (PhoenixArray) PUnsignedDateArray.INSTANCE
				.toObject(bytes, 0, bytes.length);
		assertEquals(arr, resultArr);
	}

	@Test
	public void testForUnSignedLongArray() {
		Long[] longArr = new Long[2];
		longArr[0] = 1l;
		longArr[1] = 2l;
		PhoenixArray arr = PArrayDataType.instantiatePhoenixArray(
				PUnsignedLong.INSTANCE, longArr);
		PUnsignedLongArray.INSTANCE.toObject(arr,
				PUnsignedLongArray.INSTANCE);
		byte[] bytes = PUnsignedLongArray.INSTANCE.toBytes(arr);
		PhoenixArray resultArr = (PhoenixArray) PUnsignedLongArray.INSTANCE
				.toObject(bytes, 0, bytes.length);
		assertEquals(arr, resultArr);
	}

	@Test
	public void testForUnSignedIntArray() {
		Integer[] intArr = new Integer[2];
		intArr[0] = 1;
		intArr[1] = 2;
		PhoenixArray arr = PArrayDataType.instantiatePhoenixArray(
				PUnsignedInt.INSTANCE, intArr);
		PUnsignedIntArray.INSTANCE
				.toObject(arr, PUnsignedIntArray.INSTANCE);
		byte[] bytes = PUnsignedIntArray.INSTANCE.toBytes(arr);
		PhoenixArray resultArr = (PhoenixArray) PUnsignedIntArray.INSTANCE
				.toObject(bytes, 0, bytes.length);
		assertEquals(arr, resultArr);
	}

	@Test
	public void testForUnSignedSmallIntArray() {
		Short[] shortArr = new Short[2];
		shortArr[0] = 1;
		shortArr[1] = 2;
		PhoenixArray arr = PArrayDataType.instantiatePhoenixArray(
				PUnsignedSmallint.INSTANCE, shortArr);
		PUnsignedSmallintArray.INSTANCE.toObject(arr,
				PUnsignedSmallintArray.INSTANCE);
		byte[] bytes = PUnsignedSmallintArray.INSTANCE.toBytes(arr);
		PhoenixArray resultArr = (PhoenixArray) PUnsignedSmallintArray.INSTANCE
				.toObject(bytes, 0, bytes.length);
		assertEquals(arr, resultArr);
	}

	@Test
	public void testForUnSignedTinyIntArray() {
		Byte[] byteArr = new Byte[2];
		byteArr[0] = 1;
		byteArr[1] = 2;
		PhoenixArray arr = PArrayDataType.instantiatePhoenixArray(
				PUnsignedTinyint.INSTANCE, byteArr);
		PUnsignedTinyintArray.INSTANCE.toObject(arr,
				PUnsignedTinyintArray.INSTANCE);
		byte[] bytes = PUnsignedTinyintArray.INSTANCE.toBytes(arr);
		PhoenixArray resultArr = (PhoenixArray) PUnsignedTinyintArray.INSTANCE
				.toObject(bytes, 0, bytes.length);
		assertEquals(arr, resultArr);
	}

	@Test
	public void testForUnSignedFloatArray() {
		Float[] floatArr = new Float[2];
		floatArr[0] = 1.9993f;
		floatArr[1] = 2.786f;
		PhoenixArray arr = PArrayDataType.instantiatePhoenixArray(
				PUnsignedFloat.INSTANCE, floatArr);
		PUnsignedFloatArray.INSTANCE.toObject(arr,
				PUnsignedFloatArray.INSTANCE);
		byte[] bytes = PUnsignedFloatArray.INSTANCE.toBytes(arr);
		PhoenixArray resultArr = (PhoenixArray) PUnsignedFloatArray.INSTANCE
				.toObject(bytes, 0, bytes.length);
		assertEquals(arr, resultArr);
	}

	@Test
	public void testForUnSignedDoubleArray() {
		Double[] doubleArr = new Double[2];
		doubleArr[0] = 1.9993;
		doubleArr[1] = 2.786;
		PhoenixArray arr = PArrayDataType.instantiatePhoenixArray(
				PUnsignedDouble.INSTANCE, doubleArr);
		PUnsignedDoubleArray.INSTANCE.toObject(arr,
				PUnsignedDoubleArray.INSTANCE);
		byte[] bytes = PUnsignedDoubleArray.INSTANCE.toBytes(arr);
		PhoenixArray resultArr = (PhoenixArray) PUnsignedDoubleArray.INSTANCE
				.toObject(bytes, 0, bytes.length);
		assertEquals(arr, resultArr);
	}

    @Test
    public void testForArrayComparisionsForFixedWidth() {
        Double[] doubleArr = new Double[2];
        doubleArr[0] = 1.9993;
        doubleArr[1] = 2.786;
        PhoenixArray arr = PArrayDataType.instantiatePhoenixArray(PUnsignedDouble.INSTANCE, doubleArr);
        PUnsignedDoubleArray.INSTANCE.toObject(arr, PUnsignedDoubleArray.INSTANCE);
        byte[] bytes1 = PUnsignedDoubleArray.INSTANCE.toBytes(arr);

        doubleArr = new Double[2];
        doubleArr[0] = 1.9993;
        doubleArr[1] = 2.786;
        arr = PArrayDataType.instantiatePhoenixArray(PUnsignedDouble.INSTANCE, doubleArr);
        PUnsignedDoubleArray.INSTANCE.toObject(arr, PUnsignedDoubleArray.INSTANCE);
        byte[] bytes2 = PUnsignedDoubleArray.INSTANCE.toBytes(arr);
        assertTrue(Bytes.equals(bytes1, bytes2));
    }

    @Test
    public void testForArrayComparisionsWithInEqualityForFixedWidth() {
        Double[] doubleArr = new Double[2];
        doubleArr[0] = 1.9993;
        doubleArr[1] = 2.786;
        PhoenixArray arr = PArrayDataType.instantiatePhoenixArray(PUnsignedDouble.INSTANCE, doubleArr);
        PUnsignedDoubleArray.INSTANCE.toObject(arr, PUnsignedDoubleArray.INSTANCE);
        byte[] bytes1 = PUnsignedDoubleArray.INSTANCE.toBytes(arr);

        doubleArr = new Double[3];
        doubleArr[0] = 1.9993;
        doubleArr[1] = 2.786;
        doubleArr[2] = 6.3;
        arr = PArrayDataType.instantiatePhoenixArray(PUnsignedDouble.INSTANCE, doubleArr);
        PUnsignedDoubleArray.INSTANCE.toObject(arr, PUnsignedDoubleArray.INSTANCE);
        byte[] bytes2 = PUnsignedDoubleArray.INSTANCE.toBytes(arr);
        assertTrue(Bytes.compareTo(bytes1, bytes2) < 0);
    }

    @Test
    public void testForArrayComparisonsForVarWidthArrays() {
        String[] strArr = new String[5];
        strArr[0] = "abc";
        strArr[1] = "ereref";
        strArr[2] = "random";
        strArr[3] = "random1";
        strArr[4] = "ran";
        PhoenixArray arr = PArrayDataType.instantiatePhoenixArray(PVarchar.INSTANCE, strArr);
        byte[] bytes1 = PVarcharArray.INSTANCE.toBytes(arr);

        strArr = new String[5];
        strArr[0] = "abc";
        strArr[1] = "ereref";
        strArr[2] = "random";
        strArr[3] = "random1";
        strArr[4] = "ran";
        arr = PArrayDataType.instantiatePhoenixArray(PVarchar.INSTANCE, strArr);
        byte[] bytes2 = PVarcharArray.INSTANCE.toBytes(arr);
        assertTrue(Bytes.equals(bytes1, bytes2));
    }

    @Test
    public void testForArrayComparisonsInEqualityForVarWidthArrays() {
        String[] strArr = new String[5];
        strArr[0] = "abc";
        strArr[1] = "ereref";
        strArr[2] = "random";
        strArr[3] = "random1";
        strArr[4] = "ran";
        PhoenixArray arr = PArrayDataType.instantiatePhoenixArray(PVarchar.INSTANCE, strArr);
        byte[] bytes1 = PVarcharArray.INSTANCE.toBytes(arr);

        strArr = new String[5];
        strArr[0] = "abc";
        strArr[1] = "ereref";
        strArr[2] = "random";
        strArr[3] = "random1";
        arr = PArrayDataType.instantiatePhoenixArray(PVarchar.INSTANCE, strArr);
        byte[] bytes2 = PVarcharArray.INSTANCE.toBytes(arr);
        assertTrue(Bytes.compareTo(bytes1, bytes2) > 0);
    }

    @Test
    public void testForArrayComparsionInEqualityWithNullsRepeatingInTheMiddle() {
        String[] strArr = new String[6];
        strArr[0] = null;
        strArr[1] = "ereref";
        strArr[2] = "random";
        strArr[3] = null;
        strArr[4] = "ran";
        strArr[5] = "ran";
        PhoenixArray arr = PArrayDataType.instantiatePhoenixArray(PVarchar.INSTANCE, strArr);
        byte[] bytes1 = PVarcharArray.INSTANCE.toBytes(arr);

        strArr = new String[6];
        strArr[0] = null;
        strArr[1] = "ereref";
        strArr[2] = "random";
        strArr[3] = null;
        strArr[4] = null;
        strArr[5] = "ran";
        arr = PArrayDataType.instantiatePhoenixArray(PVarchar.INSTANCE, strArr);
        byte[] bytes2 = PVarcharArray.INSTANCE.toBytes(arr);
        assertTrue(Bytes.compareTo(bytes1, bytes2) > 0);
    }
    
    @Test
    public void testVarCharArrayWithGreatherThan255NullsInMiddle() {
        String strArr[] = new String[300];
        strArr[0] = "abc";
        strArr[1] = "bcd";
        strArr[2] = null;
        strArr[3] = null;
        strArr[4] = "bcd";
        for(int i = 5; i < strArr.length - 2; i++) {
            strArr[i] = null;
        }
        strArr[strArr.length - 1] = "abc";
        PhoenixArray arr = PArrayDataType.instantiatePhoenixArray(
                PVarchar.INSTANCE, strArr);
        byte[] bytes = PVarcharArray.INSTANCE.toBytes(arr);
        PhoenixArray resultArr = (PhoenixArray) PVarcharArray.INSTANCE
                .toObject(bytes, 0, bytes.length);
        assertEquals(arr, resultArr);
    }
    
    @Test
    public void testVarCharArrayComparisonWithGreaterThan255NullsinMiddle() {
        String strArr[] = new String[240];
        strArr[0] = "abc";
        strArr[1] = "bcd";
        strArr[2] = null;
        strArr[3] = null;
        strArr[4] = "bcd";
        strArr[strArr.length - 1] = "abc";
        PhoenixArray arr = PArrayDataType.instantiatePhoenixArray(
                PVarchar.INSTANCE, strArr);
        byte[] bytes1 = PVarcharArray.INSTANCE.toBytes(arr);
        
        strArr = new String[16];
        strArr[0] = "abc";
        strArr[1] = "bcd";
        strArr[2] = null;
        strArr[3] = null;
        strArr[4] = "bcd";
        strArr[strArr.length - 1] = "abc";
        arr = PArrayDataType.instantiatePhoenixArray(
                PVarchar.INSTANCE, strArr);
        byte[] bytes2 = PVarcharArray.INSTANCE.toBytes(arr);
        assertTrue(Bytes.compareTo(bytes1, bytes2) < 0);
    }
    
    @Test
    public void testVarCharArrayComparisonWithGreaterThan255NullsinMiddle1() {
        String strArr[] = new String[500];
        strArr[0] = "abc";
        strArr[1] = "bcd";
        strArr[2] = null;
        strArr[3] = null;
        strArr[4] = "bcd";
        strArr[strArr.length - 1] = "abc";
        PhoenixArray arr = PArrayDataType.instantiatePhoenixArray(
                PVarchar.INSTANCE, strArr);
        byte[] bytes1 = PVarcharArray.INSTANCE.toBytes(arr);
        
        strArr = new String[500];
        strArr[0] = "abc";
        strArr[1] = "bcd";
        strArr[2] = null;
        strArr[3] = null;
        strArr[4] = "bcd";
        strArr[strArr.length - 1] = "abc";
        arr = PArrayDataType.instantiatePhoenixArray(
                PVarchar.INSTANCE, strArr);
        byte[] bytes2 = PVarcharArray.INSTANCE.toBytes(arr);
        assertTrue(Bytes.compareTo(bytes1, bytes2) == 0);
    }
    
}
