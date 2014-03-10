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
import org.junit.Ignore;
import org.junit.Test;

public class PDataTypeForArraysTest {
	@Test
	public void testForIntegerArray() {
		Integer[] intArr = new Integer[2];
		intArr[0] = 1;
		intArr[1] = 2;
		PhoenixArray arr = PArrayDataType.instantiatePhoenixArray(
				PDataType.INTEGER, intArr);
		PDataType.INTEGER_ARRAY.toObject(arr, PDataType.INTEGER_ARRAY);
		byte[] bytes = PDataType.INTEGER_ARRAY.toBytes(arr);
		PhoenixArray resultArr = (PhoenixArray) PDataType.INTEGER_ARRAY
				.toObject(bytes, 0, bytes.length);
		assertEquals(arr, resultArr);
	}

	@Test
	public void testForBooleanArray() {
		Boolean[] boolArr = new Boolean[2];
		boolArr[0] = true;
		boolArr[1] = false;
		PhoenixArray arr = PArrayDataType.instantiatePhoenixArray(
				PDataType.BOOLEAN, boolArr);
		PDataType.BOOLEAN_ARRAY.toObject(arr, PDataType.BOOLEAN_ARRAY);
		byte[] bytes = PDataType.BOOLEAN_ARRAY.toBytes(arr);
		PhoenixArray resultArr = (PhoenixArray) PDataType.BOOLEAN_ARRAY
				.toObject(bytes, 0, bytes.length);
		assertEquals(arr, resultArr);
	}

	@Test
	public void testForVarCharArray() {
		String[] strArr = new String[2];
		strArr[0] = "abc";
		strArr[1] = "klmnop";
		PhoenixArray arr = PArrayDataType.instantiatePhoenixArray(
				PDataType.VARCHAR, strArr);
		byte[] bytes = PDataType.VARCHAR_ARRAY.toBytes(arr);
		PhoenixArray resultArr = (PhoenixArray) PDataType.VARCHAR_ARRAY
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
                PDataType.VARCHAR, strArr);
        byte[] bytes = PDataType.VARCHAR_ARRAY.toBytes(arr);
        PhoenixArray resultArr = (PhoenixArray) PDataType.VARCHAR_ARRAY
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
        PhoenixArray arr = PArrayDataType.instantiatePhoenixArray(PDataType.VARCHAR, strArr);
        byte[] bytes = PDataType.VARCHAR_ARRAY.toBytes(arr);
        PhoenixArray resultArr = (PhoenixArray)PDataType.VARCHAR_ARRAY.toObject(bytes, 0, bytes.length);
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
        PhoenixArray arr = PArrayDataType.instantiatePhoenixArray(PDataType.VARCHAR, strArr);
        byte[] bytes = PDataType.VARCHAR_ARRAY.toBytes(arr);
        PhoenixArray resultArr = (PhoenixArray)PDataType.VARCHAR_ARRAY.toObject(bytes, 0, bytes.length);
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
        PhoenixArray arr = PArrayDataType.instantiatePhoenixArray(PDataType.VARCHAR, strArr);
        byte[] bytes = PDataType.VARCHAR_ARRAY.toBytes(arr);
        PhoenixArray resultArr = (PhoenixArray)PDataType.VARCHAR_ARRAY.toObject(bytes, 0, bytes.length);
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
        PhoenixArray arr = PArrayDataType.instantiatePhoenixArray(PDataType.VARCHAR, strArr);
        byte[] bytes = PDataType.VARCHAR_ARRAY.toBytes(arr);
        PhoenixArray resultArr = (PhoenixArray)PDataType.VARCHAR_ARRAY.toObject(bytes, 0, bytes.length);
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
        PhoenixArray arr = PArrayDataType.instantiatePhoenixArray(PDataType.VARCHAR, strArr);
        byte[] bytes = PDataType.VARCHAR_ARRAY.toBytes(arr);
        PhoenixArray resultArr = (PhoenixArray)PDataType.VARCHAR_ARRAY.toObject(bytes, 0, bytes.length);
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
        PhoenixArray arr = PArrayDataType.instantiatePhoenixArray(PDataType.VARCHAR, strArr);
        byte[] bytes = PDataType.VARCHAR_ARRAY.toBytes(arr);
        PhoenixArray resultArr = (PhoenixArray)PDataType.VARCHAR_ARRAY.toObject(bytes, 0, bytes.length);
        assertEquals(arr, resultArr);
    }

	@Test
	public void testForCharArray() {
		String[] strArr = new String[2];
		strArr[0] = "a";
		strArr[1] = "d";
		PhoenixArray arr = PArrayDataType.instantiatePhoenixArray(
				PDataType.CHAR, strArr);
		byte[] bytes = PDataType.CHAR_ARRAY.toBytes(arr);
		PhoenixArray resultArr = (PhoenixArray) PDataType.CHAR_ARRAY.toObject(
				bytes, 0, bytes.length, PDataType.CHAR_ARRAY, null, 1, null);
		assertEquals(arr, resultArr);
	}

	@Test
	public void testForLongArray() {
		Long[] longArr = new Long[2];
		longArr[0] = 1l;
		longArr[1] = 2l;
		PhoenixArray arr = PArrayDataType.instantiatePhoenixArray(
				PDataType.LONG, longArr);
		PDataType.LONG_ARRAY.toObject(arr, PDataType.LONG_ARRAY);
		byte[] bytes = PDataType.LONG_ARRAY.toBytes(arr);
		PhoenixArray resultArr = (PhoenixArray) PDataType.LONG_ARRAY.toObject(
				bytes, 0, bytes.length);
		assertEquals(arr, resultArr);
	}

	@Test
	public void testForSmallIntArray() {
		Short[] shortArr = new Short[2];
		shortArr[0] = 1;
		shortArr[1] = 2;
		PhoenixArray arr = PArrayDataType.instantiatePhoenixArray(
				PDataType.SMALLINT, shortArr);
		PDataType.SMALLINT_ARRAY.toObject(arr, PDataType.SMALLINT_ARRAY);
		byte[] bytes = PDataType.SMALLINT_ARRAY.toBytes(arr);
		PhoenixArray resultArr = (PhoenixArray) PDataType.SMALLINT_ARRAY
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
				PDataType.VARCHAR, strArr);
		byte[] bytes = PDataType.VARCHAR_ARRAY.toBytes(arr);
		PhoenixArray resultArr = (PhoenixArray) PDataType.VARCHAR_ARRAY
				.toObject(bytes, 0, bytes.length);
		assertEquals(arr, resultArr);
	}

    @Test
    public void testForVarCharArrayOneElement() {
        String[] strArr = new String[1];
        strArr[0] = "ereref";
        PhoenixArray arr = PArrayDataType.instantiatePhoenixArray(
                PDataType.VARCHAR, strArr);
        byte[] bytes = PDataType.VARCHAR_ARRAY.toBytes(arr);
        PhoenixArray resultArr = (PhoenixArray) PDataType.VARCHAR_ARRAY
                .toObject(bytes, 0, bytes.length);
        assertEquals(arr, resultArr);
    }

    @Test
    public void testForVarcharArrayWith1ElementInLargerBuffer() {
        String[] strArr = new String[1];
        strArr[0] = "abx";
        PhoenixArray arr = PArrayDataType.instantiatePhoenixArray(PDataType.VARCHAR, strArr);
        byte[] bytes = PDataType.VARCHAR_ARRAY.toBytes(arr);
        byte[] moreBytes = new byte[bytes.length + 20];
        // Generate some garbage
        for (int i = 0; i < moreBytes.length; i++) {
            moreBytes[i] = (byte)-i;
        }
        System.arraycopy(bytes, 0, moreBytes, 10, bytes.length);
        PhoenixArray resultArr = (PhoenixArray)PDataType.VARCHAR_ARRAY.toObject(moreBytes, 10, bytes.length);
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
				PDataType.VARCHAR, strArr);
		byte[] bytes = PDataType.VARCHAR_ARRAY.toBytes(arr);
		ImmutableBytesWritable ptr = new ImmutableBytesWritable(bytes);
        PArrayDataType.positionAtArrayElement(ptr, 4, PDataType.VARCHAR, PDataType.VARCHAR.getByteSize());
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
                PDataType.VARCHAR, strArr);
        byte[] bytes = PDataType.VARCHAR_ARRAY.toBytes(arr);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable(bytes);
        PArrayDataType.positionAtArrayElement(ptr, 0, PDataType.VARCHAR, PDataType.VARCHAR.getByteSize());
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
                PDataType.VARCHAR, strArr);
        byte[] bytes = PDataType.VARCHAR_ARRAY.toBytes(arr);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable(bytes);
        PArrayDataType.positionAtArrayElement(ptr, 3, PDataType.VARCHAR, PDataType.VARCHAR.getByteSize());
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
				PDataType.VARCHAR, strArr);
		byte[] bytes = PDataType.VARCHAR_ARRAY.toBytes(arr);
		ImmutableBytesWritable ptr = new ImmutableBytesWritable(bytes);
		int result = PArrayDataType.getArrayLength(ptr, PDataType.VARCHAR, null);
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
				PDataType.VARCHAR, strArr);
		byte[] bytes = PDataType.VARCHAR_ARRAY.toBytes(arr);
		ImmutableBytesWritable ptr = new ImmutableBytesWritable(bytes);
		PArrayDataType.positionAtArrayElement(ptr, 3, PDataType.VARCHAR, PDataType.VARCHAR.getByteSize());
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
                PDataType.VARCHAR, strArr);
        byte[] bytes = PDataType.VARCHAR_ARRAY.toBytes(arr);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable(bytes);
        PArrayDataType.positionAtArrayElement(ptr, 2, PDataType.VARCHAR, PDataType.VARCHAR.getByteSize());
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
                PDataType.VARCHAR, strArr);
        byte[] bytes = PDataType.VARCHAR_ARRAY.toBytes(arr);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable(bytes);
        PArrayDataType.positionAtArrayElement(ptr, 2, PDataType.VARCHAR, PDataType.VARCHAR.getByteSize());
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
                PDataType.VARCHAR, strArr);
        byte[] bytes = PDataType.VARCHAR_ARRAY.toBytes(arr);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable(bytes);
        PArrayDataType.positionAtArrayElement(ptr, 4, PDataType.VARCHAR, PDataType.VARCHAR.getByteSize());
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
                PDataType.VARCHAR, strArr);
        byte[] bytes = PDataType.VARCHAR_ARRAY.toBytes(arr);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable(bytes);
        PArrayDataType.positionAtArrayElement(ptr, 3, PDataType.VARCHAR, PDataType.VARCHAR.getByteSize());
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
                PDataType.VARCHAR, strArr);
        byte[] bytes = PDataType.VARCHAR_ARRAY.toBytes(arr);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable(bytes);
        PArrayDataType.positionAtArrayElement(ptr, 4, PDataType.VARCHAR, PDataType.VARCHAR.getByteSize());
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
                PDataType.VARCHAR, strArr);
        byte[] bytes = PDataType.VARCHAR_ARRAY.toBytes(arr);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable(bytes);
        PArrayDataType.positionAtArrayElement(ptr, 4, PDataType.VARCHAR, PDataType.VARCHAR.getByteSize());
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
                PDataType.VARCHAR, strArr);
        byte[] bytes = PDataType.VARCHAR_ARRAY.toBytes(arr);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable(bytes);
        PArrayDataType.positionAtArrayElement(ptr, 3, PDataType.VARCHAR, PDataType.VARCHAR.getByteSize());
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
                PDataType.VARCHAR, strArr);
        byte[] bytes = PDataType.VARCHAR_ARRAY.toBytes(arr);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable(bytes);
        PArrayDataType.positionAtArrayElement(ptr, 3, PDataType.VARCHAR, PDataType.VARCHAR.getByteSize());
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
                PDataType.VARCHAR, strArr);
        byte[] bytes = PDataType.VARCHAR_ARRAY.toBytes(arr);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable(bytes);
        PArrayDataType.positionAtArrayElement(ptr, 0, PDataType.VARCHAR, PDataType.VARCHAR.getByteSize());
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
                PDataType.VARCHAR, strArr);
        byte[] bytes = PDataType.VARCHAR_ARRAY.toBytes(arr);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable(bytes);
        PArrayDataType.positionAtArrayElement(ptr, 4, PDataType.VARCHAR, PDataType.VARCHAR.getByteSize());
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
                PDataType.VARCHAR, strArr);
        byte[] bytes = PDataType.VARCHAR_ARRAY.toBytes(arr);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable(bytes);
        PArrayDataType.positionAtArrayElement(ptr, 4, PDataType.VARCHAR, PDataType.VARCHAR.getByteSize());
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
				PDataType.VARCHAR, strArr);
		byte[] bytes = PDataType.VARCHAR_ARRAY.toBytes(arr);
		ImmutableBytesWritable ptr = new ImmutableBytesWritable(bytes);
		PArrayDataType.positionAtArrayElement(ptr, 0, PDataType.VARCHAR, PDataType.VARCHAR.getByteSize());
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
				PDataType.VARCHAR, strArr);
		byte[] bytes = PDataType.VARCHAR_ARRAY.toBytes(arr);
		ImmutableBytesWritable ptr = new ImmutableBytesWritable(bytes);
		PArrayDataType.positionAtArrayElement(ptr, 1, PDataType.VARCHAR, PDataType.VARCHAR.getByteSize());
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
				PDataType.LONG, longArr);
		PDataType.LONG_ARRAY.toObject(arr, PDataType.LONG_ARRAY);
		byte[] bytes = PDataType.LONG_ARRAY.toBytes(arr);
		ImmutableBytesWritable ptr = new ImmutableBytesWritable(bytes);
		PArrayDataType.positionAtArrayElement(ptr, 2, PDataType.LONG, PDataType.LONG.getByteSize());
		int offset = ptr.getOffset();
		int length = ptr.getLength();
		byte[] bs = ptr.get();
		byte[] res = new byte[length];
		System.arraycopy(bs, offset, res, 0, length);
		long result = (Long) PDataType.LONG.toObject(res);
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
				PDataType.LONG, longArr);
		PDataType.LONG_ARRAY.toObject(arr, PDataType.LONG_ARRAY);
		byte[] bytes = PDataType.LONG_ARRAY.toBytes(arr);
		ImmutableBytesWritable ptr = new ImmutableBytesWritable(bytes);
		int length = PArrayDataType.getArrayLength(ptr, PDataType.LONG, null);
		assertEquals(4, length);
	}

	@Test
	public void testForVarcharArrayBiggerArraysNumber() {
		String[] strArr = new String[101];
		for (int i = 0; i <= 100; i++) {
			strArr[i] = "abc" + i;
		}
		PhoenixArray arr = PArrayDataType.instantiatePhoenixArray(
				PDataType.VARCHAR, strArr);
		byte[] bytes = PDataType.VARCHAR_ARRAY.toBytes(arr);
		PhoenixArray resultArr = (PhoenixArray) PDataType.VARCHAR_ARRAY
				.toObject(bytes, 0, bytes.length);
		assertEquals(arr, resultArr);
	}

	@Test
	public void testForTinyIntArray() {
		Byte[] byteArr = new Byte[2];
		byteArr[0] = 1;
		byteArr[1] = 2;
		PhoenixArray arr = PArrayDataType.instantiatePhoenixArray(
				PDataType.TINYINT, byteArr);
		PDataType.TINYINT_ARRAY.toObject(arr, PDataType.TINYINT_ARRAY);
		byte[] bytes = PDataType.TINYINT_ARRAY.toBytes(arr);
		PhoenixArray resultArr = (PhoenixArray) PDataType.TINYINT_ARRAY
				.toObject(bytes, 0, bytes.length);
		assertEquals(arr, resultArr);
	}

	@Test
	public void testForFloatArray() {
		Float[] floatArr = new Float[2];
		floatArr[0] = 1.06f;
		floatArr[1] = 2.89f;
		PhoenixArray arr = PArrayDataType.instantiatePhoenixArray(
				PDataType.FLOAT, floatArr);
		PDataType.FLOAT_ARRAY.toObject(arr, PDataType.FLOAT_ARRAY);
		byte[] bytes = PDataType.FLOAT_ARRAY.toBytes(arr);
		PhoenixArray resultArr = (PhoenixArray) PDataType.FLOAT_ARRAY.toObject(
				bytes, 0, bytes.length);
		assertEquals(arr, resultArr);
	}

	@Test
	public void testForDoubleArray() {
		Double[] doubleArr = new Double[2];
		doubleArr[0] = 1.06;
		doubleArr[1] = 2.89;
		PhoenixArray arr = PArrayDataType.instantiatePhoenixArray(
				PDataType.DOUBLE, doubleArr);
		PDataType.DOUBLE_ARRAY.toObject(arr, PDataType.DOUBLE_ARRAY);
		byte[] bytes = PDataType.DOUBLE_ARRAY.toBytes(arr);
		PhoenixArray resultArr = (PhoenixArray) PDataType.DOUBLE_ARRAY
				.toObject(bytes, 0, bytes.length);
		assertEquals(arr, resultArr);
	}

	@Test
	public void testForDecimalArray() {
		BigDecimal[] bigDecimalArr = new BigDecimal[2];
		bigDecimalArr[0] = new BigDecimal(89997);
		bigDecimalArr[1] = new BigDecimal(8999.995f);
		PhoenixArray arr = PArrayDataType.instantiatePhoenixArray(
				PDataType.DECIMAL, bigDecimalArr);
		PDataType.DECIMAL_ARRAY.toObject(arr, PDataType.DECIMAL_ARRAY);
		byte[] bytes = PDataType.DECIMAL_ARRAY.toBytes(arr);
		PhoenixArray resultArr = (PhoenixArray) PDataType.DECIMAL_ARRAY
				.toObject(bytes, 0, bytes.length);
		assertEquals(arr, resultArr);
	}

	@Test
	public void testForTimeStampArray() {
		Timestamp[] timeStampArr = new Timestamp[2];
		timeStampArr[0] = new Timestamp(System.currentTimeMillis());
		timeStampArr[1] = new Timestamp(900000l);
		PhoenixArray arr = PArrayDataType.instantiatePhoenixArray(
				PDataType.TIMESTAMP, timeStampArr);
		PDataType.TIMESTAMP_ARRAY.toObject(arr, PDataType.TIMESTAMP_ARRAY);
		byte[] bytes = PDataType.TIMESTAMP_ARRAY.toBytes(arr);
		PhoenixArray resultArr = (PhoenixArray) PDataType.TIMESTAMP_ARRAY
				.toObject(bytes, 0, bytes.length);
		assertEquals(arr, resultArr);
	}

	@Test
	public void testForUnSignedTimeStampArray() {
		Timestamp[] timeStampArr = new Timestamp[2];
		timeStampArr[0] = new Timestamp(System.currentTimeMillis());
		timeStampArr[1] = new Timestamp(900000l);
		PhoenixArray arr = PArrayDataType.instantiatePhoenixArray(
				PDataType.UNSIGNED_TIMESTAMP, timeStampArr);
		PDataType.UNSIGNED_TIMESTAMP_ARRAY.toObject(arr,
				PDataType.UNSIGNED_TIMESTAMP_ARRAY);
		byte[] bytes = PDataType.UNSIGNED_TIMESTAMP_ARRAY.toBytes(arr);
		PhoenixArray resultArr = (PhoenixArray) PDataType.UNSIGNED_TIMESTAMP_ARRAY
				.toObject(bytes, 0, bytes.length);
		assertEquals(arr, resultArr);
	}

	@Test
	public void testForTimeArray() {
		Time[] timeArr = new Time[2];
		timeArr[0] = new Time(System.currentTimeMillis());
		timeArr[1] = new Time(900000l);
		PhoenixArray arr = PArrayDataType.instantiatePhoenixArray(
				PDataType.TIME, timeArr);
		PDataType.TIME_ARRAY.toObject(arr, PDataType.TIME_ARRAY);
		byte[] bytes = PDataType.TIME_ARRAY.toBytes(arr);
		PhoenixArray resultArr = (PhoenixArray) PDataType.TIME_ARRAY.toObject(
				bytes, 0, bytes.length);
		assertEquals(arr, resultArr);
	}

	@Test
	public void testForUnsignedTimeArray() {
		Time[] timeArr = new Time[2];
		timeArr[0] = new Time(System.currentTimeMillis());
		timeArr[1] = new Time(900000l);
		PhoenixArray arr = PArrayDataType.instantiatePhoenixArray(
				PDataType.UNSIGNED_TIME, timeArr);
		PDataType.UNSIGNED_TIME_ARRAY.toObject(arr,
				PDataType.UNSIGNED_TIME_ARRAY);
		byte[] bytes = PDataType.UNSIGNED_TIME_ARRAY.toBytes(arr);
		PhoenixArray resultArr = (PhoenixArray) PDataType.UNSIGNED_TIME_ARRAY
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
				PDataType.DATE, dateArr);
		PDataType.DATE_ARRAY.toObject(arr, PDataType.DATE_ARRAY);
		byte[] bytes = PDataType.DATE_ARRAY.toBytes(arr);
		PhoenixArray resultArr = (PhoenixArray) PDataType.DATE_ARRAY.toObject(
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
				PDataType.UNSIGNED_DATE, dateArr);
		PDataType.UNSIGNED_DATE_ARRAY.toObject(arr,
				PDataType.UNSIGNED_DATE_ARRAY);
		byte[] bytes = PDataType.UNSIGNED_DATE_ARRAY.toBytes(arr);
		PhoenixArray resultArr = (PhoenixArray) PDataType.UNSIGNED_DATE_ARRAY
				.toObject(bytes, 0, bytes.length);
		assertEquals(arr, resultArr);
	}

	@Test
	public void testForUnSignedLongArray() {
		Long[] longArr = new Long[2];
		longArr[0] = 1l;
		longArr[1] = 2l;
		PhoenixArray arr = PArrayDataType.instantiatePhoenixArray(
				PDataType.UNSIGNED_LONG, longArr);
		PDataType.UNSIGNED_LONG_ARRAY.toObject(arr,
				PDataType.UNSIGNED_LONG_ARRAY);
		byte[] bytes = PDataType.UNSIGNED_LONG_ARRAY.toBytes(arr);
		PhoenixArray resultArr = (PhoenixArray) PDataType.UNSIGNED_LONG_ARRAY
				.toObject(bytes, 0, bytes.length);
		assertEquals(arr, resultArr);
	}

	@Test
	public void testForUnSignedIntArray() {
		Integer[] intArr = new Integer[2];
		intArr[0] = 1;
		intArr[1] = 2;
		PhoenixArray arr = PArrayDataType.instantiatePhoenixArray(
				PDataType.UNSIGNED_INT, intArr);
		PDataType.UNSIGNED_INT_ARRAY
				.toObject(arr, PDataType.UNSIGNED_INT_ARRAY);
		byte[] bytes = PDataType.UNSIGNED_INT_ARRAY.toBytes(arr);
		PhoenixArray resultArr = (PhoenixArray) PDataType.UNSIGNED_INT_ARRAY
				.toObject(bytes, 0, bytes.length);
		assertEquals(arr, resultArr);
	}

	@Test
	public void testForUnSignedSmallIntArray() {
		Short[] shortArr = new Short[2];
		shortArr[0] = 1;
		shortArr[1] = 2;
		PhoenixArray arr = PArrayDataType.instantiatePhoenixArray(
				PDataType.UNSIGNED_SMALLINT, shortArr);
		PDataType.UNSIGNED_SMALLINT_ARRAY.toObject(arr,
				PDataType.UNSIGNED_SMALLINT_ARRAY);
		byte[] bytes = PDataType.UNSIGNED_SMALLINT_ARRAY.toBytes(arr);
		PhoenixArray resultArr = (PhoenixArray) PDataType.UNSIGNED_SMALLINT_ARRAY
				.toObject(bytes, 0, bytes.length);
		assertEquals(arr, resultArr);
	}

	@Test
	public void testForUnSignedTinyIntArray() {
		Byte[] byteArr = new Byte[2];
		byteArr[0] = 1;
		byteArr[1] = 2;
		PhoenixArray arr = PArrayDataType.instantiatePhoenixArray(
				PDataType.UNSIGNED_TINYINT, byteArr);
		PDataType.UNSIGNED_TINYINT_ARRAY.toObject(arr,
				PDataType.UNSIGNED_TINYINT_ARRAY);
		byte[] bytes = PDataType.UNSIGNED_TINYINT_ARRAY.toBytes(arr);
		PhoenixArray resultArr = (PhoenixArray) PDataType.UNSIGNED_TINYINT_ARRAY
				.toObject(bytes, 0, bytes.length);
		assertEquals(arr, resultArr);
	}

	@Test
	public void testForUnSignedFloatArray() {
		Float[] floatArr = new Float[2];
		floatArr[0] = 1.9993f;
		floatArr[1] = 2.786f;
		PhoenixArray arr = PArrayDataType.instantiatePhoenixArray(
				PDataType.UNSIGNED_FLOAT, floatArr);
		PDataType.UNSIGNED_FLOAT_ARRAY.toObject(arr,
				PDataType.UNSIGNED_FLOAT_ARRAY);
		byte[] bytes = PDataType.UNSIGNED_FLOAT_ARRAY.toBytes(arr);
		PhoenixArray resultArr = (PhoenixArray) PDataType.UNSIGNED_FLOAT_ARRAY
				.toObject(bytes, 0, bytes.length);
		assertEquals(arr, resultArr);
	}

	@Test
	public void testForUnSignedDoubleArray() {
		Double[] doubleArr = new Double[2];
		doubleArr[0] = 1.9993;
		doubleArr[1] = 2.786;
		PhoenixArray arr = PArrayDataType.instantiatePhoenixArray(
				PDataType.UNSIGNED_DOUBLE, doubleArr);
		PDataType.UNSIGNED_DOUBLE_ARRAY.toObject(arr,
				PDataType.UNSIGNED_DOUBLE_ARRAY);
		byte[] bytes = PDataType.UNSIGNED_DOUBLE_ARRAY.toBytes(arr);
		PhoenixArray resultArr = (PhoenixArray) PDataType.UNSIGNED_DOUBLE_ARRAY
				.toObject(bytes, 0, bytes.length);
		assertEquals(arr, resultArr);
	}

    @Test
    public void testForArrayComparisionsForFixedWidth() {
        Double[] doubleArr = new Double[2];
        doubleArr[0] = 1.9993;
        doubleArr[1] = 2.786;
        PhoenixArray arr = PArrayDataType.instantiatePhoenixArray(PDataType.UNSIGNED_DOUBLE, doubleArr);
        PDataType.UNSIGNED_DOUBLE_ARRAY.toObject(arr, PDataType.UNSIGNED_DOUBLE_ARRAY);
        byte[] bytes1 = PDataType.UNSIGNED_DOUBLE_ARRAY.toBytes(arr);

        doubleArr = new Double[2];
        doubleArr[0] = 1.9993;
        doubleArr[1] = 2.786;
        arr = PArrayDataType.instantiatePhoenixArray(PDataType.UNSIGNED_DOUBLE, doubleArr);
        PDataType.UNSIGNED_DOUBLE_ARRAY.toObject(arr, PDataType.UNSIGNED_DOUBLE_ARRAY);
        byte[] bytes2 = PDataType.UNSIGNED_DOUBLE_ARRAY.toBytes(arr);
        assertTrue(Bytes.equals(bytes1, bytes2));
    }

    @Test
    public void testForArrayComparisionsWithInEqualityForFixedWidth() {
        Double[] doubleArr = new Double[2];
        doubleArr[0] = 1.9993;
        doubleArr[1] = 2.786;
        PhoenixArray arr = PArrayDataType.instantiatePhoenixArray(PDataType.UNSIGNED_DOUBLE, doubleArr);
        PDataType.UNSIGNED_DOUBLE_ARRAY.toObject(arr, PDataType.UNSIGNED_DOUBLE_ARRAY);
        byte[] bytes1 = PDataType.UNSIGNED_DOUBLE_ARRAY.toBytes(arr);

        doubleArr = new Double[3];
        doubleArr[0] = 1.9993;
        doubleArr[1] = 2.786;
        doubleArr[2] = 6.3;
        arr = PArrayDataType.instantiatePhoenixArray(PDataType.UNSIGNED_DOUBLE, doubleArr);
        PDataType.UNSIGNED_DOUBLE_ARRAY.toObject(arr, PDataType.UNSIGNED_DOUBLE_ARRAY);
        byte[] bytes2 = PDataType.UNSIGNED_DOUBLE_ARRAY.toBytes(arr);
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
        PhoenixArray arr = PArrayDataType.instantiatePhoenixArray(PDataType.VARCHAR, strArr);
        byte[] bytes1 = PDataType.VARCHAR_ARRAY.toBytes(arr);

        strArr = new String[5];
        strArr[0] = "abc";
        strArr[1] = "ereref";
        strArr[2] = "random";
        strArr[3] = "random1";
        strArr[4] = "ran";
        arr = PArrayDataType.instantiatePhoenixArray(PDataType.VARCHAR, strArr);
        byte[] bytes2 = PDataType.VARCHAR_ARRAY.toBytes(arr);
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
        PhoenixArray arr = PArrayDataType.instantiatePhoenixArray(PDataType.VARCHAR, strArr);
        byte[] bytes1 = PDataType.VARCHAR_ARRAY.toBytes(arr);

        strArr = new String[5];
        strArr[0] = "abc";
        strArr[1] = "ereref";
        strArr[2] = "random";
        strArr[3] = "random1";
        arr = PArrayDataType.instantiatePhoenixArray(PDataType.VARCHAR, strArr);
        byte[] bytes2 = PDataType.VARCHAR_ARRAY.toBytes(arr);
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
        PhoenixArray arr = PArrayDataType.instantiatePhoenixArray(PDataType.VARCHAR, strArr);
        byte[] bytes1 = PDataType.VARCHAR_ARRAY.toBytes(arr);

        strArr = new String[6];
        strArr[0] = null;
        strArr[1] = "ereref";
        strArr[2] = "random";
        strArr[3] = null;
        strArr[4] = null;
        strArr[5] = "ran";
        arr = PArrayDataType.instantiatePhoenixArray(PDataType.VARCHAR, strArr);
        byte[] bytes2 = PDataType.VARCHAR_ARRAY.toBytes(arr);
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
                PDataType.VARCHAR, strArr);
        byte[] bytes = PDataType.VARCHAR_ARRAY.toBytes(arr);
        PhoenixArray resultArr = (PhoenixArray) PDataType.VARCHAR_ARRAY
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
                PDataType.VARCHAR, strArr);
        byte[] bytes1 = PDataType.VARCHAR_ARRAY.toBytes(arr);
        
        strArr = new String[16];
        strArr[0] = "abc";
        strArr[1] = "bcd";
        strArr[2] = null;
        strArr[3] = null;
        strArr[4] = "bcd";
        strArr[strArr.length - 1] = "abc";
        arr = PArrayDataType.instantiatePhoenixArray(
                PDataType.VARCHAR, strArr);
        byte[] bytes2 = PDataType.VARCHAR_ARRAY.toBytes(arr);
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
                PDataType.VARCHAR, strArr);
        byte[] bytes1 = PDataType.VARCHAR_ARRAY.toBytes(arr);
        
        strArr = new String[500];
        strArr[0] = "abc";
        strArr[1] = "bcd";
        strArr[2] = null;
        strArr[3] = null;
        strArr[4] = "bcd";
        strArr[strArr.length - 1] = "abc";
        arr = PArrayDataType.instantiatePhoenixArray(
                PDataType.VARCHAR, strArr);
        byte[] bytes2 = PDataType.VARCHAR_ARRAY.toBytes(arr);
        assertTrue(Bytes.compareTo(bytes1, bytes2) == 0);
    }
    
}
