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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.util.TestUtil;
import org.junit.Test;


public class PDataTypeTest {
    @Test
    public void testFloatToLongComparison() {
        // Basic tests
        assertTrue(PDataType.FLOAT.compareTo(PDataType.FLOAT.toBytes(1e100), 0, PDataType.FLOAT.getByteSize(), SortOrder.getDefault(),
                PDataType.LONG.toBytes(1), 0, PDataType.LONG.getByteSize(), SortOrder.getDefault(), PDataType.LONG) > 0);
        assertTrue(PDataType.FLOAT.compareTo(PDataType.FLOAT.toBytes(0.001), 0, PDataType.FLOAT.getByteSize(), SortOrder.getDefault(),
                PDataType.LONG.toBytes(1), 0, PDataType.LONG.getByteSize(), SortOrder.getDefault(), PDataType.LONG) < 0);

        // Edge tests
        assertTrue(PDataType.FLOAT.compareTo(PDataType.FLOAT.toBytes(Integer.MAX_VALUE), 0,
                PDataType.FLOAT.getByteSize(), SortOrder.getDefault(), PDataType.LONG.toBytes(Integer.MAX_VALUE - 1), 0,
                PDataType.LONG.getByteSize(), SortOrder.getDefault(), PDataType.LONG) > 0);
        assertTrue(PDataType.FLOAT.compareTo(PDataType.FLOAT.toBytes(Integer.MIN_VALUE), 0,
                PDataType.FLOAT.getByteSize(), SortOrder.getDefault(), PDataType.LONG.toBytes(Integer.MIN_VALUE + 1), 0,
                PDataType.LONG.getByteSize(), SortOrder.getDefault(), PDataType.LONG) < 0);
        assertTrue(PDataType.FLOAT.compareTo(PDataType.FLOAT.toBytes(Integer.MIN_VALUE), 0,
                PDataType.FLOAT.getByteSize(), SortOrder.getDefault(), PDataType.LONG.toBytes(Integer.MIN_VALUE), 0,
                PDataType.LONG.getByteSize(), SortOrder.getDefault(), PDataType.LONG) == 0);
        assertTrue(PDataType.FLOAT.compareTo(PDataType.FLOAT.toBytes(Integer.MAX_VALUE + 1.0F), 0,
                PDataType.FLOAT.getByteSize(), SortOrder.getDefault(), PDataType.LONG.toBytes(Integer.MAX_VALUE), 0,
                PDataType.LONG.getByteSize(), SortOrder.getDefault(), PDataType.LONG) > 0); // Passes due to rounding
        assertTrue(PDataType.FLOAT.compareTo(PDataType.FLOAT.toBytes(Integer.MAX_VALUE + 129.0F), 0,
                PDataType.FLOAT.getByteSize(), SortOrder.getDefault(), PDataType.LONG.toBytes(Integer.MAX_VALUE), 0,
                PDataType.LONG.getByteSize(), SortOrder.getDefault(), PDataType.LONG) > 0);
        assertTrue(PDataType.FLOAT.compareTo(PDataType.FLOAT.toBytes(Integer.MIN_VALUE - 128.0F), 0,
                PDataType.FLOAT.getByteSize(), SortOrder.getDefault(), PDataType.LONG.toBytes(Integer.MIN_VALUE), 0,
                PDataType.LONG.getByteSize(), SortOrder.getDefault(), PDataType.LONG) == 0);
        assertTrue(PDataType.FLOAT.compareTo(PDataType.FLOAT.toBytes(Integer.MIN_VALUE - 129.0F), 0,
                PDataType.FLOAT.getByteSize(), SortOrder.getDefault(), PDataType.LONG.toBytes(Integer.MIN_VALUE), 0,
                PDataType.LONG.getByteSize(), SortOrder.getDefault(), PDataType.LONG) < 0);

        float f1 = 9111111111111111.0F;
        float f2 = 9111111111111112.0F;
        assertTrue(f1 == f2);
        long la = 9111111111111111L;
        assertTrue(f1 > Integer.MAX_VALUE);
        assertTrue(la == f1);
        assertTrue(la == f2);
        assertTrue(PDataType.FLOAT.compareTo(PDataType.FLOAT.toBytes(f1), 0, PDataType.FLOAT.getByteSize(), SortOrder.getDefault(),
                PDataType.LONG.toBytes(la), 0, PDataType.LONG.getByteSize(), SortOrder.getDefault(), PDataType.LONG) == 0);
        assertTrue(PDataType.FLOAT.compareTo(PDataType.FLOAT.toBytes(f2), 0, PDataType.FLOAT.getByteSize(), SortOrder.getDefault(),
                PDataType.LONG.toBytes(la), 0, PDataType.LONG.getByteSize(), SortOrder.getDefault(), PDataType.LONG) == 0);

        // Same as above, but reversing LHS and RHS
        assertTrue(PDataType.LONG.compareTo(PDataType.LONG.toBytes(1), 0, PDataType.LONG.getByteSize(), SortOrder.getDefault(),
                PDataType.FLOAT.toBytes(1e100), 0, PDataType.FLOAT.getByteSize(), SortOrder.getDefault(), PDataType.FLOAT) < 0);
        assertTrue(PDataType.LONG.compareTo(PDataType.LONG.toBytes(1), 0, PDataType.LONG.getByteSize(), SortOrder.getDefault(),
                PDataType.FLOAT.toBytes(0.001), 0, PDataType.FLOAT.getByteSize(), SortOrder.getDefault(), PDataType.FLOAT) > 0);

        assertTrue(PDataType.LONG.compareTo(PDataType.LONG.toBytes(Integer.MAX_VALUE - 1), 0,
                PDataType.LONG.getByteSize(), SortOrder.getDefault(), PDataType.FLOAT.toBytes(Integer.MAX_VALUE), 0,
                PDataType.FLOAT.getByteSize(), SortOrder.getDefault(), PDataType.FLOAT) < 0);
        assertTrue(PDataType.LONG.compareTo(PDataType.LONG.toBytes(Integer.MIN_VALUE + 1), 0,
                PDataType.LONG.getByteSize(), SortOrder.getDefault(), PDataType.FLOAT.toBytes(Integer.MIN_VALUE), 0,
                PDataType.FLOAT.getByteSize(), SortOrder.getDefault(), PDataType.FLOAT) > 0);
        assertTrue(PDataType.LONG.compareTo(PDataType.LONG.toBytes(Integer.MIN_VALUE), 0, PDataType.LONG.getByteSize(),
        		SortOrder.getDefault(), PDataType.FLOAT.toBytes(Integer.MIN_VALUE), 0, PDataType.FLOAT.getByteSize(), SortOrder.getDefault(),
                PDataType.FLOAT) == 0);
        assertTrue(PDataType.LONG.compareTo(PDataType.LONG.toBytes(Integer.MAX_VALUE), 0, PDataType.LONG.getByteSize(),
        		SortOrder.getDefault(), PDataType.FLOAT.toBytes(Integer.MAX_VALUE + 1.0F), 0, PDataType.FLOAT.getByteSize(), SortOrder.getDefault(),
                PDataType.FLOAT) < 0); // Passes due to rounding
        assertTrue(PDataType.LONG.compareTo(PDataType.LONG.toBytes(Integer.MAX_VALUE), 0, PDataType.LONG.getByteSize(),
        		SortOrder.getDefault(), PDataType.FLOAT.toBytes(Integer.MAX_VALUE + 129.0F), 0, PDataType.FLOAT.getByteSize(), SortOrder.getDefault(),
                PDataType.FLOAT) < 0);
        assertTrue(PDataType.LONG.compareTo(PDataType.LONG.toBytes(Integer.MIN_VALUE), 0, PDataType.LONG.getByteSize(),
        		SortOrder.getDefault(), PDataType.FLOAT.toBytes(Integer.MIN_VALUE - 128.0F), 0, PDataType.FLOAT.getByteSize(), SortOrder.getDefault(),
                PDataType.FLOAT) == 0);
        assertTrue(PDataType.LONG.compareTo(PDataType.LONG.toBytes(Integer.MIN_VALUE), 0, PDataType.LONG.getByteSize(),
        		SortOrder.getDefault(), PDataType.FLOAT.toBytes(Integer.MIN_VALUE - 129.0F), 0, PDataType.FLOAT.getByteSize(), SortOrder.getDefault(),
                PDataType.FLOAT) > 0);

        assertTrue(PDataType.LONG.compareTo(PDataType.LONG.toBytes(la), 0, PDataType.LONG.getByteSize(), SortOrder.getDefault(),
                PDataType.FLOAT.toBytes(f1), 0, PDataType.FLOAT.getByteSize(), SortOrder.getDefault(), PDataType.FLOAT) == 0);
        assertTrue(PDataType.LONG.compareTo(PDataType.LONG.toBytes(la), 0, PDataType.LONG.getByteSize(), SortOrder.getDefault(),
                PDataType.FLOAT.toBytes(f2), 0, PDataType.FLOAT.getByteSize(), SortOrder.getDefault(), PDataType.FLOAT) == 0);
    }        
        
    @Test
    public void testDoubleToDecimalComparison() {
        // Basic tests
        assertTrue(PDataType.DOUBLE.compareTo(PDataType.DOUBLE.toBytes(1.23), 0, PDataType.DOUBLE.getByteSize(), SortOrder.getDefault(),
                   PDataType.DECIMAL.toBytes(BigDecimal.valueOf(1.24)), 0, PDataType.DECIMAL.getByteSize(), SortOrder.getDefault(), PDataType.DECIMAL) < 0);
    }
    
    @Test
    public void testDoubleToLongComparison() {
        // Basic tests
        assertTrue(PDataType.DOUBLE.compareTo(PDataType.DOUBLE.toBytes(-1e100), 0, PDataType.DOUBLE.getByteSize(), SortOrder.getDefault(),
                PDataType.LONG.toBytes(1), 0, PDataType.LONG.getByteSize(), SortOrder.getDefault(), PDataType.LONG) < 0);
        assertTrue(PDataType.DOUBLE.compareTo(PDataType.DOUBLE.toBytes(0.001), 0, PDataType.DOUBLE.getByteSize(), SortOrder.getDefault(),
                PDataType.LONG.toBytes(1), 0, PDataType.LONG.getByteSize(), SortOrder.getDefault(), PDataType.LONG) < 0);

        assertTrue(PDataType.DOUBLE.compareTo(PDataType.DOUBLE.toBytes(Long.MAX_VALUE), 0,
                PDataType.DOUBLE.getByteSize(), SortOrder.getDefault(), PDataType.LONG.toBytes(Long.MAX_VALUE - 1), 0,
                PDataType.LONG.getByteSize(), SortOrder.getDefault(), PDataType.LONG) > 0);
        assertTrue(PDataType.DOUBLE.compareTo(PDataType.DOUBLE.toBytes(Long.MIN_VALUE), 0,
                PDataType.DOUBLE.getByteSize(), SortOrder.getDefault(), PDataType.LONG.toBytes(Long.MIN_VALUE + 1), 0,
                PDataType.LONG.getByteSize(), SortOrder.getDefault(), PDataType.LONG) < 0);
        assertTrue(PDataType.DOUBLE.compareTo(PDataType.DOUBLE.toBytes(Long.MIN_VALUE), 0,
                PDataType.DOUBLE.getByteSize(), SortOrder.getDefault(), PDataType.LONG.toBytes(Long.MIN_VALUE), 0,
                PDataType.LONG.getByteSize(), SortOrder.getDefault(), PDataType.LONG) == 0);
        assertTrue(PDataType.DOUBLE.compareTo(PDataType.DOUBLE.toBytes(Long.MAX_VALUE + 1024.0), 0,
                PDataType.DOUBLE.getByteSize(), SortOrder.getDefault(), PDataType.LONG.toBytes(Long.MAX_VALUE), 0,
                PDataType.LONG.getByteSize(), SortOrder.getDefault(), PDataType.LONG) == 0);
        assertTrue(PDataType.DOUBLE.compareTo(PDataType.DOUBLE.toBytes(Long.MAX_VALUE + 1025.0), 0,
                PDataType.DOUBLE.getByteSize(), SortOrder.getDefault(), PDataType.LONG.toBytes(Long.MAX_VALUE), 0,
                PDataType.LONG.getByteSize(), SortOrder.getDefault(), PDataType.LONG) > 0);
        assertTrue(PDataType.DOUBLE.compareTo(PDataType.DOUBLE.toBytes(Long.MIN_VALUE - 1024.0), 0,
                PDataType.DOUBLE.getByteSize(), SortOrder.getDefault(), PDataType.LONG.toBytes(Long.MIN_VALUE), 0,
                PDataType.LONG.getByteSize(), SortOrder.getDefault(), PDataType.LONG) == 0);
        assertTrue(PDataType.DOUBLE.compareTo(PDataType.DOUBLE.toBytes(Long.MIN_VALUE - 1025.0), 0,
                PDataType.DOUBLE.getByteSize(), SortOrder.getDefault(), PDataType.LONG.toBytes(Long.MIN_VALUE), 0,
                PDataType.LONG.getByteSize(), SortOrder.getDefault(), PDataType.LONG) < 0);

        // Same as above, but reversing LHS and RHS
        assertTrue(PDataType.LONG.compareTo(PDataType.LONG.toBytes(1), 0, PDataType.LONG.getByteSize(), SortOrder.getDefault(),
                PDataType.DOUBLE.toBytes(-1e100), 0, PDataType.DOUBLE.getByteSize(), SortOrder.getDefault(), PDataType.DOUBLE) > 0);
        assertTrue(PDataType.LONG.compareTo(PDataType.LONG.toBytes(1), 0, PDataType.LONG.getByteSize(), SortOrder.getDefault(),
                PDataType.DOUBLE.toBytes(0.001), 0, PDataType.DOUBLE.getByteSize(), SortOrder.getDefault(), PDataType.DOUBLE) > 0);

        assertTrue(PDataType.LONG.compareTo(PDataType.LONG.toBytes(Long.MAX_VALUE - 1), 0,
                PDataType.LONG.getByteSize(), SortOrder.getDefault(), PDataType.DOUBLE.toBytes(Long.MAX_VALUE), 0,
                PDataType.DOUBLE.getByteSize(), SortOrder.getDefault(), PDataType.DOUBLE) < 0);
        assertTrue(PDataType.LONG.compareTo(PDataType.LONG.toBytes(Long.MIN_VALUE + 1), 0,
                PDataType.LONG.getByteSize(), SortOrder.getDefault(), PDataType.DOUBLE.toBytes(Long.MIN_VALUE), 0,
                PDataType.DOUBLE.getByteSize(), SortOrder.getDefault(), PDataType.DOUBLE) > 0);
        assertTrue(PDataType.LONG.compareTo(PDataType.LONG.toBytes(Long.MIN_VALUE), 0, PDataType.LONG.getByteSize(),
        		SortOrder.getDefault(), PDataType.DOUBLE.toBytes(Long.MIN_VALUE), 0, PDataType.DOUBLE.getByteSize(), SortOrder.getDefault(),
                PDataType.DOUBLE) == 0);
        assertTrue(PDataType.LONG.compareTo(PDataType.LONG.toBytes(Long.MAX_VALUE), 0, PDataType.LONG.getByteSize(),
        		SortOrder.getDefault(), PDataType.DOUBLE.toBytes(Long.MAX_VALUE + 1024.0), 0, PDataType.DOUBLE.getByteSize(), SortOrder.getDefault(),
                PDataType.DOUBLE) == 0);
        assertTrue(PDataType.LONG.compareTo(PDataType.LONG.toBytes(Long.MAX_VALUE), 0, PDataType.LONG.getByteSize(),
        		SortOrder.getDefault(), PDataType.DOUBLE.toBytes(Long.MAX_VALUE + 1025.0), 0, PDataType.DOUBLE.getByteSize(), SortOrder.getDefault(),
                PDataType.DOUBLE) < 0);
        assertTrue(PDataType.LONG.compareTo(PDataType.LONG.toBytes(Long.MIN_VALUE), 0, PDataType.LONG.getByteSize(),
        		SortOrder.getDefault(), PDataType.DOUBLE.toBytes(Long.MIN_VALUE - 1024.0), 0, PDataType.DOUBLE.getByteSize(), SortOrder.getDefault(),
                PDataType.DOUBLE) == 0);
        assertTrue(PDataType.LONG.compareTo(PDataType.LONG.toBytes(Long.MIN_VALUE), 0, PDataType.LONG.getByteSize(),
        		SortOrder.getDefault(), PDataType.DOUBLE.toBytes(Long.MIN_VALUE - 1025.0), 0, PDataType.DOUBLE.getByteSize(), SortOrder.getDefault(),
                PDataType.DOUBLE) > 0);

        long i = 10;
        long maxl = (1L << 62);
        try {
            for (; i < 100; i++) {
                double d = Math.pow(2, i);
                if ((long)d > maxl) {
                    assertTrue(i > 62);
                    continue;
                }
                long l = (1L << i) - 1;
                assertTrue(l + 1L == (long)d);
                assertTrue(l < (long)d);
            }
        } catch (AssertionError t) {
            throw t;
        }
        double d = 0.0;
        try {
            while (d <= 1024) {
                double d1 = Long.MAX_VALUE;
                double d2 = Long.MAX_VALUE + d;
                assertTrue(d2 == d1);
                d++;
            }
        } catch (AssertionError t) {
            throw t;
        }
        d = 0.0;
        try {
            while (d >= -1024) {
                double d1 = Long.MIN_VALUE;
                double d2 = Long.MIN_VALUE + d;
                assertTrue(d2 == d1);
                d--;
            }
        } catch (AssertionError t) {
            throw t;
        }
        double d1 = Long.MAX_VALUE;
        double d2 = Long.MAX_VALUE + 1024.0;
        double d3 = Long.MAX_VALUE + 1025.0;
        assertTrue(d1 == d2);
        assertTrue(d3 > d1);
        long l1 = Long.MAX_VALUE - 1;
        assertTrue((long)d1 > l1);
    }
        
    @Test
    public void testLong() {
        Long la = 4L;
        byte[] b = PDataType.LONG.toBytes(la);
        Long lb = (Long)PDataType.LONG.toObject(b);
        assertEquals(la,lb);

        Long na = 1L;
        Long nb = -1L;
        byte[] ba = PDataType.LONG.toBytes(na);
        byte[] bb = PDataType.LONG.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) > 0);
        
        Integer value = 100;
        Object obj = PDataType.LONG.toObject(value, PDataType.INTEGER);
        assertTrue(obj instanceof Long);
        assertEquals(100, ((Long)obj).longValue());
        
        Long longValue = 100l;
        Object longObj = PDataType.LONG.toObject(longValue, PDataType.LONG);
        assertTrue(longObj instanceof Long);
        assertEquals(100, ((Long)longObj).longValue());
        
        assertEquals(0, PDataType.LONG.compareTo(Long.MAX_VALUE, Float.valueOf(Long.MAX_VALUE), PDataType.FLOAT));
        assertEquals(0, PDataType.LONG.compareTo(Long.MAX_VALUE, Double.valueOf(Long.MAX_VALUE), PDataType.DOUBLE));
        assertEquals(-1, PDataType.LONG.compareTo(99, Float.valueOf(100), PDataType.FLOAT));
        assertEquals(1, PDataType.LONG.compareTo(101, Float.valueOf(100), PDataType.FLOAT));
        
        Double d = -2.0;
        Object lo = PDataType.LONG.toObject(d, PDataType.DOUBLE);
        assertEquals(-2L, ((Long)lo).longValue());
        
        byte[] bytes = PDataType.DOUBLE.toBytes(d);
        lo = PDataType.LONG.toObject(bytes,0, bytes.length, PDataType.DOUBLE);
        assertEquals(-2L, ((Long)lo).longValue());
        
        Float f = -2.0f;
        lo = PDataType.LONG.toObject(f, PDataType.FLOAT);
        assertEquals(-2L, ((Long)lo).longValue());
        
        bytes = PDataType.FLOAT.toBytes(f);
        lo = PDataType.LONG.toObject(bytes,0, bytes.length, PDataType.FLOAT);
        assertEquals(-2L, ((Long)lo).longValue());
        
        // Checks for unsignedlong
        d = 2.0;
        lo = PDataType.UNSIGNED_LONG.toObject(d, PDataType.DOUBLE);
        assertEquals(2L, ((Long)lo).longValue());
        
        bytes = PDataType.DOUBLE.toBytes(d);
        lo = PDataType.UNSIGNED_LONG.toObject(bytes,0, bytes.length, PDataType.DOUBLE);
        assertEquals(2L, ((Long)lo).longValue());
        
        f = 2.0f;
        lo = PDataType.UNSIGNED_LONG.toObject(f, PDataType.FLOAT);
        assertEquals(2L, ((Long)lo).longValue());
        
        bytes = PDataType.FLOAT.toBytes(f);
        lo = PDataType.UNSIGNED_LONG.toObject(bytes,0, bytes.length, PDataType.FLOAT);
        assertEquals(2L, ((Long)lo).longValue());
        
    }

    @Test
    public void testInt() {
        Integer na = 4;
        byte[] b = PDataType.INTEGER.toBytes(na);
        Integer nb = (Integer)PDataType.INTEGER.toObject(b);
        assertEquals(na,nb);

        na = 1;
        nb = -1;
        byte[] ba = PDataType.INTEGER.toBytes(na);
        byte[] bb = PDataType.INTEGER.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) > 0);

        na = -1;
        nb = -3;
        ba = PDataType.INTEGER.toBytes(na);
        bb = PDataType.INTEGER.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) > 0);

        na = -3;
        nb = -100000000;
        ba = PDataType.INTEGER.toBytes(na);
        bb = PDataType.INTEGER.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) > 0);
        
        Long value = 100l;
        Object obj = PDataType.INTEGER.toObject(value, PDataType.LONG);
        assertTrue(obj instanceof Integer);
        assertEquals(100, ((Integer)obj).intValue());
        
        Float unsignedFloatValue = 100f;
        Object unsignedFloatObj = PDataType.INTEGER.toObject(unsignedFloatValue, PDataType.UNSIGNED_FLOAT);
        assertTrue(unsignedFloatObj instanceof Integer);
        assertEquals(100, ((Integer)unsignedFloatObj).intValue());
        
        Double unsignedDoubleValue = 100d;
        Object unsignedDoubleObj = PDataType.INTEGER.toObject(unsignedDoubleValue, PDataType.UNSIGNED_DOUBLE);
        assertTrue(unsignedDoubleObj instanceof Integer);
        assertEquals(100, ((Integer)unsignedDoubleObj).intValue());
        
        Float floatValue = 100f;
        Object floatObj = PDataType.INTEGER.toObject(floatValue, PDataType.FLOAT);
        assertTrue(floatObj instanceof Integer);
        assertEquals(100, ((Integer)floatObj).intValue());
        
        Double doubleValue = 100d;
        Object doubleObj = PDataType.INTEGER.toObject(doubleValue, PDataType.DOUBLE);
        assertTrue(doubleObj instanceof Integer);
        assertEquals(100, ((Integer)doubleObj).intValue());
        
        Short shortValue = 100;
        Object shortObj = PDataType.INTEGER.toObject(shortValue, PDataType.SMALLINT);
        assertTrue(shortObj instanceof Integer);
        assertEquals(100, ((Integer)shortObj).intValue());
    }
    
    @Test
    public void testSmallInt() {
        Short na = 4;
        byte[] b = PDataType.SMALLINT.toBytes(na);
        Short nb = (Short)PDataType.SMALLINT.toObject(b);
        assertEquals(na,nb);
        
        na = 4;
        b = PDataType.SMALLINT.toBytes(na, SortOrder.DESC);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        ptr.set(b);
        nb = PDataType.SMALLINT.getCodec().decodeShort(ptr, SortOrder.DESC);
        assertEquals(na,nb);

        na = 1;
        nb = -1;
        byte[] ba = PDataType.SMALLINT.toBytes(na);
        byte[] bb = PDataType.SMALLINT.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) > 0);

        na = -1;
        nb = -3;
        ba = PDataType.SMALLINT.toBytes(na);
        bb = PDataType.SMALLINT.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) > 0);

        na = -3;
        nb = -10000;
        ba = PDataType.SMALLINT.toBytes(na);
        bb = PDataType.SMALLINT.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) > 0);
        
        Integer value = 100;
        Object obj = PDataType.SMALLINT.toObject(value, PDataType.INTEGER);
        assertTrue(obj instanceof Short);
        assertEquals(100, ((Short)obj).shortValue());
        
        Float unsignedFloatValue = 100f;
        Object unsignedFloatObj = PDataType.SMALLINT.toObject(unsignedFloatValue, PDataType.UNSIGNED_FLOAT);
        assertTrue(unsignedFloatObj instanceof Short);
        assertEquals(100, ((Short)unsignedFloatObj).shortValue());
        
        Double unsignedDoubleValue = 100d;
        Object unsignedDoubleObj = PDataType.SMALLINT.toObject(unsignedDoubleValue, PDataType.UNSIGNED_DOUBLE);
        assertTrue(unsignedDoubleObj instanceof Short);
        assertEquals(100, ((Short)unsignedDoubleObj).shortValue());
        
        Float floatValue = 100f;
        Object floatObj = PDataType.SMALLINT.toObject(floatValue, PDataType.FLOAT);
        assertTrue(floatObj instanceof Short);
        assertEquals(100, ((Short)floatObj).shortValue());
        
        Double doubleValue = 100d;
        Object doubleObj = PDataType.SMALLINT.toObject(doubleValue, PDataType.DOUBLE);
        assertTrue(doubleObj instanceof Short);
        assertEquals(100, ((Short)doubleObj).shortValue());
    }
    
    @Test
    public void testTinyInt() {
        Byte na = 4;
        byte[] b = PDataType.TINYINT.toBytes(na);
        Byte nb = (Byte)PDataType.TINYINT.toObject(b);
        assertEquals(na,nb);

        na = 1;
        nb = -1;
        byte[] ba = PDataType.TINYINT.toBytes(na);
        byte[] bb = PDataType.TINYINT.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) > 0);

        na = -1;
        nb = -3;
        ba = PDataType.TINYINT.toBytes(na);
        bb = PDataType.TINYINT.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) > 0);

        na = -3;
        nb = -100;
        ba = PDataType.TINYINT.toBytes(na);
        bb = PDataType.TINYINT.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) > 0);
        
        Integer value = 100;
        Object obj = PDataType.TINYINT.toObject(value, PDataType.INTEGER);
        assertTrue(obj instanceof Byte);
        assertEquals(100, ((Byte)obj).byteValue());
        
        Float floatValue = 100f;
        Object floatObj = PDataType.TINYINT.toObject(floatValue, PDataType.FLOAT);
        assertTrue(floatObj instanceof Byte);
        assertEquals(100, ((Byte)floatObj).byteValue());
        
        Float unsignedFloatValue = 100f;
        Object unsignedFloatObj = PDataType.TINYINT.toObject(unsignedFloatValue, PDataType.UNSIGNED_FLOAT);
        assertTrue(unsignedFloatObj instanceof Byte);
        assertEquals(100, ((Byte)unsignedFloatObj).byteValue());
        
        Double unsignedDoubleValue = 100d;
        Object unsignedDoubleObj = PDataType.TINYINT.toObject(unsignedDoubleValue, PDataType.UNSIGNED_DOUBLE);
        assertTrue(unsignedDoubleObj instanceof Byte);
        assertEquals(100, ((Byte)unsignedDoubleObj).byteValue());
        
        Double doubleValue = 100d;
        Object doubleObj = PDataType.TINYINT.toObject(doubleValue, PDataType.DOUBLE);
        assertTrue(doubleObj instanceof Byte);
        assertEquals(100, ((Byte)doubleObj).byteValue());
    }
    
    @Test
    public void testUnsignedSmallInt() {
        Short na = 4;
        byte[] b = PDataType.UNSIGNED_SMALLINT.toBytes(na);
        Short nb = (Short)PDataType.UNSIGNED_SMALLINT.toObject(b);
        assertEquals(na,nb);

        na = 10;
        nb = 8;
        byte[] ba = PDataType.UNSIGNED_SMALLINT.toBytes(na);
        byte[] bb = PDataType.UNSIGNED_SMALLINT.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) > 0);
        
        Integer value = 100;
        Object obj = PDataType.UNSIGNED_SMALLINT.toObject(value, PDataType.INTEGER);
        assertTrue(obj instanceof Short);
        assertEquals(100, ((Short)obj).shortValue());
        
        Float floatValue = 100f;
        Object floatObj = PDataType.UNSIGNED_SMALLINT.toObject(floatValue, PDataType.FLOAT);
        assertTrue(floatObj instanceof Short);
        assertEquals(100, ((Short)floatObj).shortValue());
        
        Float unsignedFloatValue = 100f;
        Object unsignedFloatObj = PDataType.UNSIGNED_SMALLINT.toObject(unsignedFloatValue, PDataType.UNSIGNED_FLOAT);
        assertTrue(unsignedFloatObj instanceof Short);
        assertEquals(100, ((Short)unsignedFloatObj).shortValue());
        
        Double unsignedDoubleValue = 100d;
        Object unsignedDoubleObj = PDataType.UNSIGNED_SMALLINT.toObject(unsignedDoubleValue, PDataType.UNSIGNED_DOUBLE);
        assertTrue(unsignedDoubleObj instanceof Short);
        assertEquals(100, ((Short)unsignedDoubleObj).shortValue());
        
        Double doubleValue = 100d;
        Object doubleObj = PDataType.UNSIGNED_SMALLINT.toObject(doubleValue, PDataType.DOUBLE);
        assertTrue(doubleObj instanceof Short);
        assertEquals(100, ((Short)doubleObj).shortValue());
    }
    
    @Test
    public void testUnsignedTinyInt() {
        Byte na = 4;
        byte[] b = PDataType.UNSIGNED_TINYINT.toBytes(na);
        Byte nb = (Byte)PDataType.UNSIGNED_TINYINT.toObject(b);
        assertEquals(na,nb);

        na = 10;
        nb = 8;
        byte[] ba = PDataType.UNSIGNED_TINYINT.toBytes(na);
        byte[] bb = PDataType.UNSIGNED_TINYINT.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) > 0);
        
        Integer value = 100;
        Object obj = PDataType.UNSIGNED_TINYINT.toObject(value, PDataType.INTEGER);
        assertTrue(obj instanceof Byte);
        assertEquals(100, ((Byte)obj).byteValue());
        
        Float floatValue = 100f;
        Object floatObj = PDataType.UNSIGNED_TINYINT.toObject(floatValue, PDataType.FLOAT);
        assertTrue(floatObj instanceof Byte);
        assertEquals(100, ((Byte)floatObj).byteValue());
        
        Float unsignedFloatValue = 100f;
        Object unsignedFloatObj = PDataType.UNSIGNED_TINYINT.toObject(unsignedFloatValue, PDataType.UNSIGNED_FLOAT);
        assertTrue(unsignedFloatObj instanceof Byte);
        assertEquals(100, ((Byte)unsignedFloatObj).byteValue());
        
        Double unsignedDoubleValue = 100d;
        Object unsignedDoubleObj = PDataType.UNSIGNED_TINYINT.toObject(unsignedDoubleValue, PDataType.UNSIGNED_DOUBLE);
        assertTrue(unsignedDoubleObj instanceof Byte);
        assertEquals(100, ((Byte)unsignedDoubleObj).byteValue());
        
        Double doubleValue = 100d;
        Object doubleObj = PDataType.UNSIGNED_TINYINT.toObject(doubleValue, PDataType.DOUBLE);
        assertTrue(doubleObj instanceof Byte);
        assertEquals(100, ((Byte)doubleObj).byteValue());
    }
    
    @Test
    public void testUnsignedFloat() {
        Float na = 0.005f;
        byte[] b = PDataType.UNSIGNED_FLOAT.toBytes(na);
        Float nb = (Float)PDataType.UNSIGNED_FLOAT.toObject(b);
        assertEquals(na,nb);
        
        na = 10.0f;
        b = PDataType.UNSIGNED_FLOAT.toBytes(na, SortOrder.DESC);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        ptr.set(b);
        nb = PDataType.UNSIGNED_FLOAT.getCodec().decodeFloat(ptr, SortOrder.DESC);
        assertEquals(na,nb);
        
        na = 2.0f;
        nb = 1.0f;
        byte[] ba = PDataType.UNSIGNED_FLOAT.toBytes(na);
        byte[] bb = PDataType.UNSIGNED_FLOAT.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) > 0);
        
        na = 0.0f;
        nb = Float.MIN_VALUE;
        ba = PDataType.UNSIGNED_FLOAT.toBytes(na);
        bb = PDataType.UNSIGNED_FLOAT.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) < 0);
        
        na = Float.MIN_VALUE;
        nb = Float.MAX_VALUE;
        ba = PDataType.UNSIGNED_FLOAT.toBytes(na);
        bb = PDataType.UNSIGNED_FLOAT.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) < 0);
        
        na = Float.MAX_VALUE;
        nb = Float.POSITIVE_INFINITY;
        ba = PDataType.UNSIGNED_FLOAT.toBytes(na);
        bb = PDataType.UNSIGNED_FLOAT.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) < 0);
        
        na = Float.POSITIVE_INFINITY;
        nb = Float.NaN;
        ba = PDataType.UNSIGNED_FLOAT.toBytes(na);
        bb = PDataType.UNSIGNED_FLOAT.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) < 0);
        
        Integer value = 100;
        Object obj = PDataType.UNSIGNED_FLOAT.toObject(value, PDataType.INTEGER);
        assertTrue(obj instanceof Float);
    }
    
    @Test
    public void testUnsignedDouble() {
        Double na = 0.005;
        byte[] b = PDataType.UNSIGNED_DOUBLE.toBytes(na);
        Double nb = (Double)PDataType.UNSIGNED_DOUBLE.toObject(b);
        assertEquals(na,nb);
        
        na = 10.0;
        b = PDataType.UNSIGNED_DOUBLE.toBytes(na, SortOrder.DESC);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        ptr.set(b);
        nb = PDataType.UNSIGNED_DOUBLE.getCodec().decodeDouble(ptr, SortOrder.DESC);
        assertEquals(na,nb);

        na = 2.0;
        nb = 1.0;
        byte[] ba = PDataType.UNSIGNED_DOUBLE.toBytes(na);
        byte[] bb = PDataType.UNSIGNED_DOUBLE.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) > 0);
        
        na = 0.0;
        nb = Double.MIN_VALUE;
        ba = PDataType.UNSIGNED_DOUBLE.toBytes(na);
        bb = PDataType.UNSIGNED_DOUBLE.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) < 0);
        
        na = Double.MIN_VALUE;
        nb = Double.MAX_VALUE;
        ba = PDataType.UNSIGNED_DOUBLE.toBytes(na);
        bb = PDataType.UNSIGNED_DOUBLE.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) < 0);
        
        na = Double.MAX_VALUE;
        nb = Double.POSITIVE_INFINITY;
        ba = PDataType.UNSIGNED_DOUBLE.toBytes(na);
        bb = PDataType.UNSIGNED_DOUBLE.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) < 0);
        
        na = Double.POSITIVE_INFINITY;
        nb = Double.NaN;
        ba = PDataType.UNSIGNED_DOUBLE.toBytes(na);
        bb = PDataType.UNSIGNED_DOUBLE.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) < 0);
        
        Integer value = 100;
        Object obj = PDataType.UNSIGNED_DOUBLE.toObject(value, PDataType.INTEGER);
        assertTrue(obj instanceof Double);
        
        assertEquals(1, PDataType.UNSIGNED_DOUBLE.compareTo(Double.valueOf(101), Long.valueOf(100), PDataType.LONG));
        assertEquals(0, PDataType.UNSIGNED_DOUBLE.compareTo(Double.valueOf(Long.MAX_VALUE), Long.MAX_VALUE, PDataType.LONG));
        assertEquals(-1, PDataType.UNSIGNED_DOUBLE.compareTo(Double.valueOf(1), Long.valueOf(100), PDataType.LONG));
        
        assertEquals(0, PDataType.UNSIGNED_DOUBLE.compareTo(Double.valueOf(101), BigDecimal.valueOf(101.0), PDataType.DECIMAL));
    }
    
    @Test
    public void testFloat() {
        Float na = 0.005f;
        byte[] b = PDataType.FLOAT.toBytes(na);
        Float nb = (Float)PDataType.FLOAT.toObject(b);
        assertEquals(na,nb);
        
        na = 10.0f;
        b = PDataType.FLOAT.toBytes(na, SortOrder.DESC);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        ptr.set(b);
        nb = PDataType.FLOAT.getCodec().decodeFloat(ptr, SortOrder.DESC);
        assertEquals(na,nb);
        
        na = 1.0f;
        nb = -1.0f;
        byte[] ba = PDataType.FLOAT.toBytes(na);
        byte[] bb = PDataType.FLOAT.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) > 0);

        na = -1f;
        nb = -3f;
        ba = PDataType.FLOAT.toBytes(na);
        bb = PDataType.FLOAT.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) > 0);
        
        na = Float.NEGATIVE_INFINITY;
        nb = -Float.MAX_VALUE;
        ba = PDataType.FLOAT.toBytes(na);
        bb = PDataType.FLOAT.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) < 0);
        
        na = -Float.MAX_VALUE;
        nb = -Float.MIN_VALUE;
        ba = PDataType.FLOAT.toBytes(na);
        bb = PDataType.FLOAT.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) < 0);
        
        na = -Float.MIN_VALUE;
        nb = -0.0f;
        ba = PDataType.FLOAT.toBytes(na);
        bb = PDataType.FLOAT.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) < 0);
        
        na = -0.0f;
        nb = 0.0f;
        ba = PDataType.FLOAT.toBytes(na);
        bb = PDataType.FLOAT.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) < 0);
        
        na = 0.0f;
        nb = Float.MIN_VALUE;
        ba = PDataType.FLOAT.toBytes(na);
        bb = PDataType.FLOAT.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) < 0);
        
        na = Float.MIN_VALUE;
        nb = Float.MAX_VALUE;
        ba = PDataType.FLOAT.toBytes(na);
        bb = PDataType.FLOAT.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) < 0);
        
        na = Float.MAX_VALUE;
        nb = Float.POSITIVE_INFINITY;
        ba = PDataType.FLOAT.toBytes(na);
        bb = PDataType.FLOAT.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) < 0);
        
        na = Float.POSITIVE_INFINITY;
        nb = Float.NaN;
        ba = PDataType.FLOAT.toBytes(na);
        bb = PDataType.FLOAT.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) < 0);
        
        Integer value = 100;
        Object obj = PDataType.FLOAT.toObject(value, PDataType.INTEGER);
        assertTrue(obj instanceof Float);
        
        Double dvalue = Double.NEGATIVE_INFINITY;
        obj = PDataType.FLOAT.toObject(dvalue, PDataType.DOUBLE);
        assertTrue(obj instanceof Float);
        assertEquals(Float.NEGATIVE_INFINITY, obj);
        
        na = 1.0f;
        nb = -1.0f;
        ba = PDataType.FLOAT.toBytes(na);
        bb = PDataType.FLOAT.toBytes(nb);
        float nna = PDataType.FLOAT.getCodec().decodeFloat(ba, 0, SortOrder.DESC);
        float nnb = PDataType.FLOAT.getCodec().decodeFloat(bb, 0, SortOrder.DESC);
        assertTrue(Float.compare(nna, nnb) < 0);
    }
    
    @Test
    public void testDouble() {
        Double na = 0.005;
        byte[] b = PDataType.DOUBLE.toBytes(na);
        Double nb = (Double)PDataType.DOUBLE.toObject(b);
        assertEquals(na,nb);
        
        na = 10.0;
        b = PDataType.DOUBLE.toBytes(na, SortOrder.DESC);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        ptr.set(b);
        nb = PDataType.DOUBLE.getCodec().decodeDouble(ptr, SortOrder.DESC);
        assertEquals(na,nb);

        na = 1.0;
        nb = -1.0;
        byte[] ba = PDataType.DOUBLE.toBytes(na);
        byte[] bb = PDataType.DOUBLE.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) > 0);

        na = -1.0;
        nb = -3.0;
        ba = PDataType.DOUBLE.toBytes(na);
        bb = PDataType.DOUBLE.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) > 0);
        
        na = Double.NEGATIVE_INFINITY;
        nb = -Double.MAX_VALUE;
        ba = PDataType.DOUBLE.toBytes(na);
        bb = PDataType.DOUBLE.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) < 0);
        
        na = -Double.MAX_VALUE;
        nb = -Double.MIN_VALUE;
        ba = PDataType.DOUBLE.toBytes(na);
        bb = PDataType.DOUBLE.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) < 0);
        
        na = -Double.MIN_VALUE;
        nb = -0.0;
        ba = PDataType.DOUBLE.toBytes(na);
        bb = PDataType.DOUBLE.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) < 0);
        
        na = -0.0;
        nb = 0.0;
        ba = PDataType.DOUBLE.toBytes(na);
        bb = PDataType.DOUBLE.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) < 0);
        
        na = 0.0;
        nb = Double.MIN_VALUE;
        ba = PDataType.DOUBLE.toBytes(na);
        bb = PDataType.DOUBLE.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) < 0);
        
        na = Double.MIN_VALUE;
        nb = Double.MAX_VALUE;
        ba = PDataType.DOUBLE.toBytes(na);
        bb = PDataType.DOUBLE.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) < 0);
        
        na = Double.MAX_VALUE;
        nb = Double.POSITIVE_INFINITY;
        ba = PDataType.DOUBLE.toBytes(na);
        bb = PDataType.DOUBLE.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) < 0);
        
        na = Double.POSITIVE_INFINITY;
        nb = Double.NaN;
        ba = PDataType.DOUBLE.toBytes(na);
        bb = PDataType.DOUBLE.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) < 0);
        
        Integer value = 100;
        Object obj = PDataType.DOUBLE.toObject(value, PDataType.INTEGER);
        assertTrue(obj instanceof Double);
        
        na = 1.0;
        nb = -1.0;
        ba = PDataType.DOUBLE.toBytes(na);
        bb = PDataType.DOUBLE.toBytes(nb);
        double nna = PDataType.DOUBLE.getCodec().decodeDouble(ba, 0, SortOrder.DESC);
        double nnb = PDataType.DOUBLE.getCodec().decodeDouble(bb, 0, SortOrder.DESC);
        assertTrue(Double.compare(nna, nnb) < 0);
        
        assertEquals(1, PDataType.DOUBLE.compareTo(Double.valueOf(101), Long.valueOf(100), PDataType.LONG));
        assertEquals(0, PDataType.DOUBLE.compareTo(Double.valueOf(Long.MAX_VALUE), Long.MAX_VALUE, PDataType.LONG));
        assertEquals(-1, PDataType.DOUBLE.compareTo(Double.valueOf(1), Long.valueOf(100), PDataType.LONG));
        
        assertEquals(0, PDataType.DOUBLE.compareTo(Double.valueOf(101), BigDecimal.valueOf(101.0), PDataType.DECIMAL));
    }

    @Test
    public void testBigDecimal() {
        byte[] b;
        BigDecimal na, nb;

        b = new byte[] {
                (byte)0xc2,0x02,0x10,0x36,0x22,0x22,0x22,0x22,0x22,0x22,0x0f,0x27,0x38,0x1c,0x05,0x40,0x62,0x21,0x54,0x4d,0x4e,0x01,0x14,0x36,0x0d,0x33
        };
        BigDecimal decodedBytes = (BigDecimal)PDataType.DECIMAL.toObject(b);
        assertTrue(decodedBytes.compareTo(BigDecimal.ZERO) > 0);

        na = new BigDecimal(new BigInteger("12345678901239998123456789"), 2);
        //[-52, 13, 35, 57, 79, 91, 13, 40, 100, 82, 24, 46, 68, 90]
        b = PDataType.DECIMAL.toBytes(na);
        nb = (BigDecimal)PDataType.DECIMAL.toObject(b);
        TestUtil.assertRoundEquals(na,nb);
        assertTrue(b.length <= PDataType.DECIMAL.estimateByteSize(na));

        na = new BigDecimal("115.533333333333331438552704639732837677001953125");
        b = PDataType.DECIMAL.toBytes(na);
        nb = (BigDecimal)PDataType.DECIMAL.toObject(b);
        TestUtil.assertRoundEquals(na,nb);
        assertTrue(b.length <= PDataType.DECIMAL.estimateByteSize(na));

        na = new BigDecimal(2.5);
        b = PDataType.DECIMAL.toBytes(na);
        nb = (BigDecimal)PDataType.DECIMAL.toObject(b);
        assertTrue(na.compareTo(nb) == 0);
        assertTrue(b.length <= PDataType.DECIMAL.estimateByteSize(na));

        // If we don't remove trailing zeros, this fails
        na = new BigDecimal(Double.parseDouble("96.45238095238095"));
        String naStr = na.toString();
        assertTrue(naStr != null);
        b = PDataType.DECIMAL.toBytes(na);
        nb = (BigDecimal)PDataType.DECIMAL.toObject(b);
        TestUtil.assertRoundEquals(na,nb);
        assertTrue(b.length <= PDataType.DECIMAL.estimateByteSize(na));

        // If we don't remove trailing zeros, this fails
        na = new BigDecimal(-1000);
        b = PDataType.DECIMAL.toBytes(na);
        nb = (BigDecimal)PDataType.DECIMAL.toObject(b);
        assertTrue(na.compareTo(nb) == 0);
        assertTrue(b.length <= PDataType.DECIMAL.estimateByteSize(na));

        na = new BigDecimal("1000.5829999999999913");
        b = PDataType.DECIMAL.toBytes(na);
        nb = (BigDecimal)PDataType.DECIMAL.toObject(b);
        assertTrue(na.compareTo(nb) == 0);
        assertTrue(b.length <= PDataType.DECIMAL.estimateByteSize(na));

        na = TestUtil.computeAverage(11000, 3);
        b = PDataType.DECIMAL.toBytes(na);
        nb = (BigDecimal)PDataType.DECIMAL.toObject(b);
        assertTrue(na.compareTo(nb) == 0);
        assertTrue(b.length <= PDataType.DECIMAL.estimateByteSize(na));

        na = new BigDecimal(new BigInteger("12345678901239999"), 2);
        b = PDataType.DECIMAL.toBytes(na);
        nb = (BigDecimal)PDataType.DECIMAL.toObject(b);
        assertTrue(na.compareTo(nb) == 0);
        assertTrue(b.length <= PDataType.DECIMAL.estimateByteSize(na));

        na = new BigDecimal(1);
        nb = new BigDecimal(-1);
        byte[] ba = PDataType.DECIMAL.toBytes(na);
        byte[] bb = PDataType.DECIMAL.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) > 0);
        assertTrue(ba.length <= PDataType.DECIMAL.estimateByteSize(na));
        assertTrue(bb.length <= PDataType.DECIMAL.estimateByteSize(nb));

        na = new BigDecimal(-1);
        nb = new BigDecimal(-2);
        ba = PDataType.DECIMAL.toBytes(na);
        bb = PDataType.DECIMAL.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) > 0);
        assertTrue(ba.length <= PDataType.DECIMAL.estimateByteSize(na));
        assertTrue(bb.length <= PDataType.DECIMAL.estimateByteSize(nb));

        na = new BigDecimal(-3);
        nb = new BigDecimal(-1000);
        assertTrue(na.compareTo(nb) > 0);
        ba = PDataType.DECIMAL.toBytes(na);
        bb = PDataType.DECIMAL.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) > 0);
        assertTrue(ba.length <= PDataType.DECIMAL.estimateByteSize(na));
        assertTrue(bb.length <= PDataType.DECIMAL.estimateByteSize(nb));

        na = new BigDecimal(BigInteger.valueOf(12345678901239998L), 2);
        nb = new BigDecimal(97);
        assertTrue(na.compareTo(nb) > 0);
        ba = PDataType.DECIMAL.toBytes(na);
        bb = PDataType.DECIMAL.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) > 0);
        assertTrue(ba.length <= PDataType.DECIMAL.estimateByteSize(na));
        assertTrue(bb.length <= PDataType.DECIMAL.estimateByteSize(nb));

        List<BigDecimal> values = Arrays.asList(new BigDecimal[] {
            new BigDecimal(-1000),
            new BigDecimal(-100000000),
            new BigDecimal(1000),
            new BigDecimal("-0.001"),
            new BigDecimal("0.001"),
            new BigDecimal(new BigInteger("12345678901239999"), 2),
            new BigDecimal(new BigInteger("12345678901239998"), 2),
            new BigDecimal(new BigInteger("12345678901239998123456789"), 2), // bigger than long
            new BigDecimal(new BigInteger("-1000"),3),
            new BigDecimal(new BigInteger("-1000"),10),
            new BigDecimal(99),
            new BigDecimal(97),
            new BigDecimal(-3)
        });

        List<byte[]> byteValues = new ArrayList<byte[]>();
        for (int i = 0; i < values.size(); i++) {
            byteValues.add(PDataType.DECIMAL.toBytes(values.get(i)));
        }

        for (int i = 0; i < values.size(); i++) {
            BigDecimal expected = values.get(i);
            BigDecimal actual = (BigDecimal)PDataType.DECIMAL.toObject(byteValues.get(i));
            assertTrue("For " + i + " expected " + expected + " but got " + actual,expected.round(PDataType.DEFAULT_MATH_CONTEXT).compareTo(actual.round(PDataType.DEFAULT_MATH_CONTEXT)) == 0);
            assertTrue(byteValues.get(i).length <= PDataType.DECIMAL.estimateByteSize(expected));
        }

        Collections.sort(values);
        Collections.sort(byteValues, Bytes.BYTES_COMPARATOR);

        for (int i = 0; i < values.size(); i++) {
            BigDecimal expected = values.get(i);
            byte[] bytes = PDataType.DECIMAL.toBytes(values.get(i));
            assertNotNull("bytes converted from values should not be null!", bytes);
            BigDecimal actual = (BigDecimal)PDataType.DECIMAL.toObject(byteValues.get(i));
            assertTrue("For " + i + " expected " + expected + " but got " + actual,expected.round(PDataType.DEFAULT_MATH_CONTEXT).compareTo(actual.round(PDataType.DEFAULT_MATH_CONTEXT))==0);
        }


        {
            String[] strs ={
                    "\\xC2\\x03\\x0C\\x10\\x01\\x01\\x01\\x01\\x01\\x019U#\\x13W\\x09\\x09"
                    ,"\\xC2\\x03<,ddddddN\\x1B\\x1B!.9N"
                    ,"\\xC2\\x039"
                    ,"\\xC2\\x03\\x16,\\x01\\x01\\x01\\x01\\x01\\x01E\\x16\\x16\\x03@\\x1EG"
                    ,"\\xC2\\x02d6dddddd\\x15*]\\x0E<1F"
                    ,"\\xC2\\x04 3"
                    ,"\\xC2\\x03$Ldddddd\\x0A\\x06\\x06\\x1ES\\x1C\\x08"
                    ,"\\xC2\\x03\\x1E\\x0A\\x01\\x01\\x01\\x01\\x01\\x01#\\x0B=4 AV"
                    ,"\\xC2\\x02\\\\x04dddddd\\x15*]\\x0E<1F"
                    ,"\\xC2\\x02V\"\\x01\\x01\\x01\\x01\\x01\\x02\\x1A\\x068\\x162&O"
            };
            for (String str : strs) {
                byte[] bytes = Bytes.toBytesBinary(str);
                Object o = PDataType.DECIMAL.toObject(bytes);
                assertNotNull(o);
                //System.out.println(o.getClass() +" " + bytesToHex(bytes)+" " + o+" ");
            }
        }
    }
    public static String bytesToHex(byte[] bytes) {
        final char[] hexArray = {'0','1','2','3','4','5','6','7','8','9','A','B','C','D','E','F'};
        char[] hexChars = new char[bytes.length * 2];
        int v;
        for ( int j = 0; j < bytes.length; j++ ) {
            v = bytes[j] & 0xFF;
            hexChars[j * 2] = hexArray[v >>> 4];
            hexChars[j * 2 + 1] = hexArray[v & 0x0F];
        }
        return new String(hexChars);
    }

    @Test
    public void testEmptyString() throws Throwable {
        byte[] b1 = PDataType.VARCHAR.toBytes("");
        byte[] b2 = PDataType.VARCHAR.toBytes(null);
        assert (b1.length == 0 && Bytes.compareTo(b1, b2) == 0);
    }

    @Test
    public void testNull() throws Throwable {
        byte[] b = new byte[8];
        for (PDataType type : PDataType.values()) {
            try {
				type.toBytes(null);
				type.toBytes(null, b, 0);
				type.toObject(new byte[0], 0, 0);
				type.toObject(new byte[0], 0, 0, type);
                if (type.isArrayType()) {
					type.toBytes(new PhoenixArray());
					type.toBytes(new PhoenixArray(), b, 0);
                }
            } catch (ConstraintViolationException e) {
            	if (!type.isArrayType() && ! ( type.isFixedWidth() && e.getMessage().contains("may not be null"))) {
            		// Fixed width types do not support the concept of a "null" value.
                    fail(type + ":" + e);
                }
            }
        }
    }

    @Test
    public void testValueCoersion() throws Exception {
        // Testing coercing integer to other values.
        assertFalse(PDataType.DOUBLE.isCoercibleTo(PDataType.FLOAT));
        assertTrue(PDataType.DOUBLE.isCoercibleTo(PDataType.FLOAT, 10.0));
        assertTrue(PDataType.DOUBLE.isCoercibleTo(PDataType.FLOAT, 0.0));
        assertTrue(PDataType.DOUBLE.isCoercibleTo(PDataType.FLOAT, -10.0));
        assertFalse(PDataType.DOUBLE.isCoercibleTo(PDataType.FLOAT, Double.valueOf(Float.MAX_VALUE) + Double.valueOf(Float.MAX_VALUE)));
        assertFalse(PDataType.DOUBLE.isCoercibleTo(PDataType.LONG));
        assertTrue(PDataType.DOUBLE.isCoercibleTo(PDataType.LONG, 10.0));
        assertTrue(PDataType.DOUBLE.isCoercibleTo(PDataType.LONG, 0.0));
        assertTrue(PDataType.DOUBLE.isCoercibleTo(PDataType.LONG, -10.0));
        assertFalse(PDataType.DOUBLE.isCoercibleTo(PDataType.LONG, Double.valueOf(Long.MAX_VALUE) + Double.valueOf(Long.MAX_VALUE)));
        assertFalse(PDataType.DOUBLE.isCoercibleTo(PDataType.UNSIGNED_INT));
        assertTrue(PDataType.DOUBLE.isCoercibleTo(PDataType.UNSIGNED_INT, 10.0));
        assertTrue(PDataType.DOUBLE.isCoercibleTo(PDataType.UNSIGNED_INT, 0.0));
        assertFalse(PDataType.DOUBLE.isCoercibleTo(PDataType.UNSIGNED_INT, -10.0));
        assertFalse(PDataType.DOUBLE.isCoercibleTo(PDataType.UNSIGNED_LONG));
        assertTrue(PDataType.DOUBLE.isCoercibleTo(PDataType.UNSIGNED_LONG, 10.0));
        assertTrue(PDataType.DOUBLE.isCoercibleTo(PDataType.UNSIGNED_LONG, 0.0));
        assertFalse(PDataType.DOUBLE.isCoercibleTo(PDataType.UNSIGNED_LONG, -10.0));
        assertFalse(PDataType.DOUBLE.isCoercibleTo(PDataType.SMALLINT));
        assertTrue(PDataType.DOUBLE.isCoercibleTo(PDataType.SMALLINT, 10.0));
        assertTrue(PDataType.DOUBLE.isCoercibleTo(PDataType.SMALLINT, 0.0));
        assertTrue(PDataType.DOUBLE.isCoercibleTo(PDataType.SMALLINT, -10.0));
        assertFalse(PDataType.DOUBLE.isCoercibleTo(PDataType.SMALLINT, -100000.0));
        assertFalse(PDataType.DOUBLE.isCoercibleTo(PDataType.TINYINT));
        assertTrue(PDataType.DOUBLE.isCoercibleTo(PDataType.TINYINT, 10.0));
        assertTrue(PDataType.DOUBLE.isCoercibleTo(PDataType.TINYINT, 0.0));
        assertTrue(PDataType.DOUBLE.isCoercibleTo(PDataType.TINYINT, -10.0));
        assertFalse(PDataType.DOUBLE.isCoercibleTo(PDataType.TINYINT, -1000.0));
        assertFalse(PDataType.DOUBLE.isCoercibleTo(PDataType.UNSIGNED_SMALLINT));
        assertTrue(PDataType.DOUBLE.isCoercibleTo(PDataType.UNSIGNED_SMALLINT, 10.0));
        assertTrue(PDataType.DOUBLE.isCoercibleTo(PDataType.UNSIGNED_SMALLINT, 0.0));
        assertFalse(PDataType.DOUBLE.isCoercibleTo(PDataType.UNSIGNED_SMALLINT, -10.0));
        assertFalse(PDataType.DOUBLE.isCoercibleTo(PDataType.UNSIGNED_SMALLINT, -100000.0));
        assertFalse(PDataType.DOUBLE.isCoercibleTo(PDataType.UNSIGNED_TINYINT));
        assertTrue(PDataType.DOUBLE.isCoercibleTo(PDataType.UNSIGNED_TINYINT, 10.0));
        assertTrue(PDataType.DOUBLE.isCoercibleTo(PDataType.UNSIGNED_TINYINT, 0.0));
        assertFalse(PDataType.DOUBLE.isCoercibleTo(PDataType.UNSIGNED_TINYINT, -10.0));
        assertFalse(PDataType.DOUBLE.isCoercibleTo(PDataType.UNSIGNED_TINYINT, -1000.0));
        assertFalse(PDataType.DOUBLE.isCoercibleTo(PDataType.UNSIGNED_DOUBLE));
        assertTrue(PDataType.DOUBLE.isCoercibleTo(PDataType.UNSIGNED_DOUBLE, 10.0));
        assertTrue(PDataType.DOUBLE.isCoercibleTo(PDataType.UNSIGNED_DOUBLE, 0.0));
        assertFalse(PDataType.DOUBLE.isCoercibleTo(PDataType.UNSIGNED_DOUBLE, -10.0));
        assertFalse(PDataType.DOUBLE.isCoercibleTo(PDataType.UNSIGNED_FLOAT));
        assertTrue(PDataType.DOUBLE.isCoercibleTo(PDataType.UNSIGNED_FLOAT, 10.0));
        assertTrue(PDataType.DOUBLE.isCoercibleTo(PDataType.UNSIGNED_FLOAT, 0.0));
        assertFalse(PDataType.DOUBLE.isCoercibleTo(PDataType.UNSIGNED_FLOAT, -10.0));
        assertFalse(PDataType.DOUBLE.isCoercibleTo(PDataType.UNSIGNED_FLOAT, Double.MAX_VALUE));
        
        assertTrue(PDataType.FLOAT.isCoercibleTo(PDataType.DOUBLE));
        assertFalse(PDataType.FLOAT.isCoercibleTo(PDataType.LONG));
        assertTrue(PDataType.FLOAT.isCoercibleTo(PDataType.LONG, 10.0f));
        assertTrue(PDataType.FLOAT.isCoercibleTo(PDataType.LONG, 0.0f));
        assertTrue(PDataType.FLOAT.isCoercibleTo(PDataType.LONG, -10.0f));
        assertFalse(PDataType.FLOAT.isCoercibleTo(PDataType.LONG, Float.valueOf(Long.MAX_VALUE) + Float.valueOf(Long.MAX_VALUE)));
        assertFalse(PDataType.FLOAT.isCoercibleTo(PDataType.UNSIGNED_INT));
        assertTrue(PDataType.FLOAT.isCoercibleTo(PDataType.UNSIGNED_INT, 10.0f));
        assertTrue(PDataType.FLOAT.isCoercibleTo(PDataType.UNSIGNED_INT, 0.0f));
        assertFalse(PDataType.FLOAT.isCoercibleTo(PDataType.UNSIGNED_INT, -10.0f));
        assertFalse(PDataType.FLOAT.isCoercibleTo(PDataType.UNSIGNED_LONG));
        assertTrue(PDataType.FLOAT.isCoercibleTo(PDataType.UNSIGNED_LONG, 10.0f));
        assertTrue(PDataType.FLOAT.isCoercibleTo(PDataType.UNSIGNED_LONG, 0.0f));
        assertFalse(PDataType.FLOAT.isCoercibleTo(PDataType.UNSIGNED_LONG, -10.0f));
        assertFalse(PDataType.FLOAT.isCoercibleTo(PDataType.SMALLINT));
        assertTrue(PDataType.FLOAT.isCoercibleTo(PDataType.SMALLINT, 10.0f));
        assertTrue(PDataType.FLOAT.isCoercibleTo(PDataType.SMALLINT, 0.0f));
        assertTrue(PDataType.FLOAT.isCoercibleTo(PDataType.SMALLINT, -10.0f));
        assertFalse(PDataType.FLOAT.isCoercibleTo(PDataType.SMALLINT, -100000.0f));
        assertFalse(PDataType.FLOAT.isCoercibleTo(PDataType.TINYINT));
        assertTrue(PDataType.FLOAT.isCoercibleTo(PDataType.TINYINT, 10.0f));
        assertTrue(PDataType.FLOAT.isCoercibleTo(PDataType.TINYINT, 0.0f));
        assertTrue(PDataType.FLOAT.isCoercibleTo(PDataType.TINYINT, -10.0f));
        assertFalse(PDataType.FLOAT.isCoercibleTo(PDataType.TINYINT, -1000.0f));
        assertFalse(PDataType.FLOAT.isCoercibleTo(PDataType.UNSIGNED_SMALLINT));
        assertTrue(PDataType.FLOAT.isCoercibleTo(PDataType.UNSIGNED_SMALLINT, 10.0f));
        assertTrue(PDataType.FLOAT.isCoercibleTo(PDataType.UNSIGNED_SMALLINT, 0.0f));
        assertFalse(PDataType.FLOAT.isCoercibleTo(PDataType.UNSIGNED_SMALLINT, -10.0f));
        assertFalse(PDataType.FLOAT.isCoercibleTo(PDataType.UNSIGNED_SMALLINT, -100000.0f));
        assertFalse(PDataType.FLOAT.isCoercibleTo(PDataType.UNSIGNED_TINYINT));
        assertTrue(PDataType.FLOAT.isCoercibleTo(PDataType.UNSIGNED_TINYINT, 10.0f));
        assertTrue(PDataType.FLOAT.isCoercibleTo(PDataType.UNSIGNED_TINYINT, 0.0f));
        assertFalse(PDataType.FLOAT.isCoercibleTo(PDataType.UNSIGNED_TINYINT, -10.0f));
        assertFalse(PDataType.FLOAT.isCoercibleTo(PDataType.UNSIGNED_TINYINT, -1000.0f));
        assertFalse(PDataType.FLOAT.isCoercibleTo(PDataType.UNSIGNED_DOUBLE));
        assertFalse(PDataType.FLOAT.isCoercibleTo(PDataType.UNSIGNED_FLOAT));
        assertTrue(PDataType.FLOAT.isCoercibleTo(PDataType.UNSIGNED_FLOAT, 10.0f));
        assertTrue(PDataType.FLOAT.isCoercibleTo(PDataType.UNSIGNED_FLOAT, 0.0f));
        assertFalse(PDataType.FLOAT.isCoercibleTo(PDataType.UNSIGNED_FLOAT, -10.0f));
        
        assertFalse(PDataType.UNSIGNED_DOUBLE.isCoercibleTo(PDataType.FLOAT));
        assertTrue(PDataType.UNSIGNED_DOUBLE.isCoercibleTo(PDataType.DOUBLE));
        assertTrue(PDataType.UNSIGNED_DOUBLE.isCoercibleTo(PDataType.FLOAT, 10.0));
        assertTrue(PDataType.UNSIGNED_DOUBLE.isCoercibleTo(PDataType.FLOAT, 0.0));
        assertFalse(PDataType.UNSIGNED_DOUBLE.isCoercibleTo(PDataType.FLOAT, Double.MAX_VALUE));
        assertFalse(PDataType.UNSIGNED_DOUBLE.isCoercibleTo(PDataType.LONG));
        assertTrue(PDataType.UNSIGNED_DOUBLE.isCoercibleTo(PDataType.LONG, 10.0));
        assertTrue(PDataType.UNSIGNED_DOUBLE.isCoercibleTo(PDataType.LONG, 0.0));
        assertFalse(PDataType.UNSIGNED_DOUBLE.isCoercibleTo(PDataType.LONG, Double.MAX_VALUE));
        assertFalse(PDataType.UNSIGNED_DOUBLE.isCoercibleTo(PDataType.UNSIGNED_INT));
        assertTrue(PDataType.UNSIGNED_DOUBLE.isCoercibleTo(PDataType.UNSIGNED_INT, 10.0));
        assertTrue(PDataType.UNSIGNED_DOUBLE.isCoercibleTo(PDataType.UNSIGNED_INT, 0.0));
        assertFalse(PDataType.UNSIGNED_DOUBLE.isCoercibleTo(PDataType.UNSIGNED_LONG));
        assertTrue(PDataType.UNSIGNED_DOUBLE.isCoercibleTo(PDataType.UNSIGNED_LONG, 10.0));
        assertTrue(PDataType.UNSIGNED_DOUBLE.isCoercibleTo(PDataType.UNSIGNED_LONG, 0.0));
        assertFalse(PDataType.UNSIGNED_DOUBLE.isCoercibleTo(PDataType.SMALLINT));
        assertTrue(PDataType.UNSIGNED_DOUBLE.isCoercibleTo(PDataType.SMALLINT, 10.0));
        assertTrue(PDataType.UNSIGNED_DOUBLE.isCoercibleTo(PDataType.SMALLINT, 0.0));
        assertFalse(PDataType.UNSIGNED_DOUBLE.isCoercibleTo(PDataType.TINYINT));
        assertTrue(PDataType.UNSIGNED_DOUBLE.isCoercibleTo(PDataType.TINYINT, 10.0));
        assertTrue(PDataType.UNSIGNED_DOUBLE.isCoercibleTo(PDataType.TINYINT, 0.0));
        assertFalse(PDataType.UNSIGNED_DOUBLE.isCoercibleTo(PDataType.UNSIGNED_SMALLINT));
        assertTrue(PDataType.UNSIGNED_DOUBLE.isCoercibleTo(PDataType.UNSIGNED_SMALLINT, 10.0));
        assertTrue(PDataType.UNSIGNED_DOUBLE.isCoercibleTo(PDataType.UNSIGNED_SMALLINT, 0.0));
        assertFalse(PDataType.UNSIGNED_DOUBLE.isCoercibleTo(PDataType.UNSIGNED_TINYINT));
        assertTrue(PDataType.UNSIGNED_DOUBLE.isCoercibleTo(PDataType.UNSIGNED_TINYINT, 10.0));
        assertTrue(PDataType.UNSIGNED_DOUBLE.isCoercibleTo(PDataType.UNSIGNED_TINYINT, 0.0));
        assertFalse(PDataType.UNSIGNED_DOUBLE.isCoercibleTo(PDataType.UNSIGNED_FLOAT));
        assertTrue(PDataType.UNSIGNED_DOUBLE.isCoercibleTo(PDataType.UNSIGNED_FLOAT, 10.0));
        assertTrue(PDataType.UNSIGNED_DOUBLE.isCoercibleTo(PDataType.UNSIGNED_FLOAT, 0.0));
        assertFalse(PDataType.UNSIGNED_DOUBLE.isCoercibleTo(PDataType.UNSIGNED_FLOAT, Double.MAX_VALUE));
        
        assertTrue(PDataType.UNSIGNED_FLOAT.isCoercibleTo(PDataType.FLOAT));
        assertTrue(PDataType.UNSIGNED_FLOAT.isCoercibleTo(PDataType.DOUBLE));
        assertFalse(PDataType.UNSIGNED_FLOAT.isCoercibleTo(PDataType.LONG));
        assertTrue(PDataType.UNSIGNED_FLOAT.isCoercibleTo(PDataType.LONG, 10.0f));
        assertTrue(PDataType.UNSIGNED_FLOAT.isCoercibleTo(PDataType.LONG, 0.0f));
        assertFalse(PDataType.UNSIGNED_FLOAT.isCoercibleTo(PDataType.LONG, Float.MAX_VALUE));
        assertFalse(PDataType.UNSIGNED_FLOAT.isCoercibleTo(PDataType.UNSIGNED_INT));
        assertTrue(PDataType.UNSIGNED_FLOAT.isCoercibleTo(PDataType.UNSIGNED_INT, 10.0f));
        assertTrue(PDataType.UNSIGNED_FLOAT.isCoercibleTo(PDataType.UNSIGNED_INT, 0.0f));
        assertFalse(PDataType.UNSIGNED_FLOAT.isCoercibleTo(PDataType.UNSIGNED_LONG));
        assertTrue(PDataType.UNSIGNED_FLOAT.isCoercibleTo(PDataType.UNSIGNED_LONG, 10.0f));
        assertTrue(PDataType.UNSIGNED_FLOAT.isCoercibleTo(PDataType.UNSIGNED_LONG, 0.0f));
        assertFalse(PDataType.UNSIGNED_FLOAT.isCoercibleTo(PDataType.SMALLINT));
        assertTrue(PDataType.UNSIGNED_FLOAT.isCoercibleTo(PDataType.SMALLINT, 10.0f));
        assertTrue(PDataType.UNSIGNED_FLOAT.isCoercibleTo(PDataType.SMALLINT, 0.0f));
        assertFalse(PDataType.UNSIGNED_FLOAT.isCoercibleTo(PDataType.TINYINT));
        assertTrue(PDataType.UNSIGNED_FLOAT.isCoercibleTo(PDataType.TINYINT, 10.0f));
        assertTrue(PDataType.UNSIGNED_FLOAT.isCoercibleTo(PDataType.TINYINT, 0.0f));
        assertFalse(PDataType.UNSIGNED_FLOAT.isCoercibleTo(PDataType.UNSIGNED_SMALLINT));
        assertTrue(PDataType.UNSIGNED_FLOAT.isCoercibleTo(PDataType.UNSIGNED_SMALLINT, 10.0f));
        assertTrue(PDataType.UNSIGNED_FLOAT.isCoercibleTo(PDataType.UNSIGNED_SMALLINT, 0.0f));
        assertFalse(PDataType.UNSIGNED_FLOAT.isCoercibleTo(PDataType.UNSIGNED_TINYINT));
        assertTrue(PDataType.UNSIGNED_FLOAT.isCoercibleTo(PDataType.UNSIGNED_TINYINT, 10.0f));
        assertTrue(PDataType.UNSIGNED_FLOAT.isCoercibleTo(PDataType.UNSIGNED_TINYINT, 0.0f));
        assertTrue(PDataType.UNSIGNED_FLOAT.isCoercibleTo(PDataType.UNSIGNED_DOUBLE));
        
        // Testing coercing integer to other values.
        assertTrue(PDataType.INTEGER.isCoercibleTo(PDataType.DOUBLE));
        assertTrue(PDataType.INTEGER.isCoercibleTo(PDataType.FLOAT));
        assertTrue(PDataType.INTEGER.isCoercibleTo(PDataType.LONG));
        assertTrue(PDataType.INTEGER.isCoercibleTo(PDataType.LONG, 10));
        assertTrue(PDataType.INTEGER.isCoercibleTo(PDataType.LONG, 0));
        assertTrue(PDataType.INTEGER.isCoercibleTo(PDataType.LONG, -10));
        assertFalse(PDataType.INTEGER.isCoercibleTo(PDataType.UNSIGNED_INT));
        assertTrue(PDataType.INTEGER.isCoercibleTo(PDataType.UNSIGNED_INT, 10));
        assertTrue(PDataType.INTEGER.isCoercibleTo(PDataType.UNSIGNED_INT, 0));
        assertFalse(PDataType.INTEGER.isCoercibleTo(PDataType.UNSIGNED_INT, -10));
        assertFalse(PDataType.INTEGER.isCoercibleTo(PDataType.UNSIGNED_LONG));
        assertTrue(PDataType.INTEGER.isCoercibleTo(PDataType.UNSIGNED_LONG, 10));
        assertTrue(PDataType.INTEGER.isCoercibleTo(PDataType.UNSIGNED_LONG, 0));
        assertFalse(PDataType.INTEGER.isCoercibleTo(PDataType.UNSIGNED_LONG, -10));
        assertFalse(PDataType.INTEGER.isCoercibleTo(PDataType.SMALLINT));
        assertTrue(PDataType.INTEGER.isCoercibleTo(PDataType.SMALLINT, 10));
        assertTrue(PDataType.INTEGER.isCoercibleTo(PDataType.SMALLINT, 0));
        assertTrue(PDataType.INTEGER.isCoercibleTo(PDataType.SMALLINT, -10));
        assertFalse(PDataType.INTEGER.isCoercibleTo(PDataType.SMALLINT, -100000));
        assertFalse(PDataType.INTEGER.isCoercibleTo(PDataType.TINYINT));
        assertTrue(PDataType.INTEGER.isCoercibleTo(PDataType.TINYINT, 10));
        assertTrue(PDataType.INTEGER.isCoercibleTo(PDataType.TINYINT, 0));
        assertTrue(PDataType.INTEGER.isCoercibleTo(PDataType.TINYINT, -10));
        assertFalse(PDataType.INTEGER.isCoercibleTo(PDataType.TINYINT, -1000));
        assertFalse(PDataType.INTEGER.isCoercibleTo(PDataType.UNSIGNED_SMALLINT));
        assertTrue(PDataType.INTEGER.isCoercibleTo(PDataType.UNSIGNED_SMALLINT, 10));
        assertTrue(PDataType.INTEGER.isCoercibleTo(PDataType.UNSIGNED_SMALLINT, 0));
        assertFalse(PDataType.INTEGER.isCoercibleTo(PDataType.UNSIGNED_SMALLINT, -10));
        assertFalse(PDataType.INTEGER.isCoercibleTo(PDataType.UNSIGNED_SMALLINT, -100000));
        assertFalse(PDataType.INTEGER.isCoercibleTo(PDataType.UNSIGNED_TINYINT));
        assertTrue(PDataType.INTEGER.isCoercibleTo(PDataType.UNSIGNED_TINYINT, 10));
        assertTrue(PDataType.INTEGER.isCoercibleTo(PDataType.UNSIGNED_TINYINT, 0));
        assertFalse(PDataType.INTEGER.isCoercibleTo(PDataType.UNSIGNED_TINYINT, -10));
        assertFalse(PDataType.INTEGER.isCoercibleTo(PDataType.UNSIGNED_TINYINT, -1000));
        assertFalse(PDataType.INTEGER.isCoercibleTo(PDataType.UNSIGNED_FLOAT, -10));
        assertTrue(PDataType.INTEGER.isCoercibleTo(PDataType.UNSIGNED_FLOAT, 10));
        assertTrue(PDataType.INTEGER.isCoercibleTo(PDataType.UNSIGNED_FLOAT, 0));
        assertFalse(PDataType.INTEGER.isCoercibleTo(PDataType.UNSIGNED_DOUBLE, -10));
        assertTrue(PDataType.INTEGER.isCoercibleTo(PDataType.UNSIGNED_DOUBLE, 10));
        assertTrue(PDataType.INTEGER.isCoercibleTo(PDataType.UNSIGNED_DOUBLE, 0));
        assertTrue(PDataType.INTEGER.isCoercibleTo(PDataType.VARBINARY, 0));

        // Testing coercing long to other values.
        assertTrue(PDataType.LONG.isCoercibleTo(PDataType.DOUBLE));
        assertFalse(PDataType.LONG.isCoercibleTo(PDataType.INTEGER));
        assertFalse(PDataType.LONG.isCoercibleTo(PDataType.INTEGER, Long.MAX_VALUE));
        assertFalse(PDataType.LONG.isCoercibleTo(PDataType.INTEGER, Integer.MAX_VALUE + 10L));
        assertTrue(PDataType.LONG.isCoercibleTo(PDataType.INTEGER, (long)Integer.MAX_VALUE));
        assertTrue(PDataType.LONG.isCoercibleTo(PDataType.INTEGER, Integer.MAX_VALUE - 10L));
        assertTrue(PDataType.LONG.isCoercibleTo(PDataType.INTEGER, 10L));
        assertTrue(PDataType.LONG.isCoercibleTo(PDataType.INTEGER, 0L));
        assertTrue(PDataType.LONG.isCoercibleTo(PDataType.INTEGER, -10L));
        assertTrue(PDataType.LONG.isCoercibleTo(PDataType.INTEGER, Integer.MIN_VALUE + 10L));
        assertTrue(PDataType.LONG.isCoercibleTo(PDataType.INTEGER, (long)Integer.MIN_VALUE));
        assertFalse(PDataType.LONG.isCoercibleTo(PDataType.INTEGER, Integer.MIN_VALUE - 10L));
        assertFalse(PDataType.LONG.isCoercibleTo(PDataType.INTEGER, Long.MIN_VALUE));
        assertFalse(PDataType.LONG.isCoercibleTo(PDataType.UNSIGNED_INT));
        assertTrue(PDataType.LONG.isCoercibleTo(PDataType.UNSIGNED_INT, 10L));
        assertTrue(PDataType.LONG.isCoercibleTo(PDataType.UNSIGNED_INT, 0L));
        assertFalse(PDataType.LONG.isCoercibleTo(PDataType.UNSIGNED_INT, -10L));
        assertTrue(PDataType.LONG.isCoercibleTo(PDataType.UNSIGNED_LONG, Long.MAX_VALUE));
        assertTrue(PDataType.LONG.isCoercibleTo(PDataType.UNSIGNED_LONG, 10L));
        assertTrue(PDataType.LONG.isCoercibleTo(PDataType.UNSIGNED_LONG, 0L));
        assertFalse(PDataType.LONG.isCoercibleTo(PDataType.UNSIGNED_LONG, -10L));
        assertFalse(PDataType.LONG.isCoercibleTo(PDataType.UNSIGNED_LONG, Long.MIN_VALUE));
        assertFalse(PDataType.LONG.isCoercibleTo(PDataType.SMALLINT));
        assertTrue(PDataType.LONG.isCoercibleTo(PDataType.SMALLINT, 10L));
        assertTrue(PDataType.LONG.isCoercibleTo(PDataType.SMALLINT, 0L));
        assertTrue(PDataType.LONG.isCoercibleTo(PDataType.SMALLINT, -10L));
        assertFalse(PDataType.LONG.isCoercibleTo(PDataType.SMALLINT, -100000L));
        assertFalse(PDataType.LONG.isCoercibleTo(PDataType.TINYINT));
        assertTrue(PDataType.LONG.isCoercibleTo(PDataType.TINYINT, 10L));
        assertTrue(PDataType.LONG.isCoercibleTo(PDataType.TINYINT, 0L));
        assertTrue(PDataType.LONG.isCoercibleTo(PDataType.TINYINT, -10L));
        assertFalse(PDataType.LONG.isCoercibleTo(PDataType.TINYINT, -1000L));
        assertFalse(PDataType.LONG.isCoercibleTo(PDataType.UNSIGNED_SMALLINT));
        assertTrue(PDataType.LONG.isCoercibleTo(PDataType.UNSIGNED_SMALLINT, 10L));
        assertTrue(PDataType.LONG.isCoercibleTo(PDataType.UNSIGNED_SMALLINT, 0L));
        assertFalse(PDataType.LONG.isCoercibleTo(PDataType.UNSIGNED_SMALLINT, -10L));
        assertFalse(PDataType.LONG.isCoercibleTo(PDataType.UNSIGNED_SMALLINT, -100000L));
        assertFalse(PDataType.LONG.isCoercibleTo(PDataType.UNSIGNED_TINYINT));
        assertTrue(PDataType.LONG.isCoercibleTo(PDataType.UNSIGNED_TINYINT, 10L));
        assertTrue(PDataType.LONG.isCoercibleTo(PDataType.UNSIGNED_TINYINT, 0L));
        assertFalse(PDataType.LONG.isCoercibleTo(PDataType.UNSIGNED_TINYINT, -10L));
        assertFalse(PDataType.LONG.isCoercibleTo(PDataType.UNSIGNED_TINYINT, -1000L));
		assertTrue(PDataType.LONG.isCoercibleTo(PDataType.UNSIGNED_DOUBLE, 10L));
		assertTrue(PDataType.LONG.isCoercibleTo(PDataType.UNSIGNED_DOUBLE, 0L));
		assertFalse(PDataType.LONG
				.isCoercibleTo(PDataType.UNSIGNED_DOUBLE, -1L));
		assertTrue(PDataType.LONG.isCoercibleTo(PDataType.UNSIGNED_FLOAT, 10L));
		assertTrue(PDataType.LONG.isCoercibleTo(PDataType.UNSIGNED_FLOAT, 0L));
		assertFalse(PDataType.LONG.isCoercibleTo(PDataType.UNSIGNED_FLOAT, -1L));
        
        // Testing coercing smallint to other values.
        assertTrue(PDataType.SMALLINT.isCoercibleTo(PDataType.DOUBLE));
        assertTrue(PDataType.SMALLINT.isCoercibleTo(PDataType.FLOAT));
        assertTrue(PDataType.SMALLINT.isCoercibleTo(PDataType.LONG));
        assertTrue(PDataType.SMALLINT.isCoercibleTo(PDataType.LONG, (short)10));
        assertTrue(PDataType.SMALLINT.isCoercibleTo(PDataType.LONG, (short)0));
        assertTrue(PDataType.SMALLINT.isCoercibleTo(PDataType.LONG, (short)-10));
        assertTrue(PDataType.SMALLINT.isCoercibleTo(PDataType.INTEGER));
        assertTrue(PDataType.SMALLINT.isCoercibleTo(PDataType.INTEGER, (short)10));
        assertTrue(PDataType.SMALLINT.isCoercibleTo(PDataType.INTEGER, (short)0));
        assertTrue(PDataType.SMALLINT.isCoercibleTo(PDataType.INTEGER, (short)-10));
        assertFalse(PDataType.SMALLINT.isCoercibleTo(PDataType.UNSIGNED_INT));
        assertTrue(PDataType.SMALLINT.isCoercibleTo(PDataType.UNSIGNED_INT, (short)10));
        assertTrue(PDataType.SMALLINT.isCoercibleTo(PDataType.UNSIGNED_INT, (short)0));
        assertFalse(PDataType.SMALLINT.isCoercibleTo(PDataType.UNSIGNED_INT, (short)-10));
        assertFalse(PDataType.SMALLINT.isCoercibleTo(PDataType.UNSIGNED_LONG));
        assertTrue(PDataType.SMALLINT.isCoercibleTo(PDataType.UNSIGNED_LONG, (short)10));
        assertTrue(PDataType.SMALLINT.isCoercibleTo(PDataType.UNSIGNED_LONG, (short)0));
        assertFalse(PDataType.SMALLINT.isCoercibleTo(PDataType.UNSIGNED_LONG, (short)-10));
        assertFalse(PDataType.SMALLINT.isCoercibleTo(PDataType.TINYINT));
        assertTrue(PDataType.SMALLINT.isCoercibleTo(PDataType.TINYINT, (short)10));
        assertTrue(PDataType.SMALLINT.isCoercibleTo(PDataType.TINYINT, (short)0));
        assertTrue(PDataType.SMALLINT.isCoercibleTo(PDataType.TINYINT, (short)-10));
        assertFalse(PDataType.SMALLINT.isCoercibleTo(PDataType.TINYINT, (short)1000));
        assertFalse(PDataType.SMALLINT.isCoercibleTo(PDataType.UNSIGNED_SMALLINT));
        assertTrue(PDataType.SMALLINT.isCoercibleTo(PDataType.UNSIGNED_SMALLINT, (short)10));
        assertTrue(PDataType.SMALLINT.isCoercibleTo(PDataType.UNSIGNED_SMALLINT, (short)0));
        assertFalse(PDataType.SMALLINT.isCoercibleTo(PDataType.UNSIGNED_SMALLINT, (short)-10));
        assertFalse(PDataType.SMALLINT.isCoercibleTo(PDataType.UNSIGNED_TINYINT));
        assertTrue(PDataType.SMALLINT.isCoercibleTo(PDataType.UNSIGNED_TINYINT, (short)10));
        assertTrue(PDataType.SMALLINT.isCoercibleTo(PDataType.UNSIGNED_TINYINT, (short)0));
        assertFalse(PDataType.SMALLINT.isCoercibleTo(PDataType.UNSIGNED_TINYINT, (short)-10));
        assertFalse(PDataType.SMALLINT.isCoercibleTo(PDataType.UNSIGNED_TINYINT, (short)1000));
        assertTrue(PDataType.SMALLINT.isCoercibleTo(PDataType.UNSIGNED_DOUBLE, (short)10));
        assertTrue(PDataType.SMALLINT.isCoercibleTo(PDataType.UNSIGNED_DOUBLE, (short)0));
        assertFalse(PDataType.SMALLINT.isCoercibleTo(PDataType.UNSIGNED_DOUBLE, (short)-1));
        assertTrue(PDataType.SMALLINT.isCoercibleTo(PDataType.UNSIGNED_FLOAT, (short)10));
        assertTrue(PDataType.SMALLINT.isCoercibleTo(PDataType.UNSIGNED_FLOAT, (short)0));
        assertFalse(PDataType.SMALLINT.isCoercibleTo(PDataType.UNSIGNED_FLOAT, (short)-1));
        
        // Testing coercing tinyint to other values.
        assertTrue(PDataType.TINYINT.isCoercibleTo(PDataType.DOUBLE));
        assertTrue(PDataType.TINYINT.isCoercibleTo(PDataType.FLOAT));
        assertTrue(PDataType.TINYINT.isCoercibleTo(PDataType.LONG));
        assertTrue(PDataType.TINYINT.isCoercibleTo(PDataType.LONG, (byte)10));
        assertTrue(PDataType.TINYINT.isCoercibleTo(PDataType.LONG, (byte)0));
        assertTrue(PDataType.TINYINT.isCoercibleTo(PDataType.LONG, (byte)-10));
        assertTrue(PDataType.TINYINT.isCoercibleTo(PDataType.INTEGER));
        assertTrue(PDataType.TINYINT.isCoercibleTo(PDataType.INTEGER, (byte)10));
        assertTrue(PDataType.TINYINT.isCoercibleTo(PDataType.INTEGER, (byte)0));
        assertTrue(PDataType.TINYINT.isCoercibleTo(PDataType.INTEGER, (byte)-10));
        assertTrue(PDataType.TINYINT.isCoercibleTo(PDataType.SMALLINT));
        assertTrue(PDataType.TINYINT.isCoercibleTo(PDataType.SMALLINT, (byte)100));
        assertTrue(PDataType.TINYINT.isCoercibleTo(PDataType.SMALLINT, (byte)0));
        assertTrue(PDataType.TINYINT.isCoercibleTo(PDataType.SMALLINT, (byte)-10));
        assertFalse(PDataType.TINYINT.isCoercibleTo(PDataType.UNSIGNED_INT));
        assertTrue(PDataType.TINYINT.isCoercibleTo(PDataType.UNSIGNED_INT, (byte)10));
        assertTrue(PDataType.TINYINT.isCoercibleTo(PDataType.UNSIGNED_INT, (byte)0));
        assertFalse(PDataType.TINYINT.isCoercibleTo(PDataType.UNSIGNED_INT, (byte)-10));
        assertFalse(PDataType.TINYINT.isCoercibleTo(PDataType.UNSIGNED_LONG));
        assertTrue(PDataType.TINYINT.isCoercibleTo(PDataType.UNSIGNED_LONG, (byte)10));
        assertTrue(PDataType.TINYINT.isCoercibleTo(PDataType.UNSIGNED_LONG, (byte)0));
        assertFalse(PDataType.TINYINT.isCoercibleTo(PDataType.UNSIGNED_LONG, (byte)-10));
        assertFalse(PDataType.TINYINT.isCoercibleTo(PDataType.UNSIGNED_SMALLINT));
        assertTrue(PDataType.TINYINT.isCoercibleTo(PDataType.UNSIGNED_SMALLINT, (byte)10));
        assertTrue(PDataType.TINYINT.isCoercibleTo(PDataType.UNSIGNED_SMALLINT, (byte)0));
        assertFalse(PDataType.TINYINT.isCoercibleTo(PDataType.UNSIGNED_SMALLINT, (byte)-10));
        assertFalse(PDataType.TINYINT.isCoercibleTo(PDataType.UNSIGNED_TINYINT));
        assertTrue(PDataType.TINYINT.isCoercibleTo(PDataType.UNSIGNED_TINYINT, (byte)10));
        assertTrue(PDataType.TINYINT.isCoercibleTo(PDataType.UNSIGNED_TINYINT, (byte)0));
        assertFalse(PDataType.TINYINT.isCoercibleTo(PDataType.UNSIGNED_TINYINT, (byte)-10));
        assertTrue(PDataType.TINYINT.isCoercibleTo(PDataType.UNSIGNED_DOUBLE, (byte)10));
        assertTrue(PDataType.TINYINT.isCoercibleTo(PDataType.UNSIGNED_DOUBLE, (byte)0));
        assertFalse(PDataType.TINYINT.isCoercibleTo(PDataType.UNSIGNED_DOUBLE, (byte)-1));
        assertTrue(PDataType.TINYINT.isCoercibleTo(PDataType.UNSIGNED_FLOAT, (byte)10));
        assertTrue(PDataType.TINYINT.isCoercibleTo(PDataType.UNSIGNED_FLOAT, (byte)0));
        assertFalse(PDataType.TINYINT.isCoercibleTo(PDataType.UNSIGNED_FLOAT, (byte)-1));

        // Testing coercing unsigned_int to other values.
        assertTrue(PDataType.UNSIGNED_INT.isCoercibleTo(PDataType.DOUBLE));
        assertTrue(PDataType.UNSIGNED_INT.isCoercibleTo(PDataType.FLOAT));
        assertTrue(PDataType.UNSIGNED_INT.isCoercibleTo(PDataType.INTEGER));
        assertTrue(PDataType.UNSIGNED_INT.isCoercibleTo(PDataType.INTEGER, 10));
        assertTrue(PDataType.UNSIGNED_INT.isCoercibleTo(PDataType.INTEGER, 0));
        assertTrue(PDataType.UNSIGNED_INT.isCoercibleTo(PDataType.LONG));
        assertTrue(PDataType.UNSIGNED_INT.isCoercibleTo(PDataType.LONG, 10));
        assertTrue(PDataType.UNSIGNED_INT.isCoercibleTo(PDataType.LONG, 0));
        assertTrue(PDataType.UNSIGNED_INT.isCoercibleTo(PDataType.UNSIGNED_LONG));
        assertTrue(PDataType.UNSIGNED_INT.isCoercibleTo(PDataType.UNSIGNED_LONG, 10));
        assertTrue(PDataType.UNSIGNED_INT.isCoercibleTo(PDataType.UNSIGNED_LONG, 0));
        assertFalse(PDataType.UNSIGNED_INT.isCoercibleTo(PDataType.SMALLINT));
        assertTrue(PDataType.UNSIGNED_INT.isCoercibleTo(PDataType.SMALLINT, 10));
        assertTrue(PDataType.UNSIGNED_INT.isCoercibleTo(PDataType.SMALLINT, 0));
        assertFalse(PDataType.UNSIGNED_INT.isCoercibleTo(PDataType.TINYINT));
        assertTrue(PDataType.UNSIGNED_INT.isCoercibleTo(PDataType.TINYINT, 10));
        assertTrue(PDataType.UNSIGNED_INT.isCoercibleTo(PDataType.TINYINT, 0));
        assertFalse(PDataType.UNSIGNED_INT.isCoercibleTo(PDataType.UNSIGNED_SMALLINT));
        assertTrue(PDataType.UNSIGNED_INT.isCoercibleTo(PDataType.UNSIGNED_SMALLINT, 10));
        assertTrue(PDataType.UNSIGNED_INT.isCoercibleTo(PDataType.UNSIGNED_SMALLINT, 0));
        assertFalse(PDataType.UNSIGNED_INT.isCoercibleTo(PDataType.UNSIGNED_SMALLINT, 100000));
        assertFalse(PDataType.UNSIGNED_INT.isCoercibleTo(PDataType.UNSIGNED_TINYINT));
        assertTrue(PDataType.UNSIGNED_INT.isCoercibleTo(PDataType.UNSIGNED_TINYINT, 10));
        assertTrue(PDataType.UNSIGNED_INT.isCoercibleTo(PDataType.UNSIGNED_TINYINT, 0));
        assertFalse(PDataType.UNSIGNED_INT.isCoercibleTo(PDataType.UNSIGNED_TINYINT, 1000));
        assertTrue(PDataType.UNSIGNED_INT.isCoercibleTo(PDataType.UNSIGNED_DOUBLE));
        assertTrue(PDataType.UNSIGNED_INT.isCoercibleTo(PDataType.UNSIGNED_FLOAT));

        // Testing coercing unsigned_long to other values.
        assertTrue(PDataType.UNSIGNED_LONG.isCoercibleTo(PDataType.DOUBLE));
        assertFalse(PDataType.UNSIGNED_LONG.isCoercibleTo(PDataType.INTEGER));
        assertTrue(PDataType.UNSIGNED_LONG.isCoercibleTo(PDataType.INTEGER, 10L));
        assertTrue(PDataType.UNSIGNED_LONG.isCoercibleTo(PDataType.INTEGER, 0L));
        assertTrue(PDataType.UNSIGNED_LONG.isCoercibleTo(PDataType.LONG));
        assertFalse(PDataType.UNSIGNED_LONG.isCoercibleTo(PDataType.UNSIGNED_INT));
        assertFalse(PDataType.UNSIGNED_LONG.isCoercibleTo(PDataType.SMALLINT));
        assertTrue(PDataType.UNSIGNED_LONG.isCoercibleTo(PDataType.SMALLINT, 10L));
        assertTrue(PDataType.UNSIGNED_LONG.isCoercibleTo(PDataType.SMALLINT, 0L));
        assertFalse(PDataType.UNSIGNED_LONG.isCoercibleTo(PDataType.TINYINT));
        assertTrue(PDataType.UNSIGNED_LONG.isCoercibleTo(PDataType.TINYINT, 10L));
        assertTrue(PDataType.UNSIGNED_LONG.isCoercibleTo(PDataType.TINYINT, 0L));
        assertFalse(PDataType.UNSIGNED_LONG.isCoercibleTo(PDataType.UNSIGNED_SMALLINT));
        assertTrue(PDataType.UNSIGNED_LONG.isCoercibleTo(PDataType.UNSIGNED_SMALLINT, 10L));
        assertTrue(PDataType.UNSIGNED_LONG.isCoercibleTo(PDataType.UNSIGNED_SMALLINT, 0L));
        assertFalse(PDataType.UNSIGNED_LONG.isCoercibleTo(PDataType.UNSIGNED_SMALLINT, 100000L));
        assertFalse(PDataType.UNSIGNED_INT.isCoercibleTo(PDataType.UNSIGNED_TINYINT));
        assertTrue(PDataType.UNSIGNED_LONG.isCoercibleTo(PDataType.UNSIGNED_TINYINT, 10L));
        assertTrue(PDataType.UNSIGNED_LONG.isCoercibleTo(PDataType.UNSIGNED_TINYINT, 0L));
        assertFalse(PDataType.UNSIGNED_LONG.isCoercibleTo(PDataType.UNSIGNED_TINYINT, 1000L));
        assertTrue(PDataType.UNSIGNED_LONG.isCoercibleTo(PDataType.UNSIGNED_DOUBLE));
        
        // Testing coercing unsigned_smallint to other values.
        assertTrue(PDataType.UNSIGNED_SMALLINT.isCoercibleTo(PDataType.DOUBLE));
        assertTrue(PDataType.UNSIGNED_SMALLINT.isCoercibleTo(PDataType.FLOAT));
        assertTrue(PDataType.UNSIGNED_SMALLINT.isCoercibleTo(PDataType.INTEGER));
        assertTrue(PDataType.UNSIGNED_SMALLINT.isCoercibleTo(PDataType.INTEGER, (short)10));
        assertTrue(PDataType.UNSIGNED_SMALLINT.isCoercibleTo(PDataType.INTEGER, (short)0));
        assertTrue(PDataType.UNSIGNED_SMALLINT.isCoercibleTo(PDataType.LONG));
        assertTrue(PDataType.UNSIGNED_SMALLINT.isCoercibleTo(PDataType.LONG, (short)10));
        assertTrue(PDataType.UNSIGNED_SMALLINT.isCoercibleTo(PDataType.LONG, (short)0));
        assertTrue(PDataType.UNSIGNED_SMALLINT.isCoercibleTo(PDataType.UNSIGNED_LONG));
        assertTrue(PDataType.UNSIGNED_SMALLINT.isCoercibleTo(PDataType.UNSIGNED_LONG, (short)10));
        assertTrue(PDataType.UNSIGNED_SMALLINT.isCoercibleTo(PDataType.UNSIGNED_LONG, (short)0));
        assertTrue(PDataType.UNSIGNED_SMALLINT.isCoercibleTo(PDataType.UNSIGNED_INT));
        assertTrue(PDataType.UNSIGNED_SMALLINT.isCoercibleTo(PDataType.UNSIGNED_INT, (short)10));
        assertTrue(PDataType.UNSIGNED_SMALLINT.isCoercibleTo(PDataType.UNSIGNED_INT, (short)0));
        assertTrue(PDataType.UNSIGNED_SMALLINT.isCoercibleTo(PDataType.SMALLINT));
        assertTrue(PDataType.UNSIGNED_SMALLINT.isCoercibleTo(PDataType.SMALLINT, (short)10));
        assertTrue(PDataType.UNSIGNED_SMALLINT.isCoercibleTo(PDataType.SMALLINT, (short)0));
        assertFalse(PDataType.UNSIGNED_SMALLINT.isCoercibleTo(PDataType.TINYINT));
        assertTrue(PDataType.UNSIGNED_SMALLINT.isCoercibleTo(PDataType.TINYINT, (short)10));
        assertTrue(PDataType.UNSIGNED_SMALLINT.isCoercibleTo(PDataType.TINYINT, (short)0));
        assertFalse(PDataType.UNSIGNED_SMALLINT.isCoercibleTo(PDataType.TINYINT, (short)1000));
        assertFalse(PDataType.UNSIGNED_SMALLINT.isCoercibleTo(PDataType.UNSIGNED_TINYINT));
        assertTrue(PDataType.UNSIGNED_SMALLINT.isCoercibleTo(PDataType.UNSIGNED_TINYINT, (short)10));
        assertTrue(PDataType.UNSIGNED_SMALLINT.isCoercibleTo(PDataType.UNSIGNED_TINYINT, (short)0));
        assertFalse(PDataType.UNSIGNED_SMALLINT.isCoercibleTo(PDataType.UNSIGNED_TINYINT, (short)1000));
        assertTrue(PDataType.UNSIGNED_SMALLINT.isCoercibleTo(PDataType.UNSIGNED_DOUBLE));
        assertTrue(PDataType.UNSIGNED_SMALLINT.isCoercibleTo(PDataType.UNSIGNED_FLOAT));
        
        // Testing coercing unsigned_tinyint to other values.
        assertTrue(PDataType.UNSIGNED_TINYINT.isCoercibleTo(PDataType.DOUBLE));
        assertTrue(PDataType.UNSIGNED_TINYINT.isCoercibleTo(PDataType.FLOAT));
        assertTrue(PDataType.UNSIGNED_TINYINT.isCoercibleTo(PDataType.INTEGER));
        assertTrue(PDataType.UNSIGNED_TINYINT.isCoercibleTo(PDataType.INTEGER, (byte)10));
        assertTrue(PDataType.UNSIGNED_TINYINT.isCoercibleTo(PDataType.INTEGER, (byte)0));
        assertTrue(PDataType.UNSIGNED_TINYINT.isCoercibleTo(PDataType.LONG));
        assertTrue(PDataType.UNSIGNED_TINYINT.isCoercibleTo(PDataType.LONG, (byte)10));
        assertTrue(PDataType.UNSIGNED_TINYINT.isCoercibleTo(PDataType.LONG, (byte)0));
        assertTrue(PDataType.UNSIGNED_TINYINT.isCoercibleTo(PDataType.UNSIGNED_LONG));
        assertTrue(PDataType.UNSIGNED_TINYINT.isCoercibleTo(PDataType.UNSIGNED_LONG, (byte)10));
        assertTrue(PDataType.UNSIGNED_TINYINT.isCoercibleTo(PDataType.UNSIGNED_LONG, (byte)0));
        assertTrue(PDataType.UNSIGNED_TINYINT.isCoercibleTo(PDataType.UNSIGNED_INT));
        assertTrue(PDataType.UNSIGNED_TINYINT.isCoercibleTo(PDataType.UNSIGNED_INT, (byte)10));
        assertTrue(PDataType.UNSIGNED_TINYINT.isCoercibleTo(PDataType.UNSIGNED_INT, (byte)0));
        assertTrue(PDataType.UNSIGNED_TINYINT.isCoercibleTo(PDataType.SMALLINT));
        assertTrue(PDataType.UNSIGNED_TINYINT.isCoercibleTo(PDataType.SMALLINT, (byte)10));
        assertTrue(PDataType.UNSIGNED_TINYINT.isCoercibleTo(PDataType.SMALLINT, (byte)0));
        assertTrue(PDataType.UNSIGNED_TINYINT.isCoercibleTo(PDataType.TINYINT));
        assertTrue(PDataType.UNSIGNED_TINYINT.isCoercibleTo(PDataType.TINYINT, (byte)10));
        assertTrue(PDataType.UNSIGNED_TINYINT.isCoercibleTo(PDataType.TINYINT, (byte)0));
        assertTrue(PDataType.UNSIGNED_TINYINT.isCoercibleTo(PDataType.UNSIGNED_SMALLINT));
        assertTrue(PDataType.UNSIGNED_TINYINT.isCoercibleTo(PDataType.UNSIGNED_SMALLINT, (byte)10));
        assertTrue(PDataType.UNSIGNED_TINYINT.isCoercibleTo(PDataType.UNSIGNED_SMALLINT, (byte)0));
        assertTrue(PDataType.UNSIGNED_TINYINT.isCoercibleTo(PDataType.UNSIGNED_DOUBLE));
        assertTrue(PDataType.UNSIGNED_TINYINT.isCoercibleTo(PDataType.UNSIGNED_FLOAT));
        
        // Testing coercing Date types
        assertTrue(PDataType.DATE.isCoercibleTo(PDataType.TIMESTAMP));
        assertTrue(PDataType.DATE.isCoercibleTo(PDataType.TIME));
        assertFalse(PDataType.TIMESTAMP.isCoercibleTo(PDataType.DATE));
        assertFalse(PDataType.TIMESTAMP.isCoercibleTo(PDataType.TIME));
        assertTrue(PDataType.TIME.isCoercibleTo(PDataType.TIMESTAMP));
        assertTrue(PDataType.TIME.isCoercibleTo(PDataType.DATE));
    }

    @Test
    public void testGetDeicmalPrecisionAndScaleFromRawBytes() throws Exception {
        // Special case for 0.
        BigDecimal bd = new BigDecimal("0");
        byte[] b = PDataType.DECIMAL.toBytes(bd);
        int[] v = PDataType.getDecimalPrecisionAndScale(b, 0, b.length);
        assertEquals(0, v[0]);
        assertEquals(0, v[1]);

        BigDecimal[] bds = new BigDecimal[] {
                new BigDecimal("1"),
                new BigDecimal("0.11"),
                new BigDecimal("1.1"),
                new BigDecimal("11"),
                new BigDecimal("101"),
                new BigDecimal("10.1"),
                new BigDecimal("1.01"),
                new BigDecimal("0.101"),
                new BigDecimal("1001"),
                new BigDecimal("100.1"),
                new BigDecimal("10.01"),
                new BigDecimal("1.001"),
                new BigDecimal("0.1001"),
                new BigDecimal("10001"),
                new BigDecimal("1000.1"),
                new BigDecimal("100.01"),
                new BigDecimal("10.001"),
                new BigDecimal("1.0001"),
                new BigDecimal("0.10001"),
                new BigDecimal("100000000000000000000000000000"),
                new BigDecimal("1000000000000000000000000000000"),
                new BigDecimal("0.000000000000000000000000000001"),
                new BigDecimal("0.0000000000000000000000000000001"),
                new BigDecimal("111111111111111111111111111111"),
                new BigDecimal("1111111111111111111111111111111"),
                new BigDecimal("0.111111111111111111111111111111"),
                new BigDecimal("0.1111111111111111111111111111111"),
        };

        for (int i=0; i<bds.length; i++) {
            testReadDecimalPrecisionAndScaleFromRawBytes(bds[i]);
            testReadDecimalPrecisionAndScaleFromRawBytes(bds[i].negate());
        }
        
        assertTrue(new BigDecimal("5").remainder(BigDecimal.ONE).equals(BigDecimal.ZERO));
        assertTrue(new BigDecimal("5.0").remainder(BigDecimal.ONE).compareTo(BigDecimal.ZERO)==0);
        assertTrue(new BigDecimal("5.00").remainder(BigDecimal.ONE).compareTo(BigDecimal.ZERO)==0);
        assertFalse(new BigDecimal("5.01").remainder(BigDecimal.ONE).equals(BigDecimal.ZERO));
        assertFalse(new BigDecimal("-5.1").remainder(BigDecimal.ONE).equals(BigDecimal.ZERO));
    }
    
    @Test
    public void testDateConversions() {
        long now = System.currentTimeMillis();
        Date date = new Date(now);
        Time t = new Time(now);
        Timestamp ts = new Timestamp(now);
        
        Object o = PDataType.DATE.toObject(ts, PDataType.TIMESTAMP);
        assertEquals(o.getClass(), java.sql.Date.class);
        o = PDataType.DATE.toObject(t, PDataType.TIME);
        assertEquals(o.getClass(), java.sql.Date.class);
        
        o = PDataType.TIME.toObject(date, PDataType.DATE);
        assertEquals(o.getClass(), java.sql.Time.class);
        o = PDataType.TIME.toObject(ts, PDataType.TIMESTAMP);
        assertEquals(o.getClass(), java.sql.Time.class);
                
        o = PDataType.TIMESTAMP.toObject(date, PDataType.DATE);
        assertEquals(o.getClass(), java.sql.Timestamp.class);
        o = PDataType.TIMESTAMP.toObject(t, PDataType.TIME);
        assertEquals(o.getClass(), java.sql.Timestamp.class); 
    }

    @Test
    public void testNegativeDateTime() {
        Date date1 = new Date(-1000);
        Date date2 = new Date(-2000);
        assertTrue(date1.compareTo(date2) > 0);
        
        byte[] b1 = PDataType.DATE.toBytes(date1);
        byte[] b2 = PDataType.DATE.toBytes(date2);
        assertTrue(Bytes.compareTo(b1, b2) > 0);
        
    }
    
    @Test
    public void testIllegalUnsignedDateTime() {
        Date date1 = new Date(-1000);
        try {
            PDataType.UNSIGNED_DATE.toBytes(date1);
            fail();
        } catch (IllegalDataException e) {
            
        }
    }

    @Test
    public void testGetResultSetSqlType() {
        assertEquals(Types.INTEGER, PDataType.INTEGER.getResultSetSqlType());
        assertEquals(Types.INTEGER, PDataType.UNSIGNED_INT.getResultSetSqlType());
        assertEquals(Types.BIGINT, PDataType.LONG.getResultSetSqlType());
        assertEquals(Types.BIGINT, PDataType.UNSIGNED_LONG.getResultSetSqlType());
        assertEquals(Types.SMALLINT, PDataType.SMALLINT.getResultSetSqlType());
        assertEquals(Types.SMALLINT, PDataType.UNSIGNED_SMALLINT.getResultSetSqlType());
        assertEquals(Types.TINYINT, PDataType.TINYINT.getResultSetSqlType());
        assertEquals(Types.TINYINT, PDataType.UNSIGNED_TINYINT.getResultSetSqlType());
        assertEquals(Types.FLOAT, PDataType.FLOAT.getResultSetSqlType());
        assertEquals(Types.FLOAT, PDataType.UNSIGNED_FLOAT.getResultSetSqlType());
        assertEquals(Types.DOUBLE, PDataType.DOUBLE.getResultSetSqlType());
        assertEquals(Types.DOUBLE, PDataType.UNSIGNED_DOUBLE.getResultSetSqlType());

        // Check that all array types are defined as java.sql.Types.ARRAY
        for (PDataType dataType : PDataType.values()) {
            if (dataType.isArrayType()) {
                assertEquals("Wrong datatype for " + dataType,
                        Types.ARRAY,
                        dataType.getResultSetSqlType());
            }
        }
    }

    private void testReadDecimalPrecisionAndScaleFromRawBytes(BigDecimal bd) {
        byte[] b = PDataType.DECIMAL.toBytes(bd);
        int[] v = PDataType.getDecimalPrecisionAndScale(b, 0, b.length);
        assertEquals(bd.toString(), bd.precision(), v[0]);
        assertEquals(bd.toString(), bd.scale(), v[1]);
    }

    @Test
    public void testArithmeticOnLong() {
        long startWith = -5;
        long incrementBy = 1;
        for (int i = 0; i < 10; i++) {
            long next = nextValueFor(startWith, incrementBy);
            assertEquals(startWith + incrementBy, next);
            startWith = next;
        }
        startWith = 5;
        incrementBy = -1;
        for (int i = 0; i < 10; i++) {
            long next = nextValueFor(startWith, incrementBy);
            assertEquals(startWith + incrementBy, next);
            startWith = next;
        }
        startWith = 0;
        incrementBy = 100;
        for (int i = 0; i < 10; i++) {
            long next = nextValueFor(startWith, incrementBy);
            assertEquals(startWith + incrementBy, next);
            startWith = next;
        }
    }

    // Simulate what an HBase Increment does with the value encoded as a long
    private long nextValueFor(long startWith, long incrementBy) {
        long hstartWith = Bytes.toLong(PDataType.LONG.toBytes(startWith));
        hstartWith += incrementBy;
        return (Long)PDataType.LONG.toObject(Bytes.toBytes(hstartWith));
    }
    
}