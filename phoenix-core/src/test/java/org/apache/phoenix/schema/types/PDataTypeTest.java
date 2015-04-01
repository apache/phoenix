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
package org.apache.phoenix.schema.types;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.schema.ConstraintViolationException;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.util.TestUtil;
import org.junit.Test;

import com.google.common.collect.TreeMultimap;


public class PDataTypeTest {
    @Test
    public void testFloatToLongComparison() {
        // Basic tests
        assertTrue(PFloat.INSTANCE.compareTo(PFloat.INSTANCE.toBytes(1e100), 0, PFloat.INSTANCE.getByteSize(), SortOrder
            .getDefault(),
                PLong.INSTANCE.toBytes(1), 0, PLong.INSTANCE.getByteSize(), SortOrder.getDefault(), PLong.INSTANCE) > 0);
        assertTrue(PFloat.INSTANCE.compareTo(PFloat.INSTANCE.toBytes(0.001), 0, PFloat.INSTANCE.getByteSize(), SortOrder.getDefault(),
                PLong.INSTANCE.toBytes(1), 0, PLong.INSTANCE.getByteSize(), SortOrder.getDefault(), PLong.INSTANCE) < 0);

        // Edge tests
        assertTrue(PFloat.INSTANCE.compareTo(PFloat.INSTANCE.toBytes(Integer.MAX_VALUE), 0,
                PFloat.INSTANCE.getByteSize(), SortOrder.getDefault(), PLong.INSTANCE.toBytes(Integer.MAX_VALUE - 1), 0,
                PLong.INSTANCE.getByteSize(), SortOrder.getDefault(), PLong.INSTANCE) > 0);
        assertTrue(PFloat.INSTANCE.compareTo(PFloat.INSTANCE.toBytes(Integer.MIN_VALUE), 0,
                PFloat.INSTANCE.getByteSize(), SortOrder.getDefault(), PLong.INSTANCE.toBytes(Integer.MIN_VALUE + 1), 0,
                PLong.INSTANCE.getByteSize(), SortOrder.getDefault(), PLong.INSTANCE) < 0);
        assertTrue(PFloat.INSTANCE.compareTo(PFloat.INSTANCE.toBytes(Integer.MIN_VALUE), 0,
                PFloat.INSTANCE.getByteSize(), SortOrder.getDefault(), PLong.INSTANCE.toBytes(Integer.MIN_VALUE), 0,
                PLong.INSTANCE.getByteSize(), SortOrder.getDefault(), PLong.INSTANCE) == 0);
        assertTrue(PFloat.INSTANCE.compareTo(PFloat.INSTANCE.toBytes(Integer.MAX_VALUE + 1.0F), 0,
                PFloat.INSTANCE.getByteSize(), SortOrder.getDefault(), PLong.INSTANCE.toBytes(Integer.MAX_VALUE), 0,
                PLong.INSTANCE.getByteSize(), SortOrder.getDefault(), PLong.INSTANCE) > 0); // Passes due to rounding
        assertTrue(PFloat.INSTANCE.compareTo(PFloat.INSTANCE.toBytes(Integer.MAX_VALUE + 129.0F), 0,
                PFloat.INSTANCE.getByteSize(), SortOrder.getDefault(), PLong.INSTANCE.toBytes(Integer.MAX_VALUE), 0,
                PLong.INSTANCE.getByteSize(), SortOrder.getDefault(), PLong.INSTANCE) > 0);
        assertTrue(PFloat.INSTANCE.compareTo(PFloat.INSTANCE.toBytes(Integer.MIN_VALUE - 128.0F), 0,
                PFloat.INSTANCE.getByteSize(), SortOrder.getDefault(), PLong.INSTANCE.toBytes(Integer.MIN_VALUE), 0,
                PLong.INSTANCE.getByteSize(), SortOrder.getDefault(), PLong.INSTANCE) == 0);
        assertTrue(PFloat.INSTANCE.compareTo(PFloat.INSTANCE.toBytes(Integer.MIN_VALUE - 129.0F), 0,
                PFloat.INSTANCE.getByteSize(), SortOrder.getDefault(), PLong.INSTANCE.toBytes(Integer.MIN_VALUE), 0,
                PLong.INSTANCE.getByteSize(), SortOrder.getDefault(), PLong.INSTANCE) < 0);

        float f1 = 9111111111111111.0F;
        float f2 = 9111111111111112.0F;
        assertTrue(f1 == f2);
        long la = 9111111111111111L;
        assertTrue(f1 > Integer.MAX_VALUE);
        assertTrue(la == f1);
        assertTrue(la == f2);
        assertTrue(PFloat.INSTANCE.compareTo(PFloat.INSTANCE.toBytes(f1), 0, PFloat.INSTANCE.getByteSize(), SortOrder.getDefault(),
                PLong.INSTANCE.toBytes(la), 0, PLong.INSTANCE.getByteSize(), SortOrder.getDefault(), PLong.INSTANCE) == 0);
        assertTrue(PFloat.INSTANCE.compareTo(PFloat.INSTANCE.toBytes(f2), 0, PFloat.INSTANCE.getByteSize(), SortOrder.getDefault(),
                PLong.INSTANCE.toBytes(la), 0, PLong.INSTANCE.getByteSize(), SortOrder.getDefault(), PLong.INSTANCE) == 0);

        // Same as above, but reversing LHS and RHS
        assertTrue(PLong.INSTANCE.compareTo(PLong.INSTANCE.toBytes(1), 0, PLong.INSTANCE.getByteSize(), SortOrder.getDefault(),
                PFloat.INSTANCE.toBytes(1e100), 0, PFloat.INSTANCE.getByteSize(), SortOrder.getDefault(), PFloat.INSTANCE) < 0);
        assertTrue(PLong.INSTANCE.compareTo(PLong.INSTANCE.toBytes(1), 0, PLong.INSTANCE.getByteSize(), SortOrder.getDefault(),
                PFloat.INSTANCE.toBytes(0.001), 0, PFloat.INSTANCE.getByteSize(), SortOrder.getDefault(), PFloat.INSTANCE) > 0);

        assertTrue(PLong.INSTANCE.compareTo(PLong.INSTANCE.toBytes(Integer.MAX_VALUE - 1), 0,
                PLong.INSTANCE.getByteSize(), SortOrder.getDefault(), PFloat.INSTANCE.toBytes(Integer.MAX_VALUE), 0,
                PFloat.INSTANCE.getByteSize(), SortOrder.getDefault(), PFloat.INSTANCE) < 0);
        assertTrue(PLong.INSTANCE.compareTo(PLong.INSTANCE.toBytes(Integer.MIN_VALUE + 1), 0,
                PLong.INSTANCE.getByteSize(), SortOrder.getDefault(), PFloat.INSTANCE.toBytes(Integer.MIN_VALUE), 0,
                PFloat.INSTANCE.getByteSize(), SortOrder.getDefault(), PFloat.INSTANCE) > 0);
        assertTrue(PLong.INSTANCE.compareTo(PLong.INSTANCE.toBytes(Integer.MIN_VALUE), 0, PLong.INSTANCE.getByteSize(),
        		SortOrder.getDefault(), PFloat.INSTANCE.toBytes(Integer.MIN_VALUE), 0, PFloat.INSTANCE.getByteSize(), SortOrder.getDefault(),
                PFloat.INSTANCE) == 0);
        assertTrue(PLong.INSTANCE.compareTo(PLong.INSTANCE.toBytes(Integer.MAX_VALUE), 0, PLong.INSTANCE.getByteSize(),
        		SortOrder.getDefault(), PFloat.INSTANCE.toBytes(Integer.MAX_VALUE + 1.0F), 0, PFloat.INSTANCE.getByteSize(), SortOrder.getDefault(),
                PFloat.INSTANCE) < 0); // Passes due to rounding
        assertTrue(PLong.INSTANCE.compareTo(PLong.INSTANCE.toBytes(Integer.MAX_VALUE), 0, PLong.INSTANCE.getByteSize(),
        		SortOrder.getDefault(), PFloat.INSTANCE.toBytes(Integer.MAX_VALUE + 129.0F), 0, PFloat.INSTANCE.getByteSize(), SortOrder.getDefault(),
                PFloat.INSTANCE) < 0);
        assertTrue(PLong.INSTANCE.compareTo(PLong.INSTANCE.toBytes(Integer.MIN_VALUE), 0, PLong.INSTANCE.getByteSize(),
        		SortOrder.getDefault(), PFloat.INSTANCE.toBytes(Integer.MIN_VALUE - 128.0F), 0, PFloat.INSTANCE.getByteSize(), SortOrder.getDefault(),
                PFloat.INSTANCE) == 0);
        assertTrue(PLong.INSTANCE.compareTo(PLong.INSTANCE.toBytes(Integer.MIN_VALUE), 0, PLong.INSTANCE.getByteSize(),
        		SortOrder.getDefault(), PFloat.INSTANCE.toBytes(Integer.MIN_VALUE - 129.0F), 0, PFloat.INSTANCE.getByteSize(), SortOrder.getDefault(),
                PFloat.INSTANCE) > 0);

        assertTrue(PLong.INSTANCE.compareTo(PLong.INSTANCE.toBytes(la), 0, PLong.INSTANCE.getByteSize(), SortOrder.getDefault(),
                PFloat.INSTANCE.toBytes(f1), 0, PFloat.INSTANCE.getByteSize(), SortOrder.getDefault(), PFloat.INSTANCE) == 0);
        assertTrue(PLong.INSTANCE.compareTo(PLong.INSTANCE.toBytes(la), 0, PLong.INSTANCE.getByteSize(), SortOrder.getDefault(),
                PFloat.INSTANCE.toBytes(f2), 0, PFloat.INSTANCE.getByteSize(), SortOrder.getDefault(), PFloat.INSTANCE) == 0);
    }        
        
    @Test
    public void testDoubleToDecimalComparison() {
        // Basic tests
        assertTrue(PDouble.INSTANCE.compareTo(PDouble.INSTANCE.toBytes(1.23), 0, PDouble.INSTANCE.getByteSize(), SortOrder.getDefault(),
                   PDecimal.INSTANCE.toBytes(BigDecimal.valueOf(1.24)), 0, PDecimal.INSTANCE.getByteSize(), SortOrder.getDefault(), PDecimal.INSTANCE) < 0);
    }
    
    @Test
    public void testDoubleToLongComparison() {
        // Basic tests
        assertTrue(PDouble.INSTANCE.compareTo(PDouble.INSTANCE.toBytes(-1e100), 0, PDouble.INSTANCE.getByteSize(), SortOrder.getDefault(),
                PLong.INSTANCE.toBytes(1), 0, PLong.INSTANCE.getByteSize(), SortOrder.getDefault(), PLong.INSTANCE) < 0);
        assertTrue(PDouble.INSTANCE.compareTo(PDouble.INSTANCE.toBytes(0.001), 0, PDouble.INSTANCE.getByteSize(), SortOrder.getDefault(),
                PLong.INSTANCE.toBytes(1), 0, PLong.INSTANCE.getByteSize(), SortOrder.getDefault(), PLong.INSTANCE) < 0);

        assertTrue(PDouble.INSTANCE.compareTo(PDouble.INSTANCE.toBytes(Long.MAX_VALUE), 0,
                PDouble.INSTANCE.getByteSize(), SortOrder.getDefault(), PLong.INSTANCE.toBytes(Long.MAX_VALUE - 1), 0,
                PLong.INSTANCE.getByteSize(), SortOrder.getDefault(), PLong.INSTANCE) > 0);
        assertTrue(PDouble.INSTANCE.compareTo(PDouble.INSTANCE.toBytes(Long.MIN_VALUE), 0,
                PDouble.INSTANCE.getByteSize(), SortOrder.getDefault(), PLong.INSTANCE.toBytes(Long.MIN_VALUE + 1), 0,
                PLong.INSTANCE.getByteSize(), SortOrder.getDefault(), PLong.INSTANCE) < 0);
        assertTrue(PDouble.INSTANCE.compareTo(PDouble.INSTANCE.toBytes(Long.MIN_VALUE), 0,
                PDouble.INSTANCE.getByteSize(), SortOrder.getDefault(), PLong.INSTANCE.toBytes(Long.MIN_VALUE), 0,
                PLong.INSTANCE.getByteSize(), SortOrder.getDefault(), PLong.INSTANCE) == 0);
        assertTrue(PDouble.INSTANCE.compareTo(PDouble.INSTANCE.toBytes(Long.MAX_VALUE + 1024.0), 0,
                PDouble.INSTANCE.getByteSize(), SortOrder.getDefault(), PLong.INSTANCE.toBytes(Long.MAX_VALUE), 0,
                PLong.INSTANCE.getByteSize(), SortOrder.getDefault(), PLong.INSTANCE) == 0);
        assertTrue(PDouble.INSTANCE.compareTo(PDouble.INSTANCE.toBytes(Long.MAX_VALUE + 1025.0), 0,
                PDouble.INSTANCE.getByteSize(), SortOrder.getDefault(), PLong.INSTANCE.toBytes(Long.MAX_VALUE), 0,
                PLong.INSTANCE.getByteSize(), SortOrder.getDefault(), PLong.INSTANCE) > 0);
        assertTrue(PDouble.INSTANCE.compareTo(PDouble.INSTANCE.toBytes(Long.MIN_VALUE - 1024.0), 0,
                PDouble.INSTANCE.getByteSize(), SortOrder.getDefault(), PLong.INSTANCE.toBytes(Long.MIN_VALUE), 0,
                PLong.INSTANCE.getByteSize(), SortOrder.getDefault(), PLong.INSTANCE) == 0);
        assertTrue(PDouble.INSTANCE.compareTo(PDouble.INSTANCE.toBytes(Long.MIN_VALUE - 1025.0), 0,
                PDouble.INSTANCE.getByteSize(), SortOrder.getDefault(), PLong.INSTANCE.toBytes(Long.MIN_VALUE), 0,
                PLong.INSTANCE.getByteSize(), SortOrder.getDefault(), PLong.INSTANCE) < 0);

        // Same as above, but reversing LHS and RHS
        assertTrue(PLong.INSTANCE.compareTo(PLong.INSTANCE.toBytes(1), 0, PLong.INSTANCE.getByteSize(), SortOrder.getDefault(),
                PDouble.INSTANCE.toBytes(-1e100), 0, PDouble.INSTANCE.getByteSize(), SortOrder.getDefault(), PDouble.INSTANCE) > 0);
        assertTrue(PLong.INSTANCE.compareTo(PLong.INSTANCE.toBytes(1), 0, PLong.INSTANCE.getByteSize(), SortOrder.getDefault(),
                PDouble.INSTANCE.toBytes(0.001), 0, PDouble.INSTANCE.getByteSize(), SortOrder.getDefault(), PDouble.INSTANCE) > 0);

        assertTrue(PLong.INSTANCE.compareTo(PLong.INSTANCE.toBytes(Long.MAX_VALUE - 1), 0,
                PLong.INSTANCE.getByteSize(), SortOrder.getDefault(), PDouble.INSTANCE.toBytes(Long.MAX_VALUE), 0,
                PDouble.INSTANCE.getByteSize(), SortOrder.getDefault(), PDouble.INSTANCE) < 0);
        assertTrue(PLong.INSTANCE.compareTo(PLong.INSTANCE.toBytes(Long.MIN_VALUE + 1), 0,
                PLong.INSTANCE.getByteSize(), SortOrder.getDefault(), PDouble.INSTANCE.toBytes(Long.MIN_VALUE), 0,
                PDouble.INSTANCE.getByteSize(), SortOrder.getDefault(), PDouble.INSTANCE) > 0);
        assertTrue(PLong.INSTANCE.compareTo(PLong.INSTANCE.toBytes(Long.MIN_VALUE), 0, PLong.INSTANCE.getByteSize(),
        		SortOrder.getDefault(), PDouble.INSTANCE.toBytes(Long.MIN_VALUE), 0, PDouble.INSTANCE.getByteSize(), SortOrder.getDefault(),
                PDouble.INSTANCE) == 0);
        assertTrue(PLong.INSTANCE.compareTo(PLong.INSTANCE.toBytes(Long.MAX_VALUE), 0, PLong.INSTANCE.getByteSize(),
        		SortOrder.getDefault(), PDouble.INSTANCE.toBytes(Long.MAX_VALUE + 1024.0), 0, PDouble.INSTANCE.getByteSize(), SortOrder.getDefault(),
                PDouble.INSTANCE) == 0);
        assertTrue(PLong.INSTANCE.compareTo(PLong.INSTANCE.toBytes(Long.MAX_VALUE), 0, PLong.INSTANCE.getByteSize(),
        		SortOrder.getDefault(), PDouble.INSTANCE.toBytes(Long.MAX_VALUE + 1025.0), 0, PDouble.INSTANCE.getByteSize(), SortOrder.getDefault(),
                PDouble.INSTANCE) < 0);
        assertTrue(PLong.INSTANCE.compareTo(PLong.INSTANCE.toBytes(Long.MIN_VALUE), 0, PLong.INSTANCE.getByteSize(),
        		SortOrder.getDefault(), PDouble.INSTANCE.toBytes(Long.MIN_VALUE - 1024.0), 0, PDouble.INSTANCE.getByteSize(), SortOrder.getDefault(),
                PDouble.INSTANCE) == 0);
        assertTrue(PLong.INSTANCE.compareTo(PLong.INSTANCE.toBytes(Long.MIN_VALUE), 0, PLong.INSTANCE.getByteSize(),
        		SortOrder.getDefault(), PDouble.INSTANCE.toBytes(Long.MIN_VALUE - 1025.0), 0, PDouble.INSTANCE.getByteSize(), SortOrder.getDefault(),
                PDouble.INSTANCE) > 0);

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
        byte[] b = PLong.INSTANCE.toBytes(la);
        Long lb = (Long) PLong.INSTANCE.toObject(b);
        assertEquals(la,lb);

        Long na = 1L;
        Long nb = -1L;
        byte[] ba = PLong.INSTANCE.toBytes(na);
        byte[] bb = PLong.INSTANCE.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) > 0);
        
        Integer value = 100;
        Object obj = PLong.INSTANCE.toObject(value, PInteger.INSTANCE);
        assertTrue(obj instanceof Long);
        assertEquals(100, ((Long)obj).longValue());
        
        Long longValue = 100l;
        Object longObj = PLong.INSTANCE.toObject(longValue, PLong.INSTANCE);
        assertTrue(longObj instanceof Long);
        assertEquals(100, ((Long)longObj).longValue());
        
        assertEquals(0, PLong.INSTANCE.compareTo(Long.MAX_VALUE, Float.valueOf(Long.MAX_VALUE), PFloat.INSTANCE));
        assertEquals(0, PLong.INSTANCE.compareTo(Long.MAX_VALUE, Double.valueOf(Long.MAX_VALUE), PDouble.INSTANCE));
        assertEquals(-1, PLong.INSTANCE.compareTo(99, Float.valueOf(100), PFloat.INSTANCE));
        assertEquals(1, PLong.INSTANCE.compareTo(101, Float.valueOf(100), PFloat.INSTANCE));
        
        Double d = -2.0;
        Object lo = PLong.INSTANCE.toObject(d, PDouble.INSTANCE);
        assertEquals(-2L, ((Long)lo).longValue());
        
        byte[] bytes = PDouble.INSTANCE.toBytes(d);
        lo = PLong.INSTANCE.toObject(bytes,0, bytes.length, PDouble.INSTANCE);
        assertEquals(-2L, ((Long)lo).longValue());
        
        Float f = -2.0f;
        lo = PLong.INSTANCE.toObject(f, PFloat.INSTANCE);
        assertEquals(-2L, ((Long)lo).longValue());
        
        bytes = PFloat.INSTANCE.toBytes(f);
        lo = PLong.INSTANCE.toObject(bytes,0, bytes.length, PFloat.INSTANCE);
        assertEquals(-2L, ((Long)lo).longValue());
        
        // Checks for unsignedlong
        d = 2.0;
        lo = PUnsignedLong.INSTANCE.toObject(d, PDouble.INSTANCE);
        assertEquals(2L, ((Long)lo).longValue());
        
        bytes = PDouble.INSTANCE.toBytes(d);
        lo = PUnsignedLong.INSTANCE.toObject(bytes,0, bytes.length, PDouble.INSTANCE);
        assertEquals(2L, ((Long)lo).longValue());
        
        f = 2.0f;
        lo = PUnsignedLong.INSTANCE.toObject(f, PFloat.INSTANCE);
        assertEquals(2L, ((Long)lo).longValue());
        
        bytes = PFloat.INSTANCE.toBytes(f);
        lo = PUnsignedLong.INSTANCE.toObject(bytes,0, bytes.length, PFloat.INSTANCE);
        assertEquals(2L, ((Long)lo).longValue());
        
    }

    @Test
    public void testInt() {
        Integer na = 4;
        byte[] b = PInteger.INSTANCE.toBytes(na);
        Integer nb = (Integer) PInteger.INSTANCE.toObject(b);
        assertEquals(na,nb);

        na = 1;
        nb = -1;
        byte[] ba = PInteger.INSTANCE.toBytes(na);
        byte[] bb = PInteger.INSTANCE.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) > 0);

        na = -1;
        nb = -3;
        ba = PInteger.INSTANCE.toBytes(na);
        bb = PInteger.INSTANCE.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) > 0);

        na = -3;
        nb = -100000000;
        ba = PInteger.INSTANCE.toBytes(na);
        bb = PInteger.INSTANCE.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) > 0);
        
        Long value = 100l;
        Object obj = PInteger.INSTANCE.toObject(value, PLong.INSTANCE);
        assertTrue(obj instanceof Integer);
        assertEquals(100, ((Integer)obj).intValue());
        
        Float unsignedFloatValue = 100f;
        Object unsignedFloatObj = PInteger.INSTANCE.toObject(unsignedFloatValue, PUnsignedFloat.INSTANCE);
        assertTrue(unsignedFloatObj instanceof Integer);
        assertEquals(100, ((Integer)unsignedFloatObj).intValue());
        
        Double unsignedDoubleValue = 100d;
        Object unsignedDoubleObj = PInteger.INSTANCE.toObject(unsignedDoubleValue, PUnsignedDouble.INSTANCE);
        assertTrue(unsignedDoubleObj instanceof Integer);
        assertEquals(100, ((Integer)unsignedDoubleObj).intValue());
        
        Float floatValue = 100f;
        Object floatObj = PInteger.INSTANCE.toObject(floatValue, PFloat.INSTANCE);
        assertTrue(floatObj instanceof Integer);
        assertEquals(100, ((Integer)floatObj).intValue());
        
        Double doubleValue = 100d;
        Object doubleObj = PInteger.INSTANCE.toObject(doubleValue, PDouble.INSTANCE);
        assertTrue(doubleObj instanceof Integer);
        assertEquals(100, ((Integer)doubleObj).intValue());
        
        Short shortValue = 100;
        Object shortObj = PInteger.INSTANCE.toObject(shortValue, PSmallint.INSTANCE);
        assertTrue(shortObj instanceof Integer);
        assertEquals(100, ((Integer)shortObj).intValue());
    }
    
    @Test
    public void testSmallInt() {
        Short na = 4;
        byte[] b = PSmallint.INSTANCE.toBytes(na);
        Short nb = (Short) PSmallint.INSTANCE.toObject(b);
        assertEquals(na,nb);
        
        na = 4;
        b = PSmallint.INSTANCE.toBytes(na, SortOrder.DESC);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        ptr.set(b);
        nb = PSmallint.INSTANCE.getCodec().decodeShort(ptr, SortOrder.DESC);
        assertEquals(na,nb);

        na = 1;
        nb = -1;
        byte[] ba = PSmallint.INSTANCE.toBytes(na);
        byte[] bb = PSmallint.INSTANCE.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) > 0);

        na = -1;
        nb = -3;
        ba = PSmallint.INSTANCE.toBytes(na);
        bb = PSmallint.INSTANCE.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) > 0);

        na = -3;
        nb = -10000;
        ba = PSmallint.INSTANCE.toBytes(na);
        bb = PSmallint.INSTANCE.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) > 0);
        
        Integer value = 100;
        Object obj = PSmallint.INSTANCE.toObject(value, PInteger.INSTANCE);
        assertTrue(obj instanceof Short);
        assertEquals(100, ((Short)obj).shortValue());
        
        Float unsignedFloatValue = 100f;
        Object unsignedFloatObj = PSmallint.INSTANCE.toObject(unsignedFloatValue, PUnsignedFloat.INSTANCE);
        assertTrue(unsignedFloatObj instanceof Short);
        assertEquals(100, ((Short)unsignedFloatObj).shortValue());
        
        Double unsignedDoubleValue = 100d;
        Object unsignedDoubleObj = PSmallint.INSTANCE.toObject(unsignedDoubleValue, PUnsignedDouble.INSTANCE);
        assertTrue(unsignedDoubleObj instanceof Short);
        assertEquals(100, ((Short)unsignedDoubleObj).shortValue());
        
        Float floatValue = 100f;
        Object floatObj = PSmallint.INSTANCE.toObject(floatValue, PFloat.INSTANCE);
        assertTrue(floatObj instanceof Short);
        assertEquals(100, ((Short)floatObj).shortValue());
        
        Double doubleValue = 100d;
        Object doubleObj = PSmallint.INSTANCE.toObject(doubleValue, PDouble.INSTANCE);
        assertTrue(doubleObj instanceof Short);
        assertEquals(100, ((Short)doubleObj).shortValue());
    }
    
    @Test
    public void testTinyInt() {
        Byte na = 4;
        byte[] b = PTinyint.INSTANCE.toBytes(na);
        Byte nb = (Byte) PTinyint.INSTANCE.toObject(b);
        assertEquals(na,nb);

        na = 1;
        nb = -1;
        byte[] ba = PTinyint.INSTANCE.toBytes(na);
        byte[] bb = PTinyint.INSTANCE.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) > 0);

        na = -1;
        nb = -3;
        ba = PTinyint.INSTANCE.toBytes(na);
        bb = PTinyint.INSTANCE.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) > 0);

        na = -3;
        nb = -100;
        ba = PTinyint.INSTANCE.toBytes(na);
        bb = PTinyint.INSTANCE.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) > 0);
        
        Integer value = 100;
        Object obj = PTinyint.INSTANCE.toObject(value, PInteger.INSTANCE);
        assertTrue(obj instanceof Byte);
        assertEquals(100, ((Byte)obj).byteValue());
        
        Float floatValue = 100f;
        Object floatObj = PTinyint.INSTANCE.toObject(floatValue, PFloat.INSTANCE);
        assertTrue(floatObj instanceof Byte);
        assertEquals(100, ((Byte)floatObj).byteValue());
        
        Float unsignedFloatValue = 100f;
        Object unsignedFloatObj = PTinyint.INSTANCE.toObject(unsignedFloatValue, PUnsignedFloat.INSTANCE);
        assertTrue(unsignedFloatObj instanceof Byte);
        assertEquals(100, ((Byte)unsignedFloatObj).byteValue());
        
        Double unsignedDoubleValue = 100d;
        Object unsignedDoubleObj = PTinyint.INSTANCE.toObject(unsignedDoubleValue, PUnsignedDouble.INSTANCE);
        assertTrue(unsignedDoubleObj instanceof Byte);
        assertEquals(100, ((Byte)unsignedDoubleObj).byteValue());
        
        Double doubleValue = 100d;
        Object doubleObj = PTinyint.INSTANCE.toObject(doubleValue, PDouble.INSTANCE);
        assertTrue(doubleObj instanceof Byte);
        assertEquals(100, ((Byte)doubleObj).byteValue());

        assertTrue(PTinyint.INSTANCE.isCoercibleTo(PTinyint.INSTANCE, (byte) -1));
    }
    
    @Test
    public void testUnsignedSmallInt() {
        Short na = 4;
        byte[] b = PUnsignedSmallint.INSTANCE.toBytes(na);
        Short nb = (Short) PUnsignedSmallint.INSTANCE.toObject(b);
        assertEquals(na,nb);

        na = 10;
        nb = 8;
        byte[] ba = PUnsignedSmallint.INSTANCE.toBytes(na);
        byte[] bb = PUnsignedSmallint.INSTANCE.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) > 0);
        
        Integer value = 100;
        Object obj = PUnsignedSmallint.INSTANCE.toObject(value, PInteger.INSTANCE);
        assertTrue(obj instanceof Short);
        assertEquals(100, ((Short)obj).shortValue());
        
        Float floatValue = 100f;
        Object floatObj = PUnsignedSmallint.INSTANCE.toObject(floatValue, PFloat.INSTANCE);
        assertTrue(floatObj instanceof Short);
        assertEquals(100, ((Short)floatObj).shortValue());
        
        Float unsignedFloatValue = 100f;
        Object unsignedFloatObj = PUnsignedSmallint.INSTANCE.toObject(unsignedFloatValue, PUnsignedFloat.INSTANCE);
        assertTrue(unsignedFloatObj instanceof Short);
        assertEquals(100, ((Short)unsignedFloatObj).shortValue());
        
        Double unsignedDoubleValue = 100d;
        Object unsignedDoubleObj = PUnsignedSmallint.INSTANCE.toObject(unsignedDoubleValue, PUnsignedDouble.INSTANCE);
        assertTrue(unsignedDoubleObj instanceof Short);
        assertEquals(100, ((Short)unsignedDoubleObj).shortValue());
        
        Double doubleValue = 100d;
        Object doubleObj = PUnsignedSmallint.INSTANCE.toObject(doubleValue, PDouble.INSTANCE);
        assertTrue(doubleObj instanceof Short);
        assertEquals(100, ((Short)doubleObj).shortValue());
    }
    
    @Test
    public void testUnsignedTinyInt() {
        Byte na = 4;
        byte[] b = PUnsignedTinyint.INSTANCE.toBytes(na);
        Byte nb = (Byte) PUnsignedTinyint.INSTANCE.toObject(b);
        assertEquals(na,nb);

        na = 10;
        nb = 8;
        byte[] ba = PUnsignedTinyint.INSTANCE.toBytes(na);
        byte[] bb = PUnsignedTinyint.INSTANCE.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) > 0);
        
        Integer value = 100;
        Object obj = PUnsignedTinyint.INSTANCE.toObject(value, PInteger.INSTANCE);
        assertTrue(obj instanceof Byte);
        assertEquals(100, ((Byte)obj).byteValue());
        
        Float floatValue = 100f;
        Object floatObj = PUnsignedTinyint.INSTANCE.toObject(floatValue, PFloat.INSTANCE);
        assertTrue(floatObj instanceof Byte);
        assertEquals(100, ((Byte)floatObj).byteValue());
        
        Float unsignedFloatValue = 100f;
        Object unsignedFloatObj = PUnsignedTinyint.INSTANCE.toObject(unsignedFloatValue, PUnsignedFloat.INSTANCE);
        assertTrue(unsignedFloatObj instanceof Byte);
        assertEquals(100, ((Byte)unsignedFloatObj).byteValue());
        
        Double unsignedDoubleValue = 100d;
        Object unsignedDoubleObj = PUnsignedTinyint.INSTANCE.toObject(unsignedDoubleValue, PUnsignedDouble.INSTANCE);
        assertTrue(unsignedDoubleObj instanceof Byte);
        assertEquals(100, ((Byte)unsignedDoubleObj).byteValue());
        
        Double doubleValue = 100d;
        Object doubleObj = PUnsignedTinyint.INSTANCE.toObject(doubleValue, PDouble.INSTANCE);
        assertTrue(doubleObj instanceof Byte);
        assertEquals(100, ((Byte)doubleObj).byteValue());
    }
    
    @Test
    public void testUnsignedFloat() {
        Float na = 0.005f;
        byte[] b = PUnsignedFloat.INSTANCE.toBytes(na);
        Float nb = (Float) PUnsignedFloat.INSTANCE.toObject(b);
        assertEquals(na,nb);
        
        na = 10.0f;
        b = PUnsignedFloat.INSTANCE.toBytes(na, SortOrder.DESC);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        ptr.set(b);
        nb = PUnsignedFloat.INSTANCE.getCodec().decodeFloat(ptr, SortOrder.DESC);
        assertEquals(na,nb);
        
        na = 2.0f;
        nb = 1.0f;
        byte[] ba = PUnsignedFloat.INSTANCE.toBytes(na);
        byte[] bb = PUnsignedFloat.INSTANCE.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) > 0);
        
        na = 0.0f;
        nb = Float.MIN_VALUE;
        ba = PUnsignedFloat.INSTANCE.toBytes(na);
        bb = PUnsignedFloat.INSTANCE.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) < 0);
        
        na = Float.MIN_VALUE;
        nb = Float.MAX_VALUE;
        ba = PUnsignedFloat.INSTANCE.toBytes(na);
        bb = PUnsignedFloat.INSTANCE.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) < 0);
        
        na = Float.MAX_VALUE;
        nb = Float.POSITIVE_INFINITY;
        ba = PUnsignedFloat.INSTANCE.toBytes(na);
        bb = PUnsignedFloat.INSTANCE.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) < 0);
        
        na = Float.POSITIVE_INFINITY;
        nb = Float.NaN;
        ba = PUnsignedFloat.INSTANCE.toBytes(na);
        bb = PUnsignedFloat.INSTANCE.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) < 0);
        
        Integer value = 100;
        Object obj = PUnsignedFloat.INSTANCE.toObject(value, PInteger.INSTANCE);
        assertTrue(obj instanceof Float);
    }
    
    @Test
    public void testUnsignedDouble() {
        Double na = 0.005;
        byte[] b = PUnsignedDouble.INSTANCE.toBytes(na);
        Double nb = (Double) PUnsignedDouble.INSTANCE.toObject(b);
        assertEquals(na,nb);
        
        na = 10.0;
        b = PUnsignedDouble.INSTANCE.toBytes(na, SortOrder.DESC);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        ptr.set(b);
        nb = PUnsignedDouble.INSTANCE.getCodec().decodeDouble(ptr, SortOrder.DESC);
        assertEquals(na,nb);

        na = 2.0;
        nb = 1.0;
        byte[] ba = PUnsignedDouble.INSTANCE.toBytes(na);
        byte[] bb = PUnsignedDouble.INSTANCE.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) > 0);
        
        na = 0.0;
        nb = Double.MIN_VALUE;
        ba = PUnsignedDouble.INSTANCE.toBytes(na);
        bb = PUnsignedDouble.INSTANCE.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) < 0);
        
        na = Double.MIN_VALUE;
        nb = Double.MAX_VALUE;
        ba = PUnsignedDouble.INSTANCE.toBytes(na);
        bb = PUnsignedDouble.INSTANCE.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) < 0);
        
        na = Double.MAX_VALUE;
        nb = Double.POSITIVE_INFINITY;
        ba = PUnsignedDouble.INSTANCE.toBytes(na);
        bb = PUnsignedDouble.INSTANCE.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) < 0);
        
        na = Double.POSITIVE_INFINITY;
        nb = Double.NaN;
        ba = PUnsignedDouble.INSTANCE.toBytes(na);
        bb = PUnsignedDouble.INSTANCE.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) < 0);
        
        Integer value = 100;
        Object obj = PUnsignedDouble.INSTANCE.toObject(value, PInteger.INSTANCE);
        assertTrue(obj instanceof Double);
        
        assertEquals(1, PUnsignedDouble.INSTANCE.compareTo(Double.valueOf(101), Long.valueOf(100), PLong.INSTANCE));
        assertEquals(0, PUnsignedDouble.INSTANCE.compareTo(Double.valueOf(Long.MAX_VALUE), Long.MAX_VALUE, PLong.INSTANCE));
        assertEquals(-1, PUnsignedDouble.INSTANCE.compareTo(Double.valueOf(1), Long.valueOf(100), PLong.INSTANCE));
        
        assertEquals(0, PUnsignedDouble.INSTANCE.compareTo(Double.valueOf(101), BigDecimal.valueOf(101.0), PDecimal.INSTANCE));
    }
    
    @Test
    public void testFloat() {
        Float na = 0.005f;
        byte[] b = PFloat.INSTANCE.toBytes(na);
        Float nb = (Float) PFloat.INSTANCE.toObject(b);
        assertEquals(na,nb);
        
        na = 10.0f;
        b = PFloat.INSTANCE.toBytes(na, SortOrder.DESC);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        ptr.set(b);
        nb = PFloat.INSTANCE.getCodec().decodeFloat(ptr, SortOrder.DESC);
        assertEquals(na,nb);
        
        na = 1.0f;
        nb = -1.0f;
        byte[] ba = PFloat.INSTANCE.toBytes(na);
        byte[] bb = PFloat.INSTANCE.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) > 0);

        na = -1f;
        nb = -3f;
        ba = PFloat.INSTANCE.toBytes(na);
        bb = PFloat.INSTANCE.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) > 0);
        
        na = Float.NEGATIVE_INFINITY;
        nb = -Float.MAX_VALUE;
        ba = PFloat.INSTANCE.toBytes(na);
        bb = PFloat.INSTANCE.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) < 0);
        
        na = -Float.MAX_VALUE;
        nb = -Float.MIN_VALUE;
        ba = PFloat.INSTANCE.toBytes(na);
        bb = PFloat.INSTANCE.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) < 0);
        
        na = -Float.MIN_VALUE;
        nb = -0.0f;
        ba = PFloat.INSTANCE.toBytes(na);
        bb = PFloat.INSTANCE.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) < 0);
        
        na = -0.0f;
        nb = 0.0f;
        ba = PFloat.INSTANCE.toBytes(na);
        bb = PFloat.INSTANCE.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) < 0);
        
        na = 0.0f;
        nb = Float.MIN_VALUE;
        ba = PFloat.INSTANCE.toBytes(na);
        bb = PFloat.INSTANCE.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) < 0);
        
        na = Float.MIN_VALUE;
        nb = Float.MAX_VALUE;
        ba = PFloat.INSTANCE.toBytes(na);
        bb = PFloat.INSTANCE.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) < 0);
        
        na = Float.MAX_VALUE;
        nb = Float.POSITIVE_INFINITY;
        ba = PFloat.INSTANCE.toBytes(na);
        bb = PFloat.INSTANCE.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) < 0);
        
        na = Float.POSITIVE_INFINITY;
        nb = Float.NaN;
        ba = PFloat.INSTANCE.toBytes(na);
        bb = PFloat.INSTANCE.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) < 0);
        
        Integer value = 100;
        Object obj = PFloat.INSTANCE.toObject(value, PInteger.INSTANCE);
        assertTrue(obj instanceof Float);
        
        Double dvalue = Double.NEGATIVE_INFINITY;
        obj = PFloat.INSTANCE.toObject(dvalue, PDouble.INSTANCE);
        assertTrue(obj instanceof Float);
        assertEquals(Float.NEGATIVE_INFINITY, obj);
        
        na = 1.0f;
        nb = -1.0f;
        ba = PFloat.INSTANCE.toBytes(na);
        bb = PFloat.INSTANCE.toBytes(nb);
        float nna = PFloat.INSTANCE.getCodec().decodeFloat(ba, 0, SortOrder.DESC);
        float nnb = PFloat.INSTANCE.getCodec().decodeFloat(bb, 0, SortOrder.DESC);
        assertTrue(Float.compare(nna, nnb) < 0);
    }
    
    @Test
    public void testDouble() {
        Double na = 0.005;
        byte[] b = PDouble.INSTANCE.toBytes(na);
        Double nb = (Double) PDouble.INSTANCE.toObject(b);
        assertEquals(na,nb);
        
        na = 10.0;
        b = PDouble.INSTANCE.toBytes(na, SortOrder.DESC);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        ptr.set(b);
        nb = PDouble.INSTANCE.getCodec().decodeDouble(ptr, SortOrder.DESC);
        assertEquals(na,nb);

        na = 1.0;
        nb = -1.0;
        byte[] ba = PDouble.INSTANCE.toBytes(na);
        byte[] bb = PDouble.INSTANCE.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) > 0);

        na = -1.0;
        nb = -3.0;
        ba = PDouble.INSTANCE.toBytes(na);
        bb = PDouble.INSTANCE.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) > 0);
        
        na = Double.NEGATIVE_INFINITY;
        nb = -Double.MAX_VALUE;
        ba = PDouble.INSTANCE.toBytes(na);
        bb = PDouble.INSTANCE.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) < 0);
        
        na = -Double.MAX_VALUE;
        nb = -Double.MIN_VALUE;
        ba = PDouble.INSTANCE.toBytes(na);
        bb = PDouble.INSTANCE.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) < 0);
        
        na = -Double.MIN_VALUE;
        nb = -0.0;
        ba = PDouble.INSTANCE.toBytes(na);
        bb = PDouble.INSTANCE.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) < 0);
        
        na = -0.0;
        nb = 0.0;
        ba = PDouble.INSTANCE.toBytes(na);
        bb = PDouble.INSTANCE.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) < 0);
        
        na = 0.0;
        nb = Double.MIN_VALUE;
        ba = PDouble.INSTANCE.toBytes(na);
        bb = PDouble.INSTANCE.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) < 0);
        
        na = Double.MIN_VALUE;
        nb = Double.MAX_VALUE;
        ba = PDouble.INSTANCE.toBytes(na);
        bb = PDouble.INSTANCE.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) < 0);
        
        na = Double.MAX_VALUE;
        nb = Double.POSITIVE_INFINITY;
        ba = PDouble.INSTANCE.toBytes(na);
        bb = PDouble.INSTANCE.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) < 0);
        
        na = Double.POSITIVE_INFINITY;
        nb = Double.NaN;
        ba = PDouble.INSTANCE.toBytes(na);
        bb = PDouble.INSTANCE.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) < 0);
        
        Integer value = 100;
        Object obj = PDouble.INSTANCE.toObject(value, PInteger.INSTANCE);
        assertTrue(obj instanceof Double);
        
        na = 1.0;
        nb = -1.0;
        ba = PDouble.INSTANCE.toBytes(na);
        bb = PDouble.INSTANCE.toBytes(nb);
        double nna = PDouble.INSTANCE.getCodec().decodeDouble(ba, 0, SortOrder.DESC);
        double nnb = PDouble.INSTANCE.getCodec().decodeDouble(bb, 0, SortOrder.DESC);
        assertTrue(Double.compare(nna, nnb) < 0);
        
        assertEquals(1, PDouble.INSTANCE.compareTo(Double.valueOf(101), Long.valueOf(100), PLong.INSTANCE));
        assertEquals(0, PDouble.INSTANCE.compareTo(Double.valueOf(Long.MAX_VALUE), Long.MAX_VALUE, PLong.INSTANCE));
        assertEquals(-1, PDouble.INSTANCE.compareTo(Double.valueOf(1), Long.valueOf(100), PLong.INSTANCE));
        
        assertEquals(0, PDouble.INSTANCE.compareTo(Double.valueOf(101), BigDecimal.valueOf(101.0), PDecimal.INSTANCE));
    }

    @Test
    public void testBigDecimal() {
        byte[] b;
        BigDecimal na, nb;

        b = new byte[] {
                (byte)0xc2,0x02,0x10,0x36,0x22,0x22,0x22,0x22,0x22,0x22,0x0f,0x27,0x38,0x1c,0x05,0x40,0x62,0x21,0x54,0x4d,0x4e,0x01,0x14,0x36,0x0d,0x33
        };
        BigDecimal decodedBytes = (BigDecimal) PDecimal.INSTANCE.toObject(b);
        assertTrue(decodedBytes.compareTo(BigDecimal.ZERO) > 0);

        na = new BigDecimal(new BigInteger("12345678901239998123456789"), 2);
        //[-52, 13, 35, 57, 79, 91, 13, 40, 100, 82, 24, 46, 68, 90]
        b = PDecimal.INSTANCE.toBytes(na);
        nb = (BigDecimal) PDecimal.INSTANCE.toObject(b);
        TestUtil.assertRoundEquals(na,nb);
        assertTrue(b.length <= PDecimal.INSTANCE.estimateByteSize(na));

        na = new BigDecimal("115.533333333333331438552704639732837677001953125");
        b = PDecimal.INSTANCE.toBytes(na);
        nb = (BigDecimal) PDecimal.INSTANCE.toObject(b);
        TestUtil.assertRoundEquals(na,nb);
        assertTrue(b.length <= PDecimal.INSTANCE.estimateByteSize(na));
        
        // test for negative serialization using biginteger
        na = new BigDecimal("-5.00000000000000000000000001");
        b = PDecimal.INSTANCE.toBytes(na);
        nb = (BigDecimal) PDecimal.INSTANCE.toObject(b);
        TestUtil.assertRoundEquals(na,nb);
        assertTrue(b.length <= PDecimal.INSTANCE.estimateByteSize(na));
        
        // test for serialization of 38 digits
        na = new BigDecimal("-2.4999999999999999999999999999999999999");
        b = PDecimal.INSTANCE.toBytes(na);
        nb = (BigDecimal) PDecimal.INSTANCE.toObject(b);
        TestUtil.assertRoundEquals(na,nb);
        assertTrue(b.length <= PDecimal.INSTANCE.estimateByteSize(na));
        
        // test for serialization of 39 digits, should round to -2.5
        na = new BigDecimal("-2.499999999999999999999999999999999999999");
        b = PDecimal.INSTANCE.toBytes(na);
        nb = (BigDecimal) PDecimal.INSTANCE.toObject(b);
        assertTrue(nb.compareTo(new BigDecimal("-2.5")) == 0);
        assertEquals(new BigDecimal("-2.5"), nb);
        assertTrue(b.length <= PDecimal.INSTANCE.estimateByteSize(na));

        na = new BigDecimal(2.5);
        b = PDecimal.INSTANCE.toBytes(na);
        nb = (BigDecimal) PDecimal.INSTANCE.toObject(b);
        assertTrue(na.compareTo(nb) == 0);
        assertTrue(b.length <= PDecimal.INSTANCE.estimateByteSize(na));

        // If we don't remove trailing zeros, this fails
        na = new BigDecimal(Double.parseDouble("96.45238095238095"));
        String naStr = na.toString();
        assertTrue(naStr != null);
        b = PDecimal.INSTANCE.toBytes(na);
        nb = (BigDecimal) PDecimal.INSTANCE.toObject(b);
        TestUtil.assertRoundEquals(na,nb);
        assertTrue(b.length <= PDecimal.INSTANCE.estimateByteSize(na));

        // If we don't remove trailing zeros, this fails
        na = new BigDecimal(-1000);
        b = PDecimal.INSTANCE.toBytes(na);
        nb = (BigDecimal) PDecimal.INSTANCE.toObject(b);
        assertTrue(na.compareTo(nb) == 0);
        assertTrue(b.length <= PDecimal.INSTANCE.estimateByteSize(na));

        na = new BigDecimal("1000.5829999999999913");
        b = PDecimal.INSTANCE.toBytes(na);
        nb = (BigDecimal) PDecimal.INSTANCE.toObject(b);
        assertTrue(na.compareTo(nb) == 0);
        assertTrue(b.length <= PDecimal.INSTANCE.estimateByteSize(na));

        na = TestUtil.computeAverage(11000, 3);
        b = PDecimal.INSTANCE.toBytes(na);
        nb = (BigDecimal) PDecimal.INSTANCE.toObject(b);
        assertTrue(na.compareTo(nb) == 0);
        assertTrue(b.length <= PDecimal.INSTANCE.estimateByteSize(na));

        na = new BigDecimal(new BigInteger("12345678901239999"), 2);
        b = PDecimal.INSTANCE.toBytes(na);
        nb = (BigDecimal) PDecimal.INSTANCE.toObject(b);
        assertTrue(na.compareTo(nb) == 0);
        assertTrue(b.length <= PDecimal.INSTANCE.estimateByteSize(na));

        na = new BigDecimal(1);
        nb = new BigDecimal(-1);
        byte[] ba = PDecimal.INSTANCE.toBytes(na);
        byte[] bb = PDecimal.INSTANCE.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) > 0);
        assertTrue(ba.length <= PDecimal.INSTANCE.estimateByteSize(na));
        assertTrue(bb.length <= PDecimal.INSTANCE.estimateByteSize(nb));

        na = new BigDecimal(-1);
        nb = new BigDecimal(-2);
        ba = PDecimal.INSTANCE.toBytes(na);
        bb = PDecimal.INSTANCE.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) > 0);
        assertTrue(ba.length <= PDecimal.INSTANCE.estimateByteSize(na));
        assertTrue(bb.length <= PDecimal.INSTANCE.estimateByteSize(nb));

        na = new BigDecimal(-3);
        nb = new BigDecimal(-1000);
        assertTrue(na.compareTo(nb) > 0);
        ba = PDecimal.INSTANCE.toBytes(na);
        bb = PDecimal.INSTANCE.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) > 0);
        assertTrue(ba.length <= PDecimal.INSTANCE.estimateByteSize(na));
        assertTrue(bb.length <= PDecimal.INSTANCE.estimateByteSize(nb));

        na = new BigDecimal(BigInteger.valueOf(12345678901239998L), 2);
        nb = new BigDecimal(97);
        assertTrue(na.compareTo(nb) > 0);
        ba = PDecimal.INSTANCE.toBytes(na);
        bb = PDecimal.INSTANCE.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) > 0);
        assertTrue(ba.length <= PDecimal.INSTANCE.estimateByteSize(na));
        assertTrue(bb.length <= PDecimal.INSTANCE.estimateByteSize(nb));

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
            byteValues.add(PDecimal.INSTANCE.toBytes(values.get(i)));
        }

        for (int i = 0; i < values.size(); i++) {
            BigDecimal expected = values.get(i);
            BigDecimal actual = (BigDecimal) PDecimal.INSTANCE.toObject(byteValues.get(i));
            assertTrue("For " + i + " expected " + expected + " but got " + actual,expected.round(
                PDataType.DEFAULT_MATH_CONTEXT).compareTo(actual.round(
                PDataType.DEFAULT_MATH_CONTEXT)) == 0);
            assertTrue(byteValues.get(i).length <= PDecimal.INSTANCE.estimateByteSize(expected));
        }

        Collections.sort(values);
        Collections.sort(byteValues, Bytes.BYTES_COMPARATOR);

        for (int i = 0; i < values.size(); i++) {
            BigDecimal expected = values.get(i);
            byte[] bytes = PDecimal.INSTANCE.toBytes(values.get(i));
            assertNotNull("bytes converted from values should not be null!", bytes);
            BigDecimal actual = (BigDecimal) PDecimal.INSTANCE.toObject(byteValues.get(i));
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
                Object o = PDecimal.INSTANCE.toObject(bytes);
                assertNotNull(o);
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
        byte[] b1 = PVarchar.INSTANCE.toBytes("");
        byte[] b2 = PVarchar.INSTANCE.toBytes(null);
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
        assertFalse(PDouble.INSTANCE.isCoercibleTo(PFloat.INSTANCE));
        assertTrue(PDouble.INSTANCE.isCoercibleTo(PFloat.INSTANCE, 10.0));
        assertTrue(PDouble.INSTANCE.isCoercibleTo(PFloat.INSTANCE, 0.0));
        assertTrue(PDouble.INSTANCE.isCoercibleTo(PFloat.INSTANCE, -10.0));
        assertFalse(PDouble.INSTANCE.isCoercibleTo(PFloat.INSTANCE, Double.valueOf(Float.MAX_VALUE) + Double.valueOf(Float.MAX_VALUE)));
        assertFalse(PDouble.INSTANCE.isCoercibleTo(PLong.INSTANCE));
        assertTrue(PDouble.INSTANCE.isCoercibleTo(PLong.INSTANCE, 10.0));
        assertTrue(PDouble.INSTANCE.isCoercibleTo(PLong.INSTANCE, 0.0));
        assertTrue(PDouble.INSTANCE.isCoercibleTo(PLong.INSTANCE, -10.0));
        assertFalse(PDouble.INSTANCE.isCoercibleTo(PLong.INSTANCE, Double.valueOf(Long.MAX_VALUE) + Double.valueOf(Long.MAX_VALUE)));
        assertFalse(PDouble.INSTANCE.isCoercibleTo(PUnsignedInt.INSTANCE));
        assertTrue(PDouble.INSTANCE.isCoercibleTo(PUnsignedInt.INSTANCE, 10.0));
        assertTrue(PDouble.INSTANCE.isCoercibleTo(PUnsignedInt.INSTANCE, 0.0));
        assertFalse(PDouble.INSTANCE.isCoercibleTo(PUnsignedInt.INSTANCE, -10.0));
        assertFalse(PDouble.INSTANCE.isCoercibleTo(PUnsignedLong.INSTANCE));
        assertTrue(PDouble.INSTANCE.isCoercibleTo(PUnsignedLong.INSTANCE, 10.0));
        assertTrue(PDouble.INSTANCE.isCoercibleTo(PUnsignedLong.INSTANCE, 0.0));
        assertFalse(PDouble.INSTANCE.isCoercibleTo(PUnsignedLong.INSTANCE, -10.0));
        assertFalse(PDouble.INSTANCE.isCoercibleTo(PSmallint.INSTANCE));
        assertTrue(PDouble.INSTANCE.isCoercibleTo(PSmallint.INSTANCE, 10.0));
        assertTrue(PDouble.INSTANCE.isCoercibleTo(PSmallint.INSTANCE, 0.0));
        assertTrue(PDouble.INSTANCE.isCoercibleTo(PSmallint.INSTANCE, -10.0));
        assertFalse(PDouble.INSTANCE.isCoercibleTo(PSmallint.INSTANCE, -100000.0));
        assertFalse(PDouble.INSTANCE.isCoercibleTo(PTinyint.INSTANCE));
        assertTrue(PDouble.INSTANCE.isCoercibleTo(PTinyint.INSTANCE, 10.0));
        assertTrue(PDouble.INSTANCE.isCoercibleTo(PTinyint.INSTANCE, 0.0));
        assertTrue(PDouble.INSTANCE.isCoercibleTo(PTinyint.INSTANCE, -10.0));
        assertFalse(PDouble.INSTANCE.isCoercibleTo(PTinyint.INSTANCE, -1000.0));
        assertFalse(PDouble.INSTANCE.isCoercibleTo(PUnsignedSmallint.INSTANCE));
        assertTrue(PDouble.INSTANCE.isCoercibleTo(PUnsignedSmallint.INSTANCE, 10.0));
        assertTrue(PDouble.INSTANCE.isCoercibleTo(PUnsignedSmallint.INSTANCE, 0.0));
        assertFalse(PDouble.INSTANCE.isCoercibleTo(PUnsignedSmallint.INSTANCE, -10.0));
        assertFalse(PDouble.INSTANCE.isCoercibleTo(PUnsignedSmallint.INSTANCE, -100000.0));
        assertFalse(PDouble.INSTANCE.isCoercibleTo(PUnsignedTinyint.INSTANCE));
        assertTrue(PDouble.INSTANCE.isCoercibleTo(PUnsignedTinyint.INSTANCE, 10.0));
        assertTrue(PDouble.INSTANCE.isCoercibleTo(PUnsignedTinyint.INSTANCE, 0.0));
        assertFalse(PDouble.INSTANCE.isCoercibleTo(PUnsignedTinyint.INSTANCE, -10.0));
        assertFalse(PDouble.INSTANCE.isCoercibleTo(PUnsignedTinyint.INSTANCE, -1000.0));
        assertFalse(PDouble.INSTANCE.isCoercibleTo(PUnsignedDouble.INSTANCE));
        assertTrue(PDouble.INSTANCE.isCoercibleTo(PUnsignedDouble.INSTANCE, 10.0));
        assertTrue(PDouble.INSTANCE.isCoercibleTo(PUnsignedDouble.INSTANCE, 0.0));
        assertFalse(PDouble.INSTANCE.isCoercibleTo(PUnsignedDouble.INSTANCE, -10.0));
        assertFalse(PDouble.INSTANCE.isCoercibleTo(PUnsignedFloat.INSTANCE));
        assertTrue(PDouble.INSTANCE.isCoercibleTo(PUnsignedFloat.INSTANCE, 10.0));
        assertTrue(PDouble.INSTANCE.isCoercibleTo(PUnsignedFloat.INSTANCE, 0.0));
        assertFalse(PDouble.INSTANCE.isCoercibleTo(PUnsignedFloat.INSTANCE, -10.0));
        assertFalse(PDouble.INSTANCE.isCoercibleTo(PUnsignedFloat.INSTANCE, Double.MAX_VALUE));
        
        assertTrue(PFloat.INSTANCE.isCoercibleTo(PDouble.INSTANCE));
        assertFalse(PFloat.INSTANCE.isCoercibleTo(PLong.INSTANCE));
        assertTrue(PFloat.INSTANCE.isCoercibleTo(PLong.INSTANCE, 10.0f));
        assertTrue(PFloat.INSTANCE.isCoercibleTo(PLong.INSTANCE, 0.0f));
        assertTrue(PFloat.INSTANCE.isCoercibleTo(PLong.INSTANCE, -10.0f));
        assertFalse(PFloat.INSTANCE.isCoercibleTo(PLong.INSTANCE, Float.valueOf(Long.MAX_VALUE) + Float.valueOf(Long.MAX_VALUE)));
        assertFalse(PFloat.INSTANCE.isCoercibleTo(PUnsignedInt.INSTANCE));
        assertTrue(PFloat.INSTANCE.isCoercibleTo(PUnsignedInt.INSTANCE, 10.0f));
        assertTrue(PFloat.INSTANCE.isCoercibleTo(PUnsignedInt.INSTANCE, 0.0f));
        assertFalse(PFloat.INSTANCE.isCoercibleTo(PUnsignedInt.INSTANCE, -10.0f));
        assertFalse(PFloat.INSTANCE.isCoercibleTo(PUnsignedLong.INSTANCE));
        assertTrue(PFloat.INSTANCE.isCoercibleTo(PUnsignedLong.INSTANCE, 10.0f));
        assertTrue(PFloat.INSTANCE.isCoercibleTo(PUnsignedLong.INSTANCE, 0.0f));
        assertFalse(PFloat.INSTANCE.isCoercibleTo(PUnsignedLong.INSTANCE, -10.0f));
        assertFalse(PFloat.INSTANCE.isCoercibleTo(PSmallint.INSTANCE));
        assertTrue(PFloat.INSTANCE.isCoercibleTo(PSmallint.INSTANCE, 10.0f));
        assertTrue(PFloat.INSTANCE.isCoercibleTo(PSmallint.INSTANCE, 0.0f));
        assertTrue(PFloat.INSTANCE.isCoercibleTo(PSmallint.INSTANCE, -10.0f));
        assertFalse(PFloat.INSTANCE.isCoercibleTo(PSmallint.INSTANCE, -100000.0f));
        assertFalse(PFloat.INSTANCE.isCoercibleTo(PTinyint.INSTANCE));
        assertTrue(PFloat.INSTANCE.isCoercibleTo(PTinyint.INSTANCE, 10.0f));
        assertTrue(PFloat.INSTANCE.isCoercibleTo(PTinyint.INSTANCE, 0.0f));
        assertTrue(PFloat.INSTANCE.isCoercibleTo(PTinyint.INSTANCE, -10.0f));
        assertFalse(PFloat.INSTANCE.isCoercibleTo(PTinyint.INSTANCE, -1000.0f));
        assertFalse(PFloat.INSTANCE.isCoercibleTo(PUnsignedSmallint.INSTANCE));
        assertTrue(PFloat.INSTANCE.isCoercibleTo(PUnsignedSmallint.INSTANCE, 10.0f));
        assertTrue(PFloat.INSTANCE.isCoercibleTo(PUnsignedSmallint.INSTANCE, 0.0f));
        assertFalse(PFloat.INSTANCE.isCoercibleTo(PUnsignedSmallint.INSTANCE, -10.0f));
        assertFalse(PFloat.INSTANCE.isCoercibleTo(PUnsignedSmallint.INSTANCE, -100000.0f));
        assertFalse(PFloat.INSTANCE.isCoercibleTo(PUnsignedTinyint.INSTANCE));
        assertTrue(PFloat.INSTANCE.isCoercibleTo(PUnsignedTinyint.INSTANCE, 10.0f));
        assertTrue(PFloat.INSTANCE.isCoercibleTo(PUnsignedTinyint.INSTANCE, 0.0f));
        assertFalse(PFloat.INSTANCE.isCoercibleTo(PUnsignedTinyint.INSTANCE, -10.0f));
        assertFalse(PFloat.INSTANCE.isCoercibleTo(PUnsignedTinyint.INSTANCE, -1000.0f));
        assertFalse(PFloat.INSTANCE.isCoercibleTo(PUnsignedDouble.INSTANCE));
        assertFalse(PFloat.INSTANCE.isCoercibleTo(PUnsignedFloat.INSTANCE));
        assertTrue(PFloat.INSTANCE.isCoercibleTo(PUnsignedFloat.INSTANCE, 10.0f));
        assertTrue(PFloat.INSTANCE.isCoercibleTo(PUnsignedFloat.INSTANCE, 0.0f));
        assertFalse(PFloat.INSTANCE.isCoercibleTo(PUnsignedFloat.INSTANCE, -10.0f));
        
        assertFalse(PUnsignedDouble.INSTANCE.isCoercibleTo(PFloat.INSTANCE));
        assertTrue(PUnsignedDouble.INSTANCE.isCoercibleTo(PDouble.INSTANCE));
        assertTrue(PUnsignedDouble.INSTANCE.isCoercibleTo(PFloat.INSTANCE, 10.0));
        assertTrue(PUnsignedDouble.INSTANCE.isCoercibleTo(PFloat.INSTANCE, 0.0));
        assertFalse(PUnsignedDouble.INSTANCE.isCoercibleTo(PFloat.INSTANCE, Double.MAX_VALUE));
        assertFalse(PUnsignedDouble.INSTANCE.isCoercibleTo(PLong.INSTANCE));
        assertTrue(PUnsignedDouble.INSTANCE.isCoercibleTo(PLong.INSTANCE, 10.0));
        assertTrue(PUnsignedDouble.INSTANCE.isCoercibleTo(PLong.INSTANCE, 0.0));
        assertFalse(PUnsignedDouble.INSTANCE.isCoercibleTo(PLong.INSTANCE, Double.MAX_VALUE));
        assertFalse(PUnsignedDouble.INSTANCE.isCoercibleTo(PUnsignedInt.INSTANCE));
        assertTrue(PUnsignedDouble.INSTANCE.isCoercibleTo(PUnsignedInt.INSTANCE, 10.0));
        assertTrue(PUnsignedDouble.INSTANCE.isCoercibleTo(PUnsignedInt.INSTANCE, 0.0));
        assertFalse(PUnsignedDouble.INSTANCE.isCoercibleTo(PUnsignedLong.INSTANCE));
        assertTrue(PUnsignedDouble.INSTANCE.isCoercibleTo(PUnsignedLong.INSTANCE, 10.0));
        assertTrue(PUnsignedDouble.INSTANCE.isCoercibleTo(PUnsignedLong.INSTANCE, 0.0));
        assertFalse(PUnsignedDouble.INSTANCE.isCoercibleTo(PSmallint.INSTANCE));
        assertTrue(PUnsignedDouble.INSTANCE.isCoercibleTo(PSmallint.INSTANCE, 10.0));
        assertTrue(PUnsignedDouble.INSTANCE.isCoercibleTo(PSmallint.INSTANCE, 0.0));
        assertFalse(PUnsignedDouble.INSTANCE.isCoercibleTo(PTinyint.INSTANCE));
        assertTrue(PUnsignedDouble.INSTANCE.isCoercibleTo(PTinyint.INSTANCE, 10.0));
        assertTrue(PUnsignedDouble.INSTANCE.isCoercibleTo(PTinyint.INSTANCE, 0.0));
        assertFalse(PUnsignedDouble.INSTANCE.isCoercibleTo(PUnsignedSmallint.INSTANCE));
        assertTrue(PUnsignedDouble.INSTANCE.isCoercibleTo(PUnsignedSmallint.INSTANCE, 10.0));
        assertTrue(PUnsignedDouble.INSTANCE.isCoercibleTo(PUnsignedSmallint.INSTANCE, 0.0));
        assertFalse(PUnsignedDouble.INSTANCE.isCoercibleTo(PUnsignedTinyint.INSTANCE));
        assertTrue(PUnsignedDouble.INSTANCE.isCoercibleTo(PUnsignedTinyint.INSTANCE, 10.0));
        assertTrue(PUnsignedDouble.INSTANCE.isCoercibleTo(PUnsignedTinyint.INSTANCE, 0.0));
        assertFalse(PUnsignedDouble.INSTANCE.isCoercibleTo(PUnsignedFloat.INSTANCE));
        assertTrue(PUnsignedDouble.INSTANCE.isCoercibleTo(PUnsignedFloat.INSTANCE, 10.0));
        assertTrue(PUnsignedDouble.INSTANCE.isCoercibleTo(PUnsignedFloat.INSTANCE, 0.0));
        assertFalse(PUnsignedDouble.INSTANCE.isCoercibleTo(PUnsignedFloat.INSTANCE, Double.MAX_VALUE));
        
        assertTrue(PUnsignedFloat.INSTANCE.isCoercibleTo(PFloat.INSTANCE));
        assertTrue(PUnsignedFloat.INSTANCE.isCoercibleTo(PDouble.INSTANCE));
        assertFalse(PUnsignedFloat.INSTANCE.isCoercibleTo(PLong.INSTANCE));
        assertTrue(PUnsignedFloat.INSTANCE.isCoercibleTo(PLong.INSTANCE, 10.0f));
        assertTrue(PUnsignedFloat.INSTANCE.isCoercibleTo(PLong.INSTANCE, 0.0f));
        assertFalse(PUnsignedFloat.INSTANCE.isCoercibleTo(PLong.INSTANCE, Float.MAX_VALUE));
        assertFalse(PUnsignedFloat.INSTANCE.isCoercibleTo(PUnsignedInt.INSTANCE));
        assertTrue(PUnsignedFloat.INSTANCE.isCoercibleTo(PUnsignedInt.INSTANCE, 10.0f));
        assertTrue(PUnsignedFloat.INSTANCE.isCoercibleTo(PUnsignedInt.INSTANCE, 0.0f));
        assertFalse(PUnsignedFloat.INSTANCE.isCoercibleTo(PUnsignedLong.INSTANCE));
        assertTrue(PUnsignedFloat.INSTANCE.isCoercibleTo(PUnsignedLong.INSTANCE, 10.0f));
        assertTrue(PUnsignedFloat.INSTANCE.isCoercibleTo(PUnsignedLong.INSTANCE, 0.0f));
        assertFalse(PUnsignedFloat.INSTANCE.isCoercibleTo(PSmallint.INSTANCE));
        assertTrue(PUnsignedFloat.INSTANCE.isCoercibleTo(PSmallint.INSTANCE, 10.0f));
        assertTrue(PUnsignedFloat.INSTANCE.isCoercibleTo(PSmallint.INSTANCE, 0.0f));
        assertFalse(PUnsignedFloat.INSTANCE.isCoercibleTo(PTinyint.INSTANCE));
        assertTrue(PUnsignedFloat.INSTANCE.isCoercibleTo(PTinyint.INSTANCE, 10.0f));
        assertTrue(PUnsignedFloat.INSTANCE.isCoercibleTo(PTinyint.INSTANCE, 0.0f));
        assertFalse(PUnsignedFloat.INSTANCE.isCoercibleTo(PUnsignedSmallint.INSTANCE));
        assertTrue(PUnsignedFloat.INSTANCE.isCoercibleTo(PUnsignedSmallint.INSTANCE, 10.0f));
        assertTrue(PUnsignedFloat.INSTANCE.isCoercibleTo(PUnsignedSmallint.INSTANCE, 0.0f));
        assertFalse(PUnsignedFloat.INSTANCE.isCoercibleTo(PUnsignedTinyint.INSTANCE));
        assertTrue(PUnsignedFloat.INSTANCE.isCoercibleTo(PUnsignedTinyint.INSTANCE, 10.0f));
        assertTrue(PUnsignedFloat.INSTANCE.isCoercibleTo(PUnsignedTinyint.INSTANCE, 0.0f));
        assertTrue(PUnsignedFloat.INSTANCE.isCoercibleTo(PUnsignedDouble.INSTANCE));
        
        // Testing coercing integer to other values.
        assertTrue(PInteger.INSTANCE.isCoercibleTo(PDouble.INSTANCE));
        assertTrue(PInteger.INSTANCE.isCoercibleTo(PFloat.INSTANCE));
        assertTrue(PInteger.INSTANCE.isCoercibleTo(PLong.INSTANCE));
        assertTrue(PInteger.INSTANCE.isCoercibleTo(PLong.INSTANCE, 10));
        assertTrue(PInteger.INSTANCE.isCoercibleTo(PLong.INSTANCE, 0));
        assertTrue(PInteger.INSTANCE.isCoercibleTo(PLong.INSTANCE, -10));
        assertFalse(PInteger.INSTANCE.isCoercibleTo(PUnsignedInt.INSTANCE));
        assertTrue(PInteger.INSTANCE.isCoercibleTo(PUnsignedInt.INSTANCE, 10));
        assertTrue(PInteger.INSTANCE.isCoercibleTo(PUnsignedInt.INSTANCE, 0));
        assertFalse(PInteger.INSTANCE.isCoercibleTo(PUnsignedInt.INSTANCE, -10));
        assertFalse(PInteger.INSTANCE.isCoercibleTo(PUnsignedLong.INSTANCE));
        assertTrue(PInteger.INSTANCE.isCoercibleTo(PUnsignedLong.INSTANCE, 10));
        assertTrue(PInteger.INSTANCE.isCoercibleTo(PUnsignedLong.INSTANCE, 0));
        assertFalse(PInteger.INSTANCE.isCoercibleTo(PUnsignedLong.INSTANCE, -10));
        assertFalse(PInteger.INSTANCE.isCoercibleTo(PSmallint.INSTANCE));
        assertTrue(PInteger.INSTANCE.isCoercibleTo(PSmallint.INSTANCE, 10));
        assertTrue(PInteger.INSTANCE.isCoercibleTo(PSmallint.INSTANCE, 0));
        assertTrue(PInteger.INSTANCE.isCoercibleTo(PSmallint.INSTANCE, -10));
        assertFalse(PInteger.INSTANCE.isCoercibleTo(PSmallint.INSTANCE, -100000));
        assertFalse(PInteger.INSTANCE.isCoercibleTo(PTinyint.INSTANCE));
        assertTrue(PInteger.INSTANCE.isCoercibleTo(PTinyint.INSTANCE, 10));
        assertTrue(PInteger.INSTANCE.isCoercibleTo(PTinyint.INSTANCE, 0));
        assertTrue(PInteger.INSTANCE.isCoercibleTo(PTinyint.INSTANCE, -10));
        assertFalse(PInteger.INSTANCE.isCoercibleTo(PTinyint.INSTANCE, -1000));
        assertFalse(PInteger.INSTANCE.isCoercibleTo(PUnsignedSmallint.INSTANCE));
        assertTrue(PInteger.INSTANCE.isCoercibleTo(PUnsignedSmallint.INSTANCE, 10));
        assertTrue(PInteger.INSTANCE.isCoercibleTo(PUnsignedSmallint.INSTANCE, 0));
        assertFalse(PInteger.INSTANCE.isCoercibleTo(PUnsignedSmallint.INSTANCE, -10));
        assertFalse(PInteger.INSTANCE.isCoercibleTo(PUnsignedSmallint.INSTANCE, -100000));
        assertFalse(PInteger.INSTANCE.isCoercibleTo(PUnsignedTinyint.INSTANCE));
        assertTrue(PInteger.INSTANCE.isCoercibleTo(PUnsignedTinyint.INSTANCE, 10));
        assertTrue(PInteger.INSTANCE.isCoercibleTo(PUnsignedTinyint.INSTANCE, 0));
        assertFalse(PInteger.INSTANCE.isCoercibleTo(PUnsignedTinyint.INSTANCE, -10));
        assertFalse(PInteger.INSTANCE.isCoercibleTo(PUnsignedTinyint.INSTANCE, -1000));
        assertFalse(PInteger.INSTANCE.isCoercibleTo(PUnsignedFloat.INSTANCE, -10));
        assertTrue(PInteger.INSTANCE.isCoercibleTo(PUnsignedFloat.INSTANCE, 10));
        assertTrue(PInteger.INSTANCE.isCoercibleTo(PUnsignedFloat.INSTANCE, 0));
        assertFalse(PInteger.INSTANCE.isCoercibleTo(PUnsignedDouble.INSTANCE, -10));
        assertTrue(PInteger.INSTANCE.isCoercibleTo(PUnsignedDouble.INSTANCE, 10));
        assertTrue(PInteger.INSTANCE.isCoercibleTo(PUnsignedDouble.INSTANCE, 0));
        assertTrue(PInteger.INSTANCE.isCoercibleTo(PVarbinary.INSTANCE, 0));

        // Testing coercing long to other values.
        assertTrue(PLong.INSTANCE.isCoercibleTo(PDouble.INSTANCE));
        assertFalse(PLong.INSTANCE.isCoercibleTo(PInteger.INSTANCE));
        assertFalse(PLong.INSTANCE.isCoercibleTo(PInteger.INSTANCE, Long.MAX_VALUE));
        assertFalse(PLong.INSTANCE.isCoercibleTo(PInteger.INSTANCE, Integer.MAX_VALUE + 10L));
        assertTrue(PLong.INSTANCE.isCoercibleTo(PInteger.INSTANCE, (long)Integer.MAX_VALUE));
        assertTrue(PLong.INSTANCE.isCoercibleTo(PInteger.INSTANCE, Integer.MAX_VALUE - 10L));
        assertTrue(PLong.INSTANCE.isCoercibleTo(PInteger.INSTANCE, 10L));
        assertTrue(PLong.INSTANCE.isCoercibleTo(PInteger.INSTANCE, 0L));
        assertTrue(PLong.INSTANCE.isCoercibleTo(PInteger.INSTANCE, -10L));
        assertTrue(PLong.INSTANCE.isCoercibleTo(PInteger.INSTANCE, Integer.MIN_VALUE + 10L));
        assertTrue(PLong.INSTANCE.isCoercibleTo(PInteger.INSTANCE, (long)Integer.MIN_VALUE));
        assertFalse(PLong.INSTANCE.isCoercibleTo(PInteger.INSTANCE, Integer.MIN_VALUE - 10L));
        assertFalse(PLong.INSTANCE.isCoercibleTo(PInteger.INSTANCE, Long.MIN_VALUE));
        assertFalse(PLong.INSTANCE.isCoercibleTo(PUnsignedInt.INSTANCE));
        assertTrue(PLong.INSTANCE.isCoercibleTo(PUnsignedInt.INSTANCE, 10L));
        assertTrue(PLong.INSTANCE.isCoercibleTo(PUnsignedInt.INSTANCE, 0L));
        assertFalse(PLong.INSTANCE.isCoercibleTo(PUnsignedInt.INSTANCE, -10L));
        assertTrue(PLong.INSTANCE.isCoercibleTo(PUnsignedLong.INSTANCE, Long.MAX_VALUE));
        assertTrue(PLong.INSTANCE.isCoercibleTo(PUnsignedLong.INSTANCE, 10L));
        assertTrue(PLong.INSTANCE.isCoercibleTo(PUnsignedLong.INSTANCE, 0L));
        assertFalse(PLong.INSTANCE.isCoercibleTo(PUnsignedLong.INSTANCE, -10L));
        assertFalse(PLong.INSTANCE.isCoercibleTo(PUnsignedLong.INSTANCE, Long.MIN_VALUE));
        assertFalse(PLong.INSTANCE.isCoercibleTo(PSmallint.INSTANCE));
        assertTrue(PLong.INSTANCE.isCoercibleTo(PSmallint.INSTANCE, 10L));
        assertTrue(PLong.INSTANCE.isCoercibleTo(PSmallint.INSTANCE, 0L));
        assertTrue(PLong.INSTANCE.isCoercibleTo(PSmallint.INSTANCE, -10L));
        assertFalse(PLong.INSTANCE.isCoercibleTo(PSmallint.INSTANCE, -100000L));
        assertFalse(PLong.INSTANCE.isCoercibleTo(PTinyint.INSTANCE));
        assertTrue(PLong.INSTANCE.isCoercibleTo(PTinyint.INSTANCE, 10L));
        assertTrue(PLong.INSTANCE.isCoercibleTo(PTinyint.INSTANCE, 0L));
        assertTrue(PLong.INSTANCE.isCoercibleTo(PTinyint.INSTANCE, -10L));
        assertFalse(PLong.INSTANCE.isCoercibleTo(PTinyint.INSTANCE, -1000L));
        assertFalse(PLong.INSTANCE.isCoercibleTo(PUnsignedSmallint.INSTANCE));
        assertTrue(PLong.INSTANCE.isCoercibleTo(PUnsignedSmallint.INSTANCE, 10L));
        assertTrue(PLong.INSTANCE.isCoercibleTo(PUnsignedSmallint.INSTANCE, 0L));
        assertFalse(PLong.INSTANCE.isCoercibleTo(PUnsignedSmallint.INSTANCE, -10L));
        assertFalse(PLong.INSTANCE.isCoercibleTo(PUnsignedSmallint.INSTANCE, -100000L));
        assertFalse(PLong.INSTANCE.isCoercibleTo(PUnsignedTinyint.INSTANCE));
        assertTrue(PLong.INSTANCE.isCoercibleTo(PUnsignedTinyint.INSTANCE, 10L));
        assertTrue(PLong.INSTANCE.isCoercibleTo(PUnsignedTinyint.INSTANCE, 0L));
        assertFalse(PLong.INSTANCE.isCoercibleTo(PUnsignedTinyint.INSTANCE, -10L));
        assertFalse(PLong.INSTANCE.isCoercibleTo(PUnsignedTinyint.INSTANCE, -1000L));
		assertTrue(PLong.INSTANCE.isCoercibleTo(PUnsignedDouble.INSTANCE, 10L));
		assertTrue(PLong.INSTANCE.isCoercibleTo(PUnsignedDouble.INSTANCE, 0L));
		assertFalse(PLong.INSTANCE
				.isCoercibleTo(PUnsignedDouble.INSTANCE, -1L));
		assertTrue(PLong.INSTANCE.isCoercibleTo(PUnsignedFloat.INSTANCE, 10L));
		assertTrue(PLong.INSTANCE.isCoercibleTo(PUnsignedFloat.INSTANCE, 0L));
		assertFalse(PLong.INSTANCE.isCoercibleTo(PUnsignedFloat.INSTANCE, -1L));
        
        // Testing coercing smallint to other values.
        assertTrue(PSmallint.INSTANCE.isCoercibleTo(PDouble.INSTANCE));
        assertTrue(PSmallint.INSTANCE.isCoercibleTo(PFloat.INSTANCE));
        assertTrue(PSmallint.INSTANCE.isCoercibleTo(PLong.INSTANCE));
        assertTrue(PSmallint.INSTANCE.isCoercibleTo(PLong.INSTANCE, (short)10));
        assertTrue(PSmallint.INSTANCE.isCoercibleTo(PLong.INSTANCE, (short)0));
        assertTrue(PSmallint.INSTANCE.isCoercibleTo(PLong.INSTANCE, (short)-10));
        assertTrue(PSmallint.INSTANCE.isCoercibleTo(PInteger.INSTANCE));
        assertTrue(PSmallint.INSTANCE.isCoercibleTo(PInteger.INSTANCE, (short)10));
        assertTrue(PSmallint.INSTANCE.isCoercibleTo(PInteger.INSTANCE, (short)0));
        assertTrue(PSmallint.INSTANCE.isCoercibleTo(PInteger.INSTANCE, (short)-10));
        assertFalse(PSmallint.INSTANCE.isCoercibleTo(PUnsignedInt.INSTANCE));
        assertTrue(PSmallint.INSTANCE.isCoercibleTo(PUnsignedInt.INSTANCE, (short)10));
        assertTrue(PSmallint.INSTANCE.isCoercibleTo(PUnsignedInt.INSTANCE, (short)0));
        assertFalse(PSmallint.INSTANCE.isCoercibleTo(PUnsignedInt.INSTANCE, (short)-10));
        assertFalse(PSmallint.INSTANCE.isCoercibleTo(PUnsignedLong.INSTANCE));
        assertTrue(PSmallint.INSTANCE.isCoercibleTo(PUnsignedLong.INSTANCE, (short)10));
        assertTrue(PSmallint.INSTANCE.isCoercibleTo(PUnsignedLong.INSTANCE, (short)0));
        assertFalse(PSmallint.INSTANCE.isCoercibleTo(PUnsignedLong.INSTANCE, (short)-10));
        assertFalse(PSmallint.INSTANCE.isCoercibleTo(PTinyint.INSTANCE));
        assertTrue(PSmallint.INSTANCE.isCoercibleTo(PTinyint.INSTANCE, (short)10));
        assertTrue(PSmallint.INSTANCE.isCoercibleTo(PTinyint.INSTANCE, (short)0));
        assertTrue(PSmallint.INSTANCE.isCoercibleTo(PTinyint.INSTANCE, (short)-10));
        assertFalse(PSmallint.INSTANCE.isCoercibleTo(PTinyint.INSTANCE, (short)1000));
        assertFalse(PSmallint.INSTANCE.isCoercibleTo(PUnsignedSmallint.INSTANCE));
        assertTrue(PSmallint.INSTANCE.isCoercibleTo(PUnsignedSmallint.INSTANCE, (short)10));
        assertTrue(PSmallint.INSTANCE.isCoercibleTo(PUnsignedSmallint.INSTANCE, (short)0));
        assertFalse(PSmallint.INSTANCE.isCoercibleTo(PUnsignedSmallint.INSTANCE, (short)-10));
        assertFalse(PSmallint.INSTANCE.isCoercibleTo(PUnsignedTinyint.INSTANCE));
        assertTrue(PSmallint.INSTANCE.isCoercibleTo(PUnsignedTinyint.INSTANCE, (short)10));
        assertTrue(PSmallint.INSTANCE.isCoercibleTo(PUnsignedTinyint.INSTANCE, (short)0));
        assertFalse(PSmallint.INSTANCE.isCoercibleTo(PUnsignedTinyint.INSTANCE, (short)-10));
        assertFalse(PSmallint.INSTANCE.isCoercibleTo(PUnsignedTinyint.INSTANCE, (short)1000));
        assertTrue(PSmallint.INSTANCE.isCoercibleTo(PUnsignedDouble.INSTANCE, (short)10));
        assertTrue(PSmallint.INSTANCE.isCoercibleTo(PUnsignedDouble.INSTANCE, (short)0));
        assertFalse(PSmallint.INSTANCE.isCoercibleTo(PUnsignedDouble.INSTANCE, (short)-1));
        assertTrue(PSmallint.INSTANCE.isCoercibleTo(PUnsignedFloat.INSTANCE, (short)10));
        assertTrue(PSmallint.INSTANCE.isCoercibleTo(PUnsignedFloat.INSTANCE, (short)0));
        assertFalse(PSmallint.INSTANCE.isCoercibleTo(PUnsignedFloat.INSTANCE, (short)-1));
        
        // Testing coercing tinyint to other values.
        assertTrue(PTinyint.INSTANCE.isCoercibleTo(PDouble.INSTANCE));
        assertTrue(PTinyint.INSTANCE.isCoercibleTo(PFloat.INSTANCE));
        assertTrue(PTinyint.INSTANCE.isCoercibleTo(PLong.INSTANCE));
        assertTrue(PTinyint.INSTANCE.isCoercibleTo(PLong.INSTANCE, (byte)10));
        assertTrue(PTinyint.INSTANCE.isCoercibleTo(PLong.INSTANCE, (byte)0));
        assertTrue(PTinyint.INSTANCE.isCoercibleTo(PLong.INSTANCE, (byte)-10));
        assertTrue(PTinyint.INSTANCE.isCoercibleTo(PInteger.INSTANCE));
        assertTrue(PTinyint.INSTANCE.isCoercibleTo(PInteger.INSTANCE, (byte)10));
        assertTrue(PTinyint.INSTANCE.isCoercibleTo(PInteger.INSTANCE, (byte)0));
        assertTrue(PTinyint.INSTANCE.isCoercibleTo(PInteger.INSTANCE, (byte)-10));
        assertTrue(PTinyint.INSTANCE.isCoercibleTo(PSmallint.INSTANCE));
        assertTrue(PTinyint.INSTANCE.isCoercibleTo(PSmallint.INSTANCE, (byte)100));
        assertTrue(PTinyint.INSTANCE.isCoercibleTo(PSmallint.INSTANCE, (byte)0));
        assertTrue(PTinyint.INSTANCE.isCoercibleTo(PSmallint.INSTANCE, (byte)-10));
        assertFalse(PTinyint.INSTANCE.isCoercibleTo(PUnsignedInt.INSTANCE));
        assertTrue(PTinyint.INSTANCE.isCoercibleTo(PUnsignedInt.INSTANCE, (byte)10));
        assertTrue(PTinyint.INSTANCE.isCoercibleTo(PUnsignedInt.INSTANCE, (byte)0));
        assertFalse(PTinyint.INSTANCE.isCoercibleTo(PUnsignedInt.INSTANCE, (byte)-10));
        assertFalse(PTinyint.INSTANCE.isCoercibleTo(PUnsignedLong.INSTANCE));
        assertTrue(PTinyint.INSTANCE.isCoercibleTo(PUnsignedLong.INSTANCE, (byte)10));
        assertTrue(PTinyint.INSTANCE.isCoercibleTo(PUnsignedLong.INSTANCE, (byte)0));
        assertFalse(PTinyint.INSTANCE.isCoercibleTo(PUnsignedLong.INSTANCE, (byte)-10));
        assertFalse(PTinyint.INSTANCE.isCoercibleTo(PUnsignedSmallint.INSTANCE));
        assertTrue(PTinyint.INSTANCE.isCoercibleTo(PUnsignedSmallint.INSTANCE, (byte)10));
        assertTrue(PTinyint.INSTANCE.isCoercibleTo(PUnsignedSmallint.INSTANCE, (byte)0));
        assertFalse(PTinyint.INSTANCE.isCoercibleTo(PUnsignedSmallint.INSTANCE, (byte)-10));
        assertFalse(PTinyint.INSTANCE.isCoercibleTo(PUnsignedTinyint.INSTANCE));
        assertTrue(PTinyint.INSTANCE.isCoercibleTo(PUnsignedTinyint.INSTANCE, (byte)10));
        assertTrue(PTinyint.INSTANCE.isCoercibleTo(PUnsignedTinyint.INSTANCE, (byte)0));
        assertFalse(PTinyint.INSTANCE.isCoercibleTo(PUnsignedTinyint.INSTANCE, (byte)-10));
        assertTrue(PTinyint.INSTANCE.isCoercibleTo(PUnsignedDouble.INSTANCE, (byte)10));
        assertTrue(PTinyint.INSTANCE.isCoercibleTo(PUnsignedDouble.INSTANCE, (byte)0));
        assertFalse(PTinyint.INSTANCE.isCoercibleTo(PUnsignedDouble.INSTANCE, (byte)-1));
        assertTrue(PTinyint.INSTANCE.isCoercibleTo(PUnsignedFloat.INSTANCE, (byte)10));
        assertTrue(PTinyint.INSTANCE.isCoercibleTo(PUnsignedFloat.INSTANCE, (byte)0));
        assertFalse(PTinyint.INSTANCE.isCoercibleTo(PUnsignedFloat.INSTANCE, (byte)-1));

        // Testing coercing unsigned_int to other values.
        assertTrue(PUnsignedInt.INSTANCE.isCoercibleTo(PDouble.INSTANCE));
        assertTrue(PUnsignedInt.INSTANCE.isCoercibleTo(PFloat.INSTANCE));
        assertTrue(PUnsignedInt.INSTANCE.isCoercibleTo(PInteger.INSTANCE));
        assertTrue(PUnsignedInt.INSTANCE.isCoercibleTo(PInteger.INSTANCE, 10));
        assertTrue(PUnsignedInt.INSTANCE.isCoercibleTo(PInteger.INSTANCE, 0));
        assertTrue(PUnsignedInt.INSTANCE.isCoercibleTo(PLong.INSTANCE));
        assertTrue(PUnsignedInt.INSTANCE.isCoercibleTo(PLong.INSTANCE, 10));
        assertTrue(PUnsignedInt.INSTANCE.isCoercibleTo(PLong.INSTANCE, 0));
        assertTrue(PUnsignedInt.INSTANCE.isCoercibleTo(PUnsignedLong.INSTANCE));
        assertTrue(PUnsignedInt.INSTANCE.isCoercibleTo(PUnsignedLong.INSTANCE, 10));
        assertTrue(PUnsignedInt.INSTANCE.isCoercibleTo(PUnsignedLong.INSTANCE, 0));
        assertFalse(PUnsignedInt.INSTANCE.isCoercibleTo(PSmallint.INSTANCE));
        assertTrue(PUnsignedInt.INSTANCE.isCoercibleTo(PSmallint.INSTANCE, 10));
        assertTrue(PUnsignedInt.INSTANCE.isCoercibleTo(PSmallint.INSTANCE, 0));
        assertFalse(PUnsignedInt.INSTANCE.isCoercibleTo(PTinyint.INSTANCE));
        assertTrue(PUnsignedInt.INSTANCE.isCoercibleTo(PTinyint.INSTANCE, 10));
        assertTrue(PUnsignedInt.INSTANCE.isCoercibleTo(PTinyint.INSTANCE, 0));
        assertFalse(PUnsignedInt.INSTANCE.isCoercibleTo(PUnsignedSmallint.INSTANCE));
        assertTrue(PUnsignedInt.INSTANCE.isCoercibleTo(PUnsignedSmallint.INSTANCE, 10));
        assertTrue(PUnsignedInt.INSTANCE.isCoercibleTo(PUnsignedSmallint.INSTANCE, 0));
        assertFalse(PUnsignedInt.INSTANCE.isCoercibleTo(PUnsignedSmallint.INSTANCE, 100000));
        assertFalse(PUnsignedInt.INSTANCE.isCoercibleTo(PUnsignedTinyint.INSTANCE));
        assertTrue(PUnsignedInt.INSTANCE.isCoercibleTo(PUnsignedTinyint.INSTANCE, 10));
        assertTrue(PUnsignedInt.INSTANCE.isCoercibleTo(PUnsignedTinyint.INSTANCE, 0));
        assertFalse(PUnsignedInt.INSTANCE.isCoercibleTo(PUnsignedTinyint.INSTANCE, 1000));
        assertTrue(PUnsignedInt.INSTANCE.isCoercibleTo(PUnsignedDouble.INSTANCE));
        assertTrue(PUnsignedInt.INSTANCE.isCoercibleTo(PUnsignedFloat.INSTANCE));

        // Testing coercing unsigned_long to other values.
        assertTrue(PUnsignedLong.INSTANCE.isCoercibleTo(PDouble.INSTANCE));
        assertFalse(PUnsignedLong.INSTANCE.isCoercibleTo(PInteger.INSTANCE));
        assertTrue(PUnsignedLong.INSTANCE.isCoercibleTo(PInteger.INSTANCE, 10L));
        assertTrue(PUnsignedLong.INSTANCE.isCoercibleTo(PInteger.INSTANCE, 0L));
        assertTrue(PUnsignedLong.INSTANCE.isCoercibleTo(PLong.INSTANCE));
        assertFalse(PUnsignedLong.INSTANCE.isCoercibleTo(PUnsignedInt.INSTANCE));
        assertFalse(PUnsignedLong.INSTANCE.isCoercibleTo(PSmallint.INSTANCE));
        assertTrue(PUnsignedLong.INSTANCE.isCoercibleTo(PSmallint.INSTANCE, 10L));
        assertTrue(PUnsignedLong.INSTANCE.isCoercibleTo(PSmallint.INSTANCE, 0L));
        assertFalse(PUnsignedLong.INSTANCE.isCoercibleTo(PTinyint.INSTANCE));
        assertTrue(PUnsignedLong.INSTANCE.isCoercibleTo(PTinyint.INSTANCE, 10L));
        assertTrue(PUnsignedLong.INSTANCE.isCoercibleTo(PTinyint.INSTANCE, 0L));
        assertFalse(PUnsignedLong.INSTANCE.isCoercibleTo(PUnsignedSmallint.INSTANCE));
        assertTrue(PUnsignedLong.INSTANCE.isCoercibleTo(PUnsignedSmallint.INSTANCE, 10L));
        assertTrue(PUnsignedLong.INSTANCE.isCoercibleTo(PUnsignedSmallint.INSTANCE, 0L));
        assertFalse(PUnsignedLong.INSTANCE.isCoercibleTo(PUnsignedSmallint.INSTANCE, 100000L));
        assertFalse(PUnsignedInt.INSTANCE.isCoercibleTo(PUnsignedTinyint.INSTANCE));
        assertTrue(PUnsignedLong.INSTANCE.isCoercibleTo(PUnsignedTinyint.INSTANCE, 10L));
        assertTrue(PUnsignedLong.INSTANCE.isCoercibleTo(PUnsignedTinyint.INSTANCE, 0L));
        assertFalse(PUnsignedLong.INSTANCE.isCoercibleTo(PUnsignedTinyint.INSTANCE, 1000L));
        assertTrue(PUnsignedLong.INSTANCE.isCoercibleTo(PUnsignedDouble.INSTANCE));
        
        // Testing coercing unsigned_smallint to other values.
        assertTrue(PUnsignedSmallint.INSTANCE.isCoercibleTo(PDouble.INSTANCE));
        assertTrue(PUnsignedSmallint.INSTANCE.isCoercibleTo(PFloat.INSTANCE));
        assertTrue(PUnsignedSmallint.INSTANCE.isCoercibleTo(PInteger.INSTANCE));
        assertTrue(PUnsignedSmallint.INSTANCE.isCoercibleTo(PInteger.INSTANCE, (short)10));
        assertTrue(PUnsignedSmallint.INSTANCE.isCoercibleTo(PInteger.INSTANCE, (short)0));
        assertTrue(PUnsignedSmallint.INSTANCE.isCoercibleTo(PLong.INSTANCE));
        assertTrue(PUnsignedSmallint.INSTANCE.isCoercibleTo(PLong.INSTANCE, (short)10));
        assertTrue(PUnsignedSmallint.INSTANCE.isCoercibleTo(PLong.INSTANCE, (short)0));
        assertTrue(PUnsignedSmallint.INSTANCE.isCoercibleTo(PUnsignedLong.INSTANCE));
        assertTrue(PUnsignedSmallint.INSTANCE.isCoercibleTo(PUnsignedLong.INSTANCE, (short)10));
        assertTrue(PUnsignedSmallint.INSTANCE.isCoercibleTo(PUnsignedLong.INSTANCE, (short)0));
        assertTrue(PUnsignedSmallint.INSTANCE.isCoercibleTo(PUnsignedInt.INSTANCE));
        assertTrue(PUnsignedSmallint.INSTANCE.isCoercibleTo(PUnsignedInt.INSTANCE, (short)10));
        assertTrue(PUnsignedSmallint.INSTANCE.isCoercibleTo(PUnsignedInt.INSTANCE, (short)0));
        assertTrue(PUnsignedSmallint.INSTANCE.isCoercibleTo(PSmallint.INSTANCE));
        assertTrue(PUnsignedSmallint.INSTANCE.isCoercibleTo(PSmallint.INSTANCE, (short)10));
        assertTrue(PUnsignedSmallint.INSTANCE.isCoercibleTo(PSmallint.INSTANCE, (short)0));
        assertFalse(PUnsignedSmallint.INSTANCE.isCoercibleTo(PTinyint.INSTANCE));
        assertTrue(PUnsignedSmallint.INSTANCE.isCoercibleTo(PTinyint.INSTANCE, (short)10));
        assertTrue(PUnsignedSmallint.INSTANCE.isCoercibleTo(PTinyint.INSTANCE, (short)0));
        assertFalse(PUnsignedSmallint.INSTANCE.isCoercibleTo(PTinyint.INSTANCE, (short)1000));
        assertFalse(PUnsignedSmallint.INSTANCE.isCoercibleTo(PUnsignedTinyint.INSTANCE));
        assertTrue(PUnsignedSmallint.INSTANCE.isCoercibleTo(PUnsignedTinyint.INSTANCE, (short)10));
        assertTrue(PUnsignedSmallint.INSTANCE.isCoercibleTo(PUnsignedTinyint.INSTANCE, (short)0));
        assertFalse(PUnsignedSmallint.INSTANCE.isCoercibleTo(PUnsignedTinyint.INSTANCE, (short)1000));
        assertTrue(PUnsignedSmallint.INSTANCE.isCoercibleTo(PUnsignedDouble.INSTANCE));
        assertTrue(PUnsignedSmallint.INSTANCE.isCoercibleTo(PUnsignedFloat.INSTANCE));
        
        // Testing coercing unsigned_tinyint to other values.
        assertTrue(PUnsignedTinyint.INSTANCE.isCoercibleTo(PDouble.INSTANCE));
        assertTrue(PUnsignedTinyint.INSTANCE.isCoercibleTo(PFloat.INSTANCE));
        assertTrue(PUnsignedTinyint.INSTANCE.isCoercibleTo(PInteger.INSTANCE));
        assertTrue(PUnsignedTinyint.INSTANCE.isCoercibleTo(PInteger.INSTANCE, (byte)10));
        assertTrue(PUnsignedTinyint.INSTANCE.isCoercibleTo(PInteger.INSTANCE, (byte)0));
        assertTrue(PUnsignedTinyint.INSTANCE.isCoercibleTo(PLong.INSTANCE));
        assertTrue(PUnsignedTinyint.INSTANCE.isCoercibleTo(PLong.INSTANCE, (byte)10));
        assertTrue(PUnsignedTinyint.INSTANCE.isCoercibleTo(PLong.INSTANCE, (byte)0));
        assertTrue(PUnsignedTinyint.INSTANCE.isCoercibleTo(PUnsignedLong.INSTANCE));
        assertTrue(PUnsignedTinyint.INSTANCE.isCoercibleTo(PUnsignedLong.INSTANCE, (byte)10));
        assertTrue(PUnsignedTinyint.INSTANCE.isCoercibleTo(PUnsignedLong.INSTANCE, (byte)0));
        assertTrue(PUnsignedTinyint.INSTANCE.isCoercibleTo(PUnsignedInt.INSTANCE));
        assertTrue(PUnsignedTinyint.INSTANCE.isCoercibleTo(PUnsignedInt.INSTANCE, (byte)10));
        assertTrue(PUnsignedTinyint.INSTANCE.isCoercibleTo(PUnsignedInt.INSTANCE, (byte)0));
        assertTrue(PUnsignedTinyint.INSTANCE.isCoercibleTo(PSmallint.INSTANCE));
        assertTrue(PUnsignedTinyint.INSTANCE.isCoercibleTo(PSmallint.INSTANCE, (byte)10));
        assertTrue(PUnsignedTinyint.INSTANCE.isCoercibleTo(PSmallint.INSTANCE, (byte)0));
        assertTrue(PUnsignedTinyint.INSTANCE.isCoercibleTo(PTinyint.INSTANCE));
        assertTrue(PUnsignedTinyint.INSTANCE.isCoercibleTo(PTinyint.INSTANCE, (byte)10));
        assertTrue(PUnsignedTinyint.INSTANCE.isCoercibleTo(PTinyint.INSTANCE, (byte)0));
        assertTrue(PUnsignedTinyint.INSTANCE.isCoercibleTo(PUnsignedSmallint.INSTANCE));
        assertTrue(PUnsignedTinyint.INSTANCE.isCoercibleTo(PUnsignedSmallint.INSTANCE, (byte)10));
        assertTrue(PUnsignedTinyint.INSTANCE.isCoercibleTo(PUnsignedSmallint.INSTANCE, (byte)0));
        assertTrue(PUnsignedTinyint.INSTANCE.isCoercibleTo(PUnsignedDouble.INSTANCE));
        assertTrue(PUnsignedTinyint.INSTANCE.isCoercibleTo(PUnsignedFloat.INSTANCE));
        
        // Testing coercing Date types
        assertTrue(PDate.INSTANCE.isCoercibleTo(PTimestamp.INSTANCE));
        assertTrue(PDate.INSTANCE.isCoercibleTo(PTime.INSTANCE));
        assertFalse(PTimestamp.INSTANCE.isCoercibleTo(PDate.INSTANCE));
        assertFalse(PTimestamp.INSTANCE.isCoercibleTo(PTime.INSTANCE));
        assertTrue(PTime.INSTANCE.isCoercibleTo(PTimestamp.INSTANCE));
        assertTrue(PTime.INSTANCE.isCoercibleTo(PDate.INSTANCE));
    }

    @Test
    public void testGetDeicmalPrecisionAndScaleFromRawBytes() throws Exception {
        // Special case for 0.
        BigDecimal bd = new BigDecimal("0");
        byte[] b = PDecimal.INSTANCE.toBytes(bd);
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
        
        Object o = PDate.INSTANCE.toObject(ts, PTimestamp.INSTANCE);
        assertEquals(o.getClass(), java.sql.Date.class);
        o = PDate.INSTANCE.toObject(t, PTime.INSTANCE);
        assertEquals(o.getClass(), java.sql.Date.class);
        
        o = PTime.INSTANCE.toObject(date, PDate.INSTANCE);
        assertEquals(o.getClass(), java.sql.Time.class);
        o = PTime.INSTANCE.toObject(ts, PTimestamp.INSTANCE);
        assertEquals(o.getClass(), java.sql.Time.class);
                
        o = PTimestamp.INSTANCE.toObject(date, PDate.INSTANCE);
        assertEquals(o.getClass(), java.sql.Timestamp.class);
        o = PTimestamp.INSTANCE.toObject(t, PTime.INSTANCE);
        assertEquals(o.getClass(), java.sql.Timestamp.class); 
    }

    @Test
    public void testNegativeDateTime() {
        Date date1 = new Date(-1000);
        Date date2 = new Date(-2000);
        assertTrue(date1.compareTo(date2) > 0);
        
        byte[] b1 = PDate.INSTANCE.toBytes(date1);
        byte[] b2 = PDate.INSTANCE.toBytes(date2);
        assertTrue(Bytes.compareTo(b1, b2) > 0);
        
    }
    
    @Test
    public void testIllegalUnsignedDateTime() {
        Date date1 = new Date(-1000);
        try {
            PUnsignedDate.INSTANCE.toBytes(date1);
            fail();
        } catch (RuntimeException e) {
            assertTrue(e.getCause() instanceof SQLException);
            SQLException sqlE = (SQLException)e.getCause();
            assertEquals(SQLExceptionCode.ILLEGAL_DATA.getErrorCode(), sqlE.getErrorCode());
        }
    }

    @Test
    public void testGetResultSetSqlType() {
        assertEquals(Types.INTEGER, PInteger.INSTANCE.getResultSetSqlType());
        assertEquals(Types.INTEGER, PUnsignedInt.INSTANCE.getResultSetSqlType());
        assertEquals(Types.BIGINT, PLong.INSTANCE.getResultSetSqlType());
        assertEquals(Types.BIGINT, PUnsignedLong.INSTANCE.getResultSetSqlType());
        assertEquals(Types.SMALLINT, PSmallint.INSTANCE.getResultSetSqlType());
        assertEquals(Types.SMALLINT, PUnsignedSmallint.INSTANCE.getResultSetSqlType());
        assertEquals(Types.TINYINT, PTinyint.INSTANCE.getResultSetSqlType());
        assertEquals(Types.TINYINT, PUnsignedTinyint.INSTANCE.getResultSetSqlType());
        assertEquals(Types.FLOAT, PFloat.INSTANCE.getResultSetSqlType());
        assertEquals(Types.FLOAT, PUnsignedFloat.INSTANCE.getResultSetSqlType());
        assertEquals(Types.DOUBLE, PDouble.INSTANCE.getResultSetSqlType());
        assertEquals(Types.DOUBLE, PUnsignedDouble.INSTANCE.getResultSetSqlType());
        assertEquals(Types.DATE, PDate.INSTANCE.getResultSetSqlType());
        assertEquals(Types.DATE, PUnsignedDate.INSTANCE.getResultSetSqlType());
        assertEquals(Types.TIME, PTime.INSTANCE.getResultSetSqlType());
        assertEquals(Types.TIME, PUnsignedTime.INSTANCE.getResultSetSqlType());
        assertEquals(Types.TIMESTAMP, PTimestamp.INSTANCE.getResultSetSqlType());
        assertEquals(Types.TIMESTAMP, PUnsignedTimestamp.INSTANCE.getResultSetSqlType());

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
        byte[] b = PDecimal.INSTANCE.toBytes(bd);
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
    
    @Test
    public void testGetSampleValue() {
        PDataType[] types = PDataType.values();
        // Test validity of 10 sample values for each type
        for (int i = 0; i < 10; i++) {
            for (PDataType type : types) {
                Integer maxLength = 
                        (type == PChar.INSTANCE
                        || type == PBinary.INSTANCE
                        || type == PCharArray.INSTANCE
                        || type == PBinaryArray.INSTANCE) ? 10 : null;
                int arrayLength = 10;
                Object sampleValue = type.getSampleValue(maxLength, arrayLength);
                byte[] b = type.toBytes(sampleValue);
                type.toObject(b, 0, b.length, type, SortOrder.getDefault(), maxLength, null);
            }
        }
    }

    // Simulate what an HBase Increment does with the value encoded as a long
    private long nextValueFor(long startWith, long incrementBy) {
        long hstartWith = Bytes.toLong(PLong.INSTANCE.toBytes(startWith));
        hstartWith += incrementBy;
        return (Long) PLong.INSTANCE.toObject(Bytes.toBytes(hstartWith));
    }
    

    @Test
    public void testCoercibleGoldfile() {
        TreeMultimap<String, String> coercibleToMap = TreeMultimap.create();
        PDataType[] orderedTypes = PDataTypeFactory.getInstance().getOrderedTypes();
        for (PDataType fromType : orderedTypes) {
            for (PDataType targetType : orderedTypes) {
                if (fromType.isCoercibleTo(targetType)) {
                    coercibleToMap.put(fromType.toString(), targetType.toString());
                }
            }
        }
        assertEquals(
              "{BIGINT=[BIGINT, BINARY, DECIMAL, DOUBLE, VARBINARY], "
             + "BIGINT ARRAY=[BIGINT ARRAY, BINARY ARRAY, DECIMAL ARRAY, DOUBLE ARRAY, VARBINARY ARRAY], "
             + "BINARY=[BINARY, VARBINARY], "
             + "BINARY ARRAY=[BINARY ARRAY, VARBINARY ARRAY], "
             + "BOOLEAN=[BINARY, BOOLEAN, VARBINARY], "
             + "BOOLEAN ARRAY=[BINARY ARRAY, BOOLEAN ARRAY, VARBINARY ARRAY], "
             + "CHAR=[BINARY, CHAR, VARBINARY, VARCHAR], "
             + "CHAR ARRAY=[BINARY ARRAY, CHAR ARRAY, VARBINARY ARRAY, VARCHAR ARRAY], "
             + "DATE=[BINARY, DATE, TIME, TIMESTAMP, VARBINARY], "
             + "DATE ARRAY=[BINARY ARRAY, DATE ARRAY, TIME ARRAY, TIMESTAMP ARRAY, VARBINARY ARRAY], "
             + "DECIMAL=[DECIMAL, VARBINARY], "
             + "DECIMAL ARRAY=[DECIMAL ARRAY, VARBINARY ARRAY], "
             + "DOUBLE=[BINARY, DECIMAL, DOUBLE, VARBINARY], "
             + "DOUBLE ARRAY=[BINARY ARRAY, DECIMAL ARRAY, DOUBLE ARRAY, VARBINARY ARRAY], "
             + "FLOAT=[BINARY, DECIMAL, DOUBLE, FLOAT, VARBINARY], "
             + "FLOAT ARRAY=[BINARY ARRAY, DECIMAL ARRAY, DOUBLE ARRAY, FLOAT ARRAY, VARBINARY ARRAY], "
             + "INTEGER=[BIGINT, BINARY, DECIMAL, DOUBLE, FLOAT, INTEGER, VARBINARY], "
             + "INTEGER ARRAY=[BIGINT ARRAY, BINARY ARRAY, DECIMAL ARRAY, DOUBLE ARRAY, FLOAT ARRAY, INTEGER ARRAY, VARBINARY ARRAY], "
             + "SMALLINT=[BIGINT, BINARY, DECIMAL, DOUBLE, FLOAT, INTEGER, SMALLINT, VARBINARY], "
             + "SMALLINT ARRAY=[BIGINT ARRAY, BINARY ARRAY, DECIMAL ARRAY, DOUBLE ARRAY, FLOAT ARRAY, INTEGER ARRAY, SMALLINT ARRAY, VARBINARY ARRAY], "
             + "TIME=[BINARY, DATE, TIME, TIMESTAMP, VARBINARY], "
             + "TIME ARRAY=[BINARY ARRAY, DATE ARRAY, TIME ARRAY, TIMESTAMP ARRAY, VARBINARY ARRAY], "
             + "TIMESTAMP=[BINARY, TIMESTAMP, VARBINARY], "
             + "TIMESTAMP ARRAY=[BINARY ARRAY, TIMESTAMP ARRAY, VARBINARY ARRAY], "
             + "TINYINT=[BIGINT, BINARY, DECIMAL, DOUBLE, FLOAT, INTEGER, SMALLINT, TINYINT, VARBINARY], "
             + "TINYINT ARRAY=[BIGINT ARRAY, BINARY ARRAY, DECIMAL ARRAY, DOUBLE ARRAY, FLOAT ARRAY, INTEGER ARRAY, SMALLINT ARRAY, TINYINT ARRAY, VARBINARY ARRAY], "
             + "UNSIGNED_DATE=[BINARY, DATE, TIME, TIMESTAMP, UNSIGNED_DATE, UNSIGNED_TIME, UNSIGNED_TIMESTAMP, VARBINARY], "
             + "UNSIGNED_DATE ARRAY=[BINARY ARRAY, DATE ARRAY, TIME ARRAY, TIMESTAMP ARRAY, UNSIGNED_DATE ARRAY, UNSIGNED_TIME ARRAY, UNSIGNED_TIMESTAMP ARRAY, VARBINARY ARRAY], "
             + "UNSIGNED_DOUBLE=[BINARY, DECIMAL, DOUBLE, UNSIGNED_DOUBLE, VARBINARY], "
             + "UNSIGNED_DOUBLE ARRAY=[BINARY ARRAY, DECIMAL ARRAY, DOUBLE ARRAY, UNSIGNED_DOUBLE ARRAY, VARBINARY ARRAY], "
             + "UNSIGNED_FLOAT=[BINARY, DECIMAL, DOUBLE, FLOAT, UNSIGNED_DOUBLE, UNSIGNED_FLOAT, VARBINARY], "
             + "UNSIGNED_FLOAT ARRAY=[BINARY ARRAY, DECIMAL ARRAY, DOUBLE ARRAY, FLOAT ARRAY, UNSIGNED_DOUBLE ARRAY, UNSIGNED_FLOAT ARRAY, VARBINARY ARRAY], "
             + "UNSIGNED_INT=[BIGINT, BINARY, DECIMAL, DOUBLE, FLOAT, INTEGER, UNSIGNED_DOUBLE, UNSIGNED_FLOAT, UNSIGNED_INT, UNSIGNED_LONG, VARBINARY], "
             + "UNSIGNED_INT ARRAY=[BIGINT ARRAY, BINARY ARRAY, DECIMAL ARRAY, DOUBLE ARRAY, FLOAT ARRAY, INTEGER ARRAY, UNSIGNED_DOUBLE ARRAY, UNSIGNED_FLOAT ARRAY, UNSIGNED_INT ARRAY, UNSIGNED_LONG ARRAY, VARBINARY ARRAY], "
             + "UNSIGNED_LONG=[BIGINT, BINARY, DECIMAL, DOUBLE, UNSIGNED_DOUBLE, UNSIGNED_LONG, VARBINARY], "
             + "UNSIGNED_LONG ARRAY=[BIGINT ARRAY, BINARY ARRAY, DECIMAL ARRAY, DOUBLE ARRAY, UNSIGNED_DOUBLE ARRAY, UNSIGNED_LONG ARRAY, VARBINARY ARRAY], "
             + "UNSIGNED_SMALLINT=[BIGINT, BINARY, DECIMAL, DOUBLE, FLOAT, INTEGER, SMALLINT, UNSIGNED_DOUBLE, UNSIGNED_FLOAT, UNSIGNED_INT, UNSIGNED_LONG, UNSIGNED_SMALLINT, VARBINARY], "
             + "UNSIGNED_SMALLINT ARRAY=[BIGINT ARRAY, BINARY ARRAY, DECIMAL ARRAY, DOUBLE ARRAY, FLOAT ARRAY, INTEGER ARRAY, SMALLINT ARRAY, UNSIGNED_DOUBLE ARRAY, UNSIGNED_FLOAT ARRAY, UNSIGNED_INT ARRAY, UNSIGNED_LONG ARRAY, UNSIGNED_SMALLINT ARRAY, VARBINARY ARRAY], "
             + "UNSIGNED_TIME=[BINARY, DATE, TIME, TIMESTAMP, UNSIGNED_DATE, UNSIGNED_TIME, UNSIGNED_TIMESTAMP, VARBINARY], "
             + "UNSIGNED_TIME ARRAY=[BINARY ARRAY, DATE ARRAY, TIME ARRAY, TIMESTAMP ARRAY, UNSIGNED_DATE ARRAY, UNSIGNED_TIME ARRAY, UNSIGNED_TIMESTAMP ARRAY, VARBINARY ARRAY], "
             + "UNSIGNED_TIMESTAMP=[BINARY, DATE, TIME, TIMESTAMP, UNSIGNED_DATE, UNSIGNED_TIME, UNSIGNED_TIMESTAMP, VARBINARY], "
             + "UNSIGNED_TIMESTAMP ARRAY=[BINARY ARRAY, DATE ARRAY, TIME ARRAY, TIMESTAMP ARRAY, UNSIGNED_DATE ARRAY, UNSIGNED_TIME ARRAY, UNSIGNED_TIMESTAMP ARRAY, VARBINARY ARRAY], "
             + "UNSIGNED_TINYINT=[BIGINT, BINARY, DECIMAL, DOUBLE, FLOAT, INTEGER, SMALLINT, TINYINT, UNSIGNED_DOUBLE, UNSIGNED_FLOAT, UNSIGNED_INT, UNSIGNED_LONG, UNSIGNED_SMALLINT, UNSIGNED_TINYINT, VARBINARY], "
             + "UNSIGNED_TINYINT ARRAY=[BIGINT ARRAY, BINARY ARRAY, DECIMAL ARRAY, DOUBLE ARRAY, FLOAT ARRAY, INTEGER ARRAY, SMALLINT ARRAY, TINYINT ARRAY, UNSIGNED_DOUBLE ARRAY, UNSIGNED_FLOAT ARRAY, UNSIGNED_INT ARRAY, UNSIGNED_LONG ARRAY, UNSIGNED_SMALLINT ARRAY, UNSIGNED_TINYINT ARRAY, VARBINARY ARRAY], "
             + "VARBINARY=[BINARY, VARBINARY], "
             + "VARBINARY ARRAY=[BINARY ARRAY, VARBINARY ARRAY], "
             + "VARCHAR=[BINARY, CHAR, VARBINARY, VARCHAR], "
             + "VARCHAR ARRAY=[BINARY ARRAY, CHAR ARRAY, VARBINARY ARRAY, VARCHAR ARRAY]}", 
             coercibleToMap.toString());
    }
    
    @Test
    public void testIntVersusLong() {
        long l = -1L;
        int i = -1;
        assertTrue(PLong.INSTANCE.compareTo(l, i, PInteger.INSTANCE)==0);
        assertTrue(PInteger.INSTANCE.compareTo(i, l, PLong.INSTANCE)==0);
    }
}
