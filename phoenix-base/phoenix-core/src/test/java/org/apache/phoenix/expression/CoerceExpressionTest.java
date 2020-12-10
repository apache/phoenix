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

import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.HashMap;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.schema.types.PBinary;
import org.apache.phoenix.schema.types.PChar;
import org.apache.phoenix.schema.types.PDecimal;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.schema.types.PVarbinary;
import org.apache.phoenix.schema.types.PVarchar;
import org.junit.Test;

import org.apache.phoenix.schema.types.PDataType;

/**
 * Test class for unit-testing {@link CoerceExpression}
 * 
 * 
 * @since 0.1
 * 
 */
public class CoerceExpressionTest {
    
	private static final HashMap<Class, Object> map = new HashMap<Class, Object>();
	
	static {
		map.put(String.class, "a");
		map.put(Long.class, 1l);	
		map.put(Integer.class, 1);
		map.put(Short.class, 1);
		map.put(Byte.class, 1);
		map.put(Float.class, 1.00f);
		map.put(Double.class, 1.00d);
		map.put(BigDecimal.class, BigDecimal.ONE);
		map.put(Timestamp.class, new Timestamp(0));
		map.put(Time.class, new Time(0));
		map.put(Date.class, new Date(0));
		map.put(Boolean.class, Boolean.TRUE);
		map.put(byte[].class, new byte[]{-128, 0, 0, 1});
	}
	
	@Test
    public void testCoerceExpressionSupportsCoercingIntToDecimal() throws Exception {
        LiteralExpression v = LiteralExpression.newConstant(1, PInteger.INSTANCE);
        CoerceExpression e = new CoerceExpression(v, PDecimal.INSTANCE);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        e.evaluate(null, ptr);
        Object obj = e.getDataType().toObject(ptr);
        assertTrue(obj instanceof BigDecimal);
        BigDecimal value = (BigDecimal)obj;
        assertTrue(value.equals(BigDecimal.valueOf(1)));
    }
	
	@Test
    public void testCoerceExpressionSupportsCoercingCharToVarchar() throws Exception {
        LiteralExpression v = LiteralExpression.newConstant("a", PChar.INSTANCE);
        CoerceExpression e = new CoerceExpression(v, PVarchar.INSTANCE);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        e.evaluate(null, ptr);
        Object obj = e.getDataType().toObject(ptr);
        assertTrue(obj instanceof String);
        String value = (String)obj;
        assertTrue(value.equals("a"));
    }
	
	@Test
    public void testCoerceExpressionSupportsCoercingIntToLong() throws Exception {
        LiteralExpression v = LiteralExpression.newConstant(1, PInteger.INSTANCE);
        CoerceExpression e = new CoerceExpression(v, PLong.INSTANCE);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        e.evaluate(null, ptr);
        Object obj = e.getDataType().toObject(ptr);
        assertTrue(obj instanceof Long);
        Long value = (Long)obj;
        assertTrue(value.equals(Long.valueOf(1)));
    }
	
	@Test
	public void testCoerceExpressionSupportsCoercingAllPDataTypesToVarBinary() throws Exception {
		for(PDataType p : PDataType.values()) {
			if (!p.isArrayType()) {
				LiteralExpression v = LiteralExpression.newConstant(
						map.get(p.getJavaClass()), p);
				CoerceExpression e = new CoerceExpression(v,
            PVarbinary.INSTANCE);
				ImmutableBytesWritable ptr = new ImmutableBytesWritable();
				e.evaluate(null, ptr);
				Object obj = e.getDataType().toObject(ptr);
				assertTrue("Coercing to VARBINARY failed for PDataType " + p,
						obj instanceof byte[]);
			}
		}
	}
	

	@Test
    public void testCoerceExpressionSupportsCoercingAllPDataTypesToBinary() throws Exception {
		for(PDataType p : PDataType.values()) {
			if (!p.isArrayType()) {
				LiteralExpression v = LiteralExpression.newConstant(
						map.get(p.getJavaClass()), p);
				CoerceExpression e = new CoerceExpression(v, PBinary.INSTANCE);
				ImmutableBytesWritable ptr = new ImmutableBytesWritable();
				e.evaluate(null, ptr);
				Object obj = e.getDataType().toObject(ptr);
				assertTrue("Coercing to BINARY failed for PDataType " + p,
						obj instanceof byte[]);
			}
		}
    }
	
}
