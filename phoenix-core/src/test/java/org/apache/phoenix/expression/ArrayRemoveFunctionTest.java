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

import static org.junit.Assert.assertEquals;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.function.ArrayRemoveFunction;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.types.PBoolean;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PDate;
import org.apache.phoenix.schema.types.PDecimal;
import org.apache.phoenix.schema.types.PDouble;
import org.apache.phoenix.schema.types.PFloat;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.schema.types.PSmallint;
import org.apache.phoenix.schema.types.PTime;
import org.apache.phoenix.schema.types.PTimestamp;
import org.apache.phoenix.schema.types.PTinyint;
import org.apache.phoenix.schema.types.PVarbinary;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.schema.types.PhoenixArray;
import org.junit.Test;

import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;

public class ArrayRemoveFunctionTest {

	private static void testExpression(LiteralExpression array, LiteralExpression element, PhoenixArray expected)
			throws SQLException {
		List<Expression> expressions = Lists.newArrayList((Expression) element);
		expressions.add(array);

		Expression arrayRemoveFunction = new ArrayRemoveFunction(expressions);
		ImmutableBytesWritable ptr = new ImmutableBytesWritable();
		arrayRemoveFunction.evaluate(null, ptr);
		PhoenixArray result = (PhoenixArray) arrayRemoveFunction.getDataType().toObject(ptr,
				expressions.get(1).getSortOrder(), array.getMaxLength(), array.getScale());
		assertEquals(expected, result);
	}

	private static void test(PhoenixArray array, Object element, PDataType arrayDataType, Integer arrMaxLen,
			Integer arrScale, PDataType elementDataType, Integer elemMaxLen, Integer elemScale, PhoenixArray expected,
			SortOrder arraySortOrder, SortOrder elementSortOrder) throws SQLException {
		LiteralExpression arrayLiteral, elementLiteral;
		arrayLiteral = LiteralExpression.newConstant(array, arrayDataType, arrMaxLen, arrScale, arraySortOrder,
				Determinism.ALWAYS);
		elementLiteral = LiteralExpression.newConstant(element, elementDataType, elemMaxLen, elemScale,
				elementSortOrder, Determinism.ALWAYS);
		testExpression(arrayLiteral, elementLiteral, expected);
	}

	@Test
	public void testArrayRemoveFunction1() throws Exception {
		Object[] o = new Object[] { 1, 2, -3, 4 };
		Object[] o2 = new Object[] { 1, 2, -3 };
		Object element = 4;
		PDataType baseType = PInteger.INSTANCE;

		PhoenixArray arr = new PhoenixArray.PrimitiveIntPhoenixArray(baseType, o);
		PhoenixArray expected = new PhoenixArray.PrimitiveIntPhoenixArray(baseType, o2);
		test(arr, element, PDataType.fromTypeId(baseType.getSqlType() + PDataType.ARRAY_TYPE_BASE), null, null,
				baseType, null, null, expected, SortOrder.ASC, SortOrder.ASC);
	}

	@Test
	public void testArrayRemoveFunction2() throws Exception {
		Object[] o = new Object[] { "1", "2", "3", "4" };
		Object[] o2 = new Object[] { "1", "3", "4" };
		Object element = "2";
		PDataType baseType = PVarchar.INSTANCE;

		PhoenixArray arr = new PhoenixArray(baseType, o);
		PhoenixArray expected = new PhoenixArray(baseType, o2);
		test(arr, element, PDataType.fromTypeId(baseType.getSqlType() + PDataType.ARRAY_TYPE_BASE), null, null,
				baseType, null, null, expected, SortOrder.ASC, SortOrder.ASC);
	}

	@Test
	public void testArrayRemoveFunction3() throws Exception {
		Object[] o = new Object[] { "1", "2", "2", "4" };
		Object[] o2 = new Object[] { "1", "4" };
		Object element = "2";
		PDataType baseType = PVarchar.INSTANCE;

		PhoenixArray arr = new PhoenixArray(baseType, o);
		PhoenixArray expected = new PhoenixArray(baseType, o2);
		test(arr, element, PDataType.fromTypeId(baseType.getSqlType() + PDataType.ARRAY_TYPE_BASE), null, null,
				baseType, null, null, expected, SortOrder.ASC, SortOrder.ASC);
	}

	@Test
	public void testArrayRemoveFunction4() throws Exception {
		Object[] o = new Object[] { "1", "2", "2", "4" };
		Object[] o2 = new Object[] { "1", "2", "2", "4" };
		Object element = "5";
		PDataType baseType = PVarchar.INSTANCE;

		PhoenixArray arr = new PhoenixArray(baseType, o);
		PhoenixArray expected = new PhoenixArray(baseType, o2);
		test(arr, element, PDataType.fromTypeId(baseType.getSqlType() + PDataType.ARRAY_TYPE_BASE), null, null,
				baseType, null, null, expected, SortOrder.ASC, SortOrder.ASC);
	}

	@Test
	public void testArrayRemoveFunctionBoolean() throws Exception {
		Boolean[] o = new Boolean[] { true, false, false, true };
		Boolean[] o2 = new Boolean[] { true, true };
		Boolean element = false;
		PDataType baseType = PBoolean.INSTANCE;

		PhoenixArray arr = new PhoenixArray.PrimitiveBooleanPhoenixArray(baseType, o);
		PhoenixArray expected = new PhoenixArray.PrimitiveBooleanPhoenixArray(baseType, o2);
		test(arr, element, PDataType.fromTypeId(baseType.getSqlType() + PDataType.ARRAY_TYPE_BASE), null, null,
				baseType, null, null, expected, SortOrder.ASC, SortOrder.ASC);
	}

	@Test
	public void testArrayRemoveFunction6() throws Exception {
		Object[] o = new Object[] { new Float(2.3), new Float(7.9), new Float(-9.6), new Float(2.3) };
		Object[] o2 = new Object[] { new Float(7.9), new Float(-9.6) };
		Object element = 2.3;
		PDataType baseType = PFloat.INSTANCE;

		PhoenixArray arr = new PhoenixArray.PrimitiveFloatPhoenixArray(baseType, o);
		PhoenixArray expected = new PhoenixArray.PrimitiveFloatPhoenixArray(baseType, o2);
		test(arr, element, PDataType.fromTypeId(baseType.getSqlType() + PDataType.ARRAY_TYPE_BASE), null, null,
				baseType, null, null, expected, SortOrder.ASC, SortOrder.ASC);
	}

	@Test
	public void testArrayRemoveFunction7() throws Exception {
		Object[] o = new Object[] { 4.78, 9.54, 2.34, -9.675, Double.MAX_VALUE };
		Object[] o2 = new Object[] { 9.54, 2.34, -9.675, Double.MAX_VALUE };
		Object element = 4.78;
		PDataType baseType = PDouble.INSTANCE;

		PhoenixArray arr = new PhoenixArray.PrimitiveDoublePhoenixArray(baseType, o);
		PhoenixArray expected = new PhoenixArray.PrimitiveDoublePhoenixArray(baseType, o2);
		test(arr, element, PDataType.fromTypeId(baseType.getSqlType() + PDataType.ARRAY_TYPE_BASE), null, null,
				baseType, null, null, expected, SortOrder.ASC, SortOrder.ASC);
	}

	@Test
	public void testArrayRemoveFunction8() throws Exception {
		Object[] o = new Object[] { 123l, 677l, 98789l, -78989l, 66787l };
		Object[] o2 = new Object[] { 123l, 677l, -78989l, 66787l };
		Object element = 98789l;
		PDataType baseType = PLong.INSTANCE;

		PhoenixArray arr = new PhoenixArray.PrimitiveLongPhoenixArray(baseType, o);
		PhoenixArray expected = new PhoenixArray.PrimitiveLongPhoenixArray(baseType, o2);
		test(arr, element, PDataType.fromTypeId(baseType.getSqlType() + PDataType.ARRAY_TYPE_BASE), null, null,
				baseType, null, null, expected, SortOrder.ASC, SortOrder.ASC);
	}

	@Test
	public void testArrayRemoveFunction9() throws Exception {
		Object[] o = new Object[] { (short) 34, (short) -89, (short) 999, (short) 34 };
		Object[] o2 = new Object[] { (short) 34, (short) -89, (short) 999, (short) 34 };
		Object element = (short) -23;
		PDataType baseType = PSmallint.INSTANCE;

		PhoenixArray arr = new PhoenixArray.PrimitiveShortPhoenixArray(baseType, o);
		PhoenixArray expected = new PhoenixArray.PrimitiveShortPhoenixArray(baseType, o2);
		test(arr, element, PDataType.fromTypeId(baseType.getSqlType() + PDataType.ARRAY_TYPE_BASE), null, null,
				baseType, null, null, expected, SortOrder.ASC, SortOrder.ASC);
	}

	@Test
	public void testArrayRemoveFunction10() throws Exception {
		Object[] o = new Object[] { (byte) 4, (byte) 8, (byte) 9 };
		Object[] o2 = new Object[] { (byte) 8, (byte) 9 };
		Object element = (byte) 4;
		PDataType baseType = PTinyint.INSTANCE;

		PhoenixArray arr = new PhoenixArray.PrimitiveBytePhoenixArray(baseType, o);
		PhoenixArray expected = new PhoenixArray.PrimitiveBytePhoenixArray(baseType, o2);
		test(arr, element, PDataType.fromTypeId(baseType.getSqlType() + PDataType.ARRAY_TYPE_BASE), null, null,
				baseType, null, null, expected, SortOrder.ASC, SortOrder.ASC);
	}

	@Test
	public void testArrayRemoveFunction11() throws Exception {
		Object[] o = new Object[] { BigDecimal.valueOf(2345), BigDecimal.valueOf(-23.45), BigDecimal.valueOf(785) };
		Object[] o2 = new Object[] { BigDecimal.valueOf(-23.45), BigDecimal.valueOf(785) };
		Object element = BigDecimal.valueOf(2345);
		PDataType baseType = PDecimal.INSTANCE;

		PhoenixArray arr = new PhoenixArray(baseType, o);
		PhoenixArray expected = new PhoenixArray(baseType, o2);
		test(arr, element, PDataType.fromTypeId(baseType.getSqlType() + PDataType.ARRAY_TYPE_BASE), null, null,
				baseType, null, null, expected, SortOrder.ASC, SortOrder.ASC);
	}

	@Test
	public void testArrayRemoveFunction12() throws Exception {
		Calendar calendar = Calendar.getInstance();
		java.util.Date currentDate = calendar.getTime();
		java.sql.Date date = new java.sql.Date(currentDate.getTime());
		Date date2 = new Date(new java.util.Date().getTime() + 1000);

		Object[] o = new Object[] { date, date, date, date2 };
		Object[] o2 = new Object[] { date2 };
		PDataType baseType = PDate.INSTANCE;

		PhoenixArray arr = new PhoenixArray(baseType, o);
		PhoenixArray expected = new PhoenixArray(baseType, o2);
		test(arr, date, PDataType.fromTypeId(baseType.getSqlType() + PDataType.ARRAY_TYPE_BASE), null, null, baseType,
				null, null, expected, SortOrder.ASC, SortOrder.ASC);
	}

	@Test
	public void testArrayRemoveFunction13() throws Exception {
		Calendar calendar = Calendar.getInstance();
		java.util.Date currentDate = calendar.getTime();
		java.sql.Time time = new java.sql.Time(currentDate.getTime());
		java.sql.Time time2 = new java.sql.Time(new java.util.Date().getTime() + 1000);

		Object[] o = new Object[] { time, time, time, time2 };
		Object[] o2 = new Object[] { time2 };
		PDataType baseType = PTime.INSTANCE;

		PhoenixArray arr = new PhoenixArray(baseType, o);
		PhoenixArray expected = new PhoenixArray(baseType, o2);
		test(arr, time, PDataType.fromTypeId(baseType.getSqlType() + PDataType.ARRAY_TYPE_BASE), null, null, baseType,
				null, null, expected, SortOrder.ASC, SortOrder.ASC);
	}

	@Test
	public void testArrayRemoveFunction14() throws Exception {
		Calendar calendar = Calendar.getInstance();
		java.util.Date currentDate = calendar.getTime();
		java.sql.Timestamp timestamp = new java.sql.Timestamp(currentDate.getTime());
		java.sql.Timestamp timestamp2 = new java.sql.Timestamp(new java.util.Date().getTime() + 1000);

		Object[] o = new Object[] { timestamp, timestamp2, timestamp, timestamp };
		Object[] o2 = new Object[] { timestamp2 };
		PDataType baseType = PTimestamp.INSTANCE;

		PhoenixArray arr = new PhoenixArray(baseType, o);
		PhoenixArray expected = new PhoenixArray(baseType, o2);
		test(arr, timestamp, PDataType.fromTypeId(baseType.getSqlType() + PDataType.ARRAY_TYPE_BASE), null, null,
				baseType, null, null, expected, SortOrder.ASC, SortOrder.ASC);
	}

	@Test
	public void testArrayRemoveFunction15() throws Exception {
		byte[][] o = new byte[][] { new byte[] { 2, 0, 3 }, new byte[] { 42, 3 }, new byte[] { 5, 3 },
				new byte[] { 6, 3 }, new byte[] { 2, 5 } };
		byte[][] o2 = new byte[][] { new byte[] { 42, 3 }, new byte[] { 5, 3 }, new byte[] { 6, 3 },
				new byte[] { 2, 5 } };
		byte[] element = new byte[] { 2, 0, 3 };
		PDataType baseType = PVarbinary.INSTANCE;

		PhoenixArray arr = new PhoenixArray(baseType, o);
		PhoenixArray expected = new PhoenixArray(baseType, o2);
		test(arr, element, PDataType.fromTypeId(baseType.getSqlType() + PDataType.ARRAY_TYPE_BASE), null, null,
				baseType, 1, null, expected, SortOrder.ASC, SortOrder.DESC);
	}

}
