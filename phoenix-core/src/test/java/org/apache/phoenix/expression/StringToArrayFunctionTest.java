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

import java.sql.SQLException;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.function.StringToArrayFunction;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.types.*;
import org.junit.Test;

import com.google.common.collect.Lists;

public class StringToArrayFunctionTest {

    private static void testExpression(LiteralExpression array, LiteralExpression delimiter, LiteralExpression nullString, PhoenixArray expected)
            throws SQLException {
        List<Expression> expressions = Lists.newArrayList((Expression) array);
        expressions.add(delimiter);
        expressions.add(nullString);

        Expression stringToArrayFunction = new StringToArrayFunction(expressions);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        stringToArrayFunction.evaluate(null, ptr);
        PhoenixArray result = (PhoenixArray) stringToArrayFunction.getDataType().toObject(ptr, stringToArrayFunction.getSortOrder(), stringToArrayFunction.getMaxLength(), stringToArrayFunction.getScale());
        assertEquals(expected, result);
    }

    private static void test(String string, String delimiter, String nullString, PhoenixArray expected, SortOrder stringSortOrder, SortOrder delimiterSortOrder, SortOrder nullStringSortOrder, PDataType stringType, PDataType delimiterType, PDataType nullStringType) throws SQLException {
        LiteralExpression arrayLiteral, delimiterLiteral, nullStringLiteral;
        arrayLiteral = LiteralExpression.newConstant(string, stringType, null, null, stringSortOrder, Determinism.ALWAYS);
        delimiterLiteral = LiteralExpression.newConstant(delimiter, delimiterType, null, null, delimiterSortOrder, Determinism.ALWAYS);
        nullStringLiteral = LiteralExpression.newConstant(nullString, nullStringType, null, null, nullStringSortOrder, Determinism.ALWAYS);
        testExpression(arrayLiteral, delimiterLiteral, nullStringLiteral, expected);
    }

    @Test
    public void testStringToArrayFunction1() throws SQLException {
        String string = "1,2,3,4,5";
        Object[] o1 = new Object[]{"1", "2", "3", "4", "5"};
        PhoenixArray expected = new PhoenixArray(PVarchar.INSTANCE, o1);
        String delimiter = ",";
        String nullString = "*";
        test(string, delimiter, nullString, expected, SortOrder.ASC, SortOrder.ASC, SortOrder.ASC, PVarchar.INSTANCE, PVarchar.INSTANCE, PVarchar.INSTANCE);
        test(string, delimiter, nullString, expected, SortOrder.DESC, SortOrder.ASC, SortOrder.ASC, PVarchar.INSTANCE, PVarchar.INSTANCE, PVarchar.INSTANCE);
    }

    @Test
    public void testStringToArrayFunction2() throws SQLException {
        String string = "1,2,3,4,5";
        Object[] o1 = new Object[]{"1", "2", "3", "4", "5"};
        PhoenixArray expected = new PhoenixArray(PVarchar.INSTANCE, o1);
        String delimiter = ",";
        String nullString = "";
        test(string, delimiter, nullString, expected, SortOrder.ASC, SortOrder.ASC, SortOrder.ASC, PVarchar.INSTANCE, PVarchar.INSTANCE, PVarchar.INSTANCE);
        test(string, delimiter, nullString, expected, SortOrder.DESC, SortOrder.ASC, SortOrder.ASC, PVarchar.INSTANCE, PVarchar.INSTANCE, PVarchar.INSTANCE);
    }

    @Test
    public void testStringToArrayFunction3() throws SQLException {
        String string = "1234";
        Object[] o1 = new Object[]{"1", "2", "3", "4"};
        PhoenixArray expected = new PhoenixArray(PVarchar.INSTANCE, o1);
        String delimiter = null;
        String nullString = "";
        test(string, delimiter, nullString, expected, SortOrder.ASC, SortOrder.ASC, SortOrder.ASC, PVarchar.INSTANCE, PVarchar.INSTANCE, PVarchar.INSTANCE);
        test(string, delimiter, nullString, expected, SortOrder.DESC, SortOrder.ASC, SortOrder.ASC, PVarchar.INSTANCE, PVarchar.INSTANCE, PVarchar.INSTANCE);
    }

    @Test
    public void testStringToArrayFunction4() throws SQLException {
        String string = "1";
        Object[] o1 = new Object[]{"1"};
        PhoenixArray expected = new PhoenixArray(PVarchar.INSTANCE, o1);
        String delimiter = ",";
        String nullString = "";
        test(string, delimiter, nullString, expected, SortOrder.ASC, SortOrder.ASC, SortOrder.ASC, PVarchar.INSTANCE, PVarchar.INSTANCE, PVarchar.INSTANCE);
        test(string, delimiter, nullString, expected, SortOrder.DESC, SortOrder.ASC, SortOrder.ASC, PVarchar.INSTANCE, PVarchar.INSTANCE, PVarchar.INSTANCE);
    }

    @Test
    public void testStringToArrayFunction5() throws SQLException {
        String string = "hello, hello, hello";
        Object[] o1 = new Object[]{"hello", "hello", "hello"};
        PhoenixArray expected = new PhoenixArray(PVarchar.INSTANCE, o1);
        String delimiter = ", ";
        String nullString = "";
        test(string, delimiter, nullString, expected, SortOrder.ASC, SortOrder.ASC, SortOrder.ASC, PVarchar.INSTANCE, PVarchar.INSTANCE, PVarchar.INSTANCE);
        test(string, delimiter, nullString, expected, SortOrder.DESC, SortOrder.ASC, SortOrder.ASC, PVarchar.INSTANCE, PVarchar.INSTANCE, PVarchar.INSTANCE);
    }

    @Test
    public void testStringToArrayFunction6() throws SQLException {
        String string = "1.2...2.3...5.6";
        Object[] o1 = new Object[]{"1.2", "2.3", "5.6"};
        PhoenixArray expected = new PhoenixArray(PVarchar.INSTANCE, o1);
        String delimiter = "...";
        String nullString = "";
        test(string, delimiter, nullString, expected, SortOrder.ASC, SortOrder.ASC, SortOrder.ASC, PVarchar.INSTANCE, PVarchar.INSTANCE, PVarchar.INSTANCE);
        test(string, delimiter, nullString, expected, SortOrder.DESC, SortOrder.ASC, SortOrder.ASC, PVarchar.INSTANCE, PVarchar.INSTANCE, PVarchar.INSTANCE);
    }

    @Test
    public void testStringToArrayFunction7() throws SQLException {
        String string = "a\\b\\c\\d\\e\\f";
        Object[] o1 = new Object[]{"a", "b", "c", "d", "e", "f"};
        PhoenixArray expected = new PhoenixArray(PVarchar.INSTANCE, o1);
        String delimiter = "\\";
        String nullString = "";
        test(string, delimiter, nullString, expected, SortOrder.ASC, SortOrder.ASC, SortOrder.ASC, PVarchar.INSTANCE, PVarchar.INSTANCE, PVarchar.INSTANCE);
        test(string, delimiter, nullString, expected, SortOrder.DESC, SortOrder.ASC, SortOrder.ASC, PVarchar.INSTANCE, PVarchar.INSTANCE, PVarchar.INSTANCE);
    }

    @Test
    public void testStringToArrayFunction8() throws SQLException {
        String string = "a-b-c-d-e-f-";
        Object[] o1 = new Object[]{"a", "b", "c", "d", "e", "f"};
        PhoenixArray expected = new PhoenixArray(PVarchar.INSTANCE, o1);
        String delimiter = "-";
        String nullString = "";
        test(string, delimiter, nullString, expected, SortOrder.ASC, SortOrder.ASC, SortOrder.ASC, PVarchar.INSTANCE, PVarchar.INSTANCE, PVarchar.INSTANCE);
        test(string, delimiter, nullString, expected, SortOrder.DESC, SortOrder.ASC, SortOrder.ASC, PVarchar.INSTANCE, PVarchar.INSTANCE, PVarchar.INSTANCE);
    }

    @Test
    public void testStringToArrayFunction9() throws SQLException {
        String string = "a b c d e f";
        Object[] o1 = new Object[]{"a", "b", "c", "d", "e", "f"};
        PhoenixArray expected = new PhoenixArray(PVarchar.INSTANCE, o1);
        String delimiter = " ";
        String nullString = "";
        test(string, delimiter, nullString, expected, SortOrder.ASC, SortOrder.ASC, SortOrder.ASC, PVarchar.INSTANCE, PVarchar.INSTANCE, PVarchar.INSTANCE);
        test(string, delimiter, nullString, expected, SortOrder.DESC, SortOrder.ASC, SortOrder.ASC, PVarchar.INSTANCE, PVarchar.INSTANCE, PVarchar.INSTANCE);
    }

    @Test
    public void testStringToArrayFunction10() throws SQLException {
        String string = "axbxcxdxexf";
        Object[] o1 = new Object[]{"a", "b", "c", "d", "e", "f"};
        PhoenixArray expected = new PhoenixArray(PVarchar.INSTANCE, o1);
        String delimiter = "x";
        String nullString = "";
        test(string, delimiter, nullString, expected, SortOrder.ASC, SortOrder.ASC, SortOrder.ASC, PVarchar.INSTANCE, PVarchar.INSTANCE, PVarchar.INSTANCE);
        test(string, delimiter, nullString, expected, SortOrder.DESC, SortOrder.ASC, SortOrder.ASC, PVarchar.INSTANCE, PVarchar.INSTANCE, PVarchar.INSTANCE);
    }

    @Test
    public void testStringToArrayFunction11() throws SQLException {
        String string = "axbxcxdxexfx*";
        Object[] o1 = new Object[]{"a", "b", "c", "d", "e", "f", null};
        PhoenixArray expected = new PhoenixArray(PVarchar.INSTANCE, o1);
        String delimiter = "x";
        String nullString = "*";
        test(string, delimiter, nullString, expected, SortOrder.ASC, SortOrder.ASC, SortOrder.ASC, PVarchar.INSTANCE, PVarchar.INSTANCE, PVarchar.INSTANCE);
        test(string, delimiter, nullString, expected, SortOrder.DESC, SortOrder.ASC, SortOrder.ASC, PVarchar.INSTANCE, PVarchar.INSTANCE, PVarchar.INSTANCE);
    }

    @Test
    public void testStringToArrayFunction12() throws SQLException {
        String string = "* a b c d e f";
        Object[] o1 = new Object[]{null, "a", "b", "c", "d", "e", "f"};
        PhoenixArray expected = new PhoenixArray(PVarchar.INSTANCE, o1);
        String delimiter = " ";
        String nullString = "*";
        test(string, delimiter, nullString, expected, SortOrder.ASC, SortOrder.ASC, SortOrder.ASC, PVarchar.INSTANCE, PVarchar.INSTANCE, PVarchar.INSTANCE);
        test(string, delimiter, nullString, expected, SortOrder.DESC, SortOrder.ASC, SortOrder.ASC, PVarchar.INSTANCE, PVarchar.INSTANCE, PVarchar.INSTANCE);
    }

    @Test
    public void testStringToArrayFunction13() throws SQLException {
        String string = "a * b * c d e f";
        Object[] o1 = new Object[]{"a", null, "b", null, "c", "d", "e", "f"};
        PhoenixArray expected = new PhoenixArray(PVarchar.INSTANCE, o1);
        String delimiter = " ";
        String nullString = "*";
        test(string, delimiter, nullString, expected, SortOrder.ASC, SortOrder.ASC, SortOrder.ASC, PVarchar.INSTANCE, PVarchar.INSTANCE, PVarchar.INSTANCE);
        test(string, delimiter, nullString, expected, SortOrder.DESC, SortOrder.ASC, SortOrder.ASC, PVarchar.INSTANCE, PVarchar.INSTANCE, PVarchar.INSTANCE);
    }

    @Test
    public void testStringToArrayFunction14() throws SQLException {
        String string = "null a null";
        Object[] o1 = new Object[]{null, "a", null};
        PhoenixArray expected = new PhoenixArray(PVarchar.INSTANCE, o1);
        String delimiter = " ";
        String nullString = "null";
        test(string, delimiter, nullString, expected, SortOrder.ASC, SortOrder.ASC, SortOrder.ASC, PVarchar.INSTANCE, PVarchar.INSTANCE, PVarchar.INSTANCE);
        test(string, delimiter, nullString, expected, SortOrder.DESC, SortOrder.ASC, SortOrder.ASC, PVarchar.INSTANCE, PVarchar.INSTANCE, PVarchar.INSTANCE);
    }

    @Test
    public void testStringToArrayFunction16() throws SQLException {
        String string = "null a null";
        Object[] o1 = new Object[]{null, "a", null};
        PhoenixArray expected = new PhoenixArray(PVarchar.INSTANCE, o1);
        String delimiter = " ";
        String nullString = "null";
        test(string, delimiter, nullString, expected, SortOrder.ASC, SortOrder.ASC, SortOrder.ASC, PVarchar.INSTANCE, PVarchar.INSTANCE, PVarchar.INSTANCE);
        test(string, delimiter, nullString, expected, SortOrder.DESC, SortOrder.ASC, SortOrder.ASC, PVarchar.INSTANCE, PVarchar.INSTANCE, PVarchar.INSTANCE);
    }

    @Test
    public void testStringToArrayFunction17() throws SQLException {
        String string = "null a null";
        Object[] o1 = new Object[]{null, "a", null};
        PhoenixArray expected = new PhoenixArray(PVarchar.INSTANCE, o1);
        String delimiter = " ";
        String nullString = "null";
        test(string, delimiter, nullString, expected, SortOrder.ASC, SortOrder.ASC, SortOrder.ASC, PChar.INSTANCE, PVarchar.INSTANCE, PVarchar.INSTANCE);
        test(string, delimiter, nullString, expected, SortOrder.DESC, SortOrder.ASC, SortOrder.ASC, PChar.INSTANCE, PVarchar.INSTANCE, PVarchar.INSTANCE);
    }

    @Test
    public void testStringToArrayFunction18() throws SQLException {
        String string = "null,a,null";
        Object[] o1 = new Object[]{null, "a", null};
        PhoenixArray expected = new PhoenixArray(PVarchar.INSTANCE, o1);
        String delimiter = ",";
        String nullString = "null";
        test(string, delimiter, nullString, expected, SortOrder.ASC, SortOrder.ASC, SortOrder.ASC, PChar.INSTANCE, PChar.INSTANCE, PVarchar.INSTANCE);
        test(string, delimiter, nullString, expected, SortOrder.DESC, SortOrder.ASC, SortOrder.ASC, PChar.INSTANCE, PChar.INSTANCE, PVarchar.INSTANCE);
    }

    @Test
    public void testStringToArrayFunction19() throws SQLException {
        String string = "null,a,null";
        Object[] o1 = new Object[]{null, "a", null};
        PhoenixArray expected = new PhoenixArray(PVarchar.INSTANCE, o1);
        String delimiter = ",";
        String nullString = "null";
        test(string, delimiter, nullString, expected, SortOrder.ASC, SortOrder.ASC, SortOrder.ASC, PChar.INSTANCE, PChar.INSTANCE, PChar.INSTANCE);
        test(string, delimiter, nullString, expected, SortOrder.DESC, SortOrder.ASC, SortOrder.ASC, PChar.INSTANCE, PChar.INSTANCE, PChar.INSTANCE);
    }

    @Test
    public void testStringToArrayFunction20() throws SQLException {
        String string = "abc";
        Object[] o1 = new Object[]{"a", "b", "c"};
        PhoenixArray expected = new PhoenixArray(PVarchar.INSTANCE, o1);
        String delimiter = null;
        String nullString = "null";
        test(string, delimiter, nullString, expected, SortOrder.ASC, SortOrder.ASC, SortOrder.ASC, PChar.INSTANCE, PChar.INSTANCE, PChar.INSTANCE);
        test(string, delimiter, nullString, expected, SortOrder.DESC, SortOrder.ASC, SortOrder.ASC, PChar.INSTANCE, PChar.INSTANCE, PChar.INSTANCE);
    }

    @Test
    public void testStringToArrayFunction21() throws SQLException {
        String string = "(?!^)";
        Object[] o1 = new Object[]{"(", "?", "!", "^", ")"};
        PhoenixArray expected = new PhoenixArray(PVarchar.INSTANCE, o1);
        String delimiter = null;
        String nullString = null;
        test(string, delimiter, nullString, expected, SortOrder.ASC, SortOrder.ASC, SortOrder.ASC, PVarchar.INSTANCE, PVarchar.INSTANCE, PVarchar.INSTANCE);
        test(string, delimiter, nullString, expected, SortOrder.DESC, SortOrder.ASC, SortOrder.ASC, PVarchar.INSTANCE, PVarchar.INSTANCE, PVarchar.INSTANCE);
    }
}
