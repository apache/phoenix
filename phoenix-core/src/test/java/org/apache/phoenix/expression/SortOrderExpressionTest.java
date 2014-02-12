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
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;

import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.junit.Test;

import com.google.common.collect.Lists;
import org.apache.phoenix.expression.function.FunctionArgumentType;
import org.apache.phoenix.expression.function.LTrimFunction;
import org.apache.phoenix.expression.function.LengthFunction;
import org.apache.phoenix.expression.function.LowerFunction;
import org.apache.phoenix.expression.function.RTrimFunction;
import org.apache.phoenix.expression.function.RegexpReplaceFunction;
import org.apache.phoenix.expression.function.RegexpSubstrFunction;
import org.apache.phoenix.expression.function.RoundDateExpression;
import org.apache.phoenix.expression.function.SqlTypeNameFunction;
import org.apache.phoenix.expression.function.SubstrFunction;
import org.apache.phoenix.expression.function.ToCharFunction;
import org.apache.phoenix.expression.function.ToDateFunction;
import org.apache.phoenix.expression.function.ToNumberFunction;
import org.apache.phoenix.expression.function.TrimFunction;
import org.apache.phoenix.expression.function.UpperFunction;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.PDataType;
import org.apache.phoenix.util.DateUtil;

/**
 * @since 1.2
 */
public class SortOrderExpressionTest {
    
    @Test
    public void substr() throws Exception {
        List<Expression> args = Lists.newArrayList(getInvertedLiteral("blah", PDataType.CHAR), getLiteral(3), getLiteral(2));
        evaluateAndAssertResult(new SubstrFunction(args), "ah");
    }
    
    @Test
    public void regexpSubstr() throws Exception {
        List<Expression> args = Lists.newArrayList(getInvertedLiteral("blah", PDataType.CHAR), getLiteral("l.h"), getLiteral(2));
        evaluateAndAssertResult(new RegexpSubstrFunction(args), "lah");
    }
    
    @Test
    public void regexpReplace() throws Exception {
        List<Expression> args = Lists.newArrayList(getInvertedLiteral("blah", PDataType.CHAR), getLiteral("l.h"), getLiteral("foo"));
        evaluateAndAssertResult(new RegexpReplaceFunction(args), "bfoo");
    }
    
    @Test
    public void ltrim() throws Exception {
        List<Expression> args = Lists.newArrayList(getInvertedLiteral("   blah", PDataType.CHAR));
        evaluateAndAssertResult(new LTrimFunction(args), "blah");
    }
    
    @Test
    public void substrLtrim() throws Exception {
        List<Expression> ltrimArgs = Lists.newArrayList(getInvertedLiteral("   blah", PDataType.CHAR));
        Expression ltrim = new LTrimFunction(ltrimArgs);
        List<Expression> substrArgs = Lists.newArrayList(ltrim, getLiteral(3), getLiteral(2));
        evaluateAndAssertResult(new SubstrFunction(substrArgs), "ah");
    }
    
    @Test
    public void rtrim() throws Exception {
        List<Expression> args = Lists.newArrayList(getInvertedLiteral("blah    ", PDataType.CHAR));
        evaluateAndAssertResult(new RTrimFunction(args), "blah");
    }
    
    @Test
    public void lower() throws Exception {
        List<Expression> args = Lists.newArrayList(getInvertedLiteral("BLAH", PDataType.CHAR));
        evaluateAndAssertResult(new LowerFunction(args), "blah");        
    }
    
    @Test
    public void upper() throws Exception {
        List<Expression> args = Lists.newArrayList(getInvertedLiteral("blah", PDataType.CHAR));
        evaluateAndAssertResult(new UpperFunction(args), "BLAH");        
    }
    
    @Test
    public void length() throws Exception {
        List<Expression> args = Lists.newArrayList(getInvertedLiteral("blah", PDataType.CHAR));
        evaluateAndAssertResult(new LengthFunction(args), 4);
    }
    
    @Test
    public void round() throws Exception {
        List<Expression> args = Lists.newArrayList(getInvertedLiteral(date(12, 11, 2001), PDataType.DATE), getLiteral("hour"), getLiteral(1));
        evaluateAndAssertResult(RoundDateExpression.create(args), date(12, 11, 2001));
    }
    
    @Test
    public void sqlTypeName() throws Exception {
        List<Expression> args = Lists.newArrayList(getInvertedLiteral(12, PDataType.INTEGER));
        evaluateAndAssertResult(new SqlTypeNameFunction(args), "VARCHAR");        
    }
    
    @Test
    public void toChar() throws Exception {
        List<Expression> args = Lists.newArrayList(getInvertedLiteral(date(12, 11, 2001), PDataType.DATE));
        evaluateAndAssertResult(new ToCharFunction(args, FunctionArgumentType.TEMPORAL, "", DateUtil.getDateFormatter("MM/dd/yy hh:mm a")), "12/11/01 12:00 AM");
    }
    
    @Test
    public void toDate() throws Exception {
        List<Expression> args = Lists.newArrayList(getInvertedLiteral("2001-11-30 00:00:00:0", PDataType.VARCHAR));
        evaluateAndAssertResult(new ToDateFunction(args, null, DateUtil.getDateParser("yyyy-MM-dd HH:mm:ss:S")), date(11, 30, 2001));
    }
    
    @Test
    public void toNumber() throws Exception {
        List<Expression> args = Lists.newArrayList(getInvertedLiteral("10", PDataType.VARCHAR));
        evaluateAndAssertResult(new ToNumberFunction(args, FunctionArgumentType.CHAR, "", null), new BigDecimal(BigInteger.valueOf(1), -1));
    }
    
    @Test
    public void trim() throws Exception {
        List<Expression> args = Lists.newArrayList(getInvertedLiteral("   blah    ", PDataType.CHAR));
        evaluateAndAssertResult(new TrimFunction(args), "blah");
    }
    
    @Test
    public void add() throws Exception {
        List<Expression> args = Lists.newArrayList(getInvertedLiteral(10, PDataType.INTEGER), getLiteral(2));
        evaluateAndAssertResult(new DecimalAddExpression(args), BigDecimal.valueOf(12));
        
        args = Lists.newArrayList(getInvertedLiteral(10, PDataType.INTEGER), getLiteral(2));
        evaluateAndAssertResult(new LongAddExpression(args), 12l);
        
        args = Lists.newArrayList(getInvertedLiteral(10.0, PDataType.FLOAT), getLiteral(2));
        evaluateAndAssertResult(new DoubleAddExpression(args), 12.0);
        
        args = Lists.newArrayList(getInvertedLiteral(10.0, PDataType.UNSIGNED_FLOAT), getLiteral(2));
        evaluateAndAssertResult(new DoubleAddExpression(args), 12.0);
        
        args = Lists.newArrayList(getInvertedLiteral(10.0, PDataType.UNSIGNED_DOUBLE), getLiteral(2));
        evaluateAndAssertResult(new DoubleAddExpression(args), 12.0);
        
        args = Lists.newArrayList(getInvertedLiteral(10.0, PDataType.DOUBLE), getLiteral(2));
        evaluateAndAssertResult(new DoubleAddExpression(args), 12.0);
    }

    @Test
    public void subtract() throws Exception {
        List<Expression> args = Lists.newArrayList(getInvertedLiteral(10, PDataType.INTEGER), getLiteral(2));
        evaluateAndAssertResult(new DecimalSubtractExpression(args), BigDecimal.valueOf(8));
        
        args = Lists.newArrayList(getInvertedLiteral(10, PDataType.INTEGER), getLiteral(2));
        evaluateAndAssertResult(new LongSubtractExpression(args), 8l);
        
        args = Lists.newArrayList(getInvertedLiteral(10.0, PDataType.FLOAT), getLiteral(2));
        evaluateAndAssertResult(new DoubleSubtractExpression(args), 8.0);
        
        args = Lists.newArrayList(getInvertedLiteral(10.0, PDataType.UNSIGNED_FLOAT), getLiteral(2));
        evaluateAndAssertResult(new DoubleSubtractExpression(args), 8.0);
        
        args = Lists.newArrayList(getInvertedLiteral(10.0, PDataType.UNSIGNED_DOUBLE), getLiteral(2));
        evaluateAndAssertResult(new DoubleSubtractExpression(args), 8.0);
        
        args = Lists.newArrayList(getInvertedLiteral(10.0, PDataType.DOUBLE), getLiteral(2));
        evaluateAndAssertResult(new DoubleSubtractExpression(args), 8.0);
    }
    
    @Test
    public void divide() throws Exception {
        List<Expression> args = Lists.newArrayList(getInvertedLiteral(10, PDataType.INTEGER), getLiteral(2));
        evaluateAndAssertResult(new DecimalDivideExpression(args), BigDecimal.valueOf(5));
        
        args = Lists.newArrayList(getInvertedLiteral(10, PDataType.INTEGER), getLiteral(2));
        evaluateAndAssertResult(new LongDivideExpression(args), 5l);
        
        args = Lists.newArrayList(getInvertedLiteral(10.0, PDataType.FLOAT), getLiteral(2));
        evaluateAndAssertResult(new DoubleDivideExpression(args), 5.0);
        
        args = Lists.newArrayList(getInvertedLiteral(10.0, PDataType.UNSIGNED_FLOAT), getLiteral(2));
        evaluateAndAssertResult(new DoubleDivideExpression(args), 5.0);
        
        args = Lists.newArrayList(getInvertedLiteral(10.0, PDataType.UNSIGNED_DOUBLE), getLiteral(2));
        evaluateAndAssertResult(new DoubleDivideExpression(args), 5.0);
        
        args = Lists.newArrayList(getInvertedLiteral(10.0, PDataType.DOUBLE), getLiteral(2));
        evaluateAndAssertResult(new DoubleDivideExpression(args), 5.0);
    }
    
    @Test
    public void multiply() throws Exception {
        List<Expression> args = Lists.newArrayList(getInvertedLiteral(10, PDataType.INTEGER), getLiteral(2));
        evaluateAndAssertResult(new DecimalMultiplyExpression(args), new BigDecimal(BigInteger.valueOf(2), -1));
        
        args = Lists.newArrayList(getInvertedLiteral(10, PDataType.INTEGER), getLiteral(2));
        evaluateAndAssertResult(new LongMultiplyExpression(args), 20l);
        
        args = Lists.newArrayList(getInvertedLiteral(10.0, PDataType.FLOAT), getLiteral(2));
        evaluateAndAssertResult(new DoubleMultiplyExpression(args), 20.0);
        
        args = Lists.newArrayList(getInvertedLiteral(10.0, PDataType.UNSIGNED_FLOAT), getLiteral(2));
        evaluateAndAssertResult(new DoubleMultiplyExpression(args), 20.0);
        
        args = Lists.newArrayList(getInvertedLiteral(10.0, PDataType.UNSIGNED_DOUBLE), getLiteral(2));
        evaluateAndAssertResult(new DoubleMultiplyExpression(args), 20.0);
        
        args = Lists.newArrayList(getInvertedLiteral(10.0, PDataType.DOUBLE), getLiteral(2));
        evaluateAndAssertResult(new DoubleMultiplyExpression(args), 20.0);
    }
        
    @Test
    public void compareNumbers() throws Exception {
        PDataType[] numberDataTypes = new PDataType[]{PDataType.INTEGER, PDataType.LONG, PDataType.DECIMAL, PDataType.UNSIGNED_INT, PDataType.UNSIGNED_LONG};
        for (PDataType lhsDataType : numberDataTypes) {
            for (PDataType rhsDataType : numberDataTypes) {
                runCompareTest(CompareOp.GREATER, true, 10, lhsDataType, 2, rhsDataType);
            }
        }
    }
    
    @Test
    public void compareCharacters() throws Exception {
        PDataType[] textDataTypes = new PDataType[]{PDataType.CHAR, PDataType.VARCHAR};
        for (PDataType lhsDataType : textDataTypes) {
            for (PDataType rhsDataType : textDataTypes) {
                runCompareTest(CompareOp.GREATER, true, "xxx", lhsDataType, "bbb", rhsDataType);
            }
        }
    }
    
    @Test
    public void compareBooleans() throws Exception {
        runCompareTest(CompareOp.GREATER, true, true, PDataType.BOOLEAN, false, PDataType.BOOLEAN);        
    }
    
    @Test
    public void stringConcat() throws Exception {
        List<Expression> args = Lists.newArrayList(getInvertedLiteral("blah", PDataType.VARCHAR), getInvertedLiteral("foo", PDataType.VARCHAR)); 
        evaluateAndAssertResult(new StringConcatExpression(args), "blahfoo");
        
        args = Lists.newArrayList(getInvertedLiteral("blah", PDataType.VARCHAR), getInvertedLiteral(10, PDataType.INTEGER)); 
        evaluateAndAssertResult(new StringConcatExpression(args), "blah10");        
    }
    
    private void runCompareTest(CompareOp op, boolean expectedResult, Object lhsValue, PDataType lhsDataType, Object rhsValue, PDataType rhsDataType) throws Exception {
        List<Expression> args = Lists.newArrayList(getLiteral(lhsValue, lhsDataType), getLiteral(rhsValue, rhsDataType));
        evaluateAndAssertResult(new ComparisonExpression(op, args), expectedResult, "lhsDataType: " + lhsDataType + " rhsDataType: " + rhsDataType);
        
        args = Lists.newArrayList(getInvertedLiteral(lhsValue, lhsDataType), getLiteral(rhsValue, rhsDataType));
        evaluateAndAssertResult(new ComparisonExpression(op, args), expectedResult, "lhs (inverted) dataType: " + lhsDataType + " rhsDataType: " + rhsDataType);
        
        args = Lists.newArrayList(getLiteral(lhsValue, lhsDataType), getInvertedLiteral(rhsValue, rhsDataType));
        evaluateAndAssertResult(new ComparisonExpression(op, args), expectedResult, "lhsDataType: " + lhsDataType + " rhs (inverted) dataType: " + rhsDataType);
        
        args = Lists.newArrayList(getInvertedLiteral(lhsValue, lhsDataType), getInvertedLiteral(rhsValue, rhsDataType));
        evaluateAndAssertResult(new ComparisonExpression(op, args), expectedResult, "lhs (inverted) dataType: " + lhsDataType + " rhs (inverted) dataType: " + rhsDataType);                
    }
    
    private void evaluateAndAssertResult(Expression expression, Object expectedResult) {
        evaluateAndAssertResult(expression, expectedResult, null);
    }
    
    private void evaluateAndAssertResult(Expression expression, Object expectedResult, String context) {
        context = context == null ? "" : context;
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        assertTrue(expression.evaluate(null, ptr));
        PDataType dataType = expression.getDataType();
        SortOrder sortOrder = expression.getSortOrder();
        Object result = dataType.toObject(ptr.get(), ptr.getOffset(), ptr.getLength(), dataType, sortOrder);
        assertEquals(context, expectedResult, result);
    }
    
    private Expression getLiteral(Object value) throws Exception {
        return LiteralExpression.newConstant(value);
    }
    
    private Expression getLiteral(Object value, PDataType dataType) throws Exception {
        return LiteralExpression.newConstant(value, dataType);
    }    
    
    private Expression getInvertedLiteral(Object literal, PDataType dataType) throws Exception {
        return LiteralExpression.newConstant(literal, dataType, SortOrder.DESC);
    }
    
    private static Date date(int month, int day, int year) {
        Calendar cal = new GregorianCalendar();
        cal.set(Calendar.MONTH, month-1);
        cal.set(Calendar.DAY_OF_MONTH, day);
        cal.set(Calendar.YEAR, year);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        cal.setTimeZone(DateUtil.DATE_TIME_ZONE);
        Date d = new Date(cal.getTimeInMillis()); 
        return d;
    }    
}
