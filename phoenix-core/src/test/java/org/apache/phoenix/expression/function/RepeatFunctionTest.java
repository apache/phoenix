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
package org.apache.phoenix.expression.function;

import static org.junit.Assert.assertTrue;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PVarchar;
import org.junit.Test;

public class RepeatFunctionTest {
    public static void inputExpression(String value, PDataType dataType, Integer input, PDataType datatypeInt, String expected, SortOrder order) throws SQLException{
        Expression inputArg = LiteralExpression.newConstant(value,dataType,order);
        
        Expression intRepeatExpr = LiteralExpression.newConstant(input,datatypeInt);
        List<Expression> expressions = Arrays.<Expression>asList(inputArg,intRepeatExpr);
        Expression repeatFunction = new RepeatFunction(expressions);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        repeatFunction.evaluate(null,ptr);
        String result = (String) repeatFunction.getDataType().toObject(ptr);
        assertTrue(result.equals(expected));
        
    }
    
    @Test
    public void testRepeatFunction() throws SQLException {
        inputExpression("abc",PVarchar.INSTANCE, 2 , PInteger.INSTANCE, "abcabc", SortOrder.ASC);
        
        inputExpression("abc",PVarchar.INSTANCE, 2 , PInteger.INSTANCE, "abcabc", SortOrder.DESC);
                
        inputExpression("ABc",PVarchar.INSTANCE, 2 , PInteger.INSTANCE, "ABcABc", SortOrder.ASC);
        
        inputExpression("ABc",PVarchar.INSTANCE, 2 , PInteger.INSTANCE, "ABcABc", SortOrder.DESC);
                        
        //Tests for MultiByte Character
        
        inputExpression("Aɚɦ",PVarchar.INSTANCE, 3, PInteger.INSTANCE, "AɚɦAɚɦAɚɦ", SortOrder.ASC);
        
        inputExpression("Aɚɦ",PVarchar.INSTANCE, 3, PInteger.INSTANCE, "AɚɦAɚɦAɚɦ", SortOrder.DESC);
        
    }
    
}
