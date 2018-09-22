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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PVarchar;
import org.junit.Test;

public class InstrFunctionTest {

    private static Object evaluateExpression(String value, PDataType<?> dataType, String strToSearch, SortOrder order) throws SQLException {
      Expression inputArg = LiteralExpression.newConstant(value,dataType,order);

      Expression strToSearchExp = LiteralExpression.newConstant(strToSearch,dataType);
      List<Expression> expressions = Arrays.<Expression>asList(inputArg,strToSearchExp);
      Expression instrFunction = new InstrFunction(expressions);
      ImmutableBytesWritable ptr = new ImmutableBytesWritable();
      instrFunction.evaluate(null,ptr);
      return instrFunction.getDataType().toObject(ptr);
    }
    
    public static void inputExpression(String value, PDataType<?> dataType, String strToSearch,Integer expected, SortOrder order) throws SQLException {
        Object obj = evaluateExpression(value, dataType, strToSearch, order);
        assertNotNull("Result was unexpectedly null", obj);
        assertTrue(((Integer) obj).compareTo(expected) == 0);
    }

    public static void inputNullExpression(String value, PDataType<?> dataType, String strToSearch, SortOrder order) throws SQLException {
        Object obj = evaluateExpression(value, dataType, strToSearch, order);
        assertNull("Result was unexpectedly non-null", obj);
    }
    
    
    @Test
    public void testInstrFunction() throws SQLException {
        inputExpression("abcdefghijkl",PVarchar.INSTANCE, "fgh", 6, SortOrder.ASC);
        
        inputExpression("abcdefghijkl",PVarchar.INSTANCE, "fgh", 6, SortOrder.DESC);
        
        inputExpression("abcde fghijkl",PVarchar.INSTANCE, " fgh", 6, SortOrder.ASC);
        
        inputExpression("abcde fghijkl",PVarchar.INSTANCE, " fgh", 6, SortOrder.DESC);
        
        inputExpression("abcde fghijkl",PVarchar.INSTANCE, "lmn", 0, SortOrder.DESC);
        
        inputExpression("abcde fghijkl",PVarchar.INSTANCE, "lmn", 0, SortOrder.ASC);
        
        inputExpression("ABCDEFGHIJKL",PVarchar.INSTANCE, "FGH", 6, SortOrder.ASC);
        
        inputExpression("ABCDEFGHIJKL",PVarchar.INSTANCE, "FGH", 6, SortOrder.DESC);
        
        inputExpression("ABCDEFGHiJKL",PVarchar.INSTANCE, "iJKL", 9, SortOrder.ASC);
        
        inputExpression("ABCDEFGHiJKL",PVarchar.INSTANCE, "iJKL", 9, SortOrder.DESC);
        
        inputExpression("ABCDE FGHiJKL",PVarchar.INSTANCE, " ", 6, SortOrder.ASC);
        
        inputExpression("ABCDE FGHiJKL",PVarchar.INSTANCE, " ", 6, SortOrder.DESC);

        // Phoenix can't represent empty strings, so an empty or null search string should return null
        // See PHOENIX-4884 for more chatter.
        inputNullExpression("ABCDE FGHiJKL",PVarchar.INSTANCE, "", SortOrder.ASC);
        inputNullExpression("ABCDE FGHiJKL",PVarchar.INSTANCE, "", SortOrder.DESC);
        inputNullExpression("ABCDE FGHiJKL",PVarchar.INSTANCE, null, SortOrder.ASC);
        inputNullExpression("ABCDE FGHiJKL",PVarchar.INSTANCE, null, SortOrder.DESC);
        
        inputExpression("ABCDEABC",PVarchar.INSTANCE, "ABC", 1, SortOrder.ASC);
        
        inputExpression("ABCDEABC",PVarchar.INSTANCE, "ABC", 1, SortOrder.DESC);
        
        inputExpression("AB01CDEABC",PVarchar.INSTANCE, "01C", 3, SortOrder.ASC);
        
        inputExpression("AB01CDEABC",PVarchar.INSTANCE, "01C", 3, SortOrder.DESC);
        
        inputExpression("ABCD%EFGH",PVarchar.INSTANCE, "%", 5, SortOrder.ASC);
        
        inputExpression("ABCD%EFGH",PVarchar.INSTANCE, "%", 5, SortOrder.DESC);
        
        //Tests for MultiByte Characters
        
        inputExpression("AɚɦFGH",PVarchar.INSTANCE, "ɚɦ", 2, SortOrder.ASC);
        
        inputExpression("AɚɦFGH",PVarchar.INSTANCE, "ɚɦ", 2, SortOrder.DESC);
        
        inputExpression("AɚɦFGH",PVarchar.INSTANCE, "ɦFGH", 3, SortOrder.ASC);
        
        inputExpression("AɚɦFGH",PVarchar.INSTANCE, "ɦFGH", 3, SortOrder.DESC);
        
        inputExpression("AɚɦF/GH",PVarchar.INSTANCE, "ɦF/GH", 3, SortOrder.ASC);
        
        inputExpression("AɚɦF/GH",PVarchar.INSTANCE, "ɦF/GH", 3, SortOrder.DESC);
    }
    

}
