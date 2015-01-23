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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PIntegerArray;
import org.junit.Test;

import com.google.common.collect.Lists;

public class ExternalSqlTypeIdFunctionTest {

    @Test
    public void testEvaluate() throws SQLException {
        Expression inputArg = LiteralExpression.newConstant(
                PInteger.INSTANCE.getSqlType(), PInteger.INSTANCE);

        Object returnValue = executeFunction(inputArg);

        assertEquals(Types.INTEGER, returnValue);
    }

    @Test
    public void testEvaluateArrayType() throws SQLException {
        Expression inputArg = LiteralExpression.newConstant(
                PIntegerArray.INSTANCE.getSqlType(), PInteger.INSTANCE);

        Object returnValue = executeFunction(inputArg);

        assertEquals(Types.ARRAY, returnValue);
    }

    @Test
    public void testClone() throws SQLException {
        Expression inputArg = LiteralExpression.newConstant(
                PIntegerArray.INSTANCE.getSqlType(), PInteger.INSTANCE);
        List<Expression> args = Lists.newArrayList(inputArg);
        ExternalSqlTypeIdFunction externalIdFunction =
                new ExternalSqlTypeIdFunction(args);
        ScalarFunction clone = externalIdFunction.clone(args);
        assertEquals(externalIdFunction, clone);
    }

    private Object executeFunction(Expression inputArg) throws SQLException {
        ExternalSqlTypeIdFunction externalIdFunction =
                new ExternalSqlTypeIdFunction(Lists.newArrayList(inputArg));

        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        assertTrue(externalIdFunction.evaluate(null, ptr));

        return PInteger.INSTANCE.toObject(ptr.get(), ptr.getOffset(), ptr.getLength(),
            PInteger.INSTANCE, inputArg.getSortOrder());
    }
}
