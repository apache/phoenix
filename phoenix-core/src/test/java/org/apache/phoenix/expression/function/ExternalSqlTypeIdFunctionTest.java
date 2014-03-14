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

import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.schema.PDataType;
import org.junit.Test;

import java.sql.SQLException;
import java.sql.Types;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ExternalSqlTypeIdFunctionTest {

    @Test
    public void testEvaluate() throws SQLException {
        Expression inputArg = LiteralExpression.newConstant(
                PDataType.INTEGER.getSqlType(), PDataType.INTEGER);

        Object returnValue = executeFunction(inputArg);

        assertEquals(Types.INTEGER, returnValue);
    }

    @Test
    public void testEvaluateArrayType() throws SQLException {
        Expression inputArg = LiteralExpression.newConstant(
                PDataType.INTEGER_ARRAY.getSqlType(), PDataType.INTEGER);

        Object returnValue = executeFunction(inputArg);

        assertEquals(Types.ARRAY, returnValue);
    }

    private Object executeFunction(Expression inputArg) throws SQLException {
        ExternalSqlTypeIdFunction externalIdFunction =
                new ExternalSqlTypeIdFunction(Lists.newArrayList(inputArg));

        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        assertTrue(externalIdFunction.evaluate(null, ptr));

        return PDataType.INTEGER.toObject(ptr.get(), ptr.getOffset(), ptr.getLength(),
                PDataType.INTEGER, inputArg.getSortOrder());
    }
}
