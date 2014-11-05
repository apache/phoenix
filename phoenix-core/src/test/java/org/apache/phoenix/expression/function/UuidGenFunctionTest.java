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

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.schema.PDataType;
import org.junit.Test;

import java.sql.SQLException;
import java.util.UUID;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class UuidGenFunctionTest {

    @Test
    public void testEvaluate() throws SQLException {
        Object returnValue = executeFunction();
        assertNotNull(returnValue);

        // verify the output string is a valid UUID string.
        UUID uuid = UUID.fromString(returnValue.toString());
        assertNotNull(uuid);
    }

    private Object executeFunction() throws SQLException {
        UuidGenFunction uuidGenFunction = new UuidGenFunction();

        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        assertTrue(uuidGenFunction.evaluate(null, ptr));

        return PDataType.VARCHAR.toObject(ptr.get(), ptr.getOffset(), ptr.getLength(),
                                          PDataType.VARCHAR);
    }
}
