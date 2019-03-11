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

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.function.MathPIFunction;
import org.apache.phoenix.query.BaseTest;

import org.junit.Test;

/**
 * Unit tests for {@link MathPIFunction}
 */
public class MathPIFunctionTest {

    @Test
    public void testMathPIFunction() {
        Expression mathPIFunction = new MathPIFunction();
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        boolean res = mathPIFunction.evaluate(null, ptr);
        if (res) {
            Double result = (Double) mathPIFunction.getDataType().toObject(ptr);
            assertTrue(BaseTest.twoDoubleEquals(result.doubleValue(), Math.PI));
        }
        assertTrue(res);
    }
}
