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
package org.apache.phoenix.parse;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.function.ScalarFunction;
import org.apache.phoenix.parse.FunctionParseNode.Argument;
import org.apache.phoenix.parse.FunctionParseNode.BuiltInFunction;
import org.apache.phoenix.parse.FunctionParseNode.BuiltInFunctionInfo;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PVarchar;
import org.junit.Test;

public class BuiltInFunctionInfoTest {

    private static BuiltInFunctionInfo getBuiltInFunctionInfo(Class<? extends ScalarFunction> funcClass) {
        return new BuiltInFunctionInfo(funcClass, funcClass.getAnnotation(BuiltInFunction.class));
    }

    @Test
    public void testConstruct_NoDefaultArgs() {
        BuiltInFunctionInfo funcInfo = getBuiltInFunctionInfo(NoDefaultArgsFunction.class);
        assertEquals(2, funcInfo.getArgs().length);
        assertEquals(2, funcInfo.getRequiredArgCount());
        assertEquals("NO_DEFAULT_ARGS", funcInfo.getName());
    }

    @Test
    public void testConstruct_WithOneDefaultArg() {
        BuiltInFunctionInfo funcInfo = getBuiltInFunctionInfo(WithOneDefaultArg.class);
        assertEquals(3, funcInfo.getArgs().length);
        assertEquals(2, funcInfo.getRequiredArgCount());
        assertEquals("WITH_ONE_DEFAULT_ARG", funcInfo.getName());
    }

    @Test
    public void testConstruct_WithMultipleDefaultArgs() {
        BuiltInFunctionInfo funcInfo = getBuiltInFunctionInfo(WithMultipleDefaultArgs.class);
        assertEquals(3, funcInfo.getArgs().length);
        assertEquals(1, funcInfo.getRequiredArgCount());
        assertEquals("WITH_MULTIPLE_DEFAULT_ARGS", funcInfo.getName());
    }

    private static class BaseFunctionAdapter extends ScalarFunction {


        private final String name;

        BaseFunctionAdapter(String name) {
            this.name = name;
        }

        @Override
        public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
            throw new UnsupportedOperationException("Can't evalulate a BaseTestFunction");
        }

        @Override
        public PDataType getDataType() {
            return PVarchar.INSTANCE;
        }

        @Override
        public String getName() {
            return name;
        }
    }

    @BuiltInFunction(name="NO_DEFAULT_ARGS", args={
            @Argument(allowedTypes={PVarchar.class}),
            @Argument(allowedTypes={PVarchar.class})})
    static class NoDefaultArgsFunction extends BaseFunctionAdapter {

        public NoDefaultArgsFunction(List<Expression> ignoreChildren) {
            super("NO_DEFAULT_ARGS");
        }

    }

    @BuiltInFunction(name="WITH_ONE_DEFAULT_ARG", args={
            @Argument(allowedTypes={PVarchar.class}),
            @Argument(allowedTypes={PVarchar.class}),
            @Argument(allowedTypes={PVarchar.class}, defaultValue = "'a'") })
    static class WithOneDefaultArg extends BaseFunctionAdapter {

        public WithOneDefaultArg(List<Expression> ignoreChildren) {
            super("WITH_ONE_DEFAULT_ARG");
        }
    }

    @BuiltInFunction(name="WITH_MULTIPLE_DEFAULT_ARGS", args={
            @Argument(allowedTypes={PVarchar.class}),
            @Argument(allowedTypes={PVarchar.class}, defaultValue = "'a'"),
            @Argument(allowedTypes={PVarchar.class}, defaultValue = "'b'") })
    static class WithMultipleDefaultArgs extends BaseFunctionAdapter {

        public WithMultipleDefaultArgs(List<Expression> ignoreChildren) {
            super("WITH_MULTIPLE_DEFAULT_ARGS");
        }
    }
}