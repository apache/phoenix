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

import java.io.DataInput;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.parse.FunctionParseNode;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.schema.types.PVarcharArray;
import org.apache.phoenix.schema.types.PhoenixArray;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.util.ByteUtil;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

/**
 * Function to split a string value into a {@code VARCHAR_ARRAY}.
 * <p>
 * Usage:
 * {@code REGEXP_SPLIT(&lt;source_str&gt;, &lt;split_pattern&gt;)}
 * <p>
 * {@code source_str} is the string in which we want to split. {@code split_pattern} is a
 * Java compatible regular expression string to split the source string.
 *
 * The function returns a {@link org.apache.phoenix.schema.types.PVarcharArray}
 */
 @FunctionParseNode.BuiltInFunction(name=RegexpSplitFunction.NAME, args= {
        @FunctionParseNode.Argument(allowedTypes={PVarchar.class}),
        @FunctionParseNode.Argument(allowedTypes={PVarchar.class})})
public class RegexpSplitFunction extends ScalarFunction {

    public static final String NAME = "REGEXP_SPLIT";

    private Splitter initializedSplitter = null;

    public RegexpSplitFunction() {}

    public RegexpSplitFunction(List<Expression> children) {
        super(children);
        init();
    }

    private void init() {
        Expression patternExpression = children.get(1);
        if (patternExpression instanceof LiteralExpression) {
            Object patternValue = ((LiteralExpression) patternExpression).getValue();
            if (patternValue != null) {
                initializedSplitter = Splitter.onPattern(patternValue.toString());
            }
        }
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        super.readFields(input);
        init();
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        if (!children.get(0).evaluate(tuple, ptr)) {
            return false;
        }

        Expression sourceStrExpression = children.get(0);
        String sourceStr = (String) PVarchar.INSTANCE.toObject(ptr, sourceStrExpression.getSortOrder());
        if (sourceStr == null) { // sourceStr evaluated to null
            ptr.set(ByteUtil.EMPTY_BYTE_ARRAY);
            return true;
        }

        return split(tuple, ptr, sourceStr);
    }

    private boolean split(Tuple tuple, ImmutableBytesWritable ptr, String sourceStr) {
        Splitter splitter = initializedSplitter;
        if (splitter == null) {
            Expression patternExpression = children.get(1);
            if (!patternExpression.evaluate(tuple, ptr)) {
                return false;
            }
            if (ptr.getLength() == 0) {
                return true; // ptr is already set to null
            }

            String patternStr = (String) PVarchar.INSTANCE.toObject(
                    ptr, patternExpression.getSortOrder());
            splitter = Splitter.onPattern(patternStr);
        }

        List<String> splitStrings = Lists.newArrayList(splitter.split(sourceStr));
        PhoenixArray splitArray = new PhoenixArray(PVarchar.INSTANCE, splitStrings.toArray());
        ptr.set(PVarcharArray.INSTANCE.toBytes(splitArray));
        return true;
    }


    @Override
    public PDataType getDataType() {
        return PVarcharArray.INSTANCE;
    }
}
