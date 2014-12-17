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
import java.util.regex.Pattern;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.parse.FunctionParseNode.Argument;
import org.apache.phoenix.parse.FunctionParseNode.BuiltInFunction;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.schema.tuple.Tuple;


/**
 * 
 * Function similar to the regexp_replace function in Postgres, which is used to pattern
 * match a segment of the string. Usage:
 * REGEXP_REPLACE(<source_char>,<pattern>,<replace_string>)
 * source_char is the string in which we want to perform string replacement. pattern is a
 * Java compatible regular expression string, and we replace all the matching part with 
 * replace_string. The first 2 arguments are required and are {@link org.apache.phoenix.schema.types.PVarchar},
 * the replace_string is default to empty string.
 * 
 * The function returns a {@link org.apache.phoenix.schema.types.PVarchar}
 * 
 * 
 * @since 0.1
 */
@BuiltInFunction(name=RegexpReplaceFunction.NAME, args= {
    @Argument(allowedTypes={PVarchar.class}),
    @Argument(allowedTypes={PVarchar.class}),
    @Argument(allowedTypes={PVarchar.class},defaultValue="null")} )
public class RegexpReplaceFunction extends ScalarFunction {
    public static final String NAME = "REGEXP_REPLACE";

    private boolean hasReplaceStr;
    private Pattern pattern;
    
    public RegexpReplaceFunction() { }

    // Expect 1 arguments, the pattern. 
    public RegexpReplaceFunction(List<Expression> children) {
        super(children);
        init();
    }

    private void init() {
        hasReplaceStr = ((LiteralExpression)getReplaceStrExpression()).getValue() != null;
        Object patternString = ((LiteralExpression)children.get(1)).getValue();
        if (patternString != null) {
            pattern = Pattern.compile((String)patternString);
        }
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        // Can't parse if there is no replacement pattern.
        if (pattern == null) {
            return false;
        }
        Expression sourceStrExpression = getSourceStrExpression();
        if (!sourceStrExpression.evaluate(tuple, ptr)) {
            return false;
        }
        String sourceStr = (String) PVarchar.INSTANCE.toObject(ptr, sourceStrExpression.getSortOrder());
        if (sourceStr == null) {
            return false;
        }
        String replaceStr;
        if (hasReplaceStr) {
            Expression replaceStrExpression = this.getReplaceStrExpression();
            if (!replaceStrExpression.evaluate(tuple, ptr)) {
                return false;
            }
            replaceStr = (String) PVarchar.INSTANCE.toObject(ptr, replaceStrExpression.getSortOrder());
        } else {
            replaceStr = "";
        }
        String replacedStr = pattern.matcher(sourceStr).replaceAll(replaceStr);
        ptr.set(PVarchar.INSTANCE.toBytes(replacedStr));
        return true;
    }

    private Expression getSourceStrExpression() {
        return children.get(0);
    }

    private Expression getReplaceStrExpression() {
        return children.get(2);
    }

    @Override
    public PDataType getDataType() {
        return PVarchar.INSTANCE;
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
}
