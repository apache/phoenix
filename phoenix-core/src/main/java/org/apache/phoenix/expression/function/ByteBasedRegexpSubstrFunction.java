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

import java.util.List;

import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.util.regex.AbstractBasePattern;
import org.apache.phoenix.expression.util.regex.JONIPattern;
import org.joni.Option;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.util.regex.AbstractBasePattern;
import org.apache.phoenix.expression.util.regex.JONIPattern;
import org.joni.Option;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.parse.FunctionParseNode.BuiltInFunction;
import org.apache.phoenix.parse.FunctionParseNode.Argument;
import org.apache.phoenix.parse.FunctionParseNode.FunctionClassType;

@BuiltInFunction(name=RegexpSubstrFunction.NAME,
        args= {
                @Argument(allowedTypes={PVarchar.class}),
                @Argument(allowedTypes={PVarchar.class}),
                @Argument(allowedTypes={PLong.class}, defaultValue="1")},
        classType = FunctionClassType.DERIVED
)
public class ByteBasedRegexpSubstrFunction extends RegexpSubstrFunction {
    public ByteBasedRegexpSubstrFunction() {
    }

    public ByteBasedRegexpSubstrFunction(List<Expression> children) {
        super(children);
    }

    @Override
    protected AbstractBasePattern compilePatternSpec(String value) {
        return new JONIPattern(value, Option.MULTILINE);
    }
}
