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
import org.apache.phoenix.expression.util.regex.AbstractBasePattern;
import org.apache.phoenix.parse.FunctionParseNode.Argument;
import org.apache.phoenix.parse.FunctionParseNode.BuiltInFunction;
import org.apache.phoenix.parse.RegexpSubstrParseNode;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.schema.types.PVarchar;


/**
 * 
 * Implementation of REGEXP_SUBSTR(<source>, <pattern>, <offset>) built-in function,
 * where <offset> is the offset from the start of <string>. Positive offset is treated as 1-based,
 * a zero offset is treated as 0-based, and a negative offset starts from the end of the string 
 * working backwards. The <pattern> is the pattern we would like to search for in the <source> string.
 * The function returns the first occurrence of any substring in the <source> string that matches
 * the <pattern> input as a VARCHAR. 
 * 
 * 
 * @since 0.1
 */
@BuiltInFunction(name=RegexpSubstrFunction.NAME,
    nodeClass = RegexpSubstrParseNode.class, args={
    @Argument(allowedTypes={PVarchar.class}),
    @Argument(allowedTypes={PVarchar.class}),
    @Argument(allowedTypes={PLong.class}, defaultValue="1")} )
public abstract class RegexpSubstrFunction extends PrefixFunction {
    public static final String NAME = "REGEXP_SUBSTR";

    private AbstractBasePattern pattern;
    private boolean isOffsetConstant;
    private Integer maxLength;

    private static final PDataType TYPE = PVarchar.INSTANCE;

    public RegexpSubstrFunction() { }

    public RegexpSubstrFunction(List<Expression> children) {
        super(children);
        init();
    }

    protected abstract AbstractBasePattern compilePatternSpec(String value);

    private void init() {
        Object patternString = ((LiteralExpression)children.get(1)).getValue();
        if (patternString != null) {
            pattern = compilePatternSpec((String) patternString);
        }
        // If the source string has a fixed width, then the max length would be the length 
        // of the source string minus the offset, or the absolute value of the offset if 
        // it's negative. Offset number is a required argument. However, if the source string
        // is not fixed width, the maxLength would be null.
        isOffsetConstant = getOffsetExpression() instanceof LiteralExpression;
        Number offsetNumber = (Number)((LiteralExpression)getOffsetExpression()).getValue();
        if (offsetNumber != null) {
            int offset = offsetNumber.intValue();
            PDataType type = getSourceStrExpression().getDataType();
            if (type.isFixedWidth()) {
                if (offset >= 0) {
                    Integer maxLength = getSourceStrExpression().getMaxLength();
                    this.maxLength = maxLength - offset - (offset == 0 ? 0 : 1);
                } else {
                    this.maxLength = -offset;
                }
            }
        }
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        if (pattern == null) {
            return false;
        }
        ImmutableBytesWritable srcPtr = new ImmutableBytesWritable();
        if (!getSourceStrExpression().evaluate(tuple, srcPtr)) {
            return false;
        }
        TYPE.coerceBytes(srcPtr, TYPE, getSourceStrExpression().getSortOrder(), SortOrder.ASC);

        Expression offsetExpression = getOffsetExpression();
        if (!offsetExpression.evaluate(tuple, ptr)) {
            return false;
        }
        int offset = offsetExpression.getDataType().getCodec().decodeInt(ptr, offsetExpression.getSortOrder());

        // Account for 1 versus 0-based offset
        offset = offset - (offset <= 0 ? 0 : 1);

        return pattern.substr(srcPtr, offset, ptr);
    }

    @Override
    public Integer getMaxLength() {
        return maxLength;
    }

    @Override
    public OrderPreserving preservesOrder() {
        if (isOffsetConstant) {
            LiteralExpression literal = (LiteralExpression) getOffsetExpression();
            Number offsetNumber = (Number) literal.getValue();
            if (offsetNumber != null) { 
                int offset = offsetNumber.intValue();
                if (offset == 0 || offset == 1) {
                    return OrderPreserving.YES_IF_LAST;
                }
            }
        }
        return OrderPreserving.NO;
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        super.readFields(input);
        init();
    }

    @Override
    public int getKeyFormationTraversalIndex() {
        return preservesOrder() == OrderPreserving.NO ? NO_TRAVERSAL : 0;
    }

    private Expression getOffsetExpression() {
        return children.get(2);
    }

    private Expression getSourceStrExpression() {
        return children.get(0);
    }

    @Override
    public PDataType getDataType() {
        // ALways VARCHAR since we do not know in advanced how long the 
        // matched string will be.
        return PVarchar.INSTANCE;
    }

    @Override
    public String getName() {
        return NAME;
    }

}
