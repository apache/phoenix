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
import org.apache.phoenix.expression.Determinism;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.util.regex.AbstractBasePattern;
import org.apache.phoenix.parse.FunctionParseNode;
import org.apache.phoenix.parse.FunctionParseNode.Argument;
import org.apache.phoenix.parse.FunctionParseNode.BuiltInFunction;
import org.apache.phoenix.parse.RegexpSubstrParseNode;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PInteger;
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
    @Argument(allowedTypes={PLong.class}, defaultValue="1")},
    classType = FunctionParseNode.FunctionClassType.ABSTRACT,
    derivedFunctions = {ByteBasedRegexpSubstrFunction.class, StringBasedRegexpSubstrFunction.class})
public abstract class RegexpSubstrFunction extends PrefixFunction {
    public static final String NAME = "REGEXP_SUBSTR";

    private AbstractBasePattern pattern;
    private Integer offset;
    private Integer maxLength;

    private static final PDataType TYPE = PVarchar.INSTANCE;
    
    public RegexpSubstrFunction() { }

    public RegexpSubstrFunction(List<Expression> children) {
        super(children);
        init();
    }

    protected abstract AbstractBasePattern compilePatternSpec(String value);

    private void init() {
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        Expression patternExpr = getPatternExpression();
        if (patternExpr.isStateless() && patternExpr.getDeterminism() == Determinism.ALWAYS && patternExpr.evaluate(null, ptr)) {
            String patternStr = (String) patternExpr.getDataType().toObject(ptr, patternExpr.getSortOrder());
            if (patternStr != null) {
                pattern = compilePatternSpec(patternStr);
            }
        }
        // If the source string has a fixed width, then the max length would be the length 
        // of the source string minus the offset, or the absolute value of the offset if 
        // it's negative. Offset number is a required argument. However, if the source string
        // is not fixed width, the maxLength would be null.
        Expression offsetExpr = getOffsetExpression();
        if (offsetExpr.isStateless() && offsetExpr.getDeterminism() == Determinism.ALWAYS && offsetExpr.evaluate(null, ptr)) {
            offset = (Integer)PInteger.INSTANCE.toObject(ptr, offsetExpr.getDataType(), offsetExpr.getSortOrder());
            if (offset != null) {
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
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        AbstractBasePattern pattern = this.pattern;
        if (pattern == null) {
            Expression patternExpr = getPatternExpression();
            if (!patternExpr.evaluate(tuple, ptr)) {
                return false;
            }
            if (ptr.getLength() == 0) {
                return true;
            }
            pattern = compilePatternSpec((String) patternExpr.getDataType().toObject(ptr, patternExpr.getSortOrder()));
        }
        int offset;
        if (this.offset == null) {
            Expression offsetExpression = getOffsetExpression();
            if (!offsetExpression.evaluate(tuple, ptr)) {
                return false;
            }
            if (ptr.getLength() == 0) {
                return true;
            }
            offset = offsetExpression.getDataType().getCodec().decodeInt(ptr, offsetExpression.getSortOrder());
        } else {
            offset = this.offset;
        }
        Expression strExpression = getSourceStrExpression();
        if (!strExpression.evaluate(tuple, ptr)) {
            return false;
        }
        if (ptr.get().length == 0) {
            return true;
        }

        TYPE.coerceBytes(ptr, strExpression.getDataType(), strExpression.getSortOrder(), SortOrder.ASC);

        // Account for 1 versus 0-based offset
        offset = offset - (offset <= 0 ? 0 : 1);

        pattern.substr(ptr, offset);
        return true;
    }

    @Override
    public Integer getMaxLength() {
        return maxLength;
    }

    @Override
    public OrderPreserving preservesOrder() {
        if (offset != null) {
            if (offset == 0 || offset == 1) {
                return OrderPreserving.YES_IF_LAST;
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

    private Expression getPatternExpression() {
        return children.get(1);
    }

    private Expression getSourceStrExpression() {
        return children.get(0);
    }

    @Override
    public PDataType getDataType() {
        // ALways VARCHAR since we do not know in advanced how long the 
        // matched string will be.
        return TYPE;
    }

    @Override
    public String getName() {
        return NAME;
    }

}
