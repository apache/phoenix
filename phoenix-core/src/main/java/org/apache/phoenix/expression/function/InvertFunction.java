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

import java.sql.SQLException;
import java.util.List;

import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.compile.KeyPart;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.parse.FunctionParseNode.Argument;
import org.apache.phoenix.parse.FunctionParseNode.BuiltInFunction;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.tuple.Tuple;

@BuiltInFunction(name = InvertFunction.NAME, args = { @Argument() })
public class InvertFunction extends ScalarFunction {
    public static final String NAME = "INVERT";

    public InvertFunction() throws SQLException {}

    public InvertFunction(List<Expression> children) throws SQLException {
        super(children);
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        if (!getChildExpression().evaluate(tuple, ptr)) { return false; }
        if (ptr.getLength() == 0) { return true; }
        byte[] buf = new byte[ptr.getLength()];
        SortOrder.invert(ptr.get(), ptr.getOffset(), buf, 0, ptr.getLength());
        ptr.set(buf);
        return true;
    }

    @Override
    public SortOrder getSortOrder() {
        return getChildExpression().getSortOrder() == SortOrder.ASC ? SortOrder.DESC : SortOrder.ASC;
    }

    @Override
    public PDataType getDataType() {
        return getChildExpression().getDataType();
    }

    @Override
    public Integer getMaxLength() {
        return getChildExpression().getMaxLength();
    }

    @Override
    public boolean isNullable() {
        return getChildExpression().isNullable();
    }

    @Override
    public String getName() {
        return NAME;
    }

    /**
     * INVERT may be optimized through
     */
    @Override
    public int getKeyFormationTraversalIndex() {
        return 0;
    }

    /**
     * Invert the childPart key range
     */
    @Override
    public KeyPart newKeyPart(final KeyPart childPart) {
        return new KeyPart() {

            @Override
            public KeyRange getKeyRange(CompareOp op, Expression rhs) {
                KeyRange range = childPart.getKeyRange(op, rhs);
                return range.invert();
            }

            @Override
            public List<Expression> getExtractNodes() {
                return childPart.getExtractNodes();
            }

            @Override
            public PColumn getColumn() {
                return childPart.getColumn();
            }
        };
    }

    @Override
    public OrderPreserving preservesOrder() {
        return OrderPreserving.YES;
    }

    private Expression getChildExpression() {
        return children.get(0);
    }
}
