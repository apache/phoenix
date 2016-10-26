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

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;


/**
 *
 * Internal function used to get the default value for a column not specified in UPSERT.
 * If expr1 is evaluated (can be null), then it is returned, otherwise expr2 is returned.
 *
 */
public class DefaultValueExpression extends ScalarFunction {
    public static final String NAME = "DEFAULT";

    public DefaultValueExpression() {
    }

    public DefaultValueExpression(List<Expression> children) throws SQLException {
        super(children);
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        boolean evaluated = children.get(0).evaluate(tuple, ptr);
        if (evaluated) {
            // Will potentially evaluate to null without evaluating the second expression
            return true;
        }
        if (tuple.isImmutable()) {// True for the last time an evaluation is happening on the row
            Expression secondChild = children.get(1);
            if (secondChild.evaluate(tuple, ptr)) {
                // Coerce the type of the second child to the type of the first child
                getDataType().coerceBytes(ptr, null, secondChild.getDataType(),
                        secondChild.getMaxLength(), secondChild.getScale(),
                        secondChild.getSortOrder(),
                        getMaxLength(), getScale(),
                        getSortOrder());
                return true;
            }
        }
        return false;
    }

    @Override
    public PDataType getDataType() {
        return children.get(0).getDataType();
    }

    @Override
    public Integer getMaxLength() {
        return children.get(0).getMaxLength();
    }

    @Override
    public boolean isNullable() {
        return children.get(0).isNullable() && children.get(1).isNullable();
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public boolean requiresFinalEvaluation() {
        return true;
    }
}
