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
package org.apache.phoenix.compile;


import java.sql.SQLException;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.tuple.Tuple;



/**
 * 
 * Projector for getting value from a select statement for an expression
 *
 * 
 * @since 0.1
 */
public class ExpressionProjector implements ColumnProjector {
    private final String name;
    private final Expression expression;
    private final String tableName;
    private final boolean isCaseSensitive;
    
    public ExpressionProjector(String name, String tableName, Expression expression, boolean isCaseSensitive) {
        this.name = name;
        this.expression = expression;
        this.tableName = tableName;
        this.isCaseSensitive = isCaseSensitive;
    }
    
    @Override
    public String getTableName() {
        return tableName;
    }

    @Override
    public Expression getExpression() {
        return expression;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public final Object getValue(Tuple tuple, PDataType type, ImmutableBytesWritable ptr) throws SQLException {
        try {
            Expression expression = getExpression();
            if (!expression.evaluate(tuple, ptr)) {
                return null;
            }
            if (ptr.getLength() == 0) {
                return null;
            }        
            return type.toObject(ptr, expression.getDataType(), expression.getSortOrder(), expression.getMaxLength(), expression.getScale());
        } catch (RuntimeException e) {
            // FIXME: Expression.evaluate does not throw SQLException
            // so this will unwrap throws from that.
            if (e.getCause() instanceof SQLException) {
                throw (SQLException) e.getCause();
            }
            throw e;
        }
    }

    @Override
    public boolean isCaseSensitive() {
        return isCaseSensitive;
    }
}
