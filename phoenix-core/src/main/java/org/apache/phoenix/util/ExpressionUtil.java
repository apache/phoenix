/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by
 * applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.apache.phoenix.util;

import java.sql.SQLException;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.Determinism;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.schema.ColumnRef;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.types.PBoolean;
import org.apache.phoenix.schema.types.PDataType;

public class ExpressionUtil {
	private ExpressionUtil() {
	}

	public static boolean isConstant(Expression expression) {
		return (expression.isStateless() && (expression.getDeterminism() == Determinism.ALWAYS
				|| expression.getDeterminism() == Determinism.PER_STATEMENT));
	}

    public static LiteralExpression getConstantExpression(Expression expression, ImmutableBytesWritable ptr)
            throws SQLException {
        Object value = null;
        PDataType type = expression.getDataType();
        if (expression.evaluate(null, ptr) && ptr.getLength() != 0) {
            value = type.toObject(ptr.get(), ptr.getOffset(), ptr.getLength(), type, expression.getSortOrder(), expression.getMaxLength(), expression.getScale());
        }
        return LiteralExpression.newConstant(value, type, expression.getDeterminism());
    }

    public static boolean isNull(Expression expression, ImmutableBytesWritable ptr) {
        return isConstant(expression) && (!expression.evaluate(null, ptr) || ptr.getLength() == 0);
    }

    public static LiteralExpression getNullExpression(Expression expression) throws SQLException {
        return LiteralExpression.newConstant(null, expression.getDataType(), expression.getDeterminism());
    }
    
    public static boolean evaluatesToTrue(Expression expression) {
        if (isConstant(expression)) {
            ImmutableBytesWritable ptr = new ImmutableBytesWritable();
            expression.evaluate(null, ptr);
            return Boolean.TRUE.equals(PBoolean.INSTANCE.toObject(ptr));
        }
        return false;
    }

    public static boolean isPkPositionChanging(TableRef tableRef, List<Expression> projectedExpressions) throws SQLException {
        for (int i = 0; i < tableRef.getTable().getPKColumns().size(); i++) {
            PColumn column = tableRef.getTable().getPKColumns().get(i);
            Expression source = projectedExpressions.get(i);
            if (source == null || !source
                    .equals(new ColumnRef(tableRef, column.getPosition()).newColumnExpression())) { return true; }
        }
        return false;
    }

}
