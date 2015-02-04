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

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.function.RoundDecimalExpression;
import org.apache.phoenix.expression.function.RoundTimestampExpression;
import org.apache.phoenix.schema.types.PDate;
import org.apache.phoenix.schema.types.PDecimal;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.schema.types.PTimestamp;
import org.apache.phoenix.schema.types.PUnsignedTimestamp;
import org.apache.phoenix.schema.TypeMismatchException;
import org.apache.phoenix.util.SchemaUtil;

/**
 * 
 * Node representing the CAST operator in SQL.
 * 
 * 
 * @since 0.1
 *
 */
public class CastParseNode extends UnaryParseNode {
	
	private final PDataType dt;
    private final Integer maxLength;
    private final Integer scale;
	
	CastParseNode(ParseNode expr, String dataType, Integer maxLength, Integer scale, boolean arr) {
        this(expr, PDataType.fromSqlTypeName(SchemaUtil.normalizeIdentifier(dataType)), maxLength, scale, arr);
	}

	CastParseNode(ParseNode expr, PDataType dataType, Integer maxLength, Integer scale, boolean arr) {
        super(expr);
        if (arr == true) {
            dt = PDataType.fromTypeId(dataType.getSqlType() + PDataType.ARRAY_TYPE_BASE);
        } else {
            dt = dataType;
        }
        this.maxLength = maxLength;
        this.scale = scale;
	}

	@Override
    public <T> T accept(ParseNodeVisitor<T> visitor) throws SQLException {
        List<T> l = Collections.emptyList();
        if (visitor.visitEnter(this)) {
            l = acceptChildren(visitor);
        }
        return visitor.visitLeave(this, l);
    }

	public PDataType getDataType() {
		return dt;
	}

    public Integer getMaxLength() {
        return maxLength;
    }

    public Integer getScale() {
        return scale;
    }

    // TODO: don't repeat this ugly cast logic (maybe use isCastable in the last else block.
    public static Expression convertToRoundExpressionIfNeeded(PDataType fromDataType, PDataType targetDataType, List<Expression> expressions) throws SQLException {
	    Expression firstChildExpr = expressions.get(0);
	    if(fromDataType == targetDataType) {
	        return firstChildExpr;
//        } else if((fromDataType == PDataType.DATE || fromDataType == PDataType.UNSIGNED_DATE) && targetDataType.isCoercibleTo(PDataType.LONG)) {
//            return firstChildExpr;
//        } else if(fromDataType.isCoercibleTo(PDataType.LONG) && (targetDataType == PDataType.DATE || targetDataType == PDataType.UNSIGNED_DATE)) {
//            return firstChildExpr;
	    } else if((fromDataType == PDecimal.INSTANCE || fromDataType == PTimestamp.INSTANCE || fromDataType == PUnsignedTimestamp.INSTANCE) && targetDataType.isCoercibleTo(
          PLong.INSTANCE)) {
	        return RoundDecimalExpression.create(expressions);
	    } else if((fromDataType == PDecimal.INSTANCE || fromDataType == PTimestamp.INSTANCE || fromDataType == PUnsignedTimestamp.INSTANCE) && targetDataType.isCoercibleTo(
          PDate.INSTANCE)) {
	        return RoundTimestampExpression.create(expressions);
	    } else if(fromDataType.isCastableTo(targetDataType)) {
	        return firstChildExpr;
        } else {
            throw TypeMismatchException.newException(fromDataType, targetDataType, firstChildExpr.toString());
	    }
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((dt == null) ? 0 : dt.hashCode());
		result = prime * result
				+ ((maxLength == null) ? 0 : maxLength.hashCode());
		result = prime * result + ((scale == null) ? 0 : scale.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		CastParseNode other = (CastParseNode) obj;
		if (dt == null) {
			if (other.dt != null)
				return false;
		} else if (!dt.equals(other.dt))
			return false;
		if (maxLength == null) {
			if (other.maxLength != null)
				return false;
		} else if (!maxLength.equals(other.maxLength))
			return false;
		if (scale == null) {
			if (other.scale != null)
				return false;
		} else if (!scale.equals(other.scale))
			return false;
		return true;
	}
}
