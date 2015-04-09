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

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

import org.apache.phoenix.compile.ColumnResolver;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PTimestamp;

/**
 * 
 * Node representing literal expressions such as 1,2.5,'foo', and NULL in SQL
 *
 * 
 * @since 0.1
 */
public class LiteralParseNode extends TerminalParseNode {
    public static final List<ParseNode> STAR = Collections.<ParseNode>singletonList(new LiteralParseNode(1));
    public static final ParseNode NULL = new LiteralParseNode(null);
    public static final ParseNode ZERO = new LiteralParseNode(0);
    public static final ParseNode ONE = new LiteralParseNode(1);
    public static final ParseNode MINUS_ONE = new LiteralParseNode(-1L);
    public static final ParseNode TRUE = new LiteralParseNode(true);
    // Parser representation of Long.MIN_VALUE, as ABS(Long.MIN_VALUE) is too big to fit into a Long
    public static final ParseNode MIN_LONG_AS_BIG_DECIMAL = new LiteralParseNode(BigDecimal.valueOf(Long.MIN_VALUE).abs());
    // See ParseNodeFactory.negate(), as MIN_LONG_AS_BIG_DECIMAL will be converted MIN_LONG if negated.
    public static final ParseNode MIN_LONG = new LiteralParseNode(Long.MIN_VALUE);
    
    private final Object value;
    private final PDataType type;
    
    public LiteralParseNode(Object value) {
        this.type = PDataType.fromLiteral(value);
        // This will make the value null if the value passed through represents null for the given type.
        // For example, an empty string is treated as a null.
        this.value = this.type == null ? null : this.type.toObject(value, this.type);
    }

    public LiteralParseNode(Object value, PDataType type) {
        this.type = type;
        // This will make the value null if the value passed through represents null for the given type.
        // For example, an empty string is treated as a null.
        this.value = this.type == null ? null : this.type.toObject(value, this.type);
    }

    public PDataType getType() {
        return type;
    }
    
    public Object getValue() {
        return value;
    }

    @Override
    public boolean isStateless() {
        return true;
    }
    
    @Override
    public <T> T accept(ParseNodeVisitor<T> visitor) throws SQLException {
        return visitor.visit(this);
    }

    public byte[] getBytes() {
        return type == null ? null : type.toBytes(value);
    }
    
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((type == null) ? 0 : type.hashCode());
		result = prime * result + ((value == null) ? 0 : value.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		LiteralParseNode other = (LiteralParseNode) obj;
		if (value == other.value) return true;
		if (type == null) return false;
		return type.isComparableTo(other.type) && type.compareTo(value, other.value, other.type) == 0;
	}

    @Override
    public void toSQL(ColumnResolver resolver, StringBuilder buf) {
        if (value == null) {
            buf.append(" null ");
        } else {
            // TODO: move into PDataType?
            if (type.isCoercibleTo(PTimestamp.INSTANCE)) {
                buf.append(type);
                buf.append(' ');
                buf.append(type.toStringLiteral(value, null));
            } else {
                buf.append(type.toStringLiteral(value, null));
            }
        }
    }
}
