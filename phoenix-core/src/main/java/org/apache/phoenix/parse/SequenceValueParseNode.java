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

import org.apache.phoenix.compile.ColumnResolver;


public class SequenceValueParseNode extends TerminalParseNode {
    public enum Op {
        NEXT_VALUE("NEXT"), 
        CURRENT_VALUE("CURRENT");
    
        private final String name;
        Op(String name) {
            this.name = name;
        }
        public String getName() {
            return name;
    };
    
    }
	private final TableName tableName;
	private final Op op;

	public SequenceValueParseNode(TableName tableName, Op op) {
		this.tableName = tableName;
		this.op = op;
	}

	@Override
	public <T> T accept(ParseNodeVisitor<T> visitor) throws SQLException {
		return visitor.visit(this);
	}

	public TableName getTableName() {
		return tableName;
	}

    @Override
    public boolean isStateless() {
        return true;
    }

    public Op getOp() {
        return op;
    }

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((op == null) ? 0 : op.hashCode());
		result = prime * result
				+ ((tableName == null) ? 0 : tableName.hashCode());
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
		SequenceValueParseNode other = (SequenceValueParseNode) obj;
		if (op != other.op)
			return false;
		if (tableName == null) {
			if (other.tableName != null)
				return false;
		} else if (!tableName.equals(other.tableName))
			return false;
		return true;
	}

    @Override
    public void toSQL(ColumnResolver resolver, StringBuilder buf) {
        buf.append(' ');
        buf.append(op.getName());
        buf.append(" VALUE FOR ");
        buf.append(tableName);
    }
}