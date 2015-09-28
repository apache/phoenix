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



/**
 * 
 * Node representing a subquery in SQL
 *
 * 
 * @since 0.1
 */
public class SubqueryParseNode extends TerminalParseNode {
    private final SelectStatement select;
    private final boolean expectSingleRow;

    SubqueryParseNode(SelectStatement select, boolean expectSingleRow) {
        this.select = select;
        this.expectSingleRow = expectSingleRow;
    }
    
    public SelectStatement getSelectNode() {
        return select;
    }
    
    public boolean expectSingleRow() {
        return expectSingleRow;
    }

    @Override
    public <T> T accept(ParseNodeVisitor<T> visitor) throws SQLException {
        return visitor.visit(this);
    }

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (expectSingleRow ? 1231 : 1237);
		result = prime * result + ((select == null) ? 0 : select.hashCode());
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
		SubqueryParseNode other = (SubqueryParseNode) obj;
		if (expectSingleRow != other.expectSingleRow)
			return false;
		if (select == null) {
			if (other.select != null)
				return false;
		} else if (!select.equals(other.select))
			return false;
		return true;
	}
    
    @Override
    public void toSQL(ColumnResolver resolver, StringBuilder buf) {
        buf.append('(');
        select.toSQL(resolver, buf);
        buf.append(')');
    }    
}
