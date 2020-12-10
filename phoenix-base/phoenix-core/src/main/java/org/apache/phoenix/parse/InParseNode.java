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

import org.apache.phoenix.compile.ColumnResolver;



/**
 * 
 * Node representing IN subquery expression in SQL
 *
 * 
 * @since 0.1
 */
public class InParseNode extends BinaryParseNode {
    private final boolean negate;
    private final boolean isSubqueryDistinct;

    InParseNode(ParseNode l, ParseNode r, boolean negate, boolean isSubqueryDistinct) {
        super(l, r);
        this.negate = negate;
        this.isSubqueryDistinct = isSubqueryDistinct;
    }
    
    public boolean isNegate() {
        return negate;
    }
    
    public boolean isSubqueryDistinct() {
        return isSubqueryDistinct;
    }

    @Override
    public <T> T accept(ParseNodeVisitor<T> visitor) throws SQLException {
        List<T> l = Collections.emptyList();
        if (visitor.visitEnter(this)) {
            l = acceptChildren(visitor);
        }
        return visitor.visitLeave(this, l);
    }

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + (isSubqueryDistinct ? 1231 : 1237);
		result = prime * result + (negate ? 1231 : 1237);
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
		InParseNode other = (InParseNode) obj;
		if (isSubqueryDistinct != other.isSubqueryDistinct)
			return false;
		if (negate != other.negate)
			return false;
		return true;
	}

    @Override
    public void toSQL(ColumnResolver resolver, StringBuilder buf) {
        getChildren().get(0).toSQL(resolver, buf);
        if (negate) buf.append(" NOT");
        buf.append(" IN (");
        getChildren().get(1).toSQL(resolver, buf);
        buf.append(')');
    }
}
