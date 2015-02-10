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
 * Node representing the selection of all columns (*) in the SELECT clause of SQL
 *
 * 
 * @since 0.1
 */
public class WildcardParseNode extends TerminalParseNode {
    public static final String NAME = "*";
    public static final WildcardParseNode INSTANCE = new WildcardParseNode(false);
    public static final WildcardParseNode REWRITE_INSTANCE = new WildcardParseNode(true);

    private final boolean isRewrite;

    private WildcardParseNode(boolean isRewrite) {
        this.isRewrite = isRewrite;
    }

    @Override
    public <T> T accept(ParseNodeVisitor<T> visitor) throws SQLException {
        return visitor.visit(this);
    }

    public boolean isRewrite() {
        return isRewrite;
    }

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (isRewrite ? 1231 : 1237);
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
		WildcardParseNode other = (WildcardParseNode) obj;
		if (isRewrite != other.isRewrite)
			return false;
		return true;
	}

    @Override
    public void toSQL(ColumnResolver resolver, StringBuilder buf) {
        buf.append(' ');
        buf.append(NAME);
        buf.append(' ');
    }    
    
}
