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
import org.apache.phoenix.schema.types.PArrayDataType;

/**
 * Holds the list of array elements that will be used by the upsert stmt with ARRAY column 
 *
 */
public class ArrayConstructorNode extends CompoundParseNode {

	public ArrayConstructorNode(List<ParseNode> children) {
		super(children);
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
    public void toSQL(ColumnResolver resolver, StringBuilder buf) {
        buf.append(' ');
        buf.append(PArrayDataType.ARRAY_TYPE_SUFFIX);
        buf.append('[');
        List<ParseNode> children = getChildren();
        children.get(0).toSQL(resolver, buf);
        for (int i = 1 ; i < children.size(); i++) {
            buf.append(',');
            children.get(i).toSQL(resolver, buf);
        }
        buf.append(']');
    }
}
