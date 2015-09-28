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
 * Node representing a CASE in SQL
 *
 * 
 * @since 0.1
 */
public class CaseParseNode extends CompoundParseNode {

    CaseParseNode(List<ParseNode> children) {
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
        buf.append("CASE ");
        List<ParseNode> children = getChildren();
        for (int i = 0; i < children.size() - 1; i+=2) {
            buf.append("WHEN ");
            children.get(i+1).toSQL(resolver, buf);
            buf.append(" THEN ");
            children.get(i).toSQL(resolver, buf);
        }
        if (children.size() % 2 != 0) { // has ELSE
            buf.append(" ELSE ");
            children.get(children.size()-1).toSQL(resolver, buf);
        }
        buf.append(" END ");
    }
}
