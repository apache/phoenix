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

import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;



/**
 * 
 * Node representing the IN literal list expression in SQL
 *
 * 
 * @since 0.1
 */
public class InListParseNode extends CompoundParseNode {
    private final boolean negate;

    InListParseNode(List<ParseNode> children, boolean negate) {
        super(children);
        // All values in the IN must be constant. First child is the LHS
        for (int i = 1; i < children.size(); i++) {
            ParseNode child = children.get(i);
            if (!child.isStateless()) {
                throw new ParseException(new SQLExceptionInfo.Builder(SQLExceptionCode.VALUE_IN_LIST_NOT_CONSTANT)
                .build().buildException());
            }
        }
        this.negate = negate;
    }
    
    public boolean isNegate() {
        return negate;
    }

    @Override
    public <T> T accept(ParseNodeVisitor<T> visitor) throws SQLException {
        List<T> l = Collections.emptyList();
        if (visitor.visitEnter(this)) {
            l = acceptChildren(visitor);
        }
        return visitor.visitLeave(this, l);
    }
}
