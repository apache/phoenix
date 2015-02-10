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
import java.util.List;

import org.apache.phoenix.compile.ColumnResolver;




/**
 * 
 * Abstract base class for a parse node in SQL
 *
 * 
 * @since 0.1
 */
public abstract class ParseNode {
    public abstract List<ParseNode> getChildren();
    public abstract <T> T accept(ParseNodeVisitor<T> visitor) throws SQLException;
    
    public boolean isStateless() {
        return false;
    }
    
    /**
     * Allows node to override what the alias is for a given node.
     * Useful for a column reference, as JDBC says that the alias
     * name for "a.b" should be "b"
     * @return the alias to use for this node or null for no alias
     */
    public String getAlias() {
        return null;
    }
    
    @Override
    public final String toString() {
        StringBuilder buf = new StringBuilder();
        toSQL(null, buf);
        return buf.toString();
    }
    
    public abstract void toSQL(ColumnResolver resolver, StringBuilder buf);
}
