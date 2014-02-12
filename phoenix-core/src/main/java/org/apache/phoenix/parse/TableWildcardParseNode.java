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

public class TableWildcardParseNode extends NamedParseNode {
    private final TableName tableName;
    private final boolean isRewrite;
    
    public static TableWildcardParseNode create(TableName tableName, boolean isRewrite) {
        return new TableWildcardParseNode(tableName, isRewrite);
    }

    TableWildcardParseNode(TableName tableName, boolean isRewrite) {
        super(tableName.toString());
        this.tableName = tableName;
        this.isRewrite = isRewrite;
    }
    
    public TableName getTableName() {
        return tableName;
    }
    
    public boolean isRewrite() {
        return isRewrite;
    }

    @Override
    public <T> T accept(ParseNodeVisitor<T> visitor) throws SQLException {
        return visitor.visit(this);
    }

}

