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
 * Node representing a TABLE bound using an ARRAY variable
 * TODO: modify grammar to support this
 * 
 * @since 0.1
 */
public class BindTableNode extends ConcreteTableNode {

    BindTableNode(String alias, TableName name) {
        super(alias, name);
    }

    @Override
    public <T> T accept(TableNodeVisitor<T> visitor) throws SQLException {
        return visitor.visit(this);
    }

    @Override
    public void toSQL(ColumnResolver resolver, StringBuilder buf) {
        buf.append(this.getName().toString());
        if (this.getAlias() != null) buf.append(" " + this.getAlias());
        buf.append(' ');
    }
}

