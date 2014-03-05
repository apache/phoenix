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

import com.google.common.collect.ImmutableList;

/**
 * Node representing an explicit table reference in the FROM clause of SQL
 * 
 * 
 * @since 0.1
 */
public class NamedTableNode extends ConcreteTableNode {

    private final List<ColumnDef> dynColumns;

    public static NamedTableNode create (String alias, TableName name, List<ColumnDef> dynColumns) {
        return new NamedTableNode(alias, name, dynColumns);
    }
    
    NamedTableNode(String alias, TableName name) {
        super(alias, name);
        dynColumns = Collections.<ColumnDef> emptyList();
    }

    NamedTableNode(String alias, TableName name, List<ColumnDef> dynColumns) {
        super(alias, name);
        if (dynColumns != null) {
            this.dynColumns = ImmutableList.copyOf(dynColumns);
        } else {
            this.dynColumns = Collections.<ColumnDef> emptyList();
        }
    }

    @Override
    public void accept(TableNodeVisitor visitor) throws SQLException {
        visitor.visit(this);
    }

    public List<ColumnDef> getDynamicColumns() {
        return dynColumns;
    }
}

