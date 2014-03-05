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

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.phoenix.schema.PTableType;

public class AddColumnStatement extends AlterTableStatement {
    private final List<ColumnDef> columnDefs;
    private final boolean ifNotExists;
    private final Map<String,Object> props;
    
    protected AddColumnStatement(NamedTableNode table, PTableType tableType, List<ColumnDef> columnDefs, boolean ifNotExists, Map<String, Object> props) {
        super(table, tableType);
        this.columnDefs = columnDefs;
        this.props = props == null ? Collections.<String,Object>emptyMap() : props;
        this.ifNotExists = ifNotExists;
    }

    public List<ColumnDef> getColumnDefs() {
        return columnDefs;
    }
    
    public boolean ifNotExists() {
        return ifNotExists;
    }

    public Map<String,Object> getProps() {
        return props;
    }
}
