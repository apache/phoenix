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

import java.util.*;


import org.apache.phoenix.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.phoenix.util.SchemaUtil;

/**
 * 
 * Definition of a Column Family at DDL time
 *
 * 
 * @since 0.1
 */
public class ColumnFamilyDef {
    private final String name;
    private final List<ColumnDef> columnDefs;
    private final Map<String,Object> props;
    
    ColumnFamilyDef(String name, List<ColumnDef> columnDefs, Map<String,Object> props) {
        this.name = SchemaUtil.normalizeIdentifier(name);
        this.columnDefs = ImmutableList.copyOf(columnDefs);
        this.props = props == null ? Collections.<String,Object>emptyMap() : props;
    }

    public String getName() {
        return name;
    }

    public List<ColumnDef> getColumnDefs() {
        return columnDefs;
    }

    public Map<String,Object> getProps() {
        return props;
    }
}
