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

import java.util.Set;

import org.apache.hadoop.hbase.util.Pair;

import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.thirdparty.com.google.common.collect.ArrayListMultimap;
import org.apache.phoenix.thirdparty.com.google.common.collect.ListMultimap;

public class CreateCDCStatement extends MutableStatement {
    private final NamedNode cdcObjName;
    private final TableName dataTable;
    private final ColumnName timeIdxColumn;
    private final FunctionParseNode timeIdxFunc;
    private final Set<PTable.CDCChangeScope> includeScopes;
    private final ListMultimap<String,Pair<String,Object>> props;
    private final boolean ifNotExists;
    private final int bindCount;

    public CreateCDCStatement(NamedNode cdcObjName, TableName dataTable, ColumnName timeIdxColumn,
                              FunctionParseNode timeIdxFunc,
                              Set<PTable.CDCChangeScope> includeScopes, ListMultimap<String,
                              Pair<String, Object>> props, boolean ifNotExists, int bindCount) {
        this.cdcObjName = cdcObjName;
        this.dataTable = dataTable;
        this.timeIdxColumn = timeIdxColumn;
        this.timeIdxFunc = timeIdxFunc;
        this.includeScopes = includeScopes;
        this.props = props == null ? ArrayListMultimap.<String,Pair<String,Object>>create() : props;
        this.ifNotExists = ifNotExists;
        this.bindCount = bindCount;
    }

    public NamedNode getCdcObjName() {
        return cdcObjName;
    }

    public TableName getDataTable() {
        return dataTable;
    }

    public ColumnName getTimeIdxColumn() {
        return timeIdxColumn;
    }

    public FunctionParseNode getTimeIdxFunc() {
        return timeIdxFunc;
    }

    public Set<PTable.CDCChangeScope> getIncludeScopes() {
        return includeScopes;
    }

    public ListMultimap<String, Pair<String, Object>> getProps() {
        return props;
    }

    public boolean isIfNotExists() {
        return ifNotExists;
    }

    @Override
    public int getBindCount() {
        return bindCount;
    }
}
