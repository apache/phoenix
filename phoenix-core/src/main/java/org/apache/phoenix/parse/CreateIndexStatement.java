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

import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.schema.PTable.IndexType;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;


public class CreateIndexStatement extends SingleTableStatement {
    private final TableName indexTableName;
    private final IndexKeyConstraint indexKeyConstraint;
    private final List<ColumnName> includeColumns;
    private final List<ParseNode> splitNodes;
    private final ListMultimap<String,Pair<String,Object>> props;
    private final boolean ifNotExists;
    private final IndexType indexType;
    private final boolean async;

    public CreateIndexStatement(NamedNode indexTableName, NamedTableNode dataTable, 
            IndexKeyConstraint indexKeyConstraint, List<ColumnName> includeColumns, List<ParseNode> splits,
            ListMultimap<String,Pair<String,Object>> props, boolean ifNotExists, IndexType indexType, boolean async, int bindCount) {
        super(dataTable, bindCount);
        this.indexTableName =TableName.create(dataTable.getName().getSchemaName(),indexTableName.getName());
        this.indexKeyConstraint = indexKeyConstraint == null ? IndexKeyConstraint.EMPTY : indexKeyConstraint;
        this.includeColumns = includeColumns == null ? Collections.<ColumnName>emptyList() : includeColumns;
        this.splitNodes = splits == null ? Collections.<ParseNode>emptyList() : splits;
        this.props = props == null ? ArrayListMultimap.<String,Pair<String,Object>>create() : props;
        this.ifNotExists = ifNotExists;
        this.indexType = indexType;
        this.async = async;
    }

    public IndexKeyConstraint getIndexConstraint() {
        return indexKeyConstraint;
    }

    public List<ColumnName> getIncludeColumns() {
        return includeColumns;
    }

    public TableName getIndexTableName() {
        return indexTableName;
    }

    public List<ParseNode> getSplitNodes() {
        return splitNodes;
    }

    public ListMultimap<String,Pair<String,Object>> getProps() {
        return props;
    }

    public boolean ifNotExists() {
        return ifNotExists;
    }


    public IndexType getIndexType() {
        return indexType;
    }

    public boolean isAsync() {
        return async;
    }

}
