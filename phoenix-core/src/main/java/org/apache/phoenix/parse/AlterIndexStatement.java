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

import org.apache.phoenix.thirdparty.com.google.common.collect.ImmutableListMultimap;
import org.apache.phoenix.thirdparty.com.google.common.collect.ListMultimap;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PTableType;

public class AlterIndexStatement extends SingleTableStatement {
    private final String dataTableName;
    private final boolean ifExists;
    private final PIndexState indexState;
    private boolean async;
    private boolean isRebuildAll;
    private ListMultimap<String,Pair<String,Object>> props;
    private static final PTableType tableType=PTableType.INDEX;

    public AlterIndexStatement(NamedTableNode indexTableNode, String dataTableName, boolean ifExists, PIndexState indexState, boolean isRebuildAll, boolean async) {
        this(indexTableNode,dataTableName,ifExists,indexState, isRebuildAll, async,null);
    }

    public AlterIndexStatement(NamedTableNode indexTableNode, String dataTableName, boolean ifExists, PIndexState indexState, boolean isRebuildAll, boolean async, ListMultimap<String,Pair<String,Object>> props) {
        super(indexTableNode,0);
        this.dataTableName = dataTableName;
        this.ifExists = ifExists;
        this.indexState = indexState;
        this.async = async;
        this.isRebuildAll = isRebuildAll;
        this.props= props==null ? ImmutableListMultimap.<String,Pair<String,Object>>of() : props;
    }

    public String getTableName() {
        return dataTableName;
    }

    @Override
    public int getBindCount() {
        return 0;
    }

    public boolean ifExists() {
        return ifExists;
    }

    public PIndexState getIndexState() {
        return indexState;
    }

    public boolean isAsync() {
        return async;
    }

    public boolean isRebuildAll() {
        return isRebuildAll;
    }

    public ListMultimap<String,Pair<String,Object>> getProps() { return props; }

    public PTableType getTableType(){ return tableType; }
}
