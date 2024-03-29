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

import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.thirdparty.com.google.common.collect.ImmutableListMultimap;
import org.apache.phoenix.thirdparty.com.google.common.collect.ListMultimap;

public class AlterCDCStatement extends SingleTableStatement {
    private final String dataTableName;
    private final boolean ifExists;
    private ListMultimap<String, Pair<String,Object>> props;
    private static final PTableType tableType=PTableType.CDC;

    public AlterCDCStatement(NamedTableNode cdcTableNode, String dataTableName, boolean ifExists) {
        this(cdcTableNode,dataTableName,ifExists,null);
    }

    public AlterCDCStatement(NamedTableNode cdcTableNode, String dataTableName, boolean ifExists, ListMultimap<String,Pair<String,Object>> props) {
        super(cdcTableNode,0);
        this.dataTableName = dataTableName;
        this.ifExists = ifExists;
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

    public ListMultimap<String,Pair<String,Object>> getProps() { return props; }

    public PTableType getTableType(){ return tableType; }
}
