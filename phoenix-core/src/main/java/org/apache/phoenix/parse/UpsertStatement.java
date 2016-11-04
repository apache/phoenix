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

import org.apache.hadoop.hbase.util.Pair;

public class UpsertStatement extends DMLStatement {
    private final List<ColumnName> columns;
    private final List<ParseNode> values;
    private final SelectStatement select;
    private final HintNode hint;
    private final List<Pair<ColumnName,ParseNode>> onDupKeyPairs;

    public UpsertStatement(NamedTableNode table, HintNode hint, List<ColumnName> columns,
            List<ParseNode> values, SelectStatement select, int bindCount,
            Map<String, UDFParseNode> udfParseNodes, List<Pair<ColumnName,ParseNode>> onDupKeyPairs) {
        super(table, bindCount, udfParseNodes);
        this.columns = columns == null ? Collections.<ColumnName>emptyList() : columns;
        this.values = values;
        this.select = select;
        this.hint = hint == null ? HintNode.EMPTY_HINT_NODE : hint;
        this.onDupKeyPairs = onDupKeyPairs;
    }

    public List<ColumnName> getColumns() {
        return columns;
    }

    public List<ParseNode> getValues() {
        return values;
    }

    public SelectStatement getSelect() {
        return select;
    }

    public HintNode getHint() {
        return hint;
    }

    public List<Pair<ColumnName,ParseNode>> getOnDupKeyPairs() {
        return onDupKeyPairs;
    }
}
