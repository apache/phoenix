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
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.hbase.util.Pair;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import org.apache.phoenix.schema.SortOrder;

public class PrimaryKeyConstraint extends NamedNode {
    public static final PrimaryKeyConstraint EMPTY = new PrimaryKeyConstraint(null, Collections.<Pair<ColumnName, SortOrder>>emptyList());

    private final List<Pair<ColumnName, SortOrder>> columns;
    private final HashMap<ColumnName, Pair<ColumnName, SortOrder>> columnNameToSortOrder;
    
    PrimaryKeyConstraint(String name, List<Pair<ColumnName, SortOrder>> columns) {
        super(name);
        this.columns = columns == null ? Collections.<Pair<ColumnName, SortOrder>>emptyList() : ImmutableList.copyOf(columns);
        this.columnNameToSortOrder = Maps.newHashMapWithExpectedSize(this.columns.size());
        for (Pair<ColumnName, SortOrder> p : this.columns) {
            this.columnNameToSortOrder.put(p.getFirst(), p);
        }
    }

    public List<Pair<ColumnName, SortOrder>> getColumnNames() {
        return columns;
    }
    
    public Pair<ColumnName, SortOrder> getColumn(ColumnName columnName) {
    	return columnNameToSortOrder.get(columnName);
    }
    
    public boolean contains(ColumnName columnName) {
        return columnNameToSortOrder.containsKey(columnName);
    }
    
    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj);
    }
    
}
