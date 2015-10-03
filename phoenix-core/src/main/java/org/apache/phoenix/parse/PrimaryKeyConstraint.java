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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.schema.SortOrder;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

public class PrimaryKeyConstraint extends NamedNode {
    public static final PrimaryKeyConstraint EMPTY = new PrimaryKeyConstraint(null, Collections.<ColumnDefInPkConstraint>emptyList());

    private final List<Pair<ColumnName, SortOrder>> columns;
    private final Map<ColumnName, Pair<ColumnName, SortOrder>> columnNameToSortOrder;
    private final Map<ColumnName, Pair<ColumnName, Boolean>> columnNameToRowTimestamp;
    private final int numColumnsWithRowTimestamp;
    
    PrimaryKeyConstraint(String name, List<ColumnDefInPkConstraint> columnDefs) {
        super(name);
        if (columnDefs == null) {
            this.columns = Collections.<Pair<ColumnName, SortOrder>>emptyList();
            this.columnNameToSortOrder = Collections.<ColumnName, Pair<ColumnName, SortOrder>>emptyMap();
            this.columnNameToRowTimestamp = Collections.<ColumnName, Pair<ColumnName, Boolean>>emptyMap();
            numColumnsWithRowTimestamp = 0;
        } else {
            int numRowTimestampCols = 0;
            List<Pair<ColumnName, SortOrder>> l = new ArrayList<>(columnDefs.size());
            this.columnNameToSortOrder = Maps.newHashMapWithExpectedSize(columnDefs.size());
            this.columnNameToRowTimestamp = Maps.newHashMapWithExpectedSize(columnDefs.size());
            for (ColumnDefInPkConstraint colDef : columnDefs) {
                Pair<ColumnName, SortOrder> p = Pair.newPair(colDef.getColumnName(), colDef.getSortOrder());
                l.add(p);
                this.columnNameToSortOrder.put(colDef.getColumnName(), p);
                this.columnNameToRowTimestamp.put(colDef.getColumnName(), Pair.newPair(colDef.getColumnName(), colDef.isRowTimestamp()));
                if (colDef.isRowTimestamp()) {
                    numRowTimestampCols++;
                }
            }
            this.numColumnsWithRowTimestamp = numRowTimestampCols;
            this.columns = ImmutableList.copyOf(l); 
        }
    }

    public List<Pair<ColumnName, SortOrder>> getColumnNames() {
        return columns;
    }
    
    public Pair<ColumnName, SortOrder> getColumnWithSortOrder(ColumnName columnName) {
    	return columnNameToSortOrder.get(columnName);
    }
    
    public boolean isColumnRowTimestamp(ColumnName columnName) {
        return columnNameToRowTimestamp.get(columnName) != null && columnNameToRowTimestamp.get(columnName).getSecond() == Boolean.TRUE;
    }
    
    public boolean contains(ColumnName columnName) {
        return columnNameToSortOrder.containsKey(columnName);
    }
    
    public int getNumColumnsWithRowTimestamp() {
        return numColumnsWithRowTimestamp;
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
