/*
 * Copyright 2014 The Apache Software Foundation
 *
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
import org.apache.phoenix.schema.ColumnModifier;

public class PrimaryKeyConstraint extends NamedNode {
    public static final PrimaryKeyConstraint EMPTY = new PrimaryKeyConstraint(null, Collections.<Pair<ColumnName, ColumnModifier>>emptyList());

    private final List<Pair<ColumnName, ColumnModifier>> columns;
    private final HashMap<ColumnName, Pair<ColumnName, ColumnModifier>> columnNameToModifier;
    
    PrimaryKeyConstraint(String name, List<Pair<ColumnName, ColumnModifier>> columns) {
        super(name);
        this.columns = columns == null ? Collections.<Pair<ColumnName, ColumnModifier>>emptyList() : ImmutableList.copyOf(columns);
        this.columnNameToModifier = Maps.newHashMapWithExpectedSize(this.columns.size());
        for (Pair<ColumnName, ColumnModifier> p : this.columns) {
            this.columnNameToModifier.put(p.getFirst(), p);
        }
    }

    public List<Pair<ColumnName, ColumnModifier>> getColumnNames() {
        return columns;
    }
    
    public Pair<ColumnName, ColumnModifier> getColumn(ColumnName columnName) {
    	return columnNameToModifier.get(columnName);
    }
    
    public boolean contains(ColumnName columnName) {
        return columnNameToModifier.containsKey(columnName);
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
