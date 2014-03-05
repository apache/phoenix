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
package org.apache.phoenix.schema;

import org.apache.hadoop.hbase.HConstants;



public final class TableRef {
    private final PTable table;
    private final String alias;
    private final long timeStamp;
    private final boolean hasDynamicCols;

    public TableRef(TableRef tableRef, long timeStamp) {
        this(tableRef.alias, tableRef.table, timeStamp, tableRef.hasDynamicCols);
    }
    
    public TableRef(PTable table) {
        this(null, table, HConstants.LATEST_TIMESTAMP, false);
    }

    public TableRef(String alias, PTable table, long timeStamp, boolean hasDynamicCols) {
        this.alias = alias;
        this.table = table;
        this.timeStamp = timeStamp;
        this.hasDynamicCols = hasDynamicCols;
    }
    
    public PTable getTable() {
        return table;
    }

    public String getTableAlias() {
        return alias;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = alias == null ? 0 : alias.hashCode();
        result = prime * result + this.table.getName().getString().hashCode();
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        TableRef other = (TableRef)obj;
        if ((alias == null && other.alias != null) || (alias != null && !alias.equals(other.alias))) return false;
        if (!table.getName().getString().equals(other.table.getName().getString())) return false;
        return true;
    }

    public long getTimeStamp() {
        return timeStamp;
    }

    public boolean hasDynamicCols() {
        return hasDynamicCols;
    }

}
