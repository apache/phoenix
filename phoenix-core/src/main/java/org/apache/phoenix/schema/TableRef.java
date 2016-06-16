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

import java.util.Objects;

import org.apache.phoenix.compile.TupleProjectionCompiler;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PTable.IndexType;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.SchemaUtil;



public class TableRef {
    public static final TableRef EMPTY_TABLE_REF = new TableRef(new PTableImpl());
    
    private PTable table;
    private long upperBoundTimeStamp;
    private final String alias;
    private final long lowerBoundTimeStamp;
    private final boolean hasDynamicCols;

    public TableRef(TableRef tableRef) {
        this(tableRef.alias, tableRef.table, tableRef.upperBoundTimeStamp, tableRef.lowerBoundTimeStamp, tableRef.hasDynamicCols);
    }
    
    public TableRef(TableRef tableRef, long timeStamp) {
        this(tableRef.alias, tableRef.table, timeStamp, tableRef.lowerBoundTimeStamp, tableRef.hasDynamicCols);
    }
    
    public TableRef(TableRef tableRef, String alias) {
        this(alias, tableRef.table, tableRef.upperBoundTimeStamp, tableRef.lowerBoundTimeStamp, tableRef.hasDynamicCols);
    }
    
    public TableRef(PTable table) {
        this(null, table, QueryConstants.UNSET_TIMESTAMP, false);
    }
    
    public TableRef(PTable table, long upperBoundTimeStamp, long lowerBoundTimeStamp) {
        this(null, table, upperBoundTimeStamp, lowerBoundTimeStamp, false);
    }

    public TableRef(String alias, PTable table, long upperBoundTimeStamp, boolean hasDynamicCols) {
        this(alias, table, upperBoundTimeStamp, 0, hasDynamicCols);
    }
    
    public TableRef(String alias, PTable table, long upperBoundTimeStamp, long lowerBoundTimeStamp, 
        boolean hasDynamicCols) {
        this.alias = alias;
        this.table = table;
        this.upperBoundTimeStamp = upperBoundTimeStamp;
        this.lowerBoundTimeStamp = lowerBoundTimeStamp;
        this.hasDynamicCols = hasDynamicCols;
    }
    
    public PTable getTable() {
        return table;
    }
    
    public void setTable(PTable value) {
        this.table = value;
    }

    public void setTimeStamp(long timeStamp) {
        this.upperBoundTimeStamp = timeStamp;
    }

    public String getTableAlias() {
        return alias;
    }

    public String getColumnDisplayName(ColumnRef ref, boolean cfCaseSensitive, boolean cqCaseSensitive) {
        String cf = null;
        String cq = null;       
        PColumn column = ref.getColumn();
        String name = column.getName().getString();
        boolean isIndex = IndexUtil.isIndexColumn(name);
        if ((table.getType() == PTableType.PROJECTED && TupleProjectionCompiler.PROJECTED_TABLE_SCHEMA.equals(table.getSchemaName()))
                || table.getType() == PTableType.SUBQUERY) {
            cq = name;
        }
        else if (SchemaUtil.isPKColumn(column)) {
            cq = isIndex ? IndexUtil.getDataColumnName(name) : name;
        }
        else {
            String defaultFamilyName = table.getDefaultFamilyName() == null ? QueryConstants.DEFAULT_COLUMN_FAMILY : table.getDefaultFamilyName().getString();
            // Translate to the data table column name
            String dataFamilyName = isIndex ? IndexUtil.getDataColumnFamilyName(name) : column.getFamilyName().getString() ;
            cf = (table.getIndexType()==IndexType.LOCAL? IndexUtil.getActualColumnFamilyName(defaultFamilyName):defaultFamilyName).equals(dataFamilyName) ? null : dataFamilyName;
            cq = isIndex ? IndexUtil.getDataColumnName(name) : name;
        }
        
        cf = (cf!=null && cfCaseSensitive) ? "\"" + cf + "\"" : cf;
        cq = cqCaseSensitive ? "\"" + cq + "\"" : cq;
        return SchemaUtil.getColumnDisplayName(cf, cq);
    }
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = alias == null ? 0 : alias.hashCode();
        result = prime * result + ( this.table.getName()!=null ? this.table.getName().hashCode() : 0);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        TableRef other = (TableRef)obj;
        if (!Objects.equals(alias, other.alias)) return false;
        if (!Objects.equals(table.getName(), other.table.getName())) return false;
        return true;
    }

    public long getTimeStamp() {
        return this.upperBoundTimeStamp;
    }
    
    public long getLowerBoundTimeStamp() {
        return this.lowerBoundTimeStamp;
    }

    public boolean hasDynamicCols() {
        return hasDynamicCols;
    }

}
