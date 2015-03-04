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

import org.apache.http.annotation.Immutable;
import org.apache.phoenix.expression.ColumnExpression;
import org.apache.phoenix.expression.KeyValueColumnExpression;
import org.apache.phoenix.expression.ProjectedColumnExpression;
import org.apache.phoenix.expression.RowKeyColumnExpression;
import org.apache.phoenix.util.SchemaUtil;


/**
 * 
 * Class that represents a reference to a PColumn in a PTable
 *
 * 
 * @since 0.1
 */
@Immutable
public class ColumnRef {
    private final TableRef tableRef;
    private final int columnPosition;
    private final int pkSlotPosition;
    
    protected ColumnRef(ColumnRef columnRef, long timeStamp) {
        this.tableRef = new TableRef(columnRef.tableRef, timeStamp);
        this.columnPosition = columnRef.columnPosition;
        this.pkSlotPosition = columnRef.pkSlotPosition;
    }

    public ColumnRef(TableRef tableRef, String familyName, String columnName) throws MetaDataEntityNotFoundException {
        this(tableRef, tableRef.getTable().getColumnFamily(familyName).getColumn(columnName).getPosition());
    }

    public ColumnRef(TableRef tableRef, int columnPosition) {
        if (tableRef == null) {
            throw new NullPointerException();
        }
        if (columnPosition < 0 || columnPosition >= tableRef.getTable().getColumns().size()) {
            throw new IllegalArgumentException("Column position of " + columnPosition + " must be between 0 and " + tableRef.getTable().getColumns().size() + " for table " + tableRef.getTable().getName().getString());
        }
        this.tableRef = tableRef;
        this.columnPosition = columnPosition;
        PColumn column = getColumn();
        int i = -1;
        if (SchemaUtil.isPKColumn(column)) {
            for (PColumn pkColumn : tableRef.getTable().getPKColumns()) {
                i++;
                if (pkColumn == column) {
                    break;
                }
            }
        }
        pkSlotPosition = i;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + columnPosition;
        result = prime * result + tableRef.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        ColumnRef other = (ColumnRef)obj;
        if (columnPosition != other.columnPosition) return false;
        if (!tableRef.equals(other.tableRef)) return false;
        return true;
    }
    
    public ColumnExpression newColumnExpression() {
        return newColumnExpression(false, false);
    }

    public ColumnExpression newColumnExpression(boolean schemaNameCaseSensitive, boolean colNameCaseSensitive) {
        PTable table = tableRef.getTable();
        PColumn column = this.getColumn();
        String displayName = tableRef.getColumnDisplayName(this, schemaNameCaseSensitive, colNameCaseSensitive);
        if (SchemaUtil.isPKColumn(column)) {
            return new RowKeyColumnExpression(
                    column, 
                    new RowKeyValueAccessor(table.getPKColumns(), pkSlotPosition),
                    displayName);
        }
        
        if (table.getType() == PTableType.PROJECTED || table.getType() == PTableType.SUBQUERY) {
        	return new ProjectedColumnExpression(column, table, displayName);
        }
       
        return new KeyValueColumnExpression(column, displayName);
    }

    public ColumnRef cloneAtTimestamp(long timestamp) {
        return new ColumnRef(this, timestamp);
    }

    public int getColumnPosition() {
        return columnPosition;
    }
    
    public int getPKSlotPosition() {
        return pkSlotPosition;
    }
    
    public PColumn getColumn() {
        return tableRef.getTable().getColumns().get(columnPosition);
    }

    public PTable getTable() {
        return tableRef.getTable();
    }
    
    public TableRef getTableRef() {
        return tableRef;
    }
}
