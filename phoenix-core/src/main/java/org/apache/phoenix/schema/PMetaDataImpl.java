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

import java.sql.SQLException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class PMetaDataImpl implements PMetaData {
    public static final PMetaData EMPTY_META_DATA = new PMetaDataImpl(Collections.<String,PTable>emptyMap());
    private final Map<String,PTable> metaData;
    
    public PMetaDataImpl(Map<String,PTable> tables) {
        this.metaData = ImmutableMap.copyOf(tables);
    }
    
    @Override
    public PTable getTable(String name) throws TableNotFoundException {
        PTable table = metaData.get(name);
        if (table == null) {
            throw new TableNotFoundException(name);
        }
        return table;
    }

    @Override
    public Map<String,PTable> getTables() {
        return metaData;
    }


    @Override
    public PMetaData addTable(PTable table) throws SQLException {
        Map<String,PTable> tables = Maps.newHashMap(metaData);
        PTable oldTable = tables.put(table.getName().getString(), table);
        if (table.getParentName() != null) { // Upsert new index table into parent data table list
            String parentName = table.getParentName().getString();
            PTable parentTable = tables.get(parentName);
            // If parentTable isn't cached, that's ok we can skip this
            if (parentTable != null) {
                List<PTable> oldIndexes = parentTable.getIndexes();
                List<PTable> newIndexes = Lists.newArrayListWithExpectedSize(oldIndexes.size() + 1);
                newIndexes.addAll(oldIndexes);
                if (oldTable != null) {
                    newIndexes.remove(oldTable);
                }
                newIndexes.add(table);
                tables.put(parentName, PTableImpl.makePTable(parentTable, table.getTimeStamp(), newIndexes));
            }
        }
        for (PTable index : table.getIndexes()) {
            tables.put(index.getName().getString(), index);
        }
        return new PMetaDataImpl(tables);
    }

    @Override
    public PMetaData addColumn(String tableName, List<PColumn> columnsToAdd, long tableTimeStamp, long tableSeqNum, boolean isImmutableRows) throws SQLException {
        PTable table = getTable(tableName);
        Map<String,PTable> tables = Maps.newHashMap(metaData);
        List<PColumn> oldColumns = PTableImpl.getColumnsToClone(table);
        List<PColumn> newColumns;
        if (columnsToAdd.isEmpty()) {
            newColumns = oldColumns;
        } else {
            newColumns = Lists.newArrayListWithExpectedSize(oldColumns.size() + columnsToAdd.size());
            newColumns.addAll(oldColumns);
            newColumns.addAll(columnsToAdd);
        }
        PTable newTable = PTableImpl.makePTable(table, tableTimeStamp, tableSeqNum, newColumns, isImmutableRows);
        tables.put(tableName, newTable);
        return new PMetaDataImpl(tables);
    }

    @Override
    public PMetaData removeTable(String tableName) throws SQLException {
        PTable table;
        Map<String,PTable> tables = Maps.newHashMap(metaData);
        if ((table=tables.remove(tableName)) == null) {
            throw new TableNotFoundException(tableName);
        } else {
            for (PTable index : table.getIndexes()) {
                if (tables.remove(index.getName().getString()) == null) {
                    throw new TableNotFoundException(index.getName().getString());
                }
            }
        }
        return new PMetaDataImpl(tables);
    }
    
    @Override
    public PMetaData removeColumn(String tableName, String familyName, String columnName, long tableTimeStamp, long tableSeqNum) throws SQLException {
        PTable table = getTable(tableName);
        Map<String,PTable> tables = Maps.newHashMap(metaData);
        PColumn column;
        if (familyName == null) {
            column = table.getPKColumn(columnName);
        } else {
            column = table.getColumnFamily(familyName).getColumn(columnName);
        }
        int positionOffset = 0;
        int position = column.getPosition();
        List<PColumn> oldColumns = table.getColumns();
        if (table.getBucketNum() != null) {
            position--;
            positionOffset = 1;
            oldColumns = oldColumns.subList(positionOffset, oldColumns.size());
        }
        List<PColumn> columns = Lists.newArrayListWithExpectedSize(oldColumns.size() - 1);
        columns.addAll(oldColumns.subList(0, position));
        // Update position of columns that follow removed column
        for (int i = position+1; i < oldColumns.size(); i++) {
            PColumn oldColumn = oldColumns.get(i);
            PColumn newColumn = new PColumnImpl(oldColumn.getName(), oldColumn.getFamilyName(), oldColumn.getDataType(), oldColumn.getMaxLength(), oldColumn.getScale(), oldColumn.isNullable(), i-1+positionOffset, oldColumn.getSortOrder(), oldColumn.getArraySize(), oldColumn.getViewConstant());
            columns.add(newColumn);
        }
        
        PTable newTable = PTableImpl.makePTable(table, tableTimeStamp, tableSeqNum, columns);
        tables.put(tableName, newTable);
        return new PMetaDataImpl(tables);
    }

    public static PMetaData pruneNewerTables(long scn, PMetaData metaData) {
        if (!hasNewerMetaData(scn, metaData)) {
            return metaData;
        }
        Map<String,PTable> newTables = Maps.newHashMap(metaData.getTables());
        Iterator<Map.Entry<String, PTable>> tableIterator = newTables.entrySet().iterator();
        boolean wasModified = false;
        while (tableIterator.hasNext()) {
            PTable table = tableIterator.next().getValue();
            if (table.getTimeStamp() >= scn && table.getType() != PTableType.SYSTEM) {
                tableIterator.remove();
                wasModified = true;
            }
        }
    
        if (wasModified) {
            return new PMetaDataImpl(newTables);
        }
        return metaData;
    }

    private static boolean hasNewerMetaData(long scn, PMetaData metaData) {
        for (PTable table : metaData.getTables().values()) {
            if (table.getTimeStamp() >= scn) {
                return true;
            }
        }
        return false;
    }
    
    private static boolean hasMultiTenantMetaData(PMetaData metaData) {
        for (PTable table : metaData.getTables().values()) {
            if (table.isMultiTenant()) {
                return true;
            }
        }
        return false;
    }
    
    public static PMetaData pruneMultiTenant(PMetaData metaData) {
        if (!hasMultiTenantMetaData(metaData)) {
            return metaData;
        }
        Map<String,PTable> newTables = Maps.newHashMap(metaData.getTables());
        Iterator<Map.Entry<String, PTable>> tableIterator = newTables.entrySet().iterator();
        while (tableIterator.hasNext()) {
            PTable table = tableIterator.next().getValue();
            if (table.isMultiTenant()) {
                tableIterator.remove();
            }
        }
    
        return new PMetaDataImpl(newTables);
    }
}
