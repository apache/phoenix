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
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.HConstants;
import org.apache.phoenix.parse.PFunction;
import org.apache.phoenix.parse.PSchema;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TimeKeeper;

import com.google.common.collect.Lists;

/**
 * Client-side cache of MetaData, not thread safe. Internally uses a LinkedHashMap that evicts the
 * oldest entries when size grows beyond the maxSize specified at create time.
 */
public class PMetaDataImpl implements PMetaData {
    
    private PMetaDataCache metaData;
    private final TimeKeeper timeKeeper;
    private final PTableRefFactory tableRefFactory;
    
    public PMetaDataImpl(int initialCapacity, ReadOnlyProps props) {
        this(initialCapacity, TimeKeeper.SYSTEM, props);
    }

    public PMetaDataImpl(int initialCapacity, TimeKeeper timeKeeper, ReadOnlyProps props) {
        this(new PMetaDataCache(initialCapacity, props.getLong(
            QueryServices.MAX_CLIENT_METADATA_CACHE_SIZE_ATTRIB,
            QueryServicesOptions.DEFAULT_MAX_CLIENT_METADATA_CACHE_SIZE), timeKeeper,
                PTableRefFactory.getFactory(props)), timeKeeper, PTableRefFactory.getFactory(props));
    }

    private PMetaDataImpl(PMetaDataCache metaData, TimeKeeper timeKeeper, PTableRefFactory tableRefFactory) {
        this.timeKeeper = timeKeeper;
        this.metaData = metaData;
        this.tableRefFactory = tableRefFactory;
    }

    @Override
    public PMetaDataImpl clone() {
        return new PMetaDataImpl(new PMetaDataCache(this.metaData), this.timeKeeper, this.tableRefFactory);
    }
    
    @Override
    public PTableRef getTableRef(PTableKey key) throws TableNotFoundException {
        PTableRef ref = metaData.get(key);
        if (ref == null) {
            throw new TableNotFoundException(key.getName());
        }
        return ref;
    }

    @Override
    public PFunction getFunction(PTableKey key) throws FunctionNotFoundException {
        PFunction function = metaData.functions.get(key);
        if (function == null) {
            throw new FunctionNotFoundException(key.getName());
        }
        return function;
    }

    @Override
    public int size() {
        return metaData.size();
    }

    @Override
    public void updateResolvedTimestamp(PTable table, long resolvedTimestamp) throws SQLException {
    	metaData.put(table.getKey(), tableRefFactory.makePTableRef(table, this.timeKeeper.getCurrentTime(), resolvedTimestamp));
    }

    @Override
    public void addTable(PTable table, long resolvedTime) throws SQLException {
        PTableRef tableRef = tableRefFactory.makePTableRef(table, this.timeKeeper.getCurrentTime(), resolvedTime);
        int netGain = 0;
        PTableKey key = table.getKey();
        PTableRef oldTableRef = metaData.get(key);
        if (oldTableRef != null) {
            netGain -= oldTableRef.getEstimatedSize();
        }
        PTable newParentTable = null;
        PTableRef newParentTableRef = null;
        long parentResolvedTimestamp = resolvedTime;
        if (table.getType() == PTableType.INDEX) { // Upsert new index table into parent data table list
            String parentName = table.getParentName().getString();
            PTableRef oldParentRef = metaData.get(new PTableKey(table.getTenantId(), parentName));
            // If parentTable isn't cached, that's ok we can skip this
            if (oldParentRef != null) {
                List<PTable> oldIndexes = oldParentRef.getTable().getIndexes();
                List<PTable> newIndexes = Lists.newArrayListWithExpectedSize(oldIndexes.size() + 1);
                newIndexes.addAll(oldIndexes);
                for (int i = 0; i < newIndexes.size(); i++) {
                    PTable index = newIndexes.get(i);
                    if (index.getName().equals(table.getName())) {
                        newIndexes.remove(i);
                        break;
                    }
                }
                newIndexes.add(table);
                netGain -= oldParentRef.getEstimatedSize();
                newParentTable = PTableImpl.makePTable(oldParentRef.getTable(), table.getTimeStamp(), newIndexes);
                newParentTableRef = tableRefFactory.makePTableRef(newParentTable, this.timeKeeper.getCurrentTime(), parentResolvedTimestamp);
                netGain += newParentTableRef.getEstimatedSize();
            }
        }
        if (newParentTable == null) { // Don't count in gain if we found a parent table, as its accounted for in newParentTable
            netGain += tableRef.getEstimatedSize();
        }
        long overage = metaData.getCurrentSize() + netGain - metaData.getMaxSize();
        metaData = overage <= 0 ? metaData : metaData.cloneMinusOverage(overage);
        
        if (newParentTable != null) { // Upsert new index table into parent data table list
            metaData.put(newParentTable.getKey(), newParentTableRef);
            metaData.put(table.getKey(), tableRef);
        } else {
            metaData.put(table.getKey(), tableRef);
        }
        for (PTable index : table.getIndexes()) {
            metaData.put(index.getKey(), tableRefFactory.makePTableRef(index, this.timeKeeper.getCurrentTime(), resolvedTime));
        }
    }

    @Override
    public void removeTable(PName tenantId, String tableName, String parentTableName, long tableTimeStamp) throws SQLException {
        PTableRef parentTableRef = null;
        PTableKey key = new PTableKey(tenantId, tableName);
        if (metaData.get(key) == null) {
            if (parentTableName != null) {
                parentTableRef = metaData.get(new PTableKey(tenantId, parentTableName));
            }
            if (parentTableRef == null) {
                return;
            }
        } else {
            PTable table = metaData.remove(key);
            for (PTable index : table.getIndexes()) {
                metaData.remove(index.getKey());
            }
            if (table.getParentName() != null) {
                parentTableRef = metaData.get(new PTableKey(tenantId, table.getParentName().getString()));
            }
        }
        // also remove its reference from parent table
        if (parentTableRef != null) {
            List<PTable> oldIndexes = parentTableRef.getTable().getIndexes();
            if(oldIndexes != null && !oldIndexes.isEmpty()) {
                List<PTable> newIndexes = Lists.newArrayListWithExpectedSize(oldIndexes.size());
                newIndexes.addAll(oldIndexes);
                for (int i = 0; i < newIndexes.size(); i++) {
                    PTable index = newIndexes.get(i);
                    if (index.getName().getString().equals(tableName)) {
                        newIndexes.remove(i);
                        PTable parentTable = PTableImpl.makePTable(
                                parentTableRef.getTable(),
                                tableTimeStamp == HConstants.LATEST_TIMESTAMP ? parentTableRef.getTable().getTimeStamp() : tableTimeStamp,
                                newIndexes);
                        metaData.put(parentTable.getKey(), tableRefFactory.makePTableRef(parentTable, this.timeKeeper.getCurrentTime(), parentTableRef.getResolvedTimeStamp()));
                        break;
                    }
                }
            }
        }
    }
    
    @Override
    public void removeColumn(PName tenantId, String tableName, List<PColumn> columnsToRemove, long tableTimeStamp, long tableSeqNum, long resolvedTime) throws SQLException {
        PTableRef tableRef = metaData.get(new PTableKey(tenantId, tableName));
        if (tableRef == null) {
            return;
        }
        PTable table = tableRef.getTable();
        PMetaDataCache tables = metaData;
        for (PColumn columnToRemove : columnsToRemove) {
            PColumn column;
            String familyName = columnToRemove.getFamilyName().getString();
            if (familyName == null) {
                column = table.getPKColumn(columnToRemove.getName().getString());
            } else {
                column = table.getColumnFamily(familyName).getPColumnForColumnName(columnToRemove.getName().getString());
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
                PColumn newColumn = new PColumnImpl(oldColumn.getName(), oldColumn.getFamilyName(), oldColumn.getDataType(), oldColumn.getMaxLength(), oldColumn.getScale(), oldColumn.isNullable(), i-1+positionOffset, oldColumn.getSortOrder(), oldColumn.getArraySize(), oldColumn.getViewConstant(), oldColumn.isViewReferenced(), oldColumn.getExpressionStr(), oldColumn.isRowTimestamp(), oldColumn.isDynamic(), oldColumn.getColumnQualifierBytes());
                columns.add(newColumn);
            }
            
            table = PTableImpl.makePTable(table, tableTimeStamp, tableSeqNum, columns);
        }
        tables.put(table.getKey(), tableRefFactory.makePTableRef(table, this.timeKeeper.getCurrentTime(), resolvedTime));
    }

    @Override
    public void pruneTables(Pruner pruner) {
        List<PTableKey> keysToPrune = Lists.newArrayListWithExpectedSize(this.size());
        for (PTable table : this) {
            if (pruner.prune(table)) {
                keysToPrune.add(table.getKey());
            }
        }
        if (!keysToPrune.isEmpty()) {
            for (PTableKey key : keysToPrune) {
                metaData.remove(key);
            }
        }
    }

    @Override
    public Iterator<PTable> iterator() {
        return metaData.iterator();
    }

    @Override
    public void addFunction(PFunction function) throws SQLException {
        this.metaData.functions.put(function.getKey(), function);
    }

    @Override
    public void removeFunction(PName tenantId, String function, long functionTimeStamp)
            throws SQLException {
        this.metaData.functions.remove(new PTableKey(tenantId, function));
    }

    @Override
    public void pruneFunctions(Pruner pruner) {
        List<PTableKey> keysToPrune = Lists.newArrayListWithExpectedSize(this.size());
        for (PFunction function : this.metaData.functions.values()) {
            if (pruner.prune(function)) {
                keysToPrune.add(function.getKey());
            }
        }
        if (!keysToPrune.isEmpty()) {
            for (PTableKey key : keysToPrune) {
                metaData.functions.remove(key);
            }
        }
    }

    @Override
    public long getAge(PTableRef ref) {
        return this.metaData.getAge(ref);
    }

    @Override
    public void addSchema(PSchema schema) throws SQLException {
        this.metaData.schemas.put(schema.getSchemaKey(), schema);
    }

    @Override
    public PSchema getSchema(PTableKey key) throws SchemaNotFoundException {
        PSchema schema = metaData.schemas.get(key);
        if (schema == null) { throw new SchemaNotFoundException(key.getName()); }
        return schema;
    }

    @Override
    public void removeSchema(PSchema schema, long schemaTimeStamp) {
        this.metaData.schemas.remove(SchemaUtil.getSchemaKey(schema.getSchemaName()));
    }

}
