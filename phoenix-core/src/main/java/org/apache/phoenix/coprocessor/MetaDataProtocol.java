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
package org.apache.phoenix.coprocessor;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import com.google.common.collect.Lists;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableImpl;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.MetaDataUtil;


/**
 * 
 * Coprocessor protocol for Phoenix DDL. Phoenix stores the table metadata in
 * an HBase table named SYSTEM.TABLE. Each table is represented by:
 * - one row for the table
 * - one row per column in the tabe
 * Upto {@link #DEFAULT_MAX_META_DATA_VERSIONS} versions are kept. The time
 * stamp of the metadata must always be increasing. The timestamp of the key
 * values in the data row corresponds to the schema that it's using.
 *
 * TODO: dynamically prune number of schema version kept based on whether or
 * not the data table still uses it (based on the min time stamp of the data
 * table).
 * 
 * 
 * @since 0.1
 */
public interface MetaDataProtocol extends CoprocessorProtocol {
    public static final int PHOENIX_MAJOR_VERSION = 3;
    public static final int PHOENIX_MINOR_VERSION = 0;
    public static final int PHOENIX_PATCH_NUMBER = 0;
    public static final int PHOENIX_VERSION = 
            MetaDataUtil.encodeVersion(PHOENIX_MAJOR_VERSION, PHOENIX_MINOR_VERSION, PHOENIX_PATCH_NUMBER);
    
    public static final long MIN_TABLE_TIMESTAMP = 0;
    // Increase MIN_SYSTEM_TABLE_TIMESTAMP by one for each schema change SYSTEM.TABLE schema changes.
    // For 1.0,1.1,1.2,and 1.2.1 we used MetaDataProtocol.MIN_TABLE_TIMESTAMP+1
    // For 2.0 and above, we use MetaDataProtocol.MIN_TABLE_TIMESTAMP+7 so that we can add the five new
    // columns to the existing system table (three new columns in 1.2.1 and two new columns in 1.2)
    // For 3.0 and above, we use MIN_TABLE_TIMESTAMP + 8 so that we can add the tenant_id column
    // as the first column to the existing system table.
    // For 3.1 (SNAPSHOT) and above, we use MIN_TABLE_TIMESTAMP + 9 so that we can add
    // the multi_tenant and multi_type columns for multi tenancy
    public static final long MIN_SYSTEM_TABLE_TIMESTAMP = MIN_TABLE_TIMESTAMP + 9;
    public static final int DEFAULT_MAX_META_DATA_VERSIONS = 1000;

    // TODO: pare this down to minimum, as we don't need duplicates for both table and column errors, nor should we need
    // a different code for every type of error.
    // ENTITY_ALREADY_EXISTS, ENTITY_NOT_FOUND, NEWER_ENTITY_FOUND, ENTITY_NOT_IN_REGION, CONCURRENT_MODIFICATION
    // ILLEGAL_MUTATION (+ sql code)
    public enum MutationCode {
        TABLE_ALREADY_EXISTS,
        TABLE_NOT_FOUND, 
        COLUMN_NOT_FOUND, 
        COLUMN_ALREADY_EXISTS,
        CONCURRENT_TABLE_MUTATION,
        TABLE_NOT_IN_REGION,
        NEWER_TABLE_FOUND,
        UNALLOWED_TABLE_MUTATION,
        NO_PK_COLUMNS,
        PARENT_TABLE_NOT_FOUND
    };
    
    public static class MetaDataMutationResult implements Writable {
        private MutationCode returnCode;
        private long mutationTime;
        private PTable table;
        private List<byte[]> tableNamesToDelete;
        private byte[] columnName;
        private byte[] familyName;
        private boolean wasUpdated;
        
        public MetaDataMutationResult() {
        }

        public MetaDataMutationResult(MutationCode returnCode, long currentTime, PTable table, PColumn column) {
            this(returnCode, currentTime, table);
            if(column != null){
                this.columnName = column.getName().getBytes();
                this.familyName = column.getFamilyName().getBytes();    
            }
            
        }
        
        public MetaDataMutationResult(MutationCode returnCode, long currentTime, PTable table) {
           this(returnCode, currentTime, table, Collections.<byte[]> emptyList());
        }
        
        public MetaDataMutationResult(MutationCode returnCode, long currentTime, PTable table, List<byte[]> tableNamesToDelete) {
            this.returnCode = returnCode;
            this.mutationTime = currentTime;
            this.table = table;
            this.tableNamesToDelete = tableNamesToDelete;
        }
        
        public MutationCode getMutationCode() {
            return returnCode;
        }
        
        public long getMutationTime() {
            return mutationTime;
        }
        
        public boolean wasUpdated() {
            return wasUpdated;
        }
        
        public PTable getTable() {
            return table;
        }
 
        public void setTable(PTable table) {
            this.table = table;
        }
 
        public List<byte[]> getTableNamesToDelete() {
            return tableNamesToDelete;
        }
        
        public byte[] getColumnName() {
            return columnName;
        }
        
        public byte[] getFamilyName() {
            return familyName;
        }        
        
        @Override
        public void readFields(DataInput input) throws IOException {
            this.returnCode = MutationCode.values()[WritableUtils.readVInt(input)];
            this.mutationTime = input.readLong();
            wasUpdated = input.readBoolean();
            if (wasUpdated) {
                this.table = new PTableImpl();
                this.table.readFields(input);
            }
            columnName = Bytes.readByteArray(input);
            if (columnName.length > 0) {
                familyName = Bytes.readByteArray(input);
            }
            boolean hasTablesToDelete = input.readBoolean();
            if (hasTablesToDelete) {
                int count = input.readInt();
                tableNamesToDelete = Lists.newArrayListWithExpectedSize(count);
                for( int i = 0 ; i < count ; i++ ){
                     byte[] tableName = Bytes.readByteArray(input);
                     tableNamesToDelete.add(tableName);
                }
            }
        }

        @Override
        public void write(DataOutput output) throws IOException {
            WritableUtils.writeVInt(output, returnCode.ordinal());
            output.writeLong(mutationTime);
            output.writeBoolean(table != null);
            if (table != null) {
                table.write(output);
            }
            Bytes.writeByteArray(output, columnName == null ? ByteUtil.EMPTY_BYTE_ARRAY : columnName);
            if (columnName != null) {
                 Bytes.writeByteArray(output, familyName == null ? ByteUtil.EMPTY_BYTE_ARRAY : familyName);
            }
            if(tableNamesToDelete != null && tableNamesToDelete.size() > 0 ) {
                output.writeBoolean(true);
                output.writeInt(tableNamesToDelete.size());
                for(byte[] tableName : tableNamesToDelete) {
                    Bytes.writeByteArray(output,tableName);    
                }
                
            } else {
                output.writeBoolean(false);
            }
            
        }
    }
    
    /**
     * The the latest Phoenix table at or before the given clientTimestamp. If the
     * client already has the latest (based on the tableTimestamp), then no table
     * is returned.
     * @param tenantId
     * @param schemaName
     * @param tableName
     * @param tableTimestamp
     * @param clientTimestamp
     * @return MetaDataMutationResult
     * @throws IOException
     */
    MetaDataMutationResult getTable(byte[] tenantId, byte[] schemaName, byte[] tableName, long tableTimestamp, long clientTimestamp) throws IOException;

    /**
     * Create a new Phoenix table
     * @param tableMetadata
     * @return MetaDataMutationResult
     * @throws IOException
     */
    MetaDataMutationResult createTable(List<Mutation> tableMetadata) throws IOException;

    /**
     * Drop an existing Phoenix table
     * @param tableMetadata
     * @param tableType
     * @return MetaDataMutationResult
     * @throws IOException
     */
    MetaDataMutationResult dropTable(List<Mutation> tableMetadata, String tableType) throws IOException;

    /**
     * Add a column to an existing Phoenix table
     * @param tableMetadata
     * @return MetaDataMutationResult
     * @throws IOException
     */
    MetaDataMutationResult addColumn(List<Mutation> tableMetadata) throws IOException;
    
    /**
     * Drop a column from an existing Phoenix table
     * @param tableMetadata
     * @return MetaDataMutationResult
     * @throws IOException
     */
    MetaDataMutationResult dropColumn(List<Mutation> tableMetadata) throws IOException;
    
    MetaDataMutationResult updateIndexState(List<Mutation> tableMetadata) throws IOException;

    /**
     * Clears the server-side cache of table meta data. Used between test runs to
     * ensure no side effects.
     */
    void clearCache();
    
    /**
     * Get the version of the server-side HBase and phoenix.jar. Used when initially connecting
     * to a cluster to ensure that the client and server jars are compatible.
     */
    long getVersion();
}
