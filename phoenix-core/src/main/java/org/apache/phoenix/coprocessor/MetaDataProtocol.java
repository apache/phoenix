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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.apache.hadoop.hbase.util.ByteStringer;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos.MetaDataResponse;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos.MetaDataService;
import org.apache.phoenix.coprocessor.generated.PFunctionProtos;
import org.apache.phoenix.hbase.index.util.VersionUtil;
import org.apache.phoenix.parse.PFunction;
import org.apache.phoenix.parse.PSchema;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PColumnImpl;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PNameFactory;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableImpl;
import org.apache.phoenix.util.ByteUtil;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;

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
public abstract class MetaDataProtocol extends MetaDataService {
    public static final int PHOENIX_MAJOR_VERSION = 4;
    public static final int PHOENIX_MINOR_VERSION = 10;
    public static final int PHOENIX_PATCH_NUMBER = 0;
    public static final int PHOENIX_VERSION =
            VersionUtil.encodeVersion(PHOENIX_MAJOR_VERSION, PHOENIX_MINOR_VERSION, PHOENIX_PATCH_NUMBER);

    public static final long MIN_TABLE_TIMESTAMP = 0;

    public static final int DEFAULT_MAX_META_DATA_VERSIONS = 1000;
    public static final boolean DEFAULT_META_DATA_KEEP_DELETED_CELLS = true;
    public static final int DEFAULT_MAX_STAT_DATA_VERSIONS = 1;
    public static final boolean DEFAULT_STATS_KEEP_DELETED_CELLS = false;
    
    // Min system table timestamps for every release.
    public static final long MIN_SYSTEM_TABLE_TIMESTAMP_4_1_0 = MIN_TABLE_TIMESTAMP + 3;
    public static final long MIN_SYSTEM_TABLE_TIMESTAMP_4_2_0 = MIN_TABLE_TIMESTAMP + 4;
    public static final long MIN_SYSTEM_TABLE_TIMESTAMP_4_2_1 = MIN_TABLE_TIMESTAMP + 5;
    public static final long MIN_SYSTEM_TABLE_TIMESTAMP_4_3_0 = MIN_TABLE_TIMESTAMP + 7;
    public static final long MIN_SYSTEM_TABLE_TIMESTAMP_4_5_0 = MIN_TABLE_TIMESTAMP + 8;
    public static final long MIN_SYSTEM_TABLE_TIMESTAMP_4_6_0 = MIN_TABLE_TIMESTAMP + 9;
    public static final long MIN_SYSTEM_TABLE_TIMESTAMP_4_7_0 = MIN_TABLE_TIMESTAMP + 15;
    public static final long MIN_SYSTEM_TABLE_TIMESTAMP_4_8_0 = MIN_TABLE_TIMESTAMP + 18;
    public static final long MIN_SYSTEM_TABLE_TIMESTAMP_4_8_1 = MIN_TABLE_TIMESTAMP + 18;
    public static final long MIN_SYSTEM_TABLE_TIMESTAMP_4_9_0 = MIN_TABLE_TIMESTAMP + 20;
    public static final long MIN_SYSTEM_TABLE_TIMESTAMP_4_10_0 = MIN_TABLE_TIMESTAMP + 25;
    // MIN_SYSTEM_TABLE_TIMESTAMP needs to be set to the max of all the MIN_SYSTEM_TABLE_TIMESTAMP_* constants
    public static final long MIN_SYSTEM_TABLE_TIMESTAMP = MIN_SYSTEM_TABLE_TIMESTAMP_4_10_0;
    
    // ALWAYS update this map whenever rolling out a new release (major, minor or patch release). 
    // Key is the SYSTEM.CATALOG timestamp for the version and value is the version string.
    private static final NavigableMap<Long, String> TIMESTAMP_VERSION_MAP = new TreeMap<>();
    static {
        TIMESTAMP_VERSION_MAP.put(MIN_SYSTEM_TABLE_TIMESTAMP_4_1_0, "4.1.x");
        TIMESTAMP_VERSION_MAP.put(MIN_SYSTEM_TABLE_TIMESTAMP_4_2_0, "4.2.0");
        TIMESTAMP_VERSION_MAP.put(MIN_SYSTEM_TABLE_TIMESTAMP_4_2_1, "4.2.1");
        TIMESTAMP_VERSION_MAP.put(MIN_SYSTEM_TABLE_TIMESTAMP_4_3_0, "4.3.x");
        TIMESTAMP_VERSION_MAP.put(MIN_SYSTEM_TABLE_TIMESTAMP_4_5_0, "4.5.x");
        TIMESTAMP_VERSION_MAP.put(MIN_SYSTEM_TABLE_TIMESTAMP_4_6_0, "4.6.x");
        TIMESTAMP_VERSION_MAP.put(MIN_SYSTEM_TABLE_TIMESTAMP_4_7_0, "4.7.x");
        TIMESTAMP_VERSION_MAP.put(MIN_SYSTEM_TABLE_TIMESTAMP_4_8_0, "4.8.x");
        TIMESTAMP_VERSION_MAP.put(MIN_SYSTEM_TABLE_TIMESTAMP_4_9_0, "4.9.x");
        TIMESTAMP_VERSION_MAP.put(MIN_SYSTEM_TABLE_TIMESTAMP_4_10_0, "4.10.x");
    }
    
    public static final String CURRENT_CLIENT_VERSION = PHOENIX_MAJOR_VERSION + "." + PHOENIX_MINOR_VERSION + "." + PHOENIX_PATCH_NUMBER; 
    
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
        PARENT_TABLE_NOT_FOUND,
        FUNCTION_ALREADY_EXISTS,
        FUNCTION_NOT_FOUND,
        NEWER_FUNCTION_FOUND,
        FUNCTION_NOT_IN_REGION,
        SCHEMA_ALREADY_EXISTS, 
        NEWER_SCHEMA_FOUND,
        SCHEMA_NOT_FOUND,
        SCHEMA_NOT_IN_REGION,
        TABLES_EXIST_ON_SCHEMA,
        UNALLOWED_SCHEMA_MUTATION,
        AUTO_PARTITION_SEQUENCE_NOT_FOUND,
        CANNOT_COERCE_AUTO_PARTITION_ID,
        TOO_MANY_INDEXES,
        NO_OP
    };

  public static class SharedTableState {
        private PName tenantId;
        private PName schemaName;
        private PName tableName;
        private List<PColumn> columns;
        private List<PName> physicalNames;
        private Short viewIndexId;
        
        public SharedTableState(PTable table) {
            this.tenantId = table.getTenantId();
            this.schemaName = table.getSchemaName();
            this.tableName = table.getTableName();
            this.columns = table.getColumns();
            this.physicalNames = table.getPhysicalNames();
            this.viewIndexId = table.getViewIndexId();
        }
        
        public SharedTableState(
                org.apache.phoenix.coprocessor.generated.MetaDataProtos.SharedTableState sharedTable) {
            this.tenantId = sharedTable.hasTenantId() ? PNameFactory.newName(sharedTable.getTenantId().toByteArray()) : null;
            this.schemaName = PNameFactory.newName(sharedTable.getSchemaName().toByteArray());
            this.tableName = PNameFactory.newName(sharedTable.getTableName().toByteArray());
            this.columns = Lists.transform(sharedTable.getColumnsList(),
                new Function<org.apache.phoenix.coprocessor.generated.PTableProtos.PColumn, PColumn>() {
                @Override
                public PColumn apply(org.apache.phoenix.coprocessor.generated.PTableProtos.PColumn column) {
                    return PColumnImpl.createFromProto(column);
                }
            });
            this.physicalNames = Lists.transform(sharedTable.getPhysicalNamesList(),
                new Function<ByteString, PName>() {
                @Override
                public PName apply(ByteString physicalName) {
                    return PNameFactory.newName(physicalName.toByteArray());
                }
            });
            this.viewIndexId = (short)sharedTable.getViewIndexId();
        }

        public PName getTenantId() {
            return tenantId;
        }

        public PName getSchemaName() {
            return schemaName;
        }

        public PName getTableName() {
            return tableName;
        }

        public List<PColumn> getColumns() {
            return columns;
        }

        public List<PName> getPhysicalNames() {
            return physicalNames;
        }

        public Short getViewIndexId() {
            return viewIndexId;
        }
        
  }
    
  public static class MetaDataMutationResult {
        private MutationCode returnCode;
        private long mutationTime;
        private PTable table;
        private List<byte[]> tableNamesToDelete;
        private List<SharedTableState> sharedTablesToDelete;
        private byte[] columnName;
        private byte[] familyName;
        private boolean wasUpdated;
        private PSchema schema;
        private Short viewIndexId;

        private List<PFunction> functions = new ArrayList<PFunction>(1);
        private long autoPartitionNum;

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

        public MetaDataMutationResult(MutationCode returnCode, long currentTime, List<PFunction> functions, boolean wasUpdated) {
            this.returnCode = returnCode;
            this.mutationTime = currentTime;
            this.functions = functions;
            this.wasUpdated = wasUpdated;
         }

        public MetaDataMutationResult(MutationCode returnCode, PSchema schema, long currentTime) {
            this.returnCode = returnCode;
            this.mutationTime = currentTime;
            this.schema = schema;
        }

        // For testing, so that connectionless can set wasUpdated so ColumnResolver doesn't complain
        public MetaDataMutationResult(MutationCode returnCode, long currentTime, PTable table, boolean wasUpdated) {
            this(returnCode, currentTime, table, Collections.<byte[]> emptyList());
            this.wasUpdated = wasUpdated;
         }
        
        public MetaDataMutationResult(MutationCode returnCode, long currentTime, PTable table, List<byte[]> tableNamesToDelete) {
            this.returnCode = returnCode;
            this.mutationTime = currentTime;
            this.table = table;
            this.tableNamesToDelete = tableNamesToDelete;
        }
        
        public MetaDataMutationResult(MutationCode returnCode, int currentTime, PTable table, int viewIndexId) {
            this(returnCode, currentTime, table, Collections.<byte[]> emptyList());
            this.viewIndexId = (short)viewIndexId;
        }
        
        public MetaDataMutationResult(MutationCode returnCode, long currentTime, PTable table, List<byte[]> tableNamesToDelete, List<SharedTableState> sharedTablesToDelete) {
            this(returnCode, currentTime, table, tableNamesToDelete);
            this.sharedTablesToDelete = sharedTablesToDelete;
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
        
        public void setFunction(PFunction function) {
            this.functions.add(function);
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

        public List<PFunction> getFunctions() {
            return functions;
        }
        
        public List<SharedTableState> getSharedTablesToDelete() {
            return sharedTablesToDelete;
        }

        public long getAutoPartitionNum() {
            return autoPartitionNum;
        }
        
        public Short getViewIndexId() {
            return viewIndexId;
        }

        public static MetaDataMutationResult constructFromProto(MetaDataResponse proto) {
          MetaDataMutationResult result = new MetaDataMutationResult();
          result.returnCode = MutationCode.values()[proto.getReturnCode().ordinal()];
          result.mutationTime = proto.getMutationTime();
          if (proto.hasTable()) {
            result.wasUpdated = true;
            result.table = PTableImpl.createFromProto(proto.getTable());
          }
          if (proto.getFunctionCount() > 0) {
              result.wasUpdated = true;
              for(PFunctionProtos.PFunction function: proto.getFunctionList())
              result.functions.add(PFunction.createFromProto(function));
          }
          if (proto.getTablesToDeleteCount() > 0) {
            result.tableNamesToDelete =
                Lists.newArrayListWithExpectedSize(proto.getTablesToDeleteCount());
            for (ByteString tableName : proto.getTablesToDeleteList()) {
              result.tableNamesToDelete.add(tableName.toByteArray());
            }
          }
          result.columnName = ByteUtil.EMPTY_BYTE_ARRAY;
          if(proto.hasColumnName()){
            result.columnName = proto.getColumnName().toByteArray();
          }
          if(proto.hasFamilyName()){
            result.familyName = proto.getFamilyName().toByteArray();
          }
          if(proto.getSharedTablesToDeleteCount() > 0) {
              result.sharedTablesToDelete = 
                 Lists.newArrayListWithExpectedSize(proto.getSharedTablesToDeleteCount());
              for (org.apache.phoenix.coprocessor.generated.MetaDataProtos.SharedTableState sharedTable : 
                  proto.getSharedTablesToDeleteList()) {
                result.sharedTablesToDelete.add(new SharedTableState(sharedTable));
                }
          }
          if (proto.hasSchema()) {
            result.schema = PSchema.createFromProto(proto.getSchema());
          }
          if (proto.hasAutoPartitionNum()) {
              result.autoPartitionNum = proto.getAutoPartitionNum();
          }
            if (proto.hasViewIndexId()) {
                result.viewIndexId = (short)proto.getViewIndexId();
            }
          return result;
        }

        public static MetaDataResponse toProto(MetaDataMutationResult result) {
          MetaDataProtos.MetaDataResponse.Builder builder =
              MetaDataProtos.MetaDataResponse.newBuilder();
          if (result != null) {
            builder.setReturnCode(MetaDataProtos.MutationCode.values()[result.getMutationCode()
                .ordinal()]);
            builder.setMutationTime(result.getMutationTime());
            if (result.table != null) {
              builder.setTable(PTableImpl.toProto(result.table));
            }
            if (result.getTableNamesToDelete() != null) {
              for (byte[] tableName : result.tableNamesToDelete) {
                builder.addTablesToDelete(ByteStringer.wrap(tableName));
              }
            }
            if(result.getColumnName() != null){
              builder.setColumnName(ByteStringer.wrap(result.getColumnName()));
            }
            if(result.getFamilyName() != null){
              builder.setFamilyName(ByteStringer.wrap(result.getFamilyName()));
            }
            if (result.getSharedTablesToDelete() !=null){
              for (SharedTableState sharedTableState : result.sharedTablesToDelete) {
                org.apache.phoenix.coprocessor.generated.MetaDataProtos.SharedTableState.Builder sharedTableStateBuilder =
                        org.apache.phoenix.coprocessor.generated.MetaDataProtos.SharedTableState.newBuilder();
                for (PColumn col : sharedTableState.getColumns()) {
                    sharedTableStateBuilder.addColumns(PColumnImpl.toProto(col));
                }
                for (PName physicalName : sharedTableState.getPhysicalNames()) {
                    sharedTableStateBuilder.addPhysicalNames(ByteStringer.wrap(physicalName.getBytes()));
                }
                if (sharedTableState.getTenantId()!=null) {
                    sharedTableStateBuilder.setTenantId(ByteStringer.wrap(sharedTableState.getTenantId().getBytes()));
                }
                sharedTableStateBuilder.setSchemaName(ByteStringer.wrap(sharedTableState.getSchemaName().getBytes()));
                sharedTableStateBuilder.setTableName(ByteStringer.wrap(sharedTableState.getTableName().getBytes()));
                sharedTableStateBuilder.setViewIndexId(sharedTableState.getViewIndexId());
                builder.addSharedTablesToDelete(sharedTableStateBuilder.build());
              }
            }
            if (result.getSchema() != null) {
              builder.setSchema(PSchema.toProto(result.schema));
            }
            builder.setAutoPartitionNum(result.getAutoPartitionNum());
                if (result.getViewIndexId() != null) {
                    builder.setViewIndexId(result.getViewIndexId());
                }
          }
          return builder.build();
        }

        public PSchema getSchema() {
            return schema;
        }
    }
  
    public static String getVersion(long serverTimestamp) {
        /*
         * It is possible that when clients are trying to run upgrades concurrently, we could be at an intermediate
         * server timestamp. Using floorKey provides us a range based lookup where the timestamp range for a release is
         * [timeStampForRelease, timestampForNextRelease).
         */
        String version = TIMESTAMP_VERSION_MAP.get(TIMESTAMP_VERSION_MAP.floorKey(serverTimestamp));
        return version;
    }
}
