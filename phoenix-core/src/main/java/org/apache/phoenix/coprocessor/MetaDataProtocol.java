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

import java.util.Collections;
import java.util.List;

import org.apache.phoenix.coprocessor.generated.MetaDataProtos;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos.MetaDataResponse;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos.MetaDataService;
import org.apache.phoenix.hbase.index.util.VersionUtil;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableImpl;
import org.apache.phoenix.util.ByteUtil;

import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import com.google.protobuf.HBaseZeroCopyByteString;


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
    public static final int PHOENIX_MINOR_VERSION = 4;
    public static final int PHOENIX_PATCH_NUMBER = 0;
    public static final int PHOENIX_VERSION =
            VersionUtil.encodeVersion(PHOENIX_MAJOR_VERSION, PHOENIX_MINOR_VERSION, PHOENIX_PATCH_NUMBER);

    public static final long MIN_TABLE_TIMESTAMP = 0;

    // Incremented from 5 to 7 with the addition of the STORE_NULLS table option in 4.3
    public static final long MIN_SYSTEM_TABLE_TIMESTAMP = MIN_TABLE_TIMESTAMP + 7;
    public static final int DEFAULT_MAX_META_DATA_VERSIONS = 1000;
    public static final int DEFAULT_MAX_STAT_DATA_VERSIONS = 3;
    public static final boolean DEFAULT_META_DATA_KEEP_DELETED_CELLS = true;
    
    // Min system table timestamps for every release.
    public static final long MIN_SYSTEM_TABLE_TIMESTAMP_4_1_0 = MIN_TABLE_TIMESTAMP + 3;
    public static final long MIN_SYSTEM_TABLE_TIMESTAMP_4_2_0 = MIN_TABLE_TIMESTAMP + 4;
    public static final long MIN_SYSTEM_TABLE_TIMESTAMP_4_2_1 = MIN_TABLE_TIMESTAMP + 5;
    public static final long MIN_SYSTEM_TABLE_TIMESTAMP_4_3_0 = MIN_TABLE_TIMESTAMP + 7;
    
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
        NO_OP
    };

  public static class MetaDataMutationResult {
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

        public static MetaDataMutationResult constructFromProto(MetaDataResponse proto) {
          MetaDataMutationResult result = new MetaDataMutationResult();
          result.returnCode = MutationCode.values()[proto.getReturnCode().ordinal()];
          result.mutationTime = proto.getMutationTime();
          if (proto.hasTable()) {
            result.wasUpdated = true;
            result.table = PTableImpl.createFromProto(proto.getTable());
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
                builder.addTablesToDelete(HBaseZeroCopyByteString.wrap(tableName));
              }
            }
            if(result.getColumnName() != null){
              builder.setColumnName(HBaseZeroCopyByteString.wrap(result.getColumnName()));
            }
            if(result.getFamilyName() != null){
              builder.setFamilyName(HBaseZeroCopyByteString.wrap(result.getFamilyName()));
            }
          }
          return builder.build();
        }
    }
}
