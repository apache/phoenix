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
package org.apache.phoenix.query;


import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessor.MetaDataProtocol;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.monitoring.MetricType;
import org.apache.phoenix.schema.MetaDataSplitPolicy;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PNameFactory;
import org.apache.phoenix.schema.PTable.ImmutableStorageScheme;
import org.apache.phoenix.schema.PTable.QualifierEncodingScheme;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.SystemFunctionSplitPolicy;
import org.apache.phoenix.schema.SystemStatsSplitPolicy;
import org.apache.phoenix.schema.SystemTaskSplitPolicy;
import org.apache.phoenix.schema.TableProperty;

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.APPEND_ONLY_SCHEMA;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.ARG_POSITION;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.ARRAY_SIZE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.AUTO_PARTITION_SEQ;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.BASE_COLUMN_COUNT;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.BIND_PARAMETERS;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.BUFFER_LENGTH;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.CACHE_SIZE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.CHANGE_DETECTION_ENABLED;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.CHAR_OCTET_LENGTH;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.CLASS_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.CLIENT_IP;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_COUNT;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_DEF;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_FAMILY;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_QUALIFIER;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_QUALIFIER_COUNTER;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_SIZE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.CURRENT_VALUE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.CYCLE_FLAG;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.DATA_TABLE_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.DATA_TYPE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.DECIMAL_DIGITS;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.DEFAULT_COLUMN_FAMILY_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.DEFAULT_VALUE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.DISABLE_WAL;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.ENCODING_SCHEME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.EXCEPTION_TRACE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.EXPLAIN_PLAN;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.FUNCTION_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.GLOBAL_SCAN_DETAILS;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.GUIDE_POSTS_ROW_COUNT;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.GUIDE_POSTS_WIDTH;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.GUIDE_POST_KEY;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.IMMUTABLE_ROWS;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.IMMUTABLE_STORAGE_SCHEME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.INCREMENT_BY;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.INDEX_DISABLE_TIMESTAMP;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.INDEX_STATE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.INDEX_TYPE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.IS_ARRAY;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.IS_AUTOINCREMENT;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.IS_CONSTANT;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.IS_NAMESPACE_MAPPED;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.IS_NULLABLE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.IS_ROW_TIMESTAMP;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.IS_VIEW_REFERENCED;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.JAR_PATH;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.KEY_SEQ;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.LAST_DDL_TIMESTAMP;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.LAST_STATS_UPDATE_TIME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.LIMIT_REACHED_FLAG;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.LINK_TYPE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.MAX_VALUE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.MIN_VALUE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.MULTI_TENANT;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.NO_OF_RESULTS_ITERATED;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.NULLABLE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.NUM_ARGS;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.NUM_PREC_RADIX;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.ORDINAL_POSITION;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.PHOENIX_TTL;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.PHOENIX_TTL_HWM;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.PHYSICAL_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.PK_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.QUERY;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.QUERY_ID;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.QUERY_STATUS;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.REF_GENERATION;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.REMARKS;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.RETURN_TYPE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SALT_BUCKETS;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SCAN_METRICS_JSON;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SCOPE_CATALOG;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SCOPE_SCHEMA;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SCOPE_TABLE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SELF_REFERENCING_COL_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SEQUENCE_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SEQUENCE_SCHEMA;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SORT_ORDER;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SOURCE_DATA_TYPE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SQL_DATA_TYPE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SQL_DATETIME_SUB;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.START_TIME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.START_WITH;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.STORE_NULLS;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CATALOG_SCHEMA;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CATALOG_TABLE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CHILD_LINK_TABLE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_FUNCTION_TABLE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_LOG_TABLE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_MUTEX_TABLE_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_STATS_TABLE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_TASK_TABLE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_SCHEM;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_SEQ_NUM;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_TYPE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TASK_DATA;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TASK_END_TS;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TASK_PRIORITY;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TASK_STATUS;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TASK_TABLE_TTL;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TASK_TS;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TASK_TYPE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TENANT_ID;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TRANSACTIONAL;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TRANSACTION_PROVIDER;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TTL_FOR_MUTEX;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TYPE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TYPE_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TYPE_SEQUENCE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.UPDATE_CACHE_FREQUENCY;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.USER;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.USE_STATS_FOR_PARALLELIZATION;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.VIEW_CONSTANT;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.VIEW_INDEX_ID;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.VIEW_INDEX_ID_DATA_TYPE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.VIEW_STATEMENT;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.VIEW_TYPE;

/**
 *
 * Constants used during querying
 *
 *
 * @since 0.1
 */
public interface QueryConstants {
    String NAME_SEPARATOR = ".";
    String NAMESPACE_SEPARATOR = ":";
    String CHILD_VIEW_INDEX_NAME_SEPARATOR = "#";
    byte[] NAMESPACE_SEPARATOR_BYTES = Bytes.toBytes(NAMESPACE_SEPARATOR);
    byte NAMESPACE_SEPARATOR_BYTE = NAMESPACE_SEPARATOR_BYTES[0];
    String NAME_SEPARATOR_REGEX = "\\" + NAME_SEPARATOR;
    byte[] NAME_SEPARATOR_BYTES = Bytes.toBytes(NAME_SEPARATOR);
    byte NAME_SEPARATOR_BYTE = NAME_SEPARATOR_BYTES[0];
    String NULL_DISPLAY_TEXT = "<null>";
    long UNSET_TIMESTAMP = -1;

    enum JoinType {INNER, LEFT_OUTER}
    String SYSTEM_SCHEMA_NAME = "SYSTEM";
    byte[] SYSTEM_SCHEMA_NAME_BYTES = Bytes.toBytes(SYSTEM_SCHEMA_NAME);
    String OFFSET_ROW_KEY = "_OFFSET_";
    byte[] OFFSET_ROW_KEY_BYTES = Bytes.toBytes(OFFSET_ROW_KEY);
    ImmutableBytesPtr OFFSET_ROW_KEY_PTR = new ImmutableBytesPtr(OFFSET_ROW_KEY_BYTES);

    long AGG_TIMESTAMP = HConstants.LATEST_TIMESTAMP;
    /**
     * Key used for a single row aggregation where there is no group by
     */
    byte[] UNGROUPED_AGG_ROW_KEY = Bytes.toBytes("a");
    
    /** BEGIN Set of reserved column qualifiers **/
    
    String RESERVED_COLUMN_FAMILY = "_v";
    byte[] RESERVED_COLUMN_FAMILY_BYTES = Bytes.toBytes(RESERVED_COLUMN_FAMILY);
    
    byte[] VALUE_COLUMN_FAMILY = RESERVED_COLUMN_FAMILY_BYTES;
    byte[] VALUE_COLUMN_QUALIFIER = QualifierEncodingScheme.FOUR_BYTE_QUALIFIERS.encode(1);
    
    byte[] ARRAY_VALUE_COLUMN_FAMILY = RESERVED_COLUMN_FAMILY_BYTES;
    byte[] ARRAY_VALUE_COLUMN_QUALIFIER = QualifierEncodingScheme.FOUR_BYTE_QUALIFIERS.encode(2);
    
    PName SINGLE_COLUMN_NAME = PNameFactory.newNormalizedName("s");
    PName SINGLE_COLUMN_FAMILY_NAME = PNameFactory.newNormalizedName("s");
    byte[] SINGLE_COLUMN = SINGLE_COLUMN_NAME.getBytes();
    byte[] SINGLE_COLUMN_FAMILY = SINGLE_COLUMN_FAMILY_NAME.getBytes();

    /** END Set of reserved column qualifiers **/
    
    byte[] TRUE = new byte[] {1};
    
    /**
     * The priority property for an hbase table. This is already in HTD, but older versions of
     * HBase do not have this, so we re-defined it here. Once Phoenix is HBase-1.3+, we can remote.
     */
    String PRIORITY = "PRIORITY";

    /**
     * Separator used between variable length keys for a composite key.
     * Variable length data types may not use this byte value.
     */
    byte SEPARATOR_BYTE = (byte) 0;
    byte[] SEPARATOR_BYTE_ARRAY = new byte[] {SEPARATOR_BYTE};
    byte DESC_SEPARATOR_BYTE = SortOrder.invert(SEPARATOR_BYTE);
    byte[] DESC_SEPARATOR_BYTE_ARRAY = new byte[] {DESC_SEPARATOR_BYTE};

    String DEFAULT_COPROCESS_JAR_NAME = "phoenix-[version]-server.jar";
    
    int MILLIS_IN_DAY = 1000 * 60 * 60 * 24;
    String EMPTY_COLUMN_NAME = "_0";
    // For transactional tables, the value of our empty key value can no longer be empty
    // since empty values are treated as column delete markers.
    byte[] EMPTY_COLUMN_BYTES = Bytes.toBytes(EMPTY_COLUMN_NAME);
    ImmutableBytesPtr EMPTY_COLUMN_BYTES_PTR = new ImmutableBytesPtr(EMPTY_COLUMN_BYTES);
    Integer ENCODED_EMPTY_COLUMN_NAME = 0;
    byte[] ENCODED_EMPTY_COLUMN_BYTES = QualifierEncodingScheme.FOUR_BYTE_QUALIFIERS.encode(
            ENCODED_EMPTY_COLUMN_NAME);
    String EMPTY_COLUMN_VALUE = "x";
    byte[] EMPTY_COLUMN_VALUE_BYTES = Bytes.toBytes(EMPTY_COLUMN_VALUE);
    ImmutableBytesPtr EMPTY_COLUMN_VALUE_BYTES_PTR = new ImmutableBytesPtr(
            EMPTY_COLUMN_VALUE_BYTES);
    byte[] ENCODED_EMPTY_COLUMN_VALUE_BYTES = Bytes.toBytes(EMPTY_COLUMN_VALUE);
    String DEFAULT_COLUMN_FAMILY = "0";
    byte[] DEFAULT_COLUMN_FAMILY_BYTES = Bytes.toBytes(DEFAULT_COLUMN_FAMILY);
    ImmutableBytesPtr DEFAULT_COLUMN_FAMILY_BYTES_PTR = new ImmutableBytesPtr(
            DEFAULT_COLUMN_FAMILY_BYTES);

    byte VERIFIED_BYTE = 1;
    byte UNVERIFIED_BYTE = 2;
    byte[] VERIFIED_BYTES = new byte[] { VERIFIED_BYTE };
    byte[] UNVERIFIED_BYTES = new byte[] { UNVERIFIED_BYTE };
    ImmutableBytesPtr VERIFIED_BYTES_PTR = new ImmutableBytesPtr(VERIFIED_BYTES);
    ImmutableBytesPtr UNVERIFIED_BYTES_PTR = new ImmutableBytesPtr(UNVERIFIED_BYTES);

    // column qualifier of the single key value used to store all columns for the
    // COLUMNS_STORED_IN_SINGLE_CELL storage scheme
    String SINGLE_KEYVALUE_COLUMN_QUALIFIER = "1";
    byte[] SINGLE_KEYVALUE_COLUMN_QUALIFIER_BYTES = Bytes.toBytes(SINGLE_KEYVALUE_COLUMN_QUALIFIER);
    ImmutableBytesPtr SINGLE_KEYVALUE_COLUMN_QUALIFIER_BYTES_PTR = new ImmutableBytesPtr(
            SINGLE_KEYVALUE_COLUMN_QUALIFIER_BYTES);

    String LOCAL_INDEX_COLUMN_FAMILY_PREFIX = "L#";
    byte[] LOCAL_INDEX_COLUMN_FAMILY_PREFIX_BYTES = Bytes.toBytes(LOCAL_INDEX_COLUMN_FAMILY_PREFIX);

    String DEFAULT_LOCAL_INDEX_COLUMN_FAMILY = LOCAL_INDEX_COLUMN_FAMILY_PREFIX +
            DEFAULT_COLUMN_FAMILY;
    byte[] DEFAULT_LOCAL_INDEX_COLUMN_FAMILY_BYTES = Bytes.toBytes(
            DEFAULT_LOCAL_INDEX_COLUMN_FAMILY);
    ImmutableBytesPtr DEFAULT_LOCAL_INDEX_COLUMN_FAMILY_BYTES_PTR = new ImmutableBytesPtr(
               DEFAULT_LOCAL_INDEX_COLUMN_FAMILY_BYTES);

    String ALL_FAMILY_PROPERTIES_KEY = "";
    String SYSTEM_TABLE_PK_NAME = "pk";

    double MILLIS_TO_NANOS_CONVERTOR = Math.pow(10, 6);
    BigDecimal BD_MILLIS_NANOS_CONVERSION = BigDecimal.valueOf(MILLIS_TO_NANOS_CONVERTOR);
    BigDecimal BD_MILLIS_IN_DAY = BigDecimal.valueOf(QueryConstants.MILLIS_IN_DAY);
    int MAX_ALLOWED_NANOS = 999999999;
    int DIVERGED_VIEW_BASE_COLUMN_COUNT = -100;
    int BASE_TABLE_BASE_COLUMN_COUNT = -1;

    // custom TagType
    byte VIEW_MODIFIED_PROPERTY_TAG_TYPE = (byte) 70;
    /**
     * We mark counter values 0 to 10 as reserved. Value 0 is used by
     * {@link #ENCODED_EMPTY_COLUMN_NAME}. Values 1-10
     * are reserved for special column qualifiers returned by Phoenix co-processors.
     */
    int ENCODED_CQ_COUNTER_INITIAL_VALUE = 11;
    String CREATE_TABLE_METADATA =
            // Do not use IF NOT EXISTS as we sometimes catch the TableAlreadyExists
            // exception and add columns to the SYSTEM.TABLE dynamically.
            "CREATE TABLE " + SYSTEM_CATALOG_SCHEMA + ".\"" + SYSTEM_CATALOG_TABLE + "\"(\n" +
            // PK columns
            TENANT_ID + " VARCHAR NULL," +
            TABLE_SCHEM + " VARCHAR NULL," +
            TABLE_NAME + " VARCHAR NOT NULL," +
            COLUMN_NAME + " VARCHAR NULL," + // null for table row
            COLUMN_FAMILY + " VARCHAR NULL," + // using for CF to uniqueness for columns
            // Table metadata (will be null for column rows)
            TABLE_SEQ_NUM + " BIGINT," +
            TABLE_TYPE + " CHAR(1)," +
            PK_NAME + " VARCHAR," +
            COLUMN_COUNT + " INTEGER," +
            SALT_BUCKETS + " INTEGER," +
            DATA_TABLE_NAME + " VARCHAR," +
            INDEX_STATE + " CHAR(1),\n" +
            IMMUTABLE_ROWS + " BOOLEAN,\n" +
            VIEW_STATEMENT + " VARCHAR,\n" +
            DEFAULT_COLUMN_FAMILY_NAME + " VARCHAR,\n" +
            DISABLE_WAL + " BOOLEAN,\n" +
            MULTI_TENANT + " BOOLEAN,\n" +
            VIEW_TYPE + " UNSIGNED_TINYINT,\n" +
            VIEW_INDEX_ID + " BIGINT,\n" +
            VIEW_INDEX_ID_DATA_TYPE + " INTEGER,\n" +
            PHOENIX_TTL + " BIGINT,\n" +
            PHOENIX_TTL_HWM + " BIGINT,\n" +
            LAST_DDL_TIMESTAMP + " BIGINT, " +
            CHANGE_DETECTION_ENABLED + " BOOLEAN, " +
            // Column metadata (will be null for table row)
            DATA_TYPE + " INTEGER," +
            COLUMN_SIZE + " INTEGER," +
            DECIMAL_DIGITS + " INTEGER," +
            NULLABLE + " INTEGER," +
            ORDINAL_POSITION + " INTEGER," +
            SORT_ORDER + " INTEGER," +
            ARRAY_SIZE + " INTEGER,\n" +
            VIEW_CONSTANT + " VARBINARY,\n" +
            IS_VIEW_REFERENCED + " BOOLEAN,\n" +
            KEY_SEQ + " SMALLINT,\n" +
            // Link metadata (only set on rows linking table to index or view)
            LINK_TYPE + " UNSIGNED_TINYINT,\n" +
            // Unused
            TYPE_NAME + " VARCHAR," +
            REMARKS + " VARCHAR," +
            SELF_REFERENCING_COL_NAME + " VARCHAR," +
            REF_GENERATION + " VARCHAR," +
            BUFFER_LENGTH + " INTEGER," +
            NUM_PREC_RADIX + " INTEGER," +
            COLUMN_DEF + " VARCHAR," +
            SQL_DATA_TYPE + " INTEGER," +
            SQL_DATETIME_SUB + " INTEGER," +
            CHAR_OCTET_LENGTH + " INTEGER," +
            IS_NULLABLE + " VARCHAR," +
            SCOPE_CATALOG + " VARCHAR," +
            SCOPE_SCHEMA + " VARCHAR," +
            SCOPE_TABLE + " VARCHAR," +
            SOURCE_DATA_TYPE + " SMALLINT," +
            IS_AUTOINCREMENT + " VARCHAR," +
            INDEX_TYPE + " UNSIGNED_TINYINT," +
            INDEX_DISABLE_TIMESTAMP + " BIGINT," +
            STORE_NULLS + " BOOLEAN," +
            BASE_COLUMN_COUNT + " INTEGER," +
            // Column metadata (will be null for table row)
            IS_ROW_TIMESTAMP + " BOOLEAN, " +
            TRANSACTIONAL + " BOOLEAN," +
            UPDATE_CACHE_FREQUENCY + " BIGINT," +
            IS_NAMESPACE_MAPPED + " BOOLEAN," +
            AUTO_PARTITION_SEQ + " VARCHAR," +
            APPEND_ONLY_SCHEMA + " BOOLEAN," +
            GUIDE_POSTS_WIDTH + " BIGINT," +
            COLUMN_QUALIFIER + " VARBINARY," +
            IMMUTABLE_STORAGE_SCHEME + " TINYINT, " +
            ENCODING_SCHEME + " TINYINT, " +
            COLUMN_QUALIFIER_COUNTER + " INTEGER, " +
            USE_STATS_FOR_PARALLELIZATION + " BOOLEAN, " +
            TRANSACTION_PROVIDER + " TINYINT, " +
            "CONSTRAINT " + SYSTEM_TABLE_PK_NAME + " PRIMARY KEY (" + TENANT_ID + ","
            + TABLE_SCHEM + "," + TABLE_NAME + "," + COLUMN_NAME + "," + COLUMN_FAMILY + "))\n" +
            HConstants.VERSIONS + "=%s,\n" +
            ColumnFamilyDescriptorBuilder.KEEP_DELETED_CELLS + "=%s,\n" +
            // Install split policy to prevent a tenant's metadata from being split across regions.
            TableDescriptorBuilder.SPLIT_POLICY + "='" + MetaDataSplitPolicy.class.getName() +
            "',\n" + TRANSACTIONAL + "=" + Boolean.FALSE;

    String CREATE_STATS_TABLE_METADATA =
            "CREATE TABLE " + SYSTEM_CATALOG_SCHEMA + ".\"" + SYSTEM_STATS_TABLE + "\"(\n" +
            // PK columns
            PHYSICAL_NAME  + " VARCHAR NOT NULL," +
            COLUMN_FAMILY + " VARCHAR," +
            GUIDE_POST_KEY  + " VARBINARY," +
            GUIDE_POSTS_WIDTH + " BIGINT," +
            LAST_STATS_UPDATE_TIME+ " DATE, "+
            GUIDE_POSTS_ROW_COUNT+ " BIGINT, "+
            "CONSTRAINT " + SYSTEM_TABLE_PK_NAME + " PRIMARY KEY ("
            + PHYSICAL_NAME + ","
            + COLUMN_FAMILY + ","+ GUIDE_POST_KEY+"))\n" +
            // Install split policy to prevent a physical table's stats from being split
            // across regions.
            TableDescriptorBuilder.SPLIT_POLICY + "='" + SystemStatsSplitPolicy.class.getName() + "',\n" + 
            TRANSACTIONAL + "=" + Boolean.FALSE;

    String CREATE_SEQUENCE_METADATA =
            "CREATE TABLE " + SYSTEM_CATALOG_SCHEMA + ".\"" + TYPE_SEQUENCE + "\"(\n" +
            TENANT_ID + " VARCHAR NULL," +
            SEQUENCE_SCHEMA + " VARCHAR NULL, \n" +
            SEQUENCE_NAME +  " VARCHAR NOT NULL, \n" +
            START_WITH + " BIGINT, \n" +
            CURRENT_VALUE + " BIGINT, \n" +
            INCREMENT_BY  + " BIGINT, \n" +
            CACHE_SIZE  + " BIGINT, \n" +
            //  the following three columns were added in 3.1/4.1
            MIN_VALUE + " BIGINT, \n" +
            MAX_VALUE + " BIGINT, \n" +
            CYCLE_FLAG + " BOOLEAN, \n" +
            LIMIT_REACHED_FLAG + " BOOLEAN \n" +
            " CONSTRAINT " + SYSTEM_TABLE_PK_NAME + " PRIMARY KEY (" +
            TENANT_ID + "," + SEQUENCE_SCHEMA + "," + SEQUENCE_NAME + "))\n" +
            HConstants.VERSIONS + "=%s,\n" +
            ColumnFamilyDescriptorBuilder.KEEP_DELETED_CELLS + "=%s,\n"+
            TRANSACTIONAL + "=" + Boolean.FALSE;

    String UPGRADE_TABLE_SNAPSHOT_PREFIX = "_UPGRADING_TABLE_";

    String CREATE_FUNCTION_METADATA =
            "CREATE TABLE " + SYSTEM_CATALOG_SCHEMA + ".\"" + SYSTEM_FUNCTION_TABLE + "\"(\n" +
             // Pk columns
            TENANT_ID + " VARCHAR NULL," +
            FUNCTION_NAME + " VARCHAR NOT NULL, \n" +
            NUM_ARGS + " INTEGER, \n" +
            // Function metadata (will be null for argument row)
            CLASS_NAME +  " VARCHAR, \n" +
            JAR_PATH + "  VARCHAR, \n" +
            RETURN_TYPE + " VARCHAR, \n" +
            // Argument metadata (will be null for function row)
            TYPE + " VARCHAR, \n" +
            ARG_POSITION + " VARBINARY, \n" +
            IS_ARRAY + " BOOLEAN, \n" +
            IS_CONSTANT + " BOOLEAN, \n" +
            DEFAULT_VALUE + " VARCHAR, \n" +
            MIN_VALUE + " VARCHAR, \n" +
            MAX_VALUE + " VARCHAR, \n" +
            " CONSTRAINT " + SYSTEM_TABLE_PK_NAME + " PRIMARY KEY (" + TENANT_ID + ", " +
            FUNCTION_NAME + ", " + TYPE + ", " + ARG_POSITION + "))\n" +
            HConstants.VERSIONS + "=%s,\n" +
            ColumnFamilyDescriptorBuilder.KEEP_DELETED_CELLS + "=%s,\n"+
            // Install split policy to prevent a tenant's metadata from being split across regions.
            TableDescriptorBuilder.SPLIT_POLICY + "='" + SystemFunctionSplitPolicy.class.getName() +
            "',\n" + TRANSACTIONAL + "=" + Boolean.FALSE;

    String CREATE_LOG_METADATA =
            "CREATE IMMUTABLE TABLE " + SYSTEM_CATALOG_SCHEMA + ".\"" + SYSTEM_LOG_TABLE + "\"(\n" +
            // Pk columns
            START_TIME + " DECIMAL, \n" +
            TABLE_NAME + " VARCHAR, \n" +
            QUERY_ID + " VARCHAR NOT NULL,\n" +
            TENANT_ID + " VARCHAR ," +
            USER + " VARCHAR , \n" +
            CLIENT_IP + " VARCHAR, \n" +
            // Function metadata (will be null for argument row)
            QUERY +  " VARCHAR, \n" +
            EXPLAIN_PLAN + " VARCHAR, \n" +
            // Argument metadata (will be null for function row)
            NO_OF_RESULTS_ITERATED + " BIGINT, \n" +
            QUERY_STATUS + " VARCHAR, \n" +
            EXCEPTION_TRACE + " VARCHAR, \n" +
            GLOBAL_SCAN_DETAILS + " VARCHAR, \n" +
            BIND_PARAMETERS + " VARCHAR, \n" +
            SCAN_METRICS_JSON + " VARCHAR, \n" +
            MetricType.getMetricColumnsDetails()+"\n"+
            " CONSTRAINT " + SYSTEM_TABLE_PK_NAME +
            " PRIMARY KEY (START_TIME, TABLE_NAME, QUERY_ID))\n" +
            SALT_BUCKETS + "=%s,\n"+
            TRANSACTIONAL + "=" + Boolean.FALSE+ ",\n" +
            ColumnFamilyDescriptorBuilder.TTL + "=" + MetaDataProtocol.DEFAULT_LOG_TTL+",\n"+
            TableProperty.IMMUTABLE_STORAGE_SCHEME.toString() + " = " +
            ImmutableStorageScheme.SINGLE_CELL_ARRAY_WITH_OFFSETS.name() + ",\n" +
            TableProperty.COLUMN_ENCODED_BYTES.toString()+" = 1";
    
    byte[] OFFSET_FAMILY = "f_offset".getBytes(StandardCharsets.UTF_8);
    byte[] OFFSET_COLUMN = "c_offset".getBytes(StandardCharsets.UTF_8);
    String LAST_SCAN = "LAST_SCAN";
    String HASH_JOIN_CACHE_RETRIES = "hashjoin.client.retries.number";
    int DEFAULT_HASH_JOIN_CACHE_RETRIES = 5;
    
	// Links from parent to child views are stored in a separate table for
	// scalability
	String CREATE_CHILD_LINK_METADATA = "CREATE TABLE " + SYSTEM_CATALOG_SCHEMA + ".\"" +
            SYSTEM_CHILD_LINK_TABLE + "\"(\n" +
			// PK columns
			TENANT_ID + " VARCHAR NULL," +
            TABLE_SCHEM + " VARCHAR NULL," +
            TABLE_NAME + " VARCHAR NOT NULL," +
            COLUMN_NAME + " VARCHAR NULL," +
            COLUMN_FAMILY + " VARCHAR NULL," +
            LINK_TYPE + " UNSIGNED_TINYINT,\n" +
            "CONSTRAINT " + SYSTEM_TABLE_PK_NAME + " PRIMARY KEY (" +
            TENANT_ID + "," + TABLE_SCHEM + "," + TABLE_NAME + "," +
            COLUMN_NAME + "," + COLUMN_FAMILY + "))\n" +
            HConstants.VERSIONS + "=%s,\n" +
            ColumnFamilyDescriptorBuilder.KEEP_DELETED_CELLS + "=%s,\n" +
            TRANSACTIONAL + "=" + Boolean.FALSE;

    String CREATE_MUTEX_METADATA =
            "CREATE IMMUTABLE TABLE " + SYSTEM_CATALOG_SCHEMA + ".\"" +
            SYSTEM_MUTEX_TABLE_NAME + "\"(\n" +
            // Pk columns
            TENANT_ID + " VARCHAR NULL," +
            TABLE_SCHEM + " VARCHAR NULL," +
            TABLE_NAME + " VARCHAR NOT NULL," +
            COLUMN_NAME + " VARCHAR NULL," + // null for table row
            COLUMN_FAMILY + " VARCHAR NULL " + // using for CF to uniqueness for columns
            "CONSTRAINT " + SYSTEM_TABLE_PK_NAME + " PRIMARY KEY (" + TENANT_ID + "," +
            TABLE_SCHEM + "," + TABLE_NAME + "," + COLUMN_NAME + "," + COLUMN_FAMILY + "))\n" +
            HConstants.VERSIONS + "=%s,\n" +
            ColumnFamilyDescriptorBuilder.KEEP_DELETED_CELLS + "=%s,\n" +
            TRANSACTIONAL + "=" + Boolean.FALSE + ",\n" +
            ColumnFamilyDescriptorBuilder.TTL + "=" + TTL_FOR_MUTEX;

    String CREATE_TASK_METADATA =
            "CREATE TABLE " + SYSTEM_CATALOG_SCHEMA + ".\"" + SYSTEM_TASK_TABLE + "\"(\n" +
            // PK columns
            TASK_TYPE + " UNSIGNED_TINYINT NOT NULL," +
            TASK_TS + " TIMESTAMP NOT NULL," +
            TENANT_ID + " VARCHAR NULL," +
            TABLE_SCHEM + " VARCHAR NULL," +
            TABLE_NAME + " VARCHAR NOT NULL,\n" +
            // Non-PK columns
            TASK_STATUS + " VARCHAR NULL," +
            TASK_END_TS + " TIMESTAMP NULL," +
            TASK_PRIORITY + " UNSIGNED_TINYINT NULL," +
            TASK_DATA + " VARCHAR NULL,\n" +
            "CONSTRAINT " + SYSTEM_TABLE_PK_NAME + " PRIMARY KEY (" +
            TASK_TYPE + "," + TASK_TS + " ROW_TIMESTAMP," + TENANT_ID + "," + TABLE_SCHEM + "," +
            TABLE_NAME + "))\n" +
            HConstants.VERSIONS + "=%s,\n" +
            ColumnFamilyDescriptorBuilder.KEEP_DELETED_CELLS + "=%s,\n" +
            ColumnFamilyDescriptorBuilder.TTL + "=" + TASK_TABLE_TTL + ",\n" +     // 10 days
            TableDescriptorBuilder.SPLIT_POLICY + "='"
                + SystemTaskSplitPolicy.class.getName() + "',\n" +
            TRANSACTIONAL + "=" + Boolean.FALSE + ",\n" +
            STORE_NULLS + "=" + Boolean.TRUE;
}
