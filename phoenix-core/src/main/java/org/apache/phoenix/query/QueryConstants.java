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


import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.*;

import java.math.BigDecimal;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessor.MetaDataProtocol;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.monitoring.MetricType;
import org.apache.phoenix.schema.MetaDataSplitPolicy;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PNameFactory;
import org.apache.phoenix.schema.PTable.ImmutableStorageScheme;
import org.apache.phoenix.schema.PTable.QualifierEncodingScheme;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.SystemFunctionSplitPolicy;
import org.apache.phoenix.schema.SystemStatsSplitPolicy;
import org.apache.phoenix.schema.TableProperty;


/**
 *
 * Constants used during querying
 *
 *
 * @since 0.1
 */
public interface QueryConstants {
    public static final String NAME_SEPARATOR = ".";
    public static final String NAMESPACE_SEPARATOR = ":";
    public static final String CHILD_VIEW_INDEX_NAME_SEPARATOR = "#";
    public static final byte[] NAMESPACE_SEPARATOR_BYTES = Bytes.toBytes(NAMESPACE_SEPARATOR);
    public static final byte NAMESPACE_SEPARATOR_BYTE = NAMESPACE_SEPARATOR_BYTES[0];
    public static final String NAME_SEPARATOR_REGEX = "\\" + NAME_SEPARATOR;
    public final static byte[] NAME_SEPARATOR_BYTES = Bytes.toBytes(NAME_SEPARATOR);
    public static final byte NAME_SEPARATOR_BYTE = NAME_SEPARATOR_BYTES[0];
    public static final String NULL_SCHEMA_NAME = "";
    public static final String NULL_DISPLAY_TEXT = "<null>";
    public static final long UNSET_TIMESTAMP = -1;

    public enum JoinType {INNER, LEFT_OUTER}
    public final static String SYSTEM_SCHEMA_NAME = "SYSTEM";
    public final static byte[] SYSTEM_SCHEMA_NAME_BYTES = Bytes.toBytes(SYSTEM_SCHEMA_NAME);
    public final static String HBASE_DEFAULT_SCHEMA_NAME = "default";
    public final static String PHOENIX_METADATA = "table";
    public final static String OFFSET_ROW_KEY = "_OFFSET_";
    public final static byte[] OFFSET_ROW_KEY_BYTES = Bytes.toBytes(OFFSET_ROW_KEY);
    public final static ImmutableBytesPtr OFFSET_ROW_KEY_PTR = new ImmutableBytesPtr(OFFSET_ROW_KEY_BYTES);

    public static final long AGG_TIMESTAMP = HConstants.LATEST_TIMESTAMP;
    /**
     * Key used for a single row aggregation where there is no group by
     */
    public final static byte[] UNGROUPED_AGG_ROW_KEY = Bytes.toBytes("a");
    
    /** BEGIN Set of reserved column qualifiers **/
    
    public static final String RESERVED_COLUMN_FAMILY = "_v";
    public static final byte[] RESERVED_COLUMN_FAMILY_BYTES = Bytes.toBytes(RESERVED_COLUMN_FAMILY);
    
    public static final byte[] VALUE_COLUMN_FAMILY = RESERVED_COLUMN_FAMILY_BYTES;
    public static final byte[] VALUE_COLUMN_QUALIFIER = QualifierEncodingScheme.FOUR_BYTE_QUALIFIERS.encode(1);
    
    public static final byte[] ARRAY_VALUE_COLUMN_FAMILY = RESERVED_COLUMN_FAMILY_BYTES;
    public static final byte[] ARRAY_VALUE_COLUMN_QUALIFIER = QualifierEncodingScheme.FOUR_BYTE_QUALIFIERS.encode(2);
    
    public final static PName SINGLE_COLUMN_NAME = PNameFactory.newNormalizedName("s");
    public final static PName SINGLE_COLUMN_FAMILY_NAME = PNameFactory.newNormalizedName("s");
    public final static byte[] SINGLE_COLUMN = SINGLE_COLUMN_NAME.getBytes();
    public final static byte[] SINGLE_COLUMN_FAMILY = SINGLE_COLUMN_FAMILY_NAME.getBytes();

    /** END Set of reserved column qualifiers **/
    
    public static final byte[] TRUE = new byte[] {1};
    
    /**
     * The priority property for an hbase table. This is already in HTD, but older versions of
     * HBase do not have this, so we re-defined it here. Once Phoenix is HBase-1.3+, we can remote.
     */
    public static final String PRIORITY = "PRIORITY";

    /**
     * Separator used between variable length keys for a composite key.
     * Variable length data types may not use this byte value.
     */
    public static final byte SEPARATOR_BYTE = (byte) 0;
    public static final byte[] SEPARATOR_BYTE_ARRAY = new byte[] {SEPARATOR_BYTE};
    public static final byte DESC_SEPARATOR_BYTE = SortOrder.invert(SEPARATOR_BYTE);
    public static final byte[] DESC_SEPARATOR_BYTE_ARRAY = new byte[] {DESC_SEPARATOR_BYTE};

    public static final String DEFAULT_COPROCESS_PATH = "phoenix.jar";
    public static final String DEFAULT_COPROCESS_JAR_NAME = "phoenix-[version]-server.jar";
    
    public final static int MILLIS_IN_DAY = 1000 * 60 * 60 * 24;

    public static final String EMPTY_COLUMN_NAME = "_0";
    // For transactional tables, the value of our empty key value can no longer be empty
    // since empty values are treated as column delete markers.
    public static final byte[] EMPTY_COLUMN_BYTES = Bytes.toBytes(EMPTY_COLUMN_NAME);
    public static final ImmutableBytesPtr EMPTY_COLUMN_BYTES_PTR = new ImmutableBytesPtr(
            EMPTY_COLUMN_BYTES);
    public static final Integer ENCODED_EMPTY_COLUMN_NAME = 0;
    public static final byte[] ENCODED_EMPTY_COLUMN_BYTES = QualifierEncodingScheme.FOUR_BYTE_QUALIFIERS.encode(ENCODED_EMPTY_COLUMN_NAME);
    public final static String EMPTY_COLUMN_VALUE = "x";
    public final static byte[] EMPTY_COLUMN_VALUE_BYTES = Bytes.toBytes(EMPTY_COLUMN_VALUE);
    public static final ImmutableBytesPtr EMPTY_COLUMN_VALUE_BYTES_PTR = new ImmutableBytesPtr(
            EMPTY_COLUMN_VALUE_BYTES);
    public static final String ENCODED_EMPTY_COLUMN_VALUE = EMPTY_COLUMN_VALUE;
    public final static byte[] ENCODED_EMPTY_COLUMN_VALUE_BYTES = Bytes.toBytes(EMPTY_COLUMN_VALUE);
    public static final ImmutableBytesPtr ENCODED_EMPTY_COLUMN_VALUE_BYTES_PTR = new ImmutableBytesPtr(
            ENCODED_EMPTY_COLUMN_VALUE_BYTES);
    public static final String DEFAULT_COLUMN_FAMILY = "0";
    public static final byte[] DEFAULT_COLUMN_FAMILY_BYTES = Bytes.toBytes(DEFAULT_COLUMN_FAMILY);
    public static final ImmutableBytesPtr DEFAULT_COLUMN_FAMILY_BYTES_PTR = new ImmutableBytesPtr(
            DEFAULT_COLUMN_FAMILY_BYTES);
    // column qualifier of the single key value used to store all columns for the COLUMNS_STORED_IN_SINGLE_CELL storage scheme
    public static final String SINGLE_KEYVALUE_COLUMN_QUALIFIER = "1";
    public final static byte[] SINGLE_KEYVALUE_COLUMN_QUALIFIER_BYTES = Bytes.toBytes(SINGLE_KEYVALUE_COLUMN_QUALIFIER);
    public static final ImmutableBytesPtr SINGLE_KEYVALUE_COLUMN_QUALIFIER_BYTES_PTR = new ImmutableBytesPtr(
            SINGLE_KEYVALUE_COLUMN_QUALIFIER_BYTES);

    public static final String LOCAL_INDEX_COLUMN_FAMILY_PREFIX = "L#";
    public static final byte[] LOCAL_INDEX_COLUMN_FAMILY_PREFIX_BYTES = Bytes.toBytes(LOCAL_INDEX_COLUMN_FAMILY_PREFIX);
    public static final ImmutableBytesPtr LOCAL_INDEX_COLUMN_FAMILY_PREFIX_PTR = new ImmutableBytesPtr(
        LOCAL_INDEX_COLUMN_FAMILY_PREFIX_BYTES);
    
    public static final String DEFAULT_LOCAL_INDEX_COLUMN_FAMILY = LOCAL_INDEX_COLUMN_FAMILY_PREFIX + DEFAULT_COLUMN_FAMILY;
    public static final byte[] DEFAULT_LOCAL_INDEX_COLUMN_FAMILY_BYTES = Bytes.toBytes(DEFAULT_LOCAL_INDEX_COLUMN_FAMILY);
    public static final ImmutableBytesPtr DEFAULT_LOCAL_INDEX_COLUMN_FAMILY_BYTES_PTR = new ImmutableBytesPtr(
               DEFAULT_LOCAL_INDEX_COLUMN_FAMILY_BYTES);

    public static final String ALL_FAMILY_PROPERTIES_KEY = "";
    public static final String SYSTEM_TABLE_PK_NAME = "pk";

    public static final double MILLIS_TO_NANOS_CONVERTOR = Math.pow(10, 6);
    public static final BigDecimal BD_MILLIS_NANOS_CONVERSION = BigDecimal.valueOf(MILLIS_TO_NANOS_CONVERTOR);
    public static final BigDecimal BD_MILLIS_IN_DAY = BigDecimal.valueOf(QueryConstants.MILLIS_IN_DAY);
    public static final int MAX_ALLOWED_NANOS = 999999999;
    public static final int NANOS_IN_SECOND = BigDecimal.valueOf(Math.pow(10, 9)).intValue();
    public static final int DIVERGED_VIEW_BASE_COLUMN_COUNT = -100;
    public static final int BASE_TABLE_BASE_COLUMN_COUNT = -1;
    
    /**
     * We mark counter values 0 to 10 as reserved. Value 0 is used by {@link #ENCODED_EMPTY_COLUMN_NAME}. Values 1-10
     * are reserved for special column qualifiers returned by Phoenix co-processors.
     */
    public static final int ENCODED_CQ_COUNTER_INITIAL_VALUE = 11;
    public static final String CREATE_TABLE_METADATA =
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
            TableDescriptorBuilder.SPLIT_POLICY + "='" + MetaDataSplitPolicy.class.getName() + "',\n" + 
            PhoenixDatabaseMetaData.TRANSACTIONAL + "=" + Boolean.FALSE;

    public static final String CREATE_STATS_TABLE_METADATA =
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
            // Install split policy to prevent a physical table's stats from being split across regions.
            TableDescriptorBuilder.SPLIT_POLICY + "='" + SystemStatsSplitPolicy.class.getName() + "',\n" + 
            PhoenixDatabaseMetaData.TRANSACTIONAL + "=" + Boolean.FALSE;

    public static final String CREATE_SEQUENCE_METADATA =
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
            " CONSTRAINT " + SYSTEM_TABLE_PK_NAME + " PRIMARY KEY (" + TENANT_ID + "," + SEQUENCE_SCHEMA + "," + SEQUENCE_NAME + "))\n" +
            HConstants.VERSIONS + "=%s,\n" +
            ColumnFamilyDescriptorBuilder.KEEP_DELETED_CELLS + "=%s,\n"+
            PhoenixDatabaseMetaData.TRANSACTIONAL + "=" + Boolean.FALSE;
    public static final String CREATE_SYSTEM_SCHEMA = "CREATE SCHEMA " + SYSTEM_CATALOG_SCHEMA;
    public static final String UPGRADE_TABLE_SNAPSHOT_PREFIX = "_UPGRADING_TABLE_";

    public static final String CREATE_FUNCTION_METADATA =
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
            " CONSTRAINT " + SYSTEM_TABLE_PK_NAME + " PRIMARY KEY (" + TENANT_ID + ", " + FUNCTION_NAME + ", " + TYPE + ", " + ARG_POSITION + "))\n" +
            HConstants.VERSIONS + "=%s,\n" +
            ColumnFamilyDescriptorBuilder.KEEP_DELETED_CELLS + "=%s,\n"+
            // Install split policy to prevent a tenant's metadata from being split across regions.
            TableDescriptorBuilder.SPLIT_POLICY + "='" + SystemFunctionSplitPolicy.class.getName() + "',\n" + 
            PhoenixDatabaseMetaData.TRANSACTIONAL + "=" + Boolean.FALSE;
    
    public static final String CREATE_LOG_METADATA =
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
            " CONSTRAINT " + SYSTEM_TABLE_PK_NAME + " PRIMARY KEY (START_TIME, TABLE_NAME, QUERY_ID))\n" +
            PhoenixDatabaseMetaData.SALT_BUCKETS + "=%s,\n"+
            PhoenixDatabaseMetaData.TRANSACTIONAL + "=" + Boolean.FALSE+ ",\n" +
            ColumnFamilyDescriptorBuilder.TTL + "=" + MetaDataProtocol.DEFAULT_LOG_TTL+",\n"+
            TableProperty.IMMUTABLE_STORAGE_SCHEME.toString() + " = " + ImmutableStorageScheme.SINGLE_CELL_ARRAY_WITH_OFFSETS.name() + ",\n" +
            TableProperty.COLUMN_ENCODED_BYTES.toString()+" = 1";
    
    public static final byte[] OFFSET_FAMILY = "f_offset".getBytes();
    public static final byte[] OFFSET_COLUMN = "c_offset".getBytes();
    public static final String LAST_SCAN = "LAST_SCAN";
    public static final byte[] UPGRADE_MUTEX = "UPGRADE_MUTEX".getBytes();
    public static final String HASH_JOIN_CACHE_RETRIES = "hashjoin.client.retries.number";
    public static final int DEFAULT_HASH_JOIN_CACHE_RETRIES = 5;
    
	// Links from parent to child views are stored in a separate table for
	// scalability
	public static final String CREATE_CHILD_LINK_METADATA = "CREATE TABLE " + SYSTEM_CATALOG_SCHEMA + ".\""
			+ SYSTEM_CHILD_LINK_TABLE + "\"(\n" +
			// PK columns
			TENANT_ID + " VARCHAR NULL," + TABLE_SCHEM + " VARCHAR NULL," + TABLE_NAME + " VARCHAR NOT NULL,"
			+ COLUMN_NAME + " VARCHAR NULL," + COLUMN_FAMILY + " VARCHAR NULL," + LINK_TYPE + " UNSIGNED_TINYINT,\n"
			+ "CONSTRAINT " + SYSTEM_TABLE_PK_NAME + " PRIMARY KEY (" + TENANT_ID + "," + TABLE_SCHEM + "," + TABLE_NAME
			+ "," + COLUMN_NAME + "," + COLUMN_FAMILY + "))\n" + HConstants.VERSIONS + "=%s,\n"
			+ ColumnFamilyDescriptorBuilder.KEEP_DELETED_CELLS + "=%s,\n" + PhoenixDatabaseMetaData.TRANSACTIONAL + "="
			+ Boolean.FALSE;
	
	 public static final String CREATE_MUTEX_METADTA =
	            "CREATE IMMUTABLE TABLE " + SYSTEM_CATALOG_SCHEMA + ".\"" + SYSTEM_MUTEX_TABLE_NAME + "\"(\n" +
	            // Pk columns
	            TENANT_ID + " VARCHAR NULL," +
	            TABLE_SCHEM + " VARCHAR NULL," +
	            TABLE_NAME + " VARCHAR NOT NULL," +
	            COLUMN_NAME + " VARCHAR NULL," + // null for table row
	            COLUMN_FAMILY + " VARCHAR NULL " + // using for CF to uniqueness for columns
	            "CONSTRAINT " + SYSTEM_TABLE_PK_NAME + " PRIMARY KEY (" + TENANT_ID + ","
	            + TABLE_SCHEM + "," + TABLE_NAME + "," + COLUMN_NAME + "," + COLUMN_FAMILY + "))\n" +
	            HConstants.VERSIONS + "=%s,\n" +
	            ColumnFamilyDescriptorBuilder.KEEP_DELETED_CELLS + "=%s,\n" +
	            PhoenixDatabaseMetaData.TRANSACTIONAL + "=" + Boolean.FALSE;
    
}
