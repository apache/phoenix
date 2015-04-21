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


import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.ARRAY_SIZE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.BUFFER_LENGTH;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.CACHE_SIZE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.CHAR_OCTET_LENGTH;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_COUNT;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_DEF;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_FAMILY;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_SIZE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.CURRENT_VALUE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.CYCLE_FLAG;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.DATA_TABLE_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.DATA_TYPE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.DECIMAL_DIGITS;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.DEFAULT_COLUMN_FAMILY_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.DISABLE_WAL;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.GUIDE_POSTS;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.GUIDE_POSTS_COUNT;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.GUIDE_POSTS_ROW_COUNT;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.GUIDE_POSTS_WIDTH;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.IMMUTABLE_ROWS;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.INCREMENT_BY;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.INDEX_DISABLE_TIMESTAMP;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.INDEX_STATE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.INDEX_TYPE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.IS_AUTOINCREMENT;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.IS_NULLABLE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.IS_VIEW_REFERENCED;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.KEY_SEQ;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.LAST_STATS_UPDATE_TIME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.LIMIT_REACHED_FLAG;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.LINK_TYPE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.MAX_KEY;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.MAX_VALUE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.MIN_KEY;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.MIN_VALUE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.MULTI_TENANT;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.NULLABLE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.NUM_PREC_RADIX;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.ORDINAL_POSITION;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.PHYSICAL_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.PK_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.REF_GENERATION;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.REGION_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.REMARKS;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.STORE_NULLS;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SALT_BUCKETS;
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
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.START_WITH;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CATALOG_SCHEMA;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CATALOG_TABLE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_STATS_TABLE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_SCHEM;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_SEQ_NUM;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_TYPE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TENANT_ID;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TYPE_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TYPE_SEQUENCE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.VIEW_CONSTANT;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.VIEW_INDEX_ID;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.VIEW_STATEMENT;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.VIEW_TYPE;

import java.math.BigDecimal;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessor.MetaDataProtocol;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.schema.MetaDataSplitPolicy;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PNameFactory;
import org.apache.phoenix.util.ByteUtil;


/**
 *
 * Constants used during querying
 *
 *
 * @since 0.1
 */
public interface QueryConstants {
    public static final String NAME_SEPARATOR = ".";
    public static final String NAME_SEPARATOR_REGEX = "\\" + NAME_SEPARATOR;
    public final static byte[] NAME_SEPARATOR_BYTES = Bytes.toBytes(NAME_SEPARATOR);
    public static final byte NAME_SEPARATOR_BYTE = NAME_SEPARATOR_BYTES[0];
    public static final String NULL_SCHEMA_NAME = "";
    public static final String NULL_DISPLAY_TEXT = "<null>";
    public static final long UNSET_TIMESTAMP = -1;

    public enum JoinType {INNER, LEFT_OUTER}
    public final static String SYSTEM_SCHEMA_NAME = "SYSTEM";
    public final static byte[] SYSTEM_SCHEMA_NAME_BYTES = Bytes.toBytes(SYSTEM_SCHEMA_NAME);
    public final static String PHOENIX_METADATA = "table";

    public final static PName SINGLE_COLUMN_NAME = PNameFactory.newNormalizedName("s");
    public final static PName SINGLE_COLUMN_FAMILY_NAME = PNameFactory.newNormalizedName("s");
    public final static byte[] SINGLE_COLUMN = SINGLE_COLUMN_NAME.getBytes();
    public final static byte[] SINGLE_COLUMN_FAMILY = SINGLE_COLUMN_FAMILY_NAME.getBytes();

    public static final long AGG_TIMESTAMP = HConstants.LATEST_TIMESTAMP;
    /**
     * Key used for a single row aggregation where there is no group by
     */
    public final static byte[] UNGROUPED_AGG_ROW_KEY = Bytes.toBytes("a");
    public final static PName AGG_COLUMN_NAME = SINGLE_COLUMN_NAME;
    public final static PName AGG_COLUMN_FAMILY_NAME = SINGLE_COLUMN_FAMILY_NAME;

    public static final byte[] ARRAY_VALUE_COLUMN_FAMILY = Bytes.toBytes("a");
    // Use empty byte array for column qualifier so as not to accidentally conflict with any other columns
    public static final byte[] ARRAY_VALUE_COLUMN_QUALIFIER = ByteUtil.EMPTY_BYTE_ARRAY;

    public static final byte[] TRUE = new byte[] {1};

    /**
     * Separator used between variable length keys for a composite key.
     * Variable length data types may not use this byte value.
     */
    public static final byte SEPARATOR_BYTE = (byte) 0;
    public static final byte[] SEPARATOR_BYTE_ARRAY = new byte[] {SEPARATOR_BYTE};

    public static final String DEFAULT_COPROCESS_PATH = "phoenix.jar";
    public final static int MILLIS_IN_DAY = 1000 * 60 * 60 * 24;

    public static final String EMPTY_COLUMN_NAME = "_0";
    public static final byte[] EMPTY_COLUMN_BYTES = Bytes.toBytes(EMPTY_COLUMN_NAME);
    public static final ImmutableBytesPtr EMPTY_COLUMN_BYTES_PTR = new ImmutableBytesPtr(
            EMPTY_COLUMN_BYTES);

    public static final String DEFAULT_COLUMN_FAMILY = "0";
    public static final byte[] DEFAULT_COLUMN_FAMILY_BYTES = Bytes.toBytes(DEFAULT_COLUMN_FAMILY);
    public static final ImmutableBytesPtr DEFAULT_COLUMN_FAMILY_BYTES_PTR = new ImmutableBytesPtr(
            DEFAULT_COLUMN_FAMILY_BYTES);

    public static final String ALL_FAMILY_PROPERTIES_KEY = "";
    public static final String SYSTEM_TABLE_PK_NAME = "pk";

    public static final double MILLIS_TO_NANOS_CONVERTOR = Math.pow(10, 6);
    public static final BigDecimal BD_MILLIS_NANOS_CONVERSION = BigDecimal.valueOf(MILLIS_TO_NANOS_CONVERTOR);
    public static final BigDecimal BD_MILLIS_IN_DAY = BigDecimal.valueOf(QueryConstants.MILLIS_IN_DAY);
    public static final int MAX_ALLOWED_NANOS = 999999999;
    public static final int NANOS_IN_SECOND = BigDecimal.valueOf(Math.pow(10, 9)).intValue();
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
            VIEW_INDEX_ID + " SMALLINT,\n" +
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
            "CONSTRAINT " + SYSTEM_TABLE_PK_NAME + " PRIMARY KEY (" + TENANT_ID + ","
            + TABLE_SCHEM + "," + TABLE_NAME + "," + COLUMN_NAME + "," + COLUMN_FAMILY + "))\n" +
            HConstants.VERSIONS + "=" + MetaDataProtocol.DEFAULT_MAX_META_DATA_VERSIONS + ",\n" +
            HColumnDescriptor.KEEP_DELETED_CELLS + "="  + MetaDataProtocol.DEFAULT_META_DATA_KEEP_DELETED_CELLS + ",\n" +
            // Install split policy to prevent a tenant's metadata from being split across regions.
            HTableDescriptor.SPLIT_POLICY + "='" + MetaDataSplitPolicy.class.getName() + "'\n";

    public static final String CREATE_STATS_TABLE_METADATA =
            "CREATE TABLE " + SYSTEM_CATALOG_SCHEMA + ".\"" + SYSTEM_STATS_TABLE + "\"(\n" +
            // PK columns
            PHYSICAL_NAME  + " VARCHAR NOT NULL," +
            COLUMN_FAMILY + " VARCHAR," +
            REGION_NAME + " VARCHAR," +
            GUIDE_POSTS_COUNT + " BIGINT," +
            GUIDE_POSTS  + " VARBINARY," +
            GUIDE_POSTS_WIDTH + " BIGINT," +
            MIN_KEY + " VARBINARY," +
            MAX_KEY + " VARBINARY," +
            LAST_STATS_UPDATE_TIME+ " DATE, "+
            GUIDE_POSTS_ROW_COUNT+ " BIGINT, "+
            "CONSTRAINT " + SYSTEM_TABLE_PK_NAME + " PRIMARY KEY ("
            + PHYSICAL_NAME + ","
            + COLUMN_FAMILY + ","+ REGION_NAME+"))\n" +
            HConstants.VERSIONS + "=" + MetaDataProtocol.DEFAULT_MAX_STAT_DATA_VERSIONS + ",\n" +
            HColumnDescriptor.KEEP_DELETED_CELLS + "="  + MetaDataProtocol.DEFAULT_META_DATA_KEEP_DELETED_CELLS + ",\n" +
            // Install split policy to prevent a physical table's stats from being split across regions.
            HTableDescriptor.SPLIT_POLICY + "='" + MetaDataSplitPolicy.class.getName() + "'\n";

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
            HConstants.VERSIONS + "=" + MetaDataProtocol.DEFAULT_MAX_META_DATA_VERSIONS + ",\n" +
            HColumnDescriptor.KEEP_DELETED_CELLS + "="  + MetaDataProtocol.DEFAULT_META_DATA_KEEP_DELETED_CELLS + "\n";
}
