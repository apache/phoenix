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
package org.apache.phoenix.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparatorImpl;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.compile.ColumnProjector;
import org.apache.phoenix.compile.ExpressionProjector;
import org.apache.phoenix.compile.RowProjector;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.coprocessorclient.MetaDataProtocol;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.KeyValueColumnExpression;
import org.apache.phoenix.expression.LikeExpression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.expression.RowKeyColumnExpression;
import org.apache.phoenix.expression.StringBasedLikeExpression;
import org.apache.phoenix.hbase.index.util.VersionUtil;
import org.apache.phoenix.iterate.MaterializedResultIterator;
import org.apache.phoenix.iterate.ResultIterator;
import org.apache.phoenix.parse.LikeParseNode.LikeType;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PColumnImpl;
import org.apache.phoenix.schema.PDatum;
import org.apache.phoenix.schema.PNameFactory;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.RowKeyValueAccessor;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.tuple.MultiKeyValueTuple;
import org.apache.phoenix.schema.tuple.SingleKeyValueTuple;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PBoolean;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PSmallint;
import org.apache.phoenix.schema.types.PVarbinary;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.PhoenixKeyValueUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.StringUtil;

import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;

/**
 *
 * JDBC DatabaseMetaData implementation of Phoenix.
 * 
 */
public class PhoenixDatabaseMetaData implements DatabaseMetaData {
    public static final int FAMILY_NAME_INDEX = 4;
    public static final int COLUMN_NAME_INDEX = 3;
    public static final int TABLE_NAME_INDEX = 2;
    public static final int SCHEMA_NAME_INDEX = 1;
    public static final int TENANT_ID_INDEX = 0;

    public static final int TYPE_INDEX = 2;
    public static final int FUNTION_NAME_INDEX = 1;

    public static final String SYSTEM_CATALOG_SCHEMA = QueryConstants.SYSTEM_SCHEMA_NAME;
    public static final byte[] SYSTEM_CATALOG_SCHEMA_BYTES = QueryConstants.SYSTEM_SCHEMA_NAME_BYTES;
    public static final String SYSTEM_SCHEMA_NAME = QueryConstants.SYSTEM_SCHEMA_NAME;
    public static final byte[] SYSTEM_SCHEMA_NAME_BYTES = QueryConstants.SYSTEM_SCHEMA_NAME_BYTES;
    public static final TableName SYSTEM_SCHEMA_HBASE_TABLE_NAME = TableName.valueOf(SYSTEM_SCHEMA_NAME);

    public static final String SYSTEM_CATALOG_TABLE = "CATALOG";
    public static final byte[] SYSTEM_CATALOG_TABLE_BYTES = Bytes.toBytes(SYSTEM_CATALOG_TABLE);
    public static final String SYSTEM_CATALOG = SYSTEM_CATALOG_SCHEMA + ".\"" + SYSTEM_CATALOG_TABLE + "\"";
    public static final String SYSTEM_CATALOG_NAME = SchemaUtil.getTableName(SYSTEM_CATALOG_SCHEMA,
            SYSTEM_CATALOG_TABLE);
    public static final String OLD_SYSTEM_CATALOG_NAME = SYSTEM_CATALOG_NAME.replace(QueryConstants.NAME_SEPARATOR, QueryConstants.NAMESPACE_SEPARATOR);
    public static final TableName SYSTEM_CATALOG_HBASE_TABLE_NAME = TableName.valueOf(SYSTEM_CATALOG_NAME);
    public static final byte[] SYSTEM_CATALOG_NAME_BYTES = Bytes.toBytes(SYSTEM_CATALOG_NAME);
    public static final String SYSTEM_STATS_TABLE = "STATS";
    public static final String SYSTEM_STATS_NAME = SchemaUtil.getTableName(SYSTEM_CATALOG_SCHEMA, SYSTEM_STATS_TABLE);
    public static final String IS_NAMESPACE_MAPPED = "IS_NAMESPACE_MAPPED";
    public static final byte[] IS_NAMESPACE_MAPPED_BYTES = Bytes.toBytes(IS_NAMESPACE_MAPPED);
    public static final byte[] SYSTEM_STATS_NAME_BYTES = Bytes.toBytes(SYSTEM_STATS_NAME);
    public static final byte[] SYSTEM_STATS_TABLE_BYTES = Bytes.toBytes(SYSTEM_STATS_TABLE);
    public static final TableName SYSTEM_STATS_HBASE_TABLE_NAME = TableName.valueOf(SYSTEM_STATS_NAME);
    public static final String SYSTEM_CATALOG_ALIAS = "\"SYSTEM.TABLE\"";

    public static final byte[] SYSTEM_SEQUENCE_FAMILY_BYTES = QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES;
    public static final String SYSTEM_SEQUENCE_SCHEMA = SYSTEM_CATALOG_SCHEMA;
    public static final byte[] SYSTEM_SEQUENCE_SCHEMA_BYTES = Bytes.toBytes(SYSTEM_SEQUENCE_SCHEMA);
    public static final String SYSTEM_SEQUENCE_TABLE = "SEQUENCE";
    public static final byte[] SYSTEM_SEQUENCE_TABLE_BYTES = Bytes.toBytes(SYSTEM_SEQUENCE_TABLE);
    public static final String SYSTEM_SEQUENCE = SYSTEM_CATALOG_SCHEMA + ".\"" + SYSTEM_SEQUENCE_TABLE + "\"";
    public static final String SYSTEM_SEQUENCE_NAME = SchemaUtil.getTableName(SYSTEM_SEQUENCE_SCHEMA, SYSTEM_SEQUENCE_TABLE);
    public static final byte[] SYSTEM_SEQUENCE_NAME_BYTES = Bytes.toBytes(SYSTEM_SEQUENCE_NAME);
    public static final TableName SYSTEM_SEQUENCE_HBASE_TABLE_NAME = TableName.valueOf(SYSTEM_SEQUENCE_NAME);
    
    public static final String TABLE_NAME = "TABLE_NAME";
    public static final byte[] TABLE_NAME_BYTES = Bytes.toBytes(TABLE_NAME);
    public static final String TABLE_TYPE = "TABLE_TYPE";
    public static final byte[] TABLE_TYPE_BYTES = Bytes.toBytes(TABLE_TYPE);
    public static final String PHYSICAL_NAME = "PHYSICAL_NAME";
    public static final byte[] PHYSICAL_NAME_BYTES = Bytes.toBytes(PHYSICAL_NAME);

    public static final String COLUMN_FAMILY = "COLUMN_FAMILY";
    public static final byte[] COLUMN_FAMILY_BYTES = Bytes.toBytes(COLUMN_FAMILY);
    public static final String TABLE_CAT = "TABLE_CAT";
    public static final String TABLE_CATALOG = "TABLE_CATALOG";
    public static final String TABLE_SCHEM = "TABLE_SCHEM";
    public static final String LOGICAL_TABLE_NAME = "LOGICAL_TABLE_NAME";
    public static final String LOGICAL_PARENT_NAME = "LOGICAL_PARENT_NAME";
    public static final String REMARKS = "REMARKS";
    public static final String TYPE_SCHEM = "TYPE_SCHEM";
    public static final String SELF_REFERENCING_COL_NAME = "SELF_REFERENCING_COL_NAME";
    public static final String REF_GENERATION = "REF_GENERATION";
    public static final String PK_NAME = "PK_NAME";
    public static final byte[] PK_NAME_BYTES = Bytes.toBytes(PK_NAME);
    public static final String TABLE_SEQ_NUM = "TABLE_SEQ_NUM";
    public static final byte[] TABLE_SEQ_NUM_BYTES = Bytes.toBytes(TABLE_SEQ_NUM);
    public static final String COLUMN_COUNT = "COLUMN_COUNT";
    public static final byte[] COLUMN_COUNT_BYTES = Bytes.toBytes(COLUMN_COUNT);
    public static final String SALT_BUCKETS = "SALT_BUCKETS";
    public static final byte[] SALT_BUCKETS_BYTES = Bytes.toBytes(SALT_BUCKETS);
    public static final String STORE_NULLS = "STORE_NULLS";
    public static final byte[] STORE_NULLS_BYTES = Bytes.toBytes(STORE_NULLS);

    public static final String DATA_TABLE_NAME = "DATA_TABLE_NAME";
    public static final byte[] DATA_TABLE_NAME_BYTES = Bytes.toBytes(DATA_TABLE_NAME);
    public static final String INDEX_STATE = "INDEX_STATE";
    public static final byte[] INDEX_STATE_BYTES = Bytes.toBytes(INDEX_STATE);

    public static final String TENANT_ID = "TENANT_ID";
    public static final byte[] TENANT_ID_BYTES = Bytes.toBytes(TENANT_ID);

    public static final String COLUMN_NAME = "COLUMN_NAME";
    public static final String DATA_TYPE = "DATA_TYPE";
    public static final byte[] DATA_TYPE_BYTES = Bytes.toBytes(DATA_TYPE);
    public static final String TYPE_NAME = "TYPE_NAME";
    public static final String COLUMN_SIZE = "COLUMN_SIZE";
    public static final byte[] COLUMN_SIZE_BYTES = Bytes.toBytes(COLUMN_SIZE);
    public static final String BUFFER_LENGTH = "BUFFER_LENGTH";
    public static final String DECIMAL_DIGITS = "DECIMAL_DIGITS";
    public static final byte[] DECIMAL_DIGITS_BYTES = Bytes.toBytes(DECIMAL_DIGITS);
    public static final String NUM_PREC_RADIX = "NUM_PREC_RADIX";
    public static final String NULLABLE = "NULLABLE";
    public static final byte[] NULLABLE_BYTES = Bytes.toBytes(NULLABLE);
    public static final String COLUMN_DEF = "COLUMN_DEF";
    public static final byte[] COLUMN_DEF_BYTES = Bytes.toBytes(COLUMN_DEF);
    public static final String SQL_DATA_TYPE = "SQL_DATA_TYPE";
    public static final String SQL_DATETIME_SUB = "SQL_DATETIME_SUB";
    public static final String CHAR_OCTET_LENGTH = "CHAR_OCTET_LENGTH";
    public static final String ORDINAL_POSITION = "ORDINAL_POSITION";
    public static final byte[] ORDINAL_POSITION_BYTES = Bytes.toBytes(ORDINAL_POSITION);
    public static final String IS_NULLABLE = "IS_NULLABLE";
    public static final String SCOPE_CATALOG = "SCOPE_CATALOG";
    public static final String SCOPE_SCHEMA = "SCOPE_SCHEMA";
    public static final String SCOPE_TABLE = "SCOPE_TABLE";
    public static final String SOURCE_DATA_TYPE = "SOURCE_DATA_TYPE";
    public static final String IS_AUTOINCREMENT = "IS_AUTOINCREMENT";
    public static final String SORT_ORDER = "SORT_ORDER";
    public static final byte[] SORT_ORDER_BYTES = Bytes.toBytes(SORT_ORDER);
    public static final String IMMUTABLE_ROWS = "IMMUTABLE_ROWS";
    public static final byte[] IMMUTABLE_ROWS_BYTES = Bytes.toBytes(IMMUTABLE_ROWS);
    public static final String DEFAULT_COLUMN_FAMILY_NAME = "DEFAULT_COLUMN_FAMILY";
    public static final byte[] DEFAULT_COLUMN_FAMILY_NAME_BYTES = Bytes.toBytes(DEFAULT_COLUMN_FAMILY_NAME);
    public static final String VIEW_STATEMENT = "VIEW_STATEMENT";
    public static final byte[] VIEW_STATEMENT_BYTES = Bytes.toBytes(VIEW_STATEMENT);
    public static final String DISABLE_WAL = "DISABLE_WAL";
    public static final byte[] DISABLE_WAL_BYTES = Bytes.toBytes(DISABLE_WAL);
    public static final String MULTI_TENANT = "MULTI_TENANT";
    public static final byte[] MULTI_TENANT_BYTES = Bytes.toBytes(MULTI_TENANT);
    public static final String VIEW_TYPE = "VIEW_TYPE";
    public static final byte[] VIEW_TYPE_BYTES = Bytes.toBytes(VIEW_TYPE);
    public static final String INDEX_TYPE = "INDEX_TYPE";
    public static final byte[] INDEX_TYPE_BYTES = Bytes.toBytes(INDEX_TYPE);
    public static final String LINK_TYPE = "LINK_TYPE";
    public static final byte[] LINK_TYPE_BYTES = Bytes.toBytes(LINK_TYPE);
    public static final String TASK_TYPE = "TASK_TYPE";
    public static final byte[] TASK_TYPE_BYTES = Bytes.toBytes(TASK_TYPE);
    public static final String TASK_TS = "TASK_TS";
    public static final byte[] TASK_TS_BYTES = Bytes.toBytes(TASK_TS);
    public static final String TASK_STATUS = "TASK_STATUS";
    public static final String TASK_END_TS = "TASK_END_TS";
    public static final String TASK_PRIORITY = "TASK_PRIORITY";
    public static final String TASK_DATA = "TASK_DATA";
    public static final String TASK_TABLE_TTL = "864000";
    public static final String NEW_PHYS_TABLE_NAME = "NEW_PHYS_TABLE_NAME";
    public static final String TRANSFORM_TYPE = "TRANSFORM_TYPE";
    public static final String TRANSFORM_STATUS = "STATUS";
    public static final String TRANSFORM_JOB_ID = "JOB_ID";
    public static final String TRANSFORM_RETRY_COUNT = "RETRY_COUNT";
    public static final String TRANSFORM_START_TS = "START_TS";
    public static final String TRANSFORM_LAST_STATE_TS = "END_TS";
    public static final String OLD_METADATA = "OLD_METADATA";
    public static final String NEW_METADATA = "NEW_METADATA";
    public static final String TRANSFORM_FUNCTION = "TRANSFORM_FUNCTION";
    public static final String TRANSFORM_TABLE_TTL = "7776000"; // 90 days

    public static final int TTL_FOR_MUTEX = 15 * 60; // 15min
    public static final String ARRAY_SIZE = "ARRAY_SIZE";
    public static final byte[] ARRAY_SIZE_BYTES = Bytes.toBytes(ARRAY_SIZE);
    public static final String VIEW_CONSTANT = "VIEW_CONSTANT";
    public static final byte[] VIEW_CONSTANT_BYTES = Bytes.toBytes(VIEW_CONSTANT);
    public static final String IS_VIEW_REFERENCED = "IS_VIEW_REFERENCED";
    public static final byte[] IS_VIEW_REFERENCED_BYTES = Bytes.toBytes(IS_VIEW_REFERENCED);
    public static final String VIEW_INDEX_ID = "VIEW_INDEX_ID";
    public static final byte[] VIEW_INDEX_ID_BYTES = Bytes.toBytes(VIEW_INDEX_ID);
    public static final String VIEW_INDEX_ID_DATA_TYPE = "VIEW_INDEX_ID_DATA_TYPE";
    public static final byte[] VIEW_INDEX_ID_DATA_TYPE_BYTES = Bytes.toBytes(VIEW_INDEX_ID_DATA_TYPE);
    public static final String BASE_COLUMN_COUNT = "BASE_COLUMN_COUNT";
    public static final byte[] BASE_COLUMN_COUNT_BYTES = Bytes.toBytes(BASE_COLUMN_COUNT);
    public static final String IS_ROW_TIMESTAMP = "IS_ROW_TIMESTAMP";
    public static final byte[] IS_ROW_TIMESTAMP_BYTES = Bytes.toBytes(IS_ROW_TIMESTAMP);
    
    public static final String TABLE_FAMILY = QueryConstants.DEFAULT_COLUMN_FAMILY;
    public static final byte[] TABLE_FAMILY_BYTES = QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES;
    public static final byte[] PENDING_DISABLE_COUNT_BYTES = Bytes.toBytes("PENDING_DISABLE_COUNT");

    public static final String TYPE_SEQUENCE = "SEQUENCE";
    public static final String SYSTEM_FUNCTION_TABLE = "FUNCTION";
    public static final String SYSTEM_FUNCTION = SYSTEM_CATALOG_SCHEMA + QueryConstants.NAME_SEPARATOR + "\"FUNCTION\"";
    public static final String SYSTEM_FUNCTION_NAME = SchemaUtil.getTableName(SYSTEM_CATALOG_SCHEMA, SYSTEM_FUNCTION_TABLE);
    public static final byte[] SYSTEM_FUNCTION_NAME_BYTES = Bytes.toBytes(SYSTEM_FUNCTION_NAME);
    public static final TableName SYSTEM_FUNCTION_HBASE_TABLE_NAME = TableName.valueOf(SYSTEM_FUNCTION_NAME);

    public static final String FUNCTION_NAME = "FUNCTION_NAME";
    public static final byte[] FUNCTION_NAME_BYTES = Bytes.toBytes(FUNCTION_NAME);
    public static final String CLASS_NAME = "CLASS_NAME";
    public static final byte[] CLASS_NAME_BYTES = Bytes.toBytes(CLASS_NAME);
    public static final String JAR_PATH = "JAR_PATH";
    public static final byte[] JAR_PATH_BYTES = Bytes.toBytes(JAR_PATH);
    public static final String TYPE = "TYPE";
    public static final byte[] TYPE_BYTES = Bytes.toBytes(TYPE);
    public static final String ARG_POSITION = "ARG_POSITION";
    public static final byte[] ARG_POSITION_TYPE = Bytes.toBytes(ARG_POSITION);
    public static final String RETURN_TYPE = "RETURN_TYPE";
    public static final byte[] RETURN_TYPE_BYTES = Bytes.toBytes(RETURN_TYPE);
    public static final String IS_ARRAY = "IS_ARRAY";
    public static final byte[] IS_ARRAY_BYTES = Bytes.toBytes(IS_ARRAY);
    public static final String IS_CONSTANT = "IS_CONSTANT";
    public static final byte[] IS_CONSTANT_BYTES = Bytes.toBytes(IS_CONSTANT);
    public static final String DEFAULT_VALUE = "DEFAULT_VALUE";
    public static final byte[] DEFAULT_VALUE_BYTES = Bytes.toBytes(DEFAULT_VALUE);
    public static final String NUM_ARGS = "NUM_ARGS";
    public static final byte[] NUM_ARGS_BYTES = Bytes.toBytes(NUM_ARGS);
    
    public static final String SEQUENCE_SCHEMA = "SEQUENCE_SCHEMA";
    public static final String SEQUENCE_NAME = "SEQUENCE_NAME";
    public static final String CURRENT_VALUE = "CURRENT_VALUE";
    public static final byte[] CURRENT_VALUE_BYTES = Bytes.toBytes(CURRENT_VALUE);
    public static final String START_WITH = "START_WITH";
    public static final byte[] START_WITH_BYTES = Bytes.toBytes(START_WITH);
    // MIN_VALUE, MAX_VALUE, CYCLE_FLAG and LIMIT_FLAG were added in 3.1/4.1
    public static final String MIN_VALUE = "MIN_VALUE";
    public static final byte[] MIN_VALUE_BYTES = Bytes.toBytes(MIN_VALUE);
    public static final String MAX_VALUE = "MAX_VALUE";
    public static final byte[] MAX_VALUE_BYTES = Bytes.toBytes(MAX_VALUE);
    public static final String INCREMENT_BY = "INCREMENT_BY";
    public static final byte[] INCREMENT_BY_BYTES = Bytes.toBytes(INCREMENT_BY);
    public static final String CACHE_SIZE = "CACHE_SIZE";
    public static final byte[] CACHE_SIZE_BYTES = Bytes.toBytes(CACHE_SIZE);
    public static final String CYCLE_FLAG = "CYCLE_FLAG";
    public static final byte[] CYCLE_FLAG_BYTES = Bytes.toBytes(CYCLE_FLAG);
    public static final String LIMIT_REACHED_FLAG = "LIMIT_REACHED_FLAG";
    public static final byte[] LIMIT_REACHED_FLAG_BYTES = Bytes.toBytes(LIMIT_REACHED_FLAG);
    public static final String KEY_SEQ = "KEY_SEQ";
    public static final byte[] KEY_SEQ_BYTES = Bytes.toBytes(KEY_SEQ);
    public static final String SUPERTABLE_NAME = "SUPERTABLE_NAME";

    public static final String TYPE_ID = "TYPE_ID";
    public static final String INDEX_DISABLE_TIMESTAMP = "INDEX_DISABLE_TIMESTAMP";
    public static final byte[] INDEX_DISABLE_TIMESTAMP_BYTES = Bytes.toBytes(INDEX_DISABLE_TIMESTAMP);

    public static final String REGION_NAME = "REGION_NAME";
    public static final byte[] REGION_NAME_BYTES = Bytes.toBytes(REGION_NAME);
    public static final String GUIDE_POSTS = "GUIDE_POSTS";
    public static final byte[] GUIDE_POSTS_BYTES = Bytes.toBytes(GUIDE_POSTS);
    public static final String GUIDE_POSTS_COUNT = "GUIDE_POSTS_COUNT";
    public static final byte[] GUIDE_POSTS_COUNT_BYTES = Bytes.toBytes(GUIDE_POSTS_COUNT);
    public static final String GUIDE_POSTS_WIDTH = "GUIDE_POSTS_WIDTH";
    public static final byte[] GUIDE_POSTS_WIDTH_BYTES = Bytes.toBytes(GUIDE_POSTS_WIDTH);
    public static final String GUIDE_POSTS_ROW_COUNT = "GUIDE_POSTS_ROW_COUNT";
    public static final byte[] GUIDE_POSTS_ROW_COUNT_BYTES = Bytes.toBytes(GUIDE_POSTS_ROW_COUNT);
    public static final String MIN_KEY = "MIN_KEY";
    public static final byte[] MIN_KEY_BYTES = Bytes.toBytes(MIN_KEY);
    public static final String MAX_KEY = "MAX_KEY";
    public static final byte[] MAX_KEY_BYTES = Bytes.toBytes(MAX_KEY);
    public static final String LAST_STATS_UPDATE_TIME = "LAST_STATS_UPDATE_TIME";
    public static final byte[] LAST_STATS_UPDATE_TIME_BYTES = Bytes.toBytes(LAST_STATS_UPDATE_TIME);
    public static final String GUIDE_POST_KEY = "GUIDE_POST_KEY";
    public static final String ASYNC_REBUILD_TIMESTAMP = "ASYNC_REBUILD_TIMESTAMP";
    public static final byte[] ASYNC_REBUILD_TIMESTAMP_BYTES = Bytes.toBytes(ASYNC_REBUILD_TIMESTAMP);

    public static final String COLUMN_ENCODED_BYTES = "COLUMN_ENCODED_BYTES";

    public static final String PARENT_TENANT_ID = "PARENT_TENANT_ID";
    public static final byte[] PARENT_TENANT_ID_BYTES = Bytes.toBytes(PARENT_TENANT_ID);

    private static final String TENANT_POS_SHIFT = "TENANT_POS_SHIFT";
    private static final byte[] TENANT_POS_SHIFT_BYTES = Bytes.toBytes(TENANT_POS_SHIFT);

    public static final String TRANSACTIONAL = "TRANSACTIONAL";
    public static final byte[] TRANSACTIONAL_BYTES = Bytes.toBytes(TRANSACTIONAL);

    public static final String TRANSACTION_PROVIDER = "TRANSACTION_PROVIDER";
    public static final byte[] TRANSACTION_PROVIDER_BYTES = Bytes.toBytes(TRANSACTION_PROVIDER);

    public static final String PHYSICAL_TABLE_NAME = "PHYSICAL_TABLE_NAME";
    public static final byte[] PHYSICAL_TABLE_NAME_BYTES = Bytes.toBytes(PHYSICAL_TABLE_NAME);

    public static final String UPDATE_CACHE_FREQUENCY = "UPDATE_CACHE_FREQUENCY";
    public static final byte[] UPDATE_CACHE_FREQUENCY_BYTES = Bytes.toBytes(UPDATE_CACHE_FREQUENCY);

    public static final String AUTO_PARTITION_SEQ = "AUTO_PARTITION_SEQ";
    public static final byte[] AUTO_PARTITION_SEQ_BYTES = Bytes.toBytes(AUTO_PARTITION_SEQ);
    
    public static final String APPEND_ONLY_SCHEMA = "APPEND_ONLY_SCHEMA";
    public static final byte[] APPEND_ONLY_SCHEMA_BYTES = Bytes.toBytes(APPEND_ONLY_SCHEMA);
    
    public static final String ASYNC_CREATED_DATE = "ASYNC_CREATED_DATE";
    public static final String SEQUENCE_TABLE_TYPE = SYSTEM_SEQUENCE_TABLE;

    public static final String SYNC_INDEX_CREATED_DATE = "SYNC_INDEX_CREATED_DATE";
    public static final String SYSTEM_MUTEX_COLUMN_NAME = "MUTEX_VALUE";
    public static final byte[] SYSTEM_MUTEX_COLUMN_NAME_BYTES = Bytes.toBytes(SYSTEM_MUTEX_COLUMN_NAME);
    public static final String SYSTEM_MUTEX_TABLE_NAME = "MUTEX";
    public static final String SYSTEM_MUTEX_NAME = SchemaUtil.getTableName(QueryConstants.SYSTEM_SCHEMA_NAME, SYSTEM_MUTEX_TABLE_NAME);
    public static final TableName SYSTEM_MUTEX_HBASE_TABLE_NAME = TableName.valueOf(SYSTEM_MUTEX_NAME);
    public static final byte[] SYSTEM_MUTEX_NAME_BYTES = Bytes.toBytes(SYSTEM_MUTEX_NAME);
    public static final byte[] SYSTEM_MUTEX_FAMILY_NAME_BYTES = TABLE_FAMILY_BYTES;
    
    private final PhoenixConnection connection;

    public static final int MAX_LOCAL_SI_VERSION_DISALLOW = VersionUtil.encodeVersion("0", "98", "8");
    public static final int MIN_LOCAL_SI_VERSION_DISALLOW = VersionUtil.encodeVersion("0", "98", "6");
    public static final int MIN_RENEW_LEASE_VERSION = VersionUtil.encodeVersion("1", "1", "3");
    public static final int MIN_NAMESPACE_MAPPED_PHOENIX_VERSION = VersionUtil.encodeVersion("4", "8", "0");
    public static final int MIN_PENDING_ACTIVE_INDEX = VersionUtil.encodeVersion("4", "12", "0");
    public static final int MIN_CLIENT_RETRY_INDEX_WRITES = VersionUtil.encodeVersion("4", "14", "0");
    public static final int MIN_TX_CLIENT_SIDE_MAINTENANCE = VersionUtil.encodeVersion("4", "14", "0");
    public static final int MIN_PENDING_DISABLE_INDEX = VersionUtil.encodeVersion("4", "14", "0");

    // Version below which we should turn off essential column family.
    public static final int ESSENTIAL_FAMILY_VERSION_THRESHOLD = VersionUtil.encodeVersion("0", "94", "7");
    // Version below which we should disallow usage of mutable secondary indexing.
    public static final int MUTABLE_SI_VERSION_THRESHOLD = VersionUtil.encodeVersion("0", "94", "10");
    /** Version below which we fall back on the generic KeyValueBuilder */
    public static final int CLIENT_KEY_VALUE_BUILDER_THRESHOLD = VersionUtil.encodeVersion("0", "94", "14");

    public static final String IMMUTABLE_STORAGE_SCHEME = "IMMUTABLE_STORAGE_SCHEME";
    public static final byte[] STORAGE_SCHEME_BYTES = Bytes.toBytes(IMMUTABLE_STORAGE_SCHEME);
    public static final String ENCODING_SCHEME = "ENCODING_SCHEME";
    public static final byte[] ENCODING_SCHEME_BYTES = Bytes.toBytes(ENCODING_SCHEME);
    public static final String COLUMN_QUALIFIER = "COLUMN_QUALIFIER";
    public static final byte[] COLUMN_QUALIFIER_BYTES = Bytes.toBytes(COLUMN_QUALIFIER);
    public static final String COLUMN_QUALIFIER_COUNTER = "QUALIFIER_COUNTER";
    public static final byte[] COLUMN_QUALIFIER_COUNTER_BYTES = Bytes.toBytes(COLUMN_QUALIFIER_COUNTER);
    public static final String USE_STATS_FOR_PARALLELIZATION = "USE_STATS_FOR_PARALLELIZATION";
    public static final byte[] USE_STATS_FOR_PARALLELIZATION_BYTES = Bytes.toBytes(USE_STATS_FOR_PARALLELIZATION);

    // The PHOENIX_TTL property will hold the duration after which rows will be marked as expired.
    public static final long PHOENIX_TTL_NOT_DEFINED = 0L;
    public static final String PHOENIX_TTL = "PHOENIX_TTL";
    public static final byte[] PHOENIX_TTL_BYTES = Bytes.toBytes(PHOENIX_TTL);
    // The phoenix ttl high watermark if set indicates the timestamp used for determining the expired rows.
    // otherwise the now() - ttl-duration is the timestamp used.
    public static final long MIN_PHOENIX_TTL_HWM = 0L;
    public static final String PHOENIX_TTL_HWM = "PHOENIX_TTL_HWM";
    public static final byte[] PHOENIX_TTL_HWM_BYTES = Bytes.toBytes(PHOENIX_TTL_HWM);

    public static final String LAST_DDL_TIMESTAMP = "LAST_DDL_TIMESTAMP";
    public static final byte[] LAST_DDL_TIMESTAMP_BYTES = Bytes.toBytes(LAST_DDL_TIMESTAMP);

    public static final String CHANGE_DETECTION_ENABLED = "CHANGE_DETECTION_ENABLED";
    public static final byte[] CHANGE_DETECTION_ENABLED_BYTES =
        Bytes.toBytes(CHANGE_DETECTION_ENABLED);

    public static final String SCHEMA_VERSION = "SCHEMA_VERSION";
    public static final byte[] SCHEMA_VERSION_BYTES = Bytes.toBytes(SCHEMA_VERSION);

    public static final String EXTERNAL_SCHEMA_ID = "EXTERNAL_SCHEMA_ID";
    public static final byte[] EXTERNAL_SCHEMA_ID_BYTES = Bytes.toBytes(EXTERNAL_SCHEMA_ID);

    public static final String STREAMING_TOPIC_NAME = "STREAMING_TOPIC_NAME";
    public static final byte[] STREAMING_TOPIC_NAME_BYTES = Bytes.toBytes(STREAMING_TOPIC_NAME);

    public static final String INDEX_WHERE = "INDEX_WHERE";
    public static final byte[] INDEX_WHERE_BYTES = Bytes.toBytes(INDEX_WHERE);

    public static final String SYSTEM_CHILD_LINK_TABLE = "CHILD_LINK";
    public static final String SYSTEM_CHILD_LINK_NAME = SchemaUtil.getTableName(SYSTEM_CATALOG_SCHEMA, SYSTEM_CHILD_LINK_TABLE);
    public static final byte[] SYSTEM_CHILD_LINK_NAME_BYTES = Bytes.toBytes(SYSTEM_CHILD_LINK_NAME);
    public static final byte[] SYSTEM_CHILD_LINK_NAMESPACE_BYTES =
        SchemaUtil.getPhysicalTableName(SYSTEM_CHILD_LINK_NAME_BYTES, true).getName();
    public static final TableName SYSTEM_LINK_HBASE_TABLE_NAME = TableName.valueOf(SYSTEM_CHILD_LINK_NAME);

    public static final String SYSTEM_TASK_TABLE = "TASK";
    public static final String SYSTEM_TASK_NAME = SchemaUtil.getTableName(SYSTEM_CATALOG_SCHEMA, SYSTEM_TASK_TABLE);
    public static final byte[] SYSTEM_TASK_NAME_BYTES = Bytes.toBytes(SYSTEM_TASK_NAME);
    public static final TableName SYSTEM_TASK_HBASE_TABLE_NAME = TableName.valueOf(SYSTEM_TASK_NAME);

    public static final String SYSTEM_TRANSFORM_TABLE = "TRANSFORM";
    public static final String SYSTEM_TRANSFORM_NAME = SchemaUtil.getTableName(SYSTEM_CATALOG_SCHEMA, SYSTEM_TRANSFORM_TABLE);

    //SYSTEM:LOG
    public static final String SYSTEM_LOG_TABLE = "LOG";
    public static final String SYSTEM_LOG_NAME =
            SchemaUtil.getTableName(SYSTEM_CATALOG_SCHEMA, SYSTEM_LOG_TABLE);
    public static final String QUERY_ID = "QUERY_ID";
    public static final String USER = "USER";
    public static final String CLIENT_IP = "CLIENT_IP";
    public static final String QUERY = "QUERY";
    public static final String EXPLAIN_PLAN = "EXPLAIN_PLAN";
    public static final String TOTAL_EXECUTION_TIME = "TOTAL_EXECUTION_TIME";
    public static final String NO_OF_RESULTS_ITERATED = "NO_OF_RESULTS_ITERATED";
    public static final String QUERY_STATUS = "QUERY_STATUS";
    public static final String EXCEPTION_TRACE = "EXCEPTION_TRACE";
    public static final String GLOBAL_SCAN_DETAILS = "GLOBAL_SCAN_DETAILS";
    public static final String SCAN_METRICS_JSON = "SCAN_METRICS_JSON";
    public static final String START_TIME = "START_TIME";
    public static final String BIND_PARAMETERS = "BIND_PARAMETERS";
            
    
    PhoenixDatabaseMetaData(PhoenixConnection connection) throws SQLException {
        this.connection = connection;
    }

    private PhoenixResultSet getEmptyResultSet() throws SQLException {
        PhoenixStatement stmt = new PhoenixStatement(connection);
        stmt.closeOnCompletion();
        return new PhoenixResultSet(ResultIterator.EMPTY_ITERATOR, RowProjector.EMPTY_PROJECTOR, new StatementContext(stmt, false));
    }

    @Override
    public boolean allProceduresAreCallable() throws SQLException {
        return false;
    }

    @Override
    public boolean allTablesAreSelectable() throws SQLException {
        return true;
    }

    @Override
    public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
        return false;
    }

    @Override
    public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean deletesAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
        return false;
    }

    @Override
    public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern,
            String attributeNamePattern) throws SQLException {
        return getEmptyResultSet();
    }

    @Override
    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable)
            throws SQLException {
        return getEmptyResultSet();
    }

    @Override
    public String getCatalogSeparator() throws SQLException {
        return ".";
    }

    @Override
    public String getCatalogTerm() throws SQLException {
        return "Tenant";
    }

    @Override
    public ResultSet getCatalogs() throws SQLException {
        PreparedStatement stmt = QueryUtil.getCatalogsStmt(connection);
        stmt.closeOnCompletion();
        return stmt.executeQuery();
    }

    @Override
    public ResultSet getClientInfoProperties() throws SQLException {
        return getEmptyResultSet();
    }

    @Override
    public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern)
            throws SQLException {
        return getEmptyResultSet();
    }

    public static final String GLOBAL_TENANANTS_ONLY = "null";


    private static void appendConjunction(StringBuilder buf) {
        buf.append(buf.length() == 0 ? "" : " and ");
    }
    
    // While creating the PColumns we don't care about the ordinal positiion so set it to 1
    private static final PColumnImpl TENANT_ID_COLUMN = new PColumnImpl(PNameFactory.newName(TENANT_ID),
            PNameFactory.newName(TABLE_FAMILY_BYTES), PVarchar.INSTANCE, null, null, false, 1, SortOrder.getDefault(),
            0, null, false, null, false, false, DATA_TYPE_BYTES, HConstants.LATEST_TIMESTAMP);
    private static final PColumnImpl TABLE_SCHEM_COLUMN = new PColumnImpl(PNameFactory.newName(TABLE_SCHEM),
            PNameFactory.newName(TABLE_FAMILY_BYTES), PVarchar.INSTANCE, null, null, false, 1, SortOrder.getDefault(),
            0, null, false, null, false, false, DATA_TYPE_BYTES, HConstants.LATEST_TIMESTAMP);
    private static final PColumnImpl TABLE_NAME_COLUMN = new PColumnImpl(PNameFactory.newName(TABLE_NAME),
            PNameFactory.newName(TABLE_FAMILY_BYTES), PVarchar.INSTANCE, null, null, false, 1, SortOrder.getDefault(),
            0, null, false, null, false, false, DATA_TYPE_BYTES, HConstants.LATEST_TIMESTAMP);
    private static final PColumnImpl COLUMN_NAME_COLUMN = new PColumnImpl(PNameFactory.newName(COLUMN_NAME),
            PNameFactory.newName(TABLE_FAMILY_BYTES), PVarchar.INSTANCE, null, null, false, 1, SortOrder.getDefault(),
            0, null, false, null, false, false, DATA_TYPE_BYTES, HConstants.LATEST_TIMESTAMP);
    private static final PColumnImpl DATA_TYPE_COLUMN = new PColumnImpl(PNameFactory.newName(DATA_TYPE),
            PNameFactory.newName(TABLE_FAMILY_BYTES), PInteger.INSTANCE, null, null, false, 1, SortOrder.getDefault(),
            0, null, false, null, false, false, DATA_TYPE_BYTES, HConstants.LATEST_TIMESTAMP);
    private static final PColumnImpl TYPE_NAME_COLUMN = new PColumnImpl(PNameFactory.newName(TYPE_NAME),
            PNameFactory.newName(TABLE_FAMILY_BYTES), PVarchar.INSTANCE, null, null, false, 1, SortOrder.getDefault(),
            0, null, false, null, false, false, Bytes.toBytes(TYPE_NAME), HConstants.LATEST_TIMESTAMP);
    private static final PColumnImpl COLUMN_SIZE_COLUMN = new PColumnImpl(PNameFactory.newName(COLUMN_SIZE),
            PNameFactory.newName(TABLE_FAMILY_BYTES), PInteger.INSTANCE, null, null, false, 1, SortOrder.getDefault(),
            0, null, false, null, false, false, COLUMN_SIZE_BYTES, HConstants.LATEST_TIMESTAMP);
    private static final PColumnImpl BUFFER_LENGTH_COLUMN = new PColumnImpl(PNameFactory.newName(BUFFER_LENGTH),
            PNameFactory.newName(TABLE_FAMILY_BYTES), PInteger.INSTANCE, null, null, false, 1, SortOrder.getDefault(),
            0, null, false, null, false, false, Bytes.toBytes(BUFFER_LENGTH), HConstants.LATEST_TIMESTAMP);
    private static final PColumnImpl DECIMAL_DIGITS_COLUMN = new PColumnImpl(PNameFactory.newName(DECIMAL_DIGITS),
            PNameFactory.newName(TABLE_FAMILY_BYTES), PInteger.INSTANCE, null, null, false, 1, SortOrder.getDefault(),
            0, null, false, null, false, false, DECIMAL_DIGITS_BYTES, HConstants.LATEST_TIMESTAMP);
    private static final PColumnImpl NUM_PREC_RADIX_COLUMN = new PColumnImpl(PNameFactory.newName(NUM_PREC_RADIX),
            PNameFactory.newName(TABLE_FAMILY_BYTES), PInteger.INSTANCE, null, null, false, 1, SortOrder.getDefault(),
            0, null, false, null, false, false, Bytes.toBytes(NUM_PREC_RADIX), HConstants.LATEST_TIMESTAMP);
    private static final PColumnImpl NULLABLE_COLUMN = new PColumnImpl(PNameFactory.newName(NULLABLE),
            PNameFactory.newName(TABLE_FAMILY_BYTES), PInteger.INSTANCE, null, null, false, 1, SortOrder.getDefault(),
            0, null, false, null, false, false, NULLABLE_BYTES, HConstants.LATEST_TIMESTAMP);
    private static final PColumnImpl REMARKS_COLUMN = new PColumnImpl(PNameFactory.newName(REMARKS),
            PNameFactory.newName(TABLE_FAMILY_BYTES), PVarchar.INSTANCE, null, null, false, 1, SortOrder.getDefault(),
            0, null, false, null, false, false, Bytes.toBytes(REMARKS), HConstants.LATEST_TIMESTAMP);
    private static final PColumnImpl COLUMN_DEF_COLUMN = new PColumnImpl(PNameFactory.newName(COLUMN_DEF),
            PNameFactory.newName(TABLE_FAMILY_BYTES), PVarchar.INSTANCE, null, null, false, 1, SortOrder.getDefault(),
            0, null, false, null, false, false, Bytes.toBytes(COLUMN_DEF), HConstants.LATEST_TIMESTAMP);
    private static final PColumnImpl SQL_DATA_TYPE_COLUMN = new PColumnImpl(PNameFactory.newName(SQL_DATA_TYPE),
            PNameFactory.newName(TABLE_FAMILY_BYTES), PInteger.INSTANCE, null, null, false, 1, SortOrder.getDefault(),
            0, null, false, null, false, false, Bytes.toBytes(SQL_DATA_TYPE), HConstants.LATEST_TIMESTAMP);
    private static final PColumnImpl SQL_DATETIME_SUB_COLUMN = new PColumnImpl(PNameFactory.newName(SQL_DATETIME_SUB),
            PNameFactory.newName(TABLE_FAMILY_BYTES), PInteger.INSTANCE, null, null, false, 1, SortOrder.getDefault(),
            0, null, false, null, false, false, Bytes.toBytes(SQL_DATETIME_SUB), HConstants.LATEST_TIMESTAMP);
    private static final PColumnImpl CHAR_OCTET_LENGTH_COLUMN = new PColumnImpl(PNameFactory.newName(COLUMN_SIZE),
            PNameFactory.newName(TABLE_FAMILY_BYTES), PInteger.INSTANCE, null, null, false, 1, SortOrder.getDefault(),
            0, null, false, null, false, false, Bytes.toBytes(CHAR_OCTET_LENGTH), HConstants.LATEST_TIMESTAMP);
    private static final PColumnImpl ORDINAL_POSITION_COLUMN = new PColumnImpl(PNameFactory.newName(ORDINAL_POSITION),
            PNameFactory.newName(TABLE_FAMILY_BYTES), PInteger.INSTANCE, null, null, false, 1, SortOrder.getDefault(),
            0, null, false, null, false, false, ORDINAL_POSITION_BYTES, HConstants.LATEST_TIMESTAMP);
    private static final PColumnImpl IS_NULLABLE_COLUMN = new PColumnImpl(PNameFactory.newName(IS_NULLABLE),
            PNameFactory.newName(TABLE_FAMILY_BYTES), PVarchar.INSTANCE, null, null, false, 1, SortOrder.getDefault(),
            0, null, false, null, false, false, Bytes.toBytes(IS_NULLABLE), HConstants.LATEST_TIMESTAMP);
    private static final PColumnImpl SCOPE_CATALOG_COLUMN = new PColumnImpl(PNameFactory.newName(SCOPE_CATALOG),
            PNameFactory.newName(TABLE_FAMILY_BYTES), PVarchar.INSTANCE, null, null, false, 1, SortOrder.getDefault(),
            0, null, false, null, false, false, Bytes.toBytes(SCOPE_CATALOG), HConstants.LATEST_TIMESTAMP);
    private static final PColumnImpl SCOPE_SCHEMA_COLUMN = new PColumnImpl(PNameFactory.newName(SCOPE_SCHEMA),
            PNameFactory.newName(TABLE_FAMILY_BYTES), PVarchar.INSTANCE, null, null, false, 1, SortOrder.getDefault(),
            0, null, false, null, false, false, Bytes.toBytes(SCOPE_SCHEMA), HConstants.LATEST_TIMESTAMP);
    private static final PColumnImpl SCOPE_TABLE_COLUMN = new PColumnImpl(PNameFactory.newName(SCOPE_TABLE),
            PNameFactory.newName(TABLE_FAMILY_BYTES), PVarchar.INSTANCE, null, null, false, 1, SortOrder.getDefault(),
            0, null, false, null, false, false, Bytes.toBytes(SCOPE_TABLE), HConstants.LATEST_TIMESTAMP);
    private static final PColumnImpl SOURCE_DATA_TYPE_COLUMN = new PColumnImpl(PNameFactory.newName(SOURCE_DATA_TYPE),
            PNameFactory.newName(TABLE_FAMILY_BYTES), PVarchar.INSTANCE, null, null, false, 1, SortOrder.getDefault(),
            0, null, false, null, false, false, Bytes.toBytes(SOURCE_DATA_TYPE), HConstants.LATEST_TIMESTAMP);
    private static final PColumnImpl IS_AUTOINCREMENT_COLUMN = new PColumnImpl(PNameFactory.newName(COLUMN_SIZE),
            PNameFactory.newName(TABLE_FAMILY_BYTES), PSmallint.INSTANCE, null, null, false, 1, SortOrder.getDefault(),
            0, null, false, null, false, false, Bytes.toBytes(SCOPE_CATALOG), HConstants.LATEST_TIMESTAMP);
    private static final PColumnImpl ARRAY_SIZE_COLUMN = new PColumnImpl(PNameFactory.newName(ARRAY_SIZE),
            PNameFactory.newName(TABLE_FAMILY_BYTES), PInteger.INSTANCE, null, null, false, 1, SortOrder.getDefault(),
            0, null, false, null, false, false, ARRAY_SIZE_BYTES, HConstants.LATEST_TIMESTAMP);
    private static final PColumnImpl COLUMN_FAMILY_COLUMN = new PColumnImpl(PNameFactory.newName(COLUMN_FAMILY),
            PNameFactory.newName(TABLE_FAMILY_BYTES), PVarchar.INSTANCE, null, null, false, 1, SortOrder.getDefault(),
            0, null, false, null, false, false, COLUMN_FAMILY_BYTES, HConstants.LATEST_TIMESTAMP);
    private static final PColumnImpl TYPE_ID_COLUMN = new PColumnImpl(PNameFactory.newName(COLUMN_SIZE),
            PNameFactory.newName(TABLE_FAMILY_BYTES), PInteger.INSTANCE, null, null, false, 1, SortOrder.getDefault(),
            0, null, false, null, false, false, Bytes.toBytes(TYPE_ID), HConstants.LATEST_TIMESTAMP);
    private static final PColumnImpl VIEW_CONSTANT_COLUMN = new PColumnImpl(PNameFactory.newName(VIEW_CONSTANT),
            PNameFactory.newName(TABLE_FAMILY_BYTES), PVarbinary.INSTANCE, null, null, false, 1, SortOrder.getDefault(),
            0, null, false, null, false, false, VIEW_CONSTANT_BYTES, HConstants.LATEST_TIMESTAMP);
    private static final PColumnImpl MULTI_TENANT_COLUMN = new PColumnImpl(PNameFactory.newName(MULTI_TENANT),
            PNameFactory.newName(TABLE_FAMILY_BYTES), PBoolean.INSTANCE, null, null, false, 1, SortOrder.getDefault(),
            0, null, false, null, false, false, MULTI_TENANT_BYTES, HConstants.LATEST_TIMESTAMP);
    private static final PColumnImpl KEY_SEQ_COLUMN = new PColumnImpl(PNameFactory.newName(KEY_SEQ),
            PNameFactory.newName(TABLE_FAMILY_BYTES), PSmallint.INSTANCE, null, null, false, 1, SortOrder.getDefault(),
            0, null, false, null, false, false, KEY_SEQ_BYTES, HConstants.LATEST_TIMESTAMP);
    private static final PColumnImpl PK_NAME_COLUMN = new PColumnImpl(PNameFactory.newName(PK_NAME),
        PNameFactory.newName(TABLE_FAMILY_BYTES), PVarchar.INSTANCE, null, null, false, 1, SortOrder.getDefault(),
        0, null, false, null, false, false, PK_NAME_BYTES, HConstants.LATEST_TIMESTAMP);
    public static final String ASC_OR_DESC = "ASC_OR_DESC";
    public static final byte[] ASC_OR_DESC_BYTES = Bytes.toBytes(ASC_OR_DESC);
    private static final PColumnImpl ASC_OR_DESC_COLUMN = new PColumnImpl(PNameFactory.newName(ASC_OR_DESC),
        PNameFactory.newName(TABLE_FAMILY_BYTES), PVarchar.INSTANCE, null, null, false, 1, SortOrder.getDefault(),
        0, null, false, null, false, false, ASC_OR_DESC_BYTES, HConstants.LATEST_TIMESTAMP);
    
    private static final List<PColumnImpl> PK_DATUM_LIST = Lists.newArrayList(TENANT_ID_COLUMN, TABLE_SCHEM_COLUMN, TABLE_NAME_COLUMN, COLUMN_NAME_COLUMN);
    
    private static final RowProjector GET_COLUMNS_ROW_PROJECTOR = new RowProjector(
            Arrays.<ColumnProjector> asList(
                    new ExpressionProjector(TABLE_CAT, TABLE_CAT, SYSTEM_CATALOG,
                            new RowKeyColumnExpression(TENANT_ID_COLUMN,
                                    new RowKeyValueAccessor(PK_DATUM_LIST, 0)), false),
                    new ExpressionProjector(TABLE_SCHEM, TABLE_SCHEM, SYSTEM_CATALOG,
                            new RowKeyColumnExpression(TABLE_SCHEM_COLUMN,
                                    new RowKeyValueAccessor(PK_DATUM_LIST, 1)), false),
                    new ExpressionProjector(TABLE_NAME, TABLE_NAME, SYSTEM_CATALOG,
                            new RowKeyColumnExpression(TABLE_NAME_COLUMN,
                                    new RowKeyValueAccessor(PK_DATUM_LIST, 2)), false),
                    new ExpressionProjector(COLUMN_NAME, COLUMN_NAME, SYSTEM_CATALOG,
                            new RowKeyColumnExpression(COLUMN_NAME_COLUMN,
                                    new RowKeyValueAccessor(PK_DATUM_LIST, 3)), false),
                    new ExpressionProjector(DATA_TYPE, DATA_TYPE, SYSTEM_CATALOG,
                            new KeyValueColumnExpression(DATA_TYPE_COLUMN), false),
                    new ExpressionProjector(TYPE_NAME, TYPE_NAME, SYSTEM_CATALOG,
                            new KeyValueColumnExpression(TYPE_NAME_COLUMN), false),
                    new ExpressionProjector(COLUMN_SIZE, COLUMN_SIZE, SYSTEM_CATALOG,
                            new KeyValueColumnExpression(COLUMN_SIZE_COLUMN), false),
                    new ExpressionProjector(BUFFER_LENGTH, BUFFER_LENGTH, SYSTEM_CATALOG,
                            new KeyValueColumnExpression(BUFFER_LENGTH_COLUMN), false),
                    new ExpressionProjector(DECIMAL_DIGITS, DECIMAL_DIGITS, SYSTEM_CATALOG,
                            new KeyValueColumnExpression(DECIMAL_DIGITS_COLUMN), false),
                    new ExpressionProjector(NUM_PREC_RADIX, NUM_PREC_RADIX, SYSTEM_CATALOG,
                            new KeyValueColumnExpression(NUM_PREC_RADIX_COLUMN), false),
                    new ExpressionProjector(NULLABLE, NULLABLE, SYSTEM_CATALOG,
                            new KeyValueColumnExpression(NULLABLE_COLUMN), false),
                    new ExpressionProjector(REMARKS, REMARKS, SYSTEM_CATALOG,
                            new KeyValueColumnExpression(REMARKS_COLUMN), false),
                    new ExpressionProjector(COLUMN_DEF, COLUMN_DEF, SYSTEM_CATALOG,
                            new KeyValueColumnExpression(COLUMN_DEF_COLUMN), false),
                    new ExpressionProjector(SQL_DATA_TYPE, SQL_DATA_TYPE, SYSTEM_CATALOG,
                            new KeyValueColumnExpression(SQL_DATA_TYPE_COLUMN), false),
                    new ExpressionProjector(SQL_DATETIME_SUB, SQL_DATETIME_SUB, SYSTEM_CATALOG,
                            new KeyValueColumnExpression(SQL_DATETIME_SUB_COLUMN), false),
                    new ExpressionProjector(CHAR_OCTET_LENGTH, CHAR_OCTET_LENGTH, SYSTEM_CATALOG,
                            new KeyValueColumnExpression(CHAR_OCTET_LENGTH_COLUMN), false),
                    new ExpressionProjector(ORDINAL_POSITION, ORDINAL_POSITION, SYSTEM_CATALOG,
                            new KeyValueColumnExpression(ORDINAL_POSITION_COLUMN), false),
                    new ExpressionProjector(IS_NULLABLE, IS_NULLABLE, SYSTEM_CATALOG,
                            new KeyValueColumnExpression(IS_NULLABLE_COLUMN), false),
                    new ExpressionProjector(SCOPE_CATALOG, SCOPE_CATALOG, SYSTEM_CATALOG,
                            new KeyValueColumnExpression(SCOPE_CATALOG_COLUMN), false),
                    new ExpressionProjector(SCOPE_SCHEMA, SCOPE_SCHEMA, SYSTEM_CATALOG,
                            new KeyValueColumnExpression(SCOPE_SCHEMA_COLUMN), false),
                    new ExpressionProjector(SCOPE_TABLE, SCOPE_TABLE, SYSTEM_CATALOG,
                            new KeyValueColumnExpression(SCOPE_TABLE_COLUMN), false),
                    new ExpressionProjector(SOURCE_DATA_TYPE, SOURCE_DATA_TYPE, SYSTEM_CATALOG,
                            new KeyValueColumnExpression(SOURCE_DATA_TYPE_COLUMN), false),
                    new ExpressionProjector(IS_AUTOINCREMENT, IS_AUTOINCREMENT, SYSTEM_CATALOG,
                            new KeyValueColumnExpression(IS_AUTOINCREMENT_COLUMN), false),
                    new ExpressionProjector(ARRAY_SIZE, ARRAY_SIZE, SYSTEM_CATALOG,
                            new KeyValueColumnExpression(ARRAY_SIZE_COLUMN), false),
                    new ExpressionProjector(COLUMN_FAMILY, COLUMN_FAMILY, SYSTEM_CATALOG,
                            new KeyValueColumnExpression(COLUMN_FAMILY_COLUMN), false),
                    new ExpressionProjector(TYPE_ID, TYPE_ID, SYSTEM_CATALOG,
                            new KeyValueColumnExpression(TYPE_ID_COLUMN), false),
                    new ExpressionProjector(VIEW_CONSTANT, VIEW_CONSTANT, SYSTEM_CATALOG,
                            new KeyValueColumnExpression(VIEW_CONSTANT_COLUMN), false),
                    new ExpressionProjector(MULTI_TENANT, MULTI_TENANT, SYSTEM_CATALOG,
                            new KeyValueColumnExpression(MULTI_TENANT_COLUMN), false),
                    new ExpressionProjector(KEY_SEQ, KEY_SEQ, SYSTEM_CATALOG,
                            new KeyValueColumnExpression(KEY_SEQ_COLUMN), false)
                    ), 0, true);
    
    private static final RowProjector GET_PRIMARY_KEYS_ROW_PROJECTOR =
            new RowProjector(
                    Arrays.<ColumnProjector> asList(
                        new ExpressionProjector(TABLE_CAT, TABLE_CAT, SYSTEM_CATALOG,
                                new RowKeyColumnExpression(TENANT_ID_COLUMN,
                                        new RowKeyValueAccessor(PK_DATUM_LIST, 0)),
                                false),
                        new ExpressionProjector(TABLE_SCHEM, TABLE_SCHEM, SYSTEM_CATALOG,
                                new RowKeyColumnExpression(TABLE_SCHEM_COLUMN,
                                        new RowKeyValueAccessor(PK_DATUM_LIST, 1)),
                                false),
                        new ExpressionProjector(TABLE_NAME, TABLE_NAME, SYSTEM_CATALOG,
                                new RowKeyColumnExpression(TABLE_NAME_COLUMN,
                                        new RowKeyValueAccessor(PK_DATUM_LIST, 2)),
                                false),
                        new ExpressionProjector(COLUMN_NAME, COLUMN_NAME, SYSTEM_CATALOG,
                                new RowKeyColumnExpression(COLUMN_NAME_COLUMN,
                                        new RowKeyValueAccessor(PK_DATUM_LIST, 3)),
                                false),
                        new ExpressionProjector(KEY_SEQ, KEY_SEQ, SYSTEM_CATALOG,
                                new KeyValueColumnExpression(KEY_SEQ_COLUMN), false),
                        new ExpressionProjector(PK_NAME, PK_NAME, SYSTEM_CATALOG,
                                new KeyValueColumnExpression(PK_NAME_COLUMN), false),
                        new ExpressionProjector(ASC_OR_DESC, ASC_OR_DESC, SYSTEM_CATALOG,
                                new KeyValueColumnExpression(ASC_OR_DESC_COLUMN), false),
                        new ExpressionProjector(DATA_TYPE, DATA_TYPE, SYSTEM_CATALOG,
                                new KeyValueColumnExpression(DATA_TYPE_COLUMN), false),
                        new ExpressionProjector(TYPE_NAME, TYPE_NAME, SYSTEM_CATALOG,
                                new KeyValueColumnExpression(TYPE_NAME_COLUMN), false),
                        new ExpressionProjector(COLUMN_SIZE, COLUMN_SIZE, SYSTEM_CATALOG,
                                new KeyValueColumnExpression(COLUMN_SIZE_COLUMN), false),
                        new ExpressionProjector(TYPE_ID, TYPE_ID, SYSTEM_CATALOG,
                                new KeyValueColumnExpression(TYPE_ID_COLUMN), false),
                        new ExpressionProjector(VIEW_CONSTANT, VIEW_CONSTANT, SYSTEM_CATALOG,
                                new KeyValueColumnExpression(VIEW_CONSTANT_COLUMN), false)),
                    0, true);
    
    private boolean match(String str, String pattern) throws SQLException {
        LiteralExpression strExpr = LiteralExpression.newConstant(str, PVarchar.INSTANCE, SortOrder.ASC);
        LiteralExpression patternExpr = LiteralExpression.newConstant(pattern, PVarchar.INSTANCE, SortOrder.ASC);
        List<Expression> children = Arrays.<Expression>asList(strExpr, patternExpr);
        LikeExpression likeExpr = StringBasedLikeExpression.create(children, LikeType.CASE_SENSITIVE);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        boolean evaluated = likeExpr.evaluate(null, ptr);
        Boolean result = (Boolean)likeExpr.getDataType().toObject(ptr);
        if (evaluated) {
            return result;
        }
        return false;
    }
    
    @Override
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
            throws SQLException {
        try {
        boolean isTenantSpecificConnection = connection.getTenantId() != null;
        List<Tuple> tuples = Lists.newArrayListWithExpectedSize(10);
        // Allow a "." in columnNamePattern for column family match
        String colPattern = null;
        String cfPattern = null;
        if (columnNamePattern != null && columnNamePattern.length() > 0) {
            int index = columnNamePattern.indexOf('.');
            if (index <= 0) {
                colPattern = columnNamePattern;
            } else {
                cfPattern = columnNamePattern.substring(0, index);
                if (columnNamePattern.length() > index+1) {
                    colPattern = columnNamePattern.substring(index+1);
                }
            }
        }
        try (ResultSet rs = getTables(catalog, schemaPattern, tableNamePattern, null)) {
            while (rs.next()) {
                String schemaName = rs.getString(TABLE_SCHEM);
                String tableName = rs.getString(TABLE_NAME);
                String tenantId = rs.getString(TABLE_CAT);
                String fullTableName = SchemaUtil.getTableName(schemaName, tableName);
                PTable table = connection.getTableNoCache(fullTableName);
                boolean isSalted = table.getBucketNum()!=null;
                boolean tenantColSkipped = false;
                List<PColumn> columns = table.getColumns();
                int startOffset = isSalted ? 1 : 0;
                columns = Lists.newArrayList(columns.subList(startOffset, columns.size()));
                for (PColumn column : columns) {
                    if (isTenantSpecificConnection && column.equals(table.getPKColumns().get(startOffset))) {
                        // skip the tenant column
                        tenantColSkipped = true;
                        continue;
                    }
                    String columnFamily = column.getFamilyName()!=null ? column.getFamilyName().getString() : null;
                    String columnName = column.getName().getString();
                    if (cfPattern != null && cfPattern.length() > 0) { // if null or empty, will pick up all columns
                        if (columnFamily==null || !match(columnFamily, cfPattern)) {
                            continue;
                        }
                    }
                    if (colPattern != null && colPattern.length() > 0) {
                        if (!match(columnName, colPattern)) {
                            continue;
                        }
                    }
                    // generate row key
                    // TENANT_ID, TABLE_SCHEM, TABLE_NAME , COLUMN_NAME are row key columns
                    byte[] rowKey =
                            SchemaUtil.getColumnKey(tenantId, schemaName, tableName, columnName, null);

                    // add one cell for each column info
                    List<Cell> cells = Lists.newArrayListWithCapacity(25);
                    // DATA_TYPE
                    cells.add(PhoenixKeyValueUtil.newKeyValue(rowKey, TABLE_FAMILY_BYTES,
                        DATA_TYPE_BYTES,
                        MetaDataProtocol.MIN_TABLE_TIMESTAMP,
                        PInteger.INSTANCE.toBytes(column.getDataType().getResultSetSqlType())));
                    // TYPE_NAME
                    cells.add(PhoenixKeyValueUtil.newKeyValue(rowKey, TABLE_FAMILY_BYTES,
                        Bytes.toBytes(TYPE_NAME), MetaDataProtocol.MIN_TABLE_TIMESTAMP,
                        column.getDataType().getSqlTypeNameBytes()));
                    // COLUMN_SIZE
                    cells.add(
                        PhoenixKeyValueUtil.newKeyValue(rowKey, TABLE_FAMILY_BYTES, COLUMN_SIZE_BYTES,
                            MetaDataProtocol.MIN_TABLE_TIMESTAMP,
                            column.getMaxLength() != null
                                    ? PInteger.INSTANCE.toBytes(column.getMaxLength())
                                    : ByteUtil.EMPTY_BYTE_ARRAY));
                    // BUFFER_LENGTH
                    cells.add(PhoenixKeyValueUtil.newKeyValue(rowKey, TABLE_FAMILY_BYTES,
                        Bytes.toBytes(BUFFER_LENGTH), MetaDataProtocol.MIN_TABLE_TIMESTAMP,
                        ByteUtil.EMPTY_BYTE_ARRAY));
                    // DECIMAL_DIGITS
                    cells.add(PhoenixKeyValueUtil.newKeyValue(rowKey, TABLE_FAMILY_BYTES,
                        DECIMAL_DIGITS_BYTES,
                        MetaDataProtocol.MIN_TABLE_TIMESTAMP,
                        column.getScale() != null ? PInteger.INSTANCE.toBytes(column.getScale())
                                : ByteUtil.EMPTY_BYTE_ARRAY));
                    // NUM_PREC_RADIX
                    cells.add(PhoenixKeyValueUtil.newKeyValue(rowKey, TABLE_FAMILY_BYTES,
                        Bytes.toBytes(NUM_PREC_RADIX), MetaDataProtocol.MIN_TABLE_TIMESTAMP,
                        ByteUtil.EMPTY_BYTE_ARRAY));
                    // NULLABLE
                    cells.add(PhoenixKeyValueUtil.newKeyValue(rowKey, TABLE_FAMILY_BYTES,
                        NULLABLE_BYTES,
                        MetaDataProtocol.MIN_TABLE_TIMESTAMP,
                        PInteger.INSTANCE.toBytes(SchemaUtil.getIsNullableInt(column.isNullable()))));
                    // REMARKS
                    cells.add(
                        PhoenixKeyValueUtil.newKeyValue(rowKey, TABLE_FAMILY_BYTES,
                            Bytes.toBytes(REMARKS),
                            MetaDataProtocol.MIN_TABLE_TIMESTAMP, ByteUtil.EMPTY_BYTE_ARRAY));
                    // COLUMN_DEF
                    cells.add(
                        PhoenixKeyValueUtil.newKeyValue(rowKey, TABLE_FAMILY_BYTES,
                            Bytes.toBytes(COLUMN_DEF),
                            MetaDataProtocol.MIN_TABLE_TIMESTAMP,
                            PVarchar.INSTANCE.toBytes(column.getExpressionStr())));
                    // SQL_DATA_TYPE
                    cells.add(PhoenixKeyValueUtil.newKeyValue(rowKey, TABLE_FAMILY_BYTES,
                        Bytes.toBytes(SQL_DATA_TYPE), MetaDataProtocol.MIN_TABLE_TIMESTAMP,
                        ByteUtil.EMPTY_BYTE_ARRAY));
                    // SQL_DATETIME_SUB
                    cells.add(PhoenixKeyValueUtil.newKeyValue(rowKey, TABLE_FAMILY_BYTES,
                        Bytes.toBytes(SQL_DATETIME_SUB), MetaDataProtocol.MIN_TABLE_TIMESTAMP,
                        ByteUtil.EMPTY_BYTE_ARRAY));
                    // CHAR_OCTET_LENGTH
                    cells.add(PhoenixKeyValueUtil.newKeyValue(rowKey, TABLE_FAMILY_BYTES,
                        Bytes.toBytes(CHAR_OCTET_LENGTH), MetaDataProtocol.MIN_TABLE_TIMESTAMP,
                        ByteUtil.EMPTY_BYTE_ARRAY));
                    // ORDINAL_POSITION
                    int ordinal =
                            column.getPosition() + (isSalted ? 0 : 1) - (tenantColSkipped ? 1 : 0);
                    cells.add(
                        PhoenixKeyValueUtil.newKeyValue(rowKey, TABLE_FAMILY_BYTES,
                            ORDINAL_POSITION_BYTES,
                            MetaDataProtocol.MIN_TABLE_TIMESTAMP, PInteger.INSTANCE.toBytes(ordinal)));
                    String isNullable =
                            column.isNullable() ? Boolean.TRUE.toString() : Boolean.FALSE.toString();
                    // IS_NULLABLE
                    cells.add(PhoenixKeyValueUtil.newKeyValue(rowKey, TABLE_FAMILY_BYTES,
                        Bytes.toBytes(IS_NULLABLE), MetaDataProtocol.MIN_TABLE_TIMESTAMP,
                        PVarchar.INSTANCE.toBytes(isNullable)));
                    // SCOPE_CATALOG
                    cells.add(PhoenixKeyValueUtil.newKeyValue(rowKey, TABLE_FAMILY_BYTES,
                        Bytes.toBytes(SCOPE_CATALOG), MetaDataProtocol.MIN_TABLE_TIMESTAMP,
                        ByteUtil.EMPTY_BYTE_ARRAY));
                    // SCOPE_SCHEMA
                    cells.add(PhoenixKeyValueUtil.newKeyValue(rowKey, TABLE_FAMILY_BYTES,
                        Bytes.toBytes(SCOPE_SCHEMA), MetaDataProtocol.MIN_TABLE_TIMESTAMP,
                        ByteUtil.EMPTY_BYTE_ARRAY));
                    // SCOPE_TABLE
                    cells.add(
                        PhoenixKeyValueUtil.newKeyValue(rowKey, TABLE_FAMILY_BYTES,
                            Bytes.toBytes(SCOPE_TABLE),
                            MetaDataProtocol.MIN_TABLE_TIMESTAMP, ByteUtil.EMPTY_BYTE_ARRAY));
                    // SOURCE_DATA_TYPE
                    cells.add(PhoenixKeyValueUtil.newKeyValue(rowKey, TABLE_FAMILY_BYTES,
                        Bytes.toBytes(SOURCE_DATA_TYPE), MetaDataProtocol.MIN_TABLE_TIMESTAMP,
                        ByteUtil.EMPTY_BYTE_ARRAY));
                    // IS_AUTOINCREMENT
                    cells.add(PhoenixKeyValueUtil.newKeyValue(rowKey, TABLE_FAMILY_BYTES,
                        Bytes.toBytes(IS_AUTOINCREMENT), MetaDataProtocol.MIN_TABLE_TIMESTAMP,
                        ByteUtil.EMPTY_BYTE_ARRAY));
                    // ARRAY_SIZE
                    cells.add(
                        PhoenixKeyValueUtil.newKeyValue(rowKey, TABLE_FAMILY_BYTES, ARRAY_SIZE_BYTES,
                            MetaDataProtocol.MIN_TABLE_TIMESTAMP,
                            column.getArraySize() != null
                                    ? PInteger.INSTANCE.toBytes(column.getArraySize())
                                    : ByteUtil.EMPTY_BYTE_ARRAY));
                    // COLUMN_FAMILY
                    cells.add(PhoenixKeyValueUtil.newKeyValue(rowKey, TABLE_FAMILY_BYTES,
                        COLUMN_FAMILY_BYTES,
                        MetaDataProtocol.MIN_TABLE_TIMESTAMP, column.getFamilyName() != null
                                ? column.getFamilyName().getBytes() : ByteUtil.EMPTY_BYTE_ARRAY));
                    // TYPE_ID
                    cells.add(PhoenixKeyValueUtil.newKeyValue(rowKey, TABLE_FAMILY_BYTES,
                        Bytes.toBytes(TYPE_ID), MetaDataProtocol.MIN_TABLE_TIMESTAMP,
                        PInteger.INSTANCE.toBytes(column.getDataType().getSqlType())));
                    // VIEW_CONSTANT
                    cells.add(PhoenixKeyValueUtil.newKeyValue(rowKey, TABLE_FAMILY_BYTES,
                        VIEW_CONSTANT_BYTES,
                        MetaDataProtocol.MIN_TABLE_TIMESTAMP, column.getViewConstant() != null
                                ? column.getViewConstant() : ByteUtil.EMPTY_BYTE_ARRAY));
                    // MULTI_TENANT
                    cells.add(PhoenixKeyValueUtil.newKeyValue(rowKey, TABLE_FAMILY_BYTES,
                        MULTI_TENANT_BYTES,
                        MetaDataProtocol.MIN_TABLE_TIMESTAMP,
                        PBoolean.INSTANCE.toBytes(table.isMultiTenant())));
                    // KEY_SEQ_COLUMN
                    byte[] keySeqBytes = ByteUtil.EMPTY_BYTE_ARRAY;
                    int pkPos = table.getPKColumns().indexOf(column);
                    if (pkPos!=-1) {
                        short keySeq = (short) (pkPos + 1 - startOffset - (tenantColSkipped ? 1 : 0));
                        keySeqBytes = PSmallint.INSTANCE.toBytes(keySeq);
                    }
                    cells.add(PhoenixKeyValueUtil.newKeyValue(rowKey, TABLE_FAMILY_BYTES, KEY_SEQ_BYTES,
                            MetaDataProtocol.MIN_TABLE_TIMESTAMP, keySeqBytes));
                    Collections.sort(cells, new CellComparatorImpl());
                    Tuple tuple = new MultiKeyValueTuple(cells);
                    tuples.add(tuple);
                }
            }
        }

        PhoenixStatement stmt = new PhoenixStatement(connection);
        stmt.closeOnCompletion();
        return new PhoenixResultSet(new MaterializedResultIterator(tuples), GET_COLUMNS_ROW_PROJECTOR, new StatementContext(stmt, false));
        } finally {
            if (connection.getAutoCommit()) {
                connection.commit();
            }
        }
    }

    @Override
    public Connection getConnection() throws SQLException {
        return connection;
    }

    @Override
    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable,
            String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException {
        return getEmptyResultSet();
    }

    @Override
    public int getDatabaseMajorVersion() throws SQLException {
        return MetaDataProtocol.PHOENIX_MAJOR_VERSION;
    }

    @Override
    public int getDatabaseMinorVersion() throws SQLException {
        return MetaDataProtocol.PHOENIX_MINOR_VERSION;
    }

    @Override
    public String getDatabaseProductName() throws SQLException {
        return "Phoenix";
    }

    @Override
    public String getDatabaseProductVersion() throws SQLException {
        return Integer.toString(getDatabaseMajorVersion()) + "." + Integer.toString(getDatabaseMinorVersion());
    }

    @Override
    public int getDefaultTransactionIsolation() throws SQLException {
        return connection.getTransactionIsolation();
    }

    @Override
    public int getDriverMajorVersion() {
        return Integer.parseInt(connection.getClientInfo(PhoenixEmbeddedDriver.MAJOR_VERSION_PROP));
    }

    @Override
    public int getDriverMinorVersion() {
        return Integer.parseInt(connection.getClientInfo(PhoenixEmbeddedDriver.MINOR_VERSION_PROP));
    }

    @Override
    public String getDriverName() throws SQLException {
        return connection.getClientInfo(PhoenixEmbeddedDriver.DRIVER_NAME_PROP);
    }

    @Override
    public String getDriverVersion() throws SQLException {
        return connection.getClientInfo(PhoenixEmbeddedDriver.MAJOR_VERSION_PROP) + "." + connection.getClientInfo(PhoenixEmbeddedDriver.MINOR_VERSION_PROP);
    }

    @Override
    public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
        return getEmptyResultSet();
    }

    @Override
    public String getExtraNameCharacters() throws SQLException {
        return "";
    }

    @Override
    public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern,
            String columnNamePattern) throws SQLException {
        return getEmptyResultSet();
    }

    @Override
    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern) throws SQLException {
        return getEmptyResultSet();
    }

    @Override
    public String getIdentifierQuoteString() throws SQLException {
        return SchemaUtil.ESCAPE_CHARACTER;
    }

    @Override
    public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
        return getEmptyResultSet();
    }

    @Override
    public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique,
        boolean approximate) throws SQLException {
        PreparedStatement stmt = QueryUtil.getIndexInfoStmt(connection, catalog, schema, table,
            unique, approximate);
        if (stmt == null) {
            return getEmptyResultSet();
        }
        stmt.closeOnCompletion();
        return stmt.executeQuery();
    }

    @Override
    public int getJDBCMajorVersion() throws SQLException {
        return 1;
    }

    @Override
    public int getJDBCMinorVersion() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxBinaryLiteralLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxCatalogNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxCharLiteralLength() throws SQLException {
        return 4000;
    }

    @Override
    public int getMaxColumnNameLength() throws SQLException {
        return 200;
    }

    @Override
    public int getMaxColumnsInGroupBy() throws SQLException {
        return 1;
    }

    @Override
    public int getMaxColumnsInIndex() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInOrderBy() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInSelect() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInTable() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxConnections() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxCursorNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxIndexLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxProcedureNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxRowSize() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxSchemaNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxStatementLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxStatements() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxTableNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxTablesInSelect() throws SQLException {
        return 1;
    }

    @Override
    public int getMaxUserNameLength() throws SQLException {
        return 0;
    }

    @Override
    public String getNumericFunctions() throws SQLException {
        return "";
    }

    @Override
    public ResultSet getPrimaryKeys(String catalog, String schemaName, String tableName)
            throws SQLException {
        if (tableName == null || tableName.length() == 0) {
            return getEmptyResultSet();
        }
        String fullTableName = SchemaUtil.getTableName(schemaName, tableName);
        PTable table = connection.getTableNoCache(fullTableName);
        boolean isSalted = table.getBucketNum() != null;
        boolean tenantColSkipped = false;
        List<PColumn> pkColumns = table.getPKColumns();
        List<PColumn> sorderPkColumns =
                Lists.newArrayList(pkColumns.subList(isSalted ? 1 : 0, pkColumns.size()));
        // sort the columns by name
        Collections.sort(sorderPkColumns, new Comparator<PColumn>(){
            @Override public int compare(PColumn c1, PColumn c2) {
                return c1.getName().getString().compareTo(c2.getName().getString());
            }
        });

        try {
        List<Tuple> tuples = Lists.newArrayListWithExpectedSize(10);
        try (ResultSet rs = getTables(catalog, schemaName, tableName, null)) {
            while (rs.next()) {
                String tenantId = rs.getString(TABLE_CAT);
                for (PColumn column : sorderPkColumns) {
                    String columnName = column.getName().getString();
                    // generate row key
                    // TENANT_ID, TABLE_SCHEM, TABLE_NAME , COLUMN_NAME are row key columns
                    byte[] rowKey =
                            SchemaUtil.getColumnKey(tenantId, schemaName, tableName, columnName, null);

                    // add one cell for each column info
                    List<Cell> cells = Lists.newArrayListWithCapacity(8);
                    // KEY_SEQ_COLUMN
                    byte[] keySeqBytes = ByteUtil.EMPTY_BYTE_ARRAY;
                    int pkPos = pkColumns.indexOf(column);
                    if (pkPos != -1) {
                        short keySeq =
                                (short) (pkPos + 1 - (isSalted ? 1 : 0) - (tenantColSkipped ? 1 : 0));
                        keySeqBytes = PSmallint.INSTANCE.toBytes(keySeq);
                    }
                    cells.add(PhoenixKeyValueUtil.newKeyValue(rowKey, TABLE_FAMILY_BYTES, KEY_SEQ_BYTES,
                        MetaDataProtocol.MIN_TABLE_TIMESTAMP, keySeqBytes));
                    // PK_NAME
                    cells.add(PhoenixKeyValueUtil.newKeyValue(rowKey, TABLE_FAMILY_BYTES, PK_NAME_BYTES,
                        MetaDataProtocol.MIN_TABLE_TIMESTAMP, table.getPKName() != null
                                ? table.getPKName().getBytes() : ByteUtil.EMPTY_BYTE_ARRAY));
                    // ASC_OR_DESC
                    char sortOrder = column.getSortOrder() == SortOrder.ASC ? 'A' : 'D';
                    cells.add(PhoenixKeyValueUtil.newKeyValue(rowKey, TABLE_FAMILY_BYTES,
                        ASC_OR_DESC_BYTES, MetaDataProtocol.MIN_TABLE_TIMESTAMP,
                        Bytes.toBytes(sortOrder)));
                    // DATA_TYPE
                    cells.add(PhoenixKeyValueUtil.newKeyValue(rowKey, TABLE_FAMILY_BYTES, DATA_TYPE_BYTES,
                        MetaDataProtocol.MIN_TABLE_TIMESTAMP,
                        PInteger.INSTANCE.toBytes(column.getDataType().getResultSetSqlType())));
                    // TYPE_NAME
                    cells.add(PhoenixKeyValueUtil.newKeyValue(rowKey, TABLE_FAMILY_BYTES,
                        Bytes.toBytes(TYPE_NAME), MetaDataProtocol.MIN_TABLE_TIMESTAMP,
                        column.getDataType().getSqlTypeNameBytes()));
                    // COLUMN_SIZE
                    cells.add(
                        PhoenixKeyValueUtil.newKeyValue(rowKey, TABLE_FAMILY_BYTES, COLUMN_SIZE_BYTES,
                            MetaDataProtocol.MIN_TABLE_TIMESTAMP,
                            column.getMaxLength() != null
                                    ? PInteger.INSTANCE.toBytes(column.getMaxLength())
                                    : ByteUtil.EMPTY_BYTE_ARRAY));
                    // TYPE_ID
                    cells.add(PhoenixKeyValueUtil.newKeyValue(rowKey, TABLE_FAMILY_BYTES,
                        Bytes.toBytes(TYPE_ID), MetaDataProtocol.MIN_TABLE_TIMESTAMP,
                        PInteger.INSTANCE.toBytes(column.getDataType().getSqlType())));
                    // VIEW_CONSTANT
                    cells.add(PhoenixKeyValueUtil.newKeyValue(rowKey, TABLE_FAMILY_BYTES, VIEW_CONSTANT_BYTES,
                        MetaDataProtocol.MIN_TABLE_TIMESTAMP, column.getViewConstant() != null
                                ? column.getViewConstant() : ByteUtil.EMPTY_BYTE_ARRAY));
                    Collections.sort(cells, new CellComparatorImpl());
                    Tuple tuple = new MultiKeyValueTuple(cells);
                    tuples.add(tuple);
                }
            }
        }

        PhoenixStatement stmt = new PhoenixStatement(connection);
        stmt.closeOnCompletion();
        return new PhoenixResultSet(new MaterializedResultIterator(tuples),
                GET_PRIMARY_KEYS_ROW_PROJECTOR,
                new StatementContext(stmt, false));
        } finally {
            if (connection.getAutoCommit()) {
                connection.commit();
            }
        }
    }

    @Override
    public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern,
            String columnNamePattern) throws SQLException {
        return getEmptyResultSet();
    }

    @Override
    public String getProcedureTerm() throws SQLException {
        return "procedure";
    }

    @Override
    public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern)
            throws SQLException {
        return getEmptyResultSet();
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return connection.getHoldability();
    }

    @Override
    public RowIdLifetime getRowIdLifetime() throws SQLException {
        return RowIdLifetime.ROWID_UNSUPPORTED;
    }

    @Override
    public String getSQLKeywords() throws SQLException {
        return "";
    }

    @Override
    public int getSQLStateType() throws SQLException {
        return DatabaseMetaData.sqlStateSQL99;
    }

    @Override
    public String getSchemaTerm() throws SQLException {
        return "schema";
    }

    @Override
    public ResultSet getSchemas() throws SQLException {
        return getSchemas("", null);
    }

    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        PreparedStatement stmt = QueryUtil.getSchemasStmt(connection, catalog, schemaPattern);
        stmt.closeOnCompletion();
        return stmt.executeQuery();
    }

    @Override
    public String getSearchStringEscape() throws SQLException {
        return "\\";
    }

    @Override
    public String getStringFunctions() throws SQLException {
        return "";
    }

    @Override
    // TODO does this need to change to use the PARENT_TABLE link
    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern)
        throws SQLException {
        PreparedStatement stmt = QueryUtil.getSuperTablesStmt(connection, catalog, schemaPattern,
            tableNamePattern);
        stmt.closeOnCompletion();
        return stmt.executeQuery();
    }

    @Override
    public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
        return getEmptyResultSet();
    }

    @Override
    public String getSystemFunctions() throws SQLException {
        return "";
    }

    @Override
    public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern)
            throws SQLException {
        return getEmptyResultSet();
    }
    
    private static final PDatum TABLE_TYPE_DATUM = new PDatum() {
        @Override
        public boolean isNullable() {
            return true;
        }
        @Override
        public PDataType getDataType() {
            return PVarchar.INSTANCE;
        }
        @Override
        public Integer getMaxLength() {
            return null;
        }
        @Override
        public Integer getScale() {
            return null;
        }
        @Override
        public SortOrder getSortOrder() {
            return SortOrder.getDefault();
        }
    };

    private static final RowProjector TABLE_TYPE_ROW_PROJECTOR = new RowProjector(Arrays.<ColumnProjector>asList(
            new ExpressionProjector(TABLE_TYPE, TABLE_TYPE, SYSTEM_CATALOG,
                    new RowKeyColumnExpression(TABLE_TYPE_DATUM,
                            new RowKeyValueAccessor(Collections.<PDatum>singletonList(TABLE_TYPE_DATUM), 0)), false)
            ), 0, true);
    private static final Collection<Tuple> TABLE_TYPE_TUPLES = Lists.newArrayListWithExpectedSize(PTableType.values().length);
    static {
        List<byte[]> tableTypes = Lists.<byte[]>newArrayList(
                PTableType.INDEX.getValue().getBytes(),
                Bytes.toBytes(SEQUENCE_TABLE_TYPE), 
                PTableType.SYSTEM.getValue().getBytes(),
                PTableType.TABLE.getValue().getBytes(),
                PTableType.VIEW.getValue().getBytes());
        for (byte[] tableType : tableTypes) {
            TABLE_TYPE_TUPLES.add(new SingleKeyValueTuple(PhoenixKeyValueUtil.newKeyValue(tableType, TABLE_FAMILY_BYTES, TABLE_TYPE_BYTES, MetaDataProtocol.MIN_TABLE_TIMESTAMP, ByteUtil.EMPTY_BYTE_ARRAY)));
        }
    }
    
    /**
     * Supported table types include: INDEX, SEQUENCE, SYSTEM TABLE, TABLE, VIEW
     */
    @Override
    public ResultSet getTableTypes() throws SQLException {
        PhoenixStatement stmt = new PhoenixStatement(connection);
        stmt.closeOnCompletion();
        return new PhoenixResultSet(new MaterializedResultIterator(TABLE_TYPE_TUPLES), TABLE_TYPE_ROW_PROJECTOR, new StatementContext(stmt, false));
    }

    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern,
        String[] types) throws SQLException {
        PreparedStatement stmt = QueryUtil.getTablesStmt(connection, catalog, schemaPattern,
            tableNamePattern, types);
        if (stmt == null) {
            return getEmptyResultSet();
        }
        stmt.closeOnCompletion();
        return stmt.executeQuery();
    }

    @Override
    public String getTimeDateFunctions() throws SQLException {
        return "";
    }

    @Override
    public ResultSet getTypeInfo() throws SQLException {
        return getEmptyResultSet();
    }

    @Override
    public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types)
            throws SQLException {
        return getEmptyResultSet();
    }

    @Override
    public String getURL() throws SQLException {
        return connection.getURL();
    }

    @Override
    public String getUserName() throws SQLException {
        String userName = connection.getQueryServices().getUserName();
        return userName == null ? StringUtil.EMPTY_STRING : userName;
    }

    @Override
    public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
        return getEmptyResultSet();
    }

    @Override
    public boolean insertsAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean isCatalogAtStart() throws SQLException {
        return false;
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return false;
    }

    @Override
    public boolean locatorsUpdateCopy() throws SQLException {
        return false;
    }

    @Override
    public boolean nullPlusNonNullIsNull() throws SQLException {
        return true;
    }

    @Override
    public boolean nullsAreSortedAtEnd() throws SQLException {
        return false;
    }

    @Override
    public boolean nullsAreSortedAtStart() throws SQLException {
        return true;
    }

    @Override
    public boolean nullsAreSortedHigh() throws SQLException {
        return false;
    }

    @Override
    public boolean nullsAreSortedLow() throws SQLException {
        return true;
    }

    @Override
    public boolean othersDeletesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean othersInsertsAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean othersUpdatesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean ownDeletesAreVisible(int type) throws SQLException {
        return true;
    }

    @Override
    public boolean ownInsertsAreVisible(int type) throws SQLException {
        return true;
    }

    @Override
    public boolean ownUpdatesAreVisible(int type) throws SQLException {
        return true;
    }

    @Override
    public boolean storesLowerCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesMixedCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
        return true;
    }

    @Override
    public boolean storesUpperCaseIdentifiers() throws SQLException {
        return true;
    }

    @Override
    public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsANSI92EntryLevelSQL() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsANSI92FullSQL() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsANSI92IntermediateSQL() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsAlterTableWithAddColumn() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsAlterTableWithDropColumn() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsBatchUpdates() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsCatalogsInDataManipulation() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInProcedureCalls() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInTableDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsColumnAliasing() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsConvert() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsConvert(int fromType, int toType) throws SQLException {
        // TODO
        return false;
    }

    @Override
    public boolean supportsCoreSQLGrammar() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCorrelatedSubqueries() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsDifferentTableCorrelationNames() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsExpressionsInOrderBy() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsExtendedSQLGrammar() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsFullOuterJoins() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsGetGeneratedKeys() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsGroupBy() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsGroupByBeyondSelect() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsGroupByUnrelated() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsIntegrityEnhancementFacility() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsLikeEscapeClause() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsLimitedOuterJoins() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMinimumSQLGrammar() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMixedCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsMultipleOpenResults() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsMultipleResultSets() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsMultipleTransactions() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsNamedParameters() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsNonNullableColumns() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOrderByUnrelated() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOuterJoins() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsPositionedDelete() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsPositionedUpdate() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
        // TODO: review
        return type ==  ResultSet.TYPE_FORWARD_ONLY && concurrency == Connection.TRANSACTION_READ_COMMITTED;
    }

    @Override
    public boolean supportsResultSetHoldability(int holdability) throws SQLException {
        // TODO
        return holdability == connection.getHoldability();
    }

    @Override
    public boolean supportsResultSetType(int type) throws SQLException {
        return type == ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public boolean supportsSavepoints() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInDataManipulation() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsSchemasInIndexDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInProcedureCalls() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInTableDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSelectForUpdate() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsStatementPooling() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsStoredProcedures() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInComparisons() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInExists() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInIns() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInQuantifieds() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsTableCorrelationNames() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
        return true;
    }

    @Override
    public boolean supportsTransactions() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsUnion() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsUnionAll() throws SQLException {
        return false;
    }

    @Override
    public boolean updatesAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean usesLocalFilePerTable() throws SQLException {
        return false;
    }

    @Override
    public boolean usesLocalFiles() throws SQLException {
        return false;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isInstance(this);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (!iface.isInstance(this)) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.CLASS_NOT_UNWRAPPABLE)
                .setMessage(this.getClass().getName() + " not unwrappable from " + iface.getName())
                .build().buildException();
        }
        return (T)this;
    }

    @Override
    public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern,
            String columnNamePattern) throws SQLException {
        return getEmptyResultSet();
    }

    @Override
    public boolean generatedKeyAlwaysReturned() throws SQLException {
        return false;
    }


    private void setParameters(PreparedStatement stmt, List<String> parameterValues)
            throws SQLException {
        for (int i = 0; i < parameterValues.size(); i++) {
            stmt.setString(i+1, parameterValues.get(i));
        }
    }
}
