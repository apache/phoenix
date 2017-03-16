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

import static com.google.common.collect.Sets.newLinkedHashSet;
import static com.google.common.collect.Sets.newLinkedHashSetWithExpectedSize;
import static org.apache.hadoop.hbase.HColumnDescriptor.TTL;
import static org.apache.phoenix.coprocessor.BaseScannerRegionObserver.ANALYZE_TABLE;
import static org.apache.phoenix.coprocessor.BaseScannerRegionObserver.RUN_UPDATE_STATS_ASYNC_ATTRIB;
import static org.apache.phoenix.exception.SQLExceptionCode.INSUFFICIENT_MULTI_TENANT_COLUMNS;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.APPEND_ONLY_SCHEMA;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.ARG_POSITION;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.ARRAY_SIZE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.ASYNC_CREATED_DATE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.ASYNC_REBUILD_TIMESTAMP;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.AUTO_PARTITION_SEQ;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.BASE_COLUMN_COUNT;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.CLASS_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_COUNT;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_DEF;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_FAMILY;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_QUALIFIER;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_QUALIFIER_COUNTER;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_SIZE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.DATA_TABLE_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.DATA_TYPE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.DECIMAL_DIGITS;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.DEFAULT_COLUMN_FAMILY_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.DEFAULT_VALUE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.DISABLE_WAL;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.ENCODING_SCHEME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.FUNCTION_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.GUIDE_POSTS_WIDTH;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.IMMUTABLE_ROWS;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.IMMUTABLE_STORAGE_SCHEME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.INDEX_DISABLE_TIMESTAMP;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.INDEX_STATE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.INDEX_TYPE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.IS_ARRAY;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.IS_CONSTANT;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.IS_NAMESPACE_MAPPED;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.IS_ROW_TIMESTAMP;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.IS_VIEW_REFERENCED;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.JAR_PATH;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.KEY_SEQ;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.LAST_STATS_UPDATE_TIME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.LINK_TYPE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.MAX_VALUE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.MIN_VALUE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.MULTI_TENANT;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.NULLABLE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.NUM_ARGS;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.ORDINAL_POSITION;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.PARENT_TENANT_ID;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.PHYSICAL_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.PK_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.RETURN_TYPE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SALT_BUCKETS;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SORT_ORDER;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.STORE_NULLS;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CATALOG_SCHEMA;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CATALOG_TABLE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_FUNCTION_TABLE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_SCHEM;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_SEQ_NUM;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_TYPE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TENANT_ID;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TRANSACTIONAL;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TYPE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.UPDATE_CACHE_FREQUENCY;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.VIEW_CONSTANT;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.VIEW_STATEMENT;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.VIEW_TYPE;
import static org.apache.phoenix.query.QueryConstants.BASE_TABLE_BASE_COLUMN_COUNT;
import static org.apache.phoenix.query.QueryConstants.DEFAULT_COLUMN_FAMILY;
import static org.apache.phoenix.query.QueryServices.DROP_METADATA_ATTRIB;
import static org.apache.phoenix.query.QueryServicesOptions.DEFAULT_DROP_METADATA;
import static org.apache.phoenix.query.QueryServicesOptions.DEFAULT_RUN_UPDATE_STATS_ASYNC;
import static org.apache.phoenix.schema.PTable.EncodedCQCounter.NULL_COUNTER;
import static org.apache.phoenix.schema.PTable.ImmutableStorageScheme.ONE_CELL_PER_COLUMN;
import static org.apache.phoenix.schema.PTable.ImmutableStorageScheme.SINGLE_CELL_ARRAY_WITH_OFFSETS;
import static org.apache.phoenix.schema.PTable.QualifierEncodingScheme.NON_ENCODED_QUALIFIERS;
import static org.apache.phoenix.schema.PTable.ViewType.MAPPED;
import static org.apache.phoenix.schema.PTableType.TABLE;
import static org.apache.phoenix.schema.PTableType.VIEW;
import static org.apache.phoenix.schema.types.PDataType.FALSE_BYTES;
import static org.apache.phoenix.schema.types.PDataType.TRUE_BYTES;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.compile.ColumnResolver;
import org.apache.phoenix.compile.FromCompiler;
import org.apache.phoenix.compile.IndexExpressionCompiler;
import org.apache.phoenix.compile.MutationPlan;
import org.apache.phoenix.compile.PostDDLCompiler;
import org.apache.phoenix.compile.PostIndexDDLCompiler;
import org.apache.phoenix.compile.PostLocalIndexDDLCompiler;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.compile.StatementNormalizer;
import org.apache.phoenix.coprocessor.BaseScannerRegionObserver;
import org.apache.phoenix.coprocessor.MetaDataProtocol;
import org.apache.phoenix.coprocessor.MetaDataProtocol.MetaDataMutationResult;
import org.apache.phoenix.coprocessor.MetaDataProtocol.MutationCode;
import org.apache.phoenix.coprocessor.MetaDataProtocol.SharedTableState;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.execute.MutationState;
import org.apache.phoenix.expression.Determinism;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.RowKeyColumnExpression;
import org.apache.phoenix.hbase.index.covered.update.ColumnReference;
import org.apache.phoenix.index.IndexMaintainer;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.parse.AddColumnStatement;
import org.apache.phoenix.parse.AlterIndexStatement;
import org.apache.phoenix.parse.ColumnDef;
import org.apache.phoenix.parse.ColumnDefInPkConstraint;
import org.apache.phoenix.parse.ColumnName;
import org.apache.phoenix.parse.CreateFunctionStatement;
import org.apache.phoenix.parse.CreateIndexStatement;
import org.apache.phoenix.parse.CreateSchemaStatement;
import org.apache.phoenix.parse.CreateSequenceStatement;
import org.apache.phoenix.parse.CreateTableStatement;
import org.apache.phoenix.parse.DropColumnStatement;
import org.apache.phoenix.parse.DropFunctionStatement;
import org.apache.phoenix.parse.DropIndexStatement;
import org.apache.phoenix.parse.DropSchemaStatement;
import org.apache.phoenix.parse.DropSequenceStatement;
import org.apache.phoenix.parse.DropTableStatement;
import org.apache.phoenix.parse.IndexKeyConstraint;
import org.apache.phoenix.parse.NamedTableNode;
import org.apache.phoenix.parse.PFunction;
import org.apache.phoenix.parse.PFunction.FunctionArgument;
import org.apache.phoenix.parse.PSchema;
import org.apache.phoenix.parse.ParseNode;
import org.apache.phoenix.parse.ParseNodeFactory;
import org.apache.phoenix.parse.PrimaryKeyConstraint;
import org.apache.phoenix.parse.SQLParser;
import org.apache.phoenix.parse.SelectStatement;
import org.apache.phoenix.parse.TableName;
import org.apache.phoenix.parse.UpdateStatisticsStatement;
import org.apache.phoenix.parse.UseSchemaStatement;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.ConnectionQueryServices.Feature;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.PTable.EncodedCQCounter;
import org.apache.phoenix.schema.PTable.ImmutableStorageScheme;
import org.apache.phoenix.schema.PTable.IndexType;
import org.apache.phoenix.schema.PTable.LinkType;
import org.apache.phoenix.schema.PTable.QualifierEncodingScheme;
import org.apache.phoenix.schema.PTable.QualifierEncodingScheme.QualifierOutOfRangeException;
import org.apache.phoenix.schema.PTable.ViewType;
import org.apache.phoenix.schema.stats.GuidePostsKey;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PDate;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.schema.types.PTimestamp;
import org.apache.phoenix.schema.types.PUnsignedLong;
import org.apache.phoenix.schema.types.PVarbinary;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.EncodedColumnsUtil;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.LogUtil;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.ScanUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.StringUtil;
import org.apache.phoenix.util.TransactionUtil;
import org.apache.phoenix.util.UpgradeUtil;
import org.apache.tephra.TxConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.Iterators;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;

public class MetaDataClient {
    private static final Logger logger = LoggerFactory.getLogger(MetaDataClient.class);

    private static final ParseNodeFactory FACTORY = new ParseNodeFactory();
    private static final String SET_ASYNC_CREATED_DATE =
            "UPSERT INTO " + SYSTEM_CATALOG_SCHEMA + ".\"" + SYSTEM_CATALOG_TABLE + "\"( " +
                    TENANT_ID + "," +
                    TABLE_SCHEM + "," +
                    TABLE_NAME + "," +
                    ASYNC_CREATED_DATE + " " + PDate.INSTANCE.getSqlTypeName() +
                    ") VALUES (?, ?, ?, ?)";
    private static final String CREATE_TABLE =
            "UPSERT INTO " + SYSTEM_CATALOG_SCHEMA + ".\"" + SYSTEM_CATALOG_TABLE + "\"( " +
                    TENANT_ID + "," +
                    TABLE_SCHEM + "," +
                    TABLE_NAME + "," +
                    TABLE_TYPE + "," +
                    TABLE_SEQ_NUM + "," +
                    COLUMN_COUNT + "," +
                    SALT_BUCKETS + "," +
                    PK_NAME + "," +
                    DATA_TABLE_NAME + "," +
                    INDEX_STATE + "," +
                    IMMUTABLE_ROWS + "," +
                    DEFAULT_COLUMN_FAMILY_NAME + "," +
                    VIEW_STATEMENT + "," +
                    DISABLE_WAL + "," +
                    MULTI_TENANT + "," +
                    VIEW_TYPE + "," +
                    INDEX_TYPE + "," +
                    STORE_NULLS + "," +
                    BASE_COLUMN_COUNT + "," +
                    TRANSACTIONAL + "," +
                    UPDATE_CACHE_FREQUENCY + "," +
                    IS_NAMESPACE_MAPPED + "," +
                    AUTO_PARTITION_SEQ +  "," +
                    APPEND_ONLY_SCHEMA + "," +
                    GUIDE_POSTS_WIDTH + "," +
                    IMMUTABLE_STORAGE_SCHEME + "," +
                    ENCODING_SCHEME + 
                    ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    private static final String CREATE_SCHEMA = "UPSERT INTO " + SYSTEM_CATALOG_SCHEMA + ".\"" + SYSTEM_CATALOG_TABLE
            + "\"( " + TABLE_SCHEM + "," + TABLE_NAME + ") VALUES (?,?)";

    private static final String CREATE_LINK =
            "UPSERT INTO " + SYSTEM_CATALOG_SCHEMA + ".\"" + SYSTEM_CATALOG_TABLE + "\"( " +
                    TENANT_ID + "," +
                    TABLE_SCHEM + "," +
                    TABLE_NAME + "," +
                    COLUMN_FAMILY + "," +
                    LINK_TYPE + "," +
                    TABLE_SEQ_NUM +","+ // this is actually set to the parent table's sequence number
                    TABLE_TYPE +
                    ") VALUES (?, ?, ?, ?, ?, ?, ?)";
    
    private static final String CREATE_VIEW_LINK =
            "UPSERT INTO " + SYSTEM_CATALOG_SCHEMA + ".\"" + SYSTEM_CATALOG_TABLE + "\"( " +
                    TENANT_ID + "," +
                    TABLE_SCHEM + "," +
                    TABLE_NAME + "," +
                    COLUMN_FAMILY + "," +
                    LINK_TYPE + "," +
                    PARENT_TENANT_ID + " " + PVarchar.INSTANCE.getSqlTypeName() + // Dynamic column for now to prevent schema change
                    ") VALUES (?, ?, ?, ?, ?, ?)";
    
    public static final String UPDATE_ENCODED_COLUMN_COUNTER = 
            "UPSERT INTO " + SYSTEM_CATALOG_SCHEMA + ".\"" + SYSTEM_CATALOG_TABLE + "\"( " +
            TENANT_ID + ", " + 
            TABLE_SCHEM + "," +
            TABLE_NAME + "," +
            COLUMN_FAMILY + "," +
            COLUMN_QUALIFIER_COUNTER + 
            ") VALUES (?, ?, ?, ?, ?)";

    public static final String INCREMENT_SEQ_NUM =
            "UPSERT INTO " + SYSTEM_CATALOG_SCHEMA + ".\"" + SYSTEM_CATALOG_TABLE + "\"( " +
                    TENANT_ID + "," +
                    TABLE_SCHEM + "," +
                    TABLE_NAME + "," +
                    TABLE_SEQ_NUM  +
                    ") VALUES (?, ?, ?, ?)";
    public static final String MUTATE_TABLE =
            "UPSERT INTO " + SYSTEM_CATALOG_SCHEMA + ".\"" + SYSTEM_CATALOG_TABLE + "\"( " +
                    TENANT_ID + "," +
                    TABLE_SCHEM + "," +
                    TABLE_NAME + "," +
                    TABLE_TYPE + "," +
                    TABLE_SEQ_NUM + "," +
                    COLUMN_COUNT +
                    ") VALUES (?, ?, ?, ?, ?, ?)";
    private static final String UPDATE_INDEX_STATE =
            "UPSERT INTO " + SYSTEM_CATALOG_SCHEMA + ".\"" + SYSTEM_CATALOG_TABLE + "\"( " +
                    TENANT_ID + "," +
                    TABLE_SCHEM + "," +
                    TABLE_NAME + "," +
                    INDEX_STATE + "," +
                    ASYNC_REBUILD_TIMESTAMP + " " + PLong.INSTANCE.getSqlTypeName() +
                    ") VALUES (?, ?, ?, ?, ?)";
    
    private static final String UPDATE_INDEX_REBUILD_ASYNC_STATE =
            "UPSERT INTO " + SYSTEM_CATALOG_SCHEMA + ".\"" + SYSTEM_CATALOG_TABLE + "\"( " +
                    TENANT_ID + "," +
                    TABLE_SCHEM + "," +
                    TABLE_NAME + "," +
                    ASYNC_REBUILD_TIMESTAMP + " " + PLong.INSTANCE.getSqlTypeName() +
                    ") VALUES (?, ?, ?, ?)";
    
    private static final String UPDATE_INDEX_STATE_TO_ACTIVE =
            "UPSERT INTO " + SYSTEM_CATALOG_SCHEMA + ".\"" + SYSTEM_CATALOG_TABLE + "\"( " +
                    TENANT_ID + "," +
                    TABLE_SCHEM + "," +
                    TABLE_NAME + "," +
                    INDEX_STATE + "," +
                    INDEX_DISABLE_TIMESTAMP +","+
                    ASYNC_REBUILD_TIMESTAMP + " " + PLong.INSTANCE.getSqlTypeName() +
                    ") VALUES (?, ?, ?, ?, ?, ?)";
    //TODO: merge INSERT_COLUMN_CREATE_TABLE and INSERT_COLUMN_ALTER_TABLE column when
    // the new major release is out.
    private static final String INSERT_COLUMN_CREATE_TABLE =
            "UPSERT INTO " + SYSTEM_CATALOG_SCHEMA + ".\"" + SYSTEM_CATALOG_TABLE + "\"( " +
                    TENANT_ID + "," +
                    TABLE_SCHEM + "," +
                    TABLE_NAME + "," +
                    COLUMN_NAME + "," +
                    COLUMN_FAMILY + "," +
                    DATA_TYPE + "," +
                    NULLABLE + "," +
                    COLUMN_SIZE + "," +
                    DECIMAL_DIGITS + "," +
                    ORDINAL_POSITION + "," +
                    SORT_ORDER + "," +
                    DATA_TABLE_NAME + "," + // write this both in the column and table rows for access by metadata APIs
                    ARRAY_SIZE + "," +
                    VIEW_CONSTANT + "," +
                    IS_VIEW_REFERENCED + "," +
                    PK_NAME + "," +  // write this both in the column and table rows for access by metadata APIs
                    KEY_SEQ + "," +
                    COLUMN_DEF + "," +
                    COLUMN_QUALIFIER + ", " +
                    IS_ROW_TIMESTAMP +
                    ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    private static final String INSERT_COLUMN_ALTER_TABLE =
            "UPSERT INTO " + SYSTEM_CATALOG_SCHEMA + ".\"" + SYSTEM_CATALOG_TABLE + "\"( " +
                    TENANT_ID + "," +
                    TABLE_SCHEM + "," +
                    TABLE_NAME + "," +
                    COLUMN_NAME + "," +
                    COLUMN_FAMILY + "," +
                    DATA_TYPE + "," +
                    NULLABLE + "," +
                    COLUMN_SIZE + "," +
                    DECIMAL_DIGITS + "," +
                    ORDINAL_POSITION + "," +
                    SORT_ORDER + "," +
                    DATA_TABLE_NAME + "," + // write this both in the column and table rows for access by metadata APIs
                    ARRAY_SIZE + "," +
                    VIEW_CONSTANT + "," +
                    IS_VIEW_REFERENCED + "," +
                    PK_NAME + "," +  // write this both in the column and table rows for access by metadata APIs
                    KEY_SEQ + "," +
                    COLUMN_DEF + "," +
                    COLUMN_QUALIFIER +
                    ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    /*
     * Custom sql to add a column to SYSTEM.CATALOG table during upgrade.
     * We can't use the regular INSERT_COLUMN_ALTER_TABLE sql because the COLUMN_QUALIFIER column
     * was added in 4.10. And so if upgrading from let's say 4.7, we won't be able to
     * find the COLUMN_QUALIFIER column which the INSERT_COLUMN_ALTER_TABLE sql expects.
     */
    private static final String ALTER_SYSCATALOG_TABLE_UPGRADE =
            "UPSERT INTO " + SYSTEM_CATALOG_SCHEMA + ".\"" + SYSTEM_CATALOG_TABLE + "\"( " +
                    TENANT_ID + "," +
                    TABLE_SCHEM + "," +
                    TABLE_NAME + "," +
                    COLUMN_NAME + "," +
                    COLUMN_FAMILY + "," +
                    DATA_TYPE + "," +
                    NULLABLE + "," +
                    COLUMN_SIZE + "," +
                    DECIMAL_DIGITS + "," +
                    ORDINAL_POSITION + "," +
                    SORT_ORDER + "," +
                    DATA_TABLE_NAME + "," + // write this both in the column and table rows for access by metadata APIs
                    ARRAY_SIZE + "," +
                    VIEW_CONSTANT + "," +
                    IS_VIEW_REFERENCED + "," +
                    PK_NAME + "," +  // write this both in the column and table rows for access by metadata APIs
                    KEY_SEQ + "," +
                    COLUMN_DEF +
                    ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    private static final String UPDATE_COLUMN_POSITION =
            "UPSERT INTO " + SYSTEM_CATALOG_SCHEMA + ".\"" + SYSTEM_CATALOG_TABLE + "\" ( " +
                    TENANT_ID + "," +
                    TABLE_SCHEM + "," +
                    TABLE_NAME + "," +
                    COLUMN_NAME + "," +
                    COLUMN_FAMILY + "," +
                    ORDINAL_POSITION +
                    ") VALUES (?, ?, ?, ?, ?, ?)";
    private static final String CREATE_FUNCTION =
            "UPSERT INTO " + SYSTEM_CATALOG_SCHEMA + ".\"" + SYSTEM_FUNCTION_TABLE + "\" ( " +
                    TENANT_ID +","+
                    FUNCTION_NAME + "," +
                    NUM_ARGS + "," +
                    CLASS_NAME + "," +
                    JAR_PATH + "," +
                    RETURN_TYPE +
                    ") VALUES (?, ?, ?, ?, ?, ?)";
    private static final String INSERT_FUNCTION_ARGUMENT =
            "UPSERT INTO " + SYSTEM_CATALOG_SCHEMA + ".\"" + SYSTEM_FUNCTION_TABLE + "\" ( " +
                    TENANT_ID +","+
                    FUNCTION_NAME + "," +
                    TYPE + "," +
                    ARG_POSITION +","+
                    IS_ARRAY + "," +
                    IS_CONSTANT  + "," +
                    DEFAULT_VALUE + "," +
                    MIN_VALUE + "," +
                    MAX_VALUE +
                    ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";

    public static final String EMPTY_TABLE = " ";


    private final PhoenixConnection connection;

    public MetaDataClient(PhoenixConnection connection) {
        this.connection = connection;
    }

    public PhoenixConnection getConnection() {
        return connection;
    }

    public long getCurrentTime(String schemaName, String tableName) throws SQLException {
        MetaDataMutationResult result = updateCache(schemaName, tableName, true);
        return result.getMutationTime();
    }

    /**
     * Update the cache with the latest as of the connection scn.
     * @param schemaName
     * @param tableName
     * @return the timestamp from the server, negative if the table was added to the cache and positive otherwise
     * @throws SQLException
     */
    public MetaDataMutationResult updateCache(String schemaName, String tableName) throws SQLException {
        return updateCache(schemaName, tableName, false);
    }

    private MetaDataMutationResult updateCache(String schemaName, String tableName, boolean alwaysHitServer) throws SQLException {
        return updateCache(connection.getTenantId(), schemaName, tableName, alwaysHitServer);
    }

    public MetaDataMutationResult updateCache(PName tenantId, String schemaName, String tableName) throws SQLException {
        return updateCache(tenantId, schemaName, tableName, false);
    }

    public MetaDataMutationResult updateCache(PName tenantId, String schemaName, String tableName, boolean alwaysHitServer) throws SQLException {
        return updateCache(tenantId, schemaName, tableName, alwaysHitServer, null);
    }

    /**
     * Update the cache with the latest as of the connection scn.
     * @param functioNames
     * @return the timestamp from the server, negative if the function was added to the cache and positive otherwise
     * @throws SQLException
     */
    public MetaDataMutationResult updateCache(List<String> functionNames) throws SQLException {
        return updateCache(functionNames, false);
    }

    private MetaDataMutationResult updateCache(List<String> functionNames, boolean alwaysHitServer) throws SQLException {
        return updateCache(connection.getTenantId(), functionNames, alwaysHitServer);
    }

    public MetaDataMutationResult updateCache(PName tenantId, List<String> functionNames) throws SQLException {
        return updateCache(tenantId, functionNames, false);
    }

    private long getClientTimeStamp() {
        Long scn = connection.getSCN();
        long clientTimeStamp = scn == null ? HConstants.LATEST_TIMESTAMP : scn;
        return clientTimeStamp;
    }

    private long getCurrentScn() {
        Long scn = connection.getSCN();
        long currentScn = scn == null ? HConstants.LATEST_TIMESTAMP : scn;
        return currentScn;
    }

    private MetaDataMutationResult updateCache(PName origTenantId, String schemaName, String tableName,
            boolean alwaysHitServer, Long resolvedTimestamp) throws SQLException { // TODO: pass byte[] herez
        boolean systemTable = SYSTEM_CATALOG_SCHEMA.equals(schemaName);
        // System tables must always have a null tenantId
        PName tenantId = systemTable ? null : origTenantId;
        PTable table = null;
        PTableRef tableRef = null;
        String fullTableName = SchemaUtil.getTableName(schemaName, tableName);
        long tableTimestamp = HConstants.LATEST_TIMESTAMP;
        long tableResolvedTimestamp = HConstants.LATEST_TIMESTAMP;
        try {
            tableRef = connection.getTableRef(new PTableKey(tenantId, fullTableName));
            table = tableRef.getTable();
            tableTimestamp = table.getTimeStamp();
            tableResolvedTimestamp = tableRef.getResolvedTimeStamp();
        } catch (TableNotFoundException e) {
        }

        boolean defaultTransactional = connection.getQueryServices().getProps().getBoolean(
                QueryServices.DEFAULT_TABLE_ISTRANSACTIONAL_ATTRIB,
                QueryServicesOptions.DEFAULT_TRANSACTIONAL);
        // start a txn if all table are transactional by default or if we found the table in the cache and it is transactional
        // TODO if system tables become transactional remove the check
        boolean isTransactional = defaultTransactional || (table!=null && table.isTransactional());
        if (!systemTable && isTransactional && !connection.getMutationState().isTransactionStarted()) {
            connection.getMutationState().startTransaction();
        }
        resolvedTimestamp = resolvedTimestamp==null ? TransactionUtil.getResolvedTimestamp(connection, isTransactional, HConstants.LATEST_TIMESTAMP) : resolvedTimestamp;
        // Do not make rpc to getTable if
        // 1. table is a system table
        // 2. table was already resolved as of that timestamp
        if (table != null && !alwaysHitServer
                && (systemTable || resolvedTimestamp == tableResolvedTimestamp || connection.getMetaDataCache().getAge(tableRef) < table.getUpdateCacheFrequency())) {
            return new MetaDataMutationResult(MutationCode.TABLE_ALREADY_EXISTS, QueryConstants.UNSET_TIMESTAMP, table);
        }

        int maxTryCount = tenantId == null ? 1 : 2;
        int tryCount = 0;
        MetaDataMutationResult result;

        do {
            final byte[] schemaBytes = PVarchar.INSTANCE.toBytes(schemaName);
            final byte[] tableBytes = PVarchar.INSTANCE.toBytes(tableName);
            ConnectionQueryServices queryServices = connection.getQueryServices();
            result = queryServices.getTable(tenantId, schemaBytes, tableBytes, tableTimestamp, resolvedTimestamp);
            // if the table was assumed to be transactional, but is actually not transactional then re-resolve as of the right timestamp (and vice versa)
            if (table==null && result.getTable()!=null && result.getTable().isTransactional()!=isTransactional) {
                result = queryServices.getTable(tenantId, schemaBytes, tableBytes, tableTimestamp, TransactionUtil.getResolvedTimestamp(connection, result.getTable().isTransactional(), HConstants.LATEST_TIMESTAMP));
            }

            if (SYSTEM_CATALOG_SCHEMA.equals(schemaName)) {
                return result;
            }
            MutationCode code = result.getMutationCode();
            PTable resultTable = result.getTable();
            // We found an updated table, so update our cache
            if (resultTable != null) {
                // Cache table, even if multi-tenant table found for null tenant_id
                // These may be accessed by tenant-specific connections, as the
                // tenant_id will always be added to mask other tenants data.
                // Otherwise, a tenant would be required to create a VIEW first
                // which is not really necessary unless you want to filter or add
                // columns
                addTableToCache(result);
                return result;
            } else {
                // if (result.getMutationCode() == MutationCode.NEWER_TABLE_FOUND) {
                // TODO: No table exists at the clientTimestamp, but a newer one exists.
                // Since we disallow creation or modification of a table earlier than the latest
                // timestamp, we can handle this such that we don't ask the
                // server again.
                if (table != null) {
                    // Ensures that table in result is set to table found in our cache.
                    if (code == MutationCode.TABLE_ALREADY_EXISTS) {
                        result.setTable(table);
                        // Although this table is up-to-date, the parent table may not be.
                        // In this case, we update the parent table which may in turn pull
                        // in indexes to add to this table.
                        long resolvedTime = TransactionUtil.getResolvedTime(connection, result);
                        if (addIndexesFromParentTable(result, resolvedTimestamp)) {
                            connection.addTable(result.getTable(), resolvedTime);
                        }
                        else {
                            // if we aren't adding the table, we still need to update the resolved time of the table
                            connection.updateResolvedTimestamp(table, resolvedTime);
                        }
                        return result;
                    }
                    // If table was not found at the current time stamp and we have one cached, remove it.
                    // Otherwise, we're up to date, so there's nothing to do.
                    if (code == MutationCode.TABLE_NOT_FOUND && tryCount + 1 == maxTryCount) {
                        connection.removeTable(origTenantId, fullTableName, table.getParentName() == null ? null : table.getParentName().getString(), table.getTimeStamp());
                    }
                }
            }
            tenantId = null; // Try again with global tenantId
        } while (++tryCount < maxTryCount);

        return result;
    }

    public MetaDataMutationResult updateCache(String schemaName) throws SQLException {
        return updateCache(schemaName, false);
    }

    public MetaDataMutationResult updateCache(String schemaName, boolean alwaysHitServer) throws SQLException {
        long clientTimeStamp = getClientTimeStamp();
        PSchema schema = null;
        try {
            schema = connection.getMetaDataCache().getSchema(new PTableKey(null, schemaName));
            if (schema != null
                    && !alwaysHitServer) { return new MetaDataMutationResult(MutationCode.SCHEMA_ALREADY_EXISTS, schema,
                            QueryConstants.UNSET_TIMESTAMP); }
        } catch (SchemaNotFoundException e) {

        }
        MetaDataMutationResult result;

        result = connection.getQueryServices().getSchema(schemaName, clientTimeStamp);
        return result;
    }

    private MetaDataMutationResult updateCache(PName tenantId, List<String> functionNames,
            boolean alwaysHitServer) throws SQLException { // TODO: pass byte[] herez
        long clientTimeStamp = getClientTimeStamp();
        List<PFunction> functions = new ArrayList<PFunction>(functionNames.size());
        List<Long> functionTimeStamps = new ArrayList<Long>(functionNames.size());
        Iterator<String> iterator = functionNames.iterator();
        while (iterator.hasNext()) {
            PFunction function = null;
            try {
                String functionName = iterator.next();
                function =
                        connection.getMetaDataCache().getFunction(
                                new PTableKey(tenantId, functionName));
                if (function != null && !alwaysHitServer
                        && function.getTimeStamp() == clientTimeStamp - 1) {
                    functions.add(function);
                    iterator.remove();
                    continue;
                }
                if (function != null && function.getTimeStamp() != clientTimeStamp - 1) {
                    functionTimeStamps.add(function.getTimeStamp());
                } else {
                    functionTimeStamps.add(HConstants.LATEST_TIMESTAMP);
                }
            } catch (FunctionNotFoundException e) {
                functionTimeStamps.add(HConstants.LATEST_TIMESTAMP);
            }
        }
        // Don't bother with server call: we can't possibly find a newer function
        if (functionNames.isEmpty()) {
            return new MetaDataMutationResult(MutationCode.FUNCTION_ALREADY_EXISTS,QueryConstants.UNSET_TIMESTAMP,functions, true);
        }

        int maxTryCount = tenantId == null ? 1 : 2;
        int tryCount = 0;
        MetaDataMutationResult result;

        do {
            List<Pair<byte[], Long>> functionsToFecth = new ArrayList<Pair<byte[], Long>>(functionNames.size());
            for(int i = 0; i< functionNames.size(); i++) {
                functionsToFecth.add(new Pair<byte[], Long>(PVarchar.INSTANCE.toBytes(functionNames.get(i)), functionTimeStamps.get(i)));
            }
            result = connection.getQueryServices().getFunctions(tenantId, functionsToFecth, clientTimeStamp);

            MutationCode code = result.getMutationCode();
            // We found an updated table, so update our cache
            if (result.getFunctions() != null && !result.getFunctions().isEmpty()) {
                result.getFunctions().addAll(functions);
                addFunctionToCache(result);
                return result;
            } else {
                if (code == MutationCode.FUNCTION_ALREADY_EXISTS) {
                    result.getFunctions().addAll(functions);
                    addFunctionToCache(result);
                    return result;
                }
                if (code == MutationCode.FUNCTION_NOT_FOUND && tryCount + 1 == maxTryCount) {
                    for (Pair<byte[], Long> f : functionsToFecth) {
                        connection.removeFunction(tenantId, Bytes.toString(f.getFirst()),
                                f.getSecond());
                    }
                    // TODO removeFunctions all together from cache when
                    throw new FunctionNotFoundException(functionNames.toString() + " not found");
                }
            }
            tenantId = null; // Try again with global tenantId
        } while (++tryCount < maxTryCount);

        return result;
    }

    /**
     * Fault in the parent table to the cache and add any indexes it has to the indexes
     * of the table for which we just updated.
     * TODO: combine this round trip with the one that updates the cache for the child table.
     * @param result the result from updating the cache for the current table.
     * @param resolvedTimestamp timestamp at which child table was resolved
     * @return true if the PTable contained by result was modified and false otherwise
     * @throws SQLException if the physical table cannot be found
     */
    private boolean addIndexesFromParentTable(MetaDataMutationResult result, Long resolvedTimestamp) throws SQLException {
        PTable view = result.getTable();
        // If not a view or if a view directly over an HBase table, there's nothing to do
        if (view.getType() != PTableType.VIEW || view.getViewType() == ViewType.MAPPED) {
            return false;
        }
        // a view on a table will not have a parent name but will have a physical table name (which is the parent)
        String parentName = view.getParentName().getString();
        String schemaName = SchemaUtil.getSchemaNameFromFullName(parentName);
        String tableName = SchemaUtil.getTableNameFromFullName(parentName);
        MetaDataMutationResult parentResult = updateCache(connection.getTenantId(), schemaName, tableName, false, resolvedTimestamp);
        PTable parentTable = parentResult.getTable();
        if (parentTable == null) {
            throw new TableNotFoundException(schemaName, tableName);
        }
        if (!result.wasUpdated() && !parentResult.wasUpdated()) {
            return false;
        }
        List<PTable> parentTableIndexes = parentTable.getIndexes();
        if (parentTableIndexes.isEmpty()) {
            return false;
        }
        // Filter out indexes if column doesn't exist in view
        List<PTable> indexesToAdd = Lists.newArrayListWithExpectedSize(parentTableIndexes.size() + view.getIndexes().size());
        if (result.wasUpdated()) { // Table from server never contains inherited indexes
            indexesToAdd.addAll(view.getIndexes());
        } else { // Only add original ones, as inherited ones may have changed
            for (PTable index : view.getIndexes()) {
                // Original indexes will not have a view statement while inherited ones will
                if (index.getViewStatement() == null) {
                    indexesToAdd.add(index);
                }
            }
        }
        for (PTable index : parentTableIndexes) {
            boolean containsAllReqdCols = true;
            // Ensure that all columns required to create index exist in the view too,
            // since view columns may be removed.
            IndexMaintainer indexMaintainer = index.getIndexMaintainer(parentTable, connection);
            // Check that the columns required for the index pk are present in the view
            Set<Pair<String, String>> indexedColInfos = indexMaintainer.getIndexedColumnInfo();
            for (Pair<String, String> colInfo : indexedColInfos) {
                try {
                    String colFamily = colInfo.getFirst();
                    String colName = colInfo.getSecond();
                    if (colFamily == null) {
                        view.getColumnForColumnName(colName);
                    } else {
                        view.getColumnFamily(colFamily).getPColumnForColumnName(colName);
                    }
                } catch (ColumnNotFoundException e) {
                    containsAllReqdCols = false;
                    break;
                }
            }
            
            // Ensure that constant columns (i.e. columns matched in the view WHERE clause)
            // all exist in the index on the parent table.
            for (PColumn col : view.getColumns()) {
                if (col.getViewConstant() != null) {
                    try {
                        // It'd be possible to use a local index that doesn't have all view constants,
                        // but the WHERE clause for the view statement (which is added to the index below)
                        // would fail to compile.
                        String indexColumnName = IndexUtil.getIndexColumnName(col);
                        index.getColumnForColumnName(indexColumnName);
                    } catch (ColumnNotFoundException e1) {
                        PColumn indexCol = null;
                        try {
                            String cf = col.getFamilyName()!=null ? col.getFamilyName().getString() : null;
                            String colName = col.getName().getString();
                            if (cf != null) {
                                indexCol = parentTable.getColumnFamily(cf).getPColumnForColumnName(colName);
                            }
                            else {
                                indexCol = parentTable.getColumnForColumnName(colName);
                            }
                        } catch (ColumnNotFoundException e2) { // Ignore this index and continue with others
                            containsAllReqdCols = false;
                            break;
                        }
                        if (indexCol.getViewConstant()==null || Bytes.compareTo(indexCol.getViewConstant(), col.getViewConstant())!=0) {
                            containsAllReqdCols = false;
                            break;
                        }
                    }
                }
            }
            if (containsAllReqdCols) {
                // Tack on view statement to index to get proper filtering for view
                String viewStatement = IndexUtil.rewriteViewStatement(connection, index, parentTable, view.getViewStatement());
                PName modifiedIndexName = PNameFactory.newName(index.getSchemaName().getString() + QueryConstants.CHILD_VIEW_INDEX_NAME_SEPARATOR
                        + index.getName().getString() + QueryConstants.CHILD_VIEW_INDEX_NAME_SEPARATOR + view.getName().getString());
                // add the index table with a new name so that it does not conflict with the existing index table
                // also set update cache frequency to never since the renamed index is not present on the server
                indexesToAdd.add(PTableImpl.makePTable(index, modifiedIndexName, viewStatement, Long.MAX_VALUE, view.getTenantId()));
            }
        }
        PTable allIndexesTable = PTableImpl.makePTable(view, view.getTimeStamp(), indexesToAdd);
        result.setTable(allIndexesTable);
        return true;
    }

    private void addColumnMutation(String schemaName, String tableName, PColumn column, PreparedStatement colUpsert, String parentTableName, String pkName, Short keySeq, boolean isSalted) throws SQLException {
        colUpsert.setString(1, connection.getTenantId() == null ? null : connection.getTenantId().getString());
        colUpsert.setString(2, schemaName);
        colUpsert.setString(3, tableName);
        colUpsert.setString(4, column.getName().getString());
        colUpsert.setString(5, column.getFamilyName() == null ? null : column.getFamilyName().getString());
        colUpsert.setInt(6, column.getDataType().getSqlType());
        colUpsert.setInt(7, column.isNullable() ? ResultSetMetaData.columnNullable : ResultSetMetaData.columnNoNulls);
        if (column.getMaxLength() == null) {
            colUpsert.setNull(8, Types.INTEGER);
        } else {
            colUpsert.setInt(8, column.getMaxLength());
        }
        if (column.getScale() == null) {
            colUpsert.setNull(9, Types.INTEGER);
        } else {
            colUpsert.setInt(9, column.getScale());
        }
        colUpsert.setInt(10, column.getPosition() + (isSalted ? 0 : 1));
        colUpsert.setInt(11, column.getSortOrder().getSystemValue());
        colUpsert.setString(12, parentTableName);
        if (column.getArraySize() == null) {
            colUpsert.setNull(13, Types.INTEGER);
        } else {
            colUpsert.setInt(13, column.getArraySize());
        }
        colUpsert.setBytes(14, column.getViewConstant());
        colUpsert.setBoolean(15, column.isViewReferenced());
        colUpsert.setString(16, pkName);
        if (keySeq == null) {
            colUpsert.setNull(17, Types.SMALLINT);
        } else {
            colUpsert.setShort(17, keySeq);
        }
        if (column.getExpressionStr() == null) {
            colUpsert.setNull(18, Types.VARCHAR);
        } else {
            colUpsert.setString(18, column.getExpressionStr());
        }
        if (colUpsert.getParameterMetaData().getParameterCount() > 18) {
            if (column.getColumnQualifierBytes() == null) {
                colUpsert.setNull(19, Types.VARBINARY);
            } else {
                colUpsert.setBytes(19, column.getColumnQualifierBytes());
            }
        }
        if (colUpsert.getParameterMetaData().getParameterCount() > 19) {
            colUpsert.setBoolean(20, column.isRowTimestamp());
        }
        colUpsert.execute();
    }

    private void addFunctionArgMutation(String functionName, FunctionArgument arg, PreparedStatement argUpsert, int position) throws SQLException {
        argUpsert.setString(1, connection.getTenantId() == null ? null : connection.getTenantId().getString());
        argUpsert.setString(2, functionName);
        argUpsert.setString(3, arg.getArgumentType());
        byte[] bytes = Bytes.toBytes((short)position);
        argUpsert.setBytes(4, bytes);
        argUpsert.setBoolean(5, arg.isArrayType());
        argUpsert.setBoolean(6, arg.isConstant());
        argUpsert.setString(7, arg.getDefaultValue() == null? null: arg.getDefaultValue().toString());
        argUpsert.setString(8, arg.getMinValue() == null? null: arg.getMinValue().toString());
        argUpsert.setString(9, arg.getMaxValue() == null? null: arg.getMaxValue().toString());
        argUpsert.execute();
    }

    private PColumn newColumn(int position, ColumnDef def, PrimaryKeyConstraint pkConstraint, String defaultColumnFamily, boolean addingToPK, byte[] columnQualifierBytes) throws SQLException {
        try {
            ColumnName columnDefName = def.getColumnDefName();
            SortOrder sortOrder = def.getSortOrder();
            boolean isPK = def.isPK();
            boolean isRowTimestamp = def.isRowTimestamp();
            if (pkConstraint != null) {
                Pair<ColumnName, SortOrder> pkSortOrder = pkConstraint.getColumnWithSortOrder(columnDefName);
                if (pkSortOrder != null) {
                    isPK = true;
                    sortOrder = pkSortOrder.getSecond();
                    isRowTimestamp = pkConstraint.isColumnRowTimestamp(columnDefName);
                }
            }
            String columnName = columnDefName.getColumnName();
            if (isPK && sortOrder == SortOrder.DESC && def.getDataType() == PVarbinary.INSTANCE) {
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.DESC_VARBINARY_NOT_SUPPORTED)
                .setColumnName(columnName)
                .build().buildException();
            }

            PName familyName = null;
            if (def.isPK() && !pkConstraint.getColumnNames().isEmpty() ) {
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.PRIMARY_KEY_ALREADY_EXISTS)
                .setColumnName(columnName).build().buildException();
            }
            boolean isNull = def.isNull();
            if (def.getColumnDefName().getFamilyName() != null) {
                String family = def.getColumnDefName().getFamilyName();
                if (isPK) {
                    throw new SQLExceptionInfo.Builder(SQLExceptionCode.PRIMARY_KEY_WITH_FAMILY_NAME)
                    .setColumnName(columnName).setFamilyName(family).build().buildException();
                } else if (!def.isNull()) {
                    throw new SQLExceptionInfo.Builder(SQLExceptionCode.KEY_VALUE_NOT_NULL)
                    .setColumnName(columnName).setFamilyName(family).build().buildException();
                }
                familyName = PNameFactory.newName(family);
            } else if (!isPK) {
                familyName = PNameFactory.newName(defaultColumnFamily == null ? QueryConstants.DEFAULT_COLUMN_FAMILY : defaultColumnFamily);
            }

            if (isPK && !addingToPK && pkConstraint.getColumnNames().size() <= 1) {
                if (def.isNull() && def.isNullSet()) {
                    throw new SQLExceptionInfo.Builder(SQLExceptionCode.SINGLE_PK_MAY_NOT_BE_NULL)
                    .setColumnName(columnName).build().buildException();
                }
                isNull = false;
            }
            PColumn column = new PColumnImpl(PNameFactory.newName(columnName), familyName, def.getDataType(),
                    def.getMaxLength(), def.getScale(), isNull, position, sortOrder, def.getArraySize(), null, false, def.getExpression(), isRowTimestamp, false, columnQualifierBytes);
            return column;
        } catch (IllegalArgumentException e) { // Based on precondition check in constructor
            throw new SQLException(e);
        }
    }
    
    public MutationState createTable(CreateTableStatement statement, byte[][] splits, PTable parent, String viewStatement, ViewType viewType, byte[][] viewColumnConstants, BitSet isViewColumnReferenced) throws SQLException {
        TableName tableName = statement.getTableName();
        Map<String,Object> tableProps = Maps.newHashMapWithExpectedSize(statement.getProps().size());
        Map<String,Object> commonFamilyProps = Maps.newHashMapWithExpectedSize(statement.getProps().size() + 1);
        populatePropertyMaps(statement.getProps(), tableProps, commonFamilyProps);

        boolean isAppendOnlySchema = false;
        long updateCacheFrequency = 0;
        if (parent==null) {
	        Boolean appendOnlySchemaProp = (Boolean) TableProperty.APPEND_ONLY_SCHEMA.getValue(tableProps);
	        if (appendOnlySchemaProp != null) {
	            isAppendOnlySchema = appendOnlySchemaProp;
	        }
	        Long updateCacheFrequencyProp = (Long) TableProperty.UPDATE_CACHE_FREQUENCY.getValue(tableProps);
	        if (updateCacheFrequencyProp != null) {
	            updateCacheFrequency = updateCacheFrequencyProp;
	        }
        }
        else {
        	isAppendOnlySchema = parent.isAppendOnlySchema();
        	updateCacheFrequency = parent.getUpdateCacheFrequency();
        }
        // updateCacheFrequency cannot be set to ALWAYS if isAppendOnlySchema is true
        if (isAppendOnlySchema && updateCacheFrequency==0) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.UPDATE_CACHE_FREQUENCY_INVALID)
            .setSchemaName(tableName.getSchemaName()).setTableName(tableName.getTableName())
            .build().buildException();
        }
        Boolean immutableProp = (Boolean) TableProperty.IMMUTABLE_ROWS.getValue(tableProps);
        if (statement.immutableRows()!=null && immutableProp!=null) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.IMMUTABLE_TABLE_PROPERTY_INVALID)
            .setSchemaName(tableName.getSchemaName()).setTableName(tableName.getTableName())
            .build().buildException();
        }

        PTable table = null;
        // if the APPEND_ONLY_SCHEMA attribute is true first check if the table is present in the cache
        // if it is add columns that are not already present
        if (isAppendOnlySchema) {
            // look up the table in the cache
            MetaDataMutationResult result = updateCache(tableName.getSchemaName(), tableName.getTableName());
            if (result.getMutationCode()==MutationCode.TABLE_ALREADY_EXISTS) {
                table = result.getTable();
                if (!statement.ifNotExists()) {
                    throw new NewerTableAlreadyExistsException(tableName.getSchemaName(), tableName.getTableName(), table);
                }

                List<ColumnDef> columnDefs = statement.getColumnDefs();
                PrimaryKeyConstraint pkConstraint = statement.getPrimaryKeyConstraint();
                // get the list of columns to add
                for (ColumnDef columnDef : columnDefs) {
                    if (pkConstraint.contains(columnDef.getColumnDefName())) {
                        columnDef.setIsPK(true);
                    }
                }
                // if there are new columns to add
                return addColumn(table, columnDefs, statement.getProps(), statement.ifNotExists(),
                        true, NamedTableNode.create(statement.getTableName()), statement.getTableType());
            }
        }
        table = createTableInternal(statement, splits, parent, viewStatement, viewType, viewColumnConstants, isViewColumnReferenced, false, null, null, tableProps, commonFamilyProps);

        if (table == null || table.getType() == PTableType.VIEW /*|| table.isTransactional()*/) {
            return new MutationState(0,connection);
        }
        // Hack to get around the case when an SCN is specified on the connection.
        // In this case, we won't see the table we just created yet, so we hack
        // around it by forcing the compiler to not resolve anything.
        PostDDLCompiler compiler = new PostDDLCompiler(connection);
        //connection.setAutoCommit(true);
        // Execute any necessary data updates
        Long scn = connection.getSCN();
        long ts = (scn == null ? table.getTimeStamp() : scn);
        // Getting the schema through the current connection doesn't work when the connection has an scn specified
        // Since the table won't be added to the current connection.
        TableRef tableRef = new TableRef(null, table, ts, false);
        byte[] emptyCF = SchemaUtil.getEmptyColumnFamily(table);
        MutationPlan plan = compiler.compile(Collections.singletonList(tableRef), emptyCF, null, null, tableRef.getTimeStamp());
        return connection.getQueryServices().updateData(plan);
    }

    private void populatePropertyMaps(ListMultimap<String,Pair<String,Object>> props, Map<String, Object> tableProps,
            Map<String, Object> commonFamilyProps) {
        // Somewhat hacky way of determining if property is for HColumnDescriptor or HTableDescriptor
        HColumnDescriptor defaultDescriptor = new HColumnDescriptor(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES);
        if (!props.isEmpty()) {
            Collection<Pair<String,Object>> propsList = props.get(QueryConstants.ALL_FAMILY_PROPERTIES_KEY);
            for (Pair<String,Object> prop : propsList) {
                if (defaultDescriptor.getValue(prop.getFirst()) == null) {
                    tableProps.put(prop.getFirst(), prop.getSecond());
                } else {
                    commonFamilyProps.put(prop.getFirst(), prop.getSecond());
                }
            }
        }
    }

    public MutationState updateStatistics(UpdateStatisticsStatement updateStatisticsStmt)
            throws SQLException {
        // Don't mistakenly commit pending rows
        connection.rollback();
        // Check before updating the stats if we have reached the configured time to reupdate the stats once again
        ColumnResolver resolver = FromCompiler.getResolver(updateStatisticsStmt, connection);
        PTable table = resolver.getTables().get(0).getTable();
        long rowCount = 0;
        if (updateStatisticsStmt.updateColumns()) {
            rowCount += updateStatisticsInternal(table.getPhysicalName(), table, updateStatisticsStmt.getProps());
        }
        if (updateStatisticsStmt.updateIndex()) {
            // TODO: If our table is a VIEW with multiple indexes or a TABLE with local indexes,
            // we may be doing more work that we have to here. We should union the scan ranges
            // across all indexes in that case so that we don't re-calculate the same stats
            // multiple times.
            for (PTable index : table.getIndexes()) {
                rowCount += updateStatisticsInternal(index.getPhysicalName(), index, updateStatisticsStmt.getProps());
            }
            // If analyzing the indexes of a multi-tenant table or a table with view indexes
            // then analyze all of those indexes too.
            if (table.getType() != PTableType.VIEW) {
                if (table.isMultiTenant() || MetaDataUtil.hasViewIndexTable(connection, table.getPhysicalName())) {
                    final PName physicalName = PNameFactory.newName(MetaDataUtil.getViewIndexPhysicalName(table.getPhysicalName().getBytes()));
                    PTable indexLogicalTable = new DelegateTable(table) {
                        @Override
                        public PName getPhysicalName() {
                            return physicalName;
                        }
                    };
                    rowCount += updateStatisticsInternal(physicalName, indexLogicalTable, updateStatisticsStmt.getProps());
                }
                PName physicalName = table.getPhysicalName();
                List<byte[]> localCFs = MetaDataUtil.getLocalIndexColumnFamilies(connection, physicalName.getBytes());
                if (!localCFs.isEmpty()) {
                    rowCount += updateStatisticsInternal(physicalName, table, updateStatisticsStmt.getProps(), localCFs);
                }
            }
        }
        final long count = rowCount;
        return new MutationState(1, connection) {
            @Override
            public long getUpdateCount() {
                return count;
            }
        };
    }

    private long updateStatisticsInternal(PName physicalName, PTable logicalTable, Map<String, Object> statsProps) throws SQLException {
        return updateStatisticsInternal(physicalName, logicalTable, statsProps, null);
    }
    
    private long updateStatisticsInternal(PName physicalName, PTable logicalTable, Map<String, Object> statsProps, List<byte[]> cfs) throws SQLException {
        ReadOnlyProps props = connection.getQueryServices().getProps();
        final long msMinBetweenUpdates = props
                .getLong(QueryServices.MIN_STATS_UPDATE_FREQ_MS_ATTRIB,
                        props.getLong(QueryServices.STATS_UPDATE_FREQ_MS_ATTRIB,
                                QueryServicesOptions.DEFAULT_STATS_UPDATE_FREQ_MS) / 2);
        byte[] tenantIdBytes = ByteUtil.EMPTY_BYTE_ARRAY;
        Long scn = connection.getSCN();
        // Always invalidate the cache
        long clientTimeStamp = connection.getSCN() == null ? HConstants.LATEST_TIMESTAMP : scn;
        String query = "SELECT CURRENT_DATE()," + LAST_STATS_UPDATE_TIME + " FROM " + PhoenixDatabaseMetaData.SYSTEM_STATS_NAME
                + " WHERE " + PHYSICAL_NAME + "='" + physicalName.getString() + "' AND " + COLUMN_FAMILY
                + " IS NULL AND " + LAST_STATS_UPDATE_TIME + " IS NOT NULL";
        ResultSet rs = connection.createStatement().executeQuery(query);
        long msSinceLastUpdate = Long.MAX_VALUE;
        if (rs.next()) {
            msSinceLastUpdate = rs.getLong(1) - rs.getLong(2);
        }
        long rowCount = 0;
        if (msSinceLastUpdate >= msMinBetweenUpdates) {
            /*
             * Execute a COUNT(*) through PostDDLCompiler as we need to use the logicalTable passed through,
             * since it may not represent a "real" table in the case of the view indexes of a base table.
             */
            PostDDLCompiler compiler = new PostDDLCompiler(connection);
            //even if table is transactional, while calculating stats we scan the table non-transactionally to
            //view all the data belonging to the table
            PTable nonTxnLogicalTable = new DelegateTable(logicalTable) {
                @Override
                public boolean isTransactional() {
                    return false;
                }
            };
            TableRef tableRef = new TableRef(null, nonTxnLogicalTable, clientTimeStamp, false);
            MutationPlan plan = compiler.compile(Collections.singletonList(tableRef), null, cfs, null, clientTimeStamp);
            Scan scan = plan.getContext().getScan();
            scan.setCacheBlocks(false);
            scan.setAttribute(ANALYZE_TABLE, TRUE_BYTES);
            boolean runUpdateStatsAsync = props.getBoolean(QueryServices.RUN_UPDATE_STATS_ASYNC, DEFAULT_RUN_UPDATE_STATS_ASYNC);
            scan.setAttribute(RUN_UPDATE_STATS_ASYNC_ATTRIB, runUpdateStatsAsync ? TRUE_BYTES : FALSE_BYTES);
            if (statsProps != null) {
                Object gp_width = statsProps.get(QueryServices.STATS_GUIDEPOST_WIDTH_BYTES_ATTRIB);
                if (gp_width != null) {
                    scan.setAttribute(BaseScannerRegionObserver.GUIDEPOST_WIDTH_BYTES, PLong.INSTANCE.toBytes(gp_width));
                }
                Object gp_per_region = statsProps.get(QueryServices.STATS_GUIDEPOST_PER_REGION_ATTRIB);
                if (gp_per_region != null) {
                    scan.setAttribute(BaseScannerRegionObserver.GUIDEPOST_PER_REGION, PInteger.INSTANCE.toBytes(gp_per_region));
                }
            }
            MutationState mutationState = plan.execute();
            rowCount = mutationState.getUpdateCount();
        }

        /*
         *  Update the stats table so that client will pull the new one with the updated stats.
         *  Even if we don't run the command due to the last update time, invalidate the cache.
         *  This supports scenarios in which a major compaction was manually initiated and the
         *  client wants the modified stats to be reflected immediately.
         */
        if (cfs == null) {
            List<PColumnFamily> families = logicalTable.getColumnFamilies();
            if (families.isEmpty()) {
                connection.getQueryServices().invalidateStats(new GuidePostsKey(physicalName.getBytes(), SchemaUtil.getEmptyColumnFamily(logicalTable)));
            } else {
                for (PColumnFamily family : families) {
                    connection.getQueryServices().invalidateStats(new GuidePostsKey(physicalName.getBytes(), family.getName().getBytes()));
                }
            }
        } else {
            for (byte[] cf : cfs) {
                connection.getQueryServices().invalidateStats(new GuidePostsKey(physicalName.getBytes(), cf));
            }
        }
        return rowCount;
    }

    private MutationState buildIndexAtTimeStamp(PTable index, NamedTableNode dataTableNode) throws SQLException {
        // If our connection is at a fixed point-in-time, we need to open a new
        // connection so that our new index table is visible.
        Properties props = new Properties(connection.getClientInfo());
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(connection.getSCN()+1));
        PhoenixConnection conn = new PhoenixConnection(connection, connection.getQueryServices(), props);
        MetaDataClient newClientAtNextTimeStamp = new MetaDataClient(conn);

        // Re-resolve the tableRef from the now newer connection
        conn.setAutoCommit(true);
        ColumnResolver resolver = FromCompiler.getResolver(dataTableNode, conn);
        TableRef tableRef = resolver.getTables().get(0);
        boolean success = false;
        SQLException sqlException = null;
        try {
            MutationState state = newClientAtNextTimeStamp.buildIndex(index, tableRef);
            success = true;
            return state;
        } catch (SQLException e) {
            sqlException = e;
        } finally {
            try {
                conn.close();
            } catch (SQLException e) {
                if (sqlException == null) {
                    // If we're not in the middle of throwing another exception
                    // then throw the exception we got on close.
                    if (success) {
                        sqlException = e;
                    }
                } else {
                    sqlException.setNextException(e);
                }
            }
            if (sqlException != null) {
                throw sqlException;
            }
        }
        throw new IllegalStateException(); // impossible
    }
    
    /**
     * For new mutations only should not be used if there are deletes done in the data table between start time and end
     * time passed to the method.
     */
    public MutationState buildIndex(PTable index, TableRef dataTableRef, long startTime, long EndTime)
            throws SQLException {
        boolean wasAutoCommit = connection.getAutoCommit();
        try {
            AlterIndexStatement indexStatement = FACTORY
                    .alterIndex(
                            FACTORY.namedTable(null,
                                    TableName.create(index.getSchemaName().getString(),
                                            index.getTableName().getString())),
                            dataTableRef.getTable().getTableName().getString(), false, PIndexState.INACTIVE);
            alterIndex(indexStatement);
            connection.setAutoCommit(true);
            MutationPlan mutationPlan = getMutationPlanForBuildingIndex(index, dataTableRef);
            Scan scan = mutationPlan.getContext().getScan();
            try {
                scan.setTimeRange(startTime, EndTime);
            } catch (IOException e) {
                throw new SQLException(e);
            }
            MutationState state = connection.getQueryServices().updateData(mutationPlan);
            indexStatement = FACTORY
                    .alterIndex(
                            FACTORY.namedTable(null,
                                    TableName.create(index.getSchemaName().getString(),
                                            index.getTableName().getString())),
                            dataTableRef.getTable().getTableName().getString(), false, PIndexState.ACTIVE);
            alterIndex(indexStatement);
            return state;
        } finally {
            connection.setAutoCommit(wasAutoCommit);
        }
    }

    private MutationPlan getMutationPlanForBuildingIndex(PTable index, TableRef dataTableRef) throws SQLException {
        MutationPlan mutationPlan;
        if (index.getIndexType() == IndexType.LOCAL) {
            PostLocalIndexDDLCompiler compiler =
                    new PostLocalIndexDDLCompiler(connection, getFullTableName(dataTableRef));
            mutationPlan = compiler.compile(index);
        } else {
            PostIndexDDLCompiler compiler = new PostIndexDDLCompiler(connection, dataTableRef);
            mutationPlan = compiler.compile(index);
        }
        return mutationPlan;
    }

    private MutationState buildIndex(PTable index, TableRef dataTableRef) throws SQLException {
        AlterIndexStatement indexStatement = null;
        boolean wasAutoCommit = connection.getAutoCommit();
        try {
            connection.setAutoCommit(true);
            MutationPlan mutationPlan = getMutationPlanForBuildingIndex(index, dataTableRef);
            Scan scan = mutationPlan.getContext().getScan();
            Long scn = connection.getSCN();
            try {
                if (ScanUtil.isDefaultTimeRange(scan.getTimeRange())) {
                    if (scn == null) {
                        scn = mutationPlan.getContext().getCurrentTime();
                    }
                    scan.setTimeRange(dataTableRef.getLowerBoundTimeStamp(), scn);
                }
            } catch (IOException e) {
                throw new SQLException(e);
            }

            // execute index population upsert select
            long startTime = System.currentTimeMillis();
            MutationState state = connection.getQueryServices().updateData(mutationPlan);
            long firstUpsertSelectTime = System.currentTimeMillis() - startTime;

            // for global indexes on non transactional tables we might have to
            // run a second index population upsert select to handle data rows
            // that were being written on the server while the index was created
            long sleepTime =
                    connection
                    .getQueryServices()
                    .getProps()
                    .getLong(QueryServices.INDEX_POPULATION_SLEEP_TIME,
                        QueryServicesOptions.DEFAULT_INDEX_POPULATION_SLEEP_TIME);
            if (!dataTableRef.getTable().isTransactional() && sleepTime > 0) {
                long delta = sleepTime - firstUpsertSelectTime;
                if (delta > 0) {
                    try {
                        Thread.sleep(delta);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new SQLExceptionInfo.Builder(SQLExceptionCode.INTERRUPTED_EXCEPTION)
                        .setRootCause(e).build().buildException();
                    }
                }
                // set the min timestamp of second index upsert select some time before the index
                // was created
                long minTimestamp = index.getTimeStamp() - firstUpsertSelectTime;
                try {
                    mutationPlan.getContext().getScan().setTimeRange(minTimestamp, scn);
                } catch (IOException e) {
                    throw new SQLException(e);
                }
                MutationState newMutationState =
                        connection.getQueryServices().updateData(mutationPlan);
                state.join(newMutationState);
            }

            indexStatement = FACTORY.alterIndex(FACTORY.namedTable(null,
            		    TableName.create(index.getSchemaName().getString(), index.getTableName().getString())),
            		    dataTableRef.getTable().getTableName().getString(), false, PIndexState.ACTIVE);
            alterIndex(indexStatement);

            return state;
        } finally {
            connection.setAutoCommit(wasAutoCommit);
        }
    }

    private String getFullTableName(TableRef dataTableRef) {
        String schemaName = dataTableRef.getTable().getSchemaName().getString();
        String tableName = dataTableRef.getTable().getTableName().getString();
        String fullName =
                schemaName == null ? ("\"" + tableName + "\"") : ("\"" + schemaName + "\""
                        + QueryConstants.NAME_SEPARATOR + "\"" + tableName + "\"");
        return fullName;
    }

    /**
     * Create an index table by morphing the CreateIndexStatement into a CreateTableStatement and calling
     * MetaDataClient.createTable. In doing so, we perform the following translations:
     * 1) Change the type of any columns being indexed to types that support null if the column is nullable.
     *    For example, a BIGINT type would be coerced to a DECIMAL type, since a DECIMAL type supports null
     *    when it's in the row key while a BIGINT does not.
     * 2) Append any row key column from the data table that is not in the indexed column list. Our indexes
     *    rely on having a 1:1 correspondence between the index and data rows.
     * 3) Change the name of the columns to include the column family. For example, if you have a column
     *    named "B" in a column family named "A", the indexed column name will be "A:B". This makes it easy
     *    to translate the column references in a query to the correct column references in an index table
     *    regardless of whether the column reference is prefixed with the column family name or not. It also
     *    has the side benefit of allowing the same named column in different column families to both be
     *    listed as an index column.
     * @param statement
     * @param splits
     * @return MutationState from population of index table from data table
     * @throws SQLException
     */
    public MutationState createIndex(CreateIndexStatement statement, byte[][] splits) throws SQLException {
        IndexKeyConstraint ik = statement.getIndexConstraint();
        TableName indexTableName = statement.getIndexTableName();

        Map<String,Object> tableProps = Maps.newHashMapWithExpectedSize(statement.getProps().size());
        Map<String,Object> commonFamilyProps = Maps.newHashMapWithExpectedSize(statement.getProps().size() + 1);
        populatePropertyMaps(statement.getProps(), tableProps, commonFamilyProps);
        List<Pair<ParseNode, SortOrder>> indexParseNodeAndSortOrderList = ik.getParseNodeAndSortOrderList();
        List<ColumnName> includedColumns = statement.getIncludeColumns();
        TableRef tableRef = null;
        PTable table = null;
        int numRetries = 0;
        boolean allocateIndexId = false;
        boolean isLocalIndex = statement.getIndexType() == IndexType.LOCAL;
        int hbaseVersion = connection.getQueryServices().getLowestClusterHBaseVersion();
        if (isLocalIndex) {
            if (!connection.getQueryServices().getProps().getBoolean(QueryServices.ALLOW_LOCAL_INDEX_ATTRIB, QueryServicesOptions.DEFAULT_ALLOW_LOCAL_INDEX)) {
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.UNALLOWED_LOCAL_INDEXES).setTableName(indexTableName.getTableName()).build().buildException();
            }
            if (!connection.getQueryServices().supportsFeature(Feature.LOCAL_INDEX)) {
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.NO_LOCAL_INDEXES).setTableName(indexTableName.getTableName()).build().buildException();
            }
        }
        while (true) {
            try {
                ColumnResolver resolver = FromCompiler.getResolver(statement, connection, statement.getUdfParseNodes());
                tableRef = resolver.getTables().get(0);
                Date asyncCreatedDate = null;
                if (statement.isAsync()) {
                    asyncCreatedDate = new Date(tableRef.getTimeStamp());
                }
                PTable dataTable = tableRef.getTable();
                boolean isTenantConnection = connection.getTenantId() != null;
                if (isTenantConnection) {
                    if (dataTable.getType() != PTableType.VIEW) {
                        throw new SQLFeatureNotSupportedException("An index may only be created for a VIEW through a tenant-specific connection");
                    }
                }
                if (!dataTable.isImmutableRows()) {
                    if (hbaseVersion < PhoenixDatabaseMetaData.MUTABLE_SI_VERSION_THRESHOLD) {
                        throw new SQLExceptionInfo.Builder(SQLExceptionCode.NO_MUTABLE_INDEXES).setTableName(indexTableName.getTableName()).build().buildException();
                    }
                    if (!connection.getQueryServices().hasIndexWALCodec() && !dataTable.isTransactional()) {
                        throw new SQLExceptionInfo.Builder(SQLExceptionCode.INVALID_MUTABLE_INDEX_CONFIG).setTableName(indexTableName.getTableName()).build().buildException();
                    }
                }
                int posOffset = 0;
                List<PColumn> pkColumns = dataTable.getPKColumns();
                Set<RowKeyColumnExpression> unusedPkColumns;
                if (dataTable.getBucketNum() != null) { // Ignore SALT column
                    unusedPkColumns = Sets.newLinkedHashSetWithExpectedSize(pkColumns.size()-1);
                    posOffset++;
                } else {
                    unusedPkColumns = Sets.newLinkedHashSetWithExpectedSize(pkColumns.size());
                }
                for (int i = posOffset; i < pkColumns.size(); i++) {
                    PColumn column = pkColumns.get(i);
                    unusedPkColumns.add(new RowKeyColumnExpression(column, new RowKeyValueAccessor(pkColumns, i), "\""+column.getName().getString()+"\""));
                }
                List<ColumnDefInPkConstraint> allPkColumns = Lists.newArrayListWithExpectedSize(unusedPkColumns.size());
                List<ColumnDef> columnDefs = Lists.newArrayListWithExpectedSize(includedColumns.size() + indexParseNodeAndSortOrderList.size());

                /*
                 * Allocate an index ID in two circumstances:
                 * 1) for a local index, as all local indexes will reside in the same HBase table
                 * 2) for a view on an index.
                 */
                if (isLocalIndex || (dataTable.getType() == PTableType.VIEW && dataTable.getViewType() != ViewType.MAPPED)) {
                    allocateIndexId = true;
                    PDataType dataType = MetaDataUtil.getViewIndexIdDataType();
                    ColumnName colName = ColumnName.caseSensitiveColumnName(MetaDataUtil.getViewIndexIdColumnName());
                    allPkColumns.add(new ColumnDefInPkConstraint(colName, SortOrder.getDefault(), false));
                    columnDefs.add(FACTORY.columnDef(colName, dataType.getSqlTypeName(), false, null, null, false, SortOrder.getDefault(), null, false));
                }

                if (dataTable.isMultiTenant()) {
                    PColumn col = dataTable.getPKColumns().get(posOffset);
                    RowKeyColumnExpression columnExpression = new RowKeyColumnExpression(col, new RowKeyValueAccessor(pkColumns, posOffset), col.getName().getString());
                    unusedPkColumns.remove(columnExpression);
                    PDataType dataType = IndexUtil.getIndexColumnDataType(col);
                    ColumnName colName = ColumnName.caseSensitiveColumnName(IndexUtil.getIndexColumnName(col));
                    allPkColumns.add(new ColumnDefInPkConstraint(colName, col.getSortOrder(), false));
                    columnDefs.add(FACTORY.columnDef(colName, dataType.getSqlTypeName(), col.isNullable(), col.getMaxLength(), col.getScale(), false, SortOrder.getDefault(), col.getName().getString(), col.isRowTimestamp()));
                }

                PhoenixStatement phoenixStatment = new PhoenixStatement(connection);
                StatementContext context = new StatementContext(phoenixStatment, resolver);
                IndexExpressionCompiler expressionIndexCompiler = new IndexExpressionCompiler(context);
                Set<ColumnName> indexedColumnNames = Sets.newHashSetWithExpectedSize(indexParseNodeAndSortOrderList.size());
                for (Pair<ParseNode, SortOrder> pair : indexParseNodeAndSortOrderList) {
                    ParseNode parseNode = pair.getFirst();
                    // normalize the parse node
                    parseNode = StatementNormalizer.normalize(parseNode, resolver);
                    // compile the parseNode to get an expression
                    expressionIndexCompiler.reset();
                    Expression expression = parseNode.accept(expressionIndexCompiler);
                    if (expressionIndexCompiler.isAggregate()) {
                        throw new SQLExceptionInfo.Builder(SQLExceptionCode.AGGREGATE_EXPRESSION_NOT_ALLOWED_IN_INDEX).build().buildException();
                    }
                    if (expression.getDeterminism() != Determinism.ALWAYS) {
                        throw new SQLExceptionInfo.Builder(SQLExceptionCode.NON_DETERMINISTIC_EXPRESSION_NOT_ALLOWED_IN_INDEX).build().buildException();
                    }
                    if (expression.isStateless()) {
                        throw new SQLExceptionInfo.Builder(SQLExceptionCode.STATELESS_EXPRESSION_NOT_ALLOWED_IN_INDEX).build().buildException();
                    }
                    unusedPkColumns.remove(expression);

                    // Go through parse node to get string as otherwise we
                    // can lose information during compilation
                    StringBuilder buf = new StringBuilder();
                    parseNode.toSQL(resolver, buf);
                    // need to escape backslash as this expression will be re-parsed later
                    String expressionStr = StringUtil.escapeBackslash(buf.toString());

                    ColumnName colName = null;
                    ColumnRef colRef = expressionIndexCompiler.getColumnRef();
                    boolean isRowTimestamp = false;
                    if (colRef!=null) {
                        // if this is a regular column
                        PColumn column = colRef.getColumn();
                        String columnFamilyName = column.getFamilyName()!=null ? column.getFamilyName().getString() : null;
                        colName = ColumnName.caseSensitiveColumnName(IndexUtil.getIndexColumnName(columnFamilyName, column.getName().getString()));
                        isRowTimestamp = column.isRowTimestamp();
                        if (colRef.getColumn().getExpressionStr() != null) {
                            expressionStr = colRef.getColumn().getExpressionStr();
                        }
                    }
                    else {
                        // if this is an expression
                        // TODO column names cannot have double quotes, remove this once this PHOENIX-1621 is fixed
                        String name = expressionStr.replaceAll("\"", "'");
                        colName = ColumnName.caseSensitiveColumnName(IndexUtil.getIndexColumnName(null, name));
                    }
                    indexedColumnNames.add(colName);
                    PDataType dataType = IndexUtil.getIndexColumnDataType(expression.isNullable(), expression.getDataType());
                    allPkColumns.add(new ColumnDefInPkConstraint(colName, pair.getSecond(), isRowTimestamp));
                    columnDefs.add(FACTORY.columnDef(colName, dataType.getSqlTypeName(), expression.isNullable(), expression.getMaxLength(), expression.getScale(), false, pair.getSecond(), expressionStr, isRowTimestamp));
                }

                // Next all the PK columns from the data table that aren't indexed
                if (!unusedPkColumns.isEmpty()) {
                    for (RowKeyColumnExpression colExpression : unusedPkColumns) {
                        PColumn col = dataTable.getPKColumns().get(colExpression.getPosition());
                        // Don't add columns with constant values from updatable views, as
                        // we don't need these in the index
                        if (col.getViewConstant() == null) {
                            ColumnName colName = ColumnName.caseSensitiveColumnName(IndexUtil.getIndexColumnName(col));
                            allPkColumns.add(new ColumnDefInPkConstraint(colName, colExpression.getSortOrder(), col.isRowTimestamp()));
                            PDataType dataType = IndexUtil.getIndexColumnDataType(colExpression.isNullable(), colExpression.getDataType());
                            columnDefs.add(FACTORY.columnDef(colName, dataType.getSqlTypeName(),
                                    colExpression.isNullable(), colExpression.getMaxLength(), colExpression.getScale(),
                                    false, colExpression.getSortOrder(), colExpression.toString(), col.isRowTimestamp()));
                        }
                    }
                }

                // Last all the included columns (minus any PK columns)
                for (ColumnName colName : includedColumns) {
                    PColumn col = resolver.resolveColumn(null, colName.getFamilyName(), colName.getColumnName()).getColumn();
                    colName = ColumnName.caseSensitiveColumnName(IndexUtil.getIndexColumnName(col));
                    // Check for duplicates between indexed and included columns
                    if (indexedColumnNames.contains(colName)) {
                        throw new SQLExceptionInfo.Builder(SQLExceptionCode.COLUMN_EXIST_IN_DEF).build().buildException();
                    }
                    if (!SchemaUtil.isPKColumn(col) && col.getViewConstant() == null) {
                        // Need to re-create ColumnName, since the above one won't have the column family name
                        colName = ColumnName.caseSensitiveColumnName(isLocalIndex?IndexUtil.getLocalIndexColumnFamily(col.getFamilyName().getString()):col.getFamilyName().getString(), IndexUtil.getIndexColumnName(col));
                        columnDefs.add(FACTORY.columnDef(colName, col.getDataType().getSqlTypeName(), col.isNullable(), col.getMaxLength(), col.getScale(), false, col.getSortOrder(), col.getExpressionStr(), col.isRowTimestamp()));
                    }
                }

                // Set DEFAULT_COLUMN_FAMILY_NAME of index to match data table
                // We need this in the props so that the correct column family is created
                if (dataTable.getDefaultFamilyName() != null && dataTable.getType() != PTableType.VIEW && !allocateIndexId) {
                    statement.getProps().put("", new Pair<String,Object>(DEFAULT_COLUMN_FAMILY_NAME,dataTable.getDefaultFamilyName().getString()));
                }
                PrimaryKeyConstraint pk = FACTORY.primaryKey(null, allPkColumns);
                tableProps.put(MetaDataUtil.DATA_TABLE_NAME_PROP_NAME, dataTable.getName().getString());
                CreateTableStatement tableStatement = FACTORY.createTable(indexTableName, statement.getProps(), columnDefs, pk, statement.getSplitNodes(), PTableType.INDEX, statement.ifNotExists(), null, null, statement.getBindCount(), null);
                table = createTableInternal(tableStatement, splits, dataTable, null, null, null, null, allocateIndexId, statement.getIndexType(), asyncCreatedDate, tableProps, commonFamilyProps);
                break;
            } catch (ConcurrentTableMutationException e) { // Can happen if parent data table changes while above is in progress
                if (numRetries<5) {
                    numRetries++;
                    continue;
                }
                throw e;
            }
        }
        if (table == null) {
            return new MutationState(0,connection);
        }

        if (logger.isInfoEnabled()) logger.info("Created index " + table.getName().getString() + " at " + table.getTimeStamp());
        boolean asyncIndexBuildEnabled = connection.getQueryServices().getProps().getBoolean(
                QueryServices.INDEX_ASYNC_BUILD_ENABLED,
                QueryServicesOptions.DEFAULT_INDEX_ASYNC_BUILD_ENABLED);
        // In async process, we return immediately as the MR job needs to be triggered .
        if(statement.isAsync() && asyncIndexBuildEnabled) {
            return new MutationState(0, connection);
        }

        // If our connection is at a fixed point-in-time, we need to open a new
        // connection so that our new index table is visible.
        if (connection.getSCN() != null) {
            return buildIndexAtTimeStamp(table, statement.getTable());
        }
        return buildIndex(table, tableRef);
    }

    public MutationState dropSequence(DropSequenceStatement statement) throws SQLException {
        Long scn = connection.getSCN();
        long timestamp = scn == null ? HConstants.LATEST_TIMESTAMP : scn;
        String schemaName = connection.getSchema() != null && statement.getSequenceName().getSchemaName() == null
                ? connection.getSchema() : statement.getSequenceName().getSchemaName();
        String sequenceName = statement.getSequenceName().getTableName();
        String tenantId = connection.getTenantId() == null ? null : connection.getTenantId().getString();
        try {
            connection.getQueryServices().dropSequence(tenantId, schemaName, sequenceName, timestamp);
        } catch (SequenceNotFoundException e) {
            if (statement.ifExists()) {
                return new MutationState(0, connection);
            }
            throw e;
        }
        return new MutationState(1, connection);
    }

    public MutationState createSequence(CreateSequenceStatement statement, long startWith,
            long incrementBy, long cacheSize, long minValue, long maxValue) throws SQLException {
        Long scn = connection.getSCN();
        long timestamp = scn == null ? HConstants.LATEST_TIMESTAMP : scn;
        String tenantId =
                connection.getTenantId() == null ? null : connection.getTenantId().getString();
        String schemaName=statement.getSequenceName().getSchemaName();
        if (SchemaUtil.isNamespaceMappingEnabled(null, connection.getQueryServices().getProps())) {
            if (schemaName == null || schemaName.equals(StringUtil.EMPTY_STRING)) {
                schemaName = connection.getSchema();
            }
            if (schemaName != null) {
                FromCompiler.getResolverForSchema(schemaName, connection);
            }
        }
        return createSequence(tenantId, schemaName, statement
                .getSequenceName().getTableName(), statement.ifNotExists(), startWith, incrementBy,
                cacheSize, statement.getCycle(), minValue, maxValue, timestamp);
    }

    private MutationState createSequence(String tenantId, String schemaName, String sequenceName,
            boolean ifNotExists, long startWith, long incrementBy, long cacheSize, boolean cycle,
            long minValue, long maxValue, long timestamp) throws SQLException {
        try {
            connection.getQueryServices().createSequence(tenantId, schemaName, sequenceName,
                    startWith, incrementBy, cacheSize, minValue, maxValue, cycle, timestamp);
        } catch (SequenceAlreadyExistsException e) {
            if (ifNotExists) {
                return new MutationState(0, connection);
            }
            throw e;
        }
        return new MutationState(1, connection);
    }

    public MutationState createFunction(CreateFunctionStatement stmt) throws SQLException {
        boolean wasAutoCommit = connection.getAutoCommit();
        connection.rollback();
        try {
            PFunction function = new PFunction(stmt.getFunctionInfo(), stmt.isTemporary(), stmt.isReplace());
            connection.setAutoCommit(false);
            String tenantIdStr = connection.getTenantId() == null ? null : connection.getTenantId().getString();
            List<Mutation> functionData = Lists.newArrayListWithExpectedSize(function.getFunctionArguments().size() + 1);

            List<FunctionArgument> args = function.getFunctionArguments();
            try (PreparedStatement argUpsert = connection.prepareStatement(INSERT_FUNCTION_ARGUMENT)) {
                for (int i = 0; i < args.size(); i++) {
                    FunctionArgument arg = args.get(i);
                    addFunctionArgMutation(function.getFunctionName(), arg, argUpsert, i);
                }
                functionData.addAll(connection.getMutationState().toMutations().next().getSecond());
                connection.rollback();
            }

            try (PreparedStatement functionUpsert = connection.prepareStatement(CREATE_FUNCTION)) {
                functionUpsert.setString(1, tenantIdStr);
                functionUpsert.setString(2, function.getFunctionName());
                functionUpsert.setInt(3, function.getFunctionArguments().size());
                functionUpsert.setString(4, function.getClassName());
                functionUpsert.setString(5, function.getJarPath());
                functionUpsert.setString(6, function.getReturnType());
                functionUpsert.execute();
                functionData.addAll(connection.getMutationState().toMutations(null).next().getSecond());
                connection.rollback();
            }
            MetaDataMutationResult result = connection.getQueryServices().createFunction(functionData, function, stmt.isTemporary());
            MutationCode code = result.getMutationCode();
            switch(code) {
            case FUNCTION_ALREADY_EXISTS:
                if (!function.isReplace()) {
                    throw new FunctionAlreadyExistsException(function.getFunctionName(), result
                            .getFunctions().get(0));
                } else {
                    connection.removeFunction(function.getTenantId(), function.getFunctionName(),
                            result.getMutationTime());
                    addFunctionToCache(result);
                }
            case NEWER_FUNCTION_FOUND:
                // Add function to ConnectionQueryServices so it's cached, but don't add
                // it to this connection as we can't see it.
                throw new NewerFunctionAlreadyExistsException(function.getFunctionName(), result.getFunctions().get(0));
            default:
                List<PFunction> functions = new ArrayList<PFunction>(1);
                functions.add(function);
                result = new MetaDataMutationResult(code, result.getMutationTime(), functions, true);
                if(function.isReplace()) {
                    connection.removeFunction(function.getTenantId(), function.getFunctionName(),
                            result.getMutationTime());
                }
                addFunctionToCache(result);
            }
        } finally {
            connection.setAutoCommit(wasAutoCommit);
        }
        return new MutationState(1, connection);
    }

    private static ColumnDef findColumnDefOrNull(List<ColumnDef> colDefs, ColumnName colName) {
        for (ColumnDef colDef : colDefs) {
            if (colDef.getColumnDefName().getColumnName().equals(colName.getColumnName())) {
                return colDef;
            }
        }
        return null;
    }

    private static boolean checkAndValidateRowTimestampCol(ColumnDef colDef, PrimaryKeyConstraint pkConstraint,
            boolean rowTimeStampColAlreadyFound, PTableType tableType) throws SQLException {

        ColumnName columnDefName = colDef.getColumnDefName();
        if (tableType == VIEW && (pkConstraint.getNumColumnsWithRowTimestamp() > 0 || colDef.isRowTimestamp())) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.ROWTIMESTAMP_NOT_ALLOWED_ON_VIEW)
            .setColumnName(columnDefName.getColumnName()).build().buildException();
        }
        /*
         * For indexes we have already validated that the data table has the right kind and number of row_timestamp
         * columns. So we don't need to perform any extra validations for them.
         */
        if (tableType == TABLE) {
            boolean isColumnDeclaredRowTimestamp = colDef.isRowTimestamp() || pkConstraint.isColumnRowTimestamp(columnDefName);
            if (isColumnDeclaredRowTimestamp) {
                boolean isColumnPartOfPk = colDef.isPK() || pkConstraint.contains(columnDefName);
                // A column can be declared as ROW_TIMESTAMP only if it is part of the primary key
                if (isColumnDeclaredRowTimestamp && !isColumnPartOfPk) {
                    throw new SQLExceptionInfo.Builder(SQLExceptionCode.ROWTIMESTAMP_PK_COL_ONLY)
                    .setColumnName(columnDefName.getColumnName()).build().buildException();
                }

                // A column can be declared as ROW_TIMESTAMP only if it can be represented as a long
                PDataType dataType = colDef.getDataType();
                if (isColumnDeclaredRowTimestamp && (dataType != PLong.INSTANCE && dataType != PUnsignedLong.INSTANCE && !dataType.isCoercibleTo(PTimestamp.INSTANCE))) {
                    throw new SQLExceptionInfo.Builder(SQLExceptionCode.ROWTIMESTAMP_COL_INVALID_TYPE)
                    .setColumnName(columnDefName.getColumnName()).build().buildException();
                }

                // Only one column can be declared as a ROW_TIMESTAMP column
                if (rowTimeStampColAlreadyFound && isColumnDeclaredRowTimestamp) {
                    throw new SQLExceptionInfo.Builder(SQLExceptionCode.ROWTIMESTAMP_ONE_PK_COL_ONLY)
                    .setColumnName(columnDefName.getColumnName()).build().buildException();
                }
                return true;
            }
        }
        return false;
    }

    private PTable createTableInternal(CreateTableStatement statement, byte[][] splits,
            final PTable parent, String viewStatement, ViewType viewType,
            final byte[][] viewColumnConstants, final BitSet isViewColumnReferenced, boolean allocateIndexId,
            IndexType indexType, Date asyncCreatedDate,
            Map<String,Object> tableProps,
            Map<String,Object> commonFamilyProps) throws SQLException {
        final PTableType tableType = statement.getTableType();
        boolean wasAutoCommit = connection.getAutoCommit();
        connection.rollback();
        try {
            connection.setAutoCommit(false);
            List<Mutation> tableMetaData = Lists.newArrayListWithExpectedSize(statement.getColumnDefs().size() + 3);

            TableName tableNameNode = statement.getTableName();
            final String schemaName = connection.getSchema() != null && tableNameNode.getSchemaName() == null ? connection.getSchema() : tableNameNode.getSchemaName();
            final String tableName = tableNameNode.getTableName();
            String parentTableName = null;
            PName tenantId = connection.getTenantId();
            String tenantIdStr = tenantId == null ? null : tenantId.getString();
            Long scn = connection.getSCN();
            long clientTimeStamp = scn == null ? HConstants.LATEST_TIMESTAMP : scn;
            boolean multiTenant = false;
            boolean storeNulls = false;
            boolean transactional = (parent!= null) ? parent.isTransactional() : false;
            Integer saltBucketNum = null;
            String defaultFamilyName = null;
            boolean isImmutableRows = false;
            boolean isAppendOnlySchema = false;
            List<PName> physicalNames = Collections.emptyList();
            boolean addSaltColumn = false;
            boolean rowKeyOrderOptimizable = true;
            Long timestamp = null;
            boolean isNamespaceMapped = parent == null
                    ? SchemaUtil.isNamespaceMappingEnabled(tableType, connection.getQueryServices().getProps())
                    : parent.isNamespaceMapped();
            boolean isLocalIndex = indexType == IndexType.LOCAL;
            QualifierEncodingScheme encodingScheme = NON_ENCODED_QUALIFIERS;
            ImmutableStorageScheme immutableStorageScheme = ONE_CELL_PER_COLUMN;
            if (parent != null && tableType == PTableType.INDEX) {
                timestamp = TransactionUtil.getTableTimestamp(connection, transactional);
                storeNulls = parent.getStoreNulls();
                isImmutableRows = parent.isImmutableRows();
                isAppendOnlySchema = parent.isAppendOnlySchema();

                // Index on view
                // TODO: Can we support a multi-tenant index directly on a multi-tenant
                // table instead of only a view? We don't have anywhere to put the link
                // from the table to the index, though.
                if (isLocalIndex || (parent.getType() == PTableType.VIEW && parent.getViewType() != ViewType.MAPPED)) {
                    PName physicalName = parent.getPhysicalName();

                    saltBucketNum = parent.getBucketNum();
                    addSaltColumn = (saltBucketNum != null && !isLocalIndex);
                    defaultFamilyName = parent.getDefaultFamilyName() == null ? null : parent.getDefaultFamilyName().getString();
                    if (isLocalIndex) {
                        defaultFamilyName =
                                parent.getDefaultFamilyName() == null ? QueryConstants.DEFAULT_LOCAL_INDEX_COLUMN_FAMILY
                                        : IndexUtil.getLocalIndexColumnFamily(parent.getDefaultFamilyName().getString());
                        saltBucketNum = null;
                        // Set physical name of local index table
                        physicalNames = Collections.singletonList(PNameFactory.newName(physicalName.getBytes()));
                    } else {
                        defaultFamilyName = parent.getDefaultFamilyName() == null ? QueryConstants.DEFAULT_COLUMN_FAMILY : parent.getDefaultFamilyName().getString();
                        // Set physical name of view index table
                        physicalNames = Collections.singletonList(PNameFactory.newName(MetaDataUtil.getViewIndexPhysicalName(physicalName.getBytes())));
                    }
                }

                multiTenant = parent.isMultiTenant();
                storeNulls = parent.getStoreNulls();
                parentTableName = parent.getTableName().getString();
                // Pass through data table sequence number so we can check it hasn't changed
                PreparedStatement incrementStatement = connection.prepareStatement(INCREMENT_SEQ_NUM);
                incrementStatement.setString(1, tenantIdStr);
                incrementStatement.setString(2, schemaName);
                incrementStatement.setString(3, parentTableName);
                incrementStatement.setLong(4, parent.getSequenceNumber());
                incrementStatement.execute();
                // Get list of mutations and add to table meta data that will be passed to server
                // to guarantee order. This row will always end up last
                tableMetaData.addAll(connection.getMutationState().toMutations(timestamp).next().getSecond());
                connection.rollback();

                // Add row linking from data table row to index table row
                PreparedStatement linkStatement = connection.prepareStatement(CREATE_LINK);
                linkStatement.setString(1, tenantIdStr);
                linkStatement.setString(2, schemaName);
                linkStatement.setString(3, parentTableName);
                linkStatement.setString(4, tableName);
                linkStatement.setByte(5, LinkType.INDEX_TABLE.getSerializedValue());
                linkStatement.setLong(6, parent.getSequenceNumber());
                linkStatement.setString(7, PTableType.INDEX.getSerializedValue());
                linkStatement.execute();
            }

            PrimaryKeyConstraint pkConstraint = statement.getPrimaryKeyConstraint();
            String pkName = null;
            List<Pair<ColumnName,SortOrder>> pkColumnsNames = Collections.<Pair<ColumnName,SortOrder>>emptyList();
            Iterator<Pair<ColumnName,SortOrder>> pkColumnsIterator = Iterators.emptyIterator();
            if (pkConstraint != null) {
                pkColumnsNames = pkConstraint.getColumnNames();
                pkColumnsIterator = pkColumnsNames.iterator();
                pkName = pkConstraint.getName();
            }

            // Although unusual, it's possible to set a mapped VIEW as having immutable rows.
            // This tells Phoenix that you're managing the index maintenance yourself.
            if (tableType != PTableType.INDEX && (tableType != PTableType.VIEW || viewType == ViewType.MAPPED)) {
            	// TODO remove TableProperty.IMMUTABLE_ROWS at the next major release
            	Boolean isImmutableRowsProp = statement.immutableRows()!=null? statement.immutableRows() :
            		(Boolean) TableProperty.IMMUTABLE_ROWS.getValue(tableProps);
                if (isImmutableRowsProp == null) {
                    isImmutableRows = connection.getQueryServices().getProps().getBoolean(QueryServices.IMMUTABLE_ROWS_ATTRIB, QueryServicesOptions.DEFAULT_IMMUTABLE_ROWS);
                } else {
                    isImmutableRows = isImmutableRowsProp;
                }
            }
            if (tableType == PTableType.TABLE) {
                Boolean isAppendOnlySchemaProp = (Boolean) TableProperty.APPEND_ONLY_SCHEMA.getValue(tableProps);
                isAppendOnlySchema = isAppendOnlySchemaProp!=null ? isAppendOnlySchemaProp : false;
            }

            // Can't set any of these on views or shared indexes on views
            if (tableType != PTableType.VIEW && !allocateIndexId) {
                saltBucketNum = (Integer) TableProperty.SALT_BUCKETS.getValue(tableProps);
                if (saltBucketNum != null) {
                    if (saltBucketNum < 0 || saltBucketNum > SaltingUtil.MAX_BUCKET_NUM) {
                        throw new SQLExceptionInfo.Builder(SQLExceptionCode.INVALID_BUCKET_NUM).build().buildException();
                    }
                }
                // Salt the index table if the data table is salted
                if (saltBucketNum == null) {
                    if (parent != null) {
                        saltBucketNum = parent.getBucketNum();
                    }
                } else if (saltBucketNum.intValue() == 0) {
                    saltBucketNum = null; // Provides a way for an index to not be salted if its data table is salted
                }
                addSaltColumn = (saltBucketNum != null);
            }

            // Can't set MULTI_TENANT or DEFAULT_COLUMN_FAMILY_NAME on an INDEX or a non mapped VIEW
            if (tableType != PTableType.INDEX && (tableType != PTableType.VIEW || viewType == ViewType.MAPPED)) {
                Boolean multiTenantProp = (Boolean) tableProps.get(PhoenixDatabaseMetaData.MULTI_TENANT);
                multiTenant = Boolean.TRUE.equals(multiTenantProp);
                defaultFamilyName = (String)TableProperty.DEFAULT_COLUMN_FAMILY.getValue(tableProps);
            }

            boolean disableWAL = false;
            Boolean disableWALProp = (Boolean) TableProperty.DISABLE_WAL.getValue(tableProps);
            if (disableWALProp != null) {
                disableWAL = disableWALProp;
            }
            long updateCacheFrequency = 0;
            Long updateCacheFrequencyProp = (Long) TableProperty.UPDATE_CACHE_FREQUENCY.getValue(tableProps);
            if (updateCacheFrequencyProp != null) {
                updateCacheFrequency = updateCacheFrequencyProp;
            }
            String autoPartitionSeq = (String) TableProperty.AUTO_PARTITION_SEQ.getValue(tableProps);
            Long guidePostsWidth = (Long) TableProperty.GUIDE_POSTS_WIDTH.getValue(tableProps);
            Boolean storeNullsProp = (Boolean) TableProperty.STORE_NULLS.getValue(tableProps);
            if (storeNullsProp == null) {
                if (parent == null) {
                    storeNulls = connection.getQueryServices().getProps().getBoolean(
                                    QueryServices.DEFAULT_STORE_NULLS_ATTRIB,
                                    QueryServicesOptions.DEFAULT_STORE_NULLS);
                    tableProps.put(PhoenixDatabaseMetaData.STORE_NULLS, Boolean.valueOf(storeNulls));
                }
            } else {
                storeNulls = storeNullsProp;
            }
            Boolean transactionalProp = (Boolean) TableProperty.TRANSACTIONAL.getValue(tableProps);
            if (transactionalProp != null && parent != null) {
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.ONLY_TABLE_MAY_BE_DECLARED_TRANSACTIONAL)
                .setSchemaName(schemaName).setTableName(tableName)
                .build().buildException();
            }
            if (parent == null) {
                if (transactionalProp == null) {
                    transactional = connection.getQueryServices().getProps().getBoolean(
                                    QueryServices.DEFAULT_TABLE_ISTRANSACTIONAL_ATTRIB,
                                    QueryServicesOptions.DEFAULT_TABLE_ISTRANSACTIONAL);
                } else {
                    transactional = transactionalProp;
                }
            }
            boolean transactionsEnabled = connection.getQueryServices().getProps().getBoolean(
                                            QueryServices.TRANSACTIONS_ENABLED,
                                            QueryServicesOptions.DEFAULT_TRANSACTIONS_ENABLED);
            // can't create a transactional table if transactions are not enabled
            if (!transactionsEnabled && transactional) {
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.CANNOT_CREATE_TXN_TABLE_IF_TXNS_DISABLED)
                .setSchemaName(schemaName).setTableName(tableName)
                .build().buildException();
            }
            // can't create a transactional table if it has a row timestamp column
            if (pkConstraint.getNumColumnsWithRowTimestamp() > 0 && transactional) {
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.CANNOT_CREATE_TXN_TABLE_WITH_ROW_TIMESTAMP)
                .setSchemaName(schemaName).setTableName(tableName)
                .build().buildException();
            }

            // Put potentially inferred value into tableProps as it's used by the createTable call below
            // to determine which coprocessors to install on the new table.
            tableProps.put(PhoenixDatabaseMetaData.TRANSACTIONAL, transactional);
            if (transactional) {
                // If TTL set, use Tephra TTL property name instead
                Object ttl = commonFamilyProps.remove(HColumnDescriptor.TTL);
                if (ttl != null) {
                    commonFamilyProps.put(TxConstants.PROPERTY_TTL, ttl);
                }
            }

            boolean sharedTable = statement.getTableType() == PTableType.VIEW || allocateIndexId;
            if (transactional) {
                // Tephra uses an empty value cell as its delete marker, so we need to turn on
                // storeNulls for transactional tables.
                // If we use regular column delete markers (which is what non transactional tables
                // use), then they get converted
                // on the server, but this can mess up our secondary index code as the changes get
                // committed prior to the
                // maintenance code being able to see the prior state to update the rows correctly.
                if (Boolean.FALSE.equals(storeNullsProp)) {
                    throw new SQLExceptionInfo.Builder(SQLExceptionCode.STORE_NULLS_MUST_BE_TRUE_FOR_TRANSACTIONAL)
                    .setSchemaName(schemaName).setTableName(tableName)
                    .build().buildException();
                }
                // Force STORE_NULLS to true when transactional as Tephra cannot deal with column deletes
                storeNulls = true;
                tableProps.put(PhoenixDatabaseMetaData.STORE_NULLS, Boolean.TRUE);

                if (!sharedTable) {
                    Integer maxVersionsProp = (Integer) commonFamilyProps.get(HConstants.VERSIONS);
                    if (maxVersionsProp == null) {
                        if (parent != null) {
                            HTableDescriptor desc = connection.getQueryServices().getTableDescriptor(parent.getPhysicalName().getBytes());
                            if (desc != null) {
                                maxVersionsProp = desc.getFamily(SchemaUtil.getEmptyColumnFamily(parent)).getMaxVersions();
                            }
                        }
                        if (maxVersionsProp == null) {
                            maxVersionsProp = connection.getQueryServices().getProps().getInt(
                                    QueryServices.MAX_VERSIONS_TRANSACTIONAL_ATTRIB,
                                    QueryServicesOptions.DEFAULT_MAX_VERSIONS_TRANSACTIONAL);
                        }
                        commonFamilyProps.put(HConstants.VERSIONS, maxVersionsProp);
                    }
                }
            }
            timestamp = timestamp==null ? TransactionUtil.getTableTimestamp(connection, transactional) : timestamp;

            // Delay this check as it is supported to have IMMUTABLE_ROWS and SALT_BUCKETS defined on views
            if (sharedTable) {
                if (tableProps.get(PhoenixDatabaseMetaData.DEFAULT_COLUMN_FAMILY_NAME) != null) {
                    throw new SQLExceptionInfo.Builder(SQLExceptionCode.DEFAULT_COLUMN_FAMILY_ON_SHARED_TABLE)
                    .setSchemaName(schemaName).setTableName(tableName)
                    .build().buildException();
                }
                if (SchemaUtil.hasHTableDescriptorProps(tableProps)) {
                    throw new SQLExceptionInfo.Builder(SQLExceptionCode.VIEW_WITH_PROPERTIES).build()
                            .buildException();
                }
            }

            List<ColumnDef> colDefs = statement.getColumnDefs();
            LinkedHashMap<PColumn,PColumn> columns;
            LinkedHashSet<PColumn> pkColumns;

            if (tenantId != null && !sharedTable) {
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.CANNOT_CREATE_TENANT_SPECIFIC_TABLE)
                    .setSchemaName(schemaName).setTableName(tableName).build().buildException();
            }
            if (autoPartitionSeq!=null) {
                int autoPartitionColIndex = multiTenant ? 1 : 0;
                PDataType dataType = colDefs.get(autoPartitionColIndex).getDataType();
                if (!PLong.INSTANCE.isCastableTo(dataType)) {
                    throw new SQLExceptionInfo.Builder(SQLExceptionCode.SEQUENCE_NOT_CASTABLE_TO_AUTO_PARTITION_ID_COLUMN)
                    .setSchemaName(schemaName).setTableName(tableName).build().buildException();
                }
            }

            if (tableType == PTableType.VIEW) {
                physicalNames = Collections.singletonList(PNameFactory.newName(parent.getPhysicalName().getString()));
                if (viewType == ViewType.MAPPED) {
                    columns = Maps.newLinkedHashMap();
                    pkColumns = newLinkedHashSetWithExpectedSize(colDefs.size());
                } else {
                    // Propagate property values to VIEW.
                    // TODO: formalize the known set of these properties
                    // Manually transfer the ROW_KEY_ORDER_OPTIMIZABLE_BYTES from parent as we don't
                    // want to add this hacky flag to the schema (see PHOENIX-2067).
                    rowKeyOrderOptimizable = parent.rowKeyOrderOptimizable();
                    if (rowKeyOrderOptimizable) {
                        UpgradeUtil.addRowKeyOrderOptimizableCell(tableMetaData, SchemaUtil.getTableKey(tenantIdStr, schemaName, tableName), clientTimeStamp);
                    }
                    multiTenant = parent.isMultiTenant();
                    saltBucketNum = parent.getBucketNum();
                    isAppendOnlySchema = parent.isAppendOnlySchema();
                    isImmutableRows = parent.isImmutableRows();
                    if (updateCacheFrequencyProp == null) {
                        // set to the parent value if the property is not set on the view
                        updateCacheFrequency = parent.getUpdateCacheFrequency();
                    }
                    disableWAL = (disableWALProp == null ? parent.isWALDisabled() : disableWALProp);
                    defaultFamilyName = parent.getDefaultFamilyName() == null ? null : parent.getDefaultFamilyName().getString();
                    List<PColumn> allColumns = parent.getColumns();
                    if (saltBucketNum != null) { // Don't include salt column in columns, as it should not have it when created
                        allColumns = allColumns.subList(1, allColumns.size());
                    }
                    columns = new LinkedHashMap<PColumn,PColumn>(allColumns.size() + colDefs.size());
                    for (PColumn column : allColumns) {
                        columns.put(column, column);
                    }
                    pkColumns = newLinkedHashSet(parent.getPKColumns());

                    // Add row linking from view to its parent table
                    // FIXME: not currently used, but see PHOENIX-1367
                    // as fixing that will require it's usage.
                    PreparedStatement linkStatement = connection.prepareStatement(CREATE_VIEW_LINK);
                    linkStatement.setString(1, tenantIdStr);
                    linkStatement.setString(2, schemaName);
                    linkStatement.setString(3, tableName);
                    linkStatement.setString(4, parent.getName().getString());
                    linkStatement.setByte(5, LinkType.PARENT_TABLE.getSerializedValue());
                    linkStatement.setString(6, parent.getTenantId() == null ? null : parent.getTenantId().getString());
                    linkStatement.execute();
                }
            } else {
                columns = new LinkedHashMap<PColumn,PColumn>(colDefs.size());
                pkColumns = newLinkedHashSetWithExpectedSize(colDefs.size() + 1); // in case salted
            }
            
            // Don't add link for mapped view, as it just points back to itself and causes the drop to
            // fail because it looks like there's always a view associated with it.
            if (!physicalNames.isEmpty()) {
                // Upsert physical name for mapped view only if the full physical table name is different than the full table name
                // Otherwise, we end up with a self-referencing link and then cannot ever drop the view.
                if (viewType != ViewType.MAPPED
                        || !physicalNames.get(0).getString().equals(SchemaUtil.getTableName(schemaName, tableName))) {
                    // Add row linking from data table row to physical table row
                    PreparedStatement linkStatement = connection.prepareStatement(CREATE_LINK);
                    for (PName physicalName : physicalNames) {
                        linkStatement.setString(1, tenantIdStr);
                        linkStatement.setString(2, schemaName);
                        linkStatement.setString(3, tableName);
                        linkStatement.setString(4, physicalName.getString());
                        linkStatement.setByte(5, LinkType.PHYSICAL_TABLE.getSerializedValue());
                        if (tableType == PTableType.VIEW) {
                            PTable physicalTable = connection.getTable(new PTableKey(null, physicalName.getString()
                                    .replace(QueryConstants.NAMESPACE_SEPARATOR, QueryConstants.NAME_SEPARATOR)));
                            linkStatement.setLong(6, physicalTable.getSequenceNumber());
                            linkStatement.setString(7, null);
                        } else {
                            linkStatement.setLong(6, parent.getSequenceNumber());
                            linkStatement.setString(7, PTableType.INDEX.getSerializedValue());
                        }
                        linkStatement.execute();
                    }
                    tableMetaData.addAll(connection.getMutationState().toMutations(timestamp).next().getSecond());
                    connection.rollback();
                }
            }

            Map<String, PName> familyNames = Maps.newLinkedHashMap();
            boolean isPK = false;
            boolean rowTimeStampColumnAlreadyFound = false;
            int positionOffset = columns.size();
            if (saltBucketNum != null) {
                positionOffset++;
                if (addSaltColumn) {
                    pkColumns.add(SaltingUtil.SALTING_COLUMN);
                }
            }
            int pkPositionOffset = pkColumns.size();
            int position = positionOffset;
            EncodedCQCounter cqCounter = NULL_COUNTER;
            PTable viewPhysicalTable = null;
            if (tableType == PTableType.VIEW) {
                /*
                 * We can't control what column qualifiers are used in HTable mapped to Phoenix views. So we are not
                 * able to encode column names.
                 */  
                if (viewType != MAPPED) {
                    /*
                     * For regular phoenix views, use the storage scheme of the physical table since they all share the
                     * the same HTable. Views always use the base table's column qualifier counter for doling out
                     * encoded column qualifier.
                     */
                    viewPhysicalTable = PhoenixRuntime.getTable(connection, physicalNames.get(0).getString());
                    immutableStorageScheme = viewPhysicalTable.getImmutableStorageScheme();
                    encodingScheme = viewPhysicalTable.getEncodingScheme();
					if (EncodedColumnsUtil.usesEncodedColumnNames(viewPhysicalTable)) {
                        cqCounter  = viewPhysicalTable.getEncodedCQCounter();
                    }
                }
            }
            // System tables have hard-coded column qualifiers. So we can't use column encoding for them.
            else if (!SchemaUtil.isSystemTable(Bytes.toBytes(SchemaUtil.getTableName(schemaName, tableName)))) {
                /*
                 * Indexes inherit the storage scheme of the parent data tables. Otherwise, we always attempt to 
                 * create tables with encoded column names. 
                 * 
                 * Also of note is the case with shared indexes i.e. local indexes and view indexes. In these cases, 
                 * column qualifiers for covered columns don't have to be unique because rows of the logical indexes are 
                 * partitioned by the virtue of indexId present in the row key. As such, different shared indexes can use
                 * potentially overlapping column qualifiers.
                 * 
                 * If the hbase table already exists, then possibly encoded or non-encoded column qualifiers were used. 
                 * In this case we pursue ahead with non-encoded column qualifier scheme. If the phoenix metadata for this table already exists 
                 * then we rely on the PTable, with appropriate storage scheme, returned in the MetadataMutationResult to be updated 
                 * in the client cache. If the phoenix table metadata already doesn't exist then the non-encoded column qualifier scheme works
                 * because we cannot control the column qualifiers that were used when populating the hbase table.
                 */
                
                byte[] tableNameBytes = SchemaUtil.getTableNameAsBytes(schemaName, tableName);
                boolean tableExists = true;
                try {
                    HTableDescriptor tableDescriptor = connection.getQueryServices().getTableDescriptor(tableNameBytes);
                    if (tableDescriptor == null) { // for connectionless
                        tableExists = false;
                    }
                } catch (org.apache.phoenix.schema.TableNotFoundException e) {
                    tableExists = false;
                }
                if (tableExists) {
                    encodingScheme = NON_ENCODED_QUALIFIERS;
                    immutableStorageScheme = ONE_CELL_PER_COLUMN;
                } else if (parent != null) {
                    encodingScheme = parent.getEncodingScheme();
                    immutableStorageScheme = parent.getImmutableStorageScheme();
                } else {
                	Byte encodingSchemeSerializedByte = (Byte) TableProperty.COLUMN_ENCODED_BYTES.getValue(tableProps);
                    if (encodingSchemeSerializedByte == null) {
                    	encodingSchemeSerializedByte = (byte)connection.getQueryServices().getProps().getInt(QueryServices.DEFAULT_COLUMN_ENCODED_BYTES_ATRRIB, QueryServicesOptions.DEFAULT_COLUMN_ENCODED_BYTES);
                    } 
                    encodingScheme =  QualifierEncodingScheme.fromSerializedValue(encodingSchemeSerializedByte);
                    if (isImmutableRows) {
                        immutableStorageScheme =
                                (ImmutableStorageScheme) TableProperty.IMMUTABLE_STORAGE_SCHEME
                                .getValue(tableProps);
                        if (immutableStorageScheme == null) {
                            if (multiTenant) {
                                immutableStorageScheme =
                                        ImmutableStorageScheme
                                        .valueOf(connection
                                                .getQueryServices()
                                                .getProps()
                                                .get(
                                                        QueryServices.DEFAULT_MULTITENANT_IMMUTABLE_STORAGE_SCHEME_ATTRIB,
                                                        QueryServicesOptions.DEFAULT_MULTITENANT_IMMUTABLE_STORAGE_SCHEME));
                            } else {
                                immutableStorageScheme =
                                        ImmutableStorageScheme
                                        .valueOf(connection
                                                .getQueryServices()
                                                .getProps()
                                                .get(
                                                        QueryServices.DEFAULT_IMMUTABLE_STORAGE_SCHEME_ATTRIB,
                                                        QueryServicesOptions.DEFAULT_IMMUTABLE_STORAGE_SCHEME));
                            }
                        }
                        if (immutableStorageScheme != ONE_CELL_PER_COLUMN
                                && encodingScheme == NON_ENCODED_QUALIFIERS) {
                            throw new SQLExceptionInfo.Builder(
                                    SQLExceptionCode.INVALID_IMMUTABLE_STORAGE_SCHEME_AND_COLUMN_QUALIFIER_BYTES)
                            .setSchemaName(schemaName).setTableName(tableName).build()
                            .buildException();
                        }
                    } 
                }
                cqCounter = encodingScheme != NON_ENCODED_QUALIFIERS ? new EncodedCQCounter() : NULL_COUNTER;
            }

            Map<String, Integer> changedCqCounters = new HashMap<>(colDefs.size());
            for (ColumnDef colDef : colDefs) {
                rowTimeStampColumnAlreadyFound = checkAndValidateRowTimestampCol(colDef, pkConstraint, rowTimeStampColumnAlreadyFound, tableType);
                if (colDef.isPK()) { // i.e. the column is declared as CREATE TABLE COLNAME DATATYPE PRIMARY KEY...
                    if (isPK) {
                        throw new SQLExceptionInfo.Builder(SQLExceptionCode.PRIMARY_KEY_ALREADY_EXISTS)
                            .setColumnName(colDef.getColumnDefName().getColumnName()).build().buildException();
                    }
                    isPK = true;
                } else {
                    // do not allow setting NOT-NULL constraint on non-primary columns.
                    if (  Boolean.FALSE.equals(colDef.isNull()) &&
                        ( isPK || ( pkConstraint != null && !pkConstraint.contains(colDef.getColumnDefName())))) {
                            throw new SQLExceptionInfo.Builder(SQLExceptionCode.INVALID_NOT_NULL_CONSTRAINT)
                                .setSchemaName(schemaName)
                                .setTableName(tableName)
                                .setColumnName(colDef.getColumnDefName().getColumnName()).build().buildException();
                    }
                }
                ColumnName columnDefName = colDef.getColumnDefName();
                String colDefFamily = columnDefName.getFamilyName();
                boolean isPkColumn = isPkColumn(pkConstraint, colDef, columnDefName);
                String cqCounterFamily = null;
                if (!isPkColumn) {
                    if (immutableStorageScheme == SINGLE_CELL_ARRAY_WITH_OFFSETS && encodingScheme != NON_ENCODED_QUALIFIERS) {
                        // For this scheme we track column qualifier counters at the column family level.
                        cqCounterFamily = colDefFamily != null ? colDefFamily : (defaultFamilyName != null ? defaultFamilyName : DEFAULT_COLUMN_FAMILY);
                    } else {
                        // For other schemes, column qualifier counters are tracked using the default column family.
                        cqCounterFamily = defaultFamilyName != null ? defaultFamilyName : DEFAULT_COLUMN_FAMILY;
                    }
                }
                Integer encodedCQ =  isPkColumn ? null : cqCounter.getNextQualifier(cqCounterFamily);
                byte[] columnQualifierBytes = null;
                try {
                    columnQualifierBytes = EncodedColumnsUtil.getColumnQualifierBytes(columnDefName.getColumnName(), encodedCQ, encodingScheme, isPkColumn);
                }
                catch (QualifierOutOfRangeException e) {
                    throw new SQLExceptionInfo.Builder(SQLExceptionCode.MAX_COLUMNS_EXCEEDED)
                    .setSchemaName(schemaName)
                    .setTableName(tableName).build().buildException();
                }
                PColumn column = newColumn(position++, colDef, pkConstraint, defaultFamilyName, false, columnQualifierBytes);
                if (cqCounter.increment(cqCounterFamily)) {
                    changedCqCounters.put(cqCounterFamily, cqCounter.getNextQualifier(cqCounterFamily));
                }
                if (SchemaUtil.isPKColumn(column)) {
                    // TODO: remove this constraint?
                    if (pkColumnsIterator.hasNext() && !column.getName().getString().equals(pkColumnsIterator.next().getFirst().getColumnName())) {
                        throw new SQLExceptionInfo.Builder(SQLExceptionCode.PRIMARY_KEY_OUT_OF_ORDER)
                            .setSchemaName(schemaName)
                            .setTableName(tableName)
                            .setColumnName(column.getName().getString())
                            .build().buildException();
                    }
                    if (tableType == PTableType.VIEW && viewType != ViewType.MAPPED) {
                        throwIfLastPKOfParentIsFixedLength(parent, schemaName, tableName, colDef);
                    }
                    if (!pkColumns.add(column)) {
                        throw new ColumnAlreadyExistsException(schemaName, tableName, column.getName().getString());
                    }
                }
                if (columns.put(column, column) != null) {
                    throw new ColumnAlreadyExistsException(schemaName, tableName, column.getName().getString());
                }
                if ((colDef.getDataType() == PVarbinary.INSTANCE || colDef.getDataType().isArrayType())
                        && SchemaUtil.isPKColumn(column)
                        && pkColumnsIterator.hasNext()) {
                    throw new SQLExceptionInfo.Builder(SQLExceptionCode.VARBINARY_IN_ROW_KEY)
                        .setSchemaName(schemaName)
                        .setTableName(tableName)
                        .setColumnName(column.getName().getString())
                        .build().buildException();
                }
                if (column.getFamilyName() != null) {
                    familyNames.put(
                        IndexUtil.getActualColumnFamilyName(column.getFamilyName().getString()),
                        column.getFamilyName());
                }
            }
            
            // We need a PK definition for a TABLE or mapped VIEW
            if (!isPK && pkColumnsNames.isEmpty() && tableType != PTableType.VIEW && viewType != ViewType.MAPPED) {
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.PRIMARY_KEY_MISSING)
                    .setSchemaName(schemaName)
                    .setTableName(tableName)
                    .build().buildException();
            }
            if (!pkColumnsNames.isEmpty() && pkColumnsNames.size() != pkColumns.size() - pkPositionOffset) { // Then a column name in the primary key constraint wasn't resolved
                Iterator<Pair<ColumnName,SortOrder>> pkColumnNamesIterator = pkColumnsNames.iterator();
                while (pkColumnNamesIterator.hasNext()) {
                    ColumnName colName = pkColumnNamesIterator.next().getFirst();
                    ColumnDef colDef = findColumnDefOrNull(colDefs, colName);
                    if (colDef == null) {
                        throw new ColumnNotFoundException(schemaName, tableName, null, colName.getColumnName());
                    }
                    if (colDef.getColumnDefName().getFamilyName() != null) {
                        throw new SQLExceptionInfo.Builder(SQLExceptionCode.PRIMARY_KEY_WITH_FAMILY_NAME)
                        .setSchemaName(schemaName)
                        .setTableName(tableName)
                        .setColumnName(colDef.getColumnDefName().getColumnName() )
                        .setFamilyName(colDef.getColumnDefName().getFamilyName())
                        .build().buildException();
                    }
                }
                // The above should actually find the specific one, but just in case...
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.INVALID_PRIMARY_KEY_CONSTRAINT)
                    .setSchemaName(schemaName)
                    .setTableName(tableName)
                    .build().buildException();
            }

            List<Pair<byte[],Map<String,Object>>> familyPropList = Lists.newArrayListWithExpectedSize(familyNames.size());
            if (!statement.getProps().isEmpty()) {
                for (String familyName : statement.getProps().keySet()) {
                    if (!familyName.equals(QueryConstants.ALL_FAMILY_PROPERTIES_KEY)) {
                        if (familyNames.get(familyName) == null) {
                            throw new SQLExceptionInfo.Builder(SQLExceptionCode.PROPERTIES_FOR_FAMILY)
                                .setFamilyName(familyName).build().buildException();
                        } else if (statement.getTableType() == PTableType.VIEW) {
                            throw new SQLExceptionInfo.Builder(SQLExceptionCode.VIEW_WITH_PROPERTIES).build().buildException();
                        }
                    }
                }
            }
            throwIfInsufficientColumns(schemaName, tableName, pkColumns, saltBucketNum!=null, multiTenant);

            for (PName familyName : familyNames.values()) {
                String fam = familyName.getString();
                Collection<Pair<String, Object>> props =
                        statement.getProps().get(IndexUtil.getActualColumnFamilyName(fam));
                if (props.isEmpty()) {
                    familyPropList.add(new Pair<byte[],Map<String,Object>>(familyName.getBytes(),commonFamilyProps));
                } else {
                    Map<String,Object> combinedFamilyProps = Maps.newHashMapWithExpectedSize(props.size() + commonFamilyProps.size());
                    combinedFamilyProps.putAll(commonFamilyProps);
                    for (Pair<String,Object> prop : props) {
                        // Don't allow specifying column families for TTL. TTL can only apply for the all the column families of the table
                        // i.e. it can't be column family specific.
                        if (!familyName.equals(QueryConstants.ALL_FAMILY_PROPERTIES_KEY) && prop.getFirst().equals(TTL)) {
                            throw new SQLExceptionInfo.Builder(SQLExceptionCode.COLUMN_FAMILY_NOT_ALLOWED_FOR_TTL).build().buildException();
                        }
                        combinedFamilyProps.put(prop.getFirst(), prop.getSecond());
                    }
                    familyPropList.add(new Pair<byte[],Map<String,Object>>(familyName.getBytes(),combinedFamilyProps));
                }
            }

            if (familyNames.isEmpty()) {
                //if there are no family names, use the default column family name. This also takes care of the case when
                //the table ddl has only PK cols present (which means familyNames is empty).
                byte[] cf =
                        defaultFamilyName == null ? (!isLocalIndex? QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES
                                : QueryConstants.DEFAULT_LOCAL_INDEX_COLUMN_FAMILY_BYTES)
                                : Bytes.toBytes(defaultFamilyName);
                familyPropList.add(new Pair<byte[],Map<String,Object>>(cf, commonFamilyProps));
            }

            // Bootstrapping for our SYSTEM.TABLE that creates itself before it exists
            if (SchemaUtil.isMetaTable(schemaName,tableName)) {
                // TODO: what about stats for system catalog?
                PName newSchemaName = PNameFactory.newName(schemaName);
                // Column names and qualifiers and hardcoded for system tables.
                PTable table = PTableImpl.makePTable(tenantId,newSchemaName, PNameFactory.newName(tableName), tableType,
                        null, MetaDataProtocol.MIN_TABLE_TIMESTAMP, PTable.INITIAL_SEQ_NUM,
                        PNameFactory.newName(QueryConstants.SYSTEM_TABLE_PK_NAME), null, columns.values(), null, null,
                        Collections.<PTable>emptyList(), isImmutableRows,
                        Collections.<PName>emptyList(), defaultFamilyName == null ? null :
                                PNameFactory.newName(defaultFamilyName), null,
                        Boolean.TRUE.equals(disableWAL), false, false, null, null, indexType, true, false, 0, 0L, isNamespaceMapped, autoPartitionSeq, isAppendOnlySchema, ONE_CELL_PER_COLUMN, NON_ENCODED_QUALIFIERS, PTable.EncodedCQCounter.NULL_COUNTER);
                connection.addTable(table, MetaDataProtocol.MIN_TABLE_TIMESTAMP);
            }
            
            // Update column qualifier counters
            if (EncodedColumnsUtil.usesEncodedColumnNames(encodingScheme)) {
                // Store the encoded column counter for phoenix entities that have their own hbase
                // tables i.e. base tables and indexes.
                String schemaNameToUse = tableType == VIEW ? viewPhysicalTable.getSchemaName().getString() : schemaName;
                String tableNameToUse = tableType == VIEW ? viewPhysicalTable.getTableName().getString() : tableName;
                boolean sharedIndex = tableType == PTableType.INDEX && (indexType == IndexType.LOCAL || parent.getType() == PTableType.VIEW);
                // For local indexes and indexes on views, pass on the the tenant id since all their meta-data rows have
                // tenant ids in there.
                String tenantIdToUse = connection.getTenantId() != null && sharedIndex ? connection.getTenantId().getString() : null;
                // When a view adds its own columns, then we need to increase the sequence number of the base table
                // too since we want clients to get the latest PTable of the base table.
                for (Entry<String, Integer> entry : changedCqCounters.entrySet()) {
                    try (PreparedStatement linkStatement = connection.prepareStatement(UPDATE_ENCODED_COLUMN_COUNTER)) {
                        linkStatement.setString(1, tenantIdToUse);
                        linkStatement.setString(2, schemaNameToUse);
                        linkStatement.setString(3, tableNameToUse);
                        linkStatement.setString(4, entry.getKey());
                        linkStatement.setInt(5, entry.getValue());
                        linkStatement.execute();
                    }
                }
                if (tableType == VIEW && !changedCqCounters.isEmpty()) {
                    PreparedStatement incrementStatement = connection.prepareStatement(INCREMENT_SEQ_NUM);
                    incrementStatement.setString(1, null);
                    incrementStatement.setString(2, viewPhysicalTable.getSchemaName().getString());
                    incrementStatement.setString(3, viewPhysicalTable.getTableName().getString());
                    incrementStatement.setLong(4, viewPhysicalTable.getSequenceNumber() + 1);
                    incrementStatement.execute();
                }
                if (connection.getMutationState().toMutations(timestamp).hasNext()) {
                    tableMetaData.addAll(connection.getMutationState().toMutations(timestamp).next().getSecond());
                    connection.rollback();
                }
            }

            short nextKeySeq = 0;

            List<Mutation> columnMetadata = Lists.newArrayListWithExpectedSize(columns.size());
            try (PreparedStatement colUpsert = connection.prepareStatement(INSERT_COLUMN_CREATE_TABLE)) {
                for (Map.Entry<PColumn, PColumn> entry : columns.entrySet()) {
                    PColumn column = entry.getValue();
                    final int columnPosition = column.getPosition();
                    // For client-side cache, we need to update the column
                    // set the autoPartition column attributes
                    if (parent != null && parent.getAutoPartitionSeqName() != null
                            && parent.getPKColumns().get(MetaDataUtil.getAutoPartitionColIndex(parent)).equals(column)) {
                        entry.setValue(column = new DelegateColumn(column) {
                            @Override
                            public byte[] getViewConstant() {
                                // set to non-null value so that we will generate a Put that
                                // will be set correctly on the server
                                return QueryConstants.EMPTY_COLUMN_VALUE_BYTES;
                            }

                            @Override
                            public boolean isViewReferenced() {
                                return true;
                            }
                        });
                    } else if (isViewColumnReferenced != null) {
                        if (viewColumnConstants != null && columnPosition < viewColumnConstants.length) {
                            entry.setValue(column = new DelegateColumn(column) {
                                @Override
                                public byte[] getViewConstant() {
                                    return viewColumnConstants[columnPosition];
                                }
                                
                                @Override
                                public boolean isViewReferenced() {
                                    return isViewColumnReferenced.get(columnPosition);
                                }
                            });
                        } else {
                            entry.setValue(column = new DelegateColumn(column) {
                                @Override
                                public boolean isViewReferenced() {
                                    return isViewColumnReferenced.get(columnPosition);
                                }
                            });
                        }
                    }
                    Short keySeq = SchemaUtil.isPKColumn(column) ? ++nextKeySeq : null;
                    addColumnMutation(schemaName, tableName, column, colUpsert, parentTableName, pkName, keySeq, saltBucketNum != null);
                    columnMetadata.addAll(connection.getMutationState().toMutations(timestamp).next().getSecond());
                    connection.rollback();
                }
            }
            // add the columns in reverse order since we reverse the list later
            Collections.reverse(columnMetadata);
            tableMetaData.addAll(columnMetadata);
            String dataTableName = parent == null || tableType == PTableType.VIEW ? null : parent.getTableName().getString();
            PIndexState indexState = parent == null || tableType == PTableType.VIEW  ? null : PIndexState.BUILDING;
            PreparedStatement tableUpsert = connection.prepareStatement(CREATE_TABLE);
            tableUpsert.setString(1, tenantIdStr);
            tableUpsert.setString(2, schemaName);
            tableUpsert.setString(3, tableName);
            tableUpsert.setString(4, tableType.getSerializedValue());
            tableUpsert.setLong(5, PTable.INITIAL_SEQ_NUM);
            tableUpsert.setInt(6, position);
            if (saltBucketNum != null) {
                tableUpsert.setInt(7, saltBucketNum);
            } else {
                tableUpsert.setNull(7, Types.INTEGER);
            }
            tableUpsert.setString(8, pkName);
            tableUpsert.setString(9, dataTableName);
            tableUpsert.setString(10, indexState == null ? null : indexState.getSerializedValue());
            tableUpsert.setBoolean(11, isImmutableRows);
            tableUpsert.setString(12, defaultFamilyName);
            if (parent != null && parent.getAutoPartitionSeqName() != null && viewStatement==null) {
                // set to non-null value so that we will generate a Put that
                // will be set correctly on the server
                tableUpsert.setString(13, QueryConstants.EMPTY_COLUMN_VALUE);
            }
            else {
                tableUpsert.setString(13, viewStatement);
            }
            tableUpsert.setBoolean(14, disableWAL);
            tableUpsert.setBoolean(15, multiTenant);
            if (viewType == null) {
                tableUpsert.setNull(16, Types.TINYINT);
            } else {
                tableUpsert.setByte(16, viewType.getSerializedValue());
            }
            if (indexType == null) {
                tableUpsert.setNull(17, Types.TINYINT);
            } else {
                tableUpsert.setByte(17, indexType.getSerializedValue());
            }
            tableUpsert.setBoolean(18, storeNulls);
            if (parent != null && tableType == PTableType.VIEW) {
                tableUpsert.setInt(19, parent.getColumns().size());
            } else {
                tableUpsert.setInt(19, BASE_TABLE_BASE_COLUMN_COUNT);
            }
            tableUpsert.setBoolean(20, transactional);
            tableUpsert.setLong(21, updateCacheFrequency);
            tableUpsert.setBoolean(22, isNamespaceMapped);
            if (autoPartitionSeq == null) {
                tableUpsert.setNull(23, Types.VARCHAR);
            } else {
                tableUpsert.setString(23, autoPartitionSeq);
            }
            tableUpsert.setBoolean(24, isAppendOnlySchema);
            if (guidePostsWidth == null) {
                tableUpsert.setNull(25, Types.BIGINT);                
            } else {
                tableUpsert.setLong(25, guidePostsWidth);
            }
            tableUpsert.setByte(26, immutableStorageScheme.getSerializedMetadataValue());
            tableUpsert.setByte(27, encodingScheme.getSerializedMetadataValue());
            tableUpsert.execute();

            if (asyncCreatedDate != null) {
                PreparedStatement setAsync = connection.prepareStatement(SET_ASYNC_CREATED_DATE);
                setAsync.setString(1, tenantIdStr);
                setAsync.setString(2, schemaName);
                setAsync.setString(3, tableName);
                setAsync.setDate(4, asyncCreatedDate);
                setAsync.execute();
            }
            tableMetaData.addAll(connection.getMutationState().toMutations(timestamp).next().getSecond());
            connection.rollback();

            /*
             * The table metadata must be in the following order:
             * 1) table header row
             * 2) ordered column rows
             * 3) parent table header row
             */
            Collections.reverse(tableMetaData);
            
			if (indexType != IndexType.LOCAL) {
                splits = SchemaUtil.processSplits(splits, pkColumns, saltBucketNum, connection.getQueryServices().getProps().getBoolean(
                        QueryServices.FORCE_ROW_KEY_ORDER_ATTRIB, QueryServicesOptions.DEFAULT_FORCE_ROW_KEY_ORDER));
            }
            MetaDataMutationResult result = connection.getQueryServices().createTable(
                    tableMetaData,
                    viewType == ViewType.MAPPED || allocateIndexId ? physicalNames.get(0).getBytes() : null,
                    tableType, tableProps, familyPropList, splits, isNamespaceMapped, allocateIndexId);
            MutationCode code = result.getMutationCode();
            switch(code) {
            case TABLE_ALREADY_EXISTS:
                if (result.getTable() != null) { // Can happen for transactional table that already exists as HBase table
                    addTableToCache(result);
                }
                if (!statement.ifNotExists()) {
                    throw new TableAlreadyExistsException(schemaName, tableName, result.getTable());
                }
                return null;
            case PARENT_TABLE_NOT_FOUND:
                throw new TableNotFoundException(schemaName, parent.getName().getString());
            case NEWER_TABLE_FOUND:
                // Add table to ConnectionQueryServices so it's cached, but don't add
                // it to this connection as we can't see it.
                if (!statement.ifNotExists()) {
                    throw new NewerTableAlreadyExistsException(schemaName, tableName, result.getTable());
                }
            case UNALLOWED_TABLE_MUTATION:
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.CANNOT_MUTATE_TABLE)
                    .setSchemaName(schemaName).setTableName(tableName).build().buildException();
            case CONCURRENT_TABLE_MUTATION:
                addTableToCache(result);
                throw new ConcurrentTableMutationException(schemaName, tableName);
            case AUTO_PARTITION_SEQUENCE_NOT_FOUND:
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.AUTO_PARTITION_SEQUENCE_UNDEFINED)
                .setSchemaName(schemaName).setTableName(tableName).build().buildException();
            case CANNOT_COERCE_AUTO_PARTITION_ID:
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.CANNOT_COERCE_AUTO_PARTITION_ID)
                .setSchemaName(schemaName).setTableName(tableName).build().buildException();
            case TOO_MANY_INDEXES:
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.TOO_MANY_INDEXES)
                        .setSchemaName(SchemaUtil.getSchemaNameFromFullName(parent.getPhysicalName().getString()))
                        .setTableName(SchemaUtil.getTableNameFromFullName(parent.getPhysicalName().getString())).build()
                        .buildException();
            default:
                // If the parent table of the view has the auto partition sequence name attribute,
                // set the view statement and relevant partition column attributes correctly
                if (parent!=null && parent.getAutoPartitionSeqName()!=null) {
                    final PColumn autoPartitionCol = parent.getPKColumns().get(MetaDataUtil.getAutoPartitionColIndex(parent));
                    final Long autoPartitionNum = Long.valueOf(result.getAutoPartitionNum());
                    columns.put(autoPartitionCol, new DelegateColumn(autoPartitionCol) {
                        @Override
                        public byte[] getViewConstant() {
                            PDataType dataType = autoPartitionCol.getDataType();
                            Object val = dataType.toObject(autoPartitionNum, PLong.INSTANCE);
                            byte[] bytes = new byte [dataType.getByteSize() + 1];
                            dataType.toBytes(val, bytes, 0);
                            return bytes;
                        }
                        @Override
                        public boolean isViewReferenced() {
                            return true;
                        }
                    });
                    String viewPartitionClause = QueryUtil.getViewPartitionClause(MetaDataUtil.getAutoPartitionColumnName(parent), autoPartitionNum);
                    if (viewStatement!=null) {
                        viewStatement = viewStatement + " AND " + viewPartitionClause;
                    }
                    else {
                        viewStatement = QueryUtil.getViewStatement(parent.getSchemaName().getString(), parent.getTableName().getString(), viewPartitionClause);
                    }
                }
                PName newSchemaName = PNameFactory.newName(schemaName);
                /*
                 * It doesn't hurt for the PTable of views to have the cqCounter. However, views always rely on the
                 * parent table's counter to dole out encoded column qualifiers. So setting the counter as NULL_COUNTER
                 * for extra safety.
                 */
                EncodedCQCounter cqCounterToBe = tableType == PTableType.VIEW ? NULL_COUNTER : cqCounter;
                PTable table =  PTableImpl.makePTable(
                        tenantId, newSchemaName, PNameFactory.newName(tableName), tableType, indexState, timestamp!=null ? timestamp : result.getMutationTime(),
                        PTable.INITIAL_SEQ_NUM, pkName == null ? null : PNameFactory.newName(pkName), saltBucketNum, columns.values(),
                        parent == null ? null : parent.getSchemaName(), parent == null ? null : parent.getTableName(), Collections.<PTable>emptyList(), isImmutableRows,
                        physicalNames, defaultFamilyName == null ? null : PNameFactory.newName(defaultFamilyName), viewStatement, Boolean.TRUE.equals(disableWAL), multiTenant, storeNulls, viewType,
                        result.getViewIndexId(), indexType, rowKeyOrderOptimizable, transactional, updateCacheFrequency, 0L, isNamespaceMapped, autoPartitionSeq, isAppendOnlySchema, immutableStorageScheme, encodingScheme, cqCounterToBe);
                result = new MetaDataMutationResult(code, result.getMutationTime(), table, true);
                addTableToCache(result);
                return table;
            }
        } finally {
            connection.setAutoCommit(wasAutoCommit);
        }
    }

    private static boolean isPkColumn(PrimaryKeyConstraint pkConstraint, ColumnDef colDef, ColumnName columnDefName) {
        return colDef.isPK() || (pkConstraint != null && pkConstraint.getColumnWithSortOrder(columnDefName) != null);
    }
    
    /**
     * A table can be a parent table to tenant-specific tables if all of the following conditions are true:
     * <p>
     * FOR TENANT-SPECIFIC TABLES WITH TENANT_TYPE_ID SPECIFIED:
     * <ol>
     * <li>It has 3 or more PK columns AND
     * <li>First PK (tenant id) column is not nullible AND
     * <li>Firsts PK column's data type is either VARCHAR or CHAR AND
     * <li>Second PK (tenant type id) column is not nullible AND
     * <li>Second PK column data type is either VARCHAR or CHAR
     * </ol>
     * FOR TENANT-SPECIFIC TABLES WITH NO TENANT_TYPE_ID SPECIFIED:
     * <ol>
     * <li>It has 2 or more PK columns AND
     * <li>First PK (tenant id) column is not nullible AND
     * <li>Firsts PK column's data type is either VARCHAR or CHAR
     * </ol>
     */
    private static void throwIfInsufficientColumns(String schemaName, String tableName, Collection<PColumn> columns, boolean isSalted, boolean isMultiTenant) throws SQLException {
        if (!isMultiTenant) {
            return;
        }
        int nPKColumns = columns.size() - (isSalted ? 1 : 0);
        if (nPKColumns < 2) {
            throw new SQLExceptionInfo.Builder(INSUFFICIENT_MULTI_TENANT_COLUMNS).setSchemaName(schemaName).setTableName(tableName).build().buildException();
        }
        Iterator<PColumn> iterator = columns.iterator();
        if (isSalted) {
            iterator.next();
        }
        // Tenant ID must be VARCHAR or CHAR and be NOT NULL
        // NOT NULL is a requirement, since otherwise the table key would conflict
        // potentially with the global table definition.
        PColumn tenantIdCol = iterator.next();
        if ( tenantIdCol.isNullable()) {
            throw new SQLExceptionInfo.Builder(INSUFFICIENT_MULTI_TENANT_COLUMNS).setSchemaName(schemaName).setTableName(tableName).build().buildException();
        }
    }

    public MutationState dropTable(DropTableStatement statement) throws SQLException {
        String schemaName = connection.getSchema() != null && statement.getTableName().getSchemaName() == null
                ? connection.getSchema() : statement.getTableName().getSchemaName();
        String tableName = statement.getTableName().getTableName();
        return dropTable(schemaName, tableName, null, statement.getTableType(), statement.ifExists(), statement.cascade());
    }

    public MutationState dropFunction(DropFunctionStatement statement) throws SQLException {
        return dropFunction(statement.getFunctionName(), statement.ifExists());
    }

    public MutationState dropIndex(DropIndexStatement statement) throws SQLException {
        String schemaName = statement.getTableName().getSchemaName();
        String tableName = statement.getIndexName().getName();
        String parentTableName = statement.getTableName().getTableName();
        return dropTable(schemaName, tableName, parentTableName, PTableType.INDEX, statement.ifExists(), false);
    }

    private MutationState dropFunction(String functionName,
            boolean ifExists) throws SQLException {
        connection.rollback();
        boolean wasAutoCommit = connection.getAutoCommit();
        try {
            PName tenantId = connection.getTenantId();
            byte[] key =
                    SchemaUtil.getFunctionKey(tenantId == null ? ByteUtil.EMPTY_BYTE_ARRAY
                            : tenantId.getBytes(), Bytes.toBytes(functionName));
            Long scn = connection.getSCN();
            long clientTimeStamp = scn == null ? HConstants.LATEST_TIMESTAMP : scn;
            try {
                PFunction function = connection.getMetaDataCache().getFunction(new PTableKey(tenantId, functionName));
                if (function.isTemporaryFunction()) {
                    connection.removeFunction(tenantId, functionName, clientTimeStamp);
                    return new MutationState(0, connection);
                }
            } catch(FunctionNotFoundException e) {

            }
            List<Mutation> functionMetaData = Lists.newArrayListWithExpectedSize(2);
            Delete functionDelete = new Delete(key, clientTimeStamp);
            functionMetaData.add(functionDelete);
            MetaDataMutationResult result = connection.getQueryServices().dropFunction(functionMetaData, ifExists);
            MutationCode code = result.getMutationCode();
            switch (code) {
            case FUNCTION_NOT_FOUND:
                if (!ifExists) {
                    throw new FunctionNotFoundException(functionName);
                }
                break;
            default:
                connection.removeFunction(tenantId, functionName, result.getMutationTime());
                break;
            }
            return new MutationState(0, connection);
        } finally {
            connection.setAutoCommit(wasAutoCommit);
        }
    }
    private MutationState dropTable(String schemaName, String tableName, String parentTableName, PTableType tableType,
            boolean ifExists, boolean cascade) throws SQLException {
        connection.rollback();
        boolean wasAutoCommit = connection.getAutoCommit();
        try {
            PName tenantId = connection.getTenantId();
            String tenantIdStr = tenantId == null ? null : tenantId.getString();
            byte[] key = SchemaUtil.getTableKey(tenantIdStr, schemaName, tableName);
            Long scn = connection.getSCN();
            long clientTimeStamp = scn == null ? HConstants.LATEST_TIMESTAMP : scn;
            List<Mutation> tableMetaData = Lists.newArrayListWithExpectedSize(2);
            Delete tableDelete = new Delete(key, clientTimeStamp);
            tableMetaData.add(tableDelete);
            boolean hasViewIndexTable = false;
            if (parentTableName != null) {
                byte[] linkKey = MetaDataUtil.getParentLinkKey(tenantIdStr, schemaName, parentTableName, tableName);
                Delete linkDelete = new Delete(linkKey, clientTimeStamp);
                tableMetaData.add(linkDelete);
            }
            MetaDataMutationResult result = connection.getQueryServices().dropTable(tableMetaData, tableType, cascade);
            MutationCode code = result.getMutationCode();
            PTable table = result.getTable();
            switch (code) {
            case TABLE_NOT_FOUND:
                if (!ifExists) { throw new TableNotFoundException(schemaName, tableName); }
                break;
            case NEWER_TABLE_FOUND:
                throw new NewerTableAlreadyExistsException(schemaName, tableName, result.getTable());
            case UNALLOWED_TABLE_MUTATION:
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.CANNOT_MUTATE_TABLE)

                .setSchemaName(schemaName).setTableName(tableName).build().buildException();
            default:
                connection.removeTable(tenantId, SchemaUtil.getTableName(schemaName, tableName), parentTableName, result.getMutationTime());

                if (table != null) {
                    boolean dropMetaData = false;
                    long ts = (scn == null ? result.getMutationTime() : scn);
                    List<TableRef> tableRefs = Lists.newArrayListWithExpectedSize(2 + table.getIndexes().size());
                    connection.setAutoCommit(true);
                    if (tableType == PTableType.VIEW) {
                        for (PTable index : table.getIndexes()) {
                            tableRefs.add(new TableRef(null, index, ts, false));
                        }
                    } else {
                        dropMetaData = result.getTable().getViewIndexId() == null &&
                                connection.getQueryServices().getProps().getBoolean(DROP_METADATA_ATTRIB, DEFAULT_DROP_METADATA);
                        // Create empty table and schema - they're only used to get the name from
                        // PName name, PTableType type, long timeStamp, long sequenceNumber, List<PColumn> columns
                        // All multi-tenant tables have a view index table, so no need to check in that case
                        if (parentTableName == null) {
                            hasViewIndexTable = true;// keeping always true for deletion of stats if view index present
                            // or not
                            MetaDataUtil.deleteViewIndexSequences(connection, table.getPhysicalName(),
                                    table.isNamespaceMapped());
                            byte[] viewIndexPhysicalName = MetaDataUtil
                                    .getViewIndexPhysicalName(table.getPhysicalName().getBytes());
                            if (!dropMetaData) {
                                // we need to drop rows only when actually view index exists
                                try (HBaseAdmin admin = connection.getQueryServices().getAdmin()) {
                                    hasViewIndexTable = admin.tableExists(viewIndexPhysicalName);
                                } catch (IOException e1) {
                                    // absorbing as it is not critical check
                                }
                            }
                        }
                        if (tableType == PTableType.TABLE
                                && (table.isMultiTenant() || hasViewIndexTable)) {
                            if (hasViewIndexTable) {
                                byte[] viewIndexPhysicalName = MetaDataUtil.getViewIndexPhysicalName(table.getPhysicalName().getBytes());
                                PTable viewIndexTable = new PTableImpl(null,
                                        SchemaUtil.getSchemaNameFromFullName(viewIndexPhysicalName),
                                        SchemaUtil.getTableNameFromFullName(viewIndexPhysicalName), ts,
                                        table.getColumnFamilies(),table.isNamespaceMapped(), table.getImmutableStorageScheme(), table.getEncodingScheme());
                                tableRefs.add(new TableRef(null, viewIndexTable, ts, false));
                            }
                        }
                        tableRefs.add(new TableRef(null, table, ts, false));
                        // TODO: Let the standard mutable secondary index maintenance handle this?
                        for (PTable index : table.getIndexes()) {
                            tableRefs.add(new TableRef(null, index, ts, false));
                        }
                        deleteFromStatsTable(tableRefs, ts);
                    }
                    if (!dropMetaData) {
                        MutationPlan plan = new PostDDLCompiler(connection).compile(tableRefs, null, null,
                                Collections.<PColumn> emptyList(), ts);
                        // Delete everything in the column. You'll still be able to do queries at earlier timestamps
                        return connection.getQueryServices().updateData(plan);
                    }
                }
                break;
            }
            return new MutationState(0, connection);
        } finally {
            connection.setAutoCommit(wasAutoCommit);
        }
    }

    private void deleteFromStatsTable(List<TableRef> tableRefs, long ts) throws SQLException {
        Properties props = new Properties(connection.getClientInfo());
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        Connection conn = new PhoenixConnection(connection.getQueryServices(), connection, ts);
        conn.setAutoCommit(true);
        boolean success = false;
        SQLException sqlException = null;
        try {
            StringBuilder buf = new StringBuilder("DELETE FROM SYSTEM.STATS WHERE PHYSICAL_NAME IN (");
            for (TableRef ref : tableRefs) {
                buf.append("'" + ref.getTable().getPhysicalName().getString() + "',");
            }
            buf.setCharAt(buf.length() - 1, ')');
            if(tableRefs.get(0).getTable().getIndexType()==IndexType.LOCAL) {
                buf.append(" AND COLUMN_FAMILY IN(");
                if (tableRefs.get(0).getTable().getColumnFamilies().isEmpty()) {
                    buf.append("'" + QueryConstants.DEFAULT_LOCAL_INDEX_COLUMN_FAMILY + "',");
                } else {
                    for(PColumnFamily cf : tableRefs.get(0).getTable().getColumnFamilies()) {
                        buf.append("'" + cf.getName().getString() + "',");
                    }
                }
                buf.setCharAt(buf.length() - 1, ')');
            }
            conn.createStatement().execute(buf.toString());
            success = true;
        } catch (SQLException e) {
            sqlException = e;
        } finally {
            try {
                conn.close();
            } catch (SQLException e) {
                if (sqlException == null) {
                    // If we're not in the middle of throwing another exception
                    // then throw the exception we got on close.
                    if (success) {
                        sqlException = e;
                    }
                } else {
                    sqlException.setNextException(e);
                }
            }
            if (sqlException != null) { throw sqlException; }
        }
    }

    private MutationCode processMutationResult(String schemaName, String tableName, MetaDataMutationResult result) throws SQLException {
        final MutationCode mutationCode = result.getMutationCode();
        PName tenantId = connection.getTenantId();
        switch (mutationCode) {
        case TABLE_NOT_FOUND:
            // Only called for add/remove column so parentTableName will always be null
            connection.removeTable(tenantId, SchemaUtil.getTableName(schemaName, tableName), null, HConstants.LATEST_TIMESTAMP);
            throw new TableNotFoundException(schemaName, tableName);
        case UNALLOWED_TABLE_MUTATION:
            String columnName = null;
            String familyName = null;
            String msg = null;
            // TODO: better to return error code
            if (result.getColumnName() != null) {
                familyName = result.getFamilyName() == null ? null : Bytes.toString(result.getFamilyName());
                columnName = Bytes.toString(result.getColumnName());
                msg = "Cannot add/drop column referenced by VIEW";
            }
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.CANNOT_MUTATE_TABLE)
            .setSchemaName(schemaName).setTableName(tableName).setFamilyName(familyName).setColumnName(columnName).setMessage(msg).build().buildException();
        case NO_OP:
        case COLUMN_ALREADY_EXISTS:
        case COLUMN_NOT_FOUND:
            break;
        case CONCURRENT_TABLE_MUTATION:
            addTableToCache(result);
            if (logger.isDebugEnabled()) {
                logger.debug(LogUtil.addCustomAnnotations("CONCURRENT_TABLE_MUTATION for table " + SchemaUtil.getTableName(schemaName, tableName), connection));
            }
            throw new ConcurrentTableMutationException(schemaName, tableName);
        case NEWER_TABLE_FOUND:
            // TODO: update cache?
            //            if (result.getTable() != null) {
            //                connection.addTable(result.getTable());
            //            }
            throw new NewerTableAlreadyExistsException(schemaName, tableName, result.getTable());
        case NO_PK_COLUMNS:
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.PRIMARY_KEY_MISSING)
            .setSchemaName(schemaName).setTableName(tableName).build().buildException();
        case TABLE_ALREADY_EXISTS:
            break;
        default:
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.UNEXPECTED_MUTATION_CODE).setSchemaName(schemaName)
            .setTableName(tableName).setMessage("mutation code: " + mutationCode).build().buildException();
        }
        return mutationCode;
    }

    private  long incrementTableSeqNum(PTable table, PTableType expectedType, int columnCountDelta, Boolean isTransactional, Long updateCacheFrequency) throws SQLException {
        return incrementTableSeqNum(table, expectedType, columnCountDelta, isTransactional, updateCacheFrequency, null, null, null, null, -1L, null, null);
    }

    private long incrementTableSeqNum(PTable table, PTableType expectedType, int columnCountDelta,
            Boolean isTransactional, Long updateCacheFrequency, Boolean isImmutableRows, Boolean disableWAL,
            Boolean isMultiTenant, Boolean storeNulls, Long guidePostWidth, Boolean appendOnlySchema, ImmutableStorageScheme immutableStorageScheme)
            throws SQLException {
        String schemaName = table.getSchemaName().getString();
        String tableName = table.getTableName().getString();
        // Ordinal position is 1-based and we don't count SALT column in ordinal position
        int totalColumnCount = table.getColumns().size() + (table.getBucketNum() == null ? 0 : -1);
        final long seqNum = table.getSequenceNumber() + 1;
        PreparedStatement tableUpsert = connection.prepareStatement(MUTATE_TABLE);
        String tenantId = connection.getTenantId() == null ? null : connection.getTenantId().getString();
        try {
            tableUpsert.setString(1, tenantId);
            tableUpsert.setString(2, schemaName);
            tableUpsert.setString(3, tableName);
            tableUpsert.setString(4, expectedType.getSerializedValue());
            tableUpsert.setLong(5, seqNum);
            tableUpsert.setInt(6, totalColumnCount + columnCountDelta);
            tableUpsert.execute();
        } finally {
            tableUpsert.close();
        }
        if (isImmutableRows != null) {
            mutateBooleanProperty(tenantId, schemaName, tableName, IMMUTABLE_ROWS, isImmutableRows);
        }
        if (disableWAL != null) {
            mutateBooleanProperty(tenantId, schemaName, tableName, DISABLE_WAL, disableWAL);
        }
        if (isMultiTenant != null) {
            mutateBooleanProperty(tenantId, schemaName, tableName, MULTI_TENANT, isMultiTenant);
        }
        if (storeNulls != null) {
            mutateBooleanProperty(tenantId, schemaName, tableName, STORE_NULLS, storeNulls);
        }
        if (isTransactional != null) {
            mutateBooleanProperty(tenantId, schemaName, tableName, TRANSACTIONAL, isTransactional);
        }
        if (updateCacheFrequency != null) {
            mutateLongProperty(tenantId, schemaName, tableName, UPDATE_CACHE_FREQUENCY, updateCacheFrequency);
        }
        if (guidePostWidth == null || guidePostWidth >= 0) {
            mutateLongProperty(tenantId, schemaName, tableName, GUIDE_POSTS_WIDTH, guidePostWidth);
        }
        if (appendOnlySchema !=null) {
            mutateBooleanProperty(tenantId, schemaName, tableName, APPEND_ONLY_SCHEMA, appendOnlySchema);
        }
        if (immutableStorageScheme !=null) {
            mutateStringProperty(tenantId, schemaName, tableName, IMMUTABLE_STORAGE_SCHEME, immutableStorageScheme.name());
        }
        
        return seqNum;
    }

    private void mutateBooleanProperty(String tenantId, String schemaName, String tableName,
            String propertyName, boolean propertyValue) throws SQLException {
        String updatePropertySql = "UPSERT INTO " + SYSTEM_CATALOG_SCHEMA + ".\"" + SYSTEM_CATALOG_TABLE + "\"( " +
                TENANT_ID + "," +
                TABLE_SCHEM + "," +
                TABLE_NAME + "," +
                propertyName +
                ") VALUES (?, ?, ?, ?)";
        try (PreparedStatement tableBoolUpsert = connection.prepareStatement(updatePropertySql)) {
            tableBoolUpsert.setString(1, tenantId);
            tableBoolUpsert.setString(2, schemaName);
            tableBoolUpsert.setString(3, tableName);
            tableBoolUpsert.setBoolean(4, propertyValue);
            tableBoolUpsert.execute();
        }
    }

    private void mutateLongProperty(String tenantId, String schemaName, String tableName,
            String propertyName, Long propertyValue) throws SQLException {
        String updatePropertySql = "UPSERT INTO " + SYSTEM_CATALOG_SCHEMA + ".\"" + SYSTEM_CATALOG_TABLE + "\"( " +
                TENANT_ID + "," +
                TABLE_SCHEM + "," +
                TABLE_NAME + "," +
                propertyName +
                ") VALUES (?, ?, ?, ?)";
        try (PreparedStatement tableBoolUpsert = connection.prepareStatement(updatePropertySql)) {
            tableBoolUpsert.setString(1, tenantId);
            tableBoolUpsert.setString(2, schemaName);
            tableBoolUpsert.setString(3, tableName);
            if (propertyValue == null) {
                tableBoolUpsert.setNull(4, Types.BIGINT);
            } else {
                tableBoolUpsert.setLong(4, propertyValue);
            }
            tableBoolUpsert.execute();
        }
    }
    
    private void mutateStringProperty(String tenantId, String schemaName, String tableName,
            String propertyName, String propertyValue) throws SQLException {
        String updatePropertySql = "UPSERT INTO " + SYSTEM_CATALOG_SCHEMA + ".\"" + SYSTEM_CATALOG_TABLE + "\"( " +
                TENANT_ID + "," +
                TABLE_SCHEM + "," +
                TABLE_NAME + "," +
                propertyName +
                ") VALUES (?, ?, ?, ?)";
        try (PreparedStatement tableBoolUpsert = connection.prepareStatement(updatePropertySql)) {
            tableBoolUpsert.setString(1, tenantId);
            tableBoolUpsert.setString(2, schemaName);
            tableBoolUpsert.setString(3, tableName);
            tableBoolUpsert.setString(4, propertyValue);
            tableBoolUpsert.execute();
        }
    }

    public MutationState addColumn(AddColumnStatement statement) throws SQLException {
        PTable table = FromCompiler.getResolver(statement, connection).getTables().get(0).getTable();
        return addColumn(table, statement.getColumnDefs(), statement.getProps(), statement.ifNotExists(), false, statement.getTable(), statement.getTableType());
    }

    public MutationState addColumn(PTable table, List<ColumnDef> origColumnDefs,
            ListMultimap<String, Pair<String, Object>> stmtProperties, boolean ifNotExists,
            boolean removeTableProps, NamedTableNode namedTableNode, PTableType tableType)
                    throws SQLException {
        connection.rollback();
        boolean wasAutoCommit = connection.getAutoCommit();
        try {
            connection.setAutoCommit(false);
            PName tenantId = connection.getTenantId();
            String schemaName = table.getSchemaName().getString();
            String tableName = table.getTableName().getString();
            Boolean isImmutableRowsProp = null;
            Boolean multiTenantProp = null;
            Boolean disableWALProp = null;
            Boolean storeNullsProp = null;
            Boolean isTransactionalProp = null;
            Long updateCacheFrequencyProp = null;
            Boolean appendOnlySchemaProp = null;
            Long guidePostWidth = -1L;
            ImmutableStorageScheme immutableStorageSchemeProp = null;

            Map<String, List<Pair<String, Object>>> properties = new HashMap<>(stmtProperties.size());
            List<ColumnDef> columnDefs = null;
            if (table.isAppendOnlySchema()) {
                // only make the rpc if we are adding new columns
                columnDefs = Lists.newArrayList();
                for (ColumnDef columnDef : origColumnDefs) {
                    String familyName = columnDef.getColumnDefName().getFamilyName();
                    String columnName = columnDef.getColumnDefName().getColumnName();
                    if (familyName!=null) {
                        try {
                            PColumnFamily columnFamily = table.getColumnFamily(familyName);
                            columnFamily.getPColumnForColumnName(columnName);
                            if (!ifNotExists) {
                                throw new ColumnAlreadyExistsException(schemaName, tableName, columnName);
                            }
                        }
                        catch (ColumnFamilyNotFoundException | ColumnNotFoundException e){
                            columnDefs.add(columnDef);
                        }
                    }
                    else {
                        try {
                            table.getColumnForColumnName(columnName);
                            if (!ifNotExists) {
                                throw new ColumnAlreadyExistsException(schemaName, tableName, columnName);
                            }
                        }
                        catch (ColumnNotFoundException e){
                            columnDefs.add(columnDef);
                        }
                    }
                }
            }
            else {
                columnDefs = origColumnDefs == null ? Collections.<ColumnDef>emptyList() : origColumnDefs;
            }
            for (String family : stmtProperties.keySet()) {
                List<Pair<String, Object>> origPropsList = stmtProperties.get(family);
                List<Pair<String, Object>> propsList = Lists.newArrayListWithExpectedSize(origPropsList.size());
                for (Pair<String, Object> prop : origPropsList) {
                    String propName = prop.getFirst();
                    if (TableProperty.isPhoenixTableProperty(propName)) {
                        TableProperty tableProp = TableProperty.valueOf(propName);
                        tableProp.validate(true, !family.equals(QueryConstants.ALL_FAMILY_PROPERTIES_KEY), table.getType());
                        Object value = tableProp.getValue(prop.getSecond());
                        if (propName.equals(PTable.IS_IMMUTABLE_ROWS_PROP_NAME)) {
                            isImmutableRowsProp = (Boolean)value;
                        } else if (propName.equals(PhoenixDatabaseMetaData.MULTI_TENANT)) {
                            multiTenantProp = (Boolean)value;
                        } else if (propName.equals(DISABLE_WAL)) {
                            disableWALProp = (Boolean)value;
                        } else if (propName.equals(STORE_NULLS)) {
                            storeNullsProp = (Boolean)value;
                        } else if (propName.equals(TRANSACTIONAL)) {
                            isTransactionalProp = (Boolean)value;
                        } else if (propName.equals(UPDATE_CACHE_FREQUENCY)) {
                            updateCacheFrequencyProp = (Long)value;
                        } else if (propName.equals(GUIDE_POSTS_WIDTH)) {
                            guidePostWidth = (Long)value;
                        } else if (propName.equals(APPEND_ONLY_SCHEMA)) {
                            appendOnlySchemaProp = (Boolean) value;
                        } else if (propName.equalsIgnoreCase(IMMUTABLE_STORAGE_SCHEME)) {
                            immutableStorageSchemeProp = (ImmutableStorageScheme)value;
                        }
                    }
                    // if removeTableProps is true only add the property if it is not a HTable or Phoenix Table property
                    if (!removeTableProps || (!TableProperty.isPhoenixTableProperty(propName) && !MetaDataUtil.isHTableProperty(propName))) {
                        propsList.add(prop);
                    }
                }
                properties.put(family, propsList);
            }
            boolean retried = false;
            boolean changingPhoenixTableProperty = false;
            boolean nonTxToTx = false;
            while (true) {
                ColumnResolver resolver = FromCompiler.getResolver(namedTableNode, connection);
                table = resolver.getTables().get(0).getTable();
                int nIndexes = table.getIndexes().size();
                int numCols = columnDefs.size();
                int nNewColumns = numCols;
                List<Mutation> tableMetaData = Lists.newArrayListWithExpectedSize((1 + nNewColumns) * (nIndexes + 1));
                List<Mutation> columnMetaData = Lists.newArrayListWithExpectedSize(nNewColumns * (nIndexes + 1));
                if (logger.isDebugEnabled()) {
                    logger.debug(LogUtil.addCustomAnnotations("Resolved table to " + table.getName().getString() + " with seqNum " + table.getSequenceNumber() + " at timestamp " + table.getTimeStamp() + " with " + table.getColumns().size() + " columns: " + table.getColumns(), connection));
                }

                int position = table.getColumns().size();

                List<PColumn> currentPKs = table.getPKColumns();
                PColumn lastPK = currentPKs.get(currentPKs.size()-1);
                // Disallow adding columns if the last column is VARBIANRY.
                if (lastPK.getDataType() == PVarbinary.INSTANCE || lastPK.getDataType().isArrayType()) {
                    throw new SQLExceptionInfo.Builder(SQLExceptionCode.VARBINARY_LAST_PK)
                    .setColumnName(lastPK.getName().getString()).build().buildException();
                }
                // Disallow adding columns if last column is fixed width and nullable.
                if (lastPK.isNullable() && lastPK.getDataType().isFixedWidth()) {
                    throw new SQLExceptionInfo.Builder(SQLExceptionCode.NULLABLE_FIXED_WIDTH_LAST_PK)
                    .setColumnName(lastPK.getName().getString()).build().buildException();
                }

                Boolean isImmutableRows = null;
                if (isImmutableRowsProp != null) {
                    if (isImmutableRowsProp.booleanValue() != table.isImmutableRows()) {
                    	if (table.getImmutableStorageScheme() != ImmutableStorageScheme.ONE_CELL_PER_COLUMN) {
                    		throw new SQLExceptionInfo.Builder(SQLExceptionCode.CANNOT_ALTER_IMMUTABLE_ROWS_PROPERTY)
                    		.setSchemaName(schemaName).setTableName(tableName).build().buildException();
                    	}
                        isImmutableRows = isImmutableRowsProp;
                        changingPhoenixTableProperty = true;
                    }
                }
                Boolean multiTenant = null;
                if (multiTenantProp != null) {
                    if (multiTenantProp.booleanValue() != table.isMultiTenant()) {
                        multiTenant = multiTenantProp;
                        changingPhoenixTableProperty = true;
                    }
                }
                Boolean disableWAL = null;
                if (disableWALProp != null) {
                    if (disableWALProp.booleanValue() != table.isWALDisabled()) {
                        disableWAL = disableWALProp;
                        changingPhoenixTableProperty = true;
                    }
                }
                Long updateCacheFrequency = null;
                if (updateCacheFrequencyProp != null) {
                    if (updateCacheFrequencyProp.longValue() != table.getUpdateCacheFrequency()) {
                        updateCacheFrequency = updateCacheFrequencyProp;
                        changingPhoenixTableProperty = true;
                    }
                }
                Boolean appendOnlySchema = null;
                if (appendOnlySchemaProp !=null) {
                    if (appendOnlySchemaProp != table.isAppendOnlySchema()) {
                        appendOnlySchema  = appendOnlySchemaProp;
                        changingPhoenixTableProperty = true;
                    }
                }
                ImmutableStorageScheme immutableStorageScheme = null;
                if (immutableStorageSchemeProp!=null) {
                    if (table.getImmutableStorageScheme() == ONE_CELL_PER_COLUMN || 
                            immutableStorageSchemeProp == ONE_CELL_PER_COLUMN) {
                        throw new SQLExceptionInfo.Builder(SQLExceptionCode.INVALID_IMMUTABLE_STORAGE_SCHEME_CHANGE)
                        .setSchemaName(schemaName).setTableName(tableName).build().buildException();
                    }
                    else if (immutableStorageSchemeProp != table.getImmutableStorageScheme()) {
                        immutableStorageScheme = immutableStorageSchemeProp;
                        changingPhoenixTableProperty = true;
                    }
                }
            
                if (guidePostWidth == null || guidePostWidth >= 0) {
                    changingPhoenixTableProperty = true;
                }
                Boolean storeNulls = null;
                if (storeNullsProp != null) {
                    if (storeNullsProp.booleanValue() != table.getStoreNulls()) {
                        storeNulls = storeNullsProp;
                        changingPhoenixTableProperty = true;
                    }
                }
                Boolean isTransactional = null;
                if (isTransactionalProp != null) {
                    if (isTransactionalProp.booleanValue() != table.isTransactional()) {
                        isTransactional = isTransactionalProp;
                        // We can only go one way: from non transactional to transactional
                        // Going the other way would require rewriting the cell timestamps
                        // and doing a major compaction to get rid of any Tephra specific
                        // delete markers.
                        if (!isTransactional) {
                            throw new SQLExceptionInfo.Builder(SQLExceptionCode.TX_MAY_NOT_SWITCH_TO_NON_TX)
                            .setSchemaName(schemaName).setTableName(tableName).build().buildException();
                        }
                        // cannot create a transactional table if transactions are disabled
                        boolean transactionsEnabled = connection.getQueryServices().getProps().getBoolean(
                                QueryServices.TRANSACTIONS_ENABLED,
                                QueryServicesOptions.DEFAULT_TRANSACTIONS_ENABLED);
                        if (!transactionsEnabled) {
                            throw new SQLExceptionInfo.Builder(SQLExceptionCode.CANNOT_ALTER_TO_BE_TXN_IF_TXNS_DISABLED)
                            .setSchemaName(schemaName).setTableName(tableName).build().buildException();
                        }
                        // cannot make a table transactional if it has a row timestamp column
                        if (SchemaUtil.hasRowTimestampColumn(table)) {
                            throw new SQLExceptionInfo.Builder(SQLExceptionCode.CANNOT_ALTER_TO_BE_TXN_WITH_ROW_TIMESTAMP)
                            .setSchemaName(schemaName).setTableName(tableName)
                            .build().buildException();
                        }
                        changingPhoenixTableProperty = true;
                        nonTxToTx = true;
                    }
                }
                Long timeStamp = TransactionUtil.getTableTimestamp(connection, table.isTransactional() || nonTxToTx);

                int numPkColumnsAdded = 0;
                List<PColumn> columns = Lists.newArrayListWithExpectedSize(numCols);
                Set<String> colFamiliesForPColumnsToBeAdded = new LinkedHashSet<>();
                Set<String> families = new LinkedHashSet<>();
                PTable tableForCQCounters = tableType == PTableType.VIEW ? PhoenixRuntime.getTable(connection, table.getPhysicalName().getString()) : table;;
                EncodedCQCounter cqCounterToUse = tableForCQCounters.getEncodedCQCounter();
                Map<String, Integer> changedCqCounters = new HashMap<>(numCols);
                if (numCols > 0 ) {
                    StatementContext context = new StatementContext(new PhoenixStatement(connection), resolver);
                    String addColumnSqlToUse = connection.isRunningUpgrade()
                            && tableName.equals(PhoenixDatabaseMetaData.SYSTEM_CATALOG_TABLE)
                            && schemaName.equals(PhoenixDatabaseMetaData.SYSTEM_CATALOG_SCHEMA) ? ALTER_SYSCATALOG_TABLE_UPGRADE
                            : INSERT_COLUMN_ALTER_TABLE;
                    try (PreparedStatement colUpsert = connection.prepareStatement(addColumnSqlToUse)) {
                        short nextKeySeq = SchemaUtil.getMaxKeySeq(table);
                        for( ColumnDef colDef : columnDefs) {
                            if (colDef != null && !colDef.isNull()) {
                                if(colDef.isPK()) {
                                    throw new SQLExceptionInfo.Builder(SQLExceptionCode.NOT_NULLABLE_COLUMN_IN_ROW_KEY)
                                    .setColumnName(colDef.getColumnDefName().getColumnName()).build().buildException();
                                } else {
                                    throw new SQLExceptionInfo.Builder(SQLExceptionCode.CANNOT_ADD_NOT_NULLABLE_COLUMN)
                                    .setColumnName(colDef.getColumnDefName().getColumnName()).build().buildException();
                                }
                            }
                            if (colDef != null && colDef.isPK() && table.getType() == VIEW && table.getViewType() != MAPPED) {
                                throwIfLastPKOfParentIsFixedLength(getParentOfView(table), schemaName, tableName, colDef);
                            }
                            if (colDef != null && colDef.isRowTimestamp()) {
                                throw new SQLExceptionInfo.Builder(SQLExceptionCode.ROWTIMESTAMP_CREATE_ONLY)
                                .setColumnName(colDef.getColumnDefName().getColumnName()).build().buildException();
                            }
                            if (!colDef.validateDefault(context, null)) {
                                colDef = new ColumnDef(colDef, null); // Remove DEFAULT as it's not necessary
                            }
                            Integer encodedCQ = null;
                            if (!colDef.isPK()) {
                                String colDefFamily = colDef.getColumnDefName().getFamilyName();
                                String familyName = null;
                                ImmutableStorageScheme storageScheme = table.getImmutableStorageScheme();
                                String defaultColumnFamily = tableForCQCounters.getDefaultFamilyName() != null && !Strings.isNullOrEmpty(tableForCQCounters.getDefaultFamilyName().getString()) ? 
                                        tableForCQCounters.getDefaultFamilyName().getString() : DEFAULT_COLUMN_FAMILY;
                                    if (table.getType() == PTableType.INDEX && table.getIndexType() == IndexType.LOCAL) {
                                        defaultColumnFamily = QueryConstants.LOCAL_INDEX_COLUMN_FAMILY_PREFIX + defaultColumnFamily;
                                    }
                                if (storageScheme == SINGLE_CELL_ARRAY_WITH_OFFSETS) {
                                    familyName = colDefFamily != null ? colDefFamily : defaultColumnFamily;
                                } else {
                                    familyName = defaultColumnFamily;
                                }
                                encodedCQ = cqCounterToUse.getNextQualifier(familyName);
                                if (cqCounterToUse.increment(familyName)) {
                                    changedCqCounters.put(familyName,
                                        cqCounterToUse.getNextQualifier(familyName));
                                }
                            }
                            byte[] columnQualifierBytes = null;
                            try {
                                columnQualifierBytes = EncodedColumnsUtil.getColumnQualifierBytes(colDef.getColumnDefName().getColumnName(), encodedCQ, table, colDef.isPK());
                            }
                            catch (QualifierOutOfRangeException e) {
                                throw new SQLExceptionInfo.Builder(SQLExceptionCode.MAX_COLUMNS_EXCEEDED)
                                .setSchemaName(schemaName)
                                .setTableName(tableName).build().buildException();
                            }
                            PColumn column = newColumn(position++, colDef, PrimaryKeyConstraint.EMPTY, table.getDefaultFamilyName() == null ? null : table.getDefaultFamilyName().getString(), true, columnQualifierBytes);
                            columns.add(column);
                            String pkName = null;
                            Short keySeq = null;
                            
                            // TODO: support setting properties on other families?
                            if (column.getFamilyName() == null) {
                                ++numPkColumnsAdded;
                                pkName = table.getPKName() == null ? null : table.getPKName().getString();
                                keySeq = ++nextKeySeq;
                            } else {
                                families.add(column.getFamilyName().getString());
                            }
                            colFamiliesForPColumnsToBeAdded.add(column.getFamilyName() == null ? null : column.getFamilyName().getString());
                            addColumnMutation(schemaName, tableName, column, colUpsert, null, pkName, keySeq, table.getBucketNum() != null);
                        }
                        
                        // Add any new PK columns to end of index PK
                        if (numPkColumnsAdded > 0) {
                            // create PK column list that includes the newly created columns
                            List<PColumn> pkColumns = Lists.newArrayListWithExpectedSize(table.getPKColumns().size()+numPkColumnsAdded);
                            pkColumns.addAll(table.getPKColumns());
                            for (int i=0; i<numCols; ++i) {
                                if (columnDefs.get(i).isPK()) {
                                    pkColumns.add(columns.get(i));
                                }
                            }
                            int pkSlotPosition = table.getPKColumns().size()-1;
                            for (PTable index : table.getIndexes()) {
                                short nextIndexKeySeq = SchemaUtil.getMaxKeySeq(index);
                                int indexPosition = index.getColumns().size();
                                for (int i=0; i<numCols; ++i) {
                                    ColumnDef colDef = columnDefs.get(i);
                                    if (colDef.isPK()) {
                                        PDataType indexColDataType = IndexUtil.getIndexColumnDataType(colDef.isNull(), colDef.getDataType());
                                        ColumnName indexColName = ColumnName.caseSensitiveColumnName(IndexUtil.getIndexColumnName(null, colDef.getColumnDefName().getColumnName()));
                                        Expression expression = new RowKeyColumnExpression(columns.get(i), new RowKeyValueAccessor(pkColumns, ++pkSlotPosition));
                                        ColumnDef indexColDef = FACTORY.columnDef(indexColName, indexColDataType.getSqlTypeName(), colDef.isNull(), colDef.getMaxLength(), colDef.getScale(), true, colDef.getSortOrder(), expression.toString(), colDef.isRowTimestamp());
                                        PColumn indexColumn = newColumn(indexPosition++, indexColDef, PrimaryKeyConstraint.EMPTY, null, true, null);
                                        addColumnMutation(schemaName, index.getTableName().getString(), indexColumn, colUpsert, index.getParentTableName().getString(), index.getPKName() == null ? null : index.getPKName().getString(), ++nextIndexKeySeq, index.getBucketNum() != null);
                                    }
                                }
                            }
                        }
                        columnMetaData.addAll(connection.getMutationState().toMutations(timeStamp).next().getSecond());
                        connection.rollback();
                    }
                } else {
                    // Check that HBase configured properly for mutable secondary indexing
                    // if we're changing from an immutable table to a mutable table and we
                    // have existing indexes.
                    if (Boolean.FALSE.equals(isImmutableRows) && !table.getIndexes().isEmpty()) {
                        int hbaseVersion = connection.getQueryServices().getLowestClusterHBaseVersion();
                        if (hbaseVersion < PhoenixDatabaseMetaData.MUTABLE_SI_VERSION_THRESHOLD) {
                            throw new SQLExceptionInfo.Builder(SQLExceptionCode.NO_MUTABLE_INDEXES)
                            .setSchemaName(schemaName).setTableName(tableName).build().buildException();
                        }
                        if (!connection.getQueryServices().hasIndexWALCodec() && !table.isTransactional()) {
                            throw new SQLExceptionInfo.Builder(SQLExceptionCode.INVALID_MUTABLE_INDEX_CONFIG)
                            .setSchemaName(schemaName).setTableName(tableName).build().buildException();
                        }
                    }
                    if (Boolean.TRUE.equals(multiTenant)) {
                        throwIfInsufficientColumns(schemaName, tableName, table.getPKColumns(), table.getBucketNum()!=null, multiTenant);
                    }
                }

                if (!table.getIndexes().isEmpty() && (numPkColumnsAdded>0 || nonTxToTx)) {
                    for (PTable index : table.getIndexes()) {
                        incrementTableSeqNum(index, index.getType(), numPkColumnsAdded, nonTxToTx ? Boolean.TRUE : null, updateCacheFrequency);
                    }
                    tableMetaData.addAll(connection.getMutationState().toMutations(timeStamp).next().getSecond());
                    connection.rollback();
                }
                
                if (changingPhoenixTableProperty || columnDefs.size() > 0) {
                    incrementTableSeqNum(table, tableType, columnDefs.size(), isTransactional, updateCacheFrequency, isImmutableRows,
                            disableWAL, multiTenant, storeNulls, guidePostWidth, appendOnlySchema, immutableStorageScheme);
                    tableMetaData.addAll(connection.getMutationState().toMutations(timeStamp).next().getSecond());
                    connection.rollback();
                }

                // Force the table header row to be first
                Collections.reverse(tableMetaData);
                // Add column metadata afterwards, maintaining the order so columns have more predictable ordinal position
                tableMetaData.addAll(columnMetaData);
                boolean sharedIndex = tableType == PTableType.INDEX && (table.getIndexType() == IndexType.LOCAL || table.getViewIndexId() != null);
                String tenantIdToUse = connection.getTenantId() != null && sharedIndex ? connection.getTenantId().getString() : null;
                if (!changedCqCounters.isEmpty()) {
                    PreparedStatement linkStatement;
                        linkStatement = connection.prepareStatement(UPDATE_ENCODED_COLUMN_COUNTER);
                        for (Entry<String, Integer> entry : changedCqCounters.entrySet()) {    
                            linkStatement.setString(1, tenantIdToUse);
                            linkStatement.setString(2, tableForCQCounters.getSchemaName().getString());
                            linkStatement.setString(3, tableForCQCounters.getTableName().getString());
                            linkStatement.setString(4, entry.getKey());
                            linkStatement.setInt(5, entry.getValue());
                            linkStatement.execute();
                        }

                    // When a view adds its own columns, then we need to increase the sequence number of the base table
                    // too since we want clients to get the latest PTable of the base table.
                    if (tableType == VIEW) {
                        PreparedStatement incrementStatement = connection.prepareStatement(INCREMENT_SEQ_NUM);
                        incrementStatement.setString(1, null);
                        incrementStatement.setString(2, tableForCQCounters.getSchemaName().getString());
                        incrementStatement.setString(3, tableForCQCounters.getTableName().getString());
                        incrementStatement.setLong(4, tableForCQCounters.getSequenceNumber() + 1);
                        incrementStatement.execute();
                    }
                    tableMetaData.addAll(connection.getMutationState().toMutations(timeStamp).next().getSecond());
                    connection.rollback();
                }

                byte[] family = families.size() > 0 ? families.iterator().next().getBytes() : null;

                // Figure out if the empty column family is changing as a result of adding the new column
                byte[] emptyCF = null;
                byte[] projectCF = null;
                if (table.getType() != PTableType.VIEW && family != null) {
                    if (table.getColumnFamilies().isEmpty()) {
                        emptyCF = family;
                    } else {
                        try {
                            table.getColumnFamily(family);
                        } catch (ColumnFamilyNotFoundException e) {
                            projectCF = family;
                            emptyCF = SchemaUtil.getEmptyColumnFamily(table);
                        }
                    }
                }

                MetaDataMutationResult result = connection.getQueryServices().addColumn(tableMetaData, table, properties, colFamiliesForPColumnsToBeAdded, columns);
                try {
                    MutationCode code = processMutationResult(schemaName, tableName, result);
                    if (code == MutationCode.COLUMN_ALREADY_EXISTS) {
                        addTableToCache(result);
                        if (!ifNotExists) {
                            throw new ColumnAlreadyExistsException(schemaName, tableName, SchemaUtil.findExistingColumn(result.getTable(), columns));
                        }
                        return new MutationState(0,connection);
                    }
                    // Only update client side cache if we aren't adding a PK column to a table with indexes or
                    // transitioning a table from non transactional to transactional.
                    // We could update the cache manually then too, it'd just be a pain.
                    String fullTableName = SchemaUtil.getTableName(schemaName, tableName);
                    long resolvedTimeStamp = TransactionUtil.getResolvedTime(connection, result);
                    if (table.getIndexes().isEmpty() || (numPkColumnsAdded==0 && !nonTxToTx)) {
                        connection.addTable(result.getTable(), resolvedTimeStamp);
                        table = result.getTable();
                    } else if (updateCacheFrequency != null) {
                        // Force removal from cache as the update cache frequency has changed
                        // Note that clients outside this JVM won't be affected.
                        connection.removeTable(tenantId, fullTableName, null, resolvedTimeStamp);
                    }
                    // Delete rows in view index if we haven't dropped it already
                    // We only need to do this if the multiTenant transitioned to false
                    if (table.getType() == PTableType.TABLE
                            && Boolean.FALSE.equals(multiTenant)
                            && MetaDataUtil.hasViewIndexTable(connection, table.getPhysicalName())) {
                        connection.setAutoCommit(true);
                        MetaDataUtil.deleteViewIndexSequences(connection, table.getPhysicalName(), table.isNamespaceMapped());
                        // If we're not dropping metadata, then make sure no rows are left in
                        // our view index physical table.
                        // TODO: remove this, as the DROP INDEX commands run when the DROP VIEW
                        // commands are run would remove all rows already.
                        if (!connection.getQueryServices().getProps().getBoolean(DROP_METADATA_ATTRIB, DEFAULT_DROP_METADATA)) {
                            Long scn = connection.getSCN();
                            long ts = (scn == null ? result.getMutationTime() : scn);
                            byte[] viewIndexPhysicalName = MetaDataUtil
                                    .getViewIndexPhysicalName(table.getPhysicalName().getBytes());
                            PTable viewIndexTable = new PTableImpl(null,
                                    SchemaUtil.getSchemaNameFromFullName(viewIndexPhysicalName),
                                    SchemaUtil.getTableNameFromFullName(viewIndexPhysicalName), ts,
                                    table.getColumnFamilies(), table.isNamespaceMapped(), table.getImmutableStorageScheme(), table.getEncodingScheme());
                            List<TableRef> tableRefs = Collections.singletonList(new TableRef(null, viewIndexTable, ts, false));
                            MutationPlan plan = new PostDDLCompiler(connection).compile(tableRefs, null, null,
                                    Collections.<PColumn> emptyList(), ts);
                            connection.getQueryServices().updateData(plan);
                        }
                    }
                    if (emptyCF != null) {
                        Long scn = connection.getSCN();
                        connection.setAutoCommit(true);
                        // Delete everything in the column. You'll still be able to do queries at earlier timestamps
                        long ts = (scn == null ? result.getMutationTime() : scn);
                        MutationPlan plan = new PostDDLCompiler(connection).compile(Collections.singletonList(new TableRef(null, table, ts, false)), emptyCF, projectCF == null ? null : Collections.singletonList(projectCF), null, ts);
                        return connection.getQueryServices().updateData(plan);
                    }
                    return new MutationState(0,connection);
                } catch (ConcurrentTableMutationException e) {
                    if (retried) {
                        throw e;
                    }
                    if (logger.isDebugEnabled()) {
                        logger.debug(LogUtil.addCustomAnnotations("Caught ConcurrentTableMutationException for table " + SchemaUtil.getTableName(schemaName, tableName) + ". Will try again...", connection));
                    }
                    retried = true;
                }
            }
        } finally {
            connection.setAutoCommit(wasAutoCommit);
        }
    }

    private String dropColumnMutations(PTable table, List<PColumn> columnsToDrop) throws SQLException {
        String tenantId = connection.getTenantId() == null ? "" : connection.getTenantId().getString();
        String schemaName = table.getSchemaName().getString();
        String tableName = table.getTableName().getString();
        String familyName = null;
        /*
         * Generate a fully qualified RVC with an IN clause, since that's what our optimizer can
         * handle currently. If/when the optimizer handles (A and ((B AND C) OR (D AND E))) we
         * can factor out the tenant ID, schema name, and table name columns
         */
        StringBuilder buf = new StringBuilder("DELETE FROM " + SYSTEM_CATALOG_SCHEMA + ".\"" + SYSTEM_CATALOG_TABLE + "\" WHERE ");
        buf.append("(" +
                TENANT_ID + "," + TABLE_SCHEM + "," + TABLE_NAME + "," +
                COLUMN_NAME + ", " + COLUMN_FAMILY + ") IN (");
        for(PColumn columnToDrop : columnsToDrop) {
            buf.append("('" + tenantId + "'");
            buf.append(",'" + schemaName + "'");
            buf.append(",'" + tableName + "'");
            buf.append(",'" + columnToDrop.getName().getString() + "'");
            buf.append(",'" + (columnToDrop.getFamilyName() == null ? "" : columnToDrop.getFamilyName().getString()) + "'),");
        }
        buf.setCharAt(buf.length()-1, ')');

        connection.createStatement().execute(buf.toString());

        Collections.sort(columnsToDrop,new Comparator<PColumn> () {
            @Override
            public int compare(PColumn left, PColumn right) {
                return Ints.compare(left.getPosition(), right.getPosition());
            }
        });

        boolean isSalted = table.getBucketNum() != null;
        int columnsToDropIndex = 0;
        PreparedStatement colUpdate = connection.prepareStatement(UPDATE_COLUMN_POSITION);
        colUpdate.setString(1, tenantId);
        colUpdate.setString(2, schemaName);
        colUpdate.setString(3, tableName);
        for (int i = columnsToDrop.get(columnsToDropIndex).getPosition() + 1; i < table.getColumns().size(); i++) {
            PColumn column = table.getColumns().get(i);
            if(columnsToDrop.contains(column)) {
                columnsToDropIndex++;
                continue;
            }
            colUpdate.setString(4, column.getName().getString());
            colUpdate.setString(5, column.getFamilyName() == null ? null : column.getFamilyName().getString());
            // Adjust position to not include the salt column
            colUpdate.setInt(6, column.getPosition() - columnsToDropIndex - (isSalted ? 1 : 0));
            colUpdate.execute();
        }
        return familyName;
    }

    /**
     * Calculate what the new column family will be after the column is dropped, returning null
     * if unchanged.
     * @param table table containing column to drop
     * @param columnToDrop column being dropped
     * @return the new column family or null if unchanged.
     */
    private static byte[] getNewEmptyColumnFamilyOrNull (PTable table, PColumn columnToDrop) {
        if (table.getType() != PTableType.VIEW && !SchemaUtil.isPKColumn(columnToDrop) && table.getColumnFamilies().get(0).getName().equals(columnToDrop.getFamilyName()) && table.getColumnFamilies().get(0).getColumns().size() == 1) {
            return SchemaUtil.getEmptyColumnFamily(table.getDefaultFamilyName(), table.getColumnFamilies().subList(1, table.getColumnFamilies().size()));
        }
        // If unchanged, return null
        return null;
    }

    public MutationState dropColumn(DropColumnStatement statement) throws SQLException {
        connection.rollback();
        boolean wasAutoCommit = connection.getAutoCommit();
        try {
            connection.setAutoCommit(false);
            PName tenantId = connection.getTenantId();
            TableName tableNameNode = statement.getTable().getName();
            String schemaName = tableNameNode.getSchemaName();
            String tableName = tableNameNode.getTableName();
            String fullTableName = SchemaUtil.getTableName(schemaName, tableName);
            boolean retried = false;
            while (true) {
                final ColumnResolver resolver = FromCompiler.getResolver(statement, connection);
                TableRef tableRef = resolver.getTables().get(0);
                PTable table = tableRef.getTable();

                List<ColumnName> columnRefs = statement.getColumnRefs();
                if(columnRefs == null) {
                    columnRefs = Lists.newArrayListWithCapacity(0);
                }
                List<ColumnRef> columnsToDrop = Lists.newArrayListWithExpectedSize(columnRefs.size() + table.getIndexes().size());
                List<TableRef> indexesToDrop = Lists.newArrayListWithExpectedSize(table.getIndexes().size());
                List<Mutation> tableMetaData = Lists.newArrayListWithExpectedSize((table.getIndexes().size() + 1) * (1 + table.getColumns().size() - columnRefs.size()));
                List<PColumn>  tableColumnsToDrop = Lists.newArrayListWithExpectedSize(columnRefs.size());

                for(ColumnName column : columnRefs) {
                    ColumnRef columnRef = null;
                    try {
                        columnRef = resolver.resolveColumn(null, column.getFamilyName(), column.getColumnName());
                    } catch (ColumnNotFoundException e) {
                        if (statement.ifExists()) {
                            return new MutationState(0,connection);
                        }
                        throw e;
                    }
                    PColumn columnToDrop = columnRef.getColumn();
                    tableColumnsToDrop.add(columnToDrop);
                    if (SchemaUtil.isPKColumn(columnToDrop)) {
                        throw new SQLExceptionInfo.Builder(SQLExceptionCode.CANNOT_DROP_PK)
                        .setColumnName(columnToDrop.getName().getString()).build().buildException();
                    }
                    else if (table.isAppendOnlySchema()) {
                        throw new SQLExceptionInfo.Builder(SQLExceptionCode.CANNOT_DROP_COL_APPEND_ONLY_SCHEMA)
                        .setColumnName(columnToDrop.getName().getString()).build().buildException();
                    }
                    columnsToDrop.add(new ColumnRef(columnRef.getTableRef(), columnToDrop.getPosition()));
                }

                dropColumnMutations(table, tableColumnsToDrop);
                boolean removedIndexTableOrColumn=false;
                Long timeStamp = table.isTransactional() ? tableRef.getTimeStamp() : null;
                for (PTable index : table.getIndexes()) {
                    IndexMaintainer indexMaintainer = index.getIndexMaintainer(table, connection);
                    // get the covered columns 
                    List<PColumn> indexColumnsToDrop = Lists.newArrayListWithExpectedSize(columnRefs.size());
                    Set<Pair<String, String>> indexedColsInfo = indexMaintainer.getIndexedColumnInfo();
                    Set<ColumnReference> coveredCols = indexMaintainer.getCoveredColumns();
                    for(PColumn columnToDrop : tableColumnsToDrop) {
                        Pair<String, String> columnToDropInfo = new Pair<>(columnToDrop.getFamilyName().getString(), columnToDrop.getName().getString());
                        ColumnReference colDropRef = new ColumnReference(columnToDrop.getFamilyName() == null ? null
                                : columnToDrop.getFamilyName().getBytes(), columnToDrop.getColumnQualifierBytes());
                        boolean isColumnIndexed = indexedColsInfo.contains(columnToDropInfo);
                        if (isColumnIndexed) {
                            if (index.getViewIndexId() == null) { 
                                indexesToDrop.add(new TableRef(index));
                            }
                            connection.removeTable(tenantId, SchemaUtil.getTableName(schemaName, index.getName().getString()), index.getParentName() == null ? null : index.getParentName().getString(), index.getTimeStamp());
                            removedIndexTableOrColumn = true;
                        } else if (coveredCols.contains(colDropRef)) {
                            String indexColumnName = IndexUtil.getIndexColumnName(columnToDrop);
                            PColumn indexColumn = index.getColumnForColumnName(indexColumnName);
                            indexColumnsToDrop.add(indexColumn);
                            // add the index column to be dropped so that we actually delete the column values
                            columnsToDrop.add(new ColumnRef(new TableRef(index), indexColumn.getPosition()));
                            removedIndexTableOrColumn = true;
                        }
                    }
                    if(!indexColumnsToDrop.isEmpty()) {
                        long indexTableSeqNum = incrementTableSeqNum(index, index.getType(), -indexColumnsToDrop.size(), null, null);
                        dropColumnMutations(index, indexColumnsToDrop);
                        long clientTimestamp = MutationState.getMutationTimestamp(timeStamp, connection.getSCN());
                        connection.removeColumn(tenantId, index.getName().getString(),
                                indexColumnsToDrop, clientTimestamp, indexTableSeqNum,
                                TransactionUtil.getResolvedTimestamp(connection, index.isTransactional(), clientTimestamp));
                    }
                }
                tableMetaData.addAll(connection.getMutationState().toMutations(timeStamp).next().getSecond());
                connection.rollback();

                long seqNum = incrementTableSeqNum(table, statement.getTableType(), -tableColumnsToDrop.size(), null, null);
                tableMetaData.addAll(connection.getMutationState().toMutations(timeStamp).next().getSecond());
                connection.rollback();
                // Force table header to be first in list
                Collections.reverse(tableMetaData);

                /*
                 * Ensure our "empty column family to be" exists. Somewhat of an edge case, but can occur if we drop the last column
                 * in a column family that was the empty column family. In that case, we have to pick another one. If there are no other
                 * ones, then we need to create our default empty column family. Note that this may no longer be necessary once we
                 * support declaring what the empty column family is on a table, as:
                 * - If you declare it, we'd just ensure it's created at DDL time and never switch what it is unless you change it
                 * - If you don't declare it, we can just continue to use the old empty column family in this case, dynamically updating
                 *    the empty column family name on the PTable.
                 */
                for (ColumnRef columnRefToDrop : columnsToDrop) {
                    PTable tableContainingColumnToDrop = columnRefToDrop.getTable();
                    byte[] emptyCF = getNewEmptyColumnFamilyOrNull(tableContainingColumnToDrop, columnRefToDrop.getColumn());
                    if (emptyCF != null) {
                        try {
                            tableContainingColumnToDrop.getColumnFamily(emptyCF);
                        } catch (ColumnFamilyNotFoundException e) {
                            // Only if it's not already a column family do we need to ensure it's created
                            Map<String, List<Pair<String,Object>>> family = new HashMap<>(1);
                            family.put(Bytes.toString(emptyCF), Collections.<Pair<String, Object>>emptyList());
                            // Just use a Put without any key values as the Mutation, as addColumn will treat this specially
                            // TODO: pass through schema name and table name instead to these methods as it's cleaner
                            byte[] tenantIdBytes = connection.getTenantId() == null ? null : connection.getTenantId().getBytes();
                            if (tenantIdBytes == null) tenantIdBytes = ByteUtil.EMPTY_BYTE_ARRAY;
                            connection.getQueryServices().addColumn(
                                    Collections.<Mutation>singletonList(new Put(SchemaUtil.getTableKey
                                            (tenantIdBytes, tableContainingColumnToDrop.getSchemaName().getBytes(),
                                                    tableContainingColumnToDrop.getTableName().getBytes()))),
                                                    tableContainingColumnToDrop, family, Sets.newHashSet(Bytes.toString(emptyCF)), Collections.<PColumn>emptyList());

                        }
                    }
                }
                MetaDataMutationResult result = connection.getQueryServices().dropColumn(tableMetaData, statement.getTableType());
                try {
                    MutationCode code = processMutationResult(schemaName, tableName, result);
                    if (code == MutationCode.COLUMN_NOT_FOUND) {
                        addTableToCache(result);
                        if (!statement.ifExists()) {
                            throw new ColumnNotFoundException(schemaName, tableName, Bytes.toString(result.getFamilyName()), Bytes.toString(result.getColumnName()));
                        }
                        return new MutationState(0, connection);
                    }
                    // If we've done any index metadata updates, don't bother trying to update
                    // client-side cache as it would be too painful. Just let it pull it over from
                    // the server when needed.
                    if (tableColumnsToDrop.size() > 0) {
                        if (removedIndexTableOrColumn)
                            connection.removeTable(tenantId, tableName, table.getParentName() == null ? null : table.getParentName().getString(), table.getTimeStamp());
                        else
                            connection.removeColumn(tenantId, SchemaUtil.getTableName(schemaName, tableName) , tableColumnsToDrop, result.getMutationTime(), seqNum, TransactionUtil.getResolvedTime(connection, result));
                    }
                    // If we have a VIEW, then only delete the metadata, and leave the table data alone
                    if (table.getType() != PTableType.VIEW) {
                        MutationState state = null;
                        connection.setAutoCommit(true);
                        Long scn = connection.getSCN();
                        // Delete everything in the column. You'll still be able to do queries at earlier timestamps
                        long ts = (scn == null ? result.getMutationTime() : scn);
                        PostDDLCompiler compiler = new PostDDLCompiler(connection);

                        boolean dropMetaData = connection.getQueryServices().getProps().getBoolean(DROP_METADATA_ATTRIB, DEFAULT_DROP_METADATA);
                        // if the index is a local index or view index it uses a shared physical table
                        // so we need to issue deletes markers for all the rows of the index
                        final List<TableRef> tableRefsToDrop = Lists.newArrayList();
                        Map<String, List<TableRef>> tenantIdTableRefMap = Maps.newHashMap();
                        if (result.getSharedTablesToDelete() != null) {
                            for (SharedTableState sharedTableState : result.getSharedTablesToDelete()) {
                                PTableImpl viewIndexTable = new PTableImpl(sharedTableState.getTenantId(),
                                        sharedTableState.getSchemaName(), sharedTableState.getTableName(), ts,
                                        table.getColumnFamilies(), sharedTableState.getColumns(),
                                        sharedTableState.getPhysicalNames(), sharedTableState.getViewIndexId(),
                                        table.isMultiTenant(), table.isNamespaceMapped(), table.getImmutableStorageScheme(), table.getEncodingScheme(), table.getEncodedCQCounter());
                                TableRef indexTableRef = new TableRef(viewIndexTable);
                                PName indexTableTenantId = sharedTableState.getTenantId();
                                if (indexTableTenantId==null) {
                                    tableRefsToDrop.add(indexTableRef);
                                }
                                else {
                                    if (!tenantIdTableRefMap.containsKey(indexTableTenantId)) {
                                        tenantIdTableRefMap.put(indexTableTenantId.getString(), Lists.<TableRef>newArrayList());
                                    }
                                    tenantIdTableRefMap.get(indexTableTenantId.getString()).add(indexTableRef);
                                }

                            }
                        }
                        // if dropMetaData is false delete all rows for the indexes (if it was true
                        // they would have been dropped in ConnectionQueryServices.dropColumn)
                        if (!dropMetaData) {
                            tableRefsToDrop.addAll(indexesToDrop);
                        }
                        // Drop any index tables that had the dropped column in the PK
                        state = connection.getQueryServices().updateData(compiler.compile(tableRefsToDrop, null, null, Collections.<PColumn>emptyList(), ts));

                        // Drop any tenant-specific indexes
                        if (!tenantIdTableRefMap.isEmpty()) {
                            for (Entry<String, List<TableRef>> entry : tenantIdTableRefMap.entrySet()) {
                                String indexTenantId = entry.getKey();
                                Properties props = new Properties(connection.getClientInfo());
                                props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, indexTenantId);
                                try (PhoenixConnection tenantConn = new PhoenixConnection(connection, connection.getQueryServices(), props)) {
                                    PostDDLCompiler dropCompiler = new PostDDLCompiler(tenantConn);
                                    state = tenantConn.getQueryServices().updateData(dropCompiler.compile(entry.getValue(), null, null, Collections.<PColumn>emptyList(), ts));
                                }
                            }
                        }

                        // TODO For immutable tables, if the storage scheme is not ONE_CELL_PER_COLUMN we will remove the column values at compaction time
                        // See https://issues.apache.org/jira/browse/PHOENIX-3605
                        if (!table.isImmutableRows() || table.getImmutableStorageScheme()==ImmutableStorageScheme.ONE_CELL_PER_COLUMN) {
                            // Update empty key value column if necessary
                            for (ColumnRef droppedColumnRef : columnsToDrop) {
                                // Painful, but we need a TableRef with a pre-set timestamp to prevent attempts
                                // to get any updates from the region server.
                                // TODO: move this into PostDDLCompiler
                                // TODO: consider filtering mutable indexes here, but then the issue is that
                                // we'd need to force an update of the data row empty key value if a mutable
                                // secondary index is changing its empty key value family.
                                droppedColumnRef = droppedColumnRef.cloneAtTimestamp(ts);
                                TableRef droppedColumnTableRef = droppedColumnRef.getTableRef();
                                PColumn droppedColumn = droppedColumnRef.getColumn();
                                MutationPlan plan = compiler.compile(
                                        Collections.singletonList(droppedColumnTableRef),
                                        getNewEmptyColumnFamilyOrNull(droppedColumnTableRef.getTable(), droppedColumn),
                                        null,
                                        Collections.singletonList(droppedColumn),
                                        ts);
                                state = connection.getQueryServices().updateData(plan);
                            }
                        }
                        // Return the last MutationState
                        return state;
                    }
                    return new MutationState(0, connection);
                } catch (ConcurrentTableMutationException e) {
                    if (retried) {
                        throw e;
                    }
                    table = connection.getTable(new PTableKey(tenantId, fullTableName));
                    retried = true;
                }
            }
        } finally {
            connection.setAutoCommit(wasAutoCommit);
        }
    }

    public MutationState alterIndex(AlterIndexStatement statement) throws SQLException {
        connection.rollback();
        boolean wasAutoCommit = connection.getAutoCommit();
        try {
            String dataTableName = statement.getTableName();
            String schemaName = statement.getTable().getName().getSchemaName();
            String indexName = statement.getTable().getName().getTableName();
            boolean isAsync = statement.isAsync();
            PIndexState newIndexState = statement.getIndexState();
            if (isAsync && newIndexState != PIndexState.REBUILD) { throw new SQLExceptionInfo.Builder(
                    SQLExceptionCode.ASYNC_NOT_ALLOWED)
                            .setMessage(" ASYNC building of index is allowed only with REBUILD index state")
                            .setSchemaName(schemaName).setTableName(indexName).build().buildException(); }

            if (newIndexState == PIndexState.REBUILD) {
                newIndexState = PIndexState.BUILDING;
            }
            connection.setAutoCommit(false);
            // Confirm index table is valid and up-to-date
            TableRef indexRef = FromCompiler.getResolver(statement, connection).getTables().get(0);
            PreparedStatement tableUpsert = null;
            try {
                if(newIndexState == PIndexState.ACTIVE){
                    tableUpsert = connection.prepareStatement(UPDATE_INDEX_STATE_TO_ACTIVE);
                }else{
                    tableUpsert = connection.prepareStatement(UPDATE_INDEX_STATE);
                }
                tableUpsert.setString(1, connection.getTenantId() == null ? null : connection.getTenantId().getString());
                tableUpsert.setString(2, schemaName);
                tableUpsert.setString(3, indexName);
                tableUpsert.setString(4, newIndexState.getSerializedValue());
                tableUpsert.setLong(5, 0);
                if(newIndexState == PIndexState.ACTIVE){
                    tableUpsert.setLong(6, 0);
                }
                tableUpsert.execute();
            } finally {
                if(tableUpsert != null) {
                    tableUpsert.close();
                }
            }
            Long timeStamp = indexRef.getTable().isTransactional() ? indexRef.getTimeStamp() : null;
            List<Mutation> tableMetadata = connection.getMutationState().toMutations(timeStamp).next().getSecond();
            connection.rollback();

            MetaDataMutationResult result = connection.getQueryServices().updateIndexState(tableMetadata, dataTableName);
            MutationCode code = result.getMutationCode();
            if (code == MutationCode.TABLE_NOT_FOUND) {
                throw new TableNotFoundException(schemaName,indexName);
            }
            if (code == MutationCode.UNALLOWED_TABLE_MUTATION) {
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.INVALID_INDEX_STATE_TRANSITION)
                .setMessage(" currentState=" + indexRef.getTable().getIndexState() + ". requestedState=" + newIndexState )
                .setSchemaName(schemaName).setTableName(indexName).build().buildException();
            }
            if (code == MutationCode.TABLE_ALREADY_EXISTS) {
                if (result.getTable() != null) { // To accommodate connection-less update of index state
                    addTableToCache(result);
                    // Set so that we get the table below with the potentially modified rowKeyOrderOptimizable flag set
                    indexRef.setTable(result.getTable());
                    if (newIndexState == PIndexState.BUILDING && isAsync) {
                        try {
                            tableUpsert = connection.prepareStatement(UPDATE_INDEX_REBUILD_ASYNC_STATE);
                            tableUpsert.setString(1,
                                    connection.getTenantId() == null ? null : connection.getTenantId().getString());
                            tableUpsert.setString(2, schemaName);
                            tableUpsert.setString(3, indexName);
                            tableUpsert.setLong(4, result.getTable().getTimeStamp());
                            tableUpsert.execute();
                            connection.commit();
                        } finally {
                            if (tableUpsert != null) {
                                tableUpsert.close();
                            }
                        }
                    }
                }
            }
            if (newIndexState == PIndexState.BUILDING && !isAsync) {
                PTable index = indexRef.getTable();
                // First delete any existing rows of the index
                Long scn = connection.getSCN();
                long ts = scn == null ? HConstants.LATEST_TIMESTAMP : scn;
                MutationPlan plan = new PostDDLCompiler(connection).compile(Collections.singletonList(indexRef), null, null, Collections.<PColumn>emptyList(), ts);
                connection.getQueryServices().updateData(plan);
                NamedTableNode dataTableNode = NamedTableNode.create(null, TableName.create(schemaName, dataTableName), Collections.<ColumnDef>emptyList());
                // Next rebuild the index
                connection.setAutoCommit(true);
                if (connection.getSCN() != null) {
                    return buildIndexAtTimeStamp(index, dataTableNode);
                }
                TableRef dataTableRef = FromCompiler.getResolver(dataTableNode, connection).getTables().get(0);
                return buildIndex(index, dataTableRef);
            }
            return new MutationState(1, connection);
        } catch (TableNotFoundException e) {
            if (!statement.ifExists()) {
                throw e;
            }
            return new MutationState(0, connection);
        } finally {
            connection.setAutoCommit(wasAutoCommit);
        }
    }

    private PTable addTableToCache(MetaDataMutationResult result) throws SQLException {
        addIndexesFromParentTable(result, null);
        PTable table = result.getTable();
        connection.addTable(table, TransactionUtil.getResolvedTime(connection, result));
        return table;
    }

    private List<PFunction> addFunctionToCache(MetaDataMutationResult result) throws SQLException {
        for(PFunction function: result.getFunctions()) {
            connection.addFunction(function);
        }
        return result.getFunctions();
    }

    private void addSchemaToCache(MetaDataMutationResult result) throws SQLException {
        connection.addSchema(result.getSchema());
    }

    private void throwIfLastPKOfParentIsFixedLength(PTable parent, String viewSchemaName, String viewName, ColumnDef col) throws SQLException {
        if (isLastPKVariableLength(parent)) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.CANNOT_MODIFY_VIEW_PK)
            .setSchemaName(viewSchemaName)
            .setTableName(viewName)
            .setColumnName(col.getColumnDefName().getColumnName())
            .build().buildException(); }
    }

    private boolean isLastPKVariableLength(PTable table) {
        List<PColumn> pkColumns = table.getPKColumns();
        return !pkColumns.get(pkColumns.size()-1).getDataType().isFixedWidth();
    }

    private PTable getParentOfView(PTable view) throws SQLException {
        //TODO just use view.getParentName().getString() after implementing https://issues.apache.org/jira/browse/PHOENIX-2114
        SelectStatement select = new SQLParser(view.getViewStatement()).parseQuery();
        String parentName = SchemaUtil.normalizeFullTableName(select.getFrom().toString().trim());
        return connection.getTable(new PTableKey(view.getTenantId(), parentName));
    }

    public MutationState createSchema(CreateSchemaStatement create) throws SQLException {
        boolean wasAutoCommit = connection.getAutoCommit();
        connection.rollback();
        try {
            if (!SchemaUtil.isNamespaceMappingEnabled(null,
                    connection.getQueryServices()
                    .getProps())) { throw new SQLExceptionInfo.Builder(
                            SQLExceptionCode.CREATE_SCHEMA_NOT_ALLOWED).setSchemaName(create.getSchemaName())
                            .build().buildException(); }
            boolean isIfNotExists = create.isIfNotExists();
            validateSchema(create.getSchemaName());
            PSchema schema = new PSchema(create.getSchemaName());
            connection.setAutoCommit(false);
            List<Mutation> schemaMutations;

            try (PreparedStatement schemaUpsert = connection.prepareStatement(CREATE_SCHEMA)) {
                schemaUpsert.setString(1, schema.getSchemaName());
                schemaUpsert.setString(2, MetaDataClient.EMPTY_TABLE);
                schemaUpsert.execute();
                schemaMutations = connection.getMutationState().toMutations(null).next().getSecond();
                connection.rollback();
            }
            MetaDataMutationResult result = connection.getQueryServices().createSchema(schemaMutations,
                    schema.getSchemaName());
            MutationCode code = result.getMutationCode();
            switch (code) {
            case SCHEMA_ALREADY_EXISTS:
                if (result.getSchema() != null) {
                    addSchemaToCache(result);
                }
                if (!isIfNotExists) { throw new SchemaAlreadyExistsException(schema.getSchemaName()); }
                break;
            case NEWER_SCHEMA_FOUND:
                throw new NewerSchemaAlreadyExistsException(schema.getSchemaName());
            default:
                result = new MetaDataMutationResult(code, schema, result.getMutationTime());
                addSchemaToCache(result);
            }
        } finally {
            connection.setAutoCommit(wasAutoCommit);
        }
        return new MutationState(0, connection);
    }

    private void validateSchema(String schemaName) throws SQLException {
        if (SchemaUtil.NOT_ALLOWED_SCHEMA_LIST.contains(
                schemaName.toUpperCase())) { throw new SQLExceptionInfo.Builder(SQLExceptionCode.SCHEMA_NOT_ALLOWED)
                .setSchemaName(schemaName).build().buildException(); }
    }

    public MutationState dropSchema(DropSchemaStatement executableDropSchemaStatement) throws SQLException {
        connection.rollback();
        boolean wasAutoCommit = connection.getAutoCommit();
        try {
            PSchema schema = new PSchema(executableDropSchemaStatement.getSchemaName());
            String schemaName = schema.getSchemaName();
            boolean ifExists = executableDropSchemaStatement.ifExists();
            byte[] key = SchemaUtil.getSchemaKey(schemaName);

            Long scn = connection.getSCN();
            long clientTimeStamp = scn == null ? HConstants.LATEST_TIMESTAMP : scn;
            List<Mutation> schemaMetaData = Lists.newArrayListWithExpectedSize(2);
            Delete schemaDelete = new Delete(key, clientTimeStamp);
            schemaMetaData.add(schemaDelete);
            MetaDataMutationResult result = connection.getQueryServices().dropSchema(schemaMetaData, schemaName);
            MutationCode code = result.getMutationCode();
            schema = result.getSchema();
            switch (code) {
            case SCHEMA_NOT_FOUND:
                if (!ifExists) { throw new SchemaNotFoundException(schemaName); }
                break;
            case NEWER_SCHEMA_FOUND:
                throw new NewerSchemaAlreadyExistsException(schemaName);
            case TABLES_EXIST_ON_SCHEMA:
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.CANNOT_MUTATE_SCHEMA).setSchemaName(schemaName)
                .build().buildException();
            default:
                connection.removeSchema(schema, result.getMutationTime());
                break;
            }
            return new MutationState(0, connection);
        } finally {
            connection.setAutoCommit(wasAutoCommit);
        }
    }

    public MutationState useSchema(UseSchemaStatement useSchemaStatement) throws SQLException {
        // As we allow default namespace mapped to empty schema, so this is to reset schema in connection
        if (useSchemaStatement.getSchemaName().equals(StringUtil.EMPTY_STRING)) {
            connection.setSchema(null);
        } else {
            FromCompiler.getResolverForSchema(useSchemaStatement, connection)
            .resolveSchema(useSchemaStatement.getSchemaName());
            connection.setSchema(useSchemaStatement.getSchemaName());
        }
        return new MutationState(0, connection);
    }
}
