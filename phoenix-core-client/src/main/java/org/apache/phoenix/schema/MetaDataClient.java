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

import static org.apache.phoenix.exception.SQLExceptionCode.CANNOT_TRANSFORM_TRANSACTIONAL_TABLE;
import static org.apache.phoenix.exception.SQLExceptionCode.ERROR_WRITING_TO_SCHEMA_REGISTRY;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.APPEND_ONLY_SCHEMA;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.ARG_POSITION;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.ARRAY_SIZE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.ASYNC_CREATED_DATE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.ASYNC_REBUILD_TIMESTAMP;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.AUTO_PARTITION_SEQ;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.BASE_COLUMN_COUNT;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.CHANGE_DETECTION_ENABLED;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.CLASS_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_COUNT;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_DEF;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_ENCODED_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_FAMILY;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_NAME;
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
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.INDEX_WHERE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.IS_ARRAY;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.IS_CONSTANT;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.IS_NAMESPACE_MAPPED;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.IS_VIEW_REFERENCED;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.JAR_PATH;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.KEY_SEQ;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.LAST_STATS_UPDATE_TIME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.LINK_TYPE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.MAX_VALUE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.MIN_PHOENIX_TTL_HWM;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.MIN_VALUE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.MULTI_TENANT;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.NULLABLE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.NUM_ARGS;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.ORDINAL_POSITION;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.PARENT_TENANT_ID;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.PHOENIX_TTL;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.PHOENIX_TTL_HWM;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.PHOENIX_TTL_NOT_DEFINED;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.PHYSICAL_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.PHYSICAL_TABLE_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.PK_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.RETURN_TYPE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SALT_BUCKETS;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SCHEMA_VERSION;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SORT_ORDER;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.STORE_NULLS;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.STREAMING_TOPIC_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYNC_INDEX_CREATED_DATE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CATALOG_SCHEMA;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CATALOG_TABLE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CHILD_LINK_NAMESPACE_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CHILD_LINK_NAME_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_FUNCTION_TABLE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_TASK_TABLE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_SCHEM;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_SEQ_NUM;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_TYPE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TENANT_ID;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TRANSACTIONAL;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TRANSACTION_PROVIDER;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TYPE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.UPDATE_CACHE_FREQUENCY;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.USE_STATS_FOR_PARALLELIZATION;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.VIEW_CONSTANT;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.VIEW_INDEX_ID_DATA_TYPE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.VIEW_STATEMENT;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.VIEW_TYPE;
import static org.apache.phoenix.query.QueryConstants.SPLITS_FILE;
import static org.apache.phoenix.query.QueryConstants.SYSTEM_SCHEMA_NAME;
import static org.apache.phoenix.query.QueryServices.DEFAULT_DISABLE_VIEW_SUBTREE_VALIDATION;
import static org.apache.phoenix.query.QueryServices.DISABLE_VIEW_SUBTREE_VALIDATION;
import static org.apache.phoenix.query.QueryServices.INDEX_CREATE_DEFAULT_STATE;
import static org.apache.phoenix.thirdparty.com.google.common.collect.Sets.newLinkedHashSet;
import static org.apache.phoenix.thirdparty.com.google.common.collect.Sets.newLinkedHashSetWithExpectedSize;
import static org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants.RUN_UPDATE_STATS_ASYNC_ATTRIB;
import static org.apache.phoenix.coprocessorclient.tasks.IndexRebuildTaskConstants.INDEX_NAME;
import static org.apache.phoenix.coprocessorclient.tasks.IndexRebuildTaskConstants.REBUILD_ALL;
import static org.apache.phoenix.exception.SQLExceptionCode.INSUFFICIENT_MULTI_TENANT_COLUMNS;
import static org.apache.phoenix.exception.SQLExceptionCode.PARENT_TABLE_NOT_FOUND;
import static org.apache.phoenix.monitoring.MetricType.NUM_METADATA_LOOKUP_FAILURES;
import static org.apache.phoenix.query.QueryConstants.BASE_TABLE_BASE_COLUMN_COUNT;
import static org.apache.phoenix.query.QueryConstants.DEFAULT_COLUMN_FAMILY;
import static org.apache.phoenix.query.QueryConstants.ENCODED_CQ_COUNTER_INITIAL_VALUE;
import static org.apache.phoenix.query.QueryServices.DROP_METADATA_ATTRIB;
import static org.apache.phoenix.query.QueryServicesOptions.DEFAULT_DROP_METADATA;
import static org.apache.phoenix.query.QueryServicesOptions.DEFAULT_RUN_UPDATE_STATS_ASYNC;
import static org.apache.phoenix.schema.ColumnMetaDataOps.addColumnMutation;
import static org.apache.phoenix.schema.ColumnMetaDataOps.newColumn;
import static org.apache.phoenix.schema.PTable.EncodedCQCounter.NULL_COUNTER;
import static org.apache.phoenix.schema.PTable.ImmutableStorageScheme.ONE_CELL_PER_COLUMN;
import static org.apache.phoenix.schema.PTable.ImmutableStorageScheme.SINGLE_CELL_ARRAY_WITH_OFFSETS;
import static org.apache.phoenix.schema.PTable.QualifierEncodingScheme.NON_ENCODED_QUALIFIERS;
import static org.apache.phoenix.schema.PTable.ViewType.MAPPED;
import static org.apache.phoenix.schema.PTableType.INDEX;
import static org.apache.phoenix.schema.PTableType.TABLE;
import static org.apache.phoenix.schema.PTableType.VIEW;
import static org.apache.phoenix.schema.types.PDataType.FALSE_BYTES;
import static org.apache.phoenix.schema.types.PDataType.TRUE_BYTES;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Timestamp;
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
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Table;
import org.apache.phoenix.coprocessorclient.TableInfo;
import org.apache.phoenix.query.ConnectionlessQueryServicesImpl;
import org.apache.phoenix.query.DelegateQueryServices;
import org.apache.phoenix.schema.task.SystemTaskParams;
import org.apache.phoenix.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.security.AccessDeniedException;
import org.apache.hadoop.hbase.security.access.AccessControlClient;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.util.StringUtils;
import org.apache.phoenix.compile.ColumnResolver;
import org.apache.phoenix.compile.FromCompiler;
import org.apache.phoenix.compile.IndexExpressionCompiler;
import org.apache.phoenix.compile.MutationPlan;
import org.apache.phoenix.compile.PostDDLCompiler;
import org.apache.phoenix.compile.PostIndexDDLCompiler;
import org.apache.phoenix.compile.PostLocalIndexDDLCompiler;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.compile.ServerBuildIndexCompiler;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.compile.StatementNormalizer;
import org.apache.phoenix.coprocessorclient.MetaDataProtocol;
import org.apache.phoenix.coprocessorclient.MetaDataProtocol.MetaDataMutationResult;
import org.apache.phoenix.coprocessorclient.MetaDataProtocol.MutationCode;
import org.apache.phoenix.coprocessorclient.MetaDataProtocol.SharedTableState;
import org.apache.phoenix.schema.stats.GuidePostsInfo;
import org.apache.phoenix.schema.transform.TransformClient;
import org.apache.phoenix.util.ClientUtil;
import org.apache.phoenix.util.TaskMetaDataServiceCallBack;
import org.apache.phoenix.util.ViewUtil;
import org.apache.phoenix.util.JacksonUtil;
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
import org.apache.phoenix.monitoring.TableMetricsManager;
import org.apache.phoenix.parse.AddColumnStatement;
import org.apache.phoenix.parse.AlterIndexStatement;
import org.apache.phoenix.parse.ChangePermsStatement;
import org.apache.phoenix.parse.CloseStatement;
import org.apache.phoenix.parse.ColumnDef;
import org.apache.phoenix.parse.ColumnDefInPkConstraint;
import org.apache.phoenix.parse.ColumnName;
import org.apache.phoenix.parse.CreateFunctionStatement;
import org.apache.phoenix.parse.CreateIndexStatement;
import org.apache.phoenix.parse.CreateSchemaStatement;
import org.apache.phoenix.parse.CreateSequenceStatement;
import org.apache.phoenix.parse.CreateTableStatement;
import org.apache.phoenix.parse.DeclareCursorStatement;
import org.apache.phoenix.parse.DropColumnStatement;
import org.apache.phoenix.parse.DropFunctionStatement;
import org.apache.phoenix.parse.DropIndexStatement;
import org.apache.phoenix.parse.DropSchemaStatement;
import org.apache.phoenix.parse.DropSequenceStatement;
import org.apache.phoenix.parse.DropTableStatement;
import org.apache.phoenix.parse.IndexKeyConstraint;
import org.apache.phoenix.parse.NamedNode;
import org.apache.phoenix.parse.NamedTableNode;
import org.apache.phoenix.parse.OpenStatement;
import org.apache.phoenix.parse.PFunction;
import org.apache.phoenix.parse.PFunction.FunctionArgument;
import org.apache.phoenix.parse.PSchema;
import org.apache.phoenix.parse.ParseNode;
import org.apache.phoenix.parse.ParseNodeFactory;
import org.apache.phoenix.parse.PrimaryKeyConstraint;
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
import org.apache.phoenix.schema.stats.StatisticsUtil;
import org.apache.phoenix.schema.task.Task;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PDate;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.schema.types.PTimestamp;
import org.apache.phoenix.schema.types.PUnsignedLong;
import org.apache.phoenix.schema.types.PVarbinary;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.transaction.PhoenixTransactionContext;
import org.apache.phoenix.transaction.PhoenixTransactionProvider;
import org.apache.phoenix.transaction.TransactionFactory;
import org.apache.phoenix.transaction.TransactionFactory.Provider;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.CursorUtil;
import org.apache.phoenix.util.EncodedColumnsUtil;
import org.apache.phoenix.util.EnvironmentEdgeManager;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.thirdparty.com.google.common.base.Strings;
import org.apache.phoenix.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.phoenix.thirdparty.com.google.common.collect.ListMultimap;
import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.thirdparty.com.google.common.collect.Sets;
import org.apache.phoenix.thirdparty.com.google.common.primitives.Ints;

public class MetaDataClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(MetaDataClient.class);

    private static final ParseNodeFactory FACTORY = new ParseNodeFactory();
    private static final String SET_ASYNC_CREATED_DATE =
            "UPSERT INTO " + SYSTEM_CATALOG_SCHEMA + ".\"" + SYSTEM_CATALOG_TABLE + "\"( " +
                    TENANT_ID + "," +
                    TABLE_SCHEM + "," +
                    TABLE_NAME + "," +
                    ASYNC_CREATED_DATE + " " + PDate.INSTANCE.getSqlTypeName() +
                    ") VALUES (?, ?, ?, ?)";

    private static final String SET_INDEX_SYNC_CREATED_DATE =
            "UPSERT INTO " + SYSTEM_CATALOG_SCHEMA + ".\"" + SYSTEM_CATALOG_TABLE + "\"( " +
                    TENANT_ID + "," +
                    TABLE_SCHEM + "," +
                    TABLE_NAME + "," +
                    SYNC_INDEX_CREATED_DATE + " " + PDate.INSTANCE.getSqlTypeName() +
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
                    TRANSACTION_PROVIDER + "," +
                    UPDATE_CACHE_FREQUENCY + "," +
                    IS_NAMESPACE_MAPPED + "," +
                    AUTO_PARTITION_SEQ +  "," +
                    APPEND_ONLY_SCHEMA + "," +
                    GUIDE_POSTS_WIDTH + "," +
                    IMMUTABLE_STORAGE_SCHEME + "," +
                    ENCODING_SCHEME + "," +
                    USE_STATS_FOR_PARALLELIZATION +"," +
                    VIEW_INDEX_ID_DATA_TYPE +"," +
                    PHOENIX_TTL +"," +
                    PHOENIX_TTL_HWM + "," +
                    CHANGE_DETECTION_ENABLED + "," +
                    PHYSICAL_TABLE_NAME + "," +
                    SCHEMA_VERSION + "," +
                    STREAMING_TOPIC_NAME + "," +
                    INDEX_WHERE +
                    ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, " +
                "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    private static final String CREATE_SCHEMA = "UPSERT INTO " + SYSTEM_CATALOG_SCHEMA + ".\"" + SYSTEM_CATALOG_TABLE
            + "\"( " + TABLE_SCHEM + "," + TABLE_NAME + ") VALUES (?,?)";

    public static final String CREATE_LINK =
            "UPSERT INTO " + SYSTEM_CATALOG_SCHEMA + ".\"" + SYSTEM_CATALOG_TABLE + "\"( " +
                    TENANT_ID + "," +
                    TABLE_SCHEM + "," +
                    TABLE_NAME + "," +
                    COLUMN_FAMILY + "," +
                    LINK_TYPE + "," +
                    TABLE_SEQ_NUM +","+ // this is actually set to the parent table's sequence number
                    TABLE_TYPE +
                    ") VALUES (?, ?, ?, ?, ?, ?, ?)";
    
    
    public static final String CREATE_VIEW_LINK =
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

    private static final String CREATE_CHILD_LINK =
            "UPSERT INTO " + SYSTEM_CATALOG_SCHEMA + ".\"" + SYSTEM_CATALOG_TABLE + "\"( " +
                    TENANT_ID + "," +
                    TABLE_SCHEM + "," +
                    TABLE_NAME + "," +
                    COLUMN_NAME + "," +
                    COLUMN_FAMILY + "," +
                    LINK_TYPE + 
                    ") VALUES (?, ?, ?, ?, ?, ?)";
    
    private static final String CREATE_VIEW_INDEX_PARENT_LINK =
    		"UPSERT INTO " + SYSTEM_CATALOG_SCHEMA + ".\"" + SYSTEM_CATALOG_TABLE + "\"( " +
                    TENANT_ID + "," +
                    TABLE_SCHEM + "," +
                    TABLE_NAME + "," +
                    COLUMN_FAMILY + "," +
                    LINK_TYPE + 
                    ") VALUES (?, ?, ?, ?, ?)";
    
    private static final String INCREMENT_SEQ_NUM =
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
    public static final String UPDATE_INDEX_STATE =
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
    
    public static final String UPDATE_INDEX_STATE_TO_ACTIVE =
            "UPSERT INTO " + SYSTEM_CATALOG_SCHEMA + ".\"" + SYSTEM_CATALOG_TABLE + "\"( " +
                    TENANT_ID + "," +
                    TABLE_SCHEM + "," +
                    TABLE_NAME + "," +
                    INDEX_STATE + "," +
                    INDEX_DISABLE_TIMESTAMP +","+
                    ASYNC_REBUILD_TIMESTAMP + " " + PLong.INSTANCE.getSqlTypeName() +
                    ") VALUES (?, ?, ?, ?, ?, ?)";

    /*
     * Custom sql to add a column to SYSTEM.CATALOG table during upgrade.
     * We can't use the regular ColumnMetaDataOps.UPSERT_COLUMN sql because the COLUMN_QUALIFIER column
     * was added in 4.10. And so if upgrading from let's say 4.7, we won't be able to
     * find the COLUMN_QUALIFIER column which the INSERT_COLUMN_ALTER_TABLE sql expects.
     */
    public static final String ALTER_SYSCATALOG_TABLE_UPGRADE =
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

    public MetaDataMutationResult updateCache(String schemaName, String tableName,
            boolean alwaysHitServer) throws SQLException {
        return updateCache(connection.getTenantId(), schemaName, tableName, alwaysHitServer);
    }

    public MetaDataMutationResult updateCache(PName tenantId, String schemaName, String tableName, boolean alwaysHitServer) throws SQLException {
        return updateCache(tenantId, schemaName, tableName, alwaysHitServer, null);
    }

    /**
     * Update the cache with the latest as of the connection scn.
     * @param functionNames
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

    public MetaDataMutationResult updateCache(PName origTenantId, String schemaName, String tableName,
            boolean alwaysHitServer, Long resolvedTimestamp) throws SQLException { // TODO: pass byte[] herez
        boolean systemTable = SYSTEM_CATALOG_SCHEMA.equals(schemaName);
        // System tables must always have a null tenantId
        PName tenantId = systemTable ? null : origTenantId;
        PTable table = null;
        PTableRef tableRef = null;
        String fullTableName = SchemaUtil.getTableName(schemaName, tableName);
        long tableTimestamp = HConstants.LATEST_TIMESTAMP;
        long tableResolvedTimestamp = HConstants.LATEST_TIMESTAMP;
        int tryCount = 0;
        // for tenant specific connection, look up the global table without using tenantId
        int maxTryCount = tenantId == null ? 1 : 2;
        do {
            try {
                tableRef = connection.getTableRef(new PTableKey(tenantId, fullTableName));
                table = tableRef.getTable();
                tableTimestamp = table.getTimeStamp();
                tableResolvedTimestamp = table.getTimeStamp();
                break;
            } catch (TableNotFoundException e) {
                tenantId = null;
            }
        } while (++tryCount < maxTryCount);
        // reset the tenantId if the global table isn't found in the cache
        if (table==null) {
            tenantId = systemTable ? null : origTenantId;
        }

        // start a txn if all table are transactional by default or if we found the table in the cache and it is transactional
        // TODO if system tables become transactional remove the check
        boolean isTransactional = (table!=null && table.isTransactional());
        if (isTransactional) {
            connection.getMutationState().startTransaction(table.getTransactionProvider());
        }
        // this is to allow the connection to see SYSTEM tables during an upgrade since they are not cached at the connection level
        if (connection.isRunningUpgrade() && systemTable && connection.getSCN() != null && connection.getSCN() <= MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP) {
            resolvedTimestamp = HConstants.LATEST_TIMESTAMP;
        }
        else {
            resolvedTimestamp = resolvedTimestamp==null ? TransactionUtil.getResolvedTimestamp(connection, isTransactional, HConstants.LATEST_TIMESTAMP) : resolvedTimestamp;
        }

        if (avoidRpcToGetTable(alwaysHitServer, resolvedTimestamp, systemTable, table, tableRef,
                tableResolvedTimestamp)) {
            return new MetaDataMutationResult(MutationCode.TABLE_ALREADY_EXISTS,
                    QueryConstants.UNSET_TIMESTAMP, table);
        }

        MetaDataMutationResult result;
        // if we are looking up an index on a child view that is inherited from its
        // parent, then we need to resolve the parent of the child view which will also
        // load any of its indexes instead of trying to load the inherited view index
        // which doesn't exist in SYSTEM.CATALOG
        if (tableName.contains(QueryConstants.CHILD_VIEW_INDEX_NAME_SEPARATOR)) {
            String parentViewName =
                    SchemaUtil.getSchemaNameFromFullName(tableName,
                        QueryConstants.CHILD_VIEW_INDEX_NAME_SEPARATOR);
            // recursively look up the parent view as we could have inherited this index from an ancestor
            // view(V) with Index (VIndex) -> child view (V1) -> grand child view (V2)
            // the view index name will be V2#V1#VIndex
            result =
                    updateCache(origTenantId, SchemaUtil.getSchemaNameFromFullName(parentViewName),
                        SchemaUtil.getTableNameFromFullName(parentViewName), alwaysHitServer,
                        resolvedTimestamp);
            if (result.getTable() != null) {
                try {
                    tableRef = connection.getTableRef(new PTableKey(tenantId, fullTableName));
                    table = tableRef.getTable();
                    return new MetaDataMutationResult(MutationCode.TABLE_ALREADY_EXISTS,
                            tableRef.getResolvedTimeStamp(), table);
                } catch (TableNotFoundException e) {
                    // reset the result as we looked up the parent view 
                    return new MetaDataMutationResult(MutationCode.TABLE_NOT_FOUND,
                        QueryConstants.UNSET_TIMESTAMP, null);
                }
            }
        }
        else {
            tryCount = 0;
            do {
                final byte[] schemaBytes = PVarchar.INSTANCE.toBytes(schemaName);
                final byte[] tableBytes = PVarchar.INSTANCE.toBytes(tableName);
                ConnectionQueryServices queryServices = connection.getQueryServices();
                result =
                        queryServices.getTable(tenantId, schemaBytes, tableBytes, tableTimestamp, resolvedTimestamp);
                // if the table was assumed to be non transactional, but is actually transactional
                // then re-resolve as of the right timestamp
                if (result.getTable() != null
                        && result.getTable().isTransactional()
                        && !isTransactional) {
                    long resolveTimestamp = TransactionUtil.getResolvedTimestamp(connection,
                            result.getTable().isTransactional(),
                            HConstants.LATEST_TIMESTAMP);
                    // Reresolve if table timestamp is past timestamp as of which we should see data
                    if (result.getTable().getTimeStamp() >= resolveTimestamp) {
                        result = queryServices.getTable(tenantId, schemaBytes,
                                tableBytes, tableTimestamp, resolveTimestamp);
                    }
                }

                if (SYSTEM_CATALOG_SCHEMA.equals(schemaName)) {
                    if (result.getMutationCode() == MutationCode.TABLE_ALREADY_EXISTS
                            && result.getTable() == null) {
                        result.setTable(table);
                    }
                    if (result.getTable()!=null) {
                        addTableToCache(result);
                    }
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
                            if (addColumnsAndIndexesFromAncestors(result, resolvedTimestamp, true)) {
                                connection.addTable(result.getTable(), resolvedTime);
                            } else {
                                // if we aren't adding the table, we still need to update the
                                // resolved time of the table
                                connection.updateResolvedTimestamp(table, resolvedTime);
                            }
                            return result;
                        }
                        // If table was not found at the current time stamp and we have one cached,
                        // remove it.
                        // Otherwise, we're up to date, so there's nothing to do.
                        if (code == MutationCode.TABLE_NOT_FOUND && tryCount + 1 == maxTryCount) {
                            connection
                                    .removeTable(origTenantId, fullTableName,
                                        table.getParentName() == null ? null
                                                : table.getParentName().getString(),
                                        table.getTimeStamp());
                        }
                        TableMetricsManager.updateMetricsForSystemCatalogTableMethod(
                                null, NUM_METADATA_LOOKUP_FAILURES, 1);
                    }
                }
                tenantId = null; // Try again with global tenantId
            } while (++tryCount < maxTryCount);
        }

        return result;
    }

    // Do not make rpc to getTable if
    // 1. table is a system table that does not have a ROW_TIMESTAMP column OR
    // 2. table was already resolved as of that timestamp OR
    // 3. table does not have a ROW_TIMESTAMP column and age is less then UPDATE_CACHE_FREQUENCY
    // 3a. Get the effective UPDATE_CACHE_FREQUENCY for checking the age in the following precedence order:
    // Table-level property > Connection-level property > Default value.
    private boolean avoidRpcToGetTable(boolean alwaysHitServer, Long resolvedTimestamp,
            boolean systemTable, PTable table, PTableRef tableRef, long tableResolvedTimestamp) {
        if (table != null && !alwaysHitServer) {
            if (systemTable && table.getRowTimestampColPos() == -1 ||
                    resolvedTimestamp == tableResolvedTimestamp) {
                return true;
            }

            final long effectiveUpdateCacheFreq;
            final String ucfInfoForLogging; // Only used for logging purposes

            boolean overrideUcfToDefault = false;
            if (table.getType() == INDEX) {
                overrideUcfToDefault =
                        PIndexState.PENDING_DISABLE.equals(table.getIndexState()) ||
                                !IndexMaintainer.sendIndexMaintainer(table);
            }
            if (!overrideUcfToDefault && !table.getIndexes().isEmpty()) {
                List<PTable> indexes = table.getIndexes();
                List<PTable> maintainedIndexes =
                        Lists.newArrayList(IndexMaintainer.maintainedIndexes(indexes.iterator()));
                // The maintainedIndexes contain only the indexes that are used by clients
                // while generating the mutations. If all the indexes are usable by clients,
                // we don't need to override UPDATE_CACHE_FREQUENCY. However, if any index is
                // not in usable state by the client mutations, we should override
                // UPDATE_CACHE_FREQUENCY to default value so that we make getTable() RPC calls
                // until all index states change to ACTIVE, BUILDING or other usable states.
                overrideUcfToDefault = indexes.size() != maintainedIndexes.size();
            }

            // What if the table is created with UPDATE_CACHE_FREQUENCY explicitly set to ALWAYS?
            // i.e. explicitly set to 0. We should ideally be checking for something like
            // hasUpdateCacheFrequency().

            //always fetch an Index in PENDING_DISABLE state to retrieve server timestamp
            //QueryOptimizer needs that to decide whether the index can be used
            if (overrideUcfToDefault) {
                effectiveUpdateCacheFreq =
                    (Long) ConnectionProperty.UPDATE_CACHE_FREQUENCY.getValue(
                        connection.getQueryServices().getProps()
                            .get(QueryServices.DEFAULT_UPDATE_CACHE_FREQUENCY_ATRRIB));
                ucfInfoForLogging = "connection-level-default";
            } else if (table.getUpdateCacheFrequency()
                    != QueryServicesOptions.DEFAULT_UPDATE_CACHE_FREQUENCY) {
                effectiveUpdateCacheFreq = table.getUpdateCacheFrequency();
                ucfInfoForLogging = "table-level";
            } else {
                effectiveUpdateCacheFreq =
                        (Long) ConnectionProperty.UPDATE_CACHE_FREQUENCY.getValue(
                                connection.getQueryServices().getProps().get(
                                        QueryServices.DEFAULT_UPDATE_CACHE_FREQUENCY_ATRRIB));
                ucfInfoForLogging = connection.getQueryServices().getProps().get(
                        QueryServices.DEFAULT_UPDATE_CACHE_FREQUENCY_ATRRIB) != null ?
                        "connection-level" : "default";
            }

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Using " + ucfInfoForLogging + " Update Cache Frequency (value = " +
                        effectiveUpdateCacheFreq + "ms) for " + table.getName() +
                        (table.getTenantId() != null ? ", Tenant ID: " + table.getTenantId() : ""));
            }

            return (table.getRowTimestampColPos() == -1 &&
                    connection.getMetaDataCache().getAge(tableRef) <
                            effectiveUpdateCacheFreq);
        }
        return false;
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

    public MetaDataMutationResult updateCache(PName tenantId, List<String> functionNames,
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
            for (int i = 0; i< functionNames.size(); i++) {
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
     * Looks up the ancestors of views and view indexes and adds inherited columns and
     * also any indexes of the ancestors that can be used
     *
     * @param result the result from updating the cache for the current table.
     * @param resolvedTimestamp timestamp at which child table was resolved
     * @param alwaysAddAncestorColumnsAndIndexes flag that determines whether we should recalculate
     *        all inherited columns and indexes that can be used in the view and
     * @return true if the PTable contained by result was modified and false otherwise
     * @throws SQLException if the physical table cannot be found
     */
    private boolean addColumnsAndIndexesFromAncestors(MetaDataMutationResult result, Long resolvedTimestamp,
                                                      boolean alwaysAddAncestorColumnsAndIndexes) throws SQLException {
        PTable table = result.getTable();
        boolean hasIndexId = table.getViewIndexId() != null;
        // only need to inherit columns and indexes for view indexes and views
        if ((table.getType()==PTableType.INDEX && hasIndexId)
                || (table.getType() == PTableType.VIEW && table.getViewType() != ViewType.MAPPED)) {
            String tableName = null;
            try {
                String parentName = table.getParentName().getString();
                String parentSchemaName = SchemaUtil.getSchemaNameFromFullName(parentName);
                tableName = SchemaUtil.getTableNameFromFullName(parentName);
                MetaDataMutationResult parentResult = updateCache(connection.getTenantId(), parentSchemaName, tableName,
                        false, resolvedTimestamp);
                PTable parentTable = parentResult.getTable();
                if (parentResult.getMutationCode() == MutationCode.TABLE_NOT_FOUND || parentTable == null) {
                    // Try once more with different tenant id (connection can be global but view could be tenant
                    parentResult =
                            updateCache(table.getTenantId(), parentSchemaName, tableName, false,
                                    resolvedTimestamp);
                    parentTable = parentResult.getTable();
                }
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace("addColumnsAndIndexesFromAncestors parent logical name " + table.getBaseTableLogicalName().getString() + " parent name " + table.getParentName().getString() + " tableName=" + table.getName());
                }
                if (parentResult.getMutationCode() == MutationCode.TABLE_NOT_FOUND || parentTable == null) {
                    // this mean the parent table was dropped and the child views have not yet been
                    // dropped by the TaskRegionObserver
                    String schemaName = table.getSchemaName() != null ? table.getSchemaName().getString() : null;
                    throw new TableNotFoundException(schemaName, parentName);
                }
                // only inherit columns view indexes (and not local indexes on regular tables which also have a viewIndexId)
                if (hasIndexId && parentTable.getType() != PTableType.VIEW) {
                    return false;
                }
                // if alwaysAddAncestorColumnsAndIndexes is false we only recalculate if the ancestor table or table
                // was updated from the server
                if (!alwaysAddAncestorColumnsAndIndexes && !result.wasUpdated() && !parentResult.wasUpdated()) {
                    return false;
                }
                result.setTable(ViewUtil.addDerivedColumnsAndIndexesFromParent(
                        connection, table, parentTable));
                return true;
            } catch (Throwable e) {
                TableMetricsManager.updateMetricsForSystemCatalogTableMethod(tableName, NUM_METADATA_LOOKUP_FAILURES, 1);
                throw e;
            }
        }
        return false;
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

    public MutationState createTable(CreateTableStatement statement, byte[][] splits, PTable parent, String viewStatement, ViewType viewType, PDataType viewIndexIdType, byte[][] viewColumnConstants, BitSet isViewColumnReferenced) throws SQLException {
        TableName tableName = statement.getTableName();
        Map<String,Object> tableProps = Maps.newHashMapWithExpectedSize(statement.getProps().size());
        Map<String,Object> commonFamilyProps = Maps.newHashMapWithExpectedSize(statement.getProps().size() + 1);
        populatePropertyMaps(statement.getProps(), tableProps, commonFamilyProps, statement.getTableType());

        splits = processSplits(tableProps, splits);
        boolean isAppendOnlySchema = false;
        long updateCacheFrequency = (Long) ConnectionProperty.UPDATE_CACHE_FREQUENCY.getValue(
                connection.getQueryServices().getProps().get(
                        QueryServices.DEFAULT_UPDATE_CACHE_FREQUENCY_ATRRIB));
        Long updateCacheFrequencyProp = (Long) TableProperty.UPDATE_CACHE_FREQUENCY.getValue(tableProps);
        if (parent==null) {
	        Boolean appendOnlySchemaProp = (Boolean) TableProperty.APPEND_ONLY_SCHEMA.getValue(tableProps);
	        if (appendOnlySchemaProp != null) {
	            isAppendOnlySchema = appendOnlySchemaProp;
	        }
	        if (updateCacheFrequencyProp != null) {
	            updateCacheFrequency = updateCacheFrequencyProp;
	        }
        }
        else {
        	isAppendOnlySchema = parent.isAppendOnlySchema();
        	updateCacheFrequency = (updateCacheFrequencyProp != null) ?
                    updateCacheFrequencyProp : parent.getUpdateCacheFrequency();
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
                    TableMetricsManager.updateMetricsForSystemCatalogTableMethod(
                            tableName.toString(), NUM_METADATA_LOOKUP_FAILURES, 1);
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
                        true, NamedTableNode.create(statement.getTableName()), statement.getTableType(), false, null);
            }
        }
        table = createTableInternal(statement, splits, parent, viewStatement, viewType, viewIndexIdType, viewColumnConstants, isViewColumnReferenced, false, null, null, tableProps, commonFamilyProps);

        if (table == null || table.getType() == PTableType.VIEW
                || statement.isNoVerify() /*|| table.isTransactional()*/) {
            return new MutationState(0, 0, connection);
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
        MutationPlan plan = compiler.compile(Collections.singletonList(tableRef), emptyCF, null, null, ts);
        return connection.getQueryServices().updateData(plan);
    }

    /*
      Create splits either from the provided splits or reading from SPLITS_FILE.
     */
    private byte[][] processSplits(Map<String, Object> tableProperties, byte[][] splits)
            throws SQLException {
        String splitFilesLocation = (String) tableProperties.get(SPLITS_FILE);
        if (splitFilesLocation == null || splitFilesLocation.isEmpty()) {
            splitFilesLocation = null;
        }

        // Both splits and split file location are not passed, so return empty split.
        if (splits.length == 0 && splitFilesLocation == null) {
            return splits;
        }

        // Both splits[] and splitFileLocation are provided. Throw an exception in this case.
        if (splits.length != 0 && splitFilesLocation != null) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.SPLITS_AND_SPLIT_FILE_EXISTS)
                    .build().buildException();
        }

        // This means we only have splits[] and no split file location is specified
        if (splitFilesLocation == null) {
            return splits;
        }
        // This means splits[] is empty and split file location is not null.
        File splitFile = new File(splitFilesLocation);
        // Check if file exists and is a file not a directory.
        if (!splitFile.exists() || !splitFile.isFile()) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.SPLIT_FILE_DONT_EXIST)
                    .build().buildException();
        }
        List<byte[]> splitsListFromFile = new ArrayList<>();
        Path path = Paths.get(splitFilesLocation);
        try (BufferedReader reader = Files.newBufferedReader(path)) {
            String line;
            while ((line = reader.readLine()) != null) {
                splitsListFromFile.add(Bytes.toBytes(line));
            }
        } catch (IOException ioe) {
            LOGGER.warn("Exception while reading splits file", ioe);
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.UNABLE_TO_OPEN_SPLIT_FILE)
                    .build().buildException();
        }
        return splitsListFromFile.toArray(new byte[splitsListFromFile.size()][]);
    }

    /**
     * Populate properties for the table and common properties for all column families of the table
     * @param statementProps Properties specified in SQL statement
     * @param tableProps Properties for an HTableDescriptor and Phoenix Table Properties
     * @param commonFamilyProps Properties common to all column families
     * @param tableType Used to distinguish between index creation vs. base table creation paths
     * @throws SQLException
     */
    private void populatePropertyMaps(ListMultimap<String,Pair<String,Object>> statementProps, Map<String, Object> tableProps,
            Map<String, Object> commonFamilyProps, PTableType tableType) throws SQLException {
        // Somewhat hacky way of determining if property is for HColumnDescriptor or HTableDescriptor
        ColumnFamilyDescriptor defaultDescriptor = ColumnFamilyDescriptorBuilder.of(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES);
        if (!statementProps.isEmpty()) {
            Collection<Pair<String,Object>> propsList = statementProps.get(QueryConstants.ALL_FAMILY_PROPERTIES_KEY);
            for (Pair<String,Object> prop : propsList) {
                if (tableType == PTableType.INDEX && MetaDataUtil.propertyNotAllowedToBeOutOfSync(prop.getFirst())) {
                    throw new SQLExceptionInfo.Builder(SQLExceptionCode.CANNOT_SET_OR_ALTER_PROPERTY_FOR_INDEX)
                            .setMessage("Property: " + prop.getFirst()).build()
                            .buildException();
                }
                // HTableDescriptor property or Phoenix Table Property
                if (defaultDescriptor.getValue(Bytes.toBytes(prop.getFirst())) == null) {
                    // See PHOENIX-4891
                    if (tableType == PTableType.INDEX && UPDATE_CACHE_FREQUENCY.equals(prop.getFirst())) {
                        throw new SQLExceptionInfo.Builder(
                                SQLExceptionCode.CANNOT_SET_OR_ALTER_UPDATE_CACHE_FREQ_FOR_INDEX)
                                .build()
                                .buildException();
                    }
                    tableProps.put(prop.getFirst(), prop.getSecond());
                } else { // HColumnDescriptor property
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
            rowCount += updateStatisticsInternal(table.getPhysicalName(), table, updateStatisticsStmt.getProps(), true);
        }
        if (updateStatisticsStmt.updateIndex()) {
            // TODO: If our table is a VIEW with multiple indexes or a TABLE with local indexes,
            // we may be doing more work that we have to here. We should union the scan ranges
            // across all indexes in that case so that we don't re-calculate the same stats
            // multiple times.
            for (PTable index : table.getIndexes()) {
                // If the table is a view, then we will end up calling update stats
                // here for all the view indexes on it. We take care of local indexes later.
                if (index.getIndexType() != IndexType.LOCAL) {
                    if (table.getType() != PTableType.VIEW) {
                        rowCount += updateStatisticsInternal(index.getPhysicalName(), index,
                                updateStatisticsStmt.getProps(), true);
                    } else {
                        rowCount += updateStatisticsInternal(table.getPhysicalName(), index,
                                updateStatisticsStmt.getProps(), true);
                    }
                }
            }
            /*
             * Update stats for local indexes. This takes care of local indexes on the the table
             * as well as local indexes on any views on it.
             */
            PName physicalName = table.getPhysicalName();
            List<byte[]> localCFs = MetaDataUtil.getLocalIndexColumnFamilies(connection, physicalName.getBytes());
            if (!localCFs.isEmpty()) {
                /*
                 * We need to pass checkLastStatsUpdateTime as false here. Local indexes are on the
                 * same table as the physical table. So when the user has requested to update stats
                 * for both table and indexes on it, we need to make sure that we don't re-check
                 * LAST_UPDATE_STATS time. If we don't do that then we will end up *not* collecting
                 * stats for local indexes which would be bad.
                 *
                 * Note, that this also means we don't have a way of controlling how often update
                 * stats can run for local indexes. Consider the case when the user calls UPDATE STATS TABLE
                 * followed by UPDATE STATS TABLE INDEX. When the second statement is being executed,
                 * this causes us to skip the check and execute stats collection possibly a bit too frequently.
                 */
                rowCount += updateStatisticsInternal(physicalName, table, updateStatisticsStmt.getProps(), localCFs, false);
            }
            // If analyzing the indexes of a multi-tenant table or a table with view indexes
            // then analyze all of those indexes too.
            if (table.getType() != PTableType.VIEW) {
                if (table.isMultiTenant() || MetaDataUtil.hasViewIndexTable(connection, table.getPhysicalName())) {
                    final PName viewIndexPhysicalTableName = PNameFactory.newName(MetaDataUtil.getViewIndexPhysicalName(table.getPhysicalName().getBytes()));
                    PTable indexLogicalTable = new DelegateTable(table) {
                        @Override
                        public PName getPhysicalName() {
                            return viewIndexPhysicalTableName;
                        }
                    };
                    /*
                     * Note for future maintainers: local indexes whether on a table or on a view,
                     * reside on the same physical table as the base table and not the view index
                     * table. So below call is collecting stats only for non-local view indexes.
                     */
                    rowCount += updateStatisticsInternal(viewIndexPhysicalTableName, indexLogicalTable, updateStatisticsStmt.getProps(), true);
                }
            }
        }
        final long count = rowCount;
        return new MutationState(1, 1000, connection) {
            @Override
            public long getUpdateCount() {
                return count;
            }
        };
    }

    private long updateStatisticsInternal(PName physicalName, PTable logicalTable, Map<String, Object> statsProps, boolean checkLastStatsUpdateTime) throws SQLException {
        return updateStatisticsInternal(physicalName, logicalTable, statsProps, null, checkLastStatsUpdateTime);
    }
    
    private long updateStatisticsInternal(PName physicalName, PTable logicalTable, Map<String, Object> statsProps, List<byte[]> cfs, boolean checkLastStatsUpdateTime) throws SQLException {
        ReadOnlyProps props = connection.getQueryServices().getProps();
        final long msMinBetweenUpdates = props
            .getLong(QueryServices.MIN_STATS_UPDATE_FREQ_MS_ATTRIB,
                QueryServicesOptions.DEFAULT_MIN_STATS_UPDATE_FREQ_MS);
        Long scn = connection.getSCN();
        // Always invalidate the cache
        long clientTimeStamp = connection.getSCN() == null ? HConstants.LATEST_TIMESTAMP : scn;
        long msSinceLastUpdate = Long.MAX_VALUE;
        if (checkLastStatsUpdateTime) {
            String query = "SELECT CURRENT_DATE()," + LAST_STATS_UPDATE_TIME + " FROM "
                + PhoenixDatabaseMetaData.SYSTEM_STATS_NAME
                + " WHERE " + PHYSICAL_NAME + "= ?  AND " + COLUMN_FAMILY
                + " IS NULL AND " + LAST_STATS_UPDATE_TIME + " IS NOT NULL";
            try (PreparedStatement selectStatsStmt = connection.prepareStatement(query)) {
                selectStatsStmt.setString(1, physicalName.getString());
                ResultSet rs = selectStatsStmt.executeQuery(query);
                if (rs.next()) {
                    msSinceLastUpdate = rs.getLong(1) - rs.getLong(2);
                }
            }
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
                public TransactionFactory.Provider getTransactionProvider() {
                    return null;
                }
            };
            TableRef tableRef = new TableRef(null, nonTxnLogicalTable, clientTimeStamp, false);
            MutationPlan plan = compiler.compile(Collections.singletonList(tableRef), null, cfs, null, clientTimeStamp);
            Scan scan = plan.getContext().getScan();
            StatisticsUtil.setScanAttributes(scan, statsProps);
            boolean runUpdateStatsAsync = props.getBoolean(QueryServices.RUN_UPDATE_STATS_ASYNC, DEFAULT_RUN_UPDATE_STATS_ASYNC);
            scan.setAttribute(RUN_UPDATE_STATS_ASYNC_ATTRIB, runUpdateStatsAsync ? TRUE_BYTES : FALSE_BYTES);
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
        props.setProperty(PhoenixRuntime.BUILD_INDEX_AT_ATTRIB, Long.toString(connection.getSCN()+1));
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
    
    private MutationPlan getMutationPlanForBuildingIndex(PTable index, TableRef dataTableRef) throws SQLException {
        if (index.getIndexType() == IndexType.LOCAL) {
            PostLocalIndexDDLCompiler compiler =
                    new PostLocalIndexDDLCompiler(connection, getFullTableName(dataTableRef));
            return compiler.compile(index);
        } else if (dataTableRef.getTable().isTransactional()){
            PostIndexDDLCompiler compiler = new PostIndexDDLCompiler(connection, dataTableRef);
            return compiler.compile(index);
        } else {
            ServerBuildIndexCompiler compiler = new ServerBuildIndexCompiler(connection, getFullTableName(dataTableRef));
            return compiler.compile(index);
        }
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
            long startTime = EnvironmentEdgeManager.currentTimeMillis();
            MutationState state = connection.getQueryServices().updateData(mutationPlan);
            long firstUpsertSelectTime = EnvironmentEdgeManager.currentTimeMillis() - startTime;

            // for global indexes on non transactional tables we might have to
            // run a second index population upsert select to handle data rows
            // that were being written on the server while the index was created.
            // TODO: this sleep time is really arbitrary. If any query is in progress
            // while the index is being built, we're depending on this sleep
            // waiting them out. Instead we should have a means of waiting until
            // all in progress queries are complete (though I'm not sure that's
            // feasible). See PHOENIX-4092.
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
                    // TODO: Use scn or LATEST_TIMESTAMP here? It's possible that a DML statement
                    // ran and ended up with timestamps later than this time. If we use a later
                    // timestamp, we'll need to run the partial index rebuilder here as it's
                    // possible that the updates to the table were made (such as deletes) after
                    // the scn which would not be properly reflected correctly this mechanism.
                    // See PHOENIX-4092.
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

    public MutationState declareCursor(DeclareCursorStatement statement, QueryPlan queryPlan) throws SQLException {
        CursorUtil.declareCursor(statement, queryPlan);
        return new MutationState(0, 0, connection);
    }

    public MutationState open(OpenStatement statement) throws SQLException {
        CursorUtil.openCursor(statement, connection);
        return new MutationState(0, 0, connection);
    }

    public MutationState close(CloseStatement statement) throws SQLException {
        CursorUtil.closeCursor(statement);
        return new MutationState(0, 0, connection);
    }

    /**
     * Supprort long viewIndexId only if client has explicitly set
     * the QueryServices.LONG_VIEW_INDEX_ENABLED_ATTRIB connection property to 'true'.
     * @return
     */
    private PDataType getViewIndexDataType() throws SQLException {
        boolean supportsLongViewIndexId = connection.getQueryServices().getProps().getBoolean(
                                QueryServices.LONG_VIEW_INDEX_ENABLED_ATTRIB,
                                QueryServicesOptions.DEFAULT_LONG_VIEW_INDEX_ENABLED);
        return supportsLongViewIndexId ? MetaDataUtil.getViewIndexIdDataType() : MetaDataUtil.getLegacyViewIndexIdDataType();
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
        populatePropertyMaps(statement.getProps(), tableProps, commonFamilyProps, PTableType.INDEX);
        List<Pair<ParseNode, SortOrder>> indexParseNodeAndSortOrderList = ik.getParseNodeAndSortOrderList();
        List<ColumnName> includedColumns = statement.getIncludeColumns();
        TableRef tableRef = null;
        PTable table = null;
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
        Set<String> acquiredColumnMutexSet = Sets.newHashSetWithExpectedSize(3);
        String physicalSchemaName = null;
        String physicalTableName = null;
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
                if (hbaseVersion < MetaDataProtocol.MUTABLE_SI_VERSION_THRESHOLD) {
                    throw new SQLExceptionInfo.Builder(SQLExceptionCode.NO_MUTABLE_INDEXES).setTableName(indexTableName.getTableName()).build().buildException();
                }
                if (!connection.getQueryServices().hasIndexWALCodec() && !dataTable.isTransactional()) {
                    throw new SQLExceptionInfo.Builder(SQLExceptionCode.INVALID_MUTABLE_INDEX_CONFIG).setTableName(indexTableName.getTableName()).build().buildException();
                }
                boolean tableWithRowTimestampCol = dataTable.getRowTimestampColPos() != -1;
                if (tableWithRowTimestampCol) {
                    throw new SQLExceptionInfo.Builder(SQLExceptionCode.CANNOT_CREATE_INDEX_ON_MUTABLE_TABLE_WITH_ROWTIMESTAMP).setTableName(indexTableName.getTableName()).build().buildException();
                }
            }
            if (dataTable.isTransactional()
                    && isLocalIndex
                    && dataTable.getTransactionProvider().getTransactionProvider().isUnsupported(PhoenixTransactionProvider.Feature.ALLOW_LOCAL_INDEX)) {
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.CANNOT_CREATE_LOCAL_INDEX_FOR_TXN_TABLE).setMessage(dataTable.getTransactionProvider().name()).setTableName(indexTableName.getTableName()).build().buildException();
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
                PDataType dataType = getViewIndexDataType();
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
                if (!(expression.getDeterminism() == Determinism.ALWAYS || expression.getDeterminism() == Determinism.PER_ROW)) {
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

            Configuration config = connection.getQueryServices().getConfiguration();
            if (!connection.getQueryServices().getProps()
                .getBoolean(DISABLE_VIEW_SUBTREE_VALIDATION,
                    DEFAULT_DISABLE_VIEW_SUBTREE_VALIDATION)) {
                verifyIfDescendentViewsExtendPk(dataTable, config);
            }
            // for view indexes
            if (dataTable.getType() == PTableType.VIEW) {
                String physicalName = dataTable.getPhysicalName().getString();
                physicalSchemaName = SchemaUtil.getSchemaNameFromFullName(physicalName);
                physicalTableName = SchemaUtil.getTableNameFromFullName(physicalName);
                List<ColumnName> requiredCols = Lists.newArrayList(indexedColumnNames);
                requiredCols.addAll(includedColumns);
                for (ColumnName colName : requiredCols) {
                    // acquire the mutex using the global physical table name to
                    // prevent this column from being dropped while the view is being created
                    String colNameSeparatedByDot = colName.getColumnName()
                            .replace(QueryConstants.NAMESPACE_SEPARATOR,
                                     QueryConstants.NAME_SEPARATOR);
                    // indexed column name have a ':' between the column family and column name
                    // We would like to have '.' like in other column names
                    boolean acquiredMutex = writeCell(null, physicalSchemaName, physicalTableName,
                            colNameSeparatedByDot);
                    if (!acquiredMutex) {
                        throw new ConcurrentTableMutationException(physicalSchemaName, physicalTableName);
                    }
                    acquiredColumnMutexSet.add(colNameSeparatedByDot);
                }
            }

            long threshold = Long.parseLong(config.get(QueryServices.CLIENT_INDEX_ASYNC_THRESHOLD));

            if (threshold > 0 && !statement.isAsync()) {
                Set<String> columnFamilies = new HashSet<>();
                for (ColumnDef column : columnDefs){
                    try {
                        String columnFamily = IndexUtil
                                .getDataColumnFamilyName(column.getColumnDefName().getColumnName());
                        columnFamilies.add(!columnFamily.equals("") ? columnFamily
                                : dataTable.getDefaultFamilyName()!= null ?
                                        dataTable.getDefaultFamilyName().toString()
                                        : QueryConstants.DEFAULT_COLUMN_FAMILY);
                    } catch (Exception ignored){
                        ; // We ignore any exception during this phase
                    }
                }
                long estimatedBytes = 0;
                for (String colFamily : columnFamilies) {
                    GuidePostsInfo gps = connection.getQueryServices().getTableStats(
                            new GuidePostsKey(Bytes.toBytes(tableRef.getTable().toString()),
                                    Bytes.toBytes(colFamily)));
                    long[] byteCounts = gps.getByteCounts();
                    for (long byteCount : byteCounts) {
                        estimatedBytes += byteCount;
                    }

                    if (threshold < estimatedBytes) {
                        throw new SQLExceptionInfo
                                .Builder(SQLExceptionCode.ABOVE_INDEX_NON_ASYNC_THRESHOLD)
                                .build().buildException();
                    }
                }
            }

            // Set DEFAULT_COLUMN_FAMILY_NAME of index to match data table
            // We need this in the props so that the correct column family is created
            if (dataTable.getDefaultFamilyName() != null && dataTable.getType() != PTableType.VIEW && !allocateIndexId) {
                statement.getProps().put("", new Pair<String,Object>(DEFAULT_COLUMN_FAMILY_NAME,dataTable.getDefaultFamilyName().getString()));
            }
            PrimaryKeyConstraint pk = FACTORY.primaryKey(null, allPkColumns);

            tableProps.put(MetaDataUtil.DATA_TABLE_NAME_PROP_NAME, dataTable.getPhysicalName().getString());
            CreateTableStatement tableStatement = FACTORY.createTable(indexTableName,
                    statement.getProps(), columnDefs, pk, statement.getSplitNodes(),
                    PTableType.INDEX, statement.ifNotExists(), null,
                    statement.getWhere(), statement.getBindCount(), null);
            table = createTableInternal(tableStatement, splits, dataTable, null, null,
                    getViewIndexDataType() ,null, null, allocateIndexId,
                    statement.getIndexType(), asyncCreatedDate, tableProps, commonFamilyProps);
        }
        finally {
            deleteMutexCells(physicalSchemaName, physicalTableName, acquiredColumnMutexSet);
        }
        if (table == null) {
            return new MutationState(0, 0, connection);
        }

        if (LOGGER.isInfoEnabled()) LOGGER.info("Created index " + table.getName().getString() + " at " + table.getTimeStamp());
        boolean asyncIndexBuildEnabled = connection.getQueryServices().getProps().getBoolean(
                QueryServices.INDEX_ASYNC_BUILD_ENABLED,
                QueryServicesOptions.DEFAULT_INDEX_ASYNC_BUILD_ENABLED);
        // In async process, we return immediately as the MR job needs to be triggered .
        if (statement.isAsync() && asyncIndexBuildEnabled) {
            return new MutationState(0, 0, connection);
        }

        // If we create index in create_disabled state, we will build them later
        if (table.getIndexState() == PIndexState.CREATE_DISABLE) {
            return new MutationState(0, 0, connection);
        }

        // If our connection is at a fixed point-in-time, we need to open a new
        // connection so that our new index table is visible.
        if (connection.getSCN() != null) {
            return buildIndexAtTimeStamp(table, statement.getTable());
        }

        return buildIndex(table, tableRef);
    }

    /**
     * Go through all the descendent views from the child view hierarchy and find if any of the
     * descendent views extends the primary key, throw error.
     *
     * @param tableOrView view or table on which the index is being created.
     * @param config the configuration.
     * @throws SQLException if any of the descendent views extends pk or if something goes wrong
     * while querying descendent view hierarchy.
     */
    private void verifyIfDescendentViewsExtendPk(PTable tableOrView, Configuration config)
        throws SQLException {
        if (connection.getQueryServices() instanceof ConnectionlessQueryServicesImpl) {
            return;
        }
        if (connection.getQueryServices() instanceof DelegateQueryServices) {
            DelegateQueryServices services = (DelegateQueryServices) connection.getQueryServices();
            if (services.getDelegate() instanceof ConnectionlessQueryServicesImpl) {
                return;
            }
        }
        byte[] systemChildLinkTable = SchemaUtil.isNamespaceMappingEnabled(null, config) ?
            SYSTEM_CHILD_LINK_NAMESPACE_BYTES :
            SYSTEM_CHILD_LINK_NAME_BYTES;
        try (Table childLinkTable =
                     connection.getQueryServices().getTable(systemChildLinkTable)) {
            byte[] tenantId = connection.getTenantId() == null ? null
                    : connection.getTenantId().getBytes();
            byte[] schemaNameBytes = tableOrView.getSchemaName().getBytes();
            byte[] viewOrTableName = tableOrView.getTableName().getBytes();
            Pair<List<PTable>, List<TableInfo>> descViews =
                    ViewUtil.findAllDescendantViews(
                            childLinkTable,
                            config,
                            tenantId,
                            schemaNameBytes,
                            viewOrTableName,
                            HConstants.LATEST_TIMESTAMP,
                            false);
            List<PTable> legitimateChildViews = descViews.getFirst();
            int dataTableOrViewPkCols = tableOrView.getPKColumns().size();
            if (legitimateChildViews != null && legitimateChildViews.size() > 0) {
                for (PTable childView : legitimateChildViews) {
                    if (childView.getPKColumns().size() > dataTableOrViewPkCols) {
                        LOGGER.error("Creation of view index not allowed as child view {}"
                                + " extends pk", childView.getName());
                        throw new SQLExceptionInfo.Builder(
                                SQLExceptionCode
                                        .CANNOT_CREATE_INDEX_CHILD_VIEWS_EXTEND_PK)
                                .build()
                                .buildException();
                    }
                }
            }
        } catch (IOException e) {
            LOGGER.error("Error while retrieving descendent views", e);
            throw new SQLException(e);
        }
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
                return new MutationState(0, 0, connection);
            }
            throw e;
        }
        return new MutationState(1, 1000, connection);
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
                return new MutationState(0, 0, connection);
            }
            throw e;
        }
        return new MutationState(1, 1000, connection);
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
                if (function.isReplace()) {
                    connection.removeFunction(function.getTenantId(), function.getFunctionName(),
                            result.getMutationTime());
                }
                addFunctionToCache(result);
            }
        } finally {
            connection.setAutoCommit(wasAutoCommit);
        }
        return new MutationState(1, 1000, connection);
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
    
    /**
     * While adding or dropping columns we write a cell to the SYSTEM.MUTEX table with the rowkey of the
     * physical table to prevent conflicting concurrent modifications. For eg two client adding a column
     * with the same name but different datatype, or once client dropping a column on a base table
     * while another client creating a view or view index that requires the dropped column
     */
    private boolean writeCell(String tenantId, String schemaName, String tableName, String columnName)
            throws SQLException {
        return connection.getQueryServices().writeMutexCell(tenantId, schemaName, tableName, columnName, null);
    }

    /**
     * Remove the cell that was written to to the SYSTEM.MUTEX table with the rowkey of the physical table
     */
    private void deleteCell(String tenantId, String schemaName, String tableName, String columnName)
            throws SQLException {
        connection.getQueryServices().deleteMutexCell(tenantId, schemaName, tableName, columnName, null);
    }

    /**
     *
     * Populate the properties for each column family referenced in the create table statement
     * @param familyNames column families referenced in the create table statement
     * @param commonFamilyProps properties common to all column families
     * @param statement create table statement
     * @param defaultFamilyName the default column family name
     * @param isLocalIndex true if in the create local index path
     * @param familyPropList list containing pairs of column families and their corresponding properties
     * @throws SQLException
     */
    private void populateFamilyPropsList(Map<String, PName> familyNames, Map<String,Object> commonFamilyProps,
            CreateTableStatement statement, String defaultFamilyName, boolean isLocalIndex,
            final List<Pair<byte[],Map<String,Object>>> familyPropList) throws SQLException {
        for (PName familyName : familyNames.values()) {
            String fam = familyName.getString();
            Collection<Pair<String, Object>> propsForCF =
                    statement.getProps().get(IndexUtil.getActualColumnFamilyName(fam));
            // No specific properties for this column family, so add the common family properties
            if (propsForCF.isEmpty()) {
                familyPropList.add(new Pair<>(familyName.getBytes(),commonFamilyProps));
            } else {
                Map<String,Object> combinedFamilyProps = Maps.newHashMapWithExpectedSize(propsForCF.size() + commonFamilyProps.size());
                combinedFamilyProps.putAll(commonFamilyProps);
                for (Pair<String,Object> prop : propsForCF) {
                    // Don't allow specifying column families for TTL, KEEP_DELETED_CELLS and REPLICATION_SCOPE.
                    // These properties can only be applied for all column families of a table and can't be column family specific.
                    // See PHOENIX-3955
                    if (!fam.equals(QueryConstants.ALL_FAMILY_PROPERTIES_KEY) && MetaDataUtil.propertyNotAllowedToBeOutOfSync(prop.getFirst())) {
                        throw new SQLExceptionInfo.Builder(SQLExceptionCode.COLUMN_FAMILY_NOT_ALLOWED_FOR_PROPERTY)
                                .setMessage("Property: " + prop.getFirst())
                                .build()
                                .buildException();
                    }
                    combinedFamilyProps.put(prop.getFirst(), prop.getSecond());
                }
                familyPropList.add(new Pair<>(familyName.getBytes(),combinedFamilyProps));
            }
        }

        if (familyNames.isEmpty()) {
            // If there are no family names, use the default column family name. This also takes care of the case when
            // the table ddl has only PK cols present (which means familyNames is empty).
            byte[] cf =
                    defaultFamilyName == null ? (!isLocalIndex? QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES
                            : QueryConstants.DEFAULT_LOCAL_INDEX_COLUMN_FAMILY_BYTES)
                            : Bytes.toBytes(defaultFamilyName);
            familyPropList.add(new Pair<>(cf, commonFamilyProps));
        }
    }

    private PTable createTableInternal(CreateTableStatement statement, byte[][] splits,
            final PTable parent, String viewStatement, ViewType viewType, PDataType viewIndexIdType,
            final byte[][] viewColumnConstants, final BitSet isViewColumnReferenced, boolean allocateIndexId,
            IndexType indexType, Date asyncCreatedDate,
            Map<String,Object> tableProps,
            Map<String,Object> commonFamilyProps) throws SQLException {
        final PTableType tableType = statement.getTableType();
        boolean wasAutoCommit = connection.getAutoCommit();
        TableName tableNameNode = null;
        boolean allowSystemCatalogRollback =
                connection.getQueryServices().getProps().getBoolean(
                    QueryServices.ALLOW_SPLITTABLE_SYSTEM_CATALOG_ROLLBACK,
                    QueryServicesOptions.DEFAULT_ALLOW_SPLITTABLE_SYSTEM_CATALOG_ROLLBACK);
        Set<String> acquiredColumnMutexSet = Sets.newHashSetWithExpectedSize(3);
        String parentPhysicalName =
                (parent!=null &&  parent.getPhysicalName()!=null) ? parent.getPhysicalName().getString() : null;
        String parentPhysicalSchemaName = parentPhysicalName!=null ?
                SchemaUtil.getSchemaNameFromFullName(parentPhysicalName) : null;
        String parentPhysicalTableName = parentPhysicalName!=null ?
                SchemaUtil.getTableNameFromFullName(parentPhysicalName) : null;
        connection.rollback();
        try {
            connection.setAutoCommit(false);
            List<Mutation> tableMetaData = Lists.newArrayListWithExpectedSize(statement.getColumnDefs().size() + 3);

            tableNameNode = statement.getTableName();
            final String schemaName = connection.getSchema() != null && tableNameNode.getSchemaName() == null ? connection.getSchema() : tableNameNode.getSchemaName();
            final String tableName = tableNameNode.getTableName();
            String parentTableName = null;
            PName tenantId = connection.getTenantId();
            String tenantIdStr = tenantId == null ? null : tenantId.getString();
            Long scn = connection.getSCN();
            long clientTimeStamp = scn == null ? HConstants.LATEST_TIMESTAMP : scn;
            boolean multiTenant = false;
            boolean storeNulls = false;
            TransactionFactory.Provider transactionProvider = (parent!= null) ? parent.getTransactionProvider() : null;
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
            int baseTableColumnCount =
                    tableType == PTableType.VIEW ? parent.getColumns().size()
                            : QueryConstants.BASE_TABLE_BASE_COLUMN_COUNT;

            Long phoenixTTL = PHOENIX_TTL_NOT_DEFINED;
            Long phoenixTTLHighWaterMark = MIN_PHOENIX_TTL_HWM;
            Long phoenixTTLProp = (Long) TableProperty.PHOENIX_TTL.getValue(tableProps);;

            // Validate PHOENIX_TTL prop value if set
            if (phoenixTTLProp != null) {
                if (phoenixTTLProp < 0) {
                    throw new SQLExceptionInfo.Builder(SQLExceptionCode.ILLEGAL_DATA)
                            .setMessage(String.format("entity = %s, PHOENIX_TTL value should be > 0", tableName ))
                            .build()
                            .buildException();
                }

                // TODO: PHOENIX_TABLE_TTL
                if (tableType == VIEW  && parentPhysicalName != null) {
                    TableDescriptor desc = connection.getQueryServices().getTableDescriptor(
                        parentPhysicalName.getBytes(StandardCharsets.UTF_8));
                    if (desc != null) {
                        Integer tableTTLProp = desc.getColumnFamily(SchemaUtil.getEmptyColumnFamily(parent)).getTimeToLive();
                        if ((tableTTLProp != null) && (tableTTLProp != HConstants.FOREVER)) {
                            throw new SQLExceptionInfo.Builder(SQLExceptionCode.CANNOT_SET_OR_ALTER_PHOENIX_TTL_FOR_TABLE_WITH_TTL)
                                    .setMessage(String.format("table = %s, view = %s", parentPhysicalName, tableName ))
                                    .build()
                                    .buildException();
                        }
                    }
                }

                // Cannot set PHOENIX_TTL if parent has already defined it.
                if (tableType == VIEW  && parent != null && parent.getPhoenixTTL() != PHOENIX_TTL_NOT_DEFINED) {
                    throw new SQLExceptionInfo.Builder(
                            SQLExceptionCode.CANNOT_SET_OR_ALTER_PHOENIX_TTL)
                            .setSchemaName(schemaName).setTableName(tableName).build().buildException();
                }

                if (tableType != VIEW) {
                    throw new SQLExceptionInfo.Builder(SQLExceptionCode.PHOENIX_TTL_SUPPORTED_FOR_VIEWS_ONLY)
                            .setSchemaName(schemaName)
                            .setTableName(tableName)
                            .build()
                            .buildException();
                }
            }

            Boolean isChangeDetectionEnabledProp =
                (Boolean) TableProperty.CHANGE_DETECTION_ENABLED.getValue(tableProps);
            verifyChangeDetectionTableType(tableType, isChangeDetectionEnabledProp);

            String schemaVersion = (String) TableProperty.SCHEMA_VERSION.getValue(tableProps);
            String streamingTopicName = (String) TableProperty.STREAMING_TOPIC_NAME.getValue(tableProps);

            if (parent != null && tableType == PTableType.INDEX) {
                timestamp = TransactionUtil.getTableTimestamp(connection, transactionProvider != null, transactionProvider);
                isImmutableRows = parent.isImmutableRows();
                isAppendOnlySchema = parent.isAppendOnlySchema();
                phoenixTTL = parent.getPhoenixTTL();

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
                        // Parent is a view and this is an index so we need to get _IDX_+logical name of base table.
                        // parent.getPhysicalName is Schema.Physical of base and we can't use it since the _IDX_ table is logical name of the base.
                        // parent.getName is the view name. parent.getBaseTableLogicalName is the logical name of the base table
                        PName parentName = parent.getBaseTableLogicalName();
                        physicalNames = Collections.singletonList(PNameFactory.newName(MetaDataUtil.getViewIndexPhysicalName(parentName, isNamespaceMapped)));
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

                // Add row linking index table to parent table for indexes on views
                if (parent.getType() == PTableType.VIEW) {
	                linkStatement = connection.prepareStatement(CREATE_VIEW_INDEX_PARENT_LINK);
	                linkStatement.setString(1, tenantIdStr);
	                linkStatement.setString(2, schemaName);
	                linkStatement.setString(3, tableName);
	                linkStatement.setString(4, parent.getName().getString());
	                linkStatement.setByte(5, LinkType.VIEW_INDEX_PARENT_TABLE.getSerializedValue());
	                linkStatement.execute();
                }
            }

            PrimaryKeyConstraint pkConstraint = statement.getPrimaryKeyConstraint();
            String pkName = null;
            List<Pair<ColumnName,SortOrder>> pkColumnsNames = Collections.<Pair<ColumnName,SortOrder>>emptyList();
            Iterator<Pair<ColumnName,SortOrder>> pkColumnsIterator = Collections.emptyIterator();
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
            long updateCacheFrequency = (Long) ConnectionProperty.UPDATE_CACHE_FREQUENCY.getValue(
                    connection.getQueryServices().getProps().get(
                            QueryServices.DEFAULT_UPDATE_CACHE_FREQUENCY_ATRRIB));
            if (tableType == PTableType.INDEX && parent != null) {
                updateCacheFrequency = parent.getUpdateCacheFrequency();
            }
            Long updateCacheFrequencyProp = (Long) TableProperty.UPDATE_CACHE_FREQUENCY.getValue(tableProps);
            if (tableType != PTableType.INDEX && updateCacheFrequencyProp != null) {
                updateCacheFrequency = updateCacheFrequencyProp;
            }

            String physicalTableName = (String) TableProperty.PHYSICAL_TABLE_NAME.getValue(tableProps);
            String autoPartitionSeq = (String) TableProperty.AUTO_PARTITION_SEQ.getValue(tableProps);
            Long guidePostsWidth = (Long) TableProperty.GUIDE_POSTS_WIDTH.getValue(tableProps);

            // We only allow setting guide post width for a base table
            if (guidePostsWidth != null && tableType != PTableType.TABLE) {
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.CANNOT_SET_GUIDE_POST_WIDTH)
                        .setSchemaName(schemaName).setTableName(tableName).build().buildException();
            }

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
            TransactionFactory.Provider transactionProviderProp = (TransactionFactory.Provider) TableProperty.TRANSACTION_PROVIDER.getValue(tableProps);
            if ((transactionalProp != null || transactionProviderProp != null) && parent != null) {
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.ONLY_TABLE_MAY_BE_DECLARED_TRANSACTIONAL)
                .setSchemaName(schemaName).setTableName(tableName)
                .build().buildException();
            }
            if (parent == null) {
                boolean transactional;
                if (transactionProviderProp != null) {
                    transactional = true;
                } else if (transactionalProp == null) {
                    transactional = connection.getQueryServices().getProps().getBoolean(
                                    QueryServices.DEFAULT_TABLE_ISTRANSACTIONAL_ATTRIB,
                                    QueryServicesOptions.DEFAULT_TABLE_ISTRANSACTIONAL);
                } else {
                    transactional = transactionalProp;
                }
                if (transactional) {
                    if (transactionProviderProp == null) {
                        transactionProvider = (TransactionFactory.Provider)TableProperty.TRANSACTION_PROVIDER.getValue(
                                connection.getQueryServices().getProps().get(
                                        QueryServices.DEFAULT_TRANSACTION_PROVIDER_ATTRIB,
                                        QueryServicesOptions.DEFAULT_TRANSACTION_PROVIDER));
                    } else {
                        transactionProvider = transactionProviderProp;
                    }
                }
            }
            boolean transactionsEnabled = connection.getQueryServices().getProps().getBoolean(
                                            QueryServices.TRANSACTIONS_ENABLED,
                                            QueryServicesOptions.DEFAULT_TRANSACTIONS_ENABLED);
            // can't create a transactional table if transactions are not enabled
            if (!transactionsEnabled && transactionProvider != null) {
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.CANNOT_CREATE_TXN_TABLE_IF_TXNS_DISABLED)
                .setSchemaName(schemaName).setTableName(tableName)
                .build().buildException();
            }
            // can't create a transactional table if it has a row timestamp column
            if (pkConstraint.getNumColumnsWithRowTimestamp() > 0 && transactionProvider != null) {
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.CANNOT_CREATE_TXN_TABLE_WITH_ROW_TIMESTAMP)
                .setSchemaName(schemaName).setTableName(tableName)
                .build().buildException();
            }
            if (TableProperty.TTL.getValue(commonFamilyProps) != null 
                    && transactionProvider != null 
                    && transactionProvider.getTransactionProvider().isUnsupported(PhoenixTransactionProvider.Feature.SET_TTL)) {
                throw new SQLExceptionInfo.Builder(PhoenixTransactionProvider.Feature.SET_TTL.getCode())
                .setMessage(transactionProvider.name())
                .setSchemaName(schemaName)
                .setTableName(tableName)
                .build()
                .buildException();
            }

            // Put potentially inferred value into tableProps as it's used by the createTable call below
            // to determine which coprocessors to install on the new table.
            tableProps.put(PhoenixDatabaseMetaData.TRANSACTION_PROVIDER, transactionProvider);
            if (transactionProvider != null) {
                // If TTL set, use transaction context TTL property name instead
                // Note: After PHOENIX-6627, is PhoenixTransactionContext.PROPERTY_TTL still useful?
                Object ttl = commonFamilyProps.remove(ColumnFamilyDescriptorBuilder.TTL);
                if (ttl != null) {
                    commonFamilyProps.put(PhoenixTransactionContext.PROPERTY_TTL, ttl);
                }
            }

            Boolean useStatsForParallelizationProp =
                    (Boolean) TableProperty.USE_STATS_FOR_PARALLELIZATION.getValue(tableProps);

            boolean sharedTable = statement.getTableType() == PTableType.VIEW || allocateIndexId;
            if (transactionProvider != null) {
                // We turn on storeNulls for transactional tables for compatibility. This was required
                // when Tephra was a supported txn engine option. After PHOENIX-6627, this may no longer
                // be necessary.
                // Tephra would have converted normal delete markers on the server which could mess up
                // our secondary index code as the changes get committed prior to the
                // maintenance code being able to see the prior state to update the rows correctly.
                // A future tnx engine might do the same?
                if (Boolean.FALSE.equals(storeNullsProp)) {
                    throw new SQLExceptionInfo.Builder(SQLExceptionCode.STORE_NULLS_MUST_BE_TRUE_FOR_TRANSACTIONAL)
                    .setSchemaName(schemaName).setTableName(tableName)
                    .build().buildException();
                }
                storeNulls = true;
                tableProps.put(PhoenixDatabaseMetaData.STORE_NULLS, Boolean.TRUE);

                if (!sharedTable) {
                    Integer maxVersionsProp = (Integer) commonFamilyProps.get(HConstants.VERSIONS);
                    if (maxVersionsProp == null) {
                        if (parent != null) {
                            TableDescriptor desc = connection.getQueryServices().getTableDescriptor(parent.getPhysicalName().getBytes());
                            if (desc != null) {
                                maxVersionsProp = desc.getColumnFamily(SchemaUtil.getEmptyColumnFamily(parent)).getMaxVersions();
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
            timestamp = timestamp==null ? TransactionUtil.getTableTimestamp(connection, transactionProvider != null, transactionProvider) : timestamp;

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

                    phoenixTTL = phoenixTTLProp == null ? parent.getPhoenixTTL() : phoenixTTLProp;

                    disableWAL = (disableWALProp == null ? parent.isWALDisabled() : disableWALProp);
                    defaultFamilyName = parent.getDefaultFamilyName() == null ? null : parent.getDefaultFamilyName().getString();
                    // TODO PHOENIX-4766 Add an options to stop sending parent metadata when creating views
                    List<PColumn> allColumns = parent.getColumns();
                    if (saltBucketNum != null) { // Don't include salt column in columns, as it should not have it when created
                        allColumns = allColumns.subList(1, allColumns.size());
                    }
                    columns = new LinkedHashMap<PColumn,PColumn>(allColumns.size() + colDefs.size());
                    for (PColumn column : allColumns) {
                        columns.put(column, column);
                    }
                    pkColumns = newLinkedHashSet(parent.getPKColumns());

                    // Add row linking view to its parent 
                    PreparedStatement linkStatement = connection.prepareStatement(CREATE_VIEW_LINK);
                    linkStatement.setString(1, tenantIdStr);
                    linkStatement.setString(2, schemaName);
                    linkStatement.setString(3, tableName);
                    linkStatement.setString(4, parent.getName().getString());
                    linkStatement.setByte(5, LinkType.PARENT_TABLE.getSerializedValue());
                    linkStatement.setString(6, parent.getTenantId() == null ? null : parent.getTenantId().getString());
                    linkStatement.execute();
                    // Add row linking parent to view
                    // TODO From 4.16 write the child links to SYSTEM.CHILD_LINK directly 
                    linkStatement = connection.prepareStatement(CREATE_CHILD_LINK);
                    linkStatement.setString(1, parent.getTenantId() == null ? null : parent.getTenantId().getString());
                    linkStatement.setString(2, parent.getSchemaName() == null ? null : parent.getSchemaName().getString());
                    linkStatement.setString(3, parent.getTableName().getString());
                    linkStatement.setString(4, tenantIdStr);
                    linkStatement.setString(5, SchemaUtil.getTableName(schemaName, tableName));
                    linkStatement.setByte(6, LinkType.CHILD_TABLE.getSerializedValue());
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
                    || (!physicalNames.get(0).getString().equals(SchemaUtil.getTableName(schemaName, tableName))
                    && !physicalNames.get(0).getString().equals(SchemaUtil.getPhysicalHBaseTableName(
                        schemaName, tableName, isNamespaceMapped).getString()))) {
                    // Add row linking from data table row to physical table row
                    PreparedStatement linkStatement = connection.prepareStatement(CREATE_LINK);
                    for (PName physicalName : physicalNames) {
                        linkStatement.setString(1, tenantIdStr);
                        linkStatement.setString(2, schemaName);
                        linkStatement.setString(3, tableName);
                        linkStatement.setString(4, physicalName.getString());
                        linkStatement.setByte(5, LinkType.PHYSICAL_TABLE.getSerializedValue());
                        if (tableType == PTableType.VIEW) {
                            if (parent.getType() == PTableType.TABLE) {
                                linkStatement.setString(4, SchemaUtil.getTableName(parent.getSchemaName().getString(),parent.getTableName().getString()));
                                linkStatement.setLong(6, parent.getSequenceNumber());
                            } else { //This is a grandchild view, find the physical base table
                                PTable logicalTable = connection.getTable(new PTableKey(null, SchemaUtil.replaceNamespaceSeparator(physicalName)));
                                linkStatement.setString(4, SchemaUtil.getTableName(logicalTable.getSchemaName().getString(),logicalTable.getTableName().getString()));
                                linkStatement.setLong(6, logicalTable.getSequenceNumber());
                            }
                            // Set link to logical name
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
            Map<String, Integer> changedCqCounters = new HashMap<>(colDefs.size());
            // Check for duplicate column qualifiers
            Map<String, Set<Integer>> inputCqCounters = new HashMap<>();
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
                    viewPhysicalTable = connection.getTable(physicalNames.get(0).getString());
                    immutableStorageScheme = viewPhysicalTable.getImmutableStorageScheme();
                    encodingScheme = viewPhysicalTable.getEncodingScheme();
					if (EncodedColumnsUtil.usesEncodedColumnNames(viewPhysicalTable)) {
                        cqCounter  = viewPhysicalTable.getEncodedCQCounter();
                    }
                }
            }
            // System tables have hard-coded column qualifiers. So we can't use column encoding for them.
            else if (!SchemaUtil.isSystemTable(Bytes.toBytes(SchemaUtil.getTableName(schemaName, tableName)))|| SchemaUtil.isLogTable(schemaName, tableName)) {
                /*
                 * Indexes inherit the storage scheme of the parent data tables. Otherwise, we always attempt to 
                 * create tables with encoded column names. 
                 * 
                 * Also of note is the case with shared indexes i.e. local indexes and view indexes. In these cases, 
                 * column qualifiers for covered columns don't have to be unique because rows of the logical indexes are 
                 * partitioned by the virtue of indexId present in the row key. As such, different shared indexes can use
                 * potentially overlapping column qualifiers.
                 * 
                 */
                if (parent != null) {
                    Byte encodingSchemeSerializedByte = (Byte) TableProperty.COLUMN_ENCODED_BYTES.getValue(tableProps);
                    // Table has encoding scheme defined
                    if (encodingSchemeSerializedByte != null) {
                        encodingScheme = getEncodingScheme(tableProps, schemaName, tableName, transactionProvider);
                    } else {
                        encodingScheme = parent.getEncodingScheme();
                    }

                    ImmutableStorageScheme immutableStorageSchemeProp = (ImmutableStorageScheme) TableProperty.IMMUTABLE_STORAGE_SCHEME.getValue(tableProps);
                    if (immutableStorageSchemeProp == null) {
                        immutableStorageScheme = parent.getImmutableStorageScheme();
                    } else {
                        checkImmutableStorageSchemeForIndex(immutableStorageSchemeProp, schemaName, tableName, transactionProvider);
                        immutableStorageScheme = immutableStorageSchemeProp;
                    }

                    if (immutableStorageScheme == SINGLE_CELL_ARRAY_WITH_OFFSETS) {
                        if (encodingScheme == NON_ENCODED_QUALIFIERS) {
                            if (encodingSchemeSerializedByte != null) {
                                // encoding scheme is set as non-encoded on purpose, so we should fail
                                throw new SQLExceptionInfo.Builder(SQLExceptionCode.INVALID_IMMUTABLE_STORAGE_SCHEME_AND_COLUMN_QUALIFIER_BYTES)
                                        .setSchemaName(schemaName).setTableName(tableName).build().buildException();
                            } else {
                                // encoding scheme is inherited from parent but it is not compatible with Single Cell.
                                encodingScheme =
                                        QualifierEncodingScheme.fromSerializedValue(
                                                (byte) QueryServicesOptions.DEFAULT_COLUMN_ENCODED_BYTES);
                            }
                        }
                    }

                    if (parent.getImmutableStorageScheme() == SINGLE_CELL_ARRAY_WITH_OFFSETS && immutableStorageScheme == ONE_CELL_PER_COLUMN) {
                        throw new SQLExceptionInfo.Builder(
                                SQLExceptionCode.INVALID_IMMUTABLE_STORAGE_SCHEME_CHANGE)
                                .setSchemaName(schemaName).setTableName(tableName).build()
                                .buildException();
                    }
                    LOGGER.info(String.format("STORAGE--ENCODING: %s--%s", immutableStorageScheme, encodingScheme));
                } else {
                    encodingScheme = getEncodingScheme(tableProps, schemaName, tableName, transactionProvider);

                    ImmutableStorageScheme immutableStorageSchemeProp =
                            (ImmutableStorageScheme) TableProperty.IMMUTABLE_STORAGE_SCHEME
                                    .getValue(tableProps);
                    if (immutableStorageSchemeProp == null) {
                        // Ignore default if transactional and column encoding is not supported
                        if (transactionProvider == null ||
                                !transactionProvider.getTransactionProvider().isUnsupported(
                                        PhoenixTransactionProvider.Feature.COLUMN_ENCODING)) {
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
                                if (isImmutableRows) {
                                    immutableStorageScheme =
                                            ImmutableStorageScheme
                                                    .valueOf(connection
                                                            .getQueryServices()
                                                            .getProps()
                                                            .get(
                                                                    QueryServices.DEFAULT_IMMUTABLE_STORAGE_SCHEME_ATTRIB,
                                                                    QueryServicesOptions.DEFAULT_IMMUTABLE_STORAGE_SCHEME));
                                } else {
                                    immutableStorageScheme = ONE_CELL_PER_COLUMN;
                                }
                            }
                        }
                    } else {
                        immutableStorageScheme = isImmutableRows ? immutableStorageSchemeProp : ONE_CELL_PER_COLUMN;
                        checkImmutableStorageSchemeForIndex(immutableStorageScheme, schemaName, tableName, transactionProvider);
                    }
                    if (immutableStorageScheme != ONE_CELL_PER_COLUMN
                            && encodingScheme == NON_ENCODED_QUALIFIERS) {
                        throw new SQLExceptionInfo.Builder(
                                SQLExceptionCode.INVALID_IMMUTABLE_STORAGE_SCHEME_AND_COLUMN_QUALIFIER_BYTES)
                                .setSchemaName(schemaName).setTableName(tableName).build()
                                .buildException();
                    }
                }
                cqCounter = encodingScheme != NON_ENCODED_QUALIFIERS ? new EncodedCQCounter() : NULL_COUNTER;
                if (encodingScheme != NON_ENCODED_QUALIFIERS && statement.getFamilyCQCounters() != null)
                {
                    for (Map.Entry<String, Integer> cq : statement.getFamilyCQCounters().entrySet()) {
                        if (cq.getValue() < QueryConstants.ENCODED_CQ_COUNTER_INITIAL_VALUE) {
                            throw new SQLExceptionInfo.Builder(SQLExceptionCode.INVALID_CQ)
                                    .setSchemaName(schemaName)
                                    .setTableName(tableName).build().buildException();
                        }
                        cqCounter.setValue(cq.getKey(), cq.getValue());
                        changedCqCounters.put(cq.getKey(), cqCounter.getNextQualifier(cq.getKey()));
                        inputCqCounters.putIfAbsent(cq.getKey(), new HashSet<Integer>());
                    }
                }
            }

            boolean wasPKDefined = false;
            // Keep track of all columns that are newly added to a view
            Set<Integer> viewNewColumnPositions =
                    Sets.newHashSetWithExpectedSize(colDefs.size());
            Set<String> pkColumnNames = new HashSet<>();
            for (PColumn pColumn : pkColumns) {
                pkColumnNames.add(pColumn.getName().toString());
            }
            for (ColumnDef colDef : colDefs) {
                rowTimeStampColumnAlreadyFound = checkAndValidateRowTimestampCol(colDef, pkConstraint, rowTimeStampColumnAlreadyFound, tableType);
                if (colDef.isPK()) { // i.e. the column is declared as CREATE TABLE COLNAME DATATYPE PRIMARY KEY...
                    if (wasPKDefined) {
                        throw new SQLExceptionInfo.Builder(SQLExceptionCode.PRIMARY_KEY_ALREADY_EXISTS)
                            .setColumnName(colDef.getColumnDefName().getColumnName()).build().buildException();
                    }
                    wasPKDefined = true;
                } else {
                    // do not allow setting NOT-NULL constraint on non-primary columns.
                    if (  !colDef.isNull() && !isImmutableRows &&
                        ( wasPKDefined || !isPkColumn(pkConstraint, colDef))) {
                            throw new SQLExceptionInfo.Builder(SQLExceptionCode.KEY_VALUE_NOT_NULL)
                                .setSchemaName(schemaName)
                                .setTableName(tableName)
                                .setColumnName(colDef.getColumnDefName().getColumnName()).build().buildException();
                    }
                }
                ColumnName columnDefName = colDef.getColumnDefName();
                String colDefFamily = columnDefName.getFamilyName();
                boolean isPkColumn = isPkColumn(pkConstraint, colDef);
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
                // Use position as column qualifier if APPEND_ONLY_SCHEMA to prevent gaps in
                // the column encoding (PHOENIX-4737).
                Integer encodedCQ = null;
                if (!isPkColumn) {
                    if (colDef.getEncodedQualifier() != null && encodingScheme != NON_ENCODED_QUALIFIERS) {
                        if (cqCounter.getNextQualifier(cqCounterFamily) > ENCODED_CQ_COUNTER_INITIAL_VALUE &&
                                !inputCqCounters.containsKey(cqCounterFamily)) {
                            throw new SQLExceptionInfo.Builder(SQLExceptionCode.MISSING_CQ)
                                    .setSchemaName(schemaName)
                                    .setTableName(tableName).build().buildException();
                        }

                        if (statement.getFamilyCQCounters() == null ||
                                statement.getFamilyCQCounters().get(cqCounterFamily) == null) {
                            if (colDef.getEncodedQualifier() >= cqCounter.getNextQualifier(cqCounterFamily)) {
                                cqCounter.setValue(cqCounterFamily, colDef.getEncodedQualifier());
                                cqCounter.increment(cqCounterFamily);
                            }
                            changedCqCounters.put(cqCounterFamily, cqCounter.getNextQualifier(cqCounterFamily));
                        }

                        encodedCQ = colDef.getEncodedQualifier();
                        if (encodedCQ < QueryConstants.ENCODED_CQ_COUNTER_INITIAL_VALUE ||
                                encodedCQ >= cqCounter.getNextQualifier(cqCounterFamily)) {
                            throw new SQLExceptionInfo.Builder(SQLExceptionCode.INVALID_CQ)
                                    .setSchemaName(schemaName)
                                    .setTableName(tableName).build().buildException();
                        }

                        inputCqCounters.putIfAbsent(cqCounterFamily, new HashSet<Integer>());
                        Set<Integer> familyCounters = inputCqCounters.get(cqCounterFamily);
                        if (!familyCounters.add(encodedCQ)) {
                            throw new SQLExceptionInfo.Builder(SQLExceptionCode.DUPLICATE_CQ)
                                    .setSchemaName(schemaName)
                                    .setTableName(tableName).build().buildException();
                        }
                    } else {
                        if (inputCqCounters.containsKey(cqCounterFamily)) {
                            throw new SQLExceptionInfo.Builder(SQLExceptionCode.MISSING_CQ)
                                    .setSchemaName(schemaName)
                                    .setTableName(tableName).build().buildException();
                        }

                        if (isAppendOnlySchema) {
                            encodedCQ = Integer.valueOf(ENCODED_CQ_COUNTER_INITIAL_VALUE + position);
                        } else {
                            encodedCQ = cqCounter.getNextQualifier(cqCounterFamily);
                        }
                    }
                }
                byte[] columnQualifierBytes = null;
                try {
                    columnQualifierBytes = EncodedColumnsUtil.getColumnQualifierBytes(columnDefName.getColumnName(), encodedCQ, encodingScheme, isPkColumn);
                }
                catch (QualifierOutOfRangeException e) {
                    throw new SQLExceptionInfo.Builder(SQLExceptionCode.MAX_COLUMNS_EXCEEDED)
                    .setSchemaName(schemaName)
                    .setTableName(tableName).build().buildException();
                }
                PColumn column = newColumn(position++, colDef, pkConstraint, defaultFamilyName, false, columnQualifierBytes, isImmutableRows);
                if (!isAppendOnlySchema && colDef.getEncodedQualifier() == null
                        && cqCounter.increment(cqCounterFamily)) {
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
                        throwIfLastPKOfParentIsVariableLength(parent, schemaName, tableName, colDef);
                    }
                    if (!pkColumns.add(column)) {
                        throw new ColumnAlreadyExistsException(schemaName, tableName, column.getName().getString());
                    }
                }
                // check for duplicate column
                if (isDuplicateColumn(columns, pkColumnNames, column)) {
                    throw new ColumnAlreadyExistsException(schemaName, tableName,
                            column.getName().getString());
                } else if (tableType == VIEW) {
                    viewNewColumnPositions.add(column.getPosition());
                }
                if (isPkColumn) {
                    pkColumnNames.add(column.getName().toString());
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
            if (!wasPKDefined && pkColumnsNames.isEmpty() && tableType != PTableType.VIEW && viewType != ViewType.MAPPED) {
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

            List<Pair<byte[],Map<String,Object>>> familyPropList = Lists.newArrayListWithExpectedSize(familyNames.size());
            populateFamilyPropsList(familyNames, commonFamilyProps, statement, defaultFamilyName, isLocalIndex, familyPropList);

            // Bootstrapping for our SYSTEM.TABLE that creates itself before it exists
            if (SchemaUtil.isMetaTable(schemaName,tableName)) {
                // TODO: what about stats for system catalog?
                PName newSchemaName = PNameFactory.newName(schemaName);
                // Column names and qualifiers and hardcoded for system tables.
                PTable table = new PTableImpl.Builder()
                        .setType(tableType)
                        .setTimeStamp(MetaDataProtocol.MIN_TABLE_TIMESTAMP)
                        .setIndexDisableTimestamp(0L)
                        .setSequenceNumber(PTable.INITIAL_SEQ_NUM)
                        .setImmutableRows(isImmutableRows)
                        .setDisableWAL(Boolean.TRUE.equals(disableWAL))
                        .setMultiTenant(false)
                        .setStoreNulls(false)
                        .setViewIndexIdType(viewIndexIdType)
                        .setIndexType(indexType)
                        .setUpdateCacheFrequency(0)
                        .setNamespaceMapped(isNamespaceMapped)
                        .setAutoPartitionSeqName(autoPartitionSeq)
                        .setAppendOnlySchema(isAppendOnlySchema)
                        .setImmutableStorageScheme(ONE_CELL_PER_COLUMN)
                        .setQualifierEncodingScheme(NON_ENCODED_QUALIFIERS)
                        .setBaseColumnCount(QueryConstants.BASE_TABLE_BASE_COLUMN_COUNT)
                        .setEncodedCQCounter(PTable.EncodedCQCounter.NULL_COUNTER)
                        .setUseStatsForParallelization(true)
                        .setExcludedColumns(ImmutableList.<PColumn>of())
                        .setTenantId(tenantId)
                        .setSchemaName(newSchemaName)
                        .setTableName(PNameFactory.newName(tableName))
                        .setPkName(PNameFactory.newName(QueryConstants.SYSTEM_TABLE_PK_NAME))
                        .setDefaultFamilyName(defaultFamilyName == null ? null :
                                PNameFactory.newName(defaultFamilyName))
                        .setRowKeyOrderOptimizable(true)
                        .setIndexes(Collections.<PTable>emptyList())
                        .setPhysicalNames(ImmutableList.<PName>of())
                        .setColumns(columns.values())
                        .setPhoenixTTL(PHOENIX_TTL_NOT_DEFINED)
                        .setPhoenixTTLHighWaterMark(MIN_PHOENIX_TTL_HWM)
                        .setIndexWhere(statement.getWhereClause() == null ? null
                                : statement.getWhereClause().toString())
                        .build();
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
            boolean isRegularView = (tableType == PTableType.VIEW && viewType!=ViewType.MAPPED);
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

                    // if the base table column is referenced in the view
                    // or if we are adding a new column during view creation
                    if (isViewColumnReferenced.get(columnPosition) ||
                            viewNewColumnPositions.contains(
                                    columnPosition)) {
                        // acquire the mutex using the global physical table
                        // name to prevent this column from being dropped
                        // while the view is being created or to prevent
                        // a conflicting column from being added to a parent
                        // in case the view creation adds new columns
                        boolean acquiredMutex = writeCell(
                                null,
                                parentPhysicalSchemaName,
                                parentPhysicalTableName,
                                column.toString());
                        if (!acquiredMutex) {
                            throw new ConcurrentTableMutationException(
                                    parentPhysicalSchemaName,
                                    parentPhysicalTableName);
                        }
                        acquiredColumnMutexSet.add(column.toString());
                    }
                }
                Short keySeq = SchemaUtil.isPKColumn(column) ? ++nextKeySeq : null;
                // Prior to PHOENIX-3534 we were sending the parent table column metadata while creating a
                // child view, now that we combine columns by resolving the parent table hierarchy we
                // don't need to include the parent table columns while creating a view
                // If QueryServices.ALLOW_SPLITTABLE_SYSTEM_CATALOG_ROLLBACK is true we continue
                // to store the parent table column metadata along with the child view metadata
                // so that we can rollback the upgrade if required.
                if (allowSystemCatalogRollback || !isRegularView
                        || columnPosition >= baseTableColumnCount) {
                    addColumnMutation(connection, schemaName, tableName, column, parentTableName,
                            pkName, keySeq, saltBucketNum != null);
                    columnMetadata.addAll(connection.getMutationState().toMutations(timestamp).next().getSecond());
                    connection.rollback();
                }
            }

            // add the columns in reverse order since we reverse the list later
            Collections.reverse(columnMetadata);
            tableMetaData.addAll(columnMetadata);
            String dataTableName = parent == null || tableType == PTableType.VIEW ? null : parent.getTableName().getString();
            PIndexState defaultCreateState;
            String defaultCreateStateString = connection.getClientInfo(INDEX_CREATE_DEFAULT_STATE);
            if (defaultCreateStateString == null)  {
                defaultCreateStateString = connection.getQueryServices().getConfiguration().get(
                     INDEX_CREATE_DEFAULT_STATE, QueryServicesOptions.DEFAULT_CREATE_INDEX_STATE);
            }
            defaultCreateState = PIndexState.valueOf(defaultCreateStateString);
            if (defaultCreateState == PIndexState.CREATE_DISABLE) {
                if  (indexType == IndexType.LOCAL || sharedTable) {
                    defaultCreateState = PIndexState.BUILDING;
                }
            }
            PIndexState indexState = parent == null || tableType == PTableType.VIEW  ? null : defaultCreateState;
            if (indexState == null && tableProps.containsKey(INDEX_STATE)) {
                indexState = PIndexState.fromSerializedValue(tableProps.get(INDEX_STATE).toString());
            }
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
            if (transactionProvider == null) {
                tableUpsert.setNull(20, Types.TINYINT);
            } else {
                tableUpsert.setByte(20, transactionProvider.getCode());
            }
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
            if (useStatsForParallelizationProp == null) {
                tableUpsert.setNull(28, Types.BOOLEAN);
            } else {
                tableUpsert.setBoolean(28, useStatsForParallelizationProp);
            }
            if (indexType == IndexType.LOCAL ||
                    (parent != null && parent.getType() == PTableType.VIEW
                            && tableType == PTableType.INDEX)) {
                tableUpsert.setInt(29, viewIndexIdType.getSqlType());
            } else {
                tableUpsert.setNull(29, Types.NULL);
            }

            if (phoenixTTL == null || phoenixTTL == PHOENIX_TTL_NOT_DEFINED) {
                tableUpsert.setNull(30, Types.BIGINT);
                tableUpsert.setNull(31, Types.BIGINT);
            }
            else {
                tableUpsert.setLong(30, phoenixTTL);
                tableUpsert.setLong(31, phoenixTTLHighWaterMark);
            }

            if (isChangeDetectionEnabledProp == null) {
                tableUpsert.setNull(32, Types.BOOLEAN);
            } else {
                tableUpsert.setBoolean(32, isChangeDetectionEnabledProp);
            }

            if (physicalTableName == null){
                tableUpsert.setNull(33, Types.VARCHAR);
            } else {
                tableUpsert.setString(33, physicalTableName);
            }

            if (schemaVersion == null) {
                tableUpsert.setNull(34, Types.VARCHAR);
            } else {
                tableUpsert.setString(34, schemaVersion);
            }

            if (streamingTopicName == null) {
                tableUpsert.setNull(35, Types.VARCHAR);
            } else {
                tableUpsert.setString(35, streamingTopicName);
            }
            if (tableType == INDEX && statement.getWhereClause() != null) {
                tableUpsert.setString(36, statement.getWhereClause().toString());
            } else {
                tableUpsert.setNull(36, Types.VARCHAR);
            }
            tableUpsert.execute();

            if (asyncCreatedDate != null) {
                PreparedStatement setAsync = connection.prepareStatement(SET_ASYNC_CREATED_DATE);
                setAsync.setString(1, tenantIdStr);
                setAsync.setString(2, schemaName);
                setAsync.setString(3, tableName);
                setAsync.setDate(4, asyncCreatedDate);
                setAsync.execute();
            } else {
                Date syncCreatedDate = new Date(EnvironmentEdgeManager.currentTimeMillis());
                PreparedStatement setSync = connection.prepareStatement(SET_INDEX_SYNC_CREATED_DATE);
                setSync.setString(1, tenantIdStr);
                setSync.setString(2, schemaName);
                setSync.setString(3, tableName);
                setSync.setDate(4, syncCreatedDate);
                setSync.execute();
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

			// Modularized this code for unit testing
            PName parentName = physicalNames !=null && physicalNames.size() > 0 ? physicalNames.get(0) : null;
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("createTable tableName=" + tableName + " parent=" + (parent == null ? "" : parent.getTableName() + "-" + parent.getPhysicalName()) + " parent physical=" + parentName + "-" + (physicalNames.size() > 0 ? physicalNames.get(0) : "null") + " viewType " + viewType + allocateIndexId);
            }
            MetaDataMutationResult result = connection.getQueryServices().createTable(tableMetaData
                ,viewType == ViewType.MAPPED || allocateIndexId ? physicalNames.get(0).getBytes()
                : null, tableType, tableProps, familyPropList, splits, isNamespaceMapped,
                allocateIndexId, UpgradeUtil.isNoUpgradeSet(connection.getClientInfo()), parent);
            MutationCode code = result.getMutationCode();
            try {
                if (code != MutationCode.TABLE_NOT_FOUND) {
                    boolean tableAlreadyExists = handleCreateTableMutationCode(result, code, statement,
                            schemaName, tableName, parent);
                    if (tableAlreadyExists) {
                        return null;
                    }
                }
                // If the parent table of the view has the auto partition sequence name attribute,
                // set the view statement and relevant partition column attributes correctly
                if (parent != null && parent.getAutoPartitionSeqName() != null) {
                    final PColumn autoPartitionCol = parent.getPKColumns().get(MetaDataUtil
                            .getAutoPartitionColIndex(parent));
                    final Long autoPartitionNum = Long.valueOf(result.getAutoPartitionNum());
                    columns.put(autoPartitionCol, new DelegateColumn(autoPartitionCol) {
                        @Override
                        public byte[] getViewConstant() {
                            PDataType dataType = autoPartitionCol.getDataType();
                            Object val = dataType.toObject(autoPartitionNum, PLong.INSTANCE);
                            byte[] bytes = new byte[dataType.getByteSize() + 1];
                            dataType.toBytes(val, bytes, 0);
                            return bytes;
                        }

                        @Override
                        public boolean isViewReferenced() {
                            return true;
                        }
                    });
                    String viewPartitionClause = QueryUtil.getViewPartitionClause(MetaDataUtil
                            .getAutoPartitionColumnName(parent), autoPartitionNum);
                    if (viewStatement != null) {
                        viewStatement = viewStatement + " AND " + viewPartitionClause;
                    } else {
                        viewStatement = QueryUtil.getViewStatement(parent.getSchemaName().getString(),
                                parent.getTableName().getString(), viewPartitionClause);
                    }
                }
                PName newSchemaName = PNameFactory.newName(schemaName);
                /*
                 * It doesn't hurt for the PTable of views to have the cqCounter. However, views always
                 * rely on the parent table's counter to dole out encoded column qualifiers. So setting
                 * the counter as NULL_COUNTER for extra safety.
                 */
                EncodedCQCounter cqCounterToBe = tableType == PTableType.VIEW ? NULL_COUNTER : cqCounter;
                PTable table = new PTableImpl.Builder()
                        .setType(tableType)
                        .setState(indexState)
                        .setTimeStamp(timestamp != null ? timestamp : result.getMutationTime())
                        .setIndexDisableTimestamp(0L)
                        .setSequenceNumber(PTable.INITIAL_SEQ_NUM)
                        .setImmutableRows(isImmutableRows)
                        .setViewStatement(viewStatement)
                        .setDisableWAL(Boolean.TRUE.equals(disableWAL))
                        .setMultiTenant(multiTenant)
                        .setStoreNulls(storeNulls)
                        .setViewType(viewType)
                        .setViewIndexIdType(viewIndexIdType)
                        .setViewIndexId(result.getViewIndexId())
                        .setIndexType(indexType)
                        .setTransactionProvider(transactionProvider)
                        .setUpdateCacheFrequency(updateCacheFrequency)
                        .setNamespaceMapped(isNamespaceMapped)
                        .setAutoPartitionSeqName(autoPartitionSeq)
                        .setAppendOnlySchema(isAppendOnlySchema)
                        .setImmutableStorageScheme(immutableStorageScheme)
                        .setQualifierEncodingScheme(encodingScheme)
                        .setBaseColumnCount(baseTableColumnCount)
                        .setEncodedCQCounter(cqCounterToBe)
                        .setUseStatsForParallelization(useStatsForParallelizationProp)
                        .setExcludedColumns(ImmutableList.<PColumn>of())
                        .setTenantId(tenantId)
                        .setSchemaName(newSchemaName)
                        .setTableName(PNameFactory.newName(tableName))
                        .setPkName(pkName == null ? null : PNameFactory.newName(pkName))
                        .setDefaultFamilyName(defaultFamilyName == null ?
                                null : PNameFactory.newName(defaultFamilyName))
                        .setRowKeyOrderOptimizable(rowKeyOrderOptimizable)
                        .setBucketNum(saltBucketNum)
                        .setIndexes(Collections.<PTable>emptyList())
                        .setParentSchemaName((parent == null) ? null : parent.getSchemaName())
                        .setParentTableName((parent == null) ? null : parent.getTableName())
                        .setPhysicalNames(ImmutableList.copyOf(physicalNames))
                        .setColumns(columns.values())
                        .setPhoenixTTL(phoenixTTL == null ? PHOENIX_TTL_NOT_DEFINED : phoenixTTL)
                        .setPhoenixTTLHighWaterMark(phoenixTTLHighWaterMark == null ? MIN_PHOENIX_TTL_HWM : phoenixTTLHighWaterMark)
                        .setViewModifiedUpdateCacheFrequency(tableType == PTableType.VIEW &&
                                parent != null &&
                                parent.getUpdateCacheFrequency() != updateCacheFrequency)
                        .setViewModifiedUseStatsForParallelization(tableType == PTableType.VIEW &&
                                parent != null &&
                                parent.useStatsForParallelization()
                                        != useStatsForParallelizationProp)
                        .setViewModifiedPhoenixTTL(tableType == PTableType.VIEW &&
                                parent != null && phoenixTTL != null &&
                                parent.getPhoenixTTL() != phoenixTTL)
                        .setLastDDLTimestamp(result.getTable() != null ?
                                result.getTable().getLastDDLTimestamp() : null)
                        .setIsChangeDetectionEnabled(isChangeDetectionEnabledProp)
                        .setSchemaVersion(schemaVersion)
                        .setExternalSchemaId(result.getTable() != null ?
                        result.getTable().getExternalSchemaId() : null)
                        .setStreamingTopicName(streamingTopicName)
                        .setIndexWhere(statement.getWhereClause() == null ? null
                                : statement.getWhereClause().toString())
                        .build();
                result = new MetaDataMutationResult(code, result.getMutationTime(), table, true);
                addTableToCache(result);
                return table;
            } catch (Throwable e) {
                TableMetricsManager.updateMetricsForSystemCatalogTableMethod(tableNameNode.toString(),
                        NUM_METADATA_LOOKUP_FAILURES, 1);
                throw e;
            }
        } finally {
            connection.setAutoCommit(wasAutoCommit);
            deleteMutexCells(parentPhysicalSchemaName, parentPhysicalTableName,
                    acquiredColumnMutexSet);
        }
    }

    private boolean isDuplicateColumn(LinkedHashMap<PColumn, PColumn> columns,
            Set<String> pkColumnNames, PColumn column) {
        // either column name is same within same CF or column name within
        // default CF is same as any of PK column
        return columns.put(column, column) != null
                || (column.getFamilyName() != null
                && DEFAULT_COLUMN_FAMILY.equals(column.getFamilyName().toString())
                && pkColumnNames.contains(column.getName().toString()));
    }

    private void verifyChangeDetectionTableType(PTableType tableType, Boolean isChangeDetectionEnabledProp) throws SQLException {
        if (isChangeDetectionEnabledProp != null && isChangeDetectionEnabledProp) {
            if (tableType != TABLE && tableType != VIEW) {
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.CHANGE_DETECTION_SUPPORTED_FOR_TABLES_AND_VIEWS_ONLY)
                        .build().buildException();
            }
        }
    }

    private QualifierEncodingScheme getEncodingScheme(Map<String, Object> tableProps, String schemaName, String tableName, TransactionFactory.Provider transactionProvider)
            throws SQLException {
        QualifierEncodingScheme encodingScheme = null;
        Byte encodingSchemeSerializedByte = (Byte) TableProperty.COLUMN_ENCODED_BYTES.getValue(tableProps);
        if (encodingSchemeSerializedByte == null) {
            if (tableProps.containsKey(ENCODING_SCHEME)) {
                encodingSchemeSerializedByte = QualifierEncodingScheme.valueOf(((String) tableProps.get(ENCODING_SCHEME))).getSerializedMetadataValue();
            }
        }
        if (encodingSchemeSerializedByte == null) {
            // Ignore default if transactional and column encoding is not supported (as with OMID)
            if (transactionProvider == null || !transactionProvider.getTransactionProvider().isUnsupported(PhoenixTransactionProvider.Feature.COLUMN_ENCODING) ) {
                encodingSchemeSerializedByte = (byte)connection.getQueryServices().getProps().getInt(QueryServices.DEFAULT_COLUMN_ENCODED_BYTES_ATRRIB,
                        QueryServicesOptions.DEFAULT_COLUMN_ENCODED_BYTES);
                encodingScheme =  QualifierEncodingScheme.fromSerializedValue(encodingSchemeSerializedByte);
            } else {
                encodingScheme = NON_ENCODED_QUALIFIERS;
            }
        } else {
            encodingScheme = QualifierEncodingScheme.fromSerializedValue(encodingSchemeSerializedByte);
            if (encodingScheme != NON_ENCODED_QUALIFIERS && transactionProvider != null && transactionProvider.getTransactionProvider()
                    .isUnsupported(PhoenixTransactionProvider.Feature.COLUMN_ENCODING)) {
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.UNSUPPORTED_COLUMN_ENCODING_FOR_TXN_PROVIDER)
                        .setSchemaName(schemaName).setTableName(tableName).setMessage(transactionProvider.name()).build().buildException();
            }
        }

        return encodingScheme;
    }

    private void checkImmutableStorageSchemeForIndex(ImmutableStorageScheme immutableStorageSchemeProp, String schemaName, String tableName, TransactionFactory.Provider transactionProvider)
            throws SQLException {
        if (immutableStorageSchemeProp != ONE_CELL_PER_COLUMN && transactionProvider != null && transactionProvider.getTransactionProvider().isUnsupported(PhoenixTransactionProvider.Feature.COLUMN_ENCODING) ) {
            throw new SQLExceptionInfo.Builder(
                    SQLExceptionCode.UNSUPPORTED_STORAGE_FORMAT_FOR_TXN_PROVIDER)
                    .setSchemaName(schemaName).setTableName(tableName)
                    .setMessage(transactionProvider.name())
                    .build()
                    .buildException();
        }
    }

    /* This method handles mutation codes sent by phoenix server, except for TABLE_NOT_FOUND which
    * is considered to be a success code. If TABLE_ALREADY_EXISTS in hbase, we don't need to add
    * it in ConnectionQueryServices and we return result as true. However if code is
    * NEWER_TABLE_FOUND and it does not exists in statement then we return false because we need to
    * add it ConnectionQueryServices. For other mutation codes it throws related SQLException.
    * If server is throwing new mutation code which is not being handled by client then it throws
    * SQLException stating the server side Mutation code.
    */
    @VisibleForTesting
    public boolean handleCreateTableMutationCode(MetaDataMutationResult result, MutationCode code,
                 CreateTableStatement statement, String schemaName, String tableName,
                 PTable parent) throws SQLException {
        switch(code) {
            case TABLE_ALREADY_EXISTS:
                if (result.getTable() != null) {
                    addTableToCache(result);
                }
                if (!statement.ifNotExists()) {
                    throw new TableAlreadyExistsException(schemaName, tableName, result.getTable());
                }
                return true;
            case NEWER_TABLE_FOUND:
                // Add table to ConnectionQueryServices so it's cached, but don't add
                // it to this connection as we can't see it.
                if (!statement.ifNotExists()) {
                    throw new NewerTableAlreadyExistsException(schemaName, tableName,
                            result.getTable());
                }
                return false;
            case UNALLOWED_TABLE_MUTATION:
                throwsSQLExceptionUtil("CANNOT_MUTATE_TABLE",schemaName,tableName);
            case CONCURRENT_TABLE_MUTATION:
                addTableToCache(result);
                throw new ConcurrentTableMutationException(schemaName, tableName);
            case AUTO_PARTITION_SEQUENCE_NOT_FOUND:
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.AUTO_PARTITION_SEQUENCE_UNDEFINED)
                       .setSchemaName(schemaName).setTableName(tableName).build().buildException();
            case CANNOT_COERCE_AUTO_PARTITION_ID:
            case UNABLE_TO_CREATE_CHILD_LINK:
            case PARENT_TABLE_NOT_FOUND:
            case TABLE_NOT_IN_REGION:
                throwsSQLExceptionUtil(String.valueOf(code), schemaName, tableName);
            case TOO_MANY_INDEXES:
            case UNABLE_TO_UPDATE_PARENT_TABLE:
                throwsSQLExceptionUtil(String.valueOf(code), SchemaUtil.getSchemaNameFromFullName(
                parent.getPhysicalName().getString()),SchemaUtil.getTableNameFromFullName(
                parent.getPhysicalName().getString()));
            case ERROR_WRITING_TO_SCHEMA_REGISTRY:
                throw new SQLExceptionInfo.Builder(ERROR_WRITING_TO_SCHEMA_REGISTRY)
                    .setSchemaName(schemaName).setTableName(tableName).build().buildException();
            default:
                // Cannot use SQLExecptionInfo here since not all mutation codes have their
                // corresponding codes in the enum SQLExceptionCode
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.UNEXPECTED_MUTATION_CODE)
                .setSchemaName(schemaName).setTableName(tableName).setMessage("mutation code: "
                + code).build().buildException();
        }
    }

    private void throwsSQLExceptionUtil(String code,String schemaName, String tableName)
            throws SQLException {
        throw new SQLExceptionInfo.Builder(SQLExceptionCode.valueOf(code))
                .setSchemaName(schemaName).setTableName(tableName).build().buildException();
    }

    private static boolean isPkColumn(PrimaryKeyConstraint pkConstraint, ColumnDef colDef) {
        return colDef.isPK() || (pkConstraint != null && pkConstraint.contains(colDef.getColumnDefName()));
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
		return dropTable(schemaName, tableName, null, statement.getTableType(), statement.ifExists(),
				statement.cascade(), statement.getSkipAddingParentColumns());
    }

    public MutationState dropFunction(DropFunctionStatement statement) throws SQLException {
        return dropFunction(statement.getFunctionName(), statement.ifExists());
    }

    public MutationState dropIndex(DropIndexStatement statement) throws SQLException {
        String schemaName = statement.getTableName().getSchemaName();
        String tableName = statement.getIndexName().getName();
        String parentTableName = statement.getTableName().getTableName();
		return dropTable(schemaName, tableName, parentTableName, PTableType.INDEX, statement.ifExists(), false, false);
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
                    return new MutationState(0, 0, connection);
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
            return new MutationState(0, 0, connection);
        } finally {
            connection.setAutoCommit(wasAutoCommit);
        }
    }

    MutationState dropTable(String schemaName, String tableName, String parentTableName, PTableType tableType,
            boolean ifExists, boolean cascade, boolean skipAddingParentColumns) throws SQLException {
        // Checking the parent table whether exists
        String fullTableName = SchemaUtil.getTableName(schemaName, tableName);
        try {
            PTable ptable = connection.getTable(fullTableName);
            if (parentTableName != null &&!parentTableName.equals(ptable.getParentTableName().getString())) {
                throw new SQLExceptionInfo.Builder(PARENT_TABLE_NOT_FOUND)
                        .setSchemaName(schemaName).setTableName(tableName).build().buildException();
            }
        } catch (TableNotFoundException e) {
            if (!ifExists && !e.isThrownToForceReReadForTransformingTable()) {
                if (tableType == PTableType.INDEX)
                    throw new IndexNotFoundException(e.getSchemaName(),
                            e.getTableName(), e.getTimeStamp());
                throw e;
            }
        }

        connection.rollback();
        boolean wasAutoCommit = connection.getAutoCommit();
        PName tenantId = connection.getTenantId();
        String tenantIdStr = tenantId == null ? null : tenantId.getString();
        try {
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
            case UNABLE_TO_DELETE_CHILD_LINK:
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.UNABLE_TO_DELETE_CHILD_LINK)
                        .setSchemaName(schemaName).setTableName(tableName).build().buildException();
            default:
                connection.removeTable(tenantId, SchemaUtil.getTableName(schemaName, tableName), parentTableName, result.getMutationTime());

                if (table != null) {
                    boolean dropMetaData = connection.getQueryServices().getProps()
                            .getBoolean(DROP_METADATA_ATTRIB, DEFAULT_DROP_METADATA);
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
                                try (Admin admin = connection.getQueryServices().getAdmin()) {
                                    hasViewIndexTable = admin.tableExists(org.apache.hadoop.hbase.TableName.valueOf(viewIndexPhysicalName));
                                } catch (IOException e1) {
                                    // absorbing as it is not critical check
                                }
                            }
                        }
                        if (tableType == PTableType.TABLE
                                && (table.isMultiTenant() || hasViewIndexTable)) {
                            if (hasViewIndexTable) {
                                byte[] viewIndexPhysicalName = MetaDataUtil.getViewIndexPhysicalName(table.getPhysicalName().getBytes());
                                String viewIndexSchemaName = SchemaUtil.getSchemaNameFromFullName(viewIndexPhysicalName);
                                String viewIndexTableName = SchemaUtil.getTableNameFromFullName(viewIndexPhysicalName);
                                PName viewIndexName = PNameFactory.newName(SchemaUtil.getTableName(viewIndexSchemaName, viewIndexTableName));

                                PTable viewIndexTable = new PTableImpl.Builder()
                                        .setName(viewIndexName)
                                        .setKey(new PTableKey(tenantId, viewIndexName.getString()))
                                        .setSchemaName(PNameFactory.newName(viewIndexSchemaName))
                                        .setTableName(PNameFactory.newName(viewIndexTableName))
                                        .setType(PTableType.VIEW)
                                        .setViewType(ViewType.MAPPED)
                                        .setTimeStamp(ts)
                                        .setPkColumns(Collections.<PColumn>emptyList())
                                        .setAllColumns(Collections.<PColumn>emptyList())
                                        .setRowKeySchema(RowKeySchema.EMPTY_SCHEMA)
                                        .setIndexes(Collections.<PTable>emptyList())
                                        .setFamilyAttributes(table.getColumnFamilies())
                                        .setPhysicalNames(Collections.<PName>emptyList())
                                        .setNamespaceMapped(table.isNamespaceMapped())
                                        .setImmutableStorageScheme(table.getImmutableStorageScheme())
                                        .setQualifierEncodingScheme(table.getEncodingScheme())
                                        .setUseStatsForParallelization(table.useStatsForParallelization())
                                        .build();
                                tableRefs.add(new TableRef(null, viewIndexTable, ts, false));
                            }
                        }
                        tableRefs.add(new TableRef(null, table, ts, false));
                        // TODO: Let the standard mutable secondary index maintenance handle this?
                        for (PTable index : table.getIndexes()) {
                            tableRefs.add(new TableRef(null, index, ts, false));
                        }
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
            return new MutationState(0, 0, connection);
        } finally {
            connection.setAutoCommit(wasAutoCommit);
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
        case UNALLOWED_SCHEMA_MUTATION:
            throw new SQLExceptionInfo.Builder(
                    SQLExceptionCode.CANNOT_SET_OR_ALTER_PHOENIX_TTL)
                    .setSchemaName(schemaName).setTableName(tableName).build().buildException();
        case NO_OP:
        case COLUMN_ALREADY_EXISTS:
        case COLUMN_NOT_FOUND:
            break;
        case CONCURRENT_TABLE_MUTATION:
            addTableToCache(result);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(LogUtil.addCustomAnnotations("CONCURRENT_TABLE_MUTATION for table " + SchemaUtil.getTableName(schemaName, tableName), connection));
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
        case ERROR_WRITING_TO_SCHEMA_REGISTRY:
            throw new SQLExceptionInfo.Builder(ERROR_WRITING_TO_SCHEMA_REGISTRY).
                    setSchemaName(schemaName).setTableName(tableName).build().buildException();
        default:
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.UNEXPECTED_MUTATION_CODE).setSchemaName(schemaName)
            .setTableName(tableName).setMessage("mutation code: " + mutationCode).build().buildException();
        }
        return mutationCode;
    }

    private long incrementTableSeqNum(PTable table, PTableType expectedType, int columnCountDelta,
                                      MetaPropertiesEvaluated metaPropertiesEvaluated)
            throws SQLException {
        return incrementTableSeqNum(table, expectedType, columnCountDelta,
                metaPropertiesEvaluated.getIsTransactional(),
                metaPropertiesEvaluated.getTransactionProvider(),
                metaPropertiesEvaluated.getUpdateCacheFrequency(),
                metaPropertiesEvaluated.getIsImmutableRows(),
                metaPropertiesEvaluated.getDisableWAL(),
                metaPropertiesEvaluated.getMultiTenant(),
                metaPropertiesEvaluated.getStoreNulls(),
                metaPropertiesEvaluated.getGuidePostWidth(),
                metaPropertiesEvaluated.getAppendOnlySchema(),
                metaPropertiesEvaluated.getImmutableStorageScheme(),
                metaPropertiesEvaluated.getUseStatsForParallelization(),
                metaPropertiesEvaluated.getPhoenixTTL(),
                metaPropertiesEvaluated.isChangeDetectionEnabled(),
                metaPropertiesEvaluated.getPhysicalTableName(),
                metaPropertiesEvaluated.getSchemaVersion(),
                metaPropertiesEvaluated.getColumnEncodedBytes(),
                metaPropertiesEvaluated.getStreamingTopicName());
    }

    private  long incrementTableSeqNum(PTable table, PTableType expectedType, int columnCountDelta, Boolean isTransactional,
                                       Long updateCacheFrequency, Long phoenixTTL, String physicalTableName,
                                       String schemaVersion, QualifierEncodingScheme columnEncodedBytes) throws SQLException {
        return incrementTableSeqNum(table, expectedType, columnCountDelta, isTransactional, null,
            updateCacheFrequency, null, null, null, null, -1L, null, null, null,phoenixTTL, false, physicalTableName,
                schemaVersion, columnEncodedBytes, null);
    }

    private long incrementTableSeqNum(PTable table, PTableType expectedType, int columnCountDelta,
            Boolean isTransactional, TransactionFactory.Provider transactionProvider,
            Long updateCacheFrequency, Boolean isImmutableRows, Boolean disableWAL,
            Boolean isMultiTenant, Boolean storeNulls, Long guidePostWidth, Boolean appendOnlySchema,
            ImmutableStorageScheme immutableStorageScheme, Boolean useStatsForParallelization,
            Long phoenixTTL, Boolean isChangeDetectionEnabled, String physicalTableName, String schemaVersion,
                                      QualifierEncodingScheme columnEncodedBytes, String streamingTopicName)
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
            mutateBooleanProperty(connection, tenantId, schemaName, tableName, IMMUTABLE_ROWS, isImmutableRows);
        }
        if (disableWAL != null) {
            mutateBooleanProperty(connection,tenantId, schemaName, tableName, DISABLE_WAL, disableWAL);
        }
        if (isMultiTenant != null) {
            mutateBooleanProperty(connection,tenantId, schemaName, tableName, MULTI_TENANT, isMultiTenant);
        }
        if (storeNulls != null) {
            mutateBooleanProperty(connection,tenantId, schemaName, tableName, STORE_NULLS, storeNulls);
        }
        if (isTransactional != null) {
            mutateBooleanProperty(connection,tenantId, schemaName, tableName, TRANSACTIONAL, isTransactional);
        }
        if (transactionProvider !=null) {
            mutateByteProperty(connection, tenantId, schemaName, tableName, TRANSACTION_PROVIDER, transactionProvider.getCode());
        }
        if (updateCacheFrequency != null) {
            mutateLongProperty(connection,tenantId, schemaName, tableName, UPDATE_CACHE_FREQUENCY, updateCacheFrequency);
        }
        if (guidePostWidth == null || guidePostWidth >= 0) {
            mutateLongProperty(connection, tenantId, schemaName, tableName, GUIDE_POSTS_WIDTH, guidePostWidth);
        }
        if (appendOnlySchema !=null) {
            mutateBooleanProperty(connection, tenantId, schemaName, tableName, APPEND_ONLY_SCHEMA, appendOnlySchema);
        }
        if (columnEncodedBytes !=null) {
            mutateByteProperty(connection, tenantId, schemaName, tableName, ENCODING_SCHEME, columnEncodedBytes.getSerializedMetadataValue());
        }
        if (immutableStorageScheme !=null) {
            mutateStringProperty(connection, tenantId, schemaName, tableName, IMMUTABLE_STORAGE_SCHEME, immutableStorageScheme.name());
        }
        if (useStatsForParallelization != null) {
            mutateBooleanProperty(connection, tenantId, schemaName, tableName, USE_STATS_FOR_PARALLELIZATION, useStatsForParallelization);
        }
        if (phoenixTTL != null) {
            mutateLongProperty(connection, tenantId, schemaName, tableName, PHOENIX_TTL, phoenixTTL);
        }
        if (isChangeDetectionEnabled != null) {
            mutateBooleanProperty(connection, tenantId, schemaName, tableName, CHANGE_DETECTION_ENABLED, isChangeDetectionEnabled);
        }
        if (!Strings.isNullOrEmpty(physicalTableName)) {
            mutateStringProperty(connection, tenantId, schemaName, tableName, PHYSICAL_TABLE_NAME, physicalTableName);
        }
        if (!Strings.isNullOrEmpty(schemaVersion)) {
            mutateStringProperty(connection, tenantId, schemaName, tableName, SCHEMA_VERSION, schemaVersion);
        }
        if (!Strings.isNullOrEmpty(streamingTopicName)) {
            mutateStringProperty(connection, tenantId, schemaName, tableName, STREAMING_TOPIC_NAME, streamingTopicName);
        }
        return seqNum;
    }

    public static void mutateTransformProperties(Connection connection, String tenantId, String schemaName, String tableName,
                                                 String physicalTableName,
                                                 ImmutableStorageScheme immutableStorageScheme,
                                                 QualifierEncodingScheme columnEncodedBytes) throws SQLException {
        if (columnEncodedBytes !=null) {
            mutateByteProperty(connection, tenantId, schemaName, tableName, ENCODING_SCHEME, columnEncodedBytes.getSerializedMetadataValue());
        }
        if (immutableStorageScheme !=null) {
            mutateByteProperty(connection, tenantId, schemaName, tableName, IMMUTABLE_STORAGE_SCHEME, immutableStorageScheme.getSerializedMetadataValue());
        }
        if (!Strings.isNullOrEmpty(physicalTableName)) {
            mutateStringProperty(connection, tenantId, schemaName, tableName, PHYSICAL_TABLE_NAME, physicalTableName);
        }
    }

    private static void mutateBooleanProperty(Connection connection, String tenantId, String schemaName, String tableName,
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

    private static void mutateLongProperty(Connection connection, String tenantId, String schemaName, String tableName,
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
    
    private static void mutateByteProperty(Connection connection, String tenantId, String schemaName, String tableName,
            String propertyName, Byte propertyValue) throws SQLException {
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
                tableBoolUpsert.setNull(4, Types.TINYINT);
            } else {
                tableBoolUpsert.setByte(4, propertyValue);
            }
            tableBoolUpsert.execute();
        }
    }
    
    private static void mutateStringProperty(Connection connection, String tenantId, String schemaName, String tableName,
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
        return addColumn(table, statement.getColumnDefs(), statement.getProps(), statement.ifNotExists(), false, statement.getTable(), statement.getTableType(), statement.isCascade(), statement.getIndexes());
    }

    public MutationState addColumn(PTable table, List<ColumnDef> origColumnDefs,
            ListMultimap<String, Pair<String, Object>> stmtProperties, boolean ifNotExists,
            boolean removeTableProps, NamedTableNode namedTableNode, PTableType tableType, boolean cascade, List<NamedNode> indexes)
                    throws SQLException {
        connection.rollback();
        List<PTable> indexesPTable = Lists.newArrayListWithExpectedSize(indexes != null ?
                indexes.size() : table.getIndexes().size());
        Map<PTable, Integer> indexToColumnSizeMap = new HashMap<>();

        // if cascade keyword is passed and indexes are provided either implicitly or explicitly
        if (cascade && (indexes == null || !indexes.isEmpty())) {
            indexesPTable = getIndexesPTableForCascade(indexes, table);
            if (indexesPTable.size() == 0) {
                // go back to regular behavior of altering the table/view
                cascade = false;
            } else {
                for (PTable index : indexesPTable) {
                    indexToColumnSizeMap.put(index, index.getColumns().size());
                }
            }
        }
        boolean wasAutoCommit = connection.getAutoCommit();
        List<PColumn> columns = Lists.newArrayListWithExpectedSize(origColumnDefs != null ?
            origColumnDefs.size() : 0);
        PName tenantId = connection.getTenantId();
        boolean sharedIndex = tableType == PTableType.INDEX && (table.getIndexType() == IndexType.LOCAL || table.getViewIndexId() != null);
        String tenantIdToUse = connection.getTenantId() != null && sharedIndex ? connection.getTenantId().getString() : null;
        String schemaName = table.getSchemaName().getString();
        String tableName = table.getTableName().getString();
        PName physicalName = table.getPhysicalName();
        String physicalSchemaName =
                SchemaUtil.getSchemaNameFromFullName(physicalName.getString());
        String physicalTableName =
                SchemaUtil.getTableNameFromFullName(physicalName.getString());
        Set<String> acquiredColumnMutexSet = Sets.newHashSetWithExpectedSize(3);
        boolean acquiredBaseTableMutex = false;
        try {
            connection.setAutoCommit(false);

            List<ColumnDef> columnDefs;
            if ((table.isAppendOnlySchema() || ifNotExists) && origColumnDefs != null) {
                // only make the rpc if we are adding new columns
                columnDefs = Lists.newArrayList();
                for (ColumnDef columnDef : origColumnDefs) {
                    String familyName = columnDef.getColumnDefName().getFamilyName();
                    String columnName = columnDef.getColumnDefName().getColumnName();
                    if (familyName != null) {
                        try {
                            PColumnFamily columnFamily = table.getColumnFamily(familyName);
                            columnFamily.getPColumnForColumnName(columnName);
                            if (!ifNotExists) {
                                throw new ColumnAlreadyExistsException(schemaName, tableName,
                                  columnName);
                            }
                        } catch (ColumnFamilyNotFoundException | ColumnNotFoundException e) {
                            columnDefs.add(columnDef);
                        }
                    } else {
                        try {
                            table.getColumnForColumnName(columnName);
                            if (!ifNotExists) {
                                throw new ColumnAlreadyExistsException(schemaName, tableName,
                                  columnName);
                            }
                        } catch (ColumnNotFoundException e) {
                            columnDefs.add(columnDef);
                        }
                    }
                }
            } else {
                columnDefs = origColumnDefs == null ? Collections.<ColumnDef>emptyList() : origColumnDefs;
            }

            boolean retried = false;
            boolean changingPhoenixTableProperty = false;
            MetaProperties metaProperties = new MetaProperties();
            while (true) {
                Map<String, List<Pair<String, Object>>> properties=new HashMap<>(stmtProperties.size());;
                metaProperties = loadStmtProperties(stmtProperties,properties,table,removeTableProps);

                ColumnResolver resolver = FromCompiler.getResolver(namedTableNode, connection);
                table = resolver.getTables().get(0).getTable();
                int nIndexes = table.getIndexes().size();
                int numCols = columnDefs.size();
                int nNewColumns = numCols;
                List<Mutation> tableMetaData = Lists.newArrayListWithExpectedSize((1 + nNewColumns) * (nIndexes + 1));
                List<Mutation> columnMetaData = Lists.newArrayListWithExpectedSize(nNewColumns * (nIndexes + 1));
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(LogUtil.addCustomAnnotations("Resolved table to " + table.getName().getString() + " with seqNum " + table.getSequenceNumber() + " at timestamp " + table.getTimeStamp() + " with " + table.getColumns().size() + " columns: " + table.getColumns(), connection));
                }

                int position = table.getColumns().size();

                boolean addPKColumns = columnDefs.stream().anyMatch(ColumnDef::isPK);
                if (addPKColumns) {
                    List<PColumn> currentPKs = table.getPKColumns();
                    PColumn lastPK = currentPKs.get(currentPKs.size()-1);
                    // Disallow adding columns if the last column in the primary key is VARBIANRY
                    // or ARRAY.
                    if (lastPK.getDataType() == PVarbinary.INSTANCE || lastPK.getDataType().isArrayType()) {
                        throw new SQLExceptionInfo.Builder(SQLExceptionCode.VARBINARY_LAST_PK)
                        .setColumnName(lastPK.getName().getString()).build().buildException();
                    }
                    // Disallow adding columns if last column in the primary key is fixed width
                    // and nullable.
                    if (lastPK.isNullable() && lastPK.getDataType().isFixedWidth()) {
                        throw new SQLExceptionInfo.Builder(SQLExceptionCode.NULLABLE_FIXED_WIDTH_LAST_PK)
                        .setColumnName(lastPK.getName().getString()).build().buildException();
                    }
                }

                MetaPropertiesEvaluated metaPropertiesEvaluated = new MetaPropertiesEvaluated();
                changingPhoenixTableProperty = evaluateStmtProperties(metaProperties,metaPropertiesEvaluated,table,schemaName,tableName);

                boolean isTransformNeeded = TransformClient.checkIsTransformNeeded(metaProperties, schemaName, table, tableName, null, tenantIdToUse, connection);
                if (isTransformNeeded) {
                    // We can add a support for these later. For now, not supported.
                    if (MetaDataUtil.hasLocalIndexTable(connection, physicalTableName.getBytes())) {
                        throw new SQLExceptionInfo.Builder(SQLExceptionCode.CANNOT_TRANSFORM_TABLE_WITH_LOCAL_INDEX)
                                .setSchemaName(schemaName).setTableName(tableName).build().buildException();
                    }
                    if (table.isAppendOnlySchema()) {
                        throw new SQLExceptionInfo.Builder(SQLExceptionCode.CANNOT_TRANSFORM_TABLE_WITH_APPEND_ONLY_SCHEMA)
                                .setSchemaName(schemaName).setTableName(tableName).build().buildException();
                    }
                    if (table.isTransactional()) {
                        throw new SQLExceptionInfo.Builder(CANNOT_TRANSFORM_TRANSACTIONAL_TABLE)
                                .setSchemaName(schemaName).setTableName(tableName).build().buildException();
                    }
                }

                // If changing isImmutableRows to true or it's not being changed and is already true
                boolean willBeImmutableRows = Boolean.TRUE.equals(metaPropertiesEvaluated.getIsImmutableRows()) || (metaPropertiesEvaluated.getIsImmutableRows() == null && table.isImmutableRows());
                boolean willBeTxnl = metaProperties.getNonTxToTx();
                Long timeStamp = TransactionUtil.getTableTimestamp(connection, table.isTransactional() || willBeTxnl, table.isTransactional() ? table.getTransactionProvider() : metaPropertiesEvaluated.getTransactionProvider());
                int numPkColumnsAdded = 0;
                Set<String> colFamiliesForPColumnsToBeAdded = new LinkedHashSet<>();
                Set<String> families = new LinkedHashSet<>();
                PTable tableForCQCounters = tableType == PTableType.VIEW
                        ? connection.getTable(table.getPhysicalName().getString())
                        : table;
                EncodedCQCounter cqCounterToUse = tableForCQCounters.getEncodedCQCounter();
                Map<String, Integer> changedCqCounters = new HashMap<>(numCols);
                if (numCols > 0 ) {
                    StatementContext context = new StatementContext(new PhoenixStatement(connection), resolver);
                    short nextKeySeq = SchemaUtil.getMaxKeySeq(table);
                    for ( ColumnDef colDef : columnDefs) {
                        if (colDef != null && !colDef.isNull()) {
                            if (colDef.isPK()) {
                                throw new SQLExceptionInfo.Builder(SQLExceptionCode.NOT_NULLABLE_COLUMN_IN_ROW_KEY)
                                .setColumnName(colDef.getColumnDefName().getColumnName()).build().buildException();
                            } else if (!willBeImmutableRows) {
                                throw new SQLExceptionInfo.Builder(SQLExceptionCode.KEY_VALUE_NOT_NULL)
                                .setColumnName(colDef.getColumnDefName().getColumnName()).build().buildException();
                            }
                        }
                        if (colDef != null && colDef.isPK() && table.getType() == VIEW && table.getViewType() != MAPPED) {
                            throwIfLastPKOfParentIsVariableLength(getParentOfView(table), schemaName, tableName, colDef);
                        }
                        if (colDef != null && colDef.isRowTimestamp()) {
                            throw new SQLExceptionInfo.Builder(SQLExceptionCode.ROWTIMESTAMP_CREATE_ONLY)
                            .setColumnName(colDef.getColumnDefName().getColumnName()).build().buildException();
                        }
                        if (!colDef.validateDefault(context, null)) {
                            colDef = new ColumnDef(colDef, null); // Remove DEFAULT as it's not necessary
                        }
                        String familyName = null;
                        Integer encodedCQ = null;
                        if (!colDef.isPK()) {
                            String colDefFamily = colDef.getColumnDefName().getFamilyName();
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
                            encodedCQ = table.isAppendOnlySchema() ? Integer.valueOf(ENCODED_CQ_COUNTER_INITIAL_VALUE + position) : cqCounterToUse.getNextQualifier(familyName);
                            if (!table.isAppendOnlySchema() && cqCounterToUse.increment(familyName)) {
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
                        PColumn column = newColumn(position++, colDef, PrimaryKeyConstraint.EMPTY, table.getDefaultFamilyName() == null ? null : table.getDefaultFamilyName().getString(), true, columnQualifierBytes, willBeImmutableRows);
                        HashMap<PTable, PColumn> indexToIndexColumnMap = null;
                        if (cascade) {
                            indexToIndexColumnMap = getPTablePColumnHashMapForCascade(indexesPTable, willBeImmutableRows,
                                            colDef, familyName, indexToColumnSizeMap);
                        }

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
                        addColumnMutation(connection, schemaName, tableName, column, null, pkName, keySeq, table.getBucketNum() != null);
                        // add new columns for given indexes one by one
                        if (cascade) {
                            for (PTable index: indexesPTable) {
                                LOGGER.info("Adding column "+column.getName().getString()+" to "+index.getTableName().toString());
                                addColumnMutation(connection, schemaName, index.getTableName().getString(), indexToIndexColumnMap.get(index), null, "", keySeq, index.getBucketNum() != null);
                            }
                        }
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
                                    Expression expression = new RowKeyColumnExpression(columns.get(i), new RowKeyValueAccessor(pkColumns, pkSlotPosition));
                                    ColumnDef indexColDef = FACTORY.columnDef(indexColName, indexColDataType.getSqlTypeName(), colDef.isNull(), colDef.getMaxLength(), colDef.getScale(), true, colDef.getSortOrder(), expression.toString(), colDef.isRowTimestamp());
                                    PColumn indexColumn = newColumn(indexPosition++, indexColDef, PrimaryKeyConstraint.EMPTY, null, true, null, willBeImmutableRows);
                                    addColumnMutation(connection, schemaName, index.getTableName().getString(), indexColumn, index.getParentTableName().getString(), index.getPKName() == null ? null : index.getPKName().getString(), ++nextIndexKeySeq, index.getBucketNum() != null);
                                }
                            }
                        }
                        ++pkSlotPosition;
                    }
                    columnMetaData.addAll(connection.getMutationState().toMutations(timeStamp).next().getSecond());
                    connection.rollback();
                } else {
                    // Check that HBase configured properly for mutable secondary indexing
                    // if we're changing from an immutable table to a mutable table and we
                    // have existing indexes.
                    if (Boolean.FALSE.equals(metaPropertiesEvaluated.getIsImmutableRows()) && !table.getIndexes().isEmpty()) {
                        int hbaseVersion = connection.getQueryServices().getLowestClusterHBaseVersion();
                        if (hbaseVersion < MetaDataProtocol.MUTABLE_SI_VERSION_THRESHOLD) {
                            throw new SQLExceptionInfo.Builder(SQLExceptionCode.NO_MUTABLE_INDEXES)
                            .setSchemaName(schemaName).setTableName(tableName).build().buildException();
                        }
                        if (!connection.getQueryServices().hasIndexWALCodec() && !table.isTransactional()) {
                            throw new SQLExceptionInfo.Builder(SQLExceptionCode.INVALID_MUTABLE_INDEX_CONFIG)
                            .setSchemaName(schemaName).setTableName(tableName).build().buildException();
                        }
                    }
                    if (Boolean.TRUE.equals(metaPropertiesEvaluated.getMultiTenant())) {
                        throwIfInsufficientColumns(schemaName, tableName, table.getPKColumns(), table.getBucketNum()!=null, metaPropertiesEvaluated.getMultiTenant());
                    }
                }

                if (!table.getIndexes().isEmpty() &&
                        (numPkColumnsAdded>0 || metaProperties.getNonTxToTx() ||
                                metaPropertiesEvaluated.getUpdateCacheFrequency() != null || metaPropertiesEvaluated.getPhoenixTTL() != null)) {
                    for (PTable index : table.getIndexes()) {
                        incrementTableSeqNum(index, index.getType(), numPkColumnsAdded,
                                metaProperties.getNonTxToTx() ? Boolean.TRUE : null,
                                metaPropertiesEvaluated.getUpdateCacheFrequency(),
                                metaPropertiesEvaluated.getPhoenixTTL(),
                                metaPropertiesEvaluated.getPhysicalTableName(),
                                metaPropertiesEvaluated.getSchemaVersion(),
                                metaProperties.getColumnEncodedBytesProp());
                    }
                    tableMetaData.addAll(connection.getMutationState().toMutations(timeStamp).next().getSecond());
                    connection.rollback();
                }

                if (cascade) {
                    for (PTable index : indexesPTable) {
                        incrementTableSeqNum(index, index.getType(), columnDefs.size(),
                                Boolean.FALSE,
                                metaPropertiesEvaluated.getUpdateCacheFrequency(),
                                metaPropertiesEvaluated.getPhoenixTTL(),
                                metaPropertiesEvaluated.getPhysicalTableName(),
                                metaPropertiesEvaluated.getSchemaVersion(),
                                metaPropertiesEvaluated.getColumnEncodedBytes());
                    }
                    tableMetaData.addAll(connection.getMutationState().toMutations(timeStamp).next().getSecond());
                    connection.rollback();
                }

                long seqNum = 0;
                if (changingPhoenixTableProperty || columnDefs.size() > 0) {
                    seqNum = incrementTableSeqNum(table, tableType, columnDefs.size(), metaPropertiesEvaluated);

                    tableMetaData.addAll(connection.getMutationState().toMutations(timeStamp).next().getSecond());
                    connection.rollback();
                }

                PTable transformingNewTable = null;
                if (isTransformNeeded) {
                   try {
                       transformingNewTable = TransformClient.addTransform(connection, tenantIdToUse, table, metaProperties, seqNum, PTable.TransformType.METADATA_TRANSFORM);
                    } catch (SQLException ex) {
                       connection.rollback();
                       throw ex;
                   }
                }

                // Force the table header row to be first
                Collections.reverse(tableMetaData);
                // Add column metadata afterwards, maintaining the order so columns have more predictable ordinal position
                tableMetaData.addAll(columnMetaData);
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

                byte[] family = families.size() > 0 ?
                        families.iterator().next().getBytes(StandardCharsets.UTF_8) : null;

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

                if (EncodedColumnsUtil.usesEncodedColumnNames(table)
                        && stmtProperties.isEmpty() && !acquiredBaseTableMutex) {
                    // For tables that use column encoding acquire a mutex on
                    // the base table as we need to update the encoded column
                    // qualifier counter on the base table. Not applicable to
                    // ALTER TABLE/VIEW SET <property> statements because
                    // we don't update the column qualifier counter while
                    // setting property, hence the check: stmtProperties.isEmpty()
                    acquiredBaseTableMutex = writeCell(null, physicalSchemaName,
                        physicalTableName, null);
                    if (!acquiredBaseTableMutex) {
                        throw new ConcurrentTableMutationException(
                            physicalSchemaName, physicalTableName);
                    }
                }
                for (PColumn pColumn : columns) {
                    // acquire the mutex using the global physical table name to
                    // prevent creating the same column on a table or view with
                    // a conflicting type etc
                    boolean acquiredMutex = writeCell(null, physicalSchemaName, physicalTableName,
                        pColumn.toString());
                    if (!acquiredMutex && !acquiredColumnMutexSet.contains(pColumn.toString())) {
                        throw new ConcurrentTableMutationException(physicalSchemaName, physicalTableName);
                    }
                    acquiredColumnMutexSet.add(pColumn.toString());
                }
                MetaDataMutationResult result = connection.getQueryServices().addColumn(tableMetaData, table,
                        getParentTable(table), transformingNewTable, properties, colFamiliesForPColumnsToBeAdded, columns);

                try {
                    MutationCode code = processMutationResult(schemaName, tableName, result);
                    if (code == MutationCode.COLUMN_ALREADY_EXISTS) {
                        addTableToCache(result);
                        if (!ifNotExists) {
                            throw new ColumnAlreadyExistsException(schemaName, tableName, SchemaUtil.findExistingColumn(result.getTable(), columns));
                        }
                        return new MutationState(0, 0, connection);
                    }
                    // Only update client side cache if we aren't adding a PK column to a table with indexes or
                    // transitioning a table from non transactional to transactional.
                    // We could update the cache manually then too, it'd just be a pain.
                    String fullTableName = SchemaUtil.getTableName(schemaName, tableName);
                    long resolvedTimeStamp = TransactionUtil.getResolvedTime(connection, result);
                    if (table.getIndexes().isEmpty() || (numPkColumnsAdded==0 && ! metaProperties.getNonTxToTx())) {
                        addTableToCache(result, resolvedTimeStamp);
                        table = result.getTable();
                    } else  {
                        // remove the table from the cache, it will be fetched from the server the
                        // next time it is resolved
                        connection.removeTable(tenantId, fullTableName, null, resolvedTimeStamp);
                    }
                    // Delete rows in view index if we haven't dropped it already
                    // We only need to do this if the multiTenant transitioned to false
                    if (table.getType() == PTableType.TABLE
                            && Boolean.FALSE.equals(metaPropertiesEvaluated.getMultiTenant())
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
                            String viewIndexSchemaName = SchemaUtil.getSchemaNameFromFullName(viewIndexPhysicalName);
                            String viewIndexTableName = SchemaUtil.getTableNameFromFullName(viewIndexPhysicalName);
                            PName viewIndexName = PNameFactory.newName(SchemaUtil.getTableName(viewIndexSchemaName, viewIndexTableName));

                            PTable viewIndexTable = new PTableImpl.Builder()
                                    .setName(viewIndexName)
                                    .setKey(new PTableKey(tenantId, viewIndexName.getString()))
                                    .setSchemaName(PNameFactory.newName(viewIndexSchemaName))
                                    .setTableName(PNameFactory.newName(viewIndexTableName))
                                    .setType(PTableType.VIEW)
                                    .setViewType(ViewType.MAPPED)
                                    .setTimeStamp(ts)
                                    .setPkColumns(Collections.<PColumn>emptyList())
                                    .setAllColumns(Collections.<PColumn>emptyList())
                                    .setRowKeySchema(RowKeySchema.EMPTY_SCHEMA)
                                    .setIndexes(Collections.<PTable>emptyList())
                                    .setFamilyAttributes(table.getColumnFamilies())
                                    .setPhysicalNames(Collections.<PName>emptyList())
                                    .setNamespaceMapped(table.isNamespaceMapped())
                                    .setImmutableStorageScheme(table.getImmutableStorageScheme())
                                    .setQualifierEncodingScheme(table.getEncodingScheme())
                                    .setUseStatsForParallelization(table.useStatsForParallelization())
                                    .build();
                            List<TableRef> tableRefs = Collections.singletonList(new TableRef(null, viewIndexTable, ts, false));
                            MutationPlan plan = new PostDDLCompiler(connection).compile(tableRefs, null, null,
                                    Collections.<PColumn>emptyList(), ts);
                            connection.getQueryServices().updateData(plan);
                        }
                    }
                    if (transformingNewTable != null) {
                        connection.removeTable(tenantId, fullTableName, null, resolvedTimeStamp);
                        connection.getQueryServices().clearCache();
                    }
                    if (emptyCF != null) {
                        Long scn = connection.getSCN();
                        connection.setAutoCommit(true);
                        // Delete everything in the column. You'll still be able to do queries at earlier timestamps
                        long ts = (scn == null ? result.getMutationTime() : scn);
                        MutationPlan plan = new PostDDLCompiler(connection).compile(Collections.singletonList(new TableRef(null, table, ts, false)), emptyCF, projectCF == null ? null : Collections.singletonList(projectCF), null, ts);
                        return connection.getQueryServices().updateData(plan);
                    }
                    return new MutationState(0, 0, connection);
                } catch (ConcurrentTableMutationException e) {
                    if (retried) {
                        throw e;
                    }
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug(LogUtil.addCustomAnnotations("Caught ConcurrentTableMutationException for table " + SchemaUtil.getTableName(schemaName, tableName) + ". Will try again...", connection));
                    }
                    retried = true;
                } catch(Throwable e) {
                    TableMetricsManager.updateMetricsForSystemCatalogTableMethod(tableName,
                            NUM_METADATA_LOOKUP_FAILURES, 1);
                    throw e;
                }
            }
        } finally {
            connection.setAutoCommit(wasAutoCommit);
            if (acquiredBaseTableMutex) {
                // release the mutex on the physical table (used to prevent concurrent conflicting
                // add column changes)
                deleteCell(null, physicalSchemaName, physicalTableName, null);
            }
            deleteMutexCells(physicalSchemaName, physicalTableName, acquiredColumnMutexSet);
        }
    }

    private List<PTable> getIndexesPTableForCascade(List<NamedNode> indexes, PTable table) throws SQLException {
        boolean isView = table.getType().equals(PTableType.VIEW);
        List<PTable> indexesPTable = new ArrayList<>();

        // when indexes is null, that means ALL keyword is passed and
        // we ll collect all global indexes for cascading
        if (indexes == null) {
            indexesPTable.addAll(table.getIndexes());
            for (PTable index : table.getIndexes()) {
                // a child view has access to its parents indexes,
                // this if clause ensures we only get the indexes that
                // are only created on the view itself.
                if (index.getIndexType().equals(IndexType.LOCAL)
                        || (isView && index.getTableName().toString().contains(
                        QueryConstants.CHILD_VIEW_INDEX_NAME_SEPARATOR))) {
                    indexesPTable.remove(index);
                }
            }
        } else {
            List<String> indexesParam = Lists.newArrayListWithExpectedSize(indexes.size());
            for (NamedNode index : indexes) {
                indexesParam.add(index.getName());
            }
            // gets the PTable for list of indexes passed in the function
            // if all the names in parameter list are correct, indexesParam list should be empty
            // by end of the loop
            for (PTable index : table.getIndexes()) {
                if (index.getIndexType().equals(IndexType.LOCAL)) {
                    throw new SQLExceptionInfo
                            .Builder(SQLExceptionCode.NOT_SUPPORTED_CASCADE_FEATURE_LOCAL_INDEX)
                            .setTableName(index.getName().getString())
                            .build()
                            .buildException();
                }
                if (indexesParam.remove(index.getTableName().getString())) {
                    indexesPTable.add(index);
                }
            }
            // indexesParam has index names that are not correct
            if (!indexesParam.isEmpty()) {
                throw new SQLExceptionInfo
                        .Builder(SQLExceptionCode.INCORRECT_INDEX_NAME)
                        .setTableName(StringUtils.join(",", indexesParam))
                        .build()
                        .buildException();
            }
        }
        return indexesPTable;
    }

    private HashMap<PTable, PColumn> getPTablePColumnHashMapForCascade(List<PTable> indexesPTable,
            boolean willBeImmutableRows, ColumnDef colDef, String familyName, Map<PTable, Integer> indexToColumnSizeMap) throws SQLException {
        HashMap<PTable, PColumn> indexColumn;
        if (colDef.isPK()) {
            //only supported for non pk column
            throw new SQLExceptionInfo.Builder(
                    SQLExceptionCode.NOT_SUPPORTED_CASCADE_FEATURE_PK)
                    .build()
                    .buildException();
        }
        indexColumn = new HashMap(indexesPTable.size());
        ColumnName
                indexColName = ColumnName.caseSensitiveColumnName(IndexUtil.getIndexColumnName(familyName, colDef.getColumnDefName().getColumnName()));
        ColumnDef indexColDef = FACTORY.columnDef(indexColName,
                colDef.getDataType().getSqlTypeName(), colDef.isNull(),
                colDef.getMaxLength(), colDef.getScale(), false,
                colDef.getSortOrder(), colDef.getExpression(), colDef.isRowTimestamp());
        // TODO: add support to specify tenant owned indexes in the DDL statement with CASCADE executed with Global connection
        for (PTable index : indexesPTable) {
            int iPos = indexToColumnSizeMap.get(index);
            EncodedCQCounter cqCounterToUse = index.getEncodedCQCounter();
            int baseCount = 0;
            baseCount = (cqCounterToUse != null && cqCounterToUse.getNextQualifier(familyName)!=null) ? cqCounterToUse.getNextQualifier(familyName) : 0 ;
            Integer encodedCQ = index.isAppendOnlySchema() ? Integer.valueOf(ENCODED_CQ_COUNTER_INITIAL_VALUE + iPos) : baseCount + iPos;
            byte[] columnQualifierBytes = EncodedColumnsUtil.getColumnQualifierBytes(indexColDef.getColumnDefName().getColumnName(), encodedCQ, index, indexColDef.isPK());
            PColumn iColumn = newColumn(iPos, indexColDef, null, index.getDefaultFamilyName() == null ? null : index.getDefaultFamilyName().getString(), false, columnQualifierBytes, willBeImmutableRows);
            indexColumn.put(index, iColumn);
            indexToColumnSizeMap.put(index, iPos+1);
        }
        return indexColumn;
    }

    private void deleteMutexCells(String physicalSchemaName, String physicalTableName, Set<String> acquiredColumnMutexSet) throws SQLException {
        if (!acquiredColumnMutexSet.isEmpty()) {
            for (String columnName : acquiredColumnMutexSet) {
                // release the mutex (used to prevent concurrent conflicting add column changes)
                deleteCell(null, physicalSchemaName, physicalTableName, columnName);
            }
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
        for (PColumn columnToDrop : columnsToDrop) {
            buf.append("('" + tenantId + "'");
            buf.append(",'" + schemaName + "'");
            buf.append(",'" + tableName + "'");
            buf.append(",'" + columnToDrop.getName().getString() + "'");
            buf.append(",'" + (columnToDrop.getFamilyName() == null ? "" : columnToDrop.getFamilyName().getString()) + "'),");
        }
        buf.setCharAt(buf.length()-1, ')');

        try (PreparedStatement delCol = connection.prepareStatement(buf.toString())) {
            delCol.execute();
        }
        Collections.sort(columnsToDrop,new Comparator<PColumn> () {
            @Override
            public int compare(PColumn left, PColumn right) {
                return Ints.compare(left.getPosition(), right.getPosition());
            }
        });

        boolean isSalted = table.getBucketNum() != null;
        int columnsToDropIndex = 0;
        try (PreparedStatement colUpdate = connection.prepareStatement(UPDATE_COLUMN_POSITION)) {
            colUpdate.setString(1, tenantId);
            colUpdate.setString(2, schemaName);
            colUpdate.setString(3, tableName);
            for (int i = columnsToDrop.get(columnsToDropIndex).getPosition() + 1;
                i < table.getColumns().size(); i++) {
                PColumn column = table.getColumns().get(i);
                if (columnsToDrop.contains(column)) {
                    columnsToDropIndex++;
                    continue;
                }
                colUpdate.setString(4, column.getName().getString());
                colUpdate.setString(5, column.getFamilyName() == null
                    ? null : column.getFamilyName().getString());
                // Adjust position to not include the salt column
                colUpdate.setInt(6,
                    column.getPosition() - columnsToDropIndex - (isSalted ? 1 : 0));
                colUpdate.execute();
            }
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
            return SchemaUtil.getEmptyColumnFamily(table.getDefaultFamilyName(), table.getColumnFamilies().subList(1, table.getColumnFamilies().size()), table.getIndexType() == IndexType.LOCAL);
        }
        // If unchanged, return null
        return null;
    }

    private PTable getParentTable(PTable table) throws SQLException {
        PTable parentTable = null;
        boolean hasIndexId = table.getViewIndexId() != null;
        if ( (table.getType()==PTableType.INDEX && hasIndexId)
                || (table.getType() == PTableType.VIEW && table.getViewType() != ViewType.MAPPED)) {
            parentTable = connection.getTable(table.getParentName().getString());
            if (parentTable==null) {
                String schemaName = table.getSchemaName()!=null ? table.getSchemaName().getString() : null;
                throw new TableNotFoundException(schemaName, table.getTableName().getString());
            }
            // only inherit columns view indexes (and not local indexes
            // on regular tables which also have a viewIndexId)
            if (hasIndexId && parentTable.getType() != PTableType.VIEW) {
                return null;
            }
        }
        return parentTable;
    }

    public MutationState dropColumn(DropColumnStatement statement) throws SQLException {
        connection.rollback();
        boolean wasAutoCommit = connection.getAutoCommit();
        Set<String> acquiredColumnMutexSet = Sets.newHashSetWithExpectedSize(3);
        String physicalSchemaName = null;
        String physicalTableName  = null;
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
                PName physicalName = table.getPhysicalName();
                physicalSchemaName = SchemaUtil.getSchemaNameFromFullName(physicalName.getString());
                physicalTableName = SchemaUtil.getTableNameFromFullName(physicalName.getString());

                List<ColumnName> columnRefs = statement.getColumnRefs();
                if (columnRefs == null) {
                    columnRefs = Lists.newArrayListWithCapacity(0);
                }
                List<ColumnRef> columnsToDrop = Lists.newArrayListWithExpectedSize(columnRefs.size() + table.getIndexes().size());
                List<TableRef> indexesToDrop = Lists.newArrayListWithExpectedSize(table.getIndexes().size());
                List<Mutation> tableMetaData = Lists.newArrayListWithExpectedSize((table.getIndexes().size() + 1) * (1 + table.getColumns().size() - columnRefs.size()));
                List<PColumn>  tableColumnsToDrop = Lists.newArrayListWithExpectedSize(columnRefs.size());

                for (ColumnName column : columnRefs) {
                    ColumnRef columnRef = null;
                    try {
                        columnRef = resolver.resolveColumn(null, column.getFamilyName(), column.getColumnName());
                    } catch (ColumnNotFoundException e) {
                        if (statement.ifExists()) {
                            return new MutationState(0, 0, connection);
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
                    else if (columnToDrop.isViewReferenced()) {
                        throw new SQLExceptionInfo.Builder(SQLExceptionCode.CANNOT_DROP_VIEW_REFERENCED_COL)
                                .setColumnName(columnToDrop.getName().getString()).build().buildException();
                    }
                    columnsToDrop.add(new ColumnRef(columnRef.getTableRef(), columnToDrop.getPosition()));
                    boolean acquiredMutex = writeCell(null, physicalSchemaName,
                            physicalTableName, columnToDrop.toString());
                    if (!acquiredMutex) {
                        throw new ConcurrentTableMutationException(physicalSchemaName, physicalTableName);
                    }
                    acquiredColumnMutexSet.add(columnToDrop.toString());
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
                    for (PColumn columnToDrop : tableColumnsToDrop) {
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
                    if (!indexColumnsToDrop.isEmpty()) {
                        long indexTableSeqNum = incrementTableSeqNum(index, index.getType(), -indexColumnsToDrop.size(),
                                null, null, null, null, null, null);
                        dropColumnMutations(index, indexColumnsToDrop);
                        long clientTimestamp = MutationState.getTableTimestamp(timeStamp, connection.getSCN());
                        connection.removeColumn(tenantId, index.getName().getString(),
                                indexColumnsToDrop, clientTimestamp, indexTableSeqNum,
                                TransactionUtil.getResolvedTimestamp(connection, index.isTransactional(), clientTimestamp));
                    }
                }
                tableMetaData.addAll(connection.getMutationState().toMutations(timeStamp).next().getSecond());
                connection.rollback();

                long seqNum = incrementTableSeqNum(table, statement.getTableType(), -tableColumnsToDrop.size(),
                        null, null, null, null, null, null);
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
                                                    tableContainingColumnToDrop, null, null,family, Sets.newHashSet(Bytes.toString(emptyCF)), Collections.<PColumn>emptyList());

                        }
                    }
                }

                MetaDataMutationResult result = connection.getQueryServices().dropColumn(tableMetaData,
                        statement.getTableType(), getParentTable(table));
                try {
                    MutationCode code = processMutationResult(schemaName, tableName, result);
                    if (code == MutationCode.COLUMN_NOT_FOUND) {
                        addTableToCache(result);
                        if (!statement.ifExists()) {
                            throw new ColumnNotFoundException(schemaName, tableName, Bytes.toString(result.getFamilyName()), Bytes.toString(result.getColumnName()));
                        }
                        return new MutationState(0, 0, connection);
                    }
                    // If we've done any index metadata updates, don't bother trying to update
                    // client-side cache as it would be too painful. Just let it pull it over from
                    // the server when needed.
                    if (tableColumnsToDrop.size() > 0) {
                        //need to remove the cached table because the DDL timestamp changed. We
                        // also need to remove it if we dropped an indexed column
                        connection.removeTable(tenantId, tableName, table.getParentName() == null ? null : table.getParentName().getString(), table.getTimeStamp());
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
                                ImmutableStorageScheme storageScheme = table.getImmutableStorageScheme();
                                QualifierEncodingScheme qualifierEncodingScheme = table.getEncodingScheme();
                                List<PColumn> columns = sharedTableState.getColumns();
                                if (table.getBucketNum() != null) {
                                    columns = columns.subList(1, columns.size());
                                }

                                PTableImpl viewIndexTable = new PTableImpl.Builder()
                                        .setPkColumns(Collections.<PColumn>emptyList())
                                        .setAllColumns(Collections.<PColumn>emptyList())
                                        .setRowKeySchema(RowKeySchema.EMPTY_SCHEMA)
                                        .setIndexes(Collections.<PTable>emptyList())
                                        .setFamilyAttributes(table.getColumnFamilies())
                                        .setType(PTableType.INDEX)
                                        .setTimeStamp(ts)
                                        .setMultiTenant(table.isMultiTenant())
                                        .setViewIndexIdType(sharedTableState.getViewIndexIdType())
                                        .setViewIndexId(sharedTableState.getViewIndexId())
                                        .setNamespaceMapped(table.isNamespaceMapped())
                                        .setAppendOnlySchema(false)
                                        .setImmutableStorageScheme(storageScheme == null ?
                                                ImmutableStorageScheme.ONE_CELL_PER_COLUMN : storageScheme)
                                        .setQualifierEncodingScheme(qualifierEncodingScheme == null ?
                                                QualifierEncodingScheme.NON_ENCODED_QUALIFIERS : qualifierEncodingScheme)
                                        .setEncodedCQCounter(table.getEncodedCQCounter())
                                        .setUseStatsForParallelization(table.useStatsForParallelization())
                                        .setExcludedColumns(ImmutableList.<PColumn>of())
                                        .setTenantId(sharedTableState.getTenantId())
                                        .setSchemaName(sharedTableState.getSchemaName())
                                        .setTableName(sharedTableState.getTableName())
                                        .setRowKeyOrderOptimizable(false)
                                        .setBucketNum(table.getBucketNum())
                                        .setIndexes(Collections.<PTable>emptyList())
                                        .setPhysicalNames(sharedTableState.getPhysicalNames() == null ?
                                                ImmutableList.<PName>of() :
                                                ImmutableList.copyOf(sharedTableState.getPhysicalNames()))
                                        .setColumns(columns)
                                        .build();
                                TableRef indexTableRef = new TableRef(viewIndexTable);
                                PName indexTableTenantId = sharedTableState.getTenantId();
                                if (indexTableTenantId == null) {
                                    tableRefsToDrop.add(indexTableRef);
                                } else {
                                    if (!tenantIdTableRefMap.containsKey(
                                            indexTableTenantId.getString())) {
                                        tenantIdTableRefMap.put(indexTableTenantId.getString(),
                                            Lists.<TableRef>newArrayList());
                                    }
                                    tenantIdTableRefMap.get(indexTableTenantId.getString())
                                            .add(indexTableRef);
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
                    return new MutationState(0, 0, connection);
                } catch (ConcurrentTableMutationException e) {
                    if (retried) {
                        throw e;
                    }
                    table = connection.getTable(fullTableName);
                    retried = true;
                } catch (Throwable e) {
                    TableMetricsManager.updateMetricsForSystemCatalogTableMethod(tableName, NUM_METADATA_LOOKUP_FAILURES, 1);
                    throw e;
                }
            }
        } finally {
            connection.setAutoCommit(wasAutoCommit);
            deleteMutexCells(physicalSchemaName, physicalTableName, acquiredColumnMutexSet);
        }
    }

    public MutationState alterIndex(AlterIndexStatement statement) throws SQLException {
        connection.rollback();
        boolean wasAutoCommit = connection.getAutoCommit();
        String dataTableName;
        long seqNum = 0L;
        try {
            dataTableName = statement.getTableName();
            final String indexName = statement.getTable().getName().getTableName();
            boolean isAsync = statement.isAsync();
            boolean isRebuildAll = statement.isRebuildAll();
            String tenantId = connection.getTenantId() == null ? null : connection.getTenantId().getString();
            PTable table = FromCompiler.getIndexResolver(statement, connection)
                    .getTables().get(0).getTable();

            String schemaName = statement.getTable().getName().getSchemaName();
            String tableName = table.getTableName().getString();

            Map<String, List<Pair<String, Object>>> properties=new HashMap<>(statement.getProps().size());;
            MetaProperties metaProperties = loadStmtProperties(statement.getProps(),properties,table,false);

            boolean isTransformNeeded = TransformClient.checkIsTransformNeeded(metaProperties, schemaName, table, indexName, dataTableName, tenantId, connection);
            MetaPropertiesEvaluated metaPropertiesEvaluated = new MetaPropertiesEvaluated();
            boolean changingPhoenixTableProperty= evaluateStmtProperties(metaProperties,metaPropertiesEvaluated,table,schemaName,tableName);

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
                if (newIndexState == PIndexState.ACTIVE){
                    tableUpsert = connection.prepareStatement(UPDATE_INDEX_STATE_TO_ACTIVE);
                } else {
                    tableUpsert = connection.prepareStatement(UPDATE_INDEX_STATE);
                }
                tableUpsert.setString(1, connection.getTenantId() == null ? null : connection.getTenantId().getString());
                tableUpsert.setString(2, schemaName);
                tableUpsert.setString(3, indexName);
                tableUpsert.setString(4, newIndexState.getSerializedValue());
                tableUpsert.setLong(5, 0);
                if (newIndexState == PIndexState.ACTIVE){
                    tableUpsert.setLong(6, 0);
                }
                tableUpsert.execute();
            } finally {
                if (tableUpsert != null) {
                    tableUpsert.close();
                }
            }
            Long timeStamp = indexRef.getTable().isTransactional() ? indexRef.getTimeStamp() : null;
            List<Mutation> tableMetadata = connection.getMutationState().toMutations(timeStamp).next().getSecond();
            connection.rollback();


            if (changingPhoenixTableProperty) {
                seqNum = incrementTableSeqNum(table,statement.getTableType(), 0, metaPropertiesEvaluated);
                tableMetadata.addAll(connection.getMutationState().toMutations(timeStamp).next().getSecond());
                connection.rollback();
            }

            MetaDataMutationResult result = connection.getQueryServices().updateIndexState(tableMetadata, dataTableName, properties, table);

            try {
                MutationCode code = result.getMutationCode();
                if (code == MutationCode.TABLE_NOT_FOUND) {
                    throw new TableNotFoundException(schemaName, indexName);
                }
                if (code == MutationCode.UNALLOWED_TABLE_MUTATION) {
                    throw new SQLExceptionInfo.Builder(SQLExceptionCode.INVALID_INDEX_STATE_TRANSITION)
                            .setMessage(" currentState=" + indexRef.getTable().getIndexState() + ". requestedState=" + newIndexState)
                            .setSchemaName(schemaName).setTableName(indexName).build().buildException();
                }

                if (isTransformNeeded) {
                    if (indexRef.getTable().getViewIndexId() != null) {
                        throw new SQLExceptionInfo.Builder(SQLExceptionCode.CANNOT_TRANSFORM_LOCAL_OR_VIEW_INDEX)
                                .setSchemaName(schemaName).setTableName(indexName).build().buildException();
                    }
                    try {
                        TransformClient.addTransform(connection, tenantId, table, metaProperties, seqNum, PTable.TransformType.METADATA_TRANSFORM);
                    } catch (SQLException ex) {
                        connection.rollback();
                        throw ex;
                    }
                }

                if (code == MutationCode.TABLE_ALREADY_EXISTS) {
                    if (result.getTable() != null) { // To accommodate connection-less update of index state
                        addTableToCache(result);
                        // Set so that we get the table below with the potentially modified rowKeyOrderOptimizable flag set
                        indexRef.setTable(result.getTable());
                        if (newIndexState == PIndexState.BUILDING && isAsync) {
                            if (isRebuildAll) {
                                List<Task.TaskRecord> tasks = Task.queryTaskTable(connection, null, schemaName, tableName, PTable.TaskType.INDEX_REBUILD,
                                        tenantId, indexName);
                                if (tasks == null || tasks.size() == 0) {
                                    Timestamp ts = new Timestamp(EnvironmentEdgeManager.currentTimeMillis());
                                    Map<String, Object> props = new HashMap<String, Object>() {{
                                        put(INDEX_NAME, indexName);
                                        put(REBUILD_ALL, true);
                                    }};
                                    try {
                                        String json = JacksonUtil.getObjectWriter().writeValueAsString(props);
                                        List<Mutation> sysTaskUpsertMutations = Task.getMutationsForAddTask(new SystemTaskParams.SystemTaskParamsBuilder()
                                                .setConn(connection)
                                                .setTaskType(
                                                        PTable.TaskType.INDEX_REBUILD)
                                                .setTenantId(tenantId)
                                                .setSchemaName(schemaName)
                                                .setTableName(dataTableName)
                                                .setTaskStatus(
                                                        PTable.TaskStatus.CREATED.toString())
                                                .setData(json)
                                                .setPriority(null)
                                                .setStartTs(ts)
                                                .setEndTs(null)
                                                .setAccessCheckEnabled(true)
                                                .build());
                                        byte[] rowKey = sysTaskUpsertMutations
                                                .get(0).getRow();
                                        MetaDataMutationResult metaDataMutationResult =
                                                Task.taskMetaDataCoprocessorExec(connection, rowKey,
                                                        new TaskMetaDataServiceCallBack(sysTaskUpsertMutations));
                                        if (MutationCode.UNABLE_TO_UPSERT_TASK.equals(
                                                metaDataMutationResult.getMutationCode())) {
                                            throw new SQLExceptionInfo.Builder(SQLExceptionCode.UNABLE_TO_UPSERT_TASK)
                                                    .setSchemaName(SYSTEM_SCHEMA_NAME)
                                                    .setTableName(SYSTEM_TASK_TABLE).build().buildException();
                                        }
                                    } catch (IOException e) {
                                        throw new SQLException("Exception happened while adding a System.Task" + e.toString());
                                    }
                                }
                            } else {
                                try {
                                    tableUpsert = connection.prepareStatement(UPDATE_INDEX_REBUILD_ASYNC_STATE);
                                    tableUpsert.setString(1, connection.getTenantId() == null ?
                                            null :
                                            connection.getTenantId().getString());
                                    tableUpsert.setString(2, schemaName);
                                    tableUpsert.setString(3, indexName);
                                    long beginTimestamp = result.getTable().getTimeStamp();
                                    tableUpsert.setLong(4, beginTimestamp);
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
                }
                if (newIndexState == PIndexState.BUILDING && !isAsync) {
                    PTable index = indexRef.getTable();
                    // First delete any existing rows of the index
                    if (IndexUtil.isGlobalIndex(index) && index.getViewIndexId() == null) {
                        //for a global index of a normal base table, it's safe to just truncate and
                        //rebuild. We preserve splits to reduce the amount of splitting we need to do
                        //during rebuild
                        org.apache.hadoop.hbase.TableName physicalTableName =
                                org.apache.hadoop.hbase.TableName.valueOf(index.getPhysicalName().getBytes());
                        try (Admin admin = connection.getQueryServices().getAdmin()) {
                            admin.disableTable(physicalTableName);
                            admin.truncateTable(physicalTableName, true);
                            //trunateTable automatically re-enables when it's done
                        } catch (IOException ie) {
                            String failedTable = physicalTableName.getNameAsString();
                            throw new SQLExceptionInfo.Builder(SQLExceptionCode.UNKNOWN_ERROR_CODE).
                                    setMessage("Error when truncating index table [" + failedTable +
                                            "] before rebuilding: " + ie.getMessage()).
                                    setTableName(failedTable).build().buildException();
                        }
                    } else {
                        Long scn = connection.getSCN();
                        long ts = scn == null ? HConstants.LATEST_TIMESTAMP : scn;
                        MutationPlan plan = new PostDDLCompiler(connection)
                            .compile(Collections.singletonList(indexRef), null,
                                null, Collections.<PColumn>emptyList(), ts);
                        connection.getQueryServices().updateData(plan);
                    }
                    NamedTableNode dataTableNode = NamedTableNode.create(null,
                            TableName.create(schemaName, dataTableName), Collections.<ColumnDef>emptyList());
                    // Next rebuild the index
                    connection.setAutoCommit(true);
                    if (connection.getSCN() != null) {
                        return buildIndexAtTimeStamp(index, dataTableNode);
                    }
                    TableRef dataTableRef = FromCompiler.getResolver(dataTableNode, connection).getTables().get(0);
                    return buildIndex(index, dataTableRef);
                }

                return new MutationState(1, 1000, connection);
            } catch (Throwable e) {
                TableMetricsManager.updateMetricsForSystemCatalogTableMethod(dataTableName, NUM_METADATA_LOOKUP_FAILURES, 1);
                throw e;
            }
        } catch (TableNotFoundException e) {
            if (!statement.ifExists()) {
                throw e;
            }
            return new MutationState(0, 0, connection);
        } finally {
            connection.setAutoCommit(wasAutoCommit);
        }
    }

    private void addTableToCache(MetaDataMutationResult result) throws SQLException {
        addTableToCache(result, TransactionUtil.getResolvedTime(connection, result));
    }

    private void addTableToCache(MetaDataMutationResult result, long timestamp) throws SQLException {
        addColumnsAndIndexesFromAncestors(result, null, false);
        PTable table = result.getTable();
        connection.addTable(table, timestamp);
    }

    private void addFunctionToCache(MetaDataMutationResult result) throws SQLException {
        for (PFunction function: result.getFunctions()) {
            connection.addFunction(function);
        }
    }

    private void addSchemaToCache(MetaDataMutationResult result) throws SQLException {
        connection.addSchema(result.getSchema());
    }

    private void throwIfLastPKOfParentIsVariableLength(PTable parent, String viewSchemaName, String viewName, ColumnDef col) throws SQLException {
        // if the last pk column is variable length then we read all the
        // bytes of the rowkey without looking for a separator byte see
        // https://issues.apache.org/jira/browse/PHOENIX-978?focusedCommentId=14617847&page=com.atlassian.jira.plugin.system.issuetabpanels%3Acomment-tabpanel#comment-14617847
        // so we cannot add a pk column to a view if the last pk column of the parent is variable length
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
        return connection
                .getTable(new PTableKey(view.getTenantId(), view.getParentName().getString()));
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
            PSchema schema = new PSchema(create.getSchemaName());
            // Use SchemaName from PSchema object to get the normalized SchemaName
            // See PHOENIX-4424 for details
            validateSchema(schema.getSchemaName());
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
            try {
                switch (code) {
                    case SCHEMA_ALREADY_EXISTS:
                        if (result.getSchema() != null) {
                            addSchemaToCache(result);
                        }
                        if (!isIfNotExists) {
                            throw new SchemaAlreadyExistsException(schema.getSchemaName());
                        }
                        break;
                    case NEWER_SCHEMA_FOUND:
                        throw new NewerSchemaAlreadyExistsException(schema.getSchemaName());
                    default:
                        result = new MetaDataMutationResult(code, schema, result.getMutationTime());
                        addSchemaToCache(result);
                }
            } catch(Throwable e) {
                TableMetricsManager.updateMetricsForSystemCatalogTableMethod(null, NUM_METADATA_LOOKUP_FAILURES, 1);
                throw e;
            }
        } finally {
            connection.setAutoCommit(wasAutoCommit);
        }
        return new MutationState(0, 0, connection);
    }

    private void validateSchema(String schemaName) throws SQLException {
        if (SchemaUtil.NOT_ALLOWED_SCHEMA_LIST.contains(schemaName)) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.SCHEMA_NOT_ALLOWED)
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
            try {
                switch (code) {
                    case SCHEMA_NOT_FOUND:
                        if (!ifExists) {
                            throw new SchemaNotFoundException(schemaName);
                        }
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
            } catch (Throwable e) {
                TableMetricsManager.updateMetricsForSystemCatalogTableMethod(null, NUM_METADATA_LOOKUP_FAILURES, 1);
                throw e;
            }
            return new MutationState(0, 0, connection);
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
        return new MutationState(0, 0, connection);
    }

    private MetaProperties loadStmtProperties(ListMultimap<String, Pair<String, Object>> stmtProperties, Map<String, List<Pair<String, Object>>> properties, PTable table, boolean removeTableProps)
            throws SQLException {
        MetaProperties metaProperties = new MetaProperties();
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
                        metaProperties.setImmutableRowsProp((Boolean)value);
                    } else if (propName.equals(PhoenixDatabaseMetaData.MULTI_TENANT)) {
                        metaProperties.setMultiTenantProp((Boolean)value);
                    } else if (propName.equals(DISABLE_WAL)) {
                        metaProperties.setDisableWALProp((Boolean)value);
                    } else if (propName.equals(STORE_NULLS)) {
                        metaProperties.setStoreNullsProp((Boolean)value);
                    } else if (propName.equals(TRANSACTIONAL)) {
                        metaProperties.setIsTransactionalProp((Boolean)value);
                    } else if (propName.equals(TRANSACTION_PROVIDER)) {
                        metaProperties.setTransactionProviderProp((TransactionFactory.Provider) value);
                    } else if (propName.equals(UPDATE_CACHE_FREQUENCY)) {
                        metaProperties.setUpdateCacheFrequencyProp((Long)value);
                    } else if (propName.equals(PHYSICAL_TABLE_NAME)) {
                        metaProperties.setPhysicalTableNameProp((String) value);
                    } else if (propName.equals(GUIDE_POSTS_WIDTH)) {
                        metaProperties.setGuidePostWidth((Long)value);
                    } else if (propName.equals(APPEND_ONLY_SCHEMA)) {
                        metaProperties.setAppendOnlySchemaProp((Boolean) value);
                    } else if (propName.equalsIgnoreCase(IMMUTABLE_STORAGE_SCHEME)) {
                        metaProperties.setImmutableStorageSchemeProp((ImmutableStorageScheme)value);
                    } else if (propName.equalsIgnoreCase(COLUMN_ENCODED_BYTES)) {
                        metaProperties.setColumnEncodedBytesProp(QualifierEncodingScheme.fromSerializedValue((byte)value));
                    } else if (propName.equalsIgnoreCase(USE_STATS_FOR_PARALLELIZATION)) {
                        metaProperties.setUseStatsForParallelizationProp((Boolean)value);
                    } else if (propName.equalsIgnoreCase(PHOENIX_TTL)) {
                        metaProperties.setPhoenixTTL((Long)value);
                    } else if (propName.equalsIgnoreCase(CHANGE_DETECTION_ENABLED)) {
                        metaProperties.setChangeDetectionEnabled((Boolean) value);
                    } else if (propName.equalsIgnoreCase(PHYSICAL_TABLE_NAME)) {
                        metaProperties.setPhysicalTableName((String) value);
                    } else if (propName.equalsIgnoreCase(SCHEMA_VERSION)) {
                        metaProperties.setSchemaVersion((String) value);
                    } else if (propName.equalsIgnoreCase(STREAMING_TOPIC_NAME)) {
                        metaProperties.setStreamingTopicName((String) value);
                    }
                }
                // if removeTableProps is true only add the property if it is not an HTable or Phoenix Table property
                if (!removeTableProps || (!TableProperty.isPhoenixTableProperty(propName) && !MetaDataUtil.isHTableProperty(propName))) {
                    propsList.add(prop);
                }
            }
            properties.put(family, propsList);
        }
        return metaProperties;
    }

    private boolean evaluateStmtProperties(MetaProperties metaProperties, MetaPropertiesEvaluated metaPropertiesEvaluated, PTable table, String schemaName, String tableName)
            throws SQLException {
        boolean changingPhoenixTableProperty = false;

        if (metaProperties.getImmutableRowsProp() != null) {
            if (metaProperties.getImmutableRowsProp().booleanValue() != table.isImmutableRows()) {
                if (table.getImmutableStorageScheme() != ImmutableStorageScheme.ONE_CELL_PER_COLUMN) {
                    throw new SQLExceptionInfo.Builder(SQLExceptionCode.CANNOT_ALTER_IMMUTABLE_ROWS_PROPERTY)
                            .setSchemaName(schemaName).setTableName(tableName).build().buildException();
                }
                metaPropertiesEvaluated.setIsImmutableRows(metaProperties.getImmutableRowsProp());
                changingPhoenixTableProperty = true;
            }
        }

        if (metaProperties.getImmutableRowsProp() != null && table.getType() != INDEX) {
            if (metaProperties.getImmutableRowsProp().booleanValue() != table.isImmutableRows()) {
                metaPropertiesEvaluated.setIsImmutableRows(metaProperties.getImmutableRowsProp());
                changingPhoenixTableProperty = true;
            }
        }

        if (metaProperties.getMultiTenantProp() != null) {
            if (metaProperties.getMultiTenantProp().booleanValue() != table.isMultiTenant()) {
                metaPropertiesEvaluated.setMultiTenant(metaProperties.getMultiTenantProp());
                changingPhoenixTableProperty = true;
            }
        }

        if (metaProperties.getDisableWALProp() != null) {
            if (metaProperties.getDisableWALProp().booleanValue() != table.isWALDisabled()) {
                metaPropertiesEvaluated.setDisableWAL(metaProperties.getDisableWALProp());
                changingPhoenixTableProperty = true;
            }
        }

        if (metaProperties.getUpdateCacheFrequencyProp() != null) {
            // See PHOENIX-4891
            if (table.getType() == PTableType.INDEX) {
                throw new SQLExceptionInfo.Builder(
                        SQLExceptionCode.CANNOT_SET_OR_ALTER_UPDATE_CACHE_FREQ_FOR_INDEX)
                        .build()
                        .buildException();
            }
            if (metaProperties.getUpdateCacheFrequencyProp().longValue() != table.getUpdateCacheFrequency()) {
                metaPropertiesEvaluated.setUpdateCacheFrequency(metaProperties.getUpdateCacheFrequencyProp());
                changingPhoenixTableProperty = true;
            }
        }

        if (metaProperties.getAppendOnlySchemaProp() !=null) {
            if (metaProperties.getAppendOnlySchemaProp() != table.isAppendOnlySchema()) {
                metaPropertiesEvaluated.setAppendOnlySchema(metaProperties.getAppendOnlySchemaProp());
                changingPhoenixTableProperty = true;
            }
        }

        if (metaProperties.getColumnEncodedBytesProp() != null) {
            if (metaProperties.getColumnEncodedBytesProp() != table.getEncodingScheme()) {
                // Transform is needed, so we will not be setting it here. We set the boolean to increment sequence num
                changingPhoenixTableProperty = true;
            }
        }

        if (metaProperties.getImmutableStorageSchemeProp()!=null) {
            if (metaProperties.getImmutableStorageSchemeProp() != table.getImmutableStorageScheme()) {
                // Transform is needed, so we will not be setting it here. We set the boolean to increment sequence num
                changingPhoenixTableProperty = true;
            }
        }

        // Get immutableStorageScheme and encoding and check compatibility
        ImmutableStorageScheme immutableStorageScheme = table.getImmutableStorageScheme();
        if (metaProperties.getImmutableStorageSchemeProp() != null) {
            immutableStorageScheme = metaProperties.getImmutableStorageSchemeProp();
        }
        QualifierEncodingScheme encodingScheme = table.getEncodingScheme();
        if (metaProperties.getColumnEncodedBytesProp() != null) {
            encodingScheme = metaProperties.getColumnEncodedBytesProp();
        }
        if (immutableStorageScheme == SINGLE_CELL_ARRAY_WITH_OFFSETS && encodingScheme == NON_ENCODED_QUALIFIERS) {
            // encoding scheme is set as non-encoded on purpose, so we should fail
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.INVALID_IMMUTABLE_STORAGE_SCHEME_AND_COLUMN_QUALIFIER_BYTES)
                    .setSchemaName(schemaName).setTableName(tableName).build().buildException();
        }

        if (metaProperties.getGuidePostWidth() == null || metaProperties.getGuidePostWidth() >= 0) {
            metaPropertiesEvaluated.setGuidePostWidth(metaProperties.getGuidePostWidth());
            changingPhoenixTableProperty = true;
        }

        if (metaProperties.getStoreNullsProp() != null) {
            if (metaProperties.getStoreNullsProp().booleanValue() != table.getStoreNulls()) {
                metaPropertiesEvaluated.setStoreNulls(metaProperties.getStoreNullsProp());
                changingPhoenixTableProperty = true;
            }
        }

        if (metaProperties.getUseStatsForParallelizationProp() != null
                && (table.useStatsForParallelization() == null
                || (metaProperties.getUseStatsForParallelizationProp().booleanValue() != table
                .useStatsForParallelization()))) {
            metaPropertiesEvaluated.setUseStatsForParallelization(metaProperties.getUseStatsForParallelizationProp());
            changingPhoenixTableProperty = true;
        }

        if (metaProperties.getIsTransactionalProp() != null) {
            if (metaProperties.getIsTransactionalProp().booleanValue() != table.isTransactional()) {
                metaPropertiesEvaluated.setIsTransactional(metaProperties.getIsTransactionalProp());
                // Note: Going from transactional to non transactional used to be not supportable because
                // it would have required rewriting the cell timestamps and doing a major compaction to
                // remove Tephra specific delete markers. After PHOENIX-6627, Tephra has been removed.
                // For now we continue to reject the request.
                if (!metaPropertiesEvaluated.getIsTransactional()) {
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
                TransactionFactory.Provider provider = metaProperties.getTransactionProviderProp();
                if (provider == null) {
                    provider = (Provider)
                            TableProperty.TRANSACTION_PROVIDER.getValue(
                                    connection.getQueryServices().getProps().get(
                                            QueryServices.DEFAULT_TRANSACTION_PROVIDER_ATTRIB,
                                            QueryServicesOptions.DEFAULT_TRANSACTION_PROVIDER));
                }
                metaPropertiesEvaluated.setTransactionProvider(provider);
                if (provider.getTransactionProvider().isUnsupported(PhoenixTransactionProvider.Feature.ALTER_NONTX_TO_TX)) {
                    throw new SQLExceptionInfo.Builder(SQLExceptionCode.CANNOT_ALTER_TABLE_FROM_NON_TXN_TO_TXNL)
                        .setMessage(provider.name() + ". ")
                        .setSchemaName(schemaName)
                        .setTableName(tableName)
                        .build().buildException();
                }
                changingPhoenixTableProperty = true;
                metaProperties.setNonTxToTx(true);
            }
        }

        if (metaProperties.getPhoenixTTL() != null) {
            if (table.getType() != PTableType.VIEW) {
                throw new SQLExceptionInfo.Builder(
                        SQLExceptionCode.PHOENIX_TTL_SUPPORTED_FOR_VIEWS_ONLY)
                        .build()
                        .buildException();
            }
            if (metaProperties.getPhoenixTTL().longValue() != table.getPhoenixTTL()) {
                metaPropertiesEvaluated.setPhoenixTTL(metaProperties.getPhoenixTTL());
                changingPhoenixTableProperty = true;
            }
        }

        if (metaProperties.isChangeDetectionEnabled() != null) {
            verifyChangeDetectionTableType(table.getType(),
                metaProperties.isChangeDetectionEnabled());
            if (!metaProperties.isChangeDetectionEnabled().equals(table.isChangeDetectionEnabled())) {
                metaPropertiesEvaluated.setChangeDetectionEnabled(metaProperties.isChangeDetectionEnabled());
                changingPhoenixTableProperty = true;
            }
        }

        if (!Strings.isNullOrEmpty(metaProperties.getPhysicalTableNameProp())) {
            if (!metaProperties.getPhysicalTableNameProp().equals(table.getPhysicalName(true))) {
                metaPropertiesEvaluated.setPhysicalTableName(metaProperties.getPhysicalTableNameProp());
                changingPhoenixTableProperty = true;
            }
        }

        if (!Strings.isNullOrEmpty(metaProperties.getSchemaVersion())) {
            if (!metaProperties.getSchemaVersion().equals(table.getSchemaVersion())) {
                metaPropertiesEvaluated.setSchemaVersion(metaProperties.getSchemaVersion());
                changingPhoenixTableProperty = true;
            }
        }

        if (!Strings.isNullOrEmpty(metaProperties.getStreamingTopicName())) {
            if (!metaProperties.getStreamingTopicName().equals(table.getStreamingTopicName())) {
                metaPropertiesEvaluated.
                    setStreamingTopicName(metaProperties.getStreamingTopicName());
                changingPhoenixTableProperty = true;
            }
        }

        return changingPhoenixTableProperty;
    }

    public static class MetaProperties {
        private Boolean isImmutableRowsProp = null;
        private Boolean multiTenantProp = null;
        private Boolean disableWALProp = null;
        private Boolean storeNullsProp = null;
        private TransactionFactory.Provider transactionProviderProp = null;
        private Boolean isTransactionalProp = null;
        private Long updateCacheFrequencyProp = null;
        private String physicalTableNameProp = null;
        private QualifierEncodingScheme columnEncodedBytesProp = null;
        private Boolean appendOnlySchemaProp = null;
        private Long guidePostWidth = -1L;
        private ImmutableStorageScheme immutableStorageSchemeProp = null;
        private Boolean useStatsForParallelizationProp = null;
        private boolean nonTxToTx = false;
        private Long phoenixTTL = null;
        private Boolean isChangeDetectionEnabled = null;
        private String physicalTableName = null;
        private String schemaVersion = null;
        private String streamingTopicName = null;

        public Boolean getImmutableRowsProp() {
            return isImmutableRowsProp;
        }

        public void setImmutableRowsProp(Boolean isImmutableRowsProp) {
            this.isImmutableRowsProp = isImmutableRowsProp;
        }

        public Boolean getMultiTenantProp() {
            return multiTenantProp;
        }

        public void setMultiTenantProp(Boolean multiTenantProp) {
            this.multiTenantProp = multiTenantProp;
        }

        public Boolean getDisableWALProp() {
            return disableWALProp;
        }

        public void setDisableWALProp(Boolean disableWALProp) {
            this.disableWALProp = disableWALProp;
        }

        public Boolean getStoreNullsProp() {
            return storeNullsProp;
        }

        public void setStoreNullsProp(Boolean storeNullsProp) {
            this.storeNullsProp = storeNullsProp;
        }

        public TransactionFactory.Provider getTransactionProviderProp() {
            return transactionProviderProp;
        }

        public void setTransactionProviderProp(TransactionFactory.Provider transactionProviderProp) {
            this.transactionProviderProp = transactionProviderProp;
        }

        public Boolean getIsTransactionalProp() {
            return isTransactionalProp;
        }

        public void setIsTransactionalProp(Boolean isTransactionalProp) {
            this.isTransactionalProp = isTransactionalProp;
        }

        public void setPhysicalTableNameProp(String physicalTableNameProp) {
            this.physicalTableNameProp = physicalTableNameProp;
        }

        public String getPhysicalTableNameProp() {
            return this.physicalTableNameProp;
        }

        public Long getUpdateCacheFrequencyProp() {
            return updateCacheFrequencyProp;
        }

        public void setUpdateCacheFrequencyProp(Long updateCacheFrequencyProp) {
            this.updateCacheFrequencyProp = updateCacheFrequencyProp;
        }

        public Boolean getAppendOnlySchemaProp() {
            return appendOnlySchemaProp;
        }

        public void setAppendOnlySchemaProp(Boolean appendOnlySchemaProp) {
            this.appendOnlySchemaProp = appendOnlySchemaProp;
        }

        public Long getGuidePostWidth() {
            return guidePostWidth;
        }

        public void setGuidePostWidth(Long guidePostWidth) {
            this.guidePostWidth = guidePostWidth;
        }

        public ImmutableStorageScheme getImmutableStorageSchemeProp() {
            return immutableStorageSchemeProp;
        }

        public void setImmutableStorageSchemeProp(
                ImmutableStorageScheme immutableStorageSchemeProp) {
            this.immutableStorageSchemeProp = immutableStorageSchemeProp;
        }

        public QualifierEncodingScheme getColumnEncodedBytesProp() {
            return columnEncodedBytesProp;
        }

        public void setColumnEncodedBytesProp(
                QualifierEncodingScheme columnEncodedBytesProp) {
            this.columnEncodedBytesProp = columnEncodedBytesProp;
        }

        public Boolean getUseStatsForParallelizationProp() {
            return useStatsForParallelizationProp;
        }

        public void setUseStatsForParallelizationProp(Boolean useStatsForParallelizationProp) {
            this.useStatsForParallelizationProp = useStatsForParallelizationProp;
        }

        public boolean getNonTxToTx() {
            return nonTxToTx;
        }

        public void setNonTxToTx(boolean nonTxToTx) {
            this.nonTxToTx = nonTxToTx;
        }

        public Long getPhoenixTTL() { return phoenixTTL; }

        public void setPhoenixTTL(Long phoenixTTL) { this.phoenixTTL = phoenixTTL; }

        public Boolean isChangeDetectionEnabled() {
            return isChangeDetectionEnabled;
        }

        public void setChangeDetectionEnabled(Boolean isChangeDetectionEnabled) {
            this.isChangeDetectionEnabled = isChangeDetectionEnabled;
        }

        public String getPhysicalTableName() {
            return physicalTableName;
        }

        public void setPhysicalTableName(String physicalTableName) {
            this.physicalTableName = physicalTableName;
        }

        public String getSchemaVersion() {
            return schemaVersion;
        }

        public void setSchemaVersion(String schemaVersion) {
            this.schemaVersion = schemaVersion;
        }

        public String getStreamingTopicName() { return streamingTopicName; }

        public void setStreamingTopicName(String streamingTopicName) {
            this.streamingTopicName = streamingTopicName;
        }
    }

    private static class MetaPropertiesEvaluated {
        private Boolean isImmutableRows;
        private Boolean multiTenant = null;
        private Boolean disableWAL = null;
        private Long updateCacheFrequency = null;
        private Boolean appendOnlySchema = null;
        private Long guidePostWidth = -1L;
        private ImmutableStorageScheme immutableStorageScheme = null;
        private QualifierEncodingScheme columnEncodedBytes = null;
        private Boolean storeNulls = null;
        private Boolean useStatsForParallelization = null;
        private Boolean isTransactional = null;
        private TransactionFactory.Provider transactionProvider = null;
        private Long phoenixTTL = null;
        private Boolean isChangeDetectionEnabled = null;
        private String physicalTableName = null;
        private String schemaVersion = null;
        private String streamingTopicName = null;

        public Boolean getIsImmutableRows() {
            return isImmutableRows;
        }

        public void setIsImmutableRows(Boolean isImmutableRows) {
            this.isImmutableRows = isImmutableRows;
        }

        public Boolean getMultiTenant() {
            return multiTenant;
        }

        public void setMultiTenant(Boolean multiTenant) {
            this.multiTenant = multiTenant;
        }

        public Boolean getDisableWAL() {
            return disableWAL;
        }

        public void setDisableWAL(Boolean disableWAL) {
            this.disableWAL = disableWAL;
        }

        public Long getUpdateCacheFrequency() {
            return updateCacheFrequency;
        }

        public void setUpdateCacheFrequency(Long updateCacheFrequency) {
            this.updateCacheFrequency = updateCacheFrequency;
        }

        public Boolean getAppendOnlySchema() {
            return appendOnlySchema;
        }

        public void setAppendOnlySchema(Boolean appendOnlySchema) {
            this.appendOnlySchema = appendOnlySchema;
        }

        public Long getGuidePostWidth() {
            return guidePostWidth;
        }

        public void setGuidePostWidth(Long guidePostWidth) {
            this.guidePostWidth = guidePostWidth;
        }

        public ImmutableStorageScheme getImmutableStorageScheme() {
            return immutableStorageScheme;
        }

        public void setImmutableStorageScheme(ImmutableStorageScheme immutableStorageScheme) {
            this.immutableStorageScheme = immutableStorageScheme;
        }

        public QualifierEncodingScheme getColumnEncodedBytes() {
            return columnEncodedBytes;
        }

        public void setColumnEncodedBytes(QualifierEncodingScheme columnEncodedBytes) {
            this.columnEncodedBytes = columnEncodedBytes;
        }
        public Boolean getStoreNulls() {
            return storeNulls;
        }

        public void setStoreNulls(Boolean storeNulls) {
            this.storeNulls = storeNulls;
        }

        public Boolean getUseStatsForParallelization() {
            return useStatsForParallelization;
        }

        public void setUseStatsForParallelization(Boolean useStatsForParallelization) {
            this.useStatsForParallelization = useStatsForParallelization;
        }

        public Boolean getIsTransactional() {
            return isTransactional;
        }

        public void setIsTransactional(Boolean isTransactional) {
            this.isTransactional = isTransactional;
        }
        
        public TransactionFactory.Provider getTransactionProvider() {
            return transactionProvider;
        }

        public void setTransactionProvider(TransactionFactory.Provider transactionProvider) {
            this.transactionProvider = transactionProvider;
        }

        public Long getPhoenixTTL() { return phoenixTTL; }

        public void setPhoenixTTL(Long phoenixTTL) { this.phoenixTTL = phoenixTTL; }

        public Boolean isChangeDetectionEnabled() {
            return isChangeDetectionEnabled;
        }

        public void setChangeDetectionEnabled(Boolean isChangeDetectionEnabled) {
            this.isChangeDetectionEnabled = isChangeDetectionEnabled;
        }

        public String getPhysicalTableName() {
            return physicalTableName;
        }

        public void setPhysicalTableName(String physicalTableName) {
            this.physicalTableName = physicalTableName;
        }

        public String getSchemaVersion() {
            return schemaVersion;
        }

        public void setSchemaVersion(String schemaVersion) {
            this.schemaVersion = schemaVersion;
        }

        public String getStreamingTopicName() { return streamingTopicName; }

        public void setStreamingTopicName(String streamingTopicName) {
            this.streamingTopicName = streamingTopicName;
        }
    }


    /**
     * GRANT/REVOKE statements use this method to update HBase acl's
     * Perms can be changed at Schema, Table or User level
     * @throws SQLException
     */
    public MutationState changePermissions(ChangePermsStatement changePermsStatement) throws SQLException {

        LOGGER.info(changePermsStatement.toString());

        try(Admin admin = connection.getQueryServices().getAdmin()) {
            ClusterConnection clusterConnection = (ClusterConnection) admin.getConnection();

            if (changePermsStatement.getSchemaName() != null) {
                // SYSTEM.CATALOG doesn't have any entry for "default" HBase namespace, hence we will bypass the check
                if (!changePermsStatement.getSchemaName()
                        .equals(SchemaUtil.SCHEMA_FOR_DEFAULT_NAMESPACE)) {
                    FromCompiler.getResolverForSchema(changePermsStatement.getSchemaName(),
                            connection);
                }

                changePermsOnSchema(clusterConnection, changePermsStatement);
            } else if (changePermsStatement.getTableName() != null) {
                PTable inputTable = connection.getTable(SchemaUtil.
                        normalizeFullTableName(changePermsStatement.getTableName().toString()));
                if (!(PTableType.TABLE.equals(inputTable.getType()) || PTableType.SYSTEM.equals(inputTable.getType()))) {
                    throw new AccessDeniedException("Cannot GRANT or REVOKE permissions on INDEX TABLES or VIEWS");
                }

                // Changing perms on base table and update the perms for global and view indexes
                // Views and local indexes are not physical tables and hence update perms is not needed
                changePermsOnTables(clusterConnection, admin, changePermsStatement, inputTable);
            } else {

                // User can be given perms at the global level
                changePermsOnUser(clusterConnection, changePermsStatement);
            }

        } catch (SQLException e) {
            // Bubble up the SQL Exception
            throw e;
        } catch (Throwable throwable) {
            // To change perms, the user must have ADMIN perms on that scope, otherwise it throws ADE
            // Wrap around ADE and other exceptions to PhoenixIOException
            throw ClientUtil.parseServerException(throwable);
        }

        return new MutationState(0, 0, connection);
    }

    private void changePermsOnSchema(ClusterConnection clusterConnection, ChangePermsStatement changePermsStatement) throws Throwable {
        if (changePermsStatement.isGrantStatement()) {
            AccessControlClient.grant(clusterConnection, changePermsStatement.getSchemaName(), changePermsStatement.getName(), changePermsStatement.getPermsList());
        } else {
            AccessControlClient.revoke(clusterConnection, changePermsStatement.getSchemaName(), changePermsStatement.getName(), Permission.Action.values());
        }
    }

    private void changePermsOnTables(ClusterConnection clusterConnection, Admin admin, ChangePermsStatement changePermsStatement, PTable inputTable) throws Throwable {

        org.apache.hadoop.hbase.TableName tableName = SchemaUtil.getPhysicalTableName
                (inputTable.getPhysicalName().getBytes(), inputTable.isNamespaceMapped());

        changePermsOnTable(clusterConnection, changePermsStatement, tableName);

        boolean schemaInconsistency = false;
        List<PTable> inconsistentTables = null;

        for (PTable indexTable : inputTable.getIndexes()) {
            // Local Indexes don't correspond to new physical table, they are just stored in separate CF of base table.
            if (indexTable.getIndexType().equals(IndexType.LOCAL)) {
                continue;
            }
            if (inputTable.isNamespaceMapped() != indexTable.isNamespaceMapped()) {
                schemaInconsistency = true;
                if (inconsistentTables == null) {
                    inconsistentTables = new ArrayList<>();
                }
                inconsistentTables.add(indexTable);
                continue;
            }
            LOGGER.info("Updating permissions for Index Table: " +
                    indexTable.getName() + " Base Table: " + inputTable.getName());
            tableName = SchemaUtil.getPhysicalTableName(indexTable.getPhysicalName().getBytes(), indexTable.isNamespaceMapped());
            changePermsOnTable(clusterConnection, changePermsStatement, tableName);
        }

        if (schemaInconsistency) {
            for (PTable table : inconsistentTables) {
                LOGGER.error("Fail to propagate permissions to Index Table: " + table.getName());
            }
            throw new TablesNotInSyncException(inputTable.getTableName().getString(),
                    inconsistentTables.get(0).getTableName().getString(), "Namespace properties");
        }

        // There will be only a single View Index Table for all the indexes created on views
        byte[] viewIndexTableBytes = MetaDataUtil.getViewIndexPhysicalName(inputTable.getPhysicalName().getBytes());
        tableName = org.apache.hadoop.hbase.TableName.valueOf(viewIndexTableBytes);
        boolean viewIndexTableExists = admin.tableExists(tableName);
        if (viewIndexTableExists) {
            LOGGER.info("Updating permissions for View Index Table: " +
                    Bytes.toString(viewIndexTableBytes) + " Base Table: " + inputTable.getName());
            changePermsOnTable(clusterConnection, changePermsStatement, tableName);
        } else {
            if (inputTable.isMultiTenant()) {
                LOGGER.error("View Index Table not found for MultiTenant Table: " + inputTable.getName());
                LOGGER.error("Fail to propagate permissions to view Index Table: " + tableName.getNameAsString());
                throw new TablesNotInSyncException(inputTable.getTableName().getString(),
                        Bytes.toString(viewIndexTableBytes), " View Index table should exist for MultiTenant tables");
            }
        }
    }

    private void changePermsOnTable(ClusterConnection clusterConnection, ChangePermsStatement changePermsStatement, org.apache.hadoop.hbase.TableName tableName)
            throws Throwable {
        if (changePermsStatement.isGrantStatement()) {
            AccessControlClient.grant(clusterConnection, tableName, changePermsStatement.getName(),
                    null, null, changePermsStatement.getPermsList());
        } else {
            AccessControlClient.revoke(clusterConnection, tableName, changePermsStatement.getName(),
                    null, null, Permission.Action.values());
        }
    }

    private void changePermsOnUser(ClusterConnection clusterConnection, ChangePermsStatement changePermsStatement)
            throws Throwable {
        if (changePermsStatement.isGrantStatement()) {
            AccessControlClient.grant(clusterConnection, changePermsStatement.getName(), changePermsStatement.getPermsList());
        } else {
            AccessControlClient.revoke(clusterConnection, changePermsStatement.getName(), Permission.Action.values());
        }
    }
}
