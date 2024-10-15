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

import static org.apache.hadoop.hbase.KeyValueUtil.createFirstOnRow;
import static org.apache.phoenix.coprocessor.generated.MetaDataProtos.MutationCode.UNABLE_TO_CREATE_CHILD_LINK;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.APPEND_ONLY_SCHEMA_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.ARRAY_SIZE_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.AUTO_PARTITION_SEQ_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.CHANGE_DETECTION_ENABLED_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.CLASS_NAME_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_COUNT_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_DEF_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_QUALIFIER_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_QUALIFIER_COUNTER_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_SIZE_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.DATA_TABLE_NAME_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.DATA_TYPE_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.DECIMAL_DIGITS_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.DEFAULT_COLUMN_FAMILY_NAME_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.DEFAULT_VALUE_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.DISABLE_WAL_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.ENCODING_SCHEME_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.EXTERNAL_SCHEMA_ID_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.IMMUTABLE_ROWS_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.INDEX_DISABLE_TIMESTAMP_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.INDEX_STATE_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.INDEX_TYPE_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.INDEX_WHERE_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.IS_ARRAY_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.IS_CONSTANT_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.IS_NAMESPACE_MAPPED_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.IS_ROW_TIMESTAMP_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.IS_VIEW_REFERENCED_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.JAR_PATH_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.LAST_DDL_TIMESTAMP_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.LINK_TYPE_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.MAX_VALUE_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.MIN_PHOENIX_TTL_HWM;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.MIN_VALUE_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.MULTI_TENANT_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.NULLABLE_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.NUM_ARGS_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.ORDINAL_POSITION_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.PHOENIX_TTL_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.PHOENIX_TTL_HWM_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.PHOENIX_TTL_NOT_DEFINED;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.PHYSICAL_TABLE_NAME_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.PK_NAME_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.RETURN_TYPE_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SALT_BUCKETS_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SCHEMA_VERSION_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SORT_ORDER_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.STORAGE_SCHEME_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.STORE_NULLS_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.STREAMING_TOPIC_NAME_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CHILD_LINK_NAME_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_SEQ_NUM_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_TYPE_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TRANSACTIONAL_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TRANSACTION_PROVIDER_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TYPE_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.UPDATE_CACHE_FREQUENCY_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.USE_STATS_FOR_PARALLELIZATION_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.VIEW_CONSTANT_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.VIEW_INDEX_ID_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.VIEW_INDEX_ID_DATA_TYPE_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.VIEW_STATEMENT_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.VIEW_TYPE_BYTES;
import static org.apache.phoenix.schema.PTableImpl.getColumnsToClone;
import static org.apache.phoenix.schema.PTableType.INDEX;
import static org.apache.phoenix.util.PhoenixRuntime.TENANT_ID_ATTRIB;
import static org.apache.phoenix.util.SchemaUtil.getVarCharLength;
import static org.apache.phoenix.util.SchemaUtil.getVarChars;
import static org.apache.phoenix.util.ViewUtil.findAllDescendantViews;
import static org.apache.phoenix.util.ViewUtil.getSystemTableForChildLinks;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.PrivilegedExceptionAction;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellComparatorImpl;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.ExtendedCellBuilder;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TagUtil;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.CoreCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.ipc.RpcCall;
import org.apache.hadoop.hbase.ipc.RpcUtil;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.Region.RowLock;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.apache.phoenix.cache.GlobalCache;
import org.apache.phoenix.cache.GlobalCache.FunctionBytesPtr;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.compile.ScanRanges;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos.AddColumnRequest;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos.ClearCacheRequest;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos.ClearCacheResponse;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos.ClearTableFromCacheRequest;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos.ClearTableFromCacheResponse;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos.CreateFunctionRequest;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos.CreateSchemaRequest;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos.CreateTableRequest;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos.DropColumnRequest;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos.DropFunctionRequest;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos.DropSchemaRequest;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos.DropTableRequest;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos.GetFunctionsRequest;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos.GetSchemaRequest;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos.GetTableRequest;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos.GetVersionRequest;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos.GetVersionResponse;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos.MetaDataResponse;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos.UpdateIndexStateRequest;
import org.apache.phoenix.coprocessorclient.MetaDataEndpointImplConstants;
import org.apache.phoenix.coprocessorclient.MetaDataProtocol;
import org.apache.phoenix.coprocessorclient.TableInfo;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.KeyValueColumnExpression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.expression.ProjectedColumnExpression;
import org.apache.phoenix.expression.RowKeyColumnExpression;
import org.apache.phoenix.expression.visitor.StatelessTraverseAllExpressionVisitor;
import org.apache.phoenix.hbase.index.LockManager;
import org.apache.phoenix.hbase.index.covered.update.ColumnReference;
import org.apache.phoenix.hbase.index.util.GenericKeyValueBuilder;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.hbase.index.util.KeyValueBuilder;
import org.apache.phoenix.index.IndexMaintainer;
import org.apache.phoenix.iterate.ResultIterator;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.jdbc.PhoenixResultSet;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.mapreduce.util.ConnectionUtil;
import org.apache.phoenix.metrics.Metrics;
import org.apache.phoenix.parse.LiteralParseNode;
import org.apache.phoenix.parse.PFunction;
import org.apache.phoenix.parse.PFunction.FunctionArgument;
import org.apache.phoenix.parse.PSchema;
import org.apache.phoenix.protobuf.ProtobufUtil;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.MetaDataSplitPolicy;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PColumnFamily;
import org.apache.phoenix.schema.PColumnImpl;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PMetaDataEntity;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PNameFactory;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTable.EncodedCQCounter;
import org.apache.phoenix.schema.PTable.ImmutableStorageScheme;
import org.apache.phoenix.schema.PTable.IndexType;
import org.apache.phoenix.schema.PTable.LinkType;
import org.apache.phoenix.schema.PTable.QualifierEncodingScheme;
import org.apache.phoenix.schema.PTable.ViewType;
import org.apache.phoenix.schema.PTableImpl;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.RowKeySchema;
import org.apache.phoenix.schema.SequenceAllocation;
import org.apache.phoenix.schema.SequenceAlreadyExistsException;
import org.apache.phoenix.schema.SequenceKey;
import org.apache.phoenix.schema.SequenceNotFoundException;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.phoenix.schema.export.SchemaRegistryRepository;
import org.apache.phoenix.schema.export.SchemaRegistryRepositoryFactory;
import org.apache.phoenix.schema.export.SchemaWriter;
import org.apache.phoenix.schema.export.SchemaWriterFactory;
import org.apache.phoenix.schema.metrics.MetricsMetadataSource;
import org.apache.phoenix.schema.metrics.MetricsMetadataSourceFactory;
import org.apache.phoenix.schema.task.ServerTask;
import org.apache.phoenix.schema.task.SystemTaskParams;
import org.apache.phoenix.schema.types.PBinary;
import org.apache.phoenix.schema.types.PBoolean;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.schema.types.PTinyint;
import org.apache.phoenix.schema.types.PVarbinary;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.trace.util.Tracing;
import org.apache.phoenix.transaction.TransactionFactory;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.ClientUtil;
import org.apache.phoenix.util.EncodedColumnsUtil;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.PhoenixKeyValueUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.ServerUtil;
import org.apache.phoenix.util.ServerViewUtil;
import org.apache.phoenix.util.UpgradeUtil;
import org.apache.phoenix.util.ViewUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.thirdparty.com.google.common.cache.Cache;
import org.apache.phoenix.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;

/**
 * Endpoint co-processor through which all Phoenix metadata mutations flow.
 * Phoenix metadata is stored in SYSTEM.CATALOG. The table specific information
 * is stored in a single header row. Column information is stored in a separate
 * row per column. Linking information (indexes, views etc) are stored using a
 * separate row for each link that uses the {@link LinkType} column value. The
 * {@code parent->child } links are stored in a separate SYSTEM.CHILD_LINK table.
 * Metadata for all tables/views/indexes in the same schema are stored in a
 * single region which is enforced using the {@link MetaDataSplitPolicy}.
 * <p>
 * While creating child views we only store columns added by the view. When
 * resolving a view we resolve all its parents and add their columns to the
 * PTable that is returned. We lock the parent table while creating an index to
 * ensure its metadata doesn't change.
 * While adding or dropping columns we lock the table or view to ensure that
 * concurrent conflicting changes are prevented. We also validate that there are
 * no existing conflicting child view columns when we add a column to a parent.
 * While dropping a column from a parent we check if there are any child views
 * that need the column and throw an exception. If there are view indexes that
 * required the column we drop them as well.
 * While dropping a table or view that has children using the cascade option, we
 * do not drop the child view metadata which will be removed at compaction time.
 * If we recreate a table or view that was dropped whose child metadata hasn't
 * been removed yet, we delete the child view metadata. When resolving a view,
 * we resolve all its parents, if any of them are dropped the child view is
 * considered to be dropped and we throw a TableNotFoundException.
 * <p>
 * We only allow mutations to the latest version of a Phoenix table (i.e. the
 * timeStamp must be increasing). For adding/dropping columns we use a sequence
 * number on the table to ensure that the client has the latest version.
 *
 * @since 0.1
 */
@SuppressWarnings("deprecation")
@CoreCoprocessor
public class MetaDataEndpointImpl extends MetaDataProtocol implements RegionCoprocessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(MetaDataEndpointImpl.class);

    private static final byte[] CHILD_TABLE_BYTES = new byte[]{PTable.LinkType.CHILD_TABLE.getSerializedValue()};
    private static final byte[] PHYSICAL_TABLE_BYTES =
            new byte[]{PTable.LinkType.PHYSICAL_TABLE.getSerializedValue()};

    private LockManager lockManager;
    private long metadataCacheRowLockTimeout;

    // KeyValues for Table
    private static final Cell TABLE_TYPE_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY,
        TABLE_FAMILY_BYTES, TABLE_TYPE_BYTES);
    private static final Cell TABLE_SEQ_NUM_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY,
TABLE_FAMILY_BYTES, TABLE_SEQ_NUM_BYTES);
    private static final Cell COLUMN_COUNT_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, COLUMN_COUNT_BYTES);
    private static final Cell SALT_BUCKETS_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, SALT_BUCKETS_BYTES);
    private static final Cell PK_NAME_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, PK_NAME_BYTES);
    private static final Cell DATA_TABLE_NAME_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, DATA_TABLE_NAME_BYTES);
    private static final Cell INDEX_STATE_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, INDEX_STATE_BYTES);
    private static final Cell IMMUTABLE_ROWS_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, IMMUTABLE_ROWS_BYTES);
    private static final Cell VIEW_EXPRESSION_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, VIEW_STATEMENT_BYTES);
    private static final Cell DEFAULT_COLUMN_FAMILY_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, DEFAULT_COLUMN_FAMILY_NAME_BYTES);
    private static final Cell DISABLE_WAL_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, DISABLE_WAL_BYTES);
    private static final Cell MULTI_TENANT_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, MULTI_TENANT_BYTES);
    private static final Cell VIEW_TYPE_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, VIEW_TYPE_BYTES);
    private static final Cell VIEW_INDEX_ID_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, VIEW_INDEX_ID_BYTES);
    /**
     * A designator for choosing the right type for viewIndex (Short vs Long) to be backward compatible.
     **/
    private static final Cell VIEW_INDEX_ID_DATA_TYPE_BYTES_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, VIEW_INDEX_ID_DATA_TYPE_BYTES);
    private static final Cell INDEX_TYPE_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, INDEX_TYPE_BYTES);
    private static final Cell INDEX_DISABLE_TIMESTAMP_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, INDEX_DISABLE_TIMESTAMP_BYTES);
    private static final Cell STORE_NULLS_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, STORE_NULLS_BYTES);
    private static final Cell EMPTY_KEYVALUE_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, QueryConstants.EMPTY_COLUMN_BYTES);
    private static final Cell BASE_COLUMN_COUNT_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, PhoenixDatabaseMetaData.BASE_COLUMN_COUNT_BYTES);
    private static final Cell ROW_KEY_ORDER_OPTIMIZABLE_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, MetaDataEndpointImplConstants.ROW_KEY_ORDER_OPTIMIZABLE_BYTES);
    private static final Cell TRANSACTIONAL_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, TRANSACTIONAL_BYTES);
    private static final Cell TRANSACTION_PROVIDER_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, TRANSACTION_PROVIDER_BYTES);
    private static final Cell PHYSICAL_TABLE_NAME_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, PHYSICAL_TABLE_NAME_BYTES);
    private static final Cell UPDATE_CACHE_FREQUENCY_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, UPDATE_CACHE_FREQUENCY_BYTES);
    private static final Cell IS_NAMESPACE_MAPPED_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY,
            TABLE_FAMILY_BYTES, IS_NAMESPACE_MAPPED_BYTES);
    private static final Cell AUTO_PARTITION_SEQ_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, AUTO_PARTITION_SEQ_BYTES);
    private static final Cell APPEND_ONLY_SCHEMA_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, APPEND_ONLY_SCHEMA_BYTES);
    private static final Cell STORAGE_SCHEME_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, STORAGE_SCHEME_BYTES);
    private static final Cell ENCODING_SCHEME_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, ENCODING_SCHEME_BYTES);
    private static final Cell USE_STATS_FOR_PARALLELIZATION_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, USE_STATS_FOR_PARALLELIZATION_BYTES);
    private static final Cell PHOENIX_TTL_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, PHOENIX_TTL_BYTES);
    private static final Cell PHOENIX_TTL_HWM_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, PHOENIX_TTL_HWM_BYTES);
    private static final Cell LAST_DDL_TIMESTAMP_KV =
        createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, LAST_DDL_TIMESTAMP_BYTES);
    private static final Cell CHANGE_DETECTION_ENABLED_KV =
        createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES,
            CHANGE_DETECTION_ENABLED_BYTES);
    private static final Cell SCHEMA_VERSION_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY,
            TABLE_FAMILY_BYTES, SCHEMA_VERSION_BYTES);
    private static final Cell EXTERNAL_SCHEMA_ID_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY,
        TABLE_FAMILY_BYTES, EXTERNAL_SCHEMA_ID_BYTES);
    private static final Cell STREAMING_TOPIC_NAME_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY,
        TABLE_FAMILY_BYTES, STREAMING_TOPIC_NAME_BYTES);
    private static final Cell INDEX_WHERE_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY,
            TABLE_FAMILY_BYTES, INDEX_WHERE_BYTES);

    private static final List<Cell> TABLE_KV_COLUMNS = Lists.newArrayList(
            EMPTY_KEYVALUE_KV,
            TABLE_TYPE_KV,
            TABLE_SEQ_NUM_KV,
            COLUMN_COUNT_KV,
            SALT_BUCKETS_KV,
            PK_NAME_KV,
            DATA_TABLE_NAME_KV,
            INDEX_STATE_KV,
            IMMUTABLE_ROWS_KV,
            VIEW_EXPRESSION_KV,
            DEFAULT_COLUMN_FAMILY_KV,
            DISABLE_WAL_KV,
            MULTI_TENANT_KV,
            VIEW_TYPE_KV,
            VIEW_INDEX_ID_KV,
            VIEW_INDEX_ID_DATA_TYPE_BYTES_KV,
            INDEX_TYPE_KV,
            INDEX_DISABLE_TIMESTAMP_KV,
            STORE_NULLS_KV,
            BASE_COLUMN_COUNT_KV,
            ROW_KEY_ORDER_OPTIMIZABLE_KV,
            TRANSACTIONAL_KV,
            TRANSACTION_PROVIDER_KV,
            PHYSICAL_TABLE_NAME_KV,
            UPDATE_CACHE_FREQUENCY_KV,
            IS_NAMESPACE_MAPPED_KV,
            AUTO_PARTITION_SEQ_KV,
            APPEND_ONLY_SCHEMA_KV,
            STORAGE_SCHEME_KV,
            ENCODING_SCHEME_KV,
            USE_STATS_FOR_PARALLELIZATION_KV,
            PHOENIX_TTL_KV,
            PHOENIX_TTL_HWM_KV,
            LAST_DDL_TIMESTAMP_KV,
            CHANGE_DETECTION_ENABLED_KV,
            SCHEMA_VERSION_KV,
            EXTERNAL_SCHEMA_ID_KV,
            STREAMING_TOPIC_NAME_KV,
            INDEX_WHERE_KV
    );

    static {
        Collections.sort(TABLE_KV_COLUMNS, CellComparatorImpl.COMPARATOR);
    }

    private static final int TABLE_TYPE_INDEX = TABLE_KV_COLUMNS.indexOf(TABLE_TYPE_KV);
    private static final int TABLE_SEQ_NUM_INDEX = TABLE_KV_COLUMNS.indexOf(TABLE_SEQ_NUM_KV);
    private static final int COLUMN_COUNT_INDEX = TABLE_KV_COLUMNS.indexOf(COLUMN_COUNT_KV);
    private static final int SALT_BUCKETS_INDEX = TABLE_KV_COLUMNS.indexOf(SALT_BUCKETS_KV);
    private static final int PK_NAME_INDEX = TABLE_KV_COLUMNS.indexOf(PK_NAME_KV);
    private static final int DATA_TABLE_NAME_INDEX = TABLE_KV_COLUMNS.indexOf(DATA_TABLE_NAME_KV);
    private static final int INDEX_STATE_INDEX = TABLE_KV_COLUMNS.indexOf(INDEX_STATE_KV);
    private static final int IMMUTABLE_ROWS_INDEX = TABLE_KV_COLUMNS.indexOf(IMMUTABLE_ROWS_KV);
    private static final int VIEW_STATEMENT_INDEX = TABLE_KV_COLUMNS.indexOf(VIEW_EXPRESSION_KV);
    private static final int DEFAULT_COLUMN_FAMILY_INDEX = TABLE_KV_COLUMNS.indexOf(DEFAULT_COLUMN_FAMILY_KV);
    private static final int DISABLE_WAL_INDEX = TABLE_KV_COLUMNS.indexOf(DISABLE_WAL_KV);
    private static final int MULTI_TENANT_INDEX = TABLE_KV_COLUMNS.indexOf(MULTI_TENANT_KV);
    private static final int VIEW_TYPE_INDEX = TABLE_KV_COLUMNS.indexOf(VIEW_TYPE_KV);
    private static final int VIEW_INDEX_ID_DATA_TYPE_INDEX = TABLE_KV_COLUMNS.indexOf(VIEW_INDEX_ID_DATA_TYPE_BYTES_KV);
    private static final int VIEW_INDEX_ID_INDEX = TABLE_KV_COLUMNS.indexOf(VIEW_INDEX_ID_KV);
    private static final int INDEX_TYPE_INDEX = TABLE_KV_COLUMNS.indexOf(INDEX_TYPE_KV);
    private static final int STORE_NULLS_INDEX = TABLE_KV_COLUMNS.indexOf(STORE_NULLS_KV);
    private static final int BASE_COLUMN_COUNT_INDEX = TABLE_KV_COLUMNS.indexOf(BASE_COLUMN_COUNT_KV);
    private static final int ROW_KEY_ORDER_OPTIMIZABLE_INDEX = TABLE_KV_COLUMNS.indexOf(ROW_KEY_ORDER_OPTIMIZABLE_KV);
    private static final int TRANSACTIONAL_INDEX = TABLE_KV_COLUMNS.indexOf(TRANSACTIONAL_KV);
    private static final int TRANSACTION_PROVIDER_INDEX = TABLE_KV_COLUMNS.indexOf(TRANSACTION_PROVIDER_KV);
    private static final int UPDATE_CACHE_FREQUENCY_INDEX = TABLE_KV_COLUMNS.indexOf(UPDATE_CACHE_FREQUENCY_KV);
    private static final int INDEX_DISABLE_TIMESTAMP = TABLE_KV_COLUMNS.indexOf(INDEX_DISABLE_TIMESTAMP_KV);
    private static final int IS_NAMESPACE_MAPPED_INDEX = TABLE_KV_COLUMNS.indexOf(IS_NAMESPACE_MAPPED_KV);
    private static final int AUTO_PARTITION_SEQ_INDEX = TABLE_KV_COLUMNS.indexOf(AUTO_PARTITION_SEQ_KV);
    private static final int APPEND_ONLY_SCHEMA_INDEX = TABLE_KV_COLUMNS.indexOf(APPEND_ONLY_SCHEMA_KV);
    private static final int STORAGE_SCHEME_INDEX = TABLE_KV_COLUMNS.indexOf(STORAGE_SCHEME_KV);
    private static final int QUALIFIER_ENCODING_SCHEME_INDEX = TABLE_KV_COLUMNS.indexOf(ENCODING_SCHEME_KV);
    private static final int USE_STATS_FOR_PARALLELIZATION_INDEX = TABLE_KV_COLUMNS.indexOf(USE_STATS_FOR_PARALLELIZATION_KV);
    private static final int PHYSICAL_TABLE_NAME_INDEX = TABLE_KV_COLUMNS.indexOf(PHYSICAL_TABLE_NAME_KV);
    private static final int PHOENIX_TTL_INDEX = TABLE_KV_COLUMNS.indexOf(PHOENIX_TTL_KV);
    private static final int PHOENIX_TTL_HWM_INDEX = TABLE_KV_COLUMNS.indexOf(PHOENIX_TTL_HWM_KV);
    private static final int LAST_DDL_TIMESTAMP_INDEX =
        TABLE_KV_COLUMNS.indexOf(LAST_DDL_TIMESTAMP_KV);
    private static final int CHANGE_DETECTION_ENABLED_INDEX =
        TABLE_KV_COLUMNS.indexOf(CHANGE_DETECTION_ENABLED_KV);
    private static final int SCHEMA_VERSION_INDEX = TABLE_KV_COLUMNS.indexOf(SCHEMA_VERSION_KV);
    private static final int EXTERNAL_SCHEMA_ID_INDEX =
        TABLE_KV_COLUMNS.indexOf(EXTERNAL_SCHEMA_ID_KV);
    private static final int STREAMING_TOPIC_NAME_INDEX =
        TABLE_KV_COLUMNS.indexOf(STREAMING_TOPIC_NAME_KV);
    private static final int INDEX_WHERE_INDEX =
            TABLE_KV_COLUMNS.indexOf(INDEX_WHERE_KV);
    // KeyValues for Column
    private static final KeyValue DECIMAL_DIGITS_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, DECIMAL_DIGITS_BYTES);
    private static final KeyValue COLUMN_SIZE_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, COLUMN_SIZE_BYTES);
    private static final KeyValue NULLABLE_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, NULLABLE_BYTES);
    private static final KeyValue DATA_TYPE_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, DATA_TYPE_BYTES);
    private static final KeyValue ORDINAL_POSITION_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, ORDINAL_POSITION_BYTES);
    private static final KeyValue SORT_ORDER_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, SORT_ORDER_BYTES);
    private static final KeyValue ARRAY_SIZE_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, ARRAY_SIZE_BYTES);
    private static final KeyValue VIEW_CONSTANT_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, VIEW_CONSTANT_BYTES);
    private static final KeyValue IS_VIEW_REFERENCED_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, IS_VIEW_REFERENCED_BYTES);
    private static final KeyValue COLUMN_DEF_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, COLUMN_DEF_BYTES);
    private static final KeyValue IS_ROW_TIMESTAMP_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, IS_ROW_TIMESTAMP_BYTES);
    private static final KeyValue COLUMN_QUALIFIER_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, COLUMN_QUALIFIER_BYTES);
    // this key value is used to represent a column derived from a parent that was deleted (by
    // storing a value of LinkType.EXCLUDED_COLUMN)
    private static final KeyValue LINK_TYPE_KV =
            createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, LINK_TYPE_BYTES);

    private static final List<Cell> COLUMN_KV_COLUMNS = Lists.newArrayList(
            DECIMAL_DIGITS_KV,
            COLUMN_SIZE_KV,
            NULLABLE_KV,
            DATA_TYPE_KV,
            ORDINAL_POSITION_KV,
            SORT_ORDER_KV,
            DATA_TABLE_NAME_KV, // included in both column and table row for metadata APIs
            ARRAY_SIZE_KV,
            VIEW_CONSTANT_KV,
            IS_VIEW_REFERENCED_KV,
            COLUMN_DEF_KV,
            IS_ROW_TIMESTAMP_KV,
            COLUMN_QUALIFIER_KV,
            LINK_TYPE_KV
    );

    static {
        COLUMN_KV_COLUMNS.sort(CellComparator.getInstance());
    }
    private static final Cell QUALIFIER_COUNTER_KV =
     KeyValueUtil.createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES,
        COLUMN_QUALIFIER_COUNTER_BYTES);
    private static final int DECIMAL_DIGITS_INDEX = COLUMN_KV_COLUMNS.indexOf(DECIMAL_DIGITS_KV);
    private static final int COLUMN_SIZE_INDEX = COLUMN_KV_COLUMNS.indexOf(COLUMN_SIZE_KV);
    private static final int NULLABLE_INDEX = COLUMN_KV_COLUMNS.indexOf(NULLABLE_KV);
    private static final int DATA_TYPE_INDEX = COLUMN_KV_COLUMNS.indexOf(DATA_TYPE_KV);
    private static final int ORDINAL_POSITION_INDEX = COLUMN_KV_COLUMNS.indexOf(ORDINAL_POSITION_KV);
    private static final int SORT_ORDER_INDEX = COLUMN_KV_COLUMNS.indexOf(SORT_ORDER_KV);
    private static final int ARRAY_SIZE_INDEX = COLUMN_KV_COLUMNS.indexOf(ARRAY_SIZE_KV);
    private static final int VIEW_CONSTANT_INDEX = COLUMN_KV_COLUMNS.indexOf(VIEW_CONSTANT_KV);
    private static final int IS_VIEW_REFERENCED_INDEX = COLUMN_KV_COLUMNS.indexOf(IS_VIEW_REFERENCED_KV);
    private static final int COLUMN_DEF_INDEX = COLUMN_KV_COLUMNS.indexOf(COLUMN_DEF_KV);
    private static final int IS_ROW_TIMESTAMP_INDEX = COLUMN_KV_COLUMNS.indexOf(IS_ROW_TIMESTAMP_KV);
    private static final int COLUMN_QUALIFIER_INDEX = COLUMN_KV_COLUMNS.indexOf(COLUMN_QUALIFIER_KV);
    // the index of the key value is used to represent a column derived from a parent that was
    // deleted (by storing a value of LinkType.EXCLUDED_COLUMN)
    private static final int EXCLUDED_COLUMN_LINK_TYPE_KV_INDEX =
            COLUMN_KV_COLUMNS.indexOf(LINK_TYPE_KV);

    // index for link type key value that is used to store linking rows
    private static final int LINK_TYPE_INDEX = 0;
    private static final Cell CLASS_NAME_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, CLASS_NAME_BYTES);
    private static final Cell JAR_PATH_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, JAR_PATH_BYTES);
    private static final Cell RETURN_TYPE_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, RETURN_TYPE_BYTES);
    private static final Cell NUM_ARGS_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, NUM_ARGS_BYTES);
    private static final Cell TYPE_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, TYPE_BYTES);
    private static final Cell IS_CONSTANT_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, IS_CONSTANT_BYTES);
    private static final Cell DEFAULT_VALUE_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, DEFAULT_VALUE_BYTES);
    private static final Cell MIN_VALUE_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, MIN_VALUE_BYTES);
    private static final Cell MAX_VALUE_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, MAX_VALUE_BYTES);
    private static final Cell IS_ARRAY_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, IS_ARRAY_BYTES);

    private static final List<Cell> FUNCTION_KV_COLUMNS = Arrays.<Cell>asList(
        EMPTY_KEYVALUE_KV,
        CLASS_NAME_KV,
        JAR_PATH_KV,
        RETURN_TYPE_KV,
        NUM_ARGS_KV
        );
    static {
        Collections.sort(FUNCTION_KV_COLUMNS, CellComparatorImpl.COMPARATOR);
    }

    private static final int CLASS_NAME_INDEX = FUNCTION_KV_COLUMNS.indexOf(CLASS_NAME_KV);
    private static final int JAR_PATH_INDEX = FUNCTION_KV_COLUMNS.indexOf(JAR_PATH_KV);
    private static final int RETURN_TYPE_INDEX = FUNCTION_KV_COLUMNS.indexOf(RETURN_TYPE_KV);
    private static final int NUM_ARGS_INDEX = FUNCTION_KV_COLUMNS.indexOf(NUM_ARGS_KV);

    private static final List<Cell> FUNCTION_ARG_KV_COLUMNS = Arrays.<Cell>asList(
        TYPE_KV,
        IS_ARRAY_KV,
        IS_CONSTANT_KV,
        DEFAULT_VALUE_KV,
        MIN_VALUE_KV,
        MAX_VALUE_KV
        );
    static {
        Collections.sort(FUNCTION_ARG_KV_COLUMNS, CellComparatorImpl.COMPARATOR);
    }

    private static final int IS_ARRAY_INDEX = FUNCTION_ARG_KV_COLUMNS.indexOf(IS_ARRAY_KV);
    private static final int IS_CONSTANT_INDEX = FUNCTION_ARG_KV_COLUMNS.indexOf(IS_CONSTANT_KV);
    private static final int DEFAULT_VALUE_INDEX = FUNCTION_ARG_KV_COLUMNS.indexOf(DEFAULT_VALUE_KV);
    private static final int MIN_VALUE_INDEX = FUNCTION_ARG_KV_COLUMNS.indexOf(MIN_VALUE_KV);
    private static final int MAX_VALUE_INDEX = FUNCTION_ARG_KV_COLUMNS.indexOf(MAX_VALUE_KV);

    public static PName newPName(byte[] buffer) {
        return buffer == null ? null : newPName(buffer, 0, buffer.length);
    }

    public static PName newPName(byte[] keyBuffer, int keyOffset, int keyLength) {
        if (keyLength <= 0) {
            return null;
        }
        int length = getVarCharLength(keyBuffer, keyOffset, keyLength);
        return PNameFactory.newName(keyBuffer, keyOffset, length);
    }

    private static boolean failConcurrentMutateAddColumnOneTimeForTesting = false;
    private RegionCoprocessorEnvironment env;

    private PhoenixMetaDataCoprocessorHost phoenixAccessCoprocessorHost;
    private boolean accessCheckEnabled;
    private boolean blockWriteRebuildIndex;
    private int maxIndexesPerTable;
    private boolean isTablesMappingEnabled;

    // this flag denotes that we will continue to write parent table column metadata while creating
    // a child view and also block metadata changes that were previously propagated to children
    // before 4.15, so that we can rollback the upgrade to 4.15 if required
    private boolean allowSplittableSystemCatalogRollback;

    private boolean isSystemCatalogSplittable;

    protected boolean getMetadataReadLockEnabled;

    private MetricsMetadataSource metricsSource;

    public static void setFailConcurrentMutateAddColumnOneTimeForTesting(boolean fail) {
        failConcurrentMutateAddColumnOneTimeForTesting = fail;
    }

    /**
     * Stores a reference to the coprocessor environment provided by the
     * {@link org.apache.hadoop.hbase.regionserver.RegionCoprocessorHost} from the region where this
     * coprocessor is loaded. Since this is a coprocessor endpoint, it always expects to be loaded
     * on a table region, so always expects this to be an instance of
     * {@link RegionCoprocessorEnvironment}.
     *
     * @param env the environment provided by the coprocessor host
     * @throws IOException if the provided environment is not an instance of
     *                     {@code RegionCoprocessorEnvironment}
     */
    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
        if (env instanceof RegionCoprocessorEnvironment) {
            this.env = (RegionCoprocessorEnvironment) env;
        } else {
            throw new CoprocessorException("Must be loaded on a table region!");
        }

        this.lockManager = new LockManager();
        phoenixAccessCoprocessorHost = new PhoenixMetaDataCoprocessorHost(this.env);
        Configuration config = env.getConfiguration();
        this.metadataCacheRowLockTimeout =
            config.getLong(QueryServices.PHOENIX_METADATA_CACHE_UPDATE_ROWLOCK_TIMEOUT,
                QueryServices.DEFAULT_PHOENIX_METADATA_CACHE_UPDATE_ROWLOCK_TIMEOUT);
        this.accessCheckEnabled = config.getBoolean(QueryServices.PHOENIX_ACLS_ENABLED,
                QueryServicesOptions.DEFAULT_PHOENIX_ACLS_ENABLED);
        this.blockWriteRebuildIndex = config.getBoolean(QueryServices.INDEX_FAILURE_BLOCK_WRITE,
                QueryServicesOptions.DEFAULT_INDEX_FAILURE_BLOCK_WRITE);
        this.maxIndexesPerTable = config.getInt(QueryServices.MAX_INDEXES_PER_TABLE,
                QueryServicesOptions.DEFAULT_MAX_INDEXES_PER_TABLE);
        this.isTablesMappingEnabled = SchemaUtil.isNamespaceMappingEnabled(PTableType.TABLE,
                new ReadOnlyProps(config.iterator()));
        this.allowSplittableSystemCatalogRollback = config.getBoolean(QueryServices.ALLOW_SPLITTABLE_SYSTEM_CATALOG_ROLLBACK,
                QueryServicesOptions.DEFAULT_ALLOW_SPLITTABLE_SYSTEM_CATALOG_ROLLBACK);
        this.getMetadataReadLockEnabled
                = config.getBoolean(QueryServices.PHOENIX_GET_METADATA_READ_LOCK_ENABLED,
                            QueryServicesOptions.DEFAULT_PHOENIX_GET_METADATA_READ_LOCK_ENABLED);
        this.isSystemCatalogSplittable = MetaDataSplitPolicy.isSystemCatalogSplittable(config);

        LOGGER.info("Starting Tracing-Metrics Systems");
        // Start the phoenix trace collection
        Tracing.addTraceMetricsSource();
        Metrics.ensureConfigured();
        metricsSource = MetricsMetadataSourceFactory.getMetadataMetricsSource();
        GlobalCache.getInstance(this.env).setMetricsSource(metricsSource);
    }

    @Override
    public void stop(CoprocessorEnvironment env) throws IOException {
        SchemaRegistryRepositoryFactory.close();
    }

    @Override
    public Iterable<Service> getServices() {
        return Collections.singleton(this);
    }

    @Override
    public void getTable(RpcController controller, GetTableRequest request,
                         RpcCallback<MetaDataResponse> done) {
        MetaDataResponse.Builder builder = MetaDataResponse.newBuilder();
        byte[] tenantId = request.getTenantId().toByteArray();
        byte[] schemaName = request.getSchemaName().toByteArray();
        byte[] tableName = request.getTableName().toByteArray();
        byte[] key = SchemaUtil.getTableKey(tenantId, schemaName, tableName);
        long tableTimeStamp = request.getTableTimestamp();
        try {
            // TODO: check that key is within region.getStartKey() and region.getEndKey()
            // and return special code to force client to lookup region from meta.
            Region region = env.getRegion();
            MetaDataMutationResult result = checkTableKeyInRegion(key, region);
            if (result != null) {
                done.run(MetaDataMutationResult.toProto(result));
                return;
            }

            long currentTime = EnvironmentEdgeManager.currentTimeMillis();
            PTable table =
                    doGetTable(tenantId, schemaName, tableName, request.getClientTimestamp(),
                            null, request.getClientVersion());
            if (table == null) {
                builder.setReturnCode(MetaDataProtos.MutationCode.TABLE_NOT_FOUND);
                builder.setMutationTime(currentTime);
                done.run(builder.build());
                return;
            }
            getCoprocessorHost().preGetTable(Bytes.toString(tenantId), SchemaUtil.getTableName(schemaName, tableName),
                    TableName.valueOf(table.getPhysicalName().getBytes()));

            if (request.getClientVersion() < MIN_SPLITTABLE_SYSTEM_CATALOG
                    && table.getType() == PTableType.VIEW
                    && table.getViewType() != ViewType.MAPPED) {
                try (PhoenixConnection connection = QueryUtil.getConnectionOnServer(env.getConfiguration()).unwrap(PhoenixConnection.class)) {
                    PTable pTable = connection.getTableNoCache(table.getParentName().getString());
                    table = ViewUtil.addDerivedColumnsFromParent(connection, table, pTable);
                }
            }
            builder.setReturnCode(MetaDataProtos.MutationCode.TABLE_ALREADY_EXISTS);
            builder.setMutationTime(currentTime);
            if (blockWriteRebuildIndex) {
                long disableIndexTimestamp = table.getIndexDisableTimestamp();
                long minNonZerodisableIndexTimestamp = disableIndexTimestamp > 0 ? disableIndexTimestamp : Long.MAX_VALUE;
                for (PTable index : table.getIndexes()) {
                    disableIndexTimestamp = index.getIndexDisableTimestamp();
                    if (disableIndexTimestamp > 0
                            && (index.getIndexState() == PIndexState.ACTIVE
                            || index.getIndexState() == PIndexState.PENDING_ACTIVE
                            || index.getIndexState() == PIndexState.PENDING_DISABLE)
                            && disableIndexTimestamp < minNonZerodisableIndexTimestamp) {
                        minNonZerodisableIndexTimestamp = disableIndexTimestamp;
                    }
                }
                // Freeze time for table at min non-zero value of INDEX_DISABLE_TIMESTAMP
                // This will keep the table consistent with index as the table has had one more
                // batch applied to it.
                if (minNonZerodisableIndexTimestamp != Long.MAX_VALUE) {
                    // Subtract one because we add one due to timestamp granularity in Windows
                    builder.setMutationTime(minNonZerodisableIndexTimestamp - 1);
                }
            }
            // the PTable of views and indexes on views might get updated because a column is added to one of
            // their parents (this won't change the timestamp)
            if (table.getType() != PTableType.TABLE || table.getTimeStamp() != tableTimeStamp) {
                builder.setTable(PTableImpl.toProto(table));
            }
            done.run(builder.build());
        } catch (Throwable t) {
            LOGGER.error("getTable failed", t);
            ProtobufUtil.setControllerException(controller,
                    ClientUtil.createIOException(SchemaUtil.getTableName(schemaName, tableName), t));
        }
    }

    private PhoenixMetaDataCoprocessorHost getCoprocessorHost() {
        return phoenixAccessCoprocessorHost;
    }

    private PTable buildTable(byte[] key, ImmutableBytesPtr cacheKey, Region region,
                              long clientTimeStamp, int clientVersion)
            throws IOException, SQLException {
        Scan scan = MetaDataUtil.newTableRowsScan(key, MIN_TABLE_TIMESTAMP, clientTimeStamp);
        Cache<ImmutableBytesPtr, PMetaDataEntity> metaDataCache = GlobalCache.getInstance(this.env).getMetaDataCache();
        PTable newTable;
        region.startRegionOperation();
        try (RegionScanner scanner = region.getScanner(scan)) {
            PTable oldTable = (PTable) metaDataCache.getIfPresent(cacheKey);
            if (oldTable == null) {
                metricsSource.incrementMetadataCacheMissCount();
            } else {
                metricsSource.incrementMetadataCacheHitCount();
            }
            long tableTimeStamp = oldTable == null ? MIN_TABLE_TIMESTAMP - 1 : oldTable.getTimeStamp();
            newTable = getTable(scanner, clientTimeStamp, tableTimeStamp, clientVersion);
            if (newTable != null
                    && (oldTable == null || tableTimeStamp < newTable.getTimeStamp()
                    || (blockWriteRebuildIndex && newTable.getIndexDisableTimestamp() > 0))) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Caching table "
                            + Bytes.toStringBinary(cacheKey.get(), cacheKey.getOffset(),
                            cacheKey.getLength())
                            + " at seqNum " + newTable.getSequenceNumber()
                            + " with newer timestamp " + newTable.getTimeStamp() + " versus "
                            + tableTimeStamp);
                }
                metaDataCache.put(cacheKey, newTable);
                metricsSource.incrementMetadataCacheAddCount();
                metricsSource.incrementMetadataCacheUsedSize(newTable.getEstimatedSize());
            }
        } finally {
            region.closeRegionOperation();
        }
        return newTable;
    }

    private List<PFunction> buildFunctions(List<byte[]> keys, Region region,
                                           long clientTimeStamp, boolean isReplace, List<Mutation> deleteMutationsForReplace) throws IOException, SQLException {
        List<KeyRange> keyRanges = Lists.newArrayListWithExpectedSize(keys.size());
        for (byte[] key : keys) {
            byte[] stopKey = ByteUtil.concat(key, QueryConstants.SEPARATOR_BYTE_ARRAY);
            ByteUtil.nextKey(stopKey, stopKey.length);
            keyRanges
                    .add(PVarbinary.INSTANCE.getKeyRange(key, true, stopKey, false, SortOrder.ASC));
        }
        Scan scan = new Scan();
        scan.setTimeRange(MIN_TABLE_TIMESTAMP, clientTimeStamp);
        ScanRanges scanRanges = ScanRanges.createPointLookup(keyRanges);
        scanRanges.initializeScan(scan);
        scan.setFilter(scanRanges.getSkipScanFilter());
        Cache<ImmutableBytesPtr, PMetaDataEntity> metaDataCache = GlobalCache.getInstance(this.env).getMetaDataCache();
        List<PFunction> functions = new ArrayList<PFunction>();
        PFunction function = null;
        try (RegionScanner scanner = region.getScanner(scan)) {
            for (int i = 0; i < keys.size(); i++) {
                function = null;
                function =
                        getFunction(scanner, isReplace, clientTimeStamp, deleteMutationsForReplace);
                if (function == null) {
                    return null;
                }
                byte[] functionKey =
                        SchemaUtil.getFunctionKey(
                                function.getTenantId() == null ? ByteUtil.EMPTY_BYTE_ARRAY : function
                                        .getTenantId().getBytes(), Bytes.toBytes(function
                                        .getFunctionName()));
                metaDataCache.put(new FunctionBytesPtr(functionKey), function);
                metricsSource.incrementMetadataCacheAddCount();
                metricsSource.incrementMetadataCacheUsedSize(function.getEstimatedSize());
                functions.add(function);
            }
            return functions;
        }
    }

    private List<PSchema> buildSchemas(List<byte[]> keys, Region region, long clientTimeStamp,
                                       ImmutableBytesPtr cacheKey) throws IOException, SQLException {
        List<KeyRange> keyRanges = Lists.newArrayListWithExpectedSize(keys.size());
        for (byte[] key : keys) {
            byte[] stopKey = ByteUtil.concat(key, QueryConstants.SEPARATOR_BYTE_ARRAY);
            ByteUtil.nextKey(stopKey, stopKey.length);
            keyRanges
                    .add(PVarbinary.INSTANCE.getKeyRange(key, true, stopKey, false, SortOrder.ASC));
        }
        Scan scan = new Scan();
        if (clientTimeStamp != HConstants.LATEST_TIMESTAMP
                && clientTimeStamp != HConstants.OLDEST_TIMESTAMP) {
            scan.setTimeRange(MIN_TABLE_TIMESTAMP, clientTimeStamp + 1);
        } else {
            scan.setTimeRange(MIN_TABLE_TIMESTAMP, clientTimeStamp);
        }
        ScanRanges scanRanges = ScanRanges.createPointLookup(keyRanges);
        scanRanges.initializeScan(scan);
        scan.setFilter(scanRanges.getSkipScanFilter());
        Cache<ImmutableBytesPtr, PMetaDataEntity> metaDataCache = GlobalCache.getInstance(this.env).getMetaDataCache();
        List<PSchema> schemas = new ArrayList<PSchema>();
        PSchema schema = null;
        try (RegionScanner scanner = region.getScanner(scan)) {
            for (int i = 0; i < keys.size(); i++) {
                schema = null;
                schema = getSchema(scanner, clientTimeStamp);
                if (schema == null) {
                    return null;
                }
                metaDataCache.put(cacheKey, schema);
                metricsSource.incrementMetadataCacheAddCount();
                metricsSource.incrementMetadataCacheUsedSize(schema.getEstimatedSize());
                schemas.add(schema);
            }
            return schemas;
        }
    }

    private void addIndexToTable(PName tenantId, PName schemaName, PName indexName, PName tableName,
                                 long clientTimeStamp, List<PTable> indexes, int clientVersion)
            throws IOException, SQLException {
        byte[] tenantIdBytes = tenantId == null ? ByteUtil.EMPTY_BYTE_ARRAY : tenantId.getBytes();
        PTable indexTable = doGetTable(tenantIdBytes, schemaName.getBytes(), indexName.getBytes(), clientTimeStamp,
                null, clientVersion);
        if (indexTable == null) {
            ClientUtil.throwIOException("Index not found", new TableNotFoundException(schemaName.getString(), indexName.getString()));
            return;
        }
        indexes.add(indexTable);
    }

    private void addExcludedColumnToTable(List<PColumn> pColumns, PName colName, PName famName, long timestamp) {
        PColumnImpl pColumn = PColumnImpl.createExcludedColumn(famName, colName, timestamp);
        pColumns.add(pColumn);
    }

    private void addColumnToTable(List<Cell> results, PName colName, PName famName,
                                  Cell[] colKeyValues, List<PColumn> columns, boolean isSalted, int baseColumnCount,
                                  boolean isRegularView, long timestamp) {
        int i = 0;
        int j = 0;
        while (i < results.size() && j < COLUMN_KV_COLUMNS.size()) {
            Cell kv = results.get(i);
            Cell searchKv = COLUMN_KV_COLUMNS.get(j);
            int cmp =
                    Bytes.compareTo(kv.getQualifierArray(), kv.getQualifierOffset(),
                            kv.getQualifierLength(), searchKv.getQualifierArray(),
                            searchKv.getQualifierOffset(), searchKv.getQualifierLength());
            if (cmp == 0) {
                colKeyValues[j++] = kv;
                i++;
            } else if (cmp > 0) {
                colKeyValues[j++] = null;
            } else {
                i++; // shouldn't happen - means unexpected KV in system table column row
            }
        }

        if (colKeyValues[DATA_TYPE_INDEX] == null || colKeyValues[NULLABLE_INDEX] == null
                || colKeyValues[ORDINAL_POSITION_INDEX] == null) {
            throw new IllegalStateException("Didn't find all required key values in '"
                    + colName.getString() + "' column metadata row");
        }

        Cell columnSizeKv = colKeyValues[COLUMN_SIZE_INDEX];
        Integer maxLength =
                columnSizeKv == null ? null : PInteger.INSTANCE.getCodec().decodeInt(
                        columnSizeKv.getValueArray(), columnSizeKv.getValueOffset(), SortOrder.getDefault());
        Cell decimalDigitKv = colKeyValues[DECIMAL_DIGITS_INDEX];
        Integer scale =
                decimalDigitKv == null ? null : PInteger.INSTANCE.getCodec().decodeInt(
                        decimalDigitKv.getValueArray(), decimalDigitKv.getValueOffset(), SortOrder.getDefault());
        Cell ordinalPositionKv = colKeyValues[ORDINAL_POSITION_INDEX];
        int position =
                PInteger.INSTANCE.getCodec().decodeInt(ordinalPositionKv.getValueArray(),
                        ordinalPositionKv.getValueOffset(), SortOrder.getDefault()) + (isSalted ? 1 : 0);
        ;

        // if this column was inherited from a parent and was dropped then we create an excluded column
        // which will be used to exclude the parent column while combining columns from ancestors
        Cell excludedColumnKv = colKeyValues[EXCLUDED_COLUMN_LINK_TYPE_KV_INDEX];
        if (excludedColumnKv != null && colKeyValues[DATA_TYPE_INDEX]
                .getTimestamp() <= excludedColumnKv.getTimestamp()) {
            LinkType linkType =
                    LinkType.fromSerializedValue(
                            excludedColumnKv.getValueArray()[excludedColumnKv.getValueOffset()]);
            if (linkType == LinkType.EXCLUDED_COLUMN) {
                addExcludedColumnToTable(columns, colName, famName, excludedColumnKv.getTimestamp());
            } else {
                // if we have a column metadata row that has a link type keyvalue it should
                // represent an excluded column by containing the LinkType.EXCLUDED_COLUMN
                throw new IllegalStateException(
                        "Link type should be EXCLUDED_COLUMN but found an unxpected link type for key value "
                                + excludedColumnKv);
            }
            return;
        }

        Cell nullableKv = colKeyValues[NULLABLE_INDEX];
        boolean isNullable =
                PInteger.INSTANCE.getCodec().decodeInt(nullableKv.getValueArray(),
                        nullableKv.getValueOffset(), SortOrder.getDefault()) != ResultSetMetaData.columnNoNulls;
        Cell dataTypeKv = colKeyValues[DATA_TYPE_INDEX];
        PDataType dataType =
                PDataType.fromTypeId(PInteger.INSTANCE.getCodec().decodeInt(
                        dataTypeKv.getValueArray(), dataTypeKv.getValueOffset(), SortOrder.getDefault()));
        if (maxLength == null && dataType == PBinary.INSTANCE) {
            dataType = PVarbinary.INSTANCE;   // For
        }
        // backward
        // compatibility.
        Cell sortOrderKv = colKeyValues[SORT_ORDER_INDEX];
        SortOrder sortOrder =
                sortOrderKv == null ? SortOrder.getDefault() : SortOrder.fromSystemValue(PInteger.INSTANCE
                        .getCodec().decodeInt(sortOrderKv.getValueArray(),
                                sortOrderKv.getValueOffset(), SortOrder.getDefault()));

        Cell arraySizeKv = colKeyValues[ARRAY_SIZE_INDEX];
        Integer arraySize = arraySizeKv == null ? null :
                PInteger.INSTANCE.getCodec().decodeInt(arraySizeKv.getValueArray(), arraySizeKv.getValueOffset(), SortOrder.getDefault());

        Cell viewConstantKv = colKeyValues[VIEW_CONSTANT_INDEX];
        byte[] viewConstant =
                viewConstantKv == null ? null : new ImmutableBytesPtr(
                        viewConstantKv.getValueArray(), viewConstantKv.getValueOffset(),
                        viewConstantKv.getValueLength()).copyBytesIfNecessary();
        Cell isViewReferencedKv = colKeyValues[IS_VIEW_REFERENCED_INDEX];
        boolean isViewReferenced = isViewReferencedKv != null && Boolean.TRUE.equals(PBoolean.INSTANCE.toObject(isViewReferencedKv.getValueArray(), isViewReferencedKv.getValueOffset(), isViewReferencedKv.getValueLength()));
        Cell columnDefKv = colKeyValues[COLUMN_DEF_INDEX];
        String expressionStr = columnDefKv == null ? null : (String) PVarchar.INSTANCE.toObject(columnDefKv.getValueArray(), columnDefKv.getValueOffset(), columnDefKv.getValueLength());
        Cell isRowTimestampKV = colKeyValues[IS_ROW_TIMESTAMP_INDEX];
        boolean isRowTimestamp =
                isRowTimestampKV == null ? false : Boolean.TRUE.equals(PBoolean.INSTANCE.toObject(
                        isRowTimestampKV.getValueArray(), isRowTimestampKV.getValueOffset(),
                        isRowTimestampKV.getValueLength()));

        boolean isPkColumn = famName == null || famName.getString() == null;
        Cell columnQualifierKV = colKeyValues[COLUMN_QUALIFIER_INDEX];
        // Older tables won't have column qualifier metadata present. To make things simpler, just set the
        // column qualifier bytes by using the column name.
        byte[] columnQualifierBytes = columnQualifierKV != null ?
                Arrays.copyOfRange(columnQualifierKV.getValueArray(),
                        columnQualifierKV.getValueOffset(), columnQualifierKV.getValueOffset()
                                + columnQualifierKV.getValueLength()) : (isPkColumn ? null : colName.getBytes());
        PColumn column =
                new PColumnImpl(colName, famName, dataType, maxLength, scale, isNullable,
                        position - 1, sortOrder, arraySize, viewConstant, isViewReferenced,
                        expressionStr, isRowTimestamp, false, columnQualifierBytes,
                        timestamp);
        columns.add(column);
    }

    private void addArgumentToFunction(List<Cell> results, PName functionName, PName type,
                                       Cell[] functionKeyValues, List<FunctionArgument> arguments, short argPosition) throws SQLException {
        int i = 0;
        int j = 0;
        while (i < results.size() && j < FUNCTION_ARG_KV_COLUMNS.size()) {
            Cell kv = results.get(i);
            Cell searchKv = FUNCTION_ARG_KV_COLUMNS.get(j);
            int cmp =
                    Bytes.compareTo(kv.getQualifierArray(), kv.getQualifierOffset(),
                            kv.getQualifierLength(), searchKv.getQualifierArray(),
                            searchKv.getQualifierOffset(), searchKv.getQualifierLength());
            if (cmp == 0) {
                functionKeyValues[j++] = kv;
                i++;
            } else if (cmp > 0) {
                functionKeyValues[j++] = null;
            } else {
                i++; // shouldn't happen - means unexpected KV in system table column row
            }
        }

        Cell isArrayKv = functionKeyValues[IS_ARRAY_INDEX];
        boolean isArrayType =
                isArrayKv == null ? false : Boolean.TRUE.equals(PBoolean.INSTANCE.toObject(
                        isArrayKv.getValueArray(), isArrayKv.getValueOffset(),
                        isArrayKv.getValueLength()));
        Cell isConstantKv = functionKeyValues[IS_CONSTANT_INDEX];
        boolean isConstant =
                isConstantKv == null ? false : Boolean.TRUE.equals(PBoolean.INSTANCE.toObject(
                        isConstantKv.getValueArray(), isConstantKv.getValueOffset(),
                        isConstantKv.getValueLength()));
        Cell defaultValueKv = functionKeyValues[DEFAULT_VALUE_INDEX];
        String defaultValue =
                defaultValueKv == null ? null : (String) PVarchar.INSTANCE.toObject(
                        defaultValueKv.getValueArray(), defaultValueKv.getValueOffset(),
                        defaultValueKv.getValueLength());
        Cell minValueKv = functionKeyValues[MIN_VALUE_INDEX];
        String minValue =
                minValueKv == null ? null : (String) PVarchar.INSTANCE.toObject(
                        minValueKv.getValueArray(), minValueKv.getValueOffset(),
                        minValueKv.getValueLength());
        Cell maxValueKv = functionKeyValues[MAX_VALUE_INDEX];
        String maxValue =
                maxValueKv == null ? null : (String) PVarchar.INSTANCE.toObject(
                        maxValueKv.getValueArray(), maxValueKv.getValueOffset(),
                        maxValueKv.getValueLength());
        FunctionArgument arg =
                new FunctionArgument(type.getString(), isArrayType, isConstant,
                        defaultValue == null ? null : LiteralExpression.newConstant((new LiteralParseNode(defaultValue)).getValue()),
                        minValue == null ? null : LiteralExpression.newConstant((new LiteralParseNode(minValue)).getValue()),
                        maxValue == null ? null : LiteralExpression.newConstant((new LiteralParseNode(maxValue)).getValue()),
                        argPosition);
        arguments.add(arg);
    }

    private PName getPhysicalTableName(Region region, byte[] tenantId, byte[] schema, byte[] table, long timestamp) throws IOException {
        byte[] key = SchemaUtil.getTableKey(tenantId, schema, table);
        Scan scan = MetaDataUtil.newTableRowsScan(key, MetaDataProtocol.MIN_TABLE_TIMESTAMP,
                timestamp);
        scan.addColumn(TABLE_FAMILY_BYTES, PHYSICAL_TABLE_NAME_BYTES);
        try (RegionScanner scanner = region.getScanner(scan)) {
            List<Cell> results = Lists.newArrayList();
            scanner.next(results);
            Cell physicalTableNameKv = null;
            if (results.size() > 0) {
                physicalTableNameKv = results.get(0);
            }
            PName physicalTableName =
                    physicalTableNameKv != null ? newPName(physicalTableNameKv.getValueArray(),
                            physicalTableNameKv.getValueOffset(), physicalTableNameKv.getValueLength()) : null;
            return physicalTableName;
        }
    }

    private PTable getTable(RegionScanner scanner, long clientTimeStamp, long tableTimeStamp,
                            int clientVersion)
            throws IOException, SQLException {
        List<Cell> results = Lists.newArrayList();
        scanner.next(results);
        if (results.isEmpty()) {
            return null;
        }
        List<Cell> tableCellList = results;
        results = Lists.newArrayList();
        List<List<Cell>> allColumnCellList = Lists.newArrayList();

        do {
            if (results.size() > 0) {
                allColumnCellList.add(results);
                results = Lists.newArrayList();
            }
        } while (scanner.next(results));
        if (results != null && results.size() > 0) {
            allColumnCellList.add(results);
        }

        return getTableFromCells(tableCellList, allColumnCellList, clientTimeStamp, clientVersion);
    }

    private PTable getTableFromCells(List<Cell> tableCellList, List<List<Cell>> allColumnCellList,
                                     long clientTimeStamp, int clientVersion)
        throws IOException, SQLException {
        return getTableFromCells(tableCellList, allColumnCellList, clientTimeStamp, clientVersion, null);
    }

    /**
     * Utility method to get a PTable from the HBase Cells either read from SYSTEM.CATALOG or
     * generated by a DDL statement. Optionally, an existing PTable can be provided so that its
     * properties can be merged with the "new" PTable created from the Cell. This is useful when
     * generating an updated PTable following an ALTER DDL statement
     * @param tableCellList Cells from the header row containing table level properties
     * @param allColumnCellList Cells from column or link rows
     * @param clientTimeStamp client-provided timestamp
     * @param clientVersion client-provided version
     * @param oldTable Optional parameters containing properties for an existing PTable
     * @return
     * @throws IOException
     * @throws SQLException
     */
    private PTable getTableFromCells(List<Cell> tableCellList, List<List<Cell>> allColumnCellList,
                                     long clientTimeStamp, int clientVersion, PTable oldTable)
        throws IOException, SQLException {
        Cell[] tableKeyValues = new Cell[TABLE_KV_COLUMNS.size()];
        Cell[] colKeyValues = new Cell[COLUMN_KV_COLUMNS.size()];

        // Create PTable based on KeyValues from scan
        Cell keyValue = tableCellList.get(0);
        byte[] keyBuffer = keyValue.getRowArray();
        int keyLength = keyValue.getRowLength();
        int keyOffset = keyValue.getRowOffset();
        PName tenantId = newPName(keyBuffer, keyOffset, keyLength);
        int tenantIdLength = (tenantId == null) ? 0 : tenantId.getBytes().length;
        if (tenantIdLength == 0) {
            tenantId = null;
        }
        PName schemaName = newPName(keyBuffer, keyOffset + tenantIdLength + 1, keyLength);
        int schemaNameLength = schemaName.getBytes().length;
        int tableNameLength = keyLength - schemaNameLength - 1 - tenantIdLength - 1;
        byte[] tableNameBytes = new byte[tableNameLength];
        System.arraycopy(keyBuffer, keyOffset + schemaNameLength + 1 + tenantIdLength + 1,
                tableNameBytes, 0, tableNameLength);
        PName tableName = PNameFactory.newName(tableNameBytes);

        int offset = tenantIdLength + schemaNameLength + tableNameLength + 3;
        // This will prevent the client from continually looking for the current
        // table when we know that there will never be one since we disallow updates
        // unless the table is the latest

        long timeStamp = keyValue.getTimestamp();
        PTableImpl.Builder builder = null;
        if (oldTable != null) {
            builder = PTableImpl.builderFromExisting(oldTable);
            List<PColumn> columns = oldTable.getColumns();
            if (oldTable.getBucketNum() != null && oldTable.getBucketNum() > 0) {
                //if it's salted, skip the salt column -- it will get added back during
                //the build process
                columns = columns.stream().skip(1).collect(Collectors.toList());
            }
            builder.setColumns(columns);
        } else {
            builder = new PTableImpl.Builder();
        }
        builder.setTenantId(tenantId);
        builder.setSchemaName(schemaName);
        builder.setTableName(tableName);

        int i = 0;
        int j = 0;
        while (i < tableCellList.size() && j < TABLE_KV_COLUMNS.size()) {
            Cell kv = tableCellList.get(i);
            Cell searchKv = TABLE_KV_COLUMNS.get(j);
            int cmp =
                    Bytes.compareTo(kv.getQualifierArray(), kv.getQualifierOffset(),
                            kv.getQualifierLength(), searchKv.getQualifierArray(),
                            searchKv.getQualifierOffset(), searchKv.getQualifierLength());
            if (cmp == 0) {
                timeStamp = Math.max(timeStamp, kv.getTimestamp()); // Find max timestamp of table
                // header row
                tableKeyValues[j++] = kv;
                i++;
            } else if (cmp > 0) {
                timeStamp = Math.max(timeStamp, kv.getTimestamp());
                tableKeyValues[j++] = null;
            } else {
                i++; // shouldn't happen - means unexpected KV in system table header row
            }
        }
        // TABLE_TYPE, TABLE_SEQ_NUM and COLUMN_COUNT are required.
        if (tableKeyValues[TABLE_TYPE_INDEX] == null || tableKeyValues[TABLE_SEQ_NUM_INDEX] == null
                || tableKeyValues[COLUMN_COUNT_INDEX] == null) {
            // since we allow SYSTEM.CATALOG to split in certain cases there might be child links or
            // other metadata rows that are invalid and should be ignored
            Cell cell = tableCellList.get(0);
            LOGGER.error("Found invalid metadata rows for rowkey " +
                    Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength()));
            return null;
        }

        Cell tableTypeKv = tableKeyValues[TABLE_TYPE_INDEX];
        PTableType tableType =
                PTableType
                        .fromSerializedValue(tableTypeKv.getValueArray()[tableTypeKv.getValueOffset()]);
        builder.setType(tableType);

        Cell tableSeqNumKv = tableKeyValues[TABLE_SEQ_NUM_INDEX];
        long tableSeqNum =
                PLong.INSTANCE.getCodec().decodeLong(tableSeqNumKv.getValueArray(),
                        tableSeqNumKv.getValueOffset(), SortOrder.getDefault());
        builder.setSequenceNumber(tableSeqNum);

        Cell columnCountKv = tableKeyValues[COLUMN_COUNT_INDEX];
        int columnCount =
                PInteger.INSTANCE.getCodec().decodeInt(columnCountKv.getValueArray(),
                        columnCountKv.getValueOffset(), SortOrder.getDefault());

        Cell pkNameKv = tableKeyValues[PK_NAME_INDEX];
        PName pkName =
                pkNameKv != null ? newPName(pkNameKv.getValueArray(), pkNameKv.getValueOffset(),
                        pkNameKv.getValueLength()) : null;
        builder.setPkName(pkName != null ? pkName : oldTable != null ? oldTable.getPKName() : null);

        Cell saltBucketNumKv = tableKeyValues[SALT_BUCKETS_INDEX];
        Integer saltBucketNum =
                saltBucketNumKv != null ? (Integer) PInteger.INSTANCE.getCodec().decodeInt(
                        saltBucketNumKv.getValueArray(), saltBucketNumKv.getValueOffset(), SortOrder.getDefault()) : null;
        if (saltBucketNum != null && saltBucketNum.intValue() == 0) {
            saltBucketNum = null; // Zero salt buckets means not salted
        }
        builder.setBucketNum(saltBucketNum != null ? saltBucketNum : oldTable != null ? oldTable.getBucketNum() : null);

        //data table name is used to find the parent table for indexes later
        Cell dataTableNameKv = tableKeyValues[DATA_TABLE_NAME_INDEX];
        PName dataTableName =
                dataTableNameKv != null ? newPName(dataTableNameKv.getValueArray(),
                        dataTableNameKv.getValueOffset(), dataTableNameKv.getValueLength()) : null;

        Cell physicalTableNameKv = tableKeyValues[PHYSICAL_TABLE_NAME_INDEX];
        PName physicalTableName =
                physicalTableNameKv != null ? newPName(physicalTableNameKv.getValueArray(),
                        physicalTableNameKv.getValueOffset(), physicalTableNameKv.getValueLength()) : null;
        builder.setPhysicalTableName(physicalTableName != null ? physicalTableName : oldTable != null ? oldTable.getPhysicalName(true) : null);

        Cell indexStateKv = tableKeyValues[INDEX_STATE_INDEX];
        PIndexState indexState =
                indexStateKv == null ? null : PIndexState.fromSerializedValue(indexStateKv
                        .getValueArray()[indexStateKv.getValueOffset()]);
        builder.setState(indexState != null ? indexState : oldTable != null ? oldTable.getIndexState() : null);

        Cell immutableRowsKv = tableKeyValues[IMMUTABLE_ROWS_INDEX];
        boolean isImmutableRows = immutableRowsKv != null && (Boolean) PBoolean.INSTANCE.toObject(
            immutableRowsKv.getValueArray(), immutableRowsKv.getValueOffset(),
            immutableRowsKv.getValueLength());
        builder.setImmutableRows(immutableRowsKv != null ? isImmutableRows :
            oldTable != null && oldTable.isImmutableRows());

        Cell defaultFamilyNameKv = tableKeyValues[DEFAULT_COLUMN_FAMILY_INDEX];
        PName defaultFamilyName = defaultFamilyNameKv != null ? newPName(defaultFamilyNameKv.getValueArray(), defaultFamilyNameKv.getValueOffset(), defaultFamilyNameKv.getValueLength()) : null;
        builder.setDefaultFamilyName(defaultFamilyName != null ? defaultFamilyName : oldTable != null ? oldTable.getDefaultFamilyName() : null);

        Cell viewStatementKv = tableKeyValues[VIEW_STATEMENT_INDEX];
        String viewStatement = viewStatementKv != null ? (String) PVarchar.INSTANCE.toObject(viewStatementKv.getValueArray(), viewStatementKv.getValueOffset(),
                viewStatementKv.getValueLength()) : null;
        builder.setViewStatement(viewStatement != null ? viewStatement : oldTable != null ? oldTable.getViewStatement() : null);

        Cell disableWALKv = tableKeyValues[DISABLE_WAL_INDEX];
        boolean disableWAL = disableWALKv == null ? PTable.DEFAULT_DISABLE_WAL : Boolean.TRUE.equals(
                PBoolean.INSTANCE.toObject(disableWALKv.getValueArray(), disableWALKv.getValueOffset(), disableWALKv.getValueLength()));
        builder.setDisableWAL(disableWALKv != null ? disableWAL :
            oldTable != null && oldTable.isWALDisabled());

        Cell multiTenantKv = tableKeyValues[MULTI_TENANT_INDEX];
        boolean multiTenant = multiTenantKv != null && Boolean.TRUE.equals(
            PBoolean.INSTANCE.toObject(multiTenantKv.getValueArray(),
                multiTenantKv.getValueOffset(), multiTenantKv.getValueLength()));
        builder.setMultiTenant(multiTenantKv != null ? multiTenant :
            oldTable != null && oldTable.isMultiTenant());

        Cell storeNullsKv = tableKeyValues[STORE_NULLS_INDEX];
        boolean storeNulls = storeNullsKv != null && Boolean.TRUE.equals(
            PBoolean.INSTANCE.toObject(storeNullsKv.getValueArray(), storeNullsKv.getValueOffset(),
                storeNullsKv.getValueLength()));
        builder.setStoreNulls(storeNullsKv != null ? storeNulls :
            oldTable != null && oldTable.getStoreNulls());

        Cell transactionalKv = tableKeyValues[TRANSACTIONAL_INDEX];
        Cell transactionProviderKv = tableKeyValues[TRANSACTION_PROVIDER_INDEX];
        TransactionFactory.Provider transactionProvider = null;
        if (transactionProviderKv == null) {
            if (transactionalKv != null && Boolean.TRUE.equals(
                    PBoolean.INSTANCE.toObject(
                            transactionalKv.getValueArray(),
                            transactionalKv.getValueOffset(),
                            transactionalKv.getValueLength()))) {
                // For backward compat, prior to client setting TRANSACTION_PROVIDER
                transactionProvider = TransactionFactory.Provider.NOTAVAILABLE;
            }
        } else {
            transactionProvider = TransactionFactory.Provider.fromCode(
                    PTinyint.INSTANCE.getCodec().decodeByte(
                            transactionProviderKv.getValueArray(),
                            transactionProviderKv.getValueOffset(),
                            SortOrder.getDefault()));
        }
        builder.setTransactionProvider(transactionProviderKv != null || transactionalKv != null
            ? transactionProvider : oldTable != null ? oldTable.getTransactionProvider() : null);

        Cell viewTypeKv = tableKeyValues[VIEW_TYPE_INDEX];
        ViewType viewType = viewTypeKv == null ? null : ViewType.fromSerializedValue(viewTypeKv.getValueArray()[viewTypeKv.getValueOffset()]);
        builder.setViewType(viewType != null ? viewType : oldTable != null ? oldTable.getViewType() : null);

        PDataType viewIndexIdType = oldTable != null ? oldTable.getviewIndexIdType() :
            getViewIndexIdType(tableKeyValues);
        builder.setViewIndexIdType(viewIndexIdType);

        Long viewIndexId = getViewIndexId(tableKeyValues, viewIndexIdType);
        builder.setViewIndexId(viewIndexId != null ? viewIndexId : oldTable != null ? oldTable.getViewIndexId() : null);

        Cell indexTypeKv = tableKeyValues[INDEX_TYPE_INDEX];
        IndexType indexType = indexTypeKv == null ? null : IndexType.fromSerializedValue(indexTypeKv.getValueArray()[indexTypeKv.getValueOffset()]);
        builder.setIndexType(indexType != null ? indexType : oldTable != null ? oldTable.getIndexType() : null);

        Cell baseColumnCountKv = tableKeyValues[BASE_COLUMN_COUNT_INDEX];
        int baseColumnCount = baseColumnCountKv == null ? 0 : PInteger.INSTANCE.getCodec().decodeInt(baseColumnCountKv.getValueArray(),
                baseColumnCountKv.getValueOffset(), SortOrder.getDefault());
        builder.setBaseColumnCount(baseColumnCountKv != null ? baseColumnCount : oldTable != null ? oldTable.getBaseColumnCount() : 0);

        Cell rowKeyOrderOptimizableKv = tableKeyValues[ROW_KEY_ORDER_OPTIMIZABLE_INDEX];
        boolean rowKeyOrderOptimizable = rowKeyOrderOptimizableKv != null && Boolean.TRUE.equals(
            PBoolean.INSTANCE.toObject(rowKeyOrderOptimizableKv.getValueArray(),
                rowKeyOrderOptimizableKv.getValueOffset(),
                rowKeyOrderOptimizableKv.getValueLength()));
        builder.setRowKeyOrderOptimizable(rowKeyOrderOptimizableKv != null ? rowKeyOrderOptimizable :
            oldTable != null && oldTable.rowKeyOrderOptimizable());

        Cell updateCacheFrequencyKv = tableKeyValues[UPDATE_CACHE_FREQUENCY_INDEX];
        long updateCacheFrequency = updateCacheFrequencyKv == null ? 0 :
                PLong.INSTANCE.getCodec().decodeLong(updateCacheFrequencyKv.getValueArray(),
                        updateCacheFrequencyKv.getValueOffset(), SortOrder.getDefault());
        builder.setUpdateCacheFrequency(updateCacheFrequencyKv != null ? updateCacheFrequency : oldTable != null ? oldTable.getUpdateCacheFrequency() : 0);

        // Check the cell tag to see whether the view has modified this property
        final byte[] tagUpdateCacheFreq = (updateCacheFrequencyKv == null) ?
         HConstants.EMPTY_BYTE_ARRAY :
                TagUtil.concatTags(HConstants.EMPTY_BYTE_ARRAY, updateCacheFrequencyKv);
        boolean viewModifiedUpdateCacheFrequency = (PTableType.VIEW.equals(tableType)) &&
                Bytes.contains(tagUpdateCacheFreq, MetaDataEndpointImplConstants.VIEW_MODIFIED_PROPERTY_BYTES);
        builder.setViewModifiedUpdateCacheFrequency(!Bytes.equals(tagUpdateCacheFreq,
            HConstants.EMPTY_BYTE_ARRAY) ? viewModifiedUpdateCacheFrequency :
            oldTable != null && oldTable.hasViewModifiedUpdateCacheFrequency());

        Cell indexDisableTimestampKv = tableKeyValues[INDEX_DISABLE_TIMESTAMP];
        long indexDisableTimestamp = indexDisableTimestampKv == null ? 0L : PLong.INSTANCE.getCodec().decodeLong(indexDisableTimestampKv.getValueArray(),
                indexDisableTimestampKv.getValueOffset(), SortOrder.getDefault());
        builder.setIndexDisableTimestamp(indexDisableTimestampKv != null ?
            indexDisableTimestamp : oldTable != null ? oldTable.getIndexDisableTimestamp() : 0L);

        Cell isNamespaceMappedKv = tableKeyValues[IS_NAMESPACE_MAPPED_INDEX];
        boolean isNamespaceMapped = isNamespaceMappedKv != null && Boolean.TRUE.equals(
            PBoolean.INSTANCE.toObject(isNamespaceMappedKv.getValueArray(),
                isNamespaceMappedKv.getValueOffset(), isNamespaceMappedKv.getValueLength()));
        builder.setNamespaceMapped(isNamespaceMappedKv != null ? isNamespaceMapped :
            oldTable != null && oldTable.isNamespaceMapped());

        Cell autoPartitionSeqKv = tableKeyValues[AUTO_PARTITION_SEQ_INDEX];
        String autoPartitionSeq = autoPartitionSeqKv != null ? (String) PVarchar.INSTANCE.toObject(autoPartitionSeqKv.getValueArray(), autoPartitionSeqKv.getValueOffset(),
                autoPartitionSeqKv.getValueLength()) : null;
        builder.setAutoPartitionSeqName(autoPartitionSeq != null
            ? autoPartitionSeq : oldTable != null ? oldTable.getAutoPartitionSeqName() : null);

        Cell isAppendOnlySchemaKv = tableKeyValues[APPEND_ONLY_SCHEMA_INDEX];
        boolean isAppendOnlySchema = isAppendOnlySchemaKv != null && Boolean.TRUE.equals(
            PBoolean.INSTANCE.toObject(isAppendOnlySchemaKv.getValueArray(),
                isAppendOnlySchemaKv.getValueOffset(), isAppendOnlySchemaKv.getValueLength()));
        builder.setAppendOnlySchema(isAppendOnlySchemaKv != null ? isAppendOnlySchema :
            oldTable != null && oldTable.isAppendOnlySchema());

        Cell storageSchemeKv = tableKeyValues[STORAGE_SCHEME_INDEX];
        //TODO: change this once we start having other values for storage schemes
        ImmutableStorageScheme storageScheme = storageSchemeKv == null ? ImmutableStorageScheme.ONE_CELL_PER_COLUMN : ImmutableStorageScheme
                .fromSerializedValue((byte) PTinyint.INSTANCE.toObject(storageSchemeKv.getValueArray(),
                        storageSchemeKv.getValueOffset(), storageSchemeKv.getValueLength()));
        builder.setImmutableStorageScheme(storageSchemeKv != null ? storageScheme :
            oldTable != null ? oldTable.getImmutableStorageScheme() : ImmutableStorageScheme.ONE_CELL_PER_COLUMN);

        Cell encodingSchemeKv = tableKeyValues[QUALIFIER_ENCODING_SCHEME_INDEX];
        QualifierEncodingScheme encodingScheme = encodingSchemeKv == null ? QualifierEncodingScheme.NON_ENCODED_QUALIFIERS : QualifierEncodingScheme
                .fromSerializedValue((byte) PTinyint.INSTANCE.toObject(encodingSchemeKv.getValueArray(),
                        encodingSchemeKv.getValueOffset(), encodingSchemeKv.getValueLength()));
        builder.setQualifierEncodingScheme(encodingSchemeKv != null ? encodingScheme :
            oldTable != null ? oldTable.getEncodingScheme() : QualifierEncodingScheme.NON_ENCODED_QUALIFIERS);

        Cell useStatsForParallelizationKv = tableKeyValues[USE_STATS_FOR_PARALLELIZATION_INDEX];
        Boolean useStatsForParallelization = useStatsForParallelizationKv == null ? null :
            Boolean.TRUE.equals(PBoolean.INSTANCE.toObject(useStatsForParallelizationKv.getValueArray(), useStatsForParallelizationKv.getValueOffset(), useStatsForParallelizationKv.getValueLength()));
         builder.setUseStatsForParallelization(useStatsForParallelization != null ?
             useStatsForParallelization : oldTable != null ? oldTable.useStatsForParallelization() : null);

        Cell phoenixTTLKv = tableKeyValues[PHOENIX_TTL_INDEX];
        long phoenixTTL = phoenixTTLKv == null ? PHOENIX_TTL_NOT_DEFINED :
                PLong.INSTANCE.getCodec().decodeLong(phoenixTTLKv.getValueArray(),
                        phoenixTTLKv.getValueOffset(), SortOrder.getDefault());
        builder.setPhoenixTTL(phoenixTTLKv != null ? phoenixTTL :
            oldTable != null ? oldTable.getPhoenixTTL() : PHOENIX_TTL_NOT_DEFINED);

        Cell phoenixTTLHWMKv = tableKeyValues[PHOENIX_TTL_HWM_INDEX];
        long phoenixTTLHWM = phoenixTTLHWMKv == null ? MIN_PHOENIX_TTL_HWM :
                PLong.INSTANCE.getCodec().decodeLong(phoenixTTLHWMKv.getValueArray(),
                        phoenixTTLHWMKv.getValueOffset(), SortOrder.getDefault());
        builder.setPhoenixTTLHighWaterMark(phoenixTTLHWMKv != null ? phoenixTTLHWM :
            oldTable != null ? oldTable.getPhoenixTTLHighWaterMark() : MIN_PHOENIX_TTL_HWM);

        // Check the cell tag to see whether the view has modified this property
        final byte[] tagPhoenixTTL = (phoenixTTLKv == null) ?
                HConstants.EMPTY_BYTE_ARRAY : CellUtil.getTagArray(phoenixTTLKv);
        boolean viewModifiedPhoenixTTL = (PTableType.VIEW.equals(tableType)) &&
                Bytes.contains(tagPhoenixTTL, MetaDataEndpointImplConstants.VIEW_MODIFIED_PROPERTY_BYTES);
        builder.setViewModifiedPhoenixTTL(oldTable != null ?
            oldTable.hasViewModifiedPhoenixTTL() || viewModifiedPhoenixTTL : viewModifiedPhoenixTTL);

        Cell lastDDLTimestampKv = tableKeyValues[LAST_DDL_TIMESTAMP_INDEX];
        Long lastDDLTimestamp = lastDDLTimestampKv == null ?
           null : PLong.INSTANCE.getCodec().decodeLong(lastDDLTimestampKv.getValueArray(),
                lastDDLTimestampKv.getValueOffset(), SortOrder.getDefault());
        builder.setLastDDLTimestamp(lastDDLTimestampKv != null ? lastDDLTimestamp :
            oldTable != null ? oldTable.getLastDDLTimestamp() : null);

        Cell changeDetectionEnabledKv = tableKeyValues[CHANGE_DETECTION_ENABLED_INDEX];
        boolean isChangeDetectionEnabled = changeDetectionEnabledKv != null
            && Boolean.TRUE.equals(PBoolean.INSTANCE.toObject(changeDetectionEnabledKv.getValueArray(),
            changeDetectionEnabledKv.getValueOffset(),
            changeDetectionEnabledKv.getValueLength()));
        builder.setIsChangeDetectionEnabled(changeDetectionEnabledKv != null ?
            isChangeDetectionEnabled : oldTable != null && oldTable.isChangeDetectionEnabled());

        Cell schemaVersionKv = tableKeyValues[SCHEMA_VERSION_INDEX];
        String schemaVersion = schemaVersionKv != null ? (String) PVarchar.INSTANCE.toObject(
                schemaVersionKv.getValueArray(), schemaVersionKv.getValueOffset(), schemaVersionKv.getValueLength())
                : null;
        builder.setSchemaVersion(schemaVersion != null ?
            schemaVersion : oldTable != null ? oldTable.getSchemaVersion() : null);

        Cell externalSchemaIdKv = tableKeyValues[EXTERNAL_SCHEMA_ID_INDEX];
        String externalSchemaId = externalSchemaIdKv != null ?
            (String) PVarchar.INSTANCE.toObject(externalSchemaIdKv.getValueArray(),
                externalSchemaIdKv.getValueOffset(), externalSchemaIdKv.getValueLength())
            : null;
        builder.setExternalSchemaId(externalSchemaId != null ? externalSchemaId :
            oldTable != null ? oldTable.getExternalSchemaId() : null);

        Cell streamingTopicNameKv = tableKeyValues[STREAMING_TOPIC_NAME_INDEX];
        String streamingTopicName = streamingTopicNameKv != null ?
            (String) PVarchar.INSTANCE.toObject(streamingTopicNameKv.getValueArray(),
                streamingTopicNameKv.getValueOffset(), streamingTopicNameKv.getValueLength())
            : null;
        builder.setStreamingTopicName(streamingTopicName != null ? streamingTopicName :
            oldTable != null ? oldTable.getStreamingTopicName() : null);

        Cell indexWhereKv = tableKeyValues[INDEX_WHERE_INDEX];
        String indexWhere = indexWhereKv != null
                ? (String) PVarchar.INSTANCE.toObject(indexWhereKv.getValueArray(),
                        indexWhereKv.getValueOffset(), indexWhereKv.getValueLength())
                : null;
        builder.setIndexWhere(indexWhere != null ? indexWhere
                : oldTable != null ? oldTable.getIndexWhere() : null);
        // Check the cell tag to see whether the view has modified this property
        final byte[] tagUseStatsForParallelization = (useStatsForParallelizationKv == null) ?
                HConstants.EMPTY_BYTE_ARRAY :
                TagUtil.concatTags(HConstants.EMPTY_BYTE_ARRAY, useStatsForParallelizationKv);
        boolean viewModifiedUseStatsForParallelization = (PTableType.VIEW.equals(tableType)) &&
                Bytes.contains(tagUseStatsForParallelization, MetaDataEndpointImplConstants.VIEW_MODIFIED_PROPERTY_BYTES);
        builder.setViewModifiedUseStatsForParallelization(viewModifiedUseStatsForParallelization ||
            (oldTable != null && oldTable.hasViewModifiedUseStatsForParallelization()));

        boolean setPhysicalName = false;
        List<PColumn> columns = Lists.newArrayListWithExpectedSize(columnCount);
        List<PTable> indexes = Lists.newArrayList();
        List<PName> physicalTables = Lists.newArrayList();
        PName parentTableName = tableType == INDEX ? dataTableName : null;
        PName parentSchemaName = tableType == INDEX ? schemaName : null;
        PName parentLogicalName = null;
        EncodedCQCounter cqCounter = null;
        if (oldTable != null) {
            cqCounter = oldTable.getEncodedCQCounter();
        } else {
            cqCounter = (!EncodedColumnsUtil.usesEncodedColumnNames(encodingScheme) || tableType == PTableType.VIEW) ?
                PTable.EncodedCQCounter.NULL_COUNTER :
                new EncodedCQCounter();
        }

        if (timeStamp == HConstants.LATEST_TIMESTAMP) {
            timeStamp = lastDDLTimestamp != null ? lastDDLTimestamp : clientTimeStamp;
        }
        builder.setTimeStamp(timeStamp);


        PTable transformingNewTable = null;
        boolean isRegularView = (tableType == PTableType.VIEW && viewType != ViewType.MAPPED);
        for (List<Cell> columnCellList : allColumnCellList) {

            Cell colKv = columnCellList.get(LINK_TYPE_INDEX);
            int colKeyLength = colKv.getRowLength();

            PName colName = newPName(colKv.getRowArray(), colKv.getRowOffset() + offset, colKeyLength - offset);
            if (colName == null) {
                continue;
            }
            int colKeyOffset = offset + colName.getBytes().length + 1;
            PName famName = newPName(colKv.getRowArray(), colKv.getRowOffset() + colKeyOffset, colKeyLength - colKeyOffset);

            if (isQualifierCounterKV(colKv)) {
                if (famName != null) {
                    Integer value = PInteger.INSTANCE.getCodec().decodeInt(colKv.getValueArray(), colKv.getValueOffset(), SortOrder.ASC);
                    cqCounter.setValue(famName.getString(), value);
                }
            } else if (Bytes.compareTo(LINK_TYPE_BYTES, 0, LINK_TYPE_BYTES.length, colKv.getQualifierArray(), colKv.getQualifierOffset(), colKv.getQualifierLength()) == 0) {
                LinkType linkType = LinkType.fromSerializedValue(colKv.getValueArray()[colKv.getValueOffset()]);
                if (linkType == LinkType.INDEX_TABLE) {
                    addIndexToTable(tenantId, schemaName, famName, tableName, clientTimeStamp, indexes, clientVersion);
                } else if (linkType == LinkType.PHYSICAL_TABLE) {
                    // famName contains the logical name of the parent table. We need to get the actual physical name of the table
                    PTable parentTable = null;
                    // call getTable() on famName only if it does not start with _IDX_.
                    // Table name starting with _IDX_ always must refer to HBase table that is
                    // shared by all view indexes on the given table/view hierarchy.
                    // _IDX_ is HBase table that does not have corresponding PTable representation
                    // in Phoenix, hence there is no point of calling getTable().
                    if (!famName.getString().startsWith(MetaDataUtil.VIEW_INDEX_TABLE_PREFIX)
                        && indexType != IndexType.LOCAL) {
                        try {
                            parentTable = doGetTable(null,
                                SchemaUtil.getSchemaNameFromFullName(famName.getBytes())
                                    .getBytes(StandardCharsets.UTF_8),
                                SchemaUtil.getTableNameFromFullName(famName.getBytes())
                                    .getBytes(StandardCharsets.UTF_8), clientTimeStamp,
                                clientVersion);
                        } catch (SQLException e) {
                            if (e.getErrorCode()
                                != SQLExceptionCode.GET_TABLE_ERROR.getErrorCode()) {
                                LOGGER.error(
                                    "Error while retrieving getTable for PHYSICAL_TABLE link to {}",
                                    famName, e);
                                throw e;
                            }
                        }
                        if (isSystemCatalogSplittable
                            && (parentTable == null || isTableDeleted(parentTable))) {
                            // parentTable is neither in the cache nor in the local region. Since
                            // famName is only logical name, we need to find the physical table.
                            // Hence, it is recommended to scan SYSTEM.CATALOG table again using
                            // separate CQSI connection as SYSTEM.CATALOG is splittable so the
                            // PTable with famName might be available on different region.
                            try (PhoenixConnection connection = QueryUtil.getConnectionOnServer(env.getConfiguration()).unwrap(PhoenixConnection.class)) {
                                parentTable = connection.getTableNoCache(famName.getString());
                            } catch (TableNotFoundException e) {
                                // It is ok to swallow this exception since this could be a view index and _IDX_ table is not there.
                            }
                        }
                    }

                    if (parentTable == null) {
                        if (indexType == IndexType.LOCAL) {
                            PName tablePhysicalName = getPhysicalTableName(
                                env.getRegion(),null,
                                SchemaUtil.getSchemaNameFromFullName(
                                    famName.getBytes()).getBytes(StandardCharsets.UTF_8),
                                SchemaUtil.getTableNameFromFullName(
                                    famName.getBytes()).getBytes(StandardCharsets.UTF_8),
                                clientTimeStamp);
                            if (tablePhysicalName == null) {
                                physicalTables.add(famName);
                                setPhysicalName = true;
                            } else {
                                physicalTables.add(SchemaUtil.getPhysicalHBaseTableName(schemaName, tablePhysicalName, isNamespaceMapped));
                                setPhysicalName = true;
                            }
                        } else {
                            physicalTables.add(famName);
                            setPhysicalName = true;
                        }
                        // If this is a view index, then one of the link is IDX_VW -> _IDX_ PhysicalTable link. Since famName is _IDX_ and we can't get this table hence it is null, we need to use actual view name
                        parentLogicalName = (tableType == INDEX ? SchemaUtil.getTableName(parentSchemaName, parentTableName) : famName);
                    } else {
                        String parentPhysicalTableName = parentTable.getPhysicalName().getString();
                        physicalTables.add(PNameFactory.newName(parentPhysicalTableName));
                        setPhysicalName = true;
                        parentLogicalName = SchemaUtil.getTableName(parentTable.getSchemaName(), parentTable.getTableName());
                    }
                } else if (linkType == LinkType.PARENT_TABLE) {
                    parentTableName = PNameFactory.newName(SchemaUtil.getTableNameFromFullName(famName.getBytes()));
                    parentSchemaName = PNameFactory.newName(SchemaUtil.getSchemaNameFromFullName(famName.getBytes()));
                } else if (linkType == LinkType.EXCLUDED_COLUMN) {
                    // add the excludedColumn
                    addExcludedColumnToTable(columns, colName, famName, colKv.getTimestamp());
                } else if (linkType == LinkType.TRANSFORMING_NEW_TABLE) {
                    transformingNewTable = doGetTable((tenantId == null ? ByteUtil.EMPTY_BYTE_ARRAY : tenantId.getBytes())
                            , SchemaUtil.getSchemaNameFromFullName(famName.getBytes()).getBytes(), SchemaUtil.getTableNameFromFullName(famName.getBytes()).getBytes(), clientTimeStamp, null, clientVersion);
                    if (transformingNewTable == null) {
                        // It could be global
                        transformingNewTable = doGetTable(ByteUtil.EMPTY_BYTE_ARRAY
                                , SchemaUtil.getSchemaNameFromFullName(famName.getBytes()).getBytes(), SchemaUtil.getTableNameFromFullName(famName.getBytes()).getBytes(), clientTimeStamp, null, clientVersion);
                        if (transformingNewTable == null) {
                            ClientUtil.throwIOException("Transforming new table not found", new TableNotFoundException(schemaName.getString(), famName.getString()));
                        }
                    }
                }
            } else {
                long columnTimestamp =
                    columnCellList.get(0).getTimestamp() != HConstants.LATEST_TIMESTAMP ?
                        columnCellList.get(0).getTimestamp() : timeStamp;
                boolean isSalted = saltBucketNum != null
                    || (oldTable != null &&
                    oldTable.getBucketNum() != null && oldTable.getBucketNum() > 0);
                addColumnToTable(columnCellList, colName, famName, colKeyValues, columns,
                    isSalted, baseColumnCount, isRegularView, columnTimestamp);
            }
        }
        builder.setEncodedCQCounter(cqCounter);

        builder.setIndexes(indexes != null ? indexes : oldTable != null
            ? oldTable.getIndexes() : Collections.<PTable>emptyList());

        if (physicalTables == null || physicalTables.size() == 0) {
            builder.setPhysicalNames(oldTable != null ? oldTable.getPhysicalNames()
                : ImmutableList.<PName>of());
        } else {
            builder.setPhysicalNames(ImmutableList.copyOf(physicalTables));
        }
        if (!setPhysicalName && oldTable != null) {
            builder.setPhysicalTableName(oldTable.getPhysicalName(true));
        }
        builder.setTransformingNewTable(transformingNewTable);

        builder.setExcludedColumns(ImmutableList.<PColumn>of());
        builder.setBaseTableLogicalName(parentLogicalName != null ?
            parentLogicalName : oldTable != null ? oldTable.getBaseTableLogicalName() : null);
        builder.setParentTableName(parentTableName != null ?
            parentTableName : oldTable != null ? oldTable.getParentTableName() : null);
        builder.setParentSchemaName(parentSchemaName != null ? parentSchemaName :
            oldTable != null ? oldTable.getParentSchemaName() : null);

        builder.addOrSetColumns(columns);
        // Avoid querying the stats table because we're holding the rowLock here. Issuing an RPC to a remote
        // server while holding this lock is a bad idea and likely to cause contention.
        return builder.build();
    }

    private Long getViewIndexId(Cell[] tableKeyValues, PDataType viewIndexIdType) {
        Cell viewIndexIdKv = tableKeyValues[VIEW_INDEX_ID_INDEX];
        return viewIndexIdKv == null ? null :
                decodeViewIndexId(viewIndexIdKv, viewIndexIdType);
    }

    private PTable modifyIndexStateForOldClient(int clientVersion, PTable table)
            throws SQLException {
        if (table == null) {
            return table;
        }
        // PHOENIX-5073 Sets the index state based on the client version in case of old clients.
        // If client is not yet up to 4.12, then translate PENDING_ACTIVE to ACTIVE (as would have
        // been the value in those versions) since the client won't have this index state in its
        // enum.
        if (table.getIndexState() == PIndexState.PENDING_ACTIVE
                && clientVersion < MetaDataProtocol.MIN_PENDING_ACTIVE_INDEX) {
            table =
                    PTableImpl.builderWithColumns(table, PTableImpl.getColumnsToClone(table))
                            .setState(PIndexState.ACTIVE).build();
        }
        // If client is not yet up to 4.14, then translate PENDING_DISABLE to DISABLE
        // since the client won't have this index state in its enum.
        if (table.getIndexState() == PIndexState.PENDING_DISABLE
                && clientVersion < MetaDataProtocol.MIN_PENDING_DISABLE_INDEX) {
            // note: for older clients, we have to rely on the rebuilder to transition
            // PENDING_DISABLE -> DISABLE
            table =
                    PTableImpl.builderWithColumns(table, PTableImpl.getColumnsToClone(table))
                            .setState(PIndexState.DISABLE).build();
        }
        return table;
    }

    /**
     * Returns viewIndexId based on its underlying data type
     *
     * @param viewIndexIdKv
     * @param viewIndexIdType
     * @return
     */
    private Long decodeViewIndexId(Cell viewIndexIdKv, PDataType viewIndexIdType) {
        return viewIndexIdType.getCodec().decodeLong(viewIndexIdKv.getValueArray(),
                viewIndexIdKv.getValueOffset(), SortOrder.getDefault());
    }

    private PDataType getViewIndexIdType(Cell[] tableKeyValues) {
        Cell dataTypeKv = tableKeyValues[VIEW_INDEX_ID_DATA_TYPE_INDEX];
        return dataTypeKv == null ?
                MetaDataUtil.getLegacyViewIndexIdDataType() :
                PDataType.fromTypeId(PInteger.INSTANCE.getCodec()
                        .decodeInt(dataTypeKv.getValueArray(), dataTypeKv.getValueOffset(), SortOrder.getDefault()));
    }

    private boolean isQualifierCounterKV(Cell kv) {
        int cmp =
                Bytes.compareTo(kv.getQualifierArray(), kv.getQualifierOffset(),
                        kv.getQualifierLength(), QUALIFIER_COUNTER_KV.getQualifierArray(),
                        QUALIFIER_COUNTER_KV.getQualifierOffset(), QUALIFIER_COUNTER_KV.getQualifierLength());
        return cmp == 0;
    }

    private PSchema getSchema(RegionScanner scanner, long clientTimeStamp) throws IOException, SQLException {
        List<Cell> results = Lists.newArrayList();
        scanner.next(results);
        if (results.isEmpty()) {
            return null;
        }

        Cell keyValue = results.get(0);
        byte[] keyBuffer = keyValue.getRowArray();
        int keyLength = keyValue.getRowLength();
        int keyOffset = keyValue.getRowOffset();
        PName tenantId = newPName(keyBuffer, keyOffset, keyLength);
        int tenantIdLength = (tenantId == null) ? 0 : tenantId.getBytes().length;
        if (tenantIdLength == 0) {
            tenantId = null;
        }
        PName schemaName = newPName(keyBuffer, keyOffset + tenantIdLength + 1, keyLength - tenantIdLength - 1);
        long timeStamp = keyValue.getTimestamp();
        return new PSchema(schemaName.getString(), timeStamp);
    }

    private PFunction getFunction(RegionScanner scanner, final boolean isReplace, long clientTimeStamp, List<Mutation> deleteMutationsForReplace)
            throws IOException, SQLException {
        List<Cell> results = Lists.newArrayList();
        scanner.next(results);
        if (results.isEmpty()) {
            return null;
        }
        Cell[] functionKeyValues = new Cell[FUNCTION_KV_COLUMNS.size()];
        Cell[] functionArgKeyValues = new Cell[FUNCTION_ARG_KV_COLUMNS.size()];
        // Create PFunction based on KeyValues from scan
        Cell keyValue = results.get(0);
        byte[] keyBuffer = keyValue.getRowArray();
        int keyLength = keyValue.getRowLength();
        int keyOffset = keyValue.getRowOffset();
        long currentTimeMillis = EnvironmentEdgeManager.currentTimeMillis();
        if (isReplace) {
            long deleteTimeStamp =
                    clientTimeStamp == HConstants.LATEST_TIMESTAMP ? currentTimeMillis - 1
                            : (keyValue.getTimestamp() < clientTimeStamp ? clientTimeStamp - 1
                            : keyValue.getTimestamp());
            deleteMutationsForReplace.add(new Delete(keyBuffer, keyOffset, keyLength, deleteTimeStamp));
        }
        PName tenantId = newPName(keyBuffer, keyOffset, keyLength);
        int tenantIdLength = (tenantId == null) ? 0 : tenantId.getBytes().length;
        if (tenantIdLength == 0) {
            tenantId = null;
        }
        PName functionName =
                newPName(keyBuffer, keyOffset + tenantIdLength + 1, keyLength - tenantIdLength - 1);
        int functionNameLength = functionName.getBytes().length + 1;
        int offset = tenantIdLength + functionNameLength + 1;

        long timeStamp = keyValue.getTimestamp();

        int i = 0;
        int j = 0;
        while (i < results.size() && j < FUNCTION_KV_COLUMNS.size()) {
            Cell kv = results.get(i);
            Cell searchKv = FUNCTION_KV_COLUMNS.get(j);
            int cmp =
                    Bytes.compareTo(kv.getQualifierArray(), kv.getQualifierOffset(),
                            kv.getQualifierLength(), searchKv.getQualifierArray(),
                            searchKv.getQualifierOffset(), searchKv.getQualifierLength());
            if (cmp == 0) {
                timeStamp = Math.max(timeStamp, kv.getTimestamp()); // Find max timestamp of table
                // header row
                functionKeyValues[j++] = kv;
                i++;
            } else if (cmp > 0) {
                timeStamp = Math.max(timeStamp, kv.getTimestamp());
                functionKeyValues[j++] = null;
            } else {
                i++; // shouldn't happen - means unexpected KV in system table header row
            }
        }
        // CLASS_NAME,NUM_ARGS and JAR_PATH are required.
        if (functionKeyValues[CLASS_NAME_INDEX] == null || functionKeyValues[NUM_ARGS_INDEX] == null) {
            throw new IllegalStateException(
                    "Didn't find expected key values for function row in metadata row");
        }

        Cell classNameKv = functionKeyValues[CLASS_NAME_INDEX];
        PName className = newPName(classNameKv.getValueArray(), classNameKv.getValueOffset(),
                classNameKv.getValueLength());
        Cell jarPathKv = functionKeyValues[JAR_PATH_INDEX];
        PName jarPath = null;
        if (jarPathKv != null) {
            jarPath = newPName(jarPathKv.getValueArray(), jarPathKv.getValueOffset(),
                    jarPathKv.getValueLength());
        }
        Cell numArgsKv = functionKeyValues[NUM_ARGS_INDEX];
        int numArgs =
                PInteger.INSTANCE.getCodec().decodeInt(numArgsKv.getValueArray(),
                        numArgsKv.getValueOffset(), SortOrder.getDefault());
        Cell returnTypeKv = functionKeyValues[RETURN_TYPE_INDEX];
        PName returnType =
                returnTypeKv == null ? null : newPName(returnTypeKv.getValueArray(),
                        returnTypeKv.getValueOffset(), returnTypeKv.getValueLength());

        List<FunctionArgument> arguments = Lists.newArrayListWithExpectedSize(numArgs);
        for (int k = 0; k < numArgs; k++) {
            results.clear();
            scanner.next(results);
            if (results.isEmpty()) {
                break;
            }
            Cell typeKv = results.get(0);
            if (isReplace) {
                long deleteTimeStamp =
                        clientTimeStamp == HConstants.LATEST_TIMESTAMP ? currentTimeMillis - 1
                                : (typeKv.getTimestamp() < clientTimeStamp ? clientTimeStamp - 1
                                : typeKv.getTimestamp());
                deleteMutationsForReplace.add(new Delete(typeKv.getRowArray(), typeKv
                        .getRowOffset(), typeKv.getRowLength(), deleteTimeStamp));
            }
            int typeKeyLength = typeKv.getRowLength();
            PName typeName =
                    newPName(typeKv.getRowArray(), typeKv.getRowOffset() + offset, typeKeyLength
                            - offset - 3);

            int argPositionOffset = offset + typeName.getBytes().length + 1;
            short argPosition = Bytes.toShort(typeKv.getRowArray(), typeKv.getRowOffset() + argPositionOffset, typeKeyLength
                    - argPositionOffset);
            addArgumentToFunction(results, functionName, typeName, functionArgKeyValues, arguments, argPosition);
        }
        Collections.sort(arguments, new Comparator<FunctionArgument>() {
            @Override
            public int compare(FunctionArgument o1, FunctionArgument o2) {
                return o1.getArgPosition() - o2.getArgPosition();
            }
        });
        return new PFunction(tenantId, functionName.getString(), arguments, returnType.getString(),
                className.getString(), jarPath == null ? null : jarPath.getString(), timeStamp);
    }

    private PTable buildDeletedTable(byte[] key, ImmutableBytesPtr cacheKey, Region region,
                                     long clientTimeStamp) throws IOException {
        if (clientTimeStamp == HConstants.LATEST_TIMESTAMP) {
            return null;
        }

        Scan scan = MetaDataUtil.newTableRowsScan(key, clientTimeStamp, HConstants.LATEST_TIMESTAMP);
        scan.setFilter(new FirstKeyOnlyFilter());
        scan.setRaw(true);
        List<Cell> results = Lists.<Cell>newArrayList();
        try (RegionScanner scanner = region.getScanner(scan)) {
            scanner.next(results);
        }
        for (Cell kv : results) {
            KeyValue.Type type = Type.codeToType(kv.getTypeByte());
            if (type == Type.DeleteFamily) { // Row was deleted
                Cache<ImmutableBytesPtr, PMetaDataEntity> metaDataCache =
                        GlobalCache.getInstance(this.env).getMetaDataCache();
                PTable table = newDeletedTableMarker(kv.getTimestamp());
                metaDataCache.put(cacheKey, table);
                metricsSource.incrementMetadataCacheAddCount();
                metricsSource.incrementMetadataCacheUsedSize(table.getEstimatedSize());
                return table;
            }
        }
        return null;
    }


    private PFunction buildDeletedFunction(byte[] key, ImmutableBytesPtr cacheKey, Region region,
                                           long clientTimeStamp) throws IOException {
        if (clientTimeStamp == HConstants.LATEST_TIMESTAMP) {
            return null;
        }

        Scan scan = MetaDataUtil.newTableRowsScan(key, clientTimeStamp, HConstants.LATEST_TIMESTAMP);
        scan.setFilter(new FirstKeyOnlyFilter());
        scan.setRaw(true);
        List<Cell> results = Lists.<Cell>newArrayList();
        try (RegionScanner scanner = region.getScanner(scan);) {
            scanner.next(results);
        }
        // HBase ignores the time range on a raw scan (HBASE-7362)
        if (!results.isEmpty() && results.get(0).getTimestamp() > clientTimeStamp) {
            Cell kv = results.get(0);
            if (kv.getTypeByte() == Type.Delete.getCode()) {
                Cache<ImmutableBytesPtr, PMetaDataEntity> metaDataCache =
                        GlobalCache.getInstance(this.env).getMetaDataCache();
                PFunction function = newDeletedFunctionMarker(kv.getTimestamp());
                metaDataCache.put(cacheKey, function);
                metricsSource.incrementMetadataCacheAddCount();
                metricsSource.incrementMetadataCacheUsedSize(function.getEstimatedSize());
                return function;
            }
        }
        return null;
    }

    private PSchema buildDeletedSchema(byte[] key, ImmutableBytesPtr cacheKey, Region region, long clientTimeStamp)
            throws IOException {
        if (clientTimeStamp == HConstants.LATEST_TIMESTAMP) {
            return null;
        }

        Scan scan = MetaDataUtil.newTableRowsScan(key, clientTimeStamp, HConstants.LATEST_TIMESTAMP);
        scan.setFilter(new FirstKeyOnlyFilter());
        scan.setRaw(true);
        List<Cell> results = Lists.<Cell>newArrayList();
        try (RegionScanner scanner = region.getScanner(scan);) {
            scanner.next(results);
        }
        // HBase ignores the time range on a raw scan (HBASE-7362)
        if (!results.isEmpty() && results.get(0).getTimestamp() > clientTimeStamp) {
            Cell kv = results.get(0);
            if (kv.getTypeByte() == Type.Delete.getCode()) {
                Cache<ImmutableBytesPtr, PMetaDataEntity> metaDataCache = GlobalCache.getInstance(this.env)
                        .getMetaDataCache();
                PSchema schema = newDeletedSchemaMarker(kv.getTimestamp());
                metaDataCache.put(cacheKey, schema);
                metricsSource.incrementMetadataCacheAddCount();
                metricsSource.incrementMetadataCacheUsedSize(schema.getEstimatedSize());
                return schema;
            }
        }
        return null;
    }

    private static PTable newDeletedTableMarker(long timestamp) {
        try {
            return new PTableImpl.Builder()
                    .setType(PTableType.TABLE)
                    .setTimeStamp(timestamp)
                    .setPkColumns(Collections.<PColumn>emptyList())
                    .setAllColumns(Collections.<PColumn>emptyList())
                    .setFamilyAttributes(Collections.<PColumnFamily>emptyList())
                    .setRowKeySchema(RowKeySchema.EMPTY_SCHEMA)
                    .setIndexes(Collections.<PTable>emptyList())
                    .setPhysicalNames(Collections.<PName>emptyList())
                    .build();
        } catch (SQLException e) {
            // Should never happen
            return null;
        }
    }

    private static PFunction newDeletedFunctionMarker(long timestamp) {
        return new PFunction(timestamp);
    }

    private static PSchema newDeletedSchemaMarker(long timestamp) {
        return new PSchema(timestamp);
    }

    private static boolean isTableDeleted(PTable table) {
        return table.getName() == null;
    }

    private static boolean isSchemaDeleted(PSchema schema) {
        return schema.getSchemaName() == null;
    }

    private static boolean isFunctionDeleted(PFunction function) {
        return function.getFunctionName() == null;
    }

    private PTable loadTable(RegionCoprocessorEnvironment env, byte[] key,
                             ImmutableBytesPtr cacheKey, long clientTimeStamp, long asOfTimeStamp, int clientVersion)
            throws IOException, SQLException {
        Region region = env.getRegion();
        PTable table = getTableFromCache(cacheKey, clientTimeStamp, clientVersion);
        // We always cache the latest version - fault in if not in cache
        if (table != null || (table = buildTable(key, cacheKey, region, asOfTimeStamp, clientVersion)) != null) {
            return table;
        }
        // if not found then check if newer table already exists and add delete marker for timestamp
        // found
        if (table == null
                && (table = buildDeletedTable(key, cacheKey, region, clientTimeStamp)) != null) {
            return table;
        }
        return null;
    }

    /**
     * Returns a PTable if its found in the cache.
     */
    private PTable getTableFromCache(ImmutableBytesPtr cacheKey, long clientTimeStamp, int clientVersion) {
        Cache<ImmutableBytesPtr, PMetaDataEntity> metaDataCache = GlobalCache.getInstance(this.env).getMetaDataCache();
        PTable table = (PTable) metaDataCache.getIfPresent(cacheKey);
        if (table == null) {
            metricsSource.incrementMetadataCacheMissCount();
        } else {
            metricsSource.incrementMetadataCacheHitCount();
        }
        return table;
    }

    private PFunction loadFunction(RegionCoprocessorEnvironment env, byte[] key,
                                   ImmutableBytesPtr cacheKey, long clientTimeStamp, long asOfTimeStamp, boolean isReplace, List<Mutation> deleteMutationsForReplace)
            throws IOException, SQLException {
        Region region = env.getRegion();
        Cache<ImmutableBytesPtr, PMetaDataEntity> metaDataCache = GlobalCache.getInstance(this.env).getMetaDataCache();
        PFunction function = (PFunction) metaDataCache.getIfPresent(cacheKey);
        // We always cache the latest version - fault in if not in cache
        if (function != null && !isReplace) {
            metricsSource.incrementMetadataCacheHitCount();
            return function;
        }
        metricsSource.incrementMetadataCacheMissCount();
        ArrayList<byte[]> arrayList = new ArrayList<byte[]>(1);
        arrayList.add(key);
        List<PFunction> functions = buildFunctions(arrayList, region, asOfTimeStamp, isReplace, deleteMutationsForReplace);
        if (functions != null) return functions.get(0);
        // if not found then check if newer table already exists and add delete marker for timestamp
        // found
        if (function == null
                && (function = buildDeletedFunction(key, cacheKey, region, clientTimeStamp)) != null) {
            return function;
        }
        return null;
    }

    private PSchema loadSchema(RegionCoprocessorEnvironment env, byte[] key, ImmutableBytesPtr cacheKey,
                               long clientTimeStamp, long asOfTimeStamp) throws IOException, SQLException {
        Region region = env.getRegion();
        Cache<ImmutableBytesPtr, PMetaDataEntity> metaDataCache = GlobalCache.getInstance(this.env).getMetaDataCache();
        PSchema schema = (PSchema) metaDataCache.getIfPresent(cacheKey);
        // We always cache the latest version - fault in if not in cache
        if (schema != null) {
            metricsSource.incrementMetadataCacheHitCount();
            return schema;
        }
        metricsSource.incrementMetadataCacheMissCount();
        ArrayList<byte[]> arrayList = new ArrayList<byte[]>(1);
        arrayList.add(key);
        List<PSchema> schemas = buildSchemas(arrayList, region, asOfTimeStamp, cacheKey);
        if (schemas != null) return schemas.get(0);
        // if not found then check if newer schema already exists and add delete marker for timestamp
        // found
        if (schema == null
                && (schema = buildDeletedSchema(key, cacheKey, region, clientTimeStamp)) != null) {
            return schema;
        }
        return null;
    }

    /**
     * @return null if the physical table row information is not present.
     */
    private static void getParentAndPhysicalNames(List<Mutation> tableMetadata, byte[][] parentTenantSchemaTableNames, byte[][] physicalSchemaTableNames) {
        int size = tableMetadata.size();
        byte[][] rowKeyMetaData = new byte[3][];
        MetaDataUtil.getTenantIdAndSchemaAndTableName(tableMetadata, rowKeyMetaData);
        Mutation physicalTableRow = null;
        Mutation parentTableRow = null;
        boolean physicalTableLinkFound = false;
        boolean parentTableLinkFound = false;
        if (size >= 2) {
            int i = size - 1;
            while (i >= 1) {
                Mutation m = tableMetadata.get(i);
                if (m instanceof Put) {
                    LinkType linkType = MetaDataUtil.getLinkType(m);
                    if (linkType == LinkType.PHYSICAL_TABLE) {
                        physicalTableRow = m;
                        physicalTableLinkFound = true;
                    }
                    if (linkType == LinkType.PARENT_TABLE) {
                        parentTableRow = m;
                        parentTableLinkFound = true;
                    }
                }
                if (physicalTableLinkFound && parentTableLinkFound) {
                    break;
                }
                i--;
            }
        }
        if (!parentTableLinkFound) {
            parentTenantSchemaTableNames[0] = null;
            parentTenantSchemaTableNames[1] = null;
            parentTenantSchemaTableNames[2] = null;

        }
        if (!physicalTableLinkFound) {
            physicalSchemaTableNames[0] = null;
            physicalSchemaTableNames[1] = null;
            physicalSchemaTableNames[2] = null;
        }
        if (physicalTableLinkFound) {
            getSchemaTableNames(physicalTableRow, physicalSchemaTableNames);
        }
        if (parentTableLinkFound) {
            getSchemaTableNames(parentTableRow, parentTenantSchemaTableNames);
        }
    }

    private static void getSchemaTableNames(Mutation row, byte[][] schemaTableNames) {
        byte[][] rowKeyMetaData = new byte[5][];
        getVarChars(row.getRow(), 5, rowKeyMetaData);
        byte[] tenantId = rowKeyMetaData[PhoenixDatabaseMetaData.TENANT_ID_INDEX];
        byte[] colBytes = rowKeyMetaData[PhoenixDatabaseMetaData.COLUMN_NAME_INDEX];
        byte[] famBytes = rowKeyMetaData[PhoenixDatabaseMetaData.FAMILY_NAME_INDEX];
        if ((colBytes == null || colBytes.length == 0) && (famBytes != null && famBytes.length > 0)) {
            byte[] sName =
                    SchemaUtil.getSchemaNameFromFullName(famBytes).getBytes(StandardCharsets.UTF_8);
            byte[] tName =
                    SchemaUtil.getTableNameFromFullName(famBytes).getBytes(StandardCharsets.UTF_8);
            schemaTableNames[0] = tenantId;
            schemaTableNames[1] = sName;
            schemaTableNames[2] = tName;
        }
    }

    @Override
    public void createTable(RpcController controller, CreateTableRequest request,
                            RpcCallback<MetaDataResponse> done) {
        MetaDataResponse.Builder builder = MetaDataResponse.newBuilder();
        byte[][] rowKeyMetaData = new byte[3][];
        byte[] schemaName = null;
        byte[] tableName = null;
        String fullTableName = null;
        try {
            int clientVersion = request.getClientVersion();
            List<Mutation> tableMetadata = ProtobufUtil.getMutations(request);
            MetaDataUtil.getTenantIdAndSchemaAndTableName(tableMetadata, rowKeyMetaData);
            byte[] tenantIdBytes = rowKeyMetaData[PhoenixDatabaseMetaData.TENANT_ID_INDEX];
            schemaName = rowKeyMetaData[PhoenixDatabaseMetaData.SCHEMA_NAME_INDEX];
            tableName = rowKeyMetaData[PhoenixDatabaseMetaData.TABLE_NAME_INDEX];
            fullTableName = SchemaUtil.getTableName(schemaName, tableName);
            boolean isNamespaceMapped = MetaDataUtil.isNameSpaceMapped(tableMetadata, GenericKeyValueBuilder.INSTANCE,
                    new ImmutableBytesWritable());
            final IndexType indexType = MetaDataUtil.getIndexType(tableMetadata, GenericKeyValueBuilder.INSTANCE,
                    new ImmutableBytesWritable());
            byte[] parentSchemaName = null;
            byte[] parentTableName = null;
            PTable parentTable = request.hasParentTable() ? PTableImpl.createFromProto(request.getParentTable()) : null;
            PTableType tableType = MetaDataUtil.getTableType(tableMetadata, GenericKeyValueBuilder.INSTANCE, new ImmutableBytesWritable());

            // Load table to see if it already exists
            byte[] tableKey = SchemaUtil.getTableKey(tenantIdBytes, schemaName, tableName);
            ImmutableBytesPtr cacheKey = new ImmutableBytesPtr(tableKey);
            long clientTimeStamp = MetaDataUtil.getClientTimeStamp(tableMetadata);
            boolean isChangeDetectionEnabled = MetaDataUtil.getChangeDetectionEnabled(tableMetadata);

            PTable table = null;
            // Get as of latest timestamp so we can detect if we have a newer table that already
            // exists without making an additional query
            table = loadTable(env, tableKey, cacheKey, clientTimeStamp, HConstants.LATEST_TIMESTAMP,
                    clientVersion);
            if (table != null) {
                if (table.getTimeStamp() < clientTimeStamp) {
                    // If the table is older than the client time stamp and it's deleted,
                    // continue
                    if (!isTableDeleted(table)) {
                        builder.setReturnCode(MetaDataProtos.MutationCode.TABLE_ALREADY_EXISTS);
                        builder.setMutationTime(EnvironmentEdgeManager.currentTimeMillis());
                        builder.setTable(PTableImpl.toProto(table));
                        done.run(builder.build());
                        return;
                    }
                } else {
                    builder.setReturnCode(MetaDataProtos.MutationCode.NEWER_TABLE_FOUND);
                    builder.setMutationTime(EnvironmentEdgeManager.currentTimeMillis());
                    builder.setTable(PTableImpl.toProto(table));
                    done.run(builder.build());
                    return;
                }
            }

            // check if the table was previously dropped, but had child views that have not
            // yet been cleaned up.
            // Note that for old clients connecting to a 4.15 server whose metadata hasn't been
            // upgraded, we disallow dropping a base table that has child views, so in that case
            // this is a no-op (See PHOENIX-5544)
            if (!Bytes.toString(schemaName).equals(QueryConstants.SYSTEM_SCHEMA_NAME)) {
                ServerViewUtil.dropChildViews(env, tenantIdBytes, schemaName, tableName,
                        getSystemTableForChildLinks(clientVersion, env.getConfiguration())
                                .getName());
            }

            byte[] parentTableKey = null;
            Set<TableName> indexes = new HashSet<TableName>();
            ;
            byte[] cPhysicalName = SchemaUtil.getPhysicalHBaseTableName(schemaName, tableName, isNamespaceMapped)
                    .getBytes();
            byte[] cParentPhysicalName = null;
            if (tableType == PTableType.VIEW) {
                byte[][] parentSchemaTableNames = new byte[3][];
                byte[][] parentPhysicalSchemaTableNames = new byte[3][];
                getParentAndPhysicalNames(tableMetadata, parentSchemaTableNames, parentPhysicalSchemaTableNames);
                if (parentPhysicalSchemaTableNames[2] != null) {
                    if (parentTable == null) {
                        // This is needed when we connect with a 4.14 client to
                        // a 4.15.0+ server.
                        // In that case we need to resolve the parent table on
                        // the server.
                        parentTable = doGetTable(ByteUtil.EMPTY_BYTE_ARRAY,
                                parentPhysicalSchemaTableNames[1],
                                parentPhysicalSchemaTableNames[2], clientTimeStamp, clientVersion);
                        if (parentTable == null) {
                            builder.setReturnCode(
                                    MetaDataProtos.MutationCode.PARENT_TABLE_NOT_FOUND);
                            builder.setMutationTime(EnvironmentEdgeManager.currentTimeMillis());
                            done.run(builder.build());
                            return;
                        }
                        if (parentSchemaTableNames[2] != null
                                && Bytes.compareTo(parentSchemaTableNames[2],
                                        parentPhysicalSchemaTableNames[2]) != 0) {
                            // if view is created on view
                            byte[] tenantId = parentSchemaTableNames[0] == null
                                    ? ByteUtil.EMPTY_BYTE_ARRAY
                                    : parentSchemaTableNames[0];
                            parentTable = doGetTable(tenantId, parentSchemaTableNames[1],
                                    parentSchemaTableNames[2], clientTimeStamp, clientVersion);
                            if (parentTable == null) {
                                // it could be a global view
                                parentTable = doGetTable(ByteUtil.EMPTY_BYTE_ARRAY,
                                        parentSchemaTableNames[1], parentSchemaTableNames[2],
                                        clientTimeStamp, clientVersion);
                            }
                        }
                        if (parentTable == null) {
                            builder.setReturnCode(
                                    MetaDataProtos.MutationCode.PARENT_TABLE_NOT_FOUND);
                            builder.setMutationTime(EnvironmentEdgeManager.currentTimeMillis());
                            done.run(builder.build());
                            return;
                        }
                    }
                    parentTableKey = SchemaUtil.getTableKey(ByteUtil.EMPTY_BYTE_ARRAY,
                            parentPhysicalSchemaTableNames[1], parentPhysicalSchemaTableNames[2]);
                    cParentPhysicalName = parentTable.getPhysicalName().getBytes();
                    for (PTable index : parentTable.getIndexes()) {
                        indexes.add(TableName.valueOf(index.getPhysicalName().getBytes()));
                    }
                } else {
                    // Mapped View
                    cParentPhysicalName = SchemaUtil.getPhysicalHBaseTableName(
                            schemaName, tableName, isNamespaceMapped).getBytes();

                }
                parentSchemaName = parentPhysicalSchemaTableNames[1];
                parentTableName = parentPhysicalSchemaTableNames[2];

            } else if (tableType == PTableType.INDEX) {
                parentSchemaName = schemaName;
                /*
                 * For an index we lock the parent table's row which could be a physical table or a view.
                 * If the parent table is a physical table, then the tenantIdBytes is empty because
                 * we allow creating an index with a tenant connection only if the parent table is a view.
                 */
                parentTableName = MetaDataUtil.getParentTableName(tableMetadata);
                parentTableKey = SchemaUtil.getTableKey(tenantIdBytes, parentSchemaName, parentTableName);
                if (parentTable == null) {
                    // This is needed when we connect with a 4.14 client to a 4.15.0+ server.
                    // In that case we need to resolve the parent table on the server.
                    parentTable =
                            doGetTable(tenantIdBytes, parentSchemaName, parentTableName, clientTimeStamp, null,
                                    request.getClientVersion());
                }
                if (IndexType.LOCAL == indexType) {
                    cPhysicalName = parentTable.getPhysicalName().getBytes();
                    cParentPhysicalName = parentTable.getPhysicalName().getBytes();
                } else if (parentTable.getType() == PTableType.VIEW) {
                    // The view index physical table name is constructed from logical name of base table.
                    // For example, _IDX_SC.TBL1 is the view index name and SC.TBL1 is the logical name of the base table.
                    String namepaceMappedParentLogicalName = MetaDataUtil.getNamespaceMappedName(parentTable.getBaseTableLogicalName(), isNamespaceMapped);
                    cPhysicalName = MetaDataUtil.getViewIndexPhysicalName(namepaceMappedParentLogicalName.getBytes(StandardCharsets.UTF_8));
                    cParentPhysicalName = parentTable.getPhysicalName().getBytes();
                } else {
                    cParentPhysicalName = SchemaUtil
                            .getPhysicalHBaseTableName(parentSchemaName, parentTableName, isNamespaceMapped).getBytes();
                }
            }

            getCoprocessorHost().preCreateTable(Bytes.toString(tenantIdBytes),
                    fullTableName,
                    (tableType == PTableType.VIEW) ? null : TableName.valueOf(cPhysicalName),
                    cParentPhysicalName == null ? null : TableName.valueOf(cParentPhysicalName), tableType,
                    /* TODO: During inital create we may not need the family map */
                    Collections.<byte[]>emptySet(), indexes);

            Region region = env.getRegion();
            List<RowLock> locks = Lists.newArrayList();
            // Place a lock using key for the table to be created
            try {
                acquireLock(region, tableKey, locks, false);

                // If the table key resides outside the region, return without doing anything
                MetaDataMutationResult result = checkTableKeyInRegion(tableKey, region);
                if (result != null) {
                    done.run(MetaDataMutationResult.toProto(result));
                    return;
                }

                if (parentTableName != null) {
                    // From 4.15 onwards we only need to lock the parent table :
                    // 1) when creating an index on a table or a view
                    // 2) if allowSplittableSystemCatalogRollback is true we try to lock the parent table to prevent it
                    // from changing concurrently while a view is being created
                    if (tableType == PTableType.INDEX || allowSplittableSystemCatalogRollback) {
                        result = checkTableKeyInRegion(parentTableKey, region);
                        if (result != null) {
                            LOGGER.error("Unable to lock parentTableKey " + Bytes.toStringBinary(parentTableKey));
                            // if allowSplittableSystemCatalogRollback is true and we can't lock the parentTableKey (because
                            // SYSTEM.CATALOG already split) return UNALLOWED_TABLE_MUTATION so that the client
                            // knows the create statement failed
                            MetaDataProtos.MutationCode code = tableType == PTableType.INDEX ?
                                    MetaDataProtos.MutationCode.TABLE_NOT_IN_REGION :
                                    MetaDataProtos.MutationCode.UNALLOWED_TABLE_MUTATION;
                            builder.setReturnCode(code);
                            builder.setMutationTime(EnvironmentEdgeManager.currentTimeMillis());
                            done.run(builder.build());
                            return;
                        }
                        acquireLock(region, parentTableKey, locks, false);
                    }
                    // make sure we haven't gone over our threshold for indexes on this table.
                    if (execeededIndexQuota(tableType, parentTable)) {
                        builder.setReturnCode(MetaDataProtos.MutationCode.TOO_MANY_INDEXES);
                        builder.setMutationTime(EnvironmentEdgeManager.currentTimeMillis());
                        done.run(builder.build());
                        return;
                    }
                }

                // Add cell for ROW_KEY_ORDER_OPTIMIZABLE = true, as we know that new tables
                // conform the correct row key. The exception is for a VIEW, which the client
                // sends over depending on its base physical table.
                if (tableType != PTableType.VIEW) {
                    UpgradeUtil.addRowKeyOrderOptimizableCell(tableMetadata, tableKey, clientTimeStamp);
                }
                // If the parent table of the view has the auto partition sequence name attribute, modify the
                // tableMetadata and set the view statement and partition column correctly
                if (parentTable != null && parentTable.getAutoPartitionSeqName() != null) {
                    long autoPartitionNum = 1;
                    try (PhoenixConnection connection = QueryUtil.getConnectionOnServer(env.getConfiguration()).unwrap(PhoenixConnection.class);
                         Statement stmt = connection.createStatement()) {
                        String seqName = parentTable.getAutoPartitionSeqName();
                        // Not going through the standard route of using statement.execute() as that code path
                        // is blocked if the metadata hasn't been been upgraded to the new minor release.
                        String seqNextValueSql = String.format("SELECT NEXT VALUE FOR %s", seqName);
                        PhoenixStatement ps = stmt.unwrap(PhoenixStatement.class);
                        QueryPlan plan = ps.compileQuery(seqNextValueSql);
                        ResultIterator resultIterator = plan.iterator();
                        PhoenixResultSet rs = ps.newResultSet(resultIterator, plan.getProjector(), plan.getContext());
                        rs.next();
                        autoPartitionNum = rs.getLong(1);
                    } catch (SequenceNotFoundException e) {
                        builder.setReturnCode(MetaDataProtos.MutationCode.AUTO_PARTITION_SEQUENCE_NOT_FOUND);
                        builder.setMutationTime(EnvironmentEdgeManager.currentTimeMillis());
                        done.run(builder.build());
                        return;
                    }
                    PColumn autoPartitionCol = parentTable.getPKColumns().get(MetaDataUtil.getAutoPartitionColIndex(parentTable));
                    if (!PLong.INSTANCE.isCoercibleTo(autoPartitionCol.getDataType(), autoPartitionNum)) {
                        builder.setReturnCode(MetaDataProtos.MutationCode.CANNOT_COERCE_AUTO_PARTITION_ID);
                        builder.setMutationTime(EnvironmentEdgeManager.currentTimeMillis());
                        done.run(builder.build());
                        return;
                    }
                    builder.setAutoPartitionNum(autoPartitionNum);

                    // set the VIEW STATEMENT column of the header row
                    Put tableHeaderPut = MetaDataUtil.getPutOnlyTableHeaderRow(tableMetadata);
                    NavigableMap<byte[], List<Cell>> familyCellMap = tableHeaderPut.getFamilyCellMap();
                    List<Cell> cells = familyCellMap.get(TABLE_FAMILY_BYTES);
                    Cell cell = cells.get(0);
                    String autoPartitionWhere = QueryUtil.getViewPartitionClause(MetaDataUtil.getAutoPartitionColumnName(parentTable), autoPartitionNum);
                    String hbaseVersion = VersionInfo.getVersion();
                    ImmutableBytesPtr ptr = new ImmutableBytesPtr();
                    KeyValueBuilder kvBuilder = KeyValueBuilder.get(hbaseVersion);
                    MetaDataUtil.getMutationValue(tableHeaderPut, VIEW_STATEMENT_BYTES, kvBuilder, ptr);
                    byte[] value = ptr.copyBytesIfNecessary();
                    byte[] viewStatement = null;
                    // if we have an existing where clause add the auto partition where clause to it
                    if (!Bytes.equals(value, QueryConstants.EMPTY_COLUMN_VALUE_BYTES)) {
                        viewStatement = Bytes.add(value, Bytes.toBytes(" AND "), Bytes.toBytes(autoPartitionWhere));
                    } else {
                        viewStatement = Bytes.toBytes(QueryUtil.getViewStatement(parentTable.getSchemaName().getString(), parentTable.getTableName().getString(), autoPartitionWhere));
                    }
                    Cell viewStatementCell =
                            PhoenixKeyValueUtil.newKeyValue(cell.getRowArray(),
                                cell.getRowOffset(), cell.getRowLength(), cell.getFamilyArray(),
                                cell.getFamilyOffset(), cell.getFamilyLength(),
                                VIEW_STATEMENT_BYTES, 0, VIEW_STATEMENT_BYTES.length,
                                cell.getTimestamp(), viewStatement, 0, viewStatement.length,
                                cell.getType());
                    cells.add(viewStatementCell);

                    // set the IS_VIEW_REFERENCED column of the auto partition column row
                    Put autoPartitionPut = MetaDataUtil.getPutOnlyAutoPartitionColumn(parentTable, tableMetadata);
                    familyCellMap = autoPartitionPut.getFamilyCellMap();
                    cells = familyCellMap.get(TABLE_FAMILY_BYTES);
                    cell = cells.get(0);
                    PDataType dataType = autoPartitionCol.getDataType();
                    Object val = dataType.toObject(autoPartitionNum, PLong.INSTANCE);
                    byte[] bytes = new byte[dataType.getByteSize() + 1];
                    dataType.toBytes(val, bytes, 0);
                    Cell viewConstantCell =
                            PhoenixKeyValueUtil.newKeyValue(cell.getRowArray(),
                                cell.getRowOffset(), cell.getRowLength(), cell.getFamilyArray(),
                                cell.getFamilyOffset(), cell.getFamilyLength(),
                                VIEW_CONSTANT_BYTES, 0, VIEW_CONSTANT_BYTES.length,
                                cell.getTimestamp(), bytes, 0, bytes.length, cell.getType());
                    cells.add(viewConstantCell);
                }
                Long indexId = null;
                if (request.hasAllocateIndexId() && request.getAllocateIndexId()) {
                    String tenantIdStr = tenantIdBytes.length == 0 ? null : Bytes.toString(tenantIdBytes);
                    try (PhoenixConnection connection = QueryUtil.getConnectionOnServer(env.getConfiguration()).unwrap(PhoenixConnection.class)) {
                        PName physicalName = parentTable.getPhysicalName();
                        long seqValue = getViewIndexSequenceValue(connection, tenantIdStr, parentTable);
                        Put tableHeaderPut = MetaDataUtil.getPutOnlyTableHeaderRow(tableMetadata);
                        NavigableMap<byte[], List<Cell>> familyCellMap = tableHeaderPut.getFamilyCellMap();
                        List<Cell> cells = familyCellMap.get(TABLE_FAMILY_BYTES);
                        Cell cell = cells.get(0);
                        PDataType<?> dataType = MetaDataUtil.getIndexDataType(tableMetadata,
                                GenericKeyValueBuilder.INSTANCE, new ImmutableBytesWritable());
                        Object val = dataType.toObject(seqValue, PLong.INSTANCE);
                        byte[] bytes = new byte[dataType.getByteSize() + 1];
                        dataType.toBytes(val, bytes, 0);
                        Cell indexIdCell =
                                PhoenixKeyValueUtil.newKeyValue(cell.getRowArray(),
                                    cell.getRowOffset(), cell.getRowLength(),
                                    cell.getFamilyArray(), cell.getFamilyOffset(),
                                    cell.getFamilyLength(), VIEW_INDEX_ID_BYTES, 0,
                                    VIEW_INDEX_ID_BYTES.length, cell.getTimestamp(), bytes, 0,
                                    bytes.length, cell.getType());
                        cells.add(indexIdCell);
                        indexId = seqValue;
                    }
                }

                // The mutations to create a table are written in the following order:
                // 1. Write the child link as if the next two steps fail we
                // ignore missing children while processing a parent
                // (this is already done at this point, as a separate client-server RPC
                // to the ChildLinkMetaDataEndpoint coprocessor)
                // 2. Update the encoded column qualifier for the parent table if its on a
                // different region server (for tables that use column qualifier encoding)
                // if the next step fails we end up wasting a few col qualifiers
                // 3. Finally write the mutations to create the table

                if (tableType == PTableType.VIEW) {
                    // If we are connecting with an old client to a server that has new metadata
                    // i.e. it was previously connected to by a 4.15 client, then the client will
                    // also send the parent->child link metadata to SYSTEM.CATALOG rather than using
                    // the new ChildLinkMetaDataEndpoint coprocessor. In this case, we must continue
                    // doing the server-server RPC to send these mutations to SYSTEM.CHILD_LINK.
                    if (clientVersion < MIN_SPLITTABLE_SYSTEM_CATALOG &&
                            getSystemTableForChildLinks(clientVersion, env.getConfiguration()).equals(
                                    SchemaUtil.getPhysicalTableName(SYSTEM_CHILD_LINK_NAME_BYTES,
                                            env.getConfiguration()))) {
                        List<Mutation> childLinkMutations =
                                MetaDataUtil.removeChildLinkMutations(tableMetadata);
                        MetaDataResponse response =
                                processRemoteRegionMutations(
                                        PhoenixDatabaseMetaData.SYSTEM_CHILD_LINK_NAME_BYTES,
                                        childLinkMutations, UNABLE_TO_CREATE_CHILD_LINK);
                        if (response != null) {
                            done.run(response);
                            return;
                        }
                    }
                    // Pass in the parent's PTable so that we only tag cells corresponding to the
                    // view's property in case they are different from the parent
                    ViewUtil.addTagsToPutsForViewAlteredProperties(tableMetadata, parentTable,
                            (ExtendedCellBuilder)env.getCellBuilder());
                }
                //set the last DDL timestamp to the current server time since we're creating the
                // table/index/views.
                tableMetadata.add(MetaDataUtil.getLastDDLTimestampUpdate(tableKey,
                    clientTimeStamp, EnvironmentEdgeManager.currentTimeMillis()));

                //and if we're doing change detection on this table or view, notify the
                //external schema registry and get its schema id
                if (isChangeDetectionEnabled) {
                    long startTime = EnvironmentEdgeManager.currentTimeMillis();
                    try {
                        exportSchema(tableMetadata, tableKey, clientTimeStamp, clientVersion, null);
                        metricsSource.incrementCreateExportCount();
                        metricsSource.updateCreateExportTime(EnvironmentEdgeManager.currentTimeMillis() - startTime);
                    } catch (IOException ie){
                        metricsSource.incrementCreateExportFailureCount();
                        metricsSource.updateCreateExportFailureTime(EnvironmentEdgeManager.currentTimeMillis() - startTime);
                        //If we fail to write to the schema registry, fail the entire
                        //CREATE TABLE or VIEW operation so we stay consistent
                        LOGGER.error("Error writing schema to external schema registry", ie);
                        builder.setReturnCode(
                            MetaDataProtos.MutationCode.ERROR_WRITING_TO_SCHEMA_REGISTRY);
                        builder.setMutationTime(EnvironmentEdgeManager.currentTimeMillis());
                        done.run(builder.build());
                        return;
                    }
                }

                // When we drop a view we first drop the view metadata and then drop the parent->child linking row
                List<Mutation> localMutations =
                        Lists.newArrayListWithExpectedSize(tableMetadata.size());
                List<Mutation> remoteMutations = Lists.newArrayListWithExpectedSize(2);
                // check to see if there are any mutations that should not be applied to this region
                separateLocalAndRemoteMutations(region, tableMetadata, localMutations, remoteMutations);
                if (!remoteMutations.isEmpty()) {
                    // there should only be remote mutations if we are creating a view that uses
                    // encoded column qualifiers (the remote mutations are to update the encoded
                    // column qualifier counter on the parent table)
                    if (parentTable != null && tableType == PTableType.VIEW && parentTable
                            .getEncodingScheme() != QualifierEncodingScheme.NON_ENCODED_QUALIFIERS) {
                        // TODO: Avoid doing server-server RPC when we have held row locks
                        MetaDataResponse response =
                                processRemoteRegionMutations(
                                        PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES,
                                        remoteMutations, MetaDataProtos.MutationCode.UNABLE_TO_UPDATE_PARENT_TABLE);
                        clearRemoteTableFromCache(clientTimeStamp,
                                parentTable.getSchemaName() != null
                                        ? parentTable.getSchemaName().getBytes()
                                        : ByteUtil.EMPTY_BYTE_ARRAY,
                                parentTable.getTableName().getBytes());
                        if (response != null) {
                            done.run(response);
                            return;
                        }
                    } else {
                        String msg = "Found unexpected mutations while creating " + fullTableName;
                        LOGGER.error(msg);
                        for (Mutation m : remoteMutations) {
                            LOGGER.debug("Mutation rowkey : " + Bytes.toStringBinary(m.getRow()));
                            LOGGER.debug("Mutation family cell map : " + m.getFamilyCellMap());
                        }
                        throw new IllegalStateException(msg);
                    }
                }

                // TODO: Switch this to HRegion#batchMutate when we want to support indexes on the
                // system table. Basically, we get all the locks that we don't already hold for all the
                // tableMetadata rows. This ensures we don't have deadlock situations (ensuring
                // primary and then index table locks are held, in that order). For now, we just don't support
                // indexing on the system table. This is an issue because of the way we manage batch mutation
                // in the Indexer.
                mutateRowsWithLocks(this.accessCheckEnabled, region, localMutations, Collections.<byte[]>emptySet(),
                    HConstants.NO_NONCE, HConstants.NO_NONCE);

                // Invalidate the cache - the next getTable call will add it
                // TODO: consider loading the table that was just created here, patching up the parent table, and updating the cache
                Cache<ImmutableBytesPtr, PMetaDataEntity> metaDataCache = GlobalCache.getInstance(this.env).getMetaDataCache();
                if (parentTableKey != null) {
                    metaDataCache.invalidate(new ImmutableBytesPtr(parentTableKey));
                }
                metaDataCache.invalidate(cacheKey);
                // Get timeStamp from mutations - the above method sets it if it's unset
                long currentTimeStamp = MetaDataUtil.getClientTimeStamp(tableMetadata);
                builder.setReturnCode(MetaDataProtos.MutationCode.TABLE_NOT_FOUND);
                if (indexId != null) {
                    builder.setViewIndexId(indexId);
                    builder.setViewIndexIdType(PLong.INSTANCE.getSqlType());
                }
                builder.setMutationTime(currentTimeStamp);
                //send the newly built table back because we generated the DDL timestamp server
                // side and the client doesn't have it.
                PTable newTable = buildTable(tableKey, cacheKey, region,
                    clientTimeStamp, clientVersion);
                if (newTable != null) {
                    builder.setTable(PTableImpl.toProto(newTable));
                }

                done.run(builder.build());

                updateCreateTableDdlSuccessMetrics(tableType);
                LOGGER.info("{} created successfully, tableName: {}", tableType, fullTableName);
            } finally {
                ServerUtil.releaseRowLocks(locks);
            }
        } catch (Throwable t) {
            LOGGER.error("createTable failed", t);
            ProtobufUtil.setControllerException(controller,
                    ClientUtil.createIOException(fullTableName, t));
        }
    }

    private void updateCreateTableDdlSuccessMetrics(PTableType tableType) {
        if (tableType == PTableType.TABLE || tableType == PTableType.SYSTEM) {
            metricsSource.incrementCreateTableCount();
        } else if (tableType == PTableType.VIEW) {
            metricsSource.incrementCreateViewCount();
        } else if (tableType == PTableType.INDEX) {
            metricsSource.incrementCreateIndexCount();
        }
    }

    private void exportSchema(List<Mutation> tableMetadata, byte[] tableKey, long clientTimestamp,
                              int clientVersion, PTable oldTable) throws SQLException, IOException {
        List<Cell> tableCellList = MetaDataUtil.getTableCellsFromMutations(tableMetadata);

        List<List<Cell>> allColumnsCellList = MetaDataUtil.getColumnAndLinkCellsFromMutations(tableMetadata);
        //getTableFromCells assumes the Cells are sorted as they would be when reading from HBase
        Collections.sort(tableCellList, KeyValue.COMPARATOR);
        for (List<Cell> columnCellList : allColumnsCellList) {
            Collections.sort(columnCellList, KeyValue.COMPARATOR);
        }

        PTable newTable = getTableFromCells(tableCellList, allColumnsCellList, clientTimestamp,
            clientVersion, oldTable);
        PTable parentTable = null;
        //if this is a view, we need to get the columns from its parent table / view
        if (newTable != null && newTable.getType().equals(PTableType.VIEW)) {
            try (PhoenixConnection conn = (PhoenixConnection)
                ConnectionUtil.getInputConnection(env.getConfiguration())) {
                newTable = ViewUtil.addDerivedColumnsAndIndexesFromAncestors(conn, newTable);
            }
        }
        Configuration conf = env.getConfiguration();
        SchemaRegistryRepository exporter = SchemaRegistryRepositoryFactory.
            getSchemaRegistryRepository(conf);
        if (exporter != null) {
            SchemaWriter schemaWriter = SchemaWriterFactory.getSchemaWriter(conf);
            //we export to an external schema registry, then put the schema id
            //to lookup the schema in the registry into SYSTEM.CATALOG so we
            //can look it up later (and use it in WAL annotations)

            //Note that if we succeed here but the write to SYSTEM.CATALOG fails,
            //we can have "orphaned" rows in the schema registry because there's
            //no way to make this fully atomic.
            String externalSchemaId =
                exporter.exportSchema(schemaWriter, newTable);
            tableMetadata.add(MetaDataUtil.getExternalSchemaIdUpdate(tableKey,
                externalSchemaId));

        }
    }

    private long getViewIndexSequenceValue(PhoenixConnection connection, String tenantIdStr, PTable parentTable) throws SQLException {
        int nSequenceSaltBuckets = connection.getQueryServices().getSequenceSaltBuckets();
        // parentTable is parent of the view index which is the view. View table name is _IDX_+logical name of base table
        // Since parent is the view, the parentTable.getBaseTableLogicalName() returns the logical full name of the base table
        PName parentName = parentTable.getBaseTableLogicalName();
        if (parentName == null) {
            parentName = SchemaUtil.getPhysicalHBaseTableName(parentTable.getSchemaName(), parentTable.getTableName(), parentTable.isNamespaceMapped());
        }
        SequenceKey key = MetaDataUtil.getViewIndexSequenceKey(tenantIdStr, parentName,
                nSequenceSaltBuckets, parentTable.isNamespaceMapped());
        // Earlier sequence was created at (SCN-1/LATEST_TIMESTAMP) and incremented at the client max(SCN,dataTable.getTimestamp), but it seems we should
        // use always LATEST_TIMESTAMP to avoid seeing wrong sequence values by different connection having SCN
        // or not.
        long sequenceTimestamp = HConstants.LATEST_TIMESTAMP;
        try {
            connection.getQueryServices().createSequence(key.getTenantId(), key.getSchemaName(), key.getSequenceName(),
                    Short.MIN_VALUE, 1, 1, Long.MIN_VALUE, Long.MAX_VALUE, false, sequenceTimestamp);
        } catch (SequenceAlreadyExistsException e) {
            //someone else got here first and created the sequence, or it was pre-existing. Not a problem.
        }


        long[] seqValues = new long[1];
        SQLException[] sqlExceptions = new SQLException[1];
        connection.getQueryServices().incrementSequences(Collections.singletonList(new SequenceAllocation(key, 1)),
                HConstants.LATEST_TIMESTAMP, seqValues, sqlExceptions);
        if (sqlExceptions[0] != null) {
            throw sqlExceptions[0];
        }
        return seqValues[0];
    }

    private boolean execeededIndexQuota(PTableType tableType, PTable parentTable) {
        return PTableType.INDEX == tableType && parentTable.getIndexes().size() >= maxIndexesPerTable;
    }

    private void separateLocalAndRemoteMutations(Region region, List<Mutation> mutations,
                                                 List<Mutation> localMutations, List<Mutation> remoteMutations) {
        RegionInfo regionInfo = region.getRegionInfo();
        for (Mutation mutation : mutations) {
            if (regionInfo.containsRow(mutation.getRow())) {
                localMutations.add(mutation);
            } else {
                remoteMutations.add(mutation);
            }
        }
    }

    @Override
    public void dropTable(RpcController controller, DropTableRequest request,
                          RpcCallback<MetaDataResponse> done) {
        MetaDataResponse.Builder builder = MetaDataResponse.newBuilder();
        boolean isCascade = request.getCascade();
        byte[][] rowKeyMetaData = new byte[3][];
        String tableType = request.getTableType();
        byte[] schemaName = null;
        byte[] tableOrViewName = null;
        boolean dropTableStats = false;
        final int clientVersion = request.getClientVersion();
        try {
            List<Mutation> tableMetadata = ProtobufUtil.getMutations(request);
            List<Mutation> childLinkMutations = Lists.newArrayList();
            MetaDataUtil.getTenantIdAndSchemaAndTableName(tableMetadata, rowKeyMetaData);
            byte[] tenantIdBytes = rowKeyMetaData[PhoenixDatabaseMetaData.TENANT_ID_INDEX];
            schemaName = rowKeyMetaData[PhoenixDatabaseMetaData.SCHEMA_NAME_INDEX];
            tableOrViewName = rowKeyMetaData[PhoenixDatabaseMetaData.TABLE_NAME_INDEX];
            String fullTableName = SchemaUtil.getTableName(schemaName, tableOrViewName);
            PTableType pTableType = PTableType.fromSerializedValue(tableType);
            // Disallow deletion of a system table
            if (pTableType == PTableType.SYSTEM) {
                builder.setReturnCode(MetaDataProtos.MutationCode.UNALLOWED_TABLE_MUTATION);
                builder.setMutationTime(EnvironmentEdgeManager.currentTimeMillis());
                done.run(builder.build());
                return;
            }

            List<byte[]> tableNamesToDelete = Lists.newArrayList();
            List<SharedTableState> sharedTablesToDelete = Lists.newArrayList();

            byte[] lockKey = SchemaUtil.getTableKey(tenantIdBytes, schemaName, tableOrViewName);
            Region region = env.getRegion();
            MetaDataMutationResult result = checkTableKeyInRegion(lockKey, region);
            if (result != null) {
                done.run(MetaDataMutationResult.toProto(result));
                return;
            }

            byte[] parentTableName = MetaDataUtil.getParentTableName(tableMetadata);
            byte[] parentLockKey = null;
            // Only lock parent table for indexes
            if (parentTableName != null && pTableType == PTableType.INDEX) {
                parentLockKey = SchemaUtil.getTableKey(tenantIdBytes, schemaName, parentTableName);
                result = checkTableKeyInRegion(parentLockKey, region);
                if (result != null) {
                    done.run(MetaDataMutationResult.toProto(result));
                    return;
                }
            }

            long clientTimeStamp = MetaDataUtil.getClientTimeStamp(tableMetadata);
            PTable loadedTable = doGetTable(tenantIdBytes, schemaName, tableOrViewName,
                    clientTimeStamp, null, request.getClientVersion());
            if (loadedTable == null) {
                builder.setReturnCode(MetaDataProtos.MutationCode.TABLE_NOT_FOUND);
                builder.setMutationTime(EnvironmentEdgeManager.currentTimeMillis());
                done.run(builder.build());
                return;
            }
            getCoprocessorHost().preDropTable(Bytes.toString(tenantIdBytes),
                    SchemaUtil.getTableName(schemaName, tableOrViewName),
                    TableName.valueOf(loadedTable.getPhysicalName().getBytes()),
                    getParentPhysicalTableName(loadedTable), pTableType, loadedTable.getIndexes());

            if (pTableType == PTableType.TABLE || pTableType == PTableType.VIEW) {
                // check to see if the table has any child views
                try (Table hTable = ServerUtil.getHTableForCoprocessorScan(env,
                        getSystemTableForChildLinks(clientVersion, env.getConfiguration()))) {
                    // This call needs to be done before acquiring the row lock on the header row
                    // for the table/view being dropped, otherwise the calls to resolve its child
                    // views via PhoenixRuntime.getTableNoCache() will deadlock since this call
                    // itself needs to get the parent table which needs to acquire a write lock
                    // on the same header row
                    Pair<List<PTable>, List<TableInfo>> descendantViews =
                            findAllDescendantViews(hTable, env.getConfiguration(),
                                    tenantIdBytes, schemaName, tableOrViewName, clientTimeStamp,
                                    true);
                    List<PTable> legitimateChildViews = descendantViews.getFirst();
                    List<TableInfo> orphanChildViews = descendantViews.getSecond();
                    if (!legitimateChildViews.isEmpty()) {
                        if (!isCascade) {
                            LOGGER.error("DROP without CASCADE on tables or views with child views "
                                    + "is not permitted");
                            // DROP without CASCADE on tables/views with child views is
                            // not permitted
                            builder.setReturnCode(
                                    MetaDataProtos.MutationCode.UNALLOWED_TABLE_MUTATION);
                            builder.setMutationTime(EnvironmentEdgeManager.currentTimeMillis());
                            done.run(builder.build());
                            return;
                        }
                        if (clientVersion < MIN_SPLITTABLE_SYSTEM_CATALOG &&
                                !SchemaUtil.getPhysicalTableName(SYSTEM_CHILD_LINK_NAME_BYTES,
                                        env.getConfiguration()).equals(hTable.getName())) {
                            // (See PHOENIX-5544) For an old client connecting to a non-upgraded
                            // server, we disallow dropping a base table/view that has child views.
                            LOGGER.error("Dropping a table or view that has child views is "
                                    + "not permitted for old clients connecting to a new server "
                                    + "with old metadata (even if CASCADE is provided). "
                                    + "Please upgrade the client at least to  "
                                    + MIN_SPLITTABLE_SYSTEM_CATALOG_VERSION);
                            builder.setReturnCode(
                                    MetaDataProtos.MutationCode.UNALLOWED_TABLE_MUTATION);
                            builder.setMutationTime(EnvironmentEdgeManager.currentTimeMillis());
                            done.run(builder.build());
                            return;
                        }
                    }

                    // If the CASCADE option is provided and we have at least one legitimate/orphan
                    // view stemming from this parent and the client is 4.15+ (or older but
                    // connecting to an upgraded server), we use the SYSTEM.TASK table to
                    // asynchronously drop child views
                    if (isCascade && !(legitimateChildViews.isEmpty() && orphanChildViews.isEmpty())
                            && (clientVersion >= MIN_SPLITTABLE_SYSTEM_CATALOG ||
                            SchemaUtil.getPhysicalTableName(SYSTEM_CHILD_LINK_NAME_BYTES,
                                    env.getConfiguration()).equals(hTable.getName()))) {
                        try (PhoenixConnection conn =
                                QueryUtil.getConnectionOnServer(env.getConfiguration())
                                    .unwrap(PhoenixConnection.class)) {
                            ServerTask.addTask(new SystemTaskParams.SystemTaskParamsBuilder()
                                .setConn(conn)
                                .setTaskType(PTable.TaskType.DROP_CHILD_VIEWS)
                                .setTenantId(Bytes.toString(tenantIdBytes))
                                .setSchemaName(Bytes.toString(schemaName))
                                .setTableName(Bytes.toString(tableOrViewName))
                                .setTaskStatus(
                                    PTable.TaskStatus.CREATED.toString())
                                .setData(null)
                                .setPriority(null)
                                .setStartTs(null)
                                .setEndTs(null)
                                .setAccessCheckEnabled(this.accessCheckEnabled)
                                .build());
                        } catch (Throwable t) {
                            LOGGER.error("Adding a task to drop child views failed!", t);
                        }
                    }
                }
            }

            List<RowLock> locks = Lists.newArrayList();
            try {
                acquireLock(region, lockKey, locks, false);
                if (parentLockKey != null) {
                    acquireLock(region, parentLockKey, locks, false);
                }

                List<ImmutableBytesPtr> invalidateList = new ArrayList<ImmutableBytesPtr>();
                result = doDropTable(lockKey, tenantIdBytes, schemaName, tableOrViewName,
                        parentTableName, PTableType.fromSerializedValue(tableType), tableMetadata,
                        childLinkMutations, invalidateList, tableNamesToDelete,
                        sharedTablesToDelete, request.getClientVersion());
                if (result.getMutationCode() != MutationCode.TABLE_ALREADY_EXISTS) {
                    done.run(MetaDataMutationResult.toProto(result));
                    return;
                }
                Cache<ImmutableBytesPtr, PMetaDataEntity> metaDataCache =
                        GlobalCache.getInstance(this.env).getMetaDataCache();

                List<Mutation> localMutations =
                        Lists.newArrayListWithExpectedSize(tableMetadata.size());
                List<Mutation> remoteMutations = Lists.newArrayList();
                separateLocalAndRemoteMutations(region, tableMetadata, localMutations,
                        remoteMutations);
                if (!remoteMutations.isEmpty()) {
                    // while dropping a table all the mutations should be local
                    String msg = "Found unexpected mutations while dropping table "
                            + SchemaUtil.getTableName(schemaName, tableOrViewName);
                    LOGGER.error(msg);
                    for (Mutation m : remoteMutations) {
                        LOGGER.debug("Mutation rowkey : " + Bytes.toStringBinary(m.getRow()));
                        LOGGER.debug("Mutation family cell map : " + m.getFamilyCellMap());
                    }
                    throw new IllegalStateException(msg);
                }

                // drop rows from catalog on this region
                mutateRowsWithLocks(this.accessCheckEnabled, region, localMutations,
                        Collections.<byte[]>emptySet(), HConstants.NO_NONCE, HConstants.NO_NONCE);

                long currentTime = MetaDataUtil.getClientTimeStamp(tableMetadata);
                for (ImmutableBytesPtr ckey : invalidateList) {
                    PTable table = newDeletedTableMarker(currentTime);
                    metaDataCache.put(ckey, table);
                    metricsSource.incrementMetadataCacheAddCount();
                    metricsSource.incrementMetadataCacheUsedSize(table.getEstimatedSize());
                }
                if (parentLockKey != null) {
                    ImmutableBytesPtr parentCacheKey = new ImmutableBytesPtr(parentLockKey);
                    metaDataCache.invalidate(parentCacheKey);
                }

                // after the view metadata is dropped, drop parent->child link
                MetaDataResponse response = processRemoteRegionMutations(
                        getSystemTableForChildLinks(request.getClientVersion(),
                                env.getConfiguration()).getName(), childLinkMutations,
                        MetaDataProtos.MutationCode.UNABLE_TO_DELETE_CHILD_LINK);
                if (response != null) {
                    done.run(response);
                    return;
                }

                done.run(MetaDataMutationResult.toProto(result));
                dropTableStats = true;

                updateDropTableDdlSuccessMetrics(pTableType);
                LOGGER.info("{} dropped successfully, tableName: {}", pTableType, fullTableName);
            } finally {
                ServerUtil.releaseRowLocks(locks);
                if (dropTableStats) {
                    Thread statsDeleteHandler = new Thread(new StatsDeleteHandler(env,
                        loadedTable, tableNamesToDelete, sharedTablesToDelete),
                        "thread-statsdeletehandler");
                    statsDeleteHandler.setDaemon(true);
                    statsDeleteHandler.start();
                }
            }
        } catch (Throwable t) {
          LOGGER.error("dropTable failed", t);
          ProtobufUtil.setControllerException(controller, ClientUtil.createIOException(
                  SchemaUtil.getTableName(schemaName, tableOrViewName), t));
        }
    }

    private void updateDropTableDdlSuccessMetrics(PTableType pTableType) {
        if (pTableType == PTableType.TABLE || pTableType == PTableType.SYSTEM) {
            metricsSource.incrementDropTableCount();
        } else if (pTableType == PTableType.VIEW) {
            metricsSource.incrementDropViewCount();
        } else if (pTableType == PTableType.INDEX) {
            metricsSource.incrementDropIndexCount();
        }
    }

    private static class StatsDeleteHandler implements Runnable {
        PTable deletedTable;
        List<byte[]> physicalTableNames;
        List<MetaDataProtocol.SharedTableState> sharedTableStates;
        RegionCoprocessorEnvironment env;

        StatsDeleteHandler(RegionCoprocessorEnvironment env, PTable deletedTable, List<byte[]> physicalTableNames,
                          List<MetaDataProtocol.SharedTableState> sharedTableStates) {
            this.deletedTable = deletedTable;
            this.physicalTableNames = physicalTableNames;
            this.sharedTableStates = sharedTableStates;
            this.env = env;
        }

        @Override
        public void run() {
            try {
                User.runAsLoginUser(new PrivilegedExceptionAction<Object>() {
                    @Override
                    public Object run() throws Exception {
                        try (PhoenixConnection connection =
                                     QueryUtil.getConnectionOnServer(env.getConfiguration())
                                             .unwrap(PhoenixConnection.class)) {
                            try {
                                MetaDataUtil.deleteFromStatsTable(connection, deletedTable,
                                        physicalTableNames, sharedTableStates);
                                LOGGER.info("Table stats deleted successfully, tablename is {}."
                                    , deletedTable.getPhysicalName().getString());
                            } catch(Throwable t) {
                                LOGGER.warn("Exception while deleting stats of table "
                                        + deletedTable.getPhysicalName().getString()
                                        + " please check and delete stats manually");
                            }
                        }
                        return null;
                    }
                });
            } catch (IOException e) {
                LOGGER.warn("Exception while deleting stats of table "
                        + deletedTable.getPhysicalName().getString()
                        + " please check and delete stats manually");
            }
        }
    }

    protected RowLock acquireLock(Region region, byte[] lockKey, List<RowLock> locks, boolean readLock) throws IOException {
        RowLock rowLock = region.getRowLock(lockKey, this.getMetadataReadLockEnabled && readLock);
        if (rowLock == null) {
            throw new IOException("Failed to acquire lock on " + Bytes.toStringBinary(lockKey));
        }
        if (locks != null) {
            locks.add(rowLock);
        }
        return rowLock;
    }

    private MetaDataResponse processRemoteRegionMutations(byte[] systemTableName,
                                                          List<Mutation> remoteMutations, MetaDataProtos.MutationCode mutationCode) throws IOException {
        if (remoteMutations.isEmpty()) {
            return null;
        }
        MetaDataResponse.Builder builder = MetaDataResponse.newBuilder();
        try (Table hTable =
                ServerUtil.getHTableForCoprocessorScan(env,
                    SchemaUtil.getPhysicalTableName(systemTableName, env.getConfiguration()))) {
            hTable.batch(remoteMutations, null);
        } catch (Throwable t) {
            LOGGER.error("Unable to write mutations to " + Bytes.toString(systemTableName), t);
            builder.setReturnCode(mutationCode);
            builder.setMutationTime(EnvironmentEdgeManager.currentTimeMillis());
            return builder.build();
        }
        return null;
    }

    private MetaDataMutationResult doDropTable(byte[] key, byte[] tenantId, byte[] schemaName,
            byte[] tableName, byte[] parentTableName, PTableType tableType,
            List<Mutation> catalogMutations, List<Mutation> childLinkMutations,
            List<ImmutableBytesPtr> invalidateList, List<byte[]> tableNamesToDelete,
            List<SharedTableState> sharedTablesToDelete, int clientVersion)
            throws IOException, SQLException {

        Region region = env.getRegion();
        long clientTimeStamp = MetaDataUtil.getClientTimeStamp(catalogMutations);
        ImmutableBytesPtr cacheKey = new ImmutableBytesPtr(key);

        PTable table = getTableFromCache(cacheKey, clientTimeStamp, clientVersion);

        // We always cache the latest version - fault in if not in cache
        if (table != null || (table = buildTable(key, cacheKey, region, HConstants.LATEST_TIMESTAMP,
                clientVersion)) != null) {
            if (table.getTimeStamp() < clientTimeStamp) {
                if (isTableDeleted(table) || tableType != table.getType()) {
                    return new MetaDataMutationResult(MutationCode.TABLE_NOT_FOUND,
                            EnvironmentEdgeManager.currentTimeMillis(), null);
                }
            } else {
                return new MetaDataMutationResult(MutationCode.NEWER_TABLE_FOUND,
                        EnvironmentEdgeManager.currentTimeMillis(), null);
            }
        }
        // We didn't find a table at the latest timestamp, so either there is no table or
        // there was a table, but it's been deleted. In either case we want to return.
        if (table == null) {
            if (buildDeletedTable(key, cacheKey, region, clientTimeStamp) != null) {
                return new MetaDataMutationResult(MutationCode.NEWER_TABLE_FOUND,
                        EnvironmentEdgeManager.currentTimeMillis(), null);
            }
            return new MetaDataMutationResult(MutationCode.TABLE_NOT_FOUND,
                    EnvironmentEdgeManager.currentTimeMillis(), null);
        }
        // Make sure we're not deleting the "wrong" child
        if (parentTableName != null && table.getParentTableName() != null &&
                !Arrays.equals(parentTableName, table.getParentTableName().getBytes())) {
            return new MetaDataMutationResult(MutationCode.TABLE_NOT_FOUND,
                    EnvironmentEdgeManager.currentTimeMillis(), null);
        }
        // Since we don't allow back in time DDL, we know if we have a table it's the one
        // we want to delete. FIXME: we shouldn't need a scan here, but should be able to
        // use the table to generate the Delete markers.
        Scan scan = MetaDataUtil.newTableRowsScan(key, MIN_TABLE_TIMESTAMP, clientTimeStamp);
        List<byte[]> indexNames = Lists.newArrayList();
        List<Cell> results = Lists.newArrayList();
        try (RegionScanner scanner = region.getScanner(scan);) {
            scanner.next(results);
            if (results.isEmpty()) { // Should not be possible
                return new MetaDataMutationResult(MutationCode.TABLE_NOT_FOUND,
                        EnvironmentEdgeManager.currentTimeMillis(), null);
            }

            // Add to list of HTables to delete, unless it's a view or its a shared index
            if (tableType == INDEX && table.getViewIndexId() != null) {
                sharedTablesToDelete.add(new SharedTableState(table));
            } else if (tableType != PTableType.VIEW) {
                tableNamesToDelete.add(table.getPhysicalName().getBytes());
            }
            invalidateList.add(cacheKey);
            byte[][] rowKeyMetaData = new byte[5][];
            do {
                Cell kv = results.get(LINK_TYPE_INDEX);
                int nColumns = getVarChars(kv.getRowArray(), kv.getRowOffset(), kv.getRowLength(),
                        0, rowKeyMetaData);
                if (nColumns == 5
                        && rowKeyMetaData[PhoenixDatabaseMetaData.FAMILY_NAME_INDEX].length > 0
                        && Bytes.compareTo(kv.getQualifierArray(), kv.getQualifierOffset(),
                        kv.getQualifierLength(),
                        LINK_TYPE_BYTES, 0, LINK_TYPE_BYTES.length) == 0) {
                    LinkType linkType = LinkType.fromSerializedValue(
                            kv.getValueArray()[kv.getValueOffset()]);
                    if (rowKeyMetaData[PhoenixDatabaseMetaData.COLUMN_NAME_INDEX].length == 0 &&
                            linkType == LinkType.INDEX_TABLE) {
                        indexNames.add(rowKeyMetaData[PhoenixDatabaseMetaData.FAMILY_NAME_INDEX]);
                    } else if (tableType == PTableType.VIEW && (linkType == LinkType.PARENT_TABLE ||
                            linkType == LinkType.PHYSICAL_TABLE)) {
                        // Populate the delete mutations for parent->child link for the child view
                        // in question, which we issue to SYSTEM.CHILD_LINK later
                        Cell parentTenantIdCell = MetaDataUtil.getCell(results,
                                PhoenixDatabaseMetaData.PARENT_TENANT_ID_BYTES);
                        PName parentTenantId = parentTenantIdCell != null ?
                                PNameFactory.newName(parentTenantIdCell.getValueArray(),
                                        parentTenantIdCell.getValueOffset(),
                                        parentTenantIdCell.getValueLength()) : null;
                        byte[] linkKey = MetaDataUtil.getChildLinkKey(parentTenantId,
                                table.getParentSchemaName(), table.getParentTableName(),
                                table.getTenantId(), table.getName());
                        Delete linkDelete = new Delete(linkKey, clientTimeStamp);
                        childLinkMutations.add(linkDelete);
                    }
                }
                Delete delete = new Delete(kv.getRowArray(), kv.getRowOffset(), kv.getRowLength(),
                        clientTimeStamp);
                catalogMutations.add(delete);
                results.clear();
                scanner.next(results);
            } while (!results.isEmpty());
        }

        // Recursively delete indexes
        for (byte[] indexName : indexNames) {
            byte[] indexKey = SchemaUtil.getTableKey(tenantId, schemaName, indexName);
            // FIXME: Remove when unintentionally deprecated method is fixed (HBASE-7870).
            // FIXME: the version of the Delete constructor without the lock args was introduced
            // in 0.94.4, thus if we try to use it here we can no longer use the 0.94.2 version
            // of the client.
            Delete delete = new Delete(indexKey, clientTimeStamp);
            catalogMutations.add(delete);
            MetaDataMutationResult result = doDropTable(indexKey, tenantId, schemaName, indexName,
                    tableName, PTableType.INDEX, catalogMutations, childLinkMutations,
                    invalidateList, tableNamesToDelete, sharedTablesToDelete, clientVersion);
            if (result.getMutationCode() != MutationCode.TABLE_ALREADY_EXISTS) {
                return result;
            }
        }

        if (clientVersion < MIN_SPLITTABLE_SYSTEM_CATALOG && tableType == PTableType.VIEW) {
            try (PhoenixConnection connection = QueryUtil.getConnectionOnServer(
                    env.getConfiguration()).unwrap(PhoenixConnection.class)) {
                PTable pTable = connection.getTableNoCache(table.getParentName().getString());
                table = ViewUtil.addDerivedColumnsAndIndexesFromParent(connection, table, pTable);
            }
        }
        return new MetaDataMutationResult(MutationCode.TABLE_ALREADY_EXISTS,
                EnvironmentEdgeManager.currentTimeMillis(), table, tableNamesToDelete,
                sharedTablesToDelete);
    }

    /**
     * Validate if mutation is allowed on a parent table/view based on their child views.
     * If this method returns MetaDataMutationResult, mutation is not allowed, and returned object
     * will contain returnCode (MutationCode) to indicate the underlying problem
     * (validation failure code).
     *
     * @param expectedType expected type of PTable
     * @param clientTimeStamp client timestamp, e.g check
     * {@link MetaDataUtil#getClientTimeStamp(List)}
     * @param tenantId tenant Id
     * @param schemaName schema name
     * @param tableOrViewName table or view name
     * @param childViews child views of table or parent view. Usually this is an empty list
     *     passed to this method, and this method will add child views retrieved using
     *     {@link ViewUtil#findAllDescendantViews(Table, Configuration, byte[], byte[], byte[],
     *     long, boolean)}
     * @param clientVersion client version, used to determine if mutation is allowed.
     * @return Optional.empty() if mutation is allowed on parent table/view. If not allowed,
     *     returned Optional object will contain metaDataMutationResult with MutationCode.
     * @throws IOException if something goes wrong while retrieving child views using
     *     {@link ViewUtil#findAllDescendantViews(Table, Configuration, byte[], byte[], byte[],
     *     long, boolean)}
     * @throws SQLException if something goes wrong while retrieving child views using
     *     {@link ViewUtil#findAllDescendantViews(Table, Configuration, byte[], byte[], byte[],
     *     long, boolean)}
     */
    private Optional<MetaDataMutationResult> validateIfMutationAllowedOnParent(
            final PTable parentTable,
            final List<Mutation> tableMetadata,
            final PTableType expectedType, final long clientTimeStamp,
            final byte[] tenantId, final byte[] schemaName,
            final byte[] tableOrViewName, final List<PTable> childViews,
            final int clientVersion) throws IOException, SQLException {
        boolean isMutationAllowed = true;
        boolean isSchemaMutationAllowed = true;
        if (expectedType == PTableType.TABLE || expectedType == PTableType.VIEW) {
            try (Table hTable = ServerUtil.getHTableForCoprocessorScan(env,
                    getSystemTableForChildLinks(clientVersion, env.getConfiguration()))) {
                childViews.addAll(findAllDescendantViews(hTable, env.getConfiguration(),
                        tenantId, schemaName, tableOrViewName, clientTimeStamp, false)
                        .getFirst());
            }

            if (!childViews.isEmpty()) {
                // From 4.15 onwards we allow SYSTEM.CATALOG to split and no longer
                // propagate parent metadata changes to child views.
                // If the client is on a version older than 4.15 we have to block adding a
                // column to a parent as we no longer lock the parent on the
                // server side while creating a child view to prevent conflicting changes.
                // This is handled on the client side from 4.15 onwards.
                // Also if QueryServices.ALLOW_SPLITTABLE_SYSTEM_CATALOG_ROLLBACK is true,
                // we block adding a column to a parent so that we can rollback the
                // upgrade if required.
                if (clientVersion < MIN_SPLITTABLE_SYSTEM_CATALOG) {
                    isMutationAllowed = false;
                    LOGGER.error("Unable to add or drop a column as the client is older than {}",
                            MIN_SPLITTABLE_SYSTEM_CATALOG_VERSION);
                } else if (allowSplittableSystemCatalogRollback) {
                    isMutationAllowed = false;
                    LOGGER.error("Unable to add or drop a column as the {} config is set to true",
                        QueryServices.ALLOW_SPLITTABLE_SYSTEM_CATALOG_ROLLBACK);
                }
            }
            // Validate PHOENIX_TTL property settings for views only.
            if (expectedType == PTableType.VIEW) {
                isSchemaMutationAllowed = validatePhoenixTTLAttributeSettingForView(
                        parentTable, childViews, tableMetadata, PHOENIX_TTL_BYTES);
            }
        }
        if (!isMutationAllowed) {
            MetaDataMutationResult metaDataMutationResult =
                new MetaDataMutationResult(
                    MetaDataProtocol.MutationCode.UNALLOWED_TABLE_MUTATION,
                    EnvironmentEdgeManager.currentTimeMillis(), null);
            return Optional.of(metaDataMutationResult);
        }
        if (!isSchemaMutationAllowed) {
            MetaDataMutationResult metaDataMutationResult =
                    new MetaDataMutationResult(
                            MetaDataProtocol.MutationCode.UNALLOWED_SCHEMA_MUTATION,
                            EnvironmentEdgeManager.currentTimeMillis(), null);
            return Optional.of(metaDataMutationResult);
        }
        return Optional.empty();
    }

    private MetaDataMutationResult mutateColumn(
            final List<Mutation> tableMetadata,
            final ColumnMutator mutator, final int clientVersion,
            final PTable parentTable, final PTable transformingNewTable, boolean isAddingOrDroppingColumns) throws IOException {
        byte[][] rowKeyMetaData = new byte[5][];
        MetaDataUtil.getTenantIdAndSchemaAndTableName(tableMetadata, rowKeyMetaData);
        byte[] tenantId = rowKeyMetaData[PhoenixDatabaseMetaData.TENANT_ID_INDEX];
        byte[] schemaName = rowKeyMetaData[PhoenixDatabaseMetaData.SCHEMA_NAME_INDEX];
        byte[] tableOrViewName = rowKeyMetaData[PhoenixDatabaseMetaData.TABLE_NAME_INDEX];
        byte[] key = SchemaUtil.getTableKey(tenantId, schemaName, tableOrViewName);
        String fullTableName = SchemaUtil.getTableName(schemaName, tableOrViewName);
        // server-side, except for indexing, we always expect the keyvalues to be standard KeyValues
        PTableType expectedType = MetaDataUtil.getTableType(tableMetadata,
                GenericKeyValueBuilder.INSTANCE, new ImmutableBytesWritable());
        List<byte[]> tableNamesToDelete = Lists.newArrayList();
        List<SharedTableState> sharedTablesToDelete = Lists.newArrayList();
        long clientTimeStamp = MetaDataUtil.getClientTimeStamp(tableMetadata);
        try {
            Region region = env.getRegion();
            MetaDataMutationResult result = checkTableKeyInRegion(key, region);
            if (result != null) {
                return result;
            }

            List<RowLock> locks = Lists.newArrayList();
            try {
                List<PTable> childViews = Lists.newArrayList();
                Optional<MetaDataMutationResult> mutationResult = validateIfMutationAllowedOnParent(
                        parentTable, tableMetadata,
                        expectedType, clientTimeStamp, tenantId, schemaName, tableOrViewName,
                        childViews, clientVersion);
                // only if mutation is allowed, we should get Optional.empty() here
                if (mutationResult.isPresent()) {
                    return mutationResult.get();
                }
                // We take a write row lock for tenantId, schemaName, tableOrViewName
                acquireLock(region, key, locks, false);
                ImmutableBytesPtr cacheKey = new ImmutableBytesPtr(key);
                List<ImmutableBytesPtr> invalidateList = new ArrayList<ImmutableBytesPtr>();
                invalidateList.add(cacheKey);
                PTable table = getTableFromCache(cacheKey, clientTimeStamp, clientVersion);
                if (failConcurrentMutateAddColumnOneTimeForTesting) {
                    failConcurrentMutateAddColumnOneTimeForTesting = false;
                    return new MetaDataMutationResult(MutationCode.CONCURRENT_TABLE_MUTATION,
                            EnvironmentEdgeManager.currentTimeMillis(), table);
                }
                if (LOGGER.isDebugEnabled()) {
                    if (table == null) {
                        LOGGER.debug("Table " + Bytes.toStringBinary(key)
                                + " not found in cache. Will build through scan");
                    } else {
                        LOGGER.debug("Table " + Bytes.toStringBinary(key)
                                + " found in cache with timestamp " + table.getTimeStamp()
                                + " seqNum " + table.getSequenceNumber());
                    }
                }
                // Get client timeStamp from mutations
                if (table == null && (table = buildTable(key, cacheKey, region,
                        HConstants.LATEST_TIMESTAMP, clientVersion)) == null) {
                    // if not found then call newerTableExists and add delete marker for timestamp
                    // found
                    table = buildDeletedTable(key, cacheKey, region, clientTimeStamp);
                    if (table != null) {
                        LOGGER.info("Found newer table deleted as of " + table.getTimeStamp()
                                + " versus client timestamp of " + clientTimeStamp);
                        return new MetaDataMutationResult(MutationCode.NEWER_TABLE_FOUND,
                                EnvironmentEdgeManager.currentTimeMillis(), null);
                    }
                    return new MetaDataMutationResult(MutationCode.TABLE_NOT_FOUND,
                            EnvironmentEdgeManager.currentTimeMillis(), null);
                }

                // if this is a view or view index then we need to include columns and
                // indexes derived from its ancestors
                if (parentTable != null) {
                    Properties props = new Properties();
                    if (tenantId != null) {
                        props.setProperty(TENANT_ID_ATTRIB, Bytes.toString(tenantId));
                    }
                    if (clientTimeStamp != HConstants.LATEST_TIMESTAMP) {
                        props.setProperty("CurrentSCN", Long.toString(clientTimeStamp));
                    }
                    try (PhoenixConnection connection =
                                 QueryUtil.getConnectionOnServer(props, env.getConfiguration())
                                         .unwrap(PhoenixConnection.class)) {
                        table = ViewUtil.addDerivedColumnsAndIndexesFromParent(connection, table,
                                parentTable);
                    }
                }
                if (transformingNewTable !=null) {
                    table = PTableImpl.builderWithColumns(table, getColumnsToClone(table))
                            .setTransformingNewTable(transformingNewTable).build();
                }

                if (table.getTimeStamp() >= clientTimeStamp) {
                    LOGGER.info("Found newer table as of " + table.getTimeStamp()
                            + " versus client timestamp of " + clientTimeStamp);
                    return new MetaDataMutationResult(MutationCode.NEWER_TABLE_FOUND,
                            EnvironmentEdgeManager.currentTimeMillis(), table);
                } else if (isTableDeleted(table)) {
                    return new MetaDataMutationResult(MutationCode.TABLE_NOT_FOUND,
                            EnvironmentEdgeManager.currentTimeMillis(), null);
                }
                // lookup TABLE_SEQ_NUM in tableMetaData
                long expectedSeqNum = MetaDataUtil.getSequenceNumber(tableMetadata) - 1;

                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("For table " + Bytes.toStringBinary(key) + " expecting seqNum "
                            + expectedSeqNum + " and found seqNum " + table.getSequenceNumber()
                            + " with " + table.getColumns().size() + " columns: "
                            + table.getColumns());
                }
                if (expectedSeqNum != table.getSequenceNumber()) {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("For table " + Bytes.toStringBinary(key)
                                + " returning CONCURRENT_TABLE_MUTATION due to unexpected seqNum");
                    }
                    return new MetaDataMutationResult(MutationCode.CONCURRENT_TABLE_MUTATION,
                            EnvironmentEdgeManager.currentTimeMillis(), table);
                }

                PTableType type = table.getType();
                if (type == PTableType.INDEX) {
                    // Disallow mutation of an index table
                    return new MetaDataMutationResult(MutationCode.UNALLOWED_TABLE_MUTATION,
                            EnvironmentEdgeManager.currentTimeMillis(), null);
                } else {
                    // We said to drop a table, but found a view or visa versa
                    if (type != expectedType) {
                        return new MetaDataProtocol.MetaDataMutationResult(MutationCode.TABLE_NOT_FOUND,
                                EnvironmentEdgeManager.currentTimeMillis(), null);
                    }
                }

                if (!childViews.isEmpty()) {
                    // validate the add or drop column mutations
                    result = mutator.validateWithChildViews(table, childViews, tableMetadata,
                            schemaName, tableOrViewName);
                    if (result != null) {
                        return result;
                    }
                }

                getCoprocessorHost().preAlterTable(Bytes.toString(tenantId),
                        SchemaUtil.getTableName(schemaName, tableOrViewName),
                        TableName.valueOf(table.getPhysicalName().getBytes()),
                        getParentPhysicalTableName(table), table.getType());

                result = mutator.validateAndAddMetadata(table, rowKeyMetaData, tableMetadata,
                        region, invalidateList, locks, clientTimeStamp, clientVersion,
                    ((ExtendedCellBuilder) env.getCellBuilder()), isAddingOrDroppingColumns);
                // if the update mutation caused tables to be deleted, the mutation code returned
                // will be MutationCode.TABLE_ALREADY_EXISTS
                if (result != null
                        && result.getMutationCode() != MutationCode.TABLE_ALREADY_EXISTS) {
                    return result;
                }

                // drop any indexes on the base table that need the column that is going to be
                // dropped
                List<Pair<PTable, PColumn>> tableAndDroppedColumnPairs =
                        mutator.getTableAndDroppedColumnPairs();
                Iterator<Pair<PTable, PColumn>> iterator = tableAndDroppedColumnPairs.iterator();
                while (iterator.hasNext()) {
                    Pair<PTable, PColumn> pair = iterator.next();
                    // remove the current table and column being dropped from the list and drop any
                    // indexes that require the column being dropped while holding the row lock
                    if (table.equals(pair.getFirst())) {
                        iterator.remove();
                        result = dropIndexes(env, pair.getFirst(), invalidateList, locks,
                                clientTimeStamp, tableMetadata, pair.getSecond(),
                                tableNamesToDelete, sharedTablesToDelete, clientVersion);
                        if (result != null
                                && result.getMutationCode() != MutationCode.TABLE_ALREADY_EXISTS) {
                            return result;
                        }
                    }
                }

                if (table.isChangeDetectionEnabled() || MetaDataUtil.getChangeDetectionEnabled(tableMetadata)) {
                    long startTime = EnvironmentEdgeManager.currentTimeMillis();
                    try {
                        exportSchema(tableMetadata, key, clientTimeStamp, clientVersion, table);
                        metricsSource.incrementAlterExportCount();
                        metricsSource.updateAlterExportTime(EnvironmentEdgeManager.currentTimeMillis() - startTime);
                    } catch (Exception e) {
                        LOGGER.error("Error writing to schema registry", e);
                        metricsSource.incrementAlterExportFailureCount();
                        metricsSource.updateAlterExportFailureTime(EnvironmentEdgeManager.currentTimeMillis() - startTime);
                        result = new MetaDataMutationResult(MutationCode.ERROR_WRITING_TO_SCHEMA_REGISTRY,
                            EnvironmentEdgeManager.currentTimeMillis(), table);
                        return result;
                    }
                }
                Cache<ImmutableBytesPtr, PMetaDataEntity> metaDataCache =
                        GlobalCache.getInstance(this.env).getMetaDataCache();

                // The mutations to add a column are written in the following order:
                // 1. Update the encoded column qualifier for the parent table if its on a
                // different region server (for tables that use column qualifier encoding)
                // if the next step fails we end up wasting a few col qualifiers
                // 2. Write the mutations to add the column

                List<Mutation> localMutations =
                        Lists.newArrayListWithExpectedSize(tableMetadata.size());
                List<Mutation> remoteMutations = Lists.newArrayList();
                separateLocalAndRemoteMutations(region, tableMetadata, localMutations,
                        remoteMutations);
                if (!remoteMutations.isEmpty()) {
                    // there should only be remote mutations if we are adding a column to a view
                    // that uses encoded column qualifiers (the remote mutations are to update the
                    // encoded column qualifier counter on the parent table)
                    if (( mutator.getMutateColumnType() == ColumnMutator.MutateColumnType.ADD_COLUMN
                            && type == PTableType.VIEW
                            && table.getEncodingScheme() !=
                            QualifierEncodingScheme.NON_ENCODED_QUALIFIERS)) {
                        processRemoteRegionMutations(
                                PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES, remoteMutations,
                                MetaDataProtos.MutationCode.UNABLE_TO_UPDATE_PARENT_TABLE);
                        //if we're a view or index, clear the cache for our parent
                        if ((type == PTableType.VIEW || type == INDEX) && table.getParentTableName() != null) {
                            clearRemoteTableFromCache(clientTimeStamp,
                                table.getParentSchemaName() != null
                                    ? table.getParentSchemaName().getBytes()
                                    : ByteUtil.EMPTY_BYTE_ARRAY,
                                table.getParentTableName().getBytes());
                        }
                    } else {
                        String msg = "Found unexpected mutations while adding or dropping column "
                                + "to " + fullTableName;
                        LOGGER.error(msg);
                        for (Mutation m : remoteMutations) {
                            LOGGER.debug("Mutation rowkey : " + Bytes.toStringBinary(m.getRow()));
                            LOGGER.debug("Mutation family cell map : " + m.getFamilyCellMap());
                        }
                        throw new IllegalStateException(msg);
                    }
                }
                mutateRowsWithLocks(this.accessCheckEnabled, region, localMutations,
                        Collections.<byte[]>emptySet(), HConstants.NO_NONCE, HConstants.NO_NONCE);
                // Invalidate from cache
                for (ImmutableBytesPtr invalidateKey : invalidateList) {
                    metaDataCache.invalidate(invalidateKey);
                }
                // Get client timeStamp from mutations, since it may get updated by the
                // mutateRowsWithLocks call
                long currentTime = MetaDataUtil.getClientTimeStamp(tableMetadata);
                // if the update mutation caused tables to be deleted just return the result which
                // will contain the table to be deleted
                if (result != null
                        && result.getMutationCode() != MutationCode.TABLE_ALREADY_EXISTS) {
                    return result;
                } else {
                    table = buildTable(key, cacheKey, region, HConstants.LATEST_TIMESTAMP,
                            clientVersion);
                    if (clientVersion < MIN_SPLITTABLE_SYSTEM_CATALOG && type == PTableType.VIEW) {
                        try (PhoenixConnection connection = QueryUtil.getConnectionOnServer(
                                env.getConfiguration()).unwrap(PhoenixConnection.class)) {
                            PTable pTable = connection.getTableNoCache(
                                    table.getParentName().getString());
                            table = ViewUtil.addDerivedColumnsAndIndexesFromParent(connection,
                                    table, pTable);
                        }
                    }
                    return new MetaDataMutationResult(MutationCode.TABLE_ALREADY_EXISTS,
                            currentTime, table, tableNamesToDelete, sharedTablesToDelete);
                }
            } finally {
                ServerUtil.releaseRowLocks(locks);
                // drop indexes on views that require the column being dropped. These could be on a
                // different region server so don't hold row locks while dropping them
                for (Pair<PTable, PColumn> pair : mutator.getTableAndDroppedColumnPairs()) {
                    result = dropRemoteIndexes(env, pair.getFirst(), clientTimeStamp,
                            pair.getSecond(), tableNamesToDelete, sharedTablesToDelete);
                    if (result != null
                            && result.getMutationCode() != MutationCode.TABLE_ALREADY_EXISTS) {
                        return result;
                    }
                }
            }
        } catch (Throwable t) {
            ClientUtil.throwIOException(fullTableName, t);
            return null; // impossible
        }
    }

    /**
     * Removes the table from the server side cache
     */
    private void clearRemoteTableFromCache(long clientTimeStamp, byte[] schemaName, byte[] tableName) throws SQLException {
        // remove the parent table from the metadata cache as we just mutated the table
        Properties props = new Properties();
        if (clientTimeStamp != HConstants.LATEST_TIMESTAMP) {
            props.setProperty("CurrentSCN", Long.toString(clientTimeStamp));
        }
        try (PhoenixConnection connection =
                     QueryUtil.getConnectionOnServer(props, env.getConfiguration())
                             .unwrap(PhoenixConnection.class)) {
            ConnectionQueryServices queryServices = connection.getQueryServices();
            queryServices.clearTableFromCache(ByteUtil.EMPTY_BYTE_ARRAY, schemaName, tableName,
                    clientTimeStamp);
        }
    }

    // Checks whether a non-zero PHOENIX_TTL value is being set.
    private boolean validatePhoenixTTLAttributeSettingForView(
            final PTable parentTable,
            final List<PTable> childViews,
            final List<Mutation> tableMetadata,
            final byte[] phoenixTtlBytes) {
        boolean hasNewPhoenixTTLAttribute = false;
        boolean isSchemaMutationAllowed = true;
        for (Mutation m : tableMetadata) {
            if (m instanceof Put) {
                Put p = (Put)m;
                List<Cell> cells = p.get(TABLE_FAMILY_BYTES, phoenixTtlBytes);
                if (cells != null && cells.size() > 0) {
                    Cell cell = cells.get(0);
                    long newPhoenixTTL = (long) PLong.INSTANCE.toObject(cell.getValueArray(),
                            cell.getValueOffset(), cell.getValueLength());
                    hasNewPhoenixTTLAttribute =  newPhoenixTTL != PHOENIX_TTL_NOT_DEFINED ;
                }
            }
        }

        if (hasNewPhoenixTTLAttribute) {
            // Disallow if the parent has PHOENIX_TTL set.
            if (parentTable != null &&  parentTable.getPhoenixTTL() != PHOENIX_TTL_NOT_DEFINED) {
                isSchemaMutationAllowed = false;
            }

            // Since we do not allow propagation of PHOENIX_TTL values during ALTER for now.
            // If a child view exists and this view previously had a PHOENIX_TTL value set
            // then that implies that the child view too has a valid PHOENIX_TTL (non zero).
            // In this case we do not allow for ALTER of the view's PHOENIX_TTL value.
            if (!childViews.isEmpty()) {
                isSchemaMutationAllowed = false;
            }
        }
        return isSchemaMutationAllowed;


    }

    public static class ColumnFinder extends StatelessTraverseAllExpressionVisitor<Void> {
        private boolean columnFound;
        private final Expression columnExpression;

        public ColumnFinder(Expression columnExpression) {
            this.columnExpression = columnExpression;
            columnFound = false;
        }

        private Void process(Expression expression) {
            if (expression.equals(columnExpression)) {
                columnFound = true;
            }
            return null;
        }

        @Override
        public Void visit(KeyValueColumnExpression expression) {
            return process(expression);
        }

        @Override
        public Void visit(RowKeyColumnExpression expression) {
            return process(expression);
        }

        @Override
        public Void visit(ProjectedColumnExpression expression) {
            return process(expression);
        }

        public boolean getColumnFound() {
            return columnFound;
        }
    }

    @Override
    public void addColumn(RpcController controller, final AddColumnRequest request,
                          RpcCallback<MetaDataResponse> done) {
        try {
            List<Mutation> tableMetaData = ProtobufUtil.getMutations(request);
            PTable parentTable = request.hasParentTable() ? PTableImpl.createFromProto(request.getParentTable()) : null;
            PTable transformingNewTable = request.hasTransformingNewTable() ? PTableImpl.createFromProto(request.getTransformingNewTable()) : null;
            boolean addingColumns = request.getAddingColumns();
            MetaDataMutationResult result = mutateColumn(tableMetaData, new AddColumnMutator(),
                    request.getClientVersion(), parentTable, transformingNewTable, addingColumns);
            if (result != null) {
                done.run(MetaDataMutationResult.toProto(result));

                if (result.getMutationCode() == MutationCode.TABLE_ALREADY_EXISTS) {
                    metricsSource.incrementAlterAddColumnCount();
                    LOGGER.info("Column(s) added successfully, tableName: {}",
                            result.getTable().getTableName());
                }
            }
        } catch (Throwable e) {
            LOGGER.error("Add column failed: ", e);
            ProtobufUtil.setControllerException(controller,
                    ClientUtil.createIOException("Error when adding column: ", e));
        }
    }

    private PTable doGetTable(byte[] tenantId, byte[] schemaName, byte[] tableName,
                              long clientTimeStamp, int clientVersion) throws IOException, SQLException {
        return doGetTable(tenantId, schemaName, tableName, clientTimeStamp, null, clientVersion);
    }

    /**
     * Looks up the table locally if its present on this region.
     */
    private PTable doGetTable(byte[] tenantId, byte[] schemaName, byte[] tableName,
                              long clientTimeStamp, RowLock rowLock, int clientVersion) throws IOException, SQLException {
        Region region = env.getRegion();
        final byte[] key = SchemaUtil.getTableKey(tenantId, schemaName, tableName);
        // if this region doesn't contain the metadata rows then fail
        if (!region.getRegionInfo().containsRow(key)) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.GET_TABLE_ERROR)
                    .setSchemaName(Bytes.toString(schemaName))
                    .setTableName(Bytes.toString(tableName)).build().buildException();
        }

        ImmutableBytesPtr cacheKey = new ImmutableBytesPtr(key);
        // Ask Lars about the expense of this call - if we don't take the lock, we still won't get
        // partial results
        // get the co-processor environment
        // TODO: check that key is within region.getStartKey() and region.getEndKey()
        // and return special code to force client to lookup region from meta.
        /*
         * Lock directly on key, though it may be an index table. This will just prevent a table
         * from getting rebuilt too often.
         */
        final boolean wasLocked = (rowLock != null);
        try {
            if (!wasLocked) {
                rowLock = acquireLock(region, key, null, true);
            }
            PTable table =
                getTableFromCacheWithModifiedIndexState(clientTimeStamp, clientVersion, cacheKey);
            // We only cache the latest, so we'll end up building the table with every call if the
            // client connection has specified an SCN.
            // TODO: If we indicate to the client that we're returning an older version, but there's a
            // newer version available, the client
            // can safely not call this, since we only allow modifications to the latest.
            if (table != null && table.getTimeStamp() < clientTimeStamp) {
                // Table on client is up-to-date with table on server, so just return
                if (isTableDeleted(table)) {
                    return null;
                }
                return table;
            }
            // take Phoenix row level write-lock as we need to protect metadata cache update
            // after scanning SYSTEM.CATALOG to retrieve the PTable object
            LockManager.RowLock phoenixRowLock =
                lockManager.lockRow(key, this.metadataCacheRowLockTimeout);
            try {
                table = getTableFromCacheWithModifiedIndexState(clientTimeStamp, clientVersion,
                    cacheKey);
                if (table != null && table.getTimeStamp() < clientTimeStamp) {
                    if (isTableDeleted(table)) {
                        return null;
                    }
                    return table;
                }
                // Query for the latest table first, since it's not cached
                table =
                    buildTable(key, cacheKey, region, HConstants.LATEST_TIMESTAMP, clientVersion);
                if ((table != null && table.getTimeStamp() <= clientTimeStamp) || (
                    blockWriteRebuildIndex && table.getIndexDisableTimestamp() > 0)) {
                    return table;
                }
                // Otherwise, query for an older version of the table - it won't be cached
                table = buildTable(key, cacheKey, region, clientTimeStamp, clientVersion);
                return table;
            } finally {
                phoenixRowLock.release();
            }
        } finally {
            if (!wasLocked && rowLock != null) rowLock.release();
        }
    }

    private PTable getTableFromCacheWithModifiedIndexState(long clientTimeStamp, int clientVersion,
        ImmutableBytesPtr cacheKey) throws SQLException {
        PTable table = getTableFromCache(cacheKey, clientTimeStamp, clientVersion);
        table = modifyIndexStateForOldClient(clientVersion, table);
        return table;
    }

    private List<PFunction> doGetFunctions(List<byte[]> keys, long clientTimeStamp) throws IOException, SQLException {
        Cache<ImmutableBytesPtr, PMetaDataEntity> metaDataCache =
                GlobalCache.getInstance(this.env).getMetaDataCache();
        Region region = env.getRegion();
        Collections.sort(keys, new Comparator<byte[]>() {
            @Override
            public int compare(byte[] o1, byte[] o2) {
                return Bytes.compareTo(o1, o2);
            }
        });
        /*
         * Lock directly on key, though it may be an index table. This will just prevent a table
         * from getting rebuilt too often.
         */
        List<RowLock> rowLocks = new ArrayList<RowLock>(keys.size());
        ;
        try {
            for (int i = 0; i < keys.size(); i++) {
                acquireLock(region, keys.get(i), rowLocks, true);
            }

            List<PFunction> functionsAvailable = new ArrayList<PFunction>(keys.size());
            int numFunctions = keys.size();
            Iterator<byte[]> iterator = keys.iterator();
            while (iterator.hasNext()) {
                byte[] key = iterator.next();
                PFunction function = (PFunction) metaDataCache.getIfPresent(new FunctionBytesPtr(key));
                if (function == null) {
                    metricsSource.incrementMetadataCacheMissCount();
                } else {
                    metricsSource.incrementMetadataCacheHitCount();
                }
                if (function != null && function.getTimeStamp() < clientTimeStamp) {
                    if (isFunctionDeleted(function)) {
                        return null;
                    }
                    functionsAvailable.add(function);
                    iterator.remove();
                }
            }
            if (functionsAvailable.size() == numFunctions) return functionsAvailable;

            // Query for the latest table first, since it's not cached
            List<PFunction> buildFunctions =
                    buildFunctions(keys, region, clientTimeStamp, false,
                            Collections.<Mutation>emptyList());
            if (buildFunctions == null || buildFunctions.isEmpty()) {
                return null;
            }
            functionsAvailable.addAll(buildFunctions);
            if (functionsAvailable.size() == numFunctions) return functionsAvailable;
            return null;
        } finally {
            ServerUtil.releaseRowLocks(rowLocks);
        }
    }

    @Override
    public void dropColumn(RpcController controller, final DropColumnRequest request,
                           RpcCallback<MetaDataResponse> done) {
        List<Mutation> tableMetaData = null;
        final List<byte[]> tableNamesToDelete = Lists.newArrayList();
        final List<SharedTableState> sharedTablesToDelete = Lists.newArrayList();
        try {
            tableMetaData = ProtobufUtil.getMutations(request);
            PTable parentTable = request.hasParentTable() ? PTableImpl.createFromProto(request.getParentTable()) : null;
            MetaDataMutationResult result = mutateColumn(tableMetaData, new DropColumnMutator(env.getConfiguration()),
                    request.getClientVersion(), parentTable,null, true);
            if (result != null) {
                done.run(MetaDataMutationResult.toProto(result));

                if (result.getMutationCode() == MutationCode.TABLE_ALREADY_EXISTS) {
                    metricsSource.incrementAlterDropColumnCount();
                    LOGGER.info("Column(s) dropped successfully, tableName: {}",
                            result.getTable().getTableName());
                }
            }
        } catch (Throwable e) {
            LOGGER.error("Drop column failed: ", e);
            ProtobufUtil.setControllerException(controller,
                    ClientUtil.createIOException("Error when dropping column: ", e));
        }
    }

    private MetaDataMutationResult dropIndexes(RegionCoprocessorEnvironment env, PTable table,
            List<ImmutableBytesPtr> invalidateList, List<RowLock> locks, long clientTimeStamp,
            List<Mutation> tableMetaData, PColumn columnToDelete, List<byte[]> tableNamesToDelete,
            List<SharedTableState> sharedTablesToDelete, int clientVersion)
            throws IOException, SQLException {
        // Look for columnToDelete in any indexes. If found as PK column, get lock and drop the
        // index and then invalidate it
        // Covered columns are deleted from the index by the client
        Region region = env.getRegion();
        PhoenixConnection connection =
                table.getIndexes().isEmpty() ? null : QueryUtil.getConnectionOnServer(
                        env.getConfiguration()).unwrap(PhoenixConnection.class);
        for (PTable index : table.getIndexes()) {
            // ignore any indexes derived from ancestors
            if (index.getName().getString().contains(
                    QueryConstants.CHILD_VIEW_INDEX_NAME_SEPARATOR)) {
                continue;
            }
            byte[] tenantId = index.getTenantId() == null ?
                    ByteUtil.EMPTY_BYTE_ARRAY : index.getTenantId().getBytes();
            IndexMaintainer indexMaintainer = index.getIndexMaintainer(table, connection);
            byte[] indexKey =
                    SchemaUtil.getTableKey(tenantId, index.getSchemaName().getBytes(), index
                            .getTableName().getBytes());
            Pair<String, String> columnToDeleteInfo =
                    new Pair<>(columnToDelete.getFamilyName().getString(),
                            columnToDelete.getName().getString());
            ColumnReference colDropRef = new ColumnReference(
                    columnToDelete.getFamilyName().getBytes(),
                    columnToDelete.getColumnQualifierBytes());
            boolean isColumnIndexed = indexMaintainer.getIndexedColumnInfo().contains(
                    columnToDeleteInfo);
            boolean isCoveredColumn = indexMaintainer.getCoveredColumns().contains(colDropRef);
            // If index requires this column for its pk, then drop it
            if (isColumnIndexed) {
                // Drop the index table. The doDropTable will expand
                // this to all of the table rows and invalidate the
                // index table
                Delete delete = new Delete(indexKey, clientTimeStamp);
                byte[] linkKey =
                        MetaDataUtil.getParentLinkKey(tenantId, table.getSchemaName().getBytes(),
                                table.getTableName().getBytes(), index.getTableName().getBytes());
                // Drop the link between the parent table and the
                // index table
                Delete linkDelete = new Delete(linkKey, clientTimeStamp);
                tableMetaData.add(delete);
                tableMetaData.add(linkDelete);
                // Since we're dropping the index, lock it to ensure
                // that a change in index state doesn't
                // occur while we're dropping it.
                acquireLock(region, indexKey, locks, false);
                List<Mutation> childLinksMutations = Lists.newArrayList();
                MetaDataMutationResult result = doDropTable(indexKey, tenantId,
                        index.getSchemaName().getBytes(), index.getTableName().getBytes(),
                        table.getName().getBytes(), index.getType(), tableMetaData,
                        childLinksMutations, invalidateList, tableNamesToDelete,
                        sharedTablesToDelete, clientVersion);
                if (result.getMutationCode() != MutationCode.TABLE_ALREADY_EXISTS) {
                    return result;
                }
                metricsSource.incrementDropIndexCount();
                LOGGER.info("INDEX dropped successfully, tableName: {}",
                        result.getTable().getTableName());

                // there should be no child links to delete since we are just dropping an index
                if (!childLinksMutations.isEmpty()) {
                    LOGGER.error("Found unexpected child link mutations while dropping an index "
                            + childLinksMutations);
                }
                invalidateList.add(new ImmutableBytesPtr(indexKey));
            }
            // If the dropped column is a covered index column, invalidate the index
            else if (isCoveredColumn) {
                invalidateList.add(new ImmutableBytesPtr(indexKey));
            }
        }
        if (connection != null) {
            connection.close();
        }
        return null;
    }

    private MetaDataMutationResult dropRemoteIndexes(RegionCoprocessorEnvironment env, PTable table,
                                                     long clientTimeStamp, PColumn columnToDelete,
                                                     List<byte[]> tableNamesToDelete,
                                                     List<SharedTableState> sharedTablesToDelete)
            throws SQLException {
        // Look for columnToDelete in any indexes. If found as PK column, get lock and drop the
        // index and then invalidate it
        // Covered columns are deleted from the index by the client
        PhoenixConnection connection =
                table.getIndexes().isEmpty() ? null : QueryUtil.getConnectionOnServer(
                        env.getConfiguration()).unwrap(PhoenixConnection.class);
        for (PTable index : table.getIndexes()) {
            byte[] tenantId = index.getTenantId() == null ? ByteUtil.EMPTY_BYTE_ARRAY : index.getTenantId().getBytes();
            IndexMaintainer indexMaintainer = index.getIndexMaintainer(table, connection);
            byte[] indexKey =
                    SchemaUtil.getTableKey(tenantId, index.getSchemaName().getBytes(), index
                            .getTableName().getBytes());
            Pair<String, String> columnToDeleteInfo =
                    new Pair<>(columnToDelete.getFamilyName().getString(), columnToDelete.getName().getString());
            ColumnReference colDropRef =
                    new ColumnReference(columnToDelete.getFamilyName().getBytes(),
                            columnToDelete.getColumnQualifierBytes());
            boolean isColumnIndexed = indexMaintainer.getIndexedColumnInfo().contains(columnToDeleteInfo);
            boolean isCoveredColumn = indexMaintainer.getCoveredColumns().contains(colDropRef);
            // If index requires this column for its pk, then drop it
            if (isColumnIndexed) {
                // Drop the index table. The doDropTable will expand
                // this to all of the table rows and invalidate the
                // index table
                Delete delete = new Delete(indexKey, clientTimeStamp);
                byte[] linkKey =
                        MetaDataUtil.getParentLinkKey(tenantId, table.getSchemaName().getBytes(),
                                table.getTableName().getBytes(), index.getTableName().getBytes());
                // Drop the link between the parent table and the
                // index table
                Delete linkDelete = new Delete(linkKey, clientTimeStamp);
                List<Mutation> remoteDropMetadata = Lists.newArrayListWithExpectedSize(2);
                remoteDropMetadata.add(delete);
                remoteDropMetadata.add(linkDelete);
                // if the index is not present on the current region make an rpc to drop it
                Properties props = new Properties();
                if (tenantId != null) {
                    props.setProperty(TENANT_ID_ATTRIB, Bytes.toString(tenantId));
                }
                if (clientTimeStamp != HConstants.LATEST_TIMESTAMP) {
                    props.setProperty("CurrentSCN", Long.toString(clientTimeStamp));
                }
                ConnectionQueryServices queryServices = connection.getQueryServices();
                MetaDataMutationResult result =
                        queryServices.dropTable(remoteDropMetadata, PTableType.INDEX, false);
                if (result.getTableNamesToDelete() != null && !result.getTableNamesToDelete().isEmpty())
                    tableNamesToDelete.addAll(result.getTableNamesToDelete());
                if (result.getSharedTablesToDelete() != null && !result.getSharedTablesToDelete().isEmpty())
                    sharedTablesToDelete.addAll(result.getSharedTablesToDelete());
                if (result.getMutationCode() != MutationCode.TABLE_ALREADY_EXISTS) {
                    return result;
                }
            }
            // If the dropped column is a covered index column, invalidate the index
            else if (isCoveredColumn) {
                clearRemoteTableFromCache(clientTimeStamp, index.getSchemaName() != null ?
                        index.getSchemaName().getBytes() : ByteUtil.EMPTY_BYTE_ARRAY, index.getTableName().getBytes());
            }
        }
        if (connection != null) {
            connection.close();
        }
        return null;
    }

    @Override
    public void clearCache(RpcController controller, ClearCacheRequest request,
                           RpcCallback<ClearCacheResponse> done) {
        GlobalCache cache = GlobalCache.getInstance(this.env);
        Cache<ImmutableBytesPtr, PMetaDataEntity> metaDataCache =
                GlobalCache.getInstance(this.env).getMetaDataCache();
        metaDataCache.invalidateAll();
        long unfreedBytes = cache.clearTenantCache();
        ClearCacheResponse.Builder builder = ClearCacheResponse.newBuilder();
        builder.setUnfreedBytes(unfreedBytes);
        done.run(builder.build());
    }

    @Override
    public void getVersion(RpcController controller, GetVersionRequest request, RpcCallback<GetVersionResponse> done) {

        GetVersionResponse.Builder builder = GetVersionResponse.newBuilder();
        Configuration config = env.getConfiguration();
        if (isTablesMappingEnabled
                && MetaDataProtocol.MIN_NAMESPACE_MAPPED_PHOENIX_VERSION > request.getClientVersion()) {
            LOGGER.error("Old client is not compatible when" + " system tables are upgraded to map to namespace");
            ProtobufUtil.setControllerException(controller,
                    ClientUtil.createIOException(
                            SchemaUtil.getPhysicalTableName(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES,
                                    isTablesMappingEnabled).toString(),
                            new DoNotRetryIOException(
                                    "Old client is not compatible when" + " system tables are upgraded to map to namespace")));
        }
        long version = MetaDataUtil.encodeVersion(env.getHBaseVersion(), config);

        PTable systemCatalog = null;
        try {
            systemCatalog =
                    doGetTable(ByteUtil.EMPTY_BYTE_ARRAY, PhoenixDatabaseMetaData.SYSTEM_CATALOG_SCHEMA_BYTES,
                            PhoenixDatabaseMetaData.SYSTEM_CATALOG_TABLE_BYTES, HConstants.LATEST_TIMESTAMP, null,
                            request.getClientVersion());
        } catch (Throwable t) {
            boolean isErrorSwallowed = false;
            if (t instanceof SQLException &&
                    ((SQLException) t).getErrorCode() == SQLExceptionCode.GET_TABLE_ERROR.getErrorCode()) {
                Region region = env.getRegion();
                final byte[] key = SchemaUtil.getTableKey(
                        ByteUtil.EMPTY_BYTE_ARRAY,
                        PhoenixDatabaseMetaData.SYSTEM_CATALOG_SCHEMA_BYTES,
                        PhoenixDatabaseMetaData.SYSTEM_CATALOG_TABLE_BYTES);
                if (!region.getRegionInfo().containsRow(key) &&
                        request.getClientVersion() < MIN_SPLITTABLE_SYSTEM_CATALOG) {
                    LOGGER.debug("The pre-4.15 client is trying to get SYSTEM.CATALOG " +
                            "region that contains head row");
                    isErrorSwallowed = true;
                }
            }
            if (!isErrorSwallowed) {
                LOGGER.error("loading system catalog table inside getVersion failed", t);
                ProtobufUtil.setControllerException(controller,
                        ClientUtil.createIOException(
                                SchemaUtil.getPhysicalTableName(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES,
                                        isTablesMappingEnabled).toString(), t));
            }
        }
        // In case this is the first connection, system catalog does not exist, and so we don't
        // set the optional system catalog timestamp.
        if (systemCatalog != null) {
            builder.setSystemCatalogTimestamp(systemCatalog.getTimeStamp());
        }
        builder.setVersion(version);
        done.run(builder.build());
    }

    @Override
    public void updateIndexState(RpcController controller, UpdateIndexStateRequest request,
                                 RpcCallback<MetaDataResponse> done) {
        MetaDataResponse.Builder builder = MetaDataResponse.newBuilder();
        byte[] schemaName = null;
        byte[] tableName = null;
        try {
            byte[][] rowKeyMetaData = new byte[3][];
            List<Mutation> tableMetadata = ProtobufUtil.getMutations(request);
            MetaDataUtil.getTenantIdAndSchemaAndTableName(tableMetadata, rowKeyMetaData);
            byte[] tenantId = rowKeyMetaData[PhoenixDatabaseMetaData.TENANT_ID_INDEX];
            schemaName = rowKeyMetaData[PhoenixDatabaseMetaData.SCHEMA_NAME_INDEX];
            tableName = rowKeyMetaData[PhoenixDatabaseMetaData.TABLE_NAME_INDEX];
            final byte[] key = SchemaUtil.getTableKey(tenantId, schemaName, tableName);
            Region region = env.getRegion();
            MetaDataMutationResult result = checkTableKeyInRegion(key, region);
            if (result != null) {
                done.run(MetaDataMutationResult.toProto(result));
                return;
            }
            long timeStamp = HConstants.LATEST_TIMESTAMP;
            ImmutableBytesPtr cacheKey = new ImmutableBytesPtr(key);
            List<Cell> newKVs = tableMetadata.get(0).getFamilyCellMap().get(TABLE_FAMILY_BYTES);
            Cell newKV = null;
            int disableTimeStampKVIndex = -1;
            int indexStateKVIndex = 0;
            int index = 0;
            for (Cell cell : newKVs) {
                if (Bytes.compareTo(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength(),
                        INDEX_STATE_BYTES, 0, INDEX_STATE_BYTES.length) == 0) {
                    newKV = cell;
                    indexStateKVIndex = index;
                    timeStamp = cell.getTimestamp();
                } else if (Bytes.compareTo(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength(),
                        INDEX_DISABLE_TIMESTAMP_BYTES, 0, INDEX_DISABLE_TIMESTAMP_BYTES.length) == 0) {
                    disableTimeStampKVIndex = index;
                }
                index++;
            }
            PIndexState newState =
                    PIndexState.fromSerializedValue(newKV.getValueArray()[newKV.getValueOffset()]);
            RowLock rowLock = acquireLock(region, key, null, false);
            if (rowLock == null) {
                throw new IOException("Failed to acquire lock on " + Bytes.toStringBinary(key));
            }

            Get get = new Get(key);
            get.addColumn(TABLE_FAMILY_BYTES, DATA_TABLE_NAME_BYTES);
            get.addColumn(TABLE_FAMILY_BYTES, INDEX_STATE_BYTES);
            get.addColumn(TABLE_FAMILY_BYTES, INDEX_DISABLE_TIMESTAMP_BYTES);
            get.addColumn(TABLE_FAMILY_BYTES, MetaDataEndpointImplConstants.ROW_KEY_ORDER_OPTIMIZABLE_BYTES);
            try (RegionScanner scanner = region.getScanner(new Scan(get))) {
                List<Cell> cells = new ArrayList<>();
                scanner.next(cells);
                if (cells.isEmpty()) {
                    builder.setReturnCode(MetaDataProtos.MutationCode.TABLE_NOT_FOUND);
                    builder.setMutationTime(EnvironmentEdgeManager.currentTimeMillis());
                    done.run(builder.build());
                    return;
                }
                Result currentResult = Result.create(cells);
                Cell dataTableKV = currentResult.getColumnLatestCell(TABLE_FAMILY_BYTES, DATA_TABLE_NAME_BYTES);
                Cell currentStateKV = currentResult.getColumnLatestCell(TABLE_FAMILY_BYTES, INDEX_STATE_BYTES);
                Cell currentDisableTimeStamp = currentResult.getColumnLatestCell(TABLE_FAMILY_BYTES, INDEX_DISABLE_TIMESTAMP_BYTES);
                boolean rowKeyOrderOptimizable = currentResult.getColumnLatestCell(TABLE_FAMILY_BYTES, MetaDataEndpointImplConstants.ROW_KEY_ORDER_OPTIMIZABLE_BYTES) != null;

                //check permission on data table
                long clientTimeStamp = MetaDataUtil.getClientTimeStamp(tableMetadata);
                PTable loadedTable =
                        doGetTable(tenantId, schemaName, tableName, clientTimeStamp, null,
                                request.getClientVersion());
                if (loadedTable == null) {
                    builder.setReturnCode(MetaDataProtos.MutationCode.TABLE_NOT_FOUND);
                    builder.setMutationTime(EnvironmentEdgeManager.currentTimeMillis());
                    done.run(builder.build());
                    return;
                }
                getCoprocessorHost().preIndexUpdate(Bytes.toString(tenantId),
                        SchemaUtil.getTableName(schemaName, tableName),
                        TableName.valueOf(loadedTable.getPhysicalName().getBytes()),
                        getParentPhysicalTableName(loadedTable),
                        newState);

                PIndexState currentState =
                        PIndexState.fromSerializedValue(currentStateKV.getValueArray()[currentStateKV
                                .getValueOffset()]);
                // Timestamp of INDEX_STATE gets updated with each call
                long actualTimestamp = currentStateKV.getTimestamp();
                long curTimeStampVal = 0;
                long newDisableTimeStamp = 0;
                if ((currentDisableTimeStamp != null && currentDisableTimeStamp.getValueLength() > 0)) {
                    curTimeStampVal = (Long) PLong.INSTANCE.toObject(currentDisableTimeStamp.getValueArray(),
                            currentDisableTimeStamp.getValueOffset(), currentDisableTimeStamp.getValueLength());
                    // new DisableTimeStamp is passed in
                    if (disableTimeStampKVIndex >= 0) {
                        Cell newDisableTimeStampCell = newKVs.get(disableTimeStampKVIndex);
                        long expectedTimestamp = newDisableTimeStampCell.getTimestamp();
                        // If the index status has been updated after the upper bound of the scan we use
                        // to partially rebuild the index, then we need to fail the rebuild because an
                        // index write failed before the rebuild was complete.
                        if (actualTimestamp > expectedTimestamp) {
                            builder.setReturnCode(MetaDataProtos.MutationCode.UNALLOWED_TABLE_MUTATION);
                            builder.setMutationTime(EnvironmentEdgeManager.currentTimeMillis());
                            done.run(builder.build());
                            return;
                        }
                        newDisableTimeStamp = (Long) PLong.INSTANCE.toObject(newDisableTimeStampCell.getValueArray(),
                                newDisableTimeStampCell.getValueOffset(), newDisableTimeStampCell.getValueLength());
                        // We use the sign of the INDEX_DISABLE_TIMESTAMP to differentiate the keep-index-active (negative)
                        // from block-writes-to-data-table case. In either case, we want to keep the oldest timestamp to
                        // drive the partial index rebuild rather than update it with each attempt to update the index
                        // when a new data table write occurs.
                        // We do legitimately move the INDEX_DISABLE_TIMESTAMP to be newer when we're rebuilding the
                        // index in which case the state will be INACTIVE or PENDING_ACTIVE.
                        if (curTimeStampVal != 0
                                && (newState == PIndexState.DISABLE || newState == PIndexState.PENDING_ACTIVE || newState == PIndexState.PENDING_DISABLE)
                                && Math.abs(curTimeStampVal) < Math.abs(newDisableTimeStamp)) {
                            // do not reset disable timestamp as we want to keep the min
                            newKVs.remove(disableTimeStampKVIndex);
                            disableTimeStampKVIndex = -1;
                        }
                    }
                }
                // Detect invalid transitions
                if (currentState == PIndexState.BUILDING) {
                    if (newState == PIndexState.USABLE) {
                        builder.setReturnCode(MetaDataProtos.MutationCode.UNALLOWED_TABLE_MUTATION);
                        builder.setMutationTime(EnvironmentEdgeManager.currentTimeMillis());
                        done.run(builder.build());
                        return;
                    }
                } else if (currentState == PIndexState.DISABLE) {
                    // Index already disabled, so can't revert to PENDING_DISABLE
                    if (newState == PIndexState.PENDING_DISABLE) {
                        // returning TABLE_ALREADY_EXISTS here means the client doesn't throw an exception
                        builder.setReturnCode(MetaDataProtos.MutationCode.TABLE_ALREADY_EXISTS);
                        builder.setMutationTime(EnvironmentEdgeManager.currentTimeMillis());
                        done.run(builder.build());
                        return;
                    }
                    // Can't transition back to INACTIVE if INDEX_DISABLE_TIMESTAMP is 0
                    if (newState != PIndexState.BUILDING && newState != PIndexState.DISABLE &&
                            (newState != PIndexState.INACTIVE || curTimeStampVal == 0)) {
                        builder.setReturnCode(MetaDataProtos.MutationCode.UNALLOWED_TABLE_MUTATION);
                        builder.setMutationTime(EnvironmentEdgeManager.currentTimeMillis());
                        done.run(builder.build());
                        return;
                    }
                    // Done building, but was disable before that, so that in disabled state
                    if (newState == PIndexState.ACTIVE) {
                        newState = PIndexState.DISABLE;
                    }
                }
                if (newState == PIndexState.PENDING_DISABLE && currentState != PIndexState.PENDING_DISABLE
                        && currentState != PIndexState.INACTIVE) {
                    // reset count for first PENDING_DISABLE
                    newKVs.add(PhoenixKeyValueUtil.newKeyValue(key, TABLE_FAMILY_BYTES,
                        PhoenixDatabaseMetaData.PENDING_DISABLE_COUNT_BYTES, timeStamp, Bytes.toBytes(0L)));
                }
                if (currentState == PIndexState.PENDING_DISABLE) {
                    if (newState == PIndexState.ACTIVE) {
                        //before making index ACTIVE check if all clients succeed otherwise keep it PENDING_DISABLE
                        byte[] count;
                        try (RegionScanner countScanner = region.getScanner(new Scan(get))) {
                            List<Cell> countCells = new ArrayList<>();
                            countScanner.next(countCells);
                            count = Result.create(countCells)
                                    .getValue(TABLE_FAMILY_BYTES,
                                        PhoenixDatabaseMetaData.PENDING_DISABLE_COUNT_BYTES);
                        }
                        if (count != null && Bytes.toLong(count) != 0) {
                            newState = PIndexState.PENDING_DISABLE;
                            newKVs.remove(disableTimeStampKVIndex);
                            newKVs.set(indexStateKVIndex, PhoenixKeyValueUtil.newKeyValue(key, TABLE_FAMILY_BYTES,
                                INDEX_STATE_BYTES, timeStamp, Bytes.toBytes(newState.getSerializedValue())));
                        } else if (disableTimeStampKVIndex == -1) { // clear disableTimestamp if client didn't pass it in
                            newKVs.add(PhoenixKeyValueUtil.newKeyValue(key, TABLE_FAMILY_BYTES,
                                PhoenixDatabaseMetaData.INDEX_DISABLE_TIMESTAMP_BYTES, timeStamp, PLong.INSTANCE.toBytes(0)));
                            disableTimeStampKVIndex = newKVs.size() - 1;
                        }
                    } else if (newState == PIndexState.DISABLE) {
                        //reset the counter for pending disable when transitioning from PENDING_DISABLE to DISABLE
                        newKVs.add(PhoenixKeyValueUtil.newKeyValue(key, TABLE_FAMILY_BYTES,
                            PhoenixDatabaseMetaData.PENDING_DISABLE_COUNT_BYTES, timeStamp, Bytes.toBytes(0L)));
                    }

                }

                if (newState == PIndexState.ACTIVE || newState == PIndexState.PENDING_ACTIVE || newState == PIndexState.DISABLE) {
                    newKVs.add(PhoenixKeyValueUtil.newKeyValue(key, TABLE_FAMILY_BYTES,
                        PhoenixDatabaseMetaData.PENDING_DISABLE_COUNT_BYTES, timeStamp, Bytes.toBytes(0L)));
                }

                if (currentState == PIndexState.BUILDING && newState != PIndexState.ACTIVE) {
                    timeStamp = currentStateKV.getTimestamp();
                }
                if ((currentState == PIndexState.ACTIVE || currentState == PIndexState.PENDING_ACTIVE) && newState == PIndexState.UNUSABLE) {
                    newState = PIndexState.INACTIVE;
                    newKVs.set(indexStateKVIndex, PhoenixKeyValueUtil.newKeyValue(key, TABLE_FAMILY_BYTES,
                        INDEX_STATE_BYTES, timeStamp, Bytes.toBytes(newState.getSerializedValue())));
                } else if ((currentState == PIndexState.INACTIVE || currentState == PIndexState.PENDING_ACTIVE) && newState == PIndexState.USABLE) {
                    // Don't allow manual state change to USABLE (i.e. ACTIVE) if non zero INDEX_DISABLE_TIMESTAMP
                    if (curTimeStampVal != 0) {
                        newState = currentState;
                    } else {
                        newState = PIndexState.ACTIVE;
                    }
                    newKVs.set(indexStateKVIndex, PhoenixKeyValueUtil.newKeyValue(key, TABLE_FAMILY_BYTES,
                        INDEX_STATE_BYTES, timeStamp, Bytes.toBytes(newState.getSerializedValue())));
                }

                PTable returnTable = null;
                if (currentState != newState || disableTimeStampKVIndex != -1) {
                    // make a copy of tableMetadata so we can add to it
                    tableMetadata = new ArrayList<Mutation>(tableMetadata);
                    // Always include the empty column value at latest timestamp so
                    // that clients pull over update.
                    Put emptyValue = new Put(key);
                    emptyValue.addColumn(TABLE_FAMILY_BYTES,
                            QueryConstants.EMPTY_COLUMN_BYTES,
                            HConstants.LATEST_TIMESTAMP,
                            QueryConstants.EMPTY_COLUMN_VALUE_BYTES);
                    tableMetadata.add(emptyValue);
                    byte[] dataTableKey = null;
                    if (dataTableKV != null) {
                        dataTableKey = SchemaUtil.getTableKey(tenantId, schemaName, CellUtil.cloneValue(dataTableKV));
                        // insert an empty KV to trigger time stamp update on data table row
                        Put p = new Put(dataTableKey);
                        p.addColumn(TABLE_FAMILY_BYTES,
                                QueryConstants.EMPTY_COLUMN_BYTES,
                                HConstants.LATEST_TIMESTAMP,
                                QueryConstants.EMPTY_COLUMN_VALUE_BYTES);
                        tableMetadata.add(p);
                    }
                    boolean setRowKeyOrderOptimizableCell = newState == PIndexState.BUILDING && !rowKeyOrderOptimizable;
                    // We're starting a rebuild of the index, so add our rowKeyOrderOptimizable cell
                    // so that the row keys get generated using the new row key format
                    if (setRowKeyOrderOptimizableCell) {
                        UpgradeUtil.addRowKeyOrderOptimizableCell(tableMetadata, key, timeStamp);
                    }
                    // We are updating the state of an index, so update the DDL timestamp.
                    long serverTimestamp = EnvironmentEdgeManager.currentTimeMillis();
                    tableMetadata.add(MetaDataUtil.getLastDDLTimestampUpdate(
                            key, clientTimeStamp, serverTimestamp));
                    mutateRowsWithLocks(this.accessCheckEnabled, region, tableMetadata, Collections.<byte[]>emptySet(),
                        HConstants.NO_NONCE, HConstants.NO_NONCE);
                    // Invalidate from cache
                    Cache<ImmutableBytesPtr, PMetaDataEntity> metaDataCache =
                            GlobalCache.getInstance(this.env).getMetaDataCache();
                    metaDataCache.invalidate(cacheKey);
                    if (dataTableKey != null) {
                        metaDataCache.invalidate(new ImmutableBytesPtr(dataTableKey));
                    }
                    if (setRowKeyOrderOptimizableCell || disableTimeStampKVIndex != -1
                            || currentState.isDisabled() || newState == PIndexState.BUILDING) {
                        returnTable = doGetTable(tenantId, schemaName, tableName,
                                HConstants.LATEST_TIMESTAMP, rowLock, request.getClientVersion());
                    }
                }
                // Get client timeStamp from mutations, since it may get updated by the
                // mutateRowsWithLocks call
                long currentTime = MetaDataUtil.getClientTimeStamp(tableMetadata);
                builder.setReturnCode(MetaDataProtos.MutationCode.TABLE_ALREADY_EXISTS);
                builder.setMutationTime(currentTime);
                if (returnTable != null) {
                    builder.setTable(PTableImpl.toProto(returnTable));
                }
                done.run(builder.build());
                return;
            } finally {
                rowLock.release();
            }
        } catch (Throwable t) {
            LOGGER.error("updateIndexState failed", t);
            ProtobufUtil.setControllerException(controller,
                    ClientUtil.createIOException(SchemaUtil.getTableName(schemaName, tableName), t));
        }
    }

    private static MetaDataMutationResult checkKeyInRegion(byte[] key, Region region, MutationCode code) {
        return ServerUtil.isKeyInRegion(key, region) ? null :
                new MetaDataMutationResult(code, EnvironmentEdgeManager.currentTimeMillis(), null);
    }

    private static MetaDataMutationResult checkTableKeyInRegion(byte[] key, Region region) {
        MetaDataMutationResult result = checkKeyInRegion(key, region, MutationCode.TABLE_NOT_IN_REGION);
        if (result != null) {
            LOGGER.error("Table rowkey " + Bytes.toStringBinary(key)
                    + " is not in the current region " + region.getRegionInfo());
        }
        return result;

    }

    private static MetaDataMutationResult checkFunctionKeyInRegion(byte[] key, Region region) {
        return checkKeyInRegion(key, region, MutationCode.FUNCTION_NOT_IN_REGION);
    }

    private static MetaDataMutationResult checkSchemaKeyInRegion(byte[] key, Region region) {
        return checkKeyInRegion(key, region, MutationCode.SCHEMA_NOT_IN_REGION);

    }

    private static class ViewInfo {
        private byte[] tenantId;
        private byte[] schemaName;
        private byte[] viewName;

        public ViewInfo(byte[] tenantId, byte[] schemaName, byte[] viewName) {
            super();
            this.tenantId = tenantId;
            this.schemaName = schemaName;
            this.viewName = viewName;
        }

        public byte[] getTenantId() {
            return tenantId;
        }

        public byte[] getSchemaName() {
            return schemaName;
        }

        public byte[] getViewName() {
            return viewName;
        }
    }

    @Override
    public void clearTableFromCache(RpcController controller, ClearTableFromCacheRequest request,
                                    RpcCallback<ClearTableFromCacheResponse> done) {
        byte[] schemaName = request.getSchemaName().toByteArray();
        byte[] tableName = request.getTableName().toByteArray();
        try {
            byte[] tenantId = request.getTenantId().toByteArray();
            byte[] key = SchemaUtil.getTableKey(tenantId, schemaName, tableName);
            ImmutableBytesPtr cacheKey = new ImmutableBytesPtr(key);
            Cache<ImmutableBytesPtr, PMetaDataEntity> metaDataCache =
                    GlobalCache.getInstance(this.env).getMetaDataCache();
            metaDataCache.invalidate(cacheKey);
        } catch (Throwable t) {
            LOGGER.error("clearTableFromCache failed", t);
            ProtobufUtil.setControllerException(controller,
                    ClientUtil.createIOException(SchemaUtil.getTableName(schemaName, tableName), t));
        }
    }

    @Override
    public void getSchema(RpcController controller, GetSchemaRequest request, RpcCallback<MetaDataResponse> done) {
        MetaDataResponse.Builder builder = MetaDataResponse.newBuilder();
        Region region = env.getRegion();
        String schemaName = request.getSchemaName();
        byte[] lockKey = SchemaUtil.getSchemaKey(schemaName);
        MetaDataMutationResult result = checkSchemaKeyInRegion(lockKey, region);
        if (result != null) {
            done.run(MetaDataMutationResult.toProto(result));
            return;
        }
        long clientTimeStamp = request.getClientTimestamp();
        List<RowLock> locks = Lists.newArrayList();
        try {
            getCoprocessorHost().preGetSchema(schemaName);
            acquireLock(region, lockKey, locks, false);
            // Get as of latest timestamp so we can detect if we have a
            // newer schema that already
            // exists without making an additional query
            ImmutableBytesPtr cacheKey = new ImmutableBytesPtr(lockKey);
            PSchema schema = loadSchema(env, lockKey, cacheKey, clientTimeStamp, clientTimeStamp);
            if (schema != null) {
                if (schema.getTimeStamp() < clientTimeStamp) {
                    if (!isSchemaDeleted(schema)) {
                        builder.setReturnCode(MetaDataProtos.MutationCode.SCHEMA_ALREADY_EXISTS);
                        builder.setMutationTime(EnvironmentEdgeManager.currentTimeMillis());
                        builder.setSchema(PSchema.toProto(schema));
                        done.run(builder.build());
                        return;
                    } else {
                        builder.setReturnCode(MetaDataProtos.MutationCode.NEWER_SCHEMA_FOUND);
                        builder.setMutationTime(EnvironmentEdgeManager.currentTimeMillis());
                        builder.setSchema(PSchema.toProto(schema));
                        done.run(builder.build());
                        return;
                    }
                }
            }
        } catch (Exception e) {
            long currentTime = EnvironmentEdgeManager.currentTimeMillis();
            builder.setReturnCode(MetaDataProtos.MutationCode.SCHEMA_NOT_FOUND);
            builder.setMutationTime(currentTime);
            done.run(builder.build());
            return;
        } finally {
            ServerUtil.releaseRowLocks(locks);
        }
    }

    @Override
    public void getFunctions(RpcController controller, GetFunctionsRequest request,
                             RpcCallback<MetaDataResponse> done) {
        MetaDataResponse.Builder builder = MetaDataResponse.newBuilder();
        byte[] tenantId = request.getTenantId().toByteArray();
        List<String> functionNames = new ArrayList<>(request.getFunctionNamesCount());
        try {
            Region region = env.getRegion();
            List<ByteString> functionNamesList = request.getFunctionNamesList();
            List<Long> functionTimestampsList = request.getFunctionTimestampsList();
            List<byte[]> keys = new ArrayList<byte[]>(request.getFunctionNamesCount());
            List<Pair<byte[], Long>> functions = new ArrayList<Pair<byte[], Long>>(request.getFunctionNamesCount());
            for (int i = 0; i < functionNamesList.size(); i++) {
                byte[] functionName = functionNamesList.get(i).toByteArray();
                functionNames.add(Bytes.toString(functionName));
                byte[] key = SchemaUtil.getFunctionKey(tenantId, functionName);
                MetaDataMutationResult result = checkFunctionKeyInRegion(key, region);
                if (result != null) {
                    done.run(MetaDataMutationResult.toProto(result));
                    return;
                }
                functions.add(new Pair<byte[], Long>(functionName, functionTimestampsList.get(i)));
                keys.add(key);
            }

            long currentTime = EnvironmentEdgeManager.currentTimeMillis();
            List<PFunction> functionsAvailable = doGetFunctions(keys, request.getClientTimestamp());
            if (functionsAvailable == null) {
                builder.setReturnCode(MetaDataProtos.MutationCode.FUNCTION_NOT_FOUND);
                builder.setMutationTime(currentTime);
                done.run(builder.build());
                return;
            }
            builder.setReturnCode(MetaDataProtos.MutationCode.FUNCTION_ALREADY_EXISTS);
            builder.setMutationTime(currentTime);

            for (PFunction function : functionsAvailable) {
                builder.addFunction(PFunction.toProto(function));
            }
            done.run(builder.build());
            return;
        } catch (Throwable t) {
            LOGGER.error("getFunctions failed", t);
            ProtobufUtil.setControllerException(controller,
                    ClientUtil.createIOException(functionNames.toString(), t));
        }
    }

    @Override
    public void createFunction(RpcController controller, CreateFunctionRequest request,
                               RpcCallback<MetaDataResponse> done) {
        MetaDataResponse.Builder builder = MetaDataResponse.newBuilder();
        byte[][] rowKeyMetaData = new byte[2][];
        byte[] functionName = null;
        try {
            List<Mutation> functionMetaData = ProtobufUtil.getMutations(request);
            boolean temporaryFunction = request.getTemporary();
            MetaDataUtil.getTenantIdAndFunctionName(functionMetaData, rowKeyMetaData);
            byte[] tenantIdBytes = rowKeyMetaData[PhoenixDatabaseMetaData.TENANT_ID_INDEX];
            functionName = rowKeyMetaData[PhoenixDatabaseMetaData.FUNTION_NAME_INDEX];
            byte[] lockKey = SchemaUtil.getFunctionKey(tenantIdBytes, functionName);
            Region region = env.getRegion();
            MetaDataMutationResult result = checkFunctionKeyInRegion(lockKey, region);
            if (result != null) {
                done.run(MetaDataMutationResult.toProto(result));
                return;
            }
            List<RowLock> locks = Lists.newArrayList();
            long clientTimeStamp = MetaDataUtil.getClientTimeStamp(functionMetaData);
            try {
                acquireLock(region, lockKey, locks, false);
                // Get as of latest timestamp so we can detect if we have a newer function that already
                // exists without making an additional query
                ImmutableBytesPtr cacheKey = new FunctionBytesPtr(lockKey);
                PFunction function =
                        loadFunction(env, lockKey, cacheKey, clientTimeStamp, clientTimeStamp, request.getReplace(), functionMetaData);
                if (function != null) {
                    if (function.getTimeStamp() < clientTimeStamp) {
                        // If the function is older than the client time stamp and it's deleted,
                        // continue
                        if (!isFunctionDeleted(function)) {
                            builder.setReturnCode(MetaDataProtos.MutationCode.FUNCTION_ALREADY_EXISTS);
                            builder.setMutationTime(EnvironmentEdgeManager.currentTimeMillis());
                            builder.addFunction(PFunction.toProto(function));
                            done.run(builder.build());
                            if (!request.getReplace()) {
                                return;
                            }
                        }
                    } else {
                        builder.setReturnCode(MetaDataProtos.MutationCode.NEWER_FUNCTION_FOUND);
                        builder.setMutationTime(EnvironmentEdgeManager.currentTimeMillis());
                        builder.addFunction(PFunction.toProto(function));
                        done.run(builder.build());
                        return;
                    }
                }
                // Don't store function info for temporary functions.
                if (!temporaryFunction) {
                    mutateRowsWithLocks(this.accessCheckEnabled, region, functionMetaData,
                        Collections.<byte[]>emptySet(), HConstants.NO_NONCE, HConstants.NO_NONCE);
                }

                // Invalidate the cache - the next getFunction call will add it
                // TODO: consider loading the function that was just created here, patching up the parent function, and updating the cache
                Cache<ImmutableBytesPtr, PMetaDataEntity> metaDataCache = GlobalCache.getInstance(this.env).getMetaDataCache();
                metaDataCache.invalidate(cacheKey);
                // Get timeStamp from mutations - the above method sets it if it's unset
                long currentTimeStamp = MetaDataUtil.getClientTimeStamp(functionMetaData);
                builder.setReturnCode(MetaDataProtos.MutationCode.FUNCTION_NOT_FOUND);
                builder.setMutationTime(currentTimeStamp);
                done.run(builder.build());

                metricsSource.incrementCreateFunctionCount();
                LOGGER.info("FUNCTION created successfully, functionName: {}",
                        Bytes.toString(functionName));
            } finally {
                ServerUtil.releaseRowLocks(locks);
            }
        } catch (Throwable t) {
            LOGGER.error("createFunction failed", t);
            ProtobufUtil.setControllerException(controller,
                    ClientUtil.createIOException(Bytes.toString(functionName), t));
        }
    }

    @Override
    public void dropFunction(RpcController controller, DropFunctionRequest request,
                             RpcCallback<MetaDataResponse> done) {
        byte[][] rowKeyMetaData = new byte[2][];
        byte[] functionName = null;
        try {
            List<Mutation> functionMetaData = ProtobufUtil.getMutations(request);
            MetaDataUtil.getTenantIdAndFunctionName(functionMetaData, rowKeyMetaData);
            byte[] tenantIdBytes = rowKeyMetaData[PhoenixDatabaseMetaData.TENANT_ID_INDEX];
            functionName = rowKeyMetaData[PhoenixDatabaseMetaData.FUNTION_NAME_INDEX];
            byte[] lockKey = SchemaUtil.getFunctionKey(tenantIdBytes, functionName);
            Region region = env.getRegion();
            MetaDataMutationResult result = checkFunctionKeyInRegion(lockKey, region);
            if (result != null) {
                done.run(MetaDataMutationResult.toProto(result));
                return;
            }
            List<RowLock> locks = Lists.newArrayList();
            long clientTimeStamp = MetaDataUtil.getClientTimeStamp(functionMetaData);
            try {
                acquireLock(region, lockKey, locks, false);
                List<byte[]> keys = new ArrayList<byte[]>(1);
                keys.add(lockKey);
                List<ImmutableBytesPtr> invalidateList = new ArrayList<ImmutableBytesPtr>();

                result = doDropFunction(clientTimeStamp, keys, functionMetaData, invalidateList);
                if (result.getMutationCode() != MutationCode.FUNCTION_ALREADY_EXISTS) {
                    done.run(MetaDataMutationResult.toProto(result));
                    return;
                }
                mutateRowsWithLocks(this.accessCheckEnabled, region, functionMetaData, Collections.<byte[]>emptySet(),
                    HConstants.NO_NONCE, HConstants.NO_NONCE);

                Cache<ImmutableBytesPtr, PMetaDataEntity> metaDataCache = GlobalCache.getInstance(this.env).getMetaDataCache();
                long currentTime = MetaDataUtil.getClientTimeStamp(functionMetaData);
                for (ImmutableBytesPtr ptr : invalidateList) {
                    metaDataCache.invalidate(ptr);
                    PFunction function = newDeletedFunctionMarker(currentTime);
                    metaDataCache.put(ptr, function);
                    metricsSource.incrementMetadataCacheAddCount();
                    metricsSource.incrementMetadataCacheUsedSize(function.getEstimatedSize());
                }

                done.run(MetaDataMutationResult.toProto(result));

                metricsSource.incrementDropFunctionCount();
                LOGGER.info("FUNCTION dropped successfully, functionName: {}",
                        Bytes.toString(functionName));
            } finally {
                ServerUtil.releaseRowLocks(locks);
            }
        } catch (Throwable t) {
            LOGGER.error("dropFunction failed", t);
            ProtobufUtil.setControllerException(controller,
                    ClientUtil.createIOException(Bytes.toString(functionName), t));
        }
    }

    private MetaDataMutationResult doDropFunction(long clientTimeStamp, List<byte[]> keys, List<Mutation> functionMetaData, List<ImmutableBytesPtr> invalidateList)
            throws IOException, SQLException {
        List<byte[]> keysClone = new ArrayList<byte[]>(keys);
        List<PFunction> functions = doGetFunctions(keysClone, clientTimeStamp);
        // We didn't find a table at the latest timestamp, so either there is no table or
        // there was a table, but it's been deleted. In either case we want to return.
        if (functions == null || functions.isEmpty()) {
            if (buildDeletedFunction(keys.get(0), new FunctionBytesPtr(keys.get(0)), env.getRegion(), clientTimeStamp) != null) {
                return new MetaDataMutationResult(MutationCode.FUNCTION_ALREADY_EXISTS, EnvironmentEdgeManager.currentTimeMillis(), null);
            }
            return new MetaDataMutationResult(MutationCode.FUNCTION_NOT_FOUND, EnvironmentEdgeManager.currentTimeMillis(), null);
        }

        if (functions != null && !functions.isEmpty()) {
            if (functions.get(0).getTimeStamp() < clientTimeStamp) {
                // If the function is older than the client time stamp and it's deleted,
                // continue
                if (isFunctionDeleted(functions.get(0))) {
                    return new MetaDataMutationResult(MutationCode.FUNCTION_NOT_FOUND,
                            EnvironmentEdgeManager.currentTimeMillis(), null);
                }
                invalidateList.add(new FunctionBytesPtr(keys.get(0)));
                Region region = env.getRegion();
                Scan scan = MetaDataUtil.newTableRowsScan(keys.get(0), MIN_TABLE_TIMESTAMP, clientTimeStamp);
                List<Cell> results = Lists.newArrayList();
                try (RegionScanner scanner = region.getScanner(scan)) {
                    scanner.next(results);
                    if (results.isEmpty()) { // Should not be possible
                        return new MetaDataMutationResult(MutationCode.FUNCTION_NOT_FOUND, EnvironmentEdgeManager.currentTimeMillis(), null);
                    }
                    do {
                        Cell kv = results.get(0);
                        Delete delete = new Delete(kv.getRowArray(), kv.getRowOffset(), kv.getRowLength(), clientTimeStamp);
                        functionMetaData.add(delete);
                        results.clear();
                        scanner.next(results);
                    } while (!results.isEmpty());
                }
                return new MetaDataMutationResult(MutationCode.FUNCTION_ALREADY_EXISTS,
                        EnvironmentEdgeManager.currentTimeMillis(), functions, true);
            }
        }
        return new MetaDataMutationResult(MutationCode.FUNCTION_NOT_FOUND,
                EnvironmentEdgeManager.currentTimeMillis(), null);
    }

    @Override
    public void createSchema(RpcController controller, CreateSchemaRequest request,
                             RpcCallback<MetaDataResponse> done) {
        MetaDataResponse.Builder builder = MetaDataResponse.newBuilder();
        String schemaName = null;
        try {
            List<Mutation> schemaMutations = ProtobufUtil.getMutations(request);
            schemaName = request.getSchemaName();
            //don't do the user permission checks for the SYSTEM schema, because an ordinary
            //user has to be able to create it if it doesn't already exist when bootstrapping
            //the system tables
            if (!schemaName.equals(QueryConstants.SYSTEM_SCHEMA_NAME)) {
                getCoprocessorHost().preCreateSchema(schemaName);
            }
            Mutation m = MetaDataUtil.getPutOnlyTableHeaderRow(schemaMutations);

            byte[] lockKey = m.getRow();
            Region region = env.getRegion();
            MetaDataMutationResult result = checkSchemaKeyInRegion(lockKey, region);
            if (result != null) {
                done.run(MetaDataMutationResult.toProto(result));
                return;
            }
            List<RowLock> locks = Lists.newArrayList();
            long clientTimeStamp = MetaDataUtil.getClientTimeStamp(schemaMutations);
            try {
                acquireLock(region, lockKey, locks, false);
                // Get as of latest timestamp so we can detect if we have a newer schema that already exists without
                // making an additional query
                ImmutableBytesPtr cacheKey = new ImmutableBytesPtr(lockKey);
                PSchema schema = loadSchema(env, lockKey, cacheKey, clientTimeStamp, clientTimeStamp);
                if (schema != null) {
                    if (schema.getTimeStamp() < clientTimeStamp) {
                        if (!isSchemaDeleted(schema)) {
                            builder.setReturnCode(MetaDataProtos.MutationCode.SCHEMA_ALREADY_EXISTS);
                            builder.setMutationTime(EnvironmentEdgeManager.currentTimeMillis());
                            builder.setSchema(PSchema.toProto(schema));
                            done.run(builder.build());
                            return;
                        }
                    } else {
                        builder.setReturnCode(MetaDataProtos.MutationCode.NEWER_SCHEMA_FOUND);
                        builder.setMutationTime(EnvironmentEdgeManager.currentTimeMillis());
                        builder.setSchema(PSchema.toProto(schema));
                        done.run(builder.build());
                        return;
                    }
                }
                mutateRowsWithLocks(this.accessCheckEnabled, region, schemaMutations, Collections.<byte[]>emptySet(),
                    HConstants.NO_NONCE, HConstants.NO_NONCE);

                // Invalidate the cache - the next getSchema call will add it
                Cache<ImmutableBytesPtr, PMetaDataEntity> metaDataCache =
                        GlobalCache.getInstance(this.env).getMetaDataCache();
                if (cacheKey != null) {
                    metaDataCache.invalidate(cacheKey);
                }

                // Get timeStamp from mutations - the above method sets it if
                // it's unset
                long currentTimeStamp = MetaDataUtil.getClientTimeStamp(schemaMutations);
                builder.setReturnCode(MetaDataProtos.MutationCode.SCHEMA_NOT_FOUND);
                builder.setMutationTime(currentTimeStamp);
                done.run(builder.build());

                metricsSource.incrementCreateSchemaCount();
                LOGGER.info("SCHEMA created successfully, schemaName: {}", schemaName);
            } finally {
                ServerUtil.releaseRowLocks(locks);
            }
        } catch (Throwable t) {
            LOGGER.error("Creating the schema" + schemaName + "failed", t);
            ProtobufUtil.setControllerException(controller, ClientUtil.createIOException(schemaName, t));
        }
    }

    @Override
    public void dropSchema(RpcController controller, DropSchemaRequest request, RpcCallback<MetaDataResponse> done) {
        String schemaName = null;
        try {
            List<Mutation> schemaMetaData = ProtobufUtil.getMutations(request);
            schemaName = request.getSchemaName();
            getCoprocessorHost().preDropSchema(schemaName);
            byte[] lockKey = SchemaUtil.getSchemaKey(schemaName);
            Region region = env.getRegion();
            MetaDataMutationResult result = checkSchemaKeyInRegion(lockKey, region);
            if (result != null) {
                done.run(MetaDataMutationResult.toProto(result));
                return;
            }
            List<RowLock> locks = Lists.newArrayList();
            long clientTimeStamp = MetaDataUtil.getClientTimeStamp(schemaMetaData);
            try {
                acquireLock(region, lockKey, locks, false);
                List<ImmutableBytesPtr> invalidateList = new ArrayList<ImmutableBytesPtr>(1);
                result = doDropSchema(clientTimeStamp, schemaName, lockKey, schemaMetaData, invalidateList);
                if (result.getMutationCode() != MutationCode.SCHEMA_ALREADY_EXISTS) {
                    done.run(MetaDataMutationResult.toProto(result));
                    return;
                }
                mutateRowsWithLocks(this.accessCheckEnabled, region, schemaMetaData, Collections.<byte[]>emptySet(),
                    HConstants.NO_NONCE, HConstants.NO_NONCE);
                Cache<ImmutableBytesPtr, PMetaDataEntity> metaDataCache = GlobalCache.getInstance(this.env)
                        .getMetaDataCache();
                long currentTime = MetaDataUtil.getClientTimeStamp(schemaMetaData);
                for (ImmutableBytesPtr ptr : invalidateList) {
                    metaDataCache.invalidate(ptr);
                    PSchema schema = newDeletedSchemaMarker(currentTime);
                    metaDataCache.put(ptr, schema);
                    metricsSource.incrementMetadataCacheAddCount();
                    metricsSource.incrementMetadataCacheUsedSize(schema.getEstimatedSize());
                }
                done.run(MetaDataMutationResult.toProto(result));

                metricsSource.incrementDropSchemaCount();
                LOGGER.info("SCHEMA dropped successfully, schemaName: {}", schemaName);
            } finally {
                ServerUtil.releaseRowLocks(locks);
            }
        } catch (Throwable t) {
            LOGGER.error("drop schema failed:", t);
            ProtobufUtil.setControllerException(controller, ClientUtil.createIOException(schemaName, t));
        }
    }

    private MetaDataMutationResult doDropSchema(long clientTimeStamp, String schemaName, byte[] key,
                                                List<Mutation> schemaMutations, List<ImmutableBytesPtr> invalidateList) throws IOException, SQLException {
        PSchema schema = loadSchema(env, key, new ImmutableBytesPtr(key), clientTimeStamp, clientTimeStamp);
        boolean areTablesExists = false;
        if (schema == null) {
            return new MetaDataMutationResult(MutationCode.SCHEMA_NOT_FOUND,
                    EnvironmentEdgeManager.currentTimeMillis(), null);
        }
        if (schema.getTimeStamp() < clientTimeStamp) {
            Region region = env.getRegion();
            Scan scan = MetaDataUtil.newTableRowsScan(SchemaUtil.getKeyForSchema(null, schemaName), MIN_TABLE_TIMESTAMP,
                    clientTimeStamp);
            List<Cell> results = Lists.newArrayList();
            try (RegionScanner scanner = region.getScanner(scan);) {
                scanner.next(results);
                if (results.isEmpty()) { // Should not be possible
                    return new MetaDataMutationResult(MutationCode.SCHEMA_NOT_FOUND,
                            EnvironmentEdgeManager.currentTimeMillis(), null);
                }
                do {
                    Cell kv = results.get(0);
                    if (Bytes.compareTo(kv.getRowArray(), kv.getRowOffset(), kv.getRowLength(), key, 0,
                            key.length) != 0) {
                        areTablesExists = true;
                        break;
                    }
                    results.clear();
                    scanner.next(results);
                } while (!results.isEmpty());
            }
            if (areTablesExists) {
                return new MetaDataMutationResult(MutationCode.TABLES_EXIST_ON_SCHEMA, schema,
                        EnvironmentEdgeManager.currentTimeMillis());
            }
            invalidateList.add(new ImmutableBytesPtr(key));
            return new MetaDataMutationResult(MutationCode.SCHEMA_ALREADY_EXISTS, schema,
                    EnvironmentEdgeManager.currentTimeMillis());
        }
        return new MetaDataMutationResult(MutationCode.SCHEMA_NOT_FOUND, EnvironmentEdgeManager.currentTimeMillis(),
                null);

    }

    /**
     * Perform atomic mutations on rows within a region
     *
     * @param accessCheckEnabled Use the login user to mutate rows if enabled
     * @param region Region containing rows to be mutated
     * @param mutations List of mutations for rows that must be contained within the region
     * @param rowsToLock Rows to lock
     * @param nonceGroup Optional nonce group of the operation
     * @param nonce Optional nonce of the operation
     * @throws IOException
     */
    static void mutateRowsWithLocks(final boolean accessCheckEnabled, final Region region,
            final List<Mutation> mutations, final Set<byte[]> rowsToLock, final long nonceGroup,
            final long nonce) throws IOException {
        // We need to mutate SYSTEM.CATALOG or SYSTEM.CHILD_LINK with HBase/login user
        // if access is enabled.
        if (accessCheckEnabled) {
            User.runAsLoginUser(new PrivilegedExceptionAction<Void>() {
                @Override
                public Void run() throws Exception {
                    final RpcCall rpcContext = RpcUtil.getRpcContext();
                    // Setting RPC context as null so that user can be resetted
                    try {
                        RpcUtil.setRpcContext(null);
                        region.mutateRowsWithLocks(mutations, rowsToLock, nonceGroup, nonce);
                    } catch (Throwable e) {
                        throw new IOException(e);
                    } finally {
                        // Setting RPC context back to original context of the RPC
                        RpcUtil.setRpcContext(rpcContext);
                    }
                    return null;
                }
            });
        } else {
            region.mutateRowsWithLocks(mutations, rowsToLock, nonceGroup, nonce);
        }
    }

    private TableName getParentPhysicalTableName(PTable table) {
        return (table
                .getType() == PTableType.VIEW || (table.getType() == INDEX && table.getViewIndexId() != null))
                ? TableName.valueOf(table.getPhysicalName().getBytes())
                : table.getType() == PTableType.INDEX
                ? TableName
                .valueOf(SchemaUtil
                        .getPhysicalHBaseTableName(table.getParentSchemaName(),
                                table.getParentTableName(), table.isNamespaceMapped())
                        .getBytes())
                : TableName
                .valueOf(
                        SchemaUtil
                                .getPhysicalHBaseTableName(table.getSchemaName(),
                                        table.getTableName(), table.isNamespaceMapped())
                                .getBytes());
    }
}
