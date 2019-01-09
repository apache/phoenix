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
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.APPEND_ONLY_SCHEMA_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.ARRAY_SIZE_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.AUTO_PARTITION_SEQ_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.CLASS_NAME_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_COUNT_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_DEF_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_NAME_INDEX;
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
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.FAMILY_NAME_INDEX;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.IMMUTABLE_ROWS_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.INDEX_DISABLE_TIMESTAMP_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.INDEX_STATE_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.INDEX_TYPE_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.IS_ARRAY_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.IS_CONSTANT_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.IS_NAMESPACE_MAPPED_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.IS_ROW_TIMESTAMP_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.IS_VIEW_REFERENCED_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.JAR_PATH_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.LINK_TYPE_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.MAX_VALUE_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.MIN_VALUE_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.MULTI_TENANT_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.NULLABLE_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.NUM_ARGS_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.ORDINAL_POSITION_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.PK_NAME_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.RETURN_TYPE_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SALT_BUCKETS_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SCHEMA_NAME_INDEX;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SORT_ORDER_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.STORAGE_SCHEME_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.STORE_NULLS_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_NAME_INDEX;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_SEQ_NUM_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_TYPE_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TENANT_ID_INDEX;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TRANSACTIONAL_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TRANSACTION_PROVIDER_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TYPE_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.UPDATE_CACHE_FREQUENCY_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.USE_STATS_FOR_PARALLELIZATION_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.VIEW_CONSTANT_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.VIEW_INDEX_ID_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.VIEW_STATEMENT_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.VIEW_TYPE_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.VIEW_INDEX_ID_DATA_TYPE_BYTES;
import static org.apache.phoenix.query.QueryConstants.DIVERGED_VIEW_BASE_COLUMN_COUNT;
import static org.apache.phoenix.schema.PTableType.INDEX;
import static org.apache.phoenix.schema.PTableType.TABLE;
import static org.apache.phoenix.schema.PTableImpl.getColumnsToClone;
import static org.apache.phoenix.util.SchemaUtil.getVarCharLength;
import static org.apache.phoenix.util.SchemaUtil.getVarChars;

import java.io.IOException;
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
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Properties;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
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
import org.apache.phoenix.compile.ColumnNameTrackingExpressionCompiler;
import org.apache.phoenix.compile.ColumnResolver;
import org.apache.phoenix.compile.FromCompiler;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.compile.ScanRanges;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.compile.WhereCompiler;
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
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.KeyValueColumnExpression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.expression.ProjectedColumnExpression;
import org.apache.phoenix.expression.RowKeyColumnExpression;
import org.apache.phoenix.expression.visitor.StatelessTraverseAllExpressionVisitor;
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
import org.apache.phoenix.metrics.Metrics;
import org.apache.phoenix.parse.DropTableStatement;
import org.apache.phoenix.parse.LiteralParseNode;
import org.apache.phoenix.parse.PFunction;
import org.apache.phoenix.parse.PFunction.FunctionArgument;
import org.apache.phoenix.parse.PSchema;
import org.apache.phoenix.parse.ParseNode;
import org.apache.phoenix.parse.SQLParser;
import org.apache.phoenix.protobuf.ProtobufUtil;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.ColumnFamilyNotFoundException;
import org.apache.phoenix.schema.ColumnNotFoundException;
import org.apache.phoenix.schema.ColumnRef;
import org.apache.phoenix.schema.MetaDataClient;
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
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.ParentTableNotFoundException;
import org.apache.phoenix.schema.RowKeySchema;
import org.apache.phoenix.schema.SaltingUtil;
import org.apache.phoenix.schema.SequenceAllocation;
import org.apache.phoenix.schema.SequenceAlreadyExistsException;
import org.apache.phoenix.schema.SequenceKey;
import org.apache.phoenix.schema.SequenceNotFoundException;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.types.PBinary;
import org.apache.phoenix.schema.types.PBoolean;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.schema.types.PSmallint;
import org.apache.phoenix.schema.types.PTinyint;
import org.apache.phoenix.schema.types.PVarbinary;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.trace.util.Tracing;
import org.apache.phoenix.transaction.TransactionFactory;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.EncodedColumnsUtil;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.PhoenixKeyValueUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.ServerUtil;
import org.apache.phoenix.util.UpgradeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.Cache;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
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
 * parent->child links are stored in a separate SYSTEM.CHILD_LINK table.
 * Metadata for all tables/views/indexes in the same schema are stored in a
 * single region which is enforced using the {@link MetaDataSplitPolicy}.
 * 
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
 * 
 * We only allow mutations to the latest version of a Phoenix table (i.e. the
 * timeStamp must be increasing). For adding/dropping columns we use a sequence
 * number on the table to ensure that the client has the latest version.
 *
 * @since 0.1
 */
@SuppressWarnings("deprecation")
public class MetaDataEndpointImpl extends MetaDataProtocol implements RegionCoprocessor {
    private static final Logger logger = LoggerFactory.getLogger(MetaDataEndpointImpl.class);

    // Column to track tables that have been upgraded based on PHOENIX-2067
    public static final String ROW_KEY_ORDER_OPTIMIZABLE = "ROW_KEY_ORDER_OPTIMIZABLE";
    public static final byte[] ROW_KEY_ORDER_OPTIMIZABLE_BYTES = Bytes.toBytes(ROW_KEY_ORDER_OPTIMIZABLE);

    private static final byte[] CHILD_TABLE_BYTES = new byte[] {PTable.LinkType.CHILD_TABLE.getSerializedValue()};
    private static final byte[] PHYSICAL_TABLE_BYTES =
            new byte[] { PTable.LinkType.PHYSICAL_TABLE.getSerializedValue() };

    // KeyValues for Table
    private static final KeyValue TABLE_TYPE_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, TABLE_TYPE_BYTES);
    private static final KeyValue TABLE_SEQ_NUM_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, TABLE_SEQ_NUM_BYTES);
    private static final KeyValue COLUMN_COUNT_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, COLUMN_COUNT_BYTES);
    private static final KeyValue SALT_BUCKETS_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, SALT_BUCKETS_BYTES);
    private static final KeyValue PK_NAME_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, PK_NAME_BYTES);
    private static final KeyValue DATA_TABLE_NAME_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, DATA_TABLE_NAME_BYTES);
    private static final KeyValue INDEX_STATE_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, INDEX_STATE_BYTES);
    private static final KeyValue IMMUTABLE_ROWS_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, IMMUTABLE_ROWS_BYTES);
    private static final KeyValue VIEW_EXPRESSION_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, VIEW_STATEMENT_BYTES);
    private static final KeyValue DEFAULT_COLUMN_FAMILY_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, DEFAULT_COLUMN_FAMILY_NAME_BYTES);
    private static final KeyValue DISABLE_WAL_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, DISABLE_WAL_BYTES);
    private static final KeyValue MULTI_TENANT_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, MULTI_TENANT_BYTES);
    private static final KeyValue VIEW_TYPE_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, VIEW_TYPE_BYTES);
    private static final KeyValue VIEW_INDEX_ID_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, VIEW_INDEX_ID_BYTES);
    /**
     * A designator for choosing the right type for viewIndex (Short vs Long) to be backward compatible.
     * **/
    private static final KeyValue VIEW_INDEX_ID_DATA_TYPE_BYTES_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, VIEW_INDEX_ID_DATA_TYPE_BYTES);
    private static final KeyValue INDEX_TYPE_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, INDEX_TYPE_BYTES);
    private static final KeyValue INDEX_DISABLE_TIMESTAMP_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, INDEX_DISABLE_TIMESTAMP_BYTES);
    private static final KeyValue STORE_NULLS_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, STORE_NULLS_BYTES);
    private static final KeyValue EMPTY_KEYVALUE_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, QueryConstants.EMPTY_COLUMN_BYTES);
    private static final KeyValue BASE_COLUMN_COUNT_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, PhoenixDatabaseMetaData.BASE_COLUMN_COUNT_BYTES);
    private static final KeyValue ROW_KEY_ORDER_OPTIMIZABLE_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, ROW_KEY_ORDER_OPTIMIZABLE_BYTES);
    private static final KeyValue TRANSACTIONAL_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, TRANSACTIONAL_BYTES);
    private static final KeyValue TRANSACTION_PROVIDER_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, TRANSACTION_PROVIDER_BYTES);
    private static final KeyValue UPDATE_CACHE_FREQUENCY_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, UPDATE_CACHE_FREQUENCY_BYTES);
    private static final KeyValue IS_NAMESPACE_MAPPED_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY,
            TABLE_FAMILY_BYTES, IS_NAMESPACE_MAPPED_BYTES);
    private static final Cell AUTO_PARTITION_SEQ_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, AUTO_PARTITION_SEQ_BYTES);
    private static final Cell APPEND_ONLY_SCHEMA_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, APPEND_ONLY_SCHEMA_BYTES);
    private static final Cell STORAGE_SCHEME_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, STORAGE_SCHEME_BYTES);
    private static final Cell ENCODING_SCHEME_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, ENCODING_SCHEME_BYTES);
    private static final Cell USE_STATS_FOR_PARALLELIZATION_KV = createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, USE_STATS_FOR_PARALLELIZATION_BYTES);
    
    private static final List<Cell> TABLE_KV_COLUMNS = Arrays.<Cell>asList(
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
            UPDATE_CACHE_FREQUENCY_KV,
            IS_NAMESPACE_MAPPED_KV,
            AUTO_PARTITION_SEQ_KV,
            APPEND_ONLY_SCHEMA_KV,
            STORAGE_SCHEME_KV,
            ENCODING_SCHEME_KV,
            USE_STATS_FOR_PARALLELIZATION_KV
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

    private static final List<KeyValue> COLUMN_KV_COLUMNS = Arrays.<KeyValue>asList(
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
        Collections.sort(COLUMN_KV_COLUMNS, KeyValue.COMPARATOR);
    }
    private static final Cell QUALIFIER_COUNTER_KV = KeyValueUtil.createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, TABLE_FAMILY_BYTES, COLUMN_QUALIFIER_COUNTER_BYTES);
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
    // Used to add a tag to a cell when a view modifies a table property to indicate that this
    // property should not be derived from the base table
    private static final byte[] VIEW_MODIFIED_PROPERTY_BYTES = Bytes.toBytes(1);

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
    
    private static PName newPName(byte[] buffer) {
        return buffer==null ? null : newPName(buffer, 0, buffer.length);
    }

    private static PName newPName(byte[] keyBuffer, int keyOffset, int keyLength) {
        if (keyLength <= 0) {
            return null;
        }
        int length = getVarCharLength(keyBuffer, keyOffset, keyLength);
        return PNameFactory.newName(keyBuffer, keyOffset, length);
    }

    private RegionCoprocessorEnvironment env;

    private PhoenixMetaDataCoprocessorHost phoenixAccessCoprocessorHost;
    private boolean accessCheckEnabled;
    private boolean blockWriteRebuildIndex;
    private int maxIndexesPerTable;
    private boolean isTablesMappingEnabled;

    // this flag denotes that we will continue to write parent table column metadata while creating
    // a child view and also block metadata changes that were previously propagated to children
    // before 4.15, so that we can rollback the upgrade to 4.15 if required
    private boolean allowSystemCatalogRollback;

    /**
     * Stores a reference to the coprocessor environment provided by the
     * {@link org.apache.hadoop.hbase.regionserver.RegionCoprocessorHost} from the region where this
     * coprocessor is loaded. Since this is a coprocessor endpoint, it always expects to be loaded
     * on a table region, so always expects this to be an instance of
     * {@link RegionCoprocessorEnvironment}.
     * @param env the environment provided by the coprocessor host
     * @throws IOException if the provided environment is not an instance of
     *             {@code RegionCoprocessorEnvironment}
     */
    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
        if (env instanceof RegionCoprocessorEnvironment) {
            this.env = (RegionCoprocessorEnvironment) env;
        } else {
            throw new CoprocessorException("Must be loaded on a table region!");
        }
        
        phoenixAccessCoprocessorHost = new PhoenixMetaDataCoprocessorHost(this.env);
        Configuration config = env.getConfiguration();
        this.accessCheckEnabled = config.getBoolean(QueryServices.PHOENIX_ACLS_ENABLED,
                QueryServicesOptions.DEFAULT_PHOENIX_ACLS_ENABLED);
        this.blockWriteRebuildIndex  = config.getBoolean(QueryServices.INDEX_FAILURE_BLOCK_WRITE,
                QueryServicesOptions.DEFAULT_INDEX_FAILURE_BLOCK_WRITE);
        this.maxIndexesPerTable = config.getInt(QueryServices.MAX_INDEXES_PER_TABLE,
                    QueryServicesOptions.DEFAULT_MAX_INDEXES_PER_TABLE);
        this.isTablesMappingEnabled = SchemaUtil.isNamespaceMappingEnabled(PTableType.TABLE,
                new ReadOnlyProps(config.iterator()));
        this.allowSystemCatalogRollback = config.getBoolean(QueryServices.ALLOW_SPLITTABLE_SYSTEM_CATALOG_ROLLBACK,
                QueryServicesOptions.DEFAULT_ALLOW_SPLITTABLE_SYSTEM_CATALOG_ROLLBACK);

        logger.info("Starting Tracing-Metrics Systems");
        // Start the phoenix trace collection
        Tracing.addTraceMetricsSource();
        Metrics.ensureConfigured();
    }
    
    @Override
    public void stop(CoprocessorEnvironment env) throws IOException {
        // nothing to do
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
                        null, request.getClientVersion(), request.getSkipAddingIndexes(),
                        request.getSkipAddingParentColumns(),
                        PTableImpl.createFromProto(request.getLockedAncestorTable()));
            if (table == null) {
                builder.setReturnCode(MetaDataProtos.MutationCode.TABLE_NOT_FOUND);
                builder.setMutationTime(currentTime);
                done.run(builder.build());
                return;
            }
            getCoprocessorHost().preGetTable(Bytes.toString(tenantId), SchemaUtil.getTableName(schemaName, tableName),
                    TableName.valueOf(table.getPhysicalName().getBytes()));

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
            if (table.getType()!=PTableType.TABLE || table.getTimeStamp() != tableTimeStamp) {
                builder.setTable(PTableImpl.toProto(table));
            }
            done.run(builder.build());
        } catch (Throwable t) {
            logger.error("getTable failed", t);
            ProtobufUtil.setControllerException(controller,
                ServerUtil.createIOException(SchemaUtil.getTableName(schemaName, tableName), t));
        }
    }

    /**
     * Used to add the columns present the ancestor hierarchy to the PTable of the given view or
     * view index
     * @param table PTable of the view or view index
     * @param skipAddingIndexes if true the returned PTable won't include indexes
     * @param skipAddingParentColumns if true the returned PTable won't include columns derived from
     *            ancestor tables
     * @param lockedAncestorTable ancestor table table that is being mutated (as we won't be able to
     *            resolve this table as its locked)
     */
    private Pair<PTable, MetaDataProtos.MutationCode> combineColumns(PTable table, long timestamp,
            int clientVersion, boolean skipAddingIndexes, boolean skipAddingParentColumns,
            PTable lockedAncestorTable) throws SQLException, IOException {
        boolean hasIndexId = table.getViewIndexId() != null;
        if (table.getType() != PTableType.VIEW && !hasIndexId) {
            return new Pair<PTable, MetaDataProtos.MutationCode>(table,
                    MetaDataProtos.MutationCode.TABLE_ALREADY_EXISTS);
        }
        if (!skipAddingParentColumns) {
            table =
                    addDerivedColumnsFromAncestors(table, timestamp, clientVersion,
                        lockedAncestorTable);
            if (table==null) {
                return new Pair<PTable, MetaDataProtos.MutationCode>(table,
                        MetaDataProtos.MutationCode.TABLE_NOT_FOUND);
            }
            // we need to resolve the indexes of views (to get ensure they also have all the columns
            // derived from their ancestors) 
            if (!skipAddingIndexes && !table.getIndexes().isEmpty()) {
                List<PTable> indexes = Lists.newArrayListWithExpectedSize(table.getIndexes().size());
                for (PTable index : table.getIndexes()) {
                    byte[] tenantIdBytes =
                            index.getTenantId() == null ? ByteUtil.EMPTY_BYTE_ARRAY
                                    : index.getTenantId().getBytes();
                    PTable latestIndex =
                            doGetTable(tenantIdBytes, index.getSchemaName().getBytes(),
                                index.getTableName().getBytes(), timestamp, null, clientVersion, true,
                                false, lockedAncestorTable);
                    if (latestIndex == null) {
                        throw new TableNotFoundException(
                                "Could not find index table while combining columns "
                                        + index.getTableName().getString() + " with tenant id "
                                        + index.getTenantId());
                    }
                    indexes.add(latestIndex);
                }
                table = PTableImpl.builderWithColumns(table, getColumnsToClone(table))
                        .setIndexes(indexes == null ? Collections.emptyList() : indexes)
                        .build();
            }
        }
        
        MetaDataProtos.MutationCode mutationCode =
                table != null ? MetaDataProtos.MutationCode.TABLE_ALREADY_EXISTS
                        : MetaDataProtos.MutationCode.TABLE_NOT_FOUND;
        return new Pair<PTable, MetaDataProtos.MutationCode>(table, mutationCode);
    }

    
    private PTable addDerivedColumnsFromAncestors(PTable table, long timestamp,
            int clientVersion, PTable lockedAncestorTable) throws IOException, SQLException, TableNotFoundException {
        // combine columns for view and view indexes
        byte[] tenantId =
                table.getTenantId() != null ? table.getTenantId().getBytes()
                        : ByteUtil.EMPTY_BYTE_ARRAY;
        byte[] schemaName = table.getSchemaName().getBytes();
        byte[] tableName = table.getTableName().getBytes();
        String fullTableName = SchemaUtil.getTableName(table.getSchemaName().getString(),
                table.getTableName().getString());
        boolean hasIndexId = table.getViewIndexId() != null;
        boolean isSalted = table.getBucketNum() != null;
        if (table.getType() != PTableType.VIEW && !hasIndexId) {
            return table;
        }
        boolean isDiverged = isDivergedView(table);
        // here you combine columns from the parent tables the logic is as follows, if the PColumn
        // is in the EXCLUDED_COLUMNS remove it, otherwise priority of keeping duplicate columns is
        // child -> parent
        List<TableInfo> ancestorList = Lists.newArrayList();
        TableViewFinderResult viewFinderResult = new TableViewFinderResult();
        if (PTableType.VIEW == table.getType()) {
            findAncestorViews(tenantId, schemaName, tableName, viewFinderResult,
                table.isNamespaceMapped());
        } else { // is a view index
            findAncestorViewsOfIndex(tenantId, schemaName, tableName, viewFinderResult,
                table.isNamespaceMapped());
        }
        if (viewFinderResult.getLinks().isEmpty()) {
            // no need to combine columns for local indexes on regular tables
            return table;
        }
        for (TableInfo viewInfo : viewFinderResult.getLinks()) {
            ancestorList.add(viewInfo);
        }
        List<PColumn> allColumns = Lists.newArrayList();
        List<PColumn> excludedColumns = Lists.newArrayList();
        // add my own columns first in reverse order
        List<PColumn> myColumns = table.getColumns();
        // skip salted column as it will be created automatically
        myColumns = myColumns.subList(isSalted ? 1 : 0, myColumns.size());
        for (int i = myColumns.size() - 1; i >= 0; i--) {
            PColumn pColumn = myColumns.get(i);
            if (pColumn.isExcluded()) {
                excludedColumns.add(pColumn);
            }
            allColumns.add(pColumn);
        }

        // initialize map from with indexed expression to list of required data columns
        // then remove the data columns that have not been dropped, so that we get the columns that
        // have been dropped
        Map<PColumn, List<String>> indexRequiredDroppedDataColMap =
                Maps.newHashMapWithExpectedSize(table.getColumns().size());
        if (hasIndexId) {
            int indexPosOffset = (isSalted ? 1 : 0) + (table.isMultiTenant() ? 1 : 0) + 1;
            ColumnNameTrackingExpressionCompiler expressionCompiler =
                    new ColumnNameTrackingExpressionCompiler();
            for (int i = indexPosOffset; i < table.getPKColumns().size(); i++) {
                PColumn indexColumn = table.getPKColumns().get(i);
                try {
                    expressionCompiler.reset();
                    String expressionStr = IndexUtil.getIndexColumnExpressionStr(indexColumn);
                    ParseNode parseNode = SQLParser.parseCondition(expressionStr);
                    parseNode.accept(expressionCompiler);
                    indexRequiredDroppedDataColMap.put(indexColumn,
                        Lists.newArrayList(expressionCompiler.getDataColumnNames()));
                } catch (SQLException e) {
                    throw new RuntimeException(e); // Impossible
                }
            }
        }

        // now go up from child to parent all the way to the base table:
        PTable baseTable = null;
        PTable immediateParent = null;
        long maxTableTimestamp = -1;
        int numPKCols = table.getPKColumns().size();
        for (int i = 0; i < ancestorList.size(); i++) {
            TableInfo parentTableInfo = ancestorList.get(i);
            PTable pTable;
            String fullParentTableName = SchemaUtil.getTableName(parentTableInfo.getSchemaName(),
                parentTableInfo.getTableName());
            PName parentTenantId =
                    (parentTableInfo.getTenantId() != null && parentTableInfo.getTenantId().length!=0)
                            ? PNameFactory.newName(parentTableInfo.getTenantId()) : null;
            PTableKey pTableKey = new PTableKey(parentTenantId, fullParentTableName);
            // if we already have the PTable of an ancestor that has been locked, no need to look up
            // the table
            if (lockedAncestorTable != null && lockedAncestorTable.getKey().equals(pTableKey)) {
                pTable = lockedAncestorTable;
            } else {
                // if we are currently combining columns for a view index and are looking up its
                // ancestors we do not add the indexes to the ancestor PTable (or else we end up in
                // a circular loop)
                // we also don't need to add parent columns of the ancestors as we combine columns
                // from all ancestors
                pTable =
                        doGetTable(parentTableInfo.getTenantId(), parentTableInfo.getSchemaName(),
                            parentTableInfo.getTableName(), timestamp, null, clientVersion, hasIndexId,
                            true, null);
            }
            if (pTable == null) {
                throw new ParentTableNotFoundException(parentTableInfo, fullTableName);
            } else {
                if (immediateParent == null) {
                    immediateParent = pTable;
                }
                // only combine columns for view indexes (and not local indexes on regular tables
                // which also have a viewIndexId)
                if (i == 0 && hasIndexId && pTable.getType() != PTableType.VIEW) {
                    return table;
                }
                if (TABLE.equals(pTable.getType())) {
                    baseTable = pTable;
                }
                // set the final table timestamp as the max timestamp of the view/view index or its
                // ancestors
                maxTableTimestamp = Math.max(maxTableTimestamp, pTable.getTimeStamp());
                if (hasIndexId) {
                    // add all pk columns of parent tables to indexes
                    // skip salted column as it will be added from the base table columns
                    int startIndex = pTable.getBucketNum() != null ? 1 : 0;
                    for (int index=startIndex; index<pTable.getPKColumns().size(); index++) {
                        PColumn pkColumn = pTable.getPKColumns().get(index);
                        // don't add the salt column of ancestor tables for view indexes
                        if (pkColumn.equals(SaltingUtil.SALTING_COLUMN) || pkColumn.isExcluded()) {
                            continue;
                        }
                        pkColumn = IndexUtil.getIndexPKColumn(++numPKCols, pkColumn);
                        int existingColumnIndex = allColumns.indexOf(pkColumn);
                        if (existingColumnIndex == -1) {
                            allColumns.add(0, pkColumn);
                        }
                    }
                    for (int j = 0; j < pTable.getColumns().size(); j++) {
                        PColumn tableColumn = pTable.getColumns().get(j);
                        if (tableColumn.isExcluded()) {
                            continue;
                        }
                        String dataColumnName = tableColumn.getName().getString();
                        // remove from list of columns since it has not been dropped
                        for (Entry<PColumn, List<String>> entry : indexRequiredDroppedDataColMap
                                .entrySet()) {
                            entry.getValue().remove(dataColumnName);
                        }
                    }
                } else {
                    List<PColumn> currAncestorTableCols = PTableImpl.getColumnsToClone(pTable);
                    if (currAncestorTableCols != null) {
                        // add the ancestor columns in reverse order so that the final column list
                        // contains ancestor columns and then the view columns in the right order
                        for (int j = currAncestorTableCols.size() - 1; j >= 0; j--) {
                            PColumn column = currAncestorTableCols.get(j);
                            // for diverged views we always include pk columns of the base table. We
                            // have to include these pk columns to be able to support adding pk
                            // columns to the diverged view
                            // we only include regular columns that were created before the view
                            // diverged
                            if (isDiverged && column.getFamilyName() != null
                                    && column.getTimestamp() > table.getTimeStamp()) {
                                continue;
                            }
                            // need to check if this column is in the list of excluded (dropped)
                            // columns of the view
                            int existingIndex = excludedColumns.indexOf(column);
                            if (existingIndex != -1) {
                                // if it is, only exclude the column if was created before the
                                // column was dropped in the view in order to handle the case where
                                // a base table column is dropped in a view, then dropped in the
                                // base table and then added back to the base table
                                if (column.getTimestamp() <= excludedColumns.get(existingIndex)
                                        .getTimestamp()) {
                                    continue;
                                }
                            }
                            if (column.isExcluded()) {
                                excludedColumns.add(column);
                            } else {
                                int existingColumnIndex = allColumns.indexOf(column);
                                if (existingColumnIndex != -1) {
                                    // for diverged views if the view was created before
                                    // PHOENIX-3534 the parent table columns will be present in the
                                    // view PTable (since the base column count is
                                    // QueryConstants.DIVERGED_VIEW_BASE_COLUMN_COUNT we can't
                                    // filter them out) so we always pick the parent column  
                                    // for non diverged views if the same column exists in a parent
                                    // and child, we keep the latest column
                                    PColumn existingColumn = allColumns.get(existingColumnIndex);
                                    if (isDiverged || column.getTimestamp() > existingColumn.getTimestamp()) {
                                        allColumns.remove(existingColumnIndex);
                                        allColumns.add(column);
                                    }
                                } else {
                                    allColumns.add(column);
                                }
                            }
                        }
                    }
                }
            }
        }
        // at this point indexRequiredDroppedDataColMap only contain the columns required by a view
        // index that have dropped
        for (Entry<PColumn, List<String>> entry : indexRequiredDroppedDataColMap.entrySet()) {
            if (!entry.getValue().isEmpty()) {
                PColumn indexColumnToBeDropped = entry.getKey();
                if (SchemaUtil.isPKColumn(indexColumnToBeDropped)) {
                    // if an indexed column was dropped in an ancestor then we
                    // cannot use this index an more
                    // TODO figure out a way to actually drop this view index
                    return null;
                } else {
                    allColumns.remove(indexColumnToBeDropped);
                }
            }
        }
        // remove the excluded columns if the timestamp is newer than the added column
        for (PColumn excludedColumn : excludedColumns) {
            int index = allColumns.indexOf(excludedColumn);
            if (index != -1) {
                if (allColumns.get(index).getTimestamp() <= excludedColumn.getTimestamp()) {
                    allColumns.remove(excludedColumn);
                }
            }
        }
        List<PColumn> columnsToAdd = Lists.newArrayList();
        int position = isSalted ? 1 : 0;
        // allColumns contains the columns in the reverse order
        for (int i = allColumns.size() - 1; i >= 0; i--) {
            PColumn column = allColumns.get(i);
            if (table.getColumns().contains(column)) {
                // for views this column is not derived from an ancestor
                columnsToAdd.add(new PColumnImpl(column, position++));
            } else {
                columnsToAdd.add(new PColumnImpl(column, true, position++));
            }
        }
        // we need to include the salt column when setting the base table column count in order to
        // maintain b/w compatibility
        int baseTableColumnCount =
                isDiverged ? QueryConstants.DIVERGED_VIEW_BASE_COLUMN_COUNT
                        : columnsToAdd.size() - myColumns.size() + (isSalted ? 1 : 0);

        // Inherit view-modifiable properties from the parent table/view if the current view has
        // not previously modified this property
        Long updateCacheFreq = (table.getType() != PTableType.VIEW ||
                table.hasViewModifiedUpdateCacheFrequency()) ?
                table.getUpdateCacheFrequency() : immediateParent.getUpdateCacheFrequency();
        Boolean useStatsForParallelization = (table.getType() != PTableType.VIEW ||
                table.hasViewModifiedUseStatsForParallelization()) ?
                table.useStatsForParallelization() : immediateParent.useStatsForParallelization();
        // When creating a PTable for views or view indexes, use the baseTable PTable for attributes
        // inherited from the physical base table.
        // if a TableProperty is not valid on a view we set it to the base table value
        // if a TableProperty is valid on a view and is not mutable on a view we set it to the base table value
        // if a TableProperty is valid on a view and is mutable on a view, we use the value set
        // on the view if the view had previously modified the property, otherwise we propagate the
        // value from the base table (see PHOENIX-4763)
        PTableImpl pTable = PTableImpl.builderWithColumns(table, columnsToAdd)
                .setImmutableRows(baseTable.isImmutableRows())
                .setDisableWAL(baseTable.isWALDisabled())
                .setMultiTenant(baseTable.isMultiTenant())
                .setStoreNulls(baseTable.getStoreNulls())
                .setTransactionProvider(baseTable.getTransactionProvider())
                .setAutoPartitionSeqName(baseTable.getAutoPartitionSeqName())
                .setAppendOnlySchema(baseTable.isAppendOnlySchema())
                .setImmutableStorageScheme(baseTable.getImmutableStorageScheme() == null ?
                        ImmutableStorageScheme.ONE_CELL_PER_COLUMN : baseTable.getImmutableStorageScheme())
                .setQualifierEncodingScheme(baseTable.getEncodingScheme() == null ?
                        QualifierEncodingScheme.NON_ENCODED_QUALIFIERS : baseTable.getEncodingScheme())
                .setBaseColumnCount(baseTableColumnCount)
                .setTimeStamp(maxTableTimestamp)
                .setExcludedColumns(excludedColumns == null ?
                        ImmutableList.of() : ImmutableList.copyOf(excludedColumns))
                .setUpdateCacheFrequency(updateCacheFreq)
                .setUseStatsForParallelization(useStatsForParallelization)
                .build();
        return WhereConstantParser.addViewInfoToPColumnsIfNeeded(pTable);
    }

    private PhoenixMetaDataCoprocessorHost getCoprocessorHost() {
        return phoenixAccessCoprocessorHost;
    }

    /**
     * @param skipAddingIndexes if true the PTable will not include indexes for tables or views
     * @param skipAddingParentColumns if true the PTable will not include parent columns for views
     *            or indexes
     * @param lockedAncestorTable ancestor table table that is being mutated (as we won't be able to
     *            resolve this table as its locked)
     */
    private PTable buildTable(byte[] key, ImmutableBytesPtr cacheKey, Region region,
            long clientTimeStamp, int clientVersion, boolean skipAddingIndexes,
            boolean skipAddingParentColumns, PTable lockedAncestorTable)
            throws IOException, SQLException {
        Scan scan = MetaDataUtil.newTableRowsScan(key, MIN_TABLE_TIMESTAMP, clientTimeStamp);
        Cache<ImmutableBytesPtr,PMetaDataEntity> metaDataCache = GlobalCache.getInstance(this.env).getMetaDataCache();
        PTable newTable;
        try (RegionScanner scanner = region.getScanner(scan)) {
            PTable oldTable = (PTable)metaDataCache.getIfPresent(cacheKey);
            long tableTimeStamp = oldTable == null ? MIN_TABLE_TIMESTAMP-1 : oldTable.getTimeStamp();
            newTable = getTable(scanner, clientTimeStamp, tableTimeStamp, clientVersion, skipAddingIndexes, skipAddingParentColumns);
            if (newTable != null
                    && (oldTable == null || tableTimeStamp < newTable.getTimeStamp()
                            || (blockWriteRebuildIndex && newTable.getIndexDisableTimestamp() > 0))
                    // only cache the PTable if it has the required indexes,
                    // the PTable added to the cache doesn't include parent columns as we always call 
                    // combine columns after looking up the PTable from the cache
                    && !skipAddingIndexes) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Caching table "
                            + Bytes.toStringBinary(cacheKey.get(), cacheKey.getOffset(),
                                cacheKey.getLength())
                            + " at seqNum " + newTable.getSequenceNumber()
                            + " with newer timestamp " + newTable.getTimeStamp() + " versus "
                            + tableTimeStamp);
                }
                metaDataCache.put(cacheKey, newTable);
            }
            if (newTable != null) {
                newTable = combineColumns(newTable, clientTimeStamp, clientVersion, skipAddingIndexes, skipAddingParentColumns, lockedAncestorTable).getFirst();
            }
        }
        return newTable;
    }

    private List<PFunction> buildFunctions(List<byte[]> keys, Region region,
            long clientTimeStamp, boolean isReplace, List<Mutation> deleteMutationsForReplace) throws IOException, SQLException {
        List<KeyRange> keyRanges = Lists.newArrayListWithExpectedSize(keys.size());
        for (byte[] key : keys) {
            byte[] stopKey = ByteUtil.concat(key, QueryConstants.SEPARATOR_BYTE_ARRAY);
            ByteUtil.nextKey(stopKey, stopKey.length);
            keyRanges.add(PVarbinary.INSTANCE.getKeyRange(key, true, stopKey, false));
        }
        Scan scan = new Scan();
        scan.setTimeRange(MIN_TABLE_TIMESTAMP, clientTimeStamp);
        ScanRanges scanRanges = ScanRanges.createPointLookup(keyRanges);
        scanRanges.initializeScan(scan);
        scan.setFilter(scanRanges.getSkipScanFilter());
        Cache<ImmutableBytesPtr,PMetaDataEntity> metaDataCache = GlobalCache.getInstance(this.env).getMetaDataCache();
        List<PFunction> functions = new ArrayList<PFunction>();
        PFunction function = null;
        try (RegionScanner scanner = region.getScanner(scan)) {
            for(int i = 0; i< keys.size(); i++) {
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
            keyRanges.add(PVarbinary.INSTANCE.getKeyRange(key, true, stopKey, false));
        }
        Scan scan = new Scan();
        scan.setTimeRange(MIN_TABLE_TIMESTAMP, clientTimeStamp);
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
                if (schema == null) { return null; }
                metaDataCache.put(cacheKey, schema);
                schemas.add(schema);
            }
            return schemas;
        }
    }

    private void addIndexToTable(PName tenantId, PName schemaName, PName indexName, PName tableName,
            long clientTimeStamp, List<PTable> indexes, int clientVersion, boolean skipAddingParentColumns)
            throws IOException, SQLException {
        byte[] tenantIdBytes = tenantId == null ? ByteUtil.EMPTY_BYTE_ARRAY : tenantId.getBytes();
        PTable indexTable = doGetTable(tenantIdBytes, schemaName.getBytes(), indexName.getBytes(), clientTimeStamp,
                null, clientVersion, false, skipAddingParentColumns, null);
        if (indexTable == null) {
            ServerUtil.throwIOException("Index not found", new TableNotFoundException(schemaName.getString(), indexName.getString()));
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
            boolean isRegularView) {
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
                    ordinalPositionKv.getValueOffset(), SortOrder.getDefault()) + (isSalted ? 1 : 0);;

        // Prior to PHOENIX-4766 we were sending the parent table column metadata while creating a
        // child view, now that we combine columns by resolving the parent table hierarchy we
        // don't need to include the parent table column while loading the PTable of the view
        if (isRegularView && position <= baseColumnCount) {
            return;
        }
        
        // if this column was inherited from a parent and was dropped that we create an excluded
        // column, this check is only needed to handle view metadata that was created before
        // PHOENIX-4766 where we were sending the parent table column metadata when creating a
        // childview
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
        if (maxLength == null && dataType == PBinary.INSTANCE) dataType = PVarbinary.INSTANCE;   // For
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
        String expressionStr = columnDefKv==null ? null : (String)PVarchar.INSTANCE.toObject(columnDefKv.getValueArray(), columnDefKv.getValueOffset(), columnDefKv.getValueLength());
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
                        results.get(0).getTimestamp());
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

    /**
     * @param skipAddingIndexes if true the returned PTable for a table or view won't include indexes
     * @param skipAddingParentColumns if true the returned PTable won't include inherited columns
     * @return PTable 
     */
    private PTable getTable(RegionScanner scanner, long clientTimeStamp, long tableTimeStamp,
            int clientVersion, boolean skipAddingIndexes, boolean skipAddingParentColumns)
        throws IOException, SQLException {
        List<Cell> results = Lists.newArrayList();
        scanner.next(results);
        if (results.isEmpty()) {
            return null;
        }
        Cell[] tableKeyValues = new Cell[TABLE_KV_COLUMNS.size()];
        Cell[] colKeyValues = new Cell[COLUMN_KV_COLUMNS.size()];

        // Create PTable based on KeyValues from scan
        Cell keyValue = results.get(0);
        byte[] keyBuffer = keyValue.getRowArray();
        int keyLength = keyValue.getRowLength();
        int keyOffset = keyValue.getRowOffset();
        PName tenantId = newPName(keyBuffer, keyOffset, keyLength);
        int tenantIdLength = (tenantId == null) ? 0 : tenantId.getBytes().length;
        if (tenantIdLength == 0) {
            tenantId = null;
        }
        PName schemaName = newPName(keyBuffer, keyOffset+tenantIdLength+1, keyLength);
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
        // If we already have a table newer than the one we just found and
        // the client timestamp is less that the existing table time stamp,
        // bump up the timeStamp to right before the client time stamp, since
        // we know it can't possibly change.
        long timeStamp = keyValue.getTimestamp();
        // long timeStamp = tableTimeStamp > keyValue.getTimestamp() &&
        // clientTimeStamp < tableTimeStamp
        // ? clientTimeStamp-1
        // : keyValue.getTimestamp();

        int i = 0;
        int j = 0;
        while (i < results.size() && j < TABLE_KV_COLUMNS.size()) {
            Cell kv = results.get(i);
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
            throw new IllegalStateException(
                    "Didn't find expected key values for table row in metadata row");
        }

        Cell tableTypeKv = tableKeyValues[TABLE_TYPE_INDEX];
        PTableType tableType =
                PTableType
                        .fromSerializedValue(tableTypeKv.getValueArray()[tableTypeKv.getValueOffset()]);
        Cell tableSeqNumKv = tableKeyValues[TABLE_SEQ_NUM_INDEX];
        long tableSeqNum =
            PLong.INSTANCE.getCodec().decodeLong(tableSeqNumKv.getValueArray(),
                    tableSeqNumKv.getValueOffset(), SortOrder.getDefault());
        Cell columnCountKv = tableKeyValues[COLUMN_COUNT_INDEX];
        int columnCount =
            PInteger.INSTANCE.getCodec().decodeInt(columnCountKv.getValueArray(),
                    columnCountKv.getValueOffset(), SortOrder.getDefault());
        Cell pkNameKv = tableKeyValues[PK_NAME_INDEX];
        PName pkName =
                pkNameKv != null ? newPName(pkNameKv.getValueArray(), pkNameKv.getValueOffset(),
                    pkNameKv.getValueLength()) : null;
        Cell saltBucketNumKv = tableKeyValues[SALT_BUCKETS_INDEX];
        Integer saltBucketNum =
                saltBucketNumKv != null ? (Integer) PInteger.INSTANCE.getCodec().decodeInt(
                    saltBucketNumKv.getValueArray(), saltBucketNumKv.getValueOffset(), SortOrder.getDefault()) : null;
        if (saltBucketNum != null && saltBucketNum.intValue() == 0) {
            saltBucketNum = null; // Zero salt buckets means not salted
        }
        Cell dataTableNameKv = tableKeyValues[DATA_TABLE_NAME_INDEX];
        PName dataTableName =
                dataTableNameKv != null ? newPName(dataTableNameKv.getValueArray(),
                    dataTableNameKv.getValueOffset(), dataTableNameKv.getValueLength()) : null;
        Cell indexStateKv = tableKeyValues[INDEX_STATE_INDEX];
        PIndexState indexState =
                indexStateKv == null ? null : PIndexState.fromSerializedValue(indexStateKv
                        .getValueArray()[indexStateKv.getValueOffset()]);

        Cell immutableRowsKv = tableKeyValues[IMMUTABLE_ROWS_INDEX];
        boolean isImmutableRows =
                immutableRowsKv == null ? false : (Boolean) PBoolean.INSTANCE.toObject(
                    immutableRowsKv.getValueArray(), immutableRowsKv.getValueOffset(),
                    immutableRowsKv.getValueLength());
        Cell defaultFamilyNameKv = tableKeyValues[DEFAULT_COLUMN_FAMILY_INDEX];
        PName defaultFamilyName = defaultFamilyNameKv != null ? newPName(defaultFamilyNameKv.getValueArray(), defaultFamilyNameKv.getValueOffset(), defaultFamilyNameKv.getValueLength()) : null;
        Cell viewStatementKv = tableKeyValues[VIEW_STATEMENT_INDEX];
        String viewStatement = viewStatementKv != null ? (String) PVarchar.INSTANCE.toObject(viewStatementKv.getValueArray(), viewStatementKv.getValueOffset(),
                viewStatementKv.getValueLength()) : null;
        Cell disableWALKv = tableKeyValues[DISABLE_WAL_INDEX];
        boolean disableWAL = disableWALKv == null ? PTable.DEFAULT_DISABLE_WAL : Boolean.TRUE.equals(
            PBoolean.INSTANCE.toObject(disableWALKv.getValueArray(), disableWALKv.getValueOffset(), disableWALKv.getValueLength()));
        Cell multiTenantKv = tableKeyValues[MULTI_TENANT_INDEX];
        boolean multiTenant = multiTenantKv == null ? false : Boolean.TRUE.equals(PBoolean.INSTANCE.toObject(multiTenantKv.getValueArray(), multiTenantKv.getValueOffset(), multiTenantKv.getValueLength()));
        Cell storeNullsKv = tableKeyValues[STORE_NULLS_INDEX];
        boolean storeNulls = storeNullsKv == null ? false : Boolean.TRUE.equals(PBoolean.INSTANCE.toObject(storeNullsKv.getValueArray(), storeNullsKv.getValueOffset(), storeNullsKv.getValueLength()));
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
                transactionProvider = TransactionFactory.Provider.TEPHRA;
            }
        } else {
            transactionProvider = TransactionFactory.Provider.fromCode(
                    PTinyint.INSTANCE.getCodec().decodeByte(
                            transactionProviderKv.getValueArray(),
                            transactionProviderKv.getValueOffset(), 
                            SortOrder.getDefault()));
        }
        Cell viewTypeKv = tableKeyValues[VIEW_TYPE_INDEX];
        ViewType viewType = viewTypeKv == null ? null : ViewType.fromSerializedValue(viewTypeKv.getValueArray()[viewTypeKv.getValueOffset()]);
        PDataType viewIndexIdType = getViewIndexIdType(tableKeyValues);
        Long viewIndexId = getViewIndexId(tableKeyValues, viewIndexIdType);
        Cell indexTypeKv = tableKeyValues[INDEX_TYPE_INDEX];
        IndexType indexType = indexTypeKv == null ? null : IndexType.fromSerializedValue(indexTypeKv.getValueArray()[indexTypeKv.getValueOffset()]);
        Cell baseColumnCountKv = tableKeyValues[BASE_COLUMN_COUNT_INDEX];
        int baseColumnCount = baseColumnCountKv == null ? 0 : PInteger.INSTANCE.getCodec().decodeInt(baseColumnCountKv.getValueArray(),
            baseColumnCountKv.getValueOffset(), SortOrder.getDefault());
        Cell rowKeyOrderOptimizableKv = tableKeyValues[ROW_KEY_ORDER_OPTIMIZABLE_INDEX];
        boolean rowKeyOrderOptimizable = rowKeyOrderOptimizableKv == null ? false : Boolean.TRUE.equals(PBoolean.INSTANCE.toObject(rowKeyOrderOptimizableKv.getValueArray(), rowKeyOrderOptimizableKv.getValueOffset(), rowKeyOrderOptimizableKv.getValueLength()));
        Cell updateCacheFrequencyKv = tableKeyValues[UPDATE_CACHE_FREQUENCY_INDEX];
        long updateCacheFrequency = updateCacheFrequencyKv == null ? 0 :
            PLong.INSTANCE.getCodec().decodeLong(updateCacheFrequencyKv.getValueArray(),
                    updateCacheFrequencyKv.getValueOffset(), SortOrder.getDefault());

        // Check the cell tag to see whether the view has modified this property
        byte[] tagUpdateCacheFreq = (updateCacheFrequencyKv == null) ? HConstants.EMPTY_BYTE_ARRAY :
                TagUtil.concatTags(HConstants.EMPTY_BYTE_ARRAY, updateCacheFrequencyKv);
        boolean viewModifiedUpdateCacheFrequency = (PTableType.VIEW.equals(tableType)) &&
                Bytes.contains(tagUpdateCacheFreq, VIEW_MODIFIED_PROPERTY_BYTES);
        Cell indexDisableTimestampKv = tableKeyValues[INDEX_DISABLE_TIMESTAMP];
        long indexDisableTimestamp = indexDisableTimestampKv == null ? 0L : PLong.INSTANCE.getCodec().decodeLong(indexDisableTimestampKv.getValueArray(),
                indexDisableTimestampKv.getValueOffset(), SortOrder.getDefault());
        Cell isNamespaceMappedKv = tableKeyValues[IS_NAMESPACE_MAPPED_INDEX];
        boolean isNamespaceMapped = isNamespaceMappedKv == null ? false
                : Boolean.TRUE.equals(PBoolean.INSTANCE.toObject(isNamespaceMappedKv.getValueArray(),
                        isNamespaceMappedKv.getValueOffset(), isNamespaceMappedKv.getValueLength()));
        Cell autoPartitionSeqKv = tableKeyValues[AUTO_PARTITION_SEQ_INDEX];
        String autoPartitionSeq = autoPartitionSeqKv != null ? (String) PVarchar.INSTANCE.toObject(autoPartitionSeqKv.getValueArray(), autoPartitionSeqKv.getValueOffset(),
            autoPartitionSeqKv.getValueLength()) : null;
        Cell isAppendOnlySchemaKv = tableKeyValues[APPEND_ONLY_SCHEMA_INDEX];
        boolean isAppendOnlySchema = isAppendOnlySchemaKv == null ? false
                : Boolean.TRUE.equals(PBoolean.INSTANCE.toObject(isAppendOnlySchemaKv.getValueArray(),
                    isAppendOnlySchemaKv.getValueOffset(), isAppendOnlySchemaKv.getValueLength()));
        Cell storageSchemeKv = tableKeyValues[STORAGE_SCHEME_INDEX];
        //TODO: change this once we start having other values for storage schemes
        ImmutableStorageScheme storageScheme = storageSchemeKv == null ? ImmutableStorageScheme.ONE_CELL_PER_COLUMN : ImmutableStorageScheme
                .fromSerializedValue((byte)PTinyint.INSTANCE.toObject(storageSchemeKv.getValueArray(),
                        storageSchemeKv.getValueOffset(), storageSchemeKv.getValueLength()));
        Cell encodingSchemeKv = tableKeyValues[QUALIFIER_ENCODING_SCHEME_INDEX];
        QualifierEncodingScheme encodingScheme = encodingSchemeKv == null ? QualifierEncodingScheme.NON_ENCODED_QUALIFIERS : QualifierEncodingScheme
                .fromSerializedValue((byte)PTinyint.INSTANCE.toObject(encodingSchemeKv.getValueArray(),
                    encodingSchemeKv.getValueOffset(), encodingSchemeKv.getValueLength()));
        Cell useStatsForParallelizationKv = tableKeyValues[USE_STATS_FOR_PARALLELIZATION_INDEX];
        Boolean useStatsForParallelization = useStatsForParallelizationKv == null ? null : Boolean.TRUE.equals(PBoolean.INSTANCE.toObject(useStatsForParallelizationKv.getValueArray(), useStatsForParallelizationKv.getValueOffset(), useStatsForParallelizationKv.getValueLength()));

        // Check the cell tag to see whether the view has modified this property
        byte[] tagUseStatsForParallelization = (useStatsForParallelizationKv == null) ?
                HConstants.EMPTY_BYTE_ARRAY :
                TagUtil.concatTags(HConstants.EMPTY_BYTE_ARRAY, useStatsForParallelizationKv);
        boolean viewModifiedUseStatsForParallelization = (PTableType.VIEW.equals(tableType)) &&
                Bytes.contains(tagUseStatsForParallelization, VIEW_MODIFIED_PROPERTY_BYTES);
        
        List<PColumn> columns = Lists.newArrayListWithExpectedSize(columnCount);
        List<PTable> indexes = Lists.newArrayList();
        List<PName> physicalTables = Lists.newArrayList();
        PName parentTableName = tableType == INDEX ? dataTableName : null;
        PName parentSchemaName = tableType == INDEX ? schemaName : null;
        EncodedCQCounter cqCounter =
                (!EncodedColumnsUtil.usesEncodedColumnNames(encodingScheme) || tableType == PTableType.VIEW) ? PTable.EncodedCQCounter.NULL_COUNTER
                        : new EncodedCQCounter();
        boolean isRegularView = (tableType == PTableType.VIEW && viewType!=ViewType.MAPPED);
        while (true) {
          results.clear();
          scanner.next(results);
          if (results.isEmpty()) {
              break;
          }
          Cell colKv = results.get(LINK_TYPE_INDEX);
          int colKeyLength = colKv.getRowLength();
          PName colName = newPName(colKv.getRowArray(), colKv.getRowOffset() + offset, colKeyLength-offset);
          int colKeyOffset = offset + colName.getBytes().length + 1;
          PName famName = newPName(colKv.getRowArray(), colKv.getRowOffset() + colKeyOffset, colKeyLength-colKeyOffset);
          if (isQualifierCounterKV(colKv)) {
              Integer value = PInteger.INSTANCE.getCodec().decodeInt(colKv.getValueArray(), colKv.getValueOffset(), SortOrder.ASC);
              cqCounter.setValue(famName.getString(), value);
          } else if (Bytes.compareTo(LINK_TYPE_BYTES, 0, LINK_TYPE_BYTES.length, colKv.getQualifierArray(), colKv.getQualifierOffset(), colKv.getQualifierLength())==0) {
              LinkType linkType = LinkType.fromSerializedValue(colKv.getValueArray()[colKv.getValueOffset()]);
              if (linkType == LinkType.INDEX_TABLE && !skipAddingIndexes) {
                  addIndexToTable(tenantId, schemaName, famName, tableName, clientTimeStamp, indexes, clientVersion, skipAddingParentColumns);
              } else if (linkType == LinkType.PHYSICAL_TABLE) {
                  physicalTables.add(famName);
              } else if (linkType == LinkType.PARENT_TABLE) {
                  parentTableName = PNameFactory.newName(SchemaUtil.getTableNameFromFullName(famName.getBytes()));
                  parentSchemaName = PNameFactory.newName(SchemaUtil.getSchemaNameFromFullName(famName.getBytes()));
              } else if (linkType == LinkType.EXCLUDED_COLUMN) {
                  // add the excludedColumn
                  addExcludedColumnToTable(columns, colName, famName, colKv.getTimestamp());
              }
          } else {
              addColumnToTable(results, colName, famName, colKeyValues, columns, saltBucketNum != null, baseColumnCount, isRegularView);
          }
        }
        // Avoid querying the stats table because we're holding the rowLock here. Issuing an RPC to a remote
        // server while holding this lock is a bad idea and likely to cause contention.
        return new PTableImpl.Builder()
                .setType(tableType)
                .setState(indexState)
                .setTimeStamp(timeStamp)
                .setIndexDisableTimestamp(indexDisableTimestamp)
                .setSequenceNumber(tableSeqNum)
                .setImmutableRows(isImmutableRows)
                .setViewStatement(viewStatement)
                .setDisableWAL(disableWAL)
                .setMultiTenant(multiTenant)
                .setStoreNulls(storeNulls)
                .setViewType(viewType)
                .setViewIndexIdType(viewIndexIdType)
                .setViewIndexId(viewIndexId)
                .setIndexType(indexType)
                .setTransactionProvider(transactionProvider)
                .setUpdateCacheFrequency(updateCacheFrequency)
                .setNamespaceMapped(isNamespaceMapped)
                .setAutoPartitionSeqName(autoPartitionSeq)
                .setAppendOnlySchema(isAppendOnlySchema)
                .setImmutableStorageScheme(storageScheme == null ?
                        ImmutableStorageScheme.ONE_CELL_PER_COLUMN : storageScheme)
                .setQualifierEncodingScheme(encodingScheme == null ?
                        QualifierEncodingScheme.NON_ENCODED_QUALIFIERS : encodingScheme)
                .setBaseColumnCount(baseColumnCount)
                .setEncodedCQCounter(cqCounter)
                .setUseStatsForParallelization(useStatsForParallelization)
                .setExcludedColumns(ImmutableList.of())
                .setTenantId(tenantId)
                .setSchemaName(schemaName)
                .setTableName(tableName)
                .setPkName(pkName)
                .setDefaultFamilyName(defaultFamilyName)
                .setRowKeyOrderOptimizable(rowKeyOrderOptimizable)
                .setBucketNum(saltBucketNum)
                .setIndexes(indexes == null ? Collections.emptyList() : indexes)
                .setParentSchemaName(parentSchemaName)
                .setParentTableName(parentTableName)
                .setPhysicalNames(physicalTables == null ?
                        ImmutableList.of() : ImmutableList.copyOf(physicalTables))
                .setViewModifiedUpdateCacheFrequency(viewModifiedUpdateCacheFrequency)
                .setViewModifiedUseStatsForParallelization(viewModifiedUseStatsForParallelization)
                .setColumns(columns)
                .build();
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
        if (results.isEmpty()) { return null; }

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
        if(isReplace) {
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
        int functionNameLength = functionName.getBytes().length+1;
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
        if(jarPathKv != null) {
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
            if(isReplace) {
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

            int argPositionOffset =  offset + typeName.getBytes().length + 1;
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
        List<Cell> results = Lists.<Cell> newArrayList();
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
        List<Cell> results = Lists.<Cell> newArrayList();
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
                return function;
            }
        }
        return null;
    }

    private PSchema buildDeletedSchema(byte[] key, ImmutableBytesPtr cacheKey, Region region, long clientTimeStamp)
            throws IOException {
        if (clientTimeStamp == HConstants.LATEST_TIMESTAMP) { return null; }

        Scan scan = MetaDataUtil.newTableRowsScan(key, clientTimeStamp, HConstants.LATEST_TIMESTAMP);
        scan.setFilter(new FirstKeyOnlyFilter());
        scan.setRaw(true);
        List<Cell> results = Lists.<Cell> newArrayList();
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
                    .setPkColumns(Collections.emptyList())
                    .setAllColumns(Collections.emptyList())
                    .setFamilyAttributes(Collections.emptyList())
                    .setRowKeySchema(RowKeySchema.EMPTY_SCHEMA)
                    .setIndexes(Collections.emptyList())
                    .setPhysicalNames(Collections.emptyList())
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
        PTable table = getTableFromCache(cacheKey, clientTimeStamp, clientVersion, false, false, null);
        // We always cache the latest version - fault in if not in cache
        if (table != null || (table = buildTable(key, cacheKey, region, asOfTimeStamp, clientVersion, false, false, null)) != null) {
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
   private PTable getTableFromCache(ImmutableBytesPtr cacheKey, long clientTimeStamp, int clientVersion, boolean skipAddingIndexes, boolean skipAddingParentColumns, PTable lockedAncestorTable) throws SQLException, IOException {
       Cache<ImmutableBytesPtr,PMetaDataEntity> metaDataCache = GlobalCache.getInstance(this.env).getMetaDataCache();
       PTable table = (PTable)metaDataCache.getIfPresent(cacheKey);
       if (table!=null)
           table = combineColumns(table, clientTimeStamp, clientVersion, skipAddingIndexes, skipAddingParentColumns, lockedAncestorTable).getFirst();
       return table;
   }

    private PFunction loadFunction(RegionCoprocessorEnvironment env, byte[] key,
            ImmutableBytesPtr cacheKey, long clientTimeStamp, long asOfTimeStamp, boolean isReplace, List<Mutation> deleteMutationsForReplace)
            throws IOException, SQLException {
            Region region = env.getRegion();
            Cache<ImmutableBytesPtr,PMetaDataEntity> metaDataCache = GlobalCache.getInstance(this.env).getMetaDataCache();
            PFunction function = (PFunction)metaDataCache.getIfPresent(cacheKey);
            // We always cache the latest version - fault in if not in cache
            if (function != null && !isReplace) {
                return function;
            }
            ArrayList<byte[]> arrayList = new ArrayList<byte[]>(1);
            arrayList.add(key);
            List<PFunction> functions = buildFunctions(arrayList, region, asOfTimeStamp, isReplace, deleteMutationsForReplace);
            if(functions != null) return functions.get(0);
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
        PSchema schema = (PSchema)metaDataCache.getIfPresent(cacheKey);
        // We always cache the latest version - fault in if not in cache
        if (schema != null) { return schema; }
        ArrayList<byte[]> arrayList = new ArrayList<byte[]>(1);
        arrayList.add(key);
        List<PSchema> schemas = buildSchemas(arrayList, region, asOfTimeStamp, cacheKey);
        if (schemas != null) return schemas.get(0);
        // if not found then check if newer schema already exists and add delete marker for timestamp
        // found
        if (schema == null
                && (schema = buildDeletedSchema(key, cacheKey, region, clientTimeStamp)) != null) { return schema; }
        return null;
    }

    /**
     * @return null if the physical table row information is not present.
     */
    private static Mutation getPhysicalTableRowForView(List<Mutation> tableMetadata, byte[][] parentTenantSchemaTableNames, byte[][] physicalSchemaTableNames) {
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
                        parentTableRow=m;
                        parentTableLinkFound = true;
                    }
                }
                if(physicalTableLinkFound && parentTableLinkFound){
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
            getSchemaTableNames(physicalTableRow,physicalSchemaTableNames);
        }
        if (parentTableLinkFound) {
            getSchemaTableNames(parentTableRow,parentTenantSchemaTableNames);   
        }
        return physicalTableRow;
    }
    
    private static void getSchemaTableNames(Mutation row, byte[][] schemaTableNames) {
        byte[][] rowKeyMetaData = new byte[5][];
        getVarChars(row.getRow(), 5, rowKeyMetaData);
        byte[] tenantId = rowKeyMetaData[PhoenixDatabaseMetaData.TENANT_ID_INDEX];
        byte[] colBytes = rowKeyMetaData[PhoenixDatabaseMetaData.COLUMN_NAME_INDEX];
        byte[] famBytes = rowKeyMetaData[PhoenixDatabaseMetaData.FAMILY_NAME_INDEX];
        if ((colBytes == null || colBytes.length == 0) && (famBytes != null && famBytes.length > 0)) {
            byte[] sName = SchemaUtil.getSchemaNameFromFullName(famBytes).getBytes();
            byte[] tName = SchemaUtil.getTableNameFromFullName(famBytes).getBytes();
            schemaTableNames[0]= tenantId;
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
            byte[] parentTenantId = null;
            byte[] parentSchemaName = null;
            byte[] parentTableName = null;
            PTableType tableType = MetaDataUtil.getTableType(tableMetadata, GenericKeyValueBuilder.INSTANCE, new ImmutableBytesWritable());
            ViewType viewType = MetaDataUtil.getViewType(tableMetadata, GenericKeyValueBuilder.INSTANCE, new ImmutableBytesWritable());

            // Load table to see if it already exists
            byte[] tableKey = SchemaUtil.getTableKey(tenantIdBytes, schemaName, tableName);
            ImmutableBytesPtr cacheKey = new ImmutableBytesPtr(tableKey);
            long clientTimeStamp = MetaDataUtil.getClientTimeStamp(tableMetadata);
            PTable table = null;
            try {
                // Get as of latest timestamp so we can detect if we have a newer table that already
                // exists without making an additional query
                table = loadTable(env, tableKey, cacheKey, clientTimeStamp, HConstants.LATEST_TIMESTAMP,
                        clientVersion);
            } catch (ParentTableNotFoundException e) {
                dropChildViews(env, e.getParentTenantId(), e.getParentSchemaName(), e.getParentTableName());
            }
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
            
            // check if the table was dropped, but had child views that were have not yet been cleaned up
            if (!Bytes.toString(schemaName).equals(QueryConstants.SYSTEM_SCHEMA_NAME)) {
                dropChildViews(env, tenantIdBytes, schemaName, tableName);
            }
            
            byte[] parentTableKey = null;
            Mutation viewPhysicalTableRow = null;
            Set<TableName> indexes = new HashSet<TableName>();;
            byte[] cPhysicalName = SchemaUtil.getPhysicalHBaseTableName(schemaName, tableName, isNamespaceMapped)
                    .getBytes();
            byte[] cParentPhysicalName=null;
            if (tableType == PTableType.VIEW) {
                byte[][] parentSchemaTableNames = new byte[3][];
                byte[][] parentPhysicalSchemaTableNames = new byte[3][];
                /*
                 * For a mapped view, there is no link present to the physical table. So the
                 * viewPhysicalTableRow is null in that case.
                 */
                
                viewPhysicalTableRow = getPhysicalTableRowForView(tableMetadata, parentSchemaTableNames,parentPhysicalSchemaTableNames);
                if (parentPhysicalSchemaTableNames[2] != null) {
                    
                    parentTableKey = SchemaUtil.getTableKey(ByteUtil.EMPTY_BYTE_ARRAY,
                            parentPhysicalSchemaTableNames[1], parentPhysicalSchemaTableNames[2]);
                    PTable parentTable =
                            doGetTable(ByteUtil.EMPTY_BYTE_ARRAY, parentPhysicalSchemaTableNames[1],
                                parentPhysicalSchemaTableNames[2], clientTimeStamp, clientVersion);
                    if (parentTable == null) {
                        builder.setReturnCode(MetaDataProtos.MutationCode.PARENT_TABLE_NOT_FOUND);
                        builder.setMutationTime(EnvironmentEdgeManager.currentTimeMillis());
                        done.run(builder.build());
                        return;
                    }
                    cParentPhysicalName = parentTable.getPhysicalName().getBytes();
                    if (parentSchemaTableNames[2] != null
                            && Bytes.compareTo(parentSchemaTableNames[2], parentPhysicalSchemaTableNames[2]) != 0) {
                        // if view is created on view
                        byte[] tenantId =
                                parentSchemaTableNames[0] == null ? ByteUtil.EMPTY_BYTE_ARRAY
                                        : parentSchemaTableNames[0];
                        parentTable =
                                doGetTable(tenantId, parentSchemaTableNames[1],
                                    parentSchemaTableNames[2], clientTimeStamp, clientVersion);
                        if (parentTable == null) {
                            // it could be a global view
                            parentTable =
                                    doGetTable(ByteUtil.EMPTY_BYTE_ARRAY, parentSchemaTableNames[1],
                                        parentSchemaTableNames[2], clientTimeStamp, clientVersion);
                        }
                    }
                    if (parentTable == null) {
                        builder.setReturnCode(MetaDataProtos.MutationCode.PARENT_TABLE_NOT_FOUND);
                        builder.setMutationTime(EnvironmentEdgeManager.currentTimeMillis());
                        done.run(builder.build());
                        return;
                    }
                    for (PTable index : parentTable.getIndexes()) {
                        indexes.add(TableName.valueOf(index.getPhysicalName().getBytes()));
                    }
                } else {
                    // Mapped View
                    cParentPhysicalName = SchemaUtil.getTableNameAsBytes(schemaName, tableName);
                }
                parentTenantId = ByteUtil.EMPTY_BYTE_ARRAY;
                parentSchemaName = parentPhysicalSchemaTableNames[1];
                parentTableName = parentPhysicalSchemaTableNames[2];
                    
            } else if (tableType == PTableType.INDEX) {
                parentSchemaName = schemaName;
                /* 
                 * For an index we lock the parent table's row which could be a physical table or a view.
                 * If the parent table is a physical table, then the tenantIdBytes is empty because
                 * we allow creating an index with a tenant connection only if the parent table is a view.
                 */
                parentTenantId = tenantIdBytes;
                parentTableName = MetaDataUtil.getParentTableName(tableMetadata);
                parentTableKey = SchemaUtil.getTableKey(tenantIdBytes, parentSchemaName, parentTableName);
                PTable parentTable =
                        doGetTable(tenantIdBytes, parentSchemaName, parentTableName, clientTimeStamp, null,
                            request.getClientVersion(), false, false, null);
                if (IndexType.LOCAL == indexType) {
                    cPhysicalName = parentTable.getPhysicalName().getBytes();
                    cParentPhysicalName=parentTable.getPhysicalName().getBytes();
                } else if (parentTable.getType() == PTableType.VIEW) {
                    cPhysicalName = MetaDataUtil.getViewIndexPhysicalName(parentTable.getPhysicalName().getBytes());
                    cParentPhysicalName = parentTable.getPhysicalName().getBytes();
                }else{
                    cParentPhysicalName = SchemaUtil
                            .getPhysicalHBaseTableName(parentSchemaName, parentTableName, isNamespaceMapped).getBytes();
                }
            }
            
            getCoprocessorHost().preCreateTable(Bytes.toString(tenantIdBytes),
                    fullTableName,
                    (tableType == PTableType.VIEW) ? null : TableName.valueOf(cPhysicalName),
                    cParentPhysicalName == null ? null : TableName.valueOf(cParentPhysicalName), tableType,
                    /* TODO: During inital create we may not need the family map */
                    Collections.<byte[]> emptySet(), indexes);

            Region region = env.getRegion();
            List<RowLock> locks = Lists.newArrayList();
            // Place a lock using key for the table to be created
            try {
                acquireLock(region, tableKey, locks);

                // If the table key resides outside the region, return without doing anything
                MetaDataMutationResult result = checkTableKeyInRegion(tableKey, region);
                if (result != null) {
                    done.run(MetaDataMutationResult.toProto(result));
                    return;
                }

                ImmutableBytesPtr parentCacheKey = null;
                PTable parentTable = null;
                if (parentTableName != null) {
                    // From 4.15 onwards we only need to lock the parent table :
                    // 1) when creating an index on a table or a view
                    // 2) if allowSystemCatalogRollback is true we try to lock the parent table to prevent it
                    // from changing concurrently while a view is being created
                    if (tableType == PTableType.INDEX || allowSystemCatalogRollback) {
                        result = checkTableKeyInRegion(parentTableKey, region);
                        if (result != null) {
                            logger.error("Unable to lock parentTableKey "+Bytes.toStringBinary(parentTableKey));
                            // if allowSystemCatalogRollback is true and we can't lock the parentTableKey (because
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
                        acquireLock(region, parentTableKey, locks);
                    }
                    parentTable =  doGetTable(parentTenantId, parentSchemaName, parentTableName,
                        clientTimeStamp, null, clientVersion, false, false, null);
                    parentCacheKey = new ImmutableBytesPtr(parentTableKey);
                    if (parentTable == null || isTableDeleted(parentTable)) {
                        builder.setReturnCode(MetaDataProtos.MutationCode.PARENT_TABLE_NOT_FOUND);
                        builder.setMutationTime(EnvironmentEdgeManager.currentTimeMillis());
                        done.run(builder.build());
                        return;
                    }
                    // make sure we haven't gone over our threshold for indexes on this table.
                    if (execeededIndexQuota(tableType, parentTable)) {
                        builder.setReturnCode(MetaDataProtos.MutationCode.TOO_MANY_INDEXES);
                        builder.setMutationTime(EnvironmentEdgeManager.currentTimeMillis());
                        done.run(builder.build());
                        return;
                    }
                    long parentTableSeqNumber;
                    if (tableType == PTableType.VIEW && viewPhysicalTableRow != null && request.hasClientVersion()) {
                        // Starting 4.5, the client passes the sequence number of the physical table in the table metadata.
                        parentTableSeqNumber = MetaDataUtil.getSequenceNumber(viewPhysicalTableRow);
                    } else if (tableType == PTableType.VIEW && !request.hasClientVersion()) {
                        // Before 4.5, due to a bug, the parent table key wasn't available.
                        // So don't do anything and prevent the exception from being thrown.
                        parentTableSeqNumber = parentTable.getSequenceNumber();
                    } else {
                        parentTableSeqNumber = MetaDataUtil.getParentSequenceNumber(tableMetadata);
                    }
                    // If parent table isn't at the expected sequence number, then return
                    if (parentTable.getSequenceNumber() != parentTableSeqNumber) {
                        builder.setReturnCode(MetaDataProtos.MutationCode.CONCURRENT_TABLE_MUTATION);
                        builder.setMutationTime(EnvironmentEdgeManager.currentTimeMillis());
                        builder.setTable(PTableImpl.toProto(parentTable));
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
                if (parentTable!=null && parentTable.getAutoPartitionSeqName()!=null) {
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
                    }
                    catch (SequenceNotFoundException e) {
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
                    }
                    else {
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
                    byte[] bytes = new byte [dataType.getByteSize() + 1];
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
                        int nSequenceSaltBuckets = connection.getQueryServices().getSequenceSaltBuckets();
                        SequenceKey key = MetaDataUtil.getViewIndexSequenceKey(tenantIdStr, physicalName,
                            nSequenceSaltBuckets, parentTable.isNamespaceMapped() );
                        // TODO Review Earlier sequence was created at (SCN-1/LATEST_TIMESTAMP) and incremented at the client max(SCN,dataTable.getTimestamp), but it seems we should
                        // use always LATEST_TIMESTAMP to avoid seeing wrong sequence values by different connection having SCN
                        // or not.
                        long sequenceTimestamp = HConstants.LATEST_TIMESTAMP;
                        try {
                            connection.getQueryServices().createSequence(key.getTenantId(), key.getSchemaName(), key.getSequenceName(),
                                Long.MIN_VALUE, 1, 1, Long.MIN_VALUE, Long.MAX_VALUE, false, sequenceTimestamp);
                        } catch (SequenceAlreadyExistsException e) {
                        }
                        long[] seqValues = new long[1];
                        SQLException[] sqlExceptions = new SQLException[1];
                        connection.getQueryServices().incrementSequences(Collections.singletonList(new SequenceAllocation(key, 1)),
                            HConstants.LATEST_TIMESTAMP, seqValues, sqlExceptions);
                        if (sqlExceptions[0] != null) {
                            throw sqlExceptions[0];
                        }
                        long seqValue = seqValues[0];
                        Put tableHeaderPut = MetaDataUtil.getPutOnlyTableHeaderRow(tableMetadata);

                        NavigableMap<byte[], List<Cell>> familyCellMap = tableHeaderPut.getFamilyCellMap();
                        List<Cell> cells = familyCellMap.get(TABLE_FAMILY_BYTES);
                        Cell cell = cells.get(0);
                        PDataType dataType = MetaDataUtil.getViewIndexIdDataType();
                        Object val = dataType.toObject(seqValue, PLong.INSTANCE);
                        byte[] bytes = new byte [dataType.getByteSize() + 1];
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
                // 2. Update the encoded column qualifier for the parent table if its on a
                // different region server (for tables that use column qualifier encoding)
                // if the next step fails we end up wasting a few col qualifiers
                // 3. Finally write the mutations to create the table

                // From 4.15 the parent->child links are stored in a separate table SYSTEM.CHILD_LINK
                // TODO remove this after PHOENIX-4810 is implemented
                List<Mutation> childLinkMutations = MetaDataUtil.removeChildLinks(tableMetadata);
                MetaDataResponse response =
                        processRemoteRegionMutations(
                            PhoenixDatabaseMetaData.SYSTEM_CHILD_LINK_NAME_BYTES,
                            childLinkMutations, MetaDataProtos.MutationCode.UNABLE_TO_CREATE_CHILD_LINK);
                if (response != null) {
                    done.run(response);
                    return;
                }

                if (tableType == PTableType.VIEW) {
                    // Pass in the parent's PTable so that we only tag cells corresponding to the
                    // view's property in case they are different from the parent
                    addTagsToPutsForViewAlteredProperties(tableMetadata, parentTable);
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
                        response =
                                processRemoteRegionMutations(
                                    PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES,
                                    remoteMutations, MetaDataProtos.MutationCode.UNABLE_TO_UPDATE_PARENT_TABLE);
                        clearParentTableFromCache(clientTimeStamp,
                            parentTable.getSchemaName() != null
                                    ? parentTable.getSchemaName().getBytes()
                                    : ByteUtil.EMPTY_BYTE_ARRAY,
                            parentTable.getName().getBytes());
                        if (response != null) {
                            done.run(response);
                            return;
                        }
                    }
                    else {
                        String msg = "Found unexpected mutations while creating "+fullTableName;
                        logger.error(msg);
                        for (Mutation m : remoteMutations) {
                            logger.debug("Mutation rowkey : " + Bytes.toStringBinary(m.getRow()));
                            logger.debug("Mutation family cell map : " + m.getFamilyCellMap());
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
                mutateRowsWithLocks(region, localMutations, Collections.<byte[]> emptySet(), HConstants.NO_NONCE, HConstants.NO_NONCE);

                // Invalidate the cache - the next getTable call will add it
                // TODO: consider loading the table that was just created here, patching up the parent table, and updating the cache
                Cache<ImmutableBytesPtr,PMetaDataEntity> metaDataCache = GlobalCache.getInstance(this.env).getMetaDataCache();
                if (parentCacheKey != null) {
                    metaDataCache.invalidate(parentCacheKey);
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
                done.run(builder.build());
                return;
            } finally {
                ServerUtil.releaseRowLocks(locks);
            }
        } catch (Throwable t) {
            logger.error("createTable failed", t);
            ProtobufUtil.setControllerException(controller,
                    ServerUtil.createIOException(fullTableName, t));
        }
    }

    public static void dropChildViews(RegionCoprocessorEnvironment env, byte[] tenantIdBytes, byte[] schemaName, byte[] tableName)
            throws IOException, SQLException, ClassNotFoundException {
        Table hTable =
                ServerUtil.getHTableForCoprocessorScan(env,
                        SchemaUtil.getPhysicalTableName(
                                PhoenixDatabaseMetaData.SYSTEM_CHILD_LINK_NAME_BYTES,
                                env.getConfiguration()));
        TableViewFinderResult childViewsResult = ViewFinder.findRelatedViews(hTable, tenantIdBytes, schemaName, tableName,
                PTable.LinkType.CHILD_TABLE, HConstants.LATEST_TIMESTAMP);

        if (childViewsResult.hasLinks()) {

            for (TableInfo viewInfo : childViewsResult.getLinks()) {
                byte[] viewTenantId = viewInfo.getTenantId();
                byte[] viewSchemaName = viewInfo.getSchemaName();
                byte[] viewName = viewInfo.getTableName();
                if (logger.isDebugEnabled()) {
                    logger.debug("dropChildViews :" + Bytes.toString(schemaName) + "." + Bytes.toString(tableName) +
                            " -> " + Bytes.toString(viewSchemaName) + "." + Bytes.toString(viewName) +
                            "with tenant id :" + Bytes.toString(viewTenantId));
                }
                Properties props = new Properties();
                if (viewTenantId != null && viewTenantId.length != 0)
                    props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, Bytes.toString(viewTenantId));
                try (PhoenixConnection connection = QueryUtil.getConnectionOnServer(props, env.getConfiguration())
                        .unwrap(PhoenixConnection.class)) {
                    MetaDataClient client = new MetaDataClient(connection);
                    org.apache.phoenix.parse.TableName viewTableName = org.apache.phoenix.parse.TableName
                            .create(Bytes.toString(viewSchemaName), Bytes.toString(viewName));
                    try {
                        client.dropTable(
                                new DropTableStatement(viewTableName, PTableType.VIEW, true, true, true));
                    }
                    catch (TableNotFoundException e) {
                        logger.info("Ignoring view "+viewTableName+" as it has already been dropped");
                    }
                }
            }
        }
    }
    private boolean execeededIndexQuota(PTableType tableType, PTable parentTable) {
        return PTableType.INDEX == tableType && parentTable.getIndexes().size() >= maxIndexesPerTable;
    }

    private void findAncestorViewsOfIndex(byte[] tenantId, byte[] schemaName, byte[] indexName,
            TableViewFinderResult result, boolean isNamespaceMapped) throws IOException {
        try (Table hTable =
                ServerUtil.getHTableForCoprocessorScan(env, SchemaUtil.getPhysicalTableName(
                    PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES, env.getConfiguration()))) {
            TableViewFinderResult currentResult =
                    ViewFinder.findParentViewofIndex(hTable, tenantId, schemaName, indexName);
            if (currentResult.getLinks().size() == 1) {
                result.addResult(currentResult);
                TableInfo tableInfo = currentResult.getLinks().get(0);
                findAncestorViews(tableInfo.getTenantId(), tableInfo.getSchemaName(),
                    tableInfo.getTableName(), result, isNamespaceMapped);
            }
            // else this is an index on a regular table and so we don't need to combine columns
        }
    }
    
    private void findAncestorViews(byte[] tenantId, byte[] schemaName, byte[] tableName,
            TableViewFinderResult result, boolean isNamespaceMapped) throws IOException {
        try (Table hTable =
                ServerUtil.getHTableForCoprocessorScan(env, SchemaUtil.getPhysicalTableName(
                    PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES, env.getConfiguration()))) {
            ViewFinder.findAllRelatives(hTable, tenantId, schemaName, tableName,
                LinkType.PARENT_TABLE, result);
            if (!isNamespaceMapped || schemaName.length==0) {
				// When namespace mapping is enabled and the schema name is not empty the
				// physical table name is of the form S:T while the table name is of the form
				// S.T
				// When namespace mapping is disabled or the schema name is empty the
				// child->parent link is overwritten by the child->physical table link for first
				// level children of base table as both the parent table name and physical table
				// name are the same (S.T or T) so we need to query for the PHYSICAL_TABLE link
                result.addResult(ViewFinder.findBaseTable(hTable, tenantId, schemaName, tableName));
            }
        }
    }

    private void findAllChildViews(byte[] tenantId, byte[] schemaName, byte[] tableName, TableViewFinderResult result) throws IOException {
        try (Table hTable =
                ServerUtil.getHTableForCoprocessorScan(env,
                    SchemaUtil.getPhysicalTableName(
                        PhoenixDatabaseMetaData.SYSTEM_CHILD_LINK_NAME_BYTES,
                        env.getConfiguration()))) {
            ViewFinder.findAllRelatives(hTable, tenantId, schemaName, tableName,
                LinkType.CHILD_TABLE, result);
        }
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
        byte[] tableName = null;

        try {
            List<Mutation> tableMetadata = ProtobufUtil.getMutations(request);
            List<Mutation> childLinkMutations = Lists.newArrayList();
            MetaDataUtil.getTenantIdAndSchemaAndTableName(tableMetadata, rowKeyMetaData);
            byte[] tenantIdBytes = rowKeyMetaData[PhoenixDatabaseMetaData.TENANT_ID_INDEX];
            schemaName = rowKeyMetaData[PhoenixDatabaseMetaData.SCHEMA_NAME_INDEX];
            tableName = rowKeyMetaData[PhoenixDatabaseMetaData.TABLE_NAME_INDEX];
            PTableType pTableType=PTableType.fromSerializedValue(tableType);
            // Disallow deletion of a system table
            if (pTableType == PTableType.SYSTEM) {
                builder.setReturnCode(MetaDataProtos.MutationCode.UNALLOWED_TABLE_MUTATION);
                builder.setMutationTime(EnvironmentEdgeManager.currentTimeMillis());
                done.run(builder.build());
                return;
            }
            
            List<byte[]> tableNamesToDelete = Lists.newArrayList();
            List<SharedTableState> sharedTablesToDelete = Lists.newArrayList();
            
            byte[] lockKey = SchemaUtil.getTableKey(tenantIdBytes, schemaName, tableName);
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
                result = checkTableKeyInRegion(lockKey, region);
                if (result != null) {
                    done.run(MetaDataMutationResult.toProto(result));
                    return;
                }
            }
            
            long clientTimeStamp = MetaDataUtil.getClientTimeStamp(tableMetadata);
            boolean skipAddingParentColumns = request.hasSkipAddingParentColumns()
                    ? request.getSkipAddingParentColumns()
                    : false;
            PTable loadedTable =
                    doGetTable(tenantIdBytes, schemaName, tableName, clientTimeStamp, null,
                        request.getClientVersion(), false, skipAddingParentColumns, null);
            if (loadedTable == null) {
                builder.setReturnCode(MetaDataProtos.MutationCode.TABLE_NOT_FOUND);
                builder.setMutationTime(EnvironmentEdgeManager.currentTimeMillis());
                done.run(builder.build());
                return;
            }
            getCoprocessorHost().preDropTable(Bytes.toString(tenantIdBytes),
                    SchemaUtil.getTableName(schemaName, tableName),
                    TableName.valueOf(loadedTable.getPhysicalName().getBytes()),
                    getParentPhysicalTableName(loadedTable), pTableType, loadedTable.getIndexes());
            List<RowLock> locks = Lists.newArrayList();
            try {
                acquireLock(region, lockKey, locks);
                if (parentLockKey != null) {
                    acquireLock(region, parentLockKey, locks);
                }

                List<ImmutableBytesPtr> invalidateList = new ArrayList<ImmutableBytesPtr>();
                result =
                        doDropTable(lockKey, tenantIdBytes, schemaName, tableName, parentTableName,
                            PTableType.fromSerializedValue(tableType), tableMetadata, childLinkMutations,
                            invalidateList, tableNamesToDelete, sharedTablesToDelete, isCascade, request.getClientVersion());
                if (result.getMutationCode() != MutationCode.TABLE_ALREADY_EXISTS) {
                    done.run(MetaDataMutationResult.toProto(result));
                    return;
                }
                Cache<ImmutableBytesPtr, PMetaDataEntity> metaDataCache =
                        GlobalCache.getInstance(this.env).getMetaDataCache();

                List<Mutation> localMutations =
                        Lists.newArrayListWithExpectedSize(tableMetadata.size());
                List<Mutation> remoteMutations = Lists.newArrayList();
                separateLocalAndRemoteMutations(region, tableMetadata, localMutations, remoteMutations);
                if (!remoteMutations.isEmpty()) {
                    // while dropping a table all the mutations should be local
                    String msg = "Found unexpected mutations while dropping table "+SchemaUtil.getTableName(schemaName, tableName);
                    logger.error(msg);
                    for (Mutation m : remoteMutations) {
                        logger.debug("Mutation rowkey : " + Bytes.toStringBinary(m.getRow()));
                        logger.debug("Mutation family cell map : " + m.getFamilyCellMap());
                    }
                    throw new IllegalStateException(msg);
                }

                // drop rows from catalog on this region
                mutateRowsWithLocks(region, localMutations, Collections.<byte[]> emptySet(), HConstants.NO_NONCE,
                        HConstants.NO_NONCE);

                long currentTime = MetaDataUtil.getClientTimeStamp(tableMetadata);
                for (ImmutableBytesPtr ckey : invalidateList) {
                    metaDataCache.put(ckey, newDeletedTableMarker(currentTime));
                }
                if (parentLockKey != null) {
                    ImmutableBytesPtr parentCacheKey = new ImmutableBytesPtr(parentLockKey);
                    metaDataCache.invalidate(parentCacheKey);
                }

                // after the view metadata is dropped drop parent->child link
                MetaDataResponse response =
                        processRemoteRegionMutations(
                            PhoenixDatabaseMetaData.SYSTEM_CHILD_LINK_NAME_BYTES,
                            childLinkMutations, MetaDataProtos.MutationCode.UNABLE_TO_CREATE_CHILD_LINK);
                if (response!=null) {
                    done.run(response);
                    return;
                }

                done.run(MetaDataMutationResult.toProto(result));
                return;
            } finally {
                ServerUtil.releaseRowLocks(locks);
            }
        } catch (Throwable t) {
          logger.error("dropTable failed", t);
            ProtobufUtil.setControllerException(controller,
                ServerUtil.createIOException(SchemaUtil.getTableName(schemaName, tableName), t));
        }
    }
    

    private RowLock acquireLock(Region region, byte[] lockKey, List<RowLock> locks) throws IOException {
        RowLock rowLock = region.getRowLock(lockKey, false);
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
        if (remoteMutations.isEmpty())
            return null;
        MetaDataResponse.Builder builder = MetaDataResponse.newBuilder();
        try (Table hTable =
                ServerUtil.getHTableForCoprocessorScan(env,
                    SchemaUtil.getPhysicalTableName(systemTableName, env.getConfiguration()))) {
            hTable.batch(remoteMutations, null);
        } catch (Throwable t) {
            logger.error("Unable to write mutations to " + Bytes.toString(systemTableName), t);
            builder.setReturnCode(mutationCode);
            builder.setMutationTime(EnvironmentEdgeManager.currentTimeMillis());
            return builder.build();
        }
        return null;
    }
    
    private MetaDataMutationResult doDropTable(byte[] key, byte[] tenantId, byte[] schemaName, byte[] tableName,
            byte[] parentTableName, PTableType tableType, List<Mutation> catalogMutations,
            List<Mutation> childLinkMutations, List<ImmutableBytesPtr> invalidateList, List<byte[]> tableNamesToDelete,
            List<SharedTableState> sharedTablesToDelete, boolean isCascade, int clientVersion)
            throws IOException, SQLException {

        Region region = env.getRegion();
        long clientTimeStamp = MetaDataUtil.getClientTimeStamp(catalogMutations);
        ImmutableBytesPtr cacheKey = new ImmutableBytesPtr(key);

        PTable table = getTableFromCache(cacheKey, clientTimeStamp, clientVersion, true, true, null);

        // We always cache the latest version - fault in if not in cache
        if (table != null
                || (table = buildTable(key, cacheKey, region, HConstants.LATEST_TIMESTAMP, clientVersion, true, true, null)) != null) {
            if (table.getTimeStamp() < clientTimeStamp) {
                if (isTableDeleted(table) || tableType != table.getType()) {
                    return new MetaDataMutationResult(MutationCode.TABLE_NOT_FOUND, EnvironmentEdgeManager.currentTimeMillis(), null);
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
                return new MetaDataMutationResult(MutationCode.NEWER_TABLE_FOUND, EnvironmentEdgeManager.currentTimeMillis(), null);
            }
            return new MetaDataMutationResult(MutationCode.TABLE_NOT_FOUND, EnvironmentEdgeManager.currentTimeMillis(), null);
        }
        // Make sure we're not deleting the "wrong" child
        if (parentTableName!=null && table.getParentTableName() != null && !Arrays.equals(parentTableName, table.getParentTableName().getBytes())) {
            return new MetaDataMutationResult(MutationCode.TABLE_NOT_FOUND, EnvironmentEdgeManager.currentTimeMillis(), null);
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

            if (tableType == PTableType.TABLE || tableType == PTableType.VIEW || tableType == PTableType.SYSTEM) {
                // check to see if the table has any child views
                try (Table hTable =
                        ServerUtil.getHTableForCoprocessorScan(env,
                            SchemaUtil.getPhysicalTableName(
                                PhoenixDatabaseMetaData.SYSTEM_CHILD_LINK_NAME_BYTES,
                                env.getConfiguration()))) {
                    boolean hasChildViews =
                            ViewFinder.hasChildViews(hTable, tenantId, schemaName, tableName,
                                clientTimeStamp);
                    if (hasChildViews) {
                        if (!isCascade) {
                            // DROP without CASCADE on tables with child views is not permitted
                            return new MetaDataMutationResult(MutationCode.UNALLOWED_TABLE_MUTATION,
                                    EnvironmentEdgeManager.currentTimeMillis(), null);
                        }
                        try {
                            PhoenixConnection conn = QueryUtil.getConnectionOnServer(env.getConfiguration()).unwrap(PhoenixConnection.class);
                            TaskRegionObserver.addTask(conn, PTable.TaskType.DROP_CHILD_VIEWS, Bytes.toString(tenantId),
                                Bytes.toString(schemaName), Bytes.toString(tableName), this.accessCheckEnabled);
                        } catch (Throwable t) {
                            logger.error("Adding a task to drop child views failed!", t);
                        }
                    }
                }
            }

            // Add to list of HTables to delete, unless it's a view or its a shared index 
            if (tableType == INDEX && table.getViewIndexId()!=null) {
                sharedTablesToDelete.add(new SharedTableState(table));
            }
            else if (tableType != PTableType.VIEW) { 
                tableNamesToDelete.add(table.getPhysicalName().getBytes());
            }
            invalidateList.add(cacheKey);
            byte[][] rowKeyMetaData = new byte[5][];
            do {
                Cell kv = results.get(LINK_TYPE_INDEX);
                int nColumns = getVarChars(kv.getRowArray(), kv.getRowOffset(), kv.getRowLength(), 0, rowKeyMetaData);
                if (nColumns == 5
                        && rowKeyMetaData[PhoenixDatabaseMetaData.FAMILY_NAME_INDEX].length > 0
                        && Bytes.compareTo(kv.getQualifierArray(), kv.getQualifierOffset(), kv.getQualifierLength(),
                                LINK_TYPE_BYTES, 0, LINK_TYPE_BYTES.length) == 0) {
                        LinkType linkType = LinkType.fromSerializedValue(kv.getValueArray()[kv.getValueOffset()]);
                        if (rowKeyMetaData[PhoenixDatabaseMetaData.COLUMN_NAME_INDEX].length == 0 && linkType == LinkType.INDEX_TABLE) {
                            indexNames.add(rowKeyMetaData[PhoenixDatabaseMetaData.FAMILY_NAME_INDEX]);
                        } else if (tableType == PTableType.VIEW && (linkType == LinkType.PARENT_TABLE || linkType == LinkType.PHYSICAL_TABLE)) {
                            // delete parent->child link for views
                            Cell parentTenantIdCell = MetaDataUtil.getCell(results, PhoenixDatabaseMetaData.PARENT_TENANT_ID_BYTES);
                            PName parentTenantId = parentTenantIdCell!=null ? PNameFactory.newName(parentTenantIdCell.getValueArray(), parentTenantIdCell.getValueOffset(), parentTenantIdCell.getValueLength()) : null;
                            byte[] linkKey = MetaDataUtil.getChildLinkKey(parentTenantId, table.getParentSchemaName(), table.getParentTableName(), table.getTenantId(), table.getName());
                            Delete linkDelete = new Delete(linkKey, clientTimeStamp);
                            childLinkMutations.add(linkDelete);
                        }
                }
                Delete delete = new Delete(kv.getRowArray(), kv.getRowOffset(), kv.getRowLength(), clientTimeStamp);
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
            MetaDataMutationResult result =
                    doDropTable(indexKey, tenantId, schemaName, indexName, tableName, PTableType.INDEX,
                        catalogMutations, childLinkMutations, invalidateList, tableNamesToDelete, sharedTablesToDelete, false, clientVersion);
            if (result.getMutationCode() != MutationCode.TABLE_ALREADY_EXISTS) {
                return result;
            }
        }

        return new MetaDataMutationResult(MutationCode.TABLE_ALREADY_EXISTS,
                EnvironmentEdgeManager.currentTimeMillis(), table, tableNamesToDelete, sharedTablesToDelete);
    }

    private static interface ColumnMutator {
        MetaDataMutationResult updateMutation(PTable table, byte[][] rowKeyMetaData,
                List<Mutation> tableMetadata, Region region, List<ImmutableBytesPtr> invalidateList,
                List<RowLock> locks, long clientTimeStamp) throws IOException, SQLException;
    }

    private MetaDataMutationResult
    mutateColumn(MutatateColumnType mutateColumnType, List<Mutation> tableMetadata, ColumnMutator mutator, int clientVersion) throws IOException {
        byte[][] rowKeyMetaData = new byte[5][];
        MetaDataUtil.getTenantIdAndSchemaAndTableName(tableMetadata, rowKeyMetaData);
        byte[] tenantId = rowKeyMetaData[PhoenixDatabaseMetaData.TENANT_ID_INDEX];
        byte[] schemaName = rowKeyMetaData[PhoenixDatabaseMetaData.SCHEMA_NAME_INDEX];
        byte[] tableName = rowKeyMetaData[PhoenixDatabaseMetaData.TABLE_NAME_INDEX];
        byte[] key = SchemaUtil.getTableKey(tenantId, schemaName, tableName);
        String fullTableName = SchemaUtil.getTableName(schemaName, tableName);
        try {
            Region region = env.getRegion();
            MetaDataMutationResult result = checkTableKeyInRegion(key, region);
            if (result != null) {
                return result;
            }
            List<RowLock> locks = Lists.newArrayList();
            try {
                acquireLock(region, key, locks);
                ImmutableBytesPtr cacheKey = new ImmutableBytesPtr(key);
                List<ImmutableBytesPtr> invalidateList = new ArrayList<ImmutableBytesPtr>();
                invalidateList.add(cacheKey);
                long clientTimeStamp = MetaDataUtil.getClientTimeStamp(tableMetadata);
                PTable table = getTableFromCache(cacheKey, clientTimeStamp, clientVersion, false, false, null);
                if (logger.isDebugEnabled()) {
                    if (table == null) {
                        logger.debug("Table " + Bytes.toStringBinary(key)
                                + " not found in cache. Will build through scan");
                    } else {
                        logger.debug("Table " + Bytes.toStringBinary(key)
                                + " found in cache with timestamp " + table.getTimeStamp()
                                + " seqNum " + table.getSequenceNumber());
                    }
                }
                // Get client timeStamp from mutations
                if (table == null
                        && (table = buildTable(key, cacheKey, region, HConstants.LATEST_TIMESTAMP, clientVersion, false, false, null)) == null) {
                    // if not found then call newerTableExists and add delete marker for timestamp
                    // found
                    table = buildDeletedTable(key, cacheKey, region, clientTimeStamp);
                    if (table != null) {
                        logger.info("Found newer table deleted as of " + table.getTimeStamp() + " versus client timestamp of " + clientTimeStamp);
                        return new MetaDataMutationResult(MutationCode.NEWER_TABLE_FOUND,
                                EnvironmentEdgeManager.currentTimeMillis(), null);
                    }
                    return new MetaDataMutationResult(MutationCode.TABLE_NOT_FOUND,
                            EnvironmentEdgeManager.currentTimeMillis(), null);
                }
                if (table.getTimeStamp() >= clientTimeStamp) {
                    logger.info("Found newer table as of " + table.getTimeStamp() + " versus client timestamp of "
                            + clientTimeStamp);
                    return new MetaDataMutationResult(MutationCode.NEWER_TABLE_FOUND,
                            EnvironmentEdgeManager.currentTimeMillis(), table);
                } else if (isTableDeleted(table)) { return new MetaDataMutationResult(MutationCode.TABLE_NOT_FOUND,
                        EnvironmentEdgeManager.currentTimeMillis(), null); }
                long expectedSeqNum = MetaDataUtil.getSequenceNumber(tableMetadata) - 1; // lookup TABLE_SEQ_NUM in
                                                                                         // tableMetaData

                if (logger.isDebugEnabled()) {
                    logger.debug("For table " + Bytes.toStringBinary(key) + " expecting seqNum "
                            + expectedSeqNum + " and found seqNum " + table.getSequenceNumber()
                            + " with " + table.getColumns().size() + " columns: "
                            + table.getColumns());
                }
                if (expectedSeqNum != table.getSequenceNumber()) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("For table " + Bytes.toStringBinary(key)
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
                    // server-side, except for indexing, we always expect the keyvalues to be standard KeyValues
                    PTableType expectedType = MetaDataUtil.getTableType(tableMetadata, GenericKeyValueBuilder.INSTANCE,
                            new ImmutableBytesWritable());
                    // We said to drop a table, but found a view or visa versa
                    if (type != expectedType) { return new MetaDataMutationResult(MutationCode.TABLE_NOT_FOUND,
                            EnvironmentEdgeManager.currentTimeMillis(), null); }
                }
                result = mutator.updateMutation(table, rowKeyMetaData, tableMetadata, region,
                            invalidateList, locks, clientTimeStamp);
                // if the update mutation caused tables to be deleted, the mutation code returned
                // will be MutationCode.TABLE_ALREADY_EXISTS
                if (result != null
                        && result.getMutationCode() != MutationCode.TABLE_ALREADY_EXISTS) {
                    return result;
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
                    // there should only be remote mutations if we are adding a column to a view that uses encoded column qualifiers
                    // (the remote mutations are to update the encoded column qualifier counter on the parent table)
                    if (mutateColumnType == MutatateColumnType.ADD_COLUMN && type == PTableType.VIEW && table
                            .getEncodingScheme() != QualifierEncodingScheme.NON_ENCODED_QUALIFIERS) {
                        processRemoteRegionMutations(
                            PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES, remoteMutations,
                            MetaDataProtos.MutationCode.UNABLE_TO_UPDATE_PARENT_TABLE);
                        clearParentTableFromCache(clientTimeStamp,
                            table.getParentSchemaName() != null
                                    ? table.getParentSchemaName().getBytes()
                                    : ByteUtil.EMPTY_BYTE_ARRAY,
                            table.getParentTableName().getBytes());
                    }
                    else {
                        String msg = "Found unexpected mutations while adding or dropping column to "+fullTableName;
                        logger.error(msg);
                        for (Mutation m : remoteMutations) {
                            logger.debug("Mutation rowkey : " + Bytes.toStringBinary(m.getRow()));
                            logger.debug("Mutation family cell map : " + m.getFamilyCellMap());
                        }
                        throw new IllegalStateException(msg);
                    }
                }
                mutateRowsWithLocks(region, localMutations, Collections.<byte[]> emptySet(), HConstants.NO_NONCE, HConstants.NO_NONCE);
                // Invalidate from cache
                for (ImmutableBytesPtr invalidateKey : invalidateList) {
                    metaDataCache.invalidate(invalidateKey);
                }
                // Get client timeStamp from mutations, since it may get updated by the
                // mutateRowsWithLocks call
                long currentTime = MetaDataUtil.getClientTimeStamp(tableMetadata);
                // if the update mutation caused tables to be deleted just return the result which will contain the table to be deleted
                if (result !=null) {
                    return result;
                } else {
                    table = buildTable(key, cacheKey, region, HConstants.LATEST_TIMESTAMP, clientVersion, false, false, null);
                    return new MetaDataMutationResult(MutationCode.TABLE_ALREADY_EXISTS, currentTime, table);
                }
            } finally {
                ServerUtil.releaseRowLocks(locks);
            }
        } catch (Throwable t) {
            ServerUtil.throwIOException(fullTableName, t);
            return null; // impossible
        }
    }

    /**
     * Removes the table from the server side cache
     */
    private void clearParentTableFromCache(long clientTimeStamp, byte[] schemaName, byte[] tableName) throws SQLException {
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
        } catch (ClassNotFoundException e) {
        }
    }
    
    private static boolean isDivergedView(PTable view) {
        return view.getBaseColumnCount() == QueryConstants.DIVERGED_VIEW_BASE_COLUMN_COUNT;
    }

    private boolean switchAttribute(PTable table, boolean currAttribute, List<Mutation> tableMetaData, byte[] attrQualifier) {
        for (Mutation m : tableMetaData) {
            if (m instanceof Put) {
                Put p = (Put)m;
                List<Cell> cells = p.get(TABLE_FAMILY_BYTES, attrQualifier);
                if (cells != null && cells.size() > 0) {
                    Cell cell = cells.get(0);
                    boolean newAttribute = (boolean)PBoolean.INSTANCE.toObject(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                    return currAttribute != newAttribute;
                }
            }
        }
        return false;
    }
    
    private class ColumnFinder extends StatelessTraverseAllExpressionVisitor<Void> {
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

    private MetaDataMutationResult validateColumnForAddToBaseTable(PTable basePhysicalTable,
            List<Mutation> tableMetadata, byte[][] rowKeyMetaData,
            TableViewFinderResult childViewsResult, long clientTimeStamp, int clientVersion)
            throws IOException, SQLException {
        byte[] schemaName = rowKeyMetaData[SCHEMA_NAME_INDEX];
        byte[] tableName = rowKeyMetaData[TABLE_NAME_INDEX];
        List<Put> columnPutsForBaseTable =
                Lists.newArrayListWithExpectedSize(tableMetadata.size());
        boolean salted = basePhysicalTable.getBucketNum()!=null;
        // Isolate the puts relevant to adding columns 
        for (Mutation m : tableMetadata) {
            if (m instanceof Put) {
                byte[][] rkmd = new byte[5][];
                int pkCount = getVarChars(m.getRow(), rkmd);
                // check if this put is for adding a column
                if (pkCount > COLUMN_NAME_INDEX && rkmd[COLUMN_NAME_INDEX] != null
                        && rkmd[COLUMN_NAME_INDEX].length > 0
                        && Bytes.compareTo(schemaName, rkmd[SCHEMA_NAME_INDEX]) == 0
                        && Bytes.compareTo(tableName, rkmd[TABLE_NAME_INDEX]) == 0) {
                    columnPutsForBaseTable.add((Put)m);
                }
            }
        }
        for (TableInfo viewInfo : childViewsResult.getLinks()) {
            byte[] tenantId = viewInfo.getTenantId();
            byte[] schema = viewInfo.getSchemaName();
            byte[] table = viewInfo.getTableName();
            PTable view =
                    doGetTable(tenantId, schema, table, clientTimeStamp, null, clientVersion, true,
                        true, null); 
            // we don't need to include parent columns as we are only interested in the columns
            // added by the view itself (also including parent columns will lead to a circular call
            // which will deadlock)
            // we also don't need to include indexes
            if (view == null) {
                logger.warn("Found invalid tenant view row in SYSTEM.CATALOG with tenantId:"
                        + Bytes.toString(tenantId) + ", schema:" + Bytes.toString(schema)
                        + ", table:" + Bytes.toString(table));
                continue;
            }
            /*
             * Disallow adding columns to a base table with APPEND_ONLY_SCHEMA since this
             * creates a gap in the column positions for every view (PHOENIX-4737).
             */
            if (!columnPutsForBaseTable.isEmpty() && view.isAppendOnlySchema()) {
                return new MetaDataMutationResult(MutationCode.UNALLOWED_TABLE_MUTATION, EnvironmentEdgeManager.currentTimeMillis(), basePhysicalTable);
            }
            
            // add the new columns to the child view
            List<PColumn> viewPkCols = new ArrayList<>(view.getPKColumns());
            // remove salted column
            if (salted) {
                viewPkCols.remove(0);
            }
            // remove pk columns that are present in the parent
            viewPkCols.removeAll(basePhysicalTable.getPKColumns());
            boolean addedPkColumn = false;
            for (Put columnToBeAdded : columnPutsForBaseTable) {
                PColumn existingViewColumn = null;
                byte[][] rkmd = new byte[5][];
                getVarChars(columnToBeAdded.getRow(), rkmd);
                String columnName = Bytes.toString(rkmd[COLUMN_NAME_INDEX]);
                String columnFamily =
                        rkmd[FAMILY_NAME_INDEX] == null ? null
                                : Bytes.toString(rkmd[FAMILY_NAME_INDEX]);
                try {
                    existingViewColumn =
                            columnFamily == null ? view.getColumnForColumnName(columnName)
                                    : view.getColumnFamily(columnFamily)
                                            .getPColumnForColumnName(columnName);
                } catch (ColumnFamilyNotFoundException e) {
                    // ignore since it means that the column family is not present for the column to
                    // be added.
                } catch (ColumnNotFoundException e) {
                    // ignore since it means the column is not present in the view
                }

                boolean isCurrColumnToBeAddPkCol = columnFamily == null;
                addedPkColumn |= isCurrColumnToBeAddPkCol;
                if (existingViewColumn != null) {
                    if (EncodedColumnsUtil.usesEncodedColumnNames(basePhysicalTable)
                            && !SchemaUtil.isPKColumn(existingViewColumn)) {
                        /*
                         * If the column already exists in a view, then we cannot add the column to
                         * the base table. The reason is subtle and is as follows: consider the case
                         * where a table has two views where both the views have the same key value
                         * column KV. Now, we dole out encoded column qualifiers for key value
                         * columns in views by using the counters stored in the base physical table.
                         * So the KV column can have different column qualifiers for the two views.
                         * For example, 11 for VIEW1 and 12 for VIEW2. This naturally extends to
                         * rows being inserted using the two views having different column
                         * qualifiers for the column named KV. Now, when an attempt is made to add
                         * column KV to the base table, we cannot decide which column qualifier
                         * should that column be assigned. It cannot be a number different than 11
                         * or 12 since a query like SELECT KV FROM BASETABLE would return null for
                         * KV which is incorrect since column KV is present in rows inserted from
                         * the two views. We cannot use 11 or 12 either because we will then
                         * incorrectly return value of KV column inserted using only one view.
                         */
                        return new MetaDataMutationResult(MutationCode.UNALLOWED_TABLE_MUTATION,
                                EnvironmentEdgeManager.currentTimeMillis(), basePhysicalTable);
                    }
                    // Validate data type is same
                    int baseColumnDataType =
                            getInteger(columnToBeAdded, TABLE_FAMILY_BYTES, DATA_TYPE_BYTES);
                    if (baseColumnDataType != existingViewColumn.getDataType().getSqlType()) {
                        return new MetaDataMutationResult(MutationCode.UNALLOWED_TABLE_MUTATION,
                                EnvironmentEdgeManager.currentTimeMillis(), basePhysicalTable);
                    }

                    // Validate max length is same
                    int maxLength =
                            getInteger(columnToBeAdded, TABLE_FAMILY_BYTES, COLUMN_SIZE_BYTES);
                    int existingMaxLength =
                            existingViewColumn.getMaxLength() == null ? 0
                                    : existingViewColumn.getMaxLength();
                    if (maxLength != existingMaxLength) {
                        return new MetaDataMutationResult(MutationCode.UNALLOWED_TABLE_MUTATION,
                                EnvironmentEdgeManager.currentTimeMillis(), basePhysicalTable);
                    }

                    // Validate scale is same
                    int scale =
                            getInteger(columnToBeAdded, TABLE_FAMILY_BYTES, DECIMAL_DIGITS_BYTES);
                    int existingScale =
                            existingViewColumn.getScale() == null ? 0
                                    : existingViewColumn.getScale();
                    if (scale != existingScale) {
                        return new MetaDataMutationResult(MutationCode.UNALLOWED_TABLE_MUTATION,
                                EnvironmentEdgeManager.currentTimeMillis(), basePhysicalTable);
                    }

                    // Validate sort order is same
                    int sortOrder =
                            getInteger(columnToBeAdded, TABLE_FAMILY_BYTES, SORT_ORDER_BYTES);
                    if (sortOrder != existingViewColumn.getSortOrder().getSystemValue()) {
                        return new MetaDataMutationResult(MutationCode.UNALLOWED_TABLE_MUTATION,
                                EnvironmentEdgeManager.currentTimeMillis(), basePhysicalTable);
                    }

                    // if the column to be added to the base table is a pk column, then we need to
                    // validate that the key slot position is the same
                    if (isCurrColumnToBeAddPkCol) {
                        List<Cell> keySeqCells =
                                columnToBeAdded.get(PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES,
                                    PhoenixDatabaseMetaData.KEY_SEQ_BYTES);
                        if (keySeqCells != null && keySeqCells.size() > 0) {
                            Cell cell = keySeqCells.get(0);
                            int keySeq =
                                    PSmallint.INSTANCE.getCodec().decodeInt(cell.getValueArray(),
                                        cell.getValueOffset(), SortOrder.getDefault());
                            // we need to take into account the columns inherited from the base table
                            // if the table is salted we don't include the salted column (which is
                            // present in getPKColumns())
                            int pkPosition =
                                    basePhysicalTable.getPKColumns().size()
                                            + SchemaUtil.getPKPosition(view, existingViewColumn) + 1
                                            - (salted ? 2 : 0); 
                            if (pkPosition != keySeq) {
                                return new MetaDataMutationResult(
                                        MutationCode.UNALLOWED_TABLE_MUTATION,
                                        EnvironmentEdgeManager.currentTimeMillis(),
                                        basePhysicalTable);
                            }
                        }
                    }
                }
                if (existingViewColumn!=null && isCurrColumnToBeAddPkCol) {
                    viewPkCols.remove(existingViewColumn);
                }
            }
            /*
             * Allow adding a pk columns to base table : 1. if all the view pk columns are exactly
             * the same as the base table pk columns 2. if we are adding all the existing view pk
             * columns to the base table
             */
            if (addedPkColumn && !viewPkCols.isEmpty()) {
                return new MetaDataMutationResult(MutationCode.UNALLOWED_TABLE_MUTATION,
                        EnvironmentEdgeManager.currentTimeMillis(), basePhysicalTable);
            }
        }
        return null;
    }
    
    private MetaDataMutationResult dropViewIndexes(Region region, PTable basePhysicalTable,
            List<RowLock> locks, List<Mutation> tableMetadata,
            List<Mutation> mutationsForAddingColumnsToViews, byte[] schemaName, byte[] tableName,
            List<ImmutableBytesPtr> invalidateList, long clientTimeStamp,
            TableViewFinderResult childViewsResult, List<byte[]> tableNamesToDelete,
            List<SharedTableState> sharedTablesToDelete, int clientVersion)
            throws IOException, SQLException {
        List<Delete> columnDeletesForBaseTable = new ArrayList<>(tableMetadata.size());
        // Isolate the deletes relevant to dropping columns. Also figure out what kind of columns
        // are being added.
        for (Mutation m : tableMetadata) {
            if (m instanceof Delete) {
                byte[][] rkmd = new byte[5][];
                int pkCount = getVarChars(m.getRow(), rkmd);
                if (pkCount > COLUMN_NAME_INDEX
                        && Bytes.compareTo(schemaName, rkmd[SCHEMA_NAME_INDEX]) == 0
                        && Bytes.compareTo(tableName, rkmd[TABLE_NAME_INDEX]) == 0) {
                    columnDeletesForBaseTable.add((Delete) m);
                }
            }
        }
        for (TableInfo viewInfo : childViewsResult.getLinks()) {
            byte[] viewTenantId = viewInfo.getTenantId();
            byte[] viewSchemaName = viewInfo.getSchemaName();
            byte[] viewName = viewInfo.getTableName();
            PTable view =
                    doGetTable(viewTenantId, viewSchemaName,
                        viewName, clientTimeStamp, null, clientVersion, false,
                        false, basePhysicalTable);
            for (Delete columnDeleteForBaseTable : columnDeletesForBaseTable) {
                PColumn existingViewColumn = null;
                byte[][] rkmd = new byte[5][];
                getVarChars(columnDeleteForBaseTable.getRow(), rkmd);
                String columnName = Bytes.toString(rkmd[COLUMN_NAME_INDEX]);
                String columnFamily =
                        rkmd[FAMILY_NAME_INDEX] == null ? null : Bytes
                                .toString(rkmd[FAMILY_NAME_INDEX]);
                try {   
                    existingViewColumn =
                            columnFamily == null ? view.getColumnForColumnName(columnName) : view
                                    .getColumnFamily(columnFamily).getPColumnForColumnName(columnName);
                } catch (ColumnFamilyNotFoundException e) {
                    // ignore since it means that the column family is not present for the column to
                    // be added.
                } catch (ColumnNotFoundException e) {
                    // ignore since it means the column is not present in the view
                }

                // check if the view where expression contains the column being dropped and prevent
                // it
                if (existingViewColumn != null && view.getViewStatement() != null) {
                    ParseNode viewWhere =
                            new SQLParser(view.getViewStatement()).parseQuery().getWhere();
                    PhoenixConnection conn=null;
                    try {
                        conn = QueryUtil.getConnectionOnServer(env.getConfiguration()).unwrap(
                            PhoenixConnection.class);
                    } catch (ClassNotFoundException e) {
                    }
                    PhoenixStatement statement = new PhoenixStatement(conn);
                    TableRef baseTableRef = new TableRef(basePhysicalTable);
                    ColumnResolver columnResolver = FromCompiler.getResolver(baseTableRef);
                    StatementContext context = new StatementContext(statement, columnResolver);
                    Expression whereExpression = WhereCompiler.compile(context, viewWhere);
                    Expression colExpression =
                            new ColumnRef(baseTableRef, existingViewColumn.getPosition())
                                    .newColumnExpression();
                    ColumnFinder columnFinder = new ColumnFinder(colExpression);
                    whereExpression.accept(columnFinder);
                    if (columnFinder.getColumnFound()) {
                        return new MetaDataMutationResult(MutationCode.UNALLOWED_TABLE_MUTATION,
                                EnvironmentEdgeManager.currentTimeMillis(), basePhysicalTable);
                    }
                }

                if (existingViewColumn != null) {
                    // drop any view indexes that need this column
                    MetaDataMutationResult result =
                            dropIndexes(view, region, invalidateList, locks, clientTimeStamp,
                                view.getSchemaName().getBytes(), view.getTableName().getBytes(),
                                mutationsForAddingColumnsToViews, existingViewColumn,
                                tableNamesToDelete, sharedTablesToDelete, clientVersion, basePhysicalTable);
                    if (result != null) {
                        return result;
                    }
                }
            }

        }
        return null;
    }

    private int getInteger(Put p, byte[] family, byte[] qualifier) {
        List<Cell> cells = p.get(family, qualifier);
        if (cells != null && cells.size() > 0) {
            Cell cell = cells.get(0);
            return (Integer)PInteger.INSTANCE.toObject(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
        }
        return 0;
    }

    private enum MutatateColumnType {
        ADD_COLUMN, DROP_COLUMN
    }

    @Override
    public void addColumn(RpcController controller, final AddColumnRequest request,
            RpcCallback<MetaDataResponse> done) {
        try {
            List<Mutation> tableMetaData = ProtobufUtil.getMutations(request);

            MetaDataMutationResult result = mutateColumn(MutatateColumnType.ADD_COLUMN, tableMetaData, new ColumnMutator() {
                @Override
                public MetaDataMutationResult updateMutation(PTable table, byte[][] rowKeyMetaData,
                        List<Mutation> tableMetaData, Region region, List<ImmutableBytesPtr> invalidateList,
                        List<RowLock> locks, long clientTimeStamp) throws IOException, SQLException {
                    byte[] tenantId = rowKeyMetaData[TENANT_ID_INDEX];
                    byte[] schemaName = rowKeyMetaData[SCHEMA_NAME_INDEX];
                    byte[] tableName = rowKeyMetaData[TABLE_NAME_INDEX];
                    PTableType type = table.getType();
                    byte[] tableHeaderRowKey = SchemaUtil.getTableKey(tenantId,
                            schemaName, tableName);
                    byte[] cPhysicalTableName=table.getPhysicalName().getBytes();
                    getCoprocessorHost().preAlterTable(Bytes.toString(tenantId),
                            SchemaUtil.getTableName(schemaName, tableName), TableName.valueOf(cPhysicalTableName),
                            getParentPhysicalTableName(table),type);

                    List<Mutation> additionalTableMetadataMutations = Lists.newArrayListWithExpectedSize(2);
                    if (type == PTableType.TABLE || type == PTableType.SYSTEM) {
                        TableViewFinderResult childViewsResult = new TableViewFinderResult();
                        findAllChildViews(tenantId, table.getSchemaName().getBytes(), table.getTableName().getBytes(), childViewsResult);
                        if (childViewsResult.hasLinks()) {
                            // Dis-allow if:
                            //
                            // 1) The base column count is 0 which means that the metadata hasn't been upgraded yet or
                            // the upgrade is currently in progress.
                            //
                            // 2) If the request is from a client that is older than 4.5 version of phoenix.
                            // Starting from 4.5, metadata requests have the client version included in them.
                            // We don't want to allow clients before 4.5 to add a column to the base table if it
                            // has views.
                            //
                            // 3) Trying to switch tenancy of a table that has views
                            //
                            // 4) From 4.15 onwards we allow SYSTEM.CATALOG to split and no longer propagate parent
                            // metadata changes to child views.
                            // If the client is on a version older than 4.15 we have to block adding a column to a
                            // parent able as we no longer lock the parent table on the server side while creating a
                            // child view to prevent conflicting changes. This is handled on the client side from
                            // 4.15 onwards.
                            // Also if QueryServices.ALLOW_SPLITTABLE_SYSTEM_CATALOG_ROLLBACK is true, we block adding
                            // a column to a parent table so that we can rollback the upgrade if required.
                            if (table.getBaseColumnCount() == 0
                                    || !request.hasClientVersion()
                                    || switchAttribute(table, table.isMultiTenant(), tableMetaData, MULTI_TENANT_BYTES)) {
                                return new MetaDataMutationResult(MutationCode.UNALLOWED_TABLE_MUTATION,
                                        EnvironmentEdgeManager.currentTimeMillis(), null);
                            }
                            else if (request.getClientVersion()< MIN_SPLITTABLE_SYSTEM_CATALOG ) {
                                logger.error(
                                    "Unable to add a column as the client is older than "
                                            + MIN_SPLITTABLE_SYSTEM_CATALOG);
                                return new MetaDataMutationResult(MutationCode.UNALLOWED_TABLE_MUTATION,
                                        EnvironmentEdgeManager.currentTimeMillis(), null);
                            }
                            else if (allowSystemCatalogRollback) {
                                logger.error("Unable to add a column as the "
                                        + QueryServices.ALLOW_SPLITTABLE_SYSTEM_CATALOG_ROLLBACK
                                        + " config is set to true");
                                return new MetaDataMutationResult(MutationCode.UNALLOWED_TABLE_MUTATION,
                                        EnvironmentEdgeManager.currentTimeMillis(), null);
                            }
                            else {
                                MetaDataMutationResult mutationResult =
                                        validateColumnForAddToBaseTable(table,
                                            tableMetaData, rowKeyMetaData, childViewsResult,
                                            clientTimeStamp, request.getClientVersion());
                                // return if validation was not successful
                                if (mutationResult!=null)
                                    return mutationResult;
                            } 
                        }
                    } 
                    boolean addingCol = false;
                    for (Mutation m : tableMetaData) {
                        byte[] key = m.getRow();
                        boolean addingPKColumn = false;
                        int pkCount = getVarChars(key, rowKeyMetaData);
                        // this means we have are adding a column 
                        if (pkCount > COLUMN_NAME_INDEX
                                && Bytes.compareTo(schemaName, rowKeyMetaData[SCHEMA_NAME_INDEX]) == 0
                                && Bytes.compareTo(tableName, rowKeyMetaData[TABLE_NAME_INDEX]) == 0) {
                            try {
                                addingCol = true;
                                byte[] familyName = null;
                                byte[] colName = null;
                                if (pkCount > FAMILY_NAME_INDEX) {
                                    familyName = rowKeyMetaData[PhoenixDatabaseMetaData.FAMILY_NAME_INDEX];
                                }
                                if (pkCount > COLUMN_NAME_INDEX) {
                                    colName = rowKeyMetaData[PhoenixDatabaseMetaData.COLUMN_NAME_INDEX];
                                }
                                if (table.getExcludedColumns().contains(
                                    PColumnImpl.createExcludedColumn(newPName(familyName), newPName(colName), 0l))) {
                                    // if this column was previously dropped in a view do not allow adding the column back
                                    return new MetaDataMutationResult(
                                            MutationCode.UNALLOWED_TABLE_MUTATION, EnvironmentEdgeManager.currentTimeMillis(), null);
                                }
                                if (familyName!=null && familyName.length > 0) {
                                    PColumnFamily family =
                                            table.getColumnFamily(familyName);
                                            family.getPColumnForColumnNameBytes(colName);
                                } else if (colName!=null && colName.length > 0) {
                                    addingPKColumn = true;
                                    table.getPKColumn(new String(colName));
                                } else {
                                    continue;
                                }
                                return new MetaDataMutationResult(
                                        MutationCode.COLUMN_ALREADY_EXISTS, EnvironmentEdgeManager
                                        .currentTimeMillis(), table);
                            } catch (ColumnFamilyNotFoundException e) {
                                continue;
                            } catch (ColumnNotFoundException e) {
                                if (addingPKColumn) {
                                    // We may be adding a DESC column, so if table is already
                                    // able to be rowKeyOptimized, it should continue to be so.
                                    if (table.rowKeyOrderOptimizable()) {
                                        UpgradeUtil.addRowKeyOrderOptimizableCell(additionalTableMetadataMutations, tableHeaderRowKey, clientTimeStamp);
                                    } else if (table.getType() == PTableType.VIEW){
                                        // Don't allow view PK to diverge from table PK as our upgrade code
                                        // does not handle this.
                                        return new MetaDataMutationResult(
                                                MutationCode.UNALLOWED_TABLE_MUTATION, EnvironmentEdgeManager
                                                .currentTimeMillis(), null);
                                    }
                                    // Add all indexes to invalidate list, as they will all be
                                    // adding the same PK column. No need to lock them, as we
                                    // have the parent table lock at this point.
                                    for (PTable index : table.getIndexes()) {
                                        invalidateList.add(new ImmutableBytesPtr(SchemaUtil
                                                .getTableKey(tenantId, index.getSchemaName()
                                                        .getBytes(), index.getTableName()
                                                        .getBytes())));
                                        // We may be adding a DESC column, so if index is already
                                        // able to be rowKeyOptimized, it should continue to be so.
                                        if (index.rowKeyOrderOptimizable()) {
                                            byte[] indexHeaderRowKey = SchemaUtil.getTableKey(index.getTenantId() == null ? ByteUtil.EMPTY_BYTE_ARRAY : index.getTenantId().getBytes(),
                                                    index.getSchemaName().getBytes(), index.getTableName().getBytes());
                                            UpgradeUtil.addRowKeyOrderOptimizableCell(additionalTableMetadataMutations, indexHeaderRowKey, clientTimeStamp);
                                        }
                                    }
                                }
                                continue;
                            }
                        } else if (pkCount == COLUMN_NAME_INDEX &&
                                   ! (Bytes.compareTo(schemaName, rowKeyMetaData[SCHEMA_NAME_INDEX]) == 0 &&
                                      Bytes.compareTo(tableName, rowKeyMetaData[TABLE_NAME_INDEX]) == 0 ) ) {
                            // Invalidate any table with mutations
                            // TODO: this likely means we don't need the above logic that
                            // loops through the indexes if adding a PK column, since we'd
                            // always have header rows for those.
                            invalidateList.add(new ImmutableBytesPtr(SchemaUtil
                                    .getTableKey(tenantId,
                                            rowKeyMetaData[SCHEMA_NAME_INDEX],
                                            rowKeyMetaData[TABLE_NAME_INDEX])));
                        }
                    }
                    tableMetaData.addAll(additionalTableMetadataMutations);
                    if (type == PTableType.VIEW) {
                        if (EncodedColumnsUtil.usesEncodedColumnNames(table) && addingCol
                                && !table.isAppendOnlySchema()) {
                            // When adding a column to a view that uses encoded column name
                            // scheme, we need to modify the CQ counters stored in the view's
                            // physical table. So to make sure clients get the latest PTable, we
                            // need to invalidate the cache entry.
                            // If the table uses APPEND_ONLY_SCHEMA we use the position of the
                            // column as the encoded column qualifier and so we don't need to
                            // update the CQ counter in the view physical table (see
                            // PHOENIX-4737)
                            invalidateList.add(new ImmutableBytesPtr(
                                    MetaDataUtil.getPhysicalTableRowForView(table)));
                        }
                        // Pass in null as the parent PTable, since we always want to tag the cells
                        // in this case, irrespective of the property values of the parent
                        addTagsToPutsForViewAlteredProperties(tableMetaData, null);
                    }
                    return null;
                }
            }, request.getClientVersion());
            if (result != null) {
                done.run(MetaDataMutationResult.toProto(result));
            }
        } catch (Throwable e) {
            logger.error("Add column failed: ", e);
            ProtobufUtil.setControllerException(controller,
                ServerUtil.createIOException("Error when adding column: ", e));
        }
    }

    /**
     * See PHOENIX-4763. If we are modifying any table-level properties that are mutable on a view,
     * we mark these cells in SYSTEM.CATALOG with tags to indicate that this view property should
     * not be kept in-sync with the base table and so we shouldn't propagate the base table's
     * property value when resolving the view
     * @param tableMetaData list of mutations on the view
     * @param parent PTable of the parent or null
     */
    private void addTagsToPutsForViewAlteredProperties(List<Mutation> tableMetaData,
            PTable parent) {
        byte[] parentUpdateCacheFreqBytes = null;
        byte[] parentUseStatsForParallelizationBytes = null;
        if (parent != null) {
            parentUpdateCacheFreqBytes = new byte[PLong.INSTANCE.getByteSize()];
            PLong.INSTANCE.getCodec().encodeLong(parent.getUpdateCacheFrequency(),
                    parentUpdateCacheFreqBytes, 0);
            if (parent.useStatsForParallelization() != null) {
                parentUseStatsForParallelizationBytes =
                        PBoolean.INSTANCE.toBytes(parent.useStatsForParallelization());
            }
        }
        for (Mutation m: tableMetaData) {
            if (m instanceof Put) {
                MetaDataUtil.conditionallyAddTagsToPutCells((Put)m,
                        PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES,
                        PhoenixDatabaseMetaData.UPDATE_CACHE_FREQUENCY_BYTES,
                        ((ExtendedCellBuilder) env.getCellBuilder()),
                        parentUpdateCacheFreqBytes,
                        VIEW_MODIFIED_PROPERTY_BYTES);
                MetaDataUtil.conditionallyAddTagsToPutCells((Put)m,
                        PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES,
                        PhoenixDatabaseMetaData.USE_STATS_FOR_PARALLELIZATION_BYTES,
                        ((ExtendedCellBuilder) env.getCellBuilder()),
                        parentUseStatsForParallelizationBytes,
                        VIEW_MODIFIED_PROPERTY_BYTES);
            }

        }
    }

    private PTable doGetTable(byte[] tenantId, byte[] schemaName, byte[] tableName,
            long clientTimeStamp, int clientVersion) throws IOException, SQLException {
        return doGetTable(tenantId, schemaName, tableName, clientTimeStamp, null, clientVersion,
            false, false, null);
    }

    /**
     * Looks up the table locally if its present on this region, or else makes an rpc call
     * to look up the region using PhoenixRuntime.getTable 
     * @param lockedAncestorTable TODO
     */
    private PTable doGetTable(byte[] tenantId, byte[] schemaName, byte[] tableName,
            long clientTimeStamp, RowLock rowLock, int clientVersion, boolean skipAddingIndexes,
            boolean skipAddingParentColumns, PTable lockedAncestorTable) throws IOException, SQLException {
        Region region = env.getRegion();
        final byte[] key = SchemaUtil.getTableKey(tenantId, schemaName, tableName);
        // if this region doesn't contain the metadata rows look up the table by using PhoenixRuntime.getTable
        if (!region.getRegionInfo().containsRow(key)) {
            Properties props = new Properties();
            if (tenantId != null) {
                props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, Bytes.toString(tenantId));
            }
            if (clientTimeStamp != HConstants.LATEST_TIMESTAMP) {
                props.setProperty("CurrentSCN", Long.toString(clientTimeStamp));
            }
            try (PhoenixConnection connection =
                    QueryUtil.getConnectionOnServer(props, env.getConfiguration())
                            .unwrap(PhoenixConnection.class)) {
                ConnectionQueryServices queryServices = connection.getQueryServices();
                MetaDataMutationResult result =
                        queryServices.getTable(PNameFactory.newName(tenantId), schemaName,
                            tableName, HConstants.LATEST_TIMESTAMP, clientTimeStamp,
                            skipAddingIndexes, skipAddingParentColumns, lockedAncestorTable);
                return result.getTable();
            } catch (ClassNotFoundException e) {
            }
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
                rowLock = acquireLock(region, key, null);
            }
            PTable table =
                    getTableFromCache(cacheKey, clientTimeStamp, clientVersion, skipAddingIndexes,
                        skipAddingParentColumns, lockedAncestorTable);
            table = modifyIndexStateForOldClient(clientVersion, table);
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
            // Query for the latest table first, since it's not cached
            table =
                    buildTable(key, cacheKey, region, HConstants.LATEST_TIMESTAMP, clientVersion,
                        skipAddingIndexes, skipAddingParentColumns, lockedAncestorTable);
            if ((table != null && table.getTimeStamp() < clientTimeStamp) || 
                    (blockWriteRebuildIndex && table.getIndexDisableTimestamp() > 0)) {
                return table;
            }
            // Otherwise, query for an older version of the table - it won't be cached
            table =
                    buildTable(key, cacheKey, region, clientTimeStamp, clientVersion,
                        skipAddingIndexes, skipAddingParentColumns, lockedAncestorTable);
            return table;
        } finally {
            if (!wasLocked && rowLock!=null) rowLock.release();
        }
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
        List<RowLock> rowLocks = new ArrayList<RowLock>(keys.size());;
        try {
            for (int i = 0; i < keys.size(); i++) {
                acquireLock(region, keys.get(i), rowLocks);
            }

            List<PFunction> functionsAvailable = new ArrayList<PFunction>(keys.size());
            int numFunctions = keys.size();
            Iterator<byte[]> iterator = keys.iterator();
            while(iterator.hasNext()) {
                byte[] key = iterator.next();
                PFunction function = (PFunction)metaDataCache.getIfPresent(new FunctionBytesPtr(key));
                if (function != null && function.getTimeStamp() < clientTimeStamp) {
                    if (isFunctionDeleted(function)) {
                        return null;
                    }
                    functionsAvailable.add(function);
                    iterator.remove();
                }
            }
            if(functionsAvailable.size() == numFunctions) return functionsAvailable;

            // Query for the latest table first, since it's not cached
            List<PFunction> buildFunctions =
                    buildFunctions(keys, region, clientTimeStamp, false,
                        Collections.<Mutation> emptyList());
            if(buildFunctions == null || buildFunctions.isEmpty()) {
                return null;
            }
            functionsAvailable.addAll(buildFunctions);
            if(functionsAvailable.size() == numFunctions) return functionsAvailable;
            return null;
        } finally {
            ServerUtil.releaseRowLocks(rowLocks);
        }
    }
    
    private PColumn getColumn(int pkCount, byte[][] rowKeyMetaData, PTable table) throws ColumnFamilyNotFoundException, ColumnNotFoundException {
        PColumn col = null;
        if (pkCount > FAMILY_NAME_INDEX
            && rowKeyMetaData[PhoenixDatabaseMetaData.FAMILY_NAME_INDEX].length > 0) {
            PColumnFamily family =
                table.getColumnFamily(rowKeyMetaData[PhoenixDatabaseMetaData.FAMILY_NAME_INDEX]);
            col =
                family.getPColumnForColumnNameBytes(rowKeyMetaData[PhoenixDatabaseMetaData.COLUMN_NAME_INDEX]);
        } else if (pkCount > COLUMN_NAME_INDEX
            && rowKeyMetaData[PhoenixDatabaseMetaData.COLUMN_NAME_INDEX].length > 0) {
            col = table.getPKColumn(new String(rowKeyMetaData[PhoenixDatabaseMetaData.COLUMN_NAME_INDEX]));
        }
        return col;
    }

    @Override
    public void dropColumn(RpcController controller, final DropColumnRequest request,
            RpcCallback<MetaDataResponse> done) {
        List<Mutation> tableMetaData = null;
        final List<byte[]> tableNamesToDelete = Lists.newArrayList();
        final List<SharedTableState> sharedTablesToDelete = Lists.newArrayList();
        try {
            tableMetaData = ProtobufUtil.getMutations(request);
            MetaDataMutationResult result = mutateColumn(MutatateColumnType.DROP_COLUMN, tableMetaData, new ColumnMutator() {
                @Override
                public MetaDataMutationResult updateMutation(PTable table, byte[][] rowKeyMetaData,
                        List<Mutation> tableMetaData, Region region,
                        List<ImmutableBytesPtr> invalidateList, List<RowLock> locks, long clientTimeStamp)
                        throws IOException, SQLException {

                    byte[] tenantId = rowKeyMetaData[TENANT_ID_INDEX];
                    byte[] schemaName = rowKeyMetaData[SCHEMA_NAME_INDEX];
                    byte[] tableName = rowKeyMetaData[TABLE_NAME_INDEX];
                    boolean isView = table.getType() == PTableType.VIEW;
                    boolean deletePKColumn = false;
                    getCoprocessorHost().preAlterTable(Bytes.toString(tenantId),
                            SchemaUtil.getTableName(schemaName, tableName),
                            TableName.valueOf(table.getPhysicalName().getBytes()),
                            getParentPhysicalTableName(table),table.getType());

                    List<Mutation> additionalTableMetaData = Lists.newArrayList();
                    PTableType type = table.getType();
                    if (type == PTableType.TABLE || type == PTableType.SYSTEM) {
                        TableViewFinderResult childViewsResult = new TableViewFinderResult();
                        findAllChildViews(tenantId, table.getSchemaName().getBytes(), table.getTableName().getBytes(), childViewsResult);
                        if (childViewsResult.hasLinks()) {
                            MetaDataMutationResult mutationResult =
                                    dropViewIndexes(region, table,
                                        locks, tableMetaData, additionalTableMetaData,
                                        schemaName, tableName, invalidateList,
                                        clientTimeStamp, childViewsResult, tableNamesToDelete, sharedTablesToDelete, request.getClientVersion());
                            // return if we were not able to drop view indexes that need this column successfully
                            if (mutationResult != null) return mutationResult;
                        }
                    }
                    ListIterator<Mutation> iterator = tableMetaData.listIterator();
                    while (iterator.hasNext()) {
                        Mutation mutation = iterator.next();
                        byte[] key = mutation.getRow();
                        int pkCount = getVarChars(key, rowKeyMetaData);
                        if (isView && mutation instanceof Put) {
                            PColumn column = getColumn(pkCount, rowKeyMetaData, table);
                            if (column == null)
                                continue;
                            // ignore any puts that modify the ordinal positions of columns
                            iterator.remove();
                        } 
                        else if (mutation instanceof Delete) {
                            if (pkCount > COLUMN_NAME_INDEX
                                && Bytes.compareTo(schemaName, rowKeyMetaData[SCHEMA_NAME_INDEX]) == 0
                                && Bytes.compareTo(tableName, rowKeyMetaData[TABLE_NAME_INDEX]) == 0) {
                                PColumn columnToDelete = null;
                                try {
                                    columnToDelete = getColumn(pkCount, rowKeyMetaData, table);
                                    if (columnToDelete == null)
                                        continue;
                                    deletePKColumn = columnToDelete.getFamilyName() == null;
                                    if (isView) {
                                        // if we are dropping a derived column add it to the excluded column list
                                        if (columnToDelete.isDerived()) {
                                            mutation = MetaDataUtil
                                                .cloneDeleteToPutAndAddColumn((Delete) mutation, TABLE_FAMILY_BYTES, LINK_TYPE_BYTES, LinkType.EXCLUDED_COLUMN.getSerializedValueAsByteArray());
                                            iterator.set(mutation);
                                        }

                                        if (table.getBaseColumnCount() != DIVERGED_VIEW_BASE_COLUMN_COUNT
                                            && columnToDelete.isDerived()) {
                                            /*
                                             * If the column being dropped is inherited from the base table, then the
                                             * view is about to diverge itself from the base table. The consequence of
                                             * this divergence is that that any further meta-data changes made to the
                                             * base table will not be propagated to the hierarchy of views where this
                                             * view is the root.
                                             */
                                            byte[] viewKey = SchemaUtil.getTableKey(tenantId, schemaName, tableName);
                                            Put updateBaseColumnCountPut = new Put(viewKey);
                                            byte[] baseColumnCountPtr = new byte[PInteger.INSTANCE.getByteSize()];
                                            PInteger.INSTANCE.getCodec().encodeInt(DIVERGED_VIEW_BASE_COLUMN_COUNT,
                                                baseColumnCountPtr, 0);
                                            updateBaseColumnCountPut.addColumn(PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES,
                                                PhoenixDatabaseMetaData.BASE_COLUMN_COUNT_BYTES, clientTimeStamp,
                                                baseColumnCountPtr);
                                            additionalTableMetaData.add(updateBaseColumnCountPut);
                                        }
                                    }
                                    if (columnToDelete.isViewReferenced()) { // Disallow deletion of column referenced in WHERE clause of view
                                        return new MetaDataMutationResult(MutationCode.UNALLOWED_TABLE_MUTATION, EnvironmentEdgeManager.currentTimeMillis(), table, columnToDelete);
                                    }
                                    // drop any indexes that need the column that is going to be dropped
                                    MetaDataMutationResult result = dropIndexes(table, region, invalidateList, locks,
                                        clientTimeStamp, schemaName, tableName,
                                        additionalTableMetaData, columnToDelete,
                                        tableNamesToDelete, sharedTablesToDelete, request.getClientVersion(), null);
                                    if (result != null) {
                                        return result;
                                    }
                                } catch (ColumnFamilyNotFoundException e) {
                                    return new MetaDataMutationResult(
                                        MutationCode.COLUMN_NOT_FOUND, EnvironmentEdgeManager
                                        .currentTimeMillis(), table, columnToDelete);
                                } catch (ColumnNotFoundException e) {
                                    return new MetaDataMutationResult(
                                        MutationCode.COLUMN_NOT_FOUND, EnvironmentEdgeManager
                                        .currentTimeMillis(), table, columnToDelete);
                                }
                            }
                        }

                    }
                    if (deletePKColumn) {
                        if (table.getPKColumns().size() == 1) {
                            return new MetaDataMutationResult(MutationCode.NO_PK_COLUMNS,
                                    EnvironmentEdgeManager.currentTimeMillis(), null);
                        }
                    }
                    tableMetaData.addAll(additionalTableMetaData);
                    long currentTime = MetaDataUtil.getClientTimeStamp(tableMetaData);
                    return new MetaDataMutationResult(MutationCode.TABLE_ALREADY_EXISTS, currentTime, null, tableNamesToDelete, sharedTablesToDelete);
                }
            }, request.getClientVersion());
            if (result != null) {
                done.run(MetaDataMutationResult.toProto(result));
            }
        } catch (Throwable e) {
            logger.error("Drop column failed: ", e);
            ProtobufUtil.setControllerException(controller,
                ServerUtil.createIOException("Error when dropping column: ", e));
        }
    }

    private MetaDataMutationResult dropIndexes(PTable table, Region region, List<ImmutableBytesPtr> invalidateList,
            List<RowLock> locks, long clientTimeStamp, byte[] schemaName,
            byte[] tableName, List<Mutation> additionalTableMetaData, PColumn columnToDelete, 
            List<byte[]> tableNamesToDelete, List<SharedTableState> sharedTablesToDelete, int clientVersion, PTable basePhysicalTable)
            throws IOException, SQLException {
        // Look for columnToDelete in any indexes. If found as PK column, get lock and drop the
        // index and then invalidate it
        // Covered columns are deleted from the index by the client
        PhoenixConnection connection = null;
        try {
            connection = table.getIndexes().isEmpty() ? null : QueryUtil.getConnectionOnServer(
                env.getConfiguration()).unwrap(PhoenixConnection.class);
        } catch (ClassNotFoundException e) {
        }
        for (PTable index : table.getIndexes()) {
            byte[] tenantId = index.getTenantId() == null ? ByteUtil.EMPTY_BYTE_ARRAY : index.getTenantId().getBytes();
            IndexMaintainer indexMaintainer = index.getIndexMaintainer(table, connection);
            byte[] indexKey =
                    SchemaUtil.getTableKey(tenantId, index.getSchemaName().getBytes(), index
                            .getTableName().getBytes());
            Pair<String, String> columnToDeleteInfo = new Pair<>(columnToDelete.getFamilyName().getString(), columnToDelete.getName().getString());
            ColumnReference colDropRef = new ColumnReference(columnToDelete.getFamilyName().getBytes(), columnToDelete.getColumnQualifierBytes());
            boolean isColumnIndexed = indexMaintainer.getIndexedColumnInfo().contains(columnToDeleteInfo);
            boolean isCoveredColumn = indexMaintainer.getCoveredColumns().contains(colDropRef);
            // If index requires this column for its pk, then drop it
            if (isColumnIndexed) {
                // Drop the index table. The doDropTable will expand
                // this to all of the table rows and invalidate the
                // index table
                Delete delete = new Delete(indexKey, clientTimeStamp);
                byte[] linkKey =
                        MetaDataUtil.getParentLinkKey(tenantId, schemaName, tableName, index
                                .getTableName().getBytes());
                // Drop the link between the parent table and the
                // index table
                Delete linkDelete = new Delete(linkKey, clientTimeStamp);
                List<Mutation> tableMetaData = Lists.newArrayListWithExpectedSize(2);
                Delete tableDelete = delete;
                tableMetaData.add(tableDelete);
                tableMetaData.add(linkDelete);
                // if the index is not present on the current region make an rpc to drop it
                if (!region.getRegionInfo().containsRow(indexKey)) {
                    Properties props = new Properties();
                    if (tenantId != null) {
                        props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, Bytes.toString(tenantId));
                    }
                    if (clientTimeStamp != HConstants.LATEST_TIMESTAMP) {
                        props.setProperty("CurrentSCN", Long.toString(clientTimeStamp));
                    }
                    ConnectionQueryServices queryServices = connection.getQueryServices();
                    MetaDataMutationResult result =
                            queryServices.dropTable(tableMetaData, PTableType.INDEX, false, true);
                    if (result.getTableNamesToDelete()!=null && !result.getTableNamesToDelete().isEmpty())
                        tableNamesToDelete.addAll(result.getTableNamesToDelete());
                    if (result.getSharedTablesToDelete()!=null && !result.getSharedTablesToDelete().isEmpty())
                        sharedTablesToDelete.addAll(result.getSharedTablesToDelete());
                    if (result.getMutationCode() != MutationCode.TABLE_ALREADY_EXISTS) {
                        return result;
                    }
                }
                else {
                    // Since we're dropping the index, lock it to ensure
                    // that a change in index state doesn't
                    // occur while we're dropping it.
                    acquireLock(region, indexKey, locks);
                    additionalTableMetaData.addAll(tableMetaData);
                    List<Mutation> childLinksMutations = Lists.newArrayList();
                    doDropTable(indexKey, tenantId, index.getSchemaName().getBytes(),
                        index.getTableName().getBytes(), tableName, index.getType(),
                        additionalTableMetaData, childLinksMutations, invalidateList,
                        tableNamesToDelete, sharedTablesToDelete, false, clientVersion);
                    // there should be no child links to delete since we are just dropping an index
                    assert(childLinksMutations.isEmpty());
                    invalidateList.add(new ImmutableBytesPtr(indexKey));
                }
            }
            // If the dropped column is a covered index column, invalidate the index
            else if (isCoveredColumn){
                invalidateList.add(new ImmutableBytesPtr(indexKey));
            }
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
            logger.error("Old client is not compatible when" + " system tables are upgraded to map to namespace");
            ProtobufUtil.setControllerException(controller,
                    ServerUtil.createIOException(
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
                            request.getClientVersion(), false, false, null);
        } catch (Throwable t) {
            logger.error("loading system catalog table inside getVersion failed", t);
            ProtobufUtil.setControllerException(controller,
              ServerUtil.createIOException(
                SchemaUtil.getPhysicalTableName(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES,
                  isTablesMappingEnabled).toString(), t));
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
            for(Cell cell : newKVs) {
                if(Bytes.compareTo(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength(),
                      INDEX_STATE_BYTES, 0, INDEX_STATE_BYTES.length) == 0){
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
            RowLock rowLock = acquireLock(region, key, null);
            if (rowLock == null) {
                throw new IOException("Failed to acquire lock on " + Bytes.toStringBinary(key));
            }
            try {
                Get get = new Get(key);
                get.addColumn(TABLE_FAMILY_BYTES, DATA_TABLE_NAME_BYTES);
                get.addColumn(TABLE_FAMILY_BYTES, INDEX_STATE_BYTES);
                get.addColumn(TABLE_FAMILY_BYTES, INDEX_DISABLE_TIMESTAMP_BYTES);
                get.addColumn(TABLE_FAMILY_BYTES, ROW_KEY_ORDER_OPTIMIZABLE_BYTES);
                Result currentResult = region.get(get);
                if (currentResult.rawCells().length == 0) {
                    builder.setReturnCode(MetaDataProtos.MutationCode.TABLE_NOT_FOUND);
                    builder.setMutationTime(EnvironmentEdgeManager.currentTimeMillis());
                    done.run(builder.build());
                    return;
                }
                Cell dataTableKV = currentResult.getColumnLatestCell(TABLE_FAMILY_BYTES, DATA_TABLE_NAME_BYTES);
                Cell currentStateKV = currentResult.getColumnLatestCell(TABLE_FAMILY_BYTES, INDEX_STATE_BYTES);
                Cell currentDisableTimeStamp = currentResult.getColumnLatestCell(TABLE_FAMILY_BYTES, INDEX_DISABLE_TIMESTAMP_BYTES);
                boolean rowKeyOrderOptimizable = currentResult.getColumnLatestCell(TABLE_FAMILY_BYTES, ROW_KEY_ORDER_OPTIMIZABLE_BYTES) != null;

                //check permission on data table
                long clientTimeStamp = MetaDataUtil.getClientTimeStamp(tableMetadata);
                PTable loadedTable =
                        doGetTable(tenantId, schemaName, tableName, clientTimeStamp, null,
                            request.getClientVersion(), false, false, null);
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
                if (newState == PIndexState.PENDING_DISABLE && currentState != PIndexState.PENDING_DISABLE) {
                    // reset count for first PENDING_DISABLE
                    newKVs.add(PhoenixKeyValueUtil.newKeyValue(key, TABLE_FAMILY_BYTES,
                        PhoenixDatabaseMetaData.PENDING_DISABLE_COUNT_BYTES, timeStamp, Bytes.toBytes(0L)));
                }
                if (currentState == PIndexState.PENDING_DISABLE) {
                    if (newState == PIndexState.ACTIVE) {
                        //before making index ACTIVE check if all clients succeed otherwise keep it PENDING_DISABLE
                        byte[] count = region
                                .get(new Get(key).addColumn(TABLE_FAMILY_BYTES,
                                        PhoenixDatabaseMetaData.PENDING_DISABLE_COUNT_BYTES))
                                .getValue(TABLE_FAMILY_BYTES, PhoenixDatabaseMetaData.PENDING_DISABLE_COUNT_BYTES);
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
                
                if(newState == PIndexState.ACTIVE||newState == PIndexState.PENDING_ACTIVE||newState == PIndexState.DISABLE){
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
                    mutateRowsWithLocks(region, tableMetadata, Collections.<byte[]> emptySet(), HConstants.NO_NONCE,
                        HConstants.NO_NONCE);
                    // Invalidate from cache
                    Cache<ImmutableBytesPtr,PMetaDataEntity> metaDataCache = GlobalCache.getInstance(this.env).getMetaDataCache();
                    metaDataCache.invalidate(cacheKey);
                    if(dataTableKey != null) {
                        metaDataCache.invalidate(new ImmutableBytesPtr(dataTableKey));
                    }
                    if (setRowKeyOrderOptimizableCell || disableTimeStampKVIndex != -1
                            || currentState == PIndexState.DISABLE || newState == PIndexState.BUILDING) {
                        returnTable = doGetTable(tenantId, schemaName, tableName, HConstants.LATEST_TIMESTAMP, rowLock, request.getClientVersion(), false, false, null);
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
          logger.error("updateIndexState failed", t);
            ProtobufUtil.setControllerException(controller,
                ServerUtil.createIOException(SchemaUtil.getTableName(schemaName, tableName), t));
        }
    }

    private static MetaDataMutationResult checkKeyInRegion(byte[] key, Region region, MutationCode code) {
        return ServerUtil.isKeyInRegion(key, region) ? null : 
            new MetaDataMutationResult(code, EnvironmentEdgeManager.currentTimeMillis(), null);
    }

    private static MetaDataMutationResult checkTableKeyInRegion(byte[] key, Region region) {
        MetaDataMutationResult result = checkKeyInRegion(key, region, MutationCode.TABLE_NOT_IN_REGION);
        if (result!=null) {
            logger.error("Table rowkey " + Bytes.toStringBinary(key)
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
            logger.error("clearTableFromCache failed", t);
            ProtobufUtil.setControllerException(controller,
                ServerUtil.createIOException(SchemaUtil.getTableName(schemaName, tableName), t));
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
            acquireLock(region, lockKey, locks);
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
            for(int i = 0; i< functionNamesList.size();i++) {
                byte[] functionName = functionNamesList.get(i).toByteArray();
                functionNames.add(Bytes.toString(functionName));
                byte[] key = SchemaUtil.getFunctionKey(tenantId, functionName);
                MetaDataMutationResult result = checkFunctionKeyInRegion(key, region);
                if (result != null) {
                    done.run(MetaDataMutationResult.toProto(result));
                    return;
                }
                functions.add(new Pair<byte[], Long>(functionName,functionTimestampsList.get(i)));
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
            logger.error("getFunctions failed", t);
            ProtobufUtil.setControllerException(controller,
                ServerUtil.createIOException(functionNames.toString(), t));
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
                acquireLock(region, lockKey, locks);
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
                            if(!request.getReplace()) {
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
                if(!temporaryFunction) {
                    mutateRowsWithLocks(region, functionMetaData, Collections.<byte[]> emptySet(), HConstants.NO_NONCE, HConstants.NO_NONCE);
                }

                // Invalidate the cache - the next getFunction call will add it
                // TODO: consider loading the function that was just created here, patching up the parent function, and updating the cache
                Cache<ImmutableBytesPtr,PMetaDataEntity> metaDataCache = GlobalCache.getInstance(this.env).getMetaDataCache();
                metaDataCache.invalidate(cacheKey);
                // Get timeStamp from mutations - the above method sets it if it's unset
                long currentTimeStamp = MetaDataUtil.getClientTimeStamp(functionMetaData);
                builder.setReturnCode(MetaDataProtos.MutationCode.FUNCTION_NOT_FOUND);
                builder.setMutationTime(currentTimeStamp);
                done.run(builder.build());
                return;
            } finally {
                ServerUtil.releaseRowLocks(locks);
            }
        } catch (Throwable t) {
          logger.error("createFunction failed", t);
            ProtobufUtil.setControllerException(controller,
                ServerUtil.createIOException(Bytes.toString(functionName), t));
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
                acquireLock(region, lockKey, locks);
                List<byte[]> keys = new ArrayList<byte[]>(1);
                keys.add(lockKey);
                List<ImmutableBytesPtr> invalidateList = new ArrayList<ImmutableBytesPtr>();

                result = doDropFunction(clientTimeStamp, keys, functionMetaData, invalidateList);
                if (result.getMutationCode() != MutationCode.FUNCTION_ALREADY_EXISTS) {
                    done.run(MetaDataMutationResult.toProto(result));
                    return;
                }
                mutateRowsWithLocks(region, functionMetaData, Collections.<byte[]> emptySet(), HConstants.NO_NONCE, HConstants.NO_NONCE);

                Cache<ImmutableBytesPtr,PMetaDataEntity> metaDataCache = GlobalCache.getInstance(this.env).getMetaDataCache();
                long currentTime = MetaDataUtil.getClientTimeStamp(functionMetaData);
                for(ImmutableBytesPtr ptr: invalidateList) {
                    metaDataCache.invalidate(ptr);
                    metaDataCache.put(ptr, newDeletedFunctionMarker(currentTime));

                }

                done.run(MetaDataMutationResult.toProto(result));
                return;
            } finally {
                ServerUtil.releaseRowLocks(locks);
            }
        } catch (Throwable t) {
          logger.error("dropFunction failed", t);
            ProtobufUtil.setControllerException(controller,
                ServerUtil.createIOException(Bytes.toString(functionName), t));
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
                try (RegionScanner scanner = region.getScanner(scan);) {
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
                acquireLock(region, lockKey, locks);
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
                mutateRowsWithLocks(region, schemaMutations, Collections.<byte[]> emptySet(), HConstants.NO_NONCE,
                        HConstants.NO_NONCE);

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
                return;
            } finally {
                ServerUtil.releaseRowLocks(locks);
            }
        } catch (Throwable t) {
            logger.error("Creating the schema" + schemaName + "failed", t);
            ProtobufUtil.setControllerException(controller, ServerUtil.createIOException(schemaName, t));
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
                acquireLock(region, lockKey, locks);
                List<ImmutableBytesPtr> invalidateList = new ArrayList<ImmutableBytesPtr>(1);
                result = doDropSchema(clientTimeStamp, schemaName, lockKey, schemaMetaData, invalidateList);
                if (result.getMutationCode() != MutationCode.SCHEMA_ALREADY_EXISTS) {
                    done.run(MetaDataMutationResult.toProto(result));
                    return;
                }
                mutateRowsWithLocks(region, schemaMetaData, Collections.<byte[]> emptySet(), HConstants.NO_NONCE,
                        HConstants.NO_NONCE);
                Cache<ImmutableBytesPtr, PMetaDataEntity> metaDataCache = GlobalCache.getInstance(this.env)
                        .getMetaDataCache();
                long currentTime = MetaDataUtil.getClientTimeStamp(schemaMetaData);
                for (ImmutableBytesPtr ptr : invalidateList) {
                    metaDataCache.invalidate(ptr);
                    metaDataCache.put(ptr, newDeletedSchemaMarker(currentTime));
                }
                done.run(MetaDataMutationResult.toProto(result));
                return;
            } finally {
                ServerUtil.releaseRowLocks(locks);
            }
        } catch (Throwable t) {
            logger.error("drop schema failed:", t);
            ProtobufUtil.setControllerException(controller, ServerUtil.createIOException(schemaName, t));
        }
    }

    private MetaDataMutationResult doDropSchema(long clientTimeStamp, String schemaName, byte[] key,
            List<Mutation> schemaMutations, List<ImmutableBytesPtr> invalidateList) throws IOException, SQLException {
        PSchema schema = loadSchema(env, key, new ImmutableBytesPtr(key), clientTimeStamp, clientTimeStamp);
        boolean areTablesExists = false;
        if (schema == null) { return new MetaDataMutationResult(MutationCode.SCHEMA_NOT_FOUND,
                EnvironmentEdgeManager.currentTimeMillis(), null); }
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
            if (areTablesExists) { return new MetaDataMutationResult(MutationCode.TABLES_EXIST_ON_SCHEMA, schema,
                    EnvironmentEdgeManager.currentTimeMillis()); }
            invalidateList.add(new ImmutableBytesPtr(key));
            return new MetaDataMutationResult(MutationCode.SCHEMA_ALREADY_EXISTS, schema,
                    EnvironmentEdgeManager.currentTimeMillis());
        }
        return new MetaDataMutationResult(MutationCode.SCHEMA_NOT_FOUND, EnvironmentEdgeManager.currentTimeMillis(),
                null);

    }
    
    private void mutateRowsWithLocks(final Region region, final List<Mutation> mutations, final Set<byte[]> rowsToLock,
            final long nonceGroup, final long nonce) throws IOException {
        // we need to mutate SYSTEM.CATALOG with HBase/login user if access is enabled.
        if (this.accessCheckEnabled) {
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
        return table
                .getType() == PTableType.VIEW
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
