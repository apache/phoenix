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
package org.apache.phoenix.mapreduce.util;

import static org.apache.commons.lang3.StringUtils.isNotEmpty;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat.NullDBWritable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.mapreduce.FormatToBytesWritableMapper;
import org.apache.phoenix.mapreduce.ImportPreUpsertKeyValueProcessor;
import org.apache.phoenix.mapreduce.index.IndexScrutinyTool;
import org.apache.phoenix.mapreduce.index.IndexScrutinyTool.OutputFormat;
import org.apache.phoenix.mapreduce.index.IndexScrutinyTool.SourceTable;
import org.apache.phoenix.mapreduce.index.IndexTool;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.ColumnInfo;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.QueryUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.thirdparty.com.google.common.base.Joiner;
import org.apache.phoenix.thirdparty.com.google.common.base.Preconditions;
import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;

/**
 * A utility class to set properties on the {#link Configuration} instance.
 * Used as part of Map Reduce job configuration.
 * 
 */
public final class PhoenixConfigurationUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(PhoenixConfigurationUtil.class);

    public static final String SESSION_ID = "phoenix.sessionid";
    
    public static final String UPSERT_STATEMENT = "phoenix.upsert.stmt";
    
    public static final String SELECT_STATEMENT = "phoenix.select.stmt";
    
    public static final String UPSERT_BATCH_SIZE = "phoenix.upsert.batch.size";
    
    public static final String SCHEMA_TYPE = "phoenix.select.schema.type";
    
    public static final String MAPREDUCE_SELECT_COLUMN_VALUE_PREFIX = "phoenix.mr.select.column.value";
    
    public static final String MAPREDUCE_SELECT_COLUMN_COUNT = "phoenix.mr.select.column.count";
    
    public static final String MAPREDUCE_UPSERT_COLUMN_VALUE_PREFIX = "phoenix.mr.upsert.column.value";
    
    public static final String MAPREDUCE_UPSERT_COLUMN_COUNT = "phoenix.mr.upsert.column.count";
    
    public static final String INPUT_TABLE_NAME = "phoenix.input.table.name" ;
    
    public static final String OUTPUT_TABLE_NAME = "phoenix.colinfo.table.name" ;
    
    public static final String INPUT_TABLE_CONDITIONS = "phoenix.input.table.conditions" ;
    
    /** For local indexes which are stored in a single separate physical table*/
    public static final String PHYSICAL_TABLE_NAME = "phoenix.output.table.name" ;

    public static final String TRANSFORM_RETRY_COUNT_VALUE = "phoenix.transform.retry.count";

    public static final int DEFAULT_TRANSFORM_RETRY_COUNT = 50;
    
    public static final long DEFAULT_UPSERT_BATCH_SIZE = 1000;
    
    public static final String INPUT_CLASS = "phoenix.input.class";
    
    public static final String CURRENT_SCN_VALUE = "phoenix.mr.currentscn.value";
    
    public static final String TX_SCN_VALUE = "phoenix.mr.txscn.value";
    
    public static final String TX_PROVIDER = "phoenix.mr.txprovider";

    /** Configuration key for the class name of an ImportPreUpsertKeyValueProcessor */
    public static final String UPSERT_HOOK_CLASS_CONFKEY = "phoenix.mapreduce.import.kvprocessor";

    public static final String INDEX_DISABLED_TIMESTAMP_VALUE = "phoenix.mr.index.disableTimestamp";

    public static final String INDEX_MAINTAINERS = "phoenix.mr.index.maintainers";

    public static final String SCRUTINY_DATA_TABLE_NAME = "phoenix.mr.scrutiny.data.table.name";

    public static final String SCRUTINY_INDEX_TABLE_NAME = "phoenix.mr.scrutiny.index.table.name";

    public static final String INDEX_TOOL_DATA_TABLE_NAME = "phoenix.mr.index_tool.data.table.name";

    public static final String INDEX_TOOL_INDEX_TABLE_NAME = "phoenix.mr.index_tool.index.table.name";

    public static final String INDEX_TOOL_SOURCE_TABLE = "phoenix.mr.index_tool.source.table";

    public static final String SCRUTINY_SOURCE_TABLE = "phoenix.mr.scrutiny.source.table";

    public static final String SCRUTINY_BATCH_SIZE = "phoenix.mr.scrutiny.batch.size";

    public static final String SCRUTINY_OUTPUT_INVALID_ROWS =
            "phoenix.mr.scrutiny.output.invalid.rows";

    public static final boolean DEFAULT_SCRUTINY_OUTPUT_INVALID_ROWS = false;

    public static final String SHOULD_FIX_UNVERIFIED_TRANSFORM =
            "phoenix.mr.fix.unverified.transform";

    public static final boolean DEFAULT_SHOULD_FIX_UNVERIFIED_TRANSFORM = false;

    public static final String SCRUTINY_OUTPUT_FORMAT = "phoenix.mr.scrutiny.output.format";

    public static final String SCRUTINY_EXECUTE_TIMESTAMP = "phoenix.mr.scrutiny.execute.timestamp";

    // max output rows per mapper
    public static final String SCRUTINY_OUTPUT_MAX = "phoenix.mr.scrutiny.output.max";

    public static final long DEFAULT_SCRUTINY_BATCH_SIZE = 1000;

    public static final String DISABLED_INDEXES = "phoenix.mr.index.disabledIndexes";

    public static final String VERIFY_INDEX = "phoenix.mr.index.verifyIndex";

    public static final String ONLY_VERIFY_INDEX = "phoenix.mr.index.onlyVerifyIndex";

    public static final String INDEX_VERIFY_TYPE = "phoenix.mr.index.IndexVerifyType";

    public static final String DISABLE_LOGGING_TYPE = "phoenix.mr.index" +
        ".IndexDisableLoggingType";

    // Generate splits based on scans from stats, or just from region splits
    public static final String MAPREDUCE_SPLIT_BY_STATS = "phoenix.mapreduce.split.by.stats";

    public static final boolean DEFAULT_SPLIT_BY_STATS = true;

    public static final String SNAPSHOT_NAME_KEY = "phoenix.mapreduce.snapshot.name";

    public static final String RESTORE_DIR_KEY = "phoenix.tableSnapshot.restore.dir";

    public static final String MAPREDUCE_TENANT_ID = "phoenix.mapreduce.tenantid";
    private static final String INDEX_TOOL_END_TIME = "phoenix.mr.index.endtime";
    private static final String INDEX_TOOL_START_TIME = "phoenix.mr.index.starttime";
    private static final String INDEX_TOOL_LAST_VERIFY_TIME = "phoenix.mr.index.last.verify.time";

    public static final String MAPREDUCE_JOB_TYPE = "phoenix.mapreduce.jobtype";

    // group number of views per mapper to run the deletion job
    public static final String MAPREDUCE_MULTI_INPUT_MAPPER_SPLIT_SIZE = "phoenix.mapreduce.multi.input.split.size";

    public static final String MAPREDUCE_MULTI_INPUT_QUERY_BATCH_SIZE = "phoenix.mapreduce.multi.input.batch.size";

    // phoenix ttl data deletion job for a specific view
    public static final String MAPREDUCE_PHOENIX_TTL_DELETE_JOB_PER_VIEW = "phoenix.mapreduce.phoenix_ttl.per_view";

    // phoenix ttl data deletion job for all views.
    public static final String MAPREDUCE_PHOENIX_TTL_DELETE_JOB_ALL_VIEWS = "phoenix.mapreduce.phoenix_ttl.all";

    // provide an absolute path to inject your multi input logic
    public static final String MAPREDUCE_MULTI_INPUT_STRATEGY_CLAZZ = "phoenix.mapreduce.multi.input.strategy.path";

    // provide an absolute path to inject your multi split logic
    public static final String MAPREDUCE_MULTI_INPUT_SPLIT_STRATEGY_CLAZZ = "phoenix.mapreduce.multi.split.strategy.path";

    // provide an absolute path to inject your multi input mapper logic
    public static final String MAPREDUCE_MULTI_INPUT_MAPPER_TRACKER_CLAZZ = "phoenix.mapreduce.multi.mapper.tracker.path";

    // provide control to whether or not handle mapreduce snapshot restore and cleanup operations which
    // is used by scanners on phoenix side internally or handled by caller externally
    public static final String MAPREDUCE_EXTERNAL_SNAPSHOT_RESTORE = "phoenix.mapreduce.external.snapshot.restore";

    // by default MR snapshot restore is handled internally by phoenix
    public static final boolean DEFAULT_MAPREDUCE_EXTERNAL_SNAPSHOT_RESTORE = false;

    // Is the mapreduce used for table/index transform
    public static final String IS_TRANSFORMING_VALUE = "phoenix.mr.istransforming";

    // Is force transform cutover
    public static final String FORCE_CUTOVER_VALUE = "phoenix.mr.force.cutover";

    // Is the mapreduce used for table/index transform
    public static final String TRANSFORMING_TABLE_TYPE = "phoenix.mr.transform.tabletype";

    public static final String IS_PARTIAL_TRANSFORM = "phoenix.mr.transform.ispartial";

    // Randomize mapper execution order
    public static final String MAPREDUCE_RANDOMIZE_MAPPER_EXECUTION_ORDER =
            "phoenix.mapreduce.randomize.mapper.execution.order";

    // non-index jobs benefit less from this
    public static final boolean DEFAULT_MAPREDUCE_RANDOMIZE_MAPPER_EXECUTION_ORDER = false;

    /**
     * Determines type of Phoenix Map Reduce job.
     * 1. QUERY allows running arbitrary queries without aggregates
     * 2. UPDATE_STATS collects statistics for the table
     */
    public enum MRJobType {
        QUERY,
        UPDATE_STATS
    }

    public enum SchemaType {
        TABLE,
        QUERY
    }

    private PhoenixConfigurationUtil(){
        
    }
    /**
     * 
     * @param tableName
     */
    public static void setInputTableName(final Configuration configuration, final String tableName) {
        Preconditions.checkNotNull(configuration);
        Preconditions.checkNotNull(tableName);
        configuration.set(INPUT_TABLE_NAME, tableName);
    }
    
    public static void setInputTableConditions(final Configuration configuration, final String conditions) {
        Preconditions.checkNotNull(configuration);
        Preconditions.checkNotNull(conditions);
        configuration.set(INPUT_TABLE_CONDITIONS, conditions);
    }
    
    private static void setValues(final Configuration configuration, final String[] columns, final String VALUE_COUNT, final String VALUE_NAME) {
		Preconditions.checkNotNull(configuration);
        configuration.setInt(VALUE_COUNT, columns.length);
        for (int i=0; i<columns.length; ++i) {
        	configuration.set(String.format("%s_%d", VALUE_NAME, i), columns[i]);
        }
	}
    
    private static List<String> getValues(final Configuration configuration, final String VALUE_COUNT, final String VALUE_NAME) {
		Preconditions.checkNotNull(configuration);
        int numCols = configuration.getInt(VALUE_COUNT, 0);
        List<String> cols = Lists.newArrayListWithExpectedSize(numCols);
        for (int i=0; i<numCols; ++i) {
        	cols.add(configuration.get(String.format("%s_%d", VALUE_NAME, i)));
        }
        return cols;
	}
    
    public static void setSelectColumnNames(final Configuration configuration, final String[] columns) {
        setValues(configuration, columns, MAPREDUCE_SELECT_COLUMN_COUNT, MAPREDUCE_SELECT_COLUMN_VALUE_PREFIX);
    }
	   
    public static List<String> getSelectColumnNames(final Configuration configuration) {
        return getValues(configuration, MAPREDUCE_SELECT_COLUMN_COUNT, MAPREDUCE_SELECT_COLUMN_VALUE_PREFIX);
    }
    
    public static void setInputClass(final Configuration configuration, Class<? extends DBWritable> inputClass) {
        Preconditions.checkNotNull(configuration);
        configuration.setClass(INPUT_CLASS ,inputClass,DBWritable.class);
    }
    
    public static void setInputQuery(final Configuration configuration, final String inputQuery) {
        Preconditions.checkNotNull(configuration);
        Preconditions.checkNotNull(inputQuery);
        configuration.set(SELECT_STATEMENT, inputQuery);
    }

    public static void setPropertyPolicyProviderDisabled(final Configuration configuration) {
        Preconditions.checkNotNull(configuration);
        configuration.set(QueryServices.PROPERTY_POLICY_PROVIDER_ENABLED, "false");
    }
    
    public static void setSchemaType(Configuration configuration, final SchemaType schemaType) {
        Preconditions.checkNotNull(configuration);
        configuration.set(SCHEMA_TYPE, schemaType.name());
    }

    public static void setMRJobType(Configuration configuration, final MRJobType mrJobType) {
        Preconditions.checkNotNull(configuration);
        configuration.set(MAPREDUCE_JOB_TYPE, mrJobType.name());
    }

    public static void setPhysicalTableName(final Configuration configuration, final String tableName) {
        Preconditions.checkNotNull(configuration);
        Preconditions.checkNotNull(tableName);
        configuration.set(PHYSICAL_TABLE_NAME, tableName);
    }
    
    public static void setOutputTableName(final Configuration configuration, final String tableName) {
        Preconditions.checkNotNull(configuration);
        Preconditions.checkNotNull(tableName);
        configuration.set(OUTPUT_TABLE_NAME, tableName);
    }
    
    public static void setUpsertColumnNames(final Configuration configuration,final String[] columns) {
        setValues(configuration, columns, MAPREDUCE_UPSERT_COLUMN_COUNT, MAPREDUCE_UPSERT_COLUMN_VALUE_PREFIX);
    }

    public static void setSnapshotNameKey(final Configuration configuration, final String snapshotName) {
        Preconditions.checkNotNull(configuration);
        Preconditions.checkNotNull(snapshotName);
        configuration.set(SNAPSHOT_NAME_KEY, snapshotName);
    }

    public static void setRestoreDirKey(final Configuration configuration, final String restoreDir) {
        Preconditions.checkNotNull(configuration);
        Preconditions.checkNotNull(restoreDir);
        configuration.set(RESTORE_DIR_KEY, restoreDir);
    }

    public static void setIndexToolStartTime(Configuration configuration, Long startTime) {
        Preconditions.checkNotNull(configuration);
        Preconditions.checkNotNull(startTime);
        configuration.set(INDEX_TOOL_START_TIME, Long.toString(startTime));
    }

    public static void setIndexToolLastVerifyTime(Configuration configuration, Long lastVerifyTime) {
        Preconditions.checkNotNull(configuration);
        Preconditions.checkNotNull(lastVerifyTime);
        configuration.set(INDEX_TOOL_LAST_VERIFY_TIME, Long.toString(lastVerifyTime));
    }

    public static void setCurrentScnValue(Configuration configuration, Long scn) {
        Preconditions.checkNotNull(configuration);
        Preconditions.checkNotNull(scn);
        configuration.set(CURRENT_SCN_VALUE, Long.toString(scn));
    }

    public static String getIndexToolStartTime(Configuration configuration) {
        Preconditions.checkNotNull(configuration);
        return configuration.get(INDEX_TOOL_START_TIME);
    }

    public static String getCurrentScnValue(Configuration configuration) {
        Preconditions.checkNotNull(configuration);
        return configuration.get(CURRENT_SCN_VALUE);
    }

    public static String getIndexToolLastVerifyTime(Configuration configuration) {
        Preconditions.checkNotNull(configuration);
        return configuration.get(INDEX_TOOL_LAST_VERIFY_TIME);
    }
    
    public static List<String> getUpsertColumnNames(final Configuration configuration) {
        return getValues(configuration, MAPREDUCE_UPSERT_COLUMN_COUNT, MAPREDUCE_UPSERT_COLUMN_VALUE_PREFIX);
    }
   
    public static void setBatchSize(final Configuration configuration, final Long batchSize) {
        Preconditions.checkNotNull(configuration);
        configuration.setLong(UPSERT_BATCH_SIZE, batchSize);
    }
    
    /**
     * Sets which HBase cluster a Phoenix MapReduce job should read from
     * @param configuration
     * @param quorum ZooKeeper quorum string for HBase cluster the MapReduce job will read from
     */
    @Deprecated
    public static void setInputCluster(final Configuration configuration,
            final String quorum) {
        Preconditions.checkNotNull(configuration);
        configuration.set(PhoenixConfigurationUtilHelper.MAPREDUCE_INPUT_CLUSTER_QUORUM, quorum);
    }

    /**
     * Sets which HBase cluster a Phoenix MapReduce job should write to
     * @param configuration
     * @param quorum ZooKeeper quorum string for HBase cluster the MapReduce job will write to
     */
    @Deprecated
    public static void setOutputCluster(final Configuration configuration,
            final String quorum) {
        Preconditions.checkNotNull(configuration);
        configuration.set(PhoenixConfigurationUtilHelper.MAPREDUCE_OUTPUT_CLUSTER_QUORUM, quorum);
    }

    /**
     * Sets which HBase cluster a Phoenix MapReduce job should read from
     * @param configuration
     * @param url Phoenix JDBC URL
     */
    public static void setInputClusterUrl(final Configuration configuration,
            final String url) {
        Preconditions.checkNotNull(configuration);
        configuration.set(PhoenixConfigurationUtilHelper.MAPREDUCE_INPUT_CLUSTER_URL, url);
    }

    /**
     * Sets which HBase cluster a Phoenix MapReduce job should write to
     * @param configuration
     * @param url Phoenix JDBC URL string for HBase cluster the MapReduce job will write to
     */
    public static void setOutputClusterUrl(final Configuration configuration,
            final String url) {
        Preconditions.checkNotNull(configuration);
        configuration.set(PhoenixConfigurationUtilHelper.MAPREDUCE_OUTPUT_CLUSTER_URL, url);
    }

    public static Class<?> getInputClass(final Configuration configuration) {
        return configuration.getClass(INPUT_CLASS, NullDBWritable.class);
    }
    public static SchemaType getSchemaType(final Configuration configuration) {
        final String schemaTp = configuration.get(SCHEMA_TYPE);
        Preconditions.checkNotNull(schemaTp);
        return SchemaType.valueOf(schemaTp);
    }

    public static MRJobType getMRJobType(final Configuration configuration, String defaultMRJobType) {
        final String mrJobType = configuration.get(MAPREDUCE_JOB_TYPE, defaultMRJobType);
        Preconditions.checkNotNull(mrJobType);
        return MRJobType.valueOf(mrJobType);
    }

    public static List<ColumnInfo> getUpsertColumnMetadataList(final Configuration configuration) throws SQLException {
        Preconditions.checkNotNull(configuration);
        List<ColumnInfo> columnMetadataList = null;
        columnMetadataList = ColumnInfoToStringEncoderDecoder.decode(configuration);
        if (columnMetadataList!=null && !columnMetadataList.isEmpty()) {
            return columnMetadataList;
        }
        final String tableName = getOutputTableName(configuration);
        Preconditions.checkNotNull(tableName);
        try (PhoenixConnection connection = ConnectionUtil.getOutputConnection(configuration).
                unwrap(PhoenixConnection.class)) {
            List<String> upsertColumnList =
                    PhoenixConfigurationUtil.getUpsertColumnNames(configuration);
            if(!upsertColumnList.isEmpty()) {
                LOGGER.info(String.format("UseUpsertColumns=%s, upsertColumnList.size()=%s,"
                                + " upsertColumnList=%s ",!upsertColumnList.isEmpty(),
                        upsertColumnList.size(), Joiner.on(",").join(upsertColumnList)));
            }
            columnMetadataList = PhoenixRuntime.generateColumnInfo(connection, tableName,
                    upsertColumnList);
            // we put the encoded column infos in the Configuration for re usability.
            ColumnInfoToStringEncoderDecoder.encode(configuration, columnMetadataList);
        }
		return columnMetadataList;
    }
    
     public static String getUpsertStatement(final Configuration configuration) throws SQLException {
        Preconditions.checkNotNull(configuration);
        String upsertStmt = configuration.get(UPSERT_STATEMENT);
        if(isNotEmpty(upsertStmt)) {
            return upsertStmt;
        }
        final String tableName = getOutputTableName(configuration);
        Preconditions.checkNotNull(tableName);
        List<String> upsertColumnNames = PhoenixConfigurationUtil.getUpsertColumnNames(configuration);
        final List<ColumnInfo> columnMetadataList = getUpsertColumnMetadataList(configuration);
        if (!upsertColumnNames.isEmpty()) {
            // Generating UPSERT statement without column name information.
            upsertStmt = QueryUtil.constructUpsertStatement(tableName, columnMetadataList);
            LOGGER.info("Phoenix Custom Upsert Statement: "+ upsertStmt);
        } else {
            // Generating UPSERT statement without column name information.
            upsertStmt = QueryUtil.constructGenericUpsertStatement(tableName, columnMetadataList.size());
            LOGGER.info("Phoenix Generic Upsert Statement: " + upsertStmt);
        }
        configuration.set(UPSERT_STATEMENT, upsertStmt);
        return upsertStmt;
        
    }

    public static void setUpsertStatement(final Configuration configuration, String upsertStmt)
            throws SQLException {
        Preconditions.checkNotNull(configuration);
        Preconditions.checkNotNull(upsertStmt);
        configuration.set(UPSERT_STATEMENT, upsertStmt);
    }

    public static void setMultiInputMapperSplitSize(Configuration configuration, final int splitSize) {
        Preconditions.checkNotNull(configuration);
        configuration.set(MAPREDUCE_MULTI_INPUT_MAPPER_SPLIT_SIZE, String.valueOf(splitSize));
    }

    public static void setMultiViewQueryMoreSplitSize(Configuration configuration, final int batchSize) {
        Preconditions.checkNotNull(configuration);
        configuration.set(MAPREDUCE_MULTI_INPUT_QUERY_BATCH_SIZE, String.valueOf(batchSize));
    }

    public static int getMultiViewQueryMoreSplitSize(final Configuration configuration) {
        final String batchSize = configuration.get(MAPREDUCE_MULTI_INPUT_QUERY_BATCH_SIZE);
        Preconditions.checkNotNull(batchSize);
        return Integer.parseInt(batchSize);
    }

    public static List<ColumnInfo> getSelectColumnMetadataList(final Configuration configuration) throws SQLException {
        Preconditions.checkNotNull(configuration);
        List<ColumnInfo> columnMetadataList = null;
        columnMetadataList = ColumnInfoToStringEncoderDecoder.decode(configuration);
        if (columnMetadataList!=null && !columnMetadataList.isEmpty()) {
            return columnMetadataList;
        }
        final String tableName = getInputTableName(configuration);
        Preconditions.checkNotNull(tableName);
        Properties props = new Properties();
        String tenantId = configuration.get(PhoenixConfigurationUtil.MAPREDUCE_TENANT_ID);
        if (tenantId != null) {
            props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
        }
        try (PhoenixConnection connection = ConnectionUtil.
                getInputConnection(configuration, props).unwrap(PhoenixConnection.class)) {
            final List<String> selectColumnList = getSelectColumnList(configuration);
            columnMetadataList =
                    PhoenixRuntime.generateColumnInfo(connection, tableName, selectColumnList);
            // we put the encoded column infos in the Configuration for re usability.
            ColumnInfoToStringEncoderDecoder.encode(configuration, columnMetadataList);
        }
        return columnMetadataList;
    }

    public static int getMultiViewSplitSize(final Configuration configuration) {
        final String splitSize = configuration.get(MAPREDUCE_MULTI_INPUT_MAPPER_SPLIT_SIZE);
        Preconditions.checkNotNull(splitSize);
        return Integer.parseInt(splitSize);
    }

    private static List<String> getSelectColumnList(
            final Configuration configuration) {
    	List<String> selectColumnList = PhoenixConfigurationUtil.getSelectColumnNames(configuration);
        if(!selectColumnList.isEmpty()) {
            LOGGER.info(String.format("UseSelectColumns=%s, selectColumnList.size()=%s, " +
                            "selectColumnList=%s ",!selectColumnList.isEmpty(),
                    selectColumnList.size(), Joiner.on(",").join(selectColumnList)));
        }
        return selectColumnList;
    }

    public static String getSelectStatement(final Configuration configuration) throws SQLException {
        Preconditions.checkNotNull(configuration);
        String selectStmt = configuration.get(SELECT_STATEMENT);
        if(isNotEmpty(selectStmt)) {
            LOGGER.info("Select Statement: " + selectStmt);
            return selectStmt;
        }
        final String tableName = getInputTableName(configuration);
        Preconditions.checkNotNull(tableName);
        final List<ColumnInfo> columnMetadataList = getSelectColumnMetadataList(configuration);
        final String conditions = configuration.get(INPUT_TABLE_CONDITIONS);
        LOGGER.info("Building select statement from input conditions: " + conditions);
        selectStmt = QueryUtil.constructSelectStatement(tableName, columnMetadataList, conditions);
        LOGGER.info("Select Statement: " + selectStmt);
        configuration.set(SELECT_STATEMENT, selectStmt);
        return selectStmt;
    }


    public static long getBatchSize(final Configuration configuration) throws SQLException {
        Preconditions.checkNotNull(configuration);
        long batchSize = configuration.getLong(UPSERT_BATCH_SIZE, DEFAULT_UPSERT_BATCH_SIZE);
        if(batchSize <= 0) {
           try (Connection conn = ConnectionUtil.getOutputConnection(configuration)) {
               batchSize = ((PhoenixConnection) conn).getMutateBatchSize();
           }
        }
        configuration.setLong(UPSERT_BATCH_SIZE, batchSize);
        return batchSize;
    }
    
    public static int getSelectColumnsCount(Configuration configuration,
            String tableName) throws SQLException {
        Preconditions.checkNotNull(configuration);
        final String schemaTp = configuration.get(SCHEMA_TYPE);
        final SchemaType schemaType = SchemaType.valueOf(schemaTp);
        int count = 0;
        if(SchemaType.QUERY.equals(schemaType)) {
            List<String> selectedColumnList = getSelectColumnList(configuration);
            count = selectedColumnList == null ? 0 : selectedColumnList.size();
        } else {
            List<ColumnInfo> columnInfos = getSelectColumnMetadataList(configuration);
            count = columnInfos == null ? 0 : columnInfos.size();
        }
        return count;
    }

    public static String getInputTableName(Configuration configuration) {
        Preconditions.checkNotNull(configuration);
        return configuration.get(INPUT_TABLE_NAME);
    }

    public static String getPhysicalTableName(Configuration configuration) {
        Preconditions.checkNotNull(configuration);
        return configuration.get(PHYSICAL_TABLE_NAME);
    }
    
    public static String getOutputTableName(Configuration configuration) {
        Preconditions.checkNotNull(configuration);
        return configuration.get(OUTPUT_TABLE_NAME);
    }

    public static void setIsTransforming(Configuration configuration, Boolean isTransforming) {
        Preconditions.checkNotNull(configuration);
        Preconditions.checkNotNull(isTransforming);
        configuration.set(IS_TRANSFORMING_VALUE, Boolean.toString(isTransforming));
    }

    public static Boolean getIsTransforming(Configuration configuration) {
        Preconditions.checkNotNull(configuration);
        return Boolean.valueOf(configuration.get(IS_TRANSFORMING_VALUE, "false"));
    }

    public static void setForceCutover(Configuration configuration, Boolean forceCutover) {
        Preconditions.checkNotNull(configuration);
        Preconditions.checkNotNull(forceCutover);
        configuration.set(FORCE_CUTOVER_VALUE, Boolean.toString(forceCutover));
    }

    public static Boolean getForceCutover(Configuration configuration) {
        Preconditions.checkNotNull(configuration);
        return Boolean.valueOf(configuration.get(FORCE_CUTOVER_VALUE, "false"));
    }

    public static void setTransformingTableType(Configuration configuration,
                                                SourceTable sourceTable) {
        Preconditions.checkNotNull(configuration);
        Preconditions.checkNotNull(sourceTable);
        configuration.set(TRANSFORMING_TABLE_TYPE, sourceTable.name());
    }

    public static SourceTable getTransformingTableType(Configuration configuration) {
        Preconditions.checkNotNull(configuration);
        return SourceTable.valueOf(configuration.get(TRANSFORMING_TABLE_TYPE));
    }

    public static void setIsPartialTransform(final Configuration configuration, Boolean partialTransform) throws SQLException {
        Preconditions.checkNotNull(configuration);
        Preconditions.checkNotNull(partialTransform);
        configuration.set(IS_PARTIAL_TRANSFORM, String.valueOf(partialTransform));
    }

    public static boolean getIsPartialTransform(final Configuration configuration)  {
        Preconditions.checkNotNull(configuration);
        return configuration.getBoolean(IS_PARTIAL_TRANSFORM, false);
    }

    public static void loadHBaseConfiguration(Job job) throws IOException {
        // load hbase-site.xml
        Configuration hbaseConf = HBaseConfiguration.create();
        for (Map.Entry<String, String> entry : hbaseConf) {
            if (job.getConfiguration().get(entry.getKey()) == null) {
                job.getConfiguration().set(entry.getKey(), entry.getValue());
            }
        }
        //In order to have phoenix working on a secured cluster
        TableMapReduceUtil.initCredentials(job);
    }
    
    public static ImportPreUpsertKeyValueProcessor loadPreUpsertProcessor(Configuration conf) {
        Class<? extends ImportPreUpsertKeyValueProcessor> processorClass = null;
        try {
            processorClass = conf.getClass(
                    UPSERT_HOOK_CLASS_CONFKEY, FormatToBytesWritableMapper.DefaultImportPreUpsertKeyValueProcessor.class,
                    ImportPreUpsertKeyValueProcessor.class);
        } catch (Exception e) {
            throw new IllegalStateException("Couldn't load upsert hook class", e);
        }
    
        return ReflectionUtils.newInstance(processorClass, conf);
    }

    public static byte[] getIndexMaintainers(final Configuration configuration){
        Preconditions.checkNotNull(configuration);
        return Base64.getDecoder().decode(configuration.get(INDEX_MAINTAINERS));
    }
    
    public static void setIndexMaintainers(final Configuration configuration,
            final ImmutableBytesWritable indexMetaDataPtr) {
        Preconditions.checkNotNull(configuration);
        Preconditions.checkNotNull(indexMetaDataPtr);
        configuration.set(INDEX_MAINTAINERS,Bytes.toString(Base64.getEncoder().encode(indexMetaDataPtr.get())));
    }
    
    public static void setDisableIndexes(Configuration configuration, String indexName) {
        Preconditions.checkNotNull(configuration);
        Preconditions.checkNotNull(indexName);
        configuration.set(DISABLED_INDEXES, indexName);
    }

    public static void setVerifyIndex(Configuration configuration, boolean verify) {
        Preconditions.checkNotNull(configuration);
        configuration.setBoolean(VERIFY_INDEX, verify);
    }

    public static void setOnlyVerifyIndex(Configuration configuration, boolean verify) {
        Preconditions.checkNotNull(configuration);
        configuration.setBoolean(ONLY_VERIFY_INDEX, verify);
    }

    public static void setIndexVerifyType(Configuration configuration, IndexTool.IndexVerifyType verifyType) {
        Preconditions.checkNotNull(configuration);
        configuration.set(INDEX_VERIFY_TYPE, verifyType.getValue());
    }

    public static void setDisableLoggingVerifyType(Configuration configuration,
                                                   IndexTool.IndexDisableLoggingType disableLoggingType) {
        Preconditions.checkNotNull(configuration);
        configuration.set(DISABLE_LOGGING_TYPE, disableLoggingType.getValue());
    }

    public static String getScrutinyDataTableName(Configuration configuration) {
        Preconditions.checkNotNull(configuration);
        return configuration.get(SCRUTINY_DATA_TABLE_NAME);
    }

    public static void setScrutinyDataTable(Configuration configuration, String qDataTableName) {
        Preconditions.checkNotNull(configuration);
        Preconditions.checkNotNull(qDataTableName);
        configuration.set(SCRUTINY_DATA_TABLE_NAME, qDataTableName);
    }

    public static String getScrutinyIndexTableName(Configuration configuration) {
        Preconditions.checkNotNull(configuration);
        return configuration.get(SCRUTINY_INDEX_TABLE_NAME);
    }
    public static void setIndexToolDataTableName(Configuration configuration, String qDataTableName) {
        Preconditions.checkNotNull(configuration);
        Preconditions.checkNotNull(qDataTableName);
        configuration.set(INDEX_TOOL_DATA_TABLE_NAME, qDataTableName);
    }

    public static String getIndexToolDataTableName(Configuration configuration) {
        Preconditions.checkNotNull(configuration);
        return configuration.get(INDEX_TOOL_DATA_TABLE_NAME);
    }

    public static void setScrutinyIndexTable(Configuration configuration, String qIndexTableName) {
        Preconditions.checkNotNull(configuration);
        Preconditions.checkNotNull(qIndexTableName);
        configuration.set(SCRUTINY_INDEX_TABLE_NAME, qIndexTableName);
    }

    public static SourceTable getScrutinySourceTable(Configuration configuration) {
        Preconditions.checkNotNull(configuration);
        return SourceTable.valueOf(configuration.get(SCRUTINY_SOURCE_TABLE));
    }

    public static void setIndexToolIndexTableName(Configuration configuration, String qIndexTableName) {
        Preconditions.checkNotNull(configuration);
        Preconditions.checkNotNull(qIndexTableName);
        configuration.set(INDEX_TOOL_INDEX_TABLE_NAME, qIndexTableName);
    }

    public static String getIndexToolIndexTableName(Configuration configuration) {
        Preconditions.checkNotNull(configuration);
        return configuration.get(INDEX_TOOL_INDEX_TABLE_NAME);
    }

    public static void setIndexToolSourceTable(Configuration configuration,
            IndexScrutinyTool.SourceTable sourceTable) {
        Preconditions.checkNotNull(configuration);
        Preconditions.checkNotNull(sourceTable);
        configuration.set(INDEX_TOOL_SOURCE_TABLE, sourceTable.name());
    }

    public static IndexScrutinyTool.SourceTable getIndexToolSourceTable(Configuration configuration) {
        Preconditions.checkNotNull(configuration);
        return IndexScrutinyTool.SourceTable.valueOf(configuration.get(INDEX_TOOL_SOURCE_TABLE,
            IndexScrutinyTool.SourceTable.DATA_TABLE_SOURCE.name()));
    }

    public static void setScrutinySourceTable(Configuration configuration,
            SourceTable sourceTable) {
        Preconditions.checkNotNull(configuration);
        Preconditions.checkNotNull(sourceTable);
        configuration.set(SCRUTINY_SOURCE_TABLE, sourceTable.name());
    }

    public static boolean getScrutinyOutputInvalidRows(Configuration configuration) {
        Preconditions.checkNotNull(configuration);
        return configuration.getBoolean(SCRUTINY_OUTPUT_INVALID_ROWS,
            DEFAULT_SCRUTINY_OUTPUT_INVALID_ROWS);
    }

    public static void setScrutinyOutputInvalidRows(Configuration configuration,
            boolean outputInvalidRows) {
        Preconditions.checkNotNull(configuration);
        configuration.setBoolean(SCRUTINY_OUTPUT_INVALID_ROWS, outputInvalidRows);
    }

    public static long getScrutinyBatchSize(Configuration configuration) {
        Preconditions.checkNotNull(configuration);
        return configuration.getLong(SCRUTINY_BATCH_SIZE, DEFAULT_SCRUTINY_BATCH_SIZE);
    }

    public static void setScrutinyBatchSize(Configuration configuration, long batchSize) {
        Preconditions.checkNotNull(configuration);
        configuration.setLong(SCRUTINY_BATCH_SIZE, batchSize);
    }

    public static OutputFormat getScrutinyOutputFormat(Configuration configuration) {
        Preconditions.checkNotNull(configuration);
        return OutputFormat
                .valueOf(configuration.get(SCRUTINY_OUTPUT_FORMAT, OutputFormat.FILE.name()));
    }

    public static void setScrutinyOutputFormat(Configuration configuration,
            OutputFormat outputFormat) {
        Preconditions.checkNotNull(configuration);
        Preconditions.checkNotNull(outputFormat);
        configuration.set(SCRUTINY_OUTPUT_FORMAT, outputFormat.name());
    }

    public static long getScrutinyExecuteTimestamp(Configuration configuration) {
        Preconditions.checkNotNull(configuration);
        long ts = configuration.getLong(SCRUTINY_EXECUTE_TIMESTAMP, -1);
        Preconditions.checkArgument(ts != -1);
        return ts;
    }

    public static void setScrutinyOutputMax(Configuration configuration,
            long outputMaxRows) {
        Preconditions.checkNotNull(configuration);
        configuration.setLong(SCRUTINY_OUTPUT_MAX, outputMaxRows);
    }

    public static long getScrutinyOutputMax(Configuration configuration) {
        Preconditions.checkNotNull(configuration);
        long maxRows = configuration.getLong(SCRUTINY_OUTPUT_MAX, -1);
        Preconditions.checkArgument(maxRows != -1);
        return maxRows;
    }

    public static void setScrutinyExecuteTimestamp(Configuration configuration, long ts) {
        Preconditions.checkNotNull(configuration);
        configuration.setLong(SCRUTINY_EXECUTE_TIMESTAMP, ts);
    }

    public static void setSplitByStats(final Configuration configuration, boolean value) {
        Preconditions.checkNotNull(configuration);
        configuration.setBoolean(MAPREDUCE_SPLIT_BY_STATS, value);
    }

    public static String getDisableIndexes(Configuration configuration) {
        Preconditions.checkNotNull(configuration);
        return configuration.get(DISABLED_INDEXES);
    }

    public static boolean getVerifyIndex(Configuration configuration) {
        Preconditions.checkNotNull(configuration);
        return configuration.getBoolean(VERIFY_INDEX, false);
    }

    public static boolean getOnlyVerifyIndex(Configuration configuration) {
        Preconditions.checkNotNull(configuration);
        return configuration.getBoolean(ONLY_VERIFY_INDEX, false);
    }

    public static IndexTool.IndexVerifyType getIndexVerifyType(Configuration configuration) {
        Preconditions.checkNotNull(configuration);
        String value = configuration.get(INDEX_VERIFY_TYPE, IndexTool.IndexVerifyType.NONE.getValue());
        return IndexTool.IndexVerifyType.fromValue(value);
    }

    public static boolean getShouldFixUnverifiedTransform(Configuration configuration) {
        Preconditions.checkNotNull(configuration);
        return configuration.getBoolean(SHOULD_FIX_UNVERIFIED_TRANSFORM,
                DEFAULT_SHOULD_FIX_UNVERIFIED_TRANSFORM);
    }

    public static void setShouldFixUnverifiedTransform(Configuration configuration,
                                                    boolean shouldFixUnverified) {
        Preconditions.checkNotNull(configuration);
        configuration.setBoolean(SHOULD_FIX_UNVERIFIED_TRANSFORM, shouldFixUnverified);
    }

    public static IndexTool.IndexVerifyType getDisableLoggingVerifyType(Configuration configuration) {
        Preconditions.checkNotNull(configuration);
        String value = configuration.get(DISABLE_LOGGING_TYPE, IndexTool.IndexVerifyType.NONE.getValue());
        return IndexTool.IndexVerifyType.fromValue(value);
    }

    public static boolean getSplitByStats(final Configuration configuration) {
        Preconditions.checkNotNull(configuration);
        boolean split = configuration.getBoolean(MAPREDUCE_SPLIT_BY_STATS, DEFAULT_SPLIT_BY_STATS);
        return split;
    }

    public static void setTenantId(Configuration configuration, String tenantId){
        Preconditions.checkNotNull(configuration);
        configuration.set(MAPREDUCE_TENANT_ID, tenantId);
    }

    public static void setMRSnapshotManagedExternally(Configuration configuration, Boolean isSnapshotRestoreManagedExternally) {
        Preconditions.checkNotNull(configuration);
        Preconditions.checkNotNull(isSnapshotRestoreManagedExternally);
        configuration.setBoolean(MAPREDUCE_EXTERNAL_SNAPSHOT_RESTORE, isSnapshotRestoreManagedExternally);
    }

    public static boolean isMRSnapshotManagedExternally(final Configuration configuration) {
        Preconditions.checkNotNull(configuration);
        boolean isSnapshotRestoreManagedExternally =
                configuration.getBoolean(MAPREDUCE_EXTERNAL_SNAPSHOT_RESTORE, DEFAULT_MAPREDUCE_EXTERNAL_SNAPSHOT_RESTORE);
        return isSnapshotRestoreManagedExternally;
    }

    public static boolean isMRRandomizeMapperExecutionOrder(final Configuration configuration) {
        Preconditions.checkNotNull(configuration);
        return configuration.getBoolean(MAPREDUCE_RANDOMIZE_MAPPER_EXECUTION_ORDER,
            DEFAULT_MAPREDUCE_RANDOMIZE_MAPPER_EXECUTION_ORDER);
    }
}
