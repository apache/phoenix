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

import static org.apache.commons.lang.StringUtils.isNotEmpty;
import static org.apache.phoenix.query.QueryServices.USE_STATS_FOR_PARALLELIZATION;
import static org.apache.phoenix.query.QueryServicesOptions.DEFAULT_USE_STATS_FOR_PARALLELIZATION;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat.NullDBWritable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.phoenix.iterate.BaseResultIterators;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.mapreduce.FormatToBytesWritableMapper;
import org.apache.phoenix.mapreduce.ImportPreUpsertKeyValueProcessor;
import org.apache.phoenix.mapreduce.PhoenixInputFormat;
import org.apache.phoenix.mapreduce.index.IndexScrutinyTool.OutputFormat;
import org.apache.phoenix.mapreduce.index.IndexScrutinyTool.SourceTable;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.phoenix.util.ColumnInfo;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.QueryUtil;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * A utility class to set properties on the {#link Configuration} instance.
 * Used as part of Map Reduce job configuration.
 * 
 */
public final class PhoenixConfigurationUtil {

    private static final Log LOG = LogFactory.getLog(PhoenixInputFormat.class);

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
    
    public static final long DEFAULT_UPSERT_BATCH_SIZE = 1000;
    
    public static final String INPUT_CLASS = "phoenix.input.class";
    
    public static final String CURRENT_SCN_VALUE = "phoenix.mr.currentscn.value";
    
    public static final String TX_SCN_VALUE = "phoenix.mr.txscn.value";
    
    /** Configuration key for the class name of an ImportPreUpsertKeyValueProcessor */
    public static final String UPSERT_HOOK_CLASS_CONFKEY = "phoenix.mapreduce.import.kvprocessor";

    public static final String MAPREDUCE_INPUT_CLUSTER_QUORUM = "phoenix.mapreduce.input.cluster.quorum";
    
    public static final String MAPREDUCE_OUTPUT_CLUSTER_QUORUM = "phoneix.mapreduce.output.cluster.quorum";

    public static final String INDEX_DISABLED_TIMESTAMP_VALUE = "phoenix.mr.index.disableTimestamp";

    public static final String INDEX_MAINTAINERS = "phoenix.mr.index.maintainers";

    public static final String SCRUTINY_DATA_TABLE_NAME = "phoenix.mr.scrutiny.data.table.name";

    public static final String SCRUTINY_INDEX_TABLE_NAME = "phoenix.mr.scrutiny.index.table.name";

    public static final String SCRUTINY_SOURCE_TABLE = "phoenix.mr.scrutiny.source.table";

    public static final String SCRUTINY_BATCH_SIZE = "phoenix.mr.scrutiny.batch.size";

    public static final String SCRUTINY_OUTPUT_INVALID_ROWS =
            "phoenix.mr.scrutiny.output.invalid.rows";

    public static final boolean DEFAULT_SCRUTINY_OUTPUT_INVALID_ROWS = false;

    public static final String SCRUTINY_OUTPUT_FORMAT = "phoenix.mr.scrutiny.output.format";

    public static final String SCRUTINY_EXECUTE_TIMESTAMP = "phoenix.mr.scrutiny.execute.timestamp";

    // max output rows per mapper
    public static final String SCRUTINY_OUTPUT_MAX = "phoenix.mr.scrutiny.output.max";

    public static final long DEFAULT_SCRUTINY_BATCH_SIZE = 1000;

    public static final String DISABLED_INDEXES = "phoenix.mr.index.disabledIndexes";

    // Generate splits based on scans from stats, or just from region splits
    public static final String MAPREDUCE_SPLIT_BY_STATS = "phoenix.mapreduce.split.by.stats";

    public static final boolean DEFAULT_SPLIT_BY_STATS = true;

    public static final String SNAPSHOT_NAME_KEY = "phoenix.mapreduce.snapshot.name";

    public static final String RESTORE_DIR_KEY = "phoenix.tableSnapshot.restore.dir";

    public static final String MAPREDUCE_TENANT_ID = "phoenix.mapreduce.tenantid";

    public enum SchemaType {
        TABLE,
        QUERY;
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
    
    public static void setSchemaType(Configuration configuration, final SchemaType schemaType) {
        Preconditions.checkNotNull(configuration);
        configuration.set(SCHEMA_TYPE, schemaType.name());
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
    public static void setInputCluster(final Configuration configuration,
            final String quorum) {
        Preconditions.checkNotNull(configuration);
        configuration.set(MAPREDUCE_INPUT_CLUSTER_QUORUM, quorum);
    }

    /**
     * Sets which HBase cluster a Phoenix MapReduce job should write to
     * @param configuration
     * @param quorum ZooKeeper quorum string for HBase cluster the MapReduce job will write to
     */
    public static void setOutputCluster(final Configuration configuration,
            final String quorum) {
        Preconditions.checkNotNull(configuration);
        configuration.set(MAPREDUCE_OUTPUT_CLUSTER_QUORUM, quorum);
    }
        
    public static Class<?> getInputClass(final Configuration configuration) {
        return configuration.getClass(INPUT_CLASS, NullDBWritable.class);
    }
    public static SchemaType getSchemaType(final Configuration configuration) {
        final String schemaTp = configuration.get(SCHEMA_TYPE);
        Preconditions.checkNotNull(schemaTp);
        return SchemaType.valueOf(schemaTp);
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
        final Connection connection = ConnectionUtil.getOutputConnection(configuration);
        List<String> upsertColumnList = PhoenixConfigurationUtil.getUpsertColumnNames(configuration);
        if(!upsertColumnList.isEmpty()) {
            LOG.info(String.format("UseUpsertColumns=%s, upsertColumnList.size()=%s, upsertColumnList=%s "
                    ,!upsertColumnList.isEmpty(), upsertColumnList.size(), Joiner.on(",").join(upsertColumnList)
                    ));
        } 
       columnMetadataList = PhoenixRuntime.generateColumnInfo(connection, tableName, upsertColumnList);
       // we put the encoded column infos in the Configuration for re usability.
       ColumnInfoToStringEncoderDecoder.encode(configuration, columnMetadataList); 
       connection.close();
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
            LOG.info("Phoenix Custom Upsert Statement: "+ upsertStmt);
        } else {
            // Generating UPSERT statement without column name information.
            upsertStmt = QueryUtil.constructGenericUpsertStatement(tableName, columnMetadataList.size());
            LOG.info("Phoenix Generic Upsert Statement: " + upsertStmt);
        }
        configuration.set(UPSERT_STATEMENT, upsertStmt);
        return upsertStmt;
        
    }

     public static void setUpsertStatement(final Configuration configuration, String upsertStmt) throws SQLException {
         Preconditions.checkNotNull(configuration);
         Preconditions.checkNotNull(upsertStmt);
         configuration.set(UPSERT_STATEMENT, upsertStmt);
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
        final Connection connection = ConnectionUtil.getInputConnection(configuration, props);
        final List<String> selectColumnList = getSelectColumnList(configuration);
        columnMetadataList = PhoenixRuntime.generateColumnInfo(connection, tableName, selectColumnList);
        // we put the encoded column infos in the Configuration for re usability.
        ColumnInfoToStringEncoderDecoder.encode(configuration, columnMetadataList);
        connection.close();
        return columnMetadataList;
    }

    private static List<String> getSelectColumnList(
            final Configuration configuration) {
    	List<String> selectColumnList = PhoenixConfigurationUtil.getSelectColumnNames(configuration);
        if(!selectColumnList.isEmpty()) {
            LOG.info(String.format("UseSelectColumns=%s, selectColumnList.size()=%s, selectColumnList=%s "
                    ,!selectColumnList.isEmpty(), selectColumnList.size(), Joiner.on(",").join(selectColumnList)
                    ));
        }
        return selectColumnList;
    }
    
    public static String getSelectStatement(final Configuration configuration) throws SQLException {
        Preconditions.checkNotNull(configuration);
        String selectStmt = configuration.get(SELECT_STATEMENT);
        if(isNotEmpty(selectStmt)) {
            return selectStmt;
        }
        final String tableName = getInputTableName(configuration);
        Preconditions.checkNotNull(tableName);
        final List<ColumnInfo> columnMetadataList = getSelectColumnMetadataList(configuration);
        final String conditions = configuration.get(INPUT_TABLE_CONDITIONS);
        selectStmt = QueryUtil.constructSelectStatement(tableName, columnMetadataList, conditions);
        LOG.info("Select Statement: "+ selectStmt);
        configuration.set(SELECT_STATEMENT, selectStmt);
        return selectStmt;
    }
    
    public static long getBatchSize(final Configuration configuration) throws SQLException {
        Preconditions.checkNotNull(configuration);
        long batchSize = configuration.getLong(UPSERT_BATCH_SIZE, DEFAULT_UPSERT_BATCH_SIZE);
        if(batchSize <= 0) {
           Connection conn = ConnectionUtil.getOutputConnection(configuration);
           batchSize = ((PhoenixConnection) conn).getMutateBatchSize();
           conn.close();
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
    
    /**
     * Returns the ZooKeeper quorum string for the HBase cluster a Phoenix MapReduce job will read from
     * @param configuration
     * @return ZooKeeper quorum string
     */
    public static String getInputCluster(final Configuration configuration) {
        Preconditions.checkNotNull(configuration);
        String quorum = configuration.get(MAPREDUCE_INPUT_CLUSTER_QUORUM);
        if (quorum == null) {
            quorum = configuration.get(HConstants.ZOOKEEPER_QUORUM);
        }
        return quorum;
    }

    /**
     * Returns the ZooKeeper quorum string for the HBase cluster a Phoenix MapReduce job will write to
     * @param configuration
     * @return ZooKeeper quorum string
     */
    public static String getOutputCluster(final Configuration configuration) {
        Preconditions.checkNotNull(configuration);
        String quorum = configuration.get(MAPREDUCE_OUTPUT_CLUSTER_QUORUM);
        if (quorum == null) {
            quorum = configuration.get(HConstants.ZOOKEEPER_QUORUM);
        }
        return quorum;
    }
    
    /**
     * Returns the HBase Client Port
     * @param configuration
     * @return
     */
    public static Integer getClientPort(final Configuration configuration) {
        Preconditions.checkNotNull(configuration);
        String clientPortString = configuration.get(HConstants.ZOOKEEPER_CLIENT_PORT);
        return clientPortString==null ? null : Integer.parseInt(clientPortString);
    }

    /**
     * Returns the HBase zookeeper znode parent
     * @param configuration
     * @return
     */
    public static String getZNodeParent(final Configuration configuration) {
        Preconditions.checkNotNull(configuration);
        return configuration.get(HConstants.ZOOKEEPER_ZNODE_PARENT);
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

    public static void setScrutinyIndexTable(Configuration configuration, String qIndexTableName) {
        Preconditions.checkNotNull(configuration);
        Preconditions.checkNotNull(qIndexTableName);
        configuration.set(SCRUTINY_INDEX_TABLE_NAME, qIndexTableName);
    }

    public static SourceTable getScrutinySourceTable(Configuration configuration) {
        Preconditions.checkNotNull(configuration);
        return SourceTable.valueOf(configuration.get(SCRUTINY_SOURCE_TABLE));
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

    public static String getDisableIndexes(Configuration configuration) {
        Preconditions.checkNotNull(configuration);
        return configuration.get(DISABLED_INDEXES);
    }

    public static boolean getSplitByStats(final Configuration configuration) {
        Preconditions.checkNotNull(configuration);
        boolean split = configuration.getBoolean(MAPREDUCE_SPLIT_BY_STATS, DEFAULT_SPLIT_BY_STATS);
        return split;
    }

	public static boolean getStatsForParallelizationProp(PhoenixConnection conn, PTable table) {
	    Boolean useStats = table.useStatsForParallelization();
	    if (useStats != null) {
	        return useStats;
	    }
	    /*
	     * For a view index, we use the property set on view. For indexes on base table, whether
	     * global or local, we use the property set on the base table. Null check needed when
	     * dropping local indexes.
	     */
	    PName tenantId = conn.getTenantId();
	    int retryCount = 0;
	    while (retryCount++<2) {
		    if (table.getType() == PTableType.INDEX && table.getParentName() != null) {
		        String parentTableName = table.getParentName().getString();
				try {
		            PTable parentTable =
		                    conn.getTable(new PTableKey(tenantId, parentTableName));
		            useStats = parentTable.useStatsForParallelization();
		            if (useStats != null) {
		                return useStats;
		            }
				} catch (TableNotFoundException e) {
					// try looking up the table without the tenant id (for
					// global tables)
					if (tenantId != null) {
						tenantId = null;
					} else {
						BaseResultIterators.logger.warn(
								"Unable to find parent table \"" + parentTableName + "\" of table \""
										+ table.getName().getString() + "\" to determine USE_STATS_FOR_PARALLELIZATION",
								e);
					}
				}
		    }
	    }
	    return conn.getQueryServices().getConfiguration()
	            .getBoolean(USE_STATS_FOR_PARALLELIZATION, DEFAULT_USE_STATS_FOR_PARALLELIZATION);
	}

    public static void setTenantId(Configuration configuration, String tenantId){
        Preconditions.checkNotNull(configuration);
        configuration.set(MAPREDUCE_TENANT_ID, tenantId);
    }
}
