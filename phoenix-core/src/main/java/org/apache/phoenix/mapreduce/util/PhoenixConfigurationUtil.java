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

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat.NullDBWritable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.mapreduce.CsvToKeyValueMapper.DefaultImportPreUpsertKeyValueProcessor;
import org.apache.phoenix.mapreduce.ImportPreUpsertKeyValueProcessor;
import org.apache.phoenix.mapreduce.PhoenixInputFormat;
import org.apache.phoenix.util.ColumnInfo;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.QueryUtil;

import static org.apache.commons.lang.StringUtils.isNotEmpty;

/**
 * A utility class to set properties on the {#link Configuration} instance.
 * Used as part of Map Reduce job configuration.
 * 
 */
public final class PhoenixConfigurationUtil {

    private static final Log LOG = LogFactory.getLog(PhoenixInputFormat.class);
    
    public static final String UPSERT_COLUMNS = "phoenix.upsert.columns";
    
    public static final String UPSERT_STATEMENT = "phoenix.upsert.stmt";
    
    public static final String UPSERT_COLUMN_INFO_KEY  = "phoenix.upsert.columninfos.list";
    
    public static final String SELECT_STATEMENT = "phoenix.select.stmt";
    
    public static final String UPSERT_BATCH_SIZE = "phoenix.upsert.batch.size";
    
    public static final String SELECT_COLUMNS = "phoneix.select.query.columns";
    
    public static final String SELECT_COLUMN_INFO_KEY  = "phoenix.select.columninfos.list";
    
    public static final String SCHEMA_TYPE = "phoenix.select.schema.type";
    
    public static final String COLUMN_NAMES_DELIMITER = "phoenix.column.names.delimiter";
    
    public static final String INPUT_TABLE_NAME = "phoenix.input.table.name" ;
    
    public static final String INPUT_TABLE_CONDITIONS = "phoenix.input.table.conditions" ;
    
    public static final String OUTPUT_TABLE_NAME = "phoenix.output.table.name" ;
    
    public static final long DEFAULT_UPSERT_BATCH_SIZE = 1000;
    
    public static final String DEFAULT_COLUMN_NAMES_DELIMITER = ",";

    public static final String INPUT_CLASS = "phoenix.input.class";
    
    public static final String CURRENT_SCN_VALUE = "phoenix.mr.currentscn.value";
    
    /** Configuration key for the class name of an ImportPreUpsertKeyValueProcessor */
    public static final String UPSERT_HOOK_CLASS_CONFKEY = "phoenix.mapreduce.import.kvprocessor";

    public static final String MAPREDUCE_INPUT_CLUSTER_QUORUM = "phoenix.mapreduce.input.cluster.quorum";
    
    public static final String MAPREDUCE_OUTPUT_CLUSTER_QUORUM = "phoneix.mapreduce.output.cluster.quorum";

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
    
    public static void setSelectColumnNames(final Configuration configuration,final String[] columns) {
        Preconditions.checkNotNull(configuration);
        final String selectColumnNames = Joiner.on(DEFAULT_COLUMN_NAMES_DELIMITER).join(columns);
        configuration.set(SELECT_COLUMNS, selectColumnNames);
    }
    
    public static void setSelectColumnNames(final Configuration configuration,final String columns) {
        Preconditions.checkNotNull(configuration);
        configuration.set(SELECT_COLUMNS, columns);
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
    
    public static void setOutputTableName(final Configuration configuration, final String tableName) {
        Preconditions.checkNotNull(configuration);
        Preconditions.checkNotNull(tableName);
        configuration.set(OUTPUT_TABLE_NAME, tableName);
    }
    
    public static void setUpsertColumnNames(final Configuration configuration,final String[] columns) {
        Preconditions.checkNotNull(configuration);
        final String upsertColumnNames = Joiner.on(DEFAULT_COLUMN_NAMES_DELIMITER).join(columns);
        configuration.set(UPSERT_COLUMNS, upsertColumnNames);
    }
    
    public static void setUpsertColumnNames(final Configuration configuration,final String columns) {
        Preconditions.checkNotNull(configuration);
        configuration.set(UPSERT_COLUMNS, columns);
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
        final String tableName = getOutputTableName(configuration);
        Preconditions.checkNotNull(tableName);
        final String columnInfoStr = configuration.get(UPSERT_COLUMN_INFO_KEY);
        if(isNotEmpty(columnInfoStr)) {
            return ColumnInfoToStringEncoderDecoder.decode(columnInfoStr);
        }
        final Connection connection = ConnectionUtil.getOutputConnection(configuration);
        String upsertColumns = configuration.get(UPSERT_COLUMNS);
        List<String> upsertColumnList = null;
        if(isNotEmpty(upsertColumns)) {
            final String columnNamesDelimiter = configuration.get(COLUMN_NAMES_DELIMITER, DEFAULT_COLUMN_NAMES_DELIMITER);
            upsertColumnList = Lists.newArrayList(Splitter.on(columnNamesDelimiter).omitEmptyStrings().trimResults().split(upsertColumns));
            LOG.info(String.format("UseUpsertColumns=%s, upsertColumns=%s, upsertColumnSet.size()=%s, parsedColumns=%s "
                    ,!upsertColumnList.isEmpty(),upsertColumns, upsertColumnList.size(), Joiner.on(DEFAULT_COLUMN_NAMES_DELIMITER).join(upsertColumnList)
                    ));
        } 
       List<ColumnInfo> columnMetadataList = PhoenixRuntime.generateColumnInfo(connection, tableName, upsertColumnList);
       final String encodedColumnInfos = ColumnInfoToStringEncoderDecoder.encode(columnMetadataList);
       // we put the encoded column infos in the Configuration for re usability. 
       configuration.set(UPSERT_COLUMN_INFO_KEY, encodedColumnInfos);
       connection.close();
       return columnMetadataList;
    }
    
     public static String getUpsertStatement(final Configuration configuration) throws SQLException {
        Preconditions.checkNotNull(configuration);
        final String tableName = getOutputTableName(configuration);
        Preconditions.checkNotNull(tableName);
        String upsertStmt = configuration.get(UPSERT_STATEMENT);
        if(isNotEmpty(upsertStmt)) {
            return upsertStmt;
        }
        final boolean useUpsertColumns = isNotEmpty(configuration.get(UPSERT_COLUMNS,""));
        final List<ColumnInfo> columnMetadataList = getUpsertColumnMetadataList(configuration);
        if (useUpsertColumns) {
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
    
    public static List<ColumnInfo> getSelectColumnMetadataList(final Configuration configuration) throws SQLException {
        Preconditions.checkNotNull(configuration);
        final String columnInfoStr = configuration.get(SELECT_COLUMN_INFO_KEY);
        if(isNotEmpty(columnInfoStr)) {
            return ColumnInfoToStringEncoderDecoder.decode(columnInfoStr);
        }
        final String tableName = getInputTableName(configuration);
        Preconditions.checkNotNull(tableName);
        final Connection connection = ConnectionUtil.getInputConnection(configuration);
        final List<String> selectColumnList = getSelectColumnList(configuration);
        final List<ColumnInfo> columnMetadataList = PhoenixRuntime.generateColumnInfo(connection, tableName, selectColumnList);
        final String encodedColumnInfos = ColumnInfoToStringEncoderDecoder.encode(columnMetadataList);
        // we put the encoded column infos in the Configuration for re usability. 
        configuration.set(SELECT_COLUMN_INFO_KEY, encodedColumnInfos);
        connection.close();
        return columnMetadataList;
    }

    private static List<String> getSelectColumnList(
            final Configuration configuration) {
        String selectColumns = configuration.get(SELECT_COLUMNS);
        List<String> selectColumnList = null;
        if(isNotEmpty(selectColumns)) {
            final String columnNamesDelimiter = configuration.get(COLUMN_NAMES_DELIMITER, DEFAULT_COLUMN_NAMES_DELIMITER);
            selectColumnList = Lists.newArrayList(Splitter.on(columnNamesDelimiter).omitEmptyStrings().trimResults().split(selectColumns));
            LOG.info(String.format("UseSelectColumns=%s, selectColumns=%s, selectColumnSet.size()=%s, parsedColumns=%s "
                    ,!selectColumnList.isEmpty(),selectColumns, selectColumnList.size(), Joiner.on(DEFAULT_COLUMN_NAMES_DELIMITER).join(selectColumnList)
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
                    UPSERT_HOOK_CLASS_CONFKEY, DefaultImportPreUpsertKeyValueProcessor.class,
                    ImportPreUpsertKeyValueProcessor.class);
        } catch (Exception e) {
            throw new IllegalStateException("Couldn't load upsert hook class", e);
        }
    
        return ReflectionUtils.newInstance(processorClass, conf);
    }
}
