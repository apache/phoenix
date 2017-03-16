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
package org.apache.phoenix.mapreduce.index;

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.ASYNC_REBUILD_TIMESTAMP;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.INDEX_DISABLE_TIMESTAMP;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_SCHEM;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.phoenix.compile.PostIndexDDLCompiler;
import org.apache.phoenix.coprocessor.BaseScannerRegionObserver;
import org.apache.phoenix.hbase.index.util.IndexManagementUtil;
import org.apache.phoenix.index.IndexMaintainer;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.mapreduce.CsvBulkImportUtil;
import org.apache.phoenix.mapreduce.util.ColumnInfoToStringEncoderDecoder;
import org.apache.phoenix.mapreduce.util.ConnectionUtil;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.apache.phoenix.mapreduce.util.PhoenixMapReduceUtil;
import org.apache.phoenix.parse.HintNode.Hint;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTable.IndexType;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.ColumnInfo;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TransactionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 * An MR job to populate the index table from the data table.
 *
 */
public class IndexTool extends Configured implements Tool {

    private static final Logger LOG = LoggerFactory.getLogger(IndexTool.class);

    private static final Option SCHEMA_NAME_OPTION = new Option("s", "schema", true,
            "Phoenix schema name (optional)");
    private static final Option DATA_TABLE_OPTION = new Option("dt", "data-table", true,
            "Data table name (mandatory)");
    private static final Option INDEX_TABLE_OPTION = new Option("it", "index-table", true,
            "Index table name(not required in case of partial rebuilding)");
    
    private static final Option PARTIAL_REBUILD_OPTION = new Option("pr", "partial-rebuild", false,
            "To build indexes for a data table from least disabledTimeStamp");
    
    private static final Option DIRECT_API_OPTION = new Option("direct", "direct", false,
            "If specified, we avoid the bulk load (optional)");
    private static final Option RUN_FOREGROUND_OPTION =
            new Option(
                    "runfg",
                    "run-foreground",
                    false,
                    "Applicable on top of -direct option."
                            + "If specified, runs index build in Foreground. Default - Runs the build in background.");
    private static final Option OUTPUT_PATH_OPTION = new Option("op", "output-path", true,
            "Output path where the files are written");
    private static final Option HELP_OPTION = new Option("h", "help", false, "Help");
    public static final String INDEX_JOB_NAME_TEMPLATE = "PHOENIX_%s_INDX_%s";

    private Options getOptions() {
        final Options options = new Options();
        options.addOption(SCHEMA_NAME_OPTION);
        options.addOption(DATA_TABLE_OPTION);
        options.addOption(INDEX_TABLE_OPTION);
        options.addOption(PARTIAL_REBUILD_OPTION);
        options.addOption(DIRECT_API_OPTION);
        options.addOption(RUN_FOREGROUND_OPTION);
        options.addOption(OUTPUT_PATH_OPTION);
        options.addOption(HELP_OPTION);
        return options;
    }

    /**
     * Parses the commandline arguments, throws IllegalStateException if mandatory arguments are
     * missing.
     * @param args supplied command line arguments
     * @return the parsed command line
     */
    private CommandLine parseOptions(String[] args) {

        final Options options = getOptions();

        CommandLineParser parser = new PosixParser();
        CommandLine cmdLine = null;
        try {
            cmdLine = parser.parse(options, args);
        } catch (ParseException e) {
            printHelpAndExit("Error parsing command line options: " + e.getMessage(), options);
        }

        if (cmdLine.hasOption(HELP_OPTION.getOpt())) {
            printHelpAndExit(options, 0);
        }

        if (!cmdLine.hasOption(DATA_TABLE_OPTION.getOpt())) {
            throw new IllegalStateException(DATA_TABLE_OPTION.getLongOpt() + " is a mandatory "
                    + "parameter");
        }

		if (!(cmdLine.hasOption(PARTIAL_REBUILD_OPTION.getOpt()) || cmdLine.hasOption(DIRECT_API_OPTION.getOpt()))
				&& !cmdLine.hasOption(OUTPUT_PATH_OPTION.getOpt())) {
			throw new IllegalStateException(OUTPUT_PATH_OPTION.getLongOpt() + " is a mandatory " + "parameter");
		}
        
		if (cmdLine.hasOption(PARTIAL_REBUILD_OPTION.getOpt()) && cmdLine.hasOption(INDEX_TABLE_OPTION.getOpt())) {
			throw new IllegalStateException("Index name should not be passed with " + PARTIAL_REBUILD_OPTION.getLongOpt());
		}
        		
        if (!(cmdLine.hasOption(DIRECT_API_OPTION.getOpt())) && cmdLine.hasOption(INDEX_TABLE_OPTION.getOpt())
                && cmdLine.hasOption(RUN_FOREGROUND_OPTION
                        .getOpt())) {
            throw new IllegalStateException(RUN_FOREGROUND_OPTION.getLongOpt()
                    + " is applicable only for " + DIRECT_API_OPTION.getLongOpt());
        }
        return cmdLine;
    }

    private void printHelpAndExit(String errorMessage, Options options) {
        System.err.println(errorMessage);
        printHelpAndExit(options, 1);
    }

    private void printHelpAndExit(Options options, int exitCode) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("help", options);
        System.exit(exitCode);
    }
    
    class JobFactory {
        Connection connection;
        Configuration configuration;
        private Path outputPath;
        private FileSystem fs;

        public JobFactory(Connection connection, Configuration configuration, Path outputPath) {
            this.connection = connection;
            this.configuration = configuration;
            this.outputPath = outputPath;

        }

        public Job getJob(String schemaName, String indexTable, String dataTable, boolean useDirectApi, boolean isPartialBuild) throws Exception {
            if (isPartialBuild) {
                return configureJobForPartialBuild(schemaName, dataTable);
            } else {
                return configureJobForAysncIndex(schemaName, indexTable, dataTable, useDirectApi);
            }
        }
        
        private Job configureJobForPartialBuild(String schemaName, String dataTable) throws Exception {
            final String qDataTable = SchemaUtil.getQualifiedTableName(schemaName, dataTable);
            final PTable pdataTable = PhoenixRuntime.getTable(connection, qDataTable);
            connection = ConnectionUtil.getInputConnection(configuration);
            long minDisableTimestamp = HConstants.LATEST_TIMESTAMP;
            PTable indexWithMinDisableTimestamp = null;
            
            //Get Indexes in building state, minDisabledTimestamp 
            List<String> disableIndexes = new ArrayList<String>();
            List<PTable> disabledPIndexes = new ArrayList<PTable>();
            for (PTable index : pdataTable.getIndexes()) {
                if (index.getIndexState().equals(PIndexState.BUILDING)) {
                    disableIndexes.add(index.getTableName().getString());
                    disabledPIndexes.add(index);
                    if (minDisableTimestamp > index.getIndexDisableTimestamp()) {
                        minDisableTimestamp = index.getIndexDisableTimestamp();
                        indexWithMinDisableTimestamp = index;
                    }
                }
            }
            
            if (indexWithMinDisableTimestamp == null) {
                throw new Exception("There is no index for a datatable to be rebuild:" + qDataTable);
            }
            if (minDisableTimestamp == 0) {
                throw new Exception("It seems Index " + indexWithMinDisableTimestamp
                        + " has disable timestamp as 0 , please run IndexTool with IndexName to build it first");
                // TODO probably we can initiate the job by ourself or can skip them while making the list for partial build with a warning
            }
            
            long maxTimestamp = getMaxRebuildAsyncDate(schemaName, disableIndexes);
            
            //serialize index maintaienr in job conf with Base64 TODO: Need to find better way to serialize them in conf.
            List<IndexMaintainer> maintainers = Lists.newArrayListWithExpectedSize(disabledPIndexes.size());
            for (PTable index : disabledPIndexes) {
                maintainers.add(index.getIndexMaintainer(pdataTable, connection.unwrap(PhoenixConnection.class)));
            }
            ImmutableBytesWritable indexMetaDataPtr = new ImmutableBytesWritable(ByteUtil.EMPTY_BYTE_ARRAY);
            IndexMaintainer.serializeAdditional(pdataTable, indexMetaDataPtr, disabledPIndexes, connection.unwrap(PhoenixConnection.class));
            PhoenixConfigurationUtil.setIndexMaintainers(configuration, indexMetaDataPtr);
            
            //Prepare raw scan 
            Scan scan = IndexManagementUtil.newLocalStateScan(maintainers);
            scan.setTimeRange(minDisableTimestamp - 1, maxTimestamp);
            scan.setRaw(true);
            scan.setCacheBlocks(false);
            if (pdataTable.isTransactional()) {
                long maxTimeRange = pdataTable.getTimeStamp() + 1;
                scan.setAttribute(BaseScannerRegionObserver.TX_SCN,
                        Bytes.toBytes(Long.valueOf(Long.toString(TransactionUtil.convertToNanoseconds(maxTimeRange)))));
            }
            
          
            String physicalTableName=pdataTable.getPhysicalName().getString();
            final String jobName = String.format("Phoenix Indexes build for " + pdataTable.getName().toString());
            
            PhoenixConfigurationUtil.setInputTableName(configuration, qDataTable);
            PhoenixConfigurationUtil.setPhysicalTableName(configuration, physicalTableName);
            
            //TODO: update disable indexes
            PhoenixConfigurationUtil.setDisableIndexes(configuration, StringUtils.join(",",disableIndexes));
            
            final Job job = Job.getInstance(configuration, jobName);
			if (outputPath != null) {
				FileOutputFormat.setOutputPath(job, outputPath);
			}
            job.setJarByClass(IndexTool.class);
            TableMapReduceUtil.initTableMapperJob(physicalTableName, scan, PhoenixIndexPartialBuildMapper.class, null,
                    null, job);
            TableMapReduceUtil.initCredentials(job);
            TableInputFormat.configureSplitTable(job, TableName.valueOf(physicalTableName));
            return configureSubmittableJobUsingDirectApi(job, true);
        }
        
        private long getMaxRebuildAsyncDate(String schemaName, List<String> disableIndexes) throws SQLException {
            Long maxRebuilAsyncDate=HConstants.LATEST_TIMESTAMP;
            Long maxDisabledTimeStamp=0L;
            if (disableIndexes == null || disableIndexes.isEmpty()) { return 0; }
            List<String> quotedIndexes = new ArrayList<String>(disableIndexes.size());
            for (String index : disableIndexes) {
                quotedIndexes.add("'" + index + "'");
            }
            ResultSet rs = connection.createStatement()
                    .executeQuery("SELECT MAX(" + ASYNC_REBUILD_TIMESTAMP + "),MAX("+INDEX_DISABLE_TIMESTAMP+") FROM " + SYSTEM_CATALOG_NAME + " ("
                            + ASYNC_REBUILD_TIMESTAMP + " BIGINT) WHERE " + TABLE_SCHEM
                            + (schemaName != null && schemaName.length() > 0 ? "='" + schemaName + "'" : " IS NULL")
                            + " and " + TABLE_NAME + " IN (" + StringUtils.join(",", quotedIndexes) + ")");
            if (rs.next()) {
                maxRebuilAsyncDate = rs.getLong(1);
                maxDisabledTimeStamp = rs.getLong(2);
            }
            // Do check if table is disabled again after user invoked async rebuilding during the run of the job
            if (maxRebuilAsyncDate > maxDisabledTimeStamp) {
                return maxRebuilAsyncDate;
            } else {
                throw new RuntimeException(
                        "Inconsistent state we have one or more index tables which are disabled after the async is called!!");
            }
            
        }

        private Job configureJobForAysncIndex(String schemaName, String indexTable, String dataTable, boolean useDirectApi)
                throws Exception {
            final String qDataTable = SchemaUtil.getQualifiedTableName(schemaName, dataTable);
            final String qIndexTable;
            if (schemaName != null && !schemaName.isEmpty()) {
                qIndexTable = SchemaUtil.getQualifiedTableName(schemaName, indexTable);
            } else {
                qIndexTable = indexTable;
            }
            final PTable pdataTable = PhoenixRuntime.getTable(connection, qDataTable);
            
            final PTable pindexTable = PhoenixRuntime.getTable(connection, qIndexTable);
            
            long maxTimeRange = pindexTable.getTimeStamp() + 1;
            // this is set to ensure index tables remains consistent post population.

            if (pdataTable.isTransactional()) {
                configuration.set(PhoenixConfigurationUtil.TX_SCN_VALUE,
                    Long.toString(TransactionUtil.convertToNanoseconds(maxTimeRange)));
            }
            configuration.set(PhoenixConfigurationUtil.CURRENT_SCN_VALUE,
                Long.toString(maxTimeRange));
            
            // check if the index type is LOCAL, if so, derive and set the physicalIndexName that is
            // computed from the qDataTable name.
            String physicalIndexTable = pindexTable.getPhysicalName().getString();
            

            final PhoenixConnection pConnection = connection.unwrap(PhoenixConnection.class);
            final PostIndexDDLCompiler ddlCompiler =
                    new PostIndexDDLCompiler(pConnection, new TableRef(pdataTable));
            ddlCompiler.compile(pindexTable);

            final List<String> indexColumns = ddlCompiler.getIndexColumnNames();
            final String selectQuery = ddlCompiler.getSelectQuery();
            final String upsertQuery =
                    QueryUtil.constructUpsertStatement(qIndexTable, indexColumns, Hint.NO_INDEX);

            configuration.set(PhoenixConfigurationUtil.UPSERT_STATEMENT, upsertQuery);
            PhoenixConfigurationUtil.setPhysicalTableName(configuration, physicalIndexTable);
            PhoenixConfigurationUtil.setDisableIndexes(configuration, indexTable);
            PhoenixConfigurationUtil.setUpsertColumnNames(configuration,
                indexColumns.toArray(new String[indexColumns.size()]));
            final List<ColumnInfo> columnMetadataList =
                    PhoenixRuntime.generateColumnInfo(connection, qIndexTable, indexColumns);
            ColumnInfoToStringEncoderDecoder.encode(configuration, columnMetadataList);
            fs = outputPath.getFileSystem(configuration);
            fs.delete(outputPath, true);           
 
            final String jobName = String.format(INDEX_JOB_NAME_TEMPLATE, pdataTable.getName().toString(), indexTable);
            final Job job = Job.getInstance(configuration, jobName);
            job.setJarByClass(IndexTool.class);
            job.setMapOutputKeyClass(ImmutableBytesWritable.class);
            FileOutputFormat.setOutputPath(job, outputPath);
            
            PhoenixMapReduceUtil.setInput(job, PhoenixIndexDBWritable.class, qDataTable,
                selectQuery);
            TableMapReduceUtil.initCredentials(job);
            
            
            if (useDirectApi) {
                return configureSubmittableJobUsingDirectApi(job, false);
            } else {
                return configureRunnableJobUsingBulkLoad(job, outputPath);
                
            }
            
        }

        /**
         * Submits the job and waits for completion.
         * @param job
         * @param outputPath
         * @return
         * @throws Exception
         */
        private Job configureRunnableJobUsingBulkLoad(Job job, Path outputPath) throws Exception {
            job.setMapperClass(PhoenixIndexImportMapper.class);
            job.setMapOutputKeyClass(ImmutableBytesWritable.class);
            job.setMapOutputValueClass(KeyValue.class);
            final Configuration configuration = job.getConfiguration();
            final String physicalIndexTable =
                    PhoenixConfigurationUtil.getPhysicalTableName(configuration);
            final HTable htable = new HTable(configuration, physicalIndexTable);
            HFileOutputFormat.configureIncrementalLoad(job, htable);
            return job;
               
        }
        
        /**
         * Uses the HBase Front Door Api to write to index table. Submits the job and either returns or
         * waits for the job completion based on runForeground parameter.
         * 
         * @param job
         * @param outputPath
         * @param runForeground - if true, waits for job completion, else submits and returns
         *            immediately.
         * @return
         * @throws Exception
         */
        private Job configureSubmittableJobUsingDirectApi(Job job, boolean isPartialRebuild)
                throws Exception {
            if (!isPartialRebuild) {
                //Don't configure mapper for partial build as it is configured already
                job.setMapperClass(PhoenixIndexImportDirectMapper.class);
            }
            job.setReducerClass(PhoenixIndexImportDirectReducer.class);
            Configuration conf = job.getConfiguration();
            HBaseConfiguration.merge(conf, HBaseConfiguration.create(conf));
            // Set the Physical Table name for use in DirectHTableWriter#write(Mutation)
            conf.set(TableOutputFormat.OUTPUT_TABLE,
                PhoenixConfigurationUtil.getPhysicalTableName(job.getConfiguration()));
            //Set the Output classes
            job.setMapOutputKeyClass(ImmutableBytesWritable.class);
            job.setMapOutputValueClass(IntWritable.class);
            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(NullWritable.class);
            TableMapReduceUtil.addDependencyJars(job);
            job.setNumReduceTasks(1);
            return job;
        }
        
    }

    @Override
    public int run(String[] args) throws Exception {
        Connection connection = null;
        HTable htable = null;
        try {
            CommandLine cmdLine = null;
            try {
                cmdLine = parseOptions(args);
            } catch (IllegalStateException e) {
                printHelpAndExit(e.getMessage(), getOptions());
            }
            final Configuration configuration = HBaseConfiguration.addHbaseResources(getConf());
            final String schemaName = cmdLine.getOptionValue(SCHEMA_NAME_OPTION.getOpt());
            final String dataTable = cmdLine.getOptionValue(DATA_TABLE_OPTION.getOpt());
            final String indexTable = cmdLine.getOptionValue(INDEX_TABLE_OPTION.getOpt());
            final boolean isPartialBuild = cmdLine.hasOption(PARTIAL_REBUILD_OPTION.getOpt());
            final String qDataTable = SchemaUtil.getQualifiedTableName(schemaName, dataTable);
            boolean useDirectApi = cmdLine.hasOption(DIRECT_API_OPTION.getOpt());
            String basePath=cmdLine.getOptionValue(OUTPUT_PATH_OPTION.getOpt());
            boolean isForeground = cmdLine.hasOption(RUN_FOREGROUND_OPTION.getOpt());
            connection = ConnectionUtil.getInputConnection(configuration);
            byte[][] splitKeysBeforeJob = null;
            boolean isLocalIndexBuild = false;
            PTable pindexTable = null;
            if (indexTable != null) {
                if (!isValidIndexTable(connection, qDataTable,indexTable)) {
                    throw new IllegalArgumentException(String.format(
                        " %s is not an index table for %s ", indexTable, qDataTable));
                }
                pindexTable = PhoenixRuntime.getTable(connection, schemaName != null && !schemaName.isEmpty()
                        ? SchemaUtil.getQualifiedTableName(schemaName, indexTable) : indexTable);
                htable = (HTable)connection.unwrap(PhoenixConnection.class).getQueryServices()
                        .getTable(pindexTable.getPhysicalName().getBytes());
                if (IndexType.LOCAL.equals(pindexTable.getIndexType())) {
                    isLocalIndexBuild = true;
                    splitKeysBeforeJob = htable.getRegionLocator().getStartKeys();
                }
            }
            
            PTable pdataTable = PhoenixRuntime.getTableNoCache(connection, qDataTable);
			Path outputPath = null;
			FileSystem fs = null;
			if (basePath != null) {
				outputPath = CsvBulkImportUtil.getOutputPath(new Path(basePath), pindexTable == null
						? pdataTable.getPhysicalName().getString() : pindexTable.getPhysicalName().getString());
				fs = outputPath.getFileSystem(configuration);
				fs.delete(outputPath, true);
			}
            
            Job job = new JobFactory(connection, configuration, outputPath).getJob(schemaName, indexTable, dataTable,
                    useDirectApi, isPartialBuild);
            if (!isForeground && useDirectApi) {
                LOG.info("Running Index Build in Background - Submit async and exit");
                job.submit();
                return 0;
            }
            LOG.info("Running Index Build in Foreground. Waits for the build to complete. This may take a long time!.");
            boolean result = job.waitForCompletion(true);
            
            if (result) {
                if (!useDirectApi && indexTable != null) {
                    if (isLocalIndexBuild) {
                        validateSplitForLocalIndex(splitKeysBeforeJob, htable);
                    }
                    LOG.info("Loading HFiles from {}", outputPath);
                    LoadIncrementalHFiles loader = new LoadIncrementalHFiles(configuration);
                    loader.doBulkLoad(outputPath, htable);
                    htable.close();
                    // Without direct API, we need to update the index state to ACTIVE from client.
                    IndexToolUtil.updateIndexState(connection, qDataTable, indexTable, PIndexState.ACTIVE);
                    fs.delete(outputPath, true);
                }
                return 0;
            } else {
                LOG.error("IndexTool job failed! Check logs for errors..");
                return -1;
            }
        } catch (Exception ex) {
            LOG.error("An exception occurred while performing the indexing job: "
                    + ExceptionUtils.getMessage(ex) + " at:\n" + ExceptionUtils.getStackTrace(ex));
            return -1;
        } finally {
            try {
                if (connection != null) {
                    connection.close();
                }
                if (htable != null) {
                    htable.close();
                }
            } catch (SQLException sqle) {
                LOG.error("Failed to close connection ", sqle.getMessage());
                throw new RuntimeException("Failed to close connection");
            }
        }
    }

    

    private boolean validateSplitForLocalIndex(byte[][] splitKeysBeforeJob, HTable htable) throws Exception {
        if (splitKeysBeforeJob != null
                && !IndexUtil.matchingSplitKeys(splitKeysBeforeJob, htable.getRegionLocator().getStartKeys())) {
            String errMsg = "The index to build is local index and the split keys are not matching"
                    + " before and after running the job. Please rerun the job otherwise"
                    + " there may be inconsistencies between actual data and index data";
            LOG.error(errMsg);
            throw new Exception(errMsg);
        }
        return true;
    }

    /**
     * Checks for the validity of the index table passed to the job.
     * @param connection
     * @param masterTable
     * @param indexTable
     * @return
     * @throws SQLException
     */
    private boolean isValidIndexTable(final Connection connection, final String masterTable,
            final String indexTable) throws SQLException {
        final DatabaseMetaData dbMetaData = connection.getMetaData();
        final String schemaName = SchemaUtil.getSchemaNameFromFullName(masterTable);
        final String tableName = SchemaUtil.normalizeIdentifier(SchemaUtil.getTableNameFromFullName(masterTable));

        ResultSet rs = null;
        try {
            rs = dbMetaData.getIndexInfo(null, schemaName, tableName, false, false);
            while (rs.next()) {
                final String indexName = rs.getString(6);
                if (indexTable.equalsIgnoreCase(indexName)) {
                    return true;
                }
            }
        } finally {
            if (rs != null) {
                rs.close();
            }
        }
        return false;
    }

    public static void main(final String[] args) throws Exception {
        int result = ToolRunner.run(new IndexTool(), args);
        System.exit(result);
    }

}
