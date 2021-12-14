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

import static org.apache.phoenix.mapreduce.index.IndexVerificationResultRepository.ROW_KEY_SEPARATOR;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.phoenix.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.phoenix.thirdparty.com.google.common.base.Strings;
import org.apache.phoenix.hbase.index.AbstractValueGetter;
import org.apache.phoenix.thirdparty.org.apache.commons.cli.CommandLine;
import org.apache.phoenix.thirdparty.org.apache.commons.cli.CommandLineParser;
import org.apache.phoenix.thirdparty.org.apache.commons.cli.DefaultParser;
import org.apache.phoenix.thirdparty.org.apache.commons.cli.HelpFormatter;
import org.apache.phoenix.thirdparty.org.apache.commons.cli.Option;
import org.apache.phoenix.thirdparty.org.apache.commons.cli.Options;
import org.apache.phoenix.thirdparty.org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.phoenix.compat.hbase.HbaseCompatCapabilities;
import org.apache.phoenix.compile.PostIndexDDLCompiler;
import org.apache.phoenix.coprocessor.BaseScannerRegionObserver;
import org.apache.phoenix.hbase.index.ValueGetter;
import org.apache.phoenix.hbase.index.covered.update.ColumnReference;
import org.apache.phoenix.hbase.index.util.IndexManagementUtil;
import org.apache.phoenix.index.IndexMaintainer;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixResultSet;
import org.apache.phoenix.mapreduce.CsvBulkImportUtil;
import org.apache.phoenix.mapreduce.PhoenixServerBuildIndexInputFormat;
import org.apache.phoenix.mapreduce.index.IndexScrutinyTool.SourceTable;
import org.apache.phoenix.mapreduce.index.SourceTargetColumnNames.DataSourceColNames;
import org.apache.phoenix.mapreduce.util.ColumnInfoToStringEncoderDecoder;
import org.apache.phoenix.mapreduce.util.ConnectionUtil;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.apache.phoenix.mapreduce.util.PhoenixMapReduceUtil;
import org.apache.phoenix.parse.HintNode.Hint;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTable.IndexType;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.ColumnInfo;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.EquiDepthStreamHistogram;
import org.apache.phoenix.util.EquiDepthStreamHistogram.Bucket;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TransactionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;

/**
 * An MR job to populate the index table from the data table.
 *
 */
public class IndexTool extends Configured implements Tool {
    public enum IndexVerifyType {
        BEFORE("BEFORE"),
        AFTER("AFTER"),
        BOTH("BOTH"),
        ONLY("ONLY"),
        NONE("NONE");
        private String value;
        private byte[] valueBytes;

        IndexVerifyType(String value) {
            this.value = value;
            this.valueBytes = PVarchar.INSTANCE.toBytes(value);
        }

        public String getValue() {
            return this.value;
        }

        public byte[] toBytes() {
            return this.valueBytes;
        }

        public static IndexVerifyType fromValue(String value) {
            for (IndexVerifyType verifyType: IndexVerifyType.values()) {
                if (value.equals(verifyType.getValue())) {
                    return verifyType;
                }
            }
            throw new IllegalStateException("Invalid value: "+ value + " for " + IndexVerifyType.class);
        }

        public static IndexVerifyType fromValue(byte[] value) {
            return fromValue(Bytes.toString(value));
        }
    }

    public enum IndexDisableLoggingType {
        NONE("NONE"),
        BEFORE("BEFORE"),
        AFTER("AFTER"),
        BOTH("BOTH");

        private String value;
        private byte[] valueBytes;

        IndexDisableLoggingType(String value) {
            this.value = value;
            this.valueBytes = PVarchar.INSTANCE.toBytes(value);
        }

        public String getValue() {
            return this.value;
        }

        public byte[] toBytes() {
            return this.valueBytes;
        }

        public static IndexDisableLoggingType fromValue(String value) {
            for (IndexDisableLoggingType disableLoggingType: IndexDisableLoggingType.values()) {
                if (value.equals(disableLoggingType.getValue())) {
                    return disableLoggingType;
                }
            }
            throw new IllegalStateException("Invalid value: "+ value + " for " + IndexDisableLoggingType.class);
        }

        public static IndexDisableLoggingType fromValue(byte[] value) {
            return fromValue(Bytes.toString(value));
        }
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(IndexTool.class);

    //The raw identifiers as passed in, with the escaping used in SQL
    //(double quotes for case sensitivity)
    private String schemaName;
    private String dataTable;
    private String indexTable;
    private String dataTableWithSchema;
    private String indexTableWithSchema;

    private boolean isPartialBuild, isForeground;
    private IndexVerifyType indexVerifyType = IndexVerifyType.NONE;
    private IndexDisableLoggingType disableLoggingType = IndexDisableLoggingType.NONE;
    private SourceTable sourceTable = SourceTable.DATA_TABLE_SOURCE;
    //The qualified normalized table names (no double quotes, case same as HBase table)
    private String qDataTable; //normalized with schema
    private String qIndexTable; //normalized with schema
    private String qSchemaName;
    private boolean useSnapshot;
    private boolean isLocalIndexBuild = false;
    private boolean shouldDeleteBeforeRebuild;
    private PTable pIndexTable = null;
    private PTable pDataTable;
    private String tenantId = null;
    private Job job;
    private Long startTime, endTime, lastVerifyTime;
    private IndexType indexType;
    private String basePath;
    byte[][] splitKeysBeforeJob = null;
    Configuration configuration;

    private static final Option SCHEMA_NAME_OPTION = new Option("s", "schema", true,
            "Phoenix schema name (optional)");
    private static final Option DATA_TABLE_OPTION = new Option("dt", "data-table", true,
            "Data table name (mandatory)");
    private static final Option INDEX_TABLE_OPTION = new Option("it", "index-table", true,
            "Index table name(not required in case of partial rebuilding)");
    
    private static final Option PARTIAL_REBUILD_OPTION = new Option("pr", "partial-rebuild", false,
            "To build indexes for a data table from least disabledTimeStamp");
    
    private static final Option DIRECT_API_OPTION = new Option("direct", "direct", false,
            "This parameter is deprecated. Direct mode will be used whether it is set or not. Keeping it for backwards compatibility.");

    private static final Option VERIFY_OPTION = new Option("v", "verify", true,
            "To verify every data row has a corresponding row of a global index. For other types of indexes, " +
                    "this option will be silently ignored. The accepted values are NONE, ONLY, BEFORE,  AFTER, and BOTH. " +
                    "NONE is for no inline verification, which is also the default for this option. ONLY is for " +
                    "verifying without rebuilding index rows. The rest for verifying before, after, and both before " +
                    "and after rebuilding row. If the verification is done before rebuilding rows and the correct " +
                    "index rows will not be rebuilt");

    private static final double DEFAULT_SPLIT_SAMPLING_RATE = 10.0;

    private static final Option SPLIT_INDEX_OPTION =
            new Option("sp", "split", true,
                    "Split the index table before building, to have the same # of regions as the data table.  "
                    + "The data table is sampled to get uniform index splits across the index values.  "
                    + "Takes an optional argument specifying the sampling rate,"
                    + "otherwise defaults to " + DEFAULT_SPLIT_SAMPLING_RATE);

    private static final int DEFAULT_AUTOSPLIT_NUM_REGIONS = 20;

    private static final Option AUTO_SPLIT_INDEX_OPTION =
            new Option("spa", "autosplit", true,
                    "Automatically split the index table if the # of data table regions is greater than N. "
                    + "Takes an optional argument specifying N, otherwise defaults to " + DEFAULT_AUTOSPLIT_NUM_REGIONS
                    + ".  Can be used in conjunction with -split option to specify the sampling rate");

    private static final Option RUN_FOREGROUND_OPTION =
            new Option(
                    "runfg",
                    "run-foreground",
                    false,
                    "Applicable on top of -direct option."
                            + "If specified, runs index build in Foreground. Default - Runs the build in background.");
    private static final Option OUTPUT_PATH_OPTION = new Option("op", "output-path", true,
            "Output path where the files are written");
    private static final Option SNAPSHOT_OPTION = new Option("snap", "snapshot", false,
        "If specified, uses Snapshots for async index building (optional)");
    private static final Option TENANT_ID_OPTION = new Option("tenant", "tenant-id", true,
        "If specified, uses Tenant connection for tenant view index building (optional)");

    private static final Option DELETE_ALL_AND_REBUILD_OPTION = new Option("deleteall", "delete-all-and-rebuild", false,
            "Applicable only to global indexes on tables, not to local or view indexes. "
            + "If specified, truncates the index table and rebuilds (optional)");

    private static final Option HELP_OPTION = new Option("h", "help", false, "Help");
    private static final Option START_TIME_OPTION = new Option("st", "start-time",
            true, "Start time for indextool rebuild or verify");
    private static final Option END_TIME_OPTION = new Option("et", "end-time",
            true, "End time for indextool rebuild or verify");

    private static final Option RETRY_VERIFY_OPTION = new Option("rv", "retry-verify",
            true, "Max scan ts of the last rebuild/verify that needs to be retried incrementally");

    private static final Option DISABLE_LOGGING_OPTION = new Option("dl",
        "disable-logging", true
        , "Disable logging of failed verification rows for BEFORE, " +
        "AFTER, or BOTH verify jobs");

    private static final Option USE_INDEX_TABLE_AS_SOURCE_OPTION =
        new Option("fi", "from-index", false,
            "To verify every row in the index table has a corresponding row in the data table. "
                + "Only supported for global indexes. If this option is used with -v AFTER, these "
                + "extra rows will be identified but not repaired.");

    public static final String INDEX_JOB_NAME_TEMPLATE = "PHOENIX_%s.%s_INDX_%s";

    public static final String INVALID_TIME_RANGE_EXCEPTION_MESSAGE = "startTime is greater than "
            + "or equal to endTime "
            + "or either of them are set in the future; IndexTool can't proceed.";

    public static final String FEATURE_NOT_APPLICABLE = "start-time/end-time and retry verify feature are only "
            + "applicable for local or non-transactional global indexes";

    public static final String RETRY_VERIFY_NOT_APPLICABLE = "retry verify feature accepts "
            + "non-zero ts set in the past and ts must be present in PHOENIX_INDEX_TOOL_RESULT table";

    private Options getOptions() {
        final Options options = new Options();
        options.addOption(SCHEMA_NAME_OPTION);
        options.addOption(DATA_TABLE_OPTION);
        options.addOption(INDEX_TABLE_OPTION);
        options.addOption(PARTIAL_REBUILD_OPTION);
        options.addOption(DIRECT_API_OPTION);
        options.addOption(VERIFY_OPTION);
        options.addOption(RUN_FOREGROUND_OPTION);
        options.addOption(OUTPUT_PATH_OPTION);
        options.addOption(SNAPSHOT_OPTION);
        options.addOption(TENANT_ID_OPTION);
        options.addOption(DELETE_ALL_AND_REBUILD_OPTION);
        options.addOption(HELP_OPTION);
        AUTO_SPLIT_INDEX_OPTION.setOptionalArg(true);
        SPLIT_INDEX_OPTION.setOptionalArg(true);
        START_TIME_OPTION.setOptionalArg(true);
        END_TIME_OPTION.setOptionalArg(true);
        RETRY_VERIFY_OPTION.setOptionalArg(true);
        options.addOption(AUTO_SPLIT_INDEX_OPTION);
        options.addOption(SPLIT_INDEX_OPTION);
        options.addOption(START_TIME_OPTION);
        options.addOption(END_TIME_OPTION);
        options.addOption(RETRY_VERIFY_OPTION);
        options.addOption(DISABLE_LOGGING_OPTION);
        options.addOption(USE_INDEX_TABLE_AS_SOURCE_OPTION);
        return options;
    }

    /**
     * Parses the commandline arguments, throws IllegalStateException if mandatory arguments are
     * missing.
     * @param args supplied command line arguments
     * @return the parsed command line
     */
    @VisibleForTesting
    public CommandLine parseOptions(String[] args) {

        final Options options = getOptions();

        CommandLineParser parser = new DefaultParser(false, false);
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
        
        if (cmdLine.hasOption(PARTIAL_REBUILD_OPTION.getOpt())
                && cmdLine.hasOption(INDEX_TABLE_OPTION.getOpt())) {
            throw new IllegalStateException("Index name should not be passed with "
                    + PARTIAL_REBUILD_OPTION.getLongOpt());
        }

        if (!cmdLine.hasOption(PARTIAL_REBUILD_OPTION.getOpt())
                && !cmdLine.hasOption(INDEX_TABLE_OPTION.getOpt())) {
            throw new IllegalStateException("Index name should be passed unless it is a partial rebuild.");
        }

        if (cmdLine.hasOption(PARTIAL_REBUILD_OPTION.getOpt()) && cmdLine.hasOption(DELETE_ALL_AND_REBUILD_OPTION.getOpt())) {
            throw new IllegalStateException(DELETE_ALL_AND_REBUILD_OPTION.getLongOpt() + " is not compatible with "
                    + PARTIAL_REBUILD_OPTION.getLongOpt());
        }

        boolean splitIndex = cmdLine.hasOption(AUTO_SPLIT_INDEX_OPTION.getOpt()) || cmdLine.hasOption(SPLIT_INDEX_OPTION.getOpt());
        if (splitIndex && !cmdLine.hasOption(INDEX_TABLE_OPTION.getOpt())) {
            throw new IllegalStateException("Must pass an index name for the split index option");
        }
        if (splitIndex && cmdLine.hasOption(PARTIAL_REBUILD_OPTION.getOpt())) {
            throw new IllegalStateException("Cannot split index for a partial rebuild, as the index table is dropped");
        }
        if (loggingDisabledMismatchesVerifyOption(cmdLine)){
            throw new IllegalStateException("Can't disable index verification logging when no " +
                "index verification or the wrong kind of index verification has been requested. " +
                "VerifyType: [" + cmdLine.getOptionValue(VERIFY_OPTION.getOpt()) + "] and " +
                "DisableLoggingType: ["
                + cmdLine.getOptionValue(DISABLE_LOGGING_OPTION.getOpt()) + "]");
        }
        if ((cmdLine.hasOption(START_TIME_OPTION.getOpt()) || cmdLine.hasOption(RETRY_VERIFY_OPTION.getOpt()))
            && !HbaseCompatCapabilities.isRawFilterSupported()) {
            throw new IllegalStateException("Can't do incremental index verification on this " +
                "version of HBase because raw skip scan filters are not supported.");
        }
        return cmdLine;
    }

    private boolean loggingDisabledMismatchesVerifyOption(CommandLine cmdLine) {
        boolean loggingDisabled = cmdLine.hasOption(DISABLE_LOGGING_OPTION.getOpt());
        if (!loggingDisabled) {
            return false;
        }
        boolean hasVerifyOption =
            cmdLine.hasOption(VERIFY_OPTION.getOpt());
        if (!hasVerifyOption) {
            return true;
        }
        String loggingDisableValue = cmdLine.getOptionValue(DISABLE_LOGGING_OPTION.getOpt());
        String verifyValue = cmdLine.getOptionValue(VERIFY_OPTION.getOpt());
        IndexDisableLoggingType loggingDisableType = IndexDisableLoggingType.fromValue(loggingDisableValue);
        IndexVerifyType verifyType = IndexVerifyType.fromValue(verifyValue);
        //error if we're trying to disable logging when we're not doing any verification
        if (verifyType.equals(IndexVerifyType.NONE)){
            return true;
        }
        //error if we're disabling logging after rebuild but we're not verifying after rebuild
        if ((verifyType.equals(IndexVerifyType.BEFORE) || verifyType.equals(IndexVerifyType.ONLY))
            && loggingDisableType.equals(IndexDisableLoggingType.AFTER)) {
            return true;
        }
        //error if we're disabling logging before rebuild but we're not verifying before rebuild
        if ((verifyType.equals(IndexVerifyType.AFTER))
            && loggingDisableType.equals(IndexDisableLoggingType.BEFORE)) {
            return true;
        }
        if (loggingDisableType.equals(IndexDisableLoggingType.BOTH) &&
            !verifyType.equals(IndexVerifyType.BOTH)){
            return true;
        }
        return false;
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

    public Long getStartTime() {
        return startTime;
    }

    public Long getEndTime() { return endTime; }

    public Long getLastVerifyTime() { return lastVerifyTime; }

    public IndexTool.IndexDisableLoggingType getDisableLoggingType() {
        return disableLoggingType;
    }

    public IndexScrutinyTool.SourceTable getSourceTable() { return sourceTable; }

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

        public Job getJob() throws Exception {
            if (isPartialBuild) {
                return configureJobForPartialBuild();
            } else {
                long maxTimeRange = pIndexTable.getTimeStamp() + 1;
                // this is set to ensure index tables remains consistent post population.
                if (pDataTable.isTransactional()) {
                    configuration.set(PhoenixConfigurationUtil.TX_SCN_VALUE,
                            Long.toString(TransactionUtil.convertToNanoseconds(maxTimeRange)));
                    configuration.set(PhoenixConfigurationUtil.TX_PROVIDER, pDataTable.getTransactionProvider().name());
                }
                if (useSnapshot || (!isLocalIndexBuild && pDataTable.isTransactional())) {
                    PhoenixConfigurationUtil.setCurrentScnValue(configuration, maxTimeRange);
                    if (indexVerifyType != IndexVerifyType.NONE) {
                        LOGGER.warn("Verification is not supported for snapshots and transactional"
                                + "table index rebuilds, verification parameter ignored");
                    }
                    return configureJobForAsyncIndex();
                } else {
                    // Local and non-transactional global indexes to be built on the server side
                    // It is safe not to set CURRENT_SCN_VALUE for server side rebuilds, in order to make sure that
                    // all the rows that exist so far will be rebuilt. The current time of the servers will
                    // be used to set the time range for server side scans.

                    // However, PHOENIX-5732 introduces endTime parameter to be passed optionally for IndexTool.
                    // When endTime is passed for local and non-tx global indexes, we'll override the CURRENT_SCN_VALUE.
                    if (endTime != null) {
                        PhoenixConfigurationUtil.setCurrentScnValue(configuration, endTime);
                    }
                    if (lastVerifyTime != null) {
                        PhoenixConfigurationUtil.setIndexToolLastVerifyTime(configuration, lastVerifyTime);
                    }
                    return configureJobForServerBuildIndex();
                }
            }
        }

        private Job configureJobForPartialBuild() throws Exception {
            connection = ConnectionUtil.getInputConnection(configuration);
            long minDisableTimestamp = HConstants.LATEST_TIMESTAMP;
            PTable indexWithMinDisableTimestamp = null;
            
            //Get Indexes in building state, minDisabledTimestamp 
            List<String> disableIndexes = new ArrayList<String>();
            List<PTable> disabledPIndexes = new ArrayList<PTable>();
            for (PTable index : pDataTable.getIndexes()) {
                if (index.getIndexState().equals(PIndexState.BUILDING)) {
                    disableIndexes.add(index.getTableName().getString());
                    disabledPIndexes.add(index);
                    // We need a way of differentiating the block writes to data table case from
                    // the leave index active case. In either case, we need to know the time stamp
                    // at which writes started failing so we can rebuild from that point. If we
                    // keep the index active *and* have a positive INDEX_DISABLE_TIMESTAMP_BYTES,
                    // then writes to the data table will be blocked (this is client side logic
                    // and we can't change this in a minor release). So we use the sign of the
                    // time stamp to differentiate.
                    long indexDisableTimestamp = Math.abs(index.getIndexDisableTimestamp());
                    if (minDisableTimestamp > indexDisableTimestamp) {
                        minDisableTimestamp = indexDisableTimestamp;
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
                maintainers.add(index.getIndexMaintainer(pDataTable, connection.unwrap(PhoenixConnection.class)));
            }
            ImmutableBytesWritable indexMetaDataPtr = new ImmutableBytesWritable(ByteUtil.EMPTY_BYTE_ARRAY);
            IndexMaintainer.serializeAdditional(pDataTable, indexMetaDataPtr, disabledPIndexes, connection.unwrap(PhoenixConnection.class));
            PhoenixConfigurationUtil.setIndexMaintainers(configuration, indexMetaDataPtr);
            if (!Strings.isNullOrEmpty(tenantId)) {
                PhoenixConfigurationUtil.setTenantId(configuration, tenantId);
            }

            //Prepare raw scan 
            Scan scan = IndexManagementUtil.newLocalStateScan(maintainers);
            scan.setTimeRange(minDisableTimestamp - 1, maxTimestamp);
            scan.setRaw(true);
            scan.setCacheBlocks(false);
            if (pDataTable.isTransactional()) {
                long maxTimeRange = pDataTable.getTimeStamp() + 1;
                scan.setAttribute(BaseScannerRegionObserver.TX_SCN,
                        Bytes.toBytes(TransactionUtil.convertToNanoseconds(maxTimeRange)));
            }
            
          
            String physicalTableName=pDataTable.getPhysicalName().getString();
            final String jobName = String.format("Phoenix Indexes build for " + pDataTable.getName().toString());
            
            PhoenixConfigurationUtil.setInputTableName(configuration, dataTableWithSchema);
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
            return configureSubmittableJobUsingDirectApi(job);
        }
        
        private long getMaxRebuildAsyncDate(String schemaName, List<String> disableIndexes) throws SQLException {
            Long maxRebuilAsyncDate=HConstants.LATEST_TIMESTAMP;
            Long maxDisabledTimeStamp=0L;
            if (disableIndexes == null || disableIndexes.isEmpty()) { return 0; }
            List<String> quotedIndexes = new ArrayList<String>(disableIndexes.size());
            for (String index : disableIndexes) {
                quotedIndexes.add("'" + index + "'");
            }
            try (ResultSet rs = connection.createStatement()
                    .executeQuery("SELECT MAX(" + ASYNC_REBUILD_TIMESTAMP + "),MAX("+INDEX_DISABLE_TIMESTAMP+") FROM " + SYSTEM_CATALOG_NAME + " ("
                            + ASYNC_REBUILD_TIMESTAMP + " BIGINT) WHERE " + TABLE_SCHEM
                            + (schemaName != null && schemaName.length() > 0 ? "='" + schemaName + "'" : " IS NULL")
                            + " and " + TABLE_NAME + " IN (" + StringUtils.join(",", quotedIndexes) + ")")) {
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
        }

        private Job configureJobForAsyncIndex() throws Exception {
            String physicalIndexTable = pIndexTable.getPhysicalName().getString();
            final PhoenixConnection pConnection = connection.unwrap(PhoenixConnection.class);
            final PostIndexDDLCompiler ddlCompiler =
                    new PostIndexDDLCompiler(pConnection, new TableRef(pDataTable));
            ddlCompiler.compile(pIndexTable);
            final List<String> indexColumns = ddlCompiler.getIndexColumnNames();
            final String selectQuery = ddlCompiler.getSelectQuery();
            final String upsertQuery =
                    QueryUtil.constructUpsertStatement(indexTableWithSchema, indexColumns, Hint.NO_INDEX);

            configuration.set(PhoenixConfigurationUtil.UPSERT_STATEMENT, upsertQuery);
            PhoenixConfigurationUtil.setPhysicalTableName(configuration, physicalIndexTable);
            PhoenixConfigurationUtil.setIndexToolIndexTableName(configuration, qIndexTable);
            PhoenixConfigurationUtil.setDisableIndexes(configuration, indexTable);

            PhoenixConfigurationUtil.setUpsertColumnNames(configuration,
                indexColumns.toArray(new String[indexColumns.size()]));
            if (tenantId != null) {
                PhoenixConfigurationUtil.setTenantId(configuration, tenantId);
            }
            final List<ColumnInfo> columnMetadataList =
                    PhoenixRuntime.generateColumnInfo(connection, indexTableWithSchema, indexColumns);
            ColumnInfoToStringEncoderDecoder.encode(configuration, columnMetadataList);

            if (outputPath != null) {
                fs = outputPath.getFileSystem(configuration);
                fs.delete(outputPath, true);
            }
            final String jobName = String.format(INDEX_JOB_NAME_TEMPLATE, schemaName, dataTable, indexTable);
            final Job job = Job.getInstance(configuration, jobName);
            job.setJarByClass(IndexTool.class);
            job.setMapOutputKeyClass(ImmutableBytesWritable.class);
            if (outputPath != null) {
                FileOutputFormat.setOutputPath(job, outputPath);
            }

            if (!useSnapshot) {
                PhoenixMapReduceUtil.setInput(job, PhoenixIndexDBWritable.class, dataTableWithSchema, selectQuery);
            } else {
                Admin admin = null;
                String snapshotName;
                try {
                    admin = pConnection.getQueryServices().getAdmin();
                    TableName hDdataTableName = TableName.valueOf(pDataTable.getPhysicalName().getBytes());
                    snapshotName = new StringBuilder("INDEXTOOL-")
                            .append(pDataTable.getName().getString())
                            .append("-Snapshot-")
                            .append(System.currentTimeMillis())
                            .toString();
                    //FIXME Drop this snapshot after we're done ?
                    admin.snapshot(snapshotName, hDdataTableName);
                } finally {
                    if (admin != null) {
                        admin.close();
                    }
                }
                // root dir not a subdirectory of hbase dir
                Path rootDir = new Path("hdfs:///index-snapshot-dir");
                CommonFSUtils.setRootDir(configuration, rootDir);
                Path restoreDir = new Path(CommonFSUtils.getRootDir(configuration), "restore-dir");

                // set input for map reduce job using hbase snapshots
                PhoenixMapReduceUtil
                            .setInput(job, PhoenixIndexDBWritable.class, snapshotName, dataTableWithSchema, restoreDir, selectQuery);
            }
            TableMapReduceUtil.initCredentials(job);
            
            job.setMapperClass(PhoenixIndexImportDirectMapper.class);
            return configureSubmittableJobUsingDirectApi(job);
        }

        private Job configureJobForServerBuildIndex() throws Exception {
            long indexRebuildQueryTimeoutMs =
                    configuration.getLong(QueryServices.INDEX_REBUILD_QUERY_TIMEOUT_ATTRIB,
                            QueryServicesOptions.DEFAULT_INDEX_REBUILD_QUERY_TIMEOUT);
            long indexRebuildRPCTimeoutMs =
                    configuration.getLong(QueryServices.INDEX_REBUILD_RPC_TIMEOUT_ATTRIB,
                            QueryServicesOptions.DEFAULT_INDEX_REBUILD_RPC_TIMEOUT);
            long indexRebuildClientScannerTimeOutMs =
                    configuration.getLong(QueryServices.INDEX_REBUILD_CLIENT_SCANNER_TIMEOUT_ATTRIB,
                            QueryServicesOptions.DEFAULT_INDEX_REBUILD_CLIENT_SCANNER_TIMEOUT);
            int indexRebuildRpcRetriesCounter =
                    configuration.getInt(QueryServices.INDEX_REBUILD_RPC_RETRIES_COUNTER,
                            QueryServicesOptions.DEFAULT_INDEX_REBUILD_RPC_RETRIES_COUNTER);
            // Set various phoenix and hbase level timeouts and rpc retries
            configuration.set(QueryServices.THREAD_TIMEOUT_MS_ATTRIB,
                    Long.toString(indexRebuildQueryTimeoutMs));
            configuration.set(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD,
                    Long.toString(indexRebuildClientScannerTimeOutMs));
            configuration.set(HConstants.HBASE_RPC_TIMEOUT_KEY,
                    Long.toString(indexRebuildRPCTimeoutMs));
            configuration.set(HConstants.HBASE_CLIENT_RETRIES_NUMBER,
                    Long.toString(indexRebuildRpcRetriesCounter));
            configuration.set("mapreduce.task.timeout", Long.toString(indexRebuildQueryTimeoutMs));

            PhoenixConfigurationUtil.setIndexToolDataTableName(configuration, dataTableWithSchema);
            PhoenixConfigurationUtil.setIndexToolIndexTableName(configuration, qIndexTable);
            PhoenixConfigurationUtil.setIndexToolSourceTable(configuration, sourceTable);
            if (startTime != null) {
                PhoenixConfigurationUtil.setIndexToolStartTime(configuration, startTime);
            }
            PhoenixConfigurationUtil.setIndexVerifyType(configuration, indexVerifyType);
            PhoenixConfigurationUtil.setDisableLoggingVerifyType(configuration, disableLoggingType);
            String physicalIndexTable = pIndexTable.getPhysicalName().getString();

            PhoenixConfigurationUtil.setPhysicalTableName(configuration, physicalIndexTable);
            PhoenixConfigurationUtil.setDisableIndexes(configuration, indexTable);
            if (tenantId != null) {
                PhoenixConfigurationUtil.setTenantId(configuration, tenantId);
            }

            if (outputPath != null) {
                fs = outputPath.getFileSystem(configuration);
                fs.delete(outputPath, true);
            }
            final String jobName = String.format(INDEX_JOB_NAME_TEMPLATE, schemaName, dataTable, indexTable);
            final Job job = Job.getInstance(configuration, jobName);
            job.setJarByClass(IndexTool.class);
            job.setMapOutputKeyClass(ImmutableBytesWritable.class);
            if (outputPath != null) {
                FileOutputFormat.setOutputPath(job, outputPath);
            }

            PhoenixMapReduceUtil.setInput(job, PhoenixServerBuildIndexDBWritable.class, PhoenixServerBuildIndexInputFormat.class,
                            dataTableWithSchema, "");

            TableMapReduceUtil.initCredentials(job);
            job.setMapperClass(PhoenixServerBuildIndexMapper.class);
            return configureSubmittableJobUsingDirectApi(job);
        }

        /**
         * Uses the HBase Front Door Api to write to index table. Submits the job and either returns or
         * waits for the job completion based on runForeground parameter.
         * 
         * @param job
         * @return
         * @throws Exception
         */
        private Job configureSubmittableJobUsingDirectApi(Job job) throws Exception {
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

    public Job getJob() {
        return job;
    }

    public static void createIndexToolTables(Connection connection) throws Exception {
        try (IndexVerificationResultRepository resultRepo = new IndexVerificationResultRepository();
            IndexVerificationOutputRepository outputRepo = new IndexVerificationOutputRepository()){
            resultRepo.createResultTable(connection);
            outputRepo.createOutputTable(connection);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        CommandLine cmdLine;
        try {
            cmdLine = parseOptions(args);
        } catch (IllegalStateException e) {
            printHelpAndExit(e.getMessage(), getOptions());
            return -1;
        }
        configuration = HBaseConfiguration.addHbaseResources(getConf());
        populateIndexToolAttributes(cmdLine);

        if (tenantId != null) {
            configuration.set(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
        }

        try (Connection conn = getConnection(configuration)) {
            createIndexToolTables(conn);
            if (dataTable != null && indexTable != null) {
                setupIndexAndDataTable(conn);
                checkIfFeatureApplicable(startTime, endTime, lastVerifyTime, pDataTable, isLocalIndexBuild);
                if (shouldDeleteBeforeRebuild) {
                    deleteBeforeRebuild(conn);
                }
                preSplitIndexTable(cmdLine, conn);
            }

            boolean result = submitIndexToolJob(conn, configuration);

            if (result) {
                return 0;
            } else {
                LOGGER.error("IndexTool job failed! Check logs for errors..");
                return -1;
            }
        } catch (Exception ex) {
            LOGGER.error("An exception occurred while performing the indexing job: "
                + ExceptionUtils.getMessage(ex) + " at:\n" + ExceptionUtils.getStackTrace(ex));
            return -1;
        }
    }

    public static void checkIfFeatureApplicable(Long startTime, Long endTime, Long lastVerifyTime,
            PTable pDataTable, boolean isLocalIndexBuild) {
        boolean isApplicable = isFeatureApplicable(pDataTable, isLocalIndexBuild);
        if (!isApplicable) {
            if(isTimeRangeSet(startTime, endTime) || lastVerifyTime!=null) {
                throw new RuntimeException(FEATURE_NOT_APPLICABLE);
            }
        }
    }

    private boolean submitIndexToolJob(Connection conn, Configuration configuration)
            throws Exception {
        Path outputPath = null;
        FileSystem fs;
        if (basePath != null) {
            outputPath =
                    CsvBulkImportUtil.getOutputPath(new Path(basePath),
                            pIndexTable == null ?
                                    pDataTable.getPhysicalName().getString() :
                                    pIndexTable.getPhysicalName().getString());
            fs = outputPath.getFileSystem(configuration);
            fs.delete(outputPath, true);
        }
        JobFactory jobFactory = new JobFactory(conn, configuration, outputPath);
        job = jobFactory.getJob();
        if (!isForeground) {
            LOGGER.info("Running Index Build in Background - Submit async and exit");
            job.submit();
            return true;
        }
        LOGGER.info("Running Index Build in Foreground. Waits for the build to complete."
                + " This may take a long time!.");
        return job.waitForCompletion(true);
    }

    @VisibleForTesting
    public int populateIndexToolAttributes(CommandLine cmdLine) throws Exception {
        boolean useTenantId = cmdLine.hasOption(TENANT_ID_OPTION.getOpt());
        boolean useStartTime = cmdLine.hasOption(START_TIME_OPTION.getOpt());
        boolean useEndTime = cmdLine.hasOption(END_TIME_OPTION.getOpt());
        boolean retryVerify = cmdLine.hasOption(RETRY_VERIFY_OPTION.getOpt());
        boolean verify = cmdLine.hasOption(VERIFY_OPTION.getOpt());
        boolean disableLogging = cmdLine.hasOption(DISABLE_LOGGING_OPTION.getOpt());
        boolean useIndexTableAsSource = cmdLine.hasOption(USE_INDEX_TABLE_AS_SOURCE_OPTION.getOpt());

        if (useTenantId) {
            tenantId = cmdLine.getOptionValue(TENANT_ID_OPTION.getOpt());
        }
        if(useStartTime) {
            startTime = new Long(cmdLine.getOptionValue(START_TIME_OPTION.getOpt()));
        }
        if (useEndTime) {
            endTime = new Long(cmdLine.getOptionValue(END_TIME_OPTION.getOpt()));
        }
        if(retryVerify) {
            lastVerifyTime = new Long(cmdLine.getOptionValue(RETRY_VERIFY_OPTION.getOpt()));
            validateLastVerifyTime();
        }
        if(isTimeRangeSet(startTime, endTime)) {
            validateTimeRange();
        }
        if (verify) {
            String value = cmdLine.getOptionValue(VERIFY_OPTION.getOpt());
            indexVerifyType = IndexVerifyType.fromValue(value);
            if (disableLogging) {
                disableLoggingType =
                    IndexDisableLoggingType.fromValue(
                        cmdLine.getOptionValue(DISABLE_LOGGING_OPTION.getOpt()));
            }
        }

        if (useIndexTableAsSource) {
            sourceTable = SourceTable.INDEX_TABLE_SOURCE;
        }

        schemaName = cmdLine.getOptionValue(SCHEMA_NAME_OPTION.getOpt());
        dataTable = cmdLine.getOptionValue(DATA_TABLE_OPTION.getOpt());
        indexTable = cmdLine.getOptionValue(INDEX_TABLE_OPTION.getOpt());
        isPartialBuild = cmdLine.hasOption(PARTIAL_REBUILD_OPTION.getOpt());
        dataTableWithSchema = SchemaUtil.getQualifiedPhoenixTableName(schemaName, dataTable);
        indexTableWithSchema = SchemaUtil.getQualifiedPhoenixTableName(schemaName, indexTable);
        qDataTable = SchemaUtil.getQualifiedTableName(schemaName, dataTable);
        basePath = cmdLine.getOptionValue(OUTPUT_PATH_OPTION.getOpt());
        isForeground = cmdLine.hasOption(RUN_FOREGROUND_OPTION.getOpt());
        useSnapshot = cmdLine.hasOption(SNAPSHOT_OPTION.getOpt());
        shouldDeleteBeforeRebuild = cmdLine.hasOption(DELETE_ALL_AND_REBUILD_OPTION.getOpt());
        return 0;
    }

    public int validateLastVerifyTime() throws Exception {
        Long currentTime = EnvironmentEdgeManager.currentTimeMillis();
        if (lastVerifyTime.compareTo(currentTime) > 0 || lastVerifyTime == 0L || !isValidLastVerifyTime(lastVerifyTime)) {
            throw new RuntimeException(RETRY_VERIFY_NOT_APPLICABLE);
        }
        return 0;
    }

    public boolean isValidLastVerifyTime(Long lastVerifyTime) throws Exception {
        try (Connection conn = getConnection(configuration);
                Table hIndexToolTable = conn.unwrap(PhoenixConnection.class)
                    .getQueryServices()
                    .getTable(IndexVerificationResultRepository.RESULT_TABLE_NAME_BYTES)) {
            Scan s = new Scan();
            ConnectionQueryServices cqs = conn.unwrap(PhoenixConnection.class).getQueryServices();
            boolean isNamespaceMapped = SchemaUtil.isNamespaceMappingEnabled(null, cqs.getProps());
            s.setRowPrefixFilter(Bytes.toBytes(String.format("%s%s%s", lastVerifyTime,
                ROW_KEY_SEPARATOR,
                SchemaUtil.getPhysicalHBaseTableName(qSchemaName, SchemaUtil.normalizeIdentifier(indexTable),
                    isNamespaceMapped))));
            try (ResultScanner rs = hIndexToolTable.getScanner(s)) {
                return rs.next() != null;
            }
        }
    }

    private void validateTimeRange() {
        Long currentTime = EnvironmentEdgeManager.currentTimeMillis();
        Long st = (startTime == null) ? 0 : startTime;
        Long et = (endTime == null) ? currentTime : endTime;
        if (st.compareTo(currentTime) > 0 || et.compareTo(currentTime) > 0 || st.compareTo(et) >= 0) {
            throw new RuntimeException(INVALID_TIME_RANGE_EXCEPTION_MESSAGE);
        }
    }

    private Connection getConnection(Configuration configuration) throws SQLException {
        return ConnectionUtil.getInputConnection(configuration);
    }

    private void setupIndexAndDataTable(Connection connection) throws SQLException, IOException {
        pDataTable = PhoenixRuntime.getTableNoCache(connection, qDataTable);
        if (!isValidIndexTable(connection, qDataTable, indexTable, tenantId)) {
            throw new IllegalArgumentException(
                    String.format(" %s is not an index table for %s for this connection",
                            indexTable, qDataTable));
        }
        qSchemaName = SchemaUtil.normalizeIdentifier(schemaName);
        pIndexTable = PhoenixRuntime.getTable(
            connection, SchemaUtil.getQualifiedTableName(schemaName, indexTable));
        indexType = pIndexTable.getIndexType();
        qIndexTable = SchemaUtil.getQualifiedTableName(schemaName, indexTable);
        if (IndexType.LOCAL.equals(indexType)) {
            isLocalIndexBuild = true;
            if (useSnapshot) {
                throw new IllegalArgumentException(String.format(
                    "%s is a local index. snapshots are not supported for local indexes.",
                    qIndexTable));
            }
            try (org.apache.hadoop.hbase.client.Connection hConn
                    = getTemporaryHConnection(connection.unwrap(PhoenixConnection.class))) {
                RegionLocator regionLocator = hConn
                        .getRegionLocator(TableName.valueOf(pIndexTable.getPhysicalName().getBytes()));
                splitKeysBeforeJob = regionLocator.getStartKeys();
            }
        }
        // We have to mark Disable index to Building before we can set it to Active in the reducer. Otherwise it errors out with
        // index state transition error
        changeDisabledIndexStateToBuiding(connection);
    }

    private static boolean isTimeRangeSet(Long startTime, Long endTime) {
        return startTime != null || endTime != null;
    }

    private static boolean isFeatureApplicable(PTable dataTable, boolean isLocalIndexBuild) {
        if (isLocalIndexBuild || !dataTable.isTransactional()) {
            return true;
        }
        return false;
    }

    private void changeDisabledIndexStateToBuiding(Connection connection) throws SQLException {
        if (pIndexTable != null && pIndexTable.getIndexState() == PIndexState.DISABLE) {
            IndexUtil.updateIndexState(connection.unwrap(PhoenixConnection.class),
                    pIndexTable.getName().getString(), PIndexState.BUILDING, null);
        }
    }

    private void preSplitIndexTable(CommandLine cmdLine, Connection connection)
            throws SQLException, IOException {
        boolean autosplit = cmdLine.hasOption(AUTO_SPLIT_INDEX_OPTION.getOpt());
        boolean splitIndex = cmdLine.hasOption(SPLIT_INDEX_OPTION.getOpt());
        boolean isSalted = pIndexTable.getBucketNum() != null; // no need to split salted tables
        if (!isSalted && IndexType.GLOBAL.equals(indexType) && (autosplit || splitIndex)) {
            String nOpt = cmdLine.getOptionValue(AUTO_SPLIT_INDEX_OPTION.getOpt());
            int autosplitNumRegions = nOpt == null ? DEFAULT_AUTOSPLIT_NUM_REGIONS : Integer.parseInt(nOpt);
            String rateOpt = cmdLine.getOptionValue(SPLIT_INDEX_OPTION.getOpt());
            double samplingRate = rateOpt == null ? DEFAULT_SPLIT_SAMPLING_RATE : Double.parseDouble(rateOpt);
            LOGGER.info(String.format("Will split index %s , autosplit=%s ,"
                            + " autoSplitNumRegions=%s , samplingRate=%s", indexTable, autosplit,
                    autosplitNumRegions, samplingRate));

            splitIndexTable(connection.unwrap(PhoenixConnection.class), autosplit,
                    autosplitNumRegions, samplingRate);
        }
    }

    private void deleteBeforeRebuild(Connection conn) throws SQLException, IOException {
        if (MetaDataUtil.isViewIndex(pIndexTable.getPhysicalName().getString())) {
            throw new IllegalArgumentException(String.format(
                "%s is a view index. delete-all-and-rebuild is not supported for view indexes",
                qIndexTable));
        }

        if (isLocalIndexBuild) {
            throw new IllegalArgumentException(String.format(
                    "%s is a local index.  delete-all-and-rebuild is not supported for local indexes", qIndexTable));
        } else {
            ConnectionQueryServices queryServices = conn.unwrap(PhoenixConnection.class).getQueryServices();
            try (Admin admin = queryServices.getAdmin()){
                TableName tableName = TableName.valueOf(qIndexTable);
                admin.disableTable(tableName);
                admin.truncateTable(tableName, true);
            }
        }
    }

    private void splitIndexTable(PhoenixConnection pConnection, boolean autosplit,
            int autosplitNumRegions, double samplingRate)
            throws SQLException, IOException, IllegalArgumentException {
        int numRegions;

        TableName hDataName = TableName.valueOf(pDataTable.getPhysicalName().getBytes());
        try (org.apache.hadoop.hbase.client.Connection tempHConn = getTemporaryHConnection(pConnection);
                RegionLocator regionLocator =
                        tempHConn.getRegionLocator(hDataName)) {
            numRegions = regionLocator.getStartKeys().length;
            if (autosplit && (numRegions <= autosplitNumRegions)) {
                LOGGER.info(String.format(
                    "Will not split index %s because the data table only has %s regions, autoSplitNumRegions=%s",
                    pIndexTable.getPhysicalName(), numRegions, autosplitNumRegions));
                return; // do nothing if # of regions is too low
            }
        }
        // build a tablesample query to fetch index column values from the data table
        DataSourceColNames colNames = new DataSourceColNames(pDataTable, pIndexTable);
        String qTableSample = String.format("%s TABLESAMPLE(%.2f)", qDataTable, samplingRate);
        List<String> dataColNames = colNames.getDataColNames();
        final String dataSampleQuery =
                QueryUtil.constructSelectStatement(qTableSample, dataColNames, null,
                    Hint.NO_INDEX, true);
        IndexMaintainer maintainer = IndexMaintainer.create(pDataTable, pIndexTable, pConnection);
        ImmutableBytesWritable dataRowKeyPtr = new ImmutableBytesWritable();
        try (final PhoenixResultSet rs =
                pConnection.createStatement().executeQuery(dataSampleQuery)
                        .unwrap(PhoenixResultSet.class);
                Admin admin = pConnection.getQueryServices().getAdmin()) {
            EquiDepthStreamHistogram histo = new EquiDepthStreamHistogram(numRegions);
            ValueGetter getter = getIndexValueGetter(rs, dataColNames);
            // loop over data table rows - build the index rowkey, put it in the histogram
            while (rs.next()) {
                rs.getCurrentRow().getKey(dataRowKeyPtr);
                // regionStart/EndKey only needed for local indexes, so we pass null
                byte[] indexRowKey = maintainer.buildRowKey(getter, dataRowKeyPtr, null, null,
                        rs.getCurrentRow().getValue(0).getTimestamp());
                histo.addValue(indexRowKey);
            }
            List<Bucket> buckets = histo.computeBuckets();
            // do the split
            // to get the splits, we just need the right bound of every histogram bucket, excluding the last
            byte[][] splitPoints = new byte[buckets.size() - 1][];
            int splitIdx = 0;
            for (Bucket b : buckets.subList(0, buckets.size() - 1)) {
                splitPoints[splitIdx++] = b.getRightBoundExclusive();
            }
            // drop table and recreate with appropriate splits
            TableName hIndexName = TableName.valueOf(pIndexTable.getPhysicalName().getBytes());
            HTableDescriptor descriptor = admin.getTableDescriptor(hIndexName);
            admin.disableTable(hIndexName);
            admin.deleteTable(hIndexName);
            admin.createTable(descriptor, splitPoints);
        }
    }

    private org.apache.hadoop.hbase.client.Connection getTemporaryHConnection(PhoenixConnection pConnection)
            throws SQLException, IOException {
        try (Admin admin = pConnection.getQueryServices().getAdmin()) {
            return ConnectionFactory.createConnection(admin.getConfiguration());
        }
    }

    // setup a ValueGetter to get index values from the ResultSet
    private ValueGetter getIndexValueGetter(final PhoenixResultSet rs, List<String> dataColNames) {
        // map from data col name to index in ResultSet
        final Map<String, Integer> rsIndex = new HashMap<>(dataColNames.size());
        int i = 1;
        for (String dataCol : dataColNames) {
            rsIndex.put(SchemaUtil.getEscapedFullColumnName(dataCol), i++);
        }
        return new AbstractValueGetter() {
            final ImmutableBytesWritable valuePtr = new ImmutableBytesWritable();
            final ImmutableBytesWritable rowKeyPtr = new ImmutableBytesWritable();

            @Override
            public ImmutableBytesWritable getLatestValue(ColumnReference ref, long ts) throws IOException {
                try {
                    String fullColumnName =
                            SchemaUtil.getEscapedFullColumnName(SchemaUtil
                                    .getColumnDisplayName(ref.getFamily(), ref.getQualifier()));
                    byte[] colVal = rs.getBytes(rsIndex.get(fullColumnName));
                    valuePtr.set(colVal);
                } catch (SQLException e) {
                    throw new IOException(e);
                }
                return valuePtr;
            }

            @Override
            public byte[] getRowKey() {
                rs.getCurrentRow().getKey(rowKeyPtr);
                return ByteUtil.copyKeyBytesIfNecessary(rowKeyPtr);
            }
        };
    }

    /**
     * Checks for the validity of the index table passed to the job.
     * @param connection
     * @param masterTable
     * @param indexTable
     * @param tenantId
     * @return
     * @throws SQLException
     */
    public static boolean isValidIndexTable(final Connection connection, final String masterTable,
            final String indexTable, final String tenantId) throws SQLException {
        final DatabaseMetaData dbMetaData = connection.getMetaData();
        final String schemaName = SchemaUtil.getSchemaNameFromFullName(masterTable);
        final String tableName = SchemaUtil.getTableNameFromFullName(masterTable);

        ResultSet rs = null;
        try {
            String catalog = "";
            if (tenantId != null) {
                catalog = tenantId;
            }
            rs = dbMetaData.getIndexInfo(catalog, schemaName, tableName, false, false);
            while (rs.next()) {
                final String indexName = rs.getString(6);
                if (SchemaUtil.normalizeIdentifier(indexTable).equalsIgnoreCase(indexName)) {
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

    public static Map.Entry<Integer, Job> run(Configuration conf, String schemaName, String dataTable, String indexTable,
             boolean useSnapshot, String tenantId, boolean disableBefore, boolean shouldDeleteBeforeRebuild, boolean runForeground) throws Exception {
        final List<String> args = Lists.newArrayList();
        if (schemaName != null) {
            args.add("--schema=" + schemaName);
        }
        // Work around CLI-254. The long-form arg parsing doesn't strip off double-quotes
        args.add("--data-table=" + dataTable);
        args.add("--index-table=" + indexTable);

        if (runForeground) {
            args.add("-runfg");
        }

        if (useSnapshot) {
            args.add("-snap");
        }

        if (tenantId != null) {
            args.add("-tenant");
            args.add(tenantId);
        }

        if (shouldDeleteBeforeRebuild) {
            args.add("-deleteall");
        }

        args.add("-op");
        args.add("/tmp/" + UUID.randomUUID().toString());

        if (disableBefore) {
            PhoenixConfigurationUtil.setDisableIndexes(conf, indexTable);
        }

        IndexTool indexingTool = new IndexTool();
        indexingTool.setConf(conf);
        int status = indexingTool.run(args.toArray(new String[0]));
        Job job = indexingTool.getJob();
        return new AbstractMap.SimpleEntry<>(status, job);
    }

    public static void main(final String[] args) throws Exception {
        int result = ToolRunner.run(new IndexTool(), args);
        System.exit(result);
    }

}
