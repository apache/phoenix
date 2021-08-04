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
package org.apache.phoenix.mapreduce.transform;

import org.apache.phoenix.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.phoenix.thirdparty.com.google.common.base.Strings;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobPriority;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.phoenix.compile.PostIndexDDLCompiler;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.mapreduce.CsvBulkImportUtil;
import org.apache.phoenix.mapreduce.PhoenixServerBuildIndexInputFormat;
import org.apache.phoenix.mapreduce.index.IndexScrutinyTool;
import org.apache.phoenix.mapreduce.index.IndexTool;
import org.apache.phoenix.mapreduce.index.PhoenixServerBuildIndexDBWritable;
import org.apache.phoenix.mapreduce.index.PhoenixServerBuildIndexMapper;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.apache.phoenix.mapreduce.util.PhoenixMapReduceUtil;
import org.apache.phoenix.parse.HintNode;
import org.apache.phoenix.query.HBaseFactoryProvider;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.transform.SystemTransformRecord;
import org.apache.phoenix.schema.transform.Transform;
import org.apache.phoenix.thirdparty.org.apache.commons.cli.CommandLine;
import org.apache.phoenix.thirdparty.org.apache.commons.cli.CommandLineParser;
import org.apache.phoenix.thirdparty.org.apache.commons.cli.HelpFormatter;
import org.apache.phoenix.thirdparty.org.apache.commons.cli.Option;
import org.apache.phoenix.thirdparty.org.apache.commons.cli.Options;
import org.apache.phoenix.thirdparty.org.apache.commons.cli.ParseException;
import org.apache.phoenix.thirdparty.org.apache.commons.cli.PosixParser;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.hadoop.hbase.HConstants.EMPTY_BYTE_ARRAY;
import static org.apache.phoenix.mapreduce.index.IndexTool.isTimeRangeSet;
import static org.apache.phoenix.mapreduce.index.IndexTool.validateTimeRange;
import static org.apache.phoenix.util.QueryUtil.getConnection;

public class TransformTool extends Configured implements Tool {
    private static final Logger LOGGER = LoggerFactory.getLogger(TransformTool.class);

    public enum MR_COUNTER_METRICS {
        TRANSFORM_FAILED,
        TRANSFORM_SUCCEED
    }

    private static final Option OUTPUT_PATH_OPTION = new Option("op", "output-path", true,
            "Output path where the files are written");
    private static final Option SCHEMA_NAME_OPTION = new Option("s", "schema", true,
            "Phoenix schema name (optional)");
    private static final Option DATA_TABLE_OPTION = new Option("dt", "data-table", true,
            "Data table name (mandatory)");
    private static final Option INDEX_TABLE_OPTION = new Option("it", "index-table", true,
            "Index table name(not required in case of partial rebuilding)");

    private static final Option PARTIAL_TRANSFORM_OPTION = new Option("pt", "partial-transform", false,
            "To transform a data table from a start timestamp");

    private static final Option ABORT_TRANSFORM_OPTION = new Option("abort", "abort", false,
            "Aborts the ongoing transform");

    private static final Option PAUSE_TRANSFORM_OPTION = new Option("pause", "pause", false,
            "Pauses the ongoing transform. If the ongoing transform fails, it will not be retried");

    private static final Option RESUME_TRANSFORM_OPTION = new Option("resume", "resume", false,
            "Resumes the ongoing transform");

    private static final Option JOB_PRIORITY_OPTION = new Option("p", "job-priority", true,
            "Define job priority from 0(highest) to 4. Default is 2(normal)");

    private static final int DEFAULT_AUTOSPLIT_NUM_REGIONS = 20;

    private static final Option AUTO_SPLIT_OPTION =
            new Option("spa", "autosplit", true,
                    "Automatically split the new table if the # of data table regions is greater than N. "
                            + "Takes an optional argument specifying N, otherwise defaults to " + DEFAULT_AUTOSPLIT_NUM_REGIONS
            );

    private static final Option RUN_FOREGROUND_OPTION =
            new Option(
                    "runfg",
                    "run-foreground",
                    false,
                    "If specified, runs transform in Foreground. Default - Runs the transform in background.");

    private static final Option TENANT_ID_OPTION = new Option("tenant", "tenant-id", true,
            "If specified, uses Tenant connection for tenant index transform (optional)");

    private static final Option HELP_OPTION = new Option("h", "help", false, "Help");
    private static final Option START_TIME_OPTION = new Option("st", "start-time",
            true, "Start time for transform");

    private static final Option END_TIME_OPTION = new Option("et", "end-time",
            true, "End time for transform");

    public static final String TRANSFORM_JOB_NAME_TEMPLATE = "PHOENIX_TRANS_%s.%s";

    public static final String PARTIAL_TRANSFORM_NOT_APPLICABLE = "Partial transform accepts "
            + "non-zero ts set in the past as start-time(st) option and that ts must be present in SYSTEM.TRANSFORM table";

    public static final String TRANSFORM_NOT_APPLICABLE = "Transform is not applicable for local indexes or views or transactional tables";

    public static final String PARTIAL_TRANSFORM_NOT_COMPATIBLE = "Can't abort/pause/resume/split during partial transform";

    private Configuration configuration;
    private Connection connection;
    private String tenantId;
    private String dataTable;
    private String logicalParentName;
    private String basePath;
    // logicalTableName is index table and logicalParentName is the data table if this is an index transform
    // If this is a data table transform, logicalParentName is null and logicalTableName is dataTable
    private String logicalTableName;
    private String schemaName;
    private String indexTable;
    private String qDataTable; //normalized with schema
    private PTable pIndexTable = null;
    private PTable pDataTable;
    private PTable pOldTable;
    private PTable pNewTable;

    private String oldTableWithSchema;
    private String newTableWithSchema;
    private JobPriority jobPriority;
    private String jobName;
    private boolean isForeground;
    private Long startTime, endTime, lastTransformTime;
    private boolean isPartialTransform;
    private Job job;

    public Long getStartTime() {
        return startTime;
    }

    public Long getEndTime() { return endTime; }

    public CommandLine parseOptions(String[] args) {
        final Options options = getOptions();
        CommandLineParser parser = new PosixParser();
        CommandLine cmdLine = null;
        try {
            cmdLine = parser.parse(options, args);
        } catch (ParseException e) {
            printHelpAndExit("Error parsing command line options: " + e.getMessage(),
                    options);
        }

        if (cmdLine.hasOption(HELP_OPTION.getOpt())) {
            printHelpAndExit(options, 0);
        }

        this.jobPriority = getJobPriority(cmdLine);

        boolean dataTableProvided = (cmdLine.hasOption(DATA_TABLE_OPTION.getOpt()));
        if (!dataTableProvided) {
            throw new IllegalStateException(DATA_TABLE_OPTION.getLongOpt() + " is a mandatory parameter");
        }

        return cmdLine;
    }

    private Options getOptions() {
        final Options options = new Options();
        options.addOption(OUTPUT_PATH_OPTION);
        options.addOption(SCHEMA_NAME_OPTION);
        options.addOption(DATA_TABLE_OPTION);
        options.addOption(INDEX_TABLE_OPTION);
        options.addOption(TENANT_ID_OPTION);
        options.addOption(HELP_OPTION);
        options.addOption(JOB_PRIORITY_OPTION);
        options.addOption(RUN_FOREGROUND_OPTION);
        options.addOption(PARTIAL_TRANSFORM_OPTION);
        options.addOption(START_TIME_OPTION);
        options.addOption(END_TIME_OPTION);
        options.addOption(AUTO_SPLIT_OPTION);
        options.addOption(ABORT_TRANSFORM_OPTION);
        options.addOption(PAUSE_TRANSFORM_OPTION);
        options.addOption(RESUME_TRANSFORM_OPTION);
        START_TIME_OPTION.setOptionalArg(true);
        END_TIME_OPTION.setOptionalArg(true);
        return options;
    }

    private void printHelpAndExit(String errorMessage, Options options) {
        System.err.println(errorMessage);
        LOGGER.error(errorMessage);
        printHelpAndExit(options, 1);
    }

    private void printHelpAndExit(Options options, int exitCode) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("help", options);
        System.exit(exitCode);
    }

    public CommandLine parseArgs(String[] args) throws Exception {
        CommandLine cmdLine;
        try {
            cmdLine = parseOptions(args);
        } catch (IllegalStateException e) {
            printHelpAndExit(e.getMessage(), getOptions());
            throw e;
        }

        if (getConf() == null) {
            setConf(HBaseConfiguration.create());
        }

        return cmdLine;
    }

    @VisibleForTesting
    public int populateTransformToolAttributesAndValidate(CommandLine cmdLine) throws Exception {
        boolean useStartTime = cmdLine.hasOption(START_TIME_OPTION.getOpt());
        boolean useEndTime = cmdLine.hasOption(END_TIME_OPTION.getOpt());
        basePath = cmdLine.getOptionValue(OUTPUT_PATH_OPTION.getOpt());
        isPartialTransform = cmdLine.hasOption(PARTIAL_TRANSFORM_OPTION.getOpt());
        if (useStartTime) {
            startTime = new Long(cmdLine.getOptionValue(START_TIME_OPTION.getOpt()));
        }

        if (useEndTime) {
            endTime = new Long(cmdLine.getOptionValue(END_TIME_OPTION.getOpt()));
        }

        if (isTimeRangeSet(startTime, endTime)) {
            validateTimeRange(startTime, endTime);
        }

        if (isPartialTransform &&
                (cmdLine.hasOption(AUTO_SPLIT_OPTION.getOpt()))) {
            throw new IllegalArgumentException(PARTIAL_TRANSFORM_NOT_COMPATIBLE);
        }
        if (isPartialTransform &&
                (cmdLine.hasOption(ABORT_TRANSFORM_OPTION.getOpt()) || cmdLine.hasOption(PAUSE_TRANSFORM_OPTION.getOpt())
                        || cmdLine.hasOption(RESUME_TRANSFORM_OPTION.getOpt()))) {
            throw new IllegalArgumentException(PARTIAL_TRANSFORM_NOT_COMPATIBLE);
        }

        if (isPartialTransform) {
            if (!cmdLine.hasOption(START_TIME_OPTION.getOpt())) {
                throw new IllegalArgumentException(PARTIAL_TRANSFORM_NOT_APPLICABLE);
            }
            lastTransformTime = new Long(cmdLine.getOptionValue(START_TIME_OPTION.getOpt()));
            SystemTransformRecord transformRecord = getTransformRecord(null);
            if (transformRecord == null) {
                throw new IllegalArgumentException(PARTIAL_TRANSFORM_NOT_APPLICABLE);
            }
            if (lastTransformTime == null) {
                lastTransformTime = transformRecord.getTransformEndTs().getTime();
            } else {
                validateLastTransformTime();
            }
        }

        schemaName = cmdLine.getOptionValue(SCHEMA_NAME_OPTION.getOpt());
        dataTable = cmdLine.getOptionValue(DATA_TABLE_OPTION.getOpt());
        indexTable = cmdLine.getOptionValue(INDEX_TABLE_OPTION.getOpt());
        qDataTable = SchemaUtil.getQualifiedTableName(schemaName, dataTable);
        isForeground = cmdLine.hasOption(RUN_FOREGROUND_OPTION.getOpt());
        logicalTableName = dataTable;
        logicalParentName = null;
        if (!Strings.isNullOrEmpty(indexTable)) {
            logicalTableName = indexTable;
            logicalParentName = SchemaUtil.getTableName(schemaName, dataTable);
        }

        pDataTable = PhoenixRuntime.getTable(
                connection, SchemaUtil.getQualifiedTableName(schemaName, dataTable));
        if (indexTable != null) {
            pIndexTable = PhoenixRuntime.getTable(
                    connection, SchemaUtil.getQualifiedTableName(schemaName, indexTable));
            pOldTable = pIndexTable;
        } else {
            pOldTable = pDataTable;
        }

        SystemTransformRecord transformRecord = getTransformRecord(connection.unwrap(PhoenixConnection.class));

        validateTransform(pDataTable, pIndexTable, transformRecord);
        String newTableName = SchemaUtil.getTableNameFromFullName(transformRecord.getNewPhysicalTableName());
        pNewTable = PhoenixRuntime.getTableNoCache(
                connection, SchemaUtil.getQualifiedTableName(schemaName, newTableName));


        oldTableWithSchema = SchemaUtil.getQualifiedPhoenixTableName(schemaName, SchemaUtil.getTableNameFromFullName(pOldTable.getName().getString()));
        newTableWithSchema = SchemaUtil.getQualifiedPhoenixTableName(schemaName, SchemaUtil.getTableNameFromFullName(pNewTable.getName().getString()));
        return 0;
    }

    public void validateTransform(PTable argPDataTable, PTable argIndexTable, SystemTransformRecord transformRecord) throws Exception {

        if (argPDataTable.getType() != PTableType.TABLE) {
            throw new IllegalArgumentException(TRANSFORM_NOT_APPLICABLE);
        }

        if (argIndexTable != null && argIndexTable.getType() != PTableType.INDEX) {
            throw new IllegalArgumentException(TRANSFORM_NOT_APPLICABLE);
        }

        if (argPDataTable.isTransactional()) {
            throw new IllegalArgumentException(TRANSFORM_NOT_APPLICABLE);
        }

        if (transformRecord == null){
            throw new IllegalStateException("ALTER statement has not been run and the transform has not been created for this table");
        }

        if (pDataTable != null && pIndexTable != null) {
            if (!IndexTool.isValidIndexTable(connection, qDataTable, indexTable, tenantId)) {
                throw new IllegalArgumentException(
                        String.format(" %s is not an index table for %s for this connection",
                                indexTable, qDataTable));
            }

            PTable.IndexType indexType = argIndexTable.getIndexType();
            if (PTable.IndexType.LOCAL.equals(indexType)) {
                throw new IllegalArgumentException(TRANSFORM_NOT_APPLICABLE);
            }
        }
    }

    public int validateLastTransformTime() throws Exception {
        Long currentTime = EnvironmentEdgeManager.currentTimeMillis();
        if (lastTransformTime.compareTo(currentTime) > 0 || lastTransformTime == 0L) {
            throw new RuntimeException(PARTIAL_TRANSFORM_NOT_APPLICABLE);
        }
        return 0;
    }

    public SystemTransformRecord getTransformRecord(PhoenixConnection connection) throws Exception {
        if (connection == null) {
            try (Connection conn = getConnection(configuration)) {
                SystemTransformRecord transformRecord = Transform.getTransformRecord(schemaName, logicalTableName, logicalParentName, tenantId, conn.unwrap(PhoenixConnection.class));
                return transformRecord;
            }
        } else {
            return  Transform.getTransformRecord(schemaName, logicalTableName, logicalParentName, tenantId, connection);
        }
    }

    public String getJobPriority() {
        return this.jobPriority.toString();
    }

    private JobPriority getJobPriority(CommandLine cmdLine) {
        String jobPriorityOption = cmdLine.getOptionValue(JOB_PRIORITY_OPTION.getOpt());
        if (jobPriorityOption == null) {
            return JobPriority.NORMAL;
        }

        switch (jobPriorityOption) {
            case "0" : return JobPriority.VERY_HIGH;
            case "1" : return JobPriority.HIGH;
            case "2" : return JobPriority.NORMAL;
            case "3" : return JobPriority.LOW;
            case "4" : return JobPriority.VERY_LOW;
            default:
                return JobPriority.NORMAL;
        }
    }

    public Job getJob() {
        return this.job;
    }

    public String getTenantId() {
        return this.tenantId;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public Job configureJob() throws Exception {
        final String jobName = String.format(TRANSFORM_JOB_NAME_TEMPLATE, schemaName, dataTable, indexTable);
        if (lastTransformTime != null) {
            PhoenixConfigurationUtil.setCurrentScnValue(configuration, lastTransformTime);
        }

        final PhoenixConnection pConnection = connection.unwrap(PhoenixConnection.class);
        final PostIndexDDLCompiler ddlCompiler =
                new PostIndexDDLCompiler(pConnection, new TableRef(pOldTable), true);
        ddlCompiler.compile(pNewTable);
        final List<String> newColumns = ddlCompiler.getDataColumnNames();
        //final String selectQuery = ddlCompiler.getSelectQuery();
        final String upsertQuery =
                QueryUtil.constructUpsertStatement(newTableWithSchema, newColumns, HintNode.Hint.NO_INDEX);

        configuration.set(PhoenixConfigurationUtil.UPSERT_STATEMENT, upsertQuery);
        //PhoenixConfigurationUtil.setPhysicalTableName(configuration, pNewTable.getPhysicalName().getString());

        PhoenixConfigurationUtil.setUpsertColumnNames(configuration,
                ddlCompiler.getIndexColumnNames().toArray(new String[ddlCompiler.getIndexColumnNames().size()]));
        if (tenantId != null) {
            PhoenixConfigurationUtil.setTenantId(configuration, tenantId);
        }

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

        PhoenixConfigurationUtil.setIndexToolDataTableName(configuration, oldTableWithSchema);
        PhoenixConfigurationUtil.setIndexToolIndexTableName(configuration, newTableWithSchema);
        PhoenixConfigurationUtil.setIndexToolSourceTable(configuration, IndexScrutinyTool.SourceTable.DATA_TABLE_SOURCE);
        if (startTime != null) {
            PhoenixConfigurationUtil.setIndexToolStartTime(configuration, startTime);
        }

        PhoenixConfigurationUtil.setPhysicalTableName(configuration, pNewTable.getPhysicalName().getString());
        PhoenixConfigurationUtil.setIsTransforming(configuration, true);
        Path outputPath = null;
        org.apache.hadoop.fs.FileSystem fs;
        if (basePath != null) {
            outputPath =
                    CsvBulkImportUtil.getOutputPath(new Path(basePath),
                            pIndexTable == null ?
                                    pDataTable.getPhysicalName().getString() :
                                    pIndexTable.getPhysicalName().getString());
            fs = outputPath.getFileSystem(configuration);
            fs.delete(outputPath, true);
        }
        this.job = Job.getInstance(getConf(), jobName);
        job.setJarByClass(TransformTool.class);
        job.setPriority(this.jobPriority);
        PhoenixMapReduceUtil.setInput(job, PhoenixServerBuildIndexDBWritable.class, PhoenixServerBuildIndexInputFormat.class,
                oldTableWithSchema, "");
        if (outputPath != null) {
            FileOutputFormat.setOutputPath(job, outputPath);
        }
        job.setReducerClass(PhoenixTransformReducer.class);
        job.setNumReduceTasks(1);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);

        //Set the Output classes
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);
        TableMapReduceUtil.addDependencyJars(job);
        job.setMapperClass(PhoenixServerBuildIndexMapper.class);

        TableMapReduceUtil.initCredentials(job);
        LOGGER.info("TransformTool is running for " + job.getJobName());

        return job;
    }

    public int runJob() throws IOException {
        try {
            if (isForeground) {
                LOGGER.info("Running TransformTool in foreground. " +
                        "Runs full table scans. This may take a long time!");
                return (job.waitForCompletion(true)) ? 0 : 1;
            } else {
                LOGGER.info("Running TransformTool in Background - Submit async and exit");
                job.submit();
                return 0;
            }
        } catch (Exception e) {
            LOGGER.error("Caught exception " + e + " trying to run TransformTool.");
            return 1;
        }
    }

    private void preSplitTable(CommandLine cmdLine, Connection connection,
                               Configuration configuration, PTable newTable, PTable oldTable)
            throws SQLException, IOException {
        boolean autosplit = cmdLine.hasOption(AUTO_SPLIT_OPTION.getOpt());

        if (autosplit) {
            String nOpt = cmdLine.getOptionValue(AUTO_SPLIT_OPTION.getOpt());
            int autosplitNumRegions = nOpt == null ? DEFAULT_AUTOSPLIT_NUM_REGIONS : Integer.parseInt(nOpt);
            LOGGER.info(String.format("Will split table %s , autosplit=%s ," +
                            " autoSplitNumRegions=%s", newTable.getPhysicalName(),
                    autosplit, autosplitNumRegions));

            splitTable(connection.unwrap(PhoenixConnection.class), autosplit,
                    autosplitNumRegions, newTable, oldTable);
        }
    }

    private void splitTable(PhoenixConnection pConnection, boolean autosplit,
                            int autosplitNumRegions, PTable newTable, PTable oldTable)
            throws SQLException, IOException, IllegalArgumentException {
        int numRegions;
        byte[][] oldSplitPoints = null;
        byte[][] newSplitPoints = null;
        // TODO : if the rowkey changes via transform, we need to create new split points
        try (Table hDataTable =
                     (Table) pConnection.getQueryServices()
                             .getTable(oldTable.getPhysicalName().getBytes());
             org.apache.hadoop.hbase.client.Connection connection =
                     HBaseFactoryProvider.getHConnectionFactory().createConnection(configuration)) {
            // Avoid duplicate split keys and remove the empty key
            oldSplitPoints = connection.getRegionLocator(hDataTable.getName()).getStartKeys();
            Arrays.sort(oldSplitPoints, Bytes.BYTES_COMPARATOR);
            int numSplits = oldSplitPoints.length;
            ArrayList<byte[]> splitList = new ArrayList<>();
            byte[] lastKey = null;
            for (byte[] keyBytes : oldSplitPoints) {
                if (Bytes.compareTo(keyBytes, EMPTY_BYTE_ARRAY)!=0) {
                    if (lastKey != null && !Bytes.equals(keyBytes, lastKey)) {
                        splitList.add(keyBytes);
                    }
                }
                lastKey = keyBytes;
            }
            newSplitPoints = new byte[splitList.size()][];
            for (int i=0; i < splitList.size(); i++) {
                newSplitPoints[i] = splitList.get(i);
            }
            numRegions = newSplitPoints.length;
            if (autosplit && (numRegions <= autosplitNumRegions)) {
                LOGGER.info(String.format(
                        "Will not split %s because the data table only has %s regions, autoSplitNumRegions=%s",
                        newTable.getPhysicalName(), numRegions, autosplitNumRegions));
                return; // do nothing if # of regions is too low
            }
        }

        try (HBaseAdmin admin = pConnection.getQueryServices().getAdmin()) {
            // do the split
            // drop table and recreate with appropriate splits
            TableName newTableSplitted = TableName.valueOf(newTable.getPhysicalName().getBytes());
            HTableDescriptor descriptor = admin.getTableDescriptor(newTableSplitted);
            admin.disableTable(newTableSplitted);
            admin.deleteTable(newTableSplitted);
            admin.createTable(descriptor, newSplitPoints);
        }
    }

    public void updateTransformRecord(PhoenixConnection connection, PTable.TransformStatus newStatus) throws Exception {
        SystemTransformRecord transformRecord = getTransformRecord(connection);
        updateTransformRecord(connection, transformRecord, newStatus);
    }

    public static void updateTransformRecord(PhoenixConnection connection, SystemTransformRecord transformRecord, PTable.TransformStatus newStatus) throws Exception {
        SystemTransformRecord.SystemTransformBuilder builder = new SystemTransformRecord.SystemTransformBuilder(transformRecord);
        builder.setTransformStatus(newStatus.name());
        if (newStatus == PTable.TransformStatus.COMPLETED || newStatus == PTable.TransformStatus.FAILED) {
            builder.setEndTs(new Timestamp(EnvironmentEdgeManager.currentTimeMillis()));
        }
        Transform.upsertTransform(builder.build(), connection.unwrap(PhoenixConnection.class));
    }

    protected void updateTransformRecord(Job job) throws Exception {
        if (job == null) {
            return;
        }
        SystemTransformRecord transformRecord = getTransformRecord(connection.unwrap(PhoenixConnection.class));
        SystemTransformRecord.SystemTransformBuilder builder = new SystemTransformRecord.SystemTransformBuilder(transformRecord);
        builder.setTransformJobId(job.getJobID().toString());
        builder.setStartTs(new Timestamp(EnvironmentEdgeManager.currentTimeMillis()));
        Transform.upsertTransform(builder.build(), connection.unwrap(PhoenixConnection.class));
    }

    public void killJob(SystemTransformRecord transformRecord) throws Exception{
        String jobId = transformRecord.getTransformJobId();
        if (!Strings.isNullOrEmpty(jobId)) {
            JobClient jobClient = new JobClient();
            RunningJob runningJob = jobClient.getJob(jobId);
            if (runningJob != null) {
                try {
                    runningJob.killJob();
                } catch (IOException ex) {
                    LOGGER.warn("Transform abort could not kill the job. ", ex);
                }
            }
        }
    }

    public void abortTransform() throws Exception {
        SystemTransformRecord transformRecord = getTransformRecord(connection.unwrap(PhoenixConnection.class));
        if (transformRecord.getTransformStatus().equals(PTable.TransformStatus.COMPLETED.name())) {
            throw new IllegalStateException("A completed transform cannot be aborted");
        }

        killJob(transformRecord);
        Transform.removeTransformRecord(transformRecord, connection.unwrap(PhoenixConnection.class));

        // TODO: disable transform on the old table

        // Cleanup syscat
        try (Statement stmt = connection.createStatement()) {
            if (pIndexTable != null) {
                stmt.execute("DROP INDEX " + transformRecord.getNewPhysicalTableName());
            } else {
                stmt.execute("DROP TABLE " + transformRecord.getNewPhysicalTableName());
            }
        } catch (SQLException ex) {
            LOGGER.warn("Transform abort could not drop the table " + transformRecord.getNewPhysicalTableName());
        }
    }

    public void pauseTransform() throws Exception {
        SystemTransformRecord transformRecord = getTransformRecord(connection.unwrap(PhoenixConnection.class));
        if (transformRecord.getTransformStatus().equals(PTable.TransformStatus.COMPLETED.name())) {
            throw new IllegalStateException("A completed transform cannot be paused");
        }

        updateTransformRecord(connection.unwrap(PhoenixConnection.class), PTable.TransformStatus.PAUSED);
        killJob(transformRecord);
    }

    public void resumeTransform(String[] args, CommandLine cmdLine) throws Exception {
        SystemTransformRecord transformRecord = getTransformRecord(connection.unwrap(PhoenixConnection.class));
        if (!transformRecord.getTransformStatus().equals(PTable.TransformStatus.PAUSED.name())) {
            throw new IllegalStateException("Only a paused transform can be resumed");
        }

        runTransform(args, cmdLine);
    }

    public int runTransform(String[] args, CommandLine cmdLine) throws Exception {
        int status = 0;
        updateTransformRecord(connection.unwrap(PhoenixConnection.class), PTable.TransformStatus.STARTED);
        PhoenixConfigurationUtil.setIsPartialTransform(configuration, isPartialTransform);
        PhoenixConfigurationUtil.setIsTransforming(configuration, true);

        if (!Strings.isNullOrEmpty(indexTable)) {
            PhoenixConfigurationUtil.setTransformingTableType(configuration, IndexScrutinyTool.SourceTable.INDEX_TABLE_SOURCE);
            // Index table transform. Build the index
            IndexTool indexTool = new IndexTool();
            indexTool.setConf(configuration);
            status = indexTool.run(args);
            Job job = indexTool.getJob();
            updateTransformRecord(job);
        } else {
            PhoenixConfigurationUtil.setTransformingTableType(configuration, IndexScrutinyTool.SourceTable.DATA_TABLE_SOURCE);
            if (!isPartialTransform) {
                preSplitTable(cmdLine, connection, configuration, pNewTable, pOldTable);
            }
            configureJob();
            status = runJob();
            updateTransformRecord(this.job);
        }

        // Record status
        if (status != 0) {
            LOGGER.error("TransformTool/IndexTool job failed! Check logs for errors..");
            updateTransformRecord(connection.unwrap(PhoenixConnection.class), PTable.TransformStatus.FAILED);
            return -1;
        }

        return status;
    }

    @Override
    public int run(String[] args) throws Exception {
        connection = null;
        int ret = 0;
        CommandLine cmdLine = null;
        configuration = HBaseConfiguration.addHbaseResources(getConf());
        try {
            cmdLine = parseArgs(args);
            if (cmdLine.hasOption(TENANT_ID_OPTION.getOpt())) {
                tenantId = cmdLine.getOptionValue(TENANT_ID_OPTION.getOpt());
                if (!Strings.isNullOrEmpty(tenantId)) {
                    configuration.set(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
                }
            }
            try (Connection conn = getConnection(configuration)) {
                this.connection = conn;
                this.connection.setAutoCommit(true);
                populateTransformToolAttributesAndValidate(cmdLine);
                if (cmdLine.hasOption(ABORT_TRANSFORM_OPTION.getOpt())) {
                    abortTransform();
                } else if (cmdLine.hasOption(PAUSE_TRANSFORM_OPTION.getOpt())) {
                    pauseTransform();
                } else if (cmdLine.hasOption(RESUME_TRANSFORM_OPTION.getOpt())) {
                    resumeTransform(args,  cmdLine);
                } else {
                    ret = runTransform(args, cmdLine);
                }
                return ret;
            } catch (Exception ex) {
                LOGGER.error("An error occurred while transforming " + ExceptionUtils.getMessage(ex) + " at:\n" + ExceptionUtils.getStackTrace(ex));
                return -1;
            }
        } catch (Exception e) {
            e.printStackTrace();
            printHelpAndExit(e.toString(), getOptions());
            return -1;
        }
    }

    public static void main(final String[] args) throws Exception {
        int result = ToolRunner.run(new TransformTool(), args);
        System.exit(result);
    }
}