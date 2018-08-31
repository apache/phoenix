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

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.mapreduce.CsvBulkImportUtil;
import org.apache.phoenix.mapreduce.util.ConnectionUtil;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.apache.phoenix.mapreduce.util.PhoenixMapReduceUtil;
import org.apache.phoenix.parse.HintNode.Hint;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * An MR job to verify that the index table is in sync with the data table.
 *
 */
public class IndexScrutinyTool extends Configured implements Tool {

    private static final Logger LOG = LoggerFactory.getLogger(IndexScrutinyTool.class);

    private static final Option SCHEMA_NAME_OPTION =
            new Option("s", "schema", true, "Phoenix schema name (optional)");
    private static final Option DATA_TABLE_OPTION =
            new Option("dt", "data-table", true, "Data table name (mandatory)");
    private static final Option INDEX_TABLE_OPTION =
            new Option("it", "index-table", true,
                    "Index table name (mandatory).");
    private static final Option TIMESTAMP =
            new Option("t", "time", true,
                    "Timestamp in millis used to compare the index and data tables.  Defaults to current time minus 60 seconds");

    private static final Option RUN_FOREGROUND_OPTION =
            new Option("runfg", "run-foreground", false, "Applicable on top of -direct option."
                    + "If specified, runs index scrutiny in Foreground. Default - Runs the build in background.");

    private static final Option SNAPSHOT_OPTION = //TODO check if this works
            new Option("snap", "snapshot", false,
                    "If specified, uses Snapshots for async index building (optional)");

    public static final Option BATCH_SIZE_OPTION =
            new Option("b", "batch-size", true, "Number of rows to compare at a time");
    public static final Option SOURCE_TABLE_OPTION =
            new Option("src", "source", true,
                    "Table to use as the source table, whose rows are iterated over and compared to the other table."
                            + " Options are DATA_TABLE_SOURCE, INDEX_TABLE_SOURCE, BOTH."
                            + "  Defaults to BOTH, which does two separate jobs to iterate over both tables");

    private static final Option HELP_OPTION = new Option("h", "help", false, "Help");

    private static final Option OUTPUT_INVALID_ROWS_OPTION =
            new Option("o", "output", false, "Whether to output invalid rows");
    private static final Option OUTPUT_FORMAT_OPTION =
            new Option("of", "output-format", true,
                    "Format in which to output invalid rows.  Options are FILE, TABLE.  Defaults to TABLE");
    private static final Option OUTPUT_PATH_OPTION =
            new Option("op", "output-path", true, "Output path where the files are written");
    private static final Option OUTPUT_MAX = new Option("om", "output-max", true, "Max number of invalid rows to output per mapper.  Defaults to 1M");
    public static final String INDEX_JOB_NAME_TEMPLATE = "PHOENIX_SCRUTINY_[%s]_[%s]";

    /**
     * Which table to use as the source table
     */
    public static enum SourceTable {
        DATA_TABLE_SOURCE, INDEX_TABLE_SOURCE,
        /**
         * Runs two separate jobs to iterate over both tables
         */
        BOTH;
    }

    public static enum OutputFormat {
        FILE, TABLE
    }

    private List<Job> jobs = Lists.newArrayList();

    private Options getOptions() {
        final Options options = new Options();
        options.addOption(SCHEMA_NAME_OPTION);
        options.addOption(DATA_TABLE_OPTION);
        options.addOption(INDEX_TABLE_OPTION);
        options.addOption(RUN_FOREGROUND_OPTION);
        options.addOption(OUTPUT_INVALID_ROWS_OPTION);
        options.addOption(OUTPUT_FORMAT_OPTION);
        options.addOption(OUTPUT_PATH_OPTION);
        options.addOption(OUTPUT_MAX);
        options.addOption(SNAPSHOT_OPTION);
        options.addOption(HELP_OPTION);
        options.addOption(TIMESTAMP);
        options.addOption(BATCH_SIZE_OPTION);
        options.addOption(SOURCE_TABLE_OPTION);
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

        CommandLineParser parser = new DefaultParser();
        CommandLine cmdLine = null;
        try {
            cmdLine = parser.parse(options, args);
        } catch (ParseException e) {
            printHelpAndExit("Error parsing command line options: " + e.getMessage(), options);
        }

        if (cmdLine.hasOption(HELP_OPTION.getOpt())) {
            printHelpAndExit(options, 0);
        }

        requireOption(cmdLine, DATA_TABLE_OPTION);
        requireOption(cmdLine, INDEX_TABLE_OPTION);

        return cmdLine;
    }

    private void requireOption(CommandLine cmdLine, Option option) {
        if (!cmdLine.hasOption(option.getOpt())) {
            throw new IllegalStateException(option.getLongOpt() + " is a mandatory parameter");
        }
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
        private boolean useSnapshot;
        private long ts;
        private boolean outputInvalidRows;
        private OutputFormat outputFormat;
        private String basePath;
        private long scrutinyExecuteTime;
        private long outputMaxRows; // per mapper

        public JobFactory(Connection connection, Configuration configuration, long batchSize,
                boolean useSnapshot, long ts, boolean outputInvalidRows, OutputFormat outputFormat,
                String basePath, long outputMaxRows) {
            this.outputInvalidRows = outputInvalidRows;
            this.outputFormat = outputFormat;
            this.basePath = basePath;
            this.outputMaxRows = outputMaxRows;
            PhoenixConfigurationUtil.setScrutinyBatchSize(configuration, batchSize);
            this.connection = connection;
            this.configuration = configuration;
            this.useSnapshot = useSnapshot;
            this.ts = ts; // CURRENT_SCN to set
            scrutinyExecuteTime = EnvironmentEdgeManager.currentTimeMillis(); // time at which scrutiny was run.
                                                              // Same for
            // all jobs created from this factory
            PhoenixConfigurationUtil.setScrutinyExecuteTimestamp(configuration,
                scrutinyExecuteTime);
        }

        public Job createSubmittableJob(String schemaName, String indexTable, String dataTable,
                SourceTable sourceTable) throws Exception {
            Preconditions.checkArgument(SourceTable.DATA_TABLE_SOURCE.equals(sourceTable)
                    || SourceTable.INDEX_TABLE_SOURCE.equals(sourceTable));

            final String qDataTable = SchemaUtil.getQualifiedTableName(schemaName, dataTable);
            final String qIndexTable;
            if (schemaName != null && !schemaName.isEmpty()) {
                qIndexTable = SchemaUtil.getQualifiedTableName(schemaName, indexTable);
            } else {
                qIndexTable = indexTable;
            }
            PhoenixConfigurationUtil.setScrutinyDataTable(configuration, qDataTable);
            PhoenixConfigurationUtil.setScrutinyIndexTable(configuration, qIndexTable);
            PhoenixConfigurationUtil.setScrutinySourceTable(configuration, sourceTable);
            PhoenixConfigurationUtil.setScrutinyOutputInvalidRows(configuration, outputInvalidRows);
            PhoenixConfigurationUtil.setScrutinyOutputMax(configuration, outputMaxRows);

            final PTable pdataTable = PhoenixRuntime.getTable(connection, qDataTable);
            final PTable pindexTable = PhoenixRuntime.getTable(connection, qIndexTable);

            // set CURRENT_SCN for our scan so that incoming writes don't throw off scrutiny
            configuration.set(PhoenixConfigurationUtil.CURRENT_SCN_VALUE, Long.toString(ts));

            // set the source table to either data or index table
            SourceTargetColumnNames columnNames =
                    SourceTable.DATA_TABLE_SOURCE.equals(sourceTable)
                            ? new SourceTargetColumnNames.DataSourceColNames(pdataTable,
                                    pindexTable)
                            : new SourceTargetColumnNames.IndexSourceColNames(pdataTable,
                                    pindexTable);
            String qSourceTable = columnNames.getQualifiedSourceTableName();
            List<String> sourceColumnNames = columnNames.getSourceColNames();
            List<String> sourceDynamicCols = columnNames.getSourceDynamicCols();
            List<String> targetDynamicCols = columnNames.getTargetDynamicCols();

            // Setup the select query against source - we either select the index columns from the
            // index table,
            // or select the data table equivalents of the index columns from the data table
            final String selectQuery =
                    QueryUtil.constructSelectStatement(qSourceTable, sourceColumnNames, null,
                        Hint.NO_INDEX, true);
            LOG.info("Query used on source table to feed the mapper: " + selectQuery);

            PhoenixConfigurationUtil.setScrutinyOutputFormat(configuration, outputFormat);
            // if outputting to table, setup the upsert to the output table
            if (outputInvalidRows && OutputFormat.TABLE.equals(outputFormat)) {
                String upsertStmt =
                        IndexScrutinyTableOutput.constructOutputTableUpsert(sourceDynamicCols,
                            targetDynamicCols, connection);
                PhoenixConfigurationUtil.setUpsertStatement(configuration, upsertStmt);
                LOG.info("Upsert statement used for output table: " + upsertStmt);
            }

            final String jobName =
                    String.format(INDEX_JOB_NAME_TEMPLATE, qSourceTable,
                        columnNames.getQualifiedTargetTableName());
            final Job job = Job.getInstance(configuration, jobName);

            if (!useSnapshot) {
                PhoenixMapReduceUtil.setInput(job, PhoenixIndexDBWritable.class, qDataTable,
                    selectQuery);
            } else { // TODO check if using a snapshot works
                Admin admin = null;
                String snapshotName;
                try {
                    final PhoenixConnection pConnection =
                            connection.unwrap(PhoenixConnection.class);
                    admin = pConnection.getQueryServices().getAdmin();
                    String pdataTableName = pdataTable.getName().getString();
                    snapshotName = new StringBuilder(pdataTableName).append("-Snapshot").toString();
                    admin.snapshot(snapshotName, TableName.valueOf(pdataTableName));
                } finally {
                    if (admin != null) {
                        admin.close();
                    }
                }
                // root dir not a subdirectory of hbase dir
                Path rootDir = new Path("hdfs:///index-snapshot-dir");
                FSUtils.setRootDir(configuration, rootDir);

                // set input for map reduce job using hbase snapshots
                //PhoenixMapReduceUtil.setInput(job, PhoenixIndexDBWritable.class, snapshotName,
                //    qDataTable, restoreDir, selectQuery);
            }
            TableMapReduceUtil.initCredentials(job);
            Path outputPath =
                    getOutputPath(configuration, basePath,
                        SourceTable.DATA_TABLE_SOURCE.equals(sourceTable) ? pdataTable
                                : pindexTable);

            return configureSubmittableJob(job, outputPath);
        }

        private Job configureSubmittableJob(Job job, Path outputPath) throws Exception {
            Configuration conf = job.getConfiguration();
            conf.setBoolean("mapreduce.job.user.classpath.first", true);
            HBaseConfiguration.merge(conf, HBaseConfiguration.create(conf));
            job.setJarByClass(IndexScrutinyTool.class);
            job.setOutputFormatClass(NullOutputFormat.class);
            if (outputInvalidRows && OutputFormat.FILE.equals(outputFormat)) {
                job.setOutputFormatClass(TextOutputFormat.class);
                FileOutputFormat.setOutputPath(job, outputPath);
            }
            job.setMapperClass(IndexScrutinyMapper.class);
            job.setNumReduceTasks(0);
            // Set the Output classes
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            TableMapReduceUtil.addDependencyJars(job);
            return job;
        }

        Path getOutputPath(final Configuration configuration, String basePath, PTable table)
                throws IOException {
            Path outputPath = null;
            FileSystem fs;
            if (basePath != null) {
                outputPath =
                        CsvBulkImportUtil.getOutputPath(new Path(basePath),
                            table.getPhysicalName().getString());
                fs = outputPath.getFileSystem(configuration);
                fs.delete(outputPath, true);
            }
            return outputPath;
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Connection connection = null;
        try {
            /** start - parse command line configs **/
            CommandLine cmdLine = null;
            try {
                cmdLine = parseOptions(args);
            } catch (IllegalStateException e) {
                printHelpAndExit(e.getMessage(), getOptions());
            }
            final Configuration configuration = HBaseConfiguration.addHbaseResources(getConf());
            connection = ConnectionUtil.getInputConnection(configuration);
            final String schemaName = cmdLine.getOptionValue(SCHEMA_NAME_OPTION.getOpt());
            final String dataTable = cmdLine.getOptionValue(DATA_TABLE_OPTION.getOpt());
            final String indexTable = cmdLine.getOptionValue(INDEX_TABLE_OPTION.getOpt());
            final String qDataTable = SchemaUtil.getQualifiedTableName(schemaName, dataTable);
            String basePath = cmdLine.getOptionValue(OUTPUT_PATH_OPTION.getOpt());
            boolean isForeground = cmdLine.hasOption(RUN_FOREGROUND_OPTION.getOpt());
            boolean useSnapshot = cmdLine.hasOption(SNAPSHOT_OPTION.getOpt());
            boolean outputInvalidRows = cmdLine.hasOption(OUTPUT_INVALID_ROWS_OPTION.getOpt());
            SourceTable sourceTable =
                    cmdLine.hasOption(SOURCE_TABLE_OPTION.getOpt())
                            ? SourceTable
                                    .valueOf(cmdLine.getOptionValue(SOURCE_TABLE_OPTION.getOpt()))
                            : SourceTable.BOTH;

            long batchSize =
                    cmdLine.hasOption(BATCH_SIZE_OPTION.getOpt())
                            ? Long.parseLong(cmdLine.getOptionValue(BATCH_SIZE_OPTION.getOpt()))
                            : PhoenixConfigurationUtil.DEFAULT_SCRUTINY_BATCH_SIZE;

            long ts =
                    cmdLine.hasOption(TIMESTAMP.getOpt())
                            ? Long.parseLong(cmdLine.getOptionValue(TIMESTAMP.getOpt()))
                            : EnvironmentEdgeManager.currentTimeMillis() - 60000;

            if (indexTable != null) {
                if (!isValidIndexTable(connection, qDataTable, indexTable)) {
                    throw new IllegalArgumentException(String
                            .format(" %s is not an index table for %s ", indexTable, qDataTable));
                }
            }

            String outputFormatOption = cmdLine.getOptionValue(OUTPUT_FORMAT_OPTION.getOpt());
            OutputFormat outputFormat =
                    outputFormatOption != null
                            ? OutputFormat.valueOf(outputFormatOption.toUpperCase())
                            : OutputFormat.TABLE;
            long outputMaxRows =
                    cmdLine.hasOption(OUTPUT_MAX.getOpt())
                            ? Long.parseLong(cmdLine.getOptionValue(OUTPUT_MAX.getOpt()))
                            : 1000000L;
            /** end - parse command line configs **/

            if (outputInvalidRows && OutputFormat.TABLE.equals(outputFormat)) {
                // create the output table if it doesn't exist
                try (Connection outputConn = ConnectionUtil.getOutputConnection(configuration)) {
                    outputConn.createStatement().execute(IndexScrutinyTableOutput.OUTPUT_TABLE_DDL);
                    outputConn.createStatement()
                            .execute(IndexScrutinyTableOutput.OUTPUT_METADATA_DDL);
                }
            }

            LOG.info(String.format(
                "Running scrutiny [schemaName=%s, dataTable=%s, indexTable=%s, useSnapshot=%s, timestamp=%s, batchSize=%s, outputBasePath=%s, outputFormat=%s, outputMaxRows=%s]",
                schemaName, dataTable, indexTable, useSnapshot, ts, batchSize, basePath,
                outputFormat, outputMaxRows));
            JobFactory jobFactory =
                    new JobFactory(connection, configuration, batchSize, useSnapshot, ts,
                            outputInvalidRows, outputFormat, basePath, outputMaxRows);
            // If we are running the scrutiny with both tables as the source, run two separate jobs,
            // one for each direction
            if (SourceTable.BOTH.equals(sourceTable)) {
                jobs.add(jobFactory.createSubmittableJob(schemaName, indexTable, dataTable,
                    SourceTable.DATA_TABLE_SOURCE));
                jobs.add(jobFactory.createSubmittableJob(schemaName, indexTable, dataTable,
                    SourceTable.INDEX_TABLE_SOURCE));
            } else {
                jobs.add(jobFactory.createSubmittableJob(schemaName, indexTable, dataTable,
                    sourceTable));
            }

            if (!isForeground) {
                LOG.info("Running Index Scrutiny in Background - Submit async and exit");
                for (Job job : jobs) {
                    job.submit();
                }
                return 0;
            }
            LOG.info(
                "Running Index Scrutiny in Foreground. Waits for the build to complete. This may take a long time!.");
            boolean result = true;
            for (Job job : jobs) {
                result = result && job.waitForCompletion(true);
            }

            // write the results to the output metadata table
            if (outputInvalidRows && OutputFormat.TABLE.equals(outputFormat)) {
                LOG.info("Writing results of jobs to output table "
                        + IndexScrutinyTableOutput.OUTPUT_METADATA_TABLE_NAME);
                IndexScrutinyTableOutput.writeJobResults(connection, args, jobs);
            }

            if (result) {
                return 0;
            } else {
                LOG.error("IndexScrutinyTool job failed! Check logs for errors..");
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
            } catch (SQLException sqle) {
                LOG.error("Failed to close connection ", sqle.getMessage());
                throw new RuntimeException("Failed to close connection");
            }
        }
    }

    @VisibleForTesting
    public List<Job> getJobs() {
        return jobs;
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
        final String tableName =
                SchemaUtil.normalizeIdentifier(SchemaUtil.getTableNameFromFullName(masterTable));

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
        int result = ToolRunner.run(new IndexScrutinyTool(), args);
        System.exit(result);
    }

}
