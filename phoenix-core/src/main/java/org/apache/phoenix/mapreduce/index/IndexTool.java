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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
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
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.phoenix.compile.PostIndexDDLCompiler;
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
import org.apache.phoenix.util.ColumnInfo;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TransactionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
            "Index table name(mandatory)");
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

        if (!cmdLine.hasOption(INDEX_TABLE_OPTION.getOpt())) {
            throw new IllegalStateException(INDEX_TABLE_OPTION.getLongOpt() + " is a mandatory "
                    + "parameter");
        }

        if (!cmdLine.hasOption(OUTPUT_PATH_OPTION.getOpt())) {
            throw new IllegalStateException(OUTPUT_PATH_OPTION.getLongOpt() + " is a mandatory "
                    + "parameter");
        }

        if (!cmdLine.hasOption(DIRECT_API_OPTION.getOpt())
                && cmdLine.hasOption(RUN_FOREGROUND_OPTION.getOpt())) {
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

    @Override
    public int run(String[] args) throws Exception {
        Connection connection = null;
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
            final String qDataTable = SchemaUtil.getQualifiedTableName(schemaName, dataTable);
            final String qIndexTable = SchemaUtil.getQualifiedTableName(schemaName, indexTable);

            connection = ConnectionUtil.getInputConnection(configuration);
            if (!isValidIndexTable(connection, qDataTable, indexTable)) {
                throw new IllegalArgumentException(String.format(
                    " %s is not an index table for %s ", qIndexTable, qDataTable));
            }

            final PTable pdataTable = PhoenixRuntime.getTable(connection, qDataTable);
            final PTable pindexTable = PhoenixRuntime.getTable(connection, qIndexTable);

            // this is set to ensure index tables remains consistent post population.
            long maxTimeRange = pindexTable.getTimeStamp()+1;
            if (pdataTable.isTransactional()) {
                configuration.set(PhoenixConfigurationUtil.TX_SCN_VALUE,
                    Long.toString(TransactionUtil.convertToNanoseconds(maxTimeRange)));
            }
            configuration.set(PhoenixConfigurationUtil.CURRENT_SCN_VALUE,
                Long.toString(maxTimeRange));

            // check if the index type is LOCAL, if so, derive and set the physicalIndexName that is
            // computed from the qDataTable name.
            String physicalIndexTable = pindexTable.getPhysicalName().getString();
            if (IndexType.LOCAL.equals(pindexTable.getIndexType())) {
                physicalIndexTable = qDataTable;
            }

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
            PhoenixConfigurationUtil.setOutputTableName(configuration, indexTable);
            PhoenixConfigurationUtil.setUpsertColumnNames(configuration,
                indexColumns.toArray(new String[indexColumns.size()]));
            final List<ColumnInfo> columnMetadataList =
                    PhoenixRuntime.generateColumnInfo(connection, qIndexTable, indexColumns);
            ColumnInfoToStringEncoderDecoder.encode(configuration, columnMetadataList);

            final Path outputPath = CsvBulkImportUtil
                    .getOutputPath(new Path(cmdLine.getOptionValue(OUTPUT_PATH_OPTION.getOpt())), physicalIndexTable);
            FileSystem.get(configuration).delete(outputPath, true);
            
            final String jobName = String.format(INDEX_JOB_NAME_TEMPLATE, dataTable, indexTable);
            final Job job = Job.getInstance(configuration, jobName);
            job.setJarByClass(IndexTool.class);
            job.setMapOutputKeyClass(ImmutableBytesWritable.class);
            FileOutputFormat.setOutputPath(job, outputPath);
            
            PhoenixMapReduceUtil.setInput(job, PhoenixIndexDBWritable.class, qDataTable,
                selectQuery);
            TableMapReduceUtil.initCredentials(job);
            
            boolean useDirectApi = cmdLine.hasOption(DIRECT_API_OPTION.getOpt());
            if (useDirectApi) {
                job.setMapperClass(PhoenixIndexImportDirectMapper.class);
                configureSubmittableJobUsingDirectApi(job, outputPath,
                    cmdLine.hasOption(RUN_FOREGROUND_OPTION.getOpt()));
            } else {
                job.setMapperClass(PhoenixIndexImportMapper.class);
                configureRunnableJobUsingBulkLoad(job, outputPath);
                // finally update the index state to ACTIVE.
                IndexToolUtil.updateIndexState(connection, qDataTable, indexTable,
                    PIndexState.ACTIVE);
            }
            return 0;
        } catch (Exception ex) {
            LOG.error(" An exception occured while performing the indexing job : "
                    + ExceptionUtils.getStackTrace(ex));
            return -1;
        } finally {
            try {
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException sqle) {
                LOG.error(" Failed to close connection ", sqle.getMessage());
                throw new RuntimeException("Failed to close connection");
            }
        }
    }

    /**
     * Submits the job and waits for completion.
     * @param job
     * @param outputPath
     * @return
     * @throws Exception
     */
    private int configureRunnableJobUsingBulkLoad(Job job, Path outputPath) throws Exception {
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(KeyValue.class);
        final Configuration configuration = job.getConfiguration();
        final String physicalIndexTable =
                PhoenixConfigurationUtil.getPhysicalTableName(configuration);
        final HTable htable = new HTable(configuration, physicalIndexTable);
        HFileOutputFormat.configureIncrementalLoad(job, htable);
        boolean status = job.waitForCompletion(true);
        if (!status) {
            LOG.error("Failed to run the IndexTool job. ");
            htable.close();
            return -1;
        }

        LOG.info("Loading HFiles from {}", outputPath);
        LoadIncrementalHFiles loader = new LoadIncrementalHFiles(configuration);
        loader.doBulkLoad(outputPath, htable);
        htable.close();
        
        FileSystem.get(configuration).delete(outputPath, true);
        
        return 0;
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
    private int configureSubmittableJobUsingDirectApi(Job job, Path outputPath, boolean runForeground)
            throws Exception {
        Configuration conf = job.getConfiguration();
        HBaseConfiguration.merge(conf, HBaseConfiguration.create(conf));
        // Set the Physical Table name for use in DirectHTableWriter#write(Mutation)
        conf.set(TableOutputFormat.OUTPUT_TABLE,
            PhoenixConfigurationUtil.getPhysicalTableName(job.getConfiguration()));
        
        //Set the Output classes
        job.setMapOutputValueClass(IntWritable.class);
        job.setReducerClass(PhoenixIndexToolReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);
        TableMapReduceUtil.addDependencyJars(job);
        job.setNumReduceTasks(1);

        if (!runForeground) {
            LOG.info("Running Index Build in Background - Submit async and exit");
            job.submit();
            return 0;
        }
        LOG.info("Running Index Build in Foreground. Waits for the build to complete. This may take a long time!.");
        boolean result = job.waitForCompletion(true);
        if (!result) {
            LOG.error("Job execution failed!");
            return -1;
        }
        FileSystem.get(conf).delete(outputPath, true);
        return 0;
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
