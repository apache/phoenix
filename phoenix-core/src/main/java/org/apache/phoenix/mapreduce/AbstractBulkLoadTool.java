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
package org.apache.phoenix.mapreduce;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.phoenix.index.IndexMaintainer;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.mapreduce.bulkload.TableRowkeyPair;
import org.apache.phoenix.mapreduce.bulkload.TargetTableRef;
import org.apache.phoenix.mapreduce.bulkload.TargetTableRefFunctions;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTable.IndexType;
import org.apache.phoenix.util.ColumnInfo;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

/**
 * Base tool for running MapReduce-based ingests of data.
 */
public abstract class AbstractBulkLoadTool extends Configured implements Tool {

    protected static final Logger LOG = LoggerFactory.getLogger(AbstractBulkLoadTool.class);

    static final Option ZK_QUORUM_OPT = new Option("z", "zookeeper", true, "Supply zookeeper connection details (optional)");
    static final Option INPUT_PATH_OPT = new Option("i", "input", true, "Input path(s) (comma-separated, mandatory)");
    static final Option OUTPUT_PATH_OPT = new Option("o", "output", true, "Output path for temporary HFiles (optional)");
    static final Option SCHEMA_NAME_OPT = new Option("s", "schema", true, "Phoenix schema name (optional)");
    static final Option TABLE_NAME_OPT = new Option("t", "table", true, "Phoenix table name (mandatory)");
    static final Option INDEX_TABLE_NAME_OPT = new Option("it", "index-table", true, "Phoenix index table name when just loading this particualar index table");
    static final Option IMPORT_COLUMNS_OPT = new Option("c", "import-columns", true, "Comma-separated list of columns to be imported");
    static final Option IGNORE_ERRORS_OPT = new Option("g", "ignore-errors", false, "Ignore input errors");
    static final Option HELP_OPT = new Option("h", "help", false, "Show this help and quit");

    /**
     * Set configuration values based on parsed command line options.
     *
     * @param cmdLine supplied command line options
     * @param importColumns descriptors of columns to be imported
     * @param conf job configuration
     */
    protected abstract void configureOptions(CommandLine cmdLine, List<ColumnInfo> importColumns,
                                         Configuration conf) throws SQLException;
    protected abstract void setupJob(Job job);

    protected Options getOptions() {
        Options options = new Options();
        options.addOption(INPUT_PATH_OPT);
        options.addOption(TABLE_NAME_OPT);
        options.addOption(INDEX_TABLE_NAME_OPT);
        options.addOption(ZK_QUORUM_OPT);
        options.addOption(OUTPUT_PATH_OPT);
        options.addOption(SCHEMA_NAME_OPT);
        options.addOption(IMPORT_COLUMNS_OPT);
        options.addOption(IGNORE_ERRORS_OPT);
        options.addOption(HELP_OPT);
        return options;
    }

    /**
     * Parses the commandline arguments, throws IllegalStateException if mandatory arguments are
     * missing.
     *
     * @param args supplied command line arguments
     * @return the parsed command line
     */
    protected CommandLine parseOptions(String[] args) {

        Options options = getOptions();

        CommandLineParser parser = new PosixParser();
        CommandLine cmdLine = null;
        try {
            cmdLine = parser.parse(options, args);
        } catch (ParseException e) {
            printHelpAndExit("Error parsing command line options: " + e.getMessage(), options);
        }

        if (cmdLine.hasOption(HELP_OPT.getOpt())) {
            printHelpAndExit(options, 0);
        }

        if (!cmdLine.hasOption(TABLE_NAME_OPT.getOpt())) {
            throw new IllegalStateException(TABLE_NAME_OPT.getLongOpt() + " is a mandatory " +
                    "parameter");
        }

        if (!cmdLine.getArgList().isEmpty()) {
            throw new IllegalStateException("Got unexpected extra parameters: "
                    + cmdLine.getArgList());
        }

        if (!cmdLine.hasOption(INPUT_PATH_OPT.getOpt())) {
            throw new IllegalStateException(INPUT_PATH_OPT.getLongOpt() + " is a mandatory " +
                    "parameter");
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

        Configuration conf = HBaseConfiguration.create(getConf());

        CommandLine cmdLine = null;
        try {
            cmdLine = parseOptions(args);
        } catch (IllegalStateException e) {
            printHelpAndExit(e.getMessage(), getOptions());
        }
        return loadData(conf, cmdLine);
    }

    private int loadData(Configuration conf, CommandLine cmdLine) throws Exception {
        String tableName = cmdLine.getOptionValue(TABLE_NAME_OPT.getOpt());
        String schemaName = cmdLine.getOptionValue(SCHEMA_NAME_OPT.getOpt());
        String indexTableName = cmdLine.getOptionValue(INDEX_TABLE_NAME_OPT.getOpt());
        String qualifiedTableName = SchemaUtil.getQualifiedTableName(schemaName, tableName);
        String qualifiedIndexTableName = null;
        if (indexTableName != null){
            qualifiedIndexTableName = SchemaUtil.getQualifiedTableName(schemaName, indexTableName);
        }

        if (cmdLine.hasOption(ZK_QUORUM_OPT.getOpt())) {
            // ZK_QUORUM_OPT is optional, but if it's there, use it for both the conn and the job.
            String zkQuorum = cmdLine.getOptionValue(ZK_QUORUM_OPT.getOpt());
            PhoenixDriver.ConnectionInfo info = PhoenixDriver.ConnectionInfo.create(zkQuorum);
            LOG.info("Configuring HBase connection to {}", info);
            for (Map.Entry<String,String> entry : info.asProps()) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Setting {} = {}", entry.getKey(), entry.getValue());
                }
                conf.set(entry.getKey(), entry.getValue());
            }
        }

        final Connection conn = QueryUtil.getConnection(conf);
        if (LOG.isDebugEnabled()) {
            LOG.debug("Reading columns from {} :: {}", ((PhoenixConnection) conn).getURL(),
                    qualifiedTableName);
        }
        List<ColumnInfo> importColumns = buildImportColumns(conn, cmdLine, qualifiedTableName);
        Preconditions.checkNotNull(importColumns);
        Preconditions.checkArgument(!importColumns.isEmpty(), "Column info list is empty");
        FormatToBytesWritableMapper.configureColumnInfoList(conf, importColumns);
        boolean ignoreInvalidRows = cmdLine.hasOption(IGNORE_ERRORS_OPT.getOpt());
        conf.setBoolean(FormatToBytesWritableMapper.IGNORE_INVALID_ROW_CONFKEY, ignoreInvalidRows);
        conf.set(FormatToBytesWritableMapper.TABLE_NAME_CONFKEY, qualifiedTableName);

        // give subclasses their hook
        configureOptions(cmdLine, importColumns, conf);
        try {
            validateTable(conn, schemaName, tableName);
        } finally {
            conn.close();
        }

        final String inputPaths = cmdLine.getOptionValue(INPUT_PATH_OPT.getOpt());
        final Path outputPath;
        if (cmdLine.hasOption(OUTPUT_PATH_OPT.getOpt())) {
            outputPath = new Path(cmdLine.getOptionValue(OUTPUT_PATH_OPT.getOpt()));
        } else {
            outputPath = new Path("/tmp/" + UUID.randomUUID());
        }

        List<TargetTableRef> tablesToBeLoaded = new ArrayList<TargetTableRef>();
        PTable table = PhoenixRuntime.getTable(conn, qualifiedTableName);
        tablesToBeLoaded.add(new TargetTableRef(qualifiedTableName, table.getPhysicalName().getString()));
        boolean hasLocalIndexes = false;
        for(PTable index: table.getIndexes()) {
            if (index.getIndexType() == IndexType.LOCAL) {
                hasLocalIndexes =
                        qualifiedIndexTableName == null ? true : index.getTableName().getString()
                                .equals(qualifiedIndexTableName);
                if (hasLocalIndexes) break;
            }
        }
        // using conn after it's been closed... o.O
        tablesToBeLoaded.addAll(getIndexTables(conn, schemaName, qualifiedTableName));

        // When loading a single index table, check index table name is correct
        if (qualifiedIndexTableName != null){
            TargetTableRef targetIndexRef = null;
            for (TargetTableRef tmpTable : tablesToBeLoaded){
                if (tmpTable.getLogicalName().compareToIgnoreCase(qualifiedIndexTableName) == 0) {
                    targetIndexRef = tmpTable;
                    break;
                }
            }
            if (targetIndexRef == null){
                throw new IllegalStateException("Bulk Loader error: index table " +
                        qualifiedIndexTableName + " doesn't exist");
            }
            tablesToBeLoaded.clear();
            tablesToBeLoaded.add(targetIndexRef);
        }

        return submitJob(conf, tableName, inputPaths, outputPath, tablesToBeLoaded, hasLocalIndexes);
    }

    /**
     * Submits the jobs to the cluster.
     * Loads the HFiles onto the respective tables.
     * @throws Exception 
     */
    public int submitJob(final Configuration conf, final String qualifiedTableName,
        final String inputPaths, final Path outputPath, List<TargetTableRef> tablesToBeLoaded, boolean hasLocalIndexes) throws Exception {
       
        Job job = Job.getInstance(conf, "Phoenix MapReduce import for " + qualifiedTableName);
        FileInputFormat.addInputPaths(job, inputPaths);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.setInputFormatClass(TextInputFormat.class);
        job.setMapOutputKeyClass(TableRowkeyPair.class);
        job.setMapOutputValueClass(ImmutableBytesWritable.class);
        job.setOutputKeyClass(TableRowkeyPair.class);
        job.setOutputValueClass(KeyValue.class);
        job.setReducerClass(FormatToKeyValueReducer.class);
        byte[][] splitKeysBeforeJob = null;
        HTable table = null;
        if(hasLocalIndexes) {
            try{
                table = new HTable(job.getConfiguration(), qualifiedTableName);
                splitKeysBeforeJob = table.getRegionLocator().getStartKeys();
            } finally {
                if(table != null )table.close();
            }
        }
        MultiHfileOutputFormat.configureIncrementalLoad(job, tablesToBeLoaded);

        final String tableNamesAsJson = TargetTableRefFunctions.NAMES_TO_JSON.apply(tablesToBeLoaded);
        final String logicalNamesAsJson = TargetTableRefFunctions.LOGICAL_NAMES_TO_JSON.apply(tablesToBeLoaded);

        job.getConfiguration().set(FormatToBytesWritableMapper.TABLE_NAMES_CONFKEY,tableNamesAsJson);
        job.getConfiguration().set(FormatToBytesWritableMapper.LOGICAL_NAMES_CONFKEY,logicalNamesAsJson);

        // give subclasses their hook
        setupJob(job);

        LOG.info("Running MapReduce import job from {} to {}", inputPaths, outputPath);
        boolean success = job.waitForCompletion(true);

        if (success) {
            if (hasLocalIndexes) {
                try {
                    table = new HTable(job.getConfiguration(), qualifiedTableName);
                    if(!IndexUtil.matchingSplitKeys(splitKeysBeforeJob, table.getRegionLocator().getStartKeys())) {
                        LOG.error("The table "
                                + qualifiedTableName
                                + " has local indexes and there is split key mismatch before and"
                                + " after running bulkload job. Please rerun the job otherwise"
                                + " there may be inconsistencies between actual data and index data.");
                        return -1;
                    }
                } finally {
                    if (table != null) table.close();
                }
            }
            LOG.info("Loading HFiles from {}", outputPath);
            completebulkload(conf,outputPath,tablesToBeLoaded);
            LOG.info("Removing output directory {}", outputPath);
            if(!outputPath.getFileSystem(conf).delete(outputPath, true)) {
                LOG.error("Failed to delete the output directory {}", outputPath);
            }
            return 0;
        } else {
            return -1;
        }
    }

    private void completebulkload(Configuration conf,Path outputPath , List<TargetTableRef> tablesToBeLoaded) throws Exception {
        Set<String> tableNames = new HashSet<>(tablesToBeLoaded.size());
        for(TargetTableRef table : tablesToBeLoaded) {
            if(tableNames.contains(table.getPhysicalName())){
                continue;
            }
            tableNames.add(table.getPhysicalName());
            LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
            String tableName = table.getPhysicalName();
            Path tableOutputPath = CsvBulkImportUtil.getOutputPath(outputPath, tableName);
            HTable htable = new HTable(conf,tableName);
            LOG.info("Loading HFiles for {} from {}", tableName , tableOutputPath);
            loader.doBulkLoad(tableOutputPath, htable);
            LOG.info("Incremental load complete for table=" + tableName);
        }
    }

    /**
     * Build up the list of columns to be imported. The list is taken from the command line if
     * present, otherwise it is taken from the table description.
     *
     * @param conn connection to Phoenix
     * @param cmdLine supplied command line options
     * @param qualifiedTableName table name (possibly with schema) of the table to be imported
     * @return the list of columns to be imported
     */
    List<ColumnInfo> buildImportColumns(Connection conn, CommandLine cmdLine,
                                        String qualifiedTableName) throws SQLException {
        List<String> userSuppliedColumnNames = null;
        if (cmdLine.hasOption(IMPORT_COLUMNS_OPT.getOpt())) {
            userSuppliedColumnNames = Lists.newArrayList(
                    Splitter.on(",").trimResults().split
                            (cmdLine.getOptionValue(IMPORT_COLUMNS_OPT.getOpt())));
        }
        return SchemaUtil.generateColumnInfo(
                conn, qualifiedTableName, userSuppliedColumnNames, true);
    }

    /**
     * Perform any required validation on the table being bulk loaded into:
     * - ensure no column family names start with '_', as they'd be ignored leading to problems.
     * @throws java.sql.SQLException
     */
    private void validateTable(Connection conn, String schemaName,
                               String tableName) throws SQLException {

        ResultSet rs = conn.getMetaData().getColumns(
                null, StringUtil.escapeLike(schemaName),
                StringUtil.escapeLike(tableName), null);
        while (rs.next()) {
            String familyName = rs.getString(PhoenixDatabaseMetaData.COLUMN_FAMILY);
            if (familyName != null && familyName.startsWith("_")) {
                if (QueryConstants.DEFAULT_COLUMN_FAMILY.equals(familyName)) {
                    throw new IllegalStateException(
                            "Bulk Loader error: All column names that are not part of the " +
                                    "primary key constraint must be prefixed with a column family " +
                                    "name (i.e. f.my_column VARCHAR)");
                } else {
                    throw new IllegalStateException("Bulk Loader error: Column family name " +
                            "must not start with '_': " + familyName);
                }
            }
        }
        rs.close();
    }

    /**
     * Get the index tables of current data table
     * @throws java.sql.SQLException
     */
    private List<TargetTableRef> getIndexTables(Connection conn, String schemaName, String qualifiedTableName)
            throws SQLException {
        PTable table = PhoenixRuntime.getTable(conn, qualifiedTableName);
        List<TargetTableRef> indexTables = new ArrayList<TargetTableRef>();
        for(PTable indexTable : table.getIndexes()){
            indexTables.add(new TargetTableRef(indexTable.getName().getString(), indexTable
                    .getPhysicalName().getString()));
        }
        return indexTables;
    }

}
