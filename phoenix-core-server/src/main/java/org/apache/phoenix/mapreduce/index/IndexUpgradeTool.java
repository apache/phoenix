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

import org.apache.phoenix.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.phoenix.thirdparty.com.google.common.base.Strings;
import org.apache.phoenix.thirdparty.org.apache.commons.cli.CommandLine;
import org.apache.phoenix.thirdparty.org.apache.commons.cli.CommandLineParser;
import org.apache.phoenix.thirdparty.org.apache.commons.cli.DefaultParser;
import org.apache.phoenix.thirdparty.org.apache.commons.cli.HelpFormatter;
import org.apache.phoenix.thirdparty.org.apache.commons.cli.Option;
import org.apache.phoenix.thirdparty.org.apache.commons.cli.Options;
import org.apache.phoenix.thirdparty.org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.CoprocessorDescriptorBuilder;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.phoenix.hbase.index.IndexRegionObserver;
import org.apache.phoenix.hbase.index.Indexer;
import org.apache.phoenix.hbase.index.covered.NonTxIndexBuilder;
import org.apache.phoenix.index.GlobalIndexChecker;
import org.apache.phoenix.index.PhoenixIndexBuilder;
import org.apache.phoenix.index.PhoenixIndexCodec;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.mapreduce.util.ConnectionUtil;
import org.apache.phoenix.query.ConnectionQueryServices;

import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableType;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.SchemaUtil;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.logging.FileHandler;
import java.util.logging.SimpleFormatter;

import static org.apache.phoenix.query.QueryServicesOptions.
        GLOBAL_INDEX_CHECKER_ENABLED_MAP_EXPIRATION_MIN;

public class IndexUpgradeTool extends Configured implements Tool {

    private static final Logger LOGGER = Logger.getLogger(IndexUpgradeTool.class.getName());

    private static final String INDEX_REBUILD_OPTION_SHORT_OPT = "rb";
    private static final String INDEX_TOOL_OPTION_SHORT_OPT = "tool";

    private static final Option OPERATION_OPTION = new Option("o", "operation",
            true,
            "[Required] Operation to perform (upgrade/rollback)");
    private static final Option TABLE_OPTION = new Option("tb", "table", true,
            "[Required] Tables list ex. table1,table2");
    private static final Option TABLE_CSV_FILE_OPTION = new Option("f", "file",
            true,
            "[Optional] Tables list in a csv file");
    private static final Option DRY_RUN_OPTION = new Option("d", "dry-run",
            false,
            "[Optional] If passed this will output steps that will be executed");
    private static final Option HELP_OPTION = new Option("h", "help",
            false, "Help");
    private static final Option LOG_FILE_OPTION = new Option("lf", "logfile",
            true,
            "[Optional] Log file path where the logs are written");
    private static final Option INDEX_REBUILD_OPTION = new Option(INDEX_REBUILD_OPTION_SHORT_OPT,
            "index-rebuild",
            false,
            "[Optional] Rebuild the indexes. Set -" + INDEX_TOOL_OPTION_SHORT_OPT +
             " to pass options to IndexTool.");
    private static final Option INDEX_TOOL_OPTION = new Option(INDEX_TOOL_OPTION_SHORT_OPT,
            "index-tool",
            true,
            "[Optional] Options to pass to indexTool when rebuilding indexes. " +
            "Set -" + INDEX_REBUILD_OPTION_SHORT_OPT + " to rebuild the index.");

    public static final String UPGRADE_OP = "upgrade";
    public static final String ROLLBACK_OP = "rollback";
    private static final String GLOBAL_INDEX_ID = "#NA#";
    private IndexTool indexingTool;

    private HashMap<String, HashSet<String>> tablesAndIndexes = new HashMap<>();
    private HashMap<String, HashMap<String,IndexInfo>> rebuildMap = new HashMap<>();
    private HashMap<String, String> prop = new  HashMap<>();
    private HashMap<String, String> emptyProp = new HashMap<>();

    private boolean dryRun, upgrade, rebuild;
    private String operation;
    private String inputTables;
    private String logFile;
    private String inputFile;
    private boolean isWaitComplete = false;
    private String indexToolOpts;

    private boolean test = false;
    private boolean failUpgradeTask = false;
    private boolean failDowngradeTask = false;
    private boolean hasFailure = false;

    public void setDryRun(boolean dryRun) {
        this.dryRun = dryRun;
    }

    public void setInputTables(String inputTables) {
        this.inputTables = inputTables;
    }

    public void setLogFile(String logFile) {
        this.logFile = logFile;
    }

    public void setInputFile(String inputFile) {
        this.inputFile = inputFile;
    }

    public void setTest(boolean test) { this.test = test; }

    public boolean getIsWaitComplete() { return this.isWaitComplete; }

    public boolean getDryRun() { return this.dryRun; }

    public String getInputTables() {
        return this.inputTables;
    }

    public String getLogFile() {
        return this.logFile;
    }

    public String getOperation() {
        return this.operation;
    }

    public boolean getIsRebuild() { return this.rebuild; }

    public String getIndexToolOpts() { return this.indexToolOpts; }

    @VisibleForTesting
    public void setFailUpgradeTask(boolean failInitialTask) {
        this.failUpgradeTask = failInitialTask;
    }

    public void setFailDowngradeTask(boolean failRollbackTask) {
        this.failDowngradeTask = failRollbackTask;
    }

    public IndexUpgradeTool(String mode, String tables, String inputFile,
            String outputFile, boolean dryRun, IndexTool indexTool, boolean rebuild) {
        this.operation = mode;
        this.inputTables = tables;
        this.inputFile = inputFile;
        this.logFile = outputFile;
        this.dryRun = dryRun;
        this.indexingTool = indexTool;
        this.rebuild = rebuild;
    }

    public IndexUpgradeTool () { }

    @Override
    public int run(String[] args) throws Exception {
        CommandLine cmdLine = null;
        try {
            cmdLine = parseOptions(args);
            LOGGER.info("Index Upgrade tool initiated: " + String.join(",", args));
        } catch (IllegalStateException e) {
            printHelpAndExit(e.getMessage(), getOptions());
        }
        try {
            initializeTool(cmdLine);
            prepareToolSetup();
            executeTool();
        } catch (Exception e) {
            e.printStackTrace();
            hasFailure = true;
        }
        if (hasFailure) {
            return -1;
        } else {
            return 0;
        }

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

        CommandLineParser parser = DefaultParser.builder().
                setAllowPartialMatching(false).
                setStripLeadingAndTrailingQuotes(false).
                build();
        CommandLine cmdLine = null;
        try {
            cmdLine = parser.parse(options, args);
        } catch (ParseException e) {
            printHelpAndExit("severe parsing command line options: " + e.getMessage(),
                    options);
        }
        if (cmdLine.hasOption(HELP_OPTION.getOpt())) {
            printHelpAndExit(options, 0);
        }
        if (!cmdLine.hasOption(OPERATION_OPTION.getOpt())) {
            throw new IllegalStateException(OPERATION_OPTION.getLongOpt()
                    +" is a mandatory parameter");
        }
        if (cmdLine.hasOption(DRY_RUN_OPTION.getOpt())
                && !cmdLine.hasOption(LOG_FILE_OPTION.getOpt())) {
            throw new IllegalStateException("Log file with "+TABLE_OPTION.getLongOpt()
                    + " is mandatory if " + DRY_RUN_OPTION.getLongOpt() +" is passed");
        }
        if (!(cmdLine.hasOption(TABLE_OPTION.getOpt()))
                && !(cmdLine.hasOption(TABLE_CSV_FILE_OPTION.getOpt()))) {
            throw new IllegalStateException("Tables list should be passed in either with"
                    +TABLE_OPTION.getLongOpt() + " or " + TABLE_CSV_FILE_OPTION.getLongOpt());
        }
        if ((cmdLine.hasOption(TABLE_OPTION.getOpt()))
                && (cmdLine.hasOption(TABLE_CSV_FILE_OPTION.getOpt()))) {
            throw new IllegalStateException("Tables list passed in with"
                    +TABLE_OPTION.getLongOpt() + " and " + TABLE_CSV_FILE_OPTION.getLongOpt()
                    + "; specify only one.");
        }
        if ((cmdLine.hasOption(INDEX_TOOL_OPTION.getOpt()))
                && !cmdLine.hasOption(INDEX_REBUILD_OPTION.getOpt())) {
            throw new IllegalStateException("Index tool options should be passed in with "
                    + INDEX_REBUILD_OPTION.getLongOpt());
        }
        return cmdLine;
    }

    private void printHelpAndExit(String severeMessage, Options options) {
        System.err.println(severeMessage);
        printHelpAndExit(options, 1);
    }

    private void printHelpAndExit(Options options, int exitCode) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("help", options);
        System.exit(exitCode);
    }

    private Options getOptions() {
        final Options options = new Options();
        options.addOption(OPERATION_OPTION);
        TABLE_OPTION.setOptionalArg(true);
        options.addOption(TABLE_OPTION);
        TABLE_CSV_FILE_OPTION.setOptionalArg(true);
        options.addOption(TABLE_CSV_FILE_OPTION);
        DRY_RUN_OPTION.setOptionalArg(true);
        options.addOption(DRY_RUN_OPTION);
        LOG_FILE_OPTION.setOptionalArg(true);
        options.addOption(LOG_FILE_OPTION);
        options.addOption(HELP_OPTION);
        INDEX_REBUILD_OPTION.setOptionalArg(true);
        options.addOption(INDEX_REBUILD_OPTION);
        INDEX_TOOL_OPTION.setOptionalArg(true);
        options.addOption(INDEX_TOOL_OPTION);
        return options;
    }

    @VisibleForTesting
    public void initializeTool(CommandLine cmdLine) {
        operation = cmdLine.getOptionValue(OPERATION_OPTION.getOpt());
        inputTables = cmdLine.getOptionValue(TABLE_OPTION.getOpt());
        logFile = cmdLine.getOptionValue(LOG_FILE_OPTION.getOpt());
        inputFile = cmdLine.getOptionValue(TABLE_CSV_FILE_OPTION.getOpt());
        dryRun = cmdLine.hasOption(DRY_RUN_OPTION.getOpt());
        rebuild = cmdLine.hasOption(INDEX_REBUILD_OPTION.getOpt());
        indexToolOpts = cmdLine.getOptionValue(INDEX_TOOL_OPTION.getOpt());
    }

    @VisibleForTesting
    public void prepareToolSetup() {
        try {
            if (logFile != null) {
                FileHandler fh = new FileHandler(logFile);
                fh.setFormatter(new SimpleFormatter());
                LOGGER.addHandler(fh);
            }

            prop.put(IndexUtil.INDEX_BUILDER_CONF_KEY, PhoenixIndexBuilder.class.getName());
            prop.put(NonTxIndexBuilder.CODEC_CLASS_NAME_KEY, PhoenixIndexCodec.class.getName());

            if (inputTables == null) {
                inputTables = new String(
                        Files.readAllBytes(Paths.get(inputFile)), StandardCharsets.UTF_8);
            }
            if (inputTables == null) {
                LOGGER.severe("Tables' list is not available; use -tb or -f option");
            }
            LOGGER.info("list of tables passed: " + inputTables);

            if (operation.equalsIgnoreCase(UPGRADE_OP)) {
                upgrade = true;
            } else if (operation.equalsIgnoreCase(ROLLBACK_OP)) {
                upgrade = false;
            } else {
                throw new IllegalStateException("Invalid option provided for "
                        + OPERATION_OPTION.getOpt() + " expected values: {upgrade, rollback}");
            }
            if (dryRun) {
                LOGGER.info("This is the beginning of the tool with dry run.");
            }
        } catch (IOException e) {
            LOGGER.severe("Something went wrong " + e);
            System.exit(-1);
        }
    }

    private static void setRpcRetriesAndTimeouts(Configuration conf) {
        long indexRebuildQueryTimeoutMs =
                conf.getLong(QueryServices.INDEX_REBUILD_QUERY_TIMEOUT_ATTRIB,
                        QueryServicesOptions.DEFAULT_INDEX_REBUILD_QUERY_TIMEOUT);
        long indexRebuildRPCTimeoutMs =
                conf.getLong(QueryServices.INDEX_REBUILD_RPC_TIMEOUT_ATTRIB,
                        QueryServicesOptions.DEFAULT_INDEX_REBUILD_RPC_TIMEOUT);
        long indexRebuildClientScannerTimeOutMs =
                conf.getLong(QueryServices.INDEX_REBUILD_CLIENT_SCANNER_TIMEOUT_ATTRIB,
                        QueryServicesOptions.DEFAULT_INDEX_REBUILD_CLIENT_SCANNER_TIMEOUT);
        int indexRebuildRpcRetriesCounter =
                conf.getInt(QueryServices.INDEX_REBUILD_RPC_RETRIES_COUNTER,
                        QueryServicesOptions.DEFAULT_INDEX_REBUILD_RPC_RETRIES_COUNTER);

        // Set phoenix and hbase level timeouts and rpc retries
        conf.setLong(QueryServices.THREAD_TIMEOUT_MS_ATTRIB, indexRebuildQueryTimeoutMs);
        conf.setLong(HConstants.HBASE_RPC_TIMEOUT_KEY, indexRebuildRPCTimeoutMs);
        conf.setLong(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD,
                indexRebuildClientScannerTimeOutMs);
        conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, indexRebuildRpcRetriesCounter);
    }

    @VisibleForTesting
    public static Connection getConnection(Configuration conf) throws SQLException {
        setRpcRetriesAndTimeouts(conf);
        return ConnectionUtil.getInputConnection(conf);
    }

    @VisibleForTesting
    public int executeTool() {
        Configuration conf = HBaseConfiguration.addHbaseResources(getConf());

        try (Connection conn = getConnection(conf)) {
            ConnectionQueryServices queryServices = conn.unwrap(PhoenixConnection.class)
                    .getQueryServices();

            boolean status = extractTablesAndIndexes(conn.unwrap(PhoenixConnection.class));

            if (status) {
                return executeTool(conn, queryServices, conf);
            }
        } catch (SQLException e) {
            LOGGER.severe("Something went wrong in executing tool "+ e);
        }
        return -1;
    }

    private int executeTool(Connection conn,
            ConnectionQueryServices queryServices,
            Configuration conf) {
        ArrayList<String> immutableList = new ArrayList<>();
        ArrayList<String> mutableList = new ArrayList<>();
        for (Map.Entry<String, HashSet<String>> entry :tablesAndIndexes.entrySet()) {
            String dataTableFullName = entry.getKey();
            try {
                PTable dataTable = conn.unwrap(PhoenixConnection.class).getTableNoCache(
                        dataTableFullName);
                if (dataTable.isImmutableRows()) {
                    //add to list where immutable tables are processed in a different function
                    immutableList.add(dataTableFullName);
                } else {
                    mutableList.add(dataTableFullName);
                }
            } catch (SQLException e) {
                LOGGER.severe("Something went wrong while getting the PTable "
                        + dataTableFullName + " " + e);
                return -1;
            }
        }
        long startWaitTime = executeToolForImmutableTables(queryServices, immutableList);
        executeToolForMutableTables(conn, queryServices, conf, mutableList);
        enableImmutableTables(queryServices, immutableList, startWaitTime);
        rebuildIndexes(conn, conf, immutableList);
        if (hasFailure) {
            return -1;
        } else {
            return 0;
        }
    }

    private long executeToolForImmutableTables(ConnectionQueryServices queryServices,
            List<String> immutableList) {
        if (immutableList.isEmpty()) {
            return 0;
        }
        LOGGER.info("Started " + operation + " for immutable tables");
        List<String> failedTables = new ArrayList<String>();
        for (String dataTableFullName : immutableList) {
            try (Admin admin = queryServices.getAdmin()) {
                HashSet<String> indexes = tablesAndIndexes.get(dataTableFullName);
                LOGGER.info("Executing " + operation + " of " + dataTableFullName
                        + " (immutable)");
                disableTable(admin, dataTableFullName, indexes);
                modifyTable(admin, dataTableFullName, indexes);
            } catch (Throwable e) {
                LOGGER.severe("Something went wrong while disabling "
                        + "or modifying immutable table " + e);
                handleFailure(queryServices, dataTableFullName, immutableList, failedTables);
            }
        }
        immutableList.removeAll(failedTables);
        long startWaitTime = EnvironmentEdgeManager.currentTimeMillis();
        return startWaitTime;
    }

    private void executeToolForMutableTables(Connection conn,
            ConnectionQueryServices queryServices,
            Configuration conf,
            ArrayList<String> mutableTables) {
        if (mutableTables.isEmpty()) {
            return;
        }
        LOGGER.info("Started " + operation + " for mutable tables");
        List<String> failedTables = new ArrayList<>();
        for (String dataTableFullName : mutableTables) {
            try (Admin admin = queryServices.getAdmin()) {
                HashSet<String> indexes = tablesAndIndexes.get(dataTableFullName);
                LOGGER.info("Executing " + operation + " of " + dataTableFullName);
                disableTable(admin, dataTableFullName, indexes);
                modifyTable(admin, dataTableFullName, indexes);
                enableTable(admin, dataTableFullName, indexes);
                LOGGER.info("Completed " + operation + " of " + dataTableFullName);
            } catch (Throwable e) {
                LOGGER.severe("Something went wrong while executing "
                    + operation + " steps for "+ dataTableFullName + " " + e);
                handleFailure(queryServices, dataTableFullName, mutableTables, failedTables);
            }
        }
        mutableTables.removeAll(failedTables);
        // Opportunistically kick-off index rebuilds after upgrade operation
        rebuildIndexes(conn, conf, mutableTables);
    }

    private void handleFailure(ConnectionQueryServices queryServices,
            String dataTableFullName,
            List<String> tableList,
            List<String> failedTables) {
        hasFailure = true;
        LOGGER.info("Performing error handling to revert the steps taken during " + operation);
        HashSet<String> indexes = tablesAndIndexes.get(dataTableFullName);
        try (Admin admin = queryServices.getAdmin()) {
            upgrade = !upgrade;
            disableTable(admin, dataTableFullName, indexes);
            modifyTable(admin, dataTableFullName, indexes);
            enableTable(admin, dataTableFullName, indexes);
            upgrade = !upgrade;

            tablesAndIndexes.remove(dataTableFullName); //removing from the map
            failedTables.add(dataTableFullName); //everything in failed tables will later be
            // removed from the list

            LOGGER.severe(dataTableFullName+" has been removed from the list as tool failed"
                    + " to perform "+operation);
        } catch (Throwable e) {
            LOGGER.severe("Revert of the "+operation +" failed in error handling, "
                    + "re-enabling tables and then throwing runtime exception");
            LOGGER.severe("Confirm the state for "+getSubListString(tableList, dataTableFullName));
            try (Admin admin = queryServices.getAdmin()) {
                enableTable(admin, dataTableFullName, indexes);
            } catch (Exception ex) {
                throw new RuntimeException("Error re-enabling tables after rollback failure. " +
                    "Original exception that caused the rollback: [" + e.toString() + " " + "]", ex);
            }
            throw new RuntimeException(e);
        }
    }

    private void enableImmutableTables(ConnectionQueryServices queryServices,
            ArrayList<String> immutableList,
            long startWaitTime) {
        if (immutableList.isEmpty()) {
            return;
        }
        while(true) {
            long waitMore = getWaitMoreTime(startWaitTime);
            if (waitMore <= 0) {
                isWaitComplete = true;
                break;
            }
            try {
                // If the table is immutable, we need to wait for clients to purge
                // their caches of table metadata
                Thread.sleep(waitMore);
                isWaitComplete = true;
            } catch(InterruptedException e) {
                LOGGER.warning("Sleep before starting index rebuild is interrupted. "
                        + "Attempting to sleep again! " + e.getMessage());
            }
        }

        for (String dataTableFullName: immutableList) {
            try (Admin admin = queryServices.getAdmin()) {
                HashSet<String> indexes = tablesAndIndexes.get(dataTableFullName);
                enableTable(admin, dataTableFullName, indexes);
            } catch (IOException | SQLException e) {
                LOGGER.severe("Something went wrong while enabling immutable table " + e);
                //removing to avoid any rebuilds after upgrade
                tablesAndIndexes.remove(dataTableFullName);
                immutableList.remove(dataTableFullName);
                throw new RuntimeException("Manually enable the following tables "
                        + getSubListString(immutableList, dataTableFullName)
                        + " and run the index rebuild ", e);
            }
        }
    }

    private String getSubListString(List<String> tableList, String dataTableFullName) {
        return StringUtils.join(",", tableList.subList(tableList.indexOf(dataTableFullName),
                tableList.size()));
    }

    private long getWaitMoreTime(long startWaitTime) {
        int waitTime = GLOBAL_INDEX_CHECKER_ENABLED_MAP_EXPIRATION_MIN+1;
        long endWaitTime = EnvironmentEdgeManager.currentTimeMillis();
        if(test || dryRun) {
            return 0; //no wait
        }
        return (((waitTime) * 60000) - Math.abs(endWaitTime-startWaitTime));
    }

    private void disableTable(Admin admin, String dataTable, HashSet<String>indexes)
            throws IOException {
        if (admin.isTableEnabled(TableName.valueOf(dataTable))) {
            if (!dryRun) {
                admin.disableTable(TableName.valueOf(dataTable));
            }
            LOGGER.info("Disabled data table " + dataTable);
        } else {
            LOGGER.info( "Data table " + dataTable + " is already disabled");
        }
        for (String indexName : indexes) {
            if (admin.isTableEnabled(TableName.valueOf(indexName))) {
                if (!dryRun) {
                    admin.disableTable(TableName.valueOf(indexName));
                }
                LOGGER.info("Disabled index table " + indexName);
            } else {
                LOGGER.info( "Index table " + indexName + " is already disabled");
            }
        }
    }

    private void modifyTable(Admin admin, String dataTableFullName, HashSet<String> indexes)
            throws IOException {
        if (upgrade) {
            modifyIndexTable(admin, indexes);
            modifyDataTable(admin, dataTableFullName);
            if (test && failUpgradeTask) {
                throw new RuntimeException("Test requested upgrade failure");
            }
        } else {
            modifyDataTable(admin, dataTableFullName);
            modifyIndexTable(admin, indexes);
            if (test && failDowngradeTask) {
                throw new RuntimeException("Test requested downgrade failure");
            }
        }
    }

    private void enableTable(Admin admin, String dataTable, Set<String>indexes)
            throws IOException {
        if (!admin.isTableEnabled(TableName.valueOf(dataTable))) {
            if (!dryRun) {
                admin.enableTable(TableName.valueOf(dataTable));
            }
            LOGGER.info("Enabled data table " + dataTable);
        } else {
            LOGGER.info( "Data table " + dataTable + " is already enabled");
        }
        for (String indexName : indexes) {
            if(!admin.isTableEnabled(TableName.valueOf(indexName))) {
                if (!dryRun) {
                    admin.enableTable(TableName.valueOf(indexName));
                }
                LOGGER.info("Enabled index table " + indexName);
            } else {
                LOGGER.info( "Index table " + indexName + " is already enabled");
            }
        }
    }

    private void rebuildIndexes(Connection conn, Configuration conf, ArrayList<String> tableList) {
        if (!upgrade || !rebuild) {
            return;
        }

        for (String table: tableList) {
            rebuildIndexes(conn, conf, table);
        }
    }

    private void rebuildIndexes(Connection conn, Configuration conf, String dataTableFullName) {
        try {
            HashMap<String, IndexInfo>
                    rebuildMap = prepareToRebuildIndexes(conn, dataTableFullName);

            //for rebuilding indexes in case of upgrade and if there are indexes on the table/view.
            if (rebuildMap.isEmpty()) {
                LOGGER.info("No indexes to rebuild for table " + dataTableFullName);
                return;
            }
            if(!test) {
                indexingTool = new IndexTool();
                indexingTool.setConf(conf);
            }
            startIndexRebuilds(rebuildMap, indexingTool);
        } catch (SQLException e) {
            LOGGER.severe("Failed to prepare the map for index rebuilds " + e);
            throw new RuntimeException("Failed to prepare the map for index rebuilds");
        }
    }

    private void modifyDataTable(Admin admin, String tableName)
            throws IOException {
        TableDescriptorBuilder tableDescBuilder = TableDescriptorBuilder
                .newBuilder(admin.getDescriptor(TableName.valueOf(tableName)));
        if (upgrade) {
            removeCoprocessor(admin, tableName, tableDescBuilder, Indexer.class.getName());
            addCoprocessor(admin, tableName,  tableDescBuilder, IndexRegionObserver.class.getName());
        } else {
            removeCoprocessor(admin, tableName,  tableDescBuilder, IndexRegionObserver.class.getName());
            addCoprocessor(admin, tableName,  tableDescBuilder, Indexer.class.getName());
        }
        if (!dryRun) {
            admin.modifyTable(tableDescBuilder.build());
        }
    }

    private void addCoprocessor(Admin admin, String tableName, TableDescriptorBuilder tableDescBuilder,
                                String coprocName) throws IOException {
        addCoprocessor(admin, tableName, tableDescBuilder, coprocName,
            QueryServicesOptions.DEFAULT_COPROCESSOR_PRIORITY, prop);
    }

    private void addCoprocessor(Admin admin, String tableName, TableDescriptorBuilder tableDescBuilder,
            String coprocName,int priority, Map<String, String> propsToAdd) throws IOException {
        if (!admin.getDescriptor(TableName.valueOf(tableName)).hasCoprocessor(coprocName)) {
            if (!dryRun) {
                CoprocessorDescriptorBuilder coprocBuilder =
                    CoprocessorDescriptorBuilder.newBuilder(coprocName);
                coprocBuilder.setPriority(priority).setProperties(propsToAdd);
                tableDescBuilder.setCoprocessor(coprocBuilder.build());
            }
            LOGGER.info("Loaded " + coprocName + " coprocessor on table " + tableName);
        } else {
            LOGGER.info(coprocName + " coprocessor on table " + tableName + "is already loaded");
        }
    }

    private void removeCoprocessor(Admin admin, String tableName, TableDescriptorBuilder tableDescBuilder,
            String coprocName) throws IOException {
        if (admin.getDescriptor(TableName.valueOf(tableName)).hasCoprocessor(coprocName)) {
            if (!dryRun) {
                tableDescBuilder.removeCoprocessor(coprocName);
            }
            LOGGER.info("Unloaded "+ coprocName +"coprocessor on table " + tableName);
        } else {
            LOGGER.info(coprocName + " coprocessor on table " + tableName + " is already unloaded");
        }
    }

    private void modifyIndexTable(Admin admin, HashSet<String> indexes)
            throws IOException {
        for (String indexName : indexes) {
            TableDescriptorBuilder indexTableDescBuilder = TableDescriptorBuilder
                    .newBuilder(admin.getDescriptor(TableName.valueOf(indexName)));
            if (upgrade) {
                //GlobalIndexChecker needs to be a "lower" priority than all the others so that it
                //goes first. It also doesn't get the codec props the IndexRegionObserver needs
                addCoprocessor(admin, indexName, indexTableDescBuilder, GlobalIndexChecker.class.getName(),
                    QueryServicesOptions.DEFAULT_COPROCESSOR_PRIORITY -1, emptyProp);
            } else {
                removeCoprocessor(admin, indexName, indexTableDescBuilder, GlobalIndexChecker.class.getName());
            }
            if (!dryRun) {
                admin.modifyTable(indexTableDescBuilder.build());
            }
        }
    }

    private int startIndexRebuilds(HashMap<String, IndexInfo> indexInfos,
            IndexTool indexingTool) {

        for(Map.Entry<String, IndexInfo> entry : indexInfos.entrySet()) {
            String index = entry.getKey();
            IndexInfo indexInfo = entry.getValue();
            String indexName = SchemaUtil.getTableNameFromFullName(index);
            String tenantId = indexInfo.getTenantId();
            String baseTable = indexInfo.getBaseTable();
            String schema = indexInfo.getSchemaName();
            String outFile = "/tmp/index_rebuild_" +schema+"_"+ indexName +
                    (GLOBAL_INDEX_ID.equals(tenantId)?"":"_"+tenantId) +"_"
                    + UUID.randomUUID().toString();
            String[] args = getIndexToolArgValues(schema, baseTable, indexName, outFile, tenantId);
            try {
                LOGGER.info("Rebuilding index: " + String.join(",", args));
                if (!dryRun) {
                    indexingTool.run(args);
                }
            } catch (Exception e) {
                LOGGER.severe("Something went wrong while building the index "
                        + index + " " + e);
                return -1;
            }
        }
        return 0;
    }

    public String[] getIndexToolArgValues(String schema, String baseTable, String indexName,
            String outFile, String tenantId) {
        String args[] = { "-s", schema, "-dt", baseTable, "-it", indexName,
                "-direct", "-op", outFile };
        ArrayList<String> list = new ArrayList<>(Arrays.asList(args));
        if (!GLOBAL_INDEX_ID.equals(tenantId)) {
            list.add("-tenant");
            list.add(tenantId);
        }

        if (!Strings.isNullOrEmpty(indexToolOpts)) {
            String[] options = indexToolOpts.split("\\s+");
            for (String opt : options) {
                list.add(opt);
            }
        }
        return list.toArray(new String[list.size()]);
    }

    private boolean extractTablesAndIndexes(PhoenixConnection conn) {
        String [] tables = inputTables.trim().split(",");
        PTable dataTable = null;
        try {
            for (String tableName : tables) {
                HashSet<String> physicalIndexes = new HashSet<>();
                dataTable = conn.getTableNoCache(tableName);
                String physicalTableName = dataTable.getPhysicalName().getString();
                if (!dataTable.isTransactional() && dataTable.getType().equals(PTableType.TABLE)) {
                    for (PTable indexTable : dataTable.getIndexes()) {
                        if (IndexUtil.isGlobalIndex(indexTable)) {
                            String physicalIndexName = indexTable.getPhysicalName().getString();
                            physicalIndexes.add(physicalIndexName);
                        }
                    }
                    if (MetaDataUtil.hasViewIndexTable(conn, dataTable.getPhysicalName())) {
                        String viewIndexPhysicalName = MetaDataUtil
                                .getViewIndexPhysicalName(physicalTableName);
                        physicalIndexes.add(viewIndexPhysicalName);
                    }
                    //for upgrade or rollback
                    tablesAndIndexes.put(physicalTableName, physicalIndexes);
                } else {
                    LOGGER.info("Skipping Table " + tableName + " because it is " +
                            (dataTable.isTransactional() ? "transactional" : "not a data table"));
                }
            }
            return true;
        } catch (SQLException e) {
            LOGGER.severe("Failed to find list of indexes "+e);
            if (dataTable == null) {
                LOGGER.severe("Unable to find the provided data table");
            }
            return false;
        }
    }

    private HashMap<String, IndexInfo> prepareToRebuildIndexes(Connection conn,
            String dataTableFullName) throws SQLException {

        HashMap<String, IndexInfo> indexInfos = new HashMap<>();
        HashSet<String> physicalIndexes = tablesAndIndexes.get(dataTableFullName);

        String viewIndexPhysicalName = MetaDataUtil
                .getViewIndexPhysicalName(dataTableFullName);
        boolean hasViewIndex =  physicalIndexes.contains(viewIndexPhysicalName);
        String schemaName = SchemaUtil.getSchemaNameFromFullName(dataTableFullName);
        String tableName = SchemaUtil.getTableNameFromFullName(dataTableFullName);

        for (String physicalIndexName : physicalIndexes) {
            if (physicalIndexName.equals(viewIndexPhysicalName)) {
                continue;
            }
            String indexTableName = SchemaUtil.getTableNameFromFullName(physicalIndexName);
            String pIndexName = SchemaUtil.getTableName(schemaName, indexTableName);
            IndexInfo indexInfo = new IndexInfo(schemaName, tableName, GLOBAL_INDEX_ID, pIndexName);
            indexInfos.put(physicalIndexName, indexInfo);
        }

        if (hasViewIndex) {
            String viewSql = getViewSql(tableName, schemaName);

            ResultSet rs = conn.createStatement().executeQuery(viewSql);
            while (rs.next()) {
                String viewFullName = rs.getString(1);
                String viewName = SchemaUtil.getTableNameFromFullName(viewFullName);
                String tenantId = rs.getString(2);
                ArrayList<String> viewIndexes = findViewIndexes(conn, schemaName, viewName,
                        tenantId);
                for (String viewIndex : viewIndexes) {
                    IndexInfo indexInfo = new IndexInfo(schemaName, viewName,
                        tenantId == null ? GLOBAL_INDEX_ID : tenantId, viewIndex);
                    indexInfos.put(viewIndex, indexInfo);
                }
            }
        }
        return indexInfos;
    }

    @VisibleForTesting
    public static String getViewSql(String tableName, String schemaName) {
        //column_family has the view name and column_name has the Tenant ID
        return "SELECT DISTINCT COLUMN_FAMILY, COLUMN_NAME FROM "
                + "SYSTEM.CHILD_LINK "
                + "WHERE TABLE_NAME = \'" + tableName + "\'"
                + (!Strings.isNullOrEmpty(schemaName) ? " AND TABLE_SCHEM = \'"
                + schemaName + "\'" : "")
                + " AND LINK_TYPE = "
                + PTable.LinkType.CHILD_TABLE.getSerializedValue();
    }

    private ArrayList<String> findViewIndexes(Connection conn, String schemaName, String viewName,
            String tenantId) throws SQLException {

        String viewIndexesSql = getViewIndexesSql(viewName, schemaName, tenantId);
        ArrayList<String> viewIndexes = new ArrayList<>();
        long stime = EnvironmentEdgeManager.currentTimeMillis();
        ResultSet rs = conn.createStatement().executeQuery(viewIndexesSql);
        long etime = EnvironmentEdgeManager.currentTimeMillis();
        LOGGER.info(String.format("Query %s took %d ms ", viewIndexesSql, (etime - stime)));
        while(rs.next()) {
            String viewIndexName = rs.getString(1);
            viewIndexes.add(viewIndexName);
        }
        return viewIndexes;
    }

    @VisibleForTesting
    public static String getViewIndexesSql(String viewName, String schemaName, String tenantId) {
        return "SELECT DISTINCT COLUMN_FAMILY FROM "
                + "SYSTEM.CATALOG "
                + "WHERE TABLE_NAME = \'" + viewName + "\'"
                + (!Strings.isNullOrEmpty(schemaName) ? " AND TABLE_SCHEM = \'"
                + schemaName + "\'" : "")
                + " AND LINK_TYPE = " + PTable.LinkType.INDEX_TABLE.getSerializedValue()
                + (tenantId != null ?
                    " AND TENANT_ID = \'" + tenantId + "\'" : " AND TENANT_ID IS NULL");
    }

    private static class IndexInfo {
        final private String schemaName;
        final private String baseTable;
        final private String tenantId;
        final private String indexName;

        public IndexInfo(String schemaName, String baseTable, String tenantId, String indexName) {
            this.schemaName = schemaName;
            this.baseTable = baseTable;
            this.tenantId = tenantId;
            this.indexName = indexName;
        }

        public String getSchemaName() {
            return schemaName;
        }

        public String getBaseTable() { return baseTable; }

        public String getTenantId() {
            return tenantId;
        }

        public String getIndexName() {
            return indexName;
        }
    }

    public static void main (String[] args) throws Exception {
        int result = ToolRunner.run(new IndexUpgradeTool(), args);
        System.exit(result);
    }
}
