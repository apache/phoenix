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

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configured;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.phoenix.hbase.index.IndexRegionObserver;
import org.apache.phoenix.hbase.index.Indexer;
import org.apache.phoenix.hbase.index.covered.NonTxIndexBuilder;
import org.apache.phoenix.index.GlobalIndexChecker;
import org.apache.phoenix.index.PhoenixIndexBuilder;
import org.apache.phoenix.index.PhoenixIndexCodec;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.mapreduce.util.ConnectionUtil;
import org.apache.phoenix.query.ConnectionQueryServices;

import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.PhoenixRuntime;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.StringUtil;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.logging.FileHandler;
import java.util.logging.SimpleFormatter;

import static org.apache.phoenix.query.QueryServicesOptions
        .GLOBAL_INDEX_CHECKER_ENABLED_MAP_EXPIRATION_MIN;

public class IndexUpgradeTool extends Configured implements Tool {

    private static final Logger LOGGER = Logger.getLogger(IndexUpgradeTool.class.getName());

    private static final Option OPERATION_OPTION = new Option("o", "operation",
            true,
            "[Required]Operation to perform (upgrade/rollback)");
    private static final Option TABLE_OPTION = new Option("tb", "table", true,
            "[Required]Tables list ex. table1,table2");
    private static final Option TABLE_CSV_FILE_OPTION = new Option("f", "file",
            true,
            "[Optional]Tables list in a csv file");
    private static final Option DRY_RUN_OPTION = new Option("d", "dry-run",
            false,
            "[Optional]If passed this will output steps that will be executed");
    private static final Option HELP_OPTION = new Option("h", "help",
            false, "Help");
    private static final Option LOG_FILE_OPTION = new Option("lf", "logfile",
            true,
            "Log file path where the logs are written");
    private static final Option INDEX_SYNC_REBUILD_OPTION = new Option("sr", "index-sync-rebuild",
            false,
            "[Optional]Whether or not synchronously rebuild the indexes; "
                    + "default rebuild asynchronous");

    public static final String UPGRADE_OP = "upgrade";
    public static final String ROLLBACK_OP = "rollback";
    private static final String GLOBAL_INDEX_ID = "#NA#";
    private IndexTool indexingTool;

    private HashMap<String, HashSet<String>> tablesAndIndexes = new HashMap<>();
    private HashMap<String, HashMap<String,IndexInfo>> rebuildMap = new HashMap<>();
    private HashMap<String, String> prop = new  HashMap<>();

    private boolean dryRun, upgrade, syncRebuild;
    private String operation;
    private String inputTables;
    private String logFile;
    private String inputFile;

    private boolean test = false;

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
    public boolean getDryRun() {
        return this.dryRun;
    }

    public String getInputTables() {
        return this.inputTables;
    }

    public String getLogFile() {
        return this.logFile;
    }

    public String getOperation() {
        return operation;
    }

    public IndexUpgradeTool(String mode, String tables, String inputFile,
            String outputFile, boolean dryRun, IndexTool indexTool) {
        this.operation = mode;
        this.inputTables = tables;
        this.inputFile = inputFile;
        this.logFile = outputFile;
        this.dryRun = dryRun;
        this.indexingTool = indexTool;
    }

    public IndexUpgradeTool () { }

    @Override
    public int run(String[] args) throws Exception {
        CommandLine cmdLine = null;
        try {
            cmdLine = parseOptions(args);
            LOGGER.info("Index Upgrade tool initiated: " + StringUtils.join( args, ","));
        } catch (IllegalStateException e) {
            printHelpAndExit(e.getMessage(), getOptions());
        }
        initializeTool(cmdLine);
        prepareToolSetup();
        executeTool();
        return 0;
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

        CommandLineParser parser = new PosixParser();
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
        INDEX_SYNC_REBUILD_OPTION.setOptionalArg(true);
        options.addOption(INDEX_SYNC_REBUILD_OPTION);

        return options;
    }

    @VisibleForTesting
    public void initializeTool(CommandLine cmdLine) {
        operation = cmdLine.getOptionValue(OPERATION_OPTION.getOpt());
        inputTables = cmdLine.getOptionValue(TABLE_OPTION.getOpt());
        logFile = cmdLine.getOptionValue(LOG_FILE_OPTION.getOpt());
        inputFile = cmdLine.getOptionValue(TABLE_CSV_FILE_OPTION.getOpt());
        dryRun = cmdLine.hasOption(DRY_RUN_OPTION.getOpt());
        syncRebuild = cmdLine.hasOption(INDEX_SYNC_REBUILD_OPTION.getOpt());
    }

    @VisibleForTesting
    public void prepareToolSetup() {
        try {
            if (logFile != null) {
                FileHandler fh = new FileHandler(logFile);
                fh.setFormatter(new SimpleFormatter());
                LOGGER.addHandler(fh);
            }

            prop.put(Indexer.INDEX_BUILDER_CONF_KEY, PhoenixIndexBuilder.class.getName());
            prop.put(NonTxIndexBuilder.CODEC_CLASS_NAME_KEY, PhoenixIndexCodec.class.getName());

            if (inputTables == null) {
                inputTables = new String(Files.readAllBytes(Paths.get(inputFile)));
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
            LOGGER.severe("Something went wrong "+e);
            System.exit(-1);
        }
    }

    @VisibleForTesting
    public int executeTool() {
        Configuration conf = HBaseConfiguration.addHbaseResources(getConf());

        try (Connection conn = ConnectionUtil.getInputConnection(conf)) {

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

    private int executeTool(Connection conn, ConnectionQueryServices queryServices,
            Configuration conf) {

        LOGGER.info("Executing " + operation);
        for (Map.Entry<String, HashSet<String>> entry :tablesAndIndexes.entrySet()) {
            String dataTableFullName = entry.getKey();
            HashSet<String> indexes = entry.getValue();

            try (Admin admin = queryServices.getAdmin()) {

                PTable dataTable = PhoenixRuntime.getTableNoCache(conn, dataTableFullName);
                LOGGER.info("Executing " + operation + " for " + dataTableFullName);

                boolean mutable = !(dataTable.isImmutableRows());
                if (!mutable) {
                    LOGGER.info("Data table is immutable, waiting for "
                            + (GLOBAL_INDEX_CHECKER_ENABLED_MAP_EXPIRATION_MIN + 1)
                            + " minutes for client cache to expire");
                    if (!test) {
                        Thread.sleep(
                                (GLOBAL_INDEX_CHECKER_ENABLED_MAP_EXPIRATION_MIN + 1) * 60 * 1000);
                    }
                }
                disableTable(admin, dataTableFullName, indexes);
                modifyTable(admin, dataTableFullName, indexes);
                enableTable(admin, dataTableFullName, indexes);
                rebuildIndexes(conn, conf, dataTableFullName);
                LOGGER.info("Completed " + operation + " for " + dataTableFullName);
            } catch (IOException | SQLException | InterruptedException e) {
                LOGGER.severe("Something went wrong while executing " + operation
                        + " for " + dataTableFullName + " steps " + e);
                return -1;
            }
        }
        return 0;
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
        } else {
            modifyDataTable(admin, dataTableFullName);
            modifyIndexTable(admin, indexes);
        }
    }

    private void enableTable(Admin admin, String dataTable, HashSet<String>indexes)
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

    private void rebuildIndexes(Connection conn, Configuration conf, String dataTableFullName) {
        if (upgrade) {
            prepareToRebuildIndexes(conn, dataTableFullName);
            if(!test) {
                indexingTool = new IndexTool();
            }
            indexingTool.setConf(conf);
            rebuildIndexes(dataTableFullName, indexingTool);
        }
    }

    private void modifyDataTable(Admin admin, String tableName)
            throws IOException {
        HTableDescriptor tableDesc = admin.getTableDescriptor(TableName.valueOf(tableName));
        if (upgrade) {
            removeCoprocessor(admin, tableName, tableDesc, Indexer.class.getName());
            addCoprocessor(admin, tableName,  tableDesc, IndexRegionObserver.class.getName());
        } else {
            removeCoprocessor(admin, tableName,  tableDesc, IndexRegionObserver.class.getName());
            addCoprocessor(admin, tableName,  tableDesc, Indexer.class.getName());
        }
        if (!dryRun) {
            admin.modifyTable(TableName.valueOf(tableName), tableDesc);
        }
    }

    private void addCoprocessor(Admin admin, String tableName, HTableDescriptor tableDesc,
            String coprocName) throws IOException {
        if (!admin.getTableDescriptor(TableName.valueOf(tableName)).hasCoprocessor(coprocName)) {
            if (!dryRun) {
                tableDesc.addCoprocessor(coprocName,
                        null, QueryServicesOptions.DEFAULT_COPROCESSOR_PRIORITY, prop);
            }
            LOGGER.info("Loaded " + coprocName+ " coprocessor on table " + tableName);
        } else {
            LOGGER.info(coprocName + " coprocessor on table " + tableName + " is already loaded");
        }
    }

    private void removeCoprocessor(Admin admin, String tableName, HTableDescriptor tableDesc,
            String coprocName) throws IOException {
        if (admin.getTableDescriptor(TableName.valueOf(tableName)).hasCoprocessor(coprocName)) {
            if (!dryRun) {
                tableDesc.removeCoprocessor(coprocName);
            }
            LOGGER.info("Unloaded "+ coprocName +"coprocessor on table " + tableName);
        } else {
            LOGGER.info(coprocName + " coprocessor on table " + tableName
                    + " is already unloaded");
        }
    }

    private void modifyIndexTable(Admin admin, HashSet<String> indexes)
            throws IOException {
        for (String indexName : indexes) {
            HTableDescriptor indexTableDesc = admin.getTableDescriptor(TableName.valueOf(indexName));
            if (upgrade) {
                addCoprocessor(admin, indexName, indexTableDesc, GlobalIndexChecker.class.getName());
            } else {
                removeCoprocessor(admin, indexName, indexTableDesc, GlobalIndexChecker.class.getName());
            }
            if (!dryRun) {
                admin.modifyTable(TableName.valueOf(indexName),indexTableDesc);
            }
        }
    }

    private int rebuildIndexes(String dataTable, IndexTool indexingTool) {
        for(Map.Entry<String, IndexInfo> indexMap : rebuildMap.get(dataTable).entrySet()) {
            String index = indexMap.getKey();
            IndexInfo indexInfo = indexMap.getValue();
            String indexName = SchemaUtil.getTableNameFromFullName(index);
            String tenantId = indexInfo.getTenantId();
            String baseTable = indexInfo.getBaseTable();
            String schema = indexInfo.getSchemaName();
            String outFile = "/tmp/index_rebuild_" +schema+"_"+ indexName +
                    (GLOBAL_INDEX_ID.equals(tenantId) ? "" : "_" + tenantId)
                    +"_"+ UUID.randomUUID().toString();
            String[] args = getIndexToolArgValues(schema, baseTable, indexName, outFile, tenantId);

            try {
                LOGGER.info("Rebuilding index: " + StringUtils.join( args,","));
                if (!dryRun) {
                    indexingTool.run(args);
                }
            } catch (Exception e) {
                LOGGER.severe("Something went wrong while building the index " + index + " " + e);
                return -1;
            }
        }
        return 0;
    }

    private String[] getIndexToolArgValues(String schema, String baseTable, String indexName,
            String outFile, String tenantId) {
        String args[] = { "-s", schema, "-dt", baseTable, "-it", indexName,
                "-direct", "-op", outFile };
        ArrayList<String> list = new ArrayList<>(Arrays.asList(args));
        if (!GLOBAL_INDEX_ID.equals(tenantId)) {
            list.add("-tenant");
            list.add(tenantId);
        }
        if (syncRebuild) {
            list.add("-runfg");
        }
        return list.toArray(new String[list.size()]);
    }

    private boolean extractTablesAndIndexes(PhoenixConnection conn) {
        String [] tables = inputTables.trim().split(",");
        PTable dataTable = null;
        try {
            for (String tableName : tables) {
                HashSet<String> physicalIndexes = new HashSet<>();
                dataTable = PhoenixRuntime.getTableNoCache(conn, tableName);
                String physicalTableName = dataTable.getPhysicalName().getString();
                if (!dataTable.isTransactional() && dataTable.getType().equals(PTableType.TABLE)) {
                    for (PTable indexTable : dataTable.getIndexes()) {
                        if (indexTable.getIndexType().equals(PTable.IndexType.GLOBAL)) {
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

    private void prepareToRebuildIndexes(Connection conn, String dataTableFullName) {
        try {
            Gson gson = new Gson();
            HashMap<String, IndexInfo> rebuildIndexes = new HashMap<>();

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
                IndexInfo indexInfo = new IndexInfo(schemaName, tableName,
                        GLOBAL_INDEX_ID, pIndexName);
                rebuildIndexes.put(physicalIndexName, indexInfo);
            }

            if (hasViewIndex) {

                String viewSql = "SELECT DISTINCT TABLE_NAME, TENANT_ID FROM "
                        + "SYSTEM.CATALOG "
                        + "WHERE COLUMN_FAMILY = \'" + dataTableFullName + "\' "
                        + (!StringUtil.EMPTY_STRING.equals(schemaName) ? "AND TABLE_SCHEM = \'"
                        + schemaName + "\' " : "")
                        + "AND LINK_TYPE = "
                        + PTable.LinkType.PHYSICAL_TABLE.getSerializedValue();

                ResultSet rs = conn.createStatement().executeQuery(viewSql);

                while (rs.next()) {
                    String viewName = rs.getString(1);
                    String tenantId = rs.getString(2);

                    ArrayList<String> viewIndexes = findViewIndexes(conn, schemaName, viewName,
                            tenantId);
                    for (String viewIndex : viewIndexes) {
                        IndexInfo indexInfo = new IndexInfo(schemaName, viewName,
                                        tenantId == null ? GLOBAL_INDEX_ID : tenantId, viewIndex);
                        rebuildIndexes.put(viewIndex, indexInfo);
                    }
                }
            }
            //for rebuilding indexes in case of upgrade and if there are indexes on the table/view.
            if (!rebuildIndexes.isEmpty()) {
                rebuildMap.put(dataTableFullName, rebuildIndexes);
                String json = gson.toJson(rebuildMap);
                LOGGER.info("Index rebuild map " + json);
            } else {
                LOGGER.info("No indexes to rebuild for table " + dataTableFullName);
            }

        } catch (SQLException e) {
            LOGGER.severe("Failed to prepare the map for index rebuilds " + e);
            throw new RuntimeException("Failed to prepare the map for index rebuilds");
        }
    }

    private ArrayList<String> findViewIndexes(Connection conn, String schemaName, String viewName,
            String tenantId) throws SQLException {

        String viewIndexesSql = "SELECT DISTINCT COLUMN_FAMILY FROM "
                + "SYSTEM.CATALOG "
                + "WHERE TABLE_NAME = \'" + viewName + "\'"
                + (!StringUtil.EMPTY_STRING.equals(schemaName) ? "AND TABLE_SCHEM = \'"
                + schemaName + "\' " : "")
                + "AND LINK_TYPE = " + PTable.LinkType.INDEX_TABLE.getSerializedValue()
                + (tenantId != null ? " AND TENANT_ID = \'" + tenantId + "\'" : "");
        ArrayList<String> viewIndexes = new ArrayList<>();
        ResultSet
                rs =
                conn.createStatement().executeQuery(viewIndexesSql);
        while(rs.next()) {
            String viewIndexName = rs.getString(1);
            viewIndexes.add(viewIndexName);
        }
        return viewIndexes;
    }

    private class IndexInfo {
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

        public String getBaseTable() {
            return baseTable;
        }

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