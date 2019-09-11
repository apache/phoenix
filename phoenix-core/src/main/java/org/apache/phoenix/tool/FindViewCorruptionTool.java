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
package org.apache.phoenix.tool;

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_SCHEM;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_TYPE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TENANT_ID;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_COUNT;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_NAME;


import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.mapreduce.util.ConnectionUtil;
import org.apache.phoenix.schema.PTableType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * A tool to identify corrupted views.
 */
public class FindViewCorruptionTool extends Configured implements Tool {
    private static final Logger LOGGER = LoggerFactory.getLogger(FindViewCorruptionTool.class);

    private static final String VIEW_QUERY = "SELECT " +
            TENANT_ID + ", " +
            TABLE_SCHEM + "," +
            TABLE_NAME + "," +
            COLUMN_COUNT +
            " FROM " + SYSTEM_CATALOG_NAME +
            " WHERE "+ TABLE_TYPE + " = '" + PTableType.VIEW.getSerializedValue() + "'";

    private static final String COLUMN_COUNT_QUERY = "SELECT COUNT(*) FROM " +
            SYSTEM_CATALOG_NAME + " WHERE " +
            TABLE_NAME + " = '%s' AND " +
            COLUMN_NAME + " IS NOT NULL AND LINK_TYPE IS NULL";

    // The set of views
    List<View> viewSet = new ArrayList<>();
    List<View> corruptedViewSet = new ArrayList<>();
    String outputPath;
    boolean getCorruptedViewCount = false;
    String tenantId;
    String schemaName;
    String tableName;

    public static final String fileName = "FindViewCorruptionTool.txt";

    private static final Option HELP_OPTION = new Option("h", "help", false, "Help");
    private static final Option TENANT_ID_OPTION = new Option("id", "tenant id", true,
            "Running tool for a specific tenant");
    private static final Option TABLE_OPTION = new Option("t", "table name", true,
            "Running tool for a specific table");
    private static final Option SCHEMA_OPTION = new Option("s", "schema name", true,
            "Running tool for a specific schema");
    private static final Option OUTPUT_PATH_OPTION = new Option("op", "output-path", true,
            "Output path where the files listing corrupted views are written");
    private static final Option GET_CORRUPTED_VIEWS_COUNT_OPTION = new Option("c",
            "get corrupted view count", false,
            "If specified, cleans orphan views and links");

    /**
     * Go through all the views in the system catalog table and add them
     * @param phoenixConnection
     * @throws Exception
     */
    private void populateViewSetMetadata(PhoenixConnection phoenixConnection) throws Exception {
        ResultSet viewRS = phoenixConnection.createStatement().executeQuery(getViewQuery());
        while (viewRS.next()) {
            String tenantId = viewRS.getString(1);
            String schemaName = viewRS.getString(2);
            String tableName = viewRS.getString(3);
            String columnCount = viewRS.getString(4);
            View view = new View(tenantId, schemaName, tableName, columnCount);
            viewSet.add(view);
        }
    }

    public void findCorruptedViews(PhoenixConnection phoenixConnection) throws Exception {
        // get all views from syscat
        populateViewSetMetadata(phoenixConnection);

        for (View view : viewSet) {
            ResultSet viewRS = phoenixConnection.createStatement()
                    .executeQuery(getColumnCountQuery(view));
            viewRS.next();
            String columnCount = viewRS.getString(1);
            if (view.getColumnCount() == null || !columnCount.equals(view.getColumnCount())) {
                view.setSelectColumnCount(columnCount);
                corruptedViewSet.add(view);
            }
        }
    }

    private String getViewQuery() {
        String query = VIEW_QUERY;
        if (this.schemaName != null) {
            query += " AND " + TABLE_SCHEM + "='" + this.schemaName + "'";
        }
        if (this.tableName != null) {
            query += " AND " + TABLE_NAME + "='" + this.tableName + "'";
        }
        if (this.tenantId != null) {
            query += " AND " + TENANT_ID + "='" + this.tenantId + "'";
        }

        return query;
    }

    private String getColumnCountQuery(View view) {
        String query = String.format(COLUMN_COUNT_QUERY, view.getTableName());
        if (view.getSchemaName().length() > 0) {
            query += " AND " + TABLE_SCHEM + "='" + view.getSchemaName() + "'";
        } else {
            query += " AND " + TABLE_SCHEM + " IS NULL";
        }

        if (view.getTenantId().length() > 0) {
            query += " AND " + TENANT_ID + "='" + view.getTenantId() + "'";
        } else {
            query += " AND " + TENANT_ID + " IS NULL";
        }
        return query;
    }

    private void parseOptions(String[] args) throws Exception {
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
        if (cmdLine.hasOption(TENANT_ID_OPTION.getOpt())) {
            if (TENANT_ID_OPTION.getArgName() == null) {
                throw new IllegalStateException(TENANT_ID_OPTION.getLongOpt() +
                        " requires an argument.");
            } else {
                this.tenantId = cmdLine.getOptionValue(TENANT_ID_OPTION.getOpt());
            }
        }
        if (cmdLine.hasOption(SCHEMA_OPTION.getOpt())) {
            if (SCHEMA_OPTION.getArgName() == null) {
                throw new IllegalStateException(SCHEMA_OPTION.getLongOpt() +
                        " requires an argument.");
            } else {
                this.schemaName = cmdLine.getOptionValue(SCHEMA_OPTION.getOpt());
            }
        }
        if (cmdLine.hasOption(TABLE_OPTION.getOpt())) {
            if (TABLE_OPTION.getArgName() == null) {
                throw new IllegalStateException(TABLE_OPTION.getLongOpt() +
                        " requires an argument.");
            } else {
                this.tableName = cmdLine.getOptionValue(TABLE_OPTION.getOpt());
            }
        }
        if (cmdLine.hasOption(GET_CORRUPTED_VIEWS_COUNT_OPTION.getOpt())) {
            getCorruptedViewCount = true;
        }
        outputPath = cmdLine.getOptionValue(OUTPUT_PATH_OPTION.getOpt());
    }

    private Options getOptions() {
        final Options options = new Options();
        options.addOption(OUTPUT_PATH_OPTION);
        options.addOption(TENANT_ID_OPTION);
        options.addOption(SCHEMA_OPTION);
        options.addOption(TABLE_OPTION);
        options.addOption(GET_CORRUPTED_VIEWS_COUNT_OPTION);
        options.addOption(HELP_OPTION);
        return options;
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

    private class View {
        private String tenantId;
        private String schemaName;
        private String tableName;
        private String selectColumnCount;
        private String columnCount;

        public View (String tenantId, String schemaName, String tableName, String columnCount) {
            this.schemaName = schemaName == null ? "" : schemaName;
            this.tableName = tableName  == null ? "" : tableName;
            this.tenantId = tenantId  == null ? "" : tenantId;
            this.columnCount = columnCount  == null ? "0" : columnCount;
            this.selectColumnCount = "0";
        }

        public String getTenantId() {
            return this.tenantId;
        }

        public String getSchemaName() {
            return this.schemaName;
        }

        public String getTableName() {
            return this.tableName;
        }

        public String getColumnCount() {
            return this.columnCount;
        }

        public String getSelectColumnCount() {
            return this.selectColumnCount;
        }

        public void setSelectColumnCount(String selectColumnCount) {
            this.selectColumnCount = selectColumnCount == null ? "0" : selectColumnCount;
        }
    }

    private void closeConnection(Connection connection) {
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException e) {
            LOGGER.error("Failed to close connection: ", e);
            throw new RuntimeException("Failed to close connection with exception: ", e);
        }
    }

    /*
     * @return return all corrupted view info in a string format as following:
     * "TenantId,SchemaName,TableName,ColumnCountFromHeadRow,SelectCountColumnNumber"
     */
    public List<String> getCorruptedViews() {
        List<String> corruptedView = new ArrayList<>();
        for (View view : this.corruptedViewSet) {
            corruptedView.add(view.getTenantId() + "," + view.getSchemaName() + "," +
                    view.getTableName() + "," + view.getColumnCount() + "," +
                    view.getSelectColumnCount());
        }
        return corruptedView;
    }

    public void writeCorruptedViews(String outputPath, String fileName) throws IOException {
        FileWriter fw = null;
        Path path = Paths.get(outputPath);
        Path filePath = Paths.get(path.toString(), fileName);

        try {
            fw = new FileWriter(new File(filePath.toString()));
            for (String viewInfo : getCorruptedViews()) {
                fw.write(viewInfo);
                fw.write(System.lineSeparator());
            }
        } finally {
            if (fw != null) {
                fw.close();
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Connection connection = null;
        try {
            Configuration configuration = HBaseConfiguration.addHbaseResources(getConf());
            Properties props = new Properties();
            try {
                parseOptions(args);
            } catch (IllegalStateException e) {
                printHelpAndExit(e.getMessage(), getOptions());
            }
            connection = ConnectionUtil.getInputConnection(configuration, props);
            PhoenixConnection phoenixConnection = connection.unwrap(PhoenixConnection.class);
            findCorruptedViews(phoenixConnection);
            if (outputPath != null) {
                writeCorruptedViews(outputPath, fileName);
            }
        } catch (Exception e) {
            LOGGER.error("Find View Corruption Tool : An exception occurred "
                    + ExceptionUtils.getMessage(e) + " at:\n" +
                    ExceptionUtils.getStackTrace(e));
            return -1;
        } finally {
            closeConnection(connection);
        }
        if (getCorruptedViewCount) {
            return corruptedViewSet.size();
        }
        return 0;
    }

    public static void main(final String[] args) throws Exception {
        int result = ToolRunner.run(new FindViewCorruptionTool(), args);
        System.exit(result);
    }
}
