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
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.*;

/**
 * A tool to identify corrupted views.
 */
public class FindViewCorruptionTool extends Configured implements Tool {
    private static final Logger LOGGER = LoggerFactory.getLogger(FindViewCorruptionTool.class);

    private static final String TABLE_QUERY = "SELECT " +
            TENANT_ID + ", " +
            TABLE_SCHEM + "," +
            TABLE_NAME + "," +
            COLUMN_COUNT +
            " FROM " + SYSTEM_CATALOG_NAME +
            " WHERE "+ TABLE_TYPE + " = '" + PTableType.TABLE.getSerializedValue() + "'";

    private static final String GET_ALL_CHILDREN_QUERY = "SELECT " +
            COLUMN_FAMILY +
            " FROM " + SYSTEM_CHILD_LINK_NAME + " WHERE " +
            TABLE_NAME + " = '%s' AND LINK_TYPE=4";

    private static final String GET_CHILD_INFO_QUERY = "SELECT " +
            TENANT_ID + ", " +
            TABLE_SCHEM + "," +
            TABLE_NAME + "," +
            COLUMN_COUNT +
            " FROM " + SYSTEM_CATALOG_NAME + " WHERE " +
            TABLE_NAME + " = '%s' AND " +
            TABLE_TYPE + " = '" + PTableType.VIEW.getSerializedValue() + "'";

    private static final String COLUMN_COUNT_QUERY = "SELECT COUNT(*) FROM " +
            SYSTEM_CATALOG_NAME + " WHERE " +
            TABLE_NAME + " = '%s' AND " +
            COLUMN_NAME + " IS NOT NULL AND LINK_TYPE IS NULL";

    // The set of tables or views
    List<MetaNode> set = new ArrayList<>();
    List<MetaNode> corruptedViewSet = new ArrayList<>();
    String outputPath;
    boolean getCorruptedViewCount = false;
    String schemaName;

    public static final String fileName = "FindViewCorruptionTool.txt";

    private static final Option HELP_OPTION = new Option("h", "help", false, "Help");
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
    private void populateTableSetMetadata(PhoenixConnection phoenixConnection) throws Exception {
        ResultSet viewRS = phoenixConnection.createStatement().executeQuery(getAddingSchemaQuery(
                String.format(TABLE_QUERY), this.schemaName == null ? "" : this.schemaName));
        while (viewRS.next()) {
            MetaNode node = constructMetaNode(viewRS);
            set.add(node);
        }
    }

    private MetaNode constructMetaNode(ResultSet rs) throws SQLException {
        String tenantId = rs.getString(1);
        String schemaName = rs.getString(2);
        String tableName = rs.getString(3);
        int columnCount = rs.getInt(4);
        return new MetaNode(tenantId, schemaName, tableName, columnCount);
    }

    public void findCorruptedViews(PhoenixConnection phoenixConnection) throws Exception {
        populateTableSetMetadata(phoenixConnection);
        List<MetaNode> nextLevel = new ArrayList<>();
        while (set.size() > 0) {
            for (MetaNode parent : set) {
                ResultSet childLinkInfoRS = phoenixConnection.createStatement().executeQuery(
                        getQuery(String.format(GET_ALL_CHILDREN_QUERY, parent.getTableName()),
                                parent.getSchemaName(), parent.getTenantId()));
                // get all children
                while (childLinkInfoRS.next()) {
                    String schemaName;
                    String tableName;
                    String fullName = childLinkInfoRS.getString(1);
                    String[] columns = fullName.split("\\.");
                    if (columns.length == 1) {
                        schemaName = "";
                        tableName = fullName;
                    } else {
                        schemaName = columns[0];
                        tableName = columns[1];
                    }
                    ResultSet childRS = phoenixConnection.createStatement().executeQuery(
                            getAddingSchemaQuery(
                                    String.format(GET_CHILD_INFO_QUERY, tableName), schemaName));
                    if (!childRS.next()) {
                        continue;
                    }
                    MetaNode child = constructMetaNode(childRS);
                    ResultSet selectColumnCountRS = phoenixConnection.createStatement().executeQuery(
                            getQuery(String.format(COLUMN_COUNT_QUERY, child.getTableName()),
                                    child.getSchemaName(), child.getTenantId()));
                    selectColumnCountRS.next();
                    int selectColumnCount = selectColumnCountRS.getInt(1);
                    if (selectColumnCount + parent.getColumnCount() != child.getColumnCount()) {
                        child.setSelectColumnCount(selectColumnCount);
                        corruptedViewSet.add(child);
                    } else {
                        nextLevel.add(child);
                    }
                }
            }
            set = nextLevel;
            nextLevel = new ArrayList<>();
        }
    }

    private String getAddingSchemaQuery(String query, String schemaName) {
        if (schemaName.length() > 0) {
            query += " AND " + TABLE_SCHEM + "='" + schemaName + "'";
        }

        return query;
    }

    private String getQuery(String query, String schemaName, String tenantId) {
        if (schemaName.length() > 0) {
            query += " AND " + TABLE_SCHEM + "='" + schemaName + "'";
        } else {
            query += " AND " + TABLE_SCHEM + " IS NULL";
        }

        if (tenantId.length() > 0) {
            query += " AND " + TENANT_ID + "='" + tenantId + "'";
        } else {
            query += " AND " + TENANT_ID + " IS NULL";
        }
        return query;
    }

    private void parseOptions(String[] args) {
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
        if (cmdLine.hasOption(SCHEMA_OPTION.getOpt())) {
            if (SCHEMA_OPTION.getArgName() == null) {
                throw new IllegalStateException(SCHEMA_OPTION.getLongOpt() +
                        " requires an argument.");
            } else {
                this.schemaName = cmdLine.getOptionValue(SCHEMA_OPTION.getOpt());
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
        options.addOption(SCHEMA_OPTION);
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

    private class MetaNode {
        private String tenantId;
        private String schemaName;
        private String tableName;
        private int selectColumnCount;
        private int columnCount;

        public MetaNode (String tenantId, String schemaName, String tableName, int columnCount) {
            this.schemaName = schemaName == null ? "" : schemaName;
            this.tableName = tableName  == null ? "" : tableName;
            this.tenantId = tenantId  == null ? "" : tenantId;
            this.columnCount = columnCount;
            this.selectColumnCount = 0;
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

        public int getColumnCount() {
            return this.columnCount;
        }

        public int getSelectColumnCount() {
            return this.selectColumnCount;
        }

        public void setColumnCount(int columnCount) {
            this.selectColumnCount = columnCount;
        }

        public void setSelectColumnCount(int selectColumnCount) {
            this.selectColumnCount = selectColumnCount;
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
        for (MetaNode view : this.corruptedViewSet) {
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