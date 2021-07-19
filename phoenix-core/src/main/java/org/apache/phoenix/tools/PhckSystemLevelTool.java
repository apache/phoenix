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
package org.apache.phoenix.tools;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.mapreduce.util.ConnectionUtil;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.tools.util.PhckRowCounterCheckTool;
import org.apache.phoenix.tools.util.PhckTable;
import org.apache.phoenix.tools.util.PhckUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CATALOG_TABLE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CHILD_LINK_TABLE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_FUNCTION_TABLE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_LOG_TABLE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_MUTEX_TABLE_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_SCHEMA_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_STATS_TABLE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_TASK_TABLE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TYPE_SEQUENCE;
import static org.apache.phoenix.query.QueryServices.DEFAULT_SYSTEM_KEEP_DELETED_CELLS_ATTRIB;
import static org.apache.phoenix.query.QueryServices.DEFAULT_SYSTEM_MAX_VERSIONS_ATTRIB;
import static org.apache.phoenix.query.QueryServices.LOG_SALT_BUCKETS_ATTRIB;
import static org.apache.phoenix.tools.util.PhckSupportedFeatureDocs.SYSTEM_LEVEL_TABLE;


public class PhckSystemLevelTool {

    private static final Logger LOGGER = LoggerFactory.getLogger(PhckSystemLevelTool.class);

    private final String SNAPSHOT_PREFIX = "PHCK_SYSTEM_";
    private HashMap<String,String> allSystemTables = new HashMap<>();
    private List<String> missingTables = new ArrayList<>();
    private List<String> disabledTables = new ArrayList<>();
    private List<String> mismatchedRowCountTables = new ArrayList<>();
    private List<PhckTable> mismatchedPhckTables = new ArrayList<>();

    private Configuration conf;
    private String tableName;

    /***********
     * Options
     ***********/
    private boolean isFixTableNotExistMode = false;
    private boolean isFixTableDisabledMode = false;
    private boolean isFixMismatchedRowCountMode = false;
    private boolean isMonitorMode;

    public void populateSystemTables() throws Exception {
        allSystemTables.put(SYSTEM_CATALOG_TABLE, getSystemDDLProperties(QueryConstants.CREATE_TABLE_METADATA));
        allSystemTables.put(SYSTEM_STATS_TABLE, QueryConstants.CREATE_STATS_TABLE_METADATA);
        allSystemTables.put(TYPE_SEQUENCE, getSystemDDLProperties(QueryConstants.CREATE_SEQUENCE_METADATA));
        allSystemTables.put(SYSTEM_FUNCTION_TABLE, getSystemDDLProperties(QueryConstants.CREATE_FUNCTION_METADATA));
        allSystemTables.put(SYSTEM_LOG_TABLE, getSystemLogTableDDLProperties(QueryConstants.CREATE_LOG_METADATA));
        allSystemTables.put(SYSTEM_CHILD_LINK_TABLE, getSystemDDLProperties(QueryConstants.CREATE_CHILD_LINK_METADATA));
        allSystemTables.put(SYSTEM_MUTEX_TABLE_NAME, getSystemDDLProperties(QueryConstants.CREATE_MUTEX_METADTA));
        allSystemTables.put(SYSTEM_TASK_TABLE, getSystemDDLProperties(QueryConstants.CREATE_TASK_METADATA));
    }

    private String getSystemDDLProperties(String ddl) throws Exception {
        try (Connection connection = ConnectionUtil.getInputConnection(conf);
             PhoenixConnection phoenixConnection = connection.unwrap(PhoenixConnection.class)) {
            ReadOnlyProps props = phoenixConnection.getQueryServices().getProps();
            return String.format(ddl,
                    props.getInt(DEFAULT_SYSTEM_MAX_VERSIONS_ATTRIB,
                            QueryServicesOptions.DEFAULT_SYSTEM_MAX_VERSIONS),
                    props.getBoolean(DEFAULT_SYSTEM_KEEP_DELETED_CELLS_ATTRIB,
                            QueryServicesOptions.DEFAULT_SYSTEM_KEEP_DELETED_CELLS));
        }
    }

    private String getSystemLogTableDDLProperties(String ddl) throws Exception {
        try (Connection connection = ConnectionUtil.getInputConnection(conf);
             PhoenixConnection phoenixConnection = connection.unwrap(PhoenixConnection.class)) {
            ReadOnlyProps props = phoenixConnection.getQueryServices().getProps();
            return String.format(ddl, props.getInt(LOG_SALT_BUCKETS_ATTRIB,
                    QueryServicesOptions.DEFAULT_LOG_SALT_BUCKETS));
        }
    }

    public List<String> getMissingTables() {
        return missingTables;
    }

    public List<String> getDisabledTables() {
        return disabledTables;
    }

    public List<String> getMismatchedRowCountTables() {
        return mismatchedRowCountTables;
    }

    public boolean isMonitorMode() {
        return isMonitorMode;
    }

    public boolean isFixTableNotExistMode() {
        return isFixTableNotExistMode;
    }

    public boolean isFixTableDisabledMode() {
        return isFixTableDisabledMode;
    }

    public boolean isFixMismatchedRowCountMode() {
        return isFixMismatchedRowCountMode;
    }

    public PhckSystemLevelTool() {
    }

    PhckSystemLevelTool(Configuration conf) throws Exception {
        this.conf = conf;
        populateSystemTables();
    }

    public void setMonitorMode(boolean val) {
        this.isMonitorMode = val;
    }

    public void setFixTableNotExistMode(boolean val) {
        this.isFixTableNotExistMode = true;
    }

    public void setFixTableDisabledMode(boolean val) {
        this.isFixTableDisabledMode = val;
    }

    public void setFixMismatchedRowCountMode(boolean val) {
        this.isFixMismatchedRowCountMode = true;
    }

    public void parserParam(String[] commandLine) throws Exception {
        String modeString = commandLine[0];

        switch (modeString) {
            case "-m":
            case "--monitor":
                this.isMonitorMode = true;
                break;
            case "-f":
            case "--fixMissingTable":
                this.isFixTableNotExistMode = true;
                break;
            case "-e":
            case "--enableTable":
                this.isFixTableDisabledMode = true;
                break;
            case "-c":
            case "--fixRowCount":
                this.isFixMismatchedRowCountMode = true;
                break;
            default:
                throw new Exception("Not supported mode exception.");

        }
        commandLine = PhckUtil.purgeFirst(commandLine);
        if (commandLine == null) {
            if (!this.isMonitorMode) {
                throw new Exception("Fix mode should indicate the table name.");
            }
        } else {
            this.tableName = commandLine[0];
        }
    }

    public void run() throws Exception {
        if (allSystemTables.get(tableName) == null || tableName.equals(SYSTEM_CATALOG_TABLE)) {
            LOGGER.error("Fix System level table " + tableName + " is not supported.");
            return;
        }

        this.missingTables = new ArrayList<>();
        this.disabledTables = new ArrayList<>();
        this.mismatchedRowCountTables = new ArrayList<>();
        this.mismatchedPhckTables = new ArrayList<>();

        process();
        printReport();
    }

    private void process() throws Exception {
        try (Connection connection = ConnectionUtil.getInputConnection(conf);
             PhoenixConnection phoenixConnection = connection.unwrap(PhoenixConnection.class)) {
            Admin admin = phoenixConnection.getQueryServices().getAdmin();
            if (this.isMonitorMode && this.tableName == null) {
                // only monitor mode
                checkAllSystemTableAccessibility();
                checkAllMismatchedRowCount();
            } else if (this.isMonitorMode) {
                // monitor mode for a single table
                if (!isTableExisted(this.tableName, admin)) {
                    this.missingTables.add(this.tableName);
                } else if (!isTableDisabled(this.tableName, admin)) {
                    this.disabledTables.add(this.tableName);
                } else if (!isTableRowCountMatches(this.tableName)) {
                    this.mismatchedRowCountTables.add(this.tableName);
                }
            } else if (this.isFixTableNotExistMode()) {
                if (!isTableExisted(this.tableName, admin)) {
                    this.missingTables.add(this.tableName);
                    fixTableNotExist(this.tableName);

                    //post validation
                    if (isTableExisted(this.tableName, admin)) {
                        this.missingTables.remove(this.tableName);
                        LOGGER.info("Successfully create system level table " + this.tableName);
                    } else {
                        LOGGER.info(String.format("Recreate system level table %s failed" , this.tableName));
                    }
                }
            } else if (this.isFixTableDisabledMode()) {
                if (!isTableDisabled(this.tableName, admin)) {
                    this.disabledTables.add(this.tableName);
                    fixDisabledTable(this.tableName);

                    //post validation
                    if (isTableDisabled(this.tableName, admin)) {
                        this.disabledTables.remove(this.tableName);
                        LOGGER.info("Successfully enabled system level table " + this.tableName);
                    } else {
                        LOGGER.info(String.format("Failed to enable system level table %s." , this.tableName));
                    }
                }
            } else if (this.isFixMismatchedRowCountMode()) {
                if (!isTableRowCountMatches(this.tableName)) {
                    this.mismatchedRowCountTables.add(this.tableName);
                    fixMismatchedRowCount(this.tableName);

                    //post validation
                    if (isTableRowCountMatches(this.tableName)) {
                        this.mismatchedRowCountTables.remove(this.tableName);
                        this.mismatchedPhckTables.remove(0);
                        LOGGER.info("Successfully clean/recreate system level table " + this.tableName);
                    } else {
                        LOGGER.info(String.format("Recreate system level table %s failed." , this.tableName));
                    }
                }
            }
        }
    }

    private void printReport() {
        if (this.isMonitorMode) {
            System.out.println(getMonitorReport());
        } else {
            System.out.println(getFixReport());
        }

    }

    public void fixMismatchedRowCount(String tableName) throws Exception {
        cleanPreviousSnapshot(tableName);
        takeSnapshot(tableName);
        cleanPreviousSnapshot(SYSTEM_CATALOG_TABLE);
        takeSnapshot(SYSTEM_CATALOG_TABLE);
        cleanMetaDataFromSyscat(tableName);
        createMissingTable(tableName);
    }

    public void fixDisabledTable(String tableName) throws Exception {
        try (Connection connection = ConnectionUtil.getInputConnection(conf);
             PhoenixConnection phoenixConnection = connection.unwrap(PhoenixConnection.class)) {
            Admin admin = phoenixConnection.getQueryServices().getAdmin();
            if (isTableDisabled(tableName, admin)) {
                admin.enableTable(getHBaseTableName(tableName));
            }
        }
    }

    public void fixTableNotExist(String tableName) throws Exception {
        cleanPreviousSnapshot(SYSTEM_CATALOG_TABLE);
        takeSnapshot(SYSTEM_CATALOG_TABLE);
        cleanMetaDataFromSyscat(tableName);

        if (getMetadataRowCount(tableName) == 0) {
            createMissingTable(tableName);
        } else {
            LOGGER.info("Cannot clean all metadata rows from syscat for table : " + tableName);
            return;
        }
    }

    public void takeSnapshot(String tableName) throws Exception {
        try (Connection connection = ConnectionUtil.getInputConnection(conf);
             PhoenixConnection phoenixConnection = connection.unwrap(PhoenixConnection.class);) {
            Admin admin = phoenixConnection.getQueryServices().getAdmin();
            byte[] snapshotName = getSnapshotName(tableName).getBytes();
            admin.snapshot(snapshotName, getHBaseTableName(tableName));
        }
    }

    public void cleanPreviousSnapshot(String tableName) throws Exception {
        try (Connection connection = ConnectionUtil.getInputConnection(conf);
             PhoenixConnection phoenixConnection = connection.unwrap(PhoenixConnection.class);) {
            Admin admin = phoenixConnection.getQueryServices().getAdmin();
            admin.deleteSnapshots(getSnapshotPrefix(tableName));
        }
    }

    public String getSnapshotName(String tableName) {
        return getSnapshotPrefix(tableName) + "_" + System.currentTimeMillis();
    }

    public String getSnapshotPrefix(String tableName) {
        return SNAPSHOT_PREFIX + tableName;
    }

    public String getMonitorReport() {
        if (!isMonitorMode) {
            return "";
        }

        StringBuilder sb = new StringBuilder();
        sb.append("##### Phck Monitor Report #######\n");
        if (this.missingTables.size() == 0 && this.disabledTables.size() == 0
                && this.mismatchedRowCountTables.size() == 0) {
            sb.append("No system level corruption found!");
            return sb.toString();
        }

        if (this.missingTables.size() > 0) {
            sb.append("\nNumber of missing table is : " + this.missingTables.size() + "\n");
            for (int i =0; i < this.missingTables.size(); i++) {
                sb.append(i + "." + this.missingTables.get(i) + "\n");
            }
            sb.append("Example command to run: " + SYSTEM_LEVEL_TABLE + " -f tableName\n");
        }

        if (this.disabledTables.size() > 0) {
            sb.append("Number of disabled table is : " + this.disabledTables.size() + "\n");
            for (int i =0; i < this.disabledTables.size(); i++) {
                sb.append(i + "." + this.disabledTables.get(i) + "\n");
            }
            sb.append("Example command to run: " + SYSTEM_LEVEL_TABLE + " -e tableName\n");
        }

        if (this.mismatchedRowCountTables.size() > 0) {
            sb.append("\nNumber of mismatched head row column row count table is : "
                    + this.mismatchedRowCountTables.size() + "\n");
            for (int i =0; i < this.mismatchedRowCountTables.size(); i++) {
                sb.append(i + "." + this.mismatchedRowCountTables.get(i) + "\n");
            }
            sb.append("Example command to run: " + SYSTEM_LEVEL_TABLE + " -c tableName\n");
        }

        sb.append("##### Phck End Report #######\n");
        return sb.toString();
    }

    public String getFixReport() {
        StringBuilder sb = new StringBuilder();
        sb.append("##### Phck Fix Report #######\n");
        if (this.missingTables.size() == 0 && this.disabledTables.size() == 0
                && this.mismatchedRowCountTables.size() == 0) {
            sb.append("No system level corruption found!");
            return sb.toString();
        }

        if (this.missingTables.size() > 0) {
            sb.append("Cannot fix missing table for : " + this.tableName + "\n");
        } else if (this.disabledTables.size() > 0) {
            sb.append("Cannot enable table for : " + this.tableName + "\n");
        } else if (this.mismatchedRowCountTables.size() > 0) {
            sb.append("Cannot fix mismatched row count table for : " + this.tableName + "\n");
        }

        sb.append("\n##### Phck End Report #######\n");
        return sb.toString();
    }

    public boolean isTableRowCountMatches(String tableName) throws Exception {
        PhckRowCounterCheckTool tool = new PhckRowCounterCheckTool(conf);
        PhckTable table = tool.getPhckTable(null, SYSTEM_SCHEMA_NAME, tableName);
        boolean isMatched = table.isColumnRowCountMatches();
        if (!isMatched) {
            this.mismatchedPhckTables.add(table);
        }
        return isMatched;
    }

    public void checkAllMismatchedRowCount() throws Exception {
        for (String tableName : allSystemTables.keySet()) {
            if (isTableRowCountMatches(tableName)) {
                mismatchedRowCountTables.add(tableName);
            }
        }
    }

    public void checkAllSystemTableAccessibility() throws Exception {
        try (Connection connection = ConnectionUtil.getInputConnection(conf);
             PhoenixConnection phoenixConnection = connection.unwrap(PhoenixConnection.class)){
            Admin admin = phoenixConnection.getQueryServices().getAdmin();

            for (String tableName : allSystemTables.keySet()) {
                if (!isTableExisted(tableName, admin)) {
                    missingTables.add(tableName);
                } else if (isTableDisabled(tableName, admin)) {
                    disabledTables.add(tableName);
                }
            }
        }
    }

    public String getSelectQuery(String tableName) {
        return String.format("SELECT * FROM SYSTEM.%s LIMIT 1",tableName);
    }

    public boolean isTableQueryable(String tableName) {
        try (Connection conn = ConnectionUtil.getInputConnection(conf)) {
            conn.createStatement().executeQuery(getSelectQuery(tableName));
        } catch (Exception e) {
            return false;
        }

        return true;
    }

    public void createMissingTable(String tableName) throws Exception {
        try (Connection conn = ConnectionUtil.getInputConnection(conf);
             PhoenixConnection phoenixCon = conn.unwrap(PhoenixConnection.class)) {
            String ddl = allSystemTables.get(tableName);
            boolean result = phoenixCon.createStatement().execute(ddl);
            LOGGER.info("DDL has been executed with result : " + result);
            LOGGER.info("DDL statement : " + ddl);
        }
    }

    public int getMetadataRowCount(String tableName) throws Exception {
        int rowCount = -1;
        try (Connection conn = ConnectionUtil.getInputConnection(conf)) {
            String query = String.format("SELECT COUNT(*) FROM SYSTEM.CATALOG WHERE " +
                    "TABLE_SCHEM = 'SYSTEM' AND TABLE_NAME = '%s' ", tableName);
            ResultSet rs = conn.createStatement().executeQuery(query);

            if (rs.next()) {
                rowCount = rs.getInt(1);
                LOGGER.info("Number of rows from syscat count : " + rowCount);
            }
        }

        return rowCount;
    }

    public String getCleanMetadataQuery(String tableName) {
        return String.format(" DELETE FROM SYSTEM.CATALOG WHERE TABLE_SCHEM = 'SYSTEM' " +
                "AND TABLE_NAME = '%s' ", tableName);
    }
    public void cleanMetaDataFromSyscat(String tableName) throws Exception {
        try (Connection conn = ConnectionUtil.getInputConnection(conf)) {
            String query = getCleanMetadataQuery(tableName);
            int rowCount = conn.createStatement().executeUpdate(query);
            conn.commit();
            LOGGER.info("DDL has been executed with deleted rows : " + rowCount);
            LOGGER.info("DDL statement : " + query);
        }
    }

    public boolean isTableExisted(String tableName, Admin admin) throws Exception {
        TableName fullName = getHBaseTableName(tableName);
        return admin.tableExists(fullName);
    }

    public boolean isTableDisabled(String tableName, Admin admin) throws Exception {
        TableName fullName = getHBaseTableName(tableName);
        return admin.isTableDisabled(fullName);
    }

    public TableName getHBaseTableName(String tableName) {
        return TableName.valueOf(SYSTEM_SCHEMA_NAME + "." + tableName);
    }
}
