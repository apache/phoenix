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

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_FAMILY;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.LINK_TYPE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CHILD_LINK_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_SCHEM;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_TYPE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TENANT_ID;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.VIEW_TYPE;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

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
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.mapreduce.util.ConnectionUtil;
import org.apache.phoenix.parse.DropTableStatement;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.MetaDataClient;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.PhoenixRuntime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A tool to identify orphan views and links, and drop them
 *
 */
public class OrphanViewTool extends Configured implements Tool {
    private static final Logger LOGGER = LoggerFactory.getLogger(OrphanViewTool.class);
    // Query all the views that are not "MAPPED" views
    private static final String viewQuery = "SELECT " +
            TENANT_ID + ", " +
            TABLE_SCHEM + "," +
            TABLE_NAME +
            " FROM " + SYSTEM_CATALOG_NAME +
            " WHERE "+ TABLE_TYPE + " = '" + PTableType.VIEW.getSerializedValue() +"' AND NOT " +
            VIEW_TYPE + " = " + PTable.ViewType.MAPPED.getSerializedValue();
    // Query all physical links
    private static final String physicalLinkQuery = "SELECT " +
            TENANT_ID + ", " +
            TABLE_SCHEM + ", " +
            TABLE_NAME + ", " +
            COLUMN_NAME + " AS PHYSICAL_TABLE_TENANT_ID, " +
            COLUMN_FAMILY + " AS PHYSICAL_TABLE_FULL_NAME " +
            " FROM " + SYSTEM_CATALOG_NAME +
            " WHERE "+ LINK_TYPE + " = " +
            PTable.LinkType.PHYSICAL_TABLE.getSerializedValue();
    // Query all child-parent links
    private static final String childParentLinkQuery = "SELECT " +
            TENANT_ID + ", " +
            TABLE_SCHEM + ", " +
            TABLE_NAME + ", " +
            COLUMN_NAME + " AS PARENT_VIEW_TENANT_ID, " +
            COLUMN_FAMILY + " AS PARENT_VIEW_FULL_NAME " +
            " FROM " + SYSTEM_CATALOG_NAME +
            " WHERE "+ LINK_TYPE + " = " +
            PTable.LinkType.PARENT_TABLE.getSerializedValue();
    // Query all parent-child links
    private static final String parentChildLinkQuery = "SELECT " +
            TENANT_ID + ", " +
            TABLE_SCHEM + ", " +
            TABLE_NAME + ", " +
            COLUMN_NAME + " AS CHILD_VIEW_TENANT_ID, " +
            COLUMN_FAMILY + " AS CHILD_VIEW_FULL_NAME " +
            " FROM " + SYSTEM_CHILD_LINK_NAME +
            " WHERE "+ LINK_TYPE + " = " +
            PTable.LinkType.CHILD_TABLE.getSerializedValue();

    // Query all the tables that can be a base table
    private static final String candidateBaseTableQuery = "SELECT " +
            TENANT_ID + ", " +
            TABLE_SCHEM + ", " +
            TABLE_NAME +
            " FROM " + SYSTEM_CATALOG_NAME +
            " WHERE " + TABLE_TYPE + " != '" + PTableType.VIEW.getSerializedValue() + "' AND " +
            TABLE_TYPE + " != '" + PTableType.INDEX.getSerializedValue() + "'";
    // The path of the directory of the output files
    private String outputPath;
    // The path of the directory of the input files
    private String inputPath;
    // The flag to indicate if the orphan views and links will be deleted
    private boolean clean = false;
    // The maximum level found in a view tree
    private int maxViewLevel = 0;
    // The age of a view
    private static final long defaultAgeMs = 24*60*60*1000; // 1 day
    private long ageMs = 0;

    // A separate file is maintained to list orphan views, and each type of orphan links
    public static final byte VIEW = 0;
    public static final byte PHYSICAL_TABLE_LINK = 1;
    public static final byte PARENT_TABLE_LINK = 2;
    public static final byte CHILD_TABLE_LINK = 3;
    public static final byte ORPHAN_TYPE_COUNT = 4;

    BufferedWriter writer[] = new BufferedWriter[ORPHAN_TYPE_COUNT];
    BufferedReader reader[] = new BufferedReader[ORPHAN_TYPE_COUNT];

    // The set of orphan views
    HashMap<Key, View> orphanViewSet = new HashMap<>();
    // The array list of set of views such that the views in the first set are the first level views and the views
    // in the second set is the second level views, and so on
    List<HashMap<Key, View>> viewSetArray = new ArrayList<HashMap<Key, View>>();
    // The set of base tables
    HashMap<Key, Base> baseSet = new HashMap<>();
    // The set of orphan links. These links can be CHILD_TABLE, PARENT_TABLE, or PHYSICAL_TABLE links
    HashSet<Link> orphanLinkSet = new HashSet<>();

    public static final String fileName[] = {"OrphanView.txt", "OrphanPhysicalTableLink.txt", "OrphanParentTableLink.txt", "OrphanChildTableLink.txt"};
    private static final Option OUTPUT_PATH_OPTION = new Option("op", "output-path", true,
            "Output path where the files listing orphan views and links are written");
    private static final Option INPUT_PATH_OPTION = new Option("ip", "input-path", true,
            "Input path where the files listing orphan views and links are read");
    private static final Option CLEAN_ORPHAN_VIEWS_OPTION = new Option("c", "clean", false,
            "If specified, cleans orphan views and links");
    private static final Option IDENTIFY_ORPHAN_VIEWS_OPTION = new Option("i", "identify", false,
            "If specified, identifies orphan views and links");
    private static final Option AGE_OPTION = new Option("a", "age", true,
            "The minimum age (in milliseconds) for the views (default value is " + Long.toString(defaultAgeMs) + ", i.e. 1 day)");
    private static final Option HELP_OPTION = new Option("h", "help", false, "Help");

    private Options getOptions() {
        final Options options = new Options();
        options.addOption(OUTPUT_PATH_OPTION);
        options.addOption(INPUT_PATH_OPTION);
        options.addOption(CLEAN_ORPHAN_VIEWS_OPTION);
        options.addOption(IDENTIFY_ORPHAN_VIEWS_OPTION);
        options.addOption(AGE_OPTION);
        options.addOption(HELP_OPTION);
        return options;
    }

    /**
     * Parses the commandline arguments, throws IllegalStateException if mandatory arguments are
     * missing.
     * @param args supplied command line arguments
     */
    private void parseOptions(String[] args) throws Exception {

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
        if (cmdLine.hasOption(OUTPUT_PATH_OPTION.getOpt()) && cmdLine.hasOption(INPUT_PATH_OPTION.getOpt())) {
            throw new IllegalStateException("Specify either " + OUTPUT_PATH_OPTION.getLongOpt() + " or "
                    + INPUT_PATH_OPTION.getOpt());
        }
        if (cmdLine.hasOption(INPUT_PATH_OPTION.getOpt()) && !cmdLine.hasOption(CLEAN_ORPHAN_VIEWS_OPTION.getOpt())) {
            throw new IllegalStateException(INPUT_PATH_OPTION.getLongOpt() + " is only used with "
                    + IDENTIFY_ORPHAN_VIEWS_OPTION.getOpt());
        }
        if (cmdLine.hasOption(IDENTIFY_ORPHAN_VIEWS_OPTION.getOpt()) && cmdLine.hasOption(CLEAN_ORPHAN_VIEWS_OPTION.getOpt())) {
            throw new IllegalStateException("Specify either " + IDENTIFY_ORPHAN_VIEWS_OPTION.getLongOpt() + " or "
                    + IDENTIFY_ORPHAN_VIEWS_OPTION.getOpt());
        }
        if (cmdLine.hasOption(OUTPUT_PATH_OPTION.getOpt()) && (!cmdLine.hasOption(IDENTIFY_ORPHAN_VIEWS_OPTION.getOpt()) &&
                !cmdLine.hasOption(CLEAN_ORPHAN_VIEWS_OPTION.getOpt()))) {
            throw new IllegalStateException(OUTPUT_PATH_OPTION.getLongOpt() + " requires either " +
                    IDENTIFY_ORPHAN_VIEWS_OPTION.getOpt() + " or " + CLEAN_ORPHAN_VIEWS_OPTION.getOpt());
        }
        if (cmdLine.hasOption(CLEAN_ORPHAN_VIEWS_OPTION.getOpt())) {
            clean = true;
        }
        else if (!cmdLine.hasOption(IDENTIFY_ORPHAN_VIEWS_OPTION.getOpt())) {
            throw new IllegalStateException("Specify either " +
                    IDENTIFY_ORPHAN_VIEWS_OPTION.getOpt() + " or " + CLEAN_ORPHAN_VIEWS_OPTION.getOpt());
        }
        if (cmdLine.hasOption(AGE_OPTION.getOpt())) {
            ageMs = Long.parseLong(cmdLine.getOptionValue(AGE_OPTION.getOpt()));
        }

        outputPath = cmdLine.getOptionValue(OUTPUT_PATH_OPTION.getOpt());
        inputPath = cmdLine.getOptionValue(INPUT_PATH_OPTION.getOpt());
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

    /**
     * The key that uniquely identifies a table (i.e., a base table or table view)
     */
    private static class Key {
        private String serializedValue;

        public Key (String tenantId, String schemaName, String tableName) throws IllegalArgumentException {
            if (tableName == null) {
                throw new IllegalArgumentException();
            }
            serializedValue = (tenantId != null ? tenantId + "," : ",") +
                    (schemaName != null ? schemaName + "," : ",") +
                    tableName;
        }

        public Key (String tenantId, String fullTableName) {
            String[] columns = fullTableName.split("\\.");
            String schemaName;
            String tableName;
            if (columns.length == 1) {
                schemaName = null;
                tableName = fullTableName;
            } else {
                schemaName = columns[0];
                tableName = columns[1];
            }
            if (tableName == null || tableName.compareTo("") == 0) {
                throw new IllegalArgumentException();
            }
            serializedValue = (tenantId != null ? tenantId + "," : ",") +
                    (schemaName != null ? schemaName + "," : ",") +
                    tableName;
        }

        public Key (String serializedKey) {
            serializedValue = serializedKey;
            if (this.getTableName() == null || this.getTableName().compareTo("") == 0) {
                throw new IllegalArgumentException();
            }
        }

        public String getTenantId() {
            String[] columns = serializedValue.split(",");
            return columns[0].compareTo("") == 0 ? null : columns[0];
        }

        public String getSchemaName() {
            String[] columns = serializedValue.split(",");
            return columns[1].compareTo("") == 0 ? null : columns[1];
        }

        public String getTableName() {
            String[] columns = serializedValue.split(",");
            return columns[2];
        }

        public String getSerializedValue() {
            return serializedValue;
        }
        @Override
        public int hashCode() {
            return Objects.hash(getSerializedValue());
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (getClass() != obj.getClass())
                return false;
            Key other = (Key) obj;
            if (this.getSerializedValue().compareTo(other.getSerializedValue()) != 0)
                return false;
            return true;
        }
    }

    /**
     * An abstract class to represent a table that can be a base table or table view
     */
    private static abstract class Table {
        protected Key key;
        protected List<Key> childViews;

        public void addChild(Key childView) {
            if (childViews == null) {
                childViews = new LinkedList<>();
            }
            childViews.add(childView);
        }

        public boolean isParent() {
            if (childViews == null || childViews.isEmpty()) {
                return false;
            }
            return true;
        }
    }

    /**
     * A class to represents a base table
     */
    private static class Base extends Table {
        public Base (Key key) {
            this.key = key;
        }
    }

    /**
     * A class to represents a table view
     */
    private static class View extends Table {
        Key parent;
        Key base;

        public View (Key key) {
            this.key = key;
        }

        public void setParent(Key parent) {
            this.parent = parent;
        }

        public void setBase(Key base) {
            this.base = base;
        }
    }

    private static class Link {
        Key src;
        Key dst;
        PTable.LinkType type;

        public Link(Key src, Key dst, PTable.LinkType type) {
            this.src = src;
            this.dst = dst;
            this.type = type;
        }

        public String serialize() {
            return src.getSerializedValue() + "," + dst.getSerializedValue() + "," + type.toString();
        }

        @Override
        public int hashCode() {
            return Objects.hash(serialize());
        }
    }

    private void gracefullyDropView(PhoenixConnection phoenixConnection,
            Configuration configuration, Key key) throws Exception {
        PhoenixConnection tenantConnection = null;
        boolean newConn = false;
        try {
            if (key.getTenantId() != null) {
                Properties tenantProps = new Properties();
                tenantProps.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, key.getTenantId());
                tenantConnection = ConnectionUtil.getInputConnection(configuration, tenantProps).
                        unwrap(PhoenixConnection.class);
                newConn = true;
            } else {
                tenantConnection = phoenixConnection;
            }

            MetaDataClient client = new MetaDataClient(tenantConnection);
            org.apache.phoenix.parse.TableName pTableName = org.apache.phoenix.parse.TableName
                    .create(key.getSchemaName(), key.getTableName());
            try {
                client.dropTable(
                        new DropTableStatement(pTableName, PTableType.VIEW, false, true, true));
            }
            catch (TableNotFoundException e) {
                LOGGER.info("Ignoring view " + pTableName + " as it has already been dropped");
            }
        } finally {
            if (newConn) {
                tryClosingConnection(tenantConnection);
            }
        }
    }

    private void removeLink(PhoenixConnection phoenixConnection, Key src, Key dst, PTable.LinkType linkType) throws Exception {
        String deleteQuery = "DELETE FROM " +
                ((linkType == PTable.LinkType.PHYSICAL_TABLE || linkType == PTable.LinkType.PARENT_TABLE) ? SYSTEM_CATALOG_NAME : SYSTEM_CHILD_LINK_NAME) +
                " WHERE " + TENANT_ID + (src.getTenantId() == null ? " IS NULL" : " = '" + src.getTenantId() + "'") + " AND " +
                TABLE_SCHEM + (src.getSchemaName() == null ? " IS NULL " : " = '" + src.getSchemaName() + "'") + " AND " +
                TABLE_NAME + " = '" + src.getTableName() + "' AND " +
                COLUMN_NAME + (dst.getTenantId() == null ? " IS NULL" : " = '" + dst.getTenantId() + "'") + " AND " +
                COLUMN_FAMILY + " = '" + (dst.getSchemaName() == null ? dst.getTableName() : dst.getSchemaName() + "." +
                dst.getTableName()) + "'";
        phoenixConnection.createStatement().execute(deleteQuery);
        phoenixConnection.commit();
    }

    private byte getLinkType(PTable.LinkType linkType) {
        byte type;
        if (linkType == PTable.LinkType.PHYSICAL_TABLE) {
            type = PHYSICAL_TABLE_LINK;
        }
        else if (linkType == PTable.LinkType.PARENT_TABLE) {
            type = PARENT_TABLE_LINK;
        } else if (linkType == PTable.LinkType.CHILD_TABLE) {
            type = CHILD_TABLE_LINK;
        }
        else {
            throw new AssertionError("Unknown Link Type");
        }
        return type;
    }

    private PTable.LinkType getLinkType(byte linkType) {
        PTable.LinkType type;
        if (linkType == PHYSICAL_TABLE_LINK) {
            type = PTable.LinkType.PHYSICAL_TABLE;
        }
        else if (linkType == PARENT_TABLE_LINK) {
            type = PTable.LinkType.PARENT_TABLE;
        } else if (linkType == CHILD_TABLE_LINK) {
            type = PTable.LinkType.CHILD_TABLE;
        }
        else {
            throw new AssertionError("Unknown Link Type");
        }
        return type;
    }

    private void removeOrLogOrphanLinks(PhoenixConnection phoenixConnection) {
        for (Link link : orphanLinkSet) {
            try {
                byte linkType = getLinkType(link.type);
                if (outputPath != null) {
                    writer[linkType].write(link.src.getSerializedValue() + "-->" + link.dst.getSerializedValue());
                    writer[linkType].newLine();
                }
                else if (!clean){
                    System.out.println(link.src.getSerializedValue() + "-(" + link.type + ")->" + link.dst.getSerializedValue());
                }
                if (clean) {
                    removeLink(phoenixConnection, link.src, link.dst, link.type);
                }
            } catch (Exception e) {
                // ignore
            }
        }
    }
    private void forcefullyDropView(PhoenixConnection phoenixConnection,
                                    Key key) throws Exception {
        String deleteRowsFromCatalog = "DELETE FROM " + SYSTEM_CATALOG_NAME +
                " WHERE " + TENANT_ID + (key.getTenantId() == null ? " IS NULL" : " = '" + key.getTenantId() + "'") + " AND " +
                TABLE_SCHEM + (key.getSchemaName() == null ? " IS NULL " : " = '" + key.getSchemaName() + "'") + " AND " +
                TABLE_NAME + " = '" + key.getTableName() + "'";
        String deleteRowsFromChildLink = "DELETE FROM " + SYSTEM_CHILD_LINK_NAME +
                " WHERE " + COLUMN_NAME + (key.getTenantId() == null ? " IS NULL" : " = '" + key.getTenantId() + "'") + " AND " +
                COLUMN_FAMILY + " = '" + (key.getSchemaName() == null ? key.getTableName() : key.getSchemaName() + "." + key.getTableName()) + "'";
        try {
            phoenixConnection.createStatement().execute(deleteRowsFromCatalog);
            phoenixConnection.createStatement().execute(deleteRowsFromChildLink);
            phoenixConnection.commit();
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    private void dropOrLogOrphanViews(PhoenixConnection phoenixConnection, Configuration configuration,
                          Key key) throws Exception {
        if (outputPath != null) {
            writer[VIEW].write(key.getSerializedValue());
            writer[VIEW].newLine();
        }
        else if (!clean) {
            System.out.println(key.getSerializedValue());
            return;
        }
        if (!clean) {
            return;
        }
        gracefullyDropView(phoenixConnection, configuration, key);
    }

    /**
     * Go through all the views in the system catalog table and add them to orphanViewSet
     * @param phoenixConnection
     * @throws Exception
     */
    private void populateOrphanViewSet(PhoenixConnection phoenixConnection)
            throws Exception {
        ResultSet viewRS = phoenixConnection.createStatement().executeQuery(viewQuery);
        while (viewRS.next()) {
            String tenantId = viewRS.getString(1);
            String schemaName = viewRS.getString(2);
            String tableName = viewRS.getString(3);
            Key key = new Key(tenantId, schemaName, tableName);
            View view = new View(key);
            orphanViewSet.put(key, view);
        }
    }

    /**
     * Go through all the tables in the system catalog table and update baseSet
     * @param phoenixConnection
     * @throws Exception
     */
    private void populateBaseSet(PhoenixConnection phoenixConnection)
            throws Exception {
        ResultSet baseTableRS = phoenixConnection.createStatement().executeQuery(candidateBaseTableQuery);
        while (baseTableRS.next()) {
            String tenantId = baseTableRS.getString(1);
            String schemaName = baseTableRS.getString(2);
            String tableName = baseTableRS.getString(3);
            Key key = new Key(tenantId, schemaName, tableName);
            Base base = new Base(key);
            baseSet.put(key, base);
        }
    }

    /**
     * Go through all the physical links in the system catalog table and update the base table info of the
     * view objects in orphanViewSet. If the base or view object does not exist for a given link, then add the link
     * to orphanLinkSet
     * @param phoenixConnection
     * @throws Exception
     */
    private void processPhysicalLinks(PhoenixConnection phoenixConnection)
            throws Exception {
        ResultSet physicalLinkRS = phoenixConnection.createStatement().executeQuery(physicalLinkQuery);
        while (physicalLinkRS.next()) {
            String tenantId = physicalLinkRS.getString(1);
            String schemaName = physicalLinkRS.getString(2);
            String tableName = physicalLinkRS.getString(3);
            Key viewKey = new Key(tenantId, schemaName, tableName);
            View view = orphanViewSet.get(viewKey);

            String baseTenantId = physicalLinkRS.getString(4);
            String baseFullTableName = physicalLinkRS.getString(5);
            Key baseKey = new Key(baseTenantId, baseFullTableName);
            Base base = baseSet.get(baseKey);

            if (view == null || base == null) {
                orphanLinkSet.add(new Link(viewKey, baseKey, PTable.LinkType.PHYSICAL_TABLE));
            }
            else {
                view.setBase(baseKey);
            }
        }
    }

    /**
     * Go through all the child-parent links and update the parent field of the view objects of orphanViewSet.
     * Check if the child does not exist add the link to orphanLinkSet.
     * @param phoenixConnection
     * @throws Exception
     */
    private void processChildParentLinks(PhoenixConnection phoenixConnection)
            throws Exception {
        ResultSet childParentLinkRS = phoenixConnection.createStatement().executeQuery(childParentLinkQuery);
        while (childParentLinkRS.next()) {
            String childTenantId = childParentLinkRS.getString(1);
            String childSchemaName = childParentLinkRS.getString(2);
            String childTableName = childParentLinkRS.getString(3);
            Key childKey = new Key(childTenantId, childSchemaName, childTableName);
            View childView = orphanViewSet.get(childKey);

            String parentTenantId = childParentLinkRS.getString(4);
            String parentFullTableName = childParentLinkRS.getString(5);
            Key parentKey = new Key(parentTenantId, parentFullTableName);
            View parentView = orphanViewSet.get(parentKey);

            // Check if parentTenantId is not set but it should have been the same as the childTenantId. Is this a bug?
            if (childView != null && parentView == null && parentTenantId == null && childTenantId != null) {
                Key anotherParentKey = new Key(childTenantId, parentFullTableName);
                parentView = orphanViewSet.get(anotherParentKey);
                if (parentView != null) {
                    parentKey = anotherParentKey;
                }
            }

            if (childView == null || parentView == null) {
                orphanLinkSet.add(new Link(childKey, parentKey, PTable.LinkType.PARENT_TABLE));
            }
            else {
                childView.setParent(parentKey);
            }
        }
    }

    /**
     * Go through all the parent-child links and update the parent field of the
     * child view objects of orphanViewSet and the child links of the parent objects (which can be a view from
     * orphanViewSet or a base table from baseSet. Check if the child or parent does not exist, and if so, add the link
     * to orphanLinkSet.
     * @param phoenixConnection
     * @throws Exception
     */
    private void processParentChildLinks(PhoenixConnection phoenixConnection)
            throws Exception {
        ResultSet parentChildLinkRS = phoenixConnection.createStatement().executeQuery(parentChildLinkQuery);
        while (parentChildLinkRS.next()) {
            String tenantId = parentChildLinkRS.getString(1);
            String schemaName = parentChildLinkRS.getString(2);
            String tableName = parentChildLinkRS.getString(3);
            Key parentKey = new Key(tenantId, schemaName, tableName);
            Base base = baseSet.get(parentKey);
            View parentView = orphanViewSet.get(parentKey);

            String childTenantId = parentChildLinkRS.getString(4);
            String childFullTableName = parentChildLinkRS.getString(5);
            Key childKey = new Key(childTenantId, childFullTableName);
            View childView = orphanViewSet.get(childKey);

            if (childView == null) {
                // No child for this link
                orphanLinkSet.add(new Link(parentKey, childKey, PTable.LinkType.CHILD_TABLE));
            }
            else if (base != null) {
                base.addChild(childKey);
            }
            else if (parentView != null) {
                parentView.addChild(childKey);
            }
            else {
                // No parent for this link
                orphanLinkSet.add(new Link(parentKey, childKey, PTable.LinkType.CHILD_TABLE));
            }
        }
    }

    private void removeBaseTablesWithNoChildViewFromBaseSet() {
        Iterator<Map.Entry<Key, Base>> iterator = baseSet.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Key, Base> entry = iterator.next();
            if (entry.getValue().childViews == null || entry.getValue().childViews.isEmpty()) {
                iterator.remove();
            }
        }
    }

    /**
     * Starting from the child views of the base tables from baseSet, visit views level by level and identify
     * missing or broken links, and thereby identify orphan vies
     */
    private void visitViewsLevelByLevelAndIdentifyOrphanViews() {
        if (baseSet.isEmpty())
            return;
        HashMap<Key, View> viewSet = new HashMap<>();
        viewSetArray.add(0, viewSet);
        // Remove the child views of the tables of baseSet from orphanViewSet and add them to viewSetArray[0]
        // if these views have the correct physical link
        for (Map.Entry<Key, Base> baseEntry : baseSet.entrySet()) {
            for (Key child : baseEntry.getValue().childViews) {
                View childView = orphanViewSet.get(child);
                if (childView != null &&
                        childView.base != null && childView.base.equals(baseEntry.getKey())) {
                    orphanViewSet.remove(child);
                    viewSet.put(child, childView);
                }
            }
        }
        HashMap<Key, View> parentViewSet = viewSet;
        // Remove the child views of  viewSetArray[N] from orphanViewSet and add them to viewSetArray[N+1]
        // if these view have the correct physical link and parent link
        maxViewLevel = 1;
        for (int i = 1; !parentViewSet.isEmpty(); i++) {
            HashMap<Key, View> childViewSet = new HashMap<>();
            viewSetArray.add(i, childViewSet);
            for (Map.Entry<Key, View> viewEntry : parentViewSet.entrySet()) {
                View parentView = viewEntry.getValue();
                Key parentKey = viewEntry.getKey();
                if (parentView.isParent()) {
                    for (Key child : parentView.childViews) {
                        View childView = orphanViewSet.get(child);
                        if (childView != null &&
                                childView.parent != null && childView.parent.equals(parentKey) &&
                                childView.base != null && childView.base.equals(parentView.base)) {
                            orphanViewSet.remove(child);
                            childViewSet.put(child, childView);
                        }
                    }
                }
            }
            parentViewSet = childViewSet;
            maxViewLevel += 1;
        }
    }

    private void identifyOrphanViews(PhoenixConnection phoenixConnection)
            throws Exception {
        if (inputPath != null) {
            readOrphanViews();
            return;
        }
        // Go through the views and add them to orphanViewSet
        populateOrphanViewSet(phoenixConnection);
        // Go through the candidate base tables and add them to baseSet
        populateBaseSet(phoenixConnection);
        // Go through physical links and update the views of orphanLinkSet
        processPhysicalLinks(phoenixConnection);
        // Go through the parent-child links and update the views of orphanViewSet and the tables of baseSet
        processParentChildLinks(phoenixConnection);
        // Go through index-view links and update the views of orphanLinkSet
        processChildParentLinks(phoenixConnection);

        if (baseSet == null)
            return;
        // Remove the base tables with no child from baseSet
        removeBaseTablesWithNoChildViewFromBaseSet();
        // Starting from the child views of the base tables, visit views level by level and identify
        // missing or broken links and thereby identify orphan vies
        visitViewsLevelByLevelAndIdentifyOrphanViews();
    }

    private void createSnapshot(PhoenixConnection phoenixConnection, long scn)
        throws Exception {
        try (Admin admin = phoenixConnection.getQueryServices().getAdmin()) {
            admin.snapshot("OrphanViewTool." + scn, TableName
                .valueOf(SYSTEM_CATALOG_NAME));
            admin.snapshot("OrphanViewTool." + (scn + 1), TableName
                .valueOf(SYSTEM_CHILD_LINK_NAME));
        }
    }

    private void readOrphanViews() throws Exception {
        String aLine;
        reader[VIEW] = new BufferedReader(new InputStreamReader(
                new FileInputStream(inputPath + fileName[VIEW]), StandardCharsets.UTF_8));
        while ((aLine = reader[VIEW].readLine()) != null) {
            Key key = new Key(aLine);
            orphanViewSet.put(key, new View(key));
        }
    }

    private void readAndRemoveOrphanLinks(PhoenixConnection phoenixConnection) throws Exception{
        String aLine;
        for (byte i = VIEW+1; i < ORPHAN_TYPE_COUNT; i++) {
            reader[i] = new BufferedReader(new InputStreamReader(
                    new FileInputStream(inputPath + fileName[i]), StandardCharsets.UTF_8));
            while ((aLine = reader[i].readLine()) != null) {
                String ends[] = aLine.split("-->");
                removeLink(phoenixConnection, new Key(ends[0]), new Key(ends[1]), getLinkType(i));
            }
        }
    }

    private void closeConnectionAndFiles(Connection connection) throws IOException {
        tryClosingConnection(connection);
        for (byte i = VIEW; i < ORPHAN_TYPE_COUNT; i++) {
            if (writer[i] != null) {
                writer[i].close();
            }
            if (reader[i] != null) {
                reader[i].close();
            }
        }
    }

    /**
     * Try closing a connection if it is not null
     * @param connection connection object
     * @throws RuntimeException if closing the connection fails
     */
    private void tryClosingConnection(Connection connection) {
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException sqlE) {
            LOGGER.error("Failed to close connection: ", sqlE);
            throw new RuntimeException("Failed to close connection with exception: ", sqlE);
        }
    }

    /**
     * Examples for input arguments:
     * -c : cleans orphan views
     * -c -op /tmp/ : cleans orphan views and links, and logs their names to the files named Orphan*.txt in /tmp/
     * -i : identifies orphan views and links, and prints their names on the console
     * -i -op /tmp/ : identifies orphan views and links, and logs the name of their names to files named Orphan*.txt in /tmp/
     * -c -ip /tmp/ : cleans the views listed in files at /tmp/
     */
    @Override
    public int run(String[] args) throws Exception {
        Connection connection = null;
        try {
            final Configuration configuration = HBaseConfiguration.addHbaseResources(getConf());

            try {
                parseOptions(args);
            } catch (IllegalStateException e) {
                printHelpAndExit(e.getMessage(), getOptions());
            }
            if (outputPath != null) {
                // Create files to log orphan views and links
                for (int i = VIEW; i < ORPHAN_TYPE_COUNT; i++) {
                    File file = new File(outputPath + fileName[i]);
                    if (file.exists()) {
                        file.delete();
                    }
                    file.createNewFile();
                    writer[i] = new BufferedWriter(new OutputStreamWriter(
                        new FileOutputStream(file), StandardCharsets.UTF_8));
                }
            }
            Properties props = new Properties();
            long scn = EnvironmentEdgeManager.currentTimeMillis() - ageMs;
            props.setProperty("CurrentSCN", Long.toString(scn));
            connection = ConnectionUtil.getInputConnection(configuration, props);
            PhoenixConnection phoenixConnection = connection.unwrap(PhoenixConnection.class);
            identifyOrphanViews(phoenixConnection);
            if (clean) {
                // Close the connection with SCN
                phoenixConnection.close();
                connection = ConnectionUtil.getInputConnection(configuration);
                phoenixConnection = connection.unwrap(PhoenixConnection.class);
                // Take a snapshot of system tables to be modified
                createSnapshot(phoenixConnection, scn);
            }
            for (Map.Entry<Key, View> entry : orphanViewSet.entrySet()) {
                try {
                    dropOrLogOrphanViews(phoenixConnection, configuration, entry.getKey());
                } catch (Exception e) {
                    // Ignore
                }
            };
            if (clean) {
                // Wait for the view drop tasks in the SYSTEM.TASK table to be processed
                long timeInterval = configuration.getLong(QueryServices.TASK_HANDLING_INTERVAL_MS_ATTRIB,
                        QueryServicesOptions.DEFAULT_TASK_HANDLING_INTERVAL_MS);
                Thread.sleep(maxViewLevel * timeInterval);
                // Clean up any remaining orphan view records from system tables
                for (Map.Entry<Key, View> entry : orphanViewSet.entrySet()) {
                    try {
                        forcefullyDropView(phoenixConnection, entry.getKey());
                    } catch (Exception e) {
                        // Ignore
                    }
                };
            }
            if (inputPath == null) {
                removeOrLogOrphanLinks(phoenixConnection);
            }
            else {
                readAndRemoveOrphanLinks(phoenixConnection);
            }
            return 0;
        } catch (Exception ex) {
            LOGGER.error("Orphan View Tool : An exception occurred "
                    + ExceptionUtils.getMessage(ex) + " at:\n" +
                    ExceptionUtils.getStackTrace(ex));
            return -1;
        } finally {
            closeConnectionAndFiles(connection);
        }
    }

    public static void main(final String[] args) throws Exception {
        int result = ToolRunner.run(new OrphanViewTool(), args);
        System.exit(result);
    }
}
