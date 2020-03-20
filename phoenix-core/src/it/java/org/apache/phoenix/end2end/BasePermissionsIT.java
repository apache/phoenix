/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.end2end;

import com.google.common.base.Joiner;
import com.google.common.base.Throwables;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.security.PrivilegedExceptionAction;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.AuthUtil;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.LocalHBaseCluster;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.security.AccessDeniedException;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.access.AccessControlClient;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.phoenix.coprocessor.MetaDataProtocol;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.NewerSchemaAlreadyExistsException;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runners.MethodSorters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category(NeedsOwnMiniClusterTest.class)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public abstract class BasePermissionsIT extends BaseTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(BasePermissionsIT.class);

    private static String SUPER_USER = System.getProperty("user.name");

    private static HBaseTestingUtility testUtil;
    private static final Set<String> PHOENIX_SYSTEM_TABLES =
            new HashSet<>(Arrays.asList("SYSTEM.CATALOG", "SYSTEM.SEQUENCE", "SYSTEM.STATS",
                "SYSTEM.FUNCTION", "SYSTEM.MUTEX", "SYSTEM.CHILD_LINK"));

    private static final Set<String> PHOENIX_SYSTEM_TABLES_IDENTIFIERS =
            new HashSet<>(Arrays.asList("SYSTEM.\"CATALOG\"", "SYSTEM.\"SEQUENCE\"",
                "SYSTEM.\"STATS\"", "SYSTEM.\"FUNCTION\"", "SYSTEM.\"MUTEX\"", "SYSTEM.\"CHILD_LINK\""));

    private static final String SYSTEM_SEQUENCE_IDENTIFIER =
            QueryConstants.SYSTEM_SCHEMA_NAME + "." + "\"" + PhoenixDatabaseMetaData.SYSTEM_SEQUENCE_TABLE+ "\"";

    private static final String SYSTEM_MUTEX_IDENTIFIER =
            QueryConstants.SYSTEM_SCHEMA_NAME + "." + "\""
                    + PhoenixDatabaseMetaData.SYSTEM_MUTEX_TABLE_NAME + "\"";

    static final Set<String> PHOENIX_NAMESPACE_MAPPED_SYSTEM_TABLES = new HashSet<>(Arrays.asList(
            "SYSTEM:CATALOG", "SYSTEM:SEQUENCE", "SYSTEM:STATS", "SYSTEM:FUNCTION", "SYSTEM:MUTEX", "SYSTEM:CHILD_LINK"));

    // Create Multiple users so that we can use Hadoop UGI to run tasks as various users
    // Permissions can be granted or revoke by superusers and admins only
    // DON'T USE HADOOP UserGroupInformation class to create testing users since HBase misses some of its functionality
    // Instead use org.apache.hadoop.hbase.security.User class for testing purposes.

    // Super User has all the access
    static User superUser1 = null;
    private static User superUser2 = null;

    // Regular users are granted and revoked permissions as needed
    User regularUser1 = null;
    private User regularUser2 = null;
    private User regularUser3 = null;
    private User regularUser4 = null;

    // Group User is equivalent of regular user but inside a group
    // Permissions can be granted to group should affect this user
    static final String GROUP_SYSTEM_ACCESS = "group_system_access";
    private User groupUser = null;

    // Unpriviledged User doesn't have any access and is denied for every action
    User unprivilegedUser = null;

    private static final int NUM_RECORDS = 5;

    boolean isNamespaceMapped;

    private String schemaName;
    private String tableName;
    private String fullTableName;
    private String idx1TableName;
    private String idx2TableName;
    private String idx3TableName;
    private String localIdx1TableName;
    private String view1TableName;
    private String view2TableName;

    BasePermissionsIT(final boolean isNamespaceMapped) throws Exception {
        this.isNamespaceMapped = isNamespaceMapped;
        this.tableName = generateUniqueName();
    }

    static void initCluster(boolean isNamespaceMapped) throws Exception {
        if (null != testUtil) {
            testUtil.shutdownMiniCluster();
            testUtil = null;
        }

        testUtil = new HBaseTestingUtility();

        Configuration config = testUtil.getConfiguration();
        enablePhoenixHBaseAuthorization(config);
        configureNamespacesOnServer(config, isNamespaceMapped);
        config.setBoolean(LocalHBaseCluster.ASSIGN_RANDOM_PORTS, true);

        testUtil.startMiniCluster(1);
        superUser1 = User.createUserForTesting(config, SUPER_USER, new String[0]);
        superUser2 = User.createUserForTesting(config, "superUser2", new String[0]);
    }

    @Before
    public void initUsersAndTables() {
        Configuration configuration = testUtil.getConfiguration();

        regularUser1 = User.createUserForTesting(configuration, "regularUser1_"
                + generateUniqueName(), new String[0]);
        regularUser2 = User.createUserForTesting(configuration, "regularUser2_"
                + generateUniqueName(), new String[0]);
        regularUser3 = User.createUserForTesting(configuration, "regularUser3_"
                + generateUniqueName(), new String[0]);
        regularUser4 = User.createUserForTesting(configuration, "regularUser4_"
                + generateUniqueName(), new String[0]);

        groupUser = User.createUserForTesting(testUtil.getConfiguration(), "groupUser_"
                + generateUniqueName() , new String[] {GROUP_SYSTEM_ACCESS});

        unprivilegedUser = User.createUserForTesting(configuration, "unprivilegedUser_"
                + generateUniqueName(), new String[0]);

        schemaName = generateUniqueName();
        tableName = generateUniqueName();
        fullTableName = schemaName + "." + tableName;
        idx1TableName = tableName + "_IDX1";
        idx2TableName = tableName + "_IDX2";
        idx3TableName = tableName + "_IDX3";
        localIdx1TableName = tableName + "_LIDX1";
        view1TableName = tableName + "_V1";
        view2TableName = tableName + "_V2";
    }

    private static void enablePhoenixHBaseAuthorization(Configuration config) {
        config.set("hbase.superuser", SUPER_USER + "," + "superUser2");
        config.set("hbase.security.authorization", Boolean.TRUE.toString());
        config.set("hbase.security.exec.permission.checks", Boolean.TRUE.toString());
        config.set("hbase.coprocessor.master.classes",
                "org.apache.hadoop.hbase.security.access.AccessController");
        config.set("hbase.coprocessor.region.classes",
                "org.apache.hadoop.hbase.security.access.AccessController");
        config.set("hbase.coprocessor.regionserver.classes",
                "org.apache.hadoop.hbase.security.access.AccessController");

        config.set(QueryServices.PHOENIX_ACLS_ENABLED,"true");

        config.set("hbase.regionserver.wal.codec", "org.apache.hadoop.hbase.regionserver.wal.IndexedWALEditCodec");
    }

    private static void configureNamespacesOnServer(Configuration conf, boolean isNamespaceMapped) {
        conf.set(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, Boolean.toString(isNamespaceMapped));
    }

    public static HBaseTestingUtility getUtility(){
        return testUtil;
    }

    // Utility functions to grant permissions with HBase API
    void grantPermissions(String toUser, Set<String> tablesToGrant, Permission.Action... actions) throws Throwable {
        for (String table : tablesToGrant) {
            AccessControlClient.grant(getUtility().getConnection(), TableName.valueOf(table), toUser, null, null,
                    actions);
        }
    }

    void grantPermissions(String toUser, String namespace, Permission.Action... actions) throws Throwable {
        AccessControlClient.grant(getUtility().getConnection(), namespace, toUser, actions);
    }

    void grantPermissions(String groupEntry, Permission.Action... actions) throws IOException, Throwable {
        AccessControlClient.grant(getUtility().getConnection(), groupEntry, actions);
    }

    // Utility functions to revoke permissions with HBase API
    void revokeAll() throws Throwable {
        AccessControlClient.revoke(getUtility().getConnection(), AuthUtil.toGroupEntry(GROUP_SYSTEM_ACCESS), Permission.Action.values() );
        AccessControlClient.revoke(getUtility().getConnection(), regularUser1.getShortName(), Permission.Action.values() );
        AccessControlClient.revoke(getUtility().getConnection(), unprivilegedUser.getShortName(), Permission.Action.values() );
    }

    private Properties getClientProperties(String tenantId) {
        Properties props = new Properties();
        if(tenantId != null) {
            props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
        }
        props.setProperty(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, Boolean.toString(isNamespaceMapped));
        return props;
    }

    public Connection getConnection() throws SQLException {
        return getConnection(null);
    }

    public Connection getConnection(String tenantId) throws SQLException {
        return DriverManager.getConnection(getUrl(), getClientProperties(tenantId));
    }

    protected static String getUrl() {
        return "jdbc:phoenix:localhost:" + testUtil.getZkCluster().getClientPort() + ":/hbase";
    }

    private static Set<String> getHBaseTables() throws IOException {
        Set<String> tables = new HashSet<>();
        for (TableName tn : testUtil.getHBaseAdmin().listTableNames()) {
            tables.add(tn.getNameAsString());
        }
        return tables;
    }

    // UG Object
    // 1. Instance of String --> represents GROUP name
    // 2. Instance of User --> represents HBase user
    private AccessTestAction grantPermissions(final String actions, final Object ug,
                                      final String tableOrSchemaList, final boolean isSchema) throws SQLException {
        return grantPermissions(actions, ug, Collections.singleton(tableOrSchemaList), isSchema);
    }

    private AccessTestAction grantPermissions(final String actions, final Object ug,
                                      final Set<String> tableOrSchemaList, final boolean isSchema) throws SQLException {
        return new AccessTestAction() {
            @Override
            public Object run() throws Exception {
                try (Connection conn = getConnection(); Statement stmt = conn.createStatement();) {
                    for(String tableOrSchema : tableOrSchemaList) {
                        String grantStmtSQL = "GRANT '" + actions + "' ON " + (isSchema ? " SCHEMA " : " TABLE ") + tableOrSchema + " TO "
                                + ((ug instanceof String) ? (" GROUP " + "'" + ug + "'") : ("'" + ((User)ug).getShortName() + "'"));
                        LOGGER.info("Grant Permissions SQL: " + grantStmtSQL);
                        assertFalse(stmt.execute(grantStmtSQL));
                    }
                }
                return null;
            }
        };
    }

    private AccessTestAction grantPermissions(final String actions, final User user) throws SQLException {
        return new AccessTestAction() {
            @Override
            public Object run() throws Exception {
                try (Connection conn = getConnection(); Statement stmt = conn.createStatement();) {
                    String grantStmtSQL = "GRANT '" + actions + "' TO " + " '" + user.getShortName() + "'";
                    LOGGER.info("Grant Permissions SQL: " + grantStmtSQL);
                    assertFalse(stmt.execute(grantStmtSQL));
                }
                return null;
            }
        };
    }

    private AccessTestAction revokePermissions(final Object ug,
                                       final String tableOrSchemaList, final boolean isSchema) throws SQLException {
        return revokePermissions(ug, Collections.singleton(tableOrSchemaList), isSchema);
    }

    private AccessTestAction revokePermissions(final Object ug,
                                       final Set<String> tableOrSchemaList, final boolean isSchema) throws SQLException {
        return new AccessTestAction() {
            @Override
            public Object run() throws Exception {
                try (Connection conn = getConnection(); Statement stmt = conn.createStatement();) {
                    for(String tableOrSchema : tableOrSchemaList) {
                        String revokeStmtSQL = "REVOKE ON " + (isSchema ? " SCHEMA " : " TABLE ") + tableOrSchema + " FROM "
                                + ((ug instanceof String) ? (" GROUP " + "'" + ug + "'") : ("'" + ((User)ug).getShortName() + "'"));
                        LOGGER.info("Revoke Permissions SQL: " + revokeStmtSQL);
                        assertFalse(stmt.execute(revokeStmtSQL));
                    }
                }
                return null;
            }
        };
    }

    private AccessTestAction revokePermissions(final Object ug) throws SQLException {
        return new AccessTestAction() {
            @Override
            public Object run() throws Exception {
                try (Connection conn = getConnection(); Statement stmt = conn.createStatement();) {
                    String revokeStmtSQL = "REVOKE FROM " +
                            ((ug instanceof String) ? (" GROUP " + "'" + ug + "'") : ("'" + ((User)ug).getShortName() + "'"));
                    LOGGER.info("Revoke Permissions SQL: " + revokeStmtSQL);
                    assertFalse(stmt.execute(revokeStmtSQL));
                }
                return null;
            }
        };
    }

    // Attempts to get a Phoenix Connection
    // New connections could create SYSTEM tables if appropriate perms are granted
    private AccessTestAction getConnectionAction() throws SQLException {
        return new AccessTestAction() {
            @Override
            public Object run() throws Exception {
                try (Connection conn = getConnection();) {
                }
                return null;
            }
        };
    }

    AccessTestAction createSchema(final String schemaName) throws SQLException {
        return new AccessTestAction() {
            @Override
            public Object run() throws Exception {
                if (isNamespaceMapped) {
                    try (Connection conn = getConnection(); Statement stmt = conn.createStatement();) {
                        assertFalse(stmt.execute("CREATE SCHEMA " + schemaName));
                    }
                }
                return null;
            }
        };
    }

    AccessTestAction dropSchema(final String schemaName) throws SQLException {
        return new AccessTestAction() {
            @Override
            public Object run() throws Exception {
                if (isNamespaceMapped) {
                    try (Connection conn = getConnection(); Statement stmt = conn.createStatement();) {
                        assertFalse(stmt.execute("DROP SCHEMA " + schemaName));
                    }
                }
                return null;
            }
        };
    }

    AccessTestAction createTable(final String tableName) throws SQLException {
        return new AccessTestAction() {
            @Override
            public Object run() throws Exception {
                try (Connection conn = getConnection(); Statement stmt = conn.createStatement();) {
                    assertFalse(stmt.execute("CREATE TABLE " + tableName + "(pk INTEGER not null primary key, data VARCHAR, val integer)"));
                    try (PreparedStatement pstmt = conn.prepareStatement("UPSERT INTO " + tableName + " values(?, ?, ?)")) {
                        for (int i = 0; i < NUM_RECORDS; i++) {
                            pstmt.setInt(1, i);
                            pstmt.setString(2, Integer.toString(i));
                            pstmt.setInt(3, i);
                            assertEquals(1, pstmt.executeUpdate());
                        }
                    }
                    conn.commit();
                }
                return null;
            }
        };
    }

    private AccessTestAction createMultiTenantTable(final String tableName) throws SQLException {
        return new AccessTestAction() {
            @Override
            public Object run() throws Exception {
                try (Connection conn = getConnection(); Statement stmt = conn.createStatement();) {
                    assertFalse(stmt.execute("CREATE TABLE " + tableName
                            + "(ORG_ID VARCHAR NOT NULL, PREFIX CHAR(3) NOT NULL, DATA VARCHAR, VAL INTEGER CONSTRAINT PK PRIMARY KEY (ORG_ID, PREFIX))  MULTI_TENANT=TRUE"));
                    try (PreparedStatement pstmt = conn.prepareStatement("UPSERT INTO " + tableName + " values(?, ?, ?, ?)")) {
                        for (int i = 0; i < NUM_RECORDS; i++) {
                            pstmt.setString(1, "o" + i);
                            pstmt.setString(2, "pr" + i);
                            pstmt.setString(3, Integer.toString(i));
                            pstmt.setInt(4, i);
                            assertEquals(1, pstmt.executeUpdate());
                        }
                    }
                    conn.commit();
                }
                return null;
            }
        };
    }

    private AccessTestAction dropTable(final String tableName) throws SQLException {
        return new AccessTestAction() {
            @Override
            public Object run() throws Exception {
                try (Connection conn = getConnection(); Statement stmt = conn.createStatement();) {
                    assertFalse(stmt.execute(String.format("DROP TABLE IF EXISTS %s CASCADE", tableName)));
                }
                return null;
            }
        };

    }

    // Attempts to read given table without verifying data
    // AccessDeniedException is only triggered when ResultSet#next() method is called
    // The first call triggers HBase Scan object
    // The Statement#executeQuery() method returns an iterator and doesn't interact with HBase API at all
    private AccessTestAction readTableWithoutVerification(final String tableName) throws SQLException {
        return new AccessTestAction() {
            @Override
            public Object run() throws Exception {
                try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                    ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName);
                    assertNotNull(rs);
                    while (rs.next()) {
                    }
                }
                return null;
            }
        };
    }

    private AccessTestAction readTable(final String tableName) throws SQLException {
        return readTable(tableName,null);
    }

    private AccessTestAction readTable(final String tableName, final String indexName) throws SQLException {
        return new AccessTestAction() {
            @Override
            public Object run() throws Exception {
                try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                    String readTableSQL = "SELECT "+(indexName!=null?"/*+ INDEX("+tableName+" "+indexName+")*/":"")+" pk, data, val FROM " + tableName +" where data >= '0'";
                    ResultSet rs = stmt.executeQuery(readTableSQL);
                    assertNotNull(rs);
                    int i = 0;
                    while (rs.next()) {
                        assertEquals(i, rs.getInt(1));
                        assertEquals(Integer.toString(i), rs.getString(2));
                        assertEquals(i, rs.getInt(3));
                        i++;
                    }
                    assertEquals(NUM_RECORDS, i);
                }
                return null;
            }
        };
    }

    private AccessTestAction readMultiTenantTableWithoutIndex(final String tableName) throws SQLException {
        return readMultiTenantTableWithoutIndex(tableName, null);
    }

    private AccessTestAction readMultiTenantTableWithoutIndex(final String tableName, final String tenantId) throws SQLException {
        return new AccessTestAction() {
            @Override
            public Object run() throws Exception {
                try (Connection conn = getConnection(tenantId); Statement stmt = conn.createStatement()) {
                    // Accessing all the data from the table avoids the use of index
                    String readTableSQL = "SELECT data, val FROM " + tableName;
                    ResultSet rs = stmt.executeQuery(readTableSQL);
                    assertNotNull(rs);
                    int i = 0;
                    String explainPlan = Joiner.on(" ").join(((PhoenixStatement)stmt).getQueryPlan().getExplainPlan().getPlanSteps());
                    rs = stmt.executeQuery(readTableSQL);
                    if(tenantId != null) {
                        rs.next();
                        assertFalse(explainPlan.contains("_IDX_"));
                        assertEquals(((PhoenixConnection)conn).getTenantId().toString(), tenantId);
                        // For tenant ID "o3", the value in table will be 3
                        assertEquals(Character.toString(tenantId.charAt(1)), rs.getString(1));
                        // Only 1 record is inserted per Tenant
                        assertFalse(rs.next());
                    } else {
                        while(rs.next()) {
                            assertEquals(Integer.toString(i), rs.getString(1));
                            assertEquals(i, rs.getInt(2));
                            i++;
                        }
                        assertEquals(NUM_RECORDS, i);
                    }
                }
                return null;
            }
        };
    }

    private AccessTestAction readMultiTenantTableWithIndex(final String tableName) throws SQLException {
        return readMultiTenantTableWithIndex(tableName, null);
    }

    private AccessTestAction readMultiTenantTableWithIndex(final String tableName, final String tenantId) throws SQLException {
        return new AccessTestAction() {
            @Override
            public Object run() throws Exception {
                try (Connection conn = getConnection(tenantId); Statement stmt = conn.createStatement()) {
                    // Accessing only the 'data' from the table uses index since index tables are built on 'data' column
                    String readTableSQL = "SELECT data FROM " + tableName;
                    ResultSet rs = stmt.executeQuery(readTableSQL);
                    assertNotNull(rs);
                    int i = 0;
                    String explainPlan = Joiner.on(" ").join(((PhoenixStatement) stmt).getQueryPlan().getExplainPlan().getPlanSteps());
                    assertTrue(explainPlan.contains("_IDX_"));
                    rs = stmt.executeQuery(readTableSQL);
                    if (tenantId != null) {
                        rs.next();
                        assertEquals(((PhoenixConnection) conn).getTenantId().toString(), tenantId);
                        // For tenant ID "o3", the value in table will be 3
                        assertEquals(Character.toString(tenantId.charAt(1)), rs.getString(1));
                        // Only 1 record is inserted per Tenant
                        assertFalse(rs.next());
                    } else {
                        while (rs.next()) {
                            assertEquals(Integer.toString(i), rs.getString(1));
                            i++;
                        }
                        assertEquals(NUM_RECORDS, i);
                    }
                }
                return null;
            }
        };
    }

    private AccessTestAction addProperties(final String tableName, final String property, final String value)
            throws SQLException {
        return new AccessTestAction() {
            @Override
            public Object run() throws Exception {
                try (Connection conn = getConnection(); Statement stmt = conn.createStatement();) {
                    assertFalse(stmt.execute("ALTER TABLE " + tableName + " SET " + property + "=" + value));
                }
                return null;
            }
        };
    }

    AccessTestAction addColumn(final String tableName, final String columnName) throws SQLException {
        return new AccessTestAction() {
            @Override
            public Object run() throws Exception {
                try (Connection conn = getConnection(); Statement stmt = conn.createStatement();) {
                    assertFalse(stmt.execute("ALTER TABLE " + tableName + " ADD "+columnName+" varchar"));
                }
                return null;
            }
        };
    }

    private AccessTestAction dropColumn(final String tableName, final String columnName) throws SQLException {
        return new AccessTestAction() {
            @Override
            public Object run() throws Exception {
                try (Connection conn = getConnection(); Statement stmt = conn.createStatement();) {
                    assertFalse(stmt.execute("ALTER TABLE " + tableName + " DROP COLUMN "+columnName));
                }
                return null;
            }
        };
    }

    private AccessTestAction createIndex(final String indexName, final String dataTable) throws SQLException {
        return createIndex(indexName, dataTable, null);
    }

    private AccessTestAction createIndex(final String indexName, final String dataTable, final String tenantId) throws SQLException {
        return new AccessTestAction() {
            @Override
            public Object run() throws Exception {

                try (Connection conn = getConnection(tenantId); Statement stmt = conn.createStatement();) {
                    assertFalse(stmt.execute("CREATE INDEX " + indexName + " on " + dataTable + "(data)"));
                }
                return null;
            }
        };
    }

    private AccessTestAction createLocalIndex(final String indexName, final String dataTable) throws SQLException {
        return new AccessTestAction() {
            @Override
            public Object run() throws Exception {

                try (Connection conn = getConnection(); Statement stmt = conn.createStatement();) {
                    assertFalse(stmt.execute("CREATE LOCAL INDEX " + indexName + " on " + dataTable + "(data)"));
                }
                return null;
            }
        };
    }

    private AccessTestAction dropIndex(final String indexName, final String dataTable) throws SQLException {
        return new AccessTestAction() {
            @Override
            public Object run() throws Exception {
                try (Connection conn = getConnection(); Statement stmt = conn.createStatement();) {
                    assertFalse(stmt.execute("DROP INDEX " + indexName + " on " + dataTable));
                }
                return null;
            }
        };
    }

    private AccessTestAction rebuildIndex(final String indexName, final String dataTable) throws SQLException {
        return new AccessTestAction() {
            @Override
            public Object run() throws Exception {
                try (Connection conn = getConnection(); Statement stmt = conn.createStatement();) {
                    assertFalse(stmt.execute("ALTER INDEX " + indexName + " on " + dataTable + " DISABLE"));
                    assertFalse(stmt.execute("ALTER INDEX " + indexName + " on " + dataTable + " REBUILD"));
                }
                return null;
            }
        };
    }

    private AccessTestAction dropView(final String viewName) throws SQLException {
        return new AccessTestAction() {
            @Override
            public Object run() throws Exception {
                try (Connection conn = getConnection(); Statement stmt = conn.createStatement();) {
                    assertFalse(stmt.execute(String.format("DROP VIEW %s CASCADE", viewName)));
                }
                return null;
            }
        };
    }

    AccessTestAction createView(final String viewName, final String dataTable) throws SQLException {
        return createView(viewName, dataTable, null);
    }

    private AccessTestAction createView(final String viewName, final String dataTable, final String tenantId) throws SQLException {
        return new AccessTestAction() {
            @Override
            public Object run() throws Exception {
                try (Connection conn = getConnection(tenantId); Statement stmt = conn.createStatement();) {
                    String viewStmtSQL = "CREATE VIEW " + viewName + " AS SELECT * FROM " + dataTable;
                    assertFalse(stmt.execute(viewStmtSQL));
                }
                return null;
            }
        };
    }

    interface AccessTestAction extends PrivilegedExceptionAction<Object> { }

    /** This fails only in case of ADE or empty list for any of the users. */
    void verifyAllowed(AccessTestAction action, User... users) throws Exception {
        if(users.length == 0) {
            throw new Exception("Action needs at least one user to run");
        }
        for (User user : users) {
            verifyAllowed(user, action);
        }
    }

    private void verifyAllowed(User user, AccessTestAction... actions) throws Exception {
        for (AccessTestAction action : actions) {
            try {
                Object obj = user.runAs(action);
                if (obj != null && obj instanceof List<?>) {
                    List<?> results = (List<?>) obj;
                    if (results.isEmpty()) {
                        fail("Empty non null results from action for user '" + user.getShortName() + "'");
                    }
                }
            } catch (AccessDeniedException ade) {
                fail("Expected action to pass for user '" + user.getShortName() + "' but was denied");
            }
        }
    }

    /** This passes only if desired exception is caught for all users. */
    <T> void verifyDenied(AccessTestAction action, Class<T> exception, User... users) throws Exception {
        if(users.length == 0) {
            throw new Exception("Action needs at least one user to run");
        }
        for (User user : users) {
            verifyDenied(user, exception, action);
        }
    }

    /** This passes only if desired exception is caught for all users. */
    private <T> void verifyDenied(User user, Class<T> exception, AccessTestAction... actions) throws Exception {
        for (AccessTestAction action : actions) {
            try {
                user.runAs(action);
                fail("Expected exception was not thrown for user '" + user.getShortName() + "'");
            } catch (IOException e) {
                fail("Expected exception was not thrown for user '" + user.getShortName() + "'");
            } catch (UndeclaredThrowableException ute) {
                Throwable ex = ute.getUndeclaredThrowable();

                // HBase AccessDeniedException(ADE) is handled in different ways in different parts of code
                // 1. Wrap HBase ADE in PhoenixIOException (Mostly for create, delete statements)
                // 2. Wrap HBase ADE in ExecutionException (Mostly for scans)
                // 3. Directly throwing HBase ADE or custom msg with HBase ADE
                // Thus we iterate over the chain of throwables and find ADE
                for(Throwable throwable : Throwables.getCausalChain(ex)) {
                    if(exception.equals(throwable.getClass())) {
                        if(throwable instanceof AccessDeniedException) {
                            validateAccessDeniedException((AccessDeniedException) throwable);
                        }
                        return;
                    }
                }

            } catch(RuntimeException ex) {
                // This can occur while accessing tabledescriptors from client by the unprivileged user
                if (ex.getCause() instanceof AccessDeniedException) {
                    // expected result
                    validateAccessDeniedException((AccessDeniedException) ex.getCause());
                    return;
                }
            }
            fail("Expected exception was not thrown for user '" + user.getShortName() + "'");
        }
    }

    private String surroundWithDoubleQuotes(String input) {
        return "\"" + input + "\"";
    }

    private void validateAccessDeniedException(AccessDeniedException ade) {
        String msg = ade.getMessage();
        assertTrue("Exception contained unexpected message: '" + msg + "'",
                !msg.contains("is not the scanner owner"));
    }

    @Test
    public void testSystemTablePermissions() throws Throwable {
        verifyAllowed(createTable(tableName), superUser1);
        verifyAllowed(readTable(tableName), superUser1);

        Set<String> tables = getHBaseTables();
        if(isNamespaceMapped) {
            assertTrue("HBase tables do not include expected Phoenix tables: " + tables,
                    tables.containsAll(PHOENIX_NAMESPACE_MAPPED_SYSTEM_TABLES));
        } else {
            assertTrue("HBase tables do not include expected Phoenix tables: " + tables,
                    tables.containsAll(PHOENIX_SYSTEM_TABLES));
        }

        // Grant permission to the system tables for the unprivileged user
        superUser1.runAs(new PrivilegedExceptionAction<Void>() {
            @Override
            public Void run() throws Exception {
                try {
                    if(isNamespaceMapped) {
                        grantPermissions(regularUser1.getShortName(),
                                PHOENIX_NAMESPACE_MAPPED_SYSTEM_TABLES, Permission.Action.EXEC, Permission.Action.READ);
                    } else {
                        grantPermissions(regularUser1.getShortName(), PHOENIX_SYSTEM_TABLES,
                                Permission.Action.EXEC, Permission.Action.READ);
                    }
                    grantPermissions(regularUser1.getShortName(),
                            Collections.singleton(tableName), Permission.Action.READ,Permission.Action.EXEC);
                } catch (Throwable e) {
                    if (e instanceof Exception) {
                        throw (Exception) e;
                    } else {
                        throw new Exception(e);
                    }
                }
                return null;
            }
        });

        // Make sure that the unprivileged user can now read the table
        verifyAllowed(readTable(tableName), regularUser1);
        //This verification is added to test PHOENIX-5178
        superUser1.runAs(new PrivilegedExceptionAction<Void>() {
            @Override public Void run() throws Exception {
                try {
                    if (isNamespaceMapped) {
                        grantPermissions(regularUser1.getShortName(),"SYSTEM", Permission.Action.ADMIN);
                    }
                    return null;
                } catch (Throwable e) {
                    throw new Exception(e);
                }

            }
        });
        if(isNamespaceMapped) {
            verifyAllowed(new AccessTestAction() {
                @Override public Object run() throws Exception {
                    Properties props = new Properties();
                    props.setProperty(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, Boolean.toString(isNamespaceMapped));
                    props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP));
                    //Impersonate meta connection
                    try (Connection metaConnection = DriverManager.getConnection(getUrl(), props);
                         Statement stmt = metaConnection.createStatement()) {
                        stmt.executeUpdate("CREATE SCHEMA IF NOT EXISTS SYSTEM");
                    }catch(NewerSchemaAlreadyExistsException e){

                    }
                    return null;
                }
            }, regularUser1);
        }
    }

    private void grantSystemTableAccess(User superUser, User... users) throws Exception {
        for(User user : users) {
            if(isNamespaceMapped) {
                verifyAllowed(grantPermissions("RX", user, QueryConstants.SYSTEM_SCHEMA_NAME, true), superUser);
            } else {
                verifyAllowed(grantPermissions("RX", user, PHOENIX_SYSTEM_TABLES_IDENTIFIERS, false), superUser);
            }
            verifyAllowed(grantPermissions("RWX", user, SYSTEM_SEQUENCE_IDENTIFIER, false), superUser);
            verifyAllowed(grantPermissions("RWX", user, SYSTEM_MUTEX_IDENTIFIER, false), superUser);
        }
    }

    private void revokeSystemTableAccess(User superUser, User... users) throws Exception {
        for(User user : users) {
            if(isNamespaceMapped) {
                verifyAllowed(revokePermissions(user, QueryConstants.SYSTEM_SCHEMA_NAME, true), superUser);
            } else {
                verifyAllowed(revokePermissions(user, PHOENIX_SYSTEM_TABLES_IDENTIFIERS, false), superUser);
            }
            verifyAllowed(revokePermissions(user, SYSTEM_SEQUENCE_IDENTIFIER, false), superUser);
            verifyAllowed(revokePermissions(user, SYSTEM_MUTEX_IDENTIFIER, false), superUser);
        }
    }

    /**
     * Verify that READ and EXECUTE permissions are required on SYSTEM tables to get a Phoenix Connection
     * Tests grant revoke permissions per user 1. if NS enabled -> on namespace 2. If NS disabled -> on tables
     */
    @Test
    // this test needs to be run first
    public void aTestRXPermsReqdForPhoenixConn() throws Exception {
        if(isNamespaceMapped) {
            // NS is enabled, CQSI tries creating SYSCAT, we get NamespaceNotFoundException exception for "SYSTEM" NS
            // We create custom ADE and throw it (and ignore NamespaceNotFoundException)
            // This is because we didn't had CREATE perms to create "SYSTEM" NS
            verifyDenied(getConnectionAction(), AccessDeniedException.class, regularUser1);
        } else {
            // NS is disabled, CQSI tries creating SYSCAT, Two cases here
            // 1. First client ever --> Gets ADE, runs client server compatibility check again and gets TableNotFoundException since SYSCAT doesn't exist
            // 2. Any other client --> Gets ADE, runs client server compatibility check again and gets AccessDeniedException since it doesn't have EXEC perms
            verifyDenied(getConnectionAction(), TableNotFoundException.class, regularUser1);
        }

        // Phoenix Client caches connection per user
        // If we grant permissions, get a connection and then revoke it, we can still get the cached connection
        // However it will fail for other read queries
        // Thus this test grants and revokes for 2 users, so that both functionality can be tested.
        grantSystemTableAccess(superUser1, regularUser1, regularUser2);
        verifyAllowed(getConnectionAction(), regularUser1);
        revokeSystemTableAccess(superUser1, regularUser2);
        verifyDenied(getConnectionAction(), AccessDeniedException.class, regularUser2);
    }

    /**
     * Superuser grants admin perms to user1, who will in-turn grant admin perms to user2
     * Not affected with namespace props
     * Tests grant revoke permissions on per user global level
     */
    @Test
    public void testSuperUserCanChangePerms() throws Exception {
        // Grant System Table access to all users, else they can't create a Phoenix connection
        grantSystemTableAccess(superUser1, regularUser1, regularUser2, unprivilegedUser);

        verifyAllowed(grantPermissions("A", regularUser1), superUser1);

        verifyAllowed(readTableWithoutVerification(PhoenixDatabaseMetaData.SYSTEM_CATALOG), regularUser1);
        verifyAllowed(grantPermissions("A", regularUser2), regularUser1);

        verifyAllowed(revokePermissions(regularUser1), superUser1);
        verifyDenied(grantPermissions("A", regularUser3), AccessDeniedException.class, regularUser1);

        // Don't grant ADMIN perms to unprivilegedUser, thus unprivilegedUser is unable to control other permissions.
        verifyAllowed(getConnectionAction(), unprivilegedUser);
        verifyDenied(grantPermissions("ARX", regularUser4), AccessDeniedException.class, unprivilegedUser);
    }

    /**
     * Test to verify READ permissions on table, indexes and views
     * Tests automatic grant revoke of permissions per user on a table
     */
    @Test
    public void testReadPermsOnTableIndexAndView() throws Exception {
        grantSystemTableAccess(superUser1, regularUser1, regularUser2, unprivilegedUser);

        // Create new schema and grant CREATE permissions to a user
        if(isNamespaceMapped) {
            verifyAllowed(createSchema(schemaName), superUser1);
            verifyAllowed(grantPermissions("C", regularUser1, schemaName, true), superUser1);
        } else {
            verifyAllowed(grantPermissions("C", regularUser1, surroundWithDoubleQuotes(QueryConstants.HBASE_DEFAULT_SCHEMA_NAME), true), superUser1);
        }

        // Create new table. Create indexes, views and view indexes on top of it. Verify the contents by querying it
        verifyAllowed(createTable(fullTableName), regularUser1);
        verifyAllowed(readTable(fullTableName), regularUser1);
        verifyAllowed(createIndex(idx1TableName, fullTableName), regularUser1);
        verifyAllowed(createIndex(idx2TableName, fullTableName), regularUser1);
        verifyAllowed(createLocalIndex(localIdx1TableName, fullTableName), regularUser1);
        verifyAllowed(createView(view1TableName, fullTableName), regularUser1);
        verifyAllowed(createIndex(idx3TableName, view1TableName), regularUser1);

        // RegularUser2 doesn't have any permissions. It can get a PhoenixConnection
        // However it cannot query table, indexes or views without READ perms
        verifyAllowed(getConnectionAction(), regularUser2);
        verifyDenied(readTable(fullTableName), AccessDeniedException.class, regularUser2);
        verifyDenied(readTable(fullTableName, idx1TableName), AccessDeniedException.class, regularUser2);
        verifyDenied(readTable(view1TableName), AccessDeniedException.class, regularUser2);
        verifyDenied(readTableWithoutVerification(schemaName + "." + idx1TableName), AccessDeniedException.class, regularUser2);

        // Grant READ permissions to RegularUser2 on the table
        // Permissions should propagate automatically to relevant physical tables such as global index and view index.
        verifyAllowed(grantPermissions("RX", regularUser2, fullTableName, false), regularUser1);
        // Granting permissions directly to index tables should fail
        verifyDenied(grantPermissions("W", regularUser2, schemaName + "." + idx1TableName, false), AccessDeniedException.class, regularUser1);
        // Granting permissions directly to views should fail. We expect TableNotFoundException since VIEWS are not physical tables
        verifyDenied(grantPermissions("W", regularUser2, schemaName + "." + view1TableName, false), TableNotFoundException.class, regularUser1);

        // Verify that all other access are successful now
        verifyAllowed(readTable(fullTableName), regularUser2);
        verifyAllowed(readTable(fullTableName, idx1TableName), regularUser2);
        verifyAllowed(readTable(fullTableName, idx2TableName), regularUser2);
        verifyAllowed(readTable(fullTableName, localIdx1TableName), regularUser2);
        verifyAllowed(readTableWithoutVerification(schemaName + "." + idx1TableName), regularUser2);
        verifyAllowed(readTable(view1TableName), regularUser2);
        verifyAllowed(readMultiTenantTableWithIndex(view1TableName), regularUser2);

        // Revoke READ permissions to RegularUser2 on the table
        // Permissions should propagate automatically to relevant physical tables such as global index and view index.
        verifyAllowed(revokePermissions(regularUser2, fullTableName, false), regularUser1);
        // READ query should fail now
        verifyDenied(readTable(fullTableName), AccessDeniedException.class, regularUser2);
        verifyDenied(readTableWithoutVerification(schemaName + "." + idx1TableName), AccessDeniedException.class, regularUser2);
    }

    /**
     * Verifies permissions for users present inside a group
     */
    @Test
    public void testGroupUserPerms() throws Exception {
        if(isNamespaceMapped) {
            verifyAllowed(createSchema(schemaName), superUser1);
        }
        verifyAllowed(createTable(fullTableName), superUser1);

        // Grant SYSTEM table access to GROUP_SYSTEM_ACCESS and regularUser1
        verifyAllowed(grantPermissions("RX", GROUP_SYSTEM_ACCESS, PHOENIX_SYSTEM_TABLES_IDENTIFIERS, false), superUser1);
        grantSystemTableAccess(superUser1, regularUser1);

        // Grant Permissions to Groups (Should be automatically applicable to all users inside it)
        verifyAllowed(grantPermissions("ARX", GROUP_SYSTEM_ACCESS, fullTableName, false), superUser1);
        verifyAllowed(readTable(fullTableName), groupUser);

        // GroupUser is an admin and can grant perms to other users
        verifyDenied(readTable(fullTableName), AccessDeniedException.class, regularUser1);
        verifyAllowed(grantPermissions("RX", regularUser1, fullTableName, false), groupUser);
        verifyAllowed(readTable(fullTableName), regularUser1);

        // Revoke the perms and try accessing data again
        verifyAllowed(revokePermissions(GROUP_SYSTEM_ACCESS, fullTableName, false), superUser1);
        verifyDenied(readTable(fullTableName), AccessDeniedException.class, groupUser);
    }

    /**
     * Tests permissions for MultiTenant Tables and view index tables
     */
    @Test
    public void testMultiTenantTables() throws Exception {
        grantSystemTableAccess(superUser1, regularUser1, regularUser2, regularUser3);

        if(isNamespaceMapped) {
            verifyAllowed(createSchema(schemaName), superUser1);
            verifyAllowed(grantPermissions("C", regularUser1, schemaName, true), superUser1);
        } else {
            verifyAllowed(grantPermissions("C", regularUser1, surroundWithDoubleQuotes(QueryConstants.HBASE_DEFAULT_SCHEMA_NAME), true), superUser1);
        }

        // Create MultiTenant Table (View Index Table should be automatically created)
        // At this point, the index table doesn't contain any data
        verifyAllowed(createMultiTenantTable(fullTableName), regularUser1);

        // RegularUser2 doesn't have access yet, RegularUser1 should have RWXCA on the table
        verifyDenied(readMultiTenantTableWithoutIndex(fullTableName), AccessDeniedException.class, regularUser2);

        // Grant perms to base table (Should propagate to View Index as well)
        verifyAllowed(grantPermissions("RX", regularUser2, fullTableName, false), regularUser1);
        // Try reading full table
        verifyAllowed(readMultiTenantTableWithoutIndex(fullTableName), regularUser2);

        // Create tenant specific views on the table using tenant specific Phoenix Connection
        verifyAllowed(createView(view1TableName, fullTableName, "o1"), regularUser1);
        verifyAllowed(createView(view2TableName, fullTableName, "o2"), regularUser1);

        // Create indexes on those views using tenant specific Phoenix Connection
        // It is not possible to create indexes on tenant specific views without tenant connection
        verifyAllowed(createIndex(idx1TableName, view1TableName, "o1"), regularUser1);
        verifyAllowed(createIndex(idx2TableName, view2TableName, "o2"), regularUser1);

        // Read the tables as regularUser2, with and without the use of Index table
        // If perms are propagated correctly, then both of them should work
        // The test checks if the query plan uses the index table by searching for "_IDX_" string
        // _IDX_ is the prefix used with base table name to derieve the name of view index table
        verifyAllowed(readMultiTenantTableWithIndex(view1TableName, "o1"), regularUser2);
        verifyAllowed(readMultiTenantTableWithoutIndex(view2TableName, "o2"), regularUser2);
    }

    /**
     * Grant RX permissions on the schema to regularUser1,
     * Creating view on a table with that schema by regularUser1 should be allowed
     */
    @Test
    public void testCreateViewOnTableWithRXPermsOnSchema() throws Exception {
        grantSystemTableAccess(superUser1, regularUser1, regularUser2, regularUser3);

        if(isNamespaceMapped) {
            verifyAllowed(createSchema(schemaName), superUser1);
            verifyAllowed(createTable(fullTableName), superUser1);
            verifyAllowed(grantPermissions("RX", regularUser1, schemaName, true), superUser1);
        } else {
            verifyAllowed(createTable(fullTableName), superUser1);
            verifyAllowed(grantPermissions("RX", regularUser1, surroundWithDoubleQuotes(QueryConstants.HBASE_DEFAULT_SCHEMA_NAME), true), superUser1);
        }
        verifyAllowed(createView(view1TableName, fullTableName), regularUser1);
    }

    protected void grantSystemTableAccess() throws Exception{
        try (Connection conn = getConnection()) {
            if (isNamespaceMapped) {
                grantPermissions(regularUser1.getShortName(), PHOENIX_NAMESPACE_MAPPED_SYSTEM_TABLES, Permission.Action.READ,
                        Permission.Action.EXEC);
                grantPermissions(unprivilegedUser.getShortName(), PHOENIX_NAMESPACE_MAPPED_SYSTEM_TABLES,
                        Permission.Action.READ, Permission.Action.EXEC);
                grantPermissions(AuthUtil.toGroupEntry(GROUP_SYSTEM_ACCESS), PHOENIX_NAMESPACE_MAPPED_SYSTEM_TABLES,
                        Permission.Action.READ, Permission.Action.EXEC);
                // Local Index requires WRITE permission on SYSTEM.SEQUENCE TABLE.
                grantPermissions(regularUser1.getName(), Collections.singleton("SYSTEM:SEQUENCE"), Permission.Action.WRITE,
                        Permission.Action.READ, Permission.Action.EXEC);
                grantPermissions(unprivilegedUser.getName(), Collections.singleton("SYSTEM:SEQUENCE"), Permission.Action.WRITE,
                        Permission.Action.READ, Permission.Action.EXEC);
                grantPermissions(regularUser1.getShortName(), Collections.singleton("SYSTEM:MUTEX"), Permission.Action.WRITE,
                        Permission.Action.READ, Permission.Action.EXEC);
                grantPermissions(unprivilegedUser.getShortName(), Collections.singleton("SYSTEM:MUTEX"), Permission.Action.WRITE,
                        Permission.Action.READ, Permission.Action.EXEC);

            } else {
                grantPermissions(regularUser1.getName(), PHOENIX_SYSTEM_TABLES, Permission.Action.READ, Permission.Action.EXEC);
                grantPermissions(unprivilegedUser.getName(), PHOENIX_SYSTEM_TABLES, Permission.Action.READ, Permission.Action.EXEC);
                grantPermissions(AuthUtil.toGroupEntry(GROUP_SYSTEM_ACCESS), PHOENIX_SYSTEM_TABLES, Permission.Action.READ, Permission.Action.EXEC);
                // Local Index requires WRITE permission on SYSTEM.SEQUENCE TABLE.
                grantPermissions(regularUser1.getName(), Collections.singleton("SYSTEM.SEQUENCE"), Permission.Action.WRITE,
                        Permission.Action.READ, Permission.Action.EXEC);
                grantPermissions(unprivilegedUser.getName(), Collections.singleton("SYSTEM:SEQUENCE"), Permission.Action.WRITE,
                        Permission.Action.READ, Permission.Action.EXEC);
                grantPermissions(regularUser1.getShortName(), Collections.singleton("SYSTEM.MUTEX"), Permission.Action.WRITE,
                        Permission.Action.READ, Permission.Action.EXEC);
                grantPermissions(unprivilegedUser.getShortName(), Collections.singleton("SYSTEM.MUTEX"), Permission.Action.WRITE,
                        Permission.Action.READ, Permission.Action.EXEC);
            }
        } catch (Throwable e) {
            if (e instanceof Exception) {
                throw (Exception)e;
            } else {
                throw new Exception(e);
            }
        }
    }

    @Test
    public void testAutomaticGrantWithIndexAndView() throws Throwable {
        final String schema = "TEST_INDEX_VIEW";
        final String tableName = "TABLE_DDL_PERMISSION_IT";
        final String phoenixTableName = schema + "." + tableName;
        final String indexName1 = tableName + "_IDX1";
        final String indexName2 = tableName + "_IDX2";
        final String lIndexName1 = tableName + "_LIDX1";
        final String viewName1 = schema+"."+tableName + "_V1";
        final String viewName2 = schema+"."+tableName + "_V2";
        final String viewName3 = schema+"."+tableName + "_V3";
        final String viewName4 = schema+"."+tableName + "_V4";
        final String viewIndexName1 = tableName + "_VIDX1";
        final String viewIndexName2 = tableName + "_VIDX2";
        grantSystemTableAccess();
        try {
            superUser1.runAs(new PrivilegedExceptionAction<Void>() {
                @Override
                public Void run() throws Exception {
                    try {
                        verifyAllowed(createSchema(schema), superUser1);
                        //Neded Global ADMIN for flush operation during drop table
                        AccessControlClient.grant(getUtility().getConnection(),regularUser1.getName(), Permission.Action.ADMIN);
                        if (isNamespaceMapped) {
                            grantPermissions(regularUser1.getName(), schema, Permission.Action.CREATE);
                            grantPermissions(AuthUtil.toGroupEntry(GROUP_SYSTEM_ACCESS), schema, Permission.Action.CREATE);

                        } else {
                            grantPermissions(regularUser1.getName(),
                                    NamespaceDescriptor.DEFAULT_NAMESPACE.getName(), Permission.Action.CREATE);
                            grantPermissions(AuthUtil.toGroupEntry(GROUP_SYSTEM_ACCESS),
                                    NamespaceDescriptor.DEFAULT_NAMESPACE.getName(), Permission.Action.CREATE);

                        }
                    } catch (Throwable e) {
                        if (e instanceof Exception) {
                            throw (Exception)e;
                        } else {
                            throw new Exception(e);
                        }
                    }
                    return null;
                }
            });

            verifyAllowed(createTable(phoenixTableName), regularUser1);
            verifyAllowed(createIndex(indexName1, phoenixTableName), regularUser1);
            verifyAllowed(createView(viewName1, phoenixTableName), regularUser1);
            verifyAllowed(createLocalIndex(lIndexName1, phoenixTableName), regularUser1);
            verifyAllowed(createIndex(viewIndexName1, viewName1), regularUser1);
            verifyAllowed(createIndex(viewIndexName2, viewName1), regularUser1);
            verifyAllowed(createView(viewName4, viewName1), regularUser1);
            verifyAllowed(readTable(phoenixTableName), regularUser1);

            verifyDenied(createIndex(indexName2, phoenixTableName), AccessDeniedException.class, unprivilegedUser);
            verifyDenied(createView(viewName2, phoenixTableName),AccessDeniedException.class,  unprivilegedUser);
            verifyDenied(createView(viewName3, viewName1), AccessDeniedException.class, unprivilegedUser);
            verifyDenied(dropView(viewName1), AccessDeniedException.class, unprivilegedUser);

            verifyDenied(dropIndex(indexName1, phoenixTableName), AccessDeniedException.class, unprivilegedUser);
            verifyDenied(dropTable(phoenixTableName), AccessDeniedException.class, unprivilegedUser);
            verifyDenied(rebuildIndex(indexName1, phoenixTableName), AccessDeniedException.class, unprivilegedUser);
            verifyDenied(addColumn(phoenixTableName, "val1"), AccessDeniedException.class, unprivilegedUser);
            verifyDenied(dropColumn(phoenixTableName, "val"), AccessDeniedException.class, unprivilegedUser);
            verifyDenied(addProperties(phoenixTableName, "GUIDE_POSTS_WIDTH", "100"), AccessDeniedException.class, unprivilegedUser);

            // Granting read permission to unprivileged user, now he should be able to create view but not index
            grantPermissions(unprivilegedUser.getShortName(),
                    Collections.singleton(
                            SchemaUtil.getPhysicalHBaseTableName(schema, tableName, isNamespaceMapped).getString()),
                    Permission.Action.READ, Permission.Action.EXEC);
            grantPermissions(AuthUtil.toGroupEntry(GROUP_SYSTEM_ACCESS),
                    Collections.singleton(
                            SchemaUtil.getPhysicalHBaseTableName(schema, tableName, isNamespaceMapped).getString()),
                    Permission.Action.READ, Permission.Action.EXEC);
            verifyDenied(createIndex(indexName2, phoenixTableName), AccessDeniedException.class, unprivilegedUser);
            verifyAllowed(createView(viewName2, phoenixTableName), unprivilegedUser);
            verifyAllowed(createView(viewName3, viewName1), unprivilegedUser);

            // Grant create permission in namespace
            if (isNamespaceMapped) {
                grantPermissions(unprivilegedUser.getShortName(), schema, Permission.Action.CREATE);
            } else {
                grantPermissions(unprivilegedUser.getShortName(), NamespaceDescriptor.DEFAULT_NAMESPACE.getName(),
                        Permission.Action.CREATE);
            }

            // we should be able to read the data from another index as well to which we have not given any access to
            // this user
            verifyAllowed(readTable(phoenixTableName, indexName1), unprivilegedUser);
            verifyAllowed(readTable(phoenixTableName), regularUser1);
            verifyAllowed(rebuildIndex(indexName1, phoenixTableName), regularUser1);
            verifyAllowed(addColumn(phoenixTableName, "val1"), regularUser1);
            verifyAllowed(addProperties(phoenixTableName, "GUIDE_POSTS_WIDTH", "100"), regularUser1);
            verifyAllowed(dropView(viewName1), regularUser1);
            verifyAllowed(dropView(viewName2), regularUser1);
            verifyAllowed(dropColumn(phoenixTableName, "val1"), regularUser1);
            verifyAllowed(dropIndex(indexName1, phoenixTableName), regularUser1);
            verifyAllowed(dropTable(phoenixTableName), regularUser1);

            // check again with super users
            verifyAllowed(createTable(phoenixTableName), superUser2);
            verifyAllowed(createIndex(indexName1, phoenixTableName), superUser2);
            verifyAllowed(createView(viewName1, phoenixTableName), superUser2);
            verifyAllowed(readTable(phoenixTableName), superUser2);
            verifyAllowed(dropView(viewName1), superUser2);
            verifyAllowed(dropTable(phoenixTableName), superUser2);

        } finally {
            revokeAll();
        }
    }

    @Test
    public void testUpsertIntoImmutableTable() throws Throwable {
        final String schema = generateUniqueName();
        final String tableName = generateUniqueName();
        final String phoenixTableName = schema + "." + tableName;
        grantSystemTableAccess();
        try {
            superUser1.runAs(new PrivilegedExceptionAction<Void>() {
                @Override
                public Void run() throws Exception {
                    try {
                        verifyAllowed(createSchema(schema), superUser1);
                        verifyAllowed(onlyCreateImmutableTable(phoenixTableName), superUser1);
                    } catch (Throwable e) {
                        if (e instanceof Exception) {
                            throw (Exception) e;
                        } else {
                            throw new Exception(e);
                        }
                    }
                    return null;
                }
            });

            if (isNamespaceMapped) {
                grantPermissions(unprivilegedUser.getShortName(), schema, Permission.Action.WRITE,
                    Permission.Action.READ, Permission.Action.EXEC);
            } else {
                grantPermissions(unprivilegedUser.getShortName(),
                    NamespaceDescriptor.DEFAULT_NAMESPACE.getName(), Permission.Action.WRITE,
                    Permission.Action.READ, Permission.Action.EXEC);
            }
            verifyAllowed(upsertRowsIntoTable(phoenixTableName), unprivilegedUser);
            verifyAllowed(readTable(phoenixTableName), unprivilegedUser);
        } finally {
            revokeAll();
        }
    }

    AccessTestAction onlyCreateImmutableTable(final String tableName) throws SQLException {
        return new AccessTestAction() {
            @Override
            public Object run() throws Exception {
                try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                    assertFalse(stmt.execute("CREATE IMMUTABLE TABLE " + tableName
                            + "(pk INTEGER not null primary key, data VARCHAR, val integer)"));
                }
                return null;
            }
        };
    }

    AccessTestAction upsertRowsIntoTable(final String tableName) throws SQLException {
        return new AccessTestAction() {
            @Override
            public Object run() throws Exception {
                try (Connection conn = getConnection()) {
                    try (PreparedStatement pstmt =
                            conn.prepareStatement(
                                "UPSERT INTO " + tableName + " values(?, ?, ?)")) {
                        for (int i = 0; i < NUM_RECORDS; i++) {
                            pstmt.setInt(1, i);
                            pstmt.setString(2, Integer.toString(i));
                            pstmt.setInt(3, i);
                            assertEquals(1, pstmt.executeUpdate());
                        }
                    }
                    conn.commit();
                }
                return null;
            }
        };
    }

}
