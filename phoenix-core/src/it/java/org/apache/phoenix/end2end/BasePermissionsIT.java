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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.AuthUtil;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.LocalHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.security.AccessDeniedException;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.access.AccessControlClient;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.QueryUtil;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

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
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
public class BasePermissionsIT extends BaseTest {

    private static final Log LOG = LogFactory.getLog(BasePermissionsIT.class);

    static String SUPERUSER;

    static HBaseTestingUtility testUtil;
    static final Set<String> PHOENIX_SYSTEM_TABLES = new HashSet<>(Arrays.asList(
            "SYSTEM.CATALOG", "SYSTEM.SEQUENCE", "SYSTEM.STATS", "SYSTEM.FUNCTION"));

    static final Set<String> PHOENIX_SYSTEM_TABLES_IDENTIFIERS = new HashSet<>(Arrays.asList(
            "SYSTEM.\"CATALOG\"", "SYSTEM.\"SEQUENCE\"", "SYSTEM.\"STATS\"", "SYSTEM.\"FUNCTION\""));

    static final String SYSTEM_SEQUENCE_IDENTIFIER =
            QueryConstants.SYSTEM_SCHEMA_NAME + "." + "\"" + PhoenixDatabaseMetaData.SYSTEM_SEQUENCE_TABLE+ "\"";

    static final Set<String> PHOENIX_NAMESPACE_MAPPED_SYSTEM_TABLES = new HashSet<>(Arrays.asList(
            "SYSTEM:CATALOG", "SYSTEM:SEQUENCE", "SYSTEM:STATS", "SYSTEM:FUNCTION"));

    // Create Multiple users so that we can use Hadoop UGI to run tasks as various users
    // Permissions can be granted or revoke by superusers and admins only
    // DON'T USE HADOOP UserGroupInformation class to create testing users since HBase misses some of its functionality
    // Instead use org.apache.hadoop.hbase.security.User class for testing purposes.

    // Super User has all the access
    User superUser1 = null;
    User superUser2 = null;

    // Regular users are granted and revoked permissions as needed
    User regularUser1 = null;
    User regularUser2 = null;
    User regularUser3 = null;
    User regularUser4 = null;

    // Group User is equivalent of regular user but inside a group
    // Permissions can be granted to group should affect this user
    static final String GROUP_SYSTEM_ACCESS = "group_system_access";
    User groupUser = null;

    // Unpriviledged User doesn't have any access and is denied for every action
    User unprivilegedUser = null;

    static final int NUM_RECORDS = 5;

    boolean isNamespaceMapped;

    public BasePermissionsIT(final boolean isNamespaceMapped) throws Exception {
        this.isNamespaceMapped = isNamespaceMapped;
    }

    @BeforeClass
    public static void doSetup() throws Exception {
        SUPERUSER = System.getProperty("user.name");
    }

    void startNewMiniCluster() throws Exception {
        startNewMiniCluster(new Configuration());
    }
    
    void startNewMiniCluster(Configuration overrideConf) throws Exception{
        if (null != testUtil) {
            testUtil.shutdownMiniCluster();
            testUtil = null;
        }

        testUtil = new HBaseTestingUtility();

        Configuration config = testUtil.getConfiguration();
        enablePhoenixHBaseAuthorization(config);
        configureNamespacesOnServer(config);
        config.setBoolean(LocalHBaseCluster.ASSIGN_RANDOM_PORTS, true);
        if (overrideConf != null) {
            config.addResource(overrideConf);
        }

        testUtil.startMiniCluster(1);
        initializeUsers(testUtil.getConfiguration());
    }

    private void initializeUsers(Configuration configuration) {

        superUser1 = User.createUserForTesting(configuration, SUPERUSER, new String[0]);
        superUser2 = User.createUserForTesting(configuration, "superUser2", new String[0]);

        regularUser1 = User.createUserForTesting(configuration, "regularUser1", new String[0]);
        regularUser2 = User.createUserForTesting(configuration, "regularUser2", new String[0]);
        regularUser3 = User.createUserForTesting(configuration, "regularUser3", new String[0]);
        regularUser4 = User.createUserForTesting(configuration, "regularUser4", new String[0]);

        groupUser = User.createUserForTesting(testUtil.getConfiguration(), "groupUser", new String[] {GROUP_SYSTEM_ACCESS});

        unprivilegedUser = User.createUserForTesting(configuration, "unprivilegedUser", new String[0]);
    }

    private void configureRandomHMasterPort(Configuration config) {
        // Avoid multiple clusters trying to bind the master's info port (16010)
        config.setInt(HConstants.MASTER_INFO_PORT, 0);
    }

    void enablePhoenixHBaseAuthorization(Configuration config) {
        config.set("hbase.superuser", SUPERUSER + "," + "superUser2");
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

    void configureNamespacesOnServer(Configuration conf) {
        conf.set(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, Boolean.toString(isNamespaceMapped));
    }

    @Parameterized.Parameters(name = "isNamespaceMapped={0}") // name is used by failsafe as file name in reports
    public static Collection<Boolean> data() {
        return Arrays.asList(false, true);
    }

    @After
    public void cleanup() throws Exception {
        if (testUtil != null) {
            testUtil.shutdownMiniCluster();
            testUtil = null;
        }
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

    Properties getClientProperties(String tenantId) {
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

    static Set<String> getHBaseTables() throws IOException {
        Set<String> tables = new HashSet<>();
        for (TableName tn : testUtil.getHBaseAdmin().listTableNames()) {
            tables.add(tn.getNameAsString());
        }
        return tables;
    }

    // UG Object
    // 1. Instance of String --> represents GROUP name
    // 2. Instance of User --> represents HBase user
    AccessTestAction grantPermissions(final String actions, final Object ug,
                                      final String tableOrSchemaList, final boolean isSchema) throws SQLException {
        return grantPermissions(actions, ug, Collections.singleton(tableOrSchemaList), isSchema);
    }

    AccessTestAction grantPermissions(final String actions, final Object ug,
                                      final Set<String> tableOrSchemaList, final boolean isSchema) throws SQLException {
        return new AccessTestAction() {
            @Override
            public Object run() throws Exception {
                try (Connection conn = getConnection(); Statement stmt = conn.createStatement();) {
                    for(String tableOrSchema : tableOrSchemaList) {
                        String grantStmtSQL = "GRANT '" + actions + "' ON " + (isSchema ? " SCHEMA " : " TABLE ") + tableOrSchema + " TO "
                                + ((ug instanceof String) ? (" GROUP " + "'" + ug + "'") : ("'" + ((User)ug).getShortName() + "'"));
                        LOG.info("Grant Permissions SQL: " + grantStmtSQL);
                        assertFalse(stmt.execute(grantStmtSQL));
                    }
                }
                return null;
            }
        };
    }

    AccessTestAction grantPermissions(final String actions, final User user) throws SQLException {
        return new AccessTestAction() {
            @Override
            public Object run() throws Exception {
                try (Connection conn = getConnection(); Statement stmt = conn.createStatement();) {
                    String grantStmtSQL = "GRANT '" + actions + "' TO " + " '" + user.getShortName() + "'";
                    LOG.info("Grant Permissions SQL: " + grantStmtSQL);
                    assertFalse(stmt.execute(grantStmtSQL));
                }
                return null;
            }
        };
    }

    AccessTestAction revokePermissions(final Object ug,
                                       final String tableOrSchemaList, final boolean isSchema) throws SQLException {
        return revokePermissions(ug, Collections.singleton(tableOrSchemaList), isSchema);
    }

    AccessTestAction revokePermissions(final Object ug,
                                       final Set<String> tableOrSchemaList, final boolean isSchema) throws SQLException {
        return new AccessTestAction() {
            @Override
            public Object run() throws Exception {
                try (Connection conn = getConnection(); Statement stmt = conn.createStatement();) {
                    for(String tableOrSchema : tableOrSchemaList) {
                        String revokeStmtSQL = "REVOKE ON " + (isSchema ? " SCHEMA " : " TABLE ") + tableOrSchema + " FROM "
                                + ((ug instanceof String) ? (" GROUP " + "'" + ug + "'") : ("'" + ((User)ug).getShortName() + "'"));
                        LOG.info("Revoke Permissions SQL: " + revokeStmtSQL);
                        assertFalse(stmt.execute(revokeStmtSQL));
                    }
                }
                return null;
            }
        };
    }

    AccessTestAction revokePermissions(final Object ug) throws SQLException {
        return new AccessTestAction() {
            @Override
            public Object run() throws Exception {
                try (Connection conn = getConnection(); Statement stmt = conn.createStatement();) {
                    String revokeStmtSQL = "REVOKE FROM " +
                            ((ug instanceof String) ? (" GROUP " + "'" + ug + "'") : ("'" + ((User)ug).getShortName() + "'"));
                    LOG.info("Revoke Permissions SQL: " + revokeStmtSQL);
                    assertFalse(stmt.execute(revokeStmtSQL));
                }
                return null;
            }
        };
    }

    // Attempts to get a Phoenix Connection
    // New connections could create SYSTEM tables if appropriate perms are granted
    AccessTestAction getConnectionAction() throws SQLException {
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

    AccessTestAction createMultiTenantTable(final String tableName) throws SQLException {
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

    AccessTestAction dropTable(final String tableName) throws SQLException {
        return new AccessTestAction() {
            @Override
            public Object run() throws Exception {
                try (Connection conn = getConnection(); Statement stmt = conn.createStatement();) {
                    assertFalse(stmt.execute("DROP TABLE IF EXISTS " + tableName));
                }
                return null;
            }
        };

    }

    // Attempts to read given table without verifying data
    // AccessDeniedException is only triggered when ResultSet#next() method is called
    // The first call triggers HBase Scan object
    // The Statement#executeQuery() method returns an iterator and doesn't interact with HBase API at all
    AccessTestAction readTableWithoutVerification(final String tableName) throws SQLException {
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

    AccessTestAction readTable(final String tableName) throws SQLException {
        return readTable(tableName,null);
    }

    AccessTestAction readTable(final String tableName, final String indexName) throws SQLException {
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

    AccessTestAction readMultiTenantTableWithoutIndex(final String tableName) throws SQLException {
        return readMultiTenantTableWithoutIndex(tableName, null);
    }

    AccessTestAction readMultiTenantTableWithoutIndex(final String tableName, final String tenantId) throws SQLException {
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

    AccessTestAction readMultiTenantTableWithIndex(final String tableName) throws SQLException {
        return readMultiTenantTableWithIndex(tableName, null);
    }

    AccessTestAction readMultiTenantTableWithIndex(final String tableName, final String tenantId) throws SQLException {
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

    AccessTestAction addProperties(final String tableName, final String property, final String value)
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

    AccessTestAction dropColumn(final String tableName, final String columnName) throws SQLException {
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

    AccessTestAction createIndex(final String indexName, final String dataTable) throws SQLException {
        return createIndex(indexName, dataTable, null);
    }

    AccessTestAction createIndex(final String indexName, final String dataTable, final String tenantId) throws SQLException {
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

    AccessTestAction createLocalIndex(final String indexName, final String dataTable) throws SQLException {
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

    AccessTestAction dropIndex(final String indexName, final String dataTable) throws SQLException {
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

    AccessTestAction rebuildIndex(final String indexName, final String dataTable) throws SQLException {
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

    AccessTestAction dropView(final String viewName) throws SQLException {
        return new AccessTestAction() {
            @Override
            public Object run() throws Exception {
                try (Connection conn = getConnection(); Statement stmt = conn.createStatement();) {
                    assertFalse(stmt.execute("DROP VIEW " + viewName));
                }
                return null;
            }
        };
    }

    AccessTestAction createView(final String viewName, final String dataTable) throws SQLException {
        return createView(viewName, dataTable, null);
    }

    AccessTestAction createView(final String viewName, final String dataTable, final String tenantId) throws SQLException {
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

    static interface AccessTestAction extends PrivilegedExceptionAction<Object> { }

    /** This fails only in case of ADE or empty list for any of the users. */
    void verifyAllowed(AccessTestAction action, User... users) throws Exception {
        if(users.length == 0) {
            throw new Exception("Action needs at least one user to run");
        }
        for (User user : users) {
            verifyAllowed(user, action);
        }
    }

    void verifyAllowed(User user, TableDDLPermissionsIT.AccessTestAction... actions) throws Exception {
        for (TableDDLPermissionsIT.AccessTestAction action : actions) {
            try {
                Object obj = user.runAs(action);
                if (obj != null && obj instanceof List<?>) {
                    List<?> results = (List<?>) obj;
                    if (results != null && results.isEmpty()) {
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
    <T> void verifyDenied(User user, Class<T> exception, TableDDLPermissionsIT.AccessTestAction... actions) throws Exception {
        for (TableDDLPermissionsIT.AccessTestAction action : actions) {
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

    void validateAccessDeniedException(AccessDeniedException ade) {
        String msg = ade.getMessage();
        assertTrue("Exception contained unexpected message: '" + msg + "'",
                !msg.contains("is not the scanner owner"));
    }
}
