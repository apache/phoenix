package org.apache.phoenix.end2end;

import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.AuthUtil;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.security.AccessDeniedException;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.access.AccessControlClient;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.phoenix.exception.PhoenixIOException;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.TableNotFoundException;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;
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
import java.util.ArrayList;
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
        configureRandomHMasterPort(config);
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

        groupUser = User.createUserForTesting(testUtil.getConfiguration(), "groupUser", new String[]{GROUP_SYSTEM_ACCESS});

        unprivilegedUser = User.createUserForTesting(configuration, "unprivilegedUser", new String[0]);
    }

    private void configureRandomHMasterPort(Configuration config) {
        // Avoid multiple clusters trying to bind the master's info port (16010)
        config.setInt(HConstants.MASTER_INFO_PORT, -1);
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
    void revokeAll() throws IOException, Throwable {
        AccessControlClient.revoke(getUtility().getConnection(), AuthUtil.toGroupEntry(GROUP_SYSTEM_ACCESS), Permission.Action.values() );
        AccessControlClient.revoke(getUtility().getConnection(), regularUser1.getShortName(), Permission.Action.values() );
        AccessControlClient.revoke(getUtility().getConnection(), unprivilegedUser.getShortName(), Permission.Action.values() );
    }

    Properties getClientProperties() {
        Properties props = new Properties();
        props.setProperty(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, Boolean.toString(isNamespaceMapped));
        return props;
    }

    public Connection getConnection() throws SQLException{
        return DriverManager.getConnection(getUrl(), getClientProperties());
    }

    protected static String getUrl() {
        return "jdbc:phoenix:localhost:" + testUtil.getZkCluster().getClientPort() + ":/hbase";
    }

    static Set<String> getHBaseTables() throws IOException {
        Set<String> tables = new HashSet<>();
        System.out.print("Tables in HBase: ");
        for (TableName tn : testUtil.getHBaseAdmin().listTableNames()) {
            tables.add(tn.getNameAsString());
            System.out.print(tn.getNameAsString() + " ");
        }
        System.out.println();
        return tables;
    }

    AccessTestAction grantPermissions(final String actions, final Object ug, final String tableOrSchemaList, final boolean isSchema) throws SQLException {
        return grantPermissions(actions, ug, Collections.singleton(tableOrSchemaList), isSchema);
    }

    AccessTestAction grantPermissions(final String actions, final Object ug, final Set<String> tableOrSchemaList, final boolean isSchema) throws SQLException {
        return new AccessTestAction() {
            @Override
            public Object run() throws Exception {
                try (Connection conn = getConnection(); Statement stmt = conn.createStatement();) {
                    for(String tableOrSchema : tableOrSchemaList) {
                        String grantStmtSQL = "GRANT '" + actions + "' ON " + (isSchema ? " SCHEMA " : " TABLE ") + tableOrSchema + " TO "
                                + ((ug instanceof String) ? (" GROUP " + "'" + ug + "'") : ("'" + ((User)ug).getShortName() + "'"));
                        System.out.println("grantStmtSQL: " + grantStmtSQL);
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
                    System.out.println("grantStmtSQL: " + grantStmtSQL);
                    assertFalse(stmt.execute(grantStmtSQL));
                }
                return null;
            }
        };
    }

    AccessTestAction revokePermissions(final Object ug, final String tableOrSchemaList, final boolean isSchema) throws SQLException {
        return revokePermissions(ug, Collections.singleton(tableOrSchemaList), isSchema);
    }

    AccessTestAction revokePermissions(final Object ug, final Set<String> tableOrSchemaList, final boolean isSchema) throws SQLException {
        return new AccessTestAction() {
            @Override
            public Object run() throws Exception {
                try (Connection conn = getConnection(); Statement stmt = conn.createStatement();) {
                    for(String tableOrSchema : tableOrSchemaList) {
                        String revokeStmtSQL = "REVOKE ON " + (isSchema ? " SCHEMA " : " TABLE ") + tableOrSchema + " FROM "
                                + ((ug instanceof String) ? (" GROUP " + "'" + ug + "'") : ("'" + ((User)ug).getShortName() + "'"));
                        System.out.println("revokeStmtSQL: " + revokeStmtSQL);
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
                    String revokeStmtSQL = "REVOKE FROM " + ((ug instanceof String) ? (" GROUP " + "'" + ug + "'") : ("'" + ((User)ug).getShortName() + "'"));
                    System.out.println("revokeStmtSQL: " + revokeStmtSQL);
                    assertFalse(stmt.execute(revokeStmtSQL));
                }
                return null;
            }
        };
    }

    AccessTestAction getConnectionAction() throws SQLException {
        return new AccessTestAction() {
            @Override
            public Object run() throws Exception {
                try (Connection conn = getConnection();) {
                    System.out.println("Got Connection: " + conn.toString());
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
                    assertFalse(stmt.execute("CREATE TABLE " + tableName + "(pk INTEGER not null primary key, data VARCHAR,val integer)"));
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
                    String readTableSQL = "SELECT "+(indexName!=null?"/*+ INDEX("+tableName+" "+indexName+")*/":"")+" pk, data,val FROM " + tableName +" where data>='0'";
                    System.out.println("readTableSQL = " + readTableSQL);
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
        return new AccessTestAction() {
            @Override
            public Object run() throws Exception {

                try (Connection conn = getConnection(); Statement stmt = conn.createStatement();) {
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
        return new AccessTestAction() {
            @Override
            public Object run() throws Exception {
                try (Connection conn = getConnection(); Statement stmt = conn.createStatement();) {
                    assertFalse(stmt.execute("CREATE VIEW " + viewName + " AS SELECT * FROM " + dataTable));
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

                for(Throwable throwable : Throwables.getCausalChain(ex)) {
                    System.out.println("Verify Throwable: " + throwable.getClass() + " " + throwable.getMessage());
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


//    /** This fails only in case of ADE or empty list for any of the users. */
//    void verifyAllowed(AccessTestAction action, UserGroupInformation... users) throws Exception {
//        if(users.length == 0) {
//            throw new Exception("Action needs at least one user to run");
//        }
//        for (UserGroupInformation user : users) {
//            verifyAllowed(user, action);
//        }
//    }
//
//    /** This passes only if desired exception is caught for all users. */
//    <T> void verifyDenied(AccessTestAction action, Class<T> exception, UserGroupInformation... users) throws Exception {
//        if(users.length == 0) {
//            throw new Exception("Action needs at least one user to run");
//        }
//        for (UserGroupInformation user : users) {
//            verifyDenied(user, exception, action);
//        }
//    }

//    /** This fails only in case of ADE or empty list for any of the actions. */
//    void verifyAllowed(UserGroupInformation user, TableDDLPermissionsIT.AccessTestAction... actions) throws Exception {
//        for (TableDDLPermissionsIT.AccessTestAction action : actions) {
//            try {
//                Object obj = user.doAs(action);
//                if (obj != null && obj instanceof List<?>) {
//                    List<?> results = (List<?>) obj;
//                    if (results != null && results.isEmpty()) {
//                        fail("Empty non null results from action for user '" + user.getShortName() + "'");
//                    }
//                }
//            } catch (AccessDeniedException ade) {
//                fail("Expected action to pass for user '" + user.getShortName() + "' but was denied");
//            }
//        }
//    }

//    /** This passes only if desired exception is caught for all users. */
//    <T> void verifyDenied(UserGroupInformation user, Class<T> exception, TableDDLPermissionsIT.AccessTestAction... actions) throws Exception {
//        for (TableDDLPermissionsIT.AccessTestAction action : actions) {
//            try {
//                user.doAs(action);
//                fail("Expected exception was not thrown for user '" + user.getShortName() + "'");
//            } catch (IOException e) {
//                fail("Expected exception was not thrown for user '" + user.getShortName() + "'");
//            } catch (UndeclaredThrowableException ute) {
//                Throwable ex = ute.getUndeclaredThrowable();
//
//                for(Throwable throwable : Throwables.getCausalChain(ex)) {
//                    System.out.println("Verify Throwable: " + throwable.getClass() + " " + throwable.getMessage());
//                    if(exception.equals(throwable.getClass())) {
//                        if(throwable instanceof AccessDeniedException) {
//                            validateAccessDeniedException((AccessDeniedException) throwable);
//                        }
//                        return;
//                    }
//                }
//
//            } catch(RuntimeException ex) {
//                // This can occur while accessing tabledescriptors from client by the unprivileged user
//                if (ex.getCause() instanceof AccessDeniedException) {
//                    // expected result
//                    validateAccessDeniedException((AccessDeniedException) ex.getCause());
//                    return;
//                }
//            }
//            fail("Expected exception was not thrown for user '" + user.getShortName() + "'");
//        }
//    }

    void validateAccessDeniedException(AccessDeniedException ade) {
        String msg = ade.getMessage();
        assertTrue("Exception contained unexpected message: '" + msg + "'",
                !msg.contains("is not the scanner owner"));
    }
}
