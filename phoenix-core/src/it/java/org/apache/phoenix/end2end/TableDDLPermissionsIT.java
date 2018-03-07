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
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.AuthUtil;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.security.AccessDeniedException;
import org.apache.hadoop.hbase.security.access.AccessControlClient;
import org.apache.hadoop.hbase.security.access.Permission.Action;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.phoenix.exception.PhoenixIOException;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.collect.Maps;

/**
 * Test that verifies a user can read Phoenix tables with a minimal set of permissions.
 */
@Category(NeedsOwnMiniClusterTest.class)
@RunWith(Parameterized.class)
public class TableDDLPermissionsIT{
    private static String SUPERUSER;

    private static HBaseTestingUtility testUtil;

    private static final Set<String> PHOENIX_SYSTEM_TABLES = new HashSet<>(Arrays.asList(
            "SYSTEM.CATALOG", "SYSTEM.SEQUENCE", "SYSTEM.STATS", "SYSTEM.FUNCTION",
                "SYSTEM.MUTEX"));
    // PHOENIX-XXXX SYSTEM.MUTEX isn't being created in the SYSTEM namespace as it should be.
    private static final Set<String> PHOENIX_NAMESPACE_MAPPED_SYSTEM_TABLES = new HashSet<>(
            Arrays.asList("SYSTEM:CATALOG", "SYSTEM:SEQUENCE", "SYSTEM:STATS", "SYSTEM:FUNCTION",
                "SYSTEM.MUTEX"));
    private static final String GROUP_SYSTEM_ACCESS = "group_system_access";
    final UserGroupInformation superUser = UserGroupInformation.createUserForTesting(SUPERUSER, new String[0]);
    final UserGroupInformation superUser2 = UserGroupInformation.createUserForTesting("superuser", new String[0]);
    final UserGroupInformation regularUser = UserGroupInformation.createUserForTesting("user",  new String[0]);
    final UserGroupInformation groupUser = UserGroupInformation.createUserForTesting("user2", new String[] { GROUP_SYSTEM_ACCESS });
    final UserGroupInformation unprivilegedUser = UserGroupInformation.createUserForTesting("unprivilegedUser",
            new String[0]);


    private static final int NUM_RECORDS = 5;

    private boolean isNamespaceMapped;

    public TableDDLPermissionsIT(final boolean isNamespaceMapped) throws Exception {
        this.isNamespaceMapped = isNamespaceMapped;
        Map<String, String> clientProps = Maps.newHashMapWithExpectedSize(1);
        clientProps.put(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, "true");
    }

    private void startNewMiniCluster(Configuration overrideConf) throws Exception{
        if (null != testUtil) {
            testUtil.shutdownMiniCluster();
            testUtil = null;
        }
        testUtil = new HBaseTestingUtility();

        Configuration config = testUtil.getConfiguration();
        
        config.set("hbase.coprocessor.master.classes",
                "org.apache.hadoop.hbase.security.access.AccessController");
        config.set("hbase.coprocessor.region.classes",
                "org.apache.hadoop.hbase.security.access.AccessController");
        config.set("hbase.coprocessor.regionserver.classes",
                "org.apache.hadoop.hbase.security.access.AccessController");
        config.set("hbase.security.exec.permission.checks", "true");
        config.set("hbase.security.authorization", "true");
        config.set("hbase.superuser", SUPERUSER+","+superUser2.getShortUserName());
        config.set("hbase.regionserver.wal.codec", "org.apache.hadoop.hbase.regionserver.wal.IndexedWALEditCodec");
        config.set(QueryServices.PHOENIX_ACLS_ENABLED,"true");
        config.set(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, Boolean.toString(isNamespaceMapped));
        // Avoid multiple clusters trying to bind the master's info port (16010)
        config.setInt(HConstants.MASTER_INFO_PORT, -1);
        
        if (overrideConf != null) {
            config.addResource(overrideConf);
        }
        testUtil.startMiniCluster(1);
    }
    
    private void grantSystemTableAccess() throws Exception{
        
        try (Connection conn = getConnection()) {
            if (isNamespaceMapped) {
                grantPermissions(regularUser.getShortUserName(), PHOENIX_NAMESPACE_MAPPED_SYSTEM_TABLES, Action.READ,
                        Action.EXEC);
                grantPermissions(unprivilegedUser.getShortUserName(), PHOENIX_NAMESPACE_MAPPED_SYSTEM_TABLES,
                        Action.READ, Action.EXEC);
                grantPermissions(AuthUtil.toGroupEntry(GROUP_SYSTEM_ACCESS), PHOENIX_NAMESPACE_MAPPED_SYSTEM_TABLES,
                        Action.READ, Action.EXEC);
                // Local Index requires WRITE permission on SYSTEM.SEQUENCE TABLE.
                grantPermissions(regularUser.getShortUserName(), Collections.singleton("SYSTEM:SEQUENCE"), Action.WRITE,
                        Action.READ, Action.EXEC);
                grantPermissions(unprivilegedUser.getShortUserName(), Collections.singleton("SYSTEM:SEQUENCE"), Action.WRITE,
                        Action.READ, Action.EXEC);
                
            } else {
                grantPermissions(regularUser.getShortUserName(), PHOENIX_SYSTEM_TABLES, Action.READ, Action.EXEC);
                grantPermissions(unprivilegedUser.getShortUserName(), PHOENIX_SYSTEM_TABLES, Action.READ, Action.EXEC);
                grantPermissions(AuthUtil.toGroupEntry(GROUP_SYSTEM_ACCESS), PHOENIX_SYSTEM_TABLES, Action.READ, Action.EXEC);
                // Local Index requires WRITE permission on SYSTEM.SEQUENCE TABLE.
                grantPermissions(regularUser.getShortUserName(), Collections.singleton("SYSTEM.SEQUENCE"), Action.WRITE,
                        Action.READ, Action.EXEC);
                grantPermissions(unprivilegedUser.getShortUserName(), Collections.singleton("SYSTEM:SEQUENCE"), Action.WRITE,
                        Action.READ, Action.EXEC);
            }
        } catch (Throwable e) {
            if (e instanceof Exception) {
                throw (Exception)e;
            } else {
                throw new Exception(e);
            }
        }
    }

    @Parameters(name = "isNamespaceMapped={0}") // name is used by failsafe as file name in reports
    public static Collection<Boolean> data() {
        return Arrays.asList(true, false);
    }

    @BeforeClass
    public static void doSetup() throws Exception {
        SUPERUSER = System.getProperty("user.name");
        //setUpTestDriver(new ReadOnlyProps(serverProps.entrySet().iterator()), new ReadOnlyProps(clientProps.entrySet().iterator()));
    }

    protected static String getUrl() {
        return "jdbc:phoenix:localhost:" + testUtil.getZkCluster().getClientPort() + ":/hbase";
    }

    public Connection getConnection() throws SQLException{
        Properties props = new Properties();
        props.setProperty(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, Boolean.toString(isNamespaceMapped));
        return DriverManager.getConnection(getUrl(),props);
    }

    @Test
    public void testSchemaPermissions() throws Throwable{

        if (!isNamespaceMapped) { return; }
        try {
            startNewMiniCluster(null);
            grantSystemTableAccess();
            final String schemaName = "TEST_SCHEMA_PERMISSION";
            superUser.doAs(new PrivilegedExceptionAction<Void>() {
                @Override
                public Void run() throws Exception {
                    try {
                        AccessControlClient.grant(getUtility().getConnection(), regularUser.getShortUserName(),
                                Action.ADMIN);
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
            verifyAllowed(createSchema(schemaName), regularUser);
            // Unprivileged user cannot drop a schema
            verifyDenied(dropSchema(schemaName), unprivilegedUser);
            verifyDenied(createSchema(schemaName), unprivilegedUser);

            verifyAllowed(dropSchema(schemaName), regularUser);
        } finally {
            revokeAll();
        }
    }

    @Test
    public void testAutomaticGrantDisabled() throws Throwable{
        testIndexAndView(false);
    }
    
    public void testIndexAndView(boolean isAutomaticGrant) throws Throwable {
        Configuration conf = new Configuration();
        conf.set(QueryServices.PHOENIX_AUTOMATIC_GRANT_ENABLED, Boolean.toString(isAutomaticGrant));
        startNewMiniCluster(conf);
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
            superUser.doAs(new PrivilegedExceptionAction<Void>() {
                @Override
                public Void run() throws Exception {
                    try {
                        verifyAllowed(createSchema(schema), superUser);
                        //Neded Global ADMIN for flush operation during drop table
                        AccessControlClient.grant(getUtility().getConnection(),regularUser.getShortUserName(), Action.ADMIN);
                        if (isNamespaceMapped) {
                            grantPermissions(regularUser.getShortUserName(), schema, Action.CREATE);
                            grantPermissions(AuthUtil.toGroupEntry(GROUP_SYSTEM_ACCESS), schema, Action.CREATE);

                        } else {
                            grantPermissions(regularUser.getShortUserName(),
                                    NamespaceDescriptor.DEFAULT_NAMESPACE.getName(), Action.CREATE);
                            grantPermissions(AuthUtil.toGroupEntry(GROUP_SYSTEM_ACCESS),
                                    NamespaceDescriptor.DEFAULT_NAMESPACE.getName(), Action.CREATE);

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

            verifyAllowed(createTable(phoenixTableName), regularUser);
            verifyAllowed(createIndex(indexName1, phoenixTableName), regularUser);
            verifyAllowed(createView(viewName1, phoenixTableName), regularUser);
            verifyAllowed(createLocalIndex(lIndexName1, phoenixTableName), regularUser);
            verifyAllowed(createIndex(viewIndexName1, viewName1), regularUser);
            verifyAllowed(createIndex(viewIndexName2, viewName1), regularUser);
            verifyAllowed(createView(viewName4, viewName1), regularUser);
            verifyAllowed(readTable(phoenixTableName), regularUser);

            verifyDenied(createIndex(indexName2, phoenixTableName), unprivilegedUser);
            verifyDenied(createView(viewName2, phoenixTableName), unprivilegedUser);
            verifyDenied(createView(viewName3, viewName1), unprivilegedUser);
            verifyDenied(dropView(viewName1), unprivilegedUser);
            
            verifyDenied(dropIndex(indexName1, phoenixTableName), unprivilegedUser);
            verifyDenied(dropTable(phoenixTableName), unprivilegedUser);
            verifyDenied(rebuildIndex(indexName1, phoenixTableName), unprivilegedUser);
            verifyDenied(addColumn(phoenixTableName, "val1"), unprivilegedUser);
            verifyDenied(dropColumn(phoenixTableName, "val"), unprivilegedUser);
            verifyDenied(addProperties(phoenixTableName, "GUIDE_POSTS_WIDTH", "100"), unprivilegedUser);

            // Granting read permission to unprivileged user, now he should be able to create view but not index
            grantPermissions(unprivilegedUser.getShortUserName(),
                    Collections.singleton(
                            SchemaUtil.getPhysicalHBaseTableName(schema, tableName, isNamespaceMapped).getString()),
                    Action.READ, Action.EXEC);
            grantPermissions(AuthUtil.toGroupEntry(GROUP_SYSTEM_ACCESS),
                    Collections.singleton(
                            SchemaUtil.getPhysicalHBaseTableName(schema, tableName, isNamespaceMapped).getString()),
                    Action.READ, Action.EXEC);
            verifyDenied(createIndex(indexName2, phoenixTableName), unprivilegedUser);
            if (!isAutomaticGrant) {
                // Automatic grant will read access for all indexes
                verifyDenied(createView(viewName2, phoenixTableName), unprivilegedUser);

                // Granting read permission to unprivileged user on index so that a new view can read a index as well,
                // now
                // he should be able to create view but not index
                grantPermissions(unprivilegedUser.getShortUserName(),
                        Collections.singleton(SchemaUtil
                                .getPhysicalHBaseTableName(schema, indexName1, isNamespaceMapped).getString()),
                        Action.READ, Action.EXEC);
                verifyDenied(createView(viewName3, viewName1), unprivilegedUser);
            }
            
            verifyAllowed(createView(viewName2, phoenixTableName), unprivilegedUser);
            
            if (!isAutomaticGrant) {
                // Grant access to view index for parent view
                grantPermissions(unprivilegedUser.getShortUserName(),
                        Collections.singleton(Bytes.toString(MetaDataUtil.getViewIndexPhysicalName(SchemaUtil
                                .getPhysicalHBaseTableName(schema, tableName, isNamespaceMapped).getBytes()))),
                        Action.READ, Action.EXEC);
            }
            verifyAllowed(createView(viewName3, viewName1), unprivilegedUser);
            
            // Grant create permission in namespace
            if (isNamespaceMapped) {
                grantPermissions(unprivilegedUser.getShortUserName(), schema, Action.CREATE);
            } else {
                grantPermissions(unprivilegedUser.getShortUserName(), NamespaceDescriptor.DEFAULT_NAMESPACE.getName(),
                        Action.CREATE);
            }
            if (!isAutomaticGrant) {
                verifyDenied(createIndex(indexName2, phoenixTableName), unprivilegedUser);
                // Give user of data table access to index table which will be created by unprivilegedUser
                grantPermissions(regularUser.getShortUserName(),
                        Collections.singleton(SchemaUtil
                                .getPhysicalHBaseTableName(schema, indexName2, isNamespaceMapped).getString()),
                        Action.WRITE);
                verifyDenied(createIndex(indexName2, phoenixTableName), unprivilegedUser);
                grantPermissions(regularUser.getShortUserName(),
                        Collections.singleton(SchemaUtil
                                .getPhysicalHBaseTableName(schema, indexName2, isNamespaceMapped).getString()),
                        Action.WRITE, Action.READ, Action.CREATE, Action.EXEC, Action.ADMIN);
            }
            // we should be able to read the data from another index as well to which we have not given any access to
            // this user
            verifyAllowed(createIndex(indexName2, phoenixTableName), unprivilegedUser);
            verifyAllowed(readTable(phoenixTableName, indexName1), unprivilegedUser);
            verifyAllowed(readTable(phoenixTableName, indexName2), unprivilegedUser);
            verifyAllowed(rebuildIndex(indexName2, phoenixTableName), unprivilegedUser);

            // data table user should be able to read new index
            verifyAllowed(rebuildIndex(indexName2, phoenixTableName), regularUser);
            verifyAllowed(readTable(phoenixTableName, indexName2), regularUser);

            verifyAllowed(readTable(phoenixTableName), regularUser);
            verifyAllowed(rebuildIndex(indexName1, phoenixTableName), regularUser);
            verifyAllowed(addColumn(phoenixTableName, "val1"), regularUser);
            verifyAllowed(addProperties(phoenixTableName, "GUIDE_POSTS_WIDTH", "100"), regularUser);
            verifyAllowed(dropView(viewName1), regularUser);
            verifyAllowed(dropView(viewName2), regularUser);
            verifyAllowed(dropColumn(phoenixTableName, "val1"), regularUser);
            verifyAllowed(dropIndex(indexName2, phoenixTableName), regularUser);
            verifyAllowed(dropIndex(indexName1, phoenixTableName), regularUser);
            verifyAllowed(dropTable(phoenixTableName), regularUser);

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
    public void testAutomaticGrantEnabled() throws Throwable{
        testIndexAndView(true);
    }

    private void revokeAll() throws IOException, Throwable {
        AccessControlClient.revoke(getUtility().getConnection(), AuthUtil.toGroupEntry(GROUP_SYSTEM_ACCESS),Action.values() );
        AccessControlClient.revoke(getUtility().getConnection(), regularUser.getShortUserName(),Action.values() );
        AccessControlClient.revoke(getUtility().getConnection(), unprivilegedUser.getShortUserName(),Action.values() );
        
    }

    protected void grantPermissions(String groupEntry, Action... actions) throws IOException, Throwable {
        AccessControlClient.grant(getUtility().getConnection(), groupEntry, actions);
    }

    private AccessTestAction dropTable(final String tableName) throws SQLException {
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

    private AccessTestAction createTable(final String tableName) throws SQLException {
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

    private AccessTestAction readTable(final String tableName) throws SQLException {
        return readTable(tableName,null);
    }
    private AccessTestAction readTable(final String tableName, final String indexName) throws SQLException {
        return new AccessTestAction() {
            @Override
            public Object run() throws Exception {
                try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                    ResultSet rs = stmt.executeQuery("SELECT "+(indexName!=null?"/*+ INDEX("+tableName+" "+indexName+")*/":"")+" pk, data,val FROM " + tableName +" where data>='0'");
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

    public static HBaseTestingUtility getUtility(){
        return testUtil;
    }

    private void grantPermissions(String toUser, Set<String> tablesToGrant, Action... actions) throws Throwable {
        for (String table : tablesToGrant) {
            AccessControlClient.grant(getUtility().getConnection(), TableName.valueOf(table), toUser, null, null,
                    actions);
        }
    }

    private void grantPermissions(String toUser, String namespace, Action... actions) throws Throwable {
        AccessControlClient.grant(getUtility().getConnection(), namespace, toUser, actions);
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

    private AccessTestAction addColumn(final String tableName, final String columnName) throws SQLException {
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

    private AccessTestAction dropView(final String viewName) throws SQLException {
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

    private AccessTestAction createView(final String viewName, final String dataTable) throws SQLException {
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

    private AccessTestAction createIndex(final String indexName, final String dataTable) throws SQLException {
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

    private AccessTestAction createSchema(final String schemaName) throws SQLException {
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

    private AccessTestAction dropSchema(final String schemaName) throws SQLException {
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

    static interface AccessTestAction extends PrivilegedExceptionAction<Object> { }

    @After
    public void cleanup() throws Exception {
        if (null != testUtil) {
          testUtil.shutdownMiniCluster();
          testUtil = null;
        }
    }

    /** This fails only in case of ADE or empty list for any of the users. */
    private void verifyAllowed(AccessTestAction action, UserGroupInformation... users) throws Exception {
      for (UserGroupInformation user : users) {
        verifyAllowed(user, action);
      }
    }

    /** This passes only in case of ADE for all users. */
    private void verifyDenied(AccessTestAction action, UserGroupInformation... users) throws Exception {
      for (UserGroupInformation user : users) {
        verifyDenied(user, action);
      }
    }

    /** This fails only in case of ADE or empty list for any of the actions. */
    private void verifyAllowed(UserGroupInformation user, AccessTestAction... actions) throws Exception {
      for (AccessTestAction action : actions) {
        try {
          Object obj = user.doAs(action);
          if (obj != null && obj instanceof List<?>) {
            List<?> results = (List<?>) obj;
            if (results != null && results.isEmpty()) {
              fail("Empty non null results from action for user '" + user.getShortUserName() + "'");
            }
          }
        } catch (AccessDeniedException ade) {
          fail("Expected action to pass for user '" + user.getShortUserName() + "' but was denied");
        }
      }
    }

    /** This passes only in case of ADE for all actions. */
    private void verifyDenied(UserGroupInformation user, AccessTestAction... actions) throws Exception {
        for (AccessTestAction action : actions) {
            try {
                user.doAs(action);
                fail("Expected exception was not thrown for user '" + user.getShortUserName() + "'");
            } catch (IOException e) {
                fail("Expected exception was not thrown for user '" + user.getShortUserName() + "'");
            } catch (UndeclaredThrowableException ute) {
                Throwable ex = ute.getUndeclaredThrowable();

                if (ex instanceof PhoenixIOException) {
                    if (ex.getCause() instanceof AccessDeniedException) {
                        // expected result
                        validateAccessDeniedException((AccessDeniedException) ex.getCause());
                        return;
                    }
                }
            }catch(RuntimeException ex){
                // This can occur while accessing tabledescriptors from client by the unprivileged user
                if (ex.getCause() instanceof AccessDeniedException) {
                    // expected result
                    validateAccessDeniedException((AccessDeniedException) ex.getCause());
                    return;
                }
            }
            fail("Expected exception was not thrown for user '" + user.getShortUserName() + "'");
        }
    }

    private void validateAccessDeniedException(AccessDeniedException ade) {
        String msg = ade.getMessage();
        assertTrue("Exception contained unexpected message: '" + msg + "'",
            !msg.contains("is not the scanner owner"));
    }
}
