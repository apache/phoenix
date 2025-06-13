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
package org.apache.phoenix.end2end;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.LocalHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.security.AccessDeniedException;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.access.AccessControlClient;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.phoenix.mapreduce.index.IndexTool;
import org.apache.phoenix.mapreduce.index.PhoenixIndexImportDirectMapper;
import org.apache.phoenix.mapreduce.index.PhoenixServerBuildIndexMapper;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.thirdparty.com.google.common.base.Throwables;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.*;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.security.PrivilegedExceptionAction;
import java.sql.*;
import java.sql.Connection;
import java.util.*;
import java.util.concurrent.Callable;

import static org.apache.phoenix.end2end.BasePermissionsIT.*;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.*;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.*;

@Category(NeedsOwnMiniClusterTest.class)
public class AsyncIndexPermissionIT extends BaseTest{

//    static HBaseTestingUtility testUtil;

    private static final String SUPER_USER = System.getProperty("user.name");

    boolean isNamespaceMapped;

    // Super User has all the access
    protected static User superUser1 = null;
    protected static User superUser2 = null;

    // Regular users are granted and revoked permissions as needed
    protected User regularUser1 = null;
    protected User regularUser2 = null;
    protected User regularUser3 = null;

    public AsyncIndexPermissionIT() throws Exception {
        this.isNamespaceMapped = true;
    }

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        if (null != utility) {
            utility.shutdownMiniCluster();
            utility = null;
        }

        enablePhoenixHBaseAuthorization(config, false);
        configureNamespacesOnServer(config, true);
        configureStatsConfigurations(config);
        config.setBoolean(LocalHBaseCluster.ASSIGN_RANDOM_PORTS, true);


        Map<String, String> serverProps = Maps.newHashMapWithExpectedSize(2);
        serverProps.put(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, Boolean.toString(true));

        Map<String, String> clientProps = Maps.newHashMapWithExpectedSize(2);
        clientProps.put(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, Boolean.toString(true));

        utility = new HBaseTestingUtility(config);

        setUpTestDriver(new ReadOnlyProps(serverProps.entrySet().iterator()),
                new ReadOnlyProps(clientProps.entrySet().iterator()));

        superUser1 = User.createUserForTesting(config, SUPER_USER, new String[0]);
        superUser2 = User.createUserForTesting(config, "superUser2", new String[0]);
    }

    @Before
    public void initUsersAndTables() {
        regularUser1 = User.createUserForTesting(config, "regularUser1_"
                + generateUniqueName(), new String[0]);
        regularUser2 = User.createUserForTesting(config, "regularUser2_"
                + generateUniqueName(), new String[0]);
        regularUser3 = User.createUserForTesting(config, "regularUser3_"
                + generateUniqueName(), new String[0]);
    }

    private BasePermissionsIT.AccessTestAction createIndex(final String indexName, final String dataTable, final String columns) throws SQLException {
        return new BasePermissionsIT.AccessTestAction() {
            @Override
            public Object run() throws Exception {
                try (Connection conn = DriverManager.getConnection(getUrl()); Statement stmt = conn.createStatement();) {
                    String indexStmtSQL = "CREATE index " + indexName + " on " + dataTable + " (" + columns +")";
                    assertFalse(stmt.execute(indexStmtSQL));
                }
                return null;
            }
        };
    }

    public static IndexTool runIndexTool(Configuration conf, boolean useSnapshot, String schemaName,
                                         String dataTableName, String indexTableName, String tenantId,
                                         int expectedStatus, IndexTool.IndexVerifyType verifyType, IndexTool.IndexDisableLoggingType disableLoggingType,
                                         String... additionalArgs) throws Exception {
        IndexTool indexingTool = new IndexTool();
        conf.set(QueryServices.TRANSACTIONS_ENABLED, Boolean.TRUE.toString());
        indexingTool.setConf(conf);
        final String[] cmdArgs = IndexToolIT.getArgValues(useSnapshot, schemaName, dataTableName,
                indexTableName, tenantId, verifyType, disableLoggingType);
        List<String> cmdArgList = new ArrayList<>(Arrays.asList(cmdArgs));
        cmdArgList.addAll(Arrays.asList(additionalArgs));
        int status = indexingTool.run(cmdArgList.toArray(new String[cmdArgList.size()]));

        if (expectedStatus == 0) {
            verifyMapper(indexingTool.getJob(), useSnapshot, schemaName, dataTableName, indexTableName, tenantId);
        }
        assertEquals(expectedStatus, status);
        return indexingTool;
    }

    private static void verifyMapper(Job job, boolean useSnapshot, String schemaName,
                                     String dataTableName, String indexTableName, String tenantId) throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        if (tenantId != null) {
            props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
        }

        try (Connection conn =
                     DriverManager.getConnection(getUrl(), props)) {
            PTable indexTable = PhoenixRuntime.getTableNoCache(conn,
                    SchemaUtil.normalizeFullTableName(SchemaUtil.getTableName(schemaName, indexTableName)));
            PTable dataTable = PhoenixRuntime.getTableNoCache(conn, SchemaUtil.normalizeFullTableName(SchemaUtil.getTableName(schemaName, dataTableName)));
            boolean transactional = dataTable.isTransactional();
            boolean localIndex = PTable.IndexType.LOCAL.equals(indexTable.getIndexType());
            if ((localIndex || !transactional) && !useSnapshot) {
                assertEquals(job.getMapperClass(), PhoenixServerBuildIndexMapper.class);
            } else {
                assertEquals(job.getMapperClass(), PhoenixIndexImportDirectMapper.class);
            }
        }
    }

    private BasePermissionsIT.AccessTestAction createIndexAsync(final String indexName, final String schema, final String tableName, final String columns, final int status) throws SQLException {
        return new BasePermissionsIT.AccessTestAction() {
            @Override
            public Object run() throws Exception {
                final String dataTable = SchemaUtil.getTableName(schema, tableName);
                try (Connection conn = DriverManager.getConnection(getUrl()); Statement stmt = conn.createStatement();) {
                    String indexStmtSQL = "CREATE index " + indexName + " on " + dataTable + " (" + columns +") ASYNC";
                    assertFalse(stmt.execute(indexStmtSQL));
                }
                try {
                    IndexToolIT.runIndexTool(false, schema, tableName, indexName, null, status, "-op", "/tmp/regular_User1_dir");
                } catch (Exception ignored) {
                    // Running the indexTool might fail because of AccessDeniedException
                }
                return null;
            }
        };
    }

    @Test(timeout = 80000)
    public void testCreateIndex() throws Throwable {
        final String schema = generateUniqueName();
        final String tableName = generateUniqueName();
        verifyAllowed(createSchema(schema), superUser1);
        grantPermissions(regularUser1.getShortName(), schema, Permission.Action.WRITE,
                Permission.Action.READ, Permission.Action.EXEC, Permission.Action.ADMIN);
        grantPermissions(regularUser1.getShortName(), "SYSTEM", Permission.Action.WRITE,
                Permission.Action.READ, Permission.Action.EXEC);

        Path workDir = new Path("/tmp/regular_User1_dir");
        FileSystem fs = workDir.getFileSystem(config);

        fs.mkdirs(workDir, FsPermission.valueOf("-rwxrwxrwx"));

        fs.setOwner(workDir, regularUser1.getShortName(), "");

        superUser1.runAs(new PrivilegedExceptionAction<Void>() {
            @Override
            public Void run() throws Exception {
                Admin admin = utility.getAdmin();
                TableDescriptorBuilder tdb = TableDescriptorBuilder.newBuilder(TableName.valueOf(schema + ":" + tableName));
                ColumnFamilyDescriptor cfd = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("0")).build();
                tdb.setColumnFamily(cfd);
                TableDescriptor td = tdb.build();
                admin.createTable(td);
                return null;
            }
        });



        verifyAllowed(createTable(SchemaUtil.getTableName(schema, tableName), 2), regularUser1);
        verifyAllowed(createIndex("ind1", SchemaUtil.getTableName(schema, tableName), "PK"), regularUser1);

        String ind3name = "IND3";
        regularUser1.runAs(createIndexAsync(ind3name, schema, tableName, "PK", 0));

        validateIndex(ind3name, schema, "a");
    }

    private void validateIndex(String ind3name, String schema, String expectedStatus) throws SQLException {
        String sql = "SELECT " + "TABLE_SCHEM,TABLE_NAME,TABLE_TYPE,INDEX_STATE" + " FROM " + SYSTEM_CATALOG_NAME
                + " WHERE TABLE_SCHEM = '%s' AND TABLE_NAME = '%s' and TABLE_TYPE = 'i'";
        ResultSet rs = getConnection().createStatement().executeQuery(String.format(sql, schema, ind3name));
        assertTrue(rs.next());
        assertEquals(expectedStatus, rs.getString(4));
    }

    public Connection getConnection() throws SQLException {
        return getConnection(null);
    }

    public Connection getConnection(String tenantId) throws SQLException {
        return DriverManager.getConnection(getUrl(), getClientProperties(tenantId));
    }

    private Properties getClientProperties(String tenantId) {
        Properties props = new Properties();
        if(tenantId != null) {
            props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
        }
        props.setProperty(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, Boolean.toString(isNamespaceMapped));
        return props;
    }

    public BasePermissionsIT.AccessTestAction createSchema(final String schemaName) throws SQLException {
        return new BasePermissionsIT.AccessTestAction() {
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

    BasePermissionsIT.AccessTestAction createTable(final String tableName, int numRecordsToInsert) throws SQLException {
        return new BasePermissionsIT.AccessTestAction() {
            @Override
            public Object run() throws Exception {
                try (Connection conn = DriverManager.getConnection(getUrl()); Statement stmt = conn.createStatement();) {
                    assertFalse(stmt.execute("CREATE TABLE " + tableName + "(pk INTEGER not null primary key, data VARCHAR, val integer)"));
                    try (PreparedStatement pstmt = conn.prepareStatement("UPSERT INTO " + tableName + " values(?, ?, ?)")) {
                        for (int i = 0; i < numRecordsToInsert; i++) {
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

    /** This fails only in case of ADE or empty list for any of the users. */
    public void verifyAllowed(BasePermissionsIT.AccessTestAction action, User... users) throws Exception {
        if(users.length == 0) {
            throw new Exception("Action needs at least one user to run");
        }
        for (User user : users) {
            verifyAllowed(user, action);
        }
    }

    private void verifyAllowed(User user, BasePermissionsIT.AccessTestAction... actions) throws Exception {
        for (BasePermissionsIT.AccessTestAction action : actions) {
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

    void grantPermissions(String toUser, String namespace, Permission.Action... actions) throws Throwable {
        updateACLs(getUtility(), new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                try {
                    AccessControlClient.grant(getUtility().getConnection(), namespace, toUser, actions);
                    return null;
                } catch (Throwable t) {
                    if (t instanceof Exception) {
                        throw (Exception) t;
                    } else {
                        throw new Exception(t);
                    }
                }
            }
        });
    }
}
