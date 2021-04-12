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
import java.security.PrivilegedExceptionAction;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.phoenix.coprocessor.MetaDataProtocol;
import org.apache.phoenix.exception.UpgradeInProgressException;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.ConnectionQueryServicesImpl;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(NeedsOwnMiniClusterTest.class)
public class MigrateSystemTablesToSystemNamespaceIT extends BaseTest {

    private static final Set<String> PHOENIX_SYSTEM_TABLES = new HashSet<>(Arrays.asList(
            "SYSTEM.CATALOG", "SYSTEM.SEQUENCE", "SYSTEM.STATS", "SYSTEM.FUNCTION",
            "SYSTEM.MUTEX","SYSTEM.LOG", "SYSTEM.CHILD_LINK", "SYSTEM.TASK"));
    private static final Set<String> PHOENIX_NAMESPACE_MAPPED_SYSTEM_TABLES = new HashSet<>(
            Arrays.asList("SYSTEM:CATALOG", "SYSTEM:SEQUENCE", "SYSTEM:STATS", "SYSTEM:FUNCTION",
                    "SYSTEM:MUTEX","SYSTEM:LOG", "SYSTEM:CHILD_LINK", "SYSTEM:TASK"));
    private static final String SCHEMA_NAME = "MIGRATETEST";
    private static final String TABLE_NAME =
            SCHEMA_NAME + "." + MigrateSystemTablesToSystemNamespaceIT.class.getSimpleName().toUpperCase();
    private static final int NUM_RECORDS = 5;

    private HBaseTestingUtility testUtil = null;
    private Set<String> hbaseTables;

    // Create Multiple users since Phoenix caches the connection per user
    // Migration or upgrade code will run every time for each user.
    final UserGroupInformation user1 =
            UserGroupInformation.createUserForTesting("user1", new String[0]);
    final UserGroupInformation user2 =
            UserGroupInformation.createUserForTesting("user2", new String[0]);
    final UserGroupInformation user3 =
            UserGroupInformation.createUserForTesting("user3", new String[0]);
    final UserGroupInformation user4 =
            UserGroupInformation.createUserForTesting("user4", new String[0]);

    public final void doSetup(boolean systemMappingEnabled) throws Exception {
        testUtil = new HBaseTestingUtility();
        Configuration conf = testUtil.getConfiguration();
        enableNamespacesOnServer(conf, systemMappingEnabled);
        configureRandomHMasterPort(conf);
        testUtil.startMiniCluster(1);
    }

    @After
    public void tearDownMiniCluster() {
        try {
            if (testUtil != null) {
                boolean refCountLeaked = isAnyStoreRefCountLeaked();
                testUtil.shutdownMiniCluster();
                testUtil = null;
                assertFalse("refCount leaked", refCountLeaked);
            }
        } catch (Exception e) {
            // ignore
        }
    }

    // Tests that client can create and read tables on a fresh HBase cluster with
    // system namespace mapping enabled from the start
	@Test
	public void freshClientsCreateNamespaceMappedSystemTables() throws Exception {
        doSetup(true);
        user1.doAs(new PrivilegedExceptionAction<Void>() {
            @Override
            public Void run() throws Exception {
                createConnection(getClientPropertiesWithSystemMappingEnabled());
                createTable(getClientPropertiesWithSystemMappingEnabled());
                return null;
            }
        });

        hbaseTables = getHBaseTables();
        assertTrue(hbaseTables.containsAll(PHOENIX_NAMESPACE_MAPPED_SYSTEM_TABLES));

        user1.doAs(new PrivilegedExceptionAction<Void>() {
            @Override
            public Void run() throws Exception {
                createConnection(getClientPropertiesWithSystemMappingEnabled());
                readTable(getClientPropertiesWithSystemMappingEnabled());
                return null;
            }
        });

    }

    // Tests that NEWER clients can read tables on HBase cluster after system tables are migrated
	@Test
	public void migrateSystemTablesInExistingCluster() throws Exception {
		doSetup(false);
        user1.doAs(new PrivilegedExceptionAction<Void>() {
            @Override
            public Void run() throws Exception {
                createConnection(getClientPropertiesWithSystemMappingDisabled());
                createTable(getClientPropertiesWithSystemMappingDisabled());
                return null;
            }
        });

        hbaseTables = getHBaseTables();
        assertTrue(hbaseTables.containsAll(PHOENIX_SYSTEM_TABLES));

        user2.doAs(new PrivilegedExceptionAction<Void>() {
            @Override
            public Void run() throws Exception {
                createConnection(getClientPropertiesWithSystemMappingEnabled());
                readTable(getClientPropertiesWithSystemMappingEnabled());
                return null;
            }
        });

        hbaseTables = getHBaseTables();
        assertTrue(hbaseTables.containsAll(PHOENIX_NAMESPACE_MAPPED_SYSTEM_TABLES));
    }

    // Tests that OLDER clients fail after system tables are migrated
    // Clients should be restarted with new properties which are consistent on both client and server
	@Test
	public void oldClientsAfterSystemTableMigrationShouldFail() throws Exception {
        doSetup(true);
        user1.doAs(new PrivilegedExceptionAction<Void>() {
            @Override
            public Void run() throws Exception {
                createConnection(getClientPropertiesWithSystemMappingEnabled());
                return null;
            }
        });

        hbaseTables = getHBaseTables();
        assertTrue(hbaseTables.size() == PHOENIX_NAMESPACE_MAPPED_SYSTEM_TABLES.size());
        assertTrue(hbaseTables.containsAll(PHOENIX_NAMESPACE_MAPPED_SYSTEM_TABLES));

        try {
            user2.doAs(new PrivilegedExceptionAction<Void>() {
                @Override
                public Void run() throws Exception {
                    createConnection(getClientPropertiesWithSystemMappingDisabled());
                    return null;
                }
            });
            fail("Client should not be able to connect to cluster with inconsistent SYSTEM table namespace properties");
        } catch (Exception e) {
            //ignore
        }

        hbaseTables = getHBaseTables();
        assertTrue(hbaseTables.size() == PHOENIX_NAMESPACE_MAPPED_SYSTEM_TABLES.size());
        assertTrue(hbaseTables.containsAll(PHOENIX_NAMESPACE_MAPPED_SYSTEM_TABLES));
    }

    // Tests that only one client can migrate the system table to system namespace
    // Migrate process acquires lock in SYSMUTEX table
	@Test
	public void onlyOneClientCanMigrate() throws Exception {
        doSetup(false);
        user1.doAs(new PrivilegedExceptionAction<Void>() {
            @Override
            public Void run() throws Exception {
                createConnection(getClientPropertiesWithSystemMappingDisabled());
                return null;
            }
        });

        hbaseTables = getHBaseTables();
        assertTrue(hbaseTables.size() == PHOENIX_SYSTEM_TABLES.size());
        assertTrue(hbaseTables.containsAll(PHOENIX_SYSTEM_TABLES));

        user2.doAs(new PrivilegedExceptionAction<Void>() {
            @Override
            public Void run() throws Exception {
                // Acquire Mutex Lock
                changeMutexLock(getClientPropertiesWithSystemMappingDisabled(), true);
                return null;
            }
        });

        hbaseTables = getHBaseTables();
        assertTrue(hbaseTables.size() == PHOENIX_SYSTEM_TABLES.size());
        assertTrue(hbaseTables.containsAll(PHOENIX_SYSTEM_TABLES));

        try {
            user3.doAs(new PrivilegedExceptionAction<Void>() {
                @Override
                public Void run() throws Exception {
                    createConnection(getClientPropertiesWithSystemMappingEnabled());
                    return null;
                }
            });
            fail("Multiple clients should not be able to migrate simultaneously.");
        } catch (Exception e) {
            if(!(e.getCause() instanceof UpgradeInProgressException)) {
                fail("UpgradeInProgressException expected since the user is trying to migrate when SYSMUTEX is locked.");
            }
        }

        hbaseTables = getHBaseTables();
        assertTrue(hbaseTables.size() == PHOENIX_SYSTEM_TABLES.size());
        assertTrue(hbaseTables.containsAll(PHOENIX_SYSTEM_TABLES));

        user2.doAs(new PrivilegedExceptionAction<Void>() {
            @Override
            public Void run() throws Exception {
                // Release Mutex Lock
                changeMutexLock(getClientPropertiesWithSystemMappingDisabled(), false);
                return null;
            }
        });

        hbaseTables = getHBaseTables();
        assertTrue(hbaseTables.size() == PHOENIX_SYSTEM_TABLES.size());
        assertTrue(hbaseTables.containsAll(PHOENIX_SYSTEM_TABLES));

        user3.doAs(new PrivilegedExceptionAction<Void>() {
            @Override
            public Void run() throws Exception {
                createConnection(getClientPropertiesWithSystemMappingEnabled());
                return null;
            }
        });

        hbaseTables = getHBaseTables();
        assertTrue(hbaseTables.size() == PHOENIX_NAMESPACE_MAPPED_SYSTEM_TABLES.size());
        assertTrue(hbaseTables.containsAll(PHOENIX_NAMESPACE_MAPPED_SYSTEM_TABLES));
    }

    private void changeMutexLock(Properties clientProps, boolean acquire) throws SQLException, IOException {
        ConnectionQueryServices services = null;

        try (Connection conn = DriverManager.getConnection(getJdbcUrl(), clientProps)) {
            services = conn.unwrap(PhoenixConnection.class).getQueryServices();
            if(acquire) {
               assertTrue(((ConnectionQueryServicesImpl) services)
                        .acquireUpgradeMutex(MetaDataProtocol.MIN_SYSTEM_TABLE_MIGRATION_TIMESTAMP));
            } else {
                services.deleteMutexCell(null, PhoenixDatabaseMetaData.SYSTEM_CATALOG_SCHEMA,
                    PhoenixDatabaseMetaData.SYSTEM_CATALOG_TABLE, null, null);
            }
        }
    }

    private void enableNamespacesOnServer(Configuration conf, boolean systemMappingEnabled) {
        conf.set(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, Boolean.TRUE.toString());
		conf.set(QueryServices.IS_SYSTEM_TABLE_MAPPED_TO_NAMESPACE,
				systemMappingEnabled ? Boolean.TRUE.toString() : Boolean.FALSE.toString());
    }

    // For PHOENIX-4389 (Flapping tests SystemTablePermissionsIT and MigrateSystemTablesToSystemNamespaceIT)
    private void configureRandomHMasterPort(Configuration conf) {
        // Avoid multiple clusters trying to bind the master's info port (16010)
        conf.setInt(HConstants.MASTER_INFO_PORT, -1);
    }

    private Properties getClientPropertiesWithSystemMappingEnabled() {
        Properties clientProps = new Properties();
        clientProps.setProperty(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, Boolean.TRUE.toString());
        clientProps.setProperty(QueryServices.IS_SYSTEM_TABLE_MAPPED_TO_NAMESPACE, Boolean.TRUE.toString());
        return clientProps;
    }

    private Properties getClientPropertiesWithSystemMappingDisabled() {
        Properties clientProps = new Properties();
        clientProps.setProperty(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, Boolean.TRUE.toString());
        clientProps.setProperty(QueryServices.IS_SYSTEM_TABLE_MAPPED_TO_NAMESPACE, Boolean.FALSE.toString());
        return clientProps;
    }

    private Set<String> getHBaseTables() throws IOException {
        Set<String> tables = new HashSet<>();
        for (TableName tn : testUtil.getHBaseAdmin().listTableNames()) {
            tables.add(tn.getNameAsString());
        }
        return tables;
    }

    private void createConnection(Properties clientProps) throws SQLException, IOException {
        try (Connection conn = DriverManager.getConnection(getJdbcUrl(), clientProps);
             Statement stmt = conn.createStatement();) {
            verifySyscatData(clientProps, conn.toString(), stmt);
        }
    }

    private void createTable(Properties clientProps) throws SQLException {
        try (Connection conn = DriverManager.getConnection(getJdbcUrl(), clientProps);
             Statement stmt = conn.createStatement();) {
            assertFalse(stmt.execute("DROP TABLE IF EXISTS " + TABLE_NAME));
            stmt.execute("CREATE SCHEMA " + SCHEMA_NAME);
            assertFalse(stmt.execute("CREATE TABLE " + TABLE_NAME
                    + "(pk INTEGER not null primary key, data VARCHAR)"));
            try (PreparedStatement pstmt = conn.prepareStatement("UPSERT INTO "
                    + TABLE_NAME + " values(?, ?)")) {
                for (int i = 0; i < NUM_RECORDS; i++) {
                    pstmt.setInt(1, i);
                    pstmt.setString(2, Integer.toString(i));
                    assertEquals(1, pstmt.executeUpdate());
                }
            }
            conn.commit();
        }
    }

    private void readTable(Properties clientProps) throws SQLException {
        try (Connection conn = DriverManager.getConnection(getJdbcUrl(), clientProps);
             Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT pk, data FROM " + TABLE_NAME);
            assertNotNull(rs);
            int i = 0;
            while (rs.next()) {
                assertEquals(i, rs.getInt(1));
                assertEquals(Integer.toString(i), rs.getString(2));
                i++;
            }
            assertEquals(NUM_RECORDS, i);
        }
    }

    private void verifySyscatData(Properties clientProps, String connName, Statement stmt) throws SQLException {
        ResultSet rs = stmt.executeQuery("SELECT * FROM SYSTEM.CATALOG");

        ReadOnlyProps props = new ReadOnlyProps((Map)clientProps);
        boolean systemTablesMapped = SchemaUtil.isNamespaceMappingEnabled(PTableType.SYSTEM, props);
        boolean systemSchemaExists = false;
        Set<String> namespaceMappedSystemTablesSet = new HashSet<>(PHOENIX_NAMESPACE_MAPPED_SYSTEM_TABLES);
        Set<String> systemTablesSet = new HashSet<>(PHOENIX_SYSTEM_TABLES);

        while(rs.next()) {

            if(rs.getString("IS_NAMESPACE_MAPPED") == null) {
                // Check that entry for SYSTEM namespace exists in SYSCAT
                systemSchemaExists = rs.getString("TABLE_SCHEM").equals(PhoenixDatabaseMetaData.SYSTEM_SCHEMA_NAME) ? true : systemSchemaExists;
            } else if (rs.getString("COLUMN_NAME") == null) {
                // Found the intial entry for a table in SYSCAT
                String schemaName = rs.getString("TABLE_SCHEM");
                String tableName = rs.getString("TABLE_NAME");

                if(schemaName.equals(PhoenixDatabaseMetaData.SYSTEM_SCHEMA_NAME)) {
                    if (systemTablesMapped) {
                        namespaceMappedSystemTablesSet.remove(String.valueOf
                                (TableName.valueOf(schemaName + QueryConstants.NAMESPACE_SEPARATOR + tableName)));
                        assertTrue(rs.getString("IS_NAMESPACE_MAPPED").equals(Boolean.TRUE.toString()));
                    } else {
                        systemTablesSet.remove(String.valueOf
                                (TableName.valueOf(schemaName + QueryConstants.NAME_SEPARATOR + tableName)));
                        assertTrue(rs.getString("IS_NAMESPACE_MAPPED").equals(Boolean.FALSE.toString()));
                    }
                }
            }
        }

        if (systemTablesMapped) {
            if (!systemSchemaExists) {
                fail(PhoenixDatabaseMetaData.SYSTEM_SCHEMA_NAME + " entry doesn't exist in SYSTEM.CATALOG table.");
            }
            assertTrue(namespaceMappedSystemTablesSet.isEmpty());
        } else {
            assertTrue(systemTablesSet.isEmpty());
        }
    }

    private String getJdbcUrl() {
        return "jdbc:phoenix:localhost:" + testUtil.getZkCluster().getClientPort() + ":/hbase";
    }

}
