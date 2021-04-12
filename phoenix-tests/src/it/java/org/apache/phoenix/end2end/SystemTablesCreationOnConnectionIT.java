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

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CATALOG_SCHEMA;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_MUTEX_FAMILY_NAME_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_MUTEX_HBASE_TABLE_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TTL_FOR_MUTEX;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.apache.phoenix.query.BaseTest.generateUniqueName;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceNotFoundException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.phoenix.compat.hbase.CompatUtil;
import org.apache.phoenix.coprocessor.MetaDataProtocol;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.UpgradeRequiredException;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.jdbc.PhoenixEmbeddedDriver;
import org.apache.phoenix.jdbc.PhoenixTestDriver;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.ConnectionQueryServicesImpl;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesTestImpl;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.UpgradeUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category(NeedsOwnMiniClusterTest.class)
public class SystemTablesCreationOnConnectionIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(
            SystemTablesCreationOnConnectionIT.class);

    private HBaseTestingUtility testUtil = null;
    private Set<String> hbaseTables;
    private static boolean setOldTimestampToInduceUpgrade = false;
    private static int countUpgradeAttempts;
    // This flag is used to figure out if the SYSCAT schema was actually upgraded or not, based on
    // the timestamp of SYSCAT (different from an upgrade attempt)
    private static int actualSysCatUpgrades;
    private static final String PHOENIX_NAMESPACE_MAPPED_SYSTEM_CATALOG = "SYSTEM:CATALOG";
    private static final String PHOENIX_SYSTEM_CATALOG = "SYSTEM.CATALOG";
    private static final String EXECUTE_UPGRADE_COMMAND = "EXECUTE UPGRADE";
    private static final String MODIFIED_MAX_VERSIONS = "5";
    private static final String CREATE_TABLE_STMT = "CREATE TABLE %s"
            + " (k1 VARCHAR NOT NULL, k2 VARCHAR, CONSTRAINT PK PRIMARY KEY(K1,K2))";
    private static final String SELECT_STMT = "SELECT * FROM %s";
    private static final String DELETE_STMT = "DELETE FROM %s";
    private static final String CREATE_INDEX_STMT = "CREATE INDEX DUMMYIDX ON %s (K1) INCLUDE (K2)";
    private static final String UPSERT_STMT = "UPSERT INTO %s VALUES ('A', 'B')";
    private static final String QUERY_SYSTEM_CATALOG = "SELECT * FROM SYSTEM.CATALOG LIMIT 1";

    private static final Set<String> PHOENIX_SYSTEM_TABLES = new HashSet<>(Arrays.asList(
            "SYSTEM.CATALOG", "SYSTEM.SEQUENCE", "SYSTEM.STATS", "SYSTEM.FUNCTION",
            "SYSTEM.MUTEX", "SYSTEM.LOG", "SYSTEM.CHILD_LINK", "SYSTEM.TASK"));

    private static final Set<String> PHOENIX_NAMESPACE_MAPPED_SYSTEM_TABLES = new HashSet<>(
            Arrays.asList("SYSTEM:CATALOG", "SYSTEM:SEQUENCE", "SYSTEM:STATS", "SYSTEM:FUNCTION",
                    "SYSTEM:MUTEX", "SYSTEM:LOG", "SYSTEM:CHILD_LINK", "SYSTEM:TASK"));

    private static class PhoenixSysCatCreationServices extends ConnectionQueryServicesImpl {

        PhoenixSysCatCreationServices(QueryServices services,
                                      PhoenixEmbeddedDriver.ConnectionInfo connectionInfo, Properties info) {
            super(services, connectionInfo, info);
        }

        @Override
        protected void setUpgradeRequired() {
            super.setUpgradeRequired();
            countUpgradeAttempts++;
        }

        @Override
        protected long getSystemTableVersion() {
            if (setOldTimestampToInduceUpgrade) {
                // Return the next lower version where an upgrade was performed to induce setting
                // the upgradeRequired flag
                return MetaDataProtocol.getPriorUpgradeVersion();
            }
            return MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP;
        }

        @Override
        protected PhoenixConnection upgradeSystemCatalogIfRequired(PhoenixConnection metaConnection,
                                                                   long currentServerSideTableTimeStamp) throws InterruptedException, SQLException,
                TimeoutException, IOException {
            PhoenixConnection newMetaConnection = super.upgradeSystemCatalogIfRequired(
                    metaConnection, currentServerSideTableTimeStamp);
            if (currentServerSideTableTimeStamp < MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP) {
                actualSysCatUpgrades++;
            }
            return newMetaConnection;
        }
    }

    public static class PhoenixSysCatCreationTestingDriver extends PhoenixTestDriver {
        private ConnectionQueryServices cqs;
        private final ReadOnlyProps overrideProps;

        PhoenixSysCatCreationTestingDriver(ReadOnlyProps props) {
            overrideProps = props;
        }

        @Override // public for testing
        public synchronized ConnectionQueryServices getConnectionQueryServices(String url,
                                                                               Properties info) throws SQLException {
            if (cqs == null) {
                cqs = new PhoenixSysCatCreationServices(new QueryServicesTestImpl(getDefaultProps(),
                        overrideProps), ConnectionInfo.create(url), info);
                cqs.init(url, info);
            }
            return cqs;
        }

        // NOTE: Do not use this if you want to try re-establishing a connection from the client
        // using a previously used ConnectionQueryServices instance. This is used only in cases
        // where we need to test server-side changes and don't care about client-side properties
        // set from the init method.
        // Reset the Connection Query Services instance so we can create a new connection to the
        // cluster
        void resetCQS() {
            cqs = null;
        }
    }

    @Before
    public void resetVariables() {
        setOldTimestampToInduceUpgrade = false;
        countUpgradeAttempts = 0;
        actualSysCatUpgrades = 0;
    }

    @After
    public synchronized void tearDownMiniCluster() {
        try {
            if (testUtil != null) {
                boolean isMasterAvailable =
                        testUtil.getHBaseCluster().getMaster() != null;
                boolean refCountLeaked = false;
                if (isMasterAvailable) {
                    refCountLeaked = CompatUtil.isAnyStoreRefCountLeaked(
                            testUtil.getAdmin());
                }
                testUtil.shutdownMiniCluster();
                testUtil = null;
                assertFalse("refCount leaked", refCountLeaked);
            }
        } catch (Exception e) {
            // ignore
        }
    }

    // Conditions: isDoNotUpgradePropSet is true
    // Expected: We do not create SYSTEM.CATALOG even if this is the first connection to the server
    @Test
    public void testFirstConnectionDoNotUpgradePropSet() throws Exception {
        startMiniClusterWithToggleNamespaceMapping(Boolean.FALSE.toString());
        Properties propsDoNotUpgradePropSet = new Properties();
        // Set doNotUpgradeProperty to true
        UpgradeUtil.doNotUpgradeOnFirstConnection(propsDoNotUpgradePropSet);
        PhoenixSysCatCreationTestingDriver driver =
                new PhoenixSysCatCreationTestingDriver(ReadOnlyProps.EMPTY_PROPS);

        driver.getConnectionQueryServices(getJdbcUrl(), propsDoNotUpgradePropSet);
        hbaseTables = getHBaseTables();
        assertFalse(hbaseTables.contains(PHOENIX_SYSTEM_CATALOG) ||
                hbaseTables.contains(PHOENIX_NAMESPACE_MAPPED_SYSTEM_CATALOG));
        assertEquals(0, hbaseTables.size());
        assertEquals(1, countUpgradeAttempts);
    }


    /********************* Testing SYSTEM.CATALOG/SYSTEM:CATALOG creation/upgrade behavior for subsequent connections *********************/

    // We are ignoring this test because we aren't testing SYSCAT timestamp anymore if
    // "DoNotUpgrade" config is set to true
    @Ignore
    // Conditions: server-side namespace mapping is enabled, the first connection to the server will
    // create all namespace mapped SYSTEM tables i.e. SYSTEM:.*, the SYSTEM:CATALOG timestamp at
    // creation is purposefully set to be < MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP.
    // The subsequent connection has client-side namespace mapping enabled
    // Expected: An upgrade is attempted when the second client connects to the server
    @Test
    public void testUpgradeAttempted() throws Exception {
        setOldTimestampToInduceUpgrade = true;
        PhoenixSysCatCreationTestingDriver driver = firstConnNSMappingServerEnabledClientEnabled();
        driver.resetCQS();
        Properties clientProps = getClientProperties(true, true);
        setOldTimestampToInduceUpgrade = false;
        driver.getConnectionQueryServices(getJdbcUrl(), clientProps);
        // There should be no new tables
        assertEquals(hbaseTables, getHBaseTables());
        // Since we set an old timestamp on purpose when creating SYSTEM:CATALOG,
        // the second connection attempts to upgrade it
        assertEquals(1, countUpgradeAttempts);
        assertEquals(1, actualSysCatUpgrades);
    }

    // Conditions: server-side namespace mapping is enabled, the first connection to the server will
    // create all namespace mapped SYSTEM tables i.e. SYSTEM:.*, the SYSTEM:CATALOG timestamp at
    // creation is purposefully set to be < MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP.
    // The subsequent connection has client-side namespace mapping enabled
    // Expected: An upgrade is attempted when the second client connects to the server, but this
    // fails since isDoNotUpgradePropSet is set to true. We later run EXECUTE UPGRADE manually
    @Test
    public void testUpgradeNotAllowed() throws Exception {
        setOldTimestampToInduceUpgrade = true;
        PhoenixSysCatCreationTestingDriver driver = firstConnNSMappingServerEnabledClientEnabled();
        driver.resetCQS();
        Properties clientProps = getClientProperties(true, true);
        UpgradeUtil.doNotUpgradeOnFirstConnection(clientProps);
        setOldTimestampToInduceUpgrade = false;
        try {
            driver.getConnectionQueryServices(getJdbcUrl(), clientProps);
        } catch (Exception e) {
            assertTrue(e instanceof UpgradeRequiredException);
        }
        // There should be no new tables
        assertEquals(hbaseTables, getHBaseTables());
        // Since we set an old timestamp on purpose when creating SYSTEM:CATALOG, the second
        // connection attempts to upgrade it
        assertEquals(1, countUpgradeAttempts);
        // This connection is unable to actually upgrade SYSTEM:CATALOG due to isDoNotUpgradePropSet
        assertEquals(0, actualSysCatUpgrades);
        try (Connection conn = driver.getConnectionQueryServices(getJdbcUrl(), new Properties())
                .connect(getJdbcUrl(), new Properties()); Statement stmt = conn.createStatement()) {
            stmt.execute(EXECUTE_UPGRADE_COMMAND);
            // Actually upgraded SYSTEM:CATALOG
            assertEquals(1, actualSysCatUpgrades);
        }
    }

    // Conditions: server-side namespace mapping is enabled, the first connection to the server will
    // create unmapped SYSTEM tables SYSTEM\..* whose timestamp at creation is purposefully set to
    // be < MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP.
    // The second connection has client-side namespace mapping enabled and system table to system
    // namespace mapping enabled
    // Expected: We will migrate all SYSTEM\..* tables to the SYSTEM namespace and also upgrade
    // SYSTEM:CATALOG
    @Test
    public void testMigrateToSystemNamespaceAndUpgradeSysCat() throws Exception {
        setOldTimestampToInduceUpgrade = true;
        PhoenixSysCatCreationTestingDriver driver =
                firstConnNSMappingServerEnabledClientEnabledMappingDisabled();
        driver.resetCQS();
        setOldTimestampToInduceUpgrade = false;
        Properties clientProps = getClientProperties(true, true);
        driver.getConnectionQueryServices(getJdbcUrl(), clientProps);
        hbaseTables = getHBaseTables();
        assertEquals(PHOENIX_NAMESPACE_MAPPED_SYSTEM_TABLES, hbaseTables);
        assertEquals(1, countUpgradeAttempts);
        assertEquals(1, actualSysCatUpgrades);
    }

    // Conditions: server-side namespace mapping is enabled, the first connection to the server will
    // create all namespace mapped SYSTEM tables i.e. SYSTEM:.*, the second connection has
    // client-side namespace mapping disabled
    // Expected: Throw Inconsistent namespace mapping exception from ensureTableCreated
    @Test
    public void testTablesExistInconsistentNSMappingFails() throws Exception {
        PhoenixSysCatCreationTestingDriver driver = firstConnNSMappingServerEnabledClientEnabled();
        driver.resetCQS();
        Properties clientProps = getClientProperties(false, false);
        try {
            driver.getConnectionQueryServices(getJdbcUrl(), clientProps);
            fail("Client should not be able to connect to cluster with inconsistent client-server "
                    + "namespace mapping properties");
        } catch (SQLException sqlE) {
            assertEquals(SQLExceptionCode.INCONSISTENT_NAMESPACE_MAPPING_PROPERTIES.getErrorCode(),
                    sqlE.getErrorCode());
        }
        hbaseTables = getHBaseTables();
        assertEquals(PHOENIX_NAMESPACE_MAPPED_SYSTEM_TABLES, hbaseTables);
        assertEquals(0, countUpgradeAttempts);
    }

    // Conditions: server-side namespace mapping is enabled, the first connection to the server will
    // not create any SYSTEM tables. The second connection has client-side namespace mapping enabled
    // Expected: We create SYSTEM:.* tables
    @Test
    public void testIncompatibleNSMappingServerEnabledConnectionFails() throws Exception {
        PhoenixSysCatCreationTestingDriver driver = firstConnNSMappingServerEnabledClientDisabled();
        driver.resetCQS();
        // now try a client with ns mapping enabled
        Properties clientProps = getClientProperties(true, true);
        try (Connection conn = driver.getConnectionQueryServices(getJdbcUrl(), clientProps)
                .connect(getJdbcUrl(), new Properties())) {
            hbaseTables = getHBaseTables();
            assertEquals(PHOENIX_NAMESPACE_MAPPED_SYSTEM_TABLES, hbaseTables);
            assertEquals(0, countUpgradeAttempts);

            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery(QUERY_SYSTEM_CATALOG);
                // Tests that SYSTEM:CATALOG contains necessary metadata rows for itself
                // (See PHOENIX-5302)
                assertTrue(rs.next());
            }
        }
    }

    // Conditions: server-side namespace mapping is disabled, the first connection to the server
    // will create all unmapped SYSTEM tables i.e. SYSTEM\..*, the second connection has client-side
    // namespace mapping enabled
    // Expected: Throw Inconsistent namespace mapping exception when you check client-server
    // compatibility
    //
    // Then another connection has client-side namespace mapping disabled
    // Expected: All SYSTEM\..* tables exist and no upgrade is required
    @Test
    public void testSysTablesExistNSMappingDisabled() throws Exception {
        PhoenixSysCatCreationTestingDriver driver =
                firstConnNSMappingServerDisabledClientDisabled();
        driver.resetCQS();
        Properties clientProps = getClientProperties(true, true);
        try {
            driver.getConnectionQueryServices(getJdbcUrl(), clientProps);
            fail("Client should not be able to connect to cluster with inconsistent client-server "
                    + "namespace mapping properties");
        } catch (SQLException sqlE) {
            assertEquals(SQLExceptionCode.INCONSISTENT_NAMESPACE_MAPPING_PROPERTIES.getErrorCode(),
                    sqlE.getErrorCode());
        }
        hbaseTables = getHBaseTables();
        assertEquals(PHOENIX_SYSTEM_TABLES, hbaseTables);
        assertEquals(0, countUpgradeAttempts);

        driver.resetCQS();
        clientProps = getClientProperties(false, false);
        driver.getConnectionQueryServices(getJdbcUrl(), clientProps);
        hbaseTables = getHBaseTables();
        assertEquals(PHOENIX_SYSTEM_TABLES, hbaseTables);
        assertEquals(0, countUpgradeAttempts);
    }

    // Conditions: server-side namespace mapping is disabled, the first connection to the server
    // will not create any SYSTEM tables. The second connection has client-side namespace mapping
    // disabled
    // Expected: The second connection should create all SYSTEM.* tables
    @Test
    public void testIncompatibleNSMappingServerDisabledConnectionFails() throws Exception {
        PhoenixSysCatCreationTestingDriver driver = firstConnNSMappingServerDisabledClientEnabled();
        driver.resetCQS();
        // now try a client with ns mapping disabled
        Properties clientProps = getClientProperties(false, false);
        try (Connection conn = driver.getConnectionQueryServices(getJdbcUrl(), clientProps)
                .connect(getJdbcUrl(), new Properties())) {
            hbaseTables = getHBaseTables();
            assertEquals(PHOENIX_SYSTEM_TABLES, hbaseTables);
            assertEquals(0, countUpgradeAttempts);

            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery(QUERY_SYSTEM_CATALOG);
                // Tests that SYSTEM.CATALOG contains necessary metadata rows for itself
                // (See PHOENIX-5302)
                assertTrue(rs.next());
            }
        }
    }

    // Conditions: The first connection creates all SYSTEM tables via "EXECUTE UPGRADE" since
    // auto-upgrade is disabled and the same client alters HBase metadata for SYSTEM.CATALOG
    // Expected: Another client connection (with a new ConnectionQueryServices instance) made to
    // the server does not revert the metadata change
    @Test
    public void testMetadataAlterRemainsAutoUpgradeDisabled() throws Exception {
        PhoenixSysCatCreationTestingDriver driver = firstConnAutoUpgradeToggle(false);
        assertEquals(Integer.parseInt(MODIFIED_MAX_VERSIONS), verifyModifiedTableMetadata(driver));
    }

    // Conditions: The first connection creates all SYSTEM tables (auto-upgrade is enabled) and
    // the same client alters HBase metadata for SYSTEM.CATALOG
    // Expected: Another client connection (with a new ConnectionQueryServices instance) made to
    // the server does not revert the metadata change
    @Test
    public void testMetadataAlterRemainsAutoUpgradeEnabled() throws Exception {
        PhoenixSysCatCreationTestingDriver driver = firstConnAutoUpgradeToggle(true);
        assertEquals(Integer.parseInt(MODIFIED_MAX_VERSIONS), verifyModifiedTableMetadata(driver));
    }

    // Test the case when an end-user uses the vanilla PhoenixDriver to create a connection and a
    // requirement for upgrade is detected. In this case, the user should get a connection on which
    // they are only able to run "EXECUTE UPGRADE"
    @Test
    public void testExecuteUpgradeSameConnWithPhoenixDriver() throws Exception {
        // Register the vanilla PhoenixDriver
        DriverManager.registerDriver(PhoenixDriver.INSTANCE);
        startMiniClusterWithToggleNamespaceMapping(Boolean.FALSE.toString());
        Properties propsDoNotUpgradeSet = new Properties();
        // Set doNotUpgradeProperty to true
        UpgradeUtil.doNotUpgradeOnFirstConnection(propsDoNotUpgradeSet);

        try (Connection conn = DriverManager.getConnection(getJdbcUrl(), propsDoNotUpgradeSet)) {
            hbaseTables = getHBaseTables();
            assertFalse(hbaseTables.contains(PHOENIX_SYSTEM_CATALOG)
                    || hbaseTables.contains(PHOENIX_NAMESPACE_MAPPED_SYSTEM_CATALOG));
            assertEquals(0, hbaseTables.size());

            // Test that we are unable to run any other queries using this connection
            // until we upgrade
            final String tableName = generateUniqueName();
            try (Statement stmt = conn.createStatement()) {
                stmt.execute(String.format(CREATE_TABLE_STMT, tableName));
                fail("CREATE TABLE should have failed with UpgradeRequiredException");
            } catch (UpgradeRequiredException expected) {

            }
            try (Statement stmt = conn.createStatement()) {
                stmt.execute(String.format(SELECT_STMT, tableName));
                fail("SELECT should have failed with UpgradeRequiredException");
            } catch (UpgradeRequiredException expected) {

            }
            try (Statement stmt = conn.createStatement()) {
                stmt.execute(String.format(DELETE_STMT, tableName));
                fail("DELETE should have failed with UpgradeRequiredException");
            } catch (UpgradeRequiredException expected) {

            }
            try (Statement stmt = conn.createStatement()) {
                stmt.execute(String.format(CREATE_INDEX_STMT, tableName));
                fail("CREATE INDEX should have failed with UpgradeRequiredException");
            } catch (UpgradeRequiredException expected) {

            }
            try (Statement stmt = conn.createStatement()) {
                stmt.execute(String.format(UPSERT_STMT, tableName));
                fail("UPSERT VALUES should have failed with UpgradeRequiredException");
            } catch (UpgradeRequiredException expected) {

            }

            // Now run the upgrade command. All SYSTEM tables should be created
            try (Statement stmt = conn.createStatement()) {
                stmt.execute(EXECUTE_UPGRADE_COMMAND);
            }
            hbaseTables = getHBaseTables();
            assertEquals(PHOENIX_SYSTEM_TABLES, hbaseTables);

            // Now we can run any other query/mutation using this connection object
            try (Statement stmt = conn.createStatement()) {
                stmt.execute(String.format(CREATE_TABLE_STMT, tableName));
            }
            try (Statement stmt = conn.createStatement()) {
                stmt.execute(String.format(SELECT_STMT, tableName));
            }
            try (Statement stmt = conn.createStatement()) {
                stmt.execute(String.format(DELETE_STMT, tableName));
            }
            try (Statement stmt = conn.createStatement()) {
                stmt.execute(String.format(CREATE_INDEX_STMT, tableName));
            }
            try (Statement stmt = conn.createStatement()) {
                stmt.execute(String.format(UPSERT_STMT, tableName));
            }
        }
    }

    // Test that whenever a client connects to a fresh cluster, SYSTEM.MUTEX is created with a
    // TTL as defined by TTL_FOR_MUTEX
    @Test
    public void testSysMutexHasCorrectTTL() throws Exception {
        // Register the vanilla PhoenixDriver
        DriverManager.registerDriver(PhoenixDriver.INSTANCE);
        startMiniClusterWithToggleNamespaceMapping(Boolean.FALSE.toString());
        try (Connection ignored = DriverManager.getConnection(getJdbcUrl());
             HBaseAdmin admin = testUtil.getHBaseAdmin()) {
            HTableDescriptor htd = admin.getTableDescriptor(SYSTEM_MUTEX_HBASE_TABLE_NAME);
            HColumnDescriptor hColDesc = htd.getFamily(SYSTEM_MUTEX_FAMILY_NAME_BYTES);
            assertEquals("Did not find the correct TTL for SYSTEM.MUTEX", TTL_FOR_MUTEX,
                    hColDesc.getTimeToLive());
        }
    }

    // Conditions: doNotUpgrade config should be set and HMaster should be stopped
    // Expected: Even if HMaster is stopped we should be able to get phoenix
    // connection, knowing that all system tables exists already.
    @Test
    public void testDoNotUpgradePropSet() throws Exception {
        String tableName = "HBASE_SYNTH_TEST";
        startMiniClusterWithToggleNamespaceMapping(Boolean.FALSE.toString());
        Properties propsDoNotUpgradePropSet = new Properties();
        // Create a dummy connection to make sure we have all system tables in place
        try (Connection con1 = DriverManager.getConnection(getJdbcUrl(), propsDoNotUpgradePropSet);
             Statement stmt1 = con1.createStatement()) {
            String ddl = "CREATE TABLE " + tableName + " (PK1 VARCHAR not null, " +
                    "PK2 VARCHAR not null, COL1 varchar, COL2 varchar "
                    + "CONSTRAINT pk PRIMARY KEY(PK1,PK2))";
            stmt1.execute(ddl);
            stmt1.execute(
                    "UPSERT INTO " + tableName + " values ('pk1','pk2','c1','c2')");
            con1.commit();
            // Stop HMaster to check if we can create connection without active HMaster
            testUtil.getMiniHBaseCluster().getMaster().stopMaster();
            // Set doNotUpgradeProperty to true
            UpgradeUtil.doNotUpgradeOnFirstConnection(propsDoNotUpgradePropSet);
            try (Connection con2 = DriverManager.getConnection(getJdbcUrl(), propsDoNotUpgradePropSet);
                 Statement stmt2 = con2.createStatement();
                 ResultSet rs = stmt2.executeQuery("select * from " + tableName)) {
                assertTrue(rs.next());
                assertEquals("pk1", rs.getString(1));
                assertEquals("pk2", rs.getString(2));
                assertFalse(rs.next());
            }
        } finally {
            testUtil.getMiniHBaseCluster().startMaster();
        }
    }

    /**
     * Return all created HBase tables
     *
     * @return Set of HBase table name strings
     * @throws IOException if there is a problem listing all HBase tables
     */
    private Set<String> getHBaseTables() throws IOException {
        Set<String> tables = new HashSet<>();
        for (TableName tn : testUtil.getAdmin().listTableNames()) {
            tables.add(tn.getNameAsString());
        }
        return tables;
    }

    // Check if the SYSTEM namespace has been created
    private boolean isSystemNamespaceCreated() throws IOException {
        try {
            testUtil.getAdmin().getNamespaceDescriptor(SYSTEM_CATALOG_SCHEMA);
        } catch (NamespaceNotFoundException ex) {
            return false;
        }
        return true;
    }

    /**
     * Alter the table metadata and return modified value
     *
     * @param driver testing Phoenix driver
     * @return value of VERSIONS option for the table
     * @throws Exception if there is an error modifying the HBase metadata for SYSTEM.CATALOG
     */
    private int verifyModifiedTableMetadata(PhoenixSysCatCreationTestingDriver driver)
            throws Exception {
        // Modify table metadata
        try (Connection conn = driver.getConnectionQueryServices(getJdbcUrl(), new Properties())
                .connect(getJdbcUrl(), new Properties()); Statement stmt = conn.createStatement()) {
            stmt.execute("ALTER TABLE " + PHOENIX_SYSTEM_CATALOG + " SET VERSIONS = "
                    + MODIFIED_MAX_VERSIONS);
        }

        // Connect via a client that creates a new ConnectionQueryServices instance
        driver.resetCQS();
        try (PhoenixConnection ignored = driver.getConnectionQueryServices(
                getJdbcUrl(), new Properties()).connect(getJdbcUrl(), new Properties())) {
            // do nothing
        }
        HTableDescriptor descriptor = testUtil.getHBaseAdmin()
                .getTableDescriptor(TableName.valueOf(PHOENIX_SYSTEM_CATALOG));
        return descriptor.getFamily(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES).getMaxVersions();
    }

    /**
     * Start the mini-cluster with server-side namespace mapping property specified
     *
     * @param isNamespaceMappingEnabled true if namespace mapping is enabled
     * @throws Exception if there is an error starting the minicluster
     */
    private void startMiniClusterWithToggleNamespaceMapping(String isNamespaceMappingEnabled)
            throws Exception {
        testUtil = new HBaseTestingUtility();
        Configuration conf = testUtil.getConfiguration();
        conf.set(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, isNamespaceMappingEnabled);
        // Avoid multiple clusters trying to bind to the master's info port (16010)
        conf.setInt(HConstants.MASTER_INFO_PORT, -1);
        testUtil.startMiniCluster(1);
    }

    /**
     * Get the connection string for the mini-cluster
     *
     * @return Phoenix connection string
     */
    private String getJdbcUrl() {
        return "jdbc:phoenix:localhost:" + testUtil.getZkCluster().getClientPort() + ":/hbase";
    }

    /**
     * Set namespace mapping related properties for the client connection
     *
     * @param nsMappingEnabled          true if namespace mapping is enabled
     * @param systemTableMappingEnabled true if we are mapping SYSTEM tables to their own namespace
     * @return Properties object
     */
    private Properties getClientProperties(boolean nsMappingEnabled,
                                           boolean systemTableMappingEnabled) {
        Properties clientProps = new Properties();
        clientProps.setProperty(QueryServices.IS_NAMESPACE_MAPPING_ENABLED,
                Boolean.valueOf(nsMappingEnabled).toString());
        clientProps.setProperty(QueryServices.IS_SYSTEM_TABLE_MAPPED_TO_NAMESPACE,
                Boolean.valueOf(systemTableMappingEnabled).toString());
        return clientProps;
    }

    /**
     * Initiate the first connection to the server with provided auto-upgrade property
     *
     * @param isAutoUpgradeEnabled true if auto-upgrade is enabled
     * @return Phoenix JDBC driver
     * @throws Exception if starting the minicluster fails
     */
    private PhoenixSysCatCreationTestingDriver firstConnAutoUpgradeToggle(
            boolean isAutoUpgradeEnabled) throws Exception {
        if (isAutoUpgradeEnabled) {
            return firstConnNSMappingServerDisabledClientDisabled();
        }
        return firstConnAutoUpgradeDisabled();
    }

    // Conditions: isAutoUpgradeEnabled is false
    // Expected: We do not create SYSTEM.CATALOG even if this is the first connection to the server.
    // Later, when we manually run "EXECUTE UPGRADE", we create SYSTEM tables
    private PhoenixSysCatCreationTestingDriver firstConnAutoUpgradeDisabled() throws Exception {
        startMiniClusterWithToggleNamespaceMapping(Boolean.FALSE.toString());
        Map<String, String> props = new HashMap<>();
        // Note that the isAutoUpgradeEnabled property is set when instantiating connection
        // query services, not during init
        props.put(QueryServices.AUTO_UPGRADE_ENABLED, Boolean.FALSE.toString());
        ReadOnlyProps readOnlyProps = new ReadOnlyProps(props);
        PhoenixSysCatCreationTestingDriver driver =
                new PhoenixSysCatCreationTestingDriver(readOnlyProps);

        // We should be able to get a connection, however upgradeRequired should be set so that we
        // are not allowed to run any query/mutation until "EXECUTE UPGRADE" has been run
        try (Connection conn = driver.getConnectionQueryServices(getJdbcUrl(), new Properties())
                .connect(getJdbcUrl(), new Properties())) {
            hbaseTables = getHBaseTables();
            assertFalse(hbaseTables.contains(PHOENIX_SYSTEM_CATALOG) ||
                    hbaseTables.contains(PHOENIX_NAMESPACE_MAPPED_SYSTEM_CATALOG));
            assertEquals(0, hbaseTables.size());
            assertEquals(1, countUpgradeAttempts);

            // We use the same connection to run "EXECUTE UPGRADE"
            try (Statement stmt = conn.createStatement()) {
                stmt.execute(EXECUTE_UPGRADE_COMMAND);
            }
        }
        hbaseTables = getHBaseTables();
        assertEquals(PHOENIX_SYSTEM_TABLES, hbaseTables);
        return driver;
    }

    // Conditions: server-side namespace mapping is enabled, client-side namespace mapping is
    // enabled and system tables are to be mapped to the SYSTEM namespace.
    // Expected: If this is the first connection to the server, we should be able to create all
    // namespace mapped system tables i.e. SYSTEM:.*
    private PhoenixSysCatCreationTestingDriver firstConnNSMappingServerEnabledClientEnabled()
            throws Exception {
        startMiniClusterWithToggleNamespaceMapping(Boolean.TRUE.toString());
        Properties clientProps = getClientProperties(true, true);
        PhoenixSysCatCreationTestingDriver driver =
                new PhoenixSysCatCreationTestingDriver(ReadOnlyProps.EMPTY_PROPS);
        driver.getConnectionQueryServices(getJdbcUrl(), clientProps);
        hbaseTables = getHBaseTables();
        assertEquals(PHOENIX_NAMESPACE_MAPPED_SYSTEM_TABLES, hbaseTables);
        assertEquals(0, countUpgradeAttempts);
        assertTrue(isSystemNamespaceCreated());
        return driver;
    }

    // Conditions: server-side namespace mapping is enabled, client-side namespace mapping is
    // enabled, but mapping SYSTEM tables to the SYSTEM namespace is disabled
    // Expected: If this is the first connection to the server, we will create unmapped
    // SYSTEM tables i.e. SYSTEM\..*
    private PhoenixSysCatCreationTestingDriver
    firstConnNSMappingServerEnabledClientEnabledMappingDisabled() throws Exception {
        startMiniClusterWithToggleNamespaceMapping(Boolean.TRUE.toString());
        // client-side namespace mapping is enabled, but mapping SYSTEM tables to SYSTEM namespace
        // is not
        Properties clientProps = getClientProperties(true, false);
        PhoenixSysCatCreationTestingDriver driver =
                new PhoenixSysCatCreationTestingDriver(ReadOnlyProps.EMPTY_PROPS);
        driver.getConnectionQueryServices(getJdbcUrl(), clientProps);
        hbaseTables = getHBaseTables();
        assertEquals(PHOENIX_SYSTEM_TABLES, hbaseTables);
        assertEquals(0, countUpgradeAttempts);
        assertFalse(isSystemNamespaceCreated());
        return driver;
    }

    // Conditions: server-side namespace mapping is enabled, client-side namespace mapping is
    // disabled
    // Expected: Since this is the first connection to the server, we will immediately
    // throw an exception for inconsistent namespace mapping without creating any SYSTEM tables
    private PhoenixSysCatCreationTestingDriver firstConnNSMappingServerEnabledClientDisabled()
            throws Exception {
        startMiniClusterWithToggleNamespaceMapping(Boolean.TRUE.toString());
        Properties clientProps = getClientProperties(false, false);
        PhoenixSysCatCreationTestingDriver driver =
                new PhoenixSysCatCreationTestingDriver(ReadOnlyProps.EMPTY_PROPS);
        try {
            driver.getConnectionQueryServices(getJdbcUrl(), clientProps);
            fail("Client should not be able to connect to cluster with inconsistent client-server "
                    + "namespace mapping properties");
        } catch (SQLException sqlE) {
            assertEquals(SQLExceptionCode.INCONSISTENT_NAMESPACE_MAPPING_PROPERTIES.getErrorCode(),
                    sqlE.getErrorCode());
        }
        hbaseTables = getHBaseTables();
        assertEquals(0, hbaseTables.size());
        assertEquals(0, countUpgradeAttempts);
        return driver;
    }

    // Conditions: server-side namespace mapping is disabled, client-side namespace mapping is
    // enabled
    // Expected: Since this is the first connection to the server, we will immediately throw an
    // exception for inconsistent namespace mapping without creating any SYSTEM tables or
    // SYSTEM namespace
    private PhoenixSysCatCreationTestingDriver firstConnNSMappingServerDisabledClientEnabled()
            throws Exception {
        startMiniClusterWithToggleNamespaceMapping(Boolean.FALSE.toString());
        Properties clientProps = getClientProperties(true, true);
        PhoenixSysCatCreationTestingDriver driver =
                new PhoenixSysCatCreationTestingDriver(ReadOnlyProps.EMPTY_PROPS);
        try {
            driver.getConnectionQueryServices(getJdbcUrl(), clientProps);
            fail("Client should not be able to connect to cluster with inconsistent client-server "
                    + "namespace mapping properties");
        } catch (SQLException sqlE) {
            assertEquals(SQLExceptionCode.INCONSISTENT_NAMESPACE_MAPPING_PROPERTIES.getErrorCode(),
                    sqlE.getErrorCode());
        }
        hbaseTables = getHBaseTables();
        assertEquals(0, hbaseTables.size());
        assertEquals(0, countUpgradeAttempts);
        assertFalse(isSystemNamespaceCreated());
        return driver;
    }

    // Conditions: server-side namespace mapping is disabled, client-side namespace mapping is
    // disabled
    // Expected: Since this is the first connection to the server and auto-upgrade is enabled
    // by default, we will create all SYSTEM\..* tables
    private PhoenixSysCatCreationTestingDriver firstConnNSMappingServerDisabledClientDisabled()
            throws Exception {
        startMiniClusterWithToggleNamespaceMapping(Boolean.FALSE.toString());
        Properties clientProps = getClientProperties(false, false);
        PhoenixSysCatCreationTestingDriver driver =
                new PhoenixSysCatCreationTestingDriver(ReadOnlyProps.EMPTY_PROPS);
        driver.getConnectionQueryServices(getJdbcUrl(), clientProps);
        hbaseTables = getHBaseTables();
        assertEquals(PHOENIX_SYSTEM_TABLES, hbaseTables);
        assertEquals(0, countUpgradeAttempts);
        assertFalse(isSystemNamespaceCreated());
        return driver;
    }
}