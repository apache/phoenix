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
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.phoenix.coprocessor.MetaDataProtocol;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.UpgradeRequiredException;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixEmbeddedDriver;
import org.apache.phoenix.jdbc.PhoenixTestDriver;
import org.apache.phoenix.query.*;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.UpgradeUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.*;

@Category(NeedsOwnMiniClusterTest.class)
public class SystemCatalogCreationOnConnectionIT {
    private HBaseTestingUtility testUtil = null;
    private Set<String> hbaseTables;
    private static boolean setOldTimestampToInduceUpgrade = false;
    private static int countUpgradeAttempts;
    // This flag is used to figure out if the SYSCAT schema was actually upgraded or not, based on the timestamp of SYSCAT
    // (different from an upgrade attempt)
    private static int actualSysCatUpgrades;
    private static final String PHOENIX_NAMESPACE_MAPPED_SYSTEM_CATALOG = "SYSTEM:CATALOG";
    private static final String PHOENIX_SYSTEM_CATALOG = "SYSTEM.CATALOG";
    private static final String EXECUTE_UPGRADE_COMMAND = "EXECUTE UPGRADE";
    private static final String MODIFIED_MAX_VERSIONS ="5";

    private static final Set<String> PHOENIX_SYSTEM_TABLES = new HashSet<>(Arrays.asList(
      "SYSTEM.CATALOG", "SYSTEM.SEQUENCE", "SYSTEM.STATS", "SYSTEM.FUNCTION",
      "SYSTEM.MUTEX", "SYSTEM.LOG"));

    private static final Set<String> PHOENIX_NAMESPACE_MAPPED_SYSTEM_TABLES = new HashSet<>(
      Arrays.asList("SYSTEM:CATALOG", "SYSTEM:SEQUENCE", "SYSTEM:STATS", "SYSTEM:FUNCTION",
        "SYSTEM:MUTEX", "SYSTEM:LOG"));

    private static class PhoenixSysCatCreationServices extends ConnectionQueryServicesImpl {

        public PhoenixSysCatCreationServices(QueryServices services, PhoenixEmbeddedDriver.ConnectionInfo connectionInfo, Properties info) {
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
                // Return the next lower version where an upgrade was performed to induce setting the upgradeRequired flag
                return MetaDataProtocol.getPriorUpgradeVersion();
            }
            return MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP;
        }

        @Override
        protected PhoenixConnection upgradeSystemCatalogIfRequired(PhoenixConnection metaConnection,
          long currentServerSideTableTimeStamp) throws InterruptedException, SQLException, TimeoutException, IOException {
            PhoenixConnection newMetaConnection = super.upgradeSystemCatalogIfRequired(metaConnection, currentServerSideTableTimeStamp);
            if (currentServerSideTableTimeStamp < MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP) {
                actualSysCatUpgrades++;
            }
            return newMetaConnection;
        }
    }

    public static class PhoenixSysCatCreationTestingDriver extends PhoenixTestDriver {
        private ConnectionQueryServices cqs;
        private final ReadOnlyProps overrideProps;

        public PhoenixSysCatCreationTestingDriver(ReadOnlyProps props) {
            overrideProps = props;
        }

        @Override // public for testing
        public synchronized ConnectionQueryServices getConnectionQueryServices(String url, Properties info) throws SQLException {
            if (cqs == null) {
                cqs = new PhoenixSysCatCreationServices(new QueryServicesTestImpl(getDefaultProps(), overrideProps), ConnectionInfo.create(url), info);
                cqs.init(url, info);
            }
            return cqs;
        }

        // NOTE: Do not use this if you want to try re-establishing a connection from the client using a previously
        // used ConnectionQueryServices instance. This is used only in cases where we need to test server-side
        // changes and don't care about client-side properties set from the init method.
        // Reset the Connection Query Services instance so we can create a new connection to the cluster
        public void resetCQS() {
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
    public void tearDownMiniCluster() {
        try {
            if (testUtil != null) {
                testUtil.shutdownMiniCluster();
                testUtil = null;
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
        SystemCatalogCreationOnConnectionIT.PhoenixSysCatCreationTestingDriver driver =
          new SystemCatalogCreationOnConnectionIT.PhoenixSysCatCreationTestingDriver(ReadOnlyProps.EMPTY_PROPS);
        try {
            driver.getConnectionQueryServices(getJdbcUrl(), propsDoNotUpgradePropSet);
            fail("Client should not be able to create SYSTEM.CATALOG since we set the doNotUpgrade property");
        } catch (Exception e) {
            assertTrue(e instanceof UpgradeRequiredException);
        }
        hbaseTables = getHBaseTables();
        assertFalse(hbaseTables.contains(PHOENIX_SYSTEM_CATALOG) || hbaseTables.contains(PHOENIX_NAMESPACE_MAPPED_SYSTEM_CATALOG));
        assertTrue(hbaseTables.size() == 0);
        assertEquals(1, countUpgradeAttempts);
    }


    /********************* Testing SYSTEM.CATALOG/SYSTEM:CATALOG creation/upgrade behavior for subsequent connections *********************/


    // Conditions: server-side namespace mapping is enabled, the first connection to the server will create unmapped
    // SYSTEM tables i.e. SYSTEM\..*, the second connection has client-side namespace mapping enabled and
    // system table to system namespace mapping enabled
    // Expected: We will migrate all SYSTEM\..* tables to the SYSTEM namespace
    @Test
    public void testMigrateToSystemNamespace() throws Exception {
        SystemCatalogCreationOnConnectionIT.PhoenixSysCatCreationTestingDriver driver =
          firstConnectionNSMappingServerEnabledClientEnabledMappingDisabled();
        driver.resetCQS();
        // Setting this to true to effect migration of SYSTEM tables to the SYSTEM namespace
        Properties clientProps = getClientProperties(true, true);
        driver.getConnectionQueryServices(getJdbcUrl(), clientProps);
        hbaseTables = getHBaseTables();
        assertEquals(PHOENIX_NAMESPACE_MAPPED_SYSTEM_TABLES, hbaseTables);
        assertEquals(1, countUpgradeAttempts);
    }

    // Conditions: server-side namespace mapping is enabled, the first connection to the server will create all namespace
    // mapped SYSTEM tables i.e. SYSTEM:.*, the SYSTEM:CATALOG timestamp at creation is purposefully set to be <
    // MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP. The subsequent connection has client-side namespace mapping enabled
    // Expected: An upgrade is attempted when the second client connects to the server
    @Test
    public void testUpgradeAttempted() throws Exception {
        setOldTimestampToInduceUpgrade = true;
        SystemCatalogCreationOnConnectionIT.PhoenixSysCatCreationTestingDriver driver =
          firstConnectionNSMappingServerEnabledClientEnabled();
        driver.resetCQS();
        Properties clientProps = getClientProperties(true, true);
        setOldTimestampToInduceUpgrade = false;
        driver.getConnectionQueryServices(getJdbcUrl(), clientProps);
        // There should be no new tables
        assertEquals(hbaseTables, getHBaseTables());
        // Since we set an old timestamp on purpose when creating SYSTEM:CATALOG, the second connection attempts to upgrade it
        assertEquals(1, countUpgradeAttempts);
        assertEquals(1, actualSysCatUpgrades);
    }

    // Conditions: server-side namespace mapping is enabled, the first connection to the server will create all namespace
    // mapped SYSTEM tables i.e. SYSTEM:.*, the SYSTEM:CATALOG timestamp at creation is purposefully set to be <
    // MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP. The subsequent connection has client-side namespace mapping enabled
    // Expected: An upgrade is attempted when the second client connects to the server, but this fails since the
    // isDoNotUpgradePropSet is set to true. We later run EXECUTE UPGRADE manually
    @Test
    public void testUpgradeNotAllowed() throws Exception {
        setOldTimestampToInduceUpgrade = true;
        SystemCatalogCreationOnConnectionIT.PhoenixSysCatCreationTestingDriver driver =
          firstConnectionNSMappingServerEnabledClientEnabled();
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
        // Since we set an old timestamp on purpose when creating SYSTEM:CATALOG, the second connection attempts to upgrade it
        assertEquals(1, countUpgradeAttempts);
        // This connection is unable to actually upgrade SYSTEM:CATALOG due to isDoNotUpgradePropSet
        assertEquals(0, actualSysCatUpgrades);
        Connection conn = driver.getConnectionQueryServices(getJdbcUrl(), new Properties()).connect(getJdbcUrl(), new Properties());
        try {
            conn.createStatement().execute(EXECUTE_UPGRADE_COMMAND);
            // Actually upgraded SYSTEM:CATALOG
            assertEquals(1, actualSysCatUpgrades);
        } finally {
            conn.close();
        }
    }

    // Conditions: server-side namespace mapping is enabled, the first connection to the server will create only SYSTEM.CATALOG,
    // the second connection has client-side namespace mapping enabled
    // Expected: We will migrate SYSTEM.CATALOG to SYSTEM namespace and create all other SYSTEM:.* tables
    @Test
    public void testMigrateSysCatCreateOthers() throws Exception {
        SystemCatalogCreationOnConnectionIT.PhoenixSysCatCreationTestingDriver driver =
          firstConnectionNSMappingServerEnabledClientDisabled();
        driver.resetCQS();
        Properties clientProps = getClientProperties(true, true);
        driver.getConnectionQueryServices(getJdbcUrl(), clientProps);
        hbaseTables = getHBaseTables();
        assertEquals(PHOENIX_NAMESPACE_MAPPED_SYSTEM_TABLES, hbaseTables);
        // SYSTEM.CATALOG migration to the SYSTEM namespace is counted as an upgrade
        assertEquals(1, countUpgradeAttempts);
    }

    // Conditions: server-side namespace mapping is enabled, the first connection to the server will create unmapped SYSTEM
    // tables SYSTEM\..* whose timestamp at creation is purposefully set to be < MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP.
    // The second connection has client-side namespace mapping enabled and system table to system namespace mapping enabled
    // Expected: We will migrate all SYSTEM\..* tables to the SYSTEM namespace and also upgrade SYSTEM:CATALOG
    @Test
    public void testMigrateToSystemNamespaceAndUpgradeSysCat() throws Exception {
        setOldTimestampToInduceUpgrade = true;
        SystemCatalogCreationOnConnectionIT.PhoenixSysCatCreationTestingDriver driver =
          firstConnectionNSMappingServerEnabledClientEnabledMappingDisabled();
        driver.resetCQS();
        setOldTimestampToInduceUpgrade = false;
        Properties clientProps = getClientProperties(true, true);
        driver.getConnectionQueryServices(getJdbcUrl(), clientProps);
        hbaseTables = getHBaseTables();
        assertEquals(PHOENIX_NAMESPACE_MAPPED_SYSTEM_TABLES, hbaseTables);
        assertEquals(1, countUpgradeAttempts);
        assertEquals(1, actualSysCatUpgrades);
    }

    // Conditions: server-side namespace mapping is enabled, the first connection to the server will create all namespace
    // mapped SYSTEM tables i.e. SYSTEM:.*, the second connection has client-side namespace mapping disabled
    // Expected: Throw Inconsistent namespace mapping exception from ensureTableCreated
    @Test
    public void testTablesExistInconsistentNSMappingFails() throws Exception {
        SystemCatalogCreationOnConnectionIT.PhoenixSysCatCreationTestingDriver driver =
          firstConnectionNSMappingServerEnabledClientEnabled();
        driver.resetCQS();
        Properties clientProps = getClientProperties(false, false);
        try {
            driver.getConnectionQueryServices(getJdbcUrl(), clientProps);
            fail("Client should not be able to connect to cluster with inconsistent client-server namespace mapping properties");
        } catch (SQLException sqlE) {
            assertEquals(SQLExceptionCode.INCONSISTENT_NAMESPACE_MAPPING_PROPERTIES.getErrorCode(), sqlE.getErrorCode());
        }
        hbaseTables = getHBaseTables();
        assertEquals(PHOENIX_NAMESPACE_MAPPED_SYSTEM_TABLES, hbaseTables);
        assertEquals(0, countUpgradeAttempts);
    }

    // Conditions: server-side namespace mapping is enabled, the first connection to the server will create only SYSTEM.CATALOG,
    // the second connection has client-side namespace mapping disabled
    // Expected: Throw Inconsistent namespace mapping exception when you check client-server compatibility
    @Test
    public void testUnmappedSysCatExistsInconsistentNSMappingFails() throws Exception {
        SystemCatalogCreationOnConnectionIT.PhoenixSysCatCreationTestingDriver driver =
          firstConnectionNSMappingServerEnabledClientDisabled();
        driver.resetCQS();
        Properties clientProps = getClientProperties(false, false);
        try {
            driver.getConnectionQueryServices(getJdbcUrl(), clientProps);
            fail("Client should not be able to connect to cluster with inconsistent client-server namespace mapping properties");
        } catch (SQLException sqlE) {
            assertEquals(SQLExceptionCode.INCONSISTENT_NAMESPACE_MAPPING_PROPERTIES.getErrorCode(), sqlE.getErrorCode());
        }
        hbaseTables = getHBaseTables();
        assertTrue(hbaseTables.contains(PHOENIX_SYSTEM_CATALOG));
        assertTrue(hbaseTables.size() == 1);
        assertEquals(0, countUpgradeAttempts);
    }

    // Conditions: server-side namespace mapping is disabled, the first connection to the server will create all unmapped
    // SYSTEM tables i.e. SYSTEM\..*, the second connection has client-side namespace mapping enabled
    // Expected: Throw Inconsistent namespace mapping exception when you check client-server compatibility
    @Test
    public void testSysTablesExistInconsistentNSMappingFails() throws Exception {
        SystemCatalogCreationOnConnectionIT.PhoenixSysCatCreationTestingDriver driver =
          firstConnectionNSMappingServerDisabledClientDisabled();
        driver.resetCQS();
        Properties clientProps = getClientProperties(true, true);
        try {
            driver.getConnectionQueryServices(getJdbcUrl(), clientProps);
            fail("Client should not be able to connect to cluster with inconsistent client-server namespace mapping properties");
        } catch (SQLException sqlE) {
            assertEquals(SQLExceptionCode.INCONSISTENT_NAMESPACE_MAPPING_PROPERTIES.getErrorCode(), sqlE.getErrorCode());
        }
        hbaseTables = getHBaseTables();
        assertEquals(PHOENIX_SYSTEM_TABLES, hbaseTables);
        assertEquals(0, countUpgradeAttempts);
    }

    // Conditions: server-side namespace mapping is disabled, the first connection to the server will create only SYSTEM:CATALOG
    // and the second connection has client-side namespace mapping enabled
    // Expected: Throw Inconsistent namespace mapping exception when you check client-server compatibility
    @Test
    public void testMappedSysCatExistsInconsistentNSMappingFails() throws Exception {
        SystemCatalogCreationOnConnectionIT.PhoenixSysCatCreationTestingDriver driver =
          firstConnectionNSMappingServerDisabledClientEnabled();
        driver.resetCQS();
        Properties clientProps = getClientProperties(true, true);
        try{
            driver.getConnectionQueryServices(getJdbcUrl(), clientProps);
            fail("Client should not be able to connect to cluster with inconsistent client-server namespace mapping properties");
        } catch (SQLException sqlE) {
            assertEquals(SQLExceptionCode.INCONSISTENT_NAMESPACE_MAPPING_PROPERTIES.getErrorCode(), sqlE.getErrorCode());
        }
        hbaseTables = getHBaseTables();
        assertTrue(hbaseTables.contains(PHOENIX_NAMESPACE_MAPPED_SYSTEM_CATALOG));
        assertTrue(hbaseTables.size() == 1);
        assertEquals(0, countUpgradeAttempts);
    }

    // Conditions: server-side namespace mapping is disabled, the first connection to the server will create all SYSTEM\..*
    // tables and the second connection has client-side namespace mapping disabled
    // Expected: All SYSTEM\..* tables exist and no upgrade is required
    @Test
    public void testNSMappingDisabledNoUpgradeRequired() throws Exception {
        SystemCatalogCreationOnConnectionIT.PhoenixSysCatCreationTestingDriver driver =
          firstConnectionNSMappingServerDisabledClientDisabled();
        driver.resetCQS();
        Properties clientProps = getClientProperties(false, false);
        driver.getConnectionQueryServices(getJdbcUrl(), clientProps);
        hbaseTables = getHBaseTables();
        assertEquals(PHOENIX_SYSTEM_TABLES, hbaseTables);
        assertEquals(0, countUpgradeAttempts);
    }

    // Conditions: server-side namespace mapping is disabled, the first connection to the server will create only SYSTEM:CATALOG
    // and the second connection has client-side namespace mapping disabled
    // Expected: The second connection should fail with Inconsistent namespace mapping exception
    @Test
    public void testClientNSMappingDisabledConnectionFails() throws Exception {
        SystemCatalogCreationOnConnectionIT.PhoenixSysCatCreationTestingDriver driver =
          firstConnectionNSMappingServerDisabledClientEnabled();
        driver.resetCQS();
        Properties clientProps = getClientProperties(false, false);
        try{
            driver.getConnectionQueryServices(getJdbcUrl(), clientProps);
            fail("Client should not be able to connect to cluster with inconsistent client-server namespace mapping properties");
        } catch (SQLException sqlE) {
            assertEquals(SQLExceptionCode.INCONSISTENT_NAMESPACE_MAPPING_PROPERTIES.getErrorCode(), sqlE.getErrorCode());
        }
        hbaseTables = getHBaseTables();
        assertTrue(hbaseTables.contains(PHOENIX_NAMESPACE_MAPPED_SYSTEM_CATALOG));
        assertTrue(hbaseTables.size() == 1);
        assertEquals(0, countUpgradeAttempts);
    }

    // Conditions: The first connection creates all SYSTEM tables via "EXECUTE UPGRADE" since auto-upgrade is disabled
    // and the same client alters HBase metadata for SYSTEM.CATALOG
    // Expected: Another client connection (with a new ConnectionQueryServices instance) made to the server does not
    // revert the metadata change
    @Test
    public void testMetadataAlterRemainsAutoUpgradeDisabled() throws Exception {
        SystemCatalogCreationOnConnectionIT.PhoenixSysCatCreationTestingDriver driver = firstConnectionAutoUpgradeToggle(false);
        assertEquals(Integer.parseInt(MODIFIED_MAX_VERSIONS), verifyModificationTableMetadata(driver, PHOENIX_SYSTEM_CATALOG));
    }

    // Conditions: The first connection creates all SYSTEM tables (auto-upgrade is enabled) and the same client alters
    // HBase metadata for SYSTEM.CATALOG
    // Expected: Another client connection (with a new ConnectionQueryServices instance) made to the server does not
    // revert the metadata change
    @Test
    public void testMetadataAlterRemainsAutoUpgradeEnabled() throws Exception {
        SystemCatalogCreationOnConnectionIT.PhoenixSysCatCreationTestingDriver driver = firstConnectionAutoUpgradeToggle(true);
        assertEquals(Integer.parseInt(MODIFIED_MAX_VERSIONS), verifyModificationTableMetadata(driver, PHOENIX_SYSTEM_CATALOG));
    }

    /**
     * Return all created HBase tables
     * @return Set of HBase table name strings
     * @throws IOException
     */
    private Set<String> getHBaseTables() throws IOException {
        Set<String> tables = new HashSet<>();
        for (TableName tn : testUtil.getHBaseAdmin().listTableNames()) {
            tables.add(tn.getNameAsString());
        }
        return tables;
    }

    /**
     * Alter the table metadata and return modified value
     * @param driver
     * @param tableName
     * @return value of VERSIONS option for the table
     * @throws Exception
     */
    private int verifyModificationTableMetadata(PhoenixSysCatCreationTestingDriver driver, String tableName) throws Exception {
        // Modify table metadata
        Connection conn = driver.getConnectionQueryServices(getJdbcUrl(), new Properties()).connect(getJdbcUrl(), new Properties());
        conn.createStatement().execute("ALTER TABLE " + tableName + " SET VERSIONS = " + MODIFIED_MAX_VERSIONS);

        // Connect via a client that creates a new ConnectionQueryServices instance
        driver.resetCQS();
        driver.getConnectionQueryServices(getJdbcUrl(), new Properties()).connect(getJdbcUrl(), new Properties());
        HTableDescriptor descriptor = testUtil.getHBaseAdmin().getTableDescriptor(TableName.valueOf(tableName));
        return descriptor.getFamily(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES).getMaxVersions();
    }

    /**
     * Start the mini-cluster with server-side namespace mapping property specified
     * @param isNamespaceMappingEnabled
     * @throws Exception
     */
    private void startMiniClusterWithToggleNamespaceMapping(String isNamespaceMappingEnabled) throws Exception {
        testUtil = new HBaseTestingUtility();
        Configuration conf = testUtil.getConfiguration();
        conf.set(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, isNamespaceMappingEnabled);
        // Avoid multiple clusters trying to bind to the master's info port (16010)
        conf.setInt(HConstants.MASTER_INFO_PORT, -1);
        testUtil.startMiniCluster(1);
    }

    /**
     * Get the connection string for the mini-cluster
     * @return Phoenix connection string
     */
    private String getJdbcUrl() {
        return "jdbc:phoenix:localhost:" + testUtil.getZkCluster().getClientPort() + ":/hbase";
    }

    /**
     * Set namespace mapping related properties for the client connection
     * @param nsMappingEnabled
     * @param systemTableMappingEnabled
     * @return Properties object
     */
    private Properties getClientProperties(boolean nsMappingEnabled, boolean systemTableMappingEnabled) {
        Properties clientProps = new Properties();
        clientProps.setProperty(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, Boolean.valueOf(nsMappingEnabled).toString());
        clientProps.setProperty(QueryServices.IS_SYSTEM_TABLE_MAPPED_TO_NAMESPACE, Boolean.valueOf(systemTableMappingEnabled).toString());
        return clientProps;
    }

    /**
     * Initiate the first connection to the server with provided auto-upgrade property
     * @param isAutoUpgradeEnabled
     * @return Phoenix JDBC driver
     * @throws Exception
     */
    private SystemCatalogCreationOnConnectionIT.PhoenixSysCatCreationTestingDriver firstConnectionAutoUpgradeToggle(boolean isAutoUpgradeEnabled)
    throws Exception {
        if (isAutoUpgradeEnabled) {
            return firstConnectionNSMappingServerDisabledClientDisabled();
        }
        return firstConnectionAutoUpgradeDisabled();
    }

    // Conditions: isAutoUpgradeEnabled is false
    // Expected: We do not create SYSTEM.CATALOG even if this is the first connection to the server. Later, when we manually
    // run "EXECUTE UPGRADE", we create SYSTEM tables
    private SystemCatalogCreationOnConnectionIT.PhoenixSysCatCreationTestingDriver firstConnectionAutoUpgradeDisabled() throws Exception {
        startMiniClusterWithToggleNamespaceMapping(Boolean.FALSE.toString());
        Map<String, String> props = new HashMap<>();
        // Note that the isAutoUpgradeEnabled property is set when instantiating connection query services, not during init
        props.put(QueryServices.AUTO_UPGRADE_ENABLED, Boolean.FALSE.toString());
        ReadOnlyProps readOnlyProps = new ReadOnlyProps(props);
        SystemCatalogCreationOnConnectionIT.PhoenixSysCatCreationTestingDriver driver =
          new SystemCatalogCreationOnConnectionIT.PhoenixSysCatCreationTestingDriver(readOnlyProps);
        try {
            driver.getConnectionQueryServices(getJdbcUrl(), new Properties());
            fail("Client should not be able to create SYSTEM.CATALOG since we set the isAutoUpgradeEnabled property to false");
        } catch (Exception e) {
            assertTrue(e instanceof UpgradeRequiredException);
        }
        hbaseTables = getHBaseTables();
        assertFalse(hbaseTables.contains(PHOENIX_SYSTEM_CATALOG) || hbaseTables.contains(PHOENIX_NAMESPACE_MAPPED_SYSTEM_CATALOG));
        assertTrue(hbaseTables.size() == 0);
        assertEquals(1, countUpgradeAttempts);

        // We use the same ConnectionQueryServices instance to run "EXECUTE UPGRADE"
        Connection conn = driver.getConnectionQueryServices(getJdbcUrl(), new Properties()).connect(getJdbcUrl(), new Properties());
        try {
            conn.createStatement().execute(EXECUTE_UPGRADE_COMMAND);
        } finally {
            conn.close();
        }
        hbaseTables = getHBaseTables();
        assertEquals(PHOENIX_SYSTEM_TABLES, hbaseTables);
        return driver;
    }

    // Conditions: server-side namespace mapping is enabled, client-side namespace mapping is enabled and system tables
    // are to be mapped to the SYSTEM namespace.
    // Expected: If this is the first connection to the server, we should be able to create all namespace mapped system tables i.e. SYSTEM:.*
    private SystemCatalogCreationOnConnectionIT.PhoenixSysCatCreationTestingDriver firstConnectionNSMappingServerEnabledClientEnabled()
    throws Exception {
        startMiniClusterWithToggleNamespaceMapping(Boolean.TRUE.toString());
        Properties clientProps = getClientProperties(true, true);
        SystemCatalogCreationOnConnectionIT.PhoenixSysCatCreationTestingDriver driver =
          new SystemCatalogCreationOnConnectionIT.PhoenixSysCatCreationTestingDriver(ReadOnlyProps.EMPTY_PROPS);
        driver.getConnectionQueryServices(getJdbcUrl(), clientProps);
        hbaseTables = getHBaseTables();
        assertEquals(PHOENIX_NAMESPACE_MAPPED_SYSTEM_TABLES, hbaseTables);
        assertEquals(0, countUpgradeAttempts);
        return driver;
    }

    // Conditions: server-side namespace mapping is enabled, client-side namespace mapping is enabled, but mapping
    // SYSTEM tables to the SYSTEM namespace is disabled
    // Expected: If this is the first connection to the server, we will create unmapped SYSTEM tables i.e. SYSTEM\..*
    private SystemCatalogCreationOnConnectionIT.PhoenixSysCatCreationTestingDriver firstConnectionNSMappingServerEnabledClientEnabledMappingDisabled()
    throws Exception {
        startMiniClusterWithToggleNamespaceMapping(Boolean.TRUE.toString());
        // client-side namespace mapping is enabled, but mapping SYSTEM tables to SYSTEM namespace is not
        Properties clientProps = getClientProperties(true, false);
        SystemCatalogCreationOnConnectionIT.PhoenixSysCatCreationTestingDriver driver =
          new SystemCatalogCreationOnConnectionIT.PhoenixSysCatCreationTestingDriver(ReadOnlyProps.EMPTY_PROPS);
        driver.getConnectionQueryServices(getJdbcUrl(), clientProps);
        hbaseTables = getHBaseTables();
        assertEquals(PHOENIX_SYSTEM_TABLES, hbaseTables);
        assertEquals(0, countUpgradeAttempts);
        return driver;
    }

    // Conditions: server-side namespace mapping is enabled, client-side namespace mapping is disabled
    // Expected: Since this is the first connection to the server, we will create SYSTEM.CATALOG but immediately
    // throw an exception for inconsistent namespace mapping
    private SystemCatalogCreationOnConnectionIT.PhoenixSysCatCreationTestingDriver firstConnectionNSMappingServerEnabledClientDisabled()
    throws Exception {
        startMiniClusterWithToggleNamespaceMapping(Boolean.TRUE.toString());
        Properties clientProps = getClientProperties(false, false);
        SystemCatalogCreationOnConnectionIT.PhoenixSysCatCreationTestingDriver driver =
          new SystemCatalogCreationOnConnectionIT.PhoenixSysCatCreationTestingDriver(ReadOnlyProps.EMPTY_PROPS);
        try {
            driver.getConnectionQueryServices(getJdbcUrl(), clientProps);
            fail("Client should not be able to connect to cluster with inconsistent client-server namespace mapping properties");
        } catch (SQLException sqlE) {
            assertEquals(SQLExceptionCode.INCONSISTENT_NAMESPACE_MAPPING_PROPERTIES.getErrorCode(), sqlE.getErrorCode());
        }
        hbaseTables = getHBaseTables();
        assertTrue(hbaseTables.contains(PHOENIX_SYSTEM_CATALOG));
        assertTrue(hbaseTables.size() == 1);
        assertEquals(0, countUpgradeAttempts);
        return driver;
    }

    // Conditions: server-side namespace mapping is disabled, client-side namespace mapping is enabled
    // Expected: Since this is the first connection to the server, we will create the SYSTEM namespace and create
    // SYSTEM:CATALOG and then immediately throw an exception for inconsistent namespace mapping
    private SystemCatalogCreationOnConnectionIT.PhoenixSysCatCreationTestingDriver firstConnectionNSMappingServerDisabledClientEnabled()
    throws Exception {
        startMiniClusterWithToggleNamespaceMapping(Boolean.FALSE.toString());
        Properties clientProps = getClientProperties(true, true);
        SystemCatalogCreationOnConnectionIT.PhoenixSysCatCreationTestingDriver driver =
          new SystemCatalogCreationOnConnectionIT.PhoenixSysCatCreationTestingDriver(ReadOnlyProps.EMPTY_PROPS);
        try {
            driver.getConnectionQueryServices(getJdbcUrl(), clientProps);
            fail("Client should not be able to connect to cluster with inconsistent client-server namespace mapping properties");
        } catch (SQLException sqlE) {
            assertEquals(SQLExceptionCode.INCONSISTENT_NAMESPACE_MAPPING_PROPERTIES.getErrorCode(), sqlE.getErrorCode());
        }
        hbaseTables = getHBaseTables();
        assertTrue(hbaseTables.contains(PHOENIX_NAMESPACE_MAPPED_SYSTEM_CATALOG));
        assertTrue(hbaseTables.size() == 1);
        assertEquals(0, countUpgradeAttempts);
        return driver;
    }

    // Conditions: server-side namespace mapping is disabled, client-side namespace mapping is disabled
    // Expected: Since this is the first connection to the server and auto-upgrade is enabled by default,
    // we will create all SYSTEM\..* tables
    private SystemCatalogCreationOnConnectionIT.PhoenixSysCatCreationTestingDriver firstConnectionNSMappingServerDisabledClientDisabled()
    throws Exception {
        startMiniClusterWithToggleNamespaceMapping(Boolean.FALSE.toString());
        Properties clientProps = getClientProperties(false, false);
        SystemCatalogCreationOnConnectionIT.PhoenixSysCatCreationTestingDriver driver =
          new SystemCatalogCreationOnConnectionIT.PhoenixSysCatCreationTestingDriver(ReadOnlyProps.EMPTY_PROPS);
        driver.getConnectionQueryServices(getJdbcUrl(), clientProps);
        hbaseTables = getHBaseTables();
        assertEquals(PHOENIX_SYSTEM_TABLES, hbaseTables);
        assertEquals(0, countUpgradeAttempts);
        return driver;
    }
}