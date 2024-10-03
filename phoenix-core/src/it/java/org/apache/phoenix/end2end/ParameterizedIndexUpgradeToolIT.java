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

import org.apache.phoenix.end2end.index.IndexTestUtil;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.phoenix.end2end.index.IndexCoprocIT;
import org.apache.phoenix.hbase.index.IndexRegionObserver;
import org.apache.phoenix.hbase.index.Indexer;
import org.apache.phoenix.index.GlobalIndexChecker;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.mapreduce.index.IndexTool;
import org.apache.phoenix.mapreduce.index.IndexUpgradeTool;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static org.apache.phoenix.mapreduce.index.IndexUpgradeTool.ROLLBACK_OP;
import static org.apache.phoenix.mapreduce.index.IndexUpgradeTool.UPGRADE_OP;
import static org.apache.phoenix.query.QueryServices.DROP_METADATA_ATTRIB;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.mockito.Mockito.times;

@RunWith(Parameterized.class)
@Category(NeedsOwnMiniClusterTest.class)
public class ParameterizedIndexUpgradeToolIT extends BaseTest {
    private static final String [] INDEXES_LIST = new String[8];
    private static final String [] INDEXES_LIST_NAMESPACE = new String[8];
    private static final String [] INDEXES_LIST_SIMPLIFIED = new String[1];
    private static final String [] INDEXES_LIST_NAMESPACE_SIMPLIFIED = new String[1];

    private static final String [] TRANSACTIONAL_INDEXES_LIST = new String[2];

    private static final String [] TABLE_LIST = new String[4];
    private static final String [] TABLE_LIST_NAMESPACE = new String[4];
    private static final String [] TABLE_LIST_SIMPLIFIED = new String[1];
    private static final String [] TABLE_LIST_NAMESPACE_SIMPLIFIED = new String[1];

    private static final String [] TRANSACTIONAL_TABLE_LIST = new String[1];

    private static String INPUT_LIST = "";
    private Path tmpDir;

    private static Map<String, String> serverProps = Maps.newHashMapWithExpectedSize(1),
            clientProps = Maps.newHashMapWithExpectedSize(1);

    private final boolean mutable;
    private final boolean upgrade;
    private final boolean isNamespaceEnabled;
    private final boolean rebuild;

    private StringBuilder optionsBuilder;
    private String tableDDLOptions;
    private Connection conn;
    private Connection connTenant;
    private Admin admin;
    private IndexUpgradeTool iut;
    private static String systemTmpDir = System.getProperty("java.io.tmpdir");;

    @Mock
    private IndexTool indexToolMock;

    @Captor
    private ArgumentCaptor<String []> argCapture;

    @BeforeClass
    public static synchronized void saveTmp () throws Exception {
        //The JVM will exit, so we don't need to restore after
        systemTmpDir = System.getProperty("java.io.tmpdir");
    }

    @Before
    public void setup () throws Exception {
        MockitoAnnotations.initMocks(this);

        optionsBuilder = new StringBuilder();

        setClusterProperties();

        setUpTestDriver(new ReadOnlyProps(serverProps.entrySet().iterator()),
                new ReadOnlyProps(clientProps.entrySet().iterator()));

        conn = DriverManager.getConnection(getUrl(), new Properties());
        conn.setAutoCommit(true);

        String tenantId = generateUniqueName();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
        connTenant = DriverManager.getConnection(getUrl(), props);

        ConnectionQueryServices queryServices = conn.unwrap(PhoenixConnection.class)
                .getQueryServices();
        admin = queryServices.getAdmin();
        if (!mutable) {
            optionsBuilder.append(" IMMUTABLE_ROWS=true");
        }
        tableDDLOptions = optionsBuilder.toString();
        tmpDir = Files.createTempDirectory(ParameterizedIndexUpgradeToolIT.class.getCanonicalName());
    }

    private void setClusterProperties() {
        // we need to destroy the cluster if it was initiated using property as true
        if (Boolean.toString(upgrade).equals(clientProps
                .get(QueryServices.INDEX_REGION_OBSERVER_ENABLED_ATTRIB))
                || Boolean.toString(!isNamespaceEnabled).equals(serverProps
                .get(QueryServices.IS_NAMESPACE_MAPPING_ENABLED))) {
            tearDownMiniCluster(1);
            System.setProperty("java.io.tmpdir", systemTmpDir);
        }
        //setting up properties for namespace
        clientProps.put(QueryServices.IS_NAMESPACE_MAPPING_ENABLED,
                Boolean.toString(isNamespaceEnabled));
        clientProps.put(QueryServices.IS_SYSTEM_TABLE_MAPPED_TO_NAMESPACE,
                Boolean.toString(isNamespaceEnabled));
        clientProps.put(DROP_METADATA_ATTRIB, Boolean.toString(true));
        serverProps.putAll(clientProps);
        //To mimic the upgrade/rollback scenario, so that table creation uses old/new design
        clientProps.put(QueryServices.INDEX_REGION_OBSERVER_ENABLED_ATTRIB,
                Boolean.toString(!upgrade));
    }

    private void prepareFullSetup() throws SQLException, IOException {
        clearOldTableNames();
        String mockTableOne = "TEST." + generateUniqueName();
        TABLE_LIST[0] = mockTableOne;
        String mockTableTwo = "TEST1." + generateUniqueName();
        TABLE_LIST[1] = mockTableTwo;
        String mockTableThree = "TEST." + generateUniqueName();
        TABLE_LIST[2] = mockTableThree;
        String multiTenantTable = "TEST." + generateUniqueName();
        TABLE_LIST[3] = multiTenantTable;
        INPUT_LIST = StringUtils.join(TABLE_LIST, ",");
        if (isNamespaceEnabled) {
            TABLE_LIST_NAMESPACE[0] = mockTableOne.replace(".", ":");
            TABLE_LIST_NAMESPACE[1] = mockTableTwo.replace(".", ":");
            TABLE_LIST_NAMESPACE[2] = mockTableThree.replace(".", ":");
            TABLE_LIST_NAMESPACE[3] = multiTenantTable.replace(".", ":");
            conn.createStatement().execute("CREATE SCHEMA IF NOT EXISTS TEST");
            conn.createStatement().execute("CREATE SCHEMA IF NOT EXISTS TEST1");
        }

        conn.createStatement().execute("CREATE TABLE " + mockTableOne + " (id bigint NOT NULL "
                + "PRIMARY KEY, a.name varchar, sal bigint, address varchar)" + tableDDLOptions);
        conn.createStatement().execute("CREATE TABLE "+ mockTableTwo + " (id bigint NOT NULL "
                + "PRIMARY KEY, name varchar, city varchar, phone bigint)" + tableDDLOptions);
        conn.createStatement().execute("CREATE TABLE " + mockTableThree + " (id bigint NOT NULL "
                + "PRIMARY KEY, name varchar, age bigint)" + tableDDLOptions);
        String
                createTblStr =
                "CREATE TABLE " + multiTenantTable + " (TENANT_ID VARCHAR(15) NOT NULL,ID INTEGER NOT NULL"
                        + ", NAME VARCHAR, CONSTRAINT PK_1 PRIMARY KEY (TENANT_ID, ID)) MULTI_TENANT=true";
        conn.createStatement().execute(createTblStr);

        String transactTable = generateUniqueName();
        TRANSACTIONAL_TABLE_LIST[0] = transactTable;
        conn.createStatement().execute("CREATE TABLE " + transactTable + " (id bigint NOT NULL "
                        + "PRIMARY KEY, a.name varchar, sal bigint, address varchar) "
                + " TRANSACTIONAL=true, TRANSACTION_PROVIDER='OMID' "
                + ((tableDDLOptions.trim().length() > 0) ? "," : "") + tableDDLOptions);

        String mockOneViewOne = "TEST." + generateUniqueName();
        String mockOneViewTwo = "TEST." + generateUniqueName();
        String mockTwoViewOne = "TEST1." + generateUniqueName();
        String transactView = generateUniqueName();

        //views
        conn.createStatement().execute("CREATE VIEW " + mockOneViewOne + " (view_column varchar) "
                + "AS SELECT * FROM " + mockTableOne + " WHERE a.name = 'a'");
        conn.createStatement().execute("CREATE VIEW " + mockOneViewTwo + " (view_column varchar,"
                + " zip varchar) AS SELECT * FROM " + mockTableOne + " WHERE a.name = 'a'");
        conn.createStatement().execute("CREATE VIEW " + mockTwoViewOne + " (view_column varchar,"
                + " state varchar) AS SELECT * FROM " + mockTableTwo + " WHERE name = 'c'");
        conn.createStatement().execute("CREATE VIEW " + transactView + " (view_column varchar,"
                + " state varchar) AS SELECT * FROM " + transactTable + " WHERE name = 'c'");

        //view-indexes
        String indexOneMockOneViewOne = generateUniqueName();
        String indexTwoMockOneViewTwo = generateUniqueName();
        String indexOneMockTwoViewTwo = generateUniqueName();
        String indexThreeMockOneViewOne = generateUniqueName();
        String transactViewIndex = generateUniqueName();

        INDEXES_LIST[6] = MetaDataUtil.getViewIndexPhysicalName(mockTableOne);
        INDEXES_LIST[7] = MetaDataUtil.getViewIndexPhysicalName(mockTableTwo);
        if (isNamespaceEnabled) {
            INDEXES_LIST_NAMESPACE[6] =
                MetaDataUtil.getViewIndexPhysicalName(TABLE_LIST_NAMESPACE[0]);
            INDEXES_LIST_NAMESPACE[7] =
                MetaDataUtil.getViewIndexPhysicalName(TABLE_LIST_NAMESPACE[1]);
        }

        conn.createStatement().execute("CREATE INDEX " + indexOneMockOneViewOne + " ON "
            + mockOneViewOne + " (view_column)");
        conn.createStatement().execute("CREATE INDEX " + indexTwoMockOneViewTwo + " ON "
            + mockOneViewTwo + " (zip)");
        conn.createStatement().execute("CREATE INDEX " + indexOneMockTwoViewTwo + " ON "
            + mockTwoViewOne + " (state, city)");
        conn.createStatement().execute("CREATE INDEX " + indexThreeMockOneViewOne
            + " ON " + mockOneViewOne + " (view_column)");
        conn.createStatement().execute("CREATE INDEX " + transactViewIndex + " ON " +
            transactView + " (view_column)");

        //indexes
        String indexOneMockOne = generateUniqueName();
        String indexTwoMockOne = generateUniqueName();
        String indexOneMockTwo = generateUniqueName();
        String indexTwoMockTwo = generateUniqueName();
        String indexThreeMockTwo = generateUniqueName();
        String indexThreeMockThree = generateUniqueName();
        String transactIndex = generateUniqueName();
        TRANSACTIONAL_INDEXES_LIST[0] = transactIndex;
        TRANSACTIONAL_INDEXES_LIST[1] = MetaDataUtil.getViewIndexPhysicalName(transactTable);

        if (isNamespaceEnabled) {
            INDEXES_LIST_NAMESPACE[0] = "TEST:" + indexOneMockOne;
            INDEXES_LIST_NAMESPACE[1] = "TEST:" + indexTwoMockOne;
            INDEXES_LIST_NAMESPACE[2] = "TEST1:" + indexOneMockTwo;
            INDEXES_LIST_NAMESPACE[3] = "TEST1:" + indexTwoMockTwo;
            INDEXES_LIST_NAMESPACE[4] = "TEST1:" + indexThreeMockTwo;
            INDEXES_LIST_NAMESPACE[5] = "TEST:" + indexThreeMockThree;
        } else {
            INDEXES_LIST[0] = "TEST." + indexOneMockOne;
            INDEXES_LIST[1] = "TEST." + indexTwoMockOne;
            INDEXES_LIST[2] = "TEST1." + indexOneMockTwo;
            INDEXES_LIST[3] = "TEST1." + indexTwoMockTwo;
            INDEXES_LIST[4] = "TEST1." + indexThreeMockTwo;
            INDEXES_LIST[5] = "TEST." + indexThreeMockThree;
        }

        conn.createStatement().execute("CREATE INDEX " + indexOneMockOne + " ON " + mockTableOne +
                " (sal, a.name)");
        conn.createStatement().execute("CREATE INDEX " + indexTwoMockOne + " ON " + mockTableOne
            + " (a.name)");
        conn.createStatement().execute("CREATE INDEX " + indexOneMockTwo + " ON " + mockTableTwo
            + " (city)");
        conn.createStatement().execute("CREATE INDEX " + indexTwoMockTwo + " ON " + mockTableTwo
            + " (phone)");
        conn.createStatement().execute("CREATE INDEX " + indexThreeMockTwo + " ON " +
            "" + mockTableTwo + " (name)");
        conn.createStatement().execute("CREATE INDEX " + indexThreeMockThree + " ON " +
                mockTableThree + " (age, name)");
        conn.createStatement().execute("CREATE INDEX " + transactIndex  + " ON " + transactTable +
            " (sal)");

        // Tenant ones
        String tenantView = "TEST." + generateUniqueName();
        String tenantViewIndex = generateUniqueName();
        connTenant.createStatement().execute(
                "CREATE VIEW " + tenantView + " AS SELECT * FROM " + multiTenantTable);
        connTenant.createStatement().execute("CREATE INDEX " + tenantViewIndex + " ON "
            + tenantView + " (NAME)");

        conn.createStatement().execute("ALTER INDEX " + indexTwoMockOneViewTwo + " ON "
            + mockOneViewOne + " DISABLE");
        connTenant.createStatement().execute("ALTER INDEX " + tenantViewIndex + " ON " +
            tenantView + " DISABLE");
        conn.createStatement().execute("ALTER INDEX " + indexTwoMockOne + " ON " + mockTableOne +
            " DISABLE");
        iut = new IndexUpgradeTool(upgrade ? UPGRADE_OP : ROLLBACK_OP, INPUT_LIST,
            null, Files.createTempFile(tmpDir, "index_upgrade_", null).toString(),
            true, indexToolMock, rebuild);
        iut.setConf(getUtility().getConfiguration());
        iut.setTest(true);
    }

    private void clearOldTableNames() {
        Arrays.fill(TABLE_LIST, null);
        Arrays.fill(TABLE_LIST_NAMESPACE, null);
        Arrays.fill(TABLE_LIST_SIMPLIFIED, null);
        Arrays.fill(TABLE_LIST_NAMESPACE_SIMPLIFIED, null);
        Arrays.fill(INDEXES_LIST, null);
        Arrays.fill(INDEXES_LIST_NAMESPACE, null);
        Arrays.fill(INDEXES_LIST_SIMPLIFIED, null);
        Arrays.fill(INDEXES_LIST_NAMESPACE_SIMPLIFIED, null);
        Arrays.fill(TRANSACTIONAL_INDEXES_LIST, null);
        Arrays.fill(TRANSACTIONAL_TABLE_LIST, null);
    }
    private void prepareSimplifiedSetup() throws SQLException, IOException {
        clearOldTableNames();
        String mockTableOne = "TEST." + generateUniqueName();
        INPUT_LIST = mockTableOne;
        if (isNamespaceEnabled) {
            conn.createStatement().execute("CREATE SCHEMA IF NOT EXISTS TEST");
            conn.createStatement().execute("CREATE SCHEMA IF NOT EXISTS TEST1");
            TABLE_LIST_NAMESPACE_SIMPLIFIED[0] = mockTableOne.replace(".", ":");
        } else {
            TABLE_LIST_SIMPLIFIED[0] = mockTableOne;
        }
        conn.createStatement().execute("CREATE TABLE " + mockTableOne + " (id bigint NOT NULL "
            + "PRIMARY KEY, a.name varchar, sal bigint, address varchar)" + tableDDLOptions);
        conn.commit();
        String indexOneMockOne = generateUniqueName();
        if (isNamespaceEnabled) {
            INDEXES_LIST_NAMESPACE_SIMPLIFIED[0] = "TEST:" + indexOneMockOne;
        } else {
            INDEXES_LIST_SIMPLIFIED[0] = "TEST." + indexOneMockOne;
        }

        conn.createStatement().execute("CREATE INDEX " + indexOneMockOne + " ON " + mockTableOne +
            " (sal, a.name)");
        conn.commit();
        iut = new IndexUpgradeTool(upgrade ? UPGRADE_OP : ROLLBACK_OP, INPUT_LIST,
            null,Files.createTempFile(tmpDir, "index_upgrade_", null).toString(),
            true, indexToolMock, rebuild);
        iut.setConf(getUtility().getConfiguration());
        iut.setTest(true);
    }

    private void validate(boolean pre, boolean isSimplified) throws IOException {
        String [] indexList;
        String [] tableList;
        if (isSimplified) {
            if (isNamespaceEnabled) {
                indexList = INDEXES_LIST_NAMESPACE_SIMPLIFIED;
                tableList = TABLE_LIST_NAMESPACE_SIMPLIFIED;
            } else {
                indexList = INDEXES_LIST_SIMPLIFIED;
                tableList = TABLE_LIST_SIMPLIFIED;
            }
        } else {
            if (isNamespaceEnabled) {
                indexList = INDEXES_LIST_NAMESPACE;
                tableList = TABLE_LIST_NAMESPACE;
            } else {
                indexList = INDEXES_LIST;
                tableList = TABLE_LIST;
            }
        }
        if (pre) {
            if (upgrade) {
                checkOldIndexingCoprocessors(indexList,tableList);
            } else {
                checkNewIndexingCoprocessors(indexList,tableList);
            }
        } else {
            if (upgrade) {
                checkNewIndexingCoprocessors(indexList,tableList);
            } else {
                checkOldIndexingCoprocessors(indexList,tableList);
            }
        }
    }

    private void checkNewIndexingCoprocessors(String [] indexList, String [] tableList)
            throws IOException {
        if (mutable) {
            for (String table : tableList) {
                if (table != null) {
                    TableDescriptor indexDesc = admin.getDescriptor(TableName.valueOf(table));
                    Assert.assertTrue("Can't find IndexRegionObserver for " + table,
                        indexDesc.hasCoprocessor(IndexRegionObserver.class.getName()));
                    Assert.assertFalse("Found Indexer on " + table,
                        indexDesc.hasCoprocessor(Indexer.class.getName()));
                    IndexTestUtil.assertCoprocConfig(indexDesc, IndexRegionObserver.class.getName(),
                        IndexCoprocIT.INDEX_REGION_OBSERVER_CONFIG);
                }
            }
        }
        for (String index : indexList) {
            if (index != null) {
                TableDescriptor indexDesc = admin.getDescriptor(TableName.valueOf(index));
                Assert.assertTrue("Couldn't find GlobalIndexChecker on " + index,
                    indexDesc.hasCoprocessor(GlobalIndexChecker.class.getName()));
                IndexTestUtil.assertCoprocConfig(indexDesc, GlobalIndexChecker.class.getName(),
                    IndexCoprocIT.GLOBAL_INDEX_CHECKER_CONFIG);
            }
        }
        // Transactional indexes should not have new coprocessors
        for (String index : TRANSACTIONAL_INDEXES_LIST) {
            if (index != null) {
            Assert.assertFalse("Found GlobalIndexChecker on transactional index " + index,
                admin.getDescriptor(TableName.valueOf(index))
                    .hasCoprocessor(GlobalIndexChecker.class.getName()));
            }
        }
        for (String table : TRANSACTIONAL_TABLE_LIST) {
            if (table != null) {
            Assert.assertFalse("Found IndexRegionObserver on transactional table",
                admin.getDescriptor(TableName.valueOf(table))
                    .hasCoprocessor(IndexRegionObserver.class.getName()));
            }
        }
    }

    private void checkOldIndexingCoprocessors(String [] indexList, String [] tableList)
            throws IOException {
        if (mutable) {
            for (String table : tableList) {
                TableDescriptor indexDesc = admin.getDescriptor(TableName.valueOf(table));
                Assert.assertTrue("Can't find Indexer for " + table,
                    indexDesc.hasCoprocessor(Indexer.class.getName()));
                Assert.assertFalse("Found IndexRegionObserver on " + table,
                    indexDesc.hasCoprocessor(IndexRegionObserver.class.getName()));
                IndexTestUtil.assertCoprocConfig(indexDesc, Indexer.class.getName(),
                    IndexCoprocIT.INDEXER_CONFIG);
            }
        }
        for (String index : indexList) {
            Assert.assertFalse("Found GlobalIndexChecker on " + index,
                admin.getDescriptor(TableName.valueOf(index))
                    .hasCoprocessor(GlobalIndexChecker.class.getName()));
        }
    }

    @Parameters(name ="IndexUpgradeToolIT_mutable={0},upgrade={1},isNamespaceEnabled={2}, rebuild={3}")
    public static synchronized Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
            {false, false, true, false},
            {true, false, false, true},
            {false, true, false, false},
            {true, true, true, true}
        });
    }

    public ParameterizedIndexUpgradeToolIT(boolean mutable, boolean upgrade,
                                           boolean isNamespaceEnabled, boolean rebuild) {
        this.mutable = mutable;
        this.upgrade = upgrade;
        this.isNamespaceEnabled = isNamespaceEnabled;
        this.rebuild = rebuild;
    }

    @Test
    public void testNonDryRunToolWithMultiTables() throws Exception {
        prepareFullSetup(); //test with all tables
        validate(true, false);
        iut.setDryRun(false);
        iut.setLogFile(null);
        iut.prepareToolSetup();
        iut.executeTool();
        //testing actual run
        validate(false, false);
        // testing if tool waited in case of immutable tables
        if (!mutable) {
            Assert.assertEquals("Index upgrade tool didn't wait for client cache to expire "
                    + "for immutable tables", true, iut.getIsWaitComplete());
        } else {
            Assert.assertEquals("Index upgrade tool waited for client cache to expire "
                    + "for mutable tables", false, iut.getIsWaitComplete());
        }
        if(upgrade && rebuild) {
            //verifying if index tool was started
            Mockito.verify(indexToolMock,
                    times(11)) // for every index/view-index except index on transaction table
                    .run(argCapture.capture());
        } else {
            Mockito.verifyNoMoreInteractions(indexToolMock);
        }
    }

    @Test
    public void testDryRunAndFailures() throws Exception {
        prepareFullSetup(); //test with all tables
        validate(true, false);

        // test with incorrect table
        iut.setInputTables("TEST3.TABLE_NOT_PRESENT");
        iut.prepareToolSetup();

        int status = iut.executeTool();
        Assert.assertEquals(-1, status);
        validate(true, false);

        // test with input file parameter
        Path inputPath = Paths.get(tmpDir.toString(), "input_file_index_upgrade.csv");
        BufferedWriter writer = new BufferedWriter(new FileWriter(inputPath.toFile()));
        writer.write(INPUT_LIST);
        writer.close();

        iut.setInputTables(null);
        iut.setInputFile(inputPath.toString());
        iut.prepareToolSetup();
        status = iut.executeTool();
        Assert.assertEquals(0, status);

        validate(true, false);

        // test table without index
        if (upgrade && !isNamespaceEnabled) {
            conn.createStatement().execute("CREATE TABLE TEST.NEW_TABLE (id bigint NOT NULL "
                    + "PRIMARY KEY, a.name varchar, sal bigint, address varchar)" + tableDDLOptions);
            iut.setInputTables("TEST.NEW_TABLE");
            iut.prepareToolSetup();
            status = iut.executeTool();
            Assert.assertEquals(0, status);
            conn.createStatement().execute("DROP TABLE TEST.NEW_TABLE");
        }
    }

    @Test
    public void testRollbackAfterFailure() throws Exception {
        prepareSimplifiedSetup(); //only need one table and index to verify rollback
        validate(true, true);
        if (upgrade) {
            iut.setFailUpgradeTask(true);
        } else {
            iut.setFailDowngradeTask(true);
        }
        iut.prepareToolSetup();
        int status = iut.executeTool();
        Assert.assertEquals(-1, status);
        //should have rolled back and be in the same status we started with
        validate(true, true);
    }

    @Test
    public void testTableReenableAfterDoubleFailure() throws Exception {
        prepareSimplifiedSetup(); //only need one table and index to verify re-enabling
        validate(true, true);
        //this will force the upgrade/downgrade to fail, and then the rollback to fail too
        //we want to make sure that even then, we'll try to re-enable the HBase tables
        iut.setFailUpgradeTask(true);
        iut.setFailDowngradeTask(true);
        iut.prepareToolSetup();
        try {
            iut.executeTool();
        } catch (RuntimeException e) {
            //double failures throw an exception so that the tool stops immediately
            validateTablesEnabled(INPUT_LIST);
            return;
        }
        Assert.fail("Should have thrown an exception!");
    }

    private void validateTablesEnabled(String inputList) throws IOException, SQLException {
        Admin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin();
        String[] tableNames = inputList.split(",");
        Assert.assertNotNull(tableNames);
        Assert.assertTrue(tableNames.length > 0);
        for (String tableName : tableNames) {
            String physicalTableName =
                SchemaUtil.getPhysicalHBaseTableName(SchemaUtil.getSchemaNameFromFullName(tableName),
                    SchemaUtil.getTableNameFromFullName(tableName), isNamespaceEnabled).getString();
            Assert.assertTrue(admin.isTableEnabled(TableName.valueOf(physicalTableName)));
        }
    }

}