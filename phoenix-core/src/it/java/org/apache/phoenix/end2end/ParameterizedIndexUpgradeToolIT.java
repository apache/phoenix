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

import com.google.common.collect.Maps;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.util.Bytes;
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
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
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
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
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
    private static final String [] INDEXES_LIST = {"TEST.INDEX1", "TEST.INDEX2", "TEST1.INDEX3",
            "TEST1.INDEX2","TEST1.INDEX1","TEST.INDEX3", "_IDX_TEST.MOCK1", "_IDX_TEST1.MOCK2"};
    private static final String [] INDEXES_LIST_NAMESPACE = {"TEST:INDEX1", "TEST:INDEX2"
            , "TEST1:INDEX3", "TEST1:INDEX2","TEST1:INDEX1"
            , "TEST:INDEX3", "TEST:_IDX_MOCK1", "TEST1:_IDX_MOCK2"};
    private static final String [] TRANSACTIONAL_INDEXES_LIST = {"TRANSACTIONAL_INDEX",
            "_IDX_TRANSACTIONAL_TABLE"};

    private static final String [] TABLE_LIST = {"TEST.MOCK1","TEST1.MOCK2","TEST.MOCK3","TEST.MULTI_TENANT_TABLE"};
    private static final String [] TABLE_LIST_NAMESPACE = {"TEST:MOCK1","TEST1:MOCK2","TEST:MOCK3",
            "TEST:MULTI_TENANT_TABLE"};

    private static final String [] TRANSACTIONAL_TABLE_LIST = {"TRANSACTIONAL_TABLE"};
    private static final String INPUT_LIST = "TEST.MOCK1,TEST1.MOCK2,TEST.MOCK3,TEST.MULTI_TENANT_TABLE";
    private static final String INPUT_FILE = "/tmp/input_file_index_upgrade.csv";

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

    @Mock
    private IndexTool indexToolMock;

    @Captor
    private ArgumentCaptor<String []> argCapture;

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
        iut = new IndexUpgradeTool(upgrade ? UPGRADE_OP : ROLLBACK_OP, INPUT_LIST,
                null, "/tmp/index_upgrade_" + UUID.randomUUID().toString(),
                true, indexToolMock, rebuild);
        iut.setConf(getUtility().getConfiguration());
        iut.setTest(true);
        if (!mutable) {
            optionsBuilder.append(" IMMUTABLE_ROWS=true");
        }
        tableDDLOptions = optionsBuilder.toString();
        prepareSetup();
    }

    private void setClusterProperties() {
        // we need to destroy the cluster if it was initiated using property as true
        if (Boolean.toString(upgrade).equals(clientProps
                .get(QueryServices.INDEX_REGION_OBSERVER_ENABLED_ATTRIB))
                || Boolean.toString(!isNamespaceEnabled).equals(serverProps
                .get(QueryServices.IS_NAMESPACE_MAPPING_ENABLED))) {
            tearDownMiniClusterAsync(1);
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

    private void prepareSetup() throws SQLException {
        //inputList is "TEST.MOCK1,TEST1.MOCK2,TEST.MOCK3";
        if (isNamespaceEnabled) {
            conn.createStatement().execute("CREATE SCHEMA TEST");
            conn.createStatement().execute("CREATE SCHEMA TEST1");
        }
        conn.createStatement().execute("CREATE TABLE TEST.MOCK1 (id bigint NOT NULL "
                + "PRIMARY KEY, a.name varchar, sal bigint, address varchar)" + tableDDLOptions);
        conn.createStatement().execute("CREATE TABLE TEST1.MOCK2 (id bigint NOT NULL "
                + "PRIMARY KEY, name varchar, city varchar, phone bigint)" + tableDDLOptions);
        conn.createStatement().execute("CREATE TABLE TEST.MOCK3 (id bigint NOT NULL "
                + "PRIMARY KEY, name varchar, age bigint)" + tableDDLOptions);
        String
                createTblStr =
                "CREATE TABLE TEST.MULTI_TENANT_TABLE " + " (TENANT_ID VARCHAR(15) NOT NULL,ID INTEGER NOT NULL"
                        + ", NAME VARCHAR, CONSTRAINT PK_1 PRIMARY KEY (TENANT_ID, ID)) MULTI_TENANT=true";
        conn.createStatement().execute(createTblStr);
        conn.createStatement().execute("CREATE TABLE TRANSACTIONAL_TABLE(id bigint NOT NULL "
                        + "PRIMARY KEY, a.name varchar, sal bigint, address varchar) "
                + " TRANSACTIONAL=true "
                + ((tableDDLOptions.trim().length() > 0) ? "," : "") + tableDDLOptions);

        //views
        conn.createStatement().execute("CREATE VIEW TEST.MOCK1_VIEW (view_column varchar) "
                + "AS SELECT * FROM TEST.MOCK1 WHERE a.name = 'a'");
        conn.createStatement().execute("CREATE VIEW TEST.MOCK1_VIEW1 (view_column varchar,"
                + " zip varchar) AS SELECT * FROM TEST.MOCK1 WHERE a.name = 'a'");
        conn.createStatement().execute("CREATE VIEW TEST1.MOCK2_VIEW (view_column varchar,"
                + " state varchar) AS SELECT * FROM TEST1.MOCK2 WHERE name = 'c'");
        conn.createStatement().execute("CREATE VIEW TRANSACTIONAL_VIEW (view_column varchar,"
                + " state varchar) AS SELECT * FROM TRANSACTIONAL_TABLE WHERE name = 'c'");

        //view-indexes
        conn.createStatement().execute("CREATE INDEX MOCK1_INDEX1 ON TEST.MOCK1_VIEW1 " + "(view_column)");
        conn.createStatement().execute("CREATE INDEX MOCK1_INDEX2 ON TEST.MOCK1_VIEW1 " + "(zip)");
        conn.createStatement().execute("CREATE INDEX MOCK2_INDEX1 ON TEST1.MOCK2_VIEW " + "(state, city)");
        conn.createStatement().execute("CREATE INDEX MOCK1_INDEX3 ON TEST.MOCK1_VIEW " + "(view_column)");
        conn.createStatement().execute("CREATE INDEX TRANSACTIONAL_VIEW_INDEX ON TRANSACTIONAL_VIEW " + "(view_column)");
        //indexes
        conn.createStatement().execute("CREATE INDEX INDEX1 ON TEST.MOCK1 (sal, a.name)");
        conn.createStatement().execute("CREATE INDEX INDEX2 ON TEST.MOCK1 (a.name)");
        conn.createStatement().execute("CREATE INDEX INDEX1 ON TEST1.MOCK2 (city)");
        conn.createStatement().execute("CREATE INDEX INDEX2 ON TEST1.MOCK2 (phone)");
        conn.createStatement().execute("CREATE INDEX INDEX3 ON TEST1.MOCK2 (name)");
        conn.createStatement().execute("CREATE INDEX INDEX3 ON TEST.MOCK3 (age, name)");
        conn.createStatement().execute("CREATE INDEX TRANSACTIONAL_INDEX ON TRANSACTIONAL_TABLE(sal)");

        // Tenant ones
        connTenant.createStatement().execute(
                "CREATE VIEW TEST.TEST_TENANT_VIEW AS SELECT * FROM TEST.MULTI_TENANT_TABLE");
        connTenant.createStatement().execute("CREATE INDEX MULTI_TENANT_INDEX ON TEST.TEST_TENANT_VIEW (NAME)");

        conn.createStatement().execute("ALTER INDEX MOCK1_INDEX2 ON TEST.MOCK1_VIEW1 DISABLE");
        connTenant.createStatement().execute("ALTER INDEX MULTI_TENANT_INDEX ON TEST.TEST_TENANT_VIEW DISABLE");
        conn.createStatement().execute("ALTER INDEX INDEX2 ON TEST.MOCK1 DISABLE");
    }

    private void validate(boolean pre) throws IOException {
        String [] indexList = INDEXES_LIST;
        String [] tableList = TABLE_LIST;
        if(isNamespaceEnabled) {
            indexList = INDEXES_LIST_NAMESPACE;
            tableList = TABLE_LIST_NAMESPACE;
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
                HTableDescriptor indexDesc = admin.getTableDescriptor(TableName.valueOf(table));
                Assert.assertTrue("Can't find IndexRegionObserver for " + table,
                    indexDesc.hasCoprocessor(IndexRegionObserver.class.getName()));
                Assert.assertFalse("Found Indexer on " + table,
                        indexDesc.hasCoprocessor(Indexer.class.getName()));
                IndexCoprocIT.assertCoprocConfig(indexDesc, IndexRegionObserver.class.getName(),
                    IndexCoprocIT.INDEX_REGION_OBSERVER_CONFIG);
            }

        }
        for (String index : indexList) {
            HTableDescriptor indexDesc = admin.getTableDescriptor(TableName.valueOf(index));
            Assert.assertTrue("Couldn't find GlobalIndexChecker on " + index,
                indexDesc.hasCoprocessor(GlobalIndexChecker.class.getName()));
            IndexCoprocIT.assertCoprocConfig(indexDesc, GlobalIndexChecker.class.getName(),
                IndexCoprocIT.GLOBAL_INDEX_CHECKER_CONFIG);
        }
        // Transactional indexes should not have new coprocessors
        for (String index : TRANSACTIONAL_INDEXES_LIST) {
            Assert.assertFalse("Found GlobalIndexChecker on transactional index " + index,
                admin.getTableDescriptor(TableName.valueOf(index))
                    .hasCoprocessor(GlobalIndexChecker.class.getName()));
        }
        for (String table : TRANSACTIONAL_TABLE_LIST) {
            Assert.assertFalse("Found IndexRegionObserver on transactional table",
                admin.getTableDescriptor(TableName.valueOf(table))
                    .hasCoprocessor(IndexRegionObserver.class.getName()));
        }
    }

    private void checkOldIndexingCoprocessors(String [] indexList, String [] tableList)
            throws IOException {
        if (mutable) {
            for (String table : tableList) {
                HTableDescriptor indexDesc = admin.getTableDescriptor(TableName.valueOf(table));
                Assert.assertTrue("Can't find Indexer for " + table,
                    indexDesc.hasCoprocessor(Indexer.class.getName()));
                Assert.assertFalse("Found IndexRegionObserver on " + table,
                    indexDesc.hasCoprocessor(IndexRegionObserver.class.getName()));
                IndexCoprocIT.assertCoprocConfig(indexDesc, Indexer.class.getName(),
                    IndexCoprocIT.INDEXER_CONFIG);
            }
        }
        for (String index : indexList) {
            Assert.assertFalse("Found GlobalIndexChecker on " + index,
                admin.getTableDescriptor(TableName.valueOf(index))
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
        validate(true);
        iut.setDryRun(false);
        iut.setLogFile(null);
        iut.prepareToolSetup();
        iut.executeTool();
        //testing actual run
        validate(false);
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
            Mockito.verifyZeroInteractions(indexToolMock);
        }
    }

    @Test
    public void testDryRunAndFailures() throws Exception {
        validate(true);

        // test with incorrect table
        iut.setInputTables("TEST3.TABLE_NOT_PRESENT");
        iut.prepareToolSetup();

        int status = iut.executeTool();
        Assert.assertEquals(-1, status);
        validate(true);

        // test with input file parameter
        BufferedWriter writer = new BufferedWriter(new FileWriter(new File(INPUT_FILE)));
        writer.write(INPUT_LIST);
        writer.close();

        iut.setInputTables(null);
        iut.setInputFile(INPUT_FILE);
        iut.prepareToolSetup();
        status = iut.executeTool();
        Assert.assertEquals(0, status);

        validate(true);

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
        validate(true);
        if (upgrade) {
            iut.setFailUpgradeTask(true);
        } else {
            iut.setFailDowngradeTask(true);
        }
        iut.prepareToolSetup();
        int status = iut.executeTool();
        Assert.assertEquals(-1, status);
        //should have rolled back and be in the same status we started with
        validate(true);
    }

    @Test
    public void testTableReenableAfterDoubleFailure() throws Exception {
        validate(true);
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

    @After
    public void cleanup() throws IOException, SQLException {
        if (conn == null) {
            return;
        }
        //TEST.MOCK1,TEST1.MOCK2,TEST.MOCK3
        conn.createStatement().execute("DROP INDEX INDEX1 ON TEST.MOCK1");
        conn.createStatement().execute("DROP INDEX INDEX2 ON TEST.MOCK1");
        conn.createStatement().execute("DROP INDEX INDEX1 ON TEST1.MOCK2");
        conn.createStatement().execute("DROP INDEX INDEX2 ON TEST1.MOCK2");
        conn.createStatement().execute("DROP INDEX INDEX3 ON TEST1.MOCK2");
        conn.createStatement().execute("DROP INDEX INDEX3 ON TEST.MOCK3");
        connTenant.createStatement().execute("DROP INDEX MULTI_TENANT_INDEX ON TEST.TEST_TENANT_VIEW");
        conn.createStatement().execute("DROP INDEX TRANSACTIONAL_INDEX ON TRANSACTIONAL_TABLE");

        conn.createStatement().execute("DROP INDEX MOCK1_INDEX3 ON TEST.MOCK1_VIEW");
        conn.createStatement().execute("DROP INDEX MOCK1_INDEX1 ON TEST.MOCK1_VIEW1");
        conn.createStatement().execute("DROP INDEX MOCK1_INDEX2 ON TEST.MOCK1_VIEW1");
        conn.createStatement().execute("DROP INDEX MOCK2_INDEX1 ON TEST1.MOCK2_VIEW");
        conn.createStatement().execute("DROP INDEX TRANSACTIONAL_VIEW_INDEX ON TRANSACTIONAL_VIEW");

        conn.createStatement().execute("DROP VIEW TEST.MOCK1_VIEW");
        conn.createStatement().execute("DROP VIEW TEST.MOCK1_VIEW1");
        conn.createStatement().execute("DROP VIEW TEST1.MOCK2_VIEW");
        conn.createStatement().execute("DROP VIEW TRANSACTIONAL_VIEW");
        connTenant.createStatement().execute("DROP VIEW TEST.TEST_TENANT_VIEW");

        conn.createStatement().execute("DROP TABLE TEST.MOCK1");
        conn.createStatement().execute("DROP TABLE TEST1.MOCK2");
        conn.createStatement().execute("DROP TABLE TEST.MOCK3");
        conn.createStatement().execute("DROP TABLE TEST.MULTI_TENANT_TABLE");

        conn.createStatement().execute("DROP TABLE TRANSACTIONAL_TABLE");

        if (isNamespaceEnabled) {
            conn.createStatement().execute("DROP SCHEMA TEST");
            conn.createStatement().execute("DROP SCHEMA TEST1");
        }
        conn.close();
        connTenant.close();
        assertTableNotExists("TEST.MOCK1");
        assertTableNotExists("TEST1.MOCK2");
        assertTableNotExists("TEST.MOCK3");
        assertTableNotExists("TRANSACTIONAL_TABLE");
        assertTableNotExists("TEST.MULTI_TENANT_TABLE");
    }

    private void assertTableNotExists(String table) throws IOException {
        TableName tableName =
            SchemaUtil.getPhysicalTableName(Bytes.toBytes(table), isNamespaceEnabled);
        Assert.assertFalse("Table " + table + " exists when it shouldn't",
            admin.tableExists(tableName));
    }
}