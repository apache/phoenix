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
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
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
import org.mockito.Mockito;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
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
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;

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

    private StringBuilder optionsBuilder;
    private String tableDDLOptions;
    private Connection conn;
    private Connection connTenant;
    private Admin admin;
    private IndexUpgradeTool iut;

    @Before
    public void setup () throws Exception {
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
                true, Mockito.mock(IndexTool.class));
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
                + " TRANSACTIONAL=true "//", TRANSACTION_PROVIDER='TEPHRA' "
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
                Assert.assertTrue(admin.getTableDescriptor(TableName.valueOf(table))
                        .hasCoprocessor(IndexRegionObserver.class.getName()));
                Assert.assertFalse(admin.getTableDescriptor(TableName.valueOf(table))
                        .hasCoprocessor(Indexer.class.getName()));
            }
        }
        for (String index : indexList) {
            Assert.assertTrue(admin.getTableDescriptor(TableName.valueOf(index))
                    .hasCoprocessor(GlobalIndexChecker.class.getName()));
        }
        // Transactional indexes should not have new coprocessors
        for (String index : TRANSACTIONAL_INDEXES_LIST) {
            Assert.assertFalse(admin.getTableDescriptor(TableName.valueOf(index))
                    .hasCoprocessor(GlobalIndexChecker.class.getName()));
        }
        for (String table : TRANSACTIONAL_TABLE_LIST) {
            Assert.assertFalse(admin.getTableDescriptor(TableName.valueOf(table))
                    .hasCoprocessor(IndexRegionObserver.class.getName()));
        }
    }

    private void checkOldIndexingCoprocessors(String [] indexList, String [] tableList)
            throws IOException {
        if (mutable) {
            for (String table : tableList) {
                Assert.assertTrue(admin.getTableDescriptor(TableName.valueOf(table))
                        .hasCoprocessor(Indexer.class.getName()));
                Assert.assertFalse(admin.getTableDescriptor(TableName.valueOf(table))
                        .hasCoprocessor(IndexRegionObserver.class.getName()));
            }
        }
        for (String index : indexList) {
            Assert.assertFalse(admin.getTableDescriptor(TableName.valueOf(index))
                    .hasCoprocessor(GlobalIndexChecker.class.getName()));
        }
    }

    @Parameters(name ="IndexUpgradeToolIT_mutable={0},upgrade={1},isNamespaceEnabled={2}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
            {false, false, true},
            {true, false, false},
            {false, true, false},
            {true, true, true}
        });
    }

    public ParameterizedIndexUpgradeToolIT(boolean mutable, boolean upgrade, boolean isNamespaceEnabled) {
        this.mutable = mutable;
        this.upgrade = upgrade;
        this.isNamespaceEnabled = isNamespaceEnabled;
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
    }

    @Test
    public void testToolWithIncorrectTables() throws Exception {
        validate(true);
        iut.setInputTables("TEST3.TABLE_NOT_PRESENT");
        iut.prepareToolSetup();

        int status = iut.executeTool();
        Assert.assertEquals(-1, status);
        validate(true);
    }

    @Test
    public void testToolWithInputFileParameter() throws Exception {
        BufferedWriter writer = new BufferedWriter(new FileWriter(new File(INPUT_FILE)));
        writer.write(INPUT_LIST);
        writer.close();

        validate(true);

        iut.setInputTables(null);
        iut.setInputFile(INPUT_FILE);
        iut.prepareToolSetup();
        iut.executeTool();

        validate(true);
    }

    @After
    public void cleanup() throws SQLException {
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
    }
}