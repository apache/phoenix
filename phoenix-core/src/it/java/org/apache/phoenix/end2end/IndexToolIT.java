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

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.mapreduce.index.IndexTool;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

@RunWith(Parameterized.class)
@Category(NeedsOwnMiniClusterTest.class)
public class IndexToolIT extends ParallelStatsEnabledIT {

    private final boolean localIndex;
    private final boolean transactional;
    private final boolean directApi;
    private final String tableDDLOptions;
    private final boolean mutable;
    private final boolean useSnapshot;

    public IndexToolIT(boolean transactional, boolean mutable, boolean localIndex,
            boolean directApi, boolean useSnapshot) {
        this.localIndex = localIndex;
        this.transactional = transactional;
        this.directApi = directApi;
        this.mutable = mutable;
        this.useSnapshot = useSnapshot;
        StringBuilder optionBuilder = new StringBuilder();
        if (!mutable) {
            optionBuilder.append(" IMMUTABLE_ROWS=true ");
        }
        if (transactional) {
            if (!(optionBuilder.length() == 0)) {
                optionBuilder.append(",");
            }
            optionBuilder.append(" TRANSACTIONAL=true ");
        }
        optionBuilder.append(" SPLIT ON(1,2)");
        this.tableDDLOptions = optionBuilder.toString();
    }

    @BeforeClass
    public static void setup() throws Exception {
        Map<String, String> serverProps = Maps.newHashMapWithExpectedSize(2);
        serverProps.put(QueryServices.EXTRA_JDBC_ARGUMENTS_ATTRIB,
            QueryServicesOptions.DEFAULT_EXTRA_JDBC_ARGUMENTS);
        Map<String, String> clientProps = Maps.newHashMapWithExpectedSize(2);
        clientProps.put(QueryServices.TRANSACTIONS_ENABLED, Boolean.TRUE.toString());
        clientProps.put(QueryServices.FORCE_ROW_KEY_ORDER_ATTRIB, Boolean.TRUE.toString());
        setUpTestDriver(new ReadOnlyProps(serverProps.entrySet().iterator()),
            new ReadOnlyProps(clientProps.entrySet().iterator()));
    }

    @Parameters(
            name = "transactional = {0} , mutable = {1} , localIndex = {2}, directApi = {3}, useSnapshot = {4}")
    public static Collection<Boolean[]> data() {
        List<Boolean[]> list = Lists.newArrayListWithExpectedSize(16);
        boolean[] Booleans = new boolean[] { false, true };
        for (boolean transactional : Booleans) {
            for (boolean mutable : Booleans) {
                for (boolean localIndex : Booleans) {
                    for (boolean directApi : Booleans) {
                        for (boolean useSnapshot : Booleans) {
                            list.add(new Boolean[] { transactional, mutable, localIndex, directApi, useSnapshot });
                        }
                    }
                }
            }
        }
        return list;
    }

    @Test
    public void testSecondaryIndex() throws Exception {
        String schemaName = generateUniqueName();
        String dataTableName = generateUniqueName();
        String dataTableFullName = SchemaUtil.getTableName(schemaName, dataTableName);
        String indexTableName = generateUniqueName();
        String indexTableFullName = SchemaUtil.getTableName(schemaName, indexTableName);
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            String stmString1 =
                    "CREATE TABLE " + dataTableFullName
                            + " (ID INTEGER NOT NULL PRIMARY KEY, NAME VARCHAR, ZIP INTEGER) "
                            + tableDDLOptions;
            conn.createStatement().execute(stmString1);
            String upsertQuery = String.format("UPSERT INTO %s VALUES(?, ?, ?)", dataTableFullName);
            PreparedStatement stmt1 = conn.prepareStatement(upsertQuery);

            // insert two rows
            upsertRow(stmt1, 1);
            upsertRow(stmt1, 2);
            conn.commit();

            if (transactional) {
                // insert two rows in another connection without committing so that they are not
                // visible to other transactions
                try (Connection conn2 = DriverManager.getConnection(getUrl(), props)) {
                    conn2.setAutoCommit(false);
                    PreparedStatement stmt2 = conn2.prepareStatement(upsertQuery);
                    upsertRow(stmt2, 5);
                    upsertRow(stmt2, 6);
                    ResultSet rs =
                            conn.createStatement()
                                    .executeQuery("SELECT count(*) from " + dataTableFullName);
                    assertTrue(rs.next());
                    assertEquals("Unexpected row count ", 2, rs.getInt(1));
                    assertFalse(rs.next());
                    rs =
                            conn2.createStatement()
                                    .executeQuery("SELECT count(*) from " + dataTableFullName);
                    assertTrue(rs.next());
                    assertEquals("Unexpected row count ", 4, rs.getInt(1));
                    assertFalse(rs.next());
                }
            }

            String stmtString2 =
                    String.format(
                        "CREATE %s INDEX %s ON %s  (LPAD(UPPER(NAME, 'en_US'),8,'x')||'_xyz') ASYNC ",
                        (localIndex ? "LOCAL" : ""), indexTableName, dataTableFullName);
            conn.createStatement().execute(stmtString2);

            // verify rows are fetched from data table.
            String selectSql =
                    String.format(
                        "SELECT ID FROM %s WHERE LPAD(UPPER(NAME, 'en_US'),8,'x')||'_xyz' = 'xxUNAME2_xyz'",
                        dataTableFullName);
            ResultSet rs = conn.createStatement().executeQuery("EXPLAIN " + selectSql);
            String actualExplainPlan = QueryUtil.getExplainPlan(rs);

            // assert we are pulling from data table.
            assertEquals(String.format(
                "CLIENT PARALLEL 1-WAY FULL SCAN OVER %s\n"
                        + "    SERVER FILTER BY (LPAD(UPPER(NAME, 'en_US'), 8, 'x') || '_xyz') = 'xxUNAME2_xyz'",
                dataTableFullName), actualExplainPlan);

            rs = stmt1.executeQuery(selectSql);
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            assertFalse(rs.next());
            conn.commit();

            // run the index MR job.
            runIndexTool(directApi, useSnapshot, schemaName, dataTableName, indexTableName);

            // insert two more rows
            upsertRow(stmt1, 3);
            upsertRow(stmt1, 4);
            conn.commit();

            // assert we are pulling from index table.
            rs = conn.createStatement().executeQuery("EXPLAIN " + selectSql);
            actualExplainPlan = QueryUtil.getExplainPlan(rs);
            assertExplainPlan(localIndex, actualExplainPlan, dataTableFullName, indexTableFullName);

            rs = conn.createStatement().executeQuery(selectSql);
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testSaltedVariableLengthPK() throws Exception {
        String schemaName = generateUniqueName();
        String dataTableName = generateUniqueName();
        String dataTableFullName = SchemaUtil.getTableName(schemaName, dataTableName);
        String indexTableName = generateUniqueName();
        try (Connection conn =
                DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES))) {
            String dataDDL =
                    "CREATE TABLE " + dataTableFullName + "(\n"
                            + "ID VARCHAR NOT NULL PRIMARY KEY,\n"
                            + "\"info\".CAR_NUM VARCHAR(18) NULL,\n"
                            + "\"info\".CAP_DATE VARCHAR NULL,\n" + "\"info\".ORG_ID BIGINT NULL,\n"
                            + "\"info\".ORG_NAME VARCHAR(255) NULL\n" + ") SALT_BUCKETS=3";
            conn.createStatement().execute(dataDDL);

            String upsert =
                    "UPSERT INTO " + dataTableFullName
                            + "(ID,CAR_NUM,CAP_DATE,ORG_ID,ORG_NAME) VALUES('1','car1','2016-01-01 00:00:00',11,'orgname1')";
            conn.createStatement().execute(upsert);
            conn.commit();

            String indexDDL =
                    String.format(
                        "CREATE %s INDEX %s on %s (\"info\".CAR_NUM,\"info\".CAP_DATE) ASYNC",
                        (localIndex ? "LOCAL" : ""), indexTableName, dataTableFullName);
            conn.createStatement().execute(indexDDL);

            runIndexTool(directApi, useSnapshot, schemaName, dataTableName, indexTableName);

            ResultSet rs =
                    conn.createStatement().executeQuery(
                        "SELECT ORG_ID,CAP_DATE,CAR_NUM,ORG_NAME FROM " + dataTableFullName
                                + " WHERE CAR_NUM='car1' AND CAP_DATE>='2016-01-01' AND CAP_DATE<='2016-05-02' LIMIT 10");
            assertTrue(rs.next());
            int orgId = rs.getInt(1);
            assertEquals(11, orgId);
        }
    }

    /**
     * Test presplitting an index table
     */
    @Test
    public void testSplitIndex() throws Exception {
        if (localIndex) return; // can't split local indexes
        String schemaName = generateUniqueName();
        String dataTableName = generateUniqueName();
        String dataTableFullName = SchemaUtil.getTableName(schemaName, dataTableName);
        final TableName dataTN = TableName.valueOf(dataTableFullName);
        String indexTableName = generateUniqueName();
        String indexTableFullName = SchemaUtil.getTableName(schemaName, indexTableName);
        TableName indexTN = TableName.valueOf(indexTableFullName);
        try (Connection conn =
                DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES));
                Admin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
            String dataDDL =
                    "CREATE TABLE " + dataTableFullName + "(\n"
                            + "ID VARCHAR NOT NULL PRIMARY KEY,\n"
                            + "\"info\".CAR_NUM VARCHAR(18) NULL,\n"
                            + "\"test\".CAR_NUM VARCHAR(18) NULL,\n"
                            + "\"info\".CAP_DATE VARCHAR NULL,\n" + "\"info\".ORG_ID BIGINT NULL,\n"
                            + "\"info\".ORG_NAME VARCHAR(255) NULL\n" + ") COLUMN_ENCODED_BYTES = 0";
            conn.createStatement().execute(dataDDL);

            String[] carNumPrefixes = new String[] {"a", "b", "c", "d"};

            // split the data table, as the tool splits the index table to have the same # of regions
            // doesn't really matter what the split points are, we just want a target # of regions
            int numSplits = carNumPrefixes.length;
            int targetNumRegions = numSplits + 1;
            byte[][] splitPoints = new byte[numSplits][];
            for (String prefix : carNumPrefixes) {
                splitPoints[--numSplits] = Bytes.toBytes(prefix);
            }
            HTableDescriptor dataTD = admin.getTableDescriptor(dataTN);
            admin.disableTable(dataTN);
            admin.deleteTable(dataTN);
            admin.createTable(dataTD, splitPoints);
            assertEquals(targetNumRegions, admin.getTableRegions(dataTN).size());

            // insert data where index column values start with a, b, c, d
            int idCounter = 1;
            try (PreparedStatement ps = conn.prepareStatement("UPSERT INTO " + dataTableFullName
                + "(ID,\"info\".CAR_NUM,\"test\".CAR_NUM,CAP_DATE,ORG_ID,ORG_NAME) VALUES(?,?,?,'2016-01-01 00:00:00',11,'orgname1')")){
                for (String carNum : carNumPrefixes) {
                    for (int i = 0; i < 100; i++) {
                        ps.setString(1, idCounter++ + "");
                        ps.setString(2, carNum + "_" + i);
                        ps.setString(3, "test-" + carNum + "_ " + i);
                        ps.addBatch();
                    }
                }
                ps.executeBatch();
                conn.commit();
            }

            String indexDDL =
                    String.format(
                        "CREATE INDEX %s on %s (\"info\".CAR_NUM,\"test\".CAR_NUM,\"info\".CAP_DATE) ASYNC",
                        indexTableName, dataTableFullName);
            conn.createStatement().execute(indexDDL);

            // run with 50% sampling rate, split if data table more than 3 regions
            runIndexTool(directApi, useSnapshot, schemaName, dataTableName, indexTableName, "-sp", "50", "-spa", "3");

            assertEquals(targetNumRegions, admin.getTableRegions(indexTN).size());
            List<Cell> values = new ArrayList<>();
            // every index region should have been written to, if the index table was properly split uniformly
            for (HRegion region : getUtility().getHBaseCluster().getRegions(indexTN)) {
                values.clear();
                RegionScanner scanner = region.getScanner(new Scan());
                scanner.next(values);
                if (values.isEmpty()) {
                    fail("Region did not have any results: " + region.getRegionInfo());
                }
            }
        }
    }

    public static void assertExplainPlan(boolean localIndex, String actualExplainPlan,
            String dataTableFullName, String indexTableFullName) {
        String expectedExplainPlan;
        if (localIndex) {
            expectedExplainPlan = String.format(" RANGE SCAN OVER %s [1,", dataTableFullName);
        } else {
            expectedExplainPlan = String.format(" RANGE SCAN OVER %s", indexTableFullName);
        }
        assertTrue(actualExplainPlan + "\n expected to contain \n" + expectedExplainPlan,
            actualExplainPlan.contains(expectedExplainPlan));
    }

    public static String[] getArgValues(boolean directApi, boolean useSnapshot, String schemaName,
            String dataTable, String indxTable) {
        final List<String> args = Lists.newArrayList();
        if (schemaName != null) {
            args.add("-s");
            args.add(schemaName);
        }
        args.add("-dt");
        args.add(dataTable);
        args.add("-it");
        args.add(indxTable);
        if (directApi) {
            args.add("-direct");
            // Need to run this job in foreground for the test to be deterministic
            args.add("-runfg");
        }

        if (useSnapshot) {
            args.add("-snap");
        }

        args.add("-op");
        args.add("/tmp/" + UUID.randomUUID().toString());
        return args.toArray(new String[0]);
    }

    public static void upsertRow(PreparedStatement stmt, int i) throws SQLException {
        // insert row
        stmt.setInt(1, i);
        stmt.setString(2, "uname" + String.valueOf(i));
        stmt.setInt(3, 95050 + i);
        stmt.executeUpdate();
    }

    public static void runIndexTool(boolean directApi, boolean useSnapshot, String schemaName,
            String dataTableName, String indexTableName) throws Exception {
        runIndexTool(directApi, useSnapshot, schemaName, dataTableName, indexTableName, new String[0]);
    }

    public static void runIndexTool(boolean directApi, boolean useSnapshot, String schemaName,
            String dataTableName, String indexTableName, String... additionalArgs) throws Exception {
        IndexTool indexingTool = new IndexTool();
        Configuration conf = new Configuration(getUtility().getConfiguration());
        conf.set(QueryServices.TRANSACTIONS_ENABLED, Boolean.TRUE.toString());
        indexingTool.setConf(conf);
        final String[] cmdArgs =
                getArgValues(directApi, useSnapshot, schemaName, dataTableName, indexTableName);
        List<String> cmdArgList = new ArrayList<>(Arrays.asList(cmdArgs));
        cmdArgList.addAll(Arrays.asList(additionalArgs));
        int status = indexingTool.run(cmdArgList.toArray(new String[cmdArgList.size()]));
        assertEquals(0, status);
    }
}
