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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.mapreduce.index.IndexTool;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Tests for the {@link IndexTool}
 */
@RunWith(Parameterized.class)
@Category(NeedsOwnMiniClusterTest.class)
public class IndexExtendedIT extends BaseTest {
    private final boolean localIndex;
    private final boolean transactional;
    private final boolean directApi;
    private final String tableDDLOptions;
    private final boolean mutable;
    
    @AfterClass
    public static void doTeardown() throws Exception {
        tearDownMiniCluster();
    }

    public IndexExtendedIT(boolean transactional, boolean mutable, boolean localIndex, boolean directApi) {
        this.localIndex = localIndex;
        this.transactional = transactional;
        this.directApi = directApi;
        this.mutable = mutable;
        StringBuilder optionBuilder = new StringBuilder();
        if (!mutable) {
            optionBuilder.append(" IMMUTABLE_ROWS=true ");
        }
        if (transactional) {
            if (!(optionBuilder.length()==0)) {
                optionBuilder.append(",");
            }
            optionBuilder.append(" TRANSACTIONAL=true ");
        }
        optionBuilder.append(" SPLIT ON(1,2)");
        this.tableDDLOptions = optionBuilder.toString();
    }
    
    @BeforeClass
    public static void doSetup() throws Exception {
        Map<String, String> serverProps = Maps.newHashMapWithExpectedSize(2);
        serverProps.put(QueryServices.EXTRA_JDBC_ARGUMENTS_ATTRIB, QueryServicesOptions.DEFAULT_EXTRA_JDBC_ARGUMENTS);
        Map<String, String> clientProps = Maps.newHashMapWithExpectedSize(2);
        clientProps.put(QueryServices.TRANSACTIONS_ENABLED, Boolean.TRUE.toString());
        clientProps.put(QueryServices.FORCE_ROW_KEY_ORDER_ATTRIB, Boolean.TRUE.toString());
        setUpTestDriver(new ReadOnlyProps(serverProps.entrySet().iterator()), new ReadOnlyProps(clientProps.entrySet()
                .iterator()));
    }
    
    @Parameters(name="transactional = {0} , mutable = {1} , localIndex = {2}, directApi = {3}")
    public static Collection<Boolean[]> data() {
        return Arrays.asList(new Boolean[][] {     
                 { false, false, false, false }, { false, false, false, true }, { false, false, true, false }, { false, false, true, true }, 
                 { false, true, false, false }, { false, true, false, true }, { false, true, true, false }, { false, true, true, true }, 
                 { true, false, false, false }, { true, false, false, true }, { true, false, true, false }, { true, false, true, true }, 
                 { true, true, false, false }, { true, true, false, true }, { true, true, true, false }, { true, true, true, true } 
           });
    }
    
    /**
     * This test is to assert that updates that happen to rows of a mutable table after an index is created in ASYNC mode and before
     * the MR job runs, do show up in the index table . 
     * @throws Exception
     */
    @Test
    public void testMutableIndexWithUpdates() throws Exception {
        if (!mutable || transactional) {
            return;
        }
        String schemaName = generateUniqueName();
        String dataTableName = generateUniqueName();
        String dataTableFullName = SchemaUtil.getTableName(schemaName, dataTableName);
        String indexTableName = generateUniqueName();
        String indexTableFullName = SchemaUtil.getTableName(schemaName, indexTableName);
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        Statement stmt = conn.createStatement();
        try {
            stmt.execute(String.format("CREATE TABLE %s (ID INTEGER NOT NULL PRIMARY KEY, NAME VARCHAR, ZIP INTEGER)",dataTableFullName));
            String upsertQuery = String.format("UPSERT INTO %s VALUES(?, ?, ?)",dataTableFullName);
            PreparedStatement stmt1 = conn.prepareStatement(upsertQuery);
            
            int id = 1;
            // insert two rows
            IndexExtendedIT.upsertRow(stmt1, id++);
            IndexExtendedIT.upsertRow(stmt1, id++);
            conn.commit();
            
            stmt.execute(String.format("CREATE " + (localIndex ? "LOCAL" : "") + " INDEX %s ON %s (UPPER(NAME)) ASYNC ", indexTableName,dataTableFullName));
            
            //update a row 
            stmt1.setInt(1, 1);
            stmt1.setString(2, "uname" + String.valueOf(10));
            stmt1.setInt(3, 95050 + 1);
            stmt1.executeUpdate();
            conn.commit();  
            
            //verify rows are fetched from data table.
            String selectSql = String.format("SELECT ID FROM %s WHERE UPPER(NAME) ='UNAME2'",dataTableFullName);
            ResultSet rs = conn.createStatement().executeQuery("EXPLAIN " + selectSql);
            String actualExplainPlan = QueryUtil.getExplainPlan(rs);
            
            //assert we are pulling from data table.
            assertEquals(String.format("CLIENT PARALLEL 1-WAY FULL SCAN OVER %s\n" +
                    "    SERVER FILTER BY UPPER(NAME) = 'UNAME2'",dataTableFullName),actualExplainPlan);
            
            rs = stmt1.executeQuery(selectSql);
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            assertFalse(rs.next());
           
            //run the index MR job.
            runIndexTool(schemaName, dataTableName, indexTableName);
            
            //assert we are pulling from index table.
            rs = conn.createStatement().executeQuery("EXPLAIN " + selectSql);
            actualExplainPlan = QueryUtil.getExplainPlan(rs);
            // TODO: why is it a 1-WAY parallel scan only for !transactional && mutable && localIndex
            assertExplainPlan(actualExplainPlan, dataTableFullName, indexTableFullName);
            
            rs = stmt.executeQuery(selectSql);
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    private void runIndexTool(String schemaName, String dataTableName, String indexTableName) throws Exception {
        IndexTool indexingTool = new IndexTool();
        Configuration conf = new Configuration(getUtility().getConfiguration());
        conf.set(QueryServices.TRANSACTIONS_ENABLED, Boolean.TRUE.toString());
        indexingTool.setConf(conf);
        final String[] cmdArgs = getArgValues(schemaName, dataTableName, indexTableName);
        int status = indexingTool.run(cmdArgs);
        assertEquals(0, status);
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
            String stmString1 = "CREATE TABLE " + dataTableFullName + " (ID INTEGER NOT NULL PRIMARY KEY, NAME VARCHAR, ZIP INTEGER) " +  tableDDLOptions;
            conn.createStatement().execute(stmString1);
            String upsertQuery = String.format("UPSERT INTO %s VALUES(?, ?, ?)", dataTableFullName);
            PreparedStatement stmt1 = conn.prepareStatement(upsertQuery);
            
            // insert two rows
            upsertRow(stmt1, 1);
            upsertRow(stmt1, 2);
            conn.commit();
            
            if (transactional) {
                // insert two rows in another connection without committing so that they are not visible to other transactions
                try (Connection conn2 = DriverManager.getConnection(getUrl(), props)) {
                    conn2.setAutoCommit(false);
                    PreparedStatement stmt2 = conn2.prepareStatement(upsertQuery);
                    upsertRow(stmt2, 5);
                    upsertRow(stmt2, 6);
                    ResultSet rs = conn.createStatement().executeQuery("SELECT count(*) from "+dataTableFullName);
                    assertTrue(rs.next());
                    assertEquals("Unexpected row count ", 2, rs.getInt(1));
                    assertFalse(rs.next());
                    rs = conn2.createStatement().executeQuery("SELECT count(*) from "+dataTableFullName);
                    assertTrue(rs.next());
                    assertEquals("Unexpected row count ", 4, rs.getInt(1));
                    assertFalse(rs.next());
                }
            }
            
            String stmtString2 = String.format("CREATE %s INDEX %s ON %s  (LPAD(UPPER(NAME),8,'x')||'_xyz') ASYNC ", (localIndex ? "LOCAL" : ""), indexTableName, dataTableFullName);
            conn.createStatement().execute(stmtString2);
   
            //verify rows are fetched from data table.
            String selectSql = String.format("SELECT ID FROM %s WHERE LPAD(UPPER(NAME),8,'x')||'_xyz' = 'xxUNAME2_xyz'", dataTableFullName);
            ResultSet rs = conn.createStatement().executeQuery("EXPLAIN " + selectSql);
            String actualExplainPlan = QueryUtil.getExplainPlan(rs);
            
            //assert we are pulling from data table.
            assertEquals(
                    String.format("CLIENT PARALLEL 1-WAY FULL SCAN OVER %s\n"
                    + "    SERVER FILTER BY (LPAD(UPPER(NAME), 8, 'x') || '_xyz') = 'xxUNAME2_xyz'", dataTableFullName), actualExplainPlan);
            
            rs = stmt1.executeQuery(selectSql);
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            assertFalse(rs.next());
            conn.commit();
           
            //run the index MR job.
            runIndexTool(schemaName, dataTableName, indexTableName);
            
            // insert two more rows
            upsertRow(stmt1, 3);
            upsertRow(stmt1, 4);
            conn.commit();
            
            //assert we are pulling from index table.
            rs = conn.createStatement().executeQuery("EXPLAIN " + selectSql);
            actualExplainPlan = QueryUtil.getExplainPlan(rs);
            assertExplainPlan(actualExplainPlan, dataTableFullName, indexTableFullName);
            
            rs = conn.createStatement().executeQuery(selectSql);
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    private void assertExplainPlan(final String actualExplainPlan, String dataTableFullName, String indexTableFullName) {
        String expectedExplainPlan;
        if(localIndex) {
            expectedExplainPlan = String.format(" RANGE SCAN OVER %s [1,",
                    dataTableFullName);
        } else {
            expectedExplainPlan = String.format(" RANGE SCAN OVER %s",
                    indexTableFullName);
        }
        assertTrue(actualExplainPlan + "\n expected to contain \n" + expectedExplainPlan, actualExplainPlan.contains(expectedExplainPlan));
    }

    private String[] getArgValues(String schemaName, String dataTable, String indxTable) {
        final List<String> args = Lists.newArrayList();
        if (schemaName!=null) {
            args.add("-s");
            args.add(schemaName);
        }
        args.add("-dt");
        args.add(dataTable);
        args.add("-it");
        args.add(indxTable);
        if(directApi) {
            args.add("-direct");
            // Need to run this job in foreground for the test to be deterministic
            args.add("-runfg");
        }

        args.add("-op");
        args.add("/tmp/"+UUID.randomUUID().toString());
        return args.toArray(new String[0]);
    }

    private static void upsertRow(PreparedStatement stmt, int i) throws SQLException {
        // insert row
        stmt.setInt(1, i);
        stmt.setString(2, "uname" + String.valueOf(i));
        stmt.setInt(3, 95050 + i);
        stmt.executeUpdate();
    }

    @Test
    public void testDeleteFromImmutable() throws Exception {
        if (transactional || mutable) {
            return;
        }
        if (localIndex) { // TODO: remove this return once PHOENIX-3292 is fixed
            return;
        }
        String schemaName = generateUniqueName();
        String dataTableName = generateUniqueName();
        String dataTableFullName = SchemaUtil.getTableName(schemaName, dataTableName);
        String indexTableName = generateUniqueName();
        String indexTableFullName = SchemaUtil.getTableName(schemaName, indexTableName);
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.createStatement().execute("CREATE TABLE " + dataTableFullName + " (\n" + 
                    "        pk1 VARCHAR NOT NULL,\n" + 
                    "        pk2 VARCHAR NOT NULL,\n" + 
                    "        pk3 VARCHAR\n" + 
                    "        CONSTRAINT PK PRIMARY KEY \n" + 
                    "        (\n" + 
                    "        pk1,\n" + 
                    "        pk2,\n" + 
                    "        pk3\n" + 
                    "        )\n" + 
                    "        ) IMMUTABLE_ROWS=true");
            conn.createStatement().execute("upsert into " + dataTableFullName + " (pk1, pk2, pk3) values ('a', '1', '1')");
            conn.createStatement().execute("upsert into " + dataTableFullName + " (pk1, pk2, pk3) values ('b', '2', '2')");
            conn.commit();
            conn.createStatement().execute("CREATE " + (localIndex ? "LOCAL" : "") + " INDEX " + indexTableName + " ON " + dataTableFullName + " (pk3, pk2) ASYNC");
            
            // this delete will be issued at a timestamp later than the above timestamp of the index table
            conn.createStatement().execute("delete from " + dataTableFullName + " where pk1 = 'a'");
            conn.commit();

            //run the index MR job.
            runIndexTool(schemaName, dataTableName, indexTableName);

            // upsert two more rows
            conn.createStatement().execute(
                "upsert into " + dataTableFullName + " (pk1, pk2, pk3) values ('a', '3', '3')");
            conn.createStatement().execute(
                "upsert into " + dataTableFullName + " (pk1, pk2, pk3) values ('b', '4', '4')");
            conn.commit();

            // validate that delete markers were issued correctly and only ('a', '1', 'value1') was
            // deleted
            String query = "SELECT pk3 from " + dataTableFullName + " ORDER BY pk3";
            ResultSet rs = conn.createStatement().executeQuery("EXPLAIN " + query);
            String expectedPlan =
                    "CLIENT PARALLEL 1-WAY FULL SCAN OVER " + indexTableFullName + "\n"
                            + "    SERVER FILTER BY FIRST KEY ONLY";
            assertEquals("Wrong plan ", expectedPlan, QueryUtil.getExplainPlan(rs));
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("2", rs.getString(1));
            assertTrue(rs.next());
            assertEquals("3", rs.getString(1));
            assertTrue(rs.next());
            assertEquals("4", rs.getString(1));
            assertFalse(rs.next());
        }
    }

    private static Connection getConnectionForLocalIndexTest() throws SQLException{
        Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
        props.setProperty(QueryServices.FORCE_ROW_KEY_ORDER_ATTRIB, Boolean.TRUE.toString());
        return DriverManager.getConnection(getUrl(),props);
    }

    private void createBaseTable(String tableName, String splits) throws SQLException {
        Connection conn = getConnectionForLocalIndexTest();
        String ddl = "CREATE TABLE " + tableName + " (t_id VARCHAR NOT NULL,\n" +
                "k1 INTEGER NOT NULL,\n" +
                "k2 INTEGER NOT NULL,\n" +
                "k3 INTEGER,\n" +
                "v1 VARCHAR,\n" +
                "CONSTRAINT pk PRIMARY KEY (t_id, k1, k2))\n"
                        + (splits != null ? (" split on " + splits) : "");
        conn.createStatement().execute(ddl);
        conn.close();
    }

    // Moved from LocalIndexIT because it was causing parallel runs to hang
    @Test
    public void testLocalIndexScanAfterRegionSplit() throws Exception {
        // This test just needs be run once
        if (!localIndex || transactional || mutable || directApi) {
            return;
        }
        String schemaName = generateUniqueName();
        String tableName = schemaName + "." + generateUniqueName();
        String indexName = "IDX_" + generateUniqueName();
        TableName physicalTableName = SchemaUtil.getPhysicalTableName(tableName.getBytes(), false);
        String indexPhysicalTableName = physicalTableName.getNameAsString();

        createBaseTable(tableName, "('e','j','o')");
        Connection conn1 = getConnectionForLocalIndexTest();
        try{
            String[] strings = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t","u","v","w","x","y","z"};
            for (int i = 0; i < 26; i++) {
                conn1.createStatement().execute(
                    "UPSERT INTO " + tableName + " values('"+strings[i]+"'," + i + ","
                            + (i + 1) + "," + (i + 2) + ",'" + strings[25 - i] + "')");
            }
            conn1.commit();
            conn1.createStatement().execute("CREATE LOCAL INDEX " + indexName + " ON " + tableName + "(v1)");
            conn1.createStatement().execute("CREATE LOCAL INDEX " + indexName + "_2 ON " + tableName + "(k3)");

            ResultSet rs = conn1.createStatement().executeQuery("SELECT * FROM " + tableName);
            assertTrue(rs.next());
            
            HBaseAdmin admin = conn1.unwrap(PhoenixConnection.class).getQueryServices().getAdmin();
            for (int i = 1; i < 5; i++) {
                admin.split(physicalTableName, ByteUtil.concat(Bytes.toBytes(strings[3*i])));
                List<HRegionInfo> regionsOfUserTable =
                        MetaTableAccessor.getTableRegions(getUtility().getZooKeeperWatcher(), admin.getConnection(),
                                physicalTableName, false);

                while (regionsOfUserTable.size() != (4+i)) {
                    Thread.sleep(100);
                    regionsOfUserTable = MetaTableAccessor.getTableRegions(getUtility().getZooKeeperWatcher(),
                            admin.getConnection(), physicalTableName, false);
                }
                assertEquals(4+i, regionsOfUserTable.size());
                String[] tIdColumnValues = new String[26]; 
                String[] v1ColumnValues = new String[26];
                int[] k1ColumnValue = new int[26];
                String query = "SELECT t_id,k1,v1 FROM " + tableName;
                rs = conn1.createStatement().executeQuery(query);
                Thread.sleep(1000);
                for (int j = 0; j < 26; j++) {
                    assertTrue("No row found at " + j, rs.next());
                    tIdColumnValues[j] = rs.getString("t_id");
                    k1ColumnValue[j] = rs.getInt("k1");
                    v1ColumnValues[j] = rs.getString("V1");
                }
                Arrays.sort(tIdColumnValues);
                Arrays.sort(v1ColumnValues);
                Arrays.sort(k1ColumnValue);
                assertTrue(Arrays.equals(strings, tIdColumnValues));
                assertTrue(Arrays.equals(strings, v1ColumnValues));
                for(int m=0;m<26;m++) {
                    assertEquals(m, k1ColumnValue[m]);
                }

                rs = conn1.createStatement().executeQuery("EXPLAIN " + query);
                assertEquals(
                        "CLIENT PARALLEL " + (4 + i) + "-WAY RANGE SCAN OVER "
                                + indexPhysicalTableName + " [1]\n"
                                        + "    SERVER FILTER BY FIRST KEY ONLY\n"
                                + "CLIENT MERGE SORT", QueryUtil.getExplainPlan(rs));
                
                query = "SELECT t_id,k1,k3 FROM " + tableName;
                rs = conn1.createStatement().executeQuery("EXPLAIN "+query);
                assertEquals(
                    "CLIENT PARALLEL "
                            + ((strings[3 * i].compareTo("j") < 0) ? (4 + i) : (4 + i - 1))
                            + "-WAY RANGE SCAN OVER "
                            + indexPhysicalTableName + " [2]\n"
                                    + "    SERVER FILTER BY FIRST KEY ONLY\n"
                            + "CLIENT MERGE SORT", QueryUtil.getExplainPlan(rs));
                rs = conn1.createStatement().executeQuery(query);
                Thread.sleep(1000);
                int[] k3ColumnValue = new int[26];
                for (int j = 0; j < 26; j++) {
                    assertTrue(rs.next());
                    tIdColumnValues[j] = rs.getString("t_id");
                    k1ColumnValue[j] = rs.getInt("k1");
                    k3ColumnValue[j] = rs.getInt("k3");
                }
                Arrays.sort(tIdColumnValues);
                Arrays.sort(k1ColumnValue);
                Arrays.sort(k3ColumnValue);
                assertTrue(Arrays.equals(strings, tIdColumnValues));
                for(int m=0;m<26;m++) {
                    assertEquals(m, k1ColumnValue[m]);
                    assertEquals(m+2, k3ColumnValue[m]);
                }
            }
       } finally {
            conn1.close();
        }
    }

    // Moved from LocalIndexIT because it was causing parallel runs to hang
    @Test
    public void testLocalIndexScanAfterRegionsMerge() throws Exception {
        // This test just needs be run once
        if (!localIndex || transactional || mutable || directApi) {
            return;
        }
        String schemaName = generateUniqueName();
        String tableName = schemaName + "." + generateUniqueName();
        String indexName = "IDX_" + generateUniqueName();
        TableName physicalTableName = SchemaUtil.getPhysicalTableName(tableName.getBytes(), false);
        String indexPhysicalTableName = physicalTableName.getNameAsString();

        createBaseTable(tableName, "('e','j','o')");
        Connection conn1 = getConnectionForLocalIndexTest();
        try{
            String[] strings = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t","u","v","w","x","y","z"};
            for (int i = 0; i < 26; i++) {
                conn1.createStatement().execute(
                    "UPSERT INTO " + tableName + " values('"+strings[i]+"'," + i + ","
                            + (i + 1) + "," + (i + 2) + ",'" + strings[25 - i] + "')");
            }
            conn1.commit();
            conn1.createStatement().execute("CREATE LOCAL INDEX " + indexName + " ON " + tableName + "(v1)");
            conn1.createStatement().execute("CREATE LOCAL INDEX " + indexName + "_2 ON " + tableName + "(k3)");

            ResultSet rs = conn1.createStatement().executeQuery("SELECT * FROM " + tableName);
            assertTrue(rs.next());

            HBaseAdmin admin = conn1.unwrap(PhoenixConnection.class).getQueryServices().getAdmin();
            List<HRegionInfo> regionsOfUserTable =
                    MetaTableAccessor.getTableRegions(getUtility().getZooKeeperWatcher(), admin.getConnection(),
                        physicalTableName, false);
            admin.mergeRegions(regionsOfUserTable.get(0).getEncodedNameAsBytes(),
                regionsOfUserTable.get(1).getEncodedNameAsBytes(), false);
            regionsOfUserTable =
                    MetaTableAccessor.getTableRegions(getUtility().getZooKeeperWatcher(), admin.getConnection(),
                            physicalTableName, false);

            while (regionsOfUserTable.size() != 3) {
                Thread.sleep(100);
                regionsOfUserTable = MetaTableAccessor.getTableRegions(getUtility().getZooKeeperWatcher(),
                        admin.getConnection(), physicalTableName, false);
            }
            String query = "SELECT t_id,k1,v1 FROM " + tableName;
            rs = conn1.createStatement().executeQuery(query);
            Thread.sleep(1000);
            for (int j = 0; j < 26; j++) {
                assertTrue(rs.next());
                assertEquals(strings[25 - j], rs.getString("t_id"));
                assertEquals(25 - j, rs.getInt("k1"));
                assertEquals(strings[j], rs.getString("V1"));
            }
            rs = conn1.createStatement().executeQuery("EXPLAIN " + query);
            assertEquals(
                "CLIENT PARALLEL " + 3 + "-WAY RANGE SCAN OVER "
                        + indexPhysicalTableName
                        + " [1]\n" + "    SERVER FILTER BY FIRST KEY ONLY\n"
                        + "CLIENT MERGE SORT", QueryUtil.getExplainPlan(rs));

            query = "SELECT t_id,k1,k3 FROM " + tableName;
            rs = conn1.createStatement().executeQuery("EXPLAIN " + query);
            assertEquals(
                "CLIENT PARALLEL " + 3 + "-WAY RANGE SCAN OVER "
                        + indexPhysicalTableName
                        + " [2]\n" + "    SERVER FILTER BY FIRST KEY ONLY\n"
                        + "CLIENT MERGE SORT", QueryUtil.getExplainPlan(rs));

            rs = conn1.createStatement().executeQuery(query);
            Thread.sleep(1000);
            for (int j = 0; j < 26; j++) {
                assertTrue(rs.next());
                assertEquals(strings[j], rs.getString("t_id"));
                assertEquals(j, rs.getInt("k1"));
                assertEquals(j + 2, rs.getInt("k3"));
            }
       } finally {
            conn1.close();
        }
    }    
}
