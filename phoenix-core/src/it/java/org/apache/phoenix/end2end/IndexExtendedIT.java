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
import static org.apache.phoenix.util.TestUtil.checkIndexState;
import static org.apache.phoenix.util.TestUtil.getRowCount;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.compile.ExplainPlan;
import org.apache.phoenix.compile.ExplainPlanAttributes;
import org.apache.phoenix.coprocessor.BaseScannerRegionObserver;
import org.apache.phoenix.coprocessor.IndexRebuildRegionScanner;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.mapreduce.index.IndexTool;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;

/**
 * Tests for the {@link IndexTool}
 */
@RunWith(Parameterized.class)
@Category(NeedsOwnMiniClusterTest.class)
public class IndexExtendedIT extends BaseTest {
    private final boolean localIndex;
    private final boolean useViewIndex;
    private final String tableDDLOptions;
    private final String indexDDLOptions;
    private final boolean mutable;
    private final boolean useSnapshot;

    public IndexExtendedIT( boolean mutable, boolean localIndex, boolean useViewIndex, boolean useSnapshot) {
        this.localIndex = localIndex;
        this.useViewIndex = useViewIndex;
        this.mutable = mutable;
        this.useSnapshot = useSnapshot;
        StringBuilder optionBuilder = new StringBuilder();
        StringBuilder indexOptionBuilder = new StringBuilder();
        if (!mutable) {
            optionBuilder.append(" IMMUTABLE_ROWS=true ");
        }

        if (!localIndex) {
            if (!(optionBuilder.length() == 0)) {
                optionBuilder.append(",");
            }
            optionBuilder.append(" IMMUTABLE_STORAGE_SCHEME=ONE_CELL_PER_COLUMN, COLUMN_ENCODED_BYTES=0 ");
            indexOptionBuilder.append(" IMMUTABLE_STORAGE_SCHEME=SINGLE_CELL_ARRAY_WITH_OFFSETS,COLUMN_ENCODED_BYTES=2 ");
        }
        optionBuilder.append(" SPLIT ON(1,2)");
        this.indexDDLOptions = indexOptionBuilder.toString();
        this.tableDDLOptions = optionBuilder.toString();
    }
    
    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        Map<String, String> serverProps = Maps.newHashMapWithExpectedSize(2);
        serverProps.put(QueryServices.EXTRA_JDBC_ARGUMENTS_ATTRIB, QueryServicesOptions.DEFAULT_EXTRA_JDBC_ARGUMENTS);
        serverProps.put(BaseScannerRegionObserverConstants.PHOENIX_MAX_LOOKBACK_AGE_CONF_KEY, Integer.toString(60*60)); // An hour
        Map<String, String> clientProps = Maps.newHashMapWithExpectedSize(2);
        clientProps.put(QueryServices.TRANSACTIONS_ENABLED, Boolean.TRUE.toString());
        clientProps.put(QueryServices.FORCE_ROW_KEY_ORDER_ATTRIB, Boolean.TRUE.toString());
        setUpTestDriver(new ReadOnlyProps(serverProps.entrySet().iterator()), new ReadOnlyProps(clientProps.entrySet()
                .iterator()));
    }
    
    @Parameters(name="mutable = {0} , localIndex = {1}, useViewIndex = {2}, useSnapshot = {3}")
    public static synchronized Collection<Boolean[]> data() {
        List<Boolean[]> list = Lists.newArrayListWithExpectedSize(10);
        boolean[] Booleans = new boolean[]{false, true};
        for (boolean mutable : Booleans ) {
            for (boolean localIndex : Booleans) {
                for (boolean useViewIndex : Booleans) {
                    for (boolean useSnapshot : Booleans) {
                        if(localIndex && useSnapshot) {
                            continue;
                        }
                        list.add(new Boolean[] { mutable, localIndex, useViewIndex, useSnapshot});
                    }
                }
            }
        }
        return list;
    }
    
    /**
     * This test is to assert that updates that happen to rows of a mutable table after an index is created in ASYNC mode and before
     * the MR job runs, do show up in the index table . 
     * @throws Exception
     */
    @Test
    public void testMutableIndexWithUpdates() throws Exception {
        if (!mutable) {
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
            stmt.execute(String.format("CREATE TABLE %s (ID INTEGER NOT NULL PRIMARY KEY, NAME VARCHAR, ZIP INTEGER) %s",dataTableFullName, tableDDLOptions));
            String upsertQuery = String.format("UPSERT INTO %s VALUES(?, ?, ?)",dataTableFullName);
            PreparedStatement stmt1 = conn.prepareStatement(upsertQuery);
            
            int id = 1;
            // insert two rows
            IndexToolIT.upsertRow(stmt1, id++);
            IndexToolIT.upsertRow(stmt1, id++);
            conn.commit();
            
            stmt.execute(String.format("CREATE " + (localIndex ? "LOCAL" : "") + " INDEX %s ON %s (UPPER(NAME, 'en_US')) ASYNC %s" , indexTableName,dataTableFullName, this.indexDDLOptions));
            
            //update a row 
            stmt1.setInt(1, 1);
            stmt1.setString(2, "uname" + String.valueOf(10));
            stmt1.setInt(3, 95050 + 1);
            stmt1.executeUpdate();
            conn.commit();  
            
            //verify rows are fetched from data table.
            String selectSql = String.format("SELECT ID FROM %s WHERE UPPER(NAME, 'en_US') ='UNAME2'",dataTableFullName);

            ExplainPlan plan = conn.prepareStatement(selectSql)
                .unwrap(PhoenixPreparedStatement.class).optimizeQuery()
                .getExplainPlan();
            ExplainPlanAttributes explainPlanAttributes =
                plan.getPlanStepsAsAttributes();
            assertEquals("PARALLEL 1-WAY",
                explainPlanAttributes.getIteratorTypeAndScanSize());
            assertEquals("FULL SCAN ",
                explainPlanAttributes.getExplainScanType());
            //assert we are pulling from data table.
            assertEquals(dataTableFullName,
                explainPlanAttributes.getTableName());
            assertEquals("SERVER FILTER BY UPPER(NAME, 'en_US') = 'UNAME2'",
                explainPlanAttributes.getServerWhereFilter());

            ResultSet rs = stmt1.executeQuery(selectSql);
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            assertFalse(rs.next());
           
            //run the index MR job.
            IndexToolIT.runIndexTool(useSnapshot, schemaName, dataTableName, indexTableName);
            
            plan = conn.prepareStatement(selectSql)
                .unwrap(PhoenixPreparedStatement.class).optimizeQuery()
                .getExplainPlan();
            explainPlanAttributes =
                plan.getPlanStepsAsAttributes();
            //assert we are pulling from index table.
            String expectedTableName = localIndex ?
                    indexTableFullName + "(" + dataTableFullName + ")" : indexTableFullName;
            assertEquals(expectedTableName,
                explainPlanAttributes.getTableName());

            rs = stmt.executeQuery(selectSql);
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testDeleteFromImmutable() throws Exception {
        if (mutable) {
            return;
        }
        if (localIndex) { // TODO: remove this return once PHOENIX-3292 is fixed
            return;
        }
        String schemaName = generateUniqueName();
        String dataTableName = generateUniqueName();
        String dataTableFullName = SchemaUtil.getTableName(schemaName, dataTableName);
        String indexTableName = "IDX_" + generateUniqueName();
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
                    "        ) " + tableDDLOptions);
            conn.createStatement().execute("upsert into " + dataTableFullName + " (pk1, pk2, pk3) values ('a', '1', '1')");
            conn.createStatement().execute("upsert into " + dataTableFullName + " (pk1, pk2, pk3) values ('b', '2', '2')");
            conn.commit();
            conn.createStatement().execute("CREATE " + (localIndex ? "LOCAL" : "") + " INDEX " + indexTableName + " ON " + dataTableFullName + " (pk3, pk2) ASYNC " + this.indexDDLOptions);
            
            // this delete will be issued at a timestamp later than the above timestamp of the index table
            conn.createStatement().execute("delete from " + dataTableFullName + " where pk1 = 'a'");
            conn.commit();

            //run the index MR job.
            IndexToolIT.runIndexTool(useSnapshot, schemaName, dataTableName, indexTableName);

            // upsert two more rows
            conn.createStatement().execute(
                "upsert into " + dataTableFullName + " (pk1, pk2, pk3) values ('a', '3', '3')");
            conn.createStatement().execute(
                "upsert into " + dataTableFullName + " (pk1, pk2, pk3) values ('b', '4', '4')");
            conn.commit();

            // validate that delete markers were issued correctly and only ('a', '1', 'value1') was
            // deleted
            String query = "SELECT pk3 from " + dataTableFullName + " ORDER BY pk3";

            ExplainPlan plan = conn.prepareStatement(query)
                .unwrap(PhoenixPreparedStatement.class).optimizeQuery()
                .getExplainPlan();
            ExplainPlanAttributes explainPlanAttributes =
                plan.getPlanStepsAsAttributes();
            assertEquals("PARALLEL 1-WAY",
                explainPlanAttributes.getIteratorTypeAndScanSize());
            assertEquals("FULL SCAN ",
                explainPlanAttributes.getExplainScanType());
            assertEquals(indexTableFullName,
                explainPlanAttributes.getTableName());
            assertEquals("SERVER FILTER BY FIRST KEY ONLY",
                explainPlanAttributes.getServerWhereFilter());

            ResultSet rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("2", rs.getString(1));
            assertTrue(rs.next());
            assertEquals("3", rs.getString(1));
            assertTrue(rs.next());
            assertEquals("4", rs.getString(1));
            assertFalse(rs.next());
        }
    }

    @Test
    public void testBuildDisabledIndex() throws Exception {
        if (localIndex  || useSnapshot) {
            return;
        }
        String schemaName = generateUniqueName();
        String dataTableName = generateUniqueName();
        String dataTableFullName = SchemaUtil.getTableName(schemaName, dataTableName);
        String indexName = "I_" + generateUniqueName();
        String indexFullName = SchemaUtil.getTableName(schemaName, indexName);
        String viewName = "V_" + generateUniqueName();
        String viewFullName =  SchemaUtil.getTableName(schemaName, viewName);
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        Statement stmt = conn.createStatement();
        try {
            stmt.execute(String.format(
                    "CREATE TABLE %s (ID INTEGER NOT NULL PRIMARY KEY, NAME VARCHAR, ZIP INTEGER) %s",
                    dataTableFullName, tableDDLOptions));
            if (useViewIndex) {
                stmt.execute(String.format(
                        "CREATE VIEW %s AS SELECT * FROM %s",
                        viewFullName, dataTableFullName));
            }
            String upsertQuery = String.format("UPSERT INTO %s VALUES(?, ?, ?)", dataTableFullName);
            PreparedStatement stmt1 = conn.prepareStatement(upsertQuery);

            int id = 1;
            // insert two rows
            IndexToolIT.upsertRow(stmt1, id++);
            IndexToolIT.upsertRow(stmt1, id++);
            conn.commit();

            String baseTableNameOfIndex = dataTableName;
            String baseTableFullNameOfIndex = dataTableFullName;
            String physicalTableNameOfIndex = indexFullName;
            if (useViewIndex) {
                baseTableFullNameOfIndex = viewFullName;
                baseTableNameOfIndex = viewName;
                physicalTableNameOfIndex = "_IDX_" + dataTableFullName;
            }
            Table hIndexTable = conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(Bytes.toBytes(physicalTableNameOfIndex));

            stmt.execute(
                    String.format("CREATE INDEX %s ON %s (UPPER(NAME, 'en_US')) %s", indexName,
                            baseTableFullNameOfIndex, this.indexDDLOptions));
            long dataCnt = getRowCount(conn, dataTableFullName);
            long indexCnt = getUtility().countRows(hIndexTable);
            assertEquals(dataCnt, indexCnt);

            stmt.execute(String.format("ALTER INDEX  %s ON %s DISABLE", indexName,
                    baseTableFullNameOfIndex));

            // insert 3rd row
            IndexToolIT.upsertRow(stmt1, id++);
            conn.commit();

            dataCnt = getRowCount(conn, baseTableFullNameOfIndex);
            indexCnt = getUtility().countRows(hIndexTable);
            assertEquals(dataCnt, indexCnt+1);

            //run the index MR job.
            IndexToolIT.runIndexTool(useSnapshot, schemaName, baseTableNameOfIndex, indexName);

            dataCnt = getRowCount(conn, baseTableFullNameOfIndex);
            indexCnt = getUtility().countRows(hIndexTable);
            assertEquals(dataCnt, indexCnt);

            checkIndexState(conn, indexFullName, PIndexState.ACTIVE, 0L);
        } finally {
            conn.close();
        }
    }

    @Test
    public void testIndexStateOnException() throws Exception {
        if (localIndex  || useSnapshot || useViewIndex) {
            return;
        }

        String schemaName = generateUniqueName();
        String dataTableName = generateUniqueName();
        String dataTableFullName = SchemaUtil.getTableName(schemaName, dataTableName);
        String indexTableName = generateUniqueName();
        String indexFullName = SchemaUtil.getTableName(schemaName, indexTableName);
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)){
            Statement stmt = conn.createStatement();
            stmt.execute(String.format(
                    "CREATE TABLE %s (ID INTEGER NOT NULL PRIMARY KEY, NAME VARCHAR, ZIP INTEGER) %s",
                    dataTableFullName, tableDDLOptions));

            stmt.execute(String.format(
                    "UPSERT INTO %s VALUES(1, 'Phoenix', 12345)", dataTableFullName));

            conn.commit();

            // Configure IndexRegionObserver to fail the first write phase. This should not
            // lead to any change on index and thus index verify during index rebuild should fail
            IndexRebuildRegionScanner.setIgnoreIndexRebuildForTesting(true);
            stmt.execute(String.format(
                    "CREATE INDEX %s ON %s (NAME) INCLUDE (ZIP) ASYNC %s",
                    indexTableName, dataTableFullName, this.indexDDLOptions));

            // Verify that the index table is not in the ACTIVE state
            assertFalse(checkIndexState(conn, indexFullName, PIndexState.ACTIVE, 0L));

            if (BaseScannerRegionObserver.isMaxLookbackTimeEnabled(getUtility().getConfiguration())) {
                // Run the index MR job and verify that the index table rebuild fails
                IndexToolIT.runIndexTool(false, schemaName, dataTableName,
                        indexTableName, null, -1, IndexTool.IndexVerifyType.AFTER);
                // job failed, verify that the index table is still not in the ACTIVE state
                assertFalse(checkIndexState(conn, indexFullName, PIndexState.ACTIVE, 0L));
            }

            IndexRebuildRegionScanner.setIgnoreIndexRebuildForTesting(false);
            // Run the index MR job and verify that the index table rebuild succeeds
            IndexToolIT.runIndexTool(false, schemaName, dataTableName,
                    indexTableName, null, 0, IndexTool.IndexVerifyType.AFTER);

            // job passed, verify that the index table is in the ACTIVE state
            assertTrue(checkIndexState(conn, indexFullName, PIndexState.ACTIVE, 0L));
        }
    }
}
