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
import java.sql.Statement;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.phoenix.mapreduce.index.IndexTool;
import org.apache.phoenix.query.BaseTest;
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

/**
 * Tests for the {@link IndexTool}
 */
@RunWith(Parameterized.class)
@Category(NeedsOwnMiniClusterTest.class)
public class IndexExtendedIT extends BaseTest {
    private final boolean localIndex;
    private final boolean directApi;
    private final String tableDDLOptions;
    private final boolean mutable;
    private final boolean useSnapshot;
    
    public IndexExtendedIT( boolean mutable, boolean localIndex, boolean directApi, boolean useSnapshot) {
        this.localIndex = localIndex;
        this.directApi = directApi;
        this.mutable = mutable;
        this.useSnapshot = useSnapshot;
        StringBuilder optionBuilder = new StringBuilder();
        if (!mutable) {
            optionBuilder.append(" IMMUTABLE_ROWS=true ");
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
    
    @Parameters(name="mutable = {0} , localIndex = {1}, directApi = {2}, useSnapshot = {3}")
    public static Collection<Boolean[]> data() {
        List<Boolean[]> list = Lists.newArrayListWithExpectedSize(16);
        boolean[] Booleans = new boolean[]{false, true};
        for (boolean mutable : Booleans ) {
            for (boolean localIndex : Booleans ) {
                for (boolean directApi : Booleans ) {
                    for (boolean useSnapshot : Booleans ) {
                        list.add(new Boolean[]{ mutable, localIndex, directApi, useSnapshot});
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
            IndexToolIT.runIndexTool(directApi, useSnapshot, schemaName, dataTableName, indexTableName);
            
            //assert we are pulling from index table.
            rs = conn.createStatement().executeQuery("EXPLAIN " + selectSql);
            actualExplainPlan = QueryUtil.getExplainPlan(rs);
            // TODO: why is it a 1-WAY parallel scan only for !transactional && mutable && localIndex
            IndexToolIT.assertExplainPlan(localIndex, actualExplainPlan, dataTableFullName, indexTableFullName);
            
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
                    "        ) " + tableDDLOptions);
            conn.createStatement().execute("upsert into " + dataTableFullName + " (pk1, pk2, pk3) values ('a', '1', '1')");
            conn.createStatement().execute("upsert into " + dataTableFullName + " (pk1, pk2, pk3) values ('b', '2', '2')");
            conn.commit();
            conn.createStatement().execute("CREATE " + (localIndex ? "LOCAL" : "") + " INDEX " + indexTableName + " ON " + dataTableFullName + " (pk3, pk2) ASYNC");
            
            // this delete will be issued at a timestamp later than the above timestamp of the index table
            conn.createStatement().execute("delete from " + dataTableFullName + " where pk1 = 'a'");
            conn.commit();

            //run the index MR job.
            IndexToolIT.runIndexTool(directApi, useSnapshot, schemaName, dataTableName, indexTableName);

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
    
}
