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
package org.apache.phoenix.mapreduce;

import static org.apache.phoenix.query.BaseTest.setUpConfigForMiniCluster;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.mapreduce.index.IndexTool;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Lists;

/**
 * Tests for the {@link IndexTool}
 */
@Category(NeedsOwnMiniClusterTest.class)
public class IndexToolIT {
    
    private static HBaseTestingUtility hbaseTestUtil;
    private static String zkQuorum;
  
    @BeforeClass
    public static void setUp() throws Exception {
        hbaseTestUtil = new HBaseTestingUtility();
        Configuration conf = hbaseTestUtil.getConfiguration();
        conf.setBoolean("hbase.defaults.for.version.skip", true);
        // Since we're using the real PhoenixDriver in this test, remove the
        // extra JDBC argument that causes the test driver to be used.
        conf.set(QueryServices.EXTRA_JDBC_ARGUMENTS_ATTRIB, QueryServicesOptions.DEFAULT_EXTRA_JDBC_ARGUMENTS);
        setUpConfigForMiniCluster(conf);
        hbaseTestUtil.startMiniCluster();
        hbaseTestUtil.startMiniMapReduceCluster();
        Class.forName(PhoenixDriver.class.getName());
        DriverManager.registerDriver(PhoenixDriver.INSTANCE);
        zkQuorum = "localhost:" + hbaseTestUtil.getZkCluster().getClientPort();
    }
    
    @Test
    public void testImmutableGlobalIndex() throws Exception {
        testSecondaryIndex("SCHEMA", "DATA_TABLE1", true, false);
    }
    
    @Test
    public void testImmutableLocalIndex() throws Exception {
        testSecondaryIndex("SCHEMA", "DATA_TABLE2", true, true);
    }
    
    @Test
    public void testMutableGlobalIndex() throws Exception {
        testSecondaryIndex("SCHEMA", "DATA_TABLE3", false, false);
    }
    
    @Test
    public void testMutableLocalIndex() throws Exception {
        testSecondaryIndex("SCHEMA", "DATA_TABLE4", false, true);
    }
    
    @Test
    public void testImmutableGlobalIndexDirectApi() throws Exception {
    	testSecondaryIndex("SCHEMA", "DATA_TABLE5", true, false, true);
    }
    
    @Test
    public void testImmutableLocalIndexDirectApi() throws Exception {
    	testSecondaryIndex("SCHEMA", "DATA_TABLE6", true, true, true);
    }
    
    @Test
    public void testMutableGlobalIndexDirectApi() throws Exception {
    	testSecondaryIndex("SCHEMA", "DATA_TABLE7", false, false, true);
    }
    
    @Test
    public void testMutableLocalIndexDirectApi() throws Exception {
    	testSecondaryIndex("SCHEMA", "DATA_TABLE8", false, true, true);
    }
    
    public void testSecondaryIndex(final String schemaName, final String dataTable, final boolean isImmutable , final boolean isLocal) throws Exception {
    	testSecondaryIndex(schemaName, dataTable, isImmutable, isLocal, false);
    }
    
    public void testSecondaryIndex(final String schemaName, final String dataTable, final boolean isImmutable , final boolean isLocal, final boolean directApi) throws Exception {
        
    	final String fullTableName = SchemaUtil.getTableName(schemaName, dataTable);
        final String indxTable = String.format("%s_%s",dataTable,"INDX");
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(PhoenixRuntime.JDBC_PROTOCOL + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR + zkQuorum,props);
        Statement stmt = conn.createStatement();
        try {
        
            stmt.execute(String.format("CREATE TABLE %s (ID INTEGER NOT NULL PRIMARY KEY, NAME VARCHAR, ZIP INTEGER) %s", fullTableName, (isImmutable ? "IMMUTABLE_ROWS=true" :"")));
            String upsertQuery = String.format("UPSERT INTO %s VALUES(?, ?, ?)", fullTableName);
            PreparedStatement stmt1 = conn.prepareStatement(upsertQuery);
            
            int id = 1;
            // insert two rows
            upsertRow(stmt1, id++);
            upsertRow(stmt1, id++);
            conn.commit();
            
            stmt.execute(String.format("CREATE %s INDEX %s ON %s  (LPAD(UPPER(NAME),8,'x')||'_xyz') ASYNC ", (isLocal ? "LOCAL" : ""), indxTable, fullTableName));
   
            //verify rows are fetched from data table.
            String selectSql = String.format("SELECT LPAD(UPPER(NAME),8,'x')||'_xyz',ID FROM %s", fullTableName);
            ResultSet rs = conn.createStatement().executeQuery("EXPLAIN " + selectSql);
            String actualExplainPlan = QueryUtil.getExplainPlan(rs);
            
            //assert we are pulling from data table.
            assertEquals(String.format("CLIENT 1-CHUNK PARALLEL 1-WAY FULL SCAN OVER %s", fullTableName), actualExplainPlan);
            
            rs = stmt1.executeQuery(selectSql);
            assertTrue(rs.next());
            assertEquals("xxUNAME1_xyz", rs.getString(1));    
            assertTrue(rs.next());
            assertEquals("xxUNAME2_xyz", rs.getString(1));
           
            //run the index MR job.
            final IndexTool indexingTool = new IndexTool();
            indexingTool.setConf(new Configuration(hbaseTestUtil.getConfiguration()));
            
            final String[] cmdArgs = getArgValues(schemaName, dataTable, indxTable, directApi);
            int status = indexingTool.run(cmdArgs);
            assertEquals(0, status);
            
            // insert two more rows
            upsertRow(stmt1, 3);
            upsertRow(stmt1, 4);
            conn.commit();
            
            rs = stmt1.executeQuery("SELECT * FROM "+SchemaUtil.getTableName(schemaName, indxTable));

            //assert we are pulling from index table.
            rs = conn.createStatement().executeQuery("EXPLAIN " + selectSql);
            actualExplainPlan = QueryUtil.getExplainPlan(rs);
            assertExplainPlan(actualExplainPlan,schemaName,dataTable,indxTable,isLocal);
            
            rs = stmt.executeQuery(selectSql);
//            assertTrue(rs.next());
//            assertEquals("xxUNAME1_xyz", rs.getString(1));
//            assertEquals(1, rs.getInt(2));
//            
//            assertTrue(rs.next());
//            assertEquals("xxUNAME2_xyz", rs.getString(1));
//            assertEquals(2, rs.getInt(2));
//
//            assertTrue(rs.next());
//            assertEquals("xxUNAME3_xyz", rs.getString(1));
//            assertEquals(3, rs.getInt(2));
//            
//            assertTrue(rs.next());
//            assertEquals("xxUNAME4_xyz", rs.getString(1));
//            assertEquals(4, rs.getInt(2));
//      
//            assertFalse(rs.next());
            
            conn.createStatement().execute(String.format("DROP INDEX  %s ON %s",indxTable , fullTableName));
        } finally {
            conn.close();
        }
    }
    
    
    /**
     * This test is to assert that updates that happen to rows of a mutable table after an index is created in ASYNC mode and before
     * the MR job runs, do show up in the index table . 
     * @throws Exception
     */
    @Test
    public void testMutalbleIndexWithUpdates() throws Exception {
        
        final String dataTable = "DATA_TABLE5";
        final String indxTable = String.format("%s_%s",dataTable,"INDX");
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(PhoenixRuntime.JDBC_PROTOCOL + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR + zkQuorum,props);
        Statement stmt = conn.createStatement();
        try {
        
            stmt.execute(String.format("CREATE TABLE %s (ID INTEGER NOT NULL PRIMARY KEY, NAME VARCHAR, ZIP INTEGER)",dataTable));
            String upsertQuery = String.format("UPSERT INTO %s VALUES(?, ?, ?)",dataTable);
            PreparedStatement stmt1 = conn.prepareStatement(upsertQuery);
            
            int id = 1;
            // insert two rows
            upsertRow(stmt1, id++);
            upsertRow(stmt1, id++);
            conn.commit();
            
            stmt.execute(String.format("CREATE INDEX %s ON %s (UPPER(NAME)) ASYNC ", indxTable,dataTable));
            
            //update a row 
            stmt1.setInt(1, 1);
            stmt1.setString(2, "uname" + String.valueOf(10));
            stmt1.setInt(3, 95050 + 1);
            stmt1.executeUpdate();
            conn.commit();  
            
            //verify rows are fetched from data table.
            String selectSql = String.format("SELECT UPPER(NAME),ID FROM %s",dataTable);
            ResultSet rs = conn.createStatement().executeQuery("EXPLAIN " + selectSql);
            String actualExplainPlan = QueryUtil.getExplainPlan(rs);
            
            //assert we are pulling from data table.
            assertEquals(String.format("CLIENT 1-CHUNK PARALLEL 1-WAY FULL SCAN OVER %s",dataTable),actualExplainPlan);
            
            rs = stmt1.executeQuery(selectSql);
            assertTrue(rs.next());
            assertEquals("UNAME10", rs.getString(1));
            assertTrue(rs.next());
            assertEquals("UNAME2", rs.getString(1));
           
            //run the index MR job.
            final IndexTool indexingTool = new IndexTool();
            indexingTool.setConf(new Configuration(hbaseTestUtil.getConfiguration()));
            
            final String[] cmdArgs = getArgValues(null, dataTable,indxTable);
            int status = indexingTool.run(cmdArgs);
            assertEquals(0, status);
            
            //assert we are pulling from index table.
            rs = conn.createStatement().executeQuery("EXPLAIN " + selectSql);
            actualExplainPlan = QueryUtil.getExplainPlan(rs);
            assertExplainPlan(actualExplainPlan,null,dataTable,indxTable,false);
            
            rs = stmt.executeQuery(selectSql);
            assertTrue(rs.next());
            assertEquals("UNAME10", rs.getString(1));
            assertEquals(1, rs.getInt(2));
            
            assertTrue(rs.next());
            assertEquals("UNAME2", rs.getString(1));
            assertEquals(2, rs.getInt(2));
            conn.createStatement().execute(String.format("DROP INDEX  %s ON %s",indxTable , dataTable));
        } finally {
            conn.close();
        }
    }
    
    private void assertExplainPlan(final String actualExplainPlan, String schemaName, String dataTable,
            String indxTable, boolean isLocal) {
        
        String expectedExplainPlan = "";
        if(isLocal) {
            final String localIndexName = MetaDataUtil.getLocalIndexTableName(SchemaUtil.getTableName(schemaName, dataTable));
            expectedExplainPlan = String.format("CLIENT 1-CHUNK PARALLEL 1-WAY RANGE SCAN OVER %s [-32768]"
                + "\n    SERVER FILTER BY FIRST KEY ONLY", localIndexName);
        } else {
            expectedExplainPlan = String.format("CLIENT 1-CHUNK PARALLEL 1-WAY FULL SCAN OVER %s"
                    + "\n    SERVER FILTER BY FIRST KEY ONLY",SchemaUtil.getTableName(schemaName, indxTable));
        }
        assertEquals(expectedExplainPlan,actualExplainPlan);
    }

    private String[] getArgValues(String schemaName, String dataTable, String indxTable) {
        return getArgValues(schemaName, dataTable, indxTable, false);
    }
    
    private String[] getArgValues(String schemaName, String dataTable, String indxTable, boolean directApi) {
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

    private void upsertRow(PreparedStatement stmt, int i) throws SQLException {
        // insert row
        stmt.setInt(1, i);
        stmt.setString(2, "uname" + String.valueOf(i));
        stmt.setInt(3, 95050 + i);
        stmt.executeUpdate();
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        try {
            DriverManager.deregisterDriver(PhoenixDriver.INSTANCE);
        } finally {
            try {
                hbaseTestUtil.shutdownMiniMapReduceCluster();
            } finally {
                hbaseTestUtil.shutdownMiniCluster();
            }
        }
    }
}
