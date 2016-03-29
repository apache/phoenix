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
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.phoenix.mapreduce.index.IndexTool;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Maps;

public class MutableIndexToolIT extends BaseOwnClusterHBaseManagedTimeIT {
    
    @BeforeClass
    public static void doSetup() throws Exception {
        Map<String, String> serverProps = Maps.newHashMapWithExpectedSize(1);
        serverProps.put(QueryServices.EXTRA_JDBC_ARGUMENTS_ATTRIB, QueryServicesOptions.DEFAULT_EXTRA_JDBC_ARGUMENTS);
        setUpRealDriver(new ReadOnlyProps(serverProps.entrySet().iterator()), ReadOnlyProps.EMPTY_PROPS);
    }

    /**
     * This test is to assert that updates that happen to rows of a mutable table after an index is created in ASYNC mode and before
     * the MR job runs, do show up in the index table . 
     * @throws Exception
     */
    @Test
    public void testMutableIndexWithUpdates() throws Exception {
        
        final String dataTable = "DATA_TABLE5";
        final String indxTable = String.format("%s_%s",dataTable,"INDX");
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(QueryServices.EXPLAIN_ROW_COUNT_ATTRIB, Boolean.FALSE.toString());
        Connection conn = DriverManager.getConnection(getUrl(), props);
        Statement stmt = conn.createStatement();
        try {
        
            stmt.execute(String.format("CREATE TABLE %s (ID INTEGER NOT NULL PRIMARY KEY, NAME VARCHAR, ZIP INTEGER)",dataTable));
            String upsertQuery = String.format("UPSERT INTO %s VALUES(?, ?, ?)",dataTable);
            PreparedStatement stmt1 = conn.prepareStatement(upsertQuery);
            
            int id = 1;
            // insert two rows
            IndexToolIT.upsertRow(stmt1, id++);
            IndexToolIT.upsertRow(stmt1, id++);
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
            assertEquals(String.format("CLIENT 1-CHUNK PARALLEL 1-WAY ROUND ROBIN FULL SCAN OVER %s",dataTable),actualExplainPlan);
            
            rs = stmt1.executeQuery(selectSql);
            assertTrue(rs.next());
            assertEquals("UNAME10", rs.getString(1));
            assertTrue(rs.next());
            assertEquals("UNAME2", rs.getString(1));
           
            //run the index MR job.
            final IndexTool indexingTool = new IndexTool();
            indexingTool.setConf(new Configuration(getUtility().getConfiguration()));
            
            final String[] cmdArgs = IndexToolIT.getArgValues(null, dataTable,indxTable);
            int status = indexingTool.run(cmdArgs);
            assertEquals(0, status);
            
            //assert we are pulling from index table.
            rs = conn.createStatement().executeQuery("EXPLAIN " + selectSql);
            actualExplainPlan = QueryUtil.getExplainPlan(rs);
            IndexToolIT.assertExplainPlan(actualExplainPlan,null,dataTable,indxTable,false);
            
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
    
}
