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
import org.apache.phoenix.mapreduce.index.IndexTool;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Tests for the {@link IndexTool}
 */
@RunWith(Parameterized.class)
public class IndexToolIT extends BaseOwnClusterHBaseManagedTimeIT {
    
    private final String schemaName;
    private final String dataTable;
    
    private final boolean localIndex;
    private final boolean transactional;
    private final boolean directApi;
    private final String tableDDLOptions;
    
    public IndexToolIT(boolean transactional, boolean localIndex, boolean mutable, boolean directApi) {
        this.schemaName = "S";
        this.dataTable = "T" + (transactional ? "_TXN" : "");
        this.localIndex = localIndex;
        this.transactional = transactional;
        this.directApi = directApi;
        StringBuilder optionBuilder = new StringBuilder();
        if (!mutable) 
            optionBuilder.append(" IMMUTABLE_ROWS=true ");
        if (transactional) {
            if (!(optionBuilder.length()==0))
                optionBuilder.append(",");
            optionBuilder.append(" TRANSACTIONAL=true ");
        }
        optionBuilder.append(" SPLIT ON(1,2)");
        this.tableDDLOptions = optionBuilder.toString();
    }
    
    @BeforeClass
    public static void doSetup() throws Exception {
        Map<String, String> serverProps = Maps.newHashMapWithExpectedSize(1);
        serverProps.put(QueryServices.EXTRA_JDBC_ARGUMENTS_ATTRIB, QueryServicesOptions.DEFAULT_EXTRA_JDBC_ARGUMENTS);
        Map<String, String> clientProps = Maps.newHashMapWithExpectedSize(1);
        clientProps.put(QueryServices.TRANSACTIONS_ENABLED, "true");
        setUpRealDriver(new ReadOnlyProps(serverProps.entrySet().iterator()), new ReadOnlyProps(clientProps.entrySet().iterator()));
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
    
    @Test
    public void testSecondaryIndex() throws Exception {
        final String fullTableName = SchemaUtil.getTableName(schemaName, dataTable);
        final String indxTable = String.format("%s_%s", dataTable, "INDX");
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(QueryServices.TRANSACTIONS_ENABLED, Boolean.TRUE.toString());
        props.setProperty(QueryServices.EXPLAIN_ROW_COUNT_ATTRIB, Boolean.FALSE.toString());
        Connection conn = DriverManager.getConnection(getUrl(), props);
        Statement stmt = conn.createStatement();
        try {
        
            stmt.execute(String.format("CREATE TABLE %s (ID INTEGER NOT NULL PRIMARY KEY, NAME VARCHAR, ZIP INTEGER) %s", fullTableName, tableDDLOptions));
            String upsertQuery = String.format("UPSERT INTO %s VALUES(?, ?, ?)", fullTableName);
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
                    ResultSet rs = conn.createStatement().executeQuery("SELECT count(*) from "+fullTableName);
                    assertTrue(rs.next());
                    assertEquals("Unexpected row count ", 2, rs.getInt(1));
                    assertFalse(rs.next());
                    rs = conn2.createStatement().executeQuery("SELECT count(*) from "+fullTableName);
                    assertTrue(rs.next());
                    assertEquals("Unexpected row count ", 4, rs.getInt(1));
                    assertFalse(rs.next());
                }
            }
            
            stmt.execute(String.format("CREATE %s INDEX %s ON %s  (LPAD(UPPER(NAME),8,'x')||'_xyz') ASYNC ", (localIndex ? "LOCAL" : ""), indxTable, fullTableName));
   
            //verify rows are fetched from data table.
            String selectSql = String.format("SELECT LPAD(UPPER(NAME),8,'x')||'_xyz',ID FROM %s", fullTableName);
            ResultSet rs = conn.createStatement().executeQuery("EXPLAIN " + selectSql);
            String actualExplainPlan = QueryUtil.getExplainPlan(rs);
            
            //assert we are pulling from data table.
            assertEquals(String.format("CLIENT 3-CHUNK PARALLEL 1-WAY ROUND ROBIN FULL SCAN OVER %s", fullTableName), actualExplainPlan);
            
            rs = stmt1.executeQuery(selectSql);
            assertTrue(rs.next());
            assertEquals("xxUNAME1_xyz", rs.getString(1));    
            assertTrue(rs.next());
            assertEquals("xxUNAME2_xyz", rs.getString(1));
            assertFalse(rs.next());
            conn.commit();
           
            //run the index MR job.
            final IndexTool indexingTool = new IndexTool();
            Configuration conf = new Configuration(getUtility().getConfiguration());
            conf.set(QueryServices.TRANSACTIONS_ENABLED, Boolean.TRUE.toString());
            indexingTool.setConf(conf);

            final String[] cmdArgs = getArgValues(schemaName, dataTable, indxTable, directApi);
            int status = indexingTool.run(cmdArgs);
            assertEquals(0, status);
            
            // insert two more rows
            upsertRow(stmt1, 3);
            upsertRow(stmt1, 4);
            conn.commit();
            
            rs = stmt1.executeQuery("SELECT LPAD(UPPER(NAME),8,'x')||'_xyz',ID FROM "+fullTableName);

            //assert we are pulling from index table.
            rs = conn.createStatement().executeQuery("EXPLAIN " + selectSql);
            actualExplainPlan = QueryUtil.getExplainPlan(rs);
            assertExplainPlan(actualExplainPlan, schemaName, dataTable, indxTable, localIndex);
            
            rs = stmt.executeQuery(selectSql);
            assertTrue(rs.next());
            assertEquals("xxUNAME1_xyz", rs.getString(1));
            assertEquals(1, rs.getInt(2));
            
            assertTrue(rs.next());
            assertEquals("xxUNAME2_xyz", rs.getString(1));
            assertEquals(2, rs.getInt(2));

            assertTrue(rs.next());
            assertEquals("xxUNAME3_xyz", rs.getString(1));
            assertEquals(3, rs.getInt(2));
            
            assertTrue(rs.next());
            assertEquals("xxUNAME4_xyz", rs.getString(1));
            assertEquals(4, rs.getInt(2));
      
            assertFalse(rs.next());
            
            conn.createStatement().execute(String.format("DROP INDEX  %s ON %s",indxTable , fullTableName));
        } finally {
            conn.close();
        }
    }
    
    public static void assertExplainPlan(final String actualExplainPlan, String schemaName, String dataTable,
            String indxTable, boolean isLocal) {
        
        String expectedExplainPlan = "";
        if(isLocal) {
            final String localIndexName = SchemaUtil.getTableName(schemaName, dataTable);
            expectedExplainPlan = String.format("CLIENT 3-CHUNK PARALLEL 3-WAY ROUND ROBIN RANGE SCAN OVER %s [1]"
                + "\n    SERVER FILTER BY FIRST KEY ONLY", localIndexName);
        } else {
            expectedExplainPlan = String.format("CLIENT 1-CHUNK PARALLEL 1-WAY ROUND ROBIN FULL SCAN OVER %s"
                    + "\n    SERVER FILTER BY FIRST KEY ONLY",SchemaUtil.getTableName(schemaName, indxTable));
        }
        assertEquals(expectedExplainPlan,actualExplainPlan);
    }

    public static String[] getArgValues(String schemaName, String dataTable, String indxTable) {
        return getArgValues(schemaName, dataTable, indxTable, false);
    }
    
    public static String[] getArgValues(String schemaName, String dataTable, String indxTable, boolean directApi) {
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

    public static void upsertRow(PreparedStatement stmt, int i) throws SQLException {
        // insert row
        stmt.setInt(1, i);
        stmt.setString(2, "uname" + String.valueOf(i));
        stmt.setInt(3, 95050 + i);
        stmt.executeUpdate();
    }
    
}
