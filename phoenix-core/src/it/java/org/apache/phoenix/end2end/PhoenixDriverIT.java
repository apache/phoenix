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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.BeforeClass;
import org.junit.Test;

public class PhoenixDriverIT extends BaseUniqueNamesOwnClusterIT {
    
    private static HBaseTestingUtility hbaseTestUtil;
    private static String zkQuorum;
    
    @BeforeClass
    public static void setUp() throws Exception {
        Configuration conf = HBaseConfiguration.create();
        hbaseTestUtil = new HBaseTestingUtility(conf);
        setUpConfigForMiniCluster(conf);
        conf.set(QueryServices.EXTRA_JDBC_ARGUMENTS_ATTRIB, QueryServicesOptions.DEFAULT_EXTRA_JDBC_ARGUMENTS);
        hbaseTestUtil.startMiniCluster();
        // establish url and quorum. Need to use PhoenixDriver and not PhoenixTestDriver
        zkQuorum = "localhost:" + hbaseTestUtil.getZkCluster().getClientPort();
        url = PhoenixRuntime.JDBC_PROTOCOL + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR + zkQuorum;
        DriverManager.registerDriver(PhoenixDriver.INSTANCE);
    }
    
    public Connection createConnection(boolean isMultiTenant, boolean isDifferentClient) throws SQLException {
        Properties props = new Properties();
        props.setProperty(QueryServices.RETURN_SEQUENCE_VALUES_ATTRIB, "false");
        // force the use of ConnectionQueryServicesImpl instead of ConnectionQueryServicesTestImpl
        props.put(QueryServices.EXTRA_JDBC_ARGUMENTS_ATTRIB,
            QueryServicesOptions.DEFAULT_EXTRA_JDBC_ARGUMENTS);
        if (isMultiTenant)
            props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, "tenant1");
        StringBuilder sb = new StringBuilder(url);
        if (isMultiTenant)
            sb.append(PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR + "Client2");
        return DriverManager.getConnection(sb.toString(), props);
    }
    
    @Test
    public void testReturnAllSequencesNotCalledForNoOpenConnections() throws Exception {
        String schemaName = "S";
        String sequenceNameWithoutSchema = generateUniqueSequenceName();
        String sequenceName = SchemaUtil.getTableName(schemaName, sequenceNameWithoutSchema);
        
        Connection conn = createConnection(false, false);
        conn.createStatement().execute("CREATE SEQUENCE " + sequenceName + " START WITH 3 INCREMENT BY 2 CACHE 5");
        
        String query = "SELECT NEXT VALUE FOR " + sequenceName ;
        ResultSet rs = conn.prepareStatement(query).executeQuery();
        assertTrue(rs.next());
        assertEquals(3, rs.getInt(1));
        assertFalse(rs.next());
        rs = conn.prepareStatement(query).executeQuery();
        assertTrue(rs.next());
        assertEquals(5, rs.getInt(1));
        assertFalse(rs.next());
        conn.close();
        
        conn = createConnection(false, false);
        // verify that calling close() does not return sequence values back to the server
        query = "SELECT CURRENT_VALUE FROM \"SYSTEM\".\"SEQUENCE\" WHERE SEQUENCE_SCHEMA=? AND SEQUENCE_NAME=?";
        PreparedStatement preparedStatement = conn.prepareStatement(query);
        preparedStatement.setString(1, schemaName);
        preparedStatement.setString(2, sequenceNameWithoutSchema);
        rs = preparedStatement.executeQuery();
        assertTrue(rs.next());
        assertEquals(13, rs.getInt(1));
        assertFalse(rs.next());
        conn.close();
    }
    
    @Test
    public void testViewParentIndexLookupMutipleClients() throws Exception {
        helpTestViewParentIndexLookupMutipleClients(false);
    }
    
    @Test
    public void testMulitTenantViewParentIndexLookupMutipleClients() throws Exception {
        helpTestViewParentIndexLookupMutipleClients(true);
    }
    
    public void helpTestViewParentIndexLookupMutipleClients(boolean isMultiTenant) throws Exception {
        final String baseTableName = generateUniqueName();
        final String baseTableIndexName = generateUniqueName();
        final String viewName = generateUniqueName();
        try (Connection globalConn = createConnection(false, false);
                Connection conn1 = createConnection(isMultiTenant, false);
                Connection conn2 = createConnection(isMultiTenant, false)) {
            // create base table
            String baseTableDdl = "CREATE TABLE " + baseTableName + " (" +
                    ( isMultiTenant ? "TENANT_ID VARCHAR(1) NOT NULL," : "") +
                    "PK CHAR(1) NOT NULL," +
                    "V1 CHAR(1)," +
                    "V2 CHAR(1)," +
                    "V3 CHAR(1)" + 
                    "CONSTRAINT pk PRIMARY KEY (" + (isMultiTenant ? "TENANT_ID," : "") + " pk))";
            globalConn.createStatement().execute(baseTableDdl);
            
            // create index on parent view
            globalConn.createStatement().execute("CREATE INDEX " + baseTableIndexName + " ON " + baseTableName + " (V2) INCLUDE (v1, V3)");
            
            // create a view on the base table
            String viewDDL = "CREATE VIEW " + viewName + " AS SELECT * FROM " + baseTableName + " WHERE V1 = 'X'";
            conn1.createStatement().execute(viewDDL);
            conn1.commit();

            // ensure we can use parent table index
            String sql = "SELECT V3 FROM " + viewName +" WHERE V2 = '3'";
            PhoenixStatement stmt = conn1.createStatement().unwrap(PhoenixStatement.class);
            stmt.executeQuery(sql);
            PTable indexTable = stmt.getQueryPlan().getTableRef().getTable();
            String tableName = indexTable.getName().getString();
            String expectedTableName = baseTableIndexName + QueryConstants.CHILD_VIEW_INDEX_NAME_SEPARATOR + viewName;
            assertEquals("Parent Index table is not used ", expectedTableName, tableName);
            
            // verify that we can look up the index using PhoenixRuntime from a different client
            PTable table = PhoenixRuntime.getTable(conn2, tableName);
            assertEquals(indexTable, table);
        }
    }
    
}
