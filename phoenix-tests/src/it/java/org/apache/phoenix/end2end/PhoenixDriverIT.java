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

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.LINK_TYPE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CATALOG_SCHEMA;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CATALOG_TABLE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_SCHEM;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TENANT_ID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.snapshot.SnapshotCreationException;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTable.LinkType;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.UpgradeUtil;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(NeedsOwnMiniClusterTest.class)
public class PhoenixDriverIT extends BaseTest {
    
    private static HBaseTestingUtility hbaseTestUtil;
    private static String zkQuorum;
    
    @BeforeClass
    public static synchronized void setUp() throws Exception {
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
    
    public Connection createConnection(String tenantId, boolean isDifferentClient) throws SQLException {
        Properties props = new Properties();
        props.setProperty(QueryServices.RETURN_SEQUENCE_VALUES_ATTRIB, "false");
        // force the use of ConnectionQueryServicesImpl instead of ConnectionQueryServicesTestImpl
        props.put(QueryServices.EXTRA_JDBC_ARGUMENTS_ATTRIB,
            QueryServicesOptions.DEFAULT_EXTRA_JDBC_ARGUMENTS);
        if (tenantId!=null)
            props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
        StringBuilder sb = new StringBuilder(url);
        if (isDifferentClient)
            sb.append(PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR + "Client2");
        return DriverManager.getConnection(sb.toString(), props);
    }
    
    @Test
    public void testReturnAllSequencesNotCalledForNoOpenConnections() throws Exception {
        String schemaName = "S";
        String sequenceNameWithoutSchema = generateUniqueSequenceName();
        String sequenceName = SchemaUtil.getTableName(schemaName, sequenceNameWithoutSchema);
        
        Connection conn = createConnection(null, false);
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
        
        conn = createConnection(null, false);
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
        try (Connection globalConn = createConnection(null, false);
                Connection conn1 = createConnection("tenant1", false);
                Connection conn2 = createConnection("tenant1", false)) {
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
            String expectedTableName = viewName + QueryConstants.CHILD_VIEW_INDEX_NAME_SEPARATOR + baseTableIndexName;
            assertEquals("Parent Index table is not used ", expectedTableName, tableName);
            
            // verify that we can look up the index using PhoenixRuntime from a different client
            PTable table = PhoenixRuntime.getTable(conn2, tableName);
            assertEquals(indexTable, table);
        }
    }
    
    @Test
    public void testMapMultiTenantTableToNamespaceDuringUpgrade() throws SQLException,
            SnapshotCreationException, IllegalArgumentException, IOException, InterruptedException {
        String schemaName = "S_" + generateUniqueName();
        String tableName = "T_" + generateUniqueName();
        String phoenixFullTableName = SchemaUtil.getTableName(schemaName, tableName);
        String viewName1 = "VB_" + generateUniqueName();
        String viewName2 = "VC_" + generateUniqueName();

        try (Connection conn = createConnection(null, false)) {
            conn.createStatement().execute("CREATE TABLE " + phoenixFullTableName
                    + "(k VARCHAR not null, v INTEGER not null, f INTEGER, g INTEGER NULL, h INTEGER NULL CONSTRAINT pk PRIMARY KEY(k,v)) MULTI_TENANT=true");
        }

        String[] tenantIds = new String[] { "tenant1", "tenant2" };
        for (String tenantId : tenantIds) {
            try (Connection conn = createConnection(tenantId, false)) {
                // create view
                conn.createStatement().execute("CREATE VIEW " + schemaName + "." + viewName1
                        + " (col VARCHAR) AS SELECT * FROM " + phoenixFullTableName);
                // create child view
                conn.createStatement().execute("CREATE VIEW " + schemaName + "." + viewName2
                        + " (col2 VARCHAR) AS SELECT * FROM " + schemaName + "." + viewName1);
            }
        }

        try (Connection conn = createConnection(null, true)) {
            String url = conn.unwrap(PhoenixConnection.class).getURL();
            Properties props = new Properties();
            props.setProperty(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, Boolean.toString(true));
            props.setProperty(QueryServices.IS_SYSTEM_TABLE_MAPPED_TO_NAMESPACE,
                Boolean.toString(false));
            try (PhoenixConnection phxConn =
                    DriverManager.getConnection(url, props).unwrap(PhoenixConnection.class)) {
                UpgradeUtil.upgradeTable(phxConn, phoenixFullTableName);
            }

            // verify physical table link
            String physicalTableName =
                    SchemaUtil.getPhysicalHBaseTableName(schemaName, tableName, true).getString();
            for (String tenantId : tenantIds) {
                assertEquals(physicalTableName, getPhysicalTable(conn, tenantId, schemaName, viewName1));
                assertEquals(physicalTableName, getPhysicalTable(conn, tenantId, schemaName, viewName2));
            }
        }
    }

    private String getPhysicalTable(Connection conn, String tenantId, String schemeName, String tableName) throws SQLException {
        String query =
                "SELECT COLUMN_FAMILY FROM " + SYSTEM_CATALOG_SCHEMA + "." + SYSTEM_CATALOG_TABLE
                        + " WHERE " + TENANT_ID + "=? AND " + TABLE_SCHEM + "=? AND " + TABLE_NAME
                        + "=? AND " + LINK_TYPE + "="
                        + LinkType.PHYSICAL_TABLE.getSerializedValue();
        PreparedStatement stmt = conn.prepareStatement(query);
        stmt.setString(1, tenantId);
        stmt.setString(2, schemeName);
        stmt.setString(3, tableName);
        ResultSet rs = stmt.executeQuery();
        assertTrue(rs.next());
        String physicalTableName = rs.getString(1);
        assertFalse(rs.next());
        return physicalTableName;
    }
}
