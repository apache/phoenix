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

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.ByteStringer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessor.PhoenixRegionServerEndpoint;
import org.apache.phoenix.coprocessor.generated.RegionServerEndpointProtos;
import org.apache.phoenix.exception.StaleMetadataCacheException;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.util.ClientUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.ServerUtil;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@Category({NeedsOwnMiniClusterTest.class })
public class PhoenixRegionServerEndpointIT extends BaseTest {
    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        Map<String, String> props = Maps.newHashMapWithExpectedSize(1);
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }

    // Tests that PhoenixRegionServerEndpoint validates the last ddl timestamp for base table.
    @Test
    public void testValidateLastDDLTimestampNoException() throws SQLException {
        HRegionServer regionServer = utility.getMiniHBaseCluster().getRegionServer(0);
        PhoenixRegionServerEndpoint coprocessor = getPhoenixRegionServerEndpoint(regionServer);
        assertNotNull(coprocessor);
        ServerRpcController controller = new ServerRpcController();
        String tableNameStr = generateUniqueName();
        Properties props = new Properties();
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(false);
            String ddl = getCreateTableStmt(tableNameStr);
            // Create a test table.
            conn.createStatement().execute(ddl);
            conn.commit();

            PTable pTable = PhoenixRuntime.getTable(conn, tableNameStr);
            long lastDDLTimestamp = pTable.getLastDDLTimestamp();
            RegionServerEndpointProtos.ValidateLastDDLTimestampRequest request = getRequest(
                    HConstants.EMPTY_BYTE_ARRAY, HConstants.EMPTY_BYTE_ARRAY,
                    tableNameStr.getBytes(StandardCharsets.UTF_8), lastDDLTimestamp);
            // Call coprocessor#validateLastDDLTimestamp to validate
            // client provided last ddl timestamp
            coprocessor.validateLastDDLTimestamp(controller, request, null);
            assertFalse(controller.failed());
        }
    }

    // Tests that PhoenixRegionServerEndpoint throws StaleMetadataCacheException if client
    // provided last ddl timestamp is less than server maintained last ddl timestamp.
    @Test
    public void testValidateLastDDLTimestampWithException() throws SQLException {
        HRegionServer regionServer = utility.getMiniHBaseCluster().getRegionServer(0);
        PhoenixRegionServerEndpoint coprocessor = getPhoenixRegionServerEndpoint(regionServer);
        assertNotNull(coprocessor);
        ServerRpcController controller = new ServerRpcController();
        String tableNameStr = generateUniqueName();
        Properties props = new Properties();
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(false);
            String ddl = getCreateTableStmt(tableNameStr);
            // Create a test table.
            conn.createStatement().execute(ddl);
            conn.commit();

            PTable pTable = PhoenixRuntime.getTable(conn, tableNameStr);
            long lastDDLTimestamp = pTable.getLastDDLTimestamp();
            RegionServerEndpointProtos.ValidateLastDDLTimestampRequest request = getRequest(
                    HConstants.EMPTY_BYTE_ARRAY, HConstants.EMPTY_BYTE_ARRAY,
                    tableNameStr.getBytes(StandardCharsets.UTF_8), lastDDLTimestamp - 1);
            // Call coprocessor#validateLastDDLTimestamp to validate client provided
            // last ddl timestamp and make sure it throws an StaleMetadataCacheException
            coprocessor.validateLastDDLTimestamp(controller, request, null);
            assertTrue(controller.failed());
            Exception exception = controller.getFailedOn();
            Exception parsedException = ClientUtil.parseRemoteException(exception);
            assertTrue(parsedException instanceof StaleMetadataCacheException);
        }
    }

    // Tests that PhoenixRegionServerEndpoint validates the last ddl timestamp for tenant owned
    // views
    @Test
    public void testValidateLastDDLTimestampWithTenantID() throws SQLException {
        HRegionServer regionServer = utility.getMiniHBaseCluster().getRegionServer(0);
        PhoenixRegionServerEndpoint coprocessor = getPhoenixRegionServerEndpoint(regionServer);
        assertNotNull(coprocessor);
        ServerRpcController controller = new ServerRpcController();
        String tableNameStr = generateUniqueName();
        Properties props = new Properties();
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(false);
            String ddl = getCreateTableStmt(tableNameStr);
            // Create a test table.
            conn.createStatement().execute(ddl);
        }
        String tenantId = "T_" + generateUniqueName();
        Properties tenantProps = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        tenantProps.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
        PTable tenantViewTable;
        // Create view on table.
        String whereClause = " WHERE COL1 = 1000";
        String tenantViewNameStr = generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl(), tenantProps)) {
            conn.createStatement().execute(getCreateViewStmt(tenantViewNameStr,
                    tableNameStr, whereClause));
            tenantViewTable = PhoenixRuntime.getTableNoCache(conn, tenantViewNameStr);

            byte[] tenantIDBytes = Bytes.toBytes(tenantId);
            byte[] tenantViewNameBytes = Bytes.toBytes(tenantViewNameStr);
            long lastDDLTimestamp = tenantViewTable.getLastDDLTimestamp();
            RegionServerEndpointProtos.ValidateLastDDLTimestampRequest request = getRequest(
                    tenantIDBytes, HConstants.EMPTY_BYTE_ARRAY, tenantViewNameBytes,
                    lastDDLTimestamp);
            // Call coprocessor#validateLastDDLTimestamp to validate client provided
            // last ddl timestamp for tenant owned views.
            coprocessor.validateLastDDLTimestamp(controller, request, null);
            assertFalse(controller.failed());
        }
    }

    private String getCreateTableStmt(String tableName) {
        return   "CREATE TABLE " + tableName +
                "  (a_string varchar not null, col1 integer" +
                "  CONSTRAINT pk PRIMARY KEY (a_string)) ";
    }

    private String getCreateViewStmt(String viewName, String fullTableName, String whereClause) {
        String viewStmt =  "CREATE VIEW " + viewName +
                " AS SELECT * FROM "+ fullTableName + whereClause;
        return  viewStmt;
    }

    private PhoenixRegionServerEndpoint getPhoenixRegionServerEndpoint(HRegionServer regionServer) {
        PhoenixRegionServerEndpoint coproc = regionServer
                .getRegionServerCoprocessorHost()
                .findCoprocessor(PhoenixRegionServerEndpoint.class);
        return coproc;
    }

    private RegionServerEndpointProtos.ValidateLastDDLTimestampRequest getRequest(byte[] tenantID,
            byte[] schemaName, byte[] tableName, long lastDDLTimestamp) {
        RegionServerEndpointProtos.ValidateLastDDLTimestampRequest.Builder requestBuilder
                = RegionServerEndpointProtos.ValidateLastDDLTimestampRequest.newBuilder();
        RegionServerEndpointProtos.LastDDLTimestampRequest.Builder innerBuilder
                = RegionServerEndpointProtos.LastDDLTimestampRequest.newBuilder();
        innerBuilder.setTenantId(ByteStringer.wrap(tenantID));
        innerBuilder.setSchemaName(ByteStringer.wrap(schemaName));
        innerBuilder.setTableName(ByteStringer.wrap(tableName));
        innerBuilder.setLastDDLTimestamp(lastDDLTimestamp);
        requestBuilder.addLastDDLTimestampRequests(innerBuilder);
        return  requestBuilder.build();
    }
}
