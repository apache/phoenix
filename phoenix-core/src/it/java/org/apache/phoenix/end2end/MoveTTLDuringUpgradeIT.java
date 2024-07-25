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
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.UpgradeUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.DEFAULT_TTL;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TTL_NOT_DEFINED;
import static org.apache.phoenix.query.QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES;
import static org.apache.phoenix.query.QueryServicesOptions.DEFAULT_TIMEOUT_DURING_UPGRADE_MS;
import static org.apache.phoenix.thirdparty.com.google.common.base.Preconditions.checkNotNull;
import static org.junit.Assert.assertEquals;

@Category(NeedsOwnMiniClusterTest.class)
public class MoveTTLDuringUpgradeIT extends ParallelStatsDisabledIT {
    @Test
    public void testMoveHBaseLevelTTLToSYSCAT() throws Exception {
        String schema = "S_" + generateUniqueName();
        Admin admin = getUtility().getAdmin();
        Map<String, Integer> tableTTLMap = createMultiHBaseTablesAndEquivalentInSYSCAT(schema, admin);
        try (PhoenixConnection conn = getConnection(false, null).unwrap(PhoenixConnection.class);
             Connection metaConn = getConnection(false, null, false, false)){
            PhoenixConnection phxMetaConn = metaConn.unwrap(PhoenixConnection.class);
            phxMetaConn.setRunningUpgrade(true);

            Map<String, String> options = new HashMap<>();
            options.put(HConstants.HBASE_RPC_TIMEOUT_KEY, Integer.toString(DEFAULT_TIMEOUT_DURING_UPGRADE_MS));
            options.put(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, Integer.toString(DEFAULT_TIMEOUT_DURING_UPGRADE_MS));
            String clientPort = getUtility().getConfiguration().get(QueryServices.ZOOKEEPER_PORT_ATTRIB);

            String localQuorum = String.format("localhost:%s", clientPort);
            options.put(QueryServices.ZOOKEEPER_QUORUM_ATTRIB, localQuorum);
            options.put(QueryServices.ZOOKEEPER_PORT_ATTRIB, clientPort);
            UpgradeUtil.moveHBaseLevelTTLToSYSCAT(phxMetaConn, options);

            String sql = "SELECT TABLE_NAME, TTL FROM SYSTEM.CATALOG WHERE TABLE_SCHEM = '" + schema + "'";
            ResultSet rs = conn.createStatement().executeQuery(sql);
            while (rs.next()) {
                String table = rs.getString(1);
                int ttl = tableTTLMap.get(table);
                //Check if TTL is moved to SYSCAT.
                if (ttl != HConstants.FOREVER) {
                    assertEquals(ttl, Integer.valueOf(rs.getString(2)).intValue());
                } else {
                    assertEquals(ttl, TTL_NOT_DEFINED);
                }
                //Check if TTL at HBase level is reset.
                TableDescriptor tableDescriptor = admin.getDescriptor(TableName.valueOf(SchemaUtil.getTableName(schema, table)));
                ColumnFamilyDescriptor[] columnFamilies = tableDescriptor.getColumnFamilies();
                assertEquals(DEFAULT_TTL, columnFamilies[0].getTimeToLive());
            }
        }

    }

    private Map<String, Integer> createMultiHBaseTablesAndEquivalentInSYSCAT(String schema, Admin admin) throws SQLException, IOException {
        String table = "";
        int numOfTable = 20;
        int randomTTL = 0;
        Map<String, Integer> tableTTLMap = new HashMap<>();
        for (int i = 0; i < numOfTable; i++ ) {
            table = "T_" + generateUniqueName();
            randomTTL = i%3 == 0 ? HConstants.FOREVER : 100 + (int)(Math.random() * 1000);
            tableTTLMap.put(table, randomTTL);
            TableName tableName = TableName.valueOf(SchemaUtil.getTableName(schema, table));
            TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tableName);
            builder.setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(
                    DEFAULT_COLUMN_FAMILY_BYTES).setTimeToLive(randomTTL).build());
            admin.createTable(builder.build());
            upsertIntoSYSCAT(schema, table);
        }
        return tableTTLMap;
    }

    private void upsertIntoSYSCAT(String schema, String table) throws SQLException {
        try (PhoenixConnection connection = getConnection(false, null).unwrap(PhoenixConnection.class)){
            String dml = "UPSERT INTO SYSTEM.CATALOG (TENANT_ID, TABLE_SCHEM, TABLE_NAME, TTL) VALUES (?,?,?,?)";
            PreparedStatement statement = connection.prepareStatement(dml);
            statement.setString(1, null);
            statement.setString(2, schema);
            statement.setString(3, table);
            statement.setNull(4, Types.INTEGER);
        }

    }

    private Connection getConnection(boolean tenantSpecific, String tenantId,
                                     boolean isNamespaceMappingEnabled, boolean copyChildLinksDuringUpgrade)
            throws SQLException {
        if (tenantSpecific) {
            checkNotNull(tenantId);
            return createTenantConnection(tenantId);
        }
        Properties props = new Properties();
        if (isNamespaceMappingEnabled){
            props.setProperty(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, "true");
        }
        if (copyChildLinksDuringUpgrade){
            props.setProperty(QueryServices.MOVE_CHILD_LINKS_DURING_UPGRADE_ENABLED, "false");
        }
        return DriverManager.getConnection(getUrl(), props);
    }
    private Connection getConnection(boolean tenantSpecific, String tenantId) throws SQLException {
        return getConnection(tenantSpecific, tenantId, false, false);
    }

    private Connection createTenantConnection(String tenantId) throws SQLException {
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
        return DriverManager.getConnection(getUrl(), props);
    }

}
