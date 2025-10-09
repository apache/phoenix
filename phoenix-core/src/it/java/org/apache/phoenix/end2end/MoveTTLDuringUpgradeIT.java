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
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessor.CompactionScanner;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.jdbc.PhoenixMonitoredConnection;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.ConnectionQueryServicesImpl;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.LiteralTTLExpression;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.util.ClientUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
import org.apache.phoenix.util.UpgradeUtil;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.DEFAULT_TTL;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TTL_DEFINED_IN_TABLE_DESCRIPTOR;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TTL_NOT_DEFINED;
import static org.apache.phoenix.query.QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES;
import static org.apache.phoenix.query.QueryServicesOptions.DEFAULT_TIMEOUT_DURING_UPGRADE_MS;
import static org.apache.phoenix.thirdparty.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
@Category(NeedsOwnMiniClusterTest.class)
public class MoveTTLDuringUpgradeIT extends ParallelStatsDisabledIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(MoveTTLDuringUpgradeIT.class);

    @Test
    public void testMoveHBaseLevelTTLToSYSCAT() throws Exception {
        String schema = "S_" + generateUniqueName();
        Admin admin = getUtility().getAdmin();
        Map<String, Integer> tableTTLMap = createMultiHBaseTablesAndEquivalentInSYSCAT(schema, admin);
        try (PhoenixMonitoredConnection conn = getConnection(false, null).unwrap(PhoenixMonitoredConnection.class);
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
            conn.getQueryServices().clearCache();
            phxMetaConn.getQueryServices().clearCache();

            //TestUtil.dumpTable(conn, TableName.valueOf(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES));

            String sql = String.format("SELECT TABLE_NAME, TTL FROM SYSTEM.CATALOG WHERE TABLE_SCHEM = '%s' AND TABLE_TYPE = '%s' AND TTL IS NOT NULL", schema,
                    PTableType.TABLE.getSerializedValue());
            ResultSet rs = phxMetaConn.createStatement().executeQuery(sql);
            int count = 0;
            while (rs.next()) {
                count++;
                String table = rs.getString(1);
                LOGGER.info("found table {} : {} ", count, table);
                int ttl = tableTTLMap.get(table);
                int ttlInSyscat = Integer.valueOf(rs.getString(2)).intValue();
                //Check if TTL is moved to SYSCAT.
                //assertEquals(ttl, );
                //Check if TTL at HBase level is reset.
                TableDescriptor tableDescriptor = admin.getDescriptor(TableName.valueOf(SchemaUtil.getTableName(schema, table)));
                ColumnFamilyDescriptor[] columnFamilies = tableDescriptor.getColumnFamilies();
                if (ttlInSyscat == TTL_DEFINED_IN_TABLE_DESCRIPTOR) {
                    assertEquals(ttl, columnFamilies[0].getTimeToLive());
                    assertEquals(new LiteralTTLExpression(TTL_DEFINED_IN_TABLE_DESCRIPTOR),
                            phxMetaConn.unwrap(PhoenixMonitoredConnection.class).getTableNoCache(
                                    null,
                                            SchemaUtil.getTableName(Bytes.toBytes(schema),
                                                    Bytes.toBytes(table))).getTTLExpression());
                } else {
                    // Should not have any value other than TTL_DEFINED_IN_TABLE_DESCRIPTOR when TTL is not null
                    fail();
                }

            }
            if (count == 0) {
                // Should have at least one table with TTL defined
                fail();
            }
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }

    }

    private Map<String, Integer> createMultiHBaseTablesAndEquivalentInSYSCAT(String schema, Admin admin) throws SQLException, IOException {
        String table = "";
        int numOfTable = 20;
        int randomTTL = 0;
        Map<String, Integer> tableTTLMap = new HashMap<>();
        //Set<TableDescriptor> modifiedTableDescriptors = new HashSet<>();

        for (int i = 0; i < numOfTable; i++ ) {
            table = "T_" + generateUniqueName();
            randomTTL = i%3 == 0 ? HConstants.FOREVER :  100 + (int)(Math.random() * 1000);
            tableTTLMap.put(table, randomTTL);
            String ddl = "CREATE TABLE  " + schema + "." + table +
                    "  (a_string varchar not null, b_string varbinary not null, col1 integer" +
                    "  CONSTRAINT pk PRIMARY KEY (a_string, b_string)) ";

            BaseTest.createTestTable(getUrl(), ddl);
            TableName tableName = TableName.valueOf(SchemaUtil.getTableName(schema, table));
            TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tableName);
            builder.setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(
                    DEFAULT_COLUMN_FAMILY_BYTES).setTimeToLive(randomTTL).build());
            admin.disableTable(tableName);
            admin.modifyTable(builder.build());
            admin.enableTable(tableName);
        }

        // Create table with non default column family
        for (int i = numOfTable; i < numOfTable+2; i++ ) {
            table = "T_" + generateUniqueName();
            randomTTL = i%2 == 0 ? HConstants.FOREVER :  100 + (int)(Math.random() * 1000);
            tableTTLMap.put(table, randomTTL);
            String ddl = "CREATE TABLE  " + schema + "." + table +
                    "  (a_string varchar not null, b_string varbinary not null, col1 integer" +
                    "  CONSTRAINT pk PRIMARY KEY (a_string, b_string)) DEFAULT_COLUMN_FAMILY='Z'";

            BaseTest.createTestTable(getUrl(), ddl);
            TableName tableName = TableName.valueOf(SchemaUtil.getTableName(schema, table));
            TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tableName);
            builder.setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(
                    Bytes.toBytes("Z")).setTimeToLive(randomTTL).build());
            admin.disableTable(tableName);
            admin.modifyTable(builder.build());
            admin.enableTable(tableName);
        }

        return tableTTLMap;
    }

    private Connection getConnection(boolean tenantSpecific, String tenantId,
                                     boolean isNamespaceMappingEnabled, boolean copyChildLinksDuringUpgrade)
            throws SQLException {
        if (tenantSpecific) {
            checkNotNull(tenantId);
            return createTenantConnection(tenantId);
        }
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
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
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
        return DriverManager.getConnection(getUrl(), props);
    }

}
