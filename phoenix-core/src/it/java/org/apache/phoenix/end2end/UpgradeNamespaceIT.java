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

import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.parse.PFunction;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.PMetaData;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PNameFactory;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.util.MetaDataUtil;
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
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category(NeedsOwnMiniClusterTest.class)
public class UpgradeNamespaceIT extends ParallelStatsDisabledIT {
    @Test
    public void testMapTableToNamespaceDuringUpgrade()
        throws SQLException, IOException, IllegalArgumentException, InterruptedException {
        //This test needs to run in its own test class because other tests creating views or view
        // indexes can affect the value of the sequence it checks.
        String[] strings = new String[] { "a", "b", "c", "d" };

        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String schemaName = "TEST";
            String phoenixFullTableName = schemaName + "." + generateUniqueName();
            String indexName = "IDX_" + generateUniqueName();
            String localIndexName = "LIDX_" + generateUniqueName();

            String viewName = "VIEW_" + generateUniqueName();
            String viewIndexName = "VIDX_" + generateUniqueName();

            String[] tableNames = new String[] { phoenixFullTableName, schemaName + "." + indexName,
                schemaName + "." + localIndexName, "diff." + viewName, "test." + viewName, viewName};
            String[] viewIndexes = new String[] { "diff." + viewIndexName, "test." + viewIndexName };
            conn.createStatement().execute("CREATE TABLE " + phoenixFullTableName
                + "(k VARCHAR PRIMARY KEY, v INTEGER, f INTEGER, g INTEGER NULL, h INTEGER NULL)");
            PreparedStatement upsertStmt = conn
                .prepareStatement("UPSERT INTO " + phoenixFullTableName + " VALUES(?, ?, 0, 0, 0)");
            int i = 1;
            for (String str : strings) {
                upsertStmt.setString(1, str);
                upsertStmt.setInt(2, i++);
                upsertStmt.execute();
            }
            conn.commit();
            // creating local index
            conn.createStatement()
                .execute("create local index " + localIndexName + " on " + phoenixFullTableName + "(K)");
            // creating global index
            conn.createStatement().execute("create index " + indexName + " on " + phoenixFullTableName + "(k)");
            // creating view in schema 'diff'
            conn.createStatement().execute("CREATE VIEW diff." + viewName + " (col VARCHAR) AS SELECT * FROM " + phoenixFullTableName);
            // creating view in schema 'test'
            conn.createStatement().execute("CREATE VIEW test." + viewName + " (col VARCHAR) AS SELECT * FROM " + phoenixFullTableName);
            conn.createStatement().execute("CREATE VIEW " + viewName + "(col VARCHAR) AS SELECT * FROM " + phoenixFullTableName);
            // Creating index on views
            conn.createStatement().execute("create index " + viewIndexName + "  on diff." + viewName + "(col)");
            conn.createStatement().execute("create index " + viewIndexName + " on test." + viewName + "(col)");

            // validate data
            for (String tableName : tableNames) {
                ResultSet rs = conn.createStatement().executeQuery("select * from " + tableName);
                for (String str : strings) {
                    assertTrue(rs.next());
                    assertEquals(str, rs.getString(1));
                }
            }

            // validate view Index data
            for (String viewIndex : viewIndexes) {
                ResultSet rs = conn.createStatement().executeQuery("select * from " + viewIndex);
                for (String str : strings) {
                    assertTrue(rs.next());
                    assertEquals(str, rs.getString(2));
                }
            }

            HBaseAdmin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin();
            assertTrue(admin.tableExists(phoenixFullTableName));
            assertTrue(admin.tableExists(schemaName + QueryConstants.NAME_SEPARATOR + indexName));
            assertTrue(admin.tableExists(MetaDataUtil.getViewIndexPhysicalName(Bytes.toBytes(phoenixFullTableName))));
            Properties props = new Properties();
            props.setProperty(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, Boolean.toString(true));
            props.setProperty(QueryServices.IS_SYSTEM_TABLE_MAPPED_TO_NAMESPACE, Boolean.toString(false));
            admin.close();
            PhoenixConnection phxConn = DriverManager.getConnection(getUrl(), props).unwrap(PhoenixConnection.class);
            // long oldSeqValue =
            UpgradeUtil.upgradeTable(phxConn, phoenixFullTableName);
            phxConn.close();
            props = new Properties();
            phxConn = DriverManager.getConnection(getUrl(), props).unwrap(PhoenixConnection.class);
            // purge MetaDataCache except for system tables
            phxConn.getMetaDataCache().pruneTables(new PMetaData.Pruner() {
                @Override public boolean prune(PTable table) {
                    return table.getType() != PTableType.SYSTEM;
                }

                @Override public boolean prune(PFunction function) {
                    return false;
                }
            });
            admin = phxConn.getQueryServices().getAdmin();
            String hbaseTableName = SchemaUtil.getPhysicalTableName(Bytes.toBytes(phoenixFullTableName), true)
                .getNameAsString();
            assertTrue(admin.tableExists(hbaseTableName));
            assertTrue(admin.tableExists(Bytes.toBytes(hbaseTableName)));
            assertTrue(admin.tableExists(schemaName + QueryConstants.NAMESPACE_SEPARATOR + indexName));
            assertTrue(admin.tableExists(MetaDataUtil.getViewIndexPhysicalName(Bytes.toBytes(hbaseTableName))));
            i = 0;
            // validate data
            for (String tableName : tableNames) {
                ResultSet rs = phxConn.createStatement().executeQuery("select * from " + tableName);
                for (String str : strings) {
                    assertTrue(rs.next());
                    assertEquals(str, rs.getString(1));
                }
            }
            // validate view Index data
            for (String viewIndex : viewIndexes) {
                ResultSet rs = conn.createStatement().executeQuery("select * from " + viewIndex);
                for (String str : strings) {
                    assertTrue(rs.next());
                    assertEquals(str, rs.getString(2));
                }
            }
            PName tenantId = phxConn.getTenantId();
            PName physicalName = PNameFactory.newName(hbaseTableName);
            String newSchemaName = MetaDataUtil.getViewIndexSequenceSchemaName(physicalName, true);
            String newSequenceName = MetaDataUtil.getViewIndexSequenceName(physicalName, tenantId, true);
            verifySequenceValue(null, newSequenceName, newSchemaName,Short.MIN_VALUE + 3);
            admin.close();
        }
    }
}
