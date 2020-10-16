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

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.snapshot.SnapshotCreationException;
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

            try(Admin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()){
                assertTrue(admin.tableExists(TableName.valueOf(phoenixFullTableName)));
                assertTrue(admin.tableExists(TableName.valueOf(schemaName + QueryConstants.NAME_SEPARATOR + indexName)));
                assertTrue(admin.tableExists(TableName.valueOf(MetaDataUtil.getViewIndexPhysicalName(Bytes.toBytes(phoenixFullTableName)))));
            }
            Properties props = new Properties();
            props.setProperty(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, Boolean.toString(true));
            props.setProperty(QueryServices.IS_SYSTEM_TABLE_MAPPED_TO_NAMESPACE, Boolean.toString(false));
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
            String hbaseTableName = SchemaUtil.getPhysicalTableName(Bytes.toBytes(phoenixFullTableName), true)
                .getNameAsString();
            try (Admin admin = phxConn.getQueryServices().getAdmin()){

                assertTrue(admin.tableExists(TableName.valueOf(hbaseTableName)));
                assertTrue(admin.tableExists(TableName.valueOf(Bytes.toBytes(hbaseTableName))));
                assertTrue(admin.tableExists(TableName.valueOf(schemaName + QueryConstants.NAMESPACE_SEPARATOR + indexName)));
                assertTrue(admin.tableExists(TableName.valueOf(MetaDataUtil.getViewIndexPhysicalName(Bytes.toBytes(hbaseTableName)))));
            }
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
        }
    }

    @Test
    public void testMapMultiTenantTableToNamespaceDuringUpgrade() throws SQLException, SnapshotCreationException,
        IllegalArgumentException, IOException, InterruptedException {
        String[] strings = new String[] { "a", "b", "c", "d" };
        String schemaName1 = "S_" +generateUniqueName(); // TEST
        String schemaName2 = "S_" +generateUniqueName(); // DIFF
        String phoenixFullTableName = schemaName1 + "." + generateUniqueName();
        String hbaseTableName = SchemaUtil.getPhysicalTableName(Bytes.toBytes(phoenixFullTableName), true)
            .getNameAsString();
        String indexName = "IDX_" + generateUniqueName();
        String viewName = "V_" + generateUniqueName();
        String viewName1 = "V1_" + generateUniqueName();
        String viewIndexName = "V_IDX_" + generateUniqueName();
        String tenantViewIndexName = "V1_IDX_" + generateUniqueName();

        String[] tableNames = new String[] { phoenixFullTableName, schemaName2 + "." + viewName1, schemaName1 + "." + viewName1, viewName1 };
        String[] viewIndexes = new String[] { schemaName1 + "." + viewIndexName, schemaName2 + "." + viewIndexName };
        String[] tenantViewIndexes = new String[] { schemaName1 + "." + tenantViewIndexName, schemaName2 + "." + tenantViewIndexName };
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute("CREATE TABLE " + phoenixFullTableName
                + "(k VARCHAR not null, v INTEGER not null, f INTEGER, g INTEGER NULL, h INTEGER NULL CONSTRAINT pk PRIMARY KEY(k,v)) MULTI_TENANT=true");
            PreparedStatement upsertStmt = conn
                .prepareStatement("UPSERT INTO " + phoenixFullTableName + " VALUES(?, ?, 0, 0, 0)");
            int i = 1;
            for (String str : strings) {
                upsertStmt.setString(1, str);
                upsertStmt.setInt(2, i++);
                upsertStmt.execute();
            }
            conn.commit();

            // creating global index
            conn.createStatement().execute("create index " + indexName + " on " + phoenixFullTableName + "(f)");
            // creating view in schema 'diff'
            conn.createStatement().execute("CREATE VIEW " + schemaName2 + "." + viewName + " (col VARCHAR) AS SELECT * FROM " + phoenixFullTableName);
            // creating view in schema 'test'
            conn.createStatement().execute("CREATE VIEW " + schemaName1 + "." + viewName + " (col VARCHAR) AS SELECT * FROM " + phoenixFullTableName);
            conn.createStatement().execute("CREATE VIEW " + viewName + " (col VARCHAR) AS SELECT * FROM " + phoenixFullTableName);
            // Creating index on views
            conn.createStatement().execute("create local index " + viewIndexName + " on " + schemaName2 + "." + viewName + "(col)");
            conn.createStatement().execute("create local index " + viewIndexName + " on " + schemaName1 + "." + viewName + "(col)");
        }
        Properties props = new Properties();
        String tenantId = strings[0];
        props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            PreparedStatement upsertStmt = conn
                .prepareStatement("UPSERT INTO " + phoenixFullTableName + "(k,v,f,g,h)  VALUES(?, ?, 0, 0, 0)");
            int i = 1;
            for (String str : strings) {
                upsertStmt.setString(1, str);
                upsertStmt.setInt(2, i++);
                upsertStmt.execute();
            }
            conn.commit();
            // creating view in schema 'diff'
            conn.createStatement()
                .execute("CREATE VIEW " + schemaName2 + "." + viewName1 + " (col VARCHAR) AS SELECT * FROM " + phoenixFullTableName);
            // creating view in schema 'test'
            conn.createStatement()
                .execute("CREATE VIEW " + schemaName1 + "." + viewName1 + " (col VARCHAR) AS SELECT * FROM " + phoenixFullTableName);
            conn.createStatement().execute("CREATE VIEW " + viewName1 + " (col VARCHAR) AS SELECT * FROM " + phoenixFullTableName);
            // Creating index on views
            conn.createStatement().execute("create index " + tenantViewIndexName + " on " + schemaName2 + "." + viewName1 + "(col)");
            conn.createStatement().execute("create index " + tenantViewIndexName + " on " + schemaName1 + "." + viewName1 + "(col)");
        }

        props = new Properties();
        props.setProperty(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, Boolean.toString(true));
        props.setProperty(QueryServices.IS_SYSTEM_TABLE_MAPPED_TO_NAMESPACE, Boolean.toString(false));
        PhoenixConnection phxConn = DriverManager.getConnection(getUrl(), props).unwrap(PhoenixConnection.class);
        UpgradeUtil.upgradeTable(phxConn, phoenixFullTableName);
        props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
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
        int i = 1;
        String indexPhysicalTableName = Bytes
            .toString(MetaDataUtil.getViewIndexPhysicalName(Bytes.toBytes(hbaseTableName)));
        // validate data with tenant
        for (String tableName : tableNames) {
            assertTableUsed(phxConn, tableName, hbaseTableName);
            ResultSet rs = phxConn.createStatement().executeQuery("select * from " + tableName);
            assertTrue(rs.next());
            do {
                assertEquals(i++, rs.getInt(1));
            } while (rs.next());
            i = 1;
        }
        // validate view Index data
        for (String viewIndex : tenantViewIndexes) {
            assertTableUsed(phxConn, viewIndex, indexPhysicalTableName);
            ResultSet rs = phxConn.createStatement().executeQuery("select * from " + viewIndex);
            assertTrue(rs.next());
            do {
                assertEquals(i++, rs.getInt(2));
            } while (rs.next());
            i = 1;
        }
        phxConn.close();
        props.remove(PhoenixRuntime.TENANT_ID_ATTRIB);
        phxConn = DriverManager.getConnection(getUrl(), props).unwrap(PhoenixConnection.class);

        // validate view Index data
        for (String viewIndex : viewIndexes) {
            assertTableUsed(phxConn, viewIndex, hbaseTableName);
            ResultSet rs = phxConn.createStatement().executeQuery("select * from " + viewIndex);
            for (String str : strings) {
                assertTrue(rs.next());
                assertEquals(str, rs.getString(1));
            }
        }
        phxConn.close();
    }

    public void assertTableUsed(Connection conn, String phoenixTableName, String hbaseTableName) throws SQLException {
        ResultSet rs = conn.createStatement().executeQuery("EXPLAIN SELECT * FROM " + phoenixTableName);
        assertTrue(rs.next());
        assertTrue(rs.getString(1).contains(hbaseTableName));
    }
}
