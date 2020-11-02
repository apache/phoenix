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
package org.apache.phoenix.end2end.index;

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_COUNT;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_SCHEM;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.VIEW_INDEX_ID_DATA_TYPE;
import static org.apache.phoenix.util.MetaDataUtil.getViewIndexSequenceName;
import static org.apache.phoenix.util.MetaDataUtil.getViewIndexSequenceSchemaName;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.end2end.IndexToolIT;
import org.apache.phoenix.end2end.SplitSystemCatalogIT;
import org.apache.phoenix.hbase.index.IndexRegionObserver;
import org.apache.phoenix.hbase.index.Indexer;
import org.apache.phoenix.index.GlobalIndexChecker;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.PNameFactory;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class ViewIndexIT extends SplitSystemCatalogIT {
    private boolean isNamespaceMapped;

    @Parameters(name = "ViewIndexIT_isNamespaceMapped={0}") // name is used by failsafe as file name in reports
    public static synchronized Collection<Boolean> data() {
        return Arrays.asList(true, false);
    }

    private void createBaseTable(String schemaName, String tableName, boolean multiTenant,
                                 Integer saltBuckets, String splits, boolean mutable)
            throws SQLException {
        Connection conn = getConnection();
        if (isNamespaceMapped) {
            conn.createStatement().execute("CREATE SCHEMA IF NOT EXISTS " + schemaName);
        }
        String ddl = "CREATE " + (mutable ? "" : "IMMUTABLE") +
            " TABLE " + SchemaUtil.getTableName(schemaName, tableName) +
            " (t_id VARCHAR NOT NULL,\n" +
                "k1 VARCHAR NOT NULL,\n" +
                "k2 INTEGER NOT NULL,\n" +
                "v1 VARCHAR,\n" +
                "v2 INTEGER,\n" +
                "CONSTRAINT pk PRIMARY KEY (t_id, k1, k2))\n";
        String ddlOptions = multiTenant ? "MULTI_TENANT=true" : "";
        if (saltBuckets != null) {
            ddlOptions = ddlOptions
                    + (ddlOptions.isEmpty() ? "" : ",")
                    + "salt_buckets=" + saltBuckets;
        }
        if (splits != null) {
            ddlOptions = ddlOptions
                    + (ddlOptions.isEmpty() ? "" : ",")
                    + "splits=" + splits;            
        }
        conn.createStatement().execute(ddl + ddlOptions);
        conn.close();
    }

    private void createView(Connection conn, String schemaName, String viewName, String baseTableName) throws SQLException {
        if (isNamespaceMapped) {
            conn.createStatement().execute("CREATE SCHEMA IF NOT EXISTS " + schemaName);
        }
        String fullViewName = SchemaUtil.getTableName(schemaName, viewName);
        String fullTableName = SchemaUtil.getTableName(schemaName, baseTableName);
        conn.createStatement().execute("CREATE VIEW " + fullViewName + " AS SELECT * FROM " + fullTableName);
        conn.commit();
    }

    private void createViewIndex(Connection conn, String schemaName, String indexName, String viewName,
                                 String indexColumn) throws SQLException {
        if (isNamespaceMapped) {
            conn.createStatement().execute("CREATE SCHEMA IF NOT EXISTS " + schemaName);
            conn.commit();
        }
        String fullViewName = SchemaUtil.getTableName(schemaName, viewName);
        conn.createStatement().execute("CREATE INDEX " + indexName + " ON " + fullViewName + "(" + indexColumn + ")");
        conn.commit();
    }
    
    private PhoenixConnection getConnection() throws SQLException{
        Properties props = new Properties();
        props.setProperty(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, Boolean.toString(isNamespaceMapped));
        return (PhoenixConnection) DriverManager.getConnection(getUrl(),props);
    }

    private Connection getTenantConnection(String tenant) throws SQLException {
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenant);
        props.setProperty(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, Boolean.toString(isNamespaceMapped));
        return DriverManager.getConnection(getUrl(),props);
    }
    
    public ViewIndexIT(boolean isNamespaceMapped) {
        this.isNamespaceMapped = isNamespaceMapped;
    }

    @Test
    public void testDeleteViewIndexSequences() throws Exception {
        String schemaName = generateUniqueName();
        String tableName = generateUniqueName();
        String viewSchemaName = generateUniqueName();
        String fullTableName = SchemaUtil.getTableName(schemaName, tableName);
        String indexName = "IND_" + generateUniqueName();
        String viewName = "VIEW_" + generateUniqueName();
        String fullViewName = SchemaUtil.getTableName(viewSchemaName, viewName);

        createBaseTable(schemaName, tableName, false, null, null, true);
        Connection conn1 = getConnection();
        Connection conn2 = getConnection();
        conn1.createStatement().execute("CREATE VIEW " + fullViewName + " AS SELECT * FROM " + fullTableName);
        conn1.createStatement().execute("CREATE INDEX " + indexName + " ON " + fullViewName + " (v1)");
        conn2.createStatement().executeQuery("SELECT * FROM " + fullTableName).next();
        String sequenceName = getViewIndexSequenceName(PNameFactory.newName(fullTableName), null, isNamespaceMapped);
        String sequenceSchemaName = getViewIndexSequenceSchemaName(PNameFactory.newName(fullTableName), isNamespaceMapped);
        verifySequenceValue(null, sequenceName, sequenceSchemaName, Short.MIN_VALUE + 1);
        conn1.createStatement().execute("CREATE INDEX " + indexName + "_2 ON " + fullViewName + " (v1)");
        verifySequenceValue(null, sequenceName, sequenceSchemaName, Short.MIN_VALUE + 2);
        conn1.createStatement().execute("DROP VIEW " + fullViewName);
        conn1.createStatement().execute("DROP TABLE "+ fullTableName);
        
        verifySequenceNotExists(null, sequenceName, sequenceSchemaName);
    }
    
    @Test
    public void testMultiTenantViewLocalIndex() throws Exception {
        String tableName = generateUniqueName();
		String indexName = "IND_" + generateUniqueName();
        String fullTableName = SchemaUtil.getTableName(SCHEMA1, tableName);
        String fullViewName = SchemaUtil.getTableName(SCHEMA2, generateUniqueName());
        
        createBaseTable(SCHEMA1, tableName, true, null, null, true);
        Connection conn = DriverManager.getConnection(getUrl());
        PreparedStatement stmt = conn.prepareStatement(
                "UPSERT INTO " + fullTableName
                + " VALUES(?,?,?,?,?)");
        stmt.setString(1, "10");
        stmt.setString(2, "a");
        stmt.setInt(3, 1);
        stmt.setString(4, "x1");
        stmt.setInt(5, 100);
        stmt.execute();
        stmt.setString(1, "10");
        stmt.setString(2, "b");
        stmt.setInt(3, 2);
        stmt.setString(4, "x2");
        stmt.setInt(5, 200);
        stmt.execute();
        stmt.setString(1, "10");
        stmt.setString(2, "c");
        stmt.setInt(3, 3);
        stmt.setString(4, "x3");
        stmt.setInt(5, 300);
        stmt.execute();
        stmt.setString(1, "20");
        stmt.setString(2, "d");
        stmt.setInt(3, 4);
        stmt.setString(4, "x4");
        stmt.setInt(5, 400);
        stmt.execute();
        conn.commit();
        
        Properties props  = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
        props.setProperty("TenantId", "10");
        Connection conn1 = DriverManager.getConnection(getUrl(), props);
        conn1.createStatement().execute("CREATE VIEW " + fullViewName
                + " AS select * from " + fullTableName);
        conn1.createStatement().execute("CREATE LOCAL INDEX "
                + indexName + " ON "
                + fullViewName + "(v2)");
        conn1.commit();
        
        String sql = "SELECT * FROM " + fullViewName + " WHERE v2 = 100";
        ResultSet rs = conn1.prepareStatement("EXPLAIN " + sql).executeQuery();
        assertEquals(
                "CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + SchemaUtil.getPhysicalTableName(Bytes.toBytes(fullTableName), isNamespaceMapped) + " [1,'10',100]\n" +
                "    SERVER FILTER BY FIRST KEY ONLY\n" +
                "CLIENT MERGE SORT", QueryUtil.getExplainPlan(rs));
        rs = conn1.prepareStatement(sql).executeQuery();
        assertTrue(rs.next());
        assertFalse(rs.next());
        
        TestUtil.analyzeTable(conn, fullTableName);
        List<KeyRange> guideposts = TestUtil.getAllSplits(conn, fullTableName);
        assertEquals(1, guideposts.size());
        assertEquals(KeyRange.EVERYTHING_RANGE, guideposts.get(0));
        
        conn.createStatement().execute("ALTER TABLE " + fullTableName + " SET " + PhoenixDatabaseMetaData.GUIDE_POSTS_WIDTH + "=20");
        
        TestUtil.analyzeTable(conn, fullTableName);
        guideposts = TestUtil.getAllSplits(conn, fullTableName);
        assertEquals(5, guideposts.size());

        // Confirm that when view index used, the GUIDE_POSTS_WIDTH from the data physical table
        // was used
        sql = "SELECT * FROM " + fullViewName + " WHERE v2 >= 100";
        rs = conn1.prepareStatement("EXPLAIN " + sql).executeQuery();
        stmt = conn1.prepareStatement(sql);
        stmt.executeQuery();
        QueryPlan plan = stmt.unwrap(PhoenixStatement.class).getQueryPlan();
        assertEquals(4, plan.getSplits().size());
    }

    @Test
    public void testCoprocsOnGlobalMTImmutableViewIndex() throws Exception {
        testCoprocsOnGlobalViewIndexHelper(true, false);
    }

    @Test
    public void testCoprocsOnGlobalNonMTMutableViewIndex() throws Exception {
        testCoprocsOnGlobalViewIndexHelper(false, true);
    }

    @Test
    public void testCoprocsOnGlobalMTMutableViewIndex() throws Exception {
        testCoprocsOnGlobalViewIndexHelper(true, true);
    }

    @Test
    public void testCoprocsOnGlobalNonMTImmutableViewIndex() throws Exception {
        testCoprocsOnGlobalViewIndexHelper(false, false);
    }

    @Test
    public void testDroppingColumnWhileCreatingIndex() throws Exception {
        String schemaName = "S1";
        String tableName = generateUniqueName();
        String viewSchemaName = "S1";
        String fullTableName = SchemaUtil.getTableName(schemaName, tableName);
        String indexName = "IND_" + generateUniqueName();
        String viewName = "VIEW_" + generateUniqueName();
        String fullViewName = SchemaUtil.getTableName(viewSchemaName, viewName);

        createBaseTable(schemaName, tableName, false, null, null, true);
        try (Connection conn = getConnection()) {
            conn.setAutoCommit(true);
            conn.createStatement().execute("CREATE VIEW " + fullViewName + " AS SELECT * FROM " + fullTableName);
            conn.commit();

            final AtomicInteger exceptionCode = new AtomicInteger();
            final CountDownLatch doneSignal = new CountDownLatch(2);
            Runnable r1 = new Runnable() {

                @Override public void run() {
                    try {
                        conn.createStatement().execute("CREATE INDEX " + indexName + " ON " + fullViewName + " (v1)");
                    } catch (SQLException e) {
                        exceptionCode.set(e.getErrorCode());
                        throw new RuntimeException(e);
                    } finally {
                        doneSignal.countDown();
                    }
                }

            };
            Runnable r2 = new Runnable() {

                @Override public void run() {
                    try {
                        conn.createStatement().execute("ALTER TABLE " + fullTableName + " DROP COLUMN v1");
                    } catch (SQLException e) {
                        exceptionCode.set(e.getErrorCode());
                        throw new RuntimeException(e);
                    } finally {
                        doneSignal.countDown();
                    }
                }

            };
            Thread t1 = new Thread(r1);
            t1.start();
            Thread t2 = new Thread(r2);
            t2.start();

            t1.join();
            t2.join();
            doneSignal.await(60, TimeUnit.SECONDS);
            assertEquals(exceptionCode.get(), 301);
        }
    }

    private void testCoprocsOnGlobalViewIndexHelper(boolean multiTenant, boolean mutable) throws SQLException, IOException {
        String schemaName = generateUniqueName();
        String baseTable =  generateUniqueName();
        String globalView = generateUniqueName();
        String globalViewIdx =  generateUniqueName();
        createBaseTable(schemaName, baseTable, multiTenant, null, null, mutable);
        try (PhoenixConnection conn = getConnection()) {
            createView(conn, schemaName, globalView, baseTable);
            createViewIndex(conn, schemaName, globalViewIdx, globalView, "K1");
            //now check that the right coprocs are installed
            Admin admin = conn.getQueryServices().getAdmin();
            TableDescriptor td = admin.getTableDescriptor(TableName.valueOf(
                MetaDataUtil.getViewIndexPhysicalName(SchemaUtil.getPhysicalHBaseTableName(
                    schemaName, baseTable, isNamespaceMapped).getString())));
            assertTrue(td.hasCoprocessor(GlobalIndexChecker.class.getName()));
            assertFalse(td.hasCoprocessor(IndexRegionObserver.class.getName()));
            assertFalse(td.hasCoprocessor(Indexer.class.getName()));
        }
    }

    @Test
    public void testMultiTenantViewGlobalIndex() throws Exception {
        String baseTable =  SchemaUtil.getTableName(SCHEMA1, generateUniqueName());
        String globalViewName = generateUniqueName();
        String fullGlobalViewName = SchemaUtil.getTableName(SCHEMA2, globalViewName);
        String globalViewIdx =  generateUniqueName();
        String tenantView =  generateUniqueName();
        String fullIndexName = SchemaUtil.getTableName(SCHEMA2, globalViewIdx);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute("CREATE TABLE " + baseTable + " (TENANT_ID CHAR(15) NOT NULL, PK2 DATE NOT NULL, PK3 INTEGER NOT NULL, KV1 VARCHAR, KV2 VARCHAR, KV3 CHAR(15) CONSTRAINT PK PRIMARY KEY(TENANT_ID, PK2, PK3)) MULTI_TENANT=true");
            conn.createStatement().execute("CREATE VIEW " + fullGlobalViewName + " AS SELECT * FROM " + baseTable);
            conn.createStatement().execute("CREATE INDEX " + globalViewIdx + " ON " + fullGlobalViewName + " (PK3 DESC, KV3) INCLUDE (KV1) ASYNC");

            String tenantId = "tenantId";
            Properties tenantProps = new Properties();
            tenantProps.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
            // create a tenant specific view
            try (Connection tenantConn = DriverManager.getConnection(getUrl(), tenantProps)) {
                tenantConn.createStatement().execute("CREATE VIEW " + tenantView + " AS SELECT * FROM " + fullGlobalViewName);
                PreparedStatement stmt = tenantConn.prepareStatement("UPSERT INTO  " + fullGlobalViewName + " (PK2, PK3, KV1, KV3) VALUES (?, ?, ?, ?)");
                stmt.setDate(1, new Date(100));
                stmt.setInt(2, 1);
                stmt.setString(3, "KV1");
                stmt.setString(4, "KV3");
                stmt.executeUpdate();
                stmt.setDate(1, new Date(100));
                stmt.setInt(2, 2);
                stmt.setString(3, "KV4");
                stmt.setString(4, "KV5");
                stmt.executeUpdate();
                stmt.setDate(1, new Date(100));
                stmt.setInt(2, 3);
                stmt.setString(3, "KV6");
                stmt.setString(4, "KV7");
                stmt.executeUpdate();
                stmt.setDate(1, new Date(100));
                stmt.setInt(2, 4);
                stmt.setString(3, "KV8");
                stmt.setString(4, "KV9");
                stmt.executeUpdate();
                stmt.setDate(1, new Date(100));
                stmt.setInt(2, 5);
                stmt.setString(3, "KV10");
                stmt.setString(4, "KV11");
                stmt.executeUpdate();
                tenantConn.commit();
            }

            // run the MR job
            IndexToolIT.runIndexTool(true, false, SCHEMA2, globalViewName, globalViewIdx);
            try (Connection tenantConn = DriverManager.getConnection(getUrl(), tenantProps)) {
                // Verify that query uses the global view index works while querying the tenant view
                PreparedStatement stmt = tenantConn.prepareStatement("SELECT KV1 FROM  " + tenantView + " WHERE PK3 = ? AND KV3 = ?");
                stmt.setInt(1, 1);
                stmt.setString(2, "KV3");
                ResultSet rs = stmt.executeQuery();
                QueryPlan plan = stmt.unwrap(PhoenixStatement.class).getQueryPlan();
                assertEquals(fullIndexName, plan.getTableRef().getTable().getName().getString());
                assertTrue(rs.next());
                assertEquals("KV1", rs.getString(1));
                assertFalse(rs.next());
            }

            // Verify that query against the global view index works
            PreparedStatement stmt = conn.prepareStatement("SELECT KV1 FROM  " + fullGlobalViewName + " WHERE PK3 = ? AND KV3 = ?");
            stmt.setInt(1, 1);
            stmt.setString(2, "KV3");
            ResultSet rs = stmt.executeQuery();
            QueryPlan plan = stmt.unwrap(PhoenixStatement.class).getQueryPlan();
            assertEquals(fullIndexName, plan.getTableRef().getTable().getName().getString());
            assertTrue(rs.next());
            assertEquals("KV1", rs.getString(1));
            assertFalse(rs.next());
            
            TestUtil.analyzeTable(conn, baseTable);
            List<KeyRange> guideposts = TestUtil.getAllSplits(conn, baseTable);
            assertEquals(1, guideposts.size());
            assertEquals(KeyRange.EVERYTHING_RANGE, guideposts.get(0));
            
            conn.createStatement().execute("ALTER TABLE " + baseTable + " SET " + PhoenixDatabaseMetaData.GUIDE_POSTS_WIDTH + "=20");
            
            TestUtil.analyzeTable(conn, baseTable);
            guideposts = TestUtil.getAllSplits(conn, baseTable);
            assertEquals(6, guideposts.size());

            // Confirm that when view index used, the GUIDE_POSTS_WIDTH from the data physical table
            // was used
            stmt = conn.prepareStatement("SELECT KV1 FROM  " + fullGlobalViewName + " WHERE PK3 = ? AND KV3 >= ?");
            stmt.setInt(1, 1);
            stmt.setString(2, "KV3");
            rs = stmt.executeQuery();
            plan = stmt.unwrap(PhoenixStatement.class).getQueryPlan();
            assertEquals(fullIndexName, plan.getTableRef().getTable().getName().getString());
            assertEquals(6, plan.getSplits().size());
        }
    }

    private void assertRowCount(Connection conn, String fullTableName, String fullBaseName, int expectedCount) throws SQLException {
        PhoenixStatement stmt = conn.createStatement().unwrap(PhoenixStatement.class);
        ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + fullTableName);
        assertTrue(rs.next());
        assertEquals(expectedCount, rs.getInt(1));
        // Ensure that index is being used
        rs = stmt.executeQuery("EXPLAIN SELECT COUNT(*) FROM " + fullTableName);
        if (fullBaseName != null) {
            // Uses index and finds correct number of rows
            assertTrue(QueryUtil.getExplainPlan(rs).startsWith("CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + Bytes.toString(MetaDataUtil.getViewIndexPhysicalName(Bytes.toBytes(fullBaseName))))); 
        }
        
        // Force it not to use index and still finds correct number of rows
        rs = stmt.executeQuery("SELECT /*+ NO_INDEX */ * FROM " + fullTableName);
        int count = 0;
        while (rs.next()) {
            count++;
        }
        
        assertEquals(expectedCount, count);
        // Ensure that the table, not index is being used
        assertEquals(fullTableName, stmt.getQueryPlan().getContext().getCurrentTable().getTable().getName().getString());
    }

    @Test
    public void testUpdateOnTenantViewWithGlobalView() throws Exception {
        Connection conn = getConnection();
        String baseSchemaName = generateUniqueName();
        String viewSchemaName = generateUniqueName();
        String tsViewSchemaName = generateUniqueName();
        String baseTableName = generateUniqueName();
        String baseFullName = SchemaUtil.getTableName(baseSchemaName, baseTableName);
        String viewTableName = "V_" + generateUniqueName();
        String viewFullName = SchemaUtil.getTableName(viewSchemaName, viewTableName);
        String indexName = "I_" + generateUniqueName();
        String tsViewTableName = "TSV_" + generateUniqueName();
        String tsViewFullName = SchemaUtil.getTableName(tsViewSchemaName, tsViewTableName);
        String tenantId = "tenant1";
        try {
            if (isNamespaceMapped) {
                conn.createStatement().execute("CREATE SCHEMA IF NOT EXISTS " + baseSchemaName);
            }
            conn.createStatement().execute(
                    "CREATE TABLE " + baseFullName + "(\n" + "    ORGANIZATION_ID CHAR(15) NOT NULL,\n"
                            + "    KEY_PREFIX CHAR(3) NOT NULL,\n" + "    CREATED_DATE DATE,\n"
                            + "    CREATED_BY CHAR(15),\n" + "    CONSTRAINT PK PRIMARY KEY (\n"
                            + "        ORGANIZATION_ID,\n" + "        KEY_PREFIX\n" + "    )\n"
                            + ") VERSIONS=1, IMMUTABLE_ROWS=true, MULTI_TENANT=true");
            conn.createStatement().execute(
                    "CREATE VIEW " + viewFullName + " (\n" + 
                            "INT1 BIGINT NOT NULL,\n" + 
                            "DOUBLE1 DECIMAL(12, 3),\n" +
                            "IS_BOOLEAN BOOLEAN,\n" + 
                            "TEXT1 VARCHAR,\n" + "CONSTRAINT PKVIEW PRIMARY KEY\n" + "(\n" +
                            "INT1\n" + ")) AS SELECT * FROM " + baseFullName + " WHERE KEY_PREFIX = '123'");
            conn.createStatement().execute(
                    "CREATE INDEX " + indexName + " \n" + "ON " + viewFullName + " (TEXT1 DESC, INT1)\n"
                            + "INCLUDE (CREATED_BY, DOUBLE1, IS_BOOLEAN, CREATED_DATE)");
            Properties tsProps = PropertiesUtil.deepCopy(TEST_PROPERTIES);
            tsProps.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
            Connection tsConn = DriverManager.getConnection(getUrl(), tsProps);
            tsConn.createStatement().execute("CREATE VIEW " + tsViewFullName + " AS SELECT * FROM " + viewFullName);
            tsConn.createStatement().execute("UPSERT INTO " + tsViewFullName + "(INT1,DOUBLE1,IS_BOOLEAN,TEXT1) VALUES (1,1.0, true, 'a')");
            tsConn.createStatement().execute("UPSERT INTO " + tsViewFullName + "(INT1,DOUBLE1,IS_BOOLEAN,TEXT1) VALUES (2,2.0, true, 'b')");
            tsConn.createStatement().execute("UPSERT INTO " + tsViewFullName + "(INT1,DOUBLE1,IS_BOOLEAN,TEXT1) VALUES (3,3.0, true, 'c')");
            tsConn.createStatement().execute("UPSERT INTO " + tsViewFullName + "(INT1,DOUBLE1,IS_BOOLEAN,TEXT1) VALUES (4,4.0, true, 'd')");
            tsConn.createStatement().execute("UPSERT INTO " + tsViewFullName + "(INT1,DOUBLE1,IS_BOOLEAN,TEXT1) VALUES (5,5.0, true, 'e')");
            tsConn.createStatement().execute("UPSERT INTO " + tsViewFullName + "(INT1,DOUBLE1,IS_BOOLEAN,TEXT1) VALUES (6,6.0, true, 'f')");
            tsConn.createStatement().execute("UPSERT INTO " + tsViewFullName + "(INT1,DOUBLE1,IS_BOOLEAN,TEXT1) VALUES (7,7.0, true, 'g')");
            tsConn.createStatement().execute("UPSERT INTO " + tsViewFullName + "(INT1,DOUBLE1,IS_BOOLEAN,TEXT1) VALUES (8,8.0, true, 'h')");
            tsConn.createStatement().execute("UPSERT INTO " + tsViewFullName + "(INT1,DOUBLE1,IS_BOOLEAN,TEXT1) VALUES (9,9.0, true, 'i')");
            tsConn.createStatement().execute("UPSERT INTO " + tsViewFullName + "(INT1,DOUBLE1,IS_BOOLEAN,TEXT1) VALUES (10,10.0, true, 'j')");
            tsConn.commit();
            
            String basePhysicalName = SchemaUtil.getPhysicalTableName(Bytes.toBytes(baseFullName), isNamespaceMapped).toString();
            assertRowCount(tsConn, tsViewFullName, basePhysicalName, 10);
            
            tsConn.createStatement().execute("DELETE FROM " + tsViewFullName + " WHERE TEXT1='d'");
            tsConn.commit();
            assertRowCount(tsConn, tsViewFullName, basePhysicalName, 9);

            tsConn.createStatement().execute("DELETE FROM " + tsViewFullName + " WHERE INT1=2");
            tsConn.commit();
            assertRowCount(tsConn, tsViewFullName, basePhysicalName, 8);
            
            // Use different connection for delete
            Connection tsConn2 = DriverManager.getConnection(getUrl(), tsProps);
            tsConn2.createStatement().execute("DELETE FROM " + tsViewFullName + " WHERE DOUBLE1 > 7.5 AND DOUBLE1 < 9.5");
            tsConn2.commit();
            assertRowCount(tsConn2, tsViewFullName, basePhysicalName, 6);
            
            tsConn2.createStatement().execute("DROP VIEW " + tsViewFullName);
            // Should drop view and index and remove index data
            conn.createStatement().execute("DROP VIEW " + viewFullName);
            // Deletes table data (but wouldn't update index)
            conn.setAutoCommit(true);
            conn.createStatement().execute("DELETE FROM " + baseFullName);
            Connection tsConn3 = DriverManager.getConnection(getUrl(), tsProps);
            try {
                tsConn3.createStatement().execute("SELECT * FROM " + tsViewFullName + " LIMIT 1");
                fail("Expected table not to be found");
            } catch (TableNotFoundException e) {
                
            }
            conn.createStatement().execute(
                    "CREATE VIEW " + viewFullName + " (\n" + 
                            "INT1 BIGINT NOT NULL,\n" + 
                            "DOUBLE1 DECIMAL(12, 3),\n" +
                            "IS_BOOLEAN BOOLEAN,\n" + 
                            "TEXT1 VARCHAR,\n" + "CONSTRAINT PKVIEW PRIMARY KEY\n" + "(\n" +
                            "INT1\n" + ")) AS SELECT * FROM " + baseFullName + " WHERE KEY_PREFIX = '123'");
            tsConn3.createStatement().execute("CREATE VIEW " + tsViewFullName + " AS SELECT * FROM " + viewFullName);
            conn.createStatement().execute(
                    "CREATE INDEX " + indexName + " \n" + "ON " + viewFullName + " (TEXT1 DESC, INT1)\n"
                            + "INCLUDE (CREATED_BY, DOUBLE1, IS_BOOLEAN, CREATED_DATE)");
            assertRowCount(tsConn3, tsViewFullName, basePhysicalName, 0);
            
            tsConn.close();
            tsConn2.close();
            tsConn3.close();
            
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testHintForIndexOnViewWithInclude() throws Exception {
        testHintForIndexOnView(true);
    }
    
    @Ignore("PHOENIX-4274 Hint query for index on view does not use include")
    @Test
    public void testHintForIndexOnViewWithoutInclude() throws Exception {
        testHintForIndexOnView(false);        
    }
    
    private void testHintForIndexOnView(boolean includeColumns) throws Exception {
        Properties props = new Properties();
        Connection conn1 = DriverManager.getConnection(getUrl(), props);
        conn1.setAutoCommit(true);
        String tableName = SchemaUtil.getTableName(SCHEMA1, generateUniqueName());
        String viewName = SchemaUtil.getTableName(SCHEMA2, generateUniqueName());
        String indexName = generateUniqueName();
        String fullIndexName = SchemaUtil.getTableName(SCHEMA2, indexName);
        conn1.createStatement().execute(
          "CREATE TABLE "+tableName+" (k VARCHAR PRIMARY KEY, v1 VARCHAR, v2 VARCHAR) UPDATE_CACHE_FREQUENCY=1000000");
        conn1.createStatement().execute("upsert into "+tableName+" values ('row1', 'value1', 'key1')");
        conn1.createStatement().execute(
          "CREATE VIEW "+viewName+" (v3 VARCHAR, v4 VARCHAR) AS SELECT * FROM "+tableName+" WHERE v1 = 'value1'");
        conn1.createStatement().execute("CREATE INDEX " + indexName + " ON " + viewName + "(v3)" + (includeColumns ? " INCLUDE(v4)" : ""));
        PhoenixStatement stmt = conn1.createStatement().unwrap(PhoenixStatement.class);
        ResultSet rs = stmt.executeQuery("SELECT /*+ INDEX(" + viewName + " " + fullIndexName + ") */ v1 FROM " + viewName + " WHERE v3 = 'foo' ORDER BY v4");
        assertFalse(rs.next());
        assertEquals(fullIndexName, stmt.getQueryPlan().getContext().getCurrentTable().getTable().getName().getString());
    }

    @Test
    public void testCreatingIndexOnViewBuiltOnTableWithOnlyNamedColumnFamilies() throws Exception {
        try (Connection c = getConnection(); Statement s = c.createStatement()) {
            String tableName = SchemaUtil.getTableName(SCHEMA1, generateUniqueName());
            String viewName = SchemaUtil.getTableName(SCHEMA2, generateUniqueName());
            String indexName=generateUniqueName();
            c.setAutoCommit(true);
            if (isNamespaceMapped) {
                c.createStatement().execute("CREATE SCHEMA IF NOT EXISTS " + SCHEMA1);
            }
            s.execute("CREATE TABLE " + tableName + " (COL1 VARCHAR PRIMARY KEY, CF.COL2 VARCHAR)");
            s.executeUpdate("UPSERT INTO " + tableName + " VALUES ('AAA', 'BBB')");
            s.execute("CREATE VIEW " + viewName + " AS SELECT * FROM " + tableName);
            s.execute("CREATE INDEX " + indexName + " ON " + viewName + " (CF.COL2)");

            try (ResultSet rs = s.executeQuery("SELECT * FROM " + viewName + " WHERE CF.COL2 = 'BBB'")) {
                assertTrue(rs.next());
                assertEquals("AAA", rs.getString("COL1"));
                assertEquals("BBB", rs.getString("COL2"));
            }
        }
        try (Connection c = getConnection(); Statement s = c.createStatement()) {
            String tableName = generateUniqueName();
            String viewName = generateUniqueName();
            String index1Name = generateUniqueName();
            String index2Name = generateUniqueName();

            c.setAutoCommit(true);
            s.execute("create table " + tableName + " (i1 integer primary key, c2.i2 integer, c3.i3 integer, c4.i4 integer)");
            s.execute("create view " + viewName + " as select * from " + tableName + " where c2.i2 = 1");
            s.executeUpdate("upsert into " + viewName + "(i1, c3.i3, c4.i4) VALUES (1, 1, 1)");
            s.execute("create index " + index1Name + " ON " + viewName + " (c3.i3)");
            s.execute("create index " + index2Name + " ON " + viewName + " (c3.i3) include (c4.i4)");
            s.executeUpdate("upsert into " + viewName + "(i1, c3.i3, c4.i4) VALUES (2, 2, 2)");

            try (ResultSet rs = s.executeQuery("select * from " + viewName + " WHERE c3.i3 = 1")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt("i1"));
                assertEquals(1, rs.getInt("i2"));
                assertEquals(1, rs.getInt("i3"));
                assertEquals(1, rs.getInt("i4"));
            }
        }
    }

    @Test
    public void testGlobalAndTenantViewIndexesHaveDifferentIndexIds() throws Exception {
        String tableName = "T_" + generateUniqueName();
        String globalViewName = "V_" + generateUniqueName();
        String tenantViewName = "TV_" + generateUniqueName();
        String globalViewIndexName = "GV_" + generateUniqueName();
        String tenantViewIndexName = "TV_" + generateUniqueName();
        Connection globalConn = getConnection();
        Connection tenantConn = getTenantConnection(TENANT1);
        createBaseTable(SCHEMA1, tableName, true, 0, null, true);
        createView(globalConn, SCHEMA1, globalViewName, tableName);
        createViewIndex(globalConn, SCHEMA1, globalViewIndexName, globalViewName, "v1");
        createView(tenantConn, SCHEMA1, tenantViewName, tableName);
        createViewIndex(tenantConn, SCHEMA1, tenantViewIndexName, tenantViewName, "v2");

        PTable globalViewIndexTable = PhoenixRuntime.getTable(globalConn, SCHEMA1 + "." + globalViewIndexName);
        PTable tenantViewIndexTable = PhoenixRuntime.getTable(tenantConn, SCHEMA1 + "." + tenantViewIndexName);
        Assert.assertNotNull(globalViewIndexTable);
        Assert.assertNotNull(tenantViewIndexName);
        Assert.assertNotEquals(globalViewIndexTable.getViewIndexId(), tenantViewIndexTable.getViewIndexId());
        globalConn.createStatement().execute("UPSERT INTO " + SchemaUtil.getTableName(SCHEMA1, globalViewName) + " (T_ID, K1, K2) VALUES ('GLOBAL', 'k1', 100)");
        tenantConn.createStatement().execute("UPSERT INTO " + SchemaUtil.getTableName(SCHEMA1, tenantViewName) + " (T_ID, K1, K2) VALUES ('TENANT', 'k1', 101)");

        assertEquals(1, getRowCountOfView(globalConn, SCHEMA1, globalViewIndexName));
        assertEquals(1, getRowCountOfView(tenantConn, SCHEMA1, tenantViewName));
    }

    private int getRowCountOfView(Connection conn, String schemaName, String viewName) throws SQLException {
      int size = 0;
      ResultSet rs = conn.createStatement().executeQuery("SELECT COUNT(*) FROM " + SchemaUtil.getTableName(schemaName, viewName));
      while (rs.next()){
        size++;
      }
      return size;
    }

    @Test
    public void testIndexIdDataTypeDefaultValue() throws Exception {
        String tableName = "T_" + generateUniqueName();
        String globalViewName = "V_" + generateUniqueName();
        String globalViewIndexName = "GV_" + generateUniqueName();
        try (Connection globalConn = getConnection()) {
            createBaseTable(SCHEMA1, tableName, true, 0, null, true);
            createView(globalConn, SCHEMA1, globalViewName, tableName);
            createViewIndex(globalConn, SCHEMA1, globalViewIndexName, globalViewName, "v1");

            String sql = "SELECT " + VIEW_INDEX_ID_DATA_TYPE + " FROM " + SYSTEM_CATALOG_NAME + " WHERE " +
                    TABLE_SCHEM + " = '%s' AND " +
                    TABLE_NAME + " = '%s' AND " +
                    COLUMN_COUNT + " IS NOT NULL";
            // should not have default value for table
            ResultSet rs = globalConn.createStatement().executeQuery(String.format(sql, SCHEMA1, tableName));
            if (rs.next()) {
                assertNull(rs.getObject(1));
            } else {
                fail();
            }
            // should not have default value for view
            rs = globalConn.createStatement().executeQuery(String.format(sql, SCHEMA1, globalViewName));
            if (rs.next()) {
                assertNull(rs.getObject(1));
            } else {
                fail();
            }
            // should have default value
            rs = globalConn.createStatement().executeQuery(String.format(sql, SCHEMA1, globalViewIndexName));
            if (rs.next()) {
            /*
                quote from hbase-site.xml so default value is BIGINT
                We have some hardcoded viewIndex ids in the IT tests which assumes viewIndexId is of type Long.
                However the default viewIndexId type is set to "short" by default until we upgrade all clients to
                 support  long viewIndex ids.
             */
                assertEquals(Types.BIGINT, rs.getInt(1));
            } else {
                fail();
            }
        }
    }
}
