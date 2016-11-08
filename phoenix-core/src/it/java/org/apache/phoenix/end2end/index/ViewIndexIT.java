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

import static org.apache.phoenix.util.MetaDataUtil.getViewIndexSequenceName;
import static org.apache.phoenix.util.MetaDataUtil.getViewIndexSequenceSchemaName;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.PNameFactory;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class ViewIndexIT extends ParallelStatsDisabledIT {
    private boolean isNamespaceMapped;

    @Parameters(name = "ViewIndexIT_isNamespaceMapped={0}") // name is used by failsafe as file name in reports
    public static Collection<Boolean> data() {
        return Arrays.asList(true, false);
    }

    private void createBaseTable(String schemaName, String tableName, boolean multiTenant, Integer saltBuckets, String splits)
            throws SQLException {
        Connection conn = getConnection();
        if (isNamespaceMapped) {
            conn.createStatement().execute("CREATE SCHEMA IF NOT EXISTS " + schemaName);
        }
        String ddl = "CREATE TABLE " + SchemaUtil.getTableName(schemaName, tableName) + " (t_id VARCHAR NOT NULL,\n" +
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
    
    private Connection getConnection() throws SQLException{
        Properties props = new Properties();
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
        String fullTableName = SchemaUtil.getTableName(schemaName, tableName);
        String indexName = "IND_" + generateUniqueName();
        String viewName = "VIEW_" + generateUniqueName();
        String fullViewName = SchemaUtil.getTableName(schemaName, viewName);

        createBaseTable(schemaName, tableName, false, null, null);
        Connection conn1 = getConnection();
        Connection conn2 = getConnection();
        conn1.createStatement().execute("CREATE VIEW " + fullViewName + " AS SELECT * FROM " + fullTableName);
        conn1.createStatement().execute("CREATE INDEX " + indexName + " ON " + fullViewName + " (v1)");
        conn2.createStatement().executeQuery("SELECT * FROM " + fullTableName).next();
        String sequenceName = getViewIndexSequenceName(PNameFactory.newName(fullTableName), null, isNamespaceMapped);
        String sequenceSchemaName = getViewIndexSequenceSchemaName(PNameFactory.newName(fullTableName), isNamespaceMapped);
        String seqName = getViewIndexSequenceName(PNameFactory.newName(fullTableName), null, !isNamespaceMapped);
        String seqSchemaName = getViewIndexSequenceSchemaName(PNameFactory.newName(fullTableName), !isNamespaceMapped);
        verifySequenceValue(null, sequenceName, sequenceSchemaName, -32767);
        verifySequenceValue(null, sequenceName, sequenceSchemaName, -32767);
        conn1.createStatement().execute("CREATE INDEX " + indexName + "_2 ON " + fullViewName + " (v1)");
        verifySequenceValue(null, sequenceName, sequenceSchemaName, -32766);
        // Check other format of sequence is not there as Sequences format is different for views/indexes created on
        // table which are namespace mapped and which are not.
        verifySequenceNotExists(null, seqName, seqSchemaName);
        conn1.createStatement().execute("DROP VIEW " + fullViewName);
        conn1.createStatement().execute("DROP TABLE "+ fullTableName);
        
        verifySequenceNotExists(null, sequenceName, sequenceSchemaName);
    }
    
    @Test
    public void testMultiTenantViewLocalIndex() throws Exception {
        String schemaName = generateUniqueName();
        String tableName =  generateUniqueName();
        String indexName = "IND_" + generateUniqueName();
        String viewName = "VIEW_" + generateUniqueName();
        String fullTableName = SchemaUtil.getTableName(schemaName, tableName);
        
        createBaseTable(schemaName, tableName, true, null, null);
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
        conn1.createStatement().execute("CREATE VIEW " + viewName
                + " AS select * from " + fullTableName);
        conn1.createStatement().execute("CREATE LOCAL INDEX "
                + indexName + " ON "
                + viewName + "(v2)");
        conn1.commit();
        
        String sql = "SELECT * FROM " + viewName + " WHERE v2 = 100";
        ResultSet rs = conn1.prepareStatement("EXPLAIN " + sql).executeQuery();
        assertEquals(
                "CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + SchemaUtil.getPhysicalHBaseTableName(fullTableName, isNamespaceMapped, PTableType.TABLE) + " [1,'10',100]\n" +
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
        sql = "SELECT * FROM " + viewName + " WHERE v2 >= 100";
        stmt = conn1.prepareStatement(sql);
        stmt.executeQuery();
        QueryPlan plan = stmt.unwrap(PhoenixStatement.class).getQueryPlan();
        assertEquals(4, plan.getSplits().size());
    }
    
    
    @Test
    public void testCreatingIndexOnGlobalView() throws Exception {
        String baseTable =  generateUniqueName();
        String globalView = generateUniqueName();
        String globalViewIdx =  generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute("CREATE TABLE " + baseTable + " (TENANT_ID CHAR(15) NOT NULL, PK2 DATE NOT NULL, PK3 INTEGER NOT NULL, KV1 VARCHAR, KV2 VARCHAR, KV3 CHAR(15) CONSTRAINT PK PRIMARY KEY(TENANT_ID, PK2 ROW_TIMESTAMP, PK3)) MULTI_TENANT=true");
            conn.createStatement().execute("CREATE VIEW " + globalView + " AS SELECT * FROM " + baseTable);
            conn.createStatement().execute("CREATE INDEX " + globalViewIdx + " ON " + globalView + " (PK3 DESC, KV3) INCLUDE (KV1)");
            PreparedStatement stmt = conn.prepareStatement("UPSERT INTO  " + globalView + " (TENANT_ID, PK2, PK3, KV1, KV3) VALUES (?, ?, ?, ?, ?)");
            stmt.setString(1, "tenantId");
            stmt.setDate(2, new Date(100));
            stmt.setInt(3, 1);
            stmt.setString(4, "KV1");
            stmt.setString(5, "KV3");
            stmt.executeUpdate();
            stmt.setString(1, "tenantId");
            stmt.setDate(2, new Date(100));
            stmt.setInt(3, 2);
            stmt.setString(4, "KV4");
            stmt.setString(5, "KV5");
            stmt.executeUpdate();
            stmt.setString(1, "tenantId");
            stmt.setDate(2, new Date(100));
            stmt.setInt(3, 3);
            stmt.setString(4, "KV6");
            stmt.setString(5, "KV7");
            stmt.executeUpdate();
            stmt.setString(1, "tenantId");
            stmt.setDate(2, new Date(100));
            stmt.setInt(3, 4);
            stmt.setString(4, "KV8");
            stmt.setString(5, "KV9");
            stmt.executeUpdate();
            stmt.setString(1, "tenantId");
            stmt.setDate(2, new Date(100));
            stmt.setInt(3, 5);
            stmt.setString(4, "KV10");
            stmt.setString(5, "KV11");
            stmt.executeUpdate();
            conn.commit();
            
            // Verify that query against the global view index works
            stmt = conn.prepareStatement("SELECT KV1 FROM  " + globalView + " WHERE PK3 = ? AND KV3 = ?");
            stmt.setInt(1, 1);
            stmt.setString(2, "KV3");
            ResultSet rs = stmt.executeQuery();
            QueryPlan plan = stmt.unwrap(PhoenixStatement.class).getQueryPlan();
            assertTrue(plan.getTableRef().getTable().getName().getString().equals(globalViewIdx));
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
            stmt = conn.prepareStatement("SELECT KV1 FROM  " + globalView + " WHERE PK3 = ? AND KV3 >= ?");
            stmt.setInt(1, 1);
            stmt.setString(2, "KV3");
            rs = stmt.executeQuery();
            plan = stmt.unwrap(PhoenixStatement.class).getQueryPlan();
            assertTrue(plan.getTableRef().getTable().getName().getString().equals(globalViewIdx));
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
        String baseSchemaName = "PLATFORM_ENTITY";
        String baseTableName = generateUniqueName();
        String baseFullName = SchemaUtil.getTableName(baseSchemaName, baseTableName);
        String viewTableName = "V_" + generateUniqueName();
        String viewFullName = SchemaUtil.getTableName(baseSchemaName, viewTableName);
        String indexName = "I_" + generateUniqueName();
        String tsViewTableName = "TSV_" + generateUniqueName();
        String tsViewFullName = SchemaUtil.getTableName(baseSchemaName, tsViewTableName);
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
            tsProps.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, "123451234512345");
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
            
            String basePhysicalName = SchemaUtil.getPhysicalHBaseTableName(baseFullName, isNamespaceMapped, PTableType.TABLE).getString();
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
}