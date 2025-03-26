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

import static org.apache.phoenix.end2end.ExplainPlanWithStatsEnabledIT.getByteRowEstimates;
import static org.apache.phoenix.util.MetaDataUtil.getViewIndexSequenceName;
import static org.apache.phoenix.util.MetaDataUtil.getViewIndexSequenceSchemaName;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.compile.ExplainPlan;
import org.apache.phoenix.compile.ExplainPlanAttributes;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.end2end.ExplainPlanWithStatsEnabledIT.Estimate;
import org.apache.phoenix.hbase.index.IndexRegionSplitPolicy;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.jdbc.PhoenixResultSet;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PNameFactory;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTable.IndexType;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.Test;

import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;

public class LocalIndexIT extends BaseLocalIndexIT {
    public LocalIndexIT(boolean isNamespaceMapped) {
        super(isNamespaceMapped);
    }

    @Test
    public void testPhoenixRowTimestamp() throws Exception {
        String tableName = generateUniqueName();
        String indexName = "IDX_" + generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            Timestamp initial = new Timestamp(EnvironmentEdgeManager.currentTimeMillis() - 1);
            conn.createStatement().execute("create table " + tableName +
                    " (id varchar(10) not null primary key, val1 varchar(10), val2 varchar(10), val3 varchar(10))");
            conn.createStatement().execute("upsert into " + tableName + " values ('a', 'ab', 'abc', 'abcd')");
            conn.commit();
            Timestamp before = new Timestamp(EnvironmentEdgeManager.currentTimeMillis());
            // Sleep 1ms to get a different row timestamps
            Thread.sleep(1);
            conn.createStatement().execute("upsert into " + tableName + " values ('b', 'bc', 'bcd', 'bcde')");
            conn.commit();
            Timestamp after = new Timestamp(EnvironmentEdgeManager.currentTimeMillis() + 1);
            conn.createStatement().execute("CREATE LOCAL INDEX " + indexName + " on " +
                    tableName + " (val1, PHOENIX_ROW_TIMESTAMP()) ");

            String timeZoneID = Calendar.getInstance().getTimeZone().getID();
            // Write a query to get the row with val2 = 'bc' with a time range query
            String query = "SELECT  val1, val2, PHOENIX_ROW_TIMESTAMP() from " + tableName + " WHERE val1 = 'bc' AND " +
                    "PHOENIX_ROW_TIMESTAMP() > TO_DATE('" + before.toString() + "','yyyy-MM-dd HH:mm:ss.SSS', '" + timeZoneID + "') AND " +
                    "PHOENIX_ROW_TIMESTAMP() < TO_DATE('" + after.toString() + "','yyyy-MM-dd HH:mm:ss.SSS', '" + timeZoneID + "')";
            ResultSet rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("bc", rs.getString(1));
            assertEquals("bcd", rs.getString(2));
            assertTrue(rs.getTimestamp(3).after(before));
            assertTrue(rs.getTimestamp(3).before(after));
            assertFalse(rs.next());
            // Count the number of index rows
            rs = conn.createStatement().executeQuery("SELECT COUNT(*) from " + indexName);
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            // Add one more row with val2 ='bc' and check this does not change the result of the previous
            // query
            // Sleep 1ms to get a different row timestamps
            Thread.sleep(1);
            conn.createStatement().execute("upsert into " + tableName + " values ('c', 'bc', 'ccc', 'cccc')");
            conn.commit();
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("bc", rs.getString(1));
            assertEquals("bcd", rs.getString(2));
            assertTrue(rs.getTimestamp(3).after(before));
            assertTrue(rs.getTimestamp(3).before(after));
            assertFalse(rs.next());
            // Write a time range query to get the last row with val2 ='bc'
            query = "SELECT  val1, val2, PHOENIX_ROW_TIMESTAMP() from " + tableName + " WHERE val1 = 'bc' AND " +
                    "PHOENIX_ROW_TIMESTAMP() > TO_DATE('" + after.toString() + "','yyyy-MM-dd HH:mm:ss.SSS', '" + timeZoneID + "')";
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("bc", rs.getString(1));
            assertEquals("ccc", rs.getString(2));
            assertTrue(rs.getTimestamp(3).after(after));
            assertFalse(rs.next());
            // Add a new index where the index row key starts with PHOENIX_ROW_TIMESTAMP()
            indexName = generateUniqueName();
            conn.createStatement().execute("CREATE LOCAL INDEX " + indexName + " on " +
                    tableName + " (PHOENIX_ROW_TIMESTAMP()) " + "include (val1, val2, val3) ");
            // Add one more row
            // Sleep 1ms to get a different row timestamps
            Thread.sleep(1);
            conn.createStatement().execute("upsert into " + tableName + " values ('d', 'ad', 'def', 'defg')");
            conn.commit();
            // Write a query to get all the rows in the order of their timestamps
            query = "SELECT  val1, val2, PHOENIX_ROW_TIMESTAMP() from " + tableName + " WHERE " +
                    "PHOENIX_ROW_TIMESTAMP() > TO_DATE('" + initial.toString() + "','yyyy-MM-dd HH:mm:ss.SSS', '" + timeZoneID + "')";
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("ab", rs.getString(1));
            assertEquals("abc", rs.getString(2));
            assertTrue(rs.next());
            assertEquals("bc", rs.getString(1));
            assertEquals("bcd", rs.getString(2));
            assertTrue(rs.next());
            assertEquals("bc", rs.getString(1));
            assertEquals("ccc", rs.getString(2));
            assertTrue(rs.next());
            assertEquals("ad", rs.getString(1));
            assertEquals("def", rs.getString(2));
            assertFalse(rs.next());
        }
    }
    @Test
    public void testSelectFromIndexWithUncoveredArrayIndex() throws Exception {
        if (isNamespaceMapped) {
            return;
        }
        String tableName = schemaName + "." + generateUniqueName();
        String indexName = "IDX_" + generateUniqueName();

        Connection conn = getConnection();
        conn.setAutoCommit(true);

        conn.createStatement().execute("CREATE TABLE " + tableName + " (pk INTEGER PRIMARY KEY, v1 FLOAT, v2 FLOAT[])");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES(1, 2, ARRAY[3,4])");

        conn.createStatement().execute("CREATE LOCAL INDEX " + indexName + " ON " + tableName + "(v1)");

        ResultSet rs = conn.createStatement().executeQuery("SELECT v2[1] FROM "+tableName+" WHERE v1 < 3");
        rs.next();
        assertEquals(3, rs.getInt(1));
        rs.close();

        conn.createStatement().execute("DROP INDEX " + indexName + " ON " + tableName);
        conn.createStatement().execute("CREATE LOCAL INDEX " + indexName + " ON " + tableName + "(v2[2])");

        rs = conn.createStatement().executeQuery("SELECT v2[1] FROM "+tableName+" WHERE v2[2] < 5");
        rs.next();
        assertEquals(3, rs.getInt(1));
        rs.close();
    }

    @Test
    public void testSelectFromIndexWithAdditionalWhereClause() throws Exception {
        if (isNamespaceMapped) {
            return;
        }
        String tableName = schemaName + "." + generateUniqueName();
        String indexName = "IDX_" + generateUniqueName();

        Connection conn = getConnection();
        conn.setAutoCommit(true);

        conn.createStatement().execute("CREATE TABLE " + tableName + " (pk INTEGER PRIMARY KEY, v1 FLOAT, v2 FLOAT, v3 INTEGER)");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES(1, 2, 3, 4)");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES(2, 3, 4, 5)");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES(3, 4, 5, 6)");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES(4, 5, 6, 7)");

        conn.createStatement().execute("CREATE LOCAL INDEX " + indexName + " ON " + tableName + "(v1)");
        testExtraWhere(conn, tableName);

        conn.createStatement().execute("DROP INDEX " + indexName + " ON " + tableName);
        conn.createStatement().execute("CREATE LOCAL INDEX " + indexName + " ON " + tableName + "(v1) INCLUDE (v3)");
        testExtraWhere(conn, tableName);

        conn.createStatement().execute("DROP INDEX " + indexName + " ON " + tableName);
        conn.createStatement().execute("CREATE LOCAL INDEX " + indexName + " ON " + tableName + "(v1) INCLUDE (v2)");
        testExtraWhere(conn, tableName);

        conn.createStatement().execute("DROP INDEX " + indexName + " ON " + tableName);
        conn.createStatement().execute("CREATE LOCAL INDEX " + indexName + " ON " + tableName + "(v1) INCLUDE (v2,v3)");
        testExtraWhere(conn, tableName);
    }

    private void testExtraWhere(Connection conn, String tableName) throws SQLException {
        ResultSet rs = conn.createStatement().executeQuery("SELECT COUNT(*) FROM "+tableName+" WHERE v1 < 3 AND v2 < 4");
        rs.next();
        assertEquals(1, rs.getInt(1));
        rs.close();

        rs = conn.createStatement().executeQuery("SELECT COUNT(*) FROM "+tableName+" WHERE v1 < 3 AND v3 < 5");
        rs.next();
        assertEquals(1, rs.getInt(1));
        rs.close();

        rs = conn.createStatement().executeQuery("SELECT COUNT(*) FROM "+tableName+" WHERE v1 < 10 AND v2 < 0 AND v3 < 0");
        rs.next();
        assertEquals(0, rs.getInt(1));
        rs.close();

        rs = conn.createStatement().executeQuery("SELECT COUNT(*) FROM "+tableName+" WHERE v1 <= 2 AND v2 > 0 AND v3 < 5");
        rs.next();
        assertEquals(1, rs.getInt(1));
        rs.close();

        rs = conn.createStatement().executeQuery("SELECT pk FROM "+tableName+" WHERE v1 > 3 AND v2 > 0 AND v3 > 6");
        rs.next();
        assertEquals(4, rs.getInt(1));
        rs.close();

        rs = conn.createStatement().executeQuery("SELECT v1 FROM "+tableName+" WHERE v1 > 3 AND v2 > 0 AND v3 > 6");
        rs.next();
        assertEquals(5, rs.getInt(1));
        rs.close();

        rs = conn.createStatement().executeQuery("SELECT pk FROM "+tableName+" WHERE (v1,v2) IN ((1,5),(4,5))");
        rs.next();
        assertEquals(3, rs.getInt(1));
        rs.close();

        rs = conn.createStatement().executeQuery("SELECT v3 FROM "+tableName+" WHERE (v1,v2) IN ((1,5),(4,5))");
        rs.next();
        assertEquals(6, rs.getInt(1));
        rs.close();

        rs = conn.createStatement().executeQuery("SELECT * FROM "+tableName+" WHERE v1 > 0 AND v3 > 5 LIMIT 2");
        assertTrue(rs.next());
        assertTrue(rs.next());
        assertFalse(rs.next());
        rs.close();

        rs = conn.createStatement().executeQuery("SELECT * FROM "+tableName+" WHERE v1 > 0 AND v3 > 5 LIMIT 1");
        assertTrue(rs.next());
        assertFalse(rs.next());
        rs.close();

        rs = conn.createStatement().executeQuery("SELECT * FROM "+tableName+" WHERE v3 > 5 ORDER BY v1 LIMIT 2");
        assertTrue(rs.next());
        assertTrue(rs.next());
        assertFalse(rs.next());
        rs.close();
    }

    @Test
    public void testDeleteFromLocalIndex() throws Exception {
        String tableName = schemaName + "." + generateUniqueName();
        String indexName = "IDX_" + generateUniqueName();
    
        Connection conn = getConnection();
        conn.setAutoCommit(true);
        if (isNamespaceMapped) {
            conn.createStatement().execute("CREATE SCHEMA IF NOT EXISTS " + schemaName);
        }

        conn.createStatement().execute("CREATE TABLE " + tableName + " (pk INTEGER PRIMARY KEY, v1 FLOAT, v2 FLOAT)");
        conn.createStatement().execute("CREATE LOCAL INDEX " + indexName + " ON " + tableName + "(v2)");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES(1, rand(), rand())");
        // This would fail with an NPE before PHOENIX-4933
        conn.createStatement().execute("DELETE FROM " + tableName + " WHERE v1 < 1");
        ResultSet rs = conn.createStatement().executeQuery("SELECT COUNT(*) FROM "+tableName);
        rs.next();
        assertEquals(0, rs.getInt(1));
        rs.close();
    }

    @Test
    public void testLocalIndexConsistency() throws Exception {
        if (isNamespaceMapped) {
            return;
        }
        String tableName = schemaName + "." + generateUniqueName();
        String indexName = "IDX_" + generateUniqueName();

        Connection conn = getConnection();
        conn.setAutoCommit(true);

        conn.createStatement().execute("CREATE TABLE " + tableName + " (pk INTEGER PRIMARY KEY, v1 FLOAT) SPLIT ON (4000)");
        conn.createStatement().execute("CREATE LOCAL INDEX " + indexName + " ON " + tableName + "(v1)");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES(rand() * 8000, rand())");

        for (int i=0; i<16; i++) {
            conn.createStatement().execute("UPSERT INTO " + tableName + " SELECT rand() * 8000, rand() FROM " + tableName);
            assertEquals(getCountViaIndex(conn, tableName, null), getCountViaIndex(conn, tableName, indexName));
        }
    }

    @Test
    public void testLocalIndexConsistencyWithGlobalMix() throws Exception {
        if (isNamespaceMapped) {
            return;
        }
        String tableName = schemaName + "." + generateUniqueName();
        String localIndexNames[] = {"L_" + generateUniqueName(), "L_" + generateUniqueName()};
        String globalIndexNames[] = {"G_" + generateUniqueName(), "G_" + generateUniqueName()};

        Connection conn = getConnection();
        conn.setAutoCommit(true);

        conn.createStatement().execute("CREATE TABLE " + tableName + " (pk INTEGER PRIMARY KEY, v1 FLOAT, v2 FLOAT, v3 FLOAT, v4 FLOAT) SPLIT ON (4000)");

        int idx=1;
        for (String indexName : localIndexNames) {
            conn.createStatement().execute("CREATE LOCAL INDEX " + indexName + " ON " + tableName + "(v" + idx++ +")");
        }
        for (String indexName : globalIndexNames) {
            conn.createStatement().execute("CREATE INDEX " + indexName + " ON " + tableName + "(v" + idx++ +")");
        }
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES(rand() * 8000, rand())");

        for (int i=0; i<16; i++) {
            conn.createStatement().execute("UPSERT INTO " + tableName + " SELECT rand() * 8000, rand() FROM " + tableName);

            int count = getCountViaIndex(conn, tableName, null);
            for (String indexName : localIndexNames) {
                assertEquals(count, getCountViaIndex(conn, tableName, indexName));
            }

            for (String indexName : globalIndexNames) {
                assertEquals(count, getCountViaIndex(conn, tableName, indexName));
            }
        }
    }

    private int getCountViaIndex(Connection conn, String tableName, String indexName) throws SQLException {
        String hint = indexName == null ? "NO_INDEX" : "INDEX(" + tableName + " " + indexName + ")";
        try (ResultSet rs = conn.createStatement().executeQuery("SELECT /*+ " + hint + " */ COUNT(*) FROM " + tableName)) {
            rs.next();
            return rs.getInt(1);
        }
    }

    @Test
    public void testUseUncoveredLocalIndexWithPrefix() throws Exception {
        String tableName = schemaName + "." + generateUniqueName();
        String indexName = "IDX_" + generateUniqueName();
        String fullIndexName = SchemaUtil.getTableName(schemaName, indexName);
        TableName physicalTableName = SchemaUtil.getPhysicalTableName(tableName.getBytes(), isNamespaceMapped);
        String indexPhysicalTableName = physicalTableName.getNameAsString();

        Connection conn = getConnection();
        conn.setAutoCommit(true);
        if (isNamespaceMapped) {
            conn.createStatement().execute("CREATE SCHEMA IF NOT EXISTS " + schemaName);
        }

        conn.createStatement().execute("CREATE TABLE " + tableName
                + " (pk1 INTEGER NOT NULL, pk2 INTEGER NOT NULL, pk3 INTEGER NOT NULL, v1 FLOAT, v2 FLOAT, V3 INTEGER CONSTRAINT pk PRIMARY KEY(pk1,pk2,pk3))");
        conn.createStatement().execute("CREATE LOCAL INDEX " + indexName + " ON " + tableName + "(pk1,pk2,v1,v2)");

        // 1. same prefix length, no other restrictions, but v3 is in the SELECT. Use the main table.
        ExplainPlan explainPlan = conn.prepareStatement("SELECT * FROM " + tableName + " WHERE pk1 = 3 AND pk2 = 4")
                .unwrap(PhoenixPreparedStatement.class).optimizeQuery()
                .getExplainPlan();
        ExplainPlanAttributes explainPlanAttributes =
                explainPlan.getPlanStepsAsAttributes();
        assertEquals("PARALLEL 1-WAY",
                explainPlanAttributes.getIteratorTypeAndScanSize());
        assertEquals("RANGE SCAN ",
                explainPlanAttributes.getExplainScanType());
        assertEquals(physicalTableName.toString(), explainPlanAttributes.getTableName());
        assertEquals(" [3,4]", explainPlanAttributes.getKeyRanges());

        // 2. same prefix length, no other restrictions. Only index columns used. Use the index.
        explainPlan = conn.prepareStatement("SELECT v2 FROM " + tableName + " WHERE pk1 = 3 AND pk2 = 4")
                .unwrap(PhoenixPreparedStatement.class).optimizeQuery()
                .getExplainPlan();
        explainPlanAttributes =
                explainPlan.getPlanStepsAsAttributes();
        assertEquals("PARALLEL 1-WAY",
                explainPlanAttributes.getIteratorTypeAndScanSize());
        assertEquals("RANGE SCAN ",
                explainPlanAttributes.getExplainScanType());
        assertEquals( fullIndexName+ "(" + indexPhysicalTableName + ")",
                explainPlanAttributes.getTableName());
        assertEquals(" [1,3,4]", explainPlanAttributes.getKeyRanges());
        assertEquals("SERVER FILTER BY FIRST KEY ONLY",
                explainPlanAttributes.getServerWhereFilter());
        assertEquals("CLIENT MERGE SORT",
                explainPlanAttributes.getClientSortAlgo());

        // 3. same prefix length, but there's a column not on the index
        explainPlan = conn.prepareStatement("SELECT v2 FROM " + tableName + " WHERE pk1 = 3 AND pk2 = 4 AND v3 = 1")
                .unwrap(PhoenixPreparedStatement.class).optimizeQuery()
                .getExplainPlan();
        explainPlanAttributes =
                explainPlan.getPlanStepsAsAttributes();
        assertEquals("PARALLEL 1-WAY",
                explainPlanAttributes.getIteratorTypeAndScanSize());
        assertEquals("RANGE SCAN ",
                explainPlanAttributes.getExplainScanType());
        assertEquals(physicalTableName.toString(), explainPlanAttributes.getTableName());
        assertEquals(" [3,4]", explainPlanAttributes.getKeyRanges());
        assertEquals("SERVER FILTER BY V3 = 1",
                explainPlanAttributes.getServerWhereFilter());

        // 4. Longer prefix on the index, use it.
        explainPlan = conn.prepareStatement("SELECT v2 FROM " + tableName + " WHERE pk1 = 3 AND pk2 = 4 AND v1 = 3 AND v3 = 1")
                .unwrap(PhoenixPreparedStatement.class).optimizeQuery()
                .getExplainPlan();
        explainPlanAttributes =
                explainPlan.getPlanStepsAsAttributes();
        assertEquals("PARALLEL 1-WAY",
                explainPlanAttributes.getIteratorTypeAndScanSize());
        assertEquals("RANGE SCAN ",
                explainPlanAttributes.getExplainScanType());
        assertEquals(fullIndexName+ "(" + indexPhysicalTableName + ")",
                explainPlanAttributes.getTableName());
        assertEquals(" [1,3,4,3]", explainPlanAttributes.getKeyRanges());
        assertEquals("[0.V3]", explainPlanAttributes.getServerMergeColumns().toString());
        assertEquals("SERVER FILTER BY FIRST KEY ONLY AND \"V3\" = 1",
                explainPlanAttributes.getServerWhereFilter());
        assertEquals("CLIENT MERGE SORT",
                explainPlanAttributes.getClientSortAlgo());
    }

    @Test
    public void testUseUncoveredLocalIndexWithSplitPrefix() throws Exception {
        String tableName = schemaName + "." + generateUniqueName();
        String indexName = "IDX_" + generateUniqueName();
        String fullIndexName = SchemaUtil.getTableName(schemaName, indexName);
        TableName physicalTableName = SchemaUtil.getPhysicalTableName(tableName.getBytes(), isNamespaceMapped);
        String indexPhysicalTableName = physicalTableName.getNameAsString();

        Connection conn = getConnection();
        conn.setAutoCommit(true);
        if (isNamespaceMapped) {
            conn.createStatement().execute("CREATE SCHEMA IF NOT EXISTS " + schemaName);
        }

        conn.createStatement().execute("CREATE TABLE " + tableName
                + " (pk1 INTEGER NOT NULL, pk2 INTEGER NOT NULL, pk3 INTEGER NOT NULL, v1 FLOAT, v2 FLOAT, V3 INTEGER CONSTRAINT pk PRIMARY KEY(pk1,pk2,pk3)) SALT_BUCKETS=16");
//                + " (pk1 INTEGER NOT NULL, pk2 INTEGER NOT NULL, pk3 INTEGER NOT NULL, v1 FLOAT, v2 FLOAT, V3 INTEGER CONSTRAINT pk PRIMARY KEY(pk1,pk2,pk3))");
        conn.createStatement().execute("CREATE LOCAL INDEX " + indexName + " ON " + tableName + "(pk1,pk3)");

        // 1. Still use the index
        ResultSet rs = conn.createStatement().executeQuery("EXPLAIN SELECT pk1, pk2, pk3, v1 FROM " + tableName + " WHERE pk1 = 2 AND pk3 = 3");
        assertEquals(
            "CLIENT PARALLEL 16-WAY RANGE SCAN OVER "
                    + fullIndexName +  "(" + indexPhysicalTableName + ") [1,2,3]\n"
                    + "    SERVER MERGE [0.V1]\n"
                    + "    SERVER FILTER BY FIRST KEY ONLY\n"
                    + "CLIENT MERGE SORT",
                    QueryUtil.getExplainPlan(rs));
        rs.close();
    }

    @Test
    public void testUseUncoveredLocalIndex() throws Exception {
        String tableName = schemaName + "." + generateUniqueName();
        String indexName = "IDX_" + generateUniqueName();
        String fullIndexName = SchemaUtil.getTableName(schemaName, indexName);
        TableName physicalTableName = SchemaUtil.getPhysicalTableName(tableName.getBytes(), isNamespaceMapped);
        String indexPhysicalTableName = physicalTableName.getNameAsString();

        Connection conn = getConnection();
        conn.setAutoCommit(true);
        if (isNamespaceMapped) {
            conn.createStatement().execute("CREATE SCHEMA IF NOT EXISTS " + schemaName);
        }

        conn.createStatement().execute("CREATE TABLE " + tableName
                + " (pk INTEGER PRIMARY KEY, v1 FLOAT, v2 FLOAT, V3 INTEGER, V4 INTEGER)");
        conn.createStatement().execute("CREATE LOCAL INDEX " + indexName + " ON " + tableName + "(v2, v3, v4)");

        // 1. COUNT(*) should still use the index - fewer bytes to scan
        ResultSet rs = conn.createStatement().executeQuery("EXPLAIN SELECT COUNT(*) FROM " + tableName);
        assertEquals(
            "CLIENT PARALLEL 1-WAY RANGE SCAN OVER "
                    + fullIndexName + "(" + indexPhysicalTableName + ") [1]\n"
                            + "    SERVER FILTER BY FIRST KEY ONLY\n"
                            + "    SERVER AGGREGATE INTO SINGLE ROW",
                    QueryUtil.getExplainPlan(rs));
        rs.close();

        // 2. All column projected, no filtering by indexed column, not using the index
        rs = conn.createStatement().executeQuery("EXPLAIN SELECT * FROM " + tableName);
        assertEquals(
            "CLIENT PARALLEL 1-WAY FULL SCAN OVER " + physicalTableName,
                    QueryUtil.getExplainPlan(rs));
        rs.close();

        // 3. if the index can avoid a sort operation, use it
        rs = conn.createStatement().executeQuery("EXPLAIN SELECT * FROM " + tableName + " ORDER BY v2");
        assertEquals(
            "CLIENT PARALLEL 1-WAY RANGE SCAN OVER "
                    + fullIndexName + "(" + indexPhysicalTableName + ") [1]\n"
                            + "    SERVER MERGE [0.V1]\n"
                            + "    SERVER FILTER BY FIRST KEY ONLY\n"
                            + "CLIENT MERGE SORT",
                    QueryUtil.getExplainPlan(rs));
        rs.close();

        // 4. but can't use the index if not ORDERing by a prefix of the index key.
        rs = conn.createStatement().executeQuery("EXPLAIN SELECT * FROM " + tableName + " ORDER BY v3");
        assertEquals(
            "CLIENT PARALLEL 1-WAY FULL SCAN OVER "
                    + physicalTableName + "\n"
                            + "    SERVER SORTED BY [V3]\n"
                            + "CLIENT MERGE SORT",
                    QueryUtil.getExplainPlan(rs));
        rs.close();

        // 5. If we pin the prefix of the index key we use the index avoiding sorting on the postfix
        rs = conn.createStatement().executeQuery("EXPLAIN SELECT * FROM " + tableName + " WHERE v2 = 2 ORDER BY v3");
        assertEquals(
            "CLIENT PARALLEL 1-WAY RANGE SCAN OVER "
                    + fullIndexName + "(" + indexPhysicalTableName + ") [1,2]\n"
                            + "    SERVER MERGE [0.V1]\n"
                            + "    SERVER FILTER BY FIRST KEY ONLY\n"
                            + "CLIENT MERGE SORT",
                    QueryUtil.getExplainPlan(rs));
        rs.close();

        // 6. Filtering by a non-indexed column will not use the index
        rs = conn.createStatement().executeQuery("EXPLAIN SELECT * FROM " + tableName + " WHERE v1 = 3");
        assertEquals(
            "CLIENT PARALLEL 1-WAY FULL SCAN OVER "
                    + physicalTableName + "\n"
                            + "    SERVER FILTER BY V1 = 3.0",
                    QueryUtil.getExplainPlan(rs));
        rs.close();

        // 7. Also don't use an index if not filtering on a prefix of the key
        rs = conn.createStatement().executeQuery("EXPLAIN SELECT * FROM " + tableName + " WHERE v3 = 1");
        assertEquals(
            "CLIENT PARALLEL 1-WAY FULL SCAN OVER "
                    + physicalTableName + "\n"
                            + "    SERVER FILTER BY V3 = 1",
                    QueryUtil.getExplainPlan(rs));
        rs.close();

        // 8. Filtering along a prefix of the index key can use the index
        rs = conn.createStatement().executeQuery("EXPLAIN SELECT * FROM " + tableName + " WHERE v2 = 2");
        assertEquals(
            "CLIENT PARALLEL 1-WAY RANGE SCAN OVER "
                    + fullIndexName + "(" + indexPhysicalTableName + ") [1,2]\n"
                            + "    SERVER MERGE [0.V1]\n"
                            + "    SERVER FILTER BY FIRST KEY ONLY\n"
                            + "CLIENT MERGE SORT",
                    QueryUtil.getExplainPlan(rs));
        rs.close();

        // 9. Make sure a gap in the index columns still uses the index as long as a prefix is specified
        rs = conn.createStatement().executeQuery("EXPLAIN SELECT * FROM " + tableName + " WHERE v2 = 2 AND v4 = 4");
        assertEquals(
            "CLIENT PARALLEL 1-WAY RANGE SCAN OVER "
                    + fullIndexName + "(" + indexPhysicalTableName + ") [1,2]\n"
                            + "    SERVER MERGE [0.V1]\n"
                            + "    SERVER FILTER BY FIRST KEY ONLY AND TO_INTEGER(\"V4\") = 4\n"
                            + "CLIENT MERGE SORT",
                    QueryUtil.getExplainPlan(rs));
        rs.close();

        // 10. Use index even when also filtering on non-indexed column
        rs = conn.createStatement().executeQuery("EXPLAIN SELECT * FROM " + tableName + " WHERE v2 = 2 AND v1 = 3");
        assertEquals(
            "CLIENT PARALLEL 1-WAY RANGE SCAN OVER "
                    + fullIndexName + "(" + indexPhysicalTableName + ") [1,2]\n"
                    + "    SERVER MERGE [0.V1]\n"
                    + "    SERVER FILTER BY FIRST KEY ONLY AND \"V1\" = 3.0\n"
                    + "CLIENT MERGE SORT",
                    QueryUtil.getExplainPlan(rs));
        rs.close();

        // 11. Another case of not using a prefix of the index key
        rs = conn.createStatement().executeQuery("EXPLAIN SELECT * FROM " + tableName + " WHERE v1 = 3 AND v3 = 1 AND v4 = 1");
        assertEquals(
            "CLIENT PARALLEL 1-WAY FULL SCAN OVER "
                    + physicalTableName + "\n"
                            + "    SERVER FILTER BY (V1 = 3.0 AND V3 = 1 AND V4 = 1)",
                    QueryUtil.getExplainPlan(rs));
        rs.close();
    }

    @Test
    public void testLocalIndexRoundTrip() throws Exception {
        String tableName = schemaName + "." + generateUniqueName();
        String indexName = "IDX_" + generateUniqueName();
        String indexTableName = schemaName + "." + indexName;

        createBaseTable(tableName, null, null);
        Connection conn1 = DriverManager.getConnection(getUrl());
        conn1.createStatement().execute("CREATE LOCAL INDEX " + indexName + " ON " + tableName + "(v1)");
        conn1.createStatement().executeQuery("SELECT * FROM " + tableName).next();
        PTable localIndex = conn1.unwrap(PhoenixConnection.class).getTable(new PTableKey(null, indexTableName));
        assertEquals(IndexType.LOCAL, localIndex.getIndexType());
        assertNotNull(localIndex.getViewIndexId());
        String tableName2 = "test_table" + generateUniqueName();
        String indexName2 = "idx_test_table" + generateUniqueName();
        String createTable =
                "CREATE TABLE IF NOT EXISTS "
                        + tableName2
                        + " (user_time UNSIGNED_TIMESTAMP NOT NULL,user_id varchar NOT NULL,col1 varchar,col2 double,"
                        + "CONSTRAINT pk PRIMARY KEY(user_time,user_id)) SALT_BUCKETS = 20";
        conn1.createStatement().execute(createTable);
        conn1.createStatement().execute(
            "CREATE local INDEX IF NOT EXISTS " + indexName2 + " on " + tableName2
                    + "(\"HOUR\"(user_time))");
        conn1.createStatement().execute(
            "upsert into " + tableName2 + " values(TO_TIME('2005-10-01 14:03:22.559'), 'foo')");
        conn1.commit();
        ResultSet rs =
                conn1.createStatement()
                        .executeQuery(
                            "select substr(to_char(user_time), 0, 10) as ddate, \"HOUR\"(user_time) as hhour, user_id, col1,col2 from "
                                    + tableName2
                                    + " where \"HOUR\"(user_time)=14 group by user_id, col1, col2, ddate, hhour limit 1");
        assertTrue(rs.next());
    }

    @Test
    public void testCreationOfTableWithLocalIndexColumnFamilyPrefixShouldFail() throws Exception {
        Connection conn1 = DriverManager.getConnection(getUrl());
        try {
            conn1.createStatement().execute("CREATE TABLE T(L#a varchar primary key, aL# integer)");
            fail("Column families specified in the table creation should not have local colunm prefix.");
        } catch (SQLException e) { }
    }

    @Test
    public void testLocalIndexCreationWithSplitsShouldFail() throws Exception {
        String tableName = schemaName + "." + generateUniqueName();
        String indexName = "IDX_" + generateUniqueName();

        createBaseTable(tableName, null, null);
        Connection conn1 = getConnection();
        Connection conn2 = getConnection();
        try {
            conn1.createStatement().execute("CREATE LOCAL INDEX " + indexName + " ON " + tableName + "(v1)"+" split on (1,2,3)");
            fail("Local index cannot be pre-split");
        } catch (SQLException e) { }
        try {
            conn2.createStatement().executeQuery("SELECT * FROM " + tableName).next();
            conn2.unwrap(PhoenixConnection.class).getTable(new PTableKey(null,indexName));
            fail("Local index should not be created.");
        } catch (TableNotFoundException e) { }
    }

    @Test
    public void testLocalIndexCreationWithSaltingShouldFail() throws Exception {
        String tableName = schemaName + "." + generateUniqueName();
        String indexName = "IDX_" + generateUniqueName();

        createBaseTable(tableName, null, null);
        Connection conn1 = getConnection();
        Connection conn2 = getConnection();
        try {
            conn1.createStatement().execute("CREATE LOCAL INDEX " + indexName + " ON " + tableName + "(v1)"+" salt_buckets=16");
            fail("Local index cannot be salted.");
        } catch (SQLException e) { }
        try {
            conn2.createStatement().executeQuery("SELECT * FROM " + tableName).next();
            conn2.unwrap(PhoenixConnection.class).getTable(new PTableKey(null,indexName));
            fail("Local index should not be created.");
        } catch (TableNotFoundException e) { }
    }

    @Test
    public void testLocalIndexTableRegionSplitPolicyAndSplitKeys() throws Exception {
        String tableName = schemaName + "." + generateUniqueName();
        String indexName = "IDX_" + generateUniqueName();
        TableName physicalTableName = SchemaUtil.getPhysicalTableName(tableName.getBytes(), isNamespaceMapped);
        String indexPhysicalTableName = physicalTableName.getNameAsString();

        createBaseTable(tableName, null,"('e','i','o')");
        Connection conn1 = getConnection();
        Connection conn2 = getConnection();
        conn1.createStatement().execute("CREATE LOCAL INDEX " + indexName + " ON " + tableName + "(v1)");
        conn2.createStatement().executeQuery("SELECT * FROM " + tableName).next();
        Admin admin = driver.getConnectionQueryServices(getUrl(), TestUtil.TEST_PROPERTIES).getAdmin();
        TableDescriptor htd = admin
                .getDescriptor(TableName.valueOf(indexPhysicalTableName));
        assertEquals(IndexRegionSplitPolicy.class.getName(), htd.getValue(TableDescriptorBuilder.SPLIT_POLICY));
        try(org.apache.hadoop.hbase.client.Connection c = ConnectionFactory.createConnection(admin.getConfiguration())) {
            try (RegionLocator userTable= c.getRegionLocator(SchemaUtil.getPhysicalTableName(tableName.getBytes(), isNamespaceMapped))) {
                try (RegionLocator indxTable = c.getRegionLocator(TableName.valueOf(indexPhysicalTableName))) {
                    assertArrayEquals("Both user table and index table should have same split keys.",
                        userTable.getStartKeys(), indxTable.getStartKeys());
                }
            }
        }
    }
    
    @Test
    public void testDropLocalIndexTable() throws Exception {
        String tableName = schemaName + "." + generateUniqueName();
        String indexName = "IDX_" + generateUniqueName();
        createBaseTable(tableName, null, null);

        String sequenceName = getViewIndexSequenceName(PNameFactory.newName(tableName), null, isNamespaceMapped);
        String sequenceSchemaName = getViewIndexSequenceSchemaName(PNameFactory.newName(tableName), isNamespaceMapped);

        Connection conn1 = getConnection();
        Connection conn2 = getConnection();
        conn1.createStatement().execute("CREATE LOCAL INDEX " + indexName + " ON " + tableName + "(v1)");
        verifySequenceValue(null, sequenceName, sequenceSchemaName,Short.MIN_VALUE + 1);
        conn2.createStatement().executeQuery("SELECT * FROM " + tableName).next();
        conn1.createStatement().execute("DROP TABLE "+ tableName);
        verifySequenceNotExists(null, sequenceName, sequenceSchemaName);
    }
    
    @Test
    public void testPutsToLocalIndexTable() throws Exception {
        String tableName = schemaName + "." + generateUniqueName();
        String indexName = "IDX_" + generateUniqueName();
        String indexTableName = schemaName + "." + indexName;
        TableName physicalTableName = SchemaUtil.getPhysicalTableName(tableName.getBytes(), isNamespaceMapped);
        String indexPhysicalTableName = physicalTableName.getNameAsString();

        createBaseTable(tableName, null, "('e','i','o')");
        Connection conn1 = getConnection();
        conn1.createStatement().execute("CREATE LOCAL INDEX " + indexName + " ON " + tableName + "(v1)");
        conn1.createStatement().execute("UPSERT INTO "+tableName+" values('b',1,2,4,'z')");
        conn1.createStatement().execute("UPSERT INTO "+tableName+" values('f',1,2,3,'z')");
        conn1.createStatement().execute("UPSERT INTO "+tableName+" values('j',2,4,2,'a')");
        conn1.createStatement().execute("UPSERT INTO "+tableName+" values('q',3,1,1,'c')");
        conn1.commit();
        ResultSet rs = conn1.createStatement().executeQuery("SELECT COUNT(*) FROM " + indexTableName);
        assertTrue(rs.next());
        assertEquals(4, rs.getInt(1));
        Admin admin = driver.getConnectionQueryServices(getUrl(), TestUtil.TEST_PROPERTIES).getAdmin();
        Table indexTable =
                admin.getConnection().getTable(TableName.valueOf(indexPhysicalTableName));
        Pair<byte[][], byte[][]> startEndKeys =
                admin.getConnection().getRegionLocator(TableName.valueOf(indexPhysicalTableName))
                        .getStartEndKeys();
        byte[][] startKeys = startEndKeys.getFirst();
        byte[][] endKeys = startEndKeys.getSecond();
        for (int i = 0; i < startKeys.length; i++) {
            Scan s = new Scan();
            s.addFamily(QueryConstants.DEFAULT_LOCAL_INDEX_COLUMN_FAMILY_BYTES);
            s.withStartRow(startKeys[i]);
            s.withStopRow(endKeys[i]);
            ResultScanner scanner = indexTable.getScanner(s);
            int count = 0;
            for(Result r:scanner){
                count++;
            }
            scanner.close();
            assertEquals(1, count);
        }
        indexTable.close();
    }

    @Test
    public void testLocalIndexUsedForUncoveredOrderBy() throws Exception {
        String tableName = schemaName + "." + generateUniqueName();
        String indexName = "IDX_" + generateUniqueName();
        String fullIndexName = SchemaUtil.getTableName(schemaName, indexName);
        TableName physicalTableName = SchemaUtil.getPhysicalTableName(tableName.getBytes(), isNamespaceMapped);
        String indexPhysicalTableName = physicalTableName.getNameAsString();

        createBaseTable(tableName, null, "('e','i','o')");
        try (Connection conn1 = getConnection()) {
            conn1.createStatement().execute("UPSERT INTO " + tableName + " values('b',1,2,4,'z')");
            conn1.createStatement().execute("UPSERT INTO " + tableName + " values('f',1,2,3,'a')");
            conn1.createStatement().execute("UPSERT INTO " + tableName + " values('j',2,4,2,'a')");
            conn1.createStatement().execute("UPSERT INTO " + tableName + " values('q',3,1,1,'c')");
            conn1.commit();
            conn1.createStatement().execute("CREATE LOCAL INDEX " + indexName + " ON " + tableName + "(v1)");

            String query = "SELECT * FROM " + tableName +" ORDER BY V1";
            ResultSet rs = conn1.createStatement().executeQuery("EXPLAIN "+ query);

            Admin admin = driver.getConnectionQueryServices(getUrl(), TestUtil.TEST_PROPERTIES).getAdmin();
            int numRegions = admin.getRegions(physicalTableName).size();

            assertEquals(
                "CLIENT PARALLEL " + numRegions + "-WAY RANGE SCAN OVER "
                        + fullIndexName + "(" + indexPhysicalTableName + ") [1]\n"
                                + "    SERVER MERGE [0.K3]\n"
                                + "    SERVER FILTER BY FIRST KEY ONLY\n"
                                + "CLIENT MERGE SORT",
                        QueryUtil.getExplainPlan(rs));

            rs = conn1.createStatement().executeQuery(query);
            String v = "";
            int i = 0;
            while(rs.next()) {
                String next = rs.getString("v1");
                assertTrue(v.compareTo(next) <= 0);
                v = next;
                i++;
            }
            // see PHOENIX-4967
            assertEquals(4, i);
            rs.close();

            query = "SELECT * FROM " + tableName +" ORDER BY V1 DESC NULLS LAST";
            rs = conn1.createStatement().executeQuery("EXPLAIN "+ query);
            assertEquals(
                "CLIENT PARALLEL " + numRegions + "-WAY REVERSE RANGE SCAN OVER "
                        + fullIndexName + "(" + indexPhysicalTableName + ") [1]\n"
                                + "    SERVER MERGE [0.K3]\n"
                                + "    SERVER FILTER BY FIRST KEY ONLY\n"
                                + "CLIENT MERGE SORT",
                        QueryUtil.getExplainPlan(rs));

            rs = conn1.createStatement().executeQuery(query);
            v = "zz";
            i = 0;
            while(rs.next()) {
                String next = rs.getString("v1");
                assertTrue(v.compareTo(next) >= 0);
                v = next;
                i++;
            }
            // see PHOENIX-4967
            assertEquals(4, i);
            rs.close();

        }
    }

    @Test
    public void testLocalIndexReverseScanShouldReturnAllRows() throws Exception {
        String tableName = schemaName + "." + generateUniqueName();
        String indexName = "IDX_" + generateUniqueName();
        String fullIndexName = SchemaUtil.getTableName(schemaName, indexName);
        TableName physicalTableName = SchemaUtil.getPhysicalTableName(tableName.getBytes(), isNamespaceMapped);
        String indexPhysicalTableName = physicalTableName.getNameAsString();

        createBaseTable(tableName, null, "('e','i','o')");
        try (Connection conn1 = getConnection()) {
            conn1.createStatement().execute("UPSERT INTO " + tableName + " values('b',1,2,4,'z')");
            conn1.createStatement().execute("UPSERT INTO " + tableName + " values('f',1,2,3,'a')");
            conn1.createStatement().execute("UPSERT INTO " + tableName + " values('j',2,4,2,'b')");
            conn1.createStatement().execute("UPSERT INTO " + tableName + " values('q',3,1,1,'c')");
            conn1.commit();
            conn1.createStatement().execute("CREATE LOCAL INDEX " + indexName + " ON " + tableName + "(v1)");

            String query = "SELECT V1 FROM " + tableName +" ORDER BY V1 DESC NULLS LAST";
            ResultSet rs = conn1.createStatement().executeQuery("EXPLAIN "+ query);

            Admin admin = driver.getConnectionQueryServices(getUrl(), TestUtil.TEST_PROPERTIES).getAdmin();
            int numRegions = admin.getRegions(physicalTableName).size();

            assertEquals(
                "CLIENT PARALLEL " + numRegions + "-WAY REVERSE RANGE SCAN OVER "
                        + fullIndexName + "(" + indexPhysicalTableName + ") [1]\n"
                                + "    SERVER FILTER BY FIRST KEY ONLY\n"
                                + "CLIENT MERGE SORT",
                        QueryUtil.getExplainPlan(rs));

            rs = conn1.createStatement().executeQuery(query);
            String v = "zz";
            int i = 0;
            while(rs.next()) {
                String next = rs.getString("v1");
                assertTrue(v.compareTo(next) >= 0);
                v = next;
                i++;
            }
            // see PHOENIX-4967
            assertEquals(4, i);
            rs.close();

        }
    }

    @Test
    public void testLocalIndexScanJoinColumnsFromDataTable() throws Exception {
        String tableName = schemaName + "." + generateUniqueName();
        String indexName = "IDX_" + generateUniqueName();
        String fullIndexName = SchemaUtil.getTableName(schemaName, indexName);
        String indexTableName = schemaName + "." + indexName;
        TableName physicalTableName = SchemaUtil.getPhysicalTableName(tableName.getBytes(), isNamespaceMapped);
        String indexPhysicalTableName = physicalTableName.getNameAsString();

        createBaseTable(tableName, null, "('e','i','o')");
        Connection conn1 = getConnection();
        try{
            conn1.createStatement().execute("UPSERT INTO " + tableName + " values('b',1,2,4,'z')");
            conn1.createStatement().execute("UPSERT INTO " + tableName + " values('f',1,2,3,'a')");
            conn1.createStatement().execute("UPSERT INTO " + tableName + " values('j',2,4,2,'a')");
            conn1.createStatement().execute("UPSERT INTO " + tableName + " values('q',3,1,1,'c')");
            conn1.commit();
            conn1.createStatement().execute("CREATE LOCAL INDEX " + indexName + " ON " + tableName + "(v1)");
            
            ResultSet rs = conn1.createStatement().executeQuery("SELECT COUNT(*) FROM " + indexTableName);
            assertTrue(rs.next());
            
            Admin admin = driver.getConnectionQueryServices(getUrl(), TestUtil.TEST_PROPERTIES).getAdmin();
            int numRegions = admin.getRegions(physicalTableName).size();
            
            String query = "SELECT t_id, k1, k2, k3, V1 FROM " + tableName +" where v1='a'";
            rs = conn1.createStatement().executeQuery("EXPLAIN "+ query);
            
            assertEquals(
                "CLIENT PARALLEL " + numRegions + "-WAY RANGE SCAN OVER "
                        + fullIndexName + "(" + indexPhysicalTableName + ") [1,'a']\n"
                                + "    SERVER MERGE [0.K3]\n"
                                + "    SERVER FILTER BY FIRST KEY ONLY\n"
                                + "CLIENT MERGE SORT",
                        QueryUtil.getExplainPlan(rs));
            
            rs = conn1.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("f", rs.getString("t_id"));
            assertEquals(1, rs.getInt("k1"));
            assertEquals(2, rs.getInt("k2"));
            assertEquals(3, rs.getInt("k3"));
            assertTrue(rs.next());
            assertEquals("j", rs.getString("t_id"));
            assertEquals(2, rs.getInt("k1"));
            assertEquals(4, rs.getInt("k2"));
            assertEquals(2, rs.getInt("k3"));
            assertFalse(rs.next());
            
            query = "SELECT t_id, k1, k2, k3, V1 from " + tableName + "  where v1<='z' order by V1,t_id";
            rs = conn1.createStatement().executeQuery("EXPLAIN " + query);
            
            assertEquals(
                "CLIENT PARALLEL " + numRegions + "-WAY RANGE SCAN OVER "
                        + fullIndexName + "(" + indexPhysicalTableName + ") [1,*] - [1,'z']\n"
                        + "    SERVER MERGE [0.K3]\n"
                        + "    SERVER FILTER BY FIRST KEY ONLY\n"
                         + "CLIENT MERGE SORT",
                QueryUtil.getExplainPlan(rs));
            
            rs = conn1.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("f", rs.getString("t_id"));
            assertEquals(1, rs.getInt("k1"));
            assertEquals(2, rs.getInt("k2"));
            assertEquals(3, rs.getInt("k3"));
            assertEquals("a", rs.getString("V1"));
            assertTrue(rs.next());
            assertEquals("j", rs.getString("t_id"));
            assertEquals(2, rs.getInt("k1"));
            assertEquals(4, rs.getInt("k2"));
            assertEquals(2, rs.getInt("k3"));
            assertEquals("a", rs.getString("V1"));
            assertTrue(rs.next());
            assertEquals("q", rs.getString("t_id"));
            assertEquals(3, rs.getInt("k1"));
            assertEquals(1, rs.getInt("k2"));
            assertEquals(1, rs.getInt("k3"));
            assertEquals("c", rs.getString("V1"));
            assertTrue(rs.next());
            assertEquals("b", rs.getString("t_id"));
            assertEquals(1, rs.getInt("k1"));
            assertEquals(2, rs.getInt("k2"));
            assertEquals(4, rs.getInt("k3"));
            assertEquals("z", rs.getString("V1"));
            
            query = "SELECT t_id, V1, k3 from " + tableName + "  where v1 <='z' group by v1,t_id, k3";
            rs = conn1.createStatement().executeQuery("EXPLAIN " + query);
            
            assertEquals(
                "CLIENT PARALLEL " + numRegions + "-WAY RANGE SCAN OVER "
                        + fullIndexName + "(" + indexPhysicalTableName + ") [1,*] - [1,'z']\n"
                        + "    SERVER MERGE [0.K3]\n"
                        + "    SERVER FILTER BY FIRST KEY ONLY\n"
                        + "    SERVER AGGREGATE INTO DISTINCT ROWS BY [\"V1\", \"T_ID\", \"K3\"]\nCLIENT MERGE SORT",
                QueryUtil.getExplainPlan(rs));
            
            rs = conn1.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("f", rs.getString("t_id"));
            assertEquals(3, rs.getInt("k3"));
            assertEquals("a", rs.getString("V1"));
            assertTrue(rs.next());
            assertEquals("j", rs.getString("t_id"));
            assertEquals(2, rs.getInt("k3"));
            assertEquals("a", rs.getString("V1"));
            assertTrue(rs.next());
            assertEquals("q", rs.getString("t_id"));
            assertEquals(1, rs.getInt("k3"));
            assertEquals("c", rs.getString("V1"));
            assertTrue(rs.next());
            assertEquals("b", rs.getString("t_id"));
            assertEquals(4, rs.getInt("k3"));
            assertEquals("z", rs.getString("V1"));
            
            query = "SELECT v1,sum(k3) from " + tableName + " where v1 <='z'  group by v1 order by v1";
            
            rs = conn1.createStatement().executeQuery("EXPLAIN " + query);
            assertEquals(
                "CLIENT PARALLEL " + numRegions + "-WAY RANGE SCAN OVER "
                        + fullIndexName + "(" + indexPhysicalTableName + ") [1,*] - [1,'z']\n"
                        + "    SERVER MERGE [0.K3]\n"
                        + "    SERVER FILTER BY FIRST KEY ONLY\n"
                        + "    SERVER AGGREGATE INTO ORDERED DISTINCT ROWS BY [\"V1\"]\nCLIENT MERGE SORT",
                QueryUtil.getExplainPlan(rs));
            
            PhoenixStatement stmt = conn1.createStatement().unwrap(PhoenixStatement.class);
            rs = stmt.executeQuery(query);
            QueryPlan plan = stmt.getQueryPlan();
            assertEquals(indexTableName, plan.getContext().getCurrentTable().getTable().getName().getString());
            assertEquals(BaseScannerRegionObserverConstants.KEY_ORDERED_GROUP_BY_EXPRESSIONS, plan.getGroupBy().getScanAttribName());
            assertTrue(rs.next());
            assertEquals("a", rs.getString(1));
            assertEquals(5, rs.getInt(2));
            assertTrue(rs.next());
            assertEquals("c", rs.getString(1));
            assertEquals(1, rs.getInt(2));
            assertTrue(rs.next());
            assertEquals("z", rs.getString(1));
            assertEquals(4, rs.getInt(2));
       } finally {
            conn1.close();
        }
    }

    @Test
    public void testIndexPlanSelectionIfBothGlobalAndLocalIndexesHasSameColumnsAndOrder() throws Exception {
        String tableName = schemaName + "." + generateUniqueName();
        String indexName = "IDX_" + generateUniqueName();
        String indexTableName = schemaName + "." + indexName;

        createBaseTable(tableName, null, "('e','i','o')");
        Connection conn1 = getConnection();
        conn1.createStatement().execute("UPSERT INTO "+tableName+" values('b',1,2,4,'z')");
        conn1.createStatement().execute("UPSERT INTO "+tableName+" values('f',1,2,3,'a')");
        conn1.createStatement().execute("UPSERT INTO "+tableName+" values('j',2,4,3,'a')");
        conn1.createStatement().execute("UPSERT INTO "+tableName+" values('q',3,1,1,'c')");
        conn1.commit();
        conn1.createStatement().execute("CREATE LOCAL INDEX " + indexName + " ON " + tableName + "(v1)");
        conn1.createStatement().execute("CREATE INDEX " + indexName + "2" + " ON " + tableName + "(v1)");
        String query = "SELECT t_id, k1, k2,V1 FROM " + tableName +" where v1='a'";
        ResultSet rs1 = conn1.createStatement().executeQuery("EXPLAIN "+ query);
        assertEquals("CLIENT PARALLEL 1-WAY RANGE SCAN OVER "
                + SchemaUtil.getPhysicalTableName(Bytes.toBytes(indexTableName), isNamespaceMapped) + "2" + " ['a']\n"
                + "    SERVER FILTER BY FIRST KEY ONLY", QueryUtil.getExplainPlan(rs1));
        conn1.close();
    }


    @Test
    public void testDropLocalIndexShouldDeleteDataFromLocalIndexTable() throws Exception {
        String tableName = schemaName + "." + generateUniqueName();
        String indexName = "IDX_" + generateUniqueName();

        createBaseTable(tableName, null, "('e','i','o')");
        Connection conn1 = DriverManager.getConnection(getUrl());
        try {
            conn1.createStatement().execute("UPSERT INTO " + tableName + " values('b',1,2,4,'z')");
            conn1.createStatement().execute("UPSERT INTO " + tableName + " values('f',1,2,3,'a')");
            conn1.createStatement().execute("UPSERT INTO " + tableName + " values('j',2,4,2,'a')");
            conn1.createStatement().execute("UPSERT INTO " + tableName + " values('q',3,1,1,'c')");
            conn1.commit();
            conn1.createStatement().execute("CREATE LOCAL INDEX " + indexName + " ON " + tableName + "(v1)");
            conn1.createStatement().execute("DROP INDEX " + indexName + " ON " + tableName);
            Admin admin = driver.getConnectionQueryServices(getUrl(), TestUtil.TEST_PROPERTIES).getAdmin();
            Table table =
                    admin.getConnection().getTable(TableName.valueOf(tableName));
            Pair<byte[][], byte[][]> startEndKeys =
                    admin.getConnection().getRegionLocator(TableName.valueOf(tableName))
                            .getStartEndKeys();
            byte[][] startKeys = startEndKeys.getFirst();
            byte[][] endKeys = startEndKeys.getSecond();
            // No entry should be present in local index table after drop index.
            for (int i = 0; i < startKeys.length; i++) {
                Scan s = new Scan();
                s.withStartRow(startKeys[i]);
                s.withStopRow(endKeys[i]);
                ColumnFamilyDescriptor[] families = table.getDescriptor().getColumnFamilies();
                for(ColumnFamilyDescriptor cf: families) {
                    if(cf.getNameAsString().startsWith(QueryConstants.LOCAL_INDEX_COLUMN_FAMILY_PREFIX)){
                        s.addFamily(cf.getName());
                    }
                }
                ResultScanner scanner = table.getScanner(s);
                int count = 0;
                for(Result r:scanner){
                    count++;
                }
                scanner.close();
                assertEquals(0, count);
            }
            table.close();
        } finally {
            conn1.close();
        }
    }

    @Test
    public void testLocalIndexRowsShouldBeDeletedWhenUserTableRowsDeleted() throws Exception {
        String tableName = schemaName + "." + generateUniqueName();
        String indexName = "IDX_" + generateUniqueName();
        String indexTableName = schemaName + "." + indexName;

        createBaseTable(tableName, null, "('e','i','o')");
        Connection conn1 = DriverManager.getConnection(getUrl());
        try {
            conn1.createStatement().execute("UPSERT INTO " + tableName + " values('b',1,2,4,'z')");
            conn1.createStatement().execute("UPSERT INTO " + tableName + " values('f',1,2,3,'a')");
            conn1.createStatement().execute("UPSERT INTO " + tableName + " values('j',2,4,2,'a')");
            conn1.createStatement().execute("UPSERT INTO " + tableName + " values('q',3,1,1,'c')");
            conn1.commit();
            conn1.createStatement().execute("CREATE LOCAL INDEX " + indexName + " ON " + tableName + "(v1)");
            conn1.createStatement().execute("DELETE FROM " + tableName + " where v1='a'");
            conn1.commit();
            conn1 = DriverManager.getConnection(getUrl());
            ResultSet rs = conn1.createStatement().executeQuery("SELECT COUNT(*) FROM " + indexTableName);
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
        } finally {
            conn1.close();
        }
    }
    
    @Test
    public void testLocalIndexesOnTableWithImmutableRows() throws Exception {
        String tableName = schemaName + "." + generateUniqueName();
        String indexName = "IDX_" + generateUniqueName();

        createBaseTable(tableName, null, "('e','i','o')");
        Connection conn1 = getConnection();
        try {
            conn1.createStatement().execute("ALTER TABLE "+ tableName + " SET IMMUTABLE_ROWS=true");
            conn1.createStatement().execute("CREATE LOCAL INDEX " + indexName + " ON " + tableName + "(v1)");
            conn1.createStatement().execute("CREATE INDEX " + indexName + "2 ON " + tableName + "(k3)");
            conn1.commit();
            conn1.createStatement().execute("UPSERT INTO " + tableName + " values('b',1,2,4,'z')");
            conn1.createStatement().execute("UPSERT INTO " + tableName + " values('f',1,2,3,'a')");
            conn1.createStatement().execute("UPSERT INTO " + tableName + " values('j',2,4,2,'a')");
            conn1.createStatement().execute("UPSERT INTO " + tableName + " values('q',3,1,1,'c')");
            conn1.commit();
            conn1 = DriverManager.getConnection(getUrl());
            ResultSet rs = conn1.createStatement().executeQuery("SELECT COUNT(*) FROM " + tableName);
            assertTrue(rs.next());
            assertEquals(4, rs.getInt(1));
            rs = conn1.createStatement().executeQuery("SELECT v1 FROM " + tableName);
            assertTrue(rs.next());
            assertEquals("a", rs.getString("v1"));
            assertTrue(rs.next());
            assertEquals("a", rs.getString("v1"));
            assertTrue(rs.next());
            assertEquals("c", rs.getString("v1"));
            assertTrue(rs.next());
            assertEquals("z", rs.getString("v1"));
            assertFalse(rs.next());
            rs = conn1.createStatement().executeQuery("SELECT k3 FROM " + tableName);
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("k3"));
            assertTrue(rs.next());
            assertEquals(2, rs.getInt("k3"));
            assertTrue(rs.next());
            assertEquals(3, rs.getInt("k3"));
            assertTrue(rs.next());
            assertEquals(4, rs.getInt("k3"));
            assertFalse(rs.next());
        } finally {
            conn1.close();
        }
    }

    @Test
    public void testLocalIndexScanWithInList() throws Exception {
        String tableName = schemaName + "." + generateUniqueName();
        String indexName = "IDX_" + generateUniqueName();
        String indexTableName = schemaName + "." + indexName;

        createBaseTable(tableName, null, "('e','i','o')");
        Connection conn1 = DriverManager.getConnection(getUrl());
        try{
            conn1.createStatement().execute("UPSERT INTO " + tableName + " values('b',1,2,4,'z')");
            conn1.createStatement().execute("UPSERT INTO " + tableName + " values('f',1,2,3,'a')");
            conn1.createStatement().execute("UPSERT INTO " + tableName + " values('j',2,4,2,'a')");
            conn1.createStatement().execute("UPSERT INTO " + tableName + " values('q',3,1,1,'c')");
            conn1.commit();
            conn1.createStatement().execute("CREATE LOCAL INDEX " + indexName + " ON " + tableName + "(v1) include (k3)");
            
            ResultSet rs = conn1.createStatement().executeQuery("SELECT COUNT(*) FROM " + indexTableName);
            assertTrue(rs.next());
            
            String query = "SELECT t_id FROM " + tableName +" where (v1,k3) IN (('z',4),('a',2))";
            rs = conn1.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("j", rs.getString("t_id"));
            assertTrue(rs.next());     
            assertEquals("b", rs.getString("t_id"));
            assertFalse(rs.next());
       } finally {
            conn1.close();
        }
    }

    @Test
    public void testLocalIndexCreationWithDefaultFamilyOption() throws Exception {
        Connection conn1 = DriverManager.getConnection(getUrl());
        try{
            Statement statement = conn1.createStatement();
            String tableName = generateUniqueName();
            String indexName = generateUniqueName();
            statement.execute("create table " + tableName + " (id integer not null,fn varchar,"
                    + "\"ln\" varchar constraint pk primary key(id)) DEFAULT_COLUMN_FAMILY='F'");
            statement.execute("upsert into " + tableName + "  values(1,'fn','ln')");
            statement
                    .execute("create local index " + indexName + " on " + tableName + "  (fn)");
            statement.execute("upsert into " + tableName + "  values(2,'fn1','ln1')");
            ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM " + indexName );
            assertTrue(rs.next());
       } finally {
            conn1.close();
        }
    }
    
    @Test
    public void testLocalIndexAutomaticRepair() throws Exception {
        if (isNamespaceMapped) { return; }
        PhoenixConnection conn = DriverManager.getConnection(getUrl()).unwrap(PhoenixConnection.class);
        try (Table metaTable = conn.getQueryServices().getTable(TableName.META_TABLE_NAME.getName());
                Admin admin = conn.getQueryServices().getAdmin();) {
            Statement statement = conn.createStatement();
            final String tableName = "T_AUTO_MATIC_REPAIR";
            String indexName = "IDX_T_AUTO_MATIC_REPAIR";
            String indexName1 = "IDX_T_AUTO_MATIC_REPAIR_1";
            statement.execute("create table " + tableName + " (id integer not null,fn varchar,"
                    + "cf1.ln varchar constraint pk primary key(id)) split on (400,800,1200,1600)");
            statement.execute("create local index " + indexName + " on " + tableName + "  (fn,cf1.ln)");
            statement.execute("create local index " + indexName1 + " on " + tableName + "  (fn)");
            for (int i = 0; i < 2000; i++) {
                statement.execute("upsert into " + tableName + "  values(" + i + ",'fn" + i + "','ln" + i + "')");
            }
            conn.commit();
            ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM " + indexName);
            assertTrue(rs.next());
            assertEquals(2000, rs.getLong(1));
            List<RegionInfo> tableRegions = admin.getRegions(TableName.valueOf(tableName));
            admin.disableTable(TableName.valueOf(tableName));
            copyLocalIndexHFiles(config, tableRegions.get(0), tableRegions.get(1), false);
            copyLocalIndexHFiles(config, tableRegions.get(3), tableRegions.get(0), false);
            admin.enableTable(TableName.valueOf(tableName));

            int count=getCount(conn, tableName, "L#0");
            assertTrue(count > 4000);
            admin.majorCompact(TableName.valueOf(tableName));
            int tryCount = 5;// need to wait for rebuilding of corrupted local index region
            while (tryCount-- > 0 && count != 4000) {
                Thread.sleep(15000);
                count = getCount(conn, tableName, "L#0");
            }
            assertEquals(4000, count);
            rs = statement.executeQuery("SELECT COUNT(*) FROM " + indexName1);
            assertTrue(rs.next());
            assertEquals(2000, rs.getLong(1));
            rs = statement.executeQuery("SELECT COUNT(*) FROM " + indexName);
            assertTrue(rs.next());
            assertEquals(2000, rs.getLong(1));
            statement.execute("DROP INDEX " + indexName1 + " ON " + tableName);
            admin.majorCompact(TableName.valueOf(tableName));
            statement.execute("DROP INDEX " + indexName + " ON " + tableName);
            admin.majorCompact(TableName.valueOf(tableName));
            Thread.sleep(15000);
            admin.majorCompact(TableName.valueOf(tableName));
            Thread.sleep(15000);
            rs = statement.executeQuery("SELECT COUNT(*) FROM " + tableName);
            assertTrue(rs.next());
            
        }
    }

    @Test
    public void testLocalGlobalIndexMix() throws Exception {
        if (isNamespaceMapped) { return; }
        String tableName = generateUniqueName();
        Connection conn1 = DriverManager.getConnection(getUrl());
        String ddl = "CREATE TABLE " + tableName + " (t_id VARCHAR NOT NULL,\n" +
                "k1 INTEGER NOT NULL,\n" +
                "k2 INTEGER NOT NULL,\n" +
                "k3 INTEGER,\n" +
                "v1 VARCHAR,\n" +
                "v2 VARCHAR,\n" +
                "CONSTRAINT pk PRIMARY KEY (t_id, k1, k2))\n";
        conn1.createStatement().execute(ddl);
        conn1.createStatement().execute("CREATE LOCAL INDEX LV1 ON " + tableName + "(v1)");
        conn1.createStatement().execute("CREATE INDEX GV2 ON " + tableName + "(v2)");

        conn1.createStatement().execute("UPSERT INTO " + tableName + " values('b',1,2,4,'z','3')");
        conn1.createStatement().execute("UPSERT INTO " + tableName + " values('f',1,2,3,'a','0')");
        conn1.createStatement().execute("UPSERT INTO " + tableName + " values('j',2,4,2,'a','2')");
        conn1.createStatement().execute("UPSERT INTO " + tableName + " values('q',3,1,1,'c','1')");
        conn1.commit();
        ResultSet rs = conn1.createStatement().executeQuery("SELECT COUNT(*) FROM " + tableName + " WHERE v1 = 'c'");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        rs = conn1.createStatement().executeQuery("SELECT COUNT(*) FROM " + tableName + " WHERE v2 = '2'");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        conn1.close();
    }

    @Test
    public void testLocalIndexSelfJoin() throws Exception {
      String tableName = generateUniqueName();
      String indexName = "IDX_" + generateUniqueName();
      Connection conn1 = DriverManager.getConnection(getUrl());
      if (isNamespaceMapped) {
          conn1.createStatement().execute("CREATE SCHEMA IF NOT EXISTS " + schemaName);
      }
        String ddl =
                "CREATE TABLE "
                        + tableName
                        + " (customer_id integer primary key, postal_code varchar, country_code varchar)";
        conn1.createStatement().execute(ddl);
        conn1.createStatement().execute("UPSERT INTO " + tableName + " values(1,'560103','IN')");
        conn1.commit();
        conn1.createStatement().execute(
            "CREATE LOCAL INDEX " + indexName + " ON " + tableName + "(postal_code)");
        ResultSet rs =
                conn1.createStatement()
                        .executeQuery(
                            "SELECT * from "
                                    + tableName
                                    + " c1, "
                                    + tableName
                                    + " c2 where c1.customer_id=c2.customer_id and c2.postal_code='560103'");
        assertTrue(rs.next());
        conn1.close();
    }

    private void copyLocalIndexHFiles(Configuration conf, RegionInfo fromRegion,
            RegionInfo toRegion, boolean move) throws IOException {
        Path root = CommonFSUtils.getRootDir(conf);

        Path seondRegion =
                new Path(CommonFSUtils.getTableDir(root, fromRegion.getTable()) + Path.SEPARATOR
                        + fromRegion.getEncodedName() + Path.SEPARATOR + "L#0/");
        Path hfilePath =
                CommonFSUtils.getCurrentFileSystem(conf).listFiles(seondRegion, true).next()
                        .getPath();
        Path firstRegionPath =
                new Path(CommonFSUtils.getTableDir(root, toRegion.getTable()) + Path.SEPARATOR
                        + toRegion.getEncodedName() + Path.SEPARATOR + "L#0/");
        FileSystem currentFileSystem = CommonFSUtils.getCurrentFileSystem(conf);
        assertTrue(FileUtil.copy(currentFileSystem, hfilePath, currentFileSystem, firstRegionPath,
            move, conf));
    }

    private int getCount(PhoenixConnection conn, String tableName, String columnFamily)
            throws IOException, SQLException {
        Iterator<Result> iterator = conn.getQueryServices().getTable(Bytes.toBytes(tableName))
                .getScanner(Bytes.toBytes(columnFamily)).iterator();
        int count = 0;
        while (iterator.hasNext()) {
            iterator.next();
            count++;
        }
        return count;
    }


    @Test
    public void testLocalIndexForMultiTenantTable() throws Exception {
        String tableName = schemaName + "." + generateUniqueName();
        String indexName = "IDX_" + generateUniqueName();

        Connection conn1 = getConnection();
        try{
            if (isNamespaceMapped) {
                conn1.createStatement().execute("CREATE SCHEMA IF NOT EXISTS " + schemaName);
            }
            String ddl = "CREATE TABLE " + tableName + " (t_id VARCHAR NOT NULL,\n" +
                    "k1 INTEGER NOT NULL,\n" +
                    "v1 VARCHAR,\n" +
                    "v2 VARCHAR,\n" +
                    "CONSTRAINT pk PRIMARY KEY (t_id, k1)) MULTI_TENANT=true";
            conn1.createStatement().execute(ddl);
            conn1.createStatement().execute("UPSERT INTO " + tableName + " values('b',1,'x','y')");
            conn1.commit();
            conn1.createStatement().execute("CREATE LOCAL INDEX " + indexName + " ON " + tableName + "(v1)");
            
            ResultSet rs = conn1.createStatement().executeQuery("SELECT * FROM " + tableName + " WHERE v1 = 'x'");
            assertTrue(rs.next());
            assertEquals("b", rs.getString("T_ID"));
            assertEquals("y", rs.getString("V2"));
            assertFalse(rs.next());
       } finally {
            conn1.close();
        }
    }

    @Test // See https://issues.apache.org/jira/browse/PHOENIX-4289
    public void testEstimatesWithLocalIndexes() throws Exception {
        String tableName = generateUniqueName();
        String indexName = "IDX_" + generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            int guidePostWidth = 20;
            conn.createStatement()
                    .execute("CREATE TABLE " + tableName
                            + " (k INTEGER PRIMARY KEY, a bigint, b bigint)"
                            + " GUIDE_POSTS_WIDTH=" + guidePostWidth);
            conn.createStatement().execute("upsert into " + tableName + " values (100,1,3)");
            conn.createStatement().execute("upsert into " + tableName + " values (101,2,4)");
            conn.createStatement().execute("upsert into " + tableName + " values (102,2,4)");
            conn.createStatement().execute("upsert into " + tableName + " values (103,2,4)");
            conn.createStatement().execute("upsert into " + tableName + " values (104,2,4)");
            conn.createStatement().execute("upsert into " + tableName + " values (105,2,4)");
            conn.createStatement().execute("upsert into " + tableName + " values (106,2,4)");
            conn.createStatement().execute("upsert into " + tableName + " values (107,2,4)");
            conn.createStatement().execute("upsert into " + tableName + " values (108,2,4)");
            conn.createStatement().execute("upsert into " + tableName + " values (109,2,4)");
            conn.commit();
            conn.createStatement().execute(
                "CREATE LOCAL INDEX " + indexName + " ON " + tableName + " (a) INCLUDE (b) ");
            String ddl = "ALTER TABLE " + tableName + " SET USE_STATS_FOR_PARALLELIZATION = false";
            conn.createStatement().execute(ddl);
            conn.createStatement().execute("UPDATE STATISTICS " + tableName + "");
        }
        List<Object> binds = Lists.newArrayList();
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String sql =
                    "SELECT COUNT(*) " + " FROM " + tableName;
            ResultSet rs = conn.createStatement().executeQuery(sql);
            assertTrue("Index " + indexName + " should have been used",
                rs.unwrap(PhoenixResultSet.class).getStatement().getQueryPlan().getTableRef()
                        .getTable().getName().getString().equals(indexName));
            Estimate info = getByteRowEstimates(conn, sql, binds);
            assertEquals((Long) 10l, info.getEstimatedRows());
            assertTrue(info.getEstimateInfoTs() > 0);
        }
    }
}
