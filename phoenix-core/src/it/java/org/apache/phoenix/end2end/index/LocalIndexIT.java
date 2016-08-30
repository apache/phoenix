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
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.coprocessor.BaseScannerRegionObserver;
import org.apache.phoenix.end2end.BaseHBaseManagedTimeTableReuseIT;
import org.apache.phoenix.end2end.Shadower;
import org.apache.phoenix.hbase.index.IndexRegionSplitPolicy;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.*;
import org.apache.phoenix.schema.PTable.IndexType;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.collect.Maps;

@RunWith(Parameterized.class)
public class LocalIndexIT extends BaseHBaseManagedTimeTableReuseIT {
    private boolean isNamespaceMapped;
    String schemaName="TEST";

    public LocalIndexIT(boolean isNamespaceMapped) {
        this.isNamespaceMapped = isNamespaceMapped;
    }
    
    @BeforeClass 
    @Shadower(classBeingShadowed = BaseHBaseManagedTimeTableReuseIT.class)
    public static void doSetup() throws Exception {
        Map<String,String> props = Maps.newHashMapWithExpectedSize(3);
        // Drop the HBase table metadata for this test
        props.put(QueryServices.DROP_METADATA_ATTRIB, Boolean.toString(true));
        // Must update config before starting server
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }

    private void createBaseTable(String tableName, Integer saltBuckets, String splits) throws SQLException {
        Connection conn = getConnection();
        if (isNamespaceMapped) {
            conn.createStatement().execute("CREATE SCHEMA IF NOT EXISTS " + schemaName);
        }
        String ddl = "CREATE TABLE " + tableName + " (t_id VARCHAR NOT NULL,\n" +
                "k1 INTEGER NOT NULL,\n" +
                "k2 INTEGER NOT NULL,\n" +
                "k3 INTEGER,\n" +
                "v1 VARCHAR,\n" +
                "CONSTRAINT pk PRIMARY KEY (t_id, k1, k2))\n"
                        + (saltBuckets != null && splits == null ? (" salt_buckets=" + saltBuckets) : ""
                        + (saltBuckets == null && splits != null ? (" split on " + splits) : ""));
        conn.createStatement().execute(ddl);
        conn.close();
    }

    @Parameters(name = "isNamespaceMapped = {0}")
    public static Collection<Boolean> data() {
        return Arrays.asList(true, false);
    }

    @Test
    public void testLocalIndexRoundTrip() throws Exception {
        String tableName = schemaName + "." + generateRandomString();
        String indexName = "IDX_" + generateRandomString();
        String indexTableName = schemaName + "." + indexName;

        createBaseTable(tableName, null, null);
        Connection conn1 = DriverManager.getConnection(getUrl());
        conn1.createStatement().execute("CREATE LOCAL INDEX " + indexName + " ON " + tableName + "(v1)");
        conn1.createStatement().executeQuery("SELECT * FROM " + tableName).next();
        PTable localIndex = conn1.unwrap(PhoenixConnection.class).getTable(new PTableKey(null, indexTableName));
        assertEquals(IndexType.LOCAL, localIndex.getIndexType());
        assertNotNull(localIndex.getViewIndexId());
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
        String tableName = schemaName + "." + generateRandomString();
        String indexName = "IDX_" + generateRandomString();

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
        String tableName = schemaName + "." + generateRandomString();
        String indexName = "IDX_" + generateRandomString();

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
        String tableName = schemaName + "." + generateRandomString();
        String indexName = "IDX_" + generateRandomString();
        TableName physicalTableName = SchemaUtil.getPhysicalTableName(tableName.getBytes(), isNamespaceMapped);
        String indexPhysicalTableName = physicalTableName.getNameAsString();

        createBaseTable(tableName, null,"('e','i','o')");
        Connection conn1 = getConnection();
        Connection conn2 = getConnection();
        conn1.createStatement().execute("CREATE LOCAL INDEX " + indexName + " ON " + tableName + "(v1)");
        conn2.createStatement().executeQuery("SELECT * FROM " + tableName).next();
        HBaseAdmin admin = driver.getConnectionQueryServices(getUrl(), TestUtil.TEST_PROPERTIES).getAdmin();
        HTableDescriptor htd = admin
                .getTableDescriptor(Bytes.toBytes(indexPhysicalTableName));
        assertEquals(IndexRegionSplitPolicy.class.getName(), htd.getValue(HTableDescriptor.SPLIT_POLICY));
        try (HTable userTable = new HTable(admin.getConfiguration(),
                SchemaUtil.getPhysicalTableName(tableName.getBytes(), isNamespaceMapped))) {
            try (HTable indexTable = new HTable(admin.getConfiguration(), Bytes.toBytes(indexPhysicalTableName))) {
                assertArrayEquals("Both user table and index table should have same split keys.",
                        userTable.getStartKeys(), indexTable.getStartKeys());
            }
        }
    }
    
    public Connection getConnection() throws SQLException{
        Properties props = new Properties();
        props.put(QueryServices.DROP_METADATA_ATTRIB, Boolean.toString(true));
        props.put(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, Boolean.toString(isNamespaceMapped));
        return DriverManager.getConnection(getUrl(),props);
    }


    @Test
    public void testDropLocalIndexTable() throws Exception {
        String tableName = schemaName + "." + generateRandomString();
        String indexName = "IDX_" + generateRandomString();
        createBaseTable(tableName, null, null);

        String sequenceName = getViewIndexSequenceName(PNameFactory.newName(tableName), null, isNamespaceMapped);
        String sequenceSchemaName = getViewIndexSequenceSchemaName(PNameFactory.newName(tableName), isNamespaceMapped);

        Connection conn1 = getConnection();
        Connection conn2 = getConnection();
        conn1.createStatement().execute("CREATE LOCAL INDEX " + indexName + " ON " + tableName + "(v1)");
        verifySequence(null, sequenceName, sequenceSchemaName, true);
        conn2.createStatement().executeQuery("SELECT * FROM " + tableName).next();
        conn1.createStatement().execute("DROP TABLE "+ tableName);

        verifySequence(null, sequenceName, sequenceSchemaName, false);
    }
    
    @Test
    public void testPutsToLocalIndexTable() throws Exception {
        String tableName = schemaName + "." + generateRandomString();
        String indexName = "IDX_" + generateRandomString();
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
        HBaseAdmin admin = driver.getConnectionQueryServices(getUrl(), TestUtil.TEST_PROPERTIES).getAdmin();
        HTable indexTable = new HTable(admin.getConfiguration(), indexPhysicalTableName);
        Pair<byte[][], byte[][]> startEndKeys = indexTable.getStartEndKeys();
        byte[][] startKeys = startEndKeys.getFirst();
        byte[][] endKeys = startEndKeys.getSecond();
        for (int i = 0; i < startKeys.length; i++) {
            Scan s = new Scan();
            s.addFamily(QueryConstants.DEFAULT_LOCAL_INDEX_COLUMN_FAMILY_BYTES);
            s.setStartRow(startKeys[i]);
            s.setStopRow(endKeys[i]);
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
    public void testBuildIndexWhenUserTableAlreadyHasData() throws Exception {
        String tableName = schemaName + "." + generateRandomString();
        String indexName = "IDX_" + generateRandomString();
        String indexTableName = schemaName + "." + indexName;
        TableName physicalTableName = SchemaUtil.getPhysicalTableName(tableName.getBytes(), isNamespaceMapped);
        String indexPhysicalTableName = physicalTableName.getNameAsString();

        createBaseTable(tableName, null, "('e','i','o')");
        Connection conn1 = DriverManager.getConnection(getUrl());
        conn1.createStatement().execute("UPSERT INTO "+tableName+" values('b',1,2,4,'z')");
        conn1.createStatement().execute("UPSERT INTO "+tableName+" values('f',1,2,3,'z')");
        conn1.createStatement().execute("UPSERT INTO "+tableName+" values('j',2,4,2,'a')");
        conn1.createStatement().execute("UPSERT INTO "+tableName+" values('q',3,1,1,'c')");
        conn1.commit();
        conn1.createStatement().execute("CREATE LOCAL INDEX " + indexName + " ON " + tableName + "(v1)");
        ResultSet rs = conn1.createStatement().executeQuery("SELECT COUNT(*) FROM " + indexTableName);
        assertTrue(rs.next());
        assertEquals(4, rs.getInt(1));
        HBaseAdmin admin = driver.getConnectionQueryServices(getUrl(), TestUtil.TEST_PROPERTIES).getAdmin();
        HTable indexTable = new HTable(admin.getConfiguration(),Bytes.toBytes(indexPhysicalTableName));
        Pair<byte[][], byte[][]> startEndKeys = indexTable.getStartEndKeys();
        byte[][] startKeys = startEndKeys.getFirst();
        byte[][] endKeys = startEndKeys.getSecond();
        for (int i = 0; i < startKeys.length; i++) {
            Scan s = new Scan();
            s.addFamily(QueryConstants.DEFAULT_LOCAL_INDEX_COLUMN_FAMILY_BYTES);
            s.setStartRow(startKeys[i]);
            s.setStopRow(endKeys[i]);
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
    public void testLocalIndexScan() throws Exception {
        String tableName = schemaName + "." + generateRandomString();
        String indexName = "IDX_" + generateRandomString();
        String indexTableName = schemaName + "." + indexName;
        TableName physicalTableName = SchemaUtil.getPhysicalTableName(tableName.getBytes(), isNamespaceMapped);
        String indexPhysicalTableName = physicalTableName.getNameAsString();

        createBaseTable(tableName, null, "('e','i','o')");
        Connection conn1 = DriverManager.getConnection(getUrl());
        try{
            conn1.createStatement().execute("UPSERT INTO " + tableName + " values('a',1,2,5,'y')");
            conn1.createStatement().execute("UPSERT INTO " + tableName + " values('b',1,2,4,'z')");
            conn1.createStatement().execute("UPSERT INTO " + tableName + " values('f',1,2,3,'a')");
            conn1.createStatement().execute("UPSERT INTO " + tableName + " values('e',1,2,3,'b')");
            conn1.createStatement().execute("UPSERT INTO " + tableName + " values('j',2,4,2,'a')");
            conn1.createStatement().execute("UPSERT INTO " + tableName + " values('q',3,1,1,'c')");
            conn1.commit();
            conn1.createStatement().execute("CREATE LOCAL INDEX " + indexName + " ON " + tableName + "(v1)");
            
            ResultSet rs = conn1.createStatement().executeQuery("SELECT COUNT(*) FROM " + indexTableName);
            assertTrue(rs.next());
            
            HBaseAdmin admin = driver.getConnectionQueryServices(getUrl(), TestUtil.TEST_PROPERTIES).getAdmin();
            int numRegions = admin.getTableRegions(physicalTableName).size();
            
            String query = "SELECT * FROM " + tableName +" where v1 like 'a%'";
            rs = conn1.createStatement().executeQuery("EXPLAIN "+ query);
            
            assertEquals(
                "CLIENT PARALLEL " + numRegions + "-WAY RANGE SCAN OVER "
                        + indexPhysicalTableName + " [1,'a'] - [1,'b']\n"
                                + "    SERVER FILTER BY FIRST KEY ONLY\n"
                                + "CLIENT MERGE SORT",
                        QueryUtil.getExplainPlan(rs));
            
            rs = conn1.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("f", rs.getString("t_id"));
            assertEquals(1, rs.getInt("k1"));
            assertEquals(2, rs.getInt("k2"));
            assertEquals("a", rs.getString("v1"));
            assertEquals(3, rs.getInt("k3"));
            assertTrue(rs.next());
            assertEquals("j", rs.getString("t_id"));
            assertEquals(2, rs.getInt("k1"));
            assertEquals(4, rs.getInt("k2"));
            assertEquals("a", rs.getString("v1"));
            assertEquals(2, rs.getInt("k3"));
            assertFalse(rs.next());
            query = "SELECT t_id, k1, k2,V1 FROM " + tableName +" where v1='a'";
            rs = conn1.createStatement().executeQuery("EXPLAIN "+ query);
            
            assertEquals(
                "CLIENT PARALLEL " + numRegions + "-WAY RANGE SCAN OVER "
                        + indexPhysicalTableName + " [1,'a']\n"
                        + "    SERVER FILTER BY FIRST KEY ONLY\n"
                        + "CLIENT MERGE SORT",
                        QueryUtil.getExplainPlan(rs));
            
            rs = conn1.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("f", rs.getString("t_id"));
            assertEquals(1, rs.getInt("k1"));
            assertEquals(2, rs.getInt("k2"));
            assertTrue(rs.next());
            assertEquals("j", rs.getString("t_id"));
            assertEquals(2, rs.getInt("k1"));
            assertEquals(4, rs.getInt("k2"));
            assertFalse(rs.next());
            query = "SELECT t_id, k1, k2,V1, k3 FROM " + tableName +" where v1<='z' order by k3";
            rs = conn1.createStatement().executeQuery("EXPLAIN "+ query);
            
            assertEquals("CLIENT PARALLEL " + numRegions + "-WAY RANGE SCAN OVER " + indexPhysicalTableName
                    + " [1,*] - [1,'z']\n" + "    SERVER FILTER BY FIRST KEY ONLY\n"
                    + "    SERVER SORTED BY [\"K3\"]\n" + "CLIENT MERGE SORT", QueryUtil.getExplainPlan(rs));
 
            rs = conn1.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("k3"));
            assertTrue(rs.next());
            assertEquals(2, rs.getInt("k3"));
            assertTrue(rs.next());
            assertEquals(3, rs.getInt("k3"));
            assertTrue(rs.next());
            assertEquals(3, rs.getInt("k3"));
            assertTrue(rs.next());
            assertEquals(4, rs.getInt("k3"));
            assertTrue(rs.next());
            assertEquals(5, rs.getInt("k3"));
            assertFalse(rs.next());
            
            query = "SELECT t_id, k1, k2,v1 from " + tableName + " order by V1,t_id";
            rs = conn1.createStatement().executeQuery("EXPLAIN " + query);
            
            assertEquals(
                "CLIENT PARALLEL " + numRegions + "-WAY RANGE SCAN OVER "
                        + indexPhysicalTableName +" [1]\n"
                        + "    SERVER FILTER BY FIRST KEY ONLY\n"
                        + "CLIENT MERGE SORT",
                QueryUtil.getExplainPlan(rs));
            
            rs = conn1.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("f", rs.getString("t_id"));
            assertEquals(1, rs.getInt("k1"));
            assertEquals(2, rs.getInt("k2"));
            assertEquals("a", rs.getString("V1"));
            assertTrue(rs.next());
            assertEquals("j", rs.getString("t_id"));
            assertEquals(2, rs.getInt("k1"));
            assertEquals(4, rs.getInt("k2"));
            assertEquals("a", rs.getString("V1"));
            assertTrue(rs.next());
            assertEquals("e", rs.getString("t_id"));
            assertEquals(1, rs.getInt("k1"));
            assertEquals(2, rs.getInt("k2"));
            assertEquals("b", rs.getString("V1"));
            assertTrue(rs.next());
            assertEquals("q", rs.getString("t_id"));
            assertEquals(3, rs.getInt("k1"));
            assertEquals(1, rs.getInt("k2"));
            assertEquals("c", rs.getString("V1"));
            assertTrue(rs.next());
            assertEquals("a", rs.getString("t_id"));
            assertEquals(1, rs.getInt("k1"));
            assertEquals(2, rs.getInt("k2"));
            assertEquals("y", rs.getString("V1"));
            assertTrue(rs.next());
            assertEquals("b", rs.getString("t_id"));
            assertEquals(1, rs.getInt("k1"));
            assertEquals(2, rs.getInt("k2"));
            assertEquals("z", rs.getString("V1"));
        } finally {
            conn1.close();
        }
    }

    @Test
    public void testLocalIndexScanJoinColumnsFromDataTable() throws Exception {
        String tableName = schemaName + "." + generateRandomString();
        String indexName = "IDX_" + generateRandomString();
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
            
            HBaseAdmin admin = driver.getConnectionQueryServices(getUrl(), TestUtil.TEST_PROPERTIES).getAdmin();
            int numRegions = admin.getTableRegions(physicalTableName).size();
            
            String query = "SELECT t_id, k1, k2, k3, V1 FROM " + tableName +" where v1='a'";
            rs = conn1.createStatement().executeQuery("EXPLAIN "+ query);
            
            assertEquals(
                "CLIENT PARALLEL " + numRegions + "-WAY RANGE SCAN OVER "
                        + indexPhysicalTableName + " [1,'a']\n"
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
                        + indexPhysicalTableName +" [1,*] - [1,'z']\n"
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
                        + indexPhysicalTableName +" [1,*] - [1,'z']\n"
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
                        + indexPhysicalTableName +" [1,*] - [1,'z']\n"
                        + "    SERVER FILTER BY FIRST KEY ONLY\n"
                        + "    SERVER AGGREGATE INTO ORDERED DISTINCT ROWS BY [\"V1\"]\nCLIENT MERGE SORT",
                QueryUtil.getExplainPlan(rs));
            
            PhoenixStatement stmt = conn1.createStatement().unwrap(PhoenixStatement.class);
            rs = stmt.executeQuery(query);
            QueryPlan plan = stmt.getQueryPlan();
            assertEquals(indexTableName, plan.getContext().getCurrentTable().getTable().getName().getString());
            assertEquals(BaseScannerRegionObserver.KEY_ORDERED_GROUP_BY_EXPRESSIONS, plan.getGroupBy().getScanAttribName());
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
        String tableName = schemaName + "." + generateRandomString();
        String indexName = "IDX_" + generateRandomString();
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
        String tableName = schemaName + "." + generateRandomString();
        String indexName = "IDX_" + generateRandomString();

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
            HBaseAdmin admin = driver.getConnectionQueryServices(getUrl(), TestUtil.TEST_PROPERTIES).getAdmin();
            HTable table = new HTable(admin.getConfiguration() ,TableName.valueOf(tableName));
            Pair<byte[][], byte[][]> startEndKeys = table.getStartEndKeys();
            byte[][] startKeys = startEndKeys.getFirst();
            byte[][] endKeys = startEndKeys.getSecond();
            // No entry should be present in local index table after drop index.
            for (int i = 0; i < startKeys.length; i++) {
                Scan s = new Scan();
                s.setStartRow(startKeys[i]);
                s.setStopRow(endKeys[i]);
                Collection<HColumnDescriptor> families = table.getTableDescriptor().getFamilies();
                for(HColumnDescriptor cf: families) {
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
        String tableName = schemaName + "." + generateRandomString();
        String indexName = "IDX_" + generateRandomString();
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
    public void testScanWhenATableHasMultipleLocalIndexes() throws Exception {
        String tableName = schemaName + "." + generateRandomString();
        String indexName = "IDX_" + generateRandomString();

        createBaseTable(tableName, null, "('e','i','o')");
        Connection conn1 = DriverManager.getConnection(getUrl());
        try {
            conn1.createStatement().execute("UPSERT INTO " + tableName + " values('b',1,2,4,'z')");
            conn1.createStatement().execute("UPSERT INTO " + tableName + " values('f',1,2,3,'a')");
            conn1.createStatement().execute("UPSERT INTO " + tableName + " values('j',2,4,2,'a')");
            conn1.createStatement().execute("UPSERT INTO " + tableName + " values('q',3,1,1,'c')");
            conn1.commit();
            conn1.createStatement().execute("CREATE LOCAL INDEX " + indexName + " ON " + tableName + "(v1)");
            conn1.createStatement().execute("CREATE LOCAL INDEX " + indexName + "2 ON " + tableName + "(k3)");
            conn1.commit();
            conn1 = DriverManager.getConnection(getUrl());
            ResultSet rs = conn1.createStatement().executeQuery("SELECT COUNT(*) FROM " + tableName);
            assertTrue(rs.next());
            assertEquals(4, rs.getInt(1));
        } finally {
            conn1.close();
        }
    }

    @Test
    public void testLocalIndexesOnTableWithImmutableRows() throws Exception {
        String tableName = schemaName + "." + generateRandomString();
        String indexName = "IDX_" + generateRandomString();

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
        String tableName = schemaName + "." + generateRandomString();
        String indexName = "IDX_" + generateRandomString();
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
            String tableName = generateRandomString();
            String indexName = generateRandomString();
            statement.execute("create table " + tableName + " (id integer not null,fn varchar,"
                    + "ln varchar constraint pk primary key(id)) DEFAULT_COLUMN_FAMILY='F'");
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
    public void testLocalIndexScanAfterRegionSplit() throws Exception {
        String tableName = schemaName + "." + generateRandomString();
        String indexName = "IDX_" + generateRandomString();
        TableName physicalTableName = SchemaUtil.getPhysicalTableName(tableName.getBytes(), isNamespaceMapped);
        String indexPhysicalTableName = physicalTableName.getNameAsString();

        if (isNamespaceMapped) { return; }
        createBaseTable(tableName, null, "('e','j','o')");
        Connection conn1 = getConnection();
        try{
            String[] strings = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t","u","v","w","x","y","z"};
            for (int i = 0; i < 26; i++) {
                conn1.createStatement().execute(
                    "UPSERT INTO " + tableName + " values('"+strings[i]+"'," + i + ","
                            + (i + 1) + "," + (i + 2) + ",'" + strings[25 - i] + "')");
            }
            conn1.commit();
            conn1.createStatement().execute("CREATE LOCAL INDEX " + indexName + " ON " + tableName + "(v1)");
            conn1.createStatement().execute("CREATE LOCAL INDEX " + indexName + "_2 ON " + tableName + "(k3)");

            ResultSet rs = conn1.createStatement().executeQuery("SELECT * FROM " + tableName);
            assertTrue(rs.next());
            
            HBaseAdmin admin = conn1.unwrap(PhoenixConnection.class).getQueryServices().getAdmin();
            for (int i = 1; i < 5; i++) {
                admin.split(physicalTableName, ByteUtil.concat(Bytes.toBytes(strings[3*i])));
                List<HRegionInfo> regionsOfUserTable =
                        MetaTableAccessor.getTableRegions(getUtility().getZooKeeperWatcher(), admin.getConnection(),
                                physicalTableName, false);

                while (regionsOfUserTable.size() != (4+i)) {
                    Thread.sleep(100);
                    regionsOfUserTable = MetaTableAccessor.getTableRegions(getUtility().getZooKeeperWatcher(),
                            admin.getConnection(), physicalTableName, false);
                }
                assertEquals(4+i, regionsOfUserTable.size());
                String[] tIdColumnValues = new String[26]; 
                String[] v1ColumnValues = new String[26];
                int[] k1ColumnValue = new int[26];
                String query = "SELECT t_id,k1,v1 FROM " + tableName;
                rs = conn1.createStatement().executeQuery(query);
                Thread.sleep(1000);
                for (int j = 0; j < 26; j++) {
                    assertTrue(rs.next());
                    tIdColumnValues[j] = rs.getString("t_id");
                    k1ColumnValue[j] = rs.getInt("k1");
                    v1ColumnValues[j] = rs.getString("V1");
                }
                Arrays.sort(tIdColumnValues);
                Arrays.sort(v1ColumnValues);
                Arrays.sort(k1ColumnValue);
                assertTrue(Arrays.equals(strings, tIdColumnValues));
                assertTrue(Arrays.equals(strings, v1ColumnValues));
                for(int m=0;m<26;m++) {
                    assertEquals(m, k1ColumnValue[m]);
                }

                rs = conn1.createStatement().executeQuery("EXPLAIN " + query);
                assertEquals(
                        "CLIENT PARALLEL " + (4 + i) + "-WAY RANGE SCAN OVER "
                                + indexPhysicalTableName + " [1]\n"
                                        + "    SERVER FILTER BY FIRST KEY ONLY\n"
                                + "CLIENT MERGE SORT", QueryUtil.getExplainPlan(rs));
                
                query = "SELECT t_id,k1,k3 FROM " + tableName;
                rs = conn1.createStatement().executeQuery("EXPLAIN "+query);
                assertEquals(
                    "CLIENT PARALLEL "
                            + ((strings[3 * i].compareTo("j") < 0) ? (4 + i) : (4 + i - 1))
                            + "-WAY RANGE SCAN OVER "
                            + indexPhysicalTableName + " [2]\n"
                                    + "    SERVER FILTER BY FIRST KEY ONLY\n"
                            + "CLIENT MERGE SORT", QueryUtil.getExplainPlan(rs));
                rs = conn1.createStatement().executeQuery(query);
                Thread.sleep(1000);
                int[] k3ColumnValue = new int[26];
                for (int j = 0; j < 26; j++) {
                    assertTrue(rs.next());
                    tIdColumnValues[j] = rs.getString("t_id");
                    k1ColumnValue[j] = rs.getInt("k1");
                    k3ColumnValue[j] = rs.getInt("k3");
                }
                Arrays.sort(tIdColumnValues);
                Arrays.sort(k1ColumnValue);
                Arrays.sort(k3ColumnValue);
                assertTrue(Arrays.equals(strings, tIdColumnValues));
                for(int m=0;m<26;m++) {
                    assertEquals(m, k1ColumnValue[m]);
                    assertEquals(m+2, k3ColumnValue[m]);
                }
            }
       } finally {
            conn1.close();
        }
    }

    @Test
    public void testLocalIndexScanWithSmallChunks() throws Exception {
        String tableName = schemaName + "." + generateRandomString();
        String indexName = "IDX_" + generateRandomString();

        createBaseTable(tableName, 3, null);
        Properties props = new Properties();
        props.setProperty(QueryServices.SCAN_RESULT_CHUNK_SIZE, "2");
        props.setProperty(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, Boolean.toString(isNamespaceMapped));
        Connection conn1 = DriverManager.getConnection(getUrl(), props);
        try{
            String[] strings = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t","u","v","w","x","y","z"};
            for (int i = 0; i < 26; i++) {
               conn1.createStatement().execute(
                    "UPSERT INTO " + tableName + " values('"+strings[i]+"'," + i + ","
                            + (i + 1) + "," + (i + 2) + ",'" + strings[25 - i] + "')");
            }
            conn1.commit();
            conn1.createStatement().execute("CREATE LOCAL INDEX " + indexName + " ON " + tableName + "(v1)");
            conn1.createStatement().execute("CREATE LOCAL INDEX " + indexName + "_2 ON " + tableName + "(k3)");

            ResultSet rs = conn1.createStatement().executeQuery("SELECT * FROM " + tableName);
            assertTrue(rs.next());

            String query = "SELECT t_id,k1,v1 FROM " + tableName;
            rs = conn1.createStatement().executeQuery(query);
            for (int j = 0; j < 26; j++) {
                assertTrue(rs.next());
                assertEquals(strings[25 - j], rs.getString("t_id"));
                assertEquals(25 - j, rs.getInt("k1"));
                assertEquals(strings[j], rs.getString("V1"));
            }
            query = "SELECT t_id,k1,k3 FROM " + tableName;
            rs = conn1.createStatement().executeQuery(query);
            Thread.sleep(1000);
            for (int j = 0; j < 26; j++) {
                assertTrue(rs.next());
                assertEquals(strings[j], rs.getString("t_id"));
                assertEquals(j, rs.getInt("k1"));
                assertEquals(j + 2, rs.getInt("k3"));
            }
       } finally {
            conn1.close();
        }
    }

    @Test
    public void testLocalIndexScanAfterRegionsMerge() throws Exception {
        String tableName = schemaName + "." + generateRandomString();
        String indexName = "IDX_" + generateRandomString();
        TableName physicalTableName = SchemaUtil.getPhysicalTableName(tableName.getBytes(), isNamespaceMapped);
        String indexPhysicalTableName = physicalTableName.getNameAsString();

        if (isNamespaceMapped) { return; }
        createBaseTable(tableName, null, "('e','j','o')");
        Connection conn1 = getConnection();
        try{
            String[] strings = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t","u","v","w","x","y","z"};
            for (int i = 0; i < 26; i++) {
                conn1.createStatement().execute(
                    "UPSERT INTO " + tableName + " values('"+strings[i]+"'," + i + ","
                            + (i + 1) + "," + (i + 2) + ",'" + strings[25 - i] + "')");
            }
            conn1.commit();
            conn1.createStatement().execute("CREATE LOCAL INDEX " + indexName + " ON " + tableName + "(v1)");
            conn1.createStatement().execute("CREATE LOCAL INDEX " + indexName + "_2 ON " + tableName + "(k3)");

            ResultSet rs = conn1.createStatement().executeQuery("SELECT * FROM " + tableName);
            assertTrue(rs.next());

            HBaseAdmin admin = conn1.unwrap(PhoenixConnection.class).getQueryServices().getAdmin();
            List<HRegionInfo> regionsOfUserTable =
                    MetaTableAccessor.getTableRegions(getUtility().getZooKeeperWatcher(), admin.getConnection(),
                        physicalTableName, false);
            admin.mergeRegions(regionsOfUserTable.get(0).getEncodedNameAsBytes(),
                regionsOfUserTable.get(1).getEncodedNameAsBytes(), false);
            regionsOfUserTable =
                    MetaTableAccessor.getTableRegions(getUtility().getZooKeeperWatcher(), admin.getConnection(),
                            physicalTableName, false);

            while (regionsOfUserTable.size() != 3) {
                Thread.sleep(100);
                regionsOfUserTable = MetaTableAccessor.getTableRegions(getUtility().getZooKeeperWatcher(),
                        admin.getConnection(), physicalTableName, false);
            }
            String query = "SELECT t_id,k1,v1 FROM " + tableName;
            rs = conn1.createStatement().executeQuery(query);
            Thread.sleep(1000);
            for (int j = 0; j < 26; j++) {
                assertTrue(rs.next());
                assertEquals(strings[25 - j], rs.getString("t_id"));
                assertEquals(25 - j, rs.getInt("k1"));
                assertEquals(strings[j], rs.getString("V1"));
            }
            rs = conn1.createStatement().executeQuery("EXPLAIN " + query);
            assertEquals(
                "CLIENT PARALLEL " + 3 + "-WAY RANGE SCAN OVER "
                        + indexPhysicalTableName
                        + " [1]\n" + "    SERVER FILTER BY FIRST KEY ONLY\n"
                        + "CLIENT MERGE SORT", QueryUtil.getExplainPlan(rs));

            query = "SELECT t_id,k1,k3 FROM " + tableName;
            rs = conn1.createStatement().executeQuery("EXPLAIN " + query);
            assertEquals(
                "CLIENT PARALLEL " + 3 + "-WAY RANGE SCAN OVER "
                        + indexPhysicalTableName
                        + " [2]\n" + "    SERVER FILTER BY FIRST KEY ONLY\n"
                        + "CLIENT MERGE SORT", QueryUtil.getExplainPlan(rs));

            rs = conn1.createStatement().executeQuery(query);
            Thread.sleep(1000);
            for (int j = 0; j < 26; j++) {
                assertTrue(rs.next());
                assertEquals(strings[j], rs.getString("t_id"));
                assertEquals(j, rs.getInt("k1"));
                assertEquals(j + 2, rs.getInt("k3"));
            }
       } finally {
            conn1.close();
        }
    }
}
