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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.coprocessor.BaseScannerRegionObserver;
import org.apache.phoenix.hbase.index.IndexRegionSplitPolicy;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTable.IndexType;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Maps;

public class LocalIndexIT extends BaseIndexIT {

    @BeforeClass 
    public static void doSetup() throws Exception {
        Map<String,String> props = Maps.newHashMapWithExpectedSize(3);
        // Drop the HBase table metadata for this test
        props.put(QueryServices.DROP_METADATA_ATTRIB, Boolean.toString(true));
        // Must update config before starting server
        setUpTestDriver(getUrl(), new ReadOnlyProps(props.entrySet().iterator()));
    }

    private void createBaseTable(String tableName, Integer saltBuckets, String splits) throws SQLException {
        Connection conn = DriverManager.getConnection(getUrl());
        String ddl = "CREATE TABLE " + tableName + " (t_id VARCHAR NOT NULL,\n" +
                "k1 INTEGER NOT NULL,\n" +
                "k2 INTEGER NOT NULL,\n" +
                "k3 INTEGER,\n" +
                "v1 VARCHAR,\n" +
                "CONSTRAINT pk PRIMARY KEY (t_id, k1, k2))\n"
                        + (saltBuckets != null && splits == null ? (",salt_buckets=" + saltBuckets) : "" 
                        + (saltBuckets == null && splits != null ? (" split on " + splits) : ""));
        conn.createStatement().execute(ddl);
        conn.close();
    }
    
    @Test
    public void testLocalIndexRoundTrip() throws Exception {
        createBaseTable(DATA_TABLE_NAME, null, null);
        Connection conn1 = DriverManager.getConnection(getUrl());
        Connection conn2 = DriverManager.getConnection(getUrl());
        conn1.createStatement().execute("CREATE LOCAL INDEX " + INDEX_TABLE_NAME + " ON " + DATA_TABLE_NAME + "(v1)");
        conn1.createStatement().executeQuery("SELECT * FROM " + DATA_TABLE_FULL_NAME).next();
        PTable localIndex = conn1.unwrap(PhoenixConnection.class).getMetaDataCache().getTable(new PTableKey(null,INDEX_TABLE_NAME));
        assertEquals(IndexType.LOCAL, localIndex.getIndexType());
        assertNotNull(localIndex.getViewIndexId());
    }
    
    @Test
    public void testLocalIndexCreationWithSplitsShouldFail() throws Exception {
        createBaseTable(DATA_TABLE_NAME, null, null);
        Connection conn1 = DriverManager.getConnection(getUrl());
        Connection conn2 = DriverManager.getConnection(getUrl());
        try {
            conn1.createStatement().execute("CREATE LOCAL INDEX " + INDEX_TABLE_NAME + " ON " + DATA_TABLE_NAME + "(v1)"+" split on (1,2,3)");
            fail("Local index cannot be pre-split");
        } catch (SQLException e) { }
        try {
            conn2.createStatement().executeQuery("SELECT * FROM " + DATA_TABLE_FULL_NAME).next();
            conn2.unwrap(PhoenixConnection.class).getMetaDataCache().getTable(new PTableKey(null,INDEX_TABLE_NAME));
            fail("Local index should not be created.");
        } catch (TableNotFoundException e) { }
    }

    @Test
    public void testLocalIndexCreationWithSaltingShouldFail() throws Exception {
        createBaseTable(DATA_TABLE_NAME, null, null);
        Connection conn1 = DriverManager.getConnection(getUrl());
        Connection conn2 = DriverManager.getConnection(getUrl());
        try {
            conn1.createStatement().execute("CREATE LOCAL INDEX " + INDEX_TABLE_NAME + " ON " + DATA_TABLE_NAME + "(v1)"+" salt_buckets=16");
            fail("Local index cannot be salted.");
        } catch (SQLException e) { }
        try {
            conn2.createStatement().executeQuery("SELECT * FROM " + DATA_TABLE_FULL_NAME).next();
            conn2.unwrap(PhoenixConnection.class).getMetaDataCache().getTable(new PTableKey(null,INDEX_TABLE_NAME));
            fail("Local index should not be created.");
        } catch (TableNotFoundException e) { }
    }

    @Test
    public void testLocalIndexOnTableWithImmutableRows() throws Exception {
        createBaseTable(DATA_TABLE_NAME, null, null);
        Connection conn1 = DriverManager.getConnection(getUrl());
        Connection conn2 = DriverManager.getConnection(getUrl());
        try {
            conn1.createStatement().execute("ALTER TABLE " + DATA_TABLE_NAME + " SET IMMUTABLE_ROWS=true");
            conn1.createStatement().execute("CREATE LOCAL INDEX " + INDEX_TABLE_NAME + " ON " + DATA_TABLE_NAME + "(v1)");
            fail("Local index aren't allowed on table with immutable rows");
        } catch (SQLException e) { }
        try {
            conn2.createStatement().executeQuery("SELECT * FROM " + DATA_TABLE_FULL_NAME).next();
            conn2.unwrap(PhoenixConnection.class).getMetaDataCache().getTable(new PTableKey(null,INDEX_TABLE_NAME));
            fail("Local index should not be created.");
        } catch (TableNotFoundException e) { }
    }

    @Test
    public void testLocalIndexTableRegionSplitPolicyAndSplitKeys() throws Exception {
        createBaseTable(DATA_TABLE_NAME, null,"('e','i','o')");
        Connection conn1 = DriverManager.getConnection(getUrl());
        Connection conn2 = DriverManager.getConnection(getUrl());
        conn1.createStatement().execute("CREATE LOCAL INDEX " + INDEX_TABLE_NAME + " ON " + DATA_TABLE_NAME + "(v1)");
        conn2.createStatement().executeQuery("SELECT * FROM " + DATA_TABLE_FULL_NAME).next();
        HBaseAdmin admin = driver.getConnectionQueryServices(getUrl(), TestUtil.TEST_PROPERTIES).getAdmin();
        HTableDescriptor htd = admin.getTableDescriptor(TableName.valueOf(MetaDataUtil.getLocalIndexTableName(DATA_TABLE_NAME)));
        assertEquals(IndexRegionSplitPolicy.class.getName(), htd.getValue(HTableDescriptor.SPLIT_POLICY));
        HTable userTable = new HTable(admin.getConfiguration(),TableName.valueOf(DATA_TABLE_NAME));
        HTable indexTable = new HTable(admin.getConfiguration(),TableName.valueOf(MetaDataUtil.getLocalIndexTableName(DATA_TABLE_NAME)));
        assertEquals("Both user region and index table should have same split keys.", userTable.getStartKeys(), indexTable.getStartKeys());
    }

    @Test
    public void testDropLocalIndexTable() throws Exception {
        createBaseTable(DATA_TABLE_NAME, null, null);
        Connection conn1 = DriverManager.getConnection(getUrl());
        Connection conn2 = DriverManager.getConnection(getUrl());
        conn1.createStatement().execute("CREATE LOCAL INDEX " + INDEX_TABLE_NAME + " ON " + DATA_TABLE_NAME + "(v1)");
        conn2.createStatement().executeQuery("SELECT * FROM " + DATA_TABLE_FULL_NAME).next();
        HBaseAdmin admin = driver.getConnectionQueryServices(getUrl(), TestUtil.TEST_PROPERTIES).getAdmin();
        assertTrue("Local index table should be present.", admin.tableExists(TableName.valueOf(MetaDataUtil.getLocalIndexTableName(DATA_TABLE_NAME))));
        conn1.createStatement().execute("DROP TABLE "+ DATA_TABLE_NAME);
        admin = driver.getConnectionQueryServices(getUrl(), TestUtil.TEST_PROPERTIES).getAdmin();
        assertFalse("Local index table should be deleted.", admin.tableExists(TableName.valueOf(MetaDataUtil.getLocalIndexTableName(DATA_TABLE_NAME))));
        ResultSet rs = conn2.createStatement().executeQuery("SELECT "
                + PhoenixDatabaseMetaData.SEQUENCE_SCHEMA + ","
                + PhoenixDatabaseMetaData.SEQUENCE_NAME
                + " FROM " + PhoenixDatabaseMetaData.SEQUENCE_TABLE_NAME);
        assertFalse("View index sequences should be deleted.", rs.next());
    }
    
    @Test
    public void testPutsToLocalIndexTable() throws Exception {
        createBaseTable(DATA_TABLE_NAME, null, "('e','i','o')");
        Connection conn1 = DriverManager.getConnection(getUrl());
        conn1.createStatement().execute("CREATE LOCAL INDEX " + INDEX_TABLE_NAME + " ON " + DATA_TABLE_NAME + "(v1)");
        conn1.createStatement().execute("UPSERT INTO "+DATA_TABLE_NAME+" values('b',1,2,4,'z')");
        conn1.createStatement().execute("UPSERT INTO "+DATA_TABLE_NAME+" values('f',1,2,3,'z')");
        conn1.createStatement().execute("UPSERT INTO "+DATA_TABLE_NAME+" values('j',2,4,2,'a')");
        conn1.createStatement().execute("UPSERT INTO "+DATA_TABLE_NAME+" values('q',3,1,1,'c')");
        conn1.commit();
        ResultSet rs = conn1.createStatement().executeQuery("SELECT COUNT(*) FROM " + INDEX_TABLE_NAME);
        assertTrue(rs.next());
        assertEquals(4, rs.getInt(1));
        HBaseAdmin admin = driver.getConnectionQueryServices(getUrl(), TestUtil.TEST_PROPERTIES).getAdmin();
        HTable indexTable = new HTable(admin.getConfiguration() ,TableName.valueOf(MetaDataUtil.getLocalIndexTableName(DATA_TABLE_NAME)));
        Pair<byte[][], byte[][]> startEndKeys = indexTable.getStartEndKeys();
        byte[][] startKeys = startEndKeys.getFirst();
        byte[][] endKeys = startEndKeys.getSecond();
        for (int i = 0; i < startKeys.length; i++) {
            Scan s = new Scan();
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
        createBaseTable(DATA_TABLE_NAME, null, "('e','i','o')");
        Connection conn1 = DriverManager.getConnection(getUrl());
        conn1.createStatement().execute("UPSERT INTO "+DATA_TABLE_NAME+" values('b',1,2,4,'z')");
        conn1.createStatement().execute("UPSERT INTO "+DATA_TABLE_NAME+" values('f',1,2,3,'z')");
        conn1.createStatement().execute("UPSERT INTO "+DATA_TABLE_NAME+" values('j',2,4,2,'a')");
        conn1.createStatement().execute("UPSERT INTO "+DATA_TABLE_NAME+" values('q',3,1,1,'c')");
        conn1.commit();
        conn1.createStatement().execute("CREATE LOCAL INDEX " + INDEX_TABLE_NAME + " ON " + DATA_TABLE_NAME + "(v1)");
        ResultSet rs = conn1.createStatement().executeQuery("SELECT COUNT(*) FROM " + INDEX_TABLE_NAME);
        assertTrue(rs.next());
        assertEquals(4, rs.getInt(1));
        HBaseAdmin admin = driver.getConnectionQueryServices(getUrl(), TestUtil.TEST_PROPERTIES).getAdmin();
        HTable indexTable = new HTable(admin.getConfiguration() ,TableName.valueOf(MetaDataUtil.getLocalIndexTableName(DATA_TABLE_NAME)));
        Pair<byte[][], byte[][]> startEndKeys = indexTable.getStartEndKeys();
        byte[][] startKeys = startEndKeys.getFirst();
        byte[][] endKeys = startEndKeys.getSecond();
        for (int i = 0; i < startKeys.length; i++) {
            Scan s = new Scan();
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
        createBaseTable(DATA_TABLE_NAME, null, "('e','i','o')");
        Connection conn1 = DriverManager.getConnection(getUrl());
        try{
            conn1.createStatement().execute("UPSERT INTO " + DATA_TABLE_NAME + " values('a',1,2,5,'y')");
            conn1.createStatement().execute("UPSERT INTO " + DATA_TABLE_NAME + " values('b',1,2,4,'z')");
            conn1.createStatement().execute("UPSERT INTO " + DATA_TABLE_NAME + " values('f',1,2,3,'a')");
            conn1.createStatement().execute("UPSERT INTO " + DATA_TABLE_NAME + " values('e',1,2,3,'b')");
            conn1.createStatement().execute("UPSERT INTO " + DATA_TABLE_NAME + " values('j',2,4,2,'a')");
            conn1.createStatement().execute("UPSERT INTO " + DATA_TABLE_NAME + " values('q',3,1,1,'c')");
            conn1.commit();
            conn1.createStatement().execute("CREATE LOCAL INDEX " + INDEX_TABLE_NAME + " ON " + DATA_TABLE_NAME + "(v1)");
            
            ResultSet rs = conn1.createStatement().executeQuery("SELECT COUNT(*) FROM " + INDEX_TABLE_NAME);
            assertTrue(rs.next());
            
            HBaseAdmin admin = driver.getConnectionQueryServices(getUrl(), TestUtil.TEST_PROPERTIES).getAdmin();
            int numRegions = admin.getTableRegions(TableName.valueOf(DATA_TABLE_NAME)).size();
            
            String query = "SELECT t_id, k1, k2,V1 FROM " + DATA_TABLE_NAME +" where v1='a'";
            rs = conn1.createStatement().executeQuery("EXPLAIN "+ query);
            
            assertEquals(
                "CLIENT PARALLEL " + numRegions + "-WAY RANGE SCAN OVER "
                        + MetaDataUtil.getLocalIndexTableName(DATA_TABLE_NAME) + " [-32768,'a']\nCLIENT MERGE SORT",
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
            query = "SELECT t_id, k1, k2,V1, k3 FROM " + DATA_TABLE_NAME +" where v1<='z' order by k3";
            rs = conn1.createStatement().executeQuery("EXPLAIN "+ query);
            
            assertEquals(
                "CLIENT PARALLEL " + numRegions + "-WAY RANGE SCAN OVER " + MetaDataUtil.getLocalIndexTableName(DATA_TABLE_NAME) + " [-32768,*] - [-32768,'z']\n" + 
                "    SERVER SORTED BY [K3]\n" +
                "CLIENT MERGE SORT", QueryUtil.getExplainPlan(rs));
 
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
            
            query = "SELECT t_id, k1, k2,v1 from " + DATA_TABLE_FULL_NAME + " order by V1,t_id";
            rs = conn1.createStatement().executeQuery("EXPLAIN " + query);
            
            assertEquals(
                "CLIENT PARALLEL " + numRegions + "-WAY RANGE SCAN OVER "
                        + MetaDataUtil.getLocalIndexTableName(DATA_TABLE_NAME)+" [-32768]\nCLIENT MERGE SORT",
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
        createBaseTable(DATA_TABLE_NAME, null, "('e','i','o')");
        Connection conn1 = DriverManager.getConnection(getUrl());
        try{
            conn1.createStatement().execute("UPSERT INTO " + DATA_TABLE_NAME + " values('b',1,2,4,'z')");
            conn1.createStatement().execute("UPSERT INTO " + DATA_TABLE_NAME + " values('f',1,2,3,'a')");
            conn1.createStatement().execute("UPSERT INTO " + DATA_TABLE_NAME + " values('j',2,4,2,'a')");
            conn1.createStatement().execute("UPSERT INTO " + DATA_TABLE_NAME + " values('q',3,1,1,'c')");
            conn1.commit();
            conn1.createStatement().execute("CREATE LOCAL INDEX " + INDEX_TABLE_NAME + " ON " + DATA_TABLE_NAME + "(v1)");
            
            ResultSet rs = conn1.createStatement().executeQuery("SELECT COUNT(*) FROM " + INDEX_TABLE_NAME);
            assertTrue(rs.next());
            
            HBaseAdmin admin = driver.getConnectionQueryServices(getUrl(), TestUtil.TEST_PROPERTIES).getAdmin();
            int numRegions = admin.getTableRegions(TableName.valueOf(DATA_TABLE_NAME)).size();
            
            String query = "SELECT t_id, k1, k2, k3, V1 FROM " + DATA_TABLE_NAME +" where v1='a'";
            rs = conn1.createStatement().executeQuery("EXPLAIN "+ query);
            
            assertEquals(
                "CLIENT PARALLEL " + numRegions + "-WAY RANGE SCAN OVER "
                        + MetaDataUtil.getLocalIndexTableName(DATA_TABLE_NAME) + " [-32768,'a']\nCLIENT MERGE SORT",
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
            
            query = "SELECT t_id, k1, k2, k3, V1 from " + DATA_TABLE_FULL_NAME + "  where v1<='z' order by V1,t_id";
            rs = conn1.createStatement().executeQuery("EXPLAIN " + query);
            
            assertEquals(
                "CLIENT PARALLEL " + numRegions + "-WAY RANGE SCAN OVER "
                        + MetaDataUtil.getLocalIndexTableName(DATA_TABLE_NAME)+" [-32768,*] - [-32768,'z']\nCLIENT MERGE SORT",
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
            
            query = "SELECT t_id, V1, k3 from " + DATA_TABLE_FULL_NAME + "  where v1 <='z' group by v1,t_id, k3";
            rs = conn1.createStatement().executeQuery("EXPLAIN " + query);
            
            assertEquals(
                "CLIENT PARALLEL " + numRegions + "-WAY RANGE SCAN OVER "
                        + MetaDataUtil.getLocalIndexTableName(DATA_TABLE_NAME)+" [-32768,*] - [-32768,'z']\n"
                        + "    SERVER AGGREGATE INTO DISTINCT ROWS BY [V1, T_ID, K3]\n" + "CLIENT MERGE SORT",
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
            
            query = "SELECT v1,sum(k3) from " + DATA_TABLE_FULL_NAME + " where v1 <='z'  group by v1 order by v1";
            PhoenixPreparedStatement statement = conn1.prepareStatement(query).unwrap(PhoenixPreparedStatement.class);
            QueryPlan plan = statement.compileQuery("EXPLAIN " + query);
            assertTrue(query, plan.getContext().getScan().getAttribute(BaseScannerRegionObserver.KEY_ORDERED_GROUP_BY_EXPRESSIONS) == null);
            assertTrue(query, plan.getContext().getScan().getAttribute(BaseScannerRegionObserver.UNORDERED_GROUP_BY_EXPRESSIONS) != null);
            
            rs = conn1.createStatement().executeQuery("EXPLAIN " + query);
            assertEquals(
                "CLIENT PARALLEL " + numRegions + "-WAY RANGE SCAN OVER "
                        + MetaDataUtil.getLocalIndexTableName(DATA_TABLE_NAME)+" [-32768,*] - [-32768,'z']\n"
                        + "    SERVER AGGREGATE INTO ORDERED DISTINCT ROWS BY [V1]\nCLIENT MERGE SORT",
                QueryUtil.getExplainPlan(rs));
            
            rs = conn1.createStatement().executeQuery(query);
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
        createBaseTable(DATA_TABLE_NAME, null, "('e','i','o')");
        Connection conn1 = DriverManager.getConnection(getUrl());
        conn1.createStatement().execute("UPSERT INTO "+DATA_TABLE_NAME+" values('b',1,2,4,'z')");
        conn1.createStatement().execute("UPSERT INTO "+DATA_TABLE_NAME+" values('f',1,2,3,'a')");
        conn1.createStatement().execute("UPSERT INTO "+DATA_TABLE_NAME+" values('j',2,4,3,'a')");
        conn1.createStatement().execute("UPSERT INTO "+DATA_TABLE_NAME+" values('q',3,1,1,'c')");
        conn1.commit();
        conn1.createStatement().execute("CREATE LOCAL INDEX " + INDEX_TABLE_NAME + " ON " + DATA_TABLE_NAME + "(v1)");
        conn1.createStatement().execute("CREATE INDEX " + INDEX_TABLE_NAME + "2" + " ON " + DATA_TABLE_NAME + "(v1)");
        String query = "SELECT t_id, k1, k2,V1 FROM " + DATA_TABLE_NAME +" where v1='a'";
        ResultSet rs1 = conn1.createStatement().executeQuery("EXPLAIN "+ query);
        assertEquals("CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + INDEX_TABLE_NAME + "2" + " ['a']",QueryUtil.getExplainPlan(rs1));
        conn1.close();
    }

    @Test
    public void testDropLocalIndexShouldDeleteDataFromLocalIndexTable() throws Exception {
        createBaseTable(DATA_TABLE_NAME, null, "('e','i','o')");
        Connection conn1 = DriverManager.getConnection(getUrl());
        try {
            conn1.createStatement().execute("UPSERT INTO " + DATA_TABLE_NAME + " values('b',1,2,4,'z')");
            conn1.createStatement().execute("UPSERT INTO " + DATA_TABLE_NAME + " values('f',1,2,3,'a')");
            conn1.createStatement().execute("UPSERT INTO " + DATA_TABLE_NAME + " values('j',2,4,2,'a')");
            conn1.createStatement().execute("UPSERT INTO " + DATA_TABLE_NAME + " values('q',3,1,1,'c')");
            conn1.commit();
            conn1.createStatement().execute("CREATE LOCAL INDEX " + INDEX_TABLE_NAME + " ON " + DATA_TABLE_NAME + "(v1)");
            conn1.createStatement().execute("DROP INDEX " + INDEX_TABLE_NAME + " ON " + DATA_TABLE_NAME);
            HBaseAdmin admin = driver.getConnectionQueryServices(getUrl(), TestUtil.TEST_PROPERTIES).getAdmin();
            HTable indexTable = new HTable(admin.getConfiguration() ,TableName.valueOf(MetaDataUtil.getLocalIndexTableName(DATA_TABLE_NAME)));
            Pair<byte[][], byte[][]> startEndKeys = indexTable.getStartEndKeys();
            byte[][] startKeys = startEndKeys.getFirst();
            byte[][] endKeys = startEndKeys.getSecond();
            // No entry should be present in local index table after drop index.
            for (int i = 0; i < startKeys.length; i++) {
                Scan s = new Scan();
                s.setStartRow(startKeys[i]);
                s.setStopRow(endKeys[i]);
                ResultScanner scanner = indexTable.getScanner(s);
                int count = 0;
                for(Result r:scanner){
                    count++;
                }
                scanner.close();
                assertEquals(0, count);
            }
            indexTable.close();
        } finally {
            conn1.close();
        }
    }

    @Test
    public void testLocalIndexRowsShouldBeDeletedWhenUserTableRowsDeleted() throws Exception {
        createBaseTable(DATA_TABLE_NAME, null, "('e','i','o')");
        Connection conn1 = DriverManager.getConnection(getUrl());
        try {
            conn1.createStatement().execute("UPSERT INTO " + DATA_TABLE_NAME + " values('b',1,2,4,'z')");
            conn1.createStatement().execute("UPSERT INTO " + DATA_TABLE_NAME + " values('f',1,2,3,'a')");
            conn1.createStatement().execute("UPSERT INTO " + DATA_TABLE_NAME + " values('j',2,4,2,'a')");
            conn1.createStatement().execute("UPSERT INTO " + DATA_TABLE_NAME + " values('q',3,1,1,'c')");
            conn1.commit();
            conn1.createStatement().execute("CREATE LOCAL INDEX " + INDEX_TABLE_NAME + " ON " + DATA_TABLE_NAME + "(v1)");
            conn1.createStatement().execute("DELETE FROM " + DATA_TABLE_NAME + " where v1='a'");
            conn1.commit();
            conn1 = DriverManager.getConnection(getUrl());
            ResultSet rs = conn1.createStatement().executeQuery("SELECT COUNT(*) FROM " + INDEX_TABLE_NAME);
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
        } finally {
            conn1.close();
        }
    }
    
    @Test
    public void testScanWhenATableHasMultipleLocalIndexes() throws Exception {
        createBaseTable(DATA_TABLE_NAME, null, "('e','i','o')");
        Connection conn1 = DriverManager.getConnection(getUrl());
        try {
            conn1.createStatement().execute("UPSERT INTO " + DATA_TABLE_NAME + " values('b',1,2,4,'z')");
            conn1.createStatement().execute("UPSERT INTO " + DATA_TABLE_NAME + " values('f',1,2,3,'a')");
            conn1.createStatement().execute("UPSERT INTO " + DATA_TABLE_NAME + " values('j',2,4,2,'a')");
            conn1.createStatement().execute("UPSERT INTO " + DATA_TABLE_NAME + " values('q',3,1,1,'c')");
            conn1.commit();
            conn1.createStatement().execute("CREATE LOCAL INDEX " + INDEX_TABLE_NAME + " ON " + DATA_TABLE_NAME + "(v1)");
            conn1.createStatement().execute("CREATE LOCAL INDEX " + INDEX_TABLE_NAME + "2 ON " + DATA_TABLE_NAME + "(k3)");
            conn1.commit();
            conn1 = DriverManager.getConnection(getUrl());
            ResultSet rs = conn1.createStatement().executeQuery("SELECT COUNT(*) FROM " + DATA_TABLE_NAME);
            assertTrue(rs.next());
            assertEquals(4, rs.getInt(1));
        } finally {
            conn1.close();
        }
    }

    @Test
    public void testLocalIndexScanWithInList() throws Exception {
        createBaseTable(DATA_TABLE_NAME, null, "('e','i','o')");
        Connection conn1 = DriverManager.getConnection(getUrl());
        try{
            conn1.createStatement().execute("UPSERT INTO " + DATA_TABLE_NAME + " values('b',1,2,4,'z')");
            conn1.createStatement().execute("UPSERT INTO " + DATA_TABLE_NAME + " values('f',1,2,3,'a')");
            conn1.createStatement().execute("UPSERT INTO " + DATA_TABLE_NAME + " values('j',2,4,2,'a')");
            conn1.createStatement().execute("UPSERT INTO " + DATA_TABLE_NAME + " values('q',3,1,1,'c')");
            conn1.commit();
            conn1.createStatement().execute("CREATE LOCAL INDEX " + INDEX_TABLE_NAME + " ON " + DATA_TABLE_NAME + "(v1) include (k3)");
            
            ResultSet rs = conn1.createStatement().executeQuery("SELECT COUNT(*) FROM " + INDEX_TABLE_NAME);
            assertTrue(rs.next());
            
            HBaseAdmin admin = driver.getConnectionQueryServices(getUrl(), TestUtil.TEST_PROPERTIES).getAdmin();
            
            String query = "SELECT t_id FROM " + DATA_TABLE_NAME +" where (v1,k3) IN (('z',4),('a',2))";
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
    public void testLocalIndexScanAfterRegionSplit() throws Exception {
        createBaseTable(DATA_TABLE_NAME, null, "('e','j','o')");
        Connection conn1 = DriverManager.getConnection(getUrl());
        try{
            String[] strings = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t","u","v","w","x","y","z"};
            for (int i = 0; i < 26; i++) {
                conn1.createStatement().execute(
                    "UPSERT INTO " + DATA_TABLE_NAME + " values('"+strings[i]+"'," + i + ","
                            + (i + 1) + "," + (i + 2) + ",'" + strings[25 - i] + "')");
            }
            conn1.commit();
            conn1.createStatement().execute("CREATE LOCAL INDEX " + INDEX_TABLE_NAME + " ON " + DATA_TABLE_NAME + "(v1)");
            conn1.createStatement().execute("CREATE LOCAL INDEX " + INDEX_TABLE_NAME + "_2 ON " + DATA_TABLE_NAME + "(k3)");

            ResultSet rs = conn1.createStatement().executeQuery("SELECT * FROM " + DATA_TABLE_NAME);
            assertTrue(rs.next());
            
            HBaseAdmin admin = driver.getConnectionQueryServices(getUrl(), TestUtil.TEST_PROPERTIES).getAdmin();
            HMaster master = getUtility().getHBaseCluster().getMaster();
            for (int i = 1; i < 5; i++) {
                
                admin.split(Bytes.toBytes(DATA_TABLE_NAME), ByteUtil.concat(Bytes.toBytes(strings[3*i])));
                List<HRegionInfo> regionsOfUserTable =
                        master.getAssignmentManager().getRegionStates().getRegionsOfTable(TableName.valueOf(DATA_TABLE_NAME));

                while (regionsOfUserTable.size() != (4+i)) {
                    Thread.sleep(100);
                    regionsOfUserTable = master.getAssignmentManager().getRegionStates().getRegionsOfTable(TableName.valueOf(DATA_TABLE_NAME));
                }
                assertEquals(4+i, regionsOfUserTable.size());
                List<HRegionInfo> regionsOfIndexTable = master.getAssignmentManager().getRegionStates()
                                .getRegionsOfTable(TableName.valueOf(MetaDataUtil.getLocalIndexTableName(DATA_TABLE_NAME)));
                while (regionsOfIndexTable.size() != (4+i)) {
                    Thread.sleep(100);
                    regionsOfIndexTable = master.getAssignmentManager().getRegionStates()
                            .getRegionsOfTable(TableName.valueOf(MetaDataUtil.getLocalIndexTableName(DATA_TABLE_NAME)));
                }
                assertEquals(4 + i, regionsOfIndexTable.size());
                String query = "SELECT t_id,k1,v1 FROM " + DATA_TABLE_NAME;
                rs = conn1.createStatement().executeQuery("EXPLAIN "+query);
                assertEquals(
                    "CLIENT PARALLEL " + (4+i) + "-WAY RANGE SCAN OVER "
                            + MetaDataUtil.getLocalIndexTableName(DATA_TABLE_NAME)+" [-32768]\n"+
                            "CLIENT MERGE SORT",
                    QueryUtil.getExplainPlan(rs));
                rs = conn1.createStatement().executeQuery(query);
                Thread.sleep(1000);
                for (int j = 0; j < 26; j++) {
                    assertTrue(rs.next());
                    assertEquals(strings[25-j], rs.getString("t_id"));
                    assertEquals(25-j, rs.getInt("k1"));
                    assertEquals(strings[j], rs.getString("V1"));
                }
                
                query = "SELECT t_id,k1,k3 FROM " + DATA_TABLE_NAME;
                rs = conn1.createStatement().executeQuery("EXPLAIN "+query);
                assertEquals(
                    "CLIENT PARALLEL " + (4+i) + "-WAY RANGE SCAN OVER "
                            + MetaDataUtil.getLocalIndexTableName(DATA_TABLE_NAME)+" [-32767]\n"+
                            "CLIENT MERGE SORT",
                    QueryUtil.getExplainPlan(rs));
                rs = conn1.createStatement().executeQuery(query);
                Thread.sleep(1000);
                for (int j = 0; j < 26; j++) {
                    assertTrue(rs.next());
                    assertEquals(strings[j], rs.getString("t_id"));
                    assertEquals(j, rs.getInt("k1"));
                    assertEquals(j+2, rs.getInt("k3"));
                }
            }
       } finally {
            conn1.close();
        }
    }
}
