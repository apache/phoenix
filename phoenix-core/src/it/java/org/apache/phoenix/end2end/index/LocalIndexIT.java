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
import java.util.Map;

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.hbase.index.IndexRegionSplitPolicy;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTable.IndexType;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.phoenix.util.MetaDataUtil;
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
        startServer(getUrl(), new ReadOnlyProps(props.entrySet().iterator()));
    }

    private void createBaseTable(String tableName, Integer saltBuckets, String splits) throws SQLException {
        Connection conn = DriverManager.getConnection(getUrl());
        String ddl = "CREATE TABLE " + tableName + " (t_id VARCHAR NOT NULL,\n" +
                "k1 INTEGER NOT NULL,\n" +
                "k2 INTEGER NOT NULL,\n" +
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
        conn2.createStatement().executeQuery("SELECT * FROM " + DATA_TABLE_FULL_NAME).next();
        PTable localIndex = conn2.unwrap(PhoenixConnection.class).getMetaDataCache().getTable(new PTableKey(null,INDEX_TABLE_NAME));
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
        conn1.createStatement().execute("UPSERT INTO "+DATA_TABLE_NAME+" values('b',1,2,'z')");
        conn1.createStatement().execute("UPSERT INTO "+DATA_TABLE_NAME+" values('f',1,2,'z')");
        conn1.createStatement().execute("UPSERT INTO "+DATA_TABLE_NAME+" values('j',2,4,'a')");
        conn1.createStatement().execute("UPSERT INTO "+DATA_TABLE_NAME+" values('q',3,1,'c')");
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
        conn1.createStatement().execute("UPSERT INTO "+DATA_TABLE_NAME+" values('b',1,2,'z')");
        conn1.createStatement().execute("UPSERT INTO "+DATA_TABLE_NAME+" values('f',1,2,'z')");
        conn1.createStatement().execute("UPSERT INTO "+DATA_TABLE_NAME+" values('j',2,4,'a')");
        conn1.createStatement().execute("UPSERT INTO "+DATA_TABLE_NAME+" values('q',3,1,'c')");
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
}
