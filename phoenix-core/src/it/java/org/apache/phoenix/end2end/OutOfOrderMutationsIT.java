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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Properties;

import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.util.IndexScrutiny;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.Ignore;
import org.junit.Test;

public class OutOfOrderMutationsIT extends ParallelStatsDisabledIT {
    @Test
    public void testOutOfOrderDelete() throws Exception {
        String tableName = generateUniqueName();
        String indexName = generateUniqueName();
        Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
        long ts = 1000;
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        Connection conn = DriverManager.getConnection(getUrl(), props);     
        conn.createStatement().execute("CREATE TABLE " + tableName + "(k1 CHAR(2) NOT NULL, k2 CHAR(2) NOT NULL, ts TIMESTAMP, CONSTRAINT pk PRIMARY KEY (k1,k2)) COLUMN_ENCODED_BYTES = 0");
        conn.close();

        ts = 1010;
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute("CREATE INDEX " + indexName + " ON " + tableName + "(k2,k1,ts)");
        conn.close();
        
        ts = 1020;
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        conn = DriverManager.getConnection(getUrl(), props);        
        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + tableName + " VALUES('aa','aa',?)");
        stmt.setTimestamp(1, new Timestamp(1000L));
        stmt.executeUpdate();
        conn.commit();
        conn.close();
        
        ts = 1040;
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute("DELETE FROM " + tableName + " WHERE k1='aa'");
        conn.commit();
        conn.close();
        
        ts = 1030;
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        conn = DriverManager.getConnection(getUrl(), props);
        stmt = conn.prepareStatement("UPSERT INTO " + tableName + " VALUES('aa','aa',?)");
        stmt.setTimestamp(1, new Timestamp(2000L));
        stmt.executeUpdate();
        conn.commit();
        conn.close();
        
        ts = 1050;
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        conn = DriverManager.getConnection(getUrl(), props);
        
        IndexScrutiny.scrutinizeIndex(conn, tableName, indexName);        
        assertNoTimeStampAt(conn, indexName, 1030);
        conn.close();
        
        /**
         *
            ************ dumping T000001;hconnection-0x2b137c55 **************
            aaaa/0:/1040/DeleteFamily/vlen=0/seqid=0
            aaaa/0:TS/1030/Put/vlen=12/seqid=0
            aaaa/0:TS/1020/Put/vlen=12/seqid=0
            aaaa/0:_0/1030/Put/vlen=1/seqid=0
            aaaa/0:_0/1020/Put/vlen=1/seqid=0
            -----------------------------------------------
            ************ dumping T000002;hconnection-0x2b137c55 **************
            aaaa\xC2\x0B/0:/1040/DeleteFamily/vlen=0/seqid=0
            aaaa\xC2\x0B/0:_0/1020/Put/vlen=2/seqid=0
            -----------------------------------------------
         */
    }

    private static void assertNoTimeStampAt(Connection conn, String tableName, long ts) throws SQLException, IOException {
        Scan scan = new Scan();
        scan.setRaw(true);
        scan.setMaxVersions();
        HTableInterface indexHTable = conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(Bytes.toBytes(tableName));
        ResultScanner scanner = indexHTable.getScanner(scan);
        Result result;
        while ((result = scanner.next()) != null) {
            CellScanner cellScanner = result.cellScanner();
            while (cellScanner.advance()) {
                assertNotEquals(ts, cellScanner.current().getTimestamp());
            }
        }
    }
    
    @Test
    public void testOutOfOrderUpsert() throws Exception {
        String tableName = generateUniqueName();
        String indexName = generateUniqueName();
        Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
        long ts = 1000;
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        Connection conn = DriverManager.getConnection(getUrl(), props);     
        conn.createStatement().execute("CREATE TABLE " + tableName + "(k1 CHAR(2) NOT NULL, k2 CHAR(2) NOT NULL, ts TIMESTAMP, CONSTRAINT pk PRIMARY KEY (k1,k2)) COLUMN_ENCODED_BYTES = 0");
        conn.close();

        ts = 1010;
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute("CREATE INDEX " + indexName + " ON " + tableName + "(k2,k1,ts)");
        conn.close();
        
        ts = 1020;
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        conn = DriverManager.getConnection(getUrl(), props);        
        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + tableName + " VALUES('aa','aa',?)");
        stmt.setTimestamp(1, new Timestamp(1000L));
        stmt.executeUpdate();
        conn.commit();
        conn.close();
        
        ts = 1040;
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        conn = DriverManager.getConnection(getUrl(), props);
        stmt = conn.prepareStatement("UPSERT INTO " + tableName + " VALUES('aa','aa',?)");
        Timestamp expectedTimestamp = new Timestamp(3000L);
        stmt.setTimestamp(1, expectedTimestamp);
        stmt.executeUpdate();
        conn.commit();
        conn.close();
        
        ts = 1030;
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        conn = DriverManager.getConnection(getUrl(), props);
        stmt = conn.prepareStatement("UPSERT INTO " + tableName + " VALUES('aa','aa',?)");
        stmt.setTimestamp(1, new Timestamp(2000L));
        stmt.executeUpdate();
        conn.commit();
        conn.close();
        
        ts = 1050;
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        conn = DriverManager.getConnection(getUrl(), props);
        
        IndexScrutiny.scrutinizeIndex(conn, tableName, indexName);        
        
        ResultSet rs = conn.createStatement().executeQuery("SELECT /*+ NO_INDEX */ ts FROM " + tableName);
        assertTrue(rs.next());
        assertEquals(expectedTimestamp, rs.getTimestamp(1));
        assertFalse(rs.next());
        
        rs = conn.createStatement().executeQuery("SELECT \"0:TS\" FROM " + indexName);
        assertTrue(rs.next());
        assertEquals(expectedTimestamp, rs.getTimestamp(1));
        assertFalse(rs.next());

        assertNoTimeStampAt(conn, indexName, 1030);
        conn.close();

        /**
         *
            ************ dumping T000001;hconnection-0x3e7a21d **************
            aaaa/0:TS/1040/Put/vlen=12/seqid=0
            aaaa/0:TS/1030/Put/vlen=12/seqid=0
            aaaa/0:TS/1020/Put/vlen=12/seqid=0
            aaaa/0:_0/1040/Put/vlen=1/seqid=0
            aaaa/0:_0/1030/Put/vlen=1/seqid=0
            aaaa/0:_0/1020/Put/vlen=1/seqid=0
            -----------------------------------------------
            ************ dumping T000002;hconnection-0x3e7a21d **************
            aaaa\xC2\x0B/0:/1040/DeleteFamily/vlen=0/seqid=0
            aaaa\xC2\x0B/0:_0/1020/Put/vlen=2/seqid=0
            aaaa\xC2\x1F/0:_0/1040/Put/vlen=2/seqid=0
            -----------------------------------------------
         */
    }

    private static long getRowCount(Connection conn, String tableName) throws SQLException {
        ResultSet rs = conn.createStatement().executeQuery("SELECT /*+ NO_INDEX */ count(*) FROM " + tableName);
        assertTrue(rs.next());
        return rs.getLong(1);
    }

    @Test
    public void testSetIndexedColumnToNull() throws Exception {
        String tableName = generateUniqueName();
        String indexName = generateUniqueName();
        Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
        long ts = 1000;
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        Connection conn = DriverManager.getConnection(getUrl(), props);     
        conn.createStatement().execute("CREATE TABLE " + tableName + "(k1 CHAR(2) NOT NULL, k2 CHAR(2) NOT NULL, ts TIMESTAMP, CONSTRAINT pk PRIMARY KEY (k1,k2)) COLUMN_ENCODED_BYTES = 0");
        conn.close();

        ts = 1010;
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute("CREATE INDEX " + indexName + " ON " + tableName + "(k2,k1,ts)");
        conn.close();
        
        ts = 1020;
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        conn = DriverManager.getConnection(getUrl(), props);        
        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + tableName + " VALUES('aa','aa',?)");
        stmt.setTimestamp(1, new Timestamp(1000L));
        stmt.executeUpdate();
        conn.commit();
        conn.close();
        
        ts = 1040;
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        conn = DriverManager.getConnection(getUrl(), props);
        stmt = conn.prepareStatement("UPSERT INTO " + tableName + " VALUES('aa','aa', ?)");
        Timestamp expectedTimestamp = new Timestamp(3000L);
        stmt.setTimestamp(1, expectedTimestamp);
        stmt.executeUpdate();
        conn.commit();
        conn.close();
        
        ts = 1030;
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        conn = DriverManager.getConnection(getUrl(), props);
        stmt = conn.prepareStatement("UPSERT INTO " + tableName + " VALUES('aa','aa',null)");
        stmt.executeUpdate();
        conn.commit();
        conn.close();
        
        ts = 1050;
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        conn = DriverManager.getConnection(getUrl(), props);
        
        TestUtil.dumpTable(conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(Bytes.toBytes(tableName)));
        TestUtil.dumpTable(conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(Bytes.toBytes(indexName)));

        IndexScrutiny.scrutinizeIndex(conn, tableName, indexName);        
        
        ResultSet rs = conn.createStatement().executeQuery("SELECT /*+ NO_INDEX */ ts FROM " + tableName);
        assertTrue(rs.next());
        assertEquals(expectedTimestamp, rs.getTimestamp(1));
        assertFalse(rs.next());
        
        rs = conn.createStatement().executeQuery("SELECT \"0:TS\" FROM " + indexName);
        assertTrue(rs.next());
        assertEquals(expectedTimestamp, rs.getTimestamp(1));
        assertFalse(rs.next());
        conn.close();
    }

    @Test
    public void testSetIndexedColumnToNull2() throws Exception {
        String tableName = generateUniqueName();
        String indexName = generateUniqueName();
        Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
        long ts = 1000;
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        Connection conn = DriverManager.getConnection(getUrl(), props);     
        conn.createStatement().execute("CREATE TABLE " + tableName + "(k1 CHAR(2) NOT NULL, k2 CHAR(2) NOT NULL, ts TIMESTAMP, CONSTRAINT pk PRIMARY KEY (k1,k2)) COLUMN_ENCODED_BYTES = 0");
        conn.close();

        ts = 1010;
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute("CREATE INDEX " + indexName + " ON " + tableName + "(k2,k1,ts)");
        conn.close();
        
        ts = 1020;
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        conn = DriverManager.getConnection(getUrl(), props);        
        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + tableName + " VALUES('aa','aa',?)");
        stmt.setTimestamp(1, new Timestamp(1000L));
        stmt.executeUpdate();
        conn.commit();
        conn.close();
        
        ts = 1040;
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        conn = DriverManager.getConnection(getUrl(), props);
        stmt = conn.prepareStatement("UPSERT INTO " + tableName + " VALUES('aa','aa',?)");
        Timestamp expectedTimestamp = null;
        stmt.setTimestamp(1, expectedTimestamp);
        stmt.executeUpdate();
        conn.commit();
        conn.close();
        
        ts = 1030;
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        conn = DriverManager.getConnection(getUrl(), props);
        stmt = conn.prepareStatement("UPSERT INTO " + tableName + " VALUES('aa','aa', ?)");
        stmt.setTimestamp(1, new Timestamp(3000L));
        stmt.executeUpdate();
        conn.commit();
        conn.close();
        
        ts = 1050;
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        conn = DriverManager.getConnection(getUrl(), props);
        
        TestUtil.dumpTable(conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(Bytes.toBytes(tableName)));
        TestUtil.dumpTable(conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(Bytes.toBytes(indexName)));

        IndexScrutiny.scrutinizeIndex(conn, tableName, indexName);        
        
        ResultSet rs = conn.createStatement().executeQuery("SELECT /*+ NO_INDEX */ ts FROM " + tableName);
        assertTrue(rs.next());
        assertEquals(expectedTimestamp, rs.getTimestamp(1));
        assertFalse(rs.next());
        
        rs = conn.createStatement().executeQuery("SELECT \"0:TS\" FROM " + indexName);
        assertTrue(rs.next());
        assertEquals(expectedTimestamp, rs.getTimestamp(1));
        assertFalse(rs.next());
        conn.close();
    }    
    
    @Test
    @Ignore("PHOENIX-4058 Generate correct index updates when DeleteColumn processed before Put with same timestamp")
    public void testSetIndexedColumnToNullAndValueAtSameTS() throws Exception {
        String tableName = generateUniqueName();
        String indexName = generateUniqueName();
        Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
        long ts = 1000;
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        Connection conn = DriverManager.getConnection(getUrl(), props);     
        conn.createStatement().execute("CREATE TABLE " + tableName + "(k1 CHAR(2) NOT NULL, k2 CHAR(2) NOT NULL, ts TIMESTAMP, V VARCHAR, V2 VARCHAR, CONSTRAINT pk PRIMARY KEY (k1,k2)) COLUMN_ENCODED_BYTES = 0");
        conn.close();

        ts = 1010;
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute("CREATE INDEX " + indexName + " ON " + tableName + "(k2,k1,ts) INCLUDE (V, v2)");
        conn.close();
        
        ts = 1020;
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        conn = DriverManager.getConnection(getUrl(), props);        
        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + tableName + " VALUES('aa','aa',?, '0')");
        stmt.setTimestamp(1, new Timestamp(1000L));
        stmt.executeUpdate();
        conn.commit();
        conn.close();
        
        Timestamp expectedTimestamp;
        ts = 1040;
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        conn = DriverManager.getConnection(getUrl(), props);
        stmt = conn.prepareStatement("UPSERT INTO " + tableName + " VALUES('aa','aa',?, null)");
        expectedTimestamp = null;
        stmt.setTimestamp(1, expectedTimestamp);
        stmt.executeUpdate();
        conn.commit();
        stmt.setTimestamp(1, new Timestamp(3000L));
        stmt.executeUpdate();
        conn.commit();
        conn.close();
        
        ts = 1050;
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        conn = DriverManager.getConnection(getUrl(), props);
        
        TestUtil.dumpTable(conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(Bytes.toBytes(tableName)));
        TestUtil.dumpTable(conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(Bytes.toBytes(indexName)));

        IndexScrutiny.scrutinizeIndex(conn, tableName, indexName);        
        
        ResultSet rs = conn.createStatement().executeQuery("SELECT /*+ NO_INDEX */ ts,v FROM " + tableName);
        assertTrue(rs.next());
        assertEquals(expectedTimestamp, rs.getTimestamp(1));
        assertEquals(null, rs.getString(2));
        assertFalse(rs.next());
        
        rs = conn.createStatement().executeQuery("SELECT \"0:TS\", \"0:V\" FROM " + indexName);
        assertTrue(rs.next());
        assertEquals(expectedTimestamp, rs.getTimestamp(1));
        assertEquals(null, rs.getString(2));
        assertFalse(rs.next());
        
        conn.close();
    }

    @Test
    public void testSetIndexedColumnToNullAndValueAtSameTSWithStoreNulls1() throws Exception {
        String tableName = generateUniqueName();
        String indexName = generateUniqueName();
        Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
        long ts = 1000;
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        Connection conn = DriverManager.getConnection(getUrl(), props);     
        conn.createStatement().execute("CREATE TABLE " + tableName + "(k1 CHAR(2) NOT NULL, k2 CHAR(2) NOT NULL, ts TIMESTAMP, V VARCHAR, V2 VARCHAR, CONSTRAINT pk PRIMARY KEY (k1,k2)) COLUMN_ENCODED_BYTES = 0, STORE_NULLS=true");
        conn.close();

        ts = 1010;
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute("CREATE INDEX " + indexName + " ON " + tableName + "(k2,k1,ts) INCLUDE (V, v2)");
        conn.close();
        
        ts = 1020;
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        conn = DriverManager.getConnection(getUrl(), props);        
        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + tableName + " VALUES('aa','aa',?, '0')");
        stmt.setTimestamp(1, new Timestamp(1000L));
        stmt.executeUpdate();
        conn.commit();
        conn.close();
        
        Timestamp expectedTimestamp;
        ts = 1040;
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        conn = DriverManager.getConnection(getUrl(), props);
        stmt = conn.prepareStatement("UPSERT INTO " + tableName + " VALUES('aa','aa',?, null)");
        expectedTimestamp = null;
        stmt.setTimestamp(1, expectedTimestamp);
        stmt.executeUpdate();
        conn.commit();
        expectedTimestamp = new Timestamp(3000L);
        stmt.setTimestamp(1, expectedTimestamp);
        stmt.executeUpdate();
        conn.commit();
        conn.close();
        
        ts = 1050;
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        conn = DriverManager.getConnection(getUrl(), props);
        
        TestUtil.dumpTable(conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(Bytes.toBytes(tableName)));
        TestUtil.dumpTable(conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(Bytes.toBytes(indexName)));

        IndexScrutiny.scrutinizeIndex(conn, tableName, indexName);        
        
        ResultSet rs = conn.createStatement().executeQuery("SELECT /*+ NO_INDEX */ ts,v FROM " + tableName);
        assertTrue(rs.next());
        assertEquals(expectedTimestamp, rs.getTimestamp(1));
        assertEquals(null, rs.getString(2));
        assertFalse(rs.next());
        
        rs = conn.createStatement().executeQuery("SELECT \"0:TS\", \"0:V\" FROM " + indexName);
        assertTrue(rs.next());
        assertEquals(expectedTimestamp, rs.getTimestamp(1));
        assertEquals(null, rs.getString(2));
        assertFalse(rs.next());
        
        conn.close();
    }
    
    @Test
    public void testSetIndexedColumnToNullAndValueAtSameTSWithStoreNulls2() throws Exception {
        String tableName = generateUniqueName();
        String indexName = generateUniqueName();
        Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
        long ts = 1000;
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        Connection conn = DriverManager.getConnection(getUrl(), props);     
        conn.createStatement().execute("CREATE TABLE " + tableName + "(k1 CHAR(2) NOT NULL, k2 CHAR(2) NOT NULL, ts TIMESTAMP, V VARCHAR, V2 VARCHAR, CONSTRAINT pk PRIMARY KEY (k1,k2)) COLUMN_ENCODED_BYTES = 0, STORE_NULLS=true");
        conn.close();

        ts = 1010;
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute("CREATE INDEX " + indexName + " ON " + tableName + "(k2,k1,ts) INCLUDE (V, v2)");
        conn.close();
        
        ts = 1020;
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        conn = DriverManager.getConnection(getUrl(), props);        
        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + tableName + " VALUES('aa','aa',?, '0')");
        stmt.setTimestamp(1, new Timestamp(1000L));
        stmt.executeUpdate();
        conn.commit();
        conn.close();
        
        Timestamp expectedTimestamp;
        ts = 1040;
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        conn = DriverManager.getConnection(getUrl(), props);
        stmt = conn.prepareStatement("UPSERT INTO " + tableName + " VALUES('aa','aa',?, null)");
        expectedTimestamp = new Timestamp(3000L);
        stmt.setTimestamp(1, expectedTimestamp);
        stmt.executeUpdate();
        conn.commit();
        expectedTimestamp = null;
        stmt.setTimestamp(1, expectedTimestamp);
        stmt.executeUpdate();
        conn.commit();
        conn.close();
        
        ts = 1050;
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        conn = DriverManager.getConnection(getUrl(), props);
        
        TestUtil.dumpTable(conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(Bytes.toBytes(tableName)));
        TestUtil.dumpTable(conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(Bytes.toBytes(indexName)));

        IndexScrutiny.scrutinizeIndex(conn, tableName, indexName);        
        
        ResultSet rs = conn.createStatement().executeQuery("SELECT /*+ NO_INDEX */ ts,v FROM " + tableName);
        assertTrue(rs.next());
        assertEquals(expectedTimestamp, rs.getTimestamp(1));
        assertEquals(null, rs.getString(2));
        assertFalse(rs.next());
        
        rs = conn.createStatement().executeQuery("SELECT \"0:TS\", \"0:V\" FROM " + indexName);
        assertTrue(rs.next());
        assertEquals(expectedTimestamp, rs.getTimestamp(1));
        assertEquals(null, rs.getString(2));
        assertFalse(rs.next());
        
        conn.close();
    }
    
    @Test
    public void testDeleteRowAndUpsertValueAtSameTS1() throws Exception {
        String tableName = generateUniqueName();
        String indexName = generateUniqueName();
        Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
        long ts = 1000;
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        Connection conn = DriverManager.getConnection(getUrl(), props);     
        conn.createStatement().execute("CREATE TABLE " + tableName + "(k1 CHAR(2) NOT NULL, k2 CHAR(2) NOT NULL, ts TIMESTAMP, A.V VARCHAR, B.V2 VARCHAR, CONSTRAINT pk PRIMARY KEY (k1,k2)) COLUMN_ENCODED_BYTES = 0, STORE_NULLS=true");
        conn.close();

        ts = 1010;
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute("CREATE INDEX " + indexName + " ON " + tableName + "(k2,k1,ts) INCLUDE (V, v2)");
        conn.close();
        
        ts = 1020;
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        conn = DriverManager.getConnection(getUrl(), props);        
        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + tableName + " VALUES('aa','aa',?, '0','1')");
        stmt.setTimestamp(1, new Timestamp(1000L));
        stmt.executeUpdate();
        conn.commit();
        conn.close();
        
        Timestamp expectedTimestamp;
        ts = 1040;
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        conn = DriverManager.getConnection(getUrl(), props);
        stmt = conn.prepareStatement("DELETE FROM " + tableName + " WHERE (K1,K2) = ('aa','aa')");
        stmt.executeUpdate();
        conn.commit();
        expectedTimestamp = new Timestamp(3000L);
        stmt = conn.prepareStatement("UPSERT INTO " + tableName + " VALUES('aa','aa',?, null,'3')");
        stmt.setTimestamp(1, expectedTimestamp);
        stmt.executeUpdate();
        conn.commit();
        conn.close();
        
        ts = 1050;
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        conn = DriverManager.getConnection(getUrl(), props);
        
        TestUtil.dumpTable(conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(Bytes.toBytes(tableName)));
        TestUtil.dumpTable(conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(Bytes.toBytes(indexName)));

        long rowCount = IndexScrutiny.scrutinizeIndex(conn, tableName, indexName);
        assertEquals(0,rowCount);
        
        conn.close();
    }
    
    @Test
    public void testDeleteRowAndUpsertValueAtSameTS2() throws Exception {
        String tableName = generateUniqueName();
        String indexName = generateUniqueName();
        Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
        long ts = 1000;
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        Connection conn = DriverManager.getConnection(getUrl(), props);     
        conn.createStatement().execute("CREATE TABLE " + tableName + "(k1 CHAR(2) NOT NULL, k2 CHAR(2) NOT NULL, ts TIMESTAMP, V VARCHAR, V2 VARCHAR, CONSTRAINT pk PRIMARY KEY (k1,k2)) COLUMN_ENCODED_BYTES = 0, STORE_NULLS=true");
        conn.close();

        ts = 1010;
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute("CREATE INDEX " + indexName + " ON " + tableName + "(k2,k1,ts) INCLUDE (V, v2)");
        conn.close();
        
        ts = 1020;
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        conn = DriverManager.getConnection(getUrl(), props);        
        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + tableName + " VALUES('aa','aa',?, '0')");
        stmt.setTimestamp(1, new Timestamp(1000L));
        stmt.executeUpdate();
        conn.commit();
        conn.close();
        
        Timestamp expectedTimestamp;
        ts = 1040;
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        conn = DriverManager.getConnection(getUrl(), props);
        expectedTimestamp = new Timestamp(3000L);
        stmt = conn.prepareStatement("UPSERT INTO " + tableName + " VALUES('aa','aa',?, null)");
        stmt.setTimestamp(1, expectedTimestamp);
        stmt.executeUpdate();
        conn.commit();
        stmt = conn.prepareStatement("DELETE FROM " + tableName + " WHERE (K1,K2) = ('aa','aa')");
        stmt.executeUpdate();
        conn.commit();
        conn.close();
        
        ts = 1050;
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        conn = DriverManager.getConnection(getUrl(), props);
        
        TestUtil.dumpTable(conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(Bytes.toBytes(tableName)));
        TestUtil.dumpTable(conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(Bytes.toBytes(indexName)));

        long rowCount = IndexScrutiny.scrutinizeIndex(conn, tableName, indexName);
        assertEquals(0,rowCount);
        
        conn.close();
    }
}
