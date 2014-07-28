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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

@Category(HBaseManagedTimeTest.class)
public class SkipScanAfterManualSplit extends BaseHBaseManagedTimeIT {

    private static final int BATCH_SIZE = 25;
    private static final int MAX_FILESIZE = 1024 * 10;
    private static final int PAYLOAD_SIZE = 1024;
    private static final String PAYLOAD;
    static {
        StringBuilder buf = new StringBuilder();
        for (int i = 0; i < PAYLOAD_SIZE; i++) {
            buf.append('a');
        }
        PAYLOAD = buf.toString();
    }
    //private static final String SPLIT_POINT = "j";
    private static final String TABLE_NAME = "S";
    private static final byte[] TABLE_NAME_BYTES = Bytes.toBytes(TABLE_NAME);
    private static final int MIN_CHAR = 'a';
    private static final int MAX_CHAR = 'z';
    //private static final int PERC_TO_SELECT = 4;
    private static final Random RAND = new Random();

    @BeforeClass
    @Shadower(classBeingShadowed = BaseHBaseManagedTimeIT.class)
    public static void doSetup() throws Exception {
        Map<String,String> props = Maps.newHashMapWithExpectedSize(2);
        props.put(QueryServices.THREAD_POOL_SIZE_ATTRIB, Integer.toString(32));
        props.put(QueryServices.QUEUE_SIZE_ATTRIB, Integer.toString(1000));
        setUpTestDriver(getUrl(), new ReadOnlyProps(props.entrySet().iterator()));
    }
    
    private static void initTable() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("CREATE TABLE " + TABLE_NAME + "("
                + "a VARCHAR PRIMARY KEY, b VARCHAR) " 
                + HTableDescriptor.MAX_FILESIZE + "=" + MAX_FILESIZE + ","
                + " SALT_BUCKETS = 4");
        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO s VALUES(?,?)");
        int rowCount = 0;
        for (int c1 = MIN_CHAR; c1 <= MAX_CHAR; c1++) {
            for (int c2 = MIN_CHAR; c2 <= MAX_CHAR; c2++) {
                String pk = Character.toString((char)c1) + Character.toString((char)c2);
                stmt.setString(1, pk);
                stmt.setString(2, PAYLOAD);
                stmt.execute();
                rowCount++;
                if (rowCount % BATCH_SIZE == 0) {
                    conn.commit();
                }
            }
        }
        conn.commit();
        ConnectionQueryServices services = conn.unwrap(PhoenixConnection.class).getQueryServices();
        HBaseAdmin admin = services.getAdmin();
        try {
            admin.flush(TABLE_NAME);
        } finally {
            admin.close();
        }
        conn.close();
    }
    
    public static int doFullScan(ConnectionQueryServices services) throws Exception{
        HTableInterface ht = services.getTable(TABLE_NAME_BYTES);
        ResultScanner scanner = ht.getScanner(new Scan());
        int count = 0;
        while (scanner.next() != null) {
            count++;
        }
        return count;
    }
    
    private static void traceRegionBoundaries(ConnectionQueryServices services) throws Exception {
        List<String> boundaries = Lists.newArrayList();
        List<HRegionLocation> regions = services.getAllTableRegions(TABLE_NAME_BYTES);
        for (HRegionLocation region : regions.subList(1, regions.size())) {
            boundaries.add(Bytes.toStringBinary(region.getRegionInfo().getStartKey()));
        }
        System.out.println("Region boundaries:\n" + boundaries);
    }
    
    protected int getTotalRows() {
        return (MAX_CHAR - MIN_CHAR) * (MAX_CHAR - MIN_CHAR);
    }
    protected Set<String> getRandomRows(int nRows) {
        Set<String> pks = Sets.newHashSetWithExpectedSize(nRows);
        while (pks.size() < nRows) {
            int c1 = MIN_CHAR + (Math.abs(RAND.nextInt()) % (MAX_CHAR - MIN_CHAR));
            int c2 = MIN_CHAR + (Math.abs(RAND.nextInt()) % (MAX_CHAR - MIN_CHAR));
            String pk = Character.toString((char)c1) + Character.toString((char)c2);
            pks.add(pk);
        }
        return pks;
    }
    
    @Ignore
    @Test
    public void testManualSplit() throws Exception {
        initTable();
        Connection conn = DriverManager.getConnection(getUrl());
        ConnectionQueryServices services = conn.unwrap(PhoenixConnection.class).getQueryServices();
        traceRegionBoundaries(services);
        int nRegions = services.getAllTableRegions(TABLE_NAME_BYTES).size();
        int nInitialRegions = nRegions;
        HBaseAdmin admin = services.getAdmin();
        try {
            admin.split(TABLE_NAME);
            int nTries = 0;
            while (nRegions == nInitialRegions && nTries < 10) {
                Thread.sleep(1000);
                nRegions = services.getAllTableRegions(TABLE_NAME_BYTES).size();
                nTries++;
            }
            assertEquals(nRegions, nInitialRegions);
            
            /* works
            nTries = 0;
            while (nRegions == nInitialRegions && nTries < 10) {
                Thread.sleep(1000);
                int count = doFullScan(services);
                assertEquals(26, count);
                nRegions = services.getAllTableRegions(TABLE_NAME_BYTES).size();
                nTries++;
            }
            assertNotEquals(nRegions, nInitialRegions);
            */
            /*
            nTries = 0;
            while (nRegions == nInitialRegions && nTries < 10) {
                Thread.sleep(1000);
                ResultSet rs = conn.createStatement().executeQuery("SELECT count(*) FROM " + TABLE_NAME);
                assertTrue(rs.next());
                assertEquals(26, rs.getInt(1));
                nRegions = services.getAllTableRegions(TABLE_NAME_BYTES).size();
                nTries++;
            }
            assertNotEquals(nRegions, nInitialRegions);
            */
            /*
            String select1 = "SELECT ";
            StringBuilder buf = new StringBuilder("count(*) FROM " + TABLE_NAME + " WHERE a IN (");
            int nRows = getTotalRows() * PERC_TO_SELECT / 100;
            for (int i = 0; i < nRows; i++) {
                buf.append("?,");
            }
            buf.setCharAt(buf.length()-1, ')');
            String query = buf.toString();
            */
            int nRows = 25;
            String query = "SELECT count(*) FROM S WHERE a IN ('tl','jt','ju','rj','hj','vt','hh','br','ga','vn','th','sv','dl','mj','is','op','ug','sq','mv','qe','kq','xy','ek','aa','ae')";
            /*
            PreparedStatement stmt1 = conn.prepareStatement(select1 + query);
            PreparedStatement stmt2 = conn.prepareStatement(select2 + query);
            int param = 1;
            List<String> pks = Lists.newArrayList();
            for (String pk : getRandomRows(nRows)) {
                stmt1.setString(param, pk);
                stmt2.setString(param, pk);
                pks.add(pk);
                param++;
            }
            ResultSet rs1 = stmt1.executeQuery();
            */
            ResultSet rs1 = conn.createStatement().executeQuery(query);
            assertTrue(rs1.next());
            //ResultSet rs2 = stmt2.executeQuery();
            //assertTrue(rs2.next());
            traceRegionBoundaries(services);
            nRegions = services.getAllTableRegions(TABLE_NAME_BYTES).size();
            assertNotEquals(nRegions, nInitialRegions);
            //assertEquals(nRows, rs2.getInt(1));
            if (nRows != rs1.getInt(1)) {
                ResultSet rs3 = conn.createStatement().executeQuery(query);
                assertTrue(rs3.next());
                assertEquals(nRows, rs3.getInt(1));
            }
            /*
            StringBuilder failedStmt = new StringBuilder("SELECT count(*) FROM " + TABLE_NAME + " WHERE a IN (");
            for (String pk : pks) {
                failedStmt.append("'" + pk + "',");
            }
            failedStmt.setCharAt(failedStmt.length()-1, ')');
            */
            assertEquals(nRows, rs1.getInt(1));
        } finally {
            admin.close();
        }

    }
}
