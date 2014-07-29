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

import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
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

@Category(HBaseManagedTimeTest.class)
public class SkipScanAfterManualSplitIT extends BaseHBaseManagedTimeIT {

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
    private static final String TABLE_NAME = "S";
    private static final byte[] TABLE_NAME_BYTES = Bytes.toBytes(TABLE_NAME);
    private static final int MIN_CHAR = 'a';
    private static final int MAX_CHAR = 'z';

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
    
    private static void traceRegionBoundaries(ConnectionQueryServices services) throws Exception {
        List<String> boundaries = Lists.newArrayList();
        List<HRegionLocation> regions = services.getAllTableRegions(TABLE_NAME_BYTES);
        for (HRegionLocation region : regions.subList(1, regions.size())) {
            boundaries.add(Bytes.toStringBinary(region.getRegionInfo().getStartKey()));
        }
        System.out.println("Region boundaries:\n" + boundaries);
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
            // Split finished by this time, but cache isn't updated until
            // table is accessed
            assertEquals(nRegions, nInitialRegions);
            
            int nRows = 2;
            String query = "SELECT /*+ NO_INTRA_REGION_PARALLELIZATION */ count(*) FROM S WHERE a IN ('tl','jt')";
            ResultSet rs1 = conn.createStatement().executeQuery(query);
            assertTrue(rs1.next());
            traceRegionBoundaries(services);
            nRegions = services.getAllTableRegions(TABLE_NAME_BYTES).size();
            // Region cache has been updated, as there are more regions now
            assertNotEquals(nRegions, nInitialRegions);
            if (nRows != rs1.getInt(1)) {
                // Run the same query again and it always passes now
                // (as region cache is up-to-date)
                ResultSet r2 = conn.createStatement().executeQuery(query);
                assertTrue(r2.next());
                assertEquals(nRows, r2.getInt(1));
            }
            assertEquals(nRows, rs1.getInt(1));
        } finally {
            admin.close();
        }

    }
    
    /* HBase-level repro of above issue. I believe the two scans need
     * to be issued in parallel to repro (that's the only difference
     * with the above tests).
    @Test
    public void testReproSplitBugAtHBaseLevel() throws Exception {
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
            // Split finished by this time, but cache isn't updated until
            // table is accessed
            assertEquals(nRegions, nInitialRegions);
            
            String query = "SELECT count(*) FROM S WHERE a IN ('tl','jt')";
            QueryPlan plan = conn.createStatement().unwrap(PhoenixStatement.class).compileQuery(query);
            HTableInterface table = services.getTable(TABLE_NAME_BYTES);
            Filter filter = plan.getContext().getScanRanges().getSkipScanFilter();
            Scan scan = new Scan();
            ResultScanner scanner;
            int count = 0;
            scan.setFilter(filter);
            
            scan.setStartRow(new byte[] {1, 't', 'l'});
            scan.setStopRow(new byte[] {1, 't', 'l'});
            scanner = table.getScanner(scan);
            count = 0;
            while (scanner.next() != null) {
                count++;
            }
            assertEquals(1, count);

            scan.setStartRow(new byte[] {3});
            scan.setStopRow(new byte[] {4});
            scanner = table.getScanner(scan);
            count = 0;
            while (scanner.next() != null) {
                count++;
            }
            assertEquals(1, count);
        } finally {
            admin.close();
        }
    }
    */

}
