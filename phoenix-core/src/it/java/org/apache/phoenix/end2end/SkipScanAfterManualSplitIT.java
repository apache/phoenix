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

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Maps;


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
        // needed for 64 region parallelization due to splitting
        // props.put(QueryServices.THREAD_POOL_SIZE_ATTRIB, Integer.toString(64));
        props.put(QueryServices.THREAD_POOL_SIZE_ATTRIB, Integer.toString(32));
        // enables manual splitting on salted tables
        props.put(QueryServices.ROW_KEY_ORDER_SALTED_TABLE_ATTRIB, Boolean.toString(false));
        props.put(QueryServices.QUEUE_SIZE_ATTRIB, Integer.toString(1000));
        props.put(QueryServices.DROP_METADATA_ATTRIB, Boolean.toString(true));
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
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
    
    @Test
    public void testManualSplit() throws Exception {
        initTable();
        Connection conn = DriverManager.getConnection(getUrl());
        ConnectionQueryServices services = conn.unwrap(PhoenixConnection.class).getQueryServices();
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
            String query = "SELECT count(*) FROM S WHERE a IN ('tl','jt',' a',' b',' c',' d')";
            ResultSet rs1 = conn.createStatement().executeQuery(query);
            assertTrue(rs1.next());
            nRegions = services.getAllTableRegions(TABLE_NAME_BYTES).size();
            // Region cache has been updated, as there are more regions now
            assertNotEquals(nRegions, nInitialRegions);
            /*
            if (nRows != rs1.getInt(1)) {
                // Run the same query again and it always passes now
                // (as region cache is up-to-date)
                ResultSet r2 = conn.createStatement().executeQuery(query);
                assertTrue(r2.next());
                assertEquals(nRows, r2.getInt(1));
            }
            */
            assertEquals(nRows, rs1.getInt(1));
        } finally {
            admin.close();
        }

    }
        
    
    /**
     * The length of the row keys used for this test. Needed to create split points.
     */
    private static final int REGION_BOUND_LENGTH_BYTES = 54;
    
    /**
     * Takes the given byteArrays and concatenates them in a buffer of length 
     * #REGION_BOUND_LENGTH_BYTES. if the byte arrays have a combined length of less than 
     * #REGION_BOUND_LENGTH_BYTES, pads with zeros. if they have a combined length of greater
     * than the limit, throws a BufferOverflowException.
     * @param byteArrays  the byte arrays to concatenate in the row key buffer
     * @return  the final resulting row key. 
     */
    private static byte[] bytesToRowKey(byte[]... byteArrays) {
        ByteBuffer buffer = ByteBuffer.allocate(REGION_BOUND_LENGTH_BYTES);
        
        for(byte[] byteArray : byteArrays) {
            buffer.put(byteArray);
        }
        
        return buffer.array();
    }

    /**
     * Creates a region boundary at the given row key values. Follows the schema used in
     * #testSkipScanIntersectStateReset().
     * @param salt  this region's salt byte
     * @param orgId  the first row key value, a string of length 15
     * @param parentId  the second row key value, a string of length 15
     * @param invertedDate  the long timestamp of a date value, with the sign bit flipped
     * @param entityId  the final row key value, a string of length 15
     * @return  the region boundary found at these row key values
     */
    private static byte[] getRegionBoundary(int salt, String orgId, String parentId, long invertedDate, String entityId) {
        return bytesToRowKey(new byte[] {(byte)salt}, Bytes.toBytes(orgId), Bytes.toBytes(parentId), Bytes.toBytes(invertedDate), Bytes.toBytes(entityId));
    }

    /**
     * Creates a region boundary at the given salt byte. This is the boundary that would be used
     * when pre-splitting the regions for a salted table.
     * @param salt  this region's salt byte
     * @return  the region boundary for this salt byte
     */
    private static byte[] getSaltBoundary(int salt) {
        return bytesToRowKey(new byte[] {(byte)salt});
    }
  
    /**
     * The region boundaries used to split the table at the start of the test. 
     * These region boundaries were extracted from a reproducing case used during bug fixing.
     * Only specific combinations of boundaries will interact with each other in the way needed to
     * cause regions to be missed.
     */
    private static final byte[][] REGION_BOUNDS = {
        getRegionBoundary(0,  "00Dxx0000001gER", "001xx000003DGz2", 9223370631742791807L, "017xx0000022OGX"),
        getRegionBoundary(0,  "00Dxx0000001gER", "001xx000003DHlF", 9223370631742760807L, "017xx0000022WMz"),
        getRegionBoundary(0,  "00Dxx0000001gER", "001xx000003DINU", 9223370631742737807L, "017xx0000022dPO"),
        getSaltBoundary(1), 
        getRegionBoundary(1,  "00Dxx0000001gER", "001xx000003DGu0", 9223370631742793807L, "017xx0000022Nes"),
        getRegionBoundary(1,  "00Dxx0000001gER", "001xx000003DHfN", 9223370631742900807L, "017xx0000022GtM"),
        getRegionBoundary(1,  "00Dxx0000001gER", "001xx000003DIMd", 9223370631742737807L, "017xx0000022cw6"),
        getSaltBoundary(2), 
        getRegionBoundary(2,  "00Dxx0000001gER", "001xx000003DGyV", 9223370631742791807L, "017xx0000022OJn"),
        getRegionBoundary(2,  "00Dxx0000001gER", "001xx000003DHk4", 9223370631742760807L, "017xx0000022Wb0"),
        getRegionBoundary(2,  "00Dxx0000001gER", "001xx000003DIRW", 9223370631742736807L, "017xx0000022dVq"),
        getSaltBoundary(3), 
        getRegionBoundary(3,  "00Dxx0000001gER", "001xx000003DGul", 9223370631742793807L, "017xx0000022NMC"),
        getRegionBoundary(3,  "00Dxx0000001gER", "001xx000003DHgC", 9223370631742762807L, "017xx0000022WAK"),
        getRegionBoundary(3,  "00Dxx0000001gER", "001xx000003DIMV", 9223370631742737807L, "017xx0000022d2P"),
        getSaltBoundary(4), 
        getRegionBoundary(4,  "00Dxx0000001gER", "001xx000003DGye", 9223370631742791807L, "017xx0000022NyS"),
        getRegionBoundary(4,  "00Dxx0000001gER", "001xx000003DHiz", 9223370631742762807L, "017xx0000022Vz3"),
        getRegionBoundary(4,  "00Dxx0000001gER", "001xx000003DILw", 9223370631742887807L, "017xx0000022HZv"),
        getSaltBoundary(5), 
        getRegionBoundary(5,  "00Dxx0000001gER", "001xx000003DGy7", 9223370631742791807L, "017xx0000022O8t"),
        getRegionBoundary(5,  "00Dxx0000001gER", "001xx000003DHip", 9223370631742762807L, "017xx0000022W5R"),
        getRegionBoundary(5,  "00Dxx0000001gER", "001xx000003DIMP", 9223370631742737807L, "017xx0000022d8h"),
        getSaltBoundary(6), 
        getRegionBoundary(6,  "00Dxx0000001gER", "001xx000003DGzO", 9223370631742791807L, "017xx0000022Nti"),
        getRegionBoundary(6,  "00Dxx0000001gER", "001xx000003DHmV", 9223370631742759807L, "017xx0000022XH9"),
        getRegionBoundary(6,  "00Dxx0000001gER", "001xx000003DISr", 9223370631742733807L, "017xx0000022e5A"),
        getSaltBoundary(7), 
        getRegionBoundary(7,  "00Dxx0000001gER", "001xx000003DGtW", 9223370631742916807L, "017xx0000022G7V"),
        getRegionBoundary(7,  "00Dxx0000001gER", "001xx000003DHhw", 9223370631742762807L, "017xx0000022W2c"),
        getRegionBoundary(7,  "00Dxx0000001gER", "001xx000003DIKn", 9223370631742740807L, "017xx0000022ceD"),
        getSaltBoundary(8), 
        getRegionBoundary(8,  "00Dxx0000001gER", "001xx000003DH0j", 9223370631742790807L, "017xx0000022OvN"),
        getRegionBoundary(8,  "00Dxx0000001gER", "001xx000003DHmR", 9223370631742759807L, "017xx0000022WyU"),
        getRegionBoundary(8,  "00Dxx0000001gER", "001xx000003DIMl", 9223370631742737807L, "017xx0000022czJ"),
        getSaltBoundary(9), 
        getRegionBoundary(9,  "00Dxx0000001gER", "001xx000003DGtF", 9223370631742916807L, "017xx0000022G7E"),
        getRegionBoundary(9,  "00Dxx0000001gER", "001xx000003DHhi", 9223370631742762807L, "017xx0000022Vk1"),
        getRegionBoundary(9,  "00Dxx0000001gER", "001xx000003DIKP", 9223370631742740807L, "017xx0000022cpA"),
        getSaltBoundary(10),
        getRegionBoundary(10, "00Dxx0000001gER", "001xx000003DGzU", 9223370631742791807L, "017xx0000022Nsb"),
        getRegionBoundary(10, "00Dxx0000001gER", "001xx000003DHmO", 9223370631742760807L, "017xx0000022WFU"),
        getRegionBoundary(10, "00Dxx0000001gER", "001xx000003DISr", 9223370631742733807L, "017xx0000022e55"),
        getSaltBoundary(11),
        getRegionBoundary(11, "00Dxx0000001gER", "001xx000003DGzB", 9223370631742791807L, "017xx0000022OLb"),
        getRegionBoundary(11, "00Dxx0000001gER", "001xx000003DHki", 9223370631742760807L, "017xx0000022WOU"),
        getRegionBoundary(11, "00Dxx0000001gER", "001xx000003DIOF", 9223370631742737807L, "017xx0000022dIS"),
        getSaltBoundary(12),
        getRegionBoundary(12, "00Dxx0000001gER", "001xx000003DH0X", 9223370631742790807L, "017xx0000022OoI"),
        getRegionBoundary(12, "00Dxx0000001gER", "001xx000003DHkT", 9223370631742760807L, "017xx0000022WSs"),
        getRegionBoundary(12, "00Dxx0000001gER", "001xx000003DILp", 9223370631742740807L, "017xx0000022cOL"),
        getSaltBoundary(13),
        getRegionBoundary(13, "00Dxx0000001gER", "001xx000003DGvw", 9223370631742793807L, "017xx0000022Ncy"),
        getRegionBoundary(13, "00Dxx0000001gER", "001xx000003DHi8", 9223370631742762807L, "017xx0000022VjH"),
        getRegionBoundary(13, "00Dxx0000001gER", "001xx000003DINt", 9223370631742737807L, "017xx0000022dLm"),
        getSaltBoundary(14),
        getRegionBoundary(14, "00Dxx0000001gER", "001xx000003DGzJ", 9223370631742791807L, "017xx0000022Nwo"),
        getRegionBoundary(14, "00Dxx0000001gER", "001xx000003DHls", 9223370631742760807L, "017xx0000022WH4"),
        getRegionBoundary(14, "00Dxx0000001gER", "001xx000003DIRy", 9223370631742736807L, "017xx0000022doL"),
        getSaltBoundary(15),
        getRegionBoundary(15, "00Dxx0000001gER", "001xx000003DGsy", 9223370631742794807L, "017xx0000022MsO"),
        getRegionBoundary(15, "00Dxx0000001gER", "001xx000003DHfG", 9223370631742764807L, "017xx0000022Vdz"),
        getRegionBoundary(15, "00Dxx0000001gER", "001xx000003DIM9", 9223370631742737807L, "017xx0000022dAT") 
    };
    
    /**
     * Tests that the SkipScan behaves properly with an InList of RowValueConstructors after
     * many table splits. It verifies that the SkipScan's internal state is reset properly when 
     * intersecting with region boundaries. Long row keys exposed an issue where calls to intersect 
     * would start mid-way through the range of values and produce incorrect SkipScanFilters when
     * creating region specific filters in {@link org.apache.phoenix.util.ScanUtil#intersectScanRange}
     * See PHOENIX-1133 and PHOENIX-1136 on apache JIRA for more details.
     * @throws java.sql.SQLException  from Connection
     */
    @Test
    public void testSkipScanInListOfRVCAfterManualSplit() throws SQLException {
        Connection conn = DriverManager.getConnection(getUrl());

        String ddl = "CREATE TABLE FIELD_HISTORY_ARCHIVE ( "
            + "organization_id CHAR(15) NOT NULL, "
            + "parent_id CHAR(15) NOT NULL, "
            + "created_date DATE NOT NULL, "
            + "entity_history_id CHAR(15) NOT NULL, "
            + "created_by_id VARCHAR "
            + "CONSTRAINT pk PRIMARY KEY (organization_id, parent_id, created_date DESC, entity_history_id)) "
            + "SALT_BUCKETS = 16 "
            + "SPLIT ON ("
            + "?, ?, ?, ?, ?, ?, ?, ?, "
            + "?, ?, ?, ?, ?, ?, ?, ?, "
            + "?, ?, ?, ?, ?, ?, ?, ?, "
            + "?, ?, ?, ?, ?, ?, ?, ?, "
            + "?, ?, ?, ?, ?, ?, ?, ?, "
            + "?, ?, ?, ?, ?, ?, ?, ?, "
            + "?, ?, ?, ?, ?, ?, ?, ?, "
            + "?, ?, ?, ?, ?, ?, ?"
            + ")"; // 63 split points, 64 total regions
        PreparedStatement ddlStmt = conn.prepareStatement(ddl);
        for(int i = 0; i < REGION_BOUNDS.length; i++) {
            ddlStmt.setBytes(i + 1, REGION_BOUNDS[i]);
        }
        ddlStmt.execute();
        conn.commit();
        
        final String upsertPrefix = "UPSERT INTO FIELD_HISTORY_ARCHIVE VALUES ( '00Dxx0000001gER', ";
        conn.createStatement().executeUpdate(upsertPrefix + "'001xx000003DGr4', TO_DATE('2014-07-11 20:53:01'), '017xx0000022MmH', '005xx000001Sv21' )");
        conn.createStatement().executeUpdate(upsertPrefix + "'001xx000003DGr5', TO_DATE('2014-07-11 20:53:01'), '017xx0000022Mln', '005xx000001Sv21' )");
        conn.createStatement().executeUpdate(upsertPrefix + "'001xx000003DGsy', TO_DATE('2014-07-11 20:53:01'), '017xx0000022MsO', '005xx000001Sv21' )");
        conn.createStatement().executeUpdate(upsertPrefix + "'001xx000003DGsy', TO_DATE('2014-07-11 20:53:01'), '017xx0000022MsS', '005xx000001Sv21' )");
        conn.createStatement().executeUpdate(upsertPrefix + "'001xx000003DGtE', TO_DATE('2014-07-11 20:53:01'), '017xx0000022Mnx', '005xx000001Sv21' )");
        conn.createStatement().executeUpdate(upsertPrefix + "'001xx000003DGtn', TO_DATE('2014-07-11 20:53:02'), '017xx0000022Nmv', '005xx000001Sv21' )");
        conn.commit();
        
        String sql = "SELECT "
            + "CREATED_BY_ID, PARENT_ID "
            + "FROM FIELD_HISTORY_ARCHIVE "
            + "WHERE ORGANIZATION_ID='00Dxx0000001gER' "
            + "AND (PARENT_ID,CREATED_DATE,ENTITY_HISTORY_ID)  IN  ("
            + "('001xx000003DGr4',TO_DATE('2014-07-11 20:53:01'),'017xx0000022MmH'),"
            + "('001xx000003DGr5',TO_DATE('2014-07-11 20:53:01'),'017xx0000022Mln'),"
            + "('001xx000003DGsy',TO_DATE('2014-07-11 20:53:01'),'017xx0000022MsO'),"
            + "('001xx000003DGsy',TO_DATE('2014-07-11 20:53:01'),'017xx0000022MsS'),"
            + "('001xx000003DGtE',TO_DATE('2014-07-11 20:53:01'),'017xx0000022Mnx'),"
            + "('001xx000003DGtn',TO_DATE('2014-07-11 20:53:02'),'017xx0000022Nmv')"
            + ") ORDER BY PARENT_ID";
        ResultSet rs = conn.createStatement().executeQuery(sql);
        
        final String expectedCreatedById = "005xx000001Sv21";
        final String[] expectedParentIds = {
            "001xx000003DGr4",
            "001xx000003DGr5",
            "001xx000003DGsy",
            "001xx000003DGsy",
            "001xx000003DGtE",
            "001xx000003DGtn"
        };
        for(String expectedParentId : expectedParentIds) {
            assertTrue(rs.next());
            assertEquals(expectedCreatedById, rs.getString(1));
            assertEquals(expectedParentId, rs.getString(2));
        }
        assertFalse(rs.next());
    }

    @Test
    public void testMinMaxRangeIntersection() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        
        PreparedStatement stmt = conn.prepareStatement("create table splits_test "
            + "(pk1 UNSIGNED_TINYINT NOT NULL, pk2 UNSIGNED_TINYINT NOT NULL, kv VARCHAR "
            + "CONSTRAINT pk PRIMARY KEY (pk1, pk2)) SALT_BUCKETS=4 SPLIT ON (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
        // Split each salt bucket into multiple regions
        stmt.setBytes(1, new byte[] {0, 1, 1});
        stmt.setBytes(2, new byte[] {0, 2, 1});
        stmt.setBytes(3, new byte[] {0, 3, 1});
        stmt.setBytes(4, new byte[] {1, 1, 1});
        stmt.setBytes(5, new byte[] {1, 2, 1});
        stmt.setBytes(6, new byte[] {1, 3, 1});
        stmt.setBytes(7, new byte[] {2, 1, 1});
        stmt.setBytes(8, new byte[] {2, 2, 1});
        stmt.setBytes(9, new byte[] {2, 3, 1});
        stmt.setBytes(10, new byte[] {3, 1, 1});
        stmt.setBytes(11, new byte[] {3, 2, 1});
        stmt.setBytes(12, new byte[] {3, 3, 1});
        stmt.execute();
        
        // Use a query with a RVC in a non equality expression
        ResultSet rs = conn.createStatement().executeQuery("select count(kv) from splits_test where pk1 <= 3 and (pk1,PK2) >= (3, 1)");
        assertTrue(rs.next());
    }
}
