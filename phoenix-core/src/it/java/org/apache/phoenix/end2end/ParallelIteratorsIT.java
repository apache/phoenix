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

import static org.apache.phoenix.util.TestUtil.STABLE_NAME;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.apache.phoenix.util.TestUtil.analyzeTable;
import static org.apache.phoenix.util.TestUtil.analyzeTableColumns;
import static org.apache.phoenix.util.TestUtil.analyzeTableIndex;
import static org.apache.phoenix.util.TestUtil.getAllSplits;
import static org.apache.phoenix.util.TestUtil.getSplits;
import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.List;
import java.util.Map;

import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.types.PChar;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Maps;


public class ParallelIteratorsIT extends BaseOwnClusterHBaseManagedTimeIT {

    private static final String STABLE_INDEX = "STABLE_INDEX";
    protected static final byte[] KMIN  = new byte[] {'!'};
    protected static final byte[] KMIN2  = new byte[] {'.'};
    protected static final byte[] K1  = new byte[] {'a'};
    protected static final byte[] K3  = new byte[] {'c'};
    protected static final byte[] K4  = new byte[] {'d'};
    protected static final byte[] K5  = new byte[] {'e'};
    protected static final byte[] K6  = new byte[] {'f'};
    protected static final byte[] K9  = new byte[] {'i'};
    protected static final byte[] K11 = new byte[] {'k'};
    protected static final byte[] K12 = new byte[] {'l'};
    protected static final byte[] KMAX  = new byte[] {'~'};
    protected static final byte[] KMAX2  = new byte[] {'z'};
    protected static final byte[] KR = new byte[] { 'r' };
    protected static final byte[] KP = new byte[] { 'p' };
    
    @BeforeClass
    public static void doSetup() throws Exception {
        Map<String,String> props = Maps.newHashMapWithExpectedSize(3);
        // Must update config before starting server
        props.put(QueryServices.STATS_GUIDEPOST_WIDTH_BYTES_ATTRIB, Long.toString(20));
        props.put(QueryServices.DROP_METADATA_ATTRIB, Boolean.toString(true));
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }

    @Test
    public void testGetSplits() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl(), TEST_PROPERTIES);
        initTableValues(conn);
        
        PreparedStatement stmt = conn.prepareStatement("UPDATE STATISTICS STABLE");
        stmt.execute();
        
        List<KeyRange> keyRanges;
        
        keyRanges = getAllSplits(conn);
        assertEquals("Unexpected number of splits: " + keyRanges, 7, keyRanges.size());
        assertEquals(newKeyRange(KeyRange.UNBOUND, KMIN), keyRanges.get(0));
        assertEquals(newKeyRange(KMIN, K3), keyRanges.get(1));
        assertEquals(newKeyRange(K3, K4), keyRanges.get(2));
        assertEquals(newKeyRange(K4, K9), keyRanges.get(3));
        assertEquals(newKeyRange(K9, K11), keyRanges.get(4));
        assertEquals(newKeyRange(K11, KMAX), keyRanges.get(5));
        assertEquals(newKeyRange(KMAX,  KeyRange.UNBOUND), keyRanges.get(6));
        
        keyRanges = getSplits(conn, K3, K6);
        assertEquals("Unexpected number of splits: " + keyRanges, 2, keyRanges.size());
        assertEquals(newKeyRange(K3, K4), keyRanges.get(0));
        assertEquals(newKeyRange(K4, K6), keyRanges.get(1));
        
        keyRanges = getSplits(conn, K5, K6);
        assertEquals("Unexpected number of splits: " + keyRanges, 1, keyRanges.size());
        assertEquals(newKeyRange(K5, K6), keyRanges.get(0));
        
        keyRanges = getSplits(conn, null, K1);
        assertEquals("Unexpected number of splits: " + keyRanges, 2, keyRanges.size());
        assertEquals(newKeyRange(KeyRange.UNBOUND, KMIN), keyRanges.get(0));
        assertEquals(newKeyRange(KMIN, K1), keyRanges.get(1));
        conn.close();
    }

    @Test
    public void testGuidePostsLifeCycle() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl(), TEST_PROPERTIES);
        byte[][] splits = new byte[][] { K3, K9, KR };
        ensureTableCreated(getUrl(), STABLE_NAME, splits);
        // create index
        conn.createStatement().execute("CREATE INDEX " + STABLE_INDEX + " ON " + STABLE_NAME + "( \"value\")");
        // before upserting
        List<KeyRange> keyRanges = getAllSplits(conn);
        assertEquals(4, keyRanges.size());
        upsert(conn, new byte[][] { KMIN, K4, K11 });
        // Analyze table alone
        analyzeTableColumns(conn);
        keyRanges = getAllSplits(conn);
        assertEquals(7, keyRanges.size());
        // Get all splits on the index table before calling analyze on the index table
        List<KeyRange> indexSplits = getAllSplits(conn, STABLE_INDEX);
        assertEquals(1, indexSplits.size());
        // Analyze the index table alone
        analyzeTableIndex(conn, STABLE_NAME);
        // check the splits of the main table 
        keyRanges = getAllSplits(conn);
        assertEquals(7, keyRanges.size());
        // check the splits on the index table
        indexSplits = getAllSplits(conn, STABLE_INDEX);
        assertEquals(4, indexSplits.size());
        upsert(conn, new byte[][] { KMIN2, K5, K12 });
        // Update the stats for both the table and the index table
        analyzeTable(conn);
        keyRanges = getAllSplits(conn);
        assertEquals(10, keyRanges.size());
        // the above analyze should have udpated the index splits also
        indexSplits = getAllSplits(conn, STABLE_INDEX);
        assertEquals(7, indexSplits.size());
        upsert(conn, new byte[][] { K1, K6, KP });
        // Update only the table
        analyzeTableColumns(conn);
        keyRanges = getAllSplits(conn);
        assertEquals(13, keyRanges.size());
        // No change to the index splits
        indexSplits = getAllSplits(conn, STABLE_INDEX);
        assertEquals(7, indexSplits.size());
        analyzeTableIndex(conn, STABLE_NAME);
        indexSplits = getAllSplits(conn, STABLE_INDEX);
        // the above analyze should have udpated the index splits only
        assertEquals(10, indexSplits.size());
        // No change in main table splits
        keyRanges = getAllSplits(conn);
        assertEquals(13, keyRanges.size());
        conn.close();
    }

    private static void upsert(Connection conn, byte[][] val) throws Exception {
        PreparedStatement stmt = conn.prepareStatement("upsert into " + STABLE_NAME + " VALUES (?, ?)");
        stmt.setString(1, new String(val[0]));
        stmt.setInt(2, 1);
        stmt.execute();
        stmt.setString(1, new String(val[1]));
        stmt.setInt(2, 2);
        stmt.execute();
        stmt.setString(1, new String(val[2]));
        stmt.setInt(2, 3);
        stmt.execute();
        conn.commit();
    }

    private static KeyRange newKeyRange(byte[] lowerRange, byte[] upperRange) {
        return PChar.INSTANCE.getKeyRange(lowerRange, true, upperRange, false);
    }
    
    private static void initTableValues(Connection conn) throws Exception {
        byte[][] splits = new byte[][] {K3,K4,K9,K11};
        ensureTableCreated(getUrl(),STABLE_NAME,splits);
        PreparedStatement stmt = conn.prepareStatement("upsert into " + STABLE_NAME + " VALUES (?, ?)");
        stmt.setString(1, new String(KMIN));
        stmt.setInt(2, 1);
        stmt.execute();
        stmt.setString(1, new String(KMAX));
        stmt.setInt(2, 2);
        stmt.execute();
        conn.commit();
    }
}
