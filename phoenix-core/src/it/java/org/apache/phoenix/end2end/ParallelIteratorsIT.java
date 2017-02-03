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

import static org.apache.phoenix.util.TestUtil.STABLE_PK_NAME;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.apache.phoenix.util.TestUtil.analyzeTable;
import static org.apache.phoenix.util.TestUtil.analyzeTableColumns;
import static org.apache.phoenix.util.TestUtil.analyzeTableIndex;
import static org.apache.phoenix.util.TestUtil.getAllSplits;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.coprocessor.BaseScannerRegionObserver;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.schema.types.PChar;
import org.apache.phoenix.util.TestUtil;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Joiner;


public class ParallelIteratorsIT extends ParallelStatsEnabledIT {

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

    private String tableName;
    private String indexName;
    
    @Before
    public void generateTableNames() {
        tableName = "T_" + generateUniqueName();
        indexName = "I_" + generateUniqueName();
    }
    
    private List<KeyRange> getSplits(Connection conn, byte[] lowerRange, byte[] upperRange) throws SQLException {
        return TestUtil.getSplits(conn, tableName, STABLE_PK_NAME, lowerRange, upperRange, null, "COUNT(*)");
    }

    @Test
    public void testGetSplits() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl(), TEST_PROPERTIES);
        byte[][] splits = new byte[][] {K3,K4,K9,K11};
        createTable(conn, splits);
        PreparedStatement stmt = conn.prepareStatement("upsert into " + tableName + " VALUES (?, ?)");
        stmt.setString(1, new String(KMIN));
        stmt.setInt(2, 1);
        stmt.execute();
        stmt.setString(1, new String(KMAX));
        stmt.setInt(2, 2);
        stmt.execute();
        conn.commit();
        
        conn.createStatement().execute("UPDATE STATISTICS " + tableName);
        
        List<KeyRange> keyRanges;
        
        keyRanges = getAllSplits(conn, tableName);
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
    public void testServerNameOnScan() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl(), TEST_PROPERTIES);
        byte[][] splits = new byte[][] { K3, K9, KR };
        createTable(conn, splits);
        
        PhoenixStatement stmt = conn.createStatement().unwrap(PhoenixStatement.class);
        ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName + " LIMIT 1");
        rs.next();
        QueryPlan plan = stmt.getQueryPlan();
        List<List<Scan>> nestedScans = plan.getScans();
        assertNotNull(nestedScans);
        for (List<Scan> scans : nestedScans) {
            for (Scan scan : scans) {
                byte[] serverNameBytes = scan.getAttribute(BaseScannerRegionObserver.SCAN_REGION_SERVER);
                assertNotNull(serverNameBytes);
                ServerName serverName = ServerName.parseVersionedServerName(serverNameBytes);
                assertNotNull(serverName.getHostname());
            }
        }
    }
    
    @Test
    public void testGuidePostsLifeCycle() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl(), TEST_PROPERTIES);
        byte[][] splits = new byte[][] { K3, K9, KR };
        createTable(conn, splits);
        
        // create index
        conn.createStatement().execute("CREATE INDEX " + indexName + " ON " + tableName + "( \"value\")");
        // before upserting
        List<KeyRange> keyRanges = getAllSplits(conn, tableName);
        assertEquals(4, keyRanges.size());
        upsert(conn, new byte[][] { KMIN, K4, K11 });
        // Analyze table alone
        analyzeTableColumns(conn, tableName);
        keyRanges = getAllSplits(conn, tableName);
        assertEquals(7, keyRanges.size());
        // Get all splits on the index table before calling analyze on the index table
        List<KeyRange> indexSplits = getAllSplits(conn, indexName);
        assertEquals(1, indexSplits.size());
        // Analyze the index table alone
        analyzeTableIndex(conn, tableName);
        // check the splits of the main table 
        keyRanges = getAllSplits(conn, tableName);
        assertEquals(7, keyRanges.size());
        // check the splits on the index table
        indexSplits = getAllSplits(conn, indexName);
        assertEquals(4, indexSplits.size());
        upsert(conn, new byte[][] { KMIN2, K5, K12 });
        // Update the stats for both the table and the index table
        analyzeTable(conn, tableName);
        keyRanges = getAllSplits(conn, tableName);
        assertEquals(10, keyRanges.size());
        // the above analyze should have udpated the index splits also
        indexSplits = getAllSplits(conn, indexName);
        assertEquals(7, indexSplits.size());
        upsert(conn, new byte[][] { K1, K6, KP });
        // Update only the table
        analyzeTableColumns(conn, tableName);
        keyRanges = getAllSplits(conn, tableName);
        assertEquals(13, keyRanges.size());
        // No change to the index splits
        indexSplits = getAllSplits(conn, indexName);
        assertEquals(7, indexSplits.size());
        analyzeTableIndex(conn, tableName);
        indexSplits = getAllSplits(conn, indexName);
        // the above analyze should have udpated the index splits only
        assertEquals(10, indexSplits.size());
        // No change in main table splits
        keyRanges = getAllSplits(conn, tableName);
        assertEquals(13, keyRanges.size());
        conn.close();
    }

    private void upsert(Connection conn, byte[][] val) throws Exception {
        PreparedStatement stmt = conn.prepareStatement("upsert into " + tableName + " VALUES (?, ?)");
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
    
    private void createTable (Connection conn, byte[][] splits) throws SQLException {
        List<String> splitsList = new ArrayList<String>(splits.length);
        for(byte[] split : splits) {
            splitsList.add("'" + Bytes.toString(split) + "'");
        }
        conn.createStatement().execute("create table " + tableName +
                "   (id char(1) not null primary key,\n" +
                "    \"value\" integer) SPLIT ON (" + Joiner.on(',').join(splitsList) + ")");
    }
}
