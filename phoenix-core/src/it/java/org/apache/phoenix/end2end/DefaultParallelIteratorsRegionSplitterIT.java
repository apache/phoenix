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

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.compile.SequenceManager;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.coprocessor.MetaDataProtocol.MetaDataMutationResult;
import org.apache.phoenix.iterate.DefaultParallelIteratorRegionSplitter;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.parse.HintNode;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.schema.PDataType;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;


/**
 * Tests for {@link DefaultParallelIteratorRegionSplitter}.
 * 
 * 
 * @since 0.1
 */

@Category(ClientManagedTimeTest.class)
public class DefaultParallelIteratorsRegionSplitterIT extends BaseParallelIteratorsRegionSplitterIT {
    
    private static List<KeyRange> getSplits(Connection conn, long ts, final Scan scan, PTable pTable)
            throws SQLException {
        TableRef tableRef = getTableRef(conn, ts, pTable);
        PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
        final List<HRegionLocation> regions =  pconn.getQueryServices().getAllTableRegions(tableRef.getTable().getPhysicalName().getBytes());
        PhoenixStatement statement = new PhoenixStatement(pconn);
        StatementContext context = new StatementContext(statement, null, scan, new SequenceManager(statement));
        DefaultParallelIteratorRegionSplitter splitter = new DefaultParallelIteratorRegionSplitter(context, tableRef, HintNode.EMPTY_HINT_NODE) {
            @Override
            protected List<HRegionLocation> getAllRegions() throws SQLException {
                return DefaultParallelIteratorRegionSplitter.filterRegions(regions, scan.getStartRow(), scan.getStopRow());
            }
        };
        List<KeyRange> keyRanges = splitter.getSplits();
        Collections.sort(keyRanges, new Comparator<KeyRange>() {
            @Override
            public int compare(KeyRange o1, KeyRange o2) {
                return Bytes.compareTo(o1.getLowerRange(),o2.getLowerRange());
            }
        });
        return keyRanges;
    }

    @Test
    public void testGetSplits() throws Exception {
        long ts = nextTimestamp();
        initTableValues(ts);
        String url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + ts;
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);

        Scan scan = new Scan();
        
        // number of regions > target query concurrency
        scan.setStartRow(K1);
        scan.setStopRow(K12);
        List<KeyRange> keyRanges = getSplits(conn, ts, scan, null);
        assertEquals("Unexpected number of splits: " + keyRanges, 5, keyRanges.size());
        assertEquals(newKeyRange(KeyRange.UNBOUND, K3), keyRanges.get(0));
        assertEquals(newKeyRange(K3, K4), keyRanges.get(1));
        assertEquals(newKeyRange(K4, K9), keyRanges.get(2));
        assertEquals(newKeyRange(K9, K11), keyRanges.get(3));
        assertEquals(newKeyRange(K11, KeyRange.UNBOUND), keyRanges.get(4));
        
        // (number of regions / 2) > target query concurrency
        scan.setStartRow(K3);
        scan.setStopRow(K6);
        keyRanges = getSplits(conn, ts, scan, null);
        assertEquals("Unexpected number of splits: " + keyRanges, 3, keyRanges.size());
        // note that we get a single split from R2 due to small key space
        assertEquals(newKeyRange(K3, K4), keyRanges.get(0));
        assertEquals(newKeyRange(K4, K6), keyRanges.get(1));
        assertEquals(newKeyRange(K6, K9), keyRanges.get(2));
        
        // (number of regions / 2) <= target query concurrency
        scan.setStartRow(K5);
        scan.setStopRow(K6);
        keyRanges = getSplits(conn, ts, scan, null);
        assertEquals("Unexpected number of splits: " + keyRanges, 3, keyRanges.size());
        assertEquals(newKeyRange(K4, K5), keyRanges.get(0));
        assertEquals(newKeyRange(K5, K6), keyRanges.get(1));
        assertEquals(newKeyRange(K6, K9), keyRanges.get(2));
        conn.close();
    }

    @Test
    public void testGetLowerUnboundSplits() throws Throwable {
        long ts = nextTimestamp();
        initTableValues(ts);
        String url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + ts;
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        // CAll the update statistics query here
        PreparedStatement stmt = conn.prepareStatement("UPDATE_STATISTICS STABLE");
        stmt.execute();
        ConnectionQueryServices services = driver.getConnectionQueryServices(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES));
        // This getRef does not have the stats. We may need to update the stats using getTable
        TableRef table = getTableRef(conn,ts, null);
        Scan scan = new Scan();
        
        MetaDataMutationResult result = services.getTable(null, table.getTable().getSchemaName().getBytes(), table.getTable().getName().getBytes(),
                table.getTimeStamp(), ((PhoenixConnection)conn).getSCN());
        scan.setStartRow(HConstants.EMPTY_START_ROW);
        scan.setStopRow(K1);
        List<KeyRange> keyRanges = getSplits(conn, ts, scan, result.getTable());
        assertEquals("Unexpected number of splits: " + keyRanges, 3, keyRanges.size());
        assertEquals(newKeyRange(KeyRange.UNBOUND, new byte[] {'7'}), keyRanges.get(0));
        assertEquals(newKeyRange(new byte[] {'7'}, new byte[] {'M'}), keyRanges.get(1));
        assertEquals(newKeyRange(new byte[] {'M'}, K3), keyRanges.get(2));
    }

    private static KeyRange newKeyRange(byte[] lowerRange, byte[] upperRange) {
        return PDataType.CHAR.getKeyRange(lowerRange, true, upperRange, false);
    }
}
