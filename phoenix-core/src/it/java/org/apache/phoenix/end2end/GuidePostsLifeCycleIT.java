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
import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.compile.SequenceManager;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.iterate.DefaultParallelIteratorRegionSplitter;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.parse.HintNode;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(ClientManagedTimeTest.class)
public class GuidePostsLifeCycleIT extends BaseParallelIteratorsRegionSplitterIT {

    protected static final byte[] KR = new byte[] { 'r' };
    protected static final byte[] KP = new byte[] { 'p' };

    private static List<KeyRange> getSplits(Connection conn, long ts, final Scan scan) throws SQLException {
        TableRef tableRef = getTableRef(conn, ts);
        PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
        final List<HRegionLocation> regions = pconn.getQueryServices().getAllTableRegions(
                tableRef.getTable().getPhysicalName().getBytes());
        PhoenixStatement statement = new PhoenixStatement(pconn);
        StatementContext context = new StatementContext(statement, null, scan, new SequenceManager(statement));
        DefaultParallelIteratorRegionSplitter splitter = new DefaultParallelIteratorRegionSplitter(context, tableRef,
                HintNode.EMPTY_HINT_NODE) {
            @Override
            protected List<HRegionLocation> getAllRegions() throws SQLException {
                return DefaultParallelIteratorRegionSplitter.filterRegions(regions, scan.getStartRow(),
                        scan.getStopRow());
            }
        };
        List<KeyRange> keyRanges = splitter.getSplits();
        Collections.sort(keyRanges, new Comparator<KeyRange>() {
            @Override
            public int compare(KeyRange o1, KeyRange o2) {
                return Bytes.compareTo(o1.getLowerRange(), o2.getLowerRange());
            }
        });
        return keyRanges;
    }

    // This test ensures that as we keep adding new records the splits gets updated
    @Test
    public void testGuidePostsLifeCycle() throws Exception {
        long ts = nextTimestamp();
        byte[][] splits = new byte[][] { K3, K9, KR };
        ensureTableCreated(getUrl(), STABLE_NAME, splits, ts - 2);
        String url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + ts + 2;
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        PreparedStatement stmt = conn.prepareStatement("ANALYZE STABLE");
        stmt.execute();
        Scan scan = new Scan();
        List<KeyRange> keyRanges = getSplits(conn, ts, scan);
        assertEquals(4, keyRanges.size());
        upsert(ts, new byte[][] { KMIN, K4, K11 });
        stmt = conn.prepareStatement("ANALYZE STABLE");
        stmt.execute();
        keyRanges = getSplits(conn, ts, scan);
        assertEquals(6, keyRanges.size());
        upsert(ts, new byte[][] { KMIN2, K5, K12 });
        stmt = conn.prepareStatement("ANALYZE STABLE");
        stmt.execute();
        keyRanges = getSplits(conn, ts, scan);
        assertEquals(9, keyRanges.size());
        upsert(ts, new byte[][] { K1, K6, KP });
        stmt = conn.prepareStatement("ANALYZE STABLE");
        stmt.execute();
        keyRanges = getSplits(conn, ts, scan);
        assertEquals(12, keyRanges.size());
        conn.close();
    }

    protected void upsert(long ts, byte[][] val) throws Exception {
        String url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + ts;
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
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
        conn.close();
    }
}
