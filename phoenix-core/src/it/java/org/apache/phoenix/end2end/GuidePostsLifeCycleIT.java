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
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(HBaseManagedTimeTest.class)
public class GuidePostsLifeCycleIT extends BaseHBaseManagedTimeIT {

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

    private static List<KeyRange> getSplits(Connection conn, final Scan scan) throws SQLException {
        TableRef tableRef = getTableRef(conn);
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
        byte[][] splits = new byte[][] { K3, K9, KR };
        ensureTableCreated(getUrl(), STABLE_NAME, splits);
        String url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB;
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        PreparedStatement stmt = conn.prepareStatement("ANALYZE STABLE");
        stmt.execute();
        Scan scan = new Scan();
        List<KeyRange> keyRanges = getSplits(conn, scan);
        assertEquals(4, keyRanges.size());
        upsert(new byte[][] { KMIN, K4, K11 });
        stmt = conn.prepareStatement("ANALYZE STABLE");
        stmt.execute();
        keyRanges = getSplits(conn, scan);
        assertEquals(7, keyRanges.size());
        upsert(new byte[][] { KMIN2, K5, K12 });
        stmt = conn.prepareStatement("ANALYZE STABLE");
        stmt.execute();
        keyRanges = getSplits(conn, scan);
        assertEquals(10, keyRanges.size());
        upsert(new byte[][] { K1, K6, KP });
        stmt = conn.prepareStatement("ANALYZE STABLE");
        stmt.execute();
        keyRanges = getSplits(conn, scan);
        assertEquals(13, keyRanges.size());
        conn.close();
    }

    protected void upsert( byte[][] val) throws Exception {
        String url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB;
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
    
    protected static TableRef getTableRef(Connection conn) throws SQLException {
        PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
        TableRef table = new TableRef(null, pconn.getMetaDataCache().getTable(
                new PTableKey(pconn.getTenantId(), STABLE_NAME)), System.currentTimeMillis(), false);
        return table;
    }
}
