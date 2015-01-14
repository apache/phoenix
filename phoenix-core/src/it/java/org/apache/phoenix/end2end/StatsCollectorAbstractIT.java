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

import static org.junit.Assert.fail;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Maps;
@Category(NeedsOwnMiniClusterTest.class)
public abstract class StatsCollectorAbstractIT extends BaseOwnClusterHBaseManagedTimeIT {
    protected static final String STATS_TEST_TABLE_NAME = "S";
    protected static final String STATS_TEST_TABLE_NAME_NEW = "S_NEW";
    protected static final byte[] STATS_TEST_TABLE_BYTES = Bytes.toBytes(STATS_TEST_TABLE_NAME);
    protected static final byte[] STATS_TEST_TABLE_BYTES_NEW = Bytes.toBytes(STATS_TEST_TABLE_NAME_NEW);

    @BeforeClass
    public static void doSetup() throws Exception {
        Map<String,String> props = Maps.newHashMapWithExpectedSize(3);
        // Must update config before starting server
        props.put(QueryServices.STATS_GUIDEPOST_WIDTH_BYTES_ATTRIB, Long.toString(20));
        props.put(QueryServices.EXPLAIN_CHUNK_COUNT_ATTRIB, Boolean.TRUE.toString());
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }
    
    protected void splitTable(Connection conn, byte[] splitPoint, byte[] tabName) throws IOException, InterruptedException, SQLException {
        ConnectionQueryServices services = conn.unwrap(PhoenixConnection.class).getQueryServices();
        int nRegionsNow = services.getAllTableRegions(tabName).size();
        HBaseAdmin admin = services.getAdmin();
        try {
            admin.split(tabName, splitPoint);
            int nTries = 0;
            int nRegions;
            do {
                Thread.sleep(2000);
                services.clearTableRegionCache(tabName);
                nRegions = services.getAllTableRegions(tabName).size();
                nTries++;
            } while (nRegions == nRegionsNow && nTries < 10);
            if (nRegions == nRegionsNow) {
                fail();
            }
            // FIXME: I see the commit of the stats finishing before this with a lower timestamp that the scan timestamp,
            // yet without this sleep, the query finds the old data. Seems like an HBase bug and a potentially serious one.
            Thread.sleep(8000);
        } finally {
            admin.close();
        }
    }
}
