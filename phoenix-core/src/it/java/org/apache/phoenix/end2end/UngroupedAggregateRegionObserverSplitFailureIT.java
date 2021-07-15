/*
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

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.phoenix.compat.hbase.CompatObserverContext;
import org.apache.phoenix.coprocessor.UngroupedAggregateRegionObserver;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.util.PhoenixRuntime;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class UngroupedAggregateRegionObserverSplitFailureIT extends BaseUniqueNamesOwnClusterIT {
    
    private static HBaseTestingUtility hbaseTestUtil;
    private static String url;

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        Configuration conf = HBaseConfiguration.create();
        hbaseTestUtil = new HBaseTestingUtility(conf);
        setUpConfigForMiniCluster(conf);
        hbaseTestUtil.startMiniCluster();
        // establish url and quorum. Need to use PhoenixDriver and not PhoenixTestDriver
        String zkQuorum = "localhost:" + hbaseTestUtil.getZkCluster().getClientPort();
        url = PhoenixRuntime.JDBC_PROTOCOL + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR + zkQuorum;
        DriverManager.registerDriver(PhoenixDriver.INSTANCE);
    }

    @AfterClass
    public static synchronized void tearDownAfterClass() throws Exception {
        if (hbaseTestUtil != null) {
            hbaseTestUtil.shutdownMiniCluster();
        }
    }

    @Test
    public void testDeleteAfterSplitFailure() throws SQLException, IOException {
        String tableName = generateUniqueName();
        int numRecords = 100;
        try (Connection conn = DriverManager.getConnection(url)) {
            conn.createStatement().execute(
                "CREATE TABLE " + tableName + " (PK1 INTEGER NOT NULL PRIMARY KEY, KV1 VARCHAR)");
            int i = 0;
            String upsert = "UPSERT INTO " + tableName + " VALUES (?, ?)";
            PreparedStatement stmt = conn.prepareStatement(upsert);
            while (i < numRecords) {
                stmt.setInt(1, i);
                stmt.setString(2, UUID.randomUUID().toString());
                stmt.executeUpdate();
                i++;
            }
            conn.commit();

            List<HRegion> regions =
                    hbaseTestUtil.getMiniHBaseCluster().getRegions(TableName.valueOf(tableName));
            for (HRegion region : regions) {
                UngroupedAggregateRegionObserver obs =
                        (UngroupedAggregateRegionObserver) region.getCoprocessorHost()
                                .findCoprocessor(UngroupedAggregateRegionObserver.class.getName());
                ObserverContext<RegionCoprocessorEnvironment> ctx = new CompatObserverContext<RegionCoprocessorEnvironment>(null);
                ctx.prepare((RegionCoprocessorEnvironment) region.getCoprocessorHost()
                        .findCoprocessorEnvironment(
                            UngroupedAggregateRegionObserver.class.getName()));
                // Simulate a split failure between pre and post, in this case the region still
                // remains open. Note - preRollBackSplit gets called in case of split failure before
                // rollback
                obs.preSplit(ctx, null);
                obs.preRollBackSplit(ctx);
            }
            // We want to do server side deletes here
            conn.setAutoCommit(true);
            int rowsDeleted =
                    conn.createStatement()
                            .executeUpdate("DELETE FROM " + tableName + " WHERE PK1 > 90");
            assertEquals(rowsDeleted, 9);
        }
    }
}