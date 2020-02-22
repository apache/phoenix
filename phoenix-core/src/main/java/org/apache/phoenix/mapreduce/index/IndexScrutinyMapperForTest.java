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
package org.apache.phoenix.mapreduce.index;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos;

import org.apache.hadoop.hbase.regionserver.ScanInfoUtil;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.mapreduce.util.ConnectionUtil;
import org.apache.phoenix.query.ConnectionQueryServices;

import org.apache.phoenix.util.EnvironmentEdge;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

public class IndexScrutinyMapperForTest extends IndexScrutinyMapper {

    public static final int TEST_TABLE_TTL = 3600;
    public static final int MAX_LOOKBACK = 6;
    public static class ScrutinyTestClock extends EnvironmentEdge {
        long initialTime;
        long delta;

        public ScrutinyTestClock(long delta) {
            initialTime = System.currentTimeMillis() + delta;
            this.delta = delta;
        }

        @Override
        public long currentTime() {
            return System.currentTimeMillis() + delta;
        }
    }

    @Override
    public void preQueryTargetTable(String dataTable, String targetPhysicalTable)
            throws RuntimeException {
        Configuration configuration = getConfiguration();
        boolean maxLookback = ScanInfoUtil.isMaxLookbackTimeEnabled(configuration);
        // change the current time past ttl or maxlookback
        int injectTime = maxLookback ? MAX_LOOKBACK : TEST_TABLE_TTL;
        ScrutinyTestClock clock = new ScrutinyTestClock(injectTime * 1000);
        EnvironmentEdgeManager.injectEdge(clock);
        if (!maxLookback) {
            return;
        }
        //Insert new row once max lookback has passed on existing row
        try (Connection localConnection = ConnectionUtil.getInputConnection(configuration)) {
            localConnection.createStatement()
                    .executeUpdate("UPSERT INTO " + dataTable + " VALUES (2, 'name-3', 98053)");
            localConnection.commit();
            majorCompact(localConnection, targetPhysicalTable);
        } catch (SQLException e) {
            throw new RuntimeException("Unable to insert a new row during scrutiny ", e);
        }
    }

    private void majorCompact(Connection conn, String table) {
        try {
            ConnectionQueryServices cqsi = conn.unwrap(PhoenixConnection.class).getQueryServices();
            HBaseAdmin hbaseAdmin = cqsi.getAdmin();
            hbaseAdmin.flush(table);
            hbaseAdmin.majorCompact(table);
            while ((hbaseAdmin.getCompactionState(TableName.valueOf(table))).
                    equals(AdminProtos.GetRegionInfoResponse.CompactionState.MAJOR)) {
                Thread.sleep(100);
            }
            hbaseAdmin.close();
        } catch (SQLException | IOException | InterruptedException e) {
            throw new RuntimeException("Couldn't perform major compaction; got exception ", e);
        }
    }

    private Configuration getConfiguration() {
        Configuration conf;
        try {
            conf = connection.unwrap(PhoenixConnection.class).getQueryServices().getConfiguration();
        } catch (SQLException e) {
            throw new RuntimeException("Couldn't get configuration ", e);
        }
        return conf;
    }
}
