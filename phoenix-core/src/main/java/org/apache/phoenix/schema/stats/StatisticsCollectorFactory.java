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
package org.apache.phoenix.schema.stats;

import static org.apache.phoenix.query.QueryServices.STATS_COLLECTION_ENABLED;
import static org.apache.phoenix.query.QueryServicesOptions.DEFAULT_STATS_COLLECTION_ENABLED;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.ServerUtil.ConnectionFactory;
import org.apache.phoenix.util.ServerUtil.ConnectionType;

/**
 * Provides new {@link StatisticsCollector} instances based on configuration settings for a
 * table (or system-wide configuration of statistics).
 */
public class StatisticsCollectorFactory {

    public static StatisticsCollector createStatisticsCollector(RegionCoprocessorEnvironment env,
            String tableName, long clientTimeStamp, byte[] guidepostWidthBytes,
            byte[] guidepostsPerRegionBytes) throws IOException {
        return createStatisticsCollector(env, tableName, clientTimeStamp, null, guidepostWidthBytes, guidepostsPerRegionBytes);
    }

    public static StatisticsCollector createStatisticsCollector(
            RegionCoprocessorEnvironment env, String tableName, long clientTimeStamp,
            byte[] storeName) throws IOException {
        return createStatisticsCollector(env, tableName, clientTimeStamp, storeName, null, null);
    }

    public static StatisticsCollector createStatisticsCollector(
            RegionCoprocessorEnvironment env, String tableName, long clientTimeStamp,
            byte[] storeName, byte[] guidepostWidthBytes,
            byte[] guidepostsPerRegionBytes) throws IOException {
        if (statisticsEnabled(env)) {
            StatisticsWriter statsWriter = StatisticsWriter.newWriter(env, tableName, clientTimeStamp);
            Table table = ConnectionFactory.getConnection(ConnectionType.DEFAULT_SERVER_CONNECTION, env).getTable(
                    SchemaUtil.getPhysicalTableName(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES, env.getConfiguration()));
            return new DefaultStatisticsCollector(env.getConfiguration(), env.getRegion(), tableName,
                    storeName,guidepostWidthBytes, guidepostsPerRegionBytes, statsWriter, table);
        } else {
            return new NoOpStatisticsCollector();
        }
    }
    
    /**
     * Determines if statistics are enabled (which is the default). This is done on the
     * RegionCoprocessorEnvironment for now to allow setting this on a per-table basis, although
     * it could be moved to the general table metadata in the future if there is a realistic
     * use case for that.
     */
    private static boolean statisticsEnabled(RegionCoprocessorEnvironment env) {
        return (env.getConfiguration().getBoolean(STATS_COLLECTION_ENABLED, DEFAULT_STATS_COLLECTION_ENABLED))
            && StatisticsUtil.isStatsEnabled(env.getRegionInfo().getTable());
    }

}
