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

import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.query.QueryServices;

import com.google.common.collect.Sets;

/**
 * Provides new {@link StatisticsCollector} instances based on configuration settings for a
 * table (or system-wide configuration of statistics).
 */
public class StatisticsCollectorFactory {

    public static StatisticsCollector createStatisticsCollector(RegionCoprocessorEnvironment env,
            String tableName, long clientTimestamp, byte[] guidepostWidthBytes,
            byte[] guidepostsPerRegionBytes) throws IOException {
        if (statisticsEnabled(env)) {
            return new DefaultStatisticsCollector(env, tableName, clientTimestamp, null,
                    guidepostWidthBytes, guidepostsPerRegionBytes);
        } else {
            return new NoOpStatisticsCollector();
        }
    }

    public static StatisticsCollector createStatisticsCollector(
            RegionCoprocessorEnvironment env, String tableName, long clientTimeStamp,
            byte[] storeName) throws IOException {
        if (statisticsEnabled(env)) {
            return new DefaultStatisticsCollector(env, tableName, clientTimeStamp, storeName,
                    null, null);
        } else {
            return new NoOpStatisticsCollector();
        }
    }

    // TODO: make this declarative through new DISABLE_STATS column on SYSTEM.CATALOG table.
    // Also useful would be a USE_CURRENT_TIME_FOR_STATS column on SYSTEM.CATALOG table.
    private static final Set<TableName> DISABLE_STATS = Sets.newHashSetWithExpectedSize(3);
    static {
        DISABLE_STATS.add(TableName.valueOf(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME));
        DISABLE_STATS.add(TableName.valueOf(PhoenixDatabaseMetaData.SYSTEM_FUNCTION_NAME));
        DISABLE_STATS.add(TableName.valueOf(PhoenixDatabaseMetaData.SYSTEM_SEQUENCE_NAME));
        DISABLE_STATS.add(TableName.valueOf(PhoenixDatabaseMetaData.SYSTEM_STATS_NAME));
    }
    
    /**
     * Determines if statistics are enabled (which is the default). This is done on the
     * RegionCoprocessorEnvironment for now to allow setting this on a per-table basis, although
     * it could be moved to the general table metadata in the future if there is a realistic
     * use case for that.
     */
    private static boolean statisticsEnabled(RegionCoprocessorEnvironment env) {
        return env.getConfiguration().getBoolean(QueryServices.STATS_ENABLED_ATTRIB, true) &&
                !DISABLE_STATS.contains(env.getRegionInfo().getTable());
    }

}
