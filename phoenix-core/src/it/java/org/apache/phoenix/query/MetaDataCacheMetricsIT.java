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
package org.apache.phoenix.query;

import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.monitoring.GlobalClientMetrics;
import org.apache.phoenix.util.RunUntilFailure;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.DriverManager;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(RunUntilFailure.class)
@Category(NeedsOwnMiniClusterTest.class)
public class MetaDataCacheMetricsIT extends MetaDataCachingIT {

    @Test
    public void testGlobalClientCacheMetricsOfCreateAndDropTable() throws Exception {
        GlobalClientMetrics.GLOBAL_CLIENT_METADATA_CACHE_ADD_COUNTER.getMetric().reset();
        GlobalClientMetrics.GLOBAL_CLIENT_METADATA_CACHE_REMOVAL_COUNTER.getMetric().reset();
        GlobalClientMetrics.GLOBAL_CLIENT_METADATA_CACHE_ESTIMATED_USED_SIZE.getMetric().reset();

        String tableName = generateUniqueName();
        try (PhoenixConnection conn = (PhoenixConnection) DriverManager.getConnection(getUrl())) {
            long prevCacheAddCount =
                    GlobalClientMetrics.GLOBAL_CLIENT_METADATA_CACHE_ADD_COUNTER.getMetric()
                            .getValue();
            long prevEstimatedUsedCacheSize =
                    GlobalClientMetrics.GLOBAL_CLIENT_METADATA_CACHE_ESTIMATED_USED_SIZE.getMetric()
                            .getValue();
            createTable(conn, tableName, 0);

            assertEquals("Incorrect number of client metadata cache adds",
                    prevCacheAddCount + 1,
                    GlobalClientMetrics.GLOBAL_CLIENT_METADATA_CACHE_ADD_COUNTER.getMetric()
                            .getValue());

            long currEstimatedUsedCacheSize =
                    GlobalClientMetrics.GLOBAL_CLIENT_METADATA_CACHE_ESTIMATED_USED_SIZE.getMetric()
                            .getValue();
            int tableEstimatedSize = conn.getTable(tableName).getEstimatedSize();
            assertTrue(String.format("Incorrect estimated used size of client metadata cache " +
                                    "after creating table %s: tableEstimatedSize=%d, " +
                                    "prevEstimatedUsedCacheSize=%d,  " +
                                    "currEstimatedUsedCacheSize=%d", tableName,
                            tableEstimatedSize, prevEstimatedUsedCacheSize, currEstimatedUsedCacheSize),
                    currEstimatedUsedCacheSize >= prevEstimatedUsedCacheSize + tableEstimatedSize);

            long prevCacheRemovalCount =
                    GlobalClientMetrics.GLOBAL_CLIENT_METADATA_CACHE_REMOVAL_COUNTER.getMetric()
                            .getValue();
            prevEstimatedUsedCacheSize = currEstimatedUsedCacheSize;

            conn.createStatement().execute("DROP TABLE " + tableName);

            currEstimatedUsedCacheSize =
                    GlobalClientMetrics.GLOBAL_CLIENT_METADATA_CACHE_ESTIMATED_USED_SIZE.getMetric()
                            .getValue();
            assertEquals("Incorrect number of client metadata cache removals",
                    prevCacheRemovalCount + 1,
                    GlobalClientMetrics.GLOBAL_CLIENT_METADATA_CACHE_REMOVAL_COUNTER.getMetric()
                            .getValue());
            assertTrue(String.format("Incorrect estimated used size of client metadata cache " +
                                    "after dropping table %s: tableEstimatedSize=%d, " +
                                    "prevEstimatedUsedCacheSize=%d, " +
                                    "currEstimatedUsedCacheSize=%d", tableName,
                            tableEstimatedSize, prevEstimatedUsedCacheSize, currEstimatedUsedCacheSize),
                    currEstimatedUsedCacheSize < prevEstimatedUsedCacheSize
                            && currEstimatedUsedCacheSize >=
                            prevEstimatedUsedCacheSize - tableEstimatedSize);
        }
    }

    @Test
    public void testSystemTablesAreInCache() throws Exception {
        // no-op
    }

    @Test
    public void testGlobalClientCacheMetrics() throws Exception {
        // no-op
    }
}