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

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.util.ByteUtil;

import org.apache.phoenix.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Singleton that is used to track state associated with regions undergoing stats collection at the
 * region server's JVM level.
 */
public class StatisticsCollectionRunTracker {
    private static volatile StatisticsCollectionRunTracker INSTANCE;
    private final Set<ColumnFamilyRegionInfo> updateStatsRegions = Collections
            .newSetFromMap(new ConcurrentHashMap<ColumnFamilyRegionInfo, Boolean>());
    private final Set<RegionInfo> compactingRegions = Collections
            .newSetFromMap(new ConcurrentHashMap<RegionInfo, Boolean>());
    private final ExecutorService executor;
    
    // Constants added for testing purposes
    public static final long CONCURRENT_UPDATE_STATS_ROW_COUNT = -100l;
    public static final long COMPACTION_UPDATE_STATS_ROW_COUNT = -200l;
    
    public static StatisticsCollectionRunTracker getInstance(Configuration config) {
        StatisticsCollectionRunTracker result = INSTANCE;
        if (result == null) {
            synchronized (StatisticsCollectionRunTracker.class) {
                result = INSTANCE;
                if (result == null) {
                    INSTANCE = result = new StatisticsCollectionRunTracker(config);
                }
            }
        }
        return result;
    }

    private StatisticsCollectionRunTracker(Configuration config) {
        int poolSize =
                config.getInt(QueryServices.STATS_SERVER_POOL_SIZE,
                    QueryServicesOptions.DEFAULT_STATS_POOL_SIZE);
        ThreadFactoryBuilder builder =
                new ThreadFactoryBuilder().setDaemon(true).setNameFormat(
                    "phoenix-update-statistics-%s");
        executor = Executors.newFixedThreadPool(poolSize, builder.build());
    }

    /**
     * @param regionInfo for the region that should be marked as undergoing stats collection via
     *            major compaction.
     * @return true if the region wasn't already marked for stats collection via compaction, false
     *         otherwise.
     */
    public boolean addCompactingRegion(RegionInfo regionInfo) {
        return compactingRegions.add(regionInfo);
    }

    /**
     * @param regionInfo for the region that should be unmarked as undergoing stats collection via
     *            major compaction.
     * @return true if the region was marked for stats collection via compaction, false otherwise.
     */
    public boolean removeCompactingRegion(RegionInfo regionInfo) {
        return compactingRegions.remove(regionInfo);
    }

    /**
     * @param regionInfo for the region to check for.
     * @return true if stats are being collected for the region via major compaction, false
     *         otherwise.
     */
    public boolean areStatsBeingCollectedOnCompaction(RegionInfo regionInfo) {
        return compactingRegions.contains(regionInfo);
    }

    /**
     * @param regionInfo for the region to run UPDATE STATISTICS command on.
     * @param familySet 
     * @return true if UPDATE STATISTICS wasn't already running on the region, false otherwise.
     */
    public boolean addUpdateStatsCommandRegion(RegionInfo regionInfo, Set<byte[]> familySet) {
        return updateStatsRegions.add(new ColumnFamilyRegionInfo(regionInfo, familySet));
    }

    /**
     * @param regionInfo for the region to mark as not running UPDATE STATISTICS command on.
     * @return true if UPDATE STATISTICS was running on the region, false otherwise.
     */
    public boolean removeUpdateStatsCommandRegion(RegionInfo regionInfo, Set<byte[]> familySet) {
        return updateStatsRegions.remove(new ColumnFamilyRegionInfo(regionInfo, familySet));
    }

    /**
     * Enqueues the task for execution.
     * @param <T>
     * @param c task to execute
     */
    public <T> Future<T> runTask(Callable<T> c) {
        return executor.submit(c);
    }

    private static class ColumnFamilyRegionInfo {
        private final RegionInfo regionInfo;
        private final Set<byte[]> familySet;

        public ColumnFamilyRegionInfo(RegionInfo regionInfo, Set<byte[]> familySet) {
            this.regionInfo = regionInfo;
            this.familySet = familySet;
        }

        public RegionInfo getRegionInfo() {
            return regionInfo;
        }

        public Set<byte[]> getFamilySet() {
            return familySet;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) { return true; }
            if (!(obj instanceof ColumnFamilyRegionInfo)) { return false; }

            ColumnFamilyRegionInfo c = (ColumnFamilyRegionInfo)obj;
            return c.getRegionInfo().equals(this.regionInfo) && ByteUtil.match(this.familySet, c.getFamilySet());
        }

        @Override
        public int hashCode() {
            return this.getRegionInfo().hashCode();
        }
    }

}
