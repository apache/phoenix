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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.coprocessor.MetaDataProtocol;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.TimeKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

/**
 * A default implementation of the Statistics tracker that helps to collect stats like min key, max key and guideposts.
 * TODO: review timestamps used for stats. We support the user controlling the timestamps, so we should honor that with
 * timestamps for stats as well. The issue is for compaction, though. I don't know of a way for the user to specify any
 * timestamp for that. Perhaps best to use current time across the board for now.
 */
public class StatisticsCollector {
    private static final Logger logger = LoggerFactory.getLogger(StatisticsCollector.class);
    public static final long NO_TIMESTAMP = -1;

    private long guidepostDepth;
    private long maxTimeStamp = MetaDataProtocol.MIN_TABLE_TIMESTAMP;
    private Map<ImmutableBytesPtr, Pair<Long, GuidePostsInfoBuilder>> guidePostsInfoWriterMap = Maps.newHashMap();
    protected StatisticsWriter statsTable;
    private Pair<Long, GuidePostsInfoBuilder> cachedGps = null;

    public StatisticsCollector(RegionCoprocessorEnvironment env, String tableName, long clientTimeStamp)
            throws IOException {
        this(env, tableName, clientTimeStamp, null, null, null);
    }

    public StatisticsCollector(RegionCoprocessorEnvironment env, String tableName, long clientTimeStamp,
            byte[] gp_width_bytes, byte[] gp_per_region_bytes) throws IOException {
        this(env, tableName, clientTimeStamp, null, gp_width_bytes, gp_per_region_bytes);
    }

    public StatisticsCollector(RegionCoprocessorEnvironment env, String tableName, long clientTimeStamp, byte[] family)
            throws IOException {
        this(env, tableName, clientTimeStamp, family, null, null);
    }

    public StatisticsCollector(RegionCoprocessorEnvironment env, String tableName, long clientTimeStamp, byte[] family,
            byte[] gp_width_bytes, byte[] gp_per_region_bytes) throws IOException {
        Configuration config = env.getConfiguration();
        int guidepostPerRegion = gp_per_region_bytes == null
                ? config.getInt(QueryServices.STATS_GUIDEPOST_PER_REGION_ATTRIB,
                        QueryServicesOptions.DEFAULT_STATS_GUIDEPOST_PER_REGION)
                : PInteger.INSTANCE.getCodec().decodeInt(gp_per_region_bytes, 0, SortOrder.getDefault());
        long guidepostWidth = gp_width_bytes == null
                ? config.getLong(QueryServices.STATS_GUIDEPOST_WIDTH_BYTES_ATTRIB,
                        QueryServicesOptions.DEFAULT_STATS_GUIDEPOST_WIDTH_BYTES)
                : PLong.INSTANCE.getCodec().decodeInt(gp_width_bytes, 0, SortOrder.getDefault());
        this.guidepostDepth = StatisticsUtil.getGuidePostDepth(guidepostPerRegion, guidepostWidth,
                env.getRegion().getTableDesc());
        // Get the stats table associated with the current table on which the CP is
        // triggered
        this.statsTable = StatisticsWriter.newWriter(env, tableName, clientTimeStamp);
        // in a compaction we know the one family ahead of time
        if (family != null) {
            ImmutableBytesPtr cfKey = new ImmutableBytesPtr(family);
            cachedGps = new Pair<Long, GuidePostsInfoBuilder>(0l, new GuidePostsInfoBuilder());
            guidePostsInfoWriterMap.put(cfKey, cachedGps);
        }
    }

    public long getMaxTimeStamp() {
        return maxTimeStamp;
    }

    public void close() throws IOException {
        this.statsTable.close();
    }

    public void updateStatistic(Region region) {
        try {
            ArrayList<Mutation> mutations = new ArrayList<Mutation>();
            writeStatsToStatsTable(region, true, mutations, TimeKeeper.SYSTEM.getCurrentTime());
            if (logger.isDebugEnabled()) {
                logger.debug("Committing new stats for the region " + region.getRegionInfo());
            }
            commitStats(mutations);
        } catch (IOException e) {
            logger.error("Unable to commit new stats", e);
        } finally {
            clear();
        }
    }

    private void writeStatsToStatsTable(final Region region, boolean delete, List<Mutation> mutations, long currentTime)
            throws IOException {
        try {
            // update the statistics table
            for (ImmutableBytesPtr fam : guidePostsInfoWriterMap.keySet()) {
                if (delete) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Deleting the stats for the region " + region.getRegionInfo());
                    }
                    statsTable.deleteStats(region, this, fam, mutations);
                }
                if (logger.isDebugEnabled()) {
                    logger.debug("Adding new stats for the region " + region.getRegionInfo());
                }
                statsTable.addStats(this, fam, mutations);
            }
        } catch (IOException e) {
            logger.error("Failed to update statistics table!", e);
            throw e;
        }
    }

    private void commitStats(List<Mutation> mutations) throws IOException {
        statsTable.commitStats(mutations);
    }

    /**
     * Update the current statistics based on the latest batch of key-values from the underlying scanner
     * 
     * @param results
     *            next batch of {@link KeyValue}s
     */
    public void collectStatistics(final List<Cell> results) {
        Map<ImmutableBytesPtr, Boolean> famMap = Maps.newHashMap();
        List<GuidePostsInfoBuilder> rowTracker = null;
        if (cachedGps == null) {
            rowTracker = new ArrayList<GuidePostsInfoBuilder>();
        }
        for (Cell cell : results) {
            KeyValue kv = KeyValueUtil.ensureKeyValue(cell);
            maxTimeStamp = Math.max(maxTimeStamp, kv.getTimestamp());
            Pair<Long, GuidePostsInfoBuilder> gps;
            if (cachedGps == null) {
                ImmutableBytesPtr cfKey = new ImmutableBytesPtr(kv.getFamilyArray(), kv.getFamilyOffset(),
                        kv.getFamilyLength());
                gps = guidePostsInfoWriterMap.get(cfKey);
                if (gps == null) {
                    gps = new Pair<Long, GuidePostsInfoBuilder>(0l,
                            new GuidePostsInfoBuilder());
                    guidePostsInfoWriterMap.put(cfKey, gps);
                }
                if (famMap.get(cfKey) == null) {
                    famMap.put(cfKey, true);
                    rowTracker.add(gps.getSecond());
                }
            } else {
                gps = cachedGps;
            }
            int kvLength = kv.getLength();
            long byteCount = gps.getFirst() + kvLength;
            gps.setFirst(byteCount);
            if (byteCount >= guidepostDepth) {
                byte[] row = ByteUtil.copyKeyBytesIfNecessary(
                        new ImmutableBytesWritable(kv.getRowArray(), kv.getRowOffset(), kv.getRowLength()));
                if (gps.getSecond().addGuidePosts(row, byteCount)) {
                    gps.setFirst(0l);
                }
            }
        }
        if (cachedGps == null) {
            for (GuidePostsInfoBuilder s : rowTracker) {
                s.incrementRowCount();
            }
        } else {
            cachedGps.getSecond().incrementRowCount();
        }
    }

    public InternalScanner createCompactionScanner(RegionCoprocessorEnvironment env, Store store, InternalScanner s,
            Pair<HRegionInfo, HRegionInfo> mergeRegions) throws IOException {
        // See if this is for Major compaction
        if (logger.isDebugEnabled()) {
            logger.debug("Compaction scanner created for stats");
        }
        ImmutableBytesPtr cfKey = new ImmutableBytesPtr(store.getFamily().getName());
        return getInternalScanner(env, store, s, cfKey, mergeRegions);
    }

    protected InternalScanner getInternalScanner(RegionCoprocessorEnvironment env, Store store,
            InternalScanner internalScan, ImmutableBytesPtr family, Pair<HRegionInfo, HRegionInfo> mergeRegions) {
        return new StatisticsScanner(this, statsTable, env, internalScan, family, mergeRegions);
    }

    public void clear() {
        this.guidePostsInfoWriterMap.clear();
        maxTimeStamp = MetaDataProtocol.MIN_TABLE_TIMESTAMP;
    }

    public GuidePostsInfo getGuidePosts(ImmutableBytesPtr fam) {
        Pair<Long, GuidePostsInfoBuilder> pair = guidePostsInfoWriterMap.get(fam);
        if (pair != null) { return pair.getSecond().build(); }
        return null;
    }

}
