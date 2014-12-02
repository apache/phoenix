/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership. The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the License.
 */
package org.apache.phoenix.schema.stats;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.coprocessor.MetaDataProtocol;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.TimeKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * A default implementation of the Statistics tracker that helps to collect stats like min key, max key and
 * guideposts.
 * TODO: review timestamps used for stats. We support the user controlling the timestamps, so we should
 * honor that with timestamps for stats as well. The issue is for compaction, though. I don't know of
 * a way for the user to specify any timestamp for that. Perhaps best to use current time across the
 * board for now.
 */
public class StatisticsCollector {
    private static final Logger logger = LoggerFactory.getLogger(StatisticsCollector.class);
    public static final long NO_TIMESTAMP = -1;

    private long guidepostDepth;
    private long maxTimeStamp = MetaDataProtocol.MIN_TABLE_TIMESTAMP;
    private Map<ImmutableBytesPtr, Pair<Long,GuidePostsInfo>> guidePostsMap = Maps.newHashMap();
    // Tracks the bytecount per family if it has reached the guidePostsDepth
    private Map<ImmutableBytesPtr, Boolean> familyMap = Maps.newHashMap();
    protected StatisticsWriter statsTable;
    private Pair<Long,GuidePostsInfo> cachedGps = null;

    public StatisticsCollector(RegionCoprocessorEnvironment env, String tableName, long clientTimeStamp) throws IOException {
        this(env, tableName, clientTimeStamp, null);
    }

    public StatisticsCollector(RegionCoprocessorEnvironment env, String tableName, long clientTimeStamp, byte[] family) throws IOException {
        Configuration config = env.getConfiguration();
        int guidepostPerRegion = config.getInt(QueryServices.STATS_GUIDEPOST_PER_REGION_ATTRIB, 
                QueryServicesOptions.DEFAULT_STATS_GUIDEPOST_PER_REGION);
        long guidepostWidth = config.getLong(QueryServices.STATS_GUIDEPOST_WIDTH_BYTES_ATTRIB,
                QueryServicesOptions.DEFAULT_STATS_GUIDEPOST_WIDTH_BYTES);
        this.guidepostDepth = StatisticsUtil.getGuidePostDepth(guidepostPerRegion, guidepostWidth, env.getRegion().getTableDesc());
        // Get the stats table associated with the current table on which the CP is
        // triggered
        this.statsTable = StatisticsWriter.newWriter(env, tableName, clientTimeStamp);
        // in a compaction we know the one family ahead of time
        if (family != null) {
            ImmutableBytesPtr cfKey = new ImmutableBytesPtr(family);
            familyMap.put(cfKey, true);
            cachedGps = new Pair<Long,GuidePostsInfo>(0L,new GuidePostsInfo(0, Collections.<byte[]>emptyList()));
            guidePostsMap.put(cfKey, cachedGps);
        }
    }
    
    public long getMaxTimeStamp() {
        return maxTimeStamp;
    }
    
    public void close() throws IOException {
        this.statsTable.close();
    }

    public void updateStatistic(HRegion region) {
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

    private void writeStatsToStatsTable(final HRegion region, boolean delete,
            List<Mutation> mutations, long currentTime) throws IOException {
        try {
            // update the statistics table
            for (ImmutableBytesPtr fam : familyMap.keySet()) {
                if (delete) {
                    if(logger.isDebugEnabled()) {
                        logger.debug("Deleting the stats for the region "+region.getRegionInfo());
                    }
                    statsTable.deleteStats(region.getRegionInfo().getRegionName(), this, fam,
                            mutations);
                }
                if(logger.isDebugEnabled()) {
                    logger.debug("Adding new stats for the region "+region.getRegionInfo());
                }
                statsTable.addStats((region.getRegionInfo().getRegionName()), this, fam,
                        mutations);
            }
        } catch (IOException e) {
            logger.error("Failed to update statistics table!", e);
            throw e;
        }
    }

    private void commitStats(List<Mutation> mutations) throws IOException {
        statsTable.commitStats(mutations);
    }

    public void collectStatistics(final List<KeyValue> results) {
        for (KeyValue kv : results) {
            updateStatistic(kv);
        }
    }

    public InternalScanner createCompactionScanner(HRegion region, Store store, InternalScanner s) throws IOException {
        // See if this is for Major compaction
        if (logger.isDebugEnabled()) {
            logger.debug("Compaction scanner created for stats");
        }
        ImmutableBytesPtr cfKey = new ImmutableBytesPtr(store.getFamily().getName());
        return getInternalScanner(region, store, s, cfKey);
    }

    public void splitStats(HRegion parent, HRegion left, HRegion right) {
        try {
            if (logger.isDebugEnabled()) {
                logger.debug("Collecting stats for split of " + parent.getRegionInfo() + " into " + left.getRegionInfo() + " and " + right.getRegionInfo());
            }
            List<Mutation> mutations = Lists.newArrayListWithExpectedSize(3);
            for (byte[] fam : parent.getStores().keySet()) {
            	statsTable.splitStats(parent, left, right, this, new ImmutableBytesPtr(fam), mutations);
            }
            if (logger.isDebugEnabled()) {
                logger.debug("Committing stats for the daughter regions as part of split " + parent.getRegionInfo());
            }
            statsTable.commitStats(mutations);
        } catch (IOException e) {
            logger.error("Error while capturing stats after split of region "
                    + parent.getRegionInfo().getRegionNameAsString(), e);
        }
    }

    protected InternalScanner getInternalScanner(HRegion region, Store store,
            InternalScanner internalScan, ImmutableBytesPtr family) {
        return new StatisticsScanner(this, statsTable, region, internalScan, family);
    }

    public void clear() {
        this.guidePostsMap.clear();
        this.familyMap.clear();
        maxTimeStamp = MetaDataProtocol.MIN_TABLE_TIMESTAMP;
    }

    public void addGuidePost(ImmutableBytesPtr cfKey, GuidePostsInfo info, long byteSize, long timestamp) {
    	Pair<Long,GuidePostsInfo> newInfo = new Pair<Long,GuidePostsInfo>(byteSize,info);
    	Pair<Long,GuidePostsInfo> oldInfo = guidePostsMap.put(cfKey, newInfo);
    	if (oldInfo != null) {
    		info.combine(oldInfo.getSecond());
    		newInfo.setFirst(oldInfo.getFirst() + newInfo.getFirst());
    	}
        maxTimeStamp = Math.max(maxTimeStamp, timestamp);
    }
    
    public void updateStatistic(KeyValue kv) {
        maxTimeStamp = Math.max(maxTimeStamp, kv.getTimestamp());

        Pair<Long,GuidePostsInfo> gps;
        if (cachedGps == null) {
            ImmutableBytesPtr cfKey = new ImmutableBytesPtr(kv.getBuffer(), kv.getFamilyOffset(), kv.getFamilyLength());
            familyMap.put(cfKey, true);

            // TODO : This can be moved to an interface so that we could collect guide posts in different ways
            gps = guidePostsMap.get(cfKey);
            if (gps == null) {
                gps = new Pair<Long,GuidePostsInfo>(0L,new GuidePostsInfo(0, Collections.<byte[]>emptyList()));
                guidePostsMap.put(cfKey, gps);
            }
        } else {
            gps = cachedGps;
        }
        int kvLength = kv.getLength();
        long byteCount = gps.getFirst() + kvLength;
        gps.setFirst(byteCount);
        if (byteCount >= guidepostDepth) {
            byte[] row = ByteUtil.copyKeyBytesIfNecessary(
                    new ImmutableBytesWritable(kv.getBuffer(), kv.getRowOffset(), kv.getRowLength()));
            if (gps.getSecond().addGuidePost(row, byteCount)) {
                gps.setFirst(0L);
            }
        }
    }

    public GuidePostsInfo getGuidePosts(ImmutableBytesPtr fam) {
        Pair<Long,GuidePostsInfo> pair = guidePostsMap.get(fam);
        if (pair != null) {
            return pair.getSecond();
        }
        return null;
    }
}
