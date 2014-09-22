/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership. The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the License.
 */
package org.apache.phoenix.schema.stat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseEndpointCoprocessor;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.ipc.ProtocolSignature;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.PDataType;
import org.apache.phoenix.schema.PhoenixArray;
import org.apache.phoenix.util.TimeKeeper;

import com.google.common.collect.Lists;

/**
 * An endpoint implementation that allows to collect the stats for a given region and groups the stat per family. This is also an
 * RegionObserver that collects the information on compaction also. The user would be allowed to invoke this endpoint and thus populate the
 * Phoenix stats table with the max key, min key and guide posts for the given region. The stats can be consumed by the stats associated
 * with every PTable and the same can be used to parallelize the queries
 */
public class StatisticsCollector extends BaseRegionObserver implements Coprocessor, StatisticsTracker,
        StatisticsCollectorProtocol {

    public static void addToTable(HTableDescriptor desc) throws IOException {
        desc.addCoprocessor(StatisticsCollector.class.getName());
    }

    private Map<String, byte[]> minMap = new ConcurrentHashMap<String, byte[]>();
    private Map<String, byte[]> maxMap = new ConcurrentHashMap<String, byte[]>();
    private long guidepostDepth;
    private long byteCount = 0;
    private Map<String, List<byte[]>> guidePostsMap = new ConcurrentHashMap<String, List<byte[]>>();
    private Map<ImmutableBytesPtr, Boolean> familyMap = new ConcurrentHashMap<ImmutableBytesPtr, Boolean>();
    private RegionCoprocessorEnvironment env;
    protected StatisticsTable stats;
    // Ensures that either analyze or compaction happens at any point of time.
    private ReentrantLock lock = new ReentrantLock();
    private static final Log LOG = LogFactory.getLog(StatisticsCollector.class);

    @Override
    public StatisticsCollectorResponse collectStat(KeyRange keyRange) throws IOException {
        HRegion region = env.getRegion();
        boolean heldLock = false;
        int count = 0;
        try {
            if (lock.tryLock()) {
                heldLock = true;
                // Clear all old stats
                clear();
                Scan scan = createScan(env.getConfiguration());
                scan.setStartRow(keyRange.getLowerRange());
                scan.setStopRow(keyRange.getUpperRange());
                RegionScanner scanner = null;
                try {
                    scanner = region.getScanner(scan);
                    count = scanRegion(scanner, count);
                } catch (IOException e) {
                    LOG.error(e);
                    throw e;
                } finally {
                    if (scanner != null) {
                        try {
                            ArrayList<Mutation> mutations = new ArrayList<Mutation>();
                            writeStatsToStatsTable(region, scanner, true, mutations, TimeKeeper.SYSTEM.getCurrentTime());
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("Committing new stats for the region " + region.getRegionInfo());
                            }
                            commitStats(mutations);
                        } catch (IOException e) {
                            LOG.error(e);
                            throw e;
                        }
                    }
                }
            }
        } finally {
            if (heldLock) {
                lock.unlock();
            }
        }
        StatisticsCollectorResponse response = new StatisticsCollectorResponse();
        response.setRowsScanned(count);
        return response;
    }

    private void writeStatsToStatsTable(final HRegion region, final RegionScanner scanner, boolean delete,
            List<Mutation> mutations, long currentTime) throws IOException {
        scanner.close();
        try {
            // update the statistics table
            for (ImmutableBytesPtr fam : familyMap.keySet()) {
                String tableName = region.getRegionInfo().getTableNameAsString();
                if (delete) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Deleting the stats for the region " + region.getRegionInfo());
                    }
                    stats.deleteStats(tableName, region.getRegionInfo().getRegionNameAsString(), this,
                            Bytes.toString(fam.copyBytesIfNecessary()), mutations, currentTime);
                }
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Adding new stats for the region " + region.getRegionInfo());
                }
                stats.addStats(tableName, (region.getRegionInfo().getRegionNameAsString()), this,
                        Bytes.toString(fam.copyBytesIfNecessary()), mutations, currentTime);
            }
        } catch (IOException e) {
            LOG.error("Failed to update statistics table!", e);
            throw e;
        }
    }

    private void commitStats(List<Mutation> mutations) throws IOException {
        stats.commitStats(mutations);
    }

    private void deleteStatsFromStatsTable(final HRegion region, List<Mutation> mutations, long currentTime)
            throws IOException {
        try {
            // update the statistics table
            for (ImmutableBytesPtr fam : familyMap.keySet()) {
                String tableName = region.getRegionInfo().getTableNameAsString();
                stats.deleteStats(tableName, (region.getRegionInfo().getRegionNameAsString()), this,
                        Bytes.toString(fam.copyBytesIfNecessary()), mutations, currentTime);
            }
        } catch (IOException e) {
            LOG.error("Failed to delete from statistics table!", e);
            throw e;
        }
    }

    private int scanRegion(RegionScanner scanner, int count) throws IOException {
        List<KeyValue> results = new ArrayList<KeyValue>();
        boolean hasMore = true;
        while (hasMore) {
            // Am getting duplicates here. Need to avoid that
            hasMore = scanner.next(results);
            updateStat(results);
            count += results.size();
            results.clear();
            while (!hasMore) {
                break;
            }
        }
        return count;
    }

    /**
     * Update the current statistics based on the lastest batch of key-values from the underlying scanner
     * 
     * @param results
     *            next batch of {@link KeyValue}s
     */
    protected void updateStat(final List<KeyValue> results) {
        for (KeyValue kv : results) {
            updateStatistic(kv);
        }
    }

    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
        if (env instanceof RegionCoprocessorEnvironment) {
            this.env = (RegionCoprocessorEnvironment)env;
        } else {
            throw new CoprocessorException("Must be loaded on a table region!");
        }
        String tableName = ((RegionCoprocessorEnvironment)env).getRegion().getRegionInfo().getTableNameAsString();
        if (!tableName.equals(PhoenixDatabaseMetaData.SYSTEM_STATS_NAME)) {
            HTableDescriptor desc = ((RegionCoprocessorEnvironment)env).getRegion().getTableDesc();
            // Get the stats table associated with the current table on which the CP is
            // triggered
            stats = StatisticsTable.getStatisticsTableForCoprocessor(env, desc.getName());
            guidepostDepth = env.getConfiguration().getLong(QueryServices.HISTOGRAM_BYTE_DEPTH_ATTRIB,
                    QueryServicesOptions.DEFAULT_HISTOGRAM_BYTE_DEPTH);
        }
    }

    @Override
    public void stop(CoprocessorEnvironment env) throws IOException {
        if (env instanceof RegionCoprocessorEnvironment) {
            String tableName = ((RegionCoprocessorEnvironment)env).getRegion().getRegionInfo().getTableNameAsString();
            // Close only if the table is system table
            if (tableName.equals(PhoenixDatabaseMetaData.SYSTEM_STATS_NAME)) {
                stats.close();
            }
        }
    }

    @Override
    public InternalScanner preCompactScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c, Store store,
            List<? extends KeyValueScanner> scanners, ScanType scanType, long earliestPutTs, InternalScanner s)
            throws IOException {
        InternalScanner internalScan = s;
        String tableNameAsString = c.getEnvironment().getRegion().getRegionInfo().getTableNameAsString();
        if (!tableNameAsString.equals(PhoenixDatabaseMetaData.SYSTEM_STATS_NAME)) {
            boolean heldLock = false;
            try {
                if (lock.tryLock()) {
                    heldLock = true;
                    // See if this is for Major compaction
                    if (scanType.equals(ScanType.MAJOR_COMPACT)) {
                        // this is the first CP accessed, so we need to just create a major
                        // compaction scanner, just
                        // like in the compactor
                        if (s == null) {
                            Scan scan = new Scan();
                            scan.setMaxVersions(store.getFamily().getMaxVersions());
                            long smallestReadPoint = store.getHRegion().getSmallestReadPoint();
                            internalScan = new StoreScanner(store, store.getScanInfo(), scan, scanners, scanType,
                                    smallestReadPoint, earliestPutTs);
                        }
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Compaction scanner created for stats");
                        }
                        InternalScanner scanner = getInternalScanner(c, store, internalScan,
                                store.getColumnFamilyName());
                        if (scanner != null) {
                            internalScan = scanner;
                        }
                    }
                }
            } finally {
                if (heldLock) {
                    lock.unlock();
                }
            }
        }
        return internalScan;
    }

    @Override
    public void postSplit(ObserverContext<RegionCoprocessorEnvironment> ctx, HRegion l, HRegion r) throws IOException {
        // Invoke collectStat here
        HRegion region = ctx.getEnvironment().getRegion();
        String tableName = region.getRegionInfo().getTableNameAsString();
        if (!tableName.equals(PhoenixDatabaseMetaData.SYSTEM_STATS_NAME)) {
            if (familyMap != null) {
                familyMap.clear();
            }
            // Create a delete operation on the parent region
            // Then write the new guide posts for individual regions
            // TODO : Try making this atomic
            List<Mutation> mutations = Lists.newArrayListWithExpectedSize(3);
            long currentTime = TimeKeeper.SYSTEM.getCurrentTime();
            Configuration conf = ctx.getEnvironment().getConfiguration();
            if (LOG.isDebugEnabled()) {
                LOG.debug("Collecting stats for the daughter region " + l.getRegionInfo());
            }
            collectStatsForSplitRegions(conf, l, region, true, mutations, currentTime);
            clear();
            if (LOG.isDebugEnabled()) {
                LOG.debug("Collecting stats for the daughter region " + r.getRegionInfo());
            }
            collectStatsForSplitRegions(conf, r, region, false, mutations, currentTime);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Committing stats for the daughter regions as part of split " + r.getRegionInfo());
            }
            commitStats(mutations);
        }
    }

    private void collectStatsForSplitRegions(Configuration conf, HRegion daughter, HRegion parent, boolean delete,
            List<Mutation> mutations, long currentTime) throws IOException {
        Scan scan = createScan(conf);
        RegionScanner scanner = null;
        int count = 0;
        try {
            scanner = daughter.getScanner(scan);
            count = scanRegion(scanner, count);
        } catch (IOException e) {
            LOG.error(e);
            throw e;
        } finally {
            if (scanner != null) {
                try {
                    if (delete) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Deleting the stats for the parent region " + parent.getRegionInfo());
                        }
                        deleteStatsFromStatsTable(parent, mutations, currentTime);
                    }
                    writeStatsToStatsTable(daughter, scanner, false, mutations, currentTime);
                } catch (IOException e) {
                    LOG.error(e);
                    throw e;
                }
            }
        }
    }

    private Scan createScan(Configuration conf) {
        Scan scan = new Scan();
        scan.setCaching(conf.getInt(QueryServices.SCAN_CACHE_SIZE_ATTRIB, QueryServicesOptions.DEFAULT_SCAN_CACHE_SIZE));
        // do not cache the blocks here
        scan.setCacheBlocks(false);
        return scan;
    }

    protected InternalScanner getInternalScanner(ObserverContext<RegionCoprocessorEnvironment> c, Store store,
            InternalScanner internalScan, String family) {
        return new StatisticsScanner(this, stats, c.getEnvironment().getRegion().getRegionInfo(), internalScan,
                Bytes.toBytes(family));
    }

    @Override
    public void clear() {
        this.maxMap.clear();
        this.minMap.clear();
        this.guidePostsMap.clear();
        this.familyMap.clear();
    }

    @Override
    public void updateStatistic(KeyValue kv) {
        byte[] cf = kv.getFamily();
        familyMap.put(new ImmutableBytesPtr(cf), true);

        String fam = Bytes.toString(cf);
        byte[] row = new ImmutableBytesPtr(kv.getBuffer(), kv.getRowOffset(), kv.getRowLength()).copyBytesIfNecessary();
        if (!minMap.containsKey(fam) && !maxMap.containsKey(fam)) {
            minMap.put(fam, row);
            // Ideally the max key also should be added in this case
            maxMap.put(fam, row);
        } else {
            if (Bytes.compareTo(kv.getBuffer(), kv.getRowOffset(), kv.getRowLength(), minMap.get(fam), 0,
                    minMap.get(fam).length) < 0) {
                minMap.put(fam, row);
            }
            if (Bytes.compareTo(kv.getBuffer(), kv.getRowOffset(), kv.getRowLength(), maxMap.get(fam), 0,
                    maxMap.get(fam).length) > 0) {
                maxMap.put(fam, row);
            }
        }
        byteCount += kv.getLength();
        // TODO : This can be moved to an interface so that we could collect guide posts in different ways
        if (byteCount >= guidepostDepth) {
            if (guidePostsMap.get(fam) != null) {
                guidePostsMap.get(fam).add(row);
            } else {
                List<byte[]> guidePosts = new ArrayList<byte[]>();
                guidePosts.add(row);
                guidePostsMap.put(fam, guidePosts);
            }
            // reset the count for the next key
            byteCount = 0;
        }
    }

    @Override
    public byte[] getMaxKey(String fam) {
        if (maxMap.get(fam) != null) { return maxMap.get(fam); }
        return null;
    }

    @Override
    public byte[] getMinKey(String fam) {
        if (minMap.get(fam) != null) { return minMap.get(fam); }
        return null;
    }

    @Override
    public byte[] getGuidePosts(String fam) {
        if (!guidePostsMap.isEmpty()) {
            List<byte[]> guidePosts = guidePostsMap.get(fam);
            if (guidePosts != null) {
                byte[][] array = new byte[guidePosts.size()][];
                int i = 0;
                for (byte[] element : guidePosts) {
                    array[i] = element;
                    i++;
                }
                PhoenixArray phoenixArray = new PhoenixArray(PDataType.VARBINARY, array);
                return PDataType.VARBINARY_ARRAY.toBytes(phoenixArray);
            }
        }
        return null;
    }

    @Override
    public ProtocolSignature getProtocolSignature(String protocol, long version, int clientMethodsHashCode)
            throws IOException {
        return new ProtocolSignature(BaseEndpointCoprocessor.VERSION, null);
    }

    @Override
    public long getProtocolVersion(String protocol, long clientVersion) throws IOException {
        return BaseEndpointCoprocessor.VERSION;
    }
}
