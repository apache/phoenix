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
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Sets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.coprocessor.MetaDataProtocol;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.SchemaUtil;

import com.google.common.collect.Maps;

/**
 * A default implementation of the Statistics tracker that helps to collect stats like min key, max key and guideposts.
 */
public class DefaultStatisticsCollector implements StatisticsCollector {

    private static final Log LOG = LogFactory.getLog(DefaultStatisticsCollector.class);

    final Map<ImmutableBytesPtr, Pair<GuidePostEstimation, GuidePostChunkBuilder>> guidePostsInfoWriterMap = Maps.newHashMap();
    final Set<ImmutableBytesPtr> writableGuidePosts = Sets.newHashSet();
    private final Table htable;
    private StatisticsWriter statsWriter;
    final Pair<GuidePostEstimation, GuidePostChunkBuilder> cachedGuidePosts;
    final ImmutableBytesPtr cachedColumnFamilyKey;
    final byte[] guidePostWidthBytes;
    final byte[] guidePostPerRegionBytes;
    // Where to look for GUIDE_POSTS_WIDTH in SYSTEM.CATALOG
    final byte[] ptableKey;

    private long guidePostDepth;
    private long maxTimeStamp = MetaDataProtocol.MIN_TABLE_TIMESTAMP;
    private ImmutableBytesWritable currentRow;
    private final String tableName;
    private final boolean isViewIndexTable;
    private final Region region;
    private final Configuration configuration;

    public DefaultStatisticsCollector(Configuration configuration, Region region, String tableName, byte[] family,
                               byte[] gp_width_bytes, byte[] gp_per_region_bytes, StatisticsWriter statsWriter, Table htable) {
        this.configuration = configuration;
        this.region = region;
        this.guidePostWidthBytes = gp_width_bytes;
        this.guidePostPerRegionBytes = gp_per_region_bytes;
        String pName = tableName;
        // For view index, get GUIDE_POST_WIDTH from data physical table
        // since there's no row representing those in SYSTEM.CATALOG.
        if (MetaDataUtil.isViewIndex(tableName)) {
            pName = MetaDataUtil.getViewIndexUserTableName(tableName);
            isViewIndexTable = true;
        } else {
            isViewIndexTable = false;
        }
        ptableKey = SchemaUtil.getTableKeyFromFullName(pName);
        this.tableName = tableName;
        // in a compaction we know the one family ahead of time
        if (family != null) {
            cachedColumnFamilyKey = new ImmutableBytesPtr(family);
            cachedGuidePosts = new Pair<GuidePostEstimation, GuidePostChunkBuilder>(
                    new GuidePostEstimation(), new GuidePostChunkBuilder());
            guidePostsInfoWriterMap.put(cachedColumnFamilyKey, cachedGuidePosts);
        } else {
            cachedColumnFamilyKey = null;
            cachedGuidePosts = null;
        }

        this.statsWriter = statsWriter;
        this.htable = htable;
    }

    @Override
    public void init() throws IOException {
        try {
            initGuidepostDepth();
        } catch (SQLException e) {
            throw new IOException(e);
        }
        LOG.info("Initialization complete for " +
                this.getClass() + " statistics collector for table " + tableName);
    }

    /**
     * Determine the GPW for statistics collection for the table.
     * The order of priority from highest to lowest is as follows
     * 1. Value provided in UPDATE STATISTICS SQL statement (N/A for MR jobs)
     * 2. GPW column in SYSTEM.CATALOG for the table is not null
     * Inherits the value from base table for views and indexes (PHOENIX-4332)
     * 3. Value from global configuration parameters from hbase-site.xml
     *
     * GPW of 0 disables the stats collection. If stats were previously collected, this task
     * would attempt to delete entries from SYSTEM.STATS table. Not reading '0' from SYSTEM.CATALOG
     * would mean the fall back to global value which is defaulted to DEFAULT_STATS_GUIDEPOST_PER_REGION
     */
    private void initGuidepostDepth() throws IOException, SQLException {
        if (guidePostPerRegionBytes != null || guidePostWidthBytes != null) {
            getGuidePostDepthFromStatement();
            LOG.info("Guide post depth determined from SQL statement: " + guidePostDepth);
        } else {
            long guidepostWidth = getGuidePostDepthFromSystemCatalog();
            if (guidepostWidth >= 0) {
                this.guidePostDepth = guidepostWidth;
                LOG.info("Guide post depth determined from SYSTEM.CATALOG: " + guidePostDepth);
            } else {
                this.guidePostDepth = StatisticsUtil.getGuidePostDepth(
                        configuration.getInt(
                                QueryServices.STATS_GUIDEPOST_PER_REGION_ATTRIB,
                                QueryServicesOptions.DEFAULT_STATS_GUIDEPOST_PER_REGION),
                        configuration.getLong(
                                QueryServices.STATS_GUIDEPOST_WIDTH_BYTES_ATTRIB,
                                QueryServicesOptions.DEFAULT_STATS_GUIDEPOST_WIDTH_BYTES),
                        region.getTableDesc());
                LOG.info("Guide post depth determined from global configuration: " + guidePostDepth);
            }
        }
    }

    private long getGuidePostDepthFromSystemCatalog() throws IOException, SQLException {
        try {
            long guidepostWidth = -1;
            Get get = new Get(ptableKey);
            get.addColumn(PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES, PhoenixDatabaseMetaData.GUIDE_POSTS_WIDTH_BYTES);
            Result result = htable.get(get);
            if (!result.isEmpty()) {
                Cell cell = result.listCells().get(0);
                guidepostWidth = PLong.INSTANCE.getCodec().decodeLong(cell.getValueArray(), cell.getValueOffset(), SortOrder.getDefault());
            } else if (!isViewIndexTable) {
                /*
                 * The table we are collecting stats for is potentially a base table, or local
                 * index or a global index. For view indexes, we rely on the the guide post
                 * width column in the parent data table's metadata which we already tried
                 * retrieving above.
                 */
                try (Connection conn =
                             QueryUtil.getConnectionOnServer(configuration)) {
                    PTable table = PhoenixRuntime.getTable(conn, tableName);
                    if (table.getType() == PTableType.INDEX
                            && table.getIndexType() == PTable.IndexType.GLOBAL) {
                        /*
                         * For global indexes, we need to get the parentName first and then
                         * fetch guide post width configured for the parent table.
                         */
                        PName parentName = table.getParentName();
                        byte[] parentKey =
                                SchemaUtil.getTableKeyFromFullName(parentName.getString());
                        get = new Get(parentKey);
                        get.addColumn(PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES,
                                PhoenixDatabaseMetaData.GUIDE_POSTS_WIDTH_BYTES);
                        result = htable.get(get);
                        if (!result.isEmpty()) {
                            Cell cell = result.listCells().get(0);
                            guidepostWidth =
                                    PLong.INSTANCE.getCodec().decodeLong(cell.getValueArray(),
                                            cell.getValueOffset(), SortOrder.getDefault());
                        }
                    }
                } catch (ClassNotFoundException e) {
                    throw new IOException(e);
                }
            }
            return guidepostWidth;
        } finally {
            if (htable != null) {
                try {
                    htable.close();
                } catch (IOException e) {
                    LOG.warn("Failed to close " + htable.getName(), e);
                }
            }
        }
    }

    private void getGuidePostDepthFromStatement() {
        int guidepostPerRegion = 0;
        long guidepostWidth = QueryServicesOptions.DEFAULT_STATS_GUIDEPOST_WIDTH_BYTES;
        if (guidePostPerRegionBytes != null) {
            guidepostPerRegion = PInteger.INSTANCE.getCodec().decodeInt(guidePostPerRegionBytes, 0, SortOrder.getDefault());
        }
        if (guidePostWidthBytes != null) {
            guidepostWidth = PLong.INSTANCE.getCodec().decodeInt(guidePostWidthBytes, 0, SortOrder.getDefault());
        }
        this.guidePostDepth = StatisticsUtil.getGuidePostDepth(guidepostPerRegion, guidepostWidth,
                region.getTableDesc());
    }

    @Override
    public long getMaxTimeStamp() {
        return maxTimeStamp;
    }

    @Override
    public void close() throws IOException {
        if (statsWriter != null) {
            this.statsWriter.close();
        }
    }

    @Override
    public void updateStatistics(Region region, Scan scan) {
        try {
            List<Mutation> mutations = new ArrayList<Mutation>();
            flushGuidePosts();
            writeStatistics(region, true, mutations, EnvironmentEdgeManager.currentTimeMillis(), scan);
            commitStats(mutations);
        } catch (IOException e) {
            LOG.error("Unable to update SYSTEM.STATS table.", e);
        }
    }

    @Override
    public void flushGuidePosts() {
        if (currentRow != null) {
            // For each given column family, create a guide post for the remaining data
            // if its byte count isn't 0.
            trackGuidePosts(currentRow, true);
        }
    }

    private void writeStatistics(final Region region, boolean delete, List<Mutation> mutations, long currentTime, Scan scan)
            throws IOException {
        String regionStartKey = Bytes.toStringBinary(region.getRegionInfo().getStartKey());
        LOG.info("Collect Statistics on Region with Start Key [" + regionStartKey + "]");

        // Update the statistics table.
        // Delete statistics for a region if no guide posts are collected for that region during
        // UPDATE STATISTICS. This will not impact a stats collection of single column family during
        // compaction as guidePostsInfoWriterMap cannot be empty in this case.
        if (cachedGuidePosts == null) {
            // We're either collecting stats for the data table or the local index table, but not both.
            // We can determine this based on the column families in the scan being prefixed with the
            // local index column family prefix. We always explicitly specify the local index column
            // families when we're collecting stats for a local index.
            boolean collectingForLocalIndex = scan != null &&
                    !scan.getFamilyMap().isEmpty() &&
                    MetaDataUtil.isLocalIndexFamily(scan.getFamilyMap().keySet().iterator().next());
            for (Store store : region.getStores()) {
                ImmutableBytesPtr cfKey = new ImmutableBytesPtr(store.getFamily().getName());
                boolean isLocalIndexStore = MetaDataUtil.isLocalIndexFamily(cfKey);
                if (isLocalIndexStore != collectingForLocalIndex) {
                    continue;
                }
                if (!guidePostsInfoWriterMap.containsKey(cfKey)) {
                    Pair<GuidePostEstimation, GuidePostChunkBuilder> emptyGps =
                            new Pair<GuidePostEstimation, GuidePostChunkBuilder>(new GuidePostEstimation(), new GuidePostChunkBuilder());
                    guidePostsInfoWriterMap.put(cfKey, emptyGps);
                }
            }
        }

        Set<ImmutableBytesPtr> fams = guidePostsInfoWriterMap.keySet();
        for (ImmutableBytesPtr fam : fams) {
            if (delete) {
                int oldSize = mutations.size();
                statsWriter.deleteStatsForRegion(region, this, fam, mutations);
                LOG.info("Generated " + (mutations.size() - oldSize) + " mutations to delete existing stats for table [" +
                        tableName + "], Column Family [" + Bytes.toStringBinary(fam.copyBytesIfNecessary()) + "]" +
                        " on Region with Start Key [" + regionStartKey + "]");
            }

            // If we've disabled stats, don't write any, just delete them
            if (this.guidePostDepth > 0) {
                int oldSize = mutations.size();
                statsWriter.addStats(region, this, fam, mutations, guidePostDepth);
                LOG.info("Generated " + (mutations.size() - oldSize) + " mutations for new stats for table [" +
                        tableName + "], Column Family [" + Bytes.toStringBinary(fam.copyBytesIfNecessary()) + "]" +
                        " on Region with Start Key [" + regionStartKey + "]");
            }
        }
    }

    private void commitStats(List<Mutation> mutations) throws IOException {
        statsWriter.commitStats(mutations, this);
        LOG.info("Committed " + mutations.size() + " mutations for stats for table [" + tableName + "]" +
                " on Region with Start Key [" + Bytes.toStringBinary(region.getRegionInfo().getStartKey()) + "]");
    }

    /**
     * For each given column family on track, add a new guide post.
     *
     * @param rowKey
     * @param flushAll
     *            true or false. If it is true, we'll generate a guide post even when the
     *            accumulated byte count is > 0 and < guidePostDepth.
     */
    private void trackGuidePosts(ImmutableBytesWritable rowKey, boolean flushAll) {
        if (flushAll) {
            Iterator<Map.Entry<ImmutableBytesPtr, Pair<GuidePostEstimation, GuidePostChunkBuilder>>>
                    iter = guidePostsInfoWriterMap.entrySet().iterator();

            while (iter.hasNext()) {
                Map.Entry<ImmutableBytesPtr, Pair<GuidePostEstimation, GuidePostChunkBuilder>> entry = iter.next();
                long byteCount = entry.getValue().getFirst().getByteCount();
                if (byteCount > 0 && byteCount < guidePostDepth) {
                    // For the entries with byte count >= guidePostDepth, it's already in writableGuidePosts.
                    writableGuidePosts.add(entry.getKey());
                }
            }
        }

        for (ImmutableBytesPtr cfKey : writableGuidePosts) {
            Pair<GuidePostEstimation, GuidePostChunkBuilder> gps = guidePostsInfoWriterMap.get(cfKey);
            assert (gps != null);
            GuidePostEstimation estimation = gps.getFirst(); // Pair<Byte count, Row count>
            GuidePostChunkBuilder builder = gps.getSecond();

            // When collecting guideposts, we don't care about the time at which guide post is being
            // created/updated at. So passing it as 0 here. The update/create timestamp is important
            // when we are reading guideposts out of the SYSTEM.STATS table.
            if (builder.trackGuidePost(rowKey, estimation)) {
                estimation.setByteCount(0l);
                estimation.setRowCount(0l);
            }
        }

        writableGuidePosts.clear();
    }

    /**
     * Update the current statistics based on the latest batch of key-values from the underlying scanner
     *
     * @param results
     *            next batch of {@link KeyValue}s
     *
     * @throws IOException 
     */
    @Override
    public void collectStatistics(final List<Cell> results) {
        // A guide posts depth of zero disables the collection of stats
        if (guidePostDepth == 0) {
            return;
        }

        if (results.size() > 0) {
            Map<ImmutableBytesPtr, Boolean> famMap = Maps.newHashMap();
            boolean incrementRow = false;
            Cell c = results.get(0);
            ImmutableBytesWritable
                    row =
                    new ImmutableBytesWritable(c.getRowArray(), c.getRowOffset(), c.getRowLength());
            /*
             * During compaction, it is possible that HBase will not return all the key values when
             * internalScanner.next() is called. So we need the below check to avoid counting a row more
             * than once.
             */
            if (currentRow == null || !row.equals(currentRow)) {
                if (currentRow != null && writableGuidePosts.size() > 0) {
                    trackGuidePosts(currentRow, false);
                }

                currentRow = row;
                incrementRow = true;
            }

            for (Cell cell : results) {
                KeyValue kv = KeyValueUtil.ensureKeyValue(cell);
                maxTimeStamp = Math.max(maxTimeStamp, kv.getTimestamp());
                Pair<GuidePostEstimation, GuidePostChunkBuilder> gps; // Pair<Byte count, Row count>
                ImmutableBytesPtr cfKey;
                if (cachedGuidePosts == null) {
                    cfKey = new ImmutableBytesPtr(kv.getFamilyArray(), kv.getFamilyOffset(), kv.getFamilyLength());
                    gps = guidePostsInfoWriterMap.get(cfKey);
                    if (gps == null) {
                        gps = new Pair<GuidePostEstimation, GuidePostChunkBuilder>(
                                new GuidePostEstimation(), new GuidePostChunkBuilder());
                        guidePostsInfoWriterMap.put(cfKey, gps);
                    }
                    if (incrementRow && famMap.get(cfKey) == null) {
                        famMap.put(cfKey, true);
                        gps.getFirst().addRowCount(1);
                        // Don't set incrementRow to false here, otherwise we'll increase the row count for
                        // the first column family only instead of increasing for all column families
                    }
                } else {
                    cfKey = cachedColumnFamilyKey;
                    gps = cachedGuidePosts;
                    if (incrementRow) {
                        gps.getFirst().setRowCount(gps.getFirst().getRowCount() + 1);
                        incrementRow = false;
                    }
                }

                gps.getFirst().addByteCount(kv.getLength());
                if (gps.getFirst().getByteCount() >= guidePostDepth) {
                    writableGuidePosts.add(cfKey);
                }
            }
        }
    }

    @Override
    public GuidePostChunk getGuidePosts(ImmutableBytesPtr fam) {
        Pair<GuidePostEstimation, GuidePostChunkBuilder> pair = guidePostsInfoWriterMap.get(fam);
        if (pair != null) {
            GuidePostChunk guidePostChunk = pair.getSecond().build(0);
            if (guidePostChunk == null) {
                return GuidePostChunkBuilder.buildEndingChunk(
                        0, KeyRange.UNBOUND, new GuidePostEstimation());
            }
            return guidePostChunk;
        }

        return null;
    }

    @Override
    public long getGuidePostDepth() {
        return guidePostDepth;
    }

    @Override
    public StatisticsWriter getStatisticsWriter() {
        return statsWriter;
    }

    @Override
    public InternalScanner createCompactionScanner(
            RegionCoprocessorEnvironment env, Store store, InternalScanner delegate) {
        ImmutableBytesPtr cfKey =
                new ImmutableBytesPtr(store.getFamily().getName());
        LOG.info("StatisticsScanner created for table: "
                + tableName + " CF: " + store.getColumnFamilyName());
        return new StatisticsScanner(this, statsWriter, env, delegate, cfKey);
    }

}
