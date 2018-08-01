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
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
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
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.coprocessor.MetaDataProtocol;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTable.IndexType;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.ServerUtil.ConnectionFactory;
import org.apache.phoenix.util.ServerUtil.ConnectionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;

/**
 * A default implementation of the Statistics tracker that helps to collect stats like min key, max key and guideposts.
 */
class DefaultStatisticsCollector implements StatisticsCollector {
    private static final Logger logger = LoggerFactory.getLogger(DefaultStatisticsCollector.class);
    private final Map<ImmutableBytesPtr, Pair<Long, GuidePostsInfoBuilder>> guidePostsInfoWriterMap = Maps.newHashMap();
    private StatisticsWriter statsWriter;
    private final Pair<Long, GuidePostsInfoBuilder> cachedGuidePosts;
    private final byte[] guidePostWidthBytes;
    private final byte[] guidePostPerRegionBytes;
    // Where to look for GUIDE_POSTS_WIDTH in SYSTEM.CATALOG
    private final byte[] ptableKey;
    private final RegionCoprocessorEnvironment env;
    private long guidePostDepth;
    private long maxTimeStamp = MetaDataProtocol.MIN_TABLE_TIMESTAMP;
    private static final Log LOG = LogFactory.getLog(DefaultStatisticsCollector.class);
    private ImmutableBytesWritable currentRow;
    private final long clientTimeStamp;
    private final String tableName;
    private final boolean isViewIndexTable;

    DefaultStatisticsCollector(RegionCoprocessorEnvironment env, String tableName, long clientTimeStamp, byte[] family,
            byte[] gp_width_bytes, byte[] gp_per_region_bytes) throws IOException {
        this.env = env;
        this.guidePostWidthBytes = gp_width_bytes;
        this.guidePostPerRegionBytes = gp_per_region_bytes;
        // Provides a means of clients controlling their timestamps to not use current time
        // when background tasks are updating stats. Instead we track the max timestamp of
        // the cells and use that.
        boolean useCurrentTime = env.getConfiguration().getBoolean(
                QueryServices.STATS_USE_CURRENT_TIME_ATTRIB,
                QueryServicesOptions.DEFAULT_STATS_USE_CURRENT_TIME);
        if (!useCurrentTime) {
            clientTimeStamp = DefaultStatisticsCollector.NO_TIMESTAMP;
        }
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
        this.clientTimeStamp = clientTimeStamp;
        this.tableName = tableName;
        // in a compaction we know the one family ahead of time
        if (family != null) {
            ImmutableBytesPtr cfKey = new ImmutableBytesPtr(family);
            cachedGuidePosts = new Pair<Long, GuidePostsInfoBuilder>(0l, new GuidePostsInfoBuilder());
            guidePostsInfoWriterMap.put(cfKey, cachedGuidePosts);
        } else {
            cachedGuidePosts = null;
        }
    }
    
    private void initGuidepostDepth() throws IOException, ClassNotFoundException, SQLException {
        // First check is if guidepost info set on statement itself
        if (guidePostPerRegionBytes != null || guidePostWidthBytes != null) {
            int guidepostPerRegion = 0;
            long guidepostWidth = QueryServicesOptions.DEFAULT_STATS_GUIDEPOST_WIDTH_BYTES;
            if (guidePostPerRegionBytes != null) {
                guidepostPerRegion = PInteger.INSTANCE.getCodec().decodeInt(guidePostPerRegionBytes, 0, SortOrder.getDefault());
            }
            if (guidePostWidthBytes != null) {
                guidepostWidth = PLong.INSTANCE.getCodec().decodeInt(guidePostWidthBytes, 0, SortOrder.getDefault());
            }
            this.guidePostDepth = StatisticsUtil.getGuidePostDepth(guidepostPerRegion, guidepostWidth,
                env.getRegion().getTableDescriptor());
        } else {
            long guidepostWidth = -1;
            Table htable = null;
            try {
                // Next check for GUIDE_POST_WIDTH on table
                htable = ConnectionFactory.getConnection(ConnectionType.DEFAULT_SERVER_CONNECTION, env).getTable(
                    SchemaUtil.getPhysicalTableName(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES, env.getConfiguration()));
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
                            QueryUtil.getConnectionOnServer(env.getConfiguration())) {
                        PTable table = PhoenixRuntime.getTable(conn, tableName);
                        if (table.getType() == PTableType.INDEX
                                && table.getIndexType() == IndexType.GLOBAL) {
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
                    }

                }
            } finally {
                if (htable != null) {
                    try {
                        htable.close();
                    } catch (IOException e) {
                        LOG.warn("Failed to close " + htable.getName(), e);
                    }
                }
            }
            if (guidepostWidth >= 0) {
                this.guidePostDepth = guidepostWidth;
            } else {
                // Last use global config value
                Configuration config = env.getConfiguration();
                this.guidePostDepth = StatisticsUtil.getGuidePostDepth(
                    config.getInt(
                        QueryServices.STATS_GUIDEPOST_PER_REGION_ATTRIB,
                        QueryServicesOptions.DEFAULT_STATS_GUIDEPOST_PER_REGION),
                    config.getLong(
                        QueryServices.STATS_GUIDEPOST_WIDTH_BYTES_ATTRIB,
                        QueryServicesOptions.DEFAULT_STATS_GUIDEPOST_WIDTH_BYTES),
                    env.getRegion().getTableDescriptor());
            }
        }
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
    public void updateStatistic(Region region, Scan scan) {
        try {
            ArrayList<Mutation> mutations = new ArrayList<Mutation>();
            writeStatistics(region, true, mutations, EnvironmentEdgeManager.currentTimeMillis(), scan);
            if (logger.isDebugEnabled()) {
                logger.debug("Committing new stats for the region " + region.getRegionInfo());
            }
            commitStats(mutations);
        } catch (IOException e) {
            logger.error("Unable to commit new stats", e);
        }
    }

    private void writeStatistics(final Region region, boolean delete, List<Mutation> mutations, long currentTime, Scan scan)
            throws IOException {
        try {
            Set<ImmutableBytesPtr> fams = guidePostsInfoWriterMap.keySet();
            // Update the statistics table.
            // Delete statistics for a region if no guide posts are collected for that region during
            // UPDATE STATISTICS. This will not impact a stats collection of single column family during
            // compaction as guidePostsInfoWriterMap cannot be empty in this case.
            if (cachedGuidePosts == null) {
                // We're either collecting stats for the data table or the local index table, but not both
                // We can determine this based on the column families in the scan being prefixed with the
                // local index column family prefix. We always explicitly specify the local index column
                // families when we're collecting stats for a local index.
                boolean collectingForLocalIndex = scan != null && !scan.getFamilyMap().isEmpty() && MetaDataUtil.isLocalIndexFamily(scan.getFamilyMap().keySet().iterator().next());
                for (Store store : region.getStores()) {
                    ImmutableBytesPtr cfKey = new ImmutableBytesPtr(store.getColumnFamilyDescriptor().getName());
                    boolean isLocalIndexStore = MetaDataUtil.isLocalIndexFamily(cfKey);
                    if (isLocalIndexStore != collectingForLocalIndex) {
                        continue;
                    }
                    if (!guidePostsInfoWriterMap.containsKey(cfKey)) {
                        Pair<Long, GuidePostsInfoBuilder> emptyGps = new Pair<Long, GuidePostsInfoBuilder>(0l, new GuidePostsInfoBuilder());
                        guidePostsInfoWriterMap.put(cfKey, emptyGps);
                    }
                }
            }
            for (ImmutableBytesPtr fam : fams) {
                if (delete) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Deleting the stats for the region " + region.getRegionInfo());
                    }
                    statsWriter.deleteStatsForRegion(region, this, fam, mutations);
                }
                if (logger.isDebugEnabled()) {
                    logger.debug("Adding new stats for the region " + region.getRegionInfo());
                }
                // If we've disabled stats, don't write any, just delete them
                if (this.guidePostDepth > 0) {
                    statsWriter.addStats(this, fam, mutations);
                }
            }
        } catch (IOException e) {
            logger.error("Failed to update statistics table!", e);
            throw e;
        }
    }

    private void commitStats(List<Mutation> mutations) throws IOException {
        statsWriter.commitStats(mutations, this);
    }

    /**
     * Update the current statistics based on the latest batch of key-values from the underlying scanner
     * 
     * @param results
     *            next batch of {@link KeyValue}s
     * @throws IOException 
     */
    @Override
    public void collectStatistics(final List<Cell> results) {
        // A guide posts depth of zero disables the collection of stats
        if (guidePostDepth == 0 || results.size() == 0) {
            return;
        }
        Map<ImmutableBytesPtr, Boolean> famMap = Maps.newHashMap();
        boolean incrementRow = false;
        Cell c = results.get(0);
        ImmutableBytesWritable row = new ImmutableBytesWritable(c.getRowArray(), c.getRowOffset(), c.getRowLength());
        /*
         * During compaction, it is possible that HBase will not return all the key values when
         * internalScanner.next() is called. So we need the below check to avoid counting a row more
         * than once.
         */
        if (currentRow == null || !row.equals(currentRow)) {
            currentRow = row;
            incrementRow = true;
        }
        for (Cell cell : results) {
            maxTimeStamp = Math.max(maxTimeStamp, cell.getTimestamp());
            Pair<Long, GuidePostsInfoBuilder> gps;
            if (cachedGuidePosts == null) {
                ImmutableBytesPtr cfKey = new ImmutableBytesPtr(cell.getFamilyArray(), cell.getFamilyOffset(),
                        cell.getFamilyLength());
                gps = guidePostsInfoWriterMap.get(cfKey);
                if (gps == null) {
                    gps = new Pair<Long, GuidePostsInfoBuilder>(0l,
                            new GuidePostsInfoBuilder());
                    guidePostsInfoWriterMap.put(cfKey, gps);
                }
                if (famMap.get(cfKey) == null) {
                    famMap.put(cfKey, true);
                    gps.getSecond().incrementRowCount();
                }
            } else {
                gps = cachedGuidePosts;
                if (incrementRow) {
                    cachedGuidePosts.getSecond().incrementRowCount();
                    incrementRow = false;
                }
            }
            int kvLength = KeyValueUtil.getSerializedSize(cell, true);
            long byteCount = gps.getFirst() + kvLength;
            gps.setFirst(byteCount);
            if (byteCount >= guidePostDepth) {
                if (gps.getSecond().addGuidePostOnCollection(row, byteCount, gps.getSecond().getRowCount())) {
                    gps.setFirst(0l);
                    gps.getSecond().resetRowCount();
                }
            }
        }
    }

    @Override
    public InternalScanner createCompactionScanner(RegionCoprocessorEnvironment env, Store store,
            InternalScanner s) throws IOException {
        // See if this is for Major compaction
        if (logger.isDebugEnabled()) {
            logger.debug("Compaction scanner created for stats");
        }
        ImmutableBytesPtr cfKey = new ImmutableBytesPtr(store.getColumnFamilyDescriptor().getName());
        // Potentially perform a cross region server get in order to use the correct guide posts
        // width for the table being compacted.
        init();
        StatisticsScanner scanner = new StatisticsScanner(this, statsWriter, env, s, cfKey);
        return scanner;
    }

    @Override
    public void init() throws IOException {
        try {
            initGuidepostDepth();
        } catch (ClassNotFoundException | SQLException e) {
            throw new IOException("Unable to initialize the guide post depth", e);
        }
        this.statsWriter = StatisticsWriter.newWriter(env, tableName, clientTimeStamp, guidePostDepth);
    }

    @Override
    public GuidePostsInfo getGuidePosts(ImmutableBytesPtr fam) {
        Pair<Long, GuidePostsInfoBuilder> pair = guidePostsInfoWriterMap.get(fam);
        if (pair != null) {
            return pair.getSecond().build();
        }
        return null;
    }

    @VisibleForTesting // Don't call this method anywhere else
    public long getGuidePostDepth() {
        return guidePostDepth;
    }

}
