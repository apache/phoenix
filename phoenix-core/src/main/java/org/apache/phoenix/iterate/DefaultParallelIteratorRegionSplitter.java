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
package org.apache.phoenix.iterate;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.parse.HintNode;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.ColumnFamilyNotFoundException;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;


/**
 * Default strategy for splitting regions in ParallelIterator. Refactored from the
 * original version.
 * 
 * 
 * 
 */
public class DefaultParallelIteratorRegionSplitter implements ParallelIteratorRegionSplitter {

    protected final long guidePostsDepth;
    protected final StatementContext context;
    protected final TableRef tableRef;

    private static final Logger logger = LoggerFactory.getLogger(DefaultParallelIteratorRegionSplitter.class);
    public static DefaultParallelIteratorRegionSplitter getInstance(StatementContext context, TableRef table, HintNode hintNode) {
        return new DefaultParallelIteratorRegionSplitter(context, table, hintNode);
    }

    protected DefaultParallelIteratorRegionSplitter(StatementContext context, TableRef table, HintNode hintNode) {
        this.context = context;
        this.tableRef = table;
        ReadOnlyProps props = context.getConnection().getQueryServices().getProps();
        this.guidePostsDepth = props.getLong(QueryServices.HISTOGRAM_BYTE_DEPTH_CONF_KEY,
                QueryServicesOptions.DEFAULT_HISTOGRAM_BYTE_DEPTH);
    }

    // Get the mapping between key range and the regions that contains them.
    protected List<HRegionLocation> getAllRegions() throws SQLException {
        Scan scan = context.getScan();
        PTable table = tableRef.getTable();
        List<HRegionLocation> allTableRegions = context.getConnection().getQueryServices()
                .getAllTableRegions(table.getPhysicalName().getBytes());
        // If we're not salting, then we've already intersected the minMaxRange with the scan range
        // so there's nothing to do here.
        return filterRegions(allTableRegions, scan.getStartRow(), scan.getStopRow());
    }

    /**
     * Filters out regions that intersect with key range specified by the startKey and stopKey
     * @param allTableRegions all region infos for a given table
     * @param startKey the lower bound of key range, inclusive
     * @param stopKey the upper bound of key range, inclusive
     * @return regions that intersect with the key range given by the startKey and stopKey
     */
    // exposed for tests
    public static List<HRegionLocation> filterRegions(List<HRegionLocation> allTableRegions, byte[] startKey, byte[] stopKey) {
        Iterable<HRegionLocation> regions;
        final KeyRange keyRange = KeyRange.getKeyRange(startKey, true, stopKey, false);
        if (keyRange == KeyRange.EVERYTHING_RANGE) {
            return allTableRegions;
        }
        
        regions = Iterables.filter(allTableRegions, new Predicate<HRegionLocation>() {
            @Override
            public boolean apply(HRegionLocation location) {
                KeyRange regionKeyRange = KeyRange.getKeyRange(location.getRegionInfo().getStartKey(), location
                        .getRegionInfo().getEndKey());
                return keyRange.intersect(regionKeyRange) != KeyRange.EMPTY_RANGE;
            }
        });
        return Lists.newArrayList(regions);
    }

    protected List<KeyRange> genKeyRanges(List<HRegionLocation> regions) {
        if (regions.isEmpty()) { return Collections.emptyList(); }
        Scan scan = context.getScan();
        PTable table = this.tableRef.getTable();
        byte[] defaultCF = SchemaUtil.getEmptyColumnFamily(table);
        List<byte[]> gps = null;

        if (table.getColumnFamilies().isEmpty()) {
            // For sure we can get the defaultCF from the table
            gps = table.getGuidePosts();
        } else {
            try {
                if (scan.getFamilyMap().size() > 0) {
                    if (scan.getFamilyMap().containsKey(defaultCF)) { // Favor using default CF if it's used in scan
                        gps = table.getColumnFamily(defaultCF).getGuidePosts();
                    } else { // Otherwise, just use first CF in use by scan
                        gps = table.getColumnFamily(scan.getFamilyMap().keySet().iterator().next()).getGuidePosts();
                    }
                } else {
                    gps = table.getColumnFamily(defaultCF).getGuidePosts();
                }
            } catch (ColumnFamilyNotFoundException cfne) {
                // Alter table does this
            }
        }
        
        List<KeyRange> guidePosts = Lists.newArrayListWithCapacity(regions.size());
        List<KeyRange> regionStartEndKey = Lists.newArrayListWithExpectedSize(regions.size());
        if (gps == null || gps.isEmpty()) {
            if (logger.isDebugEnabled()) {
                logger.debug("The splits formed from region start and endkeys are: " + regionStartEndKey);
            }
            for (HRegionLocation region : regions) {
                regionStartEndKey.add(KeyRange.getKeyRange(region.getRegionInfo().getStartKey(), region.getRegionInfo()
                        .getEndKey()));
            }
            return regionStartEndKey;
        } else {
            byte[] startKey = regions.get(0).getRegionInfo().getStartKey();
            int regionSize = regions.size();
            int regionIndex = 0;
            int guideIndex = 0;
            int gpsSize = gps.size();
            while ((regionIndex <= regionSize - 1) && (guideIndex <= gpsSize - 1)) {
                byte[] currentGuidePost = gps.get(guideIndex);
                byte[] regionEndKey = regions.get(regionIndex).getRegionInfo().getEndKey();
                // Even if last row just form guide posts. Otherwise for the last region we don't get any guide posts
                while (Bytes.compareTo(currentGuidePost, regionEndKey) <= 0
                        || Bytes.equals(regionEndKey, ByteUtil.EMPTY_BYTE_ARRAY)) {
                    KeyRange keyRange = KeyRange.getKeyRange(startKey, currentGuidePost);
                    if (keyRange != KeyRange.EMPTY_RANGE) {
                        guidePosts.add(keyRange);
                    }
                    if (Bytes.compareTo(currentGuidePost, startKey) >= 0) {
                        // Reset the start key only if the current guide post is greater. Otherwise start key is greater and
                        // there is no point in using a guide post which is less than the start key.
                        // This would happen in case if the region start key is 'd' and the guide post is , for eg : 'e'.
                        // In that case we should not start with 'd' but with 'e'.
                        startKey = currentGuidePost;
                    }
                    guideIndex++;
                    if ((guideIndex <= gpsSize - 1)) {
                        currentGuidePost = gps.get(guideIndex);
                    } else {
                        break;
                    }
                }
                guidePosts.add(KeyRange.getKeyRange(startKey, regionEndKey));
                regionIndex++;
                if (regionIndex <= regionSize - 1) {
                    startKey = regions.get(regionIndex).getRegionInfo().getStartKey();
                }
            }
            if (logger.isDebugEnabled()) {
                logger.debug("The captured guideposts are: " + guidePosts);
            }
            return guidePosts;
        }
    }
        
    @Override
    public List<KeyRange> getSplits() throws SQLException {
        return genKeyRanges(getAllRegions());
    }
}
