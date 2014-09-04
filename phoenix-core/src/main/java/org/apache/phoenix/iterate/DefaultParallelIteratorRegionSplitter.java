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
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.parse.HintNode;
import org.apache.phoenix.parse.HintNode.Hint;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.ColumnFamilyNotFoundException;
import org.apache.phoenix.schema.PDataType;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PhoenixArray;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.stat.PTableStats;
import org.apache.phoenix.schema.stat.StatisticsConstants;
import org.apache.phoenix.util.ReadOnlyProps;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;


/**
 * Default strategy for splitting regions in ParallelIterator. Refactored from the
 * original version.
 * 
 * 
 * 
 */
public class DefaultParallelIteratorRegionSplitter implements ParallelIteratorRegionSplitter {

    protected final int targetConcurrency;
    protected final int maxConcurrency;
    protected final long guidePostsDepth;
    protected final int maxIntraRegionParallelization;
    protected final StatementContext context;
    protected final TableRef tableRef;

    public static DefaultParallelIteratorRegionSplitter getInstance(StatementContext context, TableRef table, HintNode hintNode) {
        return new DefaultParallelIteratorRegionSplitter(context, table, hintNode);
    }

    protected DefaultParallelIteratorRegionSplitter(StatementContext context, TableRef table, HintNode hintNode) {
        this.context = context;
        this.tableRef = table;
        ReadOnlyProps props = context.getConnection().getQueryServices().getProps();
        this.targetConcurrency = props.getInt(QueryServices.TARGET_QUERY_CONCURRENCY_ATTRIB,
            QueryServicesOptions.DEFAULT_TARGET_QUERY_CONCURRENCY);
        this.maxConcurrency = props.getInt(QueryServices.MAX_QUERY_CONCURRENCY_ATTRIB,
            QueryServicesOptions.DEFAULT_MAX_QUERY_CONCURRENCY);
        Preconditions.checkArgument(targetConcurrency >= 1, "Invalid target concurrency: "
            + targetConcurrency);
        Preconditions.checkArgument(maxConcurrency >= targetConcurrency, "Invalid max concurrency: "
            + maxConcurrency);
        this.guidePostsDepth = props.getLong(StatisticsConstants.HISTOGRAM_BYTE_DEPTH_CONF_KEY,
                StatisticsConstants.HISTOGRAM_DEFAULT_BYTE_DEPTH);
        Preconditions.checkArgument(targetConcurrency >= 1, "Invalid target concurrency: " + targetConcurrency);
        Preconditions.checkArgument(maxConcurrency >= targetConcurrency , "Invalid max concurrency: " + maxConcurrency);
        this.maxIntraRegionParallelization = hintNode.hasHint(Hint.NO_INTRA_REGION_PARALLELIZATION) ? 1 : props.getInt(QueryServices.MAX_INTRA_REGION_PARALLELIZATION_ATTRIB,
                QueryServicesOptions.DEFAULT_MAX_INTRA_REGION_PARALLELIZATION);
        Preconditions.checkArgument(maxIntraRegionParallelization >= 1 , "Invalid max intra region parallelization: " + maxIntraRegionParallelization);
    }

    // Get the mapping between key range and the regions that contains them.
    protected List<HRegionLocation> getAllRegions() throws SQLException {
        Scan scan = context.getScan();
        PTable table = tableRef.getTable();
        List<HRegionLocation> allTableRegions = context.getConnection().getQueryServices().getAllTableRegions(table.getPhysicalName().getBytes());
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
                KeyRange regionKeyRange = KeyRange.getKeyRange(location.getRegionInfo().getStartKey(), location.getRegionInfo().getEndKey());
                return keyRange.intersect(regionKeyRange) != KeyRange.EMPTY_RANGE;
            }
        });
        return Lists.newArrayList(regions);
    }

    protected List<KeyRange> genKeyRanges(List<HRegionLocation> regions, Scan scan) {
        if (regions.isEmpty()) {
            return Collections.emptyList();
        }
        PTableStats tableStats = this.tableRef.getTable().getTableStats();
        Map<byte[], NavigableSet<byte[]>> familyMap = scan.getFamilyMap();
        // We have the guide posts per column family. We need to return atleast those many keyranges that is 
        // equal to the number of regions. 
        // The guide posts here may be spread across all the regions or the entire data may be spread across few regions
        // After we take the intersection of the available guide posts with the region start and end keys, we have to 
        // add the remaining guide posts with the intersected key ranges
        List<KeyRange> splits = Lists.newArrayListWithCapacity(regions.size()); 
        ListMultimap<HRegionLocation,KeyRange> keyRangesPerRegion = ArrayListMultimap.create(regions.size(),regions.size());
        ListMultimap<byte[],List<KeyRange>> guidePostsPerCf = ArrayListMultimap.create(regions.size(),regions.size());
        if (regions.size() == 1) {
            for (HRegionLocation region : regions) {
                splits.add(ParallelIterators.TO_KEY_RANGE.apply(region));
            }
            return splits;
        } else {
            List<KeyRange> regionStartEndKey = Lists.newArrayListWithExpectedSize(regions.size());
            Set<byte[]> keySet = familyMap.keySet();
            // Map<byte[], List<KeyRange>> guidePostsPerCf = Maps.newHashMapWithExpectedSize(keySet.size());
            List<KeyRange> guidePosts = Lists.newArrayListWithCapacity(regions.size());
            for (byte[] fam : keySet) {
                List<byte[]> gps = null;
                try {
                    gps = this.tableRef.getTable().getColumnFamily(fam).getGuidePosts();
                } catch (ColumnFamilyNotFoundException e) {
                    new SQLException("Column "+Bytes.toString(fam)+ "  not found", e);
                }
                if (gps != null) {
                    for (byte[] guidePost : gps) {
                        PhoenixArray array = (PhoenixArray)PDataType.VARBINARY_ARRAY.toObject(guidePost);
                        if (array != null && array.getDimensions() != 0) {
                            
                            if (array.getDimensions() > 1) {
                                // Should we really do like this or as we already have the byte[]
                                // of guide posts just use them
                                // per region?

                                // Adding all the collected guideposts to the key ranges
                                guidePosts.add(KeyRange.getKeyRange(HConstants.EMPTY_BYTE_ARRAY, array.toBytes(0)));
                                for (int i = 1; i < array.getDimensions() - 2; i++) {
                                    guidePosts.add(KeyRange.getKeyRange(array.toBytes(i), (array.toBytes(i + 1))));
                                }
                                guidePosts.add(KeyRange.getKeyRange(array.toBytes(array.getDimensions() - 2),
                                        (array.toBytes(array.getDimensions() - 1))));
                                guidePosts.add(KeyRange.getKeyRange(array.toBytes(array.getDimensions() - 1),
                                        (HConstants.EMPTY_BYTE_ARRAY)));
                                
                            } else {
                                byte[] gp = array.toBytes(0);
                                // guidePosts.add(KeyRange.getKeyRange(HConstants.EMPTY_BYTE_ARRAY, true, ScanUtil.createStopKey(gp),
                                // true));
                                guidePosts.add(KeyRange.getKeyRange(HConstants.EMPTY_BYTE_ARRAY, gp));
                                guidePosts.add(KeyRange.getKeyRange(gp, HConstants.EMPTY_BYTE_ARRAY));
                            }
                        }
                    }
                }
            }
            for(HRegionLocation region : regions) {
                regionStartEndKey.add(KeyRange.getKeyRange(region.getRegionInfo().getStartKey(), region.getRegionInfo()
                        .getEndKey()));
            }
            if (guidePosts.size() > 0) {
                List<KeyRange> intersect = KeyRange.intersect(guidePosts, regionStartEndKey);
                return intersect;
            } else {
                // atleast region based split should happen.
                // Will it better to have the splits per region concept here? so that
                // within a region we could still parallelize?
                return regionStartEndKey;
            }
        }
    }

    @Override
    public List<KeyRange> getSplits() throws SQLException {
        return genKeyRanges(getAllRegions(), context.getScan());
    }

    @Override
    public int getSplitsPerRegion(int numRegions) {
      int splitsPerRegion =
          numRegions >= targetConcurrency ? 1
                  : (numRegions > targetConcurrency / 2 ? maxConcurrency : targetConcurrency)
                          / numRegions;
  return Math.min(splitsPerRegion, maxIntraRegionParallelization);
    }
}
