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
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.parse.HintNode;
import org.apache.phoenix.parse.HintNode.Hint;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.query.StatsManager;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.util.ReadOnlyProps;


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

    protected List<KeyRange> genKeyRanges(List<HRegionLocation> regions) {
        if (regions.isEmpty()) {
            return Collections.emptyList();
        }
        
        StatsManager statsManager = context.getConnection().getQueryServices().getStatsManager();
        // the splits are computed as follows:
        //
        // let's suppose:
        // t = target concurrency
        // m = max concurrency
        // r = the number of regions we need to scan
        //
        // if r >= t:
        //    scan using regional boundaries
        // elif r > t/2:
        //    split each region in s splits such that:
        //    s = max(x) where s * x < m
        // else:
        //    split each region in s splits such that:
        //    s = max(x) where s * x < t
        //
        // The idea is to align splits with region boundaries. If rows are not evenly
        // distributed across regions, using this scheme compensates for regions that
        // have more rows than others, by applying tighter splits and therefore spawning
        // off more scans over the overloaded regions.
        int splitsPerRegion = regions.size() >= targetConcurrency ? 1 : (regions.size() > targetConcurrency / 2 ? maxConcurrency : targetConcurrency) / regions.size();
        splitsPerRegion = Math.min(splitsPerRegion, maxIntraRegionParallelization);
        // Create a multi-map of ServerName to List<KeyRange> which we'll use to round robin from to ensure
        // that we keep each region server busy for each query.
        ListMultimap<HRegionLocation,KeyRange> keyRangesPerRegion = ArrayListMultimap.create(regions.size(),regions.size() * splitsPerRegion);;
        if (splitsPerRegion == 1) {
            for (HRegionLocation region : regions) {
                keyRangesPerRegion.put(region, ParallelIterators.TO_KEY_RANGE.apply(region));
            }
        } else {
            // Maintain bucket for each server and then returns KeyRanges in round-robin
            // order to ensure all servers are utilized.
            for (HRegionLocation region : regions) {
                byte[] startKey = region.getRegionInfo().getStartKey();
                byte[] stopKey = region.getRegionInfo().getEndKey();
                boolean lowerUnbound = Bytes.compareTo(startKey, HConstants.EMPTY_START_ROW) == 0;
                boolean upperUnbound = Bytes.compareTo(stopKey, HConstants.EMPTY_END_ROW) == 0;
                /*
                 * If lower/upper unbound, get the min/max key from the stats manager.
                 * We use this as the boundary to split on, but we still use the empty
                 * byte as the boundary in the actual scan (in case our stats are out
                 * of date).
                 */
                if (lowerUnbound) {
                    startKey = statsManager.getMinKey(tableRef);
                    if (startKey == null) {
                        keyRangesPerRegion.put(region,ParallelIterators.TO_KEY_RANGE.apply(region));
                        continue;
                    }
                }
                if (upperUnbound) {
                    stopKey = statsManager.getMaxKey(tableRef);
                    if (stopKey == null) {
                        keyRangesPerRegion.put(region,ParallelIterators.TO_KEY_RANGE.apply(region));
                        continue;
                    }
                }
                
                byte[][] boundaries = null;
                // Both startKey and stopKey will be empty the first time
                if (Bytes.compareTo(startKey, stopKey) >= 0 || (boundaries = Bytes.split(startKey, stopKey, splitsPerRegion - 1)) == null) {
                    // Bytes.split may return null if the key space
                    // between start and end key is too small
                    keyRangesPerRegion.put(region,ParallelIterators.TO_KEY_RANGE.apply(region));
                } else {
                    keyRangesPerRegion.put(region,KeyRange.getKeyRange(lowerUnbound ? KeyRange.UNBOUND : boundaries[0], boundaries[1]));
                    if (boundaries.length > 1) {
                        for (int i = 1; i < boundaries.length-2; i++) {
                            keyRangesPerRegion.put(region,KeyRange.getKeyRange(boundaries[i], true, boundaries[i+1], false));
                        }
                        keyRangesPerRegion.put(region,KeyRange.getKeyRange(boundaries[boundaries.length-2], true, upperUnbound ? KeyRange.UNBOUND : boundaries[boundaries.length-1], false));
                    }
                }
            }
        }
        List<KeyRange> splits = Lists.newArrayListWithCapacity(regions.size() * splitsPerRegion);
        // as documented for ListMultimap
        Collection<Collection<KeyRange>> values = keyRangesPerRegion.asMap().values();
        List<Collection<KeyRange>> keyRangesList = Lists.newArrayList(values);
        // Randomize range order to ensure that we don't hit the region servers in the same order every time
        // thus helping to distribute the load more evenly
        Collections.shuffle(keyRangesList);
        // Transpose values in map to get regions in round-robin server order. This ensures that
        // all servers will be used to process the set of parallel threads available in our executor.
        int i = 0;
        boolean done;
        do {
            done = true;
            for (int j = 0; j < keyRangesList.size(); j++) {
                List<KeyRange> keyRanges = (List<KeyRange>)keyRangesList.get(j);
                if (i < keyRanges.size()) {
                    splits.add(keyRanges.get(i));
                    done = false;
                }
            }
            i++;
        } while (!done);
        return splits;
    }

    @Override
    public List<KeyRange> getSplits() throws SQLException {
        return genKeyRanges(getAllRegions());
    }
}
