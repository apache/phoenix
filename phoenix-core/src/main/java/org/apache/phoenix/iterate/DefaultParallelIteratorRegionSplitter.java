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

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.parse.HintNode;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.schema.PColumnFamily;
import org.apache.phoenix.schema.PDataType;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PhoenixArray;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.stat.StatisticsConstants;
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
        this.guidePostsDepth = props.getLong(StatisticsConstants.HISTOGRAM_BYTE_DEPTH_CONF_KEY,
                StatisticsConstants.HISTOGRAM_DEFAULT_BYTE_DEPTH);
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
        Map<byte[], NavigableSet<byte[]>> familyMap = scan.getFamilyMap();
        PTable table = this.tableRef.getTable();
        List<PColumnFamily> columnFamilies = table.getColumnFamilies();
        byte[] defaultCF = SchemaUtil.getEmptyColumnFamily(table);
        //boolean containsDefaultCF = false;
        List<byte[]> gps = null;
/*        if(columnFamilies.isEmpty()) {
            //Cases where column families are empty
            TreeMap<byte[], List<byte[]>> tableGps = table.getTableStats().getGuidePosts();
            // Collect from table here
            gps = tableGps.get(defaultCF);
        } else {
            containsDefaultCF = familyMap.containsKey(defaultCF);
        }*/
        
        try {
            if (table.getColumnFamilies().isEmpty()) {
                // For sure we can get the defaultCF from the table
                gps = table.getTableStats().getGuidePosts().get(defaultCF);
            } else {
                if (scan.getFamilyMap().size() > 0) {
                    if (scan.getFamilyMap().containsKey(defaultCF)) { // Favor using default CF if it's used in scan
                        gps = table.getColumnFamily(defaultCF).getGuidePosts();
                    } else { // Otherwise, just use first CF in use by scan
                        gps = table.getColumnFamily(scan.getFamilyMap().keySet().iterator().next()).getGuidePosts();
                    }
                } else {
                    gps = table.getTableStats().getGuidePosts().get(defaultCF);
                }
            }
        } catch (Exception cfne) {
            logger.error("Error while getting guideposts for the cf " + Bytes.toString(defaultCF));
        }

        List<KeyRange> regionStartEndKey = Lists.newArrayListWithExpectedSize(regions.size());
        for (HRegionLocation region : regions) {
            regionStartEndKey.add(KeyRange.getKeyRange(region.getRegionInfo().getStartKey(), region.getRegionInfo()
                    .getEndKey()));
        }
        List<KeyRange> guidePosts = Lists.newArrayListWithCapacity(regions.size());
        List<byte[]> guidePostsBytes = Lists.newArrayListWithCapacity(regions.size());
/*        PColumnFamily cftoUse = null;
        if (containsDefaultCF || familyMap.isEmpty()) {
            // Check from the cfs associated with the table
            for (PColumnFamily fam : columnFamilies) {
                if (Bytes.equals(defaultCF, fam.getName().getBytes())) {
                    cftoUse = fam;
                    gps = cftoUse.getGuidePosts();
                    break;
                }
            }
        } else {
            if (familyMap.size() > 0) {
                // 
                byte[] first = familyMap.values().iterator().next().first();
                for (PColumnFamily fam : columnFamilies) {
                    if (Bytes.equals(first, fam.getName().getBytes())) {
                        cftoUse = fam;
                        gps = cftoUse.getGuidePosts();
                        break;
                    }
                }
            }
        }*/
        // Only one cf to be used here
        if (gps != null) {
            // the guide posts will arrive in sorted order here as we are focusing on only one cf
            for (byte[] guidePost : gps) {
                PhoenixArray array = (PhoenixArray)PDataType.VARBINARY_ARRAY.toObject(guidePost);
                if (array != null && array.getDimensions() != 0) {
                    for (int j = 0; j < array.getDimensions(); j++) {
                        guidePostsBytes.add(array.toBytes(j));
                    }
                }
                /*PhoenixArray array = (PhoenixArray)PDataType.VARBINARY_ARRAY.toObject(guidePost);
                if (array != null && array.getDimensions() != 0) {
                    if (array.getDimensions() > 1) {
                        // Should we really do like this or as we already have the byte[]
                        // of guide posts just use them
                        // per region?
                        // Adding all the collected guideposts to the key ranges
                        guidePosts.add(KeyRange.getKeyRange(HConstants.EMPTY_BYTE_ARRAY, array.toBytes(0)));
                        for (int i = 0; i < array.getDimensions() - 2; i++) {
                            guidePosts.add(KeyRange.getKeyRange(array.toBytes(i), (array.toBytes(i + 1))));
                        }
                        guidePosts.add(KeyRange.getKeyRange(array.toBytes(array.getDimensions() - 2),
                                (array.toBytes(array.getDimensions() - 1))));
                        guidePosts.add(KeyRange.getKeyRange(array.toBytes(array.getDimensions() - 1),
                                (HConstants.EMPTY_BYTE_ARRAY)));

                    } else {
                        byte[] gp = array.toBytes(0);
                        guidePosts.add(KeyRange.getKeyRange(HConstants.EMPTY_BYTE_ARRAY, gp));
                        guidePosts.add(KeyRange.getKeyRange(gp, HConstants.EMPTY_BYTE_ARRAY));
                    }
                }*/
            }
        }
        int size = guidePostsBytes.size();
        if (size > 0) {
            if (size > 1) {
                guidePosts.add(KeyRange.getKeyRange(HConstants.EMPTY_BYTE_ARRAY, guidePostsBytes.get(0)));
                for (int i = 0; i < size - 2; i++) {
                    guidePosts.add(KeyRange.getKeyRange(guidePostsBytes.get(i), (guidePostsBytes.get(i + 1))));
                }
                guidePosts.add(KeyRange.getKeyRange(guidePostsBytes.get(size - 2), (guidePostsBytes.get(size - 1))));
                guidePosts.add(KeyRange.getKeyRange(guidePostsBytes.get(size - 1), (HConstants.EMPTY_BYTE_ARRAY)));
            } else {
                byte[] gp = guidePostsBytes.get(0);
                guidePosts.add(KeyRange.getKeyRange(HConstants.EMPTY_BYTE_ARRAY, gp));
                guidePosts.add(KeyRange.getKeyRange(gp, HConstants.EMPTY_BYTE_ARRAY));
            }

        }
        if (guidePosts.size() > 0) {
            List<KeyRange> intersect = KeyRange.intersect(guidePosts, regionStartEndKey);
            return intersect;
        } else {
            return regionStartEndKey;
        }
    }

    @Override
    public List<KeyRange> getSplits() throws SQLException {
        return genKeyRanges(getAllRegions());
    }
}
