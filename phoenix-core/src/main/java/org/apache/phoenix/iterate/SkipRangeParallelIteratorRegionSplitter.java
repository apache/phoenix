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
import java.util.List;

import org.apache.hadoop.hbase.HRegionLocation;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.phoenix.compile.ScanRanges;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.parse.HintNode;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.schema.SaltingUtil;
import org.apache.phoenix.schema.TableRef;


/**
 * Split the region according to the information contained in the scan's SkipScanFilter.
 */
public class SkipRangeParallelIteratorRegionSplitter extends DefaultParallelIteratorRegionSplitter {

    public static SkipRangeParallelIteratorRegionSplitter getInstance(StatementContext context, TableRef table, HintNode hintNode) {
        return new SkipRangeParallelIteratorRegionSplitter(context, table, hintNode);
    }

    protected SkipRangeParallelIteratorRegionSplitter(StatementContext context, TableRef table, HintNode hintNode) {
        super(context, table, hintNode);
    }

    @Override
    protected List<HRegionLocation> getAllRegions() throws SQLException {
        List<HRegionLocation> allTableRegions = context.getConnection().getQueryServices().getAllTableRegions(tableRef.getTable().getPhysicalName().getBytes());
        return filterRegions(allTableRegions, context.getScanRanges());
    }

    public List<HRegionLocation> filterRegions(List<HRegionLocation> allTableRegions, final ScanRanges ranges) {
        Iterable<HRegionLocation> regions;
        if (ranges == ScanRanges.EVERYTHING) {
            return allTableRegions;
        } else if (ranges == ScanRanges.NOTHING) { // TODO: why not emptyList?
            return Lists.<HRegionLocation>newArrayList();
        } else {
            regions = Iterables.filter(allTableRegions,
                    new Predicate<HRegionLocation>() {
                    @Override
                    public boolean apply(HRegionLocation region) {
                        KeyRange minMaxRange = context.getMinMaxRange();
                        if (minMaxRange != null) {
                            KeyRange range = KeyRange.getKeyRange(region.getRegionInfo().getStartKey(), region.getRegionInfo().getEndKey());
                            if (tableRef.getTable().getBucketNum() != null) {
                                // Add salt byte, as minMaxRange won't have it
                                minMaxRange = SaltingUtil.addSaltByte(region.getRegionInfo().getStartKey(), minMaxRange);
                            }
                            range = range.intersect(minMaxRange);
                            return ranges.intersect(range.getLowerRange(), range.getUpperRange());
                        }
                        return ranges.intersect(region.getRegionInfo().getStartKey(), region.getRegionInfo().getEndKey());
                    }
            });
        }
        return Lists.newArrayList(regions);
    }

}
