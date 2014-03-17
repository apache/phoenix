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
package org.apache.phoenix.index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.phoenix.compile.ScanRanges;
import org.apache.phoenix.hbase.index.covered.CoveredColumnsIndexBuilder;
import org.apache.phoenix.hbase.index.util.IndexManagementUtil;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.schema.PDataType;
import org.apache.phoenix.util.SchemaUtil;

import com.google.common.collect.Lists;

/**
 * Index builder for covered-columns index that ties into phoenix for faster use.
 */
public class PhoenixIndexBuilder extends CoveredColumnsIndexBuilder {

    @Override
    public void batchStarted(MiniBatchOperationInProgress<Mutation> miniBatchOp) throws IOException {
        // The entire purpose of this method impl is to get the existing rows for the
        // table rows being indexed into the block cache, as the index maintenance code
        // does a point scan per row
        List<KeyRange> keys = Lists.newArrayListWithExpectedSize(miniBatchOp.size());
        List<IndexMaintainer> maintainers = new ArrayList<IndexMaintainer>();
        for (int i = 0; i < miniBatchOp.size(); i++) {
            Mutation m = miniBatchOp.getOperation(i);
            keys.add(PDataType.VARBINARY.getKeyRange(m.getRow()));
            maintainers.addAll(getCodec().getIndexMaintainers(m.getAttributesMap()));
        }
        Scan scan = IndexManagementUtil.newLocalStateScan(maintainers);
        ScanRanges scanRanges = ScanRanges.create(Collections.singletonList(keys), SchemaUtil.VAR_BINARY_SCHEMA);
        scanRanges.setScanStartStopRow(scan);
        scan.setFilter(scanRanges.getSkipScanFilter());
        HRegion region = this.env.getRegion();
        RegionScanner scanner = region.getScanner(scan);
        // Run through the scanner using internal nextRaw method
        region.startRegionOperation();
        try {
            boolean hasMore;
            do {
                List<Cell> results = Lists.newArrayList();
                // Results are potentially returned even when the return value of s.next is false
                // since this is an indication of whether or not there are more values after the
                // ones returned
                hasMore = scanner.nextRaw(results);
            } while (hasMore);
        } finally {
            try {
                scanner.close();
            } finally {
                region.closeRegionOperation();
            }
        }
    }

    private PhoenixIndexCodec getCodec() {
        return (PhoenixIndexCodec)this.codec;
    }
    
    @Override
    public byte[] getBatchId(Mutation m){
        return this.codec.getBatchId(m);
    }
}