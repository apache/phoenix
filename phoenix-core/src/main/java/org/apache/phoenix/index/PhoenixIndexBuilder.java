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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.phoenix.compile.ScanRanges;
import org.apache.phoenix.filter.SkipScanFilter;
import org.apache.phoenix.hbase.index.covered.IndexMetaData;
import org.apache.phoenix.hbase.index.covered.NonTxIndexBuilder;
import org.apache.phoenix.hbase.index.util.IndexManagementUtil;
import org.apache.phoenix.hbase.index.write.IndexWriter;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.schema.types.PVarbinary;

import com.google.common.collect.Lists;

/**
 * Index builder for covered-columns index that ties into phoenix for faster use.
 */
public class PhoenixIndexBuilder extends NonTxIndexBuilder {

    @Override
    public IndexMetaData getIndexMetaData(MiniBatchOperationInProgress<Mutation> miniBatchOp) throws IOException {
        return new PhoenixIndexMetaData(env, miniBatchOp.getOperation(0).getAttributesMap());
    }

    protected PhoenixIndexCodec getCodec() {
        return (PhoenixIndexCodec)codec;
    }

    @Override
    public void setup(RegionCoprocessorEnvironment env) throws IOException {
        super.setup(env);
        Configuration conf = env.getConfiguration();
        // Install handler that will attempt to disable the index first before killing the region
        // server
        conf.setIfUnset(IndexWriter.INDEX_FAILURE_POLICY_CONF_KEY,
            PhoenixIndexFailurePolicy.class.getName());
    }

    @Override
    public void batchStarted(MiniBatchOperationInProgress<Mutation> miniBatchOp, IndexMetaData context) throws IOException {
        // The entire purpose of this method impl is to get the existing rows for the
        // table rows being indexed into the block cache, as the index maintenance code
        // does a point scan per row.
        List<IndexMaintainer> indexMaintainers = ((PhoenixIndexMetaData)context).getIndexMaintainers();
        List<KeyRange> keys = Lists.newArrayListWithExpectedSize(miniBatchOp.size());
        Map<ImmutableBytesWritable, IndexMaintainer> maintainers =
                new HashMap<ImmutableBytesWritable, IndexMaintainer>();
        ImmutableBytesWritable indexTableName = new ImmutableBytesWritable();
        for (int i = 0; i < miniBatchOp.size(); i++) {
            Mutation m = miniBatchOp.getOperation(i);
            keys.add(PVarbinary.INSTANCE.getKeyRange(m.getRow()));
            
            for(IndexMaintainer indexMaintainer: indexMaintainers) {
                if (indexMaintainer.isImmutableRows()) continue;
                indexTableName.set(indexMaintainer.getIndexTableName());
                if (maintainers.get(indexTableName) != null) continue;
                maintainers.put(indexTableName, indexMaintainer);
            }

        }
        if (maintainers.isEmpty()) return;
        Scan scan = IndexManagementUtil.newLocalStateScan(new ArrayList<IndexMaintainer>(maintainers.values()));
        ScanRanges scanRanges = ScanRanges.createPointLookup(keys);
        scanRanges.initializeScan(scan);
        scan.setFilter(new SkipScanFilter(scanRanges.getSkipScanFilter(),true));
        Region region = env.getRegion();
        RegionScanner scanner = region.getScanner(scan);
        // Run through the scanner using internal nextRaw method
        region.startRegionOperation();
        try {
            synchronized (scanner) {
                boolean hasMore;
                do {
                    List<Cell> results = Lists.newArrayList();
                    // Results are potentially returned even when the return value of s.next is
                    // false since this is an indication of whether or not there are more values
                    // after the ones returned
                    hasMore = scanner.nextRaw(results);
                } while (hasMore);
            }
        } finally {
            try {
                scanner.close();
            } finally {
                region.closeRegionOperation();
            }
        }
    }
}