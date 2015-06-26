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

import java.util.List;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTable.IndexType;
import org.apache.phoenix.schema.SaltingUtil;
import org.apache.phoenix.util.ScanUtil;

/**
 * Default implementation that creates a scan group if a plan is row key ordered (which requires a merge sort),
 * or if a scan crosses a region boundary and the table is salted or a local index.   
 */
public class DefaultParallelScanGrouper implements ParallelScanGrouper {
	
	private static final DefaultParallelScanGrouper INSTANCE = new DefaultParallelScanGrouper();

    public static DefaultParallelScanGrouper getInstance() {
        return INSTANCE;
    }
    
    private DefaultParallelScanGrouper() {}

	@Override
	public boolean shouldStartNewScan(QueryPlan plan, List<Scan> scans, byte[] startKey, boolean crossedRegionBoundary) {
		PTable table = plan.getTableRef().getTable();
		boolean startNewScanGroup = false;
        if (!plan.isRowKeyOrdered()) {
            startNewScanGroup = true;
        } else if (crossedRegionBoundary) {
            if (table.getIndexType() == IndexType.LOCAL) {
                startNewScanGroup = true;
            } else if (table.getBucketNum() != null) {
                startNewScanGroup = scans.isEmpty() ||
                        ScanUtil.crossesPrefixBoundary(startKey,
                                ScanUtil.getPrefix(scans.get(scans.size()-1).getStartRow(), SaltingUtil.NUM_SALTING_BYTES), 
                                SaltingUtil.NUM_SALTING_BYTES);
            }
        }
        return startNewScanGroup;
    }

}
