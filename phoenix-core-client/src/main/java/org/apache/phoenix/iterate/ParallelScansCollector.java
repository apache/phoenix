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

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.phoenix.compile.QueryPlan;

/**
 * Stores some state to help build the parallel Scans structure
 */
public class ParallelScansCollector {

    private final ParallelScanGrouper grouper;
    private boolean lastScanCrossedRegionBoundary = false;
    private final List<List<Scan>> parallelScans = new ArrayList<>();
    private List<Scan> lastBatch = new ArrayList<>();
    private Scan lastScan = null;
    private final List<HRegionLocation> regionLocations = new ArrayList<>();

    public ParallelScansCollector(ParallelScanGrouper grouper) {
        this.grouper = grouper;
        parallelScans.add(lastBatch);
    }

    public void addNewScan(QueryPlan plan, Scan newScan, boolean crossesRegionBoundary,
                           HRegionLocation regionLocation) {
        if (grouper.shouldStartNewScan(plan, lastScan, newScan.getStartRow(),
                lastScanCrossedRegionBoundary)) {
            lastBatch = new ArrayList<>();
            parallelScans.add(lastBatch);
        }
        lastBatch.add(newScan);
        regionLocations.add(regionLocation);

        lastScanCrossedRegionBoundary = crossesRegionBoundary;
        lastScan = newScan;
    }

    public List<List<Scan>> getParallelScans() {
        return parallelScans;
    }

    public List<HRegionLocation> getRegionLocations() {
        return regionLocations;
    }
}
