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

import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.phoenix.compile.StatementContext;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * ParallelScanGrouper implementation used for testing Phoenix-MapReduce Integration
 */
public class TestingMapReduceParallelScanGrouper extends MapReduceParallelScanGrouper {

    private static final AtomicInteger numCallsToGetRegionBoundaries = new AtomicInteger(0);
    private static final TestingMapReduceParallelScanGrouper INSTANCE =
            new TestingMapReduceParallelScanGrouper();

    public static TestingMapReduceParallelScanGrouper getInstance() {
        return INSTANCE;
    }

    @Override
    public List<HRegionLocation> getRegionBoundaries(StatementContext context,
            byte[] tableName) throws SQLException {
        List<HRegionLocation> regionLocations = super.getRegionBoundaries(context, tableName);
        numCallsToGetRegionBoundaries.incrementAndGet();
        return regionLocations;
    }

    @Override
    public List<HRegionLocation> getRegionBoundaries(StatementContext context, byte[] tableName,
        byte[] startRegionBoundaryKey, byte[] stopRegionBoundaryKey) throws SQLException {
        List<HRegionLocation> regionLocations =
            super.getRegionBoundaries(context, tableName, startRegionBoundaryKey,
                stopRegionBoundaryKey);
        numCallsToGetRegionBoundaries.incrementAndGet();
        return regionLocations;
    }

    public static int getNumCallsToGetRegionBoundaries() {
        return numCallsToGetRegionBoundaries.get();
    }

    public static void clearNumCallsToGetRegionBoundaries() {
        numCallsToGetRegionBoundaries.set(0);
    }
}
