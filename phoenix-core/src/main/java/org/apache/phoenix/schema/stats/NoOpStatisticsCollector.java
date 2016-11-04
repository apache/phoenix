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
package org.apache.phoenix.schema.stats;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;

/**
 * A drop-in statistics collector that does nothing. An instance of this class is used for tables
 * or environments where statistics collection is disabled.
 */
public class NoOpStatisticsCollector implements StatisticsCollector {

    @Override
    public long getMaxTimeStamp() {
        return NO_TIMESTAMP;
    }

    @Override
    public void close() throws IOException {
        // No-op
    }

    @Override
    public void updateStatistic(Region region, Scan scan) {
        // No-op
    }

    @Override
    public void collectStatistics(List<Cell> results) {
        // No-op
    }

    @Override
    public InternalScanner createCompactionScanner(RegionCoprocessorEnvironment env, Store store,
            InternalScanner delegate) throws IOException {
        return delegate;
    }

    @Override 
    public void init() {
        // No-op
    }

    @Override public GuidePostsInfo getGuidePosts(ImmutableBytesPtr fam) {
        return null;
    }
}
