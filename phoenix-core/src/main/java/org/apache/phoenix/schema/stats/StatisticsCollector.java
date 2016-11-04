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

import java.io.Closeable;
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
 * Statistics tracker that helps to collect stats like min key, max key and guideposts.
 */
public interface StatisticsCollector extends Closeable {

    /** Constant used if no max timestamp is available */
    long NO_TIMESTAMP = -1;

    /**
     * Returns the maximum timestamp of all cells encountered while collecting statistics.
     */
    long getMaxTimeStamp();

    /**
     * Write the collected statistics for the given region over the scan provided.
     */
    void updateStatistic(Region region, Scan scan);

    /**
     * Collect statistics for the given list of cells. This method can be called multiple times
     * during collection of statistics.
     * @throws IOException 
     */
    void collectStatistics(List<Cell> results);

    /**
     * Wrap a compaction scanner with a scanner that will collect statistics using this instance.
     */
    InternalScanner createCompactionScanner(RegionCoprocessorEnvironment env, Store store,
            InternalScanner delegate) throws IOException;

    /**
     * Called before beginning the collection of statistics through {@link #collectStatistics(List)}
     * @throws IOException 
     */
    void init() throws IOException;

    /**
     * Retrieve the calculated guide post info for the given column family.
     */
    GuidePostsInfo getGuidePosts(ImmutableBytesPtr fam);
}
