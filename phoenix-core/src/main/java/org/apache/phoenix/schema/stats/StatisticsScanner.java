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

import static org.apache.phoenix.query.QueryServices.COMMIT_STATS_ASYNC;
import static org.apache.phoenix.query.QueryServicesOptions.DEFAULT_COMMIT_STATS_ASYNC;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.regionserver.ScannerContext;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;

/**
 * The scanner that does the scanning to collect the stats during major compaction.{@link DefaultStatisticsCollector}
 */
public class StatisticsScanner implements InternalScanner {
    private static final Log LOG = LogFactory.getLog(StatisticsScanner.class);
    private InternalScanner delegate;
    private StatisticsWriter statsWriter;
    private Region region;
    private StatisticsCollector tracker;
    private ImmutableBytesPtr family;
    private final Configuration config;
    private final RegionServerServices regionServerServices;

    public StatisticsScanner(StatisticsCollector tracker, StatisticsWriter stats, RegionCoprocessorEnvironment env,
            InternalScanner delegate, ImmutableBytesPtr family) {
        this.tracker = tracker;
        this.statsWriter = stats;
        this.delegate = delegate;
        this.regionServerServices = env.getRegionServerServices();
        this.region = env.getRegion();
        this.family = family;
        this.config = env.getConfiguration();
        StatisticsCollectionRunTracker.getInstance(config).addCompactingRegion(region.getRegionInfo());
    }

    @Override
    public boolean next(List<Cell> result) throws IOException {
        boolean ret = delegate.next(result);
        updateStats(result);
        return ret;
    }

    @Override
    public boolean next(List<Cell> result, ScannerContext scannerContext) throws IOException {
        boolean ret = delegate.next(result, scannerContext);
        updateStats(result);
        return ret;
    }

    /**
     * Update the current statistics based on the lastest batch of key-values from the underlying scanner
     *
     * @param results
     *            next batch of {@link KeyValue}s
     * @throws IOException 
     */
    private void updateStats(final List<Cell> results) throws IOException {
        if (!results.isEmpty()) {
            tracker.collectStatistics(results);
        }
    }

    @Override
    public void close() throws IOException {
        boolean async = getConfig().getBoolean(COMMIT_STATS_ASYNC, DEFAULT_COMMIT_STATS_ASYNC);
        StatisticsCollectionRunTracker collectionTracker = getStatsCollectionRunTracker(config);
        StatisticsScannerCallable callable = createCallable();
        if (getRegionServerServices().isStopping() || getRegionServerServices().isStopped()) {
            LOG.debug("Not updating table statistics because the server is stopping/stopped");
            return;
        }
        if (!async) {
            callable.call();
        } else {
            collectionTracker.runTask(callable);
        }
    }

    // VisibleForTesting
    StatisticsCollectionRunTracker getStatsCollectionRunTracker(Configuration c) {
        return StatisticsCollectionRunTracker.getInstance(c);
    }

    Configuration getConfig() {
        return config;
    }

    StatisticsWriter getStatisticsWriter() {
        return statsWriter;
    }

    RegionServerServices getRegionServerServices() {
        return regionServerServices;
    }

    Region getRegion() {
        return region;
    }

    StatisticsScannerCallable createCallable() {
        return new StatisticsScannerCallable();
    }

    StatisticsCollector getTracker() {
        return tracker;
    }

    InternalScanner getDelegate() {
        return delegate;
    }

    class StatisticsScannerCallable implements Callable<Void> {
        @Override
        public Void call() throws IOException {
            IOException toThrow = null;
            StatisticsCollectionRunTracker collectionTracker = getStatsCollectionRunTracker(config);
            final HRegionInfo regionInfo = getRegion().getRegionInfo();
            try {
                // update the statistics table
                // Just verify if this if fine
                ArrayList<Mutation> mutations = new ArrayList<Mutation>();

                if (LOG.isDebugEnabled()) {
                    LOG.debug("Deleting the stats for the region " + regionInfo.getRegionNameAsString()
                            + " as part of major compaction");
                }
                getStatisticsWriter().deleteStats(region, tracker, family, mutations);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Adding new stats for the region " + regionInfo.getRegionNameAsString()
                            + " as part of major compaction");
                }
                getStatisticsWriter().addStats(tracker, family, mutations);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Committing new stats for the region " + regionInfo.getRegionNameAsString()
                            + " as part of major compaction");
                }
                getStatisticsWriter().commitStats(mutations, tracker);
            } catch (IOException e) {
                if (getRegionServerServices().isStopping() || getRegionServerServices().isStopped()) {
                    LOG.debug("Ignoring error updating statistics because region is closing/closed");
                } else {
                    LOG.error("Failed to update statistics table!", e);
                    toThrow = e;
                }
            } finally {
                try {
                    collectionTracker.removeCompactingRegion(regionInfo);
                    getStatisticsWriter().close();// close the writer
                    getTracker().close();// close the tracker
                } catch (IOException e) {
                    if (toThrow == null) toThrow = e;
                    LOG.error("Error while closing the stats table", e);
                } finally {
                    // close the delegate scanner
                    try {
                        getDelegate().close();
                    } catch (IOException e) {
                        if (toThrow == null) toThrow = e;
                        LOG.error("Error while closing the scanner", e);
                    } finally {
                        if (toThrow != null) { throw toThrow; }
                    }
                }
            }
            return null;
        }
    }

}
