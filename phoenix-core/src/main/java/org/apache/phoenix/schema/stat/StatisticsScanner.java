/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership. The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the License.
 */
package org.apache.phoenix.schema.stat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * The scanner that does the scanning to collect the stats during major compaction.{@link StatisticsCollector}
 */
public class StatisticsScanner implements InternalScanner {
    private static final Log LOG = LogFactory.getLog(StatisticsScanner.class);
    private InternalScanner delegate;
    private StatisticsTable stats;
    private HRegion region;
    private StatisticsCollector tracker;
    private byte[] family;

    public StatisticsScanner(StatisticsCollector tracker, StatisticsTable stats, HRegion region,
            InternalScanner delegate, byte[] family) {
        // should there be only one tracker?
        this.tracker = tracker;
        this.stats = stats;
        this.delegate = delegate;
        this.region = region;
        this.family = family;
        this.tracker.clear();
    }

    @Override
    public boolean next(List<KeyValue> result) throws IOException {
        boolean ret = delegate.next(result);
        updateStat(result);
        return ret;
    }

    @Override
    public boolean next(List<KeyValue> result, int limit) throws IOException {
        boolean ret = delegate.next(result, limit);
        updateStat(result);
        return ret;
    }

    /**
     * Update the current statistics based on the lastest batch of key-values from the underlying scanner
     * 
     * @param results
     *            next batch of {@link KeyValue}s
     */
    protected void updateStat(final List<KeyValue> results) {
        for (KeyValue kv : results) {
            if (kv.getType() == KeyValue.Type.Put.getCode()) {
                tracker.updateStatistic(kv);
            }
        }
    }

    @Override
    public void close() throws IOException {
        IOException toThrow = null;
        try {
            // update the statistics table
            // Just verify if this if fine
            ArrayList<Mutation> mutations = new ArrayList<Mutation>();
            if (LOG.isDebugEnabled()) {
                LOG.debug("Deleting the stats for the region " + region.getRegionNameAsString()
                        + " as part of major compaction");
            }
            stats.deleteStats(region.getRegionNameAsString(), this.tracker, Bytes.toString(family), mutations);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Adding new stats for the region " + region.getRegionNameAsString()
                        + " as part of major compaction");
            }
            stats.addStats(region.getRegionNameAsString(), this.tracker, Bytes.toString(family), mutations);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Committing new stats for the region " + region.getRegionNameAsString()
                        + " as part of major compaction");
            }
            stats.commitStats(mutations);
        } catch (IOException e) {
            LOG.error("Failed to update statistics table!", e);
            toThrow = e;
        } finally {
            try {
                stats.close();
            } catch (IOException e) {
                if (toThrow == null) toThrow = e;
                LOG.error("Error while closing the stats table", e);
            } finally {
                // close the delegate scanner
                try {
                    delegate.close();
                } catch (IOException e) {
                    if (toThrow == null) toThrow = e;
                    LOG.error("Error while closing the scanner", e);
                } finally {
                    if (toThrow != null) {
                        throw toThrow;
                    }
                }
            }
        }
    }

    @Override
    public boolean next(List<KeyValue> result, String metric) throws IOException {
        boolean ret = delegate.next(result, metric);
        updateStat(result);
        return ret;
    }

    @Override
    public boolean next(List<KeyValue> result, int limit, String metric) throws IOException {
        boolean ret = delegate.next(result, limit, metric);
        updateStat(result);
        return ret;
    }
}