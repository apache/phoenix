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
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TimeKeeper;

/**
 * The scanner that does the scanning to collect the stats during major compaction.{@link StatisticsCollector}
 */
public class StatisticsScanner implements InternalScanner {
    private static final Log LOG = LogFactory.getLog(StatisticsScanner.class);
    private InternalScanner delegate;
    private StatisticsTable stats;
    private HRegionInfo region;
    private StatisticsTracker tracker;
    private byte[] family;

    public StatisticsScanner(StatisticsTracker tracker, StatisticsTable stats, HRegionInfo region,
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
    public boolean next(List<Cell> result) throws IOException {
        boolean ret = delegate.next(result);
        updateStat(result);
        return ret;
    }

    @Override
    public boolean next(List<Cell> result, int limit) throws IOException {
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
    protected void updateStat(final List<Cell> results) {
        for (Cell c : results) {
            KeyValue kv = KeyValueUtil.ensureKeyValue(c);
            if (c.getTypeByte() == KeyValue.Type.Put.getCode()) {
                tracker.updateStatistic(kv);
            }
        }
    }

    public void close() throws IOException {
        IOException toThrow = null;
        try {
            // update the statistics table
            // Just verify if this if fine
            String tableName = SchemaUtil.getTableNameFromFullName(region.getTable().getNameAsString());
            ArrayList<Mutation> mutations = new ArrayList<Mutation>();
            long currentTime = TimeKeeper.SYSTEM.getCurrentTime();
            if (LOG.isDebugEnabled()) {
                LOG.debug("Deleting the stats for the region " + region.getRegionNameAsString()
                        + " as part of major compaction");
            }
            stats.deleteStats(tableName, region.getRegionNameAsString(), this.tracker, Bytes.toString(family),
                    mutations, currentTime);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Adding new stats for the region " + region.getRegionNameAsString()
                        + " as part of major compaction");
            }
            stats.addStats(tableName, region.getRegionNameAsString(), this.tracker, Bytes.toString(family), mutations,
                    currentTime);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Committing new stats for the region " + region.getRegionNameAsString()
                        + " as part of major compaction");
            }
            stats.commitStats(mutations);
        } catch (IOException e) {
            LOG.error("Failed to update statistics table!", e);
            toThrow = e;
        }
        // close the delegate scanner
        try {
            delegate.close();
        } catch (IOException e) {
            LOG.error("Error while closing the scanner");
            // TODO : We should throw the exception
            /*if (toThrow == null) { throw e; }
            throw MultipleIOException.createIOException(Lists.newArrayList(toThrow, e));*/
        }
    }
}