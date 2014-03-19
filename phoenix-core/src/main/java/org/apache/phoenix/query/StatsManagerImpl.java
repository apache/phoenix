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
package org.apache.phoenix.query;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.ServerUtil;
import org.apache.phoenix.util.TimeKeeper;


/**
 * 
 * Implementation of StatsManager. Table stats are updated asynchronously when they're
 * accessed and past time-to-live. In this case, future calls (after the asynchronous
 * call has completed), will have the updated stats.
 * 
 * All tables share the same HBase connection for a given connection and each connection
 * will have it's own cache for these stats. This isn't ideal and will get reworked when
 * the schema is kept on the server side. It's ok for now because:
 * 1) we only ask the server for these stats when the start/end region is queried against
 * 2) the query to get the stats pulls a single row so it's very cheap
 * 3) it's async and if it takes too long it won't lead to anything except less optimal
 *  parallelization.
 *
 * 
 * @since 0.1
 */
public class StatsManagerImpl implements StatsManager {
    private final ConnectionQueryServices services;
    private final int statsUpdateFrequencyMs;
    private final int maxStatsAgeMs;
    private final TimeKeeper timeKeeper;
    private final ConcurrentMap<String,PTableStats> tableStatsMap = new ConcurrentHashMap<String,PTableStats>();

    public StatsManagerImpl(ConnectionQueryServices services, int statsUpdateFrequencyMs, int maxStatsAgeMs) {
        this(services, statsUpdateFrequencyMs, maxStatsAgeMs, TimeKeeper.SYSTEM);
    }
    
    public StatsManagerImpl(ConnectionQueryServices services, int statsUpdateFrequencyMs, int maxStatsAgeMs, TimeKeeper timeKeeper) {
        this.services = services;
        this.statsUpdateFrequencyMs = statsUpdateFrequencyMs;
        this.maxStatsAgeMs = maxStatsAgeMs;
        this.timeKeeper = timeKeeper;
    }
    
    public long getStatsUpdateFrequency() {
        return statsUpdateFrequencyMs;
    }
    
    @Override
    public void updateStats(TableRef tableRef) throws SQLException {
        SQLException sqlE = null;
        HTableInterface hTable = services.getTable(tableRef.getTable().getPhysicalName().getBytes());
        try {
            byte[] minKey = null, maxKey = null;
            // Do a key-only scan to get the first row of a table. This is the min
            // key for the table.
            Scan scan = new Scan(HConstants.EMPTY_START_ROW, new KeyOnlyFilter());
            ResultScanner scanner = hTable.getScanner(scan);
            try {
                Result r = scanner.next(); 
                if (r != null) {
                    minKey = r.getRow();
                }
            } finally {
                scanner.close();
            }
            int maxPossibleKeyLength = SchemaUtil.estimateKeyLength(tableRef.getTable());
            byte[] maxPossibleKey = new byte[maxPossibleKeyLength];
            Arrays.fill(maxPossibleKey, (byte)255);
            // Use this deprecated method to get the key "before" the max possible key value,
            // which is the max key for a table.
            @SuppressWarnings("deprecation")
            Result r = hTable.getRowOrBefore(maxPossibleKey, tableRef.getTable().getColumnFamilies().iterator().next().getName().getBytes());
            if (r != null) {
                maxKey = r.getRow();
            }
            tableStatsMap.put(tableRef.getTable().getName().getString(), new PTableStats(timeKeeper.getCurrentTime(),minKey,maxKey));
        } catch (IOException e) {
            sqlE = ServerUtil.parseServerException(e);
        } finally {
            try {
                hTable.close();
            } catch (IOException e) {
                if (sqlE == null) {
                    sqlE = ServerUtil.parseServerException(e);
                } else {
                    sqlE.setNextException(ServerUtil.parseServerException(e));
                }
            } finally {
                if (sqlE != null) {
                    throw sqlE;
                }
            }
        }
    }
    
    private PTableStats getStats(final TableRef table) {
        PTableStats stats = tableStatsMap.get(table);
        if (stats == null) {
            PTableStats newStats = new PTableStats();
            stats = tableStatsMap.putIfAbsent(table.getTable().getName().getString(), newStats);
            stats = stats == null ? newStats : stats;
        }
        // Synchronize on the current stats for a table to prevent
        // multiple attempts to update the stats.
        synchronized (stats) {
            long initiatedTime = stats.getInitiatedTime();
            long currentTime = timeKeeper.getCurrentTime();
            // Update stats asynchronously if they haven't been updated within the specified frequency.
            // We update asynchronously because we don't ever want to block the caller - instead we'll continue
            // to use the old one.
            if ( currentTime - initiatedTime >= getStatsUpdateFrequency()) {
                stats.setInitiatedTime(currentTime);
                services.getExecutor().submit(new Callable<Void>() {

                    @Override
                    public Void call() throws Exception { // TODO: will exceptions be logged?
                        updateStats(table);
                        return null;
                    }
                    
                });
            }
            // If the stats are older than the max age, use an empty stats
            if (currentTime - stats.getCompletedTime() >= maxStatsAgeMs) {
                return PTableStats.NO_STATS;
            }
        }
        return stats;
    }
    
    @Override
    public byte[] getMinKey(TableRef table) {
        PTableStats stats = getStats(table);
        return stats.getMinKey();
    }

    @Override
    public byte[] getMaxKey(TableRef table) {
        PTableStats stats = getStats(table);
        return stats.getMaxKey();
    }

    private static class PTableStats {
        private static final PTableStats NO_STATS = new PTableStats();
        private long initiatedTime;
        private final long completedTime;
        private final byte[] minKey;
        private final byte[] maxKey;
        
        public PTableStats() {
            this(-1,null,null);
        }
        public PTableStats(long completedTime, byte[] minKey, byte[] maxKey) {
            this.minKey = minKey;
            this.maxKey = maxKey;
            this.completedTime = this.initiatedTime = completedTime;
        }

        private byte[] getMinKey() {
            return minKey;
        }

        private byte[] getMaxKey() {
            return maxKey;
        }

        private long getCompletedTime() {
            return completedTime;
        }

        private void setInitiatedTime(long initiatedTime) {
            this.initiatedTime = initiatedTime;
        }

        private long getInitiatedTime() {
            return initiatedTime;
        }
    }
    
    @Override
    public void clearStats() throws SQLException {
        tableStatsMap.clear();
    }
}
