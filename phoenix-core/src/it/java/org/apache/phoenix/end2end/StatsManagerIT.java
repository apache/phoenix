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
package org.apache.phoenix.end2end;

import static org.apache.phoenix.util.TestUtil.STABLE_NAME;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Properties;

import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.StatsManager;
import org.apache.phoenix.query.StatsManagerImpl;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.TimeKeeper;
import org.junit.Test;


/**
 * 
 * Test for stats manager, which is a client-side process that caches the
 * first and last key of a given table. The {@link #testStatsManager()}
 * test must be the only test here, as it relies on state that is only
 * cleared between test runs.
 *
 */
public class StatsManagerIT extends BaseParallelIteratorsRegionSplitterIT {
    
    private static class ManualTimeKeeper implements TimeKeeper {
        private long currentTime = 0;
        @Override
        public long getCurrentTime() {
            return currentTime;
        }
        
        public void setCurrentTime(long currentTime) {
            this.currentTime = currentTime;
        }
    }

    private static interface ChangeDetector {
        boolean isChanged();
    }

    private boolean waitForAsyncChange(ChangeDetector detector, long maxWaitTimeMs) throws Exception {
        long startTime = System.currentTimeMillis();
        do {
            if (detector.isChanged()) {
                return true;
            }
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw e;
            }
        } while (System.currentTimeMillis() - startTime < maxWaitTimeMs);
        return false;
    }

    private static class MinKeyChange implements ChangeDetector {
        private byte[] value;
        private StatsManager stats;
        private TableRef table;
        
        public MinKeyChange(StatsManager stats, TableRef table) {
            this.value = stats.getMinKey(table);
            this.stats = stats;
            this.table = table;
        }
        @Override
        public boolean isChanged() {
            return value != stats.getMinKey(table);
        }
    }

    private static class MaxKeyChange implements ChangeDetector {
        private byte[] value;
        private StatsManager stats;
        private TableRef table;
        
        public MaxKeyChange(StatsManager stats, TableRef table) {
            this.value = stats.getMaxKey(table);
            this.stats = stats;
            this.table = table;
        }
        @Override
        public boolean isChanged() {
            return value != stats.getMaxKey(table);
        }
    }

    @Test
    public void testStatsManager() throws Exception {
        long ts = nextTimestamp();
        initTableValues(ts);
        String url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + ts;
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        TableRef table = getTableRef(conn,ts);

        int updateFreq = 5;
        int maxAge = 10;
        int startTime = 100;
        long waitTime = 5000;
        
        ManualTimeKeeper timeKeeper = new ManualTimeKeeper();
        timeKeeper.setCurrentTime(startTime);
        ConnectionQueryServices services = driver.getConnectionQueryServices(getUrl(), TEST_PROPERTIES);
        StatsManager stats = new StatsManagerImpl(services, updateFreq, maxAge, timeKeeper);
        MinKeyChange minKeyChange = new MinKeyChange(stats, table);
        MaxKeyChange maxKeyChange = new MaxKeyChange(stats, table);
        
        byte[] minKey = minKeyChange.value;
        assertTrue(minKey == null);
        assertTrue(waitForAsyncChange(minKeyChange,waitTime));
        assertArrayEquals(KMIN, stats.getMinKey(table));
        assertArrayEquals(KMAX, stats.getMaxKey(table));
        minKeyChange = new MinKeyChange(stats, table);
        
        url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + ts+2;
        props = new Properties(TEST_PROPERTIES);
        conn = DriverManager.getConnection(url, props);
        PreparedStatement delStmt = conn.prepareStatement("delete from " + STABLE_NAME + " where id=?");
        delStmt.setString(1, new String(KMIN));
        delStmt.execute();
        PreparedStatement upsertStmt = conn.prepareStatement("upsert into " + STABLE_NAME + " VALUES (?, ?)");
        upsertStmt.setString(1, new String(KMIN2));
        upsertStmt.setInt(2, 1);
        upsertStmt.execute();
        conn.commit();

        assertFalse(waitForAsyncChange(minKeyChange,waitTime)); // Stats won't change until they're attempted to be retrieved again
        timeKeeper.setCurrentTime(timeKeeper.getCurrentTime() + updateFreq);
        minKeyChange = new MinKeyChange(stats, table); // Will kick off change, but will upate asynchronously
        assertArrayEquals(KMIN, minKeyChange.value);
        assertTrue(waitForAsyncChange(minKeyChange,waitTime));
        assertArrayEquals(KMIN2, stats.getMinKey(table));
        assertArrayEquals(KMAX, stats.getMaxKey(table));
        minKeyChange = new MinKeyChange(stats, table);
        
        timeKeeper.setCurrentTime(timeKeeper.getCurrentTime() + maxAge);
        minKeyChange = new MinKeyChange(stats, table); // Will kick off change, but will upate asynchronously
        assertTrue(null == minKeyChange.value);
        assertTrue(waitForAsyncChange(minKeyChange,waitTime));
        assertArrayEquals(KMIN2, stats.getMinKey(table));
        assertArrayEquals(KMAX, stats.getMaxKey(table));
        maxKeyChange = new MaxKeyChange(stats, table);
        
        delStmt.setString(1, new String(KMAX));
        delStmt.execute();
        upsertStmt.setString(1, new String(KMAX2));
        upsertStmt.setInt(2, 1);
        upsertStmt.execute();
        conn.commit();
        conn.close();

        assertFalse(waitForAsyncChange(maxKeyChange,waitTime)); // Stats won't change until they're attempted to be retrieved again
        timeKeeper.setCurrentTime(timeKeeper.getCurrentTime() + updateFreq);
        maxKeyChange = new MaxKeyChange(stats, table); // Will kick off change, but will upate asynchronously
        assertArrayEquals(KMAX, maxKeyChange.value);
        assertTrue(waitForAsyncChange(maxKeyChange,waitTime));
        assertArrayEquals(KMAX2, stats.getMaxKey(table));
        assertArrayEquals(KMIN2, stats.getMinKey(table));
        maxKeyChange = new MaxKeyChange(stats, table);
        
        timeKeeper.setCurrentTime(timeKeeper.getCurrentTime() + maxAge);
        maxKeyChange = new MaxKeyChange(stats, table); // Will kick off change, but will upate asynchronously
        assertTrue(null == maxKeyChange.value);
        assertTrue(waitForAsyncChange(maxKeyChange,waitTime));
        assertArrayEquals(KMIN2, stats.getMinKey(table));
        assertArrayEquals(KMAX2, stats.getMaxKey(table));
    }
}
