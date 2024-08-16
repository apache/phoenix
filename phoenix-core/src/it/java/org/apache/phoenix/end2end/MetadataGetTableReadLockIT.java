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

import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessor.MetaDataEndpointImpl;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.TestUtil;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

/**
 * Class which tests whether non-exclusive locking on metadata read path (getTable) works as expected.
 */
@Category(NeedsOwnMiniClusterTest.class)
public class MetadataGetTableReadLockIT extends BaseTest {

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        Map<String, String> props = Maps.newHashMapWithExpectedSize(1);
        // Disable system task handling
        props.put(QueryServices.TASK_HANDLING_INITIAL_DELAY_MS_ATTRIB, Long.toString(Long.MAX_VALUE));
        setUpTestDriver(new ReadOnlyProps(props));
    }

    /**
     * Create 2 threads which query a table.
     * Thread-1 sleeps in the getTable path after acquiring a non-exclusive read lock.
     * Thread-2 should not wait to acquire a lock for its query.
     */
    @Test
    public void testBlockedReadDoesNotBlockAnotherRead() throws Exception {
        long SLEEP_DURATION = 5000L;
        String tableName = generateUniqueName();
        CountDownLatch sleepSignal = new CountDownLatch(1);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            // create table
            conn.createStatement().execute("CREATE TABLE " + tableName
                    + "(k INTEGER NOT NULL PRIMARY KEY, v1 INTEGER, v2 INTEGER)");

            // load custom coproc which supports blocking
            TestUtil.removeCoprocessor(conn, "SYSTEM.CATALOG", MetaDataEndpointImpl.class);
            BlockingMetaDataEndpointImpl.setSleepSignal(sleepSignal);
            BlockingMetaDataEndpointImpl.setSleepDuration(SLEEP_DURATION);
            TestUtil.addCoprocessor(conn, "SYSTEM.CATALOG", BlockingMetaDataEndpointImpl.class);

            // start thread-1 and wait for signal before it starts sleeping
            Thread t1 = getQueryThread(tableName);
            Thread t2 = getQueryThread(tableName);
            t1.start();
            sleepSignal.await();

            // we don't want thread-2 to sleep at all
            BlockingMetaDataEndpointImpl.setSleepDuration(0);
            long start = System.currentTimeMillis();
            t2.start();
            t2.join();
            long end = System.currentTimeMillis();
            t1.join();

            // if thread-2 did not wait to acquire lock, it will finish faster than thread-1's SLEEP_DURATION
            Assert.assertTrue("Second thread should not have been blocked by the first thread.",
                    end-start < SLEEP_DURATION);
        }
    }

    private static Thread getQueryThread(String tableName) {
        Runnable runnable = () -> {
            try (Connection conn1 = DriverManager.getConnection(getUrl())) {
                conn1.createStatement().execute("SELECT * FROM " + tableName + " LIMIT 1");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
        Thread t = new Thread(runnable);
        return t;
    }

    /**
     * Extend MetaDataEndpointImpl with support for blocking after acquiring lock.
     */
    public static class BlockingMetaDataEndpointImpl extends MetaDataEndpointImpl {

        private static volatile long sleepDuration;
        private static CountDownLatch sleepSignal;

        public static void setSleepDuration(long sleepDuration) {
            BlockingMetaDataEndpointImpl.sleepDuration = sleepDuration;
        }

        public static void setSleepSignal(CountDownLatch sleepSignal) {
            BlockingMetaDataEndpointImpl.sleepSignal = sleepSignal;
        }

        @Override
        protected Region.RowLock acquireLock(Region region, byte[] lockKey, List<Region.RowLock> locks, boolean readLock) throws IOException {
            long tmpSleepDuration = sleepDuration;
            Region.RowLock rowLock = region.getRowLock(lockKey, this.getMetadataReadLockEnabled && readLock);
            if (rowLock == null) {
                throw new IOException("Failed to acquire lock on " + Bytes.toStringBinary(lockKey));
            }
            if (locks != null) {
                locks.add(rowLock);
            }
            sleepSignal.countDown();
            try {
                Thread.sleep(tmpSleepDuration);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return rowLock;
        }
    }

    @AfterClass
    public static synchronized void cleanup() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            TestUtil.removeCoprocessor(conn, "SYSTEM.CATALOG", BlockingMetaDataEndpointImpl.class);
            TestUtil.addCoprocessor(conn, "SYSTEM.CATALOG", MetaDataEndpointImpl.class);
        }
    }
}
