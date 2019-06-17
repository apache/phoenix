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

import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.SimpleRegionObserver;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.util.*;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.sql.*;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

@RunWith(RunUntilFailure.class) @Category(NeedsOwnMiniClusterTest.class)
public class ConcurrentMutationsExtendedIT extends ParallelStatsDisabledIT {

    private static final Random RAND = new Random(5);
    private static final String MVCC_LOCK_TEST_TABLE_PREFIX = "MVCCLOCKTEST_";
    private static final String LOCK_TEST_TABLE_PREFIX = "LOCKTEST_";
    private static final int ROW_LOCK_WAIT_TIME = 10000;

    private final Object lock = new Object();

    @Test
    public void testSynchronousDeletesAndUpsertValues() throws Exception {
        final String tableName = generateUniqueName();
        final String indexName = generateUniqueName();
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("CREATE TABLE " + tableName
                + "(k1 INTEGER NOT NULL, k2 INTEGER NOT NULL, v1 INTEGER, CONSTRAINT pk PRIMARY KEY (k1,k2)) COLUMN_ENCODED_BYTES = 0");
        TestUtil.addCoprocessor(conn, tableName, DelayingRegionObserver.class);
        conn.createStatement().execute("CREATE INDEX " + indexName + " ON " + tableName + "(v1)");
        final CountDownLatch doneSignal = new CountDownLatch(2);
        Runnable r1 = new Runnable() {

            @Override public void run() {
                try {
                    Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
                    for (int i = 0; i < 50; i++) {
                        Thread.sleep(20);
                        synchronized (lock) {
                            PhoenixConnection conn = null;
                            try {
                                conn =
                                        DriverManager.getConnection(getUrl(), props)
                                                .unwrap(PhoenixConnection.class);
                                conn.setAutoCommit(true);
                                conn.createStatement().execute("DELETE FROM " + tableName);
                            } finally {
                                if (conn != null) conn.close();
                            }
                        }
                    }
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                } catch (InterruptedException e) {
                    Thread.interrupted();
                    throw new RuntimeException(e);
                } finally {
                    doneSignal.countDown();
                }
            }

        };
        Runnable r2 = new Runnable() {

            @Override public void run() {
                try {
                    Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
                    int nRowsToUpsert = 1000;
                    for (int i = 0; i < nRowsToUpsert; i++) {
                        synchronized (lock) {
                            PhoenixConnection conn = null;
                            try {
                                conn =
                                        DriverManager.getConnection(getUrl(), props)
                                                .unwrap(PhoenixConnection.class);
                                conn.createStatement().execute(
                                        "UPSERT INTO " + tableName + " VALUES (" + (i % 10)
                                                + ", 0, 1)");
                                if ((i % 20) == 0 || i == nRowsToUpsert - 1) {
                                    conn.commit();
                                }
                            } finally {
                                if (conn != null) conn.close();
                            }
                        }
                    }
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                } finally {
                    doneSignal.countDown();
                }
            }

        };
        Thread t1 = new Thread(r1);
        t1.start();
        Thread t2 = new Thread(r2);
        t2.start();

        doneSignal.await(60, TimeUnit.SECONDS);
        IndexScrutiny.scrutinizeIndex(conn, tableName, indexName);
    }

    @Test
    public void testConcurrentDeletesAndUpsertValues() throws Exception {
        final String tableName = generateUniqueName();
        final String indexName = generateUniqueName();
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("CREATE TABLE " + tableName
                + "(k1 INTEGER NOT NULL, k2 INTEGER NOT NULL, v1 INTEGER, CONSTRAINT pk PRIMARY KEY (k1,k2))");
        TestUtil.addCoprocessor(conn, tableName, DelayingRegionObserver.class);
        conn.createStatement().execute("CREATE INDEX " + indexName + " ON " + tableName + "(v1)");
        final CountDownLatch doneSignal = new CountDownLatch(2);
        Runnable r1 = new Runnable() {

            @Override public void run() {
                try {
                    Connection conn = DriverManager.getConnection(getUrl());
                    conn.setAutoCommit(true);
                    for (int i = 0; i < 50; i++) {
                        Thread.sleep(20);
                        conn.createStatement().execute("DELETE FROM " + tableName);
                    }
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                } catch (InterruptedException e) {
                    Thread.interrupted();
                    throw new RuntimeException(e);
                } finally {
                    doneSignal.countDown();
                }
            }

        };
        Runnable r2 = new Runnable() {

            @Override public void run() {
                try {
                    Connection conn = DriverManager.getConnection(getUrl());
                    for (int i = 0; i < 1000; i++) {
                        conn.createStatement().execute(
                                "UPSERT INTO " + tableName + " VALUES (" + (i % 10) + ", 0, 1)");
                        if ((i % 20) == 0) {
                            conn.commit();
                        }
                    }
                    conn.commit();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                } finally {
                    doneSignal.countDown();
                }
            }

        };
        Thread t1 = new Thread(r1);
        t1.start();
        Thread t2 = new Thread(r2);
        t2.start();

        doneSignal.await(60, TimeUnit.SECONDS);
        IndexScrutiny.scrutinizeIndex(conn, tableName, indexName);
    }

    @Test @Repeat(5)
    public void testConcurrentUpserts() throws Exception {
        int nThreads = 4;
        final int batchSize = 200;
        final int nRows = 51;
        final int nIndexValues = 23;
        final String tableName = generateUniqueName();
        final String indexName = generateUniqueName();
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("CREATE TABLE " + tableName
                + "(k1 INTEGER NOT NULL, k2 INTEGER NOT NULL, v1 INTEGER, CONSTRAINT pk PRIMARY KEY (k1,k2))  COLUMN_ENCODED_BYTES = 0, VERSIONS=1");
        TestUtil.addCoprocessor(conn, tableName, DelayingRegionObserver.class);
        conn.createStatement().execute("CREATE INDEX " + indexName + " ON " + tableName + "(v1)");
        final CountDownLatch doneSignal = new CountDownLatch(nThreads);
        Runnable[] runnables = new Runnable[nThreads];
        for (int i = 0; i < nThreads; i++) {
            runnables[i] = new Runnable() {

                @Override public void run() {
                    try {
                        Connection conn = DriverManager.getConnection(getUrl());
                        for (int i = 0; i < 10000; i++) {
                            boolean isNull = RAND.nextBoolean();
                            int randInt = RAND.nextInt() % nIndexValues;
                            conn.createStatement().execute(
                                    "UPSERT INTO " + tableName + " VALUES (" + (i % nRows) + ", 0, "
                                            + (isNull ? null : randInt) + ")");
                            if ((i % batchSize) == 0) {
                                conn.commit();
                            }
                        }
                        conn.commit();
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    } finally {
                        doneSignal.countDown();
                    }
                }

            };
        }
        for (int i = 0; i < nThreads; i++) {
            Thread t = new Thread(runnables[i]);
            t.start();
        }

        assertTrue("Ran out of time", doneSignal.await(120, TimeUnit.SECONDS));
        long actualRowCount = IndexScrutiny.scrutinizeIndex(conn, tableName, indexName);
        assertEquals(nRows, actualRowCount);
    }

    @Test
    public void testRowLockDuringPreBatchMutateWhenIndexed() throws Exception {
        final String tableName = LOCK_TEST_TABLE_PREFIX + generateUniqueName();
        final String indexName = generateUniqueName();
        Connection conn = DriverManager.getConnection(getUrl());

        conn.createStatement().execute("CREATE TABLE " + tableName
                + "(k VARCHAR PRIMARY KEY, v INTEGER) COLUMN_ENCODED_BYTES = 0");
        TestUtil.addCoprocessor(conn, tableName, DelayingRegionObserver.class);
        conn.createStatement().execute("CREATE INDEX " + indexName + " ON " + tableName + "(v)");
        final CountDownLatch doneSignal = new CountDownLatch(2);
        final String[] failedMsg = new String[1];
        Runnable r1 = new Runnable() {

            @Override public void run() {
                try {
                    Connection conn = DriverManager.getConnection(getUrl());
                    conn.createStatement()
                            .execute("UPSERT INTO " + tableName + " VALUES ('foo',0)");
                    conn.createStatement()
                            .execute("UPSERT INTO " + tableName + " VALUES ('foo',1)");
                    conn.commit();
                } catch (Exception e) {
                    failedMsg[0] = e.getMessage();
                    throw new RuntimeException(e);
                } finally {
                    doneSignal.countDown();
                }
            }

        };
        Runnable r2 = new Runnable() {

            @Override public void run() {
                try {
                    Connection conn = DriverManager.getConnection(getUrl());
                    conn.createStatement()
                            .execute("UPSERT INTO " + tableName + " VALUES ('foo',2)");
                    conn.createStatement()
                            .execute("UPSERT INTO " + tableName + " VALUES ('foo',3)");
                    conn.commit();
                } catch (Exception e) {
                    failedMsg[0] = e.getMessage();
                    throw new RuntimeException(e);
                } finally {
                    doneSignal.countDown();
                }
            }

        };
        Thread t1 = new Thread(r1);
        t1.start();
        Thread t2 = new Thread(r2);
        t2.start();

        doneSignal.await(ROW_LOCK_WAIT_TIME + 5000, TimeUnit.SECONDS);
        assertNull(failedMsg[0], failedMsg[0]);
        long actualRowCount = IndexScrutiny.scrutinizeIndex(conn, tableName, indexName);
        assertEquals(1, actualRowCount);
    }

    @Test
    public void testLockUntilMVCCAdvanced() throws Exception {
        final String tableName = MVCC_LOCK_TEST_TABLE_PREFIX + generateUniqueName();
        final String indexName = generateUniqueName();
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("CREATE TABLE " + tableName
                + "(k VARCHAR PRIMARY KEY, v INTEGER) COLUMN_ENCODED_BYTES = 0");
        conn.createStatement().execute("CREATE INDEX " + indexName + " ON " + tableName + "(v,k)");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES ('foo',0)");
        conn.commit();
        TestUtil.addCoprocessor(conn, tableName, DelayingRegionObserver.class);
        final CountDownLatch doneSignal = new CountDownLatch(2);
        final String[] failedMsg = new String[1];
        Runnable r1 = new Runnable() {

            @Override public void run() {
                try {
                    Connection conn = DriverManager.getConnection(getUrl());
                    conn.createStatement()
                            .execute("UPSERT INTO " + tableName + " VALUES ('foo',1)");
                    conn.commit();
                } catch (Exception e) {
                    failedMsg[0] = e.getMessage();
                    throw new RuntimeException(e);
                } finally {
                    doneSignal.countDown();
                }
            }

        };
        Runnable r2 = new Runnable() {

            @Override public void run() {
                try {
                    Connection conn = DriverManager.getConnection(getUrl());
                    conn.createStatement()
                            .execute("UPSERT INTO " + tableName + " VALUES ('foo',2)");
                    conn.commit();
                } catch (Exception e) {
                    failedMsg[0] = e.getMessage();
                    throw new RuntimeException(e);
                } finally {
                    doneSignal.countDown();
                }
            }

        };
        Thread t1 = new Thread(r1);
        t1.start();
        Thread t2 = new Thread(r2);
        t2.start();

        doneSignal.await(ROW_LOCK_WAIT_TIME + 5000, TimeUnit.SECONDS);
        long actualRowCount = IndexScrutiny.scrutinizeIndex(conn, tableName, indexName);
        assertEquals(1, actualRowCount);
    }

    public static class DelayingRegionObserver extends SimpleRegionObserver {
        private volatile boolean lockedTableRow;

        @Override public void postBatchMutate(ObserverContext<RegionCoprocessorEnvironment> c,
                MiniBatchOperationInProgress<Mutation> miniBatchOp) throws IOException {
            try {
                String tableName = c.getEnvironment().getRegionInfo().getTable().getNameAsString();
                if (tableName.startsWith(MVCC_LOCK_TEST_TABLE_PREFIX)) {
                    Thread.sleep(ROW_LOCK_WAIT_TIME
                            / 2); // Wait long enough that they'll both have the same mvcc
                }
            } catch (InterruptedException e) {
            }
        }

        @Override public void preBatchMutate(ObserverContext<RegionCoprocessorEnvironment> c,
                MiniBatchOperationInProgress<Mutation> miniBatchOp) throws HBaseIOException {
            try {
                String tableName = c.getEnvironment().getRegionInfo().getTable().getNameAsString();
                if (tableName.startsWith(LOCK_TEST_TABLE_PREFIX)) {
                    if (lockedTableRow) {
                        throw new DoNotRetryIOException(
                                "Expected lock in preBatchMutate to be exclusive, but it wasn't for row "
                                        + Bytes
                                        .toStringBinary(miniBatchOp.getOperation(0).getRow()));
                    }
                    lockedTableRow = true;
                    Thread.sleep(ROW_LOCK_WAIT_TIME + 2000);
                }
                Thread.sleep(Math.abs(RAND.nextInt()) % 10);
            } catch (InterruptedException e) {
            } finally {
                lockedTableRow = false;
            }

        }
    }

}
