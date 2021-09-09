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

import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.phoenix.compat.hbase.coprocessor.CompatBaseScannerRegionObserver;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.thirdparty.com.google.common.collect.ImmutableMap;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.RunUntilFailure;
import org.apache.phoenix.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.apache.phoenix.end2end.ConcurrentMutationsExtendedIT
    .verifyIndexTable;
import static org.junit.Assert.assertTrue;


@Category(NeedsOwnMiniClusterTest.class)
@RunWith(RunUntilFailure.class)
public class ConcurrentUpsertsWithoutIndexedColsIT
        extends BaseTest {

    private static final Random RANDOM = new Random(5);
    private static final Logger LOGGER =
        LoggerFactory.getLogger(ConcurrentUpsertsWithoutIndexedColsIT.class);

    private static final Map<String, String> PROPS = ImmutableMap.of(
        QueryServices.GLOBAL_INDEX_ROW_AGE_THRESHOLD_TO_DELETE_MS_ATTRIB,
        Long.toString(0),
        CompatBaseScannerRegionObserver.PHOENIX_MAX_LOOKBACK_AGE_CONF_KEY,
        Integer.toString(1000000));

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        setUpTestDriver(new ReadOnlyProps(PROPS.entrySet().iterator()));
    }

    @Test
    public void testConcurrentUpsertsWithoutIndexedColumns() throws Exception {
        int nThreads = 4;
        final int batchSize = 100;
        final int nRows = 997;
        final String tableName = generateUniqueName();
        final String indexName = generateUniqueName();
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("CREATE TABLE " + tableName
            + "(k1 INTEGER NOT NULL, k2 INTEGER NOT NULL, a.v1 INTEGER, "
            + "b.v2 INTEGER, c.v3 INTEGER, d.v4 INTEGER,"
            + "CONSTRAINT pk PRIMARY KEY (k1,k2))  COLUMN_ENCODED_BYTES = 0, VERSIONS=1");
        TestUtil.addCoprocessor(conn, tableName,
            ConcurrentMutationsExtendedIT.DelayingRegionObserver.class);
        conn.createStatement().execute("CREATE INDEX " + indexName + " ON "
            + tableName + "(v1) INCLUDE(v2, v3)");
        final CountDownLatch doneSignal = new CountDownLatch(nThreads);
        ExecutorService executorService = Executors.newFixedThreadPool(nThreads,
            new ThreadFactoryBuilder().setDaemon(true)
                .setNameFormat("test-concurrent-upsert-%d").build());
        for (int i = 0; i < nThreads; i++) {
            TestRunnable testRunnable = new TestRunnable(tableName, nRows,
                batchSize, doneSignal);
            executorService.submit(testRunnable);
        }

        assertTrue("Ran out of time", doneSignal.await(1300, TimeUnit.SECONDS));
        executorService.shutdown();
        executorService.awaitTermination(5, TimeUnit.SECONDS);
        verifyIndexTable(tableName, indexName, conn);
    }

    private static class TestRunnable implements Runnable {
        private final String tableName;
        private final int nRows;
        private final int batchSize;
        private final CountDownLatch doneSignal;

        public TestRunnable(String tableName, int nRows, int batchSize,
                CountDownLatch doneSignal) {
            this.tableName = tableName;
            this.nRows = nRows;
            this.batchSize = batchSize;
            this.doneSignal = doneSignal;
        }

        @Override
        public void run() {
            try {
                Connection conn = DriverManager.getConnection(getUrl());
                for (int i = 0; i < 1000; i++) {
                    if (RANDOM.nextInt() % 1000 < 10) {
                        // Do not include the indexed column in upserts
                        conn.createStatement().execute(
                            "UPSERT INTO " + tableName + " (k1, k2, b.v2, c.v3, d.v4) VALUES ("
                                + (RANDOM.nextInt() % nRows) + ", 0, "
                                + (RANDOM.nextBoolean() ? null : RANDOM.nextInt()) + ", "
                                + (RANDOM.nextBoolean() ? null : RANDOM.nextInt()) + ", "
                                + (RANDOM.nextBoolean() ? null : RANDOM.nextInt()) + ")");
                    } else {
                        conn.createStatement().execute(
                            "UPSERT INTO " + tableName + " VALUES (" + (i % nRows) + ", 0, "
                                + (RANDOM.nextBoolean() ? null : RANDOM.nextInt()) + ", "
                                + (RANDOM.nextBoolean() ? null : RANDOM.nextInt()) + ", "
                                + (RANDOM.nextBoolean() ? null : RANDOM.nextInt()) + ", "
                                + (RANDOM.nextBoolean() ? null : RANDOM.nextInt()) + ")");
                    }
                    if ((i % batchSize) == 0) {
                        conn.commit();
                        LOGGER.info("Committed batch no: {}", i);
                    }
                }
                conn.commit();
            } catch (SQLException e) {
                LOGGER.error("Error during concurrent upserts. ", e);
                throw new RuntimeException(e);
            } finally {
                doneSignal.countDown();
            }
        }
    }
}
