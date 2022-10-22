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

import org.apache.phoenix.coprocessor.BaseScannerRegionObserver;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.locks.Lock;

import static org.junit.Assert.*;
@Category(ParallelStatsDisabledTest.class)
public class PreMatureTimelyAbortScanIt extends ParallelStatsDisabledIT {
    private static final Logger LOG = LoggerFactory.getLogger(PreMatureTimelyAbortScanIt.class);

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        Map<String, String> props = Maps.newHashMapWithExpectedSize(1);
        props.put(BaseScannerRegionObserver.PHOENIX_MAX_LOOKBACK_AGE_CONF_KEY, Integer.toString(60*60)); // An hour
        props.put(QueryServices.USE_STATS_FOR_PARALLELIZATION, Boolean.toString(false));
        props.put(QueryServices.PHOENIX_SERVER_PAGE_SIZE_MS, Integer.toString(0));
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }

    private String getUniqueUrl() {
        return url + generateUniqueName();
    }

    @Test
    public void testPreMatureScannerAbortForCount() throws Exception {

        try (Connection conn = DriverManager.getConnection(getUniqueUrl())) {
            conn.createStatement().execute("CREATE TABLE LONG_BUG (ID INTEGER PRIMARY KEY, AMOUNT DECIMAL) SALT_BUCKETS = 16");
        }
        try (Connection conn = DriverManager.getConnection(getUniqueUrl())) {
            for (int i = 0; i<500000 ; i++) {
                int amount = -50000 + i;
                String s = "UPSERT INTO LONG_BUG (ID, AMOUNT) VALUES( " + i + ", " + amount + ")";
                conn.createStatement().execute(s);
                conn.commit();
            }
        }

        try {
            Connection conn = DriverManager.getConnection(getUniqueUrl());
            Runnable runnable = new Runnable() {
                @Override
                public void run() {
                    try {
                        synchronized (conn) {
                            conn.wait();
                        }
                        ResultSet resultSet = conn.createStatement().executeQuery(
                                    "SELECT COUNT(*) FROM LONG_BUG WHERE ID % 2 = 0");
                        boolean result = resultSet.next();
                        assertTrue(result);
                        LOG.info("Count of modulus 2 for LONG_BUG :- " + resultSet.getInt(1));
                        fail();
                    } catch (SQLException sqe) {
                        //RESULTSET_CLOSED exception expected
                        if (sqe.getErrorCode() != SQLExceptionCode.RESULTSET_CLOSED.getErrorCode()) {
                            LOG.error("Error ", sqe);
                            fail();
                        }
                    } catch (Exception e) {
                        LOG.error("Error", e);
                        fail();
                    }
                }
            };

            Runnable runnable1 = new Runnable() {
                @Override
                public void run() {
                    try {
                        synchronized (conn) {
                            conn.notify();
                        }
                        Thread.sleep(1000);
                        conn.close();
                    } catch (InterruptedException | SQLException e) {
                        fail();
                    }
                }
            };
            Thread t = new Thread(runnable);
            Thread t1 = new Thread(runnable1);
            t.start();
            Thread.sleep(1500);
            t1.start();
            t.join();
            t1.join();
        } finally {

        }
    }
}