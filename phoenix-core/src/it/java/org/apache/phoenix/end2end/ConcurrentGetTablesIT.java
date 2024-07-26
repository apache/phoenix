/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.TestUtil;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Tests to ensure concurrent metadata RPC calls can be served with limited metadata handlers.
 */
@Category(NeedsOwnMiniClusterTest.class)
public class ConcurrentGetTablesIT extends BaseTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConcurrentGetTablesIT.class);

    private static void initCluster(int numMetaHandlers)
            throws Exception {
        Map<String, String> props = Maps.newConcurrentMap();
        props.put(BaseScannerRegionObserverConstants.PHOENIX_MAX_LOOKBACK_AGE_CONF_KEY,
                Integer.toString(60 * 60 * 1000));
        props.put(QueryServices.METADATA_HANDLER_COUNT_ATTRIB, Integer.toString(numMetaHandlers));
        // Make sure that not only tables are created with UPDATE_CACHE_FREQUENCY=ALWAYS and
        // hence queries need to go to regionserver, but also we disable enough of
        // metadata caching at server side, invalidation as well as last DDL timestamp
        // validation at client side.
        // Combine this setup with UPDATE_CACHE_FREQUENCY=ALWAYS and enough RPC calls to
        // MetaDataEndpointImpl#clearCache to ensure frequently queries need to execute
        // getTable() at server side by scanning SYSTEM.CATALOG.
        props.put(QueryServices.DEFAULT_UPDATE_CACHE_FREQUENCY_ATRRIB, "ALWAYS");
//        props.put(QueryServices.LAST_DDL_TIMESTAMP_VALIDATION_ENABLED, Boolean.toString(false));
//        props.put(QueryServices.PHOENIX_METADATA_INVALIDATE_CACHE_ENABLED, Boolean.toString(false));
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        initCluster(2);
    }

    // This test is important to verify that concurrent getTable() calls as part of executing
    // the SELECT queries would be sufficient to be served by limited metadata handlers.
    // If this test times out, we have a problem. In order to debug and understand what is
    // wrong with the system, thread dump will be required when this test seems stuck.
    // 5 min is good enough as timeout value, usually test is expected to be completed within
    // 1 or 2 min.
    @Test(timeout = 5 * 60 * 1000)
    public void testConcurrentGetTablesWithQueries() throws Throwable {
        final String tableName = generateUniqueName();
        final String view01 = "v01_" + tableName;
        final String view02 = "v02_" + tableName;
        final String index_view01 = "idx_v01_" + tableName;
        final String index_view02 = "idx_v02_" + tableName;
        final String index_view03 = "idx_v03_" + tableName;
        final String index_view04 = "idx_v04_" + tableName;

        try (Connection conn = DriverManager.getConnection(getUrl())) {
            final Statement stmt = conn.createStatement();

            stmt.execute("CREATE TABLE " + tableName
                    + " (COL1 CHAR(10) NOT NULL, COL2 CHAR(5) NOT NULL, COL3 VARCHAR,"
                    + " COL4 VARCHAR CONSTRAINT pk PRIMARY KEY(COL1, COL2))"
                    + " UPDATE_CACHE_FREQUENCY=ALWAYS");
            stmt.execute("CREATE VIEW " + view01
                    + " (VCOL1 CHAR(8), COL5 VARCHAR) AS SELECT * FROM " + tableName
                    + " WHERE COL1 = 'col1'");
            stmt.execute("CREATE VIEW " + view02 + " (VCOL2 CHAR(10), COL6 VARCHAR)"
                    + " AS SELECT * FROM " + view01 + " WHERE VCOL1 = 'vcol1'");
            stmt.execute("CREATE INDEX " + index_view01 + " ON " + view01 + " (COL5) INCLUDE "
                    + "(COL1, COL2, COL3)");
            stmt.execute("CREATE INDEX " + index_view02 + " ON " + view02 + " (COL6) INCLUDE "
                    + "(COL1, COL2, COL3)");
            stmt.execute("CREATE INDEX " + index_view03 + " ON " + view01 + " (COL5) INCLUDE "
                    + "(COL2, COL1)");
            stmt.execute("CREATE INDEX " + index_view04 + " ON " + view02 + " (COL6) INCLUDE "
                    + "(COL2, COL1)");

            stmt.execute("UPSERT INTO " + view02
                    + " (col2, vcol2, col5, col6) values ('0001', 'vcol2_01', 'col5_01', " +
                    "'col6_01')");
            stmt.execute("UPSERT INTO " + view02
                    +
                    " (col2, vcol2, col5, col6) values ('0002', 'vcol2_02', 'col5_02', 'col6_02')");
            stmt.execute("UPSERT INTO " + view02
                    +
                    " (col2, vcol2, col5, col6) values ('0003', 'vcol2_03', 'col5_03', 'col6_03')");
            stmt.execute("UPSERT INTO " + view01 + " (col2, vcol1, col3, col4, col5) values "
                    + "('0004', 'vcol2', 'col3_04', 'col4_04', 'col5_04')");
            stmt.execute("UPSERT INTO " + view01 + " (col2, vcol1, col3, col4, col5) values "
                    + "('0005', 'vcol-2', 'col3_05', 'col4_05', 'col5_05')");
            stmt.execute("UPSERT INTO " + view01 + " (col2, vcol1, col3, col4, col5) values "
                    + "('0006', 'vcol-1', 'col3_06', 'col4_06', 'col5_06')");
            stmt.execute("UPSERT INTO " + view01 + " (col2, vcol1, col3, col4, col5) values "
                    + "('0007', 'vcol1', 'col3_07', 'col4_07', 'col5_07')");
            stmt.execute("UPSERT INTO " + view02
                    +
                    " (col2, vcol2, col5, col6) values ('0008', 'vcol2_08', 'col5_08', 'col6_02')");
            conn.commit();

            TestUtil.clearMetaDataCache(conn);

            List<Callable<Void>> callableList =
                    getCallables(conn, view02);

            ExecutorService executorService = Executors.newFixedThreadPool(10);
            List<Future<Void>> futures = executorService.invokeAll(callableList);

            for (Future<Void> future : futures) {
                future.get(1, TimeUnit.MINUTES);
            }
        }
    }

    private static List<Callable<Void>> getCallables(Connection conn, String view02) {
        List<Callable<Void>> callableList = new ArrayList<>();
        for (int k = 0; k < 25; k++) {
            callableList.add(() -> {
                for (int i = 0; i < 50; i++) {
                    if (i % 7 == 0) {
                        try {
                            TestUtil.clearMetaDataCache(conn);
                        } catch (Throwable e) {
                            LOGGER.error("Something went wrong....", e);
                            throw new RuntimeException(e);
                        }
                    }
                    final Statement statement = conn.createStatement();
                    ResultSet rs =
                            statement.executeQuery(
                                    "SELECT COL2, VCOL1, VCOL2, COL5, COL6 FROM " + view02);
                    Assert.assertTrue(rs.next());
                    Assert.assertTrue(rs.next());
                    Assert.assertTrue(rs.next());
                    Assert.assertTrue(rs.next());
                    Assert.assertTrue(rs.next());
                    Assert.assertFalse(rs.next());
                }
                return null;
            });
        }
        return callableList;
    }

}
