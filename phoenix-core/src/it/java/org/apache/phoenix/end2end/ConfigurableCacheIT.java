/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */
package org.apache.phoenix.end2end;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

import org.apache.phoenix.jdbc.ConnectionInfo;
import org.apache.phoenix.query.ITGuidePostsCacheFactory;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * This tests that the configured client statistics cache is used during execution.  These tests
 * use a class ITGuidePostsCacheFactory which is for testing only that keeps track of the number
 * of cache instances generated.
 */
@Category(ParallelStatsEnabledTest.class)
public class ConfigurableCacheIT extends ParallelStatsEnabledIT {

    static String table;

    @BeforeClass
    public static synchronized void initTables() throws Exception {
        table = generateUniqueName();
        // Use phoenix test driver for setup
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement()
                    .execute("CREATE TABLE " + table
                            + " (k INTEGER PRIMARY KEY, c1.a bigint, c2.b bigint)"
                            + " GUIDE_POSTS_WIDTH=20");
            conn.createStatement().execute("upsert into " + table + " values (100,1,3)");
            conn.createStatement().execute("upsert into " + table + " values (101,2,4)");
            conn.createStatement().execute("upsert into " + table + " values (102,2,4)");
            conn.createStatement().execute("upsert into " + table + " values (103,2,4)");
            conn.createStatement().execute("upsert into " + table + " values (104,2,4)");
            conn.createStatement().execute("upsert into " + table + " values (105,2,4)");
            conn.createStatement().execute("upsert into " + table + " values (106,2,4)");
            conn.createStatement().execute("upsert into " + table + " values (107,2,4)");
            conn.createStatement().execute("upsert into " + table + " values (108,2,4)");
            conn.createStatement().execute("upsert into " + table + " values (109,2,4)");
            conn.commit();
            conn.createStatement().execute("UPDATE STATISTICS " + table);
            conn.commit();
        }
        ;
    }

    private Connection getCacheFactory(String principal, String cacheFactoryString)
            throws Exception {

        String url = getUrl();
        url = url.replace(";" + PhoenixRuntime.PHOENIX_TEST_DRIVER_URL_PARAM, "");

        // As there is a map of connections in the phoenix driver need to differentiate the url to
        // pick different QueryServices
        url = ConnectionInfo.create(url, null, null).withPrincipal(principal).toUrl();

        // Load defaults from QueryServicesTestImpl
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);

        // Parameterized URL
        props.put(QueryServices.GUIDE_POSTS_CACHE_FACTORY_CLASS, cacheFactoryString);

        // Stats Connection Props
        props.put(QueryServices.STATS_GUIDEPOST_WIDTH_BYTES_ATTRIB, Long.toString(20));
        props.put(QueryServices.STATS_UPDATE_FREQ_MS_ATTRIB, Long.toString(5l));
        props.put(QueryServices.MAX_SERVER_METADATA_CACHE_TIME_TO_LIVE_MS_ATTRIB, Long.toString(5));
        props.put(QueryServices.USE_STATS_FOR_PARALLELIZATION, Boolean.toString(true));

        Connection conn = DriverManager.getConnection(url, props);
        return conn;
    }

    /**
     * Test that if we don't specify the cacheFactory we won't increase the count of test.
     * @throws Exception
     */
    @Test
    public void testWithDefaults() throws Exception {
        int initialCount = ITGuidePostsCacheFactory.getCount();
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().executeQuery("SELECT * FROM " + table);
        }
        assertEquals(initialCount, ITGuidePostsCacheFactory.getCount());
    }

    /**
     * Tests that with a single ConnectionInfo we will not create more than one.
     * @throws Exception
     */
    @Test
    public void testWithSingle() throws Exception {
        int initialCount = ITGuidePostsCacheFactory.getCount();

        try (Connection conn =
                getCacheFactory("User1", ITGuidePostsCacheFactory.class.getTypeName())) {
            conn.createStatement().executeQuery("SELECT * FROM " + table);
        }
        try (Connection conn =
                getCacheFactory("User1", ITGuidePostsCacheFactory.class.getTypeName())) {
            conn.createStatement().executeQuery("SELECT * FROM " + table);
        }
        assertEquals(initialCount + 1, ITGuidePostsCacheFactory.getCount());
    }

    /**
     * Tests with 2 ConnectionInfo's
     * @throws Exception
     */
    @Test
    public void testWithMultiple() throws Exception {
        int initialCount = ITGuidePostsCacheFactory.getCount();
        try (Connection conn =
                getCacheFactory("User4", ITGuidePostsCacheFactory.class.getTypeName())) {
            conn.createStatement().executeQuery("SELECT * FROM " + table);
        }
        try (Connection conn =
                getCacheFactory("User6", ITGuidePostsCacheFactory.class.getTypeName())) {
            conn.createStatement().executeQuery("SELECT * FROM " + table);
        }
        assertEquals(initialCount + 2, ITGuidePostsCacheFactory.getCount());
    }

    /**
     * Tests that non-existent cacheFactory fails with exception
     * @throws Exception
     */
    @Test(expected = Exception.class)
    public void testBadCache() throws Exception {
        try (Connection conn =
                getCacheFactory("User8", "org.notreal.class")) {
        }
        fail();
    }
}
