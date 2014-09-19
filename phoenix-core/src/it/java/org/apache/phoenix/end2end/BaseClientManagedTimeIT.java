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

import static org.junit.Assert.assertTrue;

import java.sql.Date;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.phoenix.jdbc.PhoenixEmbeddedDriver;
import org.apache.phoenix.jdbc.PhoenixTestDriver;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

/**
 * Base class for tests that manage their own time stamps
 * We need to separate these from tests that rely on hbase to set
 * timestamps, because we create/destroy the Phoenix tables
 * between tests and only allow a table time stamp to increase.
 * Without this separation table deletion/creation would fail.
 * 
 * All tests extending this class use the mini cluster that is
 * different from the mini cluster used by test classes extending 
 * {@link BaseHBaseManagedTimeIT}.
 * 
 * @since 0.1
 */
@NotThreadSafe
@Category(ClientManagedTimeTest.class)
public abstract class BaseClientManagedTimeIT extends BaseTest {
    private static String url;
    protected static PhoenixTestDriver driver;
    protected static final Configuration config = HBaseConfiguration.create(); 
    private static boolean clusterInitialized = false;
    
    protected final static String getUrl() {
        return checkClusterInitialized();
    }
    
    protected static Configuration getTestClusterConfig() {
        // don't want callers to modify config.
        return new Configuration(config);
    }
    
    @After
    public void cleanUpAfterTest() throws Exception {
        long ts = nextTimestamp();
        deletePriorTables(ts - 1, getUrl());    
    }
    
    @BeforeClass
    public static void doSetup() throws Exception {
        setUpTestDriver(getUrl(), ReadOnlyProps.EMPTY_PROPS);
    }
    
    protected static void setUpTestDriver(String url, ReadOnlyProps props) throws Exception {
        if (PhoenixEmbeddedDriver.isTestUrl(url)) {
            checkClusterInitialized();
            if (driver == null) {
                driver = initAndRegisterDriver(url, props);
            }
        }
    }

    private static String checkClusterInitialized() {
        if (!clusterInitialized) {
            url = setUpTestCluster(config);
            clusterInitialized = true;
        }
        return url;
    }
    
    @AfterClass
    public static void dropTables() throws Exception {
        try {
            disableAndDropNonSystemTables(driver);
        } finally {
            try {
                assertTrue(destroyDriver(driver));
            } finally {
                driver = null;
            }
        }
    }
    
    protected static void initATableValues(String tenantId, byte[][] splits, Date date, Long ts) throws Exception {
        BaseTest.initATableValues(tenantId, splits, date, ts, getUrl());
    }
    
    protected static void initEntityHistoryTableValues(String tenantId, byte[][] splits, Date date, Long ts) throws Exception {
        BaseTest.initEntityHistoryTableValues(tenantId, splits, date, ts, getUrl());
    }
    
    protected static void initSaltedEntityHistoryTableValues(String tenantId, byte[][] splits, Date date, Long ts) throws Exception {
        BaseTest.initSaltedEntityHistoryTableValues(tenantId, splits, date, ts, getUrl());
    }
        
}
