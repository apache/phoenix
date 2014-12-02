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
package org.apache.phoenix.end2end.index;

import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL;
import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR;
import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL_TERMINATOR;
import static org.apache.phoenix.util.PhoenixRuntime.PHOENIX_TEST_DRIVER_URL_PARAM;
import static org.apache.phoenix.util.TestUtil.LOCALHOST;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.jdbc.PhoenixTestDriver;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.StringUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Maps;

@Category(NeedsOwnMiniClusterTest.class)
public class DropIndexDuringUpsertIT extends BaseTest {
    private static final int NUM_SLAVES = 4;
    private static String url;
    private static PhoenixTestDriver driver;
    private static HBaseTestingUtility util;

    private static ExecutorService service = Executors.newCachedThreadPool();

    private static final String SCHEMA_NAME = "S";
    private static final String INDEX_TABLE_NAME = "I";
    private static final String DATA_TABLE_FULL_NAME = SchemaUtil.getTableName(SCHEMA_NAME, "T");
    private static final String INDEX_TABLE_FULL_NAME = SchemaUtil.getTableName(SCHEMA_NAME, "I");

    @Before
    public void doSetup() throws Exception {
        Configuration conf = HBaseConfiguration.create();
        setUpConfigForMiniCluster(conf);
        conf.setInt("hbase.client.retries.number", 2);
        conf.setInt("hbase.client.pause", 5000);
        conf.setInt("hbase.balancer.period", Integer.MAX_VALUE);
        conf.setLong(QueryServices.INDEX_FAILURE_HANDLING_REBUILD_OVERLAP_TIME_ATTRIB, 0);
        util = new HBaseTestingUtility(conf);
        util.startMiniCluster(NUM_SLAVES);
        String clientPort = util.getConfiguration().get(QueryServices.ZOOKEEPER_PORT_ATTRIB);
        url = JDBC_PROTOCOL + JDBC_PROTOCOL_SEPARATOR + LOCALHOST + JDBC_PROTOCOL_SEPARATOR + clientPort
                + JDBC_PROTOCOL_TERMINATOR + PHOENIX_TEST_DRIVER_URL_PARAM;

        Map<String, String> props = Maps.newHashMapWithExpectedSize(1);
        // Must update config before starting server
        props.put(QueryServices.DROP_METADATA_ATTRIB, Boolean.toString(true));
        driver = initAndRegisterDriver(url, new ReadOnlyProps(props.entrySet().iterator()));
    }

    @After
    public void tearDown() throws Exception {
        try {
            destroyDriver(driver);
        } finally {
            util.shutdownMiniCluster();
        }
    }

    @Ignore // FIXME: this fails 100% of the time on the Mac
    @Test(timeout = 300000)
    public void testWriteFailureDropIndex() throws Exception {
        String query;
        ResultSet rs;

        // create the table and ensure its empty
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = driver.connect(url, props);
        conn.createStatement().execute(
                "CREATE TABLE " + DATA_TABLE_FULL_NAME + " (k VARCHAR NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR)");
        query = "SELECT * FROM " + DATA_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery(query);
        assertFalse(rs.next());

        // create the index and ensure its empty as well
        conn.createStatement().execute(
                "CREATE INDEX " + INDEX_TABLE_NAME + " ON " + DATA_TABLE_FULL_NAME + " (v1) INCLUDE (v2)");
        query = "SELECT * FROM " + INDEX_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery(query);
        assertFalse(rs.next());

        // Verify the metadata for index is correct.
        rs = conn.getMetaData().getTables(null, StringUtil.escapeLike(SCHEMA_NAME), INDEX_TABLE_NAME,
                new String[] { PTableType.INDEX.toString() });
        assertTrue(rs.next());
        assertEquals(INDEX_TABLE_NAME, rs.getString(3));
        assertEquals(PIndexState.ACTIVE.toString(), rs.getString("INDEX_STATE"));
        assertFalse(rs.next());

        // do an upsert on a separate thread
        Future<Boolean> future = service.submit(new UpsertTask());
        Thread.sleep(500);

        // at the same time, drop the index table
        conn.createStatement().execute("drop index " + INDEX_TABLE_NAME + " on " + DATA_TABLE_FULL_NAME);

        // verify index is dropped
        query = "SELECT count(1) FROM " + INDEX_TABLE_FULL_NAME;
        try {
            conn.createStatement().executeQuery(query);
            fail();
        } catch (SQLException e) {
        }

        // assert {@KillServerOnFailurePolicy} is not triggered
        assertTrue(future.get());
    }

    private static class UpsertTask implements Callable<Boolean> {

        private Connection conn = null;

        public UpsertTask() throws SQLException {
            Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
            conn = driver.connect(url, props);
        }

        @Override
        public Boolean call() throws Exception {
            PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + DATA_TABLE_FULL_NAME + " VALUES(?,?,?)");
            for (int i = 0; i < 500; i++) {
                stmt.setString(1, "a");
                stmt.setString(2, "x");
                stmt.setString(3, Integer.toString(i));
                stmt.execute();
                conn.commit();
            }
            return true;
        }

    }

}
