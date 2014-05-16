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
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixTestDriver;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.StringUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
/**
 * 
 * Test for failure of region server to write to index table.
 * For some reason dropping tables after running this test
 * fails unless it runs its own mini cluster. 
 * 
 * 
 * @since 2.1
 */
@Category(NeedsOwnMiniClusterTest.class)
public class MutableIndexFailureIT extends BaseTest {
    private static String url;
    private static PhoenixTestDriver driver;
    private static HBaseTestingUtility util;

    private static final String SCHEMA_NAME = "";
    private static final String INDEX_TABLE_NAME = "I";
    private static final String DATA_TABLE_FULL_NAME = SchemaUtil.getTableName(SCHEMA_NAME, "T");
    private static final String INDEX_TABLE_FULL_NAME = SchemaUtil.getTableName(SCHEMA_NAME, "I");

    @BeforeClass
    public static void doSetup() throws Exception {
        Configuration conf = HBaseConfiguration.create();
        setUpConfigForMiniCluster(conf);
        conf.setInt("hbase.client.retries.number", 2);
        conf.setInt("hbase.client.pause", 5000);
        util = new HBaseTestingUtility(conf);
        util.startMiniCluster();
        String clientPort = util.getConfiguration().get(QueryServices.ZOOKEEPER_PORT_ATTRIB);
        url = JDBC_PROTOCOL + JDBC_PROTOCOL_SEPARATOR + LOCALHOST + JDBC_PROTOCOL_SEPARATOR + clientPort
                + JDBC_PROTOCOL_TERMINATOR + PHOENIX_TEST_DRIVER_URL_PARAM;
        driver = initAndRegisterDriver(url, ReadOnlyProps.EMPTY_PROPS);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        try {
            destroyDriver(driver);
        } finally {
            util.shutdownMiniCluster();
        }
    }

    private static void destroyIndexTable() throws Exception {
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = driver.connect(url, props);
        ConnectionQueryServices services = conn.unwrap(PhoenixConnection.class).getQueryServices();
        HBaseAdmin admin = services.getAdmin();
        try {
            admin.disableTable(INDEX_TABLE_FULL_NAME);
            admin.deleteTable(INDEX_TABLE_FULL_NAME);
        } catch (TableNotFoundException e) {} finally {
            conn.close();
            admin.close();
        }
    }

    @Test
    public void testWriteFailureDisablesIndex() throws Exception {
        String query;
        ResultSet rs;

        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = driver.connect(url, props);
        conn.setAutoCommit(false);
        conn.createStatement().execute(
                "CREATE TABLE " + DATA_TABLE_FULL_NAME + " (k VARCHAR NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR)");
        query = "SELECT * FROM " + DATA_TABLE_FULL_NAME;
        rs = conn.createStatement().executeQuery(query);
        assertFalse(rs.next());

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

        destroyIndexTable();

        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + DATA_TABLE_FULL_NAME + " VALUES(?,?,?)");
        stmt.setString(1, "a");
        stmt.setString(2, "x");
        stmt.setString(3, "1");
        stmt.execute();
        try {
            conn.commit();
            fail();
        } catch (SQLException e) {}

        // Verify the metadata for index is correct.
        rs = conn.getMetaData().getTables(null, StringUtil.escapeLike(SCHEMA_NAME), INDEX_TABLE_NAME,
                new String[] { PTableType.INDEX.toString() });
        assertTrue(rs.next());
        assertEquals(INDEX_TABLE_NAME, rs.getString(3));
        assertEquals(PIndexState.DISABLE.toString(), rs.getString("INDEX_STATE"));
        assertFalse(rs.next());
    }
}