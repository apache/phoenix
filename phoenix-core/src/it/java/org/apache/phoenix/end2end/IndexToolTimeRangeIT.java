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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import org.apache.phoenix.mapreduce.index.IndexTool;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.util.EnvironmentEdge;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;

import static org.junit.Assert.assertFalse;

@Category(NeedsOwnMiniClusterTest.class)
public class IndexToolTimeRangeIT extends BaseTest {
    private static final String
            CREATE_TABLE_DDL = "CREATE TABLE %s (ID INTEGER NOT NULL PRIMARY KEY, "
            + "VAL1 INTEGER, VAL2 INTEGER) COLUMN_ENCODED_BYTES=0";
    public static final String CREATE_INDEX_DDL = "CREATE INDEX %s ON %s (VAL1) INCLUDE (VAL2)";
    private static final String UPSERT_TABLE_DML = "UPSERT INTO %s VALUES(?,?,?)";

    private static String dataTableFullName, indexTableFullName,
            schemaName, dataTableName, indexTableName;
    static MyClock myClock;

    @BeforeClass
    public static synchronized void setup() throws Exception {
        setupMiniCluster();
        createTableAndIndex();
        populateDataTable();
    }

    private static void createTableAndIndex() throws SQLException {
        schemaName = generateUniqueName();
        dataTableName = "D_"+generateUniqueName();
        dataTableFullName = SchemaUtil.getTableName(schemaName, dataTableName);
        indexTableName = "I_"+generateUniqueName();
        indexTableFullName = SchemaUtil.getTableName(schemaName, indexTableName);
        try (Connection conn = getConnection()) {
            conn.createStatement().execute(String.format(CREATE_TABLE_DDL, dataTableFullName));
            conn.commit();
            conn.createStatement().execute(String.format(CREATE_INDEX_DDL, indexTableName,
                    dataTableFullName));
            conn.commit();
        }
    }

    private static void populateDataTable() throws SQLException {
        myClock = new MyClock();
        EnvironmentEdgeManager.injectEdge(myClock);
        try {
            //So that we don't have to recompute the values below
            myClock.tickTime();
            myClock.tickTime();
            try (Connection conn = getConnection()) {
                //row 1-> time 4, row 2-> time 5, row 3-> time 6, row 4-> time 7, row 5-> time 8
                for (int i=0; i<5; i++) {
                    myClock.tickTime();
                    PreparedStatement ps = conn.prepareStatement(
                            String.format(UPSERT_TABLE_DML, dataTableFullName));
                    ps.setInt(1, i+1);
                    ps.setInt(2,(i+1)*10);
                    ps.setInt(3, (i+1)*100);
                    ps.execute();
                    conn.commit();
                }
            }
        } finally {
            EnvironmentEdgeManager.reset();
        }
    }

    private static void setupMiniCluster() throws Exception {
        Map<String, String> serverProps = Maps.newHashMapWithExpectedSize(2);
        serverProps.put(QueryServices.STATS_GUIDEPOST_WIDTH_BYTES_ATTRIB, Long.toString(20));
        serverProps.put(QueryServices.MAX_SERVER_METADATA_CACHE_TIME_TO_LIVE_MS_ATTRIB,
                Long.toString(5));
        serverProps.put(QueryServices.EXTRA_JDBC_ARGUMENTS_ATTRIB,
                QueryServicesOptions.DEFAULT_EXTRA_JDBC_ARGUMENTS);
        serverProps.put(QueryServices.INDEX_REBUILD_PAGE_SIZE_IN_ROWS, Long.toString(8));
        Map<String, String> clientProps = Maps.newHashMapWithExpectedSize(2);
        clientProps.put(QueryServices.USE_STATS_FOR_PARALLELIZATION, Boolean.toString(true));
        clientProps.put(QueryServices.STATS_UPDATE_FREQ_MS_ATTRIB, Long.toString(5));
        clientProps.put(QueryServices.TRANSACTIONS_ENABLED, Boolean.TRUE.toString());
        clientProps.put(QueryServices.FORCE_ROW_KEY_ORDER_ATTRIB, Boolean.TRUE.toString());
        setUpTestDriver(new ReadOnlyProps(serverProps.entrySet().iterator()),
                new ReadOnlyProps(clientProps.entrySet().iterator()));
    }

    private int countRowsInIndex() throws SQLException {
        String select = "SELECT COUNT(*) FROM "+indexTableFullName;
        try(Connection conn = getConnection()) {
            ResultSet rs = conn.createStatement().executeQuery(select);
            while(rs.next()) {
                return rs.getInt(1);
            }
        }
        return -1;
    }

    private static Connection getConnection() throws SQLException {
        return DriverManager.getConnection(getUrl());
    }

    @Test
    public void testValidTimeRange() throws Exception {
        String [] args = {"--delete-all-and-rebuild",
                "--start-time", myClock.getRelativeTimeAsString(1),
                "--end-time", myClock.getRelativeTimeAsString(9)};
        runIndexTool(args, 0);
        // all rows should be rebuilt
        Assert.assertEquals(5, countRowsInIndex());
    }

    @Test
    public void testValidTimeRange_startTimeInBetween() throws Exception {
        String [] args = {"--delete-all-and-rebuild",
                "--start-time", myClock.getRelativeTimeAsString(6),
                "--end-time", myClock.getRelativeTimeAsString(9)};
        runIndexTool(args, 0);
        // only last 3 rows should be rebuilt
        Assert.assertEquals(3, countRowsInIndex());
    }

    @Test
    public void testValidTimeRange_endTimeInBetween() throws Exception {
        String [] args = {"--delete-all-and-rebuild",
                "--start-time", myClock.getRelativeTimeAsString(1),
                "--end-time", myClock.getRelativeTimeAsString(6)};
        runIndexTool(args, 0);
        // only first 2 should be rebuilt
        Assert.assertEquals(2, countRowsInIndex());
    }

    @Test
    public void testNoTimeRangePassed() throws Exception {
        String [] args = {"--delete-all-and-rebuild"};
        runIndexTool(args, 0);
        // all rows should be rebuilt
        Assert.assertEquals(5, countRowsInIndex());
    }

    @Test
    public void testValidTimeRange_onlyStartTimePassed() throws Exception {
        //starttime passed of last upsert
        String [] args = {"--delete-all-and-rebuild",
                "--start-time", myClock.getRelativeTimeAsString(8)};
        runIndexTool(args, 0);
        Assert.assertEquals(1, countRowsInIndex());
    }

    @Test
    public void testValidTimeRange_onlyEndTimePassed() throws Exception {
        //end time passed as time of second upsert
        String [] args = {"--delete-all-and-rebuild",
                "--end-time", myClock.getRelativeTimeAsString(5)};
        runIndexTool(args, 0);
        Assert.assertEquals(1, countRowsInIndex());
    }

    private void runIndexTool(String [] args, int expectedStatus) throws Exception {
        IndexToolIT.runIndexTool(false, schemaName, dataTableName,
                indexTableName, null, expectedStatus,
                IndexTool.IndexVerifyType.NONE, args);
    }

    private static class MyClock extends EnvironmentEdge {
        public long epoch;
        public volatile long time;

        public MyClock() {
            epoch = System.currentTimeMillis();
            time = epoch;
        }

        @Override
        public long currentTime() {
            return time;
        }

        public long getRelativeTime(long n) {
            return epoch + n - 1;
        }

        public String getRelativeTimeAsString(long n) {
            return Long.toString(getRelativeTime(n));
        }

        public void tickTime() {
            time++;
        }
    }

    @AfterClass
    public static synchronized void teardown() throws Exception {
        boolean refCountLeaked = isAnyStoreRefCountLeaked();
        tearDownMiniCluster(2);
        assertFalse("refCount leaked", refCountLeaked);
    }
}
