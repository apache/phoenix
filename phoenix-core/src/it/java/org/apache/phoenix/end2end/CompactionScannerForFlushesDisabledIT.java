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

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.ManualEnvironmentEdge;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.TestUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import static org.junit.Assert.assertEquals;

@Category(NeedsOwnMiniClusterTest.class)
@RunWith(Parameterized.class)
public class CompactionScannerForFlushesDisabledIT extends BaseTest {
    private static final int MAX_LOOKBACK_AGE = 15;
    private String tableDDLOptions;
    private StringBuilder optionBuilder;
    ManualEnvironmentEdge injectEdge;
    private int ttl;
    private boolean multiCF;

    public CompactionScannerForFlushesDisabledIT(boolean multiCF) {
        this.multiCF = multiCF;
    }

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        Map<String, String> props = Maps.newHashMapWithExpectedSize(2);
        props.put(QueryServices.GLOBAL_INDEX_ROW_AGE_THRESHOLD_TO_DELETE_MS_ATTRIB, Long.toString(0));
        props.put(BaseScannerRegionObserverConstants.PHOENIX_MAX_LOOKBACK_AGE_CONF_KEY, Integer.toString(MAX_LOOKBACK_AGE));
        props.put(QueryServices.PHOENIX_COMPACTION_SCANNER_FOR_FLUSHES_ENABLED, "false");
        props.put("hbase.procedure.remote.dispatcher.delay.msec", "0");
        props.put(HRegion.MEMSTORE_PERIODIC_FLUSH_INTERVAL, "0");
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }

    @Before
    public void beforeTest(){
        EnvironmentEdgeManager.reset();
        optionBuilder = new StringBuilder();
        ttl = 30;
        optionBuilder.append(" TTL=" + ttl);
        this.tableDDLOptions = optionBuilder.toString();
        injectEdge = new ManualEnvironmentEdge();
        injectEdge.setValue(EnvironmentEdgeManager.currentTimeMillis());
    }

    @After
    public synchronized void afterTest() throws Exception {
        boolean refCountLeaked = isAnyStoreRefCountLeaked();

        EnvironmentEdgeManager.reset();
        Assert.assertFalse("refCount leaked", refCountLeaked);
    }

    @Parameterized.Parameters(name = "CompactionScannerForFlushesDisabledIT_multiCF={0}")
    public static Collection<Boolean> data() {
        return Arrays.asList(false, true);
    }

    @Test
    public void testRetainingAllRowVersions() throws Exception {
        try(Connection conn = DriverManager.getConnection(getUrl())) {
            String tableName = generateUniqueName();
            createTable(tableName);
            long timeIntervalBetweenTwoUpserts = (ttl / 2) + 1;
            injectEdge.setValue(System.currentTimeMillis());
            EnvironmentEdgeManager.injectEdge(injectEdge);
            TableName dataTableName = TableName.valueOf(tableName);
            injectEdge.incrementValue(1);
            Statement stmt = conn.createStatement();
            stmt.execute("upsert into " + tableName + " values ('a', 'ab', 'abc', 'abcd')");
            conn.commit();
            injectEdge.incrementValue(timeIntervalBetweenTwoUpserts * 1000);
            stmt.execute("upsert into " + tableName + " values ('a', 'ab1')");
            conn.commit();
            injectEdge.incrementValue(timeIntervalBetweenTwoUpserts * 1000);
            stmt.execute("upsert into " + tableName + " values ('a', 'ab2')");
            conn.commit();
            injectEdge.incrementValue(timeIntervalBetweenTwoUpserts * 1000);
            stmt.execute("upsert into " + tableName + " values ('a', 'ab3')");
            conn.commit();
            injectEdge.incrementValue(MAX_LOOKBACK_AGE * 1000);

            TestUtil.dumpTable(conn, dataTableName);
            byte[] rowKey = Bytes.toBytes("a");
            int rawCellCount = TestUtil.getRawCellCount(conn, dataTableName, rowKey);
            // 6 non-empty cells (ab3, ab2, ab1, ab, abc, abcd) + 4 empty cells (for 4 upserts)
            assertEquals(10, rawCellCount);

            TestUtil.flush(utility, dataTableName);
            injectEdge.incrementValue(1);
            rawCellCount = TestUtil.getRawCellCount(conn, dataTableName, rowKey);
            // Compaction scanner wasn't used during flushes and excess rows versions are flushed
            assertEquals(10, rawCellCount);
            TestUtil.dumpTable(conn, dataTableName);

            TestUtil.majorCompact(getUtility(), dataTableName);
            injectEdge.incrementValue(1);
            rawCellCount = TestUtil.getRawCellCount(conn, dataTableName, rowKey);
            // 1 non-empty cell (ab3) and 1 empty cell at the edge of max lookback window will be
            // retained
            // 2 non-empty cells outside max lookback window will be retained (abc, abcd)
            // 2 empty cells will be retained outside max lookback window
            assertEquals(6, rawCellCount);
            TestUtil.dumpTable(conn, dataTableName);

            ResultSet rs = stmt.executeQuery("select * from " + dataTableName + " where id = 'a'");
            while(rs.next()) {
                assertEquals("abc", rs.getString(3));
                assertEquals("abcd", rs.getString(4));
            }
        }
    }

    private void createTable(String tableName) throws SQLException {
        try(Connection conn = DriverManager.getConnection(getUrl())) {
            String createSql;
            if (multiCF) {
                createSql = "create table " + tableName +
                        " (id varchar(10) not null primary key, val1 varchar(10), " +
                        "a.val2 varchar(10), b.val3 varchar(10))" + tableDDLOptions;
            }
            else {
                createSql = "create table " + tableName +
                        " (id varchar(10) not null primary key, val1 varchar(10), " +
                        "val2 varchar(10), val3 varchar(10))" + tableDDLOptions;
            }
            conn.createStatement().execute(createSql);
            conn.commit();
        }
    }
}
