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

import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PMetaData;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.ServerUtil.ConnectionFactory;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.AfterParam;
import org.junit.runners.Parameterized.BeforeParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotNull;


@RunWith(Parameterized.class)
@Category(NeedsOwnMiniClusterTest.class)
public class IndexAsyncThresholdIT extends BaseTest {

    private static final Logger logger = LoggerFactory.getLogger(IndexAsyncThresholdIT.class);

    private final String tableName;
    private final long rows;
    private final long columns;
    private final boolean overThreshold;
    private final Mode mode;

    enum Mode {
        NORMAL,
        ASYNC,
        COVERED,
        FUNCTIONAL
    }

    public IndexAsyncThresholdIT(Long threshold, Long rows, Long columns, Long overThreshold,
                                 Long mode)
            throws Exception {
        this.tableName = generateUniqueName();
        this.rows = rows;
        this.columns = columns;
        this.overThreshold = overThreshold == 0;
        this.mode = mode.equals(0L) ? Mode.NORMAL :
                mode.equals(1L) ? Mode.ASYNC :
                        mode.equals(2L) ? Mode.COVERED :
                                Mode.FUNCTIONAL;
    }

    @Parameterized.Parameters
    public static synchronized Collection<Long[]> primeNumbers() {
        return Arrays.asList(new Long[][]{
                {100000L, 5000L, 10L, 0L, 0L},
                {Long.MAX_VALUE, 200L, 100L, 1L, 0L},
                {0L, 20L, 10L, 1L, 0L},
                {1L, 20L, 10L, 1L, 1L},
                {1L, 20L, 10L, 0L, 2L},
                {1L, 100L, 10L, 0L, 3L},
        });
    }

    @BeforeParam
    public static synchronized void setupMiniCluster(Long threshold, Long rows, Long columns,
                                                     Long overThreshold, Long mode)
            throws Exception {
        Configuration conf = HBaseConfiguration.create();
        Map<String, String> props = Maps.newHashMapWithExpectedSize(1);
        props.put(QueryServices.CLIENT_INDEX_ASYNC_THRESHOLD, Long.toString(threshold));
        url = setUpTestCluster(conf, new ReadOnlyProps(props.entrySet().iterator()));
        driver = initAndRegisterTestDriver(url, new ReadOnlyProps(props.entrySet().iterator()));
    }

    @AfterParam
    public static synchronized void tearDownMiniCluster() throws Exception {
        boolean refCountLeaked = isAnyStoreRefCountLeaked();
        destroyDriver(driver);
        try {
            HBaseTestingUtility u = new HBaseTestingUtility();
            u.shutdownMiniCluster();
        } catch (Throwable t) {
            logger.error("Exception caught when shutting down mini cluster", t);
        } finally {
            ConnectionFactory.shutdown();
        }
        assertFalse("refCount leaked", refCountLeaked);
    }

    @Test
    public void testAsyncIndexCreation() throws Exception {
        try (Connection connection = driver.connect(url, new Properties())) {
            Statement stmt = connection.createStatement();
            String indexName = "INDEX" + this.tableName;
            createAndPopulateTable(connection, this.tableName, rows, columns);
            connection.createStatement().execute("UPDATE STATISTICS " + this.tableName);
            connection.commit();
            ResultSet rs = stmt.executeQuery("select count(*) from " + this.tableName);
            assertTrue(rs.next());
            assertEquals(rows, rs.getInt(1));

            SQLException exception = null;
            try {
                String statement = "create index " + indexName + " ON " + this.tableName;
                if (this.mode == Mode.NORMAL || this.mode == Mode.ASYNC) {
                    statement += " (col2, col5, col6, col7, col8)";
                    if (this.mode == Mode.ASYNC) {
                        statement += "  ASYNC";
                    }
                } else if (this.mode == Mode.COVERED) {
                    statement += " (col2) INCLUDE(col5, col6, col7, col8)";
                } else {  // mode == Functional
                    statement += " (UPPER(col2 || col4))";
                }

                stmt.execute(statement);
            } catch (Exception e) {
                assert e instanceof SQLException;
                exception = (SQLException) e;
            }
            connection.commit();
            PTableKey key = new PTableKey(null, this.tableName);
            PMetaData metaCache = connection.unwrap(PhoenixConnection.class).getMetaDataCache();
            List<PTable> indexes = metaCache.getTableRef(key).getTable().getIndexes();
            if (!overThreshold) {
                if (this.mode == Mode.ASYNC) {
                    assertEquals(PIndexState.BUILDING, indexes.get(0).getIndexState());
                } else {
                    assertEquals(PIndexState.ACTIVE, indexes.get(0).getIndexState());
                }
                assertNull(exception);
            } else {
                assertEquals(0, indexes.size());
                assertNotNull(exception);
                assertEquals(exception.getErrorCode(),
                        SQLExceptionCode.ABOVE_INDEX_NON_ASYNC_THRESHOLD.getErrorCode());
            }
        }
    }

    private void createAndPopulateTable(Connection conn, String fullTableName, Long rows,
                                        Long columns)
            throws SQLException {
        Statement stmt = conn.createStatement();
        StringBuilder ddl = new StringBuilder("CREATE TABLE " + fullTableName
                + " (col1 varchar primary key");
        for (int i = 2; i < columns; i++) {
            ddl.append(", col").append(i).append(" varchar");
        }
        ddl.append(")");
        stmt.execute(ddl.toString());
        conn.commit();
        for (int i = 0; i < rows; i++) {
            StringBuilder dml = new StringBuilder("upsert into " + fullTableName + " values (");
            for (int j = 1; j < columns; j++) {
                dml.append("'col").append(j).append("VAL").append(i).append("'");
                if (j < columns - 1) {
                    dml.append(", ");
                }
            }
            dml.append(")");
            stmt.execute(dml.toString());
        }
        conn.commit();
    }
}