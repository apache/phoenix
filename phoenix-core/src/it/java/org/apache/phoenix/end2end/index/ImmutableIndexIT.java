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

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.SimpleRegionObserver;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.end2end.BaseHBaseManagedTimeTableReuseIT;
import org.apache.phoenix.end2end.Shadower;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;


@RunWith(Parameterized.class)
public class ImmutableIndexIT extends BaseHBaseManagedTimeTableReuseIT {

    private final boolean localIndex;
    private final boolean transactional;
    private final String tableDDLOptions;

    private volatile boolean stopThreads = false;

    private static String TABLE_NAME;
    private static String INDEX_DDL;
    public static final AtomicInteger NUM_ROWS = new AtomicInteger(0);

    public ImmutableIndexIT(boolean localIndex, boolean transactional) {
        this.localIndex = localIndex;
        this.transactional = transactional;
        StringBuilder optionBuilder = new StringBuilder("IMMUTABLE_ROWS=true");
        if (transactional) {
            optionBuilder.append(", TRANSACTIONAL=true");
        }
        this.tableDDLOptions = optionBuilder.toString();

    }

    @BeforeClass
    @Shadower(classBeingShadowed = BaseHBaseManagedTimeTableReuseIT.class)
    public static void doSetup() throws Exception {
        Map<String, String> serverProps = Maps.newHashMapWithExpectedSize(1);
        serverProps.put("hbase.coprocessor.region.classes", CreateIndexRegionObserver.class.getName());
        Map<String, String> clientProps = Maps.newHashMapWithExpectedSize(2);
        clientProps.put(QueryServices.TRANSACTIONS_ENABLED, "true");
        clientProps.put(QueryServices.INDEX_POPULATION_SLEEP_TIME, "15000");
        setUpTestDriver(new ReadOnlyProps(serverProps.entrySet().iterator()), new ReadOnlyProps(clientProps.entrySet().iterator()));
    }

    @Parameters(name="localIndex = {0} , transactional = {1}")
    public static Collection<Boolean[]> data() {
		return Arrays.asList(new Boolean[][] { 
				{ false, false }, { false, true },
				{ true, false }, { true, true } });
    }

    @Test
    @Ignore
    public void testDropIfImmutableKeyValueColumn() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String tableName = "TBL_" + generateRandomString();
        String indexName = "IND_" + generateRandomString();
        String fullTableName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, tableName);
        String fullIndexName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, indexName);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(false);
            String ddl =
                    "CREATE TABLE " + fullTableName + BaseTest.TEST_TABLE_SCHEMA + tableDDLOptions;
            Statement stmt = conn.createStatement();
            stmt.execute(ddl);
            populateTestTable(fullTableName);
            ddl =
                    "CREATE " + (localIndex ? "LOCAL" : "") + " INDEX " + indexName + " ON "
                            + fullTableName + " (long_col1)";
            stmt.execute(ddl);

            ResultSet rs;

            rs = conn.createStatement().executeQuery("SELECT COUNT(*) FROM " + fullTableName);
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));
            rs = conn.createStatement().executeQuery("SELECT COUNT(*) FROM " + fullIndexName);
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));

            conn.setAutoCommit(true);
            String dml = "DELETE from " + fullTableName + " WHERE long_col2 = 4";
            try {
                conn.createStatement().execute(dml);
                fail();
            } catch (SQLException e) {
                assertEquals(SQLExceptionCode.INVALID_FILTER_ON_IMMUTABLE_ROWS.getErrorCode(),
                    e.getErrorCode());
            }

            conn.createStatement().execute("DROP TABLE " + fullTableName);
        }
    }

    @Test
    public void testCreateIndexDuringUpsertSelect() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(QueryServices.MUTATE_BATCH_SIZE_ATTRIB, Integer.toString(100));
        String tableName = "TBL_" + generateRandomString();
        String indexName = "IND_" + generateRandomString();
        String fullTableName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, tableName);
        String fullIndexName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, indexName);
        TABLE_NAME = fullTableName;
        String ddl ="CREATE TABLE " + TABLE_NAME + BaseTest.TEST_TABLE_SCHEMA + tableDDLOptions;
        INDEX_DDL = "CREATE " + (localIndex ? "LOCAL" : "") + " INDEX IF NOT EXISTS " + indexName + " ON " + TABLE_NAME
                + " (long_pk, varchar_pk)"
                + " INCLUDE (long_col1, long_col2)";

        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(false);
            Statement stmt = conn.createStatement();
            stmt.execute(ddl);

            upsertRows(conn, TABLE_NAME, 220);
            conn.commit();

            // run the upsert select and also create an index
            conn.setAutoCommit(true);
            String upsertSelect = "UPSERT INTO " + TABLE_NAME + "(varchar_pk, char_pk, int_pk, long_pk, decimal_pk, date_pk) " +
                    "SELECT varchar_pk||'_upsert_select', char_pk, int_pk, long_pk, decimal_pk, date_pk FROM "+ TABLE_NAME;
            conn.createStatement().execute(upsertSelect);
            ResultSet rs;
            rs = conn.createStatement().executeQuery("SELECT /*+ NO_INDEX */ COUNT(*) FROM " + TABLE_NAME);
            assertTrue(rs.next());
            assertEquals(440,rs.getInt(1));
            rs = conn.createStatement().executeQuery("SELECT COUNT(*) FROM " + TABLE_NAME);
            assertTrue(rs.next());
            assertEquals(440,rs.getInt(1));
        }
    }

    // used to create an index while a batch of rows are being written
    public static class CreateIndexRegionObserver extends SimpleRegionObserver {
        @Override
        public void postPut(ObserverContext<RegionCoprocessorEnvironment> c,
                Put put, WALEdit edit, final Durability durability)
                        throws HBaseIOException {
            String tableName = c.getEnvironment().getRegion().getRegionInfo()
                    .getTable().getNameAsString();
            if (tableName.equalsIgnoreCase(TABLE_NAME)
                    // create the index after the second batch  
                    && Bytes.startsWith(put.getRow(), Bytes.toBytes("varchar200_upsert_select"))) {
                try {
                    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
                    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
                        conn.createStatement().execute(INDEX_DDL);
                    }
                } catch (SQLException e) {
                    throw new DoNotRetryIOException(e);
                } 
            }
        }
    }

    private class UpsertRunnable implements Runnable {
        private static final int NUM_ROWS_IN_BATCH = 10;
        private final String fullTableName;

        public UpsertRunnable(String fullTableName) {
            this.fullTableName = fullTableName;
        }

        @Override
        public void run() {
            Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
            try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
                while (!stopThreads) {
                    // write a large batch of rows
                    boolean fistRowInBatch = true;
                    for (int i=0; i<NUM_ROWS_IN_BATCH && !stopThreads; ++i) {
                        BaseTest.upsertRow(conn, fullTableName, NUM_ROWS.incrementAndGet(), fistRowInBatch);
                        fistRowInBatch = false;
                    }
                    conn.commit();
                    Thread.sleep(10);
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
            }
        }
    }

    @Test
    public void testCreateIndexWhileUpsertingData() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String tableName = "TBL_" + generateRandomString();
        String indexName = "IND_" + generateRandomString();
        String fullTableName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, tableName);
        String fullIndexName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, indexName);
        String ddl ="CREATE TABLE " + fullTableName + BaseTest.TEST_TABLE_SCHEMA + tableDDLOptions;
        String indexDDL = "CREATE " + (localIndex ? "LOCAL" : "") + " INDEX " + indexName + " ON " + fullTableName
                + " (long_pk, varchar_pk)"
                + " INCLUDE (long_col1, long_col2)";
        int numThreads = 2;
        ExecutorService executorService = Executors.newFixedThreadPool(numThreads, new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread t = Executors.defaultThreadFactory().newThread(r);
                t.setDaemon(true);
                t.setPriority(Thread.MIN_PRIORITY);
                return t;
            }
        });
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(true);
            Statement stmt = conn.createStatement();
            stmt.execute(ddl);

            ResultSet rs;
            rs = conn.createStatement().executeQuery("SELECT /*+ NO_INDEX */ COUNT(*) FROM " + fullTableName);
            assertTrue(rs.next());
            int dataTableRowCount = rs.getInt(1);
            assertEquals(0,dataTableRowCount);

            List<Future<?>> futureList = Lists.newArrayListWithExpectedSize(numThreads);
            for (int i =0; i<numThreads; ++i) {
                futureList.add(executorService.submit(new UpsertRunnable(fullTableName)));
            }
            // upsert some rows before creating the index 
            Thread.sleep(100);

            // create the index 
            try (Connection conn2 = DriverManager.getConnection(getUrl(), props)) {
                conn2.createStatement().execute(indexDDL);
            }

            // upsert some rows after creating the index
            Thread.sleep(50);
            // cancel the running threads
            stopThreads = true;
            executorService.shutdown();
            assertTrue(executorService.awaitTermination(30, TimeUnit.SECONDS));

            rs = conn.createStatement().executeQuery("SELECT /*+ NO_INDEX */ COUNT(*) FROM " + fullTableName);
            assertTrue(rs.next());
            dataTableRowCount = rs.getInt(1);
            rs = conn.createStatement().executeQuery("SELECT COUNT(*) FROM " + fullIndexName);
            assertTrue(rs.next());
            int indexTableRowCount = rs.getInt(1);
            assertEquals("Data and Index table should have the same number of rows ", dataTableRowCount, indexTableRowCount);
        } finally {
            executorService.shutdownNow();
        }
    }

}
