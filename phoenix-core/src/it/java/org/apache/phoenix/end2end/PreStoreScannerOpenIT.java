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
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.regionserver.CompactSplit;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.StoreFileScanner;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.ManualEnvironmentEdge;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.agent.ByteBuddyAgent;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.dynamic.loading.ClassReloadingStrategy;
import net.bytebuddy.matcher.ElementMatchers;

@Category(NeedsOwnMiniClusterTest.class)
public class PreStoreScannerOpenIT extends BaseTest {

    private static final int TTL_IN_DAYS = 30;
    // The table name is used to filter the seek/reseek calls
    public static final String TABLE_NAME = generateUniqueName();

    // Counters for tracking seek/reseek calls
    public static final AtomicLong SEEK_COUNT = new AtomicLong(0);
    public static final AtomicLong RESEEK_COUNT = new AtomicLong(0);

    // ByteBuddy advice classes for intercepting method calls
    public static class SeekAdvice {
        @Advice.OnMethodEnter
        public static void onSeek(@Advice.This StoreFileScanner scanner) {
            if (scanner.getFilePath().toString().contains(TABLE_NAME)) {
                SEEK_COUNT.incrementAndGet();
                System.out.println("SEEK called! Total seeks: " + SEEK_COUNT.get());
            }
        }
    }

    public static class ReseekAdvice {
        @Advice.OnMethodEnter
        public static void onReseek(@Advice.This StoreFileScanner scanner) {
            if (scanner.getFilePath().toString().contains(TABLE_NAME)) {
                RESEEK_COUNT.incrementAndGet();
                System.out.println("RESEEK called! Total reseeks: " + RESEEK_COUNT.get());
            }
        }
    }

    @BeforeClass
    public static void setUp() throws Exception {
        // Install ByteBuddy agent
        ByteBuddyAgent.install();

        // Instrument StoreFileScanner to track seek/reseek calls
        new ByteBuddy().redefine(StoreFileScanner.class)
            .visit(Advice.to(SeekAdvice.class).on(ElementMatchers.named("seek")))
            .visit(Advice.to(ReseekAdvice.class).on(ElementMatchers.named("reseek"))).make()
            .load(StoreFileScanner.class.getClassLoader(),
                ClassReloadingStrategy.fromInstalledAgent());

        System.out.println("ByteBuddy instrumentation installed for StoreFileScanner");

        Map<String, String> props = new HashMap<>();
        // Disable periodic flushes
        props.put(HRegion.MEMSTORE_PERIODIC_FLUSH_INTERVAL, "0");
        // Disable compactions
        props.put(CompactSplit.HBASE_REGION_SERVER_ENABLE_COMPACTION, "false");
        props.put("hbase.procedure.remote.dispatcher.delay.msec", "0");
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }

    /**
     * Test that the store scanner does not do extra seeks when scanning a table with Phoenix TTL
     * enabled. Setting min and max versions to > 1 in
     * {@link BaseScannerRegionObserver#preStoreScannerOpen} will make StoreScanner to do extra
     * seeks.
     * @throws Exception
     */
    @Test
    public void testStoreScannerNotDoingExtraSeeks() throws Exception {
        createTable(TABLE_NAME);

        try (Connection conn = DriverManager.getConnection(getUrl());
            Statement stmt = conn.createStatement()) {
            stmt.execute("UPSERT INTO " + TABLE_NAME + " (id, col1) VALUES (1, 'a')");
            stmt.execute("UPSERT INTO " + TABLE_NAME + " (id, col1) VALUES (10, 'b')");
            conn.commit();
            getUtility().flush(TableName.valueOf(TABLE_NAME));

            stmt.execute("UPSERT INTO " + TABLE_NAME + " (id, col1) VALUES (2, 'c')");
            stmt.execute("UPSERT INTO " + TABLE_NAME + " (id, col1) VALUES (9, 'd')");
            conn.commit();
            getUtility().flush(TableName.valueOf(TABLE_NAME));

            stmt.execute("UPSERT INTO " + TABLE_NAME + " (id, col1) VALUES (3, 'e')");
            conn.commit();
            // The newly inserted row will be in memstore only
        }

        Assert.assertEquals(2, getStoreFileCount(TABLE_NAME));

        try (Connection conn = DriverManager.getConnection(getUrl());
            Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT col1 FROM " + TABLE_NAME + " WHERE id = 3");
            Assert.assertTrue(rs.next());
            Assert.assertFalse(rs.next());
        }
        Assert.assertEquals(0, SEEK_COUNT.get());
        Assert.assertEquals(0, RESEEK_COUNT.get());
    }

    /**
     * Test SCN queries can see deleted rows. Setting keepDeletedCells to a
     * {@link KeepDeletedCells.FALSE} in {@link BaseScannerRegionObserver#preStoreScannerOpen} will
     * make StoreScanner to not return deleted rows for SCN queries.
     * @throws Exception
     */
    @Test
    public void testSCNScansCanSeeDeletedRows() throws Exception {
        String tableName = generateUniqueName();
        createTable(tableName);

        long beforeRowWasDeleted;
        try (Connection conn = DriverManager.getConnection(getUrl());
            Statement stmt = conn.createStatement()) {
            stmt.execute("UPSERT INTO " + tableName + " (id, col1) VALUES (1, 'a')");
            conn.commit();
            getUtility().flush(TableName.valueOf(tableName));

            beforeRowWasDeleted = EnvironmentEdgeManager.currentTimeMillis();
            // Sleep for 1ms for timestamps to be different for delete and insert
            Thread.sleep(1);

            stmt.execute("DELETE FROM " + tableName + " WHERE id = 1");
            conn.commit();
            getUtility().flush(TableName.valueOf(tableName));

            Thread.sleep(1);

            // Check that the row is not visible for non-SCN queries
            ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName + " WHERE id = 1");
            Assert.assertFalse(rs.next());
        }

        Properties props = new Properties();
        props.put(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(beforeRowWasDeleted));
        try (Connection conn = DriverManager.getConnection(getUrl(), props);
            Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName + " WHERE id = 1");
            Assert.assertTrue(rs.next());
            Assert.assertFalse(rs.next());
        }
    }

    /**
     * Test that rows are not expired partially. Setting TTL to {@link HConstants.FOREVER} in
     * {@link BaseScannerRegionObserver#preStoreScannerOpen} will make StoreScanner to not return
     * cells that are expired for HBase leading rows to expire partially.
     * @throws Exception
     */
    @Test
    public void testRowsAreNotExpiredPartially() throws Exception {
        String tableName = generateUniqueName();
        createTable(tableName);

        ManualEnvironmentEdge injectEdge = new ManualEnvironmentEdge();
        injectEdge.setValue(EnvironmentEdgeManager.currentTimeMillis());
        EnvironmentEdgeManager.injectEdge(injectEdge);

        try (Connection conn = DriverManager.getConnection(getUrl());
            Statement stmt = conn.createStatement()) {
            stmt.execute("UPSERT INTO " + tableName + " (id, col1, col2) VALUES (1, 'a', 'ab')");
            conn.commit();

            injectEdge.incrementValue(1);
            getUtility().flush(TableName.valueOf(tableName));

            Assert.assertTrue(HConstants.FOREVER / (24 * 60 * 60 * 1000) < TTL_IN_DAYS);
            injectEdge.incrementValue(HConstants.FOREVER);

            stmt.execute("UPSERT INTO " + tableName + " (id, col1) VALUES (1, 'b')");
            conn.commit();

            injectEdge.incrementValue(1);

            ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName + " WHERE id = 1");
            while (rs.next()) {
                Assert.assertEquals(1, rs.getInt("id"));
                Assert.assertEquals("b", rs.getString("col1"));
                Assert.assertEquals("ab", rs.getString("col2"));
            }
        } finally {
            EnvironmentEdgeManager.reset();
        }
    }

    private void createTable(String tableName) throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl());
            Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE " + tableName
                + " (id INTEGER PRIMARY KEY, col1 VARCHAR, col2 VARCHAR) BLOOMFILTER = NONE, TTL = "
                + TTL_IN_DAYS * 24 * 60 * 60);
            conn.commit();
        }
    }

    private int getStoreFileCount(String tableName) throws Exception {
        TableName table = TableName.valueOf(tableName);
        int totalStoreFiles = 0;

        // Get all regions for the table
        List<HRegion> regions = getUtility().getHBaseCluster().getRegionServerThreads().stream()
            .flatMap(rs -> rs.getRegionServer().getRegions(table).stream())
            .collect(Collectors.toList());

        // Count StoreFiles in each region
        for (HRegion region : regions) {
            for (HStore store : region.getStores()) {
                totalStoreFiles += store.getStorefilesCount();
            }
        }
        return totalStoreFiles;
    }
}
