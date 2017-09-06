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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.SimpleRegionObserver;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.phoenix.end2end.BaseUniqueNamesOwnClusterIT;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PMetaData;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.phoenix.util.EnvironmentEdge;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.IndexScrutiny;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.Repeat;
import org.apache.phoenix.util.RunUntilFailure;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.google.common.collect.Maps;

@RunWith(RunUntilFailure.class)
public class PartialIndexRebuilderIT extends BaseUniqueNamesOwnClusterIT {
    private static final Random RAND = new Random(5);
    private static final int WAIT_AFTER_DISABLED = 5000;

    @BeforeClass
    public static void doSetup() throws Exception {
        Map<String, String> serverProps = Maps.newHashMapWithExpectedSize(10);
        serverProps.put(QueryServices.INDEX_FAILURE_HANDLING_REBUILD_ATTRIB, Boolean.TRUE.toString());
        serverProps.put(QueryServices.INDEX_FAILURE_HANDLING_REBUILD_INTERVAL_ATTRIB, "2000");
        serverProps.put(QueryServices.INDEX_REBUILD_DISABLE_TIMESTAMP_THRESHOLD, "120000"); // give up rebuilding after 2 minutes
        serverProps.put(QueryServices.INDEX_FAILURE_HANDLING_REBUILD_OVERLAP_FORWARD_TIME_ATTRIB, Long.toString(WAIT_AFTER_DISABLED));
        setUpTestDriver(new ReadOnlyProps(serverProps.entrySet().iterator()), ReadOnlyProps.EMPTY_PROPS);
    }

    private static void mutateRandomly(final String fullTableName, final int nThreads, final int nRows, final int nIndexValues, final int batchSize, final CountDownLatch doneSignal) {
        Runnable[] runnables = new Runnable[nThreads];
        for (int i = 0; i < nThreads; i++) {
           runnables[i] = new Runnable() {
    
               @Override
               public void run() {
                   try {
                       Connection conn = DriverManager.getConnection(getUrl());
                       for (int i = 0; i < 3000; i++) {
                           boolean isNull = RAND.nextBoolean();
                           int randInt = RAND.nextInt() % nIndexValues;
                           int pk = Math.abs(RAND.nextInt()) % nRows;
                           conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES (" + pk + ", 0, " + (isNull ? null : randInt) + ")");
                           if ((i % batchSize) == 0) {
                               conn.commit();
                           }
                       }
                       conn.commit();
                       for (int i = 0; i < 3000; i++) {
                           int pk = Math.abs(RAND.nextInt()) % nRows;
                           conn.createStatement().execute("DELETE FROM " + fullTableName + " WHERE k1= " + pk + " AND k2=0");
                           if (i % batchSize == 0) {
                               conn.commit();
                           }
                       }
                       conn.commit();
                       for (int i = 0; i < 3000; i++) {
                           int randInt = RAND.nextInt() % nIndexValues;
                           int pk = Math.abs(RAND.nextInt()) % nRows;
                           conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES (" + pk + ", 0, " + randInt + ")");
                           if ((i % batchSize) == 0) {
                               conn.commit();
                           }
                       }
                       conn.commit();
                   } catch (SQLException e) {
                       throw new RuntimeException(e);
                   } finally {
                       doneSignal.countDown();
                   }
               }
                
            };
        }
        for (int i = 0; i < nThreads; i++) {
            Thread t = new Thread(runnables[i]);
            t.start();
        }
    }
    @Test
    @Repeat(5)
    public void testConcurrentUpsertsWithRebuild() throws Throwable {
        int nThreads = 5;
        final int batchSize = 200;
        final int nRows = 51;
        final int nIndexValues = 23;
        final String schemaName = "";
        final String tableName = generateUniqueName();
        final String indexName = generateUniqueName();
        final String fullTableName = SchemaUtil.getTableName(schemaName, tableName);
        final String fullIndexName = SchemaUtil.getTableName(schemaName, indexName);
        Connection conn = DriverManager.getConnection(getUrl());
        HTableInterface metaTable = conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES);
        conn.createStatement().execute("CREATE TABLE " + fullTableName + "(k1 INTEGER NOT NULL, k2 INTEGER NOT NULL, v1 INTEGER, CONSTRAINT pk PRIMARY KEY (k1,k2)) STORE_NULLS=true, VERSIONS=1");
        conn.createStatement().execute("CREATE INDEX " + indexName + " ON " + fullTableName + "(v1)");
        
        final CountDownLatch doneSignal1 = new CountDownLatch(nThreads);
        mutateRandomly(fullTableName, nThreads, nRows, nIndexValues, batchSize, doneSignal1);
        assertTrue("Ran out of time", doneSignal1.await(120, TimeUnit.SECONDS));
        
        IndexUtil.updateIndexState(fullIndexName, EnvironmentEdgeManager.currentTimeMillis(), metaTable, PIndexState.DISABLE);
        do {
            final CountDownLatch doneSignal2 = new CountDownLatch(nThreads);
            mutateRandomly(fullTableName, nThreads, nRows, nIndexValues, batchSize, doneSignal2);
            assertTrue("Ran out of time", doneSignal2.await(500, TimeUnit.SECONDS));
        } while (!TestUtil.checkIndexState(conn, fullIndexName, PIndexState.ACTIVE, 0L));
        
        long actualRowCount = IndexScrutiny.scrutinizeIndex(conn, fullTableName, fullIndexName);
        assertEquals(nRows, actualRowCount);
    }

    private static boolean mutateRandomly(Connection conn, String fullTableName, int nRows) throws Exception {
        return mutateRandomly(conn, fullTableName, nRows, false, null);
    }
    
    private static boolean hasInactiveIndex(PMetaData metaCache, PTableKey key) throws TableNotFoundException {
        PTable table = metaCache.getTableRef(key).getTable();
        for (PTable index : table.getIndexes()) {
            if (index.getIndexState() == PIndexState.ACTIVE) {
                return true;
            }
        }
        return false;
    }
    
    private static boolean hasDisabledIndex(PMetaData metaCache, PTableKey key) throws TableNotFoundException {
        PTable table = metaCache.getTableRef(key).getTable();
        for (PTable index : table.getIndexes()) {
            if (index.getIndexState() == PIndexState.DISABLE) {
                return true;
            }
        }
        return false;
    }

    private static boolean mutateRandomly(Connection conn, String fullTableName, int nRows, boolean checkForInactive, String fullIndexName) throws SQLException, InterruptedException {
        PTableKey key = new PTableKey(null,fullTableName);
        PMetaData metaCache = conn.unwrap(PhoenixConnection.class).getMetaDataCache();
        boolean hasInactiveIndex = false;
        int batchSize = 200;
        if (checkForInactive) {
            batchSize = 3;
        }
        for (int i = 0; i < 10000; i++) {
            int pk = Math.abs(RAND.nextInt()) % nRows;
            int v1 = Math.abs(RAND.nextInt()) % nRows;
            int v2 = Math.abs(RAND.nextInt()) % nRows;
            conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES(" + pk + "," + v1 + "," + v2 + ")");
            if (i % batchSize == 0) {
                conn.commit();
                if (checkForInactive) {
                    if (hasInactiveIndex(metaCache, key)) {
                        checkForInactive = false;
                        hasInactiveIndex = true;
                        batchSize = 200;
                    }
                }
            }
        }
        conn.commit();
        for (int i = 0; i < 10000; i++) {
            int pk = Math.abs(RAND.nextInt()) % nRows;
            conn.createStatement().execute("DELETE FROM " + fullTableName + " WHERE k= " + pk);
            if (i % batchSize == 0) {
                conn.commit();
                if (checkForInactive) {
                    if (hasInactiveIndex(metaCache, key)) {
                        checkForInactive = false;
                        hasInactiveIndex = true;
                        batchSize = 200;
                    }
                }
            }
        }
        conn.commit();
        for (int i = 0; i < 10000; i++) {
            int pk = Math.abs(RAND.nextInt()) % nRows;
            int v1 = Math.abs(RAND.nextInt()) % nRows;
            int v2 = Math.abs(RAND.nextInt()) % nRows;
            conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES(" + pk + "," + v1 + "," + v2 + ")");
            if (i % batchSize == 0) {
                conn.commit();
                if (checkForInactive) {
                    if (hasInactiveIndex(metaCache, key)) {
                        checkForInactive = false;
                        hasInactiveIndex = true;
                        batchSize = 200;
                    }
                }
            }
        }
        conn.commit();
        return hasInactiveIndex;
    }
    
    @Test
    public void testCompactionDuringRebuild() throws Throwable {
        String schemaName = generateUniqueName();
        String tableName = generateUniqueName();
        String indexName1 = generateUniqueName();
        String indexName2 = generateUniqueName();
        final String fullTableName = SchemaUtil.getTableName(schemaName, tableName);
        String fullIndexName1 = SchemaUtil.getTableName(schemaName, indexName1);
        String fullIndexName2 = SchemaUtil.getTableName(schemaName, indexName2);
        final MyClock clock = new MyClock(1000);
        // Use our own clock to prevent race between partial rebuilder and compaction
        EnvironmentEdgeManager.injectEdge(clock);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute("CREATE TABLE " + fullTableName + "(k INTEGER PRIMARY KEY, v1 INTEGER, v2 INTEGER) COLUMN_ENCODED_BYTES = 0, STORE_NULLS=true, GUIDE_POSTS_WIDTH=1000");
            clock.time += 100;
            conn.createStatement().execute("CREATE INDEX " + indexName1 + " ON " + fullTableName + " (v1) INCLUDE (v2)");
            clock.time += 100;
            conn.createStatement().execute("CREATE INDEX " + indexName2 + " ON " + fullTableName + " (v2) INCLUDE (v1)");
            clock.time += 100;
            conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES(1, 2, 3)");
            conn.commit();
            clock.time += 100;
            long disableTS = EnvironmentEdgeManager.currentTimeMillis();
            HTableInterface metaTable = conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES);
            IndexUtil.updateIndexState(fullIndexName1, disableTS, metaTable, PIndexState.DISABLE);
            IndexUtil.updateIndexState(fullIndexName2, disableTS, metaTable, PIndexState.DISABLE);
            clock.time += 100;
            TestUtil.doMajorCompaction(conn, fullIndexName1);
            clock.time += 100;
            assertTrue(TestUtil.checkIndexState(conn, fullIndexName1, PIndexState.DISABLE, 0L));
            assertFalse(TestUtil.checkIndexState(conn, fullIndexName2, PIndexState.DISABLE, 0L));
            TestUtil.doMajorCompaction(conn, fullTableName);
            clock.time += 100;
            assertTrue(TestUtil.checkIndexState(conn, fullIndexName2, PIndexState.DISABLE, 0L));
        } finally {
            EnvironmentEdgeManager.injectEdge(null);
        }
    }

    @Test
    @Repeat(5)
    public void testDeleteAndUpsertAfterFailure() throws Throwable {
        final int nRows = 10;
        String schemaName = generateUniqueName();
        String tableName = generateUniqueName();
        String indexName = generateUniqueName();
        final String fullTableName = SchemaUtil.getTableName(schemaName, tableName);
        String fullIndexName = SchemaUtil.getTableName(schemaName, indexName);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute("CREATE TABLE " + fullTableName + "(k INTEGER PRIMARY KEY, v1 INTEGER, v2 INTEGER) COLUMN_ENCODED_BYTES = 0, STORE_NULLS=true");
            conn.createStatement().execute("CREATE INDEX " + indexName + " ON " + fullTableName + " (v1) INCLUDE (v2)");
            mutateRandomly(conn, fullTableName, nRows);
            long disableTS = EnvironmentEdgeManager.currentTimeMillis();
            HTableInterface metaTable = conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES);
            IndexUtil.updateIndexState(fullIndexName, disableTS, metaTable, PIndexState.DISABLE);
            mutateRandomly(conn, fullTableName, nRows);
            TestUtil.waitForIndexRebuild(conn, fullIndexName, PIndexState.ACTIVE);
            
            long actualRowCount = IndexScrutiny.scrutinizeIndex(conn, fullTableName, fullIndexName);
            assertEquals(nRows,actualRowCount);
       }
    }
    
    @Test
    public void testWriteWhileRebuilding() throws Throwable {
        final int nRows = 10;
        String schemaName = generateUniqueName();
        String tableName = generateUniqueName();
        String indexName = generateUniqueName();
        final String fullTableName = SchemaUtil.getTableName(schemaName, tableName);
        final String fullIndexName = SchemaUtil.getTableName(schemaName, indexName);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute("CREATE TABLE " + fullTableName + "(k INTEGER PRIMARY KEY, v1 INTEGER, v2 INTEGER) COLUMN_ENCODED_BYTES = 0, STORE_NULLS=true");
            conn.createStatement().execute("CREATE INDEX " + indexName + " ON " + fullTableName + " (v1) INCLUDE (v2)");
            mutateRandomly(conn, fullTableName, nRows);
            HTableInterface metaTable = conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES);
            final boolean[] hasInactiveIndex = new boolean[1];
            final CountDownLatch doneSignal = new CountDownLatch(1);
            Runnable r = new Runnable() {

                @Override
                public void run() {
                    try (Connection conn = DriverManager.getConnection(getUrl())) {
                        hasInactiveIndex[0] = mutateRandomly(conn, fullTableName, nRows, true, fullIndexName);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    } finally {
                        doneSignal.countDown();
                    }
                }
                
            };
            Thread t = new Thread(r);
            t.setDaemon(true);
            t.start();
            long disableTS = EnvironmentEdgeManager.currentTimeMillis();
            IndexUtil.updateIndexState(fullIndexName, disableTS, metaTable, PIndexState.DISABLE);
            TestUtil.waitForIndexRebuild(conn, fullIndexName, PIndexState.ACTIVE);
            doneSignal.await(60, TimeUnit.SECONDS);
            assertTrue(hasInactiveIndex[0]);
            
            TestUtil.dumpIndexStatus(conn, fullIndexName);

            long actualRowCount = IndexScrutiny.scrutinizeIndex(conn, fullTableName, fullIndexName);
            assertEquals(nRows,actualRowCount);
            
       }
    }

    @Test
    public void testMultiVersionsAfterFailure() throws Throwable {
        String schemaName = generateUniqueName();
        String tableName = generateUniqueName();
        String indexName = generateUniqueName();
        String fullTableName = SchemaUtil.getTableName(schemaName, tableName);
        String fullIndexName = SchemaUtil.getTableName(schemaName, indexName);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute("CREATE TABLE " + fullTableName + "(k VARCHAR PRIMARY KEY, v VARCHAR) COLUMN_ENCODED_BYTES = 0, STORE_NULLS=true");
            conn.createStatement().execute("CREATE INDEX " + indexName + " ON " + fullTableName + " (v)");
            conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES('a','bb')");
            conn.commit();
            long disableTS = EnvironmentEdgeManager.currentTimeMillis();
            HTableInterface metaTable = conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES);
            IndexUtil.updateIndexState(fullIndexName, disableTS, metaTable, PIndexState.DISABLE);
            conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES('a','ccc')");
            conn.commit();
            conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES('a','dddd')");
            conn.commit();
            conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES('a','eeeee')");
            conn.commit();
            TestUtil.waitForIndexRebuild(conn, fullIndexName, PIndexState.ACTIVE);

            IndexScrutiny.scrutinizeIndex(conn, fullTableName, fullIndexName);
        }
    }
    
    @Test
    public void testUpsertNullAfterFailure() throws Throwable {
        String schemaName = generateUniqueName();
        String tableName = generateUniqueName();
        String indexName = generateUniqueName();
        String fullTableName = SchemaUtil.getTableName(schemaName, tableName);
        String fullIndexName = SchemaUtil.getTableName(schemaName, indexName);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute("CREATE TABLE " + fullTableName + "(k VARCHAR PRIMARY KEY, v VARCHAR) COLUMN_ENCODED_BYTES = 0, STORE_NULLS=true");
            conn.createStatement().execute("CREATE INDEX " + indexName + " ON " + fullTableName + " (v)");
            conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES('a','a')");
            conn.commit();
            long disableTS = EnvironmentEdgeManager.currentTimeMillis();
            HTableInterface metaTable = conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES);
            IndexUtil.updateIndexState(fullIndexName, disableTS, metaTable, PIndexState.DISABLE);
            conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES('a',null)");
            conn.commit();
            conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES('a','bb')");
            conn.commit();
            conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES('a','ccc')");
            conn.commit();
            TestUtil.waitForIndexRebuild(conn, fullIndexName, PIndexState.ACTIVE);

            IndexScrutiny.scrutinizeIndex(conn, fullTableName, fullIndexName);
        }
    }
    
    @Test
    public void testUpsertNullTwiceAfterFailure() throws Throwable {
        String schemaName = generateUniqueName();
        String tableName = generateUniqueName();
        String indexName = generateUniqueName();
        String fullTableName = SchemaUtil.getTableName(schemaName, tableName);
        String fullIndexName = SchemaUtil.getTableName(schemaName, indexName);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute("CREATE TABLE " + fullTableName + "(k VARCHAR PRIMARY KEY, v VARCHAR) COLUMN_ENCODED_BYTES = 0, STORE_NULLS=true");
            conn.createStatement().execute("CREATE INDEX " + indexName + " ON " + fullTableName + " (v)");
            conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES('a',null)");
            conn.commit();
            long disableTS = EnvironmentEdgeManager.currentTimeMillis();
            HTableInterface metaTable = conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES);
            IndexUtil.updateIndexState(fullIndexName, disableTS, metaTable, PIndexState.DISABLE);
            conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES('a','bb')");
            conn.commit();
            conn.createStatement().execute("DELETE FROM " + fullTableName);
            conn.commit();
            conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES('a',null)");
            conn.commit();
            TestUtil.waitForIndexRebuild(conn, fullIndexName, PIndexState.ACTIVE);

            IndexScrutiny.scrutinizeIndex(conn, fullTableName, fullIndexName);
        }
    }
    
    @Test
    public void testDeleteAfterFailure() throws Throwable {
        String schemaName = generateUniqueName();
        String tableName = generateUniqueName();
        String indexName = generateUniqueName();
        String fullTableName = SchemaUtil.getTableName(schemaName, tableName);
        String fullIndexName = SchemaUtil.getTableName(schemaName, indexName);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute("CREATE TABLE " + fullTableName + "(k VARCHAR PRIMARY KEY, v VARCHAR) COLUMN_ENCODED_BYTES = 0, STORE_NULLS=true");
            conn.createStatement().execute("CREATE INDEX " + indexName + " ON " + fullTableName + " (v)");
            conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES('a',null)");
            conn.commit();
            long disableTS = EnvironmentEdgeManager.currentTimeMillis();
            HTableInterface metaTable = conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES);
            IndexUtil.updateIndexState(fullIndexName, disableTS, metaTable, PIndexState.DISABLE);
            conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES('a','b')");
            conn.commit();
            conn.createStatement().execute("DELETE FROM " + fullTableName);
            conn.commit();
            TestUtil.waitForIndexRebuild(conn, fullIndexName, PIndexState.ACTIVE);

            IndexScrutiny.scrutinizeIndex(conn, fullTableName, fullIndexName);
       }
    }
    
    @Test
    public void testDeleteBeforeFailure() throws Throwable {
        String schemaName = generateUniqueName();
        String tableName = generateUniqueName();
        String indexName = generateUniqueName();
        String fullTableName = SchemaUtil.getTableName(schemaName, tableName);
        String fullIndexName = SchemaUtil.getTableName(schemaName, indexName);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute("CREATE TABLE " + fullTableName + "(k VARCHAR PRIMARY KEY, v VARCHAR) COLUMN_ENCODED_BYTES = 0, STORE_NULLS=true");
            conn.createStatement().execute("CREATE INDEX " + indexName + " ON " + fullTableName + " (v)");
            conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES('a',null)");
            conn.commit();
            conn.createStatement().execute("DELETE FROM " + fullTableName);
            conn.commit();
            long disableTS = EnvironmentEdgeManager.currentTimeMillis();
            HTableInterface metaTable = conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES);
            IndexUtil.updateIndexState(fullIndexName, disableTS, metaTable, PIndexState.DISABLE);
            conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES('a','b')");
            conn.commit();
            TestUtil.waitForIndexRebuild(conn, fullIndexName, PIndexState.ACTIVE);

            IndexScrutiny.scrutinizeIndex(conn, fullTableName, fullIndexName);
        }
    }
    
    private static class MyClock extends EnvironmentEdge {
        public volatile long time;
        
        public MyClock (long time) {
            this.time = time;
        }
        
        @Override
        public long currentTime() {
            return time;
        }
    }
    
    @Test
    public void testMultiValuesAtSameTS() throws Throwable {
        String schemaName = generateUniqueName();
        String tableName = generateUniqueName();
        String indexName = generateUniqueName();
        final String fullTableName = SchemaUtil.getTableName(schemaName, tableName);
        final String fullIndexName = SchemaUtil.getTableName(schemaName, indexName);
        final MyClock clock = new MyClock(1000);
        EnvironmentEdgeManager.injectEdge(clock);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute("CREATE TABLE " + fullTableName + "(k VARCHAR PRIMARY KEY, v VARCHAR) COLUMN_ENCODED_BYTES = 0, STORE_NULLS=true");
            clock.time += 100;
            conn.createStatement().execute("CREATE INDEX " + indexName + " ON " + fullTableName + " (v)");
            clock.time += 100;
            conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES('a','a')");
            conn.commit();
            clock.time += 100;
            HTableInterface metaTable = conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES);
            IndexUtil.updateIndexState(fullIndexName, clock.currentTime(), metaTable, PIndexState.DISABLE);
            conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES('a','bb')");
            conn.commit();
            conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES('a','ccc')");
            conn.commit();
            clock.time += 1000;
            advanceClockUntilPartialRebuildStarts(fullIndexName, clock);
            TestUtil.waitForIndexRebuild(conn, fullIndexName, PIndexState.ACTIVE);
            clock.time += 100;
            IndexScrutiny.scrutinizeIndex(conn, fullTableName, fullIndexName);
        } finally {
            EnvironmentEdgeManager.injectEdge(null);
        }
    }
    
    @Test
    public void testSeparateTimeBatchesRequired() throws Throwable {
        String schemaName = generateUniqueName();
        String tableName = generateUniqueName();
        String indexName = generateUniqueName();
        final String fullTableName = SchemaUtil.getTableName(schemaName, tableName);
        final String fullIndexName = SchemaUtil.getTableName(schemaName, indexName);
        PTableKey key = new PTableKey(null,fullTableName);
        final MyClock clock = new MyClock(1000);
        EnvironmentEdgeManager.injectEdge(clock);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            PMetaData metaCache = conn.unwrap(PhoenixConnection.class).getMetaDataCache();
            conn.createStatement().execute("CREATE TABLE " + fullTableName + "(k VARCHAR PRIMARY KEY, v1 VARCHAR, v2 VARCHAR) COLUMN_ENCODED_BYTES = 0, STORE_NULLS=true");
            clock.time += 100;
            conn.createStatement().execute("CREATE INDEX " + indexName + " ON " + fullTableName + " (v1, v2)");
            clock.time += 100;
            conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES('a','a','0')");
            conn.commit();
            clock.time += 100;
            HTableInterface metaTable = conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES);
            IndexUtil.updateIndexState(fullIndexName, 0L, metaTable, PIndexState.DISABLE);
            clock.time += 100;
            long disableTime = clock.currentTime();
            conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES('b','bb', '11')");
            conn.commit();
            clock.time += 100;
            assertTrue(hasDisabledIndex(metaCache, key));
            assertEquals(2,TestUtil.getRowCount(conn, fullTableName));
            assertEquals(1,TestUtil.getRowCount(conn, fullIndexName));
            conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES('a','ccc','0')");
            conn.commit();
            clock.time += 100;
            conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES('a','a')");
            conn.commit();
            clock.time += 100;
            IndexUtil.updateIndexState(fullIndexName, disableTime, metaTable, PIndexState.DISABLE);
            clock.time += 100;
            advanceClockUntilPartialRebuildStarts(fullIndexName, clock);
            TestUtil.waitForIndexRebuild(conn, fullIndexName, PIndexState.ACTIVE);
            clock.time += 100;
            IndexScrutiny.scrutinizeIndex(conn, fullTableName, fullIndexName);
        } finally {
            EnvironmentEdgeManager.injectEdge(null);
        }
    }
    
    @Test
    public void testMultiValuesWhenDisableAndInactive() throws Throwable {
        String schemaName = generateUniqueName();
        String tableName = generateUniqueName();
        String indexName = generateUniqueName();
        final String fullTableName = SchemaUtil.getTableName(schemaName, tableName);
        final String fullIndexName = SchemaUtil.getTableName(schemaName, indexName);
        PTableKey key = new PTableKey(null,fullTableName);
        final MyClock clock = new MyClock(1000);
        EnvironmentEdgeManager.injectEdge(clock);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            PMetaData metaCache = conn.unwrap(PhoenixConnection.class).getMetaDataCache();
            conn.createStatement().execute("CREATE TABLE " + fullTableName + "(k VARCHAR PRIMARY KEY, v1 VARCHAR, v2 VARCHAR, v3 VARCHAR) COLUMN_ENCODED_BYTES = 0, STORE_NULLS=true");
            clock.time += 100;
            conn.createStatement().execute("CREATE INDEX " + indexName + " ON " + fullTableName + " (v1, v2) INCLUDE (v3)");
            clock.time += 100;
            conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES('a','a','0','x')");
            conn.commit();
            clock.time += 100;
            try (HTableInterface metaTable = conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES)) {
                // By using an INDEX_DISABLE_TIMESTAMP of 0, we prevent the partial index rebuilder from triggering
                IndexUtil.updateIndexState(fullIndexName, 0L, metaTable, PIndexState.DISABLE);
                clock.time += 100;
                long disableTime = clock.currentTime();
                // Set some values while index disabled
                conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES('b','bb', '11','yy')");
                conn.commit();
                clock.time += 100;
                assertTrue(hasDisabledIndex(metaCache, key));
                conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES('a','ccc','222','zzz')");
                conn.commit();
                clock.time += 100;
                conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES('a','dddd','3333','zzzz')");
                conn.commit();
                clock.time += 100;
                // Will cause partial index rebuilder to be triggered
                IndexUtil.updateIndexState(fullIndexName, disableTime, metaTable, PIndexState.DISABLE);
            }
            final CountDownLatch doneSignal = new CountDownLatch(1);
            advanceClockUntilPartialRebuildStarts(fullIndexName, clock, doneSignal);
            doneSignal.await(30, TimeUnit.SECONDS);
            // Set some values while index is in INACTIVE state
            clock.time += 100;
            conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES('a','eeeee','44444','zzzzz')");
            conn.commit();
            clock.time += 100;
            conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES('a','fffff','55555','zzzzzz')");
            conn.commit();
            clock.time += WAIT_AFTER_DISABLED;
            // Enough time has passed, so rebuild will start now
            TestUtil.waitForIndexRebuild(conn, fullIndexName, PIndexState.ACTIVE);
            clock.time += 100;
            IndexScrutiny.scrutinizeIndex(conn, fullTableName, fullIndexName);
        } finally {
            EnvironmentEdgeManager.injectEdge(null);
        }
    }

    @Test
    public void testDeleteAndUpsertValuesAtSameTS1() throws Throwable {
        String schemaName = generateUniqueName();
        String tableName = generateUniqueName();
        String indexName = generateUniqueName();
        final String fullTableName = SchemaUtil.getTableName(schemaName, tableName);
        final String fullIndexName = SchemaUtil.getTableName(schemaName, indexName);
        final MyClock clock = new MyClock(1000);
        EnvironmentEdgeManager.injectEdge(clock);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute("CREATE TABLE " + fullTableName + "(k VARCHAR PRIMARY KEY, v VARCHAR) COLUMN_ENCODED_BYTES = 0, STORE_NULLS=true");
            clock.time += 100;
            conn.createStatement().execute("CREATE INDEX " + indexName + " ON " + fullTableName + " (v)");
            clock.time += 100;
            conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES('a','a')");
            conn.commit();
            clock.time += 100;
            HTableInterface metaTable = conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES);
            IndexUtil.updateIndexState(fullIndexName, clock.currentTime(), metaTable, PIndexState.DISABLE);
            conn.createStatement().execute("DELETE FROM " + fullTableName + " WHERE k='a'");
            conn.commit();
            conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES('a','ccc')");
            conn.commit();
            clock.time += 1000;
            advanceClockUntilPartialRebuildStarts(fullIndexName, clock);
            TestUtil.waitForIndexRebuild(conn, fullIndexName, PIndexState.ACTIVE);
            clock.time += 100;
            IndexScrutiny.scrutinizeIndex(conn, fullTableName, fullIndexName);
        } finally {
            EnvironmentEdgeManager.injectEdge(null);
        }
    }
    
    @Test
    public void testDeleteAndUpsertValuesAtSameTS2() throws Throwable {
        String schemaName = generateUniqueName();
        String tableName = generateUniqueName();
        String indexName = generateUniqueName();
        final String fullTableName = SchemaUtil.getTableName(schemaName, tableName);
        final String fullIndexName = SchemaUtil.getTableName(schemaName, indexName);
        final MyClock clock = new MyClock(1000);
        EnvironmentEdgeManager.injectEdge(clock);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute("CREATE TABLE " + fullTableName + "(k VARCHAR PRIMARY KEY, v VARCHAR) COLUMN_ENCODED_BYTES = 0, STORE_NULLS=true");
            clock.time += 100;
            conn.createStatement().execute("CREATE INDEX " + indexName + " ON " + fullTableName + " (v)");
            clock.time += 100;
            conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES('a','a')");
            conn.commit();
            clock.time += 100;
            HTableInterface metaTable = conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES);
            IndexUtil.updateIndexState(fullIndexName, clock.currentTime(), metaTable, PIndexState.DISABLE);
            conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES('a','ccc')");
            conn.commit();
            conn.createStatement().execute("DELETE FROM " + fullTableName + " WHERE k='a'");
            conn.commit();
            clock.time += 1000;
            advanceClockUntilPartialRebuildStarts(fullIndexName, clock);
            TestUtil.waitForIndexRebuild(conn, fullIndexName, PIndexState.ACTIVE);
            clock.time += 100;
            IndexScrutiny.scrutinizeIndex(conn, fullTableName, fullIndexName);
        } finally {
            EnvironmentEdgeManager.injectEdge(null);
        }
    }

    @Test
    public void testRegionsOnlineCheck() throws Throwable {
        String schemaName = generateUniqueName();
        String tableName = generateUniqueName();
        final String fullTableName = SchemaUtil.getTableName(schemaName, tableName);
        PTableKey key = new PTableKey(null,fullTableName);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            PMetaData metaCache = conn.unwrap(PhoenixConnection.class).getMetaDataCache();
            conn.createStatement().execute("CREATE TABLE " + fullTableName + "(k VARCHAR PRIMARY KEY)");
            conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES('a')");
            conn.commit();
            Configuration conf = conn.unwrap(PhoenixConnection.class).getQueryServices().getConfiguration();
            PTable table = metaCache.getTableRef(key).getTable();
            assertTrue(MetaDataUtil.tableRegionsOnline(conf, table));
            try (HBaseAdmin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
                admin.disableTable(fullTableName);
                assertFalse(MetaDataUtil.tableRegionsOnline(conf, table));
                admin.enableTable(fullTableName);
            }
            assertTrue(MetaDataUtil.tableRegionsOnline(conf, table));
        }
    }
    
    private static void advanceClockUntilPartialRebuildStarts(final String fullIndexName, final MyClock clock) throws InterruptedException {
        final CountDownLatch doneSignal = new CountDownLatch(1);
        advanceClockUntilPartialRebuildStarts(fullIndexName, clock, doneSignal);
        clock.time += WAIT_AFTER_DISABLED + 1000;
        doneSignal.await(30, TimeUnit.SECONDS);
    }
    
    private static void advanceClockUntilPartialRebuildStarts(final String fullIndexName, final MyClock clock, final CountDownLatch doneSignal) {
        Runnable r = new Runnable() {
            @Override
            public void run() {
                try (Connection conn = DriverManager.getConnection(getUrl())) {
                  int nTries = 10;
                    while (--nTries >0 && !TestUtil.checkIndexState(conn, fullIndexName, PIndexState.INACTIVE, null)) {
                        Thread.sleep(1000);
                        clock.time += 1000;
                    }
                    doneSignal.countDown();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        };
        Thread t = new Thread(r);
        t.setDaemon(true);
        t.start();
    }
    
    public static class DelayingRegionObserver extends SimpleRegionObserver {
        @Override
        public void preBatchMutate(ObserverContext<RegionCoprocessorEnvironment> c, MiniBatchOperationInProgress<Mutation> miniBatchOp) throws HBaseIOException {
            try {
                Thread.sleep(Math.abs(RAND.nextInt()) % 10);
            } catch (InterruptedException e) {
            }
            
        }
    }
    
}
