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
import static org.junit.Assert.fail;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.SimpleRegionObserver;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.phoenix.coprocessor.MetaDataRegionObserver;
import org.apache.phoenix.coprocessor.MetaDataRegionObserver.BuildIndexScheduleTask;
import org.apache.phoenix.end2end.BaseUniqueNamesOwnClusterIT;
import org.apache.phoenix.execute.CommitException;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.jdbc.PhoenixStatement;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

@SuppressWarnings("deprecation")
@RunWith(RunUntilFailure.class)
public class PartialIndexRebuilderIT extends BaseUniqueNamesOwnClusterIT {
    private static final Logger LOG = LoggerFactory.getLogger(PartialIndexRebuilderIT.class);
    private static final Random RAND = new Random(5);
    private static final int WAIT_AFTER_DISABLED = 5000;
    private static final long REBUILD_PERIOD = 50000;
    private static final long REBUILD_INTERVAL = 2000;
    private static RegionCoprocessorEnvironment indexRebuildTaskRegionEnvironment;

    
    @BeforeClass
    public static void doSetup() throws Exception {
        Map<String, String> serverProps = Maps.newHashMapWithExpectedSize(10);
        serverProps.put(QueryServices.INDEX_FAILURE_HANDLING_REBUILD_ATTRIB, Boolean.TRUE.toString());
        serverProps.put(QueryServices.INDEX_FAILURE_HANDLING_REBUILD_INTERVAL_ATTRIB, Long.toString(REBUILD_INTERVAL));
        serverProps.put(QueryServices.INDEX_REBUILD_DISABLE_TIMESTAMP_THRESHOLD, "50000000");
        serverProps.put(QueryServices.INDEX_FAILURE_HANDLING_REBUILD_PERIOD, Long.toString(REBUILD_PERIOD)); // batch at 50 seconds
        serverProps.put(QueryServices.INDEX_FAILURE_HANDLING_REBUILD_OVERLAP_FORWARD_TIME_ATTRIB, Long.toString(WAIT_AFTER_DISABLED));
        setUpTestDriver(new ReadOnlyProps(serverProps.entrySet().iterator()), ReadOnlyProps.EMPTY_PROPS);
        indexRebuildTaskRegionEnvironment =
                (RegionCoprocessorEnvironment) getUtility()
                        .getRSForFirstRegionInTable(
                            PhoenixDatabaseMetaData.SYSTEM_CATALOG_HBASE_TABLE_NAME)
                        .getOnlineRegions(PhoenixDatabaseMetaData.SYSTEM_CATALOG_HBASE_TABLE_NAME)
                        .get(0).getCoprocessorHost()
                        .findCoprocessorEnvironment(MetaDataRegionObserver.class.getName());
        MetaDataRegionObserver.initRebuildIndexConnectionProps(
            indexRebuildTaskRegionEnvironment.getConfiguration());
    }

    private static void runIndexRebuilder(String table) throws InterruptedException, SQLException {
        runIndexRebuilder(Collections.<String>singletonList(table));
    }
    
    private static void runIndexRebuilder(List<String> tables) throws InterruptedException, SQLException {
        BuildIndexScheduleTask task =
                new MetaDataRegionObserver.BuildIndexScheduleTask(
                        indexRebuildTaskRegionEnvironment, tables);
        task.run();
    }
    
    private static void runIndexRebuilderAsync(final int interval, final boolean[] cancel, String table) {
        runIndexRebuilderAsync(interval, cancel, Collections.<String>singletonList(table));
    }
    private static void runIndexRebuilderAsync(final int interval, final boolean[] cancel, final List<String> tables) {
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                while (!cancel[0]) {
                    try {
                        runIndexRebuilder(tables);
                        Thread.sleep(interval);
                    } catch (InterruptedException e) {
                        Thread.interrupted();
                        throw new RuntimeException(e);
                    } catch (SQLException e) {
                        LOG.error(e.getMessage(),e);
                    }
                }
            }
        });
        thread.setDaemon(true);
        thread.start();
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
        Table metaTable = conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES);
        conn.createStatement().execute("CREATE TABLE " + fullTableName + "(k1 INTEGER NOT NULL, k2 INTEGER NOT NULL, v1 INTEGER, CONSTRAINT pk PRIMARY KEY (k1,k2)) STORE_NULLS=true, VERSIONS=1");
        conn.createStatement().execute("CREATE INDEX " + indexName + " ON " + fullTableName + "(v1)");
        
        final CountDownLatch doneSignal1 = new CountDownLatch(nThreads);
        mutateRandomly(fullTableName, nThreads, nRows, nIndexValues, batchSize, doneSignal1);
        assertTrue("Ran out of time", doneSignal1.await(120, TimeUnit.SECONDS));
        
        IndexUtil.updateIndexState(fullIndexName, EnvironmentEdgeManager.currentTimeMillis(), metaTable, PIndexState.DISABLE);
        boolean[] cancel = new boolean[1];
        try {
            do {
                final CountDownLatch doneSignal2 = new CountDownLatch(nThreads);
                runIndexRebuilderAsync(500,cancel,fullTableName);
                mutateRandomly(fullTableName, nThreads, nRows, nIndexValues, batchSize, doneSignal2);
                assertTrue("Ran out of time", doneSignal2.await(500, TimeUnit.SECONDS));
            } while (!TestUtil.checkIndexState(conn, fullIndexName, PIndexState.ACTIVE, 0L));
        } finally {
            cancel[0] = true;
        }
        long actualRowCount = IndexScrutiny.scrutinizeIndex(conn, fullTableName, fullIndexName);
        assertEquals(nRows, actualRowCount);
    }

    private static boolean mutateRandomly(Connection conn, String fullTableName, int nRows) throws Exception {
        return mutateRandomly(conn, fullTableName, nRows, false, null);
    }
    
    private static boolean hasInactiveIndex(PMetaData metaCache, PTableKey key) throws TableNotFoundException {
        PTable table = metaCache.getTableRef(key).getTable();
        for (PTable index : table.getIndexes()) {
            if (index.getIndexState() == PIndexState.INACTIVE) {
                return true;
            }
        }
        return false;
    }

    private static boolean hasDisabledIndex(PMetaData metaCache, PTableKey key) throws TableNotFoundException {
        return hasIndexWithState(metaCache, key, PIndexState.DISABLE);
    }

    private static boolean hasIndexWithState(PMetaData metaCache, PTableKey key, PIndexState expectedState) throws TableNotFoundException {
        PTable table = metaCache.getTableRef(key).getTable();
        for (PTable index : table.getIndexes()) {
            if (index.getIndexState() == expectedState) {
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
            Table metaTable = conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES);
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
            Table metaTable = conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES);
            IndexUtil.updateIndexState(fullIndexName, disableTS, metaTable, PIndexState.DISABLE);
            boolean[] cancel = new boolean[1];
            try {
                runIndexRebuilderAsync(500,cancel,fullTableName);
                mutateRandomly(conn, fullTableName, nRows);
                TestUtil.waitForIndexRebuild(conn, fullIndexName, PIndexState.ACTIVE);
            } finally {
                cancel[0] = true;
            }
            
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
            Table metaTable = conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES);
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
            boolean[] cancel = new boolean[1];
            try {
                runIndexRebuilderAsync(500,cancel,fullTableName);
                TestUtil.waitForIndexRebuild(conn, fullIndexName, PIndexState.ACTIVE);
                doneSignal.await(60, TimeUnit.SECONDS);
            } finally {
                cancel[0] = true;
            }
            assertTrue(hasInactiveIndex[0]);
            
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
            Table metaTable = conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES);
            IndexUtil.updateIndexState(fullIndexName, disableTS, metaTable, PIndexState.DISABLE);
            conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES('a','ccc')");
            conn.commit();
            conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES('a','dddd')");
            conn.commit();
            conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES('a','eeeee')");
            conn.commit();
            runIndexRebuilder(fullTableName);
            Thread.sleep(WAIT_AFTER_DISABLED);
            runIndexRebuilder(fullTableName);
            assertTrue(TestUtil.checkIndexState(conn, fullIndexName, PIndexState.ACTIVE, 0L));

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
            Table metaTable = conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES);
            IndexUtil.updateIndexState(fullIndexName, disableTS, metaTable, PIndexState.DISABLE);
            conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES('a',null)");
            conn.commit();
            conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES('a','bb')");
            conn.commit();
            conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES('a','ccc')");
            conn.commit();
            runIndexRebuilder(fullTableName);
            Thread.sleep(WAIT_AFTER_DISABLED);
            runIndexRebuilder(fullTableName);
            assertTrue(TestUtil.checkIndexState(conn, fullIndexName, PIndexState.ACTIVE, 0L));

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
            Table metaTable = conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES);
            IndexUtil.updateIndexState(fullIndexName, disableTS, metaTable, PIndexState.DISABLE);
            conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES('a','bb')");
            conn.commit();
            conn.createStatement().execute("DELETE FROM " + fullTableName);
            conn.commit();
            conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES('a',null)");
            conn.commit();
            runIndexRebuilder(fullTableName);
            Thread.sleep(WAIT_AFTER_DISABLED);
            runIndexRebuilder(fullTableName);
            assertTrue(TestUtil.checkIndexState(conn, fullIndexName, PIndexState.ACTIVE, 0L));

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
            Table metaTable = conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES);
            IndexUtil.updateIndexState(fullIndexName, disableTS, metaTable, PIndexState.DISABLE);
            conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES('a','b')");
            conn.commit();
            conn.createStatement().execute("DELETE FROM " + fullTableName);
            conn.commit();
            runIndexRebuilder(fullTableName);
            Thread.sleep(WAIT_AFTER_DISABLED);
            runIndexRebuilder(fullTableName);
            assertTrue(TestUtil.checkIndexState(conn, fullIndexName, PIndexState.ACTIVE, 0L));

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
            Table metaTable = conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES);
            IndexUtil.updateIndexState(fullIndexName, disableTS, metaTable, PIndexState.DISABLE);
            conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES('a','b')");
            conn.commit();
            runIndexRebuilder(fullTableName);
            Thread.sleep(WAIT_AFTER_DISABLED);
            runIndexRebuilder(fullTableName);
            assertTrue(TestUtil.checkIndexState(conn, fullIndexName, PIndexState.ACTIVE, 0L));

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
    
    private static void waitForIndexState(Connection conn, String fullTableName, String fullIndexName, PIndexState expectedIndexState) throws InterruptedException, SQLException {
        int nRetries = 2;
        PIndexState actualIndexState = null;
        do {
            runIndexRebuilder(fullTableName);
            if ((actualIndexState = TestUtil.getIndexState(conn, fullIndexName)) == expectedIndexState) {
                return;
            }
            Thread.sleep(1000);
        } while (--nRetries > 0);
        fail("Expected index state of " + expectedIndexState + ", but was " + actualIndexState);
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
            Table metaTable = conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES);
            IndexUtil.updateIndexState(fullIndexName, clock.currentTime(), metaTable, PIndexState.DISABLE);
            conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES('a','bb')");
            conn.commit();
            conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES('a','ccc')");
            conn.commit();
            clock.time += 1000;
            waitForIndexState(conn, fullTableName, fullIndexName, PIndexState.INACTIVE);
            clock.time += WAIT_AFTER_DISABLED;
            runIndexRebuilder(fullTableName);
            assertTrue(TestUtil.checkIndexState(conn, fullIndexName, PIndexState.ACTIVE, 0L));
            IndexScrutiny.scrutinizeIndex(conn, fullTableName, fullIndexName);
        } finally {
            EnvironmentEdgeManager.injectEdge(null);
        }
    }
    
    @Test
    public void testTimeBatchesInCoprocessorRequired() throws Throwable {
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
            Table metaTable = conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES);
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
            waitForIndexState(conn, fullTableName, fullIndexName, PIndexState.INACTIVE);
            clock.time += WAIT_AFTER_DISABLED;
            runIndexRebuilder(fullTableName);
            assertTrue(TestUtil.checkIndexState(conn, fullIndexName, PIndexState.ACTIVE, 0L));
            IndexScrutiny.scrutinizeIndex(conn, fullTableName, fullIndexName);
        } finally {
            EnvironmentEdgeManager.injectEdge(null);
        }
    }
    
    @Test
    public void testBatchingDuringRebuild() throws Throwable {
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
            Table metaTable = conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES);
            long disableTime = clock.currentTime();
            IndexUtil.updateIndexState(fullIndexName, disableTime, metaTable, PIndexState.DISABLE);
            clock.time += 100;
            conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES('bb','bb', '11')");
            conn.commit();
            clock.time += REBUILD_PERIOD;
            assertTrue(hasDisabledIndex(metaCache, key));
            assertEquals(2,TestUtil.getRowCount(conn, fullTableName));
            assertEquals(1,TestUtil.getRowCount(conn, fullIndexName));
            conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES('ccc','ccc','222')");
            conn.commit();
            assertEquals(3,TestUtil.getRowCount(conn, fullTableName));
            assertEquals(1,TestUtil.getRowCount(conn, fullIndexName));
            clock.time += 100;
            
            waitForIndexState(conn, fullTableName, fullIndexName, PIndexState.INACTIVE);
            clock.time += WAIT_AFTER_DISABLED;
            runIndexRebuilder(fullTableName);
            assertEquals(2,TestUtil.getRowCount(conn, fullIndexName));
            
            clock.time += REBUILD_PERIOD;
            runIndexRebuilder(fullTableName);
            // Verify that other batches were processed
            assertTrue(TestUtil.checkIndexState(conn, fullIndexName, PIndexState.ACTIVE, 0L));
            IndexScrutiny.scrutinizeIndex(conn, fullTableName, fullIndexName);
        } finally {
            EnvironmentEdgeManager.injectEdge(null);
        }
    }
    
    @Test
    public void testUpperBoundSetOnRebuild() throws Throwable {
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
            Table metaTable = conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES);
            IndexUtil.updateIndexState(fullIndexName, 0L, metaTable, PIndexState.DISABLE);
            clock.time += 100;
            long disableTime = clock.currentTime();
            conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES('a','a', '0')");
            conn.commit();
            // Set clock forward in time past the "overlap" amount we wait for index maintenance to kick in
            clock.time += 2 * WAIT_AFTER_DISABLED;
            assertTrue(hasDisabledIndex(metaCache, key));
            assertEquals(1,TestUtil.getRowCount(conn, fullTableName));
            assertEquals(0,TestUtil.getRowCount(conn, fullIndexName));
            conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES('bb','bb','11')");
            conn.commit();
            assertEquals(2,TestUtil.getRowCount(conn, fullTableName));
            assertEquals(0,TestUtil.getRowCount(conn, fullIndexName));
            // Set clock back in time and start rebuild
            clock.time = disableTime + 100;
            IndexUtil.updateIndexState(fullIndexName, disableTime, metaTable, PIndexState.DISABLE);
            waitForIndexState(conn, fullTableName, fullIndexName, PIndexState.INACTIVE);
            clock.time += WAIT_AFTER_DISABLED;
            runIndexRebuilder(fullTableName);
            assertTrue(TestUtil.checkIndexState(conn, fullIndexName, PIndexState.ACTIVE, 0L));
            assertEquals(2,TestUtil.getRowCount(conn, fullTableName));
            // If an upper bound was set on the rebuilder, we should only have found one row
            assertEquals(1,TestUtil.getRowCount(conn, fullIndexName));
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
            try (Table metaTable = conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES)) {
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
            runIndexRebuilder(fullTableName);
            assertTrue(TestUtil.checkIndexState(conn, fullIndexName, PIndexState.INACTIVE, null));

            // Set some values while index is in INACTIVE state
            clock.time += 100;
            conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES('a','eeeee','44444','zzzzz')");
            conn.commit();
            clock.time += 100;
            conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES('a','fffff','55555','zzzzzz')");
            conn.commit();
            clock.time += WAIT_AFTER_DISABLED;
            // Enough time has passed, so rebuild will start now
            runIndexRebuilder(fullTableName);
            assertTrue(TestUtil.checkIndexState(conn, fullIndexName, PIndexState.ACTIVE, 0L));
            IndexScrutiny.scrutinizeIndex(conn, fullTableName, fullIndexName);
        } finally {
            EnvironmentEdgeManager.injectEdge(null);
        }
    }

    @Test
    public void testIndexWriteFailureDisablingIndex() throws Throwable {
        testIndexWriteFailureDuringRebuild(PIndexState.DISABLE);
    }
    
    @Test
    public void testIndexWriteFailureLeavingIndexActive() throws Throwable {
        testIndexWriteFailureDuringRebuild(PIndexState.PENDING_ACTIVE);
    }
    
    private void testIndexWriteFailureDuringRebuild(PIndexState indexStateOnFailure) throws Throwable {
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
            conn.createStatement().execute("CREATE TABLE " + fullTableName + "(k VARCHAR PRIMARY KEY, v1 VARCHAR, v2 VARCHAR, v3 VARCHAR) COLUMN_ENCODED_BYTES = 0, DISABLE_INDEX_ON_WRITE_FAILURE = " + (indexStateOnFailure == PIndexState.DISABLE));
            clock.time += 100;
            conn.createStatement().execute("CREATE INDEX " + indexName + " ON " + fullTableName + " (v1, v2)");
            clock.time += 100;
            conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES('a','a','0')");
            conn.commit();
            clock.time += 100;
            Table metaTable = conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES);

            long disableTime = clock.currentTime();
            // Simulates an index write failure
            IndexUtil.updateIndexState(fullIndexName, indexStateOnFailure == PIndexState.DISABLE ? disableTime : -disableTime, metaTable, indexStateOnFailure);
            
            clock.time += 100;
            conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES('bb','bb', '11')");
            conn.commit();
            
            // Large enough to be in separate time batch
            clock.time += 2 * REBUILD_PERIOD;
            assertTrue(hasIndexWithState(metaCache, key, indexStateOnFailure));
            assertEquals(2,TestUtil.getRowCount(conn, fullTableName));
            assertEquals(1,TestUtil.getRowCount(conn, fullIndexName));
            conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES('ccc','ccc','222')");
            conn.commit();
            assertEquals(3,TestUtil.getRowCount(conn, fullTableName));
            assertEquals(1,TestUtil.getRowCount(conn, fullIndexName));
            clock.time += 100;

            waitForIndexState(conn, fullTableName, fullIndexName, indexStateOnFailure == PIndexState.DISABLE ? PIndexState.INACTIVE : PIndexState.ACTIVE);
            clock.time += WAIT_AFTER_DISABLED;
            
            // First batch should have been processed
            runIndexRebuilder(fullTableName);
            assertEquals(2,TestUtil.getRowCount(conn, fullIndexName));

            // Simulate write failure
            TestUtil.addCoprocessor(conn, fullIndexName, WriteFailingRegionObserver.class);
            conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES('dddd','dddd','3333')");
            try {
                conn.commit();
                fail();
            } catch (CommitException e) {
                // Expected
            }
            assertTrue(TestUtil.checkIndexState(conn, fullIndexName, indexStateOnFailure, null));
            PhoenixStatement stmt = conn.createStatement().unwrap(PhoenixStatement.class);
            ResultSet rs = stmt.executeQuery("SELECT V2 FROM " + fullTableName + " WHERE V1 = 'a'");
            assertTrue(rs.next());
            assertEquals("0", rs.getString(1));
            assertEquals(indexStateOnFailure == PIndexState.DISABLE ? fullTableName : fullIndexName, stmt.getQueryPlan().getContext().getCurrentTable().getTable().getName().getString());
            TestUtil.removeCoprocessor(conn, fullIndexName, WriteFailingRegionObserver.class);

            clock.time += 1000;
            waitForIndexState(conn, fullTableName, fullIndexName, indexStateOnFailure == PIndexState.DISABLE ? PIndexState.INACTIVE : PIndexState.ACTIVE);
            clock.time += WAIT_AFTER_DISABLED;
            
            // First batch should have been processed again because we started over
            runIndexRebuilder(fullTableName);
            assertEquals(3,TestUtil.getRowCount(conn, fullIndexName));

            clock.time += 2 * REBUILD_PERIOD;
            // Second batch should have been processed now
            runIndexRebuilder(fullTableName);
            clock.time += 2 * REBUILD_PERIOD;
            runIndexRebuilder(fullTableName);
            TestUtil.assertIndexState(conn, fullIndexName, PIndexState.ACTIVE, 0L);
            
            // Verify that other batches were processed
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
            Table metaTable = conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES);
            IndexUtil.updateIndexState(fullIndexName, clock.currentTime(), metaTable, PIndexState.DISABLE);
            conn.createStatement().execute("DELETE FROM " + fullTableName + " WHERE k='a'");
            conn.commit();
            conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES('a','ccc')");
            conn.commit();
            clock.time += 1000;
            waitForIndexState(conn, fullTableName, fullIndexName, PIndexState.INACTIVE);
            clock.time += WAIT_AFTER_DISABLED;
            runIndexRebuilder(fullTableName);
            assertTrue(TestUtil.checkIndexState(conn, fullIndexName, PIndexState.ACTIVE, 0L));
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
            Table metaTable = conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES);
            IndexUtil.updateIndexState(fullIndexName, clock.currentTime(), metaTable, PIndexState.DISABLE);
            conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES('a','ccc')");
            conn.commit();
            conn.createStatement().execute("DELETE FROM " + fullTableName + " WHERE k='a'");
            conn.commit();
            clock.time += 1000;
            waitForIndexState(conn, fullTableName, fullIndexName, PIndexState.INACTIVE);
            clock.time += WAIT_AFTER_DISABLED;
            runIndexRebuilder(fullTableName);
            assertTrue(TestUtil.checkIndexState(conn, fullIndexName, PIndexState.ACTIVE, 0L));
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
            try (Admin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
                admin.disableTable(TableName.valueOf(fullTableName));
                assertFalse(MetaDataUtil.tableRegionsOnline(conf, table));
                admin.enableTable(TableName.valueOf(fullTableName));
            }
            assertTrue(MetaDataUtil.tableRegionsOnline(conf, table));
        }
    }
    
    public static class WriteFailingRegionObserver extends SimpleRegionObserver {
        @Override
        public void postBatchMutate(ObserverContext<RegionCoprocessorEnvironment> c, MiniBatchOperationInProgress<Mutation> miniBatchOp) throws IOException {
            throw new DoNotRetryIOException("Simulating write failure on " + c.getEnvironment().getRegionInfo().getTable().getNameAsString());
        }
    }
}
