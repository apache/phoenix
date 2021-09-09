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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.SimpleRegionObserver;
import org.apache.hadoop.hbase.exceptions.TimeoutIOException;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.phoenix.coprocessor.MetaDataRegionObserver;
import org.apache.phoenix.coprocessor.MetaDataRegionObserver.BuildIndexScheduleTask;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.RunUntilFailure;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;

@Category(NeedsOwnMiniClusterTest.class)
@RunWith(RunUntilFailure.class)
public class MutableIndexRebuilderIT extends BaseTest {
    private static final int WAIT_AFTER_DISABLED = 0;
    private static final long REBUILD_PERIOD = 50000;
    private static final long REBUILD_INTERVAL = 2000;
    private static RegionCoprocessorEnvironment indexRebuildTaskRegionEnvironment;

    /**
     * Tests that the index rebuilder retries for exactly the configured # of retries
     * @throws Exception
     */
    @Test
    public void testRebuildRetriesSuccessful() throws Throwable {
        int numberOfRetries = 5;
        Map<String, String> serverProps = Maps.newHashMapWithExpectedSize(10);
        serverProps.put(QueryServices.INDEX_FAILURE_HANDLING_REBUILD_ATTRIB, Boolean.TRUE.toString());
        serverProps.put(QueryServices.INDEX_FAILURE_HANDLING_REBUILD_INTERVAL_ATTRIB, Long.toString(REBUILD_INTERVAL));
        serverProps.put(QueryServices.INDEX_REBUILD_DISABLE_TIMESTAMP_THRESHOLD, "50000000");
        serverProps.put(QueryServices.INDEX_FAILURE_HANDLING_REBUILD_PERIOD, Long.toString(REBUILD_PERIOD)); // batch at 50 seconds
        serverProps.put(QueryServices.INDEX_FAILURE_HANDLING_REBUILD_OVERLAP_FORWARD_TIME_ATTRIB, Long.toString(WAIT_AFTER_DISABLED));
        serverProps.put(HConstants.HBASE_CLIENT_RETRIES_NUMBER, numberOfRetries + "");
        Map<String, String> clientProps = Maps.newHashMapWithExpectedSize(1);
        // Index rebuilds are not needed with IndexRegionObserver
        clientProps.put(QueryServices.INDEX_REGION_OBSERVER_ENABLED_ATTRIB, "false");
        setUpTestDriver(new ReadOnlyProps(serverProps.entrySet().iterator()), new ReadOnlyProps(clientProps.entrySet().iterator()));
        indexRebuildTaskRegionEnvironment =
                getUtility()
                         .getRSForFirstRegionInTable(
                             PhoenixDatabaseMetaData.SYSTEM_CATALOG_HBASE_TABLE_NAME)
                         .getRegions(PhoenixDatabaseMetaData.SYSTEM_CATALOG_HBASE_TABLE_NAME)
                         .get(0).getCoprocessorHost()
                         .findCoprocessorEnvironment(MetaDataRegionObserver.class.getName());
        MetaDataRegionObserver.initRebuildIndexConnectionProps(
            indexRebuildTaskRegionEnvironment.getConfiguration());
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String schemaName = generateUniqueName();
            String tableName = generateUniqueName();
            String indexName = generateUniqueName();
            final String fullTableName = SchemaUtil.getTableName(schemaName, tableName);
            final String fullIndexName = SchemaUtil.getTableName(schemaName, indexName);
            conn.createStatement().execute("CREATE TABLE " + fullTableName + "(k VARCHAR PRIMARY KEY, v1 VARCHAR, v2 VARCHAR, v3 VARCHAR) DISABLE_INDEX_ON_WRITE_FAILURE = TRUE");
            conn.createStatement().execute("CREATE INDEX " + indexName + " ON " + fullTableName + " (v1, v2)");
            Table metaTable = conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES);
            IndexUtil.updateIndexState(fullIndexName, EnvironmentEdgeManager.currentTimeMillis(), metaTable, PIndexState.DISABLE);
            conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES('a','a','0')");
            conn.commit();
            // Simulate write failure when rebuilder runs
            TestUtil.addCoprocessor(conn, fullIndexName, WriteFailingRegionObserver.class);
            waitForIndexState(conn, fullTableName, fullIndexName, PIndexState.INACTIVE);
            long pendingDisableCount = TestUtil.getPendingDisableCount(
                    conn.unwrap(PhoenixConnection.class), fullIndexName);
            // rebuild writes should retry for exactly the configured number of times
            ExecutorService executor = Executors.newSingleThreadExecutor();
            try {
                Future<Boolean> future = executor.submit(new Callable<Boolean>() {
                    @Override
                    public Boolean call() throws Exception {
                        runIndexRebuilder(fullTableName);
                        return true;
                    }});
                assertTrue(future.get(120, TimeUnit.SECONDS));
                assertEquals(numberOfRetries, WriteFailingRegionObserver.attempts.get());
                // Index rebuild write failures should not increase the pending disable count of the index table
                assertEquals(pendingDisableCount, TestUtil.getPendingDisableCount(
                        conn.unwrap(PhoenixConnection.class), fullIndexName));
            } finally {
                executor.shutdownNow();
            }
        }
    }

    public static void waitForIndexState(Connection conn, String fullTableName, String fullIndexName, PIndexState expectedIndexState) throws InterruptedException, SQLException {
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

    private static void runIndexRebuilder(String table) throws InterruptedException, SQLException {
        runIndexRebuilder(Collections.<String>singletonList(table));
    }

    private static void runIndexRebuilder(List<String> tables) throws InterruptedException, SQLException {
        BuildIndexScheduleTask task =
                new MetaDataRegionObserver.BuildIndexScheduleTask(
                        indexRebuildTaskRegionEnvironment, tables);
        task.run();
    }

    public static class WriteFailingRegionObserver extends SimpleRegionObserver {
        public static volatile AtomicInteger attempts = new AtomicInteger(0);
        @Override
        public void postBatchMutate(ObserverContext<RegionCoprocessorEnvironment> c, MiniBatchOperationInProgress<Mutation> miniBatchOp) throws IOException {
            attempts.incrementAndGet();
            throw new DoNotRetryIOException("Simulating write failure on " + c.getEnvironment().getRegionInfo().getTable().getNameAsString());
        }
    }
}
