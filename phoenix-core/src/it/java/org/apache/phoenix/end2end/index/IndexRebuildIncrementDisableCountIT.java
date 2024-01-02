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

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessor.MetaDataRegionObserver;
import org.apache.phoenix.coprocessor.MetaDataRegionObserver.BuildIndexScheduleTask;
import org.apache.phoenix.coprocessorclient.MetaDataProtocol.MutationCode;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;

@Category(NeedsOwnMiniClusterTest.class)
public class IndexRebuildIncrementDisableCountIT extends BaseTest {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(IndexRebuildIncrementDisableCountIT.class);
    private static long pendingDisableCount = 0;
    private static String ORG_PREFIX = "ORG";
    private static Result pendingDisableCountResult = null;
    private static String indexState = null;
    private static final Random RAND = new Random(5);
    private static final int WAIT_AFTER_DISABLED = 5000;
    private static final long REBUILD_PERIOD = 50000;
    private static final long REBUILD_INTERVAL = 2000;
    private static RegionCoprocessorEnvironment indexRebuildTaskRegionEnvironment;
    private static String schemaName;
    private static String tableName;
    private static String fullTableName;
    private static String indexName;
    private static String fullIndexName;
    private static Connection conn;
    private static PhoenixConnection phoenixConn;

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        Map<String, String> serverProps = Maps.newHashMapWithExpectedSize(10);
        serverProps.put(QueryServices.INDEX_FAILURE_HANDLING_REBUILD_ATTRIB,
            Boolean.TRUE.toString());
        serverProps.put(QueryServices.INDEX_FAILURE_HANDLING_REBUILD_INTERVAL_ATTRIB,
            Long.toString(REBUILD_INTERVAL));
        serverProps.put(QueryServices.INDEX_REBUILD_DISABLE_TIMESTAMP_THRESHOLD, "50000000");
        serverProps.put(QueryServices.INDEX_FAILURE_HANDLING_REBUILD_PERIOD,
            Long.toString(REBUILD_PERIOD)); // batch at 50 seconds
        serverProps.put(QueryServices.INDEX_FAILURE_HANDLING_REBUILD_OVERLAP_FORWARD_TIME_ATTRIB,
            Long.toString(WAIT_AFTER_DISABLED));
        Map<String, String> clientProps = Maps.newHashMapWithExpectedSize(1);
        clientProps.put(HConstants.HBASE_CLIENT_RETRIES_NUMBER, "2");
        setUpTestDriver(new ReadOnlyProps(serverProps.entrySet().iterator()),
            new ReadOnlyProps(clientProps.entrySet().iterator()));
        indexRebuildTaskRegionEnvironment =
                (RegionCoprocessorEnvironment) getUtility()
                        .getRSForFirstRegionInTable(
                            PhoenixDatabaseMetaData.SYSTEM_CATALOG_HBASE_TABLE_NAME)
                        .getRegions(PhoenixDatabaseMetaData.SYSTEM_CATALOG_HBASE_TABLE_NAME)
                        .get(0).getCoprocessorHost()
                        .findCoprocessorEnvironment(MetaDataRegionObserver.class.getName());
        MetaDataRegionObserver.initRebuildIndexConnectionProps(
            indexRebuildTaskRegionEnvironment.getConfiguration());
        schemaName = generateUniqueName();
        tableName = generateUniqueName();
        fullTableName = SchemaUtil.getTableName(schemaName, tableName);
        indexName = generateUniqueName();
        fullIndexName = SchemaUtil.getTableName(schemaName, indexName);
        conn = DriverManager.getConnection(getUrl());
        phoenixConn = conn.unwrap(PhoenixConnection.class);
    }

    static long getPendingDisableCount(PhoenixConnection conn, String indexTableName) {
        byte[] indexTableKey = SchemaUtil.getTableKeyFromFullName(indexTableName);
        Get get = new Get(indexTableKey);
        get.addColumn(TABLE_FAMILY_BYTES, PhoenixDatabaseMetaData.PENDING_DISABLE_COUNT_BYTES);
        get.addColumn(TABLE_FAMILY_BYTES, PhoenixDatabaseMetaData.INDEX_STATE_BYTES);

        try {
            pendingDisableCountResult =
                    conn.getQueryServices()
                            .getTable(SchemaUtil.getPhysicalTableName(
                                PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME,
                                conn.getQueryServices().getProps()).getName())
                            .get(get);
            return Bytes.toLong(pendingDisableCountResult.getValue(TABLE_FAMILY_BYTES,
                PhoenixDatabaseMetaData.PENDING_DISABLE_COUNT_BYTES));
        } catch (Exception e) {
            LOGGER.error("Exception in getPendingDisableCount: " + e);
            return 0;
        }
    }

    private static void checkIndexPendingDisableCount(final PhoenixConnection conn,
            final String indexTableName) throws Exception {
        Runnable runnable = new Runnable() {
            @Override
            public void run() {
                try {
                    while (!TestUtil.checkIndexState(conn, indexTableName, PIndexState.ACTIVE,
                        0L)) {
                        long count = getPendingDisableCount(conn, indexTableName);
                        if (count > 0) {
                            indexState =
                                    new String(
                                            pendingDisableCountResult.getValue(TABLE_FAMILY_BYTES,
                                                PhoenixDatabaseMetaData.INDEX_STATE_BYTES));
                            pendingDisableCount = count;
                        }
                        Thread.sleep(100);
                    }
                } catch (Exception e) {
                    LOGGER.error("Error in checkPendingDisableCount : " + e);
                }
            }
        };
        Thread t1 = new Thread(runnable);
        t1.start();
    }

    static String getOrgId(long id) {
        return ORG_PREFIX + "-" + id;
    }

    static String getRandomOrgId(int maxOrgId) {
        return getOrgId(Math.round(Math.random() * maxOrgId));
    }

    private static void mutateRandomly(Connection conn, String tableName, int maxOrgId) {
        try {

            Statement stmt = conn.createStatement();
            for (int i = 0; i < 10000; i++) {
                stmt.executeUpdate(
                    "UPSERT INTO " + tableName + " VALUES('" + getRandomOrgId(maxOrgId) + "'," + i
                            + "," + (i + 1) + "," + (i + 2) + ")");
            }
            conn.commit();
        } catch (Exception e) {
            LOGGER.error("Client side exception:" + e);
        }
    }

    private static MutationCode updateIndexState(PhoenixConnection phoenixConn,
            String fullIndexName, PIndexState state) throws Throwable {
        Table metaTable =
                phoenixConn.getQueryServices()
                        .getTable(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES);
        long ts = EnvironmentEdgeManager.currentTimeMillis();
        return IndexUtil.updateIndexState(fullIndexName, ts, metaTable, state).getMutationCode();
    }

    @Test
    public void testIndexStateTransitions() throws Throwable {
        // create table and indices
        String createTableSql =
                "CREATE TABLE " + fullTableName
                        + "(org_id VARCHAR NOT NULL PRIMARY KEY, v1 INTEGER, v2 INTEGER, v3 INTEGER)";
        conn.createStatement().execute(createTableSql);
        conn.createStatement()
                .execute("CREATE INDEX " + indexName + " ON " + fullTableName + "(v1)");
        conn.commit();
        updateIndexState(phoenixConn, fullIndexName, PIndexState.DISABLE);
        mutateRandomly(conn, fullTableName, 20);
        boolean[] cancel = new boolean[1];
        checkIndexPendingDisableCount(phoenixConn, fullIndexName);
        try {
            do {
                runIndexRebuilder(Collections.<String> singletonList(fullTableName));
            } while (!TestUtil.checkIndexState(conn, fullIndexName, PIndexState.ACTIVE, 0L));
        } finally {
            cancel[0] = true;
        }
        assertTrue("Index state is inactive ", indexState.equals("i"));
        assertTrue("pendingDisable count is incremented when index is inactive",
            pendingDisableCount == MetaDataRegionObserver.PENDING_DISABLE_INACTIVE_STATE_COUNT);
        assertTrue("pending disable count is 0 when index is active: ", getPendingDisableCount(phoenixConn, fullIndexName) == 0);
    }
    
    @Test
    public void checkIndexPendingDisableResetCounter() throws Throwable {
        IndexUtil.incrementCounterForIndex(phoenixConn, fullIndexName, MetaDataRegionObserver.PENDING_DISABLE_INACTIVE_STATE_COUNT);
        updateIndexState(phoenixConn, fullIndexName, PIndexState.PENDING_DISABLE);
        assertTrue("Pending disable count should reset when index moves from ACTIVE to PENDING_DISABLE ", getPendingDisableCount(phoenixConn, fullIndexName) == 0);
        IndexUtil.incrementCounterForIndex(phoenixConn, fullIndexName, MetaDataRegionObserver.PENDING_DISABLE_INACTIVE_STATE_COUNT);
        updateIndexState(phoenixConn, fullIndexName, PIndexState.INACTIVE);
        updateIndexState(phoenixConn, fullIndexName, PIndexState.PENDING_DISABLE);
        assertTrue("Pending disable count should reset when index moves from ACTIVE to PENDING_DISABLE ", getPendingDisableCount(phoenixConn, fullIndexName) == MetaDataRegionObserver.PENDING_DISABLE_INACTIVE_STATE_COUNT);
    }

    private static void runIndexRebuilder(List<String> tables)
            throws InterruptedException, SQLException {
        BuildIndexScheduleTask task =
                new MetaDataRegionObserver.BuildIndexScheduleTask(indexRebuildTaskRegionEnvironment,
                        tables);
        task.run();
    }

}
