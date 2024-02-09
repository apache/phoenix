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

import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Table;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.mapreduce.index.IndexTool;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@Category(ParallelStatsDisabledTest.class)
public class IndexToolForDeleteBeforeRebuildIT extends ParallelStatsDisabledIT {
    private Connection conn;
    private String dataTableName;
    private String schemaName;
    private String dataTableFullName;
    private String viewName;
    private String viewFullName;
    private String globalIndexName;
    private String globalIndexFullName;

    private static final String
            DATA_TABLE_DDL = "CREATE TABLE %s (TENANT_ID VARCHAR(15) NOT NULL, ID INTEGER NOT NULL, NAME VARCHAR"
            + ", ZIP INTEGER, EMPLOYER VARCHAR , CONSTRAINT PK_1 PRIMARY KEY (TENANT_ID, ID)) MULTI_TENANT=true";
    private static final String VIEW_DDL = "CREATE VIEW %s AS  SELECT * FROM %s";
    private static final String
            INDEX_GLOBAL_DDL = "CREATE INDEX %s ON %s (ID, NAME, ZIP) INCLUDE (EMPLOYER)";
    private static final String
            INDEX_LOCAL_DDL = "CREATE LOCAL INDEX %s ON %s (ZIP) INCLUDE (NAME)";
    private static final String UPSERT_SQL = "UPSERT INTO %s VALUES(?,?,?,?)";

    @BeforeClass
    public static synchronized void setup() throws Exception {
        Map<String, String> serverProps = Maps.newHashMapWithExpectedSize(3);
        serverProps.put(QueryServices.STATS_GUIDEPOST_WIDTH_BYTES_ATTRIB, Long.toString(20));
        serverProps.put(QueryServices.MAX_SERVER_METADATA_CACHE_TIME_TO_LIVE_MS_ATTRIB, Long.toString(5));
        serverProps.put(QueryServices.EXTRA_JDBC_ARGUMENTS_ATTRIB,
            QueryServicesOptions.DEFAULT_EXTRA_JDBC_ARGUMENTS);
        Map<String, String> clientProps = Maps.newHashMapWithExpectedSize(4);
        clientProps.put(QueryServices.USE_STATS_FOR_PARALLELIZATION, Boolean.toString(true));
        clientProps.put(QueryServices.STATS_UPDATE_FREQ_MS_ATTRIB, Long.toString(5));
        clientProps.put(QueryServices.TRANSACTIONS_ENABLED, Boolean.TRUE.toString());
        clientProps.put(QueryServices.FORCE_ROW_KEY_ORDER_ATTRIB, Boolean.TRUE.toString());
        setUpTestDriver(new ReadOnlyProps(serverProps.entrySet().iterator()),
            new ReadOnlyProps(clientProps.entrySet().iterator()));
    }

    @Before
    public void prepareTest() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        conn = DriverManager.getConnection(getUrl(), props);

        schemaName = generateUniqueName();
        dataTableName = generateUniqueName();
        viewName = generateUniqueName();
        dataTableFullName = SchemaUtil.getTableName(schemaName, dataTableName);
        viewFullName = SchemaUtil.getTableName(schemaName, viewName);
        globalIndexName = "GLBL_IDX_" + generateUniqueName();
        globalIndexFullName = SchemaUtil.getTableName(schemaName, globalIndexName);
        createTestTable(getUrl(), String.format(DATA_TABLE_DDL, dataTableFullName));
        createTestTable(getUrl(), String.format(VIEW_DDL, viewFullName, dataTableFullName));
        String dataTableUpsert = String.format(UPSERT_SQL, dataTableFullName);
        PreparedStatement stmt = conn.prepareStatement(dataTableUpsert);
        for (int i=1; i < 4; i++) {
            upsertRow(stmt, "tenantID1", i, "name" + i, 9990+i);
        }
        conn.commit();
    }

    private void upsertRow(PreparedStatement stmt, String tenantId, int id, String name, int zip)
            throws SQLException {
        int index = 1;
        stmt.setString(index++, tenantId);
        stmt.setInt(index++, id);
        stmt.setString(index++, name);
        stmt.setInt(index++, zip);
        stmt.executeUpdate();
    }

    @After
    public void teardown() throws Exception {
        if (conn != null) {
            boolean refCountLeaked = isAnyStoreRefCountLeaked();
            conn.close();
            assertFalse("refCount leaked", refCountLeaked);
        }
    }

    @Test
    /**
     * IndexTool should return -1 for View Indexes because view indexes share the same table.
     */
    public void testDeleteBeforeRebuildForViewIndexShouldFail() throws Exception {
        String createViewIndex = String.format(INDEX_GLOBAL_DDL, globalIndexName, viewFullName);
        PreparedStatement stmt = conn.prepareStatement(createViewIndex);
        stmt.execute();
        runIndexTool(schemaName, viewName, globalIndexName, -1);
    }

    @Test
    /**
     * Test delete before rebuild
     */
    public void testDeleteBeforeRebuildForGlobalIndex() throws Exception {
        conn.createStatement().execute(String.format(INDEX_GLOBAL_DDL, globalIndexName, dataTableFullName));
        String globalIndexUpsert = String.format(UPSERT_SQL, globalIndexFullName);
        PreparedStatement stmt = conn.prepareStatement(globalIndexUpsert);
        upsertRow(stmt, "tenantID1",11, "name11", 99911);
        conn.commit();

        ConnectionQueryServices queryServices = conn.unwrap(PhoenixConnection.class).getQueryServices();
        PTable physicalTable = conn.unwrap(PhoenixConnection.class).getTable(globalIndexFullName);
        Table hIndexTable= queryServices.getTable(physicalTable.getPhysicalName().getBytes());
        int count = getUtility().countRows(hIndexTable);
        // Confirm index has rows.
        assertEquals(4, count);

        runIndexTool(schemaName, dataTableName, globalIndexName, 0);

        count = getUtility().countRows(hIndexTable);

        // Confirm index has all the data rows
        assertEquals(3, count);
    }

    @Test
    /**
     * For local indexes the data is on the same row and all local indexes share the same column family
     * So, it should return -1 for local indexes.
     */
    public void testDeleteBeforeRebuildForLocalIndexShouldFail() throws Exception {
        String localIndexName = generateUniqueName();
        conn.createStatement().execute(String.format(INDEX_LOCAL_DDL, localIndexName, dataTableFullName));
        conn.commit();

        runIndexTool(schemaName, dataTableName, localIndexName, -1);
    }

    public static String[] getArgValues(String schemaName, String dataTable, String indxTable) {
        final List<String> args = Lists.newArrayList();
        if (schemaName != null) {
            args.add("-s");
            args.add(schemaName);
        }
        args.add("-dt");
        args.add(dataTable);
        args.add("-it");
        args.add(indxTable);

        args.add("-direct");
        // Need to run this job in foreground for the test to be deterministic
        args.add("-runfg");

        args.add("-deleteall");

        args.add("-op");
        args.add("/tmp/" + UUID.randomUUID().toString());
        return args.toArray(new String[0]);
    }


    public static void runIndexTool(String schemaName,
            String dataTableName, String indexTableName,  int expectedStatus,
            String... additionalArgs) throws Exception {
        IndexTool indexingTool = new IndexTool();
        Configuration conf = new Configuration(getUtility().getConfiguration());
        conf.set(QueryServices.TRANSACTIONS_ENABLED, Boolean.TRUE.toString());
        indexingTool.setConf(conf);
        final String[] cmdArgs =
                getArgValues(schemaName, dataTableName, indexTableName);
        List<String> cmdArgList = new ArrayList<>(Arrays.asList(cmdArgs));
        cmdArgList.addAll(Arrays.asList(additionalArgs));
        int status = indexingTool.run(cmdArgList.toArray(new String[cmdArgList.size()]));

        assertEquals(expectedStatus, status);
    }
}
