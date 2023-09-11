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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.mapreduce.PhoenixTTLTool;
import org.apache.phoenix.mapreduce.util.PhoenixMultiInputUtil;
import org.apache.phoenix.query.ConfigurationFactory;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.util.InstanceResolver;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(NeedsOwnMiniClusterTest.class)
public class PhoenixTTLToolIT extends ParallelStatsDisabledIT {

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        Map<String, String> props = Maps.newHashMapWithExpectedSize(1);
        props.put(BaseScannerRegionObserverConstants.PHOENIX_MAX_LOOKBACK_AGE_CONF_KEY, Integer.toString(60*60)); // An hour
        props.put(QueryServices.USE_STATS_FOR_PARALLELIZATION, Boolean.toString(false));
        props.put(QueryServices.PHOENIX_TTL_SERVER_SIDE_MASKING_ENABLED, Boolean.toString(true));
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));

        InstanceResolver.clearSingletons();
        // Make sure the ConnectionInfo in the tool doesn't try to pull a default Configuration
        InstanceResolver.getSingleton(ConfigurationFactory.class, new ConfigurationFactory() {
            @Override
            public Configuration getConfiguration() {
                return new Configuration(config);
            }

            @Override
            public Configuration getConfiguration(Configuration confToClone) {
                Configuration copy = new Configuration(config);
                copy.addResource(confToClone);
                return copy;
            }
        });
    }

    private final long PHOENIX_TTL_EXPIRE_IN_A_SECOND = 1;
    private final long MILLISECOND = 1000;
    private final long PHOENIX_TTL_EXPIRE_IN_A_DAY = 1000 * 60 * 60 * 24;

    private final String VIEW_PREFIX1 = "V01";
    private final String VIEW_PREFIX2 = "V02";
    private final String UPSERT_TO_GLOBAL_VIEW_QUERY =
            "UPSERT INTO %s (PK1,A,B,C,D) VALUES(1,1,1,1,1)";
    private final String UPSERT_TO_LEAF_VIEW_QUERY =
            "UPSERT INTO %s (PK1,A,B,C,D,E,F) VALUES(1,1,1,1,1,1,1)";
    private final String VIEW_DDL_WITH_ID_PREFIX_AND_TTL = "CREATE VIEW %s (" +
            "PK1 BIGINT PRIMARY KEY,A BIGINT, B BIGINT, C BIGINT, D BIGINT)" +
            " AS SELECT * FROM %s WHERE ID = '%s' PHOENIX_TTL = %d";
    private final String VIEW_INDEX_DDL = "CREATE INDEX %s ON %s(%s)";
    private final String TENANT_VIEW_DDL =
            "CREATE VIEW %s (E BIGINT, F BIGINT) AS SELECT * FROM %s";

    private void verifyNumberOfRowsFromHBaseLevel(String tableName, String regrex, int expectedRows)
            throws Exception {
        try (Table table = driver.getConnectionQueryServices(getUrl(), TestUtil.TEST_PROPERTIES)
                .getTable(SchemaUtil.getTableNameAsBytes(
                        SchemaUtil.getSchemaNameFromFullName(tableName),
                        SchemaUtil.getTableNameFromFullName(tableName)))) {
            Filter filter =
                    new RowFilter(CompareOperator.EQUAL, new RegexStringComparator(regrex));
            Scan scan = new Scan();
            scan.setFilter(filter);
            assertEquals(expectedRows, getRowCount(table,scan));
        }
    }

    private void verifyNumberOfRows(String tableName, String tenantId, int expectedRows,
                                    Connection conn) throws Exception {
        String query = "SELECT COUNT(*) FROM " + tableName;
        if (tenantId != null) {
            query = query + " WHERE TENANT_ID = '" + tenantId + "'";
        }
        try (Statement stm = conn.createStatement()) {

            ResultSet rs = stm.executeQuery(query);
            assertTrue(rs.next());
            assertEquals(expectedRows, rs.getInt(1));
        }
    }

    private long getRowCount(Table table, Scan scan) throws Exception {
        ResultScanner scanner = table.getScanner(scan);
        int numMatchingRows = 0;
        for (Result result = scanner.next(); result != null; result = scanner.next()) {
            numMatchingRows++;
        }

        scanner.close();
        return numMatchingRows;
    }
    private void createMultiTenantTable(Connection conn, String tableName) throws Exception {
        String ddl = "CREATE TABLE " + tableName +
                " (TENANT_ID CHAR(10) NOT NULL, ID CHAR(10) NOT NULL, NUM BIGINT CONSTRAINT " +
                "PK PRIMARY KEY (TENANT_ID,ID)) MULTI_TENANT=true, COLUMN_ENCODED_BYTES = 0";

        try (Statement stmt = conn.createStatement()) {
            stmt.execute(ddl);
        }
    }

    /*
                    BaseMultiTenantTable
                  GlobalView1 with TTL(1 ms)
                Index1                 Index2

        Creating 2 tenantViews and Upserting data.
        After running the MR job, it should delete all data.
     */
    @Test
    public void testTenantViewOnGlobalViewWithMoreThanOneIndex() throws Exception {
        String schema = generateUniqueName();
        String baseTableFullName = schema + "." + generateUniqueName();
        String indexTable1 = generateUniqueName() + "_IDX";
        String indexTable2 = generateUniqueName() + "_IDX";
        String globalViewName = schema + "." + generateUniqueName();
        String tenant1 = generateUniqueName();
        String tenant2 = generateUniqueName();
        String tenantView1 = schema + "." + generateUniqueName();
        String tenantView2 = schema + "." + generateUniqueName();
        String indexTable = "_IDX_" + baseTableFullName;

        try (Connection globalConn = DriverManager.getConnection(getUrl());
             Connection tenant1Connection =
                     PhoenixMultiInputUtil.buildTenantConnection(getUrl(), tenant1);
             Connection tenant2Connection =
                     PhoenixMultiInputUtil.buildTenantConnection(getUrl(), tenant2)) {

            createMultiTenantTable(globalConn, baseTableFullName);
            globalConn.createStatement().execute(String.format(VIEW_DDL_WITH_ID_PREFIX_AND_TTL,
                    globalViewName, baseTableFullName, VIEW_PREFIX1,
                    PHOENIX_TTL_EXPIRE_IN_A_SECOND));

            globalConn.createStatement().execute(
                    String.format(VIEW_INDEX_DDL, indexTable1, globalViewName, "A,B"));
            globalConn.createStatement().execute(
                    String.format(VIEW_INDEX_DDL, indexTable2, globalViewName, "C,D"));

            tenant1Connection.setAutoCommit(true);
            tenant2Connection.setAutoCommit(true);

            tenant1Connection.createStatement().execute(
                    String.format(TENANT_VIEW_DDL,tenantView1, globalViewName));
            tenant2Connection.createStatement().execute(
                    String.format(TENANT_VIEW_DDL,tenantView2, globalViewName));

            tenant1Connection.createStatement().execute(
                    String.format(UPSERT_TO_LEAF_VIEW_QUERY, tenantView1));
            verifyNumberOfRows(baseTableFullName, tenant1, 1, globalConn);
            tenant2Connection.createStatement().execute(
                    String.format(UPSERT_TO_LEAF_VIEW_QUERY, tenantView2));

            // wait the row to be expired and index to be updated
            Thread.sleep(PHOENIX_TTL_EXPIRE_IN_A_SECOND * MILLISECOND);
            verifyNumberOfRows(baseTableFullName, tenant2, 1, globalConn);

            // the view has 2 view indexes, so upsert 1 row(base table) will result
            // 2 rows(index table)
            verifyNumberOfRowsFromHBaseLevel(indexTable, ".*" + tenant1 + ".*", 2);
            verifyNumberOfRowsFromHBaseLevel(indexTable, ".*" + tenant2 + ".*", 2);

            // running MR job to delete expired rows.
            PhoenixTTLTool phoenixTtlTool = new PhoenixTTLTool();
            Configuration conf = new Configuration(getUtility().getConfiguration());
            phoenixTtlTool.setConf(conf);
            int status = phoenixTtlTool.run(new String[]{"-runfg", "-a"});
            assertEquals(0, status);

            verifyNumberOfRows(baseTableFullName, tenant1, 0, globalConn);
            verifyNumberOfRows(baseTableFullName, tenant2, 0, globalConn);
            verifyNumberOfRowsFromHBaseLevel(indexTable, ".*" + tenant2 + ".*", 0);
            verifyNumberOfRowsFromHBaseLevel(indexTable, ".*" + tenant1 + ".*", 0);
        }
    }

    /*
                                     BaseMultiTenantTable
             GlobalView1 with TTL(1 ms)            GlobalView2 with TTL(1 DAY)
        Index1                 Index2            Index3                    Index4

        Upserting data to both global views and run the MR job.
        It should only delete GlobalView1 data not remove GlobalView2 data.
     */
    @Test
    public void testGlobalViewWithMoreThanOneIndex() throws Exception {
        String schema = generateUniqueName();
        String baseTableFullName = schema + "." + generateUniqueName();
        String globalViewName1 = schema + "." + generateUniqueName();
        String globalViewName2 = schema + "." + generateUniqueName();
        String indexTable1 = generateUniqueName() + "_IDX";
        String indexTable2 = generateUniqueName() + "_IDX";
        String indexTable3 = generateUniqueName() + "_IDX";
        String indexTable4 = generateUniqueName() + "_IDX";
        String indexTable = "_IDX_" + baseTableFullName;
        String tenant1 = generateUniqueName();
        String tenant2 = generateUniqueName();

        try (Connection globalConn = DriverManager.getConnection(getUrl());
             Connection tenant1Connection =
                     PhoenixMultiInputUtil.buildTenantConnection(getUrl(), tenant1);
             Connection tenant2Connection =
                     PhoenixMultiInputUtil.buildTenantConnection(getUrl(), tenant2)) {

            createMultiTenantTable(globalConn, baseTableFullName);

            globalConn.createStatement().execute(String.format(VIEW_DDL_WITH_ID_PREFIX_AND_TTL,
                    globalViewName1, baseTableFullName, VIEW_PREFIX1,
                    PHOENIX_TTL_EXPIRE_IN_A_SECOND));
            globalConn.createStatement().execute(String.format(VIEW_DDL_WITH_ID_PREFIX_AND_TTL,
                    globalViewName2, baseTableFullName, VIEW_PREFIX2, PHOENIX_TTL_EXPIRE_IN_A_DAY));

            globalConn.createStatement().execute(
                    String.format(VIEW_INDEX_DDL, indexTable1, globalViewName1, "A,B"));
            globalConn.createStatement().execute(
                    String.format(VIEW_INDEX_DDL, indexTable2, globalViewName1, "C,D"));

            globalConn.createStatement().execute(
                    String.format(VIEW_INDEX_DDL, indexTable3, globalViewName2, "A,B"));
            globalConn.createStatement().execute(
                    String.format(VIEW_INDEX_DDL, indexTable4, globalViewName2, "C,D"));

            tenant1Connection.setAutoCommit(true);
            tenant2Connection.setAutoCommit(true);

            tenant1Connection.createStatement().execute(
                    String.format(UPSERT_TO_GLOBAL_VIEW_QUERY, globalViewName1));
            tenant1Connection.createStatement().execute(
                    String.format(UPSERT_TO_GLOBAL_VIEW_QUERY, globalViewName2));
            tenant2Connection.createStatement().execute(
                    String.format(UPSERT_TO_GLOBAL_VIEW_QUERY, globalViewName1));
            tenant2Connection.createStatement().execute(
                    String.format(UPSERT_TO_GLOBAL_VIEW_QUERY, globalViewName2));

            // wait the row to be expired and index to be updated
            Thread.sleep(PHOENIX_TTL_EXPIRE_IN_A_SECOND * MILLISECOND);

            verifyNumberOfRows(baseTableFullName, tenant1, 2, globalConn);
            verifyNumberOfRows(baseTableFullName, tenant2, 2, globalConn);

            verifyNumberOfRowsFromHBaseLevel(indexTable, ".*" + tenant1 + ".*", 4);
            verifyNumberOfRowsFromHBaseLevel(indexTable, ".*" + tenant2 + ".*", 4);

            // running MR job to delete expired rows.
            PhoenixTTLTool phoenixTtlTool = new PhoenixTTLTool();
            Configuration conf = new Configuration(getUtility().getConfiguration());
            phoenixTtlTool.setConf(conf);
            int status = phoenixTtlTool.run(new String[]{"-runfg", "-a"});
            assertEquals(0, status);

            verifyNumberOfRows(baseTableFullName, tenant1, 1, globalConn);
            verifyNumberOfRows(baseTableFullName, tenant2, 1, globalConn);
            verifyNumberOfRowsFromHBaseLevel(indexTable, ".*" + tenant2 + ".*", 2);
            verifyNumberOfRowsFromHBaseLevel(indexTable, ".*" + tenant1 + ".*", 2);
        }
    }

    /*
                                    BaseMultiTenantTable
            GlobalView1 with TTL(1 ms)            GlobalView2 with TTL(1 DAY)
       Index1                 Index2            Index3                    Index4
                TenantView1                                 TenantView2

       Upserting data to both global views, and run the MR job.
       It should only delete GlobalView1 data not remove GlobalView2 data.
    */
    @Test
    public void testTenantViewCase() throws Exception {
        String schema = generateUniqueName();
        String baseTableFullName = schema + "." + generateUniqueName();
        String globalViewName1 = schema + "." + generateUniqueName();
        String globalViewName2 = schema + "." + generateUniqueName();
        String tenantViewName1 = schema + "." + generateUniqueName();
        String tenantViewName2 = schema + "." + generateUniqueName();
        String indexTable1 = generateUniqueName() + "_IDX";
        String indexTable2 = generateUniqueName() + "_IDX";
        String indexTable3 = generateUniqueName() + "_IDX";
        String indexTable4 = generateUniqueName() + "_IDX";
        String indexTable = "_IDX_" + baseTableFullName;
        String tenant1 = generateUniqueName();
        String tenant2 = generateUniqueName();

        try (Connection globalConn = DriverManager.getConnection(getUrl());
             Connection tenant1Connection =
                     PhoenixMultiInputUtil.buildTenantConnection(getUrl(), tenant1);
             Connection tenant2Connection =
                     PhoenixMultiInputUtil.buildTenantConnection(getUrl(), tenant2)) {

            createMultiTenantTable(globalConn, baseTableFullName);
            String ddl = "CREATE VIEW %s (PK1 BIGINT PRIMARY KEY, " +
                    "A BIGINT, B BIGINT, C BIGINT, D BIGINT)" +
                    " AS SELECT * FROM " + baseTableFullName + " WHERE ID ='%s' PHOENIX_TTL = %d";

            globalConn.createStatement().execute(
                    String.format(ddl, globalViewName1, VIEW_PREFIX1,
                            PHOENIX_TTL_EXPIRE_IN_A_SECOND));
            globalConn.createStatement().execute(
                    String.format(ddl, globalViewName2, VIEW_PREFIX2, PHOENIX_TTL_EXPIRE_IN_A_DAY));

            ddl = "CREATE INDEX %s ON %s(%s)";

            globalConn.createStatement().execute(
                    String.format(ddl, indexTable1, globalViewName1, "A,B"));
            globalConn.createStatement().execute(
                    String.format(ddl, indexTable2, globalViewName1, "C,D"));

            globalConn.createStatement().execute(
                    String.format(ddl, indexTable3, globalViewName2, "A,B"));
            globalConn.createStatement().execute(
                    String.format(ddl, indexTable4, globalViewName2, "C,D"));

            ddl = "CREATE VIEW %s (E BIGINT, F BIGINT) AS SELECT * FROM %s";
            tenant1Connection.createStatement().execute(
                    String.format(ddl, tenantViewName1, globalViewName1));
            tenant1Connection.createStatement().execute(
                    String.format(ddl, tenantViewName2, globalViewName2));

            tenant2Connection.createStatement().execute(
                    String.format(ddl, tenantViewName1, globalViewName1));
            tenant2Connection.createStatement().execute(
                    String.format(ddl, tenantViewName2, globalViewName2));

            tenant1Connection.setAutoCommit(true);
            tenant2Connection.setAutoCommit(true);

            tenant1Connection.createStatement().execute(
                    String.format(UPSERT_TO_LEAF_VIEW_QUERY, tenantViewName1));
            tenant1Connection.createStatement().execute(
                    String.format(UPSERT_TO_LEAF_VIEW_QUERY, tenantViewName2));
            tenant2Connection.createStatement().execute(
                    String.format(UPSERT_TO_LEAF_VIEW_QUERY, tenantViewName1));
            tenant2Connection.createStatement().execute(
                    String.format(UPSERT_TO_LEAF_VIEW_QUERY, tenantViewName2));

            // wait the row to be expired and index to be updated
            Thread.sleep(PHOENIX_TTL_EXPIRE_IN_A_SECOND * MILLISECOND);

            verifyNumberOfRows(baseTableFullName, tenant1, 2, globalConn);
            verifyNumberOfRows(baseTableFullName, tenant2, 2, globalConn);

            verifyNumberOfRowsFromHBaseLevel(indexTable, ".*" + tenant1 + ".*", 4);
            verifyNumberOfRowsFromHBaseLevel(indexTable, ".*" + tenant2 + ".*", 4);

            // running MR job to delete expired rows.
            PhoenixTTLTool phoenixTtlTool = new PhoenixTTLTool();
            Configuration conf = new Configuration(getUtility().getConfiguration());
            phoenixTtlTool.setConf(conf);
            int status = phoenixTtlTool.run(new String[]{"-runfg", "-a"});
            assertEquals(0, status);

            verifyNumberOfRows(baseTableFullName, tenant1, 1, globalConn);
            verifyNumberOfRows(baseTableFullName, tenant2, 1, globalConn);
            verifyNumberOfRowsFromHBaseLevel(indexTable, ".*" + tenant2 + ".*", 2);
            verifyNumberOfRowsFromHBaseLevel(indexTable, ".*" + tenant1 + ".*", 2);
        }
    }

    /*
                                     BaseMultiTenantTable
             GlobalView1 with TTL(1 ms)            GlobalView2 with TTL(1 DAY)
        Upserting data to both global views, and run the MR job.
        It should only delete GlobalView1 data not remove GlobalView2 data.
     */
    @Test
    public void testGlobalViewWithNoIndex() throws Exception {
        String schema = generateUniqueName();
        String baseTableFullName = schema + "." + generateUniqueName();
        String globalViewName1 = schema + "." + generateUniqueName();
        String globalViewName2 = schema + "." + generateUniqueName();
        String tenant1 = generateUniqueName();
        String tenant2 = generateUniqueName();

        try (Connection globalConn = DriverManager.getConnection(getUrl());
             Connection tenant1Connection =
                     PhoenixMultiInputUtil.buildTenantConnection(getUrl(), tenant1);
             Connection tenant2Connection =
                     PhoenixMultiInputUtil.buildTenantConnection(getUrl(), tenant2)) {

            createMultiTenantTable(globalConn, baseTableFullName);
            String ddl = "CREATE VIEW %s (PK1 BIGINT PRIMARY KEY, " +
                    "A BIGINT, B BIGINT, C BIGINT, D BIGINT)" +
                    " AS SELECT * FROM " + baseTableFullName + " WHERE ID ='%s' PHOENIX_TTL = %d";

            globalConn.createStatement().execute(
                    String.format(ddl, globalViewName1, VIEW_PREFIX1,
                            PHOENIX_TTL_EXPIRE_IN_A_SECOND));
            globalConn.createStatement().execute(
                    String.format(ddl, globalViewName2, VIEW_PREFIX2, PHOENIX_TTL_EXPIRE_IN_A_DAY));

            tenant1Connection.setAutoCommit(true);
            tenant2Connection.setAutoCommit(true);

            tenant1Connection.createStatement().execute(
                    String.format(UPSERT_TO_GLOBAL_VIEW_QUERY, globalViewName1));
            tenant1Connection.createStatement().execute(
                    String.format(UPSERT_TO_GLOBAL_VIEW_QUERY, globalViewName2));

            tenant2Connection.createStatement().execute(
                    String.format(UPSERT_TO_GLOBAL_VIEW_QUERY, globalViewName1));
            tenant2Connection.createStatement().execute(
                    String.format(UPSERT_TO_GLOBAL_VIEW_QUERY, globalViewName2));

            // wait the row to be expired and index to be updated
            Thread.sleep(PHOENIX_TTL_EXPIRE_IN_A_SECOND * MILLISECOND);

            verifyNumberOfRows(baseTableFullName, tenant1, 2, globalConn);
            verifyNumberOfRows(baseTableFullName, tenant2, 2, globalConn);

            // running MR job to delete expired rows.
            PhoenixTTLTool phoenixTtlTool = new PhoenixTTLTool();
            Configuration conf = new Configuration(getUtility().getConfiguration());
            phoenixTtlTool.setConf(conf);
            int status = phoenixTtlTool.run(new String[]{"-runfg", "-a"});
            assertEquals(0, status);

            verifyNumberOfRows(baseTableFullName, tenant1, 1, globalConn);
            verifyNumberOfRows(baseTableFullName, tenant2, 1, globalConn);
        }
    }

    /*
                                        BaseTable
            GlobalView1 with TTL(1 ms)            GlobalView2 with TTL(1 DAY)
       Upserting data to both global views, and run the MR job.
       It should only delete GlobalView1 data not remove GlobalView2 data.
    */
    @Test
    public void testGlobalViewOnNonMultiTenantTable() throws Exception {
        String schema = generateUniqueName();
        String baseTableFullName = schema + "." + generateUniqueName();
        String globalViewName1 = schema + "." + generateUniqueName();
        String globalViewName2 = schema + "." + generateUniqueName();

        try (Connection globalConn = DriverManager.getConnection(getUrl())) {
            String ddl = "CREATE TABLE " + baseTableFullName  +
                    " (ID CHAR(10) NOT NULL PRIMARY KEY, NUM BIGINT)";
            globalConn.createStatement().execute(ddl);

            ddl = "CREATE VIEW %s (PK1 BIGINT PRIMARY KEY, " +
                    "A BIGINT, B BIGINT, C BIGINT, D BIGINT)" +
                    " AS SELECT * FROM " + baseTableFullName + " WHERE ID ='%s' PHOENIX_TTL = %d";

            globalConn.createStatement().execute(
                    String.format(ddl, globalViewName1, VIEW_PREFIX1,
                            PHOENIX_TTL_EXPIRE_IN_A_SECOND));
            globalConn.createStatement().execute(
                    String.format(ddl, globalViewName2, VIEW_PREFIX2, PHOENIX_TTL_EXPIRE_IN_A_DAY));

            globalConn.setAutoCommit(true);

            globalConn.createStatement().execute(
                    String.format(UPSERT_TO_GLOBAL_VIEW_QUERY, globalViewName1));
            globalConn.createStatement().execute(
                    String.format(UPSERT_TO_GLOBAL_VIEW_QUERY, globalViewName2));

            // wait the row to be expired and index to be updated
            Thread.sleep(PHOENIX_TTL_EXPIRE_IN_A_SECOND * MILLISECOND);

            verifyNumberOfRowsFromHBaseLevel(baseTableFullName, ".*" + VIEW_PREFIX1 + ".*", 1);
            verifyNumberOfRowsFromHBaseLevel(baseTableFullName, ".*" + VIEW_PREFIX2 + ".*", 1);

            // running MR job to delete expired rows.
            PhoenixTTLTool phoenixTtlTool = new PhoenixTTLTool();
            Configuration conf = new Configuration(getUtility().getConfiguration());
            phoenixTtlTool.setConf(conf);
            int status = phoenixTtlTool.run(new String[]{"-runfg", "-a"});
            assertEquals(0, status);
            verifyNumberOfRowsFromHBaseLevel(baseTableFullName, ".*" + VIEW_PREFIX1 + ".*", 0);
            verifyNumberOfRowsFromHBaseLevel(baseTableFullName, ".*" + VIEW_PREFIX2 + ".*", 1);
        }
    }

    /*
                                        BaseTable
            GlobalView1 with TTL(1 ms)            GlobalView2 with TTL(1 DAY)
        Index1                 Index2            Index3                    Index4

       Upserting data to both global views, and run the MR job.
       It should only delete GlobalView1 data not remove GlobalView2 data.
    */
    @Test
    public void testGlobalViewOnNonMultiTenantTableWithIndex() throws Exception {
        String schema = generateUniqueName();
        String baseTableFullName = schema + "." + generateUniqueName();
        String globalViewName1 = schema + "." + generateUniqueName();
        String globalViewName2 = schema + "." + generateUniqueName();
        String indexTable1 = generateUniqueName() + "_IDX";
        String indexTable2 = generateUniqueName() + "_IDX";
        String indexTable3 = generateUniqueName() + "_IDX";
        String indexTable4 = generateUniqueName() + "_IDX";
        String indexTable = "_IDX_" + baseTableFullName;

        try (Connection globalConn = DriverManager.getConnection(getUrl())) {
            String ddl = "CREATE TABLE " + baseTableFullName  +
                    " (PK1 BIGINT NOT NULL, ID CHAR(10) NOT NULL, NUM BIGINT CONSTRAINT " +
                    "PK PRIMARY KEY (PK1,ID))";
            globalConn.createStatement().execute(ddl);

            ddl = "CREATE VIEW %s (PK2 BIGINT PRIMARY KEY, " +
                    "A BIGINT, B BIGINT, C BIGINT, D BIGINT)" +
                    " AS SELECT * FROM " + baseTableFullName + " WHERE PK1=%d PHOENIX_TTL = %d";

            globalConn.createStatement().execute(
                    String.format(ddl, globalViewName1, 1, PHOENIX_TTL_EXPIRE_IN_A_SECOND));
            globalConn.createStatement().execute(
                    String.format(ddl, globalViewName2, 2, PHOENIX_TTL_EXPIRE_IN_A_DAY));

            ddl = "CREATE INDEX %s ON %s(%s)";
            globalConn.createStatement().execute(
                    String.format(ddl, indexTable1, globalViewName1, "A,ID,B"));
            globalConn.createStatement().execute(
                    String.format(ddl, indexTable2, globalViewName1, "C,ID,D"));
            globalConn.createStatement().execute(
                    String.format(ddl, indexTable3, globalViewName2, "A,ID,B"));
            globalConn.createStatement().execute(
                    String.format(ddl, indexTable4, globalViewName2, "C,ID,D"));

            globalConn.setAutoCommit(true);

            String query = "UPSERT INTO %s (PK2,A,B,C,D,ID) VALUES(1,1,1,1,1,'%s')";
            globalConn.createStatement().execute(
                    String.format(query, globalViewName1, VIEW_PREFIX1));
            globalConn.createStatement().execute(
                    String.format(query, globalViewName2, VIEW_PREFIX2));

            // wait the row to be expired and index to be updated
            Thread.sleep(PHOENIX_TTL_EXPIRE_IN_A_SECOND * MILLISECOND);

            verifyNumberOfRowsFromHBaseLevel(baseTableFullName, ".*" + VIEW_PREFIX1 + ".*", 1);
            verifyNumberOfRowsFromHBaseLevel(baseTableFullName, ".*" + VIEW_PREFIX2 + ".*", 1);
            verifyNumberOfRowsFromHBaseLevel(indexTable, ".*" + VIEW_PREFIX1 + ".*", 2);
            verifyNumberOfRowsFromHBaseLevel(indexTable, ".*" + VIEW_PREFIX2 + ".*", 2);

            // running MR job to delete expired rows.
            PhoenixTTLTool phoenixTtlTool = new PhoenixTTLTool();
            Configuration conf = new Configuration(getUtility().getConfiguration());
            phoenixTtlTool.setConf(conf);
            int status = phoenixTtlTool.run(new String[]{"-runfg", "-a"});
            assertEquals(0, status);
            verifyNumberOfRowsFromHBaseLevel(baseTableFullName, ".*" + VIEW_PREFIX1 + ".*", 0);
            verifyNumberOfRowsFromHBaseLevel(baseTableFullName, ".*" + VIEW_PREFIX2 + ".*", 1);
            verifyNumberOfRowsFromHBaseLevel(indexTable, ".*" + VIEW_PREFIX1 + ".*", 0);
            verifyNumberOfRowsFromHBaseLevel(indexTable, ".*" + VIEW_PREFIX2 + ".*", 2);
        }
    }

    /*
                                     BaseMultiTenantTable
                                        GlobalView1
                             TenantView1         TenantView2
     */
    @Test
    public void testDeleteByViewAndTenant() throws Exception {
        String schema = generateUniqueName();
        String baseTableFullName = schema + "." + generateUniqueName();
        String globalViewName1 = schema + "." + generateUniqueName();
        String tenantViewName1 = schema + "." + generateUniqueName();
        String tenantViewName2 = schema + "." + generateUniqueName();
        String tenant1 = generateUniqueName();

        try (Connection globalConn = DriverManager.getConnection(getUrl());
             Connection tenant1Connection =
                     PhoenixMultiInputUtil.buildTenantConnection(getUrl(), tenant1)) {

            createMultiTenantTable(globalConn, baseTableFullName);
            String ddl = "CREATE VIEW %s (PK1 BIGINT PRIMARY KEY, " +
                    "A BIGINT, B BIGINT, C BIGINT, D BIGINT)" +
                    " AS SELECT * FROM " + baseTableFullName + " WHERE NUM = 1";

            globalConn.createStatement().execute(String.format(ddl, globalViewName1));

            ddl = "CREATE VIEW %s (E BIGINT, F BIGINT) AS SELECT * FROM %s " +
                    "WHERE ID = '%s' PHOENIX_TTL = %d";
            tenant1Connection.createStatement().execute(
                    String.format(ddl, tenantViewName1, globalViewName1, VIEW_PREFIX1,
                            PHOENIX_TTL_EXPIRE_IN_A_SECOND));
            tenant1Connection.createStatement().execute(
                    String.format(ddl, tenantViewName2, globalViewName1, VIEW_PREFIX2,
                            PHOENIX_TTL_EXPIRE_IN_A_SECOND));

            tenant1Connection.setAutoCommit(true);

            tenant1Connection.createStatement().execute(
                    String.format(UPSERT_TO_LEAF_VIEW_QUERY, tenantViewName1));
            tenant1Connection.createStatement().execute(
                    String.format(UPSERT_TO_LEAF_VIEW_QUERY, tenantViewName2));

            // wait the row to be expired and index to be updated
            Thread.sleep(PHOENIX_TTL_EXPIRE_IN_A_SECOND * MILLISECOND);

            verifyNumberOfRows(baseTableFullName, tenant1, 2, globalConn);
            verifyNumberOfRowsFromHBaseLevel(baseTableFullName, ".*" + VIEW_PREFIX1 + ".*", 1);
            verifyNumberOfRowsFromHBaseLevel(baseTableFullName, ".*" + VIEW_PREFIX2 + ".*", 1);
            // running MR job to delete expired rows.
            PhoenixTTLTool phoenixTtlTool = new PhoenixTTLTool();
            Configuration conf = new Configuration(getUtility().getConfiguration());
            phoenixTtlTool.setConf(conf);
            int status = phoenixTtlTool.run(new String[]{"-runfg", "-v", tenantViewName2, "-i", tenant1});
            assertEquals(0, status);

            verifyNumberOfRows(baseTableFullName, tenant1, 1, globalConn);
            verifyNumberOfRowsFromHBaseLevel(baseTableFullName, ".*" + VIEW_PREFIX1 + ".*", 1);
            verifyNumberOfRowsFromHBaseLevel(baseTableFullName, ".*" + VIEW_PREFIX2 + ".*", 0);
        }
    }

    /*
                                     BaseMultiTenantTable
                               GlobalView1         GlobalView1
                         TenantView1  TenantView2   TenantView1  TenantView2
     */
    @Test
    public void testDeleteByTenant() throws Exception {
        String schema = generateUniqueName();
        String baseTableFullName = schema + "." + generateUniqueName();
        String globalViewName1 = schema + "." + generateUniqueName();
        String globalViewName2 = schema + "." + generateUniqueName();
        String tenantViewName1 = schema + "." + generateUniqueName();
        String tenantViewName2 = schema + "." + generateUniqueName();
        String tenantViewName3 = schema + "." + generateUniqueName();
        String tenantViewName4 = schema + "." + generateUniqueName();
        String tenant1 = generateUniqueName();
        String tenant2 = generateUniqueName();

        try (Connection globalConn = DriverManager.getConnection(getUrl());
             Connection tenant1Connection =
                     PhoenixMultiInputUtil.buildTenantConnection(getUrl(), tenant1);
             Connection tenant2Connection =
                     PhoenixMultiInputUtil.buildTenantConnection(getUrl(), tenant2)) {

            createMultiTenantTable(globalConn, baseTableFullName);
            String ddl = "CREATE VIEW %s (PK1 BIGINT PRIMARY KEY, " +
                    "A BIGINT, B BIGINT, C BIGINT, D BIGINT)" +
                    " AS SELECT * FROM " + baseTableFullName + " WHERE NUM = %d";

            globalConn.createStatement().execute(String.format(ddl, globalViewName1, 1));
            globalConn.createStatement().execute(String.format(ddl, globalViewName2, 2));

            ddl = "CREATE VIEW %s (E BIGINT, F BIGINT) AS SELECT * FROM %s " +
                    "WHERE ID = '%s' PHOENIX_TTL = %d";
            tenant1Connection.createStatement().execute(
                    String.format(ddl, tenantViewName1, globalViewName1, VIEW_PREFIX1,
                            PHOENIX_TTL_EXPIRE_IN_A_SECOND));
            tenant1Connection.createStatement().execute(
                    String.format(ddl, tenantViewName2, globalViewName2, VIEW_PREFIX2,
                            PHOENIX_TTL_EXPIRE_IN_A_SECOND));

            tenant2Connection.createStatement().execute(
                    String.format(ddl, tenantViewName3, globalViewName1, VIEW_PREFIX1,
                            PHOENIX_TTL_EXPIRE_IN_A_SECOND));
            tenant2Connection.createStatement().execute(
                    String.format(ddl, tenantViewName4, globalViewName2, VIEW_PREFIX2,
                            PHOENIX_TTL_EXPIRE_IN_A_SECOND));

            tenant1Connection.setAutoCommit(true);
            tenant2Connection.setAutoCommit(true);

            tenant1Connection.createStatement().execute(
                    String.format(UPSERT_TO_LEAF_VIEW_QUERY, tenantViewName1));
            tenant1Connection.createStatement().execute(
                    String.format(UPSERT_TO_LEAF_VIEW_QUERY, tenantViewName2));
            tenant2Connection.createStatement().execute(
                    String.format(UPSERT_TO_LEAF_VIEW_QUERY, tenantViewName3));
            tenant2Connection.createStatement().execute(
                    String.format(UPSERT_TO_LEAF_VIEW_QUERY, tenantViewName4));

            // wait the row to be expired and index to be updated
            Thread.sleep(PHOENIX_TTL_EXPIRE_IN_A_SECOND * MILLISECOND);

            verifyNumberOfRowsFromHBaseLevel(baseTableFullName, ".*" + tenant1 + ".*", 2);
            verifyNumberOfRowsFromHBaseLevel(baseTableFullName, ".*" + tenant2 + ".*", 2);

            // running MR job to delete expired rows.
            PhoenixTTLTool phoenixTtlTool = new PhoenixTTLTool();
            Configuration conf = new Configuration(getUtility().getConfiguration());
            phoenixTtlTool.setConf(conf);
            int status = phoenixTtlTool.run(new String[]{"-runfg", "-i", tenant1});
            assertEquals(0, status);

            verifyNumberOfRowsFromHBaseLevel(baseTableFullName, ".*" + tenant2 + ".*", 2);
            verifyNumberOfRowsFromHBaseLevel(baseTableFullName, ".*" + tenant1 + ".*", 0);
        }
    }

    /*
                                     BaseMultiTenantTable
                               GlobalView1         GlobalView1
                         TenantView1  TenantView2   TenantView1  TenantView2
     */
    @Test
    public void testDeleteByViewName() throws Exception {
        String schema = generateUniqueName();
        String baseTableFullName = schema + "." + generateUniqueName();
        String globalViewName1 = schema + "." + generateUniqueName();
        String globalViewName2 = schema + "." + generateUniqueName();
        String tenantViewName1 = schema + "." + generateUniqueName();
        String tenantViewName2 = schema + "." + generateUniqueName();
        String tenantViewName3 = schema + "." + generateUniqueName();
        String tenantViewName4 = schema + "." + generateUniqueName();
        String tenant1 = generateUniqueName();
        String tenant2 = generateUniqueName();

        try (Connection globalConn = DriverManager.getConnection(getUrl());
             Connection tenant1Connection =
                     PhoenixMultiInputUtil.buildTenantConnection(getUrl(), tenant1);
             Connection tenant2Connection =
                     PhoenixMultiInputUtil.buildTenantConnection(getUrl(), tenant2)) {

            createMultiTenantTable(globalConn, baseTableFullName);
            String ddl = "CREATE VIEW %s (PK1 BIGINT PRIMARY KEY, " +
                    "A BIGINT, B BIGINT, C BIGINT, D BIGINT)" +
                    " AS SELECT * FROM " + baseTableFullName + " WHERE NUM = %d PHOENIX_TTL = %d";

            globalConn.createStatement().execute(
                    String.format(ddl, globalViewName1, 1, PHOENIX_TTL_EXPIRE_IN_A_SECOND));
            globalConn.createStatement().execute(
                    String.format(ddl, globalViewName2, 2, PHOENIX_TTL_EXPIRE_IN_A_SECOND));

            ddl = "CREATE VIEW %s (E BIGINT, F BIGINT) AS SELECT * FROM %s WHERE ID = '%s'";
            tenant1Connection.createStatement().execute(
                    String.format(ddl, tenantViewName1, globalViewName1, VIEW_PREFIX1));
            tenant1Connection.createStatement().execute(
                    String.format(ddl, tenantViewName2, globalViewName2, VIEW_PREFIX2));

            tenant2Connection.createStatement().execute(
                    String.format(ddl, tenantViewName3, globalViewName1, VIEW_PREFIX1));
            tenant2Connection.createStatement().execute(
                    String.format(ddl, tenantViewName4, globalViewName2, VIEW_PREFIX2));

            tenant1Connection.setAutoCommit(true);
            tenant2Connection.setAutoCommit(true);

            tenant1Connection.createStatement().execute(
                    String.format(UPSERT_TO_LEAF_VIEW_QUERY, tenantViewName1));
            tenant1Connection.createStatement().execute(
                    String.format(UPSERT_TO_LEAF_VIEW_QUERY, tenantViewName2));
            tenant2Connection.createStatement().execute(
                    String.format(UPSERT_TO_LEAF_VIEW_QUERY, tenantViewName3));
            tenant2Connection.createStatement().execute(
                    String.format(UPSERT_TO_LEAF_VIEW_QUERY, tenantViewName4));

            // wait the row to be expired and index to be updated
            Thread.sleep(PHOENIX_TTL_EXPIRE_IN_A_SECOND * MILLISECOND);

            verifyNumberOfRowsFromHBaseLevel(baseTableFullName, ".*" + VIEW_PREFIX1 + ".*", 2);
            verifyNumberOfRowsFromHBaseLevel(baseTableFullName, ".*" + VIEW_PREFIX2 + ".*", 2);

            // running MR job to delete expired rows.
            PhoenixTTLTool phoenixTtlTool = new PhoenixTTLTool();
            Configuration conf = new Configuration(getUtility().getConfiguration());
            phoenixTtlTool.setConf(conf);
            int status = phoenixTtlTool.run(new String[]{"-runfg", "-v", globalViewName1});
            assertEquals(0, status);

            verifyNumberOfRowsFromHBaseLevel(baseTableFullName, ".*" + VIEW_PREFIX2 + ".*", 2);
            verifyNumberOfRowsFromHBaseLevel(baseTableFullName, ".*" + VIEW_PREFIX1 + ".*", 0);
        }
    }


    /*
                                             BaseTable
                                         GlobalView1 with TTL
               MiddleLevelView1 with TTL(1 ms)    MiddleLevelView2 with TTL(1 DAY)
                    LeafView1                             LeafView2
       Upserting data to both leafView, and run the MR job.
       It should only delete MiddleLevelView1 data not remove MiddleLevelView2 data.
    */
    @Test
    public void testCleanMoreThanThreeLevelViewCase() throws Exception {
        String schema = generateUniqueName();
        String baseTableFullName = schema + "." + generateUniqueName();
        String globalViewName = schema + "." + generateUniqueName();
        String middleLevelViewName1 = schema + "." + generateUniqueName();
        String middleLevelViewName2 = schema + "." + generateUniqueName();
        String leafViewName1 = schema + "." + generateUniqueName();
        String leafViewName2 = schema + "." + generateUniqueName();

        try (Connection globalConn = DriverManager.getConnection(getUrl())) {
            String baseTableDdl = "CREATE TABLE " + baseTableFullName  +
                    " (ID CHAR(10) NOT NULL PRIMARY KEY, NUM BIGINT)";
            globalConn.createStatement().execute(baseTableDdl);

            String globalViewDdl = "CREATE VIEW %s (PK1 BIGINT PRIMARY KEY, " +
                    "A BIGINT, B BIGINT)" + " AS SELECT * FROM " + baseTableFullName;

            globalConn.createStatement().execute(String.format(globalViewDdl, globalViewName));

            String middleLevelViewDdl = "CREATE VIEW %s (C BIGINT, D BIGINT)" +
                    " AS SELECT * FROM %s WHERE ID ='%s' PHOENIX_TTL = %d";

            globalConn.createStatement().execute(String.format(middleLevelViewDdl,
                    middleLevelViewName1, globalViewName,
                    VIEW_PREFIX1, PHOENIX_TTL_EXPIRE_IN_A_SECOND));
            globalConn.createStatement().execute(String.format(middleLevelViewDdl,
                    middleLevelViewName2, globalViewName, VIEW_PREFIX2,
                    PHOENIX_TTL_EXPIRE_IN_A_DAY));

            String leafViewDdl = "CREATE VIEW %s (E BIGINT, F BIGINT)" +
                    " AS SELECT * FROM %s";

            globalConn.createStatement().execute(String.format(leafViewDdl,
                    leafViewName1, middleLevelViewName1));
            globalConn.createStatement().execute(String.format(leafViewDdl,
                    leafViewName2, middleLevelViewName2));

            globalConn.setAutoCommit(true);

            globalConn.createStatement().execute(
                    String.format(UPSERT_TO_LEAF_VIEW_QUERY, leafViewName1));
            globalConn.createStatement().execute(
                    String.format(UPSERT_TO_LEAF_VIEW_QUERY, leafViewName2));

            // wait the row to be expired and index to be updated
            Thread.sleep(PHOENIX_TTL_EXPIRE_IN_A_SECOND * MILLISECOND);

            verifyNumberOfRowsFromHBaseLevel(baseTableFullName, ".*" + VIEW_PREFIX1 + ".*", 1);
            verifyNumberOfRowsFromHBaseLevel(baseTableFullName, ".*" + VIEW_PREFIX2 + ".*", 1);

            // running MR job to delete expired rows.
            PhoenixTTLTool phoenixTtlTool = new PhoenixTTLTool();
            Configuration conf = new Configuration(getUtility().getConfiguration());
            phoenixTtlTool.setConf(conf);
            int status = phoenixTtlTool.run(new String[]{"-runfg", "-a"});
            assertEquals(0, status);
            verifyNumberOfRowsFromHBaseLevel(baseTableFullName, ".*" + VIEW_PREFIX2 + ".*", 1);
            verifyNumberOfRowsFromHBaseLevel(baseTableFullName, ".*" + VIEW_PREFIX1 + ".*", 0);
        }
    }

    @Test
    public void testNoViewCase() throws Exception {
        PhoenixTTLTool phoenixTtlTool = new PhoenixTTLTool();
        Configuration conf = new Configuration(getUtility().getConfiguration());
        phoenixTtlTool.setConf(conf);
        int status = phoenixTtlTool.run(new String[]{"-runfg", "-a"});
        assertEquals(0, status);
    }
}