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

import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_OPEN_PHOENIX_CONNECTIONS;
import static org.apache.phoenix.util.PhoenixRuntime.TENANT_ID_ATTRIB;
import static org.apache.phoenix.util.PhoenixRuntime.UPSERT_BATCH_SIZE_ATTRIB;
import static org.apache.phoenix.util.TestUtil.A_VALUE;
import static org.apache.phoenix.util.TestUtil.B_VALUE;
import static org.apache.phoenix.util.TestUtil.C_VALUE;
import static org.apache.phoenix.util.TestUtil.PTSDB_NAME;
import static org.apache.phoenix.util.TestUtil.ROW6;
import static org.apache.phoenix.util.TestUtil.ROW7;
import static org.apache.phoenix.util.TestUtil.ROW8;
import static org.apache.phoenix.util.TestUtil.ROW9;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Map;
import java.util.Properties;

import org.apache.phoenix.compat.hbase.coprocessor
        .CompatBaseScannerRegionObserver;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixResultSet;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.monitoring.GlobalMetric;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@Category(ParallelStatsDisabledTest.class)
@RunWith(Parameterized.class)
public class UpsertSelectIT extends ParallelStatsDisabledIT {
    private final String allowServerSideMutations;

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        Map<String, String> props = Maps.newHashMapWithExpectedSize(1);
        // An hour - inherited from ParallelStatsDisabledIT
        props.put(
                CompatBaseScannerRegionObserver.PHOENIX_MAX_LOOKBACK_AGE_CONF_KEY,
                Integer.toString(60 * 60));
        // Postpone scans of SYSTEM.TASK indefinitely so as to prevent
        // any addition to GLOBAL_OPEN_PHOENIX_CONNECTIONS
        props.put(QueryServices.TASK_HANDLING_INTERVAL_MS_ATTRIB,
                Long.toString(Long.MAX_VALUE));
        props.put(QueryServices.TASK_HANDLING_INITIAL_DELAY_MS_ATTRIB,
                Long.toString(Long.MAX_VALUE));
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }

    public UpsertSelectIT(String allowServerSideMutations) {
        this.allowServerSideMutations = allowServerSideMutations;
    }

    @Before
    public void setup() {
        assertTrue(PhoenixRuntime.areGlobalClientMetricsBeingCollected());
        for (GlobalMetric m : PhoenixRuntime.getGlobalPhoenixClientMetrics()) {
            m.reset();
        }
    }

    @After
    public void assertNoConnLeak() throws Exception {
        boolean refCountLeaked = isAnyStoreRefCountLeaked();
        assertTrue(PhoenixRuntime.areGlobalClientMetricsBeingCollected());
        assertEquals(0, GLOBAL_OPEN_PHOENIX_CONNECTIONS.getMetric().getValue());
        assertFalse("refCount leaked", refCountLeaked);
    }

    // name is used by failsafe as file name in reports
    @Parameters(name = "UpsertSelecttIT_allowServerSideMutations={0}")
    public static synchronized Object[] data() {
        return new Object[]{"true", "false"};
    }

    @Test
    public void testUpsertSelectWithNoIndex() throws Exception {
        testUpsertSelect(false, false);
    }

    @Test
    public void testUpsertSelectWithIndex() throws Exception {
        testUpsertSelect(true, false);
    }

    @Test
    public void testUpsertSelectWithIndexWithSalt() throws Exception {
        testUpsertSelect(true, true);
    }

    @Test
    public void testUpsertSelectWithNoIndexWithSalt() throws Exception {
        testUpsertSelect(false, true);
    }

    private void testUpsertSelect(boolean createIndex, boolean saltTable) throws Exception {
        String tenantId = getOrganizationId();
        byte[][] splits = getDefaultSplits(tenantId);
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(QueryServices.ENABLE_SERVER_SIDE_UPSERT_MUTATIONS,
                allowServerSideMutations);
        String aTable = initATableValues(tenantId, saltTable ? null : splits, null,
                null, getUrl(), saltTable ? "salt_buckets = 2" : null);

        String customEntityTable = generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl(), props);
             Statement stmt = conn.createStatement()) {
            String ddl = "create table " + customEntityTable +
                    "   (organization_id char(15) not null, \n" +
                    "    key_prefix char(3) not null,\n" +
                    "    custom_entity_data_id char(12) not null,\n" +
                    "    created_by varchar,\n" +
                    "    created_date date,\n" +
                    "    currency_iso_code char(3),\n" +
                    "    deleted char(1),\n" +
                    "    division decimal(31,10),\n" +
                    "    last_activity date,\n" +
                    "    last_update date,\n" +
                    "    last_update_by varchar,\n" +
                    "    name varchar(240),\n" +
                    "    owner varchar,\n" +
                    "    record_type_id char(15),\n" +
                    "    setup_owner varchar,\n" +
                    "    system_modstamp date,\n" +
                    "    b.val0 varchar,\n" +
                    "    b.val1 varchar,\n" +
                    "    b.val2 varchar,\n" +
                    "    b.val3 varchar,\n" +
                    "    b.val4 varchar,\n" +
                    "    b.val5 varchar,\n" +
                    "    b.val6 varchar,\n" +
                    "    b.val7 varchar,\n" +
                    "    b.val8 varchar,\n" +
                    "    b.val9 varchar\n" +
                    "    CONSTRAINT pk PRIMARY KEY " +
                    "(organization_id, key_prefix, custom_entity_data_id)) " +
                    (saltTable ? "salt_buckets = 2" : "");
            stmt.execute(ddl);
        }

        String indexName = generateUniqueName();
        if (createIndex) {
            try (Connection conn = DriverManager.getConnection(getUrl(), props);
                 Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE INDEX IF NOT EXISTS " + indexName +
                        " ON " + aTable + "(a_string)");
            }
        }
        // Trigger multiple batches
        props.setProperty(UPSERT_BATCH_SIZE_ATTRIB, Integer.toString(3));
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(true);
            String upsert = "UPSERT INTO " + customEntityTable +
                    "(custom_entity_data_id, key_prefix, organization_id, created_by) " +
                    "SELECT substr(entity_id, 4), substr(entity_id, 1, 3), organization_id, " +
                    "a_string  FROM " + aTable + " WHERE ?=a_string";
            if (createIndex) { // Confirm index is used
                try (PreparedStatement upsertStmt =
                             conn.prepareStatement("EXPLAIN " + upsert)) {
                    upsertStmt.setString(1, tenantId);
                    ResultSet ers = upsertStmt.executeQuery();
                    assertTrue(ers.next());
                    String explainPlan = QueryUtil.getExplainPlan(ers);
                    assertTrue(explainPlan.contains(" SCAN OVER " + indexName));
                }
            }

            try (PreparedStatement upsertStmt = conn.prepareStatement(upsert)) {
                upsertStmt.setString(1, A_VALUE);
                int rowsInserted = upsertStmt.executeUpdate();
                assertEquals(4, rowsInserted);
            }
            conn.commit();
        }

        String query = "SELECT key_prefix, substr(custom_entity_data_id, 1, 1), created_by FROM " +
                customEntityTable + " WHERE organization_id = ? ";
        try (Connection conn = DriverManager.getConnection(getUrl(), props);
             PreparedStatement statement = conn.prepareStatement(query)) {
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();

            assertTrue(rs.next());
            assertEquals("00A", rs.getString(1));
            assertEquals("1", rs.getString(2));
            assertEquals(A_VALUE, rs.getString(3));

            assertTrue(rs.next());
            assertEquals("00A", rs.getString(1));
            assertEquals("2", rs.getString(2));
            assertEquals(A_VALUE, rs.getString(3));

            assertTrue(rs.next());
            assertEquals("00A", rs.getString(1));
            assertEquals("3", rs.getString(2));
            assertEquals(A_VALUE, rs.getString(3));

            assertTrue(rs.next());
            assertEquals("00A", rs.getString(1));
            assertEquals("4", rs.getString(2));
            assertEquals(A_VALUE, rs.getString(3));

            assertFalse(rs.next());
        }

        // Test UPSERT through coprocessor
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(true);
            String upsert = "UPSERT INTO " + customEntityTable +
                    "(custom_entity_data_id, key_prefix, organization_id, last_update_by, " +
                    "division) SELECT custom_entity_data_id, key_prefix, organization_id, " +
                    "created_by, 1.0  FROM " + customEntityTable + " WHERE organization_id = ?" +
                    " and created_by >= 'a'";

            try (PreparedStatement upsertStmt = conn.prepareStatement(upsert)) {
                upsertStmt.setString(1, tenantId);
                assertEquals(4, upsertStmt.executeUpdate());
            }
            conn.commit();
        }

        query = "SELECT key_prefix, substr(custom_entity_data_id, 1, 1), created_by, " +
                "last_update_by, division FROM " + customEntityTable + " WHERE organization_id = ?";
        try (Connection conn = DriverManager.getConnection(getUrl(), props);
             PreparedStatement statement = conn.prepareStatement(query)) {
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();

            assertTrue(rs.next());
            assertEquals("00A", rs.getString(1));
            assertEquals("1", rs.getString(2));
            assertEquals(A_VALUE, rs.getString(3));
            assertEquals(A_VALUE, rs.getString(4));
            assertTrue(BigDecimal.valueOf(1.0).compareTo(rs.getBigDecimal(5)) == 0);

            assertTrue(rs.next());
            assertEquals("00A", rs.getString(1));
            assertEquals("2", rs.getString(2));
            assertEquals(A_VALUE, rs.getString(3));
            assertEquals(A_VALUE, rs.getString(4));
            assertTrue(BigDecimal.valueOf(1.0).compareTo(rs.getBigDecimal(5)) == 0);

            assertTrue(rs.next());
            assertEquals("00A", rs.getString(1));
            assertEquals("3", rs.getString(2));
            assertEquals(A_VALUE, rs.getString(3));
            assertEquals(A_VALUE, rs.getString(4));
            assertTrue(BigDecimal.valueOf(1.0).compareTo(rs.getBigDecimal(5)) == 0);

            assertTrue(rs.next());
            assertEquals("00A", rs.getString(1));
            assertEquals("4", rs.getString(2));
            assertEquals(A_VALUE, rs.getString(3));
            assertEquals(A_VALUE, rs.getString(4));
            assertTrue(BigDecimal.valueOf(1.0).compareTo(rs.getBigDecimal(5)) == 0);

            assertFalse(rs.next());
        }
    }

    // TODO: more tests - nullable fixed length last PK column
    @Test
    public void testUpsertSelectEmptyPKColumn() throws Exception {
        String tenantId = getOrganizationId();
        String aTable = initATableValues(tenantId, getDefaultSplits(tenantId));
        String ptsdbTable = generateUniqueName();
        ensureTableCreated(getUrl(), ptsdbTable, PTSDB_NAME);
        Properties props = new Properties();
        props.setProperty(QueryServices.ENABLE_SERVER_SIDE_UPSERT_MUTATIONS,
                allowServerSideMutations);
        String upsert;
        ResultSet rs;
        int rowsInserted;

        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(false);
            upsert = "UPSERT INTO " + ptsdbTable + "(\"DATE\", val, host) " +
                    "SELECT current_date(), x_integer+2, entity_id FROM " + aTable +
                    " WHERE a_integer >= ?";
            try (PreparedStatement upsertStmt = conn.prepareStatement(upsert)) {
                upsertStmt.setInt(1, 6);
                rowsInserted = upsertStmt.executeUpdate();
                assertEquals(4, rowsInserted);
            }
            conn.commit();
        }

        String query = "SELECT inst,host,\"DATE\",val FROM " + ptsdbTable;
        try (Connection conn = DriverManager.getConnection(getUrl(), props);
             PreparedStatement statement = conn.prepareStatement(query)) {
            rs = statement.executeQuery();

            Date now = new Date(EnvironmentEdgeManager.currentTimeMillis());
            assertTrue(rs.next());
            assertEquals(null, rs.getString(1));
            assertEquals(ROW6, rs.getString(2));
            assertTrue(rs.getDate(3).before(now));
            assertEquals(null, rs.getBigDecimal(4));

            assertTrue(rs.next());
            assertEquals(null, rs.getString(1));
            assertEquals(ROW7, rs.getString(2));
            assertTrue(rs.getDate(3).before(now));
            assertTrue(BigDecimal.valueOf(7).compareTo(rs.getBigDecimal(4)) == 0);

            assertTrue(rs.next());
            assertEquals(null, rs.getString(1));
            assertEquals(ROW8, rs.getString(2));
            assertTrue(rs.getDate(3).before(now));
            assertTrue(BigDecimal.valueOf(6).compareTo(rs.getBigDecimal(4)) == 0);

            assertTrue(rs.next());
            assertEquals(null, rs.getString(1));
            assertEquals(ROW9, rs.getString(2));
            assertTrue(rs.getDate(3).before(now));
            assertTrue(BigDecimal.valueOf(5).compareTo(rs.getBigDecimal(4)) == 0);

            assertFalse(rs.next());
        }

        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(true);
            upsert = "UPSERT INTO " + ptsdbTable + "(\"DATE\", val, inst) " +
                    "SELECT \"DATE\"+1, val*10, host FROM " + ptsdbTable;
            try (PreparedStatement upsertStmt = conn.prepareStatement(upsert)) {
                rowsInserted = upsertStmt.executeUpdate();
                assertEquals(4, rowsInserted);
            }
            conn.commit();
        }
        Date now = new Date(EnvironmentEdgeManager.currentTimeMillis());
        Date then = new Date(now.getTime() + QueryConstants.MILLIS_IN_DAY);
        query = "SELECT host,inst, \"DATE\",val FROM " + ptsdbTable + " where inst is not null";

        try (Connection conn = DriverManager.getConnection(getUrl(), props);
             PreparedStatement statement = conn.prepareStatement(query)) {
            rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(null, rs.getString(1));
            assertEquals(ROW6, rs.getString(2));
            assertTrue(rs.getDate(3).after(now) && rs.getDate(3).before(then));
            assertEquals(null, rs.getBigDecimal(4));

            assertTrue(rs.next());
            assertEquals(null, rs.getString(1));
            assertEquals(ROW7, rs.getString(2));
            assertTrue(rs.getDate(3).after(now) && rs.getDate(3).before(then));
            assertTrue(BigDecimal.valueOf(70).compareTo(rs.getBigDecimal(4)) == 0);

            assertTrue(rs.next());
            assertEquals(null, rs.getString(1));
            assertEquals(ROW8, rs.getString(2));
            assertTrue(rs.getDate(3).after(now) && rs.getDate(3).before(then));
            assertTrue(BigDecimal.valueOf(60).compareTo(rs.getBigDecimal(4)) == 0);

            assertTrue(rs.next());
            assertEquals(null, rs.getString(1));
            assertEquals(ROW9, rs.getString(2));
            assertTrue(rs.getDate(3).after(now) && rs.getDate(3).before(then));
            assertTrue(BigDecimal.valueOf(50).compareTo(rs.getBigDecimal(4)) == 0);

            assertFalse(rs.next());
        }

        // Should just update all values with the same value, essentially just updating the timestamp
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(true);
            upsert = "UPSERT INTO " + ptsdbTable + " SELECT * FROM " + ptsdbTable;
            try (PreparedStatement upsertStmt = conn.prepareStatement(upsert)) {
                rowsInserted = upsertStmt.executeUpdate();
                assertEquals(8, rowsInserted);
            }
            conn.commit();
        }

        query = "SELECT * FROM " + ptsdbTable;
        try (Connection conn = DriverManager.getConnection(getUrl(), props);
             PreparedStatement statement = conn.prepareStatement(query)) {
            rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(null, rs.getString(1));
            assertEquals(ROW6, rs.getString(2));
            assertTrue(rs.getDate(3).before(now));
            assertEquals(null, rs.getBigDecimal(4));

            assertTrue(rs.next());
            assertEquals(null, rs.getString(1));
            assertEquals(ROW7, rs.getString(2));
            assertTrue(rs.getDate(3).before(now));
            assertTrue(BigDecimal.valueOf(7).compareTo(rs.getBigDecimal(4)) == 0);

            assertTrue(rs.next());
            assertEquals(null, rs.getString(1));
            assertEquals(ROW8, rs.getString(2));
            assertTrue(rs.getDate(3).before(now));
            assertTrue(BigDecimal.valueOf(6).compareTo(rs.getBigDecimal(4)) == 0);

            assertTrue(rs.next());
            assertEquals(null, rs.getString(1));
            assertEquals(ROW9, rs.getString(2));
            assertTrue(rs.getDate(3).before(now));
            assertTrue(BigDecimal.valueOf(5).compareTo(rs.getBigDecimal(4)) == 0);

            assertTrue(rs.next());
            assertEquals(ROW6, rs.getString(1));
            assertEquals(null, rs.getString(2));
            assertTrue(rs.getDate(3).after(now) && rs.getDate(3).before(then));
            assertEquals(null, rs.getBigDecimal(4));

            assertTrue(rs.next());
            assertEquals(ROW7, rs.getString(1));
            assertEquals(null, rs.getString(2));
            assertTrue(rs.getDate(3).after(now) && rs.getDate(3).before(then));
            assertTrue(BigDecimal.valueOf(70).compareTo(rs.getBigDecimal(4)) == 0);

            assertTrue(rs.next());
            assertEquals(ROW8, rs.getString(1));
            assertEquals(null, rs.getString(2));
            assertTrue(rs.getDate(3).after(now) && rs.getDate(3).before(then));
            assertTrue(BigDecimal.valueOf(60).compareTo(rs.getBigDecimal(4)) == 0);

            assertTrue(rs.next());
            assertEquals(ROW9, rs.getString(1));
            assertEquals(null, rs.getString(2));
            assertTrue(rs.getDate(3).after(now) && rs.getDate(3).before(then));
            assertTrue(BigDecimal.valueOf(50).compareTo(rs.getBigDecimal(4)) == 0);

            assertFalse(rs.next());
        }
    }

    @Test
    public void testUpsertSelectForAggAutoCommit() throws Exception {
        testUpsertSelectForAgg(true);
    }

    @Test
    public void testUpsertSelectForAgg() throws Exception {
        testUpsertSelectForAgg(false);
    }

    private void testUpsertSelectForAgg(boolean autoCommit) throws Exception {
        String tenantId = getOrganizationId();
        String aTable = initATableValues(tenantId, getDefaultSplits(tenantId));
        String ptsdbTable = generateUniqueName();
        ensureTableCreated(getUrl(), ptsdbTable, PTSDB_NAME);
        Properties props = new Properties();
        props.setProperty(QueryServices.ENABLE_SERVER_SIDE_UPSERT_MUTATIONS,
                allowServerSideMutations);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(autoCommit);
            String upsert = "UPSERT INTO " + ptsdbTable + "(\"DATE\", val, host) " +
                    "SELECT current_date(), sum(a_integer), a_string FROM " + aTable +
                    " GROUP BY a_string";
            try (PreparedStatement upsertStmt = conn.prepareStatement(upsert)) {
                int rowsInserted = upsertStmt.executeUpdate();
                assertEquals(3, rowsInserted);
            }
            if (!autoCommit) {
                conn.commit();
            }
        }

        String query = "SELECT inst,host,\"DATE\",val FROM " + ptsdbTable;
        try (Connection conn = DriverManager.getConnection(getUrl(), props);
             PreparedStatement statement = conn.prepareStatement(query)) {
            ResultSet rs = statement.executeQuery();
            Date now = new Date(EnvironmentEdgeManager.currentTimeMillis());

            assertTrue(rs.next());
            assertEquals(null, rs.getString(1));
            assertEquals(A_VALUE, rs.getString(2));
            assertTrue(rs.getDate(3).before(now));
            assertTrue(BigDecimal.valueOf(10).compareTo(rs.getBigDecimal(4)) == 0);

            assertTrue(rs.next());
            assertEquals(null, rs.getString(1));
            assertEquals(B_VALUE, rs.getString(2));
            assertTrue(rs.getDate(3).before(now));
            assertTrue(BigDecimal.valueOf(26).compareTo(rs.getBigDecimal(4)) == 0);

            assertTrue(rs.next());
            assertEquals(null, rs.getString(1));
            assertEquals(C_VALUE, rs.getString(2));
            assertTrue(rs.getDate(3).before(now));
            assertTrue(BigDecimal.valueOf(9).compareTo(rs.getBigDecimal(4)) == 0);
            assertFalse(rs.next());
        }

        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(true);
            String upsert = "UPSERT INTO " + ptsdbTable + "(\"DATE\", val, host, inst) " +
                    "SELECT current_date(), max(val), max(host), 'x' FROM " + ptsdbTable;
            try (PreparedStatement upsertStmt = conn.prepareStatement(upsert)) {
                int rowsInserted = upsertStmt.executeUpdate();
                assertEquals(1, rowsInserted);
            }
            if (!autoCommit) {
                conn.commit();
            }
        }

        query = "SELECT inst,host,\"DATE\",val FROM " + ptsdbTable + " WHERE inst='x'";
        try (Connection conn = DriverManager.getConnection(getUrl(), props);
             PreparedStatement statement = conn.prepareStatement(query)) {
            ResultSet rs = statement.executeQuery();
            Date now = new Date(EnvironmentEdgeManager.currentTimeMillis());

            assertTrue(rs.next());
            assertEquals("x", rs.getString(1));
            assertEquals(C_VALUE, rs.getString(2));
            assertTrue(rs.getDate(3).before(now));
            assertTrue(BigDecimal.valueOf(26).compareTo(rs.getBigDecimal(4)) == 0);
            assertFalse(rs.next());
        }
    }

    @Test
    public void testUpsertSelectLongToInt() throws Exception {
        byte[][] splits = new byte[][]{PInteger.INSTANCE.toBytes(1),
                PInteger.INSTANCE.toBytes(2), PInteger.INSTANCE.toBytes(3),
                PInteger.INSTANCE.toBytes(4)};
        String tableName = generateUniqueName();
        ensureTableCreated(getUrl(), tableName, "IntKeyTest", splits, null);
        Properties props = new Properties();
        props.setProperty(QueryServices.ENABLE_SERVER_SIDE_UPSERT_MUTATIONS,
                allowServerSideMutations);
        String upsert = "UPSERT INTO " + tableName + " VALUES(1)";
        int rowsInserted;

        try (Connection conn = DriverManager.getConnection(getUrl(), props);
             PreparedStatement upsertStmt = conn.prepareStatement(upsert)) {
            rowsInserted = upsertStmt.executeUpdate();
            assertEquals(1, rowsInserted);
            conn.commit();
        }

        upsert = "UPSERT INTO " + tableName + "  select i+1 from " + tableName;
        try (Connection conn = DriverManager.getConnection(getUrl(), props);
             PreparedStatement upsertStmt = conn.prepareStatement(upsert)) {
            rowsInserted = upsertStmt.executeUpdate();
            assertEquals(1, rowsInserted);
            conn.commit();
        }

        String select = "SELECT i FROM " + tableName;
        try (Connection conn = DriverManager.getConnection(getUrl(), props);
             Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery(select);
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            assertFalse(rs.next());
        }
    }

    @Test
    public void testUpsertSelectRunOnServer() throws Exception {
        byte[][] splits = new byte[][]{PInteger.INSTANCE.toBytes(1),
                PInteger.INSTANCE.toBytes(2), PInteger.INSTANCE.toBytes(3),
                PInteger.INSTANCE.toBytes(4)};
        String tableName = generateUniqueName();
        createTestTable(getUrl(), "create table " + tableName +
                " (i integer not null primary key desc, j integer)", splits, null);
        Properties props = new Properties();
        props.setProperty(QueryServices.ENABLE_SERVER_SIDE_UPSERT_MUTATIONS,
                allowServerSideMutations);
        ResultSet rs;
        int rowsInserted;
        String upsert = "UPSERT INTO " + tableName + " VALUES(1, 1)";

        try (Connection conn = DriverManager.getConnection(getUrl(), props);
             PreparedStatement upsertStmt = conn.prepareStatement(upsert)) {
            rowsInserted = upsertStmt.executeUpdate();
            assertEquals(1, rowsInserted);
            conn.commit();
        }

        String select = "SELECT i,j+1 FROM " + tableName;
        try (Connection conn = DriverManager.getConnection(getUrl(), props);
             Statement stmt = conn.createStatement()) {
            rs = stmt.executeQuery(select);
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertEquals(2, rs.getInt(2));
            assertFalse(rs.next());
        }

        upsert = "UPSERT INTO " + tableName + "(i,j) select i, j+1 from " + tableName;
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(true); // Force to run on server side.
            try (PreparedStatement upsertStmt = conn.prepareStatement(upsert)) {
                rowsInserted = upsertStmt.executeUpdate();
                assertEquals(1, rowsInserted);
            }
        }

        select = "SELECT j FROM " + tableName;
        try (Connection conn = DriverManager.getConnection(getUrl(), props);
             Statement stmt = conn.createStatement()) {
            rs = stmt.executeQuery(select);
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            assertFalse(rs.next());
        }

        upsert = "UPSERT INTO " + tableName + "(i,j) select i, i from " + tableName;
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(true); // Force to run on server side.
            try (PreparedStatement upsertStmt = conn.prepareStatement(upsert)) {
                rowsInserted = upsertStmt.executeUpdate();
                assertEquals(1, rowsInserted);
            }
        }

        select = "SELECT j FROM " + tableName;
        try (Connection conn = DriverManager.getConnection(getUrl(), props);
             Statement stmt = conn.createStatement()) {
            rs = stmt.executeQuery(select);
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertFalse(rs.next());
        }
    }

    @Test
    public void testUpsertSelectOnDescToAsc() throws Exception {
        byte[][] splits = new byte[][]{PInteger.INSTANCE.toBytes(1),
                PInteger.INSTANCE.toBytes(2), PInteger.INSTANCE.toBytes(3),
                PInteger.INSTANCE.toBytes(4)};
        String tableName = generateUniqueName();
        createTestTable(getUrl(), "create table " + tableName +
                " (i integer not null primary key desc, j integer)", splits, null);
        Properties props = new Properties();
        props.setProperty(QueryServices.ENABLE_SERVER_SIDE_UPSERT_MUTATIONS,
                allowServerSideMutations);
        ResultSet rs;
        int rowsInserted;
        String upsert = "UPSERT INTO " + tableName + " VALUES(1, 1)";

        try (Connection conn = DriverManager.getConnection(getUrl(), props);
             PreparedStatement upsertStmt = conn.prepareStatement(upsert)) {
            rowsInserted = upsertStmt.executeUpdate();
            assertEquals(1, rowsInserted);
            conn.commit();
        }

        upsert = "UPSERT INTO " + tableName + " (i,j) select i+1, j+1 from " + tableName;
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(true); // Force to run on server side.
            try (PreparedStatement upsertStmt = conn.prepareStatement(upsert)) {
                rowsInserted = upsertStmt.executeUpdate();
                assertEquals(1, rowsInserted);
            }
            conn.commit();
        }

        String select = "SELECT i,j FROM " + tableName;
        try (Connection conn = DriverManager.getConnection(getUrl(), props);
             Statement stmt = conn.createStatement()) {
            rs = stmt.executeQuery(select);
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            assertEquals(2, rs.getInt(2));
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertEquals(1, rs.getInt(2));
            assertFalse(rs.next());
        }
    }

    @Test
    public void testUpsertSelectRowKeyMutationOnSplitedTable() throws Exception {
        byte[][] splits = new byte[][]{PInteger.INSTANCE.toBytes(1),
                PInteger.INSTANCE.toBytes(2), PInteger.INSTANCE.toBytes(3),
                PInteger.INSTANCE.toBytes(4)};
        String tableName = generateUniqueName();
        ensureTableCreated(getUrl(), tableName, "IntKeyTest", splits, null, null);
        Properties props = new Properties();
        props.setProperty(QueryServices.ENABLE_SERVER_SIDE_UPSERT_MUTATIONS,
                allowServerSideMutations);
        int rowsInserted;
        ResultSet rs;

        String upsert = "UPSERT INTO " + tableName + " VALUES(?)";
        try (Connection conn = DriverManager.getConnection(getUrl(), props);
             PreparedStatement upsertStmt = conn.prepareStatement(upsert)) {
            upsertStmt.setInt(1, 1);
            upsertStmt.executeUpdate();
            upsertStmt.setInt(1, 3);
            upsertStmt.executeUpdate();
            conn.commit();
        }

        upsert = "UPSERT INTO " + tableName + " (i) SELECT i+1 from " + tableName;
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            // Normally this would force a server side update. But since this changes the
            // PK column, it would for to run on the client side.
            conn.setAutoCommit(true);
            try (PreparedStatement upsertStmt = conn.prepareStatement(upsert)) {
                rowsInserted = upsertStmt.executeUpdate();
                assertEquals(2, rowsInserted);
            }
            conn.commit();
        }

        String select = "SELECT i FROM " + tableName;
        try (Connection conn = DriverManager.getConnection(getUrl(), props);
             Statement stmt = conn.createStatement()) {
            rs = stmt.executeQuery(select);
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertTrue(rs.next());
            assertTrue(rs.next());
            assertTrue(rs.next());
            assertEquals(4, rs.getInt(1));
            assertFalse(rs.next());
        }
    }

    @Test
    public void testUpsertSelectWithLimit() throws Exception {
        Properties props = new Properties();
        props.setProperty(QueryServices.ENABLE_SERVER_SIDE_UPSERT_MUTATIONS,
                allowServerSideMutations);
        String tableName = generateUniqueName();
        ResultSet rs;
        try (Connection conn = DriverManager.getConnection(getUrl(), props);
             Statement stmt = conn.createStatement()) {
            stmt.execute("create table " + tableName +
                    " (id varchar(10) not null primary key, val varchar(10), ts timestamp)");
        }

        try (Connection conn = DriverManager.getConnection(getUrl(), props);
             Statement stmt = conn.createStatement()) {
            stmt.execute("upsert into " + tableName +
                    " values ('aaa', 'abc', current_date())");
            stmt.execute("upsert into " + tableName +
                    " values ('bbb', 'bcd', current_date())");
            stmt.execute("upsert into " + tableName +
                    " values ('ccc', 'cde', current_date())");
            conn.commit();
        }

        try (Connection conn = DriverManager.getConnection(getUrl(), props);
             Statement stmt = conn.createStatement()) {
            rs = stmt.executeQuery("select * from " + tableName);

            assertTrue(rs.next());
            assertEquals("aaa", rs.getString(1));
            assertEquals("abc", rs.getString(2));
            assertNotNull(rs.getDate(3));

            assertTrue(rs.next());
            assertEquals("bbb", rs.getString(1));
            assertEquals("bcd", rs.getString(2));
            assertNotNull(rs.getDate(3));

            assertTrue(rs.next());
            assertEquals("ccc", rs.getString(1));
            assertEquals("cde", rs.getString(2));
            assertNotNull(rs.getDate(3));

            assertFalse(rs.next());
        }

        try (Connection conn = DriverManager.getConnection(getUrl(), props);
             Statement stmt = conn.createStatement()) {
            stmt.execute("upsert into " + tableName +
                    " (id, ts) select id, CAST(null AS timestamp) from " + tableName +
                    " where id <= 'bbb' limit 1");
            conn.commit();
        }

        try (Connection conn = DriverManager.getConnection(getUrl(), props);
             Statement stmt = conn.createStatement()) {
            rs = stmt.executeQuery("select * from " + tableName);
            assertTrue(rs.next());
            assertEquals("aaa", rs.getString(1));
            assertEquals("abc", rs.getString(2));
            assertNull(rs.getDate(3));

            assertTrue(rs.next());
            assertEquals("bbb", rs.getString(1));
            assertEquals("bcd", rs.getString(2));
            assertNotNull(rs.getDate(3));

            assertTrue(rs.next());
            assertEquals("ccc", rs.getString(1));
            assertEquals("cde", rs.getString(2));
            assertNotNull(rs.getDate(3));

            assertFalse(rs.next());
        }
    }

    @Test
    public void testUpsertSelectWithSequence() throws Exception {
        Properties props = new Properties();
        props.setProperty(QueryServices.ENABLE_SERVER_SIDE_UPSERT_MUTATIONS,
                allowServerSideMutations);
        String t1 = generateUniqueName();
        String t2 = generateUniqueName();
        String seq = generateUniqueName();

        try (Connection conn = DriverManager.getConnection(getUrl(), props);
             Statement stmt = conn.createStatement()) {
            stmt.execute("create table  " + t1 +
                    " (id bigint not null primary key, v varchar)");
            stmt.execute("create table " + t2 + " (k varchar primary key)");
            stmt.execute("create sequence " + seq);
        }

        try (Connection conn = DriverManager.getConnection(getUrl(), props);
             Statement stmt = conn.createStatement()) {
            stmt.execute("upsert into " + t2 + " values ('a')");
            stmt.execute("upsert into " + t2 + " values ('b')");
            stmt.execute("upsert into " + t2 + " values ('c')");
            conn.commit();
        }

        try (Connection conn = DriverManager.getConnection(getUrl(), props);
             Statement stmt = conn.createStatement()) {
            stmt.execute("upsert into " + t1 + " select next value for  " +
                    seq + " , k from " + t2);
            conn.commit();
        }

        try (Connection conn = DriverManager.getConnection(getUrl(), props);
             Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select * from " + t1);

            assertTrue(rs.next());
            assertEquals(1, rs.getLong(1));
            assertEquals("a", rs.getString(2));

            assertTrue(rs.next());
            assertEquals(2, rs.getLong(1));
            assertEquals("b", rs.getString(2));

            assertTrue(rs.next());
            assertEquals(3, rs.getLong(1));
            assertEquals("c", rs.getString(2));

            assertFalse(rs.next());
        }
    }

    @Test
    public void testUpsertSelectWithSequenceAndOrderByWithSalting() throws Exception {
        int numOfRecords = 200;
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(QueryServices.ENABLE_SERVER_SIDE_UPSERT_MUTATIONS,
                allowServerSideMutations);
        String t1 = generateUniqueName();
        String t2 = generateUniqueName();
        String ddl = "CREATE TABLE IF NOT EXISTS " + t1 + "(ORGANIZATION_ID CHAR(15) NOT NULL, " +
                "QUERY_ID CHAR(15) NOT NULL, CURSOR_ORDER BIGINT NOT NULL, K1 INTEGER, " +
                "V1 INTEGER " + "CONSTRAINT MAIN_PK PRIMARY KEY (ORGANIZATION_ID, QUERY_ID, " +
                "CURSOR_ORDER) " + ") SALT_BUCKETS = 4";

        try (Connection conn = DriverManager.getConnection(getUrl(), props);
             Statement stmt = conn.createStatement()) {
            stmt.execute(ddl);
            stmt.execute(
                    "CREATE TABLE " + t2 + "(ORGANIZATION_ID CHAR(15) NOT NULL, k1 integer " +
                            "NOT NULL, v1 integer NOT NULL CONSTRAINT PK PRIMARY KEY " +
                            "(ORGANIZATION_ID, k1, v1) ) VERSIONS=1, SALT_BUCKETS = 4");
            stmt.execute("create sequence s cache " + Integer.MAX_VALUE);
        }

        try (Connection conn = DriverManager.getConnection(getUrl(), props);
             Statement stmt = conn.createStatement()) {
            for (int i = 0; i < numOfRecords; i++) {
                stmt.execute("UPSERT INTO " + t2 +
                        " values ('00Dxx0000001gEH'," + i + "," + (i + 2) + ")");
            }
            conn.commit();
        }

        try (Connection conn = DriverManager.getConnection(getUrl(), props);
             Statement stmt = conn.createStatement()) {
            conn.setAutoCommit(true);
            stmt.execute("UPSERT INTO " + t1 +
                    " SELECT '00Dxx0000001gEH', 'MyQueryId', NEXT VALUE FOR S, k1, v1  FROM " +
                    t2 + " ORDER BY K1, V1");

        }

        try (Connection conn = DriverManager.getConnection(getUrl(), props);
             Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select count(*) from " + t1);

            assertTrue(rs.next());
            assertEquals(numOfRecords, rs.getLong(1));

            ResultSet rs2 = stmt.executeQuery("select cursor_order, k1, v1 from " + t1 +
                    " order by cursor_order");
            long seq = 1;
            while (rs2.next()) {
                assertEquals(seq, rs2.getLong("cursor_order"));
                // This value should be the sequence - 1 as we said order by k1 in the
                // UPSERT...SELECT, but is not because of sequence processing.
                assertEquals(seq - 1, rs2.getLong("k1"));
                seq++;
            }
            // cleanup afrer ourselves
            stmt.execute("drop sequence s");
        }
    }

    @Test
    public void testUpsertSelectWithRowtimeStampColumn() throws Exception {
        String t1 = generateUniqueName();
        String t2 = generateUniqueName();
        String t3 = generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl());
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE " + t1 +
                    " (PK1 VARCHAR NOT NULL, PK2 DATE NOT NULL, KV1 VARCHAR CONSTRAINT PK " +
                    "PRIMARY KEY(PK1, PK2 DESC ROW_TIMESTAMP " + ")) ");
            stmt.execute("CREATE TABLE " + t2 +
                    " (PK1 VARCHAR NOT NULL, PK2 DATE NOT NULL, KV1 VARCHAR CONSTRAINT PK " +
                    "PRIMARY KEY(PK1, PK2 ROW_TIMESTAMP)) ");
            stmt.execute("CREATE TABLE " + t3 + " (PK1 VARCHAR NOT NULL, " +
                    "PK2 DATE NOT NULL, KV1 VARCHAR CONSTRAINT PK " +
                    "PRIMARY KEY(PK1, PK2 DESC ROW_TIMESTAMP " + ")) ");
        }

        // The timestamp of the put will be the value of the row_timestamp column.
        long rowTimestamp = EnvironmentEdgeManager.currentTimeMillis();
        Date rowTimestampDate = new Date(rowTimestamp);
        Properties props = new Properties();
        props.setProperty(QueryServices.ENABLE_SERVER_SIDE_UPSERT_MUTATIONS,
                allowServerSideMutations);
        try (Connection conn = DriverManager.getConnection(getUrl(), props);
             PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + t1 +
                     " (PK1, PK2, KV1) VALUES(?, ?, ?)")) {
            stmt.setString(1, "PK1");
            stmt.setDate(2, rowTimestampDate);
            stmt.setString(3, "KV1");
            stmt.executeUpdate();
            conn.commit();
        }

        // Upsert select data into table T2. The connection needs to be at a timestamp beyond the
        // row timestamp. Otherwise it won't see the data from table T1.
        try (Connection conn = DriverManager.getConnection(getUrl());
             Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("UPSERT INTO " + t2 + " SELECT * FROM " + t1);
            conn.commit();
            // Verify the data upserted in T2. Note that we can use the same connection here because
            // the data was inserted with a timestamp of rowTimestamp and the connection uses
            // the latest timestamp
            try (PreparedStatement prepStmt = conn.prepareStatement("SELECT * FROM " + t2 +
                    " WHERE PK1 = ? AND PK2 = ?")) {
                prepStmt.setString(1, "PK1");
                prepStmt.setDate(2, rowTimestampDate);
                ResultSet rs = prepStmt.executeQuery();
                assertTrue(rs.next());
                assertEquals("PK1", rs.getString("PK1"));
                assertEquals(rowTimestampDate, rs.getDate("PK2"));
                assertEquals("KV1", rs.getString("KV1"));
            }

        }

        // Verify that you can't see the data in T2 if the connection is at a timestamp
        // lower than the row timestamp.
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(rowTimestamp - 1));
        try (Connection conn = DriverManager.getConnection(getUrl(), props);
             PreparedStatement stmt = conn.prepareStatement("SELECT * FROM " + t2 +
                     " WHERE PK1 = ? AND PK2 = ?")) {
            stmt.setString(1, "PK1");
            stmt.setDate(2, rowTimestampDate);
            ResultSet rs = stmt.executeQuery();
            assertFalse(rs.next());
        }

        // Upsert select data into table T3. The connection needs to be at a timestamp beyond the
        // row timestamp. Otherwise it won't see the data from table T1.
        try (Connection conn = DriverManager.getConnection(getUrl());
             Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("UPSERT INTO " + t3 + " SELECT * FROM " + t1);
            conn.commit();
            // Verify the data upserted in T3. Note that we can use the same connection here
            // because the data was inserted with a timestamp of rowTimestamp and the connection
            // uses the latest timestamp
            try (PreparedStatement prepStmt = conn.prepareStatement("SELECT * FROM " + t3 +
                    " WHERE PK1 = ? AND PK2 = ?")) {
                prepStmt.setString(1, "PK1");
                prepStmt.setDate(2, rowTimestampDate);
                ResultSet rs = prepStmt.executeQuery();
                assertTrue(rs.next());
                assertEquals("PK1", rs.getString("PK1"));
                assertEquals(rowTimestampDate, rs.getDate("PK2"));
                assertEquals("KV1", rs.getString("KV1"));
            }
        }

        // Verify that you can't see the data in T2 if the connection is at next timestamp
        // (which is lower than the row timestamp).
        try (Connection conn = DriverManager.getConnection(getUrl(), props);
             PreparedStatement stmt = conn.prepareStatement("SELECT * FROM " + t3 +
                     " WHERE PK1 = ? AND PK2 = ?")) {
            stmt.setString(1, "PK1");
            stmt.setDate(2, rowTimestampDate);
            ResultSet rs = stmt.executeQuery();
            assertFalse(rs.next());
        }
    }

    @Test
    public void testUpsertSelectSameTableWithRowTimestampColumn() throws Exception {
        String tableName = generateUniqueName();
        Properties props = new Properties();
        props.setProperty(QueryServices.ENABLE_SERVER_SIDE_UPSERT_MUTATIONS,
                allowServerSideMutations);
        try (Connection conn = DriverManager.getConnection(getUrl(), props);
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE " + tableName +
                    " (PK1 INTEGER NOT NULL, PK2 DATE NOT NULL, KV1 VARCHAR CONSTRAINT PK " +
                    "PRIMARY KEY(PK1, PK2 ROW_TIMESTAMP)) ");
        }

        // The timestamp of the put will be the value of the row_timestamp column.
        long rowTimestamp = 100;
        Date rowTimestampDate = new Date(rowTimestamp);
        try (Connection conn = DriverManager.getConnection(getUrl(), props);
             PreparedStatement stmt = conn.prepareStatement("UPSERT INTO  " + tableName +
                     " (PK1, PK2, KV1) VALUES(?, ?, ?)")) {
            stmt.setInt(1, 1);
            stmt.setDate(2, rowTimestampDate);
            stmt.setString(3, "KV1");
            stmt.executeUpdate();
            conn.commit();
        }
        String seq = generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl(), props);
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE SEQUENCE " + seq);
        }
        // Upsert select data into table. The connection needs to be at a timestamp beyond the
        // row timestamp. Otherwise it won't see the data from table.
        try (Connection conn = DriverManager.getConnection(getUrl(), props);
             Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("UPSERT INTO  " + tableName +
                    "  SELECT NEXT VALUE FOR " + seq + ", PK2 FROM  " + tableName);
            conn.commit();
        }

        // Upsert select using sequences.
        try (Connection conn = DriverManager.getConnection(getUrl(), props);
             Statement stmt = conn.createStatement()) {
            conn.setAutoCommit(true);
            for (int i = 0; i < 10; i++) {
                int count = stmt.executeUpdate("UPSERT INTO  " + tableName +
                        "  SELECT NEXT VALUE FOR " + seq + ", PK2 FROM  " + tableName);
                assertEquals((int) Math.pow(2, i), count);
            }
        }
    }

    @Test
    public void testAutomaticallySettingRowtimestamp() throws Exception {
        String table1 = generateUniqueName();
        String table2 = generateUniqueName();
        String table3 = generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl());
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE " + table1 +
                    " (T1PK1 VARCHAR NOT NULL, T1PK2 DATE NOT NULL, T1KV1 VARCHAR, T1KV2 VARCHAR " +
                    "CONSTRAINT PK PRIMARY KEY(T1PK1, T1PK2 DESC ROW_TIMESTAMP)) ");
            stmt.execute("CREATE TABLE " + table2 +
                    " (T2PK1 VARCHAR NOT NULL, T2PK2 DATE NOT NULL, T2KV1 VARCHAR, T2KV2 VARCHAR" +
                    " CONSTRAINT PK PRIMARY KEY(T2PK1, T2PK2 ROW_TIMESTAMP)) ");
            stmt.execute("CREATE TABLE " + table3 +
                    " (T3PK1 VARCHAR NOT NULL, T3PK2 DATE NOT NULL, T3KV1 VARCHAR, T3KV2 VARCHAR" +
                    " CONSTRAINT PK PRIMARY KEY(T3PK1, T3PK2 DESC ROW_TIMESTAMP)) ");
        }
        long startTime = EnvironmentEdgeManager.currentTimeMillis();
        Properties props = new Properties();
        props.setProperty(QueryServices.ENABLE_SERVER_SIDE_UPSERT_MUTATIONS,
                allowServerSideMutations);
        try (Connection conn = DriverManager.getConnection(getUrl(), props);
             PreparedStatement stmt = conn.prepareStatement("UPSERT INTO  " + table1 +
                     " (T1PK1, T1KV1, T1KV2) VALUES (?, ?, ?)")) {
            // Upsert values where row_timestamp column PK2 is not set and the column names
            // are specified. This should upsert data with the value for PK2 as server timestamp
            stmt.setString(1, "PK1");
            stmt.setString(2, "KV1");
            stmt.setString(3, "KV2");
            stmt.executeUpdate();
            conn.commit();
        }
        long endTime = EnvironmentEdgeManager.currentTimeMillis();

        try (Connection conn = DriverManager.getConnection(getUrl(), props);
             PreparedStatement stmt = conn.prepareStatement("SELECT T1KV1, T1KV2 FROM " +
                     table1 + " WHERE T1PK1 = ? AND T1PK2 >= ? AND T1PK2 <= ?")) {
            // Now query for data that was upserted above. If the row key was generated correctly
            // then we should be able to see the data in this query.
            stmt.setString(1, "PK1");
            stmt.setDate(2, new Date(startTime));
            stmt.setDate(3, new Date(endTime));
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals("KV1", rs.getString(1));
            assertEquals("KV2", rs.getString(2));
            assertFalse(rs.next());
        }

        startTime = EnvironmentEdgeManager.currentTimeMillis();
        try (Connection conn = DriverManager.getConnection(getUrl(), props);
             PreparedStatement stmt = conn.prepareStatement("UPSERT INTO  " + table2 +
                     " (T2PK1, T2KV1, T2KV2) SELECT T1PK1, T1KV1, T1KV2 FROM " + table1)) {
            // Upsert select into table2 by not selecting the row timestamp column. In this case,
            // the rowtimestamp column would end up being set to the server timestamp
            stmt.executeUpdate();
            conn.commit();
        }
        endTime = EnvironmentEdgeManager.currentTimeMillis();

        try (Connection conn = DriverManager.getConnection(getUrl(), props);
             PreparedStatement stmt = conn.prepareStatement("SELECT T2KV1, T2KV2 FROM " +
                     table2 + " WHERE T2PK1 = ? AND T2PK2 >= ?  AND T2PK2 <= ?")) {
            // Now query for data that was upserted above. If the row key was generated correctly
            // then we should be able to see the data in this query.
            stmt.setString(1, "PK1");
            stmt.setDate(2, new Date(startTime));
            stmt.setDate(3, new Date(endTime));
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals("KV1", rs.getString(1));
            assertEquals("KV2", rs.getString(2));
            assertFalse(rs.next());
        }

        startTime = EnvironmentEdgeManager.currentTimeMillis();
        try (Connection conn = DriverManager.getConnection(getUrl(), props);
             PreparedStatement stmt = conn.prepareStatement("UPSERT INTO  " + table3 +
                     " (T3PK1, T3KV1, T3KV2) SELECT T2PK1, T2KV1, T2KV2 FROM " + table2)) {
            // Upsert select into table3 by not selecting the row timestamp column. In this case,
            // the rowtimestamp column would end up being set to the server timestamp
            stmt.executeUpdate();
            conn.commit();
        }
        endTime = EnvironmentEdgeManager.currentTimeMillis();

        try (Connection conn = DriverManager.getConnection(getUrl(), props);
             PreparedStatement stmt = conn.prepareStatement("SELECT T3KV1, T3KV2 FROM " +
                     table3 + " WHERE T3PK1 = ? AND T3PK2 >= ? AND T3PK2 <= ?")) {
            // Now query for data that was upserted above. If the row key was generated correctly
            // then we should be able to see the data in this query.
            stmt.setString(1, "PK1");
            stmt.setDate(2, new Date(startTime));
            stmt.setDate(3, new Date(endTime));
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals("KV1", rs.getString(1));
            assertEquals("KV2", rs.getString(2));
            assertFalse(rs.next());
        }
    }

    @Test
    public void testUpsertSelectAutoCommitWithRowTimestampColumn() throws Exception {
        String tableName1 = generateUniqueName();
        String tableName2 = generateUniqueName();
        Properties props = new Properties();
        props.setProperty(QueryServices.ENABLE_SERVER_SIDE_UPSERT_MUTATIONS,
                allowServerSideMutations);
        try (Connection conn = DriverManager.getConnection(getUrl(), props);
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE " + tableName1 +
                    " (PK1 INTEGER NOT NULL, PK2 DATE NOT NULL, PK3 INTEGER NOT NULL, KV1 VARCHAR" +
                    " CONSTRAINT PK PRIMARY KEY(PK1, PK2 ROW_TIMESTAMP, PK3)) ");
            stmt.execute("CREATE TABLE " + tableName2 +
                    " (PK1 INTEGER NOT NULL, PK2 DATE NOT NULL, PK3 INTEGER NOT NULL, KV1 VARCHAR" +
                    " CONSTRAINT PK PRIMARY KEY(PK1, PK2 DESC ROW_TIMESTAMP, PK3)) ");
        }

        String[] tableNames = {tableName1, tableName2};
        for (String tableName : tableNames) {
            // Upsert data with the row timestamp value set
            long rowTimestamp1 = 100;
            Date rowTimestampDate = new Date(rowTimestamp1);
            try (Connection conn = DriverManager.getConnection(getUrl(), props);
                 PreparedStatement stmt = conn.prepareStatement("UPSERT INTO  " +
                         tableName + " (PK1, PK2, PK3, KV1) VALUES(?, ?, ?, ?)")) {
                stmt.setInt(1, 1);
                stmt.setDate(2, rowTimestampDate);
                stmt.setInt(3, 3);
                stmt.setString(4, "KV1");
                stmt.executeUpdate();
                conn.commit();
            }

            long startTime = EnvironmentEdgeManager.currentTimeMillis();
            try (Connection conn = DriverManager.getConnection(getUrl(), props);
                 Statement stmt = conn.createStatement()) {
                conn.setAutoCommit(true);
                // Upsert select in the same table with the row_timestamp column PK2 not specified. 
                // This will end up creating a new row whose timestamp is the server time stamp 
                // which is also used for the row key
                stmt.executeUpdate("UPSERT INTO  " + tableName +
                        " (PK1, PK3, KV1) SELECT PK1, PK3, KV1 FROM  " + tableName);
            }
            long endTime = EnvironmentEdgeManager.currentTimeMillis();

            try (Connection conn = DriverManager.getConnection(getUrl(), props);
                 PreparedStatement stmt = conn.prepareStatement("SELECT * FROM  " + tableName +
                         " WHERE PK1 = ? AND PK2 >= ? AND PK2<= ? AND PK3 = ?")) {
                // Verify the row that was upserted above
                stmt.setInt(1, 1);
                stmt.setDate(2, new Date(startTime));
                stmt.setDate(3, new Date(endTime));
                stmt.setInt(4, 3);
                ResultSet rs = stmt.executeQuery();
                assertTrue(rs.next());
                assertEquals(1, rs.getInt("PK1"));
                assertEquals(3, rs.getInt("PK3"));
                assertEquals("KV1", rs.getString("KV1"));
                assertFalse(rs.next());
                // Number of rows in the table should be 2.
                try (Statement newStmt = conn.createStatement()) {
                    rs = newStmt.executeQuery("SELECT COUNT(*) FROM " + tableName);
                    assertTrue(rs.next());
                    assertEquals(2, rs.getInt(1));
                }

            }
            try (Connection conn = DriverManager.getConnection(getUrl(), props);
                 Statement stmt = conn.createStatement()) {
                conn.setAutoCommit(true);
                // Upsert select in the same table with the row_timestamp column PK2 specified.
                // This will not end up creating a new row because the destination pk columns,
                // including the row timestamp column PK2, are the same as the source column.
                stmt.executeUpdate("UPSERT INTO  " + tableName +
                        " (PK1, PK2, PK3, KV1) SELECT PK1, PK2, PK3, KV1 FROM  " + tableName);
            }
            try (Connection conn = DriverManager.getConnection(getUrl(), props);
                 Statement stmt = conn.createStatement()) {
                // Verify that two rows were created. One with rowtimestamp1 and the other
                // with rowtimestamp2
                ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + tableName);
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1));
                assertFalse(rs.next());
            }

        }
    }

    @Test
    public void testRowTimestampColWithViewsIndexesAndSaltedTables() throws Exception {
        String baseTable = generateUniqueName();
        String tenantView = generateUniqueName();
        String globalView = generateUniqueName();
        String baseTableIdx = generateUniqueName();
        String tenantViewIdx = generateUniqueName();

        Properties props = new Properties();
        props.setProperty(QueryServices.ENABLE_SERVER_SIDE_UPSERT_MUTATIONS,
                allowServerSideMutations);
        try (Connection conn = DriverManager.getConnection(getUrl(), props);
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE IMMUTABLE TABLE " + baseTable +
                    " (TENANT_ID CHAR(15) NOT NULL, PK2 DATE NOT NULL, PK3 INTEGER NOT NULL, " +
                    "KV1 VARCHAR, KV2 VARCHAR, KV3 VARCHAR CONSTRAINT PK PRIMARY KEY(TENANT_ID, " +
                    "PK2 ROW_TIMESTAMP, PK3)) MULTI_TENANT = true, SALT_BUCKETS = 8");
            stmt.execute("CREATE INDEX " + baseTableIdx + " ON " +
                    baseTable + " (PK2, KV3) INCLUDE (KV1)");
            stmt.execute("CREATE VIEW " + globalView + " AS SELECT * FROM " +
                    baseTable + " WHERE KV1 = 'KV1'");
        }

        String tenantId = "tenant1";
        try (Connection conn = getTenantConnection(tenantId);
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE VIEW " + tenantView + " AS SELECT * FROM " +
                    baseTable);
            stmt.execute("CREATE INDEX " + tenantViewIdx + " ON " +
                    tenantView + " (PK2, KV2) INCLUDE (KV1)");
        }

        // upsert data into base table without specifying the row timestamp column PK2
        long startTime = EnvironmentEdgeManager.currentTimeMillis();
        try (Connection conn = DriverManager.getConnection(getUrl(), props);
             PreparedStatement stmt = conn.prepareStatement("UPSERT INTO  " + baseTable +
                     " (TENANT_ID, PK3, KV1, KV2, KV3) VALUES (?, ?, ?, ?, ?)")) {
            // Upsert select in the same table with the row_timestamp column PK2 not specified.
            // This will end up creating a new row whose timestamp is the latest timestamp
            // (which will be used for the row key too)
            stmt.setString(1, tenantId);
            stmt.setInt(2, 3);
            stmt.setString(3, "KV1");
            stmt.setString(4, "KV2");
            stmt.setString(5, "KV3");
            stmt.executeUpdate();
            conn.commit();
        }
        long endTime = EnvironmentEdgeManager.currentTimeMillis();

        // Verify that we can see data when querying through base table, global view and index on
        // the base table
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            // Query the base table
            try (PreparedStatement stmt = conn.prepareStatement("SELECT * FROM  " + baseTable +
                    " WHERE TENANT_ID = ? AND PK2 >= ? AND PK2 <= ? AND PK3 = ?")) {
                stmt.setString(1, tenantId);
                stmt.setDate(2, new Date(startTime));
                stmt.setDate(3, new Date(endTime));
                stmt.setInt(4, 3);
                ResultSet rs = stmt.executeQuery();
                assertTrue(rs.next());
                assertEquals(tenantId, rs.getString("TENANT_ID"));
                assertEquals("KV1", rs.getString("KV1"));
                assertEquals("KV2", rs.getString("KV2"));
                assertEquals("KV3", rs.getString("KV3"));
                assertFalse(rs.next());
            }

            // Query the globalView
            try (PreparedStatement stmt = conn.prepareStatement(
                    "SELECT /*+ NO_INDEX */ * FROM  " + globalView +
                            " WHERE TENANT_ID = ? AND PK2 >= ? AND PK2 <= ? AND PK3 = ?")) {
                stmt.setString(1, tenantId);
                stmt.setDate(2, new Date(startTime));
                stmt.setDate(3, new Date(endTime));
                stmt.setInt(4, 3);
                ResultSet rs = stmt.executeQuery();
                assertTrue(rs.next());
                assertEquals(tenantId, rs.getString("TENANT_ID"));
                assertEquals("KV1", rs.getString("KV1"));
                assertEquals("KV2", rs.getString("KV2"));
                assertEquals("KV3", rs.getString("KV3"));
                assertFalse(rs.next());
            }

            // Query using the index on base table
            try (PreparedStatement stmt = conn.prepareStatement("SELECT KV1 FROM  " +
                    baseTable + " WHERE PK2 >= ? AND PK2 <= ? AND KV3 = ?")) {
                stmt.setDate(1, new Date(startTime));
                stmt.setDate(2, new Date(endTime));
                stmt.setString(3, "KV3");
                ResultSet rs = stmt.executeQuery();
                QueryPlan plan = stmt.unwrap(PhoenixStatement.class).getQueryPlan();
                assertEquals(plan.getTableRef().getTable().getName().getString(), baseTableIdx);
                assertTrue(rs.next());
                assertEquals("KV1", rs.getString("KV1"));
                assertFalse(rs.next());
            }
        }

        // Verify that data can be queried using tenant view and tenant view index
        try (Connection tenantConn = getTenantConnection(tenantId);
             PreparedStatement stmt = tenantConn.prepareStatement("SELECT * FROM  " +
                     tenantView + " WHERE PK2 >= ? AND PK2 <= ? AND PK3 = ?")) {
            // Query the tenant view
            stmt.setDate(1, new Date(startTime));
            stmt.setDate(2, new Date(endTime));
            stmt.setInt(3, 3);
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals("KV1", rs.getString("KV1"));
            assertEquals("KV2", rs.getString("KV2"));
            assertEquals("KV3", rs.getString("KV3"));
            assertFalse(rs.next());

            // Query using the index on the tenantView
            //TODO: uncomment the code after PHOENIX-2277 is fixed
//            stmt = tenantConn.prepareStatement("SELECT KV1 FROM  " + tenantView +
//                    " WHERE PK2 = ? AND KV2 = ?");
//            stmt.setDate(1, new Date(upsertedTs));
//            stmt.setString(2, "KV2");
//            rs = stmt.executeQuery();
//            QueryPlan plan = stmt.unwrap(PhoenixStatement.class).getQueryPlan();
//            assertTrue(plan.getTableRef().getTable().getName().getString().equals(tenantViewIdx));
//            assertTrue(rs.next());
//            assertEquals("KV1", rs.getString("KV1"));
//            assertFalse(rs.next());
        }

        long upsertedTs;
        try (Connection tenantConn = getTenantConnection(tenantId)) {
            // Upsert into tenant view where the row_timestamp column PK2 is not specified
            startTime = EnvironmentEdgeManager.currentTimeMillis();
            try (PreparedStatement stmt = tenantConn.prepareStatement("UPSERT INTO  " +
                    tenantView + " (PK3, KV1, KV2, KV3) VALUES (?, ?, ?, ?)")) {
                stmt.setInt(1, 33);
                stmt.setString(2, "KV13");
                stmt.setString(3, "KV23");
                stmt.setString(4, "KV33");
                stmt.executeUpdate();
            }
            tenantConn.commit();
            upsertedTs = endTime = EnvironmentEdgeManager.currentTimeMillis();

            // Upsert into tenant view where the row_timestamp column PK2 is specified
            try (PreparedStatement stmt = tenantConn.prepareStatement("UPSERT INTO  " +
                    tenantView + " (PK2, PK3, KV1, KV2, KV3) VALUES (?, ?, ?, ?, ?)")) {
                stmt.setDate(1, new Date(upsertedTs));
                stmt.setInt(2, 44);
                stmt.setString(3, "KV14");
                stmt.setString(4, "KV24");
                stmt.setString(5, "KV34");
                stmt.executeUpdate();
            }
            tenantConn.commit();
        }

        // Verify that the data upserted using the tenant view can now be queried using base table
        // and the base table index
        Date upsertedDate;
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            // Query the base table
            try (PreparedStatement stmt = conn.prepareStatement("SELECT * FROM  " + baseTable +
                    " WHERE TENANT_ID = ? AND PK2 >= ? AND PK2 <= ? AND PK3 = ? ")) {
                stmt.setString(1, tenantId);
                stmt.setDate(2, new Date(startTime));
                stmt.setDate(3, new Date(endTime));
                stmt.setInt(4, 33);
                ResultSet rs = stmt.executeQuery();
                assertTrue(rs.next());
                assertEquals(tenantId, rs.getString("TENANT_ID"));
                assertEquals("KV13", rs.getString("KV1"));
                assertEquals("KV23", rs.getString("KV2"));
                assertEquals("KV33", rs.getString("KV3"));
                upsertedDate = rs.getDate("PK2");
                assertFalse(rs.next());
            }

            try (PreparedStatement stmt = conn.prepareStatement("SELECT * FROM  " + baseTable +
                    " WHERE TENANT_ID = ? AND PK2 = ? AND PK3 = ? ")) {
                stmt.setString(1, tenantId);
                stmt.setDate(2, new Date(upsertedTs));
                stmt.setInt(3, 44);
                ResultSet rs = stmt.executeQuery();
                assertTrue(rs.next());
                assertEquals(tenantId, rs.getString("TENANT_ID"));
                assertEquals("KV14", rs.getString("KV1"));
                assertEquals("KV24", rs.getString("KV2"));
                assertEquals("KV34", rs.getString("KV3"));
                assertFalse(rs.next());
            }

            // Query using the index on base table
            try (PreparedStatement stmt = conn.prepareStatement("SELECT KV1 FROM  " + baseTable +
                    " WHERE (PK2, KV3) IN ((?, ?), (?, ?)) ORDER BY KV1")) {
                stmt.setDate(1, upsertedDate);
                stmt.setString(2, "KV33");
                stmt.setDate(3, new Date(upsertedTs));
                stmt.setString(4, "KV34");
                ResultSet rs = stmt.executeQuery();
                QueryPlan plan = stmt.unwrap(PhoenixStatement.class).getQueryPlan();
                assertTrue(plan.getTableRef().getTable().getName().getString().equals(baseTableIdx));
                assertTrue(rs.next());
                assertEquals("KV13", rs.getString("KV1"));
                assertTrue(rs.next());
                assertEquals("KV14", rs.getString("KV1"));
                assertFalse(rs.next());
            }
        }

        // Verify that the data upserted using the tenant view can now be queried using tenant view
        try (Connection tenantConn = getTenantConnection(tenantId);
             PreparedStatement stmt = tenantConn.prepareStatement("SELECT * FROM  " +
                     tenantView + " WHERE (PK2, PK3) IN ((?, ?), (?, ?)) ORDER BY KV1")) {
            // Query the base table
            stmt.setDate(1, upsertedDate);
            stmt.setInt(2, 33);
            stmt.setDate(3, new Date(upsertedTs));
            stmt.setInt(4, 44);
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals("KV13", rs.getString("KV1"));
            assertTrue(rs.next());
            assertEquals("KV14", rs.getString("KV1"));
            assertFalse(rs.next());

            //TODO: uncomment the code after PHOENIX-2277 is fixed
//            // Query using the index on the tenantView
//            stmt = tenantConn.prepareStatement("SELECT KV1 FROM  " + tenantView +
//                    " WHERE (PK2, KV2) IN (?, ?, ?, ?) ORDER BY KV1");
//            stmt.setDate(1, new Date(upsertedTs));
//            stmt.setString(2, "KV23");
//            stmt.setDate(3, new Date(upsertedTs));
//            stmt.setString(4, "KV24");
//            rs = stmt.executeQuery();
//            QueryPlan plan = stmt.unwrap(PhoenixStatement.class).getQueryPlan();
//            assertTrue(plan.getTableRef().getTable().getName().getString().equals(tenantViewIdx));
//            assertTrue(rs.next());
//            assertEquals("KV13", rs.getString("KV1"));
//            assertTrue(rs.next());
//            assertEquals("KV14", rs.getString("KV1"));
//            assertFalse(rs.next());
        }
    }

    @Test
    public void testDisallowNegativeValuesForRowTsColumn() throws Exception {
        String tableName = generateUniqueName();
        String tableName2 = generateUniqueName();
        Properties props = new Properties();
        props.setProperty(QueryServices.ENABLE_SERVER_SIDE_UPSERT_MUTATIONS,
                allowServerSideMutations);
        try (Connection conn = DriverManager.getConnection(getUrl(), props);
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE " + tableName +
                    " (PK1 BIGINT NOT NULL PRIMARY KEY ROW_TIMESTAMP, KV1 VARCHAR)");
            stmt.execute("CREATE TABLE " + tableName2 +
                    " (PK1 BIGINT NOT NULL PRIMARY KEY ROW_TIMESTAMP, KV1 VARCHAR)");
        }
        long upsertedTs = 100;
        try (Connection conn = DriverManager.getConnection(getUrl(), props);
             PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + tableName +
                     " VALUES (?, ?)")) {
            stmt.setLong(1, upsertedTs);
            stmt.setString(2, "KV1");
            stmt.executeUpdate();
            conn.commit();
        }
        try (Connection conn = DriverManager.getConnection(getUrl(), props);
             PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + tableName2 +
                     " SELECT (PK1 - 500), KV1 FROM " + tableName)) {
            stmt.executeUpdate();
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.ILLEGAL_DATA.getErrorCode(), e.getErrorCode());
        }
    }

    @Test
    public void testUpsertSelectWithFixedWidthNullByteSizeArray() throws Exception {
        Properties props = new Properties();
        props.setProperty(QueryServices.ENABLE_SERVER_SIDE_UPSERT_MUTATIONS,
                allowServerSideMutations);
        String t1 = generateUniqueName();
        ResultSet rs;

        try (Connection conn = DriverManager.getConnection(getUrl(), props);
             Statement stmt = conn.createStatement()) {
            stmt.execute("create table " + t1 +
                    " (id bigint not null primary key, ca char(3)[])");
        }

        try (Connection conn = DriverManager.getConnection(getUrl(), props);
             Statement stmt = conn.createStatement()) {
            stmt.execute("upsert into " + t1 + " values (1, ARRAY['aaa', 'bbb'])");
            conn.commit();
        }

        try (Connection conn = DriverManager.getConnection(getUrl(), props);
             Statement stmt = conn.createStatement()) {
            stmt.execute("upsert into " + t1 + " (id, ca) select id, " +
                    "ARRAY['ccc', 'ddd'] from " + t1 + " WHERE id = 1");
            conn.commit();
        }

        try (Connection conn = DriverManager.getConnection(getUrl(), props);
             Statement stmt = conn.createStatement()) {
            rs = stmt.executeQuery("select * from " + t1);
            assertTrue(rs.next());
            assertEquals(1, rs.getLong(1));
            assertEquals("['ccc', 'ddd']", rs.getArray(2).toString());

        }

        String t2 = generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl(), props);
             Statement stmt = conn.createStatement()) {
            stmt.execute("create table " + t2 +
                    " (id bigint not null primary key, ba binary(4)[])");
        }

        try (Connection conn = DriverManager.getConnection(getUrl(), props);
             Statement stmt = conn.createStatement()) {
            stmt.execute("upsert into " + t2 + " values (2, ARRAY[1, 27])");
            conn.commit();
        }

        try (Connection conn = DriverManager.getConnection(getUrl(), props);
             Statement stmt = conn.createStatement()) {
            stmt.execute("upsert into " + t2 + " (id, ba) select id, " +
                    "ARRAY[54, 1024] from " + t2 + " WHERE id = 2");
            conn.commit();
        }

        try (Connection conn = DriverManager.getConnection(getUrl(), props);
             Statement stmt = conn.createStatement()) {
            rs = stmt.executeQuery("select * from " + t2);
            assertTrue(rs.next());
            assertEquals(2, rs.getLong(1));
            assertEquals("[[128,0,0,54], [128,0,4,0]]", rs.getArray(2).toString());
        }
    }

    @Test
    public void testUpsertSelectWithMultiByteCharsNoAutoCommit() throws Exception {
        testUpsertSelectWithMultiByteChars(false);
    }

    @Test
    public void testUpsertSelectWithMultiByteCharsAutoCommit() throws Exception {
        testUpsertSelectWithMultiByteChars(true);
    }

    private void testUpsertSelectWithMultiByteChars(boolean autoCommit) throws Exception {
        Properties props = new Properties();
        props.setProperty(QueryServices.ENABLE_SERVER_SIDE_UPSERT_MUTATIONS,
                allowServerSideMutations);
        String t1 = generateUniqueName();
        String validValue = "澴粖蟤य褻酃岤豦팑薰鄩脼ժ끦碉碉碉碉碉碉";
        String invalidValue = "澴粖蟤य褻酃岤豦팑薰鄩脼ժ끦碉碉碉碉碉碉碉";
        String columnTypeInfo = "VARCHAR(20)";

        try (Connection conn = DriverManager.getConnection(getUrl(), props);
             Statement stmt = conn.createStatement()) {
            conn.setAutoCommit(autoCommit);
            stmt.execute("create table " + t1 +
                    " (id bigint not null primary key, v varchar(20))");
        }

        try (Connection conn = DriverManager.getConnection(getUrl(), props);
             Statement stmt = conn.createStatement()) {
            conn.setAutoCommit(autoCommit);
            stmt.execute("upsert into " + t1 + " values (1, 'foo')");
            conn.commit();
        }

        try (Connection conn = DriverManager.getConnection(getUrl(), props);
             Statement stmt = conn.createStatement()) {
            conn.setAutoCommit(autoCommit);
            stmt.execute("upsert into " + t1 + " (id, v) select id, "
                    + "'" + validValue + "' from " + t1 + " WHERE id = 1");
            conn.commit();
        }

        try (Connection conn = DriverManager.getConnection(getUrl(), props);
             Statement stmt = conn.createStatement()) {
            conn.setAutoCommit(autoCommit);
            ResultSet rs = stmt.executeQuery("select * from  " + t1);

            assertTrue(rs.next());
            assertEquals(1, rs.getLong(1));
            assertEquals(validValue, rs.getString(2));
        }

        try (Connection conn = DriverManager.getConnection(getUrl(), props);
             Statement stmt = conn.createStatement()) {
            conn.setAutoCommit(autoCommit);
            stmt.execute("upsert into  " + t1 + " (id, v) select id, "
                    + "'" + invalidValue + "' from " + t1 + " WHERE id = 1");
            conn.commit();
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.DATA_EXCEEDS_MAX_CAPACITY.getErrorCode(), e.getErrorCode());
            assertTrue(e.getMessage().contains(columnTypeInfo));
        }
    }

    @Test
    public void testParallelUpsertSelect() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(QueryServices.MUTATE_BATCH_SIZE_BYTES_ATTRIB, Integer.toString(512));
        props.setProperty(QueryServices.SCAN_CACHE_SIZE_ATTRIB, Integer.toString(3));
        props.setProperty(QueryServices.SCAN_RESULT_CHUNK_SIZE, Integer.toString(3));
        props.setProperty(QueryServices.ENABLE_SERVER_SIDE_UPSERT_MUTATIONS,
                allowServerSideMutations);
        String t1 = generateUniqueName();
        String t2 = generateUniqueName();
        String seq = generateUniqueName();

        try (Connection conn = DriverManager.getConnection(getUrl(), props);
             Statement stmt = conn.createStatement()) {
            conn.setAutoCommit(false);

            stmt.execute("CREATE SEQUENCE " + seq);
            stmt.execute("CREATE TABLE  " + t1 +
                    "  (pk INTEGER PRIMARY KEY, val INTEGER) SALT_BUCKETS=4");
            stmt.execute("CREATE TABLE  " + t2 +
                    "  (pk INTEGER PRIMARY KEY, val INTEGER)");
        }

        try (Connection conn = DriverManager.getConnection(getUrl(), props);
             Statement stmt = conn.createStatement()) {
            for (int i = 0; i < 100; i++) {
                stmt.execute("UPSERT INTO  " + t1 +
                        "  VALUES (NEXT VALUE FOR " + seq + ", " + (i % 10) + ")");
            }
            conn.commit();
        }

        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(true);
            try (Statement stmt = conn.createStatement()) {
                int upsertCount = stmt.executeUpdate("UPSERT INTO " + t2 +
                        " SELECT pk, val FROM  " + t1);
                assertEquals(100, upsertCount);
            }
        }
    }

    @Test // See https://issues.apache.org/jira/browse/PHOENIX-4265
    public void testLongCodecUsedForRowTimestamp() throws Exception {
        String tableName = generateUniqueName();
        String indexName = generateUniqueName();
        Properties props = new Properties();
        props.setProperty(QueryServices.ENABLE_SERVER_SIDE_UPSERT_MUTATIONS,
                allowServerSideMutations);
        try (Connection conn = DriverManager.getConnection(getUrl(), props);
             Statement statement = conn.createStatement()) {
            statement.execute("CREATE IMMUTABLE TABLE " + tableName +
                    " (k1 TIMESTAMP not null, k2 bigint not null, v bigint, constraint pk " +
                    "primary key (k1 row_timestamp, k2)) SALT_BUCKETS = 9");
            statement.execute("CREATE INDEX " + indexName + " ON " + tableName +
                    " (v) INCLUDE (k2)");
            try (PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + tableName +
                    " VALUES (?, ?, ?) ")) {
                stmt.setTimestamp(1, new Timestamp(1000));
                stmt.setLong(2, 2000);
                stmt.setLong(3, 1000);
                stmt.executeUpdate();
                stmt.setTimestamp(1, new Timestamp(2000));
                stmt.setLong(2, 5000);
                stmt.setLong(3, 5);
                stmt.executeUpdate();
                stmt.setTimestamp(1, new Timestamp(3000));
                stmt.setLong(2, 5000);
                stmt.setLong(3, 5);
                stmt.executeUpdate();
                stmt.setTimestamp(1, new Timestamp(4000));
                stmt.setLong(2, 5000);
                stmt.setLong(3, 5);
                stmt.executeUpdate();
                stmt.setTimestamp(1, new Timestamp(5000));
                stmt.setLong(2, 2000);
                stmt.setLong(3, 10);
                stmt.executeUpdate();
                stmt.setTimestamp(1, new Timestamp(6000));
                stmt.setLong(2, 2000);
                stmt.setLong(3, 20);
                stmt.executeUpdate();
            }
            conn.commit();

            ResultSet rs = statement.executeQuery("SELECT " +
                    " K2 FROM " + tableName + " WHERE V = 5");
            assertTrue("Index " + indexName + " should have been used",
                    rs.unwrap(PhoenixResultSet.class).getStatement().getQueryPlan().getTableRef()
                            .getTable().getName().getString().equals(indexName));
            assertTrue(rs.next());
            assertEquals(5000, rs.getLong("k2"));
            assertTrue(rs.next());
            assertEquals(5000, rs.getLong("k2"));
            assertTrue(rs.next());
            assertEquals(5000, rs.getLong("k2"));
            assertFalse(rs.next());

            rs = statement.executeQuery("SELECT /*+ INDEX(" + tableName + " "
                    + indexName + ") */ " + " K2 FROM " + tableName + " WHERE V = 5");
            assertTrue("Index " + indexName + " should have been used",
                    rs.unwrap(PhoenixResultSet.class).getStatement().getQueryPlan()
                            .getTableRef().getTable().getName().getString().equals(indexName));
            assertTrue(rs.next());
            assertEquals(5000, rs.getLong("k2"));
            assertTrue(rs.next());
            assertEquals(5000, rs.getLong("k2"));
            assertTrue(rs.next());
            assertEquals(5000, rs.getLong("k2"));
            assertFalse(rs.next());

        }
    }

    @Test // See https://issues.apache.org/jira/browse/PHOENIX-4646
    public void testLengthLimitedVarchar() throws Exception {
        String tableName1 = generateUniqueName();
        String tableName2 = generateUniqueName();
        Properties props = new Properties();
        props.setProperty(QueryServices.ENABLE_SERVER_SIDE_UPSERT_MUTATIONS,
                allowServerSideMutations);
        try (Connection conn = DriverManager.getConnection(getUrl(), props);
             Statement stmt = conn.createStatement()) {
            conn.setAutoCommit(true);
            stmt.execute("create table " + tableName1 +
                    "(name varchar(160) primary key, id varchar(120), address varchar(160))");
            stmt.execute("create table " + tableName2 +
                    "(name varchar(160) primary key, id varchar(10), address  varchar(10))");
            stmt.execute("upsert into " + tableName1 +
                    " values('test','test','test')");
            stmt.execute("upsert into " + tableName2 + " select * from " +
                    tableName1);
            ResultSet rs = stmt.executeQuery("select * from " +
                    tableName2);
            assertTrue(rs.next());
            assertEquals("test", rs.getString(1));
            assertEquals("test", rs.getString(2));
            assertEquals("test", rs.getString(2));
            assertFalse(rs.next());
        }
    }

    private Connection getTenantConnection(String tenantId) throws Exception {
        Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
        props.setProperty(QueryServices.ENABLE_SERVER_SIDE_UPSERT_MUTATIONS,
                allowServerSideMutations);
        props.setProperty(TENANT_ID_ATTRIB, tenantId);
        return DriverManager.getConnection(getUrl(), props);
    }

}
