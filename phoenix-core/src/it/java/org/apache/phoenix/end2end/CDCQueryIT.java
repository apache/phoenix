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

import org.apache.hadoop.hbase.TableName;
import org.apache.phoenix.end2end.index.SingleCellIndexIT;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.CDCUtil;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.ManualEnvironmentEdge;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static org.apache.phoenix.query.QueryConstants.CDC_EVENT_TYPE;
import static org.apache.phoenix.query.QueryConstants.CDC_JSON_COL_NAME;
import static org.apache.phoenix.schema.PTable.QualifierEncodingScheme.NON_ENCODED_QUALIFIERS;
import static org.apache.phoenix.schema.PTable.QualifierEncodingScheme.TWO_BYTE_QUALIFIERS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

// NOTE: To debug the query execution, add the below condition or the equivalent where you need a
// breakpoint.
//      if (<table>.getTableName().getString().equals("N000002") ||
//                 <table>.getTableName().getString().equals("__CDC__N000002")) {
//          "".isEmpty();
//      }
@RunWith(Parameterized.class)
@Category(ParallelStatsDisabledTest.class)
public class CDCQueryIT extends CDCBaseIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(CDCQueryIT.class);

    // Offset of the first column, depending on whether PHOENIX_ROW_TIMESTAMP() is in the schema
    // or not.
    private final boolean forView;
    private final boolean dataBeforeCDC;
    private final PTable.QualifierEncodingScheme encodingScheme;
    private final boolean multitenant;
    private final Integer indexSaltBuckets;
    private final Integer tableSaltBuckets;
    private final boolean withSchemaName;

    public CDCQueryIT(Boolean forView, Boolean dataBeforeCDC,
                      PTable.QualifierEncodingScheme encodingScheme, boolean multitenant,
                      Integer indexSaltBuckets, Integer tableSaltBuckets, boolean withSchemaName) {
        this.forView = forView;
        this.dataBeforeCDC = dataBeforeCDC;
        this.encodingScheme = encodingScheme;
        this.multitenant = multitenant;
        this.indexSaltBuckets = indexSaltBuckets;
        this.tableSaltBuckets = tableSaltBuckets;
        this.withSchemaName = withSchemaName;
    }

    @Parameterized.Parameters(name = "forView={0} dataBeforeCDC={1}, encodingScheme={2}, " +
            "multitenant={3}, indexSaltBuckets={4}, tableSaltBuckets={5} withSchemaName=${6}")
    public static synchronized Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                { Boolean.FALSE, Boolean.FALSE, TWO_BYTE_QUALIFIERS, Boolean.FALSE, null, null,
                        Boolean.FALSE },
                { Boolean.FALSE, Boolean.TRUE, TWO_BYTE_QUALIFIERS, Boolean.FALSE, null, null,
                        Boolean.TRUE },
                { Boolean.FALSE, Boolean.FALSE, NON_ENCODED_QUALIFIERS, Boolean.FALSE, 1, 1,
                        Boolean.FALSE },
                // Once PHOENIX-7239, change this to have different salt buckets for data and index.
                { Boolean.FALSE, Boolean.FALSE, NON_ENCODED_QUALIFIERS, Boolean.TRUE, 1, 1,
                        Boolean.TRUE },
                { Boolean.FALSE, Boolean.FALSE, NON_ENCODED_QUALIFIERS, Boolean.FALSE, 4, null,
                        Boolean.FALSE },
                { Boolean.TRUE, Boolean.FALSE, TWO_BYTE_QUALIFIERS, Boolean.FALSE, null, null,
                        Boolean.FALSE },
        });
    }

    @Before
    public void beforeTest(){
        EnvironmentEdgeManager.reset();
        injectEdge = new ManualEnvironmentEdge();
        injectEdge.setValue(EnvironmentEdgeManager.currentTimeMillis());
    }

    @Test
    public void testSelectCDC() throws Exception {
        String cdcName, cdc_sql;
        String schemaName = withSchemaName ? generateUniqueName() : null;
        String tableName = SchemaUtil.getTableName(schemaName, generateUniqueName());
        String datatableName = tableName;
        try (Connection conn = newConnection()) {
            createTable(conn, "CREATE TABLE  " + tableName + " ("
                    + (multitenant ? "TENANT_ID CHAR(5) NOT NULL, " : "")
                    + "k INTEGER NOT NULL, v1 INTEGER, v2 INTEGER, B.vb INTEGER, "
                    + "CONSTRAINT PK PRIMARY KEY " + (multitenant ? "(TENANT_ID, k) " : "(k)")
                    + ")", encodingScheme, multitenant, tableSaltBuckets, false, null);
            if (forView) {
                String viewName = SchemaUtil.getTableName(schemaName, generateUniqueName());
                createTable(conn, "CREATE VIEW " + viewName + " AS SELECT * FROM " + tableName,
                        encodingScheme);
                tableName = viewName;
            }
            cdcName = generateUniqueName();
            cdc_sql = "CREATE CDC " + cdcName + " ON " + tableName;
            if (!dataBeforeCDC) {
                createCDCAndWait(conn, tableName, cdcName, cdc_sql, encodingScheme,
                        indexSaltBuckets);
            }
        }

        String tenantId = multitenant ? "1000" : null;
        String[] tenantids = {tenantId};
        if (multitenant) {
            tenantids = new String[] {tenantId, "2000"};
        }

        long startTS = System.currentTimeMillis();
        List<ChangeRow> changes = generateChanges(startTS, tenantids, tableName, null,
                COMMIT_SUCCESS);

        if (dataBeforeCDC) {
            try (Connection conn = newConnection()) {
                createCDCAndWait(conn, tableName, cdcName, cdc_sql, encodingScheme,
                        indexSaltBuckets);
            }
            // Testing with flushed data adds more coverage.
            getUtility().getAdmin().flush(TableName.valueOf(datatableName));
            getUtility().getAdmin().flush(TableName.valueOf(SchemaUtil.getTableName(schemaName,
                    CDCUtil.getCDCIndexName(cdcName))));
        }

        //SingleCellIndexIT.dumpTable(tableName);
        //SingleCellIndexIT.dumpTable(CDCUtil.getCDCIndexName(cdcName));

        String cdcFullName = SchemaUtil.getTableName(schemaName, cdcName);
        try (Connection conn = newConnection(tenantId)) {

            // Existence of CDC shouldn't cause the regular query path to fail.
            String uncovered_sql = "SELECT " + " /*+ INDEX(" + tableName + " " +
                    CDCUtil.getCDCIndexName(cdcName) + ") */ k, v1 FROM " + tableName;
            try (ResultSet rs = conn.createStatement().executeQuery(uncovered_sql)) {
                assertTrue(rs.next());
                assertEquals(3, rs.getInt(1));
                assertEquals(300, rs.getInt(2));
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1));
                assertEquals(201, rs.getInt(2));
                assertFalse(rs.next());
            }

            Map<String, String> dataColumns = new TreeMap<String, String>() {{
                put("V1", "INTEGER");
                put("V2", "INTEGER");
                put("B.VB", "INTEGER");
            }};
            verifyChangesViaSCN(tenantId, conn.createStatement().executeQuery(
                            "SELECT /*+ CDC_INCLUDE(CHANGE) */ * FROM " + cdcFullName),
                    datatableName, dataColumns, changes, CHANGE_IMG);
            verifyChangesViaSCN(tenantId, conn.createStatement().executeQuery(
                            "SELECT /*+ CDC_INCLUDE(CHANGE) */ PHOENIX_ROW_TIMESTAMP(), K," +
                                    "\"CDC JSON\" FROM " + cdcFullName), datatableName, dataColumns,
                    changes, CHANGE_IMG);
            verifyChangesViaSCN(tenantId, conn.createStatement().executeQuery(
                            "SELECT /*+ CDC_INCLUDE(PRE, POST) */ * FROM " + cdcFullName),
                    datatableName, dataColumns, changes, PRE_POST_IMG);
            verifyChangesViaSCN(tenantId, conn.createStatement().executeQuery(
                            "SELECT * FROM " + cdcFullName),
                    datatableName, dataColumns, changes, new HashSet<>());

            HashMap<String, int[]> testQueries = new HashMap<String, int[]>() {{
                put("SELECT 'dummy', k, \"CDC JSON\" FROM " + cdcFullName,
                        new int[]{1, 2, 3, 1, 1, 1, 1, 2, 1, 1, 1, 1});
                put("SELECT PHOENIX_ROW_TIMESTAMP(), k, \"CDC JSON\" FROM " + cdcFullName +
                        " ORDER BY k ASC", new int[]{1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 3});
                put("SELECT PHOENIX_ROW_TIMESTAMP(), k, \"CDC JSON\" FROM " + cdcFullName +
                        " ORDER BY k DESC", new int[]{3, 2, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1});
                put("SELECT PHOENIX_ROW_TIMESTAMP(), k, \"CDC JSON\" FROM " + cdcFullName +
                        " ORDER BY PHOENIX_ROW_TIMESTAMP() DESC",
                        new int[]{1, 1, 1, 1, 2, 1, 1, 1, 1, 3, 2, 1});
            }};
            Map<String, String> dummyChange = new HashMap() {{
                put(CDC_EVENT_TYPE, "dummy");
            }};
            for (Map.Entry<String, int[]> testQuery : testQueries.entrySet()) {
                try (ResultSet rs = conn.createStatement().executeQuery(testQuery.getKey())) {
                    for (int i = 0; i < testQuery.getValue().length; ++i) {
                        int k = testQuery.getValue()[i];
                        assertEquals(true, rs.next());
                        assertEquals("Index: " + i + " for query: " + testQuery.getKey(),
                                k, rs.getInt(2));
                        Map<String, Object> change = mapper.reader(
                                HashMap.class).readValue(rs.getString(3));
                        change.put(CDC_EVENT_TYPE, "dummy");
                        // Verify that we are getting nothing but the event type as we specified
                        // no change scopes.
                        assertEquals(dummyChange, change);
                    }
                    assertEquals(false, rs.next());
                }
            }
        }
    }

    @Test
    public void testSelectGeneric() throws Exception {
        String cdcName, cdc_sql;
        String schemaName = withSchemaName ? generateUniqueName() : null;
        String tableName = SchemaUtil.getTableName(schemaName, generateUniqueName());
        String datatableName = tableName;
        Map<String, String> pkColumns = new TreeMap<String, String>() {{
            put("K1", "INTEGER");
            put("K2", "INTEGER");
        }};
        Map<String, String> dataColumns = new TreeMap<String, String>() {{
            put("V1", "INTEGER");
            put("V2", "VARCHAR");
            put("V3", "CHAR");
            put("V4", "DOUBLE");
            put("V5", "DATE");
            put("V6", "TIME");
            put("V7", "TIMESTAMP");
            put("V8", "VARBINARY");
            put("V9", "BINARY");
        }};
        try (Connection conn = newConnection()) {
            createTable(conn, tableName, pkColumns, dataColumns, multitenant, encodingScheme,
                    tableSaltBuckets, false, null);
            if (forView) {
                String viewName = SchemaUtil.getTableName(schemaName, generateUniqueName());
                createTable(conn, "CREATE VIEW " + viewName + " AS SELECT * FROM " + tableName,
                        encodingScheme);
                tableName = viewName;
            }
            cdcName = generateUniqueName();
            cdc_sql = "CREATE CDC " + cdcName + " ON " + tableName + " INCLUDE (change)";
            if (!dataBeforeCDC) {
                createCDCAndWait(conn, tableName, cdcName, cdc_sql, encodingScheme,
                        indexSaltBuckets);
            }
        }

        String tenantId = multitenant ? "1000" : null;
        String[] tenantids = {tenantId};
        if (multitenant) {
            tenantids = new String[] {tenantId, "2000"};
        }

        long startTS = System.currentTimeMillis();
        Map<String, List<Set<ChangeRow>>> allBatches = new HashMap<>(tenantids.length);
        for (String tid: tenantids) {
            allBatches.put(tid, generateMutations(startTS, pkColumns, dataColumns, 20, 5));
            // For debug: uncomment to see the exact mutations that are being applied.
            //LOGGER.debug("----- DUMP Mutations -----");
            //int bnr = 1, mnr = 0;
            //for (Set<ChangeRow> batch: allBatches.get(tid)) {
            //    for (ChangeRow changeRow : batch) {
            //        LOGGER.debug("Mutation: " + (++mnr) + " in batch: " + bnr + " " + changeRow);
            //    }
            //    ++bnr;
            //}
            //LOGGER.debug("----------");
            applyMutations(COMMIT_SUCCESS, tableName, tid, allBatches.get(tid));
        }

        // For debug: uncomment to see the exact HBase cells.
        //LOGGER.debug("----- DUMP data table: " + datatableName + " -----");
        //SingleCellIndexIT.dumpTable(datatableName);
        //LOGGER.debug("----- DUMP index table: " + CDCUtil.getCDCIndexName(cdcName) + " -----");
        //SingleCellIndexIT.dumpTable(CDCUtil.getCDCIndexName(cdcName));
        //LOGGER.debug("----------");

        if (dataBeforeCDC) {
            try (Connection conn = newConnection()) {
                createCDCAndWait(conn, tableName, cdcName, cdc_sql, encodingScheme,
                        indexSaltBuckets);
            }
            // Testing with flushed data adds more coverage.
            getUtility().getAdmin().flush(TableName.valueOf(datatableName));
            getUtility().getAdmin().flush(TableName.valueOf(SchemaUtil.getTableName(schemaName,
                    CDCUtil.getCDCIndexName(cdcName))));
        }

        String cdcFullName = SchemaUtil.getTableName(schemaName, cdcName);
        try (Connection conn = newConnection(tenantId)) {
            // For debug: uncomment to see the exact results logged to console.
            //try (Statement stmt = conn.createStatement()) {
            //    try (ResultSet rs = stmt.executeQuery(
            //            "SELECT /*+ CDC_INCLUDE(PRE, CHANGE) */ * FROM " + cdcFullName)) {
            //        LOGGER.debug("----- DUMP CDC: " + cdcName + " -----");
            //        for (int i = 0; rs.next(); ++i) {
            //            LOGGER.debug("CDC row: " + (i+1) + " timestamp="
            //                    + rs.getDate(1).getTime() + " "
            //                    + collectColumns(pkColumns, rs) + ", " + CDC_JSON_COL_NAME + "="
            //                    + rs.getString(pkColumns.size() + 2));
            //        }
            //        LOGGER.debug("----------");
            //    }
            //}

            List<ChangeRow> changes = new ArrayList<>();
            for (Set<ChangeRow> batch: allBatches.get(tenantId)) {
                changes.addAll(batch);
            }
            verifyChangesViaSCN(tenantId, conn.createStatement().executeQuery(
                            "SELECT * FROM " + cdcFullName),
                    datatableName, dataColumns, changes, CHANGE_IMG);
            verifyChangesViaSCN(tenantId, conn.createStatement().executeQuery(
                            "SELECT /*+ CDC_INCLUDE(CHANGE) */ * FROM " + cdcFullName),
                    datatableName, dataColumns, changes, CHANGE_IMG);
            verifyChangesViaSCN(tenantId, conn.createStatement().executeQuery(
                            "SELECT /*+ CDC_INCLUDE(PRE, POST) */ * FROM " + cdcFullName),
                    datatableName, dataColumns, changes, PRE_POST_IMG);
            verifyChangesViaSCN(tenantId, conn.createStatement().executeQuery(
                            "SELECT /*+ CDC_INCLUDE(CHANGE, PRE, POST) */ * FROM " + cdcFullName),
                    datatableName, dataColumns, changes, ALL_IMG);
        }
    }

    private static String collectColumns(Map<String, String> pkColumns, ResultSet rs) {
        return pkColumns.keySet().stream().map(
                k -> {
                    try {
                        return k + "=" + rs.getObject(k);
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                }).collect(
                Collectors.joining(", "));
    }

    private void _testSelectCDCImmutable(PTable.ImmutableStorageScheme immutableStorageScheme)
            throws Exception {
        String cdcName, cdc_sql;
        String schemaName = withSchemaName ? generateUniqueName() : null;
        String tableName = SchemaUtil.getTableName(schemaName, generateUniqueName());
        String datatableName = tableName;
        try (Connection conn = newConnection()) {
           createTable(conn, "CREATE TABLE  " + tableName + " (" +
                            (multitenant ? "TENANT_ID CHAR(5) NOT NULL, " : "") +
                            "k INTEGER NOT NULL, v1 INTEGER, v2 INTEGER, CONSTRAINT PK PRIMARY KEY " +
                            (multitenant ? "(TENANT_ID, k) " : "(k)") + ")", encodingScheme, multitenant,
                    tableSaltBuckets, true, immutableStorageScheme);
            if (forView) {
                String viewName = SchemaUtil.getTableName(schemaName, generateUniqueName());
                createTable(conn, "CREATE VIEW " + viewName + " AS SELECT * FROM " + tableName,
                        encodingScheme);
                tableName = viewName;
            }
            cdcName = generateUniqueName();
            cdc_sql = "CREATE CDC " + cdcName + " ON " + tableName;
            if (!dataBeforeCDC) {
                createCDCAndWait(conn, tableName, cdcName, cdc_sql, encodingScheme,
                        indexSaltBuckets);
            }
        }

        String tenantId = multitenant ? "1000" : null;
        String[] tenantids = {tenantId};
        if (multitenant) {
            tenantids = new String[] {tenantId, "2000"};
        }

        long startTS = System.currentTimeMillis();
        List<ChangeRow> changes = generateChangesImmutableTable(startTS, tenantids, tableName,
                COMMIT_SUCCESS);

        if (dataBeforeCDC) {
            try (Connection conn = newConnection()) {
                createCDCAndWait(conn, tableName, cdcName, cdc_sql, encodingScheme,
                        indexSaltBuckets);
            }
            // Testing with flushed data adds more coverage.
            getUtility().getAdmin().flush(TableName.valueOf(datatableName));
            getUtility().getAdmin().flush(TableName.valueOf(SchemaUtil.getTableName(schemaName,
                    CDCUtil.getCDCIndexName(cdcName))));
        }

        String cdcFullName = SchemaUtil.getTableName(schemaName, cdcName);
        Map<String, String> dataColumns = new TreeMap<String, String>() {{
            put("V1", "INTEGER");
            put("V2", "INTEGER");
        }};
        try (Connection conn = newConnection(tenantId)) {
            // For debug: uncomment to see the exact results logged to console.
            //try (Statement stmt = conn.createStatement()) {
            //    try (ResultSet rs = stmt.executeQuery(
            //            "SELECT /*+ CDC_INCLUDE(PRE, POST) */ PHOENIX_ROW_TIMESTAMP(), K," +
            //                    "\"CDC JSON\" FROM " + cdcFullName)) {
            //        while (rs.next()) {
            //            System.out.println("----- " + rs.getString(1) + " " +
            //                    rs.getInt(2) + " " + rs.getString(3));
            //        }
            //    }
            //}
            verifyChangesViaSCN(tenantId, conn.createStatement().executeQuery(
                            "SELECT /*+ CDC_INCLUDE(PRE, POST) */ * FROM " + cdcFullName),
                    datatableName, dataColumns, changes, PRE_POST_IMG);
            verifyChangesViaSCN(tenantId, conn.createStatement().executeQuery(
                            "SELECT /*+ CDC_INCLUDE(CHANGE) */ * FROM " + cdcFullName),
                    datatableName, dataColumns, changes, CHANGE_IMG);
            verifyChangesViaSCN(tenantId, conn.createStatement().executeQuery("SELECT /*+ CDC_INCLUDE(CHANGE) */ " +
                            "PHOENIX_ROW_TIMESTAMP(), K, \"CDC JSON\" FROM " + cdcFullName),
                    datatableName, dataColumns, changes, CHANGE_IMG);
        }
    }

    @Test
    public void testSelectCDCImmutableOneCellPerColumn() throws Exception {
        _testSelectCDCImmutable(PTable.ImmutableStorageScheme.ONE_CELL_PER_COLUMN);
    }

    @Test
    public void testSelectCDCImmutableSingleCell() throws Exception {
        _testSelectCDCImmutable(PTable.ImmutableStorageScheme.SINGLE_CELL_ARRAY_WITH_OFFSETS);
    }

    @Test
    public void testSelectTimeRangeQueries() throws Exception {
        String cdcName, cdc_sql;
        String schemaName = withSchemaName ? generateUniqueName() : null;
        String tableName = SchemaUtil.getTableName(schemaName, generateUniqueName());
        try (Connection conn = newConnection()) {
            createTable(conn, "CREATE TABLE  " + tableName + " (" +
                    (multitenant ? "TENANT_ID CHAR(5) NOT NULL, " : "") +
                    "k INTEGER NOT NULL, v1 INTEGER, CONSTRAINT PK PRIMARY KEY " +
                    (multitenant ? "(TENANT_ID, k) " : "(k)") + ")", encodingScheme, multitenant,
                    tableSaltBuckets, false, null);
            if (forView) {
                String viewName = SchemaUtil.getTableName(schemaName, generateUniqueName());
                createTable(conn, "CREATE VIEW " + viewName + " AS SELECT * FROM " + tableName,
                        encodingScheme);
                tableName = viewName;
            }
            cdcName = generateUniqueName();
            cdc_sql = "CREATE CDC " + cdcName + " ON " + tableName;
            if (!dataBeforeCDC) {
                createCDCAndWait(conn, tableName, cdcName, cdc_sql, encodingScheme,
                        indexSaltBuckets);
            }
        }

        EnvironmentEdgeManager.injectEdge(injectEdge);

        String tenantId = multitenant ? "1000" : null;
        String[] tenantids = {tenantId};
        if (multitenant) {
            tenantids = new String[] {tenantId, "2000"};
        }

        Timestamp ts1 = new Timestamp(System.currentTimeMillis());
        cal.setTimeInMillis(ts1.getTime());
        injectEdge.setValue(ts1.getTime());

        for (String tid: tenantids) {
            try (Connection conn = newConnection(tid)) {
                conn.createStatement().execute("UPSERT INTO " + tableName + " (k, v1) VALUES (1, 100)");
                conn.commit();
            }
        }

        injectEdge.incrementValue(100);

        for (String tid: tenantids) {
            try (Connection conn = newConnection(tid)) {
                conn.createStatement().execute("UPSERT INTO " + tableName + " (k, v1) VALUES (2, 200)");
                conn.commit();
            }
        }

        injectEdge.incrementValue(100);
        cal.add(Calendar.MILLISECOND, 200);
        Timestamp ts2 = new Timestamp(cal.getTime().getTime());
        injectEdge.incrementValue(100);

        for (String tid: tenantids) {
            try (Connection conn = newConnection(tid)) {
                conn.createStatement().execute("UPSERT INTO " + tableName + " (k, v1) VALUES (1, 101)");
                conn.commit();
                injectEdge.incrementValue(100);
                conn.createStatement().execute("UPSERT INTO " + tableName + " (k, v1) VALUES (3, 300)");
                conn.commit();
            }
        }

        injectEdge.incrementValue(100);
        cal.add(Calendar.MILLISECOND, 200 + 100 * tenantids.length);
        Timestamp ts3 = new Timestamp(cal.getTime().getTime());
        injectEdge.incrementValue(100);

        for (String tid: tenantids) {
            try (Connection conn = newConnection(tid)) {
                conn.createStatement().execute("UPSERT INTO " + tableName + " (k, v1) VALUES (1, 101)");
                conn.commit();
                injectEdge.incrementValue(100);
                conn.createStatement().execute("DELETE FROM " + tableName + " WHERE k = 2");
                conn.commit();
            }
        }

        injectEdge.incrementValue(100);
        cal.add(Calendar.MILLISECOND, 200 + 100 * tenantids.length);
        Timestamp ts4 = new Timestamp(cal.getTime().getTime());
        EnvironmentEdgeManager.reset();

        if (dataBeforeCDC) {
            try (Connection conn = newConnection()) {
                createCDCAndWait(conn, tableName, cdcName, cdc_sql, encodingScheme,
                        indexSaltBuckets);
            }
        }

        //SingleCellIndexIT.dumpTable(CDCUtil.getCDCIndexName(cdcName));

        String cdcFullName = SchemaUtil.getTableName(schemaName, cdcName);
        try (Connection conn = newConnection(tenantId)) {
            String sel_sql =
                    "SELECT to_char(phoenix_row_timestamp()), k, \"CDC JSON\" FROM " + cdcFullName +
                            " WHERE PHOENIX_ROW_TIMESTAMP() >= ? AND PHOENIX_ROW_TIMESTAMP() <= ?";
            Object[] testDataSets = new Object[] {
                    new Object[] {ts1, ts2, new int[] {1, 2}},
                    new Object[] {ts2, ts3, new int[] {1, 3}},
                    new Object[] {ts3, ts4, new int[] {1, 2}},
                    new Object[] {ts1, ts4, new int[] {1, 2, 1, 3, 1, 2}},
            };
            PreparedStatement stmt = conn.prepareStatement(sel_sql);
            // For debug: uncomment to see the exact results logged to console.
            //System.out.println("----- ts1: " + ts1 + " ts2: " + ts2 + " ts3: " + ts3 + " ts4: " +
            //        ts4);
            //for (int i = 0; i < testDataSets.length; ++i) {
            //    Object[] testData = (Object[]) testDataSets[i];
            //    stmt.setTimestamp(1, (Timestamp) testData[0]);
            //    stmt.setTimestamp(2, (Timestamp) testData[1]);
            //    try (ResultSet rs = stmt.executeQuery()) {
            //        System.out.println("----- Test data set: " + i);
            //        while (rs.next()) {
            //            System.out.println("----- " + rs.getString(1) + " " +
            //                    rs.getInt(2) + " "  + rs.getString(3));
            //        }
            //    }
            //}
            for (int i = 0; i < testDataSets.length; ++i) {
                Object[] testData = (Object[]) testDataSets[i];
                stmt.setTimestamp(1, (Timestamp) testData[0]);
                stmt.setTimestamp(2, (Timestamp) testData[1]);
                try (ResultSet rs = stmt.executeQuery()) {
                    for (int j = 0; j < ((int[]) testData[2]).length; ++j) {
                        int k = ((int[]) testData[2])[j];
                        assertEquals(" Index: " + j + " Test data set: " + i,
                                true, rs.next());
                        assertEquals(" Index: " + j + " Test data set: " + i,
                                k, rs.getInt(2));
                    }
                    assertEquals("Test data set: " + i, false, rs.next());
                }
            }

            PreparedStatement pstmt = conn.prepareStatement(
                    "SELECT * FROM " + cdcFullName + " WHERE PHOENIX_ROW_TIMESTAMP() > ?");
            pstmt.setTimestamp(1, ts4);
            try (ResultSet rs = pstmt.executeQuery()) {
                assertEquals(false, rs.next());
            }
        }
    }

    @Test
    public void testSelectCDCWithDDL() throws Exception {
        String schemaName = withSchemaName ? generateUniqueName() : null;
        String tableName = SchemaUtil.getTableName(schemaName, generateUniqueName());
        String datatableName = tableName;
        String cdcName, cdc_sql;
        try (Connection conn = newConnection()) {
            createTable(conn, "CREATE TABLE  " + tableName + " (" +
                    (multitenant ? "TENANT_ID CHAR(5) NOT NULL, " : "") +
                    "k INTEGER NOT NULL, v0 INTEGER, v1 INTEGER, v1v2 INTEGER, v2 INTEGER, B.vb INTEGER, " +
                    "v3 INTEGER, CONSTRAINT PK PRIMARY KEY " +
                    (multitenant ? "(TENANT_ID, k) " : "(k)") + ")", encodingScheme, multitenant,
                    tableSaltBuckets, false, null);
            if (forView) {
                String viewName = SchemaUtil.getTableName(schemaName, generateUniqueName());
                createTable(conn, "CREATE VIEW " + viewName + " AS SELECT * FROM " + tableName,
                        encodingScheme);
                tableName = viewName;
            }

            cdcName = generateUniqueName();
            cdc_sql = "CREATE CDC " + cdcName + " ON " + tableName;
            if (!dataBeforeCDC) {
                createCDCAndWait(conn, tableName, cdcName, cdc_sql, encodingScheme,
                        indexSaltBuckets);
            }
            conn.createStatement().execute("ALTER TABLE " + datatableName + " DROP COLUMN v0");
        }

        String tenantId = multitenant ? "1000" : null;
        String[] tenantids = {tenantId};
        if (multitenant) {
            tenantids = new String[] {tenantId, "2000"};
        }

        long startTS = System.currentTimeMillis();
        List<ChangeRow> changes = generateChanges(startTS, tenantids, tableName, datatableName,
                COMMIT_SUCCESS);

        if (dataBeforeCDC) {
            try (Connection conn = newConnection()) {
                createCDCAndWait(conn, tableName, cdcName, cdc_sql, encodingScheme,
                        indexSaltBuckets);
            }
            // Testing with flushed data adds more coverage.
            getUtility().getAdmin().flush(TableName.valueOf(datatableName));
            getUtility().getAdmin().flush(TableName.valueOf(SchemaUtil.getTableName(schemaName,
                    CDCUtil.getCDCIndexName(cdcName))));
        }

        Map<String, String> dataColumns = new TreeMap<String, String>() {{
            put("V0", "INTEGER");
            put("V1", "INTEGER");
            put("V1V2", "INTEGER");
            put("V2", "INTEGER");
            put("B.VB", "INTEGER");
            put("V3", "INTEGER");
        }};
        try (Connection conn = newConnection(tenantId)) {
            verifyChangesViaSCN(tenantId, conn.createStatement().executeQuery(
                            "SELECT /*+ CDC_INCLUDE(CHANGE) */ * FROM " + SchemaUtil.getTableName(
                                    schemaName, cdcName)),
                    datatableName, dataColumns, changes, CHANGE_IMG);
        }
    }

    @Test
    public void testSelectCDCFailDataTableUpdate() throws Exception {
        if (dataBeforeCDC == true) {
            // In this case, index will not exist at the time of upsert, so we can't simulate the
            // index failure.
            return;
        }
        String schemaName = withSchemaName ? generateUniqueName() : null;
        String tableName = SchemaUtil.getTableName(schemaName, generateUniqueName());
        String cdcName, cdc_sql;
        try (Connection conn = newConnection()) {
            createTable(conn, "CREATE TABLE  " + tableName + " (" +
                            (multitenant ? "TENANT_ID CHAR(5) NOT NULL, " : "") +
                            "k INTEGER NOT NULL, v1 INTEGER, v2 INTEGER, B.vb INTEGER, " +
                            "CONSTRAINT PK PRIMARY KEY " +
                            (multitenant ? "(TENANT_ID, k) " : "(k)") + ")", encodingScheme, multitenant,
                    tableSaltBuckets, false, null);
            if (forView) {
                String viewName = SchemaUtil.getTableName(schemaName, generateUniqueName());
                createTable(conn, "CREATE VIEW " + viewName + " AS SELECT * FROM " + tableName,
                        encodingScheme);
                tableName = viewName;
            }
            cdcName = generateUniqueName();
            cdc_sql = "CREATE CDC " + cdcName + " ON " + tableName;
            createCDCAndWait(conn, tableName, cdcName, cdc_sql, encodingScheme, indexSaltBuckets);
        }

        String tenantId = multitenant ? "1000" : null;
        String[] tenantids = {tenantId};
        if (multitenant) {
            tenantids = new String[] {tenantId, "2000"};
        }

        long startTS = System.currentTimeMillis();
        generateChanges(startTS, tenantids, tableName, null,
                COMMIT_FAILURE_EXPECTED);

        try (Connection conn = newConnection(tenantId)) {
            ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM " +
                    SchemaUtil.getTableName(schemaName, cdcName));
            assertEquals(false, rs.next());
        }
    }
}
