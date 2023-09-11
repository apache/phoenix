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

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.end2end.ParallelStatsDisabledTest;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Properties;

import static org.apache.phoenix.schema.PTable.ImmutableStorageScheme.ONE_CELL_PER_COLUMN;
import static org.apache.phoenix.schema.PTable.ImmutableStorageScheme.SINGLE_CELL_ARRAY_WITH_OFFSETS;
import static org.apache.phoenix.schema.PTable.QualifierEncodingScheme.NON_ENCODED_QUALIFIERS;
import static org.apache.phoenix.schema.PTable.QualifierEncodingScheme.TWO_BYTE_QUALIFIERS;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Category(ParallelStatsDisabledTest.class)
@RunWith(Parameterized.class)
public class SingleCellIndexIT extends ParallelStatsDisabledIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(SingleCellIndexIT.class);
    private Properties testProps = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    private boolean mutable;
    private final String tableDDLOptions;

    @Parameterized.Parameters(name = "mutable={0}")
    public static synchronized Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                {true},
                {false} });
    }

    @Before
    public void setupBefore() {
        testProps = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        testProps.put(QueryServices.DEFAULT_COLUMN_ENCODED_BYTES_ATRRIB, "0");
    }

    public SingleCellIndexIT(boolean mutable) {
        StringBuilder optionBuilder = new StringBuilder();
        this.mutable = mutable;
        optionBuilder.append(" IMMUTABLE_STORAGE_SCHEME=ONE_CELL_PER_COLUMN ");
        if (!mutable) {
            optionBuilder.append(", IMMUTABLE_ROWS=true ");
        }
        this.tableDDLOptions = optionBuilder.toString();
    }

    @Test
    public void testCreateOneCellTableAndSingleCellIndex() throws Exception {

        try (Connection conn = DriverManager.getConnection(getUrl(), testProps)) {
            conn.setAutoCommit(true);
            String tableName = "TBL_" + generateUniqueName();
            String idxName = "IND_" + generateUniqueName();

            createTableAndIndex(conn, tableName, idxName, this.tableDDLOptions, false,3);
            assertMetadata(conn, ONE_CELL_PER_COLUMN, NON_ENCODED_QUALIFIERS, tableName);
            assertMetadata(conn, SINGLE_CELL_ARRAY_WITH_OFFSETS, TWO_BYTE_QUALIFIERS, idxName);

            dumpTable(tableName);

            String selectFromData = "SELECT /*+ NO_INDEX */ PK1, INT_PK, V1, V2, V4 FROM " + tableName + " where V2 >= 3 and V4 LIKE 'V4%'";
            ResultSet rs = conn.createStatement().executeQuery("EXPLAIN " + selectFromData);
            String actualExplainPlan = QueryUtil.getExplainPlan(rs);
            assertTrue(actualExplainPlan.contains(tableName));

            rs = conn.createStatement().executeQuery(selectFromData);
            assertTrue(rs.next());
            assertEquals("PK2", rs.getString(1));
            assertEquals(2, rs.getInt(2));
            assertEquals("V12", rs.getString(3));
            assertEquals(3, rs.getInt(4));
            assertEquals("V42", rs.getString(5));
            assertTrue(rs.next());

            String selectFromIndex = "SELECT PK1, INT_PK, V1, V2, V4 FROM " + tableName + " where V2 >= 3 and V4 LIKE 'V4%'";
            rs = conn.createStatement().executeQuery("EXPLAIN " + selectFromIndex);
            actualExplainPlan = QueryUtil.getExplainPlan(rs);
            assertTrue(actualExplainPlan.contains(idxName));

            rs = conn.createStatement().executeQuery(selectFromIndex);
            assertTrue(rs.next());
            assertEquals("PK2", rs.getString(1));
            assertEquals(2, rs.getInt(2));
            assertEquals("V12", rs.getString(3));
            assertEquals(3, rs.getInt(4));
            assertEquals("V42", rs.getString(5));
            assertTrue(rs.next());
            assertEquals("PK3", rs.getString(1));
            assertEquals(3, rs.getInt(2));
            assertEquals("V13", rs.getString(3));
            assertEquals(4, rs.getInt(4));
            assertEquals("V43", rs.getString(5));
            assertFalse(rs.next());
        }
    }

    @Test
    public void testAddColumns() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl(), testProps)) {
            conn.setAutoCommit(true);

            String tableName = "TBL_" + generateUniqueName();
            String idxName = "IND_" + generateUniqueName();

            createTableAndIndex(conn, tableName, idxName, null, false,3);
            assertMetadata(conn, ONE_CELL_PER_COLUMN, NON_ENCODED_QUALIFIERS, tableName);
            assertMetadata(conn, SINGLE_CELL_ARRAY_WITH_OFFSETS, TWO_BYTE_QUALIFIERS, idxName);

            String alterTable = "ALTER TABLE " + tableName + " ADD V_NEW VARCHAR CASCADE INDEX ALL";
            conn.createStatement().execute(alterTable);

            String upsert = "UPSERT INTO " + tableName + " (PK1,INT_PK,V1, V2, V3, V4, V5, V_NEW) VALUES ('PK99',99,'V199',100,101,'V499','V699','V_NEW99')";
            conn.createStatement().executeUpdate(upsert);
            conn.commit();

            String selectFromIndex = "SELECT PK1, INT_PK, V1, V2, V4, V_NEW FROM " + tableName + " where V1='V199' AND V2=100";
            ResultSet rs = conn.createStatement().executeQuery("EXPLAIN " + selectFromIndex);
            String actualExplainPlan = QueryUtil.getExplainPlan(rs);
            assertTrue(actualExplainPlan.contains(idxName));

            rs = conn.createStatement().executeQuery(selectFromIndex);
            assertTrue(rs.next());
            assertEquals("PK99", rs.getString(1));
            assertEquals(99, rs.getInt(2));
            assertEquals("V199", rs.getString(3));
            assertEquals(100, rs.getInt(4));
            assertEquals("V499", rs.getString(5));
            assertEquals("V_NEW99", rs.getString(6));
            assertFalse(rs.next());
        }
    }

    @Test
    public void testDropColumns() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl(), testProps)) {
            conn.setAutoCommit(true);

            String tableName = "TBL_" + generateUniqueName();
            String idxName = "IND_" + generateUniqueName();

            createTableAndIndex(conn, tableName, idxName, null, false,1);
            assertMetadata(conn, ONE_CELL_PER_COLUMN, NON_ENCODED_QUALIFIERS, tableName);
            assertMetadata(conn, SINGLE_CELL_ARRAY_WITH_OFFSETS, TWO_BYTE_QUALIFIERS, idxName);

            String alterTable = "ALTER TABLE " + tableName + " DROP COLUMN V2";
            conn.createStatement().execute(alterTable);

            String star = "SELECT * FROM " + idxName ;
            ResultSet rs = conn.createStatement().executeQuery(star);
            assertTrue(rs.next());
            assertEquals("PK1", rs.getString(1));
            assertEquals(1, rs.getInt(2));
            assertEquals("V11", rs.getString(3));
            assertEquals("V41", rs.getString(4));
            assertFalse(rs.next());

            String selectFromIndex = "SELECT PK1, INT_PK, V1, V4 FROM " + tableName + " where V1='V11'";
            rs = conn.createStatement().executeQuery("EXPLAIN " + selectFromIndex);
            String actualExplainPlan = QueryUtil.getExplainPlan(rs);
            assertTrue(actualExplainPlan.contains(idxName));

            rs = conn.createStatement().executeQuery(selectFromIndex);
            assertTrue(rs.next());
            assertEquals("PK1", rs.getString(1));
            assertEquals(1, rs.getInt(2));
            assertEquals("V11", rs.getString(3));
            assertEquals("V41", rs.getString(4));
            assertFalse(rs.next());
        }
    }

    @Test
    public void testTenantViewIndexes() throws Exception {

        try (Connection conn = DriverManager.getConnection(getUrl(), testProps)) {
            conn.setAutoCommit(true);

            String tableName = "TBL_" + generateUniqueName();
            String view1IndexName = "IND_" + generateUniqueName();
            String divergedViewIndex = "DIND_" + generateUniqueName();
            String view3IndexName = "IND_" + generateUniqueName();
            String view1 = "V_" + generateUniqueName();
            String divergedView =  "V_" + generateUniqueName();
            String view3 = "V_" + generateUniqueName();

            String baseTableDDL = "CREATE  TABLE " + tableName + " (TENANT_ID VARCHAR NOT NULL, PK1 VARCHAR NOT NULL, V1 VARCHAR, V2 VARCHAR, V3 VARCHAR CONSTRAINT NAME_PK PRIMARY KEY(TENANT_ID, PK1)) MULTI_TENANT = true"
                    + (this.mutable ? "": ", IMMUTABLE_ROWS=true");
            conn.createStatement().execute(baseTableDDL);
            assertMetadata(conn, ONE_CELL_PER_COLUMN, NON_ENCODED_QUALIFIERS, tableName);

            try (Connection tenant1Conn = getTenantConnection("tenant1")) {
                String
                        view1DDL =
                        "CREATE VIEW " + view1 + " ( VIEW_COL1 VARCHAR, VIEW_COL2 VARCHAR) AS SELECT * FROM "
                                + tableName;
                tenant1Conn.createStatement().execute(view1DDL);
                String indexDDL = "CREATE INDEX " + view1IndexName + " ON " + view1 + " (V1) include (VIEW_COL2, V3) IMMUTABLE_STORAGE_SCHEME=SINGLE_CELL_ARRAY_WITH_OFFSETS,COLUMN_ENCODED_BYTES=2";
                tenant1Conn.createStatement().execute(indexDDL);
                tenant1Conn.commit();
            }

            try (Connection tenant2Conn = getTenantConnection("tenant2")) {
                String
                        divergedViewDDL =
                        "CREATE VIEW " + divergedView + " ( VIEW_COL1 VARCHAR, VIEW_COL2 VARCHAR) AS SELECT * FROM "
                                + tableName ;
                tenant2Conn.createStatement().execute(divergedViewDDL);
                // Drop column V2 from the view to have it diverge from the base table
                tenant2Conn.createStatement().execute("ALTER VIEW " + divergedView + " DROP COLUMN V2");

                // create an index on the diverged view
                String indexDDL = "CREATE INDEX " + divergedViewIndex + " ON " + divergedView + " (V1) include (VIEW_COL1, V3) IMMUTABLE_STORAGE_SCHEME=SINGLE_CELL_ARRAY_WITH_OFFSETS";
                tenant2Conn.createStatement().execute(indexDDL);
                tenant2Conn.commit();
            }

            try (Connection tenant3Conn = getTenantConnection("tenant3")) {
                String
                        view3DDL =
                        "CREATE VIEW " + view3 + " ( VIEW_COL31 VARCHAR, VIEW_COL32 VARCHAR) AS SELECT * FROM "
                                + tableName;
                tenant3Conn.createStatement().execute(view3DDL);
                String indexDDL = "CREATE INDEX " + view3IndexName + " ON " + view3 + " (V1) include (VIEW_COL32, V3)";
                tenant3Conn.createStatement().execute(indexDDL);
                tenant3Conn.commit();
            }

            String upsert = "UPSERT INTO " + view1 + " (PK1, V1, V2, V3, VIEW_COL1, VIEW_COL2) VALUES ('PK1',  'V1', 'V2', 'V3','VIEW_COL1_1','VIEW_COL2_1')";
            try (Connection viewConn = getTenantConnection("tenant1")) {
                viewConn.createStatement().executeUpdate(upsert);
                viewConn.commit();
                Statement stmt = viewConn.createStatement();
                String sql = "SELECT V3, VIEW_COL2 FROM " + view1 + " WHERE V1 = 'V1'";
                QueryPlan plan = stmt.unwrap(PhoenixStatement.class).optimizeQuery(sql);
                assertTrue(plan.getTableRef().getTable().getName().getString().equals(SchemaUtil.normalizeIdentifier(view1IndexName)));
                ResultSet rs = viewConn.createStatement().executeQuery(sql);
                assertTrue(rs.next());
                assertEquals("V3", rs.getString(1));
                assertEquals("VIEW_COL2_1", rs.getString(2));
            }

            // Upsert records in diverged view. Verify that the PK column was added to the index on it.
            upsert = "UPSERT INTO " + divergedView + " (PK1, V1, V3, VIEW_COL1, VIEW_COL2) VALUES ('PK1', 'V1', 'V3','VIEW_COL21_1','VIEW_COL22_1')";
            try (Connection viewConn = getTenantConnection("tenant2")) {
                viewConn.createStatement().executeUpdate(upsert);
                viewConn.commit();
                Statement stmt = viewConn.createStatement();
                String sql = "SELECT V3, VIEW_COL1 FROM " + divergedView + " WHERE V1 = 'V1'";
                QueryPlan plan = stmt.unwrap(PhoenixStatement.class).optimizeQuery(sql);
                assertTrue(plan.getTableRef().getTable().getName().getString().equals(SchemaUtil.normalizeIdentifier(divergedViewIndex)));
                ResultSet rs = viewConn.createStatement().executeQuery(sql);
                assertTrue(rs.next());
                assertEquals("V3", rs.getString(1));
                assertEquals("VIEW_COL21_1", rs.getString(2));
            }

            upsert = "UPSERT INTO " + view3 + " (PK1, V1, V2, V3, VIEW_COL31, VIEW_COL32) VALUES ('PK1',  'V1', 'V2', 'V3','VIEW_COL31_1','VIEW_COL32_1')";
            try (Connection viewConn = getTenantConnection("tenant3")) {
                viewConn.createStatement().executeUpdate(upsert);
                viewConn.commit();
                Statement stmt = viewConn.createStatement();
                String sql = "SELECT V3, VIEW_COL32 FROM " + view3 + " WHERE V1 = 'V1'";
                QueryPlan plan = stmt.unwrap(PhoenixStatement.class).optimizeQuery(sql);
                assertTrue(plan.getTableRef().getTable().getName().getString().equals(SchemaUtil.normalizeIdentifier(view3IndexName)));
                ResultSet rs = viewConn.createStatement().executeQuery(sql);
                assertTrue(rs.next());
                assertEquals("V3", rs.getString(1));
                assertEquals("VIEW_COL32_1", rs.getString(2));
            }
            dumpTable("_IDX_" + tableName);
        }
    }

    @Test
    public void testUpsertSelect() throws Exception {
        String tableName = generateUniqueName();
        String idxName = "IDX_" + generateUniqueName();

        try (Connection conn = DriverManager.getConnection(getUrl(), testProps)) {
            conn.setAutoCommit(true);
            createTableAndIndex(conn, tableName, idxName, null, true, 2);
            assertMetadata(conn, ONE_CELL_PER_COLUMN, NON_ENCODED_QUALIFIERS, tableName);
            assertMetadata(conn, SINGLE_CELL_ARRAY_WITH_OFFSETS, TWO_BYTE_QUALIFIERS, idxName);

            // this delete will be issued at a timestamp later than the above timestamp of the index table
            conn.createStatement().execute("delete from " + tableName + " where pk1 = 'PK1'");
            conn.commit();
            String
                    sql =
                    "UPSERT INTO " + idxName + "(\":PK1\",\":INT_PK\",\"0:V1\",\"0:V2\",\"0:V4\") "
                            + " SELECT /*+ NO_INDEX */ PK1,INT_PK, V1, V2,V4 FROM "
                            + tableName;
            conn.createStatement().executeUpdate(sql);
            conn.commit();

            // validate that delete markers were issued correctly and only ('a', '1', 'value1') was
            // deleted
            String query = "SELECT \":PK1\" from " + idxName + " ORDER BY \":PK1\"";
            ResultSet rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("PK2", rs.getString(1));
            assertFalse(rs.next());
        }
    }

    @Test
    public void testMultipleColumnFamilies() throws Exception {
        String tableName = generateUniqueName();
        String idxName = "IDX_" + generateUniqueName();
        int numOfRows = 2;

        try (Connection conn = DriverManager.getConnection(getUrl(), testProps)) {
            conn.setAutoCommit(true);
            String createTableSql = "CREATE TABLE " + tableName +
                    " (PK1 VARCHAR NOT NULL, INT_PK INTEGER NOT NULL, V1 VARCHAR, A.V2 INTEGER, B.V3 INTEGER, A.V4 VARCHAR, B.V5 VARCHAR CONSTRAINT NAME_PK PRIMARY KEY(PK1, INT_PK)) ";
            conn.createStatement().execute(createTableSql);
            String createIndexSql = "CREATE INDEX " + idxName + " ON " + tableName + " (A.V2) include (B.V3, A.V4, B.V5) "
                    + " IMMUTABLE_STORAGE_SCHEME=SINGLE_CELL_ARRAY_WITH_OFFSETS, COLUMN_ENCODED_BYTES=2";
            LOGGER.debug(createIndexSql);
            conn.createStatement().execute(createIndexSql);
            String upsert = "UPSERT INTO " + tableName + " (PK1, INT_PK, V1, A.V2, B.V3, A.V4, B.V5) VALUES (?,?,?,?,?,?,?)";
            PreparedStatement upsertStmt = conn.prepareStatement(upsert);

            for (int i=1; i <= numOfRows; i++) {
                upsertStmt.setString(1, "PK"+i);
                upsertStmt.setInt(2, i);
                upsertStmt.setString(3, "V1"+i);
                upsertStmt.setInt(4, i+1);
                upsertStmt.setInt(5, i+2);
                upsertStmt.setString(6, "V4"+i);
                upsertStmt.setString(7, "V5"+i);
                upsertStmt.executeUpdate();
            }
            assertMetadata(conn, ONE_CELL_PER_COLUMN, NON_ENCODED_QUALIFIERS, tableName);
            assertMetadata(conn, SINGLE_CELL_ARRAY_WITH_OFFSETS, TWO_BYTE_QUALIFIERS, idxName);

            String query = "SELECT * from " + idxName + " ORDER BY \":PK1\"";
            ResultSet rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("2", rs.getString(1));
            assertEquals("PK1", rs.getString(2));
            assertEquals("1", rs.getString(3));
            assertEquals("3", rs.getString(4));
            assertEquals("V41", rs.getString(5));
            assertEquals("V51", rs.getString(6));
            assertTrue(rs.next());
            assertEquals("3", rs.getString(1));
            assertEquals("PK2", rs.getString(2));
            assertEquals("2", rs.getString(3));
            assertEquals("4", rs.getString(4));
            assertEquals("V42", rs.getString(5));
            assertEquals("V52", rs.getString(6));

            String selectFromIndex = "SELECT PK1, INT_PK, A.V2, B.V3, A.V4, B.V5 FROM " + tableName + " where A.V4='V42' and B.V3 >= 3";
            rs = conn.createStatement().executeQuery("EXPLAIN " + selectFromIndex);
            String actualExplainPlan = QueryUtil.getExplainPlan(rs);
            assertTrue(actualExplainPlan.contains(idxName));
            rs = conn.createStatement().executeQuery(selectFromIndex);
            assertTrue(rs.next());
            assertEquals("PK2", rs.getString(1));
            assertEquals("2", rs.getString(2));
            assertEquals("3", rs.getString(3));
            assertEquals("4", rs.getString(4));
            assertEquals("V42", rs.getString(5));
            assertEquals("V52", rs.getString(6));
            assertFalse(rs.next());
        }
    }

    @Test
    public void testPartialUpdateSingleCellTable() throws Exception {

        try (Connection conn = DriverManager.getConnection(getUrl(), testProps)) {
            conn.setAutoCommit(true);
            String tableName = "TBL_" + generateUniqueName();
            String idxName = "IND_" + generateUniqueName();

            createTableAndIndex(conn, tableName, idxName, this.tableDDLOptions, false,3);
            assertMetadata(conn, ONE_CELL_PER_COLUMN, NON_ENCODED_QUALIFIERS, tableName);
            assertMetadata(conn, SINGLE_CELL_ARRAY_WITH_OFFSETS, TWO_BYTE_QUALIFIERS, idxName);

            // Partial update 1st row
            String upsert = "UPSERT INTO " + tableName + " (PK1, INT_PK, V4) VALUES ('PK1',1,'UpdatedV4')";
            conn.createStatement().execute(upsert);
            conn.commit();
            String selectFromData = "SELECT /*+ NO_INDEX */ PK1, INT_PK, V1, V2, V4 FROM " + tableName + " where INT_PK = 1 and V4 LIKE 'UpdatedV4'";
            ResultSet rs = conn.createStatement().executeQuery("EXPLAIN " + selectFromData);
            String actualExplainPlan = QueryUtil.getExplainPlan(rs);
            assertTrue(actualExplainPlan.contains(tableName));

            rs = conn.createStatement().executeQuery(selectFromData);
            assertTrue(rs.next());
            assertEquals("PK1", rs.getString(1));
            assertEquals(1, rs.getInt(2));
            assertEquals("V11", rs.getString(3));
            assertEquals(2, rs.getInt(4));
            assertFalse(rs.next());

            String selectFromIndex = "SELECT PK1, INT_PK, V1, V2, V4 FROM " + tableName + " where V2 >= 2 and V4 = 'UpdatedV4'";
            rs = conn.createStatement().executeQuery("EXPLAIN " + selectFromIndex);
            actualExplainPlan = QueryUtil.getExplainPlan(rs);
            assertTrue(actualExplainPlan.contains(idxName));

            rs = conn.createStatement().executeQuery(selectFromIndex);
            assertTrue(rs.next());
            assertEquals("PK1", rs.getString(1));
            assertEquals(1, rs.getInt(2));
            assertEquals("V11", rs.getString(3));
            assertEquals(2, rs.getInt(4));
            assertFalse(rs.next());
        }
    }

    private Connection getTenantConnection(String tenantId) throws Exception {
        Properties tenantProps = PropertiesUtil.deepCopy(testProps);
        tenantProps.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
        return DriverManager.getConnection(getUrl(), tenantProps);
    }

    private void createTableAndIndex(Connection conn, String tableName, String indexName, String tableDDL, boolean async, int numOfRows)
            throws SQLException {
        String createTableSql = "CREATE TABLE " + tableName + " (PK1 VARCHAR NOT NULL, INT_PK INTEGER NOT NULL, V1 VARCHAR, V2 INTEGER, V3 INTEGER, V4 VARCHAR, V5 VARCHAR CONSTRAINT NAME_PK PRIMARY KEY(PK1, INT_PK)) "
                + (tableDDL == null ? "" : tableDDL);
        LOGGER.debug(createTableSql);
        conn.createStatement().execute(createTableSql);

        String createIndexSql = "CREATE INDEX " + indexName + " ON " + tableName + " (PK1, INT_PK) include (V1,V2,V4) " + (async? "ASYNC":"")
                + " IMMUTABLE_STORAGE_SCHEME=SINGLE_CELL_ARRAY_WITH_OFFSETS, COLUMN_ENCODED_BYTES=2";
        LOGGER.debug(createIndexSql);
        conn.createStatement().execute(createIndexSql);

        String upsert = "UPSERT INTO " + tableName + " (PK1, INT_PK, V1,  V2, V3, V4, V5) VALUES (?,?,?,?,?,?,?)";
        PreparedStatement upsertStmt = conn.prepareStatement(upsert);

        for (int i=1; i <= numOfRows; i++) {
            upsertStmt.setString(1, "PK"+i);
            upsertStmt.setInt(2, i);
            upsertStmt.setString(3, "V1"+i);
            upsertStmt.setInt(4, i+1);
            upsertStmt.setInt(5, i+2);
            upsertStmt.setString(6, "V4"+i);
            upsertStmt.setString(7, "V5"+i);
            upsertStmt.executeUpdate();
        }
    }

    public static void dumpTable(String tableName) throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            Table
                    hTable = conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(tableName.getBytes());
            Scan scan = new Scan();
            scan.setRaw(true);
            LOGGER.info("***** Table Name : " + tableName);
            ResultScanner scanner = hTable.getScanner(scan);
            for (Result result = scanner.next(); result != null; result = scanner.next()) {
                for (Cell cell : result.rawCells()) {
                    String cellString = cell.toString();
                    LOGGER.info(cellString + " ****** value : " + Bytes.toStringBinary(CellUtil.cloneValue(cell)));
                }
            }
        }
    }
}
