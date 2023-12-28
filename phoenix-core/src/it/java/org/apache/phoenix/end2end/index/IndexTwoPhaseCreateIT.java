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

import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.end2end.IndexToolIT;
import org.apache.phoenix.end2end.transform.TransformToolIT;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.transform.SystemTransformRecord;
import org.apache.phoenix.schema.transform.Transform;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.phoenix.schema.PTable.ImmutableStorageScheme.ONE_CELL_PER_COLUMN;
import static org.apache.phoenix.schema.PTable.QualifierEncodingScheme.NON_ENCODED_QUALIFIERS;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.apache.phoenix.util.TestUtil.getRowCount;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class IndexTwoPhaseCreateIT extends BaseTest {
    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        Map<String, String> props = Maps.newHashMapWithExpectedSize(1);
        props.put(BaseScannerRegionObserverConstants.PHOENIX_MAX_LOOKBACK_AGE_CONF_KEY, Integer.toString(60 * 60)); // An hour
        props.put(QueryServices.USE_STATS_FOR_PARALLELIZATION, Boolean.toString(false));
        props.put(QueryServices.INDEX_CREATE_DEFAULT_STATE, PIndexState.CREATE_DISABLE.toString());
        props.put(QueryServices.DEFAULT_IMMUTABLE_STORAGE_SCHEME_ATTRIB, "ONE_CELL_PER_COLUMN");
        props.put(QueryServices.DEFAULT_COLUMN_ENCODED_BYTES_ATRRIB, "0");

        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }

    @AfterClass
    public static synchronized void freeResources() throws Exception {
        BaseTest.freeResourcesIfBeyondThreshold();
    }

    @Test
    public void testIndexCreateWithNonDefaultSettings() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String dataTable = generateUniqueName();
        String indexName = "IDX_" + generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            String tableDDL = "CREATE TABLE " + dataTable + TestUtil.TEST_TABLE_SCHEMA;
            conn.createStatement().execute(tableDDL);
            BaseTest.upsertRows(conn, dataTable, 2);
            String ddl = "CREATE INDEX " + indexName + " ON " + dataTable
                    + " (varchar_col1 ASC, varchar_col2 ASC, int_pk DESC)"
                    + " INCLUDE (int_col1, int_col2) ";
            conn.createStatement().execute(ddl);

            assertIndexOrTableState(conn, null, indexName, PTableType.INDEX, PIndexState.CREATE_DISABLE);

            assertEquals(0, getRowCount(conn, indexName));

            ddl = "ALTER INDEX " + indexName + " ON " + dataTable + " REBUILD ASYNC";
            conn.createStatement().execute(ddl);
            assertIndexOrTableState(conn, null, indexName, PTableType.INDEX, PIndexState.BUILDING);
        }
    }


    @Test
    public void testIndexCreateDisabledBuildAfter() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String dataTable = generateUniqueName();
        String indexName = "IDX_" + generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(true);
            String tableDDL = "CREATE TABLE " + dataTable + TestUtil.TEST_TABLE_SCHEMA;
            conn.createStatement().execute(tableDDL);
            BaseTest.upsertRows(conn, dataTable, 1);
            String ddl = "CREATE INDEX " + indexName + " ON " + dataTable
                    + " (varchar_col1 ASC, varchar_col2 ASC, int_pk DESC)"
                    + " INCLUDE (int_col1, int_col2) ";
            conn.createStatement().execute(ddl);

            assertIndexOrTableState(conn, null, indexName, PTableType.INDEX, PIndexState.CREATE_DISABLE);

            BaseTest.upsertRows(conn, dataTable, 3);
            long rows = getRowCount(conn, indexName);
            // Disabled table, rows don't go in
            assertEquals(0, rows);

            IndexToolIT.runIndexTool(false, null, dataTable, indexName);
            rows = getRowCount(conn, indexName);
            assertEquals(3, rows);
            assertIndexOrTableState(conn, null, indexName, PTableType.INDEX, PIndexState.ACTIVE);
        }
    }

    @Test
    public void testTransformingTableAndIndex() throws Exception {
        Properties testProps = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), testProps)) {
            conn.setAutoCommit(true);
            String tableName = "TBL_" + generateUniqueName();
            String idxName = "IND_" + generateUniqueName();

            String createTableSql = "CREATE TABLE " + tableName + " (PK1 VARCHAR NOT NULL, INT_PK INTEGER NOT NULL, " +
                    "V1 VARCHAR, V2 INTEGER CONSTRAINT NAME_PK PRIMARY KEY(PK1, INT_PK)) ";
            conn.createStatement().execute(createTableSql);

            String upsertStmt = "UPSERT INTO " + tableName + " (PK1, INT_PK, V1, V2) VALUES ('%s', %d, '%s', %d)";
            conn.createStatement().execute(String.format(upsertStmt, "a", 1, "val1", 1));

            // Note that index will not be built, since we create it with ASYNC
            String createIndexSql = "CREATE INDEX " + idxName + " ON " + tableName + " (PK1, INT_PK) include (V1)";
            conn.createStatement().execute(createIndexSql);
            assertMetadata(conn, ONE_CELL_PER_COLUMN, NON_ENCODED_QUALIFIERS, tableName);
            assertMetadata(conn, ONE_CELL_PER_COLUMN, NON_ENCODED_QUALIFIERS, idxName);

            assertEquals(0, getRowCount(conn, idxName));
            IndexToolIT.runIndexTool(false, null, tableName, idxName);
            assertEquals(1, getRowCount(conn, idxName));

            // Index transform
            conn.createStatement().execute("ALTER INDEX " + idxName + " ON " + tableName +
                    " ACTIVE IMMUTABLE_STORAGE_SCHEME=SINGLE_CELL_ARRAY_WITH_OFFSETS, COLUMN_ENCODED_BYTES=2");

            SystemTransformRecord record = Transform.getTransformRecord(null, idxName, tableName, null, conn.unwrap(PhoenixConnection.class));
            assertNotNull(record);
            assertMetadata(conn, PTable.ImmutableStorageScheme.SINGLE_CELL_ARRAY_WITH_OFFSETS, PTable.QualifierEncodingScheme.TWO_BYTE_QUALIFIERS, record.getNewPhysicalTableName());
            String newIndexTable = record.getNewPhysicalTableName();
            assertEquals(0, getRowCount(conn, newIndexTable));

            // Now do a table transform
            conn.createStatement().execute("ALTER TABLE " + tableName + " SET "
                    + " IMMUTABLE_STORAGE_SCHEME=SINGLE_CELL_ARRAY_WITH_OFFSETS, COLUMN_ENCODED_BYTES=2");
            record = Transform.getTransformRecord(null, tableName, null, null, conn.unwrap(PhoenixConnection.class));
            assertNotNull(record);
            assertMetadata(conn, PTable.ImmutableStorageScheme.SINGLE_CELL_ARRAY_WITH_OFFSETS, PTable.QualifierEncodingScheme.TWO_BYTE_QUALIFIERS, record.getNewPhysicalTableName());
            String newDataTable = record.getNewPhysicalTableName();
            assertEquals(0, getRowCount(conn, newDataTable));

            assertIndexOrTableState(conn, null, newDataTable, PTableType.TABLE, PIndexState.CREATE_DISABLE);

            // Now these live mutations should not go into the index table and the transforming table
            conn.createStatement().execute(String.format(upsertStmt, "b", 2, "val2", 2));
            conn.commit();
            assertEquals(0, getRowCount(conn, newDataTable));
            assertEquals(0, getRowCount(conn, newIndexTable));

            // Activate index and see that transforming table doesn't have records
            IndexToolIT.runIndexTool(false, null, tableName, newIndexTable);
            assertEquals(2, getRowCount(conn, newIndexTable));
            assertIndexOrTableState(conn, null, newIndexTable, PTableType.INDEX, PIndexState.ACTIVE);
            assertEquals(0, getRowCount(conn, newDataTable));

            // Now activate transforming table
            List<String> args = TransformToolIT.getArgList(null, tableName, null,
                    null, null, null, false, false, true, false, false);

            TransformToolIT.runTransformTool(args.toArray(new String[0]), 0);
            record = Transform.getTransformRecord(null, tableName, null, null, conn.unwrap(PhoenixConnection.class));
            TransformToolIT.assertTransformStatusOrPartial(PTable.TransformStatus.PENDING_CUTOVER, record);
            assertEquals(2, getRowCount(conn, newDataTable));
        }
    }

    @Test
    public void testWithViewIndex() throws Exception {
        Properties testProps = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), testProps)) {
            conn.setAutoCommit(true);
            String tableName = "TBL_" + generateUniqueName();
            String viewName = "VW_" + generateUniqueName();
            String viewIdxName = "VWIDX_" + generateUniqueName();

            String createTableSql = "CREATE TABLE " + tableName + " (PK1 VARCHAR NOT NULL, INT_PK INTEGER NOT NULL, " +
                    "V1 VARCHAR, V2 INTEGER CONSTRAINT NAME_PK PRIMARY KEY(PK1, INT_PK)) ";
            conn.createStatement().execute(createTableSql);

            String createViewSql = "CREATE VIEW " + viewName + " ( VIEW_COL1 INTEGER, VIEW_COL2 VARCHAR ) AS SELECT * FROM " + tableName;
            conn.createStatement().execute(createViewSql);

            String createViewIdxSql = "CREATE INDEX " + viewIdxName + " ON " + viewName + " (VIEW_COL1) include (VIEW_COL2) ";
            conn.createStatement().execute(createViewIdxSql);

            assertIndexOrTableState(conn, null, viewIdxName, PTableType.INDEX, PIndexState.ACTIVE);
        }
    }

    @Test
    public void testWithLocalIndex() throws Exception {
        Properties testProps = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String dataTableName = "TBL_" + generateUniqueName();
        String indexName = "LCLIDX_" + generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl(), testProps)) {
            String createTableSql = "CREATE TABLE " + dataTableName
                    + " (k INTEGER PRIMARY KEY, a bigint, b bigint, c bigint) ";
            conn.createStatement().execute(createTableSql);
            conn.createStatement().execute("CREATE LOCAL INDEX " + indexName + " ON " + dataTableName
                    + " (b) INCLUDE (c) ");
            assertIndexOrTableState(conn, null, indexName, PTableType.INDEX, PIndexState.ACTIVE);
        }
    }

    private void assertIndexOrTableState(Connection conn, String schema, String tblName, PTableType type,
                                         PIndexState state) throws SQLException {
        ResultSet rs = conn.getMetaData().getTables("", schema, tblName
                , new String[]{type.toString()});
        assertTrue(rs.next());
        assertEquals(tblName, rs.getString(3));
        assertEquals(state.toString(), rs.getString("INDEX_STATE"));
        assertFalse(rs.next());
    }
}
