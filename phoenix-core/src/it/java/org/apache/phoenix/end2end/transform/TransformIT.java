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
package org.apache.phoenix.end2end.transform;

import org.apache.phoenix.thirdparty.com.google.common.base.Strings;
import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.end2end.index.SingleCellIndexIT;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtilHelper;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.transform.SystemTransformRecord;
import org.apache.phoenix.schema.transform.Transform;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import static org.apache.phoenix.exception.SQLExceptionCode.CANNOT_TRANSFORM_LOCAL_OR_VIEW_INDEX;
import static org.apache.phoenix.exception.SQLExceptionCode.CANNOT_TRANSFORM_TABLE_WITH_LOCAL_INDEX;
import static org.apache.phoenix.exception.SQLExceptionCode.VIEW_WITH_PROPERTIES;
import static org.apache.phoenix.schema.PTable.ImmutableStorageScheme.ONE_CELL_PER_COLUMN;
import static org.apache.phoenix.schema.PTable.QualifierEncodingScheme.NON_ENCODED_QUALIFIERS;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.apache.phoenix.util.TestUtil.getRowCount;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TransformIT extends ParallelStatsDisabledIT {
    private Properties testProps = PropertiesUtil.deepCopy(TEST_PROPERTIES);

    public TransformIT() {
        testProps.put(QueryServices.DEFAULT_IMMUTABLE_STORAGE_SCHEME_ATTRIB, "ONE_CELL_PER_COLUMN");
        testProps.put(QueryServices.DEFAULT_COLUMN_ENCODED_BYTES_ATRRIB, "0");
        testProps.put(PhoenixConfigurationUtilHelper.TRANSFORM_MONITOR_ENABLED, Boolean.toString(false));
    }

    @Before
    public void setupTest() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl(), testProps)) {
            conn.setAutoCommit(true);
            conn.createStatement().execute("DELETE FROM " + PhoenixDatabaseMetaData.SYSTEM_TRANSFORM_NAME);
        }
    }

    @Test
    public void testSystemTransformTablePopulatedForIndex() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl(), testProps)) {
            conn.setAutoCommit(true);
            String tableName = "TBL_" + generateUniqueName();
            String idxName = "IND_" + generateUniqueName();

            String createTableSql = "CREATE TABLE " + tableName + " (PK1 VARCHAR NOT NULL, INT_PK INTEGER NOT NULL, " +
                    "V1 VARCHAR, V2 INTEGER, V3 INTEGER, V4 VARCHAR, V5 VARCHAR CONSTRAINT NAME_PK PRIMARY KEY(PK1, INT_PK)) ";
            conn.createStatement().execute(createTableSql);

            String createIndexSql = "CREATE INDEX " + idxName + " ON " + tableName + " (PK1, INT_PK) include (V1,V2,V4) ";
            conn.createStatement().execute(createIndexSql);
            assertMetadata(conn, ONE_CELL_PER_COLUMN, NON_ENCODED_QUALIFIERS, tableName);
            assertMetadata(conn, ONE_CELL_PER_COLUMN, NON_ENCODED_QUALIFIERS, idxName);

            assertSystemTransform(conn, 0, null, idxName, null);

            // Do an alter that doesn't require transform
            conn.createStatement().execute("ALTER INDEX " + idxName + " ON " + tableName + " ACTIVE ");
            assertSystemTransform(conn, 0, null, idxName, null);

            // Now do a transform alter and check
            conn.createStatement().execute("ALTER INDEX " + idxName + " ON " + tableName + " ACTIVE "
                    + " IMMUTABLE_STORAGE_SCHEME=SINGLE_CELL_ARRAY_WITH_OFFSETS, COLUMN_ENCODED_BYTES=2");
            assertSystemTransform(conn, 1, null, idxName, null);

            // Now do another alter and fail
            try {
                conn.createStatement().execute("ALTER INDEX " + idxName + " ON " + tableName + " ACTIVE "
                        + " IMMUTABLE_STORAGE_SCHEME=SINGLE_CELL_ARRAY_WITH_OFFSETS, COLUMN_ENCODED_BYTES=2");
                fail("This transform needs to fail");
            } catch (SQLException ex) {
                assertEquals( SQLExceptionCode.CANNOT_TRANSFORM_ALREADY_TRANSFORMING_TABLE.getErrorCode(), ex.getErrorCode());
            }
            assertSystemTransform(conn, 1, null, idxName, null);

            // Create another index and transform that one
            String idxName2 = "IND2_" + generateUniqueName();
            conn.createStatement().execute("CREATE INDEX " + idxName2 + " ON " + tableName + " (V2) include (V1) ");

            conn.createStatement().execute("ALTER INDEX " + idxName2 + " ON " + tableName + " ACTIVE COLUMN_ENCODED_BYTES=4");
            assertSystemTransform(conn, 2, null, idxName2, null);
        }
    }

    @Test
    public void testSystemTransformTablePopulatedForTable() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl(), testProps)) {
            conn.setAutoCommit(true);
            String tableName = "TBL_" + generateUniqueName();

            String createTableSql = "CREATE TABLE " + tableName + " (PK1 VARCHAR NOT NULL, INT_PK INTEGER NOT NULL, " +
                    "V1 VARCHAR, V2 INTEGER, V3 INTEGER, V4 VARCHAR, V5 VARCHAR CONSTRAINT NAME_PK PRIMARY KEY(PK1, INT_PK)) ";
            conn.createStatement().execute(createTableSql);

            assertMetadata(conn, ONE_CELL_PER_COLUMN, NON_ENCODED_QUALIFIERS, tableName);

            assertSystemTransform(conn, 0, null, tableName, null);

            // Do an alter that doesn't require transform
            conn.createStatement().execute("ALTER TABLE " + tableName + " SET TTL=300");
            assertSystemTransform(conn, 0, null, tableName, null);

            try {
                // Incorrectly set
                conn.createStatement().execute("ALTER TABLE " + tableName + " SET "
                        + " IMMUTABLE_STORAGE_SCHEME=SINGLE_CELL_ARRAY_WITH_OFFSETS");
                fail("IMMUTABLE_STORAGE_SCHEME=SINGLE_CELL_ARRAY_WITH_OFFSETS, COLUMN_ENCODED_BYTES=0 is not compatible");
            } catch (SQLException e) {
                assertEquals(SQLExceptionCode.INVALID_IMMUTABLE_STORAGE_SCHEME_AND_COLUMN_QUALIFIER_BYTES.getErrorCode(), e.getErrorCode());
            }
            try {
                // Incorrectly set
                conn.createStatement().execute("ALTER TABLE " + tableName + " SET "
                        + " IMMUTABLE_STORAGE_SCHEME=SINGLE_CELL_ARRAY_WITH_OFFSETS, COLUMN_ENCODED_BYTES=0");
                fail("IMMUTABLE_STORAGE_SCHEME=SINGLE_CELL_ARRAY_WITH_OFFSETS, COLUMN_ENCODED_BYTES=0 is not compatible");
            } catch (SQLException e) {
                assertEquals(SQLExceptionCode.INVALID_IMMUTABLE_STORAGE_SCHEME_AND_COLUMN_QUALIFIER_BYTES.getErrorCode(), e.getErrorCode());
            }

            // Now do a transform alter and check
            conn.createStatement().execute("ALTER TABLE " + tableName + " SET "
                    + " IMMUTABLE_STORAGE_SCHEME=SINGLE_CELL_ARRAY_WITH_OFFSETS, COLUMN_ENCODED_BYTES=2");
            assertSystemTransform(conn, 1, null, tableName, null);

            // Now do another alter and fail
            try {
                conn.createStatement().execute("ALTER TABLE " + tableName + " SET "
                        + " IMMUTABLE_STORAGE_SCHEME=SINGLE_CELL_ARRAY_WITH_OFFSETS, COLUMN_ENCODED_BYTES=2");
                fail("Transform should fail");
            } catch (SQLException ex) {
                assertEquals( SQLExceptionCode.CANNOT_TRANSFORM_ALREADY_TRANSFORMING_TABLE.getErrorCode(), ex.getErrorCode());
            }
            assertSystemTransform(conn, 1, null, tableName, null);

            String tableName2 = "TBL_" + generateUniqueName();

            createTableSql = "CREATE TABLE " + tableName2 + " (PK1 VARCHAR NOT NULL, INT_PK INTEGER NOT NULL, " +
                    "V1 VARCHAR, V2 INTEGER, V3 INTEGER, V4 VARCHAR, V5 VARCHAR CONSTRAINT NAME_PK PRIMARY KEY(PK1, INT_PK)) ";
            conn.createStatement().execute(createTableSql);
            conn.createStatement().execute("ALTER TABLE " + tableName2 + " SET COLUMN_ENCODED_BYTES=4");
            assertSystemTransform(conn, 2, null, tableName2, null);
        }
    }

    @Test
    public void testTransformFailsForViewIndex() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl(), testProps)) {
            conn.setAutoCommit(true);
            String schema = "S_" + generateUniqueName();
            String tableName = "TBL_" + generateUniqueName();
            String fullTableName = SchemaUtil.getTableName(schema, tableName);
            String viewName = "VW_" + generateUniqueName();
            String viewIdxName = "VWIDX_" + generateUniqueName();

            String createTableSql = "CREATE TABLE " + fullTableName + " (PK1 VARCHAR NOT NULL, INT_PK INTEGER NOT NULL, " +
                    "V1 VARCHAR, V2 INTEGER CONSTRAINT NAME_PK PRIMARY KEY(PK1, INT_PK)) ";
            conn.createStatement().execute(createTableSql);
            assertMetadata(conn, ONE_CELL_PER_COLUMN, NON_ENCODED_QUALIFIERS, fullTableName);

            String createViewSql = "CREATE VIEW " + viewName + " ( VIEW_COL1 INTEGER, VIEW_COL2 VARCHAR ) AS SELECT * FROM " + fullTableName;
            conn.createStatement().execute(createViewSql);

            String createViewIdxSql = "CREATE INDEX " + viewIdxName + " ON " + viewName + " (VIEW_COL1) include (VIEW_COL2) ";
            conn.createStatement().execute(createViewIdxSql);

            try {
                conn.createStatement().execute("ALTER INDEX " + viewIdxName + " ON "  + viewName
                        + " ACTIVE IMMUTABLE_STORAGE_SCHEME=SINGLE_CELL_ARRAY_WITH_OFFSETS, COLUMN_ENCODED_BYTES=2");
                fail();
            } catch (SQLException e) {
                assertEquals(CANNOT_TRANSFORM_LOCAL_OR_VIEW_INDEX.getErrorCode(), e.getErrorCode());
            }
        }
    }

    @Test
    public void testTransform_tableWithLocalIndex() throws Exception {
        String dataTableName = "TBL_" + generateUniqueName();
        String indexName = "LCLIDX_" + generateUniqueName();
        String createIndexStmt = "CREATE LOCAL INDEX %s ON " + dataTableName + " (NAME) INCLUDE (ZIP) ";
        try (Connection conn = DriverManager.getConnection(getUrl(), testProps)) {
            conn.setAutoCommit(true);
            int numOfRows = 1;
            TransformToolIT.createTableAndUpsertRows(conn, dataTableName, numOfRows, "");
            conn.createStatement().execute(String.format(createIndexStmt, indexName));

            SingleCellIndexIT.assertMetadata(conn, PTable.ImmutableStorageScheme.ONE_CELL_PER_COLUMN, PTable.QualifierEncodingScheme.NON_ENCODED_QUALIFIERS, dataTableName);
            try {
                conn.createStatement().execute("ALTER INDEX " + indexName + " ON " + dataTableName +
                        " ACTIVE IMMUTABLE_STORAGE_SCHEME=SINGLE_CELL_ARRAY_WITH_OFFSETS, COLUMN_ENCODED_BYTES=2");
                fail("Cannot transform local index");
            } catch (SQLException e) {
                assertEquals(CANNOT_TRANSFORM_LOCAL_OR_VIEW_INDEX.getErrorCode(), e.getErrorCode());
            }

            try {
                conn.createStatement().execute("ALTER TABLE " + dataTableName +
                        " SET IMMUTABLE_STORAGE_SCHEME=SINGLE_CELL_ARRAY_WITH_OFFSETS, COLUMN_ENCODED_BYTES=2");
                fail("Cannot transform table with local index");
            } catch (SQLException e) {
                assertEquals(CANNOT_TRANSFORM_TABLE_WITH_LOCAL_INDEX.getErrorCode(), e.getErrorCode());
            }

        }
    }

    @Test
    public void testTransformForLiveMutations_mutatingImmutableTable() throws Exception {
        testTransformForLiveMutations_mutatingTable(" IMMUTABLE_ROWS=TRUE");
    }

    @Test
    public void testTransformForLiveMutations_mutatingMutableTable() throws Exception {
        testTransformForLiveMutations_mutatingTable("");
    }

    private void testTransformForLiveMutations_mutatingTable(String tableDDL) throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl(), testProps)) {
            conn.setAutoCommit(true);
            String schema = "S_" + generateUniqueName();
            String tableName = "TBL_" + generateUniqueName();
            String idxName = "IND_" + generateUniqueName();
            String fullTableName = SchemaUtil.getTableName(schema, tableName);
            String fullIdxName = SchemaUtil.getTableName(schema, idxName);

            String createTableSql = "CREATE TABLE " + fullTableName + " (PK1 VARCHAR NOT NULL, INT_PK INTEGER NOT NULL, " +
                    "V1 VARCHAR, V2 INTEGER CONSTRAINT NAME_PK PRIMARY KEY(PK1, INT_PK)) " + tableDDL;
            conn.createStatement().execute(createTableSql);

            String upsertStmt = "UPSERT INTO " + fullTableName + " (PK1, INT_PK, V1, V2) VALUES ('%s', %d, '%s', %d)";
            conn.createStatement().execute(String.format(upsertStmt, "a", 1, "val1", 1));

            // Note that index will not be built, since we create it with ASYNC
            String createIndexSql = "CREATE INDEX " + idxName + " ON " + fullTableName + " (PK1, INT_PK) include (V1) ASYNC";
            conn.createStatement().execute(createIndexSql);
            assertMetadata(conn, ONE_CELL_PER_COLUMN, NON_ENCODED_QUALIFIERS, fullTableName);
            assertMetadata(conn, ONE_CELL_PER_COLUMN, NON_ENCODED_QUALIFIERS, fullIdxName);

            String idxName2 = "IND2_" + generateUniqueName();
            String fullIdxName2 = SchemaUtil.getTableName(schema, idxName2);
            conn.createStatement().execute("CREATE INDEX " + idxName2 + " ON " + fullTableName + " (V1) include (V2) ASYNC");

            // Now do a transform and check still the index table is empty since we didn't build it
            conn.createStatement().execute("ALTER TABLE " + fullTableName + " SET "
                    + " IMMUTABLE_STORAGE_SCHEME=SINGLE_CELL_ARRAY_WITH_OFFSETS, COLUMN_ENCODED_BYTES=2");
            SystemTransformRecord record = Transform.getTransformRecord(schema, tableName, null, null, conn.unwrap(PhoenixConnection.class));
            assertNotNull(record);
            assertMetadata(conn, PTable.ImmutableStorageScheme.SINGLE_CELL_ARRAY_WITH_OFFSETS, PTable.QualifierEncodingScheme.TWO_BYTE_QUALIFIERS, record.getNewPhysicalTableName());
            assertEquals(0, getRowCount(conn, fullIdxName));
            assertEquals(0, getRowCount(conn, fullIdxName2));

            // Now these live mutations should go into the index tables and the transforming table
            conn.createStatement().execute(String.format(upsertStmt, "b", 2, "val2", 2));
            conn.commit();
            conn.createStatement().execute(String.format(upsertStmt, "c", 3, "val3", 3));
            assertEquals(2, getRowCount(conn, fullIdxName));
            assertEquals(2, getRowCount(conn, fullIdxName2));
            assertEquals(2, getRowCount(conn, record.getNewPhysicalTableName()));

            // Delete one mutation
            conn.createStatement().execute("DELETE FROM " + fullTableName + " WHERE PK1='b'");
            assertEquals(1, getRowCount(conn, fullIdxName));
            assertEquals(1, getRowCount(conn, fullIdxName2));
            assertEquals(1, getRowCount(conn, record.getNewPhysicalTableName()));
        }
    }

    @Test
    public void testTransformForLiveMutations_mutatingBaseTableNoIndex() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl(), testProps)) {
            conn.setAutoCommit(true);
            String schema = "S_" + generateUniqueName();
            String tableName = "TBL_" + generateUniqueName();
            String fullTableName = SchemaUtil.getTableName(schema, tableName);

            String createTableSql = "CREATE TABLE " + fullTableName + " (PK1 VARCHAR NOT NULL, INT_PK INTEGER NOT NULL, " +
                    "V1 VARCHAR, V2 INTEGER CONSTRAINT NAME_PK PRIMARY KEY(PK1, INT_PK)) ";
            conn.createStatement().execute(createTableSql);
            assertMetadata(conn, ONE_CELL_PER_COLUMN, NON_ENCODED_QUALIFIERS, fullTableName);

            String upsertStmt = "UPSERT INTO " + fullTableName + " (PK1, INT_PK, V1, V2) VALUES ('%s', %d, '%s', %d)";
            conn.createStatement().execute(String.format(upsertStmt, "a", 1, "val1", 1));

            conn.createStatement().execute("ALTER TABLE " + fullTableName + " SET "
                    + " IMMUTABLE_STORAGE_SCHEME=SINGLE_CELL_ARRAY_WITH_OFFSETS, COLUMN_ENCODED_BYTES=2");
            SystemTransformRecord record = Transform.getTransformRecord(schema, tableName, null, null, conn.unwrap(PhoenixConnection.class));
            assertNotNull(record);
            assertMetadata(conn, PTable.ImmutableStorageScheme.SINGLE_CELL_ARRAY_WITH_OFFSETS, PTable.QualifierEncodingScheme.TWO_BYTE_QUALIFIERS, record.getNewPhysicalTableName());

            conn.createStatement().execute(String.format(upsertStmt, "b", 2, "val2", 2));
            conn.commit();
            conn.createStatement().execute(String.format(upsertStmt, "c", 3, "val3", 3));
            assertEquals(2, getRowCount(conn, record.getNewPhysicalTableName()));

            // Delete one mutation
            conn.createStatement().execute("DELETE FROM " + fullTableName + " WHERE PK1='b'");
            assertEquals(1, getRowCount(conn, record.getNewPhysicalTableName()));
        }
    }

    @Test
    public void testTransformForLiveMutations_mutatingMutableIndex() throws Exception {
        testTransformForLiveMutations_mutatingIndex("");
    }

    @Test
    public void testTransformForLiveMutations_mutatingImmutableIndex() throws Exception {
        testTransformForLiveMutations_mutatingIndex(" IMMUTABLE_ROWS=true");
    }

    private void testTransformForLiveMutations_mutatingIndex(String tableDDL) throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl(), testProps)) {
            conn.setAutoCommit(true);
            String schema = "S_" + generateUniqueName();
            String tableName = "TBL_" + generateUniqueName();
            String idxName = "IND_" + generateUniqueName();
            String fullTableName = SchemaUtil.getTableName(schema, tableName);
            String fullIdxName = SchemaUtil.getTableName(schema, idxName);

            String createTableSql = "CREATE TABLE " + fullTableName + " (PK1 VARCHAR NOT NULL, INT_PK INTEGER NOT NULL, " +
                    "V1 VARCHAR, V2 INTEGER CONSTRAINT NAME_PK PRIMARY KEY(PK1, INT_PK)) " + tableDDL;
            conn.createStatement().execute(createTableSql);

            // Note that index will not be built, since we create it with ASYNC
            String createIndexSql = "CREATE INDEX " + idxName + " ON " + fullTableName + " (PK1, INT_PK) include (V1) ASYNC";
            conn.createStatement().execute(createIndexSql);
            assertMetadata(conn, ONE_CELL_PER_COLUMN, NON_ENCODED_QUALIFIERS, fullIdxName);

            conn.createStatement().execute("ALTER INDEX " + idxName + " ON " + fullTableName +
                    " ACTIVE IMMUTABLE_STORAGE_SCHEME=SINGLE_CELL_ARRAY_WITH_OFFSETS, COLUMN_ENCODED_BYTES=2");

            SystemTransformRecord record = Transform.getTransformRecord(schema, idxName, fullTableName, null, conn.unwrap(PhoenixConnection.class));
            assertNotNull(record);
            assertMetadata(conn, PTable.ImmutableStorageScheme.SINGLE_CELL_ARRAY_WITH_OFFSETS, PTable.QualifierEncodingScheme.TWO_BYTE_QUALIFIERS, record.getNewPhysicalTableName());

            String upsertStmt = "UPSERT INTO " + fullTableName + " (PK1, INT_PK, V1, V2) VALUES ('%s', %d, '%s', %d)";
            // Now these live mutations should go into the index tables and the transforming table
            conn.createStatement().execute(String.format(upsertStmt, "b", 2, "val2", 2));
            conn.createStatement().execute(String.format(upsertStmt, "c", 3, "val3", 3));
            assertEquals(2, getRowCount(conn, fullIdxName));
            assertEquals(2, getRowCount(conn, record.getNewPhysicalTableName()));

            // Delete one mutation
            conn.createStatement().execute("DELETE FROM " + fullTableName + " WHERE PK1='b'");
            assertEquals(1, getRowCount(conn, fullIdxName));
            assertEquals(1, getRowCount(conn, record.getNewPhysicalTableName()));
        }

    }

    @Test
    public void testTransformForLiveMutations_mutatingMutableBaseTableForView() throws Exception {
        testTransformForLiveMutations_mutatingBaseTableForView("");
    }

    @Test
    public void testTransformForLiveMutations_mutatingImmutableBaseTableForView() throws Exception {
        testTransformForLiveMutations_mutatingBaseTableForView(" IMMUTABLE_ROWS=true");
    }

    private void testTransformForLiveMutations_mutatingBaseTableForView(String tableDDL) throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl(), testProps)) {
            conn.setAutoCommit(true);
            String schema = "S_" + generateUniqueName();
            String tableName = "TBL_" + generateUniqueName();
            String fullTableName = SchemaUtil.getTableName(schema, tableName);
            String parentViewName = "VWP_" + generateUniqueName();
            String viewName = "VW_" + generateUniqueName();
            String viewIdxName = "VWIDX_" + generateUniqueName();

            String createTableSql = "CREATE TABLE " + fullTableName + " (PK1 VARCHAR NOT NULL, INT_PK INTEGER NOT NULL, " +
                    "V1 VARCHAR, V2 INTEGER CONSTRAINT NAME_PK PRIMARY KEY(PK1, INT_PK)) " + tableDDL;
            conn.createStatement().execute(createTableSql);
            assertMetadata(conn, ONE_CELL_PER_COLUMN, NON_ENCODED_QUALIFIERS, fullTableName);

            String createParentViewSql = "CREATE VIEW " + parentViewName + " ( PARENT_VIEW_COL1 VARCHAR ) AS SELECT * FROM " + fullTableName;
            conn.createStatement().execute(createParentViewSql);

            String createViewSql = "CREATE VIEW " + viewName + " ( VIEW_COL1 INTEGER, VIEW_COL2 VARCHAR ) AS SELECT * FROM " + parentViewName;
            conn.createStatement().execute(createViewSql);

            String createViewIdxSql = "CREATE INDEX " + viewIdxName + " ON " + viewName + " (VIEW_COL1) include (VIEW_COL2) ";
            conn.createStatement().execute(createViewIdxSql);

            String upsertStmt = "UPSERT INTO " + viewName + " (PK1, INT_PK, V1, VIEW_COL1, VIEW_COL2) VALUES ('%s', %d, '%s', %d, '%s')";
            conn.createStatement().execute(String.format(upsertStmt, "a", 1, "val1", 1, "col2_1"));

            conn.createStatement().execute("ALTER TABLE " + fullTableName + " SET "
                    + " IMMUTABLE_STORAGE_SCHEME=SINGLE_CELL_ARRAY_WITH_OFFSETS, COLUMN_ENCODED_BYTES=2");
            SystemTransformRecord record = Transform.getTransformRecord(schema, tableName, null, null, conn.unwrap(PhoenixConnection.class));
            assertNotNull(record);
            assertMetadata(conn, PTable.ImmutableStorageScheme.SINGLE_CELL_ARRAY_WITH_OFFSETS, PTable.QualifierEncodingScheme.TWO_BYTE_QUALIFIERS, record.getNewPhysicalTableName());

            conn.createStatement().execute(String.format(upsertStmt, "b", 2, "val2", 2, "col2_2"));
            conn.createStatement().execute(String.format(upsertStmt, "c", 3, "val3", 3, "col2_3"));
            assertEquals(3, getRowCount(conn, viewName));
            assertEquals(3, getRowCount(conn, viewIdxName));
            // New table has 1 less since it is not aware of the previous record since we didn't run the TransformTool
            assertEquals(2, getRowCount(conn, record.getNewPhysicalTableName()));

            conn.createStatement().execute("DELETE FROM " + viewName + " WHERE VIEW_COL1=2");
            assertEquals(1, getRowCount(conn, record.getNewPhysicalTableName()));
            assertEquals(2, getRowCount(conn, viewName));
            assertEquals(2, getRowCount(conn, viewIdxName));
        }
    }

    @Test
    public void testTransformForView() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl(), testProps)) {
            conn.setAutoCommit(true);
            String schema = "S_" + generateUniqueName();
            String tableName = "TBL_" + generateUniqueName();
            String fullTableName = SchemaUtil.getTableName(schema, tableName);
            String viewName = "VW_" + generateUniqueName();
            String fullViewName = SchemaUtil.getTableName(schema, viewName);

            String createTableSql = "CREATE TABLE " + fullTableName + " (PK1 VARCHAR NOT NULL, INT_PK INTEGER NOT NULL, " +
                    "V1 VARCHAR, V2 INTEGER CONSTRAINT NAME_PK PRIMARY KEY(PK1, INT_PK)) ";
            conn.createStatement().execute(createTableSql);

            String createParentViewSql = "CREATE VIEW " + viewName + " ( VIEW_COL1 VARCHAR ) AS SELECT * FROM " + fullTableName;
            conn.createStatement().execute(createParentViewSql);

            try {
                conn.createStatement().execute("ALTER TABLE " + viewName + " SET "
                        + " IMMUTABLE_STORAGE_SCHEME=SINGLE_CELL_ARRAY_WITH_OFFSETS, COLUMN_ENCODED_BYTES=2");
                fail("View transform should fail");
            } catch (SQLException e) {
                assertEquals(e.getErrorCode(), VIEW_WITH_PROPERTIES.getErrorCode());
            }
        }
    }

    @Test
    public void testAlterNotNeedsToTransformDueToSameProps() throws Exception {
        String schema = "S_" + generateUniqueName();
        String tableName = "TBL_" + generateUniqueName();
        String indexName = "IDX_" + generateUniqueName();
        String fullTableName = SchemaUtil.getTableName(schema, tableName);
        String fullIdxName = SchemaUtil.getTableName(schema, tableName);
        try (Connection conn = DriverManager.getConnection(getUrl(), testProps)) {
            conn.setAutoCommit(true);

            String createTableSql = "CREATE TABLE " + fullTableName + " (PK1 VARCHAR NOT NULL, INT_PK INTEGER NOT NULL, " +
                    "V1 VARCHAR, V2 INTEGER, V3 INTEGER, V4 VARCHAR, V5 VARCHAR CONSTRAINT NAME_PK PRIMARY KEY(PK1, INT_PK)) " +
                    "IMMUTABLE_STORAGE_SCHEME=SINGLE_CELL_ARRAY_WITH_OFFSETS, COLUMN_ENCODED_BYTES=2";
            conn.createStatement().execute(createTableSql);
            assertMetadata(conn, PTable.ImmutableStorageScheme.SINGLE_CELL_ARRAY_WITH_OFFSETS, PTable.QualifierEncodingScheme.TWO_BYTE_QUALIFIERS, fullTableName);

            // Now do an unnecessary transform
            conn.createStatement().execute("ALTER TABLE " + fullTableName + " SET "
                    + " IMMUTABLE_STORAGE_SCHEME=SINGLE_CELL_ARRAY_WITH_OFFSETS, COLUMN_ENCODED_BYTES=2");
            SystemTransformRecord record = Transform.getTransformRecord(schema, tableName, null, null, conn.unwrap(PhoenixConnection.class));
            assertNull(record);

            String createIndexSql = "CREATE INDEX " + indexName + " ON " + fullTableName + " (PK1, INT_PK) include (V1) ASYNC";
            conn.createStatement().execute(createIndexSql);
            assertMetadata(conn, PTable.ImmutableStorageScheme.SINGLE_CELL_ARRAY_WITH_OFFSETS, PTable.QualifierEncodingScheme.TWO_BYTE_QUALIFIERS, fullIdxName);

            conn.createStatement().execute("ALTER INDEX " + indexName + " ON " + fullTableName +
                    " ACTIVE IMMUTABLE_STORAGE_SCHEME=SINGLE_CELL_ARRAY_WITH_OFFSETS, COLUMN_ENCODED_BYTES=2");

            record = Transform.getTransformRecord(schema, indexName, tableName, null, conn.unwrap(PhoenixConnection.class));
            assertNull(record);
        }
    }

    @Test
    public void testDropAfterTransform() throws Exception {
        String schema = "S_" + generateUniqueName();
        String tableName = "TBL_" + generateUniqueName();
        String viewName = "VW_" + generateUniqueName();
        String indexName = "IDX_" + generateUniqueName();
        String fullTableName = SchemaUtil.getTableName(schema, tableName);

        try (Connection conn = DriverManager.getConnection(getUrl(), testProps)) {
            conn.setAutoCommit(true);

            String createTableSql = "CREATE TABLE " + fullTableName + " (PK1 VARCHAR NOT NULL, INT_PK INTEGER NOT NULL, " +
                    "V1 VARCHAR, V2 INTEGER CONSTRAINT NAME_PK PRIMARY KEY(PK1, INT_PK)) ";
            conn.createStatement().execute(createTableSql);
            assertMetadata(conn, ONE_CELL_PER_COLUMN, NON_ENCODED_QUALIFIERS, fullTableName);

            String upsertStmt = "UPSERT INTO " + fullTableName + " (PK1, INT_PK, V1, V2) VALUES ('%s', %d, '%s', %d)";
            conn.createStatement().execute(String.format(upsertStmt, "a", 1, "val1", 1));

            String createParentViewSql = "CREATE VIEW " + viewName + " ( VIEW_COL1 VARCHAR ) AS SELECT * FROM " + fullTableName;
            conn.createStatement().execute(createParentViewSql);

            conn.createStatement().execute("CREATE INDEX " + indexName + " ON " + fullTableName + " (V2) include (V1) ");

            conn.createStatement().execute("ALTER INDEX " + indexName + " ON " + fullTableName +
                    " ACTIVE IMMUTABLE_STORAGE_SCHEME=SINGLE_CELL_ARRAY_WITH_OFFSETS, COLUMN_ENCODED_BYTES=2");
            assertSystemTransform(conn.unwrap(PhoenixConnection.class), 1, schema, indexName, null);
            conn.createStatement().execute("DROP INDEX " + indexName + " ON " + fullTableName);

            conn.createStatement().execute("ALTER TABLE " + fullTableName + " SET "
                    + " IMMUTABLE_STORAGE_SCHEME=SINGLE_CELL_ARRAY_WITH_OFFSETS, COLUMN_ENCODED_BYTES=2");
            SystemTransformRecord record = Transform.getTransformRecord(schema, tableName, null, null, conn.unwrap(PhoenixConnection.class));
            assertNotNull(record);
            conn.createStatement().execute("DROP VIEW " + viewName);
            conn.createStatement().execute("DROP TABLE " + fullTableName);
        }
    }


    private void assertSystemTransform(Connection conn, int rowCount, String schema, String logicalTableName, String tenantId) throws SQLException {
        ResultSet rs = conn.createStatement().executeQuery("SELECT /*+ NO_INDEX */ count(*) FROM "+
                PhoenixDatabaseMetaData.SYSTEM_TRANSFORM_NAME);
        assertTrue(rs.next());
        assertEquals(rowCount, rs.getInt(1));

        if (rowCount > 0) {
            rs = conn.createStatement().executeQuery("SELECT TABLE_SCHEM, TRANSFORM_TYPE FROM " + PhoenixDatabaseMetaData.SYSTEM_TRANSFORM_NAME
                    + " WHERE LOGICAL_TABLE_NAME='" + logicalTableName + "'"
                    + (Strings.isNullOrEmpty(schema)? "" : " AND TABLE_SCHEM='"+ schema + "' ")
                    + (Strings.isNullOrEmpty(tenantId)? "" : " AND TENANT_ID='"+ tenantId + "' ")
            );
            assertTrue(rs.next());
            assertEquals(schema, rs.getString(1));
            assertEquals(PTable.TransformType.METADATA_TRANSFORM.getSerializedValue(), rs.getInt(2));
            assertFalse(rs.next());
        }
    }
}
