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

import org.apache.phoenix.thirdparty.com.google.common.base.Strings;
import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import static org.apache.phoenix.schema.PTable.ImmutableStorageScheme.ONE_CELL_PER_COLUMN;
import static org.apache.phoenix.schema.PTable.QualifierEncodingScheme.NON_ENCODED_QUALIFIERS;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TransformIT extends ParallelStatsDisabledIT {
    private Properties testProps = PropertiesUtil.deepCopy(TEST_PROPERTIES);

    public TransformIT() {
        testProps.put(QueryServices.DEFAULT_IMMUTABLE_STORAGE_SCHEME_ATTRIB, "ONE_CELL_PER_COLUMN");
        testProps.put(QueryServices.DEFAULT_COLUMN_ENCODED_BYTES_ATRRIB, "0");
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
