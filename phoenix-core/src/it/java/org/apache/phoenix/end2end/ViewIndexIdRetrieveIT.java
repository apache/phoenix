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

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_SCHEM;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.VIEW_INDEX_ID;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.VIEW_INDEX_ID_DATA_TYPE;
import static org.apache.phoenix.query.QueryServices.LONG_VIEW_INDEX_ENABLED_ATTRIB;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Types;
import java.util.Map;
import java.util.Properties;

import com.google.common.collect.Maps;
import org.apache.hadoop.hbase.regionserver.ScanInfoUtil;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.BeforeClass;
import org.junit.Test;

public class ViewIndexIdRetrieveIT extends ParallelStatsDisabledIT {
    private String ddlForBaseTable = "CREATE TABLE %s (TENANT_ID CHAR(15) NOT NULL, ID CHAR(3)" +
            " NOT NULL, NUM BIGINT CONSTRAINT PK PRIMARY KEY (TENANT_ID, ID))" +
            " MULTI_TENANT = true, COLUMN_ENCODED_BYTES=0 ";
    private String ddlForView = "CREATE VIEW %s (A BIGINT PRIMARY KEY, B BIGINT)" +
            " AS SELECT * FROM %s WHERE ID='ABC'";
    private String ddlForViewIndex =
            "CREATE INDEX %s ON %s (B DESC) INCLUDE (NUM)";
    private String selectAll = "SELECT * FROM SYSTEM.CATALOG";
    private String selectRow = "SELECT VIEW_INDEX_ID,VIEW_INDEX_ID_DATA_TYPE" +
            " FROM SYSTEM.CATALOG WHERE TABLE_NAME='%s' AND COLUMN_COUNT IS NOT NULL";

    @BeforeClass
    public static void setUp() throws Exception {
        Map<String, String> props = Maps.newHashMapWithExpectedSize(1);
        props.put(ScanInfoUtil.PHOENIX_MAX_LOOKBACK_AGE_CONF_KEY, Integer.toString(60*60));
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }

    @Test
    public void testSelectViewIndexIdAsLong() throws Exception {
        testSelectViewIndexId(true);
    }

    @Test
    public void testSelectViewIndexIdAsShort() throws Exception {
        testSelectViewIndexId(false);
    }

    private void testSelectViewIndexId(boolean isTestingLongViewIndexId) throws Exception {
        String val = isTestingLongViewIndexId ? "true" : "false";
        int expectedDataType = isTestingLongViewIndexId ? Types.BIGINT : Types.SMALLINT;
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(LONG_VIEW_INDEX_ENABLED_ATTRIB, val);
        String schema = generateUniqueName();
        String baseTable = generateUniqueName();
        String fullTableName = SchemaUtil.getTableName(schema, baseTable);
        String viewName = generateUniqueName();
        String viewFullName = SchemaUtil.getTableName(schema, viewName);
        String viewIndexName = generateUniqueName();
        try (final Connection conn = DriverManager.getConnection(url,props);
             final Statement stmt = conn.createStatement()) {
            stmt.execute(String.format(ddlForBaseTable, fullTableName));
            stmt.execute(String.format(ddlForView, viewFullName, fullTableName));
            stmt.execute(String.format(ddlForViewIndex, viewIndexName, viewFullName));

            ResultSet rs = stmt.executeQuery(String.format(selectRow, viewIndexName));
            rs.next();
            assertEquals(Short.MIN_VALUE,rs.getLong(1));
            assertEquals(expectedDataType, rs.getInt(2));
            assertFalse(rs.next());
        }
    }

    @Test
    public void testMixedCase() throws Exception {
        // mixed case
        Properties propsForLongType = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        propsForLongType.setProperty(LONG_VIEW_INDEX_ENABLED_ATTRIB, "true");
        String schema = generateUniqueName();
        String baseTable = generateUniqueName();
        String fullTableName = SchemaUtil.getTableName(schema, baseTable);
        String viewName = generateUniqueName();
        String viewFullName = SchemaUtil.getTableName(schema, viewName);
        String viewIndexName1 = generateUniqueName();

        // view index id data type is long
        try (final Connection conn = DriverManager.getConnection(url,propsForLongType);
             final Statement stmt = conn.createStatement()) {
            stmt.execute(String.format(ddlForBaseTable, fullTableName));
            stmt.execute(String.format(ddlForView, viewFullName, fullTableName));
            stmt.execute(String.format(ddlForViewIndex, viewIndexName1, viewFullName));

            ResultSet rs = stmt.executeQuery(String.format(selectRow, viewIndexName1));
            rs.next();
            assertEquals(Short.MIN_VALUE,rs.getLong(1));
            assertEquals(Types.BIGINT, rs.getInt(2));
            assertFalse(rs.next());
        }

        // view index id data type is short
        String viewIndexName2 = generateUniqueName();
        Properties propsForShortType = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        propsForShortType.setProperty(LONG_VIEW_INDEX_ENABLED_ATTRIB, "false");
        try (final Connection conn = DriverManager.getConnection(url,propsForShortType);
             final Statement stmt = conn.createStatement()) {
            stmt.execute(String.format(ddlForViewIndex, viewIndexName2, viewFullName));

            ResultSet rs = stmt.executeQuery(String.format(selectRow, viewIndexName2));
            rs.next();
            assertEquals(Short.MIN_VALUE + 1,rs.getLong(1));
            assertEquals(Types.SMALLINT, rs.getInt(2));
            assertFalse(rs.next());
        }

        // check select * from syscat
        try (final Connection conn = DriverManager.getConnection(url);
             final Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery(String.format(selectAll));
            boolean checkShort = false;
            boolean checkLong = false;
            while (rs.next()) {

                String schemaName = rs.getString(TABLE_SCHEM);
                long viewIndexId = rs.getLong(VIEW_INDEX_ID);
                if (schemaName != null && schemaName.equals(schema) && viewIndexId != 0) {
                    int viewIndexIdDataType = rs.getInt(VIEW_INDEX_ID_DATA_TYPE);
                    String tableName = rs.getString(TABLE_NAME);
                    if (tableName.equals(viewIndexName1)) {
                        assertEquals(Short.MIN_VALUE, viewIndexId);
                        assertEquals(Types.BIGINT, viewIndexIdDataType);
                        checkLong = true;
                    } else if (tableName.equals(viewIndexName2)) {
                        assertEquals(Short.MIN_VALUE + 1, viewIndexId);
                        assertEquals(Types.SMALLINT, viewIndexIdDataType);
                        checkShort = true;
                    }
                }
            }
            assertTrue(checkLong);
            assertTrue(checkShort);
        }
    }
}
