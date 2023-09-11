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
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/*
    After 4.15 release, Phoenix introduced VIEW_INDEX_ID_COLUMN_TYPE and changed VIEW_INDEX_ID data
    type from SMALLINT to BIGINT. However, SELECT from syscat doesn't have the right view index id
    because the VIEW_INDEX_ID column always assume the data type is BIGINT. PHOENIX-5712 introduced
    a coproc that checks the client request version and send it back to the client.
    For more information, please see PHOENIX-3547, PHOENIX-5712
 */
@Category(NeedsOwnMiniClusterTest.class)
public class ViewIndexIdRetrieveIT extends BaseTest {
    private final String BASE_TABLE_DDL = "CREATE TABLE %s (TENANT_ID CHAR(15) NOT NULL, " +
            "ID CHAR(3) NOT NULL, NUM BIGINT CONSTRAINT PK PRIMARY KEY (TENANT_ID, ID))" +
            " MULTI_TENANT = true, COLUMN_ENCODED_BYTES=0 ";
    private final String VIEW_DDL = "CREATE VIEW %s (A BIGINT PRIMARY KEY, B BIGINT)" +
            " AS SELECT * FROM %s WHERE ID='ABC'";
    private final String VIEW_INDEX_DDL =
            "CREATE INDEX %s ON %s (B DESC) INCLUDE (NUM)";
    private final String SELECT_ALL = "SELECT * FROM SYSTEM.CATALOG";
    private final String SELECT_ROW = "SELECT VIEW_INDEX_ID,VIEW_INDEX_ID_DATA_TYPE" +
            " FROM SYSTEM.CATALOG WHERE TABLE_NAME='%s' AND COLUMN_COUNT IS NOT NULL";

    @BeforeClass
    public static synchronized void setUp() throws Exception {
        Map<String, String> props = new HashMap<>();
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
            stmt.execute(String.format(BASE_TABLE_DDL, fullTableName));
            stmt.execute(String.format(VIEW_DDL, viewFullName, fullTableName));
            stmt.execute(String.format(VIEW_INDEX_DDL, viewIndexName, viewFullName));

            ResultSet rs = stmt.executeQuery(String.format(SELECT_ROW, viewIndexName));
            rs.next();
            // even we enabled longViewIndex config, but the sequence always starts at smallest short
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
            stmt.execute(String.format(BASE_TABLE_DDL, fullTableName));
            stmt.execute(String.format(VIEW_DDL, viewFullName, fullTableName));
            stmt.execute(String.format(VIEW_INDEX_DDL, viewIndexName1, viewFullName));

            ResultSet rs = stmt.executeQuery(String.format(SELECT_ROW, viewIndexName1));
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
            stmt.execute(String.format(VIEW_INDEX_DDL, viewIndexName2, viewFullName));

            ResultSet rs = stmt.executeQuery(String.format(SELECT_ROW, viewIndexName2));
            rs.next();
            assertEquals(Short.MIN_VALUE + 1,rs.getLong(1));
            assertEquals(Types.SMALLINT, rs.getInt(2));
            assertFalse(rs.next());
        }

        // check select * from syscat
        try (final Connection conn = DriverManager.getConnection(url);
             final Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery(String.format(SELECT_ALL));
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